"""Streaming Parquet exporter for Nocoly worksheet data."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Protocol,
    Sequence,
)

import pyarrow as pa

from ..exceptions import JobCancelled, OutputValidationError
from .buffer import RowGroupBuffer
from .options import (
    ParquetExportOptions,
    PartitionSpec,
    resolve_partition_path,
    validate_output_dir,
)
from .partitions import PartitionRouter
from .schema import ParquetSchemaManager
from .writer import ParquetPartitionWriter

if TYPE_CHECKING:  # pragma: no cover - only for type hints
    from ..async_client import AsyncWorksheetClient

_LOGGER = logging.getLogger("nocoly_explorer.streaming")


class WorksheetClientLike(Protocol):
    """Minimum interface needed from a worksheet client."""

    def fetch_rows(
        self,
        *,
        page_size: int = 200,
        max_pages: int = 1000,
        filter_criteria: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]: ...


@dataclass(slots=True)
class StreamingExportConfig:
    output_dir: Path
    worksheet_id: str
    options: ParquetExportOptions = field(default_factory=ParquetExportOptions)
    partition: Optional[PartitionSpec] = None
    # Optional pyarrow.fs.FileSystem for writing to non-local destinations
    # (e.g. S3 via pyarrow.fs.S3FileSystem). When set, output_dir is treated
    # as a path within this filesystem and validate_output_dir is skipped.
    filesystem: Optional["pa.fs.FileSystem"] = None


@dataclass(slots=True)
class ExportResult:
    rows_written: int
    partitions: Dict[str, int]
    output_dir: Path
    bytes_written: int = 0
    # Max value of any tracked update column (default "_updatedAt") seen
    # across the rows ingested during this export. None if no rows carried
    # the column or the export produced zero rows. Used by incremental-
    # sync callers to update the watermark.
    max_updated_at: Optional[str] = None


_UNPARTITIONED_KEY = "__unpartitioned__"


class StreamingExporter:
    """Stream pages from a worksheet client to Parquet files on disk."""

    def __init__(self, client: WorksheetClientLike, config: StreamingExportConfig) -> None:
        self.client = client
        self.config = config
        if config.filesystem is None:
            self.output_dir = validate_output_dir(config.output_dir)
        else:
            # Non-local destination: filesystem is responsible for path
            # resolution. We keep output_dir as a Path for display purposes.
            self.output_dir = config.output_dir
        self.schema_manager = ParquetSchemaManager(config.options)
        if config.partition is not None:
            self.router = PartitionRouter(
                config.partition,
                partition_column=config.partition.column,
                max_cardinality=config.options.max_partition_cardinality,
            )
        else:
            self.router = PartitionRouter(
                PartitionSpec(column=""),  # placeholder; not used since partition_column=None
                partition_column=None,
                max_cardinality=config.options.max_partition_cardinality,
            )
        self._writers: Dict[str, ParquetPartitionWriter] = {}
        self._buffers: Dict[str, RowGroupBuffer] = {}
        self._rows_seen = 0
        self._bytes_written = 0
        self._max_updated_at: Optional[str] = None

    def export(
        self,
        *,
        page_size: int = 200,
        max_pages: int = 1000,
        filter_criteria: Optional[Dict[str, Any]] = None,
    ) -> ExportResult:
        rows = self.client.fetch_rows(
            page_size=page_size,
            max_pages=max_pages,
            filter_criteria=filter_criteria,
        )
        # The client may return either a flat list or a list of pages.
        if isinstance(rows, list) and rows and isinstance(rows[0], list):
            for page in rows:
                self._ingest(page)
        else:
            self._ingest(rows)
        self._flush_all()
        self._close_all()
        return ExportResult(
            rows_written=self._rows_seen,
            partitions={k: w.rows_written for k, w in self._writers.items()},
            output_dir=self.output_dir,
            bytes_written=self._bytes_written,
            max_updated_at=self._max_updated_at,
        )

    async def stream_async(
        self,
        client: "AsyncWorksheetClient",
        *,
        page_size: int = 200,
        max_pages: int = 1000,
        filter_criteria: Optional[Dict[str, Any]] = None,
        cancel_check: Optional[Callable[[], Awaitable[bool]]] = None,
    ) -> ExportResult:
        """Stream pages from an ``AsyncWorksheetClient`` into Parquet on disk.

        Each page yielded by ``client.fetch_pages_async()`` is ingested into
        the schema manager / partition router / row-group buffer as soon as
        it arrives. Total memory stays bounded by
        ``row_group_bytes * partitions`` plus the in-flight pages
        (``concurrency * page_size`` rows at most).

        ``cancel_check`` is an optional async callable returning ``True`` to
        request cooperative cancellation. Checked before each new page; on
        ``True``, all open writers are flushed/closed and :class:`JobCancelled`
        is raised. Writers are also flushed/closed on any other exception so
        partial files are not left dangling.

        The ``client`` argument is the ``AsyncWorksheetClient`` (or any
        object exposing ``fetch_pages_async``); ``self.client`` is not
        consulted on this code path.
        """
        try:
            async for page in client.fetch_pages_async(
                page_size=page_size,
                max_pages=max_pages,
                filter_criteria=filter_criteria,
                cancel_check=cancel_check,
            ):
                if cancel_check is not None and await cancel_check():
                    raise JobCancelled(
                        "Cancellation requested mid-stream"
                    )
                self._ingest(page)
        finally:
            # Always flush + close, even on cancel/error, so writers do not
            # leave dangling file handles.
            try:
                self._flush_all()
            finally:
                self._close_all()
        return ExportResult(
            rows_written=self._rows_seen,
            partitions={k: w.rows_written for k, w in self._writers.items()},
            output_dir=self.output_dir,
            bytes_written=self._bytes_written,
            max_updated_at=self._max_updated_at,
        )

    def _ingest(self, rows: Sequence[Dict[str, Any]]) -> None:
        if not rows:
            return
        # Step 0: track max(_updatedAt) for incremental-sync callers.
        # ISO 8601 strings sort lexicographically, so a plain > works.
        for row in rows:
            v = row.get("_updatedAt")
            if v is None:
                continue
            s = str(v)
            if self._max_updated_at is None or s > self._max_updated_at:
                self._max_updated_at = s
        # Step 1: schema manager validates/infers schema.
        self.schema_manager.ingest_page(rows)
        # Step 2: bucket rows by partition key.
        buckets: Dict[Optional[str], List[Dict[str, Any]]] = {}
        for row in rows:
            key = (
                self.router.key_for(row)
                if self.router.partition_column is not None
                else None
            )
            buckets.setdefault(key, []).append(row)
        # Step 3: convert each bucket to a PyArrow table and push through buffer.
        schema = self.schema_manager.schema
        schema_keys = [f.name for f in schema]
        for key, bucket_rows in buckets.items():
            projected = [{k: r.get(k) for k in schema_keys} for r in bucket_rows]
            table = pa.Table.from_pylist(projected, schema=schema)
            writer = self._get_writer(key)
            buffer = self._get_buffer(key, writer.schema)
            for chunk in buffer.add(table):
                writer.write_chunk(chunk)
                self._bytes_written += chunk.nbytes
            self._rows_seen += len(bucket_rows)

    def _get_writer(self, key: Optional[str]) -> ParquetPartitionWriter:
        dict_key = key if key is not None else _UNPARTITIONED_KEY
        if dict_key not in self._writers:
            target = self._resolve_target_path(key)
            self._writers[dict_key] = ParquetPartitionWriter(
                target,
                self.schema_manager.schema,
                self.config.options,
                filesystem=self.config.filesystem,
            )
        return self._writers[dict_key]

    def _resolve_target_path(self, key: Optional[str]):
        """Resolve the output path for one partition.

        Local: returns a pathlib.Path (``output_dir / data_0.parquet``).
        Non-local (filesystem is set): returns a string path within that
        filesystem, e.g. ``"s3://bucket/key/region=HK/data_0.parquet"``.
        """
        if self.config.filesystem is None:
            # Local filesystem path handling
            if key is None:
                return self.output_dir / "data_0.parquet"
            part_dir = resolve_partition_path(self.output_dir, key)
            return part_dir / "data_0.parquet"
        # Non-local filesystem: string-based concat. output_dir may be
        # Path (for display) but the partition path is built as a string.
        base = str(self.output_dir)
        if not base.endswith("/"):
            base = base + "/"
        if key is None:
            return base + "data_0.parquet"
        return base + key + "/data_0.parquet"

    def _get_buffer(self, key: Optional[str], schema: pa.Schema) -> RowGroupBuffer:
        dict_key = key if key is not None else _UNPARTITIONED_KEY
        if dict_key not in self._buffers:
            self._buffers[dict_key] = RowGroupBuffer(schema, self.config.options.row_group_bytes)
        return self._buffers[dict_key]

    def _flush_all(self) -> None:
        for key, buffer in self._buffers.items():
            writer = self._writers[key]
            for chunk in buffer.flush():
                writer.write_chunk(chunk)
                self._bytes_written += chunk.nbytes

    def _close_all(self) -> None:
        for writer in self._writers.values():
            writer.close()


class _NoopClientAdapter:
    """No-op ``WorksheetClientLike`` for use with :meth:`StreamingExporter.stream_async`.

    ``stream_async`` never calls ``fetch_rows`` on ``self.client``; this
    adapter makes the contract explicit and keeps static checkers happy when
    constructing an exporter without a worksheet client.
    """

    def fetch_rows(self, **_kwargs):  # pragma: no cover - never invoked
        raise RuntimeError(
            "StreamingExporter was constructed for stream_async; "
            "do not call export() — use stream_async(client)."
        )
