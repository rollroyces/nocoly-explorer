"""Per-partition Parquet writer with append-mode row-group flushing."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import pyarrow as pa
import pyarrow.parquet as pq

if TYPE_CHECKING:
    import pyarrow.fs

from .options import ParquetExportOptions

_LOGGER = logging.getLogger(__name__)


class ParquetPartitionWriter:
    """Wraps pyarrow.parquet.ParquetWriter for append-mode writes to one file.

    Writes locally by default. Pass ``filesystem`` to write to any
    ``pyarrow.fs.FileSystem`` (S3 via ``pyarrow.fs.S3FileSystem`` is the
    common case). With a non-local filesystem, ``self.path`` is the path
    *within* that filesystem and parent-directory creation is a no-op.
    """

    def __init__(
        self,
        path: Path,
        schema: pa.Schema,
        options: ParquetExportOptions,
        filesystem: Optional["pa.fs.FileSystem"] = None,
    ) -> None:
        self.path = path
        self.schema = schema
        self.options = options
        self.filesystem = filesystem
        self._writer: Optional[pq.ParquetWriter] = None
        self.rows_written: int = 0
        self.bytes_written: int = 0

    def _ensure_writer(self) -> pq.ParquetWriter:
        if self._writer is None:
            if self.filesystem is None:
                self.path.parent.mkdir(parents=True, exist_ok=True)
            else:
                # Best-effort: ask the filesystem to create the parent
                # directory. PyArrow's S3FileSystem accepts create_dir for
                # key-prefix "directories"; LocalFileSystem creates real
                # directories. Silently ignore errors so a non-creatable
                # filesystem (e.g. read-only) doesn't fail here.
                from pyarrow.fs import FileSelector
                parent = str(self.path).rsplit("/", 1)[0] if "/" in str(self.path) else ""
                if parent:
                    try:
                        self.filesystem.create_dir(parent, recursive=True)
                    except Exception:
                        pass
            write_kwargs = {
                "where": str(self.path),
                "schema": self.schema,
                "compression": self.options.compression,
                "write_statistics": self.options.write_statistics,
                "use_dictionary": True,
            }
            if self.filesystem is not None:
                write_kwargs["filesystem"] = self.filesystem
            self._writer = pq.ParquetWriter(**write_kwargs)
        return self._writer

    def write_chunk(self, table: pa.Table) -> None:
        if table.num_rows == 0:
            return
        writer = self._ensure_writer()
        writer.write_table(table)
        self.rows_written += table.num_rows
        self.bytes_written += table.nbytes

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None
            _LOGGER.debug(
                "streaming_parquet.partition_closed",
                extra={"path": str(self.path), "rows": self.rows_written},
            )