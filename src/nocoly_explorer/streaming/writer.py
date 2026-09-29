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

    Crash-safety (local filesystem only): the writer opens a sibling
    ``.tmp`` file and only renames it to the final path on successful
    ``close()``. If the worker is killed mid-write, only the ``.tmp``
    file is left behind, and the final ``.parquet`` is never touched.
    A subsequent export overwrites any stale ``.tmp``. Non-local
    filesystems (S3, etc.) do not support atomic rename, so the
    crash-safety guarantee applies only to local destinations.
    """

    _TMP_SUFFIX = ".tmp"

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
        self._finalized = False  # set when rename has happened
        self.rows_written: int = 0
        self.bytes_written: int = 0

    @property
    def _tmp_path(self):
        """Path (or URI) of the in-progress temp file."""
        return f"{self.path}{self._TMP_SUFFIX}"

    def _ensure_writer(self) -> pq.ParquetWriter:
        if self._writer is None:
            use_tmp = self.filesystem is None
            if use_tmp:
                self.path.parent.mkdir(parents=True, exist_ok=True)
            else:
                # Best-effort: ask the filesystem to create the parent
                # directory. PyArrow's S3FileSystem accepts create_dir for
                # key-prefix "directories"; LocalFileSystem creates real
                # directories. Silently ignore errors so a non-creatable
                # filesystem (e.g. read-only) doesn't fail here.
                from pyarrow.fs import FileSelector  # noqa: F401
                path_str = str(self.path)
                parent = path_str.rsplit("/", 1)[0] if "/" in path_str else ""
                if parent:
                    try:
                        self.filesystem.create_dir(parent, recursive=True)
                    except Exception:
                        pass
            write_kwargs = {
                "where": self._tmp_path if use_tmp else str(self.path),
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
        if self._finalized:
            raise RuntimeError(
                f"ParquetPartitionWriter for {self.path} is closed; "
                "cannot write more chunks."
            )
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
        # Only attempt the rename once, on the happy path.
        if not self._finalized:
            self._finalize()
            _LOGGER.debug(
                "streaming_parquet.partition_closed",
                extra={"path": str(self.path), "rows": self.rows_written},
            )

    def _finalize(self) -> None:
        """Rename the .tmp file to the final path. Local filesystem only.

        Best-effort: if the rename fails (e.g. non-local FS, permissions),
        we log a warning and leave the .tmp in place. The caller may
        inspect ``self.rows_written`` and ``self.bytes_written`` to
        detect a partial write.
        """
        if self.filesystem is not None:
            # Non-local FS: rename semantics are not portable. Skip the
            # rename and leave the writer's output where it landed.
            # Future work could implement upload-then-promote via copy().
            _LOGGER.debug(
                "streaming_parquet.atomic_rename_skipped",
                extra={"path": str(self.path), "filesystem": type(self.filesystem).__name__},
            )
            self._finalized = True
            return
        import os
        tmp_path = self._tmp_path
        final_path = str(self.path)
        try:
            os.replace(tmp_path, final_path)
        except FileNotFoundError:
            # Nothing was written — close() called without any chunks.
            pass
        except OSError as exc:
            _LOGGER.warning(
                "streaming_parquet.atomic_rename_failed",
                extra={"tmp": tmp_path, "final": final_path, "error": str(exc)},
            )
            # Best-effort cleanup so the .tmp doesn't linger forever.
            try:
                os.remove(tmp_path)
            except OSError:
                pass
            return
        self._finalized = True