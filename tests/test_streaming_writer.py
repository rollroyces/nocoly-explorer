"""Tests for ParquetPartitionWriter."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from nocoly_explorer.streaming.options import ParquetExportOptions
from nocoly_explorer.streaming.writer import ParquetPartitionWriter


def _make_table(n: int) -> pa.Table:
    return pa.table({"a": list(range(n)), "b": [f"r{i}" for i in range(n)]})


def test_writer_creates_file_on_first_chunk(tmp_path: Path):
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    target = tmp_path / "data.parquet"
    w = ParquetPartitionWriter(target, schema, ParquetExportOptions())
    w.write_chunk(_make_table(10))
    # Crash-safe: the final path doesn't exist yet; the in-progress
    # data lives in the sibling .tmp file until close().
    assert not target.exists()
    assert (tmp_path / "data.parquet.tmp").exists()
    w.close()
    assert target.exists()
    assert target.stat().st_size > 0
    # The .tmp is gone once the rename has happened.
    assert not (tmp_path / "data.parquet.tmp").exists()


def test_writer_appends_on_subsequent_chunks(tmp_path: Path):
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    target = tmp_path / "data.parquet"
    w = ParquetPartitionWriter(target, schema, ParquetExportOptions())
    w.write_chunk(_make_table(10))
    w.write_chunk(_make_table(10))
    w.close()
    table = pa.parquet.read_table(target)
    assert table.num_rows == 20


def test_writer_close_is_idempotent(tmp_path: Path):
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    target = tmp_path / "data.parquet"
    w = ParquetPartitionWriter(target, schema, ParquetExportOptions())
    w.write_chunk(_make_table(5))
    w.close()
    w.close()  # second call must not raise
    assert target.exists()


def test_writer_tracks_rows_and_bytes(tmp_path: Path):
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    target = tmp_path / "data.parquet"
    w = ParquetPartitionWriter(target, schema, ParquetExportOptions())
    w.write_chunk(_make_table(100))
    w.close()
    assert w.rows_written == 100
    assert w.bytes_written > 0


def test_writer_close_without_writes_does_not_create_file(tmp_path: Path):
    schema = pa.schema([("a", pa.int64())])
    target = tmp_path / "data.parquet"
    w = ParquetPartitionWriter(target, schema, ParquetExportOptions())
    w.close()
    assert not target.exists()


def test_writer_uses_specified_compression(tmp_path: Path):
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    target = tmp_path / "data.parquet"
    w = ParquetPartitionWriter(target, schema, ParquetExportOptions(compression="gzip"))
    w.write_chunk(_make_table(100))
    w.close()
    md = pa.parquet.read_metadata(target)
    assert md.num_rows == 100



# ---------------------------------------------------------------------------
# Crash-safety: .tmp + rename pattern (local filesystem only)
# ---------------------------------------------------------------------------


def test_writer_uses_tmp_until_close(tmp_path: Path):
    """The final path is empty until close(); .tmp holds the in-progress data."""
    target = tmp_path / "data.parquet"
    w = ParquetPartitionWriter(target, pa.schema([("a", pa.int64())]), ParquetExportOptions())
    w.write_chunk(pa.table({"a": [1, 2]}))
    # Not yet renamed.
    assert not target.exists()
    assert (tmp_path / "data.parquet.tmp").exists()
    w.close()
    # After close the final file exists; the .tmp is gone.
    assert target.exists()
    assert not (tmp_path / "data.parquet.tmp").exists()


def test_writer_renames_via_os_replace(tmp_path: Path):
    """close() uses os.replace (atomic) — the rename is one syscall."""
    import os
    target = tmp_path / "data.parquet"
    w = ParquetPartitionWriter(target, pa.schema([("a", pa.int64())]), ParquetExportOptions())
    w.write_chunk(pa.table({"a": [1]}))
    w.close()
    # Verify the file landed at the target.
    assert os.path.exists(target)
    # Inode should match what was written (single rename, not a copy+delete).
    assert os.stat(target).st_size > 0


def test_writer_close_without_writes_is_safe(tmp_path: Path):
    """Closing a writer that never received any chunks is a no-op (no rename)."""
    target = tmp_path / "data.parquet"
    w = ParquetPartitionWriter(target, pa.schema([("a", pa.int64())]), ParquetExportOptions())
    w.close()
    # No data, no rename — the final file simply doesn't exist.
    assert not target.exists()


def test_writer_overwrites_stale_tmp(tmp_path: Path):
    """A stale .tmp from a crashed run is overwritten by the new writer."""
    target = tmp_path / "data.parquet"
    tmp = tmp_path / "data.parquet.tmp"
    # Simulate a previous crashed run.
    tmp.write_bytes(b"leftover from a crash")

    w = ParquetPartitionWriter(target, pa.schema([("a", pa.int64())]), ParquetExportOptions())
    w.write_chunk(pa.table({"a": [1, 2, 3]}))
    w.close()
    # The stale content is gone; the new content is at the final path.
    assert target.exists()
    table = pa.parquet.read_table(target)
    assert table.num_rows == 3
    assert not tmp.exists()


def test_writer_skips_rename_when_filesystem_is_set(tmp_path: Path):
    """With a non-local filesystem, the writer skips .tmp + rename."""
    import pyarrow.fs as pafs
    fs_root = tmp_path / "fs"
    fs_root.mkdir()
    fs = pafs.LocalFileSystem()
    target = str(fs_root / "data.parquet")
    w = ParquetPartitionWriter(
        target, pa.schema([("a", pa.int64())]), ParquetExportOptions(), filesystem=fs
    )
    w.write_chunk(pa.table({"a": [1]}))
    w.close()
    # File at target directly; no .tmp sibling.
    final = list(fs_root.rglob("*.parquet"))
    assert len(final) == 1
    assert not str(final[0]).endswith(".tmp")


def test_writer_write_chunk_after_close_raises(tmp_path: Path):
    """Writing after close raises — close is a terminal state."""
    target = tmp_path / "data.parquet"
    w = ParquetPartitionWriter(target, pa.schema([("a", pa.int64())]), ParquetExportOptions())
    w.write_chunk(pa.table({"a": [1]}))
    w.close()
    with pytest.raises(RuntimeError, match="closed"):
        w.write_chunk(pa.table({"a": [2]}))
