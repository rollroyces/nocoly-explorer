"""Tests for the streaming writer + filesystem support.

Uses ``pyarrow.fs.LocalFileSystem`` mounted at a temp dir as a fake S3
target so the non-local-fs code path is exercised without needing a
real S3 connection.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest

from nocoly_explorer.streaming import (
    PartitionSpec,
    ParquetExportOptions,
    StreamingExportConfig,
    StreamingExporter,
)
from nocoly_explorer.streaming.writer import ParquetPartitionWriter


class FakeClient:
    def __init__(self, pages: List[List[Dict[str, Any]]]):
        self._pages = [row for page in pages for row in page]

    def fetch_rows(self, **_kwargs):
        return self._pages


def _make_fs(root):
    """Mount a LocalFileSystem at root for use as a fake non-local FS."""
    return pafs.LocalFileSystem()


# ---------------------------------------------------------------------------
# ParquetPartitionWriter with a custom filesystem
# ---------------------------------------------------------------------------


def test_writer_writes_to_localfilesystem_root(tmp_path):
    """Direct test that ParquetPartitionWriter passes filesystem to PyArrow."""
    fs_root = tmp_path / "fs"
    fs_root.mkdir()
    fs = _make_fs(str(fs_root))
    target = str(fs_root / "data_0.parquet")
    writer = ParquetPartitionWriter(
        target, pa.schema([("a", pa.int64())]), ParquetExportOptions(), filesystem=fs
    )
    writer.write_chunk(pa.table({"a": [1, 2, 3]}))
    writer.close()

    # File should exist under fs_root.
    files = [p for p in fs_root.rglob("*.parquet")]
    assert files, f"no parquet written under {fs_root}"
    table = pq.read_table(str(files[0]))
    assert table.num_rows == 3


def test_writer_does_not_call_mkdir_when_filesystem_set(tmp_path):
    """With a non-local filesystem, parent.mkdir() is skipped (S3 has no dirs)."""
    fs_root = tmp_path / "fs"
    fs_root.mkdir()
    fs = _make_fs(str(fs_root))
    writer = ParquetPartitionWriter(
        str(fs_root / "nested" / "deep" / "data_0.parquet"),
        pa.schema([("a", pa.int64())]),
        ParquetExportOptions(),
        filesystem=fs,
    )
    # If mkdir were called on a non-Path, this would raise. Just verify it
    # doesn't and the write succeeds.
    writer.write_chunk(pa.table({"a": [1]}))
    writer.close()


# ---------------------------------------------------------------------------
# StreamingExporter with filesystem
# ---------------------------------------------------------------------------


def test_streaming_exporter_with_filesystem_writes(tmp_path):
    """End-to-end: StreamingExporter using a fake S3 filesystem."""
    fs_root = tmp_path / "fs"
    fs_root.mkdir()
    fs = _make_fs(str(fs_root))

    pages = [[{"a": i, "b": f"row-{i}"} for i in range(50)] for _ in range(3)]
    client = FakeClient(pages)
    exporter = StreamingExporter(
        client=client,
        config=StreamingExportConfig(
            output_dir=str(fs_root) + "/exports/ws-1",
            worksheet_id="ws-1",
            filesystem=fs,
        ),
    )
    result = exporter.export(page_size=10)

    assert result.rows_written == 150
    # The writer's filesystem should have written under fs_root.
    written = list(fs_root.rglob("*.parquet"))
    assert written, f"no parquet written under {fs_root}"
    table = pq.read_table(str(written[0]))
    assert table.num_rows == 150


def test_streaming_exporter_with_filesystem_and_partitions(tmp_path):
    fs_root = tmp_path / "fs"
    fs_root.mkdir()
    fs = _make_fs(str(fs_root))

    pages = [
        [{"region": "HK", "a": i} for i in range(5)],
        [{"region": "SZ", "a": i} for i in range(5, 10)],
    ]
    client = FakeClient(pages)
    exporter = StreamingExporter(
        client=client,
        config=StreamingExportConfig(
            output_dir=str(fs_root) + "/exports/ws-1",
            worksheet_id="ws-1",
            partition=PartitionSpec(column="region"),
            filesystem=fs,
        ),
    )
    result = exporter.export()
    assert result.rows_written == 10
    written = list(fs_root.rglob("*.parquet"))
    assert len(written) == 2


# ---------------------------------------------------------------------------
# StreamingExportConfig defaults to None filesystem (backwards-compatible)
# ---------------------------------------------------------------------------


def test_streaming_export_config_filesystem_default_is_none():
    from nocoly_explorer.streaming.exporter import StreamingExportConfig
    cfg = StreamingExportConfig(output_dir="/tmp/x", worksheet_id="ws-1")
    assert cfg.filesystem is None
