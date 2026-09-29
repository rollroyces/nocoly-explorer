"""Tests for StreamingExporter.stream_async — async write path."""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List

import pytest
import pyarrow.parquet as pq

from nocoly_explorer.streaming import (
    PartitionSpec,
    ParquetExportOptions,
    StreamingExportConfig,
    StreamingExporter,
)
from nocoly_explorer.streaming.exporter import _NoopClientAdapter


def _make_pages(total_rows: int, page_size: int = 50) -> List[List[Dict[str, Any]]]:
    pages: List[List[Dict[str, Any]]] = []
    page: List[Dict[str, Any]] = []
    for i in range(total_rows):
        if len(page) >= page_size:
            pages.append(page)
            page = []
        page.append({"a": i, "b": float(i)})
    if page:
        pages.append(page)
    return pages


class _FakeAsyncClient:
    """Stand-in for AsyncWorksheetClient: yields pages from a list."""

    def __init__(self, pages: List[List[Dict[str, Any]]], *, per_page_delay: float = 0.0):
        self._pages = pages
        self._delay = per_page_delay

    async def fetch_pages_async(
        self, *, page_size, max_pages, filter_criteria=None, cancel_check=None
    ):
        from nocoly_explorer.exceptions import JobCancelled
        for page in self._pages:
            if self._delay:
                await asyncio.sleep(self._delay)
            if cancel_check is not None and await cancel_check():
                raise JobCancelled("Cancellation requested (fake client)")
            yield page


# ---------------------------------------------------------------------------
# stream_async — basic correctness
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_async_writes_all_rows_to_parquet(tmp_path):
    pages = _make_pages(total_rows=250, page_size=50)
    client = _FakeAsyncClient(pages)
    exporter = StreamingExporter(
        client=_NoopClientAdapter(),
        config=StreamingExportConfig(
            output_dir=tmp_path, worksheet_id="ws-1",
        ),
    )
    result = await exporter.stream_async(client, page_size=50, max_pages=100)

    assert result.rows_written == 250
    out_file = tmp_path / "data_0.parquet"
    assert out_file.exists()
    table = pq.read_table(str(out_file))
    assert table.num_rows == 250
    assert sorted(table.column_names) == ["a", "b"]


@pytest.mark.asyncio
async def test_stream_async_does_not_use_constructor_client(tmp_path):
    """The constructor client is never called by stream_async."""

    class _ExplodingClient:
        def fetch_rows(self, **_kwargs):
            raise AssertionError(
                "stream_async should not call self.client.fetch_rows()"
            )

    client = _FakeAsyncClient(_make_pages(total_rows=10, page_size=5))
    exporter = StreamingExporter(
        client=_ExplodingClient(),
        config=StreamingExportConfig(output_dir=tmp_path, worksheet_id="ws-1"),
    )
    result = await exporter.stream_async(client, page_size=5, max_pages=10)
    assert result.rows_written == 10


@pytest.mark.asyncio
async def test_stream_async_partitions_writes_per_partition(tmp_path):
    pages: List[List[Dict[str, Any]]] = [
        [{"region": "HK", "a": i} for i in range(5)],
        [{"region": "SZ", "a": i} for i in range(5, 10)],
        [{"region": "HK", "a": i} for i in range(10, 15)],
    ]
    client = _FakeAsyncClient(pages)
    exporter = StreamingExporter(
        client=_NoopClientAdapter(),
        config=StreamingExportConfig(
            output_dir=tmp_path,
            worksheet_id="ws-1",
            partition=PartitionSpec(column="region"),
        ),
    )
    result = await exporter.stream_async(client, page_size=5, max_pages=10)

    assert result.rows_written == 15
    hk = tmp_path / "region=HK" / "data_0.parquet"
    sz = tmp_path / "region=SZ" / "data_0.parquet"
    assert hk.exists() and sz.exists()
    assert pq.read_table(str(hk)).num_rows == 10
    assert pq.read_table(str(sz)).num_rows == 5


# ---------------------------------------------------------------------------
# stream_async — cooperative cancellation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_async_cancel_check_raises_jobcancelled(tmp_path):
    """cancel_check returning True mid-stream raises JobCancelled."""
    from nocoly_explorer.exceptions import JobCancelled

    pages = _make_pages(total_rows=200, page_size=20)  # many pages
    client = _FakeAsyncClient(pages, per_page_delay=0.02)
    cancelled = asyncio.Event()
    cancelled.set()  # Already cancelled when stream starts.

    async def cancel_check() -> bool:
        return cancelled.is_set()

    exporter = StreamingExporter(
        client=_NoopClientAdapter(),
        config=StreamingExportConfig(output_dir=tmp_path, worksheet_id="ws-1"),
    )
    with pytest.raises(JobCancelled):
        await exporter.stream_async(
            client, page_size=20, max_pages=100, cancel_check=cancel_check
        )


@pytest.mark.asyncio
async def test_stream_async_cancel_mid_stream_partial_write(tmp_path):
    """Cancelling mid-stream stops writing; partial files are closed cleanly."""
    from nocoly_explorer.exceptions import JobCancelled

    pages = _make_pages(total_rows=200, page_size=20)
    client = _FakeAsyncClient(pages, per_page_delay=0.01)

    cancelled = [False]
    check_count = 0

    async def cancel_check() -> bool:
        nonlocal check_count
        check_count += 1
        # Flip to True once stream is in progress.
        if check_count >= 3:
            cancelled[0] = True
        return cancelled[0]

    exporter = StreamingExporter(
        client=_NoopClientAdapter(),
        config=StreamingExportConfig(output_dir=tmp_path, worksheet_id="ws-1"),
    )
    with pytest.raises(JobCancelled):
        await exporter.stream_async(
            client, page_size=20, max_pages=100, cancel_check=cancel_check
        )
    # Cancelled mid-flight — fewer than 200 rows written.
    out_file = tmp_path / "data_0.parquet"
    if out_file.exists():
        rows = pq.read_table(str(out_file)).num_rows
        assert rows < 200
    # cancel_check was actually consulted mid-stream.
    assert check_count >= 3


@pytest.mark.asyncio
async def test_stream_async_no_cancel_check_completes(tmp_path):
    pages = _make_pages(total_rows=100, page_size=25)
    client = _FakeAsyncClient(pages)
    exporter = StreamingExporter(
        client=_NoopClientAdapter(),
        config=StreamingExportConfig(output_dir=tmp_path, worksheet_id="ws-1"),
    )
    result = await exporter.stream_async(
        client, page_size=25, max_pages=10, cancel_check=None
    )
    assert result.rows_written == 100


# ---------------------------------------------------------------------------
# stream_async — Writer cleanup on error
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_async_closes_writers_on_exception(tmp_path):
    """If the client raises mid-stream, writers are still closed."""

    class _ExplodingAsyncClient:
        async def fetch_pages_async(self, **_kwargs):
            yield [{"a": 1}]
            raise RuntimeError("upstream blew up")

    exporter = StreamingExporter(
        client=_NoopClientAdapter(),
        config=StreamingExportConfig(output_dir=tmp_path, worksheet_id="ws-1"),
    )
    with pytest.raises(RuntimeError, match="upstream blew up"):
        await exporter.stream_async(_ExplodingAsyncClient(), page_size=10, max_pages=10)

    # The partial file was closed (no dangling handle). We can read it.
    out_file = tmp_path / "data_0.parquet"
    assert out_file.exists()
    table = pq.read_table(str(out_file))
    assert table.num_rows == 1


# ---------------------------------------------------------------------------
# ExportResult contents
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_async_result_reports_partitions_and_bytes(tmp_path):
    pages = _make_pages(total_rows=50, page_size=10)
    client = _FakeAsyncClient(pages)
    exporter = StreamingExporter(
        client=_NoopClientAdapter(),
        config=StreamingExportConfig(output_dir=tmp_path, worksheet_id="ws-1"),
    )
    result = await exporter.stream_async(client, page_size=10, max_pages=10)
    assert result.rows_written == 50
    assert result.output_dir == tmp_path
    assert result.bytes_written > 0
    # Unpartitioned write produces a single "__unpartitioned__" partition.
    assert "__unpartitioned__" in result.partitions
    assert result.partitions["__unpartitioned__"] == 50
