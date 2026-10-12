"""End-to-end integration tests for the WorksheetWriter / AsyncWorksheetWriter.

These spin up a real mock server and walk the full request lifecycle:
write a batch, check that the server received what we sent, and verify
the client's reported counters.

Goal: catch integration bugs that unit tests miss — wrong HTTP method
on the wire, malformed payload, headers missing or wrong, body
serialization issues that only show up end-to-end.

Implementation note
------------------
The async writer uses aiohttp; its test server runs in the same asyncio
loop the test runs in. The sync writer uses ``requests`` (blocking
I/O), which would block the same loop and prevent the aiohttp test
server from responding. So this file uses TWO mock servers:

- A thread-based HTTP server (``http.server.HTTPServer`` in a daemon
  thread) for sync-writer tests, since the sync client must not share
  the asyncio loop with the mock.
- An aiohttp ``TestServer`` for async-writer tests, since aiohttp
  needs an asyncio loop.
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import decimal
import json
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple

import pytest
from aiohttp import web

from nocoly_explorer import (
    AsyncWriteConfig,
    AsyncWorksheetWriter,
    WorksheetWriter,
    WriteResult,
)
from nocoly_explorer.auth import CredentialPair


# Tight timeout for every writer in this suite. Keeps test failures fast.
_TIMEOUT = 2.0


# ---------------------------------------------------------------------------
# Thread-based mock server for sync-writer tests
# ---------------------------------------------------------------------------


class _SyncMockState:
    """Shared state between the mock server thread and the test body."""

    def __init__(self) -> None:
        self.requests: List[Dict[str, Any]] = []
        self.status_per_request: List[int] = []


class _SyncMockNocolyHandler(BaseHTTPRequestHandler):
    """Stdlib HTTP handler that records every request and replays a status cycle."""

    state: _SyncMockState = None  # set per server via factory

    def log_message(self, format, *args):  # silence default stderr logging
        return

    def _read_body(self) -> str:
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length:
            return self.rfile.read(length).decode("utf-8")
        return ""

    def do_POST(self):  # noqa: N802 - stdlib convention
        body_str = self._read_body()
        try:
            body = json.loads(body_str) if body_str else {}
        except Exception:
            body = {"_raw": body_str}
        _SyncMockNocolyHandler.state.requests.append({
            "method": "POST",
            "path": self.path,
            "headers": {k: v for k, v in self.headers.items()},
            "body": body,
        })
        state = _SyncMockNocolyHandler.state
        if state.status_per_request:
            idx = len(state.requests) - 1
            status = state.status_per_request[idx % len(state.status_per_request)]
        else:
            status = 200
        response = json.dumps({"ok": True, "received": len(state.requests) - 1}).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)


def _start_sync_server() -> Tuple[_SyncMockState, str, ThreadingHTTPServer]:
    """Spin up a thread-based HTTP server. Returns state, base_url, server."""
    state = _SyncMockState()
    # Bind to a free port on localhost.
    server = ThreadingHTTPServer(("127.0.0.1", 0), _SyncMockNocolyHandler)
    server.timeout = _TIMEOUT + 1
    # Pin the state to the handler class for this server instance.
    _SyncMockNocolyHandler.state = state
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_address[1]
    base = f"http://127.0.0.1:{port}"
    return state, base, server


class _SyncServerHandle:
    """Context-manager-style holder for the thread server."""

    def __init__(self):
        self.state, self.base, self.server = _start_sync_server()

    def stop(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def sync_server():
    h = _SyncServerHandle()
    try:
        yield h
    finally:
        h.stop()


# ---------------------------------------------------------------------------
# Async mock server (aiohttp TestServer) for async-writer tests
# ---------------------------------------------------------------------------


class _AsyncMockState:
    def __init__(self) -> None:
        self.requests: List[Dict[str, Any]] = []
        self.status_per_request: List[int] = []


def _make_async_app(state: _AsyncMockState) -> web.Application:
    async def handler(request: web.Request) -> web.Response:
        idx = len(state.requests)
        body_bytes = await request.read()
        try:
            body = json.loads(body_bytes) if body_bytes else {}
        except Exception:
            body = {"_raw": body_bytes.decode("utf-8", "replace")}
        state.requests.append({
            "method": request.method,
            "path": request.path,
            "headers": dict(request.headers),
            "body": body,
        })
        if state.status_per_request:
            status = state.status_per_request[idx % len(state.status_per_request)]
        else:
            status = 200
        return web.json_response({"ok": True, "received": idx}, status=status)

    app = web.Application()
    app.router.add_route("*", "/api/v3/app/worksheets/{wid}/rows", handler)
    app.router.add_route("*", "/api/v3/app/worksheets/{wid}/rows/upsert", handler)
    app.router.add_route("*", "/api/v3/app/worksheets/{wid}/rows/delete", handler)
    return app


@pytest.fixture
async def async_server():
    from aiohttp.test_utils import TestServer
    state = _AsyncMockState()
    server = TestServer(_make_async_app(state))
    await server.start_server()
    base = f"http://localhost:{server.port}"
    try:
        yield state, base
    finally:
        await server.close()


# ===========================================================================
# SYNC WRITER TESTS — use the thread-based server
# ===========================================================================


def test_sync_writer_e2e_against_live_server(sync_server):
    h = sync_server
    h.state.requests = []  # ensure clean

    with WorksheetWriter(
        host=h.base,
        worksheet_id="ws_1",
        credentials=CredentialPair(app_key="k", app_sign="s"),
        max_retries=0,
        timeout_seconds=_TIMEOUT,
    ) as writer:
        result = writer.add_rows([
            {"id": 1, "name": "alice", "created_at": _dt.datetime(2025, 1, 15)},
            {"id": 2, "name": "bob",   "created_at": _dt.datetime(2025, 1, 16)},
        ])

    assert result.operation == "add"
    assert result.rows_attempted == 2
    assert result.rows_succeeded == 2
    assert result.batches_sent == 1
    assert result.batches_failed == 0

    assert len(h.state.requests) == 1
    req = h.state.requests[0]
    assert req["method"] == "POST"
    assert req["path"] == "/api/v3/app/worksheets/ws_1/rows"
    # ``BaseHTTPRequestHandler`` exposes headers through a mapping where keys
    # are title-cased. Compare case-insensitively.
    headers_lc = {k.lower(): v for k, v in req["headers"].items()}
    assert headers_lc["hap-appkey"] == "k"
    assert headers_lc["hap-sign"] == "s"
    assert req["body"]["rows"] == [
        {"id": 1, "name": "alice", "created_at": "2025-01-15T00:00:00"},
        {"id": 2, "name": "bob",   "created_at": "2025-01-16T00:00:00"},
    ]


def test_sync_writer_chunks_at_batch_size(sync_server):
    h = sync_server
    h.state.requests = []

    with WorksheetWriter(
        host=h.base,
        worksheet_id="ws_1",
        credentials=CredentialPair(app_key="k", app_sign="s"),
        max_retries=0,
        timeout_seconds=_TIMEOUT,
    ) as writer:
        result = writer.add_rows(
            [{"a": i} for i in range(7)],
            batch_size=3,
        )

    assert result.batches_sent == 3
    assert result.rows_succeeded == 7
    assert len(h.state.requests) == 3
    sizes = [3, 3, 1]
    for i, req in enumerate(h.state.requests):
        assert len(req["body"]["rows"]) == sizes[i]


def test_sync_writer_uses_custom_endpoints(sync_server):
    h = sync_server
    h.state.requests = []

    with WorksheetWriter(
        host=h.base,
        worksheet_id="ws_1",
        credentials=CredentialPair(app_key="k", app_sign="s"),
        max_retries=0,
        timeout_seconds=_TIMEOUT,
        add_endpoint="/api/v3/app/worksheets/{worksheet_id}/rows/create",
        delete_endpoint="/api/v3/app/worksheets/{worksheet_id}/rows/bulk-delete",
    ) as writer:
        writer.add_rows([{"a": 1}])
        writer.delete_rows(row_ids=[1, 2, 3])

    assert h.state.requests[0]["path"].endswith("/rows/create")
    assert h.state.requests[1]["path"].endswith("/rows/bulk-delete")


def test_sync_writer_pandas_dataframe_to_wire(sync_server):
    pd = pytest.importorskip("pandas")
    h = sync_server
    h.state.requests = []

    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["alice", "bob", "carol"],
        "created_at": pd.to_datetime(["2025-01-15", "2025-01-16", "2025-01-17"]),
        "active": [True, False, True],
    })

    with WorksheetWriter(
        host=h.base, worksheet_id="ws_1",
        credentials=CredentialPair(app_key="k", app_sign="s"),
        max_retries=0,
        timeout_seconds=_TIMEOUT,
    ) as writer:
        result = writer.add_rows(df)

    assert result.rows_succeeded == 3
    sent = h.state.requests[0]["body"]["rows"]
    assert sent[0]["id"] == 1
    assert sent[0]["name"] == "alice"
    assert sent[0]["active"] is True
    assert sent[0]["created_at"] == "2025-01-15T00:00:00"


def test_sync_writer_pyarrow_table_to_wire(sync_server):
    pa = pytest.importorskip("pyarrow")
    h = sync_server
    h.state.requests = []

    table = pa.table({
        "id": [1, 2],
        "value": pa.array([1.5, 2.5], type=pa.float64()),
        "name": ["alpha", "beta"],
    })

    with WorksheetWriter(
        host=h.base, worksheet_id="ws_1",
        credentials=CredentialPair(app_key="k", app_sign="s"),
        max_retries=0,
        timeout_seconds=_TIMEOUT,
    ) as writer:
        result = writer.add_rows(table)

    assert result.rows_succeeded == 2
    sent = h.state.requests[0]["body"]["rows"]
    assert sent[0] == {"id": 1, "value": 1.5, "name": "alpha"}
    assert sent[1] == {"id": 2, "value": 2.5, "name": "beta"}


def test_sync_writer_csv_file_to_wire(sync_server, tmp_path):
    h = sync_server
    h.state.requests = []
    import csv

    csv_path = tmp_path / "data.csv"
    with csv_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["id", "name"])
        w.writeheader()
        w.writerow({"id": 1, "name": "alice"})
        w.writerow({"id": 2, "name": "bob"})

    with WorksheetWriter(
        host=h.base, worksheet_id="ws_1",
        credentials=CredentialPair(app_key="k", app_sign="s"),
        max_retries=0,
        timeout_seconds=_TIMEOUT,
    ) as writer:
        result = writer.add_rows(str(csv_path))

    assert result.rows_succeeded == 2
    sent = h.state.requests[0]["body"]["rows"]
    assert sent[0] == {"id": 1, "name": "alice"}
    assert sent[1] == {"id": 2, "name": "bob"}


def test_sync_writer_retries_on_5xx(sync_server):
    h = sync_server
    h.state.requests = []
    h.state.status_per_request = [500, 200]

    with WorksheetWriter(
        host=h.base, worksheet_id="ws_1",
        credentials=CredentialPair(app_key="k", app_sign="s"),
        max_retries=2,
        max_wait_seconds=0.05,
        timeout_seconds=_TIMEOUT,
    ) as writer:
        result = writer.add_rows([{"a": 1}])

    # 2 server calls (1 failed, 1 succeeded)
    assert len(h.state.requests) == 2
    assert result.rows_succeeded == 1
    assert result.batches_sent == 1
    assert result.batches_failed == 0


def test_sync_writer_4xx_is_non_retriable(sync_server):
    h = sync_server
    h.state.requests = []
    h.state.status_per_request = [400, 400, 400]

    with WorksheetWriter(
        host=h.base, worksheet_id="ws_1",
        credentials=CredentialPair(app_key="k", app_sign="s"),
        max_retries=2,
        max_wait_seconds=0.05,
        timeout_seconds=_TIMEOUT,
    ) as writer:
        result = writer.add_rows(
            [{"a": 1}], fail_fast=False,
        )

    # Only 1 attempt — 4xx is not retried.
    assert len(h.state.requests) == 1
    assert result.batches_failed == 1
    assert result.rows_succeeded == 0
    assert len(result.failures) == 1
    assert result.failures[0].status_code == 400


def test_sync_writer_honors_retry_after_header(sync_server):
    """Server returns 429 with Retry-After, then 200 — should honor the wait."""
    h = sync_server
    h.state.requests = []
    # Use a custom server to set Retry-After header.
    request_times: List[float] = []

    import time
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    state_holder = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a, **k): return

        def do_POST(self):
            request_times.append(time.monotonic())
            if len(request_times) == 1:
                self.send_response(429)
                self.send_header("Retry-After", "0.3")
                self.send_header("Content-Length", "0")
                self.end_headers()
            else:
                body = b'{"ok":true}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

    srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    srv.timeout = _TIMEOUT + 2
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    try:
        with WorksheetWriter(
            host=f"http://127.0.0.1:{srv.server_address[1]}",
            worksheet_id="ws_1",
            credentials=CredentialPair(app_key="k", app_sign="s"),
            max_retries=1,
            max_wait_seconds=5.0,
            timeout_seconds=_TIMEOUT,
        ) as writer:
            result = writer.add_rows([{"a": 1}])
        assert result.rows_succeeded == 1
        assert len(request_times) == 2
        elapsed = request_times[1] - request_times[0]
        assert elapsed >= 0.25, f"Retry-After not honored: {elapsed:.3f}s"
    finally:
        srv.shutdown()
        srv.server_close()


def test_sync_writer_large_export_counters(sync_server):
    h = sync_server
    h.state.requests = []

    with WorksheetWriter(
        host=h.base, worksheet_id="ws_1",
        credentials=CredentialPair(app_key="k", app_sign="s"),
        max_retries=0,
        timeout_seconds=_TIMEOUT,
    ) as writer:
        result = writer.add_rows(
            [{"a": i} for i in range(10_000)],
            batch_size=100,
        )

    assert result.batches_sent == 100
    assert result.rows_attempted == 10_000
    assert result.rows_succeeded == 10_000
    assert result.batches_failed == 0
    assert len(h.state.requests) == 100
    assert len(h.state.requests[0]["body"]["rows"]) == 100
    assert len(h.state.requests[-1]["body"]["rows"]) == 100


def test_sync_writer_partial_failure_with_fail_fast_false(sync_server):
    h = sync_server
    h.state.requests = []
    h.state.status_per_request = [200, 500, 500, 500, 200]

    with WorksheetWriter(
        host=h.base, worksheet_id="ws_1",
        credentials=CredentialPair(app_key="k", app_sign="s"),
        max_retries=2,
        max_wait_seconds=0.05,
        timeout_seconds=_TIMEOUT,
    ) as writer:
        result = writer.add_rows(
            [{"a": 0}, {"a": 1}, {"a": 2}],
            batch_size=1,
            fail_fast=False,  # <-- this is the whole point of the test
        )

    assert result.batches_sent == 3
    assert result.batches_failed == 1
    assert result.rows_succeeded == 2
    assert len(result.failures) == 1
    assert result.failures[0].status_code == 500
    assert result.failures[0].batch_index == 1
    assert len(h.state.requests) == 5


# ===========================================================================
# ASYNC WRITER TESTS — use the aiohttp TestServer
# ===========================================================================


async def test_async_writer_e2e_against_live_server(async_server):
    state, base = async_server
    state.requests = []

    async with AsyncWorksheetWriter(
        base_url=base,
        auth_token="k:s",
        worksheet_id="ws_1",
        config=AsyncWriteConfig(concurrency=4, requests_per_second=200.0),
    ) as writer:
        result = await writer.add_rows([
            {"id": 1, "balance": decimal.Decimal("100.50")},
            {"id": 2, "guid": uuid.UUID("12345678-1234-5678-1234-567812345678")},
        ])

    assert result.rows_succeeded == 2
    assert len(state.requests) == 1
    sent_rows = state.requests[0]["body"]["rows"]
    assert sent_rows[0]["balance"] == "100.50"
    assert sent_rows[1]["guid"] == "12345678-1234-5678-1234-567812345678"


async def test_async_writer_upsert_payload_shape(async_server):
    state, base = async_server
    state.requests = []

    async with AsyncWorksheetWriter(
        base_url=base,
        auth_token="k:s",
        worksheet_id="ws_1",
        config=AsyncWriteConfig(requests_per_second=200.0),
    ) as writer:
        result = await writer.upsert_rows(
            [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
            key_column="id",
        )

    assert result.rows_succeeded == 2
    body = state.requests[0]["body"]
    assert body["keyColumn"] == "id"
    assert body["rows"] == [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    assert state.requests[0]["path"].endswith("/rows/upsert")
    assert state.requests[0]["method"] == "POST"


async def test_async_writer_delete_by_filter_sends_single_batch(async_server):
    state, base = async_server
    state.requests = []

    async with AsyncWorksheetWriter(
        base_url=base,
        auth_token="k:s",
        worksheet_id="ws_1",
        config=AsyncWriteConfig(requests_per_second=200.0),
    ) as writer:
        result = await writer.delete_rows(
            filter_criteria={
                "type": "group", "logic": "AND",
                "filters": [{"type": "condition", "field": "status",
                             "operator": "EQ", "value": "inactive"}],
            }
        )

    assert len(state.requests) == 1
    body = state.requests[0]["body"]
    assert "filter" in body
    assert body["filter"]["logic"] == "AND"
    assert state.requests[0]["path"].endswith("/rows/delete")


async def test_async_writer_retries_on_5xx(async_server):
    state, base = async_server
    state.requests = []
    state.status_per_request = [500, 200]

    async with AsyncWorksheetWriter(
        base_url=base,
        auth_token="k:s",
        worksheet_id="ws_1",
        config=AsyncWriteConfig(requests_per_second=200.0),
    ) as writer:
        result = await writer.add_rows([{"a": 1}])

    assert result.rows_succeeded == 1
    assert len(state.requests) == 2


async def test_async_writer_concurrent_batches(async_server):
    state, base = async_server
    state.requests = []

    async with AsyncWorksheetWriter(
        base_url=base,
        auth_token="k:s",
        worksheet_id="ws_1",
        config=AsyncWriteConfig(concurrency=4, requests_per_second=200.0),
    ) as writer:
        result = await writer.add_rows(
            [{"a": i} for i in range(20)],
            batch_size=5,
        )

    assert result.batches_sent == 4
    assert result.rows_succeeded == 20
    assert len(state.requests) == 4
    for req in state.requests:
        assert len(req["body"]["rows"]) == 5


async def test_async_writer_cancellation_short_circuits(async_server):
    state, base = async_server
    state.requests = []

    async with AsyncWorksheetWriter(
        base_url=base,
        auth_token="k:s",
        worksheet_id="ws_1",
        config=AsyncWriteConfig(concurrency=1, requests_per_second=200.0),
    ) as writer:
        async def cancel_check() -> bool:
            return True
        result = await writer.add_rows(
            [{"a": i} for i in range(10)],
            batch_size=1,
            cancel_check=cancel_check,
            fail_fast=False,
        )

    assert len(state.requests) == 0
    assert result.rows_attempted == 10
    assert result.rows_succeeded == 0


async def test_async_writer_pandas_dataframe_to_wire(async_server):
    """End-to-end: pandas DataFrame → aiohttp server sees clean JSON."""
    pd = pytest.importorskip("pandas")
    state, base = async_server
    state.requests = []

    df = pd.DataFrame({
        "id": [1, 2],
        "name": ["alice", "bob"],
        "created_at": pd.to_datetime(["2025-01-15", "2025-01-16"]),
    })

    async with AsyncWorksheetWriter(
        base_url=base, auth_token="k:s", worksheet_id="ws_1",
        config=AsyncWriteConfig(requests_per_second=200.0),
    ) as writer:
        result = await writer.add_rows(df)

    assert result.rows_succeeded == 2
    sent = state.requests[0]["body"]["rows"]
    assert sent[0] == {"id": 1, "name": "alice", "created_at": "2025-01-15T00:00:00"}


# ---------------------------------------------------------------------------
# Mixed sync+async on the same Nocoly host (use thread server for sync, then
# async aiohttp for async). Two separate server instances here is fine — the
# test verifies the *behavior* of running both writers back-to-back, not that
# they share a single server.
# ---------------------------------------------------------------------------


def test_sync_and_async_against_separate_servers(sync_server):
    """Sync then async, against two independent mock servers (synchronous only)."""
    h_sync = sync_server
    h_sync.state.requests = []

    with WorksheetWriter(
        host=h_sync.base, worksheet_id="ws_1",
        credentials=CredentialPair(app_key="k", app_sign="s"),
        max_retries=0,
        timeout_seconds=_TIMEOUT,
    ) as sync:
        sync.add_rows([{"src": "sync"}])
        # The async writer requires its own event loop. We exercise it via
        # asyncio.run on the same machine (different event loop). Note that
        # this intentionally does NOT mix aiohttp TestServer + asyncio.run
        # in the same test — that pattern exhausts aiohttp's startup deadline
        # and is tested separately as ``test_async_writer_*`` tests against
        # the aiohttp-based ``async_server`` fixture.
        async def _run_async(base: str) -> None:
            async with AsyncWorksheetWriter(
                base_url=base, auth_token="k:s", worksheet_id="ws_1",
                config=AsyncWriteConfig(requests_per_second=200.0),
            ) as w:
                await w.add_rows([{"src": "async"}])

        # Spin up a second, dedicated thread-based server for the async call
        # (since the async client's event loop blocks the aiohttp TestServer
        # in the same loop). Reuse the helper.
        h_async = _SyncServerHandle()
        try:
            asyncio.run(_run_async(h_async.base))
        finally:
            h_async.stop()

    assert h_sync.state.requests[0]["body"]["rows"] == [{"src": "sync"}]
    assert h_async.state.requests[0]["body"]["rows"] == [{"src": "async"}]