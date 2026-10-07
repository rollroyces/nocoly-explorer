"""Tests for the asynchronous AsyncWorksheetWriter."""

from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List, Optional

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from nocoly_explorer.async_client import RetryPolicy
from nocoly_explorer.async_writer import AsyncWriteConfig, AsyncWorksheetWriter
from nocoly_explorer.exceptions import (
    NocolyWriteError,
    WriteBatchError,
    WriteValidationError,
)


# ---------------------------------------------------------------------------
# Mock HTTP server backed by aiohttp.web
# ---------------------------------------------------------------------------


def _make_aiohttp_app(
    *,
    statuses_per_request: List[int],
    json_per_request: Optional[List[Any]] = None,
    request_log: Optional[List[Dict[str, Any]]] = None,
    sleep_seconds: float = 0.0,
):
    """Build an aiohttp.web.Application that replays a canned response per
    incoming HTTP request, recording the request bodies.

    ``statuses_per_request`` and ``json_per_request`` are consumed in
    order. When the list is exhausted, the server returns 200 / {}.
    """
    call_index = {"i": 0}

    async def handler(request: web.Request) -> web.Response:
        idx = call_index["i"]
        call_index["i"] += 1
        body_bytes = await request.read()
        try:
            body = json.loads(body_bytes) if body_bytes else {}
        except Exception:
            body = {"_raw": body_bytes.decode("utf-8", "replace")}
        if request_log is not None:
            request_log.append({
                "method": request.method,
                "path": request.path,
                "body": body,
                "headers": dict(request.headers),
            })
        if sleep_seconds:
            await asyncio.sleep(sleep_seconds)
        status = (
            statuses_per_request[idx]
            if idx < len(statuses_per_request)
            else 200
        )
        if json_per_request is not None and idx < len(json_per_request):
            payload = json_per_request[idx]
        else:
            payload = {"call": idx}
        return web.json_response(payload, status=status)

    app = web.Application()
    app.router.add_route("*", "/api/v3/app/worksheets/{wid}/rows", handler)
    app.router.add_route("*", "/api/v3/app/worksheets/{wid}/rows/upsert", handler)
    app.router.add_route("*", "/api/v3/app/worksheets/{wid}/rows/delete", handler)
    return app


def _build_writer(
    base_url: str,
    *,
    config: Optional[AsyncWriteConfig] = None,
    batch_size: int = 500,
) -> AsyncWorksheetWriter:
    """Construct an AsyncWorksheetWriter against the mock URL."""
    cfg = config or AsyncWriteConfig(
        concurrency=4,
        requests_per_second=100.0,  # effectively no throttle in tests
    )
    return AsyncWorksheetWriter(
        base_url=base_url,
        auth_token="k:s",
        worksheet_id="ws_1",
        batch_size=batch_size,
        config=cfg,
    )


# ---------------------------------------------------------------------------
# Constructor validation
# ---------------------------------------------------------------------------


def test_constructor_rejects_zero_batch_size():
    with pytest.raises(ValueError):
        AsyncWorksheetWriter(
            base_url="https://example.com",
            auth_token="k:s",
            worksheet_id="ws_1",
            batch_size=0,
        )


def test_constructor_rejects_negative_concurrency():
    with pytest.raises(ValueError):
        AsyncWorksheetWriter(
            base_url="https://example.com",
            auth_token="k:s",
            worksheet_id="ws_1",
            config=AsyncWriteConfig(concurrency=0, requests_per_second=10.0),
        )


def test_constructor_rejects_zero_rate():
    with pytest.raises(ValueError):
        AsyncWorksheetWriter(
            base_url="https://example.com",
            auth_token="k:s",
            worksheet_id="ws_1",
            config=AsyncWriteConfig(concurrency=2, requests_per_second=0),
        )


# ---------------------------------------------------------------------------
# Basic operation: add_rows
# ---------------------------------------------------------------------------


async def test_add_rows_sends_all_batches():
    request_log: List[Dict[str, Any]] = []
    app = _make_aiohttp_app(
        statuses_per_request=[200, 200, 200],
        json_per_request=[{"ok": 1}, {"ok": 2}, {"ok": 3}],
        request_log=request_log,
    )

    async with TestServer(app) as server:
        writer = _build_writer(
            f"http://localhost:{server.port}",
            batch_size=2,
        )
        async with writer:
            result = await writer.add_rows(
                [{"a": i} for i in range(5)],
            )

    assert result.batches_sent == 3
    assert result.rows_succeeded == 5
    assert result.batches_failed == 0
    # Verify the *multiset* of batch sizes is {2, 2, 1}; order is not
    # guaranteed because batches are dispatched concurrently.
    batch_sizes = sorted(len(c["body"]["rows"]) for c in request_log)
    assert batch_sizes == [1, 2, 2]
    # POST method, default endpoint
    assert all(c["method"] == "POST" for c in request_log)
    assert all(c["path"].endswith("/rows") for c in request_log)
    # HAP headers (aiohttp stores header names lowercased; the dispatcher
    # itself uses the canonical mixed-case form when sending).
    for c in request_log:
        lowered = {k.lower(): v for k, v in c["headers"].items()}
        assert lowered.get("hap-appkey") == "k"
        assert lowered.get("hap-sign") == "s"


async def test_add_rows_empty_input_is_noop():
    writer = _build_writer("http://localhost:0")
    async with writer:
        result = await writer.add_rows([])
    assert result.batches_sent == 0
    assert result.rows_attempted == 0


async def test_add_rows_handles_list_of_dicts_input():
    request_log: List[Dict[str, Any]] = []
    app = _make_aiohttp_app(
        statuses_per_request=[200],
        request_log=request_log,
    )
    async with TestServer(app) as server:
        writer = _build_writer(f"http://localhost:{server.port}")
        async with writer:
            await writer.add_rows([{"a": 1, "b": 2}, {"a": 3, "b": 4}])

    assert request_log[0]["body"] == {"rows": [{"a": 1, "b": 2}, {"a": 3, "b": 4}]}


async def test_add_rows_handles_pandas_dataframe():
    pd = pytest.importorskip("pandas")
    request_log: List[Dict[str, Any]] = []
    app = _make_aiohttp_app(statuses_per_request=[200], request_log=request_log)
    async with TestServer(app) as server:
        writer = _build_writer(f"http://localhost:{server.port}")
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        async with writer:
            await writer.add_rows(df)

    assert request_log[0]["body"]["rows"] == [
        {"a": 1, "b": 3},
        {"a": 2, "b": 4},
    ]


async def test_add_rows_handles_pyarrow_table():
    pa = pytest.importorskip("pyarrow")
    request_log: List[Dict[str, Any]] = []
    app = _make_aiohttp_app(statuses_per_request=[200], request_log=request_log)
    async with TestServer(app) as server:
        writer = _build_writer(f"http://localhost:{server.port}")
        table = pa.table({"a": [1, 2]})
        async with writer:
            await writer.add_rows(table)

    assert request_log[0]["body"]["rows"] == [{"a": 1}, {"a": 2}]


async def test_add_rows_handles_csv_file(tmp_path):
    request_log: List[Dict[str, Any]] = []
    app = _make_aiohttp_app(statuses_per_request=[200], request_log=request_log)
    csv_path = tmp_path / "rows.csv"
    with csv_path.open("w") as f:
        f.write("name,score\nalice,30\nbob,25\n")

    async with TestServer(app) as server:
        writer = _build_writer(f"http://localhost:{server.port}")
        async with writer:
            await writer.add_rows(str(csv_path))

    assert request_log[0]["body"]["rows"] == [
        {"name": "alice", "score": 30},
        {"name": "bob", "score": 25},
    ]


# ---------------------------------------------------------------------------
# update_rows / upsert_rows
# ---------------------------------------------------------------------------


async def test_update_rows_requires_key_column():
    writer = _build_writer("http://localhost:0")
    async with writer:
        with pytest.raises(WriteValidationError):
            await writer.update_rows([{"id": 1}], key_column="")


async def test_update_rows_rejects_missing_key():
    writer = _build_writer("http://localhost:0")
    async with writer:
        with pytest.raises(WriteValidationError):
            await writer.update_rows(
                [{"id": 1}, {"no_id": "x"}],
                key_column="id",
            )


async def test_update_rows_sends_put_with_key_column():
    request_log: List[Dict[str, Any]] = []
    app = _make_aiohttp_app(statuses_per_request=[200], request_log=request_log)
    async with TestServer(app) as server:
        writer = _build_writer(f"http://localhost:{server.port}")
        async with writer:
            await writer.update_rows(
                [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
                key_column="id",
            )

    assert request_log[0]["method"] == "PUT"
    assert request_log[0]["body"] == {
        "rows": [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
        "keyColumn": "id",
    }


async def test_upsert_rows_sends_post_to_upsert_endpoint():
    request_log: List[Dict[str, Any]] = []
    app = _make_aiohttp_app(statuses_per_request=[200], request_log=request_log)
    async with TestServer(app) as server:
        writer = _build_writer(f"http://localhost:{server.port}")
        async with writer:
            await writer.upsert_rows(
                [{"id": 1, "name": "alice"}],
                key_column="id",
            )

    assert request_log[0]["method"] == "POST"
    assert request_log[0]["path"].endswith("/rows/upsert")
    assert request_log[0]["body"]["keyColumn"] == "id"


# ---------------------------------------------------------------------------
# delete_rows
# ---------------------------------------------------------------------------


async def test_delete_rows_requires_exactly_one_input():
    writer = _build_writer("http://localhost:0")
    async with writer:
        with pytest.raises(WriteValidationError):
            await writer.delete_rows()
        with pytest.raises(WriteValidationError):
            await writer.delete_rows(row_ids=[1], filter_criteria={"x": 1})


async def test_delete_rows_rejects_empty_ids():
    writer = _build_writer("http://localhost:0")
    async with writer:
        with pytest.raises(WriteValidationError):
            await writer.delete_rows(row_ids=[])


async def test_delete_rows_by_id_chunks():
    request_log: List[Dict[str, Any]] = []
    app = _make_aiohttp_app(
        statuses_per_request=[200, 200],
        request_log=request_log,
    )
    async with TestServer(app) as server:
        # Force concurrency=1 so batch order is deterministic.
        cfg = AsyncWriteConfig(concurrency=1, requests_per_second=100.0)
        writer = _build_writer(f"http://localhost:{server.port}", config=cfg, batch_size=2)
        async with writer:
            await writer.delete_rows(row_ids=[1, 2, 3], batch_size=2)

    assert [c["body"] for c in request_log] == [
        {"rowIds": [1, 2]},
        {"rowIds": [3]},
    ]


async def test_delete_rows_by_filter_sends_single_batch():
    request_log: List[Dict[str, Any]] = []
    app = _make_aiohttp_app(statuses_per_request=[200], request_log=request_log)
    async with TestServer(app) as server:
        writer = _build_writer(f"http://localhost:{server.port}")
        async with writer:
            await writer.delete_rows(
                filter_criteria={"type": "group", "logic": "AND", "filters": []}
            )

    assert request_log[0]["body"] == {
        "filter": {"type": "group", "logic": "AND", "filters": []}
    }


# ---------------------------------------------------------------------------
# Failure paths
# ---------------------------------------------------------------------------


async def test_add_rows_raises_on_first_failure():
    app = _make_aiohttp_app(
        statuses_per_request=[500, 500, 500],
        request_log=[],
    )
    cfg = AsyncWriteConfig(
        concurrency=2,
        requests_per_second=100.0,
        retry_policy=RetryPolicy(
            max_retries=1, base_delay=0.01, max_delay=0.05, max_wait_seconds=0.05
        ),
    )
    async with TestServer(app) as server:
        writer = _build_writer(f"http://localhost:{server.port}", config=cfg, batch_size=1)
        async with writer:
            with pytest.raises(WriteBatchError) as excinfo:
                await writer.add_rows([{"a": 1}, {"a": 2}])

    assert len(excinfo.value.failures) >= 1


async def test_add_rows_fail_false_collects_failures():
    app = _make_aiohttp_app(
        statuses_per_request=[200, 400, 200],
        request_log=[],
    )
    cfg = AsyncWriteConfig(
        concurrency=1,
        requests_per_second=100.0,
        retry_policy=RetryPolicy(max_retries=0),
    )
    async with TestServer(app) as server:
        writer = _build_writer(f"http://localhost:{server.port}", config=cfg, batch_size=1)
        async with writer:
            result = await writer.add_rows(
                [{"a": 1}, {"a": 2}, {"a": 3}],
                fail_fast=False,
            )

    assert result.batches_sent == 3
    assert result.batches_failed == 1
    assert result.rows_succeeded == 2
    assert len(result.failures) == 1
    assert result.failures[0].status_code == 400


async def test_add_rows_raises_when_not_entered():
    writer = _build_writer("http://localhost:0")
    with pytest.raises(NocolyWriteError):
        await writer.add_rows([{"a": 1}])


# ---------------------------------------------------------------------------
# Endpoint overrides
# ---------------------------------------------------------------------------


async def test_add_rows_uses_custom_endpoint():
    request_log: List[Dict[str, Any]] = []
    app = web.Application()

    async def add_handler(request: web.Request) -> web.Response:
        body = await request.read()
        request_log.append({
            "method": request.method,
            "path": request.path,
            "body": json.loads(body) if body else {},
        })
        return web.json_response({"ok": True}, status=200)

    app.router.add_route("*", "/custom/add", add_handler)

    async with TestServer(app) as server:
        writer = AsyncWorksheetWriter(
            base_url=f"http://localhost:{server.port}",
            auth_token="k:s",
            worksheet_id="ws_1",
            batch_size=10,
            config=AsyncWriteConfig(requests_per_second=100.0),
            add_endpoint="/custom/add",
        )
        async with writer:
            await writer.add_rows([{"x": 1}])

    assert request_log[0]["path"] == "/custom/add"


# ---------------------------------------------------------------------------
# Cancellation
# ---------------------------------------------------------------------------


async def test_add_rows_supports_cancel_check():
    app = _make_aiohttp_app(
        statuses_per_request=[200] * 20,
        request_log=[],
        sleep_seconds=0.05,
    )
    cfg = AsyncWriteConfig(concurrency=2, requests_per_second=100.0)
    cancel_flag = {"stop": False}

    async def cancel_check() -> bool:
        return cancel_flag["stop"]

    async with TestServer(app) as server:
        writer = _build_writer(f"http://localhost:{server.port}", config=cfg, batch_size=1)
        async with writer:
            cancel_flag["stop"] = True  # cancel immediately
            result = await writer.add_rows(
                [{"a": i} for i in range(10)],
                cancel_check=cancel_check,
                fail_fast=False,
            )

    # We cancelled before most batches could complete; no crashes is the win.
    assert result.operation == "add"
    assert result.rows_attempted == 10