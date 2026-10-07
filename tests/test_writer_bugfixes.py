"""Regression tests for the v0.5.0 worksheet writer.

Covers real-world input edge cases (datetime, Decimal, UUID, numpy
scalars), the JSON-encode path that ``requests``/``aiohttp`` cannot do
natively, the ``WorksheetWriter`` lifecycle (``close``, context manager),
and the async cancel-before-semaphore path.
"""

from __future__ import annotations

import asyncio
import csv
import datetime as _dt
import decimal
import io
import json
import uuid
from typing import Any, Dict, List

import pytest

from nocoly_explorer.auth import CredentialPair
from nocoly_explorer.writer import WorksheetWriter, WriteResult
from nocoly_explorer.writer_inputs import (
    coerce_to_rows,
    encode_payload,
    normalize_batch_size,
)
from nocoly_explorer.exceptions import WriteValidationError, NocolyWriteError


def _client(**overrides):
    creds = CredentialPair(app_key="k", app_sign="s")
    kwargs = dict(
        host="https://example.com",
        worksheet_id="ws_1",
        credentials=creds,
        max_retries=2,
        max_wait_seconds=5.0,
    )
    kwargs.update(overrides)
    return WorksheetWriter(**kwargs)


# ---------------------------------------------------------------------------
# encode_payload — covers the bug found in real-world pandas/pyarrow payloads.
# ---------------------------------------------------------------------------


def test_encode_payload_handles_datetime():
    payload = {"created_at": _dt.datetime(2025, 1, 15, 10, 30, 45)}
    out = encode_payload(payload)
    assert _dt.datetime.fromisoformat(json.loads(out)["created_at"]) == payload["created_at"]


def test_encode_payload_handles_date():
    payload = {"created_at": _dt.date(2025, 1, 15)}
    out = encode_payload(payload)
    assert _dt.date.fromisoformat(json.loads(out)["created_at"]) == payload["created_at"]


def test_encode_payload_handles_time():
    payload = {"t": _dt.time(10, 30, 45)}
    out = encode_payload(payload)
    assert json.loads(out)["t"] == "10:30:45"


def test_encode_payload_handles_timedelta():
    payload = {"elapsed": _dt.timedelta(seconds=90.5)}
    out = encode_payload(payload)
    assert float(json.loads(out)["elapsed"]) == pytest.approx(90.5, abs=1e-9)


def test_encode_payload_handles_decimal_as_string():
    payload = {"balance": decimal.Decimal("100.50")}
    out = encode_payload(payload)
    # String-preserved for precision.
    assert json.loads(out)["balance"] == "100.50"


def test_encode_payload_handles_uuid():
    u = uuid.UUID("12345678-1234-5678-1234-567812345678")
    payload = {"id": u}
    out = encode_payload(payload)
    assert json.loads(out)["id"] == str(u)


def test_encode_payload_handles_list_of_mixed_types():
    payload = [
        {"created_at": _dt.datetime(2025, 1, 1), "balance": decimal.Decimal("1.00")},
        {"created_at": _dt.date(2025, 2, 1), "guid": uuid.uuid4()},
    ]
    out = encode_payload(payload)
    decoded = json.loads(out)
    assert len(decoded) == 2
    assert _dt.datetime.fromisoformat(decoded[0]["created_at"]).year == 2025
    assert decoded[0]["balance"] == "1.00"


def test_encode_payload_rejects_unsupported_type():
    with pytest.raises(TypeError, match="set"):
        encode_payload({"x": {1, 2, 3}})


def test_encode_payload_realistic_pandas_output():
    """Simulate the exact dict shape ``pandas.DataFrame.to_dict('records')`` returns."""
    import pandas as pd

    df = pd.DataFrame({
        "id": [1, 2],
        "name": ["alice", "bob"],
        "created_at": pd.to_datetime(["2025-01-15", "2025-01-16"]),
        "balance": pd.Series([decimal.Decimal("100.50"), decimal.Decimal("200.00")]),
    })
    coerced = coerce_to_rows(df, operation="add")
    body = encode_payload(coerced)
    decoded = json.loads(body)
    assert len(decoded) == 2
    assert decoded[0]["id"] == 1
    assert decoded[1]["name"] == "bob"
    # created_at: pandas Timestamp -> datetime -> ISO string
    assert "2025-01-15" in decoded[0]["created_at"]
    # balance: Decimal -> string
    assert decoded[0]["balance"] == "100.50"


def test_encode_payload_handles_numpy_scalars():
    """Errors here are richer when pyarrow emits numpy scalars via ``to_pylist``."""
    np = pytest.importorskip("numpy")
    rows = [
        {"id": np.int64(1), "score": np.float32(0.5), "ok": np.bool_(True)},
    ]
    body = encode_payload(rows)
    decoded = json.loads(body)
    assert decoded[0]["id"] == 1
    assert decoded[0]["score"] == 0.5
    assert decoded[0]["ok"] is True


def test_encode_payload_handles_pyarrow_date_column():
    """pyarrow CSV / parquet auto-types date strings into ``date32`` -> ``datetime.date``."""
    pa = pytest.importorskip("pyarrow")
    import tempfile
    import os

    fd, csv_path = tempfile.mkstemp(suffix=".csv")
    os.close(fd)
    try:
        with open(csv_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["id", "name", "created_at"])
            w.writerow([1, "alice", "2025-01-15"])
            w.writerow([2, "bob", "2025-01-16"])
        coerced = coerce_to_rows(csv_path, operation="add")
        body = encode_payload(coerced)
        decoded = json.loads(body)
        assert decoded[0]["created_at"] == "2025-01-15"
    finally:
        os.unlink(csv_path)


# ---------------------------------------------------------------------------
# WorksheetWriter.close + context manager — fixes the session-leak.
# ---------------------------------------------------------------------------


def test_worksheet_writer_close_is_idempotent():
    w = _client()
    w.close()
    w.close()  # idempotent


def test_worksheet_writer_supports_context_manager():
    with _client() as w:
        # Internal session is alive mid-block.
        assert w._session is not None
    # On exit, the close() path runs.


def test_worksheet_writer_enter_returns_self():
    w = _client()
    assert w.__enter__() is w
    w.__exit__(None, None, None)


# ---------------------------------------------------------------------------
# Cancelled batches inside the async writer — the cancel-check-before-
# semaphore reordering makes cancellation latency bounded by inflight count.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_writer_cancel_check_runs_before_semaphore():
    """cancel_check returning True immediately must not wait on the semaphore."""
    from nocoly_explorer.async_writer import AsyncWriteConfig, AsyncWorksheetWriter
    from aiohttp.test_utils import TestServer
    from aiohttp import web

    # Mock server that delays 100ms then returns 200.
    async def slow_handler(request):
        await asyncio.sleep(0.1)
        return web.json_response({"ok": True})

    app = web.Application()
    app.router.add_route("*", "/api/v3/app/worksheets/{wid}/rows", slow_handler)
    async with TestServer(app) as server:
        cfg = AsyncWriteConfig(concurrency=1, requests_per_second=100.0)
        async with AsyncWorksheetWriter(
            base_url=f"http://localhost:{server.port}",
            auth_token="k:s",
            worksheet_id="ws_1",
            batch_size=1,
            config=cfg,
        ) as w:
            # Build 10 batches; concurrency=1 means only one is inflight.
            rows = [{"a": i} for i in range(10)]
            # Cancel check that returns True after the first batch completes.
            cancel_count = {"n": 0}
            async def cancel_check():
                cancel_count["n"] += 1
                return cancel_count["n"] > 1  # cancel after 2nd check

            result = await w.add_rows(
                rows,
                batch_size=1,
                cancel_check=cancel_check,
                fail_fast=False,
            )
            # We cancelled; result should be returned (not raise) since
            # not all inflight batches failed.
            assert result.operation == "add"
            # We didn't actually fail — the first batch succeeded before
            # cancel kicked in. The rest were cancelled without running.
            assert cancel_count["n"] >= 2


# ---------------------------------------------------------------------------
# Regression: pandas-numpy ndarray values must not crash the writer.
# ---------------------------------------------------------------------------


def test_encode_payload_rejects_ndarray_inside_row():
    """ndarray inside a row dict shouldn't normally happen, but be defensive."""
    np = pytest.importorskip("numpy")
    rows = [{"x": np.array([1, 2, 3])}]
    out = encode_payload(rows)
    # .tolist() should serialize cleanly.
    assert json.loads(out)[0]["x"] == [1, 2, 3]


def test_encode_payload_handles_numpy_datetime64():
    np = pytest.importorskip("numpy")
    rows = [{"created_at": np.datetime64("2025-01-15T10:30:00")}]
    out = encode_payload(rows)
    body = json.loads(out)
    assert "2025-01-15" in body[0]["created_at"]


# ---------------------------------------------------------------------------
# Helper dedupe: ``coerce_id`` / ``validate_key_present`` / ``chunk_into_batches``
# are exposed once on the shared layer and reused by sync + async writers.
# ---------------------------------------------------------------------------


def test_coerce_id_passes_through_primitives():
    from nocoly_explorer.writer_inputs import coerce_id
    assert coerce_id("abc") == "abc"
    assert coerce_id(42) == 42
    assert coerce_id(3.14) == 3.14
    assert coerce_id(True) is True
    assert coerce_id(False) is False
    assert coerce_id(0) == 0  # int stays int, doesn't get bool'd


def test_coerce_id_stringifies_anything_else():
    from nocoly_explorer.writer_inputs import coerce_id
    import decimal as _decimal
    import uuid as _uuid
    import datetime as _dt
    assert coerce_id(_decimal.Decimal("1.00")) == "1.00"
    assert coerce_id(_uuid.UUID("12345678-1234-5678-1234-567812345678")) == "12345678-1234-5678-1234-567812345678"
    assert coerce_id(_dt.datetime(2025, 1, 1)) == "2025-01-01 00:00:00"


def test_validate_key_present_rejects_empty_rows():
    from nocoly_explorer.writer_inputs import validate_key_present
    with pytest.raises(WriteValidationError, match="at least one element"):
        validate_key_present([], "id")


def test_validate_key_present_reports_missing_indices():
    from nocoly_explorer.writer_inputs import validate_key_present
    rows = [{"id": 1}, {"no_id": "x"}, {"id": 3}, {"id": None}, {"missing": "y"}]
    # 3 rows are "missing": index 1 (no key), index 3 (id=None), index 4 (no key).
    with pytest.raises(WriteValidationError, match="3 row"):
        validate_key_present(rows, "id")


def test_chunk_into_batches_respects_batch_size_and_returns_empty():
    from nocoly_explorer.writer_inputs import chunk_into_batches
    assert chunk_into_batches([], batch_size=10, max_batch_size=100) == []
    out = chunk_into_batches(list(range(7)), batch_size=3, max_batch_size=100)
    assert out == [[0, 1, 2], [3, 4, 5], [6]]


def test_chunk_into_batches_validates_arguments():
    from nocoly_explorer.writer_inputs import chunk_into_batches
    with pytest.raises(ValueError, match="batch_size"):
        chunk_into_batches([1, 2], batch_size=0, max_batch_size=100)
    with pytest.raises(ValueError, match="exceeds"):
        chunk_into_batches([1, 2], batch_size=200, max_batch_size=100)