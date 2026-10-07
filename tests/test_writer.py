"""Tests for the synchronous WorksheetWriter."""

from __future__ import annotations

import csv
import json
from unittest.mock import MagicMock

import pytest

from nocoly_explorer.auth import CredentialPair
from nocoly_explorer.exceptions import (
    NocolyWriteError,
    WriteBatchError,
    WriteBatchFailureInfo,
    WriteValidationError,
)
from nocoly_explorer.writer import WorksheetWriter, WriteResult
from nocoly_explorer.writer_inputs import coerce_to_rows, normalize_batch_size


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _client(**overrides):
    creds = CredentialPair(app_key="k", app_sign="s")
    kwargs = dict(
        host="https://example.com",
        worksheet_id="ws_1",
        credentials=creds,
        timeout_seconds=30.0,
        max_retries=2,
        max_wait_seconds=5.0,
    )
    kwargs.update(overrides)
    return WorksheetWriter(**kwargs)


class _FakeResponse:
    def __init__(
        self,
        *,
        status_code=200,
        json_data=None,
        headers=None,
        text="",
    ):
        self.status_code = status_code
        self.ok = 200 <= status_code < 300
        self._json = json_data
        self.text = text
        self.headers = headers or {}

    def json(self):
        return self._json


def _patch_request(client, responses):
    """Replace client._session.request with a sequence-returning mock."""
    iter_resp = iter(responses)
    request = MagicMock(side_effect=lambda *a, **kw: next(iter_resp))
    client._session.request = request
    return request


# ---------------------------------------------------------------------------
# Constructor validation
# ---------------------------------------------------------------------------


def test_constructor_rejects_empty_credentials():
    with pytest.raises(WriteValidationError):
        WorksheetWriter(
            host="https://example.com",
            worksheet_id="ws_1",
            credentials=CredentialPair(app_key="", app_sign="s"),
        )


def test_constructor_rejects_zero_batch_size():
    with pytest.raises(ValueError):
        WorksheetWriter(
            host="https://example.com",
            worksheet_id="ws_1",
            credentials=CredentialPair(app_key="k", app_sign="s"),
            batch_size=0,
        )


def test_constructor_rejects_oversized_batch():
    with pytest.raises(ValueError):
        WorksheetWriter(
            host="https://example.com",
            worksheet_id="ws_1",
            credentials=CredentialPair(app_key="k", app_sign="s"),
            batch_size=10_000,
        )


def test_constructor_rejects_negative_retries():
    with pytest.raises(ValueError):
        WorksheetWriter(
            host="https://example.com",
            worksheet_id="ws_1",
            credentials=CredentialPair(app_key="k", app_sign="s"),
            max_retries=-1,
        )


# ---------------------------------------------------------------------------
# Input coercion
# ---------------------------------------------------------------------------


def test_coerce_to_rows_passthrough_list_of_dicts():
    rows = [{"a": 1}, {"a": 2}]
    out = coerce_to_rows(rows, operation="add")
    assert out == [{"a": 1}, {"a": 2}]


def test_coerce_to_rows_rejects_single_mapping():
    with pytest.raises(WriteValidationError):
        coerce_to_rows({"a": 1}, operation="add")


def test_coerce_to_rows_rejects_none():
    with pytest.raises(WriteValidationError):
        coerce_to_rows(None, operation="add")


def test_coerce_to_rows_rejects_nonexistent_file(tmp_path):
    with pytest.raises(WriteValidationError):
        coerce_to_rows(str(tmp_path / "missing.csv"), operation="add")


def test_coerce_to_rows_reads_csv_file(tmp_path):
    path = tmp_path / "data.csv"
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "age"])
        writer.writeheader()
        writer.writerow({"name": "alice", "age": "30"})
        writer.writerow({"name": "bob", "age": "25"})

    out = coerce_to_rows(str(path), operation="add")
    # pyarrow auto-types CSV columns, so 30/25 are returned as ints.
    assert out == [{"name": "alice", "age": 30}, {"name": "bob", "age": 25}]


def test_coerce_to_rows_reads_jsonl_file(tmp_path):
    path = tmp_path / "data.jsonl"
    with path.open("w") as f:
        f.write(json.dumps({"x": 1}) + "\n")
        f.write(json.dumps({"x": 2}) + "\n")

    out = coerce_to_rows(str(path), operation="add")
    assert out == [{"x": 1}, {"x": 2}]


def test_normalize_batch_size_validates():
    assert normalize_batch_size(100, 1000) == 100
    with pytest.raises(ValueError):
        normalize_batch_size(0, 1000)
    with pytest.raises(ValueError):
        normalize_batch_size(2000, 1000)


# ---------------------------------------------------------------------------
# add_rows
# ---------------------------------------------------------------------------


def test_add_rows_chunks_large_input_into_multiple_batches():
    client = _client(batch_size=3)
    responses = [_FakeResponse(json_data={"ok": True}) for _ in range(4)]
    request = _patch_request(client, responses)

    rows = [{"a": i} for i in range(10)]
    result = client.add_rows(rows)

    assert result.operation == "add"
    assert result.rows_attempted == 10
    assert result.rows_succeeded == 10
    assert result.batches_sent == 4
    assert result.batches_failed == 0
    assert len(responses) - request.call_count == 0  # all consumed
    # First three batches have 3 rows, last has 1.
    bodies = [call.kwargs["json"]["rows"] for call in request.call_args_list]
    assert [len(b) for b in bodies] == [3, 3, 3, 1]


def test_add_rows_sends_post_to_default_endpoint():
    client = _client()
    responses = [_FakeResponse(json_data={"ok": True})]
    request = _patch_request(client, responses)

    client.add_rows([{"name": "alice"}])

    assert request.call_count == 1
    args, kwargs = request.call_args
    assert args[0] == "POST"
    assert args[1] == "https://example.com/api/v3/app/worksheets/ws_1/rows"
    assert kwargs["json"] == {"rows": [{"name": "alice"}]}
    assert kwargs["headers"]["HAP-AppKey"] == "k"
    assert kwargs["headers"]["HAP-Sign"] == "s"


def test_add_rows_empty_input_is_noop():
    client = _client()
    request = MagicMock()
    client._session.request = request

    result = client.add_rows([])
    assert result.rows_attempted == 0
    assert result.batches_sent == 0
    assert request.call_count == 0


def test_add_rows_raises_on_first_failure():
    client = _client(max_retries=1)
    responses = [
        _FakeResponse(status_code=500, text="boom"),
        _FakeResponse(status_code=500, text="boom"),
    ]
    _patch_request(client, responses)

    with pytest.raises(WriteBatchError) as excinfo:
        client.add_rows([{"a": 1}, {"b": 2}, {"c": 3}], batch_size=2)

    assert len(excinfo.value.failures) == 1
    failure = excinfo.value.failures[0]
    assert failure.status_code == 500
    assert "boom" in failure.body


def test_add_rows_fail_false_collects_failures():
    client = _client(max_retries=0)
    responses = [
        _FakeResponse(status_code=200, json_data={"ok": 1}),
        _FakeResponse(status_code=400, text="bad request"),
        _FakeResponse(status_code=200, json_data={"ok": 1}),
    ]
    _patch_request(client, responses)

    result = client.add_rows(
        [{"a": i} for i in range(6)],
        batch_size=2,
        fail_fast=False,
    )

    assert result.batches_sent == 3
    assert result.batches_failed == 1
    assert result.rows_succeeded == 4
    assert len(result.failures) == 1
    assert result.failures[0].status_code == 400


def test_add_rows_retries_429_with_retry_after_header(monkeypatch):
    client = _client(max_retries=2, max_wait_seconds=10.0)
    sleeps = []
    monkeypatch.setattr("nocoly_explorer.writer.time.sleep", lambda s: sleeps.append(s))

    responses = [
        _FakeResponse(status_code=429, headers={"Retry-After": "0"}, text="slow down"),
        _FakeResponse(json_data={"ok": True}),
    ]
    _patch_request(client, responses)

    result = client.add_rows([{"a": 1}])
    assert result.batches_sent == 1
    assert result.rows_succeeded == 1
    assert len(sleeps) >= 1


def test_add_rows_uses_override_endpoint():
    client = _client(
        add_endpoint="/api/v3/app/worksheets/{worksheet_id}/rows/create"
    )
    responses = [_FakeResponse(json_data={"ok": True})]
    request = _patch_request(client, responses)

    client.add_rows([{"a": 1}])

    assert request.call_args.args[1].endswith("/rows/create")


# ---------------------------------------------------------------------------
# update_rows / upsert_rows
# ---------------------------------------------------------------------------


def test_update_rows_requires_key_column():
    client = _client()
    with pytest.raises(WriteValidationError):
        client.update_rows([{"id": 1}], key_column="")


def test_update_rows_rejects_rows_missing_key():
    client = _client()
    with pytest.raises(WriteValidationError):
        client.update_rows(
            [{"id": 1}, {"no_id": "x"}],
            key_column="id",
        )


def test_update_rows_sends_put_with_key_column():
    client = _client()
    responses = [_FakeResponse(json_data={"ok": True})]
    request = _patch_request(client, responses)

    result = client.update_rows(
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
        key_column="id",
    )

    assert request.call_args.args[0] == "PUT"
    body = request.call_args.kwargs["json"]
    assert body["keyColumn"] == "id"
    assert body["rows"] == [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    assert result.rows_succeeded == 2


def test_upsert_rows_sends_post_with_key_column():
    client = _client()
    responses = [_FakeResponse(json_data={"ok": True})]
    request = _patch_request(client, responses)

    result = client.upsert_rows(
        [{"id": 1, "name": "alice"}],
        key_column="id",
    )

    assert request.call_args.args[0] == "POST"
    body = request.call_args.kwargs["json"]
    assert body["keyColumn"] == "id"
    assert result.rows_succeeded == 1


# ---------------------------------------------------------------------------
# delete_rows
# ---------------------------------------------------------------------------


def test_delete_rows_requires_exactly_one_input():
    client = _client()
    with pytest.raises(WriteValidationError):
        client.delete_rows()  # neither
    with pytest.raises(WriteValidationError):
        client.delete_rows(
            row_ids=[1],
            filter_criteria={"x": 1},
        )  # both


def test_delete_rows_rejects_empty_ids():
    client = _client()
    with pytest.raises(WriteValidationError):
        client.delete_rows(row_ids=[])


def test_delete_rows_by_id_chunks_and_coerces():
    client = _client(batch_size=2)
    responses = [_FakeResponse(json_data={"deleted": 2}) for _ in range(2)]
    request = _patch_request(client, responses)

    result = client.delete_rows(row_ids=[1, 2, 3], batch_size=2)

    assert result.batches_sent == 2
    assert result.batches_failed == 0
    bodies = [call.kwargs["json"] for call in request.call_args_list]
    assert bodies[0] == {"rowIds": [1, 2]}
    assert bodies[1] == {"rowIds": [3]}


def test_delete_rows_by_filter_sends_single_batch():
    client = _client()
    responses = [_FakeResponse(json_data={"deleted": 5})]
    request = _patch_request(client, responses)

    client.delete_rows(
        filter_criteria={"type": "group", "logic": "AND", "filters": []}
    )

    assert request.call_args.kwargs["json"] == {
        "filter": {"type": "group", "logic": "AND", "filters": []}
    }


def test_delete_rows_coerces_non_primitive_ids():
    client = _client()
    responses = [_FakeResponse(json_data={"ok": True})]
    request = _patch_request(client, responses)

    import uuid
    guid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    client.delete_rows(row_ids=[guid, 42, "raw-id"])

    sent = request.call_args.kwargs["json"]["rowIds"]
    assert sent == [str(guid), 42, "raw-id"]


# ---------------------------------------------------------------------------
# End-to-end with mixed inputs
# ---------------------------------------------------------------------------


def test_add_rows_from_pandas_dataframe(monkeypatch):
    pd = pytest.importorskip("pandas")
    client = _client()
    responses = [_FakeResponse(json_data={"ok": True})]
    request = _patch_request(client, responses)

    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    client.add_rows(df)

    sent = request.call_args.kwargs["json"]["rows"]
    assert sent == [{"a": 1, "b": 3}, {"a": 2, "b": 4}]


def test_add_rows_from_pyarrow_table():
    pa = pytest.importorskip("pyarrow")
    client = _client()
    responses = [_FakeResponse(json_data={"ok": True})]
    request = _patch_request(client, responses)

    table = pa.table({"a": [1, 2], "b": [3, 4]})
    client.add_rows(table)

    sent = request.call_args.kwargs["json"]["rows"]
    assert sent == [{"a": 1, "b": 3}, {"a": 2, "b": 4}]


# ---------------------------------------------------------------------------
# Error inheritance
# ---------------------------------------------------------------------------


def test_write_exception_hierarchy():
    assert issubclass(WriteValidationError, NocolyWriteError)
    assert issubclass(WriteBatchError, NocolyWriteError)
    info = WriteBatchFailureInfo(batch_index=0, status_code=500, body="x")
    exc = WriteBatchError("boom", failures=[info])
    assert exc.failures[0] is info