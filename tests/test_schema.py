"""Tests for the schema/discovery module and WorksheetClient integration."""

from __future__ import annotations

import datetime
import json
from unittest.mock import patch

import pyarrow as pa
import pytest

from nocoly_explorer.auth import CredentialPair
from nocoly_explorer.client import WorksheetClient
from nocoly_explorer.schema import (
    NocolyColumnInfo,
    NocolyWorksheetSchema,
    _parse_columns_response,
    _widen_type,
    infer_schema_from_rows,
)


# ---------------------------------------------------------------------------
# Parser — response shape tolerance
# ---------------------------------------------------------------------------


class TestParseColumnsResponse:
    def test_list_of_dicts(self):
        payload = [
            {"name": "id", "type": "integer"},
            {"name": "name", "type": "string", "nullable": False},
        ]
        cols = _parse_columns_response(payload)
        assert cols is not None
        assert len(cols) == 2
        assert cols[0].name == "id"
        assert cols[0].type == "integer"
        assert cols[0].nullable is True  # default
        assert cols[1].nullable is False

    def test_wrapped_columns_key(self):
        payload = {"columns": [{"name": "a", "type": "string"}]}
        cols = _parse_columns_response(payload)
        assert cols is not None and cols[0].name == "a"

    def test_fields_key(self):
        payload = {"fields": [{"id": "x", "type": "number"}]}
        cols = _parse_columns_response(payload)
        assert cols is not None and cols[0].name == "x"
        assert cols[0].type == "number"

    def test_columnar_shape(self):
        payload = {
            "name": ["a", "b", "c"],
            "type": ["string", "integer", "datetime"],
            "nullable": [True, False, True],
        }
        cols = _parse_columns_response(payload)
        assert cols is not None
        assert [c.name for c in cols] == ["a", "b", "c"]
        assert [c.type for c in cols] == ["string", "integer", "datetime"]
        assert [c.nullable for c in cols] == [True, False, True]

    def test_unrecognized_returns_none(self):
        assert _parse_columns_response(42) is None
        assert _parse_columns_response("not a list") is None
        assert _parse_columns_response({}) is None
        assert _parse_columns_response({"name": "only-name"}) is None

    def test_malformed_item_returns_none(self):
        payload = [{"type": "string"}, {"name": "ok"}]
        assert _parse_columns_response(payload) is None

    def test_alternate_type_keys(self):
        payload = [
            {"name": "a", "dataType": "string"},
            {"name": "b", "data_type": "number"},
            {"name": "c", "kind": "boolean"},
        ]
        cols = _parse_columns_response(payload)
        assert cols is not None
        assert [c.type for c in cols] == ["string", "number", "boolean"]

    def test_nullable_string_normalized(self):
        payload = [
            {"name": "a", "type": "string", "nullable": "no"},
            {"name": "b", "type": "string", "nullable": "true"},
        ]
        cols = _parse_columns_response(payload)
        assert cols[0].nullable is False
        assert cols[1].nullable is True


# ---------------------------------------------------------------------------
# Type widening
# ---------------------------------------------------------------------------


class TestWidenType:
    def test_same_returns_same(self):
        assert _widen_type(pa.int64(), pa.int64()) == pa.int64()

    def test_null_widens_to_other(self):
        assert _widen_type(pa.null(), pa.int64()) == pa.int64()
        assert _widen_type(pa.string(), pa.null()) == pa.string()

    def test_int_widens_to_float(self):
        assert _widen_type(pa.int64(), pa.float64()) == pa.float64()
        assert _widen_type(pa.float64(), pa.int64()) == pa.float64()

    def test_int_to_decimal(self):
        assert _widen_type(pa.int64(), pa.decimal128(38, 10)) == pa.decimal128(38, 10)

    def test_numeric_to_string(self):
        assert _widen_type(pa.int64(), pa.string()) == pa.string()
        assert _widen_type(pa.bool_(), pa.string()) == pa.string()

    def test_date_to_timestamp(self):
        assert _widen_type(pa.date32(), pa.timestamp("us")) == pa.timestamp("us")

    def test_unrelated_types_become_string(self):
        result = _widen_type(pa.list_(pa.null()), pa.timestamp("us"))
        assert result == pa.string()


# ---------------------------------------------------------------------------
# NocolyWorksheetSchema — to_pyarrow, JSON round-trip
# ---------------------------------------------------------------------------


class TestNocolyWorksheetSchema:
    def test_to_pyarrow_maps_types(self):
        schema = NocolyWorksheetSchema(
            worksheet_id="ws-1",
            columns=[
                NocolyColumnInfo("id", "integer", nullable=False),
                NocolyColumnInfo("name", "string"),
                NocolyColumnInfo("created_at", "datetime"),
            ],
        )
        pa_schema = schema.to_pyarrow()
        assert pa_schema.field("id").type == pa.int64()
        assert pa_schema.field("id").nullable is False
        assert pa_schema.field("name").type == pa.string()
        assert pa_schema.field("name").nullable is True
        assert pa_schema.field("created_at").type == pa.timestamp("us")

    def test_unknown_type_maps_to_string(self):
        schema = NocolyWorksheetSchema(
            worksheet_id="ws-1",
            columns=[NocolyColumnInfo("x", "frobnicate")],
        )
        assert schema.to_pyarrow().field("x").type == pa.string()

    def test_json_round_trip(self):
        original = NocolyWorksheetSchema(
            worksheet_id="ws-42",
            columns=[
                NocolyColumnInfo("id", "integer", nullable=False, description="PK"),
                NocolyColumnInfo("name", "string"),
            ],
            source="api",
        )
        text = original.to_json()
        payload = json.loads(text)
        assert payload["worksheet_id"] == "ws-42"
        assert payload["source"] == "api"
        roundtripped = NocolyWorksheetSchema.from_json(text)
        assert roundtripped == original

    def test_from_dict_round_trip(self):
        original = NocolyWorksheetSchema(
            worksheet_id="ws-1",
            columns=[NocolyColumnInfo("x", "string")],
            source="inferred",
        )
        roundtripped = NocolyWorksheetSchema.from_dict(original.to_dict())
        assert roundtripped == original


# ---------------------------------------------------------------------------
# infer_schema_from_rows
# ---------------------------------------------------------------------------


class TestInferSchemaFromRows:
    def test_empty_rows_raises(self):
        with pytest.raises(ValueError):
            infer_schema_from_rows([])

    def test_sample_size_must_be_positive(self):
        with pytest.raises(ValueError):
            infer_schema_from_rows([{"a": 1}], sample_size=0)

    def test_basic_inference(self):
        rows = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
        schema = infer_schema_from_rows(rows, worksheet_id="ws-1")
        assert schema.worksheet_id == "ws-1"
        assert schema.source == "inferred"
        names = [c.name for c in schema.columns]
        assert names == ["a", "b"]
        types_by_name = {c.name: c.type for c in schema.columns}
        assert types_by_name["a"] == "integer"
        assert types_by_name["b"] == "string"

    def test_widens_int_to_float(self):
        rows = [{"x": 1}, {"x": 2.5}, {"x": 3}]
        schema = infer_schema_from_rows(rows)
        assert schema.columns[0].type == "number"

    def test_widens_to_string_when_mixed(self):
        rows = [{"x": 1}, {"x": "two"}]
        schema = infer_schema_from_rows(rows)
        assert schema.columns[0].type == "string"

    def test_nullable_detected(self):
        rows = [{"x": 1}, {"x": None}, {"x": 3}]
        schema = infer_schema_from_rows(rows)
        assert schema.columns[0].nullable is True

    def test_non_nullable_when_no_nulls(self):
        rows = [{"x": 1}, {"x": 2}, {"x": 3}]
        schema = infer_schema_from_rows(rows)
        assert schema.columns[0].nullable is False

    def test_picks_datetimes(self):
        rows = [{"t": datetime.datetime(2026, 1, 1, 12, 0)}]
        schema = infer_schema_from_rows(rows)
        assert schema.columns[0].type == "datetime"

    def test_collects_extra_columns_in_later_rows(self):
        rows = [{"a": 1}, {"a": 2, "b": "extra"}]
        schema = infer_schema_from_rows(rows)
        names = [c.name for c in schema.columns]
        assert names == ["a", "b"]

    def test_sample_size_capped(self):
        rows = [{"x": i} for i in range(100)]
        schema = infer_schema_from_rows(rows, sample_size=2)
        assert len(schema.columns) == 1


# ---------------------------------------------------------------------------
# WorksheetClient.get_worksheet_schema — integration
# ---------------------------------------------------------------------------


class _FakeResp:
    def __init__(self, ok=True, payload=None, status_code=200):
        self.ok = ok
        self._payload = payload or {}
        self.status_code = status_code

    def json(self):
        return self._payload


class _MetaSession:
    """Session whose first POST returns the configured metadata response,
    and whose subsequent POSTs (fetch_rows) return a single page of rows."""

    def __init__(self, metadata_payload=None, metadata_ok=True, rows=None):
        self._payload = metadata_payload
        self._metadata_ok = metadata_ok
        self._rows = rows or []
        self.calls = 0

    def post(self, url, **kwargs):
        self.calls += 1
        if self.calls == 1:
            if self._metadata_ok:
                return _FakeResp(ok=True, payload=self._payload)
            return _FakeResp(ok=False, payload={}, status_code=404)
        # fetch_rows path
        return _FakeResp(ok=True, payload={"rows": self._rows, "has_more": False})


def _make_client(session):
    client = WorksheetClient(
        host="https://example.com",
        worksheet_id="ws-1",
        credentials=CredentialPair(app_key="k", app_sign="s"),
    )
    client._session = session
    return client


class TestWorksheetClientGetSchema:
    def test_returns_api_source_when_endpoint_returns_columns(self):
        session = _MetaSession(metadata_payload=[
            {"name": "id", "type": "integer"},
            {"name": "name", "type": "string"},
        ])
        client = _make_client(session)
        schema = client.get_worksheet_schema()
        assert schema.source == "api"
        assert [c.name for c in schema.columns] == ["id", "name"]

    def test_falls_back_to_inference_on_404(self):
        session = _MetaSession(metadata_ok=False, rows=[{"a": 1, "b": "x"}])
        client = _make_client(session)
        schema = client.get_worksheet_schema()
        assert schema.source == "inferred"
        names = [c.name for c in schema.columns]
        assert names == ["a", "b"]

    def test_falls_back_on_unrecognized_response(self):
        session = _MetaSession(metadata_payload={"totally": "unknown shape"}, rows=[{"a": 1}])
        client = _make_client(session)
        schema = client.get_worksheet_schema()
        assert schema.source == "inferred"

    def test_worksheet_id_override(self):
        session = _MetaSession(metadata_payload=[{"name": "x", "type": "string"}])
        captured = []
        original_post = session.post
        def post(url, **kwargs):
            captured.append(url)
            return original_post(url, **kwargs)
        session.post = post

        client = _make_client(session)
        schema = client.get_worksheet_schema("ws-other")
        assert schema.worksheet_id == "ws-other"
        assert any("ws-other" in u for u in captured)
        assert not any("ws-1" in u for u in captured)

    def test_metadata_endpoint_override(self):
        session = _MetaSession(metadata_payload=[{"name": "x", "type": "string"}])
        captured = []
        original_post = session.post
        def post(url, **kwargs):
            captured.append(url)
            return original_post(url, **kwargs)
        session.post = post

        client = _make_client(session)
        client.get_worksheet_schema(metadata_endpoint="/custom/path")
        assert any("/custom/path" in u for u in captured)


# ---------------------------------------------------------------------------
# Hardening: try_api flag, env var, debug logging
# ---------------------------------------------------------------------------


class TestTryApiFlag:
    def test_try_api_false_skips_endpoint(self, monkeypatch):
        """try_api=False must not call the metadata endpoint."""
        session_calls = []

        class _Session:
            def post(self, url, **kwargs):
                session_calls.append(url)
                raise AssertionError(
                    "metadata endpoint must not be called when try_api=False"
                )

        client = WorksheetClient(
            host="https://example.com",
            worksheet_id="ws-1",
            credentials=CredentialPair(app_key="k", app_sign="s"),
        )
        client._session = _Session()
        # Patch fetch_rows to return a sample without hitting the network.
        monkeypatch.setattr(
            WorksheetClient, "fetch_rows",
            lambda self, **kwargs: [{"a": 1, "b": "x"}],
        )
        schema = client.get_worksheet_schema(try_api=False)
        assert schema.source == "inferred"
        assert session_calls == []

    def test_try_api_none_uses_default(self, monkeypatch):
        """try_api=None (default) tries the endpoint, falls back on 4xx."""
        session_calls = []

        class _Session:
            def post(self, url, **kwargs):
                session_calls.append(url)
                class _Resp:
                    ok = False
                    status_code = 404
                    def json(self): return {}
                return _Resp()

        client = WorksheetClient(
            host="https://example.com",
            worksheet_id="ws-1",
            credentials=CredentialPair(app_key="k", app_sign="s"),
        )
        client._session = _Session()
        monkeypatch.setattr(
            WorksheetClient, "fetch_rows",
            lambda self, **kwargs: [{"a": 1}],
        )
        # Default try_api=None should attempt the API (and fall back on 404).
        schema = client.get_worksheet_schema()
        assert schema.source == "inferred"
        # At least one call hit the metadata endpoint (the fetch_rows fallback
        # is the second call).
        assert any("/columns" in u for u in session_calls)


class TestEnvVar:
    def test_env_var_disables_api(self, monkeypatch):
        """NOCOLY_SCHEMA_API_DISABLED=1 must skip the metadata endpoint."""
        monkeypatch.setenv("NOCOLY_SCHEMA_API_DISABLED", "1")

        session_calls = []

        class _Session:
            def post(self, url, **kwargs):
                session_calls.append(url)
                raise AssertionError(
                    "metadata endpoint must not be called when env var disables"
                )

        client = WorksheetClient(
            host="https://example.com",
            worksheet_id="ws-1",
            credentials=CredentialPair(app_key="k", app_sign="s"),
        )
        client._session = _Session()
        monkeypatch.setattr(
            WorksheetClient, "fetch_rows",
            lambda self, **kwargs: [{"a": 1}],
        )
        schema = client.get_worksheet_schema()
        assert schema.source == "inferred"
        assert session_calls == []

    def test_env_var_truthy_values(self, monkeypatch):
        """1, true, yes, on all disable the API."""
        for value in ("1", "true", "yes", "on", "TRUE", "Yes"):
            monkeypatch.setenv("NOCOLY_SCHEMA_API_DISABLED", value)
            from nocoly_explorer.client import _resolve_try_api
            assert _resolve_try_api(None) is False, f"value={value!r}"

    def test_env_var_unset_or_false_keeps_api_enabled(self, monkeypatch):
        monkeypatch.delenv("NOCOLY_SCHEMA_API_DISABLED", raising=False)
        from nocoly_explorer.client import _resolve_try_api
        assert _resolve_try_api(None) is True

        monkeypatch.setenv("NOCOLY_SCHEMA_API_DISABLED", "0")
        assert _resolve_try_api(None) is True
        monkeypatch.setenv("NOCOLY_SCHEMA_API_DISABLED", "no")
        assert _resolve_try_api(None) is True

    def test_explicit_arg_overrides_env_var(self, monkeypatch):
        """Explicit try_api=True beats env var; try_api=False beats env var."""
        from nocoly_explorer.client import _resolve_try_api
        monkeypatch.setenv("NOCOLY_SCHEMA_API_DISABLED", "1")
        # env says disabled, but explicit override wins.
        assert _resolve_try_api(True) is True
        monkeypatch.delenv("NOCOLY_SCHEMA_API_DISABLED", raising=False)
        assert _resolve_try_api(False) is False


class TestDebugLogging:
    def test_debug_log_emitted_on_attempt(self, caplog):
        """A DEBUG line is logged with the URL when the API is attempted."""
        from nocoly_explorer.client import WorksheetClient

        class _Resp:
            ok = True
            status_code = 200
            def json(self):
                return [{"name": "x", "type": "string"}]

        class _Session:
            def post(self, url, **kwargs):
                return _Resp()

        client = WorksheetClient(
            host="https://example.com",
            worksheet_id="ws-1",
            credentials=CredentialPair(app_key="k", app_sign="s"),
        )
        client._session = _Session()

        with caplog.at_level("DEBUG", logger="nocoly_explorer.client"):
            client.get_worksheet_schema()

        # At least one debug log mentions the URL.
        debug_records = [r for r in caplog.records if r.levelname == "DEBUG"]
        assert any(
            getattr(r, "url", None) and "/columns" in r.url
            for r in debug_records
        ), f"no debug log with /columns URL: {[r.__dict__ for r in debug_records]}"

    def test_no_debug_log_when_try_api_false(self, caplog, monkeypatch):
        """No DEBUG log when try_api=False (no API attempt made)."""
        client = WorksheetClient(
            host="https://example.com",
            worksheet_id="ws-1",
            credentials=CredentialPair(app_key="k", app_sign="s"),
        )
        monkeypatch.setattr(
            WorksheetClient, "fetch_rows",
            lambda self, **kwargs: [{"a": 1}],
        )
        with caplog.at_level("DEBUG", logger="nocoly_explorer.client"):
            client.get_worksheet_schema(try_api=False)
        # No "schema.try_api_*" records should appear.
        assert not any(
            r.name.startswith("schema.")
            for r in caplog.records
        )
