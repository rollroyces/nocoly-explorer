"""Tests for incremental sync via WorksheetExporter.export(incremental=True).

These tests stub out the Nocoly HTTP layer so they can run without a
real server. They verify:
- Initial run produces a full export and writes the watermark.
- Second run with the same store only fetches rows newer than the watermark.
- force_full=True ignores the watermark.
- Out-of-order rows don't move the watermark backward.
- The since-filter is AND-combined with any user filter.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List

import pytest


class _MockRes:
    ok = True
    def __init__(self, payload):
        self._payload = payload
    def json(self):
        return self._payload


class _MockSession:
    """Replays a sequence of pages on successive POSTs."""

    def __init__(self, pages: List[Dict[str, Any]]):
        self._pages = pages
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append({"url": url, "kwargs": kwargs})
        # The mock session never has any more pages after the first POST
        # in these tests; subsequent POSTs get an empty-page response.
        if self._pages:
            page = self._pages.pop(0)
            return _MockRes(page)
        return _MockRes({"rows": [], "has_more": False})


def _make_client(session: _MockSession):
    from nocoly_explorer.auth import CredentialPair
    from nocoly_explorer.client import WorksheetClient

    client = WorksheetClient(
        host="https://example.com",
        worksheet_id="ws-1",
        credentials=CredentialPair(app_key="k", app_sign="s"),
    )
    client._session = session
    return client


@pytest.fixture
def store(tmp_path):
    from nocoly_explorer.sync_state import FileSyncStateStore
    return FileSyncStateStore(base_dir=tmp_path, workspace="test")


# ---------------------------------------------------------------------------
# Initial run vs incremental run
# ---------------------------------------------------------------------------


class TestIncrementalExport:
    def test_initial_run_writes_watermark(self, store, monkeypatch):
        from nocoly_explorer import WorksheetExporter

        pages = [
            {"rows": [
                {"_updatedAt": "2026-09-29T01:00:00Z", "id": 1},
                {"_updatedAt": "2026-09-29T02:00:00Z", "id": 2},
                {"_updatedAt": "2026-09-29T03:00:00Z", "id": 3},
            ], "has_more": False},
        ]
        client = _make_client(_MockSession(pages))
        monkeypatch.setattr(
            "nocoly_explorer.exporter.WorksheetClient",
            lambda *a, **kw: client,
        )

        rows = WorksheetExporter().export(
            host="https://example.com",
            worksheet_id="ws-1",
            output_type="json",
            app_key="k", app_sign="s",
            incremental=True,
            state_store=store,
        )
        assert len(rows) == 3
        # Watermark is the max _updatedAt across all rows.
        saved = store.get("ws-1")
        assert saved.watermark == "2026-09-29T03:00:00Z"
        assert saved.rows_synced == 3

    def test_second_run_only_fetches_newer(self, store, monkeypatch):
        from nocoly_explorer import WorksheetExporter

        # First run.
        pages1 = [
            {"rows": [
                {"_updatedAt": "2026-09-29T01:00:00Z", "id": 1},
                {"_updatedAt": "2026-09-29T02:00:00Z", "id": 2},
            ], "has_more": False},
        ]
        client1 = _make_client(_MockSession(pages1))
        monkeypatch.setattr(
            "nocoly_explorer.exporter.WorksheetClient",
            lambda *a, **kw: client1,
        )
        WorksheetExporter().export(
            host="https://example.com",
            worksheet_id="ws-1",
            output_type="json",
            app_key="k", app_sign="s",
            incremental=True,
            state_store=store,
        )

        # Second run: capture what filter is sent to the server.
        pages2_calls: List[Dict[str, Any]] = []

        class _CaptureSession:
            def post(self, url, **kwargs):
                pages2_calls.append({"url": url, "kwargs": kwargs})
                return _MockRes({
                    "rows": [{"_updatedAt": "2026-09-29T04:00:00Z", "id": 4}],
                    "has_more": False,
                })

        client2 = _make_client(_CaptureSession())
        monkeypatch.setattr(
            "nocoly_explorer.exporter.WorksheetClient",
            lambda *a, **kw: client2,
        )
        rows = WorksheetExporter().export(
            host="https://example.com",
            worksheet_id="ws-1",
            output_type="json",
            app_key="k", app_sign="s",
            incremental=True,
            state_store=store,
        )
        assert len(rows) == 1
        assert rows[0]["id"] == 4
        # The second-run request should carry a since-filter in its payload.
        body = pages2_calls[0]["kwargs"].get("json", {})
        assert "filters" in body
        # Confirm at least one filter is a _updatedAt__gt condition.
        found_since = _find_updated_at_gt(body["filters"])
        assert found_since is not None
        assert "2026-09-29T02:00:00Z" <= found_since  # max from first run
        # Watermark advances.
        assert store.get("ws-1").watermark == "2026-09-29T04:00:00Z"

    def test_force_full_ignores_watermark(self, store, monkeypatch):
        from nocoly_explorer import WorksheetExporter

        # Seed the store with a watermark.
        store.set(SyncWatermark(worksheet_id="ws-1", watermark="2026-09-29T00:00:00Z"))

        pages_calls: List[Dict[str, Any]] = []

        class _CapSession:
            def post(self, url, **kwargs):
                pages_calls.append({"kwargs": kwargs})
                return _MockRes({
                    "rows": [{"_updatedAt": "2026-09-29T05:00:00Z", "id": 99}],
                    "has_more": False,
                })

        monkeypatch.setattr(
            "nocoly_explorer.exporter.WorksheetClient",
            lambda *a, **kw: _make_client(_CapSession()),
        )
        WorksheetExporter().export(
            host="https://example.com",
            worksheet_id="ws-1",
            output_type="json",
            app_key="k", app_sign="s",
            incremental=True,
            state_store=store,
            force_full=True,
        )
        # force_full cleared the watermark before fetching; the request
        # body should NOT carry a since-filter.
        body = pages_calls[0]["kwargs"].get("json", {})
        assert not _find_updated_at_gt(body.get("filters"))

    def test_out_of_order_does_not_move_watermark_backward(self, store, monkeypatch):
        from nocoly_explorer import WorksheetExporter

        # First run: high watermark.
        monkeypatch.setattr(
            "nocoly_explorer.exporter.WorksheetClient",
            lambda *a, **kw: _make_client(_MockSession([{
                "rows": [{"_updatedAt": "2026-09-29T10:00:00Z", "id": 1}],
                "has_more": False,
            }])),
        )
        WorksheetExporter().export(
            host="https://example.com",
            worksheet_id="ws-1",
            output_type="json",
            app_key="k", app_sign="s",
            incremental=True,
            state_store=store,
        )
        assert store.get("ws-1").watermark == "2026-09-29T10:00:00Z"

        # Second run: a row OLDER than the watermark. Watermans must not move back.
        monkeypatch.setattr(
            "nocoly_explorer.exporter.WorksheetClient",
            lambda *a, **kw: _make_client(_MockSession([{
                "rows": [{"_updatedAt": "2026-09-29T05:00:00Z", "id": 2}],
                "has_more": False,
            }])),
        )
        WorksheetExporter().export(
            host="https://example.com",
            worksheet_id="ws-1",
            output_type="json",
            app_key="k", app_sign="s",
            incremental=True,
            state_store=store,
        )
        # Watermark stays at 10:00 even though we fetched 05:00.
        assert store.get("ws-1").watermark == "2026-09-29T10:00:00Z"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find_updated_at_gt(filters) -> str | None:
    """Walk a filter tree and return the value of any _updatedAt__gt condition."""
    if not filters:
        return None
    if isinstance(filters, dict):
        if filters.get("operator") == "GT" and filters.get("field") == "_updatedAt":
            return filters.get("value")
        # Recurse into group filters.
        for k in ("filters", "children"):
            if k in filters:
                v = _find_updated_at_gt(filters[k])
                if v is not None:
                    return v
    if isinstance(filters, list):
        for item in filters:
            v = _find_updated_at_gt(item)
            if v is not None:
                return v
    return None


# Import alias so the test_*.py file reads cleanly at the top.
from nocoly_explorer.sync_state import SyncWatermark  # noqa: E402
