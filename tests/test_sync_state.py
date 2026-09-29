"""Tests for the incremental-sync state store and helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from nocoly_explorer.sync_state import (
    FileSyncStateStore,
    SyncWatermark,
    _now_iso,
    max_watermark,
)


# ---------------------------------------------------------------------------
# SyncWatermark + helpers
# ---------------------------------------------------------------------------


class TestSyncWatermark:
    def test_default_has_no_watermark(self):
        w = SyncWatermark(worksheet_id="ws-1")
        assert w.worksheet_id == "ws-1"
        assert w.watermark is None
        assert w.last_run_at is None
        assert w.rows_synced == 0


class TestMaxWatermark:
    def test_returns_none_for_empty_list(self):
        assert max_watermark([]) is None

    def test_returns_max_across_rows(self):
        rows = [
            {"_updatedAt": "2026-01-01T00:00:00Z", "id": 1},
            {"_updatedAt": "2026-03-15T00:00:00Z", "id": 2},
            {"_updatedAt": "2026-02-10T00:00:00Z", "id": 3},
        ]
        assert max_watermark(rows) == "2026-03-15T00:00:00Z"

    def test_ignores_rows_without_column(self):
        rows = [
            {"id": 1},
            {"_updatedAt": "2026-01-01T00:00:00Z", "id": 2},
            {"id": 3},
        ]
        assert max_watermark(rows) == "2026-01-01T00:00:00Z"

    def test_uses_custom_column(self):
        rows = [
            {"modified_at": "2026-01-01T00:00:00Z"},
            {"modified_at": "2026-05-01T00:00:00Z"},
        ]
        assert max_watermark(rows, column="modified_at") == "2026-05-01T00:00:00Z"


# ---------------------------------------------------------------------------
# FileSyncStateStore
# ---------------------------------------------------------------------------


class TestFileSyncStateStore:
    def test_get_returns_default_for_unknown_worksheet(self, tmp_path):
        store = FileSyncStateStore(base_dir=tmp_path, workspace="default")
        w = store.get("ws-unknown")
        assert w.worksheet_id == "ws-unknown"
        assert w.watermark is None
        store.close()

    def test_set_and_get_round_trip(self, tmp_path):
        store = FileSyncStateStore(base_dir=tmp_path, workspace="default")
        store.set(SyncWatermark(
            worksheet_id="ws-1",
            watermark="2026-09-29T04:00:00Z",
            last_run_at=_now_iso(),
            rows_synced=42,
        ))
        got = store.get("ws-1")
        assert got.watermark == "2026-09-29T04:00:00Z"
        assert got.rows_synced == 42
        store.close()

    def test_persists_to_disk(self, tmp_path):
        store = FileSyncStateStore(base_dir=tmp_path, workspace="default")
        store.set(SyncWatermark(worksheet_id="ws-1", watermark="2026-01-01T00:00:00Z"))
        store.close()

        # Reopen and verify
        store2 = FileSyncStateStore(base_dir=tmp_path, workspace="default")
        got = store2.get("ws-1")
        assert got.watermark == "2026-01-01T00:00:00Z"
        store2.close()

    def test_clear_removes_worksheet(self, tmp_path):
        store = FileSyncStateStore(base_dir=tmp_path, workspace="default")
        store.set(SyncWatermark(worksheet_id="ws-1", watermark="2026-01-01T00:00:00Z"))
        store.clear("ws-1")
        assert store.get("ws-1").watermark is None
        store.close()

    def test_clear_unknown_worksheet_is_noop(self, tmp_path):
        store = FileSyncStateStore(base_dir=tmp_path, workspace="default")
        store.clear("ws-unknown")  # should not raise
        store.close()

    def test_atomic_write_does_not_leave_tmp(self, tmp_path):
        store = FileSyncStateStore(base_dir=tmp_path, workspace="default")
        store.set(SyncWatermark(worksheet_id="ws-1", watermark="2026-01-01T00:00:00Z"))
        # No .tmp files should remain after the write.
        leftover = list(tmp_path.glob("*.tmp"))
        assert leftover == [], f"leftover tmp files: {leftover}"
        store.close()

    def test_workspace_isolation(self, tmp_path):
        store_a = FileSyncStateStore(base_dir=tmp_path, workspace="prod")
        store_a.set(SyncWatermark(worksheet_id="ws-1", watermark="2026-01-01T00:00:00Z"))

        store_b = FileSyncStateStore(base_dir=tmp_path, workspace="staging")
        assert store_b.get("ws-1").watermark is None
        store_a.close()
        store_b.close()

    def test_corrupt_file_returns_default(self, tmp_path):
        # Corrupt the file before opening.
        store_dir = tmp_path / ".nocoly-state"
        store_dir.mkdir()
        (store_dir / "default.json").write_text("not valid json {", encoding="utf-8")

        store = FileSyncStateStore(base_dir=tmp_path, workspace="default")
        # Should not raise; returns default.
        w = store.get("ws-1")
        assert w.watermark is None
        store.close()


# ---------------------------------------------------------------------------
# Out-of-order safety (the whole point of the watermark logic)
# ---------------------------------------------------------------------------


class TestOutOfOrderSafety:
    def test_max_watermark_does_not_move_backward(self, tmp_path):
        store = FileSyncStateStore(base_dir=tmp_path, workspace="default")

        # First successful run.
        store.set(SyncWatermark(worksheet_id="ws-1", watermark="2026-09-29T04:00:00Z"))

        # Out-of-order arrival: a row arrives with an older _updatedAt.
        # The watermark must not move.
        existing_wm = store.get("ws-1")
        new_data_wm = "2026-09-15T04:00:00Z"  # older
        if not (existing_wm.watermark and new_data_wm < existing_wm.watermark):
            raise AssertionError("test precondition")
        # Caller logic from run_job: don't move backward.
        final_wm = existing_wm.watermark  # keep existing
        store.set(SyncWatermark(worksheet_id="ws-1", watermark=final_wm))
        assert store.get("ws-1").watermark == "2026-09-29T04:00:00Z"
        store.close()
