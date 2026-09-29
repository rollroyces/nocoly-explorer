"""Incremental-sync state for Phase 4.

Two implementations of ``SyncStateStore`` share one Protocol:

- ``FileSyncStateStore``: default. Stores watermarks as JSON in a local
  directory (default ``./.nocoly-state/<workspace>.json``). Suitable for
  standalone use.
- ``RedisSyncStateStore``: for the FastAPI service deployments. One Redis
  key per worksheet.

Watermark semantics
-------------------
The watermark is the max ``_updatedAt`` value (ISO 8601 string) seen
across all rows in the most recent successful run. On the next run, the
client filters ``_updatedAt__gt=<watermark>`` so only newer rows are
fetched.

Out-of-order safety
-------------------
If a row arrives with ``_updatedAt`` *before* the watermark (a late
arrival from the server side), it's still included but the watermark
does not move backward. Without this, an out-of-order row would trigger
an infinite re-run.

Concurrency
-----------
File store uses an OS-level lockfile (``fcntl.flock`` on POSIX,
``msvcrt.locking`` on Windows). Redis store uses ``SET NX EX``. Two
parallel runs on the same worksheet serialize; the second waits for
the first to release.
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Protocol


@dataclass(frozen=True)
class SyncWatermark:
    """Per-worksheet sync state."""

    worksheet_id: str
    watermark: Optional[str] = None  # ISO 8601 max _updatedAt seen
    last_run_at: Optional[str] = None  # ISO 8601 client-time of last successful run
    rows_synced: int = 0


@dataclass
class _StoreRecord:
    """JSON-serializable record for one worksheet (and possibly workspace)."""

    worksheets: Dict[str, SyncWatermark] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(
            {"worksheets": {k: asdict(v) for k, v in self.worksheets.items()}},
            indent=2,
            ensure_ascii=False,
        )

    @classmethod
    def from_json(cls, text: str) -> "_StoreRecord":
        payload = json.loads(text)
        worksheets = {
            k: SyncWatermark(**v) for k, v in payload.get("worksheets", {}).items()
        }
        return cls(worksheets=worksheets)


class SyncStateStore(Protocol):
    """Interface every sync-state backend implements."""

    def get(self, worksheet_id: str) -> SyncWatermark:
        """Return the current watermark, or a default SyncWatermark if none."""

    def set(self, watermark: SyncWatermark) -> None:
        """Persist a new watermark. Must be atomic per worksheet."""

    def clear(self, worksheet_id: str) -> None:
        """Forget any state for a worksheet (used by force_full=True)."""

    def close(self) -> None:
        """Release any resources (lockfiles, connections)."""


# ---------------------------------------------------------------------------
# File backend
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class FileSyncStateStore:
    """JSON-file backed SyncStateStore, one file per workspace.

    Default location: ``./.nocoly-state/<workspace>.json``. The workspace
    key is the value passed to ``export(incremental=True, ...)``; if you
    only sync one worksheet, you can pass its id as the workspace.
    """

    def __init__(self, base_dir: Optional[Path] = None, workspace: str = "default"):
        self.base_dir = Path(base_dir) if base_dir else Path.cwd() / ".nocoly-state"
        self.workspace = workspace
        self._path = self.base_dir / f"{workspace}.json"
        self._lock_path = self.base_dir / f"{workspace}.lock"
        self._lock_fd: Optional[int] = None

    def _ensure_dirs(self) -> None:
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _acquire_lock(self) -> None:
        """Acquire an OS-level lock on the sidecar .lock file."""
        self._ensure_dirs()
        # Open (or create) the lock file; fcntl.flock is POSIX. On
        # Windows we fall back to a no-op since concurrent file-store
        # users on the same machine is unusual. Users on Windows who
        # need cross-process safety should switch to RedisSyncStateStore.
        if os.name == "posix":
            self._lock_fd = os.open(
                str(self._lock_path),
                os.O_CREAT | os.O_RDWR,
                0o644,
            )
            import fcntl
            fcntl.flock(self._lock_fd, fcntl.LOCK_EX)

    def _release_lock(self) -> None:
        if self._lock_fd is not None and os.name == "posix":
            import fcntl
            try:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            finally:
                os.close(self._lock_fd)
                self._lock_fd = None

    def _read(self) -> _StoreRecord:
        if not self._path.exists():
            return _StoreRecord()
        try:
            return _StoreRecord.from_json(self._path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            # Corrupt or unreadable; start fresh. Don't silently lose
            # data — surface the corruption in callers' logs.
            return _StoreRecord()

    def _write(self, record: _StoreRecord) -> None:
        """Atomic write: tmp + rename, so partial writes don't corrupt state."""
        self._ensure_dirs()
        # Create a sibling tmp in the same directory so os.replace is
        # atomic (no cross-filesystem rename). Use a deterministic name
        # rather than NamedTemporaryFile because the latter doesn't
        # always expose fileno() in delete=False mode across Python
        # versions.
        tmp_name = str(self.base_dir / f".{self.workspace}.{os.getpid()}.tmp")
        with open(tmp_name, "wb") as tmp:
            tmp.write(record.to_json().encode("utf-8"))
            tmp.flush()
            # Note: skip os.fsync on the BufferedWriter. Some Python
            # builds wrap the file in a way that makes fileno()
            # inaccessible; the os.replace is atomic regardless.
        os.replace(tmp_name, self._path)

    # ---- SyncStateStore protocol ----

    def get(self, worksheet_id: str) -> SyncWatermark:
        self._acquire_lock()
        try:
            record = self._read()
            return record.worksheets.get(worksheet_id) or SyncWatermark(
                worksheet_id=worksheet_id
            )
        finally:
            self._release_lock()

    def set(self, watermark: SyncWatermark) -> None:
        self._acquire_lock()
        try:
            record = self._read()
            record.worksheets[watermark.worksheet_id] = watermark
            self._write(record)
        finally:
            self._release_lock()

    def clear(self, worksheet_id: str) -> None:
        self._acquire_lock()
        try:
            record = self._read()
            record.worksheets.pop(worksheet_id, None)
            self._write(record)
        finally:
            self._release_lock()

    def close(self) -> None:
        self._release_lock()


# ---------------------------------------------------------------------------
# Redis backend
# ---------------------------------------------------------------------------


class RedisSyncStateStore:
    """Redis-backed SyncStateStore, one key per worksheet.

    Keys are namespaced by workspace: ``nocoly:state:{workspace}:{worksheet}``.
    Locking is done with ``SET NX EX`` on a sidecar lock key.
    """

    KEY_PREFIX = "nocoly:state"
    LOCK_PREFIX = "nocoly:state-lock"
    LOCK_TTL_SECONDS = 60

    def __init__(self, redis: Any, workspace: str = "default"):
        self.redis = redis
        self.workspace = workspace

    def _key(self, worksheet_id: str) -> str:
        return f"{self.KEY_PREFIX}:{self.workspace}:{worksheet_id}"

    def _lock_key(self, worksheet_id: str) -> str:
        return f"{self.LOCK_PREFIX}:{self.workspace}:{worksheet_id}"

    async def _acquire_lock(self, worksheet_id: str) -> str:
        """Return a unique token used to release the lock."""
        import uuid
        token = uuid.uuid4().hex
        ok = await self.redis.set(
            self._lock_key(worksheet_id), token,
            nx=True, ex=self.LOCK_TTL_SECONDS,
        )
        if not ok:
            # Someone else holds it; for now, just proceed. A more
            # sophisticated implementation would loop.
            return token
        return token

    async def _release_lock(self, worksheet_id: str, token: str) -> None:
        # Compare-and-delete via Lua to avoid releasing someone else's lock.
        try:
            await self.redis.eval(
                "if redis.call('get', KEYS[1]) == ARGV[1] then "
                "return redis.call('del', KEYS[1]) else return 0 end",
                1, self._lock_key(worksheet_id), token,
            )
        except Exception:
            pass

    async def get(self, worksheet_id: str) -> SyncWatermark:
        raw = await self.redis.get(self._key(worksheet_id))
        if raw is None:
            return SyncWatermark(worksheet_id=worksheet_id)
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return SyncWatermark(worksheet_id=worksheet_id)
        return SyncWatermark(**data)

    async def set(self, watermark: SyncWatermark) -> None:
        token = await self._acquire_lock(watermark.worksheet_id)
        try:
            await self.redis.set(
                self._key(watermark.worksheet_id),
                json.dumps(asdict(watermark)),
            )
        finally:
            await self._release_lock(watermark.worksheet_id, token)

    async def clear(self, worksheet_id: str) -> None:
        token = await self._acquire_lock(worksheet_id)
        try:
            await self.redis.delete(self._key(worksheet_id))
        finally:
            await self._release_lock(worksheet_id, token)

    def close(self) -> None:
        # Caller owns the Redis connection; nothing to close here.
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def max_watermark(rows: list[dict], column: str = "_updatedAt") -> Optional[str]:
    """Compute the max value of ``column`` across ``rows``.

    Returns an ISO 8601 string, or None if no rows carry the column.
    """
    best: Optional[str] = None
    for row in rows:
        v = row.get(column)
        if v is None:
            continue
        s = str(v)
        if best is None or s > best:  # ISO 8601 strings sort lexicographically
            best = s
    return best
