"""Shared retry / rate-limit backoff helpers.

Centralizing this logic here keeps the sync (``client.WorksheetClient``) and
async (``async_client.AsyncWorksheetClient``) implementations from drifting.
"""

from __future__ import annotations

import datetime as _dt
import random
from email.utils import parsedate_to_datetime
from typing import Optional


def compute_backoff(
    *,
    attempt: int,
    base_delay: float,
    max_delay: float,
    max_wait_seconds: float,
    jitter_factor: float = 0.5,
) -> float:
    """Exponential backoff with jitter.

    ``delay = min(base_delay * 2 ** attempt, max_delay)``
    ``jitter = delay * jitter_factor * random.random()``
    return ``min(delay + jitter, max_wait_seconds)``

    Pass ``jitter_factor=0.0`` to disable jitter (useful for deterministic
    tests and for callers that want strict exponential backoff).
    """
    if attempt < 0:
        raise ValueError(f"attempt must be >= 0 (got {attempt})")
    if base_delay <= 0:
        raise ValueError(f"base_delay must be > 0 (got {base_delay})")
    if max_delay <= 0:
        raise ValueError(f"max_delay must be > 0 (got {max_delay})")
    if max_wait_seconds <= 0:
        raise ValueError(f"max_wait_seconds must be > 0 (got {max_wait_seconds})")
    if not 0.0 <= jitter_factor <= 1.0:
        raise ValueError(f"jitter_factor must be in [0, 1] (got {jitter_factor})")

    delay = min(base_delay * (2 ** attempt), max_delay)
    jitter = delay * jitter_factor * random.random()
    return min(delay + jitter, max_wait_seconds)


def parse_retry_after(
    header: Optional[str], *, max_wait_seconds: float
) -> Optional[float]:
    """Parse a ``Retry-After`` header (delta-seconds or HTTP-date).

    Returns the parsed wait time in seconds, capped at ``max_wait_seconds``,
    or ``None`` if the header is missing, empty, or unparseable.
    """
    if header is None:
        return None
    header = header.strip()
    if not header:
        return None

    # delta-seconds form: a non-negative number.
    try:
        seconds = float(header)
        if seconds >= 0:
            return min(seconds, max_wait_seconds)
    except ValueError:
        pass

    # HTTP-date form.
    try:
        target = parsedate_to_datetime(header)
        if target is None:
            return None
        now = _dt.datetime.now(_dt.timezone.utc)
        if target.tzinfo is None:
            target = target.replace(tzinfo=_dt.timezone.utc)
        delta = (target - now).total_seconds()
        return max(0.0, min(delta, max_wait_seconds))
    except (TypeError, ValueError):
        return None
