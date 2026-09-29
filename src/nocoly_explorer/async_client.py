"""Asynchronous worksheet pagination engine."""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import random
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional, Set

import aiohttp

from .exceptions import AsyncClientError, JobCancelled, NocolyError, PaginationLimitExceeded
from .schema import (
    NocolyWorksheetSchema,
    infer_schema_from_rows,
    parse_columns_response,
)

_LOGGER = logging.getLogger("nocoly_explorer.async")


class AsyncClientError(NocolyError):
    """Base exception for async pagination failures."""


class PaginationLimitExceeded(AsyncClientError):
    """Raised when pagination exceeds the configured max_pages."""


@dataclass(slots=True)
class RetryPolicy:
    """Configuration for retry-on-transient-error behavior."""

    max_retries: int = 5
    max_wait_seconds: float = 30.0
    base_delay: float = 0.5
    max_delay: float = 30.0
    jitter_factor: float = 0.5

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError(f"max_retries must be >= 0 (got {self.max_retries})")
        if self.max_wait_seconds <= 0:
            raise ValueError(f"max_wait_seconds must be > 0 (got {self.max_wait_seconds})")
        if self.base_delay <= 0:
            raise ValueError(f"base_delay must be > 0 (got {self.base_delay})")
        if self.max_delay <= 0:
            raise ValueError(f"max_delay must be > 0 (got {self.max_delay})")
        if not 0.0 <= self.jitter_factor <= 1.0:
            raise ValueError(f"jitter_factor must be in [0, 1] (got {self.jitter_factor})")

    def compute_backoff(self, attempt: int) -> float:
        """Exponential backoff with jitter. attempt=0 → base_delay."""
        delay = min(self.base_delay * (2 ** attempt), self.max_delay)
        jitter = delay * self.jitter_factor * random.random()
        return min(delay + jitter, self.max_delay)

    def parse_retry_after(self, header: Optional[str]) -> Optional[float]:
        """Parse Retry-After header: delta-seconds or HTTP-date. Returns None on parse failure."""
        if header is None:
            return None
        header = header.strip()
        if not header:
            return None
        # delta-seconds form
        try:
            seconds = float(header)
            return min(seconds, self.max_wait_seconds)
        except ValueError:
            pass
        # HTTP-date form
        try:
            target = parsedate_to_datetime(header)
            if target is None:
                return None
            now = datetime.datetime.now(datetime.timezone.utc)
            delta = (target - now).total_seconds()
            return max(0.0, min(delta, self.max_wait_seconds))
        except (TypeError, ValueError):
            return None


def _serialize_filter(criteria: Dict[str, Any]) -> str:
    """Serialize filter criteria to a JSON string for the query parameter."""
    return json.dumps(criteria, default=str)


class TokenBucket:
    """Async token bucket rate limiter."""

    def __init__(self, rate_per_second: float, burst: float = 1.0) -> None:
        if rate_per_second <= 0:
            raise ValueError(f"rate_per_second must be > 0 (got {rate_per_second})")
        if burst <= 0:
            raise ValueError(f"burst must be > 0 (got {burst})")
        self.rate = float(rate_per_second)
        self.burst = float(burst)
        self._tokens = float(burst)
        try:
            self._last = asyncio.get_event_loop().time()
        except RuntimeError:
            # Outside an event loop; lazy init on first acquire.
            self._last = None
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            loop = asyncio.get_event_loop()
            now = loop.time()
            if self._last is None:
                self._last = now
            elapsed = now - self._last
            self._last = now
            self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return
            wait = (1.0 - self._tokens) / self.rate
            self._tokens = 0.0
        await asyncio.sleep(wait)


class AsyncWorksheetClient:
    """Asynchronous worksheet client with bounded concurrency + retry."""

    def __init__(
        self,
        base_url: str,
        auth_token: str,
        worksheet_id: str,
        *,
        max_page_size: int = 1000,
        retry_policy: Optional[RetryPolicy] = None,
        concurrency: int = 8,
        requests_per_second: float = 10.0,
        session: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        if max_page_size < 1 or max_page_size > 1000:
            raise ValueError(f"max_page_size must be in [1, 1000] (got {max_page_size})")
        if concurrency < 1:
            raise ValueError(f"concurrency must be >= 1 (got {concurrency})")
        if requests_per_second <= 0:
            raise ValueError(f"requests_per_second must be > 0 (got {requests_per_second})")
        self.base_url = base_url.rstrip("/")
        self.worksheet_id = worksheet_id
        self.auth_token = auth_token
        self.max_page_size = max_page_size
        self.retry_policy = retry_policy or RetryPolicy()
        self.concurrency = concurrency
        self.requests_per_second = requests_per_second
        self._session: Optional[aiohttp.ClientSession] = session
        self._owns_session = session is None
        self._token_bucket = TokenBucket(requests_per_second, burst=float(concurrency))

    async def __aenter__(self) -> "AsyncWorksheetClient":
        if self._session is None:
            self._session = aiohttp.ClientSession(
                headers={"Authorization": f"Bearer {self.auth_token}"}
            )
        return self

    async def __aexit__(self, *exc) -> None:
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    def _url(self) -> str:
        return f"{self.base_url}/worksheets/{self.worksheet_id}/rows"

    async def _fetch_page(
        self,
        *,
        page: int,
        page_size: int,
        filter_criteria: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch one page, with retry on 429 (Retry-After) and 5xx."""
        if self._session is None:
            raise AsyncClientError(
                "Client not entered; use 'async with AsyncWorksheetClient(...)'."
            )
        params: Dict[str, Any] = {"page": page, "page_size": page_size}
        if filter_criteria:
            params["filter"] = _serialize_filter(filter_criteria)
        last_exc: Optional[BaseException] = None
        for attempt in range(self.retry_policy.max_retries + 1):
            await self._token_bucket.acquire()
            try:
                async with self._session.get(self._url(), params=params) as resp:
                    if resp.status == 429:
                        retry_after = self.retry_policy.parse_retry_after(
                            resp.headers.get("Retry-After")
                        )
                        delay = (
                            retry_after
                            if retry_after is not None
                            else self.retry_policy.compute_backoff(attempt)
                        )
                        await asyncio.sleep(delay)
                        continue
                    if 500 <= resp.status < 600:
                        delay = self.retry_policy.compute_backoff(attempt)
                        _LOGGER.warning(
                            "async.client.5xx",
                            extra={"page": page, "attempt": attempt, "status": resp.status},
                        )
                        await asyncio.sleep(delay)
                        continue
                    if resp.status >= 400:
                        body = await resp.text()
                        raise AsyncClientError(
                            f"HTTP {resp.status} fetching page {page}: {body[:200]}"
                        )
                    payload = await resp.json()
                    return payload.get("rows", []), bool(payload.get("has_more", True))
            except aiohttp.ClientError as exc:
                last_exc = exc
                delay = self.retry_policy.compute_backoff(attempt)
                _LOGGER.warning(
                    "async.client.transport_error",
                    extra={"page": page, "attempt": attempt, "error": str(exc)},
                )
                await asyncio.sleep(delay)
                continue
        raise AsyncClientError(
            f"Page {page} failed after {self.retry_policy.max_retries + 1} attempts; "
            f"last error: {last_exc}"
        )

    async def get_worksheet_schema_async(
        self,
        worksheet_id=None,
        *,
        sample_size=1,
        metadata_endpoint=None,
    ):
        """Async version of WorksheetClient.get_worksheet_schema.

        Tries the Nocoly metadata endpoint first (POST with bearer auth);
        on failure or unrecognized shape, falls back to fetching a small
        page and inferring types from the response.

        Returns a NocolyWorksheetSchema with source="api" on success or
        source="inferred" on the fallback path.
        """
        if self._session is None:
            raise AsyncClientError(
                "Client not entered; use 'async with AsyncWorksheetClient(...)'."
            )
        target_id = worksheet_id or self.worksheet_id
        endpoint = metadata_endpoint or (
            "/api/v3/app/worksheets/" + target_id + "/columns"
        )
        url = self.base_url + endpoint
        try:
            async with self._session.post(url, json={}) as resp:
                if resp.status == 200:
                    try:
                        payload = await resp.json()
                    except Exception:
                        payload = None
                    parsed = (
                        parse_columns_response(payload)
                        if payload is not None
                        else None
                    )
                    if parsed:
                        return NocolyWorksheetSchema(
                            worksheet_id=target_id,
                            columns=parsed,
                            source="api",
                        )
        except aiohttp.ClientError as exc:
            _LOGGER.debug(
                "async.metadata_endpoint_failed",
                extra={"worksheet_id": target_id, "error": str(exc)},
            )

        # Fallback: fetch a small page and infer.
        sample_rows = []
        async for page in self.fetch_pages_async(
            page_size=max(sample_size, 1),
            max_pages=1,
        ):
            sample_rows.extend(page)
            if len(sample_rows) >= sample_size:
                break
        if not sample_rows:
            raise NocolyError(
                "No rows returned from worksheet "
                + repr(target_id)
                + "; cannot infer schema."
            )
        inferred = infer_schema_from_rows(
            sample_rows[: max(sample_size, 1)],
            sample_size=sample_size,
            worksheet_id=target_id,
        )
        return NocolyWorksheetSchema(
            worksheet_id=inferred.worksheet_id,
            columns=inferred.columns,
            source="inferred",
        )

    async def fetch_all_async(
        self,
        *,
        page_size: int = 200,
        max_pages: int = 1000,
        filter_criteria: Optional[Dict[str, Any]] = None,
        cancel_check: Optional[Callable[[], Awaitable[bool]]] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch all pages with bounded concurrency, return rows in original order.

        Stops dispatching new pages once the server signals end-of-data via a
        short/empty page, but waits for in-flight pages to complete (cancel is
        a last resort only if the consumer cancels).

        ``cancel_check`` is an optional async callable returning ``True`` to
        request cooperative cancellation. Awaited at the top of each dispatch
        loop iteration; on ``True`` any in-flight pages are cancelled and
        :class:`JobCancelled` is raised.
        """
        if page_size < 1 or page_size > self.max_page_size:
            raise ValueError(f"page_size must be in [1, {self.max_page_size}] (got {page_size})")
        if max_pages < 1:
            raise ValueError(f"max_pages must be >= 1 (got {max_pages})")
        semaphore = asyncio.Semaphore(self.concurrency)
        pages: Dict[int, List[Dict[str, Any]]] = {}
        next_page = 1
        terminator_page: Optional[int] = None

        async def fetch_one(page_num: int) -> tuple[int, List[Dict[str, Any]], bool]:
            async with semaphore:
                data, has_more = await self._fetch_page(
                    page=page_num, page_size=page_size, filter_criteria=filter_criteria,
                )
                return page_num, data, has_more

        in_flight: Set[asyncio.Task] = set()

        try:
            while next_page <= max_pages or in_flight:
                # Cooperative cancellation: bail before dispatching more pages.
                if cancel_check is not None and await cancel_check():
                    for t in in_flight:
                        t.cancel()
                    if in_flight:
                        await asyncio.gather(*in_flight, return_exceptions=True)
                    raise JobCancelled(
                        "Cancellation requested before completion"
                    )
                # Dispatch new pages up to concurrency (but only while we don't
                # already know the server has ended).
                while (
                    len(in_flight) < self.concurrency
                    and next_page <= max_pages
                    and (terminator_page is None or next_page <= terminator_page)
                ):
                    p = next_page
                    task = asyncio.create_task(fetch_one(p))
                    # Stash the page number on the task for later reference.
                    task._nocoly_page_num = p  # type: ignore[attr-defined]
                    in_flight.add(task)
                    next_page += 1
                if not in_flight:
                    break
                done, _ = await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    page_num, data, has_more = await task
                    pages[page_num] = data
                    if not has_more:
                        if terminator_page is None or page_num < terminator_page:
                            terminator_page = page_num
                    in_flight.discard(task)
                # If we now know the server has ended, cancel any in-flight
                # tasks that haven't completed yet AND whose page number is
                # beyond the terminator. (Completing them is wasteful and risks
                # 5xx retries on servers that error for out-of-range pages.)
                if terminator_page is not None and in_flight:
                    to_cancel = [
                        t for t in in_flight
                        if getattr(t, "_nocoly_page_num", 0) > terminator_page
                    ]
                    for t in to_cancel:
                        t.cancel()
                        in_flight.discard(t)
                    if to_cancel:
                        await asyncio.gather(*to_cancel, return_exceptions=True)
        finally:
            # Final cleanup if the consumer cancelled
            if in_flight:
                for task in in_flight:
                    task.cancel()
                await asyncio.gather(*in_flight, return_exceptions=True)

        if terminator_page is None and next_page > max_pages:
            # We dispatched max_pages pages and the server has not signaled end.
            # Raise to alert the caller that they may be missing data.
            raise PaginationLimitExceeded(
                f"max_pages={max_pages} reached without end-of-data signal; "
                f"consider increasing max_pages or check the server."
            )

        # Order-preserving merge
        all_rows: List[Dict[str, Any]] = []
        for p in sorted(pages):
            if terminator_page is not None and p > terminator_page:
                # Drop pages past the terminator.
                continue
            all_rows.extend(pages[p])
        return all_rows

    async def fetch_pages_async(
        self,
        *,
        page_size: int = 200,
        max_pages: int = 1000,
        filter_criteria: Optional[Dict[str, Any]] = None,
        cancel_check: Optional[Callable[[], Awaitable[bool]]] = None,
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        """Yield pages in order, one at a time.

        ``cancel_check`` is an optional async callable returning ``True`` to
        request cooperative cancellation. Awaited between pages; on ``True``
        the generator raises :class:`JobCancelled` and any in-flight pages
        are cancelled.

        Memory: in-flight bounded by `concurrency × page_size` rows; we may
        buffer up to `max_pages × page_size` rows while waiting for an
        out-of-order early page to complete (rare; usually the page completes
        before the consumer asks for the next iteration).
        """
        if page_size < 1 or page_size > self.max_page_size:
            raise ValueError(f"page_size must be in [1, {self.max_page_size}] (got {page_size})")
        if max_pages < 1:
            raise ValueError(f"max_pages must be >= 1 (got {max_pages})")
        semaphore = asyncio.Semaphore(self.concurrency)
        in_flight: Set[asyncio.Task] = set()
        next_page = 1
        next_yield = 1
        pages_buffer: Dict[int, List[Dict[str, Any]]] = {}

        async def fetch_one(page_num: int) -> tuple[int, List[Dict[str, Any]], bool]:
            async with semaphore:
                data, has_more = await self._fetch_page(
                    page=page_num, page_size=page_size, filter_criteria=filter_criteria,
                )
                return page_num, data, has_more

        try:
            terminator_page: Optional[int] = None
            while next_yield <= max_pages:
                # If we've passed the terminator, stop.
                if terminator_page is not None and next_yield > terminator_page:
                    break
                # Cooperative cancellation: check before dispatching more pages.
                if cancel_check is not None and await cancel_check():
                    for t in in_flight:
                        t.cancel()
                    if in_flight:
                        await asyncio.gather(*in_flight, return_exceptions=True)
                    raise JobCancelled(
                        "Cancellation requested before completion"
                    )
                # Dispatch new pages up to concurrency (but only while we don't
                # already know the server has ended).
                while (
                    len(in_flight) < self.concurrency
                    and next_page <= max_pages
                    and (terminator_page is None or next_page <= terminator_page)
                ):
                    p = next_page
                    task = asyncio.create_task(fetch_one(p))
                    task._nocoly_page_num = p  # type: ignore[attr-defined]
                    in_flight.add(task)
                    next_page += 1
                if not in_flight:
                    break
                done, _ = await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    page_num, data, has_more = await task
                    in_flight.discard(task)
                    pages_buffer[page_num] = data
                    if not has_more:
                        if terminator_page is None or page_num < terminator_page:
                            terminator_page = page_num
                # Yield any contiguous pages from the buffer (oldest first).
                # Stop at the terminator (if known).
                while next_yield in pages_buffer:
                    if terminator_page is not None and next_yield > terminator_page:
                        # Drop pages past the terminator.
                        pages_buffer.pop(next_yield)
                        next_yield += 1
                        continue
                    buffered = pages_buffer.pop(next_yield)
                    yield buffered
                    next_yield += 1
                # Cancel any in-flight tasks beyond the terminator so they don't
                # 5xx-retry indefinitely.
                if terminator_page is not None and in_flight:
                    to_cancel = [
                        t for t in in_flight
                        if getattr(t, "_nocoly_page_num", 0) > terminator_page
                    ]
                    for t in to_cancel:
                        t.cancel()
                        in_flight.discard(t)
                    if to_cancel:
                        await asyncio.gather(*to_cancel, return_exceptions=True)
        finally:
            for task in in_flight:
                task.cancel()
            if in_flight:
                await asyncio.gather(*in_flight, return_exceptions=True)