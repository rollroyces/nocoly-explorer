"""Asynchronous worksheet writer.

Companion to :mod:`nocoly_explorer.writer`. Same four operations, but
batches are dispatched concurrently with a shared :class:`TokenBucket` and
:class:`RetryPolicy` — mirroring how ``AsyncWorksheetClient`` paginates
reads.

The default endpoint paths match :class:`WorksheetWriter`'s. Both classes
accept ``add_endpoint`` / ``update_endpoint`` / ``upsert_endpoint`` /
``delete_endpoint`` overrides, which is the supported way to point at a
real Nocoly deployment once its write URLs are known.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from dataclasses import dataclass, field
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Union,
)

import aiohttp

from .async_client import RetryPolicy, TokenBucket
from .exceptions import (
    NocolyWriteError,
    WriteBatchError,
    WriteBatchFailureInfo,
    WriteValidationError,
)
from .writer import WriteResult
from .writer_inputs import (
    chunk_into_batches,
    coerce_id,
    coerce_to_rows,
    encode_payload,
    normalize_batch_size,
    validate_key_present,
)

_LOGGER = logging.getLogger("nocoly_explorer.async_writer")


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class AsyncWriteConfig:
    """Runtime knobs for the async writer (passed in to bound resources)."""

    concurrency: int = 4
    requests_per_second: float = 10.0
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)


class AsyncWorksheetWriter:
    """Asynchronous batched worksheet writer with bounded concurrency."""

    # Mirror the sync class defaults so users can subclass one place and
    # get matching defaults for both flavors.
    ADD_ENDPOINT = "/api/v3/app/worksheets/{worksheet_id}/rows"
    UPSERT_ENDPOINT = "/api/v3/app/worksheets/{worksheet_id}/rows/upsert"
    UPDATE_ENDPOINT = "/api/v3/app/worksheets/{worksheet_id}/rows"
    DELETE_ENDPOINT = "/api/v3/app/worksheets/{worksheet_id}/rows/delete"
    MAX_BATCH_SIZE = 1000

    def __init__(
        self,
        base_url: str,
        auth_token: str,
        worksheet_id: str,
        *,
        batch_size: int = 500,
        config: Optional[AsyncWriteConfig] = None,
        add_endpoint: Optional[str] = None,
        update_endpoint: Optional[str] = None,
        upsert_endpoint: Optional[str] = None,
        delete_endpoint: Optional[str] = None,
        session: Optional[aiohttp.ClientSession] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        if not base_url:
            raise ValueError("base_url is required")
        if not auth_token:
            raise ValueError("auth_token is required")
        if not worksheet_id:
            raise ValueError("worksheet_id is required")
        if batch_size < 1 or batch_size > self.MAX_BATCH_SIZE:
            raise ValueError(
                f"batch_size must be in [1, {self.MAX_BATCH_SIZE}] (got {batch_size})"
            )

        cfg = config or AsyncWriteConfig()
        if cfg.concurrency < 1:
            raise ValueError(f"concurrency must be >= 1 (got {cfg.concurrency})")
        if cfg.requests_per_second <= 0:
            raise ValueError(
                f"requests_per_second must be > 0 (got {cfg.requests_per_second})"
            )

        self.base_url = base_url.rstrip("/")
        self.auth_token = auth_token
        self.worksheet_id = worksheet_id
        self.batch_size = normalize_batch_size(batch_size, self.MAX_BATCH_SIZE)
        self.config = cfg
        self.add_endpoint = add_endpoint or self.ADD_ENDPOINT
        self.update_endpoint = update_endpoint or self.UPDATE_ENDPOINT
        self.upsert_endpoint = upsert_endpoint or self.UPSERT_ENDPOINT
        self.delete_endpoint = delete_endpoint or self.DELETE_ENDPOINT
        self._session: Optional[aiohttp.ClientSession] = session
        self._owns_session = session is None
        self._token_bucket = TokenBucket(cfg.requests_per_second, burst=float(cfg.concurrency))
        self._logger = logger or _LOGGER

    # --- Header / URL helpers -------------------------------------------------

    def _hap_headers(self) -> Dict[str, str]:
        token = self.auth_token
        if ":" in token:
            app_key, _, app_sign = token.partition(":")
        else:
            app_key = app_sign = token
        return {
            "HAP-AppKey": app_key,
            "HAP-Sign": app_sign,
            "Content-Type": "application/json",
        }

    def _url(self, endpoint_template: str) -> str:
        path = endpoint_template.format(worksheet_id=self.worksheet_id)
        return f"{self.base_url}{path}"

    async def __aenter__(self) -> "AsyncWorksheetWriter":
        if self._session is None:
            self._session = aiohttp.ClientSession(headers=self._hap_headers())
        return self

    async def __aexit__(self, *exc) -> None:
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    # --- Public operation methods --------------------------------------------

    async def add_rows(
        self,
        rows: Union[Sequence[Mapping[str, Any]], Any, str],
        *,
        batch_size: Optional[int] = None,
        fail_fast: bool = True,
        cancel_check: Optional[Callable[[], Awaitable[bool]]] = None,
    ) -> WriteResult:
        """Add new rows concurrently. See :meth:`WorksheetWriter.add_rows`."""
        coerced = coerce_to_rows(rows, operation="add")
        chunks = chunk_into_batches(
            coerced,
            batch_size=batch_size or self.batch_size,
            max_batch_size=self.MAX_BATCH_SIZE,
        )
        return await self._execute_batches(
            operation="add",
            chunks=chunks,
            http_method="POST",
            endpoint=self.add_endpoint,
            payload_builder=lambda batch: {"rows": list(batch)},
            fail_fast=fail_fast,
            cancel_check=cancel_check,
        )

    async def update_rows(
        self,
        rows: Union[Sequence[Mapping[str, Any]], Any, str],
        *,
        key_column: str,
        batch_size: Optional[int] = None,
        fail_fast: bool = True,
        cancel_check: Optional[Callable[[], Awaitable[bool]]] = None,
    ) -> WriteResult:
        """Update rows concurrently. See :meth:`WorksheetWriter.update_rows`."""
        if not key_column or not isinstance(key_column, str):
            raise WriteValidationError(
                "key_column is required for update_rows and must be a non-empty string"
            )
        coerced = coerce_to_rows(rows, operation="update")
        validate_key_present(coerced, key_column)
        chunks = chunk_into_batches(
            coerced,
            batch_size=batch_size or self.batch_size,
            max_batch_size=self.MAX_BATCH_SIZE,
        )
        return await self._execute_batches(
            operation="update",
            chunks=chunks,
            http_method="PUT",
            endpoint=self.update_endpoint,
            payload_builder=lambda batch: {
                "rows": list(batch),
                "keyColumn": key_column,
            },
            fail_fast=fail_fast,
            cancel_check=cancel_check,
        )

    async def upsert_rows(
        self,
        rows: Union[Sequence[Mapping[str, Any]], Any, str],
        *,
        key_column: str,
        batch_size: Optional[int] = None,
        fail_fast: bool = True,
        cancel_check: Optional[Callable[[], Awaitable[bool]]] = None,
    ) -> WriteResult:
        """Upsert rows concurrently. See :meth:`WorksheetWriter.upsert_rows`."""
        if not key_column or not isinstance(key_column, str):
            raise WriteValidationError(
                "key_column is required for upsert_rows and must be a non-empty string"
            )
        coerced = coerce_to_rows(rows, operation="upsert")
        validate_key_present(coerced, key_column)
        chunks = chunk_into_batches(
            coerced,
            batch_size=batch_size or self.batch_size,
            max_batch_size=self.MAX_BATCH_SIZE,
        )
        return await self._execute_batches(
            operation="upsert",
            chunks=chunks,
            http_method="POST",
            endpoint=self.upsert_endpoint,
            payload_builder=lambda batch: {
                "rows": list(batch),
                "keyColumn": key_column,
            },
            fail_fast=fail_fast,
            cancel_check=cancel_check,
        )

    async def delete_rows(
        self,
        *,
        row_ids: Optional[Sequence[Any]] = None,
        filter_criteria: Optional[Mapping[str, Any]] = None,
        batch_size: Optional[int] = None,
        fail_fast: bool = True,
        cancel_check: Optional[Callable[[], Awaitable[bool]]] = None,
    ) -> WriteResult:
        """Delete rows concurrently. See :meth:`WorksheetWriter.delete_rows`."""
        if (row_ids is None) == (filter_criteria is None):
            raise WriteValidationError(
                "delete_rows requires exactly one of row_ids or filter_criteria"
            )
        if row_ids is not None:
            id_list = [v for v in row_ids if v is not None]
            if not id_list:
                raise WriteValidationError("row_ids must contain at least one value")
            coerced_ids = [coerce_id(v) for v in id_list]
            chunks = chunk_into_batches(
                coerced_ids,
                batch_size=batch_size or self.batch_size,
                max_batch_size=self.MAX_BATCH_SIZE,
            )
            return await self._execute_batches(
                operation="delete",
                chunks=chunks,
                http_method="POST",
                endpoint=self.delete_endpoint,
                payload_builder=lambda batch: {"rowIds": list(batch)},
                fail_fast=fail_fast,
                cancel_check=cancel_check,
            )
        return await self._execute_batches(
            operation="delete",
            chunks=[[dict(filter_criteria)]],
            http_method="POST",
            endpoint=self.delete_endpoint,
            payload_builder=lambda batch: {"filter": batch[0]},
            fail_fast=fail_fast,
            cancel_check=cancel_check,
        )

    # --- Internal helpers ----------------------------------------------------

    async def _execute_batches(
        self,
        *,
        operation: str,
        chunks: List[List[Any]],
        http_method: str,
        endpoint: str,
        payload_builder: Callable[[List[Any]], Dict[str, Any]],
        fail_fast: bool,
        cancel_check: Optional[Callable[[], Awaitable[bool]]],
    ) -> WriteResult:
        if not chunks:
            return WriteResult(
                operation=operation,
                rows_attempted=0,
                rows_succeeded=0,
                batches_sent=0,
                batches_failed=0,
            )

        if self._session is None:
            raise NocolyWriteError(
                "Client not entered; use 'async with AsyncWorksheetWriter(...)'."
            )

        semaphore = asyncio.Semaphore(self.config.concurrency)
        results: List[Any] = [None] * len(chunks)

        async def run_with_semaphore(idx: int, batch: List[Any]) -> None:
            # Check cancellation BEFORE grabbing a semaphore slot. Otherwise a
            # task queued on the semaphore can be asked to wait for the next
            # batch to finish before observing the cancel signal.
            if cancel_check is not None and await cancel_check():
                return ("__cancelled__", idx)
            async with semaphore:
                # Re-check inside the critical section: another task that
                # ran first may have set cancel after we passed the outer
                # check. Cheap, and keeps cancellation latency at one
                # inflight request instead of one per batch.
                if cancel_check is not None and await cancel_check():
                    return ("__cancelled__", idx)
                results[idx] = await self._send_batch(
                    operation=operation,
                    batch=batch,
                    http_method=http_method,
                    endpoint=endpoint,
                    payload_builder=payload_builder,
                )

        tasks = [
            asyncio.create_task(run_with_semaphore(i, batch))
            for i, batch in enumerate(chunks)
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        rows_succeeded = 0
        batches_failed = 0
        failures: List[WriteBatchFailureInfo] = []
        response_payloads: List[Any] = []
        cancelled_observed = False

        for idx, outcome in enumerate(results):
            if outcome == "__cancelled__":
                cancelled_observed = True
                continue
            if outcome is None:
                # Task was cancelled mid-flight; treat as failure to be safe.
                batches_failed += 1
                failures.append(
                    WriteBatchFailureInfo(
                        batch_index=idx,
                        status_code=None,
                        error="cancelled before completion",
                    )
                )
                continue
            payload, info = outcome
            response_payloads.append(payload)
            if info is not None:
                batches_failed += 1
                failures.append(info)
            else:
                rows_succeeded += len(chunks[idx])

        if cancelled_observed and not failures:
            raise NocolyWriteError(
                f"{operation}: cancellation requested before completion"
            )

        if fail_fast and failures:
            raise WriteBatchError(
                f"{operation}: {len(failures)} batch(es) failed",
                failures=failures,
            )

        return WriteResult(
            operation=operation,
            rows_attempted=sum(len(c) for c in chunks),
            rows_succeeded=rows_succeeded,
            batches_sent=len(chunks),
            batches_failed=batches_failed,
            failures=failures,
            response_payloads=response_payloads,
        )

    async def _send_batch(
        self,
        *,
        operation: str,
        batch: List[Any],
        http_method: str,
        endpoint: str,
        payload_builder: Callable[[List[Any]], Dict[str, Any]],
    ) -> Any:
        """Send one batch with retry. Returns ``(payload, None)`` on success
        or ``(None, WriteBatchFailureInfo)`` on terminal failure.
        """
        url = self._url(endpoint)
        headers = self._hap_headers()
        payload = payload_builder(batch)
        # Pre-serialize so the body can carry datetime / Decimal / UUID /
        # numpy scalars without falling over stdlib ``json.dumps``. Both
        # ``requests`` and ``aiohttp`` happily accept a ``data=`` string
        # alongside ``Content-Type: application/json``.
        body = encode_payload(payload)
        rp = self.config.retry_policy
        last_exc: Optional[BaseException] = None
        last_status: Optional[int] = None
        last_body = ""

        for attempt in range(rp.max_retries + 1):
            await self._token_bucket.acquire()
            try:
                async with self._session.request(  # type: ignore[union-attr]
                    http_method, url, data=body, headers=headers
                ) as resp:
                    if resp.status == 429:
                        retry_after = rp.parse_retry_after(
                            resp.headers.get("Retry-After")
                        )
                        delay = (
                            retry_after
                            if retry_after is not None
                            else rp.compute_backoff(attempt)
                        )
                        await asyncio.sleep(delay)
                        continue
                    if 500 <= resp.status < 600:
                        last_status = resp.status
                        last_body = (await resp.text())[:2000]
                        self._logger.warning(
                            "async_writer.5xx",
                            extra={
                                "operation": operation,
                                "worksheet_id": self.worksheet_id,
                                "attempt": attempt,
                                "status": resp.status,
                            },
                        )
                        await asyncio.sleep(rp.compute_backoff(attempt))
                        continue
                    if resp.status >= 400:
                        last_status = resp.status
                        last_body = (await resp.text())[:2000]
                        break
                    try:
                        body_json = await resp.json()
                    except Exception:
                        body_json = None
                    return body_json, None
            except aiohttp.ClientError as exc:
                last_exc = exc
                self._logger.warning(
                    "async_writer.transport_error",
                    extra={
                        "operation": operation,
                        "worksheet_id": self.worksheet_id,
                        "attempt": attempt,
                        "error": str(exc),
                    },
                )
                await asyncio.sleep(rp.compute_backoff(attempt))
                continue

        return None, WriteBatchFailureInfo(
            batch_index=-1,
            status_code=last_status,
            body=last_body,
            error=str(last_exc) if last_exc else None,
        )


__all__ = [
    "AsyncWorksheetWriter",
    "AsyncWriteConfig",
    "WriteResult",
]