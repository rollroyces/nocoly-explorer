"""Synchronous worksheet writer.

This module is the write-side counterpart to :mod:`nocoly_explorer.client`.
It batches rows and POSTs/PUTs/DELETEs them to Nocoly's worksheet-write
endpoints.

.. important::

   The default endpoint paths in this module are **educated guesses** — the
   only Nocoly endpoint documented in this repository is the read endpoint
   ``POST /api/v3/app/worksheets/{id}/rows/list``. The write endpoints
   here mirror common REST conventions and the read-side path shape, but
   real Nocoly deployments may differ. Override ``add_endpoint`` /
   ``update_endpoint`` / ``upsert_endpoint`` / ``delete_endpoint`` (either
   per instance or by subclassing) once you know the real URLs.

   See the README's "Honest gaps" section for the same caveat on the
   metadata endpoint.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Union,
)

from .auth import CredentialPair
from .backoff import compute_backoff, parse_retry_after
from .exceptions import (
    NocolyWriteError,
    WriteBatchError,
    WriteBatchFailureInfo,
    WriteValidationError,
)
from .writer_inputs import coerce_to_rows, normalize_batch_size

try:  # Lazy import — the writer is unusable without requests, but a stub
    import requests
except ModuleNotFoundError as exc:  # pragma: no cover - depends on env
    raise RuntimeError(
        "The 'requests' package is required to use WorksheetWriter"
    ) from exc


_LOGGER = logging.getLogger("nocoly_explorer.writer")


# ---------------------------------------------------------------------------
# Public dataclass
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class WriteResult:
    """Outcome of a single write operation (add/update/upsert/delete).

    ``failures`` is populated only when ``fail_fast=False`` was passed to
    the writer call — otherwise the first failure is raised as a
    :class:`WriteBatchError` and no result is returned.
    """

    operation: str
    rows_attempted: int
    rows_succeeded: int
    batches_sent: int
    batches_failed: int
    failures: List[WriteBatchFailureInfo] = field(default_factory=list)
    # Optional server-side identifiers returned by Nocoly per batch (rare;
    # most Nocoly-shaped endpoints echo a status, not ids).
    response_payloads: List[Any] = field(default_factory=list)


# Public alias used by AsyncWorksheetWriter to avoid leaking private helpers.
_coerce_input = coerce_to_rows


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------


class WorksheetWriter:
    """Synchronous batched worksheet writer.

    Supports four operations:

    - :meth:`add_rows` — append new rows.
    - :meth:`update_rows` — patch existing rows by ``key_column``.
    - :meth:`upsert_rows` — add-or-update by ``key_column``.
    - :meth:`delete_rows` — remove rows by id list or filter.

    All four accept any of the input formats supported by
    :func:`coerce_to_rows` (``List[dict]``, ``pandas.DataFrame``,
    ``pyarrow.Table`` / ``pyarrow.RecordBatch``, or a file path that
    ``pyarrow`` can read as a table). The writer internally coerces to
    ``List[dict]`` and chunks into HTTP requests of ``batch_size`` rows
    each (default 500; hard-capped at 1000 to match the Nocoly read-side
    ``MAX_PAGE_SIZE``).
    """

    # --- Default endpoint paths. Override per-instance or via subclass. ---
    ADD_ENDPOINT = "/api/v3/app/worksheets/{worksheet_id}/rows"
    UPSERT_ENDPOINT = "/api/v3/app/worksheets/{worksheet_id}/rows/upsert"
    UPDATE_ENDPOINT = "/api/v3/app/worksheets/{worksheet_id}/rows"
    DELETE_ENDPOINT = "/api/v3/app/worksheets/{worksheet_id}/rows/delete"

    # Batches above this are rejected — Nocoly's documented read-side cap is
    # 1000 rows per request; we use the same ceiling for writes.
    MAX_BATCH_SIZE = 1000

    def __init__(
        self,
        host: str,
        worksheet_id: str,
        credentials: CredentialPair,
        *,
        timeout_seconds: float = 30.0,
        max_retries: int = 3,
        max_wait_seconds: float = 30.0,
        verify_ssl: bool = True,
        batch_size: int = 500,
        add_endpoint: Optional[str] = None,
        update_endpoint: Optional[str] = None,
        upsert_endpoint: Optional[str] = None,
        delete_endpoint: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        if not host:
            raise ValueError("host is required")
        if not worksheet_id:
            raise ValueError("worksheet_id is required")
        if not credentials or not credentials.app_key or not credentials.app_sign:
            raise WriteValidationError(
                "credentials.app_key and credentials.app_sign must both be set"
            )
        if batch_size < 1:
            raise ValueError(f"batch_size must be >= 1 (got {batch_size})")
        if batch_size > self.MAX_BATCH_SIZE:
            raise ValueError(
                f"batch_size {batch_size} exceeds Nocoly limit of {self.MAX_BATCH_SIZE}"
            )
        if max_retries < 0:
            raise ValueError(f"max_retries must be >= 0 (got {max_retries})")

        self.base_url = host.rstrip("/")
        self.worksheet_id = worksheet_id
        self.credentials = credentials
        self.timeout_seconds = max(0.0, timeout_seconds)
        self.max_retries = max(0, max_retries)
        self.max_wait_seconds = max(0.0, max_wait_seconds)
        self.verify_ssl = verify_ssl
        self.batch_size = normalize_batch_size(batch_size, self.MAX_BATCH_SIZE)
        self.add_endpoint = add_endpoint or self.ADD_ENDPOINT
        self.update_endpoint = update_endpoint or self.UPDATE_ENDPOINT
        self.upsert_endpoint = upsert_endpoint or self.UPSERT_ENDPOINT
        self.delete_endpoint = delete_endpoint or self.DELETE_ENDPOINT
        self._session = requests.Session()
        self._logger = logger or _LOGGER

    # --- Header / URL helpers -------------------------------------------------

    def _headers(self) -> Dict[str, str]:
        return {
            "HAP-AppKey": self.credentials.app_key,
            "HAP-Sign": self.credentials.app_sign,
            "Content-Type": "application/json",
        }

    def _url(self, endpoint_template: str) -> str:
        path = endpoint_template.format(worksheet_id=self.worksheet_id)
        return f"{self.base_url}{path}"

    # --- Public operation methods --------------------------------------------

    def add_rows(
        self,
        rows: Union[
            Sequence[Mapping[str, Any]], Any, str  # pandas/pyarrow/CSV path
        ],
        *,
        batch_size: Optional[int] = None,
        fail_fast: bool = True,
    ) -> WriteResult:
        """Add new rows to the worksheet.

        Parameters
        ----------
        rows
            Any input accepted by :func:`coerce_to_rows` (list of dicts,
            ``pandas.DataFrame``, ``pyarrow.Table`` / ``RecordBatch``, or
            a local file path that ``pyarrow`` can read).
        batch_size
            Override the instance default. Capped at
            :attr:`MAX_BATCH_SIZE`.
        fail_fast
            If ``True`` (default), the first batch failure aborts and
            raises :class:`WriteBatchError`. If ``False``, all batches
            are attempted and failures are aggregated into the returned
            :class:`WriteResult.failures`.
        """
        coerced = coerce_to_rows(rows, operation="add")
        chunks = self._chunk(coerced, batch_size=batch_size)
        return self._execute_batches(
            operation="add",
            chunks=chunks,
            http_method="POST",
            endpoint=self.add_endpoint,
            payload_builder=lambda batch: {"rows": list(batch)},
            fail_fast=fail_fast,
        )

    def update_rows(
        self,
        rows: Union[Sequence[Mapping[str, Any]], Any, str],
        *,
        key_column: str,
        batch_size: Optional[int] = None,
        fail_fast: bool = True,
    ) -> WriteResult:
        """Patch existing rows matched on ``key_column``.

        ``rows`` must be a non-empty sequence whose every element carries
        a value for ``key_column``. Rows missing the key are rejected
        client-side with :class:`WriteValidationError` (or, with
        ``fail_fast=False``, recorded in the result).
        """
        if not key_column or not isinstance(key_column, str):
            raise WriteValidationError(
                "key_column is required for update_rows and must be a non-empty string"
            )
        coerced = coerce_to_rows(rows, operation="update")
        self._validate_key_present(coerced, key_column)
        chunks = self._chunk(coerced, batch_size=batch_size)
        return self._execute_batches(
            operation="update",
            chunks=chunks,
            http_method="PUT",
            endpoint=self.update_endpoint,
            payload_builder=lambda batch: {
                "rows": list(batch),
                "keyColumn": key_column,
            },
            fail_fast=fail_fast,
        )

    def upsert_rows(
        self,
        rows: Union[Sequence[Mapping[str, Any]], Any, str],
        *,
        key_column: str,
        batch_size: Optional[int] = None,
        fail_fast: bool = True,
    ) -> WriteResult:
        """Add-or-update rows matched on ``key_column``.

        Same semantics as :meth:`update_rows` for the key-column
        requirement; the server decides whether each incoming row is
        inserted or patched.
        """
        if not key_column or not isinstance(key_column, str):
            raise WriteValidationError(
                "key_column is required for upsert_rows and must be a non-empty string"
            )
        coerced = coerce_to_rows(rows, operation="upsert")
        self._validate_key_present(coerced, key_column)
        chunks = self._chunk(coerced, batch_size=batch_size)
        return self._execute_batches(
            operation="upsert",
            chunks=chunks,
            http_method="POST",
            endpoint=self.upsert_endpoint,
            payload_builder=lambda batch: {
                "rows": list(batch),
                "keyColumn": key_column,
            },
            fail_fast=fail_fast,
        )

    def delete_rows(
        self,
        *,
        row_ids: Optional[Sequence[Any]] = None,
        filter_criteria: Optional[Mapping[str, Any]] = None,
        batch_size: Optional[int] = None,
        fail_fast: bool = True,
    ) -> WriteResult:
        """Delete rows by id list or by server-side filter.

        Exactly one of ``row_ids`` / ``filter_criteria`` must be set. With
        ``row_ids``, the writer chunks the ids; with ``filter_criteria``,
        a single batch is sent (Nocoly's documented read filter shape is
        used unchanged).
        """
        if (row_ids is None) == (filter_criteria is None):
            raise WriteValidationError(
                "delete_rows requires exactly one of row_ids or filter_criteria"
            )
        if row_ids is not None:
            id_list = [v for v in row_ids if v is not None]
            if not id_list:
                raise WriteValidationError("row_ids must contain at least one value")
            coerced_ids: List[Any] = [self._coerce_id(v) for v in id_list]
            chunks: List[List[Any]] = self._chunk(coerced_ids, batch_size=batch_size)
            return self._execute_batches(
                operation="delete",
                chunks=chunks,
                http_method="POST",
                endpoint=self.delete_endpoint,
                payload_builder=lambda batch: {"rowIds": list(batch)},
                fail_fast=fail_fast,
            )
        # filter path
        return self._execute_batches(
            operation="delete",
            chunks=[[dict(filter_criteria)]],
            http_method="POST",
            endpoint=self.delete_endpoint,
            payload_builder=lambda batch: {"filter": batch[0]},
            fail_fast=fail_fast,
        )

    # --- Internal helpers ----------------------------------------------------

    @staticmethod
    def _coerce_id(value: Any) -> Any:
        """Coerce an id to a JSON-friendly primitive (str/int/float/bool)."""
        if isinstance(value, (str, float, int, bool)):
            return value
        # Fall back to string for UUIDs, Decimals, datetimes, etc. — Nocoly's
        # write endpoints almost universally accept string ids.
        return str(value)

    @staticmethod
    def _validate_key_present(rows: Sequence[Mapping[str, Any]], key: str) -> None:
        if not rows:
            raise WriteValidationError("rows must contain at least one element")
        missing = [i for i, r in enumerate(rows) if key not in r or r.get(key) is None]
        if missing:
            preview = ", ".join(str(i) for i in missing[:5])
            raise WriteValidationError(
                f"{len(missing)} row(s) missing key column {key!r} "
                f"(first indices: {preview})"
            )

    def _chunk(
        self,
        items: Sequence[Any],
        *,
        batch_size: Optional[int],
    ) -> List[List[Any]]:
        bs = normalize_batch_size(batch_size or self.batch_size, self.MAX_BATCH_SIZE)
        if not items:
            return []
        return [list(items[i : i + bs]) for i in range(0, len(items), bs)]

    def _execute_batches(
        self,
        *,
        operation: str,
        chunks: List[List[Any]],
        http_method: str,
        endpoint: str,
        payload_builder: Any,
        fail_fast: bool,
    ) -> WriteResult:
        if not chunks:
            return WriteResult(
                operation=operation,
                rows_attempted=0,
                rows_succeeded=0,
                batches_sent=0,
                batches_failed=0,
            )

        url = self._url(endpoint)
        headers = self._headers()
        failures: List[WriteBatchFailureInfo] = []
        response_payloads: List[Any] = []
        rows_succeeded = 0
        batches_failed = 0

        for index, batch in enumerate(chunks):
            payload = payload_builder(batch)
            attempt = 0
            wait_time = 1.0
            succeeded = False
            last_status: int | None = None
            last_body = ""
            last_error: Optional[str] = None

            while True:
                attempt += 1
                self._logger.debug(
                    "writer.batch_attempt",
                    extra={
                        "operation": operation,
                        "worksheet_id": self.worksheet_id,
                        "batch_index": index,
                        "batch_size": len(batch),
                        "attempt": attempt,
                    },
                )
                try:
                    response = self._session.request(
                        http_method,
                        url,
                        json=payload,
                        headers=headers,
                        timeout=self.timeout_seconds,
                        verify=self.verify_ssl,
                    )
                except requests.RequestException as exc:
                    last_error = str(exc)
                    self._logger.warning(
                        "writer.request_error",
                        extra={
                            "operation": operation,
                            "worksheet_id": self.worksheet_id,
                            "batch_index": index,
                            "attempt": attempt,
                            "error": last_error,
                        },
                    )
                    if attempt > self.max_retries:
                        break
                    time.sleep(wait_time)
                    wait_time = min(
                        compute_backoff(
                            attempt=attempt,
                            base_delay=1.0,
                            max_delay=self.max_wait_seconds,
                            max_wait_seconds=self.max_wait_seconds,
                        ),
                        self.max_wait_seconds,
                    )
                    continue

                last_status = response.status_code
                last_body = response.text[:2000]

                if response.ok:
                    rows_succeeded += len(batch)
                    try:
                        response_payloads.append(response.json())
                    except ValueError:
                        response_payloads.append(None)
                    succeeded = True
                    break

                retry_after = parse_retry_after(
                    response.headers.get("Retry-After", ""),
                    max_wait_seconds=self.max_wait_seconds,
                )
                if response.status_code in (429, 503) and retry_after is not None:
                    self._logger.warning(
                        "writer.rate_limited",
                        extra={
                            "operation": operation,
                            "worksheet_id": self.worksheet_id,
                            "batch_index": index,
                            "status_code": response.status_code,
                            "retry_after": retry_after,
                        },
                    )
                    if attempt > self.max_retries:
                        break
                    time.sleep(min(retry_after, self.max_wait_seconds))
                    continue

                if attempt > self.max_retries:
                    break
                # Generic exponential backoff for 5xx / unexpected errors.
                if 500 <= response.status_code < 600:
                    time.sleep(
                        compute_backoff(
                            attempt=attempt,
                            base_delay=1.0,
                            max_delay=self.max_wait_seconds,
                            max_wait_seconds=self.max_wait_seconds,
                        )
                    )
                    continue

                # 4xx (non-retriable): break immediately.
                break

            if not succeeded:
                batches_failed += 1
                failures.append(
                    WriteBatchFailureInfo(
                        batch_index=index,
                        status_code=last_status,
                        body=last_body,
                        error=last_error,
                    )
                )
                if fail_fast:
                    raise WriteBatchError(
                        (
                            f"{operation} batch {index} failed "
                            f"(status={last_status}, error={last_error!r})"
                        ),
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


# ---------------------------------------------------------------------------
# Backwards-compatible helpers (re-exported from writer_inputs).
# ---------------------------------------------------------------------------

# These aliases let loaders decide between sync and async behavior of the
# underlying coerce helper without exposing the helper module directly.
__all__ = [
    "WorksheetWriter",
    "WriteResult",
    "coerce_to_rows",
]