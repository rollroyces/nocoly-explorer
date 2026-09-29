"""HTTP client for the Nocoly worksheet API."""

from __future__ import annotations

import datetime
import email.utils
import logging
import os
import time
from typing import Any, Dict, List, Optional

from .auth import CredentialPair
from .exceptions import NocolyError
from .schema import (
    NocolyWorksheetSchema,
    infer_schema_from_rows,
    parse_columns_response,
)

try:  # Lazy import to avoid hard dependency if user stubs out HTTP layer
    import requests
except ModuleNotFoundError as exc:  # pragma: no cover - depends on env
    raise RuntimeError("The 'requests' package is required to use WorksheetClient") from exc


# Per Nocoly v3 docs, page size is hard-capped server-side. Validate on the
# client side to fail fast with a clear message.
MAX_PAGE_SIZE = 1000


def _parse_retry_after(value: str) -> Optional[float]:
    """Parse a Retry-After header value (seconds or HTTP-date). Returns None if invalid."""
    if not value:
        return None
    value = value.strip()
    # delta-seconds form
    try:
        seconds = float(value)
        if seconds >= 0:
            return seconds
    except ValueError:
        pass
    # HTTP-date form
    try:
        target = email.utils.parsedate_to_datetime(value)
        if target is None:
            return None
        now = datetime.datetime.now(datetime.timezone.utc)
        if target.tzinfo is None:
            target = target.replace(tzinfo=datetime.timezone.utc)
        delta = (target - now).total_seconds()
        return max(0.0, delta)
    except (ValueError, TypeError):
        return None





def _is_truthy_env(name: str) -> bool:
    """Return True if environment variable ``name`` is set to a truthy value."""
    value = os.environ.get(name)
    if value is None:
        return False
    return value.strip().lower() in ("1", "true", "yes", "on")


def _resolve_try_api(explicit: Optional[bool]) -> bool:
    """Resolve whether the schema API attempt is enabled.

    Order of precedence:
    1. ``explicit`` argument if not None.
    2. ``NOCOLY_SCHEMA_API_DISABLED`` env var (any truthy value disables).
    3. Default: True (API attempt enabled).
    """
    if explicit is not None:
        return bool(explicit)
    if _is_truthy_env("NOCOLY_SCHEMA_API_DISABLED"):
        return False
    return True


class WorksheetClient:
    """Tiny wrapper around the Nocoly v3 worksheet endpoints."""

    ROWS_ENDPOINT = "/api/v3/app/worksheets/{worksheet_id}/rows/list"

    def __init__(
        self,
        host: str,
        worksheet_id: str,
        credentials: CredentialPair,
        timeout_seconds: float = 30.0,
        max_retries: int = 3,
        max_wait_seconds: float = 30.0,
        verify_ssl: bool = True,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.base_url = host.rstrip("/")
        self.worksheet_id = worksheet_id
        self.credentials = credentials
        self.timeout_seconds = timeout_seconds
        self.max_retries = max(0, max_retries)
        self.max_wait_seconds = max(0.0, max_wait_seconds)
        self.verify_ssl = verify_ssl
        self._session = requests.Session()
        self._logger = logger or logging.getLogger(__name__)

    def _build_headers(self) -> Dict[str, str]:
        return {
            "HAP-AppKey": self.credentials.app_key,
            "HAP-Sign": self.credentials.app_sign,
            "Content-Type": "application/json",
        }

    @staticmethod
    def _validate_paging(page_size: int, max_pages: int) -> None:
        if max_pages < 1:
            raise ValueError(
                f"max_pages must be >= 1 (got {max_pages}). "
                f"Use a large value to approximate 'unlimited' pagination."
            )
        if page_size < 1:
            raise ValueError(f"page_size must be >= 1 (got {page_size}).")
        if page_size > MAX_PAGE_SIZE:
            raise ValueError(
                f"page_size {page_size} exceeds Nocoly v3 limit of {MAX_PAGE_SIZE}."
            )

    def fetch_rows(
        self,
        columns: Optional[List[str]] = None,
        filter_criteria: Optional[Dict[str, Any]] = None,
        sorts: Optional[List[Dict[str, Any]]] = None,
        page_size: int = 200,
        view_id: str = "",
        max_pages: int = 1000,
    ) -> List[Dict[str, Any]]:
        """Fetch all rows for the worksheet with optional filters."""
        self._validate_paging(page_size=page_size, max_pages=max_pages)

        payload = {
            "pageSize": page_size,
            "page": 1,
            "filters": filter_criteria or {},
            "columns": columns or [],
            "viewId": view_id,
            "sorts": sorts or [],
        }

        rows: List[Dict[str, Any]] = []
        for page in range(1, max_pages + 1):
            payload["page"] = page
            self._logger.debug(
                "Fetching worksheet page",
                extra={"page": page, "worksheet_id": self.worksheet_id},
            )
            response = self._execute(payload)
            data = response.get("rows", [])
            has_more = response.get("has_more")

            if has_more and not data:
                self._logger.warning(
                    "Worksheet API returned empty page with has_more=True; "
                    "continuing to next page (up to max_pages=%d).",
                    max_pages,
                )

            rows.extend(data)
            # Stop only when the server signals no more pages.
            if not has_more:
                break
        return rows

    def get_worksheet_schema(
        self,
        worksheet_id: Optional[str] = None,
        *,
        sample_size: int = 1,
        metadata_endpoint: Optional[str] = None,
        try_api: Optional[bool] = None,
        timeout_seconds: Optional[float] = None,
    ) -> NocolyWorksheetSchema:
        """Discover the worksheet's column schema and data types.

        Behavior:
        1. **API attempt** (skippable). Calls the Nocoly metadata endpoint
           (``metadata_endpoint`` or the default
           ``/api/v3/app/worksheets/{id}/columns``). On success with a
           recognized response shape, returns ``source="api"``.
        2. **Fallback to inference.** If the API call fails, the endpoint
           doesn't exist, the response is unparseable, or the API attempt
           is disabled, fetches ``sample_size`` rows and infers column
           names + PyArrow-compatible types.

        Returns a :class:`NocolyWorksheetSchema` whose ``source`` field
        indicates which path produced the result.

        CAVEAT — the default endpoint is **unverified**. Nocoly v3's
        documented endpoint set in this repo only covers
        ``/api/v3/app/worksheets/{id}/rows/list``. The default
        ``/columns`` path is an educated guess at a metadata endpoint;
        real Nocoly deployments may use a different URL or expose column
        metadata only through the rows endpoint. Pass an explicit
        ``metadata_endpoint`` once you know the right URL, or set
        ``try_api=False`` to skip the network call entirely.

        Skip the API attempt via either:
        - ``try_api=False`` on the call, or
        - environment variable ``NOCOLY_SCHEMA_API_DISABLED=1`` (any
          truthy value: 1, true, yes, on).

        Each API attempt emits a DEBUG line on the call to its logger
        (``nocoly_explorer.client``) with the URL and HTTP status, so
        you can see exactly what was tried when you tail logs.
        """
        target_id = worksheet_id or self.worksheet_id
        api_enabled = _resolve_try_api(try_api)

        rows_for_inference: Optional[List[Dict[str, Any]]] = None

        if api_enabled:
            endpoint = (
                metadata_endpoint
                or f"/api/v3/app/worksheets/{target_id}/columns"
            )
            url = f"{self.base_url}{endpoint}"
            timeout = max(
                0.0,
                timeout_seconds if timeout_seconds is not None
                else self.timeout_seconds,
            )
            self._logger.debug(
                "schema.try_api_attempt",
                extra={"worksheet_id": target_id, "url": url, "timeout": timeout},
            )
            try:
                response = self._session.post(
                    url,
                    json={},
                    headers=self._build_headers(),
                    timeout=timeout,
                    verify=self.verify_ssl,
                )
                status_code = getattr(response, "status_code", None)
                self._logger.debug(
                    "schema.try_api_response",
                    extra={
                        "worksheet_id": target_id,
                        "url": url,
                        "status": status_code,
                    },
                )
                if response.ok:
                    try:
                        payload = response.json()
                    except ValueError:
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
                    self._logger.debug(
                        "schema.try_api_unrecognized_shape",
                        extra={"worksheet_id": target_id, "url": url},
                    )
            except requests.RequestException as exc:
                self._logger.debug(
                    "schema.try_api_error",
                    extra={
                        "worksheet_id": target_id,
                        "url": url,
                        "error": str(exc),
                    },
                )

            # Fall through to inference. To avoid a second HTTP round-trip
            # for the sample, we re-use the page we would have fetched
            # below — but only if the user asked for exactly the default
            # sample size. If they asked for more, fetch again later.
            if sample_size <= 1:
                try:
                    rows_for_inference = self.fetch_rows(
                        page_size=1, max_pages=1,
                    )
                except NocolyError as exc:
                    self._logger.debug(
                        "schema.inference_fetch_error",
                        extra={"worksheet_id": target_id, "error": str(exc)},
                    )

        # Fallback path: inference.
        if rows_for_inference is None:
            try:
                rows_for_inference = self.fetch_rows(
                    page_size=max(sample_size, 1),
                    max_pages=1,
                )
            except NocolyError as exc:
                self._logger.debug(
                    "schema.inference_fetch_error",
                    extra={"worksheet_id": target_id, "error": str(exc)},
                )
                raise

        sample = rows_for_inference[: max(sample_size, 1)]
        if not sample:
            raise NocolyError(
                f"No rows returned from worksheet {target_id!r}; "
                f"cannot infer schema."
            )
        inferred = infer_schema_from_rows(
            sample, sample_size=sample_size, worksheet_id=target_id
        )
        return NocolyWorksheetSchema(
            worksheet_id=inferred.worksheet_id,
            columns=inferred.columns,
            source="inferred",
        )

    def _execute(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}{self.ROWS_ENDPOINT.format(worksheet_id=self.worksheet_id)}"
        attempt = 0
        wait_time = 1.0
        headers = self._build_headers()
        while True:
            attempt += 1
            try:
                response = self._session.post(
                    url,
                    json=payload,
                    headers=headers,
                    timeout=self.timeout_seconds,
                    verify=self.verify_ssl,
                )
            except requests.RequestException as exc:
                self._logger.warning(
                    "Worksheet API request error, retrying",
                    extra={
                        "attempt": attempt,
                        "worksheet_id": self.worksheet_id,
                        "error": str(exc),
                    },
                )
                if attempt > self.max_retries:
                    raise NocolyError(f"Worksheet API request error: {exc}") from exc
                time.sleep(wait_time)
                wait_time = min(wait_time * 2, self.max_wait_seconds)
                continue

            if response.ok:
                try:
                    return response.json()
                except ValueError as exc:
                    self._logger.error(
                        "Worksheet API returned invalid JSON",
                        extra={
                            "worksheet_id": self.worksheet_id,
                            "status_code": response.status_code,
                        },
                    )
                    raise NocolyError(
                        "Worksheet API returned invalid JSON payload"
                    ) from exc

            # Honor Retry-After header (I6). After honoring, fall back to
            # exponential backoff capped at max_wait_seconds (I5).
            retry_after = _parse_retry_after(response.headers.get("Retry-After", ""))
            if response.status_code in (429, 503) and retry_after is not None:
                self._logger.warning(
                    "Worksheet API rate-limited; honoring Retry-After=%.1fs",
                    retry_after,
                    extra={"worksheet_id": self.worksheet_id, "status_code": response.status_code},
                )
                if attempt > self.max_retries:
                    self._logger.error(
                        "Worksheet API request failed after exhausting retries on rate limit",
                        extra={"worksheet_id": self.worksheet_id, "status_code": response.status_code},
                    )
                    raise NocolyError(
                        f"Worksheet API request failed after {self.max_retries} retries: "
                        f"{response.status_code} {response.text}"
                    )
                time.sleep(min(retry_after, self.max_wait_seconds))
                continue

            if attempt > self.max_retries:
                self._logger.error(
                    "Worksheet API request failed",
                    extra={
                        "status_code": response.status_code,
                        "worksheet_id": self.worksheet_id,
                        "body": response.text[:2000],
                    },
                )
                raise NocolyError(
                    f"Worksheet API request failed after {self.max_retries} retries: "
                    f"{response.status_code} {response.text}"
                )
            time.sleep(wait_time)
            wait_time = min(wait_time * 2, self.max_wait_seconds)