"""Custom exception hierarchy for the Nocoly explorer package.

Every exception carries a stable string ``code`` (e.g. ``"job_not_found"``)
so callers can match on it without relying on the exception class. Codes
mirror the exception class name in snake_case and are documented in the
README under "Error codes". **Codes never change once shipped** - new
failure modes get new codes; old codes stay even if a class is renamed.
"""

from __future__ import annotations


class NocolyError(Exception):
    """Base class for all custom errors. Carries no code itself; subclasses do."""

    code: str = "nocoly_error"


class MissingCredentialsError(NocolyError):
    """Raised when app_key/app_sign cannot be resolved."""

    code = "missing_credentials"


class OutputValidationError(NocolyError):
    """Raised when output parameters are inconsistent."""

    code = "output_validation"


class EnvironmentDetectionError(NocolyError):
    """Raised when the runtime mode cannot be determined."""

    code = "environment_detection"


class SchemaDriftError(NocolyError):
    """Raised when the API response schema diverges from the declared one."""

    code = "schema_drift"


class CardinalityExceededError(NocolyError):
    """Raised when a partition column exceeds the configured max distinct values."""

    code = "cardinality_exceeded"


class AsyncClientError(NocolyError):
    """Base exception for async pagination failures."""

    code = "async_client_error"


class PaginationLimitExceeded(AsyncClientError):
    """Raised when pagination exceeds the configured max_pages."""

    code = "pagination_limit_exceeded"


class ServiceError(NocolyError):
    """Base exception for service-layer failures."""

    code = "service_error"


class JobNotFound(ServiceError):
    """Raised when a job_id does not exist in Redis."""

    code = "job_not_found"


class JobNotReady(ServiceError):
    """Raised when /result is fetched before job completion."""

    code = "job_not_ready"


class JobCancelled(NocolyError):
    """Raised when a cooperative ``cancel_check`` returns ``True`` mid-pagination.

    ``stream_async`` and ``fetch_all_async`` raise this when their
    ``cancel_check`` callable flips to ``True``. Catch ``NocolyError`` for
    general handling, or ``JobCancelled`` for cancellation-specific paths.
    """

    code = "job_cancelled"


class NocolyWriteError(NocolyError):
    """Base exception for write (add/update/upsert/delete) failures."""

    code = "nocoly_write_error"


class WriteValidationError(NocolyWriteError):
    """Raised when write inputs are invalid (wrong type, missing key, empty payload).

    This is a client-side guard — the request was never sent. Catch it to
    distinguish "fix your call" from "the server rejected it".
    """

    code = "write_validation"


class WriteBatchError(NocolyWriteError):
    """Raised when one or more batches in a write call failed.

    The exception carries ``failures``: a list of
    :class:`WriteBatchFailureInfo` describing which batch failed and why.
    Use ``exc.failures`` for partial-success reporting when the caller
    passed ``fail_fast=False`` on the write call.
    """

    code = "write_batch"

    def __init__(
        self,
        message: str,
        *,
        failures: list["WriteBatchFailureInfo"] | None = None,
    ) -> None:
        super().__init__(message)
        self.failures: list[WriteBatchFailureInfo] = list(failures or [])


class WriteBatchFailureInfo:
    """One failed batch in a :class:`WriteBatchError`.

    Attributes are intentionally simple so the failure can be logged or
    JSON-serialized without dragging the exception along.
    """

    __slots__ = ("batch_index", "status_code", "body", "error")

    def __init__(
        self,
        *,
        batch_index: int,
        status_code: int | None,
        body: str = "",
        error: str | None = None,
    ) -> None:
        self.batch_index = batch_index
        self.status_code = status_code
        self.body = body
        self.error = error

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return (
            f"WriteBatchFailureInfo(batch_index={self.batch_index}, "
            f"status_code={self.status_code}, error={self.error!r})"
        )
