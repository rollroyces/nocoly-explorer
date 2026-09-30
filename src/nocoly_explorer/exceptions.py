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
