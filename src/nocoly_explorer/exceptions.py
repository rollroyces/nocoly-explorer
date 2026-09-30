"""Custom exception hierarchy for the Nocoly explorer package.

Every leaf exception carries a stable string ``code`` (e.g. ``"NOCOLY_021"``)
so callers can match on it without relying on the exception class. Codes are
documented in the README under "Error codes" and **never change** once
shipped - new codes are added for new failure modes instead.
"""

from __future__ import annotations


class NocolyError(Exception):
    """Base class for all custom errors. Carries no code itself; subclasses do."""

    code: str = "NOCOLY_000"


class MissingCredentialsError(NocolyError):
    """Raised when app_key/app_sign cannot be resolved."""

    code = "NOCOLY_001"


class OutputValidationError(NocolyError):
    """Raised when output parameters are inconsistent."""

    code = "NOCOLY_002"


class EnvironmentDetectionError(NocolyError):
    """Raised when the runtime mode cannot be determined."""

    code = "NOCOLY_003"


class SchemaDriftError(NocolyError):
    """Raised when the API response schema diverges from the declared one."""

    code = "NOCOLY_004"


class CardinalityExceededError(NocolyError):
    """Raised when a partition column exceeds the configured max distinct values."""

    code = "NOCOLY_005"


class AsyncClientError(NocolyError):
    """Base exception for async pagination failures."""

    code = "NOCOLY_010"


class PaginationLimitExceeded(AsyncClientError):
    """Raised when pagination exceeds the configured max_pages."""

    code = "NOCOLY_011"


class ServiceError(NocolyError):
    """Base exception for service-layer failures."""

    code = "NOCOLY_020"


class JobNotFound(ServiceError):
    """Raised when a job_id does not exist in Redis."""

    code = "NOCOLY_021"


class JobNotReady(ServiceError):
    """Raised when /result is fetched before job completion."""

    code = "NOCOLY_022"


class JobCancelled(NocolyError):
    """Raised when a cooperative ``cancel_check`` returns ``True`` mid-pagination.

    ``stream_async`` and ``fetch_all_async`` raise this when their
    ``cancel_check`` callable flips to ``True``. Catch ``NocolyError`` for
    general handling, or ``JobCancelled`` for cancellation-specific paths.
    """

    code = "NOCOLY_030"
