"""Custom exception hierarchy for the Nocoly explorer package."""

from __future__ import annotations


class NocolyError(Exception):
    """Base class for all custom errors."""


class MissingCredentialsError(NocolyError):
    """Raised when app_key/app_sign cannot be resolved."""


class OutputValidationError(NocolyError):
    """Raised when output parameters are inconsistent."""


class EnvironmentDetectionError(NocolyError):
    """Raised when the runtime mode cannot be determined."""


class SchemaDriftError(NocolyError):
    """Raised when the API response schema diverges from the declared one."""


class CardinalityExceededError(NocolyError):
    """Raised when a partition column exceeds the configured max distinct values."""


class AsyncClientError(NocolyError):
    """Base exception for async pagination failures."""


class PaginationLimitExceeded(AsyncClientError):
    """Raised when pagination exceeds the configured max_pages."""


class ServiceError(NocolyError):
    """Base exception for service-layer failures."""


class JobNotFound(ServiceError):
    """Raised when a job_id does not exist in Redis."""


class JobNotReady(ServiceError):
    """Raised when /result is fetched before job completion."""

class JobCancelled(NocolyError):
    """Raised when a cooperative ``cancel_check`` returns ``True`` mid-pagination.

    ``stream_async`` and ``fetch_all_async`` raise this when their
    ``cancel_check`` callable flips to ``True``. Catch ``NocolyError`` for
    general handling, or ``JobCancelled`` for cancellation-specific paths.
    """
