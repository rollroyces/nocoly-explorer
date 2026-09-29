"""Pydantic request/response models for the FastAPI service layer."""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from ..exceptions import NocolyError


class ServiceError(NocolyError):
    """Base exception for service-layer failures."""


class JobNotFound(ServiceError):
    """Raised when a job_id does not exist in Redis."""


class JobNotReady(ServiceError):
    """Raised when /result is fetched before job completion."""


class SinkType(str, Enum):
    PARQUET_LOCAL = "parquet_local"
    PARQUET_S3 = "parquet_s3"


class Granularity(str, Enum):
    DAY = "day"
    MONTH = "month"
    YEAR = "year"


class OutputSpec(BaseModel):
    sink: SinkType
    path: str
    partition_by: Optional[str] = None
    partition_granularity: Optional[Granularity] = None


class JobSubmission(BaseModel):
    # v0.3.0: auth_token is required. Prior to v0.2.x the worker accepted
    # jobs without one and only failed at the first Nocoly request; that
    # late-failure mode was a footgun and is removed.
    host: str = Field(min_length=1)
    worksheet_id: str = Field(min_length=1)
    auth_token: str = Field(min_length=1)
    output: Optional[OutputSpec] = None
    filter: Optional[Dict[str, Any]] = None
    columns: Optional[List[str]] = None
    page_size: int = Field(default=200, ge=1, le=1000)
    max_pages: int = Field(default=1000, ge=1)
    concurrency: int = Field(default=8, ge=1, le=32)

    # v0.4.0: incremental sync. ``state_store_kind`` selects the backend
    # ("file" or "redis"); the worker constructs the store from the
    # configured Redis URL when "redis", otherwise uses FileSyncStateStore.
    incremental: bool = False
    state_store_kind: str = Field(default="file", pattern="^(file|redis)$")
    force_full: bool = False
    updated_at_column: str = "_updatedAt"
    workspace: str = "default"


class JobStatus(BaseModel):
    job_id: str
    status: str  # queued / running / succeeded / failed / cancelled
    progress_pct: Optional[float] = None
    rows_fetched: Optional[int] = None
    error: Optional[str] = None


class JobResult(BaseModel):
    job_id: str
    status: str
    artifact_path: Optional[str] = None
    rows_written: Optional[int] = None


class HealthResponse(BaseModel):
    status: str