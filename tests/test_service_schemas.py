"""Tests for service-layer schemas."""

from __future__ import annotations

import pytest


def test_service_error_inherits_from_nocoly_error():
    from nocoly_explorer.exceptions import NocolyError
    from nocoly_explorer.service.schemas import ServiceError
    assert issubclass(ServiceError, NocolyError)


def test_job_not_found_inherits_from_service_error():
    from nocoly_explorer.service.schemas import ServiceError, JobNotFound
    assert issubclass(JobNotFound, ServiceError)


def test_job_not_ready_inherits_from_service_error():
    from nocoly_explorer.service.schemas import ServiceError, JobNotReady
    assert issubclass(JobNotReady, ServiceError)


def test_job_submission_minimum_valid():
    from nocoly_explorer.service.schemas import JobSubmission
    j = JobSubmission(host="https://example.com", worksheet_id="ws-1", auth_token="k:s")
    assert j.host == "https://example.com"
    assert j.worksheet_id == "ws-1"
    assert j.page_size == 200
    assert j.max_pages == 1000
    assert j.concurrency == 8
    assert j.output is None
    assert j.filter is None


def test_job_submission_rejects_invalid_page_size():
    from nocoly_explorer.service.schemas import JobSubmission
    with pytest.raises(ValueError):
        JobSubmission(host="https://example.com", worksheet_id="ws-1", auth_token="k:s", page_size=0)
    with pytest.raises(ValueError):
        JobSubmission(host="https://example.com", worksheet_id="ws-1", auth_token="k:s", page_size=1001)


def test_job_submission_rejects_invalid_concurrency():
    from nocoly_explorer.service.schemas import JobSubmission
    with pytest.raises(ValueError):
        JobSubmission(host="https://example.com", worksheet_id="ws-1", auth_token="k:s", concurrency=0)
    with pytest.raises(ValueError):
        JobSubmission(host="https://example.com", worksheet_id="ws-1", auth_token="k:s", concurrency=100)


def test_job_submission_validates_output_sink():
    from nocoly_explorer.service.schemas import JobSubmission, OutputSpec
    o = OutputSpec(sink="parquet_local", path="/tmp/out", partition_by="region")
    j = JobSubmission(host="https://example.com", worksheet_id="ws-1", auth_token="k:s", output=o)
    assert j.output.sink == "parquet_local"


def test_job_submission_rejects_unknown_sink():
    from nocoly_explorer.service.schemas import OutputSpec
    with pytest.raises(ValueError):
        OutputSpec(sink="parquet_mongodb", path="/tmp/out")


def test_job_status_construction():
    from nocoly_explorer.service.schemas import JobStatus
    s = JobStatus(job_id="abc", status="queued")
    assert s.job_id == "abc"
    assert s.status == "queued"
    assert s.progress_pct is None


def test_job_result_construction():
    from nocoly_explorer.service.schemas import JobResult
    r = JobResult(job_id="abc", status="succeeded", artifact_path="/tmp/out")
    assert r.artifact_path == "/tmp/out"


def test_health_response_construction():
    from nocoly_explorer.service.schemas import HealthResponse
    h = HealthResponse(status="ok")
    assert h.status == "ok"



class TestAuthTokenRequired:
    """v0.3.0: auth_token is a required field on every job submission."""

    def test_missing_auth_token_rejected(self):
        from nocoly_explorer.service.schemas import JobSubmission
        with pytest.raises(ValueError):
            JobSubmission(host="https://example.com", worksheet_id="ws-1")

    def test_empty_auth_token_rejected(self):
        from nocoly_explorer.service.schemas import JobSubmission
        with pytest.raises(ValueError):
            JobSubmission(host="https://example.com", worksheet_id="ws-1", auth_token="")

    def test_non_empty_auth_token_accepted(self):
        from nocoly_explorer.service.schemas import JobSubmission
        j = JobSubmission(host="https://example.com", worksheet_id="ws-1", auth_token="k:s")
        assert j.auth_token == "k:s"
