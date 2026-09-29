"""Tests for FastAPI app factory."""

from __future__ import annotations

import pytest

from nocoly_explorer.service.state import JobState


@pytest.fixture
def redis_factory():
    """Return a factory that produces fresh FakeRedis instances per request."""
    import fakeredis.aioredis
    server = fakeredis.FakeServer()
    def factory():
        return fakeredis.aioredis.FakeRedis(server=server)
    return factory


@pytest.fixture
def app(redis_factory):
    from nocoly_explorer.service import create_app

    async def fake_enqueue(job_id, params):
        return "queued"

    return create_app(redis_factory=redis_factory, enqueue_func=fake_enqueue)


@pytest.fixture
def client(app):
    from fastapi.testclient import TestClient
    return TestClient(app)


def test_healthz_returns_ok(client):
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_readyz_returns_ok_when_redis_reachable(client):
    r = client.get("/readyz")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_post_jobs_returns_job_id(client):
    r = client.post("/jobs", json={
        "host": "https://example.com",
        "worksheet_id": "ws-1",
        "auth_token": "k:s",
        "page_size": 100,
    })
    assert r.status_code == 200
    data = r.json()
    assert "job_id" in data
    assert isinstance(data["job_id"], str)
    assert len(data["job_id"]) > 0


def test_post_jobs_rejects_missing_required_fields(client):
    r = client.post("/jobs", json={"worksheet_id": "ws-1"})
    assert r.status_code == 422


def test_get_job_status_returns_queued(client):
    submit = client.post("/jobs", json={
        "host": "https://example.com",
        "worksheet_id": "ws-1",
        "auth_token": "k:s",
    }).json()
    job_id = submit["job_id"]
    r = client.get(f"/jobs/{job_id}")
    assert r.status_code == 200
    data = r.json()
    assert data["job_id"] == job_id
    assert data["status"] == "queued"


def test_get_job_status_404_for_unknown_job(client):
    r = client.get("/jobs/nonexistent")
    assert r.status_code == 404


def test_get_job_result_returns_409_when_not_ready(client):
    submit = client.post("/jobs", json={
        "host": "https://example.com",
        "worksheet_id": "ws-1",
        "auth_token": "k:s",
    }).json()
    r = client.get(f"/jobs/{submit['job_id']}/result")
    assert r.status_code == 409


def test_post_cancel_sets_cancel_flag(client, redis_factory):
    import asyncio
    submit = client.post("/jobs", json={
        "host": "https://example.com",
        "worksheet_id": "ws-1",
        "auth_token": "k:s",
    }).json()
    job_id = submit["job_id"]
    r = client.post(f"/jobs/{job_id}/cancel")
    assert r.status_code == 200
    assert r.json()["status"] == "cancellation_requested"

    async def check():
        state = JobState(redis_factory())
        return await state.is_cancel_requested(job_id)
    assert asyncio.run(check()) is True


def test_api_key_required_when_configured(redis_factory):
    from nocoly_explorer.service import create_app
    from fastapi.testclient import TestClient

    async def fake_enqueue(job_id, params):
        return "queued"

    app = create_app(redis_factory=redis_factory, api_key="secret-1", enqueue_func=fake_enqueue)
    client = TestClient(app)
    r = client.post("/jobs", json={"host": "https://x", "worksheet_id": "y", "auth_token": "k:s"})
    assert r.status_code == 401
    r = client.post(
        "/jobs",
        json={"host": "https://x", "worksheet_id": "y", "auth_token": "k:s"},
        headers={"Authorization": "Bearer wrong"},
    )
    assert r.status_code == 401
    r = client.post(
        "/jobs",
        json={"host": "https://x", "worksheet_id": "y", "auth_token": "k:s"},
        headers={"Authorization": "Bearer secret-1"},
    )
    assert r.status_code == 200


def test_concurrent_submissions_get_unique_job_ids(client):
    ids = set()
    for _ in range(5):
        r = client.post("/jobs", json={
            "host": "https://example.com",
            "worksheet_id": "ws-1",
            "auth_token": "k:s",
        })
        ids.add(r.json()["job_id"])
    assert len(ids) == 5