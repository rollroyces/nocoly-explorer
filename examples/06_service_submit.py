"""06 — Submit an export job to the running nocoly-explorer service.

Prerequisites (in another terminal):
    docker run -d --name nocoly-redis -p 6379:6379 redis:7-alpine
    export NOCOLY_SERVICE_API_KEY="$(openssl rand -hex 32)"
    uvicorn nocoly_explorer.service:app --host 127.0.0.1 --port 8080
    arq nocoly_explorer.service.worker.WorkerSettings

This script:
    1. POSTs /jobs to submit a streaming Parquet export.
    2. Polls /jobs/{id} until status is "succeeded" or "failed".
    3. Fetches /jobs/{id}/result for the artifact path.

Setup:
    pip install "nocoly-explorer[service]"

Environment:
    NOCOLY_SERVICE_BASE_URL   — defaults to http://127.0.0.1:8080
    NOCOLY_SERVICE_API_KEY    — must match the server's setting
    NOCOLY_WORKSET_HOST       — Nocoly host (passed into the job)
    NOCOLY_WORKSET_ID         — worksheet id
    NOCOLY_WORKSET_OUTPUT     — local directory for Parquet files
                                 (must be writable by the worker process)

Run:
    python examples/06_service_submit.py
"""
from __future__ import annotations

import os
import sys
import time

import httpx


def get_env(key: str, *, default: str | None = None, required: bool = False) -> str:
    value = os.environ.get(key, default)
    if required and not value:
        raise SystemExit(f"Missing required env var: {key}")
    return value or ""


def main() -> None:
    base_url = get_env("NOCOLY_SERVICE_BASE_URL", default="http://127.0.0.1:8080")
    api_key = get_env("NOCOLY_SERVICE_API_KEY", required=True)
    host = get_env("NOCOLY_WORKSET_HOST", default="https://bpm-uat.chinachemgroup.com")
    worksheet_id = get_env("NOCOLY_WORKSET_ID", default="ws_123")
    output_path = get_env("NOCOLY_WORKSET_OUTPUT", default="/tmp/nocoly-service-out")

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    # 1. Submit the job.
    submission = {
        "host": host,
        "worksheet_id": worksheet_id,
        "page_size": 200,
        "max_pages": 1000,
        "concurrency": 8,
        "output": {
            "sink": "parquet_local",
            "path": output_path,
            "partition_by": "created_date",
            "partition_granularity": "day",
        },
    }
    print(f"POST {base_url}/jobs")
    resp = httpx.post(f"{base_url}/jobs", headers=headers, json=submission, timeout=10.0)
    resp.raise_for_status()
    job_id = resp.json()["job_id"]
    print(f"Submitted job {job_id}")

    # 2. Poll until done.
    while True:
        resp = httpx.get(f"{base_url}/jobs/{job_id}", headers=headers, timeout=10.0)
        resp.raise_for_status()
        status = resp.json()
        print(
            f"  status={status['status']:<10s}  "
            f"progress={status.get('progress_pct', 0):>5.1f}%  "
            f"rows={status.get('rows_fetched', 0)}"
        )
        if status["status"] in ("succeeded", "failed", "cancelled"):
            break
        time.sleep(2.0)

    if status["status"] != "succeeded":
        print(f"Job ended with status={status['status']}: {status.get('error')}")
        sys.exit(1)

    # 3. Fetch the result.
    resp = httpx.get(f"{base_url}/jobs/{job_id}/result", headers=headers, timeout=10.0)
    resp.raise_for_status()
    print(f"Result: {resp.json()}")


if __name__ == "__main__":
    main()
