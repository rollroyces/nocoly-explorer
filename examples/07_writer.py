"""07 — Writing rows back to a Nocoly worksheet.

The write-side counterpart to examples 01–06. Demonstrates
``WorksheetWriter`` (sync) and ``AsyncWorksheetWriter`` (async) with
all four operations (add / update / upsert / delete) and three input
formats (list of dicts, pandas DataFrame, local CSV file).

Setup:
    pip install "nocoly-explorer[dataframe,async]"

Environment:
    NOCOLY_APP_KEY   — Nocoly application key (HAP-AppKey header)
    NOCOLY_APP_SIGN  — Nocoly application sign (HAP-Sign header)

Run:
    python examples/07_writer.py

Caveat: the default endpoint paths in this example
(``/rows``, ``/rows/upsert``, ``/rows/delete``) are educated guesses.
Nocoly v3 only documents the read endpoint in this repository. Pass
``add_endpoint=`` / ``update_endpoint=`` / ``upsert_endpoint=`` /
``delete_endpoint=`` on the writer constructor once you know the real
URLs for your Nocoly deployment.
"""
from __future__ import annotations

import asyncio
import csv
import os
import tempfile
from typing import Any

from nocoly_explorer import (
    AsyncWriteConfig,
    AsyncWorksheetWriter,
    WorksheetWriter,
    WriteResult,
)
from nocoly_explorer.auth import CredentialPair


def get_credentials() -> CredentialPair:
    """Read NOCOLY_APP_KEY and NOCOLY_APP_SIGN from the environment."""
    app_key = os.environ.get("NOCOLY_APP_KEY")
    app_sign = os.environ.get("NOCOLY_APP_SIGN")
    missing = [
        n for n, v in (
            ("NOCOLY_APP_KEY", app_key),
            ("NOCOLY_APP_SIGN", app_sign),
        )
        if not v
    ]
    if missing:
        raise SystemExit(
            f"Missing environment variables: {', '.join(missing)}. "
            "Export them before running this example."
        )
    return CredentialPair(app_key=app_key, app_sign=app_sign)


def _temp_csv(rows: list[dict[str, Any]]) -> str:
    """Write rows to a temporary CSV and return the path."""
    fd, path = tempfile.mkstemp(suffix=".csv", prefix="nocoly_writer_")
    os.close(fd)
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def sync_demo(host: str, worksheet_id: str, credentials: CredentialPair) -> None:
    """Sync writer: list of dicts, CSV file path, all four operations."""
    writer = WorksheetWriter(
        host=host,
        worksheet_id=worksheet_id,
        credentials=credentials,
        batch_size=500,
    )

    # 1. Add rows — list of dicts (simplest form).
    result: WriteResult = writer.add_rows([
        {"id": 1, "name": "alice", "region": "HK"},
        {"id": 2, "name": "bob",   "region": "SZ"},
    ])
    print(f"add_rows(list):     {result.rows_succeeded}/{result.rows_attempted} in {result.batches_sent} batch(es)")

    # 2. Add rows — from a local CSV file path. pyarrow reads it under the hood.
    csv_path = _temp_csv([
        {"id": 3, "name": "carol", "region": "SH"},
        {"id": 4, "name": "dave",  "region": "BJ"},
        {"id": 5, "name": "eve",   "region": "HK"},
    ])
    try:
        result = writer.add_rows(csv_path)
        print(f"add_rows(csv path): {result.rows_succeeded}/{result.rows_attempted} in {result.batches_sent} batch(es)")
    finally:
        os.unlink(csv_path)

    # 3. Update rows by key column — change alice's region.
    result = writer.update_rows(
        [{"id": 1, "region": "HK-W"}],
        key_column="id",
    )
    print(f"update_rows:        {result.rows_succeeded}/{result.rows_attempted}")

    # 4. Upsert by key column — id=6 is new (insert), id=1 is existing (patch).
    result = writer.upsert_rows(
        [
            {"id": 1, "region": "HK-E"},  # existing
            {"id": 6, "name": "frank", "region": "SH"},  # new
        ],
        key_column="id",
    )
    print(f"upsert_rows:        {result.rows_succeeded}/{result.rows_attempted}")

    # 5. Delete by id list — chunks automatically above batch_size.
    result = writer.delete_rows(row_ids=[2, 3])
    print(f"delete_rows(ids):   {result.rows_succeeded}/{result.rows_attempted}")

    # 6. Delete by server-side filter — single batch, no chunking.
    result = writer.delete_rows(
        filter_criteria={
            "type": "group",
            "logic": "AND",
            "filters": [
                {
                    "type": "condition",
                    "field": "region",
                    "operator": "EQ",
                    "value": "BJ",
                }
            ],
        },
    )
    print(f"delete_rows(filter): {result.rows_succeeded}/{result.rows_attempted}")


async def async_demo(host: str, worksheet_id: str, auth_token: str) -> None:
    """Async writer: same operations, dispatched concurrently under a token bucket."""
    config = AsyncWriteConfig(concurrency=4, requests_per_second=20.0)

    async with AsyncWorksheetWriter(
        base_url=host,
        auth_token=auth_token,        # "<app_key>:<app_sign>"
        worksheet_id=worksheet_id,
        batch_size=500,
        config=config,
    ) as writer:
        # Pandas DataFrame input (requires the [dataframe] extra).
        try:
            import pandas as pd
        except ImportError:
            print("async_demo: pandas not installed; skipping DataFrame branch")
        else:
            df = pd.DataFrame([
                {"id": 10, "name": "grace", "region": "HK"},
                {"id": 11, "name": "henry", "region": "SZ"},
            ])
            result = await writer.add_rows(df)
            print(f"async add_rows(df): {result.rows_succeeded}/{result.rows_attempted} in {result.batches_sent} batch(es)")

        result = await writer.update_rows(
            [{"id": 10, "region": "HK-N"}],
            key_column="id",
        )
        print(f"async update_rows:   {result.rows_succeeded}/{result.rows_attempted}")


def main() -> None:
    host = "https://bpm-uat.chinachemgroup.com"  # or your Nocoly host
    worksheet_id = "ws_123"                       # replace with real id

    credentials = get_credentials()
    auth_token = f"{credentials.app_key}:{credentials.app_sign}"

    print("--- sync ---")
    sync_demo(host, worksheet_id, credentials)
    print("--- async ---")
    asyncio.run(async_demo(host, worksheet_id, auth_token))


if __name__ == "__main__":
    main()