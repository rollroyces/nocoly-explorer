"""02 — Stream a worksheet into partitioned Parquet (memory-bounded).

For worksheets that don't fit in memory, use ``AsyncWorksheetClient``
concurrently for pagination and ``StreamingExporter.export_async`` to
write pages to Parquet as they arrive. Memory stays bounded by the
row-group size, not the row count.

This is the same code path the FastAPI / Arq worker uses internally
(see ``examples/06_service_submit.py``).

Setup:
    pip install "nocoly-explorer[streaming,async]"

Environment:
    NOCOLY_APP_KEY, NOCOLY_APP_SIGN — Nocoly credentials
    NOCOLY_OUTPUT_DIR — local directory to write Parquet files to
                          (defaults to /tmp/nocoly-export)

Run:
    python examples/02_streaming_parquet.py
"""
from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path

import pyarrow.parquet as pq

from nocoly_explorer import (
    AsyncWorksheetClient,
    ParquetExportOptions,
    PartitionSpec,
    StreamingExportConfig,
    StreamingExporter,
)


def get_credentials() -> tuple[str, str]:
    app_key = os.environ.get("NOCOLY_APP_KEY")
    app_sign = os.environ.get("NOCOLY_APP_SIGN")
    if not app_key or not app_sign:
        raise SystemExit(
            "Missing NOCOLY_APP_KEY / NOCOLY_APP_SIGN. Export them before running."
        )
    return app_key, app_sign


async def stream_worksheet_to_parquet(
    host: str,
    worksheet_id: str,
    output_dir: str,
    partition_column: str = "created_date",
) -> None:
    app_key, app_sign = get_credentials()
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    async with AsyncWorksheetClient(
        base_url=host,
        auth_token=f"{app_key}:{app_sign}",
        worksheet_id=worksheet_id,
        concurrency=8,            # 8 in-flight pages max
        requests_per_second=10.0,  # token-bucket rate limit
    ) as client:
        exporter = StreamingExporter(
            client=client,  # type: ignore[arg-type]  # export() not used
            config=StreamingExportConfig(
                output_dir=out,
                worksheet_id=worksheet_id,
                options=ParquetExportOptions(
                    row_group_bytes=128 * 1024 * 1024,  # 128 MiB sweet spot
                    compression="snappy",
                ),
                partition=PartitionSpec(column=partition_column, granularity="day"),
            ),
        )
        started = time.monotonic()
        # stream_async consumes pages as they arrive and writes through the
        # row-group buffer; memory stays bounded.
        result = await exporter.stream_async(
            client,
            page_size=200,
            cancel_check=None,  # add a cancel callback for long jobs
        )
        elapsed = time.monotonic() - started

    print(f"Wrote {result.rows_written} rows to {out}")
    print(f"Partitions: {result.partitions}")
    print(f"Bytes: {result.bytes_written:,}  ({result.bytes_written / elapsed / 1e6:.1f} MB/s)")
    # Spot-check the first partition:
    sample_files = sorted(out.glob("**/*.parquet"))[:1]
    for f in sample_files:
        table = pq.read_table(f)
        print(f"  {f}: {table.num_rows} rows, schema = {table.schema.names}")


def main() -> None:
    host = "https://bpm-uat.chinachemgroup.com"
    worksheet_id = "ws_123"
    output_dir = os.environ.get("NOCOLY_OUTPUT_DIR", "/tmp/nocoly-export")
    asyncio.run(stream_worksheet_to_parquet(host, worksheet_id, output_dir))


if __name__ == "__main__":
    main()
