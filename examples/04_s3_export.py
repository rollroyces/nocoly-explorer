"""04 — Stream a worksheet directly into Parquet files on S3.

``StreamingExportConfig.filesystem`` accepts any
``pyarrow.fs.FileSystem``. For S3, pass the result of
``pyarrow.fs.S3FileSystem.from_uri("s3://bucket/key")``.

The export streams pages into the writer as they come back from
Nocoly; memory stays bounded by the row-group size. The writer does
NOT use the .tmp + rename atomic-rename trick on non-local
filesystems (atomic rename is local-only), but for production S3
exports you can layer in a multipart-upload pattern on top.

Setup:
    pip install "nocoly-explorer[streaming,async]"

Environment:
    NOCOLY_APP_KEY, NOCOLY_APP_SIGN
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY  (or use IAM role)
    NOCOLY_S3_URI   — destination, e.g. s3://my-bucket/nocoly-exports/ws_123

Run:
    python examples/04_s3_export.py
"""
from __future__ import annotations

import asyncio
import os

import pyarrow.fs as pafs

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
        raise SystemExit("Missing NOCOLY_APP_KEY / NOCOLY_APP_SIGN.")
    return app_key, app_sign


async def stream_to_s3(
    host: str,
    worksheet_id: str,
    s3_uri: str,
    partition_column: str = "created_date",
) -> None:
    app_key, app_sign = get_credentials()
    filesystem, base_path = pafs.S3FileSystem.from_uri(s3_uri)

    async with AsyncWorksheetClient(
        base_url=host,
        auth_token=f"{app_key}:{app_sign}",
        worksheet_id=worksheet_id,
        concurrency=8,
        requests_per_second=10.0,
    ) as client:
        exporter = StreamingExporter(
            client=client,  # type: ignore[arg-type]  # export() not used
            config=StreamingExportConfig(
                output_dir=base_path,  # path within the S3 bucket
                worksheet_id=worksheet_id,
                options=ParquetExportOptions(
                    row_group_bytes=128 * 1024 * 1024,
                    compression="snappy",
                ),
                partition=PartitionSpec(column=partition_column, granularity="day"),
                filesystem=filesystem,  # ← this is the S3 knob
            ),
        )
        result = await exporter.stream_async(
            client,
            page_size=200,
            cancel_check=None,
        )

    print(f"Wrote {result.rows_written} rows to {s3_uri}")
    print(f"Partitions: {list(result.partitions.keys())}")
    # Optional: list the files back to confirm.
    files = filesystem.get_file_info(
        pafs.FileSelector(base_path, allow_not_found=False, recursive=True)
    )
    for fi in files[:5]:
        print(f"  {fi.path}  ({fi.size} bytes)")
    if len(files) > 5:
        print(f"  ... {len(files) - 5} more")


def main() -> None:
    host = "https://bpm-uat.chinachemgroup.com"
    worksheet_id = "ws_123"
    s3_uri = os.environ.get("NOCOLY_S3_URI", "s3://my-bucket/nocoly-exports/ws_123")
    asyncio.run(stream_to_s3(host, worksheet_id, s3_uri))


if __name__ == "__main__":
    main()
