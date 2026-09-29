<div align="center">

<img src="docs/assets/hero.svg" alt="nocoly-explorer" width="100%">

</div>

<br>

<div align="center">

[![Tests](https://img.shields.io/badge/tests-245%20passed-brightgreen)]()
[![Python](https://img.shields.io/badge/python-3.10–3.12-blue)]()
[![Release](https://img.shields.io/badge/release-v0.2.0-blue)](https://github.com/rollroyces/nocoly-explorer/releases/tag/v0.2.0)
[![Wheel](https://img.shields.io/badge/wheel-39_KB-blue)](https://github.com/rollroyces/nocoly-explorer/releases/download/v0.2.0/nocoly_explorer-0.2.0-py3-none-any.whl)
[![License](https://img.shields.io/badge/license-MIT%20%2B%20Apache%202.0-lightgrey)]()

</div>

A modular Python client for downloading [Nocoly](https://www.nocoly.com) worksheet
data — from one-shot scripts to enterprise data pipelines. Four layers stacked on
the same core client:

- 🟢 **`WorksheetExporter`** — fetch into pandas / PySpark / CSV / JSON / files (v0.1.1)
- 🔵 **`StreamingExporter` + `AsyncWorksheetClient`** — paginate concurrently, write
  partitioned Parquet, hold memory bounded by row-group size (v0.2.0)
- 🟣 **`create_app` + `run_job`** — FastAPI service + Arq worker for orchestration
  from n8n / Airflow / schedulers (v0.2.0)
- 🟠 **`get_worksheet_schema`** — discover column names + data types directly from
  Nocoly, with sample-based inference fallback (v0.2.0)

---

## What you get

Real artifacts from a 487-row end-to-end run against a **local mock Nocoly server**
(visible in the live demo below):

<div align="center">

<img src="docs/assets/output.png" alt="output — partition tree, JSON status, PyArrow schema, pandas rows" width="100%">

</div>

<details>
<summary><b>What's in this screenshot</b></summary>

<br>

- **Top-left** — Hive-style partition tree: `region=HK/data_0.parquet` (243 rows) and `region=SZ/data_0.parquet` (244 rows), 5.9 KB each.
- **Top-right** — `GET /jobs/{id}/result` response: `status=succeeded`, `progress_pct=100.0`, `rows_fetched=487`, `artifact_path=/tmp/nocoly-real`.
- **Middle** — `pyarrow.parquet.read_table().schema`: `int64`, `string`, `double`, `bool`, `list<string>` — types inferred automatically from the first page, no schema file required.
- **Bottom** — `pandas.read_parquet().head(3)` showing actual row contents.

All four panels are real outputs from the demo at `scripts/demo.py` — not staged.
</details>

---

## How a job flows

<div align="center">

<img src="docs/assets/architecture.png" alt="architecture diagram — caller, FastAPI, Redis, Arq worker, Nocoly, Parquet" width="100%">

</div>

One HTTP request → enqueued in Redis → Arq worker streams pages via
`AsyncWorksheetClient` → writes through `StreamingExporter.stream_async` as pages
arrive → Parquet on disk. Memory stays bounded by `row_group_bytes` rather than
growing with row count. Status and progress flow back to Redis so the API stays
responsive. A `cancel` request flips a flag the worker checks between pages —
cooperative cancellation, no orphaned half-written files.

---

## Live demo

`scripts/demo.py` boots a mock Nocoly server and walks through the API
end-to-end against it. Honest label: this is a wiring test, not a real
Nocoly run.

```bash
git clone https://github.com/rollroyces/nocoly-explorer.git
cd nocoly-explorer
pip install -e ".[service,streaming,async,test]"
./scripts/demo.py
```

Or play back the recorded terminal session:

```bash
# if you have asciinema installed
asciinema play docs/assets/demo.cast
```

<p align="center">
  <img src="docs/assets/demo.gif" alt="terminal demo" width="800">
</p>

---

## Installation

```bash
pip install nocoly-explorer
```

Pick extras based on what you need:

| Extra            | Adds                                                | When you need it                                       |
|------------------|-----------------------------------------------------|--------------------------------------------------------|
| `dataframe`      | `pandas>=2.0`                                       | `WorksheetExporter(..., output_type="dataframe")`      |
| `spark`          | `pyspark>=3.4`                                      | Databricks / PySpark output                            |
| `streaming`      | `pyarrow>=14`                                       | `StreamingExporter` (Parquet)                          |
| `async`          | `aiohttp>=3.9`                                      | `AsyncWorksheetClient` (concurrent pagination)         |
| `service`        | `fastapi`, `arq`, `uvicorn`, `httpx`                | `create_app`, `run_job` (service layer)                |
| `test`           | `fakeredis`, `pytest-asyncio`                       | running the test suite                                 |

```bash
pip install "nocoly-explorer[streaming,async]"          # export jobs
pip install "nocoly-explorer[service,streaming,async]"  # full pipeline + API
```

---

## Layer 1 — `WorksheetExporter` (v0.1.1)

The original use case: fetch a worksheet into a DataFrame (or Spark / CSV / JSON /
file), with pluggable credentials, validated filters, capped retries, and `Retry-After`
honored on 429/503.

```python
from nocoly_explorer import WorksheetExporter, NocolyFilter

df = WorksheetExporter().export(
    host="https://bpm-uat.chinachemgroup.com",
    worksheet_id="ws_123",
    filter_criteria=NocolyFilter.and_group([
        NocolyFilter.quick(Status="Active", _updatedAt__gt="2025-01-01"),
        NocolyFilter.or_group([
            NocolyFilter.quick(Region="HK"),
            NocolyFilter.quick(Region="SZ"),
        ]),
    ]),
    columns=["Name", "Region", "Status", "_updatedAt"],
    sorts=[{"field": "_updatedAt", "order": "DESC"}],
    page_size=500,
    max_pages=25,
)
```

### Output formats

| `output_type`     | Returns                          | Notes                                                  |
|-------------------|----------------------------------|--------------------------------------------------------|
| `dataframe`       | `pandas.DataFrame`               | Requires pandas.                                       |
| `spark`           | `pyspark.sql.DataFrame`          | Requires PySpark / Databricks.                         |
| `json`            | `list[dict]`                     | Raw passthrough; serialize as needed.                  |
| `string`          | JSON-formatted `str`             | Handles `datetime`, `Decimal`, `UUID`, `bytes`.        |
| `csv`             | CSV-formatted `str`              | Union of all row keys; `None`/missing → empty.         |
| `file`            | writes to disk                   | Atomic. Supports `json`, `csv`, `parquet`.            |

### Architecture (Layer 1)

```
WorksheetExporter
 ├── EnvironmentDetector       (Databricks vs. local)
 ├── CredentialProvider        (Databricks secrets vs. env vs. config)
 ├── WorksheetClient           (retries, Retry-After, MAX_PAGE_SIZE=1000)
 │    └── get_worksheet_schema (column metadata + inference fallback)
 └── OutputFormatter           (dataframe / spark / json / csv / file)
```

---

## Discovering the worksheet schema (v0.2.0)

Read a worksheet's column names and data types directly, with a tolerant response
parser and a graceful fallback to sample-based inference. Useful for pre-flight
checks, schema-aware validation, or round-tripping through JSON for reuse on the
next export.

```python
from nocoly_explorer.client import WorksheetClient
from nocoly_explorer.auth import CredentialPair

client = WorksheetClient(
    host="https://bpm-uat.chinachemgroup.com",
    worksheet_id="ws_123",
    credentials=CredentialPair(
        app_key=...,
        app_sign=...,
    ),
)

schema = client.get_worksheet_schema()
print(schema.source)               # "api" or "inferred"
for col in schema.columns:
    print(f"  {col.name}: {col.type}  nullable={col.nullable}")

# Round-trip to JSON for reuse
with open("ws_123.schema.json", "w") as f:
    f.write(schema.to_json())
```

What you get back (`NocolyWorksheetSchema`):

| Field       | Type                                       | Notes                                      |
|-------------|--------------------------------------------|--------------------------------------------|
| `worksheet_id` | `str`                                    | source worksheet id                        |
| `columns`   | `list[NocolyColumnInfo]`                   | column metadata                            |
| `source`    | `Literal["api", "inferred"]`               | which path produced the result             |

Each `NocolyColumnInfo` carries `name`, `type` (Nocoly-style string), `nullable`,
and optional `description`. The schema is convertible to a `pyarrow.Schema` via
`.to_pyarrow()`.

### How the discovery works

1. The client tries `POST /api/v3/app/worksheets/{id}/columns` (or
   `metadata_endpoint=` if you override it). A DEBUG log line shows the URL and
   HTTP status on every attempt.
2. On 200 with a recognized response shape → returns `source="api"`.
3. On 404, network error, or unrecognized shape → fetches `sample_size` rows
   (default 1) and returns `source="inferred"` using `infer_schema_from_rows()`
   with type widening (int → float → decimal → string, date → timestamp).

### Caveat on the default endpoint

The default endpoint is an **educated guess** — the only Nocoly endpoint
documented in this repo is `POST /api/v3/app/worksheets/{id}/rows/list`. The
metadata path may not exist on your Nocoly deployment. Three ways to handle this:

- Pass an explicit `metadata_endpoint="/the/real/endpoint"` once you know it.
- Set `NOCOLY_SCHEMA_API_DISABLED=1` (or any truthy value) to skip the API
  attempt entirely and go straight to inference.
- Pass `try_api=False` on the call for the same effect.

### Standalone inference

For tests, dry-runs, or when you already have row dicts in hand:

```python
from nocoly_explorer import infer_schema_from_rows

rows = client.fetch_rows(page_size=10, max_pages=1)
schema = infer_schema_from_rows(rows, sample_size=256, worksheet_id="ws_123")
```

Type widening makes heterogeneous columns safe — a column that mixes `int` and
`float` values resolves to `number`, not `integer`.

---

## Layer 2 — `StreamingExporter` + `AsyncWorksheetClient` (v0.2.0)

For worksheets that don't fit in memory. Two pieces, composable:

### `AsyncWorksheetClient` — concurrent pagination

```python
import os
from nocoly_explorer import AsyncWorksheetClient

client = AsyncWorksheetClient(
    base_url="https://bpm-uat.chinachemgroup.com",
    auth_token=f"{os.environ['NOCOLY_APP_KEY']}:{os.environ['NOCOLY_APP_SIGN']}",
    worksheet_id="ws_123",
    concurrency=8,                      # 8 in-flight pages max
    requests_per_second=10.0,           # token bucket
    max_page_size=1000,
)

rows = await client.fetch_all_async(
    page_size=200,
    max_pages=1000,        # hard safety limit
)
```

Properties:
- **Order-preserving** — rows come back in page order even at concurrency=8.
- **Token-bucket rate limited** — configurable `requests_per_second` + `burst`.
- **Honors `Retry-After`** — parses both delta-seconds and HTTP-date forms, caps at
  `max_wait_seconds`. Retries on 429/503; raises `AsyncClientError` on other 4xx.
- **End-of-data signal** — server's `has_more` controls the loop. If `max_pages`
  is hit without `has_more=False`, raises `PaginationLimitExceeded`.

End-to-end smoke: 1,000 rows × 20 pages × 50 ms/page → **0.16 s** (6.2× faster
than sequential).

### `StreamingExporter` — partitioned Parquet

```python
from nocoly_explorer import (
    StreamingExporter, StreamingExportConfig, ParquetExportOptions,
    PartitionSpec,
)

result = StreamingExporter(
    client=client,                       # any object with .fetch_rows()
    config=StreamingExportConfig(
        output_dir="/mnt/datalake/nocoly/ws_123",
        worksheet_id="ws_123",
        options=ParquetExportOptions(
            row_group_bytes=128 * 1024 * 1024,   # Databricks sweet spot
            compression="snappy",
            write_statistics=True,
            on_schema_drift="ignore",            # or "error"
            max_partition_cardinality=10_000,
        ),
        partition=PartitionSpec(column="created_date", granularity="day"),
    ),
).export()

print(result)
# ExportResult(rows_written=487, partitions_written=2,
#               files_written=2, output_dir='/mnt/datalake/nocoly/ws_123')
```

`StreamingExporter.export()` is synchronous and expects a sync `WorksheetClientLike`.
To use it directly with `AsyncWorksheetClient`, the streaming exporter exposes
`stream_async()` which consumes pages from the async client and writes each page
through the row-group buffer as soon as it arrives. This is what the service
worker's `run_job` uses internally — no `_SyncRowsClient` adapter needed:

```python
import asyncio
from nocoly_explorer import (
    AsyncWorksheetClient, StreamingExporter, StreamingExportConfig,
    ParquetExportOptions, PartitionSpec,
)
from nocoly_explorer.streaming.exporter import _NoopClientAdapter


async def export_streaming(host, token, worksheet_id, output_dir):
    async with AsyncWorksheetClient(
        base_url=host, auth_token=token, worksheet_id=worksheet_id,
    ) as aclient:
        exporter = StreamingExporter(
            client=_NoopClientAdapter(),     # stream_async ignores this
            config=StreamingExportConfig(
                output_dir=output_dir,
                worksheet_id=worksheet_id,
                options=ParquetExportOptions(),
                partition=PartitionSpec(column="created_date", granularity="day"),
            ),
        )
        result = await exporter.stream_async(
            aclient,
            page_size=200,
            cancel_check=lambda: my_cancel_flag(),  # optional
        )
    return result
```

Memory is bounded by `row_group_bytes × partitions` plus the in-flight pages
(`concurrency × page_size` rows at most) — not by the full row count.

What you get:
- **One Parquet file per partition** (Hive-style: `output_dir/created_date=YYYY-MM-DD/data_0.parquet`).
- **Schema inference + drift handling** — types inferred from the first page, drift
  policy `ignore` (drop new columns, fill missing with `null`) or `error`
  (raise `SchemaDriftError`).
- **Memory bounded** — `RowGroupBuffer` flushes by target byte size, not row count.
- **Cardinality guard** — `CardinalityExceededError` if a partition column produces
  more than `max_partition_cardinality` distinct values (default 10,000).
- **Path-traversal safe** — partition values are stripped of `..`, `/`, and unsafe
  characters before becoming directory names.

End-to-end smoke: 25,000 rows × 5 regions × day-granularity → 5 valid Parquet
files, ~1 MiB total, ~363 K rows/s on a Mac mini.

### Composition with the v0.1.1 client

```python
from nocoly_explorer.client import NocolyClient
from nocoly_explorer import StreamingExporter, StreamingExportConfig

sync_client = NocolyClient(host=..., app_key=..., app_sign=...)
StreamingExporter(
    client=sync_client,                  # WorksheetClientLike Protocol
    config=StreamingExportConfig(output_dir="...", worksheet_id="...", options=...),
).export()
```

The exporter accepts any object with `.fetch_rows()` (the `WorksheetClientLike`
Protocol) — sync or async, real or fake.

---

## Layer 3 — `create_app` + `run_job` (v0.2.0)

A FastAPI service for orchestrating exports from n8n, Airflow, cron, or anything
else that can `POST /jobs`. The service itself doesn't write data — it enqueues
into Redis and an Arq worker pulls jobs and runs `StreamingExporter`.

### Run the service

```bash
# 1. Start Redis
docker run -d --name nocoly-redis -p 6379:6379 redis:7-alpine

# 2. Start the API
export NOCOLY_SERVICE_API_KEY="$(openssl rand -hex 32)"   # recommended
uvicorn nocoly_explorer.service:app --host 127.0.0.1 --port 8080

# 3. Start the worker (one or more)
arq nocoly_explorer.service.worker.WorkerSettings
```

### Submit a job

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Authorization: Bearer $NOCOLY_SERVICE_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "host": "https://bpm-uat.chinachemgroup.com",
    "worksheet_id": "ws_123",
    "page_size": 200,
    "max_pages": 1000,
    "concurrency": 8,
    "output": {
      "sink": "parquet_local",
      "path": "/mnt/datalake/nocoly/ws_123",
      "partition_by": "created_date",
      "partition_granularity": "day"
    }
  }'
# {"job_id": "a1b2c3..."}
```

### Poll status

```bash
curl http://localhost:8080/jobs/a1b2c3... \
  -H "Authorization: Bearer $NOCOLY_SERVICE_API_KEY"
# {"job_id":"a1b2c3...","status":"running","progress_pct":42,
#  "rows_fetched":12500,"started_at":"...","finished_at":null,"error":null}
```

### Endpoints

| Method | Path                       | Body / Notes                                       |
|--------|----------------------------|----------------------------------------------------|
| `GET`  | `/healthz`                 | liveness (always 200)                              |
| `GET`  | `/readyz`                  | 200 if Redis reachable, 503 otherwise              |
| `POST` | `/jobs`                    | submit; returns `{"job_id": "..."}`                |
| `GET`  | `/jobs/{id}`               | status + progress + rows_fetched                   |
| `GET`  | `/jobs/{id}/result`        | artifact path; 409 if not yet succeeded            |
| `POST` | `/jobs/{id}/cancel`        | cooperative cancellation at page boundary          |

### API key auth

If you set `api_key=...` on `create_app` (or set `NOCOLY_SERVICE_API_KEY` in
the environment of your `uvicorn` process), every endpoint requires:

```
Authorization: Bearer <api_key>
```

If `api_key` is not set, **the service runs unauthenticated** — fine for local
dev, never fine for production. Bind to 127.0.0.1 or front with a reverse proxy.

---

## Configuration

### Environment variables

The Nocoly API auth uses an `env_prefix` strategy. The default prefix is
`NOCOLY`; you can override per-detector. Required variables:

```bash
export NOCOLY_APP_KEY="..."
export NOCOLY_APP_SIGN="..."
```

Or, for multiple environments, use a prefix:

```python
from nocoly_explorer.auth import EnvCredentialProvider
creds = EnvCredentialProvider(env_prefix="NOCOLY_UAT")
# reads NOCOLY_UAT_APP_KEY, NOCOLY_UAT_APP_SIGN
```

On Databricks, the detector automatically switches to `DatabricksSecretProvider`
unless you set `NOCOLY_DETECT_FORCE=local`.

### Other env knobs

| Variable                  | Effect                                                   |
|---------------------------|----------------------------------------------------------|
| `NOCOLY_DETECT_FORCE`     | `local` / `databricks` — override environment detection  |
| `NOCOLY_SERVICE_API_KEY`  | Bearer token required on every service endpoint          |
| `NOCOLY_REDIS_URL`        | Redis URL for service state (default `redis://localhost:6379/0`) |
| `NOCOLY_SCHEMA_API_DISABLED` | `1`/`true`/`yes`/`on` — skip the metadata endpoint in `get_worksheet_schema`, go straight to inference |

---

## Development

```bash
git clone https://github.com/rollroyces/nocoly-explorer.git
cd nocoly-explorer
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dataframe,streaming,async,service,test]"

# Run the full test suite (245 tests)
pytest

# Run only streaming tests
pytest tests/test_streaming_exporter.py -v

# Run only service tests
pytest tests/test_service_app.py -v

# Run only schema tests
pytest tests/test_schema.py -v

# Run the demo
./scripts/demo.py
```

### Examples

Self-contained runnable scripts under [`examples/`](examples/) demonstrate each
major workflow. Each script has a top-of-file docstring (purpose, setup, env
vars, run command) and reads credentials from environment variables only.

| # | File | What it does |
|---|---|---|
| 01 | [`examples/01_basic_dataframe.py`](examples/01_basic_dataframe.py) | Layer 1 — fetch into a pandas DataFrame with a filter |
| 02 | [`examples/02_streaming_parquet.py`](examples/02_streaming_parquet.py) | Layer 2 — stream pages into partitioned Parquet |
| 03 | [`examples/03_schema_discovery.py`](examples/03_schema_discovery.py) | Discover column names + data types via `get_worksheet_schema` |
| 04 | [`examples/04_s3_export.py`](examples/04_s3_export.py) | Stream directly into an S3 bucket via `pyarrow.fs.S3FileSystem` |
| 05 | [`examples/05_filters.py`](examples/05_filters.py) | Seven `NocolyFilter` DSL recipes |
| 06 | [`examples/06_service_submit.py`](examples/06_service_submit.py) | Submit an export job to the FastAPI service via httpx |

See [`examples/README.md`](examples/README.md) for the full environment-variable
reference and a quick-start for the service example.

### Project layout

```
src/nocoly_explorer/
 ├── async_client.py          # Layer 2 — AsyncWorksheetClient, RetryPolicy, TokenBucket
 ├── auth.py                  # CredentialProvider strategies
 ├── backoff.py               # Shared compute_backoff + parse_retry_after
 ├── client.py                # Layer 1 — sync client with retry / Retry-After
 ├── config.py                # User config file loading
 ├── detector.py              # Databricks vs. local detection
 ├── exceptions.py            # All custom exceptions (centralized)
 ├── exporter.py              # Layer 1 — WorksheetExporter orchestrator
 ├── filters.py               # NocolyFilter DSL
 ├── output.py                # Layer 1 — output formatters
 ├── schema.py                # NocolyColumnInfo, NocolyWorksheetSchema, infer_schema_from_rows
 ├── service/                 # Layer 3 — FastAPI service
 │   ├── __init__.py          #   create_app
 │   ├── auth.py              #   Bearer-token validator
 │   ├── schemas.py           #   Pydantic models
 │   ├── state.py             #   JobState (Redis)
 │   └── worker.py            #   run_job (Arq) — uses StreamingExporter.stream_async
 └── streaming/               # Layer 2 — StreamingExporter
     ├── buffer.py            #   RowGroupBuffer (target-bytes flushing)
     ├── exporter.py          #   StreamingExporter + StreamingExportConfig + stream_async
     ├── options.py           #   ParquetExportOptions, PartitionSpec
     ├── partitions.py        #   PartitionRouter + cardinality guard
     ├── schema.py            #   ParquetSchemaManager + drift policy
     └── writer.py            #   ParquetPartitionWriter (append mode)
```

---

## Honest gaps

This section lists what we have **not yet** verified end-to-end. Add them to your
own integration checklist:

- **Real Nocoly endpoint with real credentials.** Everything end-to-end has been
  validated against a local mock that matches the documented pagination shape.
  Field names, auth headers, and error responses on the real server have not
  been exercised here.
- **Metadata endpoint for `get_worksheet_schema`.** The default endpoint
  (`/api/v3/app/worksheets/{id}/columns`) is an educated guess; the Nocoly
  metadata path has not been verified against a real server. The sample-based
  inference fallback works regardless, but `source="api"` should be confirmed
  against your deployment before relying on it.
- **Crash-restart safety on non-local filesystems.** Local exports use a
  `.tmp` + rename pattern; S3 and other non-local destinations fall back to
  direct writes because atomic rename isn't portable. A future commit could
  implement upload-then-promote via `pyarrow.fs` copy semantics if needed.
- **Incremental sync.** Spec §6 (the Phase 4 `SyncStateStore` + `updated_at`
  filtering) is not implemented. v0.2.1 will address it.
- **Multi-row-group per partition at scale.** Tested with single-digit row groups
  per partition. Databricks recommendations (128 MB row groups) haven't been
  load-tested at hundreds of MB per partition.

---

## License

Dual-licensed under MIT and Apache 2.0 at your option.
