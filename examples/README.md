# Examples

Self-contained runnable scripts that demonstrate the most common
nocoly-explorer workflows. Each script has a top-of-file docstring
explaining what it does, what to set up, and how to run it.

All scripts use environment variables for credentials — no secrets are
hardcoded. Set them in your shell or via a `.env` file before
running.

## Setup

Install the extras your script needs:

```bash
# Layer 1 (one-shot, DataFrame)
pip install "nocoly-explorer[dataframe]"

# Layer 2 (streaming, async, Parquet)
pip install "nocoly-explorer[streaming,async]"

# Layer 2 + S3
pip install "nocoly-explorer[streaming,async]"  # pyarrow is required either way

# Layer 3 (FastAPI + Arq)
pip install "nocoly-explorer[service]"
```

## The scripts

| # | File | What it does | Run |
|---|---|---|---|
| 01 | [`01_basic_dataframe.py`](01_basic_dataframe.py) | Fetch a worksheet into a pandas DataFrame using the Layer 1 `WorksheetExporter` | `python examples/01_basic_dataframe.py` |
| 02 | [`02_streaming_parquet.py`](02_streaming_parquet.py) | Stream pages into partitioned Parquet with bounded memory | `python examples/02_streaming_parquet.py` |
| 03 | [`03_schema_discovery.py`](03_schema_discovery.py) | Discover column names + types via the schema-discovery layer | `python examples/03_schema_discovery.py` |
| 04 | [`04_s3_export.py`](04_s3_export.py) | Stream directly into an S3 bucket via `pyarrow.fs.S3FileSystem` | `python examples/04_s3_export.py` |
| 05 | [`05_filters.py`](05_filters.py) | Compose filters with the `NocolyFilter` DSL — quick / and_group / or_group | `python examples/05_filters.py` |
| 06 | [`06_service_submit.py`](06_service_submit.py) | Submit an export job to the FastAPI service and poll for completion | `python examples/06_service_submit.py` |
| 07 | [`07_writer.py`](07_writer.py) | Write rows back to a Nocoly worksheet with `WorksheetWriter` / `AsyncWorksheetWriter` — add / update / upsert / delete | `python examples/07_writer.py` |

## Required environment variables

| Variable | Used by | Purpose |
|---|---|---|
| `NOCOLY_APP_KEY` | 01, 02, 03, 04, 05 | Nocoly application key (HAP-AppKey header) |
| `NOCOLY_APP_SIGN` | 01, 02, 03, 04, 05 | Nocoly application sign (HAP-Sign header) |
| `NOCOLY_OUTPUT_DIR` | 02 | Local directory for Parquet files |
| `NOCOLY_S3_URI` | 04 | Destination URI, e.g. `s3://bucket/key` |
| `NOCOLY_SERVICE_BASE_URL` | 06 | Defaults to `http://127.0.0.1:8080` |
| `NOCOLY_SERVICE_API_KEY` | 06 | Must match the server's setting |
| `NOCOLY_WORKSET_HOST` | 06 | Nocoly host passed into the submitted job |
| `NOCOLY_WORKSET_ID` | 06 | Worksheet id passed into the submitted job |
| `NOCOLY_WORKSET_OUTPUT` | 06 | Local directory for the worker's Parquet output |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | 04 | Standard AWS credentials (or use IAM role) |

## Running the service example (06)

The service example assumes the API and worker are already running.
In two separate terminals:

```bash
# Terminal 1: Redis
docker run -d --name nocoly-redis -p 6379:6379 redis:7-alpine

# Terminal 2: API
export NOCOLY_SERVICE_API_KEY="$(openssl rand -hex 32)"
uvicorn nocoly_explorer.service:app --host 127.0.0.1 --port 8080

# Terminal 3: Worker
arq nocoly_explorer.service.worker.WorkerSettings
```

Then in a fourth:

```bash
export NOCOLY_SERVICE_API_KEY="<the same value as terminal 2>"
export NOCOLY_WORKSET_ID="ws_real"
export NOCOLY_WORKSET_OUTPUT="/tmp/nocoly-from-service"
python examples/06_service_submit.py
```

## What the scripts don't show

These examples focus on the happy path. For error handling, see:

- `nocoly_explorer.exceptions` — every exception this codebase defines
- The test files — many of them exercise failure paths

For Databricks-specific credential handling, see:

- `nocoly_explorer.auth.DatabricksCredentialProvider` — pulls from
  Databricks secret scopes via `dbutils.secrets`
