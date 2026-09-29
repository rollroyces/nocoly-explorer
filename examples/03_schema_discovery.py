"""03 — Discover a worksheet's column schema before exporting.

``WorksheetClient.get_worksheet_schema`` returns a
``NocolyWorksheetSchema`` with column names, types, and source
("api" if the metadata endpoint returned data, "inferred" if we fell
back to a sample).

Useful for:
- Pre-flight validation ("does this worksheet have the columns I expect?")
- Round-tripping a schema to JSON for reuse next time
- Building dynamic pipelines that adapt to schema changes

Setup:
    pip install "nocoly-explorer"

Environment:
    NOCOLY_APP_KEY, NOCOLY_APP_SIGN

Run:
    python examples/03_schema_discovery.py
"""
from __future__ import annotations

import json
import os

from nocoly_explorer import NocolyWorksheetSchema
from nocoly_explorer.auth import CredentialPair
from nocoly_explorer.client import WorksheetClient


def get_credentials() -> tuple[str, str]:
    app_key = os.environ.get("NOCOLY_APP_KEY")
    app_sign = os.environ.get("NOCOLY_APP_SIGN")
    if not app_key or not app_sign:
        raise SystemExit(
            "Missing NOCOLY_APP_KEY / NOCOLY_APP_SIGN. Export them before running."
        )
    return app_key, app_sign


def main() -> None:
    host = "https://bpm-uat.chinachemgroup.com"
    worksheet_id = "ws_123"

    app_key, app_sign = get_credentials()
    client = WorksheetClient(
        host=host,
        worksheet_id=worksheet_id,
        credentials=CredentialPair(app_key=app_key, app_sign=app_sign),
    )

    # Tries the metadata endpoint first; falls back to sample inference
    # on 404, network error, or unrecognized response.
    # Set NOCOLY_SCHEMA_API_DISABLED=1 (or pass try_api=False) to skip
    # the network call entirely and use inference only.
    schema = client.get_worksheet_schema(sample_size=10)

    print(f"Schema source: {schema.source}")
    print(f"Worksheet: {schema.worksheet_id}")
    print(f"Columns: {len(schema.columns)}")
    for col in schema.columns:
        print(f"  {col.name:30s}  {col.type:12s}  nullable={col.nullable}")

    # Round-trip to JSON so you can persist the schema and reuse it
    # later (e.g. as the explicit_schema for a StreamingExporter).
    json_text = schema.to_json(indent=2)
    print("--- schema.json ---")
    print(json_text)

    # Round-trip back:
    roundtripped = NocolyWorksheetSchema.from_json(json_text)
    assert roundtripped == schema, "round-trip mismatch"


if __name__ == "__main__":
    main()
