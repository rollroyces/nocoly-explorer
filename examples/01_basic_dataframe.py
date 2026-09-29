"""01 — Basic one-shot fetch into a pandas DataFrame.

The simplest possible workflow: pull a worksheet from Nocoly, apply a
filter, return a ``pandas.DataFrame``.

Setup:
    pip install "nocoly-explorer[dataframe]"

Environment:
    NOCOLY_APP_KEY   — Nocoly application key (HAP-AppKey header)
    NOCOLY_APP_SIGN  — Nocoly application sign (HAP-Sign header)

Run:
    python examples/01_basic_dataframe.py
"""
from __future__ import annotations

import os

from nocoly_explorer import NocolyFilter, WorksheetExporter


def get_credentials() -> tuple[str, str]:
    """Read NOCOLY_APP_KEY and NOCOLY_APP_SIGN from the environment.

    Raises a clear error if either is missing, instead of letting the
    downstream requests call fail with a 401.
    """
    app_key = os.environ.get("NOCOLY_APP_KEY")
    app_sign = os.environ.get("NOCOLY_APP_SIGN")
    missing = [n for n, v in (("NOCOLY_APP_KEY", app_key), ("NOCOLY_APP_SIGN", app_sign)) if not v]
    if missing:
        raise SystemExit(
            f"Missing environment variables: {', '.join(missing)}. "
            "Export them before running this example."
        )
    return app_key, app_sign


def main() -> None:
    host = "https://bpm-uat.chinachemgroup.com"  # or your Nocoly host
    worksheet_id = "ws_123"                       # replace with real id

    app_key, app_sign = get_credentials()

    # Build a filter: rows where Status is "Active" and _updatedAt is past
    # 2025-01-01, AND Region is HK or SZ.
    filters = NocolyFilter.and_group([
        NocolyFilter.quick(Status="Active", _updatedAt__gt="2025-01-01"),
        NocolyFilter.or_group([
            NocolyFilter.quick(Region="HK"),
            NocolyFilter.quick(Region="SZ"),
        ]),
    ])

    df = WorksheetExporter().export(
        host=host,
        worksheet_id=worksheet_id,
        filter_criteria=filters,
        columns=["Name", "Region", "Status", "_updatedAt"],
        sorts=[{"field": "_updatedAt", "order": "DESC"}],
        page_size=500,
        max_pages=25,
        app_key=app_key,
        app_sign=app_sign,
        # Treat empty page-with-has_more as a warning, not an error:
        verify_ssl=True,
    )

    print(f"Got {len(df)} rows from {worksheet_id}")
    print(df.head(5))


if __name__ == "__main__":
    main()
