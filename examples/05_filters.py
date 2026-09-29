"""05 — Composing filters with the NocolyFilter DSL.

The ``NocolyFilter`` DSL mirrors the v3 Nocoly worksheet query language:
``and_group`` / ``or_group`` combine sub-expressions, ``quick(**kwargs)``
builds AND conditions from keyword arguments (e.g. ``Status="Active"``,
``_updatedAt__gt="2025-01-01"``).

Depth limit: the DSL enforces a depth of 3 (root AND → OR group → AND
group from quick()). Nocoly accepts up to two nested groups under the
root.

Setup:
    pip install "nocoly-explorer"

Environment:
    NOCOLY_APP_KEY, NOCOLY_APP_SIGN

Run:
    python examples/05_filters.py
"""
from __future__ import annotations

import os

from nocoly_explorer import NocolyFilter, WorksheetExporter


def get_credentials() -> tuple[str, str]:
    app_key = os.environ.get("NOCOLY_APP_KEY")
    app_sign = os.environ.get("NOCOLY_APP_SIGN")
    if not app_key or not app_sign:
        raise SystemExit("Missing NOCOLY_APP_KEY / NOCOLY_APP_SIGN.")
    return app_key, app_sign


def build_filter_examples() -> dict[str, dict]:
    """A few filter recipes showing the DSL patterns."""
    return {
        "active rows in HK or SZ, updated this year": NocolyFilter.and_group([
            NocolyFilter.quick(Status="Active", _updatedAt__gt="2025-01-01"),
            NocolyFilter.or_group([
                NocolyFilter.quick(Region="HK"),
                NocolyFilter.quick(Region="SZ"),
            ]),
        ]),
        "name contains 'Acme' (case-sensitive)": NocolyFilter.quick(
            Name__contains="Acme",
        ),
        "name does NOT contain 'Test'": NocolyFilter.quick(
            Name__not_contains="Test",
        ),
        "score between 80 and 100": NocolyFilter.and_group([
            NocolyFilter.quick(Score__gte=80),
            NocolyFilter.quick(Score__lte=100),
        ]),
        "category in a small whitelist": NocolyFilter.quick(
            Category__in=["alpha", "beta", "gamma"],
        ),
        "name field is non-empty": NocolyFilter.quick(
            Name__not_empty=True,
        ),
        "three-way region split with an exclusion": NocolyFilter.and_group([
            NocolyFilter.quick(_updatedAt__gt="2024-01-01"),
            NocolyFilter.or_group([
                NocolyFilter.quick(Region="HK"),
                NocolyFilter.quick(Region="SZ"),
                NocolyFilter.quick(Region__neq="Closed"),
            ]),
        ]),
    }


def main() -> None:
    host = "https://bpm-uat.chinachemgroup.com"
    worksheet_id = "ws_123"
    app_key, app_sign = get_credentials()

    examples = build_filter_examples()
    for label, filter_obj in examples.items():
        print(f"--- {label} ---")
        # Each filter can be passed as filter_criteria= or serialized
        # manually via filter_obj.to_dict() if you need to log it.
        print(filter_obj.to_dict())

        rows = WorksheetExporter().export(
            host=host,
            worksheet_id=worksheet_id,
            output_type="json",  # raw list-of-dicts, easy to inspect
            filter_criteria=filter_obj,
            page_size=200,
            max_pages=2,           # cap so the demo doesn't run forever
            app_key=app_key,
            app_sign=app_sign,
        )
        print(f"  Got {len(rows)} rows")
        if rows:
            print(f"  First row sample: {rows[0]}")


if __name__ == "__main__":
    main()
