"""Public façade for downloading worksheet data."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Union

from .auth import (
    CredentialPair,
    DatabricksCredentialProvider,
    StandardCredentialProvider,
)
from .client import WorksheetClient
from .config import PackageConfig, load_package_config
from .detector import EnvironmentDetector, RuntimeMode
from .exceptions import MissingCredentialsError, OutputValidationError
from .filters import FilterExpression, NocolyFilter, ensure_filter_dict
from .output import OutputFormatter
from .sync_state import FileSyncStateStore  # default for incremental sync


class WorksheetExporter:
    """High-level orchestrator for worksheet downloads."""

    def __init__(self, config: PackageConfig | None = None) -> None:
        self.config = config or load_package_config()
        self._detector = EnvironmentDetector(self.config)

    def export(
        self,
        *,
        host: str,
        worksheet_id: str,
        output_type: str | None = None,
        filter_criteria: Optional[Union[Dict[str, Any], FilterExpression]] = None,
        columns: Optional[List[str]] = None,
        sorts: Optional[List[Dict[str, Any]]] = None,
        page_size: int = 200,
        max_pages: int = 1000,
        view_id: str = "",
        file_path: Optional[str] = None,
        file_format: Optional[str] = None,
        env_prefix: Optional[str] = None,
        app_key: Optional[str] = None,
        app_sign: Optional[str] = None,
        databricks_scope: Optional[str] = None,
        databricks_app_key_secret: Optional[str] = None,
        databricks_app_sign_secret: Optional[str] = None,
        verify_ssl: bool = True,
        logger: Optional[logging.Logger] = None,
        incremental: bool = False,
        state_store: Any = None,
        force_full: bool = False,
        updated_at_column: str = "_updatedAt",
    ) -> Any:
        """Download worksheet rows and format them as requested.

        Incremental sync (``incremental=True``): when set, the exporter
        consults ``state_store`` for the last watermark, fetches only
        rows newer than that, and writes a new watermark on success.
        Pass ``force_full=True`` to ignore the watermark (full re-sync).
        ``state_store`` defaults to ``FileSyncStateStore()`` at
        ``./.nocoly-state``; pass an instance of either file or redis
        backend for explicit control.
        """

        if not host:
            raise ValueError("host is required")
        if not worksheet_id:
            raise ValueError("worksheet_id is required")

        # Resolve incremental state before building the request.
        since_dict: Optional[Dict[str, Any]] = None
        store: Any = None
        if incremental:
            store = state_store if state_store is not None else FileSyncStateStore()
            if force_full and hasattr(store, "clear"):
                store.clear(worksheet_id)
            watermark_obj = store.get(worksheet_id)
            if watermark_obj.watermark and not force_full:
                since_dict = {
                    "type": "group",
                    "logic": "AND",
                    "filters": [
                        {
                            "type": "condition",
                            "field": updated_at_column,
                            "operator": "GT",
                            "value": watermark_obj.watermark,
                        }
                    ],
                }

        runtime_mode = self._detector.detect()
        credentials = self._resolve_credentials(
            runtime_mode=runtime_mode,
            app_key=app_key,
            app_sign=app_sign,
            env_prefix=env_prefix,
            databricks_scope=databricks_scope,
            databricks_app_key_secret=databricks_app_key_secret,
            databricks_app_sign_secret=databricks_app_sign_secret,
        )

        client = WorksheetClient(
            host=host,
            worksheet_id=worksheet_id,
            credentials=credentials,
            timeout_seconds=self.config.request_timeout_seconds,
            max_retries=self.config.max_retries,
            verify_ssl=verify_ssl,
            logger=logger,
        )
        normalized_filters = ensure_filter_dict(filter_criteria)
        if since_dict is not None:
            # AND the user's filter with the since-filter so the
            # incremental constraint composes rather than overrides.
            # (No nested ensure_filter_dict call here; since_dict is
            # already in DSL shape.)
            if normalized_filters is None:
                normalized_filters = since_dict
            else:
                # Build an AND group from the two DSL-shaped dicts.
                # NocolyFilter.and_group accepts FilterExpression children,
                # so we wrap each dict in a quick-style no-op... actually,
                # the simplest path is to nest them inside one outer AND.
                # ensure_filter_dict produces dicts with the right shape;
                # we synthesize a parent AND by calling quick() with a
                # wildcard and replacing the children. Since this is hot
                # path code, we keep it explicit.
                combined = NocolyFilter.and_group([
                    NocolyFilter.quick(),  # placeholder, replaced below
                ])
                # Easiest: just construct the parent AND via and_group
                # with two children expressed as nested groups. Since
                # ensure_filter_dict returns a dict with structure
                # {"type": "group", "logic": "AND", "filters": [...]},
                # we can put them as nested groups in one combined AND.
                combined = {
                    "type": "group",
                    "logic": "AND",
                    "filters": [normalized_filters, since_dict],
                }
                normalized_filters = combined

        rows = client.fetch_rows(
            columns=columns,
            filter_criteria=normalized_filters,
            sorts=sorts,
            page_size=page_size,
            view_id=view_id,
            max_pages=max_pages,
        )

        formatter = OutputFormatter(runtime_mode)
        resolved_output_type = output_type or self.config.default_output_type
        result = formatter.format(
            rows=rows,
            output_type=resolved_output_type,
            file_path=file_path,
            file_format=file_format,
        )

        if incremental:
            # Compute the new watermark from the rows we just fetched
            # and persist it. Only move the watermark forward.
            from .sync_state import SyncWatermark, max_watermark
            new_watermark = max_watermark(rows, column=updated_at_column)
            existing = store.get(worksheet_id)
            final_watermark = new_watermark
            if (
                existing.watermark is not None
                and new_watermark is not None
                and existing.watermark > new_watermark  # ISO 8601 lex compare
            ):
                # All rows we fetched were older than what we already
                # had recorded (out-of-order arrival). Don't move the
                # watermark backward.
                final_watermark = existing.watermark
            from .sync_state import _now_iso
            store.set(SyncWatermark(
                worksheet_id=worksheet_id,
                watermark=final_watermark,
                last_run_at=_now_iso(),
                rows_synced=len(rows) if hasattr(rows, "__len__") else 0,
            ))

        return result

    def _resolve_credentials(
        self,
        *,
        runtime_mode: RuntimeMode,
        app_key: Optional[str],
        app_sign: Optional[str],
        env_prefix: Optional[str],
        databricks_scope: Optional[str],
        databricks_app_key_secret: Optional[str],
        databricks_app_sign_secret: Optional[str],
    ) -> CredentialPair:
        if runtime_mode == "databricks":
            mapping = self.config.databricks
            scope = databricks_scope or mapping.scope
            key_secret = databricks_app_key_secret or mapping.app_key_secret
            sign_secret = databricks_app_sign_secret or mapping.app_sign_secret
            if not scope or not key_secret or not sign_secret:
                raise MissingCredentialsError(
                    "Databricks mode requires scope + secret names. Provide them via args or config."
                )
            provider = DatabricksCredentialProvider(
                scope=scope,
                app_key_secret=key_secret,
                app_sign_secret=sign_secret,
            )
            return provider.get_credentials()

        provider = StandardCredentialProvider(
            explicit_app_key=app_key,
            explicit_app_sign=app_sign,
            env_prefix=env_prefix,
            config=self.config,
        )
        return provider.get_credentials()
