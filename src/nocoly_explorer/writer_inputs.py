"""Shared helpers for the sync / async worksheet writers.

The two writers (sync ``WorksheetWriter`` and async ``AsyncWorksheetWriter``)
share the same input-coercion contract:

- ``List[Mapping[str, Any]]`` passes through (after a shallow copy).
- ``pandas.DataFrame`` is converted via ``to_dict(orient='records')``.
- ``pyarrow.Table`` / ``pyarrow.RecordBatch`` is converted via
  ``.to_pylist()``.
- A ``str`` is treated as a local file path; ``pyarrow`` reads it
  (``pyarrow.ipc.open_stream`` is tried, then fall back to
  ``pyarrow.csv.read_csv`` / ``pyarrow.json.read_json`` based on
  extension). Both CSV and JSON files are accepted.

The coercion layer never imports ``pandas`` or ``pyarrow`` at module load
time — both are optional dependencies (the ``dataframe`` and ``streaming``
extras, respectively). Importing is attempted inside :func:`coerce_to_rows`
and the corresponding ``ImportError`` is rewritten as a
:class:`WriteValidationError` so callers see one error type.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Mapping, Sequence, Union

from .exceptions import WriteValidationError

# Public type for callers annotating parameters.
WriterInput = Union[
    Sequence[Mapping[str, Any]],
    "Any",  # pandas.DataFrame or pyarrow.Table/RecordBatch
    str,    # file path
]


def normalize_batch_size(batch_size: int, max_batch_size: int) -> int:
    """Validate and normalize a requested batch size."""
    if batch_size < 1:
        raise ValueError(f"batch_size must be >= 1 (got {batch_size})")
    if batch_size > max_batch_size:
        raise ValueError(
            f"batch_size {batch_size} exceeds limit of {max_batch_size}"
        )
    return batch_size


def coerce_to_rows(
    value: Any,
    *,
    operation: str,
) -> List[Dict[str, Any]]:
    """Convert any supported input to ``List[dict]``.

    ``operation`` is the human-readable name of the caller (e.g. ``"add"``,
    ``"update"``); used only in error messages.
    """
    if value is None:
        raise WriteValidationError(
            f"{operation}: rows input is None"
        )

    # 1. Already a list / sequence of mappings.
    if isinstance(value, str):
        return _coerce_from_path(value, operation=operation)

    if isinstance(value, Mapping):
        raise WriteValidationError(
            f"{operation}: expected a sequence of row mappings, got a single mapping. "
            "Wrap it in a list: [row]."
        )

    if isinstance(value, Sequence):
        rows = list(value)
        if not rows:
            return []
        for i, r in enumerate(rows):
            if not isinstance(r, Mapping):
                raise WriteValidationError(
                    f"{operation}: row at index {i} is {type(r).__name__}, "
                    "expected a mapping (dict-like)."
                )
        return [dict(r) for r in rows]

    # 2. Pandas DataFrame (optional dep).
    try:
        import pandas as pd  # type: ignore
    except ImportError:
        pd = None  # type: ignore
    if pd is not None and isinstance(value, pd.DataFrame):
        # orient='records' produces [{col: value}, ...] — what Nocoly expects.
        return value.to_dict(orient="records")

    # 3. PyArrow Table / RecordBatch (optional dep).
    try:
        import pyarrow as pa  # type: ignore
    except ImportError:
        pa = None  # type: ignore
    if pa is not None:
        if isinstance(value, pa.Table):
            return value.to_pylist()
        if isinstance(value, pa.RecordBatch):
            return value.to_pylist()

    raise WriteValidationError(
        f"{operation}: unsupported input type {type(value).__name__}. "
        "Pass a list of dicts, a pandas DataFrame, a pyarrow Table/RecordBatch, "
        "or a file path (csv/json/ipc)."
    )


def _coerce_from_path(path: str, *, operation: str) -> List[Dict[str, Any]]:
    """Read a local file via pyarrow and return its row dicts."""
    if not os.path.exists(path):
        raise WriteValidationError(
            f"{operation}: file path does not exist: {path!r}"
        )

    try:
        import pyarrow as pa  # type: ignore
    except ImportError as exc:
        raise WriteValidationError(
            f"{operation}: reading from a file path requires pyarrow "
            "(install the [streaming] extra or pyarrow directly)"
        ) from exc

    ext = os.path.splitext(path)[1].lower()
    try:
        if ext in (".csv", ".tsv", ".txt"):
            # pyarrow.csv may not be importable on older pyarrow builds; import lazily.
            import pyarrow.csv as pacsv  # type: ignore
            read_kwargs: Dict[str, Any] = {}
            if ext == ".tsv":
                read_kwargs["delimiter"] = "\t"
            table = pacsv.read_csv(path, **read_kwargs)
        elif ext in (".json", ".jsonl", ".ndjson"):
            import pyarrow.json as pajson  # type: ignore
            table = pajson.read_json(path)
        elif ext in (".parquet",):
            import pyarrow.parquet as papq  # type: ignore
            table = papq.read_table(path)
        elif ext in (".arrow", ".ipc"):
            import pyarrow.ipc as paipc  # type: ignore
            with paipc.open_stream(path) as reader:
                table = reader.read_all()
        else:
            # Fallback: try CSV first, then JSON.
            try:
                import pyarrow.csv as pacsv  # type: ignore
                table = pacsv.read_csv(path)
            except Exception:
                import pyarrow.json as pajson  # type: ignore
                table = pajson.read_json(path)
    except Exception as exc:
        raise WriteValidationError(
            f"{operation}: failed to read file {path!r}: {exc}"
        ) from exc

    return table.to_pylist()


__all__ = [
    "coerce_to_rows",
    "normalize_batch_size",
    "WriterInput",
]