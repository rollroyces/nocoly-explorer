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

This module also exposes :func:`encode_payload` — a JSON encoder that
extends the default to handle the common Python types that appear in
real pandas / pyarrow outputs (``datetime``, ``date``, ``time``,
``timedelta``, ``Decimal``, ``UUID``, numpy scalars). The sync and async
writers use it to serialize request bodies so the
``TypeError: Object of type X is not JSON serializable`` problem
doesn't surface on the user.
"""

from __future__ import annotations

import datetime as _dt
import decimal as _decimal
import json as _json
import os
import uuid as _uuid
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


def chunk_into_batches(
    items: Sequence[Any],
    *,
    batch_size: int,
    max_batch_size: int,
) -> List[List[Any]]:
    """Split ``items`` into sub-lists of size ``<= batch_size``.

    Empty input returns an empty list (callers should treat this as a no-op).
    """
    bs = normalize_batch_size(batch_size, max_batch_size)
    if not items:
        return []
    return [list(items[i : i + bs]) for i in range(0, len(items), bs)]


def coerce_id(value: Any) -> Any:
    """Coerce a row id to a JSON-friendly primitive.

    Strings, ints, floats, and bools pass through unchanged. UUIDs, Decimals,
    datetimes, and any other non-primitive value are stringified — Nocoly's
    write endpoints almost universally accept string ids.
    """
    if isinstance(value, (str, float, int, bool)):
        return value
    return str(value)


def validate_key_present(
    rows: Sequence[Mapping[str, Any]],
    key: str,
) -> None:
    """Reject rows missing ``key`` (or with ``None`` for it) before sending."""
    if not rows:
        raise WriteValidationError("rows must contain at least one element")
    missing = [i for i, r in enumerate(rows) if key not in r or r.get(key) is None]
    if missing:
        preview = ", ".join(str(i) for i in missing[:5])
        raise WriteValidationError(
            f"{len(missing)} row(s) missing key column {key!r} "
            f"(first indices: {preview})"
        )


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


# ---------------------------------------------------------------------------
# JSON encoder for request payloads
# ---------------------------------------------------------------------------
#
# ``requests`` (via ``json=`` kwarg) and ``aiohttp`` (via ``json=`` kwarg) both
# use stdlib ``json.dumps`` with no custom encoder. That fails immediately
# when a pandas DataFrame, pyarrow Table, or any user-built payload contains
# ``datetime`` / ``date`` / ``Decimal`` / ``UUID`` / numpy values — exactly the
# values real Nocoetry data carries.
#
# We serialize the payload ourselves with :func:`encode_payload` and pass
# the result as a pre-serialized ``data=`` string with the right
# ``Content-Type`` header. This works identically with both ``requests`` and
# ``aiohttp``, is allocation-cheap (single ``json.dumps`` per batch), and
# surfaces a clean error if a truly unsupported type sneaks in.


def _json_default(obj: Any) -> Any:
    """Default encoder for ``json.dumps`` covering common pandas/pyarrow values."""
    # Order: datetime first because datetime is a common pandas/pyarrow output
    # and is the most likely thing to hit a user.
    if isinstance(obj, (_dt.datetime, _dt.date)):
        return obj.isoformat()
    if isinstance(obj, _dt.time):
        return obj.isoformat()
    if isinstance(obj, _dt.timedelta):
        # Nocoly doesn't have a documented duration type; this is a reasonable
        # lossless default and matches what pandas/pyarrow emit.
        return obj.total_seconds()
    if isinstance(obj, _decimal.Decimal):
        # ``float`` loses precision for very large or very small numbers.
        # String preserves precision; pick the safer default.
        return str(obj)
    if isinstance(obj, _uuid.UUID):
        return str(obj)
    # numpy scalars — pyarrow ``to_pylist`` returns these on some paths,
    # especially for older pyarrow versions or specific dtypes.
    try:
        import numpy as _np  # type: ignore

        if isinstance(obj, _np.bool_):
            return bool(obj)
        if isinstance(obj, _np.integer):
            return int(obj)
        if isinstance(obj, _np.floating):
            return float(obj)
        if isinstance(obj, _np.datetime64):
            # numpy has no portable datetime-as-string helper; fall back to
            # converting via datetime64 -> ISO string.
            return _np.datetime_as_string(obj, unit="us").replace("T", " ")
        # ndarray: shouldn't normally appear inside a row dict, but be defensive.
        if isinstance(obj, _np.ndarray):
            return obj.tolist()
    except ImportError:
        pass
    raise TypeError(
        f"Object of type {type(obj).__name__} is not JSON serializable for Nocoly writes. "
        "Convert to a JSON-friendly primitive (str, int, float, bool, dict, list, ISO date/datetime) before passing in."
    )


def encode_payload(payload: Any) -> str:
    """Serialize a paywall to a JSON string using :func:`_json_default`.

    Both ``requests`` and ``aiohttp`` accept a pre-serialized string via
    ``data=``; pass the result alongside ``Content-Type: application/json``.
    """
    return _json.dumps(
        payload,
        default=_json_default,
        ensure_ascii=False,
        allow_nan=True,
        separators=(",", ":"),
    )


__all__ = [
    "coerce_to_rows",
    "normalize_batch_size",
    "chunk_into_batches",
    "coerce_id",
    "validate_key_present",
    "WriterInput",
    "encode_payload",
]