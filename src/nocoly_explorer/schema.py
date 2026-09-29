"""Discover and infer Nocoly worksheet schemas / data types.

Nocoly v3 doesn't document a single canonical schema endpoint, so this module
provides a tolerant parser plus a sample-based inference path. ``WorksheetClient
.get_worksheet_schema`` tries the metadata endpoint first and falls back to
inference if the endpoint is missing or returns an unrecognized shape.
"""

from __future__ import annotations

import datetime
import decimal
import json
import logging
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Sequence

import pyarrow as pa

_LOGGER = logging.getLogger("nocoly_explorer.schema")


# ---------------------------------------------------------------------------
# Type mapping tables
# ---------------------------------------------------------------------------


# Nocoly-style type strings → PyArrow types. Strings outside this table map to
# ``pa.string()`` in :meth:`NocolyWorksheetSchema.to_pyarrow`.
_NOCOLY_TYPE_TO_PA: Dict[str, pa.DataType] = {
    "string": pa.string(),
    "text": pa.string(),
    "varchar": pa.string(),
    "boolean": pa.bool_(),
    "bool": pa.bool_(),
    "integer": pa.int64(),
    "int": pa.int64(),
    "long": pa.int64(),
    "number": pa.float64(),
    "float": pa.float64(),
    "double": pa.float64(),
    "decimal": pa.decimal128(38, 10),
    "datetime": pa.timestamp("us"),
    "date": pa.date32(),
    "time": pa.time64("us"),
    "list": pa.list_(pa.null()),
    "object": pa.struct([]),
}


# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class NocolyColumnInfo:
    """One column in a :class:`NocolyWorksheetSchema`.

    ``type`` is a Nocoly-style type name (``"string"``, ``"integer"``,
    ``"datetime"``, etc.). Unknown types round-trip as ``"string"``.
    """

    name: str
    type: str
    nullable: bool = True
    description: Optional[str] = None

    def to_pyarrow(self) -> pa.DataType:
        """Return the equivalent PyArrow type, defaulting to ``string``."""
        return _NOCOLY_TYPE_TO_PA.get(self.type.lower(), pa.string())


@dataclass(frozen=True)
class NocolyWorksheetSchema:
    """Worksheet schema with column metadata and provenance.

    ``source`` is either ``"api"`` (returned from a Nocoly metadata endpoint)
    or ``"inferred"`` (built from a sample of rows when no metadata was
    available). Callers that need to distinguish should consult ``source``.
    """

    worksheet_id: str
    columns: List[NocolyColumnInfo] = field(default_factory=list)
    source: str = "inferred"

    def to_pyarrow(self) -> pa.Schema:
        """Convert to a PyArrow schema. Unknown types map to ``string``."""
        return pa.schema(
            [pa.field(c.name, c.to_pyarrow(), nullable=c.nullable) for c in self.columns]
        )

    def to_json(self, *, indent: Optional[int] = 2) -> str:
        """Serialize to a JSON string for persistence / reuse."""
        return json.dumps(asdict(self), indent=indent, ensure_ascii=False)

    def to_dict(self) -> Dict[str, Any]:
        """Plain-dict form (useful for tests and inspection)."""
        return asdict(self)

    @classmethod
    def from_json(cls, text: str) -> "NocolyWorksheetSchema":
        """Deserialize from JSON (inverse of :meth:`to_json`)."""
        payload = json.loads(text)
        return cls.from_dict(payload)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "NocolyWorksheetSchema":
        columns = [
            NocolyColumnInfo(
                name=str(c["name"]),
                type=str(c.get("type", "string")),
                nullable=bool(c.get("nullable", True)),
                description=c.get("description"),
            )
            for c in payload.get("columns", [])
        ]
        return cls(
            worksheet_id=str(payload.get("worksheet_id", "")),
            columns=columns,
            source=str(payload.get("source", "inferred")),
        )


# ---------------------------------------------------------------------------
# Inference helpers
# ---------------------------------------------------------------------------


def _infer_pyarrow_type_from_value(value: Any) -> pa.DataType:
    """Map a single Python value to a PyArrow type."""
    if value is None:
        return pa.null()
    if isinstance(value, bool):
        return pa.bool_()
    if isinstance(value, int):
        return pa.int64()
    if isinstance(value, float):
        return pa.float64()
    if isinstance(value, decimal.Decimal):
        return pa.decimal128(38, 10)
    if isinstance(value, datetime.datetime):
        return pa.timestamp("us", tz="UTC") if value.tzinfo else pa.timestamp("us")
    if isinstance(value, datetime.date):
        return pa.date32()
    if isinstance(value, datetime.time):
        return pa.time64("us")
    if isinstance(value, datetime.timedelta):
        return pa.duration("us")
    if isinstance(value, uuid.UUID):
        return pa.string()
    if isinstance(value, bytes):
        return pa.binary()
    if isinstance(value, str):
        return pa.string()
    if isinstance(value, (list, tuple)):
        if value:
            return pa.list_(_infer_pyarrow_type_from_value(value[0]))
        return pa.list_(pa.null())
    if isinstance(value, dict):
        return pa.struct(
            [(str(k), _infer_pyarrow_type_from_value(v)) for k, v in value.items()]
        )
    raise TypeError(f"Cannot infer PyArrow type from {type(value).__name__}")


def _pa_type_to_nocoly_str(pa_type: pa.DataType) -> str:
    """Map a PyArrow type to a Nocoly-style type name."""
    if pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type):
        return "string"
    if pa.types.is_boolean(pa_type):
        return "boolean"
    if pa.types.is_integer(pa_type):
        return "integer"
    if pa.types.is_floating(pa_type):
        return "number"
    if pa.types.is_decimal(pa_type):
        return "decimal"
    if pa.types.is_timestamp(pa_type):
        return "datetime"
    if pa.types.is_date(pa_type):
        return "date"
    if pa.types.is_time(pa_type):
        return "time"
    if pa.types.is_list(pa_type) or pa.types.is_large_list(pa_type):
        return "list"
    if pa.types.is_struct(pa_type):
        return "struct"
    if pa.types.is_binary(pa_type) or pa.types.is_large_binary(pa_type):
        return "binary"
    return "string"


def _widen_type(left: pa.DataType, right: pa.DataType) -> pa.DataType:
    """Pick the wider of two PyArrow types.

    Widening follows the partial order:
        bool < integer < decimal < floating < string
        date < timestamp
        null < anything
    Mixed categories fall back to ``string``.
    """
    if left.equals(right):
        return left
    if pa.types.is_null(left):
        return right
    if pa.types.is_null(right):
        return left
    # bool < integer < decimal < floating < string
    if pa.types.is_boolean(left) and pa.types.is_integer(right):
        return right
    if pa.types.is_integer(left) and pa.types.is_boolean(right):
        return left
    if pa.types.is_integer(left) and pa.types.is_decimal(right):
        return right
    if pa.types.is_decimal(left) and pa.types.is_integer(right):
        return left
    if pa.types.is_integer(left) and pa.types.is_floating(right):
        return right
    if pa.types.is_floating(left) and pa.types.is_integer(right):
        return left
    if pa.types.is_decimal(left) and pa.types.is_floating(right):
        return right
    if pa.types.is_floating(left) and pa.types.is_decimal(right):
        return left
    if (pa.types.is_integer(left) or pa.types.is_floating(left)
            or pa.types.is_decimal(left) or pa.types.is_boolean(left)):
        if pa.types.is_string(right):
            return right
    if (pa.types.is_integer(right) or pa.types.is_floating(right)
            or pa.types.is_decimal(right) or pa.types.is_boolean(right)):
        if pa.types.is_string(left):
            return left
    # date < timestamp
    if pa.types.is_date(left) and pa.types.is_timestamp(right):
        return right
    if pa.types.is_timestamp(left) and pa.types.is_date(right):
        return left
    # Anything else → string.
    return pa.string()


def infer_schema_from_rows(
    rows: Sequence[Dict[str, Any]],
    *,
    sample_size: int = 256,
    worksheet_id: str = "(inferred)",
) -> NocolyWorksheetSchema:
    """Infer a :class:`NocolyWorksheetSchema` from a list of row dicts.

    ``sample_size`` controls how many leading rows are scanned to determine
    types. Types are widened across the sample (int < float < decimal <
    string) so heterogeneous columns are reported as the safest common type.
    """
    if not rows:
        raise ValueError("Cannot infer schema from empty rows")
    if sample_size < 1:
        raise ValueError(f"sample_size must be >= 1 (got {sample_size})")

    sample = list(rows[:sample_size])

    # Collect column names in the order they first appear, then any extras.
    column_names: List[str] = list(sample[0].keys())
    seen = set(column_names)
    for row in sample[1:]:
        for key in row:
            if key not in seen:
                column_names.append(key)
                seen.add(key)

    columns: List[NocolyColumnInfo] = []
    for name in column_names:
        nullable = any(
            name not in row or row.get(name) is None for row in sample
        )
        # Widen type across non-null samples.
        widened: Optional[pa.DataType] = None
        for row in sample:
            if name not in row:
                continue
            value = row.get(name)
            if value is None:
                continue
            try:
                t = _infer_pyarrow_type_from_value(value)
            except TypeError:
                t = pa.string()
            if widened is None:
                widened = t
            else:
                widened = _widen_type(widened, t)
        nocoly_type = _pa_type_to_nocoly_str(widened) if widened is not None else "string"
        columns.append(
            NocolyColumnInfo(name=name, type=nocoly_type, nullable=nullable)
        )

    return NocolyWorksheetSchema(
        worksheet_id=worksheet_id, columns=columns, source="inferred"
    )


# ---------------------------------------------------------------------------
# Tolerant parser for the metadata endpoint response
# ---------------------------------------------------------------------------


def _parse_columns_response(payload: Any) -> Optional[List[NocolyColumnInfo]]:
    """Parse a metadata endpoint response into a list of :class:`NocolyColumnInfo`.

    Returns ``None`` if the shape is unrecognized (the caller falls back to
    inference). Accepts several plausible shapes:

    - ``[{"name": "x", "type": "string", ...}, ...]`` (list of dicts)
    - ``{"columns": [...]}`` (wrapped list)
    - ``{"fields": [...]}`` (alternate key)
    - ``{"name": ["x", "y"], "type": ["string", "number"]}`` (columnar)
    """
    if isinstance(payload, list):
        return _parse_column_list(payload)

    if isinstance(payload, dict):
        for key in ("columns", "fields", "data"):
            if key in payload and isinstance(payload[key], list):
                result = _parse_column_list(payload[key])
                if result is not None:
                    return result
        # Columnar shape: {"name": [...], "type": [...]}
        if "name" in payload and "type" in payload:
            names = payload["name"]
            types = payload["type"]
            if (
                isinstance(names, list)
                and isinstance(types, list)
                and len(names) == len(types)
                and names
            ):
                nullables = payload.get("nullable")
                descriptions = payload.get("description")
                out: List[NocolyColumnInfo] = []
                for i, (n, t) in enumerate(zip(names, types)):
                    nullable = True
                    if isinstance(nullables, list) and i < len(nullables):
                        nullable = bool(nullables[i])
                    desc = None
                    if isinstance(descriptions, list) and i < len(descriptions):
                        desc = descriptions[i]
                    out.append(
                        NocolyColumnInfo(
                            name=str(n),
                            type=str(t),
                            nullable=nullable,
                            description=desc,
                        )
                    )
                return out
    return None


def _parse_column_list(items: List[Any]) -> Optional[List[NocolyColumnInfo]]:
    """Parse a list of column dicts. Returns ``None`` if any item is malformed."""
    out: List[NocolyColumnInfo] = []
    for item in items:
        if not isinstance(item, dict):
            return None
        name = item.get("name") or item.get("id") or item.get("key")
        if not name or not isinstance(name, str):
            return None
        type_str = (
            item.get("type")
            or item.get("dataType")
            or item.get("data_type")
            or item.get("kind")
            or "string"
        )
        nullable_raw = item.get("nullable", True)
        if isinstance(nullable_raw, str):
            nullable = nullable_raw.lower() not in ("no", "false", "0", "not_null")
        else:
            nullable = bool(nullable_raw)
        out.append(
            NocolyColumnInfo(
                name=name,
                type=str(type_str),
                nullable=nullable,
                description=item.get("description") or item.get("label"),
            )
        )
    return out or None


# Public alias used by WorksheetClient to avoid leaking private functions.
parse_columns_response = _parse_columns_response
