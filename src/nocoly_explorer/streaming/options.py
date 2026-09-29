"""Configuration objects for streaming Parquet exports."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional, Union

from ..exceptions import OutputValidationError

_VALID_GRANULARITIES = ("day", "month", "year")
_VALID_DRIFT_POLICIES = ("ignore", "error")
_VALID_COMPRESSIONS = ("snappy", "gzip", "zstd", "lz4", "brotli", None)

_PARTITION_KEY_RE = re.compile(r"^[A-Za-z0-9_.\-]+=[A-Za-z0-9_.\-]+$")


@dataclass(frozen=True, slots=True)
class PartitionSpec:
    """How to partition the output Parquet dataset."""

    column: str
    granularity: Optional[Literal["day", "month", "year"]] = None

    def __post_init__(self) -> None:
        # column may be empty for "placeholder" specs that aren't actively used
        # (e.g. when StreamingExporter constructs a Router for the no-partition case).
        if self.granularity is not None and self.granularity not in _VALID_GRANULARITIES:
            raise ValueError(
                f"PartitionSpec.granularity must be one of {_VALID_GRANULARITIES} or None; "
                f"got {self.granularity!r}"
            )


@dataclass(frozen=True, slots=True)
class ParquetExportOptions:
    """Tunables for the streaming exporter."""

    row_group_bytes: int = 128 * 1024 * 1024
    max_partition_cardinality: int = 10_000
    on_schema_drift: Literal["ignore", "error"] = "ignore"
    compression: Optional[str] = "snappy"
    write_statistics: bool = True

    def __post_init__(self) -> None:
        if self.row_group_bytes < 1024 * 1024:
            raise ValueError(
                f"row_group_bytes must be >= 1 MiB (got {self.row_group_bytes})"
            )
        if self.max_partition_cardinality < 1:
            raise ValueError(
                f"max_partition_cardinality must be >= 1 (got {self.max_partition_cardinality})"
            )
        if self.on_schema_drift not in _VALID_DRIFT_POLICIES:
            raise ValueError(
                f"on_schema_drift must be one of {_VALID_DRIFT_POLICIES}; "
                f"got {self.on_schema_drift!r}"
            )
        if self.compression not in _VALID_COMPRESSIONS:
            raise ValueError(
                f"compression must be one of {_VALID_COMPRESSIONS}; "
                f"got {self.compression!r}"
            )


def validate_output_dir(path: Union[str, Path]) -> Path:
    """Validate and create the output directory. Rejects files and .parquet paths."""
    p = Path(path)
    if p.suffix.lower() == ".parquet":
        raise OutputValidationError(
            f"output_dir must be a directory, not a Parquet file path. Got {p!s}"
        )
    if p.exists() and not p.is_dir():
        raise OutputValidationError(
            f"output_dir must be a directory; {p!s} exists and is a file."
        )
    p.mkdir(parents=True, exist_ok=True)
    return p


def resolve_partition_path(output_dir, partition_key: str):
    """Resolve a Hive-style partition key to a directory under output_dir.

    Partition keys look like 'created_date=2026-09-28'. Path traversal and
    absolute paths are rejected to keep the export sandboxed.

    Accepts either a ``pathlib.Path`` (local filesystem) or a ``str``
    starting with a URI scheme such as ``s3://bucket/key/`` (any
    ``pyarrow.fs.FileSystem``-compatible destination). String-typed
    output dirs are concatenated without filesystem-specific path
    resolution.
    """
    if not _PARTITION_KEY_RE.match(partition_key):
        raise ValueError(
            f"Invalid partition key {partition_key!r}; expected 'column=value' with safe characters."
        )
    if isinstance(output_dir, Path):
        partition_path = (output_dir / partition_key).resolve()
        output_resolved = output_dir.resolve()
        if not str(partition_path).startswith(str(output_resolved) + "/"):
            raise ValueError(f"Partition key {partition_key!r} escapes output_dir")
        return partition_path
    # Non-Path destination (e.g. s3:// URI). Concatenate as strings,
    # preserving the trailing slash on the base if present.
    base = str(output_dir)
    if not base.endswith("/"):
        base = base + "/"
    return base + partition_key