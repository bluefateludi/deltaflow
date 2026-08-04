"""Shared data models for connector-agnostic incremental synchronization."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping


@dataclass(frozen=True, order=True)
class Cursor:
    """A stable, lexicographically ordered incremental position.

    ``id`` is the tie-breaker that prevents rows from being skipped when many
    records share the same ``updated_at`` value.
    """

    updated_at: str
    id: str


@dataclass(frozen=True)
class Record:
    """A connector-neutral record plus its incremental ordering fields."""

    id: str
    updated_at: str
    values: Mapping[str, object] = field(default_factory=dict)

    @property
    def cursor(self) -> Cursor:
        return Cursor(updated_at=self.updated_at, id=self.id)


@dataclass(frozen=True)
class Batch:
    """A page returned by a source and the cursor after that page."""

    records: tuple[Record, ...]
    next_cursor: Cursor | None


@dataclass(frozen=True)
class WriteStats:
    """Result of atomically committing one batch to a sink."""

    rows_written: int


@dataclass(frozen=True)
class SyncStats:
    """Summary of work performed by one pipeline invocation."""

    source_id: str
    start_cursor: Cursor | None
    end_cursor: Cursor | None
    rows_read: int
    rows_written: int
    batches: int
    elapsed_seconds: float

    @property
    def rows_per_second(self) -> float:
        return self.rows_read / self.elapsed_seconds if self.elapsed_seconds else 0.0

    def as_dict(self) -> dict[str, object]:
        def serialize_cursor(cursor: Cursor | None) -> dict[str, str] | None:
            if cursor is None:
                return None
            return {"updated_at": cursor.updated_at, "id": cursor.id}

        return {
            "source_id": self.source_id,
            "start_cursor": serialize_cursor(self.start_cursor),
            "end_cursor": serialize_cursor(self.end_cursor),
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "batches": self.batches,
            "elapsed_seconds": round(self.elapsed_seconds, 6),
            "rows_per_second": round(self.rows_per_second, 2),
        }
