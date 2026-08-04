"""Protocols implemented by qsync source and sink connectors."""

from __future__ import annotations

from typing import Protocol, Sequence

from .models import Batch, Cursor, Record, WriteStats


class Source(Protocol):
    """Read deterministic pages ordered by ``(updated_at, id)``."""

    source_id: str

    def read_batch(self, cursor: Cursor | None, batch_size: int) -> Batch:
        """Return records strictly after ``cursor``, in ascending order."""
        ...


class Sink(Protocol):
    """Persist records and their checkpoint as a single atomic operation."""

    def load_checkpoint(self, source_id: str) -> Cursor | None:
        """Return the most recently committed cursor for ``source_id``."""
        ...

    def commit_batch(
        self,
        source_id: str,
        records: Sequence[Record],
        next_cursor: Cursor,
    ) -> WriteStats:
        """Atomically upsert ``records`` and store ``next_cursor``."""
        ...
