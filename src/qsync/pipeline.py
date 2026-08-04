"""Connector-agnostic incremental synchronization loop."""

from __future__ import annotations

import time

from .models import SyncStats
from .protocols import Sink, Source


class PipelineError(RuntimeError):
    """Raised when a connector violates the synchronization protocol."""


def run_sync(
    source: Source,
    sink: Sink,
    *,
    batch_size: int = 5_000,
    max_batches: int | None = None,
) -> SyncStats:
    """Synchronize source changes after the sink's committed checkpoint.

    A cursor is advanced in memory only after ``commit_batch`` returns. Sink
    implementations must atomically persist both records and the cursor, so a
    failed or interrupted invocation can safely resume from the last batch.
    """

    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if max_batches is not None and max_batches < 0:
        raise ValueError("max_batches cannot be negative")
    if not source.source_id:
        raise ValueError("source_id must not be empty")

    started = time.perf_counter()
    cursor = sink.load_checkpoint(source.source_id)
    start_cursor = cursor
    rows_read = 0
    rows_written = 0
    batches = 0

    while max_batches is None or batches < max_batches:
        batch = source.read_batch(cursor, batch_size)
        if not batch.records:
            break
        if batch.next_cursor is None:
            raise PipelineError("a non-empty batch must provide next_cursor")
        if cursor is not None and batch.next_cursor <= cursor:
            raise PipelineError("next_cursor must advance beyond the current cursor")

        write_stats = sink.commit_batch(
            source.source_id,
            batch.records,
            batch.next_cursor,
        )
        if write_stats.rows_written < 0:
            raise PipelineError("rows_written cannot be negative")

        # Advance only after the sink confirms its atomic commit.
        cursor = batch.next_cursor
        rows_read += len(batch.records)
        rows_written += write_stats.rows_written
        batches += 1

    return SyncStats(
        source_id=source.source_id,
        start_cursor=start_cursor,
        end_cursor=cursor,
        rows_read=rows_read,
        rows_written=rows_written,
        batches=batches,
        elapsed_seconds=time.perf_counter() - started,
    )


# A short alias reads naturally for callers while ``run_sync`` remains explicit
# at import sites that also expose legacy sync engines.
sync = run_sync
