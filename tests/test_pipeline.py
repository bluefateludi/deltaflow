import unittest

from qsync.models import Batch, Cursor, Record, WriteStats
from qsync.pipeline import PipelineError, run_sync


def record(number: int, timestamp: str = "2026-08-04T10:00:00Z") -> Record:
    return Record(
        id=f"order-{number:03d}",
        updated_at=timestamp,
        values={"status": "paid", "amount": number},
    )


class MemorySource:
    source_id = "orders:test"

    def __init__(self, records: list[Record]) -> None:
        self.records = sorted(records, key=lambda item: item.cursor)

    def read_batch(self, cursor: Cursor | None, batch_size: int) -> Batch:
        pending = [item for item in self.records if cursor is None or item.cursor > cursor]
        page = tuple(pending[:batch_size])
        return Batch(page, page[-1].cursor if page else cursor)


class MemorySink:
    def __init__(self) -> None:
        self.checkpoints: dict[str, Cursor] = {}
        self.records: dict[str, Record] = {}
        self.fail_next_commit = False

    def load_checkpoint(self, source_id: str) -> Cursor | None:
        return self.checkpoints.get(source_id)

    def commit_batch(
        self,
        source_id: str,
        records: tuple[Record, ...],
        next_cursor: Cursor,
    ) -> WriteStats:
        if self.fail_next_commit:
            self.fail_next_commit = False
            raise RuntimeError("simulated transaction failure")
        for item in records:
            existing = self.records.get(item.id)
            if existing is None or item.cursor >= existing.cursor:
                self.records[item.id] = item
        self.checkpoints[source_id] = next_cursor
        return WriteStats(rows_written=len(records))


class PipelineTests(unittest.TestCase):
    def test_syncs_multiple_batches_and_reports_stats(self) -> None:
        source = MemorySource([record(index) for index in range(7)])
        sink = MemorySink()

        stats = run_sync(source, sink, batch_size=3)

        self.assertEqual(stats.rows_read, 7)
        self.assertEqual(stats.rows_written, 7)
        self.assertEqual(stats.batches, 3)
        self.assertIsNone(stats.start_cursor)
        self.assertEqual(stats.end_cursor, record(6).cursor)
        self.assertEqual(sink.load_checkpoint(source.source_id), record(6).cursor)

    def test_max_batches_stops_then_next_run_resumes(self) -> None:
        source = MemorySource([record(index) for index in range(8)])
        sink = MemorySink()

        interrupted = run_sync(source, sink, batch_size=3, max_batches=1)
        resumed = run_sync(source, sink, batch_size=3)

        self.assertEqual(interrupted.rows_read, 3)
        self.assertEqual(interrupted.end_cursor, record(2).cursor)
        self.assertEqual(resumed.start_cursor, record(2).cursor)
        self.assertEqual(resumed.rows_read, 5)
        self.assertEqual(len(sink.records), 8)

    def test_failed_commit_does_not_advance_checkpoint(self) -> None:
        source = MemorySource([record(1), record(2)])
        sink = MemorySink()
        sink.fail_next_commit = True

        with self.assertRaisesRegex(RuntimeError, "transaction failure"):
            run_sync(source, sink, batch_size=1)

        self.assertIsNone(sink.load_checkpoint(source.source_id))
        resumed = run_sync(source, sink, batch_size=1)
        self.assertEqual(resumed.rows_read, 2)

    def test_composite_cursor_keeps_rows_with_same_timestamp(self) -> None:
        source = MemorySource([record(index) for index in range(5)])
        sink = MemorySink()

        first = run_sync(source, sink, batch_size=2, max_batches=1)
        second = run_sync(source, sink, batch_size=2)

        self.assertEqual(first.end_cursor, record(1).cursor)
        self.assertEqual(second.rows_read, 3)
        self.assertEqual(len(sink.records), 5)

    def test_rejects_non_advancing_or_missing_batch_cursor(self) -> None:
        sink = MemorySink()

        class BrokenSource(MemorySource):
            def read_batch(self, cursor: Cursor | None, batch_size: int) -> Batch:
                return Batch((record(1),), None)

        with self.assertRaisesRegex(PipelineError, "next_cursor"):
            run_sync(BrokenSource([]), sink)

    def test_noop_and_zero_max_batches_do_not_read_or_write(self) -> None:
        source = MemorySource([])
        sink = MemorySink()
        initial = run_sync(source, sink)
        limited = run_sync(MemorySource([record(1)]), sink, max_batches=0)

        self.assertEqual(initial.rows_read, 0)
        self.assertEqual(initial.batches, 0)
        self.assertEqual(limited.rows_read, 0)
        self.assertIsNone(limited.end_cursor)

    def test_validates_options(self) -> None:
        source = MemorySource([])
        sink = MemorySink()
        with self.assertRaises(ValueError):
            run_sync(source, sink, batch_size=0)
        with self.assertRaises(ValueError):
            run_sync(source, sink, max_batches=-1)


if __name__ == "__main__":
    unittest.main()
