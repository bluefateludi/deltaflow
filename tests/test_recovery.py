"""Recovery and replay tests across the public pipeline/SQLite seam."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from typing import Sequence

from qsync.connectors.sqlite import SQLiteOrderSink, SQLiteOrderSource
from qsync.models import Cursor, Record, WriteStats
from qsync.pipeline import run_sync


def order(number: int, updated_at: str, *, status: str = "paid") -> Record:
    return Record(
        id=f"order-{number:03d}",
        updated_at=updated_at,
        values={
            "customer_id": f"customer-{number:03d}",
            "status": status,
            "amount": number + 0.25,
        },
    )


class FailOnCommit:
    """Fail before delegating one commit, like a process lost between batches."""

    def __init__(self, sink: SQLiteOrderSink, commit_number: int) -> None:
        self.sink = sink
        self.commit_number = commit_number
        self.commits = 0

    def load_checkpoint(self, source_id: str) -> Cursor | None:
        return self.sink.load_checkpoint(source_id)

    def commit_batch(
        self,
        source_id: str,
        records: Sequence[Record],
        next_cursor: Cursor,
    ) -> WriteStats:
        self.commits += 1
        if self.commits == self.commit_number:
            raise RuntimeError(f"interrupted before commit {self.commits}")
        return self.sink.commit_batch(source_id, records, next_cursor)


class RecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        root = Path(self.tempdir.name)
        self.source_path = root / "source.db"
        self.target_path = root / "target.db"
        with sqlite3.connect(self.source_path) as connection:
            connection.execute(
                "CREATE TABLE orders("
                "id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, "
                "status TEXT NOT NULL, amount NUMERIC NOT NULL, "
                "updated_at TEXT NOT NULL)"
            )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def insert_orders(self, records: Sequence[Record]) -> None:
        with sqlite3.connect(self.source_path) as connection:
            connection.executemany(
                "INSERT INTO orders(id, customer_id, status, amount, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                [
                    (
                        item.id,
                        item.values["customer_id"],
                        item.values["status"],
                        item.values["amount"],
                        item.updated_at,
                    )
                    for item in records
                ],
            )

    def target_count(self) -> int:
        with sqlite3.connect(self.target_path) as connection:
            return int(connection.execute("SELECT COUNT(*) FROM orders").fetchone()[0])

    def test_interruption_on_nth_batch_resumes_from_last_commit(self) -> None:
        timestamp = "2026-08-04T10:00:00Z"
        records = [order(index, timestamp) for index in range(7)]
        self.insert_orders(records)
        source = SQLiteOrderSource(self.source_path, source_id="orders:recovery")
        durable_sink = SQLiteOrderSink(self.target_path)

        with self.assertRaisesRegex(RuntimeError, "commit 3"):
            run_sync(source, FailOnCommit(durable_sink, 3), batch_size=2)

        self.assertEqual(self.target_count(), 4)
        self.assertEqual(durable_sink.load_checkpoint(source.source_id), records[3].cursor)

        resumed = run_sync(source, durable_sink, batch_size=2)

        self.assertEqual(resumed.start_cursor, records[3].cursor)
        self.assertEqual((resumed.rows_read, resumed.batches), (3, 2))
        self.assertEqual(self.target_count(), 7)
        self.assertEqual(durable_sink.load_checkpoint(source.source_id), records[-1].cursor)

    def test_replaying_committed_batch_is_idempotent(self) -> None:
        timestamp = "2026-08-04T10:00:00Z"
        records = [order(index, timestamp) for index in range(3)]
        self.insert_orders(records)
        source = SQLiteOrderSource(self.source_path, source_id="orders:replay")
        sink = SQLiteOrderSink(self.target_path)
        batch = source.read_batch(None, 10)
        assert batch.next_cursor is not None

        first = sink.commit_batch(source.source_id, batch.records, batch.next_cursor)
        replay = sink.commit_batch(source.source_id, batch.records, batch.next_cursor)

        self.assertEqual((first.rows_written, replay.rows_written), (3, 0))
        self.assertEqual(self.target_count(), 3)
        self.assertEqual(sink.load_checkpoint(source.source_id), batch.next_cursor)

    def test_same_timestamp_boundary_survives_stop_and_resume(self) -> None:
        timestamp = "2026-08-04T10:00:00Z"
        records = [order(index, timestamp) for index in range(6)]
        self.insert_orders(list(reversed(records)))
        source = SQLiteOrderSource(self.source_path, source_id="orders:boundary")
        sink = SQLiteOrderSink(self.target_path)

        stopped = run_sync(source, sink, batch_size=2, max_batches=2)
        resumed = run_sync(source, sink, batch_size=2)

        self.assertEqual(stopped.end_cursor, records[3].cursor)
        self.assertEqual(resumed.start_cursor, records[3].cursor)
        self.assertEqual(resumed.rows_read, 2)
        self.assertEqual(self.target_count(), 6)

    def test_late_stale_version_cannot_replace_newer_target_or_checkpoint(self) -> None:
        sink = SQLiteOrderSink(self.target_path)
        latest = order(1, "2026-08-04T11:00:00Z", status="shipped")
        stale = order(1, "2026-08-04T10:00:00Z", status="pending")

        sink.commit_batch("orders:late", [latest], latest.cursor)
        stale_result = sink.commit_batch("orders:late", [stale], stale.cursor)

        self.assertEqual(stale_result.rows_written, 0)
        self.assertEqual(sink.load_checkpoint("orders:late"), latest.cursor)
        with sqlite3.connect(self.target_path) as connection:
            target = connection.execute(
                "SELECT status, updated_at FROM orders WHERE id = ?", (latest.id,)
            ).fetchone()
        self.assertEqual(target, ("shipped", latest.updated_at))


if __name__ == "__main__":
    unittest.main()
