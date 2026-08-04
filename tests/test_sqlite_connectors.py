import sqlite3
import tempfile
import unittest
from pathlib import Path

from qsync.connectors.sqlite import SQLiteOrderSink, SQLiteOrderSource
from qsync.models import Cursor, Record
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


class SQLiteConnectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        root = Path(self.tempdir.name)
        self.source_path = root / "source.db"
        self.target_path = root / "target.db"
        with sqlite3.connect(self.source_path) as connection:
            connection.execute(
                """
                CREATE TABLE orders (
                    id TEXT PRIMARY KEY,
                    customer_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    amount NUMERIC NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def insert_source_orders(self, records: list[Record]) -> None:
        with sqlite3.connect(self.source_path) as connection:
            connection.executemany(
                """
                INSERT INTO orders(id, customer_id, status, amount, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
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

    def test_keyset_pagination_does_not_skip_equal_timestamps(self) -> None:
        timestamp = "2026-08-04T10:00:00Z"
        records = [order(index, timestamp) for index in range(7)]
        self.insert_source_orders(list(reversed(records)))
        source = SQLiteOrderSource(self.source_path, source_id="orders:test")
        sink = SQLiteOrderSink(self.target_path)

        stats = run_sync(source, sink, batch_size=2)

        self.assertEqual(stats.rows_read, 7)
        self.assertEqual(stats.batches, 4)
        self.assertEqual(sink.load_checkpoint(source.source_id), records[-1].cursor)
        with sqlite3.connect(self.target_path) as connection:
            ids = [row[0] for row in connection.execute("SELECT id FROM orders ORDER BY id")]
        self.assertEqual(ids, [item.id for item in records])

    def test_replaying_batch_is_idempotent_and_stale_version_cannot_win(self) -> None:
        sink = SQLiteOrderSink(self.target_path)
        latest = order(1, "2026-08-04T11:00:00Z", status="shipped")
        stale = order(1, "2026-08-04T10:00:00Z", status="pending")

        first = sink.commit_batch("orders:test", [latest], latest.cursor)
        replay = sink.commit_batch("orders:test", [latest], latest.cursor)
        stale_write = sink.commit_batch("orders:test", [stale], stale.cursor)

        self.assertEqual(first.rows_written, 1)
        self.assertEqual(replay.rows_written, 0)
        self.assertEqual(stale_write.rows_written, 0)
        self.assertEqual(sink.load_checkpoint("orders:test"), latest.cursor)
        with sqlite3.connect(self.target_path) as connection:
            row = connection.execute(
                "SELECT status, updated_at FROM orders WHERE id = ?", (latest.id,)
            ).fetchone()
        self.assertEqual(row, ("shipped", latest.updated_at))

    def test_checkpoint_failure_rolls_back_order_upserts(self) -> None:
        sink = SQLiteOrderSink(self.target_path)
        record = order(1, "2026-08-04T10:00:00Z")
        with sqlite3.connect(self.target_path) as connection:
            connection.execute(
                """
                CREATE TRIGGER reject_checkpoint
                BEFORE INSERT ON _qsync_checkpoints
                BEGIN
                    SELECT RAISE(ABORT, 'simulated checkpoint failure');
                END
                """
            )

        with self.assertRaisesRegex(sqlite3.IntegrityError, "checkpoint failure"):
            sink.commit_batch("orders:test", [record], record.cursor)

        with sqlite3.connect(self.target_path) as connection:
            count = connection.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        self.assertEqual(count, 0)
        self.assertIsNone(sink.load_checkpoint("orders:test"))

    def test_source_reads_strictly_after_composite_cursor(self) -> None:
        timestamp = "2026-08-04T10:00:00Z"
        records = [order(index, timestamp) for index in range(4)]
        self.insert_source_orders(records)
        source = SQLiteOrderSource(self.source_path)

        page = source.read_batch(Cursor(timestamp, records[1].id), 10)

        self.assertEqual(page.records, tuple(records[2:]))
        self.assertEqual(page.next_cursor, records[-1].cursor)


if __name__ == "__main__":
    unittest.main()
