"""SQLite transaction fault injection tests."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from qsync.connectors.sqlite import SQLiteOrderSink
from qsync.models import Record


def order(number: int, updated_at: str) -> Record:
    return Record(
        id=f"order-{number:03d}",
        updated_at=updated_at,
        values={
            "customer_id": f"customer-{number:03d}",
            "status": "paid",
            "amount": number + 0.25,
        },
    )


class FaultInjectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.target_path = Path(self.tempdir.name) / "target.db"
        self.sink = SQLiteOrderSink(self.target_path)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_sink_failure_rolls_back_entire_batch_and_checkpoint(self) -> None:
        records = [
            order(1, "2026-08-04T10:00:00Z"),
            order(2, "2026-08-04T10:01:00Z"),
        ]
        with sqlite3.connect(self.target_path) as connection:
            connection.execute(
                """
                CREATE TRIGGER fail_second_order
                BEFORE INSERT ON orders
                WHEN NEW.id = 'order-002'
                BEGIN
                    SELECT RAISE(ABORT, 'simulated sink failure');
                END
                """
            )

        with self.assertRaisesRegex(sqlite3.IntegrityError, "sink failure"):
            self.sink.commit_batch("orders:fault", records, records[-1].cursor)

        with sqlite3.connect(self.target_path) as connection:
            count = connection.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        self.assertEqual(count, 0)
        self.assertIsNone(self.sink.load_checkpoint("orders:fault"))

    def test_checkpoint_update_failure_rolls_back_order_upsert(self) -> None:
        original = order(1, "2026-08-04T10:00:00Z")
        next_record = order(2, "2026-08-04T10:01:00Z")
        self.sink.commit_batch("orders:fault", [original], original.cursor)
        with sqlite3.connect(self.target_path) as connection:
            connection.execute(
                """
                CREATE TRIGGER fail_checkpoint_update
                BEFORE UPDATE ON _qsync_checkpoints
                WHEN OLD.source_id = 'orders:fault'
                BEGIN
                    SELECT RAISE(ABORT, 'simulated checkpoint failure');
                END
                """
            )

        with self.assertRaisesRegex(sqlite3.IntegrityError, "checkpoint failure"):
            self.sink.commit_batch("orders:fault", [next_record], next_record.cursor)

        with sqlite3.connect(self.target_path) as connection:
            ids = [row[0] for row in connection.execute("SELECT id FROM orders ORDER BY id")]
        self.assertEqual(ids, [original.id])
        self.assertEqual(self.sink.load_checkpoint("orders:fault"), original.cursor)


if __name__ == "__main__":
    unittest.main()
