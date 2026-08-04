"""DeltaFlow acceptance tests across the public pipeline/connector seam.

The SQLite connectors are developed independently from the core pipeline.  The
tests intentionally import their expected public API instead of carrying a
second, test-only connector implementation.  Until that branch is integrated,
this module is reported as skipped; after integration the same tests become the
end-to-end merge gate.
"""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from qsync.pipeline import run_sync

try:
    from qsync.connectors.sqlite_sink import SQLiteSink
    from qsync.connectors.sqlite_source import SQLiteSource
except ModuleNotFoundError as exc:
    if exc.name != "qsync.connectors":
        raise
    SQLiteSink = None  # type: ignore[assignment,misc]
    SQLiteSource = None  # type: ignore[assignment,misc]
    CONNECTOR_SEAM = (
        "waiting for qsync.connectors.sqlite_source.SQLiteSource and "
        "qsync.connectors.sqlite_sink.SQLiteSink"
    )
else:
    CONNECTOR_SEAM = ""


Order = tuple[str, str, str, int, str]


@unittest.skipIf(SQLiteSource is None, CONNECTOR_SEAM)
class DeltaFlowEndToEndTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.source_path = self.root / "commerce.db"
        self.target_path = self.root / "analytics.db"
        with sqlite3.connect(self.source_path) as connection:
            connection.execute(
                "CREATE TABLE orders("
                "id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, "
                "status TEXT NOT NULL, amount NUMERIC NOT NULL, "
                "updated_at TEXT NOT NULL)"
            )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def insert_orders(self, orders: list[Order]) -> None:
        with sqlite3.connect(self.source_path) as connection:
            connection.executemany(
                "INSERT INTO orders(id, customer_id, status, amount, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                orders,
            )

    def target_rows(self) -> list[Order]:
        with sqlite3.connect(self.target_path) as connection:
            return connection.execute(
                "SELECT id, customer_id, status, amount, updated_at "
                "FROM orders ORDER BY id"
            ).fetchall()

    def pipeline(self):
        return SQLiteSource(self.source_path), SQLiteSink(self.target_path)

    def test_initial_backfill_copies_every_order(self) -> None:
        orders = [
            (f"order-{index:03d}", f"customer-{index}", "paid", index * 100, timestamp)
            for index, timestamp in enumerate(
                [
                    "2026-08-04T10:00:00Z",
                    "2026-08-04T10:00:00Z",
                    "2026-08-04T10:01:00Z",
                    "2026-08-04T10:02:00Z",
                ]
            )
        ]
        self.insert_orders(orders)
        source, sink = self.pipeline()

        stats = run_sync(source, sink, batch_size=2)

        self.assertEqual((stats.rows_read, stats.rows_written, stats.batches), (4, 4, 2))
        self.assertEqual(self.target_rows(), orders)
        self.assertEqual(sink.load_checkpoint(source.source_id), stats.end_cursor)

    def test_incremental_run_reads_only_new_and_updated_orders(self) -> None:
        self.insert_orders(
            [("order-001", "customer-1", "pending", 1200, "2026-08-04T10:00:00Z")]
        )
        source, sink = self.pipeline()
        run_sync(source, sink)
        with sqlite3.connect(self.source_path) as connection:
            connection.execute(
                "UPDATE orders SET status = ?, updated_at = ? WHERE id = ?",
                ("paid", "2026-08-04T10:05:00Z", "order-001"),
            )
            connection.execute(
                "INSERT INTO orders VALUES (?, ?, ?, ?, ?)",
                ("order-002", "customer-2", "pending", 800, "2026-08-04T10:06:00Z"),
            )

        stats = run_sync(source, sink, batch_size=1)

        self.assertEqual((stats.rows_read, stats.rows_written), (2, 2))
        self.assertEqual(
            self.target_rows(),
            [
                ("order-001", "customer-1", "paid", 1200, "2026-08-04T10:05:00Z"),
                ("order-002", "customer-2", "pending", 800, "2026-08-04T10:06:00Z"),
            ],
        )

    def test_noop_run_does_not_write_or_advance(self) -> None:
        self.insert_orders(
            [("order-001", "customer-1", "paid", 1200, "2026-08-04T10:00:00Z")]
        )
        source, sink = self.pipeline()
        first = run_sync(source, sink)

        noop = run_sync(source, sink)

        self.assertEqual((noop.rows_read, noop.rows_written, noop.batches), (0, 0, 0))
        self.assertEqual(noop.start_cursor, first.end_cursor)
        self.assertEqual(noop.end_cursor, first.end_cursor)

    def test_max_batches_interruption_resumes_from_committed_cursor(self) -> None:
        orders = [
            (f"order-{index:03d}", f"customer-{index}", "paid", index, "2026-08-04T10:00:00Z")
            for index in range(7)
        ]
        self.insert_orders(orders)
        source, sink = self.pipeline()

        interrupted = run_sync(source, sink, batch_size=3, max_batches=1)
        resumed = run_sync(source, sink, batch_size=3)

        self.assertEqual(interrupted.rows_read, 3)
        self.assertEqual(resumed.start_cursor, interrupted.end_cursor)
        self.assertEqual(resumed.rows_read, 4)
        self.assertEqual(self.target_rows(), orders)

    def test_equal_timestamps_are_not_skipped_at_batch_boundaries(self) -> None:
        orders = [
            (f"order-{index:03d}", f"customer-{index}", "paid", index, "2026-08-04T10:00:00Z")
            for index in range(5)
        ]
        self.insert_orders(orders)
        source, sink = self.pipeline()

        stats = run_sync(source, sink, batch_size=2)

        self.assertEqual(stats.batches, 3)
        self.assertEqual([row[0] for row in self.target_rows()], [row[0] for row in orders])

    def test_older_source_version_does_not_overwrite_newer_target(self) -> None:
        self.insert_orders(
            [("order-001", "customer-1", "pending", 1200, "2026-08-04T10:00:00Z")]
        )
        source, sink = self.pipeline()
        with sqlite3.connect(self.target_path) as connection:
            connection.execute(
                "INSERT INTO orders VALUES (?, ?, ?, ?, ?)",
                ("order-001", "customer-1", "refunded", 1200, "2026-08-04T11:00:00Z"),
            )

        stats = run_sync(source, sink)

        self.assertEqual((stats.rows_read, stats.rows_written), (1, 0))
        self.assertEqual(
            self.target_rows(),
            [("order-001", "customer-1", "refunded", 1200, "2026-08-04T11:00:00Z")],
        )


if __name__ == "__main__":
    unittest.main()
