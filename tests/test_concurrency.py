"""Contention tests for multiple DeltaFlow instances sharing a SQLite sink."""

from __future__ import annotations

import sqlite3
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from qsync.connectors.sqlite import SQLiteOrderSink, SQLiteOrderSource
from qsync.models import Batch, Cursor, Record
from qsync.pipeline import run_sync


class FirstReadBarrierSource(SQLiteOrderSource):
    def __init__(
        self,
        database: Path,
        barrier: threading.Barrier,
        *,
        source_id: str,
    ) -> None:
        super().__init__(database, source_id=source_id)
        self.barrier = barrier
        self.first_read = True

    def read_batch(self, cursor: Cursor | None, batch_size: int) -> Batch:
        if self.first_read:
            self.first_read = False
            self.barrier.wait(timeout=5)
        return super().read_batch(cursor, batch_size)


def order(number: int) -> Record:
    return Record(
        id=f"order-{number:03d}",
        updated_at="2026-08-04T10:00:00Z",
        values={
            "customer_id": f"customer-{number:03d}",
            "status": "paid",
            "amount": number,
        },
    )


class ConcurrencyTests(unittest.TestCase):
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
            connection.executemany(
                "INSERT INTO orders VALUES (?, ?, ?, ?, ?)",
                [
                    (
                        item.id,
                        item.values["customer_id"],
                        item.values["status"],
                        item.values["amount"],
                        item.updated_at,
                    )
                    for item in (order(index) for index in range(12))
                ],
            )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_two_sync_instances_converge_on_one_target(self) -> None:
        source_id = "orders:contended"
        sink = SQLiteOrderSink(self.target_path)
        barrier = threading.Barrier(2)
        sources = [
            FirstReadBarrierSource(self.source_path, barrier, source_id=source_id),
            FirstReadBarrierSource(self.source_path, barrier, source_id=source_id),
        ]

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(run_sync, sources[0], sink, batch_size=2),
                executor.submit(run_sync, sources[1], sink, batch_size=5),
            ]
            stats = [future.result(timeout=10) for future in futures]

        with sqlite3.connect(self.target_path) as connection:
            count = connection.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        self.assertEqual(count, 12)
        self.assertEqual(sink.load_checkpoint(source_id), order(11).cursor)
        self.assertTrue(all(result.end_cursor == order(11).cursor for result in stats))

    def test_busy_writer_waits_for_lock_then_commits(self) -> None:
        sink = SQLiteOrderSink(self.target_path)
        pending = order(20)
        lock = sqlite3.connect(self.target_path, timeout=1)
        lock.execute("BEGIN IMMEDIATE")
        started = threading.Event()

        def commit_while_locked() -> None:
            started.set()
            sink.commit_batch("orders:busy", [pending], pending.cursor)

        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(commit_while_locked)
                self.assertTrue(started.wait(timeout=1))
                time.sleep(0.1)
                self.assertFalse(future.done())
                lock.rollback()
                future.result(timeout=5)
        finally:
            lock.close()

        self.assertEqual(sink.load_checkpoint("orders:busy"), pending.cursor)


if __name__ == "__main__":
    unittest.main()
