import contextlib
import io
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from qsync.cli import build_parser, main
from qsync.demo import init_demo
from qsync.models import Batch, Cursor, Record, WriteStats


class CLIParserTests(unittest.TestCase):
    def test_sync_options(self) -> None:
        args = build_parser().parse_args(
            ["sync", "source.db", "target.db", "--batch-size", "17", "--max-batches", "3"]
        )
        self.assertEqual(args.command, "sync")
        self.assertEqual(args.batch_size, 17)
        self.assertEqual(args.max_batches, 3)

    def test_init_demo_options(self) -> None:
        args = build_parser().parse_args(
            ["init-demo", "orders.db", "--count", "9", "--append", "--updates", "2"]
        )
        self.assertEqual((args.count, args.append, args.updates), (9, True, 2))


class DemoAndCLITests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.source = self.root / "source.db"
        self.target = self.root / "target.db"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def run_cli(self, *args: str) -> tuple[int, dict[str, object]]:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = main(list(args))
        return code, json.loads(output.getvalue())

    def test_init_demo_create_append_and_update(self) -> None:
        first = init_demo(self.source, 5)
        second = init_demo(self.source, 2, append=True, updates=2)

        self.assertEqual(first["orders_total"], 5)
        self.assertEqual(second["orders_total"], 7)
        with sqlite3.connect(self.source) as connection:
            rows = connection.execute(
                "SELECT id, status, updated_at FROM orders ORDER BY updated_at, id"
            ).fetchall()
        self.assertEqual(len(rows), 7)
        self.assertEqual([row[1] for row in rows[-2:]], ["fulfilled", "fulfilled"])
        self.assertEqual([row[0] for row in rows[-2:]], ["order-00000000", "order-00000001"])

    def test_init_demo_creates_composite_keyset_index(self) -> None:
        init_demo(self.source, 1)

        with sqlite3.connect(self.source) as connection:
            indexes = connection.execute("PRAGMA index_list(orders)").fetchall()
            columns = connection.execute(
                "PRAGMA index_info(idx_orders_updated_at_id)"
            ).fetchall()

        self.assertIn("idx_orders_updated_at_id", [row[1] for row in indexes])
        self.assertEqual([row[2] for row in columns], ["updated_at", "id"])

    def test_init_demo_command_returns_json(self) -> None:
        code, result = self.run_cli("init-demo", str(self.source), "--count", "3")
        self.assertEqual(code, 0)
        self.assertEqual(result["orders_inserted"], 3)
        self.assertEqual(result["orders_total"], 3)

    def test_sync_json_shape_and_max_batches(self) -> None:
        records = tuple(
            Record(str(index), "2026-01-01T00:00:00Z", {}) for index in range(3)
        )

        class Source:
            source_id = "sqlite://demo#orders"

            def read_batch(self, cursor, batch_size):
                pending = tuple(row for row in records if cursor is None or row.cursor > cursor)
                page = pending[:batch_size]
                return Batch(page, page[-1].cursor if page else cursor)

        class Sink:
            cursor = None

            def load_checkpoint(self, source_id):
                return self.cursor

            def commit_batch(self, source_id, rows, next_cursor):
                self.cursor = next_cursor
                return WriteStats(len(rows))

        with patch("qsync.cli._connectors", return_value=(Source(), Sink())):
            code, result = self.run_cli(
                "sync", str(self.source), str(self.target), "--batch-size", "2", "--max-batches", "1"
            )
        self.assertEqual(code, 0)
        self.assertEqual(set(result), {"rows_read", "rows_written", "batches", "cursor", "elapsed", "throughput"})
        self.assertEqual((result["rows_read"], result["rows_written"], result["batches"]), (2, 2, 1))
        self.assertEqual(result["cursor"]["id"], "1")

    def test_status_command_reports_rows_and_cursor(self) -> None:
        with sqlite3.connect(self.target) as connection:
            connection.executescript(
                """
                CREATE TABLE orders(id TEXT PRIMARY KEY);
                CREATE TABLE _sync_checkpoints(
                    source_id TEXT PRIMARY KEY,
                    cursor_updated_at TEXT NOT NULL,
                    cursor_id TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                INSERT INTO orders VALUES ('order-1');
                INSERT INTO _sync_checkpoints VALUES (
                    'sqlite://source#orders', '2026-01-01T00:00:00Z', 'order-1', '2026-01-02 00:00:00'
                );
                """
            )
        code, result = self.run_cli("status", str(self.target))
        self.assertEqual(code, 0)
        self.assertEqual(result["rows"], 1)
        self.assertEqual(result["checkpoints"][0]["cursor"]["id"], "order-1")


if __name__ == "__main__":
    unittest.main()
