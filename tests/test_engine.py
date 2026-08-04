import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from qsync.engine import SyncError, generate_records, inspect_target, sync_file


class SyncEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.source = self.root / "source.jsonl"
        self.target = self.root / "target.db"

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_full_then_incremental_then_noop(self) -> None:
        generate_records(self.source, 10)
        first = sync_file(self.source, self.target, batch_size=3)
        self.assertEqual(first.rows_read, 10)
        self.assertEqual(inspect_target(self.target)["records"], 10)

        generate_records(self.source, 5, append=True)
        second = sync_file(self.source, self.target, batch_size=2)
        self.assertEqual(second.rows_read, 5)
        self.assertEqual(inspect_target(self.target)["records"], 15)

        third = sync_file(self.source, self.target)
        self.assertEqual(third.rows_read, 0)
        self.assertEqual(third.start_offset, third.end_offset)

    def test_resume_from_committed_batch(self) -> None:
        generate_records(self.source, 12)
        interrupted = sync_file(
            self.source, self.target, batch_size=5, max_batches=1
        )
        self.assertEqual(interrupted.rows_read, 5)

        resumed = sync_file(self.source, self.target, batch_size=5)
        self.assertEqual(resumed.rows_read, 7)
        self.assertEqual(inspect_target(self.target)["records"], 12)

    def test_partial_trailing_line_waits_for_completion(self) -> None:
        complete = {"id": "1", "updated_at": "2026-01-01T00:00:00Z"}
        partial = b'{"id":"2","updated_at":"2026-01-01T00:00:01Z"'
        self.source.write_bytes(json.dumps(complete).encode() + b"\n" + partial)

        first = sync_file(self.source, self.target)
        self.assertEqual(first.rows_read, 1)
        with self.source.open("ab") as handle:
            handle.write(b"}\n")
        second = sync_file(self.source, self.target)
        self.assertEqual(second.rows_read, 1)

    def test_newer_record_wins_and_replay_is_idempotent(self) -> None:
        rows = [
            {"id": "same", "updated_at": "2026-01-02T00:00:00Z", "value": "new"},
            {"id": "same", "updated_at": "2026-01-01T00:00:00Z", "value": "old"},
        ]
        self.source.write_text("".join(json.dumps(row) + "\n" for row in rows))
        sync_file(self.source, self.target)
        with sqlite3.connect(self.target) as connection:
            payload = json.loads(
                connection.execute("SELECT payload FROM records WHERE id='same'").fetchone()[0]
            )
        self.assertEqual(payload["value"], "new")

        replay = sync_file(self.source, self.target, reset=True)
        self.assertEqual(replay.rows_read, 2)
        self.assertEqual(inspect_target(self.target)["records"], 1)

    def test_truncated_source_requires_explicit_reset(self) -> None:
        generate_records(self.source, 3)
        sync_file(self.source, self.target)
        self.source.write_text("")
        with self.assertRaises(SyncError):
            sync_file(self.source, self.target)


if __name__ == "__main__":
    unittest.main()

