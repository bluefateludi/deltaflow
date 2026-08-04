from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path


class SyncError(RuntimeError):
    """Raised when source data or checkpoint state is invalid."""


@dataclass(frozen=True)
class SyncStats:
    source: str
    start_offset: int
    end_offset: int
    rows_read: int
    rows_written: int
    batches: int
    elapsed_seconds: float

    @property
    def rows_per_second(self) -> float:
        return self.rows_read / self.elapsed_seconds if self.elapsed_seconds else 0.0

    def as_dict(self) -> dict[str, object]:
        return {
            "source": self.source,
            "start_offset": self.start_offset,
            "end_offset": self.end_offset,
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "batches": self.batches,
            "elapsed_seconds": round(self.elapsed_seconds, 6),
            "rows_per_second": round(self.rows_per_second, 2),
        }


def connect_target(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA temp_store=MEMORY")
    connection.execute("PRAGMA busy_timeout=30000")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS records (
            id TEXT PRIMARY KEY,
            updated_at TEXT NOT NULL,
            payload TEXT NOT NULL,
            source_offset INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS _sync_checkpoints (
            source_key TEXT PRIMARY KEY,
            source_path TEXT NOT NULL,
            byte_offset INTEGER NOT NULL,
            rows_synced INTEGER NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    return connection


def source_key(path: Path) -> str:
    canonical = str(path.expanduser().resolve())
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def get_checkpoint(connection: sqlite3.Connection, key: str) -> tuple[int, int]:
    row = connection.execute(
        "SELECT byte_offset, rows_synced FROM _sync_checkpoints WHERE source_key = ?",
        (key,),
    ).fetchone()
    return (int(row[0]), int(row[1])) if row else (0, 0)


def _parse_line(item: tuple[int, bytes]) -> tuple[str, str, str, int]:
    end_offset, raw = item
    try:
        record = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SyncError(f"invalid JSON ending at byte {end_offset}: {exc}") from exc
    if not isinstance(record, dict):
        raise SyncError(f"record ending at byte {end_offset} must be a JSON object")
    missing = {"id", "updated_at"} - record.keys()
    if missing:
        raise SyncError(
            f"record ending at byte {end_offset} missing fields: {', '.join(sorted(missing))}"
        )
    return (
        str(record["id"]),
        str(record["updated_at"]),
        json.dumps(record, ensure_ascii=False, separators=(",", ":")),
        end_offset,
    )


def _read_batch(handle, batch_size: int) -> list[tuple[int, bytes]]:
    lines: list[tuple[int, bytes]] = []
    for _ in range(batch_size):
        raw = handle.readline()
        if not raw:
            break
        if not raw.endswith(b"\n"):
            # An append may be in progress. Do not checkpoint a partial record.
            handle.seek(-len(raw), os.SEEK_CUR)
            break
        if raw.strip():
            lines.append((handle.tell(), raw))
    return lines


def _parse_batch(
    lines: list[tuple[int, bytes]], executor: ThreadPoolExecutor | None
) -> list[tuple[str, str, str, int]]:
    if executor is None:
        return [_parse_line(line) for line in lines]
    return list(executor.map(_parse_line, lines, chunksize=max(1, len(lines) // 32)))


def sync_file(
    source: Path,
    target: Path,
    *,
    batch_size: int = 5_000,
    workers: int = 1,
    reset: bool = False,
    max_batches: int | None = None,
) -> SyncStats:
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if workers < 1:
        raise ValueError("workers must be positive")
    source = source.expanduser().resolve()
    target = target.expanduser().resolve()
    if not source.is_file():
        raise SyncError(f"source does not exist or is not a file: {source}")
    if source == target:
        raise SyncError("source and target must be different files")

    key = source_key(source)
    started = time.perf_counter()
    rows_read = rows_written = batches = 0
    connection = connect_target(target)
    executor = ThreadPoolExecutor(max_workers=workers) if workers > 1 else None
    try:
        start_offset, prior_rows = get_checkpoint(connection, key)
        if reset:
            start_offset, prior_rows = 0, 0
            with connection:
                connection.execute("DELETE FROM _sync_checkpoints WHERE source_key = ?", (key,))
        source_size = source.stat().st_size
        if start_offset > source_size:
            raise SyncError(
                "source is smaller than its checkpoint (it was truncated or replaced); "
                "rerun with --reset after confirming the source"
            )

        current_offset = start_offset
        with source.open("rb") as handle:
            handle.seek(start_offset)
            while max_batches is None or batches < max_batches:
                lines = _read_batch(handle, batch_size)
                if not lines:
                    break
                parsed = _parse_batch(lines, executor)
                next_offset = lines[-1][0]
                with connection:
                    before = connection.total_changes
                    connection.executemany(
                        """
                        INSERT INTO records(id, updated_at, payload, source_offset)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(id) DO UPDATE SET
                            updated_at = excluded.updated_at,
                            payload = excluded.payload,
                            source_offset = excluded.source_offset
                        WHERE excluded.updated_at >= records.updated_at
                        """,
                        parsed,
                    )
                    changed = connection.total_changes - before
                    connection.execute(
                        """
                        INSERT INTO _sync_checkpoints(
                            source_key, source_path, byte_offset, rows_synced
                        ) VALUES (?, ?, ?, ?)
                        ON CONFLICT(source_key) DO UPDATE SET
                            source_path = excluded.source_path,
                            byte_offset = excluded.byte_offset,
                            rows_synced = excluded.rows_synced,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        (key, str(source), next_offset, prior_rows + rows_read + len(parsed)),
                    )
                rows_read += len(parsed)
                rows_written += changed
                batches += 1
                current_offset = next_offset
    finally:
        if executor is not None:
            executor.shutdown()
        connection.close()

    return SyncStats(
        source=str(source),
        start_offset=start_offset,
        end_offset=current_offset,
        rows_read=rows_read,
        rows_written=rows_written,
        batches=batches,
        elapsed_seconds=time.perf_counter() - started,
    )


def inspect_target(target: Path) -> dict[str, object]:
    target = target.expanduser().resolve()
    if not target.exists():
        raise SyncError(f"target database does not exist: {target}")
    connection = connect_target(target)
    try:
        records = connection.execute("SELECT COUNT(*) FROM records").fetchone()[0]
        checkpoints = [
            {
                "source": row[0],
                "byte_offset": row[1],
                "rows_synced": row[2],
                "updated_at": row[3],
            }
            for row in connection.execute(
                "SELECT source_path, byte_offset, rows_synced, updated_at "
                "FROM _sync_checkpoints ORDER BY source_path"
            )
        ]
        return {"target": str(target), "records": records, "checkpoints": checkpoints}
    finally:
        connection.close()


def generate_records(path: Path, count: int, *, append: bool = False) -> None:
    if count < 0:
        raise ValueError("count cannot be negative")
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    start = 0
    mode = "ab" if append else "wb"
    if append and path.exists():
        with path.open("rb") as handle:
            start = sum(1 for line in handle if line.strip())
    with path.open(mode) as handle:
        for index in range(start, start + count):
            record = {
                "id": f"record-{index}",
                "updated_at": f"2026-08-04T00:{(index // 60) % 60:02d}:{index % 60:02d}Z",
                "name": f"user-{index}",
                "score": index % 100,
            }
            handle.write(
                json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                + b"\n"
            )
