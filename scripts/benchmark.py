#!/usr/bin/env python3
"""Run the reproducible DeltaFlow SQLite benchmark matrix."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable


DEFAULT_ROWS = (100_000, 1_000_000)
DEFAULT_BATCH_SIZES = (100, 1_000, 5_000, 10_000)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark DeltaFlow across deterministic SQLite workloads."
    )
    parser.add_argument(
        "--rows",
        type=int,
        nargs="+",
        default=DEFAULT_ROWS,
        help="initial order counts (default: 100000 1000000)",
    )
    parser.add_argument(
        "--batch-sizes",
        "--batch-size",
        type=int,
        nargs="+",
        default=DEFAULT_BATCH_SIZES,
        dest="batch_sizes",
        help="batch sizes; --batch-size remains a compatible alias (default: 100 1000 5000 10000)",
    )
    parser.add_argument(
        "--incremental-rows",
        type=int,
        default=10_000,
        help="orders appended before each incremental run (default: 10000)",
    )
    parser.add_argument(
        "--workspace",
        type=Path,
        help="keep benchmark databases under this directory",
    )
    return parser.parse_args()


def timestamp(index: int) -> str:
    value = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=index // 8)
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def create_source(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.executescript(
            "CREATE TABLE orders("
            "id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, "
            "status TEXT NOT NULL, amount NUMERIC NOT NULL, updated_at TEXT NOT NULL);"
            "CREATE INDEX idx_orders_updated_at_id ON orders(updated_at, id);"
        )


def insert_orders(path: Path, start: int, count: int, chunk_size: int = 10_000) -> float:
    started = time.perf_counter()
    with sqlite3.connect(path) as connection:
        for chunk_start in range(start, start + count, chunk_size):
            chunk_end = min(start + count, chunk_start + chunk_size)
            connection.executemany(
                "INSERT INTO orders VALUES (?, ?, ?, ?, ?)",
                (
                    (
                        f"order-{index:012d}",
                        f"customer-{index % 50_000:08d}",
                        "paid" if index % 4 else "pending",
                        (index % 100_000) + 1,
                        timestamp(index),
                    )
                    for index in range(chunk_start, chunk_end)
                ),
            )
    return time.perf_counter() - started


def serialize_stats(stats: object) -> dict[str, object]:
    """Return stable, path-independent sync metrics in a fixed field order."""

    raw = stats.as_dict()  # type: ignore[attr-defined]
    return {
        "rows_read": raw["rows_read"],
        "rows_written": raw["rows_written"],
        "batches": raw["batches"],
        "elapsed_seconds": raw["elapsed_seconds"],
        "rows_per_second": raw["rows_per_second"],
        "start_cursor": raw["start_cursor"],
        "end_cursor": raw["end_cursor"],
    }


def run_case(rows: int, incremental_rows: int, batch_size: int, root: Path) -> dict[str, object]:
    from qsync.connectors.sqlite_sink import SQLiteSink
    from qsync.connectors.sqlite_source import SQLiteSource
    from qsync.pipeline import run_sync

    source_path = root / "source.db"
    target_path = root / "target.db"
    if source_path.exists() or target_path.exists():
        raise RuntimeError(f"refusing to overwrite benchmark databases in {root}")
    root.mkdir(parents=True, exist_ok=True)

    create_source(source_path)
    initial_fixture_seconds = insert_orders(source_path, 0, rows)
    source = SQLiteSource(source_path)
    sink = SQLiteSink(target_path)
    initial = run_sync(source, sink, batch_size=batch_size)

    incremental_fixture_seconds = insert_orders(source_path, rows, incremental_rows)
    incremental = run_sync(source, sink, batch_size=batch_size)
    noop = run_sync(source, sink, batch_size=batch_size)

    return {
        "config": {
            "rows": rows,
            "incremental_rows": incremental_rows,
            "batch_size": batch_size,
        },
        "fixture": {
            "initial_seconds": round(initial_fixture_seconds, 6),
            "incremental_seconds": round(incremental_fixture_seconds, 6),
        },
        "initial": serialize_stats(initial),
        "incremental": serialize_stats(incremental),
        "noop": serialize_stats(noop),
    }


def run_matrix(
    rows_values: Iterable[int],
    batch_sizes: Iterable[int],
    incremental_rows: int,
    root: Path,
) -> dict[str, object]:
    cases = []
    for rows in rows_values:
        for batch_size in batch_sizes:
            case_root = root / f"rows-{rows}-batch-{batch_size}"
            print(
                f"benchmark: running rows={rows} batch_size={batch_size}",
                file=sys.stderr,
            )
            case = run_case(rows, incremental_rows, batch_size, case_root)
            cases.append(case)
            print(
                f"benchmark: completed rows={rows} batch_size={batch_size}",
                file=sys.stderr,
            )
    return {"schema_version": 1, "benchmark": "deltaflow-sqlite-orders", "cases": cases}


def main() -> int:
    args = parse_args()
    if any(rows < 0 for rows in args.rows) or args.incremental_rows < 0:
        print("benchmark: row counts cannot be negative", file=sys.stderr)
        return 2
    if any(batch_size < 1 for batch_size in args.batch_sizes):
        print("benchmark: batch sizes must be positive", file=sys.stderr)
        return 2
    if len(set(args.rows)) != len(args.rows) or len(set(args.batch_sizes)) != len(
        args.batch_sizes
    ):
        print("benchmark: rows and batch sizes cannot contain duplicates", file=sys.stderr)
        return 2

    try:
        if args.workspace is not None:
            root = args.workspace.expanduser().resolve()
            result = run_matrix(args.rows, args.batch_sizes, args.incremental_rows, root)
        else:
            with tempfile.TemporaryDirectory(prefix="deltaflow-benchmark-") as directory:
                result = run_matrix(
                    args.rows, args.batch_sizes, args.incremental_rows, Path(directory)
                )
    except (ImportError, OSError, RuntimeError, sqlite3.Error) as exc:
        print(f"benchmark: {exc}", file=sys.stderr)
        return 2

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
