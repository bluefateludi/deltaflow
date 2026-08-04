#!/usr/bin/env python3
"""Shell-friendly DeltaFlow benchmark using the public SQLite connectors."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark initial, incremental, and no-op order synchronization."
    )
    parser.add_argument("--rows", type=int, default=100_000, help="initial order count")
    parser.add_argument(
        "--incremental-rows", type=int, default=10_000, help="orders appended before run two"
    )
    parser.add_argument("--batch-size", type=int, default=5_000)
    parser.add_argument(
        "--workspace",
        type=Path,
        help="keep benchmark databases here (directory must not already contain them)",
    )
    return parser.parse_args()


def timestamp(index: int) -> str:
    value = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=index // 8)
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def create_source(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE orders("
            "id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, "
            "status TEXT NOT NULL, amount NUMERIC NOT NULL, updated_at TEXT NOT NULL)"
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


def run_benchmark(args: argparse.Namespace, root: Path) -> dict[str, object]:
    try:
        from qsync.connectors.sqlite_sink import SQLiteSink
        from qsync.connectors.sqlite_source import SQLiteSource
        from qsync.pipeline import run_sync
    except ModuleNotFoundError as exc:
        if exc.name != "qsync.connectors":
            raise
        raise RuntimeError(
            "SQLite connector seam is not integrated: expected "
            "qsync.connectors.sqlite_source.SQLiteSource and "
            "qsync.connectors.sqlite_sink.SQLiteSink"
        ) from exc

    source_path = root / "commerce.db"
    target_path = root / "analytics.db"
    if source_path.exists() or target_path.exists():
        raise RuntimeError(f"refusing to overwrite benchmark databases in {root}")
    root.mkdir(parents=True, exist_ok=True)
    create_source(source_path)
    fixture_seconds = insert_orders(source_path, 0, args.rows)
    source = SQLiteSource(source_path)
    sink = SQLiteSink(target_path)

    initial = run_sync(source, sink, batch_size=args.batch_size)
    incremental_fixture_seconds = insert_orders(source_path, args.rows, args.incremental_rows)
    incremental = run_sync(source, sink, batch_size=args.batch_size)
    noop = run_sync(source, sink, batch_size=args.batch_size)

    return {
        "config": {
            "rows": args.rows,
            "incremental_rows": args.incremental_rows,
            "batch_size": args.batch_size,
        },
        "fixture_seconds": round(fixture_seconds + incremental_fixture_seconds, 6),
        "initial": initial.as_dict(),
        "incremental": incremental.as_dict(),
        "noop": noop.as_dict(),
    }


def main() -> int:
    args = parse_args()
    if args.rows < 0 or args.incremental_rows < 0 or args.batch_size < 1:
        print("benchmark: counts cannot be negative and batch-size must be positive", file=sys.stderr)
        return 2
    try:
        if args.workspace is not None:
            result = run_benchmark(args, args.workspace.expanduser().resolve())
        else:
            with tempfile.TemporaryDirectory(prefix="deltaflow-benchmark-") as directory:
                result = run_benchmark(args, Path(directory))
    except (OSError, RuntimeError, sqlite3.Error) as exc:
        print(f"benchmark: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
