from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

from . import __version__
from .demo import init_demo, inspect_order_target
from .models import Cursor, SyncStats
from .pipeline import PipelineError, run_sync


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="qsync",
        description="DeltaFlow: incrementally synchronize SQLite orders.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    demo = subparsers.add_parser("init-demo", help="create a SQLite orders source")
    demo.add_argument("source", type=Path)
    demo.add_argument("--count", type=int, default=1_000, help="orders to insert")
    demo.add_argument("--append", action="store_true", help="keep existing orders")
    demo.add_argument("--updates", type=int, default=0, help="existing orders to update")

    sync = subparsers.add_parser("sync", help="synchronize SQLite orders incrementally")
    sync.add_argument("source", type=Path)
    sync.add_argument("target", type=Path)
    sync.add_argument("--batch-size", type=int, default=5_000)
    sync.add_argument("--max-batches", type=int, help="stop after this many batches")

    status = subparsers.add_parser("status", help="show target rows and checkpoints")
    status.add_argument("target", type=Path)

    return parser


def _connectors(source: Path, target: Path):
    # Imported only for sync so init-demo and argument handling remain usable
    # while connector branches are developed independently.
    from .connectors import SQLiteSink, SQLiteSource

    return SQLiteSource(source), SQLiteSink(target)


def _sync_result(stats: SyncStats) -> dict[str, object]:
    cursor: Cursor | None = stats.end_cursor
    return {
        "rows_read": stats.rows_read,
        "rows_written": stats.rows_written,
        "batches": stats.batches,
        "cursor": (
            {"updated_at": cursor.updated_at, "id": cursor.id} if cursor else None
        ),
        "elapsed": round(stats.elapsed_seconds, 6),
        "throughput": round(stats.rows_per_second, 2),
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "sync":
            source, sink = _connectors(args.source, args.target)
            result = _sync_result(
                run_sync(
                    source,
                    sink,
                    batch_size=args.batch_size,
                    max_batches=args.max_batches,
                )
            )
        elif args.command == "status":
            result = inspect_order_target(args.target)
        else:
            result = init_demo(
                args.source,
                args.count,
                append=args.append,
                updates=args.updates,
            )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except (ImportError, OSError, PipelineError, sqlite3.Error, ValueError) as exc:
        print(f"qsync: error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
