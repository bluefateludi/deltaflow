from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from . import __version__
from .engine import SyncError, generate_records, inspect_target, sync_file


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="qsync", description="Incrementally synchronize append-only JSONL into SQLite."
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    sync = subparsers.add_parser("sync", help="synchronize new complete JSONL records")
    sync.add_argument("source", type=Path)
    sync.add_argument("target", type=Path)
    sync.add_argument("--batch-size", type=int, default=5_000)
    sync.add_argument("--workers", type=int, default=1, help="parallel JSON parser threads")
    sync.add_argument("--reset", action="store_true", help="restart this source from byte zero")
    sync.add_argument("--max-batches", type=int, help=argparse.SUPPRESS)

    status = subparsers.add_parser("status", help="show target rows and checkpoints")
    status.add_argument("target", type=Path)

    generate = subparsers.add_parser("generate", help="generate JSONL demo records")
    generate.add_argument("path", type=Path)
    generate.add_argument("--count", type=int, default=100_000)
    generate.add_argument("--append", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "sync":
            result = sync_file(
                args.source,
                args.target,
                batch_size=args.batch_size,
                workers=args.workers,
                reset=args.reset,
                max_batches=args.max_batches,
            ).as_dict()
        elif args.command == "status":
            result = inspect_target(args.target)
        else:
            generate_records(args.path, args.count, append=args.append)
            result = {"path": str(args.path.resolve()), "generated": args.count, "append": args.append}
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except (SyncError, OSError, ValueError) as exc:
        print(f"qsync: error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

