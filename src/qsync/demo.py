"""Utilities for creating and inspecting DeltaFlow's SQLite order demo."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path


_DEMO_EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _timestamp(sequence: int) -> str:
    value = _DEMO_EPOCH + timedelta(seconds=sequence)
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def init_demo(
    path: str | Path,
    count: int = 1_000,
    *,
    append: bool = False,
    updates: int = 0,
) -> dict[str, object]:
    """Create deterministic, realistic orders for a local sync demonstration."""

    if count < 0:
        raise ValueError("count cannot be negative")
    if updates < 0:
        raise ValueError("updates cannot be negative")

    database = Path(path).expanduser().resolve()
    database.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database) as connection:
        if not append:
            connection.execute("DROP TABLE IF EXISTS orders")
            connection.execute("DROP TABLE IF EXISTS _deltaflow_demo")
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                status TEXT NOT NULL,
                amount NUMERIC NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_orders_updated_at_id
                ON orders(updated_at, id);
            CREATE TABLE IF NOT EXISTS _deltaflow_demo (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                next_order INTEGER NOT NULL,
                next_event INTEGER NOT NULL
            );
            """
        )
        state = connection.execute(
            "SELECT next_order, next_event FROM _deltaflow_demo WHERE singleton = 1"
        ).fetchone()
        if state is None:
            existing = int(connection.execute("SELECT COUNT(*) FROM orders").fetchone()[0])
            next_order = existing
            next_event = existing
        else:
            next_order, next_event = map(int, state)

        rows = []
        statuses = ("pending", "paid", "fulfilled", "cancelled")
        for offset in range(count):
            number = next_order + offset
            rows.append(
                (
                    f"order-{number:08d}",
                    f"customer-{number % 10_000:05d}",
                    statuses[number % len(statuses)],
                    1_000 + (number * 137) % 100_000,
                    _timestamp(next_event + offset),
                )
            )
        connection.executemany(
            "INSERT INTO orders(id, customer_id, status, amount, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            rows,
        )
        next_order += count
        next_event += count

        candidates = connection.execute(
            "SELECT id, status FROM orders ORDER BY id LIMIT ?", (updates,)
        ).fetchall()
        if len(candidates) != updates:
            raise ValueError(
                f"cannot update {updates} orders; source contains {len(candidates)}"
            )
        update_rows = [
            (
                "fulfilled" if status != "fulfilled" else "refunded",
                _timestamp(next_event + offset),
                order_id,
            )
            for offset, (order_id, status) in enumerate(candidates)
        ]
        connection.executemany(
            "UPDATE orders SET status = ?, updated_at = ? WHERE id = ?", update_rows
        )
        next_event += updates
        connection.execute(
            "INSERT INTO _deltaflow_demo(singleton, next_order, next_event) VALUES (1, ?, ?) "
            "ON CONFLICT(singleton) DO UPDATE SET "
            "next_order = excluded.next_order, next_event = excluded.next_event",
            (next_order, next_event),
        )
        total = int(connection.execute("SELECT COUNT(*) FROM orders").fetchone()[0])

    return {
        "path": str(database),
        "orders_inserted": count,
        "orders_updated": updates,
        "orders_total": total,
        "append": append,
    }


def inspect_order_target(path: str | Path) -> dict[str, object]:
    """Return target row count and all source cursors without mutating the DB."""

    database = Path(path).expanduser().resolve()
    if not database.is_file():
        raise ValueError(f"target database does not exist: {database}")
    with sqlite3.connect(database) as connection:
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        if "orders" not in tables:
            raise ValueError(f"target database has no orders table: {database}")
        rows = int(connection.execute("SELECT COUNT(*) FROM orders").fetchone()[0])
        checkpoints: list[dict[str, object]] = []
        if "_sync_checkpoints" in tables:
            checkpoints = [
                {
                    "source_id": source_id,
                    "cursor": {"updated_at": updated_at, "id": cursor_id},
                    "updated_at": committed_at,
                }
                for source_id, updated_at, cursor_id, committed_at in connection.execute(
                    "SELECT source_id, cursor_updated_at, cursor_id, updated_at "
                    "FROM _sync_checkpoints ORDER BY source_id"
                )
            ]
    return {"target": str(database), "rows": rows, "checkpoints": checkpoints}
