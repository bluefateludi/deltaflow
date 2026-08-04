"""SQLite source and sink connectors for the DeltaFlow orders example."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Sequence

from qsync.models import Batch, Cursor, Record, WriteStats


def _database_path(database: str | Path) -> str:
    """Return a stable SQLite filename while preserving special SQLite names."""

    value = str(database)
    if value == ":memory:" or value.startswith("file:"):
        return value
    return str(Path(value).expanduser().resolve())


def _connect(database: str) -> sqlite3.Connection:
    connection = sqlite3.connect(database, timeout=30, uri=database.startswith("file:"))
    connection.execute("PRAGMA busy_timeout=30000")
    return connection


class SQLiteOrderSource:
    """Read current order versions in ``(updated_at, id)`` cursor order."""

    def __init__(self, database: str | Path, *, source_id: str | None = None) -> None:
        self.database = _database_path(database)
        if source_id == "":
            raise ValueError("source_id must not be empty")
        self.source_id = source_id or f"sqlite:orders:{self.database}"

    @staticmethod
    def _read_query(
        cursor: Cursor | None, batch_size: int
    ) -> tuple[str, tuple[object, ...]]:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")

        if cursor is None:
            sql = """
                SELECT id, customer_id, status, amount, updated_at
                FROM orders
                ORDER BY updated_at, id
                LIMIT ?
            """
            parameters: tuple[object, ...] = (batch_size,)
        else:
            sql = """
                SELECT id, customer_id, status, amount, updated_at
                FROM orders
                WHERE (updated_at, id) > (?, ?)
                ORDER BY updated_at, id
                LIMIT ?
            """
            parameters = (
                cursor.updated_at,
                cursor.id,
                batch_size,
            )
        return sql, parameters

    def explain_query_plan(
        self, cursor: Cursor | None, batch_size: int
    ) -> tuple[str, ...]:
        """Return SQLite's read-only plan details for the requested source page."""

        sql, parameters = self._read_query(cursor, batch_size)
        with _connect(self.database) as connection:
            rows = connection.execute(
                f"EXPLAIN QUERY PLAN {sql}", parameters
            ).fetchall()
        return tuple(str(row[3]) for row in rows)

    def read_batch(self, cursor: Cursor | None, batch_size: int) -> Batch:
        sql, parameters = self._read_query(cursor, batch_size)

        with _connect(self.database) as connection:
            rows = connection.execute(sql, parameters).fetchall()

        records = tuple(
            Record(
                id=str(row[0]),
                updated_at=str(row[4]),
                values={
                    "customer_id": row[1],
                    "status": row[2],
                    "amount": row[3],
                },
            )
            for row in rows
        )
        return Batch(
            records=records,
            next_cursor=records[-1].cursor if records else cursor,
        )


class SQLiteOrderSink:
    """Atomically upsert orders and their per-source composite checkpoint."""

    def __init__(self, database: str | Path) -> None:
        self.database = _database_path(database)
        self._initialize()

    def _initialize(self) -> None:
        if self.database != ":memory:" and not self.database.startswith("file:"):
            Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        with _connect(self.database) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS orders (
                    id TEXT PRIMARY KEY,
                    customer_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    amount NUMERIC NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS _qsync_checkpoints (
                    source_id TEXT PRIMARY KEY,
                    updated_at TEXT NOT NULL,
                    id TEXT NOT NULL
                );
                """
            )

    def load_checkpoint(self, source_id: str) -> Cursor | None:
        with _connect(self.database) as connection:
            row = connection.execute(
                """
                SELECT updated_at, id
                FROM _qsync_checkpoints
                WHERE source_id = ?
                """,
                (source_id,),
            ).fetchone()
        return Cursor(updated_at=str(row[0]), id=str(row[1])) if row else None

    def commit_batch(
        self,
        source_id: str,
        records: Sequence[Record],
        next_cursor: Cursor,
    ) -> WriteStats:
        """Commit one page; any SQLite error rolls back rows and checkpoint."""

        if not source_id:
            raise ValueError("source_id must not be empty")

        values = []
        for record in records:
            try:
                values.append(
                    (
                        record.id,
                        record.values["customer_id"],
                        record.values["status"],
                        record.values["amount"],
                        record.updated_at,
                    )
                )
            except KeyError as exc:
                raise ValueError(f"order record missing field: {exc.args[0]}") from exc

        connection = _connect(self.database)
        try:
            connection.execute("BEGIN IMMEDIATE")
            before = connection.total_changes
            connection.executemany(
                """
                INSERT INTO orders(id, customer_id, status, amount, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    customer_id = excluded.customer_id,
                    status = excluded.status,
                    amount = excluded.amount,
                    updated_at = excluded.updated_at
                WHERE excluded.updated_at > orders.updated_at
                """,
                values,
            )
            rows_written = connection.total_changes - before
            connection.execute(
                """
                INSERT INTO _qsync_checkpoints(source_id, updated_at, id)
                VALUES (?, ?, ?)
                ON CONFLICT(source_id) DO UPDATE SET
                    updated_at = excluded.updated_at,
                    id = excluded.id
                WHERE excluded.updated_at > _qsync_checkpoints.updated_at
                   OR (excluded.updated_at = _qsync_checkpoints.updated_at
                       AND excluded.id > _qsync_checkpoints.id)
                """,
                (source_id, next_cursor.updated_at, next_cursor.id),
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

        return WriteStats(rows_written=rows_written)


# Singular names remain convenient for callers that model one orders table.
SQLiteOrdersSource = SQLiteOrderSource
SQLiteOrdersSink = SQLiteOrderSink
SQLiteSource = SQLiteOrderSource
SQLiteSink = SQLiteOrderSink

__all__ = [
    "SQLiteOrderSink",
    "SQLiteOrderSource",
    "SQLiteOrdersSink",
    "SQLiteOrdersSource",
    "SQLiteSink",
    "SQLiteSource",
]
