"""Compatibility import for the DeltaFlow SQLite orders sink."""

from .sqlite import SQLiteOrderSink, SQLiteOrdersSink, SQLiteSink
from qsync.models import Cursor, Record, WriteStats

__all__ = [
    "Cursor",
    "Record",
    "SQLiteOrderSink",
    "SQLiteOrdersSink",
    "SQLiteSink",
    "WriteStats",
]
