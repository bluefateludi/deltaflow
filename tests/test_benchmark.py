from __future__ import annotations

import importlib.util
import sqlite3
import tempfile
import unittest
from pathlib import Path


BENCHMARK_PATH = Path(__file__).parents[1] / "scripts" / "benchmark.py"


def load_benchmark_module():
    spec = importlib.util.spec_from_file_location("deltaflow_benchmark", BENCHMARK_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load benchmark module: {BENCHMARK_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class BenchmarkFixtureTests(unittest.TestCase):
    def test_source_schema_has_composite_keyset_index(self) -> None:
        benchmark = load_benchmark_module()
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.db"
            benchmark.create_source(source)

            with sqlite3.connect(source) as connection:
                indexes = connection.execute("PRAGMA index_list(orders)").fetchall()
                columns = connection.execute(
                    "PRAGMA index_info(idx_orders_updated_at_id)"
                ).fetchall()

        self.assertIn("idx_orders_updated_at_id", [row[1] for row in indexes])
        self.assertEqual([row[2] for row in columns], ["updated_at", "id"])


if __name__ == "__main__":
    unittest.main()
