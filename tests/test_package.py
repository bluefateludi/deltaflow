from __future__ import annotations

import importlib.metadata
import subprocess
import sysconfig
import unittest
from pathlib import Path


class InstalledPackageTests(unittest.TestCase):
    def test_distribution_exposes_qsync_console_script(self) -> None:
        entry_points = importlib.metadata.distribution("qsync").entry_points
        qsync_scripts = [
            entry_point
            for entry_point in entry_points
            if entry_point.group == "console_scripts" and entry_point.name == "qsync"
        ]

        self.assertEqual(len(qsync_scripts), 1)
        self.assertEqual(qsync_scripts[0].value, "qsync.cli:main")

    def test_qsync_help_runs_from_installed_console_script(self) -> None:
        executable = Path(sysconfig.get_path("scripts")) / "qsync"
        self.assertTrue(executable.is_file(), "qsync console script is not installed")

        result = subprocess.run(
            [str(executable), "--help"],
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("DeltaFlow", result.stdout)
        self.assertIn("init-demo", result.stdout)


if __name__ == "__main__":
    unittest.main()
