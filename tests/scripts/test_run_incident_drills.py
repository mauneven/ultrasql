import json
import os
import stat
import subprocess
import sys
import textwrap
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "run-incident-drills.py"
COMMIT = "0123456789abcdef0123456789abcdef01234567"


def write_executable(path: Path, text: str) -> None:
    path.write_text(textwrap.dedent(text).lstrip())
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


class IncidentDrillRunnerTests(unittest.TestCase):
    def test_smoke_runner_converts_backup_and_chaos_manifests_to_reports(self) -> None:
        with tempfile_dir() as tmp_path:
            backup = tmp_path / "backup.sh"
            chaos = tmp_path / "chaos.sh"
            reports = tmp_path / "incident-drills"
            write_executable(
                backup,
                """
                #!/usr/bin/env bash
                set -euo pipefail
                mkdir -p "$BACKUP_RESTORE_OUT_DIR"
                cat > "$BACKUP_RESTORE_OUT_DIR/backup_restore_smoke_manifest.json" <<'JSON'
                {
                  "schema_version": 1,
                  "suite": "backup_restore_smoke",
                  "status": "measured",
                  "row_count_verified": true,
                  "index_query_verified": true,
                  "dump_formats_verified": ["custom", "directory", "tar"]
                }
                JSON
                """,
            )
            write_executable(
                chaos,
                """
                #!/usr/bin/env bash
                set -euo pipefail
                mkdir -p "$CHAOS_OUT_DIR"
                cat > "$CHAOS_OUT_DIR/chaos_recovery_manifest.json" <<'JSON'
                {
                  "schema_version": 1,
                  "suite": "chaos_recovery",
                  "status": "measured",
                  "passed": true,
                  "cases": [
                    {
                      "name": "random_kill",
                      "status": "passed",
                      "restarted_after_kill": true,
                      "row_count_verified": true,
                      "expected_rows": 12,
                      "recovered_rows": 12
                    },
                    {
                      "name": "disk_full",
                      "status": "passed",
                      "disk_full_recovered_without_corruption": true,
                      "row_count_verified": true,
                      "expected_rows": 80,
                      "recovered_rows": 80
                    }
                  ]
                }
                JSON
                """,
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--mode",
                    "smoke",
                    "--commit",
                    COMMIT,
                    "--backup-script",
                    str(backup),
                    "--chaos-script",
                    str(chaos),
                    "--reports-dir",
                    str(reports),
                    "--work-dir",
                    str(tmp_path / "work"),
                    "--operator-id",
                    "incident-operator",
                ],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            docs = {
                path.stem: json.loads(path.read_text())
                for path in sorted(reports.glob("*.json"))
            }
            self.assertEqual(set(docs), {"backup_restore", "disk_full", "wal_recovery"})
            for drill_type, report in docs.items():
                self.assertEqual(report["schema_version"], 2)
                self.assertEqual(report["mode"], "smoke")
                self.assertEqual(report["commit"], COMMIT)
                self.assertEqual(report["drill_type"], drill_type)
                self.assertTrue(report["correctness_verified"])
                self.assertFalse(report["data_loss_confirmed"])
                self.assertTrue(report["checks"])
                self.assertIn("manifest_path", report["artifacts"])


class tempfile_dir:
    def __enter__(self) -> Path:
        import tempfile

        self._tmp = tempfile.TemporaryDirectory()
        return Path(self._tmp.name)

    def __exit__(self, exc_type, exc, tb) -> None:
        self._tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
