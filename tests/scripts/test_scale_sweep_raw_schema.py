import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SQLITE_RUNNER = REPO / "benchmarks" / "scripts" / "run_sqlite3_writes.sh"


class ScaleSweepRawSchemaTests(unittest.TestCase):
    def test_sqlite_runner_emits_strict_raw_schema(self) -> None:
        if shutil.which("sqlite3") is None:
            self.skipTest("sqlite3 CLI is required for this runner contract")

        with tempfile.TemporaryDirectory() as tmp:
            raw_dir = Path(tmp)
            env = os.environ.copy()
            env.update(
                {
                    "RAW_DIR": str(raw_dir),
                    "N_ROWS": "4",
                    "N_ITERS": "1",
                    "INSERT_CHUNK_ROWS": "4",
                }
            )
            proc = subprocess.run(
                ["bash", str(SQLITE_RUNNER), "insert_throughput_10k"],
                cwd=REPO,
                env=env,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            artifact = raw_dir / "insert_throughput_4-sqlite3.json"
            self.assertTrue(artifact.exists(), proc.stderr + proc.stdout)
            payload = json.loads(artifact.read_text())
            self.assertEqual(payload["schema_version"], 1)
            self.assertEqual(payload["status"], "measured")
            self.assertEqual(payload["policy"], "Raw measured samples only; no ranking or winner claim.")
            self.assertEqual(payload["engine"], "sqlite3")
            self.assertEqual(payload["workload"], "insert_throughput_4")
            self.assertEqual(payload["n_rows"], 4)
            self.assertEqual(payload["samples"], 1)
            self.assertGreater(payload["median_us"], 0)
            self.assertEqual(len(payload["iterations_us"]), 1)


if __name__ == "__main__":
    unittest.main()
