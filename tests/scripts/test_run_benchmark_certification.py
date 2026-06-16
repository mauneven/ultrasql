import json
import os
import subprocess
import sys
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "run-benchmark-certification.py"
COMMIT = "0123456789abcdef0123456789abcdef01234567"


def write_executable(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    path.chmod(0o755)


class BenchmarkCertificationRunnerTests(unittest.TestCase):
    def test_runner_builds_current_binary_path_runs_sweep_and_validates_status(self) -> None:
        with tempfile_dir() as repo:
            status = repo / "status" / "benchmark_certification_status.json"
            out_dir = repo / "artifacts" / "scale-sweep"
            ultrasqld = repo / "target" / "release-ship" / "ultrasqld"
            write_executable(ultrasqld, "#!/usr/bin/env bash\nexit 0\n")
            write_executable(
                repo / "benchmarks" / "run_scale_sweep.sh",
                """#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$SCALE_SWEEP_OUT"
{
  echo "mode=$1"
  echo "ULTRASQLD_BIN=$ULTRASQLD_BIN"
  echo "SCALE_SWEEP_STORAGE=$SCALE_SWEEP_STORAGE"
} > "$SCALE_SWEEP_OUT/run.env"
""",
            )
            write_executable(
                repo / "scripts" / "validate-benchmark-certification.py",
                """#!/usr/bin/env python3
import json
import sys
from pathlib import Path
out = Path(sys.argv[sys.argv.index("--out") + 1])
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps({
    "ready": True,
    "status": "ready",
    "argv": sys.argv[1:],
}, indent=2, sort_keys=True) + "\\n")
""",
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--repo-root",
                    str(repo),
                    "--commit",
                    COMMIT,
                    "--mode",
                    "quick",
                    "--out-dir",
                    str(out_dir),
                    "--status-out",
                    str(status),
                    "--ultrasqld",
                    str(ultrasqld),
                    "--storage",
                    "data-dir",
                    "--required-engines",
                    "ultrasql,duckdb",
                    "--min-comparable-rows",
                    "1",
                    "--skip-build",
                    "--skip-clickhouse-check",
                    "--python",
                    sys.executable,
                ],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            env_file = (out_dir / "run.env").read_text()
            self.assertIn("mode=quick", env_file)
            self.assertIn(f"ULTRASQLD_BIN={ultrasqld}", env_file)
            self.assertIn("SCALE_SWEEP_STORAGE=data-dir", env_file)

            payload = json.loads(status.read_text())
            self.assertTrue(payload["ready"])
            self.assertIn("--required-storage-mode", payload["argv"])
            self.assertIn("data-dir", payload["argv"])
            self.assertIn("--required-engines", payload["argv"])
            self.assertIn("ultrasql,duckdb", payload["argv"])

    def test_no_strict_writes_status_after_sweep_failure(self) -> None:
        with tempfile_dir() as repo:
            status = repo / "status" / "benchmark_certification_status.json"
            out_dir = repo / "artifacts" / "scale-sweep"
            ultrasqld = repo / "target" / "release-ship" / "ultrasqld"
            write_executable(ultrasqld, "#!/usr/bin/env bash\nexit 0\n")
            write_executable(
                repo / "benchmarks" / "run_scale_sweep.sh",
                """#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$SCALE_SWEEP_OUT"
echo "partial artifact" > "$SCALE_SWEEP_OUT/partial.txt"
exit 7
""",
            )
            write_executable(
                repo / "scripts" / "validate-benchmark-certification.py",
                """#!/usr/bin/env python3
import json
import sys
from pathlib import Path
out = Path(sys.argv[sys.argv.index("--out") + 1])
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps({
    "ready": False,
    "status": "not_ready",
    "blockers": ["sweep_failed"],
    "argv": sys.argv[1:],
}, indent=2, sort_keys=True) + "\\n")
""",
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--repo-root",
                    str(repo),
                    "--commit",
                    COMMIT,
                    "--mode",
                    "quick",
                    "--out-dir",
                    str(out_dir),
                    "--status-out",
                    str(status),
                    "--ultrasqld",
                    str(ultrasqld),
                    "--storage",
                    "data-dir",
                    "--required-engines",
                    "ultrasql",
                    "--min-comparable-rows",
                    "1",
                    "--skip-build",
                    "--skip-clickhouse-check",
                    "--python",
                    sys.executable,
                    "--no-strict",
                ],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            self.assertIn("continuing to write not_ready status", proc.stderr)
            self.assertTrue((out_dir / "partial.txt").exists())
            payload = json.loads(status.read_text())
            self.assertFalse(payload["ready"])
            self.assertEqual(payload["status"], "not_ready")


class tempfile_dir:
    def __enter__(self) -> Path:
        import tempfile

        self._tmp = tempfile.TemporaryDirectory()
        return Path(self._tmp.name)

    def __exit__(self, exc_type, exc, tb) -> None:
        self._tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
