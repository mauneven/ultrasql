import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "benchmarks" / "scripts" / "check_supremacy.py"


def write_json(path: Path, doc: dict) -> None:
    path.write_text(json.dumps(doc, indent=2) + "\n")


def measured(workload: str, engine: str, median_us: float) -> dict:
    return {
        "schema_version": 1,
        "status": "measured",
        "workload": workload,
        "engine": engine,
        "median_us": median_us,
    }


def not_available(workload: str, engine: str) -> dict:
    return {
        "schema_version": 1,
        "status": "not_available",
        "workload": workload,
        "engine": engine,
        "median_us": None,
    }


def run_check(raw_dir: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), str(raw_dir)],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


class BenchmarkLeadCheckTests(unittest.TestCase):
    def test_partial_certification_does_not_create_competitor_loss(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw = root / "raw"
            raw.mkdir()
            write_json(raw / "select_scan-ultrasql.json", measured("select_scan", "ultrasql", 10.0))
            write_json(raw / "select_scan-duckdb.json", measured("select_scan", "duckdb", 30.0))
            write_json(raw / "clickbench-firebolt.json", measured("clickbench", "firebolt", 20.0))
            write_json(raw / "clickbench-ultrasql.json", not_available("clickbench", "ultrasql"))
            write_json(
                root / "clickbench_certification.json",
                {
                    "schema_version": 1,
                    "workload": "clickbench",
                    "status": "partial",
                    "comparison_ready": False,
                    "passed": False,
                    "reason": "missing_required_engine_results",
                },
            )

            proc = run_check(raw)

            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            self.assertIn("select_scan", proc.stdout)
            self.assertIn("clickbench", proc.stdout)
            self.assertIn("unranked", proc.stdout)

    def test_missing_ultrasql_without_partial_summary_still_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw = root / "raw"
            raw.mkdir()
            write_json(raw / "orphan-duckdb.json", measured("orphan", "duckdb", 30.0))

            proc = run_check(raw)

            self.assertEqual(proc.returncode, 1)
            self.assertIn("orphan: ultrasql sample missing", proc.stderr)


if __name__ == "__main__":
    unittest.main()
