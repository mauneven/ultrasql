import json
import subprocess
import sys
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "validate-benchmark-certification.py"
COMMIT = "0123456789abcdef0123456789abcdef01234567"
OTHER_COMMIT = "fedcba9876543210fedcba9876543210fedcba98"
ENGINES = ["ultrasql", "duckdb", "clickhouse", "sqlite3", "postgres"]


def write_json(path: Path, doc: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")


def manifest(commit: str = COMMIT, storage_mode: str = "data-dir") -> dict:
    return {
        "schema_version": 1,
        "mode": "full",
        "iters": 32,
        "warmup": 8,
        "rows": [10000],
        "ultrasql_version": "ultrasqld 0.0.9",
        "ultrasql_install_source": "ULTRASQLD_BIN",
        "ultrasql_storage_mode": storage_mode,
        "methodology": "external release artifact over TCP with ClickHouse",
        "host": {
            "hostname": "host-a",
            "os": "test-os",
            "machine": "arm64",
            "cpu_model": "test cpu",
            "logical_cpus": 8,
            "memory_bytes": 17179869184,
            "rustc": "rustc 1.95.0",
            "git_commit": commit,
        },
        "engine_versions": {
            "ultrasql": "ultrasqld 0.0.9",
            "duckdb": "duckdb 1.5",
            "clickhouse": "ClickHouse 26.5",
            "sqlite": "3.51",
            "postgres": "14.22",
        },
    }


def raw_record(workload: str, engine: str, median_us: float, *, rows: int = 10000) -> dict:
    return {
        "schema_version": 1,
        "status": "measured",
        "workload": workload,
        "engine": engine,
        "n_rows": rows,
        "median_us": median_us,
        "samples": 32,
        "iterations_us": [median_us],
    }


def write_artifact(root: Path, *, commit: str = COMMIT, missing_engine: str | None = None) -> None:
    raw_dir = root / "raw"
    workload = "select_scan_10k"
    engines = {}
    for index, engine in enumerate(ENGINES):
        if engine == missing_engine:
            continue
        median = 10.0 + index
        record = raw_record(workload, engine, median)
        path = raw_dir / f"{workload}-{engine}.json"
        write_json(path, record)
        engines[engine] = {
            "engine": engine,
            "workload": workload,
            "family": "select_scan",
            "n_rows": 10000,
            "median_us": median,
            "samples": 32,
            "server_mode": "external" if engine == "ultrasql" else None,
            "path": str(path),
        }

    write_json(root / "scale_sweep_manifest.json", manifest(commit=commit))
    write_json(
        root / "scale_sweep.json",
        {
            "schema_version": 1,
            "raw_dir": str(raw_dir),
            "engine_order": ENGINES,
            "rows": [
                {
                    "workload": "select_scan",
                    "workload_label": "SELECT scan",
                    "n_rows": 10000,
                    "engines": engines,
                    "fastest_engine": "ultrasql",
                    "fastest_median_us": 10.0,
                    "correctness_status": None,
                    "answer_sha256": None,
                }
            ],
            "policy": "Only measured raw artifacts are rendered.",
        },
    )


def run_validator(root: Path, *extra: str) -> dict:
    out = root / "benchmark_certification_status.json"
    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--artifact-dir",
            str(root),
            "--commit",
            COMMIT,
            "--out",
            str(out),
            "--min-comparable-rows",
            "1",
            *extra,
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(out.read_text())


def contains_key(value: object, key: str) -> bool:
    if isinstance(value, dict):
        return key in value or any(contains_key(child, key) for child in value.values())
    if isinstance(value, list):
        return any(contains_key(child, key) for child in value)
    return False


class BenchmarkCertificationValidatorTests(unittest.TestCase):
    def test_accepts_fresh_same_host_clickhouse_scale_sweep(self) -> None:
        with tempfile_dir() as tmp_path:
            write_artifact(tmp_path)

            status = run_validator(tmp_path)

            self.assertTrue(status["ready"])
            self.assertEqual(status["status"], "ready")
            self.assertEqual(status["release_commit"], COMMIT)
            self.assertEqual(status["comparable_row_count"], 1)
            self.assertEqual(status["ultrasql_fastest_comparable_row_count"], 1)
            self.assertEqual(status["missing_required_engine_rows"], [])
            self.assertFalse(contains_key(status, "winner"))

    def test_rejects_stale_commit(self) -> None:
        with tempfile_dir() as tmp_path:
            write_artifact(tmp_path, commit=OTHER_COMMIT)

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertIn(
                f"manifest host.git_commit expected commit {COMMIT}, got {OTHER_COMMIT}",
                status["errors"],
            )

    def test_rejects_missing_clickhouse_measurement(self) -> None:
        with tempfile_dir() as tmp_path:
            write_artifact(tmp_path, missing_engine="clickhouse")

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertEqual(status["comparable_row_count"], 0)
            self.assertEqual(
                status["missing_required_engine_rows"],
                [
                    {
                        "workload": "select_scan",
                        "n_rows": 10000,
                        "missing_engines": ["clickhouse"],
                    }
                ],
            )

    def test_rejects_rendered_fastest_that_disagrees_with_raw_medians(self) -> None:
        with tempfile_dir() as tmp_path:
            write_artifact(tmp_path)
            rendered_path = tmp_path / "scale_sweep.json"
            rendered = json.loads(rendered_path.read_text())
            rendered["rows"][0]["fastest_engine"] = "duckdb"
            rendered["rows"][0]["fastest_median_us"] = 11.0
            write_json(rendered_path, rendered)

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertTrue(
                any("rendered fastest_engine must match raw medians" in error for error in status["errors"])
            )

    def test_release_status_requires_data_dir_mode(self) -> None:
        with tempfile_dir() as tmp_path:
            write_artifact(tmp_path)
            write_json(tmp_path / "scale_sweep_manifest.json", manifest(storage_mode="memory"))

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertIn(
                "ultrasql_storage_mode expected data-dir, got memory",
                status["errors"],
            )


class tempfile_dir:
    def __enter__(self) -> Path:
        import tempfile

        self._tmp = tempfile.TemporaryDirectory()
        return Path(self._tmp.name)

    def __exit__(self, exc_type, exc, tb) -> None:
        self._tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
