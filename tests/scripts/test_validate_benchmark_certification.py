import hashlib
import json
import shutil
import subprocess
import sys
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "validate-benchmark-certification.py"
RENDERER = REPO / "benchmarks" / "scripts" / "render_scale_sweep.py"
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


def raw_record(
    workload: str,
    engine: str,
    median_us: float,
    *,
    rows: int = 10000,
    storage_mode: str = "data-dir",
    durability_mode: str = "durable",
) -> dict:
    return {
        "schema_version": 1,
        "status": "measured",
        "workload": workload,
        "engine": engine,
        "n_rows": rows,
        "storage_mode": storage_mode,
        "durability_mode": durability_mode,
        "median_us": median_us,
        "samples": 32,
        "iterations_us": [median_us] * 32,
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


def write_mixed_correctness_artifact(root: Path) -> None:
    raw_dir = root / "raw"
    workload = "mixed_correctness_10k"
    answer = [["-5000"]]
    answer_hash = hashlib.sha256(
        json.dumps(answer, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    engines = {}
    for index, engine in enumerate(ENGINES):
        median = 10.0 + index
        record = raw_record(workload, engine, median)
        record["answer"] = answer
        record["answer_sha256"] = answer_hash
        path = raw_dir / f"{workload}-{engine}.json"
        write_json(path, record)
        engines[engine] = {
            "engine": engine,
            "workload": workload,
            "family": "mixed_correctness",
            "n_rows": 10000,
            "median_us": median,
            "samples": 32,
            "server_mode": "external" if engine == "ultrasql" else None,
            "answer_sha256": answer_hash,
            "path": str(path),
        }

    write_json(root / "scale_sweep_manifest.json", manifest())
    write_json(
        root / "scale_sweep.json",
        {
            "schema_version": 1,
            "raw_dir": str(raw_dir),
            "engine_order": ENGINES,
            "rows": [
                {
                    "workload": "mixed_correctness",
                    "workload_label": "Mixed correctness",
                    "n_rows": 10000,
                    "engines": engines,
                    "fastest_engine": "ultrasql",
                    "fastest_median_us": 10.0,
                    "correctness_status": "verified",
                    "answer_sha256": answer_hash,
                }
            ],
            "policy": "Only measured raw artifacts are rendered.",
        },
    )


def render_artifact(root: Path) -> dict:
    raw_dir = root / "raw"
    workload = "select_scan_10k"
    for index, engine in enumerate(ENGINES):
        write_json(
            raw_dir / f"{workload}-{engine}.json",
            raw_record(workload, engine, 10.0 + index),
        )
    write_json(root / "scale_sweep_manifest.json", manifest())
    proc = subprocess.run(
        [
            sys.executable,
            str(RENDERER),
            "--raw-dir",
            str(raw_dir),
            "--output-md",
            str(root / "scale_sweep.md"),
            "--output-json",
            str(root / "scale_sweep.json"),
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads((root / "scale_sweep.json").read_text())


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
    def test_renderer_rejects_canonical_engine_alias_collision(self) -> None:
        with tempfile_dir() as tmp_path:
            raw_dir = tmp_path / "raw"
            write_json(
                raw_dir / "select_scan_10k-postgres.json",
                raw_record("select_scan_10k", "postgres", 10.0),
            )
            write_json(
                raw_dir / "select_scan_10k-postgresql.json",
                raw_record("select_scan_10k", "postgresql", 11.0),
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(RENDERER),
                    "--raw-dir",
                    str(raw_dir),
                    "--output-md",
                    str(tmp_path / "scale_sweep.md"),
                    "--output-json",
                    str(tmp_path / "scale_sweep.json"),
                ],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("duplicate raw benchmark evidence", proc.stderr)
            self.assertIn("engine=postgres", proc.stderr)

    def test_renderer_rejects_nonstandard_raw_directory(self) -> None:
        with tempfile_dir() as tmp_path:
            raw_dir = tmp_path / "evidence"
            write_json(
                raw_dir / "select_scan_10k-ultrasql.json",
                raw_record("select_scan_10k", "ultrasql", 10.0),
            )
            proc = subprocess.run(
                [
                    sys.executable,
                    str(RENDERER),
                    "--raw-dir",
                    str(raw_dir),
                    "--output-md",
                    str(tmp_path / "scale_sweep.md"),
                    "--output-json",
                    str(tmp_path / "scale_sweep.json"),
                ],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("must be the artifact's 'raw' directory", proc.stderr)

    def test_malformed_rendered_path_is_reported_not_crashed(self) -> None:
        with tempfile_dir() as tmp_path:
            write_artifact(tmp_path)
            rendered_path = tmp_path / "scale_sweep.json"
            rendered = json.loads(rendered_path.read_text())
            rendered["rows"][0]["engines"]["ultrasql"]["path"] = "\x00"
            write_json(rendered_path, rendered)

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertTrue(
                any("ultrasql: missing raw path" in error for error in status["errors"]),
                status["errors"],
            )

    def test_rendered_artifact_validates_after_copy_and_rejects_missing_raw(self) -> None:
        with tempfile_dir() as tmp_path:
            source = tmp_path / "source"
            rendered = render_artifact(source)

            self.assertEqual(rendered["raw_dir"], "raw")
            rendered_paths = {
                entry["path"]
                for entry in rendered["rows"][0]["engines"].values()
            }
            self.assertTrue(rendered_paths)
            self.assertTrue(
                all(path.startswith("raw/") for path in rendered_paths),
                rendered_paths,
            )
            self.assertTrue(
                all(not Path(path).is_absolute() for path in rendered_paths),
                rendered_paths,
            )

            copied = tmp_path / "copied"
            shutil.copytree(source, copied)

            status = run_validator(copied)

            self.assertTrue(status["ready"], status["errors"])

            missing_raw = copied / "raw" / "select_scan_10k-duckdb.json"
            missing_raw.unlink()
            status = run_validator(copied)

            self.assertFalse(status["ready"])
            self.assertTrue(
                any(
                    "cannot parse" in error and str(missing_raw) in error
                    for error in status["errors"]
                ),
                status["errors"],
            )

    def test_moved_legacy_absolute_paths_resolve_inside_artifact(self) -> None:
        with tempfile_dir() as tmp_path:
            source = tmp_path / "legacy-source"
            write_artifact(source)
            moved = tmp_path / "legacy-moved"
            shutil.copytree(source, moved)
            shutil.rmtree(source)

            status = run_validator(moved)

            self.assertTrue(status["ready"], status["errors"])

    def test_legacy_source_path_cannot_mask_missing_local_raw(self) -> None:
        with tempfile_dir() as tmp_path:
            source = tmp_path / "legacy-source"
            write_artifact(source)
            copied = tmp_path / "legacy-copy"
            shutil.copytree(source, copied)
            missing_raw = copied / "raw" / "select_scan_10k-duckdb.json"
            missing_raw.unlink()

            status = run_validator(copied)

            self.assertFalse(status["ready"])
            self.assertTrue(
                any(
                    "cannot parse" in error and str(missing_raw) in error
                    for error in status["errors"]
                ),
                status["errors"],
            )

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

    def test_rejects_duplicate_rendered_rows_as_comparable_count_bypass(self) -> None:
        with tempfile_dir() as tmp_path:
            write_artifact(tmp_path)
            rendered_path = tmp_path / "scale_sweep.json"
            rendered = json.loads(rendered_path.read_text())
            rendered["rows"] = rendered["rows"] * 24
            write_json(rendered_path, rendered)

            status = run_validator(tmp_path, "--min-comparable-rows", "24")

            self.assertFalse(status["ready"])
            self.assertEqual(status["comparable_row_count"], 1)
            self.assertTrue(
                any("duplicates rendered row" in error for error in status["errors"]),
                status["errors"],
            )

    def test_rejects_manifest_row_scales_not_present_in_rendered_sweep(self) -> None:
        with tempfile_dir() as tmp_path:
            write_artifact(tmp_path)
            manifest_path = tmp_path / "scale_sweep_manifest.json"
            manifest_doc = json.loads(manifest_path.read_text())
            manifest_doc["rows"] = [100_000]
            write_json(manifest_path, manifest_doc)

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertIn(
                "manifest rows do not match rendered scalable row set: "
                "manifest=[100000], rendered=[10000]",
                status["errors"],
            )

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

    def test_rejects_raw_storage_profile_mismatch_for_data_dir_release(self) -> None:
        with tempfile_dir() as tmp_path:
            write_artifact(tmp_path)
            raw_path = tmp_path / "raw" / "select_scan_10k-duckdb.json"
            raw = json.loads(raw_path.read_text())
            raw["storage_mode"] = "memory"
            raw["durability_mode"] = "volatile"
            write_json(raw_path, raw)

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertTrue(
                any(
                    "duckdb: raw storage_mode expected data-dir, got memory" in error
                    for error in status["errors"]
                )
            )
            self.assertTrue(
                any(
                    "duckdb: raw durability_mode expected durable, got volatile" in error
                    for error in status["errors"]
                )
            )

    def test_rejects_raw_sample_count_mismatch(self) -> None:
        with tempfile_dir() as tmp_path:
            write_artifact(tmp_path)
            raw_path = tmp_path / "raw" / "select_scan_10k-duckdb.json"
            raw = json.loads(raw_path.read_text())
            raw["samples"] = 31
            write_json(raw_path, raw)

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertTrue(
                any(
                    "samples=31 does not match iterations_us length 32" in error
                    for error in status["errors"]
                ),
                status["errors"],
            )

    def test_rejects_sample_count_that_disagrees_with_manifest_iterations(self) -> None:
        with tempfile_dir() as tmp_path:
            write_artifact(tmp_path)
            raw_path = tmp_path / "raw" / "select_scan_10k-duckdb.json"
            raw = json.loads(raw_path.read_text())
            raw["samples"] = 31
            raw["iterations_us"] = [11.0] * 31
            write_json(raw_path, raw)
            rendered_path = tmp_path / "scale_sweep.json"
            rendered = json.loads(rendered_path.read_text())
            rendered["rows"][0]["engines"]["duckdb"]["samples"] = 31
            write_json(rendered_path, rendered)

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertTrue(
                any(
                    "duckdb: rendered samples 31 do not match manifest iters 32"
                    in error
                    for error in status["errors"]
                ),
                status["errors"],
            )
            self.assertTrue(
                any(
                    "duckdb: raw samples do not match manifest iters 32" in error
                    for error in status["errors"]
                ),
                status["errors"],
            )

    def test_rejects_rendered_sample_count_mismatch_with_raw(self) -> None:
        with tempfile_dir() as tmp_path:
            write_artifact(tmp_path)
            rendered_path = tmp_path / "scale_sweep.json"
            rendered = json.loads(rendered_path.read_text())
            rendered["rows"][0]["engines"]["duckdb"]["samples"] = 31
            write_json(rendered_path, rendered)

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertTrue(
                any("duckdb: raw samples mismatch" in error for error in status["errors"]),
                status["errors"],
            )

    def test_rejects_non_positive_rendered_samples(self) -> None:
        with tempfile_dir() as tmp_path:
            write_artifact(tmp_path)
            rendered_path = tmp_path / "scale_sweep.json"
            rendered = json.loads(rendered_path.read_text())
            rendered["rows"][0]["engines"]["duckdb"]["samples"] = 0
            write_json(rendered_path, rendered)

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertTrue(
                any(
                    "duckdb: rendered samples must be a positive integer" in error
                    for error in status["errors"]
                ),
                status["errors"],
            )

    def test_rejects_rendered_entry_relabelled_to_another_workload_family(self) -> None:
        with tempfile_dir() as tmp_path:
            write_artifact(tmp_path)
            raw_path = tmp_path / "raw" / "select_scan_10k-duckdb.json"
            raw = json.loads(raw_path.read_text())
            raw["workload"] = "insert_throughput_10k"
            write_json(raw_path, raw)
            rendered_path = tmp_path / "scale_sweep.json"
            rendered = json.loads(rendered_path.read_text())
            rendered["rows"][0]["engines"]["duckdb"]["workload"] = (
                "insert_throughput_10k"
            )
            write_json(rendered_path, rendered)

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertTrue(
                any(
                    "rendered workload 'insert_throughput_10k' does not belong "
                    "to row family 'select_scan'" in error
                    for error in status["errors"]
                ),
                status["errors"],
            )

    def test_rejects_rendered_entry_identity_mismatch(self) -> None:
        with tempfile_dir() as tmp_path:
            write_artifact(tmp_path)
            rendered_path = tmp_path / "scale_sweep.json"
            rendered = json.loads(rendered_path.read_text())
            duckdb = rendered["rows"][0]["engines"]["duckdb"]
            duckdb["engine"] = "sqlite3"
            duckdb["n_rows"] = 100_000
            write_json(rendered_path, rendered)

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertTrue(
                any("duckdb: entry engine is sqlite3" in error for error in status["errors"]),
                status["errors"],
            )
            self.assertTrue(
                any(
                    "duckdb: rendered n_rows is 100000" in error
                    for error in status["errors"]
                ),
                status["errors"],
            )

    def test_rejects_raw_median_not_derived_from_iterations(self) -> None:
        with tempfile_dir() as tmp_path:
            write_artifact(tmp_path)
            raw_path = tmp_path / "raw" / "select_scan_10k-duckdb.json"
            raw = json.loads(raw_path.read_text())
            raw["iterations_us"] = [99.0] * 32
            write_json(raw_path, raw)

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertTrue(
                any(
                    "median_us=11.0 does not match iterations_us median 99.0" in error
                    for error in status["errors"]
                ),
                status["errors"],
            )

    def test_accepts_raw_median_at_serialization_tolerance(self) -> None:
        with tempfile_dir() as tmp_path:
            write_artifact(tmp_path)
            raw_path = tmp_path / "raw" / "select_scan_10k-duckdb.json"
            raw = json.loads(raw_path.read_text())
            raw["iterations_us"] = [11.0005] * 32
            write_json(raw_path, raw)

            status = run_validator(tmp_path)

            self.assertTrue(status["ready"], status["errors"])

    def test_rejects_raw_median_beyond_serialization_tolerance(self) -> None:
        with tempfile_dir() as tmp_path:
            write_artifact(tmp_path)
            raw_path = tmp_path / "raw" / "select_scan_10k-duckdb.json"
            raw = json.loads(raw_path.read_text())
            raw["iterations_us"] = [11.000501] * 32
            write_json(raw_path, raw)

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertTrue(
                any(
                    "does not match iterations_us median 11.000501 within 0.0005 us"
                    in error
                    for error in status["errors"]
                ),
                status["errors"],
            )

    def test_rejects_non_finite_raw_measurement(self) -> None:
        with tempfile_dir() as tmp_path:
            write_artifact(tmp_path)
            raw_path = tmp_path / "raw" / "select_scan_10k-duckdb.json"
            raw = json.loads(raw_path.read_text())
            raw["median_us"] = float("inf")
            write_json(raw_path, raw)

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertTrue(
                any(
                    "non-finite JSON number Infinity is not allowed" in error
                    for error in status["errors"]
                ),
                status["errors"],
            )

    def test_rendered_measurement_cannot_reference_not_available_raw(self) -> None:
        with tempfile_dir() as tmp_path:
            write_artifact(tmp_path)
            raw_path = tmp_path / "raw" / "select_scan_10k-duckdb.json"
            raw = json.loads(raw_path.read_text())
            raw["status"] = "not_available"
            raw["reason"] = "driver unavailable"
            for field in ["median_us", "samples", "iterations_us"]:
                raw.pop(field)
            write_json(raw_path, raw)

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertTrue(
                any(
                    "duckdb: rendered measurement must reference raw status=measured"
                    in error
                    for error in status["errors"]
                ),
                status["errors"],
            )

    def test_rejects_mixed_correctness_hash_not_derived_from_raw_answer(self) -> None:
        with tempfile_dir() as tmp_path:
            write_mixed_correctness_artifact(tmp_path)
            false_hash = "b" * 64
            for engine in ENGINES:
                raw_path = tmp_path / "raw" / f"mixed_correctness_10k-{engine}.json"
                raw = json.loads(raw_path.read_text())
                raw["answer_sha256"] = false_hash
                write_json(raw_path, raw)
            rendered_path = tmp_path / "scale_sweep.json"
            rendered = json.loads(rendered_path.read_text())
            rendered["rows"][0]["answer_sha256"] = false_hash
            for entry in rendered["rows"][0]["engines"].values():
                entry["answer_sha256"] = false_hash
            write_json(rendered_path, rendered)

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertTrue(
                any(
                    "answer_sha256 does not match canonical answer" in error
                    for error in status["errors"]
                ),
                status["errors"],
            )

    def test_rejects_mixed_correctness_raw_answer_disagreement(self) -> None:
        with tempfile_dir() as tmp_path:
            write_mixed_correctness_artifact(tmp_path)
            raw_path = tmp_path / "raw" / "mixed_correctness_10k-duckdb.json"
            raw = json.loads(raw_path.read_text())
            raw["answer"] = [["different"]]
            raw["answer_sha256"] = hashlib.sha256(
                json.dumps(
                    raw["answer"],
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
            write_json(raw_path, raw)

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertTrue(
                any(
                    "duckdb: raw answer_sha256 does not match rendered answer_sha256"
                    in error
                    for error in status["errors"]
                ),
                status["errors"],
            )

    def write_single_row(
        self,
        root: Path,
        medians: dict[str, float],
        *,
        fastest_engine: str,
        not_available_engines: dict[str, str] | None = None,
    ) -> None:
        """Render one select_scan_10k row from explicit per-engine medians.

        Engines in `medians` are measured; engines in `not_available_engines`
        get an explicit not_available raw artifact and are absent from the
        rendered row, exactly as the renderer produces them.
        """
        raw_dir = root / "raw"
        workload = "select_scan_10k"
        engines = {}
        for engine, median in medians.items():
            path = raw_dir / f"{workload}-{engine}.json"
            write_json(path, raw_record(workload, engine, median))
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
        for engine, reason in (not_available_engines or {}).items():
            write_json(
                raw_dir / f"{workload}-{engine}.json",
                {
                    "schema_version": 1,
                    "status": "not_available",
                    "engine": engine,
                    "workload": workload,
                    "n_rows": 10000,
                    "storage_mode": "data-dir",
                    "durability_mode": "durable",
                    "reason": reason,
                },
            )
        write_json(root / "scale_sweep_manifest.json", manifest())
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
                        "fastest_engine": fastest_engine,
                        "fastest_median_us": medians[fastest_engine],
                        "correctness_status": None,
                        "answer_sha256": None,
                    }
                ],
                "policy": "Only measured raw artifacts are rendered.",
            },
        )

    def test_ready_when_ultrasql_loses_a_row(self) -> None:
        # Honest semantics: a fair, complete, schema-valid sweep is ready even
        # when a competitor is faster. The loss is reported, not gated.
        with tempfile_dir() as tmp_path:
            self.write_single_row(
                tmp_path,
                {
                    "ultrasql": 20.0,
                    "duckdb": 10.0,
                    "clickhouse": 30.0,
                    "sqlite3": 40.0,
                    "postgres": 50.0,
                },
                fastest_engine="duckdb",
            )

            status = run_validator(tmp_path)

            self.assertTrue(status["ready"], status["reasons"])
            self.assertEqual(status["ultrasql_fastest_row_count"], 0)
            board = status["scoreboard"]
            self.assertEqual(board["ultrasql_win_count"], 0)
            self.assertEqual(board["ultrasql_loss_count"], 1)
            self.assertEqual(len(board["losses"]), 1)
            loss = board["losses"][0]
            self.assertEqual(loss["winner"], "duckdb")
            self.assertAlmostEqual(loss["gap_pct"], 100.0)

    def test_not_available_row_is_complete_but_not_comparable(self) -> None:
        # An explicit not_available artifact documents coverage, but it cannot
        # satisfy --min-comparable-rows because no cross-engine comparison exists.
        with tempfile_dir() as tmp_path:
            self.write_single_row(
                tmp_path,
                {
                    "duckdb": 10.0,
                    "clickhouse": 11.0,
                    "sqlite3": 12.0,
                    "postgres": 13.0,
                },
                fastest_engine="duckdb",
                not_available_engines={"ultrasql": "wal buffer full at 1M rows"},
            )

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertEqual(status["complete_row_count"], 1)
            self.assertEqual(status["comparable_row_count"], 0)
            self.assertEqual(status["missing_required_engine_rows"], [])
            self.assertIn(
                "comparable_row_count 0 below minimum 1",
                status["errors"],
            )
            board = status["scoreboard"]
            self.assertEqual(board["ultrasql_not_available_count"], 1)
            self.assertEqual(board["rows"][0]["ultrasql_result"], "not_available")

    def test_blank_not_available_reason_does_not_satisfy_coverage(self) -> None:
        with tempfile_dir() as tmp_path:
            self.write_single_row(
                tmp_path,
                {
                    "duckdb": 10.0,
                    "clickhouse": 11.0,
                    "sqlite3": 12.0,
                    "postgres": 13.0,
                },
                fastest_engine="duckdb",
                not_available_engines={"ultrasql": " "},
            )

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertTrue(
                any("reason must be a non-empty string" in error for error in status["errors"]),
                status["errors"],
            )
            self.assertEqual(
                status["missing_required_engine_rows"][0]["missing_engines"],
                ["ultrasql"],
            )

    def test_symlinked_not_available_outside_artifact_does_not_satisfy_coverage(self) -> None:
        with tempfile_dir() as tmp_path:
            artifact = tmp_path / "artifact"
            self.write_single_row(
                artifact,
                {
                    "duckdb": 10.0,
                    "clickhouse": 11.0,
                    "sqlite3": 12.0,
                    "postgres": 13.0,
                },
                fastest_engine="duckdb",
            )
            external = tmp_path / "outside-ultrasql.json"
            write_json(
                external,
                {
                    "schema_version": 1,
                    "status": "not_available",
                    "engine": "ultrasql",
                    "workload": "select_scan_10k",
                    "n_rows": 10000,
                    "reason": "external evidence must not be trusted",
                },
            )
            symlink = artifact / "raw" / "select_scan_10k-ultrasql.json"
            try:
                symlink.symlink_to(external)
            except OSError as err:
                self.skipTest(f"symlinks unavailable: {err}")

            status = run_validator(artifact)

            self.assertFalse(status["ready"])
            self.assertTrue(
                any(
                    "raw artifact path escapes raw directory" in error
                    for error in status["errors"]
                ),
                status["errors"],
            )
            self.assertEqual(
                status["missing_required_engine_rows"][0]["missing_engines"],
                ["ultrasql"],
            )

    def test_rejects_missing_raw_storage_profile_for_data_dir_release(self) -> None:
        with tempfile_dir() as tmp_path:
            write_artifact(tmp_path)
            raw_path = tmp_path / "raw" / "select_scan_10k-sqlite3.json"
            raw = json.loads(raw_path.read_text())
            raw.pop("storage_mode")
            raw.pop("durability_mode")
            write_json(raw_path, raw)

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertTrue(
                any("sqlite3: raw storage_mode must be a non-empty string" in error for error in status["errors"])
            )
            self.assertTrue(
                any(
                    "sqlite3: raw durability_mode must be a non-empty string" in error
                    for error in status["errors"]
                )
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
