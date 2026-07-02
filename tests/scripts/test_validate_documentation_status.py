import json
import subprocess
import sys
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "validate-documentation-status.py"


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value) + "\n")


def write_doc(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def write_minimal_repo(root: Path, *, audit_extra: str = "") -> None:
    write_doc(root / "README.md", "UltraSQL is alpha.\n")
    write_doc(root / ".github" / "pull_request_template.md", "## Checklist\n")
    write_doc(
        root / "docs" / "production-readiness.md",
        """
# Production Readiness Audit

UltraSQL is not production ready for v1.0 yet.

Not allowed:

```text
UltraSQL is production ready.
UltraSQL beats every database on every workload.
```

The release-artifact scale sweep shows UltraSQL fastest on all 2 comparable
rows. The benchmark certification profile is smoke only; this is not full
release benchmark certification.
""".lstrip(),
    )
    write_doc(
        root / "docs" / "known-limitations.md",
        """
# Known Limitations

The README release-artifact scale sweep is a same-host fastest-table result,
not full benchmark release certification.
""".lstrip(),
    )
    write_doc(
        root / "docs" / "documentation-status-audit.md",
        f"""
# Documentation Status Audit

UltraSQL is alpha. It is not production ready until the release gates close
with evidence.

The benchmark claim does not mean the full release benchmark-certification gate
is closed.

| File | Audit result |
| --- | --- |
| `.github/pull_request_template.md` | checked |
| `README.md` | checked |
| `docs/documentation-status-audit.md` | checked |
| `docs/known-limitations.md` | checked |
| `docs/production-readiness.md` | checked |
{audit_extra}""".lstrip(),
    )
    write_json(
        root / "benchmarks" / "results" / "latest" / "scale-sweep" / "scale_sweep.json",
        {
            "schema_version": 1,
            "rows": [
                {"workload": "select_scan", "n_rows": 10, "fastest_engine": "ultrasql"},
                {"workload": "select_scan", "n_rows": 100, "fastest_engine": "ultrasql"},
            ],
        },
    )
    write_json(
        root / "benchmarks" / "results" / "latest" / "benchmark_certification_manifest.json",
        {"profile": "smoke", "passed": True},
    )


def run_validator(root: Path) -> dict:
    out = root / "status.json"
    proc = subprocess.run(
        [sys.executable, str(SCRIPT), "--repo", str(root), "--out", str(out)],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(out.read_text())


class DocumentationStatusValidatorTests(unittest.TestCase):
    def test_accepts_complete_alpha_audit_with_scoped_benchmark_claim(self) -> None:
        with tempfile_dir() as tmp_path:
            write_minimal_repo(tmp_path)

            status = run_validator(tmp_path)

            self.assertTrue(status["ready"])
            self.assertEqual(status["first_party_markdown_count"], 5)
            self.assertEqual(status["ledger_missing"], [])
            self.assertEqual(status["old_status_matches"], [])
            self.assertEqual(status["benchmark"]["ultrasql_fastest_row_count"], 2)

    def test_scale_sweep_losses_are_reported_not_errors(self) -> None:
        # A row where another engine is fastest is an honest measurement.
        # It must appear in the summary but must never fail the audit.
        with tempfile_dir() as tmp_path:
            write_minimal_repo(tmp_path)
            write_json(
                tmp_path
                / "benchmarks"
                / "results"
                / "latest"
                / "scale-sweep"
                / "scale_sweep.json",
                {
                    "schema_version": 1,
                    "rows": [
                        {"workload": "select_scan", "n_rows": 10, "fastest_engine": "ultrasql"},
                        {"workload": "update_1m", "n_rows": 1000000, "fastest_engine": "duckdb"},
                    ],
                },
            )

            status = run_validator(tmp_path)

            self.assertTrue(status["ready"])
            self.assertEqual(status["benchmark"]["ultrasql_fastest_row_count"], 1)
            self.assertEqual(
                status["benchmark"]["non_ultrasql_fastest_rows"],
                [
                    {
                        "workload": "update_1m",
                        "n_rows": 1000000,
                        "fastest_engine": "duckdb",
                    }
                ],
            )

    def test_rejects_missing_markdown_ledger_entry(self) -> None:
        with tempfile_dir() as tmp_path:
            write_minimal_repo(tmp_path)
            write_doc(tmp_path / "docs" / "extra.md", "extra\n")

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertIn("docs/extra.md", status["ledger_missing"])

    def test_rejects_stale_lower_maturity_label(self) -> None:
        with tempfile_dir() as tmp_path:
            write_minimal_repo(tmp_path)
            write_doc(tmp_path / "README.md", "UltraSQL is pre-alpha.\n")

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertEqual(status["old_status_matches"], ["README.md:1"])

    def test_rejects_unscoped_universal_production_claim(self) -> None:
        with tempfile_dir() as tmp_path:
            write_minimal_repo(tmp_path)
            write_doc(
                tmp_path / "docs" / "production-readiness.md",
                "UltraSQL is production ready.\n",
            )

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertEqual(
                status["unsupported_claim_matches"],
                ["docs/production-readiness.md:1: UltraSQL is production ready."],
            )

    def test_rejects_smoke_manifest_without_full_certification_qualification(self) -> None:
        with tempfile_dir() as tmp_path:
            write_minimal_repo(tmp_path)
            write_doc(
                tmp_path / "docs" / "known-limitations.md",
                "The README table is fast.\n",
            )

            status = run_validator(tmp_path)

            self.assertFalse(status["ready"])
            self.assertTrue(
                any("known-limitations.md" in error for error in status["errors"])
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
