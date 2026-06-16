import json
import os
import subprocess
import sys
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "validate-operator-soak.py"
INSTALL_SH = REPO / "scripts" / "install.sh"
INSTALL_PS1 = REPO / "scripts" / "install.ps1"
COMMIT = "0123456789abcdef0123456789abcdef01234567"


def write_report(
    path: Path,
    *,
    operator_id: str,
    commit: str = COMMIT,
    mode: str = "30d",
    final_verdict: str = "pass",
    error_total: int = 0,
    consistency_passed: bool = True,
    wal_passed: bool = True,
    start: str = "2026-01-01T00:00:00Z",
    end: str = "2026-02-01T00:00:00Z",
) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "mode": mode,
                "commit": commit,
                "started_at": start,
                "ended_at": end,
                "duration_days": 31.0 if mode == "30d" else 0.0001,
                "host": {
                    "id_hash": "host-a",
                    "cpu": "test cpu",
                    "memory_bytes": 68719476736,
                    "storage": "nvme ext4",
                    "os": "linux",
                },
                "operator": {"id_hash": operator_id},
                "workload": {
                    "id": "mixed-sql-soak-v1",
                    "id_hash": "workload-a",
                    "sql_surface": ["ddl", "crud", "transactions", "jsonb"],
                },
                "db_binary": {
                    "path": "target/debug/ultrasqld",
                    "sha256": "a" * 64,
                },
                "config": {
                    "ultrasqld_command": "ultrasqld --data-dir /redacted",
                    "data_dir": "/redacted",
                    "ops_endpoint": "127.0.0.1:8080",
                    "health_check_interval": "30s",
                },
                "dataset_scale": {"rows": 1000},
                "concurrency": 8,
                "operations": {
                    "total": 100,
                    "ddl": 4,
                    "read": 30,
                    "write": 30,
                    "transactions": 20,
                    "copy": 1,
                    "export_import": 1,
                },
                "latency_ms": {"p50": 1.0, "p95": 2.0, "p99": 3.0},
                "throughput_ops_per_sec": 10.0,
                "errors": {
                    "total": error_total,
                    "availability": 0,
                    "sql": error_total,
                    "correctness": 0,
                    "corruption": 0,
                    "critical": 0,
                    "high": 0,
                },
                "restart_count": 1,
                "crash_recovery_count": 0,
                "consistency_checks": [
                    {
                        "name": "row_count",
                        "passed": consistency_passed,
                        "checksum": "b" * 64,
                    }
                ],
                "wal_replay_checks": [
                    {"name": "clean_restart", "passed": wal_passed}
                ],
                "final_verdict": final_verdict,
                "log_bundle_path": "artifact://logs",
                "signed_off_by": "reviewer",
            }
        )
        + "\n"
    )


def run_validator(reports_dir: Path, *extra: str) -> dict:
    out = reports_dir / "status.json"
    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--reports-dir",
            str(reports_dir),
            "--min-reports",
            "3",
            "--min-days",
            "30",
            "--now",
            "2026-02-02T00:00:00Z",
            "--out",
            str(out),
            *extra,
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(out.read_text())


class OperatorSoakValidatorTests(unittest.TestCase):
    def test_soak_normalizes_operator_hash_before_independence_count(self) -> None:
        with tempfile_dir() as tmp_path:
            write_report(tmp_path / "a.json", operator_id="op-a")
            write_report(tmp_path / "b.json", operator_id=" op-a ")
            write_report(tmp_path / "c.json", operator_id="op-a\t")

            status = run_validator(tmp_path, "--commit", COMMIT)

            self.assertFalse(status["ready"])
            self.assertEqual(status["independent_operator_count"], 1)
            self.assertTrue(
                any("independent operator" in reason for reason in status["reasons"])
            )

    def test_soak_rejects_non_string_operator_hashes(self) -> None:
        with tempfile_dir() as tmp_path:
            write_report(tmp_path / "a.json", operator_id=1)
            write_report(tmp_path / "b.json", operator_id=2)
            write_report(tmp_path / "c.json", operator_id=3)

            status = run_validator(tmp_path, "--commit", COMMIT)

            self.assertFalse(status["ready"])
            for report in status["reports"]:
                self.assertTrue(
                    any(
                        "operator.id_hash must be a non-empty string" in error
                        for error in report["errors"]
                    )
                )

    def test_soak_requires_positive_concurrency(self) -> None:
        with tempfile_dir() as tmp_path:
            write_report(tmp_path / "a.json", operator_id="op-a")
            write_report(tmp_path / "b.json", operator_id="op-b")
            write_report(tmp_path / "c.json", operator_id="op-c")
            report = json.loads((tmp_path / "c.json").read_text())
            report["concurrency"] = 0
            (tmp_path / "c.json").write_text(json.dumps(report) + "\n")

            status = run_validator(tmp_path, "--commit", COMMIT)

            self.assertFalse(status["ready"])
            bad_report = next(
                report for report in status["reports"] if report["operator_id"] == "op-c"
            )
            self.assertTrue(
                any(
                    "concurrency must be a positive integer" in error
                    for error in bad_report["errors"]
                )
            )

    def test_soak_requires_same_release_commit(self) -> None:
        with self.subTest("mixed commit"):
            with tempfile_dir() as tmp_path:
                write_report(tmp_path / "a.json", operator_id="op-a")
                write_report(tmp_path / "b.json", operator_id="op-b")
                write_report(
                    tmp_path / "c.json",
                    operator_id="op-c",
                    commit="fedcba9876543210fedcba9876543210fedcba98",
                )

                status = run_validator(tmp_path, "--commit", COMMIT)

                self.assertFalse(status["ready"])
                self.assertTrue(
                    any("expected commit" in reason for reason in status["reasons"])
                )
                bad_report = next(
                    report
                    for report in status["reports"]
                    if report["operator_id"] == "op-c"
                )
                self.assertTrue(
                    any("expected commit" in error for error in bad_report["errors"])
                )

    def test_soak_rejects_future_report_end(self) -> None:
        with tempfile_dir() as tmp_path:
            write_report(tmp_path / "a.json", operator_id="op-a")
            write_report(tmp_path / "b.json", operator_id="op-b")
            write_report(
                tmp_path / "c.json",
                operator_id="op-c",
                end="2026-03-01T00:00:00Z",
            )

            status = run_validator(tmp_path, "--commit", COMMIT)

            self.assertFalse(status["ready"])
            bad_report = next(
                report for report in status["reports"] if report["operator_id"] == "op-c"
            )
            self.assertTrue(
                any("ended_at is in the future" in error for error in bad_report["errors"])
            )

    def test_soak_rejects_errors_and_failed_integrity_checks(self) -> None:
        with tempfile_dir() as tmp_path:
            write_report(tmp_path / "a.json", operator_id="op-a")
            write_report(tmp_path / "b.json", operator_id="op-b")
            write_report(
                tmp_path / "c.json",
                operator_id="op-c",
                error_total=1,
                consistency_passed=False,
                wal_passed=False,
            )

            status = run_validator(tmp_path, "--commit", COMMIT)

            self.assertFalse(status["ready"])
            bad_report = next(
                report for report in status["reports"] if report["operator_id"] == "op-c"
            )
            errors = "\n".join(bad_report["errors"])
            self.assertIn("errors.total must be zero", errors)
            self.assertIn("consistency_checks[0].passed must be true", errors)
            self.assertIn("wal_replay_checks[0].passed must be true", errors)

    def test_soak_accepts_smoke_mode_only_as_non_ready_dev_check(self) -> None:
        with tempfile_dir() as tmp_path:
            write_report(
                tmp_path / "smoke.json",
                operator_id="op-smoke",
                mode="smoke",
                final_verdict="smoke_pass",
                start="2026-02-01T00:00:00Z",
                end="2026-02-01T00:10:00Z",
            )

            status = run_validator(tmp_path, "--commit", COMMIT)

            self.assertFalse(status["ready"])
            self.assertEqual(status["smoke_valid_report_count"], 1)
            self.assertTrue(any("smoke" in reason for reason in status["reasons"]))

    def test_soak_release_schema_v2_reports_can_close_gate(self) -> None:
        with tempfile_dir() as tmp_path:
            write_report(tmp_path / "a.json", operator_id="op-a")
            write_report(tmp_path / "b.json", operator_id="op-b")
            write_report(tmp_path / "c.json", operator_id="op-c")

            status = run_validator(tmp_path, "--commit", COMMIT)

            self.assertTrue(status["ready"])
            self.assertEqual(status["status"], "ready")
            self.assertEqual(status["valid_report_count"], 3)
            self.assertEqual(status["smoke_valid_report_count"], 0)
            self.assertEqual(status["valid_release_commits"], [COMMIT])


class InstallScriptHardeningTests(unittest.TestCase):
    def test_install_sh_rejects_path_like_version_before_download(self) -> None:
        with tempfile_dir() as tmp_path:
            fake_bin = tmp_path / "bin"
            fake_bin.mkdir()
            for name in ["curl", "tar"]:
                tool = fake_bin / name
                tool.write_text(
                    "#!/usr/bin/env sh\n"
                    f"echo '{name} should not run for invalid version' >&2\n"
                    "exit 99\n"
                )
                tool.chmod(0o755)
            env = os.environ.copy()
            env["PATH"] = f"{fake_bin}{os.pathsep}{env['PATH']}"
            env["HOME"] = str(tmp_path)

            proc = subprocess.run(
                ["sh", str(INSTALL_SH), "v1/bad"],
                cwd=REPO,
                env=env,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            self.assertEqual(proc.returncode, 1)
            self.assertIn("invalid release version", proc.stderr)
            self.assertNotIn("curl should not run", proc.stderr)

    def test_install_scripts_validate_archive_paths(self) -> None:
        install_sh = INSTALL_SH.read_text()
        install_ps1 = INSTALL_PS1.read_text()

        for needle in ["validate_tar_members", "tar -tzf", "archive contains unexpected path"]:
            self.assertIn(needle, install_sh)
        for needle in ["Validate-ZipMembers", "System.IO.Compression.ZipFile", "archive contains unexpected path"]:
            self.assertIn(needle, install_ps1)


class tempfile_dir:
    def __enter__(self) -> Path:
        import tempfile

        self._tmp = tempfile.TemporaryDirectory()
        return Path(self._tmp.name)

    def __exit__(self, exc_type, exc, tb) -> None:
        self._tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
