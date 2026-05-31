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
    failure_count: int = 0,
    start: str = "2026-01-01T00:00:00Z",
    end: str = "2026-02-01T00:00:00Z",
) -> None:
    path.write_text(
        json.dumps(
            {
                "operator_id": operator_id,
                "commit": commit,
                "start_time_utc": start,
                "end_time_utc": end,
                "host_cpu": "test cpu",
                "host_memory": "64 GiB",
                "host_storage": "nvme ext4",
                "os": "linux",
                "ultrasqld_command": "ultrasqld --data-dir /redacted",
                "workload": "mixed sql smoke",
                "client_count": 8,
                "data_dir": "/redacted",
                "ops_endpoint": "127.0.0.1:8080",
                "health_check_interval": "30s",
                "failure_count": failure_count,
                "correctness_issue_count": 0,
                "critical_issue_count": 0,
                "high_issue_count": 0,
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
                any("end_time_utc is in the future" in error for error in bad_report["errors"])
            )

    def test_soak_rejects_availability_failures(self) -> None:
        with tempfile_dir() as tmp_path:
            write_report(tmp_path / "a.json", operator_id="op-a")
            write_report(tmp_path / "b.json", operator_id="op-b")
            write_report(tmp_path / "c.json", operator_id="op-c", failure_count=1)

            status = run_validator(tmp_path, "--commit", COMMIT)

            self.assertFalse(status["ready"])
            bad_report = next(
                report for report in status["reports"] if report["operator_id"] == "op-c"
            )
            self.assertTrue(
                any("failure_count must be zero" in error for error in bad_report["errors"])
            )


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
