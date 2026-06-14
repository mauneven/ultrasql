#!/usr/bin/env python3
"""Unit tests for the driver certification harness."""

from __future__ import annotations

import importlib.util
import hashlib
import json
import ssl
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).with_name("driver_certification.py")
SPEC = importlib.util.spec_from_file_location("driver_certification", SCRIPT)
assert SPEC is not None
driver_certification = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules["driver_certification"] = driver_certification
SPEC.loader.exec_module(driver_certification)


class DownloadFileTests(unittest.TestCase):
    """Download helper behavior."""

    def test_download_file_falls_back_to_verified_curl_when_python_ca_fails(self) -> None:
        """A missing Python CA bundle should not disable HTTPS verification."""

        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "download.bin"
            calls: list[list[str]] = []

            def fake_run_checked(
                cmd: list[str],
                context: str,
                *,
                cwd: Path | None = None,
                env: dict[str, str] | None = None,
            ) -> subprocess.CompletedProcess[str]:
                del cwd, env
                calls.append(cmd)
                self.assertIn("curl", context)
                target.write_bytes(b"payload")
                return subprocess.CompletedProcess(cmd, 0, "", "")

            with (
                mock.patch.object(
                    driver_certification.urllib.request,
                    "urlopen",
                    side_effect=ssl.SSLCertVerificationError(
                        "certificate verify failed"
                    ),
                ),
                mock.patch.object(
                    driver_certification.shutil,
                    "which",
                    return_value="/usr/bin/curl",
                ),
                mock.patch.object(
                    driver_certification,
                    "run_checked",
                    side_effect=fake_run_checked,
                ),
            ):
                driver_certification.download_file(
                    "https://example.invalid/archive.tar.gz",
                    target,
                )

            self.assertEqual(target.read_bytes(), b"payload")
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0][0], "/usr/bin/curl")
            self.assertIn("--fail", calls[0])
            self.assertIn("--location", calls[0])
            self.assertIn("--proto", calls[0])
            self.assertIn("=https", calls[0])
            self.assertIn("--tlsv1.2", calls[0])
            self.assertIn("--output", calls[0])
            self.assertIn(str(target), calls[0])
            self.assertEqual(calls[0][-1], "https://example.invalid/archive.tar.gz")


class JdbcJarTests(unittest.TestCase):
    """JDBC driver artifact download behavior."""

    def test_jdbc_urls_prefer_canonical_maven_central(self) -> None:
        """Use the canonical Maven Central host before the legacy repo1 mirror."""

        self.assertEqual(
            driver_certification.JDBC_URLS[0],
            "https://repo.maven.apache.org/maven2/"
            "org/postgresql/postgresql/42.7.11/postgresql-42.7.11.jar",
        )
        self.assertEqual(
            driver_certification.JDBC_URLS[1],
            "https://repo1.maven.org/maven2/"
            "org/postgresql/postgresql/42.7.11/postgresql-42.7.11.jar",
        )

    def test_jdbc_download_falls_back_between_mirrors(self) -> None:
        """A transient 403 from one Maven mirror should not fail certification."""

        payload = b"jdbc jar bytes"
        expected_digest = hashlib.sha256(payload).hexdigest()

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            calls: list[list[str]] = []

            def fake_run_checked(
                cmd: list[str],
                context: str,
                *,
                cwd: Path | None = None,
                env: dict[str, str] | None = None,
            ) -> subprocess.CompletedProcess[str]:
                del cwd, env
                calls.append(cmd)
                if len(calls) == 1:
                    raise driver_certification.CertificationFailure(context)
                output_path = Path(cmd[cmd.index("-o") + 1])
                output_path.write_bytes(payload)
                return subprocess.CompletedProcess(cmd, 0, "", "")

            with (
                mock.patch.object(driver_certification, "repo_root", return_value=root),
                mock.patch.object(
                    driver_certification,
                    "require_tool",
                    return_value="/usr/bin/curl",
                ),
                mock.patch.object(
                    driver_certification,
                    "JDBC_SHA256",
                    expected_digest,
                ),
                mock.patch.object(
                    driver_certification,
                    "run_checked",
                    side_effect=fake_run_checked,
                ),
            ):
                jar = driver_certification.ensure_jdbc_jar()

            self.assertEqual(jar.read_bytes(), payload)
            self.assertEqual(len(calls), 2)
            self.assertEqual(calls[0][-1], driver_certification.JDBC_URLS[0])
            self.assertEqual(calls[1][-1], driver_certification.JDBC_URLS[1])


class DriverRoleBootstrapTests(unittest.TestCase):
    """Certification role bootstrap behavior."""

    def test_prepare_driver_cert_role_uses_bootstrap_superuser(self) -> None:
        """The harness must register driver_cert before driver checks run."""

        calls: list[dict[str, object]] = []

        def fake_run(
            cmd: list[str],
            *,
            input: str,
            text: bool,
            stdout: int,
            stderr: int,
            check: bool,
        ) -> subprocess.CompletedProcess[str]:
            calls.append(
                {
                    "cmd": cmd,
                    "input": input,
                    "text": text,
                    "stdout": stdout,
                    "stderr": stderr,
                    "check": check,
                }
            )
            return subprocess.CompletedProcess(cmd, 0, "CREATE ROLE\n", "")

        with (
            mock.patch.object(
                driver_certification,
                "require_tool",
                return_value="/usr/bin/psql",
            ),
            mock.patch.object(driver_certification.subprocess, "run", side_effect=fake_run),
        ):
            driver_certification.prepare_driver_cert_role(6543)

        self.assertEqual(len(calls), 1)
        command = calls[0]["cmd"]
        self.assertIsInstance(command, list)
        self.assertEqual(command[0], "/usr/bin/psql")
        self.assertTrue(
            any(
                part.startswith("postgresql://ultrasql@127.0.0.1:6543/ultrasql")
                for part in command
            )
        )
        script = calls[0]["input"]
        self.assertIsInstance(script, str)
        self.assertIn("CREATE ROLE IF NOT EXISTS driver_cert SUPERUSER LOGIN", script)
        self.assertTrue(calls[0]["text"])
        self.assertFalse(calls[0]["check"])


class DriverReportTests(unittest.TestCase):
    """Machine-readable certification report behavior."""

    def test_write_report_records_git_commit_for_release_evidence(self) -> None:
        """Release gates must be able to prove compatibility for the exact commit."""

        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "driver-certification.json"
            binary = Path(tmp) / "ultrasqld"
            result = driver_certification.DriverResult(
                driver="psycopg3",
                version="3.3.2",
                checks=["connect", "parameterized_select"],
            )

            with mock.patch.dict(
                driver_certification.os.environ,
                {"GITHUB_SHA": "abcdef0123456789abcdef0123456789abcdef01"},
            ):
                driver_certification.write_report(report, binary, 6543, [result])

            payload = json.loads(report.read_text())
            self.assertEqual(
                payload["commit"],
                "abcdef0123456789abcdef0123456789abcdef01",
            )
            self.assertEqual(payload["drivers"][0]["driver"], "psycopg3")
            self.assertEqual(payload["drivers"][0]["status"], "pass")


if __name__ == "__main__":
    unittest.main()
