#!/usr/bin/env python3
"""Unit tests for the driver certification harness."""

from __future__ import annotations

import importlib.util
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


if __name__ == "__main__":
    unittest.main()
