"""Behavioral tests for packaging/docker/docker-entrypoint.sh.

The entrypoint must make the container both bootable and secure: a bare run
with no auth configured refuses to start, a password enables SCRAM auth, and an
explicit trust opt-in is honored. These run the real script with `sh` against a
stub `ultrasqld` that just prints its argv, so we assert the assembled command
line without compiling or launching the server.
"""

import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
ENTRYPOINT = REPO / "packaging" / "docker" / "docker-entrypoint.sh"

STUB = """#!/bin/sh
printf 'ARGV'
for a in "$@"; do printf '\\t%s' "$a"; done
printf '\\n'
"""


class DockerEntrypointTest(unittest.TestCase):
    def setUp(self):
        self.assertTrue(ENTRYPOINT.is_file(), f"missing {ENTRYPOINT}")

    def run_entrypoint(self, *args, env=None, stub_names=("ultrasqld",)):
        """Run the entrypoint with the given argv and environment.

        Stubs each name in ``stub_names`` on PATH as an executable that echoes
        its argv. Returns (returncode, argv_list_or_None, stderr).
        """
        with tempfile.TemporaryDirectory() as bindir:
            for name in stub_names:
                stub = Path(bindir) / name
                stub.write_text(STUB)
                stub.chmod(stub.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
            run_env = {"PATH": f"{bindir}:{os.environ.get('PATH', '')}"}
            if env:
                run_env.update(env)
            proc = subprocess.run(
                ["sh", str(ENTRYPOINT), *args],
                capture_output=True,
                text=True,
                env=run_env,
            )
            argv = None
            for line in proc.stdout.splitlines():
                if line.startswith("ARGV"):
                    argv = line.split("\t")[1:]
            return proc.returncode, argv, proc.stderr

    def test_refuses_without_auth(self):
        code, argv, stderr = self.run_entrypoint()
        self.assertEqual(code, 1)
        self.assertIsNone(argv, "server must not be exec'd without auth")
        self.assertIn("no authentication configured", stderr)

    def test_password_enables_scram(self):
        code, argv, _ = self.run_entrypoint(env={"ULTRASQL_PASSWORD": "s3cret"})
        self.assertEqual(code, 0)
        self.assertIn("--auth-user", argv)
        self.assertEqual(argv[argv.index("--auth-user") + 1], "ultrasql")
        self.assertIn("--auth-method", argv)
        self.assertEqual(argv[argv.index("--auth-method") + 1], "scram")
        self.assertIn("--auth-password-file", argv)
        # Default bind + data dir are injected.
        self.assertEqual(argv[argv.index("--listen") + 1], "0.0.0.0:5432")
        self.assertEqual(argv[argv.index("--data-dir") + 1], "/var/lib/ultrasql")

    def test_password_file_used_directly(self):
        code, argv, _ = self.run_entrypoint(
            env={"ULTRASQL_PASSWORD_FILE": "/run/secrets/pw"}
        )
        self.assertEqual(code, 0)
        self.assertEqual(argv[argv.index("--auth-password-file") + 1], "/run/secrets/pw")

    def test_custom_user_method_listen(self):
        code, argv, _ = self.run_entrypoint(
            env={
                "ULTRASQL_PASSWORD": "pw",
                "ULTRASQL_USER": "app",
                "ULTRASQL_AUTH_METHOD": "md5",
                "ULTRASQL_LISTEN": "127.0.0.1:6000",
            }
        )
        self.assertEqual(code, 0)
        self.assertEqual(argv[argv.index("--auth-user") + 1], "app")
        self.assertEqual(argv[argv.index("--auth-method") + 1], "md5")
        self.assertEqual(argv[argv.index("--listen") + 1], "127.0.0.1:6000")

    def test_trust_optin(self):
        code, argv, stderr = self.run_entrypoint(
            env={"ULTRASQL_HOST_AUTH_METHOD": "trust"}
        )
        self.assertEqual(code, 0)
        self.assertIn("--insecure-no-auth", argv)
        self.assertIn("WARNING", stderr)

    def test_operator_auth_flag_respected(self):
        # An explicit auth flag wins; no env-derived auth is injected, even when
        # a password is also present.
        code, argv, _ = self.run_entrypoint(
            "ultrasqld",
            "--hba-file",
            "/etc/ultrasql/pg_hba.conf",
            env={"ULTRASQL_PASSWORD": "ignored"},
        )
        self.assertEqual(code, 0)
        self.assertNotIn("--auth-user", argv)
        self.assertIn("--hba-file", argv)

    def test_operator_listen_not_overridden(self):
        code, argv, _ = self.run_entrypoint(
            "ultrasqld",
            "--listen",
            "0.0.0.0:7777",
            env={"ULTRASQL_PASSWORD": "pw", "ULTRASQL_LISTEN": "127.0.0.1:6000"},
        )
        self.assertEqual(code, 0)
        # Operator's explicit --listen is preserved; the script does not add a
        # second one (which clap would reject).
        self.assertEqual(argv.count("--listen"), 1)
        self.assertEqual(argv[argv.index("--listen") + 1], "0.0.0.0:7777")

    def test_non_ultrasqld_command_passthrough(self):
        code, argv, _ = self.run_entrypoint(
            "othercmd", "arg1", stub_names=("ultrasqld", "othercmd")
        )
        self.assertEqual(code, 0)
        self.assertEqual(argv, ["arg1"])


if __name__ == "__main__":
    unittest.main()
