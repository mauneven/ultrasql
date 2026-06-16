import hashlib
import json
import os
import socket
import stat
import subprocess
import sys
import textwrap
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "run-operator-soak.py"
COMMIT = "0123456789abcdef0123456789abcdef01234567"


def write_executable(path: Path, text: str) -> None:
    path.write_text(textwrap.dedent(text).lstrip())
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


class OperatorSoakRunnerTests(unittest.TestCase):
    def test_smoke_runner_emits_schema_v2_report_with_workload_evidence(self) -> None:
        with tempfile_dir() as tmp_path:
            sql_log = tmp_path / "sql.log"
            fake_server = tmp_path / "ultrasqld"
            fake_psql = tmp_path / "psql"
            report = tmp_path / "operator-soak.json"
            write_executable(
                fake_server,
                """
                #!/usr/bin/env python3
                import socket
                import sys
                import time

                listen = "127.0.0.1:5432"
                for index, arg in enumerate(sys.argv):
                    if arg == "--listen":
                        listen = sys.argv[index + 1]
                host, port = listen.rsplit(":", 1)
                sock = socket.socket()
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind((host, int(port)))
                sock.listen()
                sock.settimeout(0.2)
                while True:
                    try:
                        conn, _ = sock.accept()
                        conn.close()
                    except socket.timeout:
                        time.sleep(0.05)
                """,
            )
            write_executable(
                fake_psql,
                f"""
                #!/usr/bin/env python3
                import sys
                from pathlib import Path

                sql = ""
                if "-c" in sys.argv:
                    sql = sys.argv[sys.argv.index("-c") + 1]
                Path({str(sql_log)!r}).open("a").write(sql + "\\n---\\n")
                if "COUNT" in sql or "SUM" in sql:
                    print("3|300")
                elif "COPY" in sql:
                    print("1,100")
                else:
                    print("OK")
                """,
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--mode",
                    "smoke",
                    "--commit",
                    COMMIT,
                    "--ultrasqld",
                    str(fake_server),
                    "--psql",
                    str(fake_psql),
                    "--data-dir",
                    str(tmp_path / "data"),
                    "--out",
                    str(report),
                    "--duration-seconds",
                    "0",
                    "--cycles",
                    "1",
                    "--operator-id",
                    "operator-a",
                    "--host-id",
                    "host-a",
                    "--concurrency",
                    "2",
                ],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            doc = json.loads(report.read_text())
            self.assertEqual(doc["schema_version"], 2)
            self.assertEqual(doc["mode"], "smoke")
            self.assertEqual(doc["commit"], COMMIT)
            self.assertEqual(doc["final_verdict"], "smoke_pass")
            self.assertEqual(doc["operator"]["id_hash"], hashlib.sha256(b"operator-a").hexdigest())
            self.assertEqual(doc["host"]["id_hash"], hashlib.sha256(b"host-a").hexdigest())
            self.assertEqual(doc["db_binary"]["sha256"], hashlib.sha256(fake_server.read_bytes()).hexdigest())
            self.assertGreater(doc["operations"]["total"], 0)
            self.assertGreaterEqual(doc["latency_ms"]["p99"], doc["latency_ms"]["p50"])
            self.assertTrue(doc["consistency_checks"][0]["passed"])
            self.assertTrue(doc["wal_replay_checks"][0]["passed"])
            sql = sql_log.read_text()
            for needle in ["JSONB", "CREATE VIEW", "BEGIN", "COMMIT", "COPY"]:
                self.assertIn(needle, sql)


class tempfile_dir:
    def __enter__(self) -> Path:
        import tempfile

        self._tmp = tempfile.TemporaryDirectory()
        return Path(self._tmp.name)

    def __exit__(self, exc_type, exc, tb) -> None:
        self._tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
