import json
import os
import stat
import subprocess
import sys
import textwrap
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "run-driver-release-evidence.py"
COMMIT = "0123456789abcdef0123456789abcdef01234567"


def write_executable(path: Path, text: str) -> None:
    path.write_text(textwrap.dedent(text).lstrip())
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def make_fake_repo(root: Path) -> None:
    (root / "scripts").mkdir(parents=True)
    (root / "tests" / "driver_certification").mkdir(parents=True)
    (root / "target" / "release-ship").mkdir(parents=True)

    write_executable(
        root / "scripts" / "validate-driver-compatibility.py",
        f"""
        #!/usr/bin/env python3
        import argparse
        import json
        import sys
        from pathlib import Path

        parser = argparse.ArgumentParser()
        parser.add_argument("--report", required=True)
        parser.add_argument("--commit", required=True)
        parser.add_argument("--out", required=True)
        parser.add_argument("--strict", action="store_true")
        args = parser.parse_args()
        report = json.loads(Path(args.report).read_text())
        ready = report["commit"] == args.commit
        doc = {{
            "ready": ready,
            "status": "ready" if ready else "not_ready",
            "commit": report["commit"],
            "expected_commit": args.commit,
            "strict": args.strict,
        }}
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(json.dumps(doc, sort_keys=True) + "\\n")
        sys.exit(0 if ready or not args.strict else 1)
        """,
    )
    write_executable(
        root / "tests" / "driver_certification" / "driver_certification.py",
        """
        #!/usr/bin/env python3
        import argparse
        import json
        import os
        from pathlib import Path

        parser = argparse.ArgumentParser()
        parser.add_argument("--ultrasqld", required=True)
        parser.add_argument("--json-output", required=True)
        args = parser.parse_args()
        if not Path(args.ultrasqld).exists():
            raise SystemExit("missing ultrasqld")
        Path(args.json_output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.json_output).write_text(json.dumps({
            "commit": os.environ["GITHUB_SHA"],
            "drivers": [],
        }) + "\\n")
        """,
    )


class DriverReleaseEvidenceRunnerTests(unittest.TestCase):
    def test_runner_builds_certifies_and_validates_current_commit(self) -> None:
        with tempfile_dir() as tmp_path:
            repo = tmp_path / "repo"
            repo.mkdir()
            make_fake_repo(repo)
            fake_bin = tmp_path / "bin"
            fake_bin.mkdir()
            cargo_log = tmp_path / "cargo.log"
            write_executable(
                fake_bin / "cargo",
                f"""
                #!/usr/bin/env python3
                from pathlib import Path
                import os
                import sys

                Path({str(cargo_log)!r}).write_text(" ".join(sys.argv[1:]) + "\\n")
                binary = Path.cwd() / "target" / "release-ship" / "ultrasqld"
                binary.parent.mkdir(parents=True, exist_ok=True)
                binary.write_text("#!/bin/sh\\nexit 0\\n")
                binary.chmod(0o755)
                """,
            )
            env = os.environ.copy()
            env["PATH"] = f"{fake_bin}{os.pathsep}{env['PATH']}"
            status = repo / "benchmarks" / "results" / "latest" / "driver_compatibility_status.json"

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--repo-root",
                    str(repo),
                    "--commit",
                    COMMIT,
                    "--out",
                    str(status),
                ],
                check=False,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            self.assertIn(
                "build --profile release-ship -p ultrasql-server --bin ultrasqld",
                cargo_log.read_text(),
            )
            report = json.loads((repo / "target" / "driver-certification.json").read_text())
            self.assertEqual(report["commit"], COMMIT)
            doc = json.loads(status.read_text())
            self.assertTrue(doc["ready"])
            self.assertTrue(doc["strict"])
            self.assertEqual(doc["expected_commit"], COMMIT)


class tempfile_dir:
    def __enter__(self) -> Path:
        import tempfile

        self._tmp = tempfile.TemporaryDirectory()
        return Path(self._tmp.name)

    def __exit__(self, exc_type, exc, tb) -> None:
        self._tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
