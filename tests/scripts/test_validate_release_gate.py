import json
import subprocess
import sys
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "validate-release-evidence.py"
COMMIT = "0123456789abcdef0123456789abcdef01234567"
OTHER_COMMIT = "fedcba9876543210fedcba9876543210fedcba98"


def write_json(path: Path, doc: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")


def ready_status(commit_field: str = "release_commit", commit: str = COMMIT) -> dict:
    return {
        "schema_version": 1,
        "status": "ready",
        "ready": True,
        commit_field: commit,
        "reasons": [],
        "errors": [],
    }


def write_all_ready(root: Path) -> dict[str, Path]:
    paths = {
        "driver": root / "driver.json",
        "operator": root / "operator.json",
        "audit": root / "audit.json",
        "drill": root / "drill.json",
        "benchmark": root / "benchmark.json",
        "ci": root / "ci.json",
    }
    write_json(paths["driver"], ready_status("commit"))
    write_json(paths["operator"], ready_status())
    write_json(paths["audit"], ready_status())
    write_json(paths["drill"], ready_status())
    write_json(paths["benchmark"], ready_status())
    write_json(paths["ci"], {"status": "ready", "ready": True, "commit": COMMIT, "run_id": 123})
    return paths


def run_gate(root: Path, paths: dict[str, Path], *extra: str) -> dict:
    out = root / "release_gate_status.json"
    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--commit",
            COMMIT,
            "--driver-status",
            str(paths["driver"]),
            "--operator-soak-status",
            str(paths["operator"]),
            "--external-audit-status",
            str(paths["audit"]),
            "--incident-drill-status",
            str(paths["drill"]),
            "--benchmark-status",
            str(paths["benchmark"]),
            "--ci-status",
            str(paths["ci"]),
            "--out",
            str(out),
            "--now",
            "2026-06-16T00:00:00Z",
            *extra,
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr + proc.stdout
    return json.loads(out.read_text())


class ReleaseGateValidatorTests(unittest.TestCase):
    def test_all_ready_statuses_pass_release_gate(self) -> None:
        with tempfile_dir() as tmp_path:
            paths = write_all_ready(tmp_path)

            status = run_gate(tmp_path, paths)

            self.assertTrue(status["ready"])
            self.assertEqual(status["status"], "ready")
            self.assertEqual(status["blockers"], [])
            self.assertEqual(status["expected_commit"], COMMIT)

    def test_missing_evidence_fails_closed(self) -> None:
        with tempfile_dir() as tmp_path:
            paths = write_all_ready(tmp_path)
            paths["benchmark"].unlink()

            status = run_gate(tmp_path, paths)

            self.assertFalse(status["ready"])
            self.assertEqual(status["status"], "not_ready")
            self.assertTrue(
                any(
                    blocker["gate"] == "benchmark"
                    and "missing evidence file" in blocker["reason"]
                    for blocker in status["blockers"]
                )
            )

    def test_not_ready_gate_reasons_become_blockers(self) -> None:
        with tempfile_dir() as tmp_path:
            paths = write_all_ready(tmp_path)
            write_json(
                paths["operator"],
                {
                    "status": "not_ready",
                    "ready": False,
                    "release_commit": COMMIT,
                    "reasons": ["0 valid release report(s), need 3"],
                },
            )

            status = run_gate(tmp_path, paths)

            self.assertFalse(status["ready"])
            self.assertTrue(
                any(
                    blocker["gate"] == "operator_soak"
                    and "0 valid release report(s), need 3" in blocker["reason"]
                    for blocker in status["blockers"]
                )
            )

    def test_stale_commit_fails_even_when_gate_claims_ready(self) -> None:
        with tempfile_dir() as tmp_path:
            paths = write_all_ready(tmp_path)
            write_json(paths["driver"], ready_status("commit", OTHER_COMMIT))

            status = run_gate(tmp_path, paths)

            self.assertFalse(status["ready"])
            self.assertTrue(
                any(
                    blocker["gate"] == "driver_compatibility"
                    and f"expected commit {COMMIT}, got {OTHER_COMMIT}" in blocker["reason"]
                    for blocker in status["blockers"]
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
