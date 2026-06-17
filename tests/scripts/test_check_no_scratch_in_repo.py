"""CI guard test: benchmark runners must not leak data dirs into target/.

Exercises benchmarks/check_no_scratch_in_repo.sh against hermetic temp
directories so a future script that writes a data dir under target/ fails CI
instead of silently bloating local disk.
"""

import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
GUARD = REPO / "benchmarks" / "check_no_scratch_in_repo.sh"


def run_guard(target_dir: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", str(GUARD), str(target_dir)],
        capture_output=True,
        text=True,
        check=False,
    )


class CheckNoScratchInRepoTest(unittest.TestCase):
    def test_guard_exists_and_is_executable(self) -> None:
        self.assertTrue(GUARD.exists(), f"missing guard: {GUARD}")

    def test_passes_on_only_standard_cargo_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp)
            for name in ("debug", "release", "release-ship", "doc", "tools"):
                (target / name).mkdir()
            result = run_guard(target)
            self.assertEqual(
                result.returncode, 0, msg=f"stderr: {result.stderr}"
            )

    def test_fails_on_leaked_data_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp)
            (target / "debug").mkdir()
            (target / "tpch-scale1-real").mkdir()  # leaked data dir
            result = run_guard(target)
            self.assertEqual(result.returncode, 1)
            self.assertIn("tpch-scale1-real", result.stderr)

    def test_passes_when_target_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = run_guard(Path(tmp) / "no-such-target")
            self.assertEqual(
                result.returncode, 0, msg=f"stderr: {result.stderr}"
            )


if __name__ == "__main__":
    unittest.main()
