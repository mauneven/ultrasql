import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]


def workflow_job_block(path: Path, job: str) -> str:
    lines = path.read_text().splitlines()
    jobs_line = next(
        index for index, line in enumerate(lines) if line.strip() == "jobs:"
    )
    start = None
    for index in range(jobs_line + 1, len(lines)):
        line = lines[index]
        if line.startswith(f"  {job}:"):
            start = index
            break
    if start is None:
        raise AssertionError(f"{path.relative_to(REPO)} missing job {job!r}")

    end = len(lines)
    for index in range(start + 1, len(lines)):
        line = lines[index]
        if (
            line.startswith("  ")
            and not line.startswith("    ")
            and line.rstrip().endswith(":")
        ):
            end = index
            break
    return "\n".join(lines[start:end])


class WorkflowTimeoutTests(unittest.TestCase):
    def test_heavy_ci_jobs_have_explicit_timeouts(self) -> None:
        required_jobs = {
            REPO / ".github" / "workflows" / "ci.yml": [
                "test",
                "driver_certification",
            ],
            REPO / ".github" / "workflows" / "release.yml": [
                "verify",
                "build",
            ],
        }
        offenders: list[str] = []

        for path, jobs in required_jobs.items():
            for job in jobs:
                block = workflow_job_block(path, job)
                if "timeout-minutes:" not in block:
                    offenders.append(f"{path.relative_to(REPO)}:{job}")

        self.assertEqual([], offenders)


if __name__ == "__main__":
    unittest.main()
