#!/usr/bin/env python3
"""Build, run, and validate release driver compatibility evidence."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path


GIT_COMMIT_RE = re.compile(r"^[0-9a-fA-F]{40}$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo-root",
        default=Path(__file__).resolve().parents[1],
        type=Path,
        help="UltraSQL repository root",
    )
    parser.add_argument(
        "--commit",
        help="40-hex release commit to certify; defaults to git rev-parse HEAD",
    )
    parser.add_argument(
        "--ultrasqld",
        default=Path("target/release-ship/ultrasqld"),
        type=Path,
        help="ultrasqld path relative to repo root, or absolute path",
    )
    parser.add_argument(
        "--report",
        default=Path("target/driver-certification.json"),
        type=Path,
        help="driver certification report path",
    )
    parser.add_argument(
        "--out",
        default=Path("benchmarks/results/latest/driver_compatibility_status.json"),
        type=Path,
        help="driver compatibility status path",
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python interpreter used for certification and validation scripts",
    )
    parser.add_argument(
        "--no-strict",
        action="store_true",
        help="write status without failing when validation reports not_ready",
    )
    return parser.parse_args()


def resolve_path(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def parse_commit(value: str) -> str:
    commit = value.strip().lower()
    if not GIT_COMMIT_RE.fullmatch(commit):
        raise ValueError("must be a full 40-character hex git commit")
    return commit


def current_commit(repo_root: Path) -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_root,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or "git rev-parse HEAD failed")
    return parse_commit(completed.stdout)


def run_checked(cmd: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> None:
    print("+ " + " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=cwd, env=env, check=True)


def load_status(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as err:  # noqa: BLE001 - CLI should report malformed status.
        raise RuntimeError(f"cannot read validation status {path}: {err}") from err
    if not isinstance(value, dict):
        raise RuntimeError(f"validation status must be a JSON object: {path}")
    return value


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    try:
        release_commit = parse_commit(args.commit) if args.commit else current_commit(repo_root)
    except ValueError as err:
        print(f"--commit {err}", file=sys.stderr)
        return 2
    except RuntimeError as err:
        print(str(err), file=sys.stderr)
        return 2

    ultrasqld = resolve_path(repo_root, args.ultrasqld)
    report = resolve_path(repo_root, args.report)
    out = resolve_path(repo_root, args.out)
    validator = repo_root / "scripts" / "validate-driver-compatibility.py"
    harness = repo_root / "tests" / "driver_certification" / "driver_certification.py"

    env = os.environ.copy()
    env["GITHUB_SHA"] = release_commit
    try:
        run_checked(
            [
                "cargo",
                "build",
                "--profile",
                "release-ship",
                "-p",
                "ultrasql-server",
                "--bin",
                "ultrasqld",
            ],
            cwd=repo_root,
        )
        run_checked(
            [
                args.python,
                str(harness),
                "--ultrasqld",
                str(ultrasqld),
                "--json-output",
                str(report),
            ],
            cwd=repo_root,
            env=env,
        )
        validate_cmd = [
            args.python,
            str(validator),
            "--report",
            str(report),
            "--commit",
            release_commit,
            "--out",
            str(out),
        ]
        if not args.no_strict:
            validate_cmd.append("--strict")
        run_checked(validate_cmd, cwd=repo_root)
    except subprocess.CalledProcessError as err:
        return int(err.returncode)

    status = load_status(out)
    print(json.dumps(status, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
