#!/usr/bin/env python3
"""Build, run, render, and validate release benchmark certification evidence."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
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
    parser.add_argument("--mode", choices=["quick", "full"], default="full")
    parser.add_argument(
        "--out-dir",
        default=Path("benchmarks/results/latest/scale-sweep"),
        type=Path,
        help="scale-sweep artifact output directory",
    )
    parser.add_argument(
        "--status-out",
        default=Path("benchmarks/results/latest/benchmark_certification_status.json"),
        type=Path,
        help="benchmark certification status path",
    )
    parser.add_argument(
        "--ultrasqld",
        default=Path("target/release-ship/ultrasqld"),
        type=Path,
        help="current-commit ultrasqld binary path",
    )
    parser.add_argument(
        "--storage",
        choices=["data-dir", "memory"],
        default="data-dir",
        help="UltraSQL storage mode used by benchmarks",
    )
    parser.add_argument(
        "--required-engines",
        default="ultrasql,duckdb,clickhouse,sqlite3,postgres",
        help="comma-separated engines required by validation",
    )
    parser.add_argument(
        "--min-comparable-rows",
        default=24,
        type=int,
        help="minimum fully comparable rows required by validation",
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python interpreter used for validation",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="test-only escape hatch: do not build ultrasqld before running the sweep",
    )
    parser.add_argument(
        "--skip-clickhouse-check",
        action="store_true",
        help="skip preflight checks for ClickHouse binary and Python driver",
    )
    parser.add_argument(
        "--skip-run",
        action="store_true",
        help="validate existing artifacts without running benchmarks",
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


def run_scale_sweep(
    cmd: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    continue_on_failure: bool,
) -> None:
    print("+ " + " ".join(cmd), flush=True)
    completed = subprocess.run(cmd, cwd=cwd, env=env, check=False)
    if completed.returncode == 0:
        return
    if continue_on_failure:
        print(
            "benchmark sweep exited "
            f"{completed.returncode}; continuing to write not_ready status",
            file=sys.stderr,
        )
        return
    raise subprocess.CalledProcessError(completed.returncode, cmd)


def require_clickhouse(python: str) -> None:
    missing: list[str] = []
    if shutil.which("clickhouse") is None:
        missing.append("clickhouse binary")
    completed = subprocess.run(
        [python, "-c", "import clickhouse_driver"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if completed.returncode != 0:
        missing.append("Python module clickhouse_driver")
    if missing:
        raise RuntimeError(
            "ClickHouse comparison requested but missing: "
            + ", ".join(missing)
            + ". Install prerequisites or pass --required-engines without clickhouse."
        )


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
    if args.min_comparable_rows <= 0:
        print("--min-comparable-rows must be positive", file=sys.stderr)
        return 2

    out_dir = resolve_path(repo_root, args.out_dir)
    status_out = resolve_path(repo_root, args.status_out)
    ultrasqld = resolve_path(repo_root, args.ultrasqld)
    scale_script = repo_root / "benchmarks" / "run_scale_sweep.sh"
    validator = repo_root / "scripts" / "validate-benchmark-certification.py"

    try:
        if not args.skip_run:
            if "clickhouse" in {engine.strip() for engine in args.required_engines.split(",")}:
                if not args.skip_clickhouse_check:
                    require_clickhouse(args.python)
            if not args.skip_build:
                run_checked(
                    [
                        "cargo",
                        "build",
                        "--profile",
                        "release-ship",
                        "--package",
                        "ultrasql-server",
                        "--bin",
                        "ultrasqld",
                    ],
                    cwd=repo_root,
                )
            if not ultrasqld.is_file() or not os.access(ultrasqld, os.X_OK):
                raise RuntimeError(f"ultrasqld is not executable: {ultrasqld}")
            env = os.environ.copy()
            env["ULTRASQLD_BIN"] = str(ultrasqld)
            env["SCALE_SWEEP_OUT"] = str(out_dir)
            env["SCALE_SWEEP_STORAGE"] = args.storage
            run_scale_sweep(
                [str(scale_script), args.mode],
                cwd=repo_root,
                env=env,
                continue_on_failure=args.no_strict,
            )

        validate_cmd = [
            args.python,
            str(validator),
            "--artifact-dir",
            str(out_dir),
            "--commit",
            release_commit,
            "--out",
            str(status_out),
            "--required-engines",
            args.required_engines,
            "--required-storage-mode",
            args.storage,
            "--min-comparable-rows",
            str(args.min_comparable_rows),
        ]
        if not args.no_strict:
            validate_cmd.append("--strict")
        run_checked(validate_cmd, cwd=repo_root)
        status = load_status(status_out)
    except subprocess.CalledProcessError as err:
        return int(err.returncode)
    except RuntimeError as err:
        print(str(err), file=sys.stderr)
        return 2

    print(json.dumps(status, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
