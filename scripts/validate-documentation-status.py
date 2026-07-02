#!/usr/bin/env python3
"""Validate UltraSQL's documentation status audit."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


EXCLUDED_PREFIXES = (
    "target/",
    "site/",
    ".git/",
    "tests/driver_certification/node/node_modules/",
)

AUDIT_DOC = Path("docs/documentation-status-audit.md")
SCALE_SWEEP_JSON = Path("benchmarks/results/latest/scale-sweep/scale_sweep.json")
BENCHMARK_CERTIFICATION_MANIFEST = Path(
    "benchmarks/results/latest/benchmark_certification_manifest.json"
)

OLD_STATUS_RE = re.compile(r"\bpre[- ]?alpha\b|pre--alpha", re.IGNORECASE)
UNSUPPORTED_CLAIM_RE = re.compile(
    r"UltraSQL is production ready\."
    r"|UltraSQL is the best database[^.\n]*\.?"
    r"|UltraSQL beats every database[^.\n]*\.?",
    re.IGNORECASE,
)
LEDGER_ROW_RE = re.compile(r"^\|\s+`([^`]+)`\s+\|")


def rel(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def is_excluded(relative_path: str) -> bool:
    return relative_path.startswith(EXCLUDED_PREFIXES)


def first_party_markdown(root: Path) -> list[str]:
    # Git-tracked files are the source of truth: untracked scratch,
    # worktrees, and vendored checkouts on disk are not first-party docs.
    # Fall back to a filesystem walk only where git is unavailable
    # (e.g. the validator's own unit-test fixtures).
    tracked = git_tracked_markdown(root)
    if tracked is not None:
        return sorted(path for path in tracked if not is_excluded(path))
    files = []
    for path in root.rglob("*.md"):
        relative_path = rel(path, root)
        if not is_excluded(relative_path):
            files.append(relative_path)
    return sorted(files)


def git_tracked_markdown(root: Path) -> list[str] | None:
    import subprocess

    try:
        proc = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-z", "--", "*.md"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    entries = [
        entry.decode("utf-8")
        for entry in proc.stdout.split(b"\0")
        if entry
    ]
    return entries or None


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def ledger_files(audit_text: str) -> list[str]:
    files = []
    for line in audit_text.splitlines():
        match = LEDGER_ROW_RE.match(line)
        if match:
            files.append(match.group(1))
    return sorted(files)


def old_status_matches(root: Path, files: list[str]) -> list[str]:
    matches = []
    for relative_path in files:
        for line_number, line in enumerate(
            read_text(root / relative_path).splitlines(), start=1
        ):
            if OLD_STATUS_RE.search(line):
                matches.append(f"{relative_path}:{line_number}")
    return matches


def in_not_allowed_example(lines: list[str], index: int) -> bool:
    window = lines[max(0, index - 6) : index + 1]
    return any("not allowed:" in line.lower() for line in window)


def is_negated_claim_line(line: str) -> bool:
    lowered = line.lower()
    return any(
        marker in lowered
        for marker in (
            "does not support",
            "do not claim",
            "not claim",
            "without implying",
        )
    )


def unsupported_claim_matches(root: Path, files: list[str]) -> list[str]:
    matches = []
    for relative_path in files:
        lines = read_text(root / relative_path).splitlines()
        for index, line in enumerate(lines):
            match = UNSUPPORTED_CLAIM_RE.search(line)
            if (
                match
                and not in_not_allowed_example(lines, index)
                and not is_negated_claim_line(line)
            ):
                matches.append(f"{relative_path}:{index + 1}: {match.group(0)}")
    return matches


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def benchmark_summary(root: Path, errors: list[str]) -> dict:
    summary: dict[str, object] = {
        "scale_sweep_present": False,
        "scale_sweep_row_count": 0,
        "ultrasql_fastest_row_count": 0,
        "non_ultrasql_fastest_rows": [],
        "benchmark_certification_profile": None,
        "benchmark_certification_passed": None,
    }
    scale_path = root / SCALE_SWEEP_JSON
    if scale_path.exists():
        scale = load_json(scale_path)
        rows = scale.get("rows", [])
        non_ultrasql = [
            {
                "workload": row.get("workload"),
                "n_rows": row.get("n_rows"),
                "fastest_engine": row.get("fastest_engine"),
            }
            for row in rows
            if row.get("fastest_engine") != "ultrasql"
        ]
        summary["scale_sweep_present"] = True
        summary["scale_sweep_row_count"] = len(rows)
        summary["ultrasql_fastest_row_count"] = len(rows) - len(non_ultrasql)
        # Losses are legitimate measurements and are reported, never errored:
        # a docs gate that fails when UltraSQL is not fastest everywhere is an
        # incentive to rig benchmarks, not an honesty check.
        summary["non_ultrasql_fastest_rows"] = non_ultrasql
    else:
        errors.append(f"{SCALE_SWEEP_JSON.as_posix()} is missing")

    manifest_path = root / BENCHMARK_CERTIFICATION_MANIFEST
    if manifest_path.exists():
        manifest = load_json(manifest_path)
        profile = manifest.get("profile")
        passed = manifest.get("passed")
        summary["benchmark_certification_profile"] = profile
        summary["benchmark_certification_passed"] = passed
        if profile != "full" or passed is not True:
            require_smoke_qualification(root, errors)
    else:
        errors.append(f"{BENCHMARK_CERTIFICATION_MANIFEST.as_posix()} is missing")

    return summary


def require_smoke_qualification(root: Path, errors: list[str]) -> None:
    required_docs = [
        Path("docs/documentation-status-audit.md"),
        Path("docs/known-limitations.md"),
        Path("docs/production-readiness.md"),
    ]
    markers = (
        "not full release benchmark",
        "not full benchmark release",
        "does not mean the full release benchmark-certification",
    )
    for relative_path in required_docs:
        raw_text = (
            read_text(root / relative_path).lower()
            if (root / relative_path).exists()
            else ""
        )
        text = " ".join(raw_text.split())
        if not any(marker in text for marker in markers):
            errors.append(
                f"{relative_path.as_posix()} must say smoke benchmark evidence is not full release benchmark certification"
            )


def validate(root: Path) -> dict:
    errors: list[str] = []
    files = first_party_markdown(root)
    audit_path = root / AUDIT_DOC
    audit_text = read_text(audit_path) if audit_path.exists() else ""
    if not audit_text:
        errors.append(f"{AUDIT_DOC.as_posix()} is missing or empty")

    ledger = ledger_files(audit_text)
    ledger_missing = sorted(set(files) - set(ledger))
    ledger_extra = sorted(set(ledger) - set(files))
    if ledger_missing:
        errors.append("documentation status audit ledger is missing first-party Markdown files")
    if ledger_extra:
        errors.append("documentation status audit ledger references missing files")

    old_matches = old_status_matches(root, files)
    if old_matches:
        errors.append("stale lower-maturity status labels found")

    unsupported_matches = unsupported_claim_matches(root, files)
    if unsupported_matches:
        errors.append("unsupported universal production/best-database claims found")

    benchmark = benchmark_summary(root, errors)

    ready = not errors
    return {
        "schema_version": 1,
        "status": "ready" if ready else "not_ready",
        "ready": ready,
        "first_party_markdown_count": len(files),
        "ledger_file_count": len(ledger),
        "ledger_missing": ledger_missing,
        "ledger_extra": ledger_extra,
        "old_status_matches": old_matches,
        "unsupported_claim_matches": unsupported_matches,
        "benchmark": benchmark,
        "errors": errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate UltraSQL documentation status audit evidence."
    )
    parser.add_argument("--repo", default=".", help="repository root")
    parser.add_argument(
        "--out",
        default="benchmarks/results/latest/documentation_status_audit_status.json",
        help="status JSON output path",
    )
    args = parser.parse_args()

    root = Path(args.repo).resolve()
    status = validate(root)
    out = Path(args.out)
    if not out.is_absolute():
        out = root / out
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n")
    print(json.dumps(status, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
