#!/usr/bin/env python3
"""Fail when llvm-cov JSON reports a crate below line coverage threshold."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def percent(covered: int, line_count: int) -> float:
    return covered * 100.0 / line_count


def crate_for_file(filename: str, root: Path) -> str | None:
    path = Path(filename)
    try:
        relative = path.resolve().relative_to(root.resolve())
        parts = relative.parts
    except ValueError:
        parts = path.parts
        if "crates" in parts:
            parts = parts[parts.index("crates") :]

    if len(parts) >= 3 and parts[0] == "crates" and parts[2] == "src":
        return parts[1]
    return None


def line_counts(file_entry: dict) -> tuple[int, int]:
    lines = file_entry.get("summary", {}).get("lines", {})
    return int(lines.get("count", 0)), int(lines.get("covered", 0))


def iter_files(report: dict):
    for data_entry in report.get("data", []):
        yield from data_entry.get("files", [])


def crate_rows(
    report: dict,
    root: Path,
    min_lines: float,
    excluded_crates: set[str],
) -> list[dict[str, object]]:
    crates: dict[str, list[int]] = {}
    for file_entry in iter_files(report):
        crate = crate_for_file(str(file_entry.get("filename", "")), root)
        if crate is None:
            continue
        if crate in excluded_crates:
            continue
        line_count, covered = line_counts(file_entry)
        if line_count == 0:
            continue
        totals = crates.setdefault(crate, [0, 0])
        totals[0] += line_count
        totals[1] += covered

    return [
        {
            "crate": crate,
            "line_count": line_count,
            "covered": covered,
            "percent": percent(covered, line_count),
            "meets_threshold": percent(covered, line_count) >= min_lines,
        }
        for crate, (line_count, covered) in sorted(crates.items())
    ]


def write_summary_json(
    path: Path,
    min_lines: float,
    rows: list[dict[str, object]],
    excluded_crates: list[str],
) -> None:
    failed = [row for row in rows if not row["meets_threshold"]]
    payload = {
        "threshold": min_lines,
        "excluded_crates": excluded_crates,
        "failed": [row["crate"] for row in failed],
        "crates": rows,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_summary_md(
    path: Path,
    min_lines: float,
    rows: list[dict[str, object]],
    excluded_crates: list[str],
) -> None:
    failed_count = sum(1 for row in rows if not row["meets_threshold"])
    lines = [
        "# Per-crate Coverage",
        "",
        f"Threshold: {min_lines:.2f}% line coverage per crate.",
        f"Crates checked: {len(rows)}.",
        f"Crates below threshold: {failed_count}.",
        "Excluded crates: "
        + (", ".join(excluded_crates) if excluded_crates else "none")
        + ".",
        "",
        "| Crate | Lines | Covered | Coverage | Gate |",
        "|-------|------:|--------:|---------:|------|",
    ]
    for row in rows:
        gate = "pass" if row["meets_threshold"] else "fail"
        lines.append(
            "| {crate} | {line_count} | {covered} | {percent:.2f}% | {gate} |".format(
                **row, gate=gate
            )
        )
    lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--min-lines", type=float, default=80.0)
    parser.add_argument(
        "--exclude-crate",
        action="append",
        default=[],
        help=(
            "crate name to omit from the per-crate line gate; repeat for "
            "non-shipping harness crates with separate evidence"
        ),
    )
    parser.add_argument("--summary-json", type=Path)
    parser.add_argument("--summary-md", type=Path)
    args = parser.parse_args(argv)

    excluded_crates = sorted(set(args.exclude_crate))
    report = json.loads(args.report.read_text(encoding="utf-8"))
    rows = crate_rows(report, args.root, args.min_lines, set(excluded_crates))
    if not rows:
        print("coverage gate: no crate source files found in llvm-cov report", file=sys.stderr)
        return 2

    failed = []
    for row in rows:
        crate = row["crate"]
        line_count = row["line_count"]
        covered = row["covered"]
        pct = row["percent"]
        print(f"{crate}: {pct:.2f}% lines ({covered}/{line_count})")
        if not row["meets_threshold"]:
            failed.append((crate, pct))

    if args.summary_json is not None:
        write_summary_json(args.summary_json, args.min_lines, rows, excluded_crates)
    if args.summary_md is not None:
        write_summary_md(args.summary_md, args.min_lines, rows, excluded_crates)

    if failed:
        names = ", ".join(f"{crate}={percent:.2f}%" for crate, percent in failed)
        print(f"coverage gate: below {args.min_lines:.2f}% line coverage: {names}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
