#!/usr/bin/env python3
"""Fail when llvm-cov JSON reports a crate below line coverage threshold."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


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


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--min-lines", type=float, default=80.0)
    args = parser.parse_args(argv)

    report = json.loads(args.report.read_text(encoding="utf-8"))
    crates: dict[str, list[int]] = {}
    for file_entry in iter_files(report):
        crate = crate_for_file(str(file_entry.get("filename", "")), args.root)
        if crate is None:
            continue
        line_count, covered = line_counts(file_entry)
        if line_count == 0:
            continue
        totals = crates.setdefault(crate, [0, 0])
        totals[0] += line_count
        totals[1] += covered

    if not crates:
        print("coverage gate: no crate source files found in llvm-cov report", file=sys.stderr)
        return 2

    failed = []
    for crate, (line_count, covered) in sorted(crates.items()):
        percent = covered * 100.0 / line_count
        print(f"{crate}: {percent:.2f}% lines ({covered}/{line_count})")
        if percent < args.min_lines:
            failed.append((crate, percent))

    if failed:
        names = ", ".join(f"{crate}={percent:.2f}%" for crate, percent in failed)
        print(f"coverage gate: below {args.min_lines:.2f}% line coverage: {names}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
