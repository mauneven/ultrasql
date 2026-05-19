#!/usr/bin/env python3
"""Audited SQLLogicTest corpus importer for UltraSQL.

The script copies selected `.slt` / `.test` files from a local, already-audited
checkout. It does not fetch from the network and it refuses to import without a
visible upstream license or copyright file.
"""

from __future__ import annotations

import argparse
import fnmatch
import shutil
from pathlib import Path


LICENSE_CANDIDATES = (
    "LICENSE",
    "LICENSE.txt",
    "COPYING",
    "COPYRIGHT",
    "README",
    "README.md",
)
NOTICE_CANDIDATES = (
    "NOTICE",
    "NOTICE.txt",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path, help="Audited upstream checkout")
    parser.add_argument("--commit", required=True, help="Upstream commit or immutable version")
    parser.add_argument(
        "--dest",
        default=Path("tests/slt/portable/imported"),
        type=Path,
        help="Destination for imported test files",
    )
    parser.add_argument(
        "--include",
        action="append",
        default=None,
        help="fnmatch pattern to include, relative to source",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print actions without copying")
    parser.add_argument(
        "--license-dest",
        default=Path("third_party/sqllogictest/LICENSE.upstream"),
        type=Path,
        help="Destination path for copied upstream license",
    )
    parser.add_argument(
        "--notice-dest",
        default=Path("third_party/sqllogictest/NOTICE.upstream"),
        type=Path,
        help="Destination path for copied upstream notice when present",
    )
    parser.add_argument(
        "--upstream-commit-file",
        default=Path("third_party/sqllogictest/upstream_commit.txt"),
        type=Path,
        help="Destination path for recorded upstream commit",
    )
    return parser.parse_args()


def find_license(source: Path) -> Path:
    for name in LICENSE_CANDIDATES:
        candidate = source / name
        if candidate.is_file():
            return candidate
    raise SystemExit(
        f"refusing import: no license/copyright file found in {source}. "
        "Audit provenance first."
    )


def find_notice(source: Path) -> Path | None:
    for name in NOTICE_CANDIDATES:
        candidate = source / name
        if candidate.is_file():
            return candidate
    return None


def included(path: Path, source: Path, patterns: list[str]) -> bool:
    rel = path.relative_to(source).as_posix()
    return any(fnmatch.fnmatch(rel, pattern) or fnmatch.fnmatch(path.name, pattern) for pattern in patterns)


def main() -> None:
    args = parse_args()
    include_patterns = args.include or ["*.slt", "*.test"]
    source = args.source.resolve()
    if not source.is_dir():
        raise SystemExit(f"source is not a directory: {source}")

    license_path = find_license(source)
    notice_path = find_notice(source)
    files = sorted(
        path
        for path in source.rglob("*")
        if path.is_file() and included(path, source, include_patterns)
    )
    if not files:
        raise SystemExit("no SQLLogicTest files matched include patterns")

    print(f"source: {source}")
    print(f"commit: {args.commit}")
    print(f"license: {license_path}")
    if notice_path is not None:
        print(f"notice: {notice_path}")
    print(f"files: {len(files)}")

    if args.dry_run:
        for path in files:
            print(f"would import {path.relative_to(source)}")
        return

    args.dest.mkdir(parents=True, exist_ok=True)
    manifest = args.dest / "IMPORT_MANIFEST.txt"
    with manifest.open("w", encoding="utf-8") as out:
        out.write(f"source={source}\n")
        out.write(f"commit={args.commit}\n")
        out.write(f"license={license_path.name}\n")
        if notice_path is not None:
            out.write(f"notice={notice_path.name}\n")
        for path in files:
            rel = path.relative_to(source)
            target = args.dest / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
            out.write(f"file={rel.as_posix()}\n")

    args.license_dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(license_path, args.license_dest)
    if notice_path is not None:
        args.notice_dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(notice_path, args.notice_dest)
    args.upstream_commit_file.parent.mkdir(parents=True, exist_ok=True)
    args.upstream_commit_file.write_text(f"{args.commit}\n", encoding="utf-8")
    print(f"imported {len(files)} files into {args.dest}")


if __name__ == "__main__":
    main()
