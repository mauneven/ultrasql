#!/usr/bin/env python3
"""Validate driver compatibility certification for release sign-off.

Release command: scripts/validate-driver-compatibility.py --strict
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


DEFAULT_REQUIRED_DRIVERS = [
    "libpq",
    "psql meta-commands",
    "psycopg2",
    "psycopg3",
    "SQLAlchemy",
    "Django ORM",
    "Rails ActiveRecord",
    "node-postgres",
    "lib/pq",
    "pgx",
    "GORM",
    "JDBC PostgreSQL driver",
    "Hibernate ORM",
    "Npgsql",
    "Prisma",
    "Diesel",
    "GUI introspection probes",
    "Flyway",
    "Liquibase",
    "Alembic",
]
GIT_COMMIT_RE = re.compile(r"^[0-9a-fA-F]{40}$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--report",
        default="target/driver-certification.json",
        help="driver certification JSON emitted by tests/driver_certification",
    )
    parser.add_argument(
        "--required-drivers",
        default=",".join(DEFAULT_REQUIRED_DRIVERS),
        help="comma-separated driver names required for release",
    )
    parser.add_argument(
        "--out",
        default="benchmarks/results/latest/driver_compatibility_status.json",
        help="status JSON output path",
    )
    parser.add_argument(
        "--commit",
        help="expected 40-hex release commit the certification report must cover",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="exit non-zero unless the driver compatibility gate is ready",
    )
    return parser.parse_args()


def split_csv(value: str) -> list[str]:
    return sorted({part.strip() for part in value.split(",") if part.strip()})


def parse_commit(value: Any) -> str:
    if not isinstance(value, str) or not GIT_COMMIT_RE.fullmatch(value.strip()):
        raise ValueError("must be a full 40-character hex git commit")
    return value.strip().lower()


def non_empty_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


def validate_driver(entry: Any, required_drivers: set[str]) -> dict[str, Any]:
    errors: list[str] = []
    if not isinstance(entry, dict):
        return {
            "driver": None,
            "version": None,
            "status": None,
            "check_count": 0,
            "required": False,
            "valid": False,
            "errors": ["driver entry must be a JSON object"],
        }

    try:
        driver = non_empty_text(entry.get("driver"), "driver")
    except ValueError as err:
        driver = entry.get("driver")
        errors.append(str(err))

    try:
        version = non_empty_text(entry.get("version"), "version")
    except ValueError as err:
        version = entry.get("version")
        errors.append(str(err))

    status = entry.get("status")
    if status != "pass":
        errors.append("status must be pass")

    checks = entry.get("checks")
    if not isinstance(checks, list) or not checks:
        errors.append("checks must be a non-empty list")
        check_count = 0
    else:
        check_count = len(checks)
        for index, check in enumerate(checks):
            if not isinstance(check, str) or not check.strip():
                errors.append(f"checks[{index}] must be a non-empty string")

    required = isinstance(driver, str) and driver in required_drivers
    return {
        "driver": driver,
        "version": version,
        "status": status,
        "check_count": check_count,
        "required": required,
        "valid": not errors,
        "errors": errors,
    }


def load_report(path: Path) -> tuple[dict[str, Any] | None, list[str]]:
    if not path.exists():
        return None, [f"report missing: {path}"]
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except Exception as err:  # noqa: BLE001 - report parser surfaces errors.
        return None, [f"cannot parse JSON: {err}"]
    if not isinstance(report, dict):
        return None, ["report must be a JSON object"]
    return report, []


def build_status(
    report_path: Path,
    required_drivers: list[str],
    expected_commit: str | None,
) -> dict[str, Any]:
    errors: list[str] = []
    report, load_errors = load_report(report_path)
    errors.extend(load_errors)

    commit = None
    driver_entries: list[Any] = []
    if report is not None:
        if expected_commit is not None:
            try:
                commit = parse_commit(report.get("commit"))
            except ValueError as err:
                errors.append(f"commit {err}")
            if commit is not None and commit != expected_commit:
                errors.append(f"commit expected commit {expected_commit}, got {commit}")
        elif report.get("commit") is not None:
            try:
                commit = parse_commit(report.get("commit"))
            except ValueError as err:
                errors.append(f"commit {err}")

        drivers_value = report.get("drivers")
        if isinstance(drivers_value, list):
            driver_entries = drivers_value
        else:
            errors.append("drivers must be a list")

    required_set = set(required_drivers)
    drivers = [validate_driver(entry, required_set) for entry in driver_entries]
    seen: dict[str, int] = {}
    for driver in drivers:
        name = driver.get("driver")
        if isinstance(name, str):
            seen[name] = seen.get(name, 0) + 1
    for driver in drivers:
        name = driver.get("driver")
        if isinstance(name, str) and seen[name] > 1:
            driver["valid"] = False
            driver["errors"].append("driver appears more than once")

    present_required = sorted(
        {
            driver["driver"]
            for driver in drivers
            if driver.get("required") and isinstance(driver.get("driver"), str)
        }
    )
    passing_required = sorted(
        {
            driver["driver"]
            for driver in drivers
            if driver.get("required") and driver.get("valid")
        }
    )
    failed_required = sorted(set(present_required) - set(passing_required))
    missing_required = sorted(required_set - set(present_required))
    ready = not errors and not missing_required and not failed_required
    status = "ready" if ready else "not_ready"
    return {
        "status": status,
        "ready": ready,
        "report": str(report_path),
        "commit": commit,
        "expected_commit": expected_commit,
        "required_drivers": required_drivers,
        "required_driver_count": len(required_drivers),
        "passing_required_drivers": passing_required,
        "passing_required_driver_count": len(passing_required),
        "failed_required_drivers": failed_required,
        "missing_required_drivers": missing_required,
        "total_driver_count": len(drivers),
        "valid_driver_count": sum(1 for driver in drivers if driver.get("valid")),
        "drivers": drivers,
        "errors": errors,
    }


def main() -> int:
    args = parse_args()
    required_drivers = split_csv(args.required_drivers)
    if not required_drivers:
        print("--required-drivers must include at least one driver", file=sys.stderr)
        return 2
    expected_commit = None
    if args.commit:
        try:
            expected_commit = parse_commit(args.commit)
        except ValueError as err:
            print(f"--commit {err}", file=sys.stderr)
            return 2

    status = build_status(Path(args.report), required_drivers, expected_commit)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 1 if args.strict and not status["ready"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
