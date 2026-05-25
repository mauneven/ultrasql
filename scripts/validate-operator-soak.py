#!/usr/bin/env python3
"""Validate 30-day external operator soak reports."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REQUIRED_FIELDS = [
    "operator_id",
    "commit",
    "start_time_utc",
    "end_time_utc",
    "host_cpu",
    "host_memory",
    "host_storage",
    "os",
    "ultrasqld_command",
    "workload",
    "client_count",
    "data_dir",
    "ops_endpoint",
    "health_check_interval",
    "failure_count",
    "correctness_issue_count",
    "critical_issue_count",
    "high_issue_count",
    "log_bundle_path",
    "signed_off_by",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reports-dir", default="operator-reports")
    parser.add_argument("--min-reports", type=int, default=3)
    parser.add_argument("--min-days", type=float, default=30.0)
    parser.add_argument(
        "--out",
        default="benchmarks/results/latest/operator_soak_status.json",
        help="status JSON output path",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="exit non-zero unless the operator soak gate is ready",
    )
    return parser.parse_args()


def parse_time(value: Any) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("must be a non-empty RFC3339/ISO-8601 string")
    normalized = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        raise ValueError("must include timezone")
    return parsed.astimezone(timezone.utc)


def parse_count(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def validate_report(path: Path, min_days: float) -> dict[str, Any]:
    errors: list[str] = []
    try:
        report = json.loads(path.read_text())
    except Exception as err:  # noqa: BLE001 - convert any parse/read error.
        return {
            "path": str(path),
            "operator_id": None,
            "valid": False,
            "duration_days": 0.0,
            "errors": [f"cannot parse JSON: {err}"],
        }

    if not isinstance(report, dict):
        return {
            "path": str(path),
            "operator_id": None,
            "valid": False,
            "duration_days": 0.0,
            "errors": ["report must be a JSON object"],
        }

    for field in REQUIRED_FIELDS:
        if field not in report:
            errors.append(f"missing {field}")
        elif isinstance(report[field], str) and not report[field].strip():
            errors.append(f"{field} must be non-empty")

    start = end = None
    try:
        start = parse_time(report.get("start_time_utc"))
    except Exception as err:  # noqa: BLE001 - message becomes validation error.
        errors.append(f"start_time_utc {err}")
    try:
        end = parse_time(report.get("end_time_utc"))
    except Exception as err:  # noqa: BLE001 - message becomes validation error.
        errors.append(f"end_time_utc {err}")

    duration_days = 0.0
    if start and end:
        duration_days = (end - start).total_seconds() / 86_400.0
        if duration_days < min_days:
            errors.append(f"duration {duration_days:.3f} days is below {min_days:g}")

    for field in [
        "failure_count",
        "correctness_issue_count",
        "critical_issue_count",
        "high_issue_count",
    ]:
        try:
            parse_count(report.get(field), field)
        except ValueError as err:
            errors.append(str(err))

    for field in ["correctness_issue_count", "critical_issue_count", "high_issue_count"]:
        if isinstance(report.get(field), int) and report[field] != 0:
            errors.append(f"{field} must be zero for release sign-off")

    return {
        "path": str(path),
        "operator_id": report.get("operator_id"),
        "commit": report.get("commit"),
        "valid": not errors,
        "duration_days": round(duration_days, 6),
        "failure_count": report.get("failure_count"),
        "correctness_issue_count": report.get("correctness_issue_count"),
        "critical_issue_count": report.get("critical_issue_count"),
        "high_issue_count": report.get("high_issue_count"),
        "errors": errors,
    }


def main() -> int:
    args = parse_args()
    reports_dir = Path(args.reports_dir)
    paths = sorted(reports_dir.glob("*.json")) if reports_dir.exists() else []
    reports = [validate_report(path, args.min_days) for path in paths]
    valid_reports = [report for report in reports if report["valid"]]
    unique_operators = sorted({report["operator_id"] for report in valid_reports})
    ready = (
        len(valid_reports) >= args.min_reports
        and len(unique_operators) >= args.min_reports
    )
    status = "ready" if ready else "not_ready"
    reasons: list[str] = []
    if len(valid_reports) < args.min_reports:
        reasons.append(
            f"{len(valid_reports)} valid report(s), need {args.min_reports}"
        )
    if len(unique_operators) < args.min_reports:
        reasons.append(
            f"{len(unique_operators)} independent operator(s), need {args.min_reports}"
        )

    doc = {
        "schema_version": 1,
        "status": status,
        "ready": ready,
        "min_reports": args.min_reports,
        "min_days": args.min_days,
        "reports_dir": str(reports_dir),
        "report_count": len(reports),
        "valid_report_count": len(valid_reports),
        "independent_operator_count": len(unique_operators),
        "reasons": reasons,
        "reports": reports,
    }

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
    print(json.dumps(doc, indent=2, sort_keys=True))
    return 1 if args.strict and not ready else 0


if __name__ == "__main__":
    sys.exit(main())
