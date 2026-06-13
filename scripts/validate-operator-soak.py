#!/usr/bin/env python3
"""Validate 30-day external operator soak reports."""

from __future__ import annotations

import argparse
import json
import re
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
TEXT_FIELDS = [
    "operator_id",
    "host_cpu",
    "host_memory",
    "host_storage",
    "os",
    "ultrasqld_command",
    "workload",
    "data_dir",
    "ops_endpoint",
    "health_check_interval",
    "log_bundle_path",
    "signed_off_by",
]
GIT_COMMIT_RE = re.compile(r"^[0-9a-fA-F]{40}$")


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
        "--commit",
        help="expected 40-hex release commit every valid report must cover",
    )
    parser.add_argument(
        "--now",
        help="RFC3339 timestamp used as validation time; defaults to current UTC time",
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


def parse_positive_count(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def parse_text(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a non-empty string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} must be a non-empty string")
    return normalized


def parse_commit(value: Any) -> str:
    if not isinstance(value, str) or not GIT_COMMIT_RE.fullmatch(value.strip()):
        raise ValueError("must be a full 40-character hex git commit")
    return value.strip().lower()


def validate_report(
    path: Path,
    min_days: float,
    expected_commit: str | None,
    now: datetime,
) -> dict[str, Any]:
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

    normalized_text: dict[str, str] = {}
    for field in REQUIRED_FIELDS:
        if field not in report:
            errors.append(f"missing {field}")

    for field in TEXT_FIELDS:
        if field in report:
            try:
                normalized_text[field] = parse_text(report[field], field)
            except ValueError as err:
                errors.append(str(err))

    commit = None
    try:
        commit = parse_commit(report.get("commit"))
    except ValueError as err:
        errors.append(f"commit {err}")
    if commit is not None and expected_commit is not None and commit != expected_commit:
        errors.append(f"commit expected commit {expected_commit}, got {commit}")

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
        if end <= start:
            errors.append("end_time_utc must be after start_time_utc")
        if end > now:
            errors.append("end_time_utc is in the future")
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

    try:
        parse_positive_count(report.get("client_count"), "client_count")
    except ValueError as err:
        errors.append(str(err))

    if isinstance(report.get("failure_count"), int) and report["failure_count"] != 0:
        errors.append("failure_count must be zero for a continuous soak")

    for field in ["correctness_issue_count", "critical_issue_count", "high_issue_count"]:
        if isinstance(report.get(field), int) and report[field] != 0:
            errors.append(f"{field} must be zero for release sign-off")

    return {
        "path": str(path),
        "operator_id": normalized_text.get("operator_id", report.get("operator_id")),
        "commit": commit if commit is not None else report.get("commit"),
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
    expected_commit = None
    if args.commit:
        try:
            expected_commit = parse_commit(args.commit)
        except ValueError as err:
            print(f"--commit {err}", file=sys.stderr)
            return 2
    try:
        now = parse_time(args.now) if args.now else datetime.now(timezone.utc)
    except Exception as err:  # noqa: BLE001 - CLI validation path.
        print(f"--now {err}", file=sys.stderr)
        return 2
    reports_dir = Path(args.reports_dir)
    paths = sorted(reports_dir.glob("*.json")) if reports_dir.exists() else []
    reports = [
        validate_report(path, args.min_days, expected_commit, now)
        for path in paths
    ]
    valid_reports = [report for report in reports if report["valid"]]
    unique_operators = sorted({report["operator_id"] for report in valid_reports})
    valid_commits = sorted({report["commit"] for report in valid_reports})
    ready = (
        len(valid_reports) >= args.min_reports
        and len(unique_operators) >= args.min_reports
        and len(valid_commits) == 1
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
    if len(valid_commits) != 1:
        reasons.append(
            f"{len(valid_commits)} valid release commit(s), need exactly 1"
        )
    if expected_commit is not None and valid_commits != [expected_commit]:
        reasons.append(f"valid reports must cover expected commit {expected_commit}")
    if expected_commit is not None and any(
        report.get("commit") != expected_commit for report in reports
    ):
        reasons.append(f"one or more reports did not cover expected commit {expected_commit}")

    doc = {
        "schema_version": 1,
        "status": status,
        "ready": ready,
        "release_commit": expected_commit,
        "validated_at_utc": now.isoformat().replace("+00:00", "Z"),
        "min_reports": args.min_reports,
        "min_days": args.min_days,
        "reports_dir": str(reports_dir),
        "report_count": len(reports),
        "valid_report_count": len(valid_reports),
        "independent_operator_count": len(unique_operators),
        "valid_release_commits": valid_commits,
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
