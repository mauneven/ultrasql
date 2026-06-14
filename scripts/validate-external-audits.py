#!/usr/bin/env python3
"""Validate independent external audit reports for release sign-off."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REQUIRED_FIELDS = [
    "auditor_id",
    "auditor_org",
    "auditor_contact",
    "commit",
    "audit_type",
    "report_date_utc",
    "scope",
    "methodology",
    "report_uri",
    "critical_findings_open",
    "high_findings_open",
    "medium_findings_open",
    "low_findings_open",
    "signed_off_by",
    "signature",
]
TEXT_FIELDS = [
    "auditor_id",
    "auditor_org",
    "auditor_contact",
    "audit_type",
    "scope",
    "methodology",
    "report_uri",
    "signed_off_by",
    "signature",
]
COUNT_FIELDS = [
    "critical_findings_open",
    "high_findings_open",
    "medium_findings_open",
    "low_findings_open",
]
GIT_COMMIT_RE = re.compile(r"^[0-9a-fA-F]{40}$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reports-dir", default="external-audits")
    parser.add_argument("--min-reports", type=int, default=2)
    parser.add_argument(
        "--required-audit-types",
        default="security,correctness",
        help="comma-separated audit_type values required for release",
    )
    parser.add_argument(
        "--out",
        default="benchmarks/results/latest/external_audit_status.json",
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
        help="exit non-zero unless the external audit gate is ready",
    )
    return parser.parse_args()


def split_csv(value: str) -> list[str]:
    return sorted({part.strip() for part in value.split(",") if part.strip()})


def parse_time(value: Any) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("must be a non-empty RFC3339/ISO-8601 string")
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("must include timezone")
    return parsed.astimezone(timezone.utc)


def parse_text(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a non-empty string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} must be a non-empty string")
    return normalized


def parse_count(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def parse_commit(value: Any) -> str:
    if not isinstance(value, str) or not GIT_COMMIT_RE.fullmatch(value.strip()):
        raise ValueError("must be a full 40-character hex git commit")
    return value.strip().lower()


def validate_report(
    path: Path,
    required_audit_types: set[str],
    expected_commit: str | None,
    now: datetime,
) -> dict[str, Any]:
    errors: list[str] = []
    try:
        report = json.loads(path.read_text())
    except Exception as err:  # noqa: BLE001 - convert parse/read errors.
        return {
            "path": str(path),
            "auditor_id": None,
            "audit_type": None,
            "valid": False,
            "errors": [f"cannot parse JSON: {err}"],
        }

    if not isinstance(report, dict):
        return {
            "path": str(path),
            "auditor_id": None,
            "audit_type": None,
            "valid": False,
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

    audit_type = normalized_text.get("audit_type", report.get("audit_type"))
    if (
        isinstance(audit_type, str)
        and required_audit_types
        and audit_type not in required_audit_types
    ):
        errors.append(
            "audit_type must be one of "
            + ", ".join(sorted(required_audit_types))
        )

    commit = None
    try:
        commit = parse_commit(report.get("commit"))
    except ValueError as err:
        errors.append(f"commit {err}")
    if commit is not None and expected_commit is not None and commit != expected_commit:
        errors.append(f"commit expected commit {expected_commit}, got {commit}")

    try:
        report_date = parse_time(report.get("report_date_utc"))
        if report_date > now:
            errors.append("report_date_utc is in the future")
    except Exception as err:  # noqa: BLE001 - message becomes validation error.
        errors.append(f"report_date_utc {err}")

    counts: dict[str, Any] = {}
    for field in COUNT_FIELDS:
        try:
            counts[field] = parse_count(report.get(field), field)
        except ValueError as err:
            errors.append(str(err))

    for field in ["critical_findings_open", "high_findings_open"]:
        if isinstance(report.get(field), int) and report[field] != 0:
            errors.append(f"{field} must be zero for release sign-off")

    return {
        "path": str(path),
        "auditor_id": normalized_text.get("auditor_id", report.get("auditor_id")),
        "audit_type": audit_type,
        "commit": commit if commit is not None else report.get("commit"),
        "valid": not errors,
        "critical_findings_open": counts.get(
            "critical_findings_open", report.get("critical_findings_open")
        ),
        "high_findings_open": counts.get(
            "high_findings_open", report.get("high_findings_open")
        ),
        "errors": errors,
    }


def main() -> int:
    args = parse_args()
    required_audit_types = split_csv(args.required_audit_types)
    if args.min_reports <= 0:
        print("--min-reports must be positive", file=sys.stderr)
        return 2
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
        validate_report(path, set(required_audit_types), expected_commit, now)
        for path in paths
    ]
    valid_reports = [report for report in reports if report["valid"]]
    unique_auditors = sorted({report["auditor_id"] for report in valid_reports})
    covered_audit_types = sorted({report["audit_type"] for report in valid_reports})
    valid_commits = sorted({report["commit"] for report in valid_reports})
    missing_audit_types = [
        audit_type
        for audit_type in required_audit_types
        if audit_type not in covered_audit_types
    ]
    ready = (
        len(valid_reports) >= args.min_reports
        and len(unique_auditors) >= args.min_reports
        and not missing_audit_types
        and len(valid_commits) == 1
    )
    status = "ready" if ready else "not_ready"
    reasons: list[str] = []
    if len(valid_reports) < args.min_reports:
        reasons.append(
            f"{len(valid_reports)} valid report(s), need {args.min_reports}"
        )
    if len(unique_auditors) < args.min_reports:
        reasons.append(
            f"{len(unique_auditors)} independent auditor(s), need {args.min_reports}"
        )
    if missing_audit_types:
        reasons.append(
            "missing required audit type(s): " + ", ".join(missing_audit_types)
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
        "required_audit_types": required_audit_types,
        "reports_dir": str(reports_dir),
        "report_count": len(reports),
        "valid_report_count": len(valid_reports),
        "independent_auditor_count": len(unique_auditors),
        "covered_audit_types": covered_audit_types,
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
