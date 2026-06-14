#!/usr/bin/env python3
"""Validate production incident drill reports for release sign-off."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REQUIRED_FIELDS = [
    "drill_id",
    "commit",
    "drill_type",
    "run_time_utc",
    "environment",
    "scenario",
    "operator",
    "rto_target_seconds",
    "rto_actual_seconds",
    "rpo_target_seconds",
    "rpo_actual_seconds",
    "data_loss_confirmed",
    "correctness_verified",
    "monitoring_alerted",
    "postmortem_uri",
    "unresolved_sev0_count",
    "unresolved_sev1_count",
    "signed_off_by",
]
TEXT_FIELDS = [
    "drill_id",
    "drill_type",
    "environment",
    "scenario",
    "operator",
    "postmortem_uri",
    "signed_off_by",
]
COUNT_FIELDS = [
    "rto_target_seconds",
    "rto_actual_seconds",
    "rpo_target_seconds",
    "rpo_actual_seconds",
    "unresolved_sev0_count",
    "unresolved_sev1_count",
]
GIT_COMMIT_RE = re.compile(r"^[0-9a-fA-F]{40}$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reports-dir", default="incident-drills")
    parser.add_argument(
        "--required-drill-types",
        default="backup_restore,wal_recovery,disk_full",
        help="comma-separated drill_type values required for release",
    )
    parser.add_argument(
        "--out",
        default="benchmarks/results/latest/incident_drill_status.json",
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
        help="exit non-zero unless the incident drill gate is ready",
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


def parse_bool(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field} must be a boolean")
    return value


def parse_commit(value: Any) -> str:
    if not isinstance(value, str) or not GIT_COMMIT_RE.fullmatch(value.strip()):
        raise ValueError("must be a full 40-character hex git commit")
    return value.strip().lower()


def validate_report(
    path: Path,
    required_drill_types: set[str],
    expected_commit: str | None,
    now: datetime,
) -> dict[str, Any]:
    errors: list[str] = []
    try:
        report = json.loads(path.read_text())
    except Exception as err:  # noqa: BLE001 - convert parse/read errors.
        return {
            "path": str(path),
            "drill_id": None,
            "drill_type": None,
            "valid": False,
            "errors": [f"cannot parse JSON: {err}"],
        }

    if not isinstance(report, dict):
        return {
            "path": str(path),
            "drill_id": None,
            "drill_type": None,
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

    drill_type = normalized_text.get("drill_type", report.get("drill_type"))
    if (
        isinstance(drill_type, str)
        and required_drill_types
        and drill_type not in required_drill_types
    ):
        errors.append(
            "drill_type must be one of "
            + ", ".join(sorted(required_drill_types))
        )

    commit = None
    try:
        commit = parse_commit(report.get("commit"))
    except ValueError as err:
        errors.append(f"commit {err}")
    if commit is not None and expected_commit is not None and commit != expected_commit:
        errors.append(f"commit expected commit {expected_commit}, got {commit}")

    try:
        run_time = parse_time(report.get("run_time_utc"))
        if run_time > now:
            errors.append("run_time_utc is in the future")
    except Exception as err:  # noqa: BLE001 - message becomes validation error.
        errors.append(f"run_time_utc {err}")

    counts: dict[str, Any] = {}
    for field in COUNT_FIELDS:
        try:
            counts[field] = parse_count(report.get(field), field)
        except ValueError as err:
            errors.append(str(err))

    booleans: dict[str, Any] = {}
    for field in ["data_loss_confirmed", "correctness_verified", "monitoring_alerted"]:
        try:
            booleans[field] = parse_bool(report.get(field), field)
        except ValueError as err:
            errors.append(str(err))

    if (
        isinstance(report.get("rto_actual_seconds"), int)
        and isinstance(report.get("rto_target_seconds"), int)
        and report["rto_actual_seconds"] > report["rto_target_seconds"]
    ):
        errors.append("rto_actual_seconds exceeds rto_target_seconds")
    if (
        isinstance(report.get("rpo_actual_seconds"), int)
        and isinstance(report.get("rpo_target_seconds"), int)
        and report["rpo_actual_seconds"] > report["rpo_target_seconds"]
    ):
        errors.append("rpo_actual_seconds exceeds rpo_target_seconds")
    if report.get("data_loss_confirmed") is True:
        errors.append("data_loss_confirmed must be false")
    if report.get("correctness_verified") is False:
        errors.append("correctness_verified must be true")
    if report.get("monitoring_alerted") is False:
        errors.append("monitoring_alerted must be true")
    for field in ["unresolved_sev0_count", "unresolved_sev1_count"]:
        if isinstance(report.get(field), int) and report[field] != 0:
            errors.append(f"{field} must be zero for release sign-off")

    return {
        "path": str(path),
        "drill_id": normalized_text.get("drill_id", report.get("drill_id")),
        "drill_type": drill_type,
        "commit": commit if commit is not None else report.get("commit"),
        "valid": not errors,
        "rto_target_seconds": counts.get(
            "rto_target_seconds", report.get("rto_target_seconds")
        ),
        "rto_actual_seconds": counts.get(
            "rto_actual_seconds", report.get("rto_actual_seconds")
        ),
        "rpo_target_seconds": counts.get(
            "rpo_target_seconds", report.get("rpo_target_seconds")
        ),
        "rpo_actual_seconds": counts.get(
            "rpo_actual_seconds", report.get("rpo_actual_seconds")
        ),
        "data_loss_confirmed": booleans.get(
            "data_loss_confirmed", report.get("data_loss_confirmed")
        ),
        "unresolved_sev0_count": counts.get(
            "unresolved_sev0_count", report.get("unresolved_sev0_count")
        ),
        "unresolved_sev1_count": counts.get(
            "unresolved_sev1_count", report.get("unresolved_sev1_count")
        ),
        "errors": errors,
    }


def main() -> int:
    args = parse_args()
    required_drill_types = split_csv(args.required_drill_types)
    if not required_drill_types:
        print("--required-drill-types must list at least one drill type", file=sys.stderr)
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
        validate_report(path, set(required_drill_types), expected_commit, now)
        for path in paths
    ]
    valid_reports = [report for report in reports if report["valid"]]
    covered_drill_types = sorted({report["drill_type"] for report in valid_reports})
    valid_commits = sorted({report["commit"] for report in valid_reports})
    missing_drill_types = [
        drill_type
        for drill_type in required_drill_types
        if drill_type not in covered_drill_types
    ]
    ready = (
        len(valid_reports) >= len(required_drill_types)
        and not missing_drill_types
        and len(valid_commits) == 1
    )
    status = "ready" if ready else "not_ready"
    reasons: list[str] = []
    if len(valid_reports) < len(required_drill_types):
        reasons.append(
            f"{len(valid_reports)} valid drill report(s), need {len(required_drill_types)}"
        )
    if missing_drill_types:
        reasons.append(
            "missing required drill type(s): " + ", ".join(missing_drill_types)
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
        "required_drill_types": required_drill_types,
        "reports_dir": str(reports_dir),
        "report_count": len(reports),
        "valid_report_count": len(valid_reports),
        "covered_drill_types": covered_drill_types,
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
