#!/usr/bin/env python3
"""Validate external operator soak reports for release sign-off."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


GIT_COMMIT_RE = re.compile(r"^[0-9a-fA-F]{40}$")
SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
SCHEMA_POINTERS = [
    "operator.id_hash",
    "started_at",
    "ended_at",
    "errors.total",
    "errors.critical",
    "errors.high",
    "wal_replay_checks",
    "smoke_valid_report_count",
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
        "--commit",
        help="expected 40-hex release commit every release-valid report must cover",
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


def parse_time(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty RFC3339/ISO-8601 string")
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must include timezone")
    return parsed.astimezone(timezone.utc)


def parse_commit(value: Any) -> str:
    if not isinstance(value, str) or not GIT_COMMIT_RE.fullmatch(value.strip()):
        raise ValueError("must be a full 40-character hex git commit")
    return value.strip().lower()


def parse_text(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a non-empty string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} must be a non-empty string")
    return normalized


def parse_object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be a JSON object")
    return value


def parse_count(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def parse_positive_count(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def parse_number(value: Any, field: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be a number")
    number = float(value)
    if number < 0 or (positive and number <= 0):
        requirement = "positive" if positive else "non-negative"
        raise ValueError(f"{field} must be a {requirement} number")
    return number


def parse_sha256(value: Any, field: str) -> str:
    text = parse_text(value, field).lower()
    if not SHA256_RE.fullmatch(text):
        raise ValueError(f"{field} must be a 64-character hex SHA-256 digest")
    return text


def nested_text(report: dict[str, Any], object_field: str, text_field: str) -> str:
    obj = parse_object(report.get(object_field), object_field)
    return parse_text(obj.get(text_field), f"{object_field}.{text_field}")


def validate_named_checks(value: Any, field: str, errors: list[str]) -> int:
    if not isinstance(value, list) or not value:
        errors.append(f"{field} must be a non-empty list")
        return 0
    passed_count = 0
    for index, check in enumerate(value):
        if not isinstance(check, dict):
            errors.append(f"{field}[{index}] must be a JSON object")
            continue
        try:
            parse_text(check.get("name"), f"{field}[{index}].name")
        except ValueError as err:
            errors.append(str(err))
        passed = check.get("passed")
        if passed is not True:
            errors.append(f"{field}[{index}].passed must be true")
        else:
            passed_count += 1
        if "checksum" in check:
            try:
                parse_text(check.get("checksum"), f"{field}[{index}].checksum")
            except ValueError as err:
                errors.append(str(err))
    return passed_count


def validate_report(
    path: Path,
    min_days: float,
    expected_commit: str | None,
    now: datetime,
) -> dict[str, Any]:
    errors: list[str] = []
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except Exception as err:  # noqa: BLE001 - convert any parse/read error.
        return {
            "path": str(path),
            "operator_id": None,
            "commit": None,
            "mode": None,
            "valid": False,
            "smoke_valid": False,
            "duration_days": 0.0,
            "errors": [f"cannot parse JSON: {err}"],
        }
    if not isinstance(report, dict):
        return {
            "path": str(path),
            "operator_id": None,
            "commit": None,
            "mode": None,
            "valid": False,
            "smoke_valid": False,
            "duration_days": 0.0,
            "errors": ["report must be a JSON object"],
        }

    if report.get("schema_version") != 2:
        errors.append("schema_version must be 2")

    mode = report.get("mode")
    if mode not in {"30d", "smoke"}:
        errors.append("mode must be 30d or smoke")

    commit = None
    try:
        commit = parse_commit(report.get("commit"))
    except ValueError as err:
        errors.append(f"commit {err}")
    if commit is not None and expected_commit is not None and commit != expected_commit:
        errors.append(f"commit expected commit {expected_commit}, got {commit}")

    start = end = None
    try:
        start = parse_time(report.get("started_at"), "started_at")
    except Exception as err:  # noqa: BLE001 - message becomes validation error.
        errors.append(str(err))
    try:
        end = parse_time(report.get("ended_at"), "ended_at")
    except Exception as err:  # noqa: BLE001 - message becomes validation error.
        errors.append(str(err))

    duration_days = 0.0
    if start and end:
        duration_days = (end - start).total_seconds() / 86_400.0
        if end <= start:
            errors.append("ended_at must be after started_at")
        if end > now:
            errors.append("ended_at is in the future")
    try:
        recorded_duration = parse_number(report.get("duration_days"), "duration_days")
        if duration_days and abs(recorded_duration - duration_days) > 0.01:
            errors.append("duration_days must match started_at/ended_at within 0.01 days")
    except ValueError as err:
        errors.append(str(err))

    operator_id = report.get("operator")
    host_id = workload_id = None
    for object_field, required_texts in [
        ("host", ["id_hash", "cpu", "storage", "os"]),
        ("operator", ["id_hash"]),
        ("workload", ["id", "id_hash"]),
        ("db_binary", ["path", "sha256"]),
        ("config", ["ultrasqld_command", "data_dir"]),
    ]:
        try:
            obj = parse_object(report.get(object_field), object_field)
        except ValueError as err:
            errors.append(str(err))
            continue
        for text_field in required_texts:
            try:
                value = parse_text(obj.get(text_field), f"{object_field}.{text_field}")
                if object_field == "operator" and text_field == "id_hash":
                    operator_id = value
                elif object_field == "host" and text_field == "id_hash":
                    host_id = value
                elif object_field == "workload" and text_field == "id_hash":
                    workload_id = value
            except ValueError as err:
                errors.append(str(err))
        if object_field == "host":
            try:
                parse_number(obj.get("memory_bytes"), "host.memory_bytes", positive=True)
            except ValueError as err:
                errors.append(str(err))
        if object_field == "db_binary":
            try:
                parse_sha256(obj.get("sha256"), "db_binary.sha256")
            except ValueError as err:
                errors.append(str(err))

    try:
        parse_object(report.get("dataset_scale"), "dataset_scale")
    except ValueError as err:
        errors.append(str(err))
    try:
        parse_positive_count(report.get("concurrency"), "concurrency")
    except ValueError as err:
        errors.append(str(err))

    try:
        operations = parse_object(report.get("operations"), "operations")
        total_ops = parse_positive_count(operations.get("total"), "operations.total")
        for field in ["ddl", "read", "write", "transactions", "copy", "export_import"]:
            if field in operations:
                parse_count(operations.get(field), f"operations.{field}")
    except ValueError as err:
        errors.append(str(err))
        total_ops = 0

    try:
        latency = parse_object(report.get("latency_ms"), "latency_ms")
        p50 = parse_number(latency.get("p50"), "latency_ms.p50")
        p95 = parse_number(latency.get("p95"), "latency_ms.p95")
        p99 = parse_number(latency.get("p99"), "latency_ms.p99")
        if not p50 <= p95 <= p99:
            errors.append("latency_ms must satisfy p50 <= p95 <= p99")
    except ValueError as err:
        errors.append(str(err))

    try:
        parse_number(report.get("throughput_ops_per_sec"), "throughput_ops_per_sec")
    except ValueError as err:
        errors.append(str(err))

    try:
        error_counts = parse_object(report.get("errors"), "errors")
        total_errors = parse_count(error_counts.get("total"), "errors.total")
        for field in ["availability", "sql", "correctness", "corruption", "critical", "high"]:
            if field in error_counts:
                parse_count(error_counts.get(field), f"errors.{field}")
                if error_counts.get(field) != 0:
                    errors.append(f"errors.{field} must be zero")
        if total_errors != 0:
            errors.append("errors.total must be zero")
    except ValueError as err:
        errors.append(str(err))

    for field in ["restart_count", "crash_recovery_count"]:
        try:
            parse_count(report.get(field), field)
        except ValueError as err:
            errors.append(str(err))

    consistency_passed = validate_named_checks(
        report.get("consistency_checks"),
        "consistency_checks",
        errors,
    )
    wal_passed = validate_named_checks(
        report.get("wal_replay_checks"),
        "wal_replay_checks",
        errors,
    )

    final_verdict = report.get("final_verdict")
    if mode == "smoke":
        if final_verdict != "smoke_pass":
            errors.append("final_verdict must be smoke_pass for smoke reports")
    elif final_verdict != "pass":
        errors.append("final_verdict must be pass for release reports")

    for field in ["log_bundle_path", "signed_off_by"]:
        try:
            parse_text(report.get(field), field)
        except ValueError as err:
            errors.append(str(err))

    release_errors = list(errors)
    if mode == "smoke":
        release_errors.append("smoke mode is a non-ready development check")
    if duration_days < min_days:
        release_errors.append(f"duration {duration_days:.3f} days is below {min_days:g}")

    smoke_valid = (
        mode == "smoke"
        and not errors
        and total_ops > 0
        and consistency_passed > 0
        and wal_passed > 0
    )
    release_valid = (
        mode == "30d"
        and not errors
        and duration_days >= min_days
        and total_ops > 0
        and consistency_passed > 0
        and wal_passed > 0
    )

    return {
        "path": str(path),
        "operator_id": operator_id,
        "host_id": host_id,
        "workload_id": workload_id,
        "commit": commit if commit is not None else report.get("commit"),
        "mode": mode,
        "valid": release_valid,
        "smoke_valid": smoke_valid,
        "duration_days": round(duration_days, 6),
        "final_verdict": final_verdict,
        "operations_total": total_ops,
        "errors_total": report.get("errors", {}).get("total")
        if isinstance(report.get("errors"), dict)
        else None,
        "errors": [] if release_valid else release_errors,
    }


def main() -> int:
    args = parse_args()
    expected_commit = None
    if args.min_reports <= 0:
        print("--min-reports must be positive", file=sys.stderr)
        return 2
    if args.min_days <= 0:
        print("--min-days must be positive", file=sys.stderr)
        return 2
    if args.commit:
        try:
            expected_commit = parse_commit(args.commit)
        except ValueError as err:
            print(f"--commit {err}", file=sys.stderr)
            return 2
    try:
        now = parse_time(args.now, "--now") if args.now else datetime.now(timezone.utc)
    except Exception as err:  # noqa: BLE001 - CLI validation path.
        print(str(err), file=sys.stderr)
        return 2

    reports_dir = Path(args.reports_dir)
    paths = sorted(reports_dir.glob("*.json")) if reports_dir.exists() else []
    reports = [
        validate_report(path, args.min_days, expected_commit, now)
        for path in paths
    ]
    valid_reports = [report for report in reports if report["valid"]]
    smoke_valid_reports = [report for report in reports if report["smoke_valid"]]
    unique_operators = sorted({report["operator_id"] for report in valid_reports})
    valid_commits = sorted({report["commit"] for report in valid_reports})

    ready = (
        len(valid_reports) >= args.min_reports
        and len(unique_operators) >= args.min_reports
        and len(valid_commits) == 1
        and (expected_commit is None or valid_commits == [expected_commit])
    )
    status = "ready" if ready else "not_ready"
    reasons: list[str] = []
    if len(valid_reports) < args.min_reports:
        reasons.append(
            f"{len(valid_reports)} valid release report(s), need {args.min_reports}"
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
    if smoke_valid_reports:
        reasons.append(
            f"{len(smoke_valid_reports)} smoke report(s) accepted as non-ready dev checks"
        )

    doc = {
        "schema_version": 2,
        "status": status,
        "ready": ready,
        "release_commit": expected_commit,
        "validated_at_utc": now.isoformat().replace("+00:00", "Z"),
        "min_reports": args.min_reports,
        "min_days": args.min_days,
        "reports_dir": str(reports_dir),
        "report_count": len(reports),
        "valid_report_count": len(valid_reports),
        "smoke_valid_report_count": len(smoke_valid_reports),
        "independent_operator_count": len(unique_operators),
        "valid_release_commits": valid_commits,
        "reasons": reasons,
        "reports": reports,
    }

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(doc, indent=2, sort_keys=True))
    return 1 if args.strict and not ready else 0


if __name__ == "__main__":
    sys.exit(main())
