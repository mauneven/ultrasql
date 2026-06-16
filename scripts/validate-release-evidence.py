#!/usr/bin/env python3
"""Aggregate release evidence statuses into one fail-closed gate."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


GIT_COMMIT_RE = re.compile(r"^[0-9a-fA-F]{40}$")
MAX_REASONS_PER_GATE = 8


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--driver-status",
        default="benchmarks/results/latest/driver_compatibility_status.json",
        type=Path,
    )
    parser.add_argument(
        "--operator-soak-status",
        default="benchmarks/results/latest/operator_soak_status.json",
        type=Path,
    )
    parser.add_argument(
        "--external-audit-status",
        default="benchmarks/results/latest/external_audit_status.json",
        type=Path,
    )
    parser.add_argument(
        "--incident-drill-status",
        default="benchmarks/results/latest/incident_drill_status.json",
        type=Path,
    )
    parser.add_argument(
        "--benchmark-status",
        default="benchmarks/results/latest/benchmark_certification_status.json",
        type=Path,
    )
    parser.add_argument(
        "--ci-status",
        type=Path,
        help="optional CI status JSON; if supplied it must be ready and match --commit",
    )
    parser.add_argument(
        "--commit",
        help="expected 40-hex release commit every evidence status must cover",
    )
    parser.add_argument(
        "--now",
        help="RFC3339 timestamp used as validation time; defaults to current UTC time",
    )
    parser.add_argument(
        "--out",
        default="benchmarks/results/latest/release_gate_status.json",
        type=Path,
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="exit non-zero unless the aggregate release gate is ready",
    )
    return parser.parse_args()


def parse_commit(value: Any) -> str:
    if not isinstance(value, str) or not GIT_COMMIT_RE.fullmatch(value.strip()):
        raise ValueError("must be a full 40-character hex git commit")
    return value.strip().lower()


def parse_time(value: str | None) -> str:
    if value is None:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("must include timezone")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def load_status(path: Path) -> tuple[dict[str, Any] | None, list[str]]:
    if not path.exists():
        return None, [f"missing evidence file: {path}"]
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as err:  # noqa: BLE001 - validation reports parse/read errors.
        return None, [f"cannot parse evidence file {path}: {err}"]
    if not isinstance(value, dict):
        return None, [f"evidence file must contain a JSON object: {path}"]
    return value, []


def list_text(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if isinstance(item, (str, int, float, bool))]


def extract_reasons(status: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    reasons.extend(list_text(status.get("reasons")))
    reasons.extend(list_text(status.get("errors")))
    for field in [
        "missing_required_drivers",
        "failed_required_drivers",
        "missing_required_engine_rows",
    ]:
        value = status.get(field)
        if isinstance(value, list) and value:
            reasons.append(f"{field}: {json.dumps(value, sort_keys=True)}")
    if not reasons:
        reasons.append(f"status is {status.get('status', '<missing>')}, ready is {status.get('ready')}")
    if len(reasons) > MAX_REASONS_PER_GATE:
        shown = reasons[:MAX_REASONS_PER_GATE]
        shown.append(f"{len(reasons) - MAX_REASONS_PER_GATE} more issue(s) omitted")
        return shown
    return reasons


def status_commits(status: dict[str, Any]) -> list[tuple[str, Any]]:
    fields = ["commit", "release_commit", "expected_commit", "head_sha"]
    return [(field, status.get(field)) for field in fields if field in status]


def validate_gate(
    name: str,
    path: Path,
    *,
    expected_commit: str | None,
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    blockers: list[dict[str, str]] = []
    status, load_errors = load_status(path)
    if status is None:
        for error in load_errors:
            blockers.append({"gate": name, "reason": error})
        return {
            "name": name,
            "path": str(path),
            "ready": False,
            "status": "missing",
            "release_commit": None,
            "blocker_count": len(blockers),
        }, blockers

    gate_ready = status.get("ready") is True and status.get("status") == "ready"
    if not gate_ready:
        for reason in extract_reasons(status):
            blockers.append({"gate": name, "reason": reason})

    commit_values = status_commits(status)
    normalized_commits: list[tuple[str, str]] = []
    for field, value in commit_values:
        if value is None:
            continue
        try:
            normalized_commits.append((field, parse_commit(value)))
        except ValueError as err:
            blockers.append({"gate": name, "reason": f"{field} {err}"})

    if expected_commit is not None:
        evidence_commits = [
            (field, commit)
            for field, commit in normalized_commits
            if field in {"commit", "release_commit", "head_sha"}
        ]
        if not evidence_commits:
            blockers.append({"gate": name, "reason": "no release commit recorded"})
        for field, commit in evidence_commits:
            if commit != expected_commit:
                blockers.append(
                    {
                        "gate": name,
                        "reason": f"{field} expected commit {expected_commit}, got {commit}",
                    }
                )
        for field, commit in normalized_commits:
            if field == "expected_commit" and commit != expected_commit:
                blockers.append(
                    {
                        "gate": name,
                        "reason": f"expected_commit expected commit {expected_commit}, got {commit}",
                    }
                )

    release_commit = None
    for field, commit in normalized_commits:
        if field in {"release_commit", "commit", "head_sha"}:
            release_commit = commit
            break

    return {
        "name": name,
        "path": str(path),
        "ready": gate_ready and not blockers,
        "status": status.get("status"),
        "release_commit": release_commit,
        "blocker_count": len(blockers),
    }, blockers


def build_status(
    gates: list[tuple[str, Path]],
    *,
    expected_commit: str | None,
    validated_at: str,
) -> dict[str, Any]:
    gate_summaries: list[dict[str, Any]] = []
    blockers: list[dict[str, str]] = []
    for name, path in gates:
        gate_summary, gate_blockers = validate_gate(
            name,
            path,
            expected_commit=expected_commit,
        )
        gate_summaries.append(gate_summary)
        blockers.extend(gate_blockers)

    ready = not blockers and all(gate["ready"] for gate in gate_summaries)
    status = "ready" if ready else "not_ready"
    summary = (
        f"ready: all {len(gate_summaries)} gate(s) passed"
        if ready
        else f"not_ready: {len(blockers)} blocker(s) across "
        f"{len({blocker['gate'] for blocker in blockers})} gate(s)"
    )
    return {
        "schema_version": 1,
        "status": status,
        "ready": ready,
        "validated_at_utc": validated_at,
        "expected_commit": expected_commit,
        "gates": gate_summaries,
        "blockers": blockers,
        "summary": summary,
        "policy": "Missing, malformed, not_ready, or stale release evidence fails closed.",
    }


def main() -> int:
    args = parse_args()
    try:
        expected_commit = parse_commit(args.commit) if args.commit else None
    except ValueError as err:
        print(f"--commit {err}", file=sys.stderr)
        return 2
    try:
        validated_at = parse_time(args.now)
    except Exception as err:  # noqa: BLE001 - CLI validation path.
        print(f"--now {err}", file=sys.stderr)
        return 2

    gates = [
        ("driver_compatibility", args.driver_status),
        ("operator_soak", args.operator_soak_status),
        ("external_audit", args.external_audit_status),
        ("incident_drill", args.incident_drill_status),
        ("benchmark", args.benchmark_status),
    ]
    if args.ci_status is not None:
        gates.append(("ci", args.ci_status))

    status = build_status(gates, expected_commit=expected_commit, validated_at=validated_at)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n")
    print(status["summary"])
    print(json.dumps(status, indent=2, sort_keys=True))
    if args.strict and not status["ready"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
