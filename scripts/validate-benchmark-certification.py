#!/usr/bin/env python3
"""Validate release-artifact DB-vs-DB benchmark certification evidence."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_REQUIRED_ENGINES = ["ultrasql", "duckdb", "clickhouse", "sqlite3", "postgres"]
ENGINE_VERSION_KEYS = {
    "ultrasql": "ultrasql",
    "duckdb": "duckdb",
    "clickhouse": "clickhouse",
    "sqlite3": "sqlite",
    "postgres": "postgres",
}
GIT_COMMIT_RE = re.compile(r"^[0-9a-fA-F]{40}$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--artifact-dir",
        default="benchmarks/results/latest/scale-sweep",
        type=Path,
        help="scale-sweep artifact directory",
    )
    parser.add_argument(
        "--required-engines",
        default=",".join(DEFAULT_REQUIRED_ENGINES),
        help="comma-separated engines required in every comparable row",
    )
    parser.add_argument(
        "--required-storage-mode",
        default="data-dir",
        choices=["data-dir", "memory", "any"],
        help="required UltraSQL storage mode for release certification",
    )
    parser.add_argument(
        "--min-comparable-rows",
        default=24,
        type=int,
        help="minimum fully comparable rows required for release certification",
    )
    parser.add_argument(
        "--commit",
        help="expected 40-hex release commit the benchmark artifact must cover",
    )
    parser.add_argument(
        "--now",
        help="RFC3339 timestamp used as validation time; defaults to current UTC time",
    )
    parser.add_argument(
        "--out",
        default="benchmarks/results/latest/benchmark_certification_status.json",
        type=Path,
        help="status JSON output path",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="exit non-zero unless benchmark certification is ready",
    )
    return parser.parse_args()


def split_csv(value: str) -> list[str]:
    return sorted({part.strip() for part in value.split(",") if part.strip()})


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


def load_json(path: Path) -> tuple[Any | None, list[str]]:
    try:
        return json.loads(path.read_text(encoding="utf-8")), []
    except Exception as err:  # noqa: BLE001 - validation reports parse/read errors.
        return None, [f"cannot parse {path}: {err}"]


def require_text(
    doc: dict[str, Any], field: str, errors: list[str], label: str | None = None
) -> str | None:
    value = doc.get(field)
    name = label or field
    if not isinstance(value, str) or not value.strip():
        errors.append(f"{name} must be a non-empty string")
        return None
    return value.strip()


def require_positive_int(
    doc: dict[str, Any], field: str, errors: list[str], label: str | None = None
) -> int | None:
    value = doc.get(field)
    name = label or field
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        errors.append(f"{name} must be a positive integer")
        return None
    return value


def require_positive_number(doc: dict[str, Any], field: str, errors: list[str]) -> float | None:
    value = doc.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or float(value) <= 0.0:
        errors.append(f"{field} must be a positive number")
        return None
    return float(value)


def canonical_engine(engine: str) -> str:
    if engine == "postgres17":
        return "postgres"
    if engine == "sqlite":
        return "sqlite3"
    return engine


def resolve_artifact_path(artifact_dir: Path, raw_dir: Path, text: Any) -> Path | None:
    if not isinstance(text, str) or not text.strip():
        return None
    path = Path(text)
    if path.is_absolute() or path.exists():
        return path
    candidate = artifact_dir / path
    if candidate.exists():
        return candidate
    candidate = raw_dir / path.name
    if candidate.exists():
        return candidate
    return path


def validate_manifest(
    manifest: Any,
    *,
    expected_commit: str | None,
    required_engines: list[str],
    required_storage_mode: str,
) -> tuple[str | None, str | None, list[str]]:
    errors: list[str] = []
    if not isinstance(manifest, dict):
        return None, None, ["scale_sweep_manifest.json must be a JSON object"]

    if manifest.get("schema_version") != 1:
        errors.append("manifest schema_version must be 1")
    for field in ["mode", "ultrasql_version", "ultrasql_install_source", "methodology"]:
        require_text(manifest, field, errors)
    for field in ["iters", "warmup"]:
        require_positive_int(manifest, field, errors)
    if not isinstance(manifest.get("rows"), list) or not manifest["rows"]:
        errors.append("rows must be a non-empty list")

    storage_mode = manifest.get("ultrasql_storage_mode")
    if required_storage_mode != "any" and storage_mode != required_storage_mode:
        errors.append(
            f"ultrasql_storage_mode expected {required_storage_mode}, got {storage_mode}"
        )

    host = manifest.get("host")
    release_commit = None
    if not isinstance(host, dict):
        errors.append("host must be a JSON object")
    else:
        for field in ["hostname", "os", "machine", "cpu_model", "rustc"]:
            require_text(host, field, errors, label=f"host.{field}")
        for field in ["logical_cpus", "memory_bytes"]:
            require_positive_int(host, field, errors, label=f"host.{field}")
        try:
            release_commit = parse_commit(host.get("git_commit"))
        except ValueError as err:
            errors.append(f"host.git_commit {err}")
        if release_commit is not None and expected_commit is not None and release_commit != expected_commit:
            errors.append(
                f"manifest host.git_commit expected commit {expected_commit}, got {release_commit}"
            )

    engine_versions = manifest.get("engine_versions")
    if not isinstance(engine_versions, dict):
        errors.append("engine_versions must be a JSON object")
    else:
        for engine in required_engines:
            version_key = ENGINE_VERSION_KEYS.get(engine, engine)
            version = engine_versions.get(version_key)
            if not isinstance(version, str) or not version.strip():
                errors.append(f"engine_versions.{version_key} must be recorded")

    return release_commit, storage_mode if isinstance(storage_mode, str) else None, errors


def validate_raw_file(
    path: Path, *, required_storage_mode: str
) -> tuple[dict[str, Any] | None, list[str]]:
    raw, errors = load_json(path)
    if errors:
        return None, errors
    if not isinstance(raw, dict):
        return None, [f"{path}: raw artifact must be a JSON object"]
    local_errors: list[str] = []
    if raw.get("schema_version") != 1:
        local_errors.append(f"{path}: schema_version must be 1")
    status = raw.get("status")
    if status not in {"measured", "not_available"}:
        local_errors.append(f"{path}: status must be measured or not_available")
    require_text(raw, "engine", local_errors)
    require_text(raw, "workload", local_errors)
    require_positive_int(raw, "n_rows", local_errors)
    engine = canonical_engine(str(raw.get("engine"))) if isinstance(raw.get("engine"), str) else None
    if status == "measured":
        require_positive_number(raw, "median_us", local_errors)
        require_positive_int(raw, "samples", local_errors)
        iterations = raw.get("iterations_us")
        if not isinstance(iterations, list) or not iterations:
            local_errors.append(f"{path}: iterations_us must be a non-empty list")
        storage_mode = require_text(
            raw,
            "storage_mode",
            local_errors,
            label=f"{engine}: raw storage_mode" if engine else "raw storage_mode",
        )
        durability_mode = require_text(
            raw,
            "durability_mode",
            local_errors,
            label=f"{engine}: raw durability_mode" if engine else "raw durability_mode",
        )
        if required_storage_mode == "data-dir":
            if storage_mode is not None and storage_mode != "data-dir":
                local_errors.append(
                    f"{engine}: raw storage_mode expected data-dir, got {storage_mode}"
                )
            if durability_mode is not None and durability_mode != "durable":
                local_errors.append(
                    f"{engine}: raw durability_mode expected durable, got {durability_mode}"
                )
    if status == "not_available":
        require_text(raw, "reason", local_errors)
    if local_errors:
        return None, local_errors
    return raw, []


def measured_median(entry: Any) -> float | None:
    if not isinstance(entry, dict):
        return None
    value = entry.get("median_us")
    if isinstance(value, bool) or not isinstance(value, (int, float)) or float(value) <= 0.0:
        return None
    return float(value)


def validate_rendered_rows(
    rendered: Any,
    *,
    artifact_dir: Path,
    raw_dir: Path,
    required_engines: list[str],
    required_storage_mode: str,
    min_comparable_rows: int,
) -> tuple[list[dict[str, Any]], int, int, int, list[dict[str, Any]], list[str]]:
    errors: list[str] = []
    if not isinstance(rendered, dict):
        return [], 0, 0, 0, [], ["scale_sweep.json must be a JSON object"]
    if rendered.get("schema_version") != 1:
        errors.append("scale_sweep.json schema_version must be 1")
    rows = rendered.get("rows")
    if not isinstance(rows, list) or not rows:
        return [], 0, 0, 0, [], errors + ["scale_sweep.json rows must be a non-empty list"]

    row_summaries: list[dict[str, Any]] = []
    missing_required_rows: list[dict[str, Any]] = []
    comparable_count = 0
    ultrasql_fastest_count = 0
    total_rendered = 0

    for index, row in enumerate(rows):
        total_rendered += 1
        if not isinstance(row, dict):
            errors.append(f"rows[{index}] must be a JSON object")
            continue
        workload = row.get("workload")
        n_rows = row.get("n_rows")
        if not isinstance(workload, str) or not workload.strip():
            errors.append(f"rows[{index}].workload must be a non-empty string")
            workload = f"<row-{index}>"
        if isinstance(n_rows, bool) or not isinstance(n_rows, int) or n_rows <= 0:
            errors.append(f"rows[{index}].n_rows must be a positive integer")
            n_rows = 0
        engines = row.get("engines")
        if not isinstance(engines, dict):
            errors.append(f"rows[{index}].engines must be a JSON object")
            continue

        normalized_engines = {
            canonical_engine(str(engine)): entry for engine, entry in engines.items()
        }
        measured: dict[str, float] = {}
        for engine, entry in normalized_engines.items():
            median = measured_median(entry)
            if median is not None:
                measured[engine] = median
                raw_path = resolve_artifact_path(artifact_dir, raw_dir, entry.get("path"))
                if raw_path is None:
                    errors.append(f"{workload} rows={n_rows} {engine}: missing raw path")
                    continue
                raw, raw_errors = validate_raw_file(
                    raw_path, required_storage_mode=required_storage_mode
                )
                errors.extend(raw_errors)
                if raw is None:
                    continue
                raw_engine = canonical_engine(str(raw.get("engine")))
                if raw_engine != engine:
                    errors.append(
                        f"{workload} rows={n_rows} {engine}: raw engine is {raw_engine}"
                    )
                if raw.get("workload") != entry.get("workload"):
                    errors.append(
                        f"{workload} rows={n_rows} {engine}: raw workload mismatch"
                    )
                if raw.get("n_rows") != n_rows:
                    errors.append(f"{workload} rows={n_rows} {engine}: raw n_rows mismatch")
                if abs(float(raw.get("median_us")) - median) > 0.000001:
                    errors.append(
                        f"{workload} rows={n_rows} {engine}: raw median_us mismatch"
                    )

        missing = [engine for engine in required_engines if engine not in measured]
        if missing:
            missing_required_rows.append(
                {
                    "workload": workload,
                    "n_rows": n_rows,
                    "missing_engines": missing,
                }
            )
        else:
            comparable_count += 1
            fastest_median = min(measured.values())
            fastest_engines = sorted(
                engine for engine, median in measured.items() if median == fastest_median
            )
            rendered_fastest = canonical_engine(str(row.get("fastest_engine")))
            rendered_fastest_median = measured_median({"median_us": row.get("fastest_median_us")})
            if rendered_fastest not in fastest_engines or rendered_fastest_median != fastest_median:
                errors.append(
                    f"{workload} rows={n_rows}: rendered fastest_engine must match raw medians"
                )
            if "ultrasql" in fastest_engines:
                ultrasql_fastest_count += 1

        if workload == "mixed_correctness":
            answer_hash = row.get("answer_sha256")
            if row.get("correctness_status") != "verified":
                errors.append("mixed_correctness must have correctness_status=verified")
            if not isinstance(answer_hash, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", answer_hash):
                errors.append("mixed_correctness must have a 64-hex answer_sha256")

        row_summaries.append(
            {
                "workload": workload,
                "n_rows": n_rows,
                "measured_engines": sorted(measured),
                "fastest_engine": row.get("fastest_engine"),
            }
        )

    if comparable_count < min_comparable_rows:
        errors.append(
            f"comparable_row_count {comparable_count} below minimum {min_comparable_rows}"
        )
    if ultrasql_fastest_count != comparable_count:
        errors.append(
            "UltraSQL must be fastest for every comparable row in this release artifact"
        )

    return (
        row_summaries,
        total_rendered,
        comparable_count,
        ultrasql_fastest_count,
        missing_required_rows,
        errors,
    )


def build_status(
    artifact_dir: Path,
    *,
    expected_commit: str | None,
    required_engines: list[str],
    required_storage_mode: str,
    min_comparable_rows: int,
    validated_at: str,
) -> dict[str, Any]:
    errors: list[str] = []
    if expected_commit is None:
        errors.append("expected release commit is required")

    manifest_path = artifact_dir / "scale_sweep_manifest.json"
    rendered_path = artifact_dir / "scale_sweep.json"
    raw_dir = artifact_dir / "raw"
    if not raw_dir.is_dir():
        errors.append(f"raw dir missing: {raw_dir}")

    manifest, manifest_load_errors = load_json(manifest_path)
    rendered, rendered_load_errors = load_json(rendered_path)
    errors.extend(manifest_load_errors)
    errors.extend(rendered_load_errors)

    release_commit, storage_mode, manifest_errors = validate_manifest(
        manifest,
        expected_commit=expected_commit,
        required_engines=required_engines,
        required_storage_mode=required_storage_mode,
    )
    errors.extend(manifest_errors)

    (
        rows,
        total_rendered,
        comparable_count,
        ultrasql_fastest_count,
        missing_required_rows,
        rendered_errors,
    ) = validate_rendered_rows(
        rendered,
        artifact_dir=artifact_dir,
        raw_dir=raw_dir,
        required_engines=required_engines,
        required_storage_mode=required_storage_mode,
        min_comparable_rows=min_comparable_rows,
    )
    errors.extend(rendered_errors)

    ready = not errors and not missing_required_rows
    reasons = []
    if not ready:
        if errors:
            reasons.extend(errors)
        for row in missing_required_rows:
            reasons.append(
                "{workload} rows={n_rows} missing required engines: {engines}".format(
                    workload=row["workload"],
                    n_rows=row["n_rows"],
                    engines=", ".join(row["missing_engines"]),
                )
            )

    return {
        "schema_version": 1,
        "status": "ready" if ready else "not_ready",
        "ready": ready,
        "validated_at_utc": validated_at,
        "artifact_dir": str(artifact_dir),
        "manifest": str(manifest_path),
        "rendered_json": str(rendered_path),
        "raw_dir": str(raw_dir),
        "release_commit": release_commit,
        "expected_commit": expected_commit,
        "ultrasql_storage_mode": storage_mode,
        "required_storage_mode": required_storage_mode,
        "required_engines": required_engines,
        "min_comparable_rows": min_comparable_rows,
        "total_rendered_row_count": total_rendered,
        "comparable_row_count": comparable_count,
        "ultrasql_fastest_comparable_row_count": ultrasql_fastest_count,
        "missing_required_engine_rows": missing_required_rows,
        "rows": rows,
        "errors": errors,
        "reasons": reasons,
        "policy": (
            "Validation checks only raw measured artifacts and rendered rows; "
            "missing engines and stale commits are not release-ready evidence."
        ),
    }


def main() -> int:
    args = parse_args()
    if args.min_comparable_rows <= 0:
        print("--min-comparable-rows must be positive", file=sys.stderr)
        return 2
    required_engines = split_csv(args.required_engines)
    if "ultrasql" not in required_engines:
        print("--required-engines must include ultrasql", file=sys.stderr)
        return 2
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

    status = build_status(
        args.artifact_dir,
        expected_commit=expected_commit,
        required_engines=required_engines,
        required_storage_mode=args.required_storage_mode,
        min_comparable_rows=args.min_comparable_rows,
        validated_at=validated_at,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n")
    print(json.dumps(status, indent=2, sort_keys=True))
    if args.strict and not status["ready"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
