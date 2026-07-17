#!/usr/bin/env python3
"""Validate release-artifact DB-vs-DB benchmark certification evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import statistics
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
MEDIAN_SERIALIZATION_ABS_TOLERANCE_US = 0.0005


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
    def reject_non_finite(value: str) -> None:
        raise ValueError(f"non-finite JSON number {value} is not allowed")

    try:
        return (
            json.loads(
                path.read_text(encoding="utf-8"),
                parse_constant=reject_non_finite,
            ),
            [],
        )
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
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) <= 0.0
    ):
        errors.append(f"{field} must be a positive number")
        return None
    return float(value)


def canonical_engine(engine: str) -> str:
    if engine in {"postgres17", "postgresql"}:
        return "postgres"
    if engine == "sqlite":
        return "sqlite3"
    return engine


def canonical_answer_sha256(answer: Any) -> str:
    encoded = json.dumps(
        answer,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


# Workload families, longest-prefix first, used to map a full raw workload id
# (e.g. "insert_throughput_1m") back to its rendered family + scale so an
# explicit not_available artifact can be matched to the row it belongs to.
WORKLOAD_FAMILIES = [
    "insert_throughput",
    "select_scan",
    "select_sum",
    "select_avg",
    "filter_sum",
    "update_throughput",
    "delete_throughput",
    "mixed_oltp_pgbench_like",
    "mixed_correctness",
    "window_row_number",
]
SCALED_WORKLOAD_FAMILIES = {
    "insert_throughput",
    "select_scan",
    "select_sum",
    "select_avg",
    "filter_sum",
    "update_throughput",
    "delete_throughput",
}


def workload_family(workload: str) -> str | None:
    for family in WORKLOAD_FAMILIES:
        if workload == family or workload.startswith(f"{family}_"):
            return family
    return None


def scan_not_available(
    artifact_dir: Path,
    raw_dir: Path,
    *,
    required_storage_mode: str,
) -> tuple[dict[tuple[str, str, int], str], list[str]]:
    """Map (engine, family, n_rows) -> reason for every not_available raw file.

    An engine that explicitly records `status="not_available"` with a reason is
    accounted for: the workload was attempted and the gap is documented, so it
    must not be treated as a silently missing measurement.
    """
    statuses: dict[tuple[str, str, int], str] = {}
    errors: list[str] = []
    if not raw_dir.is_dir():
        return statuses, errors
    try:
        artifact_root = artifact_dir.resolve()
        raw_root = raw_dir.resolve()
    except (OSError, RuntimeError, ValueError) as err:
        return statuses, [f"cannot resolve raw artifact directory {raw_dir}: {err}"]
    if not raw_root.is_relative_to(artifact_root):
        return statuses, [f"raw dir must be inside artifact directory: {raw_root}"]

    for path in sorted(raw_dir.glob("*.json")):
        try:
            resolved_path = path.resolve()
        except (OSError, RuntimeError, ValueError) as err:
            errors.append(f"cannot resolve raw artifact {path}: {err}")
            continue
        if not resolved_path.is_relative_to(raw_root) or not resolved_path.is_file():
            errors.append(f"raw artifact path escapes raw directory: {path}")
            continue

        doc, load_errors = load_json(resolved_path)
        errors.extend(load_errors)
        if not isinstance(doc, dict) or doc.get("status") != "not_available":
            continue

        validated, raw_errors = validate_raw_file(
            resolved_path,
            required_storage_mode=required_storage_mode,
        )
        errors.extend(raw_errors)
        if validated is None:
            continue

        engine = validated.get("engine")
        workload = validated.get("workload")
        n_rows = validated.get("n_rows")
        if not isinstance(engine, str) or not isinstance(workload, str):
            continue
        if isinstance(n_rows, bool) or not isinstance(n_rows, int):
            continue
        family = workload_family(workload)
        if family is None:
            errors.append(f"{resolved_path}: unsupported workload {workload!r}")
            continue
        reason = validated.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            continue
        key = (canonical_engine(engine), family, n_rows)
        if key in statuses:
            errors.append(
                f"{resolved_path}: duplicate not_available artifact for "
                f"{key[0]} {family} rows={n_rows}"
            )
            continue
        statuses[key] = reason.strip()
    return statuses, errors


def resolve_artifact_path(artifact_dir: Path, raw_dir: Path, text: Any) -> Path | None:
    """Resolve current and legacy raw paths within the local artifact tree.

    Current renderers store paths relative to ``artifact_dir``. Older renderers
    stored absolute or repository-relative paths; for those, the raw filename
    is resolved beneath this artifact's ``raw`` directory. External paths and
    symlinks that escape the artifact tree are never followed.
    """
    if not isinstance(text, str) or not text.strip():
        return None
    path = Path(text)
    try:
        artifact_root = artifact_dir.resolve()
        raw_root = raw_dir.resolve()
    except (OSError, RuntimeError, ValueError):
        return None
    if not raw_root.is_relative_to(artifact_root):
        return None

    if not path.is_absolute():
        try:
            candidate = (artifact_dir / path).resolve()
        except (OSError, RuntimeError, ValueError):
            return None
        if candidate.is_relative_to(raw_root) and candidate.is_file():
            return candidate

    if not path.name:
        return None
    try:
        legacy_candidate = (raw_dir / path.name).resolve()
    except (OSError, RuntimeError, ValueError):
        return None
    if legacy_candidate.is_relative_to(raw_root):
        return legacy_candidate
    return None


def validate_manifest(
    manifest: Any,
    *,
    expected_commit: str | None,
    required_engines: list[str],
    required_storage_mode: str,
) -> tuple[str | None, str | None, set[int], int | None, list[str]]:
    errors: list[str] = []
    if not isinstance(manifest, dict):
        return (
            None,
            None,
            set(),
            None,
            ["scale_sweep_manifest.json must be a JSON object"],
        )

    if manifest.get("schema_version") != 1:
        errors.append("manifest schema_version must be 1")
    for field in ["mode", "ultrasql_version", "ultrasql_install_source", "methodology"]:
        require_text(manifest, field, errors)
    iterations = require_positive_int(manifest, "iters", errors)
    require_positive_int(manifest, "warmup", errors)
    manifest_rows: set[int] = set()
    rows = manifest.get("rows")
    if not isinstance(rows, list) or not rows:
        errors.append("rows must be a non-empty list")
    else:
        for index, value in enumerate(rows):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                errors.append(f"rows[{index}] must be a positive integer")
                continue
            if value in manifest_rows:
                errors.append(f"rows contains duplicate scale {value}")
                continue
            manifest_rows.add(value)

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

    return (
        release_commit,
        storage_mode if isinstance(storage_mode, str) else None,
        manifest_rows,
        iterations,
        errors,
    )


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
    require_text(raw, "engine", local_errors, label=f"{path}: engine")
    workload = require_text(raw, "workload", local_errors, label=f"{path}: workload")
    require_positive_int(raw, "n_rows", local_errors, label=f"{path}: n_rows")
    engine = canonical_engine(str(raw.get("engine"))) if isinstance(raw.get("engine"), str) else None
    if status == "measured":
        median = require_positive_number(raw, "median_us", local_errors)
        samples = require_positive_int(raw, "samples", local_errors)
        iterations = raw.get("iterations_us")
        if not isinstance(iterations, list) or not iterations:
            local_errors.append(f"{path}: iterations_us must be a non-empty list")
        else:
            valid_iterations: list[float] = []
            for index, value in enumerate(iterations):
                if (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(float(value))
                    or float(value) <= 0.0
                ):
                    local_errors.append(
                        f"{path}: iterations_us[{index}] must be a finite positive number"
                    )
                    continue
                valid_iterations.append(float(value))
            if samples is not None and samples != len(iterations):
                local_errors.append(
                    f"{path}: samples={samples} does not match "
                    f"iterations_us length {len(iterations)}"
                )
            if median is not None and len(valid_iterations) == len(iterations):
                derived_median = float(statistics.median(valid_iterations))
                fp_epsilon = max(math.ulp(median), math.ulp(derived_median))
                if (
                    abs(median - derived_median)
                    > MEDIAN_SERIALIZATION_ABS_TOLERANCE_US + fp_epsilon
                ):
                    local_errors.append(
                        f"{path}: median_us={median} does not match "
                        f"iterations_us median {derived_median} within "
                        f"{MEDIAN_SERIALIZATION_ABS_TOLERANCE_US} us"
                    )
        if workload is not None and workload_family(workload) == "mixed_correctness":
            answer_hash = raw.get("answer_sha256")
            if not isinstance(answer_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", answer_hash):
                local_errors.append(
                    f"{path}: mixed_correctness requires a lowercase 64-hex answer_sha256"
                )
            if "answer" not in raw:
                local_errors.append(f"{path}: mixed_correctness requires an answer")
            elif isinstance(answer_hash, str) and re.fullmatch(r"[0-9a-f]{64}", answer_hash):
                derived_hash = canonical_answer_sha256(raw["answer"])
                if answer_hash != derived_hash:
                    local_errors.append(
                        f"{path}: answer_sha256 does not match canonical answer "
                        f"(expected {derived_hash})"
                    )
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
        require_text(raw, "reason", local_errors, label=f"{path}: reason")
    if local_errors:
        return None, local_errors
    return raw, []


def measured_median(entry: Any) -> float | None:
    if not isinstance(entry, dict):
        return None
    value = entry.get("median_us")
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) <= 0.0
    ):
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
    not_available: dict[tuple[str, str, int], str],
    manifest_rows: set[int],
    expected_samples: int | None,
) -> dict[str, Any]:
    errors: list[str] = []
    if not isinstance(rendered, dict):
        return {"errors": ["scale_sweep.json must be a JSON object"]}
    if rendered.get("schema_version") != 1:
        errors.append("scale_sweep.json schema_version must be 1")
    rows = rendered.get("rows")
    if not isinstance(rows, list) or not rows:
        errors.append("scale_sweep.json rows must be a non-empty list")
        return {"errors": errors}

    row_summaries: list[dict[str, Any]] = []
    scoreboard_rows: list[dict[str, Any]] = []
    losses: list[dict[str, Any]] = []
    missing_required_rows: list[dict[str, Any]] = []
    comparable_count = 0
    complete_count = 0
    ultrasql_fastest_count = 0
    ultrasql_loss_count = 0
    ultrasql_not_available_count = 0
    total_rendered = 0
    seen_row_keys: set[tuple[str, int]] = set()
    rendered_scale_rows: set[int] = set()

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
        row_key = (workload, n_rows)
        if n_rows > 0 and row_key in seen_row_keys:
            errors.append(
                f"rows[{index}] duplicates rendered row {workload} rows={n_rows}"
            )
            continue
        if n_rows > 0:
            seen_row_keys.add(row_key)
            if workload in SCALED_WORKLOAD_FAMILIES:
                rendered_scale_rows.add(n_rows)
        engines = row.get("engines")
        if not isinstance(engines, dict):
            errors.append(f"rows[{index}].engines must be a JSON object")
            continue

        normalized_engines: dict[str, Any] = {}
        rendered_samples: dict[str, int] = {}
        for rendered_engine, entry in engines.items():
            engine = canonical_engine(str(rendered_engine))
            if engine in normalized_engines:
                errors.append(
                    f"{workload} rows={n_rows}: duplicate rendered engine {engine}"
                )
                continue
            if not isinstance(entry, dict):
                errors.append(
                    f"{workload} rows={n_rows} {engine}: rendered entry must be a JSON object"
                )
                continue
            normalized_engines[engine] = entry

            entry_engine = require_text(
                entry,
                "engine",
                errors,
                label=f"{workload} rows={n_rows} {engine}: rendered engine",
            )
            if entry_engine is not None and canonical_engine(entry_engine) != engine:
                errors.append(
                    f"{workload} rows={n_rows} {engine}: "
                    f"entry engine is {canonical_engine(entry_engine)}"
                )

            entry_workload = require_text(
                entry,
                "workload",
                errors,
                label=f"{workload} rows={n_rows} {engine}: rendered workload",
            )
            if (
                entry_workload is not None
                and workload_family(entry_workload) != workload
            ):
                errors.append(
                    f"{workload} rows={n_rows} {engine}: rendered workload "
                    f"{entry_workload!r} does not belong to row family {workload!r}"
                )

            if "n_rows" in entry:
                entry_n_rows = require_positive_int(
                    entry,
                    "n_rows",
                    errors,
                    label=f"{workload} rows={n_rows} {engine}: rendered n_rows",
                )
                if entry_n_rows is not None and entry_n_rows != n_rows:
                    errors.append(
                        f"{workload} rows={n_rows} {engine}: "
                        f"rendered n_rows is {entry_n_rows}"
                    )

            entry_samples = require_positive_int(
                entry,
                "samples",
                errors,
                label=f"{workload} rows={n_rows} {engine}: rendered samples",
            )
            if entry_samples is not None:
                rendered_samples[engine] = entry_samples
                if expected_samples is not None and entry_samples != expected_samples:
                    errors.append(
                        f"{workload} rows={n_rows} {engine}: rendered samples "
                        f"{entry_samples} do not match manifest iters {expected_samples}"
                    )

        measured: dict[str, float] = {}
        measured_raw: dict[str, dict[str, Any]] = {}
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
                if raw.get("status") != "measured":
                    errors.append(
                        f"{workload} rows={n_rows} {engine}: rendered measurement "
                        "must reference raw status=measured"
                    )
                    continue
                measured_raw[engine] = raw
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
                if raw.get("samples") != rendered_samples.get(engine):
                    errors.append(
                        f"{workload} rows={n_rows} {engine}: raw samples mismatch"
                    )
                if (
                    expected_samples is not None
                    and raw.get("samples") != expected_samples
                ):
                    errors.append(
                        f"{workload} rows={n_rows} {engine}: raw samples "
                        f"do not match manifest iters {expected_samples}"
                    )
                if abs(float(raw["median_us"]) - median) > 0.000001:
                    errors.append(
                        f"{workload} rows={n_rows} {engine}: raw median_us mismatch"
                    )

        # An engine is genuinely missing only when it is neither measured nor
        # explicitly recorded as not_available with a reason.
        not_available_here = [
            engine
            for engine in required_engines
            if engine not in measured and (engine, workload, n_rows) in not_available
        ]
        missing = [
            engine
            for engine in required_engines
            if engine not in measured and engine not in not_available_here
        ]
        if missing:
            missing_required_rows.append(
                {
                    "workload": workload,
                    "n_rows": n_rows,
                    "missing_engines": missing,
                }
            )
        else:
            complete_count += 1
        if not missing and not not_available_here:
            comparable_count += 1

        # Per-row scoreboard over the measured engines. Losses are first-class
        # reported data, never a certification failure.
        fastest_engine = None
        fastest_median = None
        ultrasql_result = "missing"
        if measured:
            fastest_median = min(measured.values())
            fastest_candidates = sorted(
                engine for engine, median in measured.items() if median == fastest_median
            )
            fastest_engine = fastest_candidates[0]
            rendered_fastest = canonical_engine(str(row.get("fastest_engine")))
            rendered_fastest_median = measured_median({"median_us": row.get("fastest_median_us")})
            if (
                rendered_fastest not in fastest_candidates
                or rendered_fastest_median != fastest_median
            ):
                errors.append(
                    f"{workload} rows={n_rows}: rendered fastest_engine must match raw medians"
                )
            ultrasql_median = measured.get("ultrasql")
            if ultrasql_median is not None:
                if "ultrasql" in fastest_candidates:
                    ultrasql_fastest_count += 1
                    ultrasql_result = "win"
                else:
                    ultrasql_result = "loss"
                    ultrasql_loss_count += 1
                    gap_pct = (
                        (ultrasql_median / fastest_median - 1.0) * 100.0
                        if fastest_median > 0.0
                        else None
                    )
                    losses.append(
                        {
                            "workload": workload,
                            "n_rows": n_rows,
                            "winner": fastest_engine,
                            "winner_median_us": fastest_median,
                            "ultrasql_median_us": ultrasql_median,
                            "gap_pct": gap_pct,
                        }
                    )
            elif "ultrasql" in not_available_here:
                ultrasql_result = "not_available"
                ultrasql_not_available_count += 1

        scoreboard_rows.append(
            {
                "workload": workload,
                "n_rows": n_rows,
                "fastest_engine": fastest_engine,
                "fastest_median_us": fastest_median,
                "ultrasql_median_us": measured.get("ultrasql"),
                "ultrasql_result": ultrasql_result,
            }
        )

        if workload == "mixed_correctness":
            answer_hash = row.get("answer_sha256")
            if row.get("correctness_status") != "verified":
                errors.append("mixed_correctness must have correctness_status=verified")
            if not isinstance(answer_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", answer_hash):
                errors.append("mixed_correctness must have a lowercase 64-hex answer_sha256")
            else:
                for engine, raw in measured_raw.items():
                    raw_hash = raw.get("answer_sha256")
                    if raw_hash != answer_hash:
                        errors.append(
                            f"mixed_correctness rows={n_rows} {engine}: "
                            "raw answer_sha256 does not match rendered answer_sha256"
                        )

        row_summaries.append(
            {
                "workload": workload,
                "n_rows": n_rows,
                "measured_engines": sorted(measured),
                "fastest_engine": row.get("fastest_engine"),
            }
        )

    if rendered_scale_rows != manifest_rows:
        errors.append(
            "manifest rows do not match rendered scalable row set: "
            f"manifest={sorted(manifest_rows)}, rendered={sorted(rendered_scale_rows)}"
        )

    # "ready" requires the configured number of fully comparable rows. Explicit
    # not_available artifacts make additional rows complete and honest, but they
    # do not turn those rows into cross-engine comparisons.
    if comparable_count < min_comparable_rows:
        errors.append(
            f"comparable_row_count {comparable_count} below minimum {min_comparable_rows}"
        )

    scoreboard = {
        "rows": scoreboard_rows,
        "ultrasql_win_count": ultrasql_fastest_count,
        "ultrasql_loss_count": ultrasql_loss_count,
        "ultrasql_not_available_count": ultrasql_not_available_count,
        "rows_not_led_by_ultrasql": ultrasql_loss_count + ultrasql_not_available_count,
        "losses": losses,
    }

    return {
        "row_summaries": row_summaries,
        "total_rendered": total_rendered,
        "comparable_count": comparable_count,
        "complete_count": complete_count,
        "ultrasql_fastest_count": ultrasql_fastest_count,
        "missing_required_rows": missing_required_rows,
        "scoreboard": scoreboard,
        "errors": errors,
    }


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

    (
        release_commit,
        storage_mode,
        manifest_rows,
        manifest_iterations,
        manifest_errors,
    ) = validate_manifest(
        manifest,
        expected_commit=expected_commit,
        required_engines=required_engines,
        required_storage_mode=required_storage_mode,
    )
    errors.extend(manifest_errors)

    not_available, not_available_errors = scan_not_available(
        artifact_dir,
        raw_dir,
        required_storage_mode=required_storage_mode,
    )
    errors.extend(not_available_errors)
    rendered_result = validate_rendered_rows(
        rendered,
        artifact_dir=artifact_dir,
        raw_dir=raw_dir,
        required_engines=required_engines,
        required_storage_mode=required_storage_mode,
        min_comparable_rows=min_comparable_rows,
        not_available=not_available,
        manifest_rows=manifest_rows,
        expected_samples=manifest_iterations,
    )
    rows = rendered_result.get("row_summaries", [])
    total_rendered = rendered_result.get("total_rendered", 0)
    comparable_count = rendered_result.get("comparable_count", 0)
    complete_count = rendered_result.get("complete_count", 0)
    ultrasql_fastest_count = rendered_result.get("ultrasql_fastest_count", 0)
    missing_required_rows = rendered_result.get("missing_required_rows", [])
    scoreboard = rendered_result.get("scoreboard", {})
    errors.extend(rendered_result.get("errors", []))

    # "ready" verifies artifact completeness and provenance, not workload
    # symmetry: enough fully measured comparable rows, valid raw evidence,
    # explicit reasons for any additional not_available rows, data-dir labels,
    # a pinned release commit, and a host descriptor. Driver differences are
    # documented in BENCHMARKS.md.
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
        "complete_row_count": complete_count,
        # Informational only: not a gate. UltraSQL is not required to win rows.
        "ultrasql_fastest_comparable_row_count": ultrasql_fastest_count,
        "ultrasql_fastest_row_count": ultrasql_fastest_count,
        "scoreboard": scoreboard,
        "missing_required_engine_rows": missing_required_rows,
        "rows": rows,
        "errors": errors,
        "reasons": reasons,
        "policy": (
            "ready means the configured minimum fully comparable rows, "
            "schema-valid self-contained raw evidence, complete coverage for "
            "additional rendered rows, data-dir labels, a pinned release commit, "
            "and a host descriptor. It does not prove workload symmetry or "
            "universal performance leadership; per-row results are reported, "
            "not gated."
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
