#!/usr/bin/env python3
"""Validate public benchmark arena artifact schema and claim hygiene."""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
from typing import Any


FORBIDDEN_CLAIM_KEYS = {
    "winner",
    "rank",
    "ranking",
    "rankings",
    "vs_fastest",
    "speedup_claim",
}


def load_json(path: pathlib.Path) -> dict[str, Any]:
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{path}: invalid JSON: {exc}") from exc
    if not isinstance(doc, dict):
        raise ValueError(f"{path}: artifact root must be a JSON object")
    return doc


def has_host_metadata(doc: dict[str, Any]) -> bool:
    host = doc.get("host")
    if isinstance(host, dict):
        has_cpu = bool(host.get("cpu") or host.get("cpu_model"))
        has_memory = bool(
            host.get("ram_gb")
            or host.get("memory_bytes")
            or host.get("host_memory")
        )
        return has_cpu and has_memory
    return bool(doc.get("host_cpu")) and doc.get("host_memory") is not None


def is_leaf_artifact(doc: dict[str, Any]) -> bool:
    return bool(doc.get("engine")) and bool(doc.get("workload") or doc.get("suite"))


def validate_doc(path: pathlib.Path, doc: dict[str, Any], errors: list[str]) -> None:
    if doc.get("schema_version") != 1:
        errors.append(f"{path}: missing schema_version=1")

    for key in FORBIDDEN_CLAIM_KEYS:
        if key in doc:
            errors.append(f"{path}: fake win field '{key}' is forbidden")

    status = doc.get("status")
    if status == "not_available" and not doc.get("reason"):
        errors.append(f"{path}: not_available artifact missing reason")

    if is_leaf_artifact(doc) and not has_host_metadata(doc):
        errors.append(f"{path}: missing host metadata (host or host_cpu/host_memory)")

    if status == "measured":
        has_samples = (
            bool(doc.get("samples"))
            or bool(doc.get("iterations_us"))
            or bool(doc.get("queries"))
            or bool(doc.get("query_samples_us"))
            or bool(doc.get("benchmark_runs"))
            or bool(doc.get("engines"))
        )
        if not has_samples:
            errors.append(f"{path}: measured artifact missing samples")
        if not has_host_metadata(doc):
            errors.append(f"{path}: measured artifact missing host metadata")


def artifact_paths_from_manifest(manifest: pathlib.Path, doc: dict[str, Any]) -> list[pathlib.Path]:
    paths: list[pathlib.Path] = []
    for entry in doc.get("suites", []):
        if not isinstance(entry, dict):
            continue
        artifact = entry.get("artifact")
        if not isinstance(artifact, str) or not artifact:
            continue
        if any(token in artifact for token in ("*", "{", "}")):
            continue
        path = pathlib.Path(artifact)
        if not path.is_absolute():
            path = manifest.parent.parent.parent.parent / path
        paths.append(path)
    return paths


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=pathlib.Path)
    args = parser.parse_args()

    errors: list[str] = []
    manifest = args.manifest
    if not manifest.exists():
        print(f"{manifest}: missing arena manifest", file=sys.stderr)
        return 1

    try:
        manifest_doc = load_json(manifest)
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 1

    validate_doc(manifest, manifest_doc, errors)

    for artifact in artifact_paths_from_manifest(manifest, manifest_doc):
        if not artifact.exists():
            errors.append(f"{artifact}: referenced artifact missing")
            continue
        try:
            validate_doc(artifact, load_json(artifact), errors)
        except ValueError as exc:
            errors.append(str(exc))

    if errors:
        print("benchmark artifact schema gate failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    print(f"benchmark artifact schema gate passed: {manifest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
