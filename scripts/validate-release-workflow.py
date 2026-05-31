#!/usr/bin/env python3
"""Validate release workflow packaging invariants."""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RELEASE_WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"


def fail(message: str) -> None:
    print(f"release workflow validation failed: {message}", file=sys.stderr)
    sys.exit(1)


def require(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


def job_body(text: str, job_name: str) -> str:
    match = re.search(
        rf"(?ms)^  {re.escape(job_name)}:\n(?P<body>.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)",
        text,
    )
    if match is None:
        fail(f"missing `{job_name}` job")
    return match.group("body")


def docker_platforms(docker_body: str) -> set[str]:
    match = re.search(r"(?m)^\s+platforms:\s*([^\n#]+)", docker_body)
    if match is None:
        fail("docker job missing `platforms`")
    return {platform.strip() for platform in match.group(1).split(",") if platform.strip()}


def main() -> None:
    text = RELEASE_WORKFLOW.read_text(encoding="utf-8")
    docker = job_body(text, "docker")

    required_platforms = {"linux/amd64", "linux/arm64"}
    platforms = docker_platforms(docker)
    require(
        required_platforms <= platforms,
        f"docker platforms {sorted(platforms)} missing {sorted(required_platforms - platforms)}",
    )
    require("docker/setup-qemu-action" in docker, "docker job must set up QEMU for arm64")
    require("provenance: false" in docker, "docker provenance attestations must stay disabled")
    require("sbom: false" in docker, "docker SBOM attestations must stay disabled")

    for job_name in ("publish", "npm", "chocolatey", "aur", "homebrew"):
        job_body(text, job_name)


if __name__ == "__main__":
    main()
