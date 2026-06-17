#!/usr/bin/env python3
"""
Benchmark scoreboard reporter.

Reads every `*-<engine>.json` file under `benchmarks/results/latest/raw/`,
groups them by workload, and for each comparable workload reports the engine
with the lowest `median_us` and whether UltraSQL led, lost, or was
not_available. Per-row wins and losses are first-class reported data — an
honest loss is not a failure. The release gate lives in
`scripts/validate-benchmark-certification.py`; this script is informational and
exits 0 as long as it finds results to report.

Usage:
  check_supremacy.py [results_dir]

Default results_dir is benchmarks/results/latest/raw.
"""

from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from pathlib import Path


def main(argv: list[str]) -> int:
    here = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.abspath(os.path.join(here, "..", ".."))
    default_dir = os.path.join(repo_root, "benchmarks", "results", "latest", "raw")
    results_dir = argv[1] if len(argv) > 1 else default_dir

    if not os.path.isdir(results_dir):
        print(f"ERROR: results dir not found: {results_dir}", file=sys.stderr)
        return 2

    measured: dict[str, dict[str, float]] = defaultdict(dict)
    not_available: dict[str, set[str]] = defaultdict(set)
    for name in sorted(os.listdir(results_dir)):
        if not name.endswith(".json"):
            continue
        path = os.path.join(results_dir, name)
        try:
            with open(path, encoding="utf-8") as fh:
                obj = json.load(fh)
        except (OSError, json.JSONDecodeError) as e:
            print(f"WARN: skip {name}: {e}", file=sys.stderr)
            continue
        workload = obj.get("workload")
        engine = obj.get("engine")
        if workload is None or engine is None:
            continue
        canonical = "ultrasql" if str(engine).startswith("ultrasql") else str(engine)
        status = obj.get("status", "measured")
        if status == "not_available":
            not_available[workload].add(canonical)
            continue
        if status != "measured":
            continue
        median = obj.get("median_us")
        if median is None:
            continue
        median_us = float(median)
        previous = measured[workload].get(canonical)
        measured[workload][canonical] = (
            median_us if previous is None else min(previous, median_us)
        )

    workloads = sorted(set(measured) | set(not_available))
    if not workloads:
        print(f"ERROR: no benchmark results found in {results_dir}", file=sys.stderr)
        return 2

    wins = losses = na = 0
    print(f"== benchmark scoreboard on {results_dir} ==")
    print(f"{'workload':<28} {'fastest':<14} {'µs':>12}  ultrasql")
    for workload in workloads:
        engines = measured.get(workload, {})
        if not engines:
            print(f"{workload:<28} {'(none measured)':<14} {'-':>12}  not_available")
            if "ultrasql" in not_available.get(workload, set()):
                na += 1
            continue
        winner_name, winner_us = min(engines.items(), key=lambda kv: kv[1])
        if "ultrasql" in engines:
            ultra = engines["ultrasql"]
            if winner_name == "ultrasql":
                wins += 1
                verdict = "win"
            else:
                losses += 1
                gap = (ultra / winner_us - 1.0) * 100.0 if winner_us > 0 else float("nan")
                verdict = f"loss to {winner_name} (+{gap:.1f}%)"
        elif "ultrasql" in not_available.get(workload, set()):
            na += 1
            verdict = "not_available"
        else:
            verdict = "missing"
        print(f"{workload:<28} {winner_name:<14} {winner_us:>12.1f}  {verdict}")

    total = wins + losses + na
    print(
        f"\nUltraSQL leads {wins} of {total} comparable workloads "
        f"({losses} loss, {na} not_available). Losses are reported, not gated."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
