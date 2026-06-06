#!/usr/bin/env python3
"""
Benchmark lead check - assert UltraSQL leads every workload in the
benchmark matrix.

Reads every `*-<engine>.json` file under `benchmarks/results/latest/raw/`,
groups them by workload, and for each comparable workload picks the engine
with the lowest `median_us`. Exits 0 iff `ultrasql` is the winner of every
comparable workload that has a result.

A workload counts as "won by ultrasql" iff:
  - the ultrasql sample exists for that workload, AND
  - ultrasql.median_us <= min(other engines' median_us).

If only one engine ran a given workload, it cannot be a competitor win;
those workloads are reported as "uncontested" but do NOT fail the gate
unless ultrasql itself is missing.

Usage:
  benchmark-lead-check [results_dir]

Default results_dir is benchmarks/results/latest/raw.
"""

from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from pathlib import Path


def partial_summary_reason(results_dir: str, workload: str) -> str | None:
    summary_path = Path(results_dir).parent / f"{workload}_certification.json"
    try:
        with summary_path.open(encoding="utf-8") as fh:
            summary = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None

    if summary.get("workload") != workload:
        return None
    if summary.get("status") == "partial" or summary.get("comparison_ready") is False:
        return str(summary.get("reason") or "partial_certification")
    return None


def main(argv: list[str]) -> int:
    here = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.abspath(os.path.join(here, "..", ".."))
    default_dir = os.path.join(repo_root, "benchmarks", "results", "latest", "raw")
    results_dir = argv[1] if len(argv) > 1 else default_dir

    if not os.path.isdir(results_dir):
        print(f"ERROR: results dir not found: {results_dir}", file=sys.stderr)
        return 2

    by_workload: dict[str, dict[str, float]] = defaultdict(dict)
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
        if obj.get("status", "measured") != "measured":
            continue
        workload = obj.get("workload")
        engine = obj.get("engine")
        median = obj.get("median_us")
        if workload is None or engine is None or median is None:
            print(f"WARN: skip {name}: missing workload/engine/median_us", file=sys.stderr)
            continue
        canonical_engine = "ultrasql" if str(engine).startswith("ultrasql") else str(engine)
        median_us = float(median)
        previous = by_workload[workload].get(canonical_engine)
        by_workload[workload][canonical_engine] = (
            median_us if previous is None else min(previous, median_us)
        )

    if not by_workload:
        print(f"ERROR: no benchmark results found in {results_dir}", file=sys.stderr)
        return 2

    losses: list[str] = []
    print(f"== benchmark lead check on {results_dir} ==")
    print(f"{'workload':<28} {'winner':<14} {'µs':>12}  others")
    for workload in sorted(by_workload.keys()):
        engines = by_workload[workload]
        if "ultrasql" not in engines:
            reason = partial_summary_reason(results_dir, workload)
            if reason is not None:
                print(
                    f"{workload:<28} {'(unranked)':<14} {'-':>12}  "
                    f"partial certification: {reason}"
                )
                continue
            losses.append(f"{workload}: ultrasql sample missing")
            print(f"{workload:<28} {'(no-ultrasql)':<14} {'-':>12}  {sorted(engines)}")
            continue
        winner = min(engines.items(), key=lambda kv: kv[1])
        winner_name, winner_us = winner
        ultrasql_us = engines["ultrasql"]
        others = ", ".join(f"{e}={us:.1f}" for e, us in sorted(engines.items()) if e != "ultrasql")
        print(f"{workload:<28} {winner_name:<14} {winner_us:>12.1f}  {others}")
        if winner_name != "ultrasql":
            losses.append(
                f"{workload}: ultrasql={ultrasql_us:.1f}µs, "
                f"loser to {winner_name}={winner_us:.1f}µs"
            )

    if losses:
        print(f"\nFAIL: ultrasql does not win {len(losses)} workload(s):", file=sys.stderr)
        for line in losses:
            print(f"  - {line}", file=sys.stderr)
        return 1

    print(f"\nPASS: ultrasql wins all {len(by_workload)} workloads")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
