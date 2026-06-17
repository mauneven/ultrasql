#!/usr/bin/env python3
"""Filtered-ANN recall@10 vs an exact brute-force baseline, across filter
selectivities, measured end-to-end over the PostgreSQL wire against a running
ultrasqld.

For each selectivity the query is
`SELECT id FROM t WHERE bucket < T ORDER BY embedding <-> $probe LIMIT k`, run
through the server's selectivity-aware filtered-ANN path. The exact baseline is
computed independently in NumPy over the same filtered set. Recall and latency
are reported together; a recall floor that holds across every selectivity is the
no-cliff property.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import subprocess
import time
from pathlib import Path

import numpy as np
import psycopg


def cmd_output(*cmd: str) -> str | None:
    try:
        return subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def host_descriptor() -> dict:
    return {
        "hostname": platform.node(),
        "os": platform.platform(),
        "machine": platform.machine(),
        "cpu_model": cmd_output("sysctl", "-n", "machdep.cpu.brand_string")
        or platform.processor(),
        "logical_cpus": os.cpu_count(),
        "rustc": cmd_output("rustc", "--version"),
        "git_commit": cmd_output("git", "rev-parse", "HEAD"),
    }


def vector_literal(vec: np.ndarray) -> str:
    return "[" + ",".join(f"{x:.6f}" for x in vec.tolist()) + "]"


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(0, min(len(ordered) - 1, round(pct / 100.0 * (len(ordered) - 1))))
    return ordered[rank]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=os.environ.get("ULTRASQL_DSN", ""))
    parser.add_argument("--rows", type=int, default=20000)
    parser.add_argument("--dims", type=int, default=16)
    parser.add_argument("--queries", type=int, default=50)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    rng = np.random.default_rng(0x0A17EC)
    vectors = rng.standard_normal((args.rows, args.dims)).astype(np.float32)
    # Uniform bucket in [0, 1000): `bucket < T` selects ~T/1000 of the rows.
    buckets = rng.integers(0, 1000, size=args.rows)
    probes = rng.standard_normal((args.queries, args.dims)).astype(np.float32)

    conn = psycopg.connect(args.dsn or "", autocommit=True)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS fann")
    cur.execute(f"CREATE TABLE fann (id INT NOT NULL, bucket INT, embedding VECTOR({args.dims}))")
    build_rows_started = time.perf_counter()
    for start in range(0, args.rows, 1000):
        chunk = range(start, min(start + 1000, args.rows))
        values = ",".join(
            f"({i}, {int(buckets[i])}, '{vector_literal(vectors[i])}')" for i in chunk
        )
        cur.execute(f"INSERT INTO fann (id, bucket, embedding) VALUES {values}")
    index_started = time.perf_counter()
    cur.execute("CREATE INDEX fann_emb_hnsw ON fann USING hnsw (embedding)")
    index_build_us = (time.perf_counter() - index_started) * 1e6

    selectivities = [(1, "0.1%"), (10, "1%"), (100, "10%"), (1000, "100%")]
    per_selectivity = []
    for threshold, label in selectivities:
        mask = buckets < threshold
        matching = int(mask.sum())
        match_idx = np.where(mask)[0]
        recalls: list[float] = []
        latencies: list[float] = []
        for probe in probes:
            literal = vector_literal(probe)
            t0 = time.perf_counter()
            cur.execute(
                f"SELECT id FROM fann WHERE bucket < {threshold} "
                f"ORDER BY embedding <-> VECTOR '{literal}' LIMIT {args.k}"
            )
            got = {int(row[0]) for row in cur.fetchall()}
            latencies.append((time.perf_counter() - t0) * 1e6)
            distances = np.linalg.norm(vectors[match_idx] - probe, axis=1)
            order = match_idx[np.argsort(distances, kind="stable")]
            denom = min(args.k, matching)
            exact = {int(i) for i in order[: args.k]}
            recalls.append(len(exact & got) / denom if denom else 1.0)
        per_selectivity.append(
            {
                "selectivity": label,
                "bucket_threshold": threshold,
                "matching_rows": matching,
                "recall_at_k_mean": statistics.fmean(recalls),
                "recall_at_k_min": min(recalls),
                "p50_latency_us": percentile(latencies, 50),
                "p95_latency_us": percentile(latencies, 95),
                "p99_latency_us": percentile(latencies, 99),
            }
        )

    cur.execute("SELECT version()")
    version_row = cur.fetchone()
    conn.close()

    recall_floor = min(s["recall_at_k_mean"] for s in per_selectivity)
    artifact = {
        "schema_version": 1,
        "engine": "ultrasql",
        "status": "measured",
        "workload": f"filtered_ann_recall_{args.rows}_{args.dims}d_k{args.k}",
        "n_rows": args.rows,
        "dims": args.dims,
        "k": args.k,
        "queries": args.queries,
        "metric": "l2",
        "index": "hnsw",
        "ingest_us": (index_started - build_rows_started) * 1e6,
        "index_build_us": index_build_us,
        "recall_at_k_floor": recall_floor,
        "per_selectivity": per_selectivity,
        "server_version": version_row[0] if version_row else None,
        "host": host_descriptor(),
        "policy": (
            "Recall@k is reported with p50/p95/p99 latency at every selectivity; "
            "the exact baseline is an independent NumPy brute-force over the same "
            "filtered set. A recall floor holding across selectivities is the "
            "no-cliff property."
        ),
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
    print(f"filtered-ANN recall floor (mean recall@{args.k}): {recall_floor:.4f}")
    for sel in per_selectivity:
        print(
            f"  {sel['selectivity']:>5} ({sel['matching_rows']} rows): "
            f"recall@{args.k}={sel['recall_at_k_mean']:.4f} "
            f"p50={sel['p50_latency_us']:.0f}us p95={sel['p95_latency_us']:.0f}us"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
