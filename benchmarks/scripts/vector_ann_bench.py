#!/usr/bin/env python3
"""Honest ANN recall@k-vs-latency benchmark over a real dataset (SIFT/TEXMEX),
comparing UltraSQL against pgvector, Qdrant, and LanceDB on the same host.

Principles (do not relax):
  * Same base vectors, same query set, same ground truth, same k, same metric
    (L2) for every engine. Ground truth is the dataset's own exact k-NN file.
  * Recall@k is ALWAYS reported with p50/p95/p99 query latency. No latency is
    ever printed without the recall it was measured at.
  * Each engine is configured with matched HNSW parameters where it exposes
    them (m, ef_construction); competitors are additionally swept across
    ef_search to trace their recall/latency curve. The matched operating point
    (ef_search = UltraSQL's effective value) is the apples-to-apples row.
  * An engine that is not reachable / not installed is recorded "not_available"
    with a reason. It is never faked, skipped silently, or back-filled.

Dataset format: .fvecs / .ivecs (TEXMEX). base/query are float32 vectors framed
by a leading int32 dimension; ground truth is int32 neighbor-id lists.
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


# --------------------------------------------------------------------------
# Dataset IO
# --------------------------------------------------------------------------
def read_fvecs(path: Path, limit: int | None = None) -> np.ndarray:
    raw = np.fromfile(path, dtype=np.int32)
    if raw.size == 0:
        return np.zeros((0, 0), dtype=np.float32)
    dim = int(raw[0])
    stride = dim + 1
    rows = raw.reshape(-1, stride)
    if limit is not None:
        rows = rows[:limit]
    return rows[:, 1:].view(np.float32).astype(np.float32, copy=True)


def count_fvecs(path: Path) -> int:
    with open(path, "rb") as handle:
        head = handle.read(4)
        if len(head) < 4:
            return 0
        dim = int(np.frombuffer(head, dtype=np.int32)[0])
    row_bytes = (dim + 1) * 4
    return path.stat().st_size // row_bytes if row_bytes else 0


def read_ivecs(path: Path, limit: int | None = None) -> np.ndarray:
    raw = np.fromfile(path, dtype=np.int32)
    if raw.size == 0:
        return np.zeros((0, 0), dtype=np.int32)
    dim = int(raw[0])
    stride = dim + 1
    rows = raw.reshape(-1, stride)
    if limit is not None:
        rows = rows[:limit]
    return rows[:, 1:].astype(np.int32, copy=True)


# --------------------------------------------------------------------------
# Shared helpers
# --------------------------------------------------------------------------
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
        "git_commit": cmd_output("git", "rev-parse", "HEAD"),
    }


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(0, min(len(ordered) - 1, round(pct / 100.0 * (len(ordered) - 1))))
    return ordered[rank]


def compute_groundtruth(base: np.ndarray, queries: np.ndarray, k: int) -> np.ndarray:
    """Exact k-NN (L2) of each query over the *loaded* base, as an independent
    NumPy baseline. Computed here rather than read from the dataset's own
    ground-truth file because that file's neighbor ids index the full corpus —
    using it against a loaded subset would be wrong. Computing it ourselves is
    both correct at any base size and an independent check on every engine.
    """
    n = base.shape[0]
    kk = min(k, n)
    truth = np.empty((queries.shape[0], kk), dtype=np.int64)
    for qi in range(queries.shape[0]):
        dist = np.linalg.norm(base - queries[qi], axis=1)
        top = np.argpartition(dist, kk - 1)[:kk]
        truth[qi] = top[np.argsort(dist[top], kind="stable")]
    return truth


def recall_at_k(got_ids: list[int], truth_ids: np.ndarray, k: int) -> float:
    truth = set(int(x) for x in truth_ids[:k])
    if not truth:
        return 1.0
    return len(truth & set(got_ids[:k])) / len(truth)


def vector_literal(vec: np.ndarray) -> str:
    return "[" + ",".join(f"{x:.6f}" for x in vec.tolist()) + "]"


def summarize_point(recalls: list[float], latencies: list[float], ef: int | None) -> dict:
    return {
        "ef_search": ef,
        "recall_at_k_mean": statistics.fmean(recalls) if recalls else 0.0,
        "recall_at_k_min": min(recalls) if recalls else 0.0,
        "p50_latency_us": percentile(latencies, 50),
        "p95_latency_us": percentile(latencies, 95),
        "p99_latency_us": percentile(latencies, 99),
        "queries": len(latencies),
    }


# --------------------------------------------------------------------------
# Engine adapters. Each returns an artifact dict; on failure it returns a
# {"status": "not_available", "reason": ...} dict and never raises.
# --------------------------------------------------------------------------
def bench_ultrasql(base, queries, truth, cfg) -> dict:
    engine = {"engine": "ultrasql", "index": "hnsw"}
    try:
        import psycopg
    except ImportError as exc:
        return {**engine, "status": "not_available", "reason": f"psycopg import failed: {exc}"}
    dsn = cfg["ultrasql_dsn"]
    if not dsn:
        return {**engine, "status": "not_available", "reason": "no ULTRASQL_DSN"}
    n, dim = base.shape
    k = cfg["k"]
    try:
        conn = psycopg.connect(dsn, autocommit=True)
    except Exception as exc:  # noqa: BLE001 - report any connect failure honestly
        return {**engine, "status": "not_available", "reason": f"connect failed: {exc}"}
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS sift_bench")
    cur.execute(f"CREATE TABLE sift_bench (id INT NOT NULL, embedding VECTOR({dim}))")
    ingest_start = time.perf_counter()
    batch = 500
    for start in range(0, n, batch):
        stop = min(start + batch, n)
        values = ",".join(
            f"({i}, '{vector_literal(base[i])}')" for i in range(start, stop)
        )
        cur.execute(f"INSERT INTO sift_bench (id, embedding) VALUES {values}")
    ingest_us = (time.perf_counter() - ingest_start) * 1e6
    build_start = time.perf_counter()
    cur.execute("CREATE INDEX sift_bench_hnsw ON sift_bench USING hnsw (embedding vector_l2_ops)")
    build_us = (time.perf_counter() - build_start) * 1e6
    cur.execute("SELECT version()")
    version = cur.fetchone()[0]
    # Sweep the pgvector-compatible per-session ef_search knob to trace the
    # recall/latency curve, same as the other engines.
    points = []
    for ef in cfg["ef_search_list"]:
        cur.execute(f"SET hnsw.ef_search = {ef}")
        for q in queries[: min(5, len(queries))]:
            cur.execute(
                f"SELECT id FROM sift_bench ORDER BY embedding <-> VECTOR '{vector_literal(q)}' LIMIT {k}"
            )
            cur.fetchall()
        recalls, latencies = [], []
        for qi, q in enumerate(queries):
            lit = vector_literal(q)
            t0 = time.perf_counter()
            cur.execute(
                f"SELECT id FROM sift_bench ORDER BY embedding <-> VECTOR '{lit}' LIMIT {k}"
            )
            got = [int(r[0]) for r in cur.fetchall()]
            latencies.append((time.perf_counter() - t0) * 1e6)
            recalls.append(recall_at_k(got, truth[qi], k))
        points.append(summarize_point(recalls, latencies, ef))
    conn.close()
    return {
        **engine,
        "status": "measured",
        "server_version": version,
        "config": {"m": 16, "ef_construction": "internal_default (exact top-200 + heuristic)"},
        "ingest_us": ingest_us,
        "index_build_us": build_us,
        "points": points,
    }


def bench_pgvector(base, queries, truth, cfg) -> dict:
    engine = {"engine": "postgres17_pgvector", "index": "hnsw"}
    try:
        import psycopg
    except ImportError as exc:
        return {**engine, "status": "not_available", "reason": f"psycopg import failed: {exc}"}
    dsn = cfg["pg_dsn"]
    n, dim = base.shape
    k = cfg["k"]
    try:
        conn = psycopg.connect(dsn, autocommit=True)
    except Exception as exc:  # noqa: BLE001
        return {**engine, "status": "not_available", "reason": f"connect failed: {exc}"}
    cur = conn.cursor()
    try:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
    except Exception as exc:  # noqa: BLE001
        conn.close()
        return {**engine, "status": "not_available", "reason": f"pgvector extension unavailable: {exc}"}
    cur.execute("SELECT extversion FROM pg_extension WHERE extname='vector'")
    pgv_version = cur.fetchone()
    cur.execute("DROP TABLE IF EXISTS sift_bench")
    cur.execute(f"CREATE TABLE sift_bench (id INT PRIMARY KEY, embedding vector({dim}))")
    ingest_start = time.perf_counter()
    with cur.copy("COPY sift_bench (id, embedding) FROM STDIN") as copy:
        for i in range(n):
            copy.write_row((i, vector_literal(base[i])))
    ingest_us = (time.perf_counter() - ingest_start) * 1e6
    build_start = time.perf_counter()
    cur.execute(
        f"CREATE INDEX sift_bench_hnsw ON sift_bench USING hnsw (embedding vector_l2_ops) "
        f"WITH (m = {cfg['m']}, ef_construction = {cfg['ef_construction']})"
    )
    build_us = (time.perf_counter() - build_start) * 1e6
    cur.execute("SELECT version()")
    version = cur.fetchone()[0]
    points = []
    for ef in cfg["ef_search_list"]:
        cur.execute(f"SET hnsw.ef_search = {ef}")
        for q in queries[: min(5, len(queries))]:
            cur.execute(
                f"SELECT id FROM sift_bench ORDER BY embedding <-> '{vector_literal(q)}' LIMIT {k}"
            )
            cur.fetchall()
        recalls, latencies = [], []
        for qi, q in enumerate(queries):
            lit = vector_literal(q)
            t0 = time.perf_counter()
            cur.execute(
                f"SELECT id FROM sift_bench ORDER BY embedding <-> '{lit}' LIMIT {k}"
            )
            got = [int(r[0]) for r in cur.fetchall()]
            latencies.append((time.perf_counter() - t0) * 1e6)
            recalls.append(recall_at_k(got, truth[qi], k))
        points.append(summarize_point(recalls, latencies, ef))
    conn.close()
    return {
        **engine,
        "status": "measured",
        "server_version": version,
        "pgvector_version": pgv_version[0] if pgv_version else None,
        "config": {"m": cfg["m"], "ef_construction": cfg["ef_construction"]},
        "ingest_us": ingest_us,
        "index_build_us": build_us,
        "points": points,
    }


def bench_qdrant(base, queries, truth, cfg) -> dict:
    engine = {"engine": "qdrant", "index": "hnsw"}
    try:
        from qdrant_client import QdrantClient
        from qdrant_client import models as qmodels
    except ImportError as exc:
        return {**engine, "status": "not_available", "reason": f"qdrant_client import failed: {exc}"}
    n, dim = base.shape
    k = cfg["k"]
    try:
        client = QdrantClient(url=cfg["qdrant_url"], timeout=120)
        client.get_collections()
    except Exception as exc:  # noqa: BLE001
        return {**engine, "status": "not_available", "reason": f"connect failed: {exc}"}
    coll = "sift_bench"
    try:
        try:
            client.delete_collection(coll)
        except Exception:  # noqa: BLE001 - first run has no collection
            pass
        client.create_collection(
            collection_name=coll,
            vectors_config=qmodels.VectorParams(size=dim, distance=qmodels.Distance.EUCLID),
            hnsw_config=qmodels.HnswConfigDiff(m=cfg["m"], ef_construct=cfg["ef_construction"]),
        )
        ingest_start = time.perf_counter()
        batch = 1000
        for start in range(0, n, batch):
            stop = min(start + batch, n)
            client.upsert(
                collection_name=coll,
                points=qmodels.Batch(
                    ids=list(range(start, stop)),
                    vectors=base[start:stop].tolist(),
                ),
                wait=True,
            )
        ingest_us = (time.perf_counter() - ingest_start) * 1e6
        # Wait for Qdrant to finish optimizing (it builds HNSW async). A GREEN
        # status means optimization is complete; indexed_vectors_count stays 0
        # for small collections that Qdrant keeps in a plain (still searchable)
        # segment, so it is not a reliable readiness signal.
        build_start = time.perf_counter()
        deadline = build_start + 300
        while time.perf_counter() < deadline:
            info = client.get_collection(coll)
            if info.status == qmodels.CollectionStatus.GREEN:
                break
            time.sleep(0.5)
        build_us = (time.perf_counter() - build_start) * 1e6
    except Exception as exc:  # noqa: BLE001
        return {**engine, "status": "not_available", "reason": f"load/build failed: {exc}"}
    points = []
    for ef in cfg["ef_search_list"]:
        params = qmodels.SearchParams(hnsw_ef=ef, exact=False)
        for q in queries[: min(5, len(queries))]:
            client.query_points(coll, query=q.tolist(), limit=k, search_params=params)
        recalls, latencies = [], []
        for qi, q in enumerate(queries):
            t0 = time.perf_counter()
            res = client.query_points(coll, query=q.tolist(), limit=k, search_params=params)
            got = [int(p.id) for p in res.points]
            latencies.append((time.perf_counter() - t0) * 1e6)
            recalls.append(recall_at_k(got, truth[qi], k))
        points.append(summarize_point(recalls, latencies, ef))
    version = None
    try:
        version = client._client.openapi_client.service_api.root().version  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        version = "unknown"
    return {
        **engine,
        "status": "measured",
        "server_version": version,
        "config": {"m": cfg["m"], "ef_construction": cfg["ef_construction"]},
        "ingest_us": ingest_us,
        "index_build_us": build_us,
        "points": points,
    }


def bench_lancedb(base, queries, truth, cfg) -> dict:
    engine = {"engine": "lancedb"}
    try:
        import lancedb
        import pyarrow as pa
    except ImportError as exc:
        return {**engine, "status": "not_available", "reason": f"lancedb import failed: {exc}"}
    n, dim = base.shape
    k = cfg["k"]
    try:
        db = lancedb.connect(cfg["lancedb_dir"])
        try:
            db.drop_table("sift_bench")
        except Exception:  # noqa: BLE001 - first run has no table
            pass
        schema = pa.schema(
            [pa.field("id", pa.int32()), pa.field("vector", pa.list_(pa.float32(), dim))]
        )
        ingest_start = time.perf_counter()
        data = [{"id": i, "vector": base[i].tolist()} for i in range(n)]
        table = db.create_table("sift_bench", data=data, schema=schema)
        ingest_us = (time.perf_counter() - ingest_start) * 1e6
        build_start = time.perf_counter()
        index_kind = "IVF_HNSW_SQ"
        try:
            table.create_index(
                metric="l2",
                index_type="IVF_HNSW_SQ",
                m=cfg["m"],
                ef_construction=cfg["ef_construction"],
            )
        except Exception:  # noqa: BLE001 - fall back to the default vector index
            index_kind = "default"
            table.create_index(metric="l2")
        table.wait_for_index(["vector_idx"]) if hasattr(table, "wait_for_index") else None
        build_us = (time.perf_counter() - build_start) * 1e6
    except Exception as exc:  # noqa: BLE001
        return {**engine, "status": "not_available", "reason": f"load/build failed: {exc}"}
    # LanceDB's IVF_HNSW_SQ scalar-quantizes vectors, so recall is recovered by
    # `refine_factor` (re-rank the over-fetched candidates against exact vectors)
    # — the documented high-recall knob — not by ef/nprobes. Sweep refine_factor
    # to trace its recall/latency curve, the LanceDB analogue of ef_search.
    points = []
    for refine in cfg["lancedb_refine_list"]:
        def run_query(q):
            return (
                table.search(q.tolist())
                .metric("l2")
                .limit(k)
                .nprobes(cfg["lancedb_nprobes"])
                .refine_factor(refine)
                .to_list()
            )
        try:
            for q in queries[: min(5, len(queries))]:
                run_query(q)
            recalls, latencies = [], []
            for qi, q in enumerate(queries):
                t0 = time.perf_counter()
                res = run_query(q)
                got = [int(r["id"]) for r in res]
                latencies.append((time.perf_counter() - t0) * 1e6)
                recalls.append(recall_at_k(got, truth[qi], k))
            pt = summarize_point(recalls, latencies, None)
            pt["refine_factor"] = refine
            pt["nprobes"] = cfg["lancedb_nprobes"]
            points.append(pt)
        except Exception as exc:  # noqa: BLE001
            points.append({"refine_factor": refine, "status": "query_failed", "reason": str(exc)})
    return {
        **engine,
        "status": "measured",
        "index": index_kind,
        "tuning_knob": "refine_factor (IVF+SQ; ef_search not applicable)",
        "server_version": getattr(lancedb, "__version__", "unknown"),
        "config": {"m": cfg["m"], "ef_construction": cfg["ef_construction"], "nprobes": cfg["lancedb_nprobes"]},
        "ingest_us": ingest_us,
        "index_build_us": build_us,
        "points": points,
    }


ADAPTERS = {
    "ultrasql": bench_ultrasql,
    "pgvector": bench_pgvector,
    "qdrant": bench_qdrant,
    "lancedb": bench_lancedb,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--query", type=Path, required=True)
    parser.add_argument("--groundtruth", type=Path, required=True)
    parser.add_argument("--groundtruth-mode", choices=["compute", "file"], default="compute")
    parser.add_argument("--n-base", type=int, default=None)
    parser.add_argument("--n-queries", type=int, default=100)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--m", type=int, default=16)
    parser.add_argument("--ef-construction", type=int, default=200)
    parser.add_argument("--ef-search-list", default="10,40,64,100,200")
    parser.add_argument("--ultrasql-effective-ef", type=int, default=64)
    parser.add_argument("--lancedb-nprobes", type=int, default=20)
    parser.add_argument("--lancedb-refine-list", default="1,5,10,20,50")
    parser.add_argument("--engines", default="ultrasql,pgvector,qdrant,lancedb")
    parser.add_argument("--ultrasql-dsn", default=os.environ.get("ULTRASQL_DSN", ""))
    parser.add_argument("--pg-dsn", default=os.environ.get("PG_DSN", ""))
    parser.add_argument("--qdrant-url", default=os.environ.get("QDRANT_URL", "http://localhost:6333"))
    parser.add_argument("--lancedb-dir", default=os.environ.get("LANCEDB_DIR", "/tmp/ultrasql-lancedb"))
    parser.add_argument("--dataset-name", default="sift")
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()

    base = read_fvecs(args.base, args.n_base)
    queries = read_fvecs(args.query, args.n_queries)
    n, dim = base.shape
    print(f"dataset={args.dataset_name} base={n}x{dim} queries={len(queries)} k={args.k}")
    # Exact ground truth over the loaded base (independent NumPy baseline). The
    # dataset's own .ivecs file indexes the full 1M corpus and is only valid at
    # full scale, so computing it here keeps recall correct at every base size.
    if args.groundtruth_mode == "file" and n == count_fvecs(args.base):
        truth = read_ivecs(args.groundtruth, args.n_queries)
        print("ground truth: dataset file (full base loaded)")
    else:
        gt_start = time.perf_counter()
        truth = compute_groundtruth(base, queries, args.k)
        print(f"ground truth: computed exact k-NN in {time.perf_counter() - gt_start:.1f}s")

    cfg = {
        "k": args.k,
        "m": args.m,
        "ef_construction": args.ef_construction,
        "ef_search_list": [int(x) for x in args.ef_search_list.split(",") if x],
        "ultrasql_effective_ef": args.ultrasql_effective_ef,
        "ultrasql_dsn": args.ultrasql_dsn,
        "pg_dsn": args.pg_dsn,
        "qdrant_url": args.qdrant_url,
        "lancedb_dir": args.lancedb_dir,
        "lancedb_nprobes": args.lancedb_nprobes,
        "lancedb_refine_list": [int(x) for x in args.lancedb_refine_list.split(",") if x],
    }

    host = host_descriptor()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    n_label = f"{n // 1000}k" if n % 1000 == 0 and n < 1_000_000 else (f"{n // 1_000_000}m" if n % 1_000_000 == 0 else str(n))
    results = {}
    for name in (e for e in args.engines.split(",") if e):
        adapter = ADAPTERS.get(name)
        if adapter is None:
            print(f"  {name}: unknown engine, skipping")
            continue
        print(f"--- {name} ---")
        t0 = time.perf_counter()
        res = adapter(base, queries, truth, cfg)
        res["dataset"] = args.dataset_name
        res["n_base"] = n
        res["dims"] = dim
        res["n_queries"] = len(queries)
        res["k"] = args.k
        res["metric"] = "l2"
        res["host"] = host
        res["wall_seconds"] = round(time.perf_counter() - t0, 2)
        out = args.out_dir / f"vector_ann_{args.dataset_name}_{n_label}_k{args.k}-{res['engine']}.json"
        out.write_text(json.dumps(res, indent=2, sort_keys=True) + "\n")
        results[res["engine"]] = res
        if res.get("status") == "measured":
            for p in res["points"]:
                if "recall_at_k_mean" in p:
                    print(
                        f"  ef={p['ef_search']}: recall@{args.k}={p['recall_at_k_mean']:.4f} "
                        f"p50={p['p50_latency_us']:.0f}us p95={p['p95_latency_us']:.0f}us"
                    )
        else:
            print(f"  status={res.get('status')} reason={res.get('reason')}")

    # Matched-point comparison: every engine at ef_search == ultrasql_effective_ef.
    matched = {}
    target_ef = args.ultrasql_effective_ef
    for engine, res in results.items():
        if res.get("status") != "measured":
            matched[engine] = {"status": res.get("status"), "reason": res.get("reason")}
            continue
        candidates = [p for p in res["points"] if "recall_at_k_mean" in p]
        chosen = None
        # Engines with an ef_search knob: compare at the same ef as UltraSQL.
        for p in candidates:
            if p.get("ef_search") == target_ef:
                chosen = p
                break
        if chosen is None and candidates:
            # No matching ef (e.g. LanceDB tunes via refine_factor): pick the
            # lowest-latency point that still reaches high recall (>= 0.95), so
            # the engine is represented at a fair high-recall operating point
            # rather than its weakest one; fall back to its best recall.
            high_recall = [p for p in candidates if p["recall_at_k_mean"] >= 0.95]
            if high_recall:
                chosen = min(high_recall, key=lambda p: p["p50_latency_us"])
            else:
                chosen = max(candidates, key=lambda p: p["recall_at_k_mean"])
        if chosen:
            matched[engine] = {
                "ef_search": chosen.get("ef_search"),
                "refine_factor": chosen.get("refine_factor"),
                "recall_at_k_mean": chosen["recall_at_k_mean"],
                "p50_latency_us": chosen["p50_latency_us"],
                "p95_latency_us": chosen["p95_latency_us"],
                "p99_latency_us": chosen["p99_latency_us"],
                "qps_p50": 1_000_000.0 / chosen["p50_latency_us"] if chosen["p50_latency_us"] else None,
            }

    comparison = {
        "schema_version": 1,
        "suite": "vector_ann_recall_latency_same_host",
        "dataset": args.dataset_name,
        "n_base": n,
        "dims": dim,
        "n_queries": len(queries),
        "k": args.k,
        "metric": "l2",
        "matched_operating_point_ef_search": target_ef,
        "hnsw_config": {"m": args.m, "ef_construction": args.ef_construction},
        "engines": {e: results[e].get("status") for e in results},
        "matched_point": matched,
        "host": host,
        "policy": (
            "Same base/query/ground-truth/k/metric (L2) for every engine. Recall@k "
            "is always paired with p50/p95/p99 latency. Competitors swept across "
            "ef_search; the matched point is ef_search = UltraSQL's effective value. "
            "Unavailable engines are recorded, never faked."
        ),
    }
    comp_out = args.out_dir / f"vector_ann_{args.dataset_name}_{n_label}_k{args.k}_comparison.json"
    comp_out.write_text(json.dumps(comparison, indent=2, sort_keys=True) + "\n")
    print(f"\nMatched point (ef_search={target_ef}):")
    for engine, m in matched.items():
        if "recall_at_k_mean" in m:
            print(
                f"  {engine:>22}: recall@{args.k}={m['recall_at_k_mean']:.4f} "
                f"p50={m['p50_latency_us']:.0f}us qps={m['qps_p50']:.0f}"
            )
        else:
            print(f"  {engine:>22}: {m.get('status')} ({m.get('reason')})")
    print(f"\nWrote comparison: {comp_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
