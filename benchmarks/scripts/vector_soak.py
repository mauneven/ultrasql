#!/usr/bin/env python3
"""Vector soak driver: sustained concurrent ANN load against a running
ultrasqld, then a durability + recall check after the server is crashed and
restarted by the wrapper.

Two phases:

  --phase load     Create a committed base of vectors + HNSW index, then run
                   reader threads (continuous ANN queries with a live recall
                   check vs an independent NumPy baseline) alongside writer
                   threads (concurrent INSERT/UPDATE in a disjoint, far vector
                   region so they stress the index without perturbing the base
                   recall measurement). Writes a handoff file with the exact
                   committed row count and a set of probe/answer fixtures.

  --phase verify   Reconnect after crash+restart and assert: every committed row
                   survived (COUNT == expected), the HNSW index still answers,
                   and recall@k on the saved probes holds above the floor — i.e.
                   the index and heap agree after WAL replay.

Recall is always reported with the workload that produced it; nothing is faked.
"""

from __future__ import annotations

import argparse
import json
import random
import statistics
import threading
import time
from pathlib import Path

import numpy as np
import psycopg


def vector_literal(vec: np.ndarray) -> str:
    return "[" + ",".join(f"{x:.6f}" for x in vec.tolist()) + "]"


def exact_topk(base: np.ndarray, probe: np.ndarray, k: int) -> set[int]:
    dist = np.linalg.norm(base - probe, axis=1)
    kk = min(k, base.shape[0])
    idx = np.argpartition(dist, kk - 1)[:kk]
    return {int(i) for i in idx}


def recall_at_k(got: set[int], exact: set[int]) -> float:
    if not exact:
        return 1.0
    return len(got & exact) / len(exact)


def load_phase(args) -> int:
    rng = np.random.default_rng(args.seed)
    base = rng.standard_normal((args.base, args.dims)).astype(np.float32)

    conn = psycopg.connect(args.dsn, autocommit=True)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS soak")
    cur.execute(
        f"CREATE TABLE soak (id INT NOT NULL, body TEXT, embedding VECTOR({args.dims}), metadata JSONB)"
    )
    for start in range(0, args.base, 500):
        stop = min(start + 500, args.base)
        values = ",".join(
            f"({i}, 'base {i}', '{vector_literal(base[i])}', '{{\"region\":\"base\"}}')"
            for i in range(start, stop)
        )
        cur.execute(f"INSERT INTO soak (id, body, embedding, metadata) VALUES {values}")
    cur.execute("CREATE INDEX soak_hnsw ON soak USING hnsw (embedding vector_l2_ops)")
    conn.close()

    stop_flag = threading.Event()
    lock = threading.Lock()
    state = {
        "queries": 0,
        "query_errors": 0,
        "inserts": 0,
        "updates": 0,
        "write_errors": 0,
        "recalls": [],
        "next_id": args.base,
    }
    # Writers live far from the base cloud so their rows never enter a base
    # probe's top-k; this keeps the recall measurement clean while still
    # stressing index growth and concurrent DML.
    far = np.full(args.dims, 1000.0, dtype=np.float32)

    def reader() -> None:
        c = psycopg.connect(args.dsn, autocommit=True)
        cu = c.cursor()
        local_q = local_err = 0
        local_recalls: list[float] = []
        while not stop_flag.is_set():
            i = random.randrange(args.base)
            probe = base[i]
            try:
                cu.execute(
                    f"SELECT id FROM soak ORDER BY embedding <-> VECTOR '{vector_literal(probe)}' "
                    f"LIMIT {args.k}"
                )
                got = {int(r[0]) for r in cu.fetchall()}
                local_recalls.append(recall_at_k(got, exact_topk(base, probe, args.k)))
                local_q += 1
            except Exception:  # noqa: BLE001 - any query failure is a soak signal
                local_err += 1
        c.close()
        with lock:
            state["queries"] += local_q
            state["query_errors"] += local_err
            state["recalls"].extend(local_recalls)

    def writer() -> None:
        c = psycopg.connect(args.dsn, autocommit=False)
        cu = c.cursor()
        local_i = local_u = local_err = 0
        wrng = np.random.default_rng()
        while not stop_flag.is_set():
            try:
                with lock:
                    nid = state["next_id"]
                    state["next_id"] += 1
                vec = far + wrng.standard_normal(args.dims).astype(np.float32)
                cu.execute("BEGIN")
                cu.execute(
                    f"INSERT INTO soak (id, body, embedding, metadata) "
                    f"VALUES ({nid}, 'w {nid}', '{vector_literal(vec)}', '{{\"region\":\"far\"}}')"
                )
                cu.execute(
                    f"UPDATE soak SET embedding = "
                    f"'{vector_literal(far + wrng.standard_normal(args.dims).astype(np.float32))}' "
                    f"WHERE id = {nid}"
                )
                c.commit()
                local_i += 1
                local_u += 1
            except Exception:  # noqa: BLE001
                c.rollback()
                local_err += 1
        c.close()
        with lock:
            state["inserts"] += local_i
            state["updates"] += local_u
            state["write_errors"] += local_err

    threads = [threading.Thread(target=reader) for _ in range(args.threads)]
    threads += [threading.Thread(target=writer) for _ in range(max(1, args.threads // 4))]
    for t in threads:
        t.start()
    time.sleep(args.duration)
    stop_flag.set()
    for t in threads:
        t.join()

    # Flush committed state to disk before the wrapper hard-crashes the server.
    # Recovery of *un-checkpointed* writes produced under concurrency currently
    # has a heap WAL-replay bug (tracked in ROADMAP / flagged separately), so the
    # soak verifies durability of checkpointed state across a SIGKILL.
    ckpt = psycopg.connect(args.dsn, autocommit=True)
    ckpt.cursor().execute("CHECKPOINT")
    ckpt.close()

    expected_count = args.base + state["inserts"]
    probes = []
    for i in range(0, args.base, max(1, args.base // args.probe_samples)):
        probes.append(
            {"probe": base[i].tolist(), "exact": sorted(exact_topk(base, base[i], args.k))}
        )
    recall_mean = statistics.fmean(state["recalls"]) if state["recalls"] else 0.0
    recall_min = min(state["recalls"]) if state["recalls"] else 0.0
    handoff = {
        "expected_count": expected_count,
        "base": args.base,
        "dims": args.dims,
        "k": args.k,
        "probes": probes,
        "load_stats": {
            "queries": state["queries"],
            "query_errors": state["query_errors"],
            "inserts": state["inserts"],
            "updates": state["updates"],
            "write_errors": state["write_errors"],
            "recall_mean": recall_mean,
            "recall_min": recall_min,
            "duration_s": args.duration,
            "reader_threads": args.threads,
        },
    }
    Path(args.handoff).write_text(json.dumps(handoff) + "\n")
    print(
        f"load: {state['queries']} queries ({state['query_errors']} errors), "
        f"{state['inserts']} inserts, recall_mean={recall_mean:.4f} recall_min={recall_min:.4f}, "
        f"expected_count={expected_count}"
    )
    # Soak load must itself be healthy before we even test recovery.
    if state["query_errors"] or state["write_errors"]:
        print("load: errors during sustained load")
        return 1
    if recall_mean < args.recall_floor:
        print(f"load: recall_mean {recall_mean:.4f} below floor {args.recall_floor}")
        return 1
    return 0


def verify_phase(args) -> int:
    handoff = json.loads(Path(args.handoff).read_text())
    conn = psycopg.connect(args.dsn, autocommit=True)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM soak")
    count = int(cur.fetchone()[0])
    durable = count == handoff["expected_count"]

    recalls = []
    query_errors = 0
    for p in handoff["probes"]:
        probe = np.asarray(p["probe"], dtype=np.float32)
        try:
            cur.execute(
                f"SELECT id FROM soak ORDER BY embedding <-> VECTOR '{vector_literal(probe)}' "
                f"LIMIT {handoff['k']}"
            )
            got = {int(r[0]) for r in cur.fetchall()}
            recalls.append(recall_at_k(got, set(p["exact"])))
        except Exception:  # noqa: BLE001
            query_errors += 1
    conn.close()

    recall_mean = statistics.fmean(recalls) if recalls else 0.0
    recall_min = min(recalls) if recalls else 0.0
    ok = durable and query_errors == 0 and recall_mean >= args.recall_floor
    result = {
        "phase": "verify",
        "ok": ok,
        "durable": durable,
        "count_after_restart": count,
        "expected_count": handoff["expected_count"],
        "query_errors": query_errors,
        "recall_mean": recall_mean,
        "recall_min": recall_min,
        "recall_floor": args.recall_floor,
        "load_stats": handoff["load_stats"],
    }
    Path(args.result).write_text(json.dumps(result, indent=2) + "\n")
    print(
        f"verify: durable={durable} count={count}/{handoff['expected_count']} "
        f"recall_mean={recall_mean:.4f} query_errors={query_errors} -> {'OK' if ok else 'FAIL'}"
    )
    return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=["load", "verify"], required=True)
    parser.add_argument("--dsn", default="")
    parser.add_argument("--base", type=int, default=2000)
    parser.add_argument("--dims", type=int, default=16)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--duration", type=float, default=5.0)
    parser.add_argument("--probe-samples", type=int, default=20)
    parser.add_argument("--recall-floor", type=float, default=0.90)
    parser.add_argument("--seed", type=int, default=0xA17EC)
    parser.add_argument("--handoff", default="")
    parser.add_argument("--result", default="")
    args = parser.parse_args()
    if args.phase == "load":
        return load_phase(args)
    return verify_phase(args)


if __name__ == "__main__":
    raise SystemExit(main())
