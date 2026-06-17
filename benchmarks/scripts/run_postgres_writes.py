#!/usr/bin/env python3
"""Measure scale-sweep workloads against PostgreSQL over a persistent connection.

This driver replaces the previous `psql -c` per-query methodology. Every timed
region runs against a single long-lived `psycopg` (v3) connection with
server-side prepared statements, so the measured median reflects PostgreSQL's
execution cost, not `psql` process startup + a fresh connection + re-parse +
re-plan on every sample.

Connection target is taken from the standard libpq environment
(`PGHOST`/`PGPORT`/`PGUSER`/`PGDATABASE`/`PGPASSWORD`) or an explicit
`--dsn`. Point those at a tuned PostgreSQL 17 cluster (see
`benchmarks/scripts/pg17_bench_server.sh`) for release-grade fairness.

The JSON envelope matches every other scale-sweep raw artifact:
schema_version, engine, status, n_rows, storage_mode, durability_mode,
samples, median_us, min_us, iterations_us, policy.
"""

from __future__ import annotations

import argparse
import json
import random
import statistics
import sys
import time
from pathlib import Path
from typing import Callable

try:
    import psycopg
except ImportError:  # pragma: no cover - reported as not_available below.
    psycopg = None


SEED = 0xC0FFEE


def row_suffix(rows: int) -> str:
    if rows == 65536:
        return "65k"
    if rows >= 1_000_000 and rows % 1_000_000 == 0:
        return f"{rows // 1_000_000}m"
    if rows >= 1000 and rows % 1000 == 0:
        return f"{rows // 1000}k"
    return str(rows)


def write_doc(out: Path, doc: dict) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(doc, sort_keys=True) + "\n")


def measured_doc(
    *,
    engine: str,
    version: str,
    workload: str,
    n_rows: int,
    storage_mode: str,
    durability_mode: str,
    samples: list[float],
    extra: dict | None = None,
) -> dict:
    doc = {
        "schema_version": 1,
        "engine": engine,
        "engine_version": version,
        "workload": workload,
        "status": "measured",
        "n_rows": int(n_rows),
        "storage_mode": storage_mode,
        "durability_mode": durability_mode,
        "samples": len(samples),
        "median_us": float(statistics.median(samples)) if samples else 0.0,
        "min_us": float(min(samples)) if samples else 0.0,
        "iterations_us": [float(s) for s in samples],
        "policy": "Raw measured samples only; no ranking or winner claim.",
    }
    if extra:
        doc.update(extra)
    return doc


def unavailable_doc(
    *, engine: str, workload: str, n_rows: int, storage_mode: str, durability_mode: str, reason: str
) -> dict:
    return {
        "schema_version": 1,
        "engine": engine,
        "status": "not_available",
        "workload": workload,
        "n_rows": int(n_rows),
        "storage_mode": storage_mode,
        "durability_mode": durability_mode,
        "reason": reason,
        "policy": "No PostgreSQL benchmark claim exists until this artifact records measured samples from the same scale-sweep run.",
    }


def shuffled_write_rows(n: int) -> list[tuple[int, int]]:
    rng = random.Random(SEED)
    ids = list(range(n))
    rng.shuffle(ids)
    vals = [rng.randint(-(2**31), 2**31 - 1) for _ in range(n)]
    return list(zip(ids, vals))


def analytical_rows(n: int) -> list[tuple[int, int]]:
    return [(j, j * 10) for j in range(n)]


def preload_write_table(conn, table: str, kind: str, rows: list[tuple[int, int]], pk: bool) -> None:
    pk_clause = " PRIMARY KEY" if pk else ""
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"CREATE {kind}TABLE {table} (id BIGINT NOT NULL{pk_clause}, val BIGINT)")
        with cur.copy(f"COPY {table} (id, val) FROM STDIN") as copy:
            for row in rows:
                copy.write_row(row)
    conn.commit()


def preload_int_table(conn, table: str, kind: str, col: str, rows: list[tuple[int, int]]) -> None:
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"CREATE {kind}TABLE {table} (id INT NOT NULL, {col} INT)")
        with cur.copy(f"COPY {table} (id, {col}) FROM STDIN") as copy:
            for row in rows:
                copy.write_row(row)
    conn.commit()


def time_samples(warmup: int, iters: int, body: Callable[[], None]) -> list[float]:
    samples: list[float] = []
    for i in range(warmup + iters):
        t0 = time.perf_counter()
        body()
        dt = (time.perf_counter() - t0) * 1e6
        if i >= warmup:
            samples.append(dt)
    return samples


def run_insert(conn, args, kind: str) -> list[float]:
    table = "bench_write"
    rows = shuffled_write_rows(args.rows)
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"CREATE {kind}TABLE {table} (id BIGINT NOT NULL, val BIGINT)")
    conn.commit()

    def load() -> None:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {table}")
            with cur.copy(f"COPY {table} (id, val) FROM STDIN") as copy:
                for row in rows:
                    copy.write_row(row)
        conn.commit()

    return time_samples(args.warmup, args.iters, load)


def run_update(conn, args, kind: str) -> list[float]:
    table = "bench_write"
    preload_write_table(conn, table, kind, shuffled_write_rows(args.rows), pk=False)
    n = args.rows
    query = f"UPDATE {table} SET val = val + 1 WHERE id BETWEEN 0 AND {n - 1}"

    def body() -> None:
        with conn.cursor() as cur:
            cur.execute(query, prepare=True)
        conn.rollback()

    return time_samples(args.warmup, args.iters, body)


def run_delete(conn, args, kind: str) -> list[float]:
    table = "bench_write"
    preload_write_table(conn, table, kind, shuffled_write_rows(args.rows), pk=False)
    n = args.rows
    query = f"DELETE FROM {table} WHERE id BETWEEN 0 AND {n - 1}"

    def body() -> None:
        with conn.cursor() as cur:
            cur.execute(query, prepare=True)
        conn.rollback()

    return time_samples(args.warmup, args.iters, body)


def run_select_scan(conn, args, kind: str) -> list[float]:
    table = "bench_select_scan"
    preload_int_table(conn, table, kind, "val", analytical_rows(args.rows))
    query = f"SELECT id, val FROM {table}"

    def body() -> None:
        with conn.cursor() as cur:
            cur.execute(query, prepare=True)
            got = cur.fetchall()
        if len(got) != args.rows:
            sys.stderr.write(f"run_select_scan: row mismatch (got {len(got)}, expected {args.rows})\n")

    return time_samples(args.warmup, args.iters, body)


def run_analytical(conn, args, kind: str, query: str) -> list[float]:
    table = "bench_analytical"
    preload_int_table(conn, table, kind, "x", analytical_rows(args.rows))

    def body() -> None:
        with conn.cursor() as cur:
            cur.execute(query, prepare=True)
            cur.fetchall()

    return time_samples(args.warmup, args.iters, body)


def run_mixed(conn, args, kind: str) -> list[float]:
    table = "bench_write"
    n = args.rows
    window = 1.0
    samples: list[float] = []
    for sample in range(args.warmup + args.iters):
        preload_write_table(conn, table, kind, shuffled_write_rows(n), pk=True)
        rng = random.Random(0xBEEF + sample)
        deadline = time.perf_counter() + window
        count = 0
        next_id = n
        with conn.cursor() as cur:
            while time.perf_counter() < deadline:
                r = rng.random()
                if r < 0.50:
                    row_id = rng.randint(0, n - 1)
                    cur.execute(
                        f"SELECT val FROM {table} WHERE id = %s", (row_id,), prepare=True
                    )
                    cur.fetchall()
                elif r < 0.80:
                    row_id = rng.randint(0, n - 1)
                    cur.execute(
                        f"UPDATE {table} SET val = val + 1 WHERE id = %s", (row_id,), prepare=True
                    )
                else:
                    new_val = rng.randint(-(2**31), 2**31 - 1)
                    cur.execute(
                        f"INSERT INTO {table} (id, val) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (next_id, new_val),
                        prepare=True,
                    )
                    next_id += 1
                count += 1
        conn.commit()
        elapsed = time.perf_counter() - (deadline - window)
        if sample >= args.warmup:
            samples.append(elapsed * 1e6 / max(count, 1))
    return samples


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workload", required=True)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--iters", type=int, default=8)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--storage-mode", choices=["memory", "data-dir"], default="data-dir")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--dsn", default=None, help="explicit psycopg conninfo; default uses PG* env")
    args = parser.parse_args()

    storage_mode = args.storage_mode
    durability_mode = "durable" if storage_mode == "data-dir" else "volatile"
    kind = "" if storage_mode == "data-dir" else "UNLOGGED "

    if psycopg is None:
        write_doc(
            args.out,
            unavailable_doc(
                engine="postgres",
                workload=args.workload,
                n_rows=args.rows,
                storage_mode=storage_mode,
                durability_mode=durability_mode,
                reason="python psycopg module not installed",
            ),
        )
        return 0

    try:
        conn = psycopg.connect(args.dsn or "", autocommit=False, connect_timeout=10)
    except Exception as err:  # noqa: BLE001 - connection failure is reported as not_available.
        write_doc(
            args.out,
            unavailable_doc(
                engine="postgres",
                workload=args.workload,
                n_rows=args.rows,
                storage_mode=storage_mode,
                durability_mode=durability_mode,
                reason=f"cannot connect to PostgreSQL: {err}",
            ),
        )
        return 0

    conn.prepare_threshold = 0
    with conn.cursor() as cur:
        cur.execute("SHOW server_version")
        row = cur.fetchone()
        version = f"PostgreSQL {row[0]}" if row else "PostgreSQL unknown"
        if storage_mode != "data-dir":
            cur.execute("SET synchronous_commit = off")
    conn.commit()

    wl = args.workload
    extra: dict | None = None
    try:
        if wl.startswith("insert_throughput"):
            samples = run_insert(conn, args, kind)
        elif wl.startswith("update_throughput"):
            samples = run_update(conn, args, kind)
        elif wl.startswith("delete_throughput"):
            samples = run_delete(conn, args, kind)
        elif wl == "mixed_oltp_pgbench_like":
            samples = run_mixed(conn, args, kind)
        elif wl.startswith("select_scan"):
            samples = run_select_scan(conn, args, kind)
        elif wl.startswith("select_sum"):
            samples = run_analytical(conn, args, kind, "SELECT SUM(x) FROM bench_analytical")
        elif wl.startswith("select_avg"):
            samples = run_analytical(conn, args, kind, "SELECT AVG(x) FROM bench_analytical")
        elif wl.startswith("filter_sum"):
            threshold = args.rows * 5
            samples = run_analytical(
                conn, args, kind, f"SELECT SUM(x) FROM bench_analytical WHERE x > {threshold}"
            )
        elif wl.startswith("window_row_number"):
            samples = run_analytical(
                conn,
                args,
                kind,
                "SELECT id, row_number() OVER (ORDER BY x) FROM bench_analytical",
            )
        else:
            write_doc(
                args.out,
                unavailable_doc(
                    engine="postgres",
                    workload=wl,
                    n_rows=args.rows,
                    storage_mode=storage_mode,
                    durability_mode=durability_mode,
                    reason=f"unknown workload {wl}",
                ),
            )
            return 0
    except Exception as err:  # noqa: BLE001 - measurement failure is reported, not raised.
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        write_doc(
            args.out,
            unavailable_doc(
                engine="postgres",
                workload=wl,
                n_rows=args.rows,
                storage_mode=storage_mode,
                durability_mode=durability_mode,
                reason=f"workload failed: {err}",
            ),
        )
        return 0
    finally:
        conn.close()

    write_doc(
        args.out,
        measured_doc(
            engine="postgres",
            version=version,
            workload=wl,
            n_rows=args.rows,
            storage_mode=storage_mode,
            durability_mode=durability_mode,
            samples=samples,
            extra=extra,
        ),
    )
    median = statistics.median(samples) if samples else 0.0
    print(f"  {wl}: median {median:.3f} µs ({len(samples)} samples)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
