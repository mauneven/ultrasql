#!/usr/bin/env python3
"""Measure bulk UPDATE, bulk DELETE and SUM across several table shapes.

The scale sweep uses `(id INT, val INT)` tables only, and UltraSQL has
dedicated in-place paths for exactly that `(Int32, Int32)` layout. This
driver runs the same statements on other common shapes so the general
execution path is measured too:

    int4,int4        the scale-sweep shape
    int8,int8        same rows with BIGINT columns
    int4,int4,text   the scale-sweep shape plus a short TEXT column

For every shape and engine it loads `--rows` rows, then times
`UPDATE t SET val = val + 1 WHERE id < rows` and `DELETE FROM t WHERE id <
rows` inside BEGIN .. ROLLBACK (the rollback is outside the timer, as in the
scale sweep), and `SELECT SUM(val) FROM t`. It reports the median of
`--reps` samples in milliseconds.

Engines:
  ultrasql    an `ultrasqld` started by this script with a fresh --data-dir
  postgres    an existing server reached through --pg-dsn (psycopg 3)
  duckdb      in-process, file-backed database in a temporary directory

The UltraSQL and PostgreSQL legs use the same client library (psycopg 3),
so client overhead is symmetric between them; DuckDB runs in-process with
no client/server round trip. This is a same-host comparison of one
statement per sample, not a throughput benchmark.

Usage:
  benchmarks/scripts/schema_generality.py --ultrasqld target/release/ultrasqld \\
      --pg-dsn "host=127.0.0.1 port=55417 user=$USER dbname=ultrasql_bench" \\
      --rows 100000 --reps 7 --out schema_generality.json
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import socket
import statistics
import subprocess
import tempfile
import time
from pathlib import Path

SHAPES = {
    "int4,int4": ("id INT NOT NULL, val INT", lambda i: f"({i}, {i})"),
    "int8,int8": ("id BIGINT NOT NULL, val BIGINT", lambda i: f"({i}, {i})"),
    "int4,int4,text": ("id INT NOT NULL, val INT, note TEXT", lambda i: f"({i}, {i}, 'row-{i}')"),
}
CHUNK = 5000


def insert_chunks(n_rows: int, row) -> list[str]:
    return [
        ",".join(row(i) for i in range(start, min(n_rows, start + CHUNK)))
        for start in range(0, n_rows, CHUNK)
    ]


def median_ms(samples: list[float]) -> float:
    return statistics.median(samples) * 1e3


def time_server_engine(conn, n_rows: int, reps: int) -> dict[str, float]:
    out = {}
    for name, sql in (
        ("update", f"UPDATE t SET val = val + 1 WHERE id < {n_rows}"),
        ("delete", f"DELETE FROM t WHERE id < {n_rows}"),
    ):
        samples = []
        for _ in range(reps):
            conn.execute("BEGIN")
            started = time.perf_counter()
            conn.execute(sql)
            samples.append(time.perf_counter() - started)
            conn.execute("ROLLBACK")
        out[name] = median_ms(samples)
    samples = []
    for _ in range(reps):
        started = time.perf_counter()
        conn.execute("SELECT SUM(val) FROM t").fetchall()
        samples.append(time.perf_counter() - started)
    out["sum"] = median_ms(samples)
    return out


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def run_ultrasql(binary: str, n_rows: int, reps: int, scratch: Path) -> dict:
    import psycopg

    results = {}
    for shape, (ddl, row) in SHAPES.items():
        data_dir = Path(tempfile.mkdtemp(dir=scratch))
        port = free_port()
        env = {**os.environ, "ULTRASQL_RESULT_CACHE": "off"}
        proc = subprocess.Popen(
            [binary, "--listen", f"127.0.0.1:{port}", "--data-dir", str(data_dir), "--log-level", "warn"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
        )
        try:
            deadline = time.time() + 15
            while True:
                try:
                    socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
                    break
                except OSError:
                    if time.time() > deadline:
                        raise
                    time.sleep(0.05)
            with psycopg.connect(f"host=127.0.0.1 port={port} user=bench dbname=postgres", autocommit=True) as conn:
                conn.execute(f"CREATE TABLE t ({ddl})")
                for chunk in insert_chunks(n_rows, row):
                    conn.execute(f"INSERT INTO t VALUES {chunk}")
                results[shape] = time_server_engine(conn, n_rows, reps)
        finally:
            proc.terminate()
            proc.wait()
            shutil.rmtree(data_dir, ignore_errors=True)
    return results


def run_postgres(dsn: str, n_rows: int, reps: int) -> dict:
    import psycopg

    results = {}
    with psycopg.connect(dsn, autocommit=True) as conn:
        version = conn.execute("SHOW server_version").fetchone()[0]
        for shape, (ddl, row) in SHAPES.items():
            conn.execute("DROP TABLE IF EXISTS t")
            conn.execute(f"CREATE TABLE t ({ddl})")
            for chunk in insert_chunks(n_rows, row):
                conn.execute(f"INSERT INTO t VALUES {chunk}")
            conn.execute("VACUUM ANALYZE t")
            results[shape] = time_server_engine(conn, n_rows, reps)
            conn.execute("DROP TABLE t")
    return {"version": version, "shapes": results}


def run_duckdb(n_rows: int, reps: int, scratch: Path) -> dict:
    import duckdb

    results = {}
    for shape, (ddl, row) in SHAPES.items():
        directory = Path(tempfile.mkdtemp(dir=scratch))
        con = duckdb.connect(str(directory / "bench.duckdb"))
        try:
            con.execute(f"CREATE TABLE t ({ddl.replace(' NOT NULL', '')})")
            for chunk in insert_chunks(n_rows, row):
                con.execute(f"INSERT INTO t VALUES {chunk}")
            out = {}
            for name, sql in (
                ("update", f"UPDATE t SET val = val + 1 WHERE id < {n_rows}"),
                ("delete", f"DELETE FROM t WHERE id < {n_rows}"),
            ):
                samples = []
                for _ in range(reps):
                    con.execute("BEGIN TRANSACTION")
                    started = time.perf_counter()
                    con.execute(sql).fetchall()
                    samples.append(time.perf_counter() - started)
                    con.execute("ROLLBACK")
                out[name] = median_ms(samples)
            samples = []
            for _ in range(reps):
                started = time.perf_counter()
                con.execute("SELECT SUM(val) FROM t").fetchall()
                samples.append(time.perf_counter() - started)
            out["sum"] = median_ms(samples)
            results[shape] = out
        finally:
            con.close()
            shutil.rmtree(directory, ignore_errors=True)
    return {"version": duckdb.__version__, "shapes": results}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--ultrasqld", required=True)
    parser.add_argument("--pg-dsn", default=None)
    parser.add_argument("--no-duckdb", action="store_true")
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--reps", type=int, default=7)
    parser.add_argument("--scratch", default=None, help="directory for temporary data dirs")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    scratch = Path(args.scratch or tempfile.gettempdir())
    scratch.mkdir(parents=True, exist_ok=True)
    doc = {
        "schema_version": 1,
        "benchmark": "schema_generality",
        "n_rows": args.rows,
        "reps": args.reps,
        "unit": "ms (median)",
        "host": {"platform": platform.platform(), "cpu_count": os.cpu_count()},
        "policy": "Same-host single-statement medians; UPDATE/DELETE timed inside BEGIN, ROLLBACK untimed.",
        "engines": {"ultrasql": {"binary": args.ultrasqld, "shapes": run_ultrasql(args.ultrasqld, args.rows, args.reps, scratch)}},
    }
    if args.pg_dsn:
        doc["engines"]["postgres"] = run_postgres(args.pg_dsn, args.rows, args.reps)
    if not args.no_duckdb:
        doc["engines"]["duckdb"] = run_duckdb(args.rows, args.reps, scratch)
    args.out.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
    for engine, body in doc["engines"].items():
        for shape, values in body["shapes"].items():
            print(f"{shape:16s} {engine:9s} " + " ".join(f"{k}={v:.2f}" for k, v in sorted(values.items())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
