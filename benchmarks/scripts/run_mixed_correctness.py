#!/usr/bin/env python3
"""Run the deterministic mixed correctness benchmark for one engine."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path


WARMUP_ITERS = 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", required=True)
    parser.add_argument("--workload", required=True)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--iters", type=int, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--pg-user")
    parser.add_argument("--pg-database")
    parser.add_argument("--ch-port", type=int, default=19000)
    parser.add_argument(
        "--storage-mode",
        choices=["memory", "data-dir"],
        default="memory",
        help="storage profile to measure honestly",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("benchmarks/results/latest/scale-sweep/data-dirs/competitors"),
        help="root directory for data-dir mode engine files",
    )
    return parser.parse_args()


def row_values(row_id: int) -> tuple[int, int]:
    val = ((row_id * 17) % 1000) - 500
    return row_id, val


def base_rows(n_rows: int) -> list[tuple[int, int]]:
    return [row_values(row_id) for row_id in range(n_rows)]


def insert_rows(n_rows: int) -> list[tuple[int, int]]:
    return [row_values(n_rows)]


def values_sql(rows: list[tuple[int, ...]]) -> str:
    return ",".join(f"({','.join(str(value) for value in row)})" for row in rows)


def query_sql(table: str) -> str:
    return f"SELECT SUM(val) FROM {table} WHERE id >= 0"


def mutation_sql(state_table: str, fact_table: str, n_rows: int) -> tuple[str, str, str]:
    update = f"UPDATE {state_table} SET val = val + 7 WHERE id = 0"
    insert = f"INSERT INTO {state_table} VALUES {values_sql(insert_rows(n_rows))}"
    return update, insert, query_sql(fact_table)


def normalize_answer(rows: list[tuple[object, ...]]) -> list[list[str]]:
    return [[str(value) for value in row] for row in rows]


def answer_sha256(answer: list[list[str]]) -> str:
    data = json.dumps(answer, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(data).hexdigest()


def db_path_for(engine: str, storage_mode: str, data_root: Path, workload: str) -> Path:
    if storage_mode == "memory":
        return Path(":memory:")
    suffix = ".duckdb" if engine == "duckdb" else ".sqlite3"
    db_path = data_root / engine / f"{workload}-{os.getpid()}{suffix}"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    stale_suffixes = ("", ".wal") if engine == "duckdb" else ("", "-wal", "-shm")
    for suffix in stale_suffixes:
        candidate = Path(f"{db_path}{suffix}")
        if candidate.exists():
            candidate.unlink()
    return db_path


def run_duckdb(
    n_rows: int,
    n_iters: int,
    storage_mode: str,
    data_root: Path,
    workload: str,
) -> tuple[list[float], list[list[str]]]:
    import duckdb

    fact_table = "bench_mixed_correctness_fact"
    state_table = "bench_mixed_correctness_state"
    db_path = db_path_for("duckdb", storage_mode, data_root, workload)
    con = duckdb.connect(str(db_path))
    con.execute(
        f"CREATE TABLE {fact_table} ("
        "id BIGINT NOT NULL, val BIGINT NOT NULL)"
    )
    con.execute(
        f"CREATE TABLE {state_table} ("
        "id BIGINT NOT NULL, val BIGINT NOT NULL)"
    )
    con.executemany(f"INSERT INTO {fact_table} VALUES (?, ?)", base_rows(n_rows))
    con.executemany(f"INSERT INTO {state_table} VALUES (?, ?)", base_rows(16))
    update, insert, fact_query = mutation_sql(state_table, fact_table, n_rows)
    samples: list[float] = []
    answer: list[list[str]] = []
    for ix in range(WARMUP_ITERS + n_iters):
        con.execute("BEGIN TRANSACTION")
        started = time.perf_counter()
        con.execute(insert)
        con.execute(update)
        rows = con.execute(fact_query).fetchall()
        elapsed_us = (time.perf_counter() - started) * 1e6
        con.execute("ROLLBACK")
        if ix >= WARMUP_ITERS:
            samples.append(elapsed_us)
            answer = normalize_answer(rows)
    return samples, answer


def run_sqlite(
    n_rows: int,
    n_iters: int,
    storage_mode: str,
    data_root: Path,
    workload: str,
) -> tuple[list[float], list[list[str]]]:
    import sqlite3

    fact_table = "bench_mixed_correctness_fact"
    state_table = "bench_mixed_correctness_state"
    db_path = db_path_for("sqlite3", storage_mode, data_root, workload)
    con = sqlite3.connect(str(db_path), isolation_level=None)
    if storage_mode == "data-dir":
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=FULL")
        con.execute("PRAGMA temp_store=DEFAULT")
    else:
        con.execute("PRAGMA journal_mode=MEMORY")
        con.execute("PRAGMA synchronous=OFF")
        con.execute("PRAGMA temp_store=MEMORY")
    con.execute(
        f"CREATE TABLE {fact_table} ("
        "id INTEGER NOT NULL, val INTEGER NOT NULL)"
    )
    con.execute(
        f"CREATE TABLE {state_table} ("
        "id INTEGER NOT NULL, val INTEGER NOT NULL)"
    )
    con.execute("BEGIN")
    con.executemany(f"INSERT INTO {fact_table} VALUES (?, ?)", base_rows(n_rows))
    con.executemany(f"INSERT INTO {state_table} VALUES (?, ?)", base_rows(16))
    con.execute("COMMIT")
    update, insert, fact_query = mutation_sql(state_table, fact_table, n_rows)
    samples: list[float] = []
    answer: list[list[str]] = []
    for ix in range(WARMUP_ITERS + n_iters):
        con.execute("BEGIN")
        started = time.perf_counter()
        con.execute(insert)
        con.execute(update)
        rows = con.execute(fact_query).fetchall()
        elapsed_us = (time.perf_counter() - started) * 1e6
        con.execute("ROLLBACK")
        if ix >= WARMUP_ITERS:
            samples.append(elapsed_us)
            answer = normalize_answer(rows)
    return samples, answer


def run_postgres(
    n_rows: int,
    n_iters: int,
    pg_user: str,
    pg_database: str,
    storage_mode: str,
) -> tuple[list[float], list[list[str]]]:
    try:
        import psycopg
    except ImportError:
        return run_postgres_psql(n_rows, n_iters, pg_user, pg_database, storage_mode)

    fact_table = "bench_mixed_correctness_fact"
    state_table = "bench_mixed_correctness_state"
    table_kind = "TABLE" if storage_mode == "data-dir" else "UNLOGGED TABLE"
    setup_sql = (
        f"DROP TABLE IF EXISTS {fact_table}; "
        f"DROP TABLE IF EXISTS {state_table}; "
        f"CREATE {table_kind} {fact_table} ("
        "id BIGINT NOT NULL, val BIGINT NOT NULL); "
        f"CREATE {table_kind} {state_table} ("
        "id BIGINT NOT NULL, val BIGINT NOT NULL); "
        f"INSERT INTO {fact_table} VALUES {values_sql(base_rows(n_rows))}; "
        f"INSERT INTO {state_table} VALUES {values_sql(base_rows(16))};"
    )
    update, insert, fact_query = mutation_sql(state_table, fact_table, n_rows)
    samples: list[float] = []
    answer: list[list[str]] = []
    with psycopg.connect(user=pg_user, dbname=pg_database, autocommit=True) as con:
        con.execute(setup_sql)
        for ix in range(WARMUP_ITERS + n_iters):
            con.execute("BEGIN")
            started = time.perf_counter()
            con.execute(insert)
            con.execute(update)
            rows = con.execute(fact_query).fetchall()
            elapsed_us = (time.perf_counter() - started) * 1e6
            con.execute("ROLLBACK")
            if ix >= WARMUP_ITERS:
                samples.append(elapsed_us)
                answer = normalize_answer(rows)
    return samples, answer


def run_postgres_psql(
    n_rows: int,
    n_iters: int,
    pg_user: str,
    pg_database: str,
    storage_mode: str,
) -> tuple[list[float], list[list[str]]]:
    fact_table = "bench_mixed_correctness_fact"
    state_table = "bench_mixed_correctness_state"
    psql = [
        "psql",
        "-U",
        pg_user,
        "-d",
        pg_database,
        "-q",
        "--no-align",
        "-t",
        "-F",
        "|",
        "-v",
        "ON_ERROR_STOP=1",
    ]
    table_kind = "TABLE" if storage_mode == "data-dir" else "UNLOGGED TABLE"
    setup_sql = (
        f"DROP TABLE IF EXISTS {fact_table}; "
        f"DROP TABLE IF EXISTS {state_table}; "
        f"CREATE {table_kind} {fact_table} ("
        "id BIGINT NOT NULL, val BIGINT NOT NULL); "
        f"CREATE {table_kind} {state_table} ("
        "id BIGINT NOT NULL, val BIGINT NOT NULL); "
        f"INSERT INTO {fact_table} VALUES {values_sql(base_rows(n_rows))}; "
        f"INSERT INTO {state_table} VALUES {values_sql(base_rows(16))};"
    )
    subprocess.run(psql, input=setup_sql, text=True, check=True, capture_output=True)
    update, insert, fact_query = mutation_sql(state_table, fact_table, n_rows)
    sample_sql = f"BEGIN; {insert}; {update}; {fact_query}; ROLLBACK;"
    samples: list[float] = []
    answer: list[list[str]] = []
    for ix in range(WARMUP_ITERS + n_iters):
        started = time.perf_counter()
        proc = subprocess.run(psql, input=sample_sql, text=True, check=True, capture_output=True)
        elapsed_us = (time.perf_counter() - started) * 1e6
        rows = [line.split("|") for line in proc.stdout.splitlines() if line.strip()]
        if ix >= WARMUP_ITERS:
            samples.append(elapsed_us)
            answer = rows
    return samples, answer


def run_clickhouse(n_rows: int, n_iters: int, ch_port: int) -> tuple[list[float], list[list[str]]]:
    from clickhouse_driver import Client

    fact_table = "bench_mixed_correctness_fact"
    state_table = "bench_mixed_correctness_state"
    client = Client(host="127.0.0.1", port=ch_port, settings={"mutations_sync": 2})
    update, insert, fact_query = mutation_sql(state_table, fact_table, n_rows)
    samples: list[float] = []
    answer: list[list[str]] = []
    rows = base_rows(n_rows)
    for ix in range(WARMUP_ITERS + n_iters):
        client.execute(f"DROP TABLE IF EXISTS {fact_table} SYNC")
        client.execute(f"DROP TABLE IF EXISTS {state_table} SYNC")
        client.execute(
            f"CREATE TABLE {fact_table} ("
            "id Int64, val Int64) "
            "ENGINE = MergeTree() ORDER BY id"
        )
        client.execute(
            f"CREATE TABLE {state_table} ("
            "id Int64, val Int64) "
            "ENGINE = MergeTree() ORDER BY id"
        )
        client.execute(f"INSERT INTO {fact_table} VALUES", rows)
        client.execute(f"INSERT INTO {state_table} VALUES", base_rows(16))
        started = time.perf_counter()
        client.execute(f"INSERT INTO {state_table} VALUES", insert_rows(n_rows))
        client.execute(
            f"ALTER TABLE {state_table} UPDATE val = val + 7 "
            "WHERE id = 0"
        )
        result = client.execute(fact_query)
        elapsed_us = (time.perf_counter() - started) * 1e6
        if ix >= WARMUP_ITERS:
            samples.append(elapsed_us)
            answer = normalize_answer(result)
    return samples, answer


def emit(args: argparse.Namespace, samples: list[float], answer: list[list[str]]) -> None:
    if not answer:
        raise SystemExit("mixed correctness produced empty answer")
    doc = {
        "schema_version": 1,
        "engine": args.engine,
        "workload": args.workload,
        "status": "measured",
        "n_rows": args.rows,
        "samples": len(samples),
        "median_us": statistics.median(samples),
        "min_us": min(samples),
        "iterations_us": samples,
        "answer": answer,
        "answer_sha256": answer_sha256(answer),
        "policy": "Raw measured samples only; answer_sha256 must match across measured engines before ranking.",
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(doc, sort_keys=True) + "\n")


def main() -> None:
    args = parse_args()
    if args.engine == "duckdb":
        samples, answer = run_duckdb(
            args.rows,
            args.iters,
            args.storage_mode,
            args.data_root,
            args.workload,
        )
    elif args.engine == "sqlite3":
        samples, answer = run_sqlite(
            args.rows,
            args.iters,
            args.storage_mode,
            args.data_root,
            args.workload,
        )
    elif args.engine in {"postgres", "postgres17"}:
        if not args.pg_user or not args.pg_database:
            raise SystemExit("--pg-user and --pg-database are required for postgres")
        samples, answer = run_postgres(
            args.rows,
            args.iters,
            args.pg_user,
            args.pg_database,
            args.storage_mode,
        )
    elif args.engine == "clickhouse":
        samples, answer = run_clickhouse(args.rows, args.iters, args.ch_port)
    else:
        raise SystemExit(f"unknown engine: {args.engine}")
    emit(args, samples, answer)
    print(f"median: {statistics.median(samples):.3f} us", file=sys.stderr)


if __name__ == "__main__":
    main()
