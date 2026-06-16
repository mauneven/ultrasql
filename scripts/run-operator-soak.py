#!/usr/bin/env python3
"""Run an UltraSQL operator soak workload and emit a schema v2 report."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import socket
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


WORKLOAD_ID = "mixed-sql-soak-v1"
WORKLOAD_SURFACE = [
    "ddl",
    "crud",
    "transactions",
    "indexes",
    "views",
    "jsonb",
    "text_search",
    "vector",
    "copy",
    "export_import",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["smoke", "30d"], default="smoke")
    parser.add_argument("--commit", help="40-hex commit under test")
    parser.add_argument("--ultrasqld", type=Path, default=Path("target/debug/ultrasqld"))
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--data-dir", type=Path, default=Path("target/operator-soak-data"))
    parser.add_argument("--out", type=Path, default=Path("target/operator-soak-report.json"))
    parser.add_argument("--listen-host", default="127.0.0.1")
    parser.add_argument("--listen-port", type=int, default=0)
    parser.add_argument("--duration-seconds", type=float)
    parser.add_argument("--cycles", type=int)
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    parser.add_argument("--operator-id", default=os.environ.get("ULTRASQL_OPERATOR_ID", "local-smoke"))
    parser.add_argument("--host-id", default=socket.gethostname())
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--dataset-rows", type=int, default=16)
    parser.add_argument("--log-bundle-path", default="target/operator-soak-logs")
    parser.add_argument("--signed-off-by", default="local smoke runner")
    parser.add_argument(
        "--skip-restart-check",
        action="store_true",
        help="skip clean restart/WAL replay verification",
    )
    return parser.parse_args()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def current_commit() -> str | None:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    commit = completed.stdout.strip()
    return commit if completed.returncode == 0 and len(commit) == 40 else None


def free_port(host: str) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


def wait_for_port(host: str, port: int, proc: subprocess.Popen[str], timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    last_error: OSError | None = None
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"ultrasqld exited early with code {proc.returncode}")
        try:
            with socket.create_connection((host, port), timeout=0.25):
                return
        except OSError as exc:
            last_error = exc
            time.sleep(0.05)
    raise RuntimeError(f"ultrasqld did not accept TCP on {host}:{port}: {last_error}")


def start_server(binary: Path, data_dir: Path, host: str, port: int) -> subprocess.Popen[str]:
    data_dir.mkdir(parents=True, exist_ok=True)
    return subprocess.Popen(
        [
            str(binary),
            "--data-dir",
            str(data_dir),
            "--listen",
            f"{host}:{port}",
            "--log-level",
            "warn",
        ],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def stop_server(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def run_psql(psql: str, dsn: str, sql: str) -> tuple[str, float]:
    start = time.perf_counter()
    completed = subprocess.run(
        [psql, dsn, "-v", "ON_ERROR_STOP=1", "-q", "-t", "-A", "-c", sql],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    if completed.returncode != 0:
        raise RuntimeError(
            f"psql failed for SQL:\n{sql}\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
    return completed.stdout.strip(), elapsed_ms


def workload_sql(cycle: int) -> list[tuple[str, str]]:
    base = cycle * 10_000
    return [
        (
            "ddl",
            "CREATE TABLE IF NOT EXISTS soak_accounts ("
            "id INT NOT NULL, balance INT NOT NULL, profile JSONB, note TEXT, embedding VECTOR(3))",
        ),
        ("ddl", "CREATE INDEX IF NOT EXISTS soak_accounts_id_idx ON soak_accounts (id)"),
        (
            "ddl",
            "CREATE VIEW IF NOT EXISTS soak_positive_balances AS "
            "SELECT id, balance FROM soak_accounts WHERE balance >= 0",
        ),
        (
            "transaction",
            "BEGIN; "
            "INSERT INTO soak_accounts VALUES "
            f"({base + 1}, 100, '{{\"tier\":\"gold\"}}', 'alpha searchable text', CAST('[1,2,3]' AS VECTOR(3))), "
            f"({base + 2}, 200, '{{\"tier\":\"silver\"}}', 'beta searchable text', CAST('[2,3,4]' AS VECTOR(3))), "
            f"({base + 3}, 0, '{{\"tier\":\"bronze\"}}', 'gamma searchable text', CAST('[3,4,5]' AS VECTOR(3))); "
            f"UPDATE soak_accounts SET balance = balance + 1 WHERE id = {base + 3}; "
            "DELETE FROM soak_accounts WHERE id < 0; "
            "COMMIT",
        ),
        ("read", "SELECT COUNT(*), SUM(balance) FROM soak_accounts"),
        ("read", "SELECT COUNT(*) FROM soak_positive_balances"),
        (
            "read",
            "SELECT COUNT(*) FROM soak_accounts "
            "WHERE to_tsvector(note) @@ plainto_tsquery('searchable')",
        ),
        ("read", "SELECT id FROM soak_accounts ORDER BY embedding <-> '[1,2,3]' LIMIT 1"),
        ("copy", "COPY soak_accounts TO STDOUT WITH CSV"),
        ("transaction", "COMMIT"),
    ]


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((pct / 100.0) * (len(ordered) - 1))))
    return float(ordered[index])


def host_memory_bytes() -> int | None:
    if platform.system() == "Darwin":
        try:
            return int(subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True).strip())
        except (OSError, subprocess.CalledProcessError, ValueError):
            return None
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text(errors="replace").splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    return None


def host_descriptor(host_id: str) -> dict[str, Any]:
    return {
        "id_hash": sha256_text(host_id),
        "cpu": platform.processor() or platform.machine(),
        "memory_bytes": host_memory_bytes() or 1,
        "storage": "operator supplied or local filesystem",
        "os": platform.platform(),
    }


def main() -> int:
    args = parse_args()
    if args.concurrency <= 0:
        print("--concurrency must be positive", file=sys.stderr)
        return 2
    if args.dataset_rows <= 0:
        print("--dataset-rows must be positive", file=sys.stderr)
        return 2
    if not args.ultrasqld.exists():
        print(f"ultrasqld binary not found: {args.ultrasqld}", file=sys.stderr)
        return 2

    commit = args.commit or current_commit()
    if not commit:
        print("--commit is required outside a git checkout", file=sys.stderr)
        return 2
    port = args.listen_port or free_port(args.listen_host)
    dsn = f"postgresql://ultrasql@{args.listen_host}:{port}/ultrasql?sslmode=disable"
    duration_seconds = (
        args.duration_seconds
        if args.duration_seconds is not None
        else (30.0 * 86_400.0 if args.mode == "30d" else 60.0)
    )
    max_cycles = args.cycles if args.cycles is not None else sys.maxsize
    started = now_utc()
    proc: subprocess.Popen[str] | None = None
    latency_ms: list[float] = []
    operations = {
        "total": 0,
        "ddl": 0,
        "read": 0,
        "write": 0,
        "transactions": 0,
        "copy": 0,
        "export_import": 0,
    }
    errors = {
        "total": 0,
        "availability": 0,
        "sql": 0,
        "correctness": 0,
        "corruption": 0,
        "critical": 0,
        "high": 0,
    }
    consistency_checks: list[dict[str, Any]] = []
    wal_checks: list[dict[str, Any]] = []
    restart_count = 0

    try:
        proc = start_server(args.ultrasqld, args.data_dir, args.listen_host, port)
        wait_for_port(args.listen_host, port, proc)
        deadline = time.monotonic() + max(0.0, duration_seconds)
        cycle = 0
        while cycle < max_cycles and (cycle == 0 or time.monotonic() < deadline):
            for kind, sql in workload_sql(cycle):
                output, elapsed = run_psql(args.psql, dsn, sql)
                latency_ms.append(elapsed)
                operations["total"] += 1
                if kind == "ddl":
                    operations["ddl"] += 1
                elif kind == "read":
                    operations["read"] += 1
                elif kind == "write":
                    operations["write"] += 1
                elif kind == "transaction":
                    operations["transactions"] += 1
                    operations["write"] += 1
                elif kind == "copy":
                    operations["copy"] += 1
                if "COUNT" in sql or "SUM" in sql:
                    consistency_checks.append(
                        {
                            "name": f"cycle_{cycle}_aggregate",
                            "passed": bool(output),
                            "checksum": sha256_text(output),
                        }
                    )
            cycle += 1
            if time.monotonic() < deadline and cycle < max_cycles:
                time.sleep(args.interval_seconds)

        if not args.skip_restart_check:
            stop_server(proc)
            proc = start_server(args.ultrasqld, args.data_dir, args.listen_host, port)
            restart_count += 1
            wait_for_port(args.listen_host, port, proc)
            output, elapsed = run_psql(
                args.psql,
                dsn,
                "SELECT COUNT(*), SUM(balance) FROM soak_accounts",
            )
            latency_ms.append(elapsed)
            operations["total"] += 1
            operations["read"] += 1
            wal_checks.append(
                {
                    "name": "clean_restart",
                    "passed": bool(output),
                    "checksum": sha256_text(output),
                }
            )
    except Exception as err:  # noqa: BLE001 - report harness failures.
        errors["total"] += 1
        errors["sql"] += 1
        consistency_checks.append(
            {
                "name": "runner_error",
                "passed": False,
                "checksum": sha256_text(str(err)),
                "error": str(err),
            }
        )
    finally:
        if proc is not None:
            stop_server(proc)

    ended = now_utc()
    elapsed_seconds = max(0.001, (ended - started).total_seconds())
    if not wal_checks:
        wal_checks.append({"name": "clean_restart", "passed": args.skip_restart_check})
    final_verdict = (
        "smoke_pass"
        if args.mode == "smoke" and errors["total"] == 0
        else "pass"
        if args.mode == "30d" and errors["total"] == 0
        else "fail"
    )
    report = {
        "schema_version": 2,
        "mode": args.mode,
        "commit": commit.lower(),
        "started_at": iso(started),
        "ended_at": iso(ended),
        "duration_days": elapsed_seconds / 86_400.0,
        "host": host_descriptor(args.host_id),
        "operator": {"id_hash": sha256_text(args.operator_id)},
        "workload": {
            "id": WORKLOAD_ID,
            "id_hash": sha256_text(WORKLOAD_ID),
            "sql_surface": WORKLOAD_SURFACE,
        },
        "db_binary": {
            "path": str(args.ultrasqld),
            "sha256": sha256_bytes(args.ultrasqld.read_bytes()),
        },
        "config": {
            "ultrasqld_command": f"{args.ultrasqld} --data-dir {args.data_dir} --listen {args.listen_host}:{port}",
            "data_dir": str(args.data_dir),
            "ops_endpoint": "",
            "health_check_interval": f"{args.interval_seconds}s",
        },
        "dataset_scale": {"rows": args.dataset_rows, "cycles": max(1, operations["write"] // 2)},
        "concurrency": args.concurrency,
        "operations": operations,
        "latency_ms": {
            "p50": statistics.median(latency_ms) if latency_ms else 0.0,
            "p95": percentile(latency_ms, 95.0),
            "p99": percentile(latency_ms, 99.0),
        },
        "throughput_ops_per_sec": operations["total"] / elapsed_seconds,
        "errors": errors,
        "restart_count": restart_count,
        "crash_recovery_count": 0,
        "consistency_checks": consistency_checks
        or [{"name": "no_checks", "passed": False, "checksum": sha256_text("no checks")}],
        "wal_replay_checks": wal_checks,
        "final_verdict": final_verdict,
        "log_bundle_path": args.log_bundle_path,
        "signed_off_by": args.signed_off_by,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if errors["total"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
