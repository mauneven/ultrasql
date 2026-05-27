#!/usr/bin/env python3
"""Render scale-sweep raw benchmark artifacts.

The input directory contains one JSON file per workload and engine. This
renderer keeps the public README table honest by copying only measured
artifacts into a compact scale table; missing engines stay explicit.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


ENGINE_ORDER = ["ultrasql", "duckdb", "sqlite3", "postgres17"]
ENGINE_LABELS = {
    "ultrasql": "UltraSQL",
    "duckdb": "DuckDB",
    "sqlite3": "SQLite",
    "postgres17": "PostgreSQL",
}
WORKLOAD_ORDER = [
    "insert_throughput",
    "select_scan",
    "select_sum",
    "select_avg",
    "filter_sum",
    "update_throughput",
    "delete_throughput",
    "mixed_oltp_pgbench_like",
    "window_row_number",
]
WORKLOAD_LABELS = {
    "insert_throughput": "INSERT throughput",
    "select_scan": "SELECT scan",
    "select_sum": "SELECT SUM(x)",
    "select_avg": "SELECT AVG(x)",
    "filter_sum": "Filter + SUM",
    "update_throughput": "UPDATE throughput",
    "delete_throughput": "DELETE throughput",
    "mixed_oltp_pgbench_like": "Mixed OLTP",
    "window_row_number": "Window row_number()",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, required=True)
    parser.add_argument("--output-md", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument(
        "--title",
        default="Release-artifact scale sweep",
        help="Markdown heading to render",
    )
    parser.add_argument(
        "--note",
        default=(
            "UltraSQL is an external release binary launched as ultrasqld; "
            "competitors use installed local clients on the same host."
        ),
    )
    return parser.parse_args()


def workload_family(workload: str) -> str | None:
    for family in WORKLOAD_ORDER:
        if workload == family or workload.startswith(f"{family}_"):
            return family
    return None


def format_rows(rows: int) -> str:
    return f"{rows:,}".replace(",", " ")


def format_duration(us: float | None, family: str) -> str:
    if us is None:
        return "-"
    suffix = "/op" if family == "mixed_oltp_pgbench_like" else ""
    if us < 1000.0:
        return f"{us:.2f} µs{suffix}"
    return f"{us / 1000.0:.2f} ms{suffix}"


def load_raw(raw_dir: Path) -> list[dict]:
    records = []
    for path in sorted(raw_dir.glob("*.json")):
        try:
            doc = json.loads(path.read_text())
        except json.JSONDecodeError as exc:
            raise SystemExit(f"malformed JSON: {path}: {exc}") from exc
        if doc.get("status") == "not_available":
            continue
        if "median_us" not in doc or "engine" not in doc or "workload" not in doc:
            continue
        family = workload_family(str(doc["workload"]))
        if family is None:
            continue
        records.append(
            {
                "engine": str(doc["engine"]),
                "workload": str(doc["workload"]),
                "family": family,
                "n_rows": int(doc.get("n_rows", 0)),
                "median_us": float(doc["median_us"]),
                "samples": int(doc.get("samples", 0)),
                "server_mode": doc.get("server_mode"),
                "path": str(path),
            }
        )
    return records


def normalize(records: list[dict]) -> list[dict]:
    by_key: dict[tuple[str, int], dict[str, dict]] = {}
    for record in records:
        key = (record["family"], record["n_rows"])
        by_key.setdefault(key, {})[record["engine"]] = record

    rows = []
    order = {name: index for index, name in enumerate(WORKLOAD_ORDER)}
    for (family, n_rows), engines in sorted(
        by_key.items(), key=lambda item: (order[item[0][0]], item[0][1])
    ):
        measured = [record for record in engines.values() if record["median_us"] > 0.0]
        fastest = min(measured, key=lambda record: record["median_us"]) if measured else None
        rows.append(
            {
                "workload": family,
                "workload_label": WORKLOAD_LABELS[family],
                "n_rows": n_rows,
                "engines": engines,
                "fastest_engine": fastest["engine"] if fastest else None,
                "fastest_median_us": fastest["median_us"] if fastest else None,
            }
        )
    return rows


def render_markdown(title: str, note: str, rows: list[dict]) -> str:
    lines = [
        f"## {title}",
        "",
        note,
        "",
        "| Workload | Rows | UltraSQL | DuckDB | SQLite | PostgreSQL | Fastest |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        cells = []
        for engine in ENGINE_ORDER:
            value = row["engines"].get(engine)
            formatted = format_duration(value["median_us"] if value else None, row["workload"])
            if engine == row["fastest_engine"]:
                formatted = f"**{formatted}**"
            cells.append(formatted)
        fastest = (
            ENGINE_LABELS.get(row["fastest_engine"], row["fastest_engine"])
            if row["fastest_engine"]
            else "-"
        )
        lines.append(
            "| {workload} | {rows} | {ultrasql} | {duckdb} | {sqlite} | {postgres} | {fastest} |".format(
                workload=row["workload_label"],
                rows=format_rows(row["n_rows"]),
                ultrasql=cells[0],
                duckdb=cells[1],
                sqlite=cells[2],
                postgres=cells[3],
                fastest=fastest,
            )
        )
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    records = load_raw(args.raw_dir)
    rows = normalize(records)
    payload = {
        "schema_version": 1,
        "raw_dir": str(args.raw_dir),
        "rows": rows,
        "policy": "Only measured raw artifacts are rendered; missing engines are not ranked.",
    }
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_md.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    args.output_md.write_text(render_markdown(args.title, args.note, rows))


if __name__ == "__main__":
    main()
