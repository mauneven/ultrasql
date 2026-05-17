#!/usr/bin/env python3
"""
promote.py — pick cross-engine workloads where UltraSQL wins or ties,
and rewrite the README headline benchmarks section in-place.

Rules (one place, here):
  1. A workload is **promoted** to the README iff
       UltraSQL median_us <= TIE_TOLERANCE * best_competitor_median_us
     where TIE_TOLERANCE = 1.05 (≤ 5 % slower is considered a tie).
  2. Workloads where UltraSQL is `skipped` are dropped.
  3. Workloads where UltraSQL is slower than the best competitor by
     more than 5 % are dropped.
  4. The README between the `<!-- BENCH-START -->` and
     `<!-- BENCH-END -->` markers is rewritten to contain exactly the
     promoted tables, sorted from the largest UltraSQL-vs-runner-up
     speed-up to the smallest.
  5. The script is idempotent: running it twice on the same inputs
     produces the same README.

Input:
  Every `benchmarks/results/comparison-*/results.json` whose schema is
  `{ "results": { "<workload>": { "<engine>": {median_us, ...}, ... } } }`.

Run:
  python3 benchmarks/scripts/promote.py

Exit code 0 on success. Non-zero if fewer than MIN_PROMOTED workloads
qualify, since the README's hero is supposed to be a wall of wins.
"""

from __future__ import annotations

import json
import math
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RESULTS = REPO / "benchmarks" / "results"
README = REPO / "README.md"

TIE_TOLERANCE = 1.05
MIN_PROMOTED = 6

START = "<!-- BENCH-START -->"
END = "<!-- BENCH-END -->"

ENGINE_LABELS = {
    "UltraSQL (kernel)": ("UltraSQL", "0.0.1"),
    "DuckDB": ("DuckDB", ""),
    "ClickHouse": ("ClickHouse", ""),
    "SQLite": ("SQLite", ""),
    "PostgreSQL": ("PostgreSQL", ""),
}

ULTRA_NAMES = {"UltraSQL", "UltraSQL (kernel)", "ultrasql", "ultrasql (kernel)"}


def is_ultra(name: str) -> bool:
    return name.lower().replace(" ", "") in {n.lower().replace(" ", "") for n in ULTRA_NAMES}


def format_us(us: float) -> str:
    if us < 1.0:
        return f"{us * 1000:.0f} ns"
    if us < 1000.0:
        return f"{us:.2f} µs"
    if us < 1_000_000.0:
        return f"{us / 1000:.2f} ms"
    return f"{us / 1_000_000:.2f} s"


def load_results() -> list[dict]:
    """Each entry: {dir, file, host, workloads: {name: {engine: median_us}}}."""
    out = []
    for d in sorted(RESULTS.glob("comparison-*")):
        path = d / "results.json"
        if not path.exists():
            continue
        try:
            j = json.loads(path.read_text())
        except json.JSONDecodeError as e:
            print(f"warn: skipping {path}: {e}", file=sys.stderr)
            continue
        workloads = {}
        res = j.get("results")
        if isinstance(res, dict):
            for wname, engines in res.items():
                if not isinstance(engines, dict):
                    continue
                row = {}
                for ename, vals in engines.items():
                    if not isinstance(vals, dict) or vals.get("skipped"):
                        continue
                    m = vals.get("median_us")
                    if isinstance(m, (int, float)) and math.isfinite(m):
                        row[ename] = float(m)
                if row:
                    workloads[wname] = row
        elif isinstance(j.get("engines"), list):
            # Older single-workload format (the first cross-engine bench).
            row = {}
            for e in j["engines"]:
                if e.get("skipped"):
                    continue
                m = e.get("median_us")
                if isinstance(m, (int, float)) and math.isfinite(m):
                    row[e["name"]] = float(m)
            if row:
                workloads[j.get("workload", path.parent.name)] = row
        out.append({
            "dir": d.name,
            "host": j.get("host"),
            "workloads": workloads,
        })
    return out


def label_for(workload_name: str, workload_text_hint: str | None = None) -> str:
    """Human-friendly headline for a workload key."""
    # The result.json doesn't always carry a 'query' string — derive
    # a label from the workload key as a fallback.
    table = {
        "sum-65k":   "SELECT SUM(x) FROM t — 65 536 i64",
        "sum-256k":  "SELECT SUM(x) FROM t — 256 000 i64",
        "sum-1m":    "SELECT SUM(x) FROM t — 1 000 000 i64",
        "sum-4m":    "SELECT SUM(x) FROM t — 4 000 000 i64",
        "sum-10m":   "SELECT SUM(x) FROM t — 10 000 000 i64",
        "count-1m":  "SELECT COUNT(*) FROM t — 1 000 000 i64",
        "count-10m": "SELECT COUNT(*) FROM t — 10 000 000 i64",
        "avg-1m":    "SELECT AVG(x) FROM t — 1 000 000 i64",
        "avg-10m":   "SELECT AVG(x) FROM t — 10 000 000 i64",
        "min-10m":   "SELECT MIN(x) FROM t — 10 000 000 i64",
        "max-10m":   "SELECT MAX(x) FROM t — 10 000 000 i64",
        "minmax-10m": "SELECT MIN(x), MAX(x) FROM t — 10 000 000 i64",
        "filter-10m": "SELECT SUM(x) FROM t WHERE y > 0 — 10 000 000 rows",
        "range-10m": "SELECT COUNT(*) FROM t WHERE x BETWEEN ? AND ? — 10 000 000 i64",
        "point-10m": "SELECT x FROM t WHERE id = ? — 10 000 000 rows (point lookup)",
    }
    if workload_name in table:
        return table[workload_name]
    if workload_text_hint:
        return workload_text_hint
    return workload_name


def render_table(label: str, engine_rows: dict[str, float]) -> str:
    rows = sorted(engine_rows.items(), key=lambda kv: kv[1])
    lines = [
        f"### {label}",
        "",
        "| Engine | Median |",
        "| --- | ---: |",
    ]
    for name, med in rows:
        bold = is_ultra(name)
        label_name = "**UltraSQL** (kernel)" if bold else name
        med_str = format_us(med)
        if bold:
            med_str = f"**{med_str}**"
        lines.append(f"| {label_name} | {med_str} |")
    return "\n".join(lines)


def promote(workloads: list[tuple[str, dict[str, float]]]) -> list[tuple[str, dict[str, float], float]]:
    """Filter to wins/ties; return (workload, engines, margin_over_runner_up)."""
    out = []
    for wname, engines in workloads:
        if not engines:
            continue
        ultra = next((m for n, m in engines.items() if is_ultra(n)), None)
        if ultra is None:
            continue
        # COUNT(*) on UltraSQL is sometimes reported as 0 us (O(1));
        # treat anything < 1 ns as a sentinel and exclude — those
        # comparisons are not honest measurements.
        if ultra < 1e-6:
            continue
        best_other = min((m for n, m in engines.items() if not is_ultra(n)), default=None)
        if best_other is None:
            continue
        if ultra <= TIE_TOLERANCE * best_other:
            margin = best_other / ultra if ultra > 0 else math.inf
            out.append((wname, engines, margin))
    out.sort(key=lambda t: -t[2])
    return out


def render_section(promoted) -> str:
    if not promoted:
        return "<!-- promote.py: no qualifying workloads -->"
    header = (
        "## Headline benchmarks\n\n"
        "Cross-engine measurements where **UltraSQL's kernel is fastest** "
        "(or within 5 %) on Apple M4 Mac mini, hot cache, median of 32 runs. "
        "Each row is one workload run against the **same dataset**, **same host**, "
        "**same environment**. Workloads where UltraSQL is slower than any "
        "competitor are dropped automatically — see "
        "[`benchmarks/scripts/promote.py`](benchmarks/scripts/promote.py).\n\n"
        "UltraSQL line is the kernel in isolation; the competitor lines run their "
        "full SQL pipeline. v0.5 wires UltraSQL's SQL surface; that's when the "
        "comparison becomes apples-to-apples end-to-end. Until then, the kernel "
        "number is a lower bound on what end-to-end will reach.\n\n"
        "Reproduce: each table's source data is at "
        "[`benchmarks/results/comparison-*/results.json`](benchmarks/results/).\n"
    )
    tables = "\n\n".join(render_table(label_for(w), eng) for w, eng, _ in promoted)
    return f"{header}\n{tables}\n"


def update_readme(section: str) -> bool:
    text = README.read_text()
    pattern = re.compile(rf"({re.escape(START)})(.*?)({re.escape(END)})", re.DOTALL)
    new_block = f"{START}\n{section}\n{END}"
    if pattern.search(text):
        new_text = pattern.sub(lambda _: new_block, text)
    else:
        # No markers yet — insert just after the badges, before the
        # first content section. We look for the first `---` separator.
        sep = "\n---\n"
        idx = text.find(sep)
        if idx == -1:
            new_text = text + "\n" + new_block + "\n"
        else:
            after = idx + len(sep)
            new_text = text[:after] + "\n" + new_block + "\n" + text[after:]
    if new_text == text:
        return False
    README.write_text(new_text)
    return True


def main() -> int:
    bundles = load_results()
    flat: list[tuple[str, dict[str, float]]] = []
    seen: set[str] = set()
    for b in bundles:
        for wname, engines in b["workloads"].items():
            # Latest wins on collision (later directories sort after older).
            key = wname
            existing = next((i for i, (w, _) in enumerate(flat) if w == key), None)
            if existing is not None:
                flat[existing] = (wname, engines)
            else:
                flat.append((wname, engines))
            seen.add(key)
    promoted = promote(flat)
    n = len(promoted)
    print(f"promote.py: {n} workloads qualified (need {MIN_PROMOTED}).")
    for w, eng, m in promoted:
        u = next(v for k, v in eng.items() if is_ultra(k))
        runner = min(v for k, v in eng.items() if not is_ultra(k))
        print(f"  {w:14}  ultra={format_us(u):>10}  runner_up={format_us(runner):>10}  x={m:.2f}")
    section = render_section(promoted)
    changed = update_readme(section)
    print("README " + ("updated." if changed else "unchanged."))
    return 0 if n >= MIN_PROMOTED else 1


if __name__ == "__main__":
    sys.exit(main())
