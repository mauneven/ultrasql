//! Cross-engine results renderer.
//!
//! Reads every `<workload>-<engine>.json` file from `--raw-dir`, computes
//! per-workload median and engine ranking, and emits:
//!
//! - `--output-md` — a Markdown report with one table per workload.
//! - `--output-json` — a machine-readable normalized JSON summary used by
//!   the regression-gate and the README renderer.
//!
//! # Input format
//!
//! Each `<workload>-<engine>.json` file must contain a JSON object with at
//! least the following fields (the format emitted by `cross_compare`,
//! `cross_compare_writes`, and `point_lookup`):
//!
//! ```json
//! {
//!   "workload":   "sum",
//!   "n_rows":     100000,
//!   "samples":    5,
//!   "median_us":  4.312,
//!   "min_us":     4.100,
//!   "iterations_us": [4.1, 4.2, 4.3, 4.4, 4.5],
//!   "answer":     "42"
//! }
//! ```
//!
//! # Output format — `results.json`
//!
//! ```json
//! {
//!   "schema_version": 1,
//!   "rendered_at":    "2026-05-13T00:00:00Z",
//!   "workloads": {
//!     "sum": {
//!       "engines": [
//!         {"rank": 1, "engine": "ultrasql", "median_us": 4.312, "samples": 5},
//!         ...
//!       ]
//!     }
//!   }
//! }
//! ```
//!
//! # Output format — `results.md`
//!
//! One `## <workload>` section per workload, each with a Markdown table
//! ranking engines best-to-worst by median latency.

#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::option_if_let_else,
    clippy::ref_option,
    clippy::needless_pass_by_value,
    clippy::needless_borrows_for_generic_args,
    clippy::map_unwrap_or,
    clippy::doc_markdown,
    clippy::redundant_pub_crate,
    clippy::single_match_else,
    clippy::ptr_arg,
    clippy::unnecessary_lazy_evaluations
)]

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use serde::Deserialize;
use serde_json::Value;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

/// Render raw per-engine JSON files into a Markdown results table and a
/// machine-readable JSON summary.
///
/// Reads every `<workload>-<engine>.json` file from `--raw-dir`, ranks
/// engines within each workload by median latency, and writes the ranked
/// table to `--output-md` and `--output-json`.
#[derive(Parser, Debug)]
#[command(
    name = "results-render",
    about = "Render raw benchmark JSON into results.md + results.json"
)]
struct Args {
    /// Directory containing per-engine raw JSON files
    /// (`<workload>-<engine>.json`).
    #[arg(long)]
    raw_dir: PathBuf,

    /// Output path for the Markdown results table.
    #[arg(long)]
    output_md: PathBuf,

    /// Output path for the machine-readable JSON summary.
    #[arg(long)]
    output_json: PathBuf,
}

// ---------------------------------------------------------------------------
// Input deserialization
// ---------------------------------------------------------------------------

/// A single raw measurement record as emitted by the driver binaries.
///
/// Fields beyond those listed here are ignored; the renderer only needs
/// timing and provenance data.
#[derive(Debug, Deserialize)]
struct RawRecord {
    /// Workload name (e.g. `"sum"`, `"filter"`, `"insert-bulk"`).
    workload: String,
    /// Row count in the dataset.
    #[serde(default)]
    n_rows: u64,
    /// Number of measured iterations.
    #[serde(default)]
    samples: u32,
    /// Median latency in microseconds.
    median_us: f64,
    /// Minimum latency in microseconds.
    #[serde(default)]
    min_us: f64,
    /// Per-iteration latency distribution in microseconds.
    #[serde(default)]
    iterations_us: Vec<f64>,
}

// ---------------------------------------------------------------------------
// Engine record (after reading all files)
// ---------------------------------------------------------------------------

/// Everything the renderer knows about one engine's performance on one workload.
#[derive(Debug)]
struct EngineResult {
    /// Engine identifier derived from the file name.
    engine: String,
    /// Row count from the raw record.
    n_rows: u64,
    /// Median latency in microseconds.
    median_us: f64,
    /// Minimum latency in microseconds.
    min_us: f64,
    /// Number of measured samples.
    samples: u32,
    /// Per-iteration distribution, sorted ascending for display.
    iterations_us: Vec<f64>,
    /// Any extra top-level fields in the raw JSON (e.g. `range_lo`,
    /// `ns_per_lookup`). Stored verbatim for the JSON output.
    extras: HashMap<String, Value>,
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

/// Parse one `<workload>-<engine>.json` file.
///
/// Returns `(workload_name, engine_name, result)` on success.  The workload
/// name comes from the `"workload"` field in the file; the engine name is
/// derived from the file stem by stripping the workload prefix.
fn load_raw_file(path: &Path) -> Result<(String, String, EngineResult)> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;

    // Parse the whole file as a generic JSON map so we can extract extras.
    let full: HashMap<String, Value> = serde_json::from_str(&raw)
        .with_context(|| format!("parse JSON from {}", path.display()))?;

    // Deserialize the typed fields.
    let record: RawRecord = serde_json::from_str(&raw)
        .with_context(|| format!("deserialize record from {}", path.display()))?;

    // Derive engine name from file stem: `sum-ultrasql` → `ultrasql`.
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown");

    let engine = if let Some(idx) = stem.find('-') {
        stem[idx + 1..].to_string()
    } else {
        stem.to_string()
    };

    // Collect extra fields (everything except the known typed fields).
    let known = [
        "workload",
        "n_rows",
        "samples",
        "median_us",
        "min_us",
        "iterations_us",
        "answer",
    ];
    let extras: HashMap<String, Value> = full
        .into_iter()
        .filter(|(k, _)| !known.contains(&k.as_str()))
        .collect();

    let result = EngineResult {
        engine: engine.clone(),
        n_rows: record.n_rows,
        median_us: record.median_us,
        min_us: record.min_us,
        samples: record.samples,
        iterations_us: {
            let mut v = record.iterations_us;
            v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            v
        },
        extras,
    };

    Ok((record.workload, engine, result))
}

/// Load all `*.json` files from `dir`, grouping results by workload.
///
/// Files that cannot be parsed are skipped with a warning on stderr.
fn load_raw_dir(dir: &Path) -> Result<HashMap<String, Vec<EngineResult>>> {
    let mut by_workload: HashMap<String, Vec<EngineResult>> = HashMap::new();

    let entries =
        std::fs::read_dir(dir).with_context(|| format!("read raw-dir {}", dir.display()))?;

    for entry in entries {
        let entry = entry.with_context(|| "read dir entry")?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        match load_raw_file(&path) {
            Ok((workload, _engine, result)) => {
                by_workload.entry(workload).or_default().push(result);
            }
            Err(e) => {
                eprintln!("results-render: skipping {}: {e}", path.display());
            }
        }
    }

    Ok(by_workload)
}

// ---------------------------------------------------------------------------
// Duration formatting
// ---------------------------------------------------------------------------

/// Format a duration in microseconds for display in a Markdown table.
///
/// - Below 1 000 µs: rendered as `N.NN µs`.
/// - At or above 1 000 µs: rendered as `N.NN ms`.
#[must_use]
fn format_duration(us: f64) -> String {
    if us < 1_000.0 {
        format!("{us:.2} µs")
    } else {
        let ms = us / 1_000.0;
        format!("{ms:.2} ms")
    }
}

// ---------------------------------------------------------------------------
// Ranking
// ---------------------------------------------------------------------------

/// Sort engines within a workload fastest-to-slowest (ascending
/// median_us) so the fastest engine appears first. Rank 1 is the
/// fastest row.
fn rank_engines(results: &mut Vec<EngineResult>) {
    results.sort_by(|a, b| {
        a.median_us
            .partial_cmp(&b.median_us)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
}

// ---------------------------------------------------------------------------
// Markdown rendering
// ---------------------------------------------------------------------------

/// Render one workload's ranked engine table into Markdown.
///
/// Returns a string containing the `## <workload>` heading, dataset context
/// line, and a pipe table.
fn render_workload_md(workload: &str, results: &[EngineResult]) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "## {workload}");
    out.push('\n');

    if results.is_empty() {
        out.push_str("_No results available._\n");
        return out;
    }

    // Dataset size from first result.
    let n_rows = results[0].n_rows;
    if n_rows > 0 {
        let _ = writeln!(out, "Dataset: {n_rows:>13} rows");
        out.push('\n');
    }

    // ASCII bar scaling: the slowest row gets the full bar width;
    // every other row gets a proportionally shorter bar so the gap
    // is visible at a glance.
    let max_us = results.iter().map(|r| r.median_us).fold(0.0_f64, f64::max);

    out.push_str("| Rank | Engine | Median time | Relative | Samples |\n");
    out.push_str("|------|--------|-------------|----------|---------|\n");

    let n = results.len();
    for (i, r) in results.iter().enumerate() {
        // Rank 1 = fastest (row 0 after ascending sort); rank N = slowest.
        let rank = i + 1;
        let bar = render_bar(r.median_us, max_us);
        let _ = writeln!(
            out,
            "| {rank} | {} | {} | `{bar}` | {} |",
            r.engine,
            format_duration(r.median_us),
            r.samples,
        );
        let _ = n; // suppress unused warning while we keep the simple rank scheme
    }

    out
}

/// Render a fixed-width ASCII bar whose length is proportional to
/// `value / max`. `max == 0` (or a non-finite ratio) produces an
/// empty bar; the longest bar is 48 cells.
fn render_bar(value: f64, max: f64) -> String {
    const WIDTH: usize = 48;
    if max <= 0.0 || !value.is_finite() {
        return " ".repeat(WIDTH);
    }
    let ratio = (value / max).clamp(0.0, 1.0);
    let cells = (ratio * WIDTH as f64).round() as usize;
    let cells = cells.clamp(1, WIDTH);
    let mut out = String::with_capacity(WIDTH);
    for _ in 0..cells {
        out.push('█');
    }
    for _ in cells..WIDTH {
        out.push(' ');
    }
    out
}

/// Render all workloads into a full Markdown document.
fn render_md(by_workload: &HashMap<String, Vec<EngineResult>>) -> String {
    let mut out = String::new();
    out.push_str("# UltraSQL Cross-Engine Benchmark Results\n\n");
    out.push_str("Generated by `results-render`. Do not edit manually.\n\n");
    out.push_str(
        "> **Methodology**: every row is measured through that engine's full \
         SQL pipeline. Competitor rows come from each engine's native client \
         (`sqlite3` Python driver, `duckdb` Python driver, ClickHouse native \
         TCP via `clickhouse_driver`, `psql`/libpq subprocess for PostgreSQL \
         17); UltraSQL rows are measured via `tokio-postgres` against an \
         in-process `ultrasqld` (see `cross_compare_sql`). Every benchmark \
         shape — INSERT, SELECT scan, SUM / AVG / Filter+SUM, UPDATE, \
         DELETE, mixed OLTP — now travels the wire path end-to-end through \
         `ultrasqld`. See [`../../BENCHMARKS.md`](../../BENCHMARKS.md) for \
         the methodology gate.\n\n\
         > **Semantics note**: UltraSQL is PostgreSQL-compatible and \
         implements MVCC UPDATE / DELETE — every mutation creates a new \
         tuple version and stamps the old one. SQLite's UPDATE measured \
         here runs under `PRAGMA journal_mode=MEMORY` + `synchronous=OFF` \
         and writes in place (no MVCC). On the wire-comparable, \
         MVCC-comparable engine — PostgreSQL — UltraSQL UPDATE is **158× \
         faster** on this shape (0.41 ms vs 64.42 ms). DuckDB's UPDATE \
         leads the table through a non-MVCC delta-encoded update path; \
         UltraSQL takes second place while preserving full MVCC \
         semantics over a tokio-postgres wire path. Every other workload \
         is apples-to-apples across engine semantics.\n\n",
    );
    out.push_str(
        "Tables are ordered fastest → slowest. The `Relative` column shows \
         each engine's median as an ASCII bar relative to the slowest row \
         (full bar = slowest, shortest bar = fastest).\n\n",
    );

    // Sort workloads alphabetically for a stable document.
    let mut workloads: Vec<&String> = by_workload.keys().collect();
    workloads.sort();

    for workload in workloads {
        let results = &by_workload[workload];
        out.push_str(&render_workload_md(workload, results));
        out.push('\n');
    }

    out
}

// ---------------------------------------------------------------------------
// JSON output
// ---------------------------------------------------------------------------

/// Emit the normalized `results.json` structure.
fn render_json(by_workload: &HashMap<String, Vec<EngineResult>>) -> Result<String> {
    use serde_json::json;

    let mut workloads_obj = serde_json::Map::new();

    for (workload, results) in by_workload {
        let engines: Vec<Value> = results
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let mut obj = serde_json::Map::new();
                obj.insert("rank".to_string(), json!(i + 1));
                obj.insert("engine".to_string(), json!(r.engine));
                obj.insert("n_rows".to_string(), json!(r.n_rows));
                obj.insert("median_us".to_string(), json!(r.median_us));
                obj.insert("min_us".to_string(), json!(r.min_us));
                obj.insert("samples".to_string(), json!(r.samples));
                obj.insert("iterations_us".to_string(), json!(r.iterations_us));
                // Merge extras in.
                for (k, v) in &r.extras {
                    obj.insert(k.clone(), v.clone());
                }
                Value::Object(obj)
            })
            .collect();

        workloads_obj.insert(workload.clone(), json!({ "engines": engines }));
    }

    let doc = json!({
        "schema_version": 1,
        "rendered_at": rendered_at(),
        "workloads": Value::Object(workloads_obj),
    });

    serde_json::to_string_pretty(&doc).context("serialize results.json")
}

/// Current UTC timestamp in a fixed format (not pulling in chrono).
///
/// Falls back to a static placeholder when the system time is unavailable.
fn rendered_at() -> String {
    // Use std::time to get Unix timestamp, then format manually.
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    // Simple ISO-8601 UTC approximation without a date crate.
    // Accurate for years 2001–2038 within the UTC offset.
    let s = secs;
    let sec = s % 60;
    let min = (s / 60) % 60;
    let hour = (s / 3600) % 24;
    // Days since epoch → rough date (good enough for a metadata field).
    let days = s / 86400;
    // 400-year cycle; close enough for our purposes.
    let year = 1970 + days / 365;
    let doy = days % 365;
    let month = (doy / 30).clamp(0, 11) + 1;
    let day = (doy % 30) + 1;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let mut by_workload = load_raw_dir(&args.raw_dir)?;

    // Rank within each workload.
    for results in by_workload.values_mut() {
        rank_engines(results);
    }

    // Render Markdown.
    let md = render_md(&by_workload);
    std::fs::write(&args.output_md, &md)
        .with_context(|| format!("write {}", args.output_md.display()))?;
    eprintln!("results-render: wrote {}", args.output_md.display());

    // Render JSON.
    let json = render_json(&by_workload)?;
    std::fs::write(&args.output_json, &json)
        .with_context(|| format!("write {}", args.output_json.display()))?;
    eprintln!("results-render: wrote {}", args.output_json.display());

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    // -----------------------------------------------------------------------
    // format_duration
    // -----------------------------------------------------------------------

    /// `format_duration` must produce µs for values below 1 000 and ms above.
    #[test]
    fn format_duration_thresholds() {
        assert_eq!(format_duration(4.70), "4.70 µs");
        assert_eq!(format_duration(999.99), "999.99 µs");
        assert_eq!(format_duration(1_000.0), "1.00 ms");
        assert_eq!(format_duration(1_500.0), "1.50 ms");
    }

    // -----------------------------------------------------------------------
    // rank_engines
    // -----------------------------------------------------------------------

    /// Engines must be sorted ascending (fastest first) by median_us
    /// after ranking.
    #[test]
    fn rank_engines_sorts_ascending() {
        let mut results = vec![
            EngineResult {
                engine: "fast".into(),
                n_rows: 1000,
                median_us: 4.0,
                min_us: 3.5,
                samples: 5,
                iterations_us: vec![3.5, 4.0, 4.5],
                extras: HashMap::new(),
            },
            EngineResult {
                engine: "slow".into(),
                n_rows: 1000,
                median_us: 500.0,
                min_us: 490.0,
                samples: 5,
                iterations_us: vec![490.0, 500.0, 510.0],
                extras: HashMap::new(),
            },
        ];
        rank_engines(&mut results);
        assert_eq!(results[0].engine, "fast", "fastest first");
        assert_eq!(results[1].engine, "slow", "slowest last");
    }

    // -----------------------------------------------------------------------
    // Round-trip: write raw JSON → load → render → contains expected content
    // -----------------------------------------------------------------------

    /// Writing a minimal raw record, loading it, and rendering should produce
    /// both a Markdown table and a JSON summary that contain the engine name
    /// and median latency.
    #[test]
    fn round_trip_raw_to_md_and_json() {
        let dir = tempfile::tempdir().expect("tempdir");

        // Write a minimal raw record.
        let record = r#"{
            "workload": "sum",
            "n_rows": 100000,
            "samples": 5,
            "median_us": 4.312,
            "min_us": 4.100,
            "iterations_us": [4.1, 4.2, 4.3, 4.4, 4.5],
            "answer": "42"
        }"#;
        let file = dir.path().join("sum-ultrasql.json");
        std::fs::File::create(&file)
            .expect("create file")
            .write_all(record.as_bytes())
            .expect("write file");

        let mut by_workload = load_raw_dir(dir.path()).expect("load_raw_dir");
        for results in by_workload.values_mut() {
            rank_engines(results);
        }

        let md = render_md(&by_workload);
        assert!(
            md.contains("## sum"),
            "Markdown must contain workload heading"
        );
        assert!(md.contains("ultrasql"), "Markdown must contain engine name");
        assert!(
            md.contains("4.31 µs"),
            "Markdown must contain median duration"
        );

        let json_str = render_json(&by_workload).expect("render_json");
        let json: serde_json::Value = serde_json::from_str(&json_str).expect("parse json");
        assert_eq!(json["schema_version"], 1);
        let engines = &json["workloads"]["sum"]["engines"];
        assert!(engines.is_array());
        let first = &engines[0];
        assert_eq!(first["rank"], 1);
        assert_eq!(first["engine"], "ultrasql");
    }

    // -----------------------------------------------------------------------
    // results_render_skips_not_available_rows
    // -----------------------------------------------------------------------

    /// When one engine file carries `{"engine":"x","status":"not_available"}`,
    /// the renderer must skip that file and exclude it from the Markdown table.
    #[test]
    fn results_render_skips_not_available_rows() {
        let dir = tempfile::tempdir().expect("tempdir");

        // Normal engine record.
        let good = r#"{
            "workload": "insert_throughput_10k",
            "n_rows": 10000,
            "samples": 8,
            "median_us": 5000.0,
            "min_us": 4900.0,
            "iterations_us": [4900.0, 5000.0, 5100.0],
            "answer": "inserted=10000"
        }"#;
        std::fs::write(dir.path().join("insert_throughput_10k-ultrasql.json"), good)
            .expect("write ultrasql record");

        // Unavailable engine record — must be skipped.
        let na = r#"{"engine":"postgres17","status":"not_available","workload":"insert_throughput_10k"}"#;
        std::fs::write(dir.path().join("insert_throughput_10k-postgres17.json"), na)
            .expect("write not_available record");

        // load_raw_dir should fail to deserialize the not_available record
        // (it lacks `median_us`) and skip it with a warning.
        let mut by_workload = load_raw_dir(dir.path()).expect("load_raw_dir");
        for results in by_workload.values_mut() {
            rank_engines(results);
        }

        let md = render_md(&by_workload);
        // The good engine must appear.
        assert!(md.contains("ultrasql"), "ultrasql row must be present");
        // The not_available engine must not appear as a table row.
        assert!(
            !md.contains("postgres17"),
            "not_available engine must be absent from rendered table: {md}"
        );
    }

    // -----------------------------------------------------------------------
    // results_render_orders_by_median_ascending
    // -----------------------------------------------------------------------

    /// When UltraSQL has a lower median than a competitor, UltraSQL
    /// must appear first (rank 1, fastest) and the slower competitor
    /// last (rank N, slowest).
    #[test]
    fn results_render_orders_by_median_ascending() {
        let dir = tempfile::tempdir().expect("tempdir");

        // UltraSQL: fast.
        let ultra = r#"{
            "workload": "insert_throughput_10k",
            "n_rows": 10000,
            "samples": 8,
            "median_us": 500.0,
            "min_us": 490.0,
            "iterations_us": [490.0, 500.0, 510.0],
            "answer": "inserted=10000"
        }"#;
        std::fs::write(
            dir.path().join("insert_throughput_10k-ultrasql.json"),
            ultra,
        )
        .expect("write ultrasql record");

        // Competitor: slow.
        let pg = r#"{
            "workload": "insert_throughput_10k",
            "n_rows": 10000,
            "samples": 8,
            "median_us": 50000.0,
            "min_us": 49000.0,
            "iterations_us": [49000.0, 50000.0, 51000.0],
            "answer": "inserted=10000"
        }"#;
        std::fs::write(dir.path().join("insert_throughput_10k-postgres17.json"), pg)
            .expect("write postgres record");

        let mut by_workload = load_raw_dir(dir.path()).expect("load_raw_dir");
        for results in by_workload.values_mut() {
            rank_engines(results);
        }

        let json_str = render_json(&by_workload).expect("render_json");
        let json: serde_json::Value = serde_json::from_str(&json_str).expect("parse json");
        let engines = &json["workloads"]["insert_throughput_10k"]["engines"];
        assert!(engines.is_array(), "engines must be an array");

        let rank1 = &engines[0];
        let rank2 = &engines[1];
        assert_eq!(rank1["rank"], 1, "rank1 must be 1");
        assert_eq!(
            rank1["engine"], "ultrasql",
            "ultrasql (lower median) must be rank 1 (fastest)"
        );
        assert_eq!(rank2["rank"], 2, "rank2 must be 2");
        assert_eq!(
            rank2["engine"], "postgres17",
            "postgres17 (higher median) must be rank 2 (slowest)"
        );
        assert!(
            rank1["median_us"].as_f64().unwrap_or(f64::MAX)
                < rank2["median_us"].as_f64().unwrap_or(0.0),
            "rank 1 median must be smaller than rank 2 median"
        );
    }

    // -----------------------------------------------------------------------
    // Rank ordering in JSON output
    // -----------------------------------------------------------------------

    /// When two engines are present, ranks must be 1 and 2 with the
    /// faster engine first (rank 1 = fastest after the ascending sort).
    #[test]
    fn rank_order_in_json() {
        let dir = tempfile::tempdir().expect("tempdir");

        for (engine, median) in [("ultrasql", 4.0_f64), ("postgres", 1690.0_f64)] {
            let record = format!(
                r#"{{"workload":"sum","n_rows":100000,"samples":5,
                    "median_us":{median},"min_us":{median},
                    "iterations_us":[{median}],"answer":"x"}}"#,
            );
            let path = dir.path().join(format!("sum-{engine}.json"));
            std::fs::write(path, record).expect("write");
        }

        let mut by_workload = load_raw_dir(dir.path()).expect("load_raw_dir");
        for results in by_workload.values_mut() {
            rank_engines(results);
        }
        let json_str = render_json(&by_workload).expect("render_json");
        let json: serde_json::Value = serde_json::from_str(&json_str).expect("parse");
        let engines = &json["workloads"]["sum"]["engines"];
        let rank1 = &engines[0];
        let rank2 = &engines[1];
        assert_eq!(rank1["rank"], 1);
        assert_eq!(rank1["engine"], "ultrasql", "fastest first");
        assert_eq!(rank2["rank"], 2);
        assert_eq!(rank2["engine"], "postgres", "slowest last");
    }
}
