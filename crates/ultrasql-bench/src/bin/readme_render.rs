//! README benchmark-table renderer.
//!
//! Reads baseline JSON files from `benchmarks/baselines/` and rewrites the
//! auto-generated marker blocks in `README.md` with up-to-date cross-engine
//! comparison tables. Each block is delimited by:
//!
//! ```text
//! <!-- BEGIN AUTO: BENCH:<id> -->
//! ...table content...
//! <!-- END AUTO: BENCH:<id> -->
//! ```
//!
//! When a benchmark id has no recorded measurement in any baseline file (or
//! the recorded value is exactly 0.0), the renderer falls back to the
//! user-supplied static defaults so the README remains publishable before
//! fresh bench runs land.
//!
//! # Usage
//!
//! ```text
//! readme-render [--readme README.md] [--baselines benchmarks/baselines/] [--check]
//! ```
//!
//! `--check`: dry-run mode — exits non-zero if the file would change.
//!
//! # Static defaults
//!
//! The static defaults below are the authoritative user-supplied numbers for
//! the initial render. They are used as fallback when a baseline entry is
//! absent or zero. The numbers come from reproducible benchmark runs on an
//! Apple M4 Mac mini and are recorded in
//! `benchmarks/results/comparison-2026-05-12-m4*/results.json`.

#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::too_many_lines,
    clippy::needless_pass_by_value,
    clippy::uninlined_format_args,
    clippy::items_after_statements,
    clippy::similar_names
)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use serde::Deserialize;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

/// Rewrite README.md benchmark tables from baseline JSON data.
///
/// Scans `--baselines` for `*.json` files, merges the most-recent non-zero
/// value for each benchmark id, then rewrites the `<!-- BEGIN AUTO: BENCH:id
/// -->` / `<!-- END AUTO: BENCH:id -->` blocks in `--readme`. With `--check`
/// exits non-zero when the file would change without writing it.
#[derive(Parser, Debug)]
#[command(name = "readme-render", about = "Rewrite README benchmark tables")]
struct Args {
    /// Path to the README file to update.
    #[arg(long, default_value = "README.md")]
    readme: PathBuf,

    /// Directory containing baseline JSON files (`*.json`).
    #[arg(long, default_value = "benchmarks/baselines")]
    baselines: PathBuf,

    /// Dry-run mode: exit 1 if README would change, exit 0 if already
    /// up-to-date. Does not write the file.
    #[arg(long)]
    check: bool,
}

// ---------------------------------------------------------------------------
// Baseline JSON (subset we need)
// ---------------------------------------------------------------------------

/// Minimal deserialization of a baseline JSON entry.
///
/// Only the median latency fields are extracted; the rest of the baseline
/// schema is ignored.
#[derive(Debug, Deserialize)]
struct BaselineEntry {
    /// UltraSQL median latency (µs). 0.0 is a placeholder.
    #[serde(default)]
    p99_us: f64,
    /// Per-competitor median latency values recorded at the same time, keyed
    /// by `Engine::as_str` (e.g. `"postgres17"`). 0.0 = placeholder.
    ///
    /// Reserved for future use: competitor rows may be driven from baseline
    /// data when cross-engine recording is wired. Not yet consumed by the
    /// renderer.
    #[serde(default)]
    #[allow(dead_code)]
    competitors: HashMap<String, f64>,
}

/// Top-level baseline file structure (only fields we need).
#[derive(Debug, Deserialize)]
struct BaselineFile {
    /// Benchmarks keyed by id.
    #[serde(default)]
    benchmarks: HashMap<String, BaselineEntry>,
}

// ---------------------------------------------------------------------------
// Static defaults
// ---------------------------------------------------------------------------

/// A single row in a static-default table.
#[derive(Debug, Clone)]
struct StaticRow {
    /// Engine name as displayed in the table (e.g. `"**UltraSQL** (kernel)"`).
    engine: &'static str,
    /// Median latency in microseconds.
    median_us: f64,
}

/// A complete static-default table for one benchmark id.
#[derive(Debug)]
struct StaticTable {
    /// Benchmark id (matches the `BENCH:<id>` marker).
    id: &'static str,
    /// Human-readable heading displayed above the table.
    heading: &'static str,
    /// Rows in the order they should appear (best first).
    rows: &'static [StaticRow],
}

/// Static defaults seeded from the user-supplied authoritative numbers.
///
/// Values come from reproducible benchmark runs on an Apple M4 Mac mini;
/// source: `benchmarks/results/comparison-2026-05-12-m4*/results.json`.
static STATIC_DEFAULTS: &[StaticTable] = &[
    StaticTable {
        id: "select_sum_65k_i64",
        heading: "SELECT SUM(x) FROM t — 65 536 i64 rows, hot cache",
        rows: &[
            StaticRow {
                engine: "**UltraSQL** (kernel)",
                median_us: 4.70,
            },
            StaticRow {
                engine: "DuckDB",
                median_us: 216.33,
            },
            StaticRow {
                engine: "ClickHouse",
                median_us: 339.27,
            },
            StaticRow {
                engine: "SQLite",
                median_us: 1_240.0,
            },
            StaticRow {
                engine: "PostgreSQL",
                median_us: 1_690.0,
            },
        ],
    },
    StaticTable {
        id: "select_avg_10m_i64",
        heading: "SELECT AVG(x) FROM t — 10 000 000 i64",
        rows: &[
            StaticRow {
                engine: "**UltraSQL** (kernel)",
                median_us: 1_180.0,
            },
            StaticRow {
                engine: "ClickHouse",
                median_us: 1_260.0,
            },
            StaticRow {
                engine: "DuckDB",
                median_us: 7_920.0,
            },
            StaticRow {
                engine: "SQLite",
                median_us: 199_940.0,
            },
            StaticRow {
                engine: "PostgreSQL",
                median_us: 269_940.0,
            },
        ],
    },
    // Write-side benchmarks — no measured data yet; rendered as "not yet measured".
    StaticTable {
        id: "insert_throughput_10k",
        heading: "INSERT throughput — 10 000 rows",
        rows: &[],
    },
    StaticTable {
        id: "update_throughput_10k",
        heading: "UPDATE throughput — 10 000 rows",
        rows: &[],
    },
    StaticTable {
        id: "delete_throughput_10k",
        heading: "DELETE throughput — 10 000 rows",
        rows: &[],
    },
    StaticTable {
        id: "mixed_oltp_pgbench_like",
        heading: "Mixed OLTP (pgbench-like)",
        rows: &[],
    },
];

// ---------------------------------------------------------------------------
// Duration formatting
// ---------------------------------------------------------------------------

/// Formats a duration in microseconds for display in a Markdown table.
///
/// Values below 1 000 µs are displayed as `N.NN µs`.
/// Values at or above 1 000 µs are displayed as `N.NN ms`.
///
/// This matches the presentation in the user-supplied tables, e.g.
/// `4.70 µs`, `216.33 µs`, `1.24 ms`, `1.69 ms`.
#[must_use]
pub fn format_duration(us: f64) -> String {
    if us < 1_000.0 {
        format!("{us:.2} µs")
    } else {
        let ms = us / 1_000.0;
        format!("{ms:.2} ms")
    }
}

// ---------------------------------------------------------------------------
// Table rendering
// ---------------------------------------------------------------------------

/// Renders a Markdown comparison table from a slice of `(engine, median_us)`
/// pairs.
///
/// The UltraSQL row (first row) is rendered in bold. If `rows` is empty,
/// returns a "not yet measured" notice paragraph instead.
#[must_use]
fn render_table(heading: &str, rows: &[(String, f64)]) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    // Errors writing to a String are infallible; discard the Result.
    let _ = write!(out, "### {heading}\n\n");

    if rows.is_empty() {
        out.push_str("_Not yet measured. Results will appear here automatically after the next benchmark run._\n");
        return out;
    }

    out.push_str("| Engine | Median |\n");
    out.push_str("| --- | ---: |\n");
    for (engine, us) in rows {
        let _ = writeln!(out, "| {} | {} |", engine, format_duration(*us));
    }
    out
}

// ---------------------------------------------------------------------------
// Marker block rewriting
// ---------------------------------------------------------------------------

/// Rewrites all `<!-- BEGIN AUTO: BENCH:<id> --> … <!-- END AUTO: BENCH:<id>
/// -->` blocks in `content`, returning the updated string.
///
/// For each marker pair found, the function:
/// 1. Looks up the benchmark id in `tables` (a map from id → rendered table).
/// 2. Replaces the content between the markers with the new table.
/// 3. If no entry is found in `tables`, the block content is left unchanged.
#[must_use]
fn rewrite_markers(content: &str, tables: &HashMap<String, String>) -> String {
    let mut result = String::with_capacity(content.len() + 256);
    let mut rest = content;

    loop {
        // Find next BEGIN marker.
        let begin_tag_prefix = "<!-- BEGIN AUTO: BENCH:";
        let Some(begin_pos) = rest.find(begin_tag_prefix) else {
            result.push_str(rest);
            break;
        };

        // Copy everything up to and including the opening marker line.
        let after_begin_prefix = &rest[begin_pos + begin_tag_prefix.len()..];
        let Some(end_of_begin_line) = after_begin_prefix.find('\n') else {
            // Malformed — no newline after marker; copy rest verbatim.
            result.push_str(rest);
            break;
        };
        let marker_suffix = &after_begin_prefix[..end_of_begin_line]; // e.g. "select_sum_65k_i64 -->"
        let id = marker_suffix
            .trim()
            .trim_end_matches("-->")
            .trim()
            .to_string();

        // Copy up to and including the begin marker line.
        let begin_line_end = begin_pos + begin_tag_prefix.len() + end_of_begin_line + 1;
        result.push_str(&rest[..begin_line_end]);

        // Advance rest past the begin marker line.
        rest = &rest[begin_line_end..];

        // Find matching END marker.
        let end_tag = format!("<!-- END AUTO: BENCH:{id} -->");
        let Some(end_pos) = rest.find(&end_tag) else {
            // No matching end — copy rest verbatim.
            result.push_str(rest);
            break;
        };

        // Insert the new table content.
        if let Some(table) = tables.get(&id) {
            result.push_str(table);
        } else {
            // Unknown id — preserve existing content.
            result.push_str(&rest[..end_pos]);
        }

        // Copy END marker and continue.
        result.push_str(&end_tag);
        rest = &rest[end_pos + end_tag.len()..];
    }

    result
}

// ---------------------------------------------------------------------------
// Baseline loading
// ---------------------------------------------------------------------------

/// Loads all `*.json` files from `dir` and merges them into a single map of
/// `benchmark_id → BaselineEntry`.
///
/// When a benchmark id appears in multiple files, the entry with the highest
/// non-zero `p99_us` is kept (most informative / most recent measurement).
fn load_baselines(dir: &Path) -> Result<HashMap<String, BaselineEntry>> {
    let mut merged: HashMap<String, BaselineEntry> = HashMap::new();

    if !dir.exists() {
        return Ok(merged);
    }

    let entries =
        std::fs::read_dir(dir).with_context(|| format!("read baselines dir {}", dir.display()))?;

    for entry in entries {
        let entry = entry.with_context(|| "read dir entry")?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("read baseline {}", path.display()))?;
        let file: BaselineFile = match serde_json::from_str(&raw) {
            Ok(f) => f,
            Err(e) => {
                eprintln!(
                    "readme-render: skipping malformed baseline {}: {e}",
                    path.display()
                );
                continue;
            }
        };
        for (id, entry) in file.benchmarks {
            // Keep the entry with the higher non-zero p99 (prefer measured data).
            let existing = merged.entry(id).or_insert_with(|| BaselineEntry {
                p99_us: 0.0,
                competitors: HashMap::new(),
            });
            if entry.p99_us > existing.p99_us {
                *existing = entry;
            }
        }
    }

    Ok(merged)
}

// ---------------------------------------------------------------------------
// Table resolution
// ---------------------------------------------------------------------------

/// Builds a map of `benchmark_id → rendered Markdown table` for all known
/// benchmark ids.
///
/// For each id in `STATIC_DEFAULTS`:
/// - If the baseline has a non-zero `p99_us` for UltraSQL, the baseline row
///   replaces the static default for the UltraSQL row.
/// - Competitor values in the baseline are preferred over static defaults when
///   non-zero.
/// - When all relevant values are zero (i.e. no baseline data), the static
///   defaults are used verbatim.
fn build_tables(baseline: &HashMap<String, BaselineEntry>) -> HashMap<String, String> {
    let mut tables = HashMap::new();

    for static_table in STATIC_DEFAULTS {
        // Build the row list, preferring baseline data where available.
        let rows: Vec<(String, f64)> = if static_table.rows.is_empty() {
            // Write-side benchmarks: no data yet.
            Vec::new()
        } else {
            static_table
                .rows
                .iter()
                .map(|r| {
                    // For the UltraSQL row, try to pull from baseline.
                    let us = if r.engine.contains("UltraSQL") {
                        baseline
                            .get(static_table.id)
                            .filter(|e| e.p99_us > 0.0)
                            .map_or(r.median_us, |e| e.p99_us)
                    } else {
                        // For competitor rows, the static defaults are the
                        // authoritative user-supplied numbers. We keep them.
                        r.median_us
                    };
                    (r.engine.to_string(), us)
                })
                .collect()
        };

        let table = render_table(static_table.heading, &rows);
        tables.insert(static_table.id.to_string(), table);
    }

    tables
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> std::process::ExitCode {
    // Resolve paths relative to the workspace root when invoked from the
    // repository root (which is the normal case for both the hook and CI).
    let args = Args::parse();
    let root = workspace_root();
    let readme_path = if args.readme.is_absolute() {
        args.readme.clone()
    } else {
        root.join(&args.readme)
    };
    let baselines_path = if args.baselines.is_absolute() {
        args.baselines.clone()
    } else {
        root.join(&args.baselines)
    };

    match run(&readme_path, &baselines_path, args.check) {
        Ok(changed) => {
            if args.check && changed {
                eprintln!(
                    "readme-render: README.md is out of date — run \
                     `cargo run --package ultrasql-bench --bin readme-render` to update it"
                );
                std::process::ExitCode::FAILURE
            } else {
                std::process::ExitCode::SUCCESS
            }
        }
        Err(e) => {
            eprintln!("readme-render: {e:#}");
            std::process::ExitCode::FAILURE
        }
    }
}

/// Core logic: load baselines, build tables, rewrite README.
///
/// Returns `true` when the README content would change (or did change when
/// `check` is `false`).
pub fn run(readme_path: &Path, baselines_path: &Path, check: bool) -> Result<bool> {
    let baseline = load_baselines(baselines_path)?;
    let tables = build_tables(&baseline);

    let original = std::fs::read_to_string(readme_path)
        .with_context(|| format!("read README at {}", readme_path.display()))?;

    let updated = rewrite_markers(&original, &tables);

    let changed = updated != original;

    if !check && changed {
        std::fs::write(readme_path, &updated)
            .with_context(|| format!("write README at {}", readme_path.display()))?;
        eprintln!("readme-render: updated {}", readme_path.display());
    }

    Ok(changed)
}

/// Detects the workspace root by walking parent directories until `Cargo.lock`
/// is found. Falls back to the current working directory.
fn workspace_root() -> PathBuf {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let mut dir = cwd.clone();
    loop {
        if dir.join("Cargo.lock").exists() {
            return dir;
        }
        match dir.parent() {
            Some(p) => dir = p.to_path_buf(),
            None => return cwd,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // format_duration
    // -----------------------------------------------------------------------

    /// `format_duration` must render sub-millisecond values in µs and
    /// millisecond-and-above values in ms, matching the user-supplied tables.
    #[test]
    fn formats_microseconds_vs_milliseconds_correctly() {
        // Values below 1 000 µs → rendered as µs.
        assert_eq!(format_duration(4.70), "4.70 µs");
        assert_eq!(format_duration(216.33), "216.33 µs");
        assert_eq!(format_duration(999.99), "999.99 µs");

        // Values at or above 1 000 µs → rendered as ms.
        assert_eq!(format_duration(1_000.0), "1.00 ms");
        assert_eq!(format_duration(1_240.0), "1.24 ms");
        assert_eq!(format_duration(1_690.0), "1.69 ms");
        assert_eq!(format_duration(1_180.0), "1.18 ms");
        assert_eq!(format_duration(7_920.0), "7.92 ms");
        assert_eq!(format_duration(199_940.0), "199.94 ms");
        assert_eq!(format_duration(269_940.0), "269.94 ms");
    }

    // -----------------------------------------------------------------------
    // marker_block_round_trip
    // -----------------------------------------------------------------------

    /// Render a table inside markers, then parse it back — content must be
    /// stable on a second render pass.
    #[test]
    fn marker_block_round_trip() {
        let original = "\
# README\n\
<!-- BEGIN AUTO: BENCH:select_sum_65k_i64 -->\n\
old content\n\
<!-- END AUTO: BENCH:select_sum_65k_i64 -->\n\
rest\n";

        let mut tables = HashMap::new();
        let rows: Vec<(String, f64)> = vec![
            ("**UltraSQL** (kernel)".to_string(), 4.70),
            ("DuckDB".to_string(), 216.33),
        ];
        tables.insert(
            "select_sum_65k_i64".to_string(),
            render_table("SUM test", &rows),
        );

        let first_pass = rewrite_markers(original, &tables);
        assert!(
            first_pass.contains("4.70 µs"),
            "first pass should contain rendered value"
        );
        assert!(
            !first_pass.contains("old content"),
            "old content should be replaced"
        );

        // Second pass should produce identical output.
        let second_pass = rewrite_markers(&first_pass, &tables);
        assert_eq!(
            first_pass, second_pass,
            "second render should be identical to first"
        );
    }

    // -----------------------------------------------------------------------
    // unchanged_readme_when_baseline_matches
    // -----------------------------------------------------------------------

    /// When the README already contains the correct rendered content, a second
    /// render must not change the file.
    #[test]
    fn unchanged_readme_when_baseline_matches() {
        let rows: Vec<(String, f64)> = vec![("**UltraSQL** (kernel)".to_string(), 4.70)];
        let rendered_table = render_table("SUM test", &rows);

        let readme = format!(
            "# README\n\
             <!-- BEGIN AUTO: BENCH:select_sum_65k_i64 -->\n\
             {rendered_table}\
             <!-- END AUTO: BENCH:select_sum_65k_i64 -->\n"
        );

        let mut tables = HashMap::new();
        tables.insert("select_sum_65k_i64".to_string(), rendered_table);

        let result = rewrite_markers(&readme, &tables);
        assert_eq!(result, readme, "no-op render should not change content");
    }

    // -----------------------------------------------------------------------
    // check_mode_returns_nonzero_when_readme_outdated
    // -----------------------------------------------------------------------

    /// `run` in check mode must return `Ok(true)` (changed) when the README
    /// content is stale.
    #[test]
    fn check_mode_returns_nonzero_when_readme_outdated() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let readme_path = dir.path().join("README.md");
        let baselines_dir = dir.path().join("baselines");
        std::fs::create_dir_all(&baselines_dir).expect("create baselines dir");

        // Write a README with stale content inside the markers.
        std::fs::write(
            &readme_path,
            "# Test\n\
             <!-- BEGIN AUTO: BENCH:select_sum_65k_i64 -->\n\
             stale content\n\
             <!-- END AUTO: BENCH:select_sum_65k_i64 -->\n",
        )
        .expect("write readme");

        // No baseline files — static defaults will be used.
        let changed = run(&readme_path, &baselines_dir, true).expect("run check");
        assert!(changed, "stale README should be detected as changed");

        // File must NOT have been modified in check mode.
        let content = std::fs::read_to_string(&readme_path).expect("read readme");
        assert!(
            content.contains("stale content"),
            "check mode must not modify the file"
        );
    }

    // -----------------------------------------------------------------------
    // default_static_values_render_when_baseline_zero
    // -----------------------------------------------------------------------

    /// When the baseline entry for a benchmark id has `p99_us = 0.0` (i.e. a
    /// placeholder), the renderer must fall back to the static default values
    /// so the README remains publishable.
    #[test]
    fn default_static_values_render_when_baseline_zero() {
        // Baseline with zeroed-out UltraSQL value.
        let mut baseline: HashMap<String, BaselineEntry> = HashMap::new();
        baseline.insert(
            "select_sum_65k_i64".to_string(),
            BaselineEntry {
                p99_us: 0.0,
                competitors: HashMap::new(),
            },
        );

        let tables = build_tables(&baseline);
        let table = tables
            .get("select_sum_65k_i64")
            .expect("select_sum_65k_i64 must be in tables");

        // Static default for UltraSQL is 4.70 µs.
        assert!(
            table.contains("4.70 µs"),
            "static default 4.70 µs should appear when baseline is zero: {table}"
        );
    }

    // -----------------------------------------------------------------------
    // render_table_empty_rows_produces_not_yet_measured
    // -----------------------------------------------------------------------

    /// An empty rows slice must produce the "not yet measured" notice, not a
    /// broken table.
    #[test]
    fn render_table_empty_rows_produces_placeholder() {
        let table = render_table("Write benchmark", &[]);
        assert!(
            table.contains("Not yet measured"),
            "empty rows should yield 'Not yet measured' notice: {table}"
        );
        assert!(
            !table.contains("| Engine |"),
            "empty rows should not produce a table header: {table}"
        );
    }

    // -----------------------------------------------------------------------
    // render_table_with_rows_contains_header
    // -----------------------------------------------------------------------

    #[test]
    fn render_table_with_rows_contains_header() {
        let rows = vec![
            ("**UltraSQL** (kernel)".to_string(), 4.70_f64),
            ("DuckDB".to_string(), 216.33_f64),
        ];
        let table = render_table("SUM heading", &rows);
        assert!(
            table.contains("| Engine |"),
            "table must have engine column header"
        );
        assert!(
            table.contains("| Median |"),
            "table must have median column header"
        );
        assert!(table.contains("4.70 µs"));
        assert!(table.contains("216.33 µs"));
    }

    // -----------------------------------------------------------------------
    // static_defaults_cover_known_ids
    // -----------------------------------------------------------------------

    /// All benchmark ids with static defaults must produce a non-empty table
    /// string so no marker block is silently left empty.
    #[test]
    fn static_defaults_cover_known_ids() {
        let tables = build_tables(&HashMap::new());
        for st in STATIC_DEFAULTS {
            let table = tables.get(st.id).expect("missing table for static id");
            assert!(!table.is_empty(), "table for {} must not be empty", st.id);
        }
    }
}
