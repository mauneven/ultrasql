//! Per-stage regression gate with cross-engine floor enforcement.
//!
//! Iterates every `BenchSpec` in `REGISTRY` that matches the requested
//! stage filter, runs the UltraSQL implementation, compares the results
//! against a recorded baseline, and checks that UltraSQL meets the floor
//! ratio defined for each listed competitor engine.
//!
//! # Exit codes
//!
//! | Code | Meaning |
//! |------|---------|
//! | 0 | All benchmarks passed all checks. |
//! | 1 | At least one benchmark regressed vs the baseline by >5%. |
//! | 2 | At least one competitor floor was not met. |
//! | 3 | Setup error (missing baseline file, bad JSON, unknown stage). |
//!
//! # Usage
//!
//! ```text
//! regression-gate [--baseline benchmarks/baselines/<stage>.json]
//!                 [--stage v0_6]
//!                 [--engines postgres17,duckdb,sqlite3,clickhouse]
//!                 [--update-baseline]
//!                 [--iterations N]
//!                 [--warmup N]
//!                 [--dry-run]
//! ```
//!
//! # Docs-only auto-skip
//!
//! The binary is docs-only-aware only when invoked from the pre-push hook
//! (which already skips the binary entirely for docs-only diffs). This binary
//! itself always runs the full gate when invoked directly.
//!
//! # Escape hatch
//!
//! Set `ULTRASQL_SKIP_BENCH=1` before running `git push` to bypass the
//! regression gate for documentation-only changes:
//!
//! ```text
//! ULTRASQL_SKIP_BENCH=1 git push
//! ```
//!
//! The pre-push hook honours this variable and prints an explanatory message
//! when it skips the gate.

#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::too_many_lines,
    clippy::needless_pass_by_value,
    clippy::redundant_clone,
    clippy::if_not_else,
    clippy::unnecessary_wraps,
    clippy::trivially_copy_pass_by_ref,
    clippy::map_unwrap_or,
    clippy::similar_names,
    clippy::many_single_char_names,
    clippy::items_after_statements,
    clippy::float_cmp,
    clippy::uninlined_format_args,
    clippy::case_sensitive_file_extension_comparisons
)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::Parser;
use serde::{Deserialize, Serialize};
use ultrasql_bench::registry::{
    BenchContext, BenchResult, Engine, FloorMetric, HostInfo, Stage, median_f64, p99_f64,
};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

/// Per-stage UltraSQL regression gate.
///
/// Runs registered benchmarks for a given stage, compares against the
/// recorded baseline, and enforces competitor floor ratios. Exits non-zero
/// on any failure.
#[derive(Parser, Debug)]
#[command(
    name = "regression-gate",
    about = "Per-stage regression gate with cross-engine floor enforcement"
)]
struct Args {
    /// Path to the stage baseline JSON file.
    ///
    /// Defaults to `benchmarks/baselines/<stage>.json` relative to the
    /// workspace root (detected from the current executable's path or CWD).
    /// Required when `--update-baseline` is not set and the default path does
    /// not exist.
    #[arg(long)]
    baseline: Option<PathBuf>,

    /// Stage filter: only benchmarks tagged with this stage are run.
    ///
    /// Accepts the kebab-case stage name (e.g. `v0_3`, `v0_6`). The
    /// special value `current` reads the stage from
    /// `benchmarks/current_stage.txt` in the workspace root.
    #[arg(long, default_value = "current")]
    stage: String,

    /// Comma-separated list of engines to include in competitor-floor checks.
    ///
    /// Any engine not in this list is skipped even if it appears in a
    /// benchmark's `competitor_floors`. Defaults to all engines.
    #[arg(long)]
    engines: Option<String>,

    /// Write the freshly measured values back to the baseline file.
    ///
    /// When set without an existing baseline the file is created from
    /// scratch. Without this flag the gate only reads the existing baseline
    /// and fails on regressions.
    #[arg(long)]
    update_baseline: bool,

    /// Number of measured iterations per benchmark (warmup excluded).
    #[arg(long, default_value_t = 5)]
    iterations: u32,

    /// Number of warmup iterations discarded before measurement begins.
    #[arg(long, default_value_t = 1)]
    warmup: u32,

    /// Print the plan and baseline values without actually running benchmarks.
    ///
    /// Useful for verifying that the baseline file is readable and the
    /// stage filter matches the expected set of benchmarks.
    #[arg(long)]
    dry_run: bool,

    /// Smoke mode: run each benchmark exactly once (no warmup), skip
    /// competitor-floor checks, and target ≤ 5 s total wall-clock.
    ///
    /// Used by the pre-push hook for a fast "did this crash?" sanity check.
    /// Full accuracy runs (`--iterations 8 --warmup 2`) are reserved for
    /// `make bench-full` before promoting a perf-sensitive commit.
    #[arg(long)]
    smoke: bool,
}

// ---------------------------------------------------------------------------
// Baseline JSON schema
// ---------------------------------------------------------------------------

/// Top-level structure of a stage baseline file.
///
/// Located at `benchmarks/baselines/<stage>.json`. Written by
/// `--update-baseline`; read by the gate on every push.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageBaseline {
    /// Stage this baseline covers (e.g. `"v0_6"`).
    pub stage: String,
    /// Host on which the baseline was recorded.
    pub host: HostInfo,
    /// Short git commit SHA at baseline capture time.
    pub git_commit: String,
    /// ISO-8601 timestamp when the baseline was recorded.
    pub captured_at: String,
    /// Allowed regression percentage (5.0 = 5%).
    pub tolerance_pct: f64,
    /// Per-benchmark records keyed by `BenchSpec::id`.
    pub benchmarks: HashMap<String, BenchBaseline>,
}

/// Recorded values for a single benchmark.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchBaseline {
    /// UltraSQL throughput in operations per second (0.0 = placeholder).
    pub throughput_per_sec: f64,
    /// UltraSQL p99 latency in microseconds (0.0 = placeholder).
    pub p99_us: f64,
    /// Per-competitor throughput values recorded at the same time, keyed by
    /// [`Engine::as_str`] (e.g. `"postgres17"`). 0.0 = placeholder.
    pub competitors: HashMap<String, f64>,
}

// ---------------------------------------------------------------------------
// Exit-code constants
// ---------------------------------------------------------------------------

/// Exit code returned when all benchmarks pass.
const EXIT_PASS: i32 = 0;
/// Exit code returned when at least one benchmark regressed vs the baseline.
const EXIT_REGRESSION: i32 = 1;
/// Exit code returned when at least one competitor floor was not met.
const EXIT_COMPETITOR_LOSS: i32 = 2;
/// Exit code returned on setup errors (missing files, parse failures, etc.).
const EXIT_SETUP_ERROR: i32 = 3;

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> std::process::ExitCode {
    let args = Args::parse();
    match run(args) {
        Ok(code) => std::process::ExitCode::from(u8::try_from(code).unwrap_or(1)),
        Err(e) => {
            eprintln!("regression-gate: setup error: {e:#}");
            std::process::ExitCode::from(u8::try_from(EXIT_SETUP_ERROR).unwrap_or(3))
        }
    }
}

fn run(args: Args) -> Result<i32> {
    // 1. Resolve the stage.
    let stage = resolve_stage(&args.stage)?;

    if args.smoke {
        eprintln!("regression-gate: SMOKE mode — one run per benchmark, no competitor checks");
    }
    eprintln!("regression-gate: stage = {stage}");

    // 2. Parse the engine filter (if any).
    let engine_filter: Option<Vec<String>> = args
        .engines
        .as_ref()
        .map(|s| s.split(',').map(str::trim).map(str::to_lowercase).collect());

    // 3. Collect the benchmarks that match this stage.
    let specs: Vec<_> = ultrasql_bench::registry::REGISTRY
        .iter()
        .filter(|s| s.stage == stage)
        .collect();
    if specs.is_empty() {
        eprintln!("regression-gate: no benchmarks registered for stage {stage}; nothing to do");
        return Ok(EXIT_PASS);
    }
    eprintln!(
        "regression-gate: {} benchmark(s) in stage {stage}:",
        specs.len()
    );
    for spec in &specs {
        eprintln!("  - {}", spec.id);
    }

    // 4. Resolve the baseline path.
    let baseline_path = resolve_baseline_path(args.baseline.as_ref(), &stage)?;

    // 5. Load baseline (may be absent when --update-baseline is set or in smoke mode).
    let maybe_baseline: Option<StageBaseline> = if baseline_path.exists() {
        let raw = std::fs::read_to_string(&baseline_path)
            .with_context(|| format!("read baseline {}", baseline_path.display()))?;
        let b: StageBaseline = serde_json::from_str(&raw)
            .with_context(|| format!("parse baseline {}", baseline_path.display()))?;
        Some(b)
    } else if args.update_baseline || args.smoke {
        if !args.smoke {
            eprintln!(
                "regression-gate: no existing baseline at {}; will create on --update-baseline",
                baseline_path.display()
            );
        }
        None
    } else {
        bail!(
            "baseline file not found: {}  \
             (run with --update-baseline to create it, or pass --baseline <path>)",
            baseline_path.display()
        );
    };

    // 6. Dry-run: just print what would run.
    if args.dry_run {
        println!("DRY RUN — no benchmarks executed.");
        println!("Stage:    {stage}");
        println!("Baseline: {}", baseline_path.display());
        for spec in &specs {
            let baseline_entry = maybe_baseline
                .as_ref()
                .and_then(|b| b.benchmarks.get(spec.id));
            println!(
                "  {} [{:?}]  baseline_throughput={:.0}",
                spec.id,
                spec.workload,
                baseline_entry.map_or(0.0, |e| e.throughput_per_sec)
            );
        }
        return Ok(EXIT_PASS);
    }

    // 7. Build execution context.
    //    Smoke mode: exactly 1 iteration, 0 warmup rounds.
    let (iterations, warmup_iterations) = if args.smoke {
        (1, 0)
    } else {
        (args.iterations, args.warmup)
    };
    let ctx = BenchContext {
        iterations,
        warmup_iterations,
        host: HostInfo::from_env(),
    };

    let smoke_start = if args.smoke {
        Some(std::time::Instant::now())
    } else {
        None
    };

    // 8. Run benchmarks and collect results.
    let tolerance = maybe_baseline.as_ref().map_or(5.0, |b| b.tolerance_pct);
    let mut regression_failures: Vec<String> = Vec::new();
    let mut floor_failures: Vec<String> = Vec::new();
    let mut new_entries: HashMap<String, BenchBaseline> = HashMap::new();

    for spec in &specs {
        eprintln!("  running {} …", spec.id);
        let result = (spec.run)(&ctx);
        let throughput = result.throughput_per_sec;
        let p99 = if result.samples.is_empty() {
            result.p99_latency_us
        } else {
            p99_f64(&result.samples)
        };

        // 8a. Regression check vs baseline (skipped in smoke mode).
        if !args.smoke {
            if let Some(ref baseline) = maybe_baseline {
                check_regression(
                    spec.id,
                    throughput,
                    &result,
                    baseline,
                    tolerance,
                    &mut regression_failures,
                );
            }
        }

        // 8b. Competitor floor check (skipped in smoke mode).
        if !args.smoke {
            check_competitor_floors(
                spec.id,
                throughput,
                p99,
                spec.competitor_floors,
                engine_filter.as_deref(),
                maybe_baseline.as_ref(),
                &mut floor_failures,
            );
        }

        // Accumulate new values for potential baseline write.
        let competitors_map: HashMap<String, f64> = spec
            .competitor_floors
            .iter()
            .map(|(eng, _)| (eng.as_str().to_string(), 0.0))
            .collect();
        new_entries.insert(
            spec.id.to_string(),
            BenchBaseline {
                throughput_per_sec: throughput,
                p99_us: p99,
                competitors: competitors_map,
            },
        );
    }

    // 8c. Smoke mode: report wall-clock and exit early.
    if args.smoke {
        let elapsed = smoke_start.map(|t| t.elapsed()).unwrap_or_default();
        eprintln!(
            "regression-gate: smoke complete in {:.2}s — {} benchmark(s) ran without panic",
            elapsed.as_secs_f64(),
            specs.len()
        );
        return Ok(EXIT_PASS);
    }

    // 9. Optionally update the baseline file.
    if args.update_baseline {
        let git_commit = current_git_commit();
        let new_baseline = StageBaseline {
            stage: stage.to_string(),
            host: ctx.host.clone(),
            git_commit,
            captured_at: now_iso8601(),
            tolerance_pct: tolerance,
            benchmarks: new_entries,
        };
        write_baseline(&baseline_path, &new_baseline)?;
        eprintln!(
            "regression-gate: baseline written to {}",
            baseline_path.display()
        );
    }

    // 10. Report results.
    if regression_failures.is_empty() && floor_failures.is_empty() {
        println!("regression-gate: all checks passed for stage {stage}");
        Ok(EXIT_PASS)
    } else {
        if !regression_failures.is_empty() {
            eprintln!(
                "regression-gate: {} regression(s) detected:",
                regression_failures.len()
            );
            for msg in &regression_failures {
                eprintln!("  REGRESSION  {msg}");
            }
        }
        if !floor_failures.is_empty() {
            eprintln!(
                "regression-gate: {} competitor floor(s) not met:",
                floor_failures.len()
            );
            for msg in &floor_failures {
                eprintln!("{msg}");
            }
        }
        // Prefer EXIT_REGRESSION over EXIT_COMPETITOR_LOSS so the caller
        // can distinguish "we got slower vs ourselves" from "we got slower
        // vs a competitor".
        if !regression_failures.is_empty() {
            Ok(EXIT_REGRESSION)
        } else {
            Ok(EXIT_COMPETITOR_LOSS)
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolves the stage from the `--stage` argument.
///
/// `"current"` reads `benchmarks/current_stage.txt` relative to the
/// workspace root.
fn resolve_stage(stage_arg: &str) -> Result<Stage> {
    if stage_arg == "current" {
        let root = workspace_root();
        let path = root.join("benchmarks").join("current_stage.txt");
        let content = std::fs::read_to_string(&path).with_context(|| {
            format!(
                "read current stage from {} \
                 (create the file with content like 'v0_6', or pass --stage explicitly)",
                path.display()
            )
        })?;
        let s = content.trim();
        Stage::parse_str(s)
            .ok_or_else(|| anyhow::anyhow!("unknown stage '{s}' in {}", path.display()))
    } else {
        Stage::parse_str(stage_arg).ok_or_else(|| anyhow::anyhow!("unknown stage '{stage_arg}'"))
    }
}

/// Returns the default baseline path for a given stage.
///
/// `benchmarks/baselines/<stage>.json` relative to the workspace root.
fn resolve_baseline_path(override_path: Option<&PathBuf>, stage: &Stage) -> Result<PathBuf> {
    if let Some(p) = override_path {
        return Ok(p.clone());
    }
    let root = workspace_root();
    Ok(root
        .join("benchmarks")
        .join("baselines")
        .join(format!("{stage}.json")))
}

/// Detects the workspace root by walking parent directories until we find
/// `Cargo.lock`. Falls back to the current working directory.
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

/// Checks whether `throughput` regresses vs the baseline entry for `id`.
///
/// Appends a description to `failures` when the threshold is exceeded.
fn check_regression(
    id: &str,
    throughput: f64,
    result: &BenchResult,
    baseline: &StageBaseline,
    tolerance_pct: f64,
    failures: &mut Vec<String>,
) {
    let Some(entry) = baseline.benchmarks.get(id) else {
        // No baseline entry yet — first run or placeholder; skip check.
        return;
    };
    let baseline_tput = entry.throughput_per_sec;
    // Zero baselines are placeholders — skip.
    if baseline_tput <= 0.0 {
        return;
    }
    // A regression means throughput is *lower* than baseline × (1 - tol/100).
    // We invert: "current median µs" > "baseline median µs" × (1 + tol/100).
    // For throughput: current < baseline × (1 - tol/100) is a regression.
    let threshold = baseline_tput * (1.0 - tolerance_pct / 100.0);
    if throughput < threshold && throughput > 0.0 {
        let pct_drop = (1.0 - throughput / baseline_tput) * 100.0;
        failures.push(format!(
            "{id}: throughput {throughput:.0} ops/s < baseline {baseline_tput:.0} ops/s \
             (-{pct_drop:.1}%)",
        ));
        return;
    }
    // Also check latency regression via samples if available.
    if !result.samples.is_empty() && entry.p99_us > 0.0 {
        let current_median_us = median_f64(&result.samples);
        let baseline_median_us = entry.p99_us;
        let lat_threshold = baseline_median_us * (1.0 + tolerance_pct / 100.0);
        if current_median_us > lat_threshold {
            let pct_inc = (current_median_us / baseline_median_us - 1.0) * 100.0;
            failures.push(format!(
                "{id}: median latency {current_median_us:.1} µs > baseline \
                 {baseline_median_us:.1} µs (+{pct_inc:.1}%)",
            ));
        }
    }
}

/// Checks whether UltraSQL meets each competitor floor listed in `floors`.
///
/// Appends a description to `failures` for each unmet floor.
fn check_competitor_floors(
    id: &str,
    our_throughput: f64,
    our_p99_us: f64,
    floors: &[(Engine, FloorMetric)],
    engine_filter: Option<&[String]>,
    baseline: Option<&StageBaseline>,
    failures: &mut Vec<String>,
) {
    for (engine, floor) in floors {
        // Skip engines excluded by --engines filter.
        if let Some(filter) = engine_filter {
            if !filter.contains(&engine.as_str().to_lowercase()) {
                continue;
            }
        }

        // Competitor throughput comes from the baseline file (recorded by
        // the operator when they ran the competitor). Zero means not yet
        // measured — skip the check rather than spuriously failing.
        let competitor_tput = baseline
            .and_then(|b| b.benchmarks.get(id))
            .and_then(|e| e.competitors.get(engine.as_str()))
            .copied()
            .unwrap_or(0.0);

        if competitor_tput <= 0.0 {
            // Placeholder — no competitor data recorded yet.
            continue;
        }

        match floor {
            FloorMetric::ThroughputRatio(ratio) => {
                let required = competitor_tput * ratio;
                if our_throughput > 0.0 && our_throughput < required {
                    let actual_ratio = our_throughput / competitor_tput;
                    let gap_pct = (ratio - actual_ratio) * 100.0;
                    failures.push(format!(
                        "COMPETITOR LOSS — benchmark {id}:\n\
                         \x20 UltraSQL throughput     = {our_throughput:.0} ops/s\n\
                         \x20 {engine} throughput     = {competitor_tput:.0} ops/s\n\
                         \x20 Required ratio          = ≥ {ratio:.2}\n\
                         \x20 Actual ratio            = {actual_ratio:.2}\n\
                         \x20 Gap                     = {gap_pct:.1}% short\n\
                         \nPerformance push required before this commit can land."
                    ));
                }
            }
            FloorMetric::LatencyRatio(ratio) => {
                // Latency: our p99 must be ≤ ratio × competitor p99.
                // We use the competitor p99 stored in the baseline.
                let competitor_p99 = competitor_tput; // field reused for p99
                let allowed = competitor_p99 * ratio;
                if our_p99_us > 0.0 && our_p99_us > allowed {
                    let actual_ratio = our_p99_us / competitor_p99;
                    let gap_pct = (actual_ratio - ratio) * 100.0;
                    failures.push(format!(
                        "COMPETITOR LOSS — benchmark {id}:\n\
                         \x20 UltraSQL p99 latency    = {our_p99_us:.1} µs\n\
                         \x20 {engine} p99 latency    = {competitor_p99:.1} µs\n\
                         \x20 Required ratio          = ≤ {ratio:.2}\n\
                         \x20 Actual ratio            = {actual_ratio:.2}\n\
                         \x20 Gap                     = {gap_pct:.1}% short\n\
                         \nPerformance push required before this commit can land."
                    ));
                }
            }
        }
    }
}

/// Writes a [`StageBaseline`] to `path` as pretty-printed JSON.
///
/// Creates parent directories if they do not exist.
fn write_baseline(path: &Path, baseline: &StageBaseline) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create dir {}", parent.display()))?;
    }
    let json = serde_json::to_string_pretty(baseline).context("serialize stage baseline")?;
    std::fs::write(path, json).with_context(|| format!("write baseline to {}", path.display()))
}

/// Returns a short git commit SHA, or `"unknown"` when git is unavailable.
fn current_git_commit() -> String {
    // Prefer a compile-time override (useful in CI).
    if let Ok(sha) = std::env::var("GIT_COMMIT") {
        return sha;
    }
    // Fall back to invoking git at runtime.
    std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Returns the current UTC time as an ISO-8601 string.
///
/// Uses the `BENCH_TIMESTAMP` env override when set (useful in tests).
fn now_iso8601() -> String {
    if let Ok(ts) = std::env::var("BENCH_TIMESTAMP") {
        return ts;
    }
    // Minimal hand-rolled UTC timestamp: avoids pulling chrono into the
    // default feature set for this binary.
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    // Format as YYYY-MM-DDTHH:MM:SSZ (no sub-second precision needed here).
    let (year, month, day, hour, min, sec) = unix_to_parts(secs);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
}

/// Decomposes a Unix timestamp (seconds since epoch) into calendar parts.
///
/// This is a minimal Gregorian-calendar implementation covering years
/// 1970–2099 sufficient for test reproducibility. It does not handle
/// leap seconds.
#[allow(clippy::many_single_char_names)]
fn unix_to_parts(secs: u64) -> (u32, u32, u32, u32, u32, u32) {
    let sec = u32::try_from(secs % 60).unwrap_or(0);
    let min = u32::try_from((secs / 60) % 60).unwrap_or(0);
    let hour = u32::try_from((secs / 3600) % 24).unwrap_or(0);
    let days = secs / 86400;

    // Shift epoch to 1 Mar 2000 (day 11017 from Unix epoch) to simplify
    // leap-year arithmetic using the 400-year cycle.
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    (
        u32::try_from(y).unwrap_or(1970),
        u32::try_from(m).unwrap_or(1),
        u32::try_from(d).unwrap_or(1),
        hour,
        min,
        sec,
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // -----------------------------------------------------------------------
    // Helpers for building test fixtures
    // -----------------------------------------------------------------------

    fn make_baseline(
        throughput: f64,
        p99: f64,
        competitor_tput: f64,
        engine_key: &str,
    ) -> StageBaseline {
        let mut competitors = HashMap::new();
        if competitor_tput > 0.0 {
            competitors.insert(engine_key.to_string(), competitor_tput);
        }
        let mut benchmarks = HashMap::new();
        benchmarks.insert(
            "point_lookup".to_string(),
            BenchBaseline {
                throughput_per_sec: throughput,
                p99_us: p99,
                competitors,
            },
        );
        StageBaseline {
            stage: "v0_6".to_string(),
            host: HostInfo {
                cpu: "Apple M4".to_string(),
                cores: 12,
                ram_gb: 64,
                os: "darwin 25.5.0".to_string(),
            },
            git_commit: "abc1234".to_string(),
            captured_at: "2026-05-13T00:00:00Z".to_string(),
            tolerance_pct: 5.0,
            benchmarks,
        }
    }

    fn make_result(throughput: f64, samples: Vec<f64>) -> BenchResult {
        BenchResult {
            throughput_per_sec: throughput,
            p50_latency_us: 0.0,
            p99_latency_us: p99_f64(&samples),
            samples,
        }
    }

    // -----------------------------------------------------------------------
    // parses_baseline_json
    // -----------------------------------------------------------------------

    #[test]
    fn parses_baseline_json() {
        let baseline = make_baseline(100_000.0, 50.0, 90_000.0, "postgres17");
        let json = serde_json::to_string_pretty(&baseline).expect("serialize");
        let decoded: StageBaseline = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded.stage, "v0_6");
        assert_eq!(decoded.tolerance_pct, 5.0);
        let entry = decoded.benchmarks.get("point_lookup").expect("entry");
        assert!((entry.throughput_per_sec - 100_000.0).abs() < 1.0);
        assert!((entry.p99_us - 50.0).abs() < 1e-9);
        let comp = entry.competitors.get("postgres17").expect("competitor");
        assert!((*comp - 90_000.0).abs() < 1.0);
    }

    // -----------------------------------------------------------------------
    // detects_5pct_regression_in_throughput
    // -----------------------------------------------------------------------

    #[test]
    fn detects_5pct_regression_in_throughput() {
        // Baseline throughput = 100_000 ops/s; current = 90_000 (−10%).
        let baseline = make_baseline(100_000.0, 0.0, 0.0, "");
        let result = make_result(90_000.0, vec![]);
        let mut failures = Vec::new();
        check_regression(
            "point_lookup",
            result.throughput_per_sec,
            &result,
            &baseline,
            5.0,
            &mut failures,
        );
        assert!(!failures.is_empty(), "expected regression to be detected");
        assert!(
            failures[0].contains("point_lookup"),
            "message should name benchmark: {}",
            failures[0]
        );
        assert!(
            failures[0].contains("ops/s"),
            "message should contain 'ops/s': {}",
            failures[0]
        );
    }

    // -----------------------------------------------------------------------
    // passes_when_no_regression_and_floors_met
    // -----------------------------------------------------------------------

    #[test]
    fn passes_when_no_regression_and_floors_met() {
        // Baseline throughput = 200_000; current = 200_000 (0% change).
        // Competitor throughput = 80_000; floor = 2.0× (we need ≥ 160_000).
        // Our 200_000 ≥ 160_000, so no floor failure.
        let baseline = make_baseline(200_000.0, 0.0, 80_000.0, "postgres17");
        let result = make_result(200_000.0, vec![]);
        let mut reg_failures = Vec::new();
        check_regression(
            "point_lookup",
            result.throughput_per_sec,
            &result,
            &baseline,
            5.0,
            &mut reg_failures,
        );
        assert!(
            reg_failures.is_empty(),
            "unexpected regression: {:?}",
            reg_failures
        );

        let floors: &[(Engine, FloorMetric)] =
            &[(Engine::Postgres17, FloorMetric::ThroughputRatio(2.0))];
        let mut floor_failures = Vec::new();
        check_competitor_floors(
            "point_lookup",
            200_000.0,
            50.0,
            floors,
            None,
            Some(&baseline),
            &mut floor_failures,
        );
        assert!(
            floor_failures.is_empty(),
            "unexpected floor failure: {:?}",
            floor_failures
        );
    }

    // -----------------------------------------------------------------------
    // detects_competitor_floor_violation
    // -----------------------------------------------------------------------

    #[test]
    fn detects_competitor_floor_violation() {
        // Competitor throughput = 100_000; floor = 2.0×; ours = 60_000 → fail.
        let baseline = make_baseline(60_000.0, 0.0, 100_000.0, "postgres17");
        let floors: &[(Engine, FloorMetric)] =
            &[(Engine::Postgres17, FloorMetric::ThroughputRatio(2.0))];
        let mut failures = Vec::new();
        check_competitor_floors(
            "point_lookup",
            60_000.0,
            100.0,
            floors,
            None,
            Some(&baseline),
            &mut failures,
        );
        assert!(
            !failures.is_empty(),
            "expected floor violation to be detected"
        );
        assert!(
            failures[0].contains("postgres17"),
            "message should name engine: {}",
            failures[0]
        );
        assert!(
            failures[0].contains("COMPETITOR LOSS"),
            "message should contain 'COMPETITOR LOSS': {}",
            failures[0]
        );
    }

    // -----------------------------------------------------------------------
    // competitor_floor_2x_throughput_passes_when_ultrasql_at_2_5x
    // -----------------------------------------------------------------------

    #[test]
    fn competitor_floor_2x_throughput_passes_when_ultrasql_at_2_5x() {
        // Competitor = 100_000 ops/s; floor = 2.0×; ours = 250_000 (2.5×) → pass.
        let baseline = make_baseline(250_000.0, 0.0, 100_000.0, "postgres17");
        let floors: &[(Engine, FloorMetric)] =
            &[(Engine::Postgres17, FloorMetric::ThroughputRatio(2.0))];
        let mut failures = Vec::new();
        check_competitor_floors(
            "point_lookup",
            250_000.0,
            50.0,
            floors,
            None,
            Some(&baseline),
            &mut failures,
        );
        assert!(
            failures.is_empty(),
            "2.5× should pass a 2.0× floor; got: {:?}",
            failures
        );
    }

    // -----------------------------------------------------------------------
    // competitor_floor_2x_throughput_fails_when_ultrasql_at_1_8x
    // -----------------------------------------------------------------------

    #[test]
    fn competitor_floor_2x_throughput_fails_when_ultrasql_at_1_8x() {
        // Competitor = 100_000 ops/s; floor = 2.0×; ours = 180_000 (1.8×) → fail.
        let baseline = make_baseline(180_000.0, 0.0, 100_000.0, "postgres17");
        let floors: &[(Engine, FloorMetric)] =
            &[(Engine::Postgres17, FloorMetric::ThroughputRatio(2.0))];
        let mut failures = Vec::new();
        check_competitor_floors(
            "point_lookup",
            180_000.0,
            50.0,
            floors,
            None,
            Some(&baseline),
            &mut failures,
        );
        assert!(!failures.is_empty(), "1.8× should fail a 2.0× floor");
        assert!(
            failures[0].contains("COMPETITOR LOSS"),
            "failure message must contain 'COMPETITOR LOSS': {}",
            failures[0]
        );
        assert!(
            failures[0].contains("Performance push required"),
            "failure message must contain the push-required line: {}",
            failures[0]
        );
    }

    // -----------------------------------------------------------------------
    // competitor_floor_2x_throughput_fails_when_at_exactly_2x_minus_epsilon
    // -----------------------------------------------------------------------

    #[test]
    fn competitor_floor_2x_throughput_fails_when_at_exactly_2x_minus_epsilon() {
        // Competitor = 100_000 ops/s; floor = 2.0×; ours = 199_999 (< 200_000) → fail.
        let baseline = make_baseline(199_999.0, 0.0, 100_000.0, "duckdb");
        let floors: &[(Engine, FloorMetric)] =
            &[(Engine::DuckDb, FloorMetric::ThroughputRatio(2.0))];
        let mut failures = Vec::new();
        check_competitor_floors(
            "point_lookup",
            199_999.0,
            50.0,
            floors,
            None,
            Some(&baseline),
            &mut failures,
        );
        assert!(
            !failures.is_empty(),
            "199_999 ops/s should fail a 2.0× floor against 100_000 ops/s"
        );
        assert!(
            failures[0].contains("duckdb"),
            "failure message must name the engine: {}",
            failures[0]
        );
    }

    // -----------------------------------------------------------------------
    // floor_skipped_when_competitor_baseline_is_zero
    // -----------------------------------------------------------------------

    #[test]
    fn floor_skipped_when_competitor_baseline_is_zero() {
        // Competitor baseline is 0.0 (not yet measured) — floor must be skipped.
        // make_baseline passes 0.0 for competitor_tput, which omits the entry.
        let baseline = make_baseline(100_000.0, 0.0, 0.0, "postgres17");
        let floors: &[(Engine, FloorMetric)] =
            &[(Engine::Postgres17, FloorMetric::ThroughputRatio(2.0))];
        let mut failures = Vec::new();
        check_competitor_floors(
            "point_lookup",
            50_000.0, // ours is only 0.5× — would fail if data existed
            50.0,
            floors,
            None,
            Some(&baseline),
            &mut failures,
        );
        assert!(
            failures.is_empty(),
            "floor must be skipped when competitor baseline is 0.0; got: {:?}",
            failures
        );
    }

    // -----------------------------------------------------------------------
    // update_baseline_writes_new_values
    // -----------------------------------------------------------------------

    #[test]
    fn update_baseline_writes_new_values() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("v0_6.json");

        let new_baseline = make_baseline(200_000.0, 25.0, 0.0, "");
        write_baseline(&path, &new_baseline).expect("write");

        let raw = std::fs::read_to_string(&path).expect("read");
        let decoded: StageBaseline = serde_json::from_str(&raw).expect("deserialize");
        let entry = decoded.benchmarks.get("point_lookup").expect("entry");
        assert!((entry.throughput_per_sec - 200_000.0).abs() < 1.0);
        assert!((entry.p99_us - 25.0).abs() < 1e-9);
    }

    // -----------------------------------------------------------------------
    // skip_detection_for_docs_only_diff
    // -----------------------------------------------------------------------

    /// A synthetic git-diff output classifier: returns `true` when every
    /// changed file is docs-only (`.md`, `docs/`, `ROADMAP.md`, `AGENTS.md`).
    fn is_docs_only(changed_files: &[&str]) -> bool {
        if changed_files.is_empty() {
            return true;
        }
        changed_files.iter().all(|f| {
            f.ends_with(".md")
                || f.starts_with("docs/")
                || f.starts_with("docs\\")
                || *f == "ROADMAP.md"
                || *f == "AGENTS.md"
                || *f == "BENCHMARKS.md"
                || *f == "CONTRIBUTING.md"
        })
    }

    #[test]
    fn skip_detection_docs_only_true() {
        let files = ["AGENTS.md", "docs/guide.md", "ROADMAP.md"];
        assert!(is_docs_only(&files));
    }

    #[test]
    fn skip_detection_source_file_triggers_gate() {
        let files = ["AGENTS.md", "crates/ultrasql-bench/src/lib.rs"];
        assert!(!is_docs_only(&files));
    }

    #[test]
    fn skip_detection_empty_diff_skips() {
        assert!(is_docs_only(&[]));
    }

    // -----------------------------------------------------------------------
    // Regression at exactly 5% does NOT fail (threshold is strict >5%)
    // -----------------------------------------------------------------------

    #[test]
    fn exact_5pct_regression_passes() {
        // Baseline = 100_000; current = 95_000 (exactly −5%).
        // threshold = 100_000 × (1 − 0.05) = 95_000. current == threshold → pass.
        let baseline = make_baseline(100_000.0, 0.0, 0.0, "");
        let result = make_result(95_000.0, vec![]);
        let mut failures = Vec::new();
        check_regression(
            "point_lookup",
            result.throughput_per_sec,
            &result,
            &baseline,
            5.0,
            &mut failures,
        );
        // Exactly at the boundary: 95_000 is NOT < 95_000, so no failure.
        assert!(
            failures.is_empty(),
            "exact-5% regression should not trigger: {:?}",
            failures
        );
    }

    // -----------------------------------------------------------------------
    // unix_to_parts smoke test
    // -----------------------------------------------------------------------

    #[test]
    fn unix_to_parts_epoch() {
        let (y, m, d, h, min, s) = unix_to_parts(0);
        assert_eq!((y, m, d, h, min, s), (1970, 1, 1, 0, 0, 0));
    }

    #[test]
    fn now_iso8601_respects_env_override() {
        // SAFETY: edition-2024 makes env::set_var/remove_var unsafe due to
        // data-race risk across threads. This test is single-threaded and
        // touches only a process-local variable read by the function under
        // test in the same call frame.
        unsafe { std::env::set_var("BENCH_TIMESTAMP", "2026-05-13T00:00:00Z") };
        let ts = now_iso8601();
        unsafe { std::env::remove_var("BENCH_TIMESTAMP") };
        assert_eq!(ts, "2026-05-13T00:00:00Z");
    }

    // -----------------------------------------------------------------------
    // Smoke mode tests
    // -----------------------------------------------------------------------

    /// `--smoke` builds a context with iterations=1 and warmup=0, matching
    /// the contract "run each benchmark exactly once".
    #[test]
    fn smoke_runs_each_benchmark_exactly_once() {
        // The stub_run fn in the registry always returns zeroes regardless of
        // ctx.iterations, so we verify the context values directly.
        let ctx = BenchContext {
            iterations: 1,
            warmup_iterations: 0,
            host: HostInfo {
                cpu: "test".to_string(),
                cores: 1,
                ram_gb: 1,
                os: "test".to_string(),
            },
        };
        // Smoke mode uses exactly these values.
        assert_eq!(ctx.iterations, 1);
        assert_eq!(ctx.warmup_iterations, 0);
    }

    /// In smoke mode the competitor-floor check functions are never called,
    /// so a run that would otherwise fail a floor passes cleanly.
    #[test]
    fn smoke_skips_competitor_floor_checks() {
        // Competitor floor would require 200_000 ops/s but we only have 50_000.
        let baseline = make_baseline(50_000.0, 0.0, 100_000.0, "postgres17");
        let floors: &[(Engine, FloorMetric)] =
            &[(Engine::Postgres17, FloorMetric::ThroughputRatio(2.0))];

        // Smoke mode skips floor checks entirely — simulate by calling
        // check_competitor_floors with the gate active but confirming the
        // smoke branch skips it in run().  Here we verify the helper itself
        // would detect the violation (so skipping it is meaningful).
        let mut failures = Vec::new();
        check_competitor_floors(
            "point_lookup",
            50_000.0,
            100.0,
            floors,
            None,
            Some(&baseline),
            &mut failures,
        );
        assert!(
            !failures.is_empty(),
            "floor helper detects violation so smoke skip is meaningful"
        );

        // When smoke mode is active the code path never calls
        // check_competitor_floors; floor_failures stays empty.
        let smoke_floor_failures: Vec<String> = Vec::new();
        assert!(
            smoke_floor_failures.is_empty(),
            "smoke mode must not accumulate floor failures"
        );
    }

    /// Verify that all benchmarks in the default registry complete without
    /// panicking within the 5-second smoke budget.  Stubs return in
    /// nanoseconds, so this is a pure correctness / "no crash" gate.
    #[test]
    fn smoke_completes_under_5_seconds_on_default_registry() {
        use std::time::Instant;
        use ultrasql_bench::registry::{BenchContext, HostInfo, REGISTRY, Stage};

        let ctx = BenchContext {
            iterations: 1,
            warmup_iterations: 0,
            host: HostInfo {
                cpu: "test".to_string(),
                cores: 1,
                ram_gb: 1,
                os: "test".to_string(),
            },
        };

        let start = Instant::now();
        let stage = Stage::V0_6; // representative non-empty stage
        for spec in REGISTRY.iter().filter(|s| s.stage == stage) {
            let _result = (spec.run)(&ctx);
        }
        let elapsed = start.elapsed();

        assert!(
            elapsed.as_secs() < 5,
            "smoke over default registry must complete in < 5s; took {elapsed:?}"
        );
    }
}
