//! Baseline JSON read/write and regression detection.
//!
//! A baseline captures per-query median and p95 timings for a specific engine,
//! scale factor, host, and commit. The [`compare`] function reads two baselines
//! and returns an error when any query in `current` is more than 5% slower
//! than the corresponding query in `recorded`.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

/// Schema version for forward-compatibility checks.
pub const SCHEMA_VERSION: u32 = 1;

/// Per-query timing record stored in the baseline file.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
// f64 fields make Eq non-derivable; we compare with approx tolerance in tests.
pub struct QueryTimings {
    /// Median elapsed time across all measured runs, in milliseconds.
    pub median_ms: f64,
    /// 95th-percentile elapsed time across all measured runs, in milliseconds.
    pub p95_ms: f64,
    /// Raw per-run elapsed times in milliseconds, in execution order.
    pub runs: Vec<f64>,
}

/// Host descriptor embedded in every baseline file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostDescriptor {
    /// CPU model string, e.g. `"Apple M4"`.
    pub cpu: String,
    /// Number of logical CPU cores.
    pub cores: u32,
    /// Total system RAM in gigabytes (rounded).
    pub ram_gb: u32,
    /// Operating-system description, e.g. `"darwin 25.5.0"`.
    pub os: String,
}

/// Top-level baseline document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
// f64 values inside QueryTimings prevent full Eq.
pub struct Baseline {
    /// Must equal [`SCHEMA_VERSION`] when reading; always written as `1`.
    pub schema_version: u32,
    /// TPC-H scale factor (1, 10, 100, …).
    pub scale_factor: u32,
    /// Engine identifier string, e.g. `"postgres@17"`.
    pub engine: String,
    /// Host on which the measurements were taken.
    pub host: HostDescriptor,
    /// Short git commit SHA, e.g. `"abc1234"`.
    pub git_commit: String,
    /// SHA-256 hex digest of `Cargo.lock` at measurement time.
    pub cargo_lock_sha256: String,
    /// ISO-8601 timestamp when the baseline was recorded.
    pub recorded_at: String,
    /// Per-query timing results keyed by `"q1"` through `"q22"`.
    pub queries: BTreeMap<String, QueryTimings>,
}

impl Baseline {
    /// Writes this baseline to `path` as pretty-printed JSON.
    ///
    /// The parent directory must already exist.
    pub fn write(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self).context("serialize baseline")?;
        std::fs::write(path, json).with_context(|| format!("write baseline to {}", path.display()))
    }

    /// Reads a baseline from a JSON file at `path`.
    pub fn read(path: &Path) -> Result<Self> {
        let raw =
            std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        let b: Self =
            serde_json::from_str(&raw).with_context(|| format!("parse {}", path.display()))?;
        if b.schema_version != SCHEMA_VERSION {
            bail!(
                "unsupported baseline schema version {} (expected {})",
                b.schema_version,
                SCHEMA_VERSION
            );
        }
        Ok(b)
    }
}

/// Compares `current` timings against `recorded` and returns an error listing
/// every query whose `median_ms` exceeds `recorded.median_ms * 1.05`.
///
/// Returns `Ok(())` when no query regresses beyond the 5% threshold.
pub fn compare(recorded: &Baseline, current: &Baseline) -> Result<()> {
    let mut regressions: Vec<String> = Vec::new();

    for (key, rec_timing) in &recorded.queries {
        if let Some(cur_timing) = current.queries.get(key) {
            let threshold = rec_timing.median_ms * 1.05;
            if cur_timing.median_ms > threshold {
                let pct = (cur_timing.median_ms / rec_timing.median_ms - 1.0) * 100.0;
                regressions.push(format!(
                    "{key}: current {:.2} ms > baseline {:.2} ms (+{pct:.1}%)",
                    cur_timing.median_ms, rec_timing.median_ms
                ));
            }
        }
    }

    if regressions.is_empty() {
        Ok(())
    } else {
        bail!(
            "{} regression(s) detected (threshold: 5%):\n  {}",
            regressions.len(),
            regressions.join("\n  ")
        )
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Computes the median of a non-empty slice of `f64` values.
///
/// Returns `0.0` when `values` is empty.
pub fn median(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = sorted.len();
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        f64::midpoint(sorted[n / 2 - 1], sorted[n / 2])
    }
}

/// Computes the 95th percentile of a non-empty slice of `f64` values.
///
/// Uses nearest-rank method. Returns `0.0` when `values` is empty.
pub fn p95(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    // Nearest-rank: ceil(0.95 * n) - 1, clamped to valid range.
    let n = sorted.len();
    let rank = (95_usize * n).div_ceil(100).min(n) - 1;
    sorted[rank]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn make_baseline(q1_median: f64) -> Baseline {
        let mut queries = BTreeMap::new();
        queries.insert(
            "q1".to_string(),
            QueryTimings {
                median_ms: q1_median,
                p95_ms: q1_median * 1.1,
                runs: vec![q1_median],
            },
        );
        Baseline {
            schema_version: SCHEMA_VERSION,
            scale_factor: 1,
            engine: "postgres@17".to_string(),
            host: HostDescriptor {
                cpu: "Apple M4".to_string(),
                cores: 10,
                ram_gb: 24,
                os: "darwin 25.5.0".to_string(),
            },
            git_commit: "abc1234".to_string(),
            cargo_lock_sha256: "0".repeat(64),
            recorded_at: "2026-05-13T00:00:00Z".to_string(),
            queries,
        }
    }

    #[test]
    fn baseline_round_trip_serde() {
        let original = make_baseline(123.4);
        let json = serde_json::to_string_pretty(&original).expect("serialize");
        let decoded: Baseline = serde_json::from_str(&json).expect("deserialize");
        // Compare non-f64 fields structurally; f64 fields with tolerance.
        assert_eq!(original.schema_version, decoded.schema_version);
        assert_eq!(original.engine, decoded.engine);
        assert!(
            (decoded.queries["q1"].median_ms - 123.4).abs() < 1e-9,
            "median_ms round-trip failed"
        );
    }

    #[test]
    fn compare_detects_5pct_regression() {
        // current is 10% slower — should fail.
        let recorded = make_baseline(100.0);
        let current = make_baseline(110.0);
        let result = compare(&recorded, &current);
        assert!(result.is_err(), "expected regression error");
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("q1"), "error message should name q1: {msg}");
        assert!(msg.contains('+'), "error message should show delta: {msg}");
    }

    #[test]
    fn compare_passes_within_tolerance() {
        // current is 3% slower — within the 5% gate.
        let recorded = make_baseline(100.0);
        let current = make_baseline(103.0);
        assert!(compare(&recorded, &current).is_ok());
    }

    #[test]
    fn compare_passes_at_exact_threshold() {
        // exactly 5% slower — NOT over; should pass.
        let recorded = make_baseline(100.0);
        let current = make_baseline(105.0);
        assert!(compare(&recorded, &current).is_ok());
    }

    #[test]
    fn median_odd() {
        assert!((median(&[3.0, 1.0, 2.0]) - 2.0).abs() < 1e-9);
    }

    #[test]
    fn median_even() {
        assert!((median(&[1.0, 2.0, 3.0, 4.0]) - 2.5).abs() < 1e-9);
    }

    #[test]
    fn median_empty() {
        assert!((median(&[]) - 0.0).abs() < 1e-9);
    }

    #[test]
    fn p95_basic() {
        let vals: Vec<f64> = (1..=20).map(f64::from).collect();
        // nearest-rank of 20 values: ceil(0.95*20)=19, index 18 => 19.0
        assert!((p95(&vals) - 19.0).abs() < 1e-9);
    }
}
