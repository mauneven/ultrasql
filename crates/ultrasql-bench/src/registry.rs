//! Stage-tagged benchmark registry.
//!
//! Every benchmark in the workspace registers itself with a stage tag
//! (`V0_3`, `V0_4`, …) and an optional set of competitor floors. The
//! regression gate iterates [`REGISTRY`], runs each benchmark whose
//! stage matches the requested filter, and enforces:
//!
//! 1. No >5% throughput regression vs the corresponding entry in the
//!    stage's `benchmarks/baselines/<stage>.json` file.
//! 2. For every engine listed in a spec's [`BenchSpec::competitor_floors`],
//!    UltraSQL's metric meets the declared [`FloorMetric`].
//!
//! # Adding a new benchmark
//!
//! 1. Write a `fn run_<name>(ctx: &BenchContext) -> BenchResult` in an
//!    appropriate module.
//! 2. Add a `BenchSpec` to the `SPECS` slice at the bottom of this file
//!    and make sure it appears in `REGISTRY`.
//! 3. Add a zero-value placeholder entry to the relevant
//!    `benchmarks/baselines/<stage>.json` file (the `--update-baseline`
//!    flag of `regression-gate` will fill in real values on first run).

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A single benchmark specification.
///
/// The `id` must be unique across all entries in [`REGISTRY`] and stable
/// over time — it is the key used in baseline JSON files.
#[derive(Debug)]
pub struct BenchSpec {
    /// Stable, unique identifier for this benchmark (e.g. `"point_lookup"`).
    pub id: &'static str,
    /// Development stage at which this benchmark was introduced and remains
    /// part of the gate.
    pub stage: Stage,
    /// Logical workload category.
    pub workload: Workload,
    /// Engines against which UltraSQL is compared, together with the minimum
    /// acceptable metric ratio. An empty slice means no competitor floor is
    /// enforced for this benchmark.
    pub competitor_floors: &'static [(Engine, FloorMetric)],
    /// Function pointer that executes the benchmark and returns timing
    /// samples. The implementation must respect the iteration counts in
    /// `ctx`.
    pub run: fn(&BenchContext) -> BenchResult,
}

/// Development stage that a benchmark targets.
///
/// Variants are in chronological order. The `regression-gate` binary
/// accepts `--stage` arguments that correspond to the kebab-case names
/// of these variants (e.g. `v0_3` → `V0_3`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Stage {
    /// v0.2 baseline — parser + planner completeness.
    V0_2,
    /// v0.3 — page-and-pool storage engine.
    V0_3,
    /// v0.4 — full ACID transactions.
    V0_4,
    /// v0.5 — full physical executor + extended protocol.
    V0_5,
    /// v0.6 — cost-based optimizer.
    V0_6,
    /// v0.7 — vectorized execution.
    V0_7,
    /// v0.8 — indexes and constraints.
    V0_8,
    /// v0.9 — production operations (replication, backup, COPY).
    V0_9,
    /// v1.0 — general availability.
    V1_0,
}

impl Stage {
    /// Returns the kebab-case string that identifies this stage in CLI
    /// arguments and baseline filenames (e.g. `V0_3` → `"v0_3"`).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::V0_2 => "v0_2",
            Self::V0_3 => "v0_3",
            Self::V0_4 => "v0_4",
            Self::V0_5 => "v0_5",
            Self::V0_6 => "v0_6",
            Self::V0_7 => "v0_7",
            Self::V0_8 => "v0_8",
            Self::V0_9 => "v0_9",
            Self::V1_0 => "v1_0",
        }
    }

    /// Parses a stage from its kebab-case CLI string.
    ///
    /// Returns `None` when the string does not match any known stage.
    #[must_use]
    pub fn parse_str(s: &str) -> Option<Self> {
        match s {
            "v0_2" => Some(Self::V0_2),
            "v0_3" => Some(Self::V0_3),
            "v0_4" => Some(Self::V0_4),
            "v0_5" => Some(Self::V0_5),
            "v0_6" => Some(Self::V0_6),
            "v0_7" => Some(Self::V0_7),
            "v0_8" => Some(Self::V0_8),
            "v0_9" => Some(Self::V0_9),
            "v1_0" => Some(Self::V1_0),
            _ => None,
        }
    }
}

impl std::fmt::Display for Stage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Logical workload category. Used for display and filtering.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Workload {
    /// Single-row primary-key lookup.
    PointLookup,
    /// Sequential or B-tree range scan.
    RangeScan,
    /// Bulk INSERT throughput.
    InsertThroughput,
    /// UPDATE throughput (fetch + modify + write back).
    UpdateThroughput,
    /// DELETE throughput.
    DeleteThroughput,
    /// Mixed OLTP: reads + point-writes at a realistic ratio.
    MixedOltp,
    /// Aggregate over a wide column (SUM/COUNT/AVG/MIN/MAX).
    AnalyticAggregate,
    /// Join between two tables.
    Join,
    /// Hash aggregate over a large result set.
    HashAggregate,
    /// External sort of a large relation.
    SortLarge,
    /// TPC-H query 1 — aggregate with date filter.
    TpchQ1,
    /// TPC-H query 22 — correlated subquery over customer demographics.
    TpchQ22,
}

/// External database engines against which UltraSQL is compared.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Engine {
    /// PostgreSQL 17.
    Postgres17,
    /// DuckDB (current stable release at measurement time).
    DuckDb,
    /// `SQLite3` (current stable release at measurement time).
    Sqlite3,
    /// ClickHouse (current stable release at measurement time).
    ClickHouse,
    /// CockroachDB (current stable release at measurement time).
    CockroachDb,
}

impl Engine {
    /// Human-readable name for display and baseline JSON keys.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Postgres17 => "postgres17",
            Self::DuckDb => "duckdb",
            Self::Sqlite3 => "sqlite3",
            Self::ClickHouse => "clickhouse",
            Self::CockroachDb => "cockroachdb",
        }
    }
}

impl std::fmt::Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Minimum acceptable performance ratio between UltraSQL and a competitor.
///
/// Both variants express a ratio: values > 1.0 mean UltraSQL must outperform
/// the competitor, values < 1.0 allow UltraSQL to be proportionally slower.
/// The current ROADMAP target for v0.5 is ≥ 1.0× PostgreSQL on point lookups
/// and inserts; analytical targets widen to 2× at v0.7.
#[derive(Clone, Copy, Debug)]
pub enum FloorMetric {
    /// UltraSQL's throughput (ops/s or rows/s) must be ≥ `ratio ×`
    /// competitor throughput.
    ///
    /// Example: `ThroughputRatio(1.0)` means "at least as fast as the
    /// competitor". `ThroughputRatio(0.5)` means "no worse than 2× slower".
    ThroughputRatio(f64),

    /// UltraSQL's p99 latency must be ≤ `ratio ×` competitor p99 latency.
    ///
    /// Lower is better for latency, so `LatencyRatio(1.0)` means "p99 no
    /// worse than the competitor's p99". `LatencyRatio(2.0)` allows UltraSQL
    /// to be up to 2× slower at p99.
    LatencyRatio(f64),
}

/// Execution context passed to every benchmark `run` function.
///
/// The harness fills this in before invoking the function pointer stored in
/// [`BenchSpec::run`].
#[derive(Debug, Clone)]
pub struct BenchContext {
    /// Number of measured iterations (warmup runs excluded).
    pub iterations: u32,
    /// Number of warmup iterations discarded before measurement begins.
    pub warmup_iterations: u32,
    /// Host descriptor included in every result record.
    pub host: HostInfo,
}

/// Lightweight host description for annotating result records.
///
/// Fields are populated from environment variables (`BENCH_CPU_MODEL`,
/// `BENCH_CPU_CORES`, `BENCH_RAM_GB`, `BENCH_OS_VERSION`) or from
/// `std::env::consts` as a fallback.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostInfo {
    /// CPU model string, e.g. `"Apple M4"`.
    pub cpu: String,
    /// Number of logical CPU cores.
    pub cores: u32,
    /// Total system RAM in gigabytes (rounded).
    pub ram_gb: u32,
    /// Operating-system description, e.g. `"darwin 25.5.0"`.
    pub os: String,
}

impl HostInfo {
    /// Collects host info from environment variables with `std::env::consts`
    /// fallbacks.
    ///
    /// Environment variables checked (all optional):
    ///
    /// | Variable | Field |
    /// |----------|-------|
    /// | `BENCH_CPU_MODEL` | `cpu` |
    /// | `BENCH_CPU_CORES` | `cores` |
    /// | `BENCH_RAM_GB` | `ram_gb` |
    /// | `BENCH_OS_VERSION` | appended to `os` |
    #[must_use]
    pub fn from_env() -> Self {
        let cpu =
            std::env::var("BENCH_CPU_MODEL").unwrap_or_else(|_| std::env::consts::ARCH.to_string());
        let cores = std::env::var("BENCH_CPU_CORES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0_u32);
        let ram_gb = std::env::var("BENCH_RAM_GB")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0_u32);
        let os = format!(
            "{} {}",
            std::env::consts::OS,
            std::env::var("BENCH_OS_VERSION").unwrap_or_else(|_| "unknown".to_string())
        );
        Self {
            cpu,
            cores,
            ram_gb,
            os,
        }
    }
}

/// The result produced by a single benchmark run.
///
/// All time values are in microseconds (µs). The `samples` vec holds one
/// entry per measured iteration; warmup samples are excluded.
#[derive(Debug, Clone)]
pub struct BenchResult {
    /// Effective throughput in operations per second, computed as
    /// `n_operations / total_elapsed_seconds` for the median iteration.
    pub throughput_per_sec: f64,
    /// 50th-percentile iteration latency in microseconds.
    pub p50_latency_us: f64,
    /// 99th-percentile iteration latency in microseconds.
    pub p99_latency_us: f64,
    /// Raw per-iteration elapsed times in microseconds, in execution order.
    /// Used by the gate to detect whether the distribution has shifted.
    pub samples: Vec<f64>,
}

impl BenchResult {
    /// Computes the median of `samples`.
    ///
    /// Returns `0.0` when `samples` is empty.
    #[must_use]
    pub fn median_us(&self) -> f64 {
        median_f64(&self.samples)
    }
}

/// Computes the median of a slice of `f64` values.
///
/// Returns `0.0` for an empty slice. Does not mutate the input.
#[must_use]
pub fn median_f64(values: &[f64]) -> f64 {
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

/// Computes the p99 (99th-percentile) of a slice of `f64` values.
///
/// Uses the nearest-rank method. Returns `0.0` for an empty slice.
#[must_use]
pub fn p99_f64(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = sorted.len();
    let rank = (99_usize * n).div_ceil(100).min(n) - 1;
    sorted[rank]
}

// ---------------------------------------------------------------------------
// Stub implementations for the v0.6 starter set
// ---------------------------------------------------------------------------

/// Stub run function used for all six starter benchmarks.
///
/// Returns placeholder zeros so the registry compiles and the gate can parse
/// the baseline without a live UltraSQL execution path. Real implementations
/// will replace these stubs as the corresponding executor paths land.
const fn stub_run(_ctx: &BenchContext) -> BenchResult {
    BenchResult {
        throughput_per_sec: 0.0,
        p50_latency_us: 0.0,
        p99_latency_us: 0.0,
        samples: Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// Static slice of all registered benchmark specifications.
///
/// Entries are grouped by stage. Within a stage the order is not significant
/// but is kept stable for diff readability.
///
/// To add a new benchmark:
///
/// 1. Implement the run function (or use `stub_run` until the executor path
///    exists).
/// 2. Push a [`BenchSpec`] onto this slice.
/// 3. Add a zero-value entry to the matching
///    `benchmarks/baselines/<stage>.json`.
pub static REGISTRY: &[BenchSpec] = &SPECS;

static SPECS: [BenchSpec; 16] = [
    // ------------------------------------------------------------------
    // v0.3 — write-side (storage) benchmarks
    // ------------------------------------------------------------------
    BenchSpec {
        id: "insert_throughput_10k",
        stage: Stage::V0_3,
        workload: Workload::InsertThroughput,
        competitor_floors: &[(Engine::Postgres17, FloorMetric::ThroughputRatio(1.0))],
        run: stub_run,
    },
    BenchSpec {
        id: "update_throughput_10k",
        stage: Stage::V0_3,
        workload: Workload::UpdateThroughput,
        competitor_floors: &[(Engine::Postgres17, FloorMetric::ThroughputRatio(1.0))],
        run: stub_run,
    },
    BenchSpec {
        id: "delete_throughput_10k",
        stage: Stage::V0_3,
        workload: Workload::DeleteThroughput,
        competitor_floors: &[(Engine::Postgres17, FloorMetric::ThroughputRatio(1.0))],
        run: stub_run,
    },
    // ------------------------------------------------------------------
    // v0.5 — mixed OLTP
    // ------------------------------------------------------------------
    BenchSpec {
        id: "mixed_oltp_pgbench_like",
        stage: Stage::V0_5,
        workload: Workload::MixedOltp,
        competitor_floors: &[(Engine::Postgres17, FloorMetric::ThroughputRatio(1.0))],
        run: stub_run,
    },
    // ------------------------------------------------------------------
    // v0.6 — plan + execute benchmarks
    // ------------------------------------------------------------------
    BenchSpec {
        id: "point_lookup",
        stage: Stage::V0_6,
        workload: Workload::PointLookup,
        competitor_floors: &[
            (Engine::Postgres17, FloorMetric::ThroughputRatio(1.0)),
            (Engine::DuckDb, FloorMetric::ThroughputRatio(1.0)),
        ],
        run: stub_run,
    },
    BenchSpec {
        id: "range_scan",
        stage: Stage::V0_6,
        workload: Workload::RangeScan,
        competitor_floors: &[
            (Engine::Postgres17, FloorMetric::ThroughputRatio(1.0)),
            (Engine::DuckDb, FloorMetric::ThroughputRatio(0.5)),
        ],
        run: stub_run,
    },
    BenchSpec {
        id: "insert_throughput",
        stage: Stage::V0_6,
        workload: Workload::InsertThroughput,
        competitor_floors: &[(Engine::Postgres17, FloorMetric::ThroughputRatio(1.0))],
        run: stub_run,
    },
    BenchSpec {
        id: "hash_aggregate",
        stage: Stage::V0_6,
        workload: Workload::HashAggregate,
        competitor_floors: &[
            (Engine::Postgres17, FloorMetric::ThroughputRatio(1.0)),
            (Engine::DuckDb, FloorMetric::ThroughputRatio(0.5)),
        ],
        run: stub_run,
    },
    BenchSpec {
        id: "sort_large",
        stage: Stage::V0_6,
        workload: Workload::SortLarge,
        competitor_floors: &[(Engine::Postgres17, FloorMetric::ThroughputRatio(1.0))],
        run: stub_run,
    },
    BenchSpec {
        id: "tpch_q1",
        stage: Stage::V0_6,
        workload: Workload::TpchQ1,
        competitor_floors: &[
            (Engine::Postgres17, FloorMetric::ThroughputRatio(1.0)),
            (Engine::DuckDb, FloorMetric::ThroughputRatio(0.5)),
        ],
        run: stub_run,
    },
    // ------------------------------------------------------------------
    // v0.7 — vectorized-kernel benchmarks
    //
    // `select_sum_65k_i64`: `SELECT SUM(x) FROM t` over 65 536 i64 rows,
    //   hot cache. Competitor floor: UltraSQL (kernel) ≥ DuckDB.
    //
    // `select_avg_10m_i64`: `SELECT AVG(x) FROM t` over 10 000 000 i64.
    //   Competitor floor: UltraSQL (kernel) ≥ ClickHouse.
    // ------------------------------------------------------------------
    BenchSpec {
        id: "select_sum_65k_i64",
        stage: Stage::V0_7,
        workload: Workload::AnalyticAggregate,
        competitor_floors: &[
            (Engine::DuckDb, FloorMetric::ThroughputRatio(1.0)),
            (Engine::ClickHouse, FloorMetric::ThroughputRatio(1.0)),
            (Engine::Postgres17, FloorMetric::ThroughputRatio(1.0)),
            (Engine::Sqlite3, FloorMetric::ThroughputRatio(1.0)),
        ],
        run: stub_run,
    },
    BenchSpec {
        id: "select_avg_10m_i64",
        stage: Stage::V0_7,
        workload: Workload::AnalyticAggregate,
        competitor_floors: &[
            (Engine::ClickHouse, FloorMetric::ThroughputRatio(1.0)),
            (Engine::DuckDb, FloorMetric::ThroughputRatio(1.0)),
            (Engine::Postgres17, FloorMetric::ThroughputRatio(1.0)),
            (Engine::Sqlite3, FloorMetric::ThroughputRatio(1.0)),
        ],
        run: stub_run,
    },
    BenchSpec {
        id: "tpch_q22",
        stage: Stage::V0_7,
        workload: Workload::TpchQ22,
        competitor_floors: &[
            (Engine::Postgres17, FloorMetric::ThroughputRatio(1.0)),
            (Engine::DuckDb, FloorMetric::ThroughputRatio(0.5)),
        ],
        run: stub_run,
    },
    // ------------------------------------------------------------------
    // v0.8 — index + constraint benchmarks
    // ------------------------------------------------------------------
    BenchSpec {
        id: "btree_point_lookup",
        stage: Stage::V0_8,
        workload: Workload::PointLookup,
        competitor_floors: &[
            (Engine::Postgres17, FloorMetric::ThroughputRatio(1.0)),
            (Engine::DuckDb, FloorMetric::ThroughputRatio(1.0)),
        ],
        run: stub_run,
    },
    // ------------------------------------------------------------------
    // v0.9 — operations benchmarks
    // ------------------------------------------------------------------
    BenchSpec {
        id: "tpcb_32conn",
        stage: Stage::V0_9,
        workload: Workload::MixedOltp,
        competitor_floors: &[(Engine::Postgres17, FloorMetric::ThroughputRatio(1.0))],
        run: stub_run,
    },
    // ------------------------------------------------------------------
    // v1.0 — GA benchmarks
    // ------------------------------------------------------------------
    BenchSpec {
        id: "tpcc_5types",
        stage: Stage::V1_0,
        workload: Workload::MixedOltp,
        competitor_floors: &[(Engine::Postgres17, FloorMetric::ThroughputRatio(1.0))],
        run: stub_run,
    },
];

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_ids_are_unique() {
        let mut seen = std::collections::HashSet::new();
        for spec in REGISTRY {
            assert!(seen.insert(spec.id), "duplicate registry id: {}", spec.id);
        }
    }

    #[test]
    fn stage_round_trip_str() {
        let stages = [
            Stage::V0_2,
            Stage::V0_3,
            Stage::V0_4,
            Stage::V0_5,
            Stage::V0_6,
            Stage::V0_7,
            Stage::V0_8,
            Stage::V0_9,
            Stage::V1_0,
        ];
        for s in stages {
            let parsed = Stage::parse_str(s.as_str());
            assert_eq!(parsed, Some(s), "round-trip failed for {s:?}");
        }
    }

    #[test]
    fn stage_from_str_unknown_returns_none() {
        assert_eq!(Stage::parse_str("unknown"), None);
        assert_eq!(Stage::parse_str(""), None);
        assert_eq!(Stage::parse_str("v1_1"), None);
    }

    #[test]
    fn median_f64_odd() {
        assert!((median_f64(&[3.0, 1.0, 2.0]) - 2.0).abs() < 1e-9);
    }

    #[test]
    fn median_f64_even() {
        assert!((median_f64(&[1.0, 3.0, 2.0, 4.0]) - 2.5).abs() < 1e-9);
    }

    #[test]
    fn median_f64_empty() {
        assert!((median_f64(&[]) - 0.0).abs() < 1e-9);
    }

    #[test]
    fn p99_f64_basic() {
        let vals: Vec<f64> = (1..=100).map(f64::from).collect();
        // nearest-rank: ceil(0.99 * 100) = 99, index 98 => 99.0
        assert!((p99_f64(&vals) - 99.0).abs() < 1e-9);
    }

    #[test]
    fn bench_result_median_empty() {
        let r = BenchResult {
            throughput_per_sec: 0.0,
            p50_latency_us: 0.0,
            p99_latency_us: 0.0,
            samples: vec![],
        };
        assert!((r.median_us() - 0.0).abs() < 1e-9);
    }

    #[test]
    fn host_info_from_env_smoke() {
        // Just verify it doesn't panic without any env vars set.
        let info = HostInfo::from_env();
        assert!(!info.cpu.is_empty());
        assert!(!info.os.is_empty());
    }
}
