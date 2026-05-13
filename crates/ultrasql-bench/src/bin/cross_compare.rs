//! UltraSQL cross-engine comparison harness.
//!
//! Runs an individual workload against the existing kernels and emits
//! a single-line JSON record to stdout describing the median latency
//! and per-iteration distribution. The companion `benchmarks/run.sh`
//! invokes this binary once per workload row and parses the output.
//!
//! The supported workloads are:
//!
//! - `sum`      — `SUM(x)` over the whole column.
//! - `count`    — `COUNT(*)` over the whole column.
//! - `min`      — `MIN(x)`.
//! - `max`      — `MAX(x)`.
//! - `minmax`   — `MIN(x), MAX(x)` in a single pass (two folds).
//! - `avg`      — `AVG(x) = SUM(x) / COUNT(*)`.
//! - `filter`   — `SUM(x) FROM t WHERE y > 0`. Two columns.
//! - `range`    — `COUNT(*) FROM t WHERE x BETWEEN lo AND hi`.
//! - `point`    — point lookup against a freshly built B+ tree over
//!   `i64` keys. The build itself is excluded from the measurement —
//!   only the steady-state `lookup` is timed, mirroring how the SQL
//!   engines run a `SELECT x FROM t WHERE id = ?` against an indexed
//!   table after `CREATE INDEX`.
//!
//! Methodology
//! -----------
//!
//! - Same deterministic CSV input the SQL engines see (`--data` path).
//!   For two-column workloads the second column is provided via
//!   `--data2`.
//! - Iteration counts are controlled by the `--tier` flag:
//!   `low` (default) runs 1 warmup + 5 measured iterations at 100 000 rows;
//!   `ultra` runs 2 warmup + 8 measured iterations at 10 000 000 rows.
//!   Individual `--warmup` / `--iters` flags can further override the defaults.
//!   The medians, minimums, and individual measurements are written to stdout
//!   as one JSON object. Wall-clock is `std::time::Instant`, nanosecond
//!   resolution. Results are reported in microseconds.
//! - Each iteration consumes the entire dataset (the engines do too —
//!   there is no projection pushdown for these queries).
//! - For workloads that need a Bitmap (filter, range), the bitmap
//!   build is part of the timed region because the equivalent SQL
//!   engine query also re-evaluates the predicate per execution.

#![allow(clippy::print_stdout)]

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId, Xid};
use ultrasql_storage::btree::BTree;
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::page::Page;
use ultrasql_vec::column::NumericColumn;
use ultrasql_vec::{
    cmp_gt_i64, count_i64, max_i64, min_i64, range_mask_i64, sum_i64, sum_i64_with_mask,
};

/// Benchmark tier controlling dataset size and iteration counts.
///
/// `low` targets fast feedback (CI / development); `ultra` targets
/// publishable numbers from a large, cache-pressure-inducing dataset.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Default, ValueEnum)]
enum Tier {
    /// 100 000 rows, 5 measured iterations, 1 warmup.
    #[default]
    Low,
    /// 10 000 000 rows, 8 measured iterations, 2 warmup.
    Ultra,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, ValueEnum)]
enum Workload {
    Sum,
    Count,
    Min,
    Max,
    Minmax,
    Avg,
    Filter,
    Range,
    Point,
}

#[derive(Parser, Debug)]
#[command(
    name = "cross_compare",
    about = "UltraSQL kernel-level cross-engine comparison driver"
)]
struct Args {
    /// Workload to run.
    #[arg(long, value_enum)]
    workload: Workload,

    /// Benchmark tier: `low` (100k rows, 5 iters, 1 warmup) or
    /// `ultra` (10M rows, 8 iters, 2 warmup). Individual `--warmup`
    /// and `--iters` flags override the tier defaults when set
    /// explicitly.
    #[arg(long, value_enum, default_value_t = Tier::Low)]
    tier: Tier,

    /// Path to the single-column CSV (header `x`, then one i64 per row).
    #[arg(long)]
    data: PathBuf,

    /// Optional second CSV for two-column workloads (`filter`). Header
    /// `y`, then one i64 per row, same length as `--data`.
    #[arg(long)]
    data2: Option<PathBuf>,

    /// Lower bound for the `range` workload (inclusive).
    #[arg(long, default_value_t = 0)]
    range_lo: i64,

    /// Upper bound for the `range` workload (inclusive).
    #[arg(long, default_value_t = 1_000_000)]
    range_hi: i64,

    /// Number of warmup iterations. Overrides the tier default when
    /// set explicitly.
    #[arg(long)]
    warmup: Option<usize>,

    /// Number of measured iterations. Overrides the tier default when
    /// set explicitly.
    #[arg(long)]
    iters: Option<usize>,

    /// For the `point` workload, how many random lookups to perform
    /// per measured iteration. The reported microseconds-per-iteration
    /// is divided by this value to give ns-per-lookup.
    #[arg(long, default_value_t = 10_000)]
    point_batch: usize,

    /// For the `point` workload, number of keys to insert into the
    /// B-tree before measuring. Smaller than the row counts used by
    /// SQL engines because the in-process B+ tree pool cannot evict
    /// dirty pages and must fit the entire tree in RAM.
    #[arg(long)]
    point_n: Option<usize>,
}

impl Args {
    /// Effective warmup iteration count: explicit flag overrides tier default.
    fn effective_warmup(&self) -> usize {
        self.warmup.unwrap_or_else(|| match self.tier {
            Tier::Low => 1,
            Tier::Ultra => 2,
        })
    }

    /// Effective measured iteration count: explicit flag overrides tier default.
    fn effective_iters(&self) -> usize {
        self.iters.unwrap_or_else(|| match self.tier {
            Tier::Low => 5,
            Tier::Ultra => 8,
        })
    }

    /// Effective B-tree key count for the `point` workload.
    fn effective_point_n(&self) -> usize {
        self.point_n.unwrap_or_else(|| match self.tier {
            Tier::Low => 100_000,
            Tier::Ultra => 1_000_000,
        })
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let result = match args.workload {
        Workload::Sum => run_sum(&args)?,
        Workload::Count => run_count(&args)?,
        Workload::Min => run_min(&args)?,
        Workload::Max => run_max(&args)?,
        Workload::Minmax => run_minmax(&args)?,
        Workload::Avg => run_avg(&args)?,
        Workload::Filter => run_filter(&args)?,
        Workload::Range => run_range(&args)?,
        Workload::Point => run_point(&args)?,
    };
    println!("{result}");
    Ok(())
}

// --- common --------------------------------------------------------------

fn load_i64_csv(path: &PathBuf) -> Result<Vec<i64>> {
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let r = BufReader::new(f);
    let mut out = Vec::new();
    let mut header_skipped = false;
    for line in r.lines() {
        let line = line?;
        if !header_skipped {
            header_skipped = true;
            // First non-empty line is the header; skip if not numeric.
            if line.trim().parse::<i64>().is_err() {
                continue;
            }
        }
        if line.trim().is_empty() {
            continue;
        }
        out.push(
            line.trim()
                .parse::<i64>()
                .with_context(|| format!("parse i64 from line {:?} in {}", line, path.display()))?,
        );
    }
    Ok(out)
}

/// Run `iters` measured iterations of `body`, after `warmup` warmups,
/// and emit a JSON object describing the distribution. The closure
/// returns whatever the workload computed; we keep the last value so
/// the optimizer does not elide the call.
fn time_iters<F, T: std::fmt::Display>(
    workload: &str,
    n_rows: usize,
    warmup: usize,
    iters: usize,
    mut body: F,
    extra_fields: &[(&str, String)],
) -> String
where
    F: FnMut() -> T,
{
    let mut last = String::new();
    for _ in 0..warmup {
        let v = body();
        last = format!("{v}");
    }
    let mut us: Vec<f64> = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        let v = body();
        let dt = t0.elapsed();
        let ns = dt.as_nanos();
        // ns / 1000 with f64 precision; ns is u128.
        us.push((ns as f64) / 1000.0);
        last = format!("{v}");
    }
    emit_json(workload, n_rows, &us, &last, extra_fields)
}

fn median(xs: &[f64]) -> f64 {
    let mut s = xs.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = s.len();
    if n == 0 {
        return 0.0;
    }
    if n % 2 == 1 {
        s[n / 2]
    } else {
        f64::midpoint(s[n / 2 - 1], s[n / 2])
    }
}

fn min_f(xs: &[f64]) -> f64 {
    xs.iter().copied().fold(f64::INFINITY, f64::min)
}

fn emit_json(
    workload: &str,
    n_rows: usize,
    us: &[f64],
    answer: &str,
    extras: &[(&str, String)],
) -> String {
    let med = median(us);
    let mn = min_f(us);
    let iters_json: Vec<String> = us.iter().map(|x| format!("{x:.3}")).collect();
    let extras_json: Vec<String> = extras.iter().map(|(k, v)| format!("\"{k}\":{v}")).collect();
    let mut parts: Vec<String> = vec![
        format!("\"workload\":\"{workload}\""),
        format!("\"n_rows\":{n_rows}"),
        format!("\"samples\":{}", us.len()),
        format!("\"median_us\":{med:.3}"),
        format!("\"min_us\":{mn:.3}"),
        format!("\"iterations_us\":[{}]", iters_json.join(",")),
        format!("\"answer\":\"{answer}\""),
    ];
    parts.extend(extras_json);
    format!("{{{}}}", parts.join(","))
}

// --- workloads -----------------------------------------------------------

fn run_sum(args: &Args) -> Result<String> {
    let data = load_i64_csv(&args.data)?;
    let n = data.len();
    let col = NumericColumn::from_data(data);
    Ok(time_iters(
        "sum",
        n,
        args.effective_warmup(),
        args.effective_iters(),
        || sum_i64(&col),
        &[],
    ))
}

fn run_count(args: &Args) -> Result<String> {
    let data = load_i64_csv(&args.data)?;
    let n = data.len();
    let col = NumericColumn::from_data(data);
    Ok(time_iters(
        "count",
        n,
        args.effective_warmup(),
        args.effective_iters(),
        || count_i64(&col),
        &[],
    ))
}

fn run_min(args: &Args) -> Result<String> {
    let data = load_i64_csv(&args.data)?;
    let n = data.len();
    let col = NumericColumn::from_data(data);
    Ok(time_iters(
        "min",
        n,
        args.effective_warmup(),
        args.effective_iters(),
        || min_i64(&col).map_or(0_i64, |v| v),
        &[],
    ))
}

fn run_max(args: &Args) -> Result<String> {
    let data = load_i64_csv(&args.data)?;
    let n = data.len();
    let col = NumericColumn::from_data(data);
    Ok(time_iters(
        "max",
        n,
        args.effective_warmup(),
        args.effective_iters(),
        || max_i64(&col).map_or(0_i64, |v| v),
        &[],
    ))
}

fn run_minmax(args: &Args) -> Result<String> {
    let data = load_i64_csv(&args.data)?;
    let n = data.len();
    let col = NumericColumn::from_data(data);
    Ok(time_iters(
        "minmax",
        n,
        args.effective_warmup(),
        args.effective_iters(),
        || {
            let lo = min_i64(&col).unwrap_or(0);
            let hi = max_i64(&col).unwrap_or(0);
            // Use both so the optimizer doesn't drop one.
            lo.wrapping_add(hi)
        },
        &[],
    ))
}

fn run_avg(args: &Args) -> Result<String> {
    let data = load_i64_csv(&args.data)?;
    let n = data.len();
    let col = NumericColumn::from_data(data);
    Ok(time_iters(
        "avg",
        n,
        args.effective_warmup(),
        args.effective_iters(),
        || {
            let s = sum_i64(&col);
            let c = count_i64(&col);
            // Emit as integer dividend; SQL AVG returns NUMERIC in
            // most engines but the cost shape is sum + count + divide.
            i64::try_from(c).map_or(0_i64, |cc| if cc == 0 { 0 } else { s / cc })
        },
        &[],
    ))
}

fn run_filter(args: &Args) -> Result<String> {
    let data2_path = args
        .data2
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("--data2 required for filter workload"))?;
    let data_x = load_i64_csv(&args.data)?;
    let data_y = load_i64_csv(data2_path)?;
    if data_x.len() != data_y.len() {
        bail!(
            "filter: --data has {} rows, --data2 has {}",
            data_x.len(),
            data_y.len()
        );
    }
    let n = data_x.len();
    let col_x = NumericColumn::from_data(data_x);
    let col_y = NumericColumn::from_data(data_y);
    Ok(time_iters(
        "filter",
        n,
        args.effective_warmup(),
        args.effective_iters(),
        || {
            let mask = cmp_gt_i64(&col_y, 0);
            sum_i64_with_mask(&col_x, &mask)
        },
        &[],
    ))
}

fn run_range(args: &Args) -> Result<String> {
    let data = load_i64_csv(&args.data)?;
    let n = data.len();
    let col = NumericColumn::from_data(data);
    let lo = args.range_lo;
    let hi = args.range_hi;
    Ok(time_iters(
        "range",
        n,
        args.effective_warmup(),
        args.effective_iters(),
        || {
            let m = range_mask_i64(&col, lo, hi);
            m.count_ones()
        },
        &[("range_lo", lo.to_string()), ("range_hi", hi.to_string())],
    ))
}

// --- point lookup --------------------------------------------------------

/// In-memory B-tree loader that returns blank heap pages. The tree
/// initialises every allocated block immediately after a cache miss,
/// so the loader's contents do not matter — it only needs to never
/// return an error.
#[derive(Debug, Default)]
struct BlankLoader;

impl PageLoader for BlankLoader {
    fn load(&self, _page_id: PageId) -> ultrasql_core::Result<Page> {
        Ok(Page::new_heap())
    }
}

const fn tid(block: u32, slot: u16) -> TupleId {
    TupleId::new(
        PageId::new(RelationId::new(99), BlockNumber::new(block)),
        slot,
    )
}

fn run_point(args: &Args) -> Result<String> {
    // 1) Build a B-tree of `point_n` i64 keys. The buffer pool must
    //    be large enough to hold every dirty page because the v0.5
    //    pool does not yet evict dirty pages. We estimate ~256
    //    entries per leaf for 8-byte keys + 8-byte values + entry
    //    framing, and reserve generous headroom.
    let n = args.effective_point_n();
    // The v0.5 buffer pool refuses to evict dirty pages; we must size
    // it to hold every leaf + internal page the workload dirties. The
    // B-tree caps a leaf at 32 entries (see btree.rs::MAX_LEAF_ENTRIES),
    // so we need at least n/32 leaves + a smaller internal layer. We
    // budget n/24 frames (≈1.3× the steady-state minimum to absorb
    // mid-insert splits) plus headroom for the descent path.
    let frames = (n / 24 + 8_192).max(32_768);
    let pool = Arc::new(BufferPool::new(frames, BlankLoader));
    let mut tree = BTree::create(Arc::clone(&pool), RelationId::new(99)).context("create btree")?;

    // 2) Insert keys 0..n in xorshifted order so the tree is not
    //    sequential (sequential inserts produce a degenerate
    //    rightmost-only insert path).
    let mut s: u64 = 0xDEAD_BEEF_CAFE_F00D;
    // i64::try_from is infallible for n <= i64::MAX as usize; on 64-bit
    // targets the practical n stays well under that.
    let n_i64 = i64::try_from(n).context("point_n exceeds i64::MAX")?;
    let mut perm: Vec<i64> = (0..n_i64).collect();
    for i in (1..perm.len()).rev() {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        let j = (s as usize) % (i + 1);
        perm.swap(i, j);
    }
    for &k in &perm {
        // Encode tid deterministically.
        let block = u32::try_from(k & 0xFFFF_FFFF).unwrap_or(0);
        let slot = u16::try_from((k.wrapping_mul(31)) & 0xFFFF).unwrap_or(0);
        tree.insert::<i64>(k, tid(block, slot), Xid::FIRST_USER, None)
            .context("btree insert")?;
    }

    // 3) Pre-generate the lookup probe keys (xorshift over [0, n))
    //    so probe generation is not in the timed region.
    let probes: usize = args.point_batch;
    let mut s2: u64 = 0x1234_5678_9ABC_DEF0;
    let mut keys: Vec<i64> = Vec::with_capacity(probes);
    for _ in 0..probes {
        s2 ^= s2 << 13;
        s2 ^= s2 >> 7;
        s2 ^= s2 << 17;
        // Cast u64 -> i64 by reinterpretation; the value is then mapped
        // into [0, n) via Euclidean remainder, which is well-defined
        // for any i64 input.
        let raw = i64::from_ne_bytes(s2.to_ne_bytes());
        keys.push(raw.rem_euclid(n_i64));
    }

    // 4) Warm up.
    for _ in 0..args.effective_warmup() {
        let mut hits: i64 = 0;
        for &k in &keys {
            if let Some(t) = tree.lookup::<i64>(k).unwrap_or(None) {
                hits = hits.wrapping_add(i64::from(t.page.block.raw()));
            }
        }
        std::hint::black_box(hits);
    }

    // 5) Time `iters` runs of `probes` lookups each.
    let mut us: Vec<f64> = Vec::with_capacity(args.effective_iters());
    let mut last_hits: i64 = 0;
    for _ in 0..args.effective_iters() {
        let t0 = Instant::now();
        let mut hits: i64 = 0;
        for &k in &keys {
            if let Some(t) = tree.lookup::<i64>(k).unwrap_or(None) {
                hits = hits.wrapping_add(i64::from(t.page.block.raw()));
            }
        }
        let dt = t0.elapsed();
        std::hint::black_box(hits);
        last_hits = hits;
        us.push(dt.as_nanos() as f64 / 1000.0);
    }

    let med_us = median(&us);
    let ns_per_op = (med_us * 1000.0) / (probes as f64);
    let answer = format!("hits={last_hits}");
    Ok(emit_json(
        "point",
        n,
        &us,
        &answer,
        &[
            ("point_batch", probes.to_string()),
            ("ns_per_lookup", format!("{ns_per_op:.2}")),
        ],
    ))
}
