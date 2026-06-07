//! UltraSQL point-lookup harness for the cross-engine comparison.
//!
//! Builds a `BTree<i64>` over a deterministic set of `(id, x)` pairs in
//! the in-memory buffer pool, primes a hot session, and times **only the
//! probes**. The same probe set is shared across every engine in the
//! comparison so each engine answers the same questions in the same
//! order. The `--tier` flag controls tree size:
//! - `low` (default): 100 000 keys.
//! - `ultra`: 10 000 000 keys.
//!
//! The fairness contract is: every engine pays for its session setup
//! once (CSV load, prepared statement, warmup), then we measure only
//! the 100 000-probe loop. This binary therefore reports two numbers:
//!
//! - `median_ns_per_probe`  — median across N measured runs of
//!   `total_wall_ns / probes`.
//! - `total_wall_ns`        — total wall time of the median run.
//!
//! Run shape
//! ---------
//!
//! 1. Build a `BTree<i64>` of `point_n` keys, inserting in a shuffled
//!    order (`0xDEAD_BEEF_CAFE_F00D` xorshift seed). Each value is a
//!    deterministic [`TupleId`].
//! 2. Pre-generate `probes` random probe keys (`0xCAFE_BABE_F00D_1234`
//!    xorshift seed, reduced into `[0, point_n)`).
//! 3. Run `warmup_probes` warmup probes (throwaway).
//! 4. Run `runs` measurement runs, each of `probes` probes. Record total
//!    wall time per run. Median across runs is the headline number.
//!
//! The same dataset and probe sequence is consumed by the other engines
//! through their respective drivers in
//! `benchmarks/results/comparison-2026-05-12-m4-pointlookup/run.sh`.
//!
//! Methodology vs the prior `comparison-2026-05-12-m4-extended` row
//! ----------------------------------------------------------------
//!
//! The prior comparison's `point-10m` row reported the **per-iteration**
//! latency of a 10 000-probe batch (~6.78 ms) and called it "ms per
//! probe" in some tables — that was off by a factor of 10 000 because
//! the batch cost was not divided by the batch size. This harness fixes
//! that by reporting only `ns_per_probe` and `total_wall_ns`, with the
//! probe count explicit in the JSON output.

#![allow(
    clippy::print_stdout,
    clippy::unnecessary_lazy_evaluations,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_lossless
)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId, Xid};

/// Benchmark tier controlling B-tree size.
///
/// `low` targets fast feedback; `ultra` targets publishable numbers.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Default, ValueEnum)]
enum Tier {
    /// 100 000-key B-tree.
    #[default]
    Low,
    /// 10 000 000-key B-tree.
    Ultra,
}
use ultrasql_storage::btree::BTree;
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::page::Page;

#[derive(Parser, Debug)]
#[command(
    name = "point_lookup",
    about = "UltraSQL point-lookup driver: build BTree<i64>, time hot probes"
)]
struct Args {
    /// Benchmark tier: `low` (100k keys) or `ultra` (10M keys).
    /// The `--point-n` flag overrides the tier default when set explicitly.
    #[arg(long, value_enum, default_value_t = Tier::Low)]
    tier: Tier,

    /// Number of keys to insert into the `BTree`. Overrides the tier
    /// default when set explicitly.
    #[arg(long)]
    point_n: Option<usize>,

    /// Number of measured probes per run.
    #[arg(long, default_value_t = 100_000)]
    probes: usize,

    /// Number of warmup probes before the first measured run.
    #[arg(long, default_value_t = 10_000)]
    warmup_probes: usize,

    /// Number of measured runs (each = `probes` lookups). The reported
    /// median is taken across runs.
    #[arg(long, default_value_t = 3)]
    runs: usize,

    /// Permutation seed for shuffling the inserted key order.
    #[arg(long, default_value_t = 0xDEAD_BEEF_CAFE_F00D_u64)]
    insert_seed: u64,

    /// Probe-key generation seed.
    #[arg(long, default_value_t = 0xCAFE_BABE_F00D_1234_u64)]
    probe_seed: u64,

    /// Optional output path. If set, the JSON record is appended to the
    /// file in addition to being printed to stdout.
    #[arg(long)]
    out: Option<PathBuf>,
}

impl Args {
    /// Effective key count: explicit `--point-n` overrides tier default.
    fn effective_point_n(&self) -> usize {
        self.point_n.unwrap_or_else(|| match self.tier {
            Tier::Low => 100_000,
            Tier::Ultra => 10_000_000,
        })
    }
}

/// In-memory buffer-pool loader that returns blank heap pages.
///
/// The B-tree initialises each freshly-allocated block immediately
/// after a cache miss, so the loader's contents are not read; it just
/// needs to never fail.
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

fn median_f64(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    let mut s = xs.to_vec();
    ultrasql_bench::sort_f64_nan_last(&mut s);
    let n = s.len();
    if n % 2 == 1 {
        s[n / 2]
    } else {
        f64::midpoint(s[n / 2 - 1], s[n / 2])
    }
}

fn min_f64(xs: &[f64]) -> f64 {
    xs.iter().copied().fold(f64::INFINITY, f64::min)
}

fn max_f64(xs: &[f64]) -> f64 {
    xs.iter().copied().fold(f64::NEG_INFINITY, f64::max)
}

/// Build a fresh `BTree<i64>` with `n` keys inserted in a shuffled
/// order (seed `insert_seed`). Returns the populated tree and the wall
/// time spent on the inserts.
fn build_tree(n: usize, insert_seed: u64) -> Result<(BTree<BlankLoader>, u128)> {
    // The v0.5 buffer pool does not yet evict dirty pages, so we size
    // it to hold every leaf + internal page the workload dirties. With
    // MAX_LEAF_ENTRIES = 32 and random-key insertion, every split
    // leaves both halves at ~16 entries; the steady-state leaf count
    // is ≈ n/24. We budget ceil(n / 12) frames plus a generous fixed-
    // headroom term so repeated splits and momentarily-pinned root-to-
    // leaf descent paths never collide with the dirty-page count.
    let frames = n / 12 + 32_768;
    eprintln!(
        "point_lookup: building BTree<i64> with {n} keys, pool capacity {frames} frames \
         ({} MiB)",
        (frames * 8) / 1024
    );

    let pool = Arc::new(BufferPool::new(frames, BlankLoader));
    let mut tree = BTree::create(Arc::clone(&pool), RelationId::new(99)).context("create btree")?;

    let n_i64 = i64::try_from(n).context("point_n exceeds i64::MAX")?;
    let mut perm: Vec<i64> = (0..n_i64).collect();
    let mut s = insert_seed;
    for i in (1..perm.len()).rev() {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        let j = (s as usize) % (i + 1);
        perm.swap(i, j);
    }

    let build_t0 = Instant::now();
    for &k in &perm {
        let block = u32::try_from(k & 0xFFFF_FFFF).unwrap_or(0);
        let slot = u16::try_from((k.wrapping_mul(31)) & 0xFFFF).unwrap_or(0);
        tree.insert::<i64>(k, tid(block, slot), Xid::FIRST_USER, None)
            .context("btree insert")?;
    }
    let build_ns = build_t0.elapsed().as_nanos();
    eprintln!(
        "point_lookup: build complete in {:.2} s ({:.0} ns/insert)",
        (build_ns as f64) / 1e9,
        (build_ns as f64) / (n as f64)
    );
    Ok((tree, build_ns))
}

/// Generate `probes` deterministic probe keys via xorshift seeded by
/// `probe_seed`, each reduced into `[0, n)` via `rem_euclid`.
fn gen_probe_keys(probes: usize, n_i64: i64, probe_seed: u64) -> Vec<i64> {
    let mut keys: Vec<i64> = Vec::with_capacity(probes);
    let mut s2 = probe_seed;
    for _ in 0..probes {
        s2 ^= s2 << 13;
        s2 ^= s2 >> 7;
        s2 ^= s2 << 17;
        let raw = i64::from_ne_bytes(s2.to_ne_bytes());
        keys.push(raw.rem_euclid(n_i64));
    }
    keys
}

/// Warmup loop. `warmup_probes` cycling probes prime the prefetch /
/// branch predictor; the second pass walks every probe key so every
/// B-tree page the measurement run hits is already resident in the
/// buffer pool, eliminating cold-cache outliers in run 1.
fn warmup(tree: &BTree<BlankLoader>, keys: &[i64], warmup_probes: usize) {
    if keys.is_empty() {
        return;
    }
    let mut hits: i64 = 0;
    for probe in 0..warmup_probes {
        let k = keys[probe % keys.len()];
        if let Some(t) = tree.lookup::<i64>(k).unwrap_or(None) {
            hits = hits.wrapping_add(i64::from(t.page.block.raw()));
        }
    }
    for &k in keys {
        if let Some(t) = tree.lookup::<i64>(k).unwrap_or(None) {
            hits = hits.wrapping_add(i64::from(t.page.block.raw()));
        }
    }
    std::hint::black_box(hits);
}

/// Run `runs` measurement iterations, each timing `keys.len()`
/// lookups. Returns the per-run wall-time vector (in nanoseconds) and
/// the final `hits` accumulator (so the optimizer cannot elide the
/// loop).
fn measure(tree: &BTree<BlankLoader>, keys: &[i64], runs: usize) -> (Vec<u128>, i64) {
    let mut run_ns: Vec<u128> = Vec::with_capacity(runs);
    let mut last_hits: i64 = 0;
    for _ in 0..runs {
        let t0 = Instant::now();
        let mut hits: i64 = 0;
        for &k in keys {
            if let Some(t) = tree.lookup::<i64>(k).unwrap_or(None) {
                hits = hits.wrapping_add(i64::from(t.page.block.raw()));
            }
        }
        let dt = t0.elapsed();
        std::hint::black_box(hits);
        last_hits = hits;
        run_ns.push(dt.as_nanos());
    }
    (run_ns, last_hits)
}

/// Format the per-engine JSON result record. Mirrors the per-engine
/// shape of `comparison-2026-05-12-m4-extended/results.json` so the
/// downstream parser does not need a special case for this driver.
struct ResultJsonInput<'a> {
    n: usize,
    probes: usize,
    runs: usize,
    warmup_probes: usize,
    run_ns: &'a [u128],
    build_ns: u128,
    last_hits: i64,
}

fn format_result_json(input: ResultJsonInput<'_>) -> String {
    let ResultJsonInput {
        n,
        probes,
        runs,
        warmup_probes,
        run_ns,
        build_ns,
        last_hits,
    } = input;
    let probes_f = probes as f64;
    let per_probe_ns: Vec<f64> = run_ns.iter().map(|&t| (t as f64) / probes_f).collect();
    let med_ns = median_f64(&per_probe_ns);
    let min_ns = min_f64(&per_probe_ns);
    let max_ns = max_f64(&per_probe_ns);

    let run_ns_strs: Vec<String> = run_ns
        .iter()
        .map(std::string::ToString::to_string)
        .collect();
    let per_probe_strs: Vec<String> = per_probe_ns.iter().map(|x| format!("{x:.3}")).collect();

    let total_wall_ns_median = run_ns
        .iter()
        .copied()
        .min_by(|a, b| {
            let pa = (*a as f64) / probes_f;
            let pb = (*b as f64) / probes_f;
            (pa - med_ns)
                .abs()
                .partial_cmp(&(pb - med_ns).abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .unwrap_or(0);

    format!(
        "{{\
\"workload\":\"point-10m-probes\",\
\"engine\":\"UltraSQL (kernel, BTree<i64>)\",\
\"n_rows\":{n},\
\"probes\":{probes},\
\"runs\":{runs},\
\"warmup_probes\":{warmup_probes},\
\"median_ns_per_probe\":{med_ns:.3},\
\"min_ns_per_probe\":{min_ns:.3},\
\"max_ns_per_probe\":{max_ns:.3},\
\"total_wall_ns_median_run\":{total_wall_ns_median},\
\"run_ns\":[{run_list}],\
\"per_probe_ns\":[{per_probe_list}],\
\"build_ns\":{build_ns},\
\"answer\":\"hits={last_hits}\"\
}}",
        run_list = run_ns_strs.join(","),
        per_probe_list = per_probe_strs.join(","),
    )
}

fn main() -> Result<()> {
    let args = Args::parse();
    let point_n = args.effective_point_n();

    let (tree, build_ns) = build_tree(point_n, args.insert_seed)?;
    let n_i64 = i64::try_from(point_n).context("point_n exceeds i64::MAX")?;
    let keys = gen_probe_keys(args.probes, n_i64, args.probe_seed);
    warmup(&tree, &keys, args.warmup_probes);
    let (run_ns, last_hits) = measure(&tree, &keys, args.runs);

    let json = format_result_json(ResultJsonInput {
        n: point_n,
        probes: args.probes,
        runs: args.runs,
        warmup_probes: args.warmup_probes,
        run_ns: &run_ns,
        build_ns,
        last_hits,
    });
    println!("{json}");

    if let Some(path) = &args.out {
        use std::fs::OpenOptions;
        use std::io::Write;
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("open {}", path.display()))?;
        writeln!(f, "{json}").context("write json line")?;
    }

    Ok(())
}
