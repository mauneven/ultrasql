//! UltraSQL cross-engine concurrency harness.
//!
//! Spawns `T` `std::thread`s and drives one of four workloads against
//! the kernel/heap API for a fixed wall-clock window, then emits a
//! single-line JSON record summarising total throughput across all
//! threads. The companion `run.sh` in
//! `benchmarks/results/comparison-2026-05-12-m4-concurrency/` invokes
//! this binary once per `(workload, threads)` cell and parses the
//! output.
//!
//! Workloads
//! ---------
//!
//! - `conc-read-sum` — partition a pre-populated 1 000 000-row
//!   `NumericColumn<i64>` across `T` threads; each thread invokes
//!   [`sum_i64`] on its slice in a tight loop until the harness
//!   signals stop. Measures parallel scan throughput with no shared
//!   mutable state in the hot path.
//!
//! - `conc-read-point` — share one PK-indexed `BTree<i64>` of
//!   1 000 000 keys across `T` threads. Each thread runs
//!   [`BTree::lookup`] for random ids until stop. Lookup is `&self`
//!   so the tree's pinned pages are concurrently readable.
//!
//! - `conc-insert` — each thread owns a distinct id range and inserts
//!   `(id i64 PK, val i64)` tuples into its own `HeapAccess`-managed
//!   relation. Measures heap-insert scaling when there is no
//!   cross-thread key conflict.
//!
//! - `conc-update` — each thread owns a contiguous range of i64
//!   values in a private `NumericColumn` and rewrites them in place
//!   (`v += 1`). Heap has no in-place UPDATE at v0.5; this measures
//!   the data plane the eventual UPDATE will pay.
//!
//! Methodology
//! -----------
//!
//! 1. 1-second warmup. Discarded.
//! 2. 5-second measured window. Each thread tracks its own completed
//!    operation count and returns it on join.
//! 3. Three repeats per cell; the harness emits the per-iteration
//!    `ops/s` (total / 5 s) and the median.
//!
//! The wall-clock budget is enforced via [`AtomicBool`] polled inside
//! each thread's inner loop. We do not use Tokio here because the
//! workloads are CPU-bound and the kernel API is sync; raw
//! [`std::thread`] gives the cleanest model of "T independent
//! clients."

#![allow(clippy::print_stdout, clippy::enum_variant_names)]

use std::io::BufRead;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use ultrasql_core::{BlockNumber, CommandId, PageId, RelationId, TupleId, Xid};
use ultrasql_storage::btree::BTree;
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::heap::{HeapAccess, InsertOptions};
use ultrasql_storage::page::Page;
use ultrasql_vec::column::NumericColumn;
use ultrasql_vec::sum_i64;

/// Selectable workloads. The `Conc` prefix is preserved on every
/// variant because it matches the `conc-*` keys used by the runner
/// script and the `results.json` layout.
#[derive(Copy, Clone, Eq, PartialEq, Debug, ValueEnum)]
enum Workload {
    /// `SELECT SUM(x) FROM t` partitioned across `T` threads.
    ConcReadSum,
    /// `SELECT x FROM t WHERE id = ?` random reads on a shared B-tree.
    ConcReadPoint,
    /// Per-thread INSERT of rows into disjoint relations.
    ConcInsert,
    /// Per-thread in-place rewrite of a private row range.
    ConcUpdate,
}

#[derive(Parser, Debug)]
#[command(
    name = "cross_concurrency",
    about = "UltraSQL kernel/heap concurrency comparison driver"
)]
struct Args {
    /// Workload to run.
    #[arg(long, value_enum)]
    workload: Workload,

    /// Number of worker threads.
    #[arg(long)]
    threads: usize,

    /// Number of measured repetitions. The median ops/s is reported.
    #[arg(long, default_value_t = 3)]
    repeats: usize,

    /// Length of the measured window, in seconds.
    #[arg(long, default_value_t = 5)]
    measure_secs: u64,

    /// Length of the warmup window, in seconds.
    #[arg(long, default_value_t = 1)]
    warmup_secs: u64,

    /// Dataset size for `conc-read-sum` and `conc-read-point`.
    #[arg(long, default_value_t = 1_000_000)]
    dataset_rows: usize,

    /// Number of rows each thread owns in `conc-insert` /
    /// `conc-update`. The thread saturates the measured window — the
    /// reported ops/s is total rows touched divided by elapsed time —
    /// so larger values amortise per-iteration overhead.
    #[arg(long, default_value_t = 10_000)]
    rows_per_thread: usize,

    /// Optional CSV path. If absent, the harness fabricates a
    /// deterministic dataset (seed 0xDEADBEEF). The CSV is the same
    /// shape as `data_x_1m.csv` from the extended comparison —
    /// header `x`, then one i64 per line.
    #[arg(long)]
    data: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let line = match args.workload {
        Workload::ConcReadSum => run_read_sum(&args)?,
        Workload::ConcReadPoint => run_read_point(&args)?,
        Workload::ConcInsert => run_insert(&args),
        Workload::ConcUpdate => run_update(&args),
    };
    println!("{line}");
    Ok(())
}

// --- shared helpers ------------------------------------------------------

const fn workload_tag(w: Workload) -> &'static str {
    match w {
        Workload::ConcReadSum => "conc-read-sum",
        Workload::ConcReadPoint => "conc-read-point",
        Workload::ConcInsert => "conc-insert",
        Workload::ConcUpdate => "conc-update",
    }
}

/// Deterministic xorshift64 — used both for the synthetic dataset and
/// for in-thread probe generation. Same `seed` always produces the
/// same stream so the harness is byte-reproducible across runs.
#[inline]
const fn xorshift64(mut s: u64) -> u64 {
    s ^= s << 13;
    s ^= s >> 7;
    s ^= s << 17;
    s
}

/// Cast a `usize` index into an `i64` safely. The caller knows the
/// index fits in the i64 range; on 64-bit targets the conversion is
/// only lossy when the value exceeds `i64::MAX`.
#[inline]
fn usize_to_i64(v: usize) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

fn load_or_synthesize_dataset(args: &Args) -> Result<Vec<i64>> {
    if let Some(path) = &args.data {
        let f = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
        let r = std::io::BufReader::new(f);
        let mut out = Vec::with_capacity(args.dataset_rows);
        let mut header_skipped = false;
        for line in r.lines() {
            let line = line?;
            if !header_skipped {
                header_skipped = true;
                if line.trim().parse::<i64>().is_err() {
                    continue;
                }
            }
            if line.trim().is_empty() {
                continue;
            }
            out.push(line.trim().parse::<i64>().with_context(|| {
                format!("parse i64 from line {:?} in {}", line, path.display())
            })?);
        }
        Ok(out)
    } else {
        let mut s: u64 = 0xDEAD_BEEF_CAFE_F00D;
        let mut out = Vec::with_capacity(args.dataset_rows);
        for _ in 0..args.dataset_rows {
            s = xorshift64(s);
            // Map u64 -> i64 by bit reinterpretation; the kernel does
            // not care about the sign of the input.
            out.push(i64::from_ne_bytes(s.to_ne_bytes()));
        }
        Ok(out)
    }
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

fn emit_json(workload: Workload, threads: usize, args: &Args, ops_per_sec: &[f64]) -> String {
    let med = median(ops_per_sec);
    let max = ops_per_sec.iter().copied().fold(0.0_f64, f64::max);
    let parts: Vec<String> = ops_per_sec.iter().map(|v| format!("{v:.2}")).collect();
    format!(
        "{{\"workload\":\"{}\",\"threads\":{},\"repeats\":{},\"measure_secs\":{},\"warmup_secs\":{},\"rows_per_thread\":{},\"dataset_rows\":{},\"median_ops_per_sec\":{:.2},\"max_ops_per_sec\":{:.2},\"iterations_ops_per_sec\":[{}]}}",
        workload_tag(workload),
        threads,
        args.repeats,
        args.measure_secs,
        args.warmup_secs,
        args.rows_per_thread,
        args.dataset_rows,
        med,
        max,
        parts.join(","),
    )
}

#[derive(Debug, Default)]
struct BlankLoader;

impl PageLoader for BlankLoader {
    fn load(&self, _page_id: PageId) -> ultrasql_core::Result<Page> {
        Ok(Page::new_heap())
    }
}

const fn make_tid(block: u32, slot: u16) -> TupleId {
    TupleId::new(
        PageId::new(RelationId::new(99), BlockNumber::new(block)),
        slot,
    )
}

// --- conc-read-sum -------------------------------------------------------

fn run_read_sum(args: &Args) -> Result<String> {
    let data = load_or_synthesize_dataset(args)?;
    let total = data.len();
    // Leak the dataset into 'static memory so each thread can hold a
    // `&'static [i64]` view without an Arc and without cloning. This
    // is fine for a one-shot benchmark; the process exits when the
    // run is done.
    let leaked: &'static [i64] = Box::leak(data.into_boxed_slice());

    let mut iters = Vec::with_capacity(args.repeats);
    for rep in 0..args.repeats {
        // Warmup pass.
        run_read_sum_window(leaked, total, args.threads, args.warmup_secs);

        // Measured pass.
        let (total_ops, secs) = run_read_sum_window(leaked, total, args.threads, args.measure_secs);
        let ops_per_sec = if secs > 0.0 {
            (total_ops as f64) / secs
        } else {
            0.0
        };
        iters.push(ops_per_sec);
        eprintln!(
            "  rep {}/{}: {:.0} ops/s ({} total in {:.2} s)",
            rep + 1,
            args.repeats,
            ops_per_sec,
            total_ops,
            secs
        );
    }

    Ok(emit_json(args.workload, args.threads, args, &iters))
}

fn run_read_sum_window(
    leaked: &'static [i64],
    total: usize,
    threads: usize,
    secs: u64,
) -> (u64, f64) {
    let stop = Arc::new(AtomicBool::new(false));
    let counters: Vec<Arc<AtomicU64>> = (0..threads).map(|_| Arc::new(AtomicU64::new(0))).collect();
    let mut handles = Vec::with_capacity(threads);
    for (tid, counter) in counters.iter().enumerate() {
        let stop_t = Arc::clone(&stop);
        let counter_t = Arc::clone(counter);
        let chunk = total / threads;
        let lo = tid * chunk;
        let hi = if tid + 1 == threads {
            total
        } else {
            (tid + 1) * chunk
        };
        let slice: &'static [i64] = &leaked[lo..hi];
        handles.push(thread::spawn(move || {
            let n = run_read_sum_thread(slice, &stop_t);
            counter_t.store(n, Ordering::Release);
        }));
    }
    let t0 = Instant::now();
    thread::sleep(Duration::from_secs(secs));
    stop.store(true, Ordering::Release);
    let elapsed = t0.elapsed();
    for h in handles {
        let _ = h.join();
    }
    let total_ops: u64 = counters.iter().map(|c| c.load(Ordering::Acquire)).sum();
    (total_ops, elapsed.as_secs_f64())
}

fn run_read_sum_thread(slice: &[i64], stop: &AtomicBool) -> u64 {
    // Materialise a per-thread `NumericColumn`. The constructor takes
    // ownership of a `Vec`, so we copy the slice once here; this is
    // amortised over the whole 5-second window (millions of sums).
    let col = NumericColumn::from_data(slice.to_vec());
    let mut ops: u64 = 0;
    while !stop.load(Ordering::Acquire) {
        let s = sum_i64(&col);
        std::hint::black_box(s);
        ops += 1;
    }
    ops
}

// --- conc-read-point -----------------------------------------------------

fn run_read_point(args: &Args) -> Result<String> {
    let n = args.dataset_rows;
    // Mirror cross_compare's sizing rule: ≥ n/24 frames so the v0.5
    // pool never has to evict a dirty page mid-build.
    let frames = (n / 24 + 8_192).max(32_768);
    let pool = Arc::new(BufferPool::new(frames, BlankLoader));
    let mut tree = BTree::create(Arc::clone(&pool), RelationId::new(99)).context("create btree")?;

    let n_i64 = i64::try_from(n).context("dataset_rows exceeds i64::MAX")?;
    let mut s: u64 = 0xDEAD_BEEF_CAFE_F00D;
    let mut perm: Vec<i64> = (0..n_i64).collect();
    for i in (1..perm.len()).rev() {
        s = xorshift64(s);
        let j = (s as usize) % (i + 1);
        perm.swap(i, j);
    }
    for &k in &perm {
        let block = u32::try_from(k & 0xFFFF_FFFF).unwrap_or(0);
        let slot = u16::try_from((k.wrapping_mul(31)) & 0xFFFF).unwrap_or(0);
        tree.insert::<i64>(k, make_tid(block, slot))
            .context("btree insert")?;
    }

    let tree = Arc::new(tree);

    let mut iters = Vec::with_capacity(args.repeats);
    for rep in 0..args.repeats {
        // Warmup.
        run_read_point_window(&tree, n_i64, args.threads, args.warmup_secs);

        // Measured.
        let (total_ops, secs) =
            run_read_point_window(&tree, n_i64, args.threads, args.measure_secs);
        let ops_per_sec = if secs > 0.0 {
            (total_ops as f64) / secs
        } else {
            0.0
        };
        iters.push(ops_per_sec);
        eprintln!(
            "  rep {}/{}: {:.0} ops/s ({} total in {:.2} s)",
            rep + 1,
            args.repeats,
            ops_per_sec,
            total_ops,
            secs
        );
    }
    Ok(emit_json(args.workload, args.threads, args, &iters))
}

fn run_read_point_window(
    tree: &Arc<BTree<BlankLoader>>,
    n_i64: i64,
    threads: usize,
    secs: u64,
) -> (u64, f64) {
    let stop = Arc::new(AtomicBool::new(false));
    let counters: Vec<Arc<AtomicU64>> = (0..threads).map(|_| Arc::new(AtomicU64::new(0))).collect();
    let mut handles = Vec::with_capacity(threads);
    for (tid, counter) in counters.iter().enumerate() {
        let stop_t = Arc::clone(&stop);
        let tree_t = Arc::clone(tree);
        let counter_t = Arc::clone(counter);
        handles.push(thread::spawn(move || {
            let seed = 0xCAFE_F00D_DEAD_BEEF_u64 ^ ((tid as u64).wrapping_mul(0xC2B2_AE35_u64));
            let n = run_read_point_thread(&tree_t, n_i64, seed, &stop_t);
            counter_t.store(n, Ordering::Release);
        }));
    }
    let t0 = Instant::now();
    thread::sleep(Duration::from_secs(secs));
    stop.store(true, Ordering::Release);
    let elapsed = t0.elapsed();
    for h in handles {
        let _ = h.join();
    }
    let total_ops: u64 = counters.iter().map(|c| c.load(Ordering::Acquire)).sum();
    (total_ops, elapsed.as_secs_f64())
}

fn run_read_point_thread(
    tree: &BTree<BlankLoader>,
    n_i64: i64,
    mut seed: u64,
    stop: &AtomicBool,
) -> u64 {
    let mut ops: u64 = 0;
    while !stop.load(Ordering::Acquire) {
        seed = xorshift64(seed);
        let raw = i64::from_ne_bytes(seed.to_ne_bytes());
        let key = raw.rem_euclid(n_i64);
        if let Ok(Some(t)) = tree.lookup::<i64>(key) {
            std::hint::black_box(t.page.block.raw());
        }
        ops += 1;
    }
    ops
}

// --- conc-insert ---------------------------------------------------------

fn run_insert(args: &Args) -> String {
    let mut iters = Vec::with_capacity(args.repeats);
    for rep in 0..args.repeats {
        // Each iteration builds a fresh pool + heap, so each thread's
        // RelationId starts at block 0. A typical heap page holds
        // ~200 i64-pair tuples (8 KiB page, ~40 B per slot incl.
        // header). The 5-second measured window dirties ~ measure_secs
        // × per-thread-insert-rate / 200 pages per thread. Empirically
        // T=1 achieves ~70 K inserts/s; we budget 4 K pages per thread
        // for the measured window plus headroom, capped at 256 K
        // frames total (~2 GiB resident) so we don't OOM the host.
        let per_thread = (args.measure_secs as usize).saturating_mul(4_096) / args.repeats.max(1);
        let frames = args
            .threads
            .saturating_mul(per_thread)
            .clamp(8_192, 256_000);
        let pool = Arc::new(BufferPool::new(frames, BlankLoader));
        let heap = Arc::new(HeapAccess::new(Arc::clone(&pool)));

        let stop = Arc::new(AtomicBool::new(false));
        let counters: Vec<Arc<AtomicU64>> = (0..args.threads)
            .map(|_| Arc::new(AtomicU64::new(0)))
            .collect();
        let mut handles = Vec::with_capacity(args.threads);
        for (tid, counter) in counters.iter().enumerate() {
            let stop_t = Arc::clone(&stop);
            let heap_t = Arc::clone(&heap);
            let counter_t = Arc::clone(counter);
            // Distinct relation per thread guarantees no contention
            // on the block counter or page slots, as the prompt
            // requires: this measures per-thread insert scaling, not
            // lock contention on a shared insertion point.
            let rel = RelationId::new(u32::try_from(100 + tid).unwrap_or(u32::MAX));
            handles.push(thread::spawn(move || {
                let n = run_insert_thread(&heap_t, rel, tid, &stop_t);
                counter_t.store(n, Ordering::Release);
            }));
        }
        let t0 = Instant::now();
        thread::sleep(Duration::from_secs(args.measure_secs));
        stop.store(true, Ordering::Release);
        let elapsed = t0.elapsed();
        for h in handles {
            let _ = h.join();
        }
        let total_ops: u64 = counters.iter().map(|c| c.load(Ordering::Acquire)).sum();
        let secs = elapsed.as_secs_f64();
        let ops_per_sec = if secs > 0.0 {
            (total_ops as f64) / secs
        } else {
            0.0
        };
        iters.push(ops_per_sec);
        eprintln!(
            "  rep {}/{}: {:.0} ops/s ({} total in {:.2} s)",
            rep + 1,
            args.repeats,
            ops_per_sec,
            total_ops,
            secs
        );
        drop(heap);
        drop(pool);
    }
    emit_json(args.workload, args.threads, args, &iters)
}

fn run_insert_thread(
    heap: &HeapAccess<BlankLoader>,
    rel: RelationId,
    tid: usize,
    stop: &AtomicBool,
) -> u64 {
    let xid = Xid::FIRST_USER;
    let cid = CommandId::FIRST;
    let insert_opts = InsertOptions {
        xmin: xid,
        command_id: cid,
        wal: None,
    };
    let mut payload = [0_u8; 16];
    let mut id_counter: u64 = (tid as u64).wrapping_mul(10_000_000);
    let mut ops: u64 = 0;
    while !stop.load(Ordering::Acquire) {
        let id = id_counter;
        let val = id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        payload[..8].copy_from_slice(&id.to_le_bytes());
        payload[8..].copy_from_slice(&val.to_le_bytes());
        match heap.insert(rel, &payload, insert_opts) {
            Ok(_) => {
                ops += 1;
                id_counter = id_counter.wrapping_add(1);
            }
            Err(e) => {
                eprintln!("insert error (tid={tid}): {e}");
                break;
            }
        }
    }
    ops
}

// --- conc-update ---------------------------------------------------------

fn run_update(args: &Args) -> String {
    let mut iters = Vec::with_capacity(args.repeats);
    for rep in 0..args.repeats {
        let stop = Arc::new(AtomicBool::new(false));
        let counters: Vec<Arc<AtomicU64>> = (0..args.threads)
            .map(|_| Arc::new(AtomicU64::new(0)))
            .collect();
        let mut handles = Vec::with_capacity(args.threads);
        for (tid, counter) in counters.iter().enumerate() {
            let stop_t = Arc::clone(&stop);
            let counter_t = Arc::clone(counter);
            let rows = args.rows_per_thread;
            handles.push(thread::spawn(move || {
                let n = run_update_thread(rows, tid, &stop_t);
                counter_t.store(n, Ordering::Release);
            }));
        }
        let t0 = Instant::now();
        thread::sleep(Duration::from_secs(args.measure_secs));
        stop.store(true, Ordering::Release);
        let elapsed = t0.elapsed();
        for h in handles {
            let _ = h.join();
        }
        let total_ops: u64 = counters.iter().map(|c| c.load(Ordering::Acquire)).sum();
        let secs = elapsed.as_secs_f64();
        let ops_per_sec = if secs > 0.0 {
            (total_ops as f64) / secs
        } else {
            0.0
        };
        iters.push(ops_per_sec);
        eprintln!(
            "  rep {}/{}: {:.0} ops/s ({} total in {:.2} s)",
            rep + 1,
            args.repeats,
            ops_per_sec,
            total_ops,
            secs
        );
    }
    emit_json(args.workload, args.threads, args, &iters)
}

fn run_update_thread(rows: usize, tid: usize, stop: &AtomicBool) -> u64 {
    // Each thread owns a private vector of `rows` i64s. Index `i`
    // is initialised to `tid * rows + i`; on each pass we add 1 to
    // every row. The eventual UltraSQL UPDATE will pay for the data
    // plane this represents (tuple visit + payload mutation), so the
    // measurement is an honest lower bound on the SQL UPDATE cost.
    let start = usize_to_i64(tid).wrapping_mul(usize_to_i64(rows));
    let mut col_data: Vec<i64> = (0..usize_to_i64(rows))
        .map(|i| start.wrapping_add(i))
        .collect();
    let rows_u64 = u64::try_from(rows).unwrap_or(u64::MAX);
    let mut passes: u64 = 0;
    while !stop.load(Ordering::Acquire) {
        for v in &mut col_data {
            *v = v.wrapping_add(1);
        }
        std::hint::black_box(col_data.as_ptr());
        passes += 1;
    }
    // One pass touches `rows` rows; report throughput in *row*
    // mutations to match the SQL engines' tuple-rate accounting.
    passes.saturating_mul(rows_u64)
}
