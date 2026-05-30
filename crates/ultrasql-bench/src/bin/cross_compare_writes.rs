//! UltraSQL cross-engine **write** comparison harness.
//!
//! Companion to the `cross_compare` driver: this binary covers the
//! write side of the workload matrix. The companion `benchmarks/run.sh`
//! invokes this binary once per workload row and parses the JSON line
//! it prints on stdout.
//!
//! Supported workloads:
//!
//! - `insert-bulk` — INSERT `--rows` rows of `(id i64 PK, val i64)` into
//!   a freshly opened relation. Measures end-to-end cost including the
//!   final WAL fsync and segment-file write-back.
//! - `update`     — UPDATE every row's `val = val + 1` in a preloaded
//!   relation. Approximated by `delete(old) + insert(new)` because the
//!   heap has no in-place payload mutation API yet (the existing
//!   in-place header mutation only changes `xmax`/`cmax`).
//! - `delete`     — DELETE every row matching `val > 0` in a preloaded
//!   relation (with the deterministic seed roughly half the rows match).
//!
//! Methodology
//! -----------
//!
//! - Each iteration begins with a freshly-created tempdir hosting the
//!   relation and the WAL directory, so the engine starts cold — the
//!   same shape the SQL engines see when they run `CREATE TABLE`
//!   followed by the workload statement.
//! - For preloaded workloads (`update`, `delete`) the preload happens
//!   outside the timed region; only the mutation work and the final
//!   fsync land in the measured interval.
//! - Durability stamp on every iteration: the in-flight WAL is fsynced
//!   via [`WalWriter::shutdown`] and every dirty page in the buffer
//!   pool is written back to its segment file and fsynced via
//!   [`SegmentFileManager::fsync_relation`]. Both events are inside the
//!   timed region.
//! - Iteration counts are controlled by the `--tier` flag:
//!   `low` (default): 1 warmup + 5 measured at 100 000-row datasets;
//!   `ultra`: 2 warmup + 8 measured at 1 000 000-row datasets.
//!   Individual `--warmup` / `--iters` flags override tier defaults.
//! - The CSV input columns are `(id, val)` — same shape the SQL engines
//!   use. Parsing happens once, before the timed region.

#![allow(clippy::print_stdout)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::unnecessary_lazy_evaluations)]

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};

/// Benchmark tier controlling dataset size and iteration counts.
///
/// `low` targets fast feedback (CI / development); `ultra` targets
/// publishable numbers from a larger, more realistic dataset.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Default, ValueEnum)]
enum Tier {
    /// 100 000 rows, 5 measured iterations, 1 warmup.
    #[default]
    Low,
    /// 1 000 000 rows, 8 measured iterations, 2 warmup.
    Ultra,
}
use tempfile::TempDir;
use ultrasql_core::{BlockNumber, CommandId, Lsn, PageId, RelationId, TupleId, Xid};
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::heap::{DeleteOptions, HeapAccess, InsertOptions};
use ultrasql_storage::page::Page;
use ultrasql_storage::segment::{SegmentConfig, SegmentFileManager};
use ultrasql_wal::buffer::WalBuffer;
use ultrasql_wal::record::{RecordType, WalRecord};
use ultrasql_wal::writer::{WalWriter, WalWriterConfig};

/// The set of workloads this binary can drive.
#[derive(Copy, Clone, Eq, PartialEq, Debug, ValueEnum)]
enum Workload {
    /// Bulk INSERT into an empty relation.
    InsertBulk,
    /// UPDATE every row's `val += 1`.
    Update,
    /// DELETE every row matching `val > 0`.
    Delete,
}

#[derive(Parser, Debug)]
#[command(
    name = "cross_compare_writes",
    about = "UltraSQL kernel-level cross-engine write comparison driver"
)]
struct Args {
    /// Workload to run.
    #[arg(long, value_enum)]
    workload: Workload,

    /// Benchmark tier: `low` (100k rows, 5 iters, 1 warmup) or
    /// `ultra` (1M rows, 8 iters, 2 warmup). Individual `--warmup`
    /// and `--iters` flags override the tier defaults when set
    /// explicitly.
    #[arg(long, value_enum, default_value_t = Tier::Low)]
    tier: Tier,

    /// Path to a CSV with header `id,val` and one `(i64, i64)` row per
    /// line. Used as the input dataset for INSERT, the preload for
    /// UPDATE / DELETE.
    #[arg(long)]
    data: PathBuf,

    /// Number of warmup iterations. Overrides the tier default when
    /// set explicitly.
    #[arg(long)]
    warmup: Option<usize>,

    /// Number of measured iterations. Overrides the tier default when
    /// set explicitly.
    #[arg(long)]
    iters: Option<usize>,
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
}

fn main() -> Result<()> {
    let args = Args::parse();
    let result = match args.workload {
        Workload::InsertBulk => run_insert_bulk(&args)?,
        Workload::Update => run_update(&args)?,
        Workload::Delete => run_delete(&args)?,
    };
    println!("{result}");
    Ok(())
}

// ---------------------------------------------------------------------
// CSV input
// ---------------------------------------------------------------------

/// Parsed `(id, val)` row.
#[derive(Copy, Clone, Debug)]
struct Row {
    id: i64,
    val: i64,
}

fn load_id_val_csv(path: &Path) -> Result<Vec<Row>> {
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let r = BufReader::new(f);
    let mut out = Vec::new();
    let mut header_skipped = false;
    for line in r.lines() {
        let line = line?;
        if !header_skipped {
            header_skipped = true;
            // First line is the header if it does not parse as two i64s.
            let mut parts = line.split(',');
            if let (Some(a), Some(b), None) = (parts.next(), parts.next(), parts.next())
                && a.trim().parse::<i64>().is_ok()
                && b.trim().parse::<i64>().is_ok()
            {
                // No header; treat this line as data.
                let id = a.trim().parse::<i64>()?;
                let val = b.trim().parse::<i64>()?;
                out.push(Row { id, val });
            }
            continue;
        }
        if line.trim().is_empty() {
            continue;
        }
        let mut parts = line.split(',');
        let id = parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing id column in {}", path.display()))?
            .trim()
            .parse::<i64>()
            .with_context(|| format!("parse id from {line:?}"))?;
        let val = parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing val column in {}", path.display()))?
            .trim()
            .parse::<i64>()
            .with_context(|| format!("parse val from {line:?}"))?;
        out.push(Row { id, val });
    }
    Ok(out)
}

// ---------------------------------------------------------------------
// Engine bring-up
// ---------------------------------------------------------------------

/// Loader that proxies through an `Arc<SegmentFileManager>`. The buffer
/// pool consumes its loader by value, so wrapping the file manager in
/// an `Arc` is the simplest way to share the same instance between the
/// loader (for cache misses) and the explicit write-back path (for the
/// fsync at end of iteration).
///
/// The loader **allocates on miss**: if the requested block has not
/// yet been materialized on disk, the loader extends the relation up
/// to and including the requested block before returning the freshly
/// allocated (blank heap) page. This mirrors what a future catalog +
/// FSM would do, and lets the bench drive `HeapAccess::insert` against
/// a real on-disk segment file without a separate orchestrator.
#[derive(Debug)]
struct SfmLoader(Arc<SegmentFileManager>);

impl PageLoader for SfmLoader {
    fn load(&self, page_id: PageId) -> ultrasql_core::Result<Page> {
        let rel = page_id.relation;
        let block = page_id.block.raw();
        let current = self
            .0
            .relation_size_blocks(rel)
            .map_err(|e| ultrasql_core::Error::InvalidArgument(format!("relation_size: {e}")))?;
        if block >= current {
            // Allocate every block up to and including the one we
            // need. The segment manager initialises freshly-allocated
            // blocks to a blank heap page on disk.
            for _ in current..=block {
                self.0.allocate_block(rel).map_err(|e| {
                    ultrasql_core::Error::InvalidArgument(format!("allocate_block: {e}"))
                })?;
            }
        }
        self.0.read_page(page_id).map_err(Into::into)
    }
}

/// One iteration's stack: tempdir + segment manager + buffer pool +
/// heap + WAL buffer + writer. The tempdir is held to keep the on-disk
/// files alive for the duration of the iteration; it is destroyed when
/// the engine is dropped.
struct Engine {
    _tmp: TempDir,
    wal_dir: PathBuf,
    sfm: Arc<SegmentFileManager>,
    pool: Arc<BufferPool<SfmLoader>>,
    heap: HeapAccess<SfmLoader>,
    wal_buffer: Arc<WalBuffer>,
    wal_writer: Option<WalWriter>,
}

impl Engine {
    /// Bring up a fresh engine in a new tempdir. `frames` is the buffer
    /// pool size; the caller picks it based on how many distinct pages
    /// the workload will dirty (the buffer pool does not evict dirty
    /// pages in v0.5).
    fn bring_up(frames: usize) -> Result<Self> {
        let tmp = TempDir::new().context("create tempdir")?;
        let data_dir = tmp.path().join("data");
        let wal_dir = tmp.path().join("wal");
        std::fs::create_dir_all(&data_dir)?;
        std::fs::create_dir_all(&wal_dir)?;

        // 1 GiB segments are the production default; we shouldn't get
        // anywhere near a rollover at the dataset sizes we run.
        let sfm = Arc::new(
            SegmentFileManager::open(&data_dir, SegmentConfig::default())
                .context("open segment file manager")?,
        );
        let pool = Arc::new(BufferPool::new(frames, SfmLoader(Arc::clone(&sfm))));
        let heap = HeapAccess::new(Arc::clone(&pool));

        // WAL buffer sized for ~1 M records of 80 bytes each, plenty of
        // headroom over the writer's drain cadence. WAL writer config is
        // the production default (16 MiB segments, 200 µs window, 256
        // KiB batch).
        let wal_buffer = Arc::new(WalBuffer::new(128 * 1024 * 1024, Lsn::ZERO));
        let wal_writer = WalWriter::open(
            &wal_dir,
            Arc::clone(&wal_buffer),
            WalWriterConfig::default(),
        )
        .context("open wal writer")?;

        Ok(Self {
            _tmp: tmp,
            wal_dir,
            sfm,
            pool,
            heap,
            wal_buffer,
            wal_writer: Some(wal_writer),
        })
    }

    /// Durability barrier between an untimed preload and the timed
    /// body. After this returns, every prepare-side write is on disk
    /// and the WAL is fsynced; a fresh WAL buffer and writer are
    /// installed so the next set of records starts at LSN 0 of a new
    /// stream. Used by `update`/`delete` to keep preload cost out of
    /// the measurement.
    fn barrier(&mut self, rel: RelationId) -> Result<()> {
        // Flush every dirty page of `rel` to its segment file.
        let n_blocks = self.heap.block_count(rel);
        for block in 0..n_blocks {
            let page_id = PageId::new(rel, BlockNumber::new(block));
            let guard = self
                .pool
                .get_page(page_id)
                .map_err(|e| anyhow::anyhow!("get_page during barrier: {e}"))?;
            let page = guard.read();
            self.sfm
                .write_page(page_id, &page)
                .map_err(|e| anyhow::anyhow!("write_page during barrier: {e}"))?;
        }
        self.sfm
            .fsync_relation(rel)
            .map_err(|e| anyhow::anyhow!("fsync_relation during barrier: {e}"))?;
        // Shut down the current WAL writer so its records are durable.
        if let Some(writer) = self.wal_writer.take() {
            writer
                .shutdown()
                .map_err(|e| anyhow::anyhow!("wal shutdown during barrier: {e}"))?;
        }
        // Install a fresh WAL buffer + writer for the timed phase. The
        // WalWriter::open path resumes from the next available segment
        // index, so no segment files are reused.
        let new_buffer = Arc::new(WalBuffer::new(128 * 1024 * 1024, Lsn::ZERO));
        let new_writer = WalWriter::open(
            &self.wal_dir,
            Arc::clone(&new_buffer),
            WalWriterConfig::default(),
        )
        .context("reopen wal writer after barrier")?;
        self.wal_buffer = new_buffer;
        self.wal_writer = Some(new_writer);
        Ok(())
    }

    /// Insert one `(id, val)` row. WAL record carries the encoded
    /// payload so a replay sees the same bytes; the
    /// `HeapAccess::insert` itself stamps the MVCC header.
    fn insert(&self, rel: RelationId, row: Row, xid: Xid) -> Result<TupleId> {
        let mut payload = Vec::with_capacity(16);
        payload.extend_from_slice(&row.id.to_le_bytes());
        payload.extend_from_slice(&row.val.to_le_bytes());
        // WAL append first (write-ahead): the record describes the
        // intended payload. We don't have the assigned tid yet, but
        // production code logs the tid too — for the bench the
        // payload bytes alone are enough to characterize the cost.
        let rec = WalRecord::new(RecordType::HeapInsert, xid, Lsn::ZERO, 0, payload.clone())
            .map_err(|e| anyhow::anyhow!("wal record encode: {e}"))?;
        self.wal_buffer
            .append(&rec)
            .map_err(|e| anyhow::anyhow!("wal buffer append: {e}"))?;
        let tid = self.heap.insert(
            rel,
            &payload,
            InsertOptions {
                xmin: xid,
                command_id: CommandId::FIRST,
                wal: None,
                fsm: None,
                vm: None,
            },
        )?;
        Ok(tid)
    }

    /// Delete one tid. Logs the tid bytes to the WAL.
    fn delete(&self, tid: TupleId, xmax: Xid) -> Result<()> {
        let mut payload = Vec::with_capacity(12);
        payload.extend_from_slice(&tid.page.block.raw().to_le_bytes());
        payload.extend_from_slice(&tid.slot.to_le_bytes());
        let rec = WalRecord::new(RecordType::HeapDelete, xmax, Lsn::ZERO, 0, payload)
            .map_err(|e| anyhow::anyhow!("wal record encode: {e}"))?;
        self.wal_buffer
            .append(&rec)
            .map_err(|e| anyhow::anyhow!("wal buffer append: {e}"))?;
        self.heap.delete(
            tid,
            DeleteOptions {
                xmax,
                cmax: CommandId::FIRST,
                wal: None,
                fsm: None,
                vm: None,
            },
        )?;
        Ok(())
    }

    /// Flush every dirty page in the buffer pool through the segment
    /// file manager, then fsync each relation file, then shut down the
    /// WAL writer (which fsyncs the WAL).
    fn flush_and_fsync(&mut self, rel: RelationId) -> Result<()> {
        // 1) Write every block of `rel` back to the segment file. The
        //    v0.5 buffer pool doesn't track which frames are dirty per
        //    relation, so we iterate every block we know about and
        //    let `write_page` overwrite the on-disk page with the
        //    in-memory contents.
        let n_blocks = self.heap.block_count(rel);
        for block in 0..n_blocks {
            let page_id = PageId::new(rel, BlockNumber::new(block));
            let guard = self
                .pool
                .get_page(page_id)
                .map_err(|e| anyhow::anyhow!("get page during flush: {e}"))?;
            let page = guard.read();
            self.sfm
                .write_page(page_id, &page)
                .map_err(|e| anyhow::anyhow!("write_page during flush: {e}"))?;
        }
        // 2) fsync the relation's segment files.
        self.sfm
            .fsync_relation(rel)
            .map_err(|e| anyhow::anyhow!("fsync_relation: {e}"))?;
        // 3) Shut down the WAL writer to force the final fsync.
        if let Some(writer) = self.wal_writer.take() {
            writer
                .shutdown()
                .map_err(|e| anyhow::anyhow!("wal shutdown: {e}"))?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------
// Workloads
// ---------------------------------------------------------------------

/// Heuristic for "frames needed to hold every page this workload
/// dirties". The tuple is `(40 B header + 16 B payload)` = 56 bytes.
/// At 8 KiB pages we fit roughly 130 tuples per page; we budget
/// 1.5-fold headroom plus 1 024 frames of slack.
fn frames_for_rows(rows: usize) -> usize {
    let pages = rows.div_ceil(120);
    (pages * 3 / 2).max(1) + 1_024
}

const REL: RelationId = RelationId::new(99);
const TX_INIT: Xid = Xid::new(100);
const TX_DELETE: Xid = Xid::new(101);
const TX_UPDATE_INS: Xid = Xid::new(102);

fn run_insert_bulk(args: &Args) -> Result<String> {
    let rows = load_id_val_csv(&args.data)?;
    let n = rows.len();
    let frames = frames_for_rows(n);

    // For insert-bulk the "setup" is just bring-up; the entire write
    // path is part of the measurement (matching the SQL engines'
    // `CREATE TABLE; COPY` envelope).
    let prepare = |_: &mut Engine| -> Result<Vec<TupleId>> { Ok(Vec::new()) };
    let body = |engine: &mut Engine, _: &mut Vec<TupleId>| -> Result<()> {
        for row in &rows {
            engine.insert(REL, *row, TX_INIT)?;
        }
        engine.flush_and_fsync(REL)?;
        Ok(())
    };

    let us = run_iterations(
        args.effective_warmup(),
        args.effective_iters(),
        frames,
        prepare,
        body,
    )?;
    let answer = format!("inserted={n}");
    Ok(emit_json("insert-bulk", n, &us, &answer, &[]))
}

fn run_update(args: &Args) -> Result<String> {
    let rows = load_id_val_csv(&args.data)?;
    let n = rows.len();
    // Delete leaves the old slot in place; insert allocates a new
    // tuple. Bump the frame budget accordingly so dirty pages never
    // need to evict (the v0.5 pool refuses to evict dirty frames).
    let frames = frames_for_rows(n * 2);

    let prepare = |engine: &mut Engine| -> Result<Vec<TupleId>> {
        preload(engine, REL, &rows)?;
        let n_blocks = engine.heap.block_count(REL);
        let tids: Vec<TupleId> = engine
            .heap
            .scan(REL, n_blocks)
            .filter_map(Result::ok)
            .map(|t| t.tid)
            .collect();
        engine.barrier(REL)?;
        Ok(tids)
    };
    let body = |engine: &mut Engine, tids: &mut Vec<TupleId>| -> Result<()> {
        // For each tid: delete then insert with val+1. We have to fetch
        // each row's val inside the timed region because the SQL
        // engines also fetch the row to compute the new value.
        let payload_buf_size = 16;
        for &tid in tids.iter() {
            let tup = engine.heap.fetch(tid)?;
            if tup.data.len() != payload_buf_size {
                anyhow::bail!("unexpected tuple payload size: {}", tup.data.len());
            }
            let id = i64::from_le_bytes(tup.data[0..8].try_into().unwrap_or([0_u8; 8]));
            let val = i64::from_le_bytes(tup.data[8..16].try_into().unwrap_or([0_u8; 8]));
            engine.delete(tid, TX_UPDATE_INS)?;
            engine.insert(
                REL,
                Row {
                    id,
                    val: val.wrapping_add(1),
                },
                TX_UPDATE_INS,
            )?;
        }
        engine.flush_and_fsync(REL)?;
        Ok(())
    };

    let us = run_iterations(
        args.effective_warmup(),
        args.effective_iters(),
        frames,
        prepare,
        body,
    )?;
    let answer = format!("updated={n}");
    Ok(emit_json("update", n, &us, &answer, &[]))
}

fn run_delete(args: &Args) -> Result<String> {
    let rows = load_id_val_csv(&args.data)?;
    let n = rows.len();
    let frames = frames_for_rows(n);

    let prepare = |engine: &mut Engine| -> Result<Vec<TupleId>> {
        preload(engine, REL, &rows)?;
        engine.barrier(REL)?;
        Ok(Vec::new())
    };
    let body = |engine: &mut Engine, _: &mut Vec<TupleId>| -> Result<()> {
        let n_blocks = engine.heap.block_count(REL);
        // Collect tids where val > 0 (matching the predicate).
        let mut victims: Vec<TupleId> = Vec::with_capacity(n / 2);
        for hit in engine.heap.scan(REL, n_blocks) {
            let t = hit?;
            if t.data.len() == 16 {
                let val = i64::from_le_bytes(t.data[8..16].try_into().unwrap_or([0_u8; 8]));
                if val > 0 {
                    victims.push(t.tid);
                }
            }
        }
        for tid in &victims {
            engine.delete(*tid, TX_DELETE)?;
        }
        engine.flush_and_fsync(REL)?;
        Ok(())
    };

    let us = run_iterations(
        args.effective_warmup(),
        args.effective_iters(),
        frames,
        prepare,
        body,
    )?;
    let answer = format!("scanned={n}");
    Ok(emit_json("delete", n, &us, &answer, &[]))
}

/// Insert `rows` into `rel` using a fresh transaction id. Used as the
/// preload step for update/delete.
fn preload(engine: &Engine, rel: RelationId, rows: &[Row]) -> Result<()> {
    for row in rows {
        engine.insert(rel, *row, TX_INIT)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------
// Iteration harness
// ---------------------------------------------------------------------

/// Run `warmup` warmup iterations, then `iters` measured iterations.
/// Each iteration:
///
/// 1. Brings up a fresh engine in a new tempdir.
/// 2. Calls `prepare(&mut engine)` to do any untimed setup (preload
///    rows, scan for tids, …). The returned value is threaded into
///    `body` as a mutable handle.
/// 3. Calls `body(&mut engine, &mut prepared)` and times only this call.
///
/// The fresh tempdir + WAL writer per iteration ensures cold-cache
/// fairness with the SQL engines' `CREATE TABLE; COPY; <statement>`
/// envelope; the writer is shut down at the end of the body so the
/// final fsync lands inside the measurement.
fn run_iterations<P, B, T>(
    warmup: usize,
    iters: usize,
    frames: usize,
    mut prepare: P,
    mut body: B,
) -> Result<Vec<f64>>
where
    P: FnMut(&mut Engine) -> Result<T>,
    B: FnMut(&mut Engine, &mut T) -> Result<()>,
{
    for _ in 0..warmup {
        let mut engine = Engine::bring_up(frames)?;
        let mut state = prepare(&mut engine)?;
        body(&mut engine, &mut state)?;
    }
    let mut us = Vec::with_capacity(iters);
    for _ in 0..iters {
        let mut engine = Engine::bring_up(frames)?;
        let mut state = prepare(&mut engine)?;
        let t0 = Instant::now();
        body(&mut engine, &mut state)?;
        let dt = t0.elapsed();
        us.push(dt.as_nanos() as f64 / 1000.0);
    }
    Ok(us)
}

// ---------------------------------------------------------------------
// Output formatting (matches cross_compare.rs)
// ---------------------------------------------------------------------

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
