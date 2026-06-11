//! Parallel heap sequential scan.
//!
//! Splits a relation into disjoint block ranges, runs one `SeqScan` per
//! worker thread, and streams worker batches back to the coordinator over
//! an MPSC queue. Output order is intentionally unspecified, matching SQL
//! semantics for scans without `ORDER BY`.

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver};
use std::thread;

use ultrasql_core::{RelationId, Schema};
use ultrasql_mvcc::{Snapshot, XidStatusOracle};
use ultrasql_storage::PageLoader;
use ultrasql_storage::heap::HeapAccess;
use ultrasql_storage::vm::VisibilityMap;
use ultrasql_vec::Batch;

use crate::seq_scan::SeqScanRangeWithVmConfig;
use crate::{CancelFlag, ExecError, Operator, RowCodec, SeqScan};

enum WorkerMessage {
    Batch(Box<Batch>),
    Done,
    Error(ExecError),
}

/// Parallel block-partitioned heap scan.
#[derive(Debug)]
pub struct ParallelSeqScan<L: PageLoader + 'static, O: XidStatusOracle + ?Sized + 'static> {
    receiver: Receiver<WorkerMessage>,
    schema: Schema,
    live_workers: usize,
    row_hint: Option<usize>,
    _marker: PhantomData<(L, O)>,
}

/// Construction inputs for [`ParallelSeqScan`].
///
/// The caller is responsible for choosing `worker_count` from relation size
/// and row width so short scans do not pay thread setup overhead.
pub struct ParallelSeqScanConfig<L: PageLoader + 'static, O: XidStatusOracle + ?Sized + 'static> {
    /// Shared heap access method for the target relation.
    pub heap: Arc<HeapAccess<L>>,
    /// Relation to scan.
    pub relation: RelationId,
    /// Number of heap blocks to partition across workers.
    pub block_count: u32,
    /// MVCC snapshot used by each worker scan.
    pub snapshot: Snapshot,
    /// Transaction-status oracle shared by worker scans.
    pub oracle: Arc<O>,
    /// Visibility map shared by worker scans.
    pub vm: Arc<VisibilityMap>,
    /// Row codec for the relation payload schema.
    pub codec: RowCodec,
    /// Optional per-query cancel signal cloned into each worker.
    pub cancel_flag: Option<CancelFlag>,
    /// Requested worker count, clamped to the available block range.
    pub worker_count: usize,
}

impl<L: PageLoader + 'static, O: XidStatusOracle + ?Sized + 'static> std::fmt::Debug
    for ParallelSeqScanConfig<L, O>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParallelSeqScanConfig")
            .field("relation", &self.relation)
            .field("block_count", &self.block_count)
            .field("schema", self.codec.schema())
            .field(
                "cancelled",
                &self.cancel_flag.as_ref().is_some_and(CancelFlag::is_set),
            )
            .field("worker_count", &self.worker_count)
            .finish_non_exhaustive()
    }
}

impl<L, O> ParallelSeqScan<L, O>
where
    L: PageLoader + Send + Sync + std::fmt::Debug + 'static,
    O: XidStatusOracle + Send + Sync + std::fmt::Debug + 'static,
{
    /// Spawn workers over disjoint heap block ranges.
    ///
    /// `worker_count` is clamped to `1..=block_count`; callers should
    /// use [`choose_parallel_seq_scan_workers`] to avoid paying thread
    /// setup on small scans.
    #[must_use]
    pub fn new(config: ParallelSeqScanConfig<L, O>) -> Self {
        let ParallelSeqScanConfig {
            heap,
            relation,
            block_count,
            snapshot,
            oracle,
            vm,
            codec,
            cancel_flag,
            worker_count,
        } = config;
        let workers = worker_count
            .max(1)
            .min(usize_from_u32_saturating(block_count.max(1)));
        let schema = codec.schema().clone();
        let (sender, receiver) = mpsc::channel();
        let blocks_per_worker = block_count.div_ceil(u32::try_from(workers).unwrap_or(u32::MAX));
        let mut live_workers = 0;

        for worker in 0..workers {
            let start = u32::try_from(worker)
                .unwrap_or(u32::MAX)
                .saturating_mul(blocks_per_worker);
            let end = start.saturating_add(blocks_per_worker).min(block_count);
            if start >= end {
                continue;
            }
            live_workers += 1;
            let tx = sender.clone();
            let worker_heap = Arc::clone(&heap);
            let worker_oracle = Arc::clone(&oracle);
            let worker_vm = Arc::clone(&vm);
            let worker_snapshot = snapshot.clone();
            let worker_codec = codec.clone();
            let worker_cancel = cancel_flag.clone();
            thread::spawn(move || {
                let mut scan = SeqScan::new_range_with_vm(SeqScanRangeWithVmConfig {
                    heap: worker_heap,
                    relation,
                    start_block: start,
                    end_block: end,
                    snapshot: worker_snapshot,
                    oracle: worker_oracle,
                    vm: worker_vm,
                    codec: worker_codec,
                });
                if let Some(flag) = worker_cancel {
                    scan = scan.with_cancel_flag(flag);
                }
                loop {
                    match scan.next_batch() {
                        Ok(Some(batch)) => {
                            if tx.send(WorkerMessage::Batch(Box::new(batch))).is_err() {
                                return;
                            }
                        }
                        Ok(None) => {
                            let _ = tx.send(WorkerMessage::Done);
                            return;
                        }
                        Err(e) => {
                            let _ = tx.send(WorkerMessage::Error(e));
                            return;
                        }
                    }
                }
            });
        }
        drop(sender);

        Self {
            receiver,
            schema,
            live_workers,
            row_hint: None,
            _marker: PhantomData,
        }
    }
}

impl<L, O> Operator for ParallelSeqScan<L, O>
where
    L: PageLoader + Send + Sync + std::fmt::Debug + 'static,
    O: XidStatusOracle + Send + Sync + std::fmt::Debug + 'static,
{
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        while self.live_workers > 0 {
            match self.receiver.recv() {
                Ok(WorkerMessage::Batch(batch)) => return Ok(Some(*batch)),
                Ok(WorkerMessage::Done) => {
                    self.live_workers -= 1;
                }
                Ok(WorkerMessage::Error(e)) => return Err(e),
                Err(_) => {
                    self.live_workers = 0;
                    return Ok(None);
                }
            }
        }
        Ok(None)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn estimated_row_count(&self) -> Option<usize> {
        self.row_hint
    }
}

/// Cost gate for parallel scan selection.
///
/// Uses the existing optimizer shape: sequential cost is page IO plus rough
/// tuple CPU, parallel cost divides that by workers and adds setup. The
/// lowerer calls this only for plain read scans; TID scans and tiny tables
/// stay sequential.
#[must_use]
pub fn choose_parallel_seq_scan_workers(block_count: u32, row_width: usize) -> usize {
    const MIN_BLOCKS: u32 = 256;
    const PARALLEL_SETUP_COST: f64 = 1000.0;
    const CPU_TUPLE_COST: f64 = 0.01;
    const ROWS_PER_BLOCK_HINT: f64 = 128.0;

    if block_count < MIN_BLOCKS {
        return 1;
    }
    let available = thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    let workers = available.min(8).min(usize_from_u32_saturating(block_count));
    if workers <= 1 {
        return 1;
    }
    let row_width_penalty = f64_from_usize_saturating(row_width.max(1)).sqrt() / 8.0;
    let seq_cost =
        f64::from(block_count) + f64::from(block_count) * ROWS_PER_BLOCK_HINT * CPU_TUPLE_COST;
    let parallel_cost =
        (seq_cost / f64_from_usize_saturating(workers)) + PARALLEL_SETUP_COST + row_width_penalty;
    if parallel_cost < seq_cost { workers } else { 1 }
}

fn usize_from_u32_saturating(value: u32) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

fn f64_from_usize_saturating(value: usize) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use parking_lot::Mutex;
    use ultrasql_core::constants::PAGE_SIZE;
    use ultrasql_core::{CommandId, DataType, Field, PageId, Result, Value, Xid};
    use ultrasql_mvcc::status::test_support::MapOracle;
    use ultrasql_storage::buffer_pool::BufferPool;
    use ultrasql_storage::heap::{HeapAccess, InsertOptions};
    use ultrasql_storage::page::Page;

    use super::*;
    use crate::filter_op::batch_to_rows;

    #[derive(Debug, Default)]
    struct MapLoader {
        store: Mutex<HashMap<PageId, Box<[u8; PAGE_SIZE]>>>,
    }

    impl PageLoader for MapLoader {
        fn load(&self, page_id: PageId) -> Result<Page> {
            let stored = self.store.lock().get(&page_id).map(|bytes| {
                let mut copy: Box<[u8; PAGE_SIZE]> = vec![0_u8; PAGE_SIZE]
                    .into_boxed_slice()
                    .try_into()
                    .expect("alloc matches PAGE_SIZE");
                copy.copy_from_slice(&**bytes);
                copy
            });
            if let Some(bytes) = stored {
                return Page::from_bytes(bytes)
                    .map_err(|e| ultrasql_core::Error::Corruption(format!("test loader: {e}")));
            }
            let page = Page::new_heap();
            let mut copy: Box<[u8; PAGE_SIZE]> = vec![0_u8; PAGE_SIZE]
                .into_boxed_slice()
                .try_into()
                .expect("alloc matches PAGE_SIZE");
            copy.copy_from_slice(page.as_bytes());
            self.store.lock().insert(page_id, copy);
            Ok(page)
        }
    }

    #[test]
    fn worker_cost_gate_rejects_small_scans() {
        assert_eq!(choose_parallel_seq_scan_workers(4, 8), 1);
    }

    #[test]
    fn worker_cost_gate_handles_max_block_count() {
        assert!((1..=8).contains(&choose_parallel_seq_scan_workers(u32::MAX, 8)));
    }

    #[test]
    fn parallel_seq_scan_reads_disjoint_ranges() {
        let pool = Arc::new(BufferPool::new(64, MapLoader::default()));
        let heap = Arc::new(HeapAccess::new(pool));
        let rel = RelationId::new(7);
        let schema = Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("val", DataType::Int32),
        ])
        .unwrap();
        let codec = RowCodec::new(schema.clone());
        for i in 0..512_i32 {
            let payload = codec
                .encode(&[Value::Int32(i), Value::Int32(i * 10)])
                .unwrap();
            heap.insert(
                rel,
                &payload,
                InsertOptions {
                    xmin: Xid::new(10),
                    command_id: CommandId::FIRST,
                    n_atts: 2,
                    wal: None,
                    fsm: None,
                    vm: None,
                },
            )
            .unwrap();
        }
        let oracle = Arc::new(MapOracle::new());
        oracle.set_committed(Xid::new(10));
        let snapshot = Snapshot::new(
            Xid::new(50),
            Xid::new(500),
            Xid::new(99),
            CommandId::FIRST,
            std::iter::empty(),
        );
        let vm = Arc::new(VisibilityMap::new());
        heap.vacuum_mark_all_visible(
            rel,
            heap.block_count(rel),
            Xid::new(50),
            oracle.as_ref(),
            vm.as_ref(),
        )
        .unwrap();

        let mut scan = ParallelSeqScan::new(ParallelSeqScanConfig {
            heap,
            relation: rel,
            block_count: 4,
            snapshot,
            oracle,
            vm,
            codec,
            cancel_flag: None,
            worker_count: 4,
        });
        let mut ids = Vec::new();
        while let Some(batch) = scan.next_batch().unwrap() {
            for row in batch_to_rows(&batch, &schema).unwrap() {
                if let Value::Int32(id) = row[0] {
                    ids.push(id);
                }
            }
        }
        ids.sort_unstable();
        assert_eq!(ids, (0..512).collect::<Vec<_>>());
    }
}
