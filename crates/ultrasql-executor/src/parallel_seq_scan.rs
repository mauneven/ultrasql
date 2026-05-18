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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        heap: Arc<HeapAccess<L>>,
        relation: RelationId,
        block_count: u32,
        snapshot: Snapshot,
        oracle: Arc<O>,
        vm: Arc<VisibilityMap>,
        codec: RowCodec,
        worker_count: usize,
    ) -> Self {
        Self::new_with_cancel(
            heap,
            relation,
            block_count,
            snapshot,
            oracle,
            vm,
            codec,
            None,
            worker_count,
        )
    }

    /// Spawn workers and thread cancellation into each worker scan.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_cancel(
        heap: Arc<HeapAccess<L>>,
        relation: RelationId,
        block_count: u32,
        snapshot: Snapshot,
        oracle: Arc<O>,
        vm: Arc<VisibilityMap>,
        codec: RowCodec,
        cancel_flag: Option<CancelFlag>,
        worker_count: usize,
    ) -> Self {
        let workers = worker_count.max(1).min(block_count.max(1) as usize);
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
                let mut scan = SeqScan::new_range_with_vm(
                    worker_heap,
                    relation,
                    start,
                    end,
                    worker_snapshot,
                    worker_oracle,
                    worker_vm,
                    worker_codec,
                );
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
    let workers = available.min(8).min(block_count as usize);
    if workers <= 1 {
        return 1;
    }
    let row_width_penalty = (row_width.max(1) as f64).sqrt() / 8.0;
    let seq_cost =
        f64::from(block_count) + f64::from(block_count) * ROWS_PER_BLOCK_HINT * CPU_TUPLE_COST;
    let parallel_cost = (seq_cost / workers as f64) + PARALLEL_SETUP_COST + row_width_penalty;
    if parallel_cost < seq_cost { workers } else { 1 }
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

        let mut scan = ParallelSeqScan::new(heap, rel, 4, snapshot, oracle, vm, codec, 4);
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
