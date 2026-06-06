//! [`FusedDeleteInt32Pair`] — single-pass DELETE for the
//! `DELETE FROM t [WHERE col_j cmp lit]` shape over a
//! `(Int32, Int32)` relation.
//!
//! Mirrors the architectural shift in [`crate::fused_update`]: drop
//! the default `ModifyTable(Filter(SeqScan))` chain in favour of a
//! single page-major traversal that holds one source-page write
//! guard at a time, decodes ItemId + minimal-visibility header +
//! payload inline, and stamps the source slot's header
//! (`xmax / cmax / infomask | UPDATED`) per row that passes the
//! predicate. The slot's payload is **not** touched — DELETE is the
//! classical "mark dead via xmax" path.
//!
//! Shape recognised:
//!
//! - Relation schema is exactly `(Int32, Int32)`.
//! - Optional `WHERE col_j cmp literal` predicate where the column
//!   is `Int32` and the literal is `Int32`.
//!
//! Any other shape falls back to the default
//! `ModifyTable(Filter(SeqScan))` plan in `pipeline.rs`.

use std::sync::Arc;

use ultrasql_core::{CommandId, DataType, Field, RelationId, Schema, Xid};
use ultrasql_mvcc::Snapshot;
use ultrasql_storage::PageLoader;
use ultrasql_storage::heap::HeapAccess;
use ultrasql_storage::vm::VisibilityMap;
use ultrasql_txn::TransactionManager;
use ultrasql_vec::Batch;

use crate::affected_rows::affected_rows_batch;
use crate::fused_update::FusedPredicate;
use crate::{ExecError, Operator};

pub struct FusedDeleteInt32Pair<L: PageLoader> {
    heap: Arc<HeapAccess<L>>,
    relation: RelationId,
    snapshot: Snapshot,
    oracle: Arc<TransactionManager>,
    block_count: u32,
    predicate: Option<FusedPredicate>,
    xid: Xid,
    command_id: CommandId,
    vm: Option<Arc<VisibilityMap>>,
    schema: Schema,
    done: bool,
}

impl<L: PageLoader> std::fmt::Debug for FusedDeleteInt32Pair<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FusedDeleteInt32Pair")
            .field("relation", &self.relation)
            .field("predicate", &self.predicate)
            .field("block_count", &self.block_count)
            .finish()
    }
}

impl<L: PageLoader> FusedDeleteInt32Pair<L> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        heap: Arc<HeapAccess<L>>,
        relation: RelationId,
        snapshot: Snapshot,
        oracle: Arc<TransactionManager>,
        block_count: u32,
        predicate: Option<FusedPredicate>,
        xid: Xid,
        command_id: CommandId,
    ) -> Self {
        let schema = Schema::new_with_duplicate_names([Field::required("count", DataType::Int64)]);
        Self {
            heap,
            relation,
            snapshot,
            oracle,
            block_count,
            predicate,
            xid,
            command_id,
            vm: None,
            schema,
            done: false,
        }
    }

    #[must_use]
    pub fn with_visibility_map(mut self, vm: Arc<VisibilityMap>) -> Self {
        self.vm = Some(vm);
        self
    }
}

impl<L: PageLoader + Send + Sync + std::fmt::Debug + 'static> Operator for FusedDeleteInt32Pair<L> {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let predicate = self.predicate;

        // Build a closure that the heap path can invoke per tuple to
        // decide eligibility. None means "delete every visible tuple".
        let predicate_fn = |id: i32, val: i32| -> bool {
            match predicate {
                None => true,
                Some(pred) => {
                    let key = if pred.col_index == 0 { id } else { val };
                    pred.op.check(key, pred.literal)
                }
            }
        };
        let wal_sink_arc = self.heap.wal_sink().cloned();
        let wal_sink: Option<&dyn ultrasql_storage::WalSink> = wal_sink_arc.as_deref();
        let n = if wal_sink.is_some() {
            self.heap.delete_int32_pair_inplace(
                self.relation,
                self.block_count,
                &self.snapshot,
                &*self.oracle,
                predicate_fn,
                self.xid,
                self.command_id,
                wal_sink,
                self.vm.as_deref(),
            )
        } else {
            self.heap.delete_int32_pair_inplace_parallel_no_wal(
                self.relation,
                self.block_count,
                &self.snapshot,
                &*self.oracle,
                predicate_fn,
                self.xid,
                self.command_id,
                self.vm.as_deref(),
            )
        }
        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;

        Ok(Some(affected_rows_batch(n, "fused DELETE")?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
