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
use ultrasql_txn::TransactionManager;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};

use crate::fused_update::{FusedCmp, FusedPredicate};
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
        let schema = Schema::new([Field::required("count", DataType::Int64)])
            .expect("affected-count schema is well-formed");
        Self {
            heap,
            relation,
            snapshot,
            oracle,
            block_count,
            predicate,
            xid,
            command_id,
            schema,
            done: false,
        }
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
        let n = self
            .heap
            .delete_int32_pair_inplace(
                self.relation,
                self.block_count,
                &self.snapshot,
                &*self.oracle,
                predicate_fn,
                self.xid,
                self.command_id,
                wal_sink,
            )
            .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;

        let affected_i64 = i64::try_from(n).unwrap_or(i64::MAX);
        let batch = Batch::new([Column::Int64(NumericColumn::from_data(vec![affected_i64]))])
            .map_err(ExecError::from)?;
        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[allow(dead_code)]
#[doc(hidden)]
fn _unused_cmp_helper(_c: FusedCmp) {}
