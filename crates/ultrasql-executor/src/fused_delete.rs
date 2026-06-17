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
use ultrasql_storage::heap::{
    DeleteInt32PairScan, DeleteInt32PairStamp, HeapAccess, Int32PairCmp, Int32PairPredicate,
};
use ultrasql_storage::vm::VisibilityMap;
use ultrasql_txn::TransactionManager;
use ultrasql_vec::Batch;

use crate::affected_rows::affected_rows_batch;
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
    vm: Option<Arc<VisibilityMap>>,
    schema: Schema,
    done: bool,
}

/// Construction inputs for [`FusedDeleteInt32Pair`].
///
/// The caller supplies only plans whose relation schema and optional
/// predicate match the module-level fused DELETE shape.
pub struct FusedDeleteInt32PairConfig<L: PageLoader> {
    /// Shared heap access method for the target relation.
    pub heap: Arc<HeapAccess<L>>,
    /// Target relation identifier.
    pub relation: RelationId,
    /// Statement snapshot used for MVCC visibility.
    pub snapshot: Snapshot,
    /// Transaction manager used as the XID status oracle.
    pub oracle: Arc<TransactionManager>,
    /// Number of heap blocks to scan.
    pub block_count: u32,
    /// Optional Int32 comparison predicate.
    pub predicate: Option<FusedPredicate>,
    /// Transaction ID stamped into deleted tuples.
    pub xid: Xid,
    /// Command ID stamped into deleted tuples.
    pub command_id: CommandId,
}

impl<L: PageLoader> std::fmt::Debug for FusedDeleteInt32PairConfig<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FusedDeleteInt32PairConfig")
            .field("relation", &self.relation)
            .field("block_count", &self.block_count)
            .field("predicate", &self.predicate)
            .field("xid", &self.xid)
            .field("command_id", &self.command_id)
            .finish_non_exhaustive()
    }
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
    /// Construct the fused DELETE operator from validated lowering inputs.
    #[must_use]
    pub fn new(config: FusedDeleteInt32PairConfig<L>) -> Self {
        let FusedDeleteInt32PairConfig {
            heap,
            relation,
            snapshot,
            oracle,
            block_count,
            predicate,
            xid,
            command_id,
        } = config;
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

        let predicate = match self.predicate {
            None => Int32PairPredicate::All,
            Some(pred) => {
                let op = match pred.op {
                    FusedCmp::Eq => Int32PairCmp::Eq,
                    FusedCmp::Ne => Int32PairCmp::Ne,
                    FusedCmp::Lt => Int32PairCmp::Lt,
                    FusedCmp::Le => Int32PairCmp::Le,
                    FusedCmp::Gt => Int32PairCmp::Gt,
                    FusedCmp::Ge => Int32PairCmp::Ge,
                };
                Int32PairPredicate::ColumnCmp {
                    col_index: pred.col_index,
                    op,
                    literal: pred.literal,
                }
            }
        };

        // The heap can evaluate this typed predicate from one decoded
        // payload column, avoiding the generic closure path's full pair
        // decode on simple `WHERE col cmp literal` deletes.
        let predicate_fn = predicate;
        let wal_sink_arc = self.heap.wal_sink().cloned();
        let wal_sink: Option<&dyn ultrasql_storage::WalSink> = wal_sink_arc.as_deref();
        let scan = DeleteInt32PairScan {
            rel: self.relation,
            block_count: self.block_count,
            snapshot: &self.snapshot,
            oracle: &*self.oracle,
            predicate: predicate_fn,
        };
        let stamp = DeleteInt32PairStamp {
            xid: self.xid,
            command_id: self.command_id,
        };
        let n = if let Some(wal_sink) = wal_sink {
            self.heap.delete_int32_pair_inplace_parallel_wal(
                scan,
                stamp,
                wal_sink,
                self.vm.as_deref(),
            )
        } else {
            self.heap
                .delete_int32_pair_inplace_parallel_no_wal(scan, stamp, self.vm.as_deref())
        }
        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;

        Ok(Some(affected_rows_batch(n, "fused DELETE")?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
