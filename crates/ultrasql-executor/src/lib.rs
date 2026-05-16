//! UltraSQL execution engine.
//!
//! Hybrid push/pull pipeline executor. OLTP point queries use a tuple-at-a-time
//! pull pipeline for minimum latency; OLAP scans use a batched push pipeline
//! with vectorized operators from `ultrasql-vec`. Choice is made at planning
//! time and recorded on the physical plan.
//!
//! Scaffolding status
//! ------------------
//!
//! The execution model in this scaffold is pull-based: a root operator pulls
//! batches from its child by repeatedly calling [`Operator::next_batch`]. The
//! caller of the root operator drains the iterator until it sees `Ok(None)`,
//! which signals end-of-stream. A full push-based variant — where leaves emit
//! batches into a pipeline driver — will be layered on top of the same
//! `Operator` trait surface once the planner produces physical pipelines.
//!
//! Operators implemented here
//! --------------------------
//!
//! - [`MemTableScan`] — leaf operator emitting pre-built in-memory batches.
//!   Used as a unit-test fixture for downstream operator tests; production
//!   scans use [`SeqScan`] instead.
//! - [`SeqScan`] — sequential heap scan backed by the storage subsystem.
//!   Drives `ultrasql_storage::HeapAccess::scan_visible` with MVCC
//!   visibility, decodes tuple payloads via [`RowCodec`], and emits
//!   4096-row [`ultrasql_vec::Batch`]es.
//! - [`RowCodec`] — stable v0.5 binary codec for translating between
//!   `Vec<Value>` rows and the byte payloads stored on heap pages.
//! - [`FilterEqI32`] — predicate filter for `col == const_i32` (placeholder).
//! - [`Filter`] — general predicate filter backed by the [`Eval`] interpreter.
//! - [`ValuesScan`] — leaf operator materialising a `VALUES (...)` list.
//! - [`ModifyTable`] — INSERT/UPDATE/DELETE mutations through `HeapAccess`.
//! - [`Eval`] — scalar expression interpreter; used by [`Filter`] and callers
//!   that need row-level expression evaluation without a full operator.
//! - [`Project`] — column projection.
//! - [`Limit`] — row cap across all output batches.
//! - [`Sort`] — in-memory sort with optional spill.
//! - [`HashJoin`] — hash equi-join (Inner, `LeftOuter`).
//! - [`MergeJoin`] — merge equi-join over sorted inputs (all join types).
//! - [`HashAggregate`] — hash-based GROUP BY / aggregate.
//! - [`SortAggregate`] — streaming aggregate over sorted input.
//! - [`WindowAgg`] — window function evaluation.
//! - [`Unique`] — DISTINCT deduplication (hash or sort mode).
//! - [`SetOp`] — UNION / INTERSECT / EXCEPT.
//! - [`ResultOp`] — single-row constant projection.
//! - [`Materialize`] — pipeline-breaker buffer.
//! - [`LockRows`] — per-row lock callback pass-through.
//! - [`WorkMemBudget`] — per-query work-memory budget.
//! - [`IndexScan`] — B-tree index scan (point + range) over pre-probed payloads.
//! - [`FunctionScan`] — set-returning function scan (`generate_series`).
//! - [`CteScan`] — replay a materialised CTE buffer.

#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    clippy::cast_possible_wrap
)]

pub mod bitmap_heap_scan;
pub mod cte_scan;
pub mod direct_scalar_agg;
pub mod eval;
mod filter;
pub(crate) mod filter_op;
pub mod filter_sum_op;
pub mod function_scan;
pub mod fused_delete;
pub mod fused_update;
mod hash_aggregate;
mod hash_join;
pub mod index_scan;
mod limit;
pub mod lock_rows;
pub mod materialize;
pub mod mem_table_scan;
pub mod merge_join;
pub mod modify;
mod nested_loop_join;
pub mod physical;
mod project;
pub mod project_expr;
pub mod push_pipeline;
pub mod result_op;
mod row_codec;
mod seq_scan;
pub mod set_op;
pub mod sinks;
mod sort;
pub mod sort_aggregate;
pub mod unique;
mod values_scan;
pub mod vec_ops;
pub mod window_agg;
pub mod work_mem;

use std::fmt::Debug;

use ultrasql_core::Schema;
use ultrasql_vec::Batch;

pub use cte_scan::CteScan;
pub use direct_scalar_agg::{DirectScalarAggKind, DirectScalarAggScan};
pub use eval::{Eval, EvalError};
pub use filter::FilterEqI32;
pub use filter_op::Filter;
pub use function_scan::FunctionScan;
pub use hash_aggregate::HashAggregate;
pub use hash_join::HashJoin;
pub use index_scan::IndexScan;
pub use limit::Limit;
pub use lock_rows::LockRows;
pub use materialize::Materialize;
pub use mem_table_scan::MemTableScan;
pub use merge_join::MergeJoin;
pub use modify::{ModifyKind, ModifyTable};
pub use nested_loop_join::{NestedLoopJoin, RightFactory};
pub use project::Project;
pub use project_expr::ProjectExprs;
pub use result_op::ResultOp;
pub use row_codec::{RowCodec, RowCodecError};
pub use seq_scan::{SeqScan, build_batch};
pub use set_op::SetOp;
pub use sort::Sort;
pub use sort_aggregate::SortAggregate;
pub use unique::Unique;
pub use values_scan::ValuesScan;
pub use window_agg::{WindowAgg, WindowFunc};
pub use work_mem::WorkMemBudget;

pub use push_pipeline::{CollectSink, VectorizedPipeline, VectorizedPipelineBuilder};
pub use sinks::{CountSink, SumSink};

/// Errors raised by the executor.
///
/// The executor surface is intentionally narrow: invariant violations in
/// upstream layers (a planner that mis-types a column reference, a kernel
/// that produced a batch wider than its declared schema) surface as
/// [`ExecError::TypeMismatch`] or [`ExecError::Internal`]. Hot-path
/// arithmetic overflow and resource limits will land as additional
/// variants once the corresponding operators arrive; the enum is therefore
/// `#[non_exhaustive]`.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ExecError {
    /// An operator received a column whose runtime type does not match
    /// what the plan claimed. The string names the expectation; the
    /// caller is responsible for surfacing a richer message to the user.
    #[error("type mismatch: {0}")]
    TypeMismatch(String),

    /// A batch was produced that exceeds the configured maximum row
    /// count. The executor caps batches at 4096 rows
    /// (see `ARCHITECTURE.md`); kernels that violate this are buggy.
    #[error("batch exceeds maximum row count: {rows} > {max}")]
    BatchTooLarge {
        /// Reported row count.
        rows: usize,
        /// Configured ceiling.
        max: usize,
    },

    /// An invariant inside the executor was violated. The string literal
    /// names the invariant.
    #[error("internal invariant violation: {0}")]
    Internal(&'static str),

    /// A `ultrasql-core` error escaped to the executor surface — most
    /// often from `Schema::project` when an operator's column indices
    /// are out of range.
    #[error(transparent)]
    Core(#[from] ultrasql_core::Error),

    /// A `ultrasql-vec` batch-construction error escaped to the executor
    /// surface — typically a length-mismatch between projected columns.
    #[error(transparent)]
    Batch(#[from] ultrasql_vec::BatchError),

    /// An operator variant or join type that is not yet implemented.
    ///
    /// The string names the unsupported construct so callers can map it
    /// to a stable SQLSTATE or surface a descriptive error message.
    #[error("unsupported: {0}")]
    Unsupported(&'static str),

    /// The in-flight query was cancelled by a client `CancelRequest`.
    ///
    /// Operators that hold a [`CancelFlag`] check it between batches
    /// and return this variant as soon as the flag is set. The server
    /// maps this to PostgreSQL SQLSTATE `57014` (`query_canceled`).
    #[error("canceling statement due to user request")]
    Cancelled,

    /// A NOT-NULL constraint was violated by an `INSERT` row payload.
    /// The string carries the column name. Maps to PostgreSQL SQLSTATE
    /// `23502` (`not_null_violation`).
    #[error("null value in column \"{0}\" violates not-null constraint")]
    NotNullViolation(String),
}

/// A per-query cancel signal threaded through long-running operators.
///
/// The flag is cloned from the connection's cancel-registry entry when
/// the lowering context is built; every operator that polls loops
/// over [`CancelFlag::is_set`] inside its `next_batch` and returns
/// [`ExecError::Cancelled`] as soon as the flag fires.
///
/// `Clone` is `Arc::clone` — every clone observes the same atomic.
/// The struct lives at the crate root so executor operators and the
/// server crate share one definition without a circular dependency
/// back through `ultrasql-server`.
#[derive(Clone, Debug, Default)]
pub struct CancelFlag(std::sync::Arc<std::sync::atomic::AtomicBool>);

impl CancelFlag {
    /// Construct a fresh, uncancelled flag.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Read the flag. `Relaxed` is sufficient: a slightly stale read
    /// only delays cancellation by one batch, never compromises
    /// correctness.
    #[must_use]
    pub fn is_set(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Set the flag. Idempotent.
    pub fn cancel(&self) {
        self.0.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Physical execution operator.
///
/// The executor is *pull-based* in this scaffold: the root operator is
/// driven by a caller (a query coordinator, an integration test, or, in
/// the future, the network-facing protocol handler) that repeatedly
/// invokes [`Operator::next_batch`] until it observes `Ok(None)`. Each
/// intermediate operator implements `next_batch` by pulling from its
/// child and transforming the result.
///
/// A future push-based driver will own a graph of `Operator`s and call
/// `next_batch` itself, dispatching each emitted batch to the parent
/// pipeline. The `Operator` trait is intentionally the same shape so the
/// two drivers can share leaf and intermediate implementations.
///
/// Contract:
///
/// - `next_batch` returns `Ok(Some(batch))` for every produced batch and
///   `Ok(None)` exactly once, the first time the stream is exhausted. It
///   must not return `Ok(Some(batch))` after returning `Ok(None)`.
/// - Returned batches must conform to the operator's [`schema`]: same
///   width, same column data types, same column order.
/// - Operators may return an empty batch (zero rows) — the caller treats
///   it as "no rows in this batch, keep pulling," not as end-of-stream.
/// - Errors are terminal: once `next_batch` returns `Err`, callers must
///   stop pulling. The operator's behavior after a previous error is
///   unspecified.
/// - Operators are `Send` so a future scheduler can move them between
///   worker threads without bounding the per-operator state to a single
///   reactor; they are *not* required to be `Sync`.
///
/// [`schema`]: Operator::schema
pub trait Operator: Send + Debug {
    /// Next batch of output rows, or `None` at EOF.
    ///
    /// See the trait-level documentation for the full contract.
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError>;

    /// Schema of every batch this operator produces.
    ///
    /// The schema does not change for the lifetime of the operator.
    fn schema(&self) -> &Schema;

    /// Best-effort upper bound on the total number of rows this
    /// operator will emit across all `next_batch` calls. Used by
    /// downstream wire-encoders to pre-reserve their output buffer
    /// and skip mid-loop reallocations.
    ///
    /// The default returns `None` — callers must tolerate the absence
    /// of a hint and fall back to geometric growth. Operators that
    /// know their cardinality statically (column-cache replay,
    /// materialised CTE replay, `LIMIT n`) override this method.
    ///
    /// The hint is advisory. Returning a value that turns out to be
    /// wrong (because, e.g., a child operator hides rows under a
    /// predicate the parent does not see) is not a contract
    /// violation — the encoder grows past the reservation as usual.
    fn estimated_row_count(&self) -> Option<usize> {
        None
    }
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    use crate::{FilterEqI32, Limit, MemTableScan, Operator, Project};

    /// Build the canonical `(id i32, val i64)` schema used across the
    /// executor tests.
    fn schema_id_val() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("val", DataType::Int64),
        ])
        .expect("schema is well-formed")
    }

    /// Build a batch with the given `(id, val)` rows.
    fn batch(rows: &[(i32, i64)]) -> Batch {
        let ids: Vec<i32> = rows.iter().map(|(i, _)| *i).collect();
        let vals: Vec<i64> = rows.iter().map(|(_, v)| *v).collect();
        Batch::new([
            Column::Int32(NumericColumn::from_data(ids)),
            Column::Int64(NumericColumn::from_data(vals)),
        ])
        .expect("test batch is well-formed")
    }

    /// Collect every i64 value emitted by an operator that produces a
    /// single i64 column.
    fn drain_i64_col0(op: &mut dyn Operator) -> Vec<i64> {
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().expect("operator must not error") {
            match &b.columns()[0] {
                Column::Int64(c) => out.extend_from_slice(c.data()),
                other => panic!("expected Int64 column, got {other:?}"),
            }
        }
        out
    }

    #[test]
    fn pipeline_scan_filter_project_limit() {
        let schema = schema_id_val();
        let b1 = batch(&[(1, 10), (7, 20), (3, 30), (7, 40)]);
        let b2 = batch(&[(7, 50), (7, 60), (2, 70), (7, 80)]);
        let scan = MemTableScan::new(schema, vec![b1, b2]);
        let filter = FilterEqI32::new(Box::new(scan), 0, 7).expect("filter constructs");
        let project = Project::new(Box::new(filter), vec![1]).expect("project constructs");
        let mut limit = Limit::new(Box::new(project), 2);

        let values = drain_i64_col0(&mut limit);
        assert_eq!(
            values,
            vec![20, 40],
            "limit caps output to first two matches"
        );
    }

    #[test]
    fn pipeline_emits_eof_repeatedly_after_drain() {
        let schema = schema_id_val();
        let scan = MemTableScan::new(schema, vec![batch(&[(7, 1)])]);
        let filter = FilterEqI32::new(Box::new(scan), 0, 7).unwrap();
        let project = Project::new(Box::new(filter), vec![1]).unwrap();
        let mut limit = Limit::new(Box::new(project), 10);
        let drained = drain_i64_col0(&mut limit);
        assert_eq!(drained, vec![1]);
        // Past EOF, subsequent calls remain `None` — operators must be
        // idempotent at end-of-stream.
        let again = limit.next_batch().expect("operator must not error");
        assert!(again.is_none());
    }

    #[test]
    fn pipeline_preserves_projected_schema() {
        let schema = schema_id_val();
        let scan = MemTableScan::new(schema, vec![batch(&[(7, 1)])]);
        let filter = FilterEqI32::new(Box::new(scan), 0, 7).unwrap();
        let project = Project::new(Box::new(filter), vec![1]).unwrap();
        assert_eq!(project.schema().len(), 1);
        assert_eq!(project.schema().field_at(0).name, "val");
        assert_eq!(project.schema().field_at(0).data_type, DataType::Int64);
    }

    /// End-to-end pipeline: `MemTableScan` → `Sort` → `Unique` → `LockRows`.
    ///
    /// Uses a single-column `(id i32)` schema so that `Unique` in Sort mode
    /// deduplicates on the same key used for sorting. Verifies that the new
    /// v0.5 operators interoperate correctly: sort, deduplicate, then pass
    /// each unique row through a lock callback.
    #[test]
    fn pipeline_sort_unique_lock_rows_end_to_end() {
        use std::sync::{Arc, Mutex};
        use ultrasql_planner::{ScalarExpr, SortKey};

        use crate::{
            Sort,
            lock_rows::LockRows,
            unique::{Unique, UniqueMode},
        };

        // Single-column schema so Unique compares on the one column.
        let schema = Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok");

        // Three 7s, one 1, one 3.
        let rows: Vec<i32> = vec![7, 1, 3, 7, 7];
        let input_batch = {
            use ultrasql_vec::column::NumericColumn;
            Batch::new([Column::Int32(NumericColumn::from_data(rows))]).expect("batch ok")
        };
        let scan = MemTableScan::new(schema.clone(), vec![input_batch]);

        let sort_keys = vec![SortKey {
            expr: ScalarExpr::Column {
                name: "id".into(),
                index: 0,
                data_type: DataType::Int32,
            },
            asc: true,
            nulls_first: false,
        }];
        let sort = Sort::new(Box::new(scan), sort_keys, schema);
        let unique = Unique::new(Box::new(sort), UniqueMode::Sort);

        // Record row count seen by the lock callback.
        let locked_rows: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
        let counter = Arc::clone(&locked_rows);
        let lock_rows = LockRows::new(
            Box::new(unique),
            Box::new(move |_batch, _row_idx| {
                *counter.lock().expect("mutex ok") += 1;
                Ok(())
            }),
        );
        let mut root: Box<dyn Operator> = Box::new(lock_rows);

        // Collect all output rows.
        let mut ids: Vec<i32> = Vec::new();
        while let Some(b) = root.next_batch().expect("pipeline must not error") {
            match &b.columns()[0] {
                Column::Int32(c) => ids.extend_from_slice(c.data()),
                other => panic!("unexpected column: {other:?}"),
            }
        }

        // After sort+unique: 1, 3, 7 (deduplicated, sorted).
        assert_eq!(
            ids,
            vec![1, 3, 7],
            "sort+unique output must be deduplicated and ordered"
        );

        // LockRows must have called the callback once per output row.
        let locked = *locked_rows.lock().expect("mutex ok");
        assert_eq!(locked, 3, "lock callback must fire once per unique row");
    }
}
