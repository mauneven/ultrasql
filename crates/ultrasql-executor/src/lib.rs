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
//!   Drives [`ultrasql_storage::HeapAccess::scan_visible`] with MVCC
//!   visibility, decodes tuple payloads via [`RowCodec`], and emits
//!   4096-row [`ultrasql_vec::Batch`]es.
//! - [`RowCodec`] — stable v0.5 binary codec for translating between
//!   `Vec<Value>` rows and the byte payloads stored on heap pages.
//! - [`FilterEqI32`] — predicate filter for `col == const_i32` (placeholder).
//! - [`Filter`] — general predicate filter backed by the [`Eval`] interpreter.
//! - [`ValuesScan`] — leaf operator materialising a `VALUES (...)` list.
//! - [`ModifyTable`] — INSERT/UPDATE/DELETE mutations through [`HeapAccess`].
//! - [`Eval`] — scalar expression interpreter; used by [`Filter`] and callers
//!   that need row-level expression evaluation without a full operator.
//! - [`Project`] — column projection.
//! - [`Limit`] — row cap across all output batches.

#![forbid(unsafe_op_in_unsafe_fn)]

pub mod eval;
mod filter;
pub(crate) mod filter_op;
mod limit;
mod mem_table_scan;
pub mod modify;
pub mod physical;
mod project;
mod row_codec;
mod seq_scan;
mod values_scan;

use std::fmt::Debug;

use ultrasql_core::Schema;
use ultrasql_vec::Batch;

pub use eval::{Eval, EvalError};
pub use filter::FilterEqI32;
pub use filter_op::Filter;
pub use limit::Limit;
pub use mem_table_scan::MemTableScan;
pub use modify::{ModifyKind, ModifyTable};
pub use project::Project;
pub use row_codec::{RowCodec, RowCodecError};
pub use seq_scan::{SeqScan, build_batch};
pub use values_scan::ValuesScan;

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
}
