//! [`ModifyTable`] operator: applies INSERT / UPDATE / DELETE to a relation
//! through [`HeapAccess`].
//!
//! Drains its input operator (the rows to insert, or scan-output of rows
//! to update/delete) and reports the row count that was modified.
//!
//! # INSERT path
//!
//! The `Insert` kind drains the child operator. For each decoded row it
//! calls [`HeapAccess::insert`] with the provided MVCC metadata, then
//! tracks the count. After EOF it emits a single 1-row batch whose only
//! column is `Int64(affected_rows)` and then returns `Ok(None)`.
//!
//! # UPDATE / DELETE paths
//!
//! `Update` and `Delete` are implemented as stand-alone operator code
//! but are only reachable through direct construction (not through
//! `build_operator`) until the physical-plan lowering gains datasource
//! handles in wave 5+. Both accept a child that emits rows prepended
//! with `(tid_block: Int32, tid_slot: Int32, ...)` columns.
//!
//! # Send bound
//!
//! `ModifyTable<L>` is `Send` because [`HeapAccess<L>`] is `Send + Sync`
//! whenever `L: PageLoader + Send + Sync` and the WAL sink (when present)
//! implements `Send + Sync`.
//!
//! # Module layout
//!
//! This file declares the operator's public data types; the behavior is
//! split across sibling submodules:
//!
//! - [`builder`] — the `new` constructor and `with_*` configuration methods.
//! - [`operator`] — the [`Operator`] / `next_batch` implementation.
//! - [`methods`] — per-row edit computation and constraint checks.
//! - [`index_maintainer`] — the B-tree / vector index maintainer impls.
//! - [`types`] — the small descriptor-type impls.
//! - [`helpers`] — free helper functions shared across the above.

use std::sync::Arc;

use ultrasql_core::{CommandId, RelationId, Schema, TupleId, Value, Xid};
use ultrasql_planner::ScalarExpr;
use ultrasql_storage::PageLoader;
use ultrasql_storage::access_method::{PageBackedHnswIndex, PageBackedIvfFlatIndex};
use ultrasql_storage::btree::BTree;
use ultrasql_storage::heap::{HeapAccess, UpdatePayload};
use ultrasql_storage::sequence::Sequence;
use ultrasql_storage::vm::VisibilityMap;
use ultrasql_storage::wal_sink::WalSink;

use crate::eval::Eval;
use crate::row_codec::{RowCodec, RowCodecError};
use crate::{ExecError, Operator};

mod builder;
pub(crate) mod helpers;
mod index_maintainer;
mod index_ops;
mod methods;
mod operator;
mod types;

#[cfg(test)]
mod tests;

/// Columnar UPDATE fast-path descriptor for the
/// `UPDATE t SET col_i = col_i + literal` shape over an `(Int32, Int32)`
/// relation schema.
///
/// Detected at [`ModifyTable::new`] from the bound assignment list.
/// When `Some(_)`, [`ModifyTable::next_batch`]'s UPDATE arm bypasses
/// `batch_to_rows` + per-row `Eval` + per-row `RowCodec::encode`
/// entirely: the new column is computed by a single
/// `i32::wrapping_add(literal)` per lane and each tuple's 9-byte
/// payload is built inline from the (`id`, `new_val`) column pair.
#[derive(Clone, Copy, Debug)]
pub(crate) struct UpdateFastPathInt32Pair {
    /// 0-based index of the target column in the relation schema
    /// (also the column index in the eval expression's row). For
    /// `(id, val)` UPDATEs of `val`, this is `1`.
    pub(crate) target_col_in_relation: usize,
    /// Constant added to the target column on every row. For
    /// `val = val + 1` this is `1`; for `val = val - 5` this is `-5`.
    pub(crate) delta: i32,
}

/// Shared callback type used by insert-side index maintenance.
///
/// The callback receives the decoded target-table row and returns the
/// `i64` B-tree key for that index, or `None` when the key is SQL NULL
/// and should be omitted from the index.
pub type InsertIndexEncoder = Arc<dyn Fn(&[Value]) -> Result<Option<i64>, ExecError> + Send + Sync>;

/// Shared callback type used by vector-index maintenance.
///
/// The callback receives the decoded target-table row and returns the dense
/// `f32` vector for that index, or `None` when the key is SQL NULL and should
/// be omitted from the graph.
pub type VectorIndexEncoder =
    Arc<dyn Fn(&[Value]) -> Result<Option<Vec<f32>>, ExecError> + Send + Sync>;

/// Row-level DML constraint callback.
pub type RowConstraintCheck = Arc<dyn Fn(&[Value]) -> Result<(), ExecError> + Send + Sync>;

/// Row-level UPDATE constraint callback over `(old_row, new_row)`.
pub type RowUpdateConstraintCheck =
    Arc<dyn Fn(&[Value], &[Value]) -> Result<(), ExecError> + Send + Sync>;

/// Observer called after a sequence-backed default generates a value.
pub type SequenceNextvalObserver = Arc<dyn Fn(&str, i64) + Send + Sync>;

/// Runtime descriptor for maintaining one B-tree during
/// [`ModifyKind::Insert`].
///
/// `ModifyTable` owns the opened tree handle and calls `encode` for
/// each decoded insert row before the heap write. Duplicate keys are
/// detected before `HeapAccess::insert_batch`, so a rejected batch does
/// not leak rows into the heap.
pub struct InsertIndexMaintainer<L: PageLoader> {
    pub(crate) name: String,
    pub(crate) tree: BTree<L>,
    pub(crate) encode: InsertIndexEncoder,
    pub(crate) unique: bool,
    pub(crate) key_columns: Vec<usize>,
    pub(crate) brin: Option<Arc<ultrasql_storage::access_method::BrinIndex>>,
}

/// Runtime descriptor for maintaining one HNSW vector index during DML.
pub struct VectorIndexMaintainer {
    pub(crate) name: String,
    pub(crate) runtime: VectorIndexRuntime,
    pub(crate) encode: VectorIndexEncoder,
    pub(crate) xid: Xid,
    pub(crate) wal: Option<Arc<dyn WalSink>>,
}

pub(crate) enum VectorIndexRuntime {
    Hnsw(Arc<PageBackedHnswIndex>),
    IvfFlat(Arc<PageBackedIvfFlatIndex>),
}

pub(crate) struct ComputedUpdate {
    pub(crate) tid: TupleId,
    pub(crate) payload: UpdatePayload,
    pub(crate) index_change: Option<UpdateIndexChange>,
    pub(crate) vector_index_change: Option<VectorUpdateIndexChange>,
    pub(crate) returning_row: Option<Vec<Value>>,
}

pub(crate) struct UpdateIndexChange {
    pub(crate) old_tid: TupleId,
    pub(crate) old_keys: Vec<Option<i64>>,
    pub(crate) new_keys: Vec<Option<i64>>,
}

pub(crate) struct DeleteIndexChange {
    pub(crate) tid: TupleId,
    pub(crate) keys: Vec<Option<i64>>,
}

pub(crate) struct VectorUpdateIndexChange {
    pub(crate) old_tid: TupleId,
    pub(crate) old_keys: Vec<Option<Vec<f32>>>,
    pub(crate) new_keys: Vec<Option<Vec<f32>>>,
}

pub(crate) struct VectorDeleteIndexChange {
    pub(crate) tid: TupleId,
    pub(crate) keys: Vec<Option<Vec<f32>>>,
}

pub(crate) struct ComputedDelete {
    pub(crate) tid: TupleId,
    pub(crate) index_change: Option<DeleteIndexChange>,
    pub(crate) vector_index_change: Option<VectorDeleteIndexChange>,
}

pub(crate) struct PreparedInsert {
    pub(crate) payload: Vec<u8>,
    pub(crate) index_keys: Vec<Option<i64>>,
    pub(crate) vector_index_keys: Vec<Option<Vec<f32>>>,
}

pub(crate) type DeleteExtraction = (
    Vec<TupleId>,
    Vec<DeleteIndexChange>,
    Vec<VectorDeleteIndexChange>,
    Vec<Vec<Value>>,
);

#[derive(Clone, Debug)]
pub(crate) struct CheckEvaluator {
    pub(crate) name: String,
    pub(crate) evaluator: Eval,
}

/// Runtime descriptor for a sequence-backed column default.
#[derive(Clone)]
pub struct SequenceDefault {
    pub(crate) name: String,
    pub(crate) sequence: Arc<Sequence>,
    pub(crate) on_nextval: Option<SequenceNextvalObserver>,
    pub(crate) wal: Option<Arc<dyn WalSink>>,
    pub(crate) xid: Xid,
    pub(crate) seqrelid: RelationId,
}

// ---------------------------------------------------------------------------
// ModifyKind
// ---------------------------------------------------------------------------

/// The mutation kind for a [`ModifyTable`] operator.
#[derive(Debug)]
pub enum ModifyKind {
    /// Append all input rows to the target relation.
    Insert,

    /// Replace targeted rows in the relation.
    ///
    /// `assignments` is a list of `(target_column_index, value_expr)` pairs
    /// where the expression scope is the current row's values. The child
    /// operator must emit rows in the shape `[tid_block: Int32, tid_slot:
    /// Int32, original_col0, original_col1, ...]`.
    Update {
        /// Ordered list of `(column_index_in_relation_schema, value_expr)` pairs.
        assignments: Vec<(usize, ScalarExpr)>,
    },

    /// Mark targeted rows as dead.
    ///
    /// The child operator must emit rows in the shape
    /// `[tid_block: Int32, tid_slot: Int32, ...rest]`.
    Delete,

    /// Apply tagged `MERGE INTO` rows.
    ///
    /// The child operator emits rows in the shape
    /// `[merge_clause: Int32, tid_block: Int32, tid_slot: Int32,
    /// target_col0, ..., source_col0, ...]`. `merge_clause` indexes
    /// `clauses`; update/delete actions use the target TID, while insert
    /// actions ignore the fake TID and evaluate values against
    /// `[target..., source...]`.
    Merge {
        /// Ordered runtime branch actions.
        clauses: Vec<MergeClause>,
    },
}

/// Runtime action for a selected `MERGE INTO` branch.
#[derive(Clone, Debug)]
pub enum MergeAction {
    /// `WHEN MATCHED THEN UPDATE SET ...`.
    Update {
        /// Assignment evaluators applied against `[target..., source...]`.
        assignments: Vec<(usize, Eval)>,
    },
    /// `WHEN MATCHED THEN DELETE`.
    Delete,
    /// `WHEN NOT MATCHED THEN INSERT ...`.
    Insert {
        /// Target column map for the insert row.
        columns: Vec<usize>,
        /// Value evaluators applied against `[target..., source...]`.
        values: Vec<Eval>,
    },
}

/// Runtime metadata for one selected `MERGE INTO` branch.
#[derive(Clone, Debug)]
pub struct MergeClause {
    /// Branch action.
    pub action: MergeAction,
}

/// Runtime action for `INSERT ... ON CONFLICT`.
#[derive(Clone, Debug)]
pub enum InsertConflictAction {
    /// `ON CONFLICT [target] DO NOTHING`.
    DoNothing {
        /// Optional conflict target column set.
        target: Option<Vec<usize>>,
    },
    /// `ON CONFLICT target DO UPDATE SET ...`.
    DoUpdate {
        /// Conflict target column set.
        target: Vec<usize>,
        /// Assignment evaluators applied to the existing row.
        assignments: Vec<(usize, Eval)>,
        /// Optional predicate evaluated against the existing row.
        predicate: Option<Eval>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InsertConflict {
    Existing(TupleId),
    InBatch,
}

/// MVCC metadata stamped by a [`ModifyTable`] mutation.
///
/// INSERT uses `insert_xmin` / `insert_command_id` for new tuple
/// headers. UPDATE and DELETE use `delete_xmax` / `delete_cmax` for
/// old tuple versions marked dead.
#[derive(Clone, Copy, Debug)]
pub struct ModifyTableStamps {
    pub(crate) insert_xmin: Xid,
    pub(crate) insert_command_id: CommandId,
    pub(crate) delete_xmax: Xid,
    pub(crate) delete_cmax: CommandId,
}

// ---------------------------------------------------------------------------
// ModifyTable
// ---------------------------------------------------------------------------

/// Pull-based table mutation operator.
///
/// On each `next_batch` call, drains all remaining rows from `child` and
/// performs the configured mutation. After the child signals EOF, emits
/// exactly one batch containing the `affected_rows` count (an `Int64`
/// column) and then returns `Ok(None)` on all subsequent calls.
///
/// # UPDATE / DELETE batching
///
/// UPDATE and DELETE call into the heap's bulk-write surface
/// ([`HeapAccess::update_many`] / [`HeapAccess::delete_many`]) once
/// per child batch instead of once per row. That groups TIDs by
/// page so a 10 000-row UPDATE over 50 pages takes ~50 page guards
/// instead of 10 000. The per-batch cost still includes
/// expression evaluation (the `assignments` evaluators are built
/// once at construction time and reused) and row encoding.
pub struct ModifyTable<L: PageLoader> {
    pub(crate) heap: Arc<HeapAccess<L>>,
    pub(crate) relation: RelationId,
    /// Output schema: either `[("affected_rows", Int64)]` for plain
    /// DML or the bound `RETURNING` schema when present.
    pub(crate) schema: Schema,
    /// Row codec for INSERT and UPDATE payload encoding. Carries a
    /// cached `fixed_width_lower_bound` so per-row `encode` calls do
    /// not realloc on the first push (see [`RowCodec::new`]).
    pub(crate) codec: RowCodec,
    pub(crate) kind: ModifyKind,
    /// Pre-built per-assignment evaluators for UPDATE. `kind ==
    /// Update` populates this once at construction so the per-row
    /// loop in `next_batch` does not pay the `ScalarExpr::clone()`
    /// and evaluator-allocation cost on every iteration.
    ///
    /// Empty for INSERT and DELETE.
    pub(crate) update_evaluators: Vec<(usize, Eval)>,
    /// When true, UPDATE child rows may carry extra columns after the
    /// target row image. The first `relation_schema.len()` values remain
    /// the old target row; assignment expressions evaluate against the
    /// full row. Used by MERGE, whose update expressions bind against
    /// `[target..., source...]`.
    pub(crate) update_extra_eval_columns: bool,
    /// Cached descriptor for the columnar UPDATE fast path. `Some`
    /// when (a) the kind is UPDATE, (b) the relation schema is
    /// exactly `(Int32, Int32)`, (c) there is one assignment whose
    /// expression matches `col + lit` / `col - lit` / `lit + col`
    /// with an `Int32` literal.
    pub(crate) update_fast_path: Option<UpdateFastPathInt32Pair>,
    pub(crate) insert_xmin: Xid,
    pub(crate) insert_command_id: CommandId,
    pub(crate) delete_xmax: Xid,
    pub(crate) delete_cmax: CommandId,
    pub(crate) wal: Option<Arc<dyn WalSink>>,
    pub(crate) vm: Option<Arc<VisibilityMap>>,
    pub(crate) insert_indexes: Vec<InsertIndexMaintainer<L>>,
    pub(crate) update_indexes: Vec<InsertIndexMaintainer<L>>,
    pub(crate) delete_indexes: Vec<InsertIndexMaintainer<L>>,
    pub(crate) insert_vector_indexes: Vec<VectorIndexMaintainer>,
    pub(crate) update_vector_indexes: Vec<VectorIndexMaintainer>,
    pub(crate) delete_vector_indexes: Vec<VectorIndexMaintainer>,
    pub(crate) insert_conflict_action: Option<InsertConflictAction>,
    pub(crate) insert_column_map: Option<Vec<usize>>,
    pub(crate) column_defaults: Vec<Option<Eval>>,
    pub(crate) sequence_defaults: Vec<Option<SequenceDefault>>,
    pub(crate) identity_always: Vec<bool>,
    pub(crate) generated_stored: Vec<Option<Eval>>,
    pub(crate) check_constraints: Vec<CheckEvaluator>,
    pub(crate) foreign_key_checks: Vec<RowConstraintCheck>,
    pub(crate) exclusion_checks: Vec<RowConstraintCheck>,
    pub(crate) exclusion_update_checks: Vec<RowUpdateConstraintCheck>,
    pub(crate) referenced_by_delete_checks: Vec<RowConstraintCheck>,
    pub(crate) referenced_by_update_checks: Vec<RowUpdateConstraintCheck>,
    pub(crate) returning_evaluators: Vec<Eval>,
    pub(crate) child: Box<dyn Operator>,
    pub(crate) done: bool,
    pub(crate) affected: i64,
}

pub(crate) struct ExpandedInsertRow {
    pub(crate) values: Vec<Value>,
    pub(crate) omitted: Vec<bool>,
}
