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

use std::collections::HashSet;
use std::sync::Arc;

use ultrasql_core::{CommandId, DataType, Field, RelationId, Schema, TupleId, Value, Xid};
use ultrasql_mvcc::{InfoMask, TupleHeader};
use ultrasql_planner::{BinaryOp, ScalarExpr};
use ultrasql_storage::PageLoader;
use ultrasql_storage::access_method::{
    AccessMethod, BrinIndex, PageBackedHnswIndex, PageBackedIvfFlatIndex,
};
use ultrasql_storage::btree::{BTree, BTreeError};
use ultrasql_storage::heap::{DeleteOptions, HeapAccess, UpdateOptions, UpdatePayload};
use ultrasql_storage::sequence::Sequence;
use ultrasql_storage::vm::VisibilityMap;
use ultrasql_storage::wal_sink::WalSink;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};

use crate::eval::Eval;
use crate::filter_op::batch_to_rows;
use crate::row_codec::{RowCodec, RowCodecError};
use crate::seq_scan::build_batch;
use crate::{ExecError, Operator, eval_error_to_exec_error};

/// Enforce schema-level NOT-NULL constraints over a decoded `INSERT`
/// row before it is encoded and handed to the heap.
///
/// Surfaces [`ExecError::NotNullViolation`] on the first non-nullable
/// column carrying [`Value::Null`]; the caller maps this onto
/// PostgreSQL SQLSTATE `23502`.
fn check_not_null_violations(row: &[Value], schema: &Schema) -> Result<(), ExecError> {
    for (col, field) in row.iter().zip(schema.fields().iter()) {
        if !field.nullable && matches!(col, Value::Null) {
            return Err(ExecError::NotNullViolation(field.name.clone()));
        }
    }
    Ok(())
}

fn row_codec_error_to_exec(err: RowCodecError) -> ExecError {
    match err {
        RowCodecError::StringDataRightTruncation { detail, .. } => {
            ExecError::StringDataRightTruncation(detail)
        }
        RowCodecError::NumericFieldOverflow { detail, .. } => {
            ExecError::NumericFieldOverflow(detail)
        }
        other => ExecError::TypeMismatch(other.to_string()),
    }
}

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
struct UpdateFastPathInt32Pair {
    /// 0-based index of the target column in the relation schema
    /// (also the column index in the eval expression's row). For
    /// `(id, val)` UPDATEs of `val`, this is `1`.
    target_col_in_relation: usize,
    /// Constant added to the target column on every row. For
    /// `val = val + 1` this is `1`; for `val = val - 5` this is `-5`.
    delta: i32,
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
    name: String,
    tree: BTree<L>,
    encode: InsertIndexEncoder,
    unique: bool,
    key_columns: Vec<usize>,
    brin: Option<Arc<BrinIndex>>,
}

impl<L: PageLoader> std::fmt::Debug for InsertIndexMaintainer<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InsertIndexMaintainer")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl<L: PageLoader> InsertIndexMaintainer<L> {
    /// Construct a maintainer for one already-created B-tree index.
    #[must_use]
    pub fn new<N: Into<String>>(
        name: N,
        tree: BTree<L>,
        encode: InsertIndexEncoder,
        unique: bool,
    ) -> Self {
        Self {
            name: name.into(),
            tree,
            encode,
            unique,
            key_columns: Vec::new(),
            brin: None,
        }
    }

    /// Attach target-table column indices covered by this index key.
    #[must_use]
    pub fn with_key_columns(mut self, columns: Vec<usize>) -> Self {
        self.key_columns = columns;
        self
    }

    /// Attach the in-memory BRIN summary maintained beside this index.
    #[must_use]
    pub fn with_brin(mut self, brin: Option<Arc<BrinIndex>>) -> Self {
        self.brin = brin;
        self
    }

    fn encode_key(&self, row: &[Value]) -> Result<Option<i64>, ExecError> {
        (self.encode)(row)
    }

    fn contains_key(&self, key: i64) -> Result<bool, ExecError> {
        self.lookup_tid(key).map(|tid| tid.is_some())
    }

    fn lookup_tid(&self, key: i64) -> Result<Option<TupleId>, ExecError> {
        self.tree
            .lookup::<i64>(key)
            .map_err(|e| ExecError::TypeMismatch(format!("index lookup {}: {e}", self.name)))
    }

    const fn is_unique(&self) -> bool {
        self.unique
    }

    fn insert_key(
        &mut self,
        key: i64,
        tid: TupleId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<(), ExecError> {
        let result = if self.unique {
            self.tree.insert(key, tid, xid, wal)
        } else {
            self.tree.insert_non_unique(key, tid, xid, wal)
        };
        result.map_err(|e| match e {
            BTreeError::DuplicateKey => ExecError::UniqueViolation(self.name.clone()),
            other => ExecError::TypeMismatch(format!("index insert {}: {other}", self.name)),
        })?;
        if let Some(brin) = &self.brin {
            let brin_key = BrinIndex::encode_i64_key(key);
            brin.insert(&brin_key, tid).map_err(|e| {
                ExecError::TypeMismatch(format!("brin summary insert {}: {e}", self.name))
            })?;
        }
        Ok(())
    }

    fn delete_key(
        &mut self,
        key: i64,
        tid: TupleId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<bool, ExecError> {
        self.tree
            .delete_logged::<i64>(key, tid, xid, wal)
            .map_err(|e| ExecError::TypeMismatch(format!("index delete {}: {e}", self.name)))
    }
}

/// Runtime descriptor for maintaining one HNSW vector index during DML.
pub struct VectorIndexMaintainer {
    name: String,
    runtime: VectorIndexRuntime,
    encode: VectorIndexEncoder,
    xid: Xid,
    wal: Option<Arc<dyn WalSink>>,
}

enum VectorIndexRuntime {
    Hnsw(Arc<PageBackedHnswIndex>),
    IvfFlat(Arc<PageBackedIvfFlatIndex>),
}

impl std::fmt::Debug for VectorIndexRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hnsw(_) => f.write_str("Hnsw"),
            Self::IvfFlat(_) => f.write_str("IvfFlat"),
        }
    }
}

impl std::fmt::Debug for VectorIndexMaintainer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorIndexMaintainer")
            .field("name", &self.name)
            .field("runtime", &self.runtime)
            .finish_non_exhaustive()
    }
}

impl VectorIndexMaintainer {
    /// Construct a maintainer for one runtime HNSW graph.
    #[must_use]
    pub fn new_hnsw<N: Into<String>>(
        name: N,
        hnsw: Arc<PageBackedHnswIndex>,
        encode: VectorIndexEncoder,
        xid: Xid,
        wal: Option<Arc<dyn WalSink>>,
    ) -> Self {
        Self {
            name: name.into(),
            runtime: VectorIndexRuntime::Hnsw(hnsw),
            encode,
            xid,
            wal,
        }
    }

    /// Construct a maintainer for one runtime IVFFlat index.
    #[must_use]
    pub fn new_ivfflat<N: Into<String>>(
        name: N,
        ivfflat: Arc<PageBackedIvfFlatIndex>,
        encode: VectorIndexEncoder,
        xid: Xid,
        wal: Option<Arc<dyn WalSink>>,
    ) -> Self {
        Self {
            name: name.into(),
            runtime: VectorIndexRuntime::IvfFlat(ivfflat),
            encode,
            xid,
            wal,
        }
    }

    fn encode_key(&self, row: &[Value]) -> Result<Option<Vec<f32>>, ExecError> {
        (self.encode)(row)
    }

    fn insert_vector(&self, vector: &[f32], tid: TupleId) -> Result<(), ExecError> {
        match &self.runtime {
            VectorIndexRuntime::Hnsw(hnsw) => hnsw
                .insert_vector_logged(vector, tid, self.xid, self.wal.as_deref())
                .map_err(|e| ExecError::TypeMismatch(format!("hnsw insert {}: {e}", self.name))),
            VectorIndexRuntime::IvfFlat(ivfflat) => ivfflat
                .insert_vector_logged(vector, tid, self.xid, self.wal.as_deref())
                .map_err(|e| ExecError::TypeMismatch(format!("ivfflat insert {}: {e}", self.name))),
        }
    }

    fn delete_tid(&self, tid: TupleId) -> Result<(), ExecError> {
        match &self.runtime {
            VectorIndexRuntime::Hnsw(hnsw) => hnsw
                .mark_deleted_logged(tid, self.xid, self.wal.as_deref())
                .map_err(|e| ExecError::TypeMismatch(format!("hnsw delete {}: {e}", self.name))),
            VectorIndexRuntime::IvfFlat(ivfflat) => ivfflat
                .mark_deleted_logged(tid, self.xid, self.wal.as_deref())
                .map_err(|e| ExecError::TypeMismatch(format!("ivfflat delete {}: {e}", self.name))),
        }
    }
}

struct ComputedUpdate {
    tid: TupleId,
    payload: UpdatePayload,
    index_change: Option<UpdateIndexChange>,
    vector_index_change: Option<VectorUpdateIndexChange>,
    returning_row: Option<Vec<Value>>,
}

struct UpdateIndexChange {
    old_tid: TupleId,
    old_keys: Vec<Option<i64>>,
    new_keys: Vec<Option<i64>>,
}

struct DeleteIndexChange {
    tid: TupleId,
    keys: Vec<Option<i64>>,
}

struct VectorUpdateIndexChange {
    old_tid: TupleId,
    old_keys: Vec<Option<Vec<f32>>>,
    new_keys: Vec<Option<Vec<f32>>>,
}

struct VectorDeleteIndexChange {
    tid: TupleId,
    keys: Vec<Option<Vec<f32>>>,
}

type DeleteExtraction = (
    Vec<TupleId>,
    Vec<DeleteIndexChange>,
    Vec<VectorDeleteIndexChange>,
    Vec<Vec<Value>>,
);

#[derive(Clone, Debug)]
struct CheckEvaluator {
    name: String,
    evaluator: Eval,
}

/// Runtime descriptor for a sequence-backed column default.
#[derive(Clone)]
pub struct SequenceDefault {
    name: String,
    sequence: Arc<Sequence>,
    on_nextval: Option<SequenceNextvalObserver>,
    wal: Option<Arc<dyn WalSink>>,
    xid: Xid,
    seqrelid: RelationId,
}

impl std::fmt::Debug for SequenceDefault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SequenceDefault")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl SequenceDefault {
    /// Build a sequence default that advances `sequence` when the
    /// corresponding INSERT column is omitted.
    #[must_use]
    pub fn new<N: Into<String>>(name: N, sequence: Arc<Sequence>) -> Self {
        Self {
            name: name.into(),
            sequence,
            on_nextval: None,
            wal: None,
            xid: Xid::INVALID,
            seqrelid: RelationId::INVALID,
        }
    }

    /// Attach a session-local observer called with every generated value.
    #[must_use]
    pub fn with_observer(mut self, on_nextval: SequenceNextvalObserver) -> Self {
        self.on_nextval = Some(on_nextval);
        self
    }

    /// Attach WAL context used when this default advances the sequence.
    #[must_use]
    pub fn with_wal(
        mut self,
        wal: Option<Arc<dyn WalSink>>,
        xid: Xid,
        seqrelid: RelationId,
    ) -> Self {
        self.wal = wal;
        self.xid = xid;
        self.seqrelid = seqrelid;
        self
    }
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
enum InsertConflict {
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
    insert_xmin: Xid,
    insert_command_id: CommandId,
    delete_xmax: Xid,
    delete_cmax: CommandId,
}

impl ModifyTableStamps {
    /// Create MVCC stamp metadata for table mutation.
    #[must_use]
    pub fn new(
        insert_xmin: Xid,
        insert_command_id: CommandId,
        delete_xmax: Xid,
        delete_cmax: CommandId,
    ) -> Self {
        Self {
            insert_xmin,
            insert_command_id,
            delete_xmax,
            delete_cmax,
        }
    }
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
    heap: Arc<HeapAccess<L>>,
    relation: RelationId,
    /// Output schema: either `[("affected_rows", Int64)]` for plain
    /// DML or the bound `RETURNING` schema when present.
    schema: Schema,
    /// Row codec for INSERT and UPDATE payload encoding. Carries a
    /// cached `fixed_width_lower_bound` so per-row `encode` calls do
    /// not realloc on the first push (see [`RowCodec::new`]).
    codec: RowCodec,
    kind: ModifyKind,
    /// Pre-built per-assignment evaluators for UPDATE. `kind ==
    /// Update` populates this once at construction so the per-row
    /// loop in `next_batch` does not pay the `ScalarExpr::clone()`
    /// and evaluator-allocation cost on every iteration.
    ///
    /// Empty for INSERT and DELETE.
    update_evaluators: Vec<(usize, Eval)>,
    /// Cached descriptor for the columnar UPDATE fast path. `Some`
    /// when (a) the kind is UPDATE, (b) the relation schema is
    /// exactly `(Int32, Int32)`, (c) there is one assignment whose
    /// expression matches `col + lit` / `col - lit` / `lit + col`
    /// with an `Int32` literal.
    update_fast_path: Option<UpdateFastPathInt32Pair>,
    insert_xmin: Xid,
    insert_command_id: CommandId,
    delete_xmax: Xid,
    delete_cmax: CommandId,
    wal: Option<Arc<dyn WalSink>>,
    vm: Option<Arc<VisibilityMap>>,
    insert_indexes: Vec<InsertIndexMaintainer<L>>,
    update_indexes: Vec<InsertIndexMaintainer<L>>,
    delete_indexes: Vec<InsertIndexMaintainer<L>>,
    insert_vector_indexes: Vec<VectorIndexMaintainer>,
    update_vector_indexes: Vec<VectorIndexMaintainer>,
    delete_vector_indexes: Vec<VectorIndexMaintainer>,
    insert_conflict_action: Option<InsertConflictAction>,
    insert_column_map: Option<Vec<usize>>,
    column_defaults: Vec<Option<Eval>>,
    sequence_defaults: Vec<Option<SequenceDefault>>,
    identity_always: Vec<bool>,
    generated_stored: Vec<Option<Eval>>,
    check_constraints: Vec<CheckEvaluator>,
    foreign_key_checks: Vec<RowConstraintCheck>,
    exclusion_checks: Vec<RowConstraintCheck>,
    exclusion_update_checks: Vec<RowUpdateConstraintCheck>,
    referenced_by_delete_checks: Vec<RowConstraintCheck>,
    referenced_by_update_checks: Vec<RowUpdateConstraintCheck>,
    returning_evaluators: Vec<Eval>,
    child: Box<dyn Operator>,
    done: bool,
    affected: i64,
}

impl<L: PageLoader> std::fmt::Debug for ModifyTable<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModifyTable")
            .field("relation", &self.relation)
            .field("kind", &self.kind)
            .field("done", &self.done)
            .field("affected", &self.affected)
            .finish_non_exhaustive()
    }
}

impl<L: PageLoader> ModifyTable<L> {
    /// Output schema shared across all `ModifyTable` instances: a single
    /// `Int64` column named `"affected_rows"`.
    fn affected_rows_schema() -> Schema {
        match Schema::new([Field::required("affected_rows", DataType::Int64)]) {
            Ok(schema) => schema,
            Err(err) => {
                tracing::error!(error = %err, "modify affected_rows schema failed");
                Schema::empty()
            }
        }
    }

    fn add_affected_rows(&mut self, rows: usize) -> Result<(), ExecError> {
        let delta = i64::try_from(rows).map_err(|_| {
            ExecError::NumericFieldOverflow("DML affected row count overflow".to_owned())
        })?;
        self.affected = self.affected.checked_add(delta).ok_or_else(|| {
            ExecError::NumericFieldOverflow("DML affected row count overflow".to_owned())
        })?;
        Ok(())
    }

    /// Construct a `ModifyTable` operator.
    ///
    /// # Parameters
    ///
    /// - `heap` — shared reference to the heap access method.
    /// - `relation` — target relation id.
    /// - `relation_schema` — full column schema of the target relation
    ///   (used as the codec schema for INSERT).
    /// - `kind` — mutation kind.
    /// - `stamps` — MVCC metadata to stamp on inserted/deleted tuple versions.
    /// - `wal` — optional WAL sink; `None` skips WAL emission.
    /// - `child` — source operator.
    #[must_use]
    #[allow(clippy::similar_names)]
    pub fn new(
        heap: Arc<HeapAccess<L>>,
        relation: RelationId,
        relation_schema: Schema,
        kind: ModifyKind,
        stamps: ModifyTableStamps,
        wal: Option<Arc<dyn WalSink>>,
        child: Box<dyn Operator>,
    ) -> Self {
        // Build per-assignment evaluators once at construction so the
        // per-row UPDATE loop does not pay the `ScalarExpr::clone()`
        // and evaluator-allocation cost on every iteration. For
        // INSERT and DELETE this is empty.
        let update_evaluators: Vec<(usize, Eval)> = match &kind {
            ModifyKind::Update { assignments } => assignments
                .iter()
                .map(|(col, expr)| (*col, Eval::new(expr.clone())))
                .collect(),
            ModifyKind::Insert | ModifyKind::Delete => Vec::new(),
        };
        let update_fast_path = match &kind {
            ModifyKind::Update { assignments } => {
                detect_update_int32_pair_fast_path(assignments, &relation_schema)
            }
            _ => None,
        };
        Self {
            heap,
            relation,
            schema: Self::affected_rows_schema(),
            codec: RowCodec::new(relation_schema),
            kind,
            update_evaluators,
            update_fast_path,
            insert_xmin: stamps.insert_xmin,
            insert_command_id: stamps.insert_command_id,
            delete_xmax: stamps.delete_xmax,
            delete_cmax: stamps.delete_cmax,
            wal,
            vm: None,
            insert_indexes: Vec::new(),
            update_indexes: Vec::new(),
            delete_indexes: Vec::new(),
            insert_vector_indexes: Vec::new(),
            update_vector_indexes: Vec::new(),
            delete_vector_indexes: Vec::new(),
            insert_conflict_action: None,
            insert_column_map: None,
            column_defaults: Vec::new(),
            sequence_defaults: Vec::new(),
            identity_always: Vec::new(),
            generated_stored: Vec::new(),
            check_constraints: Vec::new(),
            foreign_key_checks: Vec::new(),
            exclusion_checks: Vec::new(),
            exclusion_update_checks: Vec::new(),
            referenced_by_delete_checks: Vec::new(),
            referenced_by_update_checks: Vec::new(),
            returning_evaluators: Vec::new(),
            child,
            done: false,
            affected: 0,
        }
    }

    /// Attach the server-owned visibility map so heap mutations clear
    /// all-visible bits for touched pages.
    #[must_use]
    pub fn with_visibility_map(mut self, vm: Arc<VisibilityMap>) -> Self {
        self.vm = Some(vm);
        self
    }

    /// Attach B-tree index maintainers used by the INSERT arm.
    ///
    /// The operator updates these indexes after the heap batch returns
    /// the inserted tuple IDs. Duplicate key checks run before the heap
    /// write so statement-level rejection remains atomic for this path.
    #[must_use]
    pub fn with_insert_indexes(mut self, indexes: Vec<InsertIndexMaintainer<L>>) -> Self {
        self.insert_indexes = indexes;
        self
    }

    /// Attach B-tree index maintainers used by the UPDATE arm.
    #[must_use]
    pub fn with_update_indexes(mut self, indexes: Vec<InsertIndexMaintainer<L>>) -> Self {
        self.update_indexes = indexes;
        self
    }

    /// Attach B-tree index maintainers used by the DELETE arm.
    #[must_use]
    pub fn with_delete_indexes(mut self, indexes: Vec<InsertIndexMaintainer<L>>) -> Self {
        self.delete_indexes = indexes;
        self
    }

    /// Attach HNSW vector-index maintainers used by the INSERT arm.
    #[must_use]
    pub fn with_insert_vector_indexes(mut self, indexes: Vec<VectorIndexMaintainer>) -> Self {
        self.insert_vector_indexes = indexes;
        self
    }

    /// Attach HNSW vector-index maintainers used by the UPDATE arm.
    #[must_use]
    pub fn with_update_vector_indexes(mut self, indexes: Vec<VectorIndexMaintainer>) -> Self {
        self.update_vector_indexes = indexes;
        self
    }

    /// Attach HNSW vector-index maintainers used by the DELETE arm.
    #[must_use]
    pub fn with_delete_vector_indexes(mut self, indexes: Vec<VectorIndexMaintainer>) -> Self {
        self.delete_vector_indexes = indexes;
        self
    }

    /// Attach `INSERT ... ON CONFLICT` behavior.
    #[must_use]
    pub fn with_insert_conflict_action(mut self, action: InsertConflictAction) -> Self {
        self.insert_conflict_action = Some(action);
        self
    }

    /// Attach a source-to-target column map for INSERT.
    ///
    /// `map[src_idx] = target_idx`. Target columns omitted by `map`
    /// are filled with [`Value::Null`] before NOT NULL checks, index
    /// key encoding, and heap row encoding run.
    #[must_use]
    pub fn with_insert_column_map(mut self, map: Vec<usize>) -> Self {
        self.insert_column_map = Some(map);
        self
    }

    /// Attach per-column DEFAULT expressions evaluated for omitted
    /// INSERT columns.
    #[must_use]
    pub fn with_column_defaults(mut self, defaults: Vec<Option<ScalarExpr>>) -> Self {
        self.column_defaults = defaults
            .into_iter()
            .map(|expr| expr.map(Eval::new))
            .collect();
        self
    }

    /// Attach per-column sequence-backed defaults.
    #[must_use]
    pub fn with_sequence_defaults(mut self, defaults: Vec<Option<SequenceDefault>>) -> Self {
        self.sequence_defaults = defaults;
        self
    }

    /// Attach per-column `GENERATED ALWAYS AS IDENTITY` flags.
    #[must_use]
    pub fn with_identity_always(mut self, identity_always: Vec<bool>) -> Self {
        self.identity_always = identity_always;
        self
    }

    /// Attach per-column stored generated expressions.
    #[must_use]
    pub fn with_generated_stored(mut self, generated: Vec<Option<ScalarExpr>>) -> Self {
        self.generated_stored = generated
            .into_iter()
            .map(|expr| expr.map(Eval::new))
            .collect();
        self
    }

    /// Attach row-level CHECK constraints evaluated for INSERT/UPDATE.
    #[must_use]
    pub fn with_check_constraints(mut self, checks: Vec<(String, ScalarExpr)>) -> Self {
        self.check_constraints = checks
            .into_iter()
            .map(|(name, expr)| CheckEvaluator {
                name,
                evaluator: Eval::new(expr),
            })
            .collect();
        self
    }

    /// Attach FOREIGN KEY checks for rows written by INSERT/UPDATE.
    #[must_use]
    pub fn with_foreign_key_checks(mut self, checks: Vec<RowConstraintCheck>) -> Self {
        self.foreign_key_checks = checks;
        self
    }

    /// Attach EXCLUDE checks for rows written by INSERT.
    #[must_use]
    pub fn with_exclusion_checks(mut self, checks: Vec<RowConstraintCheck>) -> Self {
        self.exclusion_checks = checks;
        self
    }

    /// Attach EXCLUDE checks for rows written by UPDATE.
    #[must_use]
    pub fn with_exclusion_update_checks(mut self, checks: Vec<RowUpdateConstraintCheck>) -> Self {
        self.exclusion_update_checks = checks;
        self
    }

    /// Attach RESTRICT/NO ACTION checks for parent rows deleted by this operator.
    #[must_use]
    pub fn with_referenced_by_delete_checks(mut self, checks: Vec<RowConstraintCheck>) -> Self {
        self.referenced_by_delete_checks = checks;
        self
    }

    /// Attach RESTRICT/NO ACTION checks for parent key UPDATEs.
    #[must_use]
    pub fn with_referenced_by_update_checks(
        mut self,
        checks: Vec<RowUpdateConstraintCheck>,
    ) -> Self {
        self.referenced_by_update_checks = checks;
        self
    }

    /// Replace the default affected-row output with a `RETURNING`
    /// projection evaluated over the row image the mutation exposes:
    /// inserted row for INSERT, updated row for UPDATE, old row for
    /// DELETE.
    #[must_use]
    pub fn with_returning(mut self, exprs: Vec<ScalarExpr>, schema: Schema) -> Self {
        self.returning_evaluators = exprs.into_iter().map(Eval::new).collect();
        self.schema = schema;
        self
    }
}

/// Inspect a bound UPDATE assignment list against the target
/// relation schema and return a [`UpdateFastPathInt32Pair`] descriptor
/// when the columnar fast path applies.
///
/// Conditions:
///
/// - Relation schema is exactly two non-nullable `Int32` columns
///   (matches the bench tables `(id INT, val INT)`).
/// - Exactly one assignment targets one of those two columns.
/// - The assignment expression is either `col + lit`, `lit + col`,
///   `col - lit`, where `col` references the **target** column and
///   `lit` is an `Int32` literal. `lit - col` is rejected because the
///   transformation collapses into a single add only when the column
///   is the *left* operand of a subtract.
fn detect_update_int32_pair_fast_path(
    assignments: &[(usize, ScalarExpr)],
    relation_schema: &Schema,
) -> Option<UpdateFastPathInt32Pair> {
    if relation_schema.len() != 2 {
        return None;
    }
    let fields = relation_schema.fields();
    if !matches!(fields[0].data_type, DataType::Int32)
        || !matches!(fields[1].data_type, DataType::Int32)
    {
        return None;
    }
    if assignments.len() != 1 {
        return None;
    }
    let (target_col, expr) = &assignments[0];
    let target_col = *target_col;
    if target_col > 1 {
        return None;
    }
    let (op, left, right) = match expr {
        ScalarExpr::Binary {
            op,
            left,
            right,
            data_type: DataType::Int32,
        } => (op, left.as_ref(), right.as_ref()),
        _ => return None,
    };
    let column_ref_idx = |e: &ScalarExpr| match e {
        ScalarExpr::Column {
            index,
            data_type: DataType::Int32,
            ..
        } => Some(*index),
        _ => None,
    };
    let literal_i32 = |e: &ScalarExpr| match e {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            ..
        } => Some(*v),
        _ => None,
    };
    let delta = match op {
        BinaryOp::Add => {
            if column_ref_idx(left) == Some(target_col) {
                literal_i32(right)?
            } else if column_ref_idx(right) == Some(target_col) {
                literal_i32(left)?
            } else {
                return None;
            }
        }
        BinaryOp::Sub => {
            // Only `col - lit` collapses to a single signed add.
            if column_ref_idx(left) != Some(target_col) {
                return None;
            }
            let lit = literal_i32(right)?;
            lit.checked_neg()?
        }
        _ => return None,
    };
    Some(UpdateFastPathInt32Pair {
        target_col_in_relation: target_col,
        delta,
    })
}

fn updated_ctid_target(header: &TupleHeader, current: TupleId) -> Option<TupleId> {
    if header.ctid == current {
        return None;
    }
    let redirects = header.infomask.contains(InfoMask::UPDATED)
        || header.infomask.contains(InfoMask::HOT_UPDATED);
    redirects.then_some(header.ctid)
}

fn conflict_target_columns(action: &InsertConflictAction) -> Option<&[usize]> {
    match action {
        InsertConflictAction::DoNothing { target } => target.as_deref(),
        InsertConflictAction::DoUpdate { target, .. } => Some(target.as_slice()),
    }
}

fn insert_conflict_uses_index<L: PageLoader>(
    action: &InsertConflictAction,
    index: &InsertIndexMaintainer<L>,
) -> bool {
    if !index.is_unique() {
        return false;
    }
    match conflict_target_columns(action) {
        Some(target) => columns_match_unordered(&index.key_columns, target),
        None => true,
    }
}

fn columns_match_unordered(left: &[usize], right: &[usize]) -> bool {
    left.len() == right.len() && left.iter().all(|col| right.contains(col))
}

impl<L: PageLoader + Send + Sync + std::fmt::Debug + 'static> Operator for ModifyTable<L> {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        // For UPDATE we accumulate every batch's `(old_tid, payload)`
        // edits into a single Vec and hand the whole set to
        // `heap.update_many` in one call after the child is drained.
        // The bulk-UPDATE path inside `update_many` pays a fixed
        // per-call cost (sort, page-group walk, insert_batch dispatch,
        // column-cache invalidate); coalescing across batches drops
        // that overhead from `O(n_batches)` to `O(1)` while keeping the
        // per-row cost identical.
        let mut all_update_edits: Vec<(TupleId, UpdatePayload)> = Vec::new();
        let mut all_update_index_changes: Vec<UpdateIndexChange> = Vec::new();
        let mut all_update_vector_index_changes: Vec<VectorUpdateIndexChange> = Vec::new();
        let mut returning_rows: Vec<Vec<Value>> = Vec::new();
        let returning_active = !self.returning_evaluators.is_empty();

        // Drain the entire child input.
        loop {
            let Some(batch) = self.child.next_batch()? else {
                break;
            };
            if batch.rows() == 0 {
                continue;
            }

            match &self.kind {
                ModifyKind::Delete => {
                    // Bulk path: read every TID **directly** from the
                    // batch's first two columns (`tid_block`,
                    // `tid_slot` — both non-nullable `Int32` per
                    // `SeqScan::new_with_tids`) and hand the lot to
                    // `heap.delete_many`. No `batch_to_rows`
                    // materialisation, no per-row `Vec<Value>`
                    // intermediate.
                    let (tids, delete_index_changes, delete_vector_index_changes, deleted_rows) =
                        if !returning_active
                            && self.delete_indexes.is_empty()
                            && self.delete_vector_indexes.is_empty()
                            && self.referenced_by_delete_checks.is_empty()
                        {
                            (
                                extract_tids_from_batch(&batch, self.relation)?,
                                Vec::new(),
                                Vec::new(),
                                Vec::new(),
                            )
                        } else {
                            self.extract_delete_tids_and_index_changes(&batch, returning_active)?
                        };
                    let n = tids.len();
                    let wal_ref: Option<&dyn WalSink> = self.wal.as_deref();
                    self.heap
                        .delete_many(
                            tids,
                            DeleteOptions {
                                xmax: self.delete_xmax,
                                cmax: self.delete_cmax,
                                wal: wal_ref,
                                fsm: None,
                                vm: self.vm.as_deref(),
                            },
                        )
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
                    self.apply_delete_index_changes(&delete_index_changes)?;
                    self.apply_delete_vector_index_changes(&delete_vector_index_changes)?;
                    if returning_active {
                        for row in deleted_rows {
                            returning_rows.push(self.evaluate_returning_row(&row)?);
                        }
                    }
                    self.add_affected_rows(n)?;
                }
                ModifyKind::Update { .. } => {
                    // Columnar fast path: `UPDATE t SET col_i = col_i ± lit`
                    // over an `(Int32, Int32)` relation. Builds every
                    // tuple's 9-byte payload inline from the batch's
                    // column arrays — no `batch_to_rows`, no per-row
                    // `Eval`, no per-row `RowCodec::encode` tree walk.
                    let edits = if let Some(spec) = self.update_fast_path.filter(|_| {
                        !returning_active
                            && self.check_constraints.is_empty()
                            && self.foreign_key_checks.is_empty()
                            && self.exclusion_update_checks.is_empty()
                            && self.referenced_by_update_checks.is_empty()
                            && self.update_indexes.is_empty()
                            && self.update_vector_indexes.is_empty()
                    }) {
                        build_update_edits_int32_pair(&batch, self.relation, spec)?
                    } else {
                        // Slow path: batch_to_rows + per-row eval +
                        // per-row codec.encode. Covers every UPDATE
                        // shape not matched by the fast-path detector.
                        let child_schema = self.child.schema().clone();
                        let rows = batch_to_rows(&batch, &child_schema)
                            .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
                        let mut edits: Vec<(TupleId, UpdatePayload)> =
                            Vec::with_capacity(rows.len());
                        for row in &rows {
                            let computed = self.compute_update_edit(row, returning_active)?;
                            if let Some(index_change) = computed.index_change {
                                all_update_index_changes.push(index_change);
                            }
                            if let Some(index_change) = computed.vector_index_change {
                                all_update_vector_index_changes.push(index_change);
                            }
                            if let Some(returning_row) = computed.returning_row {
                                returning_rows.push(self.evaluate_returning_row(&returning_row)?);
                            }
                            edits.push((computed.tid, computed.payload));
                        }
                        edits
                    };
                    if all_update_edits.is_empty() {
                        all_update_edits = edits;
                    } else {
                        all_update_edits.extend(edits);
                    }
                }
                ModifyKind::Insert => {
                    // Batched INSERT: encode every row in this batch
                    // once into per-row `Vec<u8>` payloads and hand
                    // the slice to `heap.insert_batch`. That bulk
                    // call pins each destination page exactly once
                    // and writes every payload under one write guard
                    // per page — replacing the prior per-row
                    // `heap.insert` loop that re-entered the buffer
                    // pool once per row.
                    let child_schema = self.child.schema().clone();
                    let rows = batch_to_rows(&batch, &child_schema)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
                    let target_schema = self.codec.schema();
                    let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
                    let mut index_keys: Vec<Vec<Option<i64>>> = self
                        .insert_indexes
                        .iter()
                        .map(|_| Vec::with_capacity(rows.len()))
                        .collect();
                    let mut vector_index_keys: Vec<Vec<Option<Vec<f32>>>> = self
                        .insert_vector_indexes
                        .iter()
                        .map(|_| Vec::with_capacity(rows.len()))
                        .collect();
                    let mut seen_keys: Vec<HashSet<i64>> =
                        self.insert_indexes.iter().map(|_| HashSet::new()).collect();
                    let conflict_action = self.insert_conflict_action.clone();
                    self.validate_insert_conflict_arbiter(conflict_action.as_ref())?;
                    for row in &rows {
                        let mut expanded_row;
                        let omitted;
                        let target_row = if self.insert_column_map.is_some()
                            || !self.column_defaults.is_empty()
                            || !self.sequence_defaults.is_empty()
                            || !self.identity_always.is_empty()
                            || !self.generated_stored.is_empty()
                            || !self.check_constraints.is_empty()
                            || !self.exclusion_checks.is_empty()
                        {
                            if let Some(column_map) = &self.insert_column_map {
                                let expanded =
                                    expand_insert_row(row, target_schema.len(), column_map)?;
                                expanded_row = expanded.values;
                                omitted = expanded.omitted;
                            } else {
                                expanded_row = row.clone();
                                omitted = vec![false; target_schema.len()];
                            }
                            self.check_identity_explicit_values(&omitted)?;
                            self.apply_insert_defaults(&mut expanded_row, &omitted)?;
                            self.check_generated_stored_explicit_values(&omitted)?;
                            self.apply_generated_stored(&mut expanded_row)?;
                            self.check_row_constraints(&expanded_row)?;
                            self.check_foreign_keys(&expanded_row)?;
                            self.check_exclusions(&expanded_row)?;
                            expanded_row.as_slice()
                        } else {
                            row.as_slice()
                        };
                        if self.insert_column_map.is_none()
                            && self.column_defaults.is_empty()
                            && self.sequence_defaults.is_empty()
                            && self.identity_always.is_empty()
                            && self.generated_stored.is_empty()
                            && self.check_constraints.is_empty()
                            && self.exclusion_checks.is_empty()
                        {
                            self.check_foreign_keys(target_row)?;
                        }
                        check_not_null_violations(target_row, target_schema)?;
                        let row_index_keys = self
                            .insert_indexes
                            .iter()
                            .map(|index| index.encode_key(target_row))
                            .collect::<Result<Vec<_>, _>>()?;
                        if let Some(action) = &conflict_action {
                            if let Some(conflict) =
                                self.find_insert_conflict(action, &row_index_keys, &seen_keys)?
                            {
                                match action {
                                    InsertConflictAction::DoNothing { .. } => continue,
                                    InsertConflictAction::DoUpdate {
                                        assignments,
                                        predicate,
                                        ..
                                    } => {
                                        let InsertConflict::Existing(tid) = conflict else {
                                            return Err(ExecError::TypeMismatch(
                                                "ON CONFLICT DO UPDATE cannot affect the same row twice"
                                                    .to_owned(),
                                            ));
                                        };
                                        let (current_tid, old_row) =
                                            self.fetch_conflict_current_row(tid)?;
                                        if let Some(computed) = self.compute_conflict_update_edit(
                                            current_tid,
                                            &old_row,
                                            target_row,
                                            assignments,
                                            predicate.as_ref(),
                                            returning_active,
                                        )? {
                                            if let Some(index_change) = computed.index_change {
                                                all_update_index_changes.push(index_change);
                                            }
                                            if let Some(index_change) = computed.vector_index_change
                                            {
                                                all_update_vector_index_changes.push(index_change);
                                            }
                                            if let Some(returning_row) = computed.returning_row {
                                                returning_rows.push(
                                                    self.evaluate_returning_row(&returning_row)?,
                                                );
                                            }
                                            all_update_edits.push((computed.tid, computed.payload));
                                        }
                                        continue;
                                    }
                                }
                            }
                            self.remember_insert_keys(&row_index_keys, &mut seen_keys);
                        } else {
                            self.reject_duplicate_insert_keys(&row_index_keys, &mut seen_keys)?;
                        }
                        for (idx, key) in row_index_keys.iter().copied().enumerate() {
                            if idx >= index_keys.len() {
                                return Err(ExecError::Internal(
                                    "insert index key vector width mismatch",
                                ));
                            }
                            index_keys[idx].push(key);
                        }
                        let row_vector_index_keys = self
                            .insert_vector_indexes
                            .iter()
                            .map(|index| index.encode_key(target_row))
                            .collect::<Result<Vec<_>, _>>()?;
                        for (idx, key) in row_vector_index_keys.into_iter().enumerate() {
                            if idx >= vector_index_keys.len() {
                                return Err(ExecError::Internal(
                                    "insert vector index key vector width mismatch",
                                ));
                            }
                            vector_index_keys[idx].push(key);
                        }
                        if returning_active {
                            returning_rows.push(self.evaluate_returning_row(target_row)?);
                        }
                        let payload = self
                            .codec
                            .encode(target_row)
                            .map_err(row_codec_error_to_exec)?;
                        payloads.push(payload);
                    }
                    let n = payloads.len();
                    let payload_refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
                    let wal_ref: Option<&dyn WalSink> = self.wal.as_deref();
                    let tids = self
                        .heap
                        .insert_batch(
                            self.relation,
                            &payload_refs,
                            ultrasql_storage::heap::InsertOptions {
                                xmin: self.insert_xmin,
                                command_id: self.insert_command_id,
                                wal: wal_ref,
                                fsm: None,
                                vm: self.vm.as_deref(),
                            },
                        )
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
                    debug_assert_eq!(tids.len(), payloads.len());
                    for (idx, index) in self.insert_indexes.iter_mut().enumerate() {
                        for (row_idx, key) in index_keys[idx].iter().enumerate() {
                            if let Some(k) = key {
                                let Some(tid) = tids.get(row_idx).copied() else {
                                    return Err(ExecError::Internal(
                                        "heap insert_batch returned fewer TIDs than payloads",
                                    ));
                                };
                                index.insert_key(*k, tid, self.insert_xmin, wal_ref)?;
                            }
                        }
                    }
                    for (idx, index) in self.insert_vector_indexes.iter().enumerate() {
                        for (row_idx, key) in vector_index_keys[idx].iter().enumerate() {
                            if let Some(vector) = key {
                                let Some(tid) = tids.get(row_idx).copied() else {
                                    return Err(ExecError::Internal(
                                        "heap insert_batch returned fewer TIDs than payloads",
                                    ));
                                };
                                index.insert_vector(vector, tid)?;
                            }
                        }
                    }
                    self.add_affected_rows(n)?;
                }
            }
        }

        // Single bulk UPDATE call after every input batch has been
        // accumulated. See the `all_update_edits` comment above.
        if !all_update_edits.is_empty() {
            let n = all_update_edits.len();
            let wal = self.wal.clone();
            let wal_ref: Option<&dyn WalSink> = wal.as_deref();
            let index_keys_unchanged = self.update_indexes.is_empty()
                || all_update_index_changes
                    .iter()
                    .all(|change| change.old_keys == change.new_keys);
            let vector_index_keys_unchanged = self.update_vector_indexes.is_empty()
                || all_update_vector_index_changes
                    .iter()
                    .all(|change| change.old_keys == change.new_keys);
            let update_opts = UpdateOptions {
                xid: self.delete_xmax,
                command_id: self.delete_cmax,
                hot_eligible: index_keys_unchanged && vector_index_keys_unchanged,
                wal: wal_ref,
                vm: self.vm.as_deref(),
            };
            if self.update_indexes.is_empty() && self.update_vector_indexes.is_empty() {
                self.heap
                    .update_many(all_update_edits, update_opts)
                    .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
            } else {
                self.precheck_update_index_changes(&all_update_index_changes)?;
                let outcomes = self
                    .heap
                    .update_many_with_outcomes(all_update_edits, update_opts)
                    .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
                self.apply_update_index_changes(&all_update_index_changes, &outcomes, wal_ref)?;
                self.apply_update_vector_index_changes(
                    &all_update_vector_index_changes,
                    &outcomes,
                )?;
            }
            self.add_affected_rows(n)?;
        }

        if returning_active {
            return build_batch(&returning_rows, &self.schema).map(Some);
        }

        // Emit the affected-row-count batch.
        let batch = Batch::new([Column::Int64(NumericColumn::from_data(vec![self.affected]))])
            .map_err(ExecError::from)?;
        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn expand_insert_row(
    row: &[Value],
    target_width: usize,
    column_map: &[usize],
) -> Result<ExpandedInsertRow, ExecError> {
    if row.len() != column_map.len() {
        return Err(ExecError::TypeMismatch(format!(
            "INSERT source row has {} columns, but column map has {} entries",
            row.len(),
            column_map.len()
        )));
    }
    let mut out = vec![Value::Null; target_width];
    let mut seen = vec![false; target_width];
    for (src_idx, target_idx) in column_map.iter().copied().enumerate() {
        if target_idx >= target_width {
            return Err(ExecError::TypeMismatch(format!(
                "INSERT target column index {target_idx} out of range (relation has {target_width} columns)"
            )));
        }
        if seen[target_idx] {
            return Err(ExecError::TypeMismatch(format!(
                "INSERT target column index {target_idx} appears more than once"
            )));
        }
        seen[target_idx] = true;
        out[target_idx] = row[src_idx].clone();
    }
    let omitted = seen.into_iter().map(|present| !present).collect();
    Ok(ExpandedInsertRow {
        values: out,
        omitted,
    })
}

struct ExpandedInsertRow {
    values: Vec<Value>,
    omitted: Vec<bool>,
}

impl<L: PageLoader + Send + Sync + std::fmt::Debug + 'static> ModifyTable<L> {
    /// Compute the `(old_tid, new_payload_bytes)` edit for a single
    /// UPDATE input row.
    ///
    /// The `row` slice must begin with `[tid_block: Int32, tid_slot:
    /// Int32, original_col0, ...]`. We extract the TID from the
    /// first two columns, apply the cached evaluators to the
    /// remaining columns to build the new row, and encode it through
    /// the operator's precomputed [`RowCodec`] (with a
    /// `fixed_width_lower_bound`-sized initial capacity so the first
    /// push does not reallocate). The encoded payload is handed to
    /// [`HeapAccess::update_many`] by the bulk caller.
    fn compute_update_edit(
        &self,
        row: &[Value],
        capture_returning_row: bool,
    ) -> Result<ComputedUpdate, ExecError> {
        let (tid, orig_row) = extract_tid_and_row(row, self.relation)?;

        // Build the new row from the original, applying assignments.
        let relation_cols = self.codec.schema().len();
        let mut new_row: Vec<Value> = orig_row.to_vec();
        if new_row.len() != relation_cols {
            return Err(ExecError::TypeMismatch(format!(
                "UPDATE row has {} columns after TID, expected {}",
                new_row.len(),
                relation_cols,
            )));
        }
        let old_keys = self.encode_update_index_keys(orig_row)?;
        let old_vector_keys = self.encode_update_vector_index_keys(orig_row)?;

        for (col_idx, evaluator) in &self.update_evaluators {
            if self
                .generated_stored
                .get(*col_idx)
                .is_some_and(Option::is_some)
            {
                return Err(ExecError::GeneratedAlwaysViolation(
                    self.codec.schema().field_at(*col_idx).name.clone(),
                ));
            }
            let val = evaluator.eval(orig_row).map_err(eval_error_to_exec_error)?;
            if *col_idx >= relation_cols {
                return Err(ExecError::TypeMismatch(format!(
                    "UPDATE assignment column index {col_idx} out of range (relation has {relation_cols} columns)"
                )));
            }
            new_row[*col_idx] = val;
        }
        self.apply_generated_stored(&mut new_row)?;
        check_not_null_violations(&new_row, self.codec.schema())?;
        self.check_row_constraints(&new_row)?;
        self.check_foreign_keys(&new_row)?;
        self.check_exclusion_update(orig_row, &new_row)?;
        self.check_referenced_by_update(orig_row, &new_row)?;
        let new_keys = self.encode_update_index_keys(&new_row)?;
        let new_vector_keys = self.encode_update_vector_index_keys(&new_row)?;

        let new_payload = self
            .codec
            .encode(&new_row)
            .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
        // Move the encoded bytes into a `SmallVec<[u8; 16]>`; rows
        // ≤ 16 bytes stay inline. `SmallVec::from_vec` reuses the
        // existing heap buffer when the row spills.
        let payload = UpdatePayload::from_vec(new_payload);
        let index_change = if self.update_indexes.is_empty() {
            None
        } else {
            Some(UpdateIndexChange {
                old_tid: tid,
                old_keys,
                new_keys,
            })
        };
        let vector_index_change = if self.update_vector_indexes.is_empty() {
            None
        } else {
            Some(VectorUpdateIndexChange {
                old_tid: tid,
                old_keys: old_vector_keys,
                new_keys: new_vector_keys,
            })
        };
        Ok(ComputedUpdate {
            tid,
            payload,
            index_change,
            vector_index_change,
            returning_row: capture_returning_row.then_some(new_row),
        })
    }

    fn compute_conflict_update_edit(
        &self,
        tid: TupleId,
        orig_row: &[Value],
        excluded_row: &[Value],
        assignments: &[(usize, Eval)],
        predicate: Option<&Eval>,
        capture_returning_row: bool,
    ) -> Result<Option<ComputedUpdate>, ExecError> {
        let mut eval_row = Vec::with_capacity(orig_row.len().saturating_add(excluded_row.len()));
        eval_row.extend_from_slice(orig_row);
        eval_row.extend_from_slice(excluded_row);
        if let Some(predicate) = predicate {
            match predicate
                .eval(&eval_row)
                .map_err(eval_error_to_exec_error)?
            {
                Value::Bool(true) => {}
                Value::Bool(false) | Value::Null => return Ok(None),
                other => {
                    return Err(ExecError::TypeMismatch(format!(
                        "ON CONFLICT DO UPDATE WHERE returned {:?}, expected Bool",
                        other.data_type()
                    )));
                }
            }
        }

        let relation_cols = self.codec.schema().len();
        let mut new_row: Vec<Value> = orig_row.to_vec();
        if new_row.len() != relation_cols {
            return Err(ExecError::TypeMismatch(format!(
                "ON CONFLICT row has {} columns, expected {}",
                new_row.len(),
                relation_cols,
            )));
        }
        let old_keys = self.encode_update_index_keys(orig_row)?;
        let old_vector_keys = self.encode_update_vector_index_keys(orig_row)?;

        for (col_idx, evaluator) in assignments {
            if self
                .generated_stored
                .get(*col_idx)
                .is_some_and(Option::is_some)
            {
                return Err(ExecError::GeneratedAlwaysViolation(
                    self.codec.schema().field_at(*col_idx).name.clone(),
                ));
            }
            if *col_idx >= relation_cols {
                return Err(ExecError::TypeMismatch(format!(
                    "ON CONFLICT assignment column index {col_idx} out of range (relation has {relation_cols} columns)"
                )));
            }
            new_row[*col_idx] = evaluator
                .eval(&eval_row)
                .map_err(eval_error_to_exec_error)?;
        }
        self.apply_generated_stored(&mut new_row)?;
        check_not_null_violations(&new_row, self.codec.schema())?;
        self.check_row_constraints(&new_row)?;
        self.check_foreign_keys(&new_row)?;
        self.check_exclusion_update(orig_row, &new_row)?;
        self.check_referenced_by_update(orig_row, &new_row)?;
        let new_keys = self.encode_update_index_keys(&new_row)?;
        let new_vector_keys = self.encode_update_vector_index_keys(&new_row)?;

        let new_payload = self
            .codec
            .encode(&new_row)
            .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
        let payload = UpdatePayload::from_vec(new_payload);
        let index_change = if self.update_indexes.is_empty() {
            None
        } else {
            Some(UpdateIndexChange {
                old_tid: tid,
                old_keys,
                new_keys,
            })
        };
        let vector_index_change = if self.update_vector_indexes.is_empty() {
            None
        } else {
            Some(VectorUpdateIndexChange {
                old_tid: tid,
                old_keys: old_vector_keys,
                new_keys: new_vector_keys,
            })
        };
        Ok(Some(ComputedUpdate {
            tid,
            payload,
            index_change,
            vector_index_change,
            returning_row: capture_returning_row.then_some(new_row),
        }))
    }

    fn fetch_conflict_current_row(&self, tid: TupleId) -> Result<(TupleId, Vec<Value>), ExecError> {
        let mut current = tid;
        for _ in 0..64 {
            let tuple = self.heap.fetch(current).map_err(|e| {
                ExecError::TypeMismatch(format!("ON CONFLICT fetch existing tuple: {e}"))
            })?;
            if let Some(next) = updated_ctid_target(&tuple.header, current) {
                current = next;
                continue;
            }
            let row = self.codec.decode(&tuple.data).map_err(|e| {
                ExecError::TypeMismatch(format!("ON CONFLICT decode existing tuple: {e}"))
            })?;
            return Ok((current, row));
        }
        Err(ExecError::Internal(
            "ON CONFLICT update ctid chain exceeded 64 hops",
        ))
    }

    fn evaluate_returning_row(&self, row: &[Value]) -> Result<Vec<Value>, ExecError> {
        self.returning_evaluators
            .iter()
            .map(|eval| eval.eval(row).map_err(eval_error_to_exec_error))
            .collect()
    }

    fn apply_insert_defaults(&self, row: &mut [Value], omitted: &[bool]) -> Result<(), ExecError> {
        if self.column_defaults.is_empty() && self.sequence_defaults.is_empty() {
            return Ok(());
        }
        if (!self.column_defaults.is_empty() && self.column_defaults.len() != row.len())
            || (!self.sequence_defaults.is_empty() && self.sequence_defaults.len() != row.len())
            || omitted.len() != row.len()
        {
            return Err(ExecError::TypeMismatch(
                "INSERT default metadata width does not match target row".to_owned(),
            ));
        }
        for idx in 0..row.len() {
            if !omitted[idx] {
                continue;
            }
            if let Some(default) = self.sequence_defaults.get(idx).and_then(Option::as_ref) {
                row[idx] = self.next_sequence_default_value(idx, default)?;
                continue;
            }
            let Some(default) = self.column_defaults.get(idx) else {
                continue;
            };
            if let Some(evaluator) = default {
                row[idx] = evaluator.eval(&[]).map_err(eval_error_to_exec_error)?;
            }
        }
        Ok(())
    }

    fn check_identity_explicit_values(&self, omitted: &[bool]) -> Result<(), ExecError> {
        if self.identity_always.is_empty() {
            return Ok(());
        }
        if self.identity_always.len() != omitted.len() {
            return Err(ExecError::TypeMismatch(
                "INSERT identity metadata width does not match target row".to_owned(),
            ));
        }
        for (idx, always) in self.identity_always.iter().copied().enumerate() {
            if always && !omitted[idx] {
                return Err(ExecError::GeneratedAlwaysViolation(
                    self.codec.schema().field_at(idx).name.clone(),
                ));
            }
        }
        Ok(())
    }

    fn check_generated_stored_explicit_values(&self, omitted: &[bool]) -> Result<(), ExecError> {
        if self.generated_stored.is_empty() {
            return Ok(());
        }
        if self.generated_stored.len() != omitted.len() {
            return Err(ExecError::TypeMismatch(
                "INSERT generated-column metadata width does not match target row".to_owned(),
            ));
        }
        for (idx, generated) in self.generated_stored.iter().enumerate() {
            if generated.is_some() && !omitted[idx] {
                return Err(ExecError::GeneratedAlwaysViolation(
                    self.codec.schema().field_at(idx).name.clone(),
                ));
            }
        }
        Ok(())
    }

    fn apply_generated_stored(&self, row: &mut [Value]) -> Result<(), ExecError> {
        if self.generated_stored.is_empty() {
            return Ok(());
        }
        if self.generated_stored.len() != row.len() {
            return Err(ExecError::TypeMismatch(
                "generated-column metadata width does not match target row".to_owned(),
            ));
        }
        for idx in 0..row.len() {
            let Some(evaluator) = self.generated_stored.get(idx).and_then(Option::as_ref) else {
                continue;
            };
            row[idx] = evaluator.eval(row).map_err(eval_error_to_exec_error)?;
        }
        Ok(())
    }

    fn check_foreign_keys(&self, row: &[Value]) -> Result<(), ExecError> {
        for check in &self.foreign_key_checks {
            check(row)?;
        }
        Ok(())
    }

    fn check_exclusions(&self, row: &[Value]) -> Result<(), ExecError> {
        for check in &self.exclusion_checks {
            check(row)?;
        }
        Ok(())
    }

    fn check_exclusion_update(
        &self,
        old_row: &[Value],
        new_row: &[Value],
    ) -> Result<(), ExecError> {
        for check in &self.exclusion_update_checks {
            check(old_row, new_row)?;
        }
        Ok(())
    }

    fn check_referenced_by_delete(&self, row: &[Value]) -> Result<(), ExecError> {
        for check in &self.referenced_by_delete_checks {
            check(row)?;
        }
        Ok(())
    }

    fn check_referenced_by_update(
        &self,
        old_row: &[Value],
        new_row: &[Value],
    ) -> Result<(), ExecError> {
        for check in &self.referenced_by_update_checks {
            check(old_row, new_row)?;
        }
        Ok(())
    }

    fn next_sequence_default_value(
        &self,
        idx: usize,
        default: &SequenceDefault,
    ) -> Result<Value, ExecError> {
        let raw = if let Some(wal) = &default.wal {
            default.sequence.nextval_logged(
                &default.name,
                default.seqrelid,
                default.xid,
                Some(wal.as_ref()),
            )
        } else {
            default.sequence.nextval()
        }
        .map_err(|e| ExecError::TypeMismatch(format!("sequence default {}: {e}", default.name)))?;
        if let Some(on_nextval) = &default.on_nextval {
            on_nextval(&default.name, raw);
        }
        let field = self.codec.schema().field_at(idx);
        match field.data_type {
            DataType::Int16 => i16::try_from(raw).map(Value::Int16).map_err(|_| {
                ExecError::TypeMismatch(format!(
                    "sequence default {} value {raw} out of range for Int16",
                    default.name
                ))
            }),
            DataType::Int32 => i32::try_from(raw).map(Value::Int32).map_err(|_| {
                ExecError::TypeMismatch(format!(
                    "sequence default {} value {raw} out of range for Int32",
                    default.name
                ))
            }),
            DataType::Int64 => Ok(Value::Int64(raw)),
            ref other => Err(ExecError::TypeMismatch(format!(
                "sequence default {} cannot populate {:?}",
                default.name, other
            ))),
        }
    }

    fn check_row_constraints(&self, row: &[Value]) -> Result<(), ExecError> {
        for check in &self.check_constraints {
            match check
                .evaluator
                .eval(row)
                .map_err(eval_error_to_exec_error)?
            {
                Value::Bool(true) | Value::Null => {}
                Value::Bool(false) => return Err(ExecError::CheckViolation(check.name.clone())),
                other => {
                    return Err(ExecError::TypeMismatch(format!(
                        "CHECK constraint {} returned {:?}, expected Bool",
                        check.name,
                        other.data_type()
                    )));
                }
            }
        }
        Ok(())
    }

    fn encode_update_index_keys(&self, row: &[Value]) -> Result<Vec<Option<i64>>, ExecError> {
        self.update_indexes
            .iter()
            .map(|index| index.encode_key(row))
            .collect()
    }

    fn encode_update_vector_index_keys(
        &self,
        row: &[Value],
    ) -> Result<Vec<Option<Vec<f32>>>, ExecError> {
        self.update_vector_indexes
            .iter()
            .map(|index| index.encode_key(row))
            .collect()
    }

    fn validate_insert_conflict_arbiter(
        &self,
        action: Option<&InsertConflictAction>,
    ) -> Result<(), ExecError> {
        let Some(action) = action else {
            return Ok(());
        };
        let Some(target) = conflict_target_columns(action) else {
            return Ok(());
        };
        if self
            .insert_indexes
            .iter()
            .any(|index| index.is_unique() && columns_match_unordered(&index.key_columns, target))
        {
            return Ok(());
        }
        Err(ExecError::TypeMismatch(format!(
            "ON CONFLICT target {:?} does not match a unique index",
            target
        )))
    }

    fn find_insert_conflict(
        &self,
        action: &InsertConflictAction,
        row_keys: &[Option<i64>],
        seen_keys: &[HashSet<i64>],
    ) -> Result<Option<InsertConflict>, ExecError> {
        for (idx, index) in self.insert_indexes.iter().enumerate() {
            if !insert_conflict_uses_index(action, index) {
                continue;
            }
            let Some(key) = row_keys.get(idx).copied().flatten() else {
                continue;
            };
            if seen_keys.get(idx).is_some_and(|seen| seen.contains(&key)) {
                return Ok(Some(InsertConflict::InBatch));
            }
            if let Some(tid) = index.lookup_tid(key)? {
                return Ok(Some(InsertConflict::Existing(tid)));
            }
        }
        Ok(None)
    }

    fn reject_duplicate_insert_keys(
        &self,
        row_keys: &[Option<i64>],
        seen_keys: &mut [HashSet<i64>],
    ) -> Result<(), ExecError> {
        for (idx, index) in self.insert_indexes.iter().enumerate() {
            let Some(key) = row_keys.get(idx).copied().flatten() else {
                continue;
            };
            if !index.is_unique() {
                continue;
            }
            if !seen_keys[idx].insert(key) || index.contains_key(key)? {
                return Err(ExecError::UniqueViolation(index.name.clone()));
            }
        }
        Ok(())
    }

    fn remember_insert_keys(&self, row_keys: &[Option<i64>], seen_keys: &mut [HashSet<i64>]) {
        for (idx, index) in self.insert_indexes.iter().enumerate() {
            let Some(key) = row_keys.get(idx).copied().flatten() else {
                continue;
            };
            if index.is_unique() {
                seen_keys[idx].insert(key);
            }
        }
    }

    fn apply_update_index_changes(
        &mut self,
        changes: &[UpdateIndexChange],
        outcomes: &[ultrasql_storage::heap::UpdateOutcome],
        wal: Option<&dyn WalSink>,
    ) -> Result<(), ExecError> {
        let outcome_by_old: std::collections::HashMap<
            TupleId,
            ultrasql_storage::heap::UpdateOutcome,
        > = outcomes
            .iter()
            .map(|outcome| (outcome.old_tid, *outcome))
            .collect();
        for change in changes {
            let Some(outcome) = outcome_by_old.get(&change.old_tid).copied() else {
                return Err(ExecError::Internal(
                    "heap update_many_with_outcomes omitted an updated TID",
                ));
            };
            let new_tid = outcome.new_tid;
            for idx in 0..self.update_indexes.len() {
                let old_key = change.old_keys[idx];
                let new_key = change.new_keys[idx];
                if old_key == new_key {
                    continue;
                }
                if let Some(key) = old_key {
                    let _ = self.update_indexes[idx].delete_key(
                        key,
                        change.old_tid,
                        self.delete_xmax,
                        wal,
                    )?;
                }
                if let Some(key) = new_key {
                    if self.update_indexes[idx].is_unique()
                        && self.update_indexes[idx].contains_key(key)?
                    {
                        return Err(ExecError::UniqueViolation(
                            self.update_indexes[idx].name.clone(),
                        ));
                    }
                    self.update_indexes[idx].insert_key(key, new_tid, self.delete_xmax, wal)?;
                }
            }
        }
        Ok(())
    }

    fn precheck_update_index_changes(
        &self,
        changes: &[UpdateIndexChange],
    ) -> Result<(), ExecError> {
        for change in changes {
            for idx in 0..self.update_indexes.len() {
                let Some(new_key) = change.new_keys[idx] else {
                    continue;
                };
                if change.old_keys[idx] == Some(new_key) {
                    continue;
                }
                if self.update_indexes[idx].is_unique()
                    && self.update_indexes[idx].contains_key(new_key)?
                {
                    return Err(ExecError::UniqueViolation(
                        self.update_indexes[idx].name.clone(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn extract_delete_tids_and_index_changes(
        &self,
        batch: &Batch,
        capture_deleted_rows: bool,
    ) -> Result<DeleteExtraction, ExecError> {
        let child_schema = self.child.schema().clone();
        let rows = batch_to_rows(batch, &child_schema)
            .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
        let mut tids: Vec<TupleId> = Vec::with_capacity(rows.len());
        let mut changes: Vec<DeleteIndexChange> = Vec::with_capacity(rows.len());
        let mut vector_changes: Vec<VectorDeleteIndexChange> = Vec::with_capacity(rows.len());
        let mut deleted_rows: Vec<Vec<Value>> = if capture_deleted_rows {
            Vec::with_capacity(rows.len())
        } else {
            Vec::new()
        };
        for row in &rows {
            let (tid, orig_row) = extract_tid_and_row(row, self.relation)?;
            self.check_referenced_by_delete(orig_row)?;
            let keys = self
                .delete_indexes
                .iter()
                .map(|index| index.encode_key(orig_row))
                .collect::<Result<Vec<_>, _>>()?;
            let vector_keys = self
                .delete_vector_indexes
                .iter()
                .map(|index| index.encode_key(orig_row))
                .collect::<Result<Vec<_>, _>>()?;
            tids.push(tid);
            changes.push(DeleteIndexChange { tid, keys });
            vector_changes.push(VectorDeleteIndexChange {
                tid,
                keys: vector_keys,
            });
            if capture_deleted_rows {
                deleted_rows.push(orig_row.to_vec());
            }
        }
        Ok((tids, changes, vector_changes, deleted_rows))
    }

    fn apply_delete_index_changes(
        &mut self,
        changes: &[DeleteIndexChange],
    ) -> Result<(), ExecError> {
        let wal = self.wal.clone();
        let wal_ref = wal.as_deref();
        for change in changes {
            for idx in 0..self.delete_indexes.len() {
                if let Some(key) = change.keys[idx] {
                    let _ = self.delete_indexes[idx].delete_key(
                        key,
                        change.tid,
                        self.delete_xmax,
                        wal_ref,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn apply_delete_vector_index_changes(
        &self,
        changes: &[VectorDeleteIndexChange],
    ) -> Result<(), ExecError> {
        for change in changes {
            for idx in 0..self.delete_vector_indexes.len() {
                if change.keys[idx].is_some() {
                    self.delete_vector_indexes[idx].delete_tid(change.tid)?;
                }
            }
        }
        Ok(())
    }

    fn apply_update_vector_index_changes(
        &self,
        changes: &[VectorUpdateIndexChange],
        outcomes: &[ultrasql_storage::heap::UpdateOutcome],
    ) -> Result<(), ExecError> {
        let outcome_by_old: std::collections::HashMap<
            TupleId,
            ultrasql_storage::heap::UpdateOutcome,
        > = outcomes
            .iter()
            .map(|outcome| (outcome.old_tid, *outcome))
            .collect();
        for change in changes {
            let Some(outcome) = outcome_by_old.get(&change.old_tid).copied() else {
                return Err(ExecError::Internal(
                    "heap update_many_with_outcomes omitted an updated TID",
                ));
            };
            let new_tid = outcome.new_tid;
            for idx in 0..self.update_vector_indexes.len() {
                if outcome.hot && change.old_keys[idx] == change.new_keys[idx] {
                    continue;
                }
                if change.old_keys[idx].is_some() {
                    self.update_vector_indexes[idx].delete_tid(change.old_tid)?;
                }
                if let Some(vector) = &change.new_keys[idx] {
                    self.update_vector_indexes[idx].insert_vector(vector, new_tid)?;
                }
            }
        }
        Ok(())
    }
}

/// Build the `(TupleId, new_payload_bytes)` edit list for the
/// `UPDATE t SET col_i = col_i ± lit` columnar fast path over a
/// `(Int32, Int32)` relation.
///
/// The input batch carries `[tid_block, tid_slot, id, val]` columns.
/// The output payload is 9 bytes wide:
///
/// ```text
///     byte 0    null bitmap        always 0 (both cols non-NULL)
///     bytes 1..5  id  LE i32       unchanged unless target_col == 0
///     bytes 5..9  val LE i32       unchanged unless target_col == 1
/// ```
///
/// The new value of the target column is `old + spec.delta` with overflow
/// checked before any payload is emitted. No `batch_to_rows`, no `Eval`,
/// no `RowCodec::encode` tree walk.
fn build_update_edits_int32_pair(
    batch: &Batch,
    relation: RelationId,
    spec: UpdateFastPathInt32Pair,
) -> Result<Vec<(TupleId, UpdatePayload)>, ExecError> {
    let cols = batch.columns();
    if cols.len() < 4 {
        return Err(ExecError::TypeMismatch(
            "UPDATE batch must carry [tid_block, tid_slot, id, val]".to_owned(),
        ));
    }
    let (
        Column::Int32(block_col),
        Column::Int32(slot_col),
        Column::Int32(id_col),
        Column::Int32(val_col),
    ) = (&cols[0], &cols[1], &cols[2], &cols[3])
    else {
        return Err(ExecError::TypeMismatch(
            "UPDATE fast path requires all four leading columns to be Int32".to_owned(),
        ));
    };
    let block_data = block_col.data();
    let slot_data = slot_col.data();
    let id_data = id_col.data();
    let val_data = val_col.data();
    let n = batch.rows();
    if block_data.len() < n || slot_data.len() < n || id_data.len() < n || val_data.len() < n {
        return Err(ExecError::TypeMismatch(
            "UPDATE column length shorter than batch rows".to_owned(),
        ));
    }
    let mut out: Vec<(TupleId, UpdatePayload)> = Vec::with_capacity(n);
    for i in 0..n {
        let block_u32 = u32::try_from(block_data[i]).map_err(|_| {
            ExecError::TypeMismatch(format!(
                "TID block value {} out of u32 range",
                block_data[i]
            ))
        })?;
        let slot_u16 = u16::try_from(slot_data[i]).map_err(|_| {
            ExecError::TypeMismatch(format!("TID slot value {} out of u16 range", slot_data[i]))
        })?;
        let id_v = id_data[i];
        let val_v = val_data[i];
        // Apply the assignment to the targeted column.
        let (new_id, new_val) =
            checked_update_int32_pair_add(id_v, val_v, spec.target_col_in_relation, spec.delta)?;
        // Inline 9-byte payload assembled into a `SmallVec<[u8; 16]>`
        // so the per-row encode pays no heap allocation: the entire
        // body lives in the SmallVec's inline buffer.
        let mut payload = UpdatePayload::new();
        payload.push(0_u8); // null bitmap: both non-NULL.
        payload.extend_from_slice(&new_id.to_le_bytes());
        payload.extend_from_slice(&new_val.to_le_bytes());
        let page_id =
            ultrasql_core::PageId::new(relation, ultrasql_core::BlockNumber::new(block_u32));
        out.push((TupleId::new(page_id, slot_u16), payload));
    }
    Ok(out)
}

fn checked_update_int32_pair_add(
    id: i32,
    val: i32,
    target_col: usize,
    delta: i32,
) -> Result<(i32, i32), ExecError> {
    if target_col == 0 {
        id.checked_add(delta)
            .map(|new_id| (new_id, val))
            .ok_or_else(|| ExecError::NumericFieldOverflow("Int32 id update overflow".into()))
    } else {
        val.checked_add(delta)
            .map(|new_val| (id, new_val))
            .ok_or_else(|| ExecError::NumericFieldOverflow("Int32 value update overflow".into()))
    }
}

/// Extract every `TupleId` from a `Batch` whose first two columns
/// are `tid_block: Int32` and `tid_slot: Int32` — the shape
/// `SeqScan::new_with_tids` emits for UPDATE / DELETE child operators.
///
/// Reads directly from the column arrays without materialising the
/// batch as `Vec<Vec<Value>>` (the `batch_to_rows` path the per-row
/// `extract_tid_and_row` helper used to drive). For a 10 000-row
/// DELETE this drops one full pass over the payload columns + 10 000
/// `Vec<Value>` allocations.
fn extract_tids_from_batch(batch: &Batch, relation: RelationId) -> Result<Vec<TupleId>, ExecError> {
    let cols = batch.columns();
    if cols.len() < 2 {
        return Err(ExecError::TypeMismatch(
            "DELETE batch must carry leading (tid_block, tid_slot) columns".to_owned(),
        ));
    }
    let (Column::Int32(block_col), Column::Int32(slot_col)) = (&cols[0], &cols[1]) else {
        return Err(ExecError::TypeMismatch(
            "TID columns must both be Int32".to_owned(),
        ));
    };
    let block_data = block_col.data();
    let slot_data = slot_col.data();
    let n = batch.rows();
    if block_data.len() < n || slot_data.len() < n {
        return Err(ExecError::TypeMismatch(
            "TID column length shorter than batch rows".to_owned(),
        ));
    }
    let mut out: Vec<TupleId> = Vec::with_capacity(n);
    for i in 0..n {
        let block_u32 = u32::try_from(block_data[i]).map_err(|_| {
            ExecError::TypeMismatch(format!(
                "TID block value {} out of u32 range",
                block_data[i]
            ))
        })?;
        let slot_u16 = u16::try_from(slot_data[i]).map_err(|_| {
            ExecError::TypeMismatch(format!("TID slot value {} out of u16 range", slot_data[i]))
        })?;
        let page_id =
            ultrasql_core::PageId::new(relation, ultrasql_core::BlockNumber::new(block_u32));
        out.push(TupleId::new(page_id, slot_u16));
    }
    Ok(out)
}

/// Extract a `TupleId` and the remaining column values from a row that
/// begins with `[tid_block: Int32, tid_slot: Int32, ...]`.
///
/// `relation` is the relation that owns the pages; it is embedded in the
/// returned `TupleId` via `PageId`.
fn extract_tid_and_row(
    row: &[Value],
    relation: RelationId,
) -> Result<(TupleId, &[Value]), ExecError> {
    if row.len() < 2 {
        return Err(ExecError::TypeMismatch(
            "UPDATE/DELETE input row must have at least two TID columns".to_owned(),
        ));
    }
    let block = match &row[0] {
        Value::Int32(b) => *b,
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "TID block must be Int32, got {other:?}"
            )));
        }
    };
    let slot = match &row[1] {
        Value::Int32(s) => *s,
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "TID slot must be Int32, got {other:?}"
            )));
        }
    };
    let block_u32 = u32::try_from(block).map_err(|_| {
        ExecError::TypeMismatch(format!("TID block value {block} out of u32 range"))
    })?;
    let slot_u16 = u16::try_from(slot)
        .map_err(|_| ExecError::TypeMismatch(format!("TID slot value {slot} out of u16 range")))?;
    let page_id = ultrasql_core::PageId::new(relation, ultrasql_core::BlockNumber::new(block_u32));
    let tid = TupleId::new(page_id, slot_u16);
    Ok((tid, &row[2..]))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use parking_lot::Mutex;
    use ultrasql_core::constants::PAGE_SIZE;
    use ultrasql_core::{
        BlockNumber, CommandId, DataType, Field, PageId, RelationId, Result, Schema, TupleId,
        Value, Xid,
    };
    use ultrasql_mvcc::{InfoMask, TupleHeader};
    use ultrasql_storage::btree::BTree;
    use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
    use ultrasql_storage::heap::{HeapAccess, InsertOptions, UpdateOutcome};
    use ultrasql_storage::page::Page;
    use ultrasql_storage::sequence::SequenceOptions;
    use ultrasql_storage::vm::VisibilityMap;
    use ultrasql_storage::wal_sink::test_support::InMemoryWalSink;
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::{
        DeleteIndexChange, InsertConflictAction, InsertIndexMaintainer, ModifyKind, ModifyTable,
        ModifyTableStamps, UpdateFastPathInt32Pair, UpdateIndexChange,
        build_update_edits_int32_pair, check_not_null_violations, columns_match_unordered,
        conflict_target_columns, detect_update_int32_pair_fast_path, expand_insert_row,
        extract_tid_and_row, extract_tids_from_batch, row_codec_error_to_exec, updated_ctid_target,
    };
    use crate::eval::Eval;
    use crate::mem_table_scan::MemTableScan;
    use crate::row_codec::{RowCodec, RowCodecError};
    use crate::values_scan::ValuesScan;
    use crate::{ExecError, Operator};
    use ultrasql_planner::{BinaryOp, ScalarExpr};

    type RowCheck = Arc<dyn Fn(&[Value]) -> std::result::Result<(), ExecError> + Send + Sync>;
    type UpdateCheck =
        Arc<dyn Fn(&[Value], &[Value]) -> std::result::Result<(), ExecError> + Send + Sync>;

    // -----------------------------------------------------------------------
    // In-memory heap fixtures (duplicated from seq_scan tests)
    // -----------------------------------------------------------------------

    #[derive(Default, Debug)]
    struct MapLoader {
        store: Mutex<HashMap<PageId, Box<[u8; PAGE_SIZE]>>>,
    }

    impl MapLoader {
        fn new() -> Self {
            Self::default()
        }
    }

    impl PageLoader for MapLoader {
        fn load(&self, page_id: PageId) -> Result<Page> {
            let stored = {
                let store = self.store.lock();
                store.get(&page_id).map(|b| {
                    let mut copy: Box<[u8; PAGE_SIZE]> = vec![0_u8; PAGE_SIZE]
                        .into_boxed_slice()
                        .try_into()
                        .expect("alloc matches PAGE_SIZE");
                    copy.copy_from_slice(&**b);
                    copy
                })
            };
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

    fn rel() -> RelationId {
        RelationId::new(42)
    }

    fn make_heap() -> Arc<HeapAccess<MapLoader>> {
        let pool = Arc::new(BufferPool::new(64, MapLoader::new()));
        Arc::new(HeapAccess::new(pool))
    }

    fn schema_i32_text() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("name", DataType::Text { max_len: None }),
        ])
        .expect("schema ok")
    }

    fn lit_i32(v: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            data_type: DataType::Int32,
        }
    }

    fn lit_text(s: &str) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Text(s.to_owned()),
            data_type: DataType::Text { max_len: None },
        }
    }

    fn lit_bool(v: bool) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Bool(v),
            data_type: DataType::Bool,
        }
    }

    fn schema_i32_pair() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("val", DataType::Int32),
        ])
        .expect("schema ok")
    }

    fn col_i32(name: &str, index: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.to_owned(),
            index,
            data_type: DataType::Int32,
        }
    }

    fn col_text(name: &str, index: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.to_owned(),
            index,
            data_type: DataType::Text { max_len: None },
        }
    }

    fn binary_i32(op: BinaryOp, left: ScalarExpr, right: ScalarExpr) -> ScalarExpr {
        ScalarExpr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
            data_type: DataType::Int32,
        }
    }

    fn tid(block: u32, slot: u16) -> TupleId {
        TupleId::new(PageId::new(rel(), BlockNumber::new(block)), slot)
    }

    fn stamps(xid: u64) -> ModifyTableStamps {
        ModifyTableStamps::new(
            Xid::new(xid),
            CommandId::FIRST,
            Xid::new(xid),
            CommandId::FIRST,
        )
    }

    fn tid_row_schema(relation_schema: &Schema) -> Schema {
        let mut fields = vec![
            Field::required("tid_block", DataType::Int32),
            Field::required("tid_slot", DataType::Int32),
        ];
        fields.extend(relation_schema.fields().iter().cloned());
        Schema::new(fields).expect("tid schema")
    }

    fn insert_payload(heap: &HeapAccess<MapLoader>, schema: &Schema, row: &[Value]) -> TupleId {
        let codec = RowCodec::new(schema.clone());
        let payload = codec.encode(row).expect("payload");
        let tids = heap
            .insert_batch(
                rel(),
                &[payload.as_slice()],
                InsertOptions {
                    xmin: Xid::new(1),
                    command_id: CommandId::FIRST,
                    wal: None,
                    fsm: None,
                    vm: None,
                },
            )
            .expect("insert row");
        tids[0]
    }

    fn btree_index(name: &str, unique: bool) -> InsertIndexMaintainer<MapLoader> {
        let pool = Arc::new(BufferPool::new(64, MapLoader::new()));
        let tree = BTree::create(pool, RelationId::new(100)).expect("btree");
        InsertIndexMaintainer::new(
            name,
            tree,
            Arc::new(|row| match row.first() {
                Some(Value::Int32(v)) => Ok(Some(i64::from(*v))),
                Some(Value::Int64(v)) => Ok(Some(*v)),
                Some(Value::Null) => Ok(None),
                other => Err(ExecError::TypeMismatch(format!("bad key {other:?}"))),
            }),
            unique,
        )
        .with_key_columns(vec![0])
    }

    // -----------------------------------------------------------------------
    // Test 1: insert writes each input row to heap and reports count
    // -----------------------------------------------------------------------

    #[test]
    fn insert_writes_each_input_row_to_heap_and_reports_count() {
        let heap = make_heap();
        let schema = schema_i32_text();
        let wal = Arc::new(InMemoryWalSink::new());

        // Source: 3 rows via ValuesScan.
        let rows = vec![
            vec![lit_i32(1), lit_text("alice")],
            vec![lit_i32(2), lit_text("bob")],
            vec![lit_i32(3), lit_text("carol")],
        ];
        let source = ValuesScan::new(rows, schema.clone());

        let mut op = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            schema,
            ModifyKind::Insert,
            stamps(10),
            Some(Arc::clone(&wal) as Arc<dyn ultrasql_storage::wal_sink::WalSink>),
            Box::new(source),
        );

        // Drain the operator.
        let batch = op
            .next_batch()
            .expect("must not error")
            .expect("must emit batch");
        assert_eq!(batch.rows(), 1, "expected single affected-rows batch");
        match &batch.columns()[0] {
            Column::Int64(c) => assert_eq!(c.data(), &[3_i64], "expected 3 affected rows"),
            other => panic!("unexpected column: {other:?}"),
        }
        assert!(
            op.next_batch().unwrap().is_none(),
            "must return None after emit"
        );

        // Verify 3 rows are present in the heap.
        assert_eq!(heap.block_count(rel()), 1, "one block should be allocated");
    }

    #[test]
    fn insert_rejects_affected_row_counter_overflow() {
        let heap = make_heap();
        let schema = schema_i32_text();
        let rows = vec![vec![lit_i32(1), lit_text("alice")]];
        let source = ValuesScan::new(rows, schema.clone());

        let mut op = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            schema,
            ModifyKind::Insert,
            stamps(10),
            None,
            Box::new(source),
        );
        op.affected = i64::MAX;

        let err = op
            .next_batch()
            .expect_err("affected row count overflow must not clamp");
        assert!(matches!(err, ExecError::NumericFieldOverflow(_)));
    }

    // -----------------------------------------------------------------------
    // Test 2: insert emits a WAL record per inserted row
    // -----------------------------------------------------------------------

    #[test]
    fn insert_emits_wal_record_per_inserted_row() {
        let heap = make_heap();
        let schema = schema_i32_text();
        let wal = Arc::new(InMemoryWalSink::new());

        let rows = vec![
            vec![lit_i32(1), lit_text("x")],
            vec![lit_i32(2), lit_text("y")],
        ];
        let source = ValuesScan::new(rows, schema.clone());

        let mut op = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            schema,
            ModifyKind::Insert,
            stamps(20),
            Some(Arc::clone(&wal) as Arc<dyn ultrasql_storage::wal_sink::WalSink>),
            Box::new(source),
        );

        op.next_batch().unwrap();

        // Each insert should emit exactly one WAL record.
        assert_eq!(wal.len(), 2, "expected 2 WAL records");
    }

    // -----------------------------------------------------------------------
    // Test 3: empty input reports zero affected rows
    // -----------------------------------------------------------------------

    #[test]
    fn insert_empty_input_reports_zero() {
        let heap = make_heap();
        let schema = schema_i32_text();
        let source = ValuesScan::new(vec![], schema.clone());

        let mut op = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            schema,
            ModifyKind::Insert,
            stamps(30),
            None,
            Box::new(source),
        );

        let batch = op.next_batch().unwrap().unwrap();
        match &batch.columns()[0] {
            Column::Int64(c) => assert_eq!(c.data(), &[0_i64]),
            other => panic!("unexpected column: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 4: schema reports affected_rows column
    // -----------------------------------------------------------------------

    #[test]
    fn modify_table_schema_is_affected_rows() {
        let heap = make_heap();
        let schema = schema_i32_text();
        let source = MemTableScan::new(schema.clone(), vec![]);
        let op = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            schema,
            ModifyKind::Insert,
            stamps(1),
            None,
            Box::new(source),
        );
        assert_eq!(op.schema().len(), 1);
        assert_eq!(op.schema().field_at(0).name, "affected_rows");
        assert_eq!(op.schema().field_at(0).data_type, DataType::Int64);
    }

    #[test]
    fn modify_table_builder_methods_store_runtime_descriptors() {
        let heap = make_heap();
        let schema = schema_i32_text();
        let source = MemTableScan::new(schema.clone(), vec![]);
        let wal = Arc::new(InMemoryWalSink::new()) as Arc<dyn ultrasql_storage::wal_sink::WalSink>;
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observed_clone = Arc::clone(&observed);
        let sequence = Arc::new(
            ultrasql_storage::sequence::Sequence::new(SequenceOptions::default())
                .expect("sequence"),
        );
        let sequence_default = super::SequenceDefault::new("users_id_seq", sequence)
            .with_observer(Arc::new(move |name, value| {
                observed_clone.lock().push((name.to_owned(), value));
            }))
            .with_wal(Some(Arc::clone(&wal)), Xid::new(9), rel());
        let returning_schema =
            Schema::new([Field::required("id", DataType::Int32)]).expect("returning");
        let row_check: RowCheck = Arc::new(|_| Ok(()));
        let update_check: UpdateCheck = Arc::new(|_, _| Ok(()));

        let op = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            schema,
            ModifyKind::Insert,
            stamps(1),
            None,
            Box::new(source),
        )
        .with_visibility_map(Arc::new(VisibilityMap::new()))
        .with_insert_conflict_action(InsertConflictAction::DoNothing {
            target: Some(vec![0]),
        })
        .with_insert_column_map(vec![1, 0])
        .with_column_defaults(vec![Some(lit_i32(7)), None])
        .with_sequence_defaults(vec![Some(sequence_default), None])
        .with_identity_always(vec![true, false])
        .with_generated_stored(vec![None, Some(lit_text("stored"))])
        .with_check_constraints(vec![("ck_true".to_owned(), lit_bool(true))])
        .with_foreign_key_checks(vec![Arc::clone(&row_check)])
        .with_exclusion_checks(vec![Arc::clone(&row_check)])
        .with_exclusion_update_checks(vec![Arc::clone(&update_check)])
        .with_referenced_by_delete_checks(vec![Arc::clone(&row_check)])
        .with_referenced_by_update_checks(vec![update_check])
        .with_returning(vec![col_i32("id", 0)], returning_schema);

        assert!(op.vm.is_some());
        assert!(matches!(
            op.insert_conflict_action,
            Some(InsertConflictAction::DoNothing { .. })
        ));
        assert_eq!(op.insert_column_map.as_deref(), Some(&[1, 0][..]));
        assert_eq!(op.column_defaults.len(), 2);
        assert_eq!(op.sequence_defaults.len(), 2);
        assert_eq!(op.identity_always, vec![true, false]);
        assert_eq!(op.generated_stored.len(), 2);
        assert_eq!(op.check_constraints[0].name, "ck_true");
        assert_eq!(op.foreign_key_checks.len(), 1);
        assert_eq!(op.exclusion_checks.len(), 1);
        assert_eq!(op.exclusion_update_checks.len(), 1);
        assert_eq!(op.referenced_by_delete_checks.len(), 1);
        assert_eq!(op.referenced_by_update_checks.len(), 1);
        assert_eq!(op.returning_evaluators.len(), 1);
        assert_eq!(op.schema.field_at(0).name, "id");
        assert!(observed.lock().is_empty());
    }

    #[test]
    fn not_null_and_row_codec_errors_map_to_sql_errors() {
        let schema = schema_i32_text();
        let err = check_not_null_violations(&[Value::Int32(1), Value::Null], &schema)
            .expect_err("not null");
        assert!(matches!(err, ExecError::NotNullViolation(ref col) if col == "name"));
        check_not_null_violations(&[Value::Int32(1), Value::Text("ok".to_owned())], &schema)
            .expect("valid row");

        let trunc = row_codec_error_to_exec(RowCodecError::StringDataRightTruncation {
            column: 1,
            ty: DataType::Char { len: Some(2) },
            detail: "too long".to_owned(),
        });
        assert!(
            matches!(trunc, ExecError::StringDataRightTruncation(ref detail) if detail == "too long")
        );

        let ty = row_codec_error_to_exec(RowCodecError::Arity { schema: 2, row: 1 });
        assert!(matches!(ty, ExecError::TypeMismatch(_)));
    }

    #[test]
    fn update_int32_pair_fast_path_detection_covers_supported_shapes() {
        let schema = schema_i32_pair();
        let add_right = binary_i32(BinaryOp::Add, col_i32("val", 1), lit_i32(3));
        let spec = detect_update_int32_pair_fast_path(&[(1, add_right)], &schema).expect("add");
        assert_eq!(spec.target_col_in_relation, 1);
        assert_eq!(spec.delta, 3);

        let add_left = binary_i32(BinaryOp::Add, lit_i32(4), col_i32("id", 0));
        let spec = detect_update_int32_pair_fast_path(&[(0, add_left)], &schema).expect("add");
        assert_eq!(spec.target_col_in_relation, 0);
        assert_eq!(spec.delta, 4);

        let sub = binary_i32(BinaryOp::Sub, col_i32("val", 1), lit_i32(5));
        let spec = detect_update_int32_pair_fast_path(&[(1, sub)], &schema).expect("sub");
        assert_eq!(spec.delta, -5);

        let lit_minus_col = binary_i32(BinaryOp::Sub, lit_i32(5), col_i32("val", 1));
        assert!(detect_update_int32_pair_fast_path(&[(1, lit_minus_col)], &schema).is_none());
        assert!(
            detect_update_int32_pair_fast_path(
                &[(2, binary_i32(BinaryOp::Add, col_i32("val", 1), lit_i32(1)))],
                &schema
            )
            .is_none()
        );
        assert!(detect_update_int32_pair_fast_path(&[], &schema).is_none());
        assert!(
            detect_update_int32_pair_fast_path(
                &[(1, binary_i32(BinaryOp::Gt, col_i32("val", 1), lit_i32(1)))],
                &schema
            )
            .is_none()
        );
    }

    #[test]
    fn tid_extraction_and_update_fast_payloads_validate_shapes() {
        let batch = Batch::new([
            Column::Int32(NumericColumn::from_data(vec![2, 3])),
            Column::Int32(NumericColumn::from_data(vec![7, 8])),
            Column::Int32(NumericColumn::from_data(vec![10, 20])),
            Column::Int32(NumericColumn::from_data(vec![100, 200])),
        ])
        .expect("batch");
        let edits = build_update_edits_int32_pair(
            &batch,
            rel(),
            UpdateFastPathInt32Pair {
                target_col_in_relation: 1,
                delta: 5,
            },
        )
        .expect("edits");
        assert_eq!(edits[0].0, tid(2, 7));
        assert_eq!(edits[0].1.as_slice(), &[0, 10, 0, 0, 0, 105, 0, 0, 0]);

        let tids = extract_tids_from_batch(&batch, rel()).expect("tids");
        assert_eq!(tids, vec![tid(2, 7), tid(3, 8)]);

        let tid_row = [Value::Int32(4), Value::Int32(9), Value::Text("x".into())];
        let (one_tid, row) = extract_tid_and_row(&tid_row, rel()).expect("tid row");
        assert_eq!(one_tid, tid(4, 9));
        assert_eq!(row, &[Value::Text("x".to_owned())]);

        let bad_short =
            Batch::new([Column::Int32(NumericColumn::from_data(vec![1]))]).expect("batch");
        assert!(extract_tids_from_batch(&bad_short, rel()).is_err());
        assert!(
            build_update_edits_int32_pair(
                &bad_short,
                rel(),
                UpdateFastPathInt32Pair {
                    target_col_in_relation: 0,
                    delta: 1,
                },
            )
            .is_err()
        );

        let bad_negative = Batch::new([
            Column::Int32(NumericColumn::from_data(vec![-1])),
            Column::Int32(NumericColumn::from_data(vec![1])),
        ])
        .expect("batch");
        assert!(extract_tids_from_batch(&bad_negative, rel()).is_err());

        assert!(extract_tid_and_row(&[Value::Text("bad".into())], rel()).is_err());
        assert!(extract_tid_and_row(&[Value::Int32(-1), Value::Int32(1)], rel()).is_err());
        assert!(extract_tid_and_row(&[Value::Int32(1), Value::Int32(70_000)], rel()).is_err());
    }

    #[test]
    fn update_fast_payloads_reject_int32_overflow() {
        let batch = Batch::new([
            Column::Int32(NumericColumn::from_data(vec![2])),
            Column::Int32(NumericColumn::from_data(vec![7])),
            Column::Int32(NumericColumn::from_data(vec![10])),
            Column::Int32(NumericColumn::from_data(vec![i32::MAX])),
        ])
        .expect("batch");

        let err = build_update_edits_int32_pair(
            &batch,
            rel(),
            UpdateFastPathInt32Pair {
                target_col_in_relation: 1,
                delta: 1,
            },
        )
        .expect_err("overflow must reject fast update payload");
        assert!(matches!(err, ExecError::NumericFieldOverflow(_)), "{err:?}");
    }

    #[test]
    fn conflict_targets_and_ctid_redirect_helpers_cover_edge_cases() {
        assert!(columns_match_unordered(&[2, 1], &[1, 2]));
        assert!(!columns_match_unordered(&[1, 2], &[1, 3]));

        let do_nothing = InsertConflictAction::DoNothing {
            target: Some(vec![1, 2]),
        };
        assert_eq!(conflict_target_columns(&do_nothing), Some(&[1, 2][..]));
        let do_nothing_any = InsertConflictAction::DoNothing { target: None };
        assert!(conflict_target_columns(&do_nothing_any).is_none());
        let do_update = InsertConflictAction::DoUpdate {
            target: vec![0],
            assignments: Vec::new(),
            predicate: None,
        };
        assert_eq!(conflict_target_columns(&do_update), Some(&[0][..]));

        let current = tid(1, 1);
        let next = tid(1, 2);
        let mut header = TupleHeader::fresh(Xid::new(1), CommandId::FIRST, current, 2);
        header.ctid = next;
        assert_eq!(updated_ctid_target(&header, current), None);
        header.infomask.set(InfoMask::UPDATED);
        assert_eq!(updated_ctid_target(&header, current), Some(next));
        header.ctid = current;
        assert_eq!(updated_ctid_target(&header, current), None);
    }

    #[test]
    fn insert_column_map_sequence_generated_constraints_and_returning() {
        let heap = make_heap();
        let target_schema = Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("name", DataType::Text { max_len: None }),
            Field::required("stored", DataType::Text { max_len: None }),
        ])
        .expect("target schema");
        let source_schema =
            Schema::new([Field::required("name", DataType::Text { max_len: None })])
                .expect("source schema");
        let source = ValuesScan::new(vec![vec![lit_text("alpha")]], source_schema);

        let observed = Arc::new(Mutex::new(Vec::new()));
        let observed_clone = Arc::clone(&observed);
        let wal = Arc::new(InMemoryWalSink::new()) as Arc<dyn ultrasql_storage::wal_sink::WalSink>;
        let sequence = Arc::new(
            ultrasql_storage::sequence::Sequence::new(SequenceOptions::default())
                .expect("sequence"),
        );
        let sequence_default = super::SequenceDefault::new("users_id_seq", sequence)
            .with_observer(Arc::new(move |name, value| {
                observed_clone.lock().push((name.to_owned(), value));
            }))
            .with_wal(Some(Arc::clone(&wal)), Xid::new(11), rel());
        let fk_hits = Arc::new(Mutex::new(0_usize));
        let fk_hits_clone = Arc::clone(&fk_hits);
        let fk: RowCheck = Arc::new(move |row| {
            assert_eq!(row[1], Value::Text("alpha".to_owned()));
            *fk_hits_clone.lock() += 1;
            Ok(())
        });
        let exclusion_hits = Arc::new(Mutex::new(0_usize));
        let exclusion_hits_clone = Arc::clone(&exclusion_hits);
        let exclusion: RowCheck = Arc::new(move |row| {
            assert_eq!(row[2], Value::Text("stored".to_owned()));
            *exclusion_hits_clone.lock() += 1;
            Ok(())
        });
        let returning_schema = Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("stored", DataType::Text { max_len: None }),
        ])
        .expect("returning schema");

        let mut op = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            target_schema,
            ModifyKind::Insert,
            stamps(11),
            Some(wal),
            Box::new(source),
        )
        .with_insert_column_map(vec![1])
        .with_sequence_defaults(vec![Some(sequence_default), None, None])
        .with_identity_always(vec![true, false, false])
        .with_generated_stored(vec![None, None, Some(lit_text("stored"))])
        .with_check_constraints(vec![("ck_true".to_owned(), lit_bool(true))])
        .with_foreign_key_checks(vec![fk])
        .with_exclusion_checks(vec![exclusion])
        .with_returning(
            vec![col_i32("id", 0), col_text("stored", 2)],
            returning_schema,
        );

        let batch = op.next_batch().expect("insert").expect("returning");
        assert_eq!(batch.rows(), 1);
        match &batch.columns()[0] {
            Column::Int32(c) => assert_eq!(c.data(), &[1]),
            other => panic!("unexpected id column {other:?}"),
        }
        assert_eq!(batch.columns()[1].text_value(0), Some("stored"));
        assert_eq!(&*observed.lock(), &[("users_id_seq".to_owned(), 1)]);
        assert_eq!(*fk_hits.lock(), 1);
        assert_eq!(*exclusion_hits.lock(), 1);
    }

    #[test]
    fn update_and_delete_operator_paths_cover_slow_branches_and_returning() {
        let heap = make_heap();
        let schema = schema_i32_text();
        let old_tid = insert_payload(
            &heap,
            &schema,
            &[Value::Int32(1), Value::Text("old".to_owned())],
        );
        let child_schema = tid_row_schema(&schema);
        let update_source = ValuesScan::new(
            vec![vec![
                lit_i32(i32::try_from(old_tid.page.block.raw()).expect("block fits")),
                lit_i32(i32::from(old_tid.slot)),
                lit_i32(1),
                lit_text("old"),
            ]],
            child_schema.clone(),
        );
        let returning_schema =
            Schema::new([Field::required("name", DataType::Text { max_len: None })])
                .expect("returning");
        let mut update = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            schema.clone(),
            ModifyKind::Update {
                assignments: vec![(1, lit_text("new"))],
            },
            stamps(2),
            None,
            Box::new(update_source),
        )
        .with_returning(vec![col_text("name", 1)], returning_schema);

        let batch = update.next_batch().expect("update").expect("returning");
        assert_eq!(batch.rows(), 1);
        assert_eq!(batch.columns()[0].text_value(0), Some("new"));

        let delete_tid = insert_payload(
            &heap,
            &schema,
            &[Value::Int32(2), Value::Text("gone".to_owned())],
        );
        let delete_source = ValuesScan::new(
            vec![vec![
                lit_i32(i32::try_from(delete_tid.page.block.raw()).expect("block fits")),
                lit_i32(i32::from(delete_tid.slot)),
                lit_i32(2),
                lit_text("gone"),
            ]],
            child_schema,
        );
        let returning_schema =
            Schema::new([Field::required("id", DataType::Int32)]).expect("returning");
        let mut delete = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            schema,
            ModifyKind::Delete,
            stamps(3),
            None,
            Box::new(delete_source),
        )
        .with_returning(vec![col_i32("id", 0)], returning_schema);

        let batch = delete.next_batch().expect("delete").expect("returning");
        assert_eq!(batch.rows(), 1);
        match &batch.columns()[0] {
            Column::Int32(c) => assert_eq!(c.data(), &[2]),
            other => panic!("unexpected returning column {other:?}"),
        }
    }

    #[test]
    fn update_conflict_defaults_and_constraint_helpers_cover_error_edges() {
        let heap = make_heap();
        let schema = schema_i32_text();
        let child = MemTableScan::new(tid_row_schema(&schema), vec![]);
        let update_op = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            schema.clone(),
            ModifyKind::Update {
                assignments: vec![(1, lit_text("computed"))],
            },
            stamps(4),
            None,
            Box::new(child),
        );
        let update_row = [
            Value::Int32(0),
            Value::Int32(7),
            Value::Int32(9),
            Value::Text("before".to_owned()),
        ];
        let computed = update_op
            .compute_update_edit(&update_row, true)
            .expect("computed update");
        assert_eq!(computed.tid, tid(0, 7));
        assert_eq!(
            computed.returning_row,
            Some(vec![Value::Int32(9), Value::Text("computed".to_owned())])
        );

        let conflict = update_op
            .compute_conflict_update_edit(
                tid(0, 8),
                &[Value::Int32(1), Value::Text("old".to_owned())],
                &[Value::Int32(1), Value::Text("excluded".to_owned())],
                &[(1, Eval::new(lit_text("merged")))],
                Some(&Eval::new(lit_bool(true))),
                true,
            )
            .expect("conflict update")
            .expect("updated");
        assert_eq!(
            conflict.returning_row,
            Some(vec![Value::Int32(1), Value::Text("merged".to_owned())])
        );
        assert!(
            update_op
                .compute_conflict_update_edit(
                    tid(0, 8),
                    &[Value::Int32(1), Value::Text("old".to_owned())],
                    &[Value::Int32(1), Value::Text("excluded".to_owned())],
                    &[(1, Eval::new(lit_text("merged")))],
                    Some(&Eval::new(lit_bool(false))),
                    false,
                )
                .expect("predicate false")
                .is_none()
        );
        assert!(
            update_op
                .compute_conflict_update_edit(
                    tid(0, 8),
                    &[Value::Int32(1), Value::Text("old".to_owned())],
                    &[Value::Int32(1), Value::Text("excluded".to_owned())],
                    &[(1, Eval::new(lit_text("merged")))],
                    Some(&Eval::new(lit_i32(1))),
                    false,
                )
                .is_err()
        );

        let insert_op = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            schema.clone(),
            ModifyKind::Insert,
            stamps(5),
            None,
            Box::new(MemTableScan::new(schema.clone(), vec![])),
        )
        .with_column_defaults(vec![Some(lit_i32(42)), None]);
        let mut row = vec![Value::Null, Value::Text("kept".to_owned())];
        insert_op
            .apply_insert_defaults(&mut row, &[true, false])
            .expect("defaults");
        assert_eq!(row[0], Value::Int32(42));
        assert!(insert_op.apply_insert_defaults(&mut row, &[true]).is_err());

        let generated_op = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            schema.clone(),
            ModifyKind::Insert,
            stamps(6),
            None,
            Box::new(MemTableScan::new(schema.clone(), vec![])),
        )
        .with_generated_stored(vec![None, Some(lit_text("stored"))])
        .with_identity_always(vec![true, false]);
        assert!(
            generated_op
                .check_identity_explicit_values(&[false, true])
                .is_err()
        );
        assert!(
            generated_op
                .check_generated_stored_explicit_values(&[true, false])
                .is_err()
        );
        let mut generated_row = vec![Value::Int32(1), Value::Null];
        generated_op
            .apply_generated_stored(&mut generated_row)
            .expect("generated");
        assert_eq!(generated_row[1], Value::Text("stored".to_owned()));
        assert!(
            generated_op
                .apply_generated_stored(&mut [Value::Int32(1)])
                .is_err()
        );

        let check_false = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            schema.clone(),
            ModifyKind::Insert,
            stamps(7),
            None,
            Box::new(MemTableScan::new(schema.clone(), vec![])),
        )
        .with_check_constraints(vec![("ck_false".to_owned(), lit_bool(false))]);
        assert!(matches!(
            check_false.check_row_constraints(&[Value::Int32(1), Value::Text("x".to_owned())]),
            Err(ExecError::CheckViolation(ref name)) if name == "ck_false"
        ));
        let check_type = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            schema.clone(),
            ModifyKind::Insert,
            stamps(8),
            None,
            Box::new(MemTableScan::new(schema, vec![])),
        )
        .with_check_constraints(vec![("ck_type".to_owned(), lit_i32(1))]);
        assert!(
            check_type
                .check_row_constraints(&[Value::Int32(1), Value::Text("x".to_owned())])
                .is_err()
        );

        let expanded = expand_insert_row(&[Value::Int32(3)], 2, &[1]).expect("expanded");
        assert_eq!(expanded.values, vec![Value::Null, Value::Int32(3)]);
        assert_eq!(expanded.omitted, vec![true, false]);
        assert!(expand_insert_row(&[Value::Int32(1)], 2, &[2]).is_err());
        assert!(expand_insert_row(&[Value::Int32(1)], 2, &[0, 0]).is_err());

        let int64_schema =
            Schema::new([Field::required("seq", DataType::Int64)]).expect("int64 schema");
        let seq_op = ModifyTable::new(
            heap,
            rel(),
            int64_schema,
            ModifyKind::Insert,
            stamps(9),
            None,
            Box::new(MemTableScan::new(
                Schema::new([Field::required("seq", DataType::Int64)]).expect("source"),
                vec![],
            )),
        );
        let seq = Arc::new(
            ultrasql_storage::sequence::Sequence::new(SequenceOptions {
                start: 9,
                ..SequenceOptions::default()
            })
            .expect("sequence"),
        );
        let default = super::SequenceDefault::new("s", seq);
        assert_eq!(
            seq_op
                .next_sequence_default_value(0, &default)
                .expect("seq"),
            Value::Int64(9)
        );
    }

    #[test]
    fn btree_index_conflict_and_maintenance_helpers_cover_index_paths() {
        let heap = make_heap();
        let schema = schema_i32_text();
        let existing = tid(0, 1);
        let mut index = btree_index("idx_users_id", true);
        assert!(format!("{index:?}").contains("idx_users_id"));
        assert_eq!(
            index
                .encode_key(&[Value::Int32(7), Value::Text("x".to_owned())])
                .expect("key"),
            Some(7)
        );
        assert!(!index.contains_key(7).expect("missing"));
        index
            .insert_key(7, existing, Xid::new(10), None)
            .expect("insert key");
        assert!(index.contains_key(7).expect("present"));
        assert!(matches!(
            index.insert_key(7, tid(0, 2), Xid::new(10), None),
            Err(ExecError::UniqueViolation(ref name)) if name == "idx_users_id"
        ));
        assert!(
            index
                .delete_key(7, existing, Xid::new(11), None)
                .expect("delete key")
        );
        assert!(!index.contains_key(7).expect("deleted"));

        let mut conflict_index = btree_index("idx_conflict", true);
        conflict_index
            .insert_key(7, existing, Xid::new(12), None)
            .expect("seed conflict");
        let op = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            schema.clone(),
            ModifyKind::Insert,
            stamps(12),
            None,
            Box::new(MemTableScan::new(schema.clone(), vec![])),
        )
        .with_insert_indexes(vec![conflict_index]);
        let action = InsertConflictAction::DoNothing {
            target: Some(vec![0]),
        };
        op.validate_insert_conflict_arbiter(Some(&action))
            .expect("arbiter");
        assert!(
            op.validate_insert_conflict_arbiter(Some(&InsertConflictAction::DoNothing {
                target: Some(vec![1])
            }))
            .is_err()
        );
        assert!(matches!(
            op.find_insert_conflict(&action, &[Some(7)], &[HashSet::new()])
                .expect("existing"),
            Some(super::InsertConflict::Existing(t)) if t == existing
        ));
        let mut seen = vec![HashSet::new()];
        seen[0].insert(8);
        assert!(matches!(
            op.find_insert_conflict(&action, &[Some(8)], &seen)
                .expect("in batch"),
            Some(super::InsertConflict::InBatch)
        ));
        op.remember_insert_keys(&[Some(9)], &mut seen);
        assert!(seen[0].contains(&9));
        let mut duplicate_seen = vec![HashSet::new()];
        op.reject_duplicate_insert_keys(&[Some(10)], &mut duplicate_seen)
            .expect("first key");
        assert!(
            op.reject_duplicate_insert_keys(&[Some(10)], &mut duplicate_seen)
                .is_err()
        );

        let mut update_index = btree_index("idx_update", true);
        let old_tid = tid(0, 3);
        let new_tid = tid(0, 4);
        update_index
            .insert_key(1, old_tid, Xid::new(13), None)
            .expect("old key");
        let mut update_op = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            schema.clone(),
            ModifyKind::Update {
                assignments: vec![(0, lit_i32(2))],
            },
            stamps(13),
            None,
            Box::new(MemTableScan::new(tid_row_schema(&schema), vec![])),
        )
        .with_update_indexes(vec![update_index]);
        let changes = vec![UpdateIndexChange {
            old_tid,
            old_keys: vec![Some(1)],
            new_keys: vec![Some(2)],
        }];
        update_op
            .precheck_update_index_changes(&changes)
            .expect("precheck");
        update_op
            .apply_update_index_changes(
                &changes,
                &[UpdateOutcome {
                    old_tid,
                    new_tid,
                    hot: false,
                }],
                None,
            )
            .expect("apply update index");
        assert!(
            !update_op.update_indexes[0]
                .contains_key(1)
                .expect("old gone")
        );
        assert!(
            update_op.update_indexes[0]
                .contains_key(2)
                .expect("new key")
        );

        let mut delete_index = btree_index("idx_delete", true);
        delete_index
            .insert_key(5, old_tid, Xid::new(14), None)
            .expect("delete seed");
        let mut delete_op = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            schema,
            ModifyKind::Delete,
            stamps(14),
            None,
            Box::new(MemTableScan::new(
                Schema::new([
                    Field::required("tid_block", DataType::Int32),
                    Field::required("tid_slot", DataType::Int32),
                ])
                .expect("delete source"),
                vec![],
            )),
        )
        .with_delete_indexes(vec![delete_index]);
        delete_op
            .apply_delete_index_changes(&[DeleteIndexChange {
                tid: old_tid,
                keys: vec![Some(5)],
            }])
            .expect("delete index");
        assert!(
            !delete_op.delete_indexes[0]
                .contains_key(5)
                .expect("delete gone")
        );
    }
}
