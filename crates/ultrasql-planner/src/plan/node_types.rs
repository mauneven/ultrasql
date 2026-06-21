//! Supporting types for query and statement-control logical plan nodes.
//!
//! These enums and structs are referenced by [`LogicalPlan`](super::LogicalPlan)
//! variants in the query / DML / transaction-control families. They were
//! split out of the original monolithic `plan.rs` verbatim.

use ultrasql_core::{DataType, Schema, Value};

use crate::expr::ScalarExpr;

// ============================================================================
// Join types
// ============================================================================

/// Logical join type.
///
/// These match the SQL standard join modifiers plus logical semi/anti joins
/// introduced by subquery decorrelation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalJoinType {
    /// `[INNER] JOIN` — only rows with matches on both sides.
    Inner,
    /// `LEFT [OUTER] JOIN` — all left rows; unmatched right columns are NULL.
    LeftOuter,
    /// `RIGHT [OUTER] JOIN` — all right rows; unmatched left columns are NULL.
    RightOuter,
    /// `FULL [OUTER] JOIN` — all rows from both sides; unmatched columns are NULL.
    FullOuter,
    /// `CROSS JOIN` or comma-separated table factor — Cartesian product.
    Cross,
    /// Semi join — emit each left row that has at least one right match.
    Semi,
    /// Anti join — emit each left row that has no right match.
    Anti,
}

/// Resolved join condition.
///
/// `On` carries a bound scalar predicate. `Using` encodes the matched
/// column index pairs `(left_idx, right_idx)` for collapsed-column USING
/// semantics. `None` means CROSS JOIN.
#[derive(Clone, Debug, PartialEq)]
pub enum LogicalJoinCondition {
    /// `ON expr` — an explicit join predicate over the concatenated schema.
    On(ScalarExpr),
    /// `USING (col, …)` — each pair is `(left_column_index, right_column_index)`.
    ///
    /// The output schema exposes the joined column once (from the left side).
    Using(Vec<(usize, usize)>),
    /// No condition (CROSS JOIN).
    None,
}

// ============================================================================
// Set-operation types
// ============================================================================

/// Set operation kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalSetOp {
    /// `UNION` — union of the two row sets.
    Union,
    /// `INTERSECT` — intersection of the two row sets.
    Intersect,
    /// `EXCEPT` — rows in left but not in right.
    Except,
}

/// Set quantifier applied to a set operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalSetQuantifier {
    /// `DISTINCT` (default per SQL standard) — duplicates are removed.
    Distinct,
    /// `ALL` — duplicates are preserved.
    All,
}

/// Index access method requested by `CREATE INDEX ... USING`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LogicalIndexMethod {
    /// B-tree method.
    #[default]
    Btree,
    /// Equality-only hash method. UltraSQL stores hash buckets in the
    /// existing page-backed index substrate in this wave.
    Hash,
    /// Generalized inverted index method.
    Gin,
    /// Generalized search tree method.
    Gist,
    /// Block range index method.
    Brin,
    /// Hierarchical navigable small world vector index method.
    Hnsw,
    /// Inverted-file flat vector index method.
    IvfFlat,
    /// Runtime aggregating-index summary method.
    Aggregating,
}

/// Bound `CREATE INDEX ... WITH (...)` storage option.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalIndexOption {
    /// Lowercase option name.
    pub name: String,
    /// Literal option value rendered as text.
    pub value: String,
}

/// Bound `ALTER TABLE ... SET (...)` relation storage option.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalTableOption {
    /// Lowercase option name.
    pub name: String,
    /// Literal option value rendered as text.
    pub value: String,
}

/// Bound metadata for `CREATE AGGREGATING INDEX`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalAggregatingIndex {
    /// Group-key table column indices, in declaration order.
    pub group_columns: Vec<usize>,
    /// Aggregate summaries maintained per group.
    pub aggregates: Vec<LogicalAggregatingIndexExpr>,
}

/// One aggregate summary stored by an aggregating index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalAggregatingIndexExpr {
    /// Aggregate function.
    pub func: AggregateFunc,
    /// Table column index for argument aggregates; `None` for `COUNT(*)`.
    pub arg_column: Option<usize>,
    /// Output name used in diagnostics.
    pub output_name: String,
    /// Aggregate result type.
    pub data_type: DataType,
}

/// Planner-selected execution family for a logical pipeline.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PipelineMode {
    /// Row-at-a-time OLTP path. Used for writes, session control, and
    /// small scalar sources where vector startup would not pay.
    #[default]
    ScalarOltp,
    /// Batch/vector OLAP path. Used for scan/filter/project/join/
    /// aggregate/sort/window plans that can consume 4096-row batches and
    /// dispatch SIMD/JIT kernels inside operators.
    VectorizedOlap,
}

// ============================================================================
// Aggregate types
// ============================================================================

/// Standard SQL aggregate functions supported by the binder.
///
/// Each variant corresponds to one built-in aggregate name recognised by
/// the binder's aggregate detection pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateFunc {
    /// `COUNT(*)` — count all rows.
    CountStar,
    /// `COUNT(expr)` — count non-NULL values.
    Count,
    /// `SUM(expr)`.
    Sum,
    /// `AVG(expr)`.
    Avg,
    /// `MIN(expr)`.
    Min,
    /// `MAX(expr)`.
    Max,
    /// `BOOL_AND(expr)`.
    BoolAnd,
    /// `BOOL_OR(expr)`.
    BoolOr,
    /// `STRING_AGG(expr, delimiter)`.
    StringAgg,
    /// `ARRAY_AGG(expr)`.
    ArrayAgg,
    /// `JSON_AGG(expr)`.
    JsonAgg,
    /// `STDDEV_SAMP(expr)` / `STDDEV(expr)` — sample standard
    /// deviation. `NULL` for fewer than two non-null inputs.
    StddevSamp,
    /// `STDDEV_POP(expr)` — population standard deviation. `NULL`
    /// when no non-null input was seen.
    StddevPop,
    /// `VAR_SAMP(expr)` / `VARIANCE(expr)` — sample variance.
    /// `NULL` for fewer than two non-null inputs.
    VarSamp,
    /// `VAR_POP(expr)` — population variance. `NULL` when no
    /// non-null input was seen.
    VarPop,
    /// `CORR(y, x)` — Pearson correlation coefficient.
    Corr,
    /// `PERCENTILE_CONT(fraction) WITHIN GROUP (ORDER BY expr)`.
    PercentileCont,
    /// `PERCENTILE_DISC(fraction) WITHIN GROUP (ORDER BY expr)`.
    PercentileDisc,
}

/// A single aggregate call in a `GROUP BY` / aggregation node.
///
/// `output_name` is the column name in the output schema; `data_type`
/// is the result type of the aggregate function.
#[derive(Clone, Debug, PartialEq)]
pub struct LogicalAggregateExpr {
    /// Which aggregate function to compute.
    pub func: AggregateFunc,
    /// The argument expression; `None` for `COUNT(*)`.
    pub arg: Option<ScalarExpr>,
    /// Direct (non-aggregated) argument: the percentile fraction for
    /// ordered-set aggregates, or the delimiter for `STRING_AGG`.
    pub direct_arg: Option<ScalarExpr>,
    /// Ordered-set aggregate sort key from `WITHIN GROUP`.
    pub order_by: Option<SortKey>,
    /// Whether `DISTINCT` was specified on the argument.
    pub distinct: bool,
    /// Output column name (from alias or derived from the call expression).
    pub output_name: String,
    /// Result data type of this aggregate.
    pub data_type: DataType,
}

/// Aggregate specification inside a logical `PIVOT`.
#[derive(Clone, Debug, PartialEq)]
pub struct LogicalPivotAggregate {
    /// Aggregate function to compute for each pivot bucket.
    pub func: AggregateFunc,
    /// Bound aggregate argument, or `None` for `COUNT(*)`.
    pub arg: Option<ScalarExpr>,
    /// Result data type for each pivot output column.
    pub data_type: DataType,
}

/// One value/output-column pair inside a logical `PIVOT`.
#[derive(Clone, Debug, PartialEq)]
pub struct LogicalPivotValue {
    /// Constant pivot-key value.
    pub value: Value,
    /// Static type of `value`.
    pub data_type: DataType,
    /// Output column name for this pivot bucket.
    pub output_name: String,
}

/// One input column/label pair inside a logical `UNPIVOT`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalUnpivotColumn {
    /// 0-based input source column index.
    pub source_column: usize,
    /// Text label emitted into the unpivot name column.
    pub label: String,
}

// ============================================================================
// SortKey and conflict types (pre-existing)
// ============================================================================

/// A sort key for `ORDER BY`.
#[derive(Clone, Debug, PartialEq)]
pub struct SortKey {
    /// Sort expression (resolved against the input schema).
    pub expr: ScalarExpr,
    /// `true` for `ASC`, `false` for `DESC`.
    pub asc: bool,
    /// Whether NULLs sort first.
    pub nulls_first: bool,
}

/// Conflict target resolved to column indices in the target table's schema.
///
/// An empty `columns` list means the conflict target was absent (only valid
/// for `DO NOTHING`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConflictTarget {
    /// 0-based indices into the target table's schema.
    pub columns: Vec<usize>,
}

/// The resolved `ON CONFLICT` clause of a logical `Insert` plan node.
///
/// `EXCLUDED` column references inside `DoUpdate::assignments` and
/// `DoUpdate::where` bind as columns after the target-table columns in
/// the expression input row. The executor evaluates those expressions
/// against `[existing_row..., excluded_row...]`.
#[derive(Clone, Debug, PartialEq)]
pub enum LogicalOnConflict {
    /// `ON CONFLICT [target] DO NOTHING`.
    DoNothing {
        /// Optional conflict target.
        target: Option<ConflictTarget>,
    },
    /// `ON CONFLICT target DO UPDATE SET …`.
    DoUpdate {
        /// Conflict target (must be non-empty).
        target: ConflictTarget,
        /// `(column-index, new-value-expression)` pairs.
        assignments: Vec<(usize, ScalarExpr)>,
        /// Optional `WHERE` filter applied to the existing row before
        /// performing the update.
        r#where: Option<ScalarExpr>,
    },
}

/// Match class for a bound `MERGE INTO` branch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalMergeMatchKind {
    /// `WHEN MATCHED`.
    Matched,
    /// `WHEN NOT MATCHED`.
    NotMatched,
}

/// Runtime action attached to a bound `MERGE INTO` branch.
#[derive(Clone, Debug, PartialEq)]
pub enum LogicalMergeAction {
    /// `THEN UPDATE SET ...`.
    Update {
        /// `(column-index, new-value-expression)` pairs against the full
        /// target table schema. Expressions bind against
        /// `[target alias columns..., source alias columns...]`.
        assignments: Vec<(usize, ScalarExpr)>,
    },
    /// `THEN DELETE`.
    Delete,
    /// `THEN INSERT [(columns)] VALUES (...)`.
    Insert {
        /// 0-based target column indices supplied by the branch.
        columns: Vec<usize>,
        /// Values bound against `[target alias columns..., source alias columns...]`.
        values: Vec<ScalarExpr>,
    },
}

/// One ordered `WHEN ... THEN ...` branch in a bound `MERGE INTO`.
#[derive(Clone, Debug, PartialEq)]
pub struct LogicalMergeClause {
    /// Whether this branch handles matched or not-matched source rows.
    pub kind: LogicalMergeMatchKind,
    /// Optional `AND` predicate, bound against the combined target/source row.
    pub condition: Option<ScalarExpr>,
    /// Mutation action to execute for this branch.
    pub action: LogicalMergeAction,
}

// ============================================================================
// Transaction control
// ============================================================================

/// Transaction isolation level as carried by [`LogicalPlan::Begin`](crate::plan::LogicalPlan::Begin).
///
/// Maps 1:1 onto `ultrasql_txn::IsolationLevel`; redefined here so the
/// planner crate does not depend on the txn crate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnIsolationLevel {
    /// `READ COMMITTED` — per-statement snapshot.
    ReadCommitted,
    /// `REPEATABLE READ` — snapshot fixed at transaction start.
    RepeatableRead,
    /// `SERIALIZABLE` — serializable isolation requested by the client.
    Serializable,
}

/// Session-setting action carried by [`LogicalPlan::SetVariable`](crate::plan::LogicalPlan::SetVariable).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalSetVariableAction {
    /// `SET [SESSION] name = value`.
    Set,
    /// `SET LOCAL name = value`.
    SetLocal,
    /// `SHOW name`.
    Show,
    /// `RESET name`.
    Reset,
}

/// Catalog object kind carried by [`LogicalDescribeTarget::Object`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalDescribeObjectKind {
    /// No explicit object-kind qualifier.
    Any,
    /// Ordinary table or table-like relation.
    Table,
    /// View relation.
    View,
}

/// Bound target metadata for `DESCRIBE`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogicalDescribeTarget {
    /// A catalog object whose stored schema should be returned.
    Object {
        /// Case-folded bare object name.
        name: String,
        /// Case-folded schema name.
        namespace: String,
        /// Object kind as validated by the binder.
        kind: LogicalDescribeObjectKind,
        /// Stored object schema.
        object_schema: Schema,
    },
    /// A query expression whose projected schema should be returned.
    Query {
        /// Bound query output schema.
        query_schema: Schema,
    },
}

// ============================================================================
// Locking
// ============================================================================

/// Row-level lock strength for `SELECT FOR UPDATE / FOR SHARE` variants.
///
/// Matches PostgreSQL's `LockClauseStrength` enum. Weaker modes block fewer
/// concurrent operations; see the PostgreSQL manual §13.3.2 for the full
/// compatibility matrix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockStrength {
    /// `FOR UPDATE` — exclusive row lock.
    Update,
    /// `FOR NO KEY UPDATE` — like Update but does not conflict with KeyShare.
    NoKeyUpdate,
    /// `FOR SHARE` — shared lock; blocks concurrent writes.
    Share,
    /// `FOR KEY SHARE` — weakest shared; only blocks FOR UPDATE.
    KeyShare,
}

/// Wait policy for a `FOR UPDATE / FOR SHARE` clause.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum LockWaitPolicy {
    /// Block until lock available (default).
    #[default]
    Wait,
    /// Raise an error immediately if any row is locked.
    NoWait,
    /// Silently skip rows that cannot be locked immediately.
    SkipLocked,
}

/// Resolved window function applied by a [`LogicalPlan::Window`](crate::plan::LogicalPlan::Window) node.
///
/// Each variant maps 1-to-1 to an `ultrasql_executor::WindowFunc`
/// variant; the pipeline lowerer performs the trivial conversion at
/// operator-construction time.
#[derive(Clone, Debug, PartialEq)]
pub enum LogicalWindowFunc {
    /// `ROW_NUMBER()` — 1-based row number within the partition.
    RowNumber,
    /// `RANK()` — rank with gaps.
    Rank,
    /// `DENSE_RANK()` — rank without gaps.
    DenseRank,
    /// `LAG(expr [, offset [, default]])`.
    Lag {
        /// The value expression.
        expr: ScalarExpr,
        /// Number of rows back (default 1).
        offset: usize,
        /// Default value when out of partition bounds.
        default: Value,
    },
    /// `LEAD(expr [, offset [, default]])`.
    Lead {
        /// The value expression.
        expr: ScalarExpr,
        /// Number of rows ahead (default 1).
        offset: usize,
        /// Default value when out of partition bounds.
        default: Value,
    },
    /// `FIRST_VALUE(expr)`.
    FirstValue(ScalarExpr),
    /// `LAST_VALUE(expr)`.
    LastValue(ScalarExpr),
    /// `NTH_VALUE(expr, n)`.
    NthValue {
        /// The value expression.
        expr: ScalarExpr,
        /// 1-based position.
        n: usize,
    },
    /// `NTILE(n)`.
    Ntile(usize),
    /// A frame-aware aggregate window function: `SUM`/`AVG`/`COUNT`/
    /// `MIN`/`MAX(expr) OVER (...)`.
    Aggregate {
        /// Which aggregate to compute over the frame.
        kind: WindowAggKind,
        /// The argument expression evaluated per row.
        expr: ScalarExpr,
    },
    /// `COUNT(*) OVER (...)` — counts all rows in the frame.
    CountStar,
}

/// The aggregate kernels usable as frame-aware window functions.
///
/// A strict subset of [`AggregateFunc`] — only the aggregates whose
/// running/frame evaluation is well-defined and required by the SQL
/// window-frame surface. The executor evaluates these against the
/// per-row computed frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowAggKind {
    /// `SUM(expr)`.
    Sum,
    /// `AVG(expr)`.
    Avg,
    /// `COUNT(expr)` — counts non-NULL values in the frame.
    Count,
    /// `MIN(expr)`.
    Min,
    /// `MAX(expr)`.
    Max,
}

/// Frame mode for a bound window frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BoundFrameUnits {
    /// `ROWS` — physical row offsets within the partition.
    Rows,
    /// `RANGE` — logical offsets by `ORDER BY` value (peers share a frame).
    Range,
    /// `GROUPS` — logical offsets by number of peer groups.
    Groups,
}

/// One endpoint of a bound window frame.
#[derive(Clone, Debug, PartialEq)]
pub enum BoundFrameBound {
    /// `UNBOUNDED PRECEDING` — start of the partition.
    UnboundedPreceding,
    /// `<offset> PRECEDING` — offset before the current row/value/group.
    Preceding(ScalarExpr),
    /// `CURRENT ROW`.
    CurrentRow,
    /// `<offset> FOLLOWING` — offset after the current row/value/group.
    Following(ScalarExpr),
    /// `UNBOUNDED FOLLOWING` — end of the partition.
    UnboundedFollowing,
}

/// `EXCLUDE` option on a bound window frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BoundFrameExclusion {
    /// `EXCLUDE NO OTHERS` (default).
    NoOthers,
    /// `EXCLUDE CURRENT ROW`.
    CurrentRow,
    /// `EXCLUDE GROUP`.
    Group,
    /// `EXCLUDE TIES`.
    Ties,
}

/// A bound window frame attached to a [`LogicalPlan::Window`] node.
///
/// Offset expressions are lowered to [`ScalarExpr`]; the binder fills
/// this with the SQL default frame when the source query omits an
/// explicit frame clause (see the window binder).
///
/// [`LogicalPlan::Window`]: super::LogicalPlan::Window
#[derive(Clone, Debug, PartialEq)]
pub struct LogicalWindowFrame {
    /// Frame mode.
    pub units: BoundFrameUnits,
    /// Frame start bound.
    pub start: BoundFrameBound,
    /// Frame end bound.
    pub end: BoundFrameBound,
    /// `EXCLUDE` option.
    pub exclude: BoundFrameExclusion,
}

impl LogicalWindowFrame {
    /// The whole-partition frame `RANGE BETWEEN UNBOUNDED PRECEDING AND
    /// UNBOUNDED FOLLOWING EXCLUDE NO OTHERS` — the default when there
    /// is no `ORDER BY`, and the frame used by frame-insensitive
    /// functions (ranking / `LAG` / `LEAD`).
    #[must_use]
    pub fn whole_partition() -> Self {
        Self {
            units: BoundFrameUnits::Range,
            start: BoundFrameBound::UnboundedPreceding,
            end: BoundFrameBound::UnboundedFollowing,
            exclude: BoundFrameExclusion::NoOthers,
        }
    }

    /// The default running frame `RANGE BETWEEN UNBOUNDED PRECEDING AND
    /// CURRENT ROW EXCLUDE NO OTHERS` — the SQL default when an
    /// `ORDER BY` is present and no explicit frame is given.
    #[must_use]
    pub fn default_running() -> Self {
        Self {
            units: BoundFrameUnits::Range,
            start: BoundFrameBound::UnboundedPreceding,
            end: BoundFrameBound::CurrentRow,
            exclude: BoundFrameExclusion::NoOthers,
        }
    }

    /// `true` when this frame is the whole-partition default with no
    /// exclusion. Used by the plan display to suppress the default
    /// frame line and keep frame-less query plans stable.
    #[must_use]
    pub fn is_whole_partition_default(&self) -> bool {
        *self == Self::whole_partition()
    }

    /// `true` when this frame is the default running frame.
    #[must_use]
    pub fn is_default_running(&self) -> bool {
        *self == Self::default_running()
    }
}
