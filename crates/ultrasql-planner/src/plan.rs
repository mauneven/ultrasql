//! Logical plan tree.
//!
//! The logical plan is the binder's output and the optimizer's input.
//! It is type-checked but not physical: it names *what* to compute, not
//! *how*. Each variant produces a [`Schema`] queryable through
//! [`LogicalPlan::schema`]; an EXPLAIN-style indented dump is available
//! through [`LogicalPlan::display`].

use std::fmt;

use ultrasql_core::{DataType, Field, Schema};

use crate::expr::ScalarExpr;

// ============================================================================
// Join types
// ============================================================================

/// Logical join type.
///
/// These match the SQL standard join modifiers: `INNER`, `LEFT OUTER`,
/// `RIGHT OUTER`, `FULL OUTER`, and `CROSS`.
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
    /// Whether `DISTINCT` was specified on the argument.
    pub distinct: bool,
    /// Output column name (from alias or derived from the call expression).
    pub output_name: String,
    /// Result data type of this aggregate.
    pub data_type: DataType,
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
/// `EXCLUDED` column references inside `DoUpdate::assignments` are not
/// supported in v0.2; the binder rejects them with
/// [`crate::error::PlanError::NotSupported`].
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

// ============================================================================
// Transaction control
// ============================================================================

/// Transaction isolation level as carried by [`LogicalPlan::Begin`].
///
/// Maps 1:1 onto `ultrasql_txn::IsolationLevel`; redefined here so the
/// planner crate does not depend on the txn crate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnIsolationLevel {
    /// `READ COMMITTED` — per-statement snapshot.
    ReadCommitted,
    /// `REPEATABLE READ` — snapshot fixed at transaction start.
    RepeatableRead,
    /// `SERIALIZABLE` — full SSI.
    Serializable,
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

// ============================================================================
// LogicalPlan
// ============================================================================

/// The bound, type-checked logical plan tree.
#[derive(Clone, Debug, PartialEq)]
pub enum LogicalPlan {
    /// Table scan. The `projection` field is reserved for the
    /// optimizer's projection pushdown; the binder always emits
    /// `None` so the scan returns the table's natural column order.
    Scan {
        /// Case-folded table name.
        table: String,
        /// Output schema (table schema, possibly already projected).
        schema: Schema,
        /// Optional list of column indices to scan. `None` means "all
        /// columns in natural order".
        projection: Option<Vec<usize>>,
    },

    /// Filter rows by a boolean predicate. The input's schema flows
    /// through unchanged.
    Filter {
        /// Input plan.
        input: Box<Self>,
        /// Boolean-valued predicate, bound against `input.schema()`.
        predicate: ScalarExpr,
    },

    /// Project a tuple of expressions out of the input, each with an
    /// output name.
    Project {
        /// Input plan.
        input: Box<Self>,
        /// Output expressions paired with their column names.
        exprs: Vec<(ScalarExpr, String)>,
        /// Output schema, derived from `exprs`.
        schema: Schema,
    },

    /// `LIMIT n OFFSET m`.
    Limit {
        /// Input plan.
        input: Box<Self>,
        /// Maximum number of rows to return.
        n: u64,
        /// Number of rows to skip before counting toward the limit.
        offset: u64,
    },

    /// Sort by a list of keys.
    Sort {
        /// Input plan.
        input: Box<Self>,
        /// Sort keys, evaluated left-to-right.
        keys: Vec<SortKey>,
    },

    /// A no-row source. Used for queries with constant-false predicates
    /// and for the placeholder produced when a statement is a `SELECT`
    /// with no `FROM`.
    Empty {
        /// Output schema (may be empty).
        schema: Schema,
    },

    /// A literal row set produced by a `VALUES` clause.
    ///
    /// All rows must have the same arity (enforced by the binder). The
    /// output schema uses PostgreSQL-compatible synthetic column names
    /// `column1`, `column2`, … Column types are the `numeric_join` of
    /// all cells in the same column across all rows; columns that are
    /// entirely NULL have type `DataType::Null`.
    Values {
        /// One inner `Vec` per row; all inner `Vec`s have the same length.
        rows: Vec<Vec<ScalarExpr>>,
        /// Output schema inferred from the rows.
        schema: Schema,
    },

    /// Insert rows into a table.
    ///
    /// The `source` child plan produces the rows to insert. The binder
    /// ensures the source's arity matches `columns.len()` (or the full
    /// table schema width when `columns` is empty).
    Insert {
        /// Case-folded target table name.
        table: String,
        /// 0-based indices into the target table's full schema for the
        /// targeted columns. Empty means "all columns in natural order".
        columns: Vec<usize>,
        /// Child plan that supplies the rows (`Values`, `Project` over
        /// `Scan`, etc.).
        source: Box<Self>,
        /// Resolved `ON CONFLICT` action, if any.
        on_conflict: Option<LogicalOnConflict>,
        /// `RETURNING` output expressions paired with their output names.
        returning: Vec<(ScalarExpr, String)>,
        /// Schema of the rows returned by `RETURNING`. Empty when there
        /// is no `RETURNING` clause.
        schema: Schema,
    },

    /// Update existing rows in a table.
    ///
    /// The `input` child plan is a `Scan` (possibly wrapped in `Filter`)
    /// that selects the rows to update.
    ///
    /// `UPDATE … FROM other_table` is not supported in v0.2; the binder
    /// returns `NotSupported` for that form.
    Update {
        /// Case-folded target table name.
        table: String,
        /// `(column-index, new-value-expression)` pairs.
        assignments: Vec<(usize, ScalarExpr)>,
        /// Input plan feeding the rows to update.
        input: Box<Self>,
        /// `RETURNING` output expressions.
        returning: Vec<(ScalarExpr, String)>,
        /// Schema of the rows returned by `RETURNING`. Empty when there
        /// is no `RETURNING` clause.
        schema: Schema,
    },

    /// Delete rows from a table.
    ///
    /// The `input` child plan is a `Scan` (possibly wrapped in `Filter`)
    /// that selects the rows to delete.
    ///
    /// `DELETE … USING other_table` is not supported in v0.2; the binder
    /// returns `NotSupported` for that form.
    Delete {
        /// Case-folded target table name.
        table: String,
        /// Input plan feeding the rows to delete.
        input: Box<Self>,
        /// `RETURNING` output expressions.
        returning: Vec<(ScalarExpr, String)>,
        /// Schema of the rows returned by `RETURNING`. Empty when there
        /// is no `RETURNING` clause.
        schema: Schema,
    },

    /// Truncate one or more tables.
    ///
    /// Every table name is validated against the catalog by the binder.
    Truncate {
        /// Case-folded table names.
        tables: Vec<String>,
        /// Whether `RESTART IDENTITY` was specified.
        restart_identity: bool,
        /// Whether `CASCADE` was specified.
        cascade: bool,
        /// Always an empty schema — `TRUNCATE` returns no rows.
        schema: Schema,
    },

    /// Create a new base table.
    ///
    /// The binder produces a fully resolved column [`Schema`] so the
    /// executor can persist the relation without re-parsing column
    /// types. The v0.5 binder accepts only NULL / NOT NULL / PRIMARY
    /// KEY (which implies NOT NULL) at the column level; DEFAULT,
    /// UNIQUE, CHECK, REFERENCES, and table-level constraints return
    /// [`crate::error::PlanError::NotSupported`].
    CreateTable {
        /// Case-folded bare relation name (no namespace qualifier).
        table_name: String,
        /// SQL namespace (e.g. `"public"`). Distinct from `columns` —
        /// PostgreSQL calls this the "schema" but inside the planner
        /// "schema" means a column shape, so we rename to avoid the
        /// double-meaning.
        namespace: String,
        /// Resolved column metadata — the row shape of the relation
        /// being created.
        columns: Schema,
        /// Whether `IF NOT EXISTS` was specified. When true the
        /// executor short-circuits if the relation already exists.
        if_not_exists: bool,
        /// Always [`Schema::empty`]; DDL emits no rows. Carried for
        /// uniform [`LogicalPlan::schema`] access by callers.
        schema: Schema,
    },

    /// Join two child plans.
    ///
    /// For `LEFT JOIN`, every column on the right side of `schema` is
    /// nullable. For `RIGHT JOIN`, every column on the left side is
    /// nullable. For `FULL OUTER JOIN`, both sides are nullable.
    /// `CROSS JOIN` has `condition = LogicalJoinCondition::None`.
    ///
    /// The `schema` is the concatenation of the left and right schemas
    /// under the appropriate outer-join nullability rules, except for
    /// `USING` joins where the joined column appears only once.
    Join {
        /// Left input plan.
        left: Box<Self>,
        /// Right input plan.
        right: Box<Self>,
        /// Join type.
        join_type: LogicalJoinType,
        /// Join condition.
        condition: LogicalJoinCondition,
        /// Output schema (concatenation under outer-join nullability rules).
        schema: Schema,
    },

    /// Group-by / aggregate computation.
    ///
    /// The output schema is `[group_by_columns ..., aggregate_columns ...]`.
    /// Group-by columns preserve the input field name except for non-column
    /// expressions which are named `group0`, `group1`, etc. Aggregate
    /// columns use `LogicalAggregateExpr::output_name`.
    Aggregate {
        /// Input plan to aggregate over.
        input: Box<Self>,
        /// Group-by key expressions.
        group_by: Vec<ScalarExpr>,
        /// Aggregate function calls.
        aggregates: Vec<LogicalAggregateExpr>,
        /// Output schema: group-by columns then aggregate columns.
        schema: Schema,
    },

    /// Set operation (UNION / INTERSECT / EXCEPT).
    ///
    /// Both sides must have the same arity; column types are the
    /// `numeric_join` of the two sides per column (binder-enforced).
    SetOp {
        /// Set operation kind.
        op: LogicalSetOp,
        /// ALL or DISTINCT quantifier.
        quantifier: LogicalSetQuantifier,
        /// Left input.
        left: Box<Self>,
        /// Right input.
        right: Box<Self>,
        /// Output schema (derived from the left side's schema).
        schema: Schema,
    },

    /// Non-recursive or flag-recursive CTE.
    ///
    /// The `definition` plan is the CTE's body. The `body` plan is the
    /// main query that may reference the CTE by name. For
    /// `WITH RECURSIVE`, `recursive = true`; the planner records this
    /// flag but the recursive fixpoint is deferred to the executor
    /// (wave 5). Until then a recursive CTE binding resolves
    /// non-recursively.
    Cte {
        /// CTE name (used in `Scan` references inside `body`).
        name: String,
        /// Whether `WITH RECURSIVE` was specified.
        ///
        /// # Note
        /// Recursion is not yet executed: the executor does not implement
        /// the fixpoint loop. This flag is preserved so planning round-trips
        /// correctly; a future executor wave will consume it.
        recursive: bool,
        /// The CTE definition plan.
        definition: Box<Self>,
        /// The main query that consumes the CTE.
        body: Box<Self>,
        /// Output schema — identical to `body.schema()`.
        schema: Schema,
    },

    /// Apply row-level locks to every row emitted by the input plan.
    ///
    /// This is the physical counterpart of `SELECT FOR UPDATE / FOR SHARE`
    /// variants. The optimizer leaves the node in place; the executor wraps
    /// the child operator with a [`ultrasql_executor::LockRows`] callback
    /// that acquires the requested lock on each row's `TupleId` before
    /// yielding the row to the caller.
    ///
    /// The `schema` flows through from `input` unchanged.
    LockRows {
        /// Child plan whose output rows will be locked.
        input: Box<Self>,
        /// Lock strength requested by the query.
        strength: LockStrength,
        /// What to do when a row cannot be locked immediately.
        wait_policy: LockWaitPolicy,
        /// Output schema (identical to `input.schema()`).
        schema: Schema,
    },

    /// Create a B+ tree index on one or more columns of a base table.
    ///
    /// The binder validates that `table_name` resolves in the catalog
    /// and that every column listed in `columns` exists in that
    /// table's schema. The optional `index_name` is preserved as-is
    /// when supplied; the binder synthesises `"{table}_{col1}_{...}_idx"`
    /// otherwise so the executor always has a stable name to register
    /// in `pg_index`.
    ///
    /// `unique` records whether `CREATE UNIQUE INDEX` was specified;
    /// it propagates into the resulting catalog entry. `if_not_exists`
    /// short-circuits without an error when the index name already
    /// exists.
    ///
    /// `USING method`, `INCLUDE`, `WHERE` partial-index predicates,
    /// and expression keys (anything other than a bare column
    /// reference) return [`crate::error::PlanError::NotSupported`]
    /// in this wave; the binder rejects them up front so the executor
    /// arm stays minimal.
    CreateIndex {
        /// Index name (caller-supplied or binder-synthesised). Always
        /// lowercase.
        index_name: String,
        /// Target table (lowercase).
        table_name: String,
        /// 0-based column indices into the table schema, in index key
        /// order.
        columns: Vec<usize>,
        /// Whether `UNIQUE` was specified.
        unique: bool,
        /// Whether `IF NOT EXISTS` was specified.
        if_not_exists: bool,
        /// Always [`Schema::empty`]; DDL emits no rows.
        schema: Schema,
    },

    /// Drop one or more base tables and (cascading) their indexes.
    ///
    /// The binder lowercases every name and validates that — for the
    /// non-`IF EXISTS` form — each named relation exists in the
    /// catalog. Missing relations with `IF EXISTS` are simply omitted
    /// from `tables` so the executor never sees them. `CASCADE` is
    /// preserved on the plan node so future revisions that consult
    /// `pg_depend` can honour it; today the catalog drop is
    /// unconditional (associated indexes are always removed).
    DropTable {
        /// Lowercase table names to drop, in the user's order.
        tables: Vec<String>,
        /// Whether `IF EXISTS` was specified.
        if_exists: bool,
        /// Whether `CASCADE` was specified.
        cascade: bool,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// Alter an existing base table.
    ///
    /// The action is one of [`LogicalAlterTableAction`]; the binder
    /// rejects every other parser-level action (DROP COLUMN, RENAME
    /// COLUMN, RENAME TO, ADD/DROP CONSTRAINT) until the corresponding
    /// rewrite path is wired in a follow-up wave.
    AlterTable {
        /// Lowercase target table name.
        table_name: String,
        /// Resolved, type-checked action.
        action: LogicalAlterTableAction,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `BEGIN [TRANSACTION]` — open an explicit transaction block.
    ///
    /// The planner produces this variant unconditionally; the server
    /// inspects the session's transaction state to either transition
    /// `Idle → InTransaction` or emit a `NoticeResponse` if a
    /// transaction is already open (matching PostgreSQL's
    /// `WARNING: there is already a transaction in progress`).
    ///
    /// `schema` is always [`Schema::empty`].
    Begin {
        /// Requested isolation level. `None` means the server's default
        /// (`ReadCommitted` in UltraSQL, matching PostgreSQL's default).
        isolation_level: Option<TxnIsolationLevel>,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `COMMIT` — finalise the current explicit transaction block.
    ///
    /// In `Failed` state the server commits as a rollback (matching
    /// PostgreSQL semantics) but still emits the `ROLLBACK` command
    /// tag. In `Idle` state the server emits a `NoticeResponse`
    /// `WARNING: there is no transaction in progress` and treats the
    /// statement as a no-op.
    ///
    /// `schema` is always [`Schema::empty`].
    Commit {
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `ROLLBACK` — abort the current explicit transaction block.
    ///
    /// In `Idle` state the server emits a `NoticeResponse`
    /// `WARNING: there is no transaction in progress` and treats the
    /// statement as a no-op.
    ///
    /// `schema` is always [`Schema::empty`].
    Rollback {
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `SAVEPOINT name` — set a savepoint inside the current
    /// transaction block.
    ///
    /// Outside a transaction the server rejects this with
    /// `25P01` (`no_active_sql_transaction`).
    ///
    /// `schema` is always [`Schema::empty`].
    Savepoint {
        /// Case-preserved savepoint name (PostgreSQL lowercases
        /// unquoted identifiers; the server treats the name
        /// case-insensitively when matching `ROLLBACK TO` /
        /// `RELEASE`).
        name: String,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `ROLLBACK TO [SAVEPOINT] name` — roll back to a named
    /// savepoint inside the current transaction block.
    ///
    /// Outside a transaction the server rejects this with `25P01`.
    /// An unknown savepoint name surfaces as `3B001`
    /// (`invalid_savepoint_specification`).
    ///
    /// `schema` is always [`Schema::empty`].
    RollbackToSavepoint {
        /// Savepoint name.
        name: String,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `RELEASE [SAVEPOINT] name` — destroy a named savepoint inside
    /// the current transaction block.
    ///
    /// Outside a transaction the server rejects this with `25P01`.
    /// An unknown savepoint name surfaces as `3B001`.
    ///
    /// `schema` is always [`Schema::empty`].
    ReleaseSavepoint {
        /// Savepoint name.
        name: String,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `PREPARE TRANSACTION 'gid'` — phase 1 of two-phase commit.
    PrepareTransaction {
        /// Global transaction identifier.
        gid: String,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `COMMIT PREPARED 'gid'` — phase 2 commit.
    CommitPrepared {
        /// Global transaction identifier to resolve.
        gid: String,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `ROLLBACK PREPARED 'gid'` — phase 2 abort.
    RollbackPrepared {
        /// Global transaction identifier to resolve.
        gid: String,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `SET TRANSACTION ISOLATION LEVEL …` — change the *current*
    /// transaction's isolation level. The server requires an active
    /// transaction (SQLSTATE `25P01` outside one).
    ///
    /// `schema` is always [`Schema::empty`].
    SetTransaction {
        /// Requested isolation level.
        isolation_level: TxnIsolationLevel,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `LISTEN channel` — subscribe the session to async notifications
    /// delivered on `channel`. The server keeps the subscription in its
    /// per-process `NotifyHub` for the lifetime of the connection;
    /// `UNLISTEN` and connection close drop it.
    ///
    /// `schema` is always [`Schema::empty`].
    Listen {
        /// Channel name as it should reach the hub. The binder
        /// lower-cases unquoted identifiers to match PostgreSQL's
        /// case-folding rules; quoted names round-trip verbatim.
        channel: String,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `NOTIFY channel [, payload]` — publish `payload` on `channel`
    /// to every session currently listening.
    ///
    /// `schema` is always [`Schema::empty`].
    Notify {
        /// Channel name to publish on.
        channel: String,
        /// Optional payload. PostgreSQL allows omitting the payload
        /// (defaults to the empty string on the wire); we keep the
        /// `Option` so the wire layer can mirror that distinction.
        payload: Option<String>,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `UNLISTEN { channel | * }` — drop one or all of this session's
    /// channel subscriptions.
    ///
    /// `schema` is always [`Schema::empty`].
    Unlisten {
        /// Channel name to drop. `None` means `UNLISTEN *` — drop every
        /// subscription owned by this session.
        channel: Option<String>,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// Set-returning function in `FROM`, e.g. `FROM generate_series(1, 10)`.
    ///
    /// The lowerer dispatches on `name` to construct the matching
    /// [`ultrasql_executor::FunctionScan`] variant.
    FunctionScan {
        /// Function name (case-folded). v0.5 supports `generate_series`.
        name: String,
        /// Bound argument expressions in declaration order.
        args: Vec<ScalarExpr>,
        /// Output schema for the function's rows.
        schema: Schema,
    },

    /// `EXPLAIN [ANALYZE] [(FORMAT TEXT|JSON)] stmt`.
    ///
    /// Wraps an inner logical plan. The server renders the wrapped
    /// plan's tree into the single `"QUERY PLAN"` Text column of
    /// `schema`; when `analyze` is true, it executes the inner plan
    /// and surfaces row count + wall time alongside the rendered
    /// tree.
    Explain {
        /// `true` for `EXPLAIN ANALYZE` — executes the inner plan.
        analyze: bool,
        /// Output format selector.
        format: ExplainFormat,
        /// The wrapped plan to describe.
        input: Box<Self>,
        /// Always a single nullable `Text` column named `"QUERY PLAN"`.
        schema: Schema,
    },

    /// `COPY table [(col_list)] { FROM | TO } { STDIN | STDOUT }
    ///     [WITH (FORMAT { TEXT | CSV }, …)]`.
    ///
    /// `relation` is the bound, lowercase target table name. `columns`
    /// is the 0-based index list into the table's full schema; an
    /// empty `columns` vector means "every column in natural order".
    /// `schema` is the row shape of the data stream the COPY transfers
    /// — the server's session dispatcher uses it to size the
    /// `CopyInResponse` / `CopyOutResponse` column-format vector and
    /// to drive per-column text encoding.
    Copy {
        /// Case-folded target table name.
        relation: String,
        /// 0-based indices into the target table's schema. Empty means
        /// "all columns in natural order".
        columns: Vec<usize>,
        /// Whether rows flow client → server or server → client.
        direction: CopyDirection,
        /// Wire endpoint — `STDIN` or `STDOUT`.
        source: CopySource,
        /// Wire format negotiated by the parser.
        format: CopyFormat,
        /// Single-character column delimiter. Defaults match the format
        /// (`\t` for TEXT, `,` for CSV).
        delimiter: char,
        /// String used to represent SQL NULL on the wire.
        null_str: String,
        /// Whether the data stream contains a header row.
        header: bool,
        /// Row shape of the data stream — derived from `columns` and
        /// the target table's schema.
        schema: Schema,
    },
}

/// EXPLAIN output format selector, mirrored from
/// [`ultrasql_parser::ast::ExplainFormat`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExplainFormat {
    /// `EXPLAIN ... (FORMAT TEXT)` — indented tree, one row per node.
    Text,
    /// `EXPLAIN ... (FORMAT JSON)` — single row carrying the JSON
    /// rendering of the plan tree.
    Json,
}

/// COPY direction, mirrored from
/// [`ultrasql_parser::ast::CopyDirection`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopyDirection {
    /// `COPY t FROM …` — client streams rows in.
    From,
    /// `COPY t TO …` — server streams rows out.
    To,
}

/// COPY source / sink, mirrored from
/// [`ultrasql_parser::ast::CopySource`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopySource {
    /// `STDIN` — client streams `CopyData` frames in.
    Stdin,
    /// `STDOUT` — server streams `CopyData` frames out.
    Stdout,
}

/// COPY wire format, mirrored from
/// [`ultrasql_parser::ast::CopyFormat`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopyFormat {
    /// PostgreSQL `TEXT` format — tab-separated, escape-encoded.
    Text,
    /// PostgreSQL `CSV` format.
    Csv,
}

/// Resolved `ALTER TABLE` action.
///
/// The binder pre-resolves every reference (column types,
/// nullability) so the executor can apply the change without touching
/// the parser again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogicalAlterTableAction {
    /// `ALTER TABLE t ADD [COLUMN] c TYPE [NULL | NOT NULL]`.
    ///
    /// The new column is appended to the end of the table's schema.
    /// `column` carries the resolved [`Field`] (name, type,
    /// nullability) so the executor can grow the schema without
    /// re-parsing the type name.
    AddColumn {
        /// The resolved column being added.
        column: Field,
    },
    /// `ALTER TABLE t DROP [COLUMN] c [CASCADE|RESTRICT]`.
    ///
    /// The column at `column_index` is removed from the table's
    /// schema and every existing tuple is rewritten without that
    /// slot. v0.5 always treats the drop as `RESTRICT`-equivalent
    /// (the binder rejects the drop if the column participates in
    /// a constraint the catalog tracks).
    DropColumn {
        /// 0-based column position resolved against the current schema.
        column_index: usize,
        /// Column name (kept for diagnostics + audit logging).
        column_name: String,
    },
    /// `ALTER TABLE t RENAME [COLUMN] old TO new`.
    ///
    /// Catalog-only: the heap is not rewritten because the rowcoded
    /// layout is positional and a column rename does not change the
    /// row encoding. The binder rejects the rename if `new` collides
    /// with an existing column.
    RenameColumn {
        /// 0-based column position resolved against the current schema.
        column_index: usize,
        /// Old column name.
        old_name: String,
        /// New column name.
        new_name: String,
    },
    /// `ALTER TABLE t RENAME TO new_name`.
    ///
    /// Catalog-only: the heap is not rewritten because relations are
    /// addressed by OID, not by name. The binder rejects the rename
    /// if `new_name` collides with an existing table in the schema.
    RenameTable {
        /// New table name.
        new_name: String,
    },
}

impl LogicalPlan {
    /// The schema of rows produced by this plan node.
    #[must_use]
    pub fn schema(&self) -> &Schema {
        match self {
            Self::Scan { schema, .. }
            | Self::Project { schema, .. }
            | Self::Empty { schema }
            | Self::Values { schema, .. }
            | Self::Insert { schema, .. }
            | Self::Update { schema, .. }
            | Self::Delete { schema, .. }
            | Self::Truncate { schema, .. }
            | Self::CreateTable { schema, .. }
            | Self::Join { schema, .. }
            | Self::Aggregate { schema, .. }
            | Self::SetOp { schema, .. }
            | Self::Cte { schema, .. }
            | Self::LockRows { schema, .. }
            | Self::CreateIndex { schema, .. }
            | Self::DropTable { schema, .. }
            | Self::AlterTable { schema, .. }
            | Self::Begin { schema, .. }
            | Self::Commit { schema }
            | Self::Rollback { schema }
            | Self::Savepoint { schema, .. }
            | Self::RollbackToSavepoint { schema, .. }
            | Self::ReleaseSavepoint { schema, .. }
            | Self::PrepareTransaction { schema, .. }
            | Self::CommitPrepared { schema, .. }
            | Self::RollbackPrepared { schema, .. }
            | Self::SetTransaction { schema, .. }
            | Self::Listen { schema, .. }
            | Self::Notify { schema, .. }
            | Self::Unlisten { schema, .. }
            | Self::Copy { schema, .. }
            | Self::Explain { schema, .. }
            | Self::FunctionScan { schema, .. } => schema,
            Self::Filter { input, .. } | Self::Limit { input, .. } | Self::Sort { input, .. } => {
                input.schema()
            }
        }
    }

    /// Render this plan in an indented EXPLAIN-style tree, where every
    /// child line is indented by two additional spaces.
    ///
    /// `indent` is the column the *root* node's text begins at.
    #[must_use]
    pub fn display(&self, indent: usize) -> String {
        let mut out = String::new();
        self.display_into(indent, &mut out);
        out
    }

    #[allow(clippy::too_many_lines)]
    fn display_into(&self, indent: usize, out: &mut String) {
        let pad = " ".repeat(indent);
        match self {
            Self::Scan { table, .. } => {
                out.push_str(&pad);
                out.push_str("Scan: ");
                out.push_str(table);
                out.push('\n');
            }
            Self::Filter { input, predicate } => {
                out.push_str(&pad);
                out.push_str("Filter: ");
                let _ = fmt::write(out, format_args!("{predicate}"));
                out.push('\n');
                input.display_into(indent + 2, out);
            }
            Self::Project { input, exprs, .. } => {
                out.push_str(&pad);
                out.push_str("Project: ");
                for (i, (e, n)) in exprs.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    let _ = fmt::write(out, format_args!("{e} AS {n}"));
                }
                out.push('\n');
                input.display_into(indent + 2, out);
            }
            Self::Limit { input, n, offset } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("Limit: n={n}, offset={offset}\n"));
                input.display_into(indent + 2, out);
            }
            Self::Sort { input, keys } => {
                out.push_str(&pad);
                out.push_str("Sort: ");
                for (i, k) in keys.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    let dir = if k.asc { "ASC" } else { "DESC" };
                    let nulls = if k.nulls_first {
                        "NULLS FIRST"
                    } else {
                        "NULLS LAST"
                    };
                    let _ = fmt::write(out, format_args!("{} {dir} {nulls}", k.expr));
                }
                out.push('\n');
                input.display_into(indent + 2, out);
            }
            Self::Empty { .. } => {
                out.push_str(&pad);
                out.push_str("Empty\n");
            }
            Self::Values { rows, .. } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("Values: {} row(s)\n", rows.len()));
            }
            Self::Insert {
                table,
                columns,
                source,
                returning,
                ..
            } => {
                out.push_str(&pad);
                out.push_str("Insert: table=");
                out.push_str(table);
                out.push_str(" cols=[");
                for (i, c) in columns.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    let _ = fmt::write(out, format_args!("{c}"));
                }
                out.push(']');
                if !returning.is_empty() {
                    out.push_str(" returning=[");
                    for (i, (e, n)) in returning.iter().enumerate() {
                        if i > 0 {
                            out.push_str(", ");
                        }
                        let _ = fmt::write(out, format_args!("{e} AS {n}"));
                    }
                    out.push(']');
                }
                out.push('\n');
                source.display_into(indent + 2, out);
            }
            Self::Update {
                table,
                assignments,
                input,
                returning,
                ..
            } => {
                out.push_str(&pad);
                out.push_str("Update: table=");
                out.push_str(table);
                out.push_str(" assignments=[");
                for (i, (idx, e)) in assignments.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    let _ = fmt::write(out, format_args!("col{idx}={e}"));
                }
                out.push(']');
                if !returning.is_empty() {
                    out.push_str(" returning=[");
                    for (i, (e, n)) in returning.iter().enumerate() {
                        if i > 0 {
                            out.push_str(", ");
                        }
                        let _ = fmt::write(out, format_args!("{e} AS {n}"));
                    }
                    out.push(']');
                }
                out.push('\n');
                input.display_into(indent + 2, out);
            }
            Self::Delete {
                table,
                input,
                returning,
                ..
            } => {
                out.push_str(&pad);
                out.push_str("Delete: table=");
                out.push_str(table);
                if !returning.is_empty() {
                    out.push_str(" returning=[");
                    for (i, (e, n)) in returning.iter().enumerate() {
                        if i > 0 {
                            out.push_str(", ");
                        }
                        let _ = fmt::write(out, format_args!("{e} AS {n}"));
                    }
                    out.push(']');
                }
                out.push('\n');
                input.display_into(indent + 2, out);
            }
            Self::Truncate {
                tables,
                restart_identity,
                cascade,
                ..
            } => {
                out.push_str(&pad);
                out.push_str("Truncate: tables=[");
                out.push_str(&tables.join(", "));
                out.push(']');
                if *restart_identity {
                    out.push_str(" RESTART IDENTITY");
                }
                if *cascade {
                    out.push_str(" CASCADE");
                }
                out.push('\n');
            }
            Self::CreateTable {
                table_name,
                namespace,
                columns,
                if_not_exists,
                ..
            } => {
                out.push_str(&pad);
                out.push_str("CreateTable: ");
                out.push_str(namespace);
                out.push('.');
                out.push_str(table_name);
                if *if_not_exists {
                    out.push_str(" IF NOT EXISTS");
                }
                out.push_str(" (");
                for (i, f) in columns.fields().iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    let _ = fmt::write(out, format_args!("{} {:?}", f.name, f.data_type));
                    if !f.nullable {
                        out.push_str(" NOT NULL");
                    }
                }
                out.push_str(")\n");
            }
            Self::Join {
                left,
                right,
                join_type,
                condition,
                ..
            } => {
                out.push_str(&pad);
                let jt = match join_type {
                    LogicalJoinType::Inner => "Inner",
                    LogicalJoinType::LeftOuter => "LeftOuter",
                    LogicalJoinType::RightOuter => "RightOuter",
                    LogicalJoinType::FullOuter => "FullOuter",
                    LogicalJoinType::Cross => "Cross",
                };
                out.push_str("Join[");
                out.push_str(jt);
                out.push_str("]: ");
                match condition {
                    LogicalJoinCondition::On(pred) => {
                        let _ = fmt::write(out, format_args!("ON {pred}"));
                    }
                    LogicalJoinCondition::Using(pairs) => {
                        out.push_str("USING(");
                        for (i, (l, r)) in pairs.iter().enumerate() {
                            if i > 0 {
                                out.push(',');
                            }
                            let _ = fmt::write(out, format_args!("{l}={r}"));
                        }
                        out.push(')');
                    }
                    LogicalJoinCondition::None => {
                        out.push_str("(none)");
                    }
                }
                out.push('\n');
                left.display_into(indent + 2, out);
                right.display_into(indent + 2, out);
            }
            Self::Aggregate {
                input,
                group_by,
                aggregates,
                ..
            } => {
                out.push_str(&pad);
                out.push_str("Aggregate: group_by=[");
                for (i, g) in group_by.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    let _ = fmt::write(out, format_args!("{g}"));
                }
                out.push_str("] aggs=[");
                for (i, agg) in aggregates.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    let func_name = match agg.func {
                        AggregateFunc::CountStar => "count(*)",
                        AggregateFunc::Count => "count",
                        AggregateFunc::Sum => "sum",
                        AggregateFunc::Avg => "avg",
                        AggregateFunc::Min => "min",
                        AggregateFunc::Max => "max",
                        AggregateFunc::BoolAnd => "bool_and",
                        AggregateFunc::BoolOr => "bool_or",
                        AggregateFunc::StringAgg => "string_agg",
                        AggregateFunc::ArrayAgg => "array_agg",
                        AggregateFunc::StddevSamp => "stddev_samp",
                        AggregateFunc::StddevPop => "stddev_pop",
                        AggregateFunc::VarSamp => "var_samp",
                        AggregateFunc::VarPop => "var_pop",
                    };
                    if let Some(arg) = &agg.arg {
                        let dist = if agg.distinct { "DISTINCT " } else { "" };
                        let _ = fmt::write(
                            out,
                            format_args!("{func_name}({dist}{arg}) AS {}", agg.output_name),
                        );
                    } else {
                        let _ = fmt::write(out, format_args!("{func_name} AS {}", agg.output_name));
                    }
                }
                out.push_str("]\n");
                input.display_into(indent + 2, out);
            }
            Self::SetOp {
                op,
                quantifier,
                left,
                right,
                ..
            } => {
                out.push_str(&pad);
                let op_str = match op {
                    LogicalSetOp::Union => "Union",
                    LogicalSetOp::Intersect => "Intersect",
                    LogicalSetOp::Except => "Except",
                };
                let q_str = match quantifier {
                    LogicalSetQuantifier::All => "All",
                    LogicalSetQuantifier::Distinct => "Distinct",
                };
                let _ = fmt::write(out, format_args!("SetOp[{op_str} {q_str}]\n"));
                left.display_into(indent + 2, out);
                right.display_into(indent + 2, out);
            }
            Self::Cte {
                name,
                recursive,
                definition,
                body,
                ..
            } => {
                out.push_str(&pad);
                let rec = if *recursive { " RECURSIVE" } else { "" };
                let _ = fmt::write(out, format_args!("Cte{rec}: {name}\n"));
                definition.display_into(indent + 2, out);
                body.display_into(indent + 2, out);
            }
            Self::LockRows {
                input,
                strength,
                wait_policy,
                ..
            } => {
                out.push_str(&pad);
                let s = match strength {
                    LockStrength::Update => "UPDATE",
                    LockStrength::NoKeyUpdate => "NO KEY UPDATE",
                    LockStrength::Share => "SHARE",
                    LockStrength::KeyShare => "KEY SHARE",
                };
                let w = match wait_policy {
                    LockWaitPolicy::Wait => "",
                    LockWaitPolicy::NoWait => " NOWAIT",
                    LockWaitPolicy::SkipLocked => " SKIP LOCKED",
                };
                let _ = fmt::write(out, format_args!("LockRows: FOR {s}{w}\n"));
                input.display_into(indent + 2, out);
            }
            Self::CreateIndex {
                index_name,
                table_name,
                columns,
                unique,
                if_not_exists,
                ..
            } => {
                out.push_str(&pad);
                let u = if *unique { "Unique" } else { "" };
                let inx = if *if_not_exists { " IF NOT EXISTS" } else { "" };
                let _ = fmt::write(
                    out,
                    format_args!(
                        "Create{u}Index{inx}: {index_name} ON {table_name} (cols=[{cols}])\n",
                        cols = columns
                            .iter()
                            .map(usize::to_string)
                            .collect::<Vec<_>>()
                            .join(",")
                    ),
                );
            }
            Self::DropTable {
                tables,
                if_exists,
                cascade,
                ..
            } => {
                out.push_str(&pad);
                let inx = if *if_exists { " IF EXISTS" } else { "" };
                let csc = if *cascade { " CASCADE" } else { "" };
                let _ = fmt::write(
                    out,
                    format_args!(
                        "DropTable{inx}: tables=[{names}]{csc}\n",
                        names = tables.join(", ")
                    ),
                );
            }
            Self::AlterTable {
                table_name, action, ..
            } => {
                out.push_str(&pad);
                match action {
                    LogicalAlterTableAction::AddColumn { column } => {
                        let _ = fmt::write(
                            out,
                            format_args!(
                                "AlterTable: {table_name} ADD COLUMN {} {:?}{}\n",
                                column.name,
                                column.data_type,
                                if column.nullable { "" } else { " NOT NULL" }
                            ),
                        );
                    }
                    LogicalAlterTableAction::DropColumn { column_name, .. } => {
                        let _ = fmt::write(
                            out,
                            format_args!("AlterTable: {table_name} DROP COLUMN {column_name}\n"),
                        );
                    }
                    LogicalAlterTableAction::RenameColumn {
                        old_name, new_name, ..
                    } => {
                        let _ = fmt::write(
                            out,
                            format_args!(
                                "AlterTable: {table_name} RENAME COLUMN {old_name} TO {new_name}\n"
                            ),
                        );
                    }
                    LogicalAlterTableAction::RenameTable { new_name } => {
                        let _ = fmt::write(
                            out,
                            format_args!("AlterTable: {table_name} RENAME TO {new_name}\n"),
                        );
                    }
                }
            }
            Self::Begin { .. } => {
                out.push_str(&pad);
                out.push_str("Begin\n");
            }
            Self::Commit { .. } => {
                out.push_str(&pad);
                out.push_str("Commit\n");
            }
            Self::Rollback { .. } => {
                out.push_str(&pad);
                out.push_str("Rollback\n");
            }
            Self::Savepoint { name, .. } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("Savepoint: {name}\n"));
            }
            Self::RollbackToSavepoint { name, .. } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("RollbackToSavepoint: {name}\n"));
            }
            Self::ReleaseSavepoint { name, .. } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("ReleaseSavepoint: {name}\n"));
            }
            Self::PrepareTransaction { gid, .. } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("PrepareTransaction: {gid}\n"));
            }
            Self::CommitPrepared { gid, .. } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("CommitPrepared: {gid}\n"));
            }
            Self::RollbackPrepared { gid, .. } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("RollbackPrepared: {gid}\n"));
            }
            Self::SetTransaction {
                isolation_level, ..
            } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("SetTransaction: {isolation_level:?}\n"));
            }
            Self::Listen { channel, .. } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("Listen: {channel}\n"));
            }
            Self::Notify {
                channel, payload, ..
            } => {
                out.push_str(&pad);
                match payload {
                    Some(p) => {
                        let _ = fmt::write(out, format_args!("Notify: {channel} '{p}'\n"));
                    }
                    None => {
                        let _ = fmt::write(out, format_args!("Notify: {channel}\n"));
                    }
                }
            }
            Self::Unlisten { channel, .. } => {
                out.push_str(&pad);
                match channel {
                    Some(c) => {
                        let _ = fmt::write(out, format_args!("Unlisten: {c}\n"));
                    }
                    None => {
                        out.push_str("Unlisten: *\n");
                    }
                }
            }
            Self::Explain {
                analyze,
                format,
                input,
                ..
            } => {
                out.push_str(&pad);
                let mode = if *analyze { "ANALYZE " } else { "" };
                let fmt_label = match format {
                    ExplainFormat::Text => "TEXT",
                    ExplainFormat::Json => "JSON",
                };
                let _ = fmt::write(out, format_args!("Explain {mode}({fmt_label})\n"));
                input.display_into(indent + 2, out);
            }
            Self::Copy {
                relation,
                columns,
                direction,
                source,
                format,
                ..
            } => {
                out.push_str(&pad);
                let dir = match direction {
                    CopyDirection::From => "FROM",
                    CopyDirection::To => "TO",
                };
                let src = match source {
                    CopySource::Stdin => "STDIN",
                    CopySource::Stdout => "STDOUT",
                };
                let fmt_label = match format {
                    CopyFormat::Text => "TEXT",
                    CopyFormat::Csv => "CSV",
                };
                let cols = if columns.is_empty() {
                    String::from("*")
                } else {
                    columns
                        .iter()
                        .map(usize::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                };
                let _ = fmt::write(
                    out,
                    format_args!("Copy: {relation} ({cols}) {dir} {src} FORMAT={fmt_label}\n"),
                );
            }
            Self::FunctionScan { name, args, .. } => {
                out.push_str(&pad);
                let arg_list = args
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                let _ = fmt::write(out, format_args!("FunctionScan: {name}({arg_list})\n"));
            }
        }
    }
}

impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display(0))
    }
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Value};

    use super::*;

    fn users_schema() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::nullable("score", DataType::Float64),
        ])
        .expect("schema invariants hold for test fixture")
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

    fn col(name: &str, index: usize, data_type: DataType) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.to_owned(),
            index,
            data_type,
        }
    }

    #[test]
    fn empty_plan_schema_round_trips() {
        let plan = LogicalPlan::Empty {
            schema: Schema::empty(),
        };
        assert!(plan.schema().is_empty());
    }

    #[test]
    fn scan_display_names_table() {
        let plan = LogicalPlan::Scan {
            table: "users".into(),
            schema: users_schema(),
            projection: None,
        };
        assert!(plan.display(0).contains("Scan: users"));
    }

    /// A `Values` plan's inferred schema columns have the right data types.
    #[test]
    fn values_schema_infers_column_types() {
        // Two rows: (1, 'alice'), (2, 'bob')
        let schema = Schema::new([
            Field::nullable("column1", DataType::Int32),
            Field::nullable("column2", DataType::Text { max_len: None }),
        ])
        .expect("schema ok");
        let plan = LogicalPlan::Values {
            rows: vec![
                vec![lit_i32(1), lit_text("alice")],
                vec![lit_i32(2), lit_text("bob")],
            ],
            schema,
        };
        assert_eq!(plan.schema().len(), 2);
        assert_eq!(plan.schema().field_at(0).data_type, DataType::Int32);
        assert_eq!(
            plan.schema().field_at(1).data_type,
            DataType::Text { max_len: None }
        );
        let dump = plan.display(0);
        assert!(dump.contains("Values: 2 row(s)"));
    }

    /// An `Insert` plan's schema matches the `RETURNING` projection.
    #[test]
    fn insert_plan_schema_matches_returning() {
        let returning_schema = Schema::new([
            Field::nullable("id", DataType::Int32),
            Field::nullable("score", DataType::Float64),
        ])
        .expect("schema ok");
        let source = LogicalPlan::Values {
            rows: vec![vec![lit_i32(42)]],
            schema: Schema::new([Field::nullable("column1", DataType::Int32)]).expect("schema ok"),
        };
        let plan = LogicalPlan::Insert {
            table: "users".into(),
            columns: vec![0],
            source: Box::new(source),
            on_conflict: None,
            returning: vec![
                (col("id", 0, DataType::Int32), "id".into()),
                (col("score", 1, DataType::Float64), "score".into()),
            ],
            schema: returning_schema.clone(),
        };
        assert_eq!(plan.schema(), &returning_schema);
    }

    /// An `Update` plan with no `RETURNING` has an empty schema.
    #[test]
    fn update_plan_schema_empty_when_no_returning() {
        let input = LogicalPlan::Scan {
            table: "users".into(),
            schema: users_schema(),
            projection: None,
        };
        let plan = LogicalPlan::Update {
            table: "users".into(),
            assignments: vec![(1, lit_i32(99))],
            input: Box::new(input),
            returning: vec![],
            schema: Schema::empty(),
        };
        assert!(plan.schema().is_empty());
    }

    /// The `display` for an `Insert` plan includes the table name and column
    /// indices.
    #[test]
    fn display_insert_includes_table_and_columns() {
        let source = LogicalPlan::Values {
            rows: vec![vec![lit_i32(1), lit_text("alice")]],
            schema: Schema::new([
                Field::nullable("column1", DataType::Int32),
                Field::nullable("column2", DataType::Text { max_len: None }),
            ])
            .expect("schema ok"),
        };
        let plan = LogicalPlan::Insert {
            table: "users".into(),
            columns: vec![0, 2, 3],
            source: Box::new(source),
            on_conflict: None,
            returning: vec![],
            schema: Schema::empty(),
        };
        let dump = plan.display(0);
        assert!(dump.contains("Insert:"), "got: {dump}");
        assert!(dump.contains("table=users"), "got: {dump}");
        assert!(dump.contains("cols=[0,2,3]"), "got: {dump}");
    }

    /// The aggregate output schema lists group-by columns first, then
    /// aggregate columns.
    #[test]
    fn aggregate_schema_orders_group_by_then_aggregates() {
        let input_schema = Schema::new([
            Field::required("id", DataType::Int32),
            Field::nullable("score", DataType::Float64),
        ])
        .expect("schema ok");
        let input = LogicalPlan::Scan {
            table: "users".into(),
            schema: input_schema,
            projection: None,
        };
        let agg_schema = Schema::new([
            Field::nullable("id", DataType::Int32),
            Field::nullable("cnt", DataType::Int64),
        ])
        .expect("schema ok");
        let plan = LogicalPlan::Aggregate {
            input: Box::new(input),
            group_by: vec![col("id", 0, DataType::Int32)],
            aggregates: vec![LogicalAggregateExpr {
                func: AggregateFunc::CountStar,
                arg: None,
                distinct: false,
                output_name: "cnt".into(),
                data_type: DataType::Int64,
            }],
            schema: agg_schema,
        };
        assert_eq!(plan.schema().len(), 2);
        assert_eq!(plan.schema().field_at(0).name, "id");
        assert_eq!(plan.schema().field_at(1).name, "cnt");
    }

    /// A Join plan's schema is the concatenation of the left and right schemas
    /// under outer-join nullability: right columns become nullable in a LEFT JOIN.
    #[test]
    fn join_schema_concatenates_under_outer_nullability() {
        let left_schema = Schema::new([Field::required("a", DataType::Int32)]).expect("schema ok");
        let right_schema =
            Schema::new([Field::nullable("b", DataType::Float64)]).expect("schema ok");
        let left = LogicalPlan::Scan {
            table: "t1".into(),
            schema: left_schema,
            projection: None,
        };
        let right = LogicalPlan::Scan {
            table: "t2".into(),
            schema: right_schema,
            projection: None,
        };
        // For a LEFT JOIN the right field 'b' is already nullable; left field
        // 'a' stays required.
        let join_schema = Schema::new([
            Field::required("a", DataType::Int32),   // left: stays required
            Field::nullable("b", DataType::Float64), // right: nullable
        ])
        .expect("schema ok");
        let plan = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: LogicalJoinType::LeftOuter,
            condition: LogicalJoinCondition::None,
            schema: join_schema,
        };
        assert_eq!(plan.schema().len(), 2);
        assert!(
            !plan.schema().field_at(0).nullable,
            "left col should be required"
        );
        assert!(
            plan.schema().field_at(1).nullable,
            "right col should be nullable"
        );
    }

    /// `display()` renders a nested join tree.
    #[test]
    fn display_renders_join_tree() {
        let s = Schema::new([Field::required("x", DataType::Int32)]).expect("schema ok");
        let scan_a = LogicalPlan::Scan {
            table: "a".into(),
            schema: s.clone(),
            projection: None,
        };
        let scan_b = LogicalPlan::Scan {
            table: "b".into(),
            schema: s,
            projection: None,
        };
        let join_schema = Schema::new([Field::required("x", DataType::Int32)]).expect("schema ok");
        let join = LogicalPlan::Join {
            left: Box::new(scan_a),
            right: Box::new(scan_b),
            join_type: LogicalJoinType::Inner,
            condition: LogicalJoinCondition::On(col("x", 0, DataType::Int32)),
            schema: join_schema,
        };
        let dump = join.display(0);
        assert!(dump.contains("Join[Inner]"), "got: {dump}");
        assert!(dump.contains("ON x"), "got: {dump}");
        assert!(dump.contains("Scan: a"), "got: {dump}");
        assert!(dump.contains("Scan: b"), "got: {dump}");
    }

    /// `display()` renders the aggregate node with function names.
    #[test]
    fn display_renders_aggregate_with_function_names() {
        let input = LogicalPlan::Scan {
            table: "t".into(),
            schema: Schema::new([Field::required("v", DataType::Int32)]).expect("schema ok"),
            projection: None,
        };
        let agg_schema =
            Schema::new([Field::nullable("total", DataType::Int64)]).expect("schema ok");
        let plan = LogicalPlan::Aggregate {
            input: Box::new(input),
            group_by: vec![],
            aggregates: vec![LogicalAggregateExpr {
                func: AggregateFunc::Sum,
                arg: Some(col("v", 0, DataType::Int32)),
                distinct: false,
                output_name: "total".into(),
                data_type: DataType::Int64,
            }],
            schema: agg_schema,
        };
        let dump = plan.display(0);
        assert!(dump.contains("sum"), "got: {dump}");
        assert!(dump.contains("total"), "got: {dump}");
    }
}
