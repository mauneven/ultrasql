//! The [`LogicalPlan`] enum — the binder's output and the optimizer's input.
//!
//! This is the central node type of the logical plan tree. It is a single
//! large enum; its inherent methods live in sibling modules
//! ([`analysis`](super::analysis), [`display`](super::display)). The
//! supporting node/DDL types live in [`node_types`](super::node_types) and
//! [`ddl_types`](super::ddl_types).

use ultrasql_core::{DataType, Schema};

use crate::expr::ScalarExpr;

use super::ddl_types::{
    CopyDirection, CopyFormat, CopySource, ExplainFormat, LogicalAlterTableAction,
    LogicalAlterViewAction, LogicalCheckConstraint, LogicalCommentTarget,
    LogicalDefaultPrivilegeOperation, LogicalExclusionConstraint, LogicalForeignKeyConstraint,
    LogicalPrivilegeObjectKind, LogicalPrivilegeSpec, LogicalRlsPolicy, LogicalRoleKind,
    LogicalRoleOptions, LogicalSequenceChange, LogicalSequenceOptions, LogicalTimePartition,
    LogicalUniqueConstraint,
};
use super::node_types::{
    LockStrength, LockWaitPolicy, LogicalAggregateExpr, LogicalAggregatingIndex,
    LogicalDescribeTarget, LogicalIndexMethod, LogicalIndexOption, LogicalJoinCondition,
    LogicalJoinType, LogicalMergeClause, LogicalOnConflict, LogicalPivotAggregate,
    LogicalPivotValue, LogicalSetOp, LogicalSetQuantifier, LogicalSetVariableAction,
    LogicalUnpivotColumn, LogicalWindowFunc, SortKey, TxnIsolationLevel,
};

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

    /// Window-function application. Wraps a child plan and appends one
    /// column carrying the per-row window result. The executor's
    /// `WindowAgg` operator consumes this shape directly.
    Window {
        /// Input plan (typically a `Filter`/`Scan` chain).
        input: Box<Self>,
        /// `PARTITION BY` keys; empty means a single partition over
        /// the whole input.
        partition_by: Vec<ScalarExpr>,
        /// `ORDER BY` keys; empty means the partition is unsorted (the
        /// kernel still emits ranks but in row-arrival order).
        order_by: Vec<SortKey>,
        /// Which window function to compute.
        func: LogicalWindowFunc,
        /// Display name for the appended output column. Borrowed by
        /// the binder when it re-references the window result from
        /// the outer projection.
        output_name: String,
        /// Output schema (`input.schema()` + one window-result column).
        schema: Schema,
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
    /// output schema uses stable synthetic column names
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

    /// Merge rows from a source relation into a target table.
    ///
    /// The source child supplies candidate rows. The executor evaluates `on`
    /// over the concatenated row `[target..., source...]`, checks ordered
    /// `clauses`, and applies at most one mutation per source row.
    Merge {
        /// Case-folded target table name.
        target: String,
        /// Optional target alias from `MERGE INTO target [AS alias]`.
        target_alias: Option<String>,
        /// Full target table schema.
        target_schema: Schema,
        /// Source plan from the `USING` relation.
        source: Box<Self>,
        /// Boolean-valued `ON` predicate over `[target..., source...]`.
        on: ScalarExpr,
        /// Ordered `WHEN` clauses.
        clauses: Vec<LogicalMergeClause>,
        /// Always empty in this wave; `MERGE RETURNING` is not supported.
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
        /// Explicit per-column collation OIDs from `COLLATE name`.
        /// `None` means the type's default collation applies.
        column_collations: Vec<Option<u32>>,
        /// Per-column default expressions. Same length as `columns`;
        /// `None` means omitted INSERT values become SQL NULL.
        defaults: Vec<Option<ScalarExpr>>,
        /// Per-column sequence default names produced by SERIAL-like
        /// pseudo-types. Same length as `columns`.
        sequence_defaults: Vec<Option<String>>,
        /// Per-column sequence options for sequence-backed defaults.
        /// Same length as `columns`; `None` means the column has no
        /// sequence default.
        sequence_options: Vec<Option<LogicalSequenceOptions>>,
        /// Per-column `GENERATED ALWAYS AS IDENTITY` flags. Same length as
        /// `columns`; `false` includes non-identity and BY DEFAULT identity.
        identity_always: Vec<bool>,
        /// Per-column stored generated expressions. Same length as `columns`;
        /// `None` means the column is not computed.
        generated_stored: Vec<Option<ScalarExpr>>,
        /// Row-level CHECK constraints bound against `columns`.
        checks: Vec<LogicalCheckConstraint>,
        /// UNIQUE / PRIMARY KEY constraints that should build unique
        /// B-tree indexes after the table entry is created.
        unique_constraints: Vec<LogicalUniqueConstraint>,
        /// FOREIGN KEY constraints that should be enforced by DML.
        foreign_keys: Vec<LogicalForeignKeyConstraint>,
        /// EXCLUDE constraints that should be enforced by DML.
        exclusion_constraints: Vec<LogicalExclusionConstraint>,
        /// Optional native time-range partitioning metadata.
        partition: Option<LogicalTimePartition>,
        /// Whether `IF NOT EXISTS` was specified. When true the
        /// executor short-circuits if the relation already exists.
        if_not_exists: bool,
        /// Always [`Schema::empty`]; DDL emits no rows. Carried for
        /// uniform [`LogicalPlan::schema`] access by callers.
        schema: Schema,
    },

    /// Create an append-only materialized view over one source relation.
    ///
    /// The binder stores both the heap-backed output schema (`columns`)
    /// and the bound SELECT plan that feeds initial population and
    /// append-only maintenance.
    CreateMaterializedView {
        /// Case-folded bare materialized-view name.
        table_name: String,
        /// SQL namespace, usually `"public"`.
        namespace: String,
        /// Heap-backed row shape of the materialized view.
        columns: Schema,
        /// Bound SELECT source.
        source: Box<Self>,
        /// Whether `IF NOT EXISTS` was specified.
        if_not_exists: bool,
        /// Always [`Schema::empty`]; DDL emits no rows.
        schema: Schema,
    },

    /// Create a regular view over a stored SELECT definition.
    CreateView {
        /// Case-folded bare view name.
        table_name: String,
        /// SQL namespace, usually `"public"`.
        namespace: String,
        /// Row shape exposed by the view.
        columns: Schema,
        /// Bound SELECT source.
        source: Box<Self>,
        /// Trimmed SELECT SQL text persisted as the view definition.
        source_sql: String,
        /// Whether `OR REPLACE` was specified.
        or_replace: bool,
        /// Always [`Schema::empty`]; DDL emits no rows.
        schema: Schema,
    },

    /// Create a user-defined enum type.
    CreateTypeEnum {
        /// Case-folded bare type name.
        type_name: String,
        /// SQL namespace, usually `"public"`.
        namespace: String,
        /// Enum labels in declaration order.
        labels: Vec<String>,
        /// Always [`Schema::empty`]; DDL emits no rows.
        schema: Schema,
    },

    /// Create a user-defined composite type.
    CreateTypeComposite {
        /// Case-folded bare type name.
        type_name: String,
        /// SQL namespace, usually `"public"`.
        namespace: String,
        /// Composite attributes in declaration order.
        attributes: Schema,
        /// Always [`Schema::empty`]; DDL emits no rows.
        schema: Schema,
    },

    /// Create a user-defined domain type.
    CreateDomain {
        /// Case-folded bare domain name.
        domain_name: String,
        /// SQL namespace, usually `"public"`.
        namespace: String,
        /// Domain base type.
        base_type: DataType,
        /// Whether the domain rejects NULL values.
        not_null: bool,
        /// Domain CHECK predicates bound against a synthetic `VALUE` column.
        checks: Vec<LogicalCheckConstraint>,
        /// Always [`Schema::empty`]; DDL emits no rows.
        schema: Schema,
    },

    /// Create a user-defined operator catalog entry.
    CreateOperator {
        /// Operator token sequence, such as `===`.
        operator_name: String,
        /// SQL namespace, usually `"public"`.
        namespace: String,
        /// Optional left operand type.
        left_type: Option<DataType>,
        /// Optional right operand type.
        right_type: Option<DataType>,
        /// Built-in function backing the operator.
        procedure: String,
        /// Result type declared by the backing function.
        result_type: DataType,
        /// Always [`Schema::empty`]; DDL emits no rows.
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
    /// `USING` joins where the joined column appears only once. Logical
    /// `Semi` and `Anti` joins expose only the left schema.
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

    /// Table-factor `PIVOT` transform.
    ///
    /// The output schema is `[group columns..., pivot value columns...]`.
    /// Group columns are inferred as input columns not consumed by the
    /// pivot key or aggregate argument.
    Pivot {
        /// Input plan to pivot.
        input: Box<Self>,
        /// Input columns carried through as grouping keys.
        group_columns: Vec<usize>,
        /// Input column whose values select the pivot bucket.
        pivot_column: usize,
        /// Aggregate computed per group and pivot value.
        aggregate: LogicalPivotAggregate,
        /// Constant pivot values and output names.
        pivot_values: Vec<LogicalPivotValue>,
        /// Output schema.
        schema: Schema,
    },

    /// Table-factor `UNPIVOT` transform.
    ///
    /// The output schema is `[passthrough columns..., name column, value column]`.
    Unpivot {
        /// Input plan to unpivot.
        input: Box<Self>,
        /// Input columns carried through unchanged.
        passthrough_columns: Vec<usize>,
        /// Input columns expanded into rows.
        columns: Vec<LogicalUnpivotColumn>,
        /// Output name column.
        name_column: String,
        /// Output value column.
        value_column: String,
        /// Whether rows with NULL unpivoted values are retained.
        include_nulls: bool,
        /// Output schema.
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
    /// `WITH RECURSIVE`, `recursive = true`; the server lowering path
    /// executes the anchor + fixpoint loop for this flag (see the
    /// `recursive` field note below).
    Cte {
        /// CTE name (used in `Scan` references inside `body`).
        name: String,
        /// Whether `WITH RECURSIVE` was specified.
        ///
        /// # Note
        /// When set, the server lowering path runs the anchor + fixpoint loop
        /// (`pipeline::cte_helpers::lower_recursive_cte`: `UNION`/`UNION ALL`
        /// with a bounded iteration cap). The standalone
        /// `executor::physical::build_operator` path used by some unit tests
        /// does not implement the fixpoint and materializes the definition
        /// non-recursively.
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
    /// the child operator with an `ultrasql_executor::LockRows` callback
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
    /// Expression keys, `INCLUDE` covering columns, and partial-index
    /// predicates are bound into runtime metadata so the server can
    /// build and maintain the index under DML. Expression and partial
    /// indexes use conservative scan selection; correctness is provided
    /// by the sequential-scan fallback whenever a predicate cannot be
    /// proven indexable.
    CreateIndex {
        /// Index name (caller-supplied or binder-synthesised). Always
        /// lowercase.
        index_name: String,
        /// Namespace that owns the index. Indexes live in the same
        /// namespace as their parent table.
        index_namespace: String,
        /// Target table (lowercase).
        table_name: String,
        /// 0-based column indices into the table schema, in index key
        /// order. Expression-key indexes carry an empty vector because
        /// there is no single attnum to store in `pg_index.indkey`.
        columns: Vec<usize>,
        /// Bound key expressions. Bare-column indexes carry the same
        /// columns as [`Self::CreateIndex::columns`] in expression
        /// form; expression indexes carry the actual key expression.
        key_exprs: Vec<ScalarExpr>,
        /// Optional operator class per key (`vector_l2_ops`, etc.).
        opclasses: Vec<Option<String>>,
        /// Bound index storage options from `WITH (...)`.
        index_options: Vec<LogicalIndexOption>,
        /// 0-based table columns listed in `INCLUDE (...)`.
        include_columns: Vec<usize>,
        /// Bound partial-index predicate, if any.
        predicate: Option<ScalarExpr>,
        /// Access method requested by `USING`.
        method: LogicalIndexMethod,
        /// Aggregating-index metadata when this is `CREATE AGGREGATING INDEX`.
        aggregating: Option<LogicalAggregatingIndex>,
        /// Whether `UNIQUE` was specified.
        unique: bool,
        /// Whether this index backs a `PRIMARY KEY` constraint. Only set
        /// for the synthesised plan that `ALTER TABLE ... ADD PRIMARY KEY`
        /// lowers into; a plain `CREATE INDEX` is never primary.
        primary_key: bool,
        /// Whether `CONCURRENTLY` was specified.
        concurrently: bool,
        /// Whether `IF NOT EXISTS` was specified.
        if_not_exists: bool,
        /// Always [`Schema::empty`]; DDL emits no rows.
        schema: Schema,
    },

    /// Drop one or more indexes.
    DropIndex {
        /// Lowercase index names to drop, in the user's order.
        indexes: Vec<String>,
        /// Explicit namespace per index, if the statement qualified it.
        index_namespaces: Vec<Option<String>>,
        /// Whether `IF EXISTS` was specified.
        if_exists: bool,
        /// Whether `CASCADE` was specified.
        cascade: bool,
        /// Always [`Schema::empty`].
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

    /// Alter an existing regular view.
    AlterView {
        /// Lowercase target view name.
        view_name: String,
        /// Resolved action.
        action: LogicalAlterViewAction,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `CREATE POLICY name ON table USING (...) WITH CHECK (...)`.
    CreatePolicy {
        /// Bound row-security policy metadata.
        policy: LogicalRlsPolicy,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `CREATE ROLE` / `CREATE USER`.
    CreateRole {
        /// Whether the statement used `ROLE` or `USER`.
        kind: LogicalRoleKind,
        /// Case-folded role name.
        role_name: String,
        /// Partial role attributes.
        options: LogicalRoleOptions,
        /// Whether `IF NOT EXISTS` was specified.
        if_not_exists: bool,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `ALTER ROLE` / `ALTER USER`.
    AlterRole {
        /// Whether the statement used `ROLE` or `USER`.
        kind: LogicalRoleKind,
        /// Case-folded role name.
        role_name: String,
        /// Partial role attributes to change.
        options: LogicalRoleOptions,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `DROP ROLE` / `DROP USER`.
    DropRole {
        /// Whether the statement used `ROLE` or `USER`.
        kind: LogicalRoleKind,
        /// Case-folded role names.
        roles: Vec<String>,
        /// Whether `IF EXISTS` was specified.
        if_exists: bool,
        /// Whether `CASCADE` was specified.
        cascade: bool,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `GRANT ... ON ... TO ...`.
    GrantPrivileges {
        /// Expanded privilege list, including optional columns.
        privileges: Vec<LogicalPrivilegeSpec>,
        /// Object class named after `ON`.
        object_kind: LogicalPrivilegeObjectKind,
        /// Folded target object names.
        objects: Vec<String>,
        /// Folded grantee role names.
        grantees: Vec<String>,
        /// Whether `WITH GRANT OPTION` was specified.
        grant_option: bool,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `REVOKE ... ON ... FROM ...`.
    RevokePrivileges {
        /// Expanded privilege list, including optional columns.
        privileges: Vec<LogicalPrivilegeSpec>,
        /// Object class named after `ON`.
        object_kind: LogicalPrivilegeObjectKind,
        /// Folded target object names.
        objects: Vec<String>,
        /// Folded grantee role names.
        grantees: Vec<String>,
        /// Whether `GRANT OPTION FOR` was specified.
        grant_option_for: bool,
        /// Whether `CASCADE` was specified.
        cascade: bool,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `ALTER DEFAULT PRIVILEGES ... GRANT/REVOKE ...`.
    AlterDefaultPrivileges {
        /// Roles whose future objects receive the default ACL. Empty
        /// means the current role at execution time.
        target_roles: Vec<String>,
        /// Optional schema filter. Empty means every schema.
        schemas: Vec<String>,
        /// Whether the action grants or revokes default ACL entries.
        operation: LogicalDefaultPrivilegeOperation,
        /// Expanded privilege list. Column lists are rejected by the binder.
        privileges: Vec<LogicalPrivilegeSpec>,
        /// Future object class named after `ON`.
        object_kind: LogicalPrivilegeObjectKind,
        /// Folded grantee role names.
        grantees: Vec<String>,
        /// Whether `WITH GRANT OPTION` was specified for grant actions.
        grant_option: bool,
        /// Whether `GRANT OPTION FOR` was specified for revoke actions.
        grant_option_for: bool,
        /// Whether `CASCADE` was specified for revoke actions.
        cascade: bool,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `GRANT role [, ...] TO role [, ...]`.
    GrantRole {
        /// Folded granted role names.
        roles: Vec<String>,
        /// Folded recipient role names.
        grantees: Vec<String>,
        /// Whether `WITH ADMIN OPTION` was specified.
        admin_option: bool,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `REVOKE role [, ...] FROM role [, ...]`.
    RevokeRole {
        /// Folded revoked role names.
        roles: Vec<String>,
        /// Folded recipient role names.
        grantees: Vec<String>,
        /// Whether `ADMIN OPTION FOR` was specified.
        admin_option_for: bool,
        /// Whether `CASCADE` was specified.
        cascade: bool,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `CREATE SCHEMA [IF NOT EXISTS] name`.
    CreateSchema {
        /// Folded schema name.
        schema_name: String,
        /// Whether `IF NOT EXISTS` was specified.
        if_not_exists: bool,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `DROP SCHEMA [IF EXISTS] name [, ...]`.
    DropSchema {
        /// Folded schema names.
        schemas: Vec<String>,
        /// Whether `IF EXISTS` was specified.
        if_exists: bool,
        /// Whether `CASCADE` was specified.
        cascade: bool,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `CREATE SEQUENCE [IF NOT EXISTS] name ...`.
    CreateSequence {
        /// Case-folded sequence name.
        sequence_name: String,
        /// SQL namespace, usually `"public"`.
        namespace: String,
        /// Resolved sequence options.
        options: LogicalSequenceOptions,
        /// Whether `IF NOT EXISTS` was specified.
        if_not_exists: bool,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `ALTER SEQUENCE name ...`.
    AlterSequence {
        /// Case-folded sequence name.
        sequence_name: String,
        /// Explicit SQL namespace from a qualified name, or `None` for bare names.
        namespace: Option<String>,
        /// Partial option changes.
        options: LogicalSequenceChange,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `DROP SEQUENCE [IF EXISTS] name [, ...]`.
    DropSequence {
        /// Case-folded sequence names.
        sequences: Vec<String>,
        /// Explicit namespaces aligned with `sequences`; `None` means bare name.
        sequence_namespaces: Vec<Option<String>>,
        /// Whether `IF EXISTS` was specified.
        if_exists: bool,
        /// Whether `CASCADE` was specified.
        cascade: bool,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `COMMENT ON TABLE/COLUMN ... IS ...`.
    Comment {
        /// Object being commented.
        target: LogicalCommentTarget,
        /// Comment text. `None` deletes the existing comment.
        comment: Option<String>,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `CHECKPOINT` — force a WAL durability barrier, flush eligible dirty
    /// pages, and append a checkpoint WAL record.
    ///
    /// `schema` is always [`Schema::empty`].
    Checkpoint {
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `EXPORT DATABASE TO 'path'` — write a deterministic logical dump.
    ///
    /// `schema` is always [`Schema::empty`].
    ExportDatabase {
        /// Destination directory path.
        path: String,
        /// Always [`Schema::empty`].
        schema: Schema,
    },

    /// `IMPORT DATABASE FROM 'path'` — restore a deterministic logical dump.
    ///
    /// `schema` is always [`Schema::empty`].
    ImportDatabase {
        /// Source directory path.
        path: String,
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

    /// `SET` / `SHOW` / `RESET` for supported runtime variables.
    SetVariable {
        /// Lower-cased variable name.
        name: String,
        /// Action to perform.
        action: LogicalSetVariableAction,
        /// Optional value string for `SET`.
        value: Option<String>,
        /// Empty for `SET` / `RESET`; one text column for `SHOW`.
        schema: Schema,
    },

    /// `DESCRIBE [TABLE|VIEW] object` or `DESCRIBE SELECT ...`.
    Describe {
        /// Bound target whose field metadata should be returned.
        target: LogicalDescribeTarget,
        /// Fixed six-column DESCRIBE output schema.
        schema: Schema,
    },

    /// `SUMMARIZE table_name`.
    Summarize {
        /// Case-folded bare table name.
        table: String,
        /// Case-folded schema name.
        namespace: String,
        /// Stored table schema to summarize.
        target_schema: Schema,
        /// Fixed summary output schema.
        schema: Schema,
    },

    /// `SET ROLE role` / `SET ROLE NONE` / `RESET ROLE`.
    SetRole {
        /// Folded target role. `None` resets to session user.
        role_name: Option<String>,
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
    /// The lowerer dispatches on `name` to construct the matching table
    /// function scan.
    FunctionScan {
        /// Function name (case-folded).
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
        /// Case-folded target table name. `None` for `COPY (SELECT ...)`.
        relation: Option<String>,
        /// Bound query input for `COPY (SELECT ...) TO ...`.
        input: Option<Box<Self>>,
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
        /// Whether CSV COPY should infer dialect/header metadata from
        /// the source before streaming rows.
        auto_detect: bool,
        /// Whether bad COPY FROM rows are quarantined instead of
        /// aborting the whole load.
        ignore_errors: bool,
        /// Maximum bad rows tolerated before aborting COPY FROM.
        max_errors: u64,
        /// Optional reject table for quarantined bad rows.
        reject_table: Option<String>,
        /// Row shape of the data stream — derived from `columns` and
        /// the target table's schema.
        schema: Schema,
    },
}
