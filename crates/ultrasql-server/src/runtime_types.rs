//! Same-process runtime types: txn state, constraints, operators, schemas,
//! RLS policies, and view runtimes.
//!
//! Moved verbatim from the crate root; behavior unchanged.
use super::*;

/// One column in an `ultrasql-local` query result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalResultColumn {
    /// Display name returned by the planner/executor.
    pub name: String,
    /// Wire type OID for the text-encoded value.
    pub type_oid: u32,
}

/// Materialised result returned by the local in-process query runner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalQueryOutput {
    /// Result columns in output order.
    pub columns: Vec<LocalResultColumn>,
    /// Text-format rows. `None` represents SQL `NULL`.
    pub rows: Vec<Vec<Option<String>>>,
    /// PostgreSQL-style command tag, e.g. `SELECT 1`.
    pub command_tag: String,
}

/// Execute one read-only SQL query against an in-process UltraSQL engine.
///
/// This is the library entry point used by `ultrasql-local`: no TCP
/// listener, no PostgreSQL wire handshake, just parser -> binder ->
/// executor over local files and in-memory catalogs.
pub fn execute_local_query(sql: &str) -> Result<LocalQueryOutput, ServerError> {
    let server = Arc::new(Server::with_sample_database());
    server.execute_local_query(sql)
}

/// Per-session transaction-block state.
///
/// PostgreSQL exposes three transaction states to its clients via the
/// `ReadyForQuery` status byte (`'I'`, `'T'`, `'E'`). UltraSQL mirrors
/// these states so any libpq-style client that depends on the byte to
/// decide whether to issue `ROLLBACK` (e.g. tokio-postgres, psql,
/// pgbench) behaves identically.
///
/// The state is per-connection and accessed only by the connection's
/// own task, so no synchronisation primitive is needed (AGENTS.md §5).
///
/// State transitions:
///
/// ```text
///                        BEGIN
///        Idle ───────────────────────────────► InTransaction
///         ▲                                          │
///         │ COMMIT (no-op + warning when Idle)       │
///         │ ROLLBACK (no-op + warning when Idle)     │
///         │                                          │
///         │             COMMIT (success)             │
///         │ ◄────────────────────────────────────────┤
///         │                                          │ statement
///         │             ROLLBACK                     │ errored
///         │ ◄────────────────────────────────────────┼─────┐
///         │                                          │     │
///         │             COMMIT  (treated as          │     ▼
///         │              ROLLBACK; tag = "ROLLBACK") │   Failed
///         │ ◄────────────────────────────────────────┼─────┤
///         │             ROLLBACK                     │     │
///         └──────────────────────────────────────────┴─────┘
/// ```
///
/// `Idle` ↔ `ReadyForQuery` `'I'`. `InTransaction` ↔ `'T'`. `Failed` ↔ `'E'`.
#[derive(Debug)]
pub enum TxnState {
    /// No explicit transaction block is open. Each statement runs
    /// inside its own autocommit transaction.
    Idle,
    /// An explicit `BEGIN` is in effect. Statements use this txn's xid
    /// + snapshot until the user issues `COMMIT` or `ROLLBACK`.
    InTransaction(Transaction),
    /// A prior statement inside an explicit transaction errored. Until
    /// the user sends `COMMIT` (treated as `ROLLBACK`) or `ROLLBACK`,
    /// every subsequent statement returns the standard PostgreSQL
    /// error: `current transaction is aborted, commands ignored until
    /// end of transaction block` (SQLSTATE `25P02`).
    Failed(Transaction),
}

/// Runtime table constraints that are not yet persisted in catalog heap rows.
///
/// `TableEntry` deliberately lives below the planner crate, so it cannot carry
/// bound [`ScalarExpr`] values. The server keeps this side map keyed by table
/// OID and threads it into DML lowering until `pg_attrdef` / `pg_constraint`
/// persistence grows a typed expression codec.
#[derive(Clone, Debug, Default)]
pub struct TableRuntimeConstraints {
    /// Per-column default expressions; same order as the table schema.
    pub defaults: Vec<Option<ScalarExpr>>,
    /// Per-column sequence names used by SERIAL-like defaults.
    pub sequence_defaults: Vec<Option<String>>,
    /// Per-column `GENERATED ALWAYS AS IDENTITY` flags.
    pub identity_always: Vec<bool>,
    /// Per-column `GENERATED ALWAYS AS (expr) STORED` expressions.
    pub generated_stored: Vec<Option<ScalarExpr>>,
    /// Bound CHECK predicates evaluated against each inserted/updated row.
    pub checks: Vec<RuntimeCheckConstraint>,
    /// Non-deferrable FOREIGN KEY constraints evaluated by DML.
    pub foreign_keys: Vec<RuntimeForeignKeyConstraint>,
    /// EXCLUDE constraints evaluated by DML.
    pub exclusion_constraints: Vec<RuntimeExclusionConstraint>,
    /// Runtime metadata for expression, partial, and covering indexes.
    ///
    /// Persistent `pg_index` rows still store only the portable column
    /// slice; this side map lets same-process DML maintain indexes whose
    /// key is an expression or whose row membership is partial.
    pub indexes: std::collections::HashMap<ultrasql_core::Oid, RuntimeIndexMetadata>,
}

/// Runtime domain metadata keyed by domain `pg_type.oid`.
#[derive(Clone, Debug)]
pub struct DomainRuntimeConstraints {
    /// Underlying base type used by storage and domain `VALUE` checks.
    pub base_type: DataType,
    /// Domain-level NOT NULL constraint.
    pub not_null: bool,
    /// Bound CHECK predicates against a synthetic `VALUE` column.
    pub checks: Vec<RuntimeCheckConstraint>,
}

/// Same-process user-defined operator metadata exposed through `pg_operator`.
#[derive(Clone, Debug)]
pub struct RuntimeOperator {
    /// Stable runtime OID for the operator row.
    pub oid: u32,
    /// Operator token sequence, such as `===`.
    pub name: String,
    /// SQL namespace name.
    pub namespace: String,
    /// Optional left operand type.
    pub left_type: Option<DataType>,
    /// Optional right operand type.
    pub right_type: Option<DataType>,
    /// Backing function/procedure name.
    pub procedure: String,
    /// Result type returned by the backing function.
    pub result_type: DataType,
}

pub(crate) fn runtime_operator_signature(
    namespace: &str,
    name: &str,
    left_type: &Option<DataType>,
    right_type: &Option<DataType>,
) -> String {
    let left = left_type
        .as_ref()
        .map_or_else(|| "none".to_owned(), ToString::to_string);
    let right = right_type
        .as_ref()
        .map_or_else(|| "none".to_owned(), ToString::to_string);
    format!("{namespace}.{name}({left},{right})")
}

pub(crate) fn runtime_operator_oid(signature: &str) -> u32 {
    const USER_OPERATOR_OID_BASE: u32 = 80_000;
    const USER_OPERATOR_OID_SPACE: u32 = 1_000_000;
    let hash = signature
        .as_bytes()
        .iter()
        .fold(0x811c_9dc5_u32, |acc, byte| {
            (acc ^ u32::from(*byte)).wrapping_mul(0x0100_0193)
        });
    USER_OPERATOR_OID_BASE + (hash % USER_OPERATOR_OID_SPACE)
}

pub(crate) fn runtime_schema_oid(name: &str) -> u32 {
    const USER_SCHEMA_OID_BASE: u32 = 70_000;
    const USER_SCHEMA_OID_SPACE: u32 = 1_000_000;
    let hash = name.as_bytes().iter().fold(0x811c_9dc5_u32, |acc, byte| {
        (acc ^ u32::from(*byte)).wrapping_mul(0x0100_0193)
    });
    USER_SCHEMA_OID_BASE + (hash % USER_SCHEMA_OID_SPACE)
}

pub(crate) fn builtin_schema_name(name: &str) -> bool {
    matches!(name, "pg_catalog" | "information_schema" | "public")
}

/// Runtime SQL schema metadata keyed by folded schema name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeSchema {
    /// Folded schema name.
    pub name: String,
    /// Folded owner role name.
    pub owner_role: String,
}

/// Same-process row-level security metadata keyed by table OID.
///
/// The first enforced slice supports tenant predicates generated by the RAG
/// helpers: `tenant_id = current_setting('ultrasql.tenant_id', true)`.
#[derive(Clone, Debug, Default)]
pub struct TableRowSecurity {
    /// Role that owns the table for PostgreSQL-style owner bypass.
    pub owner_role: String,
    /// Whether RLS is enabled for this table.
    pub enabled: bool,
    /// Policies attached to the table.
    pub policies: Vec<RuntimeRlsPolicy>,
}

/// Runtime row-security policy.
#[derive(Clone, Debug)]
pub struct RuntimeRlsPolicy {
    /// Policy name.
    pub name: String,
    /// Permissive/restrictive combination mode.
    pub permissiveness: RuntimeRlsPermissiveness,
    /// Command class this policy applies to.
    pub command: RuntimeRlsCommand,
    /// Role names this policy applies to. Empty means all roles.
    pub roles: Vec<String>,
    /// Read visibility predicate.
    pub using: Option<RuntimeTenantPolicyExpr>,
    /// Write acceptance predicate.
    pub with_check: Option<RuntimeTenantPolicyExpr>,
}

impl RuntimeRlsPolicy {
    /// Return whether this policy applies to one of the session's inherited roles.
    #[must_use]
    pub fn applies_to_roles(&self, inherited_roles: &[String]) -> bool {
        self.roles.is_empty()
            || self.roles.iter().any(|role| {
                role == "public"
                    || inherited_roles
                        .iter()
                        .any(|inherited| inherited.eq_ignore_ascii_case(role))
            })
    }
}

/// Runtime row-security policy combination mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeRlsPermissiveness {
    /// PostgreSQL `AS PERMISSIVE`.
    Permissive,
    /// PostgreSQL `AS RESTRICTIVE`.
    Restrictive,
}

/// Runtime row-security policy command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeRlsCommand {
    /// `FOR ALL`.
    All,
    /// `FOR SELECT`.
    Select,
    /// `FOR INSERT`.
    Insert,
    /// `FOR UPDATE`.
    Update,
    /// `FOR DELETE`.
    Delete,
}

impl RuntimeRlsCommand {
    /// Return whether this policy command applies to a statement command.
    #[must_use]
    pub const fn applies_to(self, statement: Self) -> bool {
        matches!(self, Self::All)
            || matches!(
                (self, statement),
                (Self::Select, Self::Select)
                    | (Self::Insert, Self::Insert)
                    | (Self::Update, Self::Update)
                    | (Self::Delete, Self::Delete)
            )
    }
}

/// Runtime tenant predicate of the form `column = current_setting(setting, true)`.
#[derive(Clone, Debug)]
pub struct RuntimeTenantPolicyExpr {
    /// Target table column index.
    pub column_index: usize,
    /// Target table column name.
    pub column_name: String,
    /// Session setting name.
    pub setting_name: String,
}

/// Runtime metadata for one append-only materialized view.
///
/// The catalog stores the view as a heap-backed relation. This sidecar keeps
/// the bound source query and how many source-query output rows have already
/// been copied into the materialized heap.
#[derive(Debug)]
pub struct MaterializedViewRuntime {
    /// Folded materialized-view table name.
    pub view_table: String,
    /// Folded single source table name.
    pub source_table: String,
    /// Bound append-safe source query.
    pub source: LogicalPlan,
    /// Number of source-query output rows already materialized.
    pub materialized_rows: std::sync::atomic::AtomicU64,
}

/// Runtime metadata for one regular SQL view.
///
/// The persistent catalog stores the view's exposed schema as a
/// `RelKind::View` relation. This sidecar keeps the stored query text and
/// the current bound source plan used to expand `Scan(view)` at execution
/// time.
#[derive(Debug)]
pub struct RegularViewRuntime {
    /// Canonical folded view lookup key.
    pub view_table: String,
    /// Trimmed `SELECT` text from the `CREATE VIEW` statement.
    pub source_sql: String,
    /// Session search path captured at creation for restart rebinding.
    pub search_path: Option<String>,
    /// Bound query used as the view source.
    pub source: LogicalPlan,
    /// Exposed view schema, including explicit column aliases.
    pub columns: Schema,
}

pub(crate) fn append_only_materialized_source_table(plan: &LogicalPlan) -> Option<&str> {
    match plan {
        LogicalPlan::Scan { table, .. } => Some(table.as_str()),
        LogicalPlan::Filter { input, .. } | LogicalPlan::Project { input, .. } => {
            append_only_materialized_source_table(input)
        }
        _ => None,
    }
}

pub(crate) fn is_regular_view_entry(entry: &TableEntry) -> bool {
    entry
        .options
        .iter()
        .any(|(key, value)| key == "ultrasql.relkind" && value == "view")
}

pub(crate) fn view_source_shape_matches(source_schema: &Schema, view_schema: &Schema) -> bool {
    source_schema.len() == view_schema.len()
        && source_schema
            .fields()
            .iter()
            .zip(view_schema.fields())
            .all(|(source, view)| {
                source.data_type == view.data_type && source.nullable == view.nullable
            })
}
