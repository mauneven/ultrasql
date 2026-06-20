//! Supporting types for DDL and access-control logical plan nodes.
//!
//! Constraints, sequence/role/privilege options, RLS policy metadata, and
//! the `ALTER TABLE` / `ALTER VIEW` action enums referenced by
//! [`LogicalPlan`](super::LogicalPlan) DDL variants. Split out of the
//! original monolithic `plan.rs` verbatim.

use ultrasql_core::Field;

use crate::expr::ScalarExpr;

use super::node_types::{LogicalIndexMethod, LogicalTableOption};

/// A bound `CHECK (...)` constraint carried by `CREATE TABLE`.
#[derive(Clone, Debug, PartialEq)]
pub struct LogicalCheckConstraint {
    /// Constraint name, explicit or binder-synthesised.
    pub name: String,
    /// Boolean expression evaluated against each new row.
    pub expr: ScalarExpr,
}

/// A bound UNIQUE or PRIMARY KEY constraint carried by `CREATE TABLE`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalUniqueConstraint {
    /// Constraint name, explicit or binder-synthesised.
    pub name: String,
    /// 0-based key column indices.
    pub columns: Vec<usize>,
    /// Whether this constraint is a PRIMARY KEY.
    pub primary_key: bool,
}

/// A bound FOREIGN KEY constraint carried by `CREATE TABLE`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalForeignKeyConstraint {
    /// Constraint name, explicit or binder-synthesised.
    pub name: String,
    /// 0-based key column indices on the referencing table.
    pub columns: Vec<usize>,
    /// Case-folded referenced table name.
    pub target_table: String,
    /// 0-based key column indices on the referenced table.
    pub target_columns: Vec<usize>,
    /// Action when a referenced row is deleted.
    pub on_delete: LogicalReferentialAction,
    /// Action when a referenced key is updated.
    pub on_update: LogicalReferentialAction,
    /// Whether this constraint may be checked at transaction commit.
    pub deferrable: bool,
    /// Whether this deferrable constraint starts in deferred mode.
    pub initially_deferred: bool,
}

/// A bound `EXCLUDE USING gist` constraint carried by `CREATE TABLE`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalExclusionConstraint {
    /// Constraint name, explicit or binder-synthesised.
    pub name: String,
    /// Access method requested by `USING`.
    pub method: LogicalIndexMethod,
    /// Column/operator pairs evaluated pairwise against existing rows.
    pub elements: Vec<LogicalExclusionElement>,
}

/// One column/operator pair in a bound exclusion constraint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalExclusionElement {
    /// 0-based table column index.
    pub column: usize,
    /// Operator used for pairwise comparison.
    pub op: ultrasql_parser::ast::BinaryOp,
}

/// Native time-range partitioning metadata bound from `CREATE TABLE`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalTimePartition {
    /// Partition key column name.
    pub column: String,
    /// Partition key column index in the table schema.
    pub column_index: usize,
}

/// Bound referential action for a foreign key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalReferentialAction {
    /// `NO ACTION`.
    NoAction,
    /// `RESTRICT`.
    Restrict,
    /// `CASCADE`.
    Cascade,
    /// `SET NULL`.
    SetNull,
    /// `SET DEFAULT`.
    SetDefault,
}

/// Bound target of a `COMMENT ON` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogicalCommentTarget {
    /// `COMMENT ON TABLE table`.
    Table {
        /// Folded table name.
        table: String,
    },
    /// `COMMENT ON INDEX index`.
    Index {
        /// Folded index name.
        index: String,
        /// Explicit namespace, if the statement qualified the index.
        namespace: Option<String>,
    },
    /// `COMMENT ON COLUMN table.column`.
    Column {
        /// Folded table name.
        table: String,
        /// Folded column name.
        column: String,
        /// 1-based attribute number.
        attnum: i32,
    },
}

/// Resolved `CREATE SEQUENCE` options.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogicalSequenceOptions {
    /// `START WITH`.
    pub start: i64,
    /// `INCREMENT BY`.
    pub increment: i64,
    /// `MINVALUE`; `None` means engine default.
    pub min: Option<i64>,
    /// `MAXVALUE`; `None` means engine default.
    pub max: Option<i64>,
    /// `CACHE`.
    pub cache: u32,
    /// `CYCLE`.
    pub cycle: bool,
}

impl Default for LogicalSequenceOptions {
    fn default() -> Self {
        Self {
            start: 1,
            increment: 1,
            min: None,
            max: None,
            cache: 1,
            cycle: false,
        }
    }
}

/// Partial `ALTER SEQUENCE` option change.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LogicalSequenceChange {
    /// New start value.
    pub start: Option<i64>,
    /// Restart current value. `Some(None)` means `RESTART` without an
    /// explicit value, so the executor restarts at the configured start
    /// value after applying any `START WITH` change.
    pub restart: Option<Option<i64>>,
    /// New increment.
    pub increment: Option<i64>,
    /// New minimum. `Some(None)` means `NO MINVALUE`.
    pub min: Option<Option<i64>>,
    /// New maximum. `Some(None)` means `NO MAXVALUE`.
    pub max: Option<Option<i64>>,
    /// New cache size.
    pub cache: Option<u32>,
    /// New cycle flag.
    pub cycle: Option<bool>,
}

/// Role-management statement family.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalRoleKind {
    /// `ROLE`.
    Role,
    /// `USER`, PostgreSQL shorthand for a login role.
    User,
}

/// Partial role-attribute set from `CREATE ROLE` / `ALTER ROLE`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LogicalRoleOptions {
    /// `SUPERUSER` / `NOSUPERUSER`.
    pub superuser: Option<bool>,
    /// `INHERIT` / `NOINHERIT`.
    pub inherit: Option<bool>,
    /// `CREATEROLE` / `NOCREATEROLE`.
    pub create_role: Option<bool>,
    /// `CREATEDB` / `NOCREATEDB`.
    pub create_db: Option<bool>,
    /// `LOGIN` / `NOLOGIN`.
    pub can_login: Option<bool>,
    /// `REPLICATION` / `NOREPLICATION`.
    pub replication: Option<bool>,
    /// `BYPASSRLS` / `NOBYPASSRLS`.
    pub bypass_rls: Option<bool>,
    /// `CONNECTION LIMIT`.
    pub connection_limit: Option<i32>,
    /// Password change. `Some(None)` means `PASSWORD NULL`.
    pub password: Option<Option<String>>,
    /// Raw `VALID UNTIL` timestamp text.
    pub valid_until: Option<String>,
}

/// Object class targeted by a privilege-management statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalPrivilegeObjectKind {
    /// Table or view privileges.
    Table,
    /// Schema privileges.
    Schema,
    /// Database privileges.
    Database,
    /// Sequence privileges.
    Sequence,
    /// Function or routine privileges.
    Function,
}

/// One concrete object privilege after `ALL PRIVILEGES` expansion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalPrivilegeKind {
    /// `SELECT`.
    Select,
    /// `INSERT`.
    Insert,
    /// `UPDATE`.
    Update,
    /// `DELETE`.
    Delete,
    /// `TRUNCATE`.
    Truncate,
    /// `REFERENCES`.
    References,
    /// `TRIGGER`.
    Trigger,
    /// `USAGE`.
    Usage,
    /// `CREATE`.
    Create,
    /// `CONNECT`.
    Connect,
    /// `TEMPORARY`.
    Temporary,
    /// `EXECUTE`.
    Execute,
}

/// One privilege item after binding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalPrivilegeSpec {
    /// Concrete object privilege.
    pub kind: LogicalPrivilegeKind,
    /// Folded column names, empty for object-level privileges.
    pub columns: Vec<String>,
}

/// Operation carried by `ALTER DEFAULT PRIVILEGES`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalDefaultPrivilegeOperation {
    /// Add default ACL entries for future objects.
    Grant,
    /// Remove default ACL entries for future objects.
    Revoke,
}

impl LogicalDefaultPrivilegeOperation {
    /// Return whether this operation grants default privileges.
    #[must_use]
    pub fn is_grant(self) -> bool {
        matches!(self, Self::Grant)
    }
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CopySource {
    /// `STDIN` — client streams `CopyData` frames in.
    Stdin,
    /// `STDOUT` — server streams `CopyData` frames out.
    Stdout,
    /// Server-side file path.
    File(String),
}

/// COPY wire format, mirrored from
/// [`ultrasql_parser::ast::CopyFormat`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopyFormat {
    /// PostgreSQL `TEXT` format — tab-separated, escape-encoded.
    Text,
    /// PostgreSQL `CSV` format.
    Csv,
    /// PostgreSQL binary COPY format.
    Binary,
    /// Apache Parquet file format for server-side file COPY.
    Parquet,
}

/// One action clause of an [`LogicalPlan::AlterTable`].
#[derive(Clone, Debug, PartialEq)]
pub enum LogicalAlterTableAction {
    /// `ALTER TABLE t ADD [COLUMN] c TYPE [DEFAULT expr] [NULL | NOT NULL]`.
    ///
    /// The new column is appended to the end of the table's schema.
    /// `column` carries the resolved [`Field`] (name, type,
    /// nullability) so the executor can grow the schema without
    /// re-parsing the type name.
    AddColumn {
        /// The resolved column being added.
        column: Field,
        /// Bound default expression for the new column, if any.
        default: Option<ScalarExpr>,
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
    /// `ALTER TABLE t ENABLE ROW LEVEL SECURITY`.
    EnableRowLevelSecurity,
    /// `ALTER TABLE t SET (...)`.
    SetOptions {
        /// Relation storage options.
        options: Vec<LogicalTableOption>,
    },
    /// `ALTER TABLE t ADD CONSTRAINT name PRIMARY KEY/UNIQUE (...)`.
    AddUniqueConstraint {
        /// Bound unique or primary-key constraint.
        constraint: LogicalUniqueConstraint,
    },
}

/// One action clause of an [`LogicalPlan::AlterView`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogicalAlterViewAction {
    /// `ALTER VIEW v RENAME TO new_name`.
    RenameView {
        /// New bare view name.
        new_name: String,
    },
    /// `ALTER VIEW v SET SCHEMA schema_name`.
    SetSchema {
        /// Target schema name.
        new_schema: String,
    },
}

/// Tenant row-security predicate supported by the v1 RLS path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalTenantPolicyExpr {
    /// Target table column index.
    pub column_index: usize,
    /// Target table column name.
    pub column_name: String,
    /// Session setting read through `current_setting(setting, true)`.
    pub setting_name: String,
}

/// Bound `CREATE POLICY` metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalRlsPolicy {
    /// Policy name.
    pub policy_name: String,
    /// Target table.
    pub table_name: String,
    /// Permissive/restrictive combination mode.
    pub permissiveness: LogicalRlsPermissiveness,
    /// Command class this policy applies to.
    pub command: LogicalRlsCommand,
    /// Role names this policy applies to. Empty means all roles.
    pub roles: Vec<String>,
    /// Read visibility predicate.
    pub using: Option<LogicalTenantPolicyExpr>,
    /// Write acceptance predicate.
    pub with_check: Option<LogicalTenantPolicyExpr>,
}

/// Logical row-security policy combination mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalRlsPermissiveness {
    /// PostgreSQL `AS PERMISSIVE`.
    Permissive,
    /// PostgreSQL `AS RESTRICTIVE`.
    Restrictive,
}

/// Logical row-security policy command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalRlsCommand {
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

