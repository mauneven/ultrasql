//! `COPY`, DDL, and access-control AST nodes.
//!
//! Covers `COPY`, the `CREATE`/`ALTER`/`DROP` family for tables, views,
//! types, domains, operators, and policies, plus role management and
//! `GRANT`/`REVOKE` privilege statements.

use crate::ast::{ColumnDef, Expr, Identifier, ObjectName, SelectStmt, TableConstraint, TypeName};
use crate::span::Span;

// ============================================================================
// COPY statement
// ============================================================================

/// Direction of a `COPY` statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopyDirection {
    /// `COPY t FROM …` — client sends rows to the server.
    From,
    /// `COPY t TO …` — server sends rows to the client.
    To,
}

/// Source / sink for a `COPY` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CopySource {
    /// `STDIN` — the client streams `CopyData` frames to the server.
    Stdin,
    /// `STDOUT` — the server streams `CopyData` frames to the client.
    Stdout,
    /// Server-side file path.
    File(String),
}

/// Wire-format kind for a `COPY` statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopyFormat {
    /// PostgreSQL text format — tab-separated columns, `\N` for SQL NULL.
    Text,
    /// PostgreSQL CSV format — comma-separated, quoted strings.
    Csv,
    /// PostgreSQL binary COPY format.
    Binary,
    /// Apache Parquet file format for server-side file COPY.
    Parquet,
}

/// One `WITH (…)` option of a `COPY` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CopyOption {
    /// `FORMAT { TEXT | CSV | BINARY | PARQUET }`.
    Format(CopyFormat),
    /// `DELIMITER 'c'` — single-character column delimiter.
    Delimiter(char),
    /// `HEADER [boolean]` — whether the first row is a header.
    Header(bool),
    /// `AUTO_DETECT [boolean]` — infer CSV dialect/header metadata.
    AutoDetect(bool),
    /// `IGNORE_ERRORS [boolean]` — quarantine bad input rows instead of aborting.
    IgnoreErrors(bool),
    /// `MAX_ERRORS integer` — maximum bad rows tolerated during COPY FROM.
    MaxErrors(u64),
    /// `REJECT_TABLE 'name'` — table receiving quarantined rows.
    RejectTable(String),
    /// `NULL 'string'` — string used to represent SQL NULL.
    Null(String),
}

/// `COPY table [(col_list)] ...` or `COPY (SELECT ...) TO ...`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyStmt {
    /// Target relation. `None` when copying a query result.
    pub table: Option<ObjectName>,
    /// Query target for `COPY (SELECT ...) TO ...`.
    pub query: Option<Box<SelectStmt>>,
    /// Optional column list `(col1, col2, …)`. Empty means "every column
    /// in natural order".
    pub columns: Vec<Identifier>,
    /// `FROM` or `TO`.
    pub direction: CopyDirection,
    /// `STDIN` or `STDOUT`.
    pub source: CopySource,
    /// Negotiated wire format. Defaults to [`CopyFormat::Text`] when no
    /// `WITH (FORMAT …)` clause is present.
    pub format: CopyFormat,
    /// Raw `WITH (…)` options preserved for downstream consumers.
    pub options: Vec<CopyOption>,
    /// Source span of the entire statement.
    pub span: Span,
}

// ============================================================================
// DDL AST nodes
// ============================================================================

/// `CREATE TABLE t (col type [constraints], …)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTableStmt {
    /// Whether `IF NOT EXISTS` was specified.
    pub if_not_exists: bool,
    /// Table name (possibly schema-qualified).
    pub name: ObjectName,
    /// Column definitions.
    pub columns: Vec<ColumnDef>,
    /// Table-level constraints.
    pub table_constraints: Vec<TableConstraint>,
    /// Optional native table partitioning clause.
    pub partition_by: Option<TablePartitionSpec>,
    /// Source span of the entire statement.
    pub span: Span,
}

/// `PARTITION BY RANGE (col)` clause on `CREATE TABLE`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TablePartitionSpec {
    /// Partitioning strategy.
    pub kind: TablePartitionKind,
    /// Column used as the partition key.
    pub column: Identifier,
    /// Source span.
    pub span: Span,
}

/// Supported native table partitioning strategies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TablePartitionKind {
    /// Range partitioning.
    Range,
}

/// `CREATE TABLE t AS SELECT …`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTableAsStmt {
    /// Whether `IF NOT EXISTS` was specified.
    pub if_not_exists: bool,
    /// Table name (possibly schema-qualified).
    pub name: ObjectName,
    /// Optional explicit column-name list `(col1, col2, …)`.
    pub columns: Vec<Identifier>,
    /// The SELECT statement that provides rows.
    pub source: Box<SelectStmt>,
    /// Source span of the entire statement.
    pub span: Span,
}

/// `CREATE MATERIALIZED VIEW name [(cols...)] AS SELECT …`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateMaterializedViewStmt {
    /// Whether `IF NOT EXISTS` was specified.
    pub if_not_exists: bool,
    /// Materialized view name (possibly schema-qualified).
    pub name: ObjectName,
    /// Optional explicit column-name list `(col1, col2, …)`.
    pub columns: Vec<Identifier>,
    /// The SELECT statement that provides rows.
    pub source: Box<SelectStmt>,
    /// Source span of the entire statement.
    pub span: Span,
}

/// `CREATE [OR REPLACE] VIEW name [(cols...)] AS SELECT …`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateViewStmt {
    /// Whether `OR REPLACE` was specified.
    pub or_replace: bool,
    /// View name (possibly schema-qualified).
    pub name: ObjectName,
    /// Optional explicit column-name list `(col1, col2, …)`.
    pub columns: Vec<Identifier>,
    /// The SELECT statement that provides rows.
    pub source: Box<SelectStmt>,
    /// Source SQL text for the SELECT definition, trimmed.
    pub source_sql: String,
    /// Source span of the entire statement.
    pub span: Span,
}

/// `DESCRIBE [TABLE|VIEW] object` or `DESCRIBE SELECT ...`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DescribeStmt {
    /// Object or query expression whose output metadata is requested.
    pub target: DescribeTarget,
    /// Source span of the entire statement.
    pub span: Span,
}

/// Target form for a `DESCRIBE` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DescribeTarget {
    /// Catalog object lookup, optionally constrained to table or view.
    Object {
        /// Requested object kind.
        kind: DescribeObjectKind,
        /// Object name, possibly schema-qualified.
        name: ObjectName,
    },
    /// Query expression whose projected schema should be described.
    Query(Box<SelectStmt>),
}

/// Catalog object kind requested by `DESCRIBE`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DescribeObjectKind {
    /// No explicit object-kind qualifier.
    Any,
    /// `DESCRIBE TABLE name`.
    Table,
    /// `DESCRIBE VIEW name`.
    View,
}

/// `SUMMARIZE table_name`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SummarizeStmt {
    /// Table whose columns should be summarized.
    pub name: ObjectName,
    /// Source span of the entire statement.
    pub span: Span,
}

/// `EXPORT DATABASE TO 'path'`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExportDatabaseStmt {
    /// Destination directory path literal.
    pub path: String,
    /// Source span of the entire statement.
    pub span: Span,
}

/// `IMPORT DATABASE FROM 'path'`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImportDatabaseStmt {
    /// Source directory path literal.
    pub path: String,
    /// Source span of the entire statement.
    pub span: Span,
}

/// `CREATE TYPE name AS ...`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTypeStmt {
    /// Type name (possibly schema-qualified).
    pub name: ObjectName,
    /// Type body.
    pub kind: CreateTypeKind,
    /// Source span of the entire statement.
    pub span: Span,
}

/// Body of a `CREATE TYPE` declaration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CreateTypeKind {
    /// `CREATE TYPE name AS ENUM ('label', ...)`.
    Enum {
        /// Enum labels in declaration order.
        labels: Vec<String>,
    },
    /// `CREATE TYPE name AS (field type, ...)`.
    Composite {
        /// Composite attributes in declaration order.
        attributes: Vec<CompositeTypeAttribute>,
    },
}

/// One attribute in `CREATE TYPE name AS (...)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompositeTypeAttribute {
    /// Attribute name.
    pub name: Identifier,
    /// Attribute type.
    pub data_type: TypeName,
    /// Source span.
    pub span: Span,
}

/// `CREATE DOMAIN name AS base_type [constraints...]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateDomainStmt {
    /// Domain name (possibly schema-qualified).
    pub name: ObjectName,
    /// Underlying base type.
    pub data_type: TypeName,
    /// Domain constraints in declaration order.
    pub constraints: Vec<DomainConstraint>,
    /// Source span of the entire statement.
    pub span: Span,
}

/// `CREATE OPERATOR name (...)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateOperatorStmt {
    /// Operator token sequence, such as `===`.
    pub name: String,
    /// Optional left operand type. Omitted only for prefix operators.
    pub left_arg: Option<TypeName>,
    /// Optional right operand type. Omitted only for postfix operators.
    pub right_arg: Option<TypeName>,
    /// Function/procedure implementing the operator.
    pub procedure: ObjectName,
    /// Source span of the entire statement.
    pub span: Span,
}

/// Constraint clause on a `CREATE DOMAIN` declaration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DomainConstraint {
    /// `NOT NULL`.
    NotNull {
        /// Optional `CONSTRAINT name` prefix.
        name: Option<Identifier>,
        /// Source span.
        span: Span,
    },
    /// `NULL`.
    Null {
        /// Optional `CONSTRAINT name` prefix.
        name: Option<Identifier>,
        /// Source span.
        span: Span,
    },
    /// `CHECK (expr)`.
    Check {
        /// Optional `CONSTRAINT name` prefix.
        name: Option<Identifier>,
        /// Check expression; `VALUE` binds to the domain input value.
        expr: Expr,
        /// Source span.
        span: Span,
    },
}

/// `CREATE POLICY` permissiveness mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyPermissiveness {
    /// `AS PERMISSIVE`; matching rows from any permissive policy are allowed
    /// before restrictive policies narrow them.
    Permissive,
    /// `AS RESTRICTIVE`; matching rows must also satisfy permissive policy.
    Restrictive,
}

/// Command a row-level security policy applies to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyCommand {
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

/// `CREATE POLICY name ON table [AS mode] [FOR command] [TO roles] [USING ...] [WITH CHECK ...]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreatePolicyStmt {
    /// Policy name.
    pub name: Identifier,
    /// Target table.
    pub table: ObjectName,
    /// Permissive/restrictive combination mode.
    pub permissiveness: PolicyPermissiveness,
    /// Command class this policy applies to.
    pub command: PolicyCommand,
    /// Role names this policy applies to. Empty means all roles.
    pub roles: Vec<Identifier>,
    /// Read visibility predicate.
    pub using: Option<Expr>,
    /// Write acceptance predicate.
    pub with_check: Option<Expr>,
    /// Source span.
    pub span: Span,
}

/// Role-management statement family.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoleStmtKind {
    /// `ROLE`.
    Role,
    /// `USER`, a PostgreSQL alias for a login-capable role.
    User,
}

/// One role attribute supplied to `CREATE ROLE` / `ALTER ROLE`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RoleOption {
    /// `SUPERUSER` / `NOSUPERUSER`.
    Superuser(bool),
    /// `INHERIT` / `NOINHERIT`.
    Inherit(bool),
    /// `CREATEROLE` / `NOCREATEROLE`.
    CreateRole(bool),
    /// `CREATEDB` / `NOCREATEDB`.
    CreateDb(bool),
    /// `LOGIN` / `NOLOGIN`.
    Login(bool),
    /// `REPLICATION` / `NOREPLICATION`.
    Replication(bool),
    /// `BYPASSRLS` / `NOBYPASSRLS`.
    BypassRls(bool),
    /// `CONNECTION LIMIT n`.
    ConnectionLimit(i32),
    /// `PASSWORD 'secret'` or `PASSWORD NULL`.
    Password(Option<String>),
    /// `VALID UNTIL 'timestamp'`.
    ValidUntil(String),
}

/// `CREATE ROLE [IF NOT EXISTS] name [WITH] [options...]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateRoleStmt {
    /// Whether the statement used `ROLE` or `USER`.
    pub kind: RoleStmtKind,
    /// Whether `IF NOT EXISTS` was specified.
    pub if_not_exists: bool,
    /// Role name.
    pub name: Identifier,
    /// Role attributes.
    pub options: Vec<RoleOption>,
    /// Source span.
    pub span: Span,
}

/// `ALTER ROLE name [WITH] [options...]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterRoleStmt {
    /// Whether the statement used `ROLE` or `USER`.
    pub kind: RoleStmtKind,
    /// Role name.
    pub name: Identifier,
    /// Role attributes to change.
    pub options: Vec<RoleOption>,
    /// Source span.
    pub span: Span,
}

/// `DROP ROLE [IF EXISTS] name [, ...]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropRoleStmt {
    /// Whether the statement used `ROLE` or `USER`.
    pub kind: RoleStmtKind,
    /// Whether `IF EXISTS` was specified.
    pub if_exists: bool,
    /// Role names.
    pub names: Vec<Identifier>,
    /// Whether `CASCADE` was specified.
    pub cascade: bool,
    /// Source span.
    pub span: Span,
}

/// Object class targeted by `GRANT` / `REVOKE`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrivilegeObjectKind {
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

/// One privilege keyword in `GRANT` / `REVOKE`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrivilegeKind {
    /// `ALL [PRIVILEGES]`.
    All,
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
    /// `TEMPORARY` / `TEMP`.
    Temporary,
    /// `EXECUTE`.
    Execute,
}

/// One privilege item with optional column list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrivilegeSpec {
    /// Privilege keyword.
    pub kind: PrivilegeKind,
    /// Column list from `SELECT(col, ...)`, empty for object-level grants.
    pub columns: Vec<Identifier>,
}

/// `GRANT privileges ON kind objects TO grantees`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrantStmt {
    /// Privileges requested by the statement.
    pub privileges: Vec<PrivilegeSpec>,
    /// Object class named after `ON`.
    pub object_kind: PrivilegeObjectKind,
    /// Target object names.
    pub objects: Vec<ObjectName>,
    /// Recipient roles, including `PUBLIC` represented as `public`.
    pub grantees: Vec<Identifier>,
    /// Whether `WITH GRANT OPTION` was specified.
    pub grant_option: bool,
    /// Source span.
    pub span: Span,
}

/// `REVOKE privileges ON kind objects FROM grantees`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RevokeStmt {
    /// Whether the statement used `GRANT OPTION FOR`.
    pub grant_option_for: bool,
    /// Privileges requested by the statement.
    pub privileges: Vec<PrivilegeSpec>,
    /// Object class named after `ON`.
    pub object_kind: PrivilegeObjectKind,
    /// Target object names.
    pub objects: Vec<ObjectName>,
    /// Roles losing the privileges, including `PUBLIC` represented as `public`.
    pub grantees: Vec<Identifier>,
    /// Whether `CASCADE` was specified. `false` means `RESTRICT`.
    pub cascade: bool,
    /// Source span.
    pub span: Span,
}

/// Grant or revoke action inside `ALTER DEFAULT PRIVILEGES`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DefaultPrivilegeAction {
    /// `GRANT privileges ON kind TO grantees [WITH GRANT OPTION]`.
    Grant {
        /// Default privileges to add.
        privileges: Vec<PrivilegeSpec>,
        /// Future object class named after `ON`.
        object_kind: PrivilegeObjectKind,
        /// Recipient roles.
        grantees: Vec<Identifier>,
        /// Whether future grants include grant option.
        grant_option: bool,
    },
    /// `REVOKE [GRANT OPTION FOR] privileges ON kind FROM grantees`.
    Revoke {
        /// Whether the statement used `GRANT OPTION FOR`.
        grant_option_for: bool,
        /// Default privileges to remove.
        privileges: Vec<PrivilegeSpec>,
        /// Future object class named after `ON`.
        object_kind: PrivilegeObjectKind,
        /// Roles losing the default privileges.
        grantees: Vec<Identifier>,
        /// Whether `CASCADE` was specified. `false` means `RESTRICT`.
        cascade: bool,
    },
}

/// `ALTER DEFAULT PRIVILEGES [FOR ROLE ...] [IN SCHEMA ...] GRANT/REVOKE ...`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterDefaultPrivilegesStmt {
    /// Roles whose future objects receive the default ACL. Empty means
    /// current role at execution time.
    pub target_roles: Vec<Identifier>,
    /// Schemas restricting the future objects. Empty means all schemas.
    pub schemas: Vec<Identifier>,
    /// Grant or revoke action.
    pub action: DefaultPrivilegeAction,
    /// Source span.
    pub span: Span,
}

/// `GRANT role [, ...] TO role [, ...]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrantRoleStmt {
    /// Granted role names.
    pub roles: Vec<Identifier>,
    /// Recipient role names.
    pub grantees: Vec<Identifier>,
    /// Whether `WITH ADMIN OPTION` was specified.
    pub admin_option: bool,
    /// Source span.
    pub span: Span,
}

/// `REVOKE role [, ...] FROM role [, ...]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RevokeRoleStmt {
    /// Whether `ADMIN OPTION FOR` was specified.
    pub admin_option_for: bool,
    /// Revoked role names.
    pub roles: Vec<Identifier>,
    /// Recipient role names.
    pub grantees: Vec<Identifier>,
    /// Whether `CASCADE` was specified. `false` means `RESTRICT`.
    pub cascade: bool,
    /// Source span.
    pub span: Span,
}
