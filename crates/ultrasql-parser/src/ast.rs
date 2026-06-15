//! SQL abstract syntax tree.
//!
//! AST nodes are owned (no lifetime parameters tying them to the source
//! string). They carry [`Span`]s so error messages can quote source
//! exactly. Identifiers preserve their original quoting state so a
//! later printer can round-trip a parsed statement.

use std::fmt;

use crate::span::Span;

/// Transaction isolation level as specified in a `BEGIN` or `SET TRANSACTION`
/// statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AstIsolationLevel {
    /// `READ COMMITTED` — each statement sees the latest committed data.
    ReadCommitted,
    /// `REPEATABLE READ` — the transaction sees a snapshot fixed at `BEGIN`.
    RepeatableRead,
    /// `SERIALIZABLE` — serializable isolation requested by the client.
    Serializable,
}

/// Top-level SQL statement.
///
/// `SelectStmt` and the DML statement types are comparatively large, so
/// they live behind a `Box` to keep the enum's stack size small. Pattern
/// matching looks the same to callers.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Statement {
    /// `SELECT ...`.
    Select(Box<SelectStmt>),
    /// `INSERT INTO ...`.
    Insert(Box<InsertStmt>),
    /// `UPDATE ...`.
    Update(Box<UpdateStmt>),
    /// `DELETE FROM ...`.
    Delete(Box<DeleteStmt>),
    /// `MERGE INTO ...`.
    Merge(Box<MergeStmt>),
    /// `TRUNCATE TABLE ...`.
    Truncate(TruncateStmt),
    /// `DESCRIBE [TABLE|VIEW] object` or `DESCRIBE SELECT ...`.
    Describe(DescribeStmt),
    /// `SUMMARIZE table_name`.
    Summarize(SummarizeStmt),
    /// `CHECKPOINT`.
    Checkpoint {
        /// Source span.
        span: Span,
    },
    /// `BEGIN [TRANSACTION] [ISOLATION LEVEL …]`.
    Begin {
        /// Optional isolation level requested by the client.
        isolation_level: Option<AstIsolationLevel>,
        /// Source span.
        span: Span,
    },
    /// `COMMIT`.
    Commit {
        /// Source span.
        span: Span,
    },
    /// `ROLLBACK`.
    Rollback {
        /// Source span.
        span: Span,
    },
    /// `CREATE TABLE …`.
    CreateTable(Box<CreateTableStmt>),
    /// `CREATE TABLE … AS SELECT …`.
    CreateTableAs(Box<CreateTableAsStmt>),
    /// `CREATE MATERIALIZED VIEW … AS SELECT …`.
    CreateMaterializedView(Box<CreateMaterializedViewStmt>),
    /// `CREATE TYPE name AS ENUM (...)`.
    CreateType(Box<CreateTypeStmt>),
    /// `CREATE DOMAIN name AS base_type [constraints...]`.
    CreateDomain(Box<CreateDomainStmt>),
    /// `CREATE OPERATOR name (...)`.
    CreateOperator(Box<CreateOperatorStmt>),
    /// `CREATE POLICY name ON table [TO roles] USING (...) WITH CHECK (...)`.
    CreatePolicy(Box<CreatePolicyStmt>),
    /// `CREATE ROLE name ...` / `CREATE USER name ...`.
    CreateRole(Box<CreateRoleStmt>),
    /// `GRANT ... ON ... TO ...`.
    Grant(Box<GrantStmt>),
    /// `REVOKE ... ON ... FROM ...`.
    Revoke(Box<RevokeStmt>),
    /// `GRANT role [, ...] TO role [, ...]`.
    GrantRole(Box<GrantRoleStmt>),
    /// `REVOKE role [, ...] FROM role [, ...]`.
    RevokeRole(Box<RevokeRoleStmt>),
    /// `DROP TABLE …`.
    DropTable(DropTableStmt),
    /// `DROP ROLE name [, ...]` / `DROP USER name [, ...]`.
    DropRole(DropRoleStmt),
    /// `ALTER TABLE …`.
    AlterTable(Box<AlterTableStmt>),
    /// `ALTER ROLE name ...` / `ALTER USER name ...`.
    AlterRole(Box<AlterRoleStmt>),
    /// `ALTER DEFAULT PRIVILEGES ...`.
    AlterDefaultPrivileges(Box<AlterDefaultPrivilegesStmt>),
    /// `CREATE SCHEMA …`.
    CreateSchema(CreateSchemaStmt),
    /// `DROP SCHEMA …`.
    DropSchema(DropSchemaStmt),
    /// `SET [VARIABLE|SESSION|LOCAL] var = val` / `SHOW var` / `RESET var`.
    SetVar(SetVarStmt),
    /// `SET ROLE role` / `SET ROLE NONE` / `RESET ROLE`.
    SetRole(SetRoleStmt),
    /// `CREATE [UNIQUE] INDEX …`.
    CreateIndex(Box<CreateIndexStmt>),
    /// `DROP INDEX …`.
    DropIndex(DropIndexStmt),
    /// `REINDEX TABLE/INDEX …`.
    Reindex(ReindexStmt),
    /// `CREATE SEQUENCE …`.
    CreateSequence(Box<CreateSequenceStmt>),
    /// `ALTER SEQUENCE …`.
    AlterSequence(Box<AlterSequenceStmt>),
    /// `DROP SEQUENCE …`.
    DropSequence(DropSequenceStmt),
    /// `COMMENT ON TABLE/COLUMN ... IS ...`.
    Comment(CommentStmt),
    /// `SAVEPOINT name`.
    Savepoint(SavepointStmt),
    /// `ROLLBACK TO [SAVEPOINT] name`.
    RollbackToSavepoint(RollbackToSavepointStmt),
    /// `RELEASE [SAVEPOINT] name`.
    ReleaseSavepoint(ReleaseSavepointStmt),
    /// `EXPLAIN [ANALYZE] [VERBOSE] [(FORMAT …)] stmt`.
    Explain(Box<ExplainStmt>),
    /// `PREPARE name [(types)] AS stmt`.
    Prepare(Box<PrepareStmt>),
    /// `EXECUTE name [(args)]`.
    Execute(ExecuteStmt),
    /// `DEALLOCATE [ALL | name]`.
    Deallocate(DeallocateStmt),
    /// `PREPARE TRANSACTION 'gid'` — phase 1 of two-phase commit.
    PrepareTransaction {
        /// Global transaction identifier supplied by the coordinator.
        gid: String,
        /// Source span.
        span: Span,
    },
    /// `COMMIT PREPARED 'gid'` — phase 2 commit of a prepared txn.
    CommitPrepared {
        /// Global transaction identifier to commit.
        gid: String,
        /// Source span.
        span: Span,
    },
    /// `ROLLBACK PREPARED 'gid'` — phase 2 abort of a prepared txn.
    RollbackPrepared {
        /// Global transaction identifier to roll back.
        gid: String,
        /// Source span.
        span: Span,
    },
    /// `SET TRANSACTION ISOLATION LEVEL …` — change the *current* transaction's
    /// isolation level. Must be issued inside a transaction block before any
    /// data has been read or written; PostgreSQL rejects late changes with
    /// SQLSTATE `25001`.
    SetTransaction {
        /// Isolation level requested by the client.
        isolation_level: AstIsolationLevel,
        /// Source span.
        span: Span,
    },
    /// `LISTEN channel` — subscribe this session to async notifications on
    /// `channel`. The channel name is taken verbatim from the identifier
    /// (case-folded for unquoted names, source-case for quoted names).
    Listen {
        /// Target channel identifier.
        channel: Identifier,
        /// Source span.
        span: Span,
    },
    /// `NOTIFY channel [, 'payload']` — deliver `payload` (the empty string
    /// if omitted) to every session currently listening on `channel`.
    Notify {
        /// Target channel identifier.
        channel: Identifier,
        /// Optional payload string literal. `None` when omitted; a present
        /// `Some(String)` carries the literal's unquoted content.
        payload: Option<String>,
        /// Source span.
        span: Span,
    },
    /// `UNLISTEN { channel | * }` — drop one or all of this session's
    /// channel subscriptions. `None` means `UNLISTEN *` (drop all).
    Unlisten {
        /// Target channel identifier, or `None` for the `*` form.
        channel: Option<Identifier>,
        /// Source span.
        span: Span,
    },
    /// `COPY table [(col_list)] { FROM | TO } { STDIN | STDOUT } [WITH (…)]`.
    Copy(Box<CopyStmt>),
}

impl Statement {
    /// Source span enclosing this statement.
    #[must_use]
    pub fn span(&self) -> Span {
        match self {
            Self::Select(s) => s.span,
            Self::Insert(s) => s.span,
            Self::Update(s) => s.span,
            Self::Delete(s) => s.span,
            Self::Merge(s) => s.span,
            Self::Truncate(s) => s.span,
            Self::Describe(s) => s.span,
            Self::Summarize(s) => s.span,
            Self::Checkpoint { span } => *span,
            Self::Begin { span, .. } | Self::Commit { span } | Self::Rollback { span } => *span,
            Self::CreateTable(s) => s.span,
            Self::CreateTableAs(s) => s.span,
            Self::CreateMaterializedView(s) => s.span,
            Self::CreateType(s) => s.span,
            Self::CreateDomain(s) => s.span,
            Self::CreateOperator(s) => s.span,
            Self::CreatePolicy(s) => s.span,
            Self::CreateRole(s) => s.span,
            Self::Grant(s) => s.span,
            Self::Revoke(s) => s.span,
            Self::GrantRole(s) => s.span,
            Self::RevokeRole(s) => s.span,
            Self::DropTable(s) => s.span,
            Self::DropRole(s) => s.span,
            Self::AlterTable(s) => s.span,
            Self::AlterRole(s) => s.span,
            Self::AlterDefaultPrivileges(s) => s.span,
            Self::CreateSchema(s) => s.span,
            Self::DropSchema(s) => s.span,
            Self::SetVar(s) => s.span,
            Self::SetRole(s) => s.span,
            Self::CreateIndex(s) => s.span,
            Self::DropIndex(s) => s.span,
            Self::Reindex(s) => s.span,
            Self::CreateSequence(s) => s.span,
            Self::AlterSequence(s) => s.span,
            Self::DropSequence(s) => s.span,
            Self::Comment(s) => s.span,
            Self::Savepoint(s) => s.span,
            Self::RollbackToSavepoint(s) => s.span,
            Self::ReleaseSavepoint(s) => s.span,
            Self::Explain(s) => s.span,
            Self::Prepare(s) => s.span,
            Self::Execute(s) => s.span,
            Self::Deallocate(s) => s.span,
            Self::PrepareTransaction { span, .. }
            | Self::CommitPrepared { span, .. }
            | Self::RollbackPrepared { span, .. }
            | Self::SetTransaction { span, .. }
            | Self::Listen { span, .. }
            | Self::Notify { span, .. }
            | Self::Unlisten { span, .. } => *span,
            Self::Copy(s) => s.span,
        }
    }
}

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

/// One column definition inside `CREATE TABLE`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDef {
    /// Column name.
    pub name: Identifier,
    /// Declared SQL type.
    pub data_type: TypeName,
    /// Optional column collation from `COLLATE name`.
    pub collation: Option<ObjectName>,
    /// Column-level constraints.
    pub constraints: Vec<ColumnConstraint>,
    /// Source span.
    pub span: Span,
}

/// Column-level constraint inside a `CREATE TABLE` column definition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnConstraint {
    /// `NOT NULL`.
    NotNull {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// Source span.
        span: Span,
    },
    /// `NULL` (explicit nullable).
    Null {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// Source span.
        span: Span,
    },
    /// `DEFAULT expr`.
    Default {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// Default value expression.
        expr: Expr,
        /// Source span.
        span: Span,
    },
    /// `PRIMARY KEY`.
    PrimaryKey {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// Source span.
        span: Span,
    },
    /// `UNIQUE`.
    Unique {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// Source span.
        span: Span,
    },
    /// `CHECK (expr)`.
    Check {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// Constraint expression.
        expr: Expr,
        /// Source span.
        span: Span,
    },
    /// `REFERENCES target_table [(target_columns)]`.
    References {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// Referenced table.
        target_table: ObjectName,
        /// Referenced columns (may be empty if targeting the primary key).
        target_columns: Vec<Identifier>,
        /// Action when a referenced row is deleted.
        on_delete: ReferentialAction,
        /// Action when a referenced key is updated.
        on_update: ReferentialAction,
        /// Whether this constraint is deferrable.
        deferrable: bool,
        /// Whether this deferrable constraint starts deferred.
        initially_deferred: bool,
        /// Source span.
        span: Span,
    },
    /// `GENERATED ALWAYS | BY DEFAULT AS IDENTITY [(sequence_options)]`.
    GeneratedIdentity {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// `true` for `ALWAYS`; `false` for `BY DEFAULT`.
        always: bool,
        /// Sequence options inside the optional identity option list.
        options: Vec<SequenceOption>,
        /// Source span.
        span: Span,
    },
    /// `GENERATED ALWAYS AS (expr) STORED`.
    GeneratedStored {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// Stored generated expression.
        expr: Expr,
        /// Source span.
        span: Span,
    },
}

/// Referential action attached to a foreign key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReferentialAction {
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

/// Table-level constraint inside `CREATE TABLE` or `ALTER TABLE ADD CONSTRAINT`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableConstraint {
    /// `PRIMARY KEY (col, …)`.
    PrimaryKey {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        /// Preserved so `ALTER TABLE … DROP CONSTRAINT name` can identify the
        /// constraint by name.
        name: Option<Identifier>,
        /// Key columns.
        columns: Vec<Identifier>,
        /// Source span.
        span: Span,
    },
    /// `UNIQUE (col, …)`.
    Unique {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// Unique columns.
        columns: Vec<Identifier>,
        /// Source span.
        span: Span,
    },
    /// `FOREIGN KEY (col, …) REFERENCES target_table [(target_col, …)]`.
    ForeignKey {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// Local columns.
        columns: Vec<Identifier>,
        /// Referenced table.
        target_table: ObjectName,
        /// Referenced columns (may be empty).
        target_columns: Vec<Identifier>,
        /// Action when a referenced row is deleted.
        on_delete: ReferentialAction,
        /// Action when a referenced key is updated.
        on_update: ReferentialAction,
        /// Whether this constraint is deferrable.
        deferrable: bool,
        /// Whether this deferrable constraint starts deferred.
        initially_deferred: bool,
        /// Source span.
        span: Span,
    },
    /// `CHECK (expr)`.
    Check {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// Constraint expression.
        expr: Expr,
        /// Source span.
        span: Span,
    },
    /// `EXCLUDE USING method (col WITH op, ...)`.
    Exclude {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// Access method name, normally `gist`.
        method: Identifier,
        /// Exclusion elements.
        elements: Vec<ExclusionElement>,
        /// Source span.
        span: Span,
    },
}

/// One `column WITH operator` element inside an exclusion constraint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExclusionElement {
    /// Column participating in exclusion.
    pub column: Identifier,
    /// Operator used to compare this column against existing rows.
    pub op: BinaryOp,
    /// Source span.
    pub span: Span,
}

/// Parsed SQL type name, including optional type modifiers and array suffixes.
///
/// Mirrors the CAST-target structure but is richer: it carries numeric
/// modifiers (e.g. `VARCHAR(255)` -> `[255]`) and array dimension count.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeName {
    /// Canonical lower-case type name.
    pub name: Identifier,
    /// Type modifiers: `VARCHAR(255)` → `[255]`, `NUMERIC(10,2)` → `[10, 2]`.
    pub type_modifiers: Vec<u32>,
    /// Whether the type has at least one trailing `[]` suffix.
    pub is_array: bool,
    /// Number of trailing `[]` suffixes.
    pub array_dimensions: u32,
    /// Source span.
    pub span: Span,
}

/// `DROP TABLE [IF EXISTS] name [, …] [CASCADE|RESTRICT]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropTableStmt {
    /// Whether `IF EXISTS` was specified.
    pub if_exists: bool,
    /// Tables to drop (one or more).
    pub names: Vec<ObjectName>,
    /// Whether `CASCADE` was specified (vs. `RESTRICT` or omitted).
    pub cascade: bool,
    /// Source span.
    pub span: Span,
}

/// `ALTER TABLE name action`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterTableStmt {
    /// Table to alter.
    pub name: ObjectName,
    /// The single action to perform.
    pub action: AlterTableAction,
    /// Source span.
    pub span: Span,
}

/// One action clause of an `ALTER TABLE` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AlterTableAction {
    /// `ADD [COLUMN] col type [constraints]`.
    AddColumn {
        /// Column definition to add.
        column: ColumnDef,
        /// Source span.
        span: Span,
    },
    /// `DROP [COLUMN] col [CASCADE|RESTRICT]`.
    DropColumn {
        /// Column to drop.
        name: Identifier,
        /// Whether `CASCADE` was specified.
        cascade: bool,
        /// Source span.
        span: Span,
    },
    /// `RENAME COLUMN old TO new`.
    RenameColumn {
        /// Old column name.
        old: Identifier,
        /// New column name.
        new: Identifier,
        /// Source span.
        span: Span,
    },
    /// `RENAME TO new_name`.
    RenameTable {
        /// New table name.
        new_name: Identifier,
        /// Source span.
        span: Span,
    },
    /// `ADD CONSTRAINT name constraint`.
    AddConstraint {
        /// Constraint definition.
        constraint: TableConstraint,
        /// Source span.
        span: Span,
    },
    /// `DROP CONSTRAINT name [CASCADE|RESTRICT]`.
    DropConstraint {
        /// Constraint name.
        name: Identifier,
        /// Whether `CASCADE` was specified.
        cascade: bool,
        /// Source span.
        span: Span,
    },
    /// `ENABLE ROW LEVEL SECURITY`.
    EnableRowLevelSecurity {
        /// Source span.
        span: Span,
    },
    /// `SET (option = value, ...)`.
    SetOptions {
        /// Relation storage options.
        options: Vec<IndexOption>,
        /// Source span.
        span: Span,
    },
}

/// `CREATE SCHEMA [IF NOT EXISTS] name`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateSchemaStmt {
    /// Whether `IF NOT EXISTS` was specified.
    pub if_not_exists: bool,
    /// Schema name.
    pub name: Identifier,
    /// Source span.
    pub span: Span,
}

/// `DROP SCHEMA [IF EXISTS] name [, …] [CASCADE|RESTRICT]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropSchemaStmt {
    /// Whether `IF EXISTS` was specified.
    pub if_exists: bool,
    /// Schema names to drop.
    pub names: Vec<Identifier>,
    /// Whether `CASCADE` was specified.
    pub cascade: bool,
    /// Source span.
    pub span: Span,
}

/// `SET [VARIABLE|SESSION|LOCAL] var = val` / `SHOW var` / `RESET var`.
///
/// A single statement covering all GUC (Grand Unified Configuration)
/// manipulation forms supported by PostgreSQL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetVarStmt {
    /// Scope modifier and action type.
    pub scope: SetScope,
    /// Variable name (e.g. `search_path`, `statement_timeout`).
    pub name: Identifier,
    /// Value to assign.
    pub value: SetValue,
    /// Source span.
    pub span: Span,
}

/// Scope or action type for a `SET`/`SHOW`/`RESET` statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetScope {
    /// `SET [SESSION] var = val` — session-level setting (default).
    Session,
    /// `SET LOCAL var = val` — transaction-local setting.
    Local,
    /// `SHOW var` — display current value.
    Show,
    /// `RESET var` — restore to default.
    Reset,
}

/// Value expression(s) for a `SET` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SetValue {
    /// `DEFAULT` — restore to default.
    Default,
    /// One or more expressions: `SET search_path TO schema, public`.
    Values(Vec<Expr>),
}

/// `SET ROLE role` / `SET ROLE NONE` / `RESET ROLE`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetRoleStmt {
    /// Target role. `None` means reset to the session user.
    pub role: Option<Identifier>,
    /// Source span.
    pub span: Span,
}

/// `CREATE [UNIQUE|AGGREGATING] INDEX [IF NOT EXISTS] [name] ON table [USING method] (columns) [INCLUDE (...)] [WITH (...)] [WHERE expr]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateIndexStmt {
    /// Whether `UNIQUE` was specified.
    pub unique: bool,
    /// Whether `AGGREGATING` was specified.
    pub aggregating: bool,
    /// Whether `CONCURRENTLY` was specified.
    pub concurrently: bool,
    /// Whether `IF NOT EXISTS` was specified.
    pub if_not_exists: bool,
    /// Optional explicit index name.
    pub name: Option<Identifier>,
    /// Table to index.
    pub table: ObjectName,
    /// Index method (`btree`, `hash`, `gin`, `gist`, …).
    pub method: Option<Identifier>,
    /// Index key columns / expressions.
    pub columns: Vec<IndexColumn>,
    /// Optional partial-index predicate.
    pub r#where: Option<Expr>,
    /// `INCLUDE (col, …)` covering columns.
    pub include: Vec<Identifier>,
    /// `WITH (name = value, …)` index storage options.
    pub options: Vec<IndexOption>,
    /// Source span.
    pub span: Span,
}

/// One `WITH (…)` storage option of a `CREATE INDEX` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexOption {
    /// Option name.
    pub name: Identifier,
    /// Option value expression.
    pub value: Expr,
    /// Source span.
    pub span: Span,
}

/// One entry in the `CREATE INDEX` column list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexColumn {
    /// Key expression (commonly a bare column reference).
    pub expr: Expr,
    /// Optional operator class name (`vector_l2_ops`, `text_pattern_ops`, …).
    pub opclass: Option<Identifier>,
    /// Sort direction.
    pub direction: SortDirection,
    /// Null ordering.
    pub nulls: NullsOrder,
    /// Source span.
    pub span: Span,
}

/// `DROP INDEX [IF EXISTS] name [, …] [CASCADE|RESTRICT]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropIndexStmt {
    /// Whether `IF EXISTS` was specified.
    pub if_exists: bool,
    /// Index names to drop.
    pub names: Vec<ObjectName>,
    /// Whether `CASCADE` was specified.
    pub cascade: bool,
    /// Source span.
    pub span: Span,
}

/// `REINDEX { INDEX | TABLE } name`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReindexStmt {
    /// Whether the target is an index or a table.
    pub kind: ReindexKind,
    /// Target object name.
    pub name: ObjectName,
    /// Source span.
    pub span: Span,
}

/// Target kind for a `REINDEX` statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReindexKind {
    /// `REINDEX INDEX name`.
    Index,
    /// `REINDEX TABLE name`.
    Table,
}

/// `CREATE SEQUENCE [IF NOT EXISTS] name [options]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateSequenceStmt {
    /// Whether `IF NOT EXISTS` was specified.
    pub if_not_exists: bool,
    /// Sequence name.
    pub name: ObjectName,
    /// Sequence options.
    pub options: Vec<SequenceOption>,
    /// Source span.
    pub span: Span,
}

/// `ALTER SEQUENCE name [options]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterSequenceStmt {
    /// Sequence name.
    pub name: ObjectName,
    /// Options to change.
    pub options: Vec<SequenceOption>,
    /// Source span.
    pub span: Span,
}

/// `DROP SEQUENCE [IF EXISTS] name [, …] [CASCADE|RESTRICT]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropSequenceStmt {
    /// Whether `IF EXISTS` was specified.
    pub if_exists: bool,
    /// Sequence names to drop.
    pub names: Vec<ObjectName>,
    /// Whether `CASCADE` was specified.
    pub cascade: bool,
    /// Source span.
    pub span: Span,
}

/// `COMMENT ON ... IS ...`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommentStmt {
    /// Commented object.
    pub target: CommentTarget,
    /// Comment body. `None` represents `IS NULL`, which removes a comment.
    pub comment: Option<String>,
    /// Source span.
    pub span: Span,
}

/// Object kind accepted by `COMMENT ON`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommentTarget {
    /// `COMMENT ON TABLE rel IS ...`.
    Table(ObjectName),
    /// `COMMENT ON INDEX idx IS ...`.
    Index(ObjectName),
    /// `COMMENT ON COLUMN rel.col IS ...`.
    Column(ObjectName),
}

/// One option clause in `CREATE SEQUENCE` or `ALTER SEQUENCE`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SequenceOption {
    /// `START [WITH] n`.
    Start(i64),
    /// `RESTART [[WITH] n]`.
    Restart(Option<i64>),
    /// `INCREMENT [BY] n`.
    Increment(i64),
    /// `MINVALUE n` or `NO MINVALUE`.
    MinValue(Option<i64>),
    /// `MAXVALUE n` or `NO MAXVALUE`.
    MaxValue(Option<i64>),
    /// `CACHE n`.
    Cache(u64),
    /// `CYCLE` or `NO CYCLE`.
    Cycle(bool),
}

/// An `INSERT` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InsertStmt {
    /// Target table.
    pub table: ObjectName,
    /// Explicit column list (empty means all columns, positional).
    pub columns: Vec<Identifier>,
    /// Source of rows to insert.
    pub source: InsertSource,
    /// Optional `ON CONFLICT` clause.
    pub on_conflict: Option<OnConflict>,
    /// Optional `RETURNING` projection list (empty = no RETURNING).
    pub returning: Vec<SelectItem>,
    /// Source span of the entire statement.
    pub span: Span,
}

/// The source of rows in an `INSERT` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InsertSource {
    /// `VALUES (a, b), (c, d), ...` — one `Vec<Expr>` per row.
    Values(Vec<Vec<Expr>>),
    /// `INSERT ... SELECT ...`.
    Select(Box<SelectStmt>),
    /// `INSERT ... DEFAULT VALUES`.
    DefaultValues,
}

/// `ON CONFLICT` clause of an `INSERT` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OnConflict {
    /// `ON CONFLICT [target] DO NOTHING`.
    DoNothing {
        /// Optional conflict target (columns).
        target: Option<ConflictTarget>,
        /// Source span.
        span: Span,
    },
    /// `ON CONFLICT target DO UPDATE SET ...`.
    DoUpdate {
        /// Conflict target (columns).
        target: ConflictTarget,
        /// `SET` assignments.
        set: Vec<Assignment>,
        /// Optional `WHERE` filter on the update.
        r#where: Option<Expr>,
        /// Source span.
        span: Span,
    },
}

/// Conflict target in an `ON CONFLICT` clause.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConflictTarget {
    /// The indexed columns whose uniqueness constraint was violated.
    pub columns: Vec<Identifier>,
    /// Source span.
    pub span: Span,
}

/// A `col = expr` assignment used in `UPDATE … SET` and
/// `ON CONFLICT … DO UPDATE SET`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Assignment {
    /// Target column name.
    pub target: Identifier,
    /// New value expression.
    pub value: Expr,
    /// Source span.
    pub span: Span,
}

/// An `UPDATE` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateStmt {
    /// Target table.
    pub table: ObjectName,
    /// Optional alias for the target table.
    pub alias: Option<Identifier>,
    /// `SET` assignments (must be non-empty).
    pub set: Vec<Assignment>,
    /// Optional `FROM` clause (additional table references).
    pub from: Vec<TableRef>,
    /// Optional `WHERE` predicate.
    pub r#where: Option<Expr>,
    /// Optional `RETURNING` projection list (empty = no RETURNING).
    pub returning: Vec<SelectItem>,
    /// Source span of the entire statement.
    pub span: Span,
}

/// A `DELETE` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteStmt {
    /// Target table.
    pub table: ObjectName,
    /// Optional alias for the target table.
    pub alias: Option<Identifier>,
    /// Optional `USING` clause (additional table references).
    pub using: Vec<TableRef>,
    /// Optional `WHERE` predicate.
    pub r#where: Option<Expr>,
    /// Optional `RETURNING` projection list (empty = no RETURNING).
    pub returning: Vec<SelectItem>,
    /// Source span of the entire statement.
    pub span: Span,
}

/// A `MERGE INTO` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeStmt {
    /// Target table.
    pub target: ObjectName,
    /// Optional alias for the target table.
    pub target_alias: Option<Identifier>,
    /// Source relation from `USING`.
    pub source: TableRef,
    /// Match predicate after `ON`.
    pub on: Expr,
    /// Ordered `WHEN ... THEN ...` clauses.
    pub clauses: Vec<MergeClause>,
    /// Source span of the entire statement.
    pub span: Span,
}

/// One `WHEN ... THEN ...` clause in a `MERGE INTO` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeClause {
    /// Whether this branch handles matched or unmatched source rows.
    pub kind: MergeMatchKind,
    /// Optional branch predicate after `AND`.
    pub condition: Option<Expr>,
    /// Action to run when this branch fires.
    pub action: MergeAction,
    /// Source span of this clause.
    pub span: Span,
}

/// Match class for a `MERGE INTO` branch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeMatchKind {
    /// `WHEN MATCHED`.
    Matched,
    /// `WHEN NOT MATCHED`.
    NotMatched,
}

/// Action attached to a `MERGE INTO` branch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MergeAction {
    /// `THEN UPDATE SET ...`.
    Update {
        /// Target assignments.
        set: Vec<Assignment>,
    },
    /// `THEN DELETE`.
    Delete,
    /// `THEN INSERT [(columns)] VALUES (...)`.
    Insert {
        /// Optional target column list.
        columns: Vec<Identifier>,
        /// Values to insert.
        values: Vec<Expr>,
    },
}

/// A `TRUNCATE TABLE` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TruncateStmt {
    /// Tables to truncate (one or more).
    pub tables: Vec<ObjectName>,
    /// Whether `RESTART IDENTITY` was specified.
    pub restart_identity: bool,
    /// Whether `CASCADE` was specified.
    pub cascade: bool,
    /// Source span of the entire statement.
    pub span: Span,
}

// ============================================================================
// SELECT-level AST — extended for v0.2
// ============================================================================

/// A `SELECT` statement, including optional WITH clause (CTEs) and set
/// operations (UNION / INTERSECT / EXCEPT).
///
/// # Breaking change (v0.2)
/// `from` is now `Vec<TableRef>` (was `Option<TableRef>`). The join tree is
/// encoded as a left-deep chain of [`TableRef::Join`] nodes; comma-separated
/// tables are canonicalised to `JoinOp::Cross`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectStmt {
    /// DISTINCT / DISTINCT ON / ALL / no keyword.
    pub distinct: Distinct,
    /// Output items.
    pub projection: Vec<SelectItem>,
    /// `FROM` clause. Zero entries means no `FROM` (e.g. `SELECT 1`).
    /// Multiple entries represent a left-deep join tree where comma-joins
    /// have been canonicalised to `JoinOp::Cross`.
    pub from: Vec<TableRef>,
    /// `WHERE` predicate.
    pub r#where: Option<Expr>,
    /// `GROUP BY` expressions.
    pub group_by: Vec<Expr>,
    /// `HAVING` predicate (only meaningful when `group_by` is non-empty).
    pub having: Option<Expr>,
    /// `ORDER BY` items.
    pub order_by: Vec<OrderItem>,
    /// `LIMIT n`.
    pub limit: Option<Expr>,
    /// `OFFSET n`.
    pub offset: Option<Expr>,
    /// Chained UNION / INTERSECT / EXCEPT tails. In PostgreSQL, set ops bind
    /// less tightly than ORDER BY / LIMIT. For v0.2 we represent the chain
    /// here and leave precedence enforcement to the binder.
    pub set_ops: Vec<SetOpTail>,
    /// Leading `WITH [RECURSIVE]` CTEs.
    pub ctes: Vec<Cte>,
    /// `FOR UPDATE` / `FOR SHARE` / `FOR NO KEY UPDATE` / `FOR KEY SHARE`
    /// locking clauses. Multiple clauses are permitted by PostgreSQL when
    /// different `OF table` targets are given; we collect all of them.
    pub locking: Vec<LockingClause>,
    /// Source span.
    pub span: Span,
}

/// `FOR UPDATE` / `FOR SHARE` / `FOR NO KEY UPDATE` / `FOR KEY SHARE`.
///
/// Describes the row-level lock strength requested by a `SELECT` statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockStrength {
    /// `FOR UPDATE` — exclusive row lock; blocks concurrent writers.
    Update,
    /// `FOR NO KEY UPDATE` — like Update but does not block `KeyShare` locks.
    NoKeyUpdate,
    /// `FOR SHARE` — shared lock; blocks concurrent writes, not other reads.
    Share,
    /// `FOR KEY SHARE` — weakest; only blocks `FOR UPDATE`.
    KeyShare,
}

/// What to do when a requested lock is unavailable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum LockWaitPolicy {
    /// Block until the lock can be acquired (default).
    #[default]
    Wait,
    /// Raise an error immediately if any row is locked.
    NoWait,
    /// Silently skip rows that are currently locked.
    SkipLocked,
}

/// One `FOR …` locking clause in a `SELECT` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LockingClause {
    /// Lock strength requested.
    pub strength: LockStrength,
    /// Wait policy when a lock cannot be acquired immediately.
    pub wait_policy: LockWaitPolicy,
    /// Optional `OF table [, …]` targets; empty means all relations in `FROM`.
    pub of_tables: Vec<ObjectName>,
}

/// DISTINCT / DISTINCT ON / ALL / (implicit none).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Distinct {
    /// No DISTINCT keyword (implicit ALL rows).
    None,
    /// `ALL` keyword was explicit.
    All,
    /// `DISTINCT` with no ON clause.
    Distinct,
    /// `DISTINCT ON (expr, …)`.
    DistinctOn(Vec<Expr>),
}

/// One UNION / INTERSECT / EXCEPT tail following a SELECT body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetOpTail {
    /// The set operation.
    pub op: SetOp,
    /// ALL or DISTINCT (default is DISTINCT per SQL standard).
    pub quantifier: SetQuantifier,
    /// Right-hand SELECT of the set operation.
    pub right: Box<SelectStmt>,
    /// Source span of this tail (from the keyword through to end of right).
    pub span: Span,
}

/// Set operation kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetOp {
    /// `UNION`.
    Union,
    /// `INTERSECT`.
    Intersect,
    /// `EXCEPT`.
    Except,
}

/// Set quantifier for a set operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetQuantifier {
    /// `DISTINCT` (default per SQL standard).
    Distinct,
    /// `ALL` — keep duplicates.
    All,
}

/// One entry in the `SELECT` projection list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SelectItem {
    /// `*`.
    Wildcard {
        /// Source span.
        span: Span,
    },
    /// `t.*` (qualified wildcard).
    QualifiedWildcard {
        /// Qualifier (table or alias name).
        qualifier: Identifier,
        /// Source span.
        span: Span,
    },
    /// `expr [AS alias]`.
    Expr {
        /// The expression.
        expr: Expr,
        /// Optional output alias.
        alias: Option<Identifier>,
        /// Source span.
        span: Span,
    },
}

/// A table reference in the `FROM` clause.
///
/// The v0.2 extension adds [`TableRef::Join`] (explicit join syntax and
/// comma-separated table lists canonicalised to CROSS JOIN) and
/// [`TableRef::Subquery`] (derived tables).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableRef {
    /// A bare table name with an optional alias.
    Named {
        /// Schema-qualified name (one or two parts).
        name: ObjectName,
        /// `AS alias` (or implicit alias).
        alias: Option<Identifier>,
        /// Source span.
        span: Span,
    },
    /// An explicit or implicit join between two table references.
    ///
    /// The parser builds a left-deep tree: the first table in the FROM
    /// clause is the leftmost leaf; each subsequent join wraps the
    /// current left side.
    Join {
        /// Left-hand table or sub-tree.
        left: Box<Self>,
        /// Join type.
        op: JoinOp,
        /// Right-hand table factor.
        right: Box<Self>,
        /// Join condition.
        condition: JoinCondition,
        /// Source span.
        span: Span,
    },
    /// A derived table (subquery in FROM).
    ///
    /// PostgreSQL requires an alias on every derived table. Parsing
    /// without an alias is a [`crate::parser::ParseError`].
    Subquery {
        /// The inner SELECT.
        select: Box<SelectStmt>,
        /// Required alias for the derived table.
        alias: Identifier,
        /// Optional column-alias list: `AS t(c1, c2, …)`.
        column_aliases: Vec<Identifier>,
        /// Source span.
        span: Span,
    },
    /// A set-returning function in the `FROM` clause, e.g.
    /// `generate_series(1, 10)`.
    Function {
        /// Function name (e.g. `generate_series`, `unnest`).
        name: Identifier,
        /// Argument expressions in declaration order.
        args: Vec<Expr>,
        /// Optional alias for the produced relation.
        alias: Option<Identifier>,
        /// Source span.
        span: Span,
    },
    /// SQL/JSON `JSON_TABLE(...)` table function.
    JsonTable {
        /// Input JSON expression.
        context: Expr,
        /// Row-pattern SQL/JSON path expression.
        row_path: String,
        /// Declared output columns.
        columns: Vec<JsonTableColumn>,
        /// Optional alias for the produced relation.
        alias: Option<Identifier>,
        /// Source span.
        span: Span,
    },
    /// `table PIVOT (agg(expr) FOR col IN (...))`.
    Pivot {
        /// Input table reference.
        input: Box<Self>,
        /// Aggregate applied to each pivot value.
        aggregate: PivotAggregate,
        /// Column whose values select the pivot bucket.
        value_column: Identifier,
        /// Literal pivot values and optional output aliases.
        pivot_values: Vec<PivotValue>,
        /// Source span.
        span: Span,
    },
    /// `table UNPIVOT ([INCLUDE|EXCLUDE NULLS] value FOR name IN (...))`.
    Unpivot {
        /// Input table reference.
        input: Box<Self>,
        /// Output value column.
        value_column: Identifier,
        /// Output name column holding each unpivoted label.
        name_column: Identifier,
        /// Input columns to unpivot.
        columns: Vec<UnpivotColumn>,
        /// Whether NULL source values are retained.
        include_nulls: bool,
        /// Source span.
        span: Span,
    },
    /// SQL/XML `XMLTABLE(...)` table function.
    XmlTable {
        /// Input XML expression.
        context: Expr,
        /// Row-pattern XPath expression.
        row_path: String,
        /// Declared output columns.
        columns: Vec<XmlTableColumn>,
        /// Optional alias for the produced relation.
        alias: Option<Identifier>,
        /// Source span.
        span: Span,
    },
}

/// Aggregate call inside a `PIVOT` table-factor transform.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PivotAggregate {
    /// Aggregate function name.
    pub function: Identifier,
    /// Aggregate argument, or `None` for `COUNT(*)`.
    pub arg: Option<Expr>,
    /// Source span.
    pub span: Span,
}

/// One value inside a `PIVOT ... IN (...)` list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PivotValue {
    /// Pivot-key value expression.
    pub value: Expr,
    /// Optional output column alias.
    pub alias: Option<Identifier>,
    /// Source span.
    pub span: Span,
}

/// One input column inside an `UNPIVOT ... IN (...)` list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnpivotColumn {
    /// Source column to unpivot.
    pub column: Identifier,
    /// Optional emitted label. Defaults to the column name.
    pub label: Option<Expr>,
    /// Source span.
    pub span: Span,
}

/// One column declared inside a `JSON_TABLE ... COLUMNS (...)` clause.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonTableColumn {
    /// Output column name.
    pub name: Identifier,
    /// Column behavior.
    pub kind: JsonTableColumnKind,
    /// Source span.
    pub span: Span,
}

/// Supported `JSON_TABLE` column forms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JsonTableColumnKind {
    /// `name FOR ORDINALITY`.
    Ordinality,
    /// `name type [PATH path_expression]`.
    Value {
        /// Declared SQL output type.
        data_type: TypeName,
        /// Optional column path. Defaults to `$.name`.
        path: Option<String>,
    },
    /// `name type EXISTS [PATH path_expression]`.
    Exists {
        /// Declared SQL output type, normally boolean.
        data_type: TypeName,
        /// Optional column path. Defaults to `$.name`.
        path: Option<String>,
    },
}

/// One column declared inside an `XMLTABLE ... COLUMNS (...)` clause.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct XmlTableColumn {
    /// Output column name.
    pub name: Identifier,
    /// Column behavior.
    pub kind: XmlTableColumnKind,
    /// Source span.
    pub span: Span,
}

/// Supported `XMLTABLE` column forms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum XmlTableColumnKind {
    /// `name FOR ORDINALITY`.
    Ordinality,
    /// `name type [PATH path_expression]`.
    Value {
        /// Declared SQL output type.
        data_type: TypeName,
        /// Optional XPath expression. Defaults to the column name.
        path: Option<String>,
        /// Optional scalar literal used when the path returns no value.
        default: Option<String>,
    },
}

/// Join operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinOp {
    /// `[INNER] JOIN`.
    Inner,
    /// `LEFT [OUTER] JOIN`.
    LeftOuter,
    /// `RIGHT [OUTER] JOIN`.
    RightOuter,
    /// `FULL [OUTER] JOIN`.
    FullOuter,
    /// `CROSS JOIN` or comma-separated table factor.
    Cross,
}

/// Join condition for an explicit join.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinCondition {
    /// `ON expr`.
    On(Expr),
    /// `USING (col, …)`.
    Using(Vec<Identifier>),
    /// `NATURAL JOIN`; binder resolves common column names to `USING`.
    Natural,
    /// No condition (CROSS JOIN).
    None,
}

/// Common table expression in a `WITH` clause.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cte {
    /// CTE name.
    pub name: Identifier,
    /// Optional column-alias list: `cte_name(c1, c2, …)`.
    pub column_aliases: Vec<Identifier>,
    /// Whether `WITH RECURSIVE` was specified.
    ///
    /// The flag is replicated on every CTE in the same WITH clause.
    pub recursive: bool,
    /// The body SELECT.
    pub query: Box<SelectStmt>,
    /// Source span.
    pub span: Span,
}

/// `OVER (PARTITION BY ... ORDER BY ...)` window specification riding on
/// a function call.
///
/// v0.5 supports an empty / present `PARTITION BY` and an empty /
/// present `ORDER BY`. Frame clauses (`ROWS BETWEEN ... AND ...`,
/// `RANGE BETWEEN ... AND ...`) are recognised at the kernel but the
/// parser does not yet emit them — the default frame is "UNBOUNDED
/// PRECEDING to CURRENT ROW for ORDER BY, UNBOUNDED PRECEDING to
/// UNBOUNDED FOLLOWING otherwise", matching PostgreSQL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowSpec {
    /// `PARTITION BY` expressions.
    pub partition_by: Vec<Expr>,
    /// `ORDER BY` items.
    pub order_by: Vec<OrderItem>,
    /// Source span.
    pub span: Span,
}

/// Order-by item.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderItem {
    /// The sort expression.
    pub expr: Expr,
    /// Sort direction.
    pub direction: SortDirection,
    /// Null ordering.
    pub nulls: NullsOrder,
    /// Source span.
    pub span: Span,
}

/// `ASC` or `DESC`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortDirection {
    /// Ascending (default).
    Asc,
    /// Descending.
    Desc,
}

/// `NULLS FIRST` or `NULLS LAST`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NullsOrder {
    /// Database default (PostgreSQL: `NULLS LAST` for `ASC`,
    /// `NULLS FIRST` for `DESC`).
    Default,
    /// `NULLS FIRST`.
    First,
    /// `NULLS LAST`.
    Last,
}

/// Multi-part object name (`db.schema.table`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectName {
    /// Components in left-to-right order.
    pub parts: Vec<Identifier>,
    /// Source span.
    pub span: Span,
}

impl fmt::Display for ObjectName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, p) in self.parts.iter().enumerate() {
            if i > 0 {
                f.write_str(".")?;
            }
            write!(f, "{}", p.value)?;
        }
        Ok(())
    }
}

/// A SQL identifier — name + quote flag + span.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identifier {
    /// The identifier text. For unquoted identifiers this is the
    /// case-folded (lower-case) form; for quoted identifiers it
    /// preserves the source case.
    pub value: String,
    /// Whether the identifier was double-quoted in the source.
    pub quoted: bool,
    /// Source span.
    pub span: Span,
}

/// SQL expression.
///
/// The enum is `#[non_exhaustive]` so that downstream crates are not broken
/// when new expression kinds are added (e.g. window functions).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Expr {
    /// Literal value.
    Literal(Literal),
    /// Column reference, optionally qualified.
    Column {
        /// Name with up to three parts.
        name: ObjectName,
    },
    /// Positional parameter (`$N`).
    Parameter {
        /// 1-based parameter index.
        index: u32,
        /// Source span.
        span: Span,
    },
    /// Unary operator.
    Unary {
        /// Operator.
        op: UnaryOp,
        /// Operand.
        expr: Box<Self>,
        /// Source span.
        span: Span,
    },
    /// Binary operator.
    Binary {
        /// Operator.
        op: BinaryOp,
        /// Left-hand operand.
        left: Box<Self>,
        /// Right-hand operand.
        right: Box<Self>,
        /// Source span.
        span: Span,
    },
    /// Function call.
    Call {
        /// Function name (may be schema-qualified).
        name: ObjectName,
        /// Argument list.
        args: Vec<Self>,
        /// Whether the argument list was `DISTINCT`-prefixed.
        distinct: bool,
        /// Optional ordered-set aggregate sort specification from
        /// `WITHIN GROUP (ORDER BY ...)`.
        within_group: Option<Vec<OrderItem>>,
        /// Optional `OVER (...)` clause turning this into a window
        /// function call. `None` for ordinary scalar / aggregate calls.
        over: Option<WindowSpec>,
        /// Source span.
        span: Span,
    },
    /// `expr IS NULL` / `expr IS NOT NULL`.
    IsNull {
        /// Operand.
        expr: Box<Self>,
        /// `true` for `IS NOT NULL`.
        negated: bool,
        /// Source span.
        span: Span,
    },
    /// Parenthesized expression.
    Paren {
        /// Inner expression.
        expr: Box<Self>,
        /// Source span.
        span: Span,
    },
    /// Array literal, e.g. `['a', 'b']`.
    ArrayLiteral {
        /// Elements in source order.
        elements: Vec<Self>,
        /// Source span.
        span: Span,
    },
    /// `CAST(expr AS type)`.
    Cast {
        /// Expression to cast.
        expr: Box<Self>,
        /// Target type identifier (parsed as an identifier; the binder
        /// turns it into a [`ultrasql_core::DataType`]).
        target: Identifier,
        /// Source span.
        span: Span,
    },
    /// Scalar subquery: `(SELECT …)`.
    ///
    /// The surrounding operator (`IN`, `EXISTS`, etc.) disambiguates the role.
    Subquery {
        /// The inner SELECT.
        select: Box<SelectStmt>,
        /// Source span.
        span: Span,
    },
    /// `EXISTS (SELECT …)` / `NOT EXISTS (SELECT …)`.
    Exists {
        /// The inner SELECT.
        select: Box<SelectStmt>,
        /// `true` for `NOT EXISTS`.
        negated: bool,
        /// Source span.
        span: Span,
    },
    /// `expr [NOT] IN (val, …)`.
    InList {
        /// The test expression.
        expr: Box<Self>,
        /// The list items (arbitrary expressions per PG).
        items: Vec<Self>,
        /// `true` for `NOT IN`.
        negated: bool,
        /// Source span.
        span: Span,
    },
    /// `expr [NOT] IN (SELECT …)`.
    InSubquery {
        /// The test expression.
        expr: Box<Self>,
        /// The inner SELECT.
        select: Box<SelectStmt>,
        /// `true` for `NOT IN`.
        negated: bool,
        /// Source span.
        span: Span,
    },
    /// `left_expr <op> ANY (SELECT …)`.
    ///
    /// The parser folds `lhs <op> ANY (sub)` into this node. Only
    /// comparison operators (`=`, `<>`, `<`, `<=`, `>`, `>=`) are valid
    /// with `ANY`.
    Any {
        /// Left-hand expression.
        expr: Box<Self>,
        /// Comparison operator.
        op: BinaryOp,
        /// The inner SELECT.
        select: Box<SelectStmt>,
        /// Source span.
        span: Span,
    },
    /// `left_expr <op> ANY (array_expr)`.
    ///
    /// PostgreSQL accepts both array literals and array-producing scalar
    /// expressions here. The parser keeps non-literal array expressions in
    /// this node so the planner can bind them as scalar membership tests.
    AnyArray {
        /// Left-hand expression.
        expr: Box<Self>,
        /// Comparison operator.
        op: BinaryOp,
        /// Array-producing expression.
        array: Box<Self>,
        /// Source span.
        span: Span,
    },
    /// `left_expr <op> ALL (SELECT …)`.
    ///
    /// Like [`Expr::Any`] but with `ALL` semantics.
    All {
        /// Left-hand expression.
        expr: Box<Self>,
        /// Comparison operator.
        op: BinaryOp,
        /// The inner SELECT.
        select: Box<SelectStmt>,
        /// Source span.
        span: Span,
    },
    /// `CASE [operand] WHEN … THEN … [ELSE …] END`.
    ///
    /// When `operand` is `Some` this is a *simple* CASE that compares the
    /// operand against each WHEN value with `=`. When `operand` is `None`
    /// it is a *searched* CASE where each WHEN branch is a boolean condition.
    Case {
        /// Optional operand for a simple CASE (`CASE expr WHEN …`).
        /// `None` for a searched CASE (`CASE WHEN cond …`).
        operand: Option<Box<Self>>,
        /// `(when_expr, then_expr)` branch pairs (at least one).
        branches: Vec<(Self, Self)>,
        /// Optional ELSE expression.
        else_expr: Option<Box<Self>>,
        /// Source span.
        span: Span,
    },
    /// `COALESCE(a, b, …)` — returns the first non-NULL argument.
    Coalesce {
        /// Argument list (at least one per SQL standard).
        args: Vec<Self>,
        /// Source span.
        span: Span,
    },
    /// `NULLIF(a, b)` — returns NULL if `a = b`, otherwise `a`.
    NullIf {
        /// First argument.
        a: Box<Self>,
        /// Second argument.
        b: Box<Self>,
        /// Source span.
        span: Span,
    },
    /// `GREATEST(a, b, …)` — returns the largest non-NULL argument.
    Greatest {
        /// Argument list (at least one).
        args: Vec<Self>,
        /// Source span.
        span: Span,
    },
    /// `LEAST(a, b, …)` — returns the smallest non-NULL argument.
    Least {
        /// Argument list (at least one).
        args: Vec<Self>,
        /// Source span.
        span: Span,
    },
    /// `expr [NOT] BETWEEN [SYMMETRIC] low AND high`.
    Between {
        /// The expression being tested.
        expr: Box<Self>,
        /// Lower bound.
        low: Box<Self>,
        /// Upper bound.
        high: Box<Self>,
        /// `true` for `NOT BETWEEN`.
        negated: bool,
        /// `true` for `BETWEEN SYMMETRIC` (ordering of bounds is irrelevant).
        symmetric: bool,
        /// Source span.
        span: Span,
    },
    /// `expr IS [NOT] DISTINCT FROM other`.
    IsDistinctFrom {
        /// Left-hand expression.
        left: Box<Self>,
        /// Right-hand expression.
        right: Box<Self>,
        /// `true` for `IS NOT DISTINCT FROM`.
        negated: bool,
        /// Source span.
        span: Span,
    },
    /// `expr IS [NOT] TRUE / FALSE / UNKNOWN`.
    IsBoolean {
        /// The expression being tested.
        expr: Box<Self>,
        /// The boolean literal being compared against (`true` = TRUE,
        /// `false` = FALSE). Unused when `is_unknown` is `true`.
        value: bool,
        /// `true` when the test is against `UNKNOWN` rather than a boolean
        /// literal.
        is_unknown: bool,
        /// `true` for `IS NOT …`.
        negated: bool,
        /// Source span.
        span: Span,
    },
    /// `expr::type_name` — PostgreSQL postfix cast syntax.
    PostfixCast {
        /// Expression being cast.
        expr: Box<Self>,
        /// Target type name.
        target: Identifier,
        /// Source span.
        span: Span,
    },
    /// `expr[index]` — single-element array subscript (1-based).
    ArraySubscript {
        /// Array expression.
        expr: Box<Self>,
        /// Index expression.
        index: Box<Self>,
        /// Source span.
        span: Span,
    },
    /// `expr[lower:upper]` — array slice.
    ///
    /// Either bound may be `None` (`arr[:3]`, `arr[2:]`, `arr[:]`).
    ArraySlice {
        /// Array expression.
        expr: Box<Self>,
        /// Optional lower bound (absent in `arr[:n]`).
        lower: Option<Box<Self>>,
        /// Optional upper bound (absent in `arr[n:]`).
        upper: Option<Box<Self>>,
        /// Source span.
        span: Span,
    },
    /// `expr AT TIME ZONE zone` — convert to/from a time zone.
    AtTimeZone {
        /// The timestamp or time expression.
        expr: Box<Self>,
        /// Time zone specifier (string literal or identifier).
        zone: Box<Self>,
        /// Source span.
        span: Span,
    },
    /// `expr COLLATE collation` — attach a collation to text comparison.
    Collate {
        /// Expression being collated.
        expr: Box<Self>,
        /// Collation name, optionally schema-qualified.
        collation: ObjectName,
        /// Source span.
        span: Span,
    },
    /// `(a, b) OVERLAPS (c, d)` — period overlap test.
    Overlaps {
        /// Start of the left period.
        left_start: Box<Self>,
        /// End of the left period.
        left_end: Box<Self>,
        /// Start of the right period.
        right_start: Box<Self>,
        /// End of the right period.
        right_end: Box<Self>,
        /// Source span.
        span: Span,
    },
    /// `ROW(a, b, …)` — explicit row constructor.
    Row {
        /// Field expressions.
        fields: Vec<Self>,
        /// Source span.
        span: Span,
    },
}

impl Expr {
    /// Source span.
    #[must_use]
    pub const fn span(&self) -> Span {
        match self {
            Self::Literal(lit) => lit.span(),
            Self::Column { name } => name.span,
            Self::Parameter { span, .. }
            | Self::Unary { span, .. }
            | Self::Binary { span, .. }
            | Self::Call { span, .. }
            | Self::IsNull { span, .. }
            | Self::Paren { span, .. }
            | Self::ArrayLiteral { span, .. }
            | Self::Cast { span, .. }
            | Self::Subquery { span, .. }
            | Self::Exists { span, .. }
            | Self::InList { span, .. }
            | Self::InSubquery { span, .. }
            | Self::Any { span, .. }
            | Self::AnyArray { span, .. }
            | Self::All { span, .. }
            | Self::Case { span, .. }
            | Self::Coalesce { span, .. }
            | Self::NullIf { span, .. }
            | Self::Greatest { span, .. }
            | Self::Least { span, .. }
            | Self::Between { span, .. }
            | Self::IsDistinctFrom { span, .. }
            | Self::IsBoolean { span, .. }
            | Self::PostfixCast { span, .. }
            | Self::ArraySubscript { span, .. }
            | Self::ArraySlice { span, .. }
            | Self::AtTimeZone { span, .. }
            | Self::Collate { span, .. }
            | Self::Overlaps { span, .. }
            | Self::Row { span, .. } => *span,
        }
    }
}

/// Literal value.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Literal {
    /// `NULL`.
    Null {
        /// Source span.
        span: Span,
    },
    /// `TRUE` / `FALSE`.
    Bool {
        /// Value.
        value: bool,
        /// Source span.
        span: Span,
    },
    /// Integer literal (kept as a string until the binder picks a
    /// width).
    Integer {
        /// Original text.
        text: String,
        /// Source span.
        span: Span,
    },
    /// Floating-point literal (kept as a string until the binder picks
    /// a width and rounds).
    Float {
        /// Original text.
        text: String,
        /// Source span.
        span: Span,
    },
    /// String literal. The body excludes the surrounding quotes and
    /// has any `''` collapsed.
    String {
        /// Decoded body.
        value: String,
        /// Source span.
        span: Span,
    },
    /// Typed string constant. SQL syntax: `TYPENAME 'literal-body'`.
    /// Covers `DATE 'YYYY-MM-DD'`, `TIMESTAMP '…'`, `TIME '…'`, and
    /// `INTERVAL '…' [unit]`. The body is the decoded string content;
    /// the binder is responsible for parsing it into the appropriate
    /// `Value` variant.
    Typed {
        /// Lowercase type-name spelling (`"date"`, `"interval"`,
        /// `"vector"`, `"vector(3)"`, ...).
        type_name: String,
        /// Decoded string body (no surrounding quotes).
        value: String,
        /// Optional trailing interval unit (`"year"`, `"month"`, …)
        /// for `INTERVAL '…' unit`. Empty for non-interval typed
        /// literals.
        unit: Option<String>,
        /// Source span covering the whole `TYPENAME '…'` construct.
        span: Span,
    },
}

impl Literal {
    /// Source span.
    #[must_use]
    pub const fn span(&self) -> Span {
        match self {
            Self::Null { span }
            | Self::Bool { span, .. }
            | Self::Integer { span, .. }
            | Self::Float { span, .. }
            | Self::String { span, .. }
            | Self::Typed { span, .. } => *span,
        }
    }
}

/// Unary operators.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    /// `-x`.
    Neg,
    /// `+x` (rare but accepted).
    Pos,
    /// `NOT x`.
    Not,
    /// `~x` — bitwise NOT (prefix position).
    ///
    /// The `~` character is also used as the binary POSIX-regex-match operator
    /// (`BinaryOp::RegexMatch`). The parser disambiguates by position: in the
    /// prefix (expression-start) slot `~` is `BitNot`; in the infix slot it is
    /// `RegexMatch`.
    BitNot,
}

/// Binary operators recognized by the parser.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryOp {
    /// `+`
    Add,
    /// `-`
    Sub,
    /// `*`
    Mul,
    /// `/`
    Div,
    /// `%`
    Mod,
    /// `^` (exponentiation)
    Pow,
    /// `||` (string concat).
    Concat,
    /// `=`
    Eq,
    /// `<>`/`!=`
    NotEq,
    /// `<`
    Lt,
    /// `<=`
    LtEq,
    /// `>`
    Gt,
    /// `>=`
    GtEq,
    /// `<->` — vector L2 distance.
    VectorL2Distance,
    /// `<#>` — vector negative inner product.
    VectorNegativeInnerProduct,
    /// `<=>` — vector cosine distance.
    VectorCosineDistance,
    /// `<+>` — vector L1 distance.
    VectorL1Distance,
    /// `AND`
    And,
    /// `OR`
    Or,
    /// `LIKE`
    Like,
    /// `NOT LIKE`
    NotLike,
    /// `ILIKE`
    Ilike,
    /// `NOT ILIKE`
    NotIlike,
    /// `~` — POSIX regex match (case-sensitive).
    RegexMatch,
    /// `~*` — POSIX regex match (case-insensitive).
    RegexIMatch,
    /// `!~` — POSIX regex non-match (case-sensitive).
    RegexNotMatch,
    /// `!~*` — POSIX regex non-match (case-insensitive).
    RegexNotIMatch,
    /// `&` — bitwise AND.
    BitAnd,
    /// `|` — bitwise OR.
    BitOr,
    /// `#` — bitwise XOR (PostgreSQL syntax).
    BitXor,
    /// `<<` — bitwise shift left.
    ShiftLeft,
    /// `>>` — bitwise shift right.
    ShiftRight,
    /// `<<=` — network contained within or equal.
    NetworkContainedEq,
    /// `>>=` — network contains or equal.
    NetworkContainsEq,
    /// `->` — JSON/JSONB element access by key, returns JSONB.
    JsonGet,
    /// `->>` — JSON/JSONB element access by key, returns text.
    JsonGetText,
    /// `#>` — JSON/JSONB path access, returns JSONB.
    JsonGetPath,
    /// `#>>` — JSON/JSONB path access, returns text.
    JsonGetPathText,
    /// `@>` — JSONB/array contains.
    JsonContains,
    /// `<@` — JSONB/array contained by.
    JsonContained,
    /// `?` — JSONB has key.
    JsonHasKey,
    /// `?|` — JSONB has any of the given keys.
    JsonHasAnyKey,
    /// `?&` — JSONB has all of the given keys.
    JsonHasAllKeys,
    /// `@@` — TSVECTOR matches TSQUERY.
    TextSearchMatch,
    /// `&&` — range/geometric overlap.
    Overlap,
}

impl BinaryOp {
    /// Operator precedence level. Higher values bind more tightly.
    ///
    /// The table mirrors PostgreSQL's operator precedence from lowest to highest:
    ///
    /// ```text
    /// Level 1 — OR
    /// Level 2 — AND
    /// Level 3 — comparison band: < > = <= >= <>, LIKE, ILIKE, regex ops (~, ~*, !~, !~*)
    /// Level 4 — JSON ops (-> ->> #> #>> @> <@ ? ?| ?&), concat ||, bitwise & | #,
    ///           vector distance ops (<-> <#> <=> <+>)
    /// Level 5 — bitwise shift << >>
    /// Level 6 — addition/subtraction + -
    /// Level 7 — multiplication/division/modulo * / %
    /// Level 8 — exponentiation ^ (right-associative)
    /// ```
    ///
    /// JSON operators and bitwise operators sit between the comparison band
    /// and arithmetic to match the most common PostgreSQL use patterns and
    /// avoid mandatory parentheses in practical queries.
    #[must_use]
    pub const fn precedence(self) -> u8 {
        match self {
            Self::Or => 1,
            Self::And => 2,
            // Comparison and regex band
            Self::Eq
            | Self::NotEq
            | Self::Lt
            | Self::LtEq
            | Self::Gt
            | Self::GtEq
            | Self::Like
            | Self::NotLike
            | Self::Ilike
            | Self::NotIlike
            | Self::RegexMatch
            | Self::RegexIMatch
            | Self::RegexNotMatch
            | Self::RegexNotIMatch
            | Self::NetworkContainedEq
            | Self::NetworkContainsEq => 3,
            // JSON operators, concat, and bitwise and/or/xor
            Self::JsonGet
            | Self::JsonGetText
            | Self::JsonGetPath
            | Self::JsonGetPathText
            | Self::JsonContains
            | Self::JsonContained
            | Self::Overlap
            | Self::JsonHasKey
            | Self::JsonHasAnyKey
            | Self::JsonHasAllKeys
            | Self::TextSearchMatch
            | Self::Concat
            | Self::BitAnd
            | Self::BitOr
            | Self::BitXor
            | Self::VectorL2Distance
            | Self::VectorNegativeInnerProduct
            | Self::VectorCosineDistance
            | Self::VectorL1Distance => 4,
            // Bitwise shift (tighter than add/sub)
            Self::ShiftLeft | Self::ShiftRight => 5,
            Self::Add | Self::Sub => 6,
            Self::Mul | Self::Div | Self::Mod => 7,
            Self::Pow => 8,
        }
    }

    /// `true` iff this operator is right-associative.
    #[must_use]
    pub const fn is_right_associative(self) -> bool {
        matches!(self, Self::Pow)
    }

    /// `true` iff this operator is a comparison operator valid for
    /// `ANY (subquery)` and `ALL (subquery)`.
    #[must_use]
    pub const fn is_comparison(self) -> bool {
        matches!(
            self,
            Self::Eq | Self::NotEq | Self::Lt | Self::LtEq | Self::Gt | Self::GtEq
        )
    }
}

// ============================================================================
// Savepoint statements
// ============================================================================

/// `SAVEPOINT name`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SavepointStmt {
    /// Savepoint name.
    pub name: Identifier,
    /// Source span.
    pub span: Span,
}

/// `ROLLBACK TO [SAVEPOINT] name`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RollbackToSavepointStmt {
    /// Savepoint name.
    pub name: Identifier,
    /// Source span.
    pub span: Span,
}

/// `RELEASE [SAVEPOINT] name`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReleaseSavepointStmt {
    /// Savepoint name.
    pub name: Identifier,
    /// Source span.
    pub span: Span,
}

// ============================================================================
// EXPLAIN statement
// ============================================================================

/// `EXPLAIN [ANALYZE] [VERBOSE] [(FORMAT TEXT|JSON)] stmt`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExplainStmt {
    /// Whether `ANALYZE` was specified.
    pub analyze: bool,
    /// Whether `VERBOSE` was specified.
    pub verbose: bool,
    /// Output format.
    pub format: ExplainFormat,
    /// The inner statement being explained.
    pub statement: Box<Statement>,
    /// Source span.
    pub span: Span,
}

/// Output format for `EXPLAIN`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExplainFormat {
    /// `FORMAT TEXT` (default).
    Text,
    /// `FORMAT JSON`.
    Json,
}

// ============================================================================
// PREPARE / EXECUTE / DEALLOCATE statements
// ============================================================================

/// `PREPARE name [(param_type, …)] AS stmt`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrepareStmt {
    /// Prepared statement name.
    pub name: Identifier,
    /// Optional parameter type list.
    pub param_types: Vec<Identifier>,
    /// The statement body.
    pub statement: Box<Statement>,
    /// Source span.
    pub span: Span,
}

/// `EXECUTE name [(arg, …)]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecuteStmt {
    /// Prepared statement name.
    pub name: Identifier,
    /// Arguments (may be empty).
    pub args: Vec<Expr>,
    /// Source span.
    pub span: Span,
}

/// `DEALLOCATE { ALL | name }`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeallocateStmt {
    /// Prepared statement name, or `None` when `ALL` was specified.
    pub name: Option<Identifier>,
    /// Whether `ALL` was specified.
    pub all: bool,
    /// Source span.
    pub span: Span,
}
