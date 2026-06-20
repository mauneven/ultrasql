//! SQL abstract syntax tree.
//!
//! AST nodes are owned (no lifetime parameters tying them to the source
//! string). They carry [`Span`]s so error messages can quote source
//! exactly. Identifiers preserve their original quoting state so a
//! later printer can round-trip a parsed statement.
//!
//! The node definitions are grouped into topical submodules and re-exported
//! here so that every `crate::ast::*` path resolves unchanged.

mod copy_ddl;
mod dml;
mod expr;
mod query;
mod schema;
mod stmt_misc;

pub use copy_ddl::*;
pub use dml::*;
pub use expr::*;
pub use query::*;
pub use schema::*;
pub use stmt_misc::*;

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
    /// `EXPORT DATABASE TO 'path'`.
    ExportDatabase(ExportDatabaseStmt),
    /// `IMPORT DATABASE FROM 'path'`.
    ImportDatabase(ImportDatabaseStmt),
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
    /// `CREATE [OR REPLACE] VIEW … AS SELECT …`.
    CreateView(Box<CreateViewStmt>),
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
    /// `ALTER VIEW …`.
    AlterView(Box<AlterViewStmt>),
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
            Self::ExportDatabase(s) => s.span,
            Self::ImportDatabase(s) => s.span,
            Self::Checkpoint { span } => *span,
            Self::Begin { span, .. } | Self::Commit { span } | Self::Rollback { span } => *span,
            Self::CreateTable(s) => s.span,
            Self::CreateTableAs(s) => s.span,
            Self::CreateMaterializedView(s) => s.span,
            Self::CreateView(s) => s.span,
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
            Self::AlterView(s) => s.span,
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
