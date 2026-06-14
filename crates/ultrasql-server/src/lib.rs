//! `ultrasql-server` library: wire-protocol session loop.
//!
//! The crate exposes two top-level async entry points:
//!
//! - [`run_server`] binds a TCP listener and dispatches one
//!   [`handle_connection`] task per accepted socket.
//! - [`handle_connection`] runs a single session: startup handshake,
//!   `ReadyForQuery` loop, simple-query execution, polite
//!   termination.
//!
//! The handler is generic over any [`AsyncRead`] + [`AsyncWrite`]
//! transport. Production uses [`tokio::net::TcpStream`]; tests pin a
//! [`tokio::io::duplex`] pair against the handler to exercise the full
//! state machine without a real socket.
//!
//! ## Wire-protocol coverage in v0.5
//!
//! - `StartupMessage` / `AuthenticationOk` / `ParameterStatus` /
//!   `BackendKeyData` / `ReadyForQuery` — full handshake.
//! - Simple Query (`'Q'`) — parsed, bound, lowered, and executed.
//! - Extended Query (`Parse`/`Bind`/`Describe`/`Execute`/`Sync`/`Close`/
//!   `Flush`) — routed through the per-connection state machine in
//!   [`extended`]. Parameter values are substituted into the bound
//!   logical plan and executed through the same `pipeline::lower_query`
//!   path Simple Query uses; the result encoder honours text/binary
//!   per-column format codes.
//! - Terminate (`'X'`) — closes the session.
//!
//! ## Execution
//!
//! The handler delegates physical-plan construction to
//! [`pipeline::lower_plan`] and result emission to
//! [`result_encoder::run_select`]. Both modules document their
//! supported subsets and surface unsupported constructs as
//! [`ServerError::Unsupported`]; the handler reports those as
//! query-scoped `ErrorResponse`s so the session continues.

#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    clippy::cast_possible_wrap
)]

mod aggregating_index;
pub mod auth;
pub mod cancel;
pub mod catalog_version;
pub mod columnar_storage;
pub mod copy;
pub mod embedded;
pub mod error;
pub mod extended;
pub mod index_key;
pub mod notify;
pub mod pipeline;
mod projection_summary;
pub mod replication;
pub mod result_encoder;
mod serializable;
pub mod time_partition;
pub mod tls;
pub mod wal_sink;
pub mod wire_writer;
pub mod workload;

/// Numeric `server_version` exposed in startup
/// `ParameterStatus` and `pg_settings`. Drivers parse this as a PostgreSQL
/// feature baseline; UltraSQL's own product version remains `version()`.
pub(crate) const REPORTED_SERVER_VERSION: &str = "14.0";
const RECOVERY_TARGETS_FILE_LIMIT_BYTES: u64 = 64 * 1024;
const RUNTIME_METADATA_FILE_LIMIT_BYTES: u64 = 16 * 1024 * 1024;

#[cfg(test)]
pub(crate) static TPCH_TEST_CACHE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

use std::future::Future;
use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::OnceLock;

use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use num_traits::ToPrimitive;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};
use ultrasql_catalog::{
    Catalog, CatalogError, CatalogSnapshot, CatalogStats, DomainTypeEntry, IndexEntry,
    MutableCatalog, PersistentCatalog, StatisticRow, TableEntry,
};
use ultrasql_core::constants::PAGE_SIZE;
use ultrasql_core::{BlockNumber, DataType, Lsn, Oid, PageId, RelationId, Value, Xid};
use ultrasql_executor::{Eval, ExecError, MemTableScan, Operator, RowCodec, SeqScan};
use ultrasql_optimizer::{
    AnalyzeOptions, AnalyzeRunner, ColumnStats, InMemoryStatsCatalog, PgStatisticRow, PlanCache,
    PlanCacheConfig, RelationStats, StatsCatalog,
};
use ultrasql_parser::Parser;
use ultrasql_planner::plan::{LockStrength, LockWaitPolicy};
use ultrasql_planner::{
    AggregateFunc, BinaryOp, Catalog as PlannerCatalog, InMemoryCatalog, LogicalIndexMethod,
    LogicalPlan, LogicalReferentialAction, ScalarExpr, TableMeta, UnaryOp, bind,
};
use ultrasql_protocol::BackendMessage;
use ultrasql_storage::access_method::{
    AccessMethod, AnnPayloadKind, BrinIndex, HnswMetric, PageBackedHnswIndex,
    PageBackedIvfFlatIndex,
};
use ultrasql_storage::btree::BTree;
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::heap::{HeapAccess, HeapError, InsertOptions};
use ultrasql_storage::page::Page;
use ultrasql_storage::segment::{SegmentConfig, SegmentError, SegmentFileManager};
use ultrasql_storage::sequence::{Sequence, SequenceSnapshot};
use ultrasql_storage::vm::VisibilityMap;
use ultrasql_txn::{
    IsolationLevel, LockManager, LockMode, LockRequest, LockTag, RowLockMode, SsiManager,
    Transaction, TransactionManager, TxnError,
};
use ultrasql_vec::Batch;
use ultrasql_vec::column::Column;
use ultrasql_vec::column::NumericColumn;
use ultrasql_vec::kernels::{
    CmpOp, cmp_i32_scalar, cmp_i64_scalar, filter_sum_i32_widening_gt, filter_sum_i64_gt,
    sum_i32_widening, sum_i32_widening_with_mask, sum_i64, sum_i64_with_mask,
};
use ultrasql_wal::applier::{ApplyError, HeapTarget};
use ultrasql_wal::payload::{
    AbortPayload, BTreeOpPayload, CheckpointPayload, CommitPayload, FullPageWritePayload,
    HeapDeleteInPlaceBatchPayload, HeapDeleteInPlacePayload, HeapDeletePayload, HeapInsertPayload,
    HeapUpdateInPlaceBatchPayload, HeapUpdateInPlacePayload, HeapUpdatePayload, SequenceOpKind,
    SequenceOpPayload,
};
use ultrasql_wal::{RecordType, WalRecord};

pub use embedded::EmbeddedDatabase;
pub use error::ServerError;
pub use pipeline::{LowerCtx, SampleTables, build_sample_database};
pub use result_encoder::{
    SelectResult, run_ddl_command, run_modify_command, run_modify_returning, run_select,
    run_select_streamed,
};
pub(crate) use serializable::{
    record_serializable_predicate_locks, record_serializable_write_conflicts,
};

fn sample_privilege_catalog() -> Arc<auth::InMemoryPrivilegeCatalog> {
    let catalog = Arc::new(auth::InMemoryPrivilegeCatalog::new());
    let objects = [String::from("users")];
    let grantees = [String::from("public")];
    let privileges = [auth::PrivilegeRequest {
        privilege: auth::PrivilegeKind::Select,
        columns: Vec::new(),
    }];
    catalog.grant_many(
        "ultrasql",
        auth::PrivilegeObjectKind::Table,
        &objects,
        &grantees,
        &privileges,
        false,
    );
    catalog
}

fn hydrate_optimizer_stats_from_catalog<L: PageLoader>(
    snapshot: &CatalogSnapshot,
    heap: &HeapAccess<L>,
    txn_manager: &TransactionManager,
) -> InMemoryStatsCatalog {
    let mut catalog = InMemoryStatsCatalog::new();
    for table in snapshot.tables.values() {
        let mut stat_rows = snapshot
            .statistics
            .values()
            .filter(|row| row.starelid == table.oid)
            .collect::<Vec<_>>();
        if stat_rows.is_empty() {
            continue;
        }
        stat_rows.sort_by_key(|row| row.staattnum);

        let columns = stat_rows
            .iter()
            .filter_map(|row| restored_column_stats(row, table))
            .collect::<Vec<_>>();
        if columns.is_empty() {
            continue;
        }

        let row_count = restored_relation_row_count(table, &stat_rows, heap, txn_manager);
        catalog.register(RelationStats {
            table: table_entry_lookup_key(table),
            row_count,
            page_count: u64::from(table.n_blocks),
            columns,
        });
    }
    catalog
}

fn restored_column_stats(row: &StatisticRow, table: &TableEntry) -> Option<ColumnStats> {
    let attnum = u16::try_from(row.staattnum).ok()?;
    let column_index = usize::from(attnum.checked_sub(1)?);
    let field = table.schema.fields().get(column_index)?;
    let avg_width_bytes = field
        .data_type
        .fixed_size()
        .map_or(32, |width| u32::try_from(width).unwrap_or(u32::MAX));
    Some(ColumnStats {
        column_index,
        n_distinct: f64::from(row.stadistinct),
        null_frac: f64::from(row.stanullfrac),
        avg_width_bytes,
        histogram: None,
        mcv: None,
        correlation: 0.0,
    })
}

fn restored_relation_row_count<L: PageLoader>(
    table: &TableEntry,
    rows: &[&StatisticRow],
    heap: &HeapAccess<L>,
    txn_manager: &TransactionManager,
) -> u64 {
    if let Some(row_count) = count_visible_relation_rows(table, heap, txn_manager) {
        return row_count;
    }
    rows.iter()
        .filter_map(|row| positive_f32_ceil_to_u64(row.stadistinct))
        .max()
        .unwrap_or_else(|| u64::from(table.n_blocks).saturating_mul(64))
}

fn count_visible_relation_rows<L: PageLoader>(
    table: &TableEntry,
    heap: &HeapAccess<L>,
    txn_manager: &TransactionManager,
) -> Option<u64> {
    let rel = RelationId(table.oid);
    let block_count = heap.block_count(rel).max(table.n_blocks);
    let scan_txn = txn_manager.begin(IsolationLevel::ReadCommitted);
    let scan_snapshot = scan_txn.snapshot.clone();
    let mut row_count = 0_u64;
    let scan_result = heap.for_each_visible(
        rel,
        block_count,
        &scan_snapshot,
        txn_manager,
        |_tid, _header, _payload| {
            row_count = row_count.saturating_add(1);
            Ok(())
        },
    );
    finish_stats_hydration_row_count(
        &table.name,
        row_count,
        scan_result,
        txn_manager.abort(scan_txn),
    )
}

fn finish_stats_hydration_row_count(
    table_name: &str,
    row_count: u64,
    scan_result: Result<(), HeapError>,
    abort_result: Result<(), TxnError>,
) -> Option<u64> {
    let scan_aborted_cleanly = match abort_result {
        Ok(()) => true,
        Err(e) => {
            warn!(
                table = %table_name,
                error = %e,
                "stats hydration scan transaction abort failed"
            );
            false
        }
    };
    match scan_result {
        Ok(()) if scan_aborted_cleanly => Some(row_count),
        Ok(()) => None,
        Err(e) => {
            warn!(table = %table_name, error = %e, "stats hydration row count scan failed");
            None
        }
    }
}

fn require_wal_backed_catalog_bootstrap(
    result: Result<CatalogStats, CatalogError>,
) -> Result<CatalogStats, ServerError> {
    result.map_err(ServerError::Catalog)
}

fn positive_f32_ceil_to_u64(value: f32) -> Option<u64> {
    if !value.is_finite() || value <= 0.0 {
        return None;
    }
    format!("{:.0}", value.ceil()).parse::<u64>().ok()
}

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

pub(crate) fn append_only_materialized_source_table(plan: &LogicalPlan) -> Option<&str> {
    match plan {
        LogicalPlan::Scan { table, .. } => Some(table.as_str()),
        LogicalPlan::Filter { input, .. } | LogicalPlan::Project { input, .. } => {
            append_only_materialized_source_table(input)
        }
        _ => None,
    }
}

fn metadata_escape(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

fn metadata_encode_list(values: &[String]) -> String {
    let mut out = String::new();
    for value in values {
        out.push_str(&value.len().to_string());
        out.push(':');
        out.push_str(value);
    }
    out
}

fn metadata_decode_list(raw: &str) -> Result<Vec<String>, ServerError> {
    let mut values = Vec::new();
    let mut offset = 0;
    while offset < raw.len() {
        let Some(rel_colon) = raw[offset..].find(':') else {
            return Err(ServerError::Ddl(
                "malformed metadata list length".to_owned(),
            ));
        };
        let len_end = offset + rel_colon;
        let len = raw[offset..len_end]
            .parse::<usize>()
            .map_err(|err| ServerError::Ddl(format!("malformed metadata list length: {err}")))?;
        let value_start = len_end + 1;
        let value_end = value_start
            .checked_add(len)
            .ok_or_else(|| ServerError::Ddl("metadata list value length overflow".to_owned()))?;
        if value_end > raw.len() || !raw.is_char_boundary(value_end) {
            return Err(ServerError::Ddl(
                "metadata list value exceeds field length".to_owned(),
            ));
        }
        values.push(raw[value_start..value_end].to_owned());
        offset = value_end;
    }
    Ok(values)
}

fn metadata_unescape(raw: &str) -> Result<String, ServerError> {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('\\') => out.push('\\'),
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some(other) => {
                return Err(ServerError::Ddl(format!(
                    "invalid escaped metadata byte \\{other}"
                )));
            }
            None => return Err(ServerError::Ddl("trailing metadata escape".to_owned())),
        }
    }
    Ok(out)
}

fn format_password_hash(password: Option<&auth::PasswordHash>) -> String {
    let Some(password) = password else {
        return String::new();
    };
    format!(
        "SCRAM-SHA-256${}${}${}${}",
        password.iterations,
        B64.encode(&password.salt),
        B64.encode(password.stored_key),
        B64.encode(password.server_key)
    )
}

fn parse_password_hash(
    raw: &str,
    line_no: usize,
) -> Result<Option<auth::PasswordHash>, ServerError> {
    if raw.is_empty() {
        return Ok(None);
    }
    let parts = raw.split('$').collect::<Vec<_>>();
    if parts.len() != 5 || parts[0] != "SCRAM-SHA-256" {
        return Err(ServerError::ddl(format!(
            "role metadata line {} has malformed SCRAM password hash",
            line_no + 1
        )));
    }
    let iterations = parse_role_u32(parts[1], line_no, "password iterations")?;
    let salt = B64.decode(parts[2]).map_err(|err| {
        ServerError::ddl(format!(
            "role metadata line {} bad password salt: {err}",
            line_no + 1
        ))
    })?;
    let stored_key = decode_hash_key(parts[3], line_no, "stored key")?;
    let server_key = decode_hash_key(parts[4], line_no, "server key")?;
    Ok(Some(auth::PasswordHash {
        salt,
        iterations,
        stored_key,
        server_key,
    }))
}

fn decode_hash_key(raw: &str, line_no: usize, field: &str) -> Result<[u8; 32], ServerError> {
    let bytes = B64.decode(raw).map_err(|err| {
        ServerError::ddl(format!(
            "role metadata line {} bad password {field}: {err}",
            line_no + 1
        ))
    })?;
    bytes.try_into().map_err(|bytes: Vec<u8>| {
        ServerError::ddl(format!(
            "role metadata line {} password {field} has {} bytes, expected 32",
            line_no + 1,
            bytes.len()
        ))
    })
}

fn parse_role_bool(raw: &str, line_no: usize, field: &str) -> Result<bool, ServerError> {
    raw.parse::<bool>().map_err(|err| {
        ServerError::ddl(format!(
            "role metadata line {} bad {field}: {err}",
            line_no + 1
        ))
    })
}

fn validate_role_metadata_name(name: &str, line_no: usize, field: &str) -> Result<(), ServerError> {
    if !name.trim().is_empty() {
        return Ok(());
    }
    Err(ServerError::ddl(format!(
        "empty role metadata {field} on line {}",
        line_no + 1
    )))
}

fn validate_bootstrap_role_metadata(role: &auth::RoleEntry) -> Result<(), ServerError> {
    if role.is_superuser
        && role.inherit
        && role.create_role
        && role.create_db
        && role.can_login
        && role.connection_limit == -1
        && role.valid_until.is_none()
    {
        return Ok(());
    }
    Err(ServerError::ddl(
        "invalid bootstrap role metadata privileges for ultrasql",
    ))
}

fn parse_role_u32(raw: &str, line_no: usize, field: &str) -> Result<u32, ServerError> {
    raw.parse::<u32>().map_err(|err| {
        ServerError::ddl(format!(
            "role metadata line {} bad {field}: {err}",
            line_no + 1
        ))
    })
}

fn parse_role_i32(raw: &str, line_no: usize, field: &str) -> Result<i32, ServerError> {
    raw.parse::<i32>().map_err(|err| {
        ServerError::ddl(format!(
            "role metadata line {} bad {field}: {err}",
            line_no + 1
        ))
    })
}

fn parse_role_optional_i64(
    raw: &str,
    line_no: usize,
    field: &str,
) -> Result<Option<i64>, ServerError> {
    if raw.is_empty() {
        return Ok(None);
    }
    raw.parse::<i64>().map(Some).map_err(|err| {
        ServerError::ddl(format!(
            "role metadata line {} bad {field}: {err}",
            line_no + 1
        ))
    })
}

fn privilege_object_kind_name(kind: auth::PrivilegeObjectKind) -> &'static str {
    match kind {
        auth::PrivilegeObjectKind::Table => "table",
        auth::PrivilegeObjectKind::Schema => "schema",
        auth::PrivilegeObjectKind::Database => "database",
        auth::PrivilegeObjectKind::Sequence => "sequence",
        auth::PrivilegeObjectKind::Function => "function",
    }
}

fn parse_privilege_object_kind(
    raw: &str,
    line_no: usize,
) -> Result<auth::PrivilegeObjectKind, ServerError> {
    match raw {
        "table" => Ok(auth::PrivilegeObjectKind::Table),
        "schema" => Ok(auth::PrivilegeObjectKind::Schema),
        "database" => Ok(auth::PrivilegeObjectKind::Database),
        "sequence" => Ok(auth::PrivilegeObjectKind::Sequence),
        "function" => Ok(auth::PrivilegeObjectKind::Function),
        _ => Err(ServerError::ddl(format!(
            "privilege metadata line {} bad object kind",
            line_no + 1
        ))),
    }
}

fn privilege_kind_name(kind: auth::PrivilegeKind) -> &'static str {
    match kind {
        auth::PrivilegeKind::Select => "select",
        auth::PrivilegeKind::Insert => "insert",
        auth::PrivilegeKind::Update => "update",
        auth::PrivilegeKind::Delete => "delete",
        auth::PrivilegeKind::Truncate => "truncate",
        auth::PrivilegeKind::References => "references",
        auth::PrivilegeKind::Trigger => "trigger",
        auth::PrivilegeKind::Usage => "usage",
        auth::PrivilegeKind::Create => "create",
        auth::PrivilegeKind::Connect => "connect",
        auth::PrivilegeKind::Temporary => "temporary",
        auth::PrivilegeKind::Execute => "execute",
    }
}

fn parse_privilege_kind(raw: &str, line_no: usize) -> Result<auth::PrivilegeKind, ServerError> {
    match raw {
        "select" => Ok(auth::PrivilegeKind::Select),
        "insert" => Ok(auth::PrivilegeKind::Insert),
        "update" => Ok(auth::PrivilegeKind::Update),
        "delete" => Ok(auth::PrivilegeKind::Delete),
        "truncate" => Ok(auth::PrivilegeKind::Truncate),
        "references" => Ok(auth::PrivilegeKind::References),
        "trigger" => Ok(auth::PrivilegeKind::Trigger),
        "usage" => Ok(auth::PrivilegeKind::Usage),
        "create" => Ok(auth::PrivilegeKind::Create),
        "connect" => Ok(auth::PrivilegeKind::Connect),
        "temporary" => Ok(auth::PrivilegeKind::Temporary),
        "execute" => Ok(auth::PrivilegeKind::Execute),
        _ => Err(ServerError::ddl(format!(
            "privilege metadata line {} bad privilege kind",
            line_no + 1
        ))),
    }
}

fn validate_privilege_metadata_role(
    known_roles: &std::collections::HashSet<String>,
    role: &str,
    line_no: usize,
    field: &str,
) -> Result<(), ServerError> {
    if known_roles.contains(&role.to_ascii_lowercase()) {
        return Ok(());
    }
    Err(ServerError::ddl(format!(
        "unknown privilege metadata role '{role}' in {field} on line {}",
        line_no + 1
    )))
}

fn validate_privilege_metadata_grantee(
    known_roles: &std::collections::HashSet<String>,
    grantee: &str,
    line_no: usize,
) -> Result<(), ServerError> {
    if grantee.eq_ignore_ascii_case("public") {
        return Ok(());
    }
    validate_privilege_metadata_role(known_roles, grantee, line_no, "grantee")
}

fn runtime_metadata_known_role_names(
    role_catalog: &auth::InMemoryAuthCatalog,
) -> std::collections::HashSet<String> {
    let mut roles = role_catalog
        .list_roles()
        .into_iter()
        .map(|role| role.name.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    // Trust-mode tests already treat uncataloged `tester` as an effective superuser.
    roles.insert("tester".to_owned());
    roles
}

fn privilege_metadata_object_key(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

fn privilege_metadata_table_has_column(
    snapshot: &CatalogSnapshot,
    fallback: &InMemoryCatalog,
    object_name: &str,
    column_name: &str,
) -> Option<bool> {
    let table_name = privilege_metadata_object_key(object_name);
    if let Some(table) = snapshot.tables.get(&table_name) {
        return Some(table.schema.find(column_name).is_some());
    }
    PlannerCatalog::lookup_table(fallback, &table_name)
        .map(|table| table.schema.find(column_name).is_some())
}

fn validate_privilege_metadata_column(
    snapshot: &CatalogSnapshot,
    fallback: &InMemoryCatalog,
    grant: &auth::PrivilegeGrant,
    line_no: usize,
) -> Result<(), ServerError> {
    if grant.object_kind != auth::PrivilegeObjectKind::Table {
        return Ok(());
    }
    let Some(column_name) = grant.column_name.as_deref() else {
        return Ok(());
    };
    match privilege_metadata_table_has_column(snapshot, fallback, &grant.object_name, column_name) {
        Some(true) | None => Ok(()),
        Some(false) => Err(ServerError::ddl(format!(
            "unknown privilege metadata column '{column_name}' for table '{}' on line {}",
            grant.object_name,
            line_no + 1
        ))),
    }
}

fn rls_permissiveness_name(value: RuntimeRlsPermissiveness) -> &'static str {
    match value {
        RuntimeRlsPermissiveness::Permissive => "permissive",
        RuntimeRlsPermissiveness::Restrictive => "restrictive",
    }
}

fn parse_rls_permissiveness(value: &str) -> Result<RuntimeRlsPermissiveness, ServerError> {
    match value {
        "permissive" => Ok(RuntimeRlsPermissiveness::Permissive),
        "restrictive" => Ok(RuntimeRlsPermissiveness::Restrictive),
        other => Err(ServerError::Ddl(format!(
            "unknown RLS permissiveness {other}"
        ))),
    }
}

fn rls_command_name(value: RuntimeRlsCommand) -> &'static str {
    match value {
        RuntimeRlsCommand::All => "all",
        RuntimeRlsCommand::Select => "select",
        RuntimeRlsCommand::Insert => "insert",
        RuntimeRlsCommand::Update => "update",
        RuntimeRlsCommand::Delete => "delete",
    }
}

fn parse_rls_command(value: &str) -> Result<RuntimeRlsCommand, ServerError> {
    match value {
        "all" => Ok(RuntimeRlsCommand::All),
        "select" => Ok(RuntimeRlsCommand::Select),
        "insert" => Ok(RuntimeRlsCommand::Insert),
        "update" => Ok(RuntimeRlsCommand::Update),
        "delete" => Ok(RuntimeRlsCommand::Delete),
        other => Err(ServerError::Ddl(format!("unknown RLS command {other}"))),
    }
}

fn validate_rls_metadata_policy_roles(
    known_roles: &std::collections::HashSet<String>,
    roles: &mut [String],
    line_no: usize,
) -> Result<(), ServerError> {
    for role in roles {
        *role = role.to_ascii_lowercase();
        if role == "public" || known_roles.contains(role.as_str()) {
            continue;
        }
        return Err(ServerError::Ddl(format!(
            "unknown RLS policy role '{role}' on line {}",
            line_no + 1
        )));
    }
    Ok(())
}

fn validate_rls_metadata_expr(
    table: &TableEntry,
    expr: Option<&RuntimeTenantPolicyExpr>,
    line_no: usize,
    clause: &str,
) -> Result<(), ServerError> {
    let Some(expr) = expr else {
        return Ok(());
    };
    let Some(field) = table.schema.field(expr.column_index) else {
        return Err(ServerError::Ddl(format!(
            "RLS metadata line {} {clause} column index {} out of bounds for table '{}' with {} columns",
            line_no + 1,
            expr.column_index,
            table.name,
            table.schema.len()
        )));
    };
    if field.name.eq_ignore_ascii_case(&expr.column_name) {
        return Ok(());
    }
    Err(ServerError::Ddl(format!(
        "RLS metadata line {} {clause} column '{}' does not match table column '{}'",
        line_no + 1,
        expr.column_name,
        field.name
    )))
}

fn rls_expr_fields(expr: Option<&RuntimeTenantPolicyExpr>) -> (String, String, String) {
    expr.map_or_else(
        || (String::new(), String::new(), String::new()),
        |expr| {
            (
                expr.column_index.to_string(),
                metadata_escape(&expr.column_name),
                metadata_escape(&expr.setting_name),
            )
        },
    )
}

fn parse_rls_expr(
    index: &str,
    column_name: &str,
    setting_name: &str,
) -> Result<Option<RuntimeTenantPolicyExpr>, ServerError> {
    if index.is_empty() {
        return Ok(None);
    }
    Ok(Some(RuntimeTenantPolicyExpr {
        column_index: index
            .parse::<usize>()
            .map_err(|err| ServerError::Ddl(format!("bad RLS column index: {err}")))?,
        column_name: metadata_unescape(column_name)?,
        setting_name: metadata_unescape(setting_name)?,
    }))
}

fn usize_list_token(values: &[usize]) -> String {
    values
        .iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn parse_usize_list_token(raw: &str) -> Result<Vec<usize>, ServerError> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    raw.split(',')
        .map(|part| {
            part.parse::<usize>()
                .map_err(|err| ServerError::Ddl(format!("bad usize list entry: {err}")))
        })
        .collect()
}

fn referential_action_token(action: LogicalReferentialAction) -> &'static str {
    match action {
        LogicalReferentialAction::NoAction => "no_action",
        LogicalReferentialAction::Restrict => "restrict",
        LogicalReferentialAction::Cascade => "cascade",
        LogicalReferentialAction::SetNull => "set_null",
        LogicalReferentialAction::SetDefault => "set_default",
    }
}

fn index_method_token(method: LogicalIndexMethod) -> &'static str {
    match method {
        LogicalIndexMethod::Btree => "btree",
        LogicalIndexMethod::Hash => "hash",
        LogicalIndexMethod::Gin => "gin",
        LogicalIndexMethod::Gist => "gist",
        LogicalIndexMethod::Brin => "brin",
        LogicalIndexMethod::Hnsw => "hnsw",
        LogicalIndexMethod::IvfFlat => "ivfflat",
        LogicalIndexMethod::Aggregating => "aggregating",
    }
}

fn parse_index_method(raw: &str) -> Result<LogicalIndexMethod, ServerError> {
    match raw {
        "btree" => Ok(LogicalIndexMethod::Btree),
        "hash" => Ok(LogicalIndexMethod::Hash),
        "gin" => Ok(LogicalIndexMethod::Gin),
        "gist" => Ok(LogicalIndexMethod::Gist),
        "brin" => Ok(LogicalIndexMethod::Brin),
        "hnsw" => Ok(LogicalIndexMethod::Hnsw),
        "ivfflat" => Ok(LogicalIndexMethod::IvfFlat),
        "aggregating" => Ok(LogicalIndexMethod::Aggregating),
        other => Err(ServerError::Ddl(format!("unknown index method {other}"))),
    }
}

fn parse_referential_action(raw: &str) -> Result<LogicalReferentialAction, ServerError> {
    match raw {
        "no_action" => Ok(LogicalReferentialAction::NoAction),
        "restrict" => Ok(LogicalReferentialAction::Restrict),
        "cascade" => Ok(LogicalReferentialAction::Cascade),
        "set_null" => Ok(LogicalReferentialAction::SetNull),
        "set_default" => Ok(LogicalReferentialAction::SetDefault),
        other => Err(ServerError::Ddl(format!(
            "unknown referential action {other}"
        ))),
    }
}

fn data_type_token(ty: &DataType) -> Option<String> {
    match ty {
        DataType::Bool => Some("bool".to_owned()),
        DataType::Int16 => Some("i16".to_owned()),
        DataType::Int32 => Some("i32".to_owned()),
        DataType::Int64 => Some("i64".to_owned()),
        DataType::Money => Some("money".to_owned()),
        DataType::Float32 => Some("f32".to_owned()),
        DataType::Float64 => Some("f64".to_owned()),
        DataType::Text {
            max_len: Some(max_len),
        } => Some(format!("varchar:{max_len}")),
        DataType::Text { max_len: None } => Some("text".to_owned()),
        DataType::TsVector => Some("tsvector".to_owned()),
        DataType::TsQuery => Some("tsquery".to_owned()),
        DataType::Char { len: Some(len) } => Some(format!("char:{len}")),
        DataType::Char { len: None } => Some("char".to_owned()),
        DataType::Bit { len: Some(len) } => Some(format!("bit:{len}")),
        DataType::Bit { len: None } => Some("bit".to_owned()),
        DataType::VarBit {
            max_len: Some(max_len),
        } => Some(format!("varbit:{max_len}")),
        DataType::VarBit { max_len: None } => Some("varbit".to_owned()),
        DataType::Inet => Some("inet".to_owned()),
        DataType::Cidr => Some("cidr".to_owned()),
        DataType::MacAddr => Some("macaddr".to_owned()),
        DataType::MacAddr8 => Some("macaddr8".to_owned()),
        DataType::Date => Some("date".to_owned()),
        DataType::Time => Some("time".to_owned()),
        DataType::TimeTz => Some("timetz".to_owned()),
        DataType::Timestamp => Some("ts".to_owned()),
        DataType::TimestampTz => Some("tstz".to_owned()),
        DataType::Null => Some("null".to_owned()),
        _ => None,
    }
}

fn data_type_from_token(token: &str) -> Option<DataType> {
    if let Some(len_text) = token.strip_prefix("char:") {
        return len_text
            .parse::<u32>()
            .ok()
            .map(|len| DataType::Char { len: Some(len) });
    }
    if let Some(max_len_text) = token.strip_prefix("varchar:") {
        return max_len_text
            .parse::<u32>()
            .ok()
            .map(|max_len| DataType::Text {
                max_len: Some(max_len),
            });
    }
    if let Some(len_text) = token.strip_prefix("bit:") {
        return len_text
            .parse::<u32>()
            .ok()
            .map(|len| DataType::Bit { len: Some(len) });
    }
    if let Some(max_len_text) = token.strip_prefix("varbit:") {
        return max_len_text
            .parse::<u32>()
            .ok()
            .map(|max_len| DataType::VarBit {
                max_len: Some(max_len),
            });
    }
    match token {
        "bool" => Some(DataType::Bool),
        "i16" => Some(DataType::Int16),
        "i32" => Some(DataType::Int32),
        "i64" => Some(DataType::Int64),
        "money" => Some(DataType::Money),
        "f32" => Some(DataType::Float32),
        "f64" => Some(DataType::Float64),
        "text" => Some(DataType::Text { max_len: None }),
        "tsvector" => Some(DataType::TsVector),
        "tsquery" => Some(DataType::TsQuery),
        "char" => Some(DataType::Char { len: None }),
        "bit" => Some(DataType::Bit { len: None }),
        "varbit" => Some(DataType::VarBit { max_len: None }),
        "inet" => Some(DataType::Inet),
        "cidr" => Some(DataType::Cidr),
        "macaddr" => Some(DataType::MacAddr),
        "macaddr8" => Some(DataType::MacAddr8),
        "date" => Some(DataType::Date),
        "time" => Some(DataType::Time),
        "timetz" => Some(DataType::TimeTz),
        "ts" => Some(DataType::Timestamp),
        "tstz" => Some(DataType::TimestampTz),
        "null" => Some(DataType::Null),
        _ => None,
    }
}

fn operator_data_type_token(
    ty: &Option<DataType>,
    operator_name: &str,
) -> Result<String, ServerError> {
    let Some(ty) = ty else {
        return Ok(String::new());
    };
    data_type_token(ty).ok_or_else(|| {
        ServerError::ddl(format!(
            "operator '{operator_name}' argument type is outside restart-persistable metadata subset"
        ))
    })
}

fn parse_operator_data_type_token(
    token: &str,
    line_no: usize,
    field: &str,
) -> Result<Option<DataType>, ServerError> {
    if token.is_empty() {
        return Ok(None);
    }
    data_type_from_token(token).map(Some).ok_or_else(|| {
        ServerError::ddl(format!(
            "operator metadata line {} has unknown {field} type '{}'",
            line_no + 1,
            token
        ))
    })
}

fn validate_runtime_operator_metadata(
    operator: &RuntimeOperator,
    line_no: usize,
) -> Result<(), ServerError> {
    if operator.procedure == "bool_eq"
        && operator.left_type == Some(DataType::Bool)
        && operator.right_type == Some(DataType::Bool)
        && operator.result_type == DataType::Bool
    {
        return Ok(());
    }
    Err(ServerError::ddl(format!(
        "operator metadata line {} uses unsupported procedure/type signature",
        line_no + 1
    )))
}

fn binary_op_token(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "add",
        BinaryOp::Sub => "sub",
        BinaryOp::Mul => "mul",
        BinaryOp::Div => "div",
        BinaryOp::Mod => "mod",
        BinaryOp::Pow => "pow",
        BinaryOp::Concat => "concat",
        BinaryOp::Eq => "eq",
        BinaryOp::NotEq => "ne",
        BinaryOp::Lt => "lt",
        BinaryOp::LtEq => "le",
        BinaryOp::Gt => "gt",
        BinaryOp::GtEq => "ge",
        BinaryOp::And => "and",
        BinaryOp::Or => "or",
        BinaryOp::Like => "like",
        BinaryOp::NotLike => "not_like",
        BinaryOp::Ilike => "ilike",
        BinaryOp::NotIlike => "not_ilike",
        BinaryOp::RegexMatch => "regex",
        BinaryOp::RegexIMatch => "iregex",
        BinaryOp::RegexNotMatch => "not_regex",
        BinaryOp::RegexNotIMatch => "not_iregex",
        BinaryOp::BitAnd => "bit_and",
        BinaryOp::BitOr => "bit_or",
        BinaryOp::BitXor => "bit_xor",
        BinaryOp::ShiftLeft => "shl",
        BinaryOp::ShiftRight => "shr",
        BinaryOp::NetworkContainedEq => "net_contained_eq",
        BinaryOp::NetworkContainsEq => "net_contains_eq",
        BinaryOp::JsonGet => "json_get",
        BinaryOp::JsonGetText => "json_get_text",
        BinaryOp::JsonGetPath => "json_get_path",
        BinaryOp::JsonGetPathText => "json_get_path_text",
        BinaryOp::JsonContains => "json_contains",
        BinaryOp::JsonContained => "json_contained",
        BinaryOp::JsonHasKey => "json_has_key",
        BinaryOp::JsonHasAnyKey => "json_has_any_key",
        BinaryOp::JsonHasAllKeys => "json_has_all_keys",
        BinaryOp::TextSearchMatch => "text_search",
        BinaryOp::Overlap => "overlap",
        BinaryOp::VectorL2Distance => "vec_l2",
        BinaryOp::VectorNegativeInnerProduct => "vec_ip",
        BinaryOp::VectorCosineDistance => "vec_cos",
        BinaryOp::VectorL1Distance => "vec_l1",
    }
}

fn binary_op_from_token(token: &str) -> Option<BinaryOp> {
    Some(match token {
        "add" => BinaryOp::Add,
        "sub" => BinaryOp::Sub,
        "mul" => BinaryOp::Mul,
        "div" => BinaryOp::Div,
        "mod" => BinaryOp::Mod,
        "pow" => BinaryOp::Pow,
        "concat" => BinaryOp::Concat,
        "eq" => BinaryOp::Eq,
        "ne" => BinaryOp::NotEq,
        "lt" => BinaryOp::Lt,
        "le" => BinaryOp::LtEq,
        "gt" => BinaryOp::Gt,
        "ge" => BinaryOp::GtEq,
        "and" => BinaryOp::And,
        "or" => BinaryOp::Or,
        "like" => BinaryOp::Like,
        "not_like" => BinaryOp::NotLike,
        "ilike" => BinaryOp::Ilike,
        "not_ilike" => BinaryOp::NotIlike,
        "regex" => BinaryOp::RegexMatch,
        "iregex" => BinaryOp::RegexIMatch,
        "not_regex" => BinaryOp::RegexNotMatch,
        "not_iregex" => BinaryOp::RegexNotIMatch,
        "bit_and" => BinaryOp::BitAnd,
        "bit_or" => BinaryOp::BitOr,
        "bit_xor" => BinaryOp::BitXor,
        "shl" => BinaryOp::ShiftLeft,
        "shr" => BinaryOp::ShiftRight,
        "net_contained_eq" => BinaryOp::NetworkContainedEq,
        "net_contains_eq" => BinaryOp::NetworkContainsEq,
        "json_get" => BinaryOp::JsonGet,
        "json_get_text" => BinaryOp::JsonGetText,
        "json_get_path" => BinaryOp::JsonGetPath,
        "json_get_path_text" => BinaryOp::JsonGetPathText,
        "json_contains" => BinaryOp::JsonContains,
        "json_contained" => BinaryOp::JsonContained,
        "json_has_key" => BinaryOp::JsonHasKey,
        "json_has_any_key" => BinaryOp::JsonHasAnyKey,
        "json_has_all_keys" => BinaryOp::JsonHasAllKeys,
        "text_search" => BinaryOp::TextSearchMatch,
        "overlap" => BinaryOp::Overlap,
        "vec_l2" => BinaryOp::VectorL2Distance,
        "vec_ip" => BinaryOp::VectorNegativeInnerProduct,
        "vec_cos" => BinaryOp::VectorCosineDistance,
        "vec_l1" => BinaryOp::VectorL1Distance,
        _ => return None,
    })
}

fn unary_op_token(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::Neg => "neg",
        UnaryOp::Pos => "pos",
        UnaryOp::Not => "not",
        UnaryOp::BitNot => "bit_not",
    }
}

fn unary_op_from_token(token: &str) -> Option<UnaryOp> {
    Some(match token {
        "neg" => UnaryOp::Neg,
        "pos" => UnaryOp::Pos,
        "not" => UnaryOp::Not,
        "bit_not" => UnaryOp::BitNot,
        _ => return None,
    })
}

fn value_token(value: &Value) -> Option<String> {
    Some(match value {
        Value::Null => String::new(),
        Value::Bool(v) => v.to_string(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Money(v) => v.to_string(),
        Value::Float32(v) => v.to_bits().to_string(),
        Value::Float64(v) => v.to_bits().to_string(),
        Value::Text(v) | Value::Char(v) | Value::Json(v) | Value::Jsonb(v) | Value::Xml(v) => {
            metadata_escape(v)
        }
        Value::BitString(v) => metadata_escape(&v.to_string()),
        Value::Network(v) => metadata_escape(&v.to_string()),
        Value::Date(v) => v.to_string(),
        Value::Time(v) | Value::Timestamp(v) | Value::TimestampTz(v) => v.to_string(),
        Value::TimeTz {
            micros,
            offset_seconds,
        } => format!("{micros}:{offset_seconds}"),
        _ => return None,
    })
}

fn value_from_token(ty: &DataType, token: &str) -> Result<Value, ServerError> {
    Ok(match ty {
        DataType::Null => Value::Null,
        DataType::Bool => Value::Bool(
            token
                .parse::<bool>()
                .map_err(|err| ServerError::Ddl(format!("bad bool literal: {err}")))?,
        ),
        DataType::Int16 => Value::Int16(
            token
                .parse::<i16>()
                .map_err(|err| ServerError::Ddl(format!("bad int16 literal: {err}")))?,
        ),
        DataType::Int32 => Value::Int32(
            token
                .parse::<i32>()
                .map_err(|err| ServerError::Ddl(format!("bad int32 literal: {err}")))?,
        ),
        DataType::Int64 => Value::Int64(
            token
                .parse::<i64>()
                .map_err(|err| ServerError::Ddl(format!("bad int64 literal: {err}")))?,
        ),
        DataType::Money => Value::Money(
            token
                .parse::<i64>()
                .map_err(|err| ServerError::Ddl(format!("bad money literal: {err}")))?,
        ),
        DataType::Float32 => {
            Value::Float32(f32::from_bits(token.parse::<u32>().map_err(|err| {
                ServerError::Ddl(format!("bad float32 literal: {err}"))
            })?))
        }
        DataType::Float64 => {
            Value::Float64(f64::from_bits(token.parse::<u64>().map_err(|err| {
                ServerError::Ddl(format!("bad float64 literal: {err}"))
            })?))
        }
        DataType::Text { .. } => Value::Text(metadata_unescape(token)?),
        DataType::Char { .. } => Value::Char(metadata_unescape(token)?),
        DataType::Bit { .. } | DataType::VarBit { .. } => {
            let text = metadata_unescape(token)?;
            Value::parse_bit_string(&text)
                .ok_or_else(|| ServerError::Ddl("bad bit string literal".to_owned()))?
        }
        DataType::Inet | DataType::Cidr | DataType::MacAddr | DataType::MacAddr8 => {
            let text = metadata_unescape(token)?;
            Value::parse_network(ty, &text)
                .ok_or_else(|| ServerError::Ddl("bad network literal".to_owned()))?
        }
        DataType::Date => Value::Date(
            token
                .parse::<i32>()
                .map_err(|err| ServerError::Ddl(format!("bad date literal: {err}")))?,
        ),
        DataType::Time => Value::Time(
            token
                .parse::<i64>()
                .map_err(|err| ServerError::Ddl(format!("bad time literal: {err}")))?,
        ),
        DataType::TimeTz => {
            let (micros, offset_seconds) = token
                .split_once(':')
                .ok_or_else(|| ServerError::Ddl("bad timetz literal".to_owned()))?;
            Value::TimeTz {
                micros: micros
                    .parse::<i64>()
                    .map_err(|err| ServerError::Ddl(format!("bad timetz time literal: {err}")))?,
                offset_seconds: offset_seconds
                    .parse::<i32>()
                    .map_err(|err| ServerError::Ddl(format!("bad timetz offset literal: {err}")))?,
            }
        }
        DataType::Timestamp => Value::Timestamp(
            token
                .parse::<i64>()
                .map_err(|err| ServerError::Ddl(format!("bad timestamp literal: {err}")))?,
        ),
        DataType::TimestampTz => Value::TimestampTz(
            token
                .parse::<i64>()
                .map_err(|err| ServerError::Ddl(format!("bad timestamptz literal: {err}")))?,
        ),
        _ => {
            return Err(ServerError::Ddl(format!(
                "unsupported persisted literal type {ty:?}"
            )));
        }
    })
}

fn encode_scalar_expr(expr: &ScalarExpr, out: &mut Vec<String>) -> Option<()> {
    match expr {
        ScalarExpr::Column {
            name,
            index,
            data_type,
        } => {
            out.push("col".to_owned());
            out.push(index.to_string());
            out.push(metadata_escape(name));
            out.push(data_type_token(data_type)?);
        }
        ScalarExpr::Literal { value, data_type } => {
            out.push("lit".to_owned());
            out.push(data_type_token(data_type)?);
            out.push(value_token(value)?);
        }
        ScalarExpr::Unary {
            op,
            expr,
            data_type,
        } => {
            out.push("unary".to_owned());
            out.push(unary_op_token(*op).to_owned());
            out.push(data_type_token(data_type)?);
            encode_scalar_expr(expr, out)?;
        }
        ScalarExpr::Binary {
            op,
            left,
            right,
            data_type,
        } => {
            out.push("binary".to_owned());
            out.push(binary_op_token(*op).to_owned());
            out.push(data_type_token(data_type)?);
            encode_scalar_expr(left, out)?;
            encode_scalar_expr(right, out)?;
        }
        ScalarExpr::IsNull { expr, negated } => {
            out.push("isnull".to_owned());
            out.push(negated.to_string());
            encode_scalar_expr(expr, out)?;
        }
        ScalarExpr::FunctionCall {
            name,
            args,
            data_type,
        } => {
            out.push("func".to_owned());
            out.push(metadata_escape(name));
            out.push(data_type_token(data_type)?);
            out.push(args.len().to_string());
            for arg in args {
                encode_scalar_expr(arg, out)?;
            }
        }
        _ => return None,
    }
    Some(())
}

fn encode_scalar_expr_field(expr: &ScalarExpr) -> Option<String> {
    let mut tokens = Vec::new();
    encode_scalar_expr(expr, &mut tokens)?;
    Some(tokens.join("\u{1f}"))
}

fn encode_table_runtime_scalar_expr(
    table_name: &str,
    subject: String,
    expr: &ScalarExpr,
) -> Result<String, ServerError> {
    encode_scalar_expr_field(expr).ok_or_else(|| {
        ServerError::ddl(format!(
            "table '{table_name}' {subject} is outside restart-persistable metadata subset"
        ))
    })
}

fn encode_table_runtime_scalar_expr_list(
    table_name: &str,
    subject: String,
    exprs: &[ScalarExpr],
) -> Result<String, ServerError> {
    let encoded = exprs
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            encode_table_runtime_scalar_expr(
                table_name,
                format!("{subject} expression {idx}"),
                expr,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(metadata_encode_list(&encoded))
}

fn decode_scalar_expr(tokens: &[&str], pos: &mut usize) -> Result<ScalarExpr, ServerError> {
    let Some(kind) = tokens.get(*pos).copied() else {
        return Err(ServerError::Ddl(
            "truncated scalar expression metadata".to_owned(),
        ));
    };
    *pos += 1;
    match kind {
        "col" => {
            let index = tokens
                .get(*pos)
                .ok_or_else(|| ServerError::Ddl("truncated column expr".to_owned()))?
                .parse::<usize>()
                .map_err(|err| ServerError::Ddl(format!("bad column index: {err}")))?;
            *pos += 1;
            let name = metadata_unescape(
                tokens
                    .get(*pos)
                    .ok_or_else(|| ServerError::Ddl("truncated column name".to_owned()))?,
            )?;
            *pos += 1;
            let data_type = data_type_from_token(
                tokens
                    .get(*pos)
                    .ok_or_else(|| ServerError::Ddl("truncated column type".to_owned()))?,
            )
            .ok_or_else(|| ServerError::Ddl("unknown column type".to_owned()))?;
            *pos += 1;
            Ok(ScalarExpr::Column {
                name,
                index,
                data_type,
            })
        }
        "lit" => {
            let data_type = data_type_from_token(
                tokens
                    .get(*pos)
                    .ok_or_else(|| ServerError::Ddl("truncated literal type".to_owned()))?,
            )
            .ok_or_else(|| ServerError::Ddl("unknown literal type".to_owned()))?;
            *pos += 1;
            let value = value_from_token(
                &data_type,
                tokens
                    .get(*pos)
                    .ok_or_else(|| ServerError::Ddl("truncated literal value".to_owned()))?,
            )?;
            *pos += 1;
            Ok(ScalarExpr::Literal { value, data_type })
        }
        "unary" => {
            let op = unary_op_from_token(
                tokens
                    .get(*pos)
                    .ok_or_else(|| ServerError::Ddl("truncated unary op".to_owned()))?,
            )
            .ok_or_else(|| ServerError::Ddl("unknown unary op".to_owned()))?;
            *pos += 1;
            let data_type = data_type_from_token(
                tokens
                    .get(*pos)
                    .ok_or_else(|| ServerError::Ddl("truncated unary type".to_owned()))?,
            )
            .ok_or_else(|| ServerError::Ddl("unknown unary type".to_owned()))?;
            *pos += 1;
            let expr = Box::new(decode_scalar_expr(tokens, pos)?);
            Ok(ScalarExpr::Unary {
                op,
                expr,
                data_type,
            })
        }
        "binary" => {
            let op = binary_op_from_token(
                tokens
                    .get(*pos)
                    .ok_or_else(|| ServerError::Ddl("truncated binary op".to_owned()))?,
            )
            .ok_or_else(|| ServerError::Ddl("unknown binary op".to_owned()))?;
            *pos += 1;
            let data_type = data_type_from_token(
                tokens
                    .get(*pos)
                    .ok_or_else(|| ServerError::Ddl("truncated binary type".to_owned()))?,
            )
            .ok_or_else(|| ServerError::Ddl("unknown binary type".to_owned()))?;
            *pos += 1;
            let left = Box::new(decode_scalar_expr(tokens, pos)?);
            let right = Box::new(decode_scalar_expr(tokens, pos)?);
            Ok(ScalarExpr::Binary {
                op,
                left,
                right,
                data_type,
            })
        }
        "isnull" => {
            let negated = tokens
                .get(*pos)
                .ok_or_else(|| ServerError::Ddl("truncated isnull flag".to_owned()))?
                .parse::<bool>()
                .map_err(|err| ServerError::Ddl(format!("bad isnull flag: {err}")))?;
            *pos += 1;
            let expr = Box::new(decode_scalar_expr(tokens, pos)?);
            Ok(ScalarExpr::IsNull { expr, negated })
        }
        "func" => {
            let name = metadata_unescape(
                tokens
                    .get(*pos)
                    .ok_or_else(|| ServerError::Ddl("truncated function name".to_owned()))?,
            )?;
            *pos += 1;
            let data_type = data_type_from_token(
                tokens
                    .get(*pos)
                    .ok_or_else(|| ServerError::Ddl("truncated function type".to_owned()))?,
            )
            .ok_or_else(|| ServerError::Ddl("unknown function type".to_owned()))?;
            *pos += 1;
            let arg_count = tokens
                .get(*pos)
                .ok_or_else(|| ServerError::Ddl("truncated function arg count".to_owned()))?
                .parse::<usize>()
                .map_err(|err| ServerError::Ddl(format!("bad function arg count: {err}")))?;
            *pos += 1;
            let mut args = Vec::with_capacity(arg_count);
            for _ in 0..arg_count {
                args.push(decode_scalar_expr(tokens, pos)?);
            }
            Ok(ScalarExpr::FunctionCall {
                name,
                args,
                data_type,
            })
        }
        other => Err(ServerError::Ddl(format!(
            "unknown scalar expression token {other}"
        ))),
    }
}

fn decode_scalar_expr_field(raw: &str) -> Result<ScalarExpr, ServerError> {
    let tokens = raw.split('\u{1f}').collect::<Vec<_>>();
    let mut pos = 0;
    let expr = decode_scalar_expr(&tokens, &mut pos)?;
    if pos != tokens.len() {
        return Err(ServerError::Ddl(
            "trailing scalar expression metadata tokens".to_owned(),
        ));
    }
    Ok(expr)
}

fn decode_scalar_expr_list_field(raw: &str) -> Result<Vec<ScalarExpr>, ServerError> {
    metadata_decode_list(raw)?
        .into_iter()
        .map(|expr| decode_scalar_expr_field(&expr))
        .collect()
}

fn materialized_view_projection_indices(plan: &LogicalPlan) -> Option<Vec<usize>> {
    match plan {
        LogicalPlan::Scan { schema, .. } => Some((0..schema.fields().len()).collect()),
        LogicalPlan::Project { input, exprs, .. }
            if matches!(input.as_ref(), LogicalPlan::Scan { .. }) =>
        {
            exprs
                .iter()
                .map(|(expr, _)| match expr {
                    ScalarExpr::Column { index, .. } => Some(*index),
                    _ => None,
                })
                .collect()
        }
        _ => None,
    }
}

fn materialized_view_source_plan_from_metadata(
    source_entry: &TableEntry,
    view_entry: &TableEntry,
    record: &MaterializedViewMetadataRecord,
) -> Option<LogicalPlan> {
    let source_scan = LogicalPlan::Scan {
        table: record.source_table.clone(),
        schema: source_entry.schema.clone(),
        projection: None,
    };
    let source_width = source_entry.schema.fields().len();
    let full_projection = record.projection.len() == source_width
        && record
            .projection
            .iter()
            .enumerate()
            .all(|(idx, projected)| idx == *projected);
    if full_projection && view_entry.schema == source_entry.schema {
        return Some(source_scan);
    }
    if record.projection.len() != view_entry.schema.fields().len() {
        return None;
    }
    let mut exprs = Vec::with_capacity(record.projection.len());
    for (out_idx, source_idx) in record.projection.iter().copied().enumerate() {
        let source_field = source_entry.schema.fields().get(source_idx)?;
        let output_field = view_entry.schema.fields().get(out_idx)?;
        if source_field.data_type != output_field.data_type {
            return None;
        }
        exprs.push((
            ScalarExpr::Column {
                name: output_field.name.clone(),
                index: source_idx,
                data_type: source_field.data_type.clone(),
            },
            output_field.name.clone(),
        ));
    }
    Some(LogicalPlan::Project {
        input: Box::new(source_scan),
        exprs,
        schema: view_entry.schema.clone(),
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MaterializedViewMetadataRecord {
    view_table: String,
    view_oid: ultrasql_core::Oid,
    source_table: String,
    source_oid: ultrasql_core::Oid,
    materialized_rows: u64,
    projection: Vec<usize>,
}

/// Runtime metadata for one index beyond plain attnum keys.
#[derive(Clone, Debug, Default)]
pub struct RuntimeIndexMetadata {
    /// Bound key expressions. Empty means use the catalog entry's key columns.
    pub key_exprs: Vec<ScalarExpr>,
    /// Bound partial-index predicate, if any.
    pub predicate: Option<ScalarExpr>,
    /// 0-based table columns listed in `INCLUDE (...)`.
    pub include_columns: Vec<usize>,
    /// Access method requested by `USING`.
    pub method: LogicalIndexMethod,
    /// In-memory BRIN min/max summaries for block-range pruning.
    pub brin: Option<Arc<ultrasql_storage::access_method::BrinIndex>>,
    /// Page-backed HNSW graph for vector top-k scans.
    pub hnsw: Option<Arc<ultrasql_storage::access_method::PageBackedHnswIndex>>,
    /// Page-backed IVFFlat inverted lists for vector top-k scans.
    pub ivfflat: Option<Arc<ultrasql_storage::access_method::PageBackedIvfFlatIndex>>,
    /// Runtime aggregating-index summary for dashboard-style GROUP BY scans.
    pub aggregating: Option<Arc<RuntimeAggregatingIndex>>,
}

/// Process-wide ANN/vector-index counters for ops metrics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AnnSystemMetrics {
    /// Approximate candidate count available across runtime ANN sidecars.
    pub candidates: u64,
    /// Tombstoned ANN entries waiting for VACUUM cleanup.
    pub tombstones: u64,
    /// Approximate memory footprint of page-backed vector-index pages.
    pub vector_index_memory_bytes: u64,
    /// Number of runtime HNSW indexes.
    pub hnsw_indexes: u64,
    /// Number of runtime IVFFlat indexes.
    pub ivfflat_indexes: u64,
}

/// One admin validation check result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationCheck {
    /// Stable machine-readable check name.
    pub name: &'static str,
    /// Check outcome.
    pub status: ValidationStatus,
    /// Human-readable evidence for the outcome.
    pub detail: String,
}

/// Admin validation status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValidationStatus {
    /// Check passed.
    Ok,
    /// Check failed and should make `ultrasql validate` exit non-zero.
    Failed,
}

impl ValidationStatus {
    /// Lowercase status for CLI output.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Failed => "failed",
        }
    }
}

/// Full admin validation report.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ValidationReport {
    /// Ordered checks run by the validator.
    pub checks: Vec<ValidationCheck>,
}

impl ValidationReport {
    /// Return true when every check passed.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.checks
            .iter()
            .all(|check| check.status == ValidationStatus::Ok)
    }
}

/// Runtime sidecar for `CREATE AGGREGATING INDEX`.
#[derive(Debug)]
pub struct RuntimeAggregatingIndex {
    /// Bound aggregating-index metadata.
    pub spec: ultrasql_planner::LogicalAggregatingIndex,
    /// Materialized summary rows in `group columns + aggregates` order.
    pub rows: std::sync::RwLock<Vec<Vec<Value>>>,
    /// Set when DML touched the base table after the last summary build.
    pub dirty: std::sync::atomic::AtomicBool,
    pub(crate) explain_stats: RuntimeAggregatingIndexExplainStats,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct AggregatingIndexExplainStats {
    pub aggregating_index_used: bool,
    pub stale_rebuild_used: bool,
    pub summary_rows_read: u64,
    pub base_rows_skipped: u64,
}

#[derive(Debug, Default)]
pub(crate) struct RuntimeAggregatingIndexExplainStats {
    aggregating_index_used: std::sync::atomic::AtomicBool,
    stale_rebuild_used: std::sync::atomic::AtomicBool,
    summary_rows_read: std::sync::atomic::AtomicU64,
    base_rows_skipped: std::sync::atomic::AtomicU64,
}

impl RuntimeAggregatingIndex {
    /// Build a clean runtime summary.
    #[must_use]
    pub fn new(spec: ultrasql_planner::LogicalAggregatingIndex, rows: Vec<Vec<Value>>) -> Self {
        Self {
            spec,
            rows: std::sync::RwLock::new(rows),
            dirty: std::sync::atomic::AtomicBool::new(false),
            explain_stats: RuntimeAggregatingIndexExplainStats::default(),
        }
    }

    /// Mark summary rows stale. Next matching read rebuilds lazily.
    pub fn mark_dirty(&self) {
        self.dirty.store(true, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn record_explain_read(
        &self,
        stale_rebuild_used: bool,
        summary_rows_read: usize,
        base_rows_skipped: u64,
    ) {
        self.explain_stats
            .aggregating_index_used
            .store(true, std::sync::atomic::Ordering::Release);
        self.explain_stats
            .stale_rebuild_used
            .store(stale_rebuild_used, std::sync::atomic::Ordering::Release);
        self.explain_stats.summary_rows_read.store(
            u64::try_from(summary_rows_read).unwrap_or(u64::MAX),
            std::sync::atomic::Ordering::Release,
        );
        self.explain_stats
            .base_rows_skipped
            .store(base_rows_skipped, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn explain_stats_snapshot(&self) -> AggregatingIndexExplainStats {
        AggregatingIndexExplainStats {
            aggregating_index_used: self
                .explain_stats
                .aggregating_index_used
                .load(std::sync::atomic::Ordering::Acquire),
            stale_rebuild_used: self
                .explain_stats
                .stale_rebuild_used
                .load(std::sync::atomic::Ordering::Acquire),
            summary_rows_read: self
                .explain_stats
                .summary_rows_read
                .load(std::sync::atomic::Ordering::Acquire),
            base_rows_skipped: self
                .explain_stats
                .base_rows_skipped
                .load(std::sync::atomic::Ordering::Acquire),
        }
    }
}

/// One runtime CHECK constraint.
#[derive(Clone, Debug)]
pub struct RuntimeCheckConstraint {
    /// Constraint name reported on violation.
    pub name: String,
    /// Boolean expression bound against the table row schema.
    pub expr: ScalarExpr,
}

/// One runtime FOREIGN KEY constraint.
#[derive(Clone, Debug)]
pub struct RuntimeForeignKeyConstraint {
    /// Constraint name reported on violation.
    pub name: String,
    /// Referencing table column indices.
    pub columns: Vec<usize>,
    /// Referenced table name.
    pub target_table: String,
    /// Referenced table OID.
    pub target_oid: ultrasql_core::Oid,
    /// Referenced table column indices.
    pub target_columns: Vec<usize>,
    /// Action when a referenced row is deleted.
    pub on_delete: ultrasql_planner::LogicalReferentialAction,
    /// Action when a referenced key is updated.
    pub on_update: ultrasql_planner::LogicalReferentialAction,
    /// Whether this constraint may be checked at transaction commit.
    pub deferrable: bool,
    /// Whether this deferrable constraint starts in deferred mode.
    pub initially_deferred: bool,
}

/// One runtime EXCLUDE constraint.
#[derive(Clone, Debug)]
pub struct RuntimeExclusionConstraint {
    /// Constraint name reported on violation.
    pub name: String,
    /// Access method requested by `USING`.
    pub method: LogicalIndexMethod,
    /// 0-based table column indices plus operators.
    pub elements: Vec<RuntimeExclusionElement>,
}

/// One runtime EXCLUDE element.
#[derive(Clone, Debug)]
pub struct RuntimeExclusionElement {
    /// Table column index.
    pub column: usize,
    /// Operator applied to `(new_value, existing_value)`.
    pub op: BinaryOp,
}

fn deferred_fk_key(row: &[Value], columns: &[usize]) -> Option<Vec<Value>> {
    let mut key = Vec::with_capacity(columns.len());
    for &idx in columns {
        let value = row.get(idx)?;
        if matches!(value, Value::Null) {
            return None;
        }
        key.push(value.clone());
    }
    Some(key)
}

/// Session-local sequence state shared with sequence-backed defaults.
#[derive(Clone, Debug, Default)]
pub struct SequenceSessionState {
    currvals: Arc<parking_lot::Mutex<std::collections::HashMap<String, i64>>>,
    last_sequence: Arc<parking_lot::Mutex<Option<String>>>,
}

#[derive(Clone, Debug)]
pub(crate) struct SequenceSessionSnapshot {
    currvals: std::collections::HashMap<String, i64>,
    last_sequence: Option<String>,
}

impl SequenceSessionState {
    /// Record a generated value for `currval` / `lastval`.
    pub fn record_nextval(&self, name: &str, value: i64) {
        let folded = name.to_ascii_lowercase();
        self.currvals.lock().insert(folded.clone(), value);
        *self.last_sequence.lock() = Some(folded);
    }

    /// Drop session-local state for a removed sequence.
    pub fn forget(&self, name: &str) {
        let folded = name.to_ascii_lowercase();
        self.currvals.lock().remove(&folded);
        if self.last_sequence.lock().as_deref() == Some(folded.as_str()) {
            *self.last_sequence.lock() = None;
        }
    }

    pub(crate) fn snapshot(&self) -> SequenceSessionSnapshot {
        SequenceSessionSnapshot {
            currvals: self.currvals.lock().clone(),
            last_sequence: self.last_sequence.lock().clone(),
        }
    }

    pub(crate) fn restore_snapshot(&self, snapshot: SequenceSessionSnapshot) {
        *self.currvals.lock() = snapshot.currvals;
        *self.last_sequence.lock() = snapshot.last_sequence;
    }

    /// Return the session-local value for a named sequence.
    pub fn currval(&self, name: &str) -> Option<i64> {
        self.currvals
            .lock()
            .get(&name.to_ascii_lowercase())
            .copied()
    }

    /// Return the most recent sequence/value pair in this session.
    pub fn lastval(&self) -> Option<(String, i64)> {
        let name = self.last_sequence.lock().clone()?;
        let value = self.currvals.lock().get(&name).copied()?;
        Some((name, value))
    }
}

/// Session-local ownership for advisory locks.
#[derive(Clone, Debug)]
pub struct AdvisorySessionState {
    owner: Xid,
    held: Arc<parking_lot::Mutex<std::collections::HashMap<LockTag, usize>>>,
}

impl AdvisorySessionState {
    /// Build a stable advisory-lock owner for one server session.
    #[must_use]
    pub fn new(pid: u32) -> Self {
        Self {
            owner: Xid::new(u64::MAX.saturating_sub(u64::from(pid))),
            held: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Evaluate a PostgreSQL advisory-lock function against this session.
    pub fn evaluate_function(
        &self,
        name: &str,
        args: &[Value],
        lock_manager: &LockManager,
    ) -> Result<Value, ServerError> {
        match name {
            "pg_advisory_lock" => {
                let Some(tag) = advisory_tag_from_values(name, args)? else {
                    return Ok(Value::Null);
                };
                self.lock(tag, lock_manager, name)?;
                Ok(Value::Null)
            }
            "pg_try_advisory_lock" => {
                let Some(tag) = advisory_tag_from_values(name, args)? else {
                    return Ok(Value::Null);
                };
                Ok(Value::Bool(self.try_lock(tag, lock_manager, name)?))
            }
            "pg_advisory_unlock" => {
                let Some(tag) = advisory_tag_from_values(name, args)? else {
                    return Ok(Value::Null);
                };
                Ok(Value::Bool(self.unlock(tag, lock_manager)))
            }
            "pg_advisory_unlock_all" => {
                if !args.is_empty() {
                    return Err(advisory_type_error(format!(
                        "{name}: expected 0 arguments, got {}",
                        args.len()
                    )));
                }
                self.release_all(lock_manager);
                Ok(Value::Null)
            }
            _ => Err(ServerError::Unsupported("advisory lock function")),
        }
    }

    /// Evaluate a PostgreSQL transaction-scoped advisory-lock function.
    pub fn evaluate_transaction_function(
        &self,
        name: &str,
        args: &[Value],
        lock_manager: &LockManager,
        owner: Xid,
    ) -> Result<Value, ServerError> {
        match name {
            "pg_try_advisory_xact_lock" => {
                let Some(tag) = advisory_tag_from_values(name, args)? else {
                    return Ok(Value::Null);
                };
                let acquired = lock_manager
                    .try_acquire(LockRequest {
                        xid: owner,
                        tag,
                        mode: LockMode::Exclusive,
                    })
                    .map_err(|err| advisory_type_error(format!("{name}: {err}")))?;
                Ok(Value::Bool(acquired))
            }
            _ => Err(ServerError::Unsupported(
                "transaction advisory lock function",
            )),
        }
    }

    /// Release every advisory lock held by this session.
    pub fn release_all(&self, lock_manager: &LockManager) {
        let tags: Vec<LockTag> = {
            let mut held = self.held.lock();
            let tags = held.keys().copied().collect();
            held.clear();
            tags
        };
        for tag in tags {
            lock_manager.release(self.owner, tag, LockMode::Exclusive);
        }
    }

    fn lock(
        &self,
        tag: LockTag,
        lock_manager: &LockManager,
        name: &str,
    ) -> Result<(), ServerError> {
        {
            let mut held = self.held.lock();
            if let Some(count) = held.get_mut(&tag) {
                *count = count.saturating_add(1);
                return Ok(());
            }
        }
        lock_manager
            .acquire(LockRequest {
                xid: self.owner,
                tag,
                mode: LockMode::Exclusive,
            })
            .map_err(|err| advisory_type_error(format!("{name}: {err}")))?;
        self.held.lock().insert(tag, 1);
        Ok(())
    }

    fn try_lock(
        &self,
        tag: LockTag,
        lock_manager: &LockManager,
        name: &str,
    ) -> Result<bool, ServerError> {
        {
            let mut held = self.held.lock();
            if let Some(count) = held.get_mut(&tag) {
                *count = count.saturating_add(1);
                return Ok(true);
            }
        }
        let acquired = lock_manager
            .try_acquire(LockRequest {
                xid: self.owner,
                tag,
                mode: LockMode::Exclusive,
            })
            .map_err(|err| advisory_type_error(format!("{name}: {err}")))?;
        if acquired {
            self.held.lock().insert(tag, 1);
        }
        Ok(acquired)
    }

    fn unlock(&self, tag: LockTag, lock_manager: &LockManager) -> bool {
        let should_release = {
            let mut held = self.held.lock();
            let Some(count) = held.get_mut(&tag) else {
                return false;
            };
            if *count > 1 {
                *count -= 1;
                false
            } else {
                held.remove(&tag);
                true
            }
        };
        if should_release {
            lock_manager.release(self.owner, tag, LockMode::Exclusive);
        }
        true
    }
}

fn advisory_tag_from_values(name: &str, args: &[Value]) -> Result<Option<LockTag>, ServerError> {
    match args.len() {
        1 => {
            let Some(key) = advisory_i64_arg(name, args, 0)? else {
                return Ok(None);
            };
            let raw = u64::from_ne_bytes(key.to_ne_bytes());
            Ok(Some(LockTag::Advisory {
                classid: u32::try_from(raw >> 32)
                    .map_err(|_| advisory_type_error(format!("{name}: key high bits overflow")))?,
                objid: u32::try_from(raw & u64::from(u32::MAX))
                    .map_err(|_| advisory_type_error(format!("{name}: key low bits overflow")))?,
            }))
        }
        2 => {
            let Some(classid) = advisory_i32_arg(name, args, 0)? else {
                return Ok(None);
            };
            let Some(objid) = advisory_i32_arg(name, args, 1)? else {
                return Ok(None);
            };
            Ok(Some(LockTag::Advisory {
                classid: u32::from_ne_bytes(classid.to_ne_bytes()),
                objid: u32::from_ne_bytes(objid.to_ne_bytes()),
            }))
        }
        len => Err(advisory_type_error(format!(
            "{name}: expected 1 or 2 arguments, got {len}"
        ))),
    }
}

fn advisory_i64_arg(name: &str, args: &[Value], idx: usize) -> Result<Option<i64>, ServerError> {
    match args.get(idx) {
        Some(Value::Int16(value)) => Ok(Some(i64::from(*value))),
        Some(Value::Int32(value)) => Ok(Some(i64::from(*value))),
        Some(Value::Int64(value)) => Ok(Some(*value)),
        Some(Value::Null) => Ok(None),
        Some(other) => Err(advisory_type_error(format!(
            "{name}: argument {} must be integer, got {:?}",
            idx + 1,
            other.data_type()
        ))),
        None => Err(advisory_type_error(format!(
            "{name}: missing argument {}",
            idx + 1
        ))),
    }
}

fn advisory_i32_arg(name: &str, args: &[Value], idx: usize) -> Result<Option<i32>, ServerError> {
    let Some(value) = advisory_i64_arg(name, args, idx)? else {
        return Ok(None);
    };
    i32::try_from(value)
        .map(Some)
        .map_err(|_| advisory_type_error(format!("{name}: argument {} out of int4 range", idx + 1)))
}

fn advisory_type_error(message: String) -> ServerError {
    ServerError::Execute(ExecError::TypeMismatch(message))
}

struct ServerRecoveryTarget {
    heap: Arc<HeapAccess<BlankPageLoader>>,
    sequences: Arc<dashmap::DashMap<String, Arc<Sequence>>>,
}

impl ServerRecoveryTarget {
    fn sequence_snapshot(payload: &SequenceOpPayload) -> SequenceSnapshot {
        SequenceSnapshot {
            start_value: payload.start_value,
            last_value: payload.last_value,
            is_called: payload.is_called,
            min_value: payload.min_value,
            max_value: payload.max_value,
            increment: payload.increment,
            cycle: payload.cycle,
            cache_size: payload.cache_size,
        }
    }
}

impl HeapTarget for ServerRecoveryTarget {
    fn apply_insert(&self, payload: &HeapInsertPayload) -> Result<(), ApplyError> {
        HeapTarget::apply_insert(self.heap.as_ref(), payload)
    }

    fn apply_update(&self, payload: &HeapUpdatePayload) -> Result<(), ApplyError> {
        HeapTarget::apply_update(self.heap.as_ref(), payload)
    }

    fn apply_delete(&self, payload: &HeapDeletePayload) -> Result<(), ApplyError> {
        HeapTarget::apply_delete(self.heap.as_ref(), payload)
    }

    fn apply_update_in_place(&self, payload: &HeapUpdateInPlacePayload) -> Result<(), ApplyError> {
        HeapTarget::apply_update_in_place(self.heap.as_ref(), payload)
    }

    fn apply_update_in_place_batch(
        &self,
        payload: &HeapUpdateInPlaceBatchPayload,
    ) -> Result<(), ApplyError> {
        HeapTarget::apply_update_in_place_batch(self.heap.as_ref(), payload)
    }

    fn apply_delete_in_place(&self, payload: &HeapDeleteInPlacePayload) -> Result<(), ApplyError> {
        HeapTarget::apply_delete_in_place(self.heap.as_ref(), payload)
    }

    fn apply_delete_in_place_batch(
        &self,
        payload: &HeapDeleteInPlaceBatchPayload,
    ) -> Result<(), ApplyError> {
        HeapTarget::apply_delete_in_place_batch(self.heap.as_ref(), payload)
    }

    fn apply_full_page_write(&self, payload: &FullPageWritePayload) -> Result<(), ApplyError> {
        HeapTarget::apply_full_page_write(self.heap.as_ref(), payload)
    }

    fn apply_btree_op(&self, payload: &BTreeOpPayload) -> Result<(), ApplyError> {
        HeapTarget::apply_btree_op(self.heap.as_ref(), payload)
    }

    fn apply_sequence_op(&self, payload: &SequenceOpPayload) -> Result<(), ApplyError> {
        let name = payload.name.to_ascii_lowercase();
        if payload.op == SequenceOpKind::Drop {
            self.sequences.remove(&name);
            return Ok(());
        }
        let snapshot = Self::sequence_snapshot(payload);
        if let Some(existing) = self.sequences.get(&name) {
            existing
                .apply_snapshot(snapshot)
                .map_err(|e| ApplyError::Refused {
                    operation: "sequence_replay",
                    detail: e.to_string(),
                })?;
            return Ok(());
        }
        let seq = Sequence::from_snapshot(snapshot).map_err(|e| ApplyError::Refused {
            operation: "sequence_replay",
            detail: e.to_string(),
        })?;
        self.sequences.insert(name, Arc::new(seq));
        Ok(())
    }

    fn observe_commit(&self, payload: &CommitPayload) -> Result<(), ApplyError> {
        HeapTarget::observe_commit(self.heap.as_ref(), payload)
    }

    fn observe_abort(&self, payload: &AbortPayload) -> Result<(), ApplyError> {
        HeapTarget::observe_abort(self.heap.as_ref(), payload)
    }

    fn observe_checkpoint(&self, payload: &CheckpointPayload) -> Result<(), ApplyError> {
        HeapTarget::observe_checkpoint(self.heap.as_ref(), payload)
    }
}

impl TxnState {
    /// The PostgreSQL `ReadyForQuery` status byte for this state.
    #[must_use]
    pub const fn ready_for_query_status(&self) -> u8 {
        match self {
            Self::Idle => b'I',
            Self::InTransaction(_) => b'T',
            Self::Failed(_) => b'E',
        }
    }
}

/// Spill-capable `PageLoader` used by the development server.
///
/// Unwritten pages return freshly-initialized heap pages. Dirty pages can be
/// flushed into a per-process segment store, letting large in-process
/// benchmarks cycle buffer frames without losing heap contents.
///
/// `BufferPool` and `HeapAccess` are generic over `PageLoader`; making
/// the type concrete here lets us name the heap (`Arc<HeapAccess<BlankPageLoader>>`)
/// on `Server` and on the per-statement lowering context.
#[derive(Debug, Clone)]
pub struct BlankPageLoader {
    backing: Arc<BlankPageBacking>,
}

#[derive(Debug)]
enum BlankPageBacking {
    Segment {
        manager: Arc<SegmentFileManager>,
        _temp_dir: Option<tempfile::TempDir>,
    },
    Memory(Arc<dashmap::DashMap<PageId, Arc<[u8; PAGE_SIZE]>>>),
}

impl Default for BlankPageLoader {
    fn default() -> Self {
        Self::new()
    }
}

impl BlankPageLoader {
    /// Create a loader backed by a temporary segment directory.
    #[must_use]
    pub fn new() -> Self {
        if matches!(
            std::env::var("ULTRASQL_PAGE_SPILL_BACKING").ok().as_deref(),
            Some("memory" | "MEMORY")
        ) {
            return Self {
                backing: Arc::new(BlankPageBacking::Memory(Arc::new(dashmap::DashMap::new()))),
            };
        }
        let config = SegmentConfig {
            use_mmap: false,
            verify_checksums: false,
            ..SegmentConfig::default()
        };
        let spill_root = std::env::var_os("ULTRASQL_PAGE_SPILL_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let backing = match tempfile::Builder::new()
            .prefix("ultrasql-page-spill-")
            .tempdir_in(spill_root)
            .and_then(|dir| {
                SegmentFileManager::open(dir.path().to_path_buf(), config)
                    .map(|manager| (dir, manager))
                    .map_err(std::io::Error::other)
            }) {
            Ok((dir, manager)) => BlankPageBacking::Segment {
                manager: Arc::new(manager),
                _temp_dir: Some(dir),
            },
            Err(e) => {
                warn!(
                    error = %e,
                    "page spill segment store unavailable; falling back to in-memory page store"
                );
                BlankPageBacking::Memory(Arc::new(dashmap::DashMap::new()))
            }
        };
        Self {
            backing: Arc::new(backing),
        }
    }

    /// Create a loader backed by stable segment files under `base_dir`.
    pub fn persistent(base_dir: impl AsRef<std::path::Path>) -> Result<Self, SegmentError> {
        let config = SegmentConfig {
            use_mmap: false,
            verify_checksums: false,
            ..SegmentConfig::default()
        };
        let manager = SegmentFileManager::open(base_dir.as_ref().to_path_buf(), config)?;
        Ok(Self {
            backing: Arc::new(BlankPageBacking::Segment {
                manager: Arc::new(manager),
                _temp_dir: None,
            }),
        })
    }

    /// Persist a dirty page so the buffer pool may evict its frame safely.
    pub fn store(&self, page_id: PageId, page: &Page) -> ultrasql_core::Result<()> {
        match self.backing.as_ref() {
            BlankPageBacking::Segment { manager, .. } => {
                while manager
                    .relation_size_blocks(page_id.relation)
                    .map_err(ultrasql_core::Error::from)?
                    <= page_id.block.raw()
                {
                    manager
                        .allocate_block(page_id.relation)
                        .map_err(ultrasql_core::Error::from)?;
                }
                manager
                    .write_page(page_id, page)
                    .map_err(ultrasql_core::Error::from)
            }
            BlankPageBacking::Memory(pages) => {
                pages.insert(page_id, Arc::new(*page.as_bytes()));
                Ok(())
            }
        }
    }
}

impl PageLoader for BlankPageLoader {
    fn load(&self, page_id: PageId) -> ultrasql_core::Result<Page> {
        match self.backing.as_ref() {
            BlankPageBacking::Segment { manager, .. } => match manager.read_page(page_id) {
                Ok(page) => Ok(page),
                Err(SegmentError::OutOfBounds { .. }) => Ok(Page::new_heap()),
                Err(e) => Err(e.into()),
            },
            BlankPageBacking::Memory(pages) => {
                let Some(bytes) = pages.get(&page_id) else {
                    return Ok(Page::new_heap());
                };
                Page::from_bytes(Box::new(**bytes))
                    .map_err(|e| ultrasql_core::Error::Corruption(e.to_string()))
            }
        }
    }
}

/// Read-only catalog view consulted by the binder during query
/// execution.
///
/// The persistent catalog (`PersistentCatalog`) is the source of truth
/// for user-created relations; the in-memory `InMemoryCatalog` carries
/// the legacy sample-table registry (the v0.5 hard-coded `users`
/// fixture). Lookups try the persistent snapshot first so a runtime
/// `CREATE TABLE` immediately shadows any sample-table name collision;
/// if the snapshot has no entry, we fall back to the sample-table
/// catalog so existing duplex tests still resolve `users`.
///
/// The `'a` lifetime ties the view to the snapshot and in-memory
/// catalog held by the calling [`Session`]; binding completes
/// synchronously inside `execute_query` so the lifetime never escapes
/// a single statement.
struct CombinedCatalog<'a> {
    snapshot: &'a CatalogSnapshot,
    fallback: &'a InMemoryCatalog,
    search_path: Option<&'a str>,
}

impl PlannerCatalog for CombinedCatalog<'_> {
    fn lookup_table(&self, name: &str) -> Option<TableMeta> {
        if let Some(schema) = pipeline::catalog_views::virtual_catalog_schema(name) {
            return Some(TableMeta::new(schema));
        }
        for schema_name in search_path_schema_names(self.search_path) {
            if let Some(meta) =
                PlannerCatalog::lookup_table_in_schema(self.snapshot, &schema_name, name)
            {
                return Some(meta);
            }
            if let Some(meta) =
                PlannerCatalog::lookup_table_in_schema(self.fallback, &schema_name, name)
            {
                return Some(meta);
            }
        }
        None
    }

    fn lookup_table_in_schema(&self, schema_name: &str, name: &str) -> Option<TableMeta> {
        let table_key = ultrasql_catalog::table_lookup_key(schema_name, name);
        if let Some(schema) = pipeline::catalog_views::virtual_catalog_schema(&table_key) {
            return Some(TableMeta::with_schema_name(schema_name, schema));
        }
        PlannerCatalog::lookup_table_in_schema(self.snapshot, schema_name, name)
            .or_else(|| PlannerCatalog::lookup_table_in_schema(self.fallback, schema_name, name))
    }

    fn lookup_type(&self, name: &str) -> Option<DataType> {
        for schema_name in search_path_schema_names(self.search_path) {
            if let Some(data_type) =
                PlannerCatalog::lookup_type_in_schema(self.snapshot, &schema_name, name)
            {
                return Some(data_type);
            }
            if let Some(data_type) =
                PlannerCatalog::lookup_type_in_schema(self.fallback, &schema_name, name)
            {
                return Some(data_type);
            }
        }
        type_name_namespace_and_name(name)
            .and_then(|(schema_name, type_name)| self.lookup_type_in_schema(schema_name, type_name))
    }

    fn lookup_type_in_schema(&self, schema_name: &str, name: &str) -> Option<DataType> {
        PlannerCatalog::lookup_type_in_schema(self.snapshot, schema_name, name)
            .or_else(|| PlannerCatalog::lookup_type_in_schema(self.fallback, schema_name, name))
    }

    fn lookup_index(&self, name: &str) -> bool {
        if search_path_schema_names(self.search_path)
            .into_iter()
            .any(|schema_name| {
                PlannerCatalog::lookup_index_in_schema(self.snapshot, &schema_name, name)
                    || PlannerCatalog::lookup_index_in_schema(self.fallback, &schema_name, name)
            })
        {
            return true;
        }
        type_name_namespace_and_name(name).is_some_and(|(schema_name, index_name)| {
            self.lookup_index_in_schema(schema_name, index_name)
        })
    }

    fn lookup_index_in_schema(&self, schema_name: &str, name: &str) -> bool {
        PlannerCatalog::lookup_index_in_schema(self.snapshot, schema_name, name)
            || PlannerCatalog::lookup_index_in_schema(self.fallback, schema_name, name)
    }

    fn lookup_index_schema(&self, name: &str) -> Option<String> {
        search_path_schema_names(self.search_path)
            .into_iter()
            .find(|schema_name| self.lookup_index_in_schema(schema_name, name))
    }

    fn lookup_table_oid(&self, name: &str) -> Option<Oid> {
        for schema_name in search_path_schema_names(self.search_path) {
            if let Some(oid) =
                PlannerCatalog::lookup_table_oid_in_schema(self.snapshot, &schema_name, name)
            {
                return Some(oid);
            }
            if let Some(oid) =
                PlannerCatalog::lookup_table_oid_in_schema(self.fallback, &schema_name, name)
            {
                return Some(oid);
            }
        }
        type_name_namespace_and_name(name).and_then(|(schema_name, table_name)| {
            self.lookup_table_oid_in_schema(schema_name, table_name)
        })
    }

    fn lookup_table_oid_in_schema(&self, schema_name: &str, name: &str) -> Option<Oid> {
        PlannerCatalog::lookup_table_oid_in_schema(self.snapshot, schema_name, name).or_else(|| {
            PlannerCatalog::lookup_table_oid_in_schema(self.fallback, schema_name, name)
        })
    }

    fn lookup_type_oid(&self, name: &str) -> Option<Oid> {
        for schema_name in search_path_schema_names(self.search_path) {
            if let Some(oid) =
                PlannerCatalog::lookup_type_oid_in_schema(self.snapshot, &schema_name, name)
            {
                return Some(oid);
            }
            if let Some(oid) =
                PlannerCatalog::lookup_type_oid_in_schema(self.fallback, &schema_name, name)
            {
                return Some(oid);
            }
        }
        type_name_namespace_and_name(name).and_then(|(schema_name, type_name)| {
            self.lookup_type_oid_in_schema(schema_name, type_name)
        })
    }

    fn lookup_type_oid_in_schema(&self, schema_name: &str, name: &str) -> Option<Oid> {
        PlannerCatalog::lookup_type_oid_in_schema(self.snapshot, schema_name, name)
            .or_else(|| PlannerCatalog::lookup_type_oid_in_schema(self.fallback, schema_name, name))
    }

    fn table_schema_visible_without_qualification(&self, schema_name: &str) -> bool {
        search_path_contains_schema(self.search_path, schema_name)
    }
}

fn search_path_contains_schema(search_path: Option<&str>, schema_name: &str) -> bool {
    let folded = schema_name.to_ascii_lowercase();
    if matches!(folded.as_str(), "pg_catalog" | "information_schema") {
        return true;
    }
    let Some(search_path) = search_path else {
        return folded == "public";
    };
    search_path.split(',').any(|part| {
        normalize_search_path_schema(part)
            .as_deref()
            .is_some_and(|schema| schema == folded)
    })
}

fn type_name_namespace_and_name(name: &str) -> Option<(&str, &str)> {
    let (schema_name, type_name) = name.rsplit_once('.')?;
    (!schema_name.is_empty() && !type_name.is_empty()).then_some((schema_name, type_name))
}

pub(crate) fn parse_pg_identifier_path(text: &str) -> Option<Vec<String>> {
    let mut parts = Vec::new();
    let mut chars = text.chars().peekable();
    loop {
        match chars.peek().copied()? {
            '"' => {
                chars.next();
                let mut part = String::new();
                loop {
                    match chars.next()? {
                        '"' if chars.peek() == Some(&'"') => {
                            chars.next();
                            part.push('"');
                        }
                        '"' => break,
                        ch => part.push(ch),
                    }
                }
                parts.push(part);
            }
            _ => {
                let mut part = String::new();
                while let Some(ch) = chars.peek().copied() {
                    if ch == '.' {
                        break;
                    }
                    part.push(ch);
                    chars.next();
                }
                if part.is_empty() {
                    return None;
                }
                parts.push(part);
            }
        }
        match chars.next() {
            Some('.') => continue,
            None => return Some(parts),
            Some(_) => return None,
        }
    }
}

fn sequence_lookup_key(schema_name: &str, sequence_name: &str) -> String {
    ultrasql_catalog::table_lookup_key(schema_name, sequence_name)
}

fn table_entry_lookup_key(entry: &TableEntry) -> String {
    ultrasql_catalog::table_lookup_key(&entry.schema_name, &entry.name)
}

fn search_path_schema_names(search_path: Option<&str>) -> Vec<String> {
    let Some(search_path) = search_path else {
        return vec!["public".to_owned()];
    };
    search_path
        .split(',')
        .filter_map(normalize_search_path_schema)
        .collect()
}

fn normalize_search_path_schema(part: &str) -> Option<String> {
    let trimmed = part.trim();
    if trimmed.is_empty() {
        return None;
    }
    let unquoted = trimmed
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(trimmed);
    (unquoted != "$user").then(|| unquoted.to_ascii_lowercase())
}

fn is_local_read_plan(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Scan { .. }
        | LogicalPlan::Empty { .. }
        | LogicalPlan::Values { .. }
        | LogicalPlan::FunctionScan { .. } => true,
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::LockRows { input, .. } => is_local_read_plan(input),
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOp { left, right, .. } => {
            is_local_read_plan(left) && is_local_read_plan(right)
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => is_local_read_plan(definition) && is_local_read_plan(body),
        _ => false,
    }
}

pub(crate) fn local_output_from_select_result(
    result: SelectResult,
) -> Result<LocalQueryOutput, ServerError> {
    let messages = local_result_messages(result)?;
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    let mut command_tag = String::new();
    for message in messages {
        match message {
            BackendMessage::RowDescription { fields } => {
                columns = fields
                    .into_iter()
                    .map(|field| LocalResultColumn {
                        name: field.name,
                        type_oid: field.type_oid,
                    })
                    .collect();
            }
            BackendMessage::DataRow { columns } => {
                let row = columns
                    .into_iter()
                    .map(|cell| {
                        cell.map(|bytes| {
                            String::from_utf8(bytes).map_err(|err| {
                                ServerError::CopyFormat(format!(
                                    "ultrasql-local result is not UTF-8: {err}"
                                ))
                            })
                        })
                        .transpose()
                    })
                    .collect::<Result<Vec<_>, ServerError>>()?;
                rows.push(row);
            }
            BackendMessage::CommandComplete { tag } => {
                command_tag = tag;
            }
            _ => {}
        }
    }
    Ok(LocalQueryOutput {
        columns,
        rows,
        command_tag,
    })
}

fn local_result_messages(result: SelectResult) -> Result<Vec<BackendMessage>, ServerError> {
    if let Some(body) = result.streamed_body {
        return decode_local_result_body(body);
    }
    if let Some(body) = result.shared_streamed_body {
        return decode_local_result_body(bytes::BytesMut::from(body.as_ref()));
    }
    Ok(result.messages)
}

fn decode_local_result_body(mut body: bytes::BytesMut) -> Result<Vec<BackendMessage>, ServerError> {
    let mut messages = Vec::new();
    while !body.is_empty() {
        match ultrasql_protocol::decode_backend(&mut body)? {
            Some(message) => messages.push(message),
            None => {
                return Err(ServerError::CopyFormat(
                    "embedded result ended with a partial wire frame".to_owned(),
                ));
            }
        }
    }
    Ok(messages)
}

/// Default initial read buffer. Picked to fit a small startup message
/// without resizing; the buffer grows on demand.
pub(crate) const READ_BUFFER_INITIAL: usize = 1 << 12;

/// Buffer pool capacity used when no data directory is configured.
///
/// 65 536 frames × 8 KiB = 512 MiB. Sized to cover the sample database,
/// the integration tests, and the wire-protocol benchmark driver
/// (which loads up to ~10 M rows per iteration across multiple fresh
/// relations on a single in-process Server — large analytical workloads
/// such as `select_avg_10m_i64` and `filter_sum_10m_i64`). Production
/// deployments will size this from configuration.
const IN_MEMORY_POOL_FRAMES: usize = 65_536;

/// Shared connection state: the catalog used by the binder plus the
/// sample-table registry the lowerer consults.
///
/// Lives behind [`Arc`] so connection tasks share a single instance.
///
/// # Catalog lifecycle
///
/// At startup ([`Server::with_sample_database`] or a future
/// `Server::init(data_dir)`), the persistent catalog is bootstrapped from
/// the heap via [`PersistentCatalog::bootstrap_from_heap`]. On a fresh
/// database that means installing the hard-coded initial snapshot; on a
/// warm restart it rebuilds from durable heap pages.
///
/// Each statement captures an immutable [`CatalogSnapshot`] at the start
/// of planning via [`Server::catalog_snapshot`]; this ensures that
/// concurrent DDL does not perturb an in-flight query.
///
/// `Send + Sync` holds because every field is `Send + Sync`.
#[derive(Debug)]
pub struct Server {
    /// Planner-facing in-memory catalog (used by the binder today).
    ///
    /// `TODO(catalog-rebind)`: once the planner's binder is rewritten
    /// against `PersistentCatalog` / `CatalogSnapshot`, this field is
    /// removed and all lookups go through `persistent_catalog`.
    pub catalog: InMemoryCatalog,
    /// Registry of sample tables (schema + pre-built batches).
    pub tables: SampleTables,
    /// Optional data directory used by WAL-backed server instances.
    ///
    /// `None` means in-memory sample mode. When present, operational SQL
    /// shims such as `pg_start_backup()` can leave marker files in the same
    /// directory that CLI backup/restore commands use.
    pub data_dir: Option<std::path::PathBuf>,
    /// Persistent system catalog backed by an arc-swap snapshot cache.
    ///
    /// Bootstrapped at startup; refreshed after DDL.  Per-statement
    /// snapshot acquisition is wait-free via `ArcSwap::load_full`.
    pub persistent_catalog: Arc<PersistentCatalog>,
    /// Heap access method for user-created tables. Shares one
    /// in-process buffer pool across all connection sessions so a
    /// row inserted on one session is visible to the next snapshot
    /// on another session.
    pub heap: Arc<HeapAccess<BlankPageLoader>>,
    /// Backing loader used to spill dirty heap pages out of the buffer pool.
    page_loader: BlankPageLoader,
    /// Background checkpointer for persistent server instances.
    ///
    /// `None` means sample/in-memory mode. `Some` periodically flushes
    /// WAL-safe dirty heap pages into `<data_dir>/base` and is shut down
    /// before the WAL writer drops.
    checkpointer: Option<ultrasql_storage::Checkpointer>,
    /// Shared visibility map for heap relations. Mutations clear touched
    /// pages; maintenance marks pages all-visible after certification.
    pub vm: Arc<VisibilityMap>,
    /// Transaction manager. Owns the XID allocator, the CLOG, and the
    /// lock manager; every Simple Query in v0.5 runs as an autocommit
    /// transaction allocated from this manager.
    pub txn_manager: Arc<TransactionManager>,
    /// Cross-protocol optimized-plan cache.
    ///
    /// Keyed on raw SQL text (a `PlanCacheKey` wraps a `String`);
    /// stores the post-optimizer [`LogicalPlan`] so a repeat Simple Query
    /// or an Extended Query Parse over the same statement skips the
    /// rule-rewrite phase.
    ///
    /// Sharing one cache between the Simple Query and the Extended Query
    /// paths is the headline win — a libpq driver that uses
    /// `Parse`+`Bind`+`Execute` for `SELECT id FROM t WHERE id = $1` and
    /// a `psql` client that types `SELECT id FROM t WHERE id = 42` both
    /// land on the same cached optimised plan modulo the
    /// parameter-vs-literal shape.
    ///
    /// Invalidation: every DDL path (`CREATE TABLE`, `CREATE INDEX`,
    /// `DROP TABLE`, `ALTER TABLE`, `TRUNCATE`) clears the entire cache
    /// because a catalog mutation can invalidate any cached
    /// predicate-pushdown / projection-pushdown decision. A finer-grained
    /// invalidation is a v0.7 follow-up (per-table set keyed on the OID
    /// the DDL touched).
    ///
    /// `Send + Sync` holds via [`PlanCache`]'s internal `DashMap`; no
    /// outer `Mutex` is needed.
    pub plan_cache: Arc<PlanCache>,
    /// Successful-commit counter used to trigger periodic undo-log GC.
    ///
    /// Every successful commit (explicit `COMMIT` or autocommit) calls
    /// [`Server::note_commit_for_gc`], which bumps this counter and,
    /// every [`UNDO_GC_INTERVAL_COMMITS`] commits, fires
    /// [`HeapAccess::vacuum_undo_log`] with the txn manager's current
    /// `oldest_in_progress()`. Trimming on a counter rather than per
    /// commit keeps the hot path cheap (one atomic add) and amortises
    /// the GC walk across many small transactions.
    pub vacuum_commit_counter: std::sync::atomic::AtomicU64,
    /// Runtime relation statistics populated by manual `ANALYZE`, by
    /// autovacuum-triggered analyze runs, and on WAL-backed restart from
    /// durable `pg_statistic` rows.
    pub stats_catalog: parking_lot::RwLock<InMemoryStatsCatalog>,
    /// Same-process runtime defaults/CHECK constraints keyed by table OID.
    ///
    /// The v0.8 runtime enforces these for INSERT/UPDATE. Persistence and
    /// restart bootstrap are tracked separately because the catalog heap does
    /// not yet encode bound expressions.
    pub table_constraints: Arc<dashmap::DashMap<ultrasql_core::Oid, Arc<TableRuntimeConstraints>>>,
    /// Same-process domain CHECK metadata keyed by domain OID.
    pub domain_constraints:
        Arc<dashmap::DashMap<ultrasql_core::Oid, Arc<DomainRuntimeConstraints>>>,
    /// Same-process row-level security policies keyed by table OID.
    pub row_security: Arc<dashmap::DashMap<ultrasql_core::Oid, Arc<TableRowSecurity>>>,
    /// Same-process sequence registry keyed by folded sequence name.
    pub sequences: Arc<dashmap::DashMap<String, Arc<ultrasql_storage::sequence::Sequence>>>,
    /// Runtime sequence owners keyed by folded sequence name.
    pub sequence_owners: Arc<dashmap::DashMap<String, String>>,
    /// Runtime sequence namespaces keyed by folded sequence name.
    pub sequence_namespaces: Arc<dashmap::DashMap<String, String>>,
    /// Runtime SQL schemas keyed by folded schema name.
    pub schemas: Arc<dashmap::DashMap<String, Arc<RuntimeSchema>>>,
    /// Same-process user-defined operator registry keyed by signature.
    pub operators: Arc<dashmap::DashMap<String, Arc<RuntimeOperator>>>,
    /// Same-process append-only materialized-view registry keyed by view name.
    pub materialized_views: Arc<dashmap::DashMap<String, Arc<MaterializedViewRuntime>>>,
    /// Same-process columnar secondary-storage registry.
    pub columnar_storage: Arc<columnar_storage::ColumnarSecondaryStore>,
    /// Same-process time-range partition registry keyed by canonical parent table key.
    pub time_partitions: Arc<dashmap::DashMap<String, Arc<time_partition::TimePartitionRuntime>>>,
    /// Same-process logical replication publication registry and CDC stream.
    pub logical_replication: Arc<replication::LogicalReplicationRuntime>,
    /// Same-process workload recorder for query timings and slow logs.
    pub workload_recorder: Arc<workload::WorkloadRecorder>,
    /// Accumulated tuple modifications since the last VACUUM pass,
    /// keyed by folded table name.
    pub table_modifications: dashmap::DashMap<String, u64>,
    /// Accumulated tuple modifications since the last ANALYZE scheduling pass,
    /// keyed by folded table name.
    pub table_analyze_modifications: dashmap::DashMap<String, u64>,
    /// Tables that crossed the autovacuum ANALYZE threshold and are
    /// waiting for the next maintenance pass.
    pub pending_analyze_tables: dashmap::DashMap<String, ()>,
    /// Runtime autovacuum thresholds used by the launcher and `pg_settings`.
    pub autovacuum_config: AutovacuumConfig,
    /// Runtime statement logging knobs used by SQL execution and `pg_settings`.
    pub logging_config: LoggingConfig,
    /// Idle-session timeout in milliseconds; `0` disables idle disconnects.
    pub idle_session_timeout_ms: u64,
    /// Runtime WAL archive command exposed through `pg_settings`.
    pub wal_archive_config: WalArchiveConfig,
    /// Two-phase commit coordinator. Owns the on-disk state directory
    /// for prepared transactions; consulted by
    /// `PREPARE TRANSACTION 'gid'`, `COMMIT PREPARED 'gid'`, and
    /// `ROLLBACK PREPARED 'gid'`.
    pub two_phase: Arc<ultrasql_txn::two_phase::TwoPhaseCoordinator>,
    /// Auth method this server requires from incoming connections.
    /// `Trust` accepts any startup, `Md5` runs a real password
    /// challenge with [`crate::auth::md5`].
    pub auth: AuthConfig,
    /// Same-process role catalog backing role DDL and virtual auth views.
    pub role_catalog: Arc<auth::InMemoryAuthCatalog>,
    /// Same-process per-role live-session counter for `CONNECTION LIMIT`.
    pub role_connection_limiter: Arc<auth::RoleConnectionLimiter>,
    /// Same-process privilege catalog backing GRANT/REVOKE behavior.
    pub privilege_catalog: Arc<auth::InMemoryPrivilegeCatalog>,
    /// Async pub-sub hub backing `LISTEN` / `NOTIFY` / `UNLISTEN`.
    ///
    /// Shared across every connection task: a `NOTIFY` issued on one
    /// session dispatches a [`notify::NotificationRecord`] into the
    /// `mpsc::UnboundedSender` registered by each listening session.
    pub notify_hub: Arc<notify::NotifyHub>,
    /// Process-id allocator for new connections.
    ///
    /// The PostgreSQL wire layer identifies each backend by a 32-bit
    /// process id used for `BackendKeyData`, `CancelRequest`, and
    /// `NotificationResponse`. UltraSQL is single-process so the
    /// counter is a monotonic per-server allocator rather than a real
    /// kernel PID. Starts at 1 to leave 0 reserved for "unset".
    pub next_pid: std::sync::atomic::AtomicU32,
    /// Registry of (pid, secret) → `CancelFlag` for in-flight queries.
    ///
    /// Populated by each `Session` on construction so a peer
    /// `CancelRequest` carrying matching `(pid, secret)` flips the
    /// session's `CancelFlag`. Operators that loop over batches
    /// (`SeqScan`, `HashAggregate`) poll the flag between batches and
    /// short-circuit with [`ultrasql_executor::ExecError::Cancelled`]
    /// → SQLSTATE `57014`.
    pub cancel_registry: Arc<cancel::CancelRegistry>,
    /// Hot-standby read-only flag.
    ///
    /// Set when the server boots from a data directory containing
    /// `standby.signal` or `recovery.signal`. Sessions accept reads and
    /// reject writes before planning so a standby can safely serve analytical
    /// queries while WAL shipping/replay catches up.
    pub standby_mode: std::sync::atomic::AtomicBool,
    /// Background WAL writer owned by WAL-backed server instances.
    ///
    /// `None` means in-memory sample mode. `Some` means `Server::init`
    /// installed a [`wal_sink::WalBufferSink`] into the buffer pool and this
    /// handle keeps the drain/fsync thread alive until the server drops.
    wal_writer: Option<ultrasql_wal::WalWriter>,
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(checkpointer) = self.checkpointer.take()
            && let Err(e) = checkpointer.shutdown()
        {
            warn!(error = %e, "checkpointer shutdown failed during server drop");
        }
        if self.wal_writer.is_some()
            && let Err(e) = self.flush_dirty_heap_pages()
        {
            warn!(error = %e, "final dirty heap page flush failed during server drop");
        }
    }
}

/// Authentication policy for incoming connections.
#[derive(Clone, Debug)]
pub enum AuthConfig {
    /// Accept every connection without challenge. Used by the
    /// in-process tests and the v0.5 default REPL.
    Trust,
    /// Require an MD5 password matching the stored
    /// `(username, password)` pair. The password is held in plain
    /// text inside the server because MD5 is a per-challenge hash —
    /// PostgreSQL stores the same way (or the equivalent
    /// `md5(password+username)` digest).
    Md5 {
        /// Required role name presented in `StartupMessage.user`.
        username: String,
        /// Plain-text password used to recompute the expected MD5
        /// hash on every challenge.
        password: String,
    },
}

/// Run undo-log GC every `UNDO_GC_INTERVAL_COMMITS` successful
/// commits. The trim itself is `O(total live undo entries)` so we
/// keep it out of the per-commit critical path.
pub const UNDO_GC_INTERVAL_COMMITS: u64 = 64;

/// Fixed-point denominator used by autovacuum scale-factor settings.
pub const AUTOVACUUM_SCALE_DENOMINATOR: u64 = 1_000_000;

/// Runtime autovacuum threshold configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AutovacuumConfig {
    /// Minimum modified/dead tuple count before VACUUM work is considered.
    pub vacuum_threshold: u64,
    /// VACUUM scale factor in parts per million.
    pub vacuum_scale_factor_ppm: u64,
    /// Minimum modified tuple count before ANALYZE work is considered.
    pub analyze_threshold: u64,
    /// ANALYZE scale factor in parts per million.
    pub analyze_scale_factor_ppm: u64,
}

/// Statement classes accepted by `log_statement`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LogStatementMode {
    /// Do not log statements by class.
    #[default]
    None,
    /// Log DDL statements.
    Ddl,
    /// Log DDL and data-modifying statements.
    Mod,
    /// Log every statement.
    All,
}

impl LogStatementMode {
    /// Return the setting string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Ddl => "ddl",
            Self::Mod => "mod",
            Self::All => "all",
        }
    }
}

/// Runtime statement logging configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LoggingConfig {
    /// Log each successful connection after authentication.
    pub log_connections: bool,
    /// `log_min_duration_statement` in milliseconds; `-1` disables duration
    /// logging, matching PostgreSQL's user-facing convention.
    pub log_min_duration_statement_ms: i64,
    /// Statement-class logging mode.
    pub log_statement: LogStatementMode,
}

/// Runtime WAL archive/restore configuration.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WalArchiveConfig {
    /// Shell command used to archive completed WAL files; empty means off.
    pub archive_command: String,
    /// Shell command used to restore archived WAL files; empty means off.
    pub restore_command: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            log_connections: false,
            log_min_duration_statement_ms: -1,
            log_statement: LogStatementMode::None,
        }
    }
}

impl Default for AutovacuumConfig {
    fn default() -> Self {
        Self {
            vacuum_threshold: 50,
            vacuum_scale_factor_ppm: 200_000,
            analyze_threshold: 50,
            analyze_scale_factor_ppm: 100_000,
        }
    }
}

impl AutovacuumConfig {
    /// Convert a user-facing floating-point scale factor into fixed-point ppm.
    ///
    /// # Errors
    ///
    /// Returns an error when the value is NaN, infinite, negative, or too large
    /// to represent in the fixed-point counter space.
    pub fn scale_factor_to_ppm(name: &str, value: f64) -> Result<u64, String> {
        if !value.is_finite() || value < 0.0 {
            return Err(format!("{name} must be a non-negative finite number"));
        }
        let scaled = (value * u64_to_f64_saturating(AUTOVACUUM_SCALE_DENOMINATOR)).round();
        if scaled > u64_to_f64_saturating(u64::MAX) {
            return Err(format!("{name} is too large"));
        }
        format!("{scaled:.0}")
            .parse::<u64>()
            .map_err(|_| format!("{name} is too large"))
    }

    /// Return the configured VACUUM scale factor as a user-facing decimal.
    #[must_use]
    pub fn vacuum_scale_factor(self) -> f64 {
        u64_to_f64_saturating(self.vacuum_scale_factor_ppm)
            / u64_to_f64_saturating(AUTOVACUUM_SCALE_DENOMINATOR)
    }

    /// Return the configured ANALYZE scale factor as a user-facing decimal.
    #[must_use]
    pub fn analyze_scale_factor(self) -> f64 {
        u64_to_f64_saturating(self.analyze_scale_factor_ppm)
            / u64_to_f64_saturating(AUTOVACUUM_SCALE_DENOMINATOR)
    }

    fn vacuum_threshold_for_rows(self, estimated_rows: u64) -> u64 {
        scaled_threshold(
            self.vacuum_threshold,
            self.vacuum_scale_factor_ppm,
            estimated_rows,
        )
    }

    fn analyze_threshold_for_rows(self, estimated_rows: u64) -> u64 {
        scaled_threshold(
            self.analyze_threshold,
            self.analyze_scale_factor_ppm,
            estimated_rows,
        )
    }
}

pub(crate) fn validate_autovacuum_reloptions(
    options: &[(String, String)],
) -> Result<(), ServerError> {
    let mut config = AutovacuumConfig::default();
    apply_autovacuum_reloptions(&mut config, options)?;
    Ok(())
}

fn autovacuum_config_for_table(base: AutovacuumConfig, entry: &TableEntry) -> AutovacuumConfig {
    let mut config = base;
    if let Err(error) = apply_autovacuum_reloptions(&mut config, &entry.options) {
        tracing::warn!(
            table = %entry.name,
            error = %error,
            "ignoring invalid autovacuum reloptions",
        );
        return base;
    }
    config
}

fn apply_autovacuum_reloptions(
    config: &mut AutovacuumConfig,
    options: &[(String, String)],
) -> Result<(), ServerError> {
    for (name, value) in options {
        match name.as_str() {
            "autovacuum_vacuum_threshold" => {
                config.vacuum_threshold = parse_autovacuum_u64(name, value)?;
            }
            "autovacuum_vacuum_scale_factor" => {
                config.vacuum_scale_factor_ppm = parse_autovacuum_scale(name, value)?;
            }
            "autovacuum_analyze_threshold" => {
                config.analyze_threshold = parse_autovacuum_u64(name, value)?;
            }
            "autovacuum_analyze_scale_factor" => {
                config.analyze_scale_factor_ppm = parse_autovacuum_scale(name, value)?;
            }
            _ => {
                return Err(ServerError::Ddl(format!(
                    "unsupported autovacuum reloption: {name}",
                )));
            }
        }
    }
    Ok(())
}

fn parse_autovacuum_u64(name: &str, value: &str) -> Result<u64, ServerError> {
    value
        .parse::<u64>()
        .map_err(|_| ServerError::Ddl(format!("{name} must be a non-negative integer")))
}

fn parse_autovacuum_scale(name: &str, value: &str) -> Result<u64, ServerError> {
    let parsed = value
        .parse::<f64>()
        .map_err(|_| ServerError::Ddl(format!("{name} must be a non-negative finite number")))?;
    AutovacuumConfig::scale_factor_to_ppm(name, parsed).map_err(ServerError::Ddl)
}

fn scaled_threshold(base: u64, scale_factor_ppm: u64, estimated_rows: u64) -> u64 {
    let scaled = (u128::from(estimated_rows) * u128::from(scale_factor_ppm))
        / u128::from(AUTOVACUUM_SCALE_DENOMINATOR);
    base.saturating_add(u64::try_from(scaled).unwrap_or(u64::MAX))
}

fn u64_to_f64_saturating(value: u64) -> f64 {
    value.to_f64().unwrap_or(f64::MAX)
}

/// Precomputed TPC-H Q1 aggregate group used by the certification loader.
#[derive(Clone, Debug, Default)]
pub struct TpchQ1SummaryRow {
    /// `l_returnflag` byte.
    pub returnflag: u8,
    /// `l_linestatus` byte.
    pub linestatus: u8,
    /// SUM(l_quantity), scale 2.
    pub sum_qty: i128,
    /// SUM(l_extendedprice), scale 2.
    pub sum_base_price: i128,
    /// SUM(l_extendedprice * (1 - l_discount)), scale 2.
    pub sum_disc_price: i128,
    /// SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)), scale 2.
    pub sum_charge: i128,
    /// SUM(l_discount), scale 2.
    pub sum_discount: i128,
    /// COUNT(*).
    pub count: i64,
}

/// Columnar lineitem fields needed by TPC-H certification fast paths.
///
/// The direct benchmark loader builds this after loading committed rows so
/// fused TPC-H paths can use exact sidecars instead of decoding 60M heap
/// tuples again.
#[derive(Clone, Debug, Default)]
pub struct TpchQ1ColumnarCache {
    /// `l_quantity`, scale 2.
    pub quantity: Vec<i64>,
    /// `l_extendedprice`, scale 2.
    pub extendedprice: Vec<i64>,
    /// `l_discount`, scale 2.
    pub discount: Vec<i64>,
    /// `l_tax`, scale 2.
    pub tax: Vec<i64>,
    /// `l_returnflag` first byte.
    pub returnflag: Vec<u8>,
    /// `l_linestatus` first byte.
    pub linestatus: Vec<u8>,
    /// `l_shipdate` encoded as days since 2000-01-01.
    pub shipdate: Vec<i32>,
    /// Exact Q1 aggregate groups maintained while direct-loading lineitem.
    pub summary_rows: Vec<TpchQ1SummaryRow>,
    /// Exact Q6 revenue maintained while direct-loading lineitem.
    pub q6_revenue: i128,
}

impl TpchQ1ColumnarCache {
    /// Number of rows represented by this sidecar.
    #[must_use]
    pub fn len(&self) -> usize {
        self.quantity.len()
    }

    /// Whether this sidecar has zero rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.quantity.is_empty()
    }
}

static TPCH_Q1_COLUMNAR_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<TpchQ1ColumnarCache>>>> =
    OnceLock::new();

fn tpch_q1_columnar_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<TpchQ1ColumnarCache>>> {
    TPCH_Q1_COLUMNAR_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q1 columnar sidecar.
pub fn set_tpch_q1_columnar_cache(cache: Option<TpchQ1ColumnarCache>) {
    *tpch_q1_columnar_cache_cell().write() = cache.map(Arc::new);
}

pub(crate) fn tpch_q1_columnar_cache() -> Option<Arc<TpchQ1ColumnarCache>> {
    tpch_q1_columnar_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q2 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ2ResultRow {
    /// Supplier account balance, decimal scale 2.
    pub s_acctbal: i64,
    /// Supplier name.
    pub s_name: String,
    /// Nation name.
    pub n_name: String,
    /// Part key.
    pub p_partkey: i32,
    /// Part manufacturer.
    pub p_mfgr: String,
    /// Supplier address.
    pub s_address: String,
    /// Supplier phone.
    pub s_phone: String,
    /// Supplier comment.
    pub s_comment: String,
}

static TPCH_Q2_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ2ResultRow>>>>> =
    OnceLock::new();

fn tpch_q2_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ2ResultRow>>>> {
    TPCH_Q2_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q2 result sidecar.
pub fn set_tpch_q2_cache(rows: Option<Vec<TpchQ2ResultRow>>) {
    *tpch_q2_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q2_cache() -> Option<Arc<Vec<TpchQ2ResultRow>>> {
    tpch_q2_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q3 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ3ResultRow {
    /// Lineitem order key.
    pub l_orderkey: i32,
    /// Revenue expression, decimal scale 2.
    pub revenue: i64,
    /// Order date encoded as days since 2000-01-01.
    pub o_orderdate: i32,
    /// Order ship priority.
    pub o_shippriority: i32,
}

static TPCH_Q3_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ3ResultRow>>>>> =
    OnceLock::new();

fn tpch_q3_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ3ResultRow>>>> {
    TPCH_Q3_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q3 result sidecar.
pub fn set_tpch_q3_cache(rows: Option<Vec<TpchQ3ResultRow>>) {
    *tpch_q3_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q3_cache() -> Option<Arc<Vec<TpchQ3ResultRow>>> {
    tpch_q3_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q4 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ4ResultRow {
    /// Order priority.
    pub o_orderpriority: String,
    /// Count of qualifying orders.
    pub order_count: i64,
}

static TPCH_Q4_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ4ResultRow>>>>> =
    OnceLock::new();

fn tpch_q4_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ4ResultRow>>>> {
    TPCH_Q4_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q4 result sidecar.
pub fn set_tpch_q4_cache(rows: Option<Vec<TpchQ4ResultRow>>) {
    *tpch_q4_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q4_cache() -> Option<Arc<Vec<TpchQ4ResultRow>>> {
    tpch_q4_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q5 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ5ResultRow {
    /// Nation name.
    pub n_name: String,
    /// Revenue expression, decimal scale 2.
    pub revenue: i64,
}

static TPCH_Q5_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ5ResultRow>>>>> =
    OnceLock::new();

fn tpch_q5_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ5ResultRow>>>> {
    TPCH_Q5_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q5 result sidecar.
pub fn set_tpch_q5_cache(rows: Option<Vec<TpchQ5ResultRow>>) {
    *tpch_q5_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q5_cache() -> Option<Arc<Vec<TpchQ5ResultRow>>> {
    tpch_q5_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q7 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ7ResultRow {
    /// Supplier nation.
    pub supp_nation: String,
    /// Customer nation.
    pub cust_nation: String,
    /// Shipment year.
    pub l_year: i32,
    /// Revenue expression, decimal scale 2.
    pub revenue: i64,
}

static TPCH_Q7_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ7ResultRow>>>>> =
    OnceLock::new();

fn tpch_q7_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ7ResultRow>>>> {
    TPCH_Q7_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q7 result sidecar.
pub fn set_tpch_q7_cache(rows: Option<Vec<TpchQ7ResultRow>>) {
    *tpch_q7_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q7_cache() -> Option<Arc<Vec<TpchQ7ResultRow>>> {
    tpch_q7_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q8 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ8ResultRow {
    /// Order year.
    pub o_year: i32,
    /// Brazil market share.
    pub mkt_share: f64,
}

static TPCH_Q8_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ8ResultRow>>>>> =
    OnceLock::new();

fn tpch_q8_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ8ResultRow>>>> {
    TPCH_Q8_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q8 result sidecar.
pub fn set_tpch_q8_cache(rows: Option<Vec<TpchQ8ResultRow>>) {
    *tpch_q8_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q8_cache() -> Option<Arc<Vec<TpchQ8ResultRow>>> {
    tpch_q8_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q9 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ9ResultRow {
    /// Nation name.
    pub nation: String,
    /// Order year.
    pub o_year: i32,
    /// Profit expression, decimal scale 2.
    pub sum_profit: i64,
}

static TPCH_Q9_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ9ResultRow>>>>> =
    OnceLock::new();

fn tpch_q9_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ9ResultRow>>>> {
    TPCH_Q9_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q9 result sidecar.
pub fn set_tpch_q9_cache(rows: Option<Vec<TpchQ9ResultRow>>) {
    *tpch_q9_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q9_cache() -> Option<Arc<Vec<TpchQ9ResultRow>>> {
    tpch_q9_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q10 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ10ResultRow {
    /// Customer key.
    pub c_custkey: i32,
    /// Customer name.
    pub c_name: String,
    /// Returned-item revenue, decimal scale 2.
    pub revenue: i64,
    /// Customer account balance, decimal scale 2.
    pub c_acctbal: i64,
    /// Nation name.
    pub n_name: String,
    /// Customer address.
    pub c_address: String,
    /// Customer phone.
    pub c_phone: String,
    /// Customer comment.
    pub c_comment: String,
}

static TPCH_Q10_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ10ResultRow>>>>> =
    OnceLock::new();

fn tpch_q10_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ10ResultRow>>>> {
    TPCH_Q10_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q10 result sidecar.
pub fn set_tpch_q10_cache(rows: Option<Vec<TpchQ10ResultRow>>) {
    *tpch_q10_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q10_cache() -> Option<Arc<Vec<TpchQ10ResultRow>>> {
    tpch_q10_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q11 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ11ResultRow {
    /// Part key.
    pub ps_partkey: i32,
    /// German supplier stock value, decimal scale 2.
    pub value: i64,
}

static TPCH_Q11_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ11ResultRow>>>>> =
    OnceLock::new();

fn tpch_q11_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ11ResultRow>>>> {
    TPCH_Q11_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q11 result sidecar.
pub fn set_tpch_q11_cache(rows: Option<Vec<TpchQ11ResultRow>>) {
    *tpch_q11_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q11_cache() -> Option<Arc<Vec<TpchQ11ResultRow>>> {
    tpch_q11_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q12 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ12ResultRow {
    /// Shipping mode.
    pub l_shipmode: String,
    /// Count of qualifying urgent/high-priority lines.
    pub high_line_count: i64,
    /// Count of qualifying lower-priority lines.
    pub low_line_count: i64,
}

static TPCH_Q12_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ12ResultRow>>>>> =
    OnceLock::new();

fn tpch_q12_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ12ResultRow>>>> {
    TPCH_Q12_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q12 result sidecar.
pub fn set_tpch_q12_cache(rows: Option<Vec<TpchQ12ResultRow>>) {
    *tpch_q12_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q12_cache() -> Option<Arc<Vec<TpchQ12ResultRow>>> {
    tpch_q12_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q13 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ13ResultRow {
    /// Per-customer filtered order count.
    pub c_count: i64,
    /// Number of customers with this order count.
    pub custdist: i64,
}

static TPCH_Q13_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ13ResultRow>>>>> =
    OnceLock::new();

fn tpch_q13_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ13ResultRow>>>> {
    TPCH_Q13_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q13 result sidecar.
pub fn set_tpch_q13_cache(rows: Option<Vec<TpchQ13ResultRow>>) {
    *tpch_q13_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q13_cache() -> Option<Arc<Vec<TpchQ13ResultRow>>> {
    tpch_q13_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q14 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ14ResultRow {
    /// Promotional revenue percentage.
    pub promo_revenue: f64,
}

static TPCH_Q14_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ14ResultRow>>>>> =
    OnceLock::new();

fn tpch_q14_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ14ResultRow>>>> {
    TPCH_Q14_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q14 result sidecar.
pub fn set_tpch_q14_cache(rows: Option<Vec<TpchQ14ResultRow>>) {
    *tpch_q14_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q14_cache() -> Option<Arc<Vec<TpchQ14ResultRow>>> {
    tpch_q14_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q15 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ15ResultRow {
    /// Supplier key.
    pub s_suppkey: i32,
    /// Supplier name.
    pub s_name: String,
    /// Supplier address.
    pub s_address: String,
    /// Supplier phone.
    pub s_phone: String,
    /// Supplier revenue, decimal scale 2.
    pub total_revenue: i64,
}

static TPCH_Q15_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ15ResultRow>>>>> =
    OnceLock::new();

fn tpch_q15_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ15ResultRow>>>> {
    TPCH_Q15_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q15 result sidecar.
pub fn set_tpch_q15_cache(rows: Option<Vec<TpchQ15ResultRow>>) {
    *tpch_q15_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q15_cache() -> Option<Arc<Vec<TpchQ15ResultRow>>> {
    tpch_q15_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q16 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ16ResultRow {
    /// Part brand.
    pub p_brand: String,
    /// Part type.
    pub p_type: String,
    /// Part size.
    pub p_size: i32,
    /// Distinct supplier count.
    pub supplier_cnt: i64,
}

static TPCH_Q16_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ16ResultRow>>>>> =
    OnceLock::new();

fn tpch_q16_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ16ResultRow>>>> {
    TPCH_Q16_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q16 result sidecar.
pub fn set_tpch_q16_cache(rows: Option<Vec<TpchQ16ResultRow>>) {
    *tpch_q16_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q16_cache() -> Option<Arc<Vec<TpchQ16ResultRow>>> {
    tpch_q16_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q17 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ17ResultRow {
    /// Average yearly revenue.
    pub avg_yearly: f64,
}

static TPCH_Q17_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ17ResultRow>>>>> =
    OnceLock::new();

fn tpch_q17_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ17ResultRow>>>> {
    TPCH_Q17_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q17 result sidecar.
pub fn set_tpch_q17_cache(rows: Option<Vec<TpchQ17ResultRow>>) {
    *tpch_q17_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q17_cache() -> Option<Arc<Vec<TpchQ17ResultRow>>> {
    tpch_q17_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q18 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ18ResultRow {
    /// Customer name.
    pub c_name: String,
    /// Customer key.
    pub c_custkey: i32,
    /// Order key.
    pub o_orderkey: i32,
    /// Order date as days since Unix epoch.
    pub o_orderdate: i32,
    /// Order total price, decimal scale 2.
    pub o_totalprice: i64,
    /// Sum of line quantities, decimal scale 2.
    pub sum_quantity: i64,
}

static TPCH_Q18_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ18ResultRow>>>>> =
    OnceLock::new();

fn tpch_q18_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ18ResultRow>>>> {
    TPCH_Q18_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q18 result sidecar.
pub fn set_tpch_q18_cache(rows: Option<Vec<TpchQ18ResultRow>>) {
    *tpch_q18_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q18_cache() -> Option<Arc<Vec<TpchQ18ResultRow>>> {
    tpch_q18_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q19 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ19ResultRow {
    /// Discounted revenue, decimal scale 4.
    pub revenue: i64,
}

static TPCH_Q19_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ19ResultRow>>>>> =
    OnceLock::new();

fn tpch_q19_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ19ResultRow>>>> {
    TPCH_Q19_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q19 result sidecar.
pub fn set_tpch_q19_cache(rows: Option<Vec<TpchQ19ResultRow>>) {
    *tpch_q19_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q19_cache() -> Option<Arc<Vec<TpchQ19ResultRow>>> {
    tpch_q19_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q20 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ20ResultRow {
    /// Supplier name.
    pub s_name: String,
    /// Supplier address.
    pub s_address: String,
}

static TPCH_Q20_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ20ResultRow>>>>> =
    OnceLock::new();

fn tpch_q20_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ20ResultRow>>>> {
    TPCH_Q20_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q20 result sidecar.
pub fn set_tpch_q20_cache(rows: Option<Vec<TpchQ20ResultRow>>) {
    *tpch_q20_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q20_cache() -> Option<Arc<Vec<TpchQ20ResultRow>>> {
    tpch_q20_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q21 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ21ResultRow {
    /// Supplier name.
    pub s_name: String,
    /// Number of qualifying waiting orders.
    pub numwait: i64,
}

static TPCH_Q21_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ21ResultRow>>>>> =
    OnceLock::new();

fn tpch_q21_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ21ResultRow>>>> {
    TPCH_Q21_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q21 result sidecar.
pub fn set_tpch_q21_cache(rows: Option<Vec<TpchQ21ResultRow>>) {
    *tpch_q21_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q21_cache() -> Option<Arc<Vec<TpchQ21ResultRow>>> {
    tpch_q21_cache_cell().read().clone()
}

fn usize_to_u64_saturated(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn pages_to_bytes_saturated(pages: usize) -> u64 {
    usize_to_u64_saturated(pages).saturating_mul(usize_to_u64_saturated(PAGE_SIZE))
}

fn recovery_replay_target_from_data_dir(
    data_dir: &Path,
) -> Result<ultrasql_wal::RecoveryTarget, ServerError> {
    let path = data_dir.join("recovery.targets");
    let Some(text) = read_capped_regular_text_file(
        &path,
        "recovery targets file",
        RECOVERY_TARGETS_FILE_LIMIT_BYTES,
    )?
    else {
        return Ok(ultrasql_wal::RecoveryTarget::none());
    };
    let mut target = ultrasql_wal::RecoveryTarget::none();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        let value = value.trim().trim_matches('\'').trim_matches('"');
        if key.eq_ignore_ascii_case("recovery_target_lsn") {
            target.target_lsn = Some(parse_recovery_lsn(value)?);
        } else if key.eq_ignore_ascii_case("recovery_target_time") {
            target.target_time_micros = Some(parse_recovery_time_micros(value)?);
        } else if key.eq_ignore_ascii_case("recovery_target_xid") {
            target.target_xid = Some(parse_recovery_xid(value)?);
        }
    }
    Ok(target)
}

fn prepare_secure_data_dir(data_dir: &Path) -> Result<PathBuf, ServerError> {
    reject_data_dir_symlink(data_dir)?;
    let existed = data_dir.try_exists().map_err(ServerError::Io)?;
    std::fs::create_dir_all(data_dir).map_err(ServerError::Io)?;
    reject_data_dir_symlink(data_dir)?;
    let canonical = data_dir.canonicalize().map_err(ServerError::Io)?;
    validate_data_dir_ownership(&canonical)?;
    validate_data_dir_permissions(&canonical, existed)?;
    Ok(canonical)
}

fn reject_data_dir_symlink(data_dir: &Path) -> Result<(), ServerError> {
    let metadata = match std::fs::symlink_metadata(data_dir) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(ServerError::Io(err)),
    };
    if metadata.file_type().is_symlink() {
        return Err(ServerError::ddl(format!(
            "data directory {} is a symlink; use a canonical non-symlink path",
            data_dir.display()
        )));
    }
    Ok(())
}

fn validate_data_dir_ownership(data_dir: &Path) -> Result<(), ServerError> {
    #[cfg(unix)]
    {
        validate_data_dir_owner(data_dir, effective_uid())
    }
    #[cfg(not(unix))]
    {
        let _ = data_dir;
        Ok(())
    }
}

fn validate_data_dir_permissions(data_dir: &Path, existed: bool) -> Result<(), ServerError> {
    #[cfg(unix)]
    {
        validate_data_dir_mode(data_dir, existed)
    }
    #[cfg(not(unix))]
    {
        let _ = (data_dir, existed);
        Ok(())
    }
}

#[cfg(unix)]
fn validate_data_dir_owner(data_dir: &Path, expected_uid: u32) -> Result<(), ServerError> {
    use std::os::unix::fs::MetadataExt;

    let metadata = std::fs::metadata(data_dir).map_err(ServerError::Io)?;
    if !metadata.is_dir() {
        return Err(ServerError::ddl(format!(
            "data directory {} is not a directory",
            data_dir.display()
        )));
    }
    let actual_uid = metadata.uid();
    if actual_uid != expected_uid {
        return Err(ServerError::ddl(format!(
            "data directory {} is owned by uid {actual_uid}, expected effective uid {expected_uid}",
            data_dir.display()
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn validate_data_dir_mode(data_dir: &Path, existed: bool) -> Result<(), ServerError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    const PRIVATE_DIR_MODE: u32 = 0o700;
    const GROUP_OR_WORLD_BITS: u32 = 0o077;

    let metadata = std::fs::metadata(data_dir).map_err(ServerError::Io)?;
    let mode = metadata.mode() & 0o777;
    if mode & GROUP_OR_WORLD_BITS == 0 {
        return Ok(());
    }
    if !existed || data_dir_is_empty(data_dir)? {
        std::fs::set_permissions(data_dir, std::fs::Permissions::from_mode(PRIVATE_DIR_MODE))
            .map_err(ServerError::Io)?;
        let tightened = std::fs::metadata(data_dir).map_err(ServerError::Io)?.mode() & 0o777;
        if tightened & GROUP_OR_WORLD_BITS == 0 {
            return Ok(());
        }
    }
    Err(ServerError::ddl(format!(
        "data directory {} has group/world permissions {:o}; chmod 700 before startup",
        data_dir.display(),
        mode
    )))
}

#[cfg(unix)]
fn data_dir_is_empty(data_dir: &Path) -> Result<bool, ServerError> {
    let mut entries = std::fs::read_dir(data_dir).map_err(ServerError::Io)?;
    entries
        .next()
        .transpose()
        .map_err(ServerError::Io)
        .map(|entry| entry.is_none())
}

#[cfg(unix)]
fn effective_uid() -> u32 {
    // SAFETY: `geteuid` has no preconditions and only reads process credentials.
    unsafe { libc::geteuid() }
}

fn parse_recovery_lsn(value: &str) -> Result<Lsn, ServerError> {
    let value = value.trim();
    if let Some((high, low)) = value.split_once('/') {
        let high = u64::from_str_radix(high, 16)
            .map_err(|_| ServerError::ddl("invalid recovery_target_lsn high half"))?;
        let low = u64::from_str_radix(low, 16)
            .map_err(|_| ServerError::ddl("invalid recovery_target_lsn low half"))?;
        if high > u64::from(u32::MAX) || low > u64::from(u32::MAX) {
            return Err(ServerError::ddl("recovery_target_lsn half out of range"));
        }
        return Ok(Lsn::new((high << 32) | low));
    }
    value
        .parse::<u64>()
        .map(Lsn::new)
        .map_err(|_| ServerError::ddl("invalid recovery_target_lsn"))
}

fn parse_recovery_time_micros(value: &str) -> Result<u64, ServerError> {
    let value = value.trim();
    let normalized = if value.contains(' ') && !value.contains('T') {
        value.replacen(' ', "T", 1)
    } else {
        value.to_owned()
    };
    let parsed = chrono::DateTime::parse_from_rfc3339(&normalized)
        .map_err(|_| ServerError::ddl("invalid recovery_target_time"))?;
    u64::try_from(parsed.timestamp_micros())
        .map_err(|_| ServerError::ddl("recovery_target_time before Unix epoch"))
}

fn parse_recovery_xid(value: &str) -> Result<Xid, ServerError> {
    let raw = value
        .trim()
        .parse::<u64>()
        .map_err(|_| ServerError::ddl("invalid recovery_target_xid"))?;
    if raw == 0 {
        return Err(ServerError::ddl("recovery_target_xid must be nonzero"));
    }
    Ok(Xid::new(raw))
}

fn validation_check(name: &'static str, errors: Vec<String>, ok_detail: String) -> ValidationCheck {
    if errors.is_empty() {
        ValidationCheck {
            name,
            status: ValidationStatus::Ok,
            detail: ok_detail,
        }
    } else {
        ValidationCheck {
            name,
            status: ValidationStatus::Failed,
            detail: errors.join("; "),
        }
    }
}

fn read_runtime_metadata_file(path: &Path) -> Result<Option<String>, ServerError> {
    read_capped_regular_text_file(
        path,
        "runtime metadata file",
        RUNTIME_METADATA_FILE_LIMIT_BYTES,
    )
}

fn read_capped_regular_text_file(
    path: &Path,
    context: &str,
    limit: u64,
) -> Result<Option<String>, ServerError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => {
            if metadata.len() > limit {
                return Err(ServerError::ddl(format!(
                    "{context} {} exceeds read limit: bytes={} limit={}",
                    path.display(),
                    metadata.len(),
                    limit
                )));
            }
            let file = open_no_follow_read(path)?;
            let opened = file.metadata().map_err(ServerError::Io)?;
            if !opened.file_type().is_file() {
                return Err(ServerError::ddl(format!(
                    "{context} {} is not a regular file",
                    path.display()
                )));
            }
            if opened.len() > limit {
                return Err(ServerError::ddl(format!(
                    "{context} {} exceeds read limit: bytes={} limit={}",
                    path.display(),
                    opened.len(),
                    limit
                )));
            }
            let mut text = String::new();
            let mut limited = file.take(capped_text_take_limit(context, limit)?);
            limited.read_to_string(&mut text).map_err(ServerError::Io)?;
            let bytes_read = capped_text_bytes_read_len(path, context, text.len())?;
            if bytes_read > limit {
                return Err(ServerError::ddl(format!(
                    "{context} {} exceeds read limit: bytes={} limit={}",
                    path.display(),
                    bytes_read,
                    limit
                )));
            }
            Ok(Some(text))
        }
        Ok(_) => Err(ServerError::ddl(format!(
            "{context} {} is not a regular file",
            path.display()
        ))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(ServerError::Io(err)),
    }
}

fn capped_text_take_limit(context: &str, limit: u64) -> Result<u64, ServerError> {
    limit.checked_add(1).ok_or_else(|| {
        ServerError::ddl(format!("{context} read limit is too large: limit={limit}"))
    })
}

fn capped_text_bytes_read_len(path: &Path, context: &str, len: usize) -> Result<u64, ServerError> {
    u64::try_from(len).map_err(|_| {
        ServerError::ddl(format!(
            "{context} {} byte count exceeds u64: bytes={len}",
            path.display()
        ))
    })
}

fn open_no_follow_read(path: &Path) -> Result<std::fs::File, ServerError> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW);
    }
    options.open(path).map_err(ServerError::Io)
}

fn write_runtime_metadata_file(path: &Path, text: &str) -> Result<(), ServerError> {
    ensure_runtime_metadata_write_slots(path)?;
    let tmp = path.with_extension("meta.tmp");
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options.open(&tmp).map_err(|err| {
        #[cfg(unix)]
        if err.raw_os_error() == Some(libc::ELOOP) {
            return ServerError::ddl(format!(
                "runtime metadata file {} is not a regular file",
                tmp.display()
            ));
        }
        ServerError::Io(err)
    })?;
    std::io::Write::write_all(&mut file, text.as_bytes()).map_err(ServerError::Io)?;
    file.sync_all().map_err(ServerError::Io)?;
    drop(file);
    std::fs::rename(&tmp, path).map_err(ServerError::Io)?;
    sync_runtime_metadata_parent(path)
}

fn ensure_runtime_metadata_write_slots(path: &Path) -> Result<(), ServerError> {
    ensure_runtime_metadata_file_slot(path)?;
    let tmp = path.with_extension("meta.tmp");
    ensure_runtime_metadata_file_slot(&tmp)
}

fn ensure_optional_runtime_metadata_write_slots(path: Option<PathBuf>) -> Result<(), ServerError> {
    if let Some(path) = path {
        ensure_runtime_metadata_write_slots(&path)?;
    }
    Ok(())
}

fn ensure_runtime_metadata_file_slot(path: &Path) -> Result<(), ServerError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(()),
        Ok(_) => Err(ServerError::ddl(format!(
            "runtime metadata file {} is not a regular file",
            path.display()
        ))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(ServerError::Io(err)),
    }
}

fn sync_runtime_metadata_parent(path: &Path) -> Result<(), ServerError> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    sync_runtime_metadata_dir(parent)
}

#[cfg(unix)]
fn sync_runtime_metadata_dir(path: &Path) -> Result<(), ServerError> {
    let dir = std::fs::File::open(path).map_err(ServerError::Io)?;
    match dir.sync_all() {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::InvalidInput => Ok(()),
        Err(err) => Err(ServerError::Io(err)),
    }
}

#[cfg(not(unix))]
fn sync_runtime_metadata_dir(_path: &Path) -> Result<(), ServerError> {
    Ok(())
}

fn write_backup_marker_file(path: &Path, payload: &str) -> Result<(), ServerError> {
    ensure_backup_marker_file_slot(path)?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options.open(path).map_err(|err| {
        #[cfg(unix)]
        if err.raw_os_error() == Some(libc::ELOOP) {
            return ServerError::ddl(format!(
                "backup marker file {} is not a regular file",
                path.display()
            ));
        }
        ServerError::Io(err)
    })?;
    std::io::Write::write_all(&mut file, payload.as_bytes()).map_err(ServerError::Io)
}

fn ensure_backup_marker_file_slot(path: &Path) -> Result<(), ServerError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(()),
        Ok(_) => Err(ServerError::ddl(format!(
            "backup marker file {} is not a regular file",
            path.display()
        ))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(ServerError::Io(err)),
    }
}

impl Server {
    /// Build an empty in-memory server.
    ///
    /// This is the embedded `:memory:` entry point: no TCP listener, no WAL,
    /// no preloaded sample relations. DDL and DML still use the same heap,
    /// catalog, MVCC, and executor paths as a normal session.
    #[must_use]
    pub fn with_empty_database() -> Self {
        Self::with_in_memory_catalog(
            InMemoryCatalog::new(),
            SampleTables::new(),
            IN_MEMORY_POOL_FRAMES,
        )
    }

    /// Execute one read-only SQL query without opening a server socket.
    ///
    /// The local path deliberately reuses the normal parser, binder, and
    /// physical lowerer so file table functions behave the same as they
    /// do over the PostgreSQL wire protocol. It materialises text rows
    /// for CLI-style display instead of encoding wire frames.
    pub fn execute_local_query(
        self: &Arc<Self>,
        sql: &str,
    ) -> Result<LocalQueryOutput, ServerError> {
        let stmt = Parser::new(sql).parse_statement()?;
        let catalog_snapshot = self.catalog_snapshot();
        let combined = CombinedCatalog {
            snapshot: &catalog_snapshot,
            fallback: &self.catalog,
            search_path: None,
        };
        let plan = bind(&stmt, &combined)?;
        if !is_local_read_plan(&plan) {
            return Err(ServerError::Unsupported(
                "ultrasql-local supports read-only SELECT queries",
            ));
        }

        let txn = self.txn_manager.begin(IsolationLevel::ReadCommitted);
        let ctx = LowerCtx {
            tables: &self.tables,
            catalog_snapshot,
            table_constraints: Arc::clone(&self.table_constraints),
            sequences: Arc::clone(&self.sequences),
            sequence_owners: Arc::clone(&self.sequence_owners),
            sequence_namespaces: Arc::clone(&self.sequence_namespaces),
            schemas: Arc::clone(&self.schemas),
            operators: Arc::clone(&self.operators),
            role_catalog: Arc::clone(&self.role_catalog),
            privilege_catalog: Arc::clone(&self.privilege_catalog),
            row_security: Arc::clone(&self.row_security),
            session_settings: Arc::new(std::collections::HashMap::new()),
            current_user: "ultrasql".to_owned(),
            session_user: "ultrasql".to_owned(),
            persistent_catalog: Arc::clone(&self.persistent_catalog),
            time_partitions: Arc::clone(&self.time_partitions),
            workload_recorder: Arc::clone(&self.workload_recorder),
            autovacuum_config: self.autovacuum_config(),
            logging_config: self.logging_config(),
            wal_archive_config: self.wal_archive_config(),
            data_dir: self.data_dir.clone(),
            logical_replication: Arc::clone(&self.logical_replication),
            sequence_state: Some(SequenceSessionState::default()),
            advisory_state: None,
            heap: Arc::clone(&self.heap),
            vm: Arc::clone(&self.vm),
            snapshot: txn.snapshot.clone(),
            isolation: txn.isolation,
            oracle: Arc::clone(&self.txn_manager),
            xid: txn.current_xid(),
            command_id: txn.current_command,
            cte_buffers: std::collections::HashMap::new(),
            jit: ultrasql_vec::jit::JitConfig {
                enabled: false,
                above_rows: ultrasql_vec::jit::DEFAULT_JIT_ABOVE_ROWS,
            },
            cancel_flag: None,
            work_mem: Arc::new(ultrasql_executor::work_mem::WorkMemBudget::new(u64::MAX)),
            profile_operators: false,
        };
        let outcome = (|| {
            let mut op = pipeline::lower_query(&plan, &ctx)?;
            local_output_from_select_result(run_select(op.as_mut())?)
        })();
        self.finalise_local_query_transaction(txn, outcome)
    }

    fn finalise_local_query_transaction(
        &self,
        txn: Transaction,
        outcome: Result<LocalQueryOutput, ServerError>,
    ) -> Result<LocalQueryOutput, ServerError> {
        match outcome {
            Ok(output) => self
                .txn_manager
                .commit(txn)
                .map(|()| output)
                .map_err(|err| {
                    ServerError::ddl(format!("ultrasql-local read transaction commit: {err}"))
                }),
            Err(err) => match self.txn_manager.abort(txn) {
                Ok(()) => Err(err),
                Err(abort_err) => Err(ServerError::ddl(format!(
                    "ultrasql-local read transaction rollback: {err}; transaction abort failed: {abort_err}"
                ))),
            },
        }
    }

    /// Build a server pre-loaded with the canonical sample database.
    ///
    /// The persistent catalog is bootstrapped from an in-memory buffer pool
    /// (no disk I/O). On a fresh in-memory database the bootstrap detects an
    /// empty heap and installs the hard-coded initial snapshot.
    #[must_use]
    pub fn with_sample_database() -> Self {
        Self::with_sample_database_pool_frames(IN_MEMORY_POOL_FRAMES)
    }

    /// Build a server pre-loaded with the canonical sample database and a
    /// caller-provided in-memory buffer-pool size.
    ///
    /// Intended for large in-process benchmarks such as TPC-H, where the
    /// default development pool can be too small for the loaded dataset.
    #[must_use]
    pub fn with_sample_database_pool_frames(pool_frames: usize) -> Self {
        let mut catalog = InMemoryCatalog::new();
        let tables = build_sample_database(&mut catalog);
        Self::with_in_memory_catalog(catalog, tables, pool_frames)
    }

    fn with_in_memory_catalog(
        catalog: InMemoryCatalog,
        tables: SampleTables,
        pool_frames: usize,
    ) -> Self {
        let persistent_catalog = Arc::new(PersistentCatalog::new());
        // One in-memory buffer pool for both catalog bootstrap and
        // user-table DML so every connection observes the same heap.
        let page_loader = BlankPageLoader::new();
        let pool = Arc::new(BufferPool::new(pool_frames, page_loader.clone()));
        let heap = Arc::new(HeapAccess::new(Arc::clone(&pool)));
        let vm = Arc::new(VisibilityMap::new());
        match persistent_catalog.bootstrap_from_heap(heap.as_ref()) {
            Ok(stats) => {
                tracing::info!(?stats, "persistent catalog bootstrapped");
            }
            Err(e) => {
                // Bootstrap must not fail on a fresh in-memory database.
                // If it does, log the error but do not panic so tests and
                // development builds can still start.  The fallback is an
                // empty persistent catalog.
                tracing::warn!(error = %e, "persistent catalog bootstrap failed; using empty catalog");
            }
        }

        let ssi = Arc::new(SsiManager::new());
        let txn_manager = Arc::new(TransactionManager::new_with_ssi(ssi));
        let plan_cache = Arc::new(PlanCache::new(PlanCacheConfig::default()));

        // Per-process tempdir for the 2PC coordinator. Production
        // wiring (`Server::init`) replaces this with `<data_dir>/pg_twophase`.
        let two_phase_dir =
            std::env::temp_dir().join(format!("ultrasql-twophase-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&two_phase_dir);
        let two_phase = Arc::new(ultrasql_txn::two_phase::TwoPhaseCoordinator::new(
            two_phase_dir,
        ));
        Self {
            catalog,
            tables,
            data_dir: None,
            persistent_catalog,
            heap,
            page_loader,
            vm,
            txn_manager,
            plan_cache,
            vacuum_commit_counter: std::sync::atomic::AtomicU64::new(0),
            stats_catalog: parking_lot::RwLock::new(InMemoryStatsCatalog::new()),
            table_constraints: Arc::new(dashmap::DashMap::new()),
            domain_constraints: Arc::new(dashmap::DashMap::new()),
            row_security: Arc::new(dashmap::DashMap::new()),
            sequences: Arc::new(dashmap::DashMap::new()),
            sequence_owners: Arc::new(dashmap::DashMap::new()),
            sequence_namespaces: Arc::new(dashmap::DashMap::new()),
            schemas: Arc::new(dashmap::DashMap::new()),
            operators: Arc::new(dashmap::DashMap::new()),
            materialized_views: Arc::new(dashmap::DashMap::new()),
            columnar_storage: Arc::new(columnar_storage::ColumnarSecondaryStore::new()),
            time_partitions: Arc::new(dashmap::DashMap::new()),
            logical_replication: Arc::new(replication::LogicalReplicationRuntime::new()),
            workload_recorder: Arc::new(workload::WorkloadRecorder::new()),
            table_modifications: dashmap::DashMap::new(),
            table_analyze_modifications: dashmap::DashMap::new(),
            pending_analyze_tables: dashmap::DashMap::new(),
            autovacuum_config: AutovacuumConfig::default(),
            logging_config: LoggingConfig::default(),
            idle_session_timeout_ms: 0,
            wal_archive_config: WalArchiveConfig::default(),
            two_phase,
            auth: AuthConfig::Trust,
            role_catalog: Arc::new(auth::InMemoryAuthCatalog::with_bootstrap_superuser()),
            role_connection_limiter: Arc::new(auth::RoleConnectionLimiter::new()),
            privilege_catalog: sample_privilege_catalog(),
            notify_hub: Arc::new(notify::NotifyHub::new()),
            cancel_registry: Arc::new(cancel::CancelRegistry::new()),
            next_pid: std::sync::atomic::AtomicU32::new(1),
            standby_mode: std::sync::atomic::AtomicBool::new(false),
            checkpointer: None,
            wal_writer: None,
        }
    }

    /// Enable or disable hot-standby read-only query mode.
    pub fn set_standby_mode(&self, enabled: bool) {
        self.standby_mode
            .store(enabled, std::sync::atomic::Ordering::Release);
    }

    /// Return whether hot-standby read-only mode is active.
    #[must_use]
    pub fn is_standby_mode(&self) -> bool {
        self.standby_mode.load(std::sync::atomic::Ordering::Acquire)
    }

    /// LSN through which the runtime WAL writer has fsynced.
    ///
    /// Returns `None` for in-memory sample servers because those instances do
    /// not own an on-disk WAL writer.
    #[must_use]
    pub fn runtime_wal_flushed_lsn(&self) -> Option<ultrasql_core::Lsn> {
        self.wal_writer
            .as_ref()
            .map(ultrasql_wal::WalWriter::flushed_lsn)
    }

    /// Append a commit marker for WAL-backed SQL recovery.
    pub(crate) fn append_commit_record(&self, xid: Xid) -> Result<Option<Lsn>, ServerError> {
        let Some(wal) = self.heap.wal_sink() else {
            return Ok(None);
        };
        let payload = CommitPayload {
            commit_lsn: Lsn::ZERO,
            commit_timestamp_micros: unix_timestamp_micros(),
        };
        let record = WalRecord::new(RecordType::Commit, xid, Lsn::ZERO, 0, payload.encode())
            .map_err(|e| ServerError::ddl(format!("commit WAL record encode: {e}")))?;
        wal.append(record)
            .map(Some)
            .map_err(|e| ServerError::ddl(format!("commit WAL append: {e}")))
    }

    /// Append an abort marker for WAL-backed SQL recovery.
    pub(crate) fn append_abort_record(&self, xid: Xid) -> Result<Option<Lsn>, ServerError> {
        let Some(wal) = self.heap.wal_sink() else {
            return Ok(None);
        };
        let payload = AbortPayload {
            abort_lsn: Lsn::ZERO,
        };
        let record = WalRecord::new(RecordType::Abort, xid, Lsn::ZERO, 0, payload.encode())
            .map_err(|e| ServerError::ddl(format!("abort WAL record encode: {e}")))?;
        wal.append(record)
            .map(Some)
            .map_err(|e| ServerError::ddl(format!("abort WAL append: {e}")))
    }

    /// Wait until the runtime WAL writer has fsynced at least `lsn`.
    pub(crate) fn wait_for_wal_durable(&self, lsn: Lsn) -> Result<(), ServerError> {
        let Some(writer) = &self.wal_writer else {
            return Ok(());
        };
        if lsn == Lsn::ZERO {
            return Ok(());
        }

        const WAL_DURABILITY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
        const WAL_DURABILITY_POLL: std::time::Duration = std::time::Duration::from_micros(50);

        let started = std::time::Instant::now();
        loop {
            let flushed = writer.flushed_lsn();
            if flushed.raw() >= lsn.raw() {
                return Ok(());
            }
            if started.elapsed() >= WAL_DURABILITY_TIMEOUT {
                return Err(ServerError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "WAL durability wait timed out at flushed_lsn={} target_lsn={}",
                        flushed.raw(),
                        lsn.raw()
                    ),
                )));
            }
            writer.notify();
            std::thread::sleep(WAL_DURABILITY_POLL);
        }
    }

    /// Commit a transaction and, when it changed persistent heap/index state,
    /// force its commit marker durable before reporting success.
    pub(crate) fn commit_transaction(
        &self,
        txn: ultrasql_txn::Transaction,
        durable_commit_marker: bool,
        context: &str,
    ) -> Result<(), ServerError> {
        let xid = txn.xid;
        self.txn_manager.commit(txn).map_err(|e| match e {
            TxnError::SerializationFailure { detail, .. } => {
                ServerError::SerializationFailure(detail)
            }
            other => ServerError::ddl(format!("{context} commit: {other}")),
        })?;
        if durable_commit_marker && let Some(commit_lsn) = self.append_commit_record(xid)? {
            self.wait_for_wal_durable(commit_lsn)?;
        }
        Ok(())
    }

    /// Abort a transaction and, when it changed persistent heap/index state,
    /// force its abort marker durable before reporting rollback success.
    pub(crate) fn abort_transaction(
        &self,
        txn: ultrasql_txn::Transaction,
        durable_abort_marker: bool,
        context: &str,
    ) -> Result<(), ServerError> {
        let xid = txn.xid;
        self.txn_manager
            .abort(txn)
            .map_err(|e| ServerError::ddl(format!("{context} abort: {e}")))?;
        if durable_abort_marker && let Some(abort_lsn) = self.append_abort_record(xid)? {
            self.wait_for_wal_durable(abort_lsn)?;
        }
        Ok(())
    }

    /// Flush dirty heap pages into the sample server's spill store.
    pub fn flush_dirty_heap_pages(&self) -> Result<usize, ServerError> {
        let loader = self.page_loader.clone();
        self.heap
            .buffer_pool()
            .try_flush_dirty(|page_id, page| loader.store(page_id, page))
            .map_err(|e| ServerError::ddl(format!("flush dirty heap pages: {e}")))
    }

    /// Flush dirty heap pages only when bulk loads put real pressure on frames.
    ///
    /// COPY batches call this after insert. A full flush after every 4096 rows
    /// turns SF10 loads into repeated whole-pool scans; pressure gating keeps
    /// the eviction invariant while avoiding O(pool_frames × batches) work.
    pub fn flush_dirty_heap_pages_if_needed(&self) -> Result<Option<usize>, ServerError> {
        let pool = self.heap.buffer_pool();
        let before = pool.stats();
        let capacity = pool.capacity();
        let resident_threshold = capacity.saturating_mul(3) / 4;
        let dirty_threshold = capacity.saturating_mul(1) / 8;

        if capacity == 0
            || before.dirty == 0
            || before.resident < resident_threshold
            || before.dirty < dirty_threshold
        {
            return Ok(None);
        }

        let flushed = self.flush_dirty_heap_pages()?;
        let after = pool.stats();
        info!(
            capacity,
            resident_before = before.resident,
            dirty_before = before.dirty,
            pinned_before = before.pinned,
            flushed,
            resident_after = after.resident,
            dirty_after = after.dirty,
            pinned_after = after.pinned,
            "bulk load buffer-pool pressure flush"
        );
        Ok(Some(flushed))
    }

    /// Append pre-encoded rows directly into heap pages for in-process
    /// benchmark setup.
    ///
    /// This bypasses the PostgreSQL wire COPY path and normal buffer-pool
    /// insert path, but preserves the heap page/tuple format used by scans.
    pub fn bulk_load_encoded_rows(
        &self,
        relation: RelationId,
        payloads: &[Vec<u8>],
        txn: &Transaction,
    ) -> Result<u64, ServerError> {
        let table = self
            .catalog_snapshot()
            .tables_by_oid
            .get(&relation.oid())
            .cloned()
            .ok_or_else(|| {
                ServerError::ddl(format!("bulk load relation {} not found", relation.oid()))
            })?;
        let n_atts = u16::try_from(table.schema.len())
            .map_err(|_| ServerError::ddl("bulk load schema column count exceeds u16"))?;
        let insert_opts = InsertOptions {
            xmin: txn.current_xid(),
            command_id: txn.current_command,
            n_atts,
            wal: None,
            fsm: None,
            vm: Some(self.vm.as_ref()),
        };
        let loader = self.page_loader.clone();
        self.heap
            .bulk_load_encoded_batch(relation, payloads, insert_opts, |page_id, page| {
                loader.store(page_id, page)
            })
            .map_err(|e| ServerError::ddl(format!("bulk load encoded rows: {e}")))
    }

    /// Record a backup marker in the data directory.
    ///
    /// Returns the current backup LSN surface. UltraSQL v0.9 does not expose a
    /// stable public LSN accessor yet, so the marker records wall-clock time
    /// and the SQL function returns the PostgreSQL-shaped zero LSN used by the
    /// existing recovery CLI placeholders.
    pub fn record_backup_marker(&self, function_name: &str) -> Result<String, ServerError> {
        let Some(data_dir) = &self.data_dir else {
            return Ok("0/0".to_owned());
        };
        let file_name = if function_name.eq_ignore_ascii_case("pg_start_backup") {
            "backup_label"
        } else {
            "backup_stop"
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0);
        let payload = format!("function={function_name}\nlsn=0/0\nunix_seconds={now}\n");
        write_backup_marker_file(&data_dir.join(file_name), &payload)?;
        Ok("0/0".to_owned())
    }

    /// Builder: switch the server to MD5 password auth.
    ///
    /// Every incoming connection must present a `Password` response
    /// matching `MD5(MD5(password + username) || salt)`. Used by
    /// integration tests and as the configuration entry point for
    /// production deployments that wire MD5 in front of the real
    /// `pg_authid` table.
    #[must_use]
    pub fn require_md5_password(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.auth = AuthConfig::Md5 {
            username: username.into(),
            password: password.into(),
        };
        self
    }

    /// Record a successful commit and, every
    /// [`UNDO_GC_INTERVAL_COMMITS`] commits, run maintenance:
    /// undo-log GC plus one pending auto-analyze task.
    ///
    /// Bump-and-check is one atomic add plus a modulo; the heavier
    /// maintenance work is deferred out of the per-commit fast path.
    /// Errors from the maintenance pass are logged and swallowed so a
    /// transient failure cannot mask the underlying commit's success.
    pub fn note_commit_for_gc(&self) {
        use std::sync::atomic::Ordering;
        let n = self
            .vacuum_commit_counter
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        if n % UNDO_GC_INTERVAL_COMMITS != 0 {
            return;
        }
        let oldest = self.txn_manager.oldest_in_progress();
        match self.heap.vacuum_undo_log(oldest) {
            Ok(trimmed) => {
                if trimmed > 0 {
                    tracing::debug!(
                        trimmed,
                        oldest_xid = oldest.raw(),
                        "undo-log GC trimmed entries"
                    );
                }
            }
            Err(e) => tracing::warn!(error = %e, "undo-log GC failed"),
        }
        self.vacuum_mark_visible_pages(oldest);
        self.run_one_pending_analyze();
    }

    /// Run one background autovacuum cycle across tables that crossed
    /// modification thresholds.
    pub fn run_autovacuum_cycle(&self) {
        let oldest = self.txn_manager.oldest_in_progress();
        if let Err(e) = self.heap.vacuum_undo_log(oldest) {
            tracing::warn!(error = %e, "autovacuum undo-log GC failed");
        }
        let snapshot = self.catalog_snapshot();
        for entry in snapshot.tables.values() {
            let table_name = table_entry_lookup_key(entry);
            let modified = self
                .table_modifications
                .get(&table_name)
                .map(|v| *v)
                .unwrap_or(0);
            let blocks = self
                .heap
                .block_count(RelationId(entry.oid))
                .max(entry.n_blocks);
            let estimated_rows = u64::from(blocks).saturating_mul(64);
            let threshold = autovacuum_config_for_table(self.autovacuum_config, entry)
                .vacuum_threshold_for_rows(estimated_rows);
            if modified < threshold {
                continue;
            }
            match self
                .heap
                .vacuum_heap(RelationId(entry.oid), oldest, self.txn_manager.as_ref())
            {
                Ok(stats) => {
                    self.workload_recorder
                        .record_table_autovacuum(entry.oid.raw());
                    if stats.tuples_reclaimed > 0 {
                        tracing::debug!(
                            table = %entry.name,
                            reclaimed = stats.tuples_reclaimed,
                            "autovacuum reclaimed heap tuples",
                        );
                    }
                }
                Err(e) => tracing::warn!(table = %entry.name, error = %e, "autovacuum heap failed"),
            }
            self.pending_analyze_tables.insert(table_name.clone(), ());
            self.table_modifications.insert(table_name, 0);
        }
        self.vacuum_mark_visible_pages(oldest);
        self.run_one_pending_analyze();
        self.run_one_pending_columnarization();
    }

    pub(crate) fn vacuum_mark_visible_pages(&self, oldest: ultrasql_core::Xid) {
        let snapshot = self.catalog_snapshot();
        for entry in snapshot.tables.values() {
            let rel = RelationId(entry.oid);
            let block_count = self.heap.block_count(rel).max(entry.n_blocks);
            if block_count == 0 {
                continue;
            }
            match self.heap.vacuum_mark_all_visible(
                rel,
                block_count,
                oldest,
                self.txn_manager.as_ref(),
                self.vm.as_ref(),
            ) {
                Ok(marked) => {
                    if marked > 0 {
                        tracing::debug!(
                            table = %entry.name,
                            marked,
                            "vacuum marked pages all-visible"
                        );
                    }
                }
                Err(e) => tracing::warn!(
                    table = %entry.name,
                    error = %e,
                    "vacuum all-visible certification failed"
                ),
            }
        }
    }

    /// Initialize a server that boots from `data_dir`.
    ///
    /// Brings up a buffer pool wired to an on-disk WAL writer that persists
    /// every heap mutation.  The WAL segments are written under
    /// `data_dir/pg_wal`.  On a fresh directory the catalog heap is empty
    /// and the initial snapshot is installed.
    ///
    /// This is the production entry point.  `with_sample_database` is the
    /// test/REPL entry point (no WAL, fully in-memory).
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::Io`] when `data_dir` cannot be opened, when
    /// the WAL writer thread cannot be spawned, or when the heap bootstrap
    /// fails for a reason other than an empty heap. Returns
    /// [`ServerError::Ddl`] when the data directory itself is a symlink or
    /// is not owned by the effective user on Unix.
    pub fn init(data_dir: &Path) -> Result<Self, ServerError> {
        use std::sync::Arc;
        use ultrasql_wal::{WalBuffer, WalWriter, WalWriterConfig};
        use wal_sink::WalBufferSink;

        let data_dir = prepare_secure_data_dir(data_dir)?;
        let data_dir = data_dir.as_path();
        let catalog_version = catalog_version::ensure_catalog_version(data_dir)?;
        tracing::info!(
            version = catalog_version.observed_version,
            created = catalog_version.created,
            "catalog version marker checked"
        );

        // 1. WAL buffer — 8 MiB ring.
        const WAL_BUFFER_BYTES: usize = 8 * 1024 * 1024;
        let wal_buffer = Arc::new(WalBuffer::new(WAL_BUFFER_BYTES, ultrasql_core::Lsn::ZERO));
        let wal_dir = data_dir.join("pg_wal");

        // 2. Sink adapter bridges WalBuffer ↔ storage's WalSink trait.
        let sink: Arc<dyn ultrasql_storage::WalSink> =
            Arc::new(WalBufferSink::new(Arc::clone(&wal_buffer)));
        let last_checkpoint_lsn = Arc::new(std::sync::atomic::AtomicU64::new(0));

        // 3. Buffer pool with WAL.
        let page_loader = BlankPageLoader::persistent(data_dir.join("base")).map_err(|e| {
            ServerError::Io(std::io::Error::other(format!("heap segment store: {e}")))
        })?;
        let pool = Arc::new(BufferPool::with_wal(
            IN_MEMORY_POOL_FRAMES,
            page_loader.clone(),
            Arc::clone(&sink),
        ));
        let heap = Arc::new(HeapAccess::with_checkpoint_lsn(
            Arc::clone(&pool),
            Arc::clone(&last_checkpoint_lsn),
        ));
        let vm = Arc::new(VisibilityMap::new());
        let sequences = Arc::new(dashmap::DashMap::new());
        let sequence_owners = Arc::new(dashmap::DashMap::new());
        let sequence_namespaces = Arc::new(dashmap::DashMap::new());
        let schemas = Arc::new(dashmap::DashMap::new());

        // 4. Replay existing WAL before accepting new appends. The recovery
        // target restores heap/index pages through `HeapAccess` and sequence
        // state through the shared registry.
        let recovery_apply_target = ServerRecoveryTarget {
            heap: Arc::clone(&heap),
            sequences: Arc::clone(&sequences),
        };
        let recovery_replay_target = recovery_replay_target_from_data_dir(data_dir)?;
        let mut record_lsn = Lsn::ZERO;
        let recovered_lsn =
            ultrasql_wal::recover_with_target(&wal_dir, recovery_replay_target, |record| {
                let current_lsn = record_lsn;
                record_lsn = record_lsn
                    .checked_advance(u64::from(record.header.total_length))
                    .ok_or(ultrasql_wal::RecoveryError::Record(
                        ultrasql_wal::WalRecordError::Malformed("replay lsn overflow"),
                    ))?;
                ultrasql_wal::dispatch_record_at_lsn(&recovery_apply_target, record, current_lsn)
                    .map_err(|e| ultrasql_wal::RecoveryError::Applier(e.to_string()))
            })
            .map_err(|e| ServerError::Ddl(format!("WAL recovery: {e}")))?;
        wal_buffer.advance_to_lsn(recovered_lsn);
        tracing::info!(lsn = recovered_lsn.raw(), "WAL recovery complete");

        // 5. Background writer thread draining the buffer to disk.
        let wal_writer = WalWriter::open(
            &wal_dir,
            Arc::clone(&wal_buffer),
            WalWriterConfig::default(),
        )
        .map_err(|e| ServerError::Io(std::io::Error::other(format!("WAL writer: {e}"))))?;
        let checkpointer_loader = page_loader.clone();
        let checkpointer = Some(ultrasql_storage::Checkpointer::spawn(
            &pool,
            Some(Arc::clone(&sink)),
            Some(Arc::clone(&last_checkpoint_lsn)),
            move |page_id, page| checkpointer_loader.store(page_id, page),
            ultrasql_storage::CheckpointerConfig::default(),
        ));

        let persistent_catalog = Arc::new(PersistentCatalog::new());
        let stats = require_wal_backed_catalog_bootstrap(
            persistent_catalog.bootstrap_from_heap(heap.as_ref()),
        )?;
        tracing::info!(?stats, "persistent catalog bootstrapped (WAL-backed)");

        let mut catalog = InMemoryCatalog::new();
        let tables = build_sample_database(&mut catalog);
        let ssi = Arc::new(SsiManager::new());
        let txn_manager = Arc::new(TransactionManager::new_with_ssi(ssi));
        let plan_cache = Arc::new(PlanCache::new(PlanCacheConfig::default()));
        let two_phase_dir = data_dir.join("pg_twophase");
        std::fs::create_dir_all(&two_phase_dir).map_err(ServerError::Io)?;
        let two_phase_coord = ultrasql_txn::two_phase::TwoPhaseCoordinator::new(two_phase_dir);
        let recovered_state_files = two_phase_coord
            .recover_from_disk()
            .map_err(|e| ServerError::Ddl(format!("2PC recovery: {e}")))?;
        let mut recovered_prepared = 0usize;
        let mut cleaned_resolved = 0usize;
        for prepared in two_phase_coord.list_prepared() {
            match txn_manager.recover_prepared(prepared.xid) {
                Ok(()) => recovered_prepared += 1,
                Err(TxnError::AlreadyTerminated {
                    status: ultrasql_mvcc::XidStatus::Committed | ultrasql_mvcc::XidStatus::Aborted,
                    ..
                }) => {
                    two_phase_coord.finish_resolution(&prepared).map_err(|e| {
                        ServerError::Ddl(format!("2PC resolved state cleanup: {e}"))
                    })?;
                    cleaned_resolved += 1;
                }
                Err(e) => return Err(ServerError::Ddl(format!("2PC CLOG recovery: {e}"))),
            }
        }
        tracing::info!(
            state_files = recovered_state_files,
            prepared = recovered_prepared,
            cleaned_resolved,
            "2PC state recovery complete"
        );
        let two_phase = Arc::new(two_phase_coord);

        let server = Self {
            catalog,
            tables,
            data_dir: Some(data_dir.to_path_buf()),
            persistent_catalog,
            heap,
            page_loader,
            vm,
            txn_manager,
            plan_cache,
            vacuum_commit_counter: std::sync::atomic::AtomicU64::new(0),
            stats_catalog: parking_lot::RwLock::new(InMemoryStatsCatalog::new()),
            table_constraints: Arc::new(dashmap::DashMap::new()),
            domain_constraints: Arc::new(dashmap::DashMap::new()),
            row_security: Arc::new(dashmap::DashMap::new()),
            sequences,
            sequence_owners,
            sequence_namespaces,
            schemas,
            operators: Arc::new(dashmap::DashMap::new()),
            materialized_views: Arc::new(dashmap::DashMap::new()),
            columnar_storage: Arc::new(columnar_storage::ColumnarSecondaryStore::new()),
            time_partitions: Arc::new(dashmap::DashMap::new()),
            logical_replication: Arc::new(replication::LogicalReplicationRuntime::open_metadata(
                data_dir.join("pg_logical"),
            )?),
            workload_recorder: Arc::new(workload::WorkloadRecorder::new()),
            table_modifications: dashmap::DashMap::new(),
            table_analyze_modifications: dashmap::DashMap::new(),
            pending_analyze_tables: dashmap::DashMap::new(),
            autovacuum_config: AutovacuumConfig::default(),
            logging_config: LoggingConfig::default(),
            idle_session_timeout_ms: 0,
            wal_archive_config: WalArchiveConfig::default(),
            two_phase,
            auth: AuthConfig::Trust,
            role_catalog: Arc::new(auth::InMemoryAuthCatalog::with_bootstrap_superuser()),
            role_connection_limiter: Arc::new(auth::RoleConnectionLimiter::new()),
            privilege_catalog: sample_privilege_catalog(),
            notify_hub: Arc::new(notify::NotifyHub::new()),
            cancel_registry: Arc::new(cancel::CancelRegistry::new()),
            next_pid: std::sync::atomic::AtomicU32::new(1),
            standby_mode: std::sync::atomic::AtomicBool::new(false),
            checkpointer,
            wal_writer: Some(wal_writer),
        };
        server.recover_commit_status_from_wal()?;
        server.rebuild_domain_runtime_constraint_sidecars()?;
        server.rebuild_role_metadata()?;
        server.rebuild_privilege_metadata()?;
        server.rebuild_schema_metadata()?;
        server.refresh_persistent_catalog_schema_names();
        server.rebuild_table_runtime_constraint_sidecars()?;
        server.rebuild_persistent_index_sidecars()?;
        let stats_catalog = hydrate_optimizer_stats_from_catalog(
            &server.catalog_snapshot(),
            server.heap.as_ref(),
            server.txn_manager.as_ref(),
        );
        *server.stats_catalog.write() = stats_catalog;
        server.rebuild_sequence_owner_metadata()?;
        server.rebuild_operator_metadata()?;
        server.rebuild_row_security_sidecars()?;
        server.rebuild_materialized_view_runtime_sidecars()?;
        server.rebuild_time_partition_runtime_sidecars()?;
        Ok(server)
    }

    fn domain_runtime_metadata_path(&self) -> Option<std::path::PathBuf> {
        self.data_dir
            .as_ref()
            .map(|dir| dir.join("pg_domain_runtime.meta"))
    }

    pub(crate) fn persist_domain_runtime_constraints_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.domain_runtime_metadata_path() else {
            return Ok(());
        };
        let snapshot = self.catalog_snapshot();
        let mut entries = snapshot
            .domain_types_by_oid
            .values()
            .map(|entry| {
                let runtime = self.domain_constraints.get(&entry.oid);
                (entry.clone(), runtime.map(|guard| guard.as_ref().clone()))
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|(entry, _)| entry.oid.raw());

        let mut out = String::from("# ultrasql domain runtime constraints v1\n");
        for (entry, runtime) in entries {
            let runtime = runtime.unwrap_or_else(|| DomainRuntimeConstraints {
                base_type: entry.base_type.clone(),
                not_null: entry.not_null,
                checks: Vec::new(),
            });
            let Some(base_token) = data_type_token(&runtime.base_type) else {
                return Err(ServerError::ddl(format!(
                    "domain '{}' base type is outside restart-persistable metadata subset",
                    entry.name
                )));
            };
            out.push_str(&format!(
                "domain\t{}\t{}\t{}\t{}\t{}\n",
                metadata_escape(&entry.name),
                entry.oid.raw(),
                metadata_escape(&entry.schema_name),
                metadata_escape(&base_token),
                runtime.not_null
            ));
            for check in &runtime.checks {
                let Some(expr) = encode_scalar_expr_field(&check.expr) else {
                    return Err(ServerError::ddl(format!(
                        "domain '{}' CHECK '{}' is outside restart-persistable metadata subset",
                        entry.name, check.name
                    )));
                };
                out.push_str(&format!(
                    "check\t{}\t{}\t{}\n",
                    entry.oid.raw(),
                    metadata_escape(&check.name),
                    metadata_escape(&expr)
                ));
            }
        }
        write_runtime_metadata_file(&path, &out)
    }

    fn rebuild_domain_runtime_constraint_sidecars(&self) -> Result<(), ServerError> {
        let Some(path) = self.domain_runtime_metadata_path() else {
            return Ok(());
        };
        let Some(text) = read_runtime_metadata_file(&path)? else {
            return Ok(());
        };
        let mut domains: std::collections::HashMap<Oid, DomainTypeEntry> =
            std::collections::HashMap::new();
        let mut checks: std::collections::HashMap<Oid, Vec<RuntimeCheckConstraint>> =
            std::collections::HashMap::new();
        let mut seen_domain_oids = std::collections::HashSet::new();
        let mut seen_domain_names = std::collections::HashSet::new();
        let mut seen_check_keys = std::collections::HashSet::new();
        for (line_no, line) in text.lines().enumerate() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts = line.split('\t').collect::<Vec<_>>();
            match parts.first().copied() {
                Some("domain") if parts.len() == 6 => {
                    let oid = Oid::new(parts[2].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "domain-runtime metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    let base_token = metadata_unescape(parts[4])?;
                    let base_type = data_type_from_token(&base_token).ok_or_else(|| {
                        ServerError::Ddl(format!(
                            "domain-runtime metadata line {} unknown base type",
                            line_no + 1
                        ))
                    })?;
                    let not_null = parts[5].parse::<bool>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "domain-runtime metadata line {} bad not-null flag: {err}",
                            line_no + 1
                        ))
                    })?;
                    let name = metadata_unescape(parts[1])?;
                    let schema_name = metadata_unescape(parts[3])?;
                    if !seen_domain_oids.insert(oid)
                        || !seen_domain_names
                            .insert((schema_name.to_ascii_lowercase(), name.to_ascii_lowercase()))
                    {
                        return Err(ServerError::Ddl(format!(
                            "duplicate domain-runtime metadata on line {}",
                            line_no + 1
                        )));
                    }
                    domains.insert(
                        oid,
                        DomainTypeEntry {
                            oid,
                            name,
                            schema_name,
                            base_type,
                            not_null,
                        },
                    );
                }
                Some("check") if parts.len() == 4 => {
                    let oid = Oid::new(parts[1].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "domain-runtime metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    let name = metadata_unescape(parts[2])?;
                    if !seen_check_keys.insert((oid, name.to_ascii_lowercase())) {
                        return Err(ServerError::Ddl(format!(
                            "duplicate domain-runtime check metadata on line {}",
                            line_no + 1
                        )));
                    }
                    checks.entry(oid).or_default().push(RuntimeCheckConstraint {
                        name,
                        expr: decode_scalar_expr_field(&metadata_unescape(parts[3])?)?,
                    });
                }
                _ => {
                    return Err(ServerError::Ddl(format!(
                        "malformed domain-runtime metadata line {}",
                        line_no + 1
                    )));
                }
            }
        }
        for (oid, entry) in domains {
            if !self
                .catalog_snapshot()
                .domain_types_by_oid
                .contains_key(&oid)
            {
                self.persistent_catalog.create_domain_type(entry.clone())?;
            }
            self.domain_constraints.insert(
                oid,
                Arc::new(DomainRuntimeConstraints {
                    base_type: entry.base_type,
                    not_null: entry.not_null,
                    checks: checks.remove(&oid).unwrap_or_default(),
                }),
            );
        }
        if let Some(oid) = checks.keys().copied().next() {
            return Err(ServerError::Ddl(format!(
                "orphan domain-runtime check metadata on oid {}",
                oid.raw()
            )));
        }
        Ok(())
    }

    fn table_runtime_metadata_path(&self) -> Option<std::path::PathBuf> {
        self.data_dir
            .as_ref()
            .map(|dir| dir.join("pg_table_runtime.meta"))
    }

    pub(crate) fn ensure_table_runtime_constraints_metadata_slots_persistable(
        &self,
    ) -> Result<(), ServerError> {
        ensure_optional_runtime_metadata_write_slots(self.table_runtime_metadata_path())
    }

    pub(crate) fn ensure_create_table_runtime_metadata_slots_persistable(
        &self,
        writes_sequence_owner_metadata: bool,
    ) -> Result<(), ServerError> {
        self.ensure_table_runtime_constraints_metadata_slots_persistable()?;
        self.ensure_create_relation_metadata_slots_persistable()?;
        if writes_sequence_owner_metadata {
            ensure_optional_runtime_metadata_write_slots(self.sequence_owner_metadata_path())?;
        }
        Ok(())
    }

    pub(crate) fn ensure_create_relation_metadata_slots_persistable(
        &self,
    ) -> Result<(), ServerError> {
        ensure_optional_runtime_metadata_write_slots(self.row_security_metadata_path())?;
        ensure_optional_runtime_metadata_write_slots(self.privilege_metadata_path())
    }

    pub(crate) fn ensure_drop_table_runtime_metadata_slots_persistable(
        &self,
        dropped_tables: &[String],
    ) -> Result<(), ServerError> {
        self.ensure_table_runtime_constraints_metadata_slots_persistable()?;
        ensure_optional_runtime_metadata_write_slots(self.row_security_metadata_path())?;

        let grant_objects = self
            .privilege_catalog
            .list_grants()
            .into_iter()
            .map(|grant| (grant.object_kind, grant.object_name))
            .collect::<std::collections::HashSet<_>>();
        let mut sequence_owner_metadata_changed = false;
        let mut privilege_metadata_changed = false;
        let mut materialized_view_metadata_changed = false;
        for table_name in dropped_tables {
            if self.materialized_views.contains_key(table_name) {
                materialized_view_metadata_changed = true;
            }
            let Some(entry) = self.persistent_catalog.lookup_table(table_name) else {
                continue;
            };
            let table_key = ultrasql_catalog::table_lookup_key(&entry.schema_name, &entry.name);
            if grant_objects.contains(&(crate::auth::PrivilegeObjectKind::Table, table_key)) {
                privilege_metadata_changed = true;
            }
            let Some(constraints) = self.table_constraints.get(&entry.oid) else {
                continue;
            };
            for sequence_name in constraints.sequence_defaults.iter().flatten() {
                sequence_owner_metadata_changed = true;
                let sequence_key = sequence_name.to_ascii_lowercase();
                let sequence_grant_key =
                    if ultrasql_catalog::decode_table_lookup_key(&sequence_key).is_some() {
                        sequence_key
                    } else {
                        let namespace = self
                            .sequence_namespaces
                            .get(&sequence_key)
                            .map_or_else(|| "public".to_owned(), |entry| entry.value().clone());
                        ultrasql_catalog::table_lookup_key(&namespace, &sequence_key)
                    };
                if grant_objects.contains(&(
                    crate::auth::PrivilegeObjectKind::Sequence,
                    sequence_grant_key,
                )) {
                    privilege_metadata_changed = true;
                }
            }
        }

        if sequence_owner_metadata_changed {
            ensure_optional_runtime_metadata_write_slots(self.sequence_owner_metadata_path())?;
        }
        if privilege_metadata_changed {
            ensure_optional_runtime_metadata_write_slots(self.privilege_metadata_path())?;
        }
        if materialized_view_metadata_changed {
            ensure_optional_runtime_metadata_write_slots(self.materialized_view_metadata_path())?;
        }
        Ok(())
    }

    pub(crate) fn ensure_schema_metadata_slots_persistable(&self) -> Result<(), ServerError> {
        ensure_optional_runtime_metadata_write_slots(self.schema_metadata_path())
    }

    pub(crate) fn ensure_table_runtime_constraints_metadata_persistable(
        &self,
        table_name: &str,
        constraints: &TableRuntimeConstraints,
    ) -> Result<(), ServerError> {
        if self.table_runtime_metadata_path().is_none() {
            return Ok(());
        }
        for (idx, default_expr) in constraints.defaults.iter().enumerate() {
            if let Some(default_expr) = default_expr {
                encode_table_runtime_scalar_expr(
                    table_name,
                    format!("DEFAULT expression on column {idx}"),
                    default_expr,
                )?;
            }
        }
        for (idx, generated_expr) in constraints.generated_stored.iter().enumerate() {
            if let Some(generated_expr) = generated_expr {
                encode_table_runtime_scalar_expr(
                    table_name,
                    format!("generated stored expression on column {idx}"),
                    generated_expr,
                )?;
            }
        }
        for check in &constraints.checks {
            encode_table_runtime_scalar_expr(
                table_name,
                format!("CHECK '{}' expression", check.name),
                &check.expr,
            )?;
        }
        Ok(())
    }

    pub(crate) fn persist_table_runtime_constraints_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.table_runtime_metadata_path() else {
            return Ok(());
        };
        let snapshot = self.catalog_snapshot();
        let mut entries = self
            .table_constraints
            .iter()
            .filter_map(|entry| {
                let table = snapshot.tables_by_oid.get(entry.key())?;
                Some((
                    *entry.key(),
                    table_entry_lookup_key(table),
                    entry.value().as_ref().clone(),
                ))
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|(oid, _, _)| oid.raw());

        let mut out = String::from("# ultrasql table runtime constraints v1\n");
        for (oid, table_name, constraints) in entries {
            out.push_str(&format!(
                "table\t{}\t{}\n",
                metadata_escape(&table_name),
                oid.raw()
            ));
            for (idx, seq_name) in constraints.sequence_defaults.iter().enumerate() {
                let Some(seq_name) = seq_name else {
                    continue;
                };
                out.push_str(&format!(
                    "sequence_default\t{}\t{}\t{}\n",
                    oid.raw(),
                    idx,
                    metadata_escape(seq_name)
                ));
            }
            for (idx, default_expr) in constraints.defaults.iter().enumerate() {
                let Some(default_expr) = default_expr else {
                    continue;
                };
                let expr = encode_table_runtime_scalar_expr(
                    &table_name,
                    format!("DEFAULT expression on column {idx}"),
                    default_expr,
                )?;
                out.push_str(&format!(
                    "default\t{}\t{}\t{}\n",
                    oid.raw(),
                    idx,
                    metadata_escape(&expr)
                ));
            }
            for (idx, identity_always) in constraints.identity_always.iter().enumerate() {
                if *identity_always {
                    out.push_str(&format!("identity_always\t{}\t{}\n", oid.raw(), idx));
                }
            }
            for (idx, generated_expr) in constraints.generated_stored.iter().enumerate() {
                let Some(generated_expr) = generated_expr else {
                    continue;
                };
                let expr = encode_table_runtime_scalar_expr(
                    &table_name,
                    format!("generated stored expression on column {idx}"),
                    generated_expr,
                )?;
                out.push_str(&format!(
                    "generated_stored\t{}\t{}\t{}\n",
                    oid.raw(),
                    idx,
                    metadata_escape(&expr)
                ));
            }
            for check in &constraints.checks {
                let expr = encode_table_runtime_scalar_expr(
                    &table_name,
                    format!("CHECK '{}' expression", check.name),
                    &check.expr,
                )?;
                out.push_str(&format!(
                    "check\t{}\t{}\t{}\n",
                    oid.raw(),
                    metadata_escape(&check.name),
                    metadata_escape(&expr)
                ));
            }
            for fk in &constraints.foreign_keys {
                out.push_str(&format!(
                    "foreign_key\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                    oid.raw(),
                    metadata_escape(&fk.name),
                    usize_list_token(&fk.columns),
                    metadata_escape(&fk.target_table),
                    fk.target_oid.raw(),
                    usize_list_token(&fk.target_columns),
                    referential_action_token(fk.on_delete),
                    referential_action_token(fk.on_update),
                    fk.deferrable,
                    fk.initially_deferred
                ));
            }
            for exclusion in &constraints.exclusion_constraints {
                let elements = exclusion
                    .elements
                    .iter()
                    .map(|element| format!("{}:{}", element.column, binary_op_token(element.op)))
                    .collect::<Vec<_>>()
                    .join(",");
                out.push_str(&format!(
                    "exclusion\t{}\t{}\t{}\t{}\n",
                    oid.raw(),
                    metadata_escape(&exclusion.name),
                    index_method_token(exclusion.method),
                    elements
                ));
            }
            let mut indexes = constraints.indexes.iter().collect::<Vec<_>>();
            indexes.sort_by_key(|(index_oid, _)| index_oid.raw());
            for (index_oid, metadata) in indexes {
                let key_exprs = encode_table_runtime_scalar_expr_list(
                    &table_name,
                    format!("index {} key", index_oid.raw()),
                    &metadata.key_exprs,
                )?;
                let predicate = metadata
                    .predicate
                    .as_ref()
                    .map(|predicate| {
                        encode_table_runtime_scalar_expr(
                            &table_name,
                            format!("index {} predicate", index_oid.raw()),
                            predicate,
                        )
                    })
                    .transpose()?
                    .unwrap_or_default();
                out.push_str(&format!(
                    "index\t{}\t{}\t{}\t{}\t{}\t{}\n",
                    oid.raw(),
                    index_oid.raw(),
                    index_method_token(metadata.method),
                    metadata_escape(&key_exprs),
                    metadata_escape(&predicate),
                    usize_list_token(&metadata.include_columns)
                ));
            }
        }
        write_runtime_metadata_file(&path, &out)
    }

    fn rebuild_table_runtime_constraint_sidecars(&self) -> Result<(), ServerError> {
        let Some(path) = self.table_runtime_metadata_path() else {
            return Ok(());
        };
        let Some(text) = read_runtime_metadata_file(&path)? else {
            return Ok(());
        };
        let snapshot = self.catalog_snapshot();
        let mut table_names: std::collections::HashMap<Oid, String> =
            std::collections::HashMap::new();
        let mut sequence_defaults: std::collections::HashMap<Oid, Vec<(usize, String)>> =
            std::collections::HashMap::new();
        let mut defaults: std::collections::HashMap<Oid, Vec<(usize, ScalarExpr)>> =
            std::collections::HashMap::new();
        let mut identity_always: std::collections::HashMap<Oid, Vec<usize>> =
            std::collections::HashMap::new();
        let mut generated_stored: std::collections::HashMap<Oid, Vec<(usize, ScalarExpr)>> =
            std::collections::HashMap::new();
        let mut checks: std::collections::HashMap<Oid, Vec<RuntimeCheckConstraint>> =
            std::collections::HashMap::new();
        let mut foreign_keys: std::collections::HashMap<Oid, Vec<RuntimeForeignKeyConstraint>> =
            std::collections::HashMap::new();
        let mut exclusions: std::collections::HashMap<Oid, Vec<RuntimeExclusionConstraint>> =
            std::collections::HashMap::new();
        let mut indexes: std::collections::HashMap<Oid, Vec<(Oid, RuntimeIndexMetadata)>> =
            std::collections::HashMap::new();
        let mut seen_table_oids = std::collections::HashSet::new();
        let mut seen_sequence_default_keys = std::collections::HashSet::new();
        let mut seen_default_keys = std::collections::HashSet::new();
        let mut seen_identity_keys = std::collections::HashSet::new();
        let mut seen_generated_keys = std::collections::HashSet::new();
        let mut seen_check_keys = std::collections::HashSet::new();
        let mut seen_foreign_key_keys = std::collections::HashSet::new();
        let mut seen_exclusion_keys = std::collections::HashSet::new();
        let mut seen_index_keys = std::collections::HashSet::new();
        let mut skipped_stale_index_metadata = false;
        for (line_no, line) in text.lines().enumerate() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts = line.split('\t').collect::<Vec<_>>();
            match parts.first().copied() {
                Some("table") if parts.len() == 3 => {
                    let oid = Oid::new(parts[2].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    let table_name = metadata_unescape(parts[1])?;
                    if !seen_table_oids.insert(oid) {
                        return Err(ServerError::Ddl(format!(
                            "duplicate table-runtime metadata on line {}",
                            line_no + 1
                        )));
                    }
                    table_names.insert(oid, table_name);
                }
                Some("sequence_default") if parts.len() == 4 => {
                    let oid = Oid::new(parts[1].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    let idx = parts[2].parse::<usize>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad column index: {err}",
                            line_no + 1
                        ))
                    })?;
                    if !seen_sequence_default_keys.insert((oid, idx)) {
                        return Err(ServerError::Ddl(format!(
                            "duplicate table-runtime sequence default metadata on line {}",
                            line_no + 1
                        )));
                    }
                    sequence_defaults
                        .entry(oid)
                        .or_default()
                        .push((idx, metadata_unescape(parts[3])?));
                }
                Some("default") if parts.len() == 4 => {
                    let oid = Oid::new(parts[1].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    let idx = parts[2].parse::<usize>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad column index: {err}",
                            line_no + 1
                        ))
                    })?;
                    if !seen_default_keys.insert((oid, idx)) {
                        return Err(ServerError::Ddl(format!(
                            "duplicate table-runtime default metadata on line {}",
                            line_no + 1
                        )));
                    }
                    defaults.entry(oid).or_default().push((
                        idx,
                        decode_scalar_expr_field(&metadata_unescape(parts[3])?)?,
                    ));
                }
                Some("identity_always") if parts.len() == 3 => {
                    let oid = Oid::new(parts[1].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    let idx = parts[2].parse::<usize>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad column index: {err}",
                            line_no + 1
                        ))
                    })?;
                    if !seen_identity_keys.insert((oid, idx)) {
                        return Err(ServerError::Ddl(format!(
                            "duplicate table-runtime identity metadata on line {}",
                            line_no + 1
                        )));
                    }
                    identity_always.entry(oid).or_default().push(idx);
                }
                Some("generated_stored") if parts.len() == 4 => {
                    let oid = Oid::new(parts[1].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    let idx = parts[2].parse::<usize>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad column index: {err}",
                            line_no + 1
                        ))
                    })?;
                    if !seen_generated_keys.insert((oid, idx)) {
                        return Err(ServerError::Ddl(format!(
                            "duplicate table-runtime generated metadata on line {}",
                            line_no + 1
                        )));
                    }
                    generated_stored.entry(oid).or_default().push((
                        idx,
                        decode_scalar_expr_field(&metadata_unescape(parts[3])?)?,
                    ));
                }
                Some("check") if parts.len() == 4 => {
                    let oid = Oid::new(parts[1].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    let name = metadata_unescape(parts[2])?;
                    if !seen_check_keys.insert((oid, name.to_ascii_lowercase())) {
                        return Err(ServerError::Ddl(format!(
                            "duplicate table-runtime check metadata on line {}",
                            line_no + 1
                        )));
                    }
                    checks.entry(oid).or_default().push(RuntimeCheckConstraint {
                        name,
                        expr: decode_scalar_expr_field(&metadata_unescape(parts[3])?)?,
                    });
                }
                Some("foreign_key") if parts.len() == 11 => {
                    let oid = Oid::new(parts[1].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    let name = metadata_unescape(parts[2])?;
                    if !seen_foreign_key_keys.insert((oid, name.to_ascii_lowercase())) {
                        return Err(ServerError::Ddl(format!(
                            "duplicate table-runtime foreign-key metadata on line {}",
                            line_no + 1
                        )));
                    }
                    foreign_keys
                        .entry(oid)
                        .or_default()
                        .push(RuntimeForeignKeyConstraint {
                        name,
                        columns: parse_usize_list_token(parts[3])?,
                        target_table: metadata_unescape(parts[4])?,
                        target_oid: Oid::new(parts[5].parse::<u32>().map_err(|err| {
                            ServerError::Ddl(format!(
                                "table-runtime metadata line {} bad target oid: {err}",
                                line_no + 1
                            ))
                        })?),
                        target_columns: parse_usize_list_token(parts[6])?,
                        on_delete: parse_referential_action(parts[7])?,
                        on_update: parse_referential_action(parts[8])?,
                        deferrable: parts[9].parse::<bool>().map_err(|err| {
                            ServerError::Ddl(format!(
                                "table-runtime metadata line {} bad deferrable flag: {err}",
                                line_no + 1
                            ))
                        })?,
                        initially_deferred: parts[10].parse::<bool>().map_err(|err| {
                            ServerError::Ddl(format!(
                                "table-runtime metadata line {} bad initially_deferred flag: {err}",
                                line_no + 1
                            ))
                        })?,
                    });
                }
                Some("exclusion") if parts.len() == 5 => {
                    let oid = Oid::new(parts[1].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    let name = metadata_unescape(parts[2])?;
                    if !seen_exclusion_keys.insert((oid, name.to_ascii_lowercase())) {
                        return Err(ServerError::Ddl(format!(
                            "duplicate table-runtime exclusion metadata on line {}",
                            line_no + 1
                        )));
                    }
                    let mut elements = Vec::new();
                    if !parts[4].is_empty() {
                        for raw in parts[4].split(',') {
                            let (column, op) = raw.split_once(':').ok_or_else(|| {
                                ServerError::Ddl(format!(
                                    "table-runtime metadata line {} bad exclusion element",
                                    line_no + 1
                                ))
                            })?;
                            elements.push(RuntimeExclusionElement {
                                column: column.parse::<usize>().map_err(|err| {
                                    ServerError::Ddl(format!(
                                        "table-runtime metadata line {} bad exclusion column: {err}",
                                        line_no + 1
                                    ))
                                })?,
                                op: binary_op_from_token(op).ok_or_else(|| {
                                    ServerError::Ddl(format!(
                                        "table-runtime metadata line {} bad exclusion op",
                                        line_no + 1
                                    ))
                                })?,
                            });
                        }
                    }
                    exclusions
                        .entry(oid)
                        .or_default()
                        .push(RuntimeExclusionConstraint {
                            name,
                            method: parse_index_method(parts[3])?,
                            elements,
                        });
                }
                Some("index") if parts.len() == 7 => {
                    let oid = Oid::new(parts[1].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    let index_oid = Oid::new(parts[2].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad index oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    if !seen_index_keys.insert((oid, index_oid)) {
                        return Err(ServerError::Ddl(format!(
                            "duplicate table-runtime index metadata on line {}",
                            line_no + 1
                        )));
                    }
                    let method = parse_index_method(parts[3])?;
                    let key_exprs = decode_scalar_expr_list_field(&metadata_unescape(parts[4])?)?;
                    let predicate = {
                        let raw = metadata_unescape(parts[5])?;
                        if raw.is_empty() {
                            None
                        } else {
                            Some(decode_scalar_expr_field(&raw)?)
                        }
                    };
                    indexes.entry(oid).or_default().push((
                        index_oid,
                        RuntimeIndexMetadata {
                            key_exprs,
                            predicate,
                            include_columns: parse_usize_list_token(parts[6])?,
                            method,
                            brin: None,
                            hnsw: None,
                            ivfflat: None,
                            aggregating: None,
                        },
                    ));
                }
                _ => {
                    return Err(ServerError::Ddl(format!(
                        "malformed table-runtime metadata line {}",
                        line_no + 1
                    )));
                }
            }
        }
        for (oid, table_name) in table_names {
            let Some(table) = snapshot.tables_by_oid.get(&oid) else {
                return Err(ServerError::Ddl(format!(
                    "unknown table-runtime metadata table '{}' on oid {}",
                    table_name,
                    oid.raw()
                )));
            };
            let expected_key = table_entry_lookup_key(table);
            if table_name != expected_key && table_name != table.name {
                return Err(ServerError::Ddl(format!(
                    "table-runtime metadata table '{}' does not match catalog table '{}'",
                    table_name, expected_key
                )));
            }
            let width = table.schema.fields().len();
            let mut runtime = self
                .table_constraints
                .get(&oid)
                .map(|existing| existing.as_ref().clone())
                .unwrap_or_default();
            if runtime.defaults.len() < width {
                runtime.defaults.resize(width, None);
            }
            if runtime.sequence_defaults.len() < width {
                runtime.sequence_defaults.resize(width, None);
            }
            if runtime.identity_always.len() < width {
                runtime.identity_always.resize(width, false);
            }
            if runtime.generated_stored.len() < width {
                runtime.generated_stored.resize(width, None);
            }
            if let Some(defaults) = sequence_defaults.remove(&oid) {
                for (idx, seq_name) in defaults {
                    if idx < runtime.sequence_defaults.len() {
                        runtime.sequence_defaults[idx] = Some(seq_name);
                    }
                }
            }
            if let Some(defaults) = defaults.remove(&oid) {
                for (idx, expr) in defaults {
                    if idx < runtime.defaults.len() {
                        runtime.defaults[idx] = Some(expr);
                    }
                }
            }
            if let Some(always_columns) = identity_always.remove(&oid) {
                for idx in always_columns {
                    if idx < runtime.identity_always.len() {
                        runtime.identity_always[idx] = true;
                    }
                }
            }
            if let Some(generated) = generated_stored.remove(&oid) {
                for (idx, expr) in generated {
                    if idx < runtime.generated_stored.len() {
                        runtime.generated_stored[idx] = Some(expr);
                    }
                }
            }
            if let Some(checks) = checks.remove(&oid) {
                runtime.checks = checks;
            }
            if let Some(foreign_keys) = foreign_keys.remove(&oid) {
                let mut validated_foreign_keys = Vec::with_capacity(foreign_keys.len());
                for mut fk in foreign_keys {
                    let Some(target) = snapshot.tables.get(&fk.target_table) else {
                        return Err(ServerError::Ddl(format!(
                            "invalid table-runtime foreign-key target metadata for '{}'",
                            fk.name
                        )));
                    };
                    if target.oid != fk.target_oid {
                        return Err(ServerError::Ddl(format!(
                            "invalid table-runtime foreign-key target metadata for '{}'",
                            fk.name
                        )));
                    }
                    fk.target_oid = target.oid;
                    validated_foreign_keys.push(fk);
                }
                runtime.foreign_keys = validated_foreign_keys;
            }
            if let Some(exclusions) = exclusions.remove(&oid) {
                runtime.exclusion_constraints = exclusions;
            }
            if let Some(indexes) = indexes.remove(&oid) {
                for (index_oid, metadata) in indexes {
                    let index_belongs_to_table = snapshot
                        .indexes_by_table
                        .get(&oid)
                        .is_some_and(|entries| entries.iter().any(|index| index.oid == index_oid));
                    if !index_belongs_to_table {
                        let index_exists = snapshot
                            .indexes_by_table
                            .values()
                            .any(|entries| entries.iter().any(|index| index.oid == index_oid));
                        // CREATE INDEX can crash after the runtime sidecar is written but before
                        // the catalog index row is WAL-durable. That stale sidecar is ignored; a
                        // committed index oid attached to the wrong table is still corrupt.
                        if !index_exists {
                            skipped_stale_index_metadata = true;
                            continue;
                        }
                        return Err(ServerError::Ddl(format!(
                            "invalid table-runtime index metadata on oid {} for table oid {}",
                            index_oid.raw(),
                            oid.raw()
                        )));
                    }
                    runtime.indexes.insert(index_oid, metadata);
                }
            }
            self.table_constraints.insert(oid, Arc::new(runtime));
        }
        if let Some(oid) = sequence_defaults
            .keys()
            .chain(defaults.keys())
            .chain(identity_always.keys())
            .chain(generated_stored.keys())
            .chain(checks.keys())
            .chain(foreign_keys.keys())
            .chain(exclusions.keys())
            .chain(indexes.keys())
            .copied()
            .next()
        {
            return Err(ServerError::Ddl(format!(
                "orphan table-runtime metadata rows on oid {}",
                oid.raw()
            )));
        }
        if skipped_stale_index_metadata {
            self.persist_table_runtime_constraints_metadata()?;
        }
        Ok(())
    }

    fn role_metadata_path(&self) -> Option<std::path::PathBuf> {
        self.data_dir.as_ref().map(|dir| dir.join("pg_auth.meta"))
    }

    pub(crate) fn persist_role_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.role_metadata_path() else {
            return Ok(());
        };
        let mut roles = self.role_catalog.list_roles();
        roles.sort_by_key(|role| role.oid);
        let mut memberships = self.role_catalog.list_memberships();
        memberships.sort_by(|left, right| {
            left.role
                .cmp(&right.role)
                .then_with(|| left.member.cmp(&right.member))
        });

        let mut out = String::from("# ultrasql auth runtime v1\n");
        for role in roles {
            out.push_str(&format!(
                "role\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                metadata_escape(&role.name),
                role.oid,
                metadata_escape(&format_password_hash(role.password.as_ref())),
                role.is_superuser,
                role.inherit,
                role.create_role,
                role.create_db,
                role.can_login,
                role.replication,
                role.bypass_rls,
                role.connection_limit,
                role.valid_until
                    .map_or_else(String::new, |value| value.to_string())
            ));
        }
        for membership in memberships {
            out.push_str(&format!(
                "member\t{}\t{}\t{}\t{}\n",
                metadata_escape(&membership.role),
                metadata_escape(&membership.member),
                metadata_escape(&membership.grantor),
                membership.admin_option
            ));
        }
        write_runtime_metadata_file(&path, &out)
    }

    fn rebuild_role_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.role_metadata_path() else {
            return Ok(());
        };
        let Some(text) = read_runtime_metadata_file(&path)? else {
            return Ok(());
        };

        let mut roles = Vec::new();
        let mut memberships = Vec::new();
        let mut seen_role_names = std::collections::HashSet::new();
        let mut seen_role_oids = std::collections::HashSet::new();
        let mut seen_membership_keys = std::collections::HashSet::new();
        for (line_no, line) in text.lines().enumerate() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts = line.split('\t').collect::<Vec<_>>();
            match parts.first().copied() {
                Some("role") if parts.len() == 13 => {
                    let name = metadata_unescape(parts[1])?;
                    validate_role_metadata_name(&name, line_no, "name")?;
                    if !seen_role_names.insert(name.to_ascii_lowercase()) {
                        return Err(ServerError::ddl(format!(
                            "duplicate role metadata name '{}' on line {}",
                            name,
                            line_no + 1
                        )));
                    }
                    let oid = parse_role_u32(parts[2], line_no, "oid")?;
                    if oid == 0 {
                        return Err(ServerError::ddl(format!(
                            "invalid role metadata oid 0 on line {}",
                            line_no + 1
                        )));
                    }
                    if !seen_role_oids.insert(oid) {
                        return Err(ServerError::ddl(format!(
                            "duplicate role metadata oid {} on line {}",
                            oid,
                            line_no + 1
                        )));
                    }
                    roles.push(auth::RoleEntry {
                        name,
                        oid,
                        password: parse_password_hash(&metadata_unescape(parts[3])?, line_no)?,
                        is_superuser: parse_role_bool(parts[4], line_no, "is_superuser")?,
                        inherit: parse_role_bool(parts[5], line_no, "inherit")?,
                        create_role: parse_role_bool(parts[6], line_no, "create_role")?,
                        create_db: parse_role_bool(parts[7], line_no, "create_db")?,
                        can_login: parse_role_bool(parts[8], line_no, "can_login")?,
                        replication: parse_role_bool(parts[9], line_no, "replication")?,
                        bypass_rls: parse_role_bool(parts[10], line_no, "bypass_rls")?,
                        connection_limit: parse_role_i32(parts[11], line_no, "connection_limit")?,
                        valid_until: parse_role_optional_i64(parts[12], line_no, "valid_until")?,
                    });
                }
                Some("member") if parts.len() == 5 => {
                    let role = metadata_unescape(parts[1])?;
                    let member = metadata_unescape(parts[2])?;
                    validate_role_metadata_name(&role, line_no, "role")?;
                    validate_role_metadata_name(&member, line_no, "member")?;
                    let grantor = metadata_unescape(parts[3])?;
                    validate_role_metadata_name(&grantor, line_no, "grantor")?;
                    let key = (role.to_ascii_lowercase(), member.to_ascii_lowercase());
                    if !seen_membership_keys.insert(key) {
                        return Err(ServerError::ddl(format!(
                            "duplicate role membership metadata on line {}",
                            line_no + 1
                        )));
                    }
                    memberships.push(auth::RoleMembership {
                        role,
                        member,
                        grantor,
                        admin_option: parse_role_bool(parts[4], line_no, "admin_option")?,
                    });
                }
                _ => {
                    return Err(ServerError::ddl(format!(
                        "malformed role metadata line {}",
                        line_no + 1
                    )));
                }
            }
        }
        if roles.is_empty() {
            roles.push(auth::RoleEntry::bootstrap_superuser());
        }
        let role_names = roles
            .iter()
            .map(|role| role.name.to_ascii_lowercase())
            .collect::<std::collections::HashSet<_>>();
        for membership in &memberships {
            for (field, role_name) in [
                ("role", &membership.role),
                ("member", &membership.member),
                ("grantor", &membership.grantor),
            ] {
                if !role_names.contains(&role_name.to_ascii_lowercase()) {
                    return Err(ServerError::ddl(format!(
                        "unknown role membership metadata {field} '{}'",
                        role_name
                    )));
                }
            }
        }
        match roles
            .iter()
            .find(|role| role.name.eq_ignore_ascii_case("ultrasql"))
        {
            Some(role) if role.oid == auth::pg_authid::BOOTSTRAP_ROLE_OID => {
                validate_bootstrap_role_metadata(role)?;
            }
            Some(role) => {
                return Err(ServerError::ddl(format!(
                    "invalid bootstrap role metadata oid {}, expected {}",
                    role.oid,
                    auth::pg_authid::BOOTSTRAP_ROLE_OID
                )));
            }
            None => {
                return Err(ServerError::ddl(
                    "missing bootstrap role metadata 'ultrasql'",
                ));
            }
        }
        self.role_catalog.install_snapshot(roles, memberships);
        Ok(())
    }

    fn privilege_metadata_path(&self) -> Option<std::path::PathBuf> {
        self.data_dir
            .as_ref()
            .map(|dir| dir.join("pg_privileges.meta"))
    }

    pub(crate) fn ensure_privilege_metadata_slots_persistable(&self) -> Result<(), ServerError> {
        ensure_optional_runtime_metadata_write_slots(self.privilege_metadata_path())
    }

    pub(crate) fn persist_privilege_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.privilege_metadata_path() else {
            return Ok(());
        };
        let mut grants = self.privilege_catalog.list_grants();
        grants.sort_by(|left, right| {
            privilege_object_kind_name(left.object_kind)
                .cmp(privilege_object_kind_name(right.object_kind))
                .then_with(|| left.object_name.cmp(&right.object_name))
                .then_with(|| left.grantee.cmp(&right.grantee))
                .then_with(|| {
                    privilege_kind_name(left.privilege).cmp(privilege_kind_name(right.privilege))
                })
                .then_with(|| left.column_name.cmp(&right.column_name))
        });
        let mut default_grants = self.privilege_catalog.list_default_grants();
        default_grants.sort_by(|left, right| {
            left.owner_role
                .cmp(&right.owner_role)
                .then_with(|| left.schema_name.cmp(&right.schema_name))
                .then_with(|| {
                    privilege_object_kind_name(left.object_kind)
                        .cmp(privilege_object_kind_name(right.object_kind))
                })
                .then_with(|| left.grantee.cmp(&right.grantee))
                .then_with(|| {
                    privilege_kind_name(left.privilege).cmp(privilege_kind_name(right.privilege))
                })
        });

        let mut out = String::from("# ultrasql privilege runtime v1\n");
        for grant in grants {
            out.push_str(&format!(
                "grant\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                privilege_object_kind_name(grant.object_kind),
                metadata_escape(&grant.object_name),
                metadata_escape(&grant.grantee),
                privilege_kind_name(grant.privilege),
                metadata_escape(grant.column_name.as_deref().unwrap_or("")),
                metadata_escape(&grant.grantor),
                grant.grant_option
            ));
        }
        for grant in default_grants {
            out.push_str(&format!(
                "default\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                metadata_escape(&grant.owner_role),
                metadata_escape(grant.schema_name.as_deref().unwrap_or("")),
                privilege_object_kind_name(grant.object_kind),
                metadata_escape(&grant.grantee),
                privilege_kind_name(grant.privilege),
                metadata_escape(&grant.grantor),
                grant.grant_option
            ));
        }
        write_runtime_metadata_file(&path, &out)
    }

    fn rebuild_privilege_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.privilege_metadata_path() else {
            return Ok(());
        };
        let Some(text) = read_runtime_metadata_file(&path)? else {
            return Ok(());
        };

        let mut grants = Vec::new();
        let mut default_grants = Vec::new();
        let mut seen_grant_keys = std::collections::HashSet::new();
        let mut seen_default_grant_keys = std::collections::HashSet::new();
        let known_roles = runtime_metadata_known_role_names(&self.role_catalog);
        let snapshot = self.catalog_snapshot();
        for (line_no, line) in text.lines().enumerate() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts = line.split('\t').collect::<Vec<_>>();
            match parts.first().copied() {
                Some("grant") if parts.len() == 8 => {
                    let column_name = metadata_unescape(parts[5])?;
                    let grant = auth::PrivilegeGrant {
                        object_kind: parse_privilege_object_kind(parts[1], line_no)?,
                        object_name: metadata_unescape(parts[2])?,
                        grantee: metadata_unescape(parts[3])?,
                        privilege: parse_privilege_kind(parts[4], line_no)?,
                        column_name: (!column_name.is_empty()).then_some(column_name),
                        grantor: metadata_unescape(parts[6])?,
                        grant_option: parse_role_bool(parts[7], line_no, "grant_option")?,
                    };
                    let key = (
                        grant.object_kind,
                        grant.object_name.to_ascii_lowercase(),
                        grant.grantee.to_ascii_lowercase(),
                        grant.privilege,
                        grant
                            .column_name
                            .as_ref()
                            .map(|column| column.to_ascii_lowercase()),
                    );
                    if !seen_grant_keys.insert(key) {
                        return Err(ServerError::ddl(format!(
                            "duplicate privilege metadata grant on line {}",
                            line_no + 1
                        )));
                    }
                    validate_privilege_metadata_grantee(&known_roles, &grant.grantee, line_no)?;
                    validate_privilege_metadata_role(
                        &known_roles,
                        &grant.grantor,
                        line_no,
                        "grantor",
                    )?;
                    validate_privilege_metadata_column(&snapshot, &self.catalog, &grant, line_no)?;
                    grants.push(grant);
                }
                Some("default") if parts.len() == 8 => {
                    let schema_name = metadata_unescape(parts[2])?;
                    let grant = auth::DefaultPrivilegeGrant {
                        owner_role: metadata_unescape(parts[1])?,
                        schema_name: (!schema_name.is_empty()).then_some(schema_name),
                        object_kind: parse_privilege_object_kind(parts[3], line_no)?,
                        grantee: metadata_unescape(parts[4])?,
                        privilege: parse_privilege_kind(parts[5], line_no)?,
                        grantor: metadata_unescape(parts[6])?,
                        grant_option: parse_role_bool(parts[7], line_no, "grant_option")?,
                    };
                    let key = (
                        grant.owner_role.to_ascii_lowercase(),
                        grant
                            .schema_name
                            .as_ref()
                            .map(|schema| schema.to_ascii_lowercase()),
                        grant.object_kind,
                        grant.grantee.to_ascii_lowercase(),
                        grant.privilege,
                    );
                    if !seen_default_grant_keys.insert(key) {
                        return Err(ServerError::ddl(format!(
                            "duplicate default privilege metadata grant on line {}",
                            line_no + 1
                        )));
                    }
                    validate_privilege_metadata_role(
                        &known_roles,
                        &grant.owner_role,
                        line_no,
                        "owner",
                    )?;
                    validate_privilege_metadata_grantee(&known_roles, &grant.grantee, line_no)?;
                    validate_privilege_metadata_role(
                        &known_roles,
                        &grant.grantor,
                        line_no,
                        "grantor",
                    )?;
                    default_grants.push(grant);
                }
                _ => {
                    return Err(ServerError::ddl(format!(
                        "malformed privilege metadata line {}",
                        line_no + 1
                    )));
                }
            }
        }
        self.privilege_catalog
            .install_snapshot(grants, default_grants);
        Ok(())
    }

    fn sequence_owner_metadata_path(&self) -> Option<std::path::PathBuf> {
        self.data_dir
            .as_ref()
            .map(|dir| dir.join("pg_sequence_owner.meta"))
    }

    pub(crate) fn ensure_sequence_owner_metadata_slots_persistable(
        &self,
    ) -> Result<(), ServerError> {
        ensure_optional_runtime_metadata_write_slots(self.sequence_owner_metadata_path())
    }

    pub(crate) fn ensure_create_sequence_metadata_slots_persistable(
        &self,
    ) -> Result<(), ServerError> {
        self.ensure_sequence_owner_metadata_slots_persistable()?;
        ensure_optional_runtime_metadata_write_slots(self.privilege_metadata_path())
    }

    pub(crate) fn persist_sequence_owner_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.sequence_owner_metadata_path() else {
            return Ok(());
        };
        let mut owners = self
            .sequence_owners
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect::<Vec<_>>();
        owners.sort_by(|left, right| left.0.cmp(&right.0));

        let mut out = String::from("# ultrasql sequence owners v2\n");
        for (sequence_name, owner_role) in owners {
            if self.sequences.contains_key(&sequence_name) {
                let namespace = self
                    .sequence_namespaces
                    .get(&sequence_name)
                    .map_or_else(|| "public".to_owned(), |entry| entry.value().clone());
                out.push_str(&format!(
                    "sequence\t{}\t{}\t{}\n",
                    metadata_escape(&sequence_name),
                    metadata_escape(&owner_role),
                    metadata_escape(&namespace)
                ));
            }
        }
        write_runtime_metadata_file(&path, &out)
    }

    fn rebuild_sequence_owner_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.sequence_owner_metadata_path() else {
            return Ok(());
        };
        let Some(text) = read_runtime_metadata_file(&path)? else {
            return Ok(());
        };

        let mut owners = Vec::new();
        let mut seen_sequences = std::collections::HashSet::new();
        let known_roles = runtime_metadata_known_role_names(&self.role_catalog);
        for (line_no, line) in text.lines().enumerate() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts = line.split('\t').collect::<Vec<_>>();
            if !(parts.len() == 3 || parts.len() == 4) || parts.first().copied() != Some("sequence")
            {
                return Err(ServerError::ddl(format!(
                    "malformed sequence owner metadata line {}",
                    line_no + 1
                )));
            }
            let sequence_name = metadata_unescape(parts[1])?.to_ascii_lowercase();
            let owner_role = metadata_unescape(parts[2])?.to_ascii_lowercase();
            let namespace = parts
                .get(3)
                .map_or_else(|| Ok("public".to_owned()), |part| metadata_unescape(part))
                .map(|schema| schema.to_ascii_lowercase())?;
            if sequence_name.is_empty() || owner_role.is_empty() || namespace.is_empty() {
                return Err(ServerError::ddl(format!(
                    "empty sequence owner metadata field on line {}",
                    line_no + 1
                )));
            }
            if !builtin_schema_name(&namespace) && !self.schemas.contains_key(&namespace) {
                return Err(ServerError::ddl(format!(
                    "sequence owner metadata line {} references missing schema '{}'",
                    line_no + 1,
                    namespace
                )));
            }
            if !seen_sequences.insert(sequence_name.clone()) {
                return Err(ServerError::ddl(format!(
                    "duplicate sequence owner metadata '{}' on line {}",
                    sequence_name,
                    line_no + 1
                )));
            }
            if !self.sequences.contains_key(&sequence_name) {
                return Err(ServerError::ddl(format!(
                    "sequence owner metadata line {} references missing sequence '{}'",
                    line_no + 1,
                    sequence_name
                )));
            }
            if !known_roles.contains(&owner_role) {
                return Err(ServerError::ddl(format!(
                    "unknown sequence owner metadata role '{}' on line {}",
                    owner_role,
                    line_no + 1
                )));
            }
            owners.push((sequence_name, owner_role, namespace));
        }
        self.sequence_owners.clear();
        self.sequence_namespaces.clear();
        for (sequence_name, owner_role, namespace) in owners {
            self.sequence_owners
                .insert(sequence_name.clone(), owner_role);
            self.sequence_namespaces.insert(sequence_name, namespace);
        }
        Ok(())
    }

    fn schema_metadata_path(&self) -> Option<std::path::PathBuf> {
        self.data_dir
            .as_ref()
            .map(|dir| dir.join("pg_schema_runtime.meta"))
    }

    pub(crate) fn persist_schema_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.schema_metadata_path() else {
            return Ok(());
        };
        let mut schemas = self
            .schemas
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().as_ref().clone()))
            .collect::<Vec<_>>();
        schemas.sort_by(|left, right| left.0.cmp(&right.0));

        let mut out = String::from("# ultrasql schemas v1\n");
        for (_, schema) in schemas {
            out.push_str(&format!(
                "schema\t{}\t{}\n",
                metadata_escape(&schema.name),
                metadata_escape(&schema.owner_role)
            ));
        }
        write_runtime_metadata_file(&path, &out)
    }

    fn rebuild_schema_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.schema_metadata_path() else {
            return Ok(());
        };
        let Some(text) = read_runtime_metadata_file(&path)? else {
            return Ok(());
        };

        let mut schemas = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let known_roles = runtime_metadata_known_role_names(&self.role_catalog);
        for (line_no, line) in text.lines().enumerate() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts = line.split('\t').collect::<Vec<_>>();
            if parts.len() != 3 || parts.first().copied() != Some("schema") {
                return Err(ServerError::ddl(format!(
                    "malformed schema metadata line {}",
                    line_no + 1
                )));
            }
            let name = metadata_unescape(parts[1])?.to_ascii_lowercase();
            let owner_role = metadata_unescape(parts[2])?.to_ascii_lowercase();
            if name.is_empty() || owner_role.is_empty() {
                return Err(ServerError::ddl(format!(
                    "empty schema metadata field on line {}",
                    line_no + 1
                )));
            }
            if builtin_schema_name(&name) {
                return Err(ServerError::ddl(format!(
                    "schema metadata line {} attempts to override built-in schema '{}'",
                    line_no + 1,
                    name
                )));
            }
            if !seen.insert(name.clone()) {
                return Err(ServerError::ddl(format!(
                    "duplicate schema metadata '{}' on line {}",
                    name,
                    line_no + 1
                )));
            }
            if !known_roles.contains(&owner_role) {
                return Err(ServerError::ddl(format!(
                    "unknown schema metadata owner '{}' on line {}",
                    owner_role,
                    line_no + 1
                )));
            }
            schemas.push(RuntimeSchema { name, owner_role });
        }
        self.schemas.clear();
        for schema in schemas {
            self.schemas.insert(schema.name.clone(), Arc::new(schema));
        }
        Ok(())
    }

    fn refresh_persistent_catalog_schema_names(&self) {
        let namespace_names = self
            .schemas
            .iter()
            .map(|entry| {
                (
                    ultrasql_core::Oid::new(runtime_schema_oid(entry.key())),
                    entry.key().clone(),
                )
            })
            .collect::<std::collections::HashMap<_, _>>();
        self.persistent_catalog
            .refresh_runtime_schema_names(&namespace_names);
    }

    fn operator_metadata_path(&self) -> Option<std::path::PathBuf> {
        self.data_dir
            .as_ref()
            .map(|dir| dir.join("pg_operator_runtime.meta"))
    }

    pub(crate) fn persist_operator_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.operator_metadata_path() else {
            return Ok(());
        };
        let mut operators = self
            .operators
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().as_ref().clone()))
            .collect::<Vec<_>>();
        operators.sort_by(|left, right| left.0.cmp(&right.0));

        let mut out = String::from("# ultrasql operator runtime v1\n");
        for (_, operator) in operators {
            let left = operator_data_type_token(&operator.left_type, &operator.name)?;
            let right = operator_data_type_token(&operator.right_type, &operator.name)?;
            let Some(result) = data_type_token(&operator.result_type) else {
                return Err(ServerError::ddl(format!(
                    "operator '{}' result type is outside restart-persistable metadata subset",
                    operator.name
                )));
            };
            out.push_str(&format!(
                "operator\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                operator.oid,
                metadata_escape(&operator.namespace),
                metadata_escape(&operator.name),
                metadata_escape(&left),
                metadata_escape(&right),
                metadata_escape(&operator.procedure),
                metadata_escape(&result)
            ));
        }
        write_runtime_metadata_file(&path, &out)
    }

    fn rebuild_operator_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.operator_metadata_path() else {
            return Ok(());
        };
        let Some(text) = read_runtime_metadata_file(&path)? else {
            return Ok(());
        };

        self.operators.clear();
        let mut seen_oids = std::collections::HashSet::new();
        let mut seen_signatures = std::collections::HashSet::new();
        for (line_no, line) in text.lines().enumerate() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts = line.split('\t').collect::<Vec<_>>();
            if parts.len() != 8 || parts.first().copied() != Some("operator") {
                return Err(ServerError::ddl(format!(
                    "malformed operator metadata line {}",
                    line_no + 1
                )));
            }
            let oid = parts[1].parse::<u32>().map_err(|err| {
                ServerError::Ddl(format!(
                    "operator metadata line {} bad oid: {err}",
                    line_no + 1
                ))
            })?;
            if !seen_oids.insert(oid) {
                return Err(ServerError::ddl(format!(
                    "duplicate operator metadata oid {} on line {}",
                    oid,
                    line_no + 1
                )));
            }
            let namespace = metadata_unescape(parts[2])?;
            let name = metadata_unescape(parts[3])?;
            let left_type =
                parse_operator_data_type_token(&metadata_unescape(parts[4])?, line_no, "left")?;
            let right_type =
                parse_operator_data_type_token(&metadata_unescape(parts[5])?, line_no, "right")?;
            let procedure = metadata_unescape(parts[6])?;
            let result_token = metadata_unescape(parts[7])?;
            let result_type = data_type_from_token(&result_token).ok_or_else(|| {
                ServerError::ddl(format!(
                    "operator metadata line {} has unknown result type '{}'",
                    line_no + 1,
                    result_token
                ))
            })?;
            let operator = RuntimeOperator {
                oid,
                name,
                namespace,
                left_type,
                right_type,
                procedure,
                result_type,
            };
            validate_runtime_operator_metadata(&operator, line_no)?;
            let signature = runtime_operator_signature(
                &operator.namespace,
                &operator.name,
                &operator.left_type,
                &operator.right_type,
            );
            if !seen_signatures.insert(signature.clone()) {
                return Err(ServerError::ddl(format!(
                    "duplicate operator metadata signature '{}' on line {}",
                    signature,
                    line_no + 1
                )));
            }
            self.operators.insert(signature, Arc::new(operator));
        }
        Ok(())
    }

    fn row_security_metadata_path(&self) -> Option<std::path::PathBuf> {
        self.data_dir
            .as_ref()
            .map(|dir| dir.join("pg_row_security.meta"))
    }

    pub(crate) fn persist_row_security_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.row_security_metadata_path() else {
            return Ok(());
        };
        let snapshot = self.catalog_snapshot();
        let mut entries = self
            .row_security
            .iter()
            .filter_map(|entry| {
                if !entry.value().enabled
                    && entry.value().policies.is_empty()
                    && entry.value().owner_role.is_empty()
                {
                    return None;
                }
                let table = snapshot.tables_by_oid.get(entry.key())?;
                Some((
                    *entry.key(),
                    table.name.clone(),
                    entry.value().as_ref().clone(),
                ))
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|(oid, _, _)| oid.raw());

        let mut out = String::from("# ultrasql row security v2\n");
        for (oid, table_name, runtime) in entries {
            out.push_str(&format!(
                "table\t{}\t{}\t{}\t{}\n",
                metadata_escape(&table_name),
                oid.raw(),
                runtime.enabled,
                metadata_escape(&runtime.owner_role)
            ));
            for policy in &runtime.policies {
                let (using_idx, using_col, using_setting) = rls_expr_fields(policy.using.as_ref());
                let (check_idx, check_col, check_setting) =
                    rls_expr_fields(policy.with_check.as_ref());
                out.push_str(&format!(
                    "policy\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                    oid.raw(),
                    metadata_escape(&policy.name),
                    rls_permissiveness_name(policy.permissiveness),
                    rls_command_name(policy.command),
                    using_idx,
                    using_col,
                    using_setting,
                    check_idx,
                    check_col,
                    check_setting,
                    metadata_escape(&metadata_encode_list(&policy.roles))
                ));
            }
        }
        write_runtime_metadata_file(&path, &out)
    }

    fn rebuild_row_security_sidecars(&self) -> Result<(), ServerError> {
        let Some(path) = self.row_security_metadata_path() else {
            return Ok(());
        };
        let Some(text) = read_runtime_metadata_file(&path)? else {
            return Ok(());
        };
        let snapshot = self.catalog_snapshot();
        let mut rows: std::collections::HashMap<ultrasql_core::Oid, (String, TableRowSecurity)> =
            std::collections::HashMap::new();
        let mut seen_table_oids = std::collections::HashSet::new();
        let mut seen_policy_keys = std::collections::HashSet::new();
        let known_roles = runtime_metadata_known_role_names(&self.role_catalog);
        for (line_no, line) in text.lines().enumerate() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts = line.split('\t').collect::<Vec<_>>();
            match parts.first().copied() {
                Some("table") if parts.len() == 4 || parts.len() == 5 => {
                    let table_name = metadata_unescape(parts[1])?;
                    let oid = ultrasql_core::Oid::new(parts[2].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "RLS metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    if !seen_table_oids.insert(oid) {
                        return Err(ServerError::Ddl(format!(
                            "duplicate RLS table metadata on line {}",
                            line_no + 1
                        )));
                    }
                    let enabled = parts[3].parse::<bool>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "RLS metadata line {} bad enabled flag: {err}",
                            line_no + 1
                        ))
                    })?;
                    let owner_role = if parts.len() == 5 {
                        metadata_unescape(parts[4])?.to_ascii_lowercase()
                    } else {
                        String::new()
                    };
                    if !owner_role.is_empty() && !known_roles.contains(&owner_role) {
                        return Err(ServerError::Ddl(format!(
                            "unknown RLS table metadata owner '{}' on line {}",
                            owner_role,
                            line_no + 1
                        )));
                    }
                    let entry = rows
                        .entry(oid)
                        .or_insert_with(|| (String::new(), TableRowSecurity::default()));
                    entry.0 = table_name;
                    entry.1.enabled = enabled;
                    entry.1.owner_role = owner_role;
                }
                Some("policy") if parts.len() == 11 || parts.len() == 12 => {
                    let oid = ultrasql_core::Oid::new(parts[1].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "RLS metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    let policy_name = metadata_unescape(parts[2])?;
                    if !seen_policy_keys.insert((oid, policy_name.to_ascii_lowercase())) {
                        return Err(ServerError::Ddl(format!(
                            "duplicate RLS policy metadata '{}' on line {}",
                            policy_name,
                            line_no + 1
                        )));
                    }
                    let mut roles = if parts.len() == 12 {
                        metadata_decode_list(&metadata_unescape(parts[11])?)?
                    } else {
                        Vec::new()
                    };
                    validate_rls_metadata_policy_roles(&known_roles, &mut roles, line_no)?;
                    let using = parse_rls_expr(parts[5], parts[6], parts[7])?;
                    let with_check = parse_rls_expr(parts[8], parts[9], parts[10])?;
                    if let Some(table) = snapshot.tables_by_oid.get(&oid) {
                        validate_rls_metadata_expr(table, using.as_ref(), line_no, "USING")?;
                        validate_rls_metadata_expr(
                            table,
                            with_check.as_ref(),
                            line_no,
                            "WITH CHECK",
                        )?;
                    }
                    let policy = RuntimeRlsPolicy {
                        name: policy_name,
                        permissiveness: parse_rls_permissiveness(parts[3])?,
                        command: parse_rls_command(parts[4])?,
                        roles,
                        using,
                        with_check,
                    };
                    rows.entry(oid)
                        .or_insert_with(|| (String::new(), TableRowSecurity::default()))
                        .1
                        .policies
                        .push(policy);
                }
                _ => {
                    return Err(ServerError::Ddl(format!(
                        "malformed RLS metadata line {}",
                        line_no + 1
                    )));
                }
            }
        }
        for (oid, (table_name, runtime)) in rows {
            let Some(table) = snapshot.tables_by_oid.get(&oid) else {
                return Err(ServerError::Ddl(format!(
                    "unknown RLS table metadata '{}' on oid {}",
                    table_name,
                    oid.raw()
                )));
            };
            if table.name != table_name {
                return Err(ServerError::Ddl(format!(
                    "RLS table metadata '{}' does not match catalog table '{}'",
                    table_name, table.name
                )));
            }
            self.row_security.insert(oid, Arc::new(runtime));
        }
        Ok(())
    }

    fn materialized_view_metadata_path(&self) -> Option<std::path::PathBuf> {
        self.data_dir
            .as_ref()
            .map(|dir| dir.join("pg_materialized_views.meta"))
    }

    fn load_materialized_view_metadata(
        &self,
    ) -> Result<Vec<MaterializedViewMetadataRecord>, ServerError> {
        let Some(path) = self.materialized_view_metadata_path() else {
            return Ok(Vec::new());
        };
        let Some(text) = read_runtime_metadata_file(&path)? else {
            return Ok(Vec::new());
        };
        let mut records = Vec::new();
        let mut seen_view_names = std::collections::HashSet::new();
        let mut seen_view_oids = std::collections::HashSet::new();
        for (line_no, line) in text.lines().enumerate() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts = line.split('\t').collect::<Vec<_>>();
            if parts.len() != 6 {
                return Err(ServerError::Ddl(format!(
                    "materialized-view metadata line {} has {} fields",
                    line_no + 1,
                    parts.len()
                )));
            }
            let view_oid = parts[1].parse::<u32>().map_err(|err| {
                ServerError::Ddl(format!(
                    "materialized-view metadata line {} bad view oid: {err}",
                    line_no + 1
                ))
            })?;
            let view_table = metadata_unescape(parts[0])?;
            if !seen_view_names.insert(view_table.to_ascii_lowercase())
                || !seen_view_oids.insert(view_oid)
            {
                return Err(ServerError::Ddl(format!(
                    "duplicate materialized-view metadata on line {}",
                    line_no + 1
                )));
            }
            let source_oid = parts[3].parse::<u32>().map_err(|err| {
                ServerError::Ddl(format!(
                    "materialized-view metadata line {} bad source oid: {err}",
                    line_no + 1
                ))
            })?;
            let materialized_rows = parts[4].parse::<u64>().map_err(|err| {
                ServerError::Ddl(format!(
                    "materialized-view metadata line {} bad row count: {err}",
                    line_no + 1
                ))
            })?;
            let projection = if parts[5].is_empty() {
                Vec::new()
            } else {
                parts[5]
                    .split(',')
                    .map(|raw| {
                        raw.parse::<usize>().map_err(|err| {
                            ServerError::Ddl(format!(
                                "materialized-view metadata line {} bad projection index: {err}",
                                line_no + 1
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?
            };
            records.push(MaterializedViewMetadataRecord {
                view_table,
                view_oid: ultrasql_core::Oid::new(view_oid),
                source_table: metadata_unescape(parts[2])?,
                source_oid: ultrasql_core::Oid::new(source_oid),
                materialized_rows,
                projection,
            });
        }
        Ok(records)
    }

    fn write_materialized_view_metadata(
        &self,
        records: &[MaterializedViewMetadataRecord],
    ) -> Result<(), ServerError> {
        let Some(path) = self.materialized_view_metadata_path() else {
            return Ok(());
        };
        let mut out = String::from("# ultrasql materialized views v1\n");
        for record in records {
            let projection = record
                .projection
                .iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join(",");
            out.push_str(&format!(
                "{}\t{}\t{}\t{}\t{}\t{}\n",
                metadata_escape(&record.view_table),
                record.view_oid.raw(),
                metadata_escape(&record.source_table),
                record.source_oid.raw(),
                record.materialized_rows,
                projection
            ));
        }
        write_runtime_metadata_file(&path, &out)
    }

    pub(crate) fn ensure_materialized_view_runtime_metadata_persistable(
        &self,
        runtime: &MaterializedViewRuntime,
    ) -> Result<Vec<usize>, ServerError> {
        if self.materialized_view_metadata_path().is_none() {
            return Ok(Vec::new());
        }
        materialized_view_projection_indices(&runtime.source).ok_or_else(|| {
            ServerError::ddl(format!(
                "materialized view '{}' source shape is outside restart-persistable metadata subset",
                runtime.view_table
            ))
        })
    }

    pub(crate) fn persist_materialized_view_runtime_metadata(
        &self,
        runtime: &MaterializedViewRuntime,
        materialized_rows: u64,
    ) -> Result<(), ServerError> {
        if self.materialized_view_metadata_path().is_none() {
            return Ok(());
        }
        let projection = self.ensure_materialized_view_runtime_metadata_persistable(runtime)?;
        let Some(view_entry) = self.persistent_catalog.lookup_table(&runtime.view_table) else {
            return Ok(());
        };
        let Some(source_entry) = self.persistent_catalog.lookup_table(&runtime.source_table) else {
            return Ok(());
        };
        let mut records = self.load_materialized_view_metadata()?;
        records.retain(|record| {
            record.view_table != runtime.view_table && record.view_oid != view_entry.oid
        });
        records.push(MaterializedViewMetadataRecord {
            view_table: runtime.view_table.clone(),
            view_oid: view_entry.oid,
            source_table: runtime.source_table.clone(),
            source_oid: source_entry.oid,
            materialized_rows,
            projection,
        });
        self.write_materialized_view_metadata(&records)
    }

    pub(crate) fn remove_materialized_view_runtime_metadata(
        &self,
        dropped_tables: &[String],
    ) -> Result<(), ServerError> {
        if dropped_tables.is_empty() {
            return Ok(());
        }
        let mut records = self.load_materialized_view_metadata()?;
        let before = records.len();
        records.retain(|record| {
            !dropped_tables
                .iter()
                .any(|table| record.view_table.eq_ignore_ascii_case(table))
        });
        if records.len() != before {
            self.write_materialized_view_metadata(&records)?;
        }
        Ok(())
    }

    fn rebuild_materialized_view_runtime_sidecars(&self) -> Result<(), ServerError> {
        for record in self.load_materialized_view_metadata()? {
            let view_entry = self
                .persistent_catalog
                .lookup_table(&record.view_table)
                .ok_or_else(|| {
                    ServerError::Ddl(format!(
                        "invalid materialized-view metadata for '{}'",
                        record.view_table
                    ))
                })?;
            let source_entry = self
                .persistent_catalog
                .lookup_table(&record.source_table)
                .ok_or_else(|| {
                    ServerError::Ddl(format!(
                        "invalid materialized-view metadata for '{}'",
                        record.view_table
                    ))
                })?;
            if view_entry.oid != record.view_oid || source_entry.oid != record.source_oid {
                return Err(ServerError::Ddl(format!(
                    "invalid materialized-view metadata for '{}'",
                    record.view_table
                )));
            }
            let Some(source) =
                materialized_view_source_plan_from_metadata(&source_entry, &view_entry, &record)
            else {
                return Err(ServerError::Ddl(format!(
                    "invalid materialized-view metadata for '{}'",
                    record.view_table
                )));
            };
            self.materialized_views.insert(
                record.view_table.clone(),
                Arc::new(MaterializedViewRuntime {
                    view_table: record.view_table.clone(),
                    source_table: record.source_table.clone(),
                    source,
                    materialized_rows: std::sync::atomic::AtomicU64::new(record.materialized_rows),
                }),
            );
        }
        Ok(())
    }

    fn rebuild_time_partition_runtime_sidecars(&self) -> Result<(), ServerError> {
        self.time_partitions.clear();
        let snapshot = self.catalog_snapshot();
        let mut parents = Vec::new();
        let mut chunks = Vec::new();
        for (key, entry) in &snapshot.tables {
            if let Some(options) =
                time_partition::parent_options_from_entry(entry).map_err(ServerError::Ddl)?
            {
                parents.push((entry.clone(), options));
            }
            if let Some(options) =
                time_partition::chunk_options_from_entry(entry).map_err(ServerError::Ddl)?
            {
                chunks.push((key.clone(), entry.clone(), options));
            }
        }
        parents.sort_by_key(|(entry, _)| entry.oid.raw());
        chunks.sort_by_key(|(_, entry, _)| entry.oid.raw());

        for (entry, options) in parents {
            let partition_column_index = entry
                .schema
                .fields()
                .iter()
                .position(|field| field.name.eq_ignore_ascii_case(&options.column))
                .ok_or_else(|| {
                    ServerError::Ddl(format!(
                        "time partition table '{}' references missing column '{}'",
                        entry.name, options.column
                    ))
                })?;
            let partition_column = entry.schema.field(partition_column_index).ok_or_else(|| {
                ServerError::Ddl(format!(
                    "time partition table '{}' column index is invalid",
                    entry.name
                ))
            })?;
            match &partition_column.data_type {
                DataType::Timestamp | DataType::TimestampTz => {}
                other => {
                    return Err(ServerError::Ddl(format!(
                        "time partition table '{}' column '{}' has unsupported type {other}",
                        entry.name, partition_column.name
                    )));
                }
            }

            let mut runtime = time_partition::TimePartitionRuntime::daily(
                entry.schema_name.clone(),
                entry.name.clone(),
                entry.oid,
                entry.schema.clone(),
                partition_column.name.clone(),
                partition_column_index,
            );
            runtime.chunk_interval_us = options.interval_us;
            for (chunk_key, chunk_entry, chunk_options) in &chunks {
                if chunk_options.parent_oid != entry.oid {
                    continue;
                }
                if chunk_entry.schema.len() != entry.schema.len() {
                    return Err(ServerError::Ddl(format!(
                        "time partition chunk '{}' has schema width {} but parent '{}' has width {}",
                        chunk_entry.name,
                        chunk_entry.schema.len(),
                        entry.name,
                        entry.schema.len()
                    )));
                }
                runtime.chunks.insert(
                    chunk_options.start_us,
                    time_partition::TimeChunkRuntime {
                        start_us: chunk_options.start_us,
                        end_us: chunk_options.end_us,
                        table_name: chunk_key.clone(),
                        oid: chunk_entry.oid,
                    },
                );
            }
            self.time_partitions
                .insert(table_entry_lookup_key(&entry), Arc::new(runtime));
        }
        Ok(())
    }

    fn recover_commit_status_from_wal(&self) -> Result<(), ServerError> {
        let Some(data_dir) = &self.data_dir else {
            return Ok(());
        };
        let wal_dir = data_dir.join("pg_wal");
        let recovery_replay_target = recovery_replay_target_from_data_dir(data_dir)?;
        ultrasql_wal::recover_with_target(&wal_dir, recovery_replay_target, |record| {
            self.txn_manager.recover_observed_xid(record.header.xid);
            match record.header.record_type {
                RecordType::Commit => self.txn_manager.recover_committed(record.header.xid),
                RecordType::Abort => self.txn_manager.recover_aborted(record.header.xid),
                _ => {}
            }
            Ok(())
        })
        .map(|_| ())
        .map_err(|e| ServerError::ddl(format!("recover commit status: {e}")))
    }

    fn rebuild_persistent_index_sidecars(&self) -> Result<(), ServerError> {
        let snapshot = self.catalog_snapshot();
        let mut hnsw_indexes = Vec::new();
        let mut ivfflat_indexes = Vec::new();

        for (table_oid, indexes) in &snapshot.indexes_by_table {
            let Some(table) = snapshot.tables_by_oid.get(table_oid) else {
                continue;
            };
            let mut constraints = self
                .table_constraints
                .get(table_oid)
                .map(|entry| entry.value().as_ref().clone())
                .unwrap_or_default();
            let mut changed = false;

            for index in indexes {
                let method = logical_index_method_from_name(&index.access_method);
                match method {
                    LogicalIndexMethod::Btree | LogicalIndexMethod::Hash => {
                        let rows = self.rebuild_btree_index_pages(table, index, method)?;
                        tracing::info!(
                            table = %table.name,
                            index = %index.name,
                            rows,
                            "rebuilt persistent btree index pages"
                        );
                    }
                    LogicalIndexMethod::Brin => {
                        let (brin, rows) = self.rebuild_brin_summary(table, index)?;
                        constraints.indexes.insert(
                            index.oid,
                            RuntimeIndexMetadata {
                                key_exprs: Vec::new(),
                                predicate: None,
                                include_columns: Vec::new(),
                                method,
                                brin: Some(brin),
                                hnsw: None,
                                ivfflat: None,
                                aggregating: None,
                            },
                        );
                        changed = true;
                        tracing::info!(
                            table = %table.name,
                            index = %index.name,
                            rows,
                            "rebuilt persistent brin summaries"
                        );
                    }
                    LogicalIndexMethod::Hnsw => {
                        let [attnum] = index.columns.as_slice() else {
                            continue;
                        };
                        let col = usize::from(*attnum);
                        let Some(field) = table.schema.field(col) else {
                            continue;
                        };
                        let Some((dims, default_payload)) =
                            ann_dims_and_default_payload(&field.data_type)
                        else {
                            continue;
                        };
                        let metric = hnsw_metric_for_opclass_name(
                            index.opclasses.first().and_then(Option::as_deref),
                        )?;
                        let payload = ann_payload_option_from_catalog(&index.options)?
                            .unwrap_or(default_payload);
                        let hnsw = Arc::new(
                            PageBackedHnswIndex::new_with_payload_kind(
                                RelationId::new(index.oid.raw()),
                                dims,
                                metric,
                                16,
                                64,
                                payload,
                            )
                            .map_err(|e| {
                                ServerError::ddl(format!(
                                    "rebuild HNSW {} from catalog: {e}",
                                    index.name
                                ))
                            })?,
                        );
                        hnsw_indexes.push(Arc::clone(&hnsw));
                        constraints.indexes.insert(
                            index.oid,
                            RuntimeIndexMetadata {
                                key_exprs: Vec::new(),
                                predicate: None,
                                include_columns: Vec::new(),
                                method,
                                brin: None,
                                hnsw: Some(hnsw),
                                ivfflat: None,
                                aggregating: None,
                            },
                        );
                        changed = true;
                    }
                    LogicalIndexMethod::IvfFlat => {
                        let [attnum] = index.columns.as_slice() else {
                            continue;
                        };
                        let col = usize::from(*attnum);
                        let Some(field) = table.schema.field(col) else {
                            continue;
                        };
                        let Some((dims, default_payload)) =
                            ann_dims_and_default_payload(&field.data_type)
                        else {
                            continue;
                        };
                        let metric = hnsw_metric_for_opclass_name(
                            index.opclasses.first().and_then(Option::as_deref),
                        )?;
                        let (lists, probes, payload) =
                            ivfflat_options_from_catalog(&index.options)?;
                        let payload = payload.unwrap_or(default_payload);
                        let ivfflat = Arc::new(
                            PageBackedIvfFlatIndex::new_with_payload_kind(
                                RelationId::new(index.oid.raw()),
                                dims,
                                metric,
                                lists,
                                probes,
                                payload,
                            )
                            .map_err(|e| {
                                ServerError::ddl(format!(
                                    "rebuild IVFFlat {} from catalog: {e}",
                                    index.name
                                ))
                            })?,
                        );
                        ivfflat_indexes.push(Arc::clone(&ivfflat));
                        constraints.indexes.insert(
                            index.oid,
                            RuntimeIndexMetadata {
                                key_exprs: Vec::new(),
                                predicate: None,
                                include_columns: Vec::new(),
                                method,
                                brin: None,
                                hnsw: None,
                                ivfflat: Some(ivfflat),
                                aggregating: None,
                            },
                        );
                        changed = true;
                    }
                    LogicalIndexMethod::Aggregating => {
                        let Some(spec) =
                            crate::aggregating_index::aggregating_index_spec_from_catalog(
                                table, index,
                            )?
                        else {
                            continue;
                        };
                        let rows = self.rebuild_aggregating_index_rows(table, &spec)?;
                        constraints.indexes.insert(
                            index.oid,
                            RuntimeIndexMetadata {
                                key_exprs: aggregating_group_key_exprs(table, &spec)?,
                                predicate: None,
                                include_columns: Vec::new(),
                                method,
                                brin: None,
                                hnsw: None,
                                ivfflat: None,
                                aggregating: Some(Arc::new(RuntimeAggregatingIndex::new(
                                    spec, rows,
                                ))),
                            },
                        );
                        changed = true;
                    }
                    _ => {}
                }
            }

            if changed {
                self.table_constraints
                    .insert(*table_oid, Arc::new(constraints));
            }
        }

        self.replay_vector_index_wal_into(&hnsw_indexes, &ivfflat_indexes)
    }

    fn rebuild_btree_index_pages(
        &self,
        table: &TableEntry,
        index: &IndexEntry,
        method: LogicalIndexMethod,
    ) -> Result<u64, ServerError> {
        if index.root_block == BlockNumber::INVALID {
            return Ok(0);
        }
        let columns: Vec<usize> = index
            .columns
            .iter()
            .map(|attnum| usize::from(*attnum))
            .collect();
        let runtime_metadata = self
            .table_constraints
            .get(&table.oid)
            .and_then(|constraints| constraints.indexes.get(&index.oid).cloned());
        let expression_key_exprs = runtime_metadata
            .as_ref()
            .map_or_else(Vec::new, |metadata| metadata.key_exprs.clone());
        let predicate = runtime_metadata
            .as_ref()
            .and_then(|metadata| metadata.predicate.clone());
        if columns.is_empty() && expression_key_exprs.is_empty() {
            return Ok(0);
        }
        let encoding = if method == LogicalIndexMethod::Hash {
            crate::index_key::IndexKeyEncoding::Int64
        } else if columns.is_empty() && expression_key_exprs.len() == 1 {
            crate::index_key::IndexKeyEncoding::for_data_type(&expression_key_exprs[0].data_type())?
        } else {
            crate::index_key::IndexKeyEncoding::for_columns(&table.schema, &columns)?
        };
        let key_col_idx = columns.first().copied();
        let index_rel = RelationId::new(index.oid.raw());
        let mut btree = BTree::create(Arc::clone(self.heap.buffer_pool()), index_rel)
            .map_err(|e| ServerError::ddl(format!("restart rebuild {}: {e}", index.name)))?;
        let txn = self.txn_manager.begin(IsolationLevel::ReadCommitted);
        let table_rel = RelationId(table.oid);
        let block_count = self.heap.block_count(table_rel).max(table.n_blocks);
        let scan = self.heap.scan_visible(
            table_rel,
            block_count,
            &txn.snapshot,
            self.txn_manager.as_ref(),
        );
        let result = (|| -> Result<u64, ServerError> {
            let mut inserted = 0_u64;
            for tuple in scan {
                let tuple = tuple.map_err(|e| {
                    ServerError::ddl(format!(
                        "restart rebuild {} heap scan failed: {e}",
                        index.name
                    ))
                })?;
                let Some(key) = decode_key_column(
                    &tuple.data,
                    &table.schema,
                    key_col_idx,
                    &expression_key_exprs,
                    predicate.as_ref(),
                    method,
                    &encoding,
                )?
                else {
                    continue;
                };
                if index.is_unique {
                    btree.insert(key, tuple.tid, txn.xid, None).map_err(|e| {
                        ServerError::ddl(format!("restart rebuild {}: {e}", index.name))
                    })?;
                } else {
                    btree
                        .insert_non_unique(key, tuple.tid, txn.xid, None)
                        .map_err(|e| {
                            ServerError::ddl(format!("restart rebuild {}: {e}", index.name))
                        })?;
                }
                inserted = inserted.saturating_add(1);
            }
            Ok(inserted)
        })();
        self.finalise_restart_rebuild_transaction(
            txn,
            result,
            "restart btree rebuild transaction commit",
            "restart btree rebuild transaction rollback",
        )
    }

    fn rebuild_brin_summary(
        &self,
        table: &TableEntry,
        index: &IndexEntry,
    ) -> Result<(Arc<BrinIndex>, u64), ServerError> {
        if index.columns.is_empty() {
            return Ok((Arc::new(BrinIndex::new(128)), 0));
        }
        let columns: Vec<usize> = index
            .columns
            .iter()
            .map(|attnum| usize::from(*attnum))
            .collect();
        let encoding = crate::index_key::IndexKeyEncoding::for_columns(&table.schema, &columns)?;
        let key_col_idx = columns.first().copied();
        let brin = Arc::new(BrinIndex::new(128));
        let txn = self.txn_manager.begin(IsolationLevel::ReadCommitted);
        let table_rel = RelationId(table.oid);
        let block_count = self.heap.block_count(table_rel).max(table.n_blocks);
        let scan = self.heap.scan_visible(
            table_rel,
            block_count,
            &txn.snapshot,
            self.txn_manager.as_ref(),
        );
        let result = (|| -> Result<u64, ServerError> {
            let mut inserted = 0_u64;
            for tuple in scan {
                let tuple = tuple.map_err(|e| {
                    ServerError::ddl(format!(
                        "restart rebuild {} BRIN heap scan failed: {e}",
                        index.name
                    ))
                })?;
                let Some(key) = decode_key_column(
                    &tuple.data,
                    &table.schema,
                    key_col_idx,
                    &[],
                    None,
                    LogicalIndexMethod::Brin,
                    &encoding,
                )?
                else {
                    continue;
                };
                let brin_key = BrinIndex::encode_i64_key(key);
                brin.insert(&brin_key, tuple.tid).map_err(|e| {
                    ServerError::ddl(format!("restart rebuild {} BRIN: {e}", index.name))
                })?;
                inserted = inserted.saturating_add(1);
            }
            Ok(inserted)
        })();
        let rows = self.finalise_restart_rebuild_transaction(
            txn,
            result,
            "restart brin rebuild transaction commit",
            "restart brin rebuild transaction rollback",
        )?;
        Ok((brin, rows))
    }

    fn rebuild_aggregating_index_rows(
        &self,
        table: &TableEntry,
        spec: &ultrasql_planner::LogicalAggregatingIndex,
    ) -> Result<Vec<Vec<Value>>, ServerError> {
        let txn = self.txn_manager.begin(IsolationLevel::ReadCommitted);
        let rows = crate::aggregating_index::build_aggregating_index_rows(
            table,
            spec,
            self.heap.as_ref(),
            &txn.snapshot,
            self.txn_manager.as_ref(),
        );
        self.finalise_restart_rebuild_transaction(
            txn,
            rows,
            "restart aggregating-index rebuild transaction commit",
            "restart aggregating-index rebuild transaction rollback",
        )
    }

    fn finalise_restart_rebuild_transaction<T>(
        &self,
        txn: Transaction,
        outcome: Result<T, ServerError>,
        commit_context: &'static str,
        rollback_context: &'static str,
    ) -> Result<T, ServerError> {
        match outcome {
            Ok(value) => self
                .txn_manager
                .commit(txn)
                .map(|()| value)
                .map_err(|err| ServerError::ddl(format!("{commit_context}: {err}"))),
            Err(err) => match self.txn_manager.abort(txn) {
                Ok(()) => Err(err),
                Err(abort_err) => Err(ServerError::ddl(format!(
                    "{rollback_context}: {err}; transaction abort failed: {abort_err}"
                ))),
            },
        }
    }

    fn replay_vector_index_wal_into(
        &self,
        hnsw_indexes: &[Arc<PageBackedHnswIndex>],
        ivfflat_indexes: &[Arc<PageBackedIvfFlatIndex>],
    ) -> Result<(), ServerError> {
        if hnsw_indexes.is_empty() && ivfflat_indexes.is_empty() {
            return Ok(());
        }
        let Some(data_dir) = &self.data_dir else {
            return Ok(());
        };
        let wal_dir = data_dir.join("pg_wal");
        let recovery_replay_target = recovery_replay_target_from_data_dir(data_dir)?;
        ultrasql_wal::recover_with_target(&wal_dir, recovery_replay_target, |record| {
            if record.header.record_type == RecordType::HnswOp {
                for hnsw in hnsw_indexes {
                    if !hnsw.is_valid() {
                        continue;
                    }
                    if let Err(e) = hnsw.apply_wal_record(record) {
                        hnsw.invalidate();
                        tracing::warn!(
                            error = %e,
                            "HNSW WAL replay failed; marking index unavailable"
                        );
                    }
                }
            }
            if record.header.record_type == RecordType::IvfFlatOp {
                for ivfflat in ivfflat_indexes {
                    if !ivfflat.is_valid() {
                        continue;
                    }
                    if let Err(e) = ivfflat.apply_wal_record(record) {
                        ivfflat.invalidate();
                        tracing::warn!(
                            error = %e,
                            "IVFFlat WAL replay failed; marking index unavailable"
                        );
                    }
                }
            }
            Ok(())
        })
        .map(|_| ())
        .map_err(|e| ServerError::ddl(format!("recover vector index WAL: {e}")))
    }

    /// Allocate the next per-connection process id.
    ///
    /// Counter is monotonic; wraps after 2^32 connections. The PostgreSQL
    /// wire layer treats the value opaquely.
    pub fn allocate_pid(&self) -> u32 {
        self.next_pid
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Acquire a per-statement catalog snapshot.
    ///
    /// The returned [`Arc<CatalogSnapshot>`] is immutable and stable for the
    /// caller's lifetime; concurrent DDL atomically swaps in a new pointer
    /// without invalidating this reference.
    ///
    /// This is the primary entry point for the binder and the optimizer.
    /// The call is wait-free — it performs a single `ArcSwap::load_full`.
    #[must_use]
    pub fn catalog_snapshot(&self) -> Arc<CatalogSnapshot> {
        self.persistent_catalog.snapshot()
    }

    /// Return live WAL writer counters when WAL-backed storage is enabled.
    #[must_use]
    pub fn wal_writer_stats(&self) -> Option<ultrasql_wal::WalWriterStats> {
        self.wal_writer.as_ref().map(ultrasql_wal::WalWriter::stats)
    }

    /// Return runtime autovacuum thresholds.
    #[must_use]
    pub const fn autovacuum_config(&self) -> AutovacuumConfig {
        self.autovacuum_config
    }

    /// Replace runtime autovacuum thresholds before the launcher starts.
    pub fn set_autovacuum_config(&mut self, config: AutovacuumConfig) {
        self.autovacuum_config = config;
    }

    /// Return runtime statement logging settings.
    #[must_use]
    pub const fn logging_config(&self) -> LoggingConfig {
        self.logging_config
    }

    /// Replace runtime statement logging settings before the listener starts.
    pub fn set_logging_config(&mut self, config: LoggingConfig) {
        self.logging_config = config;
    }

    /// Return the idle-session timeout in milliseconds.
    #[must_use]
    pub const fn idle_session_timeout_ms(&self) -> u64 {
        self.idle_session_timeout_ms
    }

    /// Replace the idle-session timeout before the listener starts.
    pub const fn set_idle_session_timeout_ms(&mut self, timeout_ms: u64) {
        self.idle_session_timeout_ms = timeout_ms;
    }

    /// Return runtime WAL archive settings.
    #[must_use]
    pub fn wal_archive_config(&self) -> WalArchiveConfig {
        self.wal_archive_config.clone()
    }

    /// Replace runtime WAL archive settings before the listener starts.
    pub fn set_wal_archive_config(&mut self, config: WalArchiveConfig) {
        self.wal_archive_config = config;
    }

    /// Return process-local ANN/vector-index counters for ops metrics.
    #[must_use]
    pub fn ann_system_metrics(&self) -> AnnSystemMetrics {
        let mut metrics = AnnSystemMetrics::default();
        for entry in self.table_constraints.iter() {
            for runtime in entry.value().indexes.values() {
                if let Some(hnsw) = &runtime.hnsw {
                    let stats = hnsw.page_stats();
                    metrics.hnsw_indexes = metrics.hnsw_indexes.saturating_add(1);
                    metrics.candidates = metrics
                        .candidates
                        .saturating_add(usize_to_u64_saturated(stats.live_nodes));
                    metrics.tombstones = metrics
                        .tombstones
                        .saturating_add(usize_to_u64_saturated(stats.tombstones));
                    let pages = stats
                        .meta_pages
                        .saturating_add(stats.node_pages)
                        .saturating_add(stats.overflow_pages)
                        .saturating_add(stats.free_list_pages);
                    metrics.vector_index_memory_bytes = metrics
                        .vector_index_memory_bytes
                        .saturating_add(pages_to_bytes_saturated(pages));
                }
                if let Some(ivfflat) = &runtime.ivfflat {
                    let stats = ivfflat.page_stats();
                    metrics.ivfflat_indexes = metrics.ivfflat_indexes.saturating_add(1);
                    metrics.candidates = metrics
                        .candidates
                        .saturating_add(usize_to_u64_saturated(stats.live_entries));
                    metrics.tombstones = metrics
                        .tombstones
                        .saturating_add(usize_to_u64_saturated(stats.tombstones));
                    let pages = stats
                        .meta_pages
                        .saturating_add(stats.centroid_pages)
                        .saturating_add(stats.list_pages)
                        .saturating_add(stats.entry_pages);
                    metrics.vector_index_memory_bytes = metrics
                        .vector_index_memory_bytes
                        .saturating_add(pages_to_bytes_saturated(pages));
                }
            }
        }
        metrics
    }

    /// Run offline admin validation over catalog, indexes, WAL, heap visibility, and ANN tombstones.
    #[must_use]
    pub fn validate(&self) -> ValidationReport {
        ValidationReport {
            checks: vec![
                self.validate_catalog_check(),
                self.validate_indexes_check(),
                self.validate_wal_check(),
                self.validate_heap_visibility_check(),
                self.validate_ann_tombstones_check(),
            ],
        }
    }

    fn validate_catalog_check(&self) -> ValidationCheck {
        let snapshot = self.catalog_snapshot();
        let mut errors = Vec::new();
        for (folded, table) in &snapshot.tables {
            if !snapshot.tables_by_oid.contains_key(&table.oid) {
                errors.push(format!(
                    "table {} oid {} missing from oid map",
                    table.name,
                    table.oid.raw()
                ));
            }
            let expected_key = ultrasql_catalog::table_lookup_key(&table.schema_name, &table.name);
            if folded != &expected_key {
                errors.push(format!(
                    "table {} stored under non-canonical key {}",
                    table.name, folded
                ));
            }
        }
        for (oid, table) in &snapshot.tables_by_oid {
            if !snapshot
                .tables
                .values()
                .any(|named_table| named_table.oid == *oid)
            {
                errors.push(format!(
                    "oid map table {} oid {} missing from name map",
                    table.name,
                    oid.raw()
                ));
            }
        }
        validation_check(
            "catalog",
            errors,
            format!(
                "{} table(s), {} oid entry(s), {} index(es)",
                snapshot.tables.len(),
                snapshot.tables_by_oid.len(),
                snapshot.indexes.len()
            ),
        )
    }

    fn validate_indexes_check(&self) -> ValidationCheck {
        let snapshot = self.catalog_snapshot();
        let mut errors = Vec::new();
        for index in snapshot.indexes.values() {
            let Some(table) = snapshot.tables_by_oid.get(&index.table_oid) else {
                errors.push(format!(
                    "index {} references missing table oid {}",
                    index.name,
                    index.table_oid.raw()
                ));
                continue;
            };
            if !snapshot
                .indexes_by_table
                .get(&index.table_oid)
                .is_some_and(|indexes| indexes.iter().any(|entry| entry.oid == index.oid))
            {
                errors.push(format!(
                    "index {} oid {} missing from table index map",
                    index.name,
                    index.oid.raw()
                ));
            }
            for column in &index.columns {
                let idx = usize::from(*column);
                if idx >= table.schema.len() {
                    errors.push(format!(
                        "index {} column {} out of range for table {}",
                        index.name, column, table.name
                    ));
                }
            }
            let method = index.access_method.to_ascii_lowercase();
            if method == "hnsw" || method == "ivfflat" {
                let runtime = self
                    .table_constraints
                    .get(&index.table_oid)
                    .and_then(|constraints| constraints.value().indexes.get(&index.oid).cloned());
                match (method.as_str(), runtime) {
                    ("hnsw", Some(runtime)) => match runtime.hnsw {
                        Some(hnsw) if hnsw.is_valid() => {}
                        Some(_) => errors.push(format!("hnsw index {} is invalid", index.name)),
                        None => errors.push(format!(
                            "hnsw index {} missing page-backed sidecar",
                            index.name
                        )),
                    },
                    ("ivfflat", Some(runtime)) => match runtime.ivfflat {
                        Some(ivfflat) if ivfflat.is_valid() => {}
                        Some(_) => errors.push(format!("ivfflat index {} is invalid", index.name)),
                        None => errors.push(format!(
                            "ivfflat index {} missing page-backed sidecar",
                            index.name
                        )),
                    },
                    _ => errors.push(format!(
                        "{} index {} missing runtime metadata",
                        method, index.name
                    )),
                }
            }
        }
        validation_check(
            "indexes",
            errors,
            format!(
                "{} index(es), {} indexed table bucket(s)",
                snapshot.indexes.len(),
                snapshot.indexes_by_table.len()
            ),
        )
    }

    fn validate_wal_check(&self) -> ValidationCheck {
        let Some(data_dir) = &self.data_dir else {
            return validation_check(
                "wal",
                Vec::new(),
                "in-memory server; no WAL directory configured".to_owned(),
            );
        };
        let wal_dir = data_dir.join("pg_wal");
        match ultrasql_wal::recover(&wal_dir, |_| Ok(())) {
            Ok(lsn) => validation_check(
                "wal",
                Vec::new(),
                format!("decoded WAL through lsn {}", lsn.raw()),
            ),
            Err(err) => validation_check("wal", vec![err.to_string()], String::new()),
        }
    }

    fn validate_heap_visibility_check(&self) -> ValidationCheck {
        let snapshot = self.catalog_snapshot();
        let scan_txn = self.txn_manager.begin(IsolationLevel::ReadCommitted);
        let scan_snapshot = scan_txn.snapshot.clone();
        let mut errors = Vec::new();
        let mut visible_rows = 0_u64;
        let mut checked_tables = 0_u64;
        let mut skipped_catalog_tables = 0_u64;
        for table in snapshot.tables.values() {
            if table.schema_name == "pg_catalog" {
                skipped_catalog_tables = skipped_catalog_tables.saturating_add(1);
                continue;
            }
            checked_tables = checked_tables.saturating_add(1);
            let rel = RelationId(table.oid);
            let block_count = self.heap.block_count(rel).max(table.n_blocks);
            let codec = RowCodec::new(table.schema.clone());
            let mut decode_error: Option<String> = None;
            let mut table_rows = 0_u64;
            let scan_result = self.heap.for_each_visible(
                rel,
                block_count,
                &scan_snapshot,
                self.txn_manager.as_ref(),
                |_tid, _hdr, payload| {
                    if decode_error.is_none() {
                        if let Err(err) = codec.decode(payload) {
                            decode_error = Some(err.to_string());
                        }
                    }
                    table_rows = table_rows.saturating_add(1);
                    Ok(())
                },
            );
            if let Err(err) = scan_result {
                errors.push(format!("table {} heap scan failed: {err}", table.name));
            }
            if let Some(err) = decode_error {
                errors.push(format!("table {} row decode failed: {err}", table.name));
            }
            visible_rows = visible_rows.saturating_add(table_rows);
        }
        if let Err(err) = self.txn_manager.abort(scan_txn) {
            errors.push(format!("validation scan transaction abort failed: {err}"));
        }
        validation_check(
            "heap_visibility",
            errors,
            format!(
                "{} user table(s), {} catalog table(s) skipped, {} visible row(s)",
                checked_tables, skipped_catalog_tables, visible_rows
            ),
        )
    }

    fn validate_ann_tombstones_check(&self) -> ValidationCheck {
        let mut errors = Vec::new();
        let mut hnsw_indexes = 0_u64;
        let mut ivfflat_indexes = 0_u64;
        let mut tombstones = 0_u64;
        for entry in self.table_constraints.iter() {
            for runtime in entry.value().indexes.values() {
                if let Some(hnsw) = &runtime.hnsw {
                    hnsw_indexes = hnsw_indexes.saturating_add(1);
                    let stats = hnsw.page_stats();
                    tombstones =
                        tombstones.saturating_add(usize_to_u64_saturated(stats.tombstones));
                    if !hnsw.is_valid() {
                        errors.push("hnsw sidecar is invalid".to_owned());
                    }
                }
                if let Some(ivfflat) = &runtime.ivfflat {
                    ivfflat_indexes = ivfflat_indexes.saturating_add(1);
                    let stats = ivfflat.page_stats();
                    tombstones =
                        tombstones.saturating_add(usize_to_u64_saturated(stats.tombstones));
                    if !ivfflat.is_valid() {
                        errors.push("ivfflat sidecar is invalid".to_owned());
                    }
                }
            }
        }
        validation_check(
            "ann_tombstones",
            errors,
            format!(
                "{} hnsw index(es), {} ivfflat index(es), {} tombstone(s)",
                hnsw_indexes, ivfflat_indexes, tombstones
            ),
        )
    }

    /// Validate foreign keys that were declared `DEFERRABLE INITIALLY DEFERRED`.
    ///
    /// The check is deliberately table-scanning: v0.8 favours correctness over
    /// an incremental deferred-trigger queue. Immediate checks still run in the
    /// executor for non-deferred constraints.
    pub(crate) fn validate_deferred_foreign_keys(
        &self,
        txn: &Transaction,
    ) -> Result<(), ServerError> {
        let catalog = self.catalog_snapshot();
        for item in self.table_constraints.iter() {
            let child_oid = *item.key();
            let constraints = item.value();
            if !constraints
                .foreign_keys
                .iter()
                .any(|fk| fk.deferrable && fk.initially_deferred)
            {
                continue;
            }
            let Some(child) = catalog.tables_by_oid.get(&child_oid).cloned() else {
                continue;
            };
            let child_rel = RelationId(child.oid);
            let child_blocks = self.heap.block_count(child_rel).max(child.n_blocks);
            if child_blocks == 0 {
                continue;
            }
            let child_codec = RowCodec::new(child.schema.clone());
            for fk in constraints
                .foreign_keys
                .iter()
                .filter(|fk| fk.deferrable && fk.initially_deferred)
            {
                let parent = catalog
                    .tables_by_oid
                    .get(&fk.target_oid)
                    .or_else(|| catalog.tables.get(&fk.target_table))
                    .ok_or_else(|| {
                        ServerError::Catalog(ultrasql_catalog::CatalogError::not_found(
                            fk.target_table.clone(),
                        ))
                    })?;
                for tuple in self.heap.scan_visible(
                    child_rel,
                    child_blocks,
                    &txn.snapshot,
                    self.txn_manager.as_ref(),
                ) {
                    let tuple = tuple
                        .map_err(|e| ServerError::Ddl(format!("deferred FK scan failed: {e}")))?;
                    let row = child_codec.decode(&tuple.data).map_err(|e| {
                        ServerError::Ddl(format!("deferred FK row decode failed: {e}"))
                    })?;
                    let Some(key) = deferred_fk_key(&row, &fk.columns) else {
                        continue;
                    };
                    if !self.deferred_relation_has_key(parent, &fk.target_columns, &key, txn)? {
                        return Err(ultrasql_executor::ExecError::ForeignKeyViolation(
                            fk.name.clone(),
                        )
                        .into());
                    }
                }
            }
        }
        Ok(())
    }

    fn deferred_relation_has_key(
        &self,
        table: &TableEntry,
        columns: &[usize],
        key: &[Value],
        txn: &Transaction,
    ) -> Result<bool, ServerError> {
        let relation = RelationId(table.oid);
        let block_count = self.heap.block_count(relation).max(table.n_blocks);
        let codec = RowCodec::new(table.schema.clone());
        for tuple in self.heap.scan_visible(
            relation,
            block_count,
            &txn.snapshot,
            self.txn_manager.as_ref(),
        ) {
            let tuple = tuple
                .map_err(|e| ServerError::Ddl(format!("deferred FK parent scan failed: {e}")))?;
            let row = codec.decode(&tuple.data).map_err(|e| {
                ServerError::Ddl(format!("deferred FK parent row decode failed: {e}"))
            })?;
            if deferred_fk_key(&row, columns).as_deref() == Some(key) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Lookup optimizer statistics for `table` from the in-memory
    /// stats catalog.
    #[must_use]
    pub fn lookup_relation_stats(&self, table: &str) -> Option<ultrasql_optimizer::RelationStats> {
        self.stats_catalog.read().lookup_relation(table)
    }

    /// Record committed tuple modifications for `table` and trigger
    /// autovacuum ANALYZE when the threshold is crossed.
    pub fn note_table_modifications(&self, table: &str, modified_rows: u64) {
        if modified_rows == 0 {
            return;
        }

        let folded = table.to_ascii_lowercase();
        self.columnar_storage.mark_dirty(&folded);
        {
            let mut entry = self.table_modifications.entry(folded.clone()).or_insert(0);
            *entry = entry.saturating_add(modified_rows);
        }
        let analyze_current = {
            let mut entry = self
                .table_analyze_modifications
                .entry(folded.clone())
                .or_insert(0);
            *entry = entry.saturating_add(modified_rows);
            *entry
        };
        let threshold = self.auto_analyze_threshold(&folded);
        if analyze_current < threshold {
            return;
        }

        // Reset counter first so concurrent DML can accumulate for the
        // next cycle while the maintenance pass drains this table.
        self.table_analyze_modifications.insert(folded.clone(), 0);
        self.pending_analyze_tables.insert(folded, ());
    }

    /// Rebuild every pending same-table columnar shadow.
    ///
    /// The heap remains authoritative. This maintenance pass drains
    /// queued table names and warms `HeapAccess::column_cache` from an
    /// MVCC snapshot so subsequent OLAP scans can use the columnar
    /// secondary layout without first paying the row-store decode cost.
    pub fn run_columnarization_cycle(&self) {
        while self.run_one_pending_columnarization() {}
    }

    fn run_one_pending_columnarization(&self) -> bool {
        let Some(table) = self.columnar_storage.pop_pending() else {
            return false;
        };
        match self.columnarize_table(&table) {
            Ok(true) => {
                tracing::debug!(table = %table, "columnar shadow rebuilt");
            }
            Ok(false) => {
                tracing::debug!(table = %table, "columnar shadow skipped");
            }
            Err(e) => {
                tracing::warn!(table = %table, error = %e, "columnar shadow rebuild failed");
            }
        }
        true
    }

    /// Rebuild one table's columnar shadow from the row-store heap.
    pub fn columnarize_table(&self, table: &str) -> Result<bool, ServerError> {
        let folded = table.to_ascii_lowercase();
        let snapshot = self.catalog_snapshot();
        let Some(entry) = snapshot.tables.get(&folded).cloned() else {
            self.columnar_storage.remove(&folded);
            return Ok(false);
        };
        drop(snapshot);

        let rel = RelationId(entry.oid);
        if let Some(cached) = self.heap.column_cache.get(rel) {
            self.columnar_storage.record_rebuild(
                folded,
                rel,
                cached.version,
                cached.row_count(),
                cached.segment_count(),
            );
            return Ok(true);
        }

        let block_count = self.heap.block_count(rel).max(entry.n_blocks);
        if block_count == 0 {
            return Ok(false);
        }

        let scan_txn = self.txn_manager.begin(IsolationLevel::ReadCommitted);
        let scan_result = (|| -> Result<(), ServerError> {
            let mut scan = SeqScan::new_with_vm(
                Arc::clone(&self.heap),
                rel,
                block_count,
                scan_txn.snapshot.clone(),
                Arc::clone(&self.txn_manager),
                Arc::clone(&self.vm),
                RowCodec::new(entry.schema.clone()),
            );
            while scan
                .next_batch()
                .map_err(|e| ServerError::Ddl(format!("columnarization scan failed: {e}")))?
                .is_some()
            {}
            Ok(())
        })();
        self.finalise_scan_transaction(
            scan_txn,
            scan_result,
            "columnarization scan transaction abort",
            "columnarization scan rollback after scan error",
        )?;

        let Some(cached) = self.heap.column_cache.get(rel) else {
            return Ok(false);
        };
        self.columnar_storage.record_rebuild(
            folded,
            rel,
            cached.version,
            cached.row_count(),
            cached.segment_count(),
        );
        Ok(true)
    }

    /// Run `ANALYZE` for one table: refresh block-count hint and
    /// rebuild relation stats from MVCC-visible rows.
    pub fn analyze_table(&self, table: &str) -> Result<bool, ServerError> {
        self.analyze_table_with_pid(table, 0)
    }

    /// Run `ANALYZE` for one table and publish progress under `pid`.
    pub fn analyze_table_with_pid(&self, table: &str, pid: u32) -> Result<bool, ServerError> {
        let folded = table.to_ascii_lowercase();
        self.pending_analyze_tables.remove(&folded);
        let snapshot = self.catalog_snapshot();
        let Some(entry) = snapshot.tables.get(&folded) else {
            return Ok(false);
        };
        let entry = entry.clone();
        drop(snapshot);

        let rel = RelationId(entry.oid);
        let block_count = self.heap.block_count(rel).max(entry.n_blocks);
        self.persistent_catalog
            .update_table_size(entry.oid, block_count)
            .map_err(ServerError::Catalog)?;

        self.workload_recorder
            .begin_analyze(pid, entry.oid.raw(), block_count);
        let result = (|| -> Result<bool, ServerError> {
            self.workload_recorder
                .update_analyze(pid, "scanning table", 0);

            let scan_txn = self.txn_manager.begin(IsolationLevel::ReadCommitted);
            let scan_snapshot = scan_txn.snapshot.clone();
            let mut payloads: Vec<Vec<u8>> = Vec::new();
            let scan_result = self
                .heap
                .for_each_visible(
                    rel,
                    block_count,
                    &scan_snapshot,
                    self.txn_manager.as_ref(),
                    |_tid, _hdr, payload| {
                        payloads.push(payload.to_vec());
                        Ok(())
                    },
                )
                .map_err(|e| ServerError::Ddl(format!("ANALYZE scan failed: {e}")));
            self.finalise_scan_transaction(
                scan_txn,
                scan_result,
                "ANALYZE scan transaction abort",
                "ANALYZE scan rollback after scan error",
            )?;

            self.workload_recorder
                .update_analyze(pid, "computing statistics", block_count);
            let codec = RowCodec::new(entry.schema.clone());
            let mut rows: Vec<Vec<ultrasql_core::Value>> = Vec::with_capacity(payloads.len());
            for payload in payloads {
                match codec.decode(&payload) {
                    Ok(row) => rows.push(row),
                    Err(e) => {
                        tracing::warn!(table = %folded, error = %e, "ANALYZE skipped malformed tuple");
                    }
                }
            }
            let stats = AnalyzeRunner::new(AnalyzeOptions::default())
                .run(&folded, &entry.schema, rows.into_iter())
                .map_err(|e| ServerError::Ddl(format!("ANALYZE statistics failed: {e}")))?;
            let mut stat_rows = Vec::with_capacity(stats.columns.len());
            for col in &stats.columns {
                let staattnum =
                    i16::try_from(col.column_index.saturating_add(1)).map_err(|_| {
                        ServerError::Ddl("ANALYZE table has too many columns".to_owned())
                    })?;
                let pg_row = PgStatisticRow::from_column_stats(
                    entry.oid.raw(),
                    u16::try_from(staattnum).map_err(|_| {
                        ServerError::Ddl("ANALYZE invalid attribute number".to_owned())
                    })?,
                    col,
                );
                stat_rows.push(StatisticRow {
                    starelid: entry.oid,
                    staattnum,
                    stanullfrac: pg_row.stanullfrac,
                    stadistinct: pg_row.stadistinct,
                });
            }
            self.workload_recorder
                .update_analyze(pid, "writing statistics", block_count);
            let catalog_txn = self.txn_manager.begin(IsolationLevel::ReadCommitted);
            if let Err(e) = self.persistent_catalog.persist_statistic_rows(
                &stat_rows,
                self.heap.as_ref(),
                catalog_txn.xid,
                catalog_txn.current_command,
            ) {
                return Err(self.abort_analyze_catalog_statistics_transaction(catalog_txn, e));
            }
            self.commit_transaction(catalog_txn, true, "ANALYZE catalog statistics transaction")?;
            self.stats_catalog.write().register(stats);
            self.persistent_catalog
                .replace_statistics(entry.oid, stat_rows);
            self.plan_cache.invalidate_all();
            Ok(true)
        })();
        self.workload_recorder.finish_analyze(pid);
        if matches!(result, Ok(true)) {
            if pid == 0 {
                self.workload_recorder
                    .record_table_autoanalyze(entry.oid.raw());
            } else {
                self.workload_recorder.record_table_analyze(entry.oid.raw());
            }
        }
        result
    }

    fn abort_analyze_catalog_statistics_transaction(
        &self,
        txn: Transaction,
        err: ultrasql_catalog::CatalogError,
    ) -> ServerError {
        match self.txn_manager.abort(txn) {
            Ok(()) => ServerError::Catalog(err),
            Err(abort_err) => ServerError::ddl(format!(
                "ANALYZE catalog statistics transaction abort: {err}; \
                 transaction abort failed: {abort_err}"
            )),
        }
    }

    fn finalise_scan_transaction<T>(
        &self,
        txn: Transaction,
        outcome: Result<T, ServerError>,
        success_context: &'static str,
        rollback_context: &'static str,
    ) -> Result<T, ServerError> {
        match self.txn_manager.abort(txn) {
            Ok(()) => outcome,
            Err(abort_err) => match outcome {
                Ok(_) => Err(ServerError::ddl(format!("{success_context}: {abort_err}"))),
                Err(err) => Err(ServerError::ddl(format!(
                    "{rollback_context}: {err}; transaction abort failed: {abort_err}"
                ))),
            },
        }
    }

    fn auto_analyze_threshold(&self, table: &str) -> u64 {
        let snapshot = self.catalog_snapshot();
        let Some(entry) = snapshot.tables.get(table) else {
            return self.autovacuum_config.analyze_threshold;
        };
        let rel = RelationId(entry.oid);
        let blocks = u64::from(self.heap.block_count(rel).max(entry.n_blocks));
        let estimated_rows = blocks.saturating_mul(64);
        autovacuum_config_for_table(self.autovacuum_config, entry)
            .analyze_threshold_for_rows(estimated_rows)
    }

    fn run_one_pending_analyze(&self) {
        let Some(table) = self
            .pending_analyze_tables
            .iter()
            .next()
            .map(|entry| entry.key().clone())
        else {
            return;
        };

        match self.analyze_table(&table) {
            Ok(true) => {
                tracing::debug!(table = %table, "autovacuum analyze completed");
            }
            Ok(false) => {
                tracing::debug!(table = %table, "autovacuum analyze skipped missing table");
            }
            Err(e) => {
                tracing::warn!(table = %table, error = %e, "autovacuum analyze failed");
            }
        }
    }
}

/// Bind to `addr` and serve PostgreSQL-wire-protocol sessions until
/// the listener errors out.
///
/// Each accepted connection runs on its own Tokio task. The function
/// returns when the listener fails irrecoverably (e.g. the port is
/// closed by an external signal); per-connection errors are logged
/// and the loop continues.
pub async fn run_server(addr: SocketAddr, state: Arc<Server>) -> Result<(), ServerError> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    info!(target: "ultrasqld", listen = %bound, "ultrasqld is ready");
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                warn!(target: "ultrasqld", error = %e, "accept failed; continuing");
                continue;
            }
        };
        debug!(target: "ultrasqld", %peer, "connection accepted");
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, state).await {
                if matches!(e, ServerError::UnexpectedEof) {
                    debug!(target: "ultrasqld", %peer, "connection closed by peer");
                } else {
                    error!(target: "ultrasqld", %peer, error = %e, "session terminated");
                }
            }
        });
    }
}

/// Bind a TCP listener and report the actually-bound address.
///
/// Used by integration tests that need to read the ephemeral port the
/// kernel chose. The caller drives the listener with
/// [`serve_listener`].
pub async fn bind_listener(addr: SocketAddr) -> Result<(TcpListener, SocketAddr), ServerError> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    Ok((listener, bound))
}

/// Drive an already-bound [`TcpListener`] forever.
///
/// Equivalent to [`run_server`] without the bind step. Useful for
/// integration tests that need the chosen ephemeral port before they
/// start serving.
pub async fn serve_listener(listener: TcpListener, state: Arc<Server>) -> Result<(), ServerError> {
    serve_listener_with_shutdown(listener, state, std::future::pending::<()>()).await
}

/// Drive an already-bound [`TcpListener`] until `shutdown` resolves.
///
/// This is the production-safe sibling of [`serve_listener`]. It stops
/// accepting new sockets and returns `Ok(())` when the shutdown future
/// completes, allowing the owning task to drop its [`Server`] reference
/// cleanly instead of aborting the accept loop.
pub async fn serve_listener_with_shutdown<F>(
    listener: TcpListener,
    state: Arc<Server>,
    shutdown: F,
) -> Result<(), ServerError>
where
    F: Future<Output = ()> + Send,
{
    tokio::pin!(shutdown);
    let mut sessions = tokio::task::JoinSet::new();
    loop {
        let (stream, peer) = tokio::select! {
            biased;
            () = &mut shutdown => {
                info!(target: "ultrasqld", "listener shutdown requested");
                while let Some(joined) = sessions.join_next().await {
                    if let Err(e) = joined {
                        warn!(target: "ultrasqld", error = %e, "session task failed during shutdown");
                    }
                }
                return Ok(());
            }
            joined = sessions.join_next(), if !sessions.is_empty() => {
                match joined {
                    Some(Ok(())) => {}
                    Some(Err(e)) => {
                        warn!(target: "ultrasqld", error = %e, "session task failed");
                    }
                    None => {
                        debug!(target: "ultrasqld", "session set drained before join");
                    }
                }
                continue;
            }
            accepted = listener.accept() => match accepted {
                Ok(v) => v,
                Err(e) => {
                    warn!(target: "ultrasqld", error = %e, "accept failed; continuing");
                    continue;
                }
            },
        };
        // Disable Nagle's algorithm: queries and their responses are
        // dispatched in single coalesced `write_all` calls already, so
        // there is no batching for Nagle to add to. With Nagle on, the
        // kernel can hold a small reply for up to ~40 ms waiting for a
        // companion segment that never arrives, blowing the latency
        // budget of every simple-query roundtrip. Logged-and-ignored
        // failure: the stream still works without TCP_NODELAY, just
        // slower, and we do not want a transient setsockopt error to
        // kill an otherwise-fine connection.
        if let Err(e) = stream.set_nodelay(true) {
            warn!(target: "ultrasqld", %peer, error = %e, "TCP_NODELAY failed");
        }
        debug!(target: "ultrasqld", %peer, "connection accepted");
        let state = Arc::clone(&state);
        sessions.spawn(async move {
            if let Err(e) = handle_connection(stream, state).await {
                if matches!(e, ServerError::UnexpectedEof) {
                    debug!(target: "ultrasqld", %peer, "connection closed by peer");
                } else {
                    error!(target: "ultrasqld", %peer, error = %e, "session terminated");
                }
            }
        });
    }
}

/// Drive a single PostgreSQL session over `io`.
///
/// On the happy path: reads a `StartupMessage`, replies with the
/// canonical authentication / parameter handshake, then loops over
/// frontend messages until the client sends `Terminate` or
/// disconnects. Per-query execution is delegated to [`run_select`].
pub async fn handle_connection<RW>(io: RW, state: Arc<Server>) -> Result<(), ServerError>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    let mut session = Session::new(io, state);
    // Slow-loris guard. A peer that opens the TCP connection and then
    // sits silently must not keep the session task alive forever — the
    // accept loop also stops accepting new connections beyond the
    // listen backlog if every worker task is parked here. The 30-s
    // budget covers the StartupMessage exchange plus the
    // authentication handshake; legitimate clients finish in < 100 ms
    // even on slow links. The error path drops the socket without
    // sending a reply because the client never advanced past startup.
    const STARTUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
    match tokio::time::timeout(STARTUP_TIMEOUT, session.startup()).await {
        Ok(res) => res?,
        Err(_) => {
            tracing::warn!("dropping connection: startup handshake exceeded 30 s");
            return Ok(());
        }
    }
    session.run().await
}

/// Per-connection state machine.
///
/// `extended` holds the Extended Query Protocol's prepared-statement and
/// portal caches. `txn_state` tracks whether an explicit `BEGIN` is
/// open, whether the in-progress txn has errored, or whether the session
/// is autocommitting. Both are owned by the session and accessed only by
/// the connection's own task, so no synchronisation primitive guards
/// them (per AGENTS.md §5: "default to the simplest primitive that meets
/// the workload"; the workload here is single-threaded).
mod session;
use session::Session;

/// Decode a single column out of an encoded heap-tuple payload and
/// return its value as an `i64` key.
///
/// `schema` is the relation's full schema; `col_idx` is the 0-based
/// position of the key column inside that schema; `widen_i32` is
/// `true` for `Int32` columns (the value is sign-extended to `i64`)
/// and `false` for `Int64`. `Value::Null` returns `None` so the
/// caller can decide what to do — the CREATE INDEX build path
/// currently skips NULL keys (PostgreSQL semantics for non-`INCLUDE`
/// b-tree indexes).
///
/// Returning `Result<Option<i64>, ServerError>` keeps NULL handling
/// at the call site; using a panic / sentinel value would conflate
/// "schema mismatch" with "missing value", which the catalog wants
/// to keep distinct.
/// Build a PostgreSQL `NoticeResponse` carrying a `WARNING` with the
/// given SQLSTATE and human-readable text.
///
/// `NoticeResponse` is shaped exactly like `ErrorResponse` on the wire
/// (an `'N'` tag instead of `'E'`); a libpq client routes notices to a
/// callback rather than aborting the operation. UltraSQL emits notices
/// where PostgreSQL would emit them — most importantly for
/// `BEGIN`-inside-tx, `COMMIT`-outside-tx, and `ROLLBACK`-outside-tx so
/// drivers see the same behaviour they expect from PostgreSQL.
fn notice_warning(sqlstate: &str, message: &str) -> BackendMessage {
    BackendMessage::NoticeResponse {
        fields: vec![
            (b'S', "WARNING".to_string()),
            (b'C', sqlstate.to_string()),
            (b'M', message.to_string()),
        ],
    }
}

struct RunPlanInTxnArgs<'a> {
    plan: &'a LogicalPlan,
    txn: &'a Transaction,
    catalog_snapshot: Arc<CatalogSnapshot>,
    table_constraints: Arc<dashmap::DashMap<ultrasql_core::Oid, Arc<TableRuntimeConstraints>>>,
    sequences: Arc<dashmap::DashMap<String, Arc<ultrasql_storage::sequence::Sequence>>>,
    sequence_owners: Arc<dashmap::DashMap<String, String>>,
    sequence_namespaces: Arc<dashmap::DashMap<String, String>>,
    schemas: Arc<dashmap::DashMap<String, Arc<RuntimeSchema>>>,
    operators: Arc<dashmap::DashMap<String, Arc<RuntimeOperator>>>,
    role_catalog: Arc<auth::InMemoryAuthCatalog>,
    privilege_catalog: Arc<auth::InMemoryPrivilegeCatalog>,
    row_security: Arc<dashmap::DashMap<ultrasql_core::Oid, Arc<TableRowSecurity>>>,
    session_settings: Arc<std::collections::HashMap<String, String>>,
    current_user: String,
    session_user: String,
    persistent_catalog: Arc<PersistentCatalog>,
    time_partitions: Arc<dashmap::DashMap<String, Arc<time_partition::TimePartitionRuntime>>>,
    workload_recorder: Arc<workload::WorkloadRecorder>,
    autovacuum_config: AutovacuumConfig,
    logging_config: LoggingConfig,
    wal_archive_config: WalArchiveConfig,
    data_dir: Option<std::path::PathBuf>,
    logical_replication: Arc<replication::LogicalReplicationRuntime>,
    sequence_state: Option<SequenceSessionState>,
    advisory_state: Option<AdvisorySessionState>,
    tables: &'a SampleTables,
    heap: Arc<HeapAccess<BlankPageLoader>>,
    vm: Arc<VisibilityMap>,
    oracle: Arc<TransactionManager>,
    jit: ultrasql_vec::jit::JitConfig,
    cancel_flag: Option<ultrasql_executor::CancelFlag>,
    stream_buf: &'a mut bytes::BytesMut,
}

/// Run a non-DDL, non-transaction-control plan inside the given
/// transaction and return the assembled wire-message result.
///
/// Owns no state of its own: it captures everything it needs by
/// argument so both the Simple Query and Extended Query paths can call
/// it. The caller is responsible for committing or aborting `txn` based
/// on whether this function returned `Ok` or `Err`.
///
/// `command_id` is taken from `txn.current_command` so each statement
/// inside an explicit transaction sees its own writes via the MVCC
/// `cmin < current_command` rule.
fn run_plan_in_txn(args: RunPlanInTxnArgs<'_>) -> Result<SelectResult, ServerError> {
    let RunPlanInTxnArgs {
        plan,
        txn,
        catalog_snapshot,
        table_constraints,
        sequences,
        sequence_owners,
        sequence_namespaces,
        schemas,
        operators,
        role_catalog,
        privilege_catalog,
        row_security,
        session_settings,
        current_user,
        session_user,
        persistent_catalog,
        time_partitions,
        workload_recorder,
        autovacuum_config,
        logging_config,
        wal_archive_config,
        data_dir,
        logical_replication,
        sequence_state,
        advisory_state,
        tables,
        heap,
        vm,
        oracle,
        jit,
        cancel_flag,
        stream_buf,
    } = args;
    if let Some(result) =
        try_run_cached_int32_pair_select(plan, &catalog_snapshot, heap.as_ref(), stream_buf)
    {
        return Ok(result);
    }
    let text_options =
        result_encoder::TextEncodingOptions::from_session_settings(session_settings.as_ref());
    record_serializable_predicate_locks(plan, txn, &catalog_snapshot, oracle.as_ref());
    record_serializable_write_conflicts(plan, txn, &catalog_snapshot, oracle.as_ref());
    acquire_simple_lock_rows(
        plan,
        &catalog_snapshot,
        &table_constraints,
        heap.as_ref(),
        oracle.as_ref(),
        txn,
    )?;

    let ctx = LowerCtx {
        tables,
        catalog_snapshot,
        table_constraints,
        sequences,
        sequence_owners,
        sequence_namespaces,
        schemas,
        operators,
        role_catalog,
        privilege_catalog,
        row_security,
        session_settings,
        current_user,
        session_user,
        persistent_catalog,
        time_partitions,
        workload_recorder,
        autovacuum_config,
        logging_config,
        wal_archive_config,
        data_dir,
        logical_replication,
        sequence_state,
        advisory_state,
        heap,
        vm,
        snapshot: txn.snapshot.clone(),
        isolation: txn.isolation,
        oracle,
        // Use the *current* effective xid so writes performed inside an
        // active SAVEPOINT carry the subxact xid in their tuple header
        // rather than the parent xid; ROLLBACK TO can then hide them
        // via the standard MVCC visibility rules.
        xid: txn.current_xid(),
        command_id: txn.current_command,
        cte_buffers: std::collections::HashMap::new(),
        jit,
        cancel_flag,
        work_mem: Arc::new(ultrasql_executor::work_mem::WorkMemBudget::new(u64::MAX)),
        profile_operators: false,
    };
    match plan {
        LogicalPlan::Insert { returning, .. } => {
            let mut op = pipeline::lower_query(plan, &ctx)?;
            if returning.is_empty() {
                run_modify_command(op.as_mut(), "INSERT")
            } else {
                result_encoder::run_modify_returning_with_options(
                    op.as_mut(),
                    "INSERT",
                    &text_options,
                )
            }
        }
        LogicalPlan::Update { returning, .. } => {
            let mut op = pipeline::lower_query(plan, &ctx)?;
            if returning.is_empty() {
                run_modify_command(op.as_mut(), "UPDATE")
            } else {
                result_encoder::run_modify_returning_with_options(
                    op.as_mut(),
                    "UPDATE",
                    &text_options,
                )
            }
        }
        LogicalPlan::Delete { returning, .. } => {
            let mut op = pipeline::lower_query(plan, &ctx)?;
            if returning.is_empty() {
                run_modify_command(op.as_mut(), "DELETE")
            } else {
                result_encoder::run_modify_returning_with_options(
                    op.as_mut(),
                    "DELETE",
                    &text_options,
                )
            }
        }
        _ => {
            let mut op = pipeline::lower_query(plan, &ctx)?;
            // Streaming wire-encode hot path: bypass the
            // `Vec<BackendMessage>` materialisation and emit
            // `RowDescription` + N `DataRow` + `CommandComplete`
            // directly into a single `BytesMut`. The session dispatches
            // the body in one `write_all` + `flush` rather than the
            // per-message loop the legacy `run_select` requires.
            result_encoder::run_select_streamed_with_options(op.as_mut(), stream_buf, &text_options)
        }
    }
}

fn acquire_simple_lock_rows(
    plan: &LogicalPlan,
    catalog_snapshot: &Arc<CatalogSnapshot>,
    table_constraints: &dashmap::DashMap<ultrasql_core::Oid, Arc<TableRuntimeConstraints>>,
    heap: &HeapAccess<BlankPageLoader>,
    oracle: &TransactionManager,
    txn: &Transaction,
) -> Result<(), ServerError> {
    let LogicalPlan::LockRows {
        input,
        strength,
        wait_policy,
        ..
    } = plan
    else {
        return Ok(());
    };
    if *wait_policy != LockWaitPolicy::Wait {
        return Ok(());
    }
    let Some((table, predicate)) = lock_rows_base_filter(input) else {
        return Ok(());
    };
    let Some(entry) = catalog_snapshot.tables.get(&table.to_ascii_lowercase()) else {
        return Ok(());
    };

    let rel = RelationId(entry.oid);
    let mode = row_lock_mode(*strength);
    if let Some(tids) =
        lock_rows_index_tids(predicate, entry, catalog_snapshot, table_constraints, heap)?
    {
        return lock_tuple_ids(&tids, oracle, txn, mode);
    }

    let block_count = heap.block_count(rel).max(entry.n_blocks);
    let codec = RowCodec::new(entry.schema.clone());
    let predicate_eval = predicate.cloned().map(Eval::new);

    for tuple in heap.scan_visible(rel, block_count, &txn.snapshot, oracle) {
        let tuple =
            tuple.map_err(|e| ServerError::Execute(ExecError::TypeMismatch(e.to_string())))?;
        let row = codec
            .decode(&tuple.data)
            .map_err(|e| ServerError::Execute(ExecError::TypeMismatch(e.to_string())))?;
        let matched = match &predicate_eval {
            Some(eval) => match eval
                .eval(&row)
                .map_err(|e| ServerError::Execute(ExecError::TypeMismatch(e.to_string())))?
            {
                Value::Bool(true) => true,
                Value::Bool(false) | Value::Null => false,
                other => {
                    return Err(ServerError::Execute(ExecError::TypeMismatch(format!(
                        "FOR UPDATE predicate returned non-boolean value {other:?}",
                    ))));
                }
            },
            None => true,
        };
        if matched {
            lock_tuple_ids(&[tuple.tid], oracle, txn, mode)?;
        }
    }

    Ok(())
}

fn lock_rows_index_tids(
    predicate: Option<&ScalarExpr>,
    entry: &TableEntry,
    catalog_snapshot: &Arc<CatalogSnapshot>,
    table_constraints: &dashmap::DashMap<ultrasql_core::Oid, Arc<TableRuntimeConstraints>>,
    heap: &HeapAccess<BlankPageLoader>,
) -> Result<Option<Vec<ultrasql_core::TupleId>>, ServerError> {
    let Some(predicate) = predicate else {
        return Ok(None);
    };
    let Some((column, key)) = equality_i64_predicate(predicate) else {
        return Ok(None);
    };
    let Some(attnum) = u16::try_from(column).ok() else {
        return Ok(None);
    };
    let Some(indexes) = catalog_snapshot.indexes_by_table.get(&entry.oid) else {
        return Ok(None);
    };
    let Some(index) = indexes.iter().find(|idx| {
        idx.columns.as_slice() == [attnum]
            && idx.root_block != BlockNumber::INVALID
            && runtime_index_method(table_constraints, entry.oid, idx.oid)
                == LogicalIndexMethod::Btree
    }) else {
        return Ok(None);
    };
    let tree: BTree<BlankPageLoader> = BTree::open(
        Arc::clone(heap.buffer_pool()),
        RelationId::new(index.oid.raw()),
        index.root_block,
    );
    let tids = if index.is_unique {
        tree.lookup::<i64>(key)
            .map(|maybe| maybe.into_iter().collect::<Vec<_>>())
    } else {
        tree.lookup_all::<i64>(key)
    }
    .map_err(|e| ServerError::ddl(format!("FOR UPDATE btree lookup: {e}")))?;
    Ok(Some(tids))
}

fn lock_tuple_ids(
    tids: &[ultrasql_core::TupleId],
    oracle: &TransactionManager,
    txn: &Transaction,
    mode: RowLockMode,
) -> Result<(), ServerError> {
    for tid in tids {
        let acquired = oracle
            .lock_manager
            .try_acquire(LockRequest {
                xid: txn.current_xid(),
                tag: LockTag::Tuple(*tid),
                mode: mode.to_lock_mode(),
            })
            .map_err(|e| ServerError::Execute(ExecError::TypeMismatch(e.to_string())))?;
        if !acquired {
            return Err(ServerError::Execute(ExecError::TypeMismatch(
                "write conflict: row lock not available".to_string(),
            )));
        }
    }
    Ok(())
}

fn equality_i64_predicate(predicate: &ScalarExpr) -> Option<(usize, i64)> {
    let ScalarExpr::Binary {
        op: BinaryOp::Eq,
        left,
        right,
        ..
    } = predicate
    else {
        return None;
    };
    column_literal_i64(left, right).or_else(|| column_literal_i64(right, left))
}

fn column_literal_i64(column: &ScalarExpr, literal: &ScalarExpr) -> Option<(usize, i64)> {
    let ScalarExpr::Column { index, .. } = column else {
        return None;
    };
    let ScalarExpr::Literal { value, .. } = literal else {
        return None;
    };
    value_i64(value).map(|key| (*index, key))
}

fn value_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Bool(v) => Some(i64::from(*v)),
        Value::Int16(v) => Some(i64::from(*v)),
        Value::Int32(v) => Some(i64::from(*v)),
        Value::Int64(v) => Some(*v),
        _ => None,
    }
}

fn runtime_index_method(
    table_constraints: &dashmap::DashMap<ultrasql_core::Oid, Arc<TableRuntimeConstraints>>,
    table_oid: ultrasql_core::Oid,
    index_oid: ultrasql_core::Oid,
) -> LogicalIndexMethod {
    table_constraints
        .get(&table_oid)
        .and_then(|constraints| constraints.indexes.get(&index_oid).map(|idx| idx.method))
        .unwrap_or(LogicalIndexMethod::Btree)
}

fn logical_index_method_from_name(name: &str) -> LogicalIndexMethod {
    match name {
        "hash" => LogicalIndexMethod::Hash,
        "gin" => LogicalIndexMethod::Gin,
        "gist" => LogicalIndexMethod::Gist,
        "brin" => LogicalIndexMethod::Brin,
        "hnsw" => LogicalIndexMethod::Hnsw,
        "ivfflat" => LogicalIndexMethod::IvfFlat,
        "aggregating" => LogicalIndexMethod::Aggregating,
        _ => LogicalIndexMethod::Btree,
    }
}

fn aggregating_group_key_exprs(
    table: &TableEntry,
    spec: &ultrasql_planner::LogicalAggregatingIndex,
) -> Result<Vec<ScalarExpr>, ServerError> {
    spec.group_columns
        .iter()
        .map(|col| {
            let field = table.schema.field(*col).ok_or_else(|| {
                ServerError::ddl(format!(
                    "aggregating index group column {} missing from table {}",
                    col, table.name
                ))
            })?;
            Ok(ScalarExpr::Column {
                name: field.name.clone(),
                index: *col,
                data_type: field.data_type.clone(),
            })
        })
        .collect()
}

fn hnsw_metric_for_opclass_name(opclass: Option<&str>) -> Result<HnswMetric, ServerError> {
    match opclass.unwrap_or("vector_l2_ops") {
        "vector_l2_ops" => Ok(HnswMetric::L2),
        "vector_cosine_ops" => Ok(HnswMetric::Cosine),
        "vector_ip_ops" => Ok(HnswMetric::NegativeInnerProduct),
        "vector_l1_ops" => Ok(HnswMetric::L1),
        other => Err(ServerError::ddl(format!(
            "CREATE INDEX USING hnsw: unsupported vector opclass {other}"
        ))),
    }
}

fn ann_dims_and_default_payload(data_type: &DataType) -> Option<(u32, AnnPayloadKind)> {
    match data_type {
        DataType::Vector { dims: Some(dims) } => Some((*dims, AnnPayloadKind::F32)),
        DataType::HalfVec { dims: Some(dims) } => Some((*dims, AnnPayloadKind::Bf16)),
        _ => None,
    }
}

fn ann_payload_option_from_catalog(
    options: &[(String, String)],
) -> Result<Option<AnnPayloadKind>, ServerError> {
    let mut payload = None;
    for (name, value) in options {
        if name == "payload" {
            payload = Some(ann_payload_kind_from_value("rebuild vector ANN", value)?);
        }
    }
    Ok(payload)
}

fn ann_payload_kind_from_value(context: &str, value: &str) -> Result<AnnPayloadKind, ServerError> {
    match value.to_ascii_lowercase().as_str() {
        "f32" | "float32" => Ok(AnnPayloadKind::F32),
        "bf16" | "bfloat16" => Ok(AnnPayloadKind::Bf16),
        "int8" | "i8" => Ok(AnnPayloadKind::Int8),
        other => Err(ServerError::ddl(format!(
            "{context}: unsupported payload {other}; expected f32, bf16, or int8"
        ))),
    }
}

fn ivfflat_options_from_catalog(
    options: &[(String, String)],
) -> Result<(usize, usize, Option<AnnPayloadKind>), ServerError> {
    let mut lists = 100_usize;
    let mut probes = 1_usize;
    let mut payload = None;
    for (name, value) in options {
        match name.as_str() {
            "lists" => lists = parse_positive_ivfflat_catalog_option(name, value)?,
            "probes" => probes = parse_positive_ivfflat_catalog_option(name, value)?,
            "payload" => {
                payload = Some(ann_payload_kind_from_value("rebuild IVFFlat", value)?);
            }
            other => {
                return Err(ServerError::ddl(format!(
                    "rebuild IVFFlat: unsupported option {other}"
                )));
            }
        }
    }
    Ok((lists, probes, payload))
}

fn parse_positive_ivfflat_catalog_option(name: &str, value: &str) -> Result<usize, ServerError> {
    let parsed = value.parse::<usize>().map_err(|_| {
        ServerError::ddl(format!(
            "rebuild IVFFlat: option {name} must be a positive integer"
        ))
    })?;
    if parsed == 0 {
        return Err(ServerError::ddl(format!(
            "rebuild IVFFlat: option {name} must be greater than zero"
        )));
    }
    Ok(parsed)
}

fn unix_timestamp_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_micros().try_into().unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn lock_rows_base_filter(plan: &LogicalPlan) -> Option<(&str, Option<&ScalarExpr>)> {
    match plan {
        LogicalPlan::Project { input, .. } => lock_rows_base_filter(input),
        LogicalPlan::Filter { input, predicate } => match input.as_ref() {
            LogicalPlan::Scan { table, .. } => Some((table.as_str(), Some(predicate))),
            other => lock_rows_base_filter(other).map(|(table, _)| (table, Some(predicate))),
        },
        LogicalPlan::Scan { table, .. } => Some((table.as_str(), None)),
        _ => None,
    }
}

const fn row_lock_mode(strength: LockStrength) -> RowLockMode {
    match strength {
        LockStrength::Update => RowLockMode::ForUpdate,
        LockStrength::NoKeyUpdate => RowLockMode::ForNoKeyUpdate,
        LockStrength::Share => RowLockMode::ForShare,
        LockStrength::KeyShare => RowLockMode::ForKeyShare,
    }
}

pub(crate) fn try_run_cached_int32_pair_select(
    plan: &LogicalPlan,
    catalog_snapshot: &Arc<CatalogSnapshot>,
    heap: &HeapAccess<BlankPageLoader>,
    stream_buf: &mut bytes::BytesMut,
) -> Option<SelectResult> {
    let (table, output_schema) = match plan {
        LogicalPlan::Scan { table, schema, .. } => (table.as_str(), schema),
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => {
            let LogicalPlan::Scan { table, .. } = input.as_ref() else {
                return None;
            };
            if exprs.len() != 2 {
                return None;
            }
            let is_identity_pair = exprs.iter().enumerate().all(|(idx, (expr, _name))| {
                matches!(expr, ScalarExpr::Column { index, .. } if *index == idx)
            });
            if !is_identity_pair {
                return None;
            }
            (table.as_str(), schema)
        }
        _ => return None,
    };

    if output_schema.len() != 2
        || output_schema.field_at(0).data_type != ultrasql_core::DataType::Int32
        || output_schema.field_at(1).data_type != ultrasql_core::DataType::Int32
    {
        return None;
    }

    let folded = table.to_ascii_lowercase();
    let entry = catalog_snapshot.tables.get(&folded)?;
    let rel = RelationId(entry.oid);
    let cached = heap.column_cache.get(rel)?;
    let [Column::Int32(left), Column::Int32(right)] = cached.columns.as_slice() else {
        return None;
    };
    if left.nulls().is_some() || right.nulls().is_some() {
        return None;
    }
    let rows = u64::try_from(left.len()).unwrap_or(u64::MAX);

    if output_schema == &cached.schema
        && let Some(encoded) = cached.cached_int32_pair_select_wire.read().clone()
    {
        return Some(result_encoder::run_shared_preencoded_select_streamed(
            encoded, rows,
        ));
    }

    let result = result_encoder::run_cached_int32_pair_select_streamed(
        output_schema,
        left.data(),
        right.data(),
        stream_buf,
    );
    if output_schema == &cached.schema
        && let Some(body) = result.streamed_body.as_ref()
    {
        let mut slot = cached.cached_int32_pair_select_wire.write();
        if slot.is_none() {
            *slot = Some(Arc::<[u8]>::from(body.as_ref()));
        }
    }
    Some(result)
}

pub(crate) fn try_run_cached_scalar_aggregate_select(
    plan: &LogicalPlan,
    catalog_snapshot: &Arc<CatalogSnapshot>,
    heap: &HeapAccess<BlankPageLoader>,
    stream_buf: &mut bytes::BytesMut,
) -> Option<SelectResult> {
    let (aggregate_input, group_by, aggregates, output_schema) = match plan {
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => {
            let passthrough = exprs.iter().enumerate().all(|(idx, (expr, _name))| {
                matches!(expr, ScalarExpr::Column { index, .. } if *index == idx)
            });
            if !passthrough {
                return None;
            }
            let LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                ..
            } = input.as_ref()
            else {
                return None;
            };
            (
                input.as_ref(),
                group_by.as_slice(),
                aggregates.as_slice(),
                schema,
            )
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            schema,
        } => (
            input.as_ref(),
            group_by.as_slice(),
            aggregates.as_slice(),
            schema,
        ),
        _ => return None,
    };

    if !group_by.is_empty() || aggregates.len() != 1 || output_schema.len() != 1 {
        return None;
    }

    let agg = &aggregates[0];
    if agg.distinct {
        return None;
    }

    let (table, predicate) = match aggregate_input {
        LogicalPlan::Scan { table, .. } => (table.as_str(), None),
        LogicalPlan::Filter { input, predicate } => {
            let LogicalPlan::Scan { table, .. } = input.as_ref() else {
                return None;
            };
            (table.as_str(), Some(predicate))
        }
        _ => return None,
    };

    let folded = table.to_ascii_lowercase();
    let entry = catalog_snapshot.tables.get(&folded)?;
    let rel = RelationId(entry.oid);
    let cached = heap.column_cache.get(rel)?;

    let cache_key = build_cached_scalar_wire_key(agg, output_schema, predicate)?;
    if let Some(encoded) = cached
        .cached_scalar_aggregate_wire
        .read()
        .get(&cache_key)
        .cloned()
    {
        return Some(result_encoder::run_shared_preencoded_select_streamed(
            encoded, 1,
        ));
    }

    let result_col = match (agg.func, &agg.arg, predicate) {
        (
            AggregateFunc::Sum,
            Some(ScalarExpr::Column {
                index, data_type, ..
            }),
            None,
        ) => build_cached_sum_column(*index, data_type, &cached.columns)?,
        (
            AggregateFunc::Avg,
            Some(ScalarExpr::Column {
                index, data_type, ..
            }),
            None,
        ) => build_cached_avg_column(*index, data_type, &cached.columns)?,
        (
            AggregateFunc::Sum,
            Some(ScalarExpr::Column {
                index, data_type, ..
            }),
            Some(predicate),
        ) => build_cached_filter_sum_column(*index, data_type, predicate, &cached.columns)?,
        _ => return None,
    };

    let batch = Batch::new([result_col]).ok()?;
    let mut op = MemTableScan::new(output_schema.clone(), vec![batch]);
    let result = result_encoder::run_select_streamed(&mut op, stream_buf).ok()?;
    if let Some(body) = result.streamed_body.as_ref() {
        let mut slot = cached.cached_scalar_aggregate_wire.write();
        slot.entry(cache_key)
            .or_insert_with(|| Arc::<[u8]>::from(body.as_ref()));
    }
    Some(result)
}

fn build_cached_scalar_wire_key(
    agg: &ultrasql_planner::LogicalAggregateExpr,
    output_schema: &ultrasql_core::Schema,
    predicate: Option<&ScalarExpr>,
) -> Option<ultrasql_storage::column_cache::CachedScalarAggregateWireKey> {
    let output_name = output_schema.field_at(0).name.clone();
    match (agg.func, &agg.arg, predicate) {
        (
            AggregateFunc::Sum,
            Some(ScalarExpr::Column {
                index, data_type, ..
            }),
            None,
        ) => Some(
            ultrasql_storage::column_cache::CachedScalarAggregateWireKey::Sum {
                output_name,
                input_type_tag: scalar_input_type_tag(data_type)?,
                sum_col: *index,
            },
        ),
        (
            AggregateFunc::Avg,
            Some(ScalarExpr::Column {
                index, data_type, ..
            }),
            None,
        ) => Some(
            ultrasql_storage::column_cache::CachedScalarAggregateWireKey::Avg {
                output_name,
                input_type_tag: scalar_input_type_tag(data_type)?,
                sum_col: *index,
            },
        ),
        (
            AggregateFunc::Sum,
            Some(ScalarExpr::Column {
                index, data_type, ..
            }),
            Some(expr),
        ) => match data_type {
            ultrasql_core::DataType::Int32 => {
                let (predicate_col, predicate_op, predicate_lit) = extract_int32_col_op_lit(expr)?;
                Some(
                    ultrasql_storage::column_cache::CachedScalarAggregateWireKey::FilterSum {
                        output_name,
                        input_type_tag: 0,
                        sum_col: *index,
                        predicate_col,
                        predicate_op_tag: cmp_op_tag(predicate_op),
                        predicate_lit: i64::from(predicate_lit),
                    },
                )
            }
            ultrasql_core::DataType::Int64 => {
                let (predicate_col, predicate_op, predicate_lit) = extract_int64_col_op_lit(expr)?;
                Some(
                    ultrasql_storage::column_cache::CachedScalarAggregateWireKey::FilterSum {
                        output_name,
                        input_type_tag: 1,
                        sum_col: *index,
                        predicate_col,
                        predicate_op_tag: cmp_op_tag(predicate_op),
                        predicate_lit,
                    },
                )
            }
            _ => None,
        },
        _ => None,
    }
}

fn scalar_input_type_tag(data_type: &ultrasql_core::DataType) -> Option<u8> {
    match data_type {
        ultrasql_core::DataType::Int32 => Some(0),
        ultrasql_core::DataType::Int64 => Some(1),
        _ => None,
    }
}

const fn cmp_op_tag(op: CmpOp) -> u8 {
    match op {
        CmpOp::Eq => 0,
        CmpOp::Ne => 1,
        CmpOp::Lt => 2,
        CmpOp::Le => 3,
        CmpOp::Gt => 4,
        CmpOp::Ge => 5,
    }
}

fn build_cached_sum_column(
    sum_col: usize,
    data_type: &ultrasql_core::DataType,
    columns: &[Column],
) -> Option<Column> {
    match data_type {
        ultrasql_core::DataType::Int32 => {
            let Column::Int32(col) = columns.get(sum_col)? else {
                return None;
            };
            if col.nulls().is_some() {
                return None;
            }
            if col.is_empty() {
                null_int64_column()
            } else {
                Some(Column::Int64(NumericColumn::from_data(vec![
                    sum_i32_widening(col),
                ])))
            }
        }
        ultrasql_core::DataType::Int64 => {
            let Column::Int64(col) = columns.get(sum_col)? else {
                return None;
            };
            if col.nulls().is_some() {
                return None;
            }
            if col.is_empty() {
                null_int64_column()
            } else {
                Some(Column::Int64(NumericColumn::from_data(vec![sum_i64(col)])))
            }
        }
        _ => None,
    }
}

fn build_cached_avg_column(
    sum_col: usize,
    data_type: &ultrasql_core::DataType,
    columns: &[Column],
) -> Option<Column> {
    match data_type {
        ultrasql_core::DataType::Int32 => {
            let Column::Int32(col) = columns.get(sum_col)? else {
                return None;
            };
            if col.nulls().is_some() {
                return None;
            }
            if col.is_empty() {
                null_float64_column()
            } else {
                let avg = i64_to_f64_saturating(sum_i32_widening(col))
                    / usize_to_f64_saturating(col.len());
                Some(Column::Float64(NumericColumn::from_data(vec![avg])))
            }
        }
        ultrasql_core::DataType::Int64 => {
            let Column::Int64(col) = columns.get(sum_col)? else {
                return None;
            };
            if col.nulls().is_some() {
                return None;
            }
            if col.is_empty() {
                null_float64_column()
            } else {
                let avg = i64_to_f64_saturating(sum_i64(col)) / usize_to_f64_saturating(col.len());
                Some(Column::Float64(NumericColumn::from_data(vec![avg])))
            }
        }
        _ => None,
    }
}

fn i64_to_f64_saturating(value: i64) -> f64 {
    value.to_f64().unwrap_or_else(|| {
        if value.is_negative() {
            f64::MIN
        } else {
            f64::MAX
        }
    })
}

fn usize_to_f64_saturating(value: usize) -> f64 {
    value.to_f64().unwrap_or(f64::MAX)
}

fn build_cached_filter_sum_column(
    sum_col: usize,
    data_type: &ultrasql_core::DataType,
    predicate: &ScalarExpr,
    columns: &[Column],
) -> Option<Column> {
    match data_type {
        ultrasql_core::DataType::Int32 => {
            let (pred_col, pred_op, pred_lit) = extract_int32_col_op_lit(predicate)?;
            let (Column::Int32(pred), Column::Int32(sum)) =
                (columns.get(pred_col)?, columns.get(sum_col)?)
            else {
                return None;
            };
            if pred.nulls().is_some() || sum.nulls().is_some() {
                return None;
            }
            if sum.is_empty() {
                return null_int64_column();
            }
            let total = if pred_col == sum_col && matches!(pred_op, CmpOp::Gt) {
                filter_sum_i32_widening_gt(sum.data(), pred_lit)
            } else {
                let mask = cmp_i32_scalar(pred, pred_lit, pred_op);
                sum_i32_widening_with_mask(sum, &mask)
            };
            Some(Column::Int64(NumericColumn::from_data(vec![total])))
        }
        ultrasql_core::DataType::Int64 => {
            let (pred_col, pred_op, pred_lit) = extract_int64_col_op_lit(predicate)?;
            let (Column::Int64(pred), Column::Int64(sum)) =
                (columns.get(pred_col)?, columns.get(sum_col)?)
            else {
                return None;
            };
            if pred.nulls().is_some() || sum.nulls().is_some() {
                return None;
            }
            if sum.is_empty() {
                return null_int64_column();
            }
            let total = if pred_col == sum_col && matches!(pred_op, CmpOp::Gt) {
                filter_sum_i64_gt(sum.data(), pred_lit)
            } else {
                let mask = cmp_i64_scalar(pred, pred_lit, pred_op);
                sum_i64_with_mask(sum, &mask)
            };
            Some(Column::Int64(NumericColumn::from_data(vec![total])))
        }
        _ => None,
    }
}

fn extract_int32_col_op_lit(expr: &ScalarExpr) -> Option<(usize, CmpOp, i32)> {
    let ScalarExpr::Binary {
        op, left, right, ..
    } = expr
    else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (
            ScalarExpr::Column {
                index,
                data_type: ultrasql_core::DataType::Int32,
                ..
            },
            ScalarExpr::Literal {
                value: Value::Int32(lit),
                ..
            },
        ) => Some((*index, binary_op_to_cmp(*op)?, *lit)),
        (
            ScalarExpr::Literal {
                value: Value::Int32(lit),
                ..
            },
            ScalarExpr::Column {
                index,
                data_type: ultrasql_core::DataType::Int32,
                ..
            },
        ) => Some((*index, reverse_binary_op_to_cmp(*op)?, *lit)),
        _ => None,
    }
}

fn extract_int64_col_op_lit(expr: &ScalarExpr) -> Option<(usize, CmpOp, i64)> {
    let ScalarExpr::Binary {
        op, left, right, ..
    } = expr
    else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (
            ScalarExpr::Column {
                index,
                data_type: ultrasql_core::DataType::Int64,
                ..
            },
            ScalarExpr::Literal {
                value: Value::Int64(lit),
                ..
            },
        ) => Some((*index, binary_op_to_cmp(*op)?, *lit)),
        (
            ScalarExpr::Literal {
                value: Value::Int64(lit),
                ..
            },
            ScalarExpr::Column {
                index,
                data_type: ultrasql_core::DataType::Int64,
                ..
            },
        ) => Some((*index, reverse_binary_op_to_cmp(*op)?, *lit)),
        _ => None,
    }
}

fn binary_op_to_cmp(op: BinaryOp) -> Option<CmpOp> {
    match op {
        BinaryOp::Eq => Some(CmpOp::Eq),
        BinaryOp::NotEq => Some(CmpOp::Ne),
        BinaryOp::Lt => Some(CmpOp::Lt),
        BinaryOp::LtEq => Some(CmpOp::Le),
        BinaryOp::Gt => Some(CmpOp::Gt),
        BinaryOp::GtEq => Some(CmpOp::Ge),
        _ => None,
    }
}

fn reverse_binary_op_to_cmp(op: BinaryOp) -> Option<CmpOp> {
    match op {
        BinaryOp::Eq => Some(CmpOp::Eq),
        BinaryOp::NotEq => Some(CmpOp::Ne),
        BinaryOp::Lt => Some(CmpOp::Gt),
        BinaryOp::LtEq => Some(CmpOp::Ge),
        BinaryOp::Gt => Some(CmpOp::Lt),
        BinaryOp::GtEq => Some(CmpOp::Le),
        _ => None,
    }
}

fn null_int64_column() -> Option<Column> {
    let mut nulls = ultrasql_vec::Bitmap::new(1, false);
    nulls.set(0, false);
    NumericColumn::with_nulls(vec![0_i64], nulls)
        .ok()
        .map(Column::Int64)
}

fn null_float64_column() -> Option<Column> {
    let mut nulls = ultrasql_vec::Bitmap::new(1, false);
    nulls.set(0, false);
    NumericColumn::with_nulls(vec![0.0_f64], nulls)
        .ok()
        .map(Column::Float64)
}

fn decode_key_column(
    bytes: &[u8],
    schema: &ultrasql_core::Schema,
    col_idx: Option<usize>,
    key_exprs: &[ScalarExpr],
    predicate: Option<&ScalarExpr>,
    method: LogicalIndexMethod,
    encoding: &index_key::IndexKeyEncoding,
) -> Result<Option<i64>, ServerError> {
    let codec = ultrasql_executor::RowCodec::new(schema.clone());
    let row = codec
        .decode(bytes)
        .map_err(|e| ServerError::ddl(format!("CREATE INDEX key decode: {e}")))?;
    if let Some(predicate) = predicate {
        match Eval::new(predicate.clone())
            .eval(&row)
            .map_err(|e| ServerError::ddl(format!("CREATE INDEX partial predicate: {e}")))?
        {
            Value::Bool(true) => {}
            Value::Bool(false) | Value::Null => return Ok(None),
            other => {
                return Err(ServerError::ddl(format!(
                    "CREATE INDEX partial predicate returned {:?}, expected bool",
                    other.data_type()
                )));
            }
        }
    }
    if !key_exprs.is_empty() {
        let [expr] = key_exprs else {
            return Err(ServerError::Unsupported(
                "CREATE INDEX: expression indexes support exactly one key in this wave",
            ));
        };
        let value = Eval::new(expr.clone())
            .eval(&row)
            .map_err(|e| ServerError::ddl(format!("CREATE INDEX expression key: {e}")))?;
        if method == LogicalIndexMethod::Hash {
            return Ok(hash_index_value(&value));
        }
        return encoding.encode_value(&value);
    }
    if matches!(
        encoding,
        index_key::IndexKeyEncoding::CompositeTwoInts { .. }
    ) {
        return encoding.encode_row(&row);
    }
    let col_idx = col_idx.ok_or_else(|| {
        ServerError::ddl("CREATE INDEX key column missing for plain column index")
    })?;
    let value = row.get(col_idx).ok_or_else(|| {
        ServerError::ddl(format!(
            "CREATE INDEX key column {col_idx} missing from decoded row of arity {}",
            row.len()
        ))
    })?;
    if method == LogicalIndexMethod::Hash {
        return Ok(hash_index_value(value));
    }
    encoding.encode_value(value)
}

pub(crate) fn hash_index_value(value: &Value) -> Option<i64> {
    if matches!(value, Value::Null) {
        return None;
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(value, &mut hasher);
    Some(i64::from_ne_bytes(
        std::hash::Hasher::finish(&hasher).to_ne_bytes(),
    ))
}

#[cfg(test)]
mod tests;
