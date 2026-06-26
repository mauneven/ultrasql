//! Logical-plan to physical-operator conversion.
//!
//! The v0.5 server lowers a small subset of `LogicalPlan` nodes into
//! the executor's `Operator` tree. Anything outside that subset is
//! reported via `ServerError::Unsupported` so the client sees a
//! precise error rather than a panic.
//!
//! Supported lowerings:
//!
//! - `LogicalPlan::Scan` -> `MemTableScan` backed by per-table
//!   pre-materialized batches loaded by [`SampleTables`] at startup.
//! - `LogicalPlan::Filter` -> `Filter` backed by the executor scalar
//!   expression evaluator plus vectorised comparison fast paths.
//! - `LogicalPlan::Project` over pure column references ->
//!   `Project`.
//! - `LogicalPlan::Limit` -> `Limit` (`LIMIT n OFFSET m`,
//!   `OFFSET m` with no `LIMIT`, and the common `LIMIT n OFFSET 0`).
//! - `LogicalPlan::Sort` -> `Sort` (in-memory or external sorted runs,
//!   depending on the statement `work_mem` budget).
//! - `LogicalPlan::SetOp` -> `SetOp` for `UNION`, `INTERSECT`, and
//!   `EXCEPT` (each in both `ALL` and `DISTINCT` quantifier forms).
//!   The two children are lowered recursively through the same path so
//!   a set-op can sit on top of any other supported lowering. The
//!   binder is responsible for arity and per-column type compatibility
//!   (see `binder::bind_set_op`); we re-check arity at lowering time
//!   so a hand-built plan that bypasses the binder still surfaces a
//!   precise error rather than producing wrong rows.
//! - `LogicalPlan::Cte` -> materialise the definition into
//!   `CteScan`-backed batches once per query execution; the body is
//!   lowered with the CTE name bound to the buffer via the
//!   [`LowerCtx::cte_buffers`] overlay, so every body-side reference
//!   reuses the same materialised rows. `WITH RECURSIVE` is rejected
//!   in this wave; the executor's fixpoint loop is the v0.6 follow-up.
//!
//! ## Why an inline lowerer
//!
//! The executor crate ships [`ultrasql_executor::physical::build_operator`],
//! which performs the same lowering at a higher level. The sample lowerer
//! stays separate because it reads pre-built [`SampleTables`] batches instead
//! of heap relations; the integration point is `lower_plan` and its
//! `SampleTables` parameter.

use std::collections::HashMap;
use std::sync::Arc;

use ultrasql_catalog::{CatalogSnapshot, persistent::PersistentCatalog};
use ultrasql_core::{CommandId, Schema, Xid};
use ultrasql_executor::physical::{BuildError, DataSource};
use ultrasql_mvcc::Snapshot;
use ultrasql_planner::{InMemoryCatalog, TableMeta};

#[cfg(test)]
use crate::error::ServerError;
#[cfg(test)]
use ultrasql_core::{Field, RelationId};
#[cfg(test)]
use ultrasql_executor::{Operator, RowCodec};
#[cfg(test)]
use ultrasql_planner::{BinaryOp, LogicalJoinCondition, LogicalJoinType};
use ultrasql_storage::heap::HeapAccess;
use ultrasql_storage::vm::VisibilityMap;
use ultrasql_txn::{IsolationLevel, TransactionManager};
use ultrasql_vec::Batch;
#[cfg(test)]
use ultrasql_vec::column::Column;

use crate::BlankPageLoader;

/// Saturate a `u64` row count from the binder into the executor's
/// `usize` row-count space.
///
/// `Limit::with_offset` accepts `usize` for both the row cap and the
/// row-skip count. On 64-bit targets `usize == u64`, so the conversion
/// never truncates. On 32-bit targets a plan value larger than
/// `usize::MAX` saturates to "no further rows" — the operator handles
/// `usize::MAX` as the "no limit" sentinel, matching how the binder
/// already represents `OFFSET m` with no `LIMIT` clause. Saturation is
/// safer than rejecting the statement: the binder may legitimately
/// produce `u64::MAX` for the `LIMIT NULL` case, and a 32-bit user
/// asking for a literal `LIMIT 5_000_000_000` is asking for "all rows"
/// in practice.
mod agg_fuse;
pub(crate) mod catalog_views;
mod csv_scan;
mod cte_helpers;
mod external_scan;
mod hybrid_search;
mod index_scan;
mod join;
mod json_table_scan;
mod lower_query;
mod lower_simple;
pub(crate) mod modify;
mod object_stream;
mod parquet_scan;
pub(crate) use parquet_scan::{
    ParquetRowGroupSummary, parquet_columns_read_for_plan, parquet_row_group_summary_for_plan,
};
mod scan;
mod time_partition;
mod tpch_q1;
mod tpch_q10;
mod tpch_q11;
mod tpch_q12;
mod tpch_q13;
mod tpch_q14;
mod tpch_q15;
mod tpch_q16;
mod tpch_q17;
mod tpch_q18;
mod tpch_q19;
mod tpch_q2;
mod tpch_q20;
mod tpch_q21;
mod tpch_q3;
mod tpch_q4;
mod tpch_q5;
mod tpch_q6;
mod tpch_q7;
mod tpch_q8;
mod tpch_q9;
mod xml_table_scan;

#[cfg(test)]
mod tests;

pub(crate) use index_scan::{
    late_materialization_summary_for_plan, literal_as_i64, literal_in_same_unit_class_as_column,
    match_indexable_predicate,
};
pub use lower_query::lower_query;
pub use lower_simple::{build_sample_database, lower_plan};

/// Pre-authorization gate for reading server-LOCAL files through the external
/// table functions (`read_csv`, `read_parquet`, `read_json`, `read_ndjson`,
/// `read_arrow`, `read_iceberg`, `iceberg_scan`) and `sniff_csv`.
///
/// These functions open files on the database host using the server process's
/// own filesystem privileges, exactly like a server-side `COPY ... FROM/TO
/// '<path>'`. Without this gate any authenticated non-superuser could read
/// arbitrary server-readable files — TLS private keys, other databases' data
/// files, `/etc/...` — via e.g. `SELECT * FROM read_csv('/etc/passwd')`. We
/// therefore mirror the COPY server-file gate (`Session::ensure_copy_server_file_access`):
/// access is permitted only when `allow_server_files` is `true` (the caller
/// passes `Session::current_role_is_superuser()`), and otherwise the same
/// [`crate::error::ServerError::InsufficientPrivilege`] variant is returned.
///
/// Object-store / remote URIs (`s3://`, `r2://`, …) are NOT host-filesystem
/// access and remain allowed for every role — the walk only fires on path
/// arguments that resolve to a local server file.
///
/// `allow_server_files == true` short-circuits before any plan inspection, so
/// superusers pay nothing. The walk visits every sub-plan (subqueries, CTEs,
/// joins, set operations, `INSERT ... SELECT`, `COPY (SELECT ...)`, and the
/// `EXPLAIN` body) so no query shape can smuggle a local read past the gate.
pub fn ensure_external_local_file_access(
    plan: &ultrasql_planner::LogicalPlan,
    allow_server_files: bool,
) -> Result<(), crate::error::ServerError> {
    if allow_server_files {
        return Ok(());
    }
    if external_scan::plan_reads_local_external_file(plan)? {
        return Err(crate::error::ServerError::InsufficientPrivilege(
            "permission denied for reading server-side files: must be superuser \
             (read_csv/read_parquet/read_json/read_ndjson/read_arrow/read_iceberg/\
             iceberg_scan/sniff_csv on a local path require SUPERUSER; object-store \
             URIs such as s3:// are unaffected)"
                .to_owned(),
        ));
    }
    Ok(())
}

pub(super) fn saturate_row_count(n: u64) -> usize {
    usize::try_from(n).unwrap_or(usize::MAX)
}

/// Per-table fixture: schema plus pre-built batches.
#[derive(Clone, Debug)]
struct SampleTable {
    schema: Schema,
    batches: Vec<Batch>,
}

/// In-memory sample-table registry.
///
/// The server registers tables with the planner's
/// [`InMemoryCatalog`] *and* keeps their pre-built batch contents
/// here. When the lowerer sees a `Scan` it consults the registry to
/// build a fresh `MemTableScan`; the catalog tells the planner what
/// columns exist, the registry tells the executor what rows to emit.
///
/// The registry is `Send + Sync` so a single `Arc<SampleTables>` can
/// be shared across connection tasks.
#[derive(Debug, Default)]
pub struct SampleTables {
    tables: HashMap<String, SampleTable>,
}

impl SampleTables {
    /// Build an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Register a table. The catalog is updated with the schema; the
    /// batches are kept for the executor to find later.
    pub fn register(
        &mut self,
        catalog: &mut InMemoryCatalog,
        name: &str,
        schema: Schema,
        batches: Vec<Batch>,
    ) {
        catalog.register(name, TableMeta::new(schema.clone()));
        self.tables
            .insert(name.to_ascii_lowercase(), SampleTable { schema, batches });
    }

    /// Look up a sample table by case-insensitive name.
    fn lookup(&self, name: &str) -> Option<&SampleTable> {
        self.tables.get(&name.to_ascii_lowercase())
    }
}

/// Bridge for [`DataSource`]: the executor's `build_operator` would
/// also work via this trait, but the inline lowerer here goes direct.
/// The impl is kept so external callers that prefer
/// [`ultrasql_executor::physical::build_operator`] can wire it
/// without ceremony.
impl DataSource for SampleTables {
    fn scan(&self, table: &str) -> Result<(Schema, Vec<Batch>), BuildError> {
        self.lookup(table)
            .map(|t| (t.schema.clone(), t.batches.clone()))
            .ok_or_else(|| BuildError::Source(format!("table not found: '{table}'")))
    }
}

/// Lower a logical plan to a boxed `Operator` tree.
#[derive(Debug)]
pub struct LowerCtx<'a> {
    /// Legacy sample-table registry; used when the catalog snapshot has
    /// no entry for a referenced table.
    pub tables: &'a SampleTables,
    /// Per-statement immutable catalog snapshot.
    pub catalog_snapshot: Arc<CatalogSnapshot>,
    /// Runtime defaults/CHECK constraints keyed by table OID.
    pub table_constraints:
        Arc<dashmap::DashMap<ultrasql_core::Oid, Arc<crate::TableRuntimeConstraints>>>,
    /// Runtime sequence registry keyed by folded sequence name.
    pub sequences: Arc<dashmap::DashMap<String, Arc<ultrasql_storage::sequence::Sequence>>>,
    /// Runtime sequence owners keyed by folded sequence name.
    pub sequence_owners: Arc<dashmap::DashMap<String, String>>,
    /// Runtime sequence namespaces keyed by folded sequence name.
    pub sequence_namespaces: Arc<dashmap::DashMap<String, String>>,
    /// Runtime SQL schemas keyed by folded schema name.
    pub schemas: Arc<dashmap::DashMap<String, Arc<crate::RuntimeSchema>>>,
    /// Runtime user-defined operator registry keyed by signature.
    pub operators: Arc<dashmap::DashMap<String, Arc<crate::RuntimeOperator>>>,
    /// Runtime role catalog backing virtual auth views.
    pub role_catalog: Arc<crate::auth::InMemoryAuthCatalog>,
    /// Runtime privilege catalog backing GRANT/REVOKE behavior.
    pub privilege_catalog: Arc<crate::auth::InMemoryPrivilegeCatalog>,
    /// Runtime row-security registry keyed by table OID.
    pub row_security: Arc<dashmap::DashMap<ultrasql_core::Oid, Arc<crate::TableRowSecurity>>>,
    /// Session-local GUCs used by row-security policy predicates.
    pub session_settings: Arc<HashMap<String, String>>,
    /// Effective role for `current_user` and privilege checks.
    pub current_user: String,
    /// Startup role for `session_user`.
    pub session_user: String,
    /// Mutable persistent catalog used by operators that auto-create
    /// physical children, such as time-series chunks.
    pub persistent_catalog: Arc<PersistentCatalog>,
    /// Runtime time-range partition registry keyed by parent table name.
    pub time_partitions:
        Arc<dashmap::DashMap<String, Arc<crate::time_partition::TimePartitionRuntime>>>,
    /// Same-process workload recorder used by virtual statistics views.
    pub workload_recorder: Arc<crate::workload::WorkloadRecorder>,
    /// Runtime autovacuum settings used by virtual `pg_settings`.
    pub autovacuum_config: crate::AutovacuumConfig,
    /// Runtime statement logging settings used by virtual `pg_settings`.
    pub logging_config: crate::LoggingConfig,
    /// Runtime WAL archive settings used by virtual `pg_settings`.
    pub wal_archive_config: crate::WalArchiveConfig,
    /// Optional WAL-backed data directory used by replication catalog views.
    pub data_dir: Option<std::path::PathBuf>,
    /// Same-process logical replication registry for publication views.
    pub logical_replication: Arc<crate::replication::LogicalReplicationRuntime>,
    /// Session-local sequence observer for defaults that call `nextval`.
    pub sequence_state: Option<crate::SequenceSessionState>,
    /// Session-local PostgreSQL advisory-lock owner.
    pub advisory_state: Option<crate::AdvisorySessionState>,
    /// Shared heap access handle.
    pub heap: Arc<HeapAccess<BlankPageLoader>>,
    /// Shared visibility map for persistent heap relations.
    pub vm: Arc<VisibilityMap>,
    /// MVCC snapshot taken at statement start.
    pub snapshot: Snapshot,
    /// Transaction isolation for the statement being lowered.
    pub isolation: IsolationLevel,
    /// Transaction manager (also serves as `XidStatusOracle` for
    /// `SeqScan`'s visibility check).
    pub oracle: Arc<TransactionManager>,
    /// XID of the autocommit transaction.
    pub xid: Xid,
    /// Command id within `xid` for the current statement.
    pub command_id: CommandId,
    /// Materialised non-recursive CTE bindings, keyed by lower-cased CTE
    /// name. Populated by the `LogicalPlan::Cte` arm in `lower_query`
    /// before lowering the body, then consulted by
    /// `lower_catalog_or_sample_scan` so a body-side
    /// `Scan { table: "<cte_name>" }` resolves to a `CteScan` over the
    /// materialised buffer instead of a catalog or sample-table lookup.
    ///
    /// Default-empty so the constructors used outside the CTE path do
    /// not need to opt in; the field flows through recursive lowering
    /// for free.
    pub cte_buffers: HashMap<String, CteBuffer>,
    /// Per-statement JIT policy. Lowerers pass this into narrow
    /// compiled-kernel operators; generic plans ignore it.
    pub jit: ultrasql_vec::jit::JitConfig,
    /// Per-statement cancel flag clone. The session's `CancelRegistry`
    /// entry holds the canonical flag; this is a shared clone the
    /// lowerer threads into cancellation-aware operators (`SeqScan`,
    /// `HashAggregate`) via their `with_cancel_flag` builders. `None`
    /// for callers that do not participate in a wire session (tests,
    /// in-process fixtures).
    pub cancel_flag: Option<ultrasql_executor::CancelFlag>,
    /// Per-statement memory budget for operators that accumulate
    /// state (sort buffers, hash tables, the join-build side). The
    /// budget is shared by reference so a single statement's working
    /// set is policed across every operator in the plan. The companion
    /// `temp_file_limit` constant
    /// (`ultrasql_executor::work_mem::temp_file_limit`) caps temp-file bytes
    /// each spill writer may generate.
    pub work_mem: std::sync::Arc<ultrasql_executor::work_mem::WorkMemBudget>,
    /// Wrap every lowered physical node in a runtime profiler.
    ///
    /// This is enabled for `EXPLAIN ANALYZE` only. Normal query execution
    /// keeps direct operators so benchmark and production paths avoid
    /// profiling overhead.
    pub profile_operators: bool,
    /// Whether the current role may read server-LOCAL files through the
    /// external table functions (`read_csv`, `read_parquet`, …) and
    /// `sniff_csv`. Set to `Session::current_role_is_superuser()` at every
    /// session construction site — mirroring the COPY server-file gate — and
    /// enforced by [`ensure_external_local_file_access`] at the `lower_query`
    /// entry. Object-store reads are never affected. In-process callers that
    /// do not represent a wire session (tests, fixtures) set this to `true`.
    pub allow_server_files: bool,
}

/// Materialised non-recursive CTE binding.
///
/// Owns the batches produced by running the CTE's definition plan to
/// completion, plus the schema those batches conform to. Multiple
/// references to the same CTE inside the body produce independent
/// `CteScan` operators backed by the same `Arc`-shared buffer, so the
/// definition is evaluated exactly once per query execution (matching
/// PostgreSQL's CTE-materialisation semantics).
#[derive(Clone, Debug)]
pub struct CteBuffer {
    /// All batches produced by the CTE definition plan.
    pub batches: Arc<Vec<Batch>>,
    /// Schema every batch in `batches` conforms to. This is the
    /// definition's output schema (post column-alias rename, since the
    /// binder records aliases on the definition's schema before exposing
    /// the CTE to the body).
    pub schema: Schema,
}
