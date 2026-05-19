//! Logical-plan to physical-operator conversion.
//!
//! The v0.5 server lowers a small subset of [`LogicalPlan`] nodes into
//! the executor's `Operator` tree. Anything outside that subset is
//! reported via [`ServerError::Unsupported`] so the client sees a
//! precise error rather than a panic.
//!
//! Supported lowerings:
//!
//! - [`LogicalPlan::Scan`] -> [`MemTableScan`] backed by per-table
//!   pre-materialized batches loaded by [`SampleTables`] at startup.
//! - [`LogicalPlan::Filter`] with predicate `col = i32_literal` ->
//!   [`FilterEqI32`].
//! - [`LogicalPlan::Project`] over pure column references ->
//!   [`Project`].
//! - [`LogicalPlan::Limit`] -> [`Limit`] (`LIMIT n OFFSET m`,
//!   `OFFSET m` with no `LIMIT`, and the common `LIMIT n OFFSET 0`).
//! - [`LogicalPlan::Sort`] -> [`Sort`] (in-memory; spill-to-disk lands
//!   with the `work_mem` budget in v0.6).
//! - [`LogicalPlan::SetOp`] -> [`SetOp`] for `UNION`, `INTERSECT`, and
//!   `EXCEPT` (each in both `ALL` and `DISTINCT` quantifier forms).
//!   The two children are lowered recursively through the same path so
//!   a set-op can sit on top of any other supported lowering. The
//!   binder is responsible for arity and per-column type compatibility
//!   (see `binder::bind_set_op`); we re-check arity at lowering time
//!   so a hand-built plan that bypasses the binder still surfaces a
//!   precise error rather than producing wrong rows.
//! - [`LogicalPlan::Cte`] -> materialise the definition into
//!   [`CteScan`]-backed batches once per query execution; the body is
//!   lowered with the CTE name bound to the buffer via the
//!   [`LowerCtx::cte_buffers`] overlay, so every body-side reference
//!   reuses the same materialised rows. `WITH RECURSIVE` is rejected
//!   in this wave; the executor's fixpoint loop is the v0.6 follow-up.
//!
//! ## Why an inline lowerer
//!
//! The executor crate ships [`ultrasql_executor::physical::build_operator`],
//! which performs the same lowering at a higher level. The lowerer
//! here is intentionally separate for one reason: the v0.5
//! [`FilterEqI32`] operator only handles numeric columns and rejects
//! a batch that contains a Utf8 column at any position. The server's
//! sample table includes a `name TEXT` column, so we push the
//! projection-required-for-evaluation below the filter and pass the
//! filter only columns it can chew through.
//!
//! Once the executor grows a general expression evaluator and the
//! filter operator stops being type-fussy, this module collapses to a
//! one-line delegation to
//! [`ultrasql_executor::physical::build_operator`]; the integration
//! point is `lower_plan` and its `SampleTables` parameter.

use std::collections::HashMap;
use std::sync::Arc;

use ultrasql_catalog::CatalogSnapshot;
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
use ultrasql_txn::TransactionManager;
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
mod cte_helpers;
mod index_scan;
mod join;
mod lower_query;
mod lower_simple;
mod modify;
mod scan;
mod tpch_q1;
mod tpch_q6;

#[cfg(test)]
mod tests;

pub use lower_query::lower_query;
pub use lower_simple::{build_sample_database, lower_plan};

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
/// build a fresh [`MemTableScan`]; the catalog tells the planner what
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

/// Lower a logical plan to a boxed [`Operator`] tree.
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
    /// Session-local sequence observer for defaults that call `nextval`.
    pub sequence_state: Option<crate::SequenceSessionState>,
    /// Shared heap access handle.
    pub heap: Arc<HeapAccess<BlankPageLoader>>,
    /// Shared visibility map for persistent heap relations.
    pub vm: Arc<VisibilityMap>,
    /// MVCC snapshot taken at statement start.
    pub snapshot: Snapshot,
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
    /// [`lower_catalog_or_sample_scan`] so a body-side
    /// `Scan { table: "<cte_name>" }` resolves to a [`CteScan`] over the
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
    /// set is policed across every operator in the plan. v0.5 sets
    /// the budget to `u64::MAX` (effectively unlimited) because no
    /// operator yet spills to disk; the field is plumbed so v0.6
    /// can light up the spill paths without touching the dispatch
    /// surface. The companion `temp_file_limit` constant
    /// (`ultrasql_executor::work_mem::temp_file_limit`) caps total
    /// temp-file bytes any future spill writer is allowed to
    /// generate; it is enforced vacuously today because no spill
    /// path exists.
    pub work_mem: std::sync::Arc<ultrasql_executor::work_mem::WorkMemBudget>,
}

/// Materialised non-recursive CTE binding.
///
/// Owns the batches produced by running the CTE's definition plan to
/// completion, plus the schema those batches conform to. Multiple
/// references to the same CTE inside the body produce independent
/// [`CteScan`] operators backed by the same `Arc`-shared buffer, so the
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
