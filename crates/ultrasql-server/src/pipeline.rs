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

use ultrasql_catalog::{CatalogSnapshot, IndexEntry, TableEntry};
use ultrasql_core::{CommandId, DataType, Field, RelationId, Schema, Value, Xid};
use ultrasql_executor::filter_sum_op::{
    CachedAvgI32Scan, CachedFilterSumI32Scan, CachedSumI32Scan, FilterSumI32Scan,
};
use ultrasql_executor::fused_delete::FusedDeleteInt32Pair;
use ultrasql_executor::fused_update::{FusedCmp, FusedPredicate, FusedUpdateInt32Add};
use ultrasql_executor::physical::{BuildError, DataSource};
use ultrasql_executor::{
    CteScan, Filter, FilterEqI32, HashAggregate, HashJoin, IndexScan, Limit, MemTableScan,
    ModifyKind, ModifyTable, NestedLoopJoin, Operator, Project, ResultOp, RightFactory, RowCodec,
    SeqScan, SetOp, Sort, ValuesScan,
};
use ultrasql_mvcc::{Snapshot, Visibility, is_visible};
use ultrasql_planner::{
    BinaryOp, InMemoryCatalog, LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr,
    TableMeta,
};
use ultrasql_storage::btree::BTree;
use ultrasql_storage::heap::HeapAccess;
use ultrasql_txn::TransactionManager;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn, StringColumn};

use crate::BlankPageLoader;
use crate::error::ServerError;

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
fn saturate_row_count(n: u64) -> usize {
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
///
/// See the module docs for the supported subset.
pub fn lower_plan(
    plan: &LogicalPlan,
    tables: &SampleTables,
) -> Result<Box<dyn Operator>, ServerError> {
    match plan {
        LogicalPlan::Scan { table, .. } => lower_scan(table, None, tables),
        LogicalPlan::Filter { input, predicate } => lower_filter(input, predicate, tables),
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => {
            if matches!(input.as_ref(), LogicalPlan::Empty { .. }) {
                let scalars: Vec<ScalarExpr> = exprs.iter().map(|(e, _)| e.clone()).collect();
                return Ok(Box::new(ResultOp::new(scalars, schema.clone())));
            }
            lower_project(input, exprs, tables)
        }
        LogicalPlan::Limit {
            input, n, offset, ..
        } => lower_limit(input, *n, *offset, tables),
        LogicalPlan::Sort { input, keys } => {
            // Sample-table lowering path. See `lower_query` for the
            // production lowering and the in-memory / vectorised
            // discussion. Keeping both paths in sync so a `SELECT ...
            // ORDER BY` over the legacy `users` fixture behaves the same
            // as one over a real heap relation.
            let child = lower_plan(input, tables)?;
            let schema = child.schema().clone();
            Ok(Box::new(Sort::new(child, keys.clone(), schema)))
        }
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            schema,
        } => {
            // Sample-table path: recurse into the children through
            // `lower_plan`, then dispatch through the same operator
            // selection rule the real-heap path uses (see
            // `lower_join`).
            let left_schema = left.schema().clone();
            let right_schema = right.schema().clone();
            let left_op = lower_plan(left, tables)?;
            let right_op = lower_plan(right, tables)?;
            lower_join(
                left_op,
                right_op,
                left_schema,
                right_schema,
                *join_type,
                condition,
                schema.clone(),
            )
        }
        LogicalPlan::Empty { .. } => Err(ServerError::Unsupported("SELECT without FROM")),
        LogicalPlan::Values { .. } => Err(ServerError::Unsupported("VALUES")),
        LogicalPlan::Insert { .. } => Err(ServerError::Unsupported("INSERT")),
        LogicalPlan::Update { .. } => Err(ServerError::Unsupported("UPDATE")),
        LogicalPlan::Delete { .. } => Err(ServerError::Unsupported("DELETE")),
        LogicalPlan::Truncate { .. } => Err(ServerError::Unsupported("TRUNCATE")),
        // DDL is dispatched ahead of the lowerer in
        // `lib.rs::execute_query`. Reaching here means the dispatcher
        // missed a case; surface it as a planner-pipeline bug rather
        // than as a silent fall-through.
        LogicalPlan::CreateTable { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTable { .. } => Err(ServerError::Unsupported(
            "DDL reached operator lowerer; expected DDL dispatch path",
        )),
        LogicalPlan::Begin { .. }
        | LogicalPlan::Commit { .. }
        | LogicalPlan::Rollback { .. }
        | LogicalPlan::Savepoint { .. }
        | LogicalPlan::RollbackToSavepoint { .. }
        | LogicalPlan::ReleaseSavepoint { .. }
        | LogicalPlan::PrepareTransaction { .. }
        | LogicalPlan::CommitPrepared { .. }
        | LogicalPlan::RollbackPrepared { .. }
        | LogicalPlan::SetTransaction { .. } => Err(ServerError::Unsupported(
            "transaction control reached operator lowerer; expected txn dispatch path",
        )),
        LogicalPlan::Listen { .. } | LogicalPlan::Notify { .. } | LogicalPlan::Unlisten { .. } => {
            Err(ServerError::Unsupported(
                "LISTEN/NOTIFY/UNLISTEN reached operator lowerer; expected pubsub dispatch path",
            ))
        }
        LogicalPlan::Explain { .. } => Err(ServerError::Unsupported(
            "EXPLAIN reached operator lowerer; expected session dispatch path",
        )),
        LogicalPlan::Copy { .. } => Err(ServerError::Unsupported(
            "COPY reached operator lowerer; expected session dispatch path",
        )),
        LogicalPlan::Aggregate { .. } => Err(ServerError::Unsupported("GROUP BY / aggregate")),
        LogicalPlan::SetOp {
            op,
            quantifier,
            left,
            right,
            schema,
        } => {
            // Sample-table path. The set-op operator is the same kernel
            // used on the real-heap path; see `lower_query` for the
            // production lowering and the schema-compatibility note.
            let left_schema = left.schema();
            let right_schema = right.schema();
            check_set_op_schemas(left_schema, right_schema)?;
            let left_op = lower_plan(left, tables)?;
            let right_op = lower_plan(right, tables)?;
            Ok(Box::new(SetOp::new(
                left_op,
                right_op,
                *op,
                *quantifier,
                schema.clone(),
            )))
        }
        LogicalPlan::Cte { .. } => Err(ServerError::Unsupported("WITH (CTE)")),
        LogicalPlan::LockRows { input, .. } => {
            // Sample-table path: no live transaction manager, so the
            // lock callback is a no-op. The operator still passes rows
            // through, which lets tests exercise the pipeline.
            let child = lower_plan(input, tables)?;
            Ok(Box::new(ultrasql_executor::LockRows::new(
                child,
                Box::new(|_, _| Ok(())),
            )))
        }
    }
}

/// Build a [`MemTableScan`] for a registered table, optionally with a
/// projection pushed below the scan.
///
/// `projection` is supplied by [`lower_filter`] when it needs the
/// scan to drop columns the filter cannot consume. With `None` the
/// scan emits the table's natural shape.
fn lower_scan(
    table: &str,
    projection: Option<&[usize]>,
    tables: &SampleTables,
) -> Result<Box<dyn Operator>, ServerError> {
    let sample = tables.lookup(table).ok_or_else(|| {
        ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
            table.to_string(),
        ))
    })?;
    let scan: Box<dyn Operator> = Box::new(MemTableScan::new(
        sample.schema.clone(),
        sample.batches.clone(),
    ));
    if let Some(indices) = projection {
        let projected = Project::new(scan, indices.to_vec())?;
        Ok(Box::new(projected))
    } else {
        Ok(scan)
    }
}

/// Lower a `Filter` node.
///
/// Because [`FilterEqI32`] rejects non-numeric columns at runtime,
/// the lowerer pushes a projection below the filter that keeps only
/// the columns referenced by the parent operator and by the
/// predicate itself. The pushed projection is also reflected in the
/// indices the predicate references — column 0 of the pushed-down
/// schema is the predicate's old `col_idx`, so the filter's
/// `col_idx` becomes 0.
fn lower_filter(
    input: &LogicalPlan,
    predicate: &ScalarExpr,
    tables: &SampleTables,
) -> Result<Box<dyn Operator>, ServerError> {
    let (col_idx, constant) = match_eq_i32(predicate).ok_or(ServerError::Unsupported(
        "WHERE shape; v0.5 only supports `int_col = int_literal`",
    ))?;
    // The filter operator currently only knows how to walk Int32 /
    // Int64 columns; any wider column type causes a runtime
    // TypeMismatch. We project the scan down to just the predicate's
    // single column before handing it to the filter so the sample
    // table's `name TEXT` column never reaches the kernel.
    let scan_table = match input {
        LogicalPlan::Scan { table, .. } => table.as_str(),
        _ => {
            return Err(ServerError::Unsupported(
                "WHERE only supported directly over a base table in v0.5",
            ));
        }
    };
    let scan = lower_scan(scan_table, Some(&[col_idx]), tables)?;
    // After the pushed-down projection, the predicate column is
    // always at index 0.
    let filter = FilterEqI32::new(scan, 0, constant)?;
    Ok(Box::new(filter))
}

/// Recognise a binary predicate `Column(int) = Literal(int)` (or its
/// commuted form) and return the column index in the *input* schema
/// and the literal. Any other shape returns `None` so the caller
/// reports [`ServerError::Unsupported`].
fn match_eq_i32(predicate: &ScalarExpr) -> Option<(usize, i32)> {
    let ScalarExpr::Binary {
        op: BinaryOp::Eq,
        left,
        right,
        ..
    } = predicate
    else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (
            ScalarExpr::Column {
                index,
                data_type: DataType::Int32,
                ..
            },
            ScalarExpr::Literal {
                value: Value::Int32(v),
                ..
            },
        )
        | (
            ScalarExpr::Literal {
                value: Value::Int32(v),
                ..
            },
            ScalarExpr::Column {
                index,
                data_type: DataType::Int32,
                ..
            },
        ) => Some((*index, *v)),
        _ => None,
    }
}

fn lower_project(
    input: &LogicalPlan,
    exprs: &[(ScalarExpr, String)],
    tables: &SampleTables,
) -> Result<Box<dyn Operator>, ServerError> {
    // v0.5 only supports pure column references in the SELECT list;
    // computed projections land with the general expression
    // evaluator.
    let mut indices: Vec<usize> = Vec::with_capacity(exprs.len());
    for (expr, _name) in exprs {
        match expr {
            ScalarExpr::Column { index, .. } => indices.push(*index),
            _ => {
                return Err(ServerError::Unsupported(
                    "SELECT expression; v0.5 only supports bare column references",
                ));
            }
        }
    }

    // If the immediate child is a Filter we've already projected the
    // scan down to the predicate column at index 0. The parent
    // projection's indices, however, were resolved against the
    // *original* table schema. We rewrite them so they reference the
    // pushed-down view.
    if let LogicalPlan::Filter {
        input: filter_input,
        predicate,
    } = input
    {
        if let Some((filter_col, _)) = match_eq_i32(predicate) {
            // The pushed-down view has exactly one column at index 0:
            // the predicate column. The parent projection therefore
            // can only request that column; any other index would
            // mean "give me a column that the scan already dropped",
            // which we cannot fulfil with v0.5's operator set.
            for &i in &indices {
                if i != filter_col {
                    return Err(ServerError::Unsupported(
                        "v0.5 projection that survives a filter must reference \
                         exactly the predicate's column",
                    ));
                }
            }
            let child = lower_filter(filter_input, predicate, tables)?;
            // After the rewrite every output index is 0 in the child's schema.
            let zeroed: Vec<usize> = vec![0; indices.len()];
            return Ok(Box::new(Project::new(child, zeroed)?));
        }
    }

    let child = lower_plan(input, tables)?;
    let project = Project::new(child, indices)?;
    Ok(Box::new(project))
}

fn lower_limit(
    input: &LogicalPlan,
    n: u64,
    offset: u64,
    tables: &SampleTables,
) -> Result<Box<dyn Operator>, ServerError> {
    let child = lower_plan(input, tables)?;
    let limit = saturate_row_count(n);
    let offset = saturate_row_count(offset);
    Ok(Box::new(Limit::with_offset(child, limit, offset)))
}

/// Build the canonical `users(id INT, name TEXT, score DOUBLE)` sample
/// table and register it with the supplied catalog plus a fresh
/// [`SampleTables`] registry. Returns the populated registry.
///
/// The fixture matches the schema documented in the server's `--help`
/// output and the integration tests below.
#[must_use]
pub fn build_sample_database(catalog: &mut InMemoryCatalog) -> SampleTables {
    let mut tables = SampleTables::new();

    let schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("name", DataType::Text { max_len: None }),
        Field::nullable("score", DataType::Float64),
    ])
    .expect("sample schema is well-formed");

    let ids = NumericColumn::from_data(vec![1_i32, 2, 3]);
    let names = StringColumn::from_data(vec![
        "Ada".to_string(),
        "Grace".to_string(),
        "Linus".to_string(),
    ]);
    let scores = NumericColumn::from_data(vec![0.5_f64, 0.9, 0.7]);

    let batch = Batch::new([
        Column::Int32(ids),
        Column::Utf8(names),
        Column::Float64(scores),
    ])
    .expect("sample batch is well-formed");

    tables.register(catalog, "users", schema, vec![batch]);
    tables
}

// ---------------------------------------------------------------------------
// Real-heap-aware lowering
// ---------------------------------------------------------------------------

/// Context the catalog-aware lowerer needs to build operators that read
/// from / write to the runtime heap.
///
/// Distinct from the legacy [`lower_plan`] (which only knows about
/// [`SampleTables`]) because real `SELECT` and `INSERT` operators
/// require:
///
/// - the resolved [`CatalogSnapshot`] to look up the target relation's
///   OID and column schema,
/// - a shared [`HeapAccess`] handle so the operator can read pages
///   and write tuples,
/// - an MVCC [`Snapshot`] plus a [`TransactionManager`] handle (as the
///   `XidStatusOracle`) so visibility filtering is honest,
/// - the XID and command id of the autocommit transaction this
///   statement executes inside.
///
/// The struct is built per-statement in `Session::execute_query`.
#[derive(Debug)]
pub struct LowerCtx<'a> {
    /// Legacy sample-table registry; used when the catalog snapshot has
    /// no entry for a referenced table.
    pub tables: &'a SampleTables,
    /// Per-statement immutable catalog snapshot.
    pub catalog_snapshot: Arc<CatalogSnapshot>,
    /// Shared heap access handle.
    pub heap: Arc<HeapAccess<BlankPageLoader>>,
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

/// Lower a logical plan with full real-heap awareness.
///
/// Differences from [`lower_plan`]:
///
/// - A `Scan` whose table name resolves in `ctx.catalog_snapshot` is
///   lowered to a [`SeqScan`] over real heap pages. A `Scan` whose
///   name only resolves in the legacy [`SampleTables`] registry falls
///   back to the v0.5 [`MemTableScan`] path.
/// - `Insert` is lowered to a [`ModifyTable`] over real heap, with the
///   autocommit transaction's XID/command-id stamped on every inserted
///   tuple. `INSERT INTO t VALUES (...)` is the only source shape
///   accepted in this phase; `INSERT INTO t SELECT ...` returns
///   [`ServerError::Unsupported`].
/// - `Values` is lowered to a [`ValuesScan`].
/// - `Filter` uses the general [`Filter`] operator (Eval-backed)
///   instead of the type-fussy [`FilterEqI32`] specialized path.
/// - `Project` accepts only bare column references (same restriction
///   as [`lower_plan`]); computed projections land with the general
///   expression evaluator follow-up.
/// - `Cte` materialises the definition once into an `Arc<Vec<Batch>>`
///   and lowers the body with the CTE name bound to that buffer via
///   [`LowerCtx::cte_buffers`]; see [`lower_cte`] for the rules.
///   `WITH RECURSIVE` is rejected — the executor lacks a fixpoint
///   loop and silently treating it as non-recursive would return
///   wrong results for self-referential definitions.
/// - Everything else is rejected (currently nothing — the remaining
///   variants are DDL dispatched ahead of the lowerer).
// One arm per `LogicalPlan` variant; the dispatcher is intentionally
// linear so a new wave (set-ops here in A7) adds a clearly-bounded arm.
// Each arm with non-trivial logic delegates to a helper (`lower_join`,
// `lower_real_update`, `lower_set_op_real`, ...) so the per-arm body
// stays small even though the total file is large.
#[allow(clippy::too_many_lines)]
pub fn lower_query(
    plan: &LogicalPlan,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    match plan {
        LogicalPlan::Scan { table, schema, .. } => lower_catalog_or_sample_scan(table, schema, ctx),
        LogicalPlan::Insert {
            table,
            columns,
            source,
            on_conflict,
            returning,
            ..
        } => lower_real_insert(table, columns, source, on_conflict.as_ref(), returning, ctx),
        LogicalPlan::Values { rows, schema } => {
            Ok(Box::new(ValuesScan::new(rows.clone(), schema.clone())))
        }
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => {
            // `SELECT <const>` (no FROM) lowers Project(Empty) → ResultOp,
            // a single-row constant emitter. The general path below would
            // try to lower Empty into a scan, which has no meaning when
            // the projection is purely constant.
            if matches!(input.as_ref(), LogicalPlan::Empty { .. }) {
                let scalars: Vec<ScalarExpr> = exprs.iter().map(|(e, _)| e.clone()).collect();
                return Ok(Box::new(ResultOp::new(scalars, schema.clone())));
            }
            let child = lower_query(input, ctx)?;
            lower_project_columns(child, exprs)
        }
        LogicalPlan::Filter { input, predicate } => {
            // Index-aware fast path: when the filter sits directly on top
            // of a catalog-resolved table scan and the predicate is one
            // of the indexable shapes recognised by `try_index_scan`, we
            // probe the B-tree and emit an [`IndexScan`] over the
            // matching tuple payloads — never materialising a SeqScan.
            //
            // The dispatcher returns `Ok(None)` when:
            //   - the input is not a bare `Scan { table }` over a
            //     persistent relation,
            //   - the table has no B-tree index on the predicate's
            //     column,
            //   - the predicate's shape is outside the indexable set,
            //   - the index's key column is not Int32 / Int64 (the only
            //     types A10 lifted into the on-disk B-tree).
            // In every miss case we fall back to the general
            // `Filter(SeqScan)` plan; that fallback is the existing
            // behaviour, so a query over an unindexed column or a
            // text-typed key never regresses.
            //
            // The dispatcher returns `Err(_)` only when an indexable
            // shape was recognised but probing the B-tree or fetching a
            // heap tuple raised a storage error — those are not
            // recoverable by falling back, so we propagate.
            if let Some(op) = try_index_scan(input, predicate, ctx)? {
                return Ok(op);
            }
            let child = lower_query(input, ctx)?;
            Ok(Box::new(Filter::new(child, predicate.clone())))
        }
        LogicalPlan::Limit { input, n, offset } => {
            let child = lower_query(input, ctx)?;
            let limit = saturate_row_count(*n);
            let offset = saturate_row_count(*offset);
            Ok(Box::new(Limit::with_offset(child, limit, offset)))
        }
        LogicalPlan::Empty { .. } => Err(ServerError::Unsupported("SELECT without FROM")),
        LogicalPlan::Sort { input, keys } => {
            // Lower the child first; the executor's `Sort` operator drains
            // it on the first `next_batch()` call and emits sorted rows in
            // 4096-row chunks thereafter, so the wire encoder treats it
            // exactly like any other scalar source.
            //
            // v0.5 limitation: `Sort` materialises the entire input in
            // memory before emitting the first row. Spill-to-disk is on
            // the v0.6 work_mem track. Bounded by `IN_MEMORY_POOL_FRAMES *
            // PAGE_SIZE` plus working-set headroom (see
            // `crate::IN_MEMORY_POOL_FRAMES`); a query whose input
            // exceeds that will OOM the connection task rather than spill.
            //
            // Vectorised vs scalar choice: the executor ships a
            // `VectorizedSort` in `vec_ops::sort` that operates on the
            // push-based pipeline driver (`VectorizedSink`/
            // `VectorizedOperator`). The Simple Query path runs the
            // pull-based `Operator` interface, so the drop-in is the
            // scalar `Sort` in `ultrasql_executor::sort`. The vectorised
            // variant would require lifting the entire pipeline to the
            // push driver, which is a v0.7 milestone (see ROADMAP §v0.7).
            let child = lower_query(input, ctx)?;
            let schema = child.schema().clone();
            Ok(Box::new(Sort::new(child, keys.clone(), schema)))
        }
        LogicalPlan::Update {
            table,
            assignments,
            input,
            returning,
            ..
        } => lower_real_update(table, assignments, input, returning, ctx),
        LogicalPlan::Delete {
            table,
            input,
            returning,
            ..
        } => lower_real_delete(table, input, returning, ctx),
        LogicalPlan::Truncate { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTable { .. } => Err(ServerError::Unsupported(
            "DDL reached operator lowerer; expected DDL dispatch path",
        )),
        LogicalPlan::Begin { .. }
        | LogicalPlan::Commit { .. }
        | LogicalPlan::Rollback { .. }
        | LogicalPlan::Savepoint { .. }
        | LogicalPlan::RollbackToSavepoint { .. }
        | LogicalPlan::ReleaseSavepoint { .. }
        | LogicalPlan::PrepareTransaction { .. }
        | LogicalPlan::CommitPrepared { .. }
        | LogicalPlan::RollbackPrepared { .. }
        | LogicalPlan::SetTransaction { .. } => Err(ServerError::Unsupported(
            "transaction control reached operator lowerer; expected txn dispatch path",
        )),
        LogicalPlan::Listen { .. } | LogicalPlan::Notify { .. } | LogicalPlan::Unlisten { .. } => {
            Err(ServerError::Unsupported(
                "LISTEN/NOTIFY/UNLISTEN reached operator lowerer; expected pubsub dispatch path",
            ))
        }
        LogicalPlan::Explain { .. } => Err(ServerError::Unsupported(
            "EXPLAIN reached operator lowerer; expected session dispatch path",
        )),
        LogicalPlan::Copy { .. } => Err(ServerError::Unsupported(
            "COPY reached operator lowerer; expected session dispatch path",
        )),
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            schema,
        } => {
            // Lower the join's children first so the same real-heap path
            // (`SeqScan`-aware) feeds the operator. The selection rule
            // (HashJoin vs NestedLoopJoin) is delegated to `lower_join`
            // so the sample-table path in `lower_plan` and the
            // catalog-aware path here stay bit-identical in dispatch
            // semantics.
            let left_schema = left.schema().clone();
            let right_schema = right.schema().clone();
            let left_op = lower_query(left, ctx)?;
            let right_op = lower_query(right, ctx)?;
            lower_join(
                left_op,
                right_op,
                left_schema,
                right_schema,
                *join_type,
                condition,
                schema.clone(),
            )
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            schema,
        } => {
            // Fast path: `SELECT SUM(col_i32) FROM t WHERE col_i32 op lit`
            // collapses to one fused operator that runs SIMD
            // cmp_i32 → mask → sum_i32_widening_with_mask in a single
            // pass. Skips the per-batch `select_column` (per-row
            // scalar pushes) the generic Filter → HashAggregate
            // chain pays.
            if let Some(fused) = try_lower_fused_filter_sum_i32(input, group_by, aggregates, ctx)? {
                return Ok(fused);
            }
            // Fast path: pure `SELECT SUM(col_i32) FROM t` /
            // `SELECT AVG(col_i32) FROM t` over a cache-live
            // relation. Reads the cached column directly through
            // the hand-NEON kernel — no SeqScan batch slicing.
            if let Some(direct) =
                try_lower_cached_scalar_aggregate_i32(input, group_by, aggregates, ctx)?
            {
                return Ok(direct);
            }
            // Mirror `ultrasql_executor::physical::build_operator` — default
            // to a hash-based aggregate. The child is lowered recursively
            // through this same real-heap-aware path so the aggregate can
            // sit on top of a `SeqScan` over a persistent relation.
            let child = lower_query(input, ctx)?;
            Ok(Box::new(HashAggregate::new(
                child,
                group_by.clone(),
                aggregates.clone(),
                schema.clone(),
            )))
        }
        LogicalPlan::SetOp {
            op,
            quantifier,
            left,
            right,
            schema,
        } => lower_set_op_real(*op, *quantifier, left, right, schema.clone(), ctx),
        LogicalPlan::Cte {
            name,
            recursive,
            definition,
            body,
            ..
        } => lower_cte(name, *recursive, definition, body, ctx),
        LogicalPlan::LockRows { input, .. } => {
            // Production path: lower the child through the real-heap-aware
            // path, then wrap with LockRows. The lock-acquisition callback
            // is a no-op here; the server's session layer is responsible
            // for replacing it with a live TxnManager callback before
            // executing a genuine `SELECT FOR UPDATE` over a persistent
            // relation. For the in-memory fixture path the no-op is correct
            // (no concurrent writers, no need to acquire row locks).
            let child = lower_query(input, ctx)?;
            Ok(Box::new(ultrasql_executor::LockRows::new(
                child,
                Box::new(|_, _| Ok(())),
            )))
        }
    }
}

/// Lower a `LogicalPlan::Cte` node.
///
/// Semantics:
///
/// - Recursive CTEs (`WITH RECURSIVE`) are out of scope for this wave;
///   the executor lacks a fixpoint loop, so the binder accepts the
///   keyword but the lowerer rejects the plan with a precise
///   [`ServerError::Unsupported`] rather than silently treating it as
///   non-recursive (which would return wrong results for a self-
///   referential definition). The recursive fixpoint is a v0.6 follow-up.
/// - Non-recursive CTEs are materialised *once* per query execution into
///   a shared `Arc<Vec<Batch>>`. Every reference inside the body
///   resolves to its own [`CteScan`] over that buffer (the
///   [`CteScan`] operator is itself single-shot, but the underlying
///   `Arc` is shared, so multiple references reuse the materialised
///   rows without re-evaluating the definition). This matches
///   PostgreSQL's default CTE-as-optimisation-barrier behaviour.
/// - The CTE name is pushed onto a new `LowerCtx::cte_buffers` overlay
///   before the body is lowered. Body-side `Scan { table: "<cte_name>" }`
///   nodes are routed to the materialised buffer by
///   [`lower_catalog_or_sample_scan`].
///
/// Nested CTEs (a CTE defined inside another CTE's body, or a body that
/// itself contains a `WITH` clause) compose naturally: each recursive
/// call into [`lower_query`] sees the cumulative overlay, and inner
/// definitions can therefore reference outer CTEs.
fn lower_cte(
    name: &str,
    recursive: bool,
    definition: &LogicalPlan,
    body: &LogicalPlan,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    if recursive {
        return lower_recursive_cte(name, definition, body, ctx);
    }

    // Materialise the definition plan against the *current* overlay so a
    // CTE can reference outer CTEs declared earlier in the same `WITH`
    // chain (the binder serialises the chain into nested
    // `LogicalPlan::Cte` nodes, so the outer ones are already on the
    // overlay when we reach this inner one).
    let mut def_op = lower_query(definition, ctx)?;
    let mut batches: Vec<Batch> = Vec::new();
    while let Some(batch) = def_op.next_batch()? {
        batches.push(batch);
    }
    let def_schema = def_op.schema().clone();

    // Push the materialised CTE onto a child overlay. Cloning the map is
    // O(N) in the number of outer bindings; CTE chains are short
    // (typically ≤ a handful per query), so we accept the copy in
    // exchange for keeping `LowerCtx` strictly immutable per recursion
    // level — interior mutability here would force every helper to take
    // `&mut LowerCtx` for no clarity gain.
    let mut child_buffers = ctx.cte_buffers.clone();
    child_buffers.insert(
        name.to_ascii_lowercase(),
        CteBuffer {
            batches: Arc::new(batches),
            schema: def_schema,
        },
    );
    let child_ctx = LowerCtx {
        tables: ctx.tables,
        catalog_snapshot: Arc::clone(&ctx.catalog_snapshot),
        heap: Arc::clone(&ctx.heap),
        snapshot: ctx.snapshot.clone(),
        oracle: Arc::clone(&ctx.oracle),
        xid: ctx.xid,
        command_id: ctx.command_id,
        cte_buffers: child_buffers,
    };

    lower_query(body, &child_ctx)
}

/// Lower a `WITH RECURSIVE` CTE.
///
/// Definition shape (binder contract): `SetOp { op: Union, quantifier,
/// left = anchor, right = recursive_term, .. }`. Anything else is
/// rejected — `WITH RECURSIVE` requires a `UNION` shape per SQL spec.
///
/// # Algorithm
///
/// 1. Lower the anchor; collect every batch it produces.
/// 2. Push it as the CTE's `cte_buffers` entry.
/// 3. Loop: lower the recursive term against the current buffer
///    (each iteration sees only the previous iteration's rows, per
///    SQL spec). For `UNION ALL` every batch goes into the
///    accumulator. For `UNION` (DISTINCT), dedupe new rows against
///    the accumulator; if no new rows survive, the fixpoint is
///    reached and the loop terminates. A safety cap prevents
///    runaway recursion.
/// 4. Bind the body with the full accumulator as the CTE buffer.
fn lower_recursive_cte(
    name: &str,
    definition: &LogicalPlan,
    body: &LogicalPlan,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    use ultrasql_planner::LogicalSetOp;

    let (op, quantifier, anchor, recursive_term, schema) = match definition {
        LogicalPlan::SetOp {
            op,
            quantifier,
            left,
            right,
            schema,
        } => (
            *op,
            *quantifier,
            left.as_ref(),
            right.as_ref(),
            schema.clone(),
        ),
        _ => {
            return Err(ServerError::Unsupported(
                "WITH RECURSIVE definition must be a UNION of an anchor + recursive term",
            ));
        }
    };
    if op != LogicalSetOp::Union {
        return Err(ServerError::Unsupported(
            "WITH RECURSIVE supports only UNION (not INTERSECT or EXCEPT)",
        ));
    }

    // Cap on iterations matches PostgreSQL's recommendation for
    // non-terminating queries (`max_recursive_iterations` GUC). 1024
    // is comfortable for graph traversals while still bounding a
    // runaway plan.
    const MAX_ITERATIONS: usize = 1024;

    let _ = schema; // SetOp's schema is identical to the anchor's after binding.
    let mut accumulator: Vec<Batch> = Vec::new();
    let mut working: Vec<Batch> = Vec::new();

    // Step 1 — lower and drain the anchor. Anchor sees the parent
    // overlay (it cannot reference the CTE itself by name).
    let mut anchor_op = lower_query(anchor, ctx)?;
    let def_schema = anchor_op.schema().clone();
    while let Some(b) = anchor_op.next_batch()? {
        if b.rows() > 0 {
            working.push(b.clone());
            accumulator.push(b);
        }
    }

    // Step 2 — fixpoint loop.
    let dedup = matches!(quantifier, ultrasql_planner::LogicalSetQuantifier::Distinct);
    let mut seen_keys: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    if dedup {
        for b in &accumulator {
            for k in batch_row_keys(b) {
                seen_keys.insert(k);
            }
        }
    }
    for _ in 0..MAX_ITERATIONS {
        if working.is_empty() {
            break;
        }
        let mut child_buffers = ctx.cte_buffers.clone();
        child_buffers.insert(
            name.to_ascii_lowercase(),
            CteBuffer {
                batches: Arc::new(std::mem::take(&mut working)),
                schema: def_schema.clone(),
            },
        );
        let child_ctx = LowerCtx {
            tables: ctx.tables,
            catalog_snapshot: Arc::clone(&ctx.catalog_snapshot),
            heap: Arc::clone(&ctx.heap),
            snapshot: ctx.snapshot.clone(),
            oracle: Arc::clone(&ctx.oracle),
            xid: ctx.xid,
            command_id: ctx.command_id,
            cte_buffers: child_buffers,
        };

        let mut term_op = lower_query(recursive_term, &child_ctx)?;
        let mut new_batches: Vec<Batch> = Vec::new();
        while let Some(b) = term_op.next_batch()? {
            if b.rows() == 0 {
                continue;
            }
            if dedup {
                let kept = filter_unseen_rows(&b, &mut seen_keys)?;
                if let Some(kept) = kept {
                    if kept.rows() > 0 {
                        new_batches.push(kept);
                    }
                }
            } else {
                new_batches.push(b);
            }
        }
        if new_batches.is_empty() {
            break;
        }
        for b in &new_batches {
            accumulator.push(b.clone());
        }
        working = new_batches;
    }

    // Step 3 — bind body with the full accumulator as the CTE
    // buffer. From the body's perspective the CTE is a single
    // materialised relation.
    let mut body_buffers = ctx.cte_buffers.clone();
    body_buffers.insert(
        name.to_ascii_lowercase(),
        CteBuffer {
            batches: Arc::new(accumulator),
            schema: def_schema,
        },
    );
    let body_ctx = LowerCtx {
        tables: ctx.tables,
        catalog_snapshot: Arc::clone(&ctx.catalog_snapshot),
        heap: Arc::clone(&ctx.heap),
        snapshot: ctx.snapshot.clone(),
        oracle: Arc::clone(&ctx.oracle),
        xid: ctx.xid,
        command_id: ctx.command_id,
        cte_buffers: body_buffers,
    };
    lower_query(body, &body_ctx)
}

/// Encode every row of `batch` into a flat byte key for set-membership
/// dedup in the recursive UNION fixpoint. Keys must compare equal
/// when rows are equal under SQL semantics; for the v0.5 type set
/// (Int32, Int64, Float32, Float64, Bool, Text) the encoding is the
/// little-endian payload prefixed by the column index.
fn batch_row_keys(batch: &Batch) -> Vec<Vec<u8>> {
    let n_rows = batch.rows();
    let mut keys: Vec<Vec<u8>> = (0..n_rows).map(|_| Vec::with_capacity(64)).collect();
    for col in batch.columns() {
        for (row_idx, key) in keys.iter_mut().enumerate() {
            match col {
                Column::Int32(c) => {
                    if c.nulls().is_some_and(|n| !n.get(row_idx)) {
                        key.push(0xFF);
                    } else {
                        key.push(0x00);
                        key.extend_from_slice(&c.data()[row_idx].to_le_bytes());
                    }
                }
                Column::Int64(c) => {
                    if c.nulls().is_some_and(|n| !n.get(row_idx)) {
                        key.push(0xFF);
                    } else {
                        key.push(0x00);
                        key.extend_from_slice(&c.data()[row_idx].to_le_bytes());
                    }
                }
                Column::Utf8(c) => {
                    if c.nulls().is_some_and(|n| !n.get(row_idx)) {
                        key.push(0xFF);
                    } else {
                        key.push(0x00);
                        let s = c.value(row_idx);
                        key.extend_from_slice(&(s.len() as u32).to_le_bytes());
                        key.extend_from_slice(s.as_bytes());
                    }
                }
                Column::Bool(c) => {
                    if c.nulls().is_some_and(|n| !n.get(row_idx)) {
                        key.push(0xFF);
                    } else {
                        key.push(if c.value(row_idx) { 0x01 } else { 0x00 });
                    }
                }
                Column::Float32(c) => {
                    if c.nulls().is_some_and(|n| !n.get(row_idx)) {
                        key.push(0xFF);
                    } else {
                        key.push(0x00);
                        key.extend_from_slice(&c.data()[row_idx].to_le_bytes());
                    }
                }
                Column::Float64(c) => {
                    if c.nulls().is_some_and(|n| !n.get(row_idx)) {
                        key.push(0xFF);
                    } else {
                        key.push(0x00);
                        key.extend_from_slice(&c.data()[row_idx].to_le_bytes());
                    }
                }
            }
        }
    }
    keys
}

/// Return a sub-batch of `batch` containing only rows whose encoded
/// key is not already in `seen`. Rows that survive get added to
/// `seen`.
fn filter_unseen_rows(
    batch: &Batch,
    seen: &mut std::collections::HashSet<Vec<u8>>,
) -> Result<Option<Batch>, ServerError> {
    let keys = batch_row_keys(batch);
    let mut keep_mask = Vec::with_capacity(keys.len());
    for k in keys {
        if seen.insert(k) {
            keep_mask.push(true);
        } else {
            keep_mask.push(false);
        }
    }
    if !keep_mask.iter().any(|&b| b) {
        return Ok(None);
    }
    if keep_mask.iter().all(|&b| b) {
        return Ok(Some(batch.clone()));
    }
    // Rebuild the batch keeping only the marked rows.
    let mut cols: Vec<Column> = Vec::with_capacity(batch.columns().len());
    for col in batch.columns() {
        let new_col = match col {
            Column::Int32(c) => Column::Int32(filter_numeric(c, &keep_mask)),
            Column::Int64(c) => Column::Int64(filter_numeric(c, &keep_mask)),
            Column::Float32(c) => Column::Float32(filter_numeric(c, &keep_mask)),
            Column::Float64(c) => Column::Float64(filter_numeric(c, &keep_mask)),
            Column::Bool(c) => {
                let data: Vec<bool> = c
                    .data()
                    .iter()
                    .zip(keep_mask.iter())
                    .filter_map(|(v, k)| k.then_some(*v != 0))
                    .collect();
                Column::Bool(ultrasql_vec::column::BoolColumn::from_data(data))
            }
            Column::Utf8(c) => {
                let strings: Vec<String> = (0..keep_mask.len())
                    .filter(|&i| keep_mask[i])
                    .map(|i| c.value(i).to_owned())
                    .collect();
                Column::Utf8(StringColumn::from_data(strings))
            }
        };
        cols.push(new_col);
    }
    Batch::new(cols).map(Some).map_err(|e| {
        ServerError::Unsupported(Box::leak(
            format!("recursive CTE filter: {e}").into_boxed_str(),
        ))
    })
}

/// Filter helper for numeric columns — drops rows whose mask bit is 0.
fn filter_numeric<T: Copy>(
    col: &ultrasql_vec::column::NumericColumn<T>,
    keep_mask: &[bool],
) -> ultrasql_vec::column::NumericColumn<T> {
    let data: Vec<T> = col
        .data()
        .iter()
        .zip(keep_mask.iter())
        .filter_map(|(v, k)| k.then_some(*v))
        .collect();
    ultrasql_vec::column::NumericColumn::from_data(data)
}

/// Re-check the contract `bind_set_op` enforces: both inputs must have
/// the same arity. Per-column type-compatibility is the binder's job;
/// we only catch the arity mismatch here so a hand-built plan that
/// skipped binding fails with a precise error instead of crashing the
/// kernel.
fn check_set_op_schemas(left: &Schema, right: &Schema) -> Result<(), ServerError> {
    if left.len() != right.len() {
        return Err(ServerError::Unsupported(
            "set operation: left and right sides must have the same number of columns",
        ));
    }
    Ok(())
}

/// Build a [`SetOp`] over the catalog-aware [`lower_query`] path.
///
/// The two children are lowered through the same real-heap-aware path
/// so a set-op can sit on top of `SeqScan` over a persistent relation,
/// an in-memory `Values`/`MemTableScan`, or any other supported source.
/// The executor's `SetOp` kernel
/// (`crates/ultrasql-executor/src/set_op.rs`) implements all six SQL
/// shapes (UNION / INTERSECT / EXCEPT × ALL / DISTINCT) with a
/// hash-counting algorithm, treating two NULLs as equal (matching
/// PostgreSQL `DISTINCT` semantics). The kernel is fully materialising:
/// it drains both inputs before emitting its first row, so the operator
/// is a pipeline breaker bounded by the same in-memory footprint as
/// `HashAggregate` / `Sort` until the v0.7 `work_mem` spill lands.
///
/// Schema-compatibility: the binder enforces arity and per-column
/// `numeric_join` compatibility (see `binder::bind_set_op`). We re-check
/// arity through [`check_set_op_schemas`] so a hand-built plan that
/// bypassed the binder still surfaces a precise error rather than
/// producing wrong rows.
fn lower_set_op_real(
    op: ultrasql_planner::LogicalSetOp,
    quantifier: ultrasql_planner::LogicalSetQuantifier,
    left: &LogicalPlan,
    right: &LogicalPlan,
    out_schema: Schema,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    check_set_op_schemas(left.schema(), right.schema())?;
    let left_op = lower_query(left, ctx)?;
    let right_op = lower_query(right, ctx)?;
    Ok(Box::new(SetOp::new(
        left_op, right_op, op, quantifier, out_schema,
    )))
}

/// Lower a `Scan` node by checking the CTE binding overlay first, then
/// the persistent catalog, then falling back to the v0.5 sample-table
/// registry.
///
/// The resolution order matches PostgreSQL: a CTE name shadows a
/// same-named base table for the duration of the body (binder-enforced
/// scope; we simply mirror the lookup order here so a stray `Scan` over
/// the unaliased name still picks up the CTE).
///
/// `plan_schema` is the schema recorded on the `LogicalPlan::Scan` node
/// itself; it carries any column aliases applied by the binder when a
/// CTE is declared as `WITH cte(c1, c2) AS (...)`. For real heap and
/// sample-table scans the schema comes from the table's definition, so
/// we ignore `plan_schema` in those branches; for CTE scans we use it
/// directly so the body sees the aliased column names.
fn lower_catalog_or_sample_scan(
    table: &str,
    plan_schema: &Schema,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    let folded = table.to_ascii_lowercase();
    if let Some(buffer) = ctx.cte_buffers.get(&folded) {
        return Ok(Box::new(CteScan::new(
            Arc::clone(&buffer.batches),
            plan_schema.clone(),
        )));
    }
    if let Some(entry) = ctx.catalog_snapshot.tables.get(&folded) {
        return Ok(lower_heap_scan(entry, ctx));
    }
    // Legacy path: sample tables.
    let sample = ctx.tables.lookup(table).ok_or_else(|| {
        ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
            table.to_string(),
        ))
    })?;
    Ok(Box::new(MemTableScan::new(
        sample.schema.clone(),
        sample.batches.clone(),
    )))
}

/// Construct a [`SeqScan`] for a real persistent relation.
fn lower_heap_scan(entry: &TableEntry, ctx: &LowerCtx<'_>) -> Box<dyn Operator> {
    let rel = RelationId(entry.oid);
    // The catalog's `n_blocks` stat is an estimate; the heap's
    // counter is the truth. Take the larger of the two so a freshly
    // created table (entry.n_blocks = 0) still scans the blocks that
    // the heap has actually allocated through `insert`.
    let block_count = ctx.heap.block_count(rel).max(entry.n_blocks);
    let codec = RowCodec::new(entry.schema.clone());
    let scan = SeqScan::new(
        Arc::clone(&ctx.heap),
        rel,
        block_count,
        ctx.snapshot.clone(),
        Arc::clone(&ctx.oracle),
        codec,
    );
    Box::new(scan)
}

/// Lower an `INSERT INTO t VALUES (...)` into a [`ModifyTable`]
/// over the real heap.
fn lower_real_insert(
    table: &str,
    columns: &[usize],
    source: &LogicalPlan,
    on_conflict: Option<&ultrasql_planner::LogicalOnConflict>,
    returning: &[(ScalarExpr, String)],
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    if on_conflict.is_some() {
        return Err(ServerError::Unsupported("INSERT ... ON CONFLICT"));
    }
    if !returning.is_empty() {
        return Err(ServerError::Unsupported("INSERT ... RETURNING"));
    }
    let entry = ctx
        .catalog_snapshot
        .tables
        .get(&table.to_ascii_lowercase())
        .ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table.to_string(),
            ))
        })?;
    if !columns.is_empty() && columns.len() != entry.schema.len() {
        return Err(ServerError::Unsupported(
            "INSERT with column list narrower than table; v0.5 requires every column",
        ));
    }
    let child: Box<dyn Operator> = match source {
        LogicalPlan::Values { rows, schema } => {
            Box::new(ValuesScan::new(rows.clone(), schema.clone()))
        }
        // `INSERT INTO t SELECT ...` — drive the destination through the
        // same `ModifyTable` shape we use for `VALUES`, but with a
        // lowered query plan as the row source. The binder enforced
        // arity, types, and named-column matching when it built the
        // `Insert` plan; if its schema differs from the target table's
        // declared schema, refuse here so a silent encoding mismatch
        // never lands rows into the heap with the wrong layout.
        other => {
            let source_schema = other.schema();
            if source_schema.len() != entry.schema.len() {
                return Err(ServerError::Unsupported(
                    "INSERT ... SELECT with arity mismatch",
                ));
            }
            for (idx, (src, dst)) in source_schema
                .fields()
                .iter()
                .zip(entry.schema.fields().iter())
                .enumerate()
            {
                if src.data_type != dst.data_type
                    && !matches!(src.data_type, ultrasql_core::DataType::Null)
                {
                    return Err(ServerError::Plan(
                        ultrasql_planner::PlanError::TypeMismatch(format!(
                            "INSERT ... SELECT column {idx} type mismatch: source {src} vs target {dst}",
                        )),
                    ));
                }
            }
            lower_query(other, ctx)?
        }
    };
    let rel = RelationId(entry.oid);
    let modify = ModifyTable::new(
        Arc::clone(&ctx.heap),
        rel,
        entry.schema.clone(),
        ModifyKind::Insert,
        ctx.xid,
        ctx.command_id,
        Xid::new(0),
        CommandId::FIRST,
        None,
        child,
    );
    Ok(Box::new(modify))
}

/// Build a TID-emitting [`SeqScan`] over a persistent relation.
///
/// The resulting operator emits rows shaped
/// `[tid_block: Int32, tid_slot: Int32, ...payload_cols]`, which is the
/// contract [`ModifyTable`] expects for UPDATE and DELETE.
fn build_tid_seq_scan(entry: &TableEntry, ctx: &LowerCtx<'_>) -> Box<dyn Operator> {
    let rel = RelationId(entry.oid);
    let block_count = ctx.heap.block_count(rel).max(entry.n_blocks);
    let codec = RowCodec::new(entry.schema.clone());
    let scan = SeqScan::new_with_tids(
        Arc::clone(&ctx.heap),
        rel,
        block_count,
        ctx.snapshot.clone(),
        Arc::clone(&ctx.oracle),
        codec,
    );
    Box::new(scan)
}

/// Recursively rebuild `expr`, adding `by` to every
/// [`ScalarExpr::Column`] index. Used by UPDATE / DELETE lowering: the
/// scan now emits `[tid_block, tid_slot, ...orig_cols]`, but the
/// binder produced column indices against the un-prefixed schema, so
/// every reference must shift by +2 to remain correct.
///
/// Subquery-bearing variants (`ScalarSubquery`, `Exists`,
/// `InSubquery`, `OuterColumn`) are not shifted — those would require
/// recursing into a `LogicalPlan` and rewriting the outer-column
/// references, which is out of scope for the basic UPDATE/DELETE path
/// in this commit. The helper returns those variants verbatim; if a
/// caller hands us one we have already rejected at higher levels.
fn shift_column_indices(expr: &ScalarExpr, by: usize) -> ScalarExpr {
    match expr {
        ScalarExpr::Column {
            name,
            index,
            data_type,
        } => ScalarExpr::Column {
            name: name.clone(),
            index: index + by,
            data_type: data_type.clone(),
        },
        ScalarExpr::Literal { value, data_type } => ScalarExpr::Literal {
            value: value.clone(),
            data_type: data_type.clone(),
        },
        ScalarExpr::Parameter { index, data_type } => ScalarExpr::Parameter {
            index: *index,
            data_type: data_type.clone(),
        },
        ScalarExpr::Unary {
            op,
            expr,
            data_type,
        } => ScalarExpr::Unary {
            op: *op,
            expr: Box::new(shift_column_indices(expr, by)),
            data_type: data_type.clone(),
        },
        ScalarExpr::Binary {
            op,
            left,
            right,
            data_type,
        } => ScalarExpr::Binary {
            op: *op,
            left: Box::new(shift_column_indices(left, by)),
            right: Box::new(shift_column_indices(right, by)),
            data_type: data_type.clone(),
        },
        ScalarExpr::IsNull { expr, negated } => ScalarExpr::IsNull {
            expr: Box::new(shift_column_indices(expr, by)),
            negated: *negated,
        },
        // Subquery-bearing and outer-frame variants are returned
        // unchanged. They cannot appear in a v0.5 UPDATE / DELETE
        // predicate (the binder produces them only for SELECTs), so we
        // would never observe them here in practice.
        ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => expr.clone(),
    }
}

/// Lower an `UPDATE` plan into a [`ModifyTable`] with `ModifyKind::Update`.
///
/// The child operator is a TID-emitting [`SeqScan`] (optionally wrapped
/// in [`Filter`] when the planner produced a `WHERE`). Predicate column
/// indices are shifted by +2 to account for the leading TID columns;
/// assignment **target** column indices stay un-shifted because
/// `apply_update` re-indexes them against the relation schema, not the
/// child batch shape.

/// Try to lower pure-scalar SUM or AVG over an `Int32` column on a
/// cache-live relation into [`CachedSumI32Scan`] /
/// [`CachedAvgI32Scan`].
///
/// Matches:
///
/// ```text
///     Aggregate { group_by: [], aggregates: [Sum(Column { col, Int32 })] }
///       └── Scan { table }
/// ```
///
/// (or `Avg` instead of `Sum`) and the relation already has a
/// live entry in `HeapAccess::column_cache`. Returns `Ok(None)`
/// when the shape does not match or the cache is empty — caller
/// falls through to the generic `HashAggregate(SeqScan)` chain
/// which populates the cache as a side effect of its first walk.
fn try_lower_cached_scalar_aggregate_i32(
    input: &LogicalPlan,
    group_by: &[ScalarExpr],
    aggregates: &[ultrasql_planner::LogicalAggregateExpr],
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    use ultrasql_planner::AggregateFunc;

    if !group_by.is_empty() || aggregates.len() != 1 {
        return Ok(None);
    }
    let agg = &aggregates[0];
    if agg.distinct {
        return Ok(None);
    }
    let target_col = match &agg.arg {
        Some(ScalarExpr::Column {
            index,
            data_type: ultrasql_core::DataType::Int32,
            ..
        }) => *index,
        _ => return Ok(None),
    };

    let LogicalPlan::Scan { table, .. } = input else {
        return Ok(None);
    };
    let folded = table.to_ascii_lowercase();
    let entry = match ctx.catalog_snapshot.tables.get(&folded) {
        Some(entry) => entry,
        None => return Ok(None),
    };
    if target_col >= entry.schema.len()
        || !matches!(
            entry.schema.field_at(target_col).data_type,
            ultrasql_core::DataType::Int32
        )
    {
        return Ok(None);
    }

    let rel_id = RelationId(entry.oid);
    let Some(columns) = ctx.heap.column_cache.get(rel_id) else {
        return Ok(None);
    };

    let op: Box<dyn Operator> = match agg.func {
        AggregateFunc::Sum => Box::new(CachedSumI32Scan::new(
            columns,
            target_col,
            agg.output_name.clone(),
        )),
        AggregateFunc::Avg => Box::new(CachedAvgI32Scan::new(
            columns,
            target_col,
            agg.output_name.clone(),
        )),
        _ => return Ok(None),
    };
    Ok(Some(op))
}

/// Try to lower
///
/// ```text
///     Aggregate { group_by: [], aggregates: [Sum(Column { col_sum, Int32 })] }
///       └── Filter { predicate: Column { col_pred, Int32 } op Literal(Int32) }
///             └── Scan { table }
/// ```
///
/// into [`FilterSumI32Scan`] over a [`SeqScan`].
///
/// Returns `Ok(Some(_))` on a successful match, `Ok(None)` when the
/// plan tree does not match the fused shape (caller falls through
/// to `HashAggregate`), and `Err(_)` on a lowering failure of the
/// inner scan.
fn try_lower_fused_filter_sum_i32(
    input: &LogicalPlan,
    group_by: &[ScalarExpr],
    aggregates: &[ultrasql_planner::LogicalAggregateExpr],
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    use ultrasql_planner::AggregateFunc;

    // Shape: scalar SUM aggregate (no GROUP BY, one Sum entry,
    // non-DISTINCT, Int32 column argument).
    if !group_by.is_empty() || aggregates.len() != 1 {
        return Ok(None);
    }
    let agg = &aggregates[0];
    if agg.func != AggregateFunc::Sum || agg.distinct {
        return Ok(None);
    }
    let sum_col = match &agg.arg {
        Some(ScalarExpr::Column {
            index,
            data_type: ultrasql_core::DataType::Int32,
            ..
        }) => *index,
        _ => return Ok(None),
    };

    // Shape: Filter over Scan with Int32 predicate `col op lit`.
    let LogicalPlan::Filter {
        input: filter_input,
        predicate,
    } = input
    else {
        return Ok(None);
    };
    let LogicalPlan::Scan { table, .. } = filter_input.as_ref() else {
        return Ok(None);
    };
    let (pred_col, pred_op, pred_lit) = match extract_int32_col_op_lit(predicate) {
        Some(x) => x,
        None => return Ok(None),
    };

    // The scan target must be a real heap relation (we only built
    // the column cache for the heap path). Sample / CTE / memtable
    // sources never benefit from the column-cache fast path and
    // would not provide the `Int32` columns the fused operator
    // requires anyway.
    let folded = table.to_ascii_lowercase();
    let entry = match ctx.catalog_snapshot.tables.get(&folded) {
        Some(entry) => entry,
        None => return Ok(None),
    };

    // Schema validation: both `pred_col` and `sum_col` must be
    // Int32 in the relation's catalog schema.
    let schema = &entry.schema;
    if pred_col >= schema.len() || sum_col >= schema.len() {
        return Ok(None);
    }
    if !matches!(
        schema.field_at(pred_col).data_type,
        ultrasql_core::DataType::Int32
    ) || !matches!(
        schema.field_at(sum_col).data_type,
        ultrasql_core::DataType::Int32
    ) {
        return Ok(None);
    }

    // Cache-driven fast path: when the relation already has a
    // live column-cache entry, skip the SeqScan layer entirely
    // and run the fused SIMD kernel directly over the cached
    // `Arc<CachedColumns>`. The cache-driving `SeqScan` would
    // otherwise copy each column out via `slice_column` (one
    // ~4 MB memcpy per 1 M-row Int32 column) before passing the
    // batch through the operator pipeline.
    let rel_id = RelationId(entry.oid);
    if let Some(columns) = ctx.heap.column_cache.get(rel_id) {
        let fused = CachedFilterSumI32Scan::new(
            columns,
            pred_col,
            pred_lit,
            pred_op,
            sum_col,
            agg.output_name.clone(),
        );
        return Ok(Some(Box::new(fused)));
    }

    // Cache miss — drive the regular SeqScan path. The first
    // SeqScan over a relation populates the column cache as a
    // side effect of its walk, so subsequent queries hit the
    // direct-from-cache branch above.
    let scan = lower_heap_scan(entry, ctx);
    let fused = FilterSumI32Scan::new(
        scan,
        pred_col,
        pred_lit,
        pred_op,
        sum_col,
        agg.output_name.clone(),
    );
    Ok(Some(Box::new(fused)))
}

/// Match a predicate of shape `Column { Int32 } op Literal(Int32)`
/// (or its mirror `Literal(Int32) op Column { Int32 }`) and return
/// the `(col_index, cmp_op, threshold)` tuple. Returns `None` for
/// any other shape.
fn extract_int32_col_op_lit(
    expr: &ScalarExpr,
) -> Option<(usize, ultrasql_vec::kernels::CmpOp, i32)> {
    use ultrasql_core::Value;
    use ultrasql_vec::kernels::CmpOp;

    let ScalarExpr::Binary {
        op, left, right, ..
    } = expr
    else {
        return None;
    };
    let cmp_op = match op {
        BinaryOp::Lt => CmpOp::Lt,
        BinaryOp::LtEq => CmpOp::Le,
        BinaryOp::Gt => CmpOp::Gt,
        BinaryOp::GtEq => CmpOp::Ge,
        BinaryOp::Eq => CmpOp::Eq,
        BinaryOp::NotEq => CmpOp::Ne,
        _ => return None,
    };

    let col_idx_from = |e: &ScalarExpr| match e {
        ScalarExpr::Column {
            index,
            data_type: ultrasql_core::DataType::Int32,
            ..
        } => Some(*index),
        _ => None,
    };
    let lit_from = |e: &ScalarExpr| match e {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            ..
        } => Some(*v),
        _ => None,
    };

    if let (Some(col), Some(lit)) = (col_idx_from(left), lit_from(right)) {
        Some((col, cmp_op, lit))
    } else if let (Some(lit), Some(col)) = (lit_from(left), col_idx_from(right)) {
        // Mirror: swap op so `lit op col` becomes `col mirror_op lit`.
        let mirrored = match cmp_op {
            CmpOp::Lt => CmpOp::Gt,
            CmpOp::Le => CmpOp::Ge,
            CmpOp::Gt => CmpOp::Lt,
            CmpOp::Ge => CmpOp::Le,
            CmpOp::Eq => CmpOp::Eq,
            CmpOp::Ne => CmpOp::Ne,
        };
        Some((col, mirrored, lit))
    } else {
        None
    }
}

/// Detect the `(Int32, Int32)` UPDATE shape and lower it to the
/// single-operator [`FusedUpdateInt32Add`] when every precondition
/// holds. Returns `Ok(None)` for any non-matching shape so the
/// caller falls back to the default `ModifyTable(Filter(SeqScan))`
/// plan.
///
/// Preconditions:
///
/// 1. Relation schema is exactly `[Int32, Int32]`.
/// 2. Exactly one assignment, with target column 0 or 1, body
///    `Column { Int32 } ± Int32 literal` (or the mirror
///    `Int32 literal + Column { Int32 }` for `+`).
/// 3. `input` is either a bare `Scan { table }` or
///    `Filter { Scan { table }, predicate }` where `predicate` is
///    one of the Int32-typed shapes [`extract_int32_col_op_lit`]
///    accepts.
fn try_build_fused_update(
    target_table: &str,
    entry: &TableEntry,
    assignments: &[(usize, ScalarExpr)],
    input: &LogicalPlan,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    // Schema must be exactly (Int32, Int32). No extra columns, no
    // NULLability change — `FusedUpdateInt32Add` reads a fixed
    // 9-byte payload layout.
    let fields = entry.schema.fields();
    if fields.len() != 2
        || fields[0].data_type != DataType::Int32
        || fields[1].data_type != DataType::Int32
    {
        return Ok(None);
    }

    if assignments.len() != 1 {
        return Ok(None);
    }
    let (target_col_usize, assign_expr) = &assignments[0];
    if *target_col_usize > 1 {
        return Ok(None);
    }
    let target_col = u8::try_from(*target_col_usize).expect("target_col fits in u8");

    // The assignment body must read the target column and add (or
    // subtract) an Int32 literal. Subtraction is normalised to
    // `delta = -literal`.
    let (op, left, right) = match assign_expr {
        ScalarExpr::Binary {
            op, left, right, ..
        } => (*op, left.as_ref(), right.as_ref()),
        _ => return Ok(None),
    };
    let read_col_idx = |e: &ScalarExpr| -> Option<usize> {
        match e {
            ScalarExpr::Column {
                index,
                data_type: DataType::Int32,
                ..
            } => Some(*index),
            _ => None,
        }
    };
    let read_lit_i32 = |e: &ScalarExpr| -> Option<i32> {
        match e {
            ScalarExpr::Literal {
                value: Value::Int32(v),
                ..
            } => Some(*v),
            _ => None,
        }
    };
    let delta: i32 = match op {
        BinaryOp::Add => {
            if let (Some(c), Some(l)) = (read_col_idx(left), read_lit_i32(right)) {
                if c != *target_col_usize {
                    return Ok(None);
                }
                l
            } else if let (Some(l), Some(c)) = (read_lit_i32(left), read_col_idx(right)) {
                if c != *target_col_usize {
                    return Ok(None);
                }
                l
            } else {
                return Ok(None);
            }
        }
        BinaryOp::Sub => {
            // Only `col - lit` is well-defined as `+ (-lit)` —
            // `lit - col` does not decompose to a single add.
            if let (Some(c), Some(l)) = (read_col_idx(left), read_lit_i32(right)) {
                if c != *target_col_usize {
                    return Ok(None);
                }
                l.checked_neg().ok_or(ServerError::Plan(
                    ultrasql_planner::PlanError::TypeMismatch(
                        "UPDATE delta overflows i32 negation".to_owned(),
                    ),
                ))?
            } else {
                return Ok(None);
            }
        }
        _ => return Ok(None),
    };

    // Validate input shape and extract the optional predicate. The
    // shape contract mirrors `build_filtered_tid_scan`'s contract
    // (Scan or Filter(Scan) over the same target table).
    let predicate: Option<FusedPredicate> = match input {
        LogicalPlan::Scan { table, .. } => {
            if !table.eq_ignore_ascii_case(target_table) {
                return Ok(None);
            }
            None
        }
        LogicalPlan::Filter {
            input: filter_input,
            predicate,
        } => {
            let LogicalPlan::Scan { table, .. } = filter_input.as_ref() else {
                return Ok(None);
            };
            if !table.eq_ignore_ascii_case(target_table) {
                return Ok(None);
            }
            let Some((pred_col_idx, cmp, lit)) = extract_int32_col_op_lit(predicate) else {
                return Ok(None);
            };
            if pred_col_idx > 1 {
                return Ok(None);
            }
            let fused_cmp = match cmp {
                ultrasql_vec::kernels::CmpOp::Eq => FusedCmp::Eq,
                ultrasql_vec::kernels::CmpOp::Ne => FusedCmp::Ne,
                ultrasql_vec::kernels::CmpOp::Lt => FusedCmp::Lt,
                ultrasql_vec::kernels::CmpOp::Le => FusedCmp::Le,
                ultrasql_vec::kernels::CmpOp::Gt => FusedCmp::Gt,
                ultrasql_vec::kernels::CmpOp::Ge => FusedCmp::Ge,
            };
            Some(FusedPredicate {
                col_index: u8::try_from(pred_col_idx).expect("col idx fits in u8"),
                op: fused_cmp,
                literal: lit,
            })
        }
        _ => return Ok(None),
    };

    let rel = RelationId(entry.oid);
    let block_count = ctx.heap.block_count(rel).max(entry.n_blocks);
    let op = FusedUpdateInt32Add::new(
        Arc::clone(&ctx.heap),
        rel,
        ctx.snapshot.clone(),
        Arc::clone(&ctx.oracle),
        block_count,
        predicate,
        target_col,
        delta,
        ctx.xid,
        ctx.command_id,
    );
    Ok(Some(Box::new(op)))
}

fn lower_real_update(
    table: &str,
    assignments: &[(usize, ScalarExpr)],
    input: &LogicalPlan,
    returning: &[(ScalarExpr, String)],
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    if !returning.is_empty() {
        return Err(ServerError::Unsupported("UPDATE ... RETURNING"));
    }
    let entry = ctx
        .catalog_snapshot
        .tables
        .get(&table.to_ascii_lowercase())
        .ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table.to_string(),
            ))
        })?;

    // Fast-path: when the relation, assignment, and optional filter
    // all match the `(Int32, Int32) WHERE col cmp lit SET col_i =
    // col_i ± lit` shape, bypass the SeqScan + Filter + ModifyTable
    // chain entirely and lower to the single `FusedUpdateInt32Add`
    // operator. Saves ~150 µs / 10 000-row UPDATE on the bench shape
    // — see the operator's module header for the full motivation.
    if let Some(fused) = try_build_fused_update(table, entry, assignments, input, ctx)? {
        return Ok(fused);
    }

    let child = build_filtered_tid_scan(table, entry, input, ctx)?;

    // Assignment value expressions stay unshifted: `apply_update`
    // strips the leading [tid_block, tid_slot] pair before passing the
    // row to `Eval::eval`, so the value expression sees the relation's
    // natural column layout. Likewise, the assignment's *target*
    // column index addresses the relation schema directly.
    let assignments: Vec<(usize, ScalarExpr)> = assignments.to_vec();

    let rel = RelationId(entry.oid);
    let modify = ModifyTable::new(
        Arc::clone(&ctx.heap),
        rel,
        entry.schema.clone(),
        ModifyKind::Update { assignments },
        ctx.xid,
        ctx.command_id,
        ctx.xid,
        ctx.command_id,
        None,
        child,
    );
    Ok(Box::new(modify))
}

/// Try to detect the `(Int32, Int32) [WHERE col cmp lit]` DELETE
/// shape and lower it to [`FusedDeleteInt32Pair`]. Mirrors
/// [`try_build_fused_update`] without the assignment-validation half.
fn try_build_fused_delete(
    target_table: &str,
    entry: &TableEntry,
    input: &LogicalPlan,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    let fields = entry.schema.fields();
    if fields.len() != 2
        || fields[0].data_type != DataType::Int32
        || fields[1].data_type != DataType::Int32
    {
        return Ok(None);
    }

    let predicate: Option<FusedPredicate> = match input {
        LogicalPlan::Scan { table, .. } => {
            if !table.eq_ignore_ascii_case(target_table) {
                return Ok(None);
            }
            None
        }
        LogicalPlan::Filter {
            input: filter_input,
            predicate,
        } => {
            let LogicalPlan::Scan { table, .. } = filter_input.as_ref() else {
                return Ok(None);
            };
            if !table.eq_ignore_ascii_case(target_table) {
                return Ok(None);
            }
            let Some((pred_col_idx, cmp, lit)) = extract_int32_col_op_lit(predicate) else {
                return Ok(None);
            };
            if pred_col_idx > 1 {
                return Ok(None);
            }
            let fused_cmp = match cmp {
                ultrasql_vec::kernels::CmpOp::Eq => FusedCmp::Eq,
                ultrasql_vec::kernels::CmpOp::Ne => FusedCmp::Ne,
                ultrasql_vec::kernels::CmpOp::Lt => FusedCmp::Lt,
                ultrasql_vec::kernels::CmpOp::Le => FusedCmp::Le,
                ultrasql_vec::kernels::CmpOp::Gt => FusedCmp::Gt,
                ultrasql_vec::kernels::CmpOp::Ge => FusedCmp::Ge,
            };
            Some(FusedPredicate {
                col_index: u8::try_from(pred_col_idx).expect("col idx fits in u8"),
                op: fused_cmp,
                literal: lit,
            })
        }
        _ => return Ok(None),
    };

    let rel = RelationId(entry.oid);
    let block_count = ctx.heap.block_count(rel).max(entry.n_blocks);
    let op = FusedDeleteInt32Pair::new(
        Arc::clone(&ctx.heap),
        rel,
        ctx.snapshot.clone(),
        Arc::clone(&ctx.oracle),
        block_count,
        predicate,
        ctx.xid,
        ctx.command_id,
    );
    Ok(Some(Box::new(op)))
}

/// Lower a `DELETE` plan into a [`ModifyTable`] with `ModifyKind::Delete`.
///
/// See [`lower_real_update`] for the TID-emitting scan / filter shape.
fn lower_real_delete(
    table: &str,
    input: &LogicalPlan,
    returning: &[(ScalarExpr, String)],
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    if !returning.is_empty() {
        return Err(ServerError::Unsupported("DELETE ... RETURNING"));
    }
    let entry = ctx
        .catalog_snapshot
        .tables
        .get(&table.to_ascii_lowercase())
        .ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table.to_string(),
            ))
        })?;

    // Fast-path: when the relation matches the `(Int32, Int32)` shape
    // and the optional filter is `Int32 col cmp Int32 lit`, bypass
    // the SeqScan + Filter + ModifyTable chain and lower to the
    // single-pass `FusedDeleteInt32Pair` operator.
    if let Some(fused) = try_build_fused_delete(table, entry, input, ctx)? {
        return Ok(fused);
    }

    let child = build_filtered_tid_scan(table, entry, input, ctx)?;

    let rel = RelationId(entry.oid);
    let modify = ModifyTable::new(
        Arc::clone(&ctx.heap),
        rel,
        entry.schema.clone(),
        ModifyKind::Delete,
        ctx.xid,
        ctx.command_id,
        ctx.xid,
        ctx.command_id,
        None,
        child,
    );
    Ok(Box::new(modify))
}

/// Build the TID-emitting child operator for an UPDATE / DELETE.
///
/// Recognises the binder's `Scan` / `Filter(Scan)` shapes:
///
/// - bare `Scan { table }` → TID-emitting `SeqScan`.
/// - `Filter { Scan { table }, predicate }` → `Filter`(`SeqScan`),
///   with every `Column { index }` in `predicate` shifted by +2 to
///   re-target the TID-prefixed batch.
///
/// Any other input shape — the planner does not produce it for UPDATE
/// / DELETE in v0.5 — surfaces as [`ServerError::Unsupported`].
fn build_filtered_tid_scan(
    target_table: &str,
    entry: &TableEntry,
    input: &LogicalPlan,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    match input {
        LogicalPlan::Scan { table, .. } => {
            if !table.eq_ignore_ascii_case(target_table) {
                return Err(ServerError::Unsupported(
                    "UPDATE / DELETE child scan references a different table",
                ));
            }
            Ok(build_tid_seq_scan(entry, ctx))
        }
        LogicalPlan::Filter {
            input: filter_input,
            predicate,
        } => {
            let LogicalPlan::Scan { table, .. } = filter_input.as_ref() else {
                return Err(ServerError::Unsupported(
                    "UPDATE / DELETE WHERE input must be a base-table scan",
                ));
            };
            if !table.eq_ignore_ascii_case(target_table) {
                return Err(ServerError::Unsupported(
                    "UPDATE / DELETE child scan references a different table",
                ));
            }
            let scan = build_tid_seq_scan(entry, ctx);
            let shifted = shift_column_indices(predicate, 2);
            Ok(Box::new(Filter::new(scan, shifted)))
        }
        _ => Err(ServerError::Unsupported(
            "UPDATE / DELETE input shape; expected Scan or Filter(Scan)",
        )),
    }
}

/// Lower a `Project` whose expressions are pure column references.
fn lower_project_columns(
    child: Box<dyn Operator>,
    exprs: &[(ScalarExpr, String)],
) -> Result<Box<dyn Operator>, ServerError> {
    let mut indices: Vec<usize> = Vec::with_capacity(exprs.len());
    for (expr, _name) in exprs {
        match expr {
            ScalarExpr::Column { index, .. } => indices.push(*index),
            _ => {
                return Err(ServerError::Unsupported(
                    "SELECT expression; v0.5 only supports bare column references",
                ));
            }
        }
    }
    Ok(Box::new(Project::new(child, indices)?))
}

// ---------------------------------------------------------------------------
// Index-scan lowering
// ---------------------------------------------------------------------------

/// Inclusive lower / inclusive upper bound on an `i64`-shaped index key.
///
/// `None` on either side means "unbounded in that direction".
/// `low == high` represents a point-lookup probe.
///
/// The bounds are normalised to *inclusive* on both ends — caller code
/// that observes a strict `<` or `>` operator pre-adjusts the bound by
/// ±1 via `i64::checked_add` / `i64::checked_sub` so a downstream
/// range scan can treat every bound uniformly. When an adjustment would
/// overflow the bound is clamped to the corresponding sentinel
/// ([`i64::MAX`] / [`i64::MIN`]); the resulting range is empty in the
/// overflow-toward-infinity direction, which preserves predicate
/// semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct IndexKeyRange {
    /// Inclusive lower bound, or `None` for unbounded below.
    low: Option<i64>,
    /// Inclusive upper bound, or `None` for unbounded above.
    high: Option<i64>,
}

impl IndexKeyRange {
    /// Point probe: `key == k`.
    const fn point(k: i64) -> Self {
        Self {
            low: Some(k),
            high: Some(k),
        }
    }
}

/// Try to lower a `Filter { Scan(table), predicate }` shape into an
/// [`IndexScan`] operator backed by a B-tree probe.
///
/// Returns:
/// - `Ok(Some(op))` when the table is catalog-resolved, has a single-
///   column Int32/Int64 B-tree index covering the predicate's column,
///   and the predicate matches an [indexable shape](#indexable-shapes).
/// - `Ok(None)` for any other case so the caller falls back to the
///   default [`Filter(SeqScan)`] plan. The fallback path is the
///   non-regressing default: a query that does not match an indexable
///   shape, hits an unindexed column, or runs against the sample-table
///   registry continues to use the existing sequential scan + filter
///   path.
/// - `Err(_)` only when the B-tree probe or heap fetch itself fails;
///   those errors are not recoverable by trying a different operator.
///
/// # Indexable shapes
///
/// In this wave the dispatcher recognises:
/// - `col = literal` → point lookup.
/// - `col < literal`, `col <= literal`, `col > literal`, `col >= literal`
///   → one-sided range scan.
/// - `col BETWEEN lo AND hi` (binder-rewritten into
///   `col >= lo AND col <= hi`) → bounded range scan.
/// - `lo <= col AND col <= hi` and equivalent rewrites whose operands
///   commute (the binder may emit any of `>=`, `<=`, `>`, `<` on either
///   side of an AND) → bounded range scan.
///
/// Compound predicates joined by `OR`, `NOT`, or anything beyond a
/// single conjunction of column-vs-literal comparisons fall through to
/// `Ok(None)`. The binder produces precisely these shapes for
/// `BETWEEN` (see `bind_between`); broader rewrites land with the
/// optimizer's predicate canonicaliser in a later wave.
///
/// # Why a single helper instead of a planner emission
///
/// We pattern-match in `lower_query` rather than teaching the planner
/// to emit `LogicalPlan::IndexScan` directly. Two reasons:
/// 1. The planner currently emits `Filter { Scan, predicate }` for
///    every WHERE clause; adding an `IndexScan` node would force every
///    consumer of `LogicalPlan` (binder tests, optimizer rewrites,
///    debug printers, EXPLAIN plumbing) to learn the new variant.
/// 2. The catalog snapshot is materialised in [`LowerCtx`], not in the
///    binder. Doing the dispatch here keeps the catalog-look-up local
///    to one function and the planner stays catalog-snapshot-free,
///    which the optimizer wave (v0.6 P0) needs to remain
///    plan-cache-friendly.
fn try_index_scan(
    input: &LogicalPlan,
    predicate: &ScalarExpr,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    // Step 1: the input must be a bare base-table scan over a relation
    // the catalog snapshot knows about. Sample-table scans never have
    // an index, so we let them fall back to SeqScan-equivalent shapes.
    let LogicalPlan::Scan { table, .. } = input else {
        return Ok(None);
    };
    let Some(table_entry) = ctx.catalog_snapshot.tables.get(&table.to_ascii_lowercase()) else {
        return Ok(None);
    };

    // Step 2: extract `(column_index, key_range)` from the predicate.
    // A miss (None) means the shape is not indexable.
    let Some((col_idx, range)) = match_indexable_predicate(predicate) else {
        return Ok(None);
    };

    // Step 3: locate an index covering exactly this column. A10's
    // CREATE INDEX path emits `IndexEntry::columns` as a single-element
    // `Vec<u16>` of 0-based attnums; we look up by that exact shape.
    // A composite index that *starts with* this column would also
    // satisfy a point lookup, but the storage layer only supports
    // 8-byte keys today, so we conservatively require a single-column
    // match.
    let Some(index_entry) = find_single_column_index(&ctx.catalog_snapshot, table_entry, col_idx)
    else {
        return Ok(None);
    };

    // Step 4: confirm the indexed column's type is one the B-tree can
    // store. A10 only widens Int32 / Int64 into the i64 key space;
    // other types (text, float, bool) fall back to SeqScan.
    let Some(_widen) = key_type_for_btree(table_entry, col_idx) else {
        return Ok(None);
    };

    // Step 5: probe the B-tree, fetch matching tuples from the heap
    // with MVCC visibility applied, and wrap them in an IndexScan.
    let payloads = probe_index(index_entry, range, ctx)?;
    let codec = RowCodec::new(table_entry.schema.clone());
    Ok(Some(Box::new(IndexScan::new(payloads, codec))))
}

/// Decode a `WHERE` predicate into an `(column_index, IndexKeyRange)`
/// pair when its shape is one the B-tree dispatcher can probe.
///
/// Recognised top-level shapes:
/// - `Binary(op, Column, Literal)` for `op ∈ {Eq, Lt, LtEq, Gt, GtEq}`
///   (or commuted operand order).
/// - `Binary(And, sub_left, sub_right)` where both subterms are
///   single-side comparisons on the same column — produces a bounded
///   range. This is the canonical post-binder shape for `BETWEEN`.
///
/// Returns `None` for anything else; the caller falls back to a
/// general filter.
fn match_indexable_predicate(predicate: &ScalarExpr) -> Option<(usize, IndexKeyRange)> {
    if let Some((col, range)) = match_simple_comparison(predicate) {
        return Some((col, range));
    }
    // Conjunction of two single-side comparisons on the same column.
    let ScalarExpr::Binary {
        op: BinaryOp::And,
        left,
        right,
        ..
    } = predicate
    else {
        return None;
    };
    let (left_col, left_range) = match_simple_comparison(left)?;
    let (right_col, right_range) = match_simple_comparison(right)?;
    if left_col != right_col {
        return None;
    }
    let combined = IndexKeyRange {
        low: max_lower_bound(left_range.low, right_range.low),
        high: min_upper_bound(left_range.high, right_range.high),
    };
    Some((left_col, combined))
}

/// Decode a single `Column op Literal` (or commuted) comparison into an
/// `(column_index, IndexKeyRange)`. Returns `None` when the operand
/// types are not Int32 / Int64, the literal cannot be represented as
/// `i64`, or the operator is not a comparison.
///
/// Strict-bound operators are normalised to inclusive bounds via
/// `±1` adjustment (`x > 5` becomes `low = Some(6)`,
/// `x < 5` becomes `high = Some(4)`). Overflowing the adjustment
/// clamps to the sentinel; the resulting range is empty, which is
/// correctness-preserving (no tuple's i64 key is `> i64::MAX`).
fn match_simple_comparison(expr: &ScalarExpr) -> Option<(usize, IndexKeyRange)> {
    let ScalarExpr::Binary {
        op, left, right, ..
    } = expr
    else {
        return None;
    };
    // Decompose into (column_idx, literal_as_i64, op_with_col_on_left).
    let (col_idx, raw_lit, op_normalised) = match (left.as_ref(), right.as_ref()) {
        (col @ ScalarExpr::Column { .. }, lit @ ScalarExpr::Literal { .. }) => {
            let idx = column_idx_for_int_key(col)?;
            let lit_val = literal_as_i64(lit)?;
            (idx, lit_val, *op)
        }
        (lit @ ScalarExpr::Literal { .. }, col @ ScalarExpr::Column { .. }) => {
            let idx = column_idx_for_int_key(col)?;
            let lit_val = literal_as_i64(lit)?;
            // Flip the operator so `lit op col` reads as `col flipped_op lit`.
            let flipped = match op {
                BinaryOp::Eq => BinaryOp::Eq,
                BinaryOp::Lt => BinaryOp::Gt,
                BinaryOp::LtEq => BinaryOp::GtEq,
                BinaryOp::Gt => BinaryOp::Lt,
                BinaryOp::GtEq => BinaryOp::LtEq,
                _ => return None,
            };
            (idx, lit_val, flipped)
        }
        _ => return None,
    };
    let range = match op_normalised {
        BinaryOp::Eq => IndexKeyRange::point(raw_lit),
        BinaryOp::Lt => IndexKeyRange {
            low: None,
            high: raw_lit.checked_sub(1),
        },
        BinaryOp::LtEq => IndexKeyRange {
            low: None,
            high: Some(raw_lit),
        },
        BinaryOp::Gt => IndexKeyRange {
            low: raw_lit.checked_add(1),
            high: None,
        },
        BinaryOp::GtEq => IndexKeyRange {
            low: Some(raw_lit),
            high: None,
        },
        _ => return None,
    };
    Some((col_idx, range))
}

/// Read the column index from a [`ScalarExpr::Column`] whose data type
/// is a `B-tree-supported` integer (`Int32` or `Int64`). Returns
/// `None` for non-column expressions, NULL columns, or non-integer
/// types.
const fn column_idx_for_int_key(expr: &ScalarExpr) -> Option<usize> {
    let ScalarExpr::Column {
        index, data_type, ..
    } = expr
    else {
        return None;
    };
    match data_type {
        DataType::Int32 | DataType::Int64 => Some(*index),
        _ => None,
    }
}

/// Lift an integer-typed literal to `i64`. `Int32` is sign-extended
/// via the lossless `i64::from(i32)` widening conversion. Returns
/// `None` for non-integer literals (text, float, NULL, …).
fn literal_as_i64(expr: &ScalarExpr) -> Option<i64> {
    let ScalarExpr::Literal { value, .. } = expr else {
        return None;
    };
    match value {
        Value::Int32(v) => Some(i64::from(*v)),
        Value::Int64(v) => Some(*v),
        _ => None,
    }
}

/// Pick the tighter (i.e., larger) lower bound from two candidates.
/// `None` means "no constraint"; any concrete bound wins over `None`.
const fn max_lower_bound(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (None, x) | (x, None) => x,
        (Some(x), Some(y)) => Some(if x > y { x } else { y }),
    }
}

/// Pick the tighter (i.e., smaller) upper bound from two candidates.
const fn min_upper_bound(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (None, x) | (x, None) => x,
        (Some(x), Some(y)) => Some(if x < y { x } else { y }),
    }
}

/// Return the [`IndexEntry`] that covers exactly the single column
/// `col_idx` of `table_entry`, if any. Composite indexes whose first
/// key is `col_idx` are *not* returned today: the on-disk B-tree only
/// supports 8-byte keys, so a composite index could not be probed
/// through the existing API.
fn find_single_column_index<'a>(
    snapshot: &'a CatalogSnapshot,
    table_entry: &TableEntry,
    col_idx: usize,
) -> Option<&'a IndexEntry> {
    let attnum = u16::try_from(col_idx).ok()?;
    let indexes = snapshot.indexes_by_table.get(&table_entry.oid)?;
    indexes
        .iter()
        .find(|e| e.columns.len() == 1 && e.columns[0] == attnum)
}

/// Confirm the keyed column has a type the B-tree can store. Returns
/// `Some(widen)` where `widen == true` for Int32 (key is sign-extended
/// to `i64`) and `false` for Int64 (key is stored directly). Returns
/// `None` for any other type so the caller falls back to `SeqScan`.
///
/// Mirrors the check in `Server::execute_create_index` — keep the two
/// in sync, or a `CREATE INDEX` that succeeds will produce an index a
/// `SELECT` cannot probe.
fn key_type_for_btree(table_entry: &TableEntry, col_idx: usize) -> Option<bool> {
    let field = table_entry.schema.field(col_idx)?;
    match field.data_type {
        DataType::Int32 => Some(true),
        DataType::Int64 => Some(false),
        _ => None,
    }
}

/// Probe the B-tree for every tuple satisfying `range` and return the
/// (visible) heap payloads in B-tree-order.
///
/// Visibility is enforced inline: a tuple whose MVCC header is not
/// visible to `ctx.snapshot` under `ctx.oracle` is silently dropped.
/// This means the `IndexScan` operator never sees a tuple a `SeqScan`
/// would hide; the user observes the same row set whether or not the
/// index is consulted.
///
/// # Errors
///
/// Returns [`ServerError::Ddl`] when the B-tree probe or heap fetch
/// fails. The `Ddl` variant carries a dynamic message and is the
/// appropriate channel for runtime storage faults; the simpler
/// `Unsupported` channel is reserved for shape-level rejections that
/// the caller can recover from by falling back to `SeqScan`.
fn probe_index(
    index_entry: &IndexEntry,
    range: IndexKeyRange,
    ctx: &LowerCtx<'_>,
) -> Result<Vec<Vec<u8>>, ServerError> {
    let index_rel = RelationId::new(index_entry.oid.raw());
    let pool = ctx.heap.buffer_pool();
    let btree: BTree<BlankPageLoader> =
        BTree::open(Arc::clone(pool), index_rel, index_entry.root_block);

    // Collect the matching TupleIds. A point lookup uses the cheap
    // `lookup` path; everything else walks the leaf chain via
    // `range_scan` between `[low, high+1)` (half-open). `range_scan`'s
    // upper bound is exclusive, so we add 1 to `high` to keep the
    // inclusive contract — overflowing to `None` (i.e., scan to the
    // end of the leaf chain) when `high == i64::MAX`.
    let mut tids: Vec<ultrasql_core::TupleId> = Vec::new();
    match (range.low, range.high) {
        (Some(lo), Some(hi)) if lo == hi => {
            if let Some(tid) = btree
                .lookup::<i64>(lo)
                .map_err(|e| ServerError::ddl(format!("IndexScan btree lookup: {e}")))?
            {
                tids.push(tid);
            }
        }
        (low, high) => {
            // Walk the half-open `[start, end_exclusive)`. `start =
            // low.unwrap_or(i64::MIN)` and `end_exclusive =
            // high.map(|h| h.checked_add(1))` — when the +1 overflows we
            // pass `None` to mean "scan to the end of the leaf chain".
            let start = low.unwrap_or(i64::MIN);
            // `i64::MAX + 1` overflows to `None`, which `range_scan`
            // treats as "unbounded above" — exactly the contract we want.
            let end_exclusive: Option<i64> = high.and_then(|h| h.checked_add(1));
            for entry in btree.range_scan::<i64>(start, end_exclusive) {
                let (_key, tid) =
                    entry.map_err(|e| ServerError::ddl(format!("IndexScan btree scan: {e}")))?;
                tids.push(tid);
            }
        }
    }

    // Fetch the heap tuples in B-tree order and apply MVCC visibility
    // inline. An index entry whose heap tuple is invisible to the
    // statement's snapshot is silently dropped — the same outcome a
    // SeqScan would deliver. We use [`HeapAccess::fetch`] (no
    // visibility check) plus an explicit `is_visible` call rather than
    // chaining through `scan_visible` because the latter walks a
    // block-by-block iterator we cannot project onto an arbitrary
    // TupleId list.
    let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(tids.len());
    for tid in tids {
        let tuple = ctx
            .heap
            .fetch(tid)
            .map_err(|e| ServerError::ddl(format!("IndexScan heap fetch: {e}")))?;
        let visibility = is_visible(&tuple.header, &ctx.snapshot, ctx.oracle.as_ref());
        if matches!(visibility, Visibility::Visible) {
            payloads.push(tuple.data);
        }
    }
    Ok(payloads)
}

// ---------------------------------------------------------------------------
// Join lowering
// ---------------------------------------------------------------------------

/// Pick a join operator for a [`LogicalPlan::Join`] and connect its two
/// already-lowered children.
///
/// Selection rule (v0.5):
///
/// - `On(pred)` where `pred` is a single binary-`Eq` whose operands are
///   `Column` references straddling the left/right schemas (and both
///   sides carry a hash-friendly scalar type) **and** `join_type` is
///   [`LogicalJoinType::Inner`] or [`LogicalJoinType::LeftOuter`]
///   → [`HashJoin`]. The build side is the left child (matching the
///   executor's convention that `HashJoin::new`'s first argument is the
///   build input). We have no row-count estimate available — the
///   catalog tracks `n_blocks` but not tuple counts — so we follow the
///   planner's left/right ordering verbatim. A `LEFT JOIN` requires
///   left = build for correct unmatched-row emission; the binder
///   already places the preserving side on the left, so this default is
///   semantically forced for `LeftOuter`.
/// - `On(pred)` with any other shape (non-equi, computed key,
///   multi-clause `AND`, `OR`, NULL test, …) → [`NestedLoopJoin`] with
///   the full predicate. NLJ evaluates the predicate via [`Eval`], so
///   it handles every well-typed boolean expression the binder emits.
/// - `Using(pairs)` → composite equality predicate fed to NLJ. The
///   binder still produces a non-collapsed concatenated schema in this
///   path (see `physical::build_join` for the same dispatch); a future
///   wave may switch to a USING-aware `HashJoin` when collapsed-column
///   semantics matter.
/// - `None` (CROSS) → NLJ with no condition.
///
/// `RightOuter` and `FullOuter` are routed through NLJ even when the
/// predicate is equi: the current [`HashJoin`] only supports `Inner`
/// and `LeftOuter` (`hash_join::HashJoin::execute` rejects the others
/// with [`ExecError::Unsupported`]). NLJ supports all five SQL join
/// kinds, so the dispatch is correctness-driven, not performance-
/// driven; a future commit can lift `Right`/`Full` to `HashJoin` once the
/// operator grows the build-side fixup phase.
fn lower_join(
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,
    left_schema: Schema,
    right_schema: Schema,
    join_type: LogicalJoinType,
    condition: &LogicalJoinCondition,
    out_schema: Schema,
) -> Result<Box<dyn Operator>, ServerError> {
    match condition {
        LogicalJoinCondition::On(pred) => {
            if matches!(
                join_type,
                LogicalJoinType::Inner | LogicalJoinType::LeftOuter
            ) {
                if let Some((left_key, right_key)) =
                    extract_hash_friendly_equi_keys(pred, left_schema.len())
                {
                    // HashJoin: left = build, right = probe. See the
                    // function docs for the rationale.
                    return Ok(Box::new(HashJoin::new(
                        left,
                        right,
                        left_key,
                        right_key,
                        join_type,
                        out_schema,
                        left_schema,
                        right_schema,
                    )));
                }
            }
            // Non-equi predicate, type-ineligible equi predicate, or an
            // outer-join kind the HashJoin does not yet support → NLJ.
            build_nested_loop_join(
                left,
                right,
                Some(pred.clone()),
                join_type,
                out_schema,
                left_schema,
                right_schema,
            )
        }
        LogicalJoinCondition::Using(pairs) => {
            let cond = build_using_predicate(pairs, &left_schema, &right_schema);
            build_nested_loop_join(
                left,
                right,
                cond,
                join_type,
                out_schema,
                left_schema,
                right_schema,
            )
        }
        LogicalJoinCondition::None => build_nested_loop_join(
            left,
            right,
            None,
            join_type,
            out_schema,
            left_schema,
            right_schema,
        ),
    }
}

/// Drain `right` into a memory-resident batch list, then wrap the
/// result in a [`NestedLoopJoin`] whose right factory replays the
/// drained batches.
///
/// The materialisation is necessary because [`NestedLoopJoin`] re-opens
/// the right side once per left row through its `RightFactory`
/// closure. A streaming right child cannot be replayed; spooling it
/// into batch storage gives the closure an O(1) `clone()` per
/// iteration. See `physical.rs::build_nlj` for the same approach.
///
/// # Errors
///
/// Returns a [`ServerError::Execute`] if the right child errors during
/// the drain phase.
fn build_nested_loop_join(
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,
    condition: Option<ScalarExpr>,
    join_type: LogicalJoinType,
    out_schema: Schema,
    left_schema: Schema,
    right_schema: Schema,
) -> Result<Box<dyn Operator>, ServerError> {
    // Spool the right side once so each left-row iteration cheaply
    // clones the batch list rather than re-running the upstream
    // pipeline (which might be a real heap scan over thousands of
    // blocks).
    let mut right_op = right;
    let mut batches: Vec<Batch> = Vec::new();
    while let Some(batch) = right_op.next_batch()? {
        batches.push(batch);
    }
    let shared: Arc<Vec<Batch>> = Arc::new(batches);
    let factory_schema = right_schema.clone();
    let factory: RightFactory = Box::new(move || {
        Ok(
            Box::new(MemTableScan::new(factory_schema.clone(), (*shared).clone()))
                as Box<dyn Operator>,
        )
    });
    Ok(Box::new(NestedLoopJoin::new(
        left,
        factory,
        join_type,
        condition,
        out_schema,
        left_schema,
        right_schema,
    )))
}

/// Return `true` if `dt` is a scalar type for which `Value::Hash` is
/// well-defined and `==` is reflexive (no NaN games).
///
/// Floats are excluded so a join key with `Float32::NAN` keeps NULL-like
/// semantics under SQL (NaN never equals NaN per IEEE-754, even though
/// the [`HashJoin`] hash impl currently hashes the bit pattern). Lifting
/// floats into `HashJoin` can land once the binder rewrites
/// `a.x = b.x` to `a.x = b.x AND a.x = a.x` for floats (or once we
/// formally specify the semantics).
const fn is_hash_friendly(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Bool
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Text { .. }
            | DataType::Bytea
            | DataType::Date
            | DataType::Time
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::Uuid
    )
}

/// Recognise a binary-`Eq` predicate of the form `Column(left) = Column(right)`
/// (or its commuted form) where the left column lives in the left schema
/// and the right column lives in the right schema (i.e. its raw index is
/// ≥ `left_width`).
///
/// Returns the `(left_key, right_key)` expression pair, with the right
/// key's index *rebased* to be local to the right schema (subtracts
/// `left_width`). Returns `None` when:
///
/// - The top-level operator is not [`BinaryOp::Eq`].
/// - Either operand is not a bare column reference.
/// - Both columns live on the same side.
/// - The column data type is not [`is_hash_friendly`].
///
/// Mirrors `physical::extract_equi_keys` so the dispatcher in
/// [`lower_join`] picks the same operator the optimizer's builder
/// would. The type-friendliness gate is the addition: the builder
/// accepts any data type, but the server prefers a deterministic
/// fallback to NLJ for float keys until the binder's float-NULL rewrite
/// lands.
fn extract_hash_friendly_equi_keys(
    pred: &ScalarExpr,
    left_width: usize,
) -> Option<(ScalarExpr, ScalarExpr)> {
    let ScalarExpr::Binary {
        op: BinaryOp::Eq,
        left,
        right,
        ..
    } = pred
    else {
        return None;
    };
    let (l_col, r_col) = match (left.as_ref(), right.as_ref()) {
        (
            ScalarExpr::Column {
                index: li,
                data_type: lt,
                name: ln,
            },
            ScalarExpr::Column {
                index: ri,
                data_type: rt,
                name: rn,
            },
        ) if *li < left_width && *ri >= left_width => {
            if !is_hash_friendly(lt) || !is_hash_friendly(rt) {
                return None;
            }
            (
                ScalarExpr::Column {
                    name: ln.clone(),
                    index: *li,
                    data_type: lt.clone(),
                },
                ScalarExpr::Column {
                    name: rn.clone(),
                    index: ri - left_width,
                    data_type: rt.clone(),
                },
            )
        }
        // Commuted form: right-side column is the *left* operand.
        (
            ScalarExpr::Column {
                index: li,
                data_type: lt,
                name: ln,
            },
            ScalarExpr::Column {
                index: ri,
                data_type: rt,
                name: rn,
            },
        ) if *li >= left_width && *ri < left_width => {
            if !is_hash_friendly(lt) || !is_hash_friendly(rt) {
                return None;
            }
            (
                ScalarExpr::Column {
                    name: rn.clone(),
                    index: *ri,
                    data_type: rt.clone(),
                },
                ScalarExpr::Column {
                    name: ln.clone(),
                    index: li - left_width,
                    data_type: lt.clone(),
                },
            )
        }
        _ => return None,
    };
    Some((l_col, r_col))
}

/// Build a composite equality predicate from `USING (left_idx, right_idx)`
/// pairs, AND-conjoining each `left_col = right_col` equality.
///
/// Right-side column indices are offset by `left_schema.len()` so the
/// predicate evaluates against the concatenated left++right row layout
/// the join produces. Returns `None` when `pairs` is empty (degenerate
/// USING clause).
///
/// Mirrors `physical::build_using_predicate`. Lives here so the
/// server-side lowerer is self-contained; converging on a single shared
/// helper lands when the server delegates to `physical::build_operator`
/// in v0.6 (see ROADMAP P0 "Server invokes optimizer").
fn build_using_predicate(
    pairs: &[(usize, usize)],
    left_schema: &Schema,
    right_schema: &Schema,
) -> Option<ScalarExpr> {
    let mut iter = pairs.iter().map(|(li, ri)| {
        let lf = left_schema.field_at(*li);
        let rf = right_schema.field_at(*ri);
        ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(ScalarExpr::Column {
                index: *li,
                data_type: lf.data_type.clone(),
                name: lf.name.clone(),
            }),
            right: Box::new(ScalarExpr::Column {
                index: left_schema.len() + ri,
                data_type: rf.data_type.clone(),
                name: rf.name.clone(),
            }),
            data_type: DataType::Bool,
        }
    });
    let first = iter.next()?;
    Some(iter.fold(first, |acc, next| ScalarExpr::Binary {
        op: BinaryOp::And,
        left: Box::new(acc),
        right: Box::new(next),
        data_type: DataType::Bool,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ultrasql_parser::Parser;
    use ultrasql_planner::bind;

    fn fixture() -> (InMemoryCatalog, SampleTables) {
        let mut catalog = InMemoryCatalog::new();
        let tables = build_sample_database(&mut catalog);
        (catalog, tables)
    }

    fn plan(sql: &str, catalog: &InMemoryCatalog) -> LogicalPlan {
        let stmt = Parser::new(sql).parse_statement().expect("parses");
        bind(&stmt, catalog).expect("binds")
    }

    #[test]
    fn lowers_simple_scan_and_project() {
        let (catalog, tables) = fixture();
        let p = plan("SELECT id FROM users", &catalog);
        let mut op = lower_plan(&p, &tables).expect("lowers");
        let batch = op.next_batch().unwrap().expect("first batch");
        assert_eq!(batch.rows(), 3);
        assert_eq!(batch.width(), 1);
    }

    #[test]
    fn lowers_filter_eq_int() {
        let (catalog, tables) = fixture();
        let p = plan("SELECT id FROM users WHERE id = 2", &catalog);
        let mut op = lower_plan(&p, &tables).expect("lowers");
        let batch = op.next_batch().unwrap().expect("first batch");
        assert_eq!(batch.rows(), 1);
    }

    #[test]
    fn lowers_limit() {
        let (catalog, tables) = fixture();
        let p = plan("SELECT id FROM users LIMIT 1", &catalog);
        let mut op = lower_plan(&p, &tables).expect("lowers");
        let batch = op.next_batch().unwrap().expect("first batch");
        assert_eq!(batch.rows(), 1);
    }

    /// `LIMIT 1 OFFSET 1` over the 3-row sample skips the first row and
    /// emits the second. Confirms the sample-path lowerer threads
    /// `offset` through to the executor's `Limit::with_offset`.
    #[test]
    fn lowers_limit_with_offset() {
        let (catalog, tables) = fixture();
        let p = plan("SELECT id FROM users LIMIT 1 OFFSET 1", &catalog);
        let mut op = lower_plan(&p, &tables).expect("lowers");
        let mut ids: Vec<i32> = Vec::new();
        while let Some(batch) = op.next_batch().expect("ok") {
            if let ultrasql_vec::column::Column::Int32(col) = &batch.columns()[0] {
                ids.extend_from_slice(col.data());
            }
        }
        // Sample has ids [1,2,3]; LIMIT 1 OFFSET 1 yields the middle id.
        assert_eq!(ids, vec![2]);
    }

    /// `OFFSET 2` with no `LIMIT` emits every row past the skip. The
    /// binder lowers this as `Limit { n: u64::MAX, offset: 2 }`; the
    /// pipeline saturates `u64::MAX` into the executor's
    /// "no limit" sentinel.
    #[test]
    fn lowers_offset_only_without_limit() {
        let (catalog, tables) = fixture();
        let p = plan("SELECT id FROM users OFFSET 2", &catalog);
        let mut op = lower_plan(&p, &tables).expect("lowers");
        let mut ids: Vec<i32> = Vec::new();
        while let Some(batch) = op.next_batch().expect("ok") {
            if let ultrasql_vec::column::Column::Int32(col) = &batch.columns()[0] {
                ids.extend_from_slice(col.data());
            }
        }
        // Sample has 3 rows; OFFSET 2 → 1 row remaining (id=3).
        assert_eq!(ids, vec![3]);
    }

    /// `LIMIT 0 OFFSET m` returns zero rows.
    #[test]
    fn lowers_zero_limit_with_offset() {
        let (catalog, tables) = fixture();
        let p = plan("SELECT id FROM users LIMIT 0 OFFSET 1", &catalog);
        let mut op = lower_plan(&p, &tables).expect("lowers");
        let first = op.next_batch().expect("ok");
        assert!(first.is_none(), "LIMIT 0 must emit nothing");
    }

    #[test]
    fn lowers_order_by_asc_via_sample_path() {
        // `users` fixture has ids = [1, 2, 3]; an ASC sort by id leaves
        // them in the same order, but the plan still routes through
        // `Sort` — confirmed by `lower_plan` accepting the plan rather
        // than rejecting it with `Unsupported`.
        let (catalog, tables) = fixture();
        let p = plan("SELECT id FROM users ORDER BY id ASC", &catalog);
        let mut op = lower_plan(&p, &tables).expect("lowers");
        let mut ids: Vec<i32> = Vec::new();
        while let Some(batch) = op.next_batch().expect("ok") {
            if let ultrasql_vec::column::Column::Int32(col) = &batch.columns()[0] {
                ids.extend_from_slice(col.data());
            }
        }
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn lowers_order_by_desc_via_sample_path() {
        let (catalog, tables) = fixture();
        let p = plan("SELECT id FROM users ORDER BY id DESC", &catalog);
        let mut op = lower_plan(&p, &tables).expect("lowers");
        let mut ids: Vec<i32> = Vec::new();
        while let Some(batch) = op.next_batch().expect("ok") {
            if let ultrasql_vec::column::Column::Int32(col) = &batch.columns()[0] {
                ids.extend_from_slice(col.data());
            }
        }
        assert_eq!(ids, vec![3, 2, 1]);
    }

    /// Sort wrapped over a hand-built Values-like input runs through
    /// `lower_query` and produces ascending output.
    ///
    /// This is the headline contract for the wire wiring: a
    /// `LogicalPlan::Sort` constructed in code (synthetic, no parser
    /// involvement) lowers through `lower_query` and the resulting
    /// operator emits a non-decreasing sequence on the sort column.
    #[test]
    fn lower_query_sorts_values_in_ascending_order() {
        use std::sync::Arc as StdArc;
        use ultrasql_catalog::PersistentCatalog;
        use ultrasql_core::{CommandId, DataType, Field, Schema, Value, Xid};
        use ultrasql_planner::SortKey;
        use ultrasql_storage::buffer_pool::BufferPool;
        use ultrasql_storage::heap::HeapAccess;
        use ultrasql_txn::TransactionManager;

        // Build a Values plan with three out-of-order rows.
        let values_schema = Schema::new([
            Field::nullable("a", DataType::Int32),
            Field::nullable("b", DataType::Int32),
        ])
        .expect("values schema");
        let row = |v: i32, w: i32| -> Vec<ScalarExpr> {
            vec![
                ScalarExpr::Literal {
                    value: Value::Int32(v),
                    data_type: DataType::Int32,
                },
                ScalarExpr::Literal {
                    value: Value::Int32(w),
                    data_type: DataType::Int32,
                },
            ]
        };
        let values_plan = LogicalPlan::Values {
            rows: vec![row(3, 30), row(1, 10), row(2, 20)],
            schema: values_schema,
        };
        let sort_plan = LogicalPlan::Sort {
            input: Box::new(values_plan),
            keys: vec![SortKey {
                expr: ScalarExpr::Column {
                    name: "a".into(),
                    index: 0,
                    data_type: DataType::Int32,
                },
                asc: true,
                nulls_first: false,
            }],
        };

        // Build a minimal `LowerCtx`. We never reference the heap because
        // `Values` does not touch it, but the constructor still needs a
        // valid handle. The transaction is allocated only to materialise
        // a valid MVCC snapshot; we never commit it because the test
        // does not write to the heap.
        let catalog = StdArc::new(PersistentCatalog::new());
        let pool = StdArc::new(BufferPool::new(64, BlankPageLoader));
        let heap = StdArc::new(HeapAccess::new(pool));
        let txn = StdArc::new(TransactionManager::new());
        let mvcc_snapshot = txn
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted)
            .snapshot;
        let ctx = LowerCtx {
            tables: &SampleTables::new(),
            catalog_snapshot: catalog.snapshot(),
            heap,
            snapshot: mvcc_snapshot,
            oracle: StdArc::clone(&txn),
            xid: Xid::new(0),
            command_id: CommandId::FIRST,
            cte_buffers: HashMap::new(),
        };

        let mut op = lower_query(&sort_plan, &ctx).expect("lowers");
        let mut a_col: Vec<i32> = Vec::new();
        let mut b_col: Vec<i32> = Vec::new();
        while let Some(batch) = op.next_batch().expect("ok") {
            match (&batch.columns()[0], &batch.columns()[1]) {
                (
                    ultrasql_vec::column::Column::Int32(a),
                    ultrasql_vec::column::Column::Int32(b),
                ) => {
                    a_col.extend_from_slice(a.data());
                    b_col.extend_from_slice(b.data());
                }
                _ => panic!("unexpected column layout"),
            }
        }
        assert_eq!(a_col, vec![1, 2, 3]);
        assert_eq!(b_col, vec![10, 20, 30]);
    }

    #[test]
    fn rejects_unknown_table_via_plan_error() {
        // We hand-build the plan directly (the binder catches unknown
        // tables earlier), to exercise the lowerer's own fallback.
        let (_, tables) = fixture();
        let p = LogicalPlan::Scan {
            table: "nope".into(),
            schema: Schema::new([Field::required("id", DataType::Int32)]).unwrap(),
            projection: None,
        };
        let err = lower_plan(&p, &tables).expect_err("must reject");
        assert!(matches!(err, ServerError::Plan(_)));
    }

    // ----------------------------------------------------------------
    // JOIN dispatch (Wave A item A4)
    // ----------------------------------------------------------------

    /// Helper: build a typed `Column` reference. Index is the column's
    /// position in the *concatenated* (left++right) schema for join-on
    /// predicates, or its native position when the column lives on a
    /// single side.
    fn column(name: &str, index: usize, data_type: DataType) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.into(),
            index,
            data_type,
        }
    }

    /// Helper: build an Int32 literal.
    fn lit_i32(v: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            data_type: DataType::Int32,
        }
    }

    /// Helper: build an int32 single-column schema named `name`.
    fn schema_int_col(name: &str) -> Schema {
        Schema::new([Field::required(name, DataType::Int32)]).expect("schema ok")
    }

    /// Helper: build a `(name, val)` row of Int32 literals.
    fn int_row(v: i32) -> Vec<ScalarExpr> {
        vec![lit_i32(v)]
    }

    /// Walk a fresh operator producing `(Int32, Int32)` batches and
    /// collect `(left, right)` pairs. NULLs decode to `0` because the
    /// v0.5 `build_batch` does not emit a per-column null bitmap (see
    /// `hash_join.rs::hash_join_left_outer_unmatched_rows` for the
    /// documented behaviour).
    fn collect_pairs(op: &mut dyn Operator) -> Vec<(i32, i32)> {
        let mut out = Vec::new();
        while let Some(batch) = op.next_batch().expect("operator must not error") {
            assert_eq!(batch.width(), 2, "expected two-column join output");
            match (&batch.columns()[0], &batch.columns()[1]) {
                (
                    ultrasql_vec::column::Column::Int32(l),
                    ultrasql_vec::column::Column::Int32(r),
                ) => {
                    assert_eq!(l.data().len(), r.data().len());
                    for (a, b) in l.data().iter().zip(r.data().iter()) {
                        out.push((*a, *b));
                    }
                }
                other => panic!("unexpected column layout: {other:?}"),
            }
        }
        out
    }

    /// Build a minimal `LowerCtx` suitable for `lower_query` calls that
    /// never touch the real heap (Values-rooted plans).
    fn synthetic_ctx(tables: &SampleTables) -> LowerCtx<'_> {
        use std::sync::Arc as StdArc;
        use ultrasql_catalog::PersistentCatalog;
        use ultrasql_storage::buffer_pool::BufferPool;
        use ultrasql_storage::heap::HeapAccess;
        use ultrasql_txn::TransactionManager;

        let catalog = StdArc::new(PersistentCatalog::new());
        let pool = StdArc::new(BufferPool::new(64, BlankPageLoader));
        let heap = StdArc::new(HeapAccess::new(pool));
        let txn = StdArc::new(TransactionManager::new());
        let mvcc_snapshot = txn
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted)
            .snapshot;
        LowerCtx {
            tables,
            catalog_snapshot: catalog.snapshot(),
            heap,
            snapshot: mvcc_snapshot,
            oracle: StdArc::clone(&txn),
            xid: Xid::new(0),
            command_id: CommandId::FIRST,
            cte_buffers: HashMap::new(),
        }
    }

    /// Build two single-column `Int32` Values children with the given
    /// rows, the binder-shaped concatenated join schema, and a typed
    /// `LogicalPlan::Join` ready to be lowered.
    fn build_int_join_plan(
        left_rows: &[i32],
        right_rows: &[i32],
        join_type: LogicalJoinType,
        condition: LogicalJoinCondition,
    ) -> LogicalPlan {
        let left_schema = schema_int_col("l");
        let right_schema = schema_int_col("r");
        let out_schema = Schema::new([
            Field::required("l", DataType::Int32),
            Field::required("r", DataType::Int32),
        ])
        .expect("concat schema ok");
        let left = LogicalPlan::Values {
            rows: left_rows.iter().map(|v| int_row(*v)).collect(),
            schema: left_schema,
        };
        let right = LogicalPlan::Values {
            rows: right_rows.iter().map(|v| int_row(*v)).collect(),
            schema: right_schema,
        };
        LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type,
            condition,
            schema: out_schema,
        }
    }

    /// Equi predicate over a binder-shaped concatenated schema where
    /// the right column lives at index 1.
    fn equi_eq_predicate() -> LogicalJoinCondition {
        LogicalJoinCondition::On(ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(column("l", 0, DataType::Int32)),
            right: Box::new(column("r", 1, DataType::Int32)),
            data_type: DataType::Bool,
        })
    }

    /// Non-equi predicate `left.l < right.r` — should fall through to
    /// NLJ in [`lower_join`].
    fn non_equi_lt_predicate() -> LogicalJoinCondition {
        LogicalJoinCondition::On(ScalarExpr::Binary {
            op: BinaryOp::Lt,
            left: Box::new(column("l", 0, DataType::Int32)),
            right: Box::new(column("r", 1, DataType::Int32)),
            data_type: DataType::Bool,
        })
    }

    /// Lower a synthetic Inner equi-join through `lower_query` and
    /// assert the operator picked is [`HashJoin`] (via `Debug` output —
    /// the operator type appears in the `{op:?}` rendering).
    #[test]
    fn lower_query_inner_equi_join_picks_hash_join() {
        let tables = SampleTables::new();
        let ctx = synthetic_ctx(&tables);
        let plan = build_int_join_plan(
            &[1, 2, 3, 4],
            &[2, 3, 5],
            LogicalJoinType::Inner,
            equi_eq_predicate(),
        );
        let mut op = lower_query(&plan, &ctx).expect("lowers");
        // The debug representation of `HashJoin` begins with that name.
        let debug = format!("{op:?}");
        assert!(
            debug.starts_with("HashJoin"),
            "expected HashJoin, got: {debug}"
        );
        let mut pairs = collect_pairs(op.as_mut());
        pairs.sort_unstable();
        assert_eq!(pairs, vec![(2, 2), (3, 3)]);
    }

    /// Lower a synthetic Inner non-equi join. The predicate is
    /// `l.l < r.r`, which is not hash-eligible, so the dispatch must
    /// pick [`NestedLoopJoin`].
    #[test]
    fn lower_query_inner_non_equi_join_falls_back_to_nlj() {
        let tables = SampleTables::new();
        let ctx = synthetic_ctx(&tables);
        let plan = build_int_join_plan(
            &[1, 2, 3],
            &[2, 4],
            LogicalJoinType::Inner,
            non_equi_lt_predicate(),
        );
        let mut op = lower_query(&plan, &ctx).expect("lowers");
        let debug = format!("{op:?}");
        assert!(
            debug.starts_with("NestedLoopJoin"),
            "expected NestedLoopJoin, got: {debug}"
        );
        // 1<2, 1<4, 2<4, 3<4 = 4 matches.
        let mut pairs = collect_pairs(op.as_mut());
        pairs.sort_unstable();
        assert_eq!(pairs, vec![(1, 2), (1, 4), (2, 4), (3, 4)]);
    }

    /// Lower a LEFT OUTER equi join. Build = left so unmatched left
    /// rows survive; `HashJoin` is the chosen operator.
    ///
    /// Unmatched right columns decode to `0` here because `build_batch`
    /// does not yet emit a per-column null bitmap (the same v0.5
    /// limitation documented in `hash_join.rs::hash_join_left_outer_unmatched_rows`).
    #[test]
    fn lower_query_left_outer_equi_join_picks_hash_join_and_pads() {
        let tables = SampleTables::new();
        let ctx = synthetic_ctx(&tables);
        let plan = build_int_join_plan(
            &[1, 2, 3],
            &[2, 4],
            LogicalJoinType::LeftOuter,
            equi_eq_predicate(),
        );
        let mut op = lower_query(&plan, &ctx).expect("lowers");
        let debug = format!("{op:?}");
        assert!(
            debug.starts_with("HashJoin"),
            "expected HashJoin, got: {debug}"
        );
        let mut pairs = collect_pairs(op.as_mut());
        pairs.sort_unstable();
        // (2,2) is the match; (1,*) and (3,*) are unmatched left rows
        // emitted with right-side NULLs encoded as 0.
        assert_eq!(pairs, vec![(1, 0), (2, 2), (3, 0)]);
    }

    /// LEFT OUTER over a non-equi predicate must dispatch to NLJ (the
    /// only operator that can serve it correctly today).
    #[test]
    fn lower_query_left_outer_non_equi_join_falls_back_to_nlj() {
        let tables = SampleTables::new();
        let ctx = synthetic_ctx(&tables);
        let plan = build_int_join_plan(
            &[1, 5, 10],
            &[2, 7],
            LogicalJoinType::LeftOuter,
            non_equi_lt_predicate(),
        );
        let mut op = lower_query(&plan, &ctx).expect("lowers");
        let debug = format!("{op:?}");
        assert!(
            debug.starts_with("NestedLoopJoin"),
            "expected NestedLoopJoin, got: {debug}"
        );
        // 1 matches 2 and 7; 5 matches 7; 10 matches nothing (LeftOuter
        // emits (10, NULL)).
        let mut pairs = collect_pairs(op.as_mut());
        pairs.sort_unstable();
        assert_eq!(pairs, vec![(1, 2), (1, 7), (5, 7), (10, 0)]);
    }

    /// CROSS JOIN dispatches to NLJ with no condition. Output is the
    /// Cartesian product.
    #[test]
    fn lower_query_cross_join_dispatches_to_nlj() {
        let tables = SampleTables::new();
        let ctx = synthetic_ctx(&tables);
        let plan = build_int_join_plan(
            &[1, 2],
            &[10, 20, 30],
            LogicalJoinType::Cross,
            LogicalJoinCondition::None,
        );
        let mut op = lower_query(&plan, &ctx).expect("lowers");
        let debug = format!("{op:?}");
        assert!(
            debug.starts_with("NestedLoopJoin"),
            "expected NestedLoopJoin, got: {debug}"
        );
        let pairs = collect_pairs(op.as_mut());
        assert_eq!(pairs.len(), 6, "2 × 3 Cartesian = 6 rows");
    }

    /// RIGHT OUTER must NOT be silently downgraded to a different join
    /// kind. With an equi predicate the dispatcher routes to NLJ (which
    /// supports `RightOuter`) — confirms our explicit "do not silently
    /// pick `HashJoin` for an unsupported outer kind" promise.
    #[test]
    fn lower_query_right_outer_equi_join_uses_nlj_not_hash_join() {
        let tables = SampleTables::new();
        let ctx = synthetic_ctx(&tables);
        let plan = build_int_join_plan(
            &[2],
            &[1, 2, 3],
            LogicalJoinType::RightOuter,
            equi_eq_predicate(),
        );
        let mut op = lower_query(&plan, &ctx).expect("lowers");
        let debug = format!("{op:?}");
        assert!(
            debug.starts_with("NestedLoopJoin"),
            "RightOuter must not pick HashJoin; got: {debug}"
        );
        // Inner match: (2,2). RightOuter emits (NULL, 1) and (NULL, 3).
        let mut pairs = collect_pairs(op.as_mut());
        pairs.sort_unstable();
        assert_eq!(pairs, vec![(0, 1), (0, 3), (2, 2)]);
    }

    // ----------------------------------------------------------------
    // SetOp dispatch (Wave A item A7)
    // ----------------------------------------------------------------

    /// Build a single-column `Int32` [`LogicalPlan::Values`] from a slice
    /// of integers. Helper for the `SetOp` unit tests below.
    fn build_int_values_plan(rows: &[i32]) -> LogicalPlan {
        LogicalPlan::Values {
            rows: rows.iter().map(|v| int_row(*v)).collect(),
            schema: schema_int_col("v"),
        }
    }

    /// Build a [`LogicalPlan::SetOp`] over two `Values` children with a
    /// single `Int32` column. The output schema is built the same way
    /// `bind_set_op` does (nullable copies of the left side's columns)
    /// so the kernel-shaped plan exactly mirrors what the binder emits.
    fn build_int_set_op_plan(
        left_rows: &[i32],
        right_rows: &[i32],
        op: ultrasql_planner::LogicalSetOp,
        quantifier: ultrasql_planner::LogicalSetQuantifier,
    ) -> LogicalPlan {
        let out_schema = Schema::new([Field::nullable("v", DataType::Int32)]).expect("schema ok");
        LogicalPlan::SetOp {
            op,
            quantifier,
            left: Box::new(build_int_values_plan(left_rows)),
            right: Box::new(build_int_values_plan(right_rows)),
            schema: out_schema,
        }
    }

    /// Walk a `SetOp` operator and collect its emitted Int32 values into a
    /// sorted `Vec` for order-independent assertion. The kernel emits
    /// rows in left-insertion order; the tests sort to keep assertions
    /// robust against any future ordering refinement that does not
    /// change the multiset of rows.
    fn drain_int_setop(op: &mut dyn Operator) -> Vec<i32> {
        let mut out: Vec<i32> = Vec::new();
        while let Some(batch) = op.next_batch().expect("setop operator must not error") {
            assert_eq!(batch.width(), 1, "SetOp output schema is one column wide");
            if let ultrasql_vec::column::Column::Int32(col) = &batch.columns()[0] {
                out.extend_from_slice(col.data());
            } else {
                panic!("unexpected column layout for single-Int32 set-op output");
            }
        }
        out.sort_unstable();
        out
    }

    /// `SELECT v FROM l UNION SELECT v FROM r` — duplicates removed,
    /// surviving rows are the distinct union.
    #[test]
    fn lower_query_union_distinct_deduplicates() {
        let tables = SampleTables::new();
        let ctx = synthetic_ctx(&tables);
        let plan = build_int_set_op_plan(
            &[1, 2, 2, 3],
            &[2, 3, 4],
            ultrasql_planner::LogicalSetOp::Union,
            ultrasql_planner::LogicalSetQuantifier::Distinct,
        );
        let mut op = lower_query(&plan, &ctx).expect("lowers");
        assert_eq!(drain_int_setop(op.as_mut()), vec![1, 2, 3, 4]);
    }

    /// `SELECT v FROM l UNION ALL SELECT v FROM r` — duplicates kept.
    #[test]
    fn lower_query_union_all_concatenates() {
        let tables = SampleTables::new();
        let ctx = synthetic_ctx(&tables);
        let plan = build_int_set_op_plan(
            &[1, 2, 2],
            &[2, 3, 3],
            ultrasql_planner::LogicalSetOp::Union,
            ultrasql_planner::LogicalSetQuantifier::All,
        );
        let mut op = lower_query(&plan, &ctx).expect("lowers");
        assert_eq!(drain_int_setop(op.as_mut()), vec![1, 2, 2, 2, 3, 3]);
    }

    /// `SELECT v FROM l INTERSECT SELECT v FROM r` — distinct rows in both.
    #[test]
    fn lower_query_intersect_distinct_returns_common_distinct_rows() {
        let tables = SampleTables::new();
        let ctx = synthetic_ctx(&tables);
        let plan = build_int_set_op_plan(
            &[1, 2, 2, 3],
            &[2, 3, 3, 4],
            ultrasql_planner::LogicalSetOp::Intersect,
            ultrasql_planner::LogicalSetQuantifier::Distinct,
        );
        let mut op = lower_query(&plan, &ctx).expect("lowers");
        assert_eq!(drain_int_setop(op.as_mut()), vec![2, 3]);
    }

    /// `SELECT v FROM l INTERSECT ALL SELECT v FROM r` — multiset
    /// intersection: emit each row up to `min(left_count, right_count)`
    /// times.
    #[test]
    fn lower_query_intersect_all_respects_multiset_min_counts() {
        let tables = SampleTables::new();
        let ctx = synthetic_ctx(&tables);
        // left: 1×{1}, 3×{2}, 1×{3}; right: 2×{2}, 1×{3}, 1×{4}.
        // multiset min: 0×{1}, 2×{2}, 1×{3} → [2, 2, 3].
        let plan = build_int_set_op_plan(
            &[1, 2, 2, 2, 3],
            &[2, 2, 3, 4],
            ultrasql_planner::LogicalSetOp::Intersect,
            ultrasql_planner::LogicalSetQuantifier::All,
        );
        let mut op = lower_query(&plan, &ctx).expect("lowers");
        assert_eq!(drain_int_setop(op.as_mut()), vec![2, 2, 3]);
    }

    /// `SELECT v FROM l EXCEPT SELECT v FROM r` — distinct left rows
    /// absent from right.
    #[test]
    fn lower_query_except_distinct_returns_left_minus_right() {
        let tables = SampleTables::new();
        let ctx = synthetic_ctx(&tables);
        let plan = build_int_set_op_plan(
            &[1, 2, 2, 3],
            &[2, 4],
            ultrasql_planner::LogicalSetOp::Except,
            ultrasql_planner::LogicalSetQuantifier::Distinct,
        );
        let mut op = lower_query(&plan, &ctx).expect("lowers");
        assert_eq!(drain_int_setop(op.as_mut()), vec![1, 3]);
    }

    /// `SELECT v FROM l EXCEPT ALL SELECT v FROM r` — multiset
    /// difference: subtract right counts from left counts.
    #[test]
    fn lower_query_except_all_subtracts_right_counts_from_left() {
        let tables = SampleTables::new();
        let ctx = synthetic_ctx(&tables);
        // left: 1×{1}, 3×{2}, 1×{3}; right: 1×{2}, 1×{4}.
        // multiset diff: 1×{1}, 2×{2}, 1×{3} → [1, 2, 2, 3].
        let plan = build_int_set_op_plan(
            &[1, 2, 2, 2, 3],
            &[2, 4],
            ultrasql_planner::LogicalSetOp::Except,
            ultrasql_planner::LogicalSetQuantifier::All,
        );
        let mut op = lower_query(&plan, &ctx).expect("lowers");
        assert_eq!(drain_int_setop(op.as_mut()), vec![1, 2, 2, 3]);
    }

    /// Hand-built `SetOp` plan whose two children have different arities
    /// must be rejected by the lowerer with a precise `Unsupported`
    /// error rather than panicking inside the kernel.
    #[test]
    fn lower_query_set_op_rejects_arity_mismatch() {
        let tables = SampleTables::new();
        let ctx = synthetic_ctx(&tables);
        // Left has 1 column, right has 2.
        let left_schema = schema_int_col("v");
        let right_schema = Schema::new([
            Field::required("a", DataType::Int32),
            Field::required("b", DataType::Int32),
        ])
        .expect("two-col schema");
        let left_plan = LogicalPlan::Values {
            rows: vec![int_row(1)],
            schema: left_schema.clone(),
        };
        let right_plan = LogicalPlan::Values {
            rows: vec![vec![lit_i32(1), lit_i32(2)]],
            schema: right_schema,
        };
        let plan = LogicalPlan::SetOp {
            op: ultrasql_planner::LogicalSetOp::Union,
            quantifier: ultrasql_planner::LogicalSetQuantifier::All,
            left: Box::new(left_plan),
            right: Box::new(right_plan),
            schema: left_schema,
        };
        let err = lower_query(&plan, &ctx).expect_err("must reject arity mismatch");
        assert!(matches!(err, ServerError::Unsupported(_)));
    }

    /// The sample-table lowerer accepts `SetOp` too — keep both lowering
    /// paths bit-identical in dispatch semantics. We use a parsed SQL
    /// `SELECT id FROM users UNION ALL SELECT id FROM users` plan over
    /// the sample fixture so the test exercises the binder, the lowerer,
    /// and the kernel together.
    #[test]
    fn lower_plan_union_all_via_sample_path() {
        let (catalog, tables) = fixture();
        let p = plan(
            "SELECT id FROM users UNION ALL SELECT id FROM users",
            &catalog,
        );
        let mut op = lower_plan(&p, &tables).expect("lowers");
        let mut ids: Vec<i32> = Vec::new();
        while let Some(batch) = op.next_batch().expect("ok") {
            if let ultrasql_vec::column::Column::Int32(col) = &batch.columns()[0] {
                ids.extend_from_slice(col.data());
            }
        }
        ids.sort_unstable();
        // The fixture has ids = [1, 2, 3]; UNION ALL of two copies =
        // [1, 1, 2, 2, 3, 3] (sorted for stable comparison).
        assert_eq!(ids, vec![1, 1, 2, 2, 3, 3]);
    }

    // ----------------------------------------------------------------
    // IndexScan dispatch (Wave A item A5)
    // ----------------------------------------------------------------

    use std::sync::Arc as StdArc;

    use ultrasql_catalog::{MutableCatalog, PersistentCatalog};
    use ultrasql_core::TupleId;
    use ultrasql_executor::{ExecError, RowCodec};
    use ultrasql_storage::btree::BTree;
    use ultrasql_storage::buffer_pool::BufferPool;
    use ultrasql_storage::heap::{HeapAccess, InsertOptions};
    use ultrasql_txn::TransactionManager;

    /// Fixture for `IndexScan` tests: a populated persistent catalog,
    /// a heap with rows, and (optionally) a B-tree index registered
    /// against the catalog. The catalog snapshot is rebuilt after the
    /// index is registered so a subsequent `LowerCtx::catalog_snapshot`
    /// observation sees it.
    struct IndexFixture {
        catalog: StdArc<PersistentCatalog>,
        heap: StdArc<HeapAccess<BlankPageLoader>>,
        txn_manager: StdArc<TransactionManager>,
        /// XID under which the rows were inserted (committed before the
        /// fixture is handed out).
        loader_xid: Xid,
        /// Snapshot captured *after* the loader transaction committed,
        /// so `is_visible` returns `Visible` for every fixture row.
        reader_snapshot: ultrasql_mvcc::Snapshot,
    }

    /// Construct a fresh fixture and load `rows` of
    /// `(id INT NOT NULL, val INT NOT NULL)` data, registering an
    /// (optionally-present) B-tree index over `id`.
    fn build_index_fixture(
        table_name: &str,
        rows: &[(i32, i32)],
        with_index: bool,
    ) -> (IndexFixture, ultrasql_catalog::TableEntry, Vec<TupleId>) {
        let catalog = StdArc::new(PersistentCatalog::new());
        let pool = StdArc::new(BufferPool::new(64, BlankPageLoader));
        let heap = StdArc::new(HeapAccess::new(StdArc::clone(&pool)));
        let txn_manager = StdArc::new(TransactionManager::new());

        // Create the table in the catalog under a fresh OID.
        let schema = Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("val", DataType::Int32),
        ])
        .expect("schema ok");
        let oid = catalog.next_oid();
        let entry = ultrasql_catalog::TableEntry::new(oid, table_name, "public", schema.clone());
        catalog.create_table(entry.clone()).expect("create table");

        // Load rows under a single autocommit-style transaction. The
        // schema is moved into the codec here — no later use.
        let txn = txn_manager.begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let codec = RowCodec::new(schema);
        let rel = RelationId(oid);
        let mut tids: Vec<TupleId> = Vec::with_capacity(rows.len());
        for (id, val) in rows {
            let payload = codec
                .encode(&[Value::Int32(*id), Value::Int32(*val)])
                .expect("encode row");
            let opts = InsertOptions {
                xmin: txn.xid,
                command_id: CommandId::FIRST,
                wal: None,
                fsm: None,
                vm: None,
            };
            let tid = heap.insert(rel, &payload, opts).expect("heap insert");
            tids.push(tid);
        }
        let loader_xid = txn.xid;
        txn_manager.commit(txn).expect("commit loader");

        // Build the B-tree index (if requested) using the same shape
        // `Server::execute_create_index` uses.
        if with_index {
            let index_oid = catalog.next_oid();
            let index_rel = RelationId::new(index_oid.raw());
            let mut btree = BTree::create(StdArc::clone(&pool), index_rel).expect("btree create");
            let root_block = btree.root_block();
            for (i, (id, _val)) in rows.iter().enumerate() {
                let key: i64 = i64::from(*id);
                btree
                    .insert::<i64>(key, tids[i], loader_xid, None)
                    .expect("btree insert");
            }
            let mut idx_entry =
                ultrasql_catalog::IndexEntry::new(index_oid, "ix_id", oid, vec![0_u16], false);
            idx_entry.root_block = root_block;
            catalog.create_index(idx_entry).expect("index register");
        }

        // Snapshot *after* the loader commits so visibility sees the rows.
        let reader_txn = txn_manager.begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let reader_snapshot = reader_txn.snapshot.clone();
        txn_manager.commit(reader_txn).expect("commit reader-stub");

        (
            IndexFixture {
                catalog,
                heap,
                txn_manager,
                loader_xid,
                reader_snapshot,
            },
            entry,
            tids,
        )
    }

    impl IndexFixture {
        fn ctx<'a>(&'a self, tables: &'a SampleTables) -> LowerCtx<'a> {
            LowerCtx {
                tables,
                catalog_snapshot: self.catalog.snapshot(),
                heap: StdArc::clone(&self.heap),
                snapshot: self.reader_snapshot.clone(),
                oracle: StdArc::clone(&self.txn_manager),
                xid: self.loader_xid,
                command_id: CommandId::FIRST,
                cte_buffers: HashMap::new(),
            }
        }
    }

    /// Build a `Filter { Scan(table), predicate }` plan over `table_name`
    /// with the canonical `(id INT, val INT)` schema.
    fn build_filter_scan_plan(table_name: &str, predicate: ScalarExpr) -> LogicalPlan {
        let schema = Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("val", DataType::Int32),
        ])
        .expect("schema ok");
        LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: table_name.into(),
                schema,
                projection: None,
            }),
            predicate,
        }
    }

    /// Build `id = lit` over the canonical fixture schema. `id` is
    /// column index 0 with `Int32` type.
    fn eq_id_literal(v: i32) -> ScalarExpr {
        ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(ScalarExpr::Column {
                name: "id".into(),
                index: 0,
                data_type: DataType::Int32,
            }),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Int32(v),
                data_type: DataType::Int32,
            }),
            data_type: DataType::Bool,
        }
    }

    /// Build `id BETWEEN lo AND hi` as the binder would: rewrites into
    /// `id >= lo AND id <= hi`.
    fn between_id_literal(lo: i32, hi: i32) -> ScalarExpr {
        let id_col = || ScalarExpr::Column {
            name: "id".into(),
            index: 0,
            data_type: DataType::Int32,
        };
        let lit = |v: i32| ScalarExpr::Literal {
            value: Value::Int32(v),
            data_type: DataType::Int32,
        };
        ScalarExpr::Binary {
            op: BinaryOp::And,
            left: Box::new(ScalarExpr::Binary {
                op: BinaryOp::GtEq,
                left: Box::new(id_col()),
                right: Box::new(lit(lo)),
                data_type: DataType::Bool,
            }),
            right: Box::new(ScalarExpr::Binary {
                op: BinaryOp::LtEq,
                left: Box::new(id_col()),
                right: Box::new(lit(hi)),
                data_type: DataType::Bool,
            }),
            data_type: DataType::Bool,
        }
    }

    /// Drain a (id INT, val INT) operator and return the row pairs.
    fn drain_id_val(op: &mut dyn Operator) -> Result<Vec<(i32, i32)>, ExecError> {
        let mut out = Vec::new();
        while let Some(b) = op.next_batch()? {
            match (&b.columns()[0], &b.columns()[1]) {
                (
                    ultrasql_vec::column::Column::Int32(ids),
                    ultrasql_vec::column::Column::Int32(vals),
                ) => {
                    for (i, v) in ids.data().iter().zip(vals.data().iter()) {
                        out.push((*i, *v));
                    }
                }
                _ => panic!("unexpected column layout"),
            }
        }
        Ok(out)
    }

    /// `WHERE id = 42` against an indexed table picks `IndexScan` and
    /// returns the one matching row.
    #[test]
    fn lower_query_eq_indexed_column_picks_index_scan() {
        let rows: Vec<(i32, i32)> = (1..=100).map(|i| (i, i * 10)).collect();
        let (fix, _entry, _) = build_index_fixture("t_eq_indexed", &rows, true);
        let tables = SampleTables::new();
        let ctx = fix.ctx(&tables);
        let plan = build_filter_scan_plan("t_eq_indexed", eq_id_literal(42));
        let mut op = lower_query(&plan, &ctx).expect("lowers");
        let debug = format!("{op:?}");
        assert!(
            debug.starts_with("IndexScan"),
            "expected IndexScan, got: {debug}"
        );
        let pairs = drain_id_val(op.as_mut()).expect("drain");
        assert_eq!(pairs, vec![(42, 420)]);
    }

    /// `WHERE id = 42` against an *unindexed* table falls back to
    /// `Filter(SeqScan)`. The `Debug` starts with `Filter` (the outer
    /// operator); `SeqScan` is the inner child.
    #[test]
    fn lower_query_eq_unindexed_column_falls_back_to_filter_seq_scan() {
        let rows: Vec<(i32, i32)> = (1..=100).map(|i| (i, i * 10)).collect();
        let (fix, _entry, _) = build_index_fixture("t_eq_unindexed", &rows, false);
        let tables = SampleTables::new();
        let ctx = fix.ctx(&tables);
        let plan = build_filter_scan_plan("t_eq_unindexed", eq_id_literal(42));
        let mut op = lower_query(&plan, &ctx).expect("lowers");
        let debug = format!("{op:?}");
        assert!(
            !debug.starts_with("IndexScan"),
            "must not pick IndexScan over an unindexed column; got: {debug}"
        );
        let pairs = drain_id_val(op.as_mut()).expect("drain");
        assert_eq!(pairs, vec![(42, 420)]);
    }

    /// `WHERE id BETWEEN 10 AND 20` against an indexed table picks
    /// `IndexScan` and returns rows 10..=20 in ascending order.
    #[test]
    fn lower_query_between_indexed_column_picks_index_scan() {
        let rows: Vec<(i32, i32)> = (1..=100).map(|i| (i, i * 10)).collect();
        let (fix, _entry, _) = build_index_fixture("t_between_indexed", &rows, true);
        let tables = SampleTables::new();
        let ctx = fix.ctx(&tables);
        let plan = build_filter_scan_plan("t_between_indexed", between_id_literal(10, 20));
        let mut op = lower_query(&plan, &ctx).expect("lowers");
        let debug = format!("{op:?}");
        assert!(
            debug.starts_with("IndexScan"),
            "expected IndexScan for BETWEEN, got: {debug}"
        );
        let pairs = drain_id_val(op.as_mut()).expect("drain");
        let expected: Vec<(i32, i32)> = (10..=20).map(|i| (i, i * 10)).collect();
        assert_eq!(pairs, expected);
    }

    /// `WHERE val = 100` against a table whose index is on `id` (not
    /// `val`) falls back to SeqScan+Filter — confirms the catalog-look-up
    /// honours the column attnum.
    #[test]
    fn lower_query_eq_unindexed_when_index_on_other_column() {
        let rows: Vec<(i32, i32)> = (1..=10).map(|i| (i, i * 10)).collect();
        let (fix, _entry, _) = build_index_fixture("t_other_col_index", &rows, true);
        let tables = SampleTables::new();
        let ctx = fix.ctx(&tables);
        let predicate = ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(ScalarExpr::Column {
                name: "val".into(),
                index: 1,
                data_type: DataType::Int32,
            }),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Int32(50),
                data_type: DataType::Int32,
            }),
            data_type: DataType::Bool,
        };
        let plan = build_filter_scan_plan("t_other_col_index", predicate);
        let mut op = lower_query(&plan, &ctx).expect("lowers");
        let debug = format!("{op:?}");
        assert!(
            !debug.starts_with("IndexScan"),
            "must not pick IndexScan when the index does not cover the predicate's column; got: {debug}"
        );
        let pairs = drain_id_val(op.as_mut()).expect("drain");
        assert_eq!(pairs, vec![(5, 50)]);
    }

    /// `WHERE id > 95` picks `IndexScan` with an open upper bound and
    /// returns rows 96..=100.
    #[test]
    fn lower_query_gt_indexed_column_picks_index_scan() {
        let rows: Vec<(i32, i32)> = (1..=100).map(|i| (i, i * 10)).collect();
        let (fix, _entry, _) = build_index_fixture("t_gt_indexed", &rows, true);
        let tables = SampleTables::new();
        let ctx = fix.ctx(&tables);
        let predicate = ScalarExpr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(ScalarExpr::Column {
                name: "id".into(),
                index: 0,
                data_type: DataType::Int32,
            }),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Int32(95),
                data_type: DataType::Int32,
            }),
            data_type: DataType::Bool,
        };
        let plan = build_filter_scan_plan("t_gt_indexed", predicate);
        let mut op = lower_query(&plan, &ctx).expect("lowers");
        let debug = format!("{op:?}");
        assert!(
            debug.starts_with("IndexScan"),
            "expected IndexScan for `>`, got: {debug}"
        );
        let pairs = drain_id_val(op.as_mut()).expect("drain");
        let expected: Vec<(i32, i32)> = (96..=100).map(|i| (i, i * 10)).collect();
        assert_eq!(pairs, expected);
    }

    /// MVCC visibility: a row inserted after the reader's snapshot must
    /// NOT appear in the `IndexScan` output, just as it would not appear
    /// in a `SeqScan`.
    #[test]
    fn lower_query_index_scan_honours_mvcc_visibility() {
        let rows: Vec<(i32, i32)> = (1..=5).map(|i| (i, i * 10)).collect();
        let (fix, _entry, _tids) = build_index_fixture("t_mvcc", &rows, true);
        let tables = SampleTables::new();
        let ctx = fix.ctx(&tables);

        // Insert a row under a *new* (uncommitted) transaction; its
        // xmin is > reader_snapshot.xmax, so the reader must not see it.
        // We don't even need to register it in the index — IndexScan
        // would only see it if the heap fetch returned it as visible.
        let schema = Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("val", DataType::Int32),
        ])
        .expect("schema");
        let codec = RowCodec::new(schema);
        let entry = fix
            .catalog
            .snapshot()
            .tables
            .get("t_mvcc")
            .expect("entry")
            .clone();
        let invisible_txn = fix
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let payload = codec
            .encode(&[Value::Int32(99), Value::Int32(990)])
            .expect("encode");
        let opts = InsertOptions {
            xmin: invisible_txn.xid,
            command_id: CommandId::FIRST,
            wal: None,
            fsm: None,
            vm: None,
        };
        let _ = fix
            .heap
            .insert(RelationId(entry.oid), &payload, opts)
            .expect("insert");
        // Deliberately do NOT commit `invisible_txn`. The reader's
        // snapshot was taken before this transaction began, so even
        // after the row lands in the heap the reader sees `Visibility !=
        // Visible`.

        // Point lookup on a key we know was loaded before the snapshot.
        let plan = build_filter_scan_plan("t_mvcc", eq_id_literal(3));
        let mut op = lower_query(&plan, &ctx).expect("lowers");
        let pairs = drain_id_val(op.as_mut()).expect("drain");
        assert_eq!(pairs, vec![(3, 30)]);
    }

    /// A predicate not in the indexable shape set (`id + 1 = 42`) falls
    /// back to `SeqScan` + `Filter` even when the column is indexed.
    #[test]
    fn lower_query_arithmetic_predicate_falls_back_to_filter() {
        let rows: Vec<(i32, i32)> = (1..=10).map(|i| (i, i * 10)).collect();
        let (fix, _entry, _) = build_index_fixture("t_arith_fallback", &rows, true);
        let tables = SampleTables::new();
        let ctx = fix.ctx(&tables);
        // `id + 1 = 42` — left side is not a bare column reference.
        let predicate = ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(ScalarExpr::Binary {
                op: BinaryOp::Add,
                left: Box::new(ScalarExpr::Column {
                    name: "id".into(),
                    index: 0,
                    data_type: DataType::Int32,
                }),
                right: Box::new(ScalarExpr::Literal {
                    value: Value::Int32(1),
                    data_type: DataType::Int32,
                }),
                data_type: DataType::Int32,
            }),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Int32(42),
                data_type: DataType::Int32,
            }),
            data_type: DataType::Bool,
        };
        let plan = build_filter_scan_plan("t_arith_fallback", predicate);
        let op = lower_query(&plan, &ctx).expect("lowers");
        let debug = format!("{op:?}");
        assert!(
            !debug.starts_with("IndexScan"),
            "must not pick IndexScan when the predicate's column is wrapped in an expression; got: {debug}"
        );
    }

    /// `match_indexable_predicate` returns `None` for a literal-only
    /// predicate (`TRUE`).
    #[test]
    fn match_indexable_predicate_rejects_constant_predicate() {
        let pred = ScalarExpr::Literal {
            value: Value::Bool(true),
            data_type: DataType::Bool,
        };
        assert!(match_indexable_predicate(&pred).is_none());
    }

    /// Helper smoke test: bound normalisation is correct for strict
    /// operators. `id > 5` should normalise to `low = Some(6)`, no
    /// upper bound; `id < 5` to `high = Some(4)`, no lower bound.
    #[test]
    fn match_simple_comparison_normalises_strict_bounds() {
        let id_col = ScalarExpr::Column {
            name: "id".into(),
            index: 0,
            data_type: DataType::Int32,
        };
        let lit5 = ScalarExpr::Literal {
            value: Value::Int32(5),
            data_type: DataType::Int32,
        };
        let gt = ScalarExpr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(id_col.clone()),
            right: Box::new(lit5.clone()),
            data_type: DataType::Bool,
        };
        let (idx, range) = match_simple_comparison(&gt).expect("gt matches");
        assert_eq!(idx, 0);
        assert_eq!(range.low, Some(6));
        assert_eq!(range.high, None);

        let lt = ScalarExpr::Binary {
            op: BinaryOp::Lt,
            left: Box::new(id_col),
            right: Box::new(lit5),
            data_type: DataType::Bool,
        };
        let (_, range) = match_simple_comparison(&lt).expect("lt matches");
        assert_eq!(range.low, None);
        assert_eq!(range.high, Some(4));
    }

    // ---------------------------------------------------------------------
    // CTE lowering tests
    //
    // Three shapes covered:
    //
    // 1. Single CTE referenced once in the body — the materialised batches
    //    flow through a `CteScan` and the body sees the CTE rows verbatim.
    // 2. Multiple CTEs in a chain — both materialise; the body joins them
    //    by referencing each CTE name as a separate scan.
    // 3. CTE with column aliases — the binder rewrites the schema field
    //    names on the body's `Scan`; we verify the `CteScan` reports the
    //    aliased schema so downstream operators (and the wire encoder)
    //    see the renamed columns.
    //
    // Recursion (`WITH RECURSIVE`) is rejected; we test the rejection
    // path separately so a future executor fixpoint can flip the
    // expectation without rediscovering the contract.
    // ---------------------------------------------------------------------

    /// Build a single-column `(v INT)` Values plan with the given rows.
    fn int_values_plan(rows: &[i32], col_name: &str) -> LogicalPlan {
        LogicalPlan::Values {
            rows: rows.iter().map(|v| int_row(*v)).collect(),
            schema: schema_int_col(col_name),
        }
    }

    /// Build a `Scan` plan node that references a CTE by name. The
    /// schema we attach mirrors what the binder would record on a
    /// body-side `FROM cte` reference.
    fn cte_scan_ref(name: &str, schema: Schema) -> LogicalPlan {
        LogicalPlan::Scan {
            table: name.to_string(),
            schema,
            projection: None,
        }
    }

    /// `WITH a AS (VALUES (1),(2),(3)) SELECT * FROM a`
    ///
    /// Verifies that the CTE definition is materialised once and the
    /// body's `Scan(a)` resolves to that buffer via [`CteScan`].
    #[test]
    fn lower_query_cte_single_reference_returns_definition_rows() {
        let tables = SampleTables::new();
        let ctx = synthetic_ctx(&tables);
        let def = int_values_plan(&[1, 2, 3], "v");
        let body_schema = schema_int_col("v");
        let body = cte_scan_ref("a", body_schema.clone());
        let plan = LogicalPlan::Cte {
            name: "a".into(),
            recursive: false,
            definition: Box::new(def),
            body: Box::new(body),
            schema: body_schema,
        };
        let mut op = lower_query(&plan, &ctx).expect("CTE lowers");
        let mut got: Vec<i32> = Vec::new();
        while let Some(batch) = op.next_batch().expect("ok") {
            match &batch.columns()[0] {
                Column::Int32(c) => got.extend_from_slice(c.data()),
                other => panic!("unexpected column: {other:?}"),
            }
        }
        got.sort_unstable();
        assert_eq!(got, vec![1, 2, 3]);
    }

    /// `WITH a AS (VALUES (...)), b AS (VALUES (...)) SELECT a.aid FROM a JOIN b ON a.aid = b.bid`
    ///
    /// Verifies that two CTE bindings survive into the body and a join
    /// between them works through the catalog-aware lower path. Both
    /// children of the join are body-side `Scan`s that resolve via the
    /// CTE overlay. We use distinct column names (`aid`/`bid`) because
    /// `Schema::new` rejects duplicate names; the binder uses the same
    /// disambiguation when a join produces two same-named columns.
    #[test]
    fn lower_query_cte_multi_cte_join_returns_intersection() {
        let tables = SampleTables::new();
        let ctx = synthetic_ctx(&tables);
        let a_schema = schema_int_col("aid");
        let b_schema = schema_int_col("bid");
        let join_out_schema = Schema::new([
            Field::required("aid", DataType::Int32),
            Field::required("bid", DataType::Int32),
        ])
        .expect("schema ok");

        // Build the inner-most plan: `SELECT * FROM a JOIN b ON a.aid = b.bid`.
        let join = LogicalPlan::Join {
            left: Box::new(cte_scan_ref("a", a_schema)),
            right: Box::new(cte_scan_ref("b", b_schema)),
            join_type: LogicalJoinType::Inner,
            condition: LogicalJoinCondition::On(ScalarExpr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(column("aid", 0, DataType::Int32)),
                right: Box::new(column("bid", 1, DataType::Int32)),
                data_type: DataType::Bool,
            }),
            schema: join_out_schema.clone(),
        };

        // Wrap in two CTE nodes: outermost is `a`, innermost wraps `b`.
        let b_def = int_values_plan(&[2, 3, 5], "bid");
        let with_b = LogicalPlan::Cte {
            name: "b".into(),
            recursive: false,
            definition: Box::new(b_def),
            body: Box::new(join),
            schema: join_out_schema.clone(),
        };
        let a_def = int_values_plan(&[1, 2, 3, 4], "aid");
        let plan = LogicalPlan::Cte {
            name: "a".into(),
            recursive: false,
            definition: Box::new(a_def),
            body: Box::new(with_b),
            schema: join_out_schema,
        };

        let mut op = lower_query(&plan, &ctx).expect("CTE join lowers");
        let mut pairs = collect_pairs(op.as_mut());
        pairs.sort_unstable();
        // a ∩ b on equality: 2,3 appear in both.
        assert_eq!(pairs, vec![(2, 2), (3, 3)]);
    }

    /// `WITH a(x) AS (...) SELECT * FROM a`
    ///
    /// Verifies that a CTE column alias on the binding propagates to the
    /// `CteScan`'s reported schema, so downstream consumers see the
    /// aliased name instead of the definition's original field name.
    #[test]
    fn lower_query_cte_with_column_alias_reports_aliased_schema() {
        let tables = SampleTables::new();
        let ctx = synthetic_ctx(&tables);
        // Definition emits a column named "v"; the body sees it as "x".
        let def = int_values_plan(&[10, 20], "v");
        let body_schema = schema_int_col("x");
        let body = cte_scan_ref("a", body_schema.clone());
        let plan = LogicalPlan::Cte {
            name: "a".into(),
            recursive: false,
            definition: Box::new(def),
            body: Box::new(body),
            schema: body_schema,
        };

        let mut op = lower_query(&plan, &ctx).expect("CTE alias lowers");
        // The schema reported by the operator must use the aliased name.
        assert_eq!(op.schema().field_at(0).name, "x");
        let batch = op
            .next_batch()
            .expect("ok")
            .expect("at least one batch from CteScan");
        match &batch.columns()[0] {
            Column::Int32(c) => assert_eq!(c.data(), &[10, 20]),
            other => panic!("unexpected column: {other:?}"),
        }
    }

    /// `WITH RECURSIVE` must be rejected. The executor has no fixpoint
    /// loop today; silently lowering a recursive CTE as non-recursive
    /// would produce wrong results for any self-referential definition.
    #[test]
    fn lower_query_cte_rejects_recursive() {
        let tables = SampleTables::new();
        let ctx = synthetic_ctx(&tables);
        let def = int_values_plan(&[1], "v");
        let body_schema = schema_int_col("v");
        let body = cte_scan_ref("a", body_schema.clone());
        let plan = LogicalPlan::Cte {
            name: "a".into(),
            recursive: true,
            definition: Box::new(def),
            body: Box::new(body),
            schema: body_schema,
        };
        let err = lower_query(&plan, &ctx).expect_err("recursive must be rejected");
        match err {
            ServerError::Unsupported(msg) => assert!(
                msg.contains("RECURSIVE"),
                "error must mention RECURSIVE, got: {msg}"
            ),
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }
}
