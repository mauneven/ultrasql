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
//! - [`LogicalPlan::Limit`] (without offset) -> [`Limit`].
//! - [`LogicalPlan::Sort`] -> [`Sort`] (in-memory; spill-to-disk lands
//!   with the `work_mem` budget in v0.6).
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

use ultrasql_catalog::{CatalogSnapshot, TableEntry};
use ultrasql_core::{CommandId, DataType, Field, RelationId, Schema, Value, Xid};
use ultrasql_executor::physical::{BuildError, DataSource};
use ultrasql_executor::{
    Filter, FilterEqI32, HashAggregate, HashJoin, Limit, MemTableScan, ModifyKind, ModifyTable,
    NestedLoopJoin, Operator, Project, RightFactory, RowCodec, SeqScan, Sort, ValuesScan,
};
use ultrasql_mvcc::Snapshot;
use ultrasql_planner::{
    BinaryOp, InMemoryCatalog, LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr,
    TableMeta,
};
use ultrasql_storage::heap::HeapAccess;
use ultrasql_txn::TransactionManager;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn, StringColumn};

use crate::BlankPageLoader;
use crate::error::ServerError;

/// Maximum LIMIT a v0.5 query may request. `Limit::new` takes a
/// `usize`, so we clamp `u64` plan values to a generous ceiling.
const MAX_LIMIT: u64 = 1 << 32;

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
        LogicalPlan::Project { input, exprs, .. } => lower_project(input, exprs, tables),
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
        LogicalPlan::Aggregate { .. } => Err(ServerError::Unsupported("GROUP BY / aggregate")),
        LogicalPlan::SetOp { .. } => Err(ServerError::Unsupported("UNION / INTERSECT / EXCEPT")),
        LogicalPlan::Cte { .. } => Err(ServerError::Unsupported("WITH (CTE)")),
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
    if offset != 0 {
        return Err(ServerError::Unsupported("LIMIT with OFFSET"));
    }
    if n > MAX_LIMIT {
        return Err(ServerError::Unsupported("LIMIT exceeds server cap"));
    }
    let child = lower_plan(input, tables)?;
    // Clamp into usize. We just verified `n <= MAX_LIMIT < usize::MAX`
    // on any 64-bit target, so this conversion never truncates.
    let n = usize::try_from(n).unwrap_or(usize::MAX);
    Ok(Box::new(Limit::new(child, n)))
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
/// - Everything else is rejected — JOIN, GROUP BY, set ops, CTEs land
///   in subsequent waves.
pub fn lower_query(
    plan: &LogicalPlan,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    match plan {
        LogicalPlan::Scan { table, .. } => lower_catalog_or_sample_scan(table, ctx),
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
        LogicalPlan::Project { input, exprs, .. } => {
            let child = lower_query(input, ctx)?;
            lower_project_columns(child, exprs)
        }
        LogicalPlan::Filter { input, predicate } => {
            let child = lower_query(input, ctx)?;
            Ok(Box::new(Filter::new(child, predicate.clone())))
        }
        LogicalPlan::Limit { input, n, offset } => {
            if *offset != 0 {
                return Err(ServerError::Unsupported("LIMIT with OFFSET"));
            }
            if *n > MAX_LIMIT {
                return Err(ServerError::Unsupported("LIMIT exceeds server cap"));
            }
            let child = lower_query(input, ctx)?;
            let n = usize::try_from(*n).unwrap_or(usize::MAX);
            Ok(Box::new(Limit::new(child, n)))
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
        LogicalPlan::Truncate { .. } => Err(ServerError::Unsupported("TRUNCATE")),
        LogicalPlan::CreateTable { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTable { .. } => Err(ServerError::Unsupported(
            "DDL reached operator lowerer; expected DDL dispatch path",
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
        LogicalPlan::SetOp { .. } => Err(ServerError::Unsupported("UNION / INTERSECT / EXCEPT")),
        LogicalPlan::Cte { .. } => Err(ServerError::Unsupported("WITH (CTE)")),
    }
}

/// Lower a `Scan` node by trying the persistent catalog first; if the
/// name is not registered there, falls back to the v0.5
/// sample-table registry.
fn lower_catalog_or_sample_scan(
    table: &str,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    let folded = table.to_ascii_lowercase();
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
    let LogicalPlan::Values { rows, schema } = source else {
        return Err(ServerError::Unsupported(
            "INSERT source other than VALUES (e.g. INSERT INTO t SELECT)",
        ));
    };
    let child: Box<dyn Operator> = Box::new(ValuesScan::new(rows.clone(), schema.clone()));
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
}
