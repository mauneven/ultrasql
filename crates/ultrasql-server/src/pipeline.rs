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

use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_executor::physical::{BuildError, DataSource};
use ultrasql_executor::{FilterEqI32, Limit, MemTableScan, Operator, Project};
use ultrasql_planner::{BinaryOp, InMemoryCatalog, LogicalPlan, ScalarExpr, TableMeta};
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn, StringColumn};

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
        LogicalPlan::Sort { .. } => Err(ServerError::Unsupported("ORDER BY")),
        LogicalPlan::Empty { .. } => Err(ServerError::Unsupported("SELECT without FROM")),
        LogicalPlan::Values { .. } => Err(ServerError::Unsupported("VALUES")),
        LogicalPlan::Insert { .. } => Err(ServerError::Unsupported("INSERT")),
        LogicalPlan::Update { .. } => Err(ServerError::Unsupported("UPDATE")),
        LogicalPlan::Delete { .. } => Err(ServerError::Unsupported("DELETE")),
        LogicalPlan::Truncate { .. } => Err(ServerError::Unsupported("TRUNCATE")),
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
    fn rejects_order_by() {
        let (catalog, tables) = fixture();
        let p = plan("SELECT id FROM users ORDER BY id", &catalog);
        let err = lower_plan(&p, &tables).expect_err("must reject");
        assert!(matches!(err, ServerError::Unsupported(_)));
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
}
