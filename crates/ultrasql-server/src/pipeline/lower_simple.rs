//! Simple per-variant lowerers used by the top-level [`super::lower_plan`]
//! dispatcher, plus the sample-database loader.

use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_executor::{
    FilterEqI32, Limit, MemTableScan, Operator, Project, ResultOp, SetOp, Sort,
};
use ultrasql_planner::{BinaryOp, InMemoryCatalog, LogicalPlan, ScalarExpr};
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn, StringColumn};

use crate::error::ServerError;

use super::SampleTables;
use super::cte_helpers::check_set_op_schemas;
use super::join::{LowerJoinArgs, lower_join};
use super::saturate_row_count;
use super::scan::lower_function_scan;

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
            lower_join(LowerJoinArgs {
                left_plan: left,
                right_plan: right,
                left: left_op,
                right: right_op,
                left_schema,
                right_schema,
                join_type: *join_type,
                condition,
                out_schema: schema.clone(),
                work_mem: None,
            })
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
        | LogicalPlan::CreateMaterializedView { .. }
        | LogicalPlan::CreateTypeEnum { .. }
        | LogicalPlan::CreateTypeComposite { .. }
        | LogicalPlan::CreateDomain { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::CreatePolicy { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTable { .. }
        | LogicalPlan::CreateSequence { .. }
        | LogicalPlan::AlterSequence { .. }
        | LogicalPlan::DropSequence { .. }
        | LogicalPlan::Comment { .. } => Err(ServerError::Unsupported(
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
        | LogicalPlan::SetTransaction { .. }
        | LogicalPlan::SetVariable { .. } => Err(ServerError::Unsupported(
            "session control reached operator lowerer; expected direct dispatch path",
        )),
        LogicalPlan::Listen { .. } | LogicalPlan::Notify { .. } | LogicalPlan::Unlisten { .. } => {
            Err(ServerError::Unsupported(
                "LISTEN/NOTIFY/UNLISTEN reached operator lowerer; expected pubsub dispatch path",
            ))
        }
        LogicalPlan::FunctionScan { name, args, .. } => lower_function_scan(name, args, None),
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
        LogicalPlan::Window { .. } => Err(ServerError::Unsupported(
            "window functions only supported on the catalog-aware path",
        )),
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
