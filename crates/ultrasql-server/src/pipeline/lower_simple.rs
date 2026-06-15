//! Simple per-variant lowerers used by the top-level [`super::lower_plan`]
//! dispatcher, plus the sample-database loader.

use ultrasql_core::{DataType, Field, Schema};
use ultrasql_executor::{
    Filter, Limit, MemTableScan, Operator, Pivot, Project, ResultOp, SetOp, Sort, Unpivot,
};
use ultrasql_planner::{InMemoryCatalog, LogicalPlan, ScalarExpr};
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
        LogicalPlan::Scan { table, .. } => lower_scan(table, tables),
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
        LogicalPlan::Merge { .. } => Err(ServerError::Unsupported("MERGE")),
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
        | LogicalPlan::CreateOperator { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropIndex { .. }
        | LogicalPlan::CreatePolicy { .. }
        | LogicalPlan::CreateRole { .. }
        | LogicalPlan::AlterRole { .. }
        | LogicalPlan::DropRole { .. }
        | LogicalPlan::GrantPrivileges { .. }
        | LogicalPlan::RevokePrivileges { .. }
        | LogicalPlan::AlterDefaultPrivileges { .. }
        | LogicalPlan::GrantRole { .. }
        | LogicalPlan::RevokeRole { .. }
        | LogicalPlan::CreateSchema { .. }
        | LogicalPlan::DropSchema { .. }
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
        | LogicalPlan::SetVariable { .. }
        | LogicalPlan::Describe { .. }
        | LogicalPlan::Summarize { .. }
        | LogicalPlan::Checkpoint { .. }
        | LogicalPlan::SetRole { .. } => Err(ServerError::Unsupported(
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
        LogicalPlan::Pivot {
            input,
            group_columns,
            pivot_column,
            aggregate,
            pivot_values,
            schema,
        } => {
            let child = lower_plan(input, tables)?;
            Ok(Box::new(Pivot::try_new(
                child,
                group_columns.clone(),
                *pivot_column,
                aggregate.clone(),
                pivot_values.clone(),
                schema.clone(),
            )?))
        }
        LogicalPlan::Unpivot {
            input,
            passthrough_columns,
            columns,
            include_nulls,
            schema,
            ..
        } => {
            let child = lower_plan(input, tables)?;
            Ok(Box::new(Unpivot::new(
                child,
                passthrough_columns.clone(),
                columns.clone(),
                *include_nulls,
                schema.clone(),
            )))
        }
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

/// Build a [`MemTableScan`] for a registered table in its natural shape.
fn lower_scan(table: &str, tables: &SampleTables) -> Result<Box<dyn Operator>, ServerError> {
    let sample = tables.lookup(table).ok_or_else(|| {
        ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
            table.to_string(),
        ))
    })?;
    Ok(Box::new(MemTableScan::new(
        sample.schema.clone(),
        sample.batches.clone(),
    )))
}

/// Lower a `Filter` node.
///
/// The sample path uses the same general predicate operator as the heap-backed
/// path, so filtering preserves the child's full row shape and any parent
/// projection may still reference non-predicate columns.
fn lower_filter(
    input: &LogicalPlan,
    predicate: &ScalarExpr,
    tables: &SampleTables,
) -> Result<Box<dyn Operator>, ServerError> {
    let child = lower_plan(input, tables)?;
    Ok(Box::new(Filter::new(child, predicate.clone())))
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
    ]);
    let schema = match schema {
        Ok(schema) => schema,
        Err(err) => {
            tracing::error!(error = %err, "sample schema construction failed");
            return tables;
        }
    };

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
    ]);
    let batch = match batch {
        Ok(batch) => batch,
        Err(err) => {
            tracing::error!(error = %err, "sample batch construction failed");
            return tables;
        }
    };

    tables.register(catalog, "users", schema, vec![batch]);
    tables
}
