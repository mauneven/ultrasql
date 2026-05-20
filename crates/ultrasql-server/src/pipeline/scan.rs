//! Scan-lowering helpers and table-function scan.

use std::sync::Arc;

use ultrasql_catalog::TableEntry;
use ultrasql_core::{DataType, RelationId, Value};
use ultrasql_executor::{
    CteScan, Eval, MemTableScan, Operator, ParallelSeqScan, Project, RowCodec, SeqScan,
    choose_parallel_seq_scan_workers,
};
use ultrasql_iceberg::plan_iceberg_scan;
use ultrasql_planner::{LogicalPlan, ScalarExpr};

use crate::error::ServerError;

use super::LowerCtx;
use super::catalog_views::try_virtual_catalog_scan;
use super::csv_scan::{CsvSniffScan, CsvTableScan};
use super::parquet_scan::{ParquetPredicate, ParquetTableScan};

pub(super) fn lower_catalog_or_sample_scan(
    table: &str,
    projection: Option<&[usize]>,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    let folded = table.to_ascii_lowercase();
    if let Some(buffer) = ctx.cte_buffers.get(&folded) {
        let scan: Box<dyn Operator> = Box::new(CteScan::new(
            Arc::clone(&buffer.batches),
            buffer.schema.clone(),
        ));
        return apply_projection(scan, projection);
    }
    if let Some(scan) = try_virtual_catalog_scan(table, ctx)? {
        return apply_projection(scan, projection);
    }
    if let Some(entry) = ctx.catalog_snapshot.tables.get(&folded) {
        return lower_heap_scan(entry, projection, ctx);
    }
    // Legacy path: sample tables.
    let sample = ctx.tables.lookup(table).ok_or_else(|| {
        ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
            table.to_string(),
        ))
    })?;
    let scan: Box<dyn Operator> = Box::new(MemTableScan::new(
        sample.schema.clone(),
        sample.batches.clone(),
    ));
    apply_projection(scan, projection)
}

/// Construct a [`SeqScan`] for a real persistent relation.
pub(super) fn lower_heap_scan(
    entry: &TableEntry,
    projection: Option<&[usize]>,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    let rel = RelationId(entry.oid);
    // The catalog's `n_blocks` stat is an estimate; the heap's
    // counter is the truth. Take the larger of the two so a freshly
    // created table (entry.n_blocks = 0) still scans the blocks that
    // the heap has actually allocated through `insert`.
    let block_count = ctx.heap.block_count(rel).max(entry.n_blocks);
    let codec = RowCodec::new(entry.schema.clone());
    let workers = choose_parallel_seq_scan_workers(block_count, entry.schema.len());
    if workers > 1 && projection.is_none() {
        let scan = ParallelSeqScan::new_with_cancel(
            Arc::clone(&ctx.heap),
            rel,
            block_count,
            ctx.snapshot.clone(),
            Arc::clone(&ctx.oracle),
            Arc::clone(&ctx.vm),
            codec,
            ctx.cancel_flag.clone(),
            workers,
        );
        return Ok(Box::new(scan));
    }
    let mut scan = SeqScan::new_with_vm(
        Arc::clone(&ctx.heap),
        rel,
        block_count,
        ctx.snapshot.clone(),
        Arc::clone(&ctx.oracle),
        Arc::clone(&ctx.vm),
        codec,
    );
    // Thread the session's cancel flag through so `next_batch` returns
    // `ExecError::Cancelled` (→ SQLSTATE 57014) when a peer
    // `CancelRequest` flips it.
    if let Some(flag) = &ctx.cancel_flag {
        scan = scan.with_cancel_flag(flag.clone());
    }
    let scan: Box<dyn Operator> = Box::new(scan);
    apply_projection(scan, projection)
}

/// Construct a cache-building single-threaded [`SeqScan`] for a real
/// persistent relation.
///
/// Used by scalar aggregate miss paths where paying one serial scan to
/// populate the relation's `ColumnCache` is cheaper over repeated
/// executions than repeatedly choosing `ParallelSeqScan`, which does
/// not publish cache entries.
pub(super) fn lower_heap_seq_scan(
    entry: &TableEntry,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    let rel = RelationId(entry.oid);
    let block_count = ctx.heap.block_count(rel).max(entry.n_blocks);
    let codec = RowCodec::new(entry.schema.clone());
    let mut scan = SeqScan::new_with_vm(
        Arc::clone(&ctx.heap),
        rel,
        block_count,
        ctx.snapshot.clone(),
        Arc::clone(&ctx.oracle),
        Arc::clone(&ctx.vm),
        codec,
    );
    if let Some(flag) = &ctx.cancel_flag {
        scan = scan.with_cancel_flag(flag.clone());
    }
    Ok(Box::new(scan))
}

fn apply_projection(
    scan: Box<dyn Operator>,
    projection: Option<&[usize]>,
) -> Result<Box<dyn Operator>, ServerError> {
    if let Some(indices) = projection {
        Ok(Box::new(Project::new(scan, indices.to_vec())?))
    } else {
        Ok(scan)
    }
}

/// Lower a `LogicalPlan::FunctionScan { name, args, .. }` into the
/// matching set-returning-function operator.
pub(super) fn lower_function_scan(
    name: &str,
    args: &[ScalarExpr],
) -> Result<Box<dyn Operator>, ServerError> {
    if name == "sniff_csv" {
        if args.len() != 1 {
            return Err(ServerError::Unsupported(
                "sniff_csv: expected one path argument",
            ));
        }
        let value = Eval::new(args[0].clone())
            .eval(&[])
            .map_err(|e| ServerError::Ddl(format!("sniff_csv argument evaluation failed: {e}")))?;
        let Value::Text(path) = value else {
            return Err(ServerError::Unsupported(
                "sniff_csv: path argument must be a string literal",
            ));
        };
        return Ok(Box::new(CsvSniffScan::from_path(&path)?));
    }
    if name == "read_csv" {
        if args.len() != 1 {
            return Err(ServerError::Unsupported(
                "read_csv: expected one path, glob, or path-list argument",
            ));
        }
        let value = Eval::new(args[0].clone())
            .eval(&[])
            .map_err(|e| ServerError::Ddl(format!("read_csv argument evaluation failed: {e}")))?;
        let path_specs = read_csv_path_specs(&value)?;
        return Ok(Box::new(CsvTableScan::from_path_specs(&path_specs)?));
    }
    if name == "read_parquet" {
        if args.len() != 1 {
            return Err(ServerError::Unsupported(
                "read_parquet: expected one path, glob, or path-list argument",
            ));
        }
        let path_specs = read_parquet_path_specs(args)?;
        return Ok(Box::new(ParquetTableScan::from_path_specs(
            &path_specs,
            None,
            None,
        )?));
    }
    if name == "iceberg_scan" {
        if args.len() != 1 {
            return Err(ServerError::Unsupported(
                "iceberg_scan: expected one table root or metadata JSON path argument",
            ));
        }
        let path = read_iceberg_path_arg(args)?;
        let plan = plan_iceberg_scan(&path)
            .map_err(|err| ServerError::CopyFormat(format!("iceberg_scan: {err}")))?;
        if plan.data_files.is_empty() {
            return Ok(Box::new(MemTableScan::new(plan.schema, vec![])));
        }
        return Ok(Box::new(ParquetTableScan::from_path_specs(
            &plan.data_files,
            None,
            None,
        )?));
    }
    if name == "unnest" {
        if args.len() != 1 {
            return Err(ServerError::Unsupported(
                "unnest: expected one array argument",
            ));
        }
        let value = Eval::new(args[0].clone())
            .eval(&[])
            .map_err(|e| ServerError::Ddl(format!("unnest argument evaluation failed: {e}")))?;
        let Value::Array {
            element_type,
            elements,
        } = value
        else {
            return Err(ServerError::Unsupported("unnest: argument must be array"));
        };
        return Ok(Box::new(ultrasql_executor::FunctionScan::unnest(
            element_type,
            elements,
        )));
    }
    if name != "generate_series" {
        return Err(ServerError::Unsupported(
            "table function (only generate_series, unnest, read_csv, read_parquet, iceberg_scan, and sniff_csv supported)",
        ));
    }
    if args.len() < 2 || args.len() > 3 {
        return Err(ServerError::Unsupported(
            "generate_series: expected (start, stop) or (start, stop, step)",
        ));
    }
    fn to_i64(e: &ScalarExpr) -> Result<i64, ServerError> {
        match e {
            ScalarExpr::Literal {
                value: Value::Int64(v),
                ..
            } => Ok(*v),
            ScalarExpr::Literal {
                value: Value::Int32(v),
                ..
            } => Ok(i64::from(*v)),
            ScalarExpr::Unary {
                op: ultrasql_planner::UnaryOp::Neg,
                expr,
                ..
            } => to_i64(expr).map(i64::wrapping_neg),
            _ => Err(ServerError::Unsupported(
                "generate_series: arguments must be integer literals",
            )),
        }
    }
    let start = to_i64(&args[0])?;
    let stop = to_i64(&args[1])?;
    let step = if args.len() == 3 {
        to_i64(&args[2])?
    } else {
        1
    };
    Ok(Box::new(ultrasql_executor::FunctionScan::generate_series(
        start, stop, step,
    )))
}

/// Lower `Project(read_parquet(...))` and
/// `Project(Filter(read_parquet(...)))` with Parquet projection and
/// predicate pushdown when the expressions have a direct Parquet shape.
pub(super) fn try_lower_read_parquet_project(
    input: &LogicalPlan,
    exprs: &[(ScalarExpr, String)],
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    let Some(projection) = projection_names(exprs) else {
        return Ok(None);
    };
    match input {
        LogicalPlan::FunctionScan { name, args, .. } if name == "read_parquet" => {
            let path_specs = read_parquet_path_specs(args)?;
            Ok(Some(Box::new(ParquetTableScan::from_path_specs(
                &path_specs,
                Some(&projection),
                None,
            )?)))
        }
        LogicalPlan::Filter {
            input, predicate, ..
        } => {
            let LogicalPlan::FunctionScan { name, args, .. } = input.as_ref() else {
                return Ok(None);
            };
            if name != "read_parquet" {
                return Ok(None);
            }
            let Some(predicate) = ParquetPredicate::from_scalar(predicate) else {
                return Ok(None);
            };
            let path_specs = read_parquet_path_specs(args)?;
            Ok(Some(Box::new(ParquetTableScan::from_path_specs(
                &path_specs,
                Some(&projection),
                Some(&predicate),
            )?)))
        }
        _ => Ok(None),
    }
}

/// Lower `Filter(read_parquet(...))` with Parquet predicate pushdown
/// when the predicate is a simple `column OP literal` comparison.
pub(super) fn try_lower_read_parquet_filter(
    input: &LogicalPlan,
    predicate: &ScalarExpr,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    let LogicalPlan::FunctionScan { name, args, .. } = input else {
        return Ok(None);
    };
    if name != "read_parquet" {
        return Ok(None);
    }
    let Some(predicate) = ParquetPredicate::from_scalar(predicate) else {
        return Ok(None);
    };
    let path_specs = read_parquet_path_specs(args)?;
    Ok(Some(Box::new(ParquetTableScan::from_path_specs(
        &path_specs,
        None,
        Some(&predicate),
    )?)))
}

fn projection_names(exprs: &[(ScalarExpr, String)]) -> Option<Vec<String>> {
    exprs
        .iter()
        .map(|(expr, alias)| match expr {
            ScalarExpr::Column { name, .. } if name == alias => Some(name.clone()),
            _ => None,
        })
        .collect()
}

fn read_parquet_path_specs(args: &[ScalarExpr]) -> Result<Vec<String>, ServerError> {
    if args.len() != 1 {
        return Err(ServerError::Unsupported(
            "read_parquet: expected one path, glob, or path-list argument",
        ));
    }
    let value = Eval::new(args[0].clone())
        .eval(&[])
        .map_err(|e| ServerError::Ddl(format!("read_parquet argument evaluation failed: {e}")))?;
    match value {
        Value::Text(pattern) => Ok(vec![pattern]),
        Value::Array {
            element_type: DataType::Text { max_len: None },
            elements,
        } => elements
            .into_iter()
            .map(|element| match element {
                Value::Text(path) => Ok(path),
                _ => Err(ServerError::Unsupported(
                    "read_parquet: path-list elements must be string literals",
                )),
            })
            .collect(),
        _ => Err(ServerError::Unsupported(
            "read_parquet: argument must be a string literal or text array literal",
        )),
    }
}

fn read_iceberg_path_arg(args: &[ScalarExpr]) -> Result<String, ServerError> {
    let value = Eval::new(args[0].clone())
        .eval(&[])
        .map_err(|e| ServerError::Ddl(format!("iceberg_scan argument evaluation failed: {e}")))?;
    match value {
        Value::Text(path) => Ok(path),
        _ => Err(ServerError::Unsupported(
            "iceberg_scan: argument must be a string literal",
        )),
    }
}

fn read_csv_path_specs(value: &Value) -> Result<Vec<String>, ServerError> {
    match value {
        Value::Text(pattern) => Ok(vec![pattern.clone()]),
        Value::Array {
            element_type,
            elements,
        } if matches!(element_type, &DataType::Text { max_len: None }) => elements
            .iter()
            .map(|element| match element {
                Value::Text(path) => Ok(path.clone()),
                _ => Err(ServerError::Unsupported(
                    "read_csv: path-list elements must be string literals",
                )),
            })
            .collect(),
        _ => Err(ServerError::Unsupported(
            "read_csv: argument must be a string literal or text array literal",
        )),
    }
}
