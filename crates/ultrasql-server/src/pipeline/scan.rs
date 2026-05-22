//! Scan-lowering helpers and table-function scan.

use std::sync::Arc;

use serde_json::Value as JsonValue;
use ultrasql_catalog::TableEntry;
use ultrasql_core::{DataType, Field, RelationId, Schema, Value};
use ultrasql_executor::{
    CteScan, Eval, MemTableScan, Operator, ParallelSeqScan, Project, RowCodec, SeqScan,
    build_batch, choose_parallel_seq_scan_workers,
};
use ultrasql_planner::{LogicalPlan, ScalarExpr};

use crate::error::ServerError;

use super::LowerCtx;
use super::catalog_views::try_virtual_catalog_scan;
use super::csv_scan::{CsvPredicate, CsvSniffScan, CsvTableScan};
use super::external_scan::{
    is_external_table_function, lower_external_parquet_scan, lower_external_table_scan,
    read_csv_external_args, read_external_path_specs,
};
use super::json_table_scan::lower_json_table_scan;
use super::parquet_scan::ParquetPredicate;

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
    if let Some(scan) =
        super::time_partition::try_lower_time_partition_scan(&folded, projection, ctx)?
    {
        return Ok(scan);
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
    if is_external_table_function(name) {
        return lower_external_table_scan(name, args);
    }
    if name == "json_table" {
        return lower_json_table_scan(args);
    }
    if name == "json_each" {
        return lower_json_each(args);
    }
    if name == "jsonb_path_query" {
        return lower_jsonb_path_query(args);
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
            "table function (only generate_series, unnest, json_each, jsonb_path_query, json_table, read_csv, read_parquet, read_json, read_ndjson, read_arrow, read_iceberg, iceberg_scan, and sniff_csv supported)",
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

fn lower_json_each(args: &[ScalarExpr]) -> Result<Box<dyn Operator>, ServerError> {
    if args.len() != 1 {
        return Err(ServerError::Unsupported(
            "json_each: expected one json/jsonb argument",
        ));
    }
    let value = Eval::new(args[0].clone())
        .eval(&[])
        .map_err(|e| ServerError::Ddl(format!("json_each argument evaluation failed: {e}")))?;
    let document = match value {
        Value::Jsonb(text) | Value::Text(text) => serde_json::from_str::<JsonValue>(&text)
            .map_err(|err| ServerError::CopyFormat(format!("json_each parse jsonb: {err}")))?,
        Value::Null => JsonValue::Null,
        other => {
            return Err(ServerError::CopyFormat(format!(
                "json_each: argument must be jsonb or text, got {:?}",
                other.data_type()
            )));
        }
    };
    let schema = Schema::new([
        Field::required("key", DataType::Text { max_len: None }),
        Field::nullable("value", DataType::Jsonb),
    ])
    .map_err(|err| ServerError::CopyFormat(format!("json_each schema: {err}")))?;
    let rows = match document {
        JsonValue::Object(object) => object
            .into_iter()
            .map(|(key, value)| vec![Value::Text(key), Value::Jsonb(value.to_string())])
            .collect::<Vec<_>>(),
        JsonValue::Array(values) => values
            .into_iter()
            .enumerate()
            .map(|(idx, value)| {
                vec![
                    Value::Text(idx.to_string()),
                    Value::Jsonb(value.to_string()),
                ]
            })
            .collect::<Vec<_>>(),
        JsonValue::Null => Vec::new(),
        other => {
            return Err(ServerError::CopyFormat(format!(
                "json_each: expected object or array, got {other}"
            )));
        }
    };
    let batches = if rows.is_empty() {
        Vec::new()
    } else {
        vec![
            build_batch(&rows, &schema)
                .map_err(|err| ServerError::CopyFormat(format!("json_each batch: {err}")))?,
        ]
    };
    Ok(Box::new(MemTableScan::new(schema, batches)))
}

#[derive(Debug)]
enum BasicJsonPathStep {
    Key(String),
    All,
    Index(usize),
}

fn lower_jsonb_path_query(args: &[ScalarExpr]) -> Result<Box<dyn Operator>, ServerError> {
    if args.len() != 2 {
        return Err(ServerError::Unsupported(
            "jsonb_path_query: expected jsonb document and path",
        ));
    }
    let document_value = Eval::new(args[0].clone()).eval(&[]).map_err(|e| {
        ServerError::Ddl(format!("jsonb_path_query document evaluation failed: {e}"))
    })?;
    let document = match document_value {
        Value::Jsonb(text) | Value::Text(text) => serde_json::from_str::<JsonValue>(&text)
            .map_err(|err| {
                ServerError::CopyFormat(format!("jsonb_path_query parse jsonb: {err}"))
            })?,
        Value::Null => JsonValue::Null,
        other => {
            return Err(ServerError::CopyFormat(format!(
                "jsonb_path_query: document must be jsonb or text, got {:?}",
                other.data_type()
            )));
        }
    };
    let path_value = Eval::new(args[1].clone())
        .eval(&[])
        .map_err(|e| ServerError::Ddl(format!("jsonb_path_query path evaluation failed: {e}")))?;
    let Value::Text(path) = path_value else {
        return Err(ServerError::CopyFormat(format!(
            "jsonb_path_query: path must be text, got {:?}",
            path_value.data_type()
        )));
    };
    let steps = parse_basic_json_path(&path)?;
    let selected = select_basic_json_path(&document, &steps);
    let schema = Schema::new([Field::nullable("value", DataType::Jsonb)])
        .map_err(|err| ServerError::CopyFormat(format!("jsonb_path_query schema: {err}")))?;
    let rows = selected
        .into_iter()
        .map(|value| vec![Value::Jsonb(value.to_string())])
        .collect::<Vec<_>>();
    let batches = if rows.is_empty() {
        Vec::new()
    } else {
        vec![
            build_batch(&rows, &schema)
                .map_err(|err| ServerError::CopyFormat(format!("jsonb_path_query batch: {err}")))?,
        ]
    };
    Ok(Box::new(MemTableScan::new(schema, batches)))
}

fn parse_basic_json_path(path: &str) -> Result<Vec<BasicJsonPathStep>, ServerError> {
    let bytes = path.as_bytes();
    if bytes.first() != Some(&b'$') {
        return Err(ServerError::CopyFormat(format!(
            "jsonb_path_query path must start with $: {path}"
        )));
    }
    let mut steps = Vec::new();
    let mut idx = 1;
    while idx < bytes.len() {
        match bytes[idx] {
            b'.' => {
                idx += 1;
                let start = idx;
                while idx < bytes.len()
                    && (bytes[idx].is_ascii_alphanumeric() || bytes[idx] == b'_')
                {
                    idx += 1;
                }
                if start == idx {
                    return Err(ServerError::CopyFormat(format!(
                        "jsonb_path_query empty object key in path {path}"
                    )));
                }
                steps.push(BasicJsonPathStep::Key(path[start..idx].to_owned()));
            }
            b'[' => {
                idx += 1;
                if idx < bytes.len() && bytes[idx] == b'*' {
                    idx += 1;
                    if bytes.get(idx) != Some(&b']') {
                        return Err(ServerError::CopyFormat(format!(
                            "jsonb_path_query expected ] in path {path}"
                        )));
                    }
                    idx += 1;
                    steps.push(BasicJsonPathStep::All);
                } else {
                    let start = idx;
                    while idx < bytes.len() && bytes[idx].is_ascii_digit() {
                        idx += 1;
                    }
                    if bytes.get(idx) != Some(&b']') || start == idx {
                        return Err(ServerError::CopyFormat(format!(
                            "jsonb_path_query expected array index in path {path}"
                        )));
                    }
                    let index = path[start..idx].parse::<usize>().map_err(|err| {
                        ServerError::CopyFormat(format!("jsonb_path_query path index: {err}"))
                    })?;
                    idx += 1;
                    steps.push(BasicJsonPathStep::Index(index));
                }
            }
            _ => {
                return Err(ServerError::CopyFormat(format!(
                    "jsonb_path_query unsupported path syntax: {path}"
                )));
            }
        }
    }
    Ok(steps)
}

fn select_basic_json_path<'a>(
    root: &'a JsonValue,
    steps: &[BasicJsonPathStep],
) -> Vec<&'a JsonValue> {
    let mut current = vec![root];
    for step in steps {
        let mut next = Vec::new();
        for value in current {
            match (step, value) {
                (BasicJsonPathStep::Key(key), JsonValue::Object(object)) => {
                    if let Some(value) = object.get(key) {
                        next.push(value);
                    }
                }
                (BasicJsonPathStep::All, JsonValue::Array(values)) => next.extend(values),
                (BasicJsonPathStep::Index(index), JsonValue::Array(values)) => {
                    if let Some(value) = values.get(*index) {
                        next.push(value);
                    }
                }
                _ => {}
            }
        }
        current = next;
    }
    current
}

/// Lower `Project(read_csv(...))` with CSV projection pushdown when the
/// expressions are direct column references.
pub(super) fn try_lower_read_csv_project(
    input: &LogicalPlan,
    exprs: &[(ScalarExpr, String)],
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    let Some(projection) = projection_names(exprs) else {
        return Ok(None);
    };
    let LogicalPlan::FunctionScan { name, args, .. } = input else {
        return Ok(None);
    };
    if name != "read_csv" {
        return Ok(None);
    }
    let csv_args = read_csv_external_args(args)?;
    Ok(Some(Box::new(CsvTableScan::from_path_specs_with_options(
        &csv_args.path_specs,
        Some(&projection),
        None,
        csv_args.reject_path.as_deref(),
    )?)))
}

/// Lower `Filter(read_csv(...))` with CSV predicate pushdown when the
/// predicate is a simple `column OP literal` comparison.
pub(super) fn try_lower_read_csv_filter(
    input: &LogicalPlan,
    predicate: &ScalarExpr,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    let LogicalPlan::FunctionScan { name, args, .. } = input else {
        return Ok(None);
    };
    if name != "read_csv" {
        return Ok(None);
    }
    let Some(predicate) = CsvPredicate::from_scalar(predicate) else {
        return Ok(None);
    };
    let csv_args = read_csv_external_args(args)?;
    Ok(Some(Box::new(CsvTableScan::from_path_specs_with_options(
        &csv_args.path_specs,
        None,
        Some(&predicate),
        csv_args.reject_path.as_deref(),
    )?)))
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
            let path_specs = read_external_path_specs("read_parquet", args)?;
            Ok(Some(lower_external_parquet_scan(
                &path_specs,
                Some(&projection),
                None,
            )?))
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
            let path_specs = read_external_path_specs("read_parquet", args)?;
            Ok(Some(lower_external_parquet_scan(
                &path_specs,
                Some(&projection),
                Some(&predicate),
            )?))
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
    let path_specs = read_external_path_specs("read_parquet", args)?;
    Ok(Some(lower_external_parquet_scan(
        &path_specs,
        None,
        Some(&predicate),
    )?))
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

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::BinaryOp;
    use ultrasql_vec::column::Column;

    fn text_lit(value: String) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Text(value),
            data_type: DataType::Text { max_len: None },
        }
    }

    fn text_col(name: &str, index: usize) -> (ScalarExpr, String) {
        (
            ScalarExpr::Column {
                name: name.to_owned(),
                index,
                data_type: DataType::Text { max_len: None },
            },
            name.to_owned(),
        )
    }

    fn text_column_expr(name: &str, index: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.to_owned(),
            index,
            data_type: DataType::Text { max_len: None },
        }
    }

    fn binary_predicate(left: ScalarExpr, op: BinaryOp, right: ScalarExpr) -> ScalarExpr {
        ScalarExpr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
            data_type: DataType::Bool,
        }
    }

    fn read_csv_input(csv_path: &std::path::Path, header: &[&str]) -> LogicalPlan {
        let mut fields = header
            .iter()
            .map(|name| Field::nullable(*name, DataType::Text { max_len: None }))
            .collect::<Vec<_>>();
        fields.push(Field::nullable(
            "_filename",
            DataType::Text { max_len: None },
        ));
        fields.push(Field::required("_row_number", DataType::Int64));
        LogicalPlan::FunctionScan {
            name: "read_csv".to_owned(),
            args: vec![text_lit(csv_path.display().to_string())],
            schema: Schema::new(fields).expect("schema"),
        }
    }

    #[test]
    fn read_csv_project_pushdown_builds_only_wanted_columns() {
        let dir = tempfile::tempdir().expect("tempdir");
        let csv_path = dir.path().join("people.csv");
        fs::write(&csv_path, "id,name,unused\n1,Ada,skip\n").expect("write csv");
        let input_schema = Schema::new([
            Field::nullable("id", DataType::Text { max_len: None }),
            Field::nullable("name", DataType::Text { max_len: None }),
            Field::nullable("unused", DataType::Text { max_len: None }),
            Field::nullable("_filename", DataType::Text { max_len: None }),
            Field::required("_row_number", DataType::Int64),
        ])
        .expect("schema");
        let input = LogicalPlan::FunctionScan {
            name: "read_csv".to_owned(),
            args: vec![text_lit(csv_path.display().to_string())],
            schema: input_schema,
        };
        let exprs = vec![text_col("name", 1)];

        let mut scan = try_lower_read_csv_project(&input, &exprs)
            .expect("lower read_csv project")
            .expect("read_csv project pushdown");
        assert_eq!(scan.schema().len(), 1);
        assert_eq!(scan.schema().field_at(0).name, "name");

        let batch = scan
            .next_batch()
            .expect("read projected csv batch")
            .expect("projected csv batch");
        assert_eq!(batch.width(), 1);
        assert_eq!(batch.rows(), 1);
    }

    #[test]
    fn read_csv_project_pushdown_preserves_virtual_columns() {
        let dir = tempfile::tempdir().expect("tempdir");
        let csv_path = dir.path().join("people.csv");
        fs::write(&csv_path, "id,name\n1,Ada\n").expect("write csv");
        let input_schema = Schema::new([
            Field::nullable("id", DataType::Text { max_len: None }),
            Field::nullable("name", DataType::Text { max_len: None }),
            Field::nullable("_filename", DataType::Text { max_len: None }),
            Field::required("_row_number", DataType::Int64),
        ])
        .expect("schema");
        let input = LogicalPlan::FunctionScan {
            name: "read_csv".to_owned(),
            args: vec![text_lit(csv_path.display().to_string())],
            schema: input_schema,
        };
        let exprs = vec![
            text_col("_filename", 2),
            (
                ScalarExpr::Column {
                    name: "_row_number".to_owned(),
                    index: 3,
                    data_type: DataType::Int64,
                },
                "_row_number".to_owned(),
            ),
        ];

        let mut scan = try_lower_read_csv_project(&input, &exprs)
            .expect("lower read_csv project")
            .expect("read_csv project pushdown");
        assert_eq!(scan.schema().len(), 2);
        assert_eq!(scan.schema().field_at(0).name, "_filename");
        assert_eq!(scan.schema().field_at(1).name, "_row_number");

        let batch = scan
            .next_batch()
            .expect("read projected csv batch")
            .expect("projected csv batch");
        assert_eq!(batch.width(), 2);
        assert_eq!(batch.rows(), 1);
    }

    #[test]
    fn read_csv_filter_pushdown_applies_text_equality() {
        let dir = tempfile::tempdir().expect("tempdir");
        let csv_path = dir.path().join("people.csv");
        fs::write(&csv_path, "id,name\n1,Ada\n2,Bob\n3,Ada\n").expect("write csv");
        let input = read_csv_input(&csv_path, &["id", "name"]);
        let predicate = binary_predicate(
            text_column_expr("name", 1),
            BinaryOp::Eq,
            text_lit("Ada".to_owned()),
        );

        let mut scan = try_lower_read_csv_filter(&input, &predicate)
            .expect("lower read_csv filter")
            .expect("read_csv filter pushdown");
        let batch = scan
            .next_batch()
            .expect("read filtered csv batch")
            .expect("filtered csv batch");
        assert_eq!(batch.rows(), 2);
        let Column::Utf8(ids) = &batch.columns()[0] else {
            panic!("expected id text column");
        };
        assert_eq!(ids.value(0), "1");
        assert_eq!(ids.value(1), "3");
    }

    #[test]
    fn read_csv_filter_pushdown_applies_numeric_comparison() {
        let dir = tempfile::tempdir().expect("tempdir");
        let csv_path = dir.path().join("people.csv");
        fs::write(&csv_path, "id,name\n1,Ada\n2,Bob\n10,Cy\n").expect("write csv");
        let input = read_csv_input(&csv_path, &["id", "name"]);
        let predicate = binary_predicate(
            text_column_expr("id", 0),
            BinaryOp::Gt,
            ScalarExpr::Literal {
                value: Value::Int32(2),
                data_type: DataType::Int32,
            },
        );

        let mut scan = try_lower_read_csv_filter(&input, &predicate)
            .expect("lower read_csv filter")
            .expect("read_csv filter pushdown");
        let batch = scan
            .next_batch()
            .expect("read filtered csv batch")
            .expect("filtered csv batch");
        assert_eq!(batch.rows(), 1);
        let Column::Utf8(ids) = &batch.columns()[0] else {
            panic!("expected id text column");
        };
        assert_eq!(ids.value(0), "10");
    }

    #[test]
    fn read_csv_filter_pushdown_skips_text_ordering() {
        let dir = tempfile::tempdir().expect("tempdir");
        let csv_path = dir.path().join("people.csv");
        fs::write(&csv_path, "id,name\n1,Ada\n").expect("write csv");
        let input = read_csv_input(&csv_path, &["id", "name"]);
        let predicate = binary_predicate(
            text_column_expr("name", 1),
            BinaryOp::Gt,
            text_lit("Ada".to_owned()),
        );

        let scan = try_lower_read_csv_filter(&input, &predicate).expect("lower read_csv filter");
        assert!(scan.is_none());
    }
}
