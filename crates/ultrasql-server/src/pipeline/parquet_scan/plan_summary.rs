//! `EXPLAIN`-time summaries of Parquet row groups and columns a plan reads.

use std::path::Path;
use std::sync::Arc;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use ultrasql_objectstore::{ObjectLocation, expand_object_store_specs};
use ultrasql_planner::{LogicalPlan, ScalarExpr};

use crate::error::ServerError;

use super::ParquetRowGroupSummary;
use super::object_range::ObjectRangeChunkReader;
use super::paths::{expand_parquet_path_specs, path_specs_use_object_store};
use super::predicate::ParquetPredicate;
use super::pruning::row_group_summary_with_dictionary;
use super::schema::{read_arrow_schema, read_object_arrow_schema, resolve_projection_names};

/// Summarize Parquet row groups that a lowered plan shape will scan.
pub(crate) fn parquet_row_group_summary_for_plan(
    plan: &LogicalPlan,
) -> Result<Option<ParquetRowGroupSummary>, ServerError> {
    let mut summary = None;
    collect_parquet_row_group_summary(plan, &mut summary)?;
    Ok(summary)
}

/// Summarize physical Parquet columns read by a lowered plan shape.
pub(crate) fn parquet_columns_read_for_plan(
    plan: &LogicalPlan,
) -> Result<Option<Vec<String>>, ServerError> {
    let mut columns = None;
    collect_parquet_columns_read(plan, &mut columns)?;
    Ok(columns)
}

fn collect_parquet_row_group_summary(
    plan: &LogicalPlan,
    summary: &mut Option<ParquetRowGroupSummary>,
) -> Result<(), ServerError> {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            if let LogicalPlan::FunctionScan { name, args, .. } = input.as_ref()
                && name == "read_parquet"
            {
                let pushed = ParquetPredicate::from_scalar(predicate);
                add_parquet_function_summary(args, pushed.as_ref(), summary)?;
                return Ok(());
            }
            collect_parquet_row_group_summary(input, summary)
        }
        LogicalPlan::FunctionScan { name, args, .. } if name == "read_parquet" => {
            add_parquet_function_summary(args, None, summary)
        }
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::LockRows { input, .. }
        | LogicalPlan::Explain { input, .. }
        | LogicalPlan::Update { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Delete { input, .. } => collect_parquet_row_group_summary(input, summary),
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOp { left, right, .. } => {
            collect_parquet_row_group_summary(left, summary)?;
            collect_parquet_row_group_summary(right, summary)
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => {
            collect_parquet_row_group_summary(definition, summary)?;
            collect_parquet_row_group_summary(body, summary)
        }
        LogicalPlan::Insert { source, .. } => collect_parquet_row_group_summary(source, summary),
        _ => Ok(()),
    }
}

fn collect_parquet_columns_read(
    plan: &LogicalPlan,
    columns: &mut Option<Vec<String>>,
) -> Result<(), ServerError> {
    match plan {
        LogicalPlan::Project { input, exprs, .. } => {
            let projection = projection_names_from_exprs(exprs);
            match input.as_ref() {
                LogicalPlan::FunctionScan { name, args, .. } if name == "read_parquet" => {
                    add_parquet_columns_read(args, projection.as_deref(), None, columns)?;
                    Ok(())
                }
                LogicalPlan::Filter {
                    input, predicate, ..
                } => {
                    if let LogicalPlan::FunctionScan { name, args, .. } = input.as_ref()
                        && name == "read_parquet"
                    {
                        let pushed = ParquetPredicate::from_scalar(predicate);
                        add_parquet_columns_read(
                            args,
                            projection.as_deref(),
                            pushed.as_ref(),
                            columns,
                        )?;
                        return Ok(());
                    }
                    collect_parquet_columns_read(input, columns)
                }
                _ => collect_parquet_columns_read(input, columns),
            }
        }
        LogicalPlan::Filter { input, predicate } => {
            if let LogicalPlan::FunctionScan { name, args, .. } = input.as_ref()
                && name == "read_parquet"
            {
                let pushed = ParquetPredicate::from_scalar(predicate);
                add_parquet_columns_read(args, None, pushed.as_ref(), columns)?;
                return Ok(());
            }
            collect_parquet_columns_read(input, columns)
        }
        LogicalPlan::FunctionScan { name, args, .. } if name == "read_parquet" => {
            add_parquet_columns_read(args, None, None, columns)
        }
        LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::LockRows { input, .. }
        | LogicalPlan::Explain { input, .. }
        | LogicalPlan::Update { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Delete { input, .. } => collect_parquet_columns_read(input, columns),
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOp { left, right, .. } => {
            collect_parquet_columns_read(left, columns)?;
            collect_parquet_columns_read(right, columns)
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => {
            collect_parquet_columns_read(definition, columns)?;
            collect_parquet_columns_read(body, columns)
        }
        LogicalPlan::Insert { source, .. } => collect_parquet_columns_read(source, columns),
        _ => Ok(()),
    }
}

fn add_parquet_function_summary(
    args: &[ScalarExpr],
    predicate: Option<&ParquetPredicate>,
    summary: &mut Option<ParquetRowGroupSummary>,
) -> Result<(), ServerError> {
    let path_specs = super::super::external_scan::read_external_path_specs("read_parquet", args)?;
    let next = parquet_row_group_summary_for_path_specs(&path_specs, predicate)?;
    if let Some(summary) = summary {
        summary.add(next);
    } else {
        *summary = Some(next);
    }
    Ok(())
}

fn add_parquet_columns_read(
    args: &[ScalarExpr],
    projection: Option<&[String]>,
    predicate: Option<&ParquetPredicate>,
    columns: &mut Option<Vec<String>>,
) -> Result<(), ServerError> {
    let path_specs = super::super::external_scan::read_external_path_specs("read_parquet", args)?;
    let mut next = parquet_columns_read_for_path_specs(&path_specs, projection, predicate)?;
    if let Some(columns) = columns {
        columns.append(&mut next);
        columns.sort();
        columns.dedup();
    } else {
        next.sort();
        next.dedup();
        *columns = Some(next);
    }
    Ok(())
}

fn parquet_row_group_summary_for_path_specs(
    patterns: &[String],
    predicate: Option<&ParquetPredicate>,
) -> Result<ParquetRowGroupSummary, ServerError> {
    if path_specs_use_object_store("read_parquet", patterns)? {
        let objects = expand_object_store_specs(patterns)
            .map_err(|err| ServerError::CopyFormat(format!("read_parquet: {err}")))?;
        let mut summary = ParquetRowGroupSummary::default();
        for object in objects {
            summary.add(parquet_object_row_group_summary(&object, predicate)?);
        }
        return Ok(summary);
    }
    let mut summary = ParquetRowGroupSummary::default();
    for path in expand_parquet_path_specs(patterns)? {
        summary.add(parquet_path_row_group_summary(&path, predicate)?);
    }
    Ok(summary)
}

fn parquet_columns_read_for_path_specs(
    patterns: &[String],
    projection: Option<&[String]>,
    predicate: Option<&ParquetPredicate>,
) -> Result<Vec<String>, ServerError> {
    let schema = if path_specs_use_object_store("read_parquet", patterns)? {
        let objects = expand_object_store_specs(patterns)
            .map_err(|err| ServerError::CopyFormat(format!("read_parquet: {err}")))?;
        let Some(first) = objects.first() else {
            return Err(ServerError::CopyFormat(
                "read_parquet object expansion returned no files".to_owned(),
            ));
        };
        read_object_arrow_schema(first)?
    } else {
        let paths = expand_parquet_path_specs(patterns)?;
        let Some(first) = paths.first() else {
            return Err(ServerError::CopyFormat(
                "read_parquet path expansion returned no files".to_owned(),
            ));
        };
        read_arrow_schema(first)?
    };
    let mut columns = match projection {
        Some(projection) => {
            resolve_projection_names(schema.as_ref(), Some(projection))?.unwrap_or_default()
        }
        None => schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>(),
    };
    if let Some(predicate) = predicate {
        let predicate = predicate.resolved_for_schema(schema.as_ref())?;
        if !columns.iter().any(|column| column == &predicate.column) {
            columns.push(predicate.column);
        }
    }
    columns.sort();
    columns.dedup();
    Ok(columns)
}

fn projection_names_from_exprs(exprs: &[(ScalarExpr, String)]) -> Option<Vec<String>> {
    exprs
        .iter()
        .map(|(expr, alias)| match expr {
            ScalarExpr::Column { name, .. } if name == alias => Some(name.clone()),
            _ => None,
        })
        .collect()
}

pub(super) fn parquet_path_row_group_summary(
    path: &Path,
    predicate: Option<&ParquetPredicate>,
) -> Result<ParquetRowGroupSummary, ServerError> {
    let display = path.display().to_string();
    let file = super::scan::open_regular_parquet_file(path, &display, "open")?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|err| {
        ServerError::CopyFormat(format!(
            "read_parquet cannot inspect {}: {err}",
            path.display()
        ))
    })?;
    let predicate = predicate
        .map(|predicate| predicate.resolved_for_schema(builder.schema().as_ref()))
        .transpose()?;
    row_group_summary_with_dictionary(
        Arc::new(super::scan::open_regular_parquet_file(
            path,
            &display,
            "open for pruning",
        )?),
        builder.metadata(),
        builder.schema().as_ref(),
        predicate.as_ref(),
    )
}

fn parquet_object_row_group_summary(
    object: &ObjectLocation,
    predicate: Option<&ParquetPredicate>,
) -> Result<ParquetRowGroupSummary, ServerError> {
    let display = object.display_uri();
    let reader = ObjectRangeChunkReader::new(object.clone())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader.clone()).map_err(|err| {
        ServerError::CopyFormat(format!("read_parquet cannot inspect {display}: {err}"))
    })?;
    let predicate = predicate
        .map(|predicate| predicate.resolved_for_schema(builder.schema().as_ref()))
        .transpose()?;
    row_group_summary_with_dictionary(
        Arc::new(reader.clone()),
        builder.metadata(),
        builder.schema().as_ref(),
        predicate.as_ref(),
    )
}
