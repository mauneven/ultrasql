//! FROM clause and JOIN binding. Split out of `binder/mod.rs` to keep each
//! file under the 600-line ceiling.

use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};
use std::io::{self, Cursor, Read};
use std::path::{Path, PathBuf};

use arrow_ipc::reader::FileReader as ArrowFileReader;
use arrow_schema::DataType as ArrowDataType;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::reader::{ChunkReader, Length};
use serde_json::{Map as JsonMap, Value as JsonValue};
use ultrasql_core::{
    DataType, Field, Schema, Value,
    csv::{CsvParseOptions, parse_csv_records_with_options, read_csv_header_from_specs},
};
use ultrasql_iceberg::read_iceberg_schema;
use ultrasql_objectstore::{
    ObjectLocation, expand_object_store_specs, is_object_store_uri, read_first_object_bytes,
    read_object_range, read_object_range_with_metadata,
};
use ultrasql_parser::ast::{
    Identifier, JoinCondition, JoinOp, JsonTableColumnKind, PivotAggregate, PivotValue, TableRef,
    TypeName, UnpivotColumn, XmlTableColumnKind,
};

const READ_CSV_HEADER_SAMPLE_BYTES: u64 = 64 * 1024;
const JSON_STREAM_CHUNK_BYTES: u64 = 64 * 1024;
const PLANNER_JSON_RECORD_LIMIT_BYTES: usize = 16 * 1024 * 1024;
const MAX_JOIN_DEPTH: usize = 64;

use super::aggregate::{aggregate_return_type, classify_aggregate};
use super::ddl::resolve_type_name;
use super::expr_bind::coerce_literal_to_type;
use super::expr_type::comparable;
use super::{
    AggregateFunc, Catalog, LogicalJoinCondition, LogicalJoinType, LogicalPivotAggregate,
    LogicalPivotValue, LogicalPlan, LogicalUnpivotColumn, PlanError, ScalarExpr, ScopeEntry,
    ScopeStack, apply_column_aliases, bind_expr_with_ctes, bind_select_with_ctes,
    lookup_table_reference, schema_for_qualified_binding,
};

pub(super) fn bind_from(
    from_items: &[TableRef],
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    outer_scope: &mut ScopeStack,
) -> Result<(LogicalPlan, Vec<ScopeEntry>), PlanError> {
    let join_depth = from_clause_join_depth(from_items);
    if join_depth > MAX_JOIN_DEPTH {
        return Err(PlanError::not_supported(format!(
            "join depth {join_depth} exceeds planner limit {MAX_JOIN_DEPTH}"
        )));
    }

    if from_items.is_empty() {
        return Ok((
            LogicalPlan::Empty {
                schema: Schema::empty(),
            },
            vec![],
        ));
    }

    let Some(first) = from_items.first() else {
        return Ok((
            LogicalPlan::Empty {
                schema: Schema::empty(),
            },
            vec![],
        ));
    };
    let iter = from_items.iter().skip(1);
    let (mut plan, mut from_scope) = bind_table_ref(first, catalog, cte_catalog, outer_scope)?;

    for item in iter {
        let (right_plan, right_scope) = bind_table_ref(item, catalog, cte_catalog, outer_scope)?;
        let offset = from_scope.len();
        let join_schema = concat_schemas_cross(plan.schema(), right_plan.schema())?;
        let merged_scope = merge_scopes(from_scope, right_scope, offset);
        plan = LogicalPlan::Join {
            left: Box::new(plan),
            right: Box::new(right_plan),
            join_type: LogicalJoinType::Cross,
            condition: LogicalJoinCondition::None,
            schema: join_schema,
        };
        from_scope = merged_scope;
    }

    Ok((plan, from_scope))
}

fn from_clause_join_depth(from_items: &[TableRef]) -> usize {
    let mut items = from_items.iter();
    let Some(first) = items.next() else {
        return 0;
    };

    let mut depth = table_ref_join_depth(first);
    for item in items {
        depth = depth.max(table_ref_join_depth(item)).saturating_add(1);
    }
    depth
}

fn table_ref_join_depth(table_ref: &TableRef) -> usize {
    match table_ref {
        TableRef::Join { left, right, .. } => table_ref_join_depth(left)
            .max(table_ref_join_depth(right))
            .saturating_add(1),
        TableRef::Named { .. }
        | TableRef::Subquery { .. }
        | TableRef::Function { .. }
        | TableRef::JsonTable { .. }
        | TableRef::Pivot { .. }
        | TableRef::Unpivot { .. }
        | TableRef::XmlTable { .. } => 0,
    }
}

fn bind_table_ref(
    table_ref: &TableRef,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<(LogicalPlan, Vec<ScopeEntry>), PlanError> {
    match table_ref {
        TableRef::Named { name, alias, .. } => {
            let raw_table_name = name
                .parts
                .last()
                .map_or_else(String::new, |p| p.value.to_ascii_lowercase());
            let system_table_name = qualified_system_name(name);
            let mut table_name = system_table_name
                .clone()
                .unwrap_or_else(|| raw_table_name.clone());
            let qualifier = alias
                .as_ref()
                .map_or_else(|| raw_table_name.clone(), |a| a.value.clone());

            let schema = if let Some((_, s)) = cte_catalog
                .iter()
                .rev()
                .find(|(n, _)| n.eq_ignore_ascii_case(&table_name))
            {
                s.clone()
            } else if system_table_name.is_none() {
                let resolved = lookup_table_reference(catalog, name)?;
                table_name = resolved.plan_name;
                resolved.meta.schema
            } else {
                let meta = catalog
                    .lookup_table(&table_name)
                    .ok_or_else(|| PlanError::TableNotFound(table_name.clone()))?;
                meta.schema
            };

            let from_scope: Vec<ScopeEntry> = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| ScopeEntry {
                    qualifier: qualifier.clone(),
                    field_index: i,
                    field: f.clone(),
                })
                .collect();
            let plan = LogicalPlan::Scan {
                table: table_name,
                schema,
                projection: None,
            };
            Ok((plan, from_scope))
        }
        TableRef::Subquery {
            select,
            alias,
            column_aliases,
            ..
        } => {
            let inner_plan = bind_select_with_ctes(select, catalog, cte_catalog, scope)?;
            let inner_schema = inner_plan.schema().clone();
            let inner_schema = if column_aliases.is_empty() {
                inner_schema
            } else {
                apply_column_aliases(&inner_schema, column_aliases)?
            };
            let qualifier = alias.value.clone();
            let from_scope: Vec<ScopeEntry> = inner_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| ScopeEntry {
                    qualifier: qualifier.clone(),
                    field_index: i,
                    field: f.clone(),
                })
                .collect();
            let plan = rebuild_subquery_plan(inner_plan, &inner_schema)?;
            Ok((plan, from_scope))
        }
        TableRef::Join {
            left,
            op,
            right,
            condition,
            ..
        } => bind_explicit_join(left, *op, right, condition, catalog, cte_catalog, scope),
        TableRef::Function {
            name, args, alias, ..
        } => bind_table_function(name, args, alias.as_ref(), catalog, cte_catalog, scope),
        TableRef::JsonTable {
            context,
            row_path,
            columns,
            alias,
            ..
        } => bind_json_table_ref(
            context,
            row_path,
            columns,
            alias.as_ref(),
            catalog,
            cte_catalog,
            scope,
        ),
        TableRef::Pivot {
            input,
            aggregate,
            value_column,
            pivot_values,
            ..
        } => bind_pivot_ref(
            input,
            aggregate,
            value_column,
            pivot_values,
            catalog,
            cte_catalog,
            scope,
        ),
        TableRef::Unpivot {
            input,
            value_column,
            name_column,
            columns,
            include_nulls,
            ..
        } => bind_unpivot_ref(
            UnpivotRefSpec {
                input,
                value_column,
                name_column,
                columns,
                include_nulls: *include_nulls,
            },
            catalog,
            cte_catalog,
            scope,
        ),
        TableRef::XmlTable {
            context,
            row_path,
            columns,
            alias,
            ..
        } => bind_xml_table_ref(
            context,
            row_path,
            columns,
            alias.as_ref(),
            catalog,
            cte_catalog,
            scope,
        ),
    }
}

fn bind_pivot_ref(
    input: &TableRef,
    aggregate: &PivotAggregate,
    value_column: &Identifier,
    pivot_values: &[PivotValue],
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<(LogicalPlan, Vec<ScopeEntry>), PlanError> {
    if pivot_values.is_empty() {
        return Err(PlanError::TypeMismatch(
            "PIVOT requires at least one IN value".to_owned(),
        ));
    }

    let (input_plan, input_scope) = bind_table_ref(input, catalog, cte_catalog, scope)?;
    let input_schema = input_plan.schema().clone();
    let pivot_column = resolve_schema_column(&input_schema, &value_column.value)?;
    let pivot_type = input_schema.field_at(pivot_column).data_type.clone();
    let binding_schema = schema_for_qualified_binding(&input_schema, &input_scope)?;

    let func_name = aggregate.function.value.to_ascii_lowercase();
    let func = classify_aggregate(&func_name, aggregate.arg.is_none()).ok_or({
        PlanError::NotSupported("PIVOT supports COUNT, SUM, AVG, MIN, and MAX aggregates")
    })?;
    if !matches!(
        func,
        AggregateFunc::CountStar
            | AggregateFunc::Count
            | AggregateFunc::Sum
            | AggregateFunc::Avg
            | AggregateFunc::Min
            | AggregateFunc::Max
    ) {
        return Err(PlanError::NotSupported(
            "PIVOT supports COUNT, SUM, AVG, MIN, and MAX aggregates",
        ));
    }
    if aggregate.arg.is_none() && func != AggregateFunc::CountStar {
        return Err(PlanError::TypeMismatch(format!(
            "PIVOT {func_name}(*) is invalid; only COUNT(*) accepts *"
        )));
    }

    let (arg, arg_column, arg_type) = if let Some(arg_ast) = &aggregate.arg {
        let bound = bind_expr_with_ctes(arg_ast, &binding_schema, catalog, cte_catalog, scope)?;
        if let ScalarExpr::Column {
            index, data_type, ..
        } = &bound
        {
            let arg_column = *index;
            let arg_type = data_type.clone();
            (Some(bound), Some(arg_column), arg_type)
        } else {
            return Err(PlanError::NotSupported(
                "PIVOT aggregate argument must be a source column or *",
            ));
        }
    } else {
        (None, None, DataType::Null)
    };
    validate_pivot_aggregate_arg(func, &arg_type)?;
    let aggregate_data_type = aggregate_return_type(func, arg_type);
    if matches!(aggregate_data_type, DataType::Null) && func != AggregateFunc::CountStar {
        return Err(PlanError::TypeMismatch(format!(
            "PIVOT aggregate {func_name} cannot infer an output type"
        )));
    }

    let mut group_columns = Vec::new();
    for idx in 0..input_schema.len() {
        if idx != pivot_column && Some(idx) != arg_column {
            group_columns.push(idx);
        }
    }

    let mut output_fields = Vec::with_capacity(group_columns.len() + pivot_values.len());
    for idx in &group_columns {
        output_fields.push(input_schema.field_at(*idx).clone());
    }

    let mut seen_outputs = HashSet::new();
    for field in &output_fields {
        seen_outputs.insert(field.name.to_ascii_lowercase());
    }

    let mut bound_values = Vec::with_capacity(pivot_values.len());
    let mut seen_pivot_values = HashSet::new();
    let empty_schema = Schema::empty();
    for pivot_value in pivot_values {
        let mut bound = bind_expr_with_ctes(
            &pivot_value.value,
            &empty_schema,
            catalog,
            cte_catalog,
            scope,
        )?;
        coerce_literal_to_type(&mut bound, &pivot_type);
        let ScalarExpr::Literal { value, data_type } = bound else {
            return Err(PlanError::NotSupported(
                "PIVOT IN values must be literal constants",
            ));
        };
        if matches!(value, Value::Null) {
            return Err(PlanError::TypeMismatch(
                "PIVOT IN values cannot be NULL".to_owned(),
            ));
        }
        if !comparable(&pivot_type, &data_type) {
            return Err(PlanError::TypeMismatch(format!(
                "PIVOT value type {data_type} is not comparable with pivot column {}",
                pivot_type
            )));
        }
        if data_type != pivot_type {
            return Err(PlanError::TypeMismatch(format!(
                "PIVOT value type {data_type} cannot be coerced to pivot column {}",
                pivot_type
            )));
        }
        if !seen_pivot_values.insert(value.clone()) {
            return Err(PlanError::TypeMismatch(format!(
                "duplicate PIVOT value {value}"
            )));
        }
        let output_name = pivot_value
            .alias
            .as_ref()
            .map_or_else(|| pivot_output_name(&value), |alias| alias.value.clone());
        let output_key = output_name.to_ascii_lowercase();
        if !seen_outputs.insert(output_key) {
            return Err(PlanError::TypeMismatch(format!(
                "duplicate PIVOT output column '{output_name}'"
            )));
        }
        output_fields.push(Field::nullable(
            output_name.clone(),
            aggregate_data_type.clone(),
        ));
        bound_values.push(LogicalPivotValue {
            value,
            data_type,
            output_name,
        });
    }

    let schema = Schema::new(output_fields)
        .map_err(|err| PlanError::TypeMismatch(format!("PIVOT output schema: {err}")))?;
    let qualifier = transform_qualifier(&input_scope, "pivot");
    let from_scope = scope_entries_for_schema(&qualifier, &schema);
    let plan = LogicalPlan::Pivot {
        input: Box::new(input_plan),
        group_columns,
        pivot_column,
        aggregate: LogicalPivotAggregate {
            func,
            arg,
            data_type: aggregate_data_type,
        },
        pivot_values: bound_values,
        schema,
    };
    Ok((plan, from_scope))
}

struct UnpivotRefSpec<'a> {
    input: &'a TableRef,
    value_column: &'a Identifier,
    name_column: &'a Identifier,
    columns: &'a [UnpivotColumn],
    include_nulls: bool,
}

fn bind_unpivot_ref(
    spec: UnpivotRefSpec<'_>,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<(LogicalPlan, Vec<ScopeEntry>), PlanError> {
    let UnpivotRefSpec {
        input,
        value_column,
        name_column,
        columns,
        include_nulls,
    } = spec;
    if columns.is_empty() {
        return Err(PlanError::TypeMismatch(
            "UNPIVOT requires at least one source column".to_owned(),
        ));
    }

    let (input_plan, input_scope) = bind_table_ref(input, catalog, cte_catalog, scope)?;
    let input_schema = input_plan.schema().clone();
    let mut source_columns = Vec::with_capacity(columns.len());
    let mut source_set = HashSet::new();
    let mut value_type = DataType::Null;
    let empty_schema = Schema::empty();

    for column in columns {
        let source_column = resolve_schema_column(&input_schema, &column.column.value)?;
        source_set.insert(source_column);
        let source_type = input_schema.field_at(source_column).data_type.clone();
        value_type = unpivot_common_type(&value_type, &source_type)?;
        let label = if let Some(label_expr) = &column.label {
            let bound =
                bind_expr_with_ctes(label_expr, &empty_schema, catalog, cte_catalog, scope)?;
            let ScalarExpr::Literal { value, .. } = bound else {
                return Err(PlanError::NotSupported(
                    "UNPIVOT labels must be literal constants",
                ));
            };
            if matches!(value, Value::Null) {
                return Err(PlanError::TypeMismatch(
                    "UNPIVOT labels cannot be NULL".to_owned(),
                ));
            }
            value.to_string()
        } else {
            column.column.value.clone()
        };
        source_columns.push(LogicalUnpivotColumn {
            source_column,
            label,
        });
    }

    let passthrough_columns: Vec<usize> = (0..input_schema.len())
        .filter(|idx| !source_set.contains(idx))
        .collect();
    let mut output_fields = Vec::with_capacity(passthrough_columns.len() + 2);
    for idx in &passthrough_columns {
        output_fields.push(input_schema.field_at(*idx).clone());
    }
    output_fields.push(Field::required(
        name_column.value.clone(),
        DataType::Text { max_len: None },
    ));
    let value_field = if include_nulls {
        Field::nullable(value_column.value.clone(), value_type)
    } else {
        Field::required(value_column.value.clone(), value_type)
    };
    output_fields.push(value_field);
    reject_duplicate_output_fields("UNPIVOT", &output_fields)?;

    let schema = Schema::new(output_fields)
        .map_err(|err| PlanError::TypeMismatch(format!("UNPIVOT output schema: {err}")))?;
    let qualifier = transform_qualifier(&input_scope, "unpivot");
    let from_scope = scope_entries_for_schema(&qualifier, &schema);
    let plan = LogicalPlan::Unpivot {
        input: Box::new(input_plan),
        passthrough_columns,
        columns: source_columns,
        name_column: name_column.value.clone(),
        value_column: value_column.value.clone(),
        include_nulls,
        schema,
    };
    Ok((plan, from_scope))
}

fn validate_pivot_aggregate_arg(func: AggregateFunc, arg_type: &DataType) -> Result<(), PlanError> {
    match func {
        AggregateFunc::CountStar
        | AggregateFunc::Count
        | AggregateFunc::Min
        | AggregateFunc::Max => Ok(()),
        AggregateFunc::Sum | AggregateFunc::Avg
            if matches!(
                arg_type,
                DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
            ) =>
        {
            Ok(())
        }
        AggregateFunc::Sum | AggregateFunc::Avg => Err(PlanError::TypeMismatch(format!(
            "PIVOT {:?} argument must be SMALLINT, INTEGER, BIGINT, REAL, or DOUBLE PRECISION; got {arg_type}",
            func
        ))),
        _ => Err(PlanError::NotSupported(
            "PIVOT supports COUNT, SUM, AVG, MIN, and MAX aggregates",
        )),
    }
}

fn unpivot_common_type(left: &DataType, right: &DataType) -> Result<DataType, PlanError> {
    if matches!(left, DataType::Null) {
        return Ok(right.clone());
    }
    if matches!(right, DataType::Null) || left == right {
        return Ok(left.clone());
    }
    if left.is_numeric() && right.is_numeric() {
        return left.numeric_join(right).map_err(|_| {
            PlanError::TypeMismatch(format!("UNPIVOT cannot combine {left} and {right}"))
        });
    }
    if left.is_textlike() && right.is_textlike() {
        return Ok(DataType::Text { max_len: None });
    }
    Err(PlanError::TypeMismatch(format!(
        "UNPIVOT source columns must have compatible types, got {left} and {right}"
    )))
}

fn resolve_schema_column(schema: &Schema, name: &str) -> Result<usize, PlanError> {
    let mut matches = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| field.name.eq_ignore_ascii_case(name))
        .map(|(idx, _)| idx);
    let Some(first) = matches.next() else {
        return Err(PlanError::ColumnNotFound(name.to_owned()));
    };
    if matches.next().is_some() {
        return Err(PlanError::Ambiguous(name.to_owned()));
    }
    Ok(first)
}

fn pivot_output_name(value: &Value) -> String {
    let raw = value.to_string();
    let mut out = String::with_capacity(raw.len().max(1));
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if !out.ends_with('_') {
            out.push('_');
        }
    }
    while out.ends_with('_') {
        out.pop();
    }
    if out.is_empty() {
        out.push_str("value");
    }
    if out.as_bytes().first().is_some_and(u8::is_ascii_digit) {
        out.insert_str(0, "value_");
    }
    out
}

fn transform_qualifier(input_scope: &[ScopeEntry], fallback: &str) -> String {
    let Some(first) = input_scope.first() else {
        return fallback.to_owned();
    };
    if input_scope
        .iter()
        .all(|entry| entry.qualifier.eq_ignore_ascii_case(&first.qualifier))
    {
        first.qualifier.clone()
    } else {
        fallback.to_owned()
    }
}

fn scope_entries_for_schema(qualifier: &str, schema: &Schema) -> Vec<ScopeEntry> {
    schema
        .fields()
        .iter()
        .cloned()
        .enumerate()
        .map(|(field_index, field)| ScopeEntry {
            qualifier: qualifier.to_owned(),
            field_index,
            field,
        })
        .collect()
}

fn reject_duplicate_output_fields(context: &str, fields: &[Field]) -> Result<(), PlanError> {
    let mut seen = HashSet::new();
    for field in fields {
        let key = field.name.to_ascii_lowercase();
        if !seen.insert(key) {
            return Err(PlanError::TypeMismatch(format!(
                "{context} output column '{}' conflicts with another output column",
                field.name
            )));
        }
    }
    Ok(())
}

fn qualified_system_name(name: &ultrasql_parser::ast::ObjectName) -> Option<String> {
    if name.parts.len() != 2 {
        return None;
    }
    let namespace = name.parts[0].value.to_ascii_lowercase();
    if !matches!(namespace.as_str(), "pg_catalog" | "information_schema") {
        return None;
    }
    let relation = name.parts[1].value.to_ascii_lowercase();
    Some(format!("{namespace}.{relation}"))
}

fn bind_table_function(
    name: &ultrasql_parser::ast::Identifier,
    args: &[ultrasql_parser::ast::Expr],
    alias: Option<&ultrasql_parser::ast::Identifier>,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<(LogicalPlan, Vec<ScopeEntry>), PlanError> {
    let func_name = name.value.to_ascii_lowercase();
    let qualifier = alias.map_or_else(|| func_name.clone(), |a| a.value.clone());
    let mut bound_args: Vec<ScalarExpr> = Vec::with_capacity(args.len());
    let empty_schema = Schema::empty();
    for a in args {
        bound_args.push(bind_expr_with_ctes(
            a,
            &empty_schema,
            catalog,
            cte_catalog,
            scope,
        )?);
    }
    let (schema, from_scope) = match func_name.as_str() {
        "generate_series" => {
            let col_type = DataType::Int64;
            let field = Field::required("generate_series", col_type);
            let schema = Schema::new([field.clone()])
                .map_err(|e| PlanError::TypeMismatch(format!("generate_series schema: {e}")))?;
            (
                schema,
                vec![ScopeEntry {
                    qualifier: qualifier.clone(),
                    field_index: 0,
                    field,
                }],
            )
        }
        "unnest" => {
            if bound_args.len() != 1 {
                return Err(PlanError::NotSupported(
                    "unnest: expected one array argument",
                ));
            }
            let DataType::Array(element_type) = bound_args[0].data_type() else {
                return Err(PlanError::TypeMismatch(
                    "unnest: argument must be an array".to_owned(),
                ));
            };
            let col_type = array_base_type(&element_type).clone();
            let field = Field::required("unnest", col_type);
            let schema = Schema::new([field.clone()])
                .map_err(|e| PlanError::TypeMismatch(format!("unnest schema: {e}")))?;
            (
                schema,
                vec![ScopeEntry {
                    qualifier: qualifier.clone(),
                    field_index: 0,
                    field,
                }],
            )
        }
        "json_each" => {
            if bound_args.len() != 1 {
                return Err(PlanError::NotSupported(
                    "json_each: expected one json/jsonb argument",
                ));
            }
            let key = Field::required("key", DataType::Text { max_len: None });
            let value = Field::nullable("value", DataType::Jsonb);
            let schema = Schema::new([key.clone(), value.clone()])
                .map_err(|e| PlanError::TypeMismatch(format!("json_each schema: {e}")))?;
            (
                schema,
                vec![
                    ScopeEntry {
                        qualifier: qualifier.clone(),
                        field_index: 0,
                        field: key,
                    },
                    ScopeEntry {
                        qualifier: qualifier.clone(),
                        field_index: 1,
                        field: value,
                    },
                ],
            )
        }
        "jsonb_path_query" => {
            if !(2..=3).contains(&bound_args.len()) {
                return Err(PlanError::NotSupported(
                    "jsonb_path_query: expected jsonb document, path, and optional vars",
                ));
            }
            let field = Field::nullable("value", DataType::Jsonb);
            let schema = Schema::new([field.clone()])
                .map_err(|e| PlanError::TypeMismatch(format!("jsonb_path_query schema: {e}")))?;
            (
                schema,
                vec![ScopeEntry {
                    qualifier: qualifier.clone(),
                    field_index: 0,
                    field,
                }],
            )
        }
        "read_csv" => bind_read_csv_table_function(&bound_args, &qualifier)?,
        "read_parquet" => bind_read_parquet_table_function(&bound_args, &qualifier)?,
        "read_json" => {
            bind_json_table_function("read_json", JsonInputKind::Json, &bound_args, &qualifier)?
        }
        "read_ndjson" => bind_json_table_function(
            "read_ndjson",
            JsonInputKind::Ndjson,
            &bound_args,
            &qualifier,
        )?,
        "read_arrow" => bind_read_arrow_table_function(&bound_args, &qualifier)?,
        "read_iceberg" | "iceberg_scan" => {
            bind_iceberg_scan_table_function(&func_name, &bound_args, &qualifier)?
        }
        "sniff_csv" => bind_sniff_csv_table_function(&bound_args, &qualifier)?,
        _ => {
            return Err(PlanError::NotSupported(
                "table function (only generate_series, unnest, json_each, jsonb_path_query, json_table, xmltable, read_csv, read_parquet, read_json, read_ndjson, read_arrow, read_iceberg, iceberg_scan, and sniff_csv supported)",
            ));
        }
    };
    let plan = LogicalPlan::FunctionScan {
        name: func_name,
        args: bound_args,
        schema,
    };
    Ok((plan, from_scope))
}

fn bind_json_table_ref(
    context: &ultrasql_parser::ast::Expr,
    row_path: &str,
    columns: &[ultrasql_parser::ast::JsonTableColumn],
    alias: Option<&ultrasql_parser::ast::Identifier>,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<(LogicalPlan, Vec<ScopeEntry>), PlanError> {
    let empty_schema = Schema::empty();
    let context = bind_expr_with_ctes(context, &empty_schema, catalog, cte_catalog, scope)?;
    let qualifier = alias.map_or_else(|| "json_table".to_owned(), |a| a.value.clone());
    let mut fields = Vec::with_capacity(columns.len());
    let mut spec_columns = Vec::with_capacity(columns.len());
    for column in columns {
        match &column.kind {
            JsonTableColumnKind::Ordinality => {
                fields.push(Field::required(column.name.value.clone(), DataType::Int64));
                spec_columns.push(serde_json::json!({
                    "name": column.name.value,
                    "kind": "ordinality",
                }));
            }
            JsonTableColumnKind::Value { data_type, path } => {
                let data_type_resolved = resolve_type_name(data_type)?;
                if matches!(data_type_resolved, DataType::Array(_)) {
                    return Err(PlanError::NotSupported(
                        "JSON_TABLE array column types are not supported in this slice",
                    ));
                }
                fields.push(Field::nullable(
                    column.name.value.clone(),
                    data_type_resolved,
                ));
                spec_columns.push(serde_json::json!({
                    "name": column.name.value,
                    "kind": "value",
                    "type": json_table_type_name(data_type),
                    "path": path,
                }));
            }
            JsonTableColumnKind::Exists { data_type, path } => {
                let data_type_resolved = resolve_type_name(data_type)?;
                if data_type_resolved != DataType::Bool {
                    return Err(PlanError::TypeMismatch(
                        "JSON_TABLE EXISTS columns must be boolean".to_owned(),
                    ));
                }
                fields.push(Field::required(column.name.value.clone(), DataType::Bool));
                spec_columns.push(serde_json::json!({
                    "name": column.name.value,
                    "kind": "exists",
                    "type": json_table_type_name(data_type),
                    "path": path,
                }));
            }
        }
    }
    let schema = Schema::new(fields.clone())
        .map_err(|err| PlanError::TypeMismatch(format!("JSON_TABLE schema: {err}")))?;
    let from_scope = scope_entries(&qualifier, fields);
    let spec = serde_json::json!({ "columns": spec_columns }).to_string();
    let args = vec![
        context,
        ScalarExpr::Literal {
            value: Value::Text(row_path.to_owned()),
            data_type: DataType::Text { max_len: None },
        },
        ScalarExpr::Literal {
            value: Value::Text(spec),
            data_type: DataType::Text { max_len: None },
        },
    ];
    let plan = LogicalPlan::FunctionScan {
        name: "json_table".to_owned(),
        args,
        schema,
    };
    Ok((plan, from_scope))
}

fn bind_xml_table_ref(
    context: &ultrasql_parser::ast::Expr,
    row_path: &str,
    columns: &[ultrasql_parser::ast::XmlTableColumn],
    alias: Option<&ultrasql_parser::ast::Identifier>,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<(LogicalPlan, Vec<ScopeEntry>), PlanError> {
    let empty_schema = Schema::empty();
    let context = bind_expr_with_ctes(context, &empty_schema, catalog, cte_catalog, scope)?;
    let qualifier = alias.map_or_else(|| "xmltable".to_owned(), |a| a.value.clone());
    let mut fields = Vec::with_capacity(columns.len());
    let mut spec_columns = Vec::with_capacity(columns.len());
    for column in columns {
        match &column.kind {
            XmlTableColumnKind::Ordinality => {
                fields.push(Field::required(column.name.value.clone(), DataType::Int64));
                spec_columns.push(serde_json::json!({
                    "name": column.name.value,
                    "kind": "ordinality",
                }));
            }
            XmlTableColumnKind::Value {
                data_type,
                path,
                default,
            } => {
                let data_type_resolved = resolve_type_name(data_type)?;
                if matches!(data_type_resolved, DataType::Array(_)) {
                    return Err(PlanError::NotSupported(
                        "XMLTABLE array column types are not supported in this slice",
                    ));
                }
                fields.push(Field::nullable(
                    column.name.value.clone(),
                    data_type_resolved,
                ));
                spec_columns.push(serde_json::json!({
                    "name": column.name.value,
                    "kind": "value",
                    "type": json_table_type_name(data_type),
                    "path": path,
                    "default": default,
                }));
            }
        }
    }
    let schema = Schema::new(fields.clone())
        .map_err(|err| PlanError::TypeMismatch(format!("XMLTABLE schema: {err}")))?;
    let from_scope = scope_entries(&qualifier, fields);
    let spec = serde_json::json!({ "columns": spec_columns }).to_string();
    let args = vec![
        context,
        ScalarExpr::Literal {
            value: Value::Text(row_path.to_owned()),
            data_type: DataType::Text { max_len: None },
        },
        ScalarExpr::Literal {
            value: Value::Text(spec),
            data_type: DataType::Text { max_len: None },
        },
    ];
    let plan = LogicalPlan::FunctionScan {
        name: "xml_table".to_owned(),
        args,
        schema,
    };
    Ok((plan, from_scope))
}

fn json_table_type_name(data_type: &TypeName) -> String {
    let mut out = data_type.name.value.clone();
    if !data_type.type_modifiers.is_empty() {
        let mods = data_type
            .type_modifiers
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(",");
        out.push('(');
        out.push_str(&mods);
        out.push(')');
    }
    if data_type.is_array {
        let dimensions = data_type.array_dimensions.max(1);
        for _ in 0..dimensions {
            out.push_str("[]");
        }
    }
    out
}

fn array_base_type(ty: &DataType) -> &DataType {
    match ty {
        DataType::Array(inner) => array_base_type(inner),
        other => other,
    }
}

fn bind_read_parquet_table_function(
    bound_args: &[ScalarExpr],
    qualifier: &str,
) -> Result<(Schema, Vec<ScopeEntry>), PlanError> {
    if bound_args.len() != 1 {
        return Err(PlanError::NotSupported(
            "read_parquet: expected one path, glob, or path-list argument",
        ));
    }
    let path_specs = read_file_path_specs("read_parquet", &bound_args[0])?;
    let arrow_schema = if path_specs_use_object_store("read_parquet", &path_specs)? {
        read_parquet_object_schema(&path_specs)?
    } else {
        let first_path = first_expanded_file("read_parquet", &path_specs)?;
        read_parquet_arrow_schema(&first_path)?
    };
    let fields = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let data_type = parquet_arrow_type_to_sql(field.data_type())?;
            Ok(if field.is_nullable() {
                Field::nullable(field.name().clone(), data_type)
            } else {
                Field::required(field.name().clone(), data_type)
            })
        })
        .collect::<Result<Vec<_>, PlanError>>()?;
    let schema = Schema::new(fields.clone())
        .map_err(|err| PlanError::TypeMismatch(format!("read_parquet schema: {err}")))?;
    let from_scope = fields
        .into_iter()
        .enumerate()
        .map(|(field_index, field)| ScopeEntry {
            qualifier: qualifier.to_owned(),
            field_index,
            field,
        })
        .collect();
    Ok((schema, from_scope))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JsonInputKind {
    Json,
    Ndjson,
}

fn bind_json_table_function(
    function_name: &str,
    kind: JsonInputKind,
    bound_args: &[ScalarExpr],
    qualifier: &str,
) -> Result<(Schema, Vec<ScopeEntry>), PlanError> {
    if bound_args.len() != 1 {
        return Err(PlanError::TypeMismatch(format!(
            "{function_name}: expected one path, glob, or path-list argument"
        )));
    }
    let path_specs = read_file_path_specs(function_name, &bound_args[0])?;
    let fields = infer_json_fields_from_path_specs(function_name, kind, &path_specs)?;
    let schema = Schema::new(fields.clone())
        .map_err(|err| PlanError::TypeMismatch(format!("{function_name} schema: {err}")))?;
    let from_scope = scope_entries(qualifier, fields);
    Ok((schema, from_scope))
}

fn bind_read_arrow_table_function(
    bound_args: &[ScalarExpr],
    qualifier: &str,
) -> Result<(Schema, Vec<ScopeEntry>), PlanError> {
    if bound_args.len() != 1 {
        return Err(PlanError::NotSupported(
            "read_arrow: expected one path, glob, or path-list argument",
        ));
    }
    let path_specs = read_file_path_specs("read_arrow", &bound_args[0])?;
    let arrow_schema = read_arrow_schema_from_path_specs(&path_specs)?;
    let fields = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let data_type = arrow_type_to_sql("read_arrow", field.data_type())?;
            Ok(if field.is_nullable() {
                Field::nullable(field.name().clone(), data_type)
            } else {
                Field::required(field.name().clone(), data_type)
            })
        })
        .collect::<Result<Vec<_>, PlanError>>()?;
    let schema = Schema::new(fields.clone())
        .map_err(|err| PlanError::TypeMismatch(format!("read_arrow schema: {err}")))?;
    let from_scope = scope_entries(qualifier, fields);
    Ok((schema, from_scope))
}

fn bind_iceberg_scan_table_function(
    function_name: &str,
    bound_args: &[ScalarExpr],
    qualifier: &str,
) -> Result<(Schema, Vec<ScopeEntry>), PlanError> {
    if bound_args.len() != 1 {
        return Err(PlanError::TypeMismatch(format!(
            "{function_name}: expected one table root or metadata JSON path argument"
        )));
    }
    let path_specs = read_file_path_specs(function_name, &bound_args[0])?;
    let [path] = path_specs.as_slice() else {
        return Err(PlanError::TypeMismatch(format!(
            "{function_name}: expected one table root or metadata JSON path argument"
        )));
    };
    let schema = read_iceberg_schema(path)
        .map_err(|err| PlanError::TypeMismatch(format!("{function_name}: {err}")))?;
    let from_scope = schema
        .fields()
        .iter()
        .cloned()
        .enumerate()
        .map(|(field_index, field)| ScopeEntry {
            qualifier: qualifier.to_owned(),
            field_index,
            field,
        })
        .collect();
    Ok((schema, from_scope))
}

fn scope_entries(qualifier: &str, fields: Vec<Field>) -> Vec<ScopeEntry> {
    fields
        .into_iter()
        .enumerate()
        .map(|(field_index, field)| ScopeEntry {
            qualifier: qualifier.to_owned(),
            field_index,
            field,
        })
        .collect()
}

fn read_parquet_arrow_schema(path: &Path) -> Result<arrow_schema::SchemaRef, PlanError> {
    let file = open_local_regular_file("read_parquet", path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|err| {
        PlanError::TypeMismatch(format!(
            "read_parquet cannot inspect {}: {err}",
            path.display()
        ))
    })?;
    Ok(builder.schema().clone())
}

fn read_parquet_object_schema(patterns: &[String]) -> Result<arrow_schema::SchemaRef, PlanError> {
    let objects = expand_object_store_specs(patterns)
        .map_err(|err| PlanError::TypeMismatch(format!("read_parquet: {err}")))?;
    let location = objects.first().ok_or_else(|| {
        PlanError::TypeMismatch("read_parquet object path list is empty".to_owned())
    })?;
    let reader = PlannerObjectRangeChunkReader::new(location.clone())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader).map_err(|err| {
        PlanError::TypeMismatch(format!(
            "read_parquet cannot inspect {}: {err}",
            location.display_uri()
        ))
    })?;
    Ok(builder.schema().clone())
}

#[derive(Clone, Debug)]
struct PlannerObjectRangeChunkReader {
    location: ObjectLocation,
    display: String,
    len: u64,
}

impl PlannerObjectRangeChunkReader {
    fn new(location: ObjectLocation) -> Result<Self, PlanError> {
        let display = location.display_uri();
        let probe = read_object_range_with_metadata(&location, 0, 1)
            .map_err(|err| PlanError::TypeMismatch(format!("read_parquet: {err}")))?;
        let len = probe.object_size().ok_or_else(|| {
            PlanError::TypeMismatch(format!(
                "read_parquet cannot determine object size for {display}: missing Content-Range"
            ))
        })?;
        Ok(Self {
            location,
            display,
            len,
        })
    }
}

impl Length for PlannerObjectRangeChunkReader {
    fn len(&self) -> u64 {
        self.len
    }
}

impl ChunkReader for PlannerObjectRangeChunkReader {
    type T = PlannerObjectRangeReadCursor;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        if start > self.len {
            return Err(planner_parquet_range_error(format!(
                "read_parquet range start {start} beyond {} length {}",
                self.display, self.len
            )));
        }
        Ok(PlannerObjectRangeReadCursor {
            location: self.location.clone(),
            display: self.display.clone(),
            pos: start,
            len: self.len,
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        let length = validate_planner_object_range(&self.display, start, length, self.len)?;
        let bytes = read_object_range(&self.location, start, length).map_err(|err| {
            planner_parquet_range_error(format!(
                "read_parquet range GET {} bytes {start}+{length}: {err}",
                self.display
            ))
        })?;
        Ok(Bytes::from(bytes))
    }
}

#[derive(Debug)]
struct PlannerObjectRangeReadCursor {
    location: ObjectLocation,
    display: String,
    pos: u64,
    len: u64,
}

impl Read for PlannerObjectRangeReadCursor {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() || self.pos >= self.len {
            return Ok(0);
        }
        let remaining = self.len - self.pos;
        let requested = remaining.min(u64::try_from(buf.len()).unwrap_or(u64::MAX));
        let bytes = read_object_range(&self.location, self.pos, requested).map_err(|err| {
            io::Error::other(format!(
                "read_parquet range GET {} bytes {}+{}: {err}",
                self.display, self.pos, requested
            ))
        })?;
        let read = bytes.len().min(buf.len());
        buf[..read].copy_from_slice(&bytes[..read]);
        self.pos = self
            .pos
            .saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
        Ok(read)
    }
}

fn validate_planner_object_range(
    display: &str,
    start: u64,
    length: usize,
    object_len: u64,
) -> ParquetResult<u64> {
    let length = u64::try_from(length).map_err(|err| {
        planner_parquet_range_error(format!(
            "read_parquet range length overflow for {display}: {err}"
        ))
    })?;
    let end = start.checked_add(length).ok_or_else(|| {
        planner_parquet_range_error(format!(
            "read_parquet range overflows for {display}: start={start} length={length}"
        ))
    })?;
    if end > object_len {
        return Err(planner_parquet_range_error(format!(
            "read_parquet range beyond {display}: start={start} length={length} object_len={object_len}"
        )));
    }
    Ok(length)
}

fn planner_parquet_range_error(message: String) -> ParquetError {
    ParquetError::External(Box::new(io::Error::other(message)))
}

fn parquet_arrow_type_to_sql(data_type: &ArrowDataType) -> Result<DataType, PlanError> {
    arrow_type_to_sql("read_parquet", data_type)
}

fn arrow_type_to_sql(
    function_name: &str,
    data_type: &ArrowDataType,
) -> Result<DataType, PlanError> {
    match data_type {
        ArrowDataType::Boolean => Ok(DataType::Bool),
        ArrowDataType::Int32 => Ok(DataType::Int32),
        ArrowDataType::Int64 => Ok(DataType::Int64),
        ArrowDataType::Float32 => Ok(DataType::Float32),
        ArrowDataType::Float64 => Ok(DataType::Float64),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Ok(DataType::Text { max_len: None }),
        other => Err(PlanError::TypeMismatch(format!(
            "{function_name} unsupported Arrow type: {other}"
        ))),
    }
}

fn bind_read_csv_table_function(
    bound_args: &[ScalarExpr],
    qualifier: &str,
) -> Result<(Schema, Vec<ScopeEntry>), PlanError> {
    if !matches!(bound_args.len(), 1 | 2) {
        return Err(PlanError::NotSupported(
            "read_csv: expected path, glob, or path-list argument plus optional reject path",
        ));
    }
    let path_specs = read_csv_path_specs(&bound_args[0])?;
    let has_reject_path = bound_args.get(1).is_some();
    if let Some(reject_arg) = bound_args.get(1) {
        validate_read_csv_reject_path_arg(reject_arg)?;
    }
    let header = if has_reject_path {
        read_csv_header_from_path_specs_with_rejects(&path_specs)?
    } else {
        read_csv_header_from_path_specs(&path_specs)?
    };
    let mut fields = header
        .into_iter()
        .map(|name| Field::nullable(name, DataType::Text { max_len: None }))
        .collect::<Vec<_>>();
    fields.push(Field::nullable(
        "_filename",
        DataType::Text { max_len: None },
    ));
    fields.push(Field::required("_row_number", DataType::Int64));
    let schema = Schema::new(fields.clone())
        .map_err(|err| PlanError::TypeMismatch(format!("read_csv schema: {err}")))?;
    let from_scope = fields
        .into_iter()
        .enumerate()
        .map(|(field_index, field)| ScopeEntry {
            qualifier: qualifier.to_owned(),
            field_index,
            field,
        })
        .collect();
    Ok((schema, from_scope))
}

fn read_csv_path_specs(arg: &ScalarExpr) -> Result<Vec<String>, PlanError> {
    read_file_path_specs("read_csv", arg)
}

fn validate_read_csv_reject_path_arg(arg: &ScalarExpr) -> Result<(), PlanError> {
    let ScalarExpr::Literal {
        value: Value::Text(path),
        ..
    } = arg
    else {
        return Err(PlanError::TypeMismatch(
            "read_csv: reject path must be a string literal".to_owned(),
        ));
    };
    if path.is_empty() {
        return Err(PlanError::TypeMismatch(
            "read_csv: reject path must not be empty".to_owned(),
        ));
    }
    if is_object_store_uri(path) {
        return Err(PlanError::TypeMismatch(
            "read_csv: reject path must be a local file path".to_owned(),
        ));
    }
    Ok(())
}

fn read_file_path_specs(function_name: &str, arg: &ScalarExpr) -> Result<Vec<String>, PlanError> {
    match arg {
        ScalarExpr::Literal {
            value: Value::Text(pattern),
            ..
        } => Ok(vec![pattern.clone()]),
        ScalarExpr::Literal {
            value:
                Value::Array {
                    element_type,
                    elements,
                },
            ..
        } if matches!(element_type, &DataType::Text { max_len: None }) => elements
            .iter()
            .map(|value| match value {
                Value::Text(path) => Ok(path.clone()),
                _ => Err(PlanError::TypeMismatch(format!(
                    "{function_name}: path-list elements must be string literals"
                ))),
            })
            .collect(),
        _ => Err(PlanError::TypeMismatch(format!(
            "{function_name}: argument must be a string literal or text array literal"
        ))),
    }
}

type JsonObject = JsonMap<String, JsonValue>;

#[derive(Clone, Debug)]
enum PlannerStreamSpec {
    Local(PathBuf),
    Object(ObjectLocation),
}

impl PlannerStreamSpec {
    fn display(&self) -> String {
        match self {
            Self::Local(path) => path.display().to_string(),
            Self::Object(object) => object.display_uri(),
        }
    }
}

fn planner_stream_specs(
    function_name: &str,
    path_specs: &[String],
) -> Result<Vec<PlannerStreamSpec>, PlanError> {
    if path_specs_use_object_store(function_name, path_specs)? {
        let objects = expand_object_store_specs(path_specs)
            .map_err(|err| PlanError::TypeMismatch(format!("{function_name}: {err}")))?;
        return Ok(objects.into_iter().map(PlannerStreamSpec::Object).collect());
    }
    Ok(expand_file_path_specs(function_name, path_specs)?
        .into_iter()
        .map(PlannerStreamSpec::Local)
        .collect())
}

fn open_planner_stream(
    function_name: &str,
    source: &PlannerStreamSpec,
) -> Result<Box<dyn Read>, PlanError> {
    match source {
        PlannerStreamSpec::Local(path) => {
            let file = open_local_regular_file(function_name, path)?;
            Ok(Box::new(file))
        }
        PlannerStreamSpec::Object(object) => {
            Ok(Box::new(PlannerObjectRangeReader::new(object.clone())))
        }
    }
}

struct PlannerObjectRangeReader {
    location: ObjectLocation,
    display: String,
    pos: u64,
    object_size: Option<u64>,
    buffer: Vec<u8>,
    cursor: usize,
    eof: bool,
}

impl PlannerObjectRangeReader {
    fn new(location: ObjectLocation) -> Self {
        let display = location.display_uri();
        Self {
            location,
            display,
            pos: 0,
            object_size: None,
            buffer: Vec::new(),
            cursor: 0,
            eof: false,
        }
    }

    fn refill(&mut self) -> io::Result<()> {
        if self.cursor < self.buffer.len() || self.eof {
            return Ok(());
        }
        self.buffer.clear();
        self.cursor = 0;
        let requested = self.object_size.map_or(JSON_STREAM_CHUNK_BYTES, |size| {
            size.saturating_sub(self.pos).min(JSON_STREAM_CHUNK_BYTES)
        });
        if requested == 0 {
            self.eof = true;
            return Ok(());
        }
        let range = read_object_range_with_metadata(&self.location, self.pos, requested)
            .map_err(|err| io::Error::other(format!("{}: {err}", self.display)))?;
        if let Some(size) = range.object_size() {
            self.object_size = Some(size);
        }
        let bytes = range.into_bytes();
        if bytes.is_empty() {
            self.eof = true;
            return Ok(());
        }
        let read_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        self.pos = self.pos.saturating_add(read_len);
        if self.object_size.is_some_and(|size| self.pos >= size)
            || self.object_size.is_none() && read_len < requested
        {
            self.eof = true;
        }
        self.buffer = bytes;
        Ok(())
    }
}

impl Read for PlannerObjectRangeReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        self.refill()?;
        let available = self.buffer.len().saturating_sub(self.cursor);
        if available == 0 {
            return Ok(0);
        }
        let n = available.min(out.len());
        out[..n].copy_from_slice(&self.buffer[self.cursor..self.cursor + n]);
        self.cursor += n;
        Ok(n)
    }
}

fn infer_json_fields_from_path_specs(
    function_name: &str,
    kind: JsonInputKind,
    path_specs: &[String],
) -> Result<Vec<Field>, PlanError> {
    let sources = planner_stream_specs(function_name, path_specs)?;
    let mut acc = JsonFieldAccumulator::default();
    for source in sources {
        let display = source.display();
        let mut reader =
            PlannerJsonRecordReader::new(kind, open_planner_stream(function_name, &source)?);
        while let Some((row_number, text)) = reader.next_text(function_name, &display)? {
            let value = serde_json::from_str::<JsonValue>(&text).map_err(|err| {
                PlanError::TypeMismatch(format!(
                    "{function_name} parse {display} row {row_number}: {err}"
                ))
            })?;
            let row = json_value_to_object(function_name, &display, row_number, value)?;
            acc.observe(function_name, &row)?;
        }
    }
    Ok(acc.finish())
}

enum PlannerJsonRecordReader {
    Ndjson {
        reader: Box<dyn Read>,
        line_number: usize,
    },
    Json {
        reader: Box<dyn Read>,
        state: PlannerJsonDocumentState,
        row_number: usize,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PlannerJsonDocumentState {
    Start,
    Array,
    Done,
}

impl PlannerJsonRecordReader {
    fn new(kind: JsonInputKind, reader: Box<dyn Read>) -> Self {
        match kind {
            JsonInputKind::Ndjson => Self::Ndjson {
                reader,
                line_number: 0,
            },
            JsonInputKind::Json => Self::Json {
                reader,
                state: PlannerJsonDocumentState::Start,
                row_number: 0,
            },
        }
    }

    fn next_text(
        &mut self,
        function_name: &str,
        display: &str,
    ) -> Result<Option<(usize, String)>, PlanError> {
        match self {
            Self::Ndjson {
                reader,
                line_number,
            } => planner_next_ndjson_text(reader.as_mut(), line_number, function_name, display),
            Self::Json {
                reader,
                state,
                row_number,
            } => planner_next_json_text(reader.as_mut(), state, row_number, function_name, display),
        }
    }
}

fn planner_next_ndjson_text(
    reader: &mut dyn Read,
    line_number: &mut usize,
    function_name: &str,
    display: &str,
) -> Result<Option<(usize, String)>, PlanError> {
    let mut bytes = Vec::new();
    let mut byte = [0_u8; 1];
    loop {
        bytes.clear();
        loop {
            let read = reader.read(&mut byte).map_err(|err| {
                PlanError::TypeMismatch(format!("{function_name} cannot read {display}: {err}"))
            })?;
            if read == 0 {
                if bytes.is_empty() {
                    return Ok(None);
                }
                break;
            }
            bytes.push(byte[0]);
            if bytes.len() > PLANNER_JSON_RECORD_LIMIT_BYTES {
                return Err(PlanError::TypeMismatch(format!(
                    "{function_name} record in {display} exceeds record limit: limit={PLANNER_JSON_RECORD_LIMIT_BYTES}"
                )));
            }
            if byte[0] == b'\n' {
                break;
            }
        }
        *line_number = line_number.saturating_add(1);
        let text = String::from_utf8(bytes.clone()).map_err(|err| {
            PlanError::TypeMismatch(format!("{function_name} cannot decode {display}: {err}"))
        })?;
        let trimmed = text.trim();
        if !trimmed.is_empty() {
            return Ok(Some((*line_number, trimmed.to_owned())));
        }
    }
}

fn planner_next_json_text(
    reader: &mut dyn Read,
    state: &mut PlannerJsonDocumentState,
    row_number: &mut usize,
    function_name: &str,
    display: &str,
) -> Result<Option<(usize, String)>, PlanError> {
    loop {
        match state {
            PlannerJsonDocumentState::Start => {
                let Some(byte) = planner_read_non_ws_byte(reader, function_name, display)? else {
                    return Ok(None);
                };
                match byte {
                    b'{' => {
                        *state = PlannerJsonDocumentState::Done;
                        *row_number = 1;
                        return planner_read_json_container(reader, byte, function_name, display)
                            .map(|text| Some((*row_number, text)));
                    }
                    b'[' => *state = PlannerJsonDocumentState::Array,
                    other => {
                        return Err(PlanError::TypeMismatch(format!(
                            "{function_name} expected object or array of objects in {display}, got byte {other}"
                        )));
                    }
                }
            }
            PlannerJsonDocumentState::Array => {
                let Some(byte) = planner_read_non_ws_byte(reader, function_name, display)? else {
                    return Err(PlanError::TypeMismatch(format!(
                        "{function_name} array in {display} ended before closing bracket"
                    )));
                };
                match byte {
                    b']' => {
                        *state = PlannerJsonDocumentState::Done;
                        return Ok(None);
                    }
                    b',' => {}
                    b'{' => {
                        *row_number = row_number.saturating_add(1);
                        return planner_read_json_container(reader, byte, function_name, display)
                            .map(|text| Some((*row_number, text)));
                    }
                    other => {
                        return Err(PlanError::TypeMismatch(format!(
                            "{function_name} expected object in array {display}, got byte {other}"
                        )));
                    }
                }
            }
            PlannerJsonDocumentState::Done => return Ok(None),
        }
    }
}

fn planner_read_non_ws_byte(
    reader: &mut dyn Read,
    function_name: &str,
    display: &str,
) -> Result<Option<u8>, PlanError> {
    let mut buf = [0_u8; 1];
    loop {
        let read = reader.read(&mut buf).map_err(|err| {
            PlanError::TypeMismatch(format!("{function_name} cannot read {display}: {err}"))
        })?;
        if read == 0 {
            return Ok(None);
        }
        if !buf[0].is_ascii_whitespace() {
            return Ok(Some(buf[0]));
        }
    }
}

fn planner_read_json_container(
    reader: &mut dyn Read,
    first: u8,
    function_name: &str,
    display: &str,
) -> Result<String, PlanError> {
    let mut bytes = vec![first];
    let mut depth = 1_i32;
    let mut in_string = false;
    let mut escaped = false;
    let mut byte = [0_u8; 1];
    while depth > 0 {
        let read = reader.read(&mut byte).map_err(|err| {
            PlanError::TypeMismatch(format!("{function_name} cannot read {display}: {err}"))
        })?;
        if read == 0 {
            return Err(PlanError::TypeMismatch(format!(
                "{function_name} object in {display} ended before closing brace"
            )));
        }
        let b = byte[0];
        bytes.push(b);
        if bytes.len() > PLANNER_JSON_RECORD_LIMIT_BYTES {
            return Err(PlanError::TypeMismatch(format!(
                "{function_name} record in {display} exceeds record limit: limit={PLANNER_JSON_RECORD_LIMIT_BYTES}"
            )));
        }
        if in_string {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' | b'[' => depth = depth.saturating_add(1),
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    String::from_utf8(bytes).map_err(|err| {
        PlanError::TypeMismatch(format!("{function_name} cannot decode {display}: {err}"))
    })
}

fn json_value_to_object(
    function_name: &str,
    display: &str,
    row_number: usize,
    value: JsonValue,
) -> Result<JsonObject, PlanError> {
    match value {
        JsonValue::Object(object) => Ok(object),
        _ => Err(PlanError::TypeMismatch(format!(
            "{function_name} row {row_number} in {display} is not a JSON object"
        ))),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JsonColumnKind {
    Unknown,
    Bool,
    Int64,
    Float64,
    Text,
}

#[derive(Clone, Debug)]
struct JsonFieldSpec {
    name: String,
    kind: JsonColumnKind,
    nullable: bool,
}

#[derive(Default)]
struct JsonFieldAccumulator {
    columns: BTreeMap<String, JsonFieldSpec>,
    present: BTreeMap<String, usize>,
    rows: usize,
}

impl JsonFieldAccumulator {
    fn observe(&mut self, function_name: &str, row: &JsonObject) -> Result<(), PlanError> {
        self.rows = self.rows.saturating_add(1);
        for (name, value) in row {
            if name.is_empty() {
                return Err(PlanError::TypeMismatch(format!(
                    "{function_name}: JSON object contains an empty column name"
                )));
            }
            let kind = json_value_kind(value);
            self.columns
                .entry(name.clone())
                .and_modify(|spec| {
                    spec.kind = widen_json_kind(spec.kind, kind);
                    spec.nullable |= value.is_null();
                })
                .or_insert_with(|| JsonFieldSpec {
                    name: name.clone(),
                    kind,
                    nullable: value.is_null(),
                });
            *self.present.entry(name.clone()).or_insert(0) += 1;
        }
        Ok(())
    }

    fn finish(mut self) -> Vec<Field> {
        for spec in self.columns.values_mut() {
            if self.present.get(&spec.name).copied().unwrap_or(0) < self.rows {
                spec.nullable = true;
            }
        }
        self.columns
            .into_values()
            .map(|spec| {
                let data_type = match spec.kind {
                    JsonColumnKind::Unknown => DataType::Text { max_len: None },
                    JsonColumnKind::Bool => DataType::Bool,
                    JsonColumnKind::Int64 => DataType::Int64,
                    JsonColumnKind::Float64 => DataType::Float64,
                    JsonColumnKind::Text => DataType::Text { max_len: None },
                };
                if spec.nullable {
                    Field::nullable(spec.name, data_type)
                } else {
                    Field::required(spec.name, data_type)
                }
            })
            .collect()
    }
}

fn json_value_kind(value: &JsonValue) -> JsonColumnKind {
    match value {
        JsonValue::Null => JsonColumnKind::Unknown,
        JsonValue::Bool(_) => JsonColumnKind::Bool,
        JsonValue::Number(number) => {
            if number.as_i64().is_some()
                || number
                    .as_u64()
                    .is_some_and(|value| i64::try_from(value).is_ok())
            {
                JsonColumnKind::Int64
            } else if number.as_f64().is_some() {
                JsonColumnKind::Float64
            } else {
                JsonColumnKind::Text
            }
        }
        JsonValue::String(_) | JsonValue::Array(_) | JsonValue::Object(_) => JsonColumnKind::Text,
    }
}

fn widen_json_kind(left: JsonColumnKind, right: JsonColumnKind) -> JsonColumnKind {
    match (left, right) {
        (JsonColumnKind::Unknown, kind) | (kind, JsonColumnKind::Unknown) => kind,
        (JsonColumnKind::Text, _) | (_, JsonColumnKind::Text) => JsonColumnKind::Text,
        (JsonColumnKind::Float64, _) | (_, JsonColumnKind::Float64) => JsonColumnKind::Float64,
        (JsonColumnKind::Int64, JsonColumnKind::Int64) => JsonColumnKind::Int64,
        (JsonColumnKind::Bool, JsonColumnKind::Bool) => JsonColumnKind::Bool,
        _ => JsonColumnKind::Text,
    }
}

fn read_arrow_schema_from_path_specs(
    path_specs: &[String],
) -> Result<arrow_schema::SchemaRef, PlanError> {
    if path_specs_use_object_store("read_arrow", path_specs)? {
        let (location, bytes) = read_first_object_bytes(path_specs)
            .map_err(|err| PlanError::TypeMismatch(format!("read_arrow: {err}")))?;
        let reader = ArrowFileReader::try_new(Cursor::new(bytes), None).map_err(|err| {
            PlanError::TypeMismatch(format!(
                "read_arrow cannot inspect {}: {err}",
                location.display_uri()
            ))
        })?;
        return Ok(reader.schema());
    }

    let first_path = first_expanded_file("read_arrow", path_specs)?;
    let file = open_local_regular_file("read_arrow", &first_path)?;
    let reader = ArrowFileReader::try_new(file, None).map_err(|err| {
        PlanError::TypeMismatch(format!(
            "read_arrow cannot inspect {}: {err}",
            first_path.display()
        ))
    })?;
    Ok(reader.schema())
}

fn read_csv_header_from_path_specs_with_rejects(
    path_specs: &[String],
) -> Result<Vec<String>, PlanError> {
    match read_csv_header_from_path_specs(path_specs) {
        Ok(header) => Ok(header),
        Err(original) => match read_csv_header_from_first_record(path_specs) {
            Ok(header) => Ok(header),
            Err(_) => Err(original),
        },
    }
}

fn read_csv_header_from_first_record(path_specs: &[String]) -> Result<Vec<String>, PlanError> {
    let (display, bytes) = if path_specs_use_object_store("read_csv", path_specs)? {
        read_first_object_csv_sample(path_specs)?
    } else {
        let first = first_expanded_file("read_csv", path_specs)?;
        let display = first.display().to_string();
        let bytes = read_local_csv_header_sample(&display, &first)?;
        (display, bytes)
    };
    let text = String::from_utf8(bytes).map_err(|err| {
        PlanError::TypeMismatch(format!("read_csv: {display} is not UTF-8: {err}"))
    })?;
    infer_csv_header_from_first_record(&display, &text)
}

fn read_local_csv_header_sample(display: &str, path: &Path) -> Result<Vec<u8>, PlanError> {
    let file = open_local_regular_file("read_csv", path)?;
    let mut bytes = Vec::new();
    file.take(READ_CSV_HEADER_SAMPLE_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|err| PlanError::TypeMismatch(format!("read_csv cannot read {display}: {err}")))?;
    let sample_limit = usize::try_from(READ_CSV_HEADER_SAMPLE_BYTES).unwrap_or(usize::MAX);
    if bytes.len() > sample_limit {
        if !csv_header_sample_has_complete_record(&bytes[..sample_limit]) {
            return Err(PlanError::TypeMismatch(format!(
                "read_csv: {display} first record exceeds sample limit: limit={READ_CSV_HEADER_SAMPLE_BYTES}"
            )));
        }
        bytes.truncate(sample_limit);
    }
    Ok(bytes)
}

fn open_local_regular_file(function_name: &str, path: &Path) -> Result<File, PlanError> {
    let metadata = fs::symlink_metadata(path).map_err(|err| {
        PlanError::TypeMismatch(format!(
            "{function_name} cannot inspect {}: {err}",
            path.display()
        ))
    })?;
    if !metadata.file_type().is_file() {
        return Err(PlanError::TypeMismatch(format!(
            "{function_name} path is not a regular file: {}",
            path.display()
        )));
    }
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW);
    }
    options.open(path).map_err(|err| {
        PlanError::TypeMismatch(format!(
            "{function_name} cannot open {}: {err}",
            path.display()
        ))
    })
}

fn csv_header_sample_has_complete_record(sample: &[u8]) -> bool {
    let mut in_quotes = false;
    let mut i = 0;
    while i < sample.len() {
        match sample[i] {
            b'"' if in_quotes && i + 1 < sample.len() && sample[i + 1] == b'"' => {
                i += 2;
                continue;
            }
            b'"' => in_quotes = !in_quotes,
            b'\n' | b'\r' if !in_quotes => return true,
            _ => {}
        }
        i += 1;
    }
    false
}

fn infer_csv_header_from_first_record(display: &str, text: &str) -> Result<Vec<String>, PlanError> {
    let mut best: Option<Vec<String>> = None;
    let mut last_error: Option<String> = None;
    for delimiter in [',', ';', '\t', '|'] {
        let options = CsvParseOptions {
            delimiter,
            quote: Some('"'),
            escape: Some('"'),
        };
        match first_csv_record_with_options(display, text, options) {
            Ok(record) if best.as_ref().is_none_or(|best| record.len() > best.len()) => {
                best = Some(record);
            }
            Ok(_) => {}
            Err(err) => last_error = Some(err),
        }
    }
    let Some(header) = best else {
        return Err(PlanError::TypeMismatch(last_error.unwrap_or_else(|| {
            format!("read_csv header missing in {display}")
        })));
    };
    if header.is_empty() || header.iter().any(String::is_empty) {
        return Err(PlanError::TypeMismatch(format!(
            "read_csv: header contains an empty column name: {display}"
        )));
    }
    Ok(header)
}

fn first_csv_record_with_options(
    display: &str,
    text: &str,
    options: CsvParseOptions,
) -> Result<Vec<String>, String> {
    let mut buffer = String::new();
    for line in text.split_inclusive('\n') {
        buffer.push_str(line);
        match parse_csv_records_with_options(&buffer, options) {
            Ok(mut records) if records.len() == 1 => {
                return Ok(records.remove(0));
            }
            Ok(records) if records.is_empty() => buffer.clear(),
            Ok(_) => {
                return Err(format!(
                    "read_csv parse {display}: first-record buffer produced multiple records"
                ));
            }
            Err(err) if err.to_string().contains("unterminated quoted field") => {}
            Err(err) => return Err(format!("read_csv parse {display}: {err}")),
        }
    }
    if buffer.is_empty() {
        return Err(format!("read_csv header missing in {display}"));
    }
    let mut records = parse_csv_records_with_options(&buffer, options)
        .map_err(|err| format!("read_csv parse {display}: {err}"))?;
    if records.len() == 1 {
        Ok(records.remove(0))
    } else {
        Err(format!(
            "read_csv parse {display}: first-record buffer produced {} records",
            records.len()
        ))
    }
}

fn read_csv_header_from_path_specs(path_specs: &[String]) -> Result<Vec<String>, PlanError> {
    if path_specs_use_object_store("read_csv", path_specs)? {
        let (display, bytes) = read_first_object_csv_sample(path_specs)?;
        let text = String::from_utf8(bytes).map_err(|err| {
            PlanError::TypeMismatch(format!("read_csv: {display} is not UTF-8: {err}"))
        })?;
        let header = infer_csv_header_from_first_record(&display, &text)?;
        if header.is_empty() || header.iter().any(String::is_empty) {
            return Err(PlanError::TypeMismatch(format!(
                "read_csv: header contains an empty column name: {display}"
            )));
        }
        return Ok(header);
    }
    read_csv_header_from_specs(path_specs)
        .map_err(|err| PlanError::TypeMismatch(format!("read_csv: {err}")))
}

fn read_first_object_csv_sample(path_specs: &[String]) -> Result<(String, Vec<u8>), PlanError> {
    let objects = expand_object_store_specs(path_specs)
        .map_err(|err| PlanError::TypeMismatch(format!("read_csv: {err}")))?;
    let first = objects
        .first()
        .ok_or_else(|| PlanError::TypeMismatch("read_csv: object path list is empty".to_owned()))?;
    let bytes = read_object_range_with_metadata(first, 0, READ_CSV_HEADER_SAMPLE_BYTES)
        .map_err(|err| PlanError::TypeMismatch(format!("read_csv: {err}")))?
        .into_bytes();
    Ok((first.display_uri(), bytes))
}

fn path_specs_use_object_store(
    function_name: &str,
    path_specs: &[String],
) -> Result<bool, PlanError> {
    let object_count = path_specs
        .iter()
        .filter(|spec| is_object_store_uri(spec))
        .count();
    if object_count == 0 {
        return Ok(false);
    }
    if object_count == path_specs.len() {
        return Ok(true);
    }
    Err(PlanError::TypeMismatch(format!(
        "{function_name}: cannot mix local and object-store paths"
    )))
}

fn expand_file_path_specs(
    function_name: &str,
    patterns: &[String],
) -> Result<Vec<PathBuf>, PlanError> {
    if patterns.is_empty() {
        return Err(PlanError::TypeMismatch(format!(
            "{function_name}: path list cannot be empty"
        )));
    }
    let mut paths = Vec::new();
    for pattern in patterns {
        paths.extend(expand_file_paths(function_name, pattern)?);
    }
    Ok(paths)
}

fn first_expanded_file(function_name: &str, patterns: &[String]) -> Result<PathBuf, PlanError> {
    expand_file_path_specs(function_name, patterns)?
        .into_iter()
        .next()
        .ok_or_else(|| {
            PlanError::TypeMismatch(format!("{function_name}: path expansion produced no files"))
        })
}

fn expand_file_paths(function_name: &str, pattern: &str) -> Result<Vec<PathBuf>, PlanError> {
    let path = Path::new(pattern);
    let file_pattern = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            PlanError::TypeMismatch(format!(
                "{function_name}: path must name a file or wildcard: {pattern}"
            ))
        })?;
    if !contains_wildcard(file_pattern) {
        return Ok(vec![path.to_path_buf()]);
    }

    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(parent).map_err(|err| {
        PlanError::TypeMismatch(format!(
            "{function_name}: cannot read directory {}: {err}",
            parent.display()
        ))
    })? {
        let entry =
            entry.map_err(|err| PlanError::TypeMismatch(format!("{function_name}: {err}")))?;
        let Some(name) = entry.file_name().to_str().map(ToOwned::to_owned) else {
            continue;
        };
        if wildcard_match(file_pattern, &name) {
            paths.push(entry.path());
        }
    }
    paths.sort();
    if paths.is_empty() {
        return Err(PlanError::TypeMismatch(format!(
            "{function_name}: pattern matched no files: {pattern}"
        )));
    }
    Ok(paths)
}

fn contains_wildcard(s: &str) -> bool {
    s.chars().any(|ch| matches!(ch, '*' | '?'))
}

fn wildcard_match(pattern: &str, text: &str) -> bool {
    let pattern = pattern.chars().collect::<Vec<_>>();
    let text = text.chars().collect::<Vec<_>>();
    let mut dp = vec![vec![false; text.len() + 1]; pattern.len() + 1];
    dp[0][0] = true;
    for (i, ch) in pattern.iter().enumerate() {
        if *ch == '*' {
            dp[i + 1][0] = dp[i][0];
        }
    }
    for (i, pattern_ch) in pattern.iter().enumerate() {
        for (j, text_ch) in text.iter().enumerate() {
            dp[i + 1][j + 1] = match pattern_ch {
                '*' => dp[i][j + 1] || dp[i + 1][j],
                '?' => dp[i][j],
                ch => dp[i][j] && ch == text_ch,
            };
        }
    }
    dp[pattern.len()][text.len()]
}

fn bind_sniff_csv_table_function(
    bound_args: &[ScalarExpr],
    qualifier: &str,
) -> Result<(Schema, Vec<ScopeEntry>), PlanError> {
    if bound_args.len() != 1 {
        return Err(PlanError::NotSupported(
            "sniff_csv: expected one path argument",
        ));
    }
    let ScalarExpr::Literal {
        value: Value::Text(_),
        ..
    } = &bound_args[0]
    else {
        return Err(PlanError::TypeMismatch(
            "sniff_csv: path argument must be a string literal".to_owned(),
        ));
    };
    let fields = vec![
        Field::nullable("Delimiter", DataType::Text { max_len: None }),
        Field::nullable("Quote", DataType::Text { max_len: None }),
        Field::nullable("Escape", DataType::Text { max_len: None }),
        Field::nullable("NewLineDelimiter", DataType::Text { max_len: None }),
        Field::required("SkipRows", DataType::Int64),
        Field::required("HasHeader", DataType::Bool),
        Field::nullable("Columns", DataType::Text { max_len: None }),
        Field::nullable("DateFormat", DataType::Text { max_len: None }),
        Field::nullable("TimestampFormat", DataType::Text { max_len: None }),
        Field::nullable("UserArguments", DataType::Text { max_len: None }),
        Field::nullable("Prompt", DataType::Text { max_len: None }),
    ];
    let schema = Schema::new(fields.clone())
        .map_err(|err| PlanError::TypeMismatch(format!("sniff_csv schema: {err}")))?;
    let from_scope = fields
        .into_iter()
        .enumerate()
        .map(|(field_index, field)| ScopeEntry {
            qualifier: qualifier.to_owned(),
            field_index,
            field,
        })
        .collect();
    Ok((schema, from_scope))
}

fn rebuild_subquery_plan(
    inner_plan: LogicalPlan,
    alias_schema: &Schema,
) -> Result<LogicalPlan, PlanError> {
    let exprs: Vec<(ScalarExpr, String)> = alias_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let expr = ScalarExpr::Column {
                name: f.name.clone(),
                index: i,
                data_type: f.data_type.clone(),
            };
            (expr, f.name.clone())
        })
        .collect();
    let proj_fields: Vec<Field> = alias_schema.fields().to_vec();
    let proj_schema = Schema::new(proj_fields)
        .map_err(|e| PlanError::TypeMismatch(format!("subquery alias schema: {e}")))?;
    Ok(LogicalPlan::Project {
        input: Box::new(inner_plan),
        exprs,
        schema: proj_schema,
    })
}

fn bind_explicit_join(
    left_ref: &TableRef,
    op: JoinOp,
    right_ref: &TableRef,
    condition: &JoinCondition,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<(LogicalPlan, Vec<ScopeEntry>), PlanError> {
    let (left_plan, left_scope) = bind_table_ref(left_ref, catalog, cte_catalog, scope)?;
    let (right_plan, right_scope) = bind_table_ref(right_ref, catalog, cte_catalog, scope)?;

    let join_type = match op {
        JoinOp::Inner => LogicalJoinType::Inner,
        JoinOp::LeftOuter => LogicalJoinType::LeftOuter,
        JoinOp::RightOuter => LogicalJoinType::RightOuter,
        JoinOp::FullOuter => LogicalJoinType::FullOuter,
        JoinOp::Cross => LogicalJoinType::Cross,
    };

    match condition {
        JoinCondition::None => {
            let join_schema = concat_schemas_cross(left_plan.schema(), right_plan.schema())?;
            let left_len = left_scope.len();
            let out_scope = merge_scopes(left_scope, right_scope, left_len);
            Ok((
                LogicalPlan::Join {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    join_type,
                    condition: LogicalJoinCondition::None,
                    schema: join_schema,
                },
                out_scope,
            ))
        }
        JoinCondition::On(pred_ast) => {
            let concat_schema =
                concat_schemas_for_join(left_plan.schema(), right_plan.schema(), join_type)?;
            let left_len = left_scope.len();
            let out_scope = merge_scopes(left_scope, right_scope, left_len);
            let binding_schema = schema_for_qualified_binding(&concat_schema, &out_scope)?;
            let pred = bind_expr_with_ctes(pred_ast, &binding_schema, catalog, cte_catalog, scope)?;
            if pred.data_type() != DataType::Bool && pred.data_type() != DataType::Null {
                return Err(PlanError::TypeMismatch(format!(
                    "JOIN ON predicate must be boolean, got {}",
                    pred.data_type()
                )));
            }
            Ok((
                LogicalPlan::Join {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    join_type,
                    condition: LogicalJoinCondition::On(pred),
                    schema: concat_schema,
                },
                out_scope,
            ))
        }
        JoinCondition::Using(cols) => {
            let pairs = resolve_using_pairs(cols, left_plan.schema(), right_plan.schema())?;
            bind_using_join(
                left_plan,
                right_plan,
                left_scope,
                right_scope,
                join_type,
                pairs,
            )
        }
        JoinCondition::Natural => {
            let pairs = resolve_natural_pairs(left_plan.schema(), right_plan.schema());
            bind_using_join(
                left_plan,
                right_plan,
                left_scope,
                right_scope,
                join_type,
                pairs,
            )
        }
    }
}

fn bind_using_join(
    left_plan: LogicalPlan,
    right_plan: LogicalPlan,
    left_scope: Vec<ScopeEntry>,
    right_scope: Vec<ScopeEntry>,
    join_type: LogicalJoinType,
    pairs: Vec<(usize, usize)>,
) -> Result<(LogicalPlan, Vec<ScopeEntry>), PlanError> {
    let schema = build_using_schema(left_plan.schema(), right_plan.schema(), &pairs, join_type)?;
    let out_scope = build_using_scope(&left_scope, &right_scope, &pairs);
    Ok((
        LogicalPlan::Join {
            left: Box::new(left_plan),
            right: Box::new(right_plan),
            join_type,
            condition: LogicalJoinCondition::Using(pairs),
            schema,
        },
        out_scope,
    ))
}

fn build_using_scope(
    left_scope: &[ScopeEntry],
    right_scope: &[ScopeEntry],
    pairs: &[(usize, usize)],
) -> Vec<ScopeEntry> {
    let left_using: std::collections::HashSet<usize> =
        pairs.iter().map(|(left_idx, _)| *left_idx).collect();
    let right_using: std::collections::HashSet<usize> =
        pairs.iter().map(|(_, right_idx)| *right_idx).collect();
    let mut out = Vec::with_capacity(left_scope.len() + right_scope.len() - right_using.len());
    for (left_idx, _) in pairs {
        if let Some(entry) = left_scope.get(*left_idx) {
            push_scope_entry(&mut out, entry);
        }
    }
    for (left_idx, entry) in left_scope.iter().enumerate() {
        if !left_using.contains(&left_idx) {
            push_scope_entry(&mut out, entry);
        }
    }
    for (right_idx, entry) in right_scope.iter().enumerate() {
        if !right_using.contains(&right_idx) {
            push_scope_entry(&mut out, entry);
        }
    }
    out
}

fn push_scope_entry(out: &mut Vec<ScopeEntry>, entry: &ScopeEntry) {
    out.push(ScopeEntry {
        qualifier: entry.qualifier.clone(),
        field_index: out.len(),
        field: entry.field.clone(),
    });
}

fn resolve_using_pairs(
    cols: &[ultrasql_parser::ast::Identifier],
    left: &Schema,
    right: &Schema,
) -> Result<Vec<(usize, usize)>, PlanError> {
    let mut pairs: Vec<(usize, usize)> = Vec::with_capacity(cols.len());
    for ident in cols {
        let col_name = &ident.value;
        let left_idx = left
            .find(col_name)
            .ok_or_else(|| PlanError::ColumnNotFound(col_name.clone()))?
            .0;
        let right_idx = right
            .find(col_name)
            .ok_or_else(|| PlanError::ColumnNotFound(col_name.clone()))?
            .0;
        pairs.push((left_idx, right_idx));
    }
    Ok(pairs)
}

fn resolve_natural_pairs(left: &Schema, right: &Schema) -> Vec<(usize, usize)> {
    let mut pairs = Vec::new();
    for (left_idx, left_field) in left.fields().iter().enumerate() {
        if let Some((right_idx, _)) = right.find(&left_field.name) {
            pairs.push((left_idx, right_idx));
        }
    }
    pairs
}

fn build_using_schema(
    left: &Schema,
    right: &Schema,
    pairs: &[(usize, usize)],
    join_type: LogicalJoinType,
) -> Result<Schema, PlanError> {
    let using_set: std::collections::HashSet<usize> = pairs.iter().map(|(l, _)| *l).collect();
    let right_using_set: std::collections::HashSet<usize> = pairs.iter().map(|(_, r)| *r).collect();

    let mut out_fields: Vec<Field> = Vec::new();
    for &(left_idx, _) in pairs {
        let f = left.field_at(left_idx);
        let nullable = matches!(join_type, LogicalJoinType::FullOuter) || f.nullable;
        out_fields.push(Field {
            name: f.name.clone(),
            data_type: f.data_type.clone(),
            nullable,
        });
    }
    for (i, f) in left.fields().iter().enumerate() {
        if using_set.contains(&i) {
            continue;
        }
        let nullable = matches!(
            join_type,
            LogicalJoinType::RightOuter | LogicalJoinType::FullOuter
        ) || f.nullable;
        out_fields.push(Field {
            name: f.name.clone(),
            data_type: f.data_type.clone(),
            nullable,
        });
    }
    for (i, f) in right.fields().iter().enumerate() {
        if right_using_set.contains(&i) {
            continue;
        }
        let nullable = matches!(
            join_type,
            LogicalJoinType::LeftOuter | LogicalJoinType::FullOuter
        ) || f.nullable;
        out_fields.push(Field {
            name: f.name.clone(),
            data_type: f.data_type.clone(),
            nullable,
        });
    }
    Schema::new(out_fields).map_err(|e| PlanError::TypeMismatch(format!("USING join schema: {e}")))
}

pub(super) fn concat_schemas_cross(left: &Schema, right: &Schema) -> Result<Schema, PlanError> {
    let mut fields: Vec<Field> = Vec::with_capacity(left.len() + right.len());
    let mut used_names = std::collections::HashSet::new();
    for f in left.fields() {
        used_names.insert(f.name.to_ascii_lowercase());
        fields.push(f.clone());
    }
    for f in right.fields() {
        let name = unique_join_field_name(&f.name, &mut used_names);
        fields.push(Field {
            name,
            data_type: f.data_type.clone(),
            nullable: f.nullable,
        });
    }
    Schema::new(fields).map_err(|e| PlanError::TypeMismatch(format!("join schema: {e}")))
}

pub(super) fn concat_schemas_for_join(
    left: &Schema,
    right: &Schema,
    join_type: LogicalJoinType,
) -> Result<Schema, PlanError> {
    let make_left_nullable = matches!(
        join_type,
        LogicalJoinType::RightOuter | LogicalJoinType::FullOuter
    );
    let make_right_nullable = matches!(
        join_type,
        LogicalJoinType::LeftOuter | LogicalJoinType::FullOuter
    );

    let mut fields: Vec<Field> = Vec::with_capacity(left.len() + right.len());
    let mut used_names = std::collections::HashSet::new();
    for f in left.fields() {
        used_names.insert(f.name.to_ascii_lowercase());
        fields.push(Field {
            name: f.name.clone(),
            data_type: f.data_type.clone(),
            nullable: f.nullable || make_left_nullable,
        });
    }
    for f in right.fields() {
        let name = unique_join_field_name(&f.name, &mut used_names);
        fields.push(Field {
            name,
            data_type: f.data_type.clone(),
            nullable: f.nullable || make_right_nullable,
        });
    }
    Schema::new(fields).map_err(|e| PlanError::TypeMismatch(format!("join schema: {e}")))
}

fn unique_join_field_name(
    base: &str,
    used_names: &mut std::collections::HashSet<String>,
) -> String {
    if used_names.insert(base.to_ascii_lowercase()) {
        return base.to_owned();
    }
    for suffix in 1.. {
        let candidate = format!("{base}_{suffix}");
        if used_names.insert(candidate.to_ascii_lowercase()) {
            return candidate;
        }
    }
    unreachable!("unbounded suffix search returns before overflow")
}

pub(super) fn merge_scopes(
    left: Vec<ScopeEntry>,
    right: Vec<ScopeEntry>,
    left_len: usize,
) -> Vec<ScopeEntry> {
    let mut out = left;
    for e in right {
        out.push(ScopeEntry {
            qualifier: e.qualifier,
            field_index: e.field_index + left_len,
            field: e.field,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use serde_json::json;
    use ultrasql_parser::Parser;
    use ultrasql_parser::ast::Statement;

    use super::*;
    use crate::catalog::{InMemoryCatalog, TableMeta};

    fn text_lit(value: impl Into<String>) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Text(value.into()),
            data_type: DataType::Text { max_len: None },
        }
    }

    fn text_array(values: &[String]) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Array {
                element_type: DataType::Text { max_len: None },
                elements: values.iter().cloned().map(Value::Text).collect(),
            },
            data_type: DataType::Array(Box::new(DataType::Text { max_len: None })),
        }
    }

    fn planner_test_catalog() -> InMemoryCatalog {
        let users = Schema::new([
            Field::required("id", DataType::Int32),
            Field::nullable("name", DataType::Text { max_len: None }),
        ])
        .expect("users schema");
        let orders = Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("user_id", DataType::Int32),
        ])
        .expect("orders schema");
        let mut catalog = InMemoryCatalog::new();
        catalog.register("users", TableMeta::new(users.clone()));
        catalog.register("orders", TableMeta::new(orders));
        catalog.register("pg_class", TableMeta::with_schema_name("pg_catalog", users));
        catalog
    }

    fn parse_from(sql: &str) -> Vec<TableRef> {
        match Parser::new(sql).parse_statement().expect(sql) {
            Statement::Select(select) => select.from,
            other => panic!("expected select, got {other:?}"),
        }
    }

    fn bind_from_sql(sql: &str) -> (LogicalPlan, Vec<ScopeEntry>) {
        let catalog = planner_test_catalog();
        let from = parse_from(sql);
        bind_from(&from, &catalog, &[], &mut ScopeStack::new()).expect(sql)
    }

    #[test]
    fn local_path_specs_globs_and_mixing_errors_are_explicit() {
        let dir = tempfile::tempdir().expect("tempdir");
        let a = dir.path().join("a.csv");
        let b = dir.path().join("b.csv");
        std::fs::write(&a, "id\n1\n").expect("write a");
        std::fs::write(&b, "id\n2\n").expect("write b");

        assert!(contains_wildcard("*.csv"));
        assert!(wildcard_match("?.csv", "a.csv"));
        assert!(!wildcard_match("?.csv", "ab.csv"));

        let pattern = dir.path().join("*.csv").display().to_string();
        let expanded = expand_file_path_specs("read_csv", &[pattern]).expect("expand glob");
        assert_eq!(expanded, vec![a, b]);

        let mixed =
            path_specs_use_object_store("read_csv", &["s3://bucket/a.csv".into(), "b.csv".into()])
                .expect_err("mixed path specs rejected");
        assert!(
            mixed
                .to_string()
                .contains("cannot mix local and object-store paths"),
            "{mixed}"
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn bind_from_covers_table_ref_families_and_join_scope_shapes() {
        let (empty, empty_scope) =
            bind_from(&[], &planner_test_catalog(), &[], &mut ScopeStack::new()).expect("empty");
        assert!(matches!(empty, LogicalPlan::Empty { .. }));
        assert!(empty_scope.is_empty());

        let (system_scan, system_scope) = bind_from_sql("SELECT * FROM pg_catalog.pg_class AS c");
        assert_eq!(system_scan.schema().field_at(0).name, "id");
        assert_eq!(system_scope[0].qualifier, "c");

        let catalog = planner_test_catalog();
        let cte_schema =
            Schema::new([Field::required("cte_id", DataType::Int64)]).expect("cte schema");
        let cte_from = parse_from("SELECT * FROM latest AS l");
        let (cte_scan, cte_scope) = bind_from(
            &cte_from,
            &catalog,
            &[("latest".to_owned(), cte_schema.clone())],
            &mut ScopeStack::new(),
        )
        .expect("cte from");
        assert_eq!(cte_scan.schema(), &cte_schema);
        assert_eq!(cte_scope[0].qualifier, "l");

        let (subquery, subquery_scope) =
            bind_from_sql("SELECT * FROM (SELECT id, name FROM users) AS q(user_id, username)");
        assert_eq!(subquery.schema().field_at(0).name, "user_id");
        assert_eq!(subquery_scope[1].field.name, "username");

        for (sql, expected_fields) in [
            (
                "SELECT * FROM generate_series(1, 3) AS g",
                vec![("generate_series", DataType::Int64)],
            ),
            (
                "SELECT * FROM unnest([[1, 2], [3, 4]]) AS u",
                vec![("unnest", DataType::Int32)],
            ),
            (
                "SELECT * FROM json_each(jsonb '{\"a\":1}') AS j",
                vec![
                    ("key", DataType::Text { max_len: None }),
                    ("value", DataType::Jsonb),
                ],
            ),
            (
                "SELECT * FROM jsonb_path_query(jsonb '{\"a\":1}', '$.a') AS p",
                vec![("value", DataType::Jsonb)],
            ),
            (
                "SELECT * FROM sniff_csv('/tmp/no-read-needed.csv') AS sniff",
                vec![
                    ("Delimiter", DataType::Text { max_len: None }),
                    ("Quote", DataType::Text { max_len: None }),
                    ("Escape", DataType::Text { max_len: None }),
                    ("NewLineDelimiter", DataType::Text { max_len: None }),
                    ("SkipRows", DataType::Int64),
                    ("HasHeader", DataType::Bool),
                    ("Columns", DataType::Text { max_len: None }),
                    ("DateFormat", DataType::Text { max_len: None }),
                    ("TimestampFormat", DataType::Text { max_len: None }),
                    ("UserArguments", DataType::Text { max_len: None }),
                    ("Prompt", DataType::Text { max_len: None }),
                ],
            ),
            (
                "SELECT * FROM JSON_TABLE(\
                 jsonb '[{\"id\":1,\"name\":\"Ada\"}]', \
                 '$[*]' COLUMNS (\
                     ord FOR ORDINALITY, \
                     id bigint PATH '$.id', \
                     name text, \
                     has_name boolean EXISTS PATH '$.name'\
                 )) jt",
                vec![
                    ("ord", DataType::Int64),
                    ("id", DataType::Int64),
                    ("name", DataType::Text { max_len: None }),
                    ("has_name", DataType::Bool),
                ],
            ),
            (
                "SELECT * FROM XMLTABLE(\
                 '/root/item' PASSING XML '<root><item id=\"1\"><name>Ada</name></item></root>' \
                 COLUMNS (\
                     ord FOR ORDINALITY, \
                     id bigint PATH '@id', \
                     name text PATH 'name/text()'\
                 )) xt",
                vec![
                    ("ord", DataType::Int64),
                    ("id", DataType::Int64),
                    ("name", DataType::Text { max_len: None }),
                ],
            ),
        ] {
            let (plan, scope) = bind_from_sql(sql);
            assert_eq!(plan.schema().len(), expected_fields.len(), "{sql}");
            assert_eq!(scope.len(), expected_fields.len(), "{sql}");
            for (idx, (name, data_type)) in expected_fields.into_iter().enumerate() {
                assert_eq!(plan.schema().field_at(idx).name, name, "{sql}");
                assert_eq!(plan.schema().field_at(idx).data_type, data_type, "{sql}");
            }
        }

        let (joined, joined_scope) =
            bind_from_sql("SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id");
        let LogicalPlan::Join {
            join_type,
            condition,
            schema,
            ..
        } = joined
        else {
            panic!("expected join");
        };
        assert_eq!(join_type, LogicalJoinType::LeftOuter);
        assert!(matches!(condition, LogicalJoinCondition::On(_)));
        assert!(schema.field_at(2).nullable, "right side left-join nullable");
        assert_eq!(joined_scope[2].qualifier, "o");

        let (using_join, _) = bind_from_sql("SELECT * FROM users FULL JOIN orders USING (id)");
        let LogicalPlan::Join {
            join_type,
            condition,
            schema,
            ..
        } = using_join
        else {
            panic!("expected using join");
        };
        assert_eq!(join_type, LogicalJoinType::FullOuter);
        assert!(matches!(condition, LogicalJoinCondition::Using(_)));
        assert_eq!(schema.field_at(0).name, "id");
        assert!(schema.field_at(0).nullable);

        for sql in [
            "SELECT * FROM missing_table",
            "SELECT * FROM no_such_function()",
            "SELECT * FROM unnest(1)",
            "SELECT * FROM json_each()",
            "SELECT * FROM jsonb_path_query(jsonb '{\"a\":1}')",
            "SELECT * FROM users u JOIN orders o ON u.id",
            "SELECT * FROM users u JOIN orders o USING (missing)",
        ] {
            let catalog = planner_test_catalog();
            let from = parse_from(sql);
            let Err(err) = bind_from(&from, &catalog, &[], &mut ScopeStack::new()) else {
                panic!("expected bind error for {sql}");
            };
            assert!(
                matches!(
                    err,
                    PlanError::TableNotFound(_)
                        | PlanError::ColumnNotFound(_)
                        | PlanError::TypeMismatch(_)
                        | PlanError::NotSupported(_)
                ),
                "{sql}: {err:?}"
            );
        }
    }

    #[test]
    fn path_argument_reader_accepts_text_arrays_and_rejects_bad_shapes() {
        let paths = vec!["a.csv".to_owned(), "b.csv".to_owned()];
        assert_eq!(
            read_file_path_specs("read_csv", &text_array(&paths)).expect("text array"),
            paths
        );
        let bad_array = ScalarExpr::Literal {
            value: Value::Array {
                element_type: DataType::Int32,
                elements: vec![Value::Int32(1)],
            },
            data_type: DataType::Array(Box::new(DataType::Int32)),
        };
        assert!(read_file_path_specs("read_csv", &bad_array).is_err());
        assert!(validate_read_csv_reject_path_arg(&text_lit("rejects.csv")).is_ok());
        assert!(validate_read_csv_reject_path_arg(&text_lit("")).is_err());
        assert!(validate_read_csv_reject_path_arg(&text_lit("s3://bucket/rejects.csv")).is_err());
        let bad_scalar = ScalarExpr::Literal {
            value: Value::Int32(1),
            data_type: DataType::Int32,
        };
        assert!(read_file_path_specs("read_csv", &bad_scalar).is_err());
        assert!(expand_file_path_specs("read_csv", &[]).is_err());
        assert!(expand_file_paths("read_csv", "/").is_err());
    }

    #[test]
    fn csv_header_inference_handles_delimiters_and_multiline_quotes() {
        assert_eq!(
            infer_csv_header_from_first_record("comma.csv", "id,name\n1,alice\n")
                .expect("comma header"),
            vec!["id".to_owned(), "name".to_owned()]
        );
        assert_eq!(
            infer_csv_header_from_first_record("semi.csv", "id;name;score\n1;a;2\n")
                .expect("semicolon header"),
            vec!["id".to_owned(), "name".to_owned(), "score".to_owned()]
        );
        assert_eq!(
            infer_csv_header_from_first_record("quoted.csv", "\"id\npart\",name\n1,a\n")
                .expect("multiline header"),
            vec!["id\npart".to_owned(), "name".to_owned()]
        );
        assert!(infer_csv_header_from_first_record("bad.csv", ",name\n").is_err());
        assert!(
            first_csv_record_with_options(
                "empty.csv",
                "",
                CsvParseOptions {
                    delimiter: ',',
                    quote: Some('"'),
                    escape: Some('"'),
                },
            )
            .is_err()
        );
        assert!(infer_csv_header_from_first_record("multi.csv", "a,b\n1,2\n").is_ok());
    }

    #[test]
    fn csv_header_fallback_rejects_oversized_first_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("large-header.csv");
        fs::write(
            &path,
            format!(
                "{}\n1\n",
                "a".repeat(
                    usize::try_from(READ_CSV_HEADER_SAMPLE_BYTES)
                        .expect("CSV header sample limit fits usize")
                        + 1,
                )
            ),
        )
        .expect("write csv");

        let err = read_csv_header_from_first_record(&[path.display().to_string()])
            .expect_err("oversized first record rejected");

        assert!(err.to_string().contains("exceeds sample limit"), "{err}");
    }

    #[test]
    fn json_record_readers_stream_objects_and_report_malformed_rows() {
        let mut ndjson = PlannerJsonRecordReader::new(
            JsonInputKind::Ndjson,
            Box::new(Cursor::new(b"\n{\"id\":1}\n{\"id\":2}\n".to_vec())),
        );
        assert_eq!(
            ndjson
                .next_text("read_ndjson", "rows.ndjson")
                .expect("first ndjson"),
            Some((2, "{\"id\":1}".to_owned()))
        );
        assert_eq!(
            ndjson
                .next_text("read_ndjson", "rows.ndjson")
                .expect("second ndjson"),
            Some((3, "{\"id\":2}".to_owned()))
        );
        assert_eq!(
            ndjson
                .next_text("read_ndjson", "rows.ndjson")
                .expect("eof ndjson"),
            None
        );

        let mut json = PlannerJsonRecordReader::new(
            JsonInputKind::Json,
            Box::new(Cursor::new(br#"[{"id":1},{"id":2}]"#.to_vec())),
        );
        assert_eq!(
            json.next_text("read_json", "rows.json")
                .expect("first json row"),
            Some((1, "{\"id\":1}".to_owned()))
        );
        assert_eq!(
            json.next_text("read_json", "rows.json")
                .expect("second json row"),
            Some((2, "{\"id\":2}".to_owned()))
        );
        assert_eq!(
            json.next_text("read_json", "rows.json").expect("json eof"),
            None
        );

        assert!(json_value_to_object("read_json", "rows.json", 1, json!(["not-object"])).is_err());

        let mut object = PlannerJsonRecordReader::new(
            JsonInputKind::Json,
            Box::new(Cursor::new(br#"{"id":{"nested":true}}"#.to_vec())),
        );
        assert_eq!(
            object
                .next_text("read_json", "object.json")
                .expect("single object"),
            Some((1, "{\"id\":{\"nested\":true}}".to_owned()))
        );
        assert_eq!(
            object.next_text("read_json", "object.json").expect("done"),
            None
        );

        let mut scalar = PlannerJsonRecordReader::new(
            JsonInputKind::Json,
            Box::new(Cursor::new(b"42".to_vec())),
        );
        assert!(scalar.next_text("read_json", "scalar.json").is_err());

        let mut bad_array = PlannerJsonRecordReader::new(
            JsonInputKind::Json,
            Box::new(Cursor::new(b"[1]".to_vec())),
        );
        assert!(bad_array.next_text("read_json", "bad-array.json").is_err());

        let mut truncated_array = PlannerJsonRecordReader::new(
            JsonInputKind::Json,
            Box::new(Cursor::new(br#"[{"id":1}"#.to_vec())),
        );
        assert_eq!(
            truncated_array
                .next_text("read_json", "truncated-array.json")
                .expect("first object"),
            Some((1, "{\"id\":1}".to_owned()))
        );
        assert!(
            truncated_array
                .next_text("read_json", "truncated-array.json")
                .is_err()
        );

        let mut truncated_object = PlannerJsonRecordReader::new(
            JsonInputKind::Json,
            Box::new(Cursor::new(br#"{"id":"unterminated"#.to_vec())),
        );
        assert!(
            truncated_object
                .next_text("read_json", "truncated-object.json")
                .is_err()
        );
    }

    #[test]
    fn json_record_readers_reject_oversized_records() {
        let payload = "x".repeat(PLANNER_JSON_RECORD_LIMIT_BYTES);
        let object = format!("{{\"payload\":\"{payload}\"}}");
        let mut ndjson = PlannerJsonRecordReader::new(
            JsonInputKind::Ndjson,
            Box::new(Cursor::new(format!("{object}\n").into_bytes())),
        );
        assert_json_record_limit(
            ndjson.next_text("read_ndjson", "large.ndjson"),
            "read_ndjson",
        );

        let mut json = PlannerJsonRecordReader::new(
            JsonInputKind::Json,
            Box::new(Cursor::new(format!("[{object}]").into_bytes())),
        );
        assert_json_record_limit(json.next_text("read_json", "large.json"), "read_json");
    }

    fn assert_json_record_limit(result: Result<Option<(usize, String)>, PlanError>, name: &str) {
        match result {
            Err(err) => assert!(err.to_string().contains("exceeds record limit"), "{err}"),
            Ok(_) => panic!("{name} oversized record accepted"),
        }
    }

    #[test]
    fn json_field_accumulator_widens_and_marks_missing_values_nullable() {
        let mut acc = JsonFieldAccumulator::default();
        let first = JsonMap::from_iter([
            ("id".to_owned(), json!(1)),
            ("flag".to_owned(), json!(true)),
        ]);
        let second = JsonMap::from_iter([
            ("id".to_owned(), json!(2.5)),
            ("note".to_owned(), json!(null)),
        ]);
        acc.observe("read_json", &first).expect("first row");
        acc.observe("read_json", &second).expect("second row");
        let fields = acc.finish();

        let id = fields.iter().find(|f| f.name == "id").expect("id field");
        assert_eq!(id.data_type, DataType::Float64);
        assert!(!id.nullable);
        let flag = fields
            .iter()
            .find(|f| f.name == "flag")
            .expect("flag field");
        assert!(flag.nullable, "missing in second row marks nullable");
        let note = fields
            .iter()
            .find(|f| f.name == "note")
            .expect("note field");
        assert_eq!(note.data_type, DataType::Text { max_len: None });
        assert!(note.nullable);

        let empty_name = JsonMap::from_iter([("".to_owned(), json!(1))]);
        let mut bad = JsonFieldAccumulator::default();
        assert!(bad.observe("read_json", &empty_name).is_err());

        assert_eq!(json_value_kind(&json!(null)), JsonColumnKind::Unknown);
        assert_eq!(json_value_kind(&json!(true)), JsonColumnKind::Bool);
        assert_eq!(json_value_kind(&json!(1)), JsonColumnKind::Int64);
        assert_eq!(json_value_kind(&json!(1.5)), JsonColumnKind::Float64);
        assert_eq!(json_value_kind(&json!("x")), JsonColumnKind::Text);
        assert_eq!(
            widen_json_kind(JsonColumnKind::Bool, JsonColumnKind::Int64),
            JsonColumnKind::Text
        );
    }

    #[test]
    fn arrow_and_range_helpers_cover_supported_and_error_paths() {
        for (arrow, sql) in [
            (ArrowDataType::Boolean, DataType::Bool),
            (ArrowDataType::Int32, DataType::Int32),
            (ArrowDataType::Int64, DataType::Int64),
            (ArrowDataType::Float32, DataType::Float32),
            (ArrowDataType::Float64, DataType::Float64),
            (ArrowDataType::Utf8, DataType::Text { max_len: None }),
            (ArrowDataType::LargeUtf8, DataType::Text { max_len: None }),
        ] {
            assert_eq!(arrow_type_to_sql("read_arrow", &arrow).unwrap(), sql);
        }
        assert!(arrow_type_to_sql("read_arrow", &ArrowDataType::Date32).is_err());

        assert_eq!(validate_planner_object_range("obj", 2, 3, 10).unwrap(), 3);
        assert!(validate_planner_object_range("obj", 8, 3, 10).is_err());
        assert!(validate_planner_object_range("obj", u64::MAX, 1, u64::MAX).is_err());

        let err = planner_parquet_range_error("bad range".to_owned());
        assert!(err.to_string().contains("bad range"));
    }

    #[test]
    fn local_csv_and_json_table_functions_infer_scoped_schemas() {
        let dir = tempfile::tempdir().expect("tempdir");
        let csv = dir.path().join("rows.csv");
        std::fs::write(&csv, "id,name\n1,alice\n").expect("write csv");
        let json = dir.path().join("rows.json");
        std::fs::write(&json, r#"[{"id":1,"name":"alice"},{"id":2}]"#).expect("write json");
        let ndjson = dir.path().join("rows.ndjson");
        std::fs::write(&ndjson, "{\"id\":1}\n{\"id\":2,\"ok\":true}\n").expect("write ndjson");

        let (csv_schema, csv_scope) =
            bind_read_csv_table_function(&[text_lit(csv.display().to_string())], "c")
                .expect("csv schema");
        assert_eq!(csv_schema.field_at(0).name, "id");
        assert_eq!(csv_schema.field_at(1).name, "name");
        assert_eq!(csv_schema.field_at(2).name, "_filename");
        assert_eq!(csv_scope[0].qualifier, "c");

        let (json_schema, json_scope) = bind_json_table_function(
            "read_json",
            JsonInputKind::Json,
            &[text_lit(json.display().to_string())],
            "j",
        )
        .expect("json schema");
        assert_eq!(json_scope[0].qualifier, "j");
        assert!(json_schema.find("name").expect("name").1.nullable);

        let (ndjson_schema, _) = bind_json_table_function(
            "read_ndjson",
            JsonInputKind::Ndjson,
            &[text_lit(ndjson.display().to_string())],
            "n",
        )
        .expect("ndjson schema");
        assert_eq!(
            ndjson_schema.find("ok").expect("ok").1.data_type,
            DataType::Bool
        );

        let (sniff_schema, sniff_scope) =
            bind_sniff_csv_table_function(&[text_lit(csv.display().to_string())], "sniff")
                .expect("sniff schema");
        assert_eq!(sniff_schema.field_at(0).name, "Delimiter");
        assert_eq!(sniff_scope[0].qualifier, "sniff");

        assert!(bind_read_csv_table_function(&[], "c").is_err());
        assert!(
            bind_read_csv_table_function(&[text_lit(csv.display().to_string()), text_lit("")], "c")
                .is_err()
        );
        assert!(bind_json_table_function("read_json", JsonInputKind::Json, &[], "j").is_err());
        assert!(bind_sniff_csv_table_function(&[], "sniff").is_err());
        assert!(
            bind_sniff_csv_table_function(
                &[ScalarExpr::Literal {
                    value: Value::Int32(1),
                    data_type: DataType::Int32,
                }],
                "sniff",
            )
            .is_err()
        );
    }
}
