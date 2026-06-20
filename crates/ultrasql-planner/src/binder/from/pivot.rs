//! PIVOT and UNPIVOT table-reference binding, plus their schema/scope helpers.

use std::collections::HashSet;

use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_parser::ast::{Identifier, PivotAggregate, PivotValue, TableRef, UnpivotColumn};

use super::super::aggregate::{aggregate_return_type, classify_aggregate};
use super::super::expr_bind::coerce_literal_to_type;
use super::super::expr_type::comparable;
use super::{
    AggregateFunc, Catalog, LogicalPivotAggregate, LogicalPivotValue, LogicalPlan,
    LogicalUnpivotColumn, PlanError, ScalarExpr, ScopeEntry, ScopeStack, bind_expr_with_ctes,
    bind_table_ref, schema_for_qualified_binding,
};

pub(super) fn bind_pivot_ref(
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

pub(super) struct UnpivotRefSpec<'a> {
    pub(super) input: &'a TableRef,
    pub(super) value_column: &'a Identifier,
    pub(super) name_column: &'a Identifier,
    pub(super) columns: &'a [UnpivotColumn],
    pub(super) include_nulls: bool,
}

pub(super) fn bind_unpivot_ref(
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

pub(super) fn resolve_schema_column(schema: &Schema, name: &str) -> Result<usize, PlanError> {
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

pub(super) fn transform_qualifier(input_scope: &[ScopeEntry], fallback: &str) -> String {
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

pub(super) fn scope_entries_for_schema(qualifier: &str, schema: &Schema) -> Vec<ScopeEntry> {
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
