//! Table-function references: the `bind_table_function` dispatcher, JSON_TABLE
//! and XMLTABLE binding, and the file-reading function binders.

use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_iceberg::read_iceberg_schema;
use ultrasql_parser::ast::{JsonTableColumnKind, TypeName, XmlTableColumnKind};

use super::super::ddl::resolve_type_name;
use super::csv_schema::{bind_read_csv_table_function, bind_sniff_csv_table_function};
use super::json_reader::{JsonInputKind, infer_json_fields_from_path_specs};
use super::paths::{first_expanded_file, path_specs_use_object_store, read_file_path_specs};
use super::readers::{
    arrow_type_to_sql, parquet_arrow_type_to_sql, read_arrow_schema_from_path_specs,
    read_parquet_arrow_schema, read_parquet_object_schema,
};
use super::{
    Catalog, LogicalPlan, PlanError, ScalarExpr, ScopeEntry, ScopeStack, bind_expr_with_ctes,
};

pub(super) fn bind_table_function(
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

pub(super) fn bind_json_table_ref(
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

pub(super) fn bind_xml_table_ref(
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

pub(super) fn bind_json_table_function(
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
