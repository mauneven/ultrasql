//! FROM clause and JOIN binding. Split out of `binder/mod.rs` to keep each
//! file under the 600-line ceiling.

use std::fs::File;
use std::path::{Path, PathBuf};

use arrow_schema::DataType as ArrowDataType;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use ultrasql_core::{
    DataType, Field, Schema, Value,
    csv::{read_csv_data_from_text, read_csv_header_from_specs},
};
use ultrasql_objectstore::{is_object_store_uri, read_first_object_bytes};
use ultrasql_parser::ast::{JoinCondition, JoinOp, TableRef};

use super::{
    Catalog, LogicalJoinCondition, LogicalJoinType, LogicalPlan, PlanError, ScalarExpr, ScopeEntry,
    ScopeStack, apply_column_aliases, bind_expr_with_ctes, bind_select_with_ctes,
    schema_for_qualified_binding,
};

pub(super) fn bind_from(
    from_items: &[TableRef],
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    outer_scope: &mut ScopeStack,
) -> Result<(LogicalPlan, Vec<ScopeEntry>), PlanError> {
    if from_items.is_empty() {
        return Ok((
            LogicalPlan::Empty {
                schema: Schema::empty(),
            },
            vec![],
        ));
    }

    let mut iter = from_items.iter();
    let first = iter.next().expect("at least one item checked above");
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
            let table_name = qualified_system_name(name).unwrap_or_else(|| raw_table_name.clone());
            let qualifier = alias
                .as_ref()
                .map_or_else(|| raw_table_name.clone(), |a| a.value.clone());

            let schema = if let Some((_, s)) = cte_catalog
                .iter()
                .rev()
                .find(|(n, _)| n.eq_ignore_ascii_case(&table_name))
            {
                s.clone()
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
    }
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
            let col_type = *element_type;
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
        "read_csv" => bind_read_csv_table_function(&bound_args, &qualifier)?,
        "read_parquet" => bind_read_parquet_table_function(&bound_args, &qualifier)?,
        "sniff_csv" => bind_sniff_csv_table_function(&bound_args, &qualifier)?,
        _ => {
            return Err(PlanError::NotSupported(
                "table function (only generate_series, unnest, read_csv, read_parquet, and sniff_csv supported)",
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
        let first_path = expand_file_path_specs("read_parquet", &path_specs)?
            .into_iter()
            .next()
            .expect("path expansion returns at least one file");
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

fn read_parquet_arrow_schema(path: &Path) -> Result<arrow_schema::SchemaRef, PlanError> {
    let file = File::open(path).map_err(|err| {
        PlanError::TypeMismatch(format!(
            "read_parquet cannot open {}: {err}",
            path.display()
        ))
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|err| {
        PlanError::TypeMismatch(format!(
            "read_parquet cannot inspect {}: {err}",
            path.display()
        ))
    })?;
    Ok(builder.schema().clone())
}

fn read_parquet_object_schema(patterns: &[String]) -> Result<arrow_schema::SchemaRef, PlanError> {
    let (location, bytes) = read_first_object_bytes(patterns)
        .map_err(|err| PlanError::TypeMismatch(format!("read_parquet: {err}")))?;
    let bytes = Bytes::from(bytes);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(|err| {
        PlanError::TypeMismatch(format!(
            "read_parquet cannot inspect {}: {err}",
            location.display_uri()
        ))
    })?;
    Ok(builder.schema().clone())
}

fn parquet_arrow_type_to_sql(data_type: &ArrowDataType) -> Result<DataType, PlanError> {
    match data_type {
        ArrowDataType::Boolean => Ok(DataType::Bool),
        ArrowDataType::Int32 => Ok(DataType::Int32),
        ArrowDataType::Int64 => Ok(DataType::Int64),
        ArrowDataType::Float32 => Ok(DataType::Float32),
        ArrowDataType::Float64 => Ok(DataType::Float64),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Ok(DataType::Text { max_len: None }),
        other => Err(PlanError::TypeMismatch(format!(
            "read_parquet unsupported Arrow type: {other}"
        ))),
    }
}

fn bind_read_csv_table_function(
    bound_args: &[ScalarExpr],
    qualifier: &str,
) -> Result<(Schema, Vec<ScopeEntry>), PlanError> {
    if bound_args.len() != 1 {
        return Err(PlanError::NotSupported(
            "read_csv: expected one path, glob, or path-list argument",
        ));
    }
    let path_specs = read_csv_path_specs(&bound_args[0])?;
    let header = read_csv_header_from_path_specs(&path_specs)?;
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

fn read_csv_header_from_path_specs(path_specs: &[String]) -> Result<Vec<String>, PlanError> {
    if path_specs_use_object_store("read_csv", path_specs)? {
        let (location, bytes) = read_first_object_bytes(path_specs)
            .map_err(|err| PlanError::TypeMismatch(format!("read_csv: {err}")))?;
        let text = String::from_utf8(bytes).map_err(|err| {
            PlanError::TypeMismatch(format!(
                "read_csv: {} is not UTF-8: {err}",
                location.display_uri()
            ))
        })?;
        let data = read_csv_data_from_text(&location.display_uri(), &text)
            .map_err(|err| PlanError::TypeMismatch(format!("read_csv: {err}")))?;
        let header = data.header;
        if header.is_empty() || header.iter().any(String::is_empty) {
            return Err(PlanError::TypeMismatch(format!(
                "read_csv: header contains an empty column name: {}",
                location.display_uri()
            )));
        }
        return Ok(header);
    }
    read_csv_header_from_specs(path_specs)
        .map_err(|err| PlanError::TypeMismatch(format!("read_csv: {err}")))
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
            let schema =
                build_using_schema(left_plan.schema(), right_plan.schema(), &pairs, join_type)?;
            let left_len = left_scope.len();
            let out_scope = merge_scopes(left_scope, right_scope, left_len);
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
    }
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
    let left_names: std::collections::HashSet<String> = left
        .fields()
        .iter()
        .map(|f| f.name.to_ascii_lowercase())
        .collect();
    for f in left.fields() {
        fields.push(f.clone());
    }
    for f in right.fields() {
        let name = if left_names.contains(&f.name.to_ascii_lowercase()) {
            format!("{}_1", f.name)
        } else {
            f.name.clone()
        };
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

    let left_names: std::collections::HashSet<String> = left
        .fields()
        .iter()
        .map(|f| f.name.to_ascii_lowercase())
        .collect();

    let mut fields: Vec<Field> = Vec::with_capacity(left.len() + right.len());
    for f in left.fields() {
        fields.push(Field {
            name: f.name.clone(),
            data_type: f.data_type.clone(),
            nullable: f.nullable || make_left_nullable,
        });
    }
    for f in right.fields() {
        let name = if left_names.contains(&f.name.to_ascii_lowercase()) {
            format!("{}_1", f.name)
        } else {
            f.name.clone()
        };
        fields.push(Field {
            name,
            data_type: f.data_type.clone(),
            nullable: f.nullable || make_right_nullable,
        });
    }
    Schema::new(fields).map_err(|e| PlanError::TypeMismatch(format!("join schema: {e}")))
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
