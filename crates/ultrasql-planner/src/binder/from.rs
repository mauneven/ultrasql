//! FROM clause and JOIN binding. Split out of `binder/mod.rs` to keep each
//! file under the 600-line ceiling.

use std::collections::BTreeMap;
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
use ultrasql_parser::ast::{JoinCondition, JoinOp, JsonTableColumnKind, TableRef, TypeName};

const READ_CSV_HEADER_SAMPLE_BYTES: u64 = 64 * 1024;
const JSON_STREAM_CHUNK_BYTES: u64 = 64 * 1024;

use super::ddl::resolve_type_name;
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
            if bound_args.len() != 2 {
                return Err(PlanError::NotSupported(
                    "jsonb_path_query: expected jsonb document and path",
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
                "table function (only generate_series, unnest, json_each, jsonb_path_query, json_table, read_csv, read_parquet, read_json, read_ndjson, read_arrow, read_iceberg, iceberg_scan, and sniff_csv supported)",
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
        out.push_str("[]");
    }
    out
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
            let display = path.display();
            let file = File::open(path).map_err(|err| {
                PlanError::TypeMismatch(format!("{function_name} cannot open {display}: {err}"))
            })?;
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

    let first_path = expand_file_path_specs("read_arrow", path_specs)?
        .into_iter()
        .next()
        .expect("path expansion returns at least one file");
    let file = File::open(&first_path).map_err(|err| {
        PlanError::TypeMismatch(format!(
            "read_arrow cannot open {}: {err}",
            first_path.display()
        ))
    })?;
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
        let paths = expand_file_path_specs("read_csv", path_specs)?;
        let first = paths
            .first()
            .expect("expand_file_path_specs returns non-empty paths");
        let display = first.display().to_string();
        let bytes = fs::read(first).map_err(|err| {
            PlanError::TypeMismatch(format!("read_csv cannot read {display}: {err}"))
        })?;
        (display, bytes)
    };
    let text = String::from_utf8(bytes).map_err(|err| {
        PlanError::TypeMismatch(format!("read_csv: {display} is not UTF-8: {err}"))
    })?;
    infer_csv_header_from_first_record(&display, &text)
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
