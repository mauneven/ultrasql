//! Unified executor for external file table functions.
//!
//! Format-specific readers parse CSV, Parquet, JSON, NDJSON, Arrow IPC,
//! and Iceberg metadata into UltraSQL batches. This operator owns the
//! common scan contract seen by the rest of the executor.

use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use arrow_ipc::reader::FileReader as ArrowFileReader;
use serde_json::{Map as JsonMap, Value as JsonValue};
use ultrasql_arrow::{record_batch_to_ultrasql_batch, schema_from_arrow};
use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_executor::{Eval, ExecError, MemTableScan, Operator};
use ultrasql_iceberg::plan_iceberg_scan;
use ultrasql_objectstore::{expand_object_store_specs, is_object_store_uri, read_object_bytes};
use ultrasql_planner::ScalarExpr;
use ultrasql_vec::Batch;
use ultrasql_vec::Bitmap;
use ultrasql_vec::column::{BoolColumn, Column, NumericColumn, StringColumn};

use crate::error::ServerError;

use super::csv_scan::CsvTableScan;
use super::parquet_scan::{ParquetPredicate, ParquetTableScan};

const EXTERNAL_BATCH_TARGET_ROWS: usize = 4096;

/// Return true for file-backed table functions lowered through
/// [`ExternalTableScan`].
pub(super) fn is_external_table_function(name: &str) -> bool {
    ExternalTableFormat::from_function_name(name).is_some()
}

/// Lower one supported external table function into the shared scan.
pub(super) fn lower_external_table_scan(
    name: &str,
    args: &[ScalarExpr],
) -> Result<Box<dyn Operator>, ServerError> {
    let Some(format) = ExternalTableFormat::from_function_name(name) else {
        return Err(ServerError::Unsupported(
            "external table function name is not supported",
        ));
    };
    let scan = match format {
        ExternalTableFormat::Csv => ExternalTableScan::from_csv(args)?,
        ExternalTableFormat::Parquet => ExternalTableScan::from_parquet(args)?,
        ExternalTableFormat::Json => ExternalTableScan::from_json(args, JsonInputKind::Json)?,
        ExternalTableFormat::Ndjson => ExternalTableScan::from_json(args, JsonInputKind::Ndjson)?,
        ExternalTableFormat::Arrow => ExternalTableScan::from_arrow(args)?,
        ExternalTableFormat::Iceberg => ExternalTableScan::from_iceberg(name, args)?,
    };
    Ok(Box::new(scan))
}

/// Lower a Parquet scan with optional projection/predicate pushdown into
/// the shared external scan executor.
pub(super) fn lower_external_parquet_scan(
    path_specs: &[String],
    projection: Option<&[String]>,
    predicate: Option<&ParquetPredicate>,
) -> Result<Box<dyn Operator>, ServerError> {
    Ok(Box::new(ExternalTableScan::from_parquet_path_specs(
        path_specs, projection, predicate,
    )?))
}

/// Evaluate a table-function path argument into one or more path specs.
pub(super) fn read_external_path_specs(
    function_name: &str,
    args: &[ScalarExpr],
) -> Result<Vec<String>, ServerError> {
    if args.len() != 1 {
        return Err(ServerError::CopyFormat(format!(
            "{function_name}: expected one path, glob, or path-list argument"
        )));
    }
    let value = Eval::new(args[0].clone()).eval(&[]).map_err(|err| {
        ServerError::Ddl(format!("{function_name} argument evaluation failed: {err}"))
    })?;
    match value {
        Value::Text(pattern) => Ok(vec![pattern]),
        Value::Array {
            element_type: DataType::Text { max_len: None },
            elements,
        } => elements
            .into_iter()
            .map(|element| match element {
                Value::Text(path) => Ok(path),
                _ => Err(ServerError::CopyFormat(format!(
                    "{function_name}: path-list elements must be string literals"
                ))),
            })
            .collect(),
        _ => Err(ServerError::CopyFormat(format!(
            "{function_name}: argument must be a string literal or text array literal"
        ))),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExternalTableFormat {
    Csv,
    Parquet,
    Json,
    Ndjson,
    Arrow,
    Iceberg,
}

impl ExternalTableFormat {
    fn from_function_name(name: &str) -> Option<Self> {
        match name {
            "read_csv" => Some(Self::Csv),
            "read_parquet" => Some(Self::Parquet),
            "read_json" => Some(Self::Json),
            "read_ndjson" => Some(Self::Ndjson),
            "read_arrow" => Some(Self::Arrow),
            "read_iceberg" | "iceberg_scan" => Some(Self::Iceberg),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JsonInputKind {
    Json,
    Ndjson,
}

impl JsonInputKind {
    const fn function_name(self) -> &'static str {
        match self {
            Self::Json => "read_json",
            Self::Ndjson => "read_ndjson",
        }
    }
}

/// Shared scan node for external table functions.
#[derive(Debug)]
pub(super) struct ExternalTableScan {
    schema: Schema,
    source: ExternalScanSource,
}

#[derive(Debug)]
enum ExternalScanSource {
    Streaming(Box<dyn Operator>),
    Buffered(VecDeque<Batch>),
}

impl ExternalTableScan {
    fn streaming(source: Box<dyn Operator>) -> Self {
        let schema = source.schema().clone();
        Self {
            schema,
            source: ExternalScanSource::Streaming(source),
        }
    }

    fn buffered(schema: Schema, batches: VecDeque<Batch>) -> Self {
        Self {
            schema,
            source: ExternalScanSource::Buffered(batches),
        }
    }

    fn from_csv(args: &[ScalarExpr]) -> Result<Self, ServerError> {
        let path_specs = read_external_path_specs("read_csv", args)?;
        let scan = CsvTableScan::from_path_specs(&path_specs)?;
        Ok(Self::streaming(Box::new(scan)))
    }

    fn from_parquet(args: &[ScalarExpr]) -> Result<Self, ServerError> {
        let path_specs = read_external_path_specs("read_parquet", args)?;
        Self::from_parquet_path_specs(&path_specs, None, None)
    }

    fn from_parquet_path_specs(
        path_specs: &[String],
        projection: Option<&[String]>,
        predicate: Option<&ParquetPredicate>,
    ) -> Result<Self, ServerError> {
        let scan = ParquetTableScan::from_path_specs(path_specs, projection, predicate)?;
        Ok(Self::streaming(Box::new(scan)))
    }

    fn from_json(args: &[ScalarExpr], kind: JsonInputKind) -> Result<Self, ServerError> {
        let function_name = kind.function_name();
        let path_specs = read_external_path_specs(function_name, args)?;
        let sources = read_external_sources(function_name, &path_specs)?;
        let rows = read_json_rows(kind, &sources)?;
        let columns = infer_json_columns(function_name, &rows)?;
        let schema = json_schema(function_name, &columns)?;
        let batches = json_batches(function_name, &columns, &rows)?;
        Ok(Self::buffered(schema, batches))
    }

    fn from_arrow(args: &[ScalarExpr]) -> Result<Self, ServerError> {
        let path_specs = read_external_path_specs("read_arrow", args)?;
        let sources = read_external_sources("read_arrow", &path_specs)?;
        let (schema, batches) = read_arrow_batches(&sources)?;
        Ok(Self::buffered(schema, batches))
    }

    fn from_iceberg(function_name: &str, args: &[ScalarExpr]) -> Result<Self, ServerError> {
        let path_specs = read_external_path_specs(function_name, args)?;
        let [path] = path_specs.as_slice() else {
            return Err(ServerError::CopyFormat(format!(
                "{function_name}: expected one table root or metadata JSON path argument"
            )));
        };
        let plan = plan_iceberg_scan(path)
            .map_err(|err| ServerError::CopyFormat(format!("{function_name}: {err}")))?;
        let source: Box<dyn Operator> = if plan.data_files.is_empty() {
            Box::new(MemTableScan::new(plan.schema, vec![]))
        } else {
            Box::new(ParquetTableScan::from_path_specs(
                &plan.data_files,
                None,
                None,
            )?)
        };
        Ok(Self::streaming(source))
    }
}

impl Operator for ExternalTableScan {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        match &mut self.source {
            ExternalScanSource::Streaming(source) => source.next_batch(),
            ExternalScanSource::Buffered(batches) => Ok(batches.pop_front()),
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[derive(Clone, Debug)]
struct ExternalBytes {
    display: String,
    bytes: Vec<u8>,
}

fn read_external_sources(
    function_name: &str,
    path_specs: &[String],
) -> Result<Vec<ExternalBytes>, ServerError> {
    if path_specs_use_object_store(function_name, path_specs)? {
        let objects = expand_object_store_specs(path_specs)
            .map_err(|err| ServerError::CopyFormat(format!("{function_name}: {err}")))?;
        return objects
            .into_iter()
            .map(|object| {
                let display = object.display_uri();
                let bytes = read_object_bytes(&object)
                    .map_err(|err| ServerError::CopyFormat(format!("{function_name}: {err}")))?;
                Ok(ExternalBytes { display, bytes })
            })
            .collect();
    }

    let paths = expand_file_path_specs(function_name, path_specs)?;
    paths
        .into_iter()
        .map(|path| {
            let display = path.display().to_string();
            let bytes = fs::read(&path).map_err(|err| {
                ServerError::CopyFormat(format!("{function_name} cannot read {display}: {err}"))
            })?;
            Ok(ExternalBytes { display, bytes })
        })
        .collect()
}

fn path_specs_use_object_store(
    function_name: &str,
    path_specs: &[String],
) -> Result<bool, ServerError> {
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
    Err(ServerError::CopyFormat(format!(
        "{function_name}: cannot mix local and object-store paths"
    )))
}

fn expand_file_path_specs(
    function_name: &str,
    patterns: &[String],
) -> Result<Vec<PathBuf>, ServerError> {
    if patterns.is_empty() {
        return Err(ServerError::CopyFormat(format!(
            "{function_name}: path list cannot be empty"
        )));
    }
    let mut paths = Vec::new();
    for pattern in patterns {
        paths.extend(expand_file_paths(function_name, pattern)?);
    }
    Ok(paths)
}

fn expand_file_paths(function_name: &str, pattern: &str) -> Result<Vec<PathBuf>, ServerError> {
    let path = Path::new(pattern);
    let file_pattern = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            ServerError::CopyFormat(format!(
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
    for entry in fs::read_dir(parent).map_err(|err| {
        ServerError::CopyFormat(format!(
            "{function_name}: cannot read directory {}: {err}",
            parent.display()
        ))
    })? {
        let entry =
            entry.map_err(|err| ServerError::CopyFormat(format!("{function_name}: {err}")))?;
        let Some(name) = entry.file_name().to_str().map(ToOwned::to_owned) else {
            continue;
        };
        if wildcard_match(file_pattern, &name) {
            paths.push(entry.path());
        }
    }
    paths.sort();
    if paths.is_empty() {
        return Err(ServerError::CopyFormat(format!(
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

type JsonObject = JsonMap<String, JsonValue>;

fn read_json_rows(
    kind: JsonInputKind,
    sources: &[ExternalBytes],
) -> Result<Vec<JsonObject>, ServerError> {
    let mut rows = Vec::new();
    for source in sources {
        let text = String::from_utf8(source.bytes.clone()).map_err(|err| {
            ServerError::CopyFormat(format!(
                "{} cannot decode {} as UTF-8: {err}",
                kind.function_name(),
                source.display
            ))
        })?;
        match kind {
            JsonInputKind::Json => rows.extend(parse_json_document(&source.display, &text)?),
            JsonInputKind::Ndjson => rows.extend(parse_ndjson_document(&source.display, &text)?),
        }
    }
    Ok(rows)
}

fn parse_json_document(display: &str, text: &str) -> Result<Vec<JsonObject>, ServerError> {
    let value = serde_json::from_str::<JsonValue>(text)
        .map_err(|err| ServerError::CopyFormat(format!("read_json parse {display}: {err}")))?;
    match value {
        JsonValue::Array(values) => values
            .into_iter()
            .enumerate()
            .map(|(idx, value)| json_value_to_object("read_json", display, idx + 1, value))
            .collect(),
        JsonValue::Object(object) => Ok(vec![object]),
        _ => Err(ServerError::CopyFormat(format!(
            "read_json expected object or array of objects in {display}"
        ))),
    }
}

fn parse_ndjson_document(display: &str, text: &str) -> Result<Vec<JsonObject>, ServerError> {
    let mut rows = Vec::new();
    for (idx, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value = serde_json::from_str::<JsonValue>(line).map_err(|err| {
            ServerError::CopyFormat(format!(
                "read_ndjson parse {} line {}: {err}",
                display,
                idx + 1
            ))
        })?;
        rows.push(json_value_to_object(
            "read_ndjson",
            display,
            idx + 1,
            value,
        )?);
    }
    Ok(rows)
}

fn json_value_to_object(
    function_name: &str,
    display: &str,
    row_number: usize,
    value: JsonValue,
) -> Result<JsonObject, ServerError> {
    match value {
        JsonValue::Object(object) => Ok(object),
        _ => Err(ServerError::CopyFormat(format!(
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
struct JsonColumnSpec {
    name: String,
    kind: JsonColumnKind,
    nullable: bool,
}

fn infer_json_columns(
    function_name: &str,
    rows: &[JsonObject],
) -> Result<Vec<JsonColumnSpec>, ServerError> {
    let mut columns: BTreeMap<String, JsonColumnSpec> = BTreeMap::new();
    let mut present: BTreeMap<String, usize> = BTreeMap::new();
    for row in rows {
        for (name, value) in row {
            if name.is_empty() {
                return Err(ServerError::CopyFormat(format!(
                    "{function_name}: JSON object contains an empty column name"
                )));
            }
            let kind = json_value_kind(value);
            let nullable = value.is_null();
            columns
                .entry(name.clone())
                .and_modify(|spec| {
                    spec.kind = widen_json_kind(spec.kind, kind);
                    spec.nullable |= nullable;
                })
                .or_insert_with(|| JsonColumnSpec {
                    name: name.clone(),
                    kind,
                    nullable,
                });
            *present.entry(name.clone()).or_insert(0) += 1;
        }
    }
    for spec in columns.values_mut() {
        if present.get(&spec.name).copied().unwrap_or(0) < rows.len() {
            spec.nullable = true;
        }
    }
    Ok(columns.into_values().collect())
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

fn json_schema(function_name: &str, columns: &[JsonColumnSpec]) -> Result<Schema, ServerError> {
    let fields = columns
        .iter()
        .map(|column| {
            let data_type = match column.kind {
                JsonColumnKind::Unknown => DataType::Text { max_len: None },
                JsonColumnKind::Bool => DataType::Bool,
                JsonColumnKind::Int64 => DataType::Int64,
                JsonColumnKind::Float64 => DataType::Float64,
                JsonColumnKind::Text => DataType::Text { max_len: None },
            };
            if column.nullable {
                Field::nullable(column.name.clone(), data_type)
            } else {
                Field::required(column.name.clone(), data_type)
            }
        })
        .collect::<Vec<_>>();
    Schema::new(fields)
        .map_err(|err| ServerError::CopyFormat(format!("{function_name} schema: {err}")))
}

fn json_batches(
    function_name: &str,
    columns: &[JsonColumnSpec],
    rows: &[JsonObject],
) -> Result<VecDeque<Batch>, ServerError> {
    let mut batches = VecDeque::new();
    for chunk in rows.chunks(EXTERNAL_BATCH_TARGET_ROWS) {
        let batch = json_batch(function_name, columns, chunk)?;
        if !batch.is_empty() {
            batches.push_back(batch);
        }
    }
    Ok(batches)
}

fn json_batch(
    function_name: &str,
    columns: &[JsonColumnSpec],
    rows: &[JsonObject],
) -> Result<Batch, ServerError> {
    let mut batch_columns = Vec::with_capacity(columns.len());
    for column in columns {
        batch_columns.push(json_column(function_name, column, rows)?);
    }
    Batch::new(batch_columns)
        .map_err(|err| ServerError::CopyFormat(format!("{function_name} batch: {err}")))
}

fn json_column(
    function_name: &str,
    column: &JsonColumnSpec,
    rows: &[JsonObject],
) -> Result<Column, ServerError> {
    let mut validity = Bitmap::new(rows.len(), true);
    match column.kind {
        JsonColumnKind::Unknown | JsonColumnKind::Text => {
            let mut values = Vec::with_capacity(rows.len());
            for (idx, row) in rows.iter().enumerate() {
                match row.get(&column.name) {
                    Some(JsonValue::Null) | None => {
                        values.push(String::new());
                        validity.set(idx, false);
                    }
                    Some(JsonValue::String(value)) => values.push(value.clone()),
                    Some(value) => values.push(value.to_string()),
                }
            }
            string_column(function_name, values, validity)
        }
        JsonColumnKind::Bool => {
            let mut values = Vec::with_capacity(rows.len());
            for (idx, row) in rows.iter().enumerate() {
                if let Some(value) = row.get(&column.name).and_then(JsonValue::as_bool) {
                    values.push(value);
                } else {
                    values.push(false);
                    validity.set(idx, false);
                }
            }
            bool_column(function_name, values, validity)
        }
        JsonColumnKind::Int64 => {
            let mut values = Vec::with_capacity(rows.len());
            for (idx, row) in rows.iter().enumerate() {
                if let Some(value) = row.get(&column.name).and_then(json_i64) {
                    values.push(value);
                } else {
                    values.push(0_i64);
                    validity.set(idx, false);
                }
            }
            i64_column(function_name, values, validity)
        }
        JsonColumnKind::Float64 => {
            let mut values = Vec::with_capacity(rows.len());
            for (idx, row) in rows.iter().enumerate() {
                if let Some(value) = row.get(&column.name).and_then(json_f64) {
                    values.push(value);
                } else {
                    values.push(0.0_f64);
                    validity.set(idx, false);
                }
            }
            f64_column(function_name, values, validity)
        }
    }
}

fn json_i64(value: &JsonValue) -> Option<i64> {
    let number = value.as_number()?;
    number
        .as_i64()
        .or_else(|| number.as_u64().and_then(|value| i64::try_from(value).ok()))
}

fn json_f64(value: &JsonValue) -> Option<f64> {
    value.as_number()?.as_f64()
}

fn bool_column(
    function_name: &str,
    values: Vec<bool>,
    validity: Bitmap,
) -> Result<Column, ServerError> {
    if validity.count_ones() == validity.len() {
        Ok(Column::Bool(BoolColumn::from_data(values)))
    } else {
        BoolColumn::with_nulls(values, validity)
            .map(Column::Bool)
            .map_err(|err| ServerError::CopyFormat(format!("{function_name} bool column: {err}")))
    }
}

fn i64_column(
    function_name: &str,
    values: Vec<i64>,
    validity: Bitmap,
) -> Result<Column, ServerError> {
    if validity.count_ones() == validity.len() {
        Ok(Column::Int64(NumericColumn::from_data(values)))
    } else {
        NumericColumn::with_nulls(values, validity)
            .map(Column::Int64)
            .map_err(|err| ServerError::CopyFormat(format!("{function_name} int64 column: {err}")))
    }
}

fn f64_column(
    function_name: &str,
    values: Vec<f64>,
    validity: Bitmap,
) -> Result<Column, ServerError> {
    if validity.count_ones() == validity.len() {
        Ok(Column::Float64(NumericColumn::from_data(values)))
    } else {
        NumericColumn::with_nulls(values, validity)
            .map(Column::Float64)
            .map_err(|err| {
                ServerError::CopyFormat(format!("{function_name} float64 column: {err}"))
            })
    }
}

fn string_column(
    function_name: &str,
    values: Vec<String>,
    validity: Bitmap,
) -> Result<Column, ServerError> {
    if validity.count_ones() == validity.len() {
        Ok(Column::Utf8(StringColumn::from_data(values)))
    } else {
        StringColumn::with_nulls(values, validity)
            .map(Column::Utf8)
            .map_err(|err| ServerError::CopyFormat(format!("{function_name} text column: {err}")))
    }
}

fn read_arrow_batches(sources: &[ExternalBytes]) -> Result<(Schema, VecDeque<Batch>), ServerError> {
    let mut expected_schema: Option<arrow_schema::SchemaRef> = None;
    let mut batches = VecDeque::new();

    for source in sources {
        let cursor = Cursor::new(source.bytes.clone());
        let reader = ArrowFileReader::try_new(cursor, None).map_err(|err| {
            ServerError::CopyFormat(format!(
                "read_arrow cannot inspect {}: {err}",
                source.display
            ))
        })?;
        let arrow_schema = reader.schema();
        if let Some(expected) = &expected_schema {
            if arrow_schema.as_ref() != expected.as_ref() {
                return Err(ServerError::CopyFormat(format!(
                    "read_arrow schema mismatch in {}",
                    source.display
                )));
            }
        } else {
            expected_schema = Some(arrow_schema);
        }

        for batch in reader {
            let batch = batch.map_err(|err| {
                ServerError::CopyFormat(format!("read_arrow read {}: {err}", source.display))
            })?;
            if batch.num_rows() == 0 {
                continue;
            }
            let batch = record_batch_to_ultrasql_batch(batch).map_err(|err| {
                ServerError::CopyFormat(format!("read_arrow Arrow bridge: {err}"))
            })?;
            batches.push_back(batch);
        }
    }

    let Some(arrow_schema) = expected_schema else {
        return Err(ServerError::CopyFormat(
            "read_arrow path list cannot be empty".to_owned(),
        ));
    };
    let schema = schema_from_arrow(arrow_schema.as_ref())
        .map_err(|err| ServerError::CopyFormat(format!("read_arrow Arrow bridge: {err}")))?;
    Ok((schema, batches))
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    #[derive(Debug)]
    struct CountingScan {
        schema: Schema,
        pulls: Arc<AtomicUsize>,
    }

    impl CountingScan {
        fn new(pulls: Arc<AtomicUsize>) -> Self {
            Self {
                schema: Schema::new([Field::required("id", DataType::Int64)]).expect("test schema"),
                pulls,
            }
        }
    }

    impl Operator for CountingScan {
        fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
            let previous = self.pulls.fetch_add(1, Ordering::SeqCst);
            if previous > 0 {
                return Ok(None);
            }
            Batch::new([Column::Int64(NumericColumn::from_data(vec![1_i64, 2]))])
                .map(Some)
                .map_err(ExecError::from)
        }

        fn schema(&self) -> &Schema {
            &self.schema
        }
    }

    #[test]
    fn streaming_source_is_not_drained_at_construction() {
        let pulls = Arc::new(AtomicUsize::new(0));
        let child = CountingScan::new(Arc::clone(&pulls));
        let mut scan = ExternalTableScan::streaming(Box::new(child));

        assert_eq!(pulls.load(Ordering::SeqCst), 0);
        let batch = scan
            .next_batch()
            .expect("stream next")
            .expect("first batch");
        assert_eq!(batch.rows(), 2);
        assert_eq!(pulls.load(Ordering::SeqCst), 1);
        assert!(scan.next_batch().expect("stream eof").is_none());
    }
}
