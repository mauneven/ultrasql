//! SQL/JSON `JSON_TABLE` lowering.
//!
//! This first slice supports constant JSON input, a row-pattern path,
//! scalar value columns, boolean `EXISTS` columns, and `FOR ORDINALITY`.

use serde_json::Value as JsonValue;
use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_executor::{
    Eval, MemTableScan, Operator,
    json_path::{parse_json_path, select_json_path},
};
use ultrasql_planner::ScalarExpr;
use ultrasql_vec::Batch;
use ultrasql_vec::Bitmap;
use ultrasql_vec::column::{BoolColumn, Column, NumericColumn, StringColumn};

use crate::error::ServerError;

const JSON_TABLE_BATCH_ROWS: usize = 4096;

#[derive(Clone, Debug)]
struct JsonTableColumn {
    name: String,
    kind: JsonTableColumnKind,
}

#[derive(Clone, Debug)]
enum JsonTableColumnKind {
    Ordinality,
    Value {
        data_type: DataType,
        path: Option<String>,
    },
    Exists {
        path: Option<String>,
    },
}

/// Lower a `JSON_TABLE` logical function scan into a memory-backed scan.
pub(super) fn lower_json_table_scan(args: &[ScalarExpr]) -> Result<Box<dyn Operator>, ServerError> {
    if args.len() != 3 {
        return Err(ServerError::Unsupported(
            "JSON_TABLE: expected context, row path, and column spec",
        ));
    }
    let context = Eval::new(args[0].clone())
        .eval(&[])
        .map_err(|err| ServerError::Ddl(format!("JSON_TABLE context evaluation failed: {err}")))?;
    let row_path = eval_text_arg("JSON_TABLE row path", &args[1])?;
    let spec = eval_text_arg("JSON_TABLE column spec", &args[2])?;
    let columns = parse_json_table_columns(&spec)?;
    let schema = json_table_schema(&columns)?;
    let document = parse_json_document(context)?;
    let row_steps = parse_json_path(&row_path)
        .map_err(|err| ServerError::CopyFormat(format!("JSON_TABLE row path: {err}")))?;
    let row_items = select_json_path(&document, &row_steps)
        .map_err(|err| ServerError::CopyFormat(format!("JSON_TABLE row path: {err}")))?;
    let rows = row_items
        .iter()
        .enumerate()
        .map(|(idx, item)| json_table_row(&columns, idx, item))
        .collect::<Result<Vec<_>, ServerError>>()?;
    let batches = rows_to_batches(&schema, &rows)?;
    Ok(Box::new(MemTableScan::new(schema, batches)))
}

fn eval_text_arg(label: &str, expr: &ScalarExpr) -> Result<String, ServerError> {
    match Eval::new(expr.clone())
        .eval(&[])
        .map_err(|err| ServerError::Ddl(format!("{label} evaluation failed: {err}")))?
    {
        Value::Text(value) => Ok(value),
        other => Err(ServerError::CopyFormat(format!(
            "{label}: expected text literal, got {other:?}"
        ))),
    }
}

fn parse_json_document(value: Value) -> Result<JsonValue, ServerError> {
    match value {
        Value::Json(text) | Value::Jsonb(text) | Value::Text(text) => serde_json::from_str(&text)
            .map_err(|err| ServerError::CopyFormat(format!("JSON_TABLE parse context: {err}"))),
        Value::Null => Ok(JsonValue::Null),
        other => Err(ServerError::CopyFormat(format!(
            "JSON_TABLE context must be json/jsonb or text, got {other:?}"
        ))),
    }
}

fn parse_json_table_columns(spec: &str) -> Result<Vec<JsonTableColumn>, ServerError> {
    let spec = serde_json::from_str::<JsonValue>(spec)
        .map_err(|err| ServerError::CopyFormat(format!("JSON_TABLE column spec: {err}")))?;
    let columns = spec
        .get("columns")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| ServerError::CopyFormat("JSON_TABLE column spec missing columns".into()))?;
    columns.iter().map(parse_json_table_column).collect()
}

fn parse_json_table_column(value: &JsonValue) -> Result<JsonTableColumn, ServerError> {
    let name = value
        .get("name")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| ServerError::CopyFormat("JSON_TABLE column missing name".into()))?
        .to_owned();
    let kind = value
        .get("kind")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| ServerError::CopyFormat(format!("JSON_TABLE column {name} missing kind")))?;
    let path = value
        .get("path")
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned);
    let kind = match kind {
        "ordinality" => JsonTableColumnKind::Ordinality,
        "value" => {
            let type_name = value
                .get("type")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| {
                    ServerError::CopyFormat(format!("JSON_TABLE column {name} missing type"))
                })?;
            JsonTableColumnKind::Value {
                data_type: json_table_data_type(type_name)?,
                path,
            }
        }
        "exists" => JsonTableColumnKind::Exists { path },
        other => {
            return Err(ServerError::CopyFormat(format!(
                "JSON_TABLE column {name} has unsupported kind {other}"
            )));
        }
    };
    Ok(JsonTableColumn { name, kind })
}

fn json_table_data_type(type_name: &str) -> Result<DataType, ServerError> {
    if type_name.ends_with("[]") {
        return Err(ServerError::CopyFormat(
            "JSON_TABLE array column types are not supported".to_owned(),
        ));
    }
    let base = type_name
        .split_once('(')
        .map_or(type_name, |(base, _)| base)
        .to_ascii_lowercase();
    match base.as_str() {
        "bool" | "boolean" => Ok(DataType::Bool),
        "smallint" | "int2" => Ok(DataType::Int16),
        "int" | "integer" | "int4" => Ok(DataType::Int32),
        "bigint" | "int8" => Ok(DataType::Int64),
        "float" | "float8" | "double" => Ok(DataType::Float64),
        "text" | "varchar" | "char" | "character" => Ok(DataType::Text { max_len: None }),
        "json" => Ok(DataType::Json),
        "jsonb" => Ok(DataType::Jsonb),
        other => Err(ServerError::CopyFormat(format!(
            "JSON_TABLE column type {other} is not supported"
        ))),
    }
}

fn json_table_schema(columns: &[JsonTableColumn]) -> Result<Schema, ServerError> {
    let fields = columns
        .iter()
        .map(|column| match &column.kind {
            JsonTableColumnKind::Ordinality => {
                Field::required(column.name.clone(), DataType::Int64)
            }
            JsonTableColumnKind::Value { data_type, .. } => {
                Field::nullable(column.name.clone(), data_type.clone())
            }
            JsonTableColumnKind::Exists { .. } => {
                Field::required(column.name.clone(), DataType::Bool)
            }
        })
        .collect::<Vec<_>>();
    Schema::new(fields).map_err(|err| ServerError::CopyFormat(format!("JSON_TABLE schema: {err}")))
}

fn json_table_row(
    columns: &[JsonTableColumn],
    ordinal_zero: usize,
    item: &JsonValue,
) -> Result<Vec<Value>, ServerError> {
    let mut row = Vec::with_capacity(columns.len());
    for column in columns {
        match &column.kind {
            JsonTableColumnKind::Ordinality => {
                let ord = i64::try_from(ordinal_zero + 1).map_err(|_| {
                    ServerError::CopyFormat("JSON_TABLE ordinality overflow".to_owned())
                })?;
                row.push(Value::Int64(ord));
            }
            JsonTableColumnKind::Exists { path } => {
                let path = path
                    .as_deref()
                    .map_or_else(|| default_column_path(&column.name), ToOwned::to_owned);
                let steps = parse_json_path(&path).map_err(|err| {
                    ServerError::CopyFormat(format!("JSON_TABLE column path {path}: {err}"))
                })?;
                let selected = select_json_path(item, &steps).map_err(|err| {
                    ServerError::CopyFormat(format!("JSON_TABLE column path {path}: {err}"))
                })?;
                row.push(Value::Bool(!selected.is_empty()));
            }
            JsonTableColumnKind::Value { data_type, path } => {
                let path = path
                    .as_deref()
                    .map_or_else(|| default_column_path(&column.name), ToOwned::to_owned);
                let steps = parse_json_path(&path).map_err(|err| {
                    ServerError::CopyFormat(format!("JSON_TABLE column path {path}: {err}"))
                })?;
                let selected = select_json_path(item, &steps).map_err(|err| {
                    ServerError::CopyFormat(format!("JSON_TABLE column path {path}: {err}"))
                })?;
                let value = selected
                    .first()
                    .map_or(Ok(Value::Null), |value| json_value_to_sql(value, data_type))?;
                row.push(value);
            }
        }
    }
    Ok(row)
}

fn default_column_path(name: &str) -> String {
    format!("$.{name}")
}

fn json_value_to_sql(value: &JsonValue, data_type: &DataType) -> Result<Value, ServerError> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    match data_type {
        DataType::Bool => Ok(value.as_bool().map_or(Value::Null, Value::Bool)),
        DataType::Int16 => json_i64(value).map_or(Ok(Value::Null), |value| {
            i16::try_from(value)
                .map(Value::Int16)
                .map_err(|err| ServerError::CopyFormat(format!("JSON_TABLE int2: {err}")))
        }),
        DataType::Int32 => json_i64(value).map_or(Ok(Value::Null), |value| {
            i32::try_from(value)
                .map(Value::Int32)
                .map_err(|err| ServerError::CopyFormat(format!("JSON_TABLE int4: {err}")))
        }),
        DataType::Int64 => Ok(json_i64(value).map_or(Value::Null, Value::Int64)),
        DataType::Float64 => Ok(json_f64(value).map_or(Value::Null, Value::Float64)),
        DataType::Text { .. } => Ok(Value::Text(match value {
            JsonValue::String(text) => text.clone(),
            other => other.to_string(),
        })),
        DataType::Json => Ok(Value::Json(value.to_string())),
        DataType::Jsonb => Ok(Value::Jsonb(value.to_string())),
        other => Err(ServerError::CopyFormat(format!(
            "JSON_TABLE cannot project {other:?}"
        ))),
    }
}

fn json_i64(value: &JsonValue) -> Option<i64> {
    match value {
        JsonValue::Number(number) => number
            .as_i64()
            .or_else(|| number.as_u64().and_then(|value| i64::try_from(value).ok())),
        JsonValue::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn json_f64(value: &JsonValue) -> Option<f64> {
    match value {
        JsonValue::Number(number) => number.as_f64(),
        JsonValue::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn rows_to_batches(schema: &Schema, rows: &[Vec<Value>]) -> Result<Vec<Batch>, ServerError> {
    let mut batches = Vec::new();
    for chunk in rows.chunks(JSON_TABLE_BATCH_ROWS) {
        batches.push(rows_to_batch(schema, chunk)?);
    }
    Ok(batches)
}

fn rows_to_batch(schema: &Schema, rows: &[Vec<Value>]) -> Result<Batch, ServerError> {
    let columns = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| values_to_column(rows, idx, &field.data_type))
        .collect::<Result<Vec<_>, ServerError>>()?;
    Batch::new(columns).map_err(|err| ServerError::CopyFormat(format!("JSON_TABLE batch: {err}")))
}

fn values_to_column(
    rows: &[Vec<Value>],
    idx: usize,
    data_type: &DataType,
) -> Result<Column, ServerError> {
    match data_type {
        DataType::Bool => {
            let mut values = Vec::with_capacity(rows.len());
            let mut validity = Bitmap::new(rows.len(), true);
            for (row_idx, row) in rows.iter().enumerate() {
                match row.get(idx) {
                    Some(Value::Bool(value)) => values.push(*value),
                    Some(Value::Null) | None => {
                        values.push(false);
                        validity.set(row_idx, false);
                    }
                    other => return Err(type_mismatch(idx, "bool", other)),
                }
            }
            bool_column(values, validity)
        }
        DataType::Int16 | DataType::Int32 => {
            let mut values = Vec::with_capacity(rows.len());
            let mut validity = Bitmap::new(rows.len(), true);
            for (row_idx, row) in rows.iter().enumerate() {
                match row.get(idx) {
                    Some(Value::Int16(value)) => values.push(i32::from(*value)),
                    Some(Value::Int32(value)) => values.push(*value),
                    Some(Value::Null) | None => {
                        values.push(0);
                        validity.set(row_idx, false);
                    }
                    other => return Err(type_mismatch(idx, "int4", other)),
                }
            }
            i32_column(values, validity)
        }
        DataType::Int64 => {
            let mut values = Vec::with_capacity(rows.len());
            let mut validity = Bitmap::new(rows.len(), true);
            for (row_idx, row) in rows.iter().enumerate() {
                match row.get(idx) {
                    Some(Value::Int64(value)) => values.push(*value),
                    Some(Value::Null) | None => {
                        values.push(0_i64);
                        validity.set(row_idx, false);
                    }
                    other => return Err(type_mismatch(idx, "int8", other)),
                }
            }
            i64_column(values, validity)
        }
        DataType::Float64 => {
            let mut values = Vec::with_capacity(rows.len());
            let mut validity = Bitmap::new(rows.len(), true);
            for (row_idx, row) in rows.iter().enumerate() {
                match row.get(idx) {
                    Some(Value::Float64(value)) => values.push(*value),
                    Some(Value::Null) | None => {
                        values.push(0.0_f64);
                        validity.set(row_idx, false);
                    }
                    other => return Err(type_mismatch(idx, "float8", other)),
                }
            }
            f64_column(values, validity)
        }
        DataType::Text { .. } | DataType::Json | DataType::Jsonb => {
            let mut values = Vec::with_capacity(rows.len());
            let mut validity = Bitmap::new(rows.len(), true);
            for (row_idx, row) in rows.iter().enumerate() {
                match row.get(idx) {
                    Some(Value::Text(value) | Value::Json(value) | Value::Jsonb(value)) => {
                        values.push(value.clone());
                    }
                    Some(Value::Null) | None => {
                        values.push(String::new());
                        validity.set(row_idx, false);
                    }
                    other => return Err(type_mismatch(idx, "text", other)),
                }
            }
            string_column(values, validity)
        }
        other => Err(ServerError::CopyFormat(format!(
            "JSON_TABLE column type {other:?} is not supported"
        ))),
    }
}

fn type_mismatch(idx: usize, expected: &str, actual: Option<&Value>) -> ServerError {
    ServerError::CopyFormat(format!(
        "JSON_TABLE column {idx}: expected {expected}, got {actual:?}"
    ))
}

fn bool_column(values: Vec<bool>, validity: Bitmap) -> Result<Column, ServerError> {
    if validity.count_ones() == validity.len() {
        Ok(Column::Bool(BoolColumn::from_data(values)))
    } else {
        BoolColumn::with_nulls(values, validity)
            .map(Column::Bool)
            .map_err(|err| ServerError::CopyFormat(format!("JSON_TABLE bool column: {err}")))
    }
}

fn i32_column(values: Vec<i32>, validity: Bitmap) -> Result<Column, ServerError> {
    if validity.count_ones() == validity.len() {
        Ok(Column::Int32(NumericColumn::from_data(values)))
    } else {
        NumericColumn::with_nulls(values, validity)
            .map(Column::Int32)
            .map_err(|err| ServerError::CopyFormat(format!("JSON_TABLE int4 column: {err}")))
    }
}

fn i64_column(values: Vec<i64>, validity: Bitmap) -> Result<Column, ServerError> {
    if validity.count_ones() == validity.len() {
        Ok(Column::Int64(NumericColumn::from_data(values)))
    } else {
        NumericColumn::with_nulls(values, validity)
            .map(Column::Int64)
            .map_err(|err| ServerError::CopyFormat(format!("JSON_TABLE int8 column: {err}")))
    }
}

fn f64_column(values: Vec<f64>, validity: Bitmap) -> Result<Column, ServerError> {
    if validity.count_ones() == validity.len() {
        Ok(Column::Float64(NumericColumn::from_data(values)))
    } else {
        NumericColumn::with_nulls(values, validity)
            .map(Column::Float64)
            .map_err(|err| ServerError::CopyFormat(format!("JSON_TABLE float8 column: {err}")))
    }
}

fn string_column(values: Vec<String>, validity: Bitmap) -> Result<Column, ServerError> {
    if validity.count_ones() == validity.len() {
        Ok(Column::Utf8(StringColumn::from_data(values)))
    } else {
        StringColumn::with_nulls(values, validity)
            .map(Column::Utf8)
            .map_err(|err| ServerError::CopyFormat(format!("JSON_TABLE text column: {err}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lit(value: Value, data_type: DataType) -> ScalarExpr {
        ScalarExpr::Literal { value, data_type }
    }

    fn lit_text(value: &str) -> ScalarExpr {
        lit(
            Value::Text(value.to_owned()),
            DataType::Text { max_len: None },
        )
    }

    #[test]
    fn json_table_lowers_rows_columns_nulls_and_exists_flags() {
        let context = r#"{
            "items": [
                {"id": "1", "name": "ada", "score": "42.5", "raw": {"k": 1}},
                {"id": 2, "score": 3.5}
            ]
        }"#;
        let spec = r#"{
            "columns": [
                {"name": "ord", "kind": "ordinality"},
                {"name": "id", "kind": "value", "type": "int4", "path": "$.id"},
                {"name": "name", "kind": "value", "type": "text"},
                {"name": "has_score", "kind": "exists", "path": "$.score"},
                {"name": "score", "kind": "value", "type": "float8"},
                {"name": "raw", "kind": "value", "type": "jsonb", "path": "$.raw"}
            ]
        }"#;
        let args = [
            lit(Value::Jsonb(context.to_owned()), DataType::Jsonb),
            lit_text("$.items[*]"),
            lit_text(spec),
        ];

        let mut op = lower_json_table_scan(&args).expect("lower json table");
        assert_eq!(op.schema().len(), 6);
        let batch = op.next_batch().expect("first batch").expect("batch");
        assert_eq!(batch.rows(), 2);
        assert!(op.next_batch().expect("eof").is_none());

        let Column::Int64(ord) = &batch.columns()[0] else {
            panic!("ordinality column");
        };
        assert_eq!(ord.data(), &[1, 2]);
        let Column::Int32(ids) = &batch.columns()[1] else {
            panic!("id column");
        };
        assert_eq!(ids.data(), &[1, 2]);
        assert_eq!(batch.columns()[2].text_value(0), Some("ada"));
        assert_eq!(batch.columns()[2].text_value(1), None);
        let Column::Bool(flags) = &batch.columns()[3] else {
            panic!("exists column");
        };
        assert_eq!(flags.data(), &[1, 1]);
        let Column::Float64(scores) = &batch.columns()[4] else {
            panic!("score column");
        };
        assert_eq!(scores.data(), &[42.5, 3.5]);
        assert_eq!(batch.columns()[5].text_value(0), Some(r#"{"k":1}"#));
        assert_eq!(batch.columns()[5].text_value(1), None);
    }

    #[test]
    fn json_table_rejects_bad_specs_and_bad_values() {
        assert!(lower_json_table_scan(&[]).is_err());
        assert!(parse_json_document(Value::Int32(1)).is_err());
        assert!(parse_json_table_columns("{}").is_err());
        assert!(
            parse_json_table_columns(r#"{"columns":[{"kind":"value","type":"int4"}]}"#).is_err()
        );
        assert!(parse_json_table_columns(r#"{"columns":[{"name":"x","kind":"value"}]}"#).is_err());
        assert!(
            parse_json_table_columns(
                r#"{"columns":[{"name":"x","kind":"value","type":"int4[]"}]}"#
            )
            .is_err()
        );
        assert!(parse_json_table_columns(r#"{"columns":[{"name":"x","kind":"other"}]}"#).is_err());
        assert!(json_value_to_sql(&serde_json::json!(40000), &DataType::Int16).is_err());
        assert!(json_value_to_sql(&serde_json::json!({}), &DataType::Uuid).is_err());
        assert!(json_i64(&serde_json::json!(u64::MAX)).is_none());
        assert!(json_f64(&serde_json::json!("not-a-float")).is_none());
    }

    #[test]
    fn json_table_batch_helpers_cover_nullable_and_mismatch_paths() {
        let schema = Schema::new([
            Field::nullable("b", DataType::Bool),
            Field::nullable("i2", DataType::Int16),
            Field::nullable("i8", DataType::Int64),
            Field::nullable("f8", DataType::Float64),
            Field::nullable("txt", DataType::Text { max_len: None }),
        ])
        .expect("schema");
        let rows = vec![
            vec![
                Value::Bool(true),
                Value::Int16(7),
                Value::Int64(9),
                Value::Float64(1.5),
                Value::Text("x".to_owned()),
            ],
            vec![
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
            ],
        ];
        let batch = rows_to_batch(&schema, &rows).expect("nullable batch");
        assert_eq!(batch.rows(), 2);
        assert_eq!(batch.columns()[0].len(), 2);
        assert_eq!(batch.columns()[4].text_value(1), None);

        assert!(
            values_to_column(&[vec![Value::Text("bad".to_owned())]], 0, &DataType::Bool).is_err()
        );
        assert!(values_to_column(&[vec![Value::Bool(true)]], 0, &DataType::Int64).is_err());
        assert!(values_to_column(&[vec![Value::Int32(1)]], 0, &DataType::Uuid).is_err());
    }
}
