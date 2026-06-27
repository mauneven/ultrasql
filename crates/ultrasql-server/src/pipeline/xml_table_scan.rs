//! SQL/XML `XMLTABLE` lowering.
//!
//! This first slice uses UltraSQL's local XML validator and bounded XPath
//! subset. It supports constant XML input, element row paths, scalar value
//! columns, and `FOR ORDINALITY`.

use serde_json::Value as JsonValue;
use ultrasql_core::{
    DataType, Field, Schema, Value, pack_timetz, parse_date_text, parse_decimal_text,
    parse_money_text, parse_time_text, parse_timestamp_text, parse_timestamptz_text,
    parse_timetz_text, xml_document_is_well_formed, xml_xpath_element_fragments,
};
use ultrasql_executor::{Eval, MemTableScan, Operator};
use ultrasql_planner::ScalarExpr;
use ultrasql_vec::Batch;
use ultrasql_vec::Bitmap;
use ultrasql_vec::column::{BoolColumn, Column, NumericColumn, StringColumn};

use crate::error::ServerError;

const XML_TABLE_BATCH_ROWS: usize = 4096;

#[derive(Clone, Debug)]
struct XmlTableColumn {
    name: String,
    kind: XmlTableColumnKind,
}

#[derive(Clone, Debug)]
enum XmlTableColumnKind {
    Ordinality,
    Value {
        data_type: DataType,
        path: Option<String>,
        default: Option<String>,
    },
}

/// Lower an `XMLTABLE` logical function scan into a memory-backed scan.
pub(super) fn lower_xml_table_scan(args: &[ScalarExpr]) -> Result<Box<dyn Operator>, ServerError> {
    if args.len() != 3 {
        return Err(ServerError::Unsupported(
            "XMLTABLE: expected context, row path, and column spec",
        ));
    }
    let context = Eval::new(args[0].clone())
        .eval(&[])
        .map_err(|err| ServerError::Ddl(format!("XMLTABLE context evaluation failed: {err}")))?;
    let row_path = eval_text_arg("XMLTABLE row path", &args[1])?;
    let spec = eval_text_arg("XMLTABLE column spec", &args[2])?;
    let columns = parse_xml_table_columns(&spec)?;
    let schema = xml_table_schema(&columns)?;
    let Some(document) = xml_document(context)? else {
        return Ok(Box::new(MemTableScan::new(schema, vec![])));
    };
    let row_items = xml_xpath_element_fragments(&row_path, &document).ok_or_else(|| {
        ServerError::CopyFormat(format!("XMLTABLE row path {row_path}: unsupported XPath"))
    })?;
    let rows = row_items
        .iter()
        .enumerate()
        .map(|(idx, item)| xml_table_row(&columns, idx, item))
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

fn xml_document(value: Value) -> Result<Option<String>, ServerError> {
    match value {
        Value::Xml(text) | Value::Text(text) => {
            if xml_document_is_well_formed(&text) {
                Ok(Some(text))
            } else {
                Err(ServerError::CopyFormat(
                    "XMLTABLE context must be a well-formed XML document".to_owned(),
                ))
            }
        }
        Value::Null => Ok(None),
        other => Err(ServerError::CopyFormat(format!(
            "XMLTABLE context must be xml or text, got {other:?}"
        ))),
    }
}

fn parse_xml_table_columns(spec: &str) -> Result<Vec<XmlTableColumn>, ServerError> {
    let spec = serde_json::from_str::<JsonValue>(spec)
        .map_err(|err| ServerError::CopyFormat(format!("XMLTABLE column spec: {err}")))?;
    let columns = spec
        .get("columns")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| ServerError::CopyFormat("XMLTABLE column spec missing columns".into()))?;
    columns.iter().map(parse_xml_table_column).collect()
}

fn parse_xml_table_column(value: &JsonValue) -> Result<XmlTableColumn, ServerError> {
    let name = value
        .get("name")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| ServerError::CopyFormat("XMLTABLE column missing name".into()))?
        .to_owned();
    let kind = value
        .get("kind")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| ServerError::CopyFormat(format!("XMLTABLE column {name} missing kind")))?;
    let path = value
        .get("path")
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned);
    let default = value
        .get("default")
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned);
    let kind = match kind {
        "ordinality" => XmlTableColumnKind::Ordinality,
        "value" => {
            let type_name = value
                .get("type")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| {
                    ServerError::CopyFormat(format!("XMLTABLE column {name} missing type"))
                })?;
            XmlTableColumnKind::Value {
                data_type: xml_table_data_type(type_name)?,
                path,
                default,
            }
        }
        other => {
            return Err(ServerError::CopyFormat(format!(
                "XMLTABLE column {name} has unsupported kind {other}"
            )));
        }
    };
    Ok(XmlTableColumn { name, kind })
}

fn xml_table_data_type(type_name: &str) -> Result<DataType, ServerError> {
    if type_name.ends_with("[]") {
        return Err(ServerError::CopyFormat(
            "XMLTABLE array column types are not supported".to_owned(),
        ));
    }
    let (base, modifiers) = split_xml_table_type(type_name)?;
    match base.as_str() {
        "bool" | "boolean" => Ok(DataType::Bool),
        "smallint" | "int2" => Ok(DataType::Int16),
        "int" | "integer" | "int4" => Ok(DataType::Int32),
        "bigint" | "int8" => Ok(DataType::Int64),
        "float" | "float8" | "double" => Ok(DataType::Float64),
        "numeric" | "decimal" => Ok(DataType::Decimal {
            precision: modifiers.first().copied(),
            scale: modifiers
                .get(1)
                .map(|value| {
                    i32::try_from(*value).map_err(|err| {
                        ServerError::CopyFormat(format!("XMLTABLE numeric scale: {err}"))
                    })
                })
                .transpose()?,
        }),
        "money" => Ok(DataType::Money),
        "date" => Ok(DataType::Date),
        "time" | "time without time zone" => Ok(DataType::Time),
        "timetz" | "time with time zone" => Ok(DataType::TimeTz),
        "timestamp" | "timestamp without time zone" => Ok(DataType::Timestamp),
        "timestamptz" | "timestamp with time zone" => Ok(DataType::TimestampTz),
        "text" | "varchar" | "char" | "character" => Ok(DataType::Text { max_len: None }),
        "xml" => Ok(DataType::Xml),
        other => Err(ServerError::CopyFormat(format!(
            "XMLTABLE column type {other} is not supported"
        ))),
    }
}

fn split_xml_table_type(type_name: &str) -> Result<(String, Vec<u32>), ServerError> {
    let trimmed = type_name.trim();
    let Some((base, rest)) = trimmed.split_once('(') else {
        return Ok((trimmed.to_ascii_lowercase(), Vec::new()));
    };
    let modifiers = rest
        .strip_suffix(')')
        .ok_or_else(|| ServerError::CopyFormat(format!("XMLTABLE type {type_name}: bad typmod")))?
        .split(',')
        .map(|part| {
            part.trim().parse::<u32>().map_err(|err| {
                ServerError::CopyFormat(format!("XMLTABLE type {type_name}: bad typmod: {err}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((base.trim().to_ascii_lowercase(), modifiers))
}

fn xml_table_schema(columns: &[XmlTableColumn]) -> Result<Schema, ServerError> {
    let fields = columns
        .iter()
        .map(|column| match &column.kind {
            XmlTableColumnKind::Ordinality => Field::required(column.name.clone(), DataType::Int64),
            XmlTableColumnKind::Value { data_type, .. } => {
                Field::nullable(column.name.clone(), data_type.clone())
            }
        })
        .collect::<Vec<_>>();
    Schema::new(fields).map_err(|err| ServerError::CopyFormat(format!("XMLTABLE schema: {err}")))
}

fn xml_table_row(
    columns: &[XmlTableColumn],
    ordinal_zero: usize,
    row_fragment: &str,
) -> Result<Vec<Value>, ServerError> {
    if !xml_document_is_well_formed(row_fragment) {
        return Err(ServerError::CopyFormat(
            "XMLTABLE row path must select XML elements in this slice".to_owned(),
        ));
    }
    let mut row = Vec::with_capacity(columns.len());
    for column in columns {
        match &column.kind {
            XmlTableColumnKind::Ordinality => {
                let ord = i64::try_from(ordinal_zero + 1).map_err(|_| {
                    ServerError::CopyFormat("XMLTABLE ordinality overflow".to_owned())
                })?;
                row.push(Value::Int64(ord));
            }
            XmlTableColumnKind::Value {
                data_type,
                path,
                default,
            } => {
                let path = path.as_deref().map_or_else(
                    || default_xml_column_path(&column.name, data_type),
                    ToOwned::to_owned,
                );
                let selected = select_xml_column(row_fragment, &path)?;
                let value = match selected.first() {
                    Some(value) => xml_value_to_sql(value, data_type)?,
                    None => default
                        .as_deref()
                        .map_or(Ok(Value::Null), |value| xml_value_to_sql(value, data_type))?,
                };
                row.push(value);
            }
        }
    }
    Ok(row)
}

fn default_xml_column_path(name: &str, data_type: &DataType) -> String {
    if matches!(data_type, DataType::Xml) {
        name.to_owned()
    } else {
        format!("{name}/text()")
    }
}

fn select_xml_column(row_fragment: &str, path: &str) -> Result<Vec<String>, ServerError> {
    let path = path.trim();
    if path == "." {
        return Ok(vec![row_fragment.to_owned()]);
    }
    if path.is_empty() {
        return Err(ServerError::CopyFormat(
            "XMLTABLE column path must not be empty".to_owned(),
        ));
    }
    let normalized = if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/*/{path}")
    };
    xml_xpath_element_fragments(&normalized, row_fragment).ok_or_else(|| {
        ServerError::CopyFormat(format!("XMLTABLE column path {path}: unsupported XPath"))
    })
}

fn xml_value_to_sql(value: &str, data_type: &DataType) -> Result<Value, ServerError> {
    let trimmed = value.trim();
    match data_type {
        DataType::Bool => Ok(match trimmed.to_ascii_lowercase().as_str() {
            "true" | "t" | "1" => Value::Bool(true),
            "false" | "f" | "0" => Value::Bool(false),
            _ => Value::Null,
        }),
        DataType::Int16 => trimmed
            .parse::<i64>()
            .ok()
            .map_or(Ok(Value::Null), |value| {
                i16::try_from(value)
                    .map(Value::Int16)
                    .map_err(|err| ServerError::CopyFormat(format!("XMLTABLE int2: {err}")))
            }),
        DataType::Int32 => trimmed
            .parse::<i64>()
            .ok()
            .map_or(Ok(Value::Null), |value| {
                i32::try_from(value)
                    .map(Value::Int32)
                    .map_err(|err| ServerError::CopyFormat(format!("XMLTABLE int4: {err}")))
            }),
        DataType::Int64 => Ok(trimmed.parse::<i64>().map_or(Value::Null, Value::Int64)),
        DataType::Float64 => Ok(trimmed.parse::<f64>().map_or(Value::Null, Value::Float64)),
        DataType::Decimal { scale, .. } => Ok(parse_decimal_text(trimmed, *scale)
            .ok()
            .unwrap_or(Value::Null)),
        DataType::Money => Ok(parse_money_text(trimmed).ok().unwrap_or(Value::Null)),
        DataType::Date => Ok(parse_date_text(trimmed).map_or(Value::Null, Value::Date)),
        DataType::Time => Ok(parse_time_text(trimmed).map_or(Value::Null, Value::Time)),
        DataType::TimeTz => Ok(parse_timetz_text(trimmed).map_or(
            Value::Null,
            |(micros, offset_seconds)| Value::TimeTz {
                micros,
                offset_seconds,
            },
        )),
        DataType::Timestamp => {
            Ok(parse_timestamp_text(trimmed).map_or(Value::Null, Value::Timestamp))
        }
        DataType::TimestampTz => {
            Ok(parse_timestamptz_text(trimmed).map_or(Value::Null, Value::TimestampTz))
        }
        DataType::Text { .. } => Ok(Value::Text(value.to_owned())),
        DataType::Xml => {
            if xml_document_is_well_formed(value) {
                Ok(Value::Xml(value.to_owned()))
            } else {
                Err(ServerError::CopyFormat(
                    "XMLTABLE xml column value must be a well-formed XML document".to_owned(),
                ))
            }
        }
        other => Err(ServerError::CopyFormat(format!(
            "XMLTABLE cannot project {other:?}"
        ))),
    }
}

fn rows_to_batches(schema: &Schema, rows: &[Vec<Value>]) -> Result<Vec<Batch>, ServerError> {
    let mut batches = Vec::new();
    for chunk in rows.chunks(XML_TABLE_BATCH_ROWS) {
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
    Batch::new(columns).map_err(|err| ServerError::CopyFormat(format!("XMLTABLE batch: {err}")))
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
        DataType::Date => {
            let mut values = Vec::with_capacity(rows.len());
            let mut validity = Bitmap::new(rows.len(), true);
            for (row_idx, row) in rows.iter().enumerate() {
                match row.get(idx) {
                    Some(Value::Date(value)) => values.push(*value),
                    Some(Value::Null) | None => {
                        values.push(0);
                        validity.set(row_idx, false);
                    }
                    other => return Err(type_mismatch(idx, "date", other)),
                }
            }
            i32_column(values, validity)
        }
        DataType::Decimal { .. } => {
            // Decimal columns materialise as decimal text (i128-backed,
            // lossless) rather than a fixed-width Int64 column.
            let mut values = Vec::with_capacity(rows.len());
            let mut validity = Bitmap::new(rows.len(), true);
            for (row_idx, row) in rows.iter().enumerate() {
                match row.get(idx) {
                    Some(value @ Value::Decimal { .. }) => values.push(value.to_string()),
                    Some(Value::Null) | None => {
                        values.push(String::new());
                        validity.set(row_idx, false);
                    }
                    other => return Err(type_mismatch(idx, "numeric", other)),
                }
            }
            string_column(values, validity)
        }
        DataType::Money
        | DataType::Time
        | DataType::Timestamp
        | DataType::TimestampTz
        | DataType::TimeTz => {
            let mut values = Vec::with_capacity(rows.len());
            let mut validity = Bitmap::new(rows.len(), true);
            for (row_idx, row) in rows.iter().enumerate() {
                match numeric_payload(row.get(idx), data_type)? {
                    Some(value) => values.push(value),
                    None => {
                        values.push(0_i64);
                        validity.set(row_idx, false);
                    }
                }
            }
            i64_column(values, validity)
        }
        DataType::Text { .. } | DataType::Xml => {
            let mut values = Vec::with_capacity(rows.len());
            let mut validity = Bitmap::new(rows.len(), true);
            for (row_idx, row) in rows.iter().enumerate() {
                match row.get(idx) {
                    Some(Value::Text(value) | Value::Xml(value)) => {
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
            "XMLTABLE column type {other:?} is not supported"
        ))),
    }
}

fn numeric_payload(
    value: Option<&Value>,
    data_type: &DataType,
) -> Result<Option<i64>, ServerError> {
    match (value, data_type) {
        (Some(Value::Money(value)), DataType::Money) => Ok(Some(*value)),
        (Some(Value::Time(value)), DataType::Time) => Ok(Some(*value)),
        (Some(Value::Timestamp(value)), DataType::Timestamp) => Ok(Some(*value)),
        (Some(Value::TimestampTz(value)), DataType::TimestampTz) => Ok(Some(*value)),
        (
            Some(Value::TimeTz {
                micros,
                offset_seconds,
            }),
            DataType::TimeTz,
        ) => pack_timetz(*micros, *offset_seconds)
            .map(Some)
            .ok_or_else(|| ServerError::CopyFormat("XMLTABLE timetz out of range".to_owned())),
        (Some(Value::Null) | None, _) => Ok(None),
        (other, _) => Err(type_mismatch(0, &data_type.to_string(), other)),
    }
}

fn type_mismatch(idx: usize, expected: &str, actual: Option<&Value>) -> ServerError {
    ServerError::CopyFormat(format!(
        "XMLTABLE column {idx}: expected {expected}, got {actual:?}"
    ))
}

fn bool_column(values: Vec<bool>, validity: Bitmap) -> Result<Column, ServerError> {
    if validity.count_ones() == validity.len() {
        Ok(Column::Bool(BoolColumn::from_data(values)))
    } else {
        BoolColumn::with_nulls(values, validity)
            .map(Column::Bool)
            .map_err(|err| ServerError::CopyFormat(format!("XMLTABLE bool column: {err}")))
    }
}

fn i32_column(values: Vec<i32>, validity: Bitmap) -> Result<Column, ServerError> {
    if validity.count_ones() == validity.len() {
        Ok(Column::Int32(NumericColumn::from_data(values)))
    } else {
        NumericColumn::with_nulls(values, validity)
            .map(Column::Int32)
            .map_err(|err| ServerError::CopyFormat(format!("XMLTABLE int4 column: {err}")))
    }
}

fn i64_column(values: Vec<i64>, validity: Bitmap) -> Result<Column, ServerError> {
    if validity.count_ones() == validity.len() {
        Ok(Column::Int64(NumericColumn::from_data(values)))
    } else {
        NumericColumn::with_nulls(values, validity)
            .map(Column::Int64)
            .map_err(|err| ServerError::CopyFormat(format!("XMLTABLE int8 column: {err}")))
    }
}

fn f64_column(values: Vec<f64>, validity: Bitmap) -> Result<Column, ServerError> {
    if validity.count_ones() == validity.len() {
        Ok(Column::Float64(NumericColumn::from_data(values)))
    } else {
        NumericColumn::with_nulls(values, validity)
            .map(Column::Float64)
            .map_err(|err| ServerError::CopyFormat(format!("XMLTABLE float8 column: {err}")))
    }
}

fn string_column(values: Vec<String>, validity: Bitmap) -> Result<Column, ServerError> {
    if validity.count_ones() == validity.len() {
        Ok(Column::Utf8(StringColumn::from_data(values)))
    } else {
        StringColumn::with_nulls(values, validity)
            .map(Column::Utf8)
            .map_err(|err| ServerError::CopyFormat(format!("XMLTABLE text column: {err}")))
    }
}
