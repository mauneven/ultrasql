//! `Vec<Vec<Value>>` → [`Batch`] conversion used by the legacy
//! materialised scan path.
//!
//! The streaming [`SeqScan`](super::SeqScan) no longer uses
//! [`build_batch`]; it decodes tuple payloads directly into typed
//! column builders. This function is kept for callers that still hold a
//! `Vec<Vec<Value>>` and want a [`Batch`].

use ultrasql_core::{DataType, Schema, Value, coerce_bpchar_text, format_interval_pg, pack_timetz};
use ultrasql_vec::bitmap::Bitmap;
use ultrasql_vec::column::{BoolColumn, Column, NumericColumn};
use ultrasql_vec::{Batch, DictionaryEncodingPolicy, StringEncoding, encode_strings_auto};

use crate::ExecError;

/// Convert a slice of decoded rows into a [`Batch`] matching `schema`.
///
/// Kept for backwards compatibility with callers that still want the
/// `Vec<Vec<Value>>` → [`Batch`] path. The streaming [`SeqScan`](super::SeqScan) no
/// longer uses this function.
#[allow(clippy::too_many_lines)]
pub fn build_batch(rows: &[Vec<Value>], schema: &Schema) -> Result<Batch, ExecError> {
    if rows.is_empty() {
        return Batch::new(std::iter::empty::<Column>()).map_err(ExecError::from);
    }

    let n_cols = schema.len();
    let n_rows = rows.len();

    // Helper closure: scan column `col_idx` once and build a validity
    // bitmap (1 = valid, 0 = null per Arrow convention). Returns
    // `None` when no row in this column is null — the column is
    // emitted without a bitmap so kernels keep their fast path.
    let build_validity = |col_idx: usize| -> Option<Bitmap> {
        let mut any_null = false;
        let mut bitmap = Bitmap::new(n_rows, true);
        for (row_idx, row) in rows.iter().enumerate() {
            if matches!(row[col_idx], Value::Null) {
                bitmap.set(row_idx, false);
                any_null = true;
            }
        }
        any_null.then_some(bitmap)
    };

    let mut columns: Vec<Column> = Vec::with_capacity(n_cols);

    for col_idx in 0..n_cols {
        let field = schema.field_at(col_idx);
        let storage_type = field.data_type.storage_type();
        let col = match storage_type {
            DataType::Null => {
                for (row_idx, row) in rows.iter().enumerate() {
                    if !matches!(row[col_idx], Value::Null) {
                        return Err(ExecError::TypeMismatch(format!(
                            "expected NULL at row {row_idx} col {col_idx}, got {:?}",
                            row[col_idx].data_type()
                        )));
                    }
                }
                Column::Int32(
                    NumericColumn::with_nulls(vec![0_i32; n_rows], Bitmap::new(n_rows, false))
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?,
                )
            }
            DataType::Bool => {
                let mut data: Vec<bool> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        Value::Bool(v) => data.push(*v),
                        Value::Null => data.push(false),
                        other => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected Bool at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    }
                }
                let col = if let Some(nulls) = build_validity(col_idx) {
                    BoolColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?
                } else {
                    BoolColumn::from_data(data)
                };
                Column::Bool(col)
            }
            DataType::Int16 => {
                let mut data: Vec<i32> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        Value::Int16(v) => data.push(i32::from(*v)),
                        Value::Null => data.push(0),
                        other => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected Int16 at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    }
                }
                let col = if let Some(nulls) = build_validity(col_idx) {
                    NumericColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?
                } else {
                    NumericColumn::from_data(data)
                };
                Column::Int32(col)
            }
            DataType::Int32 => {
                let mut data: Vec<i32> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        Value::Int32(v) => data.push(*v),
                        Value::Null => data.push(0),
                        other => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected Int32 at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    }
                }
                let col = if let Some(nulls) = build_validity(col_idx) {
                    NumericColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?
                } else {
                    NumericColumn::from_data(data)
                };
                Column::Int32(col)
            }
            DataType::Int64 => {
                let mut data: Vec<i64> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        Value::Int64(v) => data.push(*v),
                        Value::Null => data.push(0),
                        other => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected Int64 at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    }
                }
                let col = if let Some(nulls) = build_validity(col_idx) {
                    NumericColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?
                } else {
                    NumericColumn::from_data(data)
                };
                Column::Int64(col)
            }
            DataType::Oid | DataType::RegClass | DataType::RegType => {
                let mut data: Vec<i64> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    let raw = match (&storage_type, &row[col_idx]) {
                        (DataType::Oid, Value::Oid(v))
                        | (DataType::RegClass, Value::RegClass(v))
                        | (DataType::RegType, Value::RegType(v)) => i64::from(v.raw()),
                        (_, Value::Null) => 0,
                        (_, other) => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected {storage_type} at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    };
                    data.push(raw);
                }
                let col = if let Some(nulls) = build_validity(col_idx) {
                    NumericColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?
                } else {
                    NumericColumn::from_data(data)
                };
                Column::Int64(col)
            }
            DataType::Float32 => {
                let mut data: Vec<f32> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        Value::Float32(v) => data.push(*v),
                        Value::Null => data.push(0.0),
                        other => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected Float32 at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    }
                }
                let col = if let Some(nulls) = build_validity(col_idx) {
                    NumericColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?
                } else {
                    NumericColumn::from_data(data)
                };
                Column::Float32(col)
            }
            DataType::Float64 => {
                let mut data: Vec<f64> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        Value::Float64(v) => data.push(*v),
                        Value::Null => data.push(0.0),
                        other => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected Float64 at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    }
                }
                let col = if let Some(nulls) = build_validity(col_idx) {
                    NumericColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?
                } else {
                    NumericColumn::from_data(data)
                };
                Column::Float64(col)
            }
            DataType::Text { .. }
            | DataType::Enum { .. }
            | DataType::Composite { .. }
            | DataType::Char { .. }
            | DataType::Bit { .. }
            | DataType::VarBit { .. }
            | DataType::Inet
            | DataType::Cidr
            | DataType::MacAddr
            | DataType::MacAddr8
            | DataType::Json
            | DataType::Jsonb
            | DataType::Xml
            | DataType::TsVector
            | DataType::TsQuery
            | DataType::PgLsn
            | DataType::Vector { .. }
            | DataType::HalfVec { .. }
            | DataType::SparseVec { .. }
            | DataType::BitVec { .. }
            | DataType::Range(_)
            | DataType::Geometry(_)
            | DataType::Array(_)
            | DataType::Uuid
            | DataType::Bytea => {
                let mut strings: Vec<Option<String>> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    match (&field.data_type, &row[col_idx]) {
                        (DataType::Text { .. }, Value::Text(s)) => strings.push(Some(s.clone())),
                        (DataType::TsVector | DataType::TsQuery, Value::Text(s)) => {
                            strings.push(Some(s.clone()));
                        }
                        (DataType::Enum { labels, .. }, Value::Text(s))
                            if labels.iter().any(|label| label == s) =>
                        {
                            strings.push(Some(s.clone()));
                        }
                        (DataType::Composite { .. }, Value::Text(s)) => {
                            strings.push(Some(s.clone()));
                        }
                        (DataType::Char { .. }, Value::Char(s)) => strings.push(Some(s.clone())),
                        (DataType::Char { len }, Value::Text(s)) => {
                            let coerced = coerce_bpchar_text(s, *len, false).map_err(|err| {
                                ExecError::StringDataRightTruncation(err.to_string())
                            })?;
                            strings.push(Some(coerced));
                        }
                        (DataType::Json, Value::Json(s))
                        | (DataType::Jsonb, Value::Jsonb(s))
                        | (DataType::Xml, Value::Xml(s)) => strings.push(Some(s.clone())),
                        (DataType::PgLsn, Value::PgLsn(lsn)) => {
                            strings.push(Some(lsn.to_string()));
                        }
                        (
                            DataType::Bit { .. } | DataType::VarBit { .. },
                            Value::BitString(bits),
                        ) if bits.matches_type(&field.data_type) => {
                            strings.push(Some(bits.to_string()));
                        }
                        (
                            DataType::Bit { .. } | DataType::VarBit { .. },
                            Value::BitString(bits),
                        ) => {
                            return Err(ExecError::StringDataRightTruncation(format!(
                                "bit string length {} does not match type {}",
                                bits.len(),
                                field.data_type
                            )));
                        }
                        (
                            DataType::Inet
                            | DataType::Cidr
                            | DataType::MacAddr
                            | DataType::MacAddr8,
                            Value::Network(network),
                        ) if network.data_type() == field.data_type => {
                            strings.push(Some(network.to_string()));
                        }
                        (DataType::Vector { dims }, Value::Vector(values))
                            if dims.is_none() || u32::try_from(values.len()).ok() == *dims =>
                        {
                            strings.push(Some(row[col_idx].to_string()));
                        }
                        (expected, value)
                            if expected.is_vector_family()
                                && vector_family_value_matches(expected, value) =>
                        {
                            strings.push(Some(row[col_idx].to_string()));
                        }
                        (DataType::Range(expected), Value::Range(v))
                            if expected == &v.range_type =>
                        {
                            strings.push(Some(v.to_string()));
                        }
                        (DataType::Geometry(expected), Value::Geometry(v))
                            if expected == &v.geometry_type =>
                        {
                            strings.push(Some(v.to_string()));
                        }
                        (DataType::Array(expected), Value::Array { element_type, .. })
                            if expected.as_ref() == element_type =>
                        {
                            strings.push(Some(row[col_idx].to_string()));
                        }
                        (DataType::Uuid, Value::Uuid(v)) => {
                            strings.push(Some(Value::Uuid(*v).to_string()));
                        }
                        (DataType::Bytea, Value::Bytea(v)) => {
                            strings.push(Some(Value::Bytea(v.clone()).to_string()));
                        }
                        (_, Value::Null) => strings.push(None),
                        (_, other) => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected {} at row {row_idx} col {col_idx}, got {:?}",
                                field.data_type,
                                other.data_type()
                            )));
                        }
                    }
                }
                match encode_strings_auto(
                    strings.iter().map(|v| v.as_deref()),
                    DictionaryEncodingPolicy::default(),
                ) {
                    StringEncoding::Raw(c) => Column::Utf8(c),
                    StringEncoding::Dictionary(c) => Column::DictionaryUtf8(c),
                }
            }
            DataType::Date => {
                // Date values share the Int32 batch column: the
                // storage is the same 4-byte little-endian payload
                // (days since 2000-01-01). The schema field still
                // reports `DataType::Date` so downstream operators
                // that care about date semantics keep the type tag.
                let mut data: Vec<i32> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        Value::Date(v) => data.push(*v),
                        Value::Null => data.push(0),
                        other => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected Date at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    }
                }
                let col = if let Some(nulls) = build_validity(col_idx) {
                    NumericColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?
                } else {
                    NumericColumn::from_data(data)
                };
                Column::Int32(col)
            }
            DataType::Decimal { .. } => {
                // Decimal columns materialise as decimal text so the full
                // i128-backed mantissa (~38 digits) round-trips losslessly
                // through the batch; a fixed-width Int64 batch column would
                // silently truncate values beyond i64.
                let mut strings: Vec<Option<String>> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        Value::Decimal { .. } => strings.push(Some(row[col_idx].to_string())),
                        Value::Null => strings.push(None),
                        other => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected Decimal at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    }
                }
                match encode_strings_auto(
                    strings.iter().map(|value| value.as_deref()),
                    DictionaryEncodingPolicy::default(),
                ) {
                    StringEncoding::Raw(c) => Column::Utf8(c),
                    StringEncoding::Dictionary(c) => Column::DictionaryUtf8(c),
                }
            }
            DataType::Money
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::Time
            | DataType::TimeTz => {
                // Money / Timestamp / Time values share the Int64
                // batch column. Schema field carries the semantic tag.
                let mut data: Vec<i64> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    let v_i64 = match &row[col_idx] {
                        Value::Money(v) => *v,
                        Value::Timestamp(v) | Value::TimestampTz(v) | Value::Time(v) => *v,
                        Value::TimeTz {
                            micros,
                            offset_seconds,
                        } => pack_timetz(*micros, *offset_seconds).ok_or_else(|| {
                            ExecError::TypeMismatch(format!(
                                "invalid TimeTz at row {row_idx} col {col_idx}"
                            ))
                        })?,
                        Value::Int16(v) => i64::from(*v),
                        Value::Int32(v) => i64::from(*v),
                        Value::Int64(v) => *v,
                        Value::Null => 0,
                        other => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected Decimal/Money/Timestamp/Time at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    };
                    data.push(v_i64);
                }
                let col = if let Some(nulls) = build_validity(col_idx) {
                    NumericColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?
                } else {
                    NumericColumn::from_data(data)
                };
                Column::Int64(col)
            }
            DataType::Interval => {
                // Interval columns materialise as PostgreSQL-canonical text so
                // the full month/day/microsecond triple round-trips through the
                // batch, mirroring the streaming row-codec column builder. The
                // schema field carries the `DataType::Interval` tag so the wire
                // OID (1186) is preserved and `batch_to_rows` re-parses the text
                // back into a `Value::Interval`.
                let mut strings: Vec<Option<String>> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        Value::Interval {
                            months,
                            days,
                            microseconds,
                        } => strings.push(Some(format_interval_pg(*months, *days, *microseconds))),
                        Value::Null => strings.push(None),
                        other => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected Interval at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    }
                }
                match encode_strings_auto(
                    strings.iter().map(|value| value.as_deref()),
                    DictionaryEncodingPolicy::default(),
                ) {
                    StringEncoding::Raw(c) => Column::Utf8(c),
                    StringEncoding::Dictionary(c) => Column::DictionaryUtf8(c),
                }
            }
            other => {
                return Err(ExecError::TypeMismatch(format!(
                    "SeqScan: unsupported column type {other} for batch building"
                )));
            }
        };
        columns.push(col);
    }

    Batch::new(columns).map_err(ExecError::from)
}

fn vector_family_value_matches(expected: &DataType, value: &Value) -> bool {
    let actual = value.data_type();
    vector_family_kind(expected) == vector_family_kind(&actual)
        && dims_compatible(
            expected.vector_dims().flatten(),
            actual.vector_dims().flatten(),
        )
}

fn vector_family_kind(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Vector { .. } => Some(0),
        DataType::HalfVec { .. } => Some(1),
        DataType::SparseVec { .. } => Some(2),
        DataType::BitVec { .. } => Some(3),
        _ => None,
    }
}

const fn dims_compatible(left: Option<u32>, right: Option<u32>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left == right,
        _ => true,
    }
}
