//! Batch-to-rows decoding for the [`Filter`](super::Filter)
//! row-at-a-time fallback.
//!
//! [`batch_to_rows`] is the inverse of `build_batch`: it reconstructs
//! the row representation from the columnar batch so the scalar
//! interpreter can apply the predicate per row.

use ultrasql_core::{
    DataType, GeometryValue, Oid, RangeValue, Schema, Value, parse_decimal_text, unpack_timetz,
};
use ultrasql_vec::Batch;
use ultrasql_vec::column::Column;

use crate::ExecError;

/// Decode a [`Batch`] into a `Vec` of rows (each row is a `Vec<Value>`).
///
/// This is the inverse of `build_batch`: it reconstructs the row-at-a-time
/// representation from the columnar batch. Each column is decoded into the
/// corresponding `Value` variant; NULL cells use `Value::Null` (the
/// `BoolColumn` and numeric columns use a sentinel zero for NULL which is
/// re-encoded here as `Value::Null` only when the schema field is nullable
/// and the value equals the sentinel — for v0.5 simplicity we keep the
/// sentinel as-is since nullability is represented in the batch validity
/// bitmaps in future work; for now the filter treats the sentinel as a
/// non-null value).
///
/// For v0.5 this is a pure column-to-value decode without bitmap support.
#[allow(unreachable_pub)]
pub fn batch_to_rows(batch: &Batch, schema: &Schema) -> Result<Vec<Vec<Value>>, ExecError> {
    let n_rows = batch.rows();
    let n_cols = schema.len();
    let cols = batch.columns();

    if cols.len() != n_cols {
        return Err(ExecError::TypeMismatch(format!(
            "batch has {} columns but schema has {}",
            cols.len(),
            n_cols,
        )));
    }

    let mut rows: Vec<Vec<Value>> = (0..n_rows).map(|_| Vec::with_capacity(n_cols)).collect();

    for (col_idx, (col, field)) in cols.iter().zip(schema.fields().iter()).enumerate() {
        // Validity convention: 1 = valid, 0 = null. `is_null(i)` returns
        // `true` when the bitmap exists and the bit is unset.
        let is_null = |nulls: Option<&ultrasql_vec::bitmap::Bitmap>, i: usize| -> bool {
            nulls.is_some_and(|b| !b.get(i))
        };
        let column_is_null = |column: &Column, i: usize| -> bool {
            match column {
                Column::Int32(c) => is_null(c.nulls(), i),
                Column::Int64(c) => is_null(c.nulls(), i),
                Column::Float32(c) => is_null(c.nulls(), i),
                Column::Float64(c) => is_null(c.nulls(), i),
                Column::Bool(c) => is_null(c.nulls(), i),
                Column::Utf8(c) => is_null(c.nulls(), i),
                Column::DictionaryUtf8(c) => is_null(c.codes.nulls(), i),
            }
        };
        let storage_type = field.data_type.storage_type();
        match (col, storage_type) {
            (_, DataType::Null) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if column_is_null(col, row_idx) {
                        row.push(Value::Null);
                    } else {
                        return Err(ExecError::TypeMismatch(format!(
                            "column {col_idx} ({name}): expected NULL at row {row_idx}",
                            name = field.name,
                        )));
                    }
                }
            }
            (Column::Int32(c), DataType::Int16) => {
                let nulls = c.nulls();
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if is_null(nulls, row_idx) {
                        row.push(Value::Null);
                    } else {
                        let v = i16::try_from(c.data()[row_idx]).map_err(|_| {
                            ExecError::TypeMismatch(format!(
                                "column {col_idx} ({name}): Int16 value out of range",
                                name = field.name,
                            ))
                        })?;
                        row.push(Value::Int16(v));
                    }
                }
            }
            (Column::Int32(c), DataType::Int32) => {
                let nulls = c.nulls();
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if is_null(nulls, row_idx) {
                        row.push(Value::Null);
                    } else {
                        row.push(Value::Int32(c.data()[row_idx]));
                    }
                }
            }
            (Column::Int64(c), DataType::Int64) => {
                let nulls = c.nulls();
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if is_null(nulls, row_idx) {
                        row.push(Value::Null);
                    } else {
                        row.push(Value::Int64(c.data()[row_idx]));
                    }
                }
            }
            (Column::Int64(c), DataType::Money) => {
                let nulls = c.nulls();
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if is_null(nulls, row_idx) {
                        row.push(Value::Null);
                    } else {
                        row.push(Value::Money(c.data()[row_idx]));
                    }
                }
            }
            (Column::Int64(c), DataType::Oid | DataType::RegClass | DataType::RegType) => {
                let nulls = c.nulls();
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if is_null(nulls, row_idx) {
                        row.push(Value::Null);
                    } else {
                        let raw = u32::try_from(c.data()[row_idx]).map_err(|_| {
                            ExecError::TypeMismatch(format!(
                                "column {col_idx} ({name}): OID value out of range",
                                name = field.name,
                            ))
                        })?;
                        let oid = Oid::new(raw);
                        row.push(match storage_type {
                            DataType::Oid => Value::Oid(oid),
                            DataType::RegClass => Value::RegClass(oid),
                            DataType::RegType => Value::RegType(oid),
                            _ => unreachable!(),
                        });
                    }
                }
            }
            (Column::Float32(c), DataType::Float32) => {
                let nulls = c.nulls();
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if is_null(nulls, row_idx) {
                        row.push(Value::Null);
                    } else {
                        row.push(Value::Float32(c.data()[row_idx]));
                    }
                }
            }
            (Column::Float64(c), DataType::Float64) => {
                let nulls = c.nulls();
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if is_null(nulls, row_idx) {
                        row.push(Value::Null);
                    } else {
                        row.push(Value::Float64(c.data()[row_idx]));
                    }
                }
            }
            (Column::Bool(c), DataType::Bool) => {
                let nulls = c.nulls();
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if is_null(nulls, row_idx) {
                        row.push(Value::Null);
                    } else {
                        row.push(Value::Bool(c.value(row_idx)));
                    }
                }
            }
            (Column::Utf8(_) | Column::DictionaryUtf8(_), DataType::Text { .. }) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => row.push(Value::Text(v.to_owned())),
                        None => row.push(Value::Null),
                    }
                }
            }
            (Column::Utf8(_) | Column::DictionaryUtf8(_), DataType::Enum { .. }) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => row.push(Value::Text(v.to_owned())),
                        None => row.push(Value::Null),
                    }
                }
            }
            (Column::Utf8(_) | Column::DictionaryUtf8(_), DataType::Composite { .. }) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => row.push(Value::Text(v.to_owned())),
                        None => row.push(Value::Null),
                    }
                }
            }
            (Column::Utf8(_) | Column::DictionaryUtf8(_), DataType::Char { .. }) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => row.push(Value::Char(v.to_owned())),
                        None => row.push(Value::Null),
                    }
                }
            }
            (
                Column::Utf8(_) | Column::DictionaryUtf8(_),
                DataType::Bit { .. } | DataType::VarBit { .. },
            ) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => row.push(parse_bit_string_text_cell(
                            v,
                            &field.data_type,
                            col_idx,
                            field.name.as_str(),
                        )?),
                        None => row.push(Value::Null),
                    }
                }
            }
            (
                Column::Utf8(_) | Column::DictionaryUtf8(_),
                DataType::Inet | DataType::Cidr | DataType::MacAddr | DataType::MacAddr8,
            ) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => {
                            row
                                .push(Value::parse_network(&field.data_type, v).ok_or_else(|| {
                                ExecError::TypeMismatch(format!(
                                    "column {col_idx} ({name}): invalid {expected_type} literal",
                                    name = field.name,
                                    expected_type = field.data_type,
                                ))
                            })?)
                        }
                        None => row.push(Value::Null),
                    }
                }
            }
            (Column::Utf8(_) | Column::DictionaryUtf8(_), DataType::Json) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => row.push(Value::Json(v.to_owned())),
                        None => row.push(Value::Null),
                    }
                }
            }
            (Column::Utf8(_) | Column::DictionaryUtf8(_), DataType::Jsonb) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => row.push(Value::Jsonb(v.to_owned())),
                        None => row.push(Value::Null),
                    }
                }
            }
            (Column::Utf8(_) | Column::DictionaryUtf8(_), DataType::Xml) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => row.push(Value::Xml(v.to_owned())),
                        None => row.push(Value::Null),
                    }
                }
            }
            (Column::Utf8(_) | Column::DictionaryUtf8(_), DataType::PgLsn) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => {
                            let lsn = Value::parse_pg_lsn_text(v).ok_or_else(|| {
                                ExecError::TypeMismatch(format!(
                                    "column {col_idx} ({name}): invalid pg_lsn literal",
                                    name = field.name,
                                ))
                            })?;
                            row.push(Value::PgLsn(lsn));
                        }
                        None => row.push(Value::Null),
                    }
                }
            }
            (Column::Utf8(_) | Column::DictionaryUtf8(_), DataType::Vector { dims }) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => {
                            let vector = Value::parse_vector(v).ok_or_else(|| {
                                ExecError::TypeMismatch(format!(
                                    "column {col_idx} ({name}): invalid {expected_type} literal",
                                    name = field.name,
                                    expected_type = field.data_type,
                                ))
                            })?;
                            if let Value::Vector(values) = &vector
                                && dims.is_some_and(|expected| {
                                    u32::try_from(values.len()).ok() != Some(expected)
                                })
                            {
                                return Err(ExecError::TypeMismatch(format!(
                                    "column {col_idx} ({name}): invalid {expected_type} dimension",
                                    name = field.name,
                                    expected_type = field.data_type,
                                )));
                            }
                            row.push(vector);
                        }
                        None => row.push(Value::Null),
                    }
                }
            }
            (Column::Utf8(_) | Column::DictionaryUtf8(_), ty) if ty.is_vector_family() => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => row.push(parse_vector_family_text_cell(
                            v,
                            ty,
                            col_idx,
                            field.name.as_str(),
                        )?),
                        None => row.push(Value::Null),
                    }
                }
            }
            (Column::Utf8(_) | Column::DictionaryUtf8(_), DataType::Range(range_type)) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => {
                            let range = RangeValue::parse(*range_type, v).ok_or_else(|| {
                                ExecError::TypeMismatch(format!(
                                    "column {col_idx} ({name}): invalid {expected_type} literal",
                                    name = field.name,
                                    expected_type = field.data_type,
                                ))
                            })?;
                            row.push(Value::Range(range));
                        }
                        None => row.push(Value::Null),
                    }
                }
            }
            (Column::Utf8(_) | Column::DictionaryUtf8(_), DataType::Array(element_type)) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => {
                            let array =
                                Value::parse_array((**element_type).clone(), v).ok_or_else(
                                    || {
                                        ExecError::TypeMismatch(format!(
                                            "column {col_idx} ({name}): invalid {expected_type} literal",
                                            name = field.name,
                                            expected_type = field.data_type,
                                        ))
                                    },
                                )?;
                            row.push(array);
                        }
                        None => row.push(Value::Null),
                    }
                }
            }
            (Column::Utf8(_) | Column::DictionaryUtf8(_), DataType::Geometry(geometry_type)) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => {
                            let geometry =
                                GeometryValue::parse(*geometry_type, v).ok_or_else(|| {
                                    ExecError::TypeMismatch(format!(
                                        "column {col_idx} ({name}): invalid {expected_type} literal",
                                        name = field.name,
                                        expected_type = field.data_type,
                                    ))
                                })?;
                            row.push(Value::Geometry(geometry));
                        }
                        None => row.push(Value::Null),
                    }
                }
            }
            (Column::Utf8(_) | Column::DictionaryUtf8(_), DataType::Uuid) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => {
                            let uuid = Value::parse_uuid(v).ok_or_else(|| {
                                ExecError::TypeMismatch(format!(
                                    "column {col_idx} ({name}): invalid uuid literal",
                                    name = field.name,
                                ))
                            })?;
                            row.push(Value::Uuid(uuid));
                        }
                        None => row.push(Value::Null),
                    }
                }
            }
            (Column::Utf8(_) | Column::DictionaryUtf8(_), DataType::Bytea) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => {
                            let bytes = Value::parse_bytea(v).ok_or_else(|| {
                                ExecError::TypeMismatch(format!(
                                    "column {col_idx} ({name}): invalid bytea literal",
                                    name = field.name,
                                ))
                            })?;
                            row.push(Value::Bytea(bytes));
                        }
                        None => row.push(Value::Null),
                    }
                }
            }
            (Column::Int32(c), DataType::Date) => {
                // Date columns store as `Int32` (days since
                // 2000-01-01). The row materialiser re-tags the value
                // as `Value::Date` so downstream operators that
                // pattern-match on `Value` see the date semantics.
                let nulls = c.nulls();
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if is_null(nulls, row_idx) {
                        row.push(Value::Null);
                    } else {
                        row.push(Value::Date(c.data()[row_idx]));
                    }
                }
            }
            (Column::Utf8(_) | Column::DictionaryUtf8(_), DataType::Decimal { .. }) => {
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if column_is_null(col, row_idx) {
                        row.push(Value::Null);
                        continue;
                    }
                    let text = col.text_value(row_idx).ok_or_else(|| {
                        ExecError::TypeMismatch(format!(
                            "column {col_idx} ({name}): missing dynamic numeric text at row {row_idx}",
                            name = field.name,
                        ))
                    })?;
                    let value = parse_decimal_text(text, None).map_err(|err| {
                        ExecError::TypeMismatch(format!(
                            "column {col_idx} ({name}): invalid dynamic numeric text {text:?}: {err}",
                            name = field.name,
                        ))
                    })?;
                    row.push(value);
                }
            }
            (Column::Int64(c), DataType::Timestamp) => {
                let nulls = c.nulls();
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if is_null(nulls, row_idx) {
                        row.push(Value::Null);
                    } else {
                        row.push(Value::Timestamp(c.data()[row_idx]));
                    }
                }
            }
            (Column::Int64(c), DataType::TimestampTz) => {
                let nulls = c.nulls();
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if is_null(nulls, row_idx) {
                        row.push(Value::Null);
                    } else {
                        row.push(Value::TimestampTz(c.data()[row_idx]));
                    }
                }
            }
            (Column::Int64(c), DataType::Time) => {
                let nulls = c.nulls();
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if is_null(nulls, row_idx) {
                        row.push(Value::Null);
                    } else {
                        row.push(Value::Time(c.data()[row_idx]));
                    }
                }
            }
            (Column::Int64(c), DataType::TimeTz) => {
                let nulls = c.nulls();
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    if is_null(nulls, row_idx) {
                        row.push(Value::Null);
                    } else {
                        let (micros, offset_seconds) = unpack_timetz(c.data()[row_idx])
                            .ok_or_else(|| {
                                ExecError::TypeMismatch(format!(
                                    "invalid TimeTz payload at row {row_idx}"
                                ))
                            })?;
                        row.push(Value::TimeTz {
                            micros,
                            offset_seconds,
                        });
                    }
                }
            }
            (Column::Utf8(_) | Column::DictionaryUtf8(_), DataType::Interval) => {
                // Interval columns materialise as text in the batch (see
                // `build_batch`). Re-parse the `Value::Interval` display form
                // (`"{months}mon {days}d {microseconds}us"`) so joins/sorts
                // that re-decode the column see a real `Value::Interval`
                // downstream rather than an opaque text string.
                for (row_idx, row) in rows.iter_mut().enumerate() {
                    match col.text_value(row_idx) {
                        Some(v) => {
                            let interval = parse_interval_display_text(v).ok_or_else(|| {
                                ExecError::TypeMismatch(format!(
                                    "column {col_idx} ({name}): invalid interval text {v:?}",
                                    name = field.name,
                                ))
                            })?;
                            row.push(interval);
                        }
                        None => row.push(Value::Null),
                    }
                }
            }
            (col_var, expected_type) => {
                return Err(ExecError::TypeMismatch(format!(
                    "column {col_idx} ({name}): batch column type {:?} does not match schema type {expected_type}",
                    col_var.data_type(),
                    name = field.name,
                )));
            }
        }
    }

    Ok(rows)
}

/// Parse the [`Value::Interval`] display form produced by `build_batch`.
///
/// The materialised text is exactly `"{months}mon {days}d {microseconds}us"`
/// (see [`ultrasql_core::Value`]'s `Display` impl), so each component is a
/// signed integer with a fixed suffix. Returns `None` if the text does not
/// match that shape — the only producer is our own batch builder, so a
/// mismatch indicates a corrupt or foreign column.
fn parse_interval_display_text(text: &str) -> Option<Value> {
    let mut parts = text.split_whitespace();
    let months: i32 = parts.next()?.strip_suffix("mon")?.parse().ok()?;
    let days: i32 = parts.next()?.strip_suffix('d')?.parse().ok()?;
    let microseconds: i64 = parts.next()?.strip_suffix("us")?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some(Value::Interval {
        months,
        days,
        microseconds,
    })
}

fn parse_bit_string_text_cell(
    text: &str,
    expected_type: &DataType,
    col_idx: usize,
    name: &str,
) -> Result<Value, ExecError> {
    let bits = Value::parse_bit_string(text).ok_or_else(|| {
        ExecError::TypeMismatch(format!(
            "column {col_idx} ({name}): invalid {expected_type} literal"
        ))
    })?;
    match &bits {
        Value::BitString(bit_string) if bit_string.matches_type(expected_type) => Ok(bits),
        _ => Err(ExecError::TypeMismatch(format!(
            "column {col_idx} ({name}): invalid {expected_type} length"
        ))),
    }
}

fn parse_vector_family_text_cell(
    text: &str,
    expected_type: &DataType,
    col_idx: usize,
    name: &str,
) -> Result<Value, ExecError> {
    let value = match expected_type {
        DataType::Vector { .. } => Value::parse_vector(text),
        DataType::HalfVec { .. } => Value::parse_halfvec(text),
        DataType::SparseVec { .. } => Value::parse_sparsevec(text),
        DataType::BitVec { .. } => Value::parse_bitvec(text),
        _ => None,
    }
    .ok_or_else(|| {
        ExecError::TypeMismatch(format!(
            "column {col_idx} ({name}): invalid {expected_type} literal"
        ))
    })?;
    let actual = value.data_type();
    if vector_family_kind(expected_type) == vector_family_kind(&actual)
        && dims_compatible(
            expected_type.vector_dims().flatten(),
            actual.vector_dims().flatten(),
        )
    {
        Ok(value)
    } else {
        Err(ExecError::TypeMismatch(format!(
            "column {col_idx} ({name}): invalid {expected_type} dimension"
        )))
    }
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
