//! Column selection and empty-batch materialisation for
//! [`Filter`](super::Filter).
//!
//! [`select_column`] compacts a single column down to the rows chosen
//! by a predicate mask; [`build_empty_batch`] produces a correctly
//! shaped zero-row batch when a non-empty input survives no rows.

use ultrasql_core::{DataType, Schema};
use ultrasql_vec::bitmap::Bitmap;
use ultrasql_vec::column::{BoolColumn, Column, NumericColumn, StringColumn};
use ultrasql_vec::{Batch, DictionaryEncodingPolicy, StringEncoding, encode_strings_auto};

use crate::ExecError;

/// Materialise the rows of `column` selected by `mask`. The output
/// length equals `selected` (the popcount of the mask, passed in to
/// avoid re-counting once per column).
///
/// Every per-type branch allocates a fresh column and preserves the source
/// column's validity bitmap for selected rows. A predicate mask only proves
/// that the predicate's referenced columns passed SQL 3VL; unrelated projected
/// columns may still be NULL and must stay NULL after compaction.
pub(crate) fn select_column(
    column: &Column,
    mask: &Bitmap,
    selected: usize,
) -> Result<Column, ExecError> {
    match column {
        Column::Int32(c) => Ok(Column::Int32(select_numeric_column(c, mask, selected)?)),
        Column::Int64(c) => Ok(Column::Int64(select_numeric_column(c, mask, selected)?)),
        Column::Float32(c) => Ok(Column::Float32(select_numeric_column(c, mask, selected)?)),
        Column::Float64(c) => Ok(Column::Float64(select_numeric_column(c, mask, selected)?)),
        Column::Bool(c) => Ok(Column::Bool(select_bool_column(c, mask, selected)?)),
        Column::Utf8(_) | Column::DictionaryUtf8(_) => {
            let mut out: Vec<Option<String>> = Vec::with_capacity(selected);
            for i in mask.iter_ones() {
                out.push(column.text_value(i).map(str::to_owned));
            }
            match encode_strings_auto(
                out.iter().map(|v| v.as_deref()),
                DictionaryEncodingPolicy::default(),
            ) {
                StringEncoding::Raw(c) => Ok(Column::Utf8(c)),
                StringEncoding::Dictionary(c) => Ok(Column::DictionaryUtf8(c)),
            }
        }
    }
}

fn select_numeric_column<T: Copy>(
    column: &NumericColumn<T>,
    mask: &Bitmap,
    selected: usize,
) -> Result<NumericColumn<T>, ExecError> {
    let data = column.data();
    let mut out = Vec::with_capacity(selected);
    match column.nulls() {
        Some(nulls) => {
            let mut out_nulls = Bitmap::new(selected, false);
            for (out_idx, input_idx) in mask.iter_ones().enumerate() {
                out.push(data[input_idx]);
                if nulls.get(input_idx) {
                    out_nulls.set(out_idx, true);
                }
            }
            NumericColumn::with_nulls(out, out_nulls).map_err(|err| {
                ExecError::TypeMismatch(format!("filter selection validity mismatch: {err}"))
            })
        }
        None => {
            for input_idx in mask.iter_ones() {
                out.push(data[input_idx]);
            }
            Ok(NumericColumn::from_data(out))
        }
    }
}

fn select_bool_column(
    column: &BoolColumn,
    mask: &Bitmap,
    selected: usize,
) -> Result<BoolColumn, ExecError> {
    let mut out = Vec::with_capacity(selected);
    match column.nulls() {
        Some(nulls) => {
            let mut out_nulls = Bitmap::new(selected, false);
            for (out_idx, input_idx) in mask.iter_ones().enumerate() {
                out.push(column.value(input_idx));
                if nulls.get(input_idx) {
                    out_nulls.set(out_idx, true);
                }
            }
            BoolColumn::with_nulls(out, out_nulls).map_err(|err| {
                ExecError::TypeMismatch(format!("filter selection validity mismatch: {err}"))
            })
        }
        None => {
            for input_idx in mask.iter_ones() {
                out.push(column.value(input_idx));
            }
            Ok(BoolColumn::from_data(out))
        }
    }
}

/// Build an empty batch whose column types match `schema`.
///
/// The returned batch has 0 rows but the correct number of columns, each
/// with an empty data vec. This is required when the filter passes no rows
/// from a non-empty input batch — the caller must not mistake 0 rows for
/// EOF.
pub(super) fn build_empty_batch(schema: &Schema) -> Result<Batch, ExecError> {
    let cols: Vec<Column> = schema
        .fields()
        .iter()
        .map(|f| match &f.data_type {
            DataType::Bool => Column::Bool(BoolColumn::from_data(vec![])),
            DataType::Int16 | DataType::Int32 | DataType::Date => {
                Column::Int32(NumericColumn::from_data(vec![]))
            }
            DataType::Int64 | DataType::Oid | DataType::RegClass | DataType::RegType => {
                Column::Int64(NumericColumn::from_data(vec![]))
            }
            DataType::Decimal { .. }
            | DataType::Money
            | DataType::Time
            | DataType::TimeTz
            | DataType::Timestamp
            | DataType::TimestampTz => Column::Int64(NumericColumn::from_data(vec![])),
            DataType::Float32 => Column::Float32(NumericColumn::from_data(vec![])),
            DataType::Float64 => Column::Float64(NumericColumn::from_data(vec![])),
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
            | DataType::PgLsn
            | DataType::Vector { .. }
            | DataType::HalfVec { .. }
            | DataType::SparseVec { .. }
            | DataType::BitVec { .. }
            | DataType::Range(_)
            | DataType::Geometry(_)
            | DataType::Array(_) => Column::Utf8(StringColumn::from_data(vec![])),
            // For Int32 and any other type, fall back to an Int32 column.
            // In practice the binder only produces the above types at v0.5.
            _ => Column::Int32(NumericColumn::from_data(vec![])),
        })
        .collect();
    Batch::new(cols).map_err(ExecError::from)
}
