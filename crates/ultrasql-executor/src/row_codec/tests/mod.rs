//! Unit and property tests for the row codec.

pub(super) use super::{
    ColumnBuilder, RowCodec, RowCodecError, VECTOR_DIMS_WIDTH, VECTOR_ELEMENT_WIDTH,
    checked_fixed_end, decode_varlena_text,
};
pub(super) use std::sync::Arc;
pub(super) use ultrasql_core::{
    BitString, DataType, Field, GeometryType, GeometryValue, Lsn, NetworkValue, Oid, RangeType,
    RangeValue, Schema, SparseVector, Value,
};
pub(super) use ultrasql_vec::column::Column;

mod builders;
mod errors;
mod numeric;
mod roundtrip;

pub(super) fn schema_bool() -> Schema {
    Schema::new([Field::required("b", DataType::Bool)]).unwrap()
}
pub(super) fn schema_i16() -> Schema {
    Schema::new([Field::required("n", DataType::Int16)]).unwrap()
}
pub(super) fn schema_i32() -> Schema {
    Schema::new([Field::required("n", DataType::Int32)]).unwrap()
}
pub(super) fn schema_i64() -> Schema {
    Schema::new([Field::required("n", DataType::Int64)]).unwrap()
}
pub(super) fn schema_f32() -> Schema {
    Schema::new([Field::required("f", DataType::Float32)]).unwrap()
}
pub(super) fn schema_f64() -> Schema {
    Schema::new([Field::required("f", DataType::Float64)]).unwrap()
}
pub(super) fn schema_text() -> Schema {
    Schema::new([Field::required("s", DataType::Text { max_len: None })]).unwrap()
}
pub(super) fn schema_varchar3() -> Schema {
    Schema::new([Field::required("s", DataType::Text { max_len: Some(3) })]).unwrap()
}
pub(super) fn schema_char4() -> Schema {
    Schema::new([Field::required("c", DataType::Char { len: Some(4) })]).unwrap()
}
pub(super) fn schema_decimal(scale: Option<i32>) -> Schema {
    Schema::new([Field::required(
        "n",
        DataType::Decimal {
            precision: None,
            scale,
        },
    )])
    .unwrap()
}
pub(super) fn schema_money() -> Schema {
    Schema::new([Field::required("amount", DataType::Money)]).unwrap()
}
pub(super) fn schema_mixed() -> Schema {
    Schema::new([
        Field::nullable("id", DataType::Int32),
        Field::required("name", DataType::Text { max_len: None }),
        Field::nullable("score", DataType::Float64),
    ])
    .unwrap()
}
pub(super) fn schema_all_nullable() -> Schema {
    Schema::new([
        Field::nullable("a", DataType::Int32),
        Field::nullable("b", DataType::Text { max_len: None }),
    ])
    .unwrap()
}

#[test]
fn checked_fixed_end_rejects_overflow() {
    let err = checked_fixed_end(usize::MAX, 1, 0).unwrap_err();
    assert!(matches!(
        err,
        RowCodecError::Truncated {
            needed: usize::MAX,
            have: 0
        }
    ));
}

#[test]
fn decode_varlena_text_rejects_cursor_overflow() {
    let mut cursor = usize::MAX;
    let err = decode_varlena_text(&[], &mut cursor, "text column").unwrap_err();
    assert!(matches!(
        err,
        RowCodecError::Truncated {
            needed: usize::MAX,
            have: 0
        }
    ));
    assert_eq!(cursor, usize::MAX);
}
