//! NULL handling, projection, batch, and error-path tests.

use super::*;
use proptest::prelude::*;

#[test]
fn all_null_row() {
    let codec = RowCodec::new(schema_all_nullable());
    let row = vec![Value::Null, Value::Null];
    assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
}
#[test]
fn mixed_nulls() {
    let codec = RowCodec::new(schema_mixed());
    let row = vec![Value::Null, Value::Text("alice".into()), Value::Null];
    assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
}
#[test]
fn no_nulls_in_mixed_schema() {
    let codec = RowCodec::new(schema_mixed());
    let row = vec![
        Value::Int32(1),
        Value::Text("bob".into()),
        Value::Float64(9.9),
    ];
    assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
}

#[test]
fn decode_projected_returns_requested_columns_in_output_order() {
    let codec = RowCodec::new(schema_mixed());
    let row = vec![Value::Int32(7), Value::Text("payload".into()), Value::Null];
    let bytes = codec.encode(&row).unwrap();

    assert_eq!(
        codec.decode_projected(&bytes, &[2, 0, 1, 1]).unwrap(),
        vec![
            Value::Null,
            Value::Int32(7),
            Value::Text("payload".into()),
            Value::Text("payload".into())
        ]
    );
}

#[test]
fn finish_batch_auto_dictionary_encodes_low_cardinality_text() {
    let schema = schema_text();
    let codec = RowCodec::new(schema.clone());
    let mut builders = vec![ColumnBuilder::new(&schema.field_at(0).data_type, 2048, 0).unwrap()];

    for i in 0..2048 {
        let row = vec![Value::Text(format!("region{}", i % 4))];
        let bytes = codec.encode(&row).unwrap();
        codec.decode_into_builders(&bytes, &mut builders).unwrap();
    }

    let batch = RowCodec::finish_batch(builders).unwrap();
    match &batch.columns()[0] {
        Column::DictionaryUtf8(c) => {
            assert_eq!(c.len(), 2048);
            assert_eq!(c.dict.len(), 4);
            assert_eq!(c.decode_at(5), "region1");
        }
        other => panic!("expected dictionary text column, got {other:?}"),
    }
}

#[test]
fn finish_batch_dictionary_text_preserves_nulls() {
    let schema = Schema::new([Field::nullable("s", DataType::Text { max_len: None })]).unwrap();
    let codec = RowCodec::new(schema.clone());
    let mut builders = vec![ColumnBuilder::new(&schema.field_at(0).data_type, 2048, 0).unwrap()];

    for i in 0..2048 {
        let row = if i % 8 == 0 {
            vec![Value::Null]
        } else {
            vec![Value::Text(format!("code{}", i % 3))]
        };
        let bytes = codec.encode(&row).unwrap();
        codec.decode_into_builders(&bytes, &mut builders).unwrap();
    }

    let batch = RowCodec::finish_batch(builders).unwrap();
    match &batch.columns()[0] {
        Column::DictionaryUtf8(c) => {
            let nulls = c.codes.nulls().expect("dictionary text should be nullable");
            assert!(!nulls.get(0));
            assert!(nulls.get(1));
            assert_eq!(c.decode_at(1), "code1");
        }
        other => panic!("expected nullable dictionary text column, got {other:?}"),
    }
}

#[test]
fn arity_mismatch_on_encode_returns_arity_error() {
    let codec = RowCodec::new(schema_i32());
    let err = codec
        .encode(&[Value::Int32(1), Value::Int32(2)])
        .expect_err("arity mismatch");
    assert!(matches!(err, RowCodecError::Arity { schema: 1, row: 2 }));
}
#[test]
fn arity_mismatch_empty_row_on_nonempty_schema() {
    let codec = RowCodec::new(schema_i32());
    let err = codec.encode(&[]).expect_err("arity mismatch");
    assert!(matches!(err, RowCodecError::Arity { schema: 1, row: 0 }));
}
#[test]
fn truncated_payload_on_decode_returns_truncated_error() {
    let codec = RowCodec::new(schema_i32());
    let err = codec.decode(&[0x00, 0x01, 0x02]).expect_err("truncated");
    assert!(matches!(err, RowCodecError::Truncated { .. }));
}
#[test]
fn empty_payload_on_nonempty_schema_returns_truncated() {
    let codec = RowCodec::new(schema_i32());
    let err = codec.decode(&[]).expect_err("truncated");
    assert!(matches!(err, RowCodecError::Truncated { .. }));
}

proptest! {
    #[test]
    fn prop_round_trip_i32(v: i32) {
        let codec = RowCodec::new(schema_i32());
        let row = vec![Value::Int32(v)];
        prop_assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
    }
    #[test]
    fn prop_round_trip_i64(v: i64) {
        let codec = RowCodec::new(schema_i64());
        let row = vec![Value::Int64(v)];
        prop_assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
    }
    #[test]
    fn prop_round_trip_text(s in ".*") {
        let codec = RowCodec::new(schema_text());
        let row = vec![Value::Text(s)];
        prop_assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
    }
    #[test]
    fn prop_round_trip_mixed(id: i32, name in "[a-zA-Z0-9]{0,32}", score: f64) {
        let codec = RowCodec::new(schema_mixed());
        let row = vec![Value::Int32(id), Value::Text(name), Value::Float64(score)];
        prop_assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
    }
}
