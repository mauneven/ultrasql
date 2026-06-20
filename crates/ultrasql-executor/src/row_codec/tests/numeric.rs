//! NUMERIC / DECIMAL / MONEY codec tests.

use super::*;

#[test]
fn decimal_binary_layout_uses_postgres_numeric_groups() {
    let codec = RowCodec::new(schema_decimal(Some(4)));
    let encoded = codec
        .encode(&[Value::Decimal {
            value: 1_234_567_890_123,
            scale: 4,
        }])
        .unwrap();

    assert_eq!(
        encoded,
        vec![
            0x00, // null bitmap: one non-null column
            0x10, 0x00, 0x00, 0x00, // numeric payload length: 16 bytes
            0x00, 0x04, // ndigits
            0x00, 0x02, // weight
            0x00, 0x00, // sign: NUMERIC_POS
            0x00, 0x04, // dscale
            0x00, 0x01, // 1
            0x09, 0x29, // 2345
            0x1a, 0x85, // 6789
            0x00, 0x7b, // 0123
        ]
    );
}

#[test]
fn decimal_precision_rejects_integer_overflow() {
    let schema = Schema::new([Field::required(
        "n",
        DataType::Decimal {
            precision: Some(4),
            scale: Some(2),
        },
    )])
    .unwrap();
    let codec = RowCodec::new(schema);

    assert!(
        codec
            .encode(&[Value::Decimal {
                value: 1_234,
                scale: 2,
            }])
            .is_ok()
    );
    assert!(matches!(
        codec.encode(&[Value::Decimal {
            value: 12_345,
            scale: 2,
        }]),
        Err(RowCodecError::NumericFieldOverflow { column: 0, .. })
    ));
}

#[test]
fn decimal_round_trip_preserves_fractional_weight() {
    let codec = RowCodec::new(schema_decimal(Some(6)));
    let row = vec![Value::Decimal {
        value: -12,
        scale: 6,
    }];
    let encoded = codec.encode(&row).unwrap();

    assert_eq!(
        encoded,
        vec![
            0x00, // null bitmap
            0x0a, 0x00, 0x00, 0x00, // payload length: header + one digit
            0x00, 0x01, // ndigits
            0xff, 0xfe, // weight: -2
            0x40, 0x00, // sign: NUMERIC_NEG
            0x00, 0x06, // dscale
            0x04, 0xb0, // 1200
        ]
    );
    assert_eq!(codec.decode(&encoded).unwrap(), row);
}

#[test]
fn decimal_decode_rejects_digit_outside_nbase() {
    let codec = RowCodec::new(schema_decimal(Some(0)));
    let encoded = vec![
        0x00, // null bitmap
        0x0a, 0x00, 0x00, 0x00, // payload length
        0x00, 0x01, // ndigits
        0x00, 0x00, // weight
        0x00, 0x00, // sign
        0x00, 0x00, // dscale
        0x27, 0x10, // 10000, invalid in base-10000
    ];

    let err = codec.decode(&encoded).expect_err("invalid numeric digit");
    assert!(matches!(err, RowCodecError::Type { column: 0, .. }));
}

#[test]
fn money_round_trip_uses_i64_cash_storage() {
    let codec = RowCodec::new(schema_money());
    let row = vec![Value::Money(123_456)];
    let encoded = codec.encode(&row).unwrap();

    assert_eq!(
        encoded,
        vec![
            0x00, // null bitmap
            0x40, 0xe2, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
        ]
    );
    assert_eq!(codec.decode(&encoded).unwrap(), row);
}

#[test]
fn decode_into_builders_reads_money_cash_payload() {
    let schema = schema_money();
    let codec = RowCodec::new(schema.clone());
    let encoded = codec.encode(&[Value::Money(-123)]).unwrap();
    let mut builders = vec![ColumnBuilder::new(&schema.field_at(0).data_type, 1, 0).unwrap()];

    codec.decode_into_builders(&encoded, &mut builders).unwrap();
    let batch = RowCodec::finish_batch(builders).unwrap();
    match &batch.columns()[0] {
        Column::Int64(c) => assert_eq!(c.data()[0], -123),
        other => panic!("expected money Int64 builder output, got {other:?}"),
    }
}

#[test]
fn decode_projected_skips_decimal_varlena_payload() {
    let schema = Schema::new([
        Field::required(
            "n",
            DataType::Decimal {
                precision: None,
                scale: Some(4),
            },
        ),
        Field::required("id", DataType::Int32),
    ])
    .unwrap();
    let codec = RowCodec::new(schema);
    let encoded = codec
        .encode(&[
            Value::Decimal {
                value: 1_234_567_890_123,
                scale: 4,
            },
            Value::Int32(7),
        ])
        .unwrap();

    assert_eq!(
        codec.decode_projected(&encoded, &[1]).unwrap(),
        vec![Value::Int32(7)]
    );
}

#[test]
fn decode_into_builders_reads_decimal_numeric_payload() {
    let schema = schema_decimal(Some(4));
    let codec = RowCodec::new(schema.clone());
    let encoded = codec
        .encode(&[Value::Decimal {
            value: 1_234_567_890_123,
            scale: 4,
        }])
        .unwrap();
    let mut builders = vec![ColumnBuilder::new(&schema.field_at(0).data_type, 1, 0).unwrap()];

    codec.decode_into_builders(&encoded, &mut builders).unwrap();
    let batch = RowCodec::finish_batch(builders).unwrap();
    match &batch.columns()[0] {
        Column::Int64(c) => assert_eq!(c.data()[0], 1_234_567_890_123),
        other => panic!("expected decimal Int64 builder output, got {other:?}"),
    }
}

#[test]
fn decode_into_builders_preserves_bare_decimal_scale() {
    let schema = schema_decimal(None);
    let codec = RowCodec::new(schema.clone());
    let encoded = codec
        .encode(&[Value::Decimal {
            value: 166_667,
            scale: 6,
        }])
        .unwrap();
    let mut builders = vec![ColumnBuilder::new(&schema.field_at(0).data_type, 1, 0).unwrap()];

    codec.decode_into_builders(&encoded, &mut builders).unwrap();
    let batch = RowCodec::finish_batch(builders).unwrap();
    assert_eq!(batch.columns()[0].text_value(0), Some("0.166667"));
}
