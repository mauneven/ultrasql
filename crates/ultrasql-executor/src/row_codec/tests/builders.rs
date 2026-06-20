//! Streaming column-builder decode tests.

use super::*;

#[test]
fn decode_into_builders_fast_paths_cover_fixed_width_shapes() {
    for (schema, row, expected_width) in [
        (
            Schema::new([Field::required("a", DataType::Int32)]).unwrap(),
            vec![Value::Int32(1)],
            1,
        ),
        (
            Schema::new([
                Field::required("a", DataType::Int32),
                Field::required("b", DataType::Int32),
            ])
            .unwrap(),
            vec![Value::Int32(1), Value::Int32(2)],
            2,
        ),
        (
            Schema::new([
                Field::required("a", DataType::Int32),
                Field::required("b", DataType::Int32),
                Field::required("c", DataType::Int32),
            ])
            .unwrap(),
            vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)],
            3,
        ),
        (
            Schema::new([Field::required("a", DataType::Int64)]).unwrap(),
            vec![Value::Int64(4)],
            1,
        ),
        (
            Schema::new([
                Field::required("a", DataType::Int64),
                Field::required("b", DataType::Int64),
            ])
            .unwrap(),
            vec![Value::Int64(4), Value::Int64(5)],
            2,
        ),
    ] {
        let codec = RowCodec::new(schema.clone());
        let encoded = codec.encode(&row).expect("encode");
        let mut builders = schema
            .fields()
            .iter()
            .map(|field| ColumnBuilder::new(&field.data_type, 1, 0).expect("builder"))
            .collect::<Vec<_>>();

        codec
            .decode_into_builders(&encoded, &mut builders)
            .expect("fast decode");
        let batch = RowCodec::finish_batch(builders).expect("finish");
        assert_eq!(batch.width(), expected_width);
        assert_eq!(batch.rows(), 1);
        assert_eq!(codec.decode(&encoded).expect("decode"), row);
    }
}

#[test]
fn decode_into_builders_fast_path_falls_back_for_nulls_and_mismatched_builders() {
    let schema = Schema::new([
        Field::required("a", DataType::Int32),
        Field::required("b", DataType::Int32),
    ])
    .unwrap();
    let codec = RowCodec::new(schema.clone());
    let encoded = codec
        .encode(&[Value::Null, Value::Int32(7)])
        .expect("encode");
    let mut builders = codec.new_builders(1).expect("builders");
    codec
        .decode_into_builders(&encoded, &mut builders)
        .expect("generic decode");
    let batch = RowCodec::finish_batch(builders).expect("finish");
    match &batch.columns()[0] {
        Column::Int32(c) => assert!(c.nulls().is_some_and(|n| !n.get(0))),
        other => panic!("expected int32 column, got {other:?}"),
    }
    match &batch.columns()[1] {
        Column::Int32(c) => assert_eq!(c.data()[0], 7),
        other => panic!("expected int32 column, got {other:?}"),
    }

    let mut wrong_builders = vec![
        ColumnBuilder::new(&DataType::Int64, 1, 0).expect("wrong builder"),
        ColumnBuilder::new(&DataType::Int64, 1, 1).expect("wrong builder"),
    ];
    let err = codec
        .decode_into_builders(
            &codec
                .encode(&[Value::Int32(1), Value::Int32(2)])
                .expect("encode"),
            &mut wrong_builders,
        )
        .expect_err("builder mismatch");
    assert!(matches!(
        err,
        RowCodecError::UnsupportedType { column: 0, .. }
    ));
}

#[test]
fn decode_into_builders_fast_path_reports_truncated_fixed_width_payloads() {
    for schema in [
        Schema::new([Field::required("a", DataType::Int32)]).unwrap(),
        Schema::new([
            Field::required("a", DataType::Int32),
            Field::required("b", DataType::Int32),
        ])
        .unwrap(),
        Schema::new([
            Field::required("a", DataType::Int32),
            Field::required("b", DataType::Int32),
            Field::required("c", DataType::Int32),
        ])
        .unwrap(),
        Schema::new([Field::required("a", DataType::Int64)]).unwrap(),
        Schema::new([
            Field::required("a", DataType::Int64),
            Field::required("b", DataType::Int64),
        ])
        .unwrap(),
    ] {
        let codec = RowCodec::new(schema);
        let mut builders = codec.new_builders(1).expect("builders");
        let err = codec
            .decode_into_builders(&[0], &mut builders)
            .expect_err("truncated fixed payload");
        assert!(matches!(err, RowCodecError::Truncated { .. }));
    }
}

#[test]
fn decode_into_builders_rejects_invalid_utf8_without_owned_error_roundtrip() {
    let codec = RowCodec::new(schema_text());
    let mut builders = codec.new_builders(1).expect("builders");
    let err = codec
        .decode_into_builders(&[0x00, 0x01, 0x00, 0x00, 0x00, 0xff], &mut builders)
        .expect_err("invalid utf8");
    assert!(matches!(
        err,
        RowCodecError::InvalidUtf8Slice(_, "text column")
    ));
}

#[test]
fn decode_into_builders_generic_covers_bool_smallint_float_and_nulls() {
    let schema = Schema::new([
        Field::nullable("b", DataType::Bool),
        Field::nullable("s", DataType::Int16),
        Field::nullable("f4", DataType::Float32),
        Field::nullable("f8", DataType::Float64),
    ])
    .unwrap();
    let codec = RowCodec::new(schema.clone());
    assert_eq!(codec.fixed_width_lower_bound(), 1 + 1 + 2 + 4 + 8);
    let rows = [
        vec![
            Value::Bool(true),
            Value::Int16(-7),
            Value::Float32(1.5),
            Value::Float64(-2.25),
        ],
        vec![Value::Null, Value::Null, Value::Null, Value::Null],
    ];
    let mut builders = codec.new_builders(rows.len()).expect("builders");
    for row in rows {
        let encoded = codec.encode(&row).expect("encode");
        codec
            .decode_into_builders(&encoded, &mut builders)
            .expect("decode builders");
    }

    let batch = RowCodec::finish_batch(builders).expect("finish");
    assert_eq!(batch.rows(), 2);
    match &batch.columns()[0] {
        Column::Bool(c) => {
            assert_eq!(c.data(), &[1, 0]);
            let nulls = c.nulls().expect("bool nulls");
            assert!(nulls.get(0));
            assert!(!nulls.get(1));
        }
        other => panic!("expected bool column, got {other:?}"),
    }
    match &batch.columns()[1] {
        Column::Int32(c) => {
            assert_eq!(c.data(), &[-7, 0]);
            let nulls = c.nulls().expect("int16 nulls");
            assert!(nulls.get(0));
            assert!(!nulls.get(1));
        }
        other => panic!("expected int32-backed int16 column, got {other:?}"),
    }
    match &batch.columns()[2] {
        Column::Float32(c) => {
            assert_eq!(c.data(), &[1.5, 0.0]);
            let nulls = c.nulls().expect("float32 nulls");
            assert!(nulls.get(0));
            assert!(!nulls.get(1));
        }
        other => panic!("expected float32 column, got {other:?}"),
    }
    match &batch.columns()[3] {
        Column::Float64(c) => {
            assert_eq!(c.data(), &[-2.25, 0.0]);
            let nulls = c.nulls().expect("float64 nulls");
            assert!(nulls.get(0));
            assert!(!nulls.get(1));
        }
        other => panic!("expected float64 column, got {other:?}"),
    }
}
