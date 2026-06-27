//! Tests for the binary `PGCOPY` round trip and malformed-payload rejection.

use ultrasql_core::{DataType, Field, Value};
use ultrasql_executor::RowCodec;

use super::super::super::jsonb_ingest::JsonbShapeCache;
use super::super::binary::{
    append_binary_copy_header, append_binary_copy_row, append_i16_be, binary_copy_cell_bytes,
    decode_binary_copy_cell, decode_binary_copy_payload,
};
use super::{entry_with_schema, schema};

/// PostgreSQL's binary `timetz` encodes the zone as seconds WEST of UTC,
/// while UltraSQL's internal `offset_seconds` is east-positive. The binary
/// COPY boundary must therefore negate the zone on the wire. This asserts the
/// exact wire int32 matches PG's west-positive convention (a plain internal
/// round trip would hide the sign error, since two negations cancel).
#[test]
fn binary_copy_timetz_zone_offset_uses_postgres_west_positive_sign() {
    // +05:30 east of UTC → internal +19800 → PG wire -19800 (west-positive).
    for (offset_seconds, pg_wire_zone) in [(19_800_i32, -19_800_i32), (-28_800, 28_800)] {
        let value = Value::TimeTz {
            micros: 4_000,
            offset_seconds,
        };
        let bytes = binary_copy_cell_bytes(&value, &DataType::TimeTz).expect("encode timetz");
        assert_eq!(bytes.len(), 12);
        let wire_zone = i32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        assert_eq!(
            wire_zone, pg_wire_zone,
            "binary timetz zone must be west-positive for internal offset {offset_seconds}"
        );

        // And decoding those PG-convention bytes restores the internal sign.
        let mut cache = JsonbShapeCache::default();
        let decoded = decode_binary_copy_cell(&bytes, &DataType::TimeTz, 0, &mut cache)
            .expect("decode timetz");
        assert_eq!(
            decoded, value,
            "binary timetz must round-trip with correct sign"
        );
    }
}

#[test]
fn binary_copy_round_trips_rows_and_rejects_malformed_payloads() {
    let table_schema = schema([
        Field::required("b", DataType::Bool),
        Field::required("i2", DataType::Int16),
        Field::required("i4", DataType::Int32),
        Field::required("i8", DataType::Int64),
        Field::required("f4", DataType::Float32),
        Field::required("f8", DataType::Float64),
        Field::required("d", DataType::Date),
        Field::required("t", DataType::Time),
        Field::required("ts", DataType::Timestamp),
        Field::required("tstz", DataType::TimestampTz),
        Field::required("ttz", DataType::TimeTz),
        Field::required(
            "n",
            DataType::Decimal {
                precision: Some(10),
                scale: Some(2),
            },
        ),
        Field::required("m", DataType::Money),
        Field::required("txt", DataType::Text { max_len: None }),
        Field::required("ch", DataType::Char { len: Some(4) }),
        Field::required("bits", DataType::Bit { len: Some(4) }),
        Field::required("inet", DataType::Inet),
        Field::required("json", DataType::Json),
        Field::required("jsonb", DataType::Jsonb),
        Field::required("xml", DataType::Xml),
        Field::required("bytea", DataType::Bytea),
        Field::required("uuid", DataType::Uuid),
    ]);
    let entry = entry_with_schema(table_schema.clone());
    let row = vec![
        Value::Bool(true),
        Value::Int16(-2),
        Value::Int32(32),
        Value::Int64(64),
        Value::Float32(1.25),
        Value::Float64(-2.5),
        Value::Date(0),
        Value::Time(1_000),
        Value::Timestamp(2_000),
        Value::TimestampTz(3_000),
        Value::TimeTz {
            micros: 4_000,
            offset_seconds: -18_000,
        },
        Value::Decimal {
            value: 12_34,
            scale: 2,
        },
        Value::Money(56_78),
        Value::Text("hello".to_owned()),
        Value::Char("xy  ".to_owned()),
        Value::parse_bit_string("1010").expect("bit string"),
        Value::parse_network(&DataType::Inet, "127.0.0.1").expect("inet"),
        Value::Json("{\"a\":1}".to_owned()),
        Value::Jsonb("{\"a\":1}".to_owned()),
        Value::Xml("<root/>".to_owned()),
        Value::Bytea(vec![1, 2, 3]),
        Value::Uuid([7; 16]),
    ];

    let mut encoded = Vec::new();
    append_binary_copy_header(&mut encoded);
    append_binary_copy_row(&mut encoded, &row, &table_schema, &[], &table_schema)
        .expect("append row");
    append_i16_be(&mut encoded, -1);

    let codec = RowCodec::new(table_schema.clone());
    let mut cache = JsonbShapeCache::default();
    let payloads = decode_binary_copy_payload(
        &encoded,
        &entry,
        &[],
        &table_schema,
        &codec,
        &mut cache,
        false,
    )
    .expect("decode binary copy");
    assert_eq!(payloads.len(), 1);
    assert_eq!(codec.decode(&payloads[0]).expect("row decode"), row);

    assert!(
        decode_binary_copy_payload(
            b"bad",
            &entry,
            &[],
            &table_schema,
            &codec,
            &mut cache,
            false
        )
        .is_err()
    );

    let mut negative_ext = Vec::new();
    negative_ext.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    negative_ext.extend_from_slice(&0_i32.to_be_bytes());
    negative_ext.extend_from_slice(&(-1_i32).to_be_bytes());
    assert!(
        decode_binary_copy_payload(
            &negative_ext,
            &entry,
            &[],
            &table_schema,
            &codec,
            &mut cache,
            false
        )
        .is_err()
    );

    let mut wrong_count = Vec::new();
    append_binary_copy_header(&mut wrong_count);
    append_i16_be(&mut wrong_count, 1);
    wrong_count.extend_from_slice(&(-1_i32).to_be_bytes());
    assert!(
        decode_binary_copy_payload(
            &wrong_count,
            &entry,
            &[],
            &table_schema,
            &codec,
            &mut cache,
            false
        )
        .is_err()
    );

    let mut bad_len = Vec::new();
    append_binary_copy_header(&mut bad_len);
    append_i16_be(
        &mut bad_len,
        i16::try_from(table_schema.len()).expect("column count"),
    );
    bad_len.extend_from_slice(&(-2_i32).to_be_bytes());
    assert!(
        decode_binary_copy_payload(
            &bad_len,
            &entry,
            &[],
            &table_schema,
            &codec,
            &mut cache,
            false
        )
        .is_err()
    );
}

#[test]
fn binary_copy_rejects_unsupported_critical_header_flags() {
    let table_schema = schema([Field::required("id", DataType::Int32)]);
    let entry = entry_with_schema(table_schema.clone());
    let codec = RowCodec::new(table_schema.clone());
    let mut cache = JsonbShapeCache::default();

    let mut encoded = Vec::new();
    encoded.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    encoded.extend_from_slice(&(1_i32 << 16).to_be_bytes());
    encoded.extend_from_slice(&0_i32.to_be_bytes());
    append_i16_be(&mut encoded, -1);

    let err = decode_binary_copy_payload(
        &encoded,
        &entry,
        &[],
        &table_schema,
        &codec,
        &mut cache,
        false,
    )
    .expect_err("unsupported critical binary COPY flags must fail closed");

    assert!(
        err.to_string()
            .contains("unsupported binary COPY critical flags"),
        "unexpected error: {err:?}"
    );
}
