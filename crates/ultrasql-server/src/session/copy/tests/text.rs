//! Tests for textual COPY cell decoding, projection, and binary cell helpers.

use ultrasql_core::{DataType, Field, GeometryType, Oid, RangeType, Value};
use ultrasql_executor::RowCodec;

use super::super::super::jsonb_ingest::JsonbShapeCache;
use super::super::binary::{
    binary_copy_cell_bytes, decode_binary_copy_cell, read_i16_be, read_i32_be,
};
use super::super::decode::{
    days_in_month, decode_copy_cell, decode_copy_cells_to_payload, decode_one_copy_row,
    format_float_f32, format_float_f64, parse_copy_date, parse_copy_time, parse_copy_timestamp,
    parse_copy_timestamptz, parse_copy_timetz,
};
use super::super::{CopyRowDecodeContext, ServerCopyFormat};
use super::{copy_opts, entry_with_schema, schema};

#[test]
fn copy_text_cell_decoding_covers_types_and_errors() {
    let mut cache = JsonbShapeCache::default();
    assert_eq!(
        decode_copy_cell(Some(b"yes"), &DataType::Bool, 0, &mut cache).expect("bool"),
        Value::Bool(true)
    );
    assert_eq!(
        decode_copy_cell(Some(b"N"), &DataType::Bool, 0, &mut cache).expect("bool"),
        Value::Bool(false)
    );
    assert!(decode_copy_cell(Some(b"maybe"), &DataType::Bool, 0, &mut cache).is_err());
    assert_eq!(
        decode_copy_cell(Some(b"123"), &DataType::Oid, 0, &mut cache).expect("oid"),
        Value::Oid(Oid::new(123))
    );
    assert_eq!(
        decode_copy_cell(Some(b"124"), &DataType::RegClass, 0, &mut cache).expect("regclass"),
        Value::RegClass(Oid::new(124))
    );
    assert_eq!(
        decode_copy_cell(Some(b"125"), &DataType::RegType, 0, &mut cache).expect("regtype"),
        Value::RegType(Oid::new(125))
    );
    assert!(decode_copy_cell(Some(b"bad"), &DataType::PgLsn, 0, &mut cache).is_err());
    assert_eq!(
        decode_copy_cell(Some(b"1.25"), &DataType::Float32, 0, &mut cache).expect("float4"),
        Value::Float32(1.25)
    );
    assert_eq!(
        decode_copy_cell(Some(b"-2.5"), &DataType::Float64, 0, &mut cache).expect("float8"),
        Value::Float64(-2.5)
    );
    assert_eq!(
        decode_copy_cell(
            Some(b"12.345"),
            &DataType::Decimal {
                precision: Some(8),
                scale: Some(2),
            },
            0,
            &mut cache,
        )
        .expect("decimal"),
        Value::Decimal {
            value: 1235,
            scale: 2,
        }
    );
    assert_eq!(
        decode_copy_cell(Some(b"$1.25"), &DataType::Money, 0, &mut cache).expect("money"),
        Value::Money(125)
    );
    assert_eq!(
        decode_copy_cell(Some(b"1970-01-02"), &DataType::Date, 0, &mut cache).expect("date"),
        Value::Date(-10_956)
    );
    assert!(decode_copy_cell(Some(b"2024-02-30"), &DataType::Date, 0, &mut cache).is_err());
    assert_eq!(
        decode_copy_cell(Some(b"00:00:01"), &DataType::Time, 0, &mut cache).expect("time"),
        Value::Time(1_000_000)
    );
    assert_eq!(
        decode_copy_cell(Some(b"00:00:01+05"), &DataType::TimeTz, 0, &mut cache).expect("timetz"),
        Value::TimeTz {
            micros: 1_000_000,
            offset_seconds: 18_000,
        }
    );
    assert_eq!(
        decode_copy_cell(
            Some(b"1970-01-01 00:00:01"),
            &DataType::Timestamp,
            0,
            &mut cache,
        )
        .expect("timestamp"),
        Value::Timestamp(-946_684_799_000_000)
    );
    assert_eq!(
        decode_copy_cell(
            Some(b"1970-01-01 00:00:01+00"),
            &DataType::TimestampTz,
            0,
            &mut cache,
        )
        .expect("timestamptz"),
        Value::TimestampTz(-946_684_799_000_000)
    );
    assert_eq!(
        decode_copy_cell(
            Some(b"2000-07-01 00:00:00 America/New_York"),
            &DataType::TimestampTz,
            0,
            &mut cache,
        )
        .expect("named-zone timestamptz"),
        Value::TimestampTz(15_739_200_000_000)
    );
    assert_eq!(
        decode_copy_cell(Some(b"xy"), &DataType::Char { len: Some(4) }, 0, &mut cache,)
            .expect("char"),
        Value::Char("xy  ".to_owned())
    );
    assert_eq!(
        decode_copy_cell(
            Some(b"1010"),
            &DataType::Bit { len: Some(4) },
            0,
            &mut cache,
        )
        .expect("bit"),
        Value::parse_bit_string("1010").expect("bit string")
    );
    assert!(
        decode_copy_cell(Some(b"101"), &DataType::Bit { len: Some(4) }, 0, &mut cache,).is_err()
    );
    assert!(decode_copy_cell(Some(b"{"), &DataType::Json, 0, &mut cache).is_err());
    assert_eq!(
        decode_copy_cell(Some(b"<root/>"), &DataType::Xml, 0, &mut cache).expect("xml"),
        Value::Xml("<root/>".to_owned())
    );
    assert!(decode_copy_cell(Some(b"<root>"), &DataType::Xml, 0, &mut cache).is_err());
    assert_eq!(
        decode_copy_cell(
            Some(b"00000000-0000-0000-0000-000000000007"),
            &DataType::Uuid,
            0,
            &mut cache,
        )
        .expect("uuid"),
        Value::Uuid([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7])
    );
    assert!(
        decode_copy_cell(
            Some(b"[1,2]"),
            &DataType::Vector { dims: Some(3) },
            0,
            &mut cache,
        )
        .is_err()
    );
    assert_eq!(
        decode_copy_cell(
            Some(b"[1,2]"),
            &DataType::HalfVec { dims: Some(2) },
            0,
            &mut cache,
        )
        .expect("halfvec"),
        Value::parse_halfvec("[1,2]").expect("halfvec")
    );
    assert_eq!(
        decode_copy_cell(
            Some(b"{1:1,3:2}/3"),
            &DataType::SparseVec { dims: Some(3) },
            0,
            &mut cache,
        )
        .expect("sparsevec"),
        Value::parse_sparsevec("{1:1,3:2}/3").expect("sparsevec")
    );
    assert_eq!(
        decode_copy_cell(
            Some(b"101"),
            &DataType::BitVec { dims: Some(3) },
            0,
            &mut cache,
        )
        .expect("bitvec"),
        Value::parse_bitvec("101").expect("bitvec")
    );
    assert!(
        decode_copy_cell(
            Some(b"101"),
            &DataType::BitVec { dims: Some(4) },
            0,
            &mut cache,
        )
        .is_err()
    );
    assert_eq!(
        decode_copy_cell(
            Some(b"[1,10)"),
            &DataType::Range(RangeType::Int4),
            0,
            &mut cache,
        )
        .expect("range"),
        Value::Range(ultrasql_core::RangeValue::parse(RangeType::Int4, "[1,10)").expect("range"))
    );
    assert!(
        decode_copy_cell(
            Some(b"bad"),
            &DataType::Range(RangeType::Int4),
            0,
            &mut cache,
        )
        .is_err()
    );
    assert_eq!(
        decode_copy_cell(
            Some(b"(1,2)"),
            &DataType::Geometry(GeometryType::Point),
            0,
            &mut cache,
        )
        .expect("point"),
        Value::Geometry(
            ultrasql_core::GeometryValue::parse(GeometryType::Point, "(1,2)").expect("point")
        )
    );
    assert!(
        decode_copy_cell(
            Some(b"(1)"),
            &DataType::Geometry(GeometryType::Point),
            0,
            &mut cache,
        )
        .is_err()
    );
    assert_eq!(
        decode_copy_cell(Some(b"\\x0a0b"), &DataType::Bytea, 0, &mut cache).expect("bytea"),
        Value::Bytea(vec![10, 11])
    );
    assert_eq!(
        decode_copy_cell(Some(b"deadbeef"), &DataType::Bytea, 0, &mut cache).expect("raw bytea"),
        Value::Bytea(b"deadbeef".to_vec())
    );
    assert!(decode_copy_cell(Some(b"\\xabc"), &DataType::Bytea, 0, &mut cache).is_err());
    assert!(
        decode_copy_cell(
            Some(&[0xff]),
            &DataType::Text { max_len: None },
            0,
            &mut cache
        )
        .is_err()
    );
    assert_eq!(
        decode_copy_cell(None, &DataType::Int32, 0, &mut cache).expect("null"),
        Value::Null
    );
    assert!(decode_copy_cell(Some(b"x"), &DataType::Null, 0, &mut cache).is_err());
}

#[test]
fn copy_row_and_binary_cell_helpers_cover_projection_and_errors() {
    let table_schema = schema([
        Field::required("id", DataType::Int32),
        Field::required("name", DataType::Text { max_len: None }),
        Field::nullable("optional", DataType::Int64),
    ]);
    let stream_schema = schema([
        Field::required("name", DataType::Text { max_len: None }),
        Field::required("id", DataType::Int32),
    ]);
    let entry = entry_with_schema(table_schema.clone());
    let codec = RowCodec::new(table_schema.clone());
    let mut cache = JsonbShapeCache::default();
    {
        let mut context = CopyRowDecodeContext {
            entry: &entry,
            columns: &[1, 0],
            schema: &stream_schema,
            codec: &codec,
            jsonb_shape_cache: &mut cache,
        };
        let payload = decode_copy_cells_to_payload(&[Some(b"ada"), Some(b"7")], &mut context)
            .expect("decode projected payload");
        let decoded = codec.decode(&payload).expect("payload row");
        assert_eq!(
            decoded,
            vec![Value::Int32(7), Value::Text("ada".to_owned()), Value::Null]
        );
        assert!(decode_copy_cells_to_payload(&[Some(b"ada")], &mut context).is_err());
    }

    let opts = copy_opts(ServerCopyFormat::Csv);
    let payload = decode_one_copy_row(
        b"ada,7\n",
        &opts,
        CopyRowDecodeContext {
            entry: &entry,
            columns: &[1, 0],
            schema: &stream_schema,
            codec: &codec,
            jsonb_shape_cache: &mut cache,
        },
    )
    .expect("fast csv decode");
    assert_eq!(
        codec.decode(&payload).expect("fast row")[0],
        Value::Int32(7)
    );
    assert!(
        decode_one_copy_row(
            b"ada,7\n",
            &copy_opts(ServerCopyFormat::Binary),
            CopyRowDecodeContext {
                entry: &entry,
                columns: &[1, 0],
                schema: &stream_schema,
                codec: &codec,
                jsonb_shape_cache: &mut cache,
            },
        )
        .is_err()
    );

    assert!(read_i16_be(&[1], &mut 0).is_err());
    assert!(read_i32_be(&[1, 2, 3], &mut 0).is_err());
    assert_eq!(
        binary_copy_cell_bytes(&Value::Null, &DataType::Text { max_len: None })
            .expect("null fallback"),
        b"NULL".to_vec()
    );
    assert!(decode_binary_copy_cell(&[1, 2], &DataType::Int32, 0, &mut cache).is_err());
    assert!(decode_binary_copy_cell(b"bad", &DataType::Jsonb, 0, &mut cache).is_err());
    assert!(decode_binary_copy_cell(b"<bad>", &DataType::Xml, 0, &mut cache).is_err());
    assert_eq!(
        decode_binary_copy_cell(&[9; 16], &DataType::Uuid, 0, &mut cache).expect("uuid"),
        Value::Uuid([9; 16])
    );

    assert_eq!(parse_copy_date("2000-02-29", 0).expect("leap"), 59);
    assert!(parse_copy_date("20000229", 0).is_err());
    assert!(parse_copy_date("year-02-29", 0).is_err());
    assert!(parse_copy_date("2024-mm-29", 0).is_err());
    assert!(parse_copy_date("2024-02-dd", 0).is_err());
    assert!(parse_copy_timestamp("1970-01-01", 0).is_err());
    assert!(parse_copy_timestamptz("1970-01-01 bad", 0).is_err());
    assert!(parse_copy_time("bad", 0).is_err());
    assert!(parse_copy_timetz("bad", 0).is_err());
    assert_eq!(days_in_month(2024, 2), 29);
    assert_eq!(days_in_month(2023, 2), 28);
    assert_eq!(days_in_month(2023, 13), 0);
    assert_eq!(format_float_f32(f32::INFINITY), b"Infinity".to_vec());
    assert_eq!(format_float_f64(f64::NEG_INFINITY), b"-Infinity".to_vec());
    assert_eq!(format_float_f64(f64::NAN), b"NaN".to_vec());
}
