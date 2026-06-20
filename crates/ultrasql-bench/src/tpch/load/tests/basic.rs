//! Loader smoke tests: `.tbl` parsing, column counts, and row encoders.

#[cfg(any(test, feature = "sql-bench"))]
use crate::tpch::load::encode::build_ultrasql_insert_sql;
#[cfg(feature = "sql-bench")]
use crate::tpch::load::encode::encode_direct_tbl_row;
use crate::tpch::load::{column_count, read_tbl};

#[test]
fn read_tbl_strips_trailing_pipe() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("test.tbl");
    std::fs::write(&path, "1|Alice|42|\n2|Bob|7|\n").expect("write");
    let rows = read_tbl(&path).expect("read");
    assert_eq!(rows.len(), 2);
    // Trailing pipe stripped — 3 fields per row.
    assert_eq!(rows[0].len(), 3, "row 0 should have 3 fields");
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[0][1], "Alice");
    assert_eq!(rows[0][2], "42");
}

#[test]
fn read_tbl_empty_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("empty.tbl");
    std::fs::write(&path, "").expect("write");
    let rows = read_tbl(&path).expect("read empty");
    assert!(rows.is_empty());
}

#[test]
fn column_count_all_tables() {
    assert_eq!(column_count("region"), 3);
    assert_eq!(column_count("nation"), 4);
    assert_eq!(column_count("supplier"), 7);
    assert_eq!(column_count("customer"), 8);
    assert_eq!(column_count("part"), 9);
    assert_eq!(column_count("partsupp"), 5);
    assert_eq!(column_count("orders"), 9);
    assert_eq!(column_count("lineitem"), 16);
    assert_eq!(column_count("unknown"), 0);
}

#[test]
fn ultrasql_insert_sql_formats_typed_literals() {
    let sql = build_ultrasql_insert_sql(
        "orders",
        &[vec![
            "1".to_owned(),
            "2".to_owned(),
            "O".to_owned(),
            "123.45".to_owned(),
            "1994-01-01".to_owned(),
            "5-LOW".to_owned(),
            "Clerk#000000001".to_owned(),
            "0".to_owned(),
            "note's ok".to_owned(),
        ]],
    )
    .expect("build INSERT sql");
    assert!(sql.contains("123.45"), "decimal literal stays numeric");
    assert!(sql.contains("DATE '1994-01-01'"), "date literal is typed");
    assert!(sql.contains("'note''s ok'"), "text is SQL-escaped");
}

#[cfg(feature = "sql-bench")]
#[test]
fn direct_lineitem_encoder_round_trips_through_row_codec() {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_executor::RowCodec;

    let schema = Schema::new([
        Field::required("l_orderkey", DataType::Int32),
        Field::required("l_partkey", DataType::Int32),
        Field::required("l_suppkey", DataType::Int32),
        Field::required("l_linenumber", DataType::Int32),
        Field::required(
            "l_quantity",
            DataType::Decimal {
                precision: Some(15),
                scale: Some(2),
            },
        ),
        Field::required(
            "l_extendedprice",
            DataType::Decimal {
                precision: Some(15),
                scale: Some(2),
            },
        ),
        Field::required(
            "l_discount",
            DataType::Decimal {
                precision: Some(15),
                scale: Some(2),
            },
        ),
        Field::required(
            "l_tax",
            DataType::Decimal {
                precision: Some(15),
                scale: Some(2),
            },
        ),
        Field::required("l_returnflag", DataType::Text { max_len: None }),
        Field::required("l_linestatus", DataType::Text { max_len: None }),
        Field::required("l_shipdate", DataType::Date),
        Field::required("l_commitdate", DataType::Date),
        Field::required("l_receiptdate", DataType::Date),
        Field::required("l_shipinstruct", DataType::Text { max_len: None }),
        Field::required("l_shipmode", DataType::Text { max_len: None }),
        Field::required("l_comment", DataType::Text { max_len: None }),
    ])
    .expect("lineitem schema");
    let payload = encode_direct_tbl_row(
        &schema,
        "1|2|3|4|5.00|100.00|0.10|0.05|N|O|1998-09-01|1998-09-02|1998-09-03|DELIVER IN PERSON|AIR|comment",
    )
    .expect("direct encode");
    let row = RowCodec::new(schema).decode(&payload).expect("row decode");

    assert_eq!(row[0], Value::Int32(1));
    assert_eq!(
        row[4],
        Value::Decimal {
            value: 500,
            scale: 2
        }
    );
    assert_eq!(row[8], Value::Text("N".to_owned()));
    assert_eq!(row[10], Value::Date(-487));
    assert_eq!(row[15], Value::Text("comment".to_owned()));
}

#[cfg(feature = "sql-bench")]
#[test]
fn direct_char_encoder_round_trips_padded_bpchar() {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_executor::RowCodec;

    let schema = Schema::new([
        Field::required("r_name", DataType::Char { len: Some(4) }),
        Field::required("r_comment", DataType::Text { max_len: None }),
    ])
    .expect("char schema");
    let payload = encode_direct_tbl_row(&schema, "EU|comment").expect("direct encode");
    let row = RowCodec::new(schema).decode(&payload).expect("row decode");

    assert_eq!(row[0], Value::Char("EU  ".to_owned()));
    assert_eq!(row[1], Value::Text("comment".to_owned()));
}
