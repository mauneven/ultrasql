//! Loader smoke tests: `.tbl` parsing, column counts, and row encoders.

use crate::tpch::load::encode::build_ultrasql_insert_sql;
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
