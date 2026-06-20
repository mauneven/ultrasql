//! Tests for reject-table validation, CSV record framing, and textual
//! row/cell helpers.

use ultrasql_core::{DataType, Field, Value};
use ultrasql_protocol::BackendMessage;

use super::super::decode::copy_rows_from_select_result;
use super::super::fs_io::{
    copy_cells_from_row, copy_format_code, csv_record_complete, csv_sample_record_complete,
    projected_schema, read_copy_file_sample, read_copy_input_file, reject_column_type_matches,
    single_byte_delimiter, validate_copy_reject_table, write_copy_output_file, RejectColumnType,
};
use super::super::ServerCopyFormat;
use super::{copy_env_test_lock, copy_opts, entry_with_schema, schema};
use crate::result_encoder::SelectResult;

#[test]
fn copy_reject_table_validation_and_textual_helpers_cover_edges() {
    let valid = entry_with_schema(schema([
        Field::required("filename", DataType::Text { max_len: None }),
        Field::required("line_number", DataType::Int64),
        Field::required("raw_row", DataType::Char { len: Some(64) }),
        Field::required("error", DataType::Text { max_len: None }),
    ]));
    validate_copy_reject_table(&valid).expect("valid reject table");

    let wrong_len = entry_with_schema(schema([Field::required(
        "filename",
        DataType::Text { max_len: None },
    )]));
    assert!(validate_copy_reject_table(&wrong_len).is_err());
    let wrong_name = entry_with_schema(schema([
        Field::required("path", DataType::Text { max_len: None }),
        Field::required("line_number", DataType::Int64),
        Field::required("raw_row", DataType::Text { max_len: None }),
        Field::required("error", DataType::Text { max_len: None }),
    ]));
    assert!(validate_copy_reject_table(&wrong_name).is_err());
    assert!(reject_column_type_matches(
        &DataType::Char { len: Some(8) },
        RejectColumnType::Text,
    ));
    assert!(!reject_column_type_matches(
        &DataType::Int32,
        RejectColumnType::Int64,
    ));

    let opts = copy_opts(ServerCopyFormat::Csv);
    assert!(
        csv_record_complete(
            br#""a","b
c""#,
            &opts
        )
        .expect("record check")
    );
    assert!(!csv_record_complete(br#""a","b"#, &opts).expect("record check"));
    assert!(csv_sample_record_complete(
        br#""a
b""#
    ));
    assert!(!csv_sample_record_complete(
        br#""a
b"#
    ));
    assert_eq!(single_byte_delimiter('|').expect("delimiter"), b'|');
    assert!(single_byte_delimiter('¿').is_err());
    assert_eq!(copy_format_code(ServerCopyFormat::Text), 0);
    assert_eq!(copy_format_code(ServerCopyFormat::Csv), 0);
    assert_eq!(copy_format_code(ServerCopyFormat::Binary), 1);
    assert_eq!(copy_format_code(ServerCopyFormat::Parquet), 0);

    let file = tempfile::NamedTempFile::new().expect("sample file");
    std::fs::write(file.path(), b"col1,col2\n\"multi\nline\",2\n").expect("write sample");
    let sample =
        read_copy_file_sample(file.path().to_str().expect("utf8 path")).expect("copy sample");
    assert!(sample.contains("multi"));

    let _env_guard = copy_env_test_lock();
    // SAFETY: copy_env_test_lock serializes process-env mutation in this
    // module's tests.
    unsafe {
        std::env::set_var("ULTRASQL_COPY_BINARY_FILE_LIMIT_BYTES", "3");
    }
    let oversized = tempfile::NamedTempFile::new().expect("oversized file");
    std::fs::write(oversized.path(), b"abcd").expect("write oversized");
    let err = read_copy_input_file(oversized.path().to_str().expect("utf8 oversized"))
        .expect_err("oversized binary COPY input rejected");
    assert!(err.to_string().contains("COPY binary file exceeds limit"));
    // SAFETY: copy_env_test_lock serializes process-env mutation in this
    // module's tests.
    unsafe {
        std::env::remove_var("ULTRASQL_COPY_BINARY_FILE_LIMIT_BYTES");
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let dir = tempfile::TempDir::new().expect("copy symlink dir");
        let link = dir.path().join("sample.csv");
        symlink(file.path(), &link).expect("symlink sample");
        assert!(read_copy_file_sample(link.to_str().expect("utf8 link")).is_err());

        let target = dir.path().join("target.out");
        let output_link = dir.path().join("output.csv");
        std::fs::write(&target, b"keep").expect("write target");
        symlink(&target, &output_link).expect("symlink output");
        assert!(
            write_copy_output_file(output_link.to_str().expect("utf8 output"), b"new").is_err()
        );
        assert_eq!(std::fs::read(&target).expect("read target"), b"keep");
    }

    let table_schema = schema([
        Field::required("id", DataType::Int32),
        Field::required("name", DataType::Text { max_len: None }),
        Field::required("created", DataType::Date),
        Field::required(
            "amount",
            DataType::Decimal {
                precision: Some(12),
                scale: Some(2),
            },
        ),
        Field::required("paid", DataType::Money),
    ]);
    let entry = entry_with_schema(table_schema.clone());
    let projected = projected_schema(&entry, &[1, 3]).expect("projected schema");
    assert_eq!(projected.fields()[0].name, "name");
    assert_eq!(projected.fields()[1].name, "amount");

    let row = vec![
        Value::Int32(7),
        Value::Text("ada".to_owned()),
        Value::Date(0),
        Value::Int64(12_34),
        Value::Money(56_78),
    ];
    let cells = copy_cells_from_row(&row, &table_schema, &[0, 2, 3, 4]);
    assert_eq!(cells[0].as_deref(), Some(&b"7"[..]));
    assert_eq!(cells[1].as_deref(), Some(&b"2000-01-01"[..]));
    assert_eq!(cells[2].as_deref(), Some(&b"12.34"[..]));
    assert_eq!(cells[3].as_deref(), Some(&b"$56.78"[..]));

    let select = SelectResult {
        messages: vec![
            BackendMessage::RowDescription { fields: Vec::new() },
            BackendMessage::DataRow {
                columns: vec![Some(b"1".to_vec()), Some(b"ada".to_vec())],
            },
            BackendMessage::CommandComplete {
                tag: "SELECT 1".to_owned(),
            },
        ],
        streamed_body: None,
        shared_streamed_body: None,
        rows: 1,
    };
    let stream_schema = schema([
        Field::required("id", DataType::Int32),
        Field::required("name", DataType::Text { max_len: None }),
    ]);
    let mut text_opts = copy_opts(ServerCopyFormat::Text);
    text_opts.header = true;
    let (payload, rows) =
        copy_rows_from_select_result(&select, &stream_schema, &text_opts).expect("copy rows");
    assert_eq!(rows, 1);
    assert!(
        String::from_utf8(payload)
            .expect("utf8")
            .starts_with("id,name\n")
    );
    assert!(
        copy_rows_from_select_result(
            &select,
            &stream_schema,
            &copy_opts(ServerCopyFormat::Binary),
        )
        .is_err()
    );
}
