//! Codec tests.
//!
//! - This file holds the shared `round_trip_*` helpers, the startup
//!   tests, the basic frontend round-trips (Query, Parse, Bind,
//!   Describe, Execute, Sync, Terminate, Password), and the
//!   authentication / parameter / key / ready-for-query round-trips.
//! - [`round_trip`] holds the remaining frontend round-trips plus the
//!   error-handling / framing tests (truncation, length bounds,
//!   describe-invalid-kind, encode-appends-does-not-clear).
//! - [`backend`] holds the backend round-trips (parameter description,
//!   portal suspended, copy in/out/data, notification), the
//!   extended-query pipeline specimen, the golden-byte fixtures, and
//!   the proptest.

use bytes::{BufMut, BytesMut};

use crate::error::ProtocolError;
use crate::messages::{BackendMessage, DescribeKind, FieldDescription, FrontendMessage};
use super::{decode_backend, decode_frontend, encode_backend, encode_frontend};

mod backend;
mod round_trip;

pub(super) fn round_trip_frontend(msg: &FrontendMessage) -> FrontendMessage {
    let mut buf = BytesMut::new();
    encode_frontend(msg, &mut buf);
    let decoded = decode_frontend(&mut buf).expect("decode").expect("some");
    assert!(buf.is_empty(), "decoder did not consume all bytes");
    decoded
}

pub(super) fn round_trip_backend(msg: &BackendMessage) -> BackendMessage {
    let mut buf = BytesMut::new();
    encode_backend(msg, &mut buf);
    let decoded = decode_backend(&mut buf).expect("decode").expect("some");
    assert!(buf.is_empty(), "decoder did not consume all bytes");
    decoded
}

// -------------------------------------------------------------------
// StartupMessage
// -------------------------------------------------------------------

#[test]
fn startup_round_trip() {
    let msg = FrontendMessage::StartupMessage {
        protocol_major: 3,
        protocol_minor: 0,
        params: vec![
            ("user".into(), "ultrasql".into()),
            ("database".into(), "ultrasql".into()),
            ("application_name".into(), "psql".into()),
        ],
    };
    assert_eq!(round_trip_frontend(&msg), msg);
}

#[test]
fn startup_has_no_leading_type_byte() {
    let msg = FrontendMessage::StartupMessage {
        protocol_major: 3,
        protocol_minor: 0,
        params: vec![("user".into(), "x".into())],
    };
    let mut buf = BytesMut::new();
    encode_frontend(&msg, &mut buf);
    // First byte is the high byte of an i32 length; for a short
    // startup it must be 0, never an ASCII tag like b'Q' or b'P'.
    assert_eq!(buf[0], 0);
    // Bytes 4..6 must be the major version 3 big-endian.
    assert_eq!(&buf[4..6], &[0, 3]);
}

#[test]
fn startup_empty_params() {
    let msg = FrontendMessage::StartupMessage {
        protocol_major: 3,
        protocol_minor: 0,
        params: vec![],
    };
    assert_eq!(round_trip_frontend(&msg), msg);
}

#[test]
fn startup_missing_terminator_rejected() {
    // Length 8 ⇒ 4-byte length plus two `u16` protocol numbers,
    // and no terminator. The PostgreSQL spec requires a trailing
    // NUL to end the parameter list.
    let mut bytes = BytesMut::new();
    bytes.put_i32(8);
    bytes.put_u16(3);
    bytes.put_u16(0);
    let err = decode_frontend(&mut bytes).unwrap_err();
    assert!(matches!(err, ProtocolError::Malformed(_)));
}

// -------------------------------------------------------------------
// Frontend round-trips
// -------------------------------------------------------------------

#[test]
fn query_round_trip() {
    let msg = FrontendMessage::Query {
        sql: "SELECT 1".into(),
    };
    assert_eq!(round_trip_frontend(&msg), msg);
}

#[test]
fn parse_round_trip() {
    let msg = FrontendMessage::Parse {
        name: "stmt1".into(),
        sql: "SELECT $1::int + $2::int".into(),
        param_types: vec![23, 23],
    };
    assert_eq!(round_trip_frontend(&msg), msg);
}

#[test]
fn bind_round_trip() {
    let msg = FrontendMessage::Bind {
        portal_name: "p1".into(),
        statement_name: "stmt1".into(),
        param_formats: vec![],
        params: vec![
            Some(b"42".to_vec()),
            None,
            Some(b"hello world".to_vec()),
            Some(Vec::new()),
        ],
        result_formats: vec![0, 1],
    };
    assert_eq!(round_trip_frontend(&msg), msg);
}

#[test]
fn bind_round_trip_with_binary_param_formats() {
    // libpq's "all-same" shortcut: one format code applies to
    // every parameter. Also verifies the per-parameter form.
    let all_binary = FrontendMessage::Bind {
        portal_name: String::new(),
        statement_name: String::new(),
        param_formats: vec![1],
        params: vec![Some(42_i32.to_be_bytes().to_vec())],
        result_formats: vec![1],
    };
    assert_eq!(round_trip_frontend(&all_binary), all_binary);

    let per_param = FrontendMessage::Bind {
        portal_name: "p".into(),
        statement_name: "s".into(),
        param_formats: vec![1, 0, 1],
        params: vec![
            Some(7_i64.to_be_bytes().to_vec()),
            Some(b"text".to_vec()),
            Some(vec![1]),
        ],
        result_formats: vec![],
    };
    assert_eq!(round_trip_frontend(&per_param), per_param);
}

#[test]
fn describe_statement_round_trip() {
    let msg = FrontendMessage::Describe {
        kind: DescribeKind::Statement,
        name: "stmt1".into(),
    };
    assert_eq!(round_trip_frontend(&msg), msg);
}

#[test]
fn describe_portal_round_trip() {
    let msg = FrontendMessage::Describe {
        kind: DescribeKind::Portal,
        name: String::new(),
    };
    assert_eq!(round_trip_frontend(&msg), msg);
}

#[test]
fn execute_round_trip() {
    let msg = FrontendMessage::Execute {
        portal: "p1".into(),
        max_rows: 100,
    };
    assert_eq!(round_trip_frontend(&msg), msg);
}

#[test]
fn execute_unlimited_round_trip() {
    let msg = FrontendMessage::Execute {
        portal: String::new(),
        max_rows: 0,
    };
    assert_eq!(round_trip_frontend(&msg), msg);
}

#[test]
fn sync_round_trip() {
    assert_eq!(
        round_trip_frontend(&FrontendMessage::Sync),
        FrontendMessage::Sync
    );
}

#[test]
fn terminate_round_trip() {
    assert_eq!(
        round_trip_frontend(&FrontendMessage::Terminate),
        FrontendMessage::Terminate
    );
}

#[test]
fn password_round_trip() {
    let msg = FrontendMessage::Password {
        password: "hunter2".into(),
    };
    assert_eq!(round_trip_frontend(&msg), msg);
}

// -------------------------------------------------------------------
// Backend round-trips
// -------------------------------------------------------------------

#[test]
fn authentication_ok_round_trip() {
    assert_eq!(
        round_trip_backend(&BackendMessage::AuthenticationOk),
        BackendMessage::AuthenticationOk
    );
}

#[test]
fn authentication_cleartext_round_trip() {
    assert_eq!(
        round_trip_backend(&BackendMessage::AuthenticationCleartextPassword),
        BackendMessage::AuthenticationCleartextPassword
    );
}

#[test]
fn authentication_md5_round_trip() {
    let msg = BackendMessage::AuthenticationMD5Password {
        salt: [0xDE, 0xAD, 0xBE, 0xEF],
    };
    assert_eq!(round_trip_backend(&msg), msg);
}

#[test]
fn parameter_status_round_trip() {
    let msg = BackendMessage::ParameterStatus {
        name: "server_version".into(),
        value: "16.0 (UltraSQL)".into(),
    };
    assert_eq!(round_trip_backend(&msg), msg);
}

#[test]
fn backend_key_data_round_trip() {
    let msg = BackendMessage::BackendKeyData {
        process_id: 12345,
        secret_key: -42,
    };
    assert_eq!(round_trip_backend(&msg), msg);
}

#[test]
fn ready_for_query_round_trip() {
    for status in [b'I', b'T', b'E'] {
        let msg = BackendMessage::ReadyForQuery { status };
        let decoded = round_trip_backend(&msg);
        assert_eq!(decoded, msg);
    }
}

#[test]
fn ready_for_query_rejects_invalid_status() {
    let mut buf = BytesMut::new();
    // Build a Z message with status 'X' — not in {I, T, E}.
    encode_backend(&BackendMessage::ReadyForQuery { status: b'I' }, &mut buf);
    // Mutate the status byte: payload is at offset 5 (tag + len).
    buf[5] = b'X';
    let err = decode_backend(&mut buf).unwrap_err();
    assert!(matches!(err, ProtocolError::Malformed(_)));
}

#[test]
fn row_description_round_trip() {
    let msg = BackendMessage::RowDescription {
        fields: vec![
            FieldDescription {
                name: "id".into(),
                table_oid: 1234,
                col_attnum: 1,
                type_oid: 23, // int4
                type_size: 4,
                type_modifier: -1,
                format_code: 0,
            },
            FieldDescription {
                name: "label".into(),
                table_oid: 1234,
                col_attnum: 2,
                type_oid: 25, // text
                type_size: -1,
                type_modifier: -1,
                format_code: 0,
            },
        ],
    };
    assert_eq!(round_trip_backend(&msg), msg);
}

#[test]
fn row_description_empty_round_trip() {
    let msg = BackendMessage::RowDescription { fields: vec![] };
    assert_eq!(round_trip_backend(&msg), msg);
}

#[test]
fn data_row_round_trip() {
    let msg = BackendMessage::DataRow {
        columns: vec![
            Some(b"1".to_vec()),
            None,
            Some(b"alpha".to_vec()),
            Some(vec![]),
        ],
    };
    assert_eq!(round_trip_backend(&msg), msg);
}

#[test]
fn command_complete_round_trip() {
    let msg = BackendMessage::CommandComplete {
        tag: "SELECT 42".into(),
    };
    assert_eq!(round_trip_backend(&msg), msg);
}

#[test]
fn error_response_round_trip() {
    let msg = BackendMessage::ErrorResponse {
        fields: vec![
            (b'S', "ERROR".into()),
            (b'C', "42601".into()),
            (b'M', "syntax error".into()),
        ],
    };
    assert_eq!(round_trip_backend(&msg), msg);
}

#[test]
fn empty_query_round_trip() {
    assert_eq!(
        round_trip_backend(&BackendMessage::EmptyQueryResponse),
        BackendMessage::EmptyQueryResponse
    );
}

#[test]
fn notice_response_round_trip() {
    let msg = BackendMessage::NoticeResponse {
        fields: vec![(b'S', "NOTICE".into()), (b'M', "interesting fact".into())],
    };
    assert_eq!(round_trip_backend(&msg), msg);
}

// -------------------------------------------------------------------
// Truncation / error behavior
// -------------------------------------------------------------------

