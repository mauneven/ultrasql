//! Frontend round-trip and error-handling tests.
//!
//! Covers the remaining frontend round-trips (close, flush, copy data /
//! done / fail) and the framing-error tests: truncated inputs, unknown
//! message types, negative or oversized length fields, invalid CStrings,
//! `Describe` invalid-kind bytes, `Bind` lying about its parameter
//! count, and the `encode_appends_does_not_clear` contract.

use bytes::{BufMut, BytesMut};

use super::super::{
    MAX_MESSAGE_BYTES, decode_backend, decode_frontend, encode_backend, encode_frontend,
};
use super::round_trip_frontend;
use crate::error::ProtocolError;
use crate::messages::{BackendMessage, DescribeKind, FrontendMessage};

#[test]
fn truncated_returns_none_without_consuming() {
    let msg = FrontendMessage::Query {
        sql: "SELECT 1".into(),
    };
    let mut full = BytesMut::new();
    encode_frontend(&msg, &mut full);
    // Try every prefix shorter than the full message — each must
    // be reported as "not enough data yet" without consuming.
    for cut in 0..full.len() {
        let mut buf = BytesMut::from(&full[..cut]);
        let before = buf.len();
        let result = decode_frontend(&mut buf).expect("no protocol error on prefix");
        assert!(result.is_none(), "expected None at cut={cut}");
        assert_eq!(
            buf.len(),
            before,
            "decoder consumed on truncation cut={cut}"
        );
    }
}

#[test]
fn truncated_backend_returns_none() {
    let msg = BackendMessage::ParameterStatus {
        name: "client_encoding".into(),
        value: "UTF8".into(),
    };
    let mut full = BytesMut::new();
    encode_backend(&msg, &mut full);
    for cut in 0..full.len() {
        let mut buf = BytesMut::from(&full[..cut]);
        let before = buf.len();
        let result = decode_backend(&mut buf).expect("no protocol error on prefix");
        assert!(result.is_none(), "expected None at cut={cut}");
        assert_eq!(buf.len(), before);
    }
}

#[test]
fn unknown_frontend_type_rejected() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'?'); // not a valid frontend tag
    buf.put_i32(4);
    let err = decode_frontend(&mut buf).unwrap_err();
    match err {
        ProtocolError::UnknownMessageType(t) => assert_eq!(t, b'?'),
        other => panic!("expected UnknownMessageType, got {other:?}"),
    }
}

#[test]
fn unknown_backend_type_rejected() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'?');
    buf.put_i32(4);
    let err = decode_backend(&mut buf).unwrap_err();
    assert!(matches!(err, ProtocolError::UnknownMessageType(b'?')));
}

#[test]
fn negative_length_rejected() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'Q');
    buf.put_i32(-1);
    let err = decode_frontend(&mut buf).unwrap_err();
    assert!(matches!(err, ProtocolError::Malformed(_)));
}

#[test]
fn length_too_small_rejected() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'Q');
    buf.put_i32(3); // must be ≥ 4
    let err = decode_frontend(&mut buf).unwrap_err();
    assert!(matches!(err, ProtocolError::Malformed(_)));
}

#[test]
fn invalid_utf8_cstring_rejected() {
    // Hand-build a Query message with an invalid UTF-8 byte in
    // the SQL string.
    let mut buf = BytesMut::new();
    buf.put_u8(b'Q');
    // length = 4 (length itself) + 1 (bad byte) + 1 (NUL) = 6
    buf.put_i32(6);
    buf.put_u8(0xFF);
    buf.put_u8(0);
    let err = decode_frontend(&mut buf).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidUtf8(_)));
}

#[test]
fn cstring_missing_nul_reported_as_malformed() {
    // A frontend Query whose declared length covers the bytes,
    // but where the SQL string never reaches a NUL terminator.
    let mut buf = BytesMut::new();
    buf.put_u8(b'Q');
    buf.put_i32(8); // 4 + 4 bytes of payload
    buf.put_slice(b"ABCD"); // no NUL
    let err = decode_frontend(&mut buf).unwrap_err();
    // No NUL inside a complete frame is payload-internal truncation,
    // which must be a definitive protocol violation rather than a
    // streaming "read more bytes" condition.
    assert!(matches!(err, ProtocolError::Malformed(_)), "got {err:?}");
}

#[test]
fn multiple_messages_decoded_in_sequence() {
    let mut buf = BytesMut::new();
    encode_frontend(
        &FrontendMessage::Query {
            sql: "SELECT 1".into(),
        },
        &mut buf,
    );
    encode_frontend(&FrontendMessage::Sync, &mut buf);
    encode_frontend(&FrontendMessage::Terminate, &mut buf);

    let first = decode_frontend(&mut buf).unwrap().unwrap();
    let second = decode_frontend(&mut buf).unwrap().unwrap();
    let third = decode_frontend(&mut buf).unwrap().unwrap();
    assert!(buf.is_empty());
    assert!(matches!(first, FrontendMessage::Query { .. }));
    assert!(matches!(second, FrontendMessage::Sync));
    assert!(matches!(third, FrontendMessage::Terminate));
}

#[test]
fn describe_invalid_kind_rejected() {
    let mut buf = BytesMut::new();
    // Build a Describe with an invalid kind byte.
    buf.put_u8(b'D');
    // length = 4 + 1 (kind) + 1 (NUL for empty name) = 6
    buf.put_i32(6);
    buf.put_u8(b'X'); // not S or P — must be rejected
    buf.put_u8(0);
    let err = decode_frontend(&mut buf).unwrap_err();
    assert!(matches!(err, ProtocolError::Malformed(_)));
}

#[test]
fn encode_appends_does_not_clear() {
    // Encoding into a non-empty buffer must append, not overwrite.
    let mut buf = BytesMut::new();
    buf.put_slice(b"prefix");
    encode_frontend(&FrontendMessage::Sync, &mut buf);
    assert!(buf.starts_with(b"prefix"));
    assert!(buf.len() > b"prefix".len());
}

// -------------------------------------------------------------------
// Adversarial inputs: a hostile client must not be able to force a
// gigabyte-class allocation or starve the server's memory by
// advertising a giant message length.
// -------------------------------------------------------------------

/// A frontend Query whose declared length is just past the
/// configured ceiling. The decoder must reject the frame as
/// malformed BEFORE attempting to read or allocate a payload of
/// that size.
#[test]
fn frontend_length_above_max_rejected() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'Q');
    // i32::try_from is safe: MAX_MESSAGE_BYTES fits in i32 by
    // construction (16 MiB).
    let oversized = i32::try_from(MAX_MESSAGE_BYTES + 1).unwrap();
    buf.put_i32(oversized);
    // Don't bother supplying the payload; we should reject before
    // even looking at it.
    let err = decode_frontend(&mut buf).unwrap_err();
    assert!(matches!(err, ProtocolError::Malformed(_)));
}

/// Same scenario via the backend decoder. Defense in depth: even
/// though backend frames originate from our own server, the codec
/// must enforce the bound symmetrically because the decoder is
/// also exercised by clients (psql, libpq-style drivers)
/// against malicious servers.
#[test]
fn backend_length_above_max_rejected() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'D');
    let oversized = i32::try_from(MAX_MESSAGE_BYTES + 1).unwrap();
    buf.put_i32(oversized);
    let err = decode_backend(&mut buf).unwrap_err();
    assert!(matches!(err, ProtocolError::Malformed(_)));
}

/// A tagged frontend message that advertises a length above the
/// configured bound must be rejected as malformed before the
/// codec attempts to pre-allocate or wait for the payload bytes.
/// (The startup-discriminator path covers the same bound through
/// the routing logic in `decode_frontend_inner`; this test pins
/// the bound at the tagged-decode site that handles every
/// message after handshake.)
#[test]
fn tagged_frontend_length_above_max_rejected() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'Q'); // any ASCII tag
    let oversized = i32::try_from(MAX_MESSAGE_BYTES + 1).unwrap();
    buf.put_i32(oversized);
    let err = decode_frontend(&mut buf).unwrap_err();
    assert!(matches!(err, ProtocolError::Malformed(_)), "got {err:?}");
}

/// A startup packet has no type tag: the first four bytes are the
/// length. An oversized length must be rejected after those four bytes
/// are available, even when the high byte is non-zero. Otherwise a
/// hostile first packet can make the server wait for a tagged-message
/// byte that will never be needed.
#[test]
fn startup_length_above_max_rejected() {
    let mut buf = BytesMut::new();
    let oversized = i32::try_from(MAX_MESSAGE_BYTES + 1).unwrap();
    buf.put_i32(oversized);
    let err = decode_frontend(&mut buf).unwrap_err();
    assert!(matches!(err, ProtocolError::Malformed(_)), "got {err:?}");
}

/// Even with a legitimate length below the bound, a parameter
/// count claim that does not fit the available bytes must be
/// caught by the per-element reader. The bound on `MAX_MESSAGE_BYTES`
/// already limits the absolute damage; this test asserts the
/// per-element reader does its share.
#[test]
fn bind_lies_about_param_count_caught_by_truncation() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'B');
    // length placeholder; back-fill at end
    let len_pos = buf.len();
    buf.put_i32(0);
    let payload_start = buf.len();
    // portal name + statement name (both NUL).
    buf.put_u8(0);
    buf.put_u8(0);
    // 0 format codes
    buf.put_i16(0);
    // i16::MAX parameter count, but only one bogus byte of payload
    buf.put_i16(i16::MAX);
    buf.put_u8(0xAA);
    // 0 result format codes
    buf.put_i16(0);
    let payload_end = buf.len();
    let length = i32::try_from(payload_end - payload_start + 4).unwrap();
    buf[len_pos..len_pos + 4].copy_from_slice(&length.to_be_bytes());
    let err = decode_frontend(&mut buf).unwrap_err();
    assert!(matches!(err, ProtocolError::Malformed(_)), "got {err:?}");
}

// -------------------------------------------------------------------
// Extended Query Protocol — frontend messages
// -------------------------------------------------------------------

#[test]
fn close_statement_round_trip() {
    let msg = FrontendMessage::Close {
        kind: DescribeKind::Statement,
        name: "my_stmt".into(),
    };
    assert_eq!(round_trip_frontend(&msg), msg);
}

#[test]
fn close_portal_round_trip() {
    let msg = FrontendMessage::Close {
        kind: DescribeKind::Portal,
        name: String::new(),
    };
    assert_eq!(round_trip_frontend(&msg), msg);
}

#[test]
fn close_invalid_kind_rejected() {
    // Hand-build a Close ('C') with an invalid kind byte.
    // length = 4 + 1 (kind) + 1 (NUL for empty name) = 6
    let mut buf = BytesMut::new();
    buf.put_u8(b'C');
    buf.put_i32(6);
    buf.put_u8(b'Z'); // not S or P
    buf.put_u8(0);
    let err = decode_frontend(&mut buf).unwrap_err();
    assert!(matches!(err, ProtocolError::Malformed(_)));
}

#[test]
fn flush_round_trip() {
    assert_eq!(
        round_trip_frontend(&FrontendMessage::Flush),
        FrontendMessage::Flush
    );
}

#[test]
fn copy_data_frontend_round_trip() {
    let msg = FrontendMessage::CopyData(b"row1\trow2\n".to_vec());
    assert_eq!(round_trip_frontend(&msg), msg);
}

#[test]
fn copy_data_frontend_empty_round_trip() {
    let msg = FrontendMessage::CopyData(Vec::new());
    assert_eq!(round_trip_frontend(&msg), msg);
}

#[test]
fn copy_done_frontend_round_trip() {
    assert_eq!(
        round_trip_frontend(&FrontendMessage::CopyDone),
        FrontendMessage::CopyDone
    );
}

#[test]
fn copy_fail_round_trip() {
    let msg = FrontendMessage::CopyFail("client aborted COPY".into());
    assert_eq!(round_trip_frontend(&msg), msg);
}

#[test]
fn copy_fail_empty_message_round_trip() {
    let msg = FrontendMessage::CopyFail(String::new());
    assert_eq!(round_trip_frontend(&msg), msg);
}

#[test]
fn function_call_round_trip() {
    let msg = FrontendMessage::FunctionCall {
        function_oid: 1234,
        arg_formats: vec![0, 1],
        args: vec![Some(b"hello".to_vec()), None, Some(b"world".to_vec())],
        result_format: 0,
    };
    assert_eq!(round_trip_frontend(&msg), msg);
}

#[test]
fn function_call_no_args_round_trip() {
    let msg = FrontendMessage::FunctionCall {
        function_oid: 42,
        arg_formats: vec![],
        args: vec![],
        result_format: 1,
    };
    assert_eq!(round_trip_frontend(&msg), msg);
}
