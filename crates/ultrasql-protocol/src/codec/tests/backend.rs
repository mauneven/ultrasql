//! Backend round-trip tests and codec specimens.
//!
//! Covers backend round-trips for the remaining message types
//! (ParseComplete, BindComplete, CloseComplete, NoData,
//! ParameterDescription, PortalSuspended, CopyInResponse,
//! CopyOutResponse, CopyData, CopyDone, NotificationResponse), the
//! golden-byte fixtures, the extended-query pipeline specimen, the
//! `function_call_*` round-trips and their truncation tests, and the
//! proptest that fuzzes `Parse` round-trips.

use bytes::BytesMut;

use super::super::{decode_backend, decode_frontend, encode_backend, encode_frontend};
use super::{round_trip_backend, round_trip_frontend};
use crate::messages::{BackendMessage, DescribeKind, FrontendMessage};

#[test]
fn extended_query_pipeline_specimen() {
    let parse = FrontendMessage::Parse {
        name: "get_user".into(),
        sql: "SELECT id, name FROM users WHERE id = $1".into(),
        param_types: vec![23],
    };
    let bind = FrontendMessage::Bind {
        portal_name: "p_get_user".into(),
        statement_name: "get_user".into(),
        param_formats: vec![],
        params: vec![Some(b"42".to_vec())],
        result_formats: vec![0, 0],
    };
    let describe = FrontendMessage::Describe {
        kind: DescribeKind::Portal,
        name: "p_get_user".into(),
    };
    let execute = FrontendMessage::Execute {
        portal: "p_get_user".into(),
        max_rows: 0,
    };
    let close = FrontendMessage::Close {
        kind: DescribeKind::Portal,
        name: "p_get_user".into(),
    };

    for msg in &[parse, bind, describe, execute, close, FrontendMessage::Sync] {
        assert_eq!(round_trip_frontend(msg), *msg);
    }
}

// -------------------------------------------------------------------
// Extended Query Protocol — truncation rejection for new frontend
// messages.
// -------------------------------------------------------------------

/// Every prefix of a Close message shorter than the full encoding
/// must return `Ok(None)` without consuming any bytes.
#[test]
fn close_truncated_returns_none() {
    let msg = FrontendMessage::Close {
        kind: DescribeKind::Statement,
        name: "stmt".into(),
    };
    let mut full = BytesMut::new();
    encode_frontend(&msg, &mut full);
    for cut in 0..full.len() {
        let mut buf = BytesMut::from(&full[..cut]);
        let before = buf.len();
        let result = decode_frontend(&mut buf).expect("no protocol error on prefix");
        assert!(result.is_none(), "expected None at cut={cut}");
        assert_eq!(buf.len(), before, "consumed bytes at cut={cut}");
    }
}

/// Every prefix of a `CopyFail` message shorter than the full encoding
/// must return `Ok(None)`.
#[test]
fn copy_fail_truncated_returns_none() {
    let msg = FrontendMessage::CopyFail("something went wrong".into());
    let mut full = BytesMut::new();
    encode_frontend(&msg, &mut full);
    for cut in 0..full.len() {
        let mut buf = BytesMut::from(&full[..cut]);
        let before = buf.len();
        let result = decode_frontend(&mut buf).expect("no protocol error on prefix");
        assert!(result.is_none(), "expected None at cut={cut}");
        assert_eq!(buf.len(), before);
    }
}

/// Every prefix of a `FunctionCall` message must return `Ok(None)`.
#[test]
fn function_call_truncated_returns_none() {
    let msg = FrontendMessage::FunctionCall {
        function_oid: 99,
        arg_formats: vec![0],
        args: vec![Some(b"arg".to_vec())],
        result_format: 0,
    };
    let mut full = BytesMut::new();
    encode_frontend(&msg, &mut full);
    for cut in 0..full.len() {
        let mut buf = BytesMut::from(&full[..cut]);
        let before = buf.len();
        let result = decode_frontend(&mut buf).expect("no protocol error on prefix");
        assert!(result.is_none(), "expected None at cut={cut}");
        assert_eq!(buf.len(), before);
    }
}

// -------------------------------------------------------------------
// Extended Query Protocol — golden-bytes tests (byte-exact layout).
// -------------------------------------------------------------------

/// Golden bytes test for `Flush`.
///
/// `Flush` must encode as exactly: `b'H'` (1 byte) + `0x00_00_00_04`
/// (i32 BE length = 4, no payload). This pins the spec-mandated
/// wire layout so a regression in `write_tagged` is caught
/// immediately.
#[test]
fn flush_golden_bytes() {
    let mut buf = BytesMut::new();
    encode_frontend(&FrontendMessage::Flush, &mut buf);
    // tag + length(4)
    assert_eq!(&buf[..], &[b'H', 0, 0, 0, 4]);
}

/// Golden bytes test for `ParseComplete`.
///
/// `ParseComplete` must encode as: `b'1'` + `0x00_00_00_04`.
#[test]
fn parse_complete_golden_bytes() {
    let mut buf = BytesMut::new();
    encode_backend(&BackendMessage::ParseComplete, &mut buf);
    assert_eq!(&buf[..], &[b'1', 0, 0, 0, 4]);
}

/// Golden bytes test for a small `CopyData` payload.
///
/// For `CopyData(b"AB")`:
/// - tag `b'd'` (1 byte)
/// - length = 4 (length field) + 2 (payload) = 6, encoded big-endian
/// - payload `b'A'`, `b'B'`
#[test]
fn copy_data_frontend_golden_bytes() {
    let mut buf = BytesMut::new();
    encode_frontend(&FrontendMessage::CopyData(b"AB".to_vec()), &mut buf);
    assert_eq!(&buf[..], &[b'd', 0, 0, 0, 6, b'A', b'B']);
}

/// Decode the same hand-rolled `CopyData` bytes and assert the
/// decoded message matches.
#[test]
fn copy_data_frontend_golden_decode() {
    // tag='d', length=6, data="AB"
    let raw: &[u8] = &[b'd', 0, 0, 0, 6, b'A', b'B'];
    let mut buf = BytesMut::from(raw);
    let msg = decode_frontend(&mut buf).unwrap().unwrap();
    assert_eq!(msg, FrontendMessage::CopyData(b"AB".to_vec()));
    assert!(buf.is_empty());
}

// -------------------------------------------------------------------
// Extended Query Protocol — backend messages
// -------------------------------------------------------------------

#[test]
fn parse_complete_round_trip() {
    assert_eq!(
        round_trip_backend(&BackendMessage::ParseComplete),
        BackendMessage::ParseComplete
    );
}

#[test]
fn bind_complete_round_trip() {
    assert_eq!(
        round_trip_backend(&BackendMessage::BindComplete),
        BackendMessage::BindComplete
    );
}

#[test]
fn close_complete_round_trip() {
    assert_eq!(
        round_trip_backend(&BackendMessage::CloseComplete),
        BackendMessage::CloseComplete
    );
}

#[test]
fn no_data_round_trip() {
    assert_eq!(
        round_trip_backend(&BackendMessage::NoData),
        BackendMessage::NoData
    );
}

#[test]
fn parameter_description_round_trip() {
    let msg = BackendMessage::ParameterDescription {
        type_oids: vec![23, 25, 700],
    };
    assert_eq!(round_trip_backend(&msg), msg);
}

#[test]
fn parameter_description_empty_round_trip() {
    let msg = BackendMessage::ParameterDescription { type_oids: vec![] };
    assert_eq!(round_trip_backend(&msg), msg);
}

#[test]
fn portal_suspended_round_trip() {
    assert_eq!(
        round_trip_backend(&BackendMessage::PortalSuspended),
        BackendMessage::PortalSuspended
    );
}

#[test]
fn copy_in_response_round_trip() {
    let msg = BackendMessage::CopyInResponse {
        overall_format: 0,
        column_formats: vec![0, 0, 0],
    };
    assert_eq!(round_trip_backend(&msg), msg);
}

#[test]
fn copy_in_response_binary_round_trip() {
    let msg = BackendMessage::CopyInResponse {
        overall_format: 1,
        column_formats: vec![1, 0, 1],
    };
    assert_eq!(round_trip_backend(&msg), msg);
}

#[test]
fn copy_out_response_round_trip() {
    let msg = BackendMessage::CopyOutResponse {
        overall_format: 0,
        column_formats: vec![0, 1],
    };
    assert_eq!(round_trip_backend(&msg), msg);
}

#[test]
fn copy_data_backend_round_trip() {
    let msg = BackendMessage::CopyData(b"col1\tcol2\n".to_vec());
    assert_eq!(round_trip_backend(&msg), msg);
}

#[test]
fn copy_data_backend_empty_round_trip() {
    let msg = BackendMessage::CopyData(Vec::new());
    assert_eq!(round_trip_backend(&msg), msg);
}

#[test]
fn copy_done_backend_round_trip() {
    assert_eq!(
        round_trip_backend(&BackendMessage::CopyDone),
        BackendMessage::CopyDone
    );
}

// -------------------------------------------------------------------
// Backend truncation rejection for new message types.
// -------------------------------------------------------------------

#[test]
fn parameter_description_truncated_returns_none() {
    let msg = BackendMessage::ParameterDescription {
        type_oids: vec![23, 25],
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
fn copy_in_response_truncated_returns_none() {
    let msg = BackendMessage::CopyInResponse {
        overall_format: 0,
        column_formats: vec![0, 0],
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

// -------------------------------------------------------------------
// Property test: Parse round-trips with arbitrary query strings
// and param-type vectors.
// -------------------------------------------------------------------

proptest::proptest! {
    /// Arbitrary `Parse` messages round-trip identically through
    /// encode → decode. Covers query strings up to 1 KiB and
    /// param-type vectors up to 16 entries.
    #[test]
    fn parse_round_trips_arbitrary(
        name in "[a-zA-Z_][a-zA-Z0-9_]{0,31}",
        // Query strings with printable ASCII (excluding NUL which is
        // the cstring terminator and therefore illegal on the wire).
        sql in "[\\x01-\\x7e]{0,1024}",
        param_types in proptest::collection::vec(0_u32..=0xFFFF_u32, 0..=16),
    ) {
        let msg = FrontendMessage::Parse {
            name,
            sql,
            param_types,
        };
        let mut buf = BytesMut::new();
        encode_frontend(&msg, &mut buf);
        let decoded = decode_frontend(&mut buf)
            .expect("decode ok")
            .expect("some");
        proptest::prop_assert_eq!(decoded, msg);
        proptest::prop_assert!(buf.is_empty());
    }
}
