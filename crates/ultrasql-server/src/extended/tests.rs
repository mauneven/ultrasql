//! Unit tests for the Extended Query module.

use super::codec::{DecodeError, decode_param};
use super::handlers::{
    handle_bind, handle_close, handle_describe_portal, handle_describe_statement, handle_parse,
};
use super::params::{infer_parameter_types, walk_plan_exprs};
use super::substitute::substitute_parameters_in_plan;
use super::{
    ExtendedConnState, PG_OID_BOOL, PG_OID_BPCHAR, PG_OID_BYTEA, PG_OID_FLOAT4, PG_OID_FLOAT8,
    PG_OID_INT2, PG_OID_INT4, PG_OID_INT8, PG_OID_OID, PG_OID_TEXT, PG_OID_VARCHAR,
};
use crate::error::ServerError;
use crate::pipeline::LowerCtx;
use ultrasql_core::{DataType, Value};
use ultrasql_planner::{BinaryOp, InMemoryCatalog, LogicalPlan, ScalarExpr};
use ultrasql_protocol::{BackendMessage, DescribeKind};

fn fixture_catalog() -> InMemoryCatalog {
    let mut catalog = InMemoryCatalog::new();
    let _ = crate::pipeline::build_sample_database(&mut catalog);
    catalog
}

// ── Text-format param decoding ───────────────────────────────────────────

#[test]
fn decode_text_int4_parses() {
    let v = decode_param(Some(b"42"), 0, Some(PG_OID_INT4)).unwrap();
    assert_eq!(v, Value::Int32(42));
}

#[test]
fn decode_text_int8_parses() {
    let v = decode_param(Some(b"9000000000"), 0, Some(PG_OID_INT8)).unwrap();
    assert_eq!(v, Value::Int64(9_000_000_000));
}

#[test]
fn decode_text_bool_t_and_f() {
    assert_eq!(
        decode_param(Some(b"t"), 0, Some(PG_OID_BOOL)).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        decode_param(Some(b"f"), 0, Some(PG_OID_BOOL)).unwrap(),
        Value::Bool(false)
    );
}

#[test]
fn decode_text_null_returns_null() {
    assert_eq!(
        decode_param(None, 0, Some(PG_OID_INT4)).unwrap(),
        Value::Null
    );
}

#[test]
fn decode_text_text_oid_returns_text() {
    let v = decode_param(Some(b"hello"), 0, Some(PG_OID_TEXT)).unwrap();
    assert_eq!(v, Value::Text("hello".to_string()));
}

#[test]
fn decode_text_no_oid_infers_int32_for_numeric() {
    // libpq's "I haven't told you the type" path.
    let v = decode_param(Some(b"42"), 0, None).unwrap();
    assert_eq!(v, Value::Int32(42));
}

// ── Binary-format param decoding ─────────────────────────────────────────

#[test]
fn decode_binary_int4_parses() {
    let bytes = 42_i32.to_be_bytes();
    let v = decode_param(Some(&bytes), 1, Some(PG_OID_INT4)).unwrap();
    assert_eq!(v, Value::Int32(42));
}

#[test]
fn decode_binary_int8_parses() {
    let bytes = 9_000_000_000_i64.to_be_bytes();
    let v = decode_param(Some(&bytes), 1, Some(PG_OID_INT8)).unwrap();
    assert_eq!(v, Value::Int64(9_000_000_000));
}

#[test]
fn decode_binary_bool_byte() {
    let v = decode_param(Some(&[1]), 1, Some(PG_OID_BOOL)).unwrap();
    assert_eq!(v, Value::Bool(true));
    let v = decode_param(Some(&[0]), 1, Some(PG_OID_BOOL)).unwrap();
    assert_eq!(v, Value::Bool(false));
}

#[test]
fn decode_binary_wrong_length_errors() {
    let three_bytes = [0_u8, 0, 0];
    let err = decode_param(Some(&three_bytes), 1, Some(PG_OID_INT4)).unwrap_err();
    assert!(matches!(err, DecodeError::BadBytes));
}

#[test]
fn decode_unknown_format_errors() {
    let err = decode_param(Some(b"42"), 2, None).unwrap_err();
    assert!(matches!(err, DecodeError::BadFormat));
}

// ── Parameter substitution / counting ────────────────────────────────────

#[test]
fn substitute_simple_eq_predicate() {
    // SELECT id FROM users WHERE id = $1   →   id = 1
    let catalog = fixture_catalog();
    let mut state = ExtendedConnState::new();
    let _ = handle_parse(
        &mut state,
        "s1".to_string(),
        "SELECT id FROM users WHERE id = $1".to_string(),
        vec![PG_OID_INT4],
        &catalog,
    )
    .expect("parse ok");
    let stmt = state.statements.get("s1").unwrap();
    assert_eq!(stmt.n_params, 1);

    // Substitute and check the predicate became id = 1 literal.
    let sub = substitute_parameters_in_plan(stmt.plan.as_ref().unwrap(), &[Value::Int32(1)]);
    // The plan is Project(Filter(Scan)); reach into Filter.predicate.
    let mut found = false;
    walk_plan_exprs(&sub, &mut |e| {
        if let ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
            ..
        } = e
        {
            match (left.as_ref(), right.as_ref()) {
                (
                    ScalarExpr::Column { .. },
                    ScalarExpr::Literal {
                        value: Value::Int32(1),
                        ..
                    },
                )
                | (
                    ScalarExpr::Literal {
                        value: Value::Int32(1),
                        ..
                    },
                    ScalarExpr::Column { .. },
                ) => found = true,
                _ => {}
            }
        }
    });
    assert!(found, "Parameter not substituted into Filter predicate");
}

#[test]
fn counting_zero_parameters_returns_zero() {
    let catalog = fixture_catalog();
    let mut state = ExtendedConnState::new();
    handle_parse(
        &mut state,
        "s".to_string(),
        "SELECT id FROM users".to_string(),
        vec![],
        &catalog,
    )
    .expect("parse ok");
    assert_eq!(state.statements.get("s").unwrap().n_params, 0);
}

// ── Close / describe ─────────────────────────────────────────────────────

#[test]
fn close_removes_statement() {
    let catalog = fixture_catalog();
    let mut state = ExtendedConnState::new();
    handle_parse(
        &mut state,
        "s".to_string(),
        "SELECT id FROM users".to_string(),
        vec![],
        &catalog,
    )
    .expect("parse ok");
    let msg = handle_close(&mut state, DescribeKind::Statement, "s");
    assert!(matches!(msg, BackendMessage::CloseComplete));
    assert!(!state.statements.contains_key("s"));
}

#[test]
fn describe_statement_emits_parameter_then_row_description() {
    let catalog = fixture_catalog();
    let mut state = ExtendedConnState::new();
    handle_parse(
        &mut state,
        "s".to_string(),
        "SELECT id FROM users WHERE id = $1".to_string(),
        vec![PG_OID_INT4],
        &catalog,
    )
    .expect("parse ok");
    let msgs = handle_describe_statement(
        &state,
        "s",
        Some(&catalog as &dyn ultrasql_planner::Catalog),
    )
    .expect("describe ok");
    assert_eq!(msgs.len(), 2);
    assert!(matches!(
        msgs[0],
        BackendMessage::ParameterDescription { .. }
    ));
    assert!(matches!(msgs[1], BackendMessage::RowDescription { .. }));
}

#[test]
fn describe_portal_for_select_returns_row_description() {
    let catalog = fixture_catalog();
    let mut state = ExtendedConnState::new();
    handle_parse(
        &mut state,
        "s".to_string(),
        "SELECT id FROM users".to_string(),
        vec![],
        &catalog,
    )
    .expect("parse ok");
    handle_bind(&mut state, String::new(), "s", &[], &[], vec![], None).expect("bind ok");
    let msg = handle_describe_portal(&state, "").expect("describe ok");
    assert!(matches!(msg, BackendMessage::RowDescription { .. }));
}

#[test]
fn bind_unknown_statement_errors() {
    let mut state = ExtendedConnState::new();
    let err = handle_bind(&mut state, String::new(), "nope", &[], &[], vec![], None)
        .expect_err("bind must fail");
    assert!(matches!(err, ServerError::Unsupported(_)));
}
