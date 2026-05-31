//! Unit tests for the Extended Query module.

use super::codec::{DecodeError, decode_param};
use super::handlers::{
    handle_bind, handle_close, handle_describe_portal, handle_describe_statement, handle_parse,
};
use super::params::walk_plan_exprs;
use super::substitute::substitute_parameters_in_plan;
use super::{ExtendedConnState, PG_OID_BOOL, PG_OID_INT4, PG_OID_INT8, PG_OID_TEXT};
use crate::error::ServerError;
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
fn substitute_int16_parameter_matches_int32_column_predicate() {
    // psycopg3/libpq may send small Python integers as binary int2 when
    // the parse message leaves parameter types unspecified. The prepared
    // plan still compares against an INT column, so substitution must widen
    // the concrete literal before execution.
    let catalog = fixture_catalog();
    let mut state = ExtendedConnState::new();
    let _ = handle_parse(
        &mut state,
        "s1".to_string(),
        "SELECT id FROM users WHERE id = $1".to_string(),
        vec![],
        &catalog,
    )
    .expect("parse ok");
    let stmt = state.statements.get("s1").unwrap();
    assert_eq!(stmt.n_params, 1);

    let sub = substitute_parameters_in_plan(stmt.plan.as_ref().unwrap(), &[Value::Int16(2)]);
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
                        value: Value::Int32(2),
                        ..
                    },
                )
                | (
                    ScalarExpr::Literal {
                        value: Value::Int32(2),
                        ..
                    },
                    ScalarExpr::Column { .. },
                ) => found = true,
                _ => {}
            }
        }
    });
    assert!(
        found,
        "Int16 parameter was not widened to Int32 predicate literal"
    );
}

#[test]
fn substitute_int16_parameter_matches_insert_target_column() {
    // INSERT VALUES carries the target table schema after binding. When a
    // driver supplies a narrower concrete integer at Bind time, the cell
    // must still be widened to the destination column type.
    let catalog = fixture_catalog();
    let mut state = ExtendedConnState::new();
    let _ = handle_parse(
        &mut state,
        "s1".to_string(),
        "INSERT INTO users VALUES ($1, $2, $3)".to_string(),
        vec![],
        &catalog,
    )
    .expect("parse ok");
    let stmt = state.statements.get("s1").unwrap();
    assert_eq!(stmt.n_params, 3);

    let sub = substitute_parameters_in_plan(
        stmt.plan.as_ref().unwrap(),
        &[
            Value::Int16(4),
            Value::Text("Alan".to_string()),
            Value::Float64(1.5),
        ],
    );
    let LogicalPlan::Insert { source, .. } = sub else {
        panic!("expected Insert plan");
    };
    let LogicalPlan::Values { rows, schema } = source.as_ref() else {
        panic!("expected Values source");
    };
    assert_eq!(schema.field_at(0).data_type, DataType::Int32);
    assert!(matches!(
        rows[0][0],
        ScalarExpr::Literal {
            value: Value::Int32(4),
            data_type: DataType::Int32
        }
    ));
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

#[test]
fn parse_rejects_parameter_slots_beyond_protocol_count() {
    let catalog = fixture_catalog();
    let mut state = ExtendedConnState::new();
    let err = handle_parse(
        &mut state,
        "s".to_string(),
        "SELECT $32768".to_string(),
        vec![],
        &catalog,
    )
    .expect_err("extended protocol cannot describe more than i16::MAX parameters");

    assert!(
        matches!(err, ServerError::Unsupported(message) if message.contains("parameter count exceeds protocol limit")),
        "unexpected error: {err:?}"
    );
    assert!(!state.statements.contains_key("s"));
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
fn explicit_cast_parameter_infers_text_oid_for_binary_bind() {
    let catalog = fixture_catalog();
    let mut state = ExtendedConnState::new();
    handle_parse(
        &mut state,
        "s".to_string(),
        "SELECT $1::VARCHAR = 'sqlalchemy_cert'".to_string(),
        vec![],
        &catalog,
    )
    .expect("parse ok");
    let msgs = handle_describe_statement(
        &state,
        "s",
        Some(&catalog as &dyn ultrasql_planner::Catalog),
    )
    .expect("describe ok");
    assert!(matches!(
        &msgs[0],
        BackendMessage::ParameterDescription { type_oids } if type_oids == &vec![PG_OID_TEXT]
    ));

    handle_bind(
        &mut state,
        String::new(),
        "s",
        &[1],
        &[Some(b"sqlalchemy_cert".to_vec())],
        vec![],
        Some(&catalog as &dyn ultrasql_planner::Catalog),
    )
    .expect("bind ok");
    let portal = state.portals.get("").expect("unnamed portal");
    let plan = portal.plan.as_ref().expect("bound plan");
    let mut saw_text = false;
    let mut saw_bytea = false;
    walk_plan_exprs(plan, &mut |expr| {
        if let ScalarExpr::Literal { value, .. } = expr {
            match value {
                Value::Text(text) if text == "sqlalchemy_cert" => saw_text = true,
                Value::Bytea(_) => saw_bytea = true,
                _ => {}
            }
        }
    });
    assert!(saw_text, "cast parameter should decode as text");
    assert!(!saw_bytea, "cast parameter must not fall back to bytea");
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
