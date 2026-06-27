//! Postfix decorator parser tests.
//!
//! Covers `BETWEEN`, `IS DISTINCT FROM`, `IS TRUE`/`FALSE`/`UNKNOWN`,
//! the postfix `::` cast, array subscript and slice, `AT TIME ZONE`,
//! and `OVERLAPS`. These pair with the production code in
//! [`super::super::expr_postfix`].

use super::*;
use crate::ast::{BinaryOp, Expr};

// ── BETWEEN ─────────────────────────────────────────────────────────────

#[test]
fn between_basic() {
    let expr = parse_expr("x BETWEEN 1 AND 10");
    let Expr::Between {
        negated, symmetric, ..
    } = expr
    else {
        panic!()
    };
    assert!(!negated);
    assert!(!symmetric);
}

#[test]
fn not_between() {
    let expr = parse_expr("x NOT BETWEEN 1 AND 10");
    let Expr::Between { negated, .. } = expr else {
        panic!()
    };
    assert!(negated);
}

#[test]
fn between_symmetric() {
    let expr = parse_expr("x BETWEEN SYMMETRIC 10 AND 1");
    let Expr::Between { symmetric, .. } = expr else {
        panic!()
    };
    assert!(symmetric);
}

#[test]
fn between_missing_and_is_error() {
    let err = parse_err("x BETWEEN 1 10");
    assert!(matches!(err, ParseError::Expected { .. }));
}

#[test]
fn between_and_does_not_consume_outer_and() {
    // The AND inside BETWEEN must not eat the outer boolean AND.
    let expr = parse_expr("x BETWEEN 1 AND 10 AND y = 2");
    // Top-level should be a boolean AND of (Between, Binary{Eq}).
    assert!(matches!(
        expr,
        Expr::Binary {
            op: BinaryOp::And,
            ..
        }
    ));
}

// ── COLLATE ─────────────────────────────────────────────────────────────

#[test]
fn collate_postfix_parses_expression() {
    let expr = parse_expr("name COLLATE \"C\"");
    let Expr::Collate {
        expr, collation, ..
    } = expr
    else {
        panic!("expected COLLATE expression")
    };
    assert!(matches!(*expr, Expr::Column { .. }));
    assert_eq!(collation.to_string(), "C");
}

#[test]
fn collate_postfix_allows_schema_qualified_name() {
    let expr = parse_expr("name COLLATE pg_catalog.\"POSIX\"");
    let Expr::Collate { collation, .. } = expr else {
        panic!("expected COLLATE expression")
    };
    assert_eq!(collation.parts.len(), 2);
    assert_eq!(collation.parts[0].value, "pg_catalog");
    assert_eq!(collation.parts[1].value, "POSIX");
}

// ── IS DISTINCT FROM ────────────────────────────────────────────────────

#[test]
fn is_distinct_from_basic() {
    let expr = parse_expr("x IS DISTINCT FROM NULL");
    assert!(matches!(expr, Expr::IsDistinctFrom { negated: false, .. }));
}

#[test]
fn is_not_distinct_from() {
    let expr = parse_expr("x IS NOT DISTINCT FROM NULL");
    assert!(matches!(expr, Expr::IsDistinctFrom { negated: true, .. }));
}

#[test]
fn is_distinct_from_missing_from_is_error() {
    let err = parse_err("x IS DISTINCT NULL");
    assert!(matches!(err, ParseError::Expected { .. }));
}

// ── IS TRUE / FALSE / UNKNOWN ────────────────────────────────────────────

#[test]
fn is_true() {
    let expr = parse_expr("x IS TRUE");
    assert!(matches!(
        expr,
        Expr::IsBoolean {
            value: true,
            is_unknown: false,
            negated: false,
            ..
        }
    ));
}

#[test]
fn is_not_false() {
    let expr = parse_expr("x IS NOT FALSE");
    assert!(matches!(
        expr,
        Expr::IsBoolean {
            value: false,
            negated: true,
            ..
        }
    ));
}

#[test]
fn is_unknown() {
    let expr = parse_expr("x IS UNKNOWN");
    assert!(matches!(
        expr,
        Expr::IsBoolean {
            is_unknown: true,
            negated: false,
            ..
        }
    ));
}

#[test]
fn is_not_unknown() {
    let expr = parse_expr("x IS NOT UNKNOWN");
    assert!(matches!(
        expr,
        Expr::IsBoolean {
            is_unknown: true,
            negated: true,
            ..
        }
    ));
}

// ── postfix cast `::` ────────────────────────────────────────────────────

#[test]
fn postfix_cast_integer() {
    let expr = parse_expr("x::integer");
    let Expr::PostfixCast { target, .. } = expr else {
        panic!()
    };
    assert_eq!(target.value, "integer");
}

#[test]
fn postfix_cast_array_type() {
    // `::int[]` must be the cast target, not a subscript on `(x::int)`.
    let expr = parse_expr("x::int[]");
    let Expr::PostfixCast { target, expr, .. } = expr else {
        panic!("expected postfix array cast, got {expr:?}");
    };
    assert_eq!(target.value, "int[]");
    assert!(
        matches!(*expr, Expr::Column { .. }),
        "operand should be the column, not a subscript"
    );
}

#[test]
fn postfix_cast_array_literal_text() {
    // `'{1,2,3}'::int[]` previously mis-parsed the `[` as a subscript.
    let expr = parse_expr("'{1,2,3}'::int[]");
    let Expr::PostfixCast { target, .. } = expr else {
        panic!("expected postfix array cast, got {expr:?}");
    };
    assert_eq!(target.value, "int[]");
}

#[test]
fn postfix_cast_array_type_collapses_dimensions_and_size() {
    // Multiple `[]` and a declared size all collapse to a single `[]`.
    for (sql, expected) in [
        ("x::text[][]", "text[]"),
        ("x::int[3]", "int[]"),
        ("x::numeric[]", "numeric[]"),
    ] {
        let expr = parse_expr(sql);
        let Expr::PostfixCast { target, .. } = expr else {
            panic!("expected postfix array cast for {sql}")
        };
        assert_eq!(target.value, expected);
    }
}

#[test]
fn cast_expr_array_type() {
    // `CAST(x AS text[])` prefix form.
    let expr = parse_expr("CAST(x AS text[])");
    let Expr::Cast { target, .. } = expr else {
        panic!("expected CAST array, got {expr:?}");
    };
    assert_eq!(target.value, "text[]");
}

#[test]
fn postfix_cast_vector_with_modifier() {
    let expr = parse_expr("'[1,2,3]'::VECTOR(3)");
    let Expr::PostfixCast { target, .. } = expr else {
        panic!()
    };
    assert_eq!(target.value, "vector(3)");
}

#[test]
fn postfix_cast_vector_family_with_modifier() {
    for (sql, expected) in [
        ("'[1,2,3]'::HALFVEC(3)", "halfvec(3)"),
        ("'{1:1}/5'::SPARSEVEC(5)", "sparsevec(5)"),
        ("'1010'::BITVEC(4)", "bitvec(4)"),
    ] {
        let expr = parse_expr(sql);
        let Expr::PostfixCast { target, .. } = expr else {
            panic!("expected postfix cast for {sql}")
        };
        assert_eq!(target.value, expected);
    }
}

#[test]
fn postfix_cast_chain() {
    // x::text::varchar — two successive casts.
    let expr = parse_expr("x::text::varchar");
    let Expr::PostfixCast {
        expr: inner,
        target: outer_target,
        ..
    } = expr
    else {
        panic!()
    };
    assert_eq!(outer_target.value, "varchar");
    assert!(matches!(*inner, Expr::PostfixCast { .. }));
}

#[test]
fn postfix_cast_accepts_schema_qualified_pg_catalog_type() {
    let expr = parse_expr("x::pg_catalog.regtype::pg_catalog.text");
    let Expr::PostfixCast {
        expr: inner,
        target: outer_target,
        ..
    } = expr
    else {
        panic!()
    };
    assert_eq!(outer_target.value, "pg_catalog.text");
    let Expr::PostfixCast {
        target: inner_target,
        ..
    } = *inner
    else {
        panic!()
    };
    assert_eq!(inner_target.value, "pg_catalog.regtype");
}

#[test]
fn postfix_cast_missing_type_is_error() {
    let err = parse_err("x::");
    assert!(matches!(
        err,
        ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
    ));
}

// ── array subscript `[]` ─────────────────────────────────────────────────

#[test]
fn array_subscript_basic() {
    let expr = parse_expr("arr[1]");
    assert!(matches!(expr, Expr::ArraySubscript { .. }));
}

#[test]
fn array_subscript_expression_index() {
    let expr = parse_expr("arr[i + 1]");
    let Expr::ArraySubscript { index, .. } = expr else {
        panic!()
    };
    assert!(matches!(
        *index,
        Expr::Binary {
            op: BinaryOp::Add,
            ..
        }
    ));
}

#[test]
fn array_subscript_unclosed_is_error() {
    let err = parse_err("arr[1");
    assert!(matches!(
        err,
        ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
    ));
}

// ── array slice `[:]` ────────────────────────────────────────────────────

#[test]
fn array_slice_both_bounds() {
    let expr = parse_expr("arr[2:5]");
    let Expr::ArraySlice { lower, upper, .. } = expr else {
        panic!()
    };
    assert!(lower.is_some());
    assert!(upper.is_some());
}

#[test]
fn array_slice_lower_only() {
    let expr = parse_expr("arr[2:]");
    let Expr::ArraySlice { lower, upper, .. } = expr else {
        panic!()
    };
    assert!(lower.is_some());
    assert!(upper.is_none());
}

#[test]
fn array_slice_upper_only() {
    let expr = parse_expr("arr[:5]");
    let Expr::ArraySlice { lower, upper, .. } = expr else {
        panic!()
    };
    assert!(lower.is_none());
    assert!(upper.is_some());
}

// ── AT TIME ZONE ─────────────────────────────────────────────────────────

#[test]
fn at_time_zone_string_literal() {
    let expr = parse_expr("now() AT TIME ZONE 'UTC'");
    assert!(matches!(expr, Expr::AtTimeZone { .. }));
}

#[test]
fn at_time_zone_identifier() {
    let expr = parse_expr("ts AT TIME ZONE tz_col");
    assert!(matches!(expr, Expr::AtTimeZone { .. }));
}

#[test]
fn at_time_zone_missing_zone_expr_is_error() {
    // The zone expression is mandatory after AT TIME ZONE.
    let err = parse_err("ts AT TIME ZONE");
    assert!(matches!(
        err,
        ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
    ));
}

// ── OVERLAPS ─────────────────────────────────────────────────────────────

#[test]
fn overlaps_basic() {
    let expr = parse_expr("(a, b) OVERLAPS (c, d)");
    assert!(matches!(expr, Expr::Overlaps { .. }));
}

#[test]
fn overlaps_fields_are_captured() {
    let expr = parse_expr("(t1, t2) OVERLAPS (t3, t4)");
    let Expr::Overlaps {
        left_start,
        left_end,
        right_start,
        right_end,
        ..
    } = expr
    else {
        panic!()
    };
    // Check all four fields were parsed as column references.
    assert!(matches!(*left_start, Expr::Column { .. }));
    assert!(matches!(*left_end, Expr::Column { .. }));
    assert!(matches!(*right_start, Expr::Column { .. }));
    assert!(matches!(*right_end, Expr::Column { .. }));
}

#[test]
fn overlaps_missing_second_pair_is_error() {
    let err = parse_err("(a, b) OVERLAPS c");
    assert!(matches!(err, ParseError::Expected { .. }));
}
