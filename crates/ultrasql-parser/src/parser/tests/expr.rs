//! Expression-level parser tests.
//!
//! Covers basic expression precedence, function calls, `CAST`, the
//! `CASE`, `COALESCE`, `NULLIF`, `GREATEST`/`LEAST`, and `ROW`
//! constructions. Postfix decorators live in [`super::postfix`];
//! binary-operator coverage lives in [`super::binary_ops`].

use super::*;
use crate::ast::{BinaryOp, Expr, Literal, SelectItem, Statement};

#[test]
fn expression_precedence() {
    let stmt = parse("SELECT 1 + 2 * 3 = 7 FROM x");
    let Statement::Select(s) = stmt else { panic!() };
    // (1 + (2 * 3)) = 7  → top operator is Eq.
    let SelectItem::Expr { expr, .. } = &s.projection[0] else {
        panic!()
    };
    assert!(matches!(
        expr,
        Expr::Binary {
            op: BinaryOp::Eq,
            ..
        }
    ));
}

#[test]
fn function_call_with_distinct() {
    let stmt = parse("SELECT count(DISTINCT id) FROM users");
    let Statement::Select(s) = stmt else { panic!() };
    let SelectItem::Expr { expr, .. } = &s.projection[0] else {
        panic!()
    };
    let Expr::Call {
        distinct,
        args,
        name,
        ..
    } = expr
    else {
        panic!()
    };
    assert!(distinct);
    assert_eq!(args.len(), 1);
    assert_eq!(name.parts[0].value, "count");
}

#[test]
fn cast_expression() {
    let stmt = parse("SELECT CAST(x AS integer) FROM t");
    let Statement::Select(s) = stmt else { panic!() };
    let SelectItem::Expr { expr, .. } = &s.projection[0] else {
        panic!()
    };
    assert!(matches!(expr, Expr::Cast { .. }));
}

#[test]
fn cast_expression_accepts_vector_type_modifier() {
    let stmt = parse("SELECT CAST('[1,2,3]' AS VECTOR(3)) FROM t");
    let Statement::Select(s) = stmt else { panic!() };
    let SelectItem::Expr { expr, .. } = &s.projection[0] else {
        panic!()
    };
    let Expr::Cast { target, .. } = expr else {
        panic!("expected CAST expression");
    };
    assert_eq!(target.value, "vector(3)");
}

#[test]
fn cast_expression_accepts_vector_family_type_modifiers() {
    for (sql, expected) in [
        ("SELECT CAST('[1,2,3]' AS HALFVEC(3)) FROM t", "halfvec(3)"),
        (
            "SELECT CAST('{1:1}/5' AS SPARSEVEC(5)) FROM t",
            "sparsevec(5)",
        ),
        ("SELECT CAST('1010' AS BITVEC(4)) FROM t", "bitvec(4)"),
    ] {
        let stmt = parse(sql);
        let Statement::Select(s) = stmt else { panic!() };
        let SelectItem::Expr { expr, .. } = &s.projection[0] else {
            panic!()
        };
        let Expr::Cast { target, .. } = expr else {
            panic!("expected CAST expression for {sql}");
        };
        assert_eq!(target.value, expected);
    }
}

#[test]
fn cast_expression_accepts_numeric_type_modifiers() {
    for (sql, expected) in [
        (
            "SELECT CAST('12.30' AS NUMERIC(8,2)) FROM t",
            "numeric(8,2)",
        ),
        ("SELECT CAST('12' AS DECIMAL(8)) FROM t", "decimal(8)"),
    ] {
        let stmt = parse(sql);
        let Statement::Select(s) = stmt else { panic!() };
        let SelectItem::Expr { expr, .. } = &s.projection[0] else {
            panic!()
        };
        let Expr::Cast { target, .. } = expr else {
            panic!("expected CAST expression for {sql}");
        };
        assert_eq!(target.value, expected);
    }
}

#[test]
fn cast_expression_preserves_qualified_and_quoted_type_name() {
    let stmt = parse(r#"SELECT CAST('ok' AS app."mood.type") FROM t"#);
    let Statement::Select(s) = stmt else { panic!() };
    let SelectItem::Expr { expr, .. } = &s.projection[0] else {
        panic!()
    };
    let Expr::Cast { target, .. } = expr else {
        panic!("expected CAST expression");
    };
    assert_eq!(target.value, r#"app."mood.type""#);

    let stmt = parse(r#"SELECT CAST('ok' AS "mood.type") FROM t"#);
    let Statement::Select(s) = stmt else { panic!() };
    let SelectItem::Expr { expr, .. } = &s.projection[0] else {
        panic!()
    };
    let Expr::Cast { target, .. } = expr else {
        panic!("expected CAST expression");
    };
    assert_eq!(target.value, r#""mood.type""#);
}

#[test]
fn xml_parse_and_serialize_syntax_lower_to_builtin_calls() {
    let stmt = parse(
        "SELECT \
            XMLPARSE(DOCUMENT '<root/>'), \
            XMLSERIALIZE(CONTENT XML '<root/>' AS TEXT)",
    );
    let Statement::Select(s) = stmt else { panic!() };
    let SelectItem::Expr {
        expr: Expr::Call { name, args, .. },
        ..
    } = &s.projection[0]
    else {
        panic!("expected XMLPARSE builtin call");
    };
    assert_eq!(name.parts[0].value, "xmlparse");
    assert_eq!(args.len(), 2);
    assert!(matches!(
        &args[0],
        Expr::Literal(Literal::String { value, .. }) if value == "document"
    ));

    let SelectItem::Expr {
        expr: Expr::Call { name, args, .. },
        ..
    } = &s.projection[1]
    else {
        panic!("expected XMLSERIALIZE builtin call");
    };
    assert_eq!(name.parts[0].value, "xmlserialize");
    assert_eq!(args.len(), 3);
    assert!(matches!(
        &args[0],
        Expr::Literal(Literal::String { value, .. }) if value == "content"
    ));
    assert!(matches!(
        &args[2],
        Expr::Literal(Literal::String { value, .. }) if value == "text"
    ));
}

#[test]
fn vector_typed_literal() {
    let expr = parse_expr("VECTOR '[1,2,3]'");
    let Expr::Literal(Literal::Typed {
        type_name, value, ..
    }) = expr
    else {
        panic!("expected typed vector literal");
    };
    assert_eq!(type_name, "vector");
    assert_eq!(value, "[1,2,3]");
}

#[test]
fn vector_typed_literal_with_modifier() {
    let expr = parse_expr("VECTOR(3) '[1,2,3]'");
    let Expr::Literal(Literal::Typed {
        type_name, value, ..
    }) = expr
    else {
        panic!("expected typed vector literal");
    };
    assert_eq!(type_name, "vector(3)");
    assert_eq!(value, "[1,2,3]");
}

#[test]
fn vector_family_typed_literals_with_modifiers() {
    for (sql, expected_type, expected_value) in [
        ("HALFVEC(3) '[1,2,3]'", "halfvec(3)", "[1,2,3]"),
        ("SPARSEVEC(5) '{1:1}/5'", "sparsevec(5)", "{1:1}/5"),
        ("BITVEC(4) '1010'", "bitvec(4)", "1010"),
    ] {
        let expr = parse_expr(sql);
        let Expr::Literal(Literal::Typed {
            type_name, value, ..
        }) = expr
        else {
            panic!("expected typed vector-family literal for {sql}");
        };
        assert_eq!(type_name, expected_type);
        assert_eq!(value, expected_value);
    }
}

#[test]
fn is_null_chain() {
    let stmt = parse("SELECT x IS NOT NULL FROM t");
    let Statement::Select(s) = stmt else { panic!() };
    let SelectItem::Expr { expr, .. } = &s.projection[0] else {
        panic!()
    };
    assert!(matches!(expr, Expr::IsNull { negated: true, .. }));
}

#[test]
fn parameter_token() {
    let stmt = parse("SELECT $1 FROM t WHERE x = $2");
    let Statement::Select(s) = stmt else { panic!() };
    let SelectItem::Expr { expr, .. } = &s.projection[0] else {
        panic!()
    };
    assert!(matches!(expr, Expr::Parameter { index: 1, .. }));
}

#[test]
fn parameter_zero_rejected() {
    let err = parse_err("$0");
    assert!(
        matches!(err, ParseError::ParameterOutOfRange { ref text, .. } if text == "$0"),
        "unexpected error: {err}"
    );
}

// ── CASE expressions ────────────────────────────────────────────────────

#[test]
fn searched_case_basic() {
    let expr = parse_expr("CASE WHEN x > 0 THEN 'pos' WHEN x < 0 THEN 'neg' ELSE 'zero' END");
    let Expr::Case {
        operand,
        branches,
        else_expr,
        ..
    } = expr
    else {
        panic!()
    };
    assert!(operand.is_none(), "searched CASE has no operand");
    assert_eq!(branches.len(), 2);
    assert!(else_expr.is_some());
}

#[test]
fn simple_case_basic() {
    let expr = parse_expr("CASE x WHEN 1 THEN 'one' WHEN 2 THEN 'two' END");
    let Expr::Case {
        operand,
        branches,
        else_expr,
        ..
    } = expr
    else {
        panic!()
    };
    assert!(operand.is_some(), "simple CASE has operand");
    assert_eq!(branches.len(), 2);
    assert!(else_expr.is_none());
}

#[test]
fn case_no_when_is_error() {
    // CASE END without at least one WHEN clause is a parse error.
    let err = parse_err("CASE END");
    assert!(matches!(
        err,
        ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
    ));
}

// ── COALESCE ────────────────────────────────────────────────────────────

#[test]
fn coalesce_two_args() {
    let expr = parse_expr("COALESCE(a, 0)");
    let Expr::Coalesce { args, .. } = expr else {
        panic!()
    };
    assert_eq!(args.len(), 2);
}

#[test]
fn coalesce_many_args() {
    let expr = parse_expr("COALESCE(a, b, c, d)");
    let Expr::Coalesce { args, .. } = expr else {
        panic!()
    };
    assert_eq!(args.len(), 4);
}

#[test]
fn coalesce_empty_is_error() {
    let err = parse_err("COALESCE()");
    assert!(matches!(
        err,
        ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
    ));
}

// ── NULLIF ──────────────────────────────────────────────────────────────

#[test]
fn nullif_basic() {
    let expr = parse_expr("NULLIF(x, 0)");
    assert!(matches!(expr, Expr::NullIf { .. }));
}

#[test]
fn nullif_with_string() {
    let expr = parse_expr("NULLIF(name, '')");
    let Expr::NullIf { a, b, .. } = expr else {
        panic!()
    };
    assert!(matches!(*a, Expr::Column { .. }));
    assert!(matches!(*b, Expr::Literal(_)));
}

#[test]
fn nullif_too_few_args_is_error() {
    let err = parse_err("NULLIF(x)");
    assert!(matches!(
        err,
        ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
    ));
}

// ── GREATEST / LEAST ────────────────────────────────────────────────────

#[test]
fn greatest_two_args() {
    let expr = parse_expr("GREATEST(a, b)");
    let Expr::Greatest { args, .. } = expr else {
        panic!()
    };
    assert_eq!(args.len(), 2);
}

#[test]
fn least_many_args() {
    let expr = parse_expr("LEAST(1, 2, 3, 4)");
    let Expr::Least { args, .. } = expr else {
        panic!()
    };
    assert_eq!(args.len(), 4);
}

#[test]
fn greatest_empty_is_error() {
    let err = parse_err("GREATEST()");
    assert!(matches!(
        err,
        ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
    ));
}

// ── ROW constructor ──────────────────────────────────────────────────────

#[test]
fn row_explicit_keyword() {
    let expr = parse_expr("ROW(1, 2, 3)");
    let Expr::Row { fields, .. } = expr else {
        panic!()
    };
    assert_eq!(fields.len(), 3);
}

#[test]
fn row_single_field() {
    let expr = parse_expr("ROW(42)");
    let Expr::Row { fields, .. } = expr else {
        panic!()
    };
    assert_eq!(fields.len(), 1);
}

#[test]
fn row_empty_is_accepted() {
    // PostgreSQL accepts ROW() as a zero-element row constructor.
    let expr = parse_expr("ROW()");
    let Expr::Row { fields, .. } = expr else {
        panic!()
    };
    assert_eq!(fields.len(), 0);
}

#[test]
fn row_unclosed_paren_is_error() {
    let err = parse_err("ROW(1, 2");
    assert!(matches!(
        err,
        ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
    ));
}

#[test]
fn array_constructor_accepts_parenthesized_subquery() {
    let expr = parse_expr("array(select rolname from pg_roles)");
    assert!(matches!(expr, Expr::Subquery { .. }));
}

#[test]
fn over_clause_partition_and_order() {
    let expr = parse_expr("row_number() OVER (PARTITION BY a ORDER BY b ASC)");
    let Expr::Call { over, .. } = expr else {
        panic!()
    };
    let spec = over.expect("OVER spec");
    assert_eq!(spec.partition_by.len(), 1);
    assert_eq!(spec.order_by.len(), 1);
    assert_eq!(spec.order_by[0].direction, SortDirection::Asc);
}

#[test]
fn over_clause_empty_window() {
    let expr = parse_expr("count(*) OVER ()");
    let Expr::Call { over, .. } = expr else {
        panic!()
    };
    let spec = over.expect("OVER spec");
    assert!(spec.partition_by.is_empty());
    assert!(spec.order_by.is_empty());
}

#[test]
fn function_call_without_over_keeps_none() {
    let expr = parse_expr("count(*)");
    let Expr::Call { over, .. } = expr else {
        panic!()
    };
    assert!(over.is_none());
}

#[test]
fn over_clause_without_frame_keeps_frame_none() {
    let expr = parse_expr("count(*) OVER ()");
    let Expr::Call { over, .. } = expr else {
        panic!()
    };
    let spec = over.expect("OVER spec");
    assert!(spec.frame.is_none());
}

#[test]
fn frame_rows_between_preceding_and_current_row() {
    use crate::ast::{FrameBound, FrameExclusion, FrameUnits};
    let expr = parse_expr("sum(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)");
    let Expr::Call { over, .. } = expr else {
        panic!()
    };
    let frame = over.expect("OVER spec").frame.expect("frame");
    assert_eq!(frame.units, FrameUnits::Rows);
    assert!(matches!(frame.start, FrameBound::Preceding(_)));
    assert_eq!(frame.end, FrameBound::CurrentRow);
    assert_eq!(frame.exclude, FrameExclusion::NoOthers);
}

#[test]
fn frame_range_unbounded_both_sides() {
    use crate::ast::{FrameBound, FrameUnits};
    let expr =
        parse_expr("sum(v) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)");
    let Expr::Call { over, .. } = expr else {
        panic!()
    };
    let frame = over.expect("OVER spec").frame.expect("frame");
    assert_eq!(frame.units, FrameUnits::Range);
    assert_eq!(frame.start, FrameBound::UnboundedPreceding);
    assert_eq!(frame.end, FrameBound::UnboundedFollowing);
}

#[test]
fn frame_groups_current_row_to_following() {
    use crate::ast::{FrameBound, FrameUnits};
    let expr = parse_expr("sum(v) OVER (ORDER BY g GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING)");
    let Expr::Call { over, .. } = expr else {
        panic!()
    };
    let frame = over.expect("OVER spec").frame.expect("frame");
    assert_eq!(frame.units, FrameUnits::Groups);
    assert_eq!(frame.start, FrameBound::CurrentRow);
    assert!(matches!(frame.end, FrameBound::Following(_)));
}

#[test]
fn frame_bare_start_expands_end_to_current_row() {
    use crate::ast::{FrameBound, FrameUnits};
    let expr = parse_expr("sum(v) OVER (ORDER BY id ROWS 2 PRECEDING)");
    let Expr::Call { over, .. } = expr else {
        panic!()
    };
    let frame = over.expect("OVER spec").frame.expect("frame");
    assert_eq!(frame.units, FrameUnits::Rows);
    assert!(matches!(frame.start, FrameBound::Preceding(_)));
    // Bare frame_start shorthand fills the end with CURRENT ROW.
    assert_eq!(frame.end, FrameBound::CurrentRow);
}

#[test]
fn frame_exclude_variants_parse() {
    use crate::ast::FrameExclusion;
    let cases = [
        ("EXCLUDE CURRENT ROW", FrameExclusion::CurrentRow),
        ("EXCLUDE GROUP", FrameExclusion::Group),
        ("EXCLUDE TIES", FrameExclusion::Ties),
        ("EXCLUDE NO OTHERS", FrameExclusion::NoOthers),
    ];
    for (excl, expected) in cases {
        let sql = format!(
            "sum(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW {excl})"
        );
        let expr = parse_expr(&sql);
        let Expr::Call { over, .. } = expr else {
            panic!()
        };
        let frame = over.expect("OVER spec").frame.expect("frame");
        assert_eq!(frame.exclude, expected, "exclude clause: {excl}");
    }
}
