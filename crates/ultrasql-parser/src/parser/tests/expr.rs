//! Expression-level parser tests.
//!
//! Covers basic expression precedence, function calls, `CAST`, the
//! `CASE`, `COALESCE`, `NULLIF`, `GREATEST`/`LEAST`, and `ROW`
//! constructions. Postfix decorators live in [`super::postfix`];
//! binary-operator coverage lives in [`super::binary_ops`].

use super::*;
use crate::ast::{BinaryOp, Expr, SelectItem, Statement};

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
