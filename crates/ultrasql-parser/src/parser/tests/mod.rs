//! Parser tests.
//!
//! The tests are bucketed by what they exercise:
//!
//! - This file holds statement-level tests (SELECT shape, BEGIN/COMMIT,
//!   SET TRANSACTION, parens-depth guard, statement-stream parsing) plus
//!   the `parse` / `parse_expr` / `parse_err` helpers used by sibling
//!   modules.
//! - [`expr`] holds the expression-level tests for expression precedence,
//!   function calls, `CAST`, `CASE`, `COALESCE`, `NULLIF`, `GREATEST`,
//!   `LEAST`, and `ROW`.
//! - [`postfix`] holds tests for the postfix decorators `BETWEEN`, `IS`,
//!   `::`, `[…]`, `AT TIME ZONE`, and `OVERLAPS`.
//! - [`binary_ops`] holds tests for the regex / bitwise / shift / JSON
//!   operators and the precedence cross-product.

use super::*;
use crate::ast::{Distinct, Expr, SelectItem, SortDirection, Statement};

mod binary_ops;
mod expr;
mod postfix;

fn max_parse_depth_usize() -> usize {
    usize::try_from(MAX_PARSE_DEPTH).expect("MAX_PARSE_DEPTH fits usize")
}

/// Parse a full statement and return it, panicking on error.
pub(super) fn parse(src: &str) -> Statement {
    Parser::new(src)
        .parse_statement()
        .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
}

/// Parse a bare expression from `SELECT <expr>` and return it.
pub(super) fn parse_expr(src: &str) -> Expr {
    let sql = format!("SELECT {src}");
    let stmt = Parser::new(&sql)
        .parse_statement()
        .unwrap_or_else(|e| panic!("parse_expr failed for {src:?}: {e}"));
    let Statement::Select(s) = stmt else { panic!() };
    let SelectItem::Expr { expr, .. } = s.projection.into_iter().next().unwrap() else {
        panic!()
    };
    expr
}

/// Expect parsing `SELECT <src>` to produce a [`ParseError`].
pub(super) fn parse_err(src: &str) -> ParseError {
    let sql = format!("SELECT {src}");
    Parser::new(&sql)
        .parse_statement()
        .expect_err("expected parse error")
}

#[test]
fn select_star() {
    let stmt = parse("SELECT * FROM users");
    let Statement::Select(s) = stmt else { panic!() };
    assert!(matches!(s.distinct, Distinct::None));
    assert!(matches!(s.projection[0], SelectItem::Wildcard { .. }));
    assert!(!s.from.is_empty());
}

#[test]
fn select_columns_and_alias() {
    let stmt = parse("SELECT id, name AS n FROM users");
    let Statement::Select(s) = stmt else { panic!() };
    assert_eq!(s.projection.len(), 2);
    if let SelectItem::Expr { alias, .. } = &s.projection[1] {
        assert_eq!(alias.as_ref().unwrap().value, "n");
    } else {
        panic!("expected aliased item");
    }
}

#[test]
fn select_alias_allows_copy_option_keyword_after_as() {
    let stmt = parse("SELECT typdelim AS delimiter FROM pg_type");
    let Statement::Select(s) = stmt else { panic!() };
    if let SelectItem::Expr { alias, .. } = &s.projection[0] {
        assert_eq!(alias.as_ref().unwrap().value, "delimiter");
    } else {
        panic!("expected aliased item");
    }
}

#[test]
fn select_alias_allows_comment_keyword_after_as() {
    let stmt = parse("SELECT col_description(a.attrelid, a.attnum) AS comment FROM pg_attribute a");
    let Statement::Select(s) = stmt else { panic!() };
    if let SelectItem::Expr { alias, .. } = &s.projection[0] {
        assert_eq!(alias.as_ref().unwrap().value, "comment");
    } else {
        panic!("expected aliased item");
    }
}

#[test]
fn select_alias_allows_identity_keyword_after_as() {
    let stmt = parse("SELECT '' AS identity FROM pg_attribute");
    let Statement::Select(s) = stmt else { panic!() };
    if let SelectItem::Expr { alias, .. } = &s.projection[0] {
        assert_eq!(alias.as_ref().unwrap().value, "identity");
    } else {
        panic!("expected aliased item");
    }
}

#[test]
fn select_projection_allows_locked_column_name() {
    let stmt = parse("SELECT LOCKED FROM databasechangeloglock");
    let Statement::Select(s) = stmt else { panic!() };
    let SelectItem::Expr {
        expr: Expr::Column { name },
        ..
    } = &s.projection[0]
    else {
        panic!("expected locked column");
    };
    assert_eq!(name.to_string(), "locked");
}

#[test]
fn select_eq_any_array_literal_parses_as_in_list() {
    let stmt =
        parse("SELECT * FROM pg_class WHERE relkind = ANY (ARRAY['r'::VARCHAR, 'v'::VARCHAR])");
    let Statement::Select(s) = stmt else { panic!() };
    let Some(Expr::InList { items, negated, .. }) = &s.r#where else {
        panic!("expected = ANY array to lower to IN-list");
    };
    assert!(!negated);
    assert_eq!(items.len(), 2);
}

#[test]
fn select_eq_any_array_function_parses() {
    let stmt = parse(
        "SELECT c.relname \
         FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = ANY (current_schemas(false))",
    );
    let Statement::Select(s) = stmt else { panic!() };
    let Some(Expr::AnyArray { .. }) = &s.r#where else {
        panic!("expected = ANY array expression");
    };
}

#[test]
fn select_with_where_clause() {
    let stmt = parse("SELECT id FROM users WHERE age >= 18 AND active = TRUE");
    let Statement::Select(s) = stmt else { panic!() };
    assert!(s.r#where.is_some());
}

#[test]
fn select_with_order_limit_offset() {
    let stmt = parse("SELECT id FROM users ORDER BY id DESC LIMIT 10 OFFSET 5");
    let Statement::Select(s) = stmt else { panic!() };
    assert_eq!(s.order_by.len(), 1);
    assert_eq!(s.order_by[0].direction, SortDirection::Desc);
    assert!(s.limit.is_some());
    assert!(s.offset.is_some());
}

#[test]
fn qualified_wildcard() {
    let stmt = parse("SELECT u.* FROM users u");
    let Statement::Select(s) = stmt else { panic!() };
    assert!(matches!(
        s.projection[0],
        SelectItem::QualifiedWildcard { .. }
    ));
}

#[test]
fn begin_commit_rollback_transactions() {
    assert!(matches!(parse("BEGIN"), Statement::Begin { .. }));
    assert!(matches!(
        parse("BEGIN TRANSACTION"),
        Statement::Begin { .. }
    ));
    assert!(matches!(parse("COMMIT"), Statement::Commit { .. }));
    assert!(matches!(parse("ROLLBACK"), Statement::Rollback { .. }));
}

#[test]
fn set_transaction_isolation_level() {
    use crate::ast::AstIsolationLevel;
    let stmt = parse("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
    let Statement::SetTransaction {
        isolation_level, ..
    } = stmt
    else {
        panic!()
    };
    assert_eq!(isolation_level, AstIsolationLevel::ReadCommitted);

    let stmt = parse("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
    let Statement::SetTransaction {
        isolation_level, ..
    } = stmt
    else {
        panic!()
    };
    assert_eq!(isolation_level, AstIsolationLevel::RepeatableRead);

    let stmt = parse("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
    let Statement::SetTransaction {
        isolation_level, ..
    } = stmt
    else {
        panic!()
    };
    assert_eq!(isolation_level, AstIsolationLevel::Serializable);

    // SET <var> = … must still parse as SetVar (not SetTransaction).
    let stmt = parse("SET search_path TO public");
    assert!(matches!(stmt, Statement::SetVar(_)));
}

#[test]
fn begin_isolation_level() {
    use crate::ast::AstIsolationLevel;
    let stmt = parse("BEGIN ISOLATION LEVEL READ COMMITTED");
    let Statement::Begin {
        isolation_level, ..
    } = stmt
    else {
        panic!()
    };
    assert_eq!(isolation_level, Some(AstIsolationLevel::ReadCommitted));

    let stmt = parse("BEGIN ISOLATION LEVEL READ UNCOMMITTED");
    let Statement::Begin {
        isolation_level, ..
    } = stmt
    else {
        panic!()
    };
    assert_eq!(isolation_level, Some(AstIsolationLevel::ReadCommitted));

    let stmt = parse("BEGIN ISOLATION LEVEL REPEATABLE READ");
    let Statement::Begin {
        isolation_level, ..
    } = stmt
    else {
        panic!()
    };
    assert_eq!(isolation_level, Some(AstIsolationLevel::RepeatableRead));

    let stmt = parse("BEGIN ISOLATION LEVEL SERIALIZABLE");
    let Statement::Begin {
        isolation_level, ..
    } = stmt
    else {
        panic!()
    };
    assert_eq!(isolation_level, Some(AstIsolationLevel::Serializable));

    let stmt = parse("BEGIN");
    let Statement::Begin {
        isolation_level, ..
    } = stmt
    else {
        panic!()
    };
    assert_eq!(isolation_level, None);
}

#[test]
fn parse_two_statements_separated_by_semicolons() {
    let mut p = Parser::new("BEGIN; SELECT 1 FROM t; COMMIT");
    let stmts = p.parse_statements().unwrap();
    assert_eq!(stmts.len(), 3);
    assert!(matches!(stmts[0], Statement::Begin { .. }));
    assert!(matches!(stmts[1], Statement::Select(_)));
    assert!(matches!(stmts[2], Statement::Commit { .. }));
}

#[test]
fn parse_statement_slices_preserve_source_text() {
    let mut p = Parser::new(" BEGIN ; SELECT ';' FROM t ; COMMIT ");
    let slices = p.parse_statement_slices().unwrap();
    assert_eq!(slices, vec!["BEGIN", "SELECT ';' FROM t", "COMMIT"]);
}

#[test]
fn describe_table_statement_is_accepted() {
    let stmt = Parser::new("DESCRIBE TABLE users")
        .parse_statement()
        .expect("DESCRIBE TABLE must parse");
    assert_eq!(stmt.span().start_usize(), 0);
}

#[test]
fn describe_object_statement_is_accepted() {
    let stmt = Parser::new("DESCRIBE users")
        .parse_statement()
        .expect("DESCRIBE object must parse");
    assert_eq!(stmt.span().start_usize(), 0);
}

#[test]
fn describe_query_statement_is_accepted() {
    let stmt = Parser::new("DESCRIBE SELECT 1 AS answer")
        .parse_statement()
        .expect("DESCRIBE query must parse");
    assert_eq!(stmt.span().start_usize(), 0);
}

#[test]
fn missing_from_returns_select_without_from() {
    let stmt = parse("SELECT 1 + 1");
    let Statement::Select(s) = stmt else { panic!() };
    assert!(s.from.is_empty());
}

#[test]
fn unexpected_eof_in_where_errors() {
    let err = Parser::new("SELECT x FROM t WHERE")
        .parse_statement()
        .unwrap_err();
    assert!(matches!(
        err,
        ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
    ));
}

#[test]
fn unsupported_statement_rejected() {
    // A truly unknown statement keyword should produce an error.
    let err = Parser::new("VACUUM t").parse_statement().unwrap_err();
    assert!(matches!(err, ParseError::Expected { .. }));
}

/// Adversarial input: deeply-nested parentheses must be rejected
/// with a `DepthExceeded` error rather than overflow the call
/// stack. The depth bound is [`MAX_PARSE_DEPTH`]; we craft input
/// that comfortably exceeds it.
#[test]
fn deeply_nested_parens_rejected_without_overflow() {
    let depth = max_parse_depth_usize() + 64;
    let mut sql = String::with_capacity(depth * 2 + 16);
    sql.push_str("SELECT ");
    for _ in 0..depth {
        sql.push('(');
    }
    sql.push('1');
    for _ in 0..depth {
        sql.push(')');
    }
    let err = Parser::new(&sql).parse_statement().unwrap_err();
    assert!(
        matches!(err, ParseError::DepthExceeded { .. }),
        "expected DepthExceeded, got {err:?}"
    );
}

/// A query at a depth comfortably below the limit must still
/// succeed; the bound exists to refuse pathological inputs, not
/// reasonable ones.
#[test]
fn parens_below_limit_succeed() {
    let depth = max_parse_depth_usize() / 2;
    let mut sql = String::with_capacity(depth * 2 + 16);
    sql.push_str("SELECT ");
    for _ in 0..depth {
        sql.push('(');
    }
    sql.push('1');
    for _ in 0..depth {
        sql.push(')');
    }
    let stmt = Parser::new(&sql).parse_statement().expect("must parse");
    assert!(matches!(stmt, Statement::Select(_)));
}
