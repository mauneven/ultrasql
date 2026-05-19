//! Binary-operator parser tests.
//!
//! Covers the regex, bitwise, shift, and JSON operator families, the
//! per-pair precedence cross-product, and `^` right-associativity. These
//! pair with the production code in [`super::super::binary_ops`].

use super::*;
use crate::ast::{BinaryOp, Expr, UnaryOp};

// ── regex operators ─────────────────────────────────────────────────────

#[test]
fn regex_match_operator() {
    let expr = parse_expr("name ~ '^A'");
    assert!(matches!(
        expr,
        Expr::Binary {
            op: BinaryOp::RegexMatch,
            ..
        }
    ));
}

#[test]
fn regex_imatch_operator() {
    let expr = parse_expr("name ~* '^a'");
    assert!(matches!(
        expr,
        Expr::Binary {
            op: BinaryOp::RegexIMatch,
            ..
        }
    ));
}

#[test]
fn regex_not_match_operator() {
    let expr = parse_expr("name !~ '^A'");
    assert!(matches!(
        expr,
        Expr::Binary {
            op: BinaryOp::RegexNotMatch,
            ..
        }
    ));
}

#[test]
fn regex_not_imatch_operator() {
    let expr = parse_expr("name !~* '^a'");
    assert!(matches!(
        expr,
        Expr::Binary {
            op: BinaryOp::RegexNotIMatch,
            ..
        }
    ));
}

// ── bitwise operators ────────────────────────────────────────────────────

#[test]
fn bitwise_and_operator() {
    let expr = parse_expr("x & 0xff");
    assert!(matches!(
        expr,
        Expr::Binary {
            op: BinaryOp::BitAnd,
            ..
        }
    ));
}

#[test]
fn bitwise_or_operator() {
    let expr = parse_expr("x | 0x01");
    assert!(matches!(
        expr,
        Expr::Binary {
            op: BinaryOp::BitOr,
            ..
        }
    ));
}

#[test]
fn bitwise_xor_operator() {
    let expr = parse_expr("x # y");
    assert!(matches!(
        expr,
        Expr::Binary {
            op: BinaryOp::BitXor,
            ..
        }
    ));
}

#[test]
fn shift_left_operator() {
    let expr = parse_expr("x << 2");
    assert!(matches!(
        expr,
        Expr::Binary {
            op: BinaryOp::ShiftLeft,
            ..
        }
    ));
}

#[test]
fn shift_right_operator() {
    let expr = parse_expr("x >> 2");
    assert!(matches!(
        expr,
        Expr::Binary {
            op: BinaryOp::ShiftRight,
            ..
        }
    ));
}

#[test]
fn unary_bitnot_operator() {
    let expr = parse_expr("~x");
    assert!(matches!(
        expr,
        Expr::Unary {
            op: UnaryOp::BitNot,
            ..
        }
    ));
}

#[test]
fn bitwise_precedence_tighter_than_comparison() {
    // `x & mask = 0` should parse as `(x & mask) = 0` not `x & (mask = 0)`.
    let expr = parse_expr("x & 255 = 0");
    assert!(matches!(
        expr,
        Expr::Binary {
            op: BinaryOp::Eq,
            ..
        }
    ));
}

#[test]
fn shift_lower_precedence_than_add() {
    // Level 5 (<<) is *lower* than level 6 (+), so `a + b << 3`
    // parses as `(a + b) << 3` — top-level operator is ShiftLeft.
    let expr = parse_expr("a + b << 3");
    assert!(matches!(
        expr,
        Expr::Binary {
            op: BinaryOp::ShiftLeft,
            ..
        }
    ));
}

// ── JSON operators ───────────────────────────────────────────────────────

#[test]
fn json_get_by_key() {
    let expr = parse_expr("doc -> 'key'");
    assert!(matches!(
        expr,
        Expr::Binary {
            op: BinaryOp::JsonGet,
            ..
        }
    ));
}

#[test]
fn json_get_text() {
    let expr = parse_expr("doc ->> 'key'");
    assert!(matches!(
        expr,
        Expr::Binary {
            op: BinaryOp::JsonGetText,
            ..
        }
    ));
}

#[test]
fn json_get_path() {
    let expr = parse_expr("doc #> '{a,b}'");
    assert!(matches!(
        expr,
        Expr::Binary {
            op: BinaryOp::JsonGetPath,
            ..
        }
    ));
}

#[test]
fn json_get_path_text() {
    let expr = parse_expr("doc #>> '{a,b}'");
    assert!(matches!(
        expr,
        Expr::Binary {
            op: BinaryOp::JsonGetPathText,
            ..
        }
    ));
}

#[test]
fn json_contains() {
    let expr = parse_expr("doc @> '{\"a\":1}'");
    assert!(matches!(
        expr,
        Expr::Binary {
            op: BinaryOp::JsonContains,
            ..
        }
    ));
}

#[test]
fn json_contained_by() {
    let expr = parse_expr("doc <@ '{\"a\":1}'");
    assert!(matches!(
        expr,
        Expr::Binary {
            op: BinaryOp::JsonContained,
            ..
        }
    ));
}

#[test]
fn json_has_key() {
    let expr = parse_expr("doc ? 'key'");
    assert!(matches!(
        expr,
        Expr::Binary {
            op: BinaryOp::JsonHasKey,
            ..
        }
    ));
}

#[test]
fn json_has_any_key() {
    let expr = parse_expr("doc ?| keys");
    assert!(matches!(
        expr,
        Expr::Binary {
            op: BinaryOp::JsonHasAnyKey,
            ..
        }
    ));
}

#[test]
fn json_has_all_keys() {
    let expr = parse_expr("doc ?& keys");
    assert!(matches!(
        expr,
        Expr::Binary {
            op: BinaryOp::JsonHasAllKeys,
            ..
        }
    ));
}

#[test]
fn text_search_match() {
    let expr = parse_expr("doc @@ query");
    assert!(matches!(
        expr,
        Expr::Binary {
            op: BinaryOp::TextSearchMatch,
            ..
        }
    ));
}

/// JSON operators bind tighter than comparison: `doc -> 'k' = 'v'`
/// parses as `(doc -> 'k') = 'v'`.
#[test]
fn json_get_tighter_than_eq() {
    let expr = parse_expr("doc -> 'k' = 'v'");
    assert!(matches!(
        expr,
        Expr::Binary {
            op: BinaryOp::Eq,
            ..
        }
    ));
}

// ── operator precedence property test ────────────────────────────────────

/// A table-driven precedence check: build an expression `a OP1 b OP2 c`
/// and assert the parse tree reflects the correct associativity.
///
/// For each pair `(low_op, high_op)` where `high_op` binds more tightly,
/// `a LOW b HIGH c` must parse as `a LOW (b HIGH c)` — i.e. the top-level
/// operator is `low_op`.
#[test]
fn binary_op_precedence_pairs() {
    let cases: &[(&str, BinaryOp, &str, BinaryOp)] = &[
        // low_expr, low_op, high_expr, high_op
        ("a OR b AND c", BinaryOp::Or, "b AND c", BinaryOp::And),
        ("a AND b = c", BinaryOp::And, "b = c", BinaryOp::Eq),
        ("a = b + c", BinaryOp::Eq, "b + c", BinaryOp::Add),
        ("a + b * c", BinaryOp::Add, "b * c", BinaryOp::Mul),
        ("a * b ^ c", BinaryOp::Mul, "b ^ c", BinaryOp::Pow),
        ("a << b + c", BinaryOp::ShiftLeft, "b + c", BinaryOp::Add),
        ("a = b & c", BinaryOp::Eq, "b & c", BinaryOp::BitAnd),
    ];

    for (src, expected_top, _rhs_src, expected_rhs) in cases {
        let expr = parse_expr(src);
        let Expr::Binary {
            op: top_op, right, ..
        } = expr
        else {
            panic!("expected Binary for {src:?}, got {expr:?}");
        };
        assert_eq!(top_op, *expected_top, "top op mismatch for {src:?}");
        // The right operand should carry the tighter operator.
        let Expr::Binary { op: rhs_op, .. } = *right else {
            panic!("expected Binary rhs for {src:?}");
        };
        assert_eq!(rhs_op, *expected_rhs, "rhs op mismatch for {src:?}");
    }
}

/// Right-associativity of `^`: `a ^ b ^ c` must parse as `a ^ (b ^ c)`.
#[test]
fn pow_is_right_associative() {
    let expr = parse_expr("a ^ b ^ c");
    let Expr::Binary {
        op: BinaryOp::Pow,
        right,
        ..
    } = expr
    else {
        panic!()
    };
    assert!(matches!(
        *right,
        Expr::Binary {
            op: BinaryOp::Pow,
            ..
        }
    ));
}
