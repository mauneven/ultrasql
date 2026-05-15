//! Postfix expression decorators.
//!
//! These methods are invoked from the Pratt loop in [`super::expr`] when
//! the lookahead at "next-token" position is one of the postfix
//! introducers: `BETWEEN`, `IS`, `[`, `AT TIME ZONE`. Each decorator
//! consumes its trailing tokens and returns a wrapper expression that
//! the Pratt loop continues operating on.

use super::{ParseError, Parser};
use crate::ast::Expr;
use crate::span::Span;
use crate::token::TokenKind;

impl<'src> Parser<'src> {
    /// Parse `BETWEEN [SYMMETRIC] low AND high` after `expr` and the (optional
    /// `NOT` and the) `BETWEEN` keyword have been consumed by the caller.
    ///
    /// The AND inside `BETWEEN … AND …` is not a boolean AND: we parse the
    /// lower and upper bounds at precedence 4 (one above comparison) so that
    /// a bare `AND` terminates the bound expression without consuming it.
    pub(super) fn parse_between_body(
        &mut self,
        expr: Expr,
        negated: bool,
    ) -> Result<Expr, ParseError> {
        let start = expr.span().start;
        self.advance()?; // BETWEEN
        let symmetric = self.match_kw(TokenKind::KwSymmetric);
        // Precedence 4 stops before AND (prec 2) but allows arithmetic.
        let low = self.parse_expr_with_precedence(4)?;
        self.expect(TokenKind::KwAnd, "AND")?;
        let high = self.parse_expr_with_precedence(4)?;
        let span = Span::new(start, high.span().end);
        Ok(Expr::Between {
            expr: Box::new(expr),
            low: Box::new(low),
            high: Box::new(high),
            negated,
            symmetric,
            span,
        })
    }

    /// Parse `IS [NOT] NULL / TRUE / FALSE / UNKNOWN / DISTINCT FROM`.
    ///
    /// The `IS` keyword is the current token and has not been consumed.
    pub(super) fn parse_is_postfix(&mut self, expr: Expr) -> Result<Expr, ParseError> {
        let _is_tok = self.advance()?; // IS
        let start = expr.span().start;
        let negated = self.match_kw(TokenKind::KwNot);

        let tok = self.peek()?;
        match tok.kind {
            TokenKind::KwNull => {
                let end = self.advance()?.span.end;
                Ok(Expr::IsNull {
                    expr: Box::new(expr),
                    negated,
                    span: Span::new(start, end),
                })
            }
            TokenKind::KwTrue => {
                let end = self.advance()?.span.end;
                Ok(Expr::IsBoolean {
                    expr: Box::new(expr),
                    value: true,
                    is_unknown: false,
                    negated,
                    span: Span::new(start, end),
                })
            }
            TokenKind::KwFalse => {
                let end = self.advance()?.span.end;
                Ok(Expr::IsBoolean {
                    expr: Box::new(expr),
                    value: false,
                    is_unknown: false,
                    negated,
                    span: Span::new(start, end),
                })
            }
            TokenKind::KwUnknown => {
                let end = self.advance()?.span.end;
                Ok(Expr::IsBoolean {
                    expr: Box::new(expr),
                    // `value` is unused when `is_unknown` is true; use `true`
                    // as a neutral placeholder.
                    value: true,
                    is_unknown: true,
                    negated,
                    span: Span::new(start, end),
                })
            }
            TokenKind::KwDistinct => {
                self.advance()?; // DISTINCT
                self.expect(TokenKind::KwFrom, "FROM")?;
                let right = self.parse_expr_with_precedence(4)?;
                let span = Span::new(start, right.span().end);
                Ok(Expr::IsDistinctFrom {
                    left: Box::new(expr),
                    right: Box::new(right),
                    negated,
                    span,
                })
            }
            other => Err(ParseError::Expected {
                expected: "NULL, TRUE, FALSE, UNKNOWN, or DISTINCT FROM after IS",
                found: other,
                offset: tok.span.start as usize,
            }),
        }
    }

    /// Parse `expr[index]` or `expr[lower:upper]` after identifying `[`.
    pub(super) fn parse_subscript_or_slice(&mut self, expr: Expr) -> Result<Expr, ParseError> {
        let start = expr.span().start;
        self.advance()?; // [

        // `[:upper]` — lower absent, colon present immediately.
        if self.peek()?.kind == TokenKind::Colon {
            self.advance()?; // :
            let upper = if self.peek()?.kind == TokenKind::RBracket {
                None
            } else {
                Some(Box::new(self.parse_expr()?))
            };
            let end = self.expect(TokenKind::RBracket, "]")?.span.end;
            return Ok(Expr::ArraySlice {
                expr: Box::new(expr),
                lower: None,
                upper,
                span: Span::new(start, end),
            });
        }

        // Parse an expression; decide slice vs subscript by the next token.
        let inner = self.parse_expr()?;

        if self.peek()?.kind == TokenKind::Colon {
            // Slice: `[lower:upper]` or `[lower:]`.
            self.advance()?; // :
            let upper = if self.peek()?.kind == TokenKind::RBracket {
                None
            } else {
                Some(Box::new(self.parse_expr()?))
            };
            let end = self.expect(TokenKind::RBracket, "]")?.span.end;
            Ok(Expr::ArraySlice {
                expr: Box::new(expr),
                lower: Some(Box::new(inner)),
                upper,
                span: Span::new(start, end),
            })
        } else {
            // Subscript: `[index]`.
            let end = self.expect(TokenKind::RBracket, "]")?.span.end;
            Ok(Expr::ArraySubscript {
                expr: Box::new(expr),
                index: Box::new(inner),
                span: Span::new(start, end),
            })
        }
    }

    /// Parse `expr AT TIME ZONE zone_expr`.
    ///
    /// The `AT` keyword is the current token (not yet consumed).
    pub(super) fn parse_at_time_zone(&mut self, expr: Expr) -> Result<Expr, ParseError> {
        let start = expr.span().start;
        self.advance()?; // AT
        self.expect(TokenKind::KwTime, "TIME")?;
        self.expect(TokenKind::KwZone, "ZONE")?;
        // Zone is a high-precedence expression (e.g. string literal or ident).
        let zone = self.parse_expr_with_precedence(8)?;
        let span = Span::new(start, zone.span().end);
        Ok(Expr::AtTimeZone {
            expr: Box::new(expr),
            zone: Box::new(zone),
            span,
        })
    }
}
