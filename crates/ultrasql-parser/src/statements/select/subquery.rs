//! Subquery-expression parsing that conceptually belongs to `parser.rs`'s
//! `parse_prefix`, but lives here alongside the `SELECT` machinery it drives:
//! scalar subqueries, `EXISTS`, `IN` / `NOT IN`, and `ANY` / `ALL`.

use crate::ast::Expr;
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Called from `parse_prefix` after `(` has been consumed: if the
    /// next token is `SELECT` or `WITH`, parse a scalar subquery and
    /// return `Ok(Some(Expr::Subquery {…}))`. Otherwise return
    /// `Ok(None)` — the caller is responsible for parsing the inner
    /// expression and the closing `)`.
    ///
    /// This design avoids calling `lookahead_at` (which allocates a
    /// temporary `Lexer` on the stack) at every recursion level. Only
    /// a single `peek()` is needed once the `(` is already consumed.
    pub(crate) fn try_parse_subquery_after_lparen(
        &mut self,
        lp_span: crate::span::Span,
    ) -> Result<Option<Expr>, ParseError> {
        if !matches!(self.peek()?.kind, TokenKind::KwSelect | TokenKind::KwWith) {
            return Ok(None);
        }
        let select = self.parse_select()?;
        let rp = self.expect(TokenKind::RParen, ")")?;
        Ok(Some(Expr::Subquery {
            select: Box::new(select),
            span: Span::new(lp_span.start, rp.span.end),
        }))
    }

    /// Parse `EXISTS ( SELECT … )` / `NOT EXISTS ( SELECT … )` when the
    /// `EXISTS` keyword has already been identified as the current token.
    pub(crate) fn parse_exists_expr(&mut self, negated: bool) -> Result<Expr, ParseError> {
        let kw = self.advance()?; // EXISTS
        self.expect(TokenKind::LParen, "(")?;
        let select = self.parse_select()?;
        let rp = self.expect(TokenKind::RParen, ")")?;
        Ok(Expr::Exists {
            select: Box::new(select),
            negated,
            span: Span::new(kw.span.start, rp.span.end),
        })
    }

    /// Parse `expr [NOT] IN (…)` after the `expr` and the (optional `NOT`)
    /// `IN` keywords have been consumed.
    ///
    /// Returns either `Expr::InSubquery` or `Expr::InList`.
    pub(crate) fn parse_in_expr(&mut self, expr: Expr, negated: bool) -> Result<Expr, ParseError> {
        let start = expr.span().start;
        self.expect(TokenKind::LParen, "(")?;

        // Peek whether the contents are a SELECT.
        if matches!(self.peek()?.kind, TokenKind::KwSelect | TokenKind::KwWith) {
            let select = self.parse_select()?;
            let rp = self.expect(TokenKind::RParen, ")")?;
            return Ok(Expr::InSubquery {
                expr: Box::new(expr),
                select: Box::new(select),
                negated,
                span: Span::new(start, rp.span.end),
            });
        }

        // Otherwise parse a comma-separated literal/expression list.
        let mut items = Vec::new();
        loop {
            items.push(self.parse_expr()?);
            if self.peek()?.kind != TokenKind::Comma {
                break;
            }
            self.advance()?;
        }
        let rp = self.expect(TokenKind::RParen, ")")?;
        Ok(Expr::InList {
            expr: Box::new(expr),
            items,
            negated,
            span: Span::new(start, rp.span.end),
        })
    }

    /// Parse `expr <op> ANY/ALL (SELECT …)` given that `lhs` and `op` have
    /// been parsed and the next token is `ANY` or `ALL`.
    pub(crate) fn parse_any_all_expr(
        &mut self,
        lhs: Expr,
        op: crate::ast::BinaryOp,
    ) -> Result<Option<Expr>, ParseError> {
        if !op.is_comparison() {
            return Ok(None);
        }

        let kind = self.peek()?.kind;
        let is_any = kind == TokenKind::KwAny;
        let is_all = kind == TokenKind::KwAll;
        if !is_any && !is_all {
            return Ok(None);
        }

        // Peek ahead: next must be `(`
        let after_any_all = self.lookahead_at(1)?;
        if after_any_all.kind != TokenKind::LParen {
            return Ok(None);
        }
        // And after `(` may be `SELECT` / `WITH` for subquery ANY, or
        // `ARRAY[...]` for ORM catalog probes such as `relkind = ANY (...)`.
        let after_lparen = self.lookahead_at(2)?;
        if !matches!(after_lparen.kind, TokenKind::KwSelect | TokenKind::KwWith) {
            if !is_any || op != crate::ast::BinaryOp::Eq {
                return Ok(None);
            }
            let kw_tok = self.advance()?; // ANY
            self.expect(TokenKind::LParen, "(")?;
            let array_expr = self.parse_expr()?;
            let rp = self.expect(TokenKind::RParen, ")")?;
            let Expr::ArrayLiteral { elements, .. } = array_expr else {
                return Ok(Some(Expr::AnyArray {
                    expr: Box::new(lhs),
                    op,
                    array: Box::new(array_expr),
                    span: Span::new(kw_tok.span.start, rp.span.end),
                }));
            };
            return Ok(Some(Expr::InList {
                expr: Box::new(lhs),
                items: elements,
                negated: false,
                span: Span::new(kw_tok.span.start, rp.span.end),
            }));
        }

        let kw_tok = self.advance()?; // ANY / ALL
        self.expect(TokenKind::LParen, "(")?;
        let select = self.parse_select()?;
        let rp = self.expect(TokenKind::RParen, ")")?;

        let span = Span::new(lhs.span().start, rp.span.end);
        if is_any {
            Ok(Some(Expr::Any {
                expr: Box::new(lhs),
                op,
                select: Box::new(select),
                span: Span::new(kw_tok.span.start, span.end),
            }))
        } else {
            Ok(Some(Expr::All {
                expr: Box::new(lhs),
                op,
                select: Box::new(select),
                span: Span::new(kw_tok.span.start, span.end),
            }))
        }
    }
}
