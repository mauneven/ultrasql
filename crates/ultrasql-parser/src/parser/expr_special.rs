//! Syntactic-shape expressions whose first token is a keyword.
//!
//! `CASE`, `COALESCE`, `NULLIF`, `GREATEST`/`LEAST`, and `ROW` each have
//! a fixed shape and are detected by [`super::expr::Parser::parse_prefix`]
//! and dispatched to the corresponding method below.

use super::{ParseError, Parser};
use crate::ast::Expr;
use crate::span::Span;
use crate::token::TokenKind;

impl<'src> Parser<'src> {
    /// Parse `CASE [operand] WHEN … THEN … [ELSE …] END`.
    pub(super) fn parse_case_expr(&mut self) -> Result<Expr, ParseError> {
        let case_tok = self.advance()?; // CASE
        let start = case_tok.span.start;

        // Optional operand (simple CASE): absent when the next token is WHEN,
        // ELSE, or END (i.e. this is a searched CASE).
        let operand = if matches!(
            self.peek()?.kind,
            TokenKind::KwWhen | TokenKind::KwElse | TokenKind::KwEnd
        ) {
            None
        } else {
            Some(Box::new(self.parse_expr()?))
        };

        // One or more WHEN … THEN … branches.
        let mut branches = Vec::new();
        while self.peek()?.kind == TokenKind::KwWhen {
            self.advance()?; // WHEN
            let when_expr = self.parse_expr()?;
            self.expect(TokenKind::KwThen, "THEN")?;
            let then_expr = self.parse_expr()?;
            branches.push((when_expr, then_expr));
        }
        if branches.is_empty() {
            return Err(ParseError::Expected {
                expected: "WHEN clause in CASE expression",
                found: self.peek()?.kind,
                offset: self.peek()?.span.start as usize,
            });
        }

        // Optional ELSE.
        let else_expr = if self.match_kw(TokenKind::KwElse) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        let end_tok = self.expect(TokenKind::KwEnd, "END")?;
        Ok(Expr::Case {
            operand,
            branches,
            else_expr,
            span: Span::new(start, end_tok.span.end),
        })
    }

    /// Parse `COALESCE(a, b, …)`.
    pub(super) fn parse_coalesce_expr(&mut self) -> Result<Expr, ParseError> {
        let kw = self.advance()?; // COALESCE
        self.expect(TokenKind::LParen, "(")?;
        let args = self.parse_expr_list_inner()?;
        let rp = self.expect(TokenKind::RParen, ")")?;
        Ok(Expr::Coalesce {
            args,
            span: Span::new(kw.span.start, rp.span.end),
        })
    }

    /// Parse `NULLIF(a, b)`.
    pub(super) fn parse_nullif_expr(&mut self) -> Result<Expr, ParseError> {
        let kw = self.advance()?; // NULLIF
        self.expect(TokenKind::LParen, "(")?;
        let a = self.parse_expr()?;
        self.expect(TokenKind::Comma, ",")?;
        let b = self.parse_expr()?;
        let rp = self.expect(TokenKind::RParen, ")")?;
        Ok(Expr::NullIf {
            a: Box::new(a),
            b: Box::new(b),
            span: Span::new(kw.span.start, rp.span.end),
        })
    }

    /// Parse `GREATEST(…)` or `LEAST(…)`.
    pub(super) fn parse_greatest_least_expr(&mut self, is_greatest: bool) -> Result<Expr, ParseError> {
        let kw = self.advance()?; // GREATEST / LEAST
        self.expect(TokenKind::LParen, "(")?;
        let args = self.parse_expr_list_inner()?;
        let rp = self.expect(TokenKind::RParen, ")")?;
        let span = Span::new(kw.span.start, rp.span.end);
        if is_greatest {
            Ok(Expr::Greatest { args, span })
        } else {
            Ok(Expr::Least { args, span })
        }
    }

    /// Parse `ROW(a, b, …)`.
    pub(super) fn parse_row_expr(&mut self) -> Result<Expr, ParseError> {
        let kw = self.advance()?; // ROW
        self.expect(TokenKind::LParen, "(")?;
        let fields = if self.peek()?.kind == TokenKind::RParen {
            Vec::new()
        } else {
            self.parse_expr_list_inner()?
        };
        let rp = self.expect(TokenKind::RParen, ")")?;
        Ok(Expr::Row {
            fields,
            span: Span::new(kw.span.start, rp.span.end),
        })
    }
}
