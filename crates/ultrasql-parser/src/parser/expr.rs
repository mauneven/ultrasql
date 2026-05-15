//! Pratt-style expression parser.
//!
//! [`Parser::parse_expr_with_precedence`] is the entry point used by
//! every consumer (statement parsers and postfix decorators alike).
//! [`Parser::parse_prefix`] handles the unary / literal / parenthesised
//! head of an expression, and the Pratt loop in
//! [`Parser::parse_expr_with_precedence_inner`] threads through the
//! postfix decorators in [`super::expr_postfix`] and the binary
//! operators in [`super::binary_ops`].

use super::{ParseError, Parser, is_type_name_keyword};
use crate::ast::{Expr, Identifier, Literal, UnaryOp};
use crate::span::Span;
use crate::token::TokenKind;

impl<'src> Parser<'src> {
    pub(crate) fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_expr_with_precedence(0)
    }

    pub(super) fn parse_expr_with_precedence(
        &mut self,
        min_prec: u8,
    ) -> Result<Expr, ParseError> {
        self.enter_depth()?;
        let result = self.parse_expr_with_precedence_inner(min_prec);
        self.leave_depth();
        result
    }

    #[allow(clippy::too_many_lines)]
    fn parse_expr_with_precedence_inner(&mut self, min_prec: u8) -> Result<Expr, ParseError> {
        let mut left = self.parse_prefix()?;

        // Pratt binary-operator loop with integrated postfix constructs.
        'outer: loop {
            // ----------------------------------------------------------------
            // Postfix operators evaluated before the next binary-op check.
            // Ordering: :: (cast) > [] (subscript) > AT TIME ZONE > BETWEEN >
            //           IS > NOT BETWEEN/IN > IN.
            // ----------------------------------------------------------------

            // postfix `::type` — may chain (e.g. `x::int::text`).
            while self.peek()?.kind == TokenKind::ColonColon {
                self.advance()?; // ::
                let target = self.parse_type_name()?;
                let span = Span::new(left.span().start, target.span.end);
                left = Expr::PostfixCast {
                    expr: Box::new(left),
                    target,
                    span,
                };
            }

            // postfix `[index]` or `[lower:upper]` — may chain.
            while self.peek()?.kind == TokenKind::LBracket {
                left = self.parse_subscript_or_slice(left)?;
            }

            // `expr AT TIME ZONE zone`
            if self.peek()?.kind == TokenKind::KwAt
                && self.lookahead_two_is(TokenKind::KwTime, TokenKind::KwZone)
            {
                left = self.parse_at_time_zone(left)?;
                continue 'outer;
            }

            // `expr [NOT] BETWEEN [SYMMETRIC] low AND high`
            if self.peek()?.kind == TokenKind::KwBetween {
                left = self.parse_between_body(left, false)?;
                continue 'outer;
            }
            if self.peek()?.kind == TokenKind::KwNot {
                // Peek: NOT BETWEEN
                if self.lookahead_at(1)?.kind == TokenKind::KwBetween {
                    self.advance()?; // NOT
                    left = self.parse_between_body(left, true)?;
                    continue 'outer;
                }
            }

            // `expr IS [NOT] NULL / TRUE / FALSE / UNKNOWN / DISTINCT FROM`
            if self.peek()?.kind == TokenKind::KwIs {
                left = self.parse_is_postfix(left)?;
                continue 'outer;
            }

            // `expr [NOT] IN (…)` — consumed here rather than the binary loop.
            if self.peek()?.kind == TokenKind::KwIn {
                self.advance()?; // IN
                return self.parse_in_expr(left, false);
            }
            if self.peek()?.kind == TokenKind::KwNot
                && self.lookahead_at(1)?.kind == TokenKind::KwIn
            {
                self.advance()?; // NOT
                self.advance()?; // IN
                return self.parse_in_expr(left, true);
            }

            // ----------------------------------------------------------------
            // Standard Pratt binary-op check.
            // ----------------------------------------------------------------
            let Some((op, op_span)) = self.peek_binary_op()? else {
                break 'outer;
            };
            let prec = op.precedence();
            if prec < min_prec {
                break 'outer;
            }
            self.consume_binary_op(op)?;

            let next_min = if op.is_right_associative() {
                prec
            } else {
                prec + 1
            };

            // Special post-infix: `<op> ANY/ALL (SELECT …)`.
            if let Some(any_all) = self.parse_any_all_expr(left.clone(), op)? {
                left = any_all;
                continue 'outer;
            }

            let right = self.parse_expr_with_precedence(next_min)?;
            let span = Span::new(left.span().start, right.span().end);
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
                span: Span::new(span.start, span.end.max(op_span.end)),
            };
        }

        Ok(left)
    }

    #[allow(clippy::too_many_lines)]
    fn parse_prefix(&mut self) -> Result<Expr, ParseError> {
        let tok = self.peek()?;
        match tok.kind {
            TokenKind::Plus | TokenKind::Minus | TokenKind::KwNot | TokenKind::Tilde => {
                let op_tok = self.advance()?;
                let op = match op_tok.kind {
                    TokenKind::Plus => UnaryOp::Pos,
                    TokenKind::Minus => UnaryOp::Neg,
                    TokenKind::KwNot => UnaryOp::Not,
                    TokenKind::Tilde => UnaryOp::BitNot,
                    _ => unreachable!(),
                };
                // Special case: NOT EXISTS
                if op == UnaryOp::Not && self.peek()?.kind == TokenKind::KwExists {
                    return self.parse_exists_expr(true);
                }
                // Unary operators bind tighter than any binary operator.
                let rhs = self.parse_expr_with_precedence(9)?;
                let span = Span::new(op_tok.span.start, rhs.span().end);
                Ok(Expr::Unary {
                    op,
                    expr: Box::new(rhs),
                    span,
                })
            }

            TokenKind::Integer => {
                let t = self.advance()?;
                Ok(Expr::Literal(Literal::Integer {
                    text: t.text(self.source).unwrap_or("").to_owned(),
                    span: t.span,
                }))
            }
            TokenKind::Float => {
                let t = self.advance()?;
                Ok(Expr::Literal(Literal::Float {
                    text: t.text(self.source).unwrap_or("").to_owned(),
                    span: t.span,
                }))
            }
            TokenKind::String | TokenKind::EscapedString | TokenKind::DollarString => {
                let t = self.advance()?;
                let raw = t.text(self.source).unwrap_or("");
                // Strip the surrounding quotes for the standard form;
                // escape and dollar-quoted strings are left as-is for
                // the binder to interpret.
                let value = if matches!(t.kind, TokenKind::String) {
                    raw[1..raw.len() - 1].replace("''", "'")
                } else {
                    raw.to_owned()
                };
                Ok(Expr::Literal(Literal::String {
                    value,
                    span: t.span,
                }))
            }
            TokenKind::KwNull => {
                let t = self.advance()?;
                Ok(Expr::Literal(Literal::Null { span: t.span }))
            }
            TokenKind::KwTrue => {
                let t = self.advance()?;
                Ok(Expr::Literal(Literal::Bool {
                    value: true,
                    span: t.span,
                }))
            }
            TokenKind::KwFalse => {
                let t = self.advance()?;
                Ok(Expr::Literal(Literal::Bool {
                    value: false,
                    span: t.span,
                }))
            }

            TokenKind::Parameter => {
                let t = self.advance()?;
                let raw = t.text(self.source).unwrap_or("");
                let n: u32 = raw[1..]
                    .parse()
                    .map_err(|_| ParseError::ParameterOutOfRange {
                        text: raw.to_owned(),
                        offset: t.span.start as usize,
                    })?;
                Ok(Expr::Parameter {
                    index: n,
                    span: t.span,
                })
            }

            TokenKind::LParen => {
                // Consume `(` first, then peek to determine if it's a
                // subquery or a ROW/OVERLAPS paren list.
                let lp = self.advance()?;
                if let Some(subq) = self.try_parse_subquery_after_lparen(lp.span)? {
                    return Ok(subq);
                }
                // Parse a parenthesised expression list: could be a single
                // paren-expr, a ROW, or the LHS of OVERLAPS.
                let first = self.parse_expr()?;
                if self.peek()?.kind == TokenKind::Comma {
                    // Multiple expressions inside parens — candidate ROW or OVERLAPS LHS.
                    let mut fields = vec![first];
                    while self.peek()?.kind == TokenKind::Comma {
                        self.advance()?;
                        fields.push(self.parse_expr()?);
                    }
                    let rp = self.expect(TokenKind::RParen, ")")?;
                    let paren_span = Span::new(lp.span.start, rp.span.end);

                    // If `OVERLAPS` follows, this is the LHS period.
                    if self.peek()?.kind == TokenKind::KwOverlaps {
                        if fields.len() != 2 {
                            return Err(ParseError::Expected {
                                expected: "exactly two expressions before OVERLAPS",
                                found: TokenKind::KwOverlaps,
                                offset: self.peek()?.span.start as usize,
                            });
                        }
                        self.advance()?; // OVERLAPS
                        self.expect(TokenKind::LParen, "(")?;
                        let rs = self.parse_expr()?;
                        self.expect(TokenKind::Comma, ",")?;
                        let re = self.parse_expr()?;
                        let rp2 = self.expect(TokenKind::RParen, ")")?;
                        let mut iter = fields.into_iter();
                        let ls = iter.next().expect("len checked above");
                        let le = iter.next().expect("len checked above");
                        return Ok(Expr::Overlaps {
                            left_start: Box::new(ls),
                            left_end: Box::new(le),
                            right_start: Box::new(rs),
                            right_end: Box::new(re),
                            span: Span::new(lp.span.start, rp2.span.end),
                        });
                    }

                    // Otherwise emit a Row expression.
                    return Ok(Expr::Row {
                        fields,
                        span: paren_span,
                    });
                }

                let rp = self.expect(TokenKind::RParen, ")")?;
                Ok(Expr::Paren {
                    expr: Box::new(first),
                    span: Span::new(lp.span.start, rp.span.end),
                })
            }

            TokenKind::KwExists => self.parse_exists_expr(false),

            TokenKind::KwCast => self.parse_cast_expr(),

            TokenKind::KwCase => self.parse_case_expr(),

            TokenKind::KwCoalesce => self.parse_coalesce_expr(),

            TokenKind::KwNullif => self.parse_nullif_expr(),

            TokenKind::KwGreatest => self.parse_greatest_least_expr(true),

            TokenKind::KwLeast => self.parse_greatest_least_expr(false),

            TokenKind::KwRow => self.parse_row_expr(),

            TokenKind::Identifier | TokenKind::QuotedIdentifier => self.parse_ident_or_call(),

            other => Err(ParseError::Expected {
                expected: "expression",
                found: other,
                offset: tok.span.start as usize,
            }),
        }
    }

    /// Parse a comma-separated expression list for function argument lists.
    ///
    /// Unlike [`crate::statements::select::Parser::parse_expr_list`], this
    /// helper is used exclusively inside parentheses and always returns at
    /// least one expression.
    pub(super) fn parse_expr_list_inner(&mut self) -> Result<Vec<Expr>, ParseError> {
        let mut args = vec![self.parse_expr()?];
        while self.peek()?.kind == TokenKind::Comma {
            self.advance()?;
            args.push(self.parse_expr()?);
        }
        Ok(args)
    }

    fn parse_ident_or_call(&mut self) -> Result<Expr, ParseError> {
        let name = self.parse_object_name()?;
        if self.peek()?.kind == TokenKind::LParen {
            self.advance()?;
            // Optional DISTINCT.
            let distinct = self.match_kw(TokenKind::KwDistinct);
            let mut args = Vec::new();
            if self.peek()?.kind != TokenKind::RParen {
                loop {
                    // Special case: `COUNT(*)` and similar aggregate
                    // wildcard forms. Represent `*` as a `Column` with
                    // a single-part name `*` — the binder turns this
                    // into an aggregate wildcard.
                    if self.peek()?.kind == TokenKind::Star {
                        let star = self.advance()?;
                        args.push(Expr::Column {
                            name: crate::ast::ObjectName {
                                parts: vec![crate::ast::Identifier {
                                    value: "*".to_owned(),
                                    quoted: false,
                                    span: star.span,
                                }],
                                span: star.span,
                            },
                        });
                    } else {
                        args.push(self.parse_expr()?);
                    }
                    if self.peek()?.kind != TokenKind::Comma {
                        break;
                    }
                    self.advance()?;
                }
            }
            let rp = self.expect(TokenKind::RParen, ")")?;
            // Optional `OVER (...)` turning this into a window function call.
            let over = if self.peek()?.kind == TokenKind::KwOver {
                Some(self.parse_over_clause()?)
            } else {
                None
            };
            let end = over.as_ref().map_or(rp.span.end, |s| s.span.end);
            Ok(Expr::Call {
                args,
                distinct,
                over,
                span: Span::new(name.span.start, end),
                name,
            })
        } else {
            Ok(Expr::Column { name })
        }
    }

    /// Parse `OVER ( [PARTITION BY expr (, expr)*] [ORDER BY item (, item)*] )`.
    ///
    /// Called immediately after the closing `)` of a function call when
    /// the next token is `OVER`. Frame clauses (`ROWS`/`RANGE`) are
    /// recognised at the executor but the parser does not yet emit them
    /// — the default frame is the v0.6 follow-up.
    fn parse_over_clause(&mut self) -> Result<crate::ast::WindowSpec, ParseError> {
        let over_tok = self.expect(TokenKind::KwOver, "OVER")?;
        self.expect(TokenKind::LParen, "(")?;
        let mut partition_by: Vec<Expr> = Vec::new();
        let mut order_by: Vec<crate::ast::OrderItem> = Vec::new();
        if self.peek()?.kind == TokenKind::KwPartition {
            self.advance()?; // PARTITION
            self.expect(TokenKind::KwBy, "BY")?;
            loop {
                partition_by.push(self.parse_expr()?);
                if self.peek()?.kind != TokenKind::Comma {
                    break;
                }
                self.advance()?;
            }
        }
        if self.peek()?.kind == TokenKind::KwOrder {
            self.advance()?; // ORDER
            self.expect(TokenKind::KwBy, "BY")?;
            loop {
                let expr = self.parse_expr()?;
                let start = expr.span().start;
                let direction = if self.match_kw(TokenKind::KwAsc) {
                    crate::ast::SortDirection::Asc
                } else if self.match_kw(TokenKind::KwDesc) {
                    crate::ast::SortDirection::Desc
                } else {
                    crate::ast::SortDirection::Asc
                };
                let nulls = if self.match_kw(TokenKind::KwNulls) {
                    let n = self.advance()?;
                    if n.text(self.source)
                        .is_some_and(|t| t.eq_ignore_ascii_case("first"))
                    {
                        crate::ast::NullsOrder::First
                    } else if n
                        .text(self.source)
                        .is_some_and(|t| t.eq_ignore_ascii_case("last"))
                    {
                        crate::ast::NullsOrder::Last
                    } else {
                        return Err(ParseError::Expected {
                            expected: "FIRST or LAST after NULLS",
                            found: n.kind,
                            offset: n.span.start as usize,
                        });
                    }
                } else {
                    crate::ast::NullsOrder::Default
                };
                let end = self
                    .peeked
                    .as_ref()
                    .map_or(start, |t| t.span.start)
                    .max(start);
                order_by.push(crate::ast::OrderItem {
                    expr,
                    direction,
                    nulls,
                    span: crate::span::Span::new(start, end),
                });
                if self.peek()?.kind != TokenKind::Comma {
                    break;
                }
                self.advance()?;
            }
        }
        let rp = self.expect(TokenKind::RParen, ")")?;
        Ok(crate::ast::WindowSpec {
            partition_by,
            order_by,
            span: crate::span::Span::new(over_tok.span.start, rp.span.end),
        })
    }

    pub(crate) fn parse_cast_expr(&mut self) -> Result<Expr, ParseError> {
        let cast = self.advance()?; // CAST
        self.expect(TokenKind::LParen, "(")?;
        let expr = self.parse_expr()?;
        self.expect(TokenKind::KwAs, "AS")?;
        let target = self.parse_type_name()?;
        let rp = self.expect(TokenKind::RParen, ")")?;
        Ok(Expr::Cast {
            expr: Box::new(expr),
            target,
            span: Span::new(cast.span.start, rp.span.end),
        })
    }

    /// Parse a type name. Type names may be either ordinary
    /// identifiers (`my_domain`) or the SQL reserved type-name
    /// keywords (`integer`, `varchar`, `timestamp`, ...). This helper
    /// accepts both.
    pub(crate) fn parse_type_name(&mut self) -> Result<Identifier, ParseError> {
        let tok = self.peek()?;
        match tok.kind {
            TokenKind::Identifier | TokenKind::QuotedIdentifier => self.parse_identifier(),
            kind if is_type_name_keyword(kind) => {
                let tok = self.advance()?;
                let raw = tok.text(self.source).unwrap_or("");
                Ok(Identifier {
                    value: raw.to_ascii_lowercase(),
                    quoted: false,
                    span: tok.span,
                })
            }
            other => Err(ParseError::Expected {
                expected: "type name",
                found: other,
                offset: tok.span.start as usize,
            }),
        }
    }
}
