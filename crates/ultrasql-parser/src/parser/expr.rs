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

    pub(super) fn parse_expr_with_precedence(&mut self, min_prec: u8) -> Result<Expr, ParseError> {
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
                let target = self.parse_cast_type_name()?;
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
            // The earlier implementation `return`ed straight out of the
            // Pratt loop, which dropped every binary operator chained
            // after the IN clause (e.g. the trailing `AND foo > bar`
            // inside a WHERE block). We feed the IN/NOT-IN result back
            // through the loop so the standard Pratt walk keeps
            // composing the remaining boolean chain.
            if self.peek()?.kind == TokenKind::KwIn {
                self.advance()?; // IN
                left = self.parse_in_expr(left, false)?;
                continue 'outer;
            }
            if self.peek()?.kind == TokenKind::KwNot
                && self.lookahead_at(1)?.kind == TokenKind::KwIn
            {
                self.advance()?; // NOT
                self.advance()?; // IN
                left = self.parse_in_expr(left, true)?;
                continue 'outer;
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
        let tok_kind = tok.kind;
        let tok_span = tok.span;
        match tok_kind {
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

            // `DATE 'YYYY-MM-DD'`, `TIMESTAMP 'YYYY-MM-DD …'`,
            // `TIME 'HH:MM:SS'`, `INTERVAL '…' [UNIT]` — typed string
            // constants. The opening token is a type-name keyword; the
            // next token must be a string literal. The lookahead check
            // is done inside the arm body (rather than as a match guard)
            // because the borrow checker rejects the second mutable
            // borrow of `self` a guard expression introduces.
            TokenKind::KwDate
            | TokenKind::KwTime
            | TokenKind::KwTimestamp
            | TokenKind::KwInterval => {
                let next_is_string = matches!(
                    self.lookahead_at(1).map(|t| t.kind),
                    Ok(TokenKind::String | TokenKind::EscapedString)
                );
                if !next_is_string {
                    return Err(ParseError::Expected {
                        expected: "expression",
                        found: tok_kind,
                        offset: tok_span.start as usize,
                    });
                }
                let kw_tok = self.advance()?;
                let str_tok = self.advance()?;
                let type_name = match kw_tok.kind {
                    TokenKind::KwDate => "date",
                    TokenKind::KwTime => "time",
                    TokenKind::KwTimestamp => "timestamp",
                    TokenKind::KwInterval => "interval",
                    _ => unreachable!(),
                };
                let raw = str_tok.text(self.source).unwrap_or("");
                let value = if matches!(str_tok.kind, TokenKind::String) {
                    raw[1..raw.len() - 1].replace("''", "'")
                } else {
                    raw.to_owned()
                };
                let mut span_end = str_tok.span.end;
                let unit = if kw_tok.kind == TokenKind::KwInterval {
                    let next_kind = self.peek().map(|t| t.kind).unwrap_or(TokenKind::Eof);
                    if matches!(next_kind, TokenKind::Identifier) {
                        let id_tok = self.advance()?;
                        let id_text = id_tok.text(self.source).unwrap_or("").to_lowercase();
                        span_end = id_tok.span.end;
                        Some(id_text)
                    } else {
                        None
                    }
                } else {
                    None
                };
                Ok(Expr::Literal(Literal::Typed {
                    type_name: type_name.to_owned(),
                    value,
                    unit,
                    span: Span::new(kw_tok.span.start, span_end),
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

            TokenKind::Identifier | TokenKind::QuotedIdentifier => {
                if self.looks_like_vector_family_typed_literal()? {
                    self.parse_vector_family_typed_literal()
                } else {
                    self.parse_ident_or_call()
                }
            }

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

            // Special non-standard call shape: `EXTRACT(unit FROM expr)`.
            // The PostgreSQL / SQL-standard `extract` function uses the
            // keyword `FROM` instead of a comma between the unit and
            // the source expression. We desugar to the canonical
            // `extract(unit_text, expr)` call shape so the binder can
            // dispatch through its usual function-resolution path.
            let is_extract =
                name.parts.len() == 1 && name.parts[0].value.eq_ignore_ascii_case("extract");
            if is_extract && self.peek()?.kind != TokenKind::RParen {
                let unit_tok = self.advance()?;
                // Allow an identifier or any keyword token as the unit;
                // PostgreSQL accepts a quoted string here too. The
                // binder normalises the spelling to lowercase.
                let unit_text = unit_tok
                    .text(self.source)
                    .unwrap_or("")
                    .trim_matches(|c| c == '"' || c == '\'')
                    .to_ascii_lowercase();
                self.expect(TokenKind::KwFrom, "FROM")?;
                let target = self.parse_expr()?;
                let rp = self.expect(TokenKind::RParen, ")")?;
                return Ok(Expr::Call {
                    args: vec![
                        Expr::Literal(crate::ast::Literal::String {
                            value: unit_text,
                            span: unit_tok.span,
                        }),
                        target,
                    ],
                    distinct: false,
                    over: None,
                    span: Span::new(name.span.start, rp.span.end),
                    name,
                });
            }

            // Special non-standard call shape: `SUBSTRING(s FROM n [FOR k])`.
            // The SQL-standard `substring` accepts `FROM` and `FOR`
            // keyword separators instead of commas. We desugar to the
            // canonical `substring(s, n)` or `substring(s, n, k)` call
            // so the binder's function-resolution path stays uniform.
            // The comma form `substring(s, n, k)` is parsed by the
            // normal argument loop below; we only intercept the
            // keyword form.
            let is_substring =
                name.parts.len() == 1 && name.parts[0].value.eq_ignore_ascii_case("substring");
            if is_substring && self.peek()?.kind != TokenKind::RParen {
                // Peek 2 ahead to decide whether keyword form is in
                // use. The keyword form puts `FROM` after the first
                // expression; the comma form puts a comma. We commit
                // to keyword form once we have parsed the first arg
                // and seen `FROM` next.
                let first_arg = self.parse_expr()?;
                if self.peek()?.kind == TokenKind::KwFrom {
                    self.advance()?; // FROM
                    let from_expr = self.parse_expr()?;
                    let mut args = vec![first_arg, from_expr];
                    // Optional `FOR length`. `KwFor` is the standard
                    // FOR keyword (seen in `FOR UPDATE`, etc.).
                    if self.peek()?.kind == TokenKind::KwFor {
                        self.advance()?; // FOR
                        args.push(self.parse_expr()?);
                    }
                    let rp = self.expect(TokenKind::RParen, ")")?;
                    return Ok(Expr::Call {
                        args,
                        distinct: false,
                        over: None,
                        span: Span::new(name.span.start, rp.span.end),
                        name,
                    });
                }
                // Comma form: feed the first arg back into the normal
                // loop by initialising the argument vector.
                let mut args = vec![first_arg];
                while self.peek()?.kind == TokenKind::Comma {
                    self.advance()?;
                    args.push(self.parse_expr()?);
                }
                let rp = self.expect(TokenKind::RParen, ")")?;
                return Ok(Expr::Call {
                    args,
                    distinct: false,
                    over: None,
                    span: Span::new(name.span.start, rp.span.end),
                    name,
                });
            }

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
        let target = self.parse_cast_type_name()?;
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

    fn looks_like_vector_family_typed_literal(&mut self) -> Result<bool, ParseError> {
        let tok = *self.peek()?;
        if tok.kind != TokenKind::Identifier
            || tok
                .text(self.source)
                .and_then(vector_family_type_base)
                .is_none()
        {
            return Ok(false);
        }
        match self.lookahead_at(1)?.kind {
            TokenKind::String | TokenKind::EscapedString => Ok(true),
            TokenKind::LParen => Ok(self.lookahead_at(2)?.kind == TokenKind::Integer
                && self.lookahead_at(3)?.kind == TokenKind::RParen
                && matches!(
                    self.lookahead_at(4)?.kind,
                    TokenKind::String | TokenKind::EscapedString
                )),
            _ => Ok(false),
        }
    }

    fn parse_vector_family_typed_literal(&mut self) -> Result<Expr, ParseError> {
        let type_tok = self.advance()?;
        let base = type_tok
            .text(self.source)
            .and_then(vector_family_type_base)
            .expect("caller checked vector-family typed literal");
        let mut type_name = base.to_owned();
        let mut span_end = type_tok.span.end;
        if self.peek()?.kind == TokenKind::LParen {
            self.advance()?; // (
            let dim_tok = self.expect(TokenKind::Integer, "integer vector dimension")?;
            let dim = dim_tok.text(self.source).unwrap_or("");
            let rp = self.expect(TokenKind::RParen, ")")?;
            type_name = format!("{base}({dim})");
            span_end = rp.span.end;
        }
        let str_tok = self.advance()?;
        if !matches!(str_tok.kind, TokenKind::String | TokenKind::EscapedString) {
            return Err(ParseError::Expected {
                expected: "vector-family literal string",
                found: str_tok.kind,
                offset: str_tok.span.start as usize,
            });
        }
        let raw = str_tok.text(self.source).unwrap_or("");
        let value = if matches!(str_tok.kind, TokenKind::String) {
            raw[1..raw.len() - 1].replace("''", "'")
        } else {
            raw.to_owned()
        };
        span_end = str_tok.span.end.max(span_end);
        Ok(Expr::Literal(Literal::Typed {
            type_name,
            value,
            unit: None,
            span: Span::new(type_tok.span.start, span_end),
        }))
    }

    fn parse_cast_type_name(&mut self) -> Result<Identifier, ParseError> {
        let mut target = self.parse_type_name()?;
        if let Some(base) = vector_family_type_base(&target.value)
            && self.peek()?.kind == TokenKind::LParen
        {
            self.advance()?; // (
            let dim_tok = self.expect(TokenKind::Integer, "integer vector dimension")?;
            let dim = dim_tok.text(self.source).unwrap_or("");
            let rp = self.expect(TokenKind::RParen, ")")?;
            target.value = format!("{base}({dim})");
            target.span = Span::new(target.span.start, rp.span.end);
        }
        Ok(target)
    }
}

fn vector_family_type_base(text: &str) -> Option<&'static str> {
    if text.eq_ignore_ascii_case("vector") {
        Some("vector")
    } else if text.eq_ignore_ascii_case("halfvec") {
        Some("halfvec")
    } else if text.eq_ignore_ascii_case("sparsevec") {
        Some("sparsevec")
    } else if text.eq_ignore_ascii_case("bitvec") {
        Some("bitvec")
    } else {
        None
    }
}
