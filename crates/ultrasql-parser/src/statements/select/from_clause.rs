//! Parser methods for the `FROM` clause: joins, table factors, and the
//! PIVOT / UNPIVOT table transforms.
//!
//! These methods were carved out of the original `select.rs`; they form a
//! cohesive group around resolving a comma- and JOIN-separated list of table
//! factors into a single left-deep `TableRef` tree.

use crate::ast::{
    Expr, Identifier, JoinCondition, JoinOp, Literal, PivotAggregate, PivotValue, TableRef,
    UnpivotColumn,
};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

use super::TableRefSpan;

impl Parser<'_> {
    // ------------------------------------------------------------------ //
    // FROM clause — joins                                                 //
    // ------------------------------------------------------------------ //

    /// Parse the FROM clause, returning a list of `TableRef` nodes.
    ///
    /// The grammar here is:
    /// ```text
    /// from_clause  ::= table_factor ( join_clause | ',' table_factor )*
    /// ```
    /// Comma-separated tables are canonicalised as `JoinOp::Cross` with
    /// `JoinCondition::None`, building a left-deep join tree. Each explicit
    /// JOIN folds the current left side into a new `TableRef::Join`.
    ///
    /// The resulting `Vec<TableRef>` always has exactly one entry that is the
    /// root of a left-deep join tree (or a single `TableRef::Named` /
    /// `TableRef::Subquery` if there are no joins).
    pub(crate) fn parse_from_clause(&mut self) -> Result<Vec<TableRef>, ParseError> {
        let mut lhs = self.parse_table_factor()?;

        loop {
            let kind = self.peek()?.kind;
            match kind {
                // Comma → implicit CROSS JOIN
                TokenKind::Comma => {
                    let comma_tok = self.advance()?;
                    let rhs = self.parse_table_factor()?;
                    let span = Span::new(lhs.ref_span().start, rhs.ref_span().end);
                    lhs = TableRef::Join {
                        left: Box::new(lhs),
                        op: JoinOp::Cross,
                        right: Box::new(rhs),
                        condition: JoinCondition::None,
                        span: Span::new(comma_tok.span.start, span.end),
                    };
                }
                // Explicit join keywords
                TokenKind::KwInner
                | TokenKind::KwLeft
                | TokenKind::KwRight
                | TokenKind::KwFull
                | TokenKind::KwCross
                | TokenKind::KwJoin
                | TokenKind::KwNatural => {
                    lhs = self.parse_join(lhs)?;
                }
                _ => break,
            }
        }

        Ok(vec![lhs])
    }

    /// Parse one explicit join clause, given the already-parsed LHS.
    fn parse_join(&mut self, lhs: TableRef) -> Result<TableRef, ParseError> {
        let start = lhs.ref_span().start;
        let natural = self.match_kw(TokenKind::KwNatural);

        let op = match self.peek()?.kind {
            TokenKind::KwCross => {
                if natural {
                    return Err(ParseError::Unsupported {
                        what: "NATURAL CROSS JOIN",
                        offset: self.peek()?.span.start_usize(),
                    });
                }
                self.advance()?; // CROSS
                self.expect(TokenKind::KwJoin, "JOIN")?;
                JoinOp::Cross
            }
            TokenKind::KwInner => {
                self.advance()?; // INNER
                self.expect(TokenKind::KwJoin, "JOIN")?;
                JoinOp::Inner
            }
            TokenKind::KwLeft => {
                self.advance()?; // LEFT
                self.match_kw(TokenKind::KwOuter); // optional OUTER
                self.expect(TokenKind::KwJoin, "JOIN")?;
                JoinOp::LeftOuter
            }
            TokenKind::KwRight => {
                self.advance()?; // RIGHT
                self.match_kw(TokenKind::KwOuter); // optional OUTER
                self.expect(TokenKind::KwJoin, "JOIN")?;
                JoinOp::RightOuter
            }
            TokenKind::KwFull => {
                self.advance()?; // FULL
                self.match_kw(TokenKind::KwOuter); // optional OUTER
                self.expect(TokenKind::KwJoin, "JOIN")?;
                JoinOp::FullOuter
            }
            TokenKind::KwJoin => {
                self.advance()?; // JOIN (bare — INNER implied)
                JoinOp::Inner
            }
            other => {
                return Err(ParseError::Expected {
                    expected: "JOIN keyword",
                    found: other,
                    offset: self.peek()?.span.start_usize(),
                });
            }
        };

        let rhs = self.parse_table_factor()?;

        let condition = match op {
            JoinOp::Cross => JoinCondition::None,
            _ if natural => JoinCondition::Natural,
            _ => {
                if self.peek()?.kind == TokenKind::KwOn {
                    self.advance()?; // ON
                    JoinCondition::On(self.parse_expr()?)
                } else if self.peek()?.kind == TokenKind::KwUsing {
                    self.advance()?; // USING
                    self.expect(TokenKind::LParen, "(")?;
                    let cols = self.parse_identifier_list()?;
                    self.expect(TokenKind::RParen, ")")?;
                    JoinCondition::Using(cols)
                } else {
                    return Err(ParseError::Expected {
                        expected: "ON or USING",
                        found: self.peek()?.kind,
                        offset: self.peek()?.span.start_usize(),
                    });
                }
            }
        };

        let end = rhs.ref_span().end;
        Ok(TableRef::Join {
            left: Box::new(lhs),
            op,
            right: Box::new(rhs),
            condition,
            span: Span::new(start, end),
        })
    }

    /// Parse a single table factor: a named table, a parenthesised subquery,
    /// or a parenthesised joined table.
    ///
    /// ```text
    /// table_factor ::=
    ///     name [ [ AS ] alias ]
    ///   | '(' SELECT … ')' AS alias [ '(' col_alias, … ')' ]
    ///   | '(' joined_table ')'
    /// ```
    /// Parse a single table factor, bounding statement-level recursion.
    ///
    /// `parse_table_factor` is the recursion hub for the FROM clause: a
    /// derived table recurses into [`Self::parse_select`] and a parenthesised
    /// joined table recurses into [`Self::parse_from_clause`] (which calls
    /// back here). The expression-depth guard does not cover this path, so
    /// nested `FROM (SELECT ...)` or `(((t)))` joins could overflow the stack
    /// and abort the process on untrusted input. Charging each level against
    /// the shared [`MAX_PARSE_DEPTH`](crate::parser::MAX_PARSE_DEPTH) budget
    /// turns that crash into a recoverable `DepthExceeded` error. The
    /// `enter_depth`/`leave_depth` pair is balanced on every exit path.
    pub(crate) fn parse_table_factor(&mut self) -> Result<TableRef, ParseError> {
        self.enter_depth()?;
        let result = self.parse_table_factor_inner();
        self.leave_depth();
        result
    }

    fn parse_table_factor_inner(&mut self) -> Result<TableRef, ParseError> {
        if matches!(
            self.peek()?.kind,
            TokenKind::String | TokenKind::EscapedString | TokenKind::DollarString
        ) {
            let factor = self.parse_file_table_factor()?;
            return self.parse_table_transform_suffix(factor);
        }

        if self.peek()?.kind == TokenKind::LParen {
            let lp = self.advance()?;

            if self.peek()?.kind == TokenKind::KwSelect || self.peek()?.kind == TokenKind::KwWith {
                // Derived table (subquery).
                let select = self.parse_select()?;
                let rp = self.expect(TokenKind::RParen, ")")?;

                // PostgreSQL requires an alias on derived tables.
                self.match_kw(TokenKind::KwAs); // optional AS
                let alias = self.parse_identifier().map_err(|_| ParseError::Expected {
                    expected: "alias for derived table (PostgreSQL requires AS alias)",
                    found: self.peek().map_or(TokenKind::Eof, |t| t.kind),
                    offset: rp.span.end_usize(),
                })?;

                let column_aliases = if self.peek()?.kind == TokenKind::LParen {
                    self.advance()?; // (
                    let aliases = self.parse_identifier_list()?;
                    self.expect(TokenKind::RParen, ")")?;
                    aliases
                } else {
                    Vec::new()
                };

                let end = column_aliases.last().map_or(alias.span.end, |a| a.span.end);
                let result = TableRef::Subquery {
                    select: Box::new(select),
                    alias,
                    column_aliases,
                    span: Span::new(lp.span.start, end),
                };
                return self.parse_table_transform_suffix(result);
            }

            // Parenthesised table reference / joined table.
            let inner = self.parse_from_clause()?;
            self.expect(TokenKind::RParen, ")")?;

            // A parenthesised single table factor with an optional alias.
            let mut result = inner.into_iter().next().ok_or(ParseError::UnexpectedEof {
                expected: "table reference",
            })?;

            // Allow alias after closing paren: `(t1 JOIN t2 ON …) AS x`
            if self.match_kw(TokenKind::KwAs)
                || matches!(
                    self.peek().map(|t| t.kind),
                    Ok(TokenKind::Identifier | TokenKind::QuotedIdentifier)
                )
            {
                if let Ok(alias) = self.parse_identifier() {
                    // For a Named ref, set the alias; for a Join, wrap doesn't
                    // make sense at AST level — leave the span as-is.
                    if let TableRef::Named {
                        alias: ref mut a, ..
                    } = result
                    {
                        *a = Some(alias);
                    }
                }
            }

            return self.parse_table_transform_suffix(result);
        }

        // Named table reference.
        let factor = self.parse_table_ref()?;
        self.parse_table_transform_suffix(factor)
    }

    fn parse_table_transform_suffix(
        &mut self,
        mut input: TableRef,
    ) -> Result<TableRef, ParseError> {
        loop {
            match self.peek()?.kind {
                TokenKind::KwPivot => {
                    input = self.parse_pivot_suffix(input)?;
                }
                TokenKind::KwUnpivot => {
                    input = self.parse_unpivot_suffix(input)?;
                }
                _ => return Ok(input),
            }
        }
    }

    fn parse_pivot_suffix(&mut self, input: TableRef) -> Result<TableRef, ParseError> {
        let start = input.ref_span().start;
        self.expect(TokenKind::KwPivot, "PIVOT")?;
        self.expect(TokenKind::LParen, "(")?;
        let aggregate = self.parse_pivot_aggregate()?;
        self.expect(TokenKind::KwFor, "FOR")?;
        let value_column = self.parse_identifier()?;
        self.expect(TokenKind::KwIn, "IN")?;
        self.expect(TokenKind::LParen, "(")?;
        let mut pivot_values = Vec::new();
        loop {
            pivot_values.push(self.parse_pivot_value()?);
            if self.peek()?.kind == TokenKind::Comma {
                self.advance()?;
            } else {
                break;
            }
        }
        self.expect(TokenKind::RParen, ")")?;
        let rp = self.expect(TokenKind::RParen, ")")?;
        Ok(TableRef::Pivot {
            input: Box::new(input),
            aggregate,
            value_column,
            pivot_values,
            span: Span::new(start, rp.span.end),
        })
    }

    fn parse_pivot_aggregate(&mut self) -> Result<PivotAggregate, ParseError> {
        let function = self.parse_identifier()?;
        let start = function.span.start;
        self.expect(TokenKind::LParen, "(")?;
        let arg = if self.peek()?.kind == TokenKind::Star {
            self.advance()?;
            None
        } else {
            Some(self.parse_expr()?)
        };
        let rp = self.expect(TokenKind::RParen, ")")?;
        Ok(PivotAggregate {
            function,
            arg,
            span: Span::new(start, rp.span.end),
        })
    }

    fn parse_pivot_value(&mut self) -> Result<PivotValue, ParseError> {
        let value = self.parse_expr()?;
        let start = value.span().start;
        let alias = if self.match_kw(TokenKind::KwAs) {
            Some(self.parse_alias_identifier()?)
        } else {
            None
        };
        let end = alias.as_ref().map_or(value.span().end, |a| a.span.end);
        Ok(PivotValue {
            value,
            alias,
            span: Span::new(start, end),
        })
    }

    fn parse_unpivot_suffix(&mut self, input: TableRef) -> Result<TableRef, ParseError> {
        let start = input.ref_span().start;
        self.expect(TokenKind::KwUnpivot, "UNPIVOT")?;
        let include_nulls = if self.match_kw(TokenKind::KwInclude) {
            self.expect(TokenKind::KwNulls, "NULLS")?;
            true
        } else if self.match_kw(TokenKind::KwExclude) {
            self.expect(TokenKind::KwNulls, "NULLS")?;
            false
        } else {
            false
        };
        self.expect(TokenKind::LParen, "(")?;
        let value_column = self.parse_identifier()?;
        self.expect(TokenKind::KwFor, "FOR")?;
        let name_column = self.parse_identifier()?;
        self.expect(TokenKind::KwIn, "IN")?;
        self.expect(TokenKind::LParen, "(")?;
        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_unpivot_column()?);
            if self.peek()?.kind == TokenKind::Comma {
                self.advance()?;
            } else {
                break;
            }
        }
        self.expect(TokenKind::RParen, ")")?;
        let rp = self.expect(TokenKind::RParen, ")")?;
        Ok(TableRef::Unpivot {
            input: Box::new(input),
            value_column,
            name_column,
            columns,
            include_nulls,
            span: Span::new(start, rp.span.end),
        })
    }

    fn parse_unpivot_column(&mut self) -> Result<UnpivotColumn, ParseError> {
        let column = self.parse_identifier()?;
        let start = column.span.start;
        let label = if self.match_kw(TokenKind::KwAs) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let end = label
            .as_ref()
            .map_or(column.span.end, |expr| expr.span().end);
        Ok(UnpivotColumn {
            column,
            label,
            span: Span::new(start, end),
        })
    }

    fn parse_file_table_factor(&mut self) -> Result<TableRef, ParseError> {
        let arg = self.parse_expr()?;
        let Expr::Literal(Literal::String { value, span }) = &arg else {
            return Err(ParseError::Expected {
                expected: "file path string literal",
                found: self.peek()?.kind,
                offset: arg.span().start_usize(),
            });
        };
        let function = file_table_function_name(value).ok_or(ParseError::Unsupported {
            what: "file table literal without supported external file extension",
            offset: span.start_usize(),
        })?;
        let alias = if self.match_kw(TokenKind::KwAs) {
            Some(self.parse_alias_identifier()?)
        } else if matches!(
            self.peek()?.kind,
            TokenKind::Identifier | TokenKind::QuotedIdentifier
        ) && !self.next_token_is_reserved_clause()
        {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        let end = alias.as_ref().map_or(span.end, |a| a.span.end);
        Ok(TableRef::Function {
            span: Span::new(span.start, end),
            name: Identifier {
                value: function.to_owned(),
                quoted: false,
                span: *span,
            },
            args: vec![arg],
            alias,
        })
    }
}

fn file_table_function_name(path: &str) -> Option<&'static str> {
    let lower = path.to_ascii_lowercase();
    if lower.contains(".parquet") {
        Some("read_parquet")
    } else if lower.contains(".ndjson") {
        Some("read_ndjson")
    } else if lower.contains(".json") {
        Some("read_json")
    } else if lower.contains(".arrow") || lower.contains(".ipc") || lower.contains(".feather") {
        Some("read_arrow")
    } else if lower.contains(".csv") {
        Some("read_csv")
    } else {
        None
    }
}
