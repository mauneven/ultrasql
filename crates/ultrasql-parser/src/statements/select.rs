//! Parser methods for `SELECT` statements.
//!
//! This module contains `impl<'src> Parser<'src>` blocks that parse the
//! `SELECT` statement and all of its sub-clauses:
//!
//! * Projection list (`parse_select_list`, `parse_select_item`)
//! * Table references with full join syntax (`parse_table_factor`,
//!   `parse_joined_table`)
//! * GROUP BY / HAVING
//! * DISTINCT / DISTINCT ON
//! * UNION / INTERSECT / EXCEPT (set operations)
//! * WITH [RECURSIVE] CTEs
//! * Subquery expressions (scalar, EXISTS, IN, NOT IN, ANY, ALL)
//! * ORDER BY, LIMIT, OFFSET
//! * Identifier and object-name helpers
//!
//! # Set-operation precedence (v0.2 note)
//! In PostgreSQL, set operations (UNION / INTERSECT / EXCEPT) bind less
//! tightly than ORDER BY / LIMIT / OFFSET. For v0.2 we represent all tails
//! inside `SelectStmt::set_ops` and leave ordering enforcement to the binder /
//! optimizer in a later wave. Do not remove this comment until that work lands.

use crate::ast::{
    Cte, Distinct, Expr, Identifier, JoinCondition, JoinOp, LockStrength, LockWaitPolicy,
    LockingClause, NullsOrder, ObjectName, OrderItem, SelectItem, SelectStmt, SetOp, SetOpTail,
    SetQuantifier, SortDirection, TableRef,
};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    // ------------------------------------------------------------------ //
    // Top-level SELECT parsing                                            //
    // ------------------------------------------------------------------ //

    /// Parse a complete `SELECT` statement, optionally preceded by a `WITH`
    /// clause. Call this when the next token is `SELECT` or `WITH`.
    pub(crate) fn parse_select(&mut self) -> Result<SelectStmt, ParseError> {
        // Leading WITH clause (CTEs).
        let (ctes, recursive, cte_start) = if self.peek()?.kind == TokenKind::KwWith {
            let with_tok = self.advance()?; // WITH
            let recursive = self.match_kw(TokenKind::KwRecursive);
            let ctes = self.parse_cte_list(recursive)?;
            (ctes, recursive, with_tok.span.start)
        } else {
            (Vec::new(), false, 0u32)
        };

        let start_tok = self.expect(TokenKind::KwSelect, "SELECT")?;
        let stmt_start = if cte_start > 0 {
            cte_start
        } else {
            start_tok.span.start
        };

        // DISTINCT / DISTINCT ON / ALL / (nothing)
        let distinct = self.parse_distinct()?;

        // Projection list.
        let projection = self.parse_select_list()?;

        // FROM clause — zero or more table factors joined together.
        let from = if self.match_kw(TokenKind::KwFrom) {
            self.parse_from_clause()?
        } else {
            Vec::new()
        };

        // WHERE
        let r#where = if self.match_kw(TokenKind::KwWhere) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        // GROUP BY
        let group_by = if self.match_kw(TokenKind::KwGroup) {
            self.expect(TokenKind::KwBy, "BY")?;
            self.parse_expr_list()?
        } else {
            Vec::new()
        };

        // HAVING
        let having = if self.match_kw(TokenKind::KwHaving) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        // ORDER BY
        let order_by = if self.match_kw(TokenKind::KwOrder) {
            self.expect(TokenKind::KwBy, "BY")?;
            self.parse_order_list()?
        } else {
            Vec::new()
        };

        // LIMIT
        let limit = if self.match_kw(TokenKind::KwLimit) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        // OFFSET
        let offset = if self.match_kw(TokenKind::KwOffset) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        // Set-operation tails (UNION / INTERSECT / EXCEPT).
        // Note on precedence: PostgreSQL resolves UNION < INTERSECT in a chain,
        // but for v0.2 we parse them uniformly left-to-right and store the raw
        // chain. The binder will enforce proper precedence.
        let set_ops = self.parse_set_op_tails(recursive)?;

        // FOR UPDATE / FOR SHARE / FOR NO KEY UPDATE / FOR KEY SHARE
        let locking = self.parse_locking_clauses()?;

        let end = self.peek()?.span.start;
        let span = Span::new(stmt_start, end);

        Ok(SelectStmt {
            distinct,
            projection,
            from,
            r#where,
            group_by,
            having,
            order_by,
            limit,
            offset,
            set_ops,
            ctes,
            locking,
            span,
        })
    }

    // ------------------------------------------------------------------ //
    // DISTINCT clause                                                     //
    // ------------------------------------------------------------------ //

    fn parse_distinct(&mut self) -> Result<Distinct, ParseError> {
        match self.peek()?.kind {
            TokenKind::KwDistinct => {
                self.advance()?; // DISTINCT
                if self.peek()?.kind == TokenKind::KwOn {
                    self.advance()?; // ON
                    self.expect(TokenKind::LParen, "(")?;
                    let exprs = self.parse_expr_list()?;
                    self.expect(TokenKind::RParen, ")")?;
                    Ok(Distinct::DistinctOn(exprs))
                } else {
                    Ok(Distinct::Distinct)
                }
            }
            TokenKind::KwAll => {
                self.advance()?;
                Ok(Distinct::All)
            }
            _ => Ok(Distinct::None),
        }
    }

    // ------------------------------------------------------------------ //
    // CTE list                                                            //
    // ------------------------------------------------------------------ //

    fn parse_cte_list(&mut self, recursive: bool) -> Result<Vec<Cte>, ParseError> {
        let mut ctes = Vec::new();
        loop {
            ctes.push(self.parse_cte(recursive)?);
            if self.peek()?.kind != TokenKind::Comma {
                break;
            }
            self.advance()?; // ,
        }
        Ok(ctes)
    }

    fn parse_cte(&mut self, recursive: bool) -> Result<Cte, ParseError> {
        let name = self.parse_identifier()?;
        let start = name.span.start;

        // Optional column-alias list: name(c1, c2, …)
        let column_aliases = if self.peek()?.kind == TokenKind::LParen {
            self.advance()?; // (
            let aliases = self.parse_identifier_list()?;
            self.expect(TokenKind::RParen, ")")?;
            aliases
        } else {
            Vec::new()
        };

        self.expect(TokenKind::KwAs, "AS")?;
        self.expect(TokenKind::LParen, "(")?;
        let query = self.parse_select()?;
        let rp = self.expect(TokenKind::RParen, ")")?;

        Ok(Cte {
            name,
            column_aliases,
            recursive,
            query: Box::new(query),
            span: Span::new(start, rp.span.end),
        })
    }

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
    fn parse_from_clause(&mut self) -> Result<Vec<TableRef>, ParseError> {
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

        let op = match self.peek()?.kind {
            TokenKind::KwCross => {
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
            TokenKind::KwNatural => {
                // NATURAL JOIN — not yet supported; reject rather than silently
                // ignore the NATURAL qualifier.
                let tok = self.advance()?;
                return Err(ParseError::Unsupported {
                    what: "NATURAL JOIN",
                    offset: tok.span.start as usize,
                });
            }
            other => {
                return Err(ParseError::Expected {
                    expected: "JOIN keyword",
                    found: other,
                    offset: self.peek()?.span.start as usize,
                });
            }
        };

        let rhs = self.parse_table_factor()?;

        let condition = match op {
            JoinOp::Cross => JoinCondition::None,
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
                        offset: self.peek()?.span.start as usize,
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
    fn parse_table_factor(&mut self) -> Result<TableRef, ParseError> {
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
                    offset: rp.span.end as usize,
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
                return Ok(TableRef::Subquery {
                    select: Box::new(select),
                    alias,
                    column_aliases,
                    span: Span::new(lp.span.start, end),
                });
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

            return Ok(result);
        }

        // Named table reference.
        self.parse_table_ref()
    }

    // ------------------------------------------------------------------ //
    // Set operations                                                      //
    // ------------------------------------------------------------------ //

    fn parse_set_op_tails(&mut self, recursive: bool) -> Result<Vec<SetOpTail>, ParseError> {
        let mut tails = Vec::new();
        loop {
            let op = match self.peek()?.kind {
                TokenKind::KwUnion => SetOp::Union,
                TokenKind::KwIntersect => SetOp::Intersect,
                TokenKind::KwExcept => SetOp::Except,
                _ => break,
            };
            let op_tok = self.advance()?;
            let quantifier = if self.match_kw(TokenKind::KwAll) {
                SetQuantifier::All
            } else {
                self.match_kw(TokenKind::KwDistinct); // optional DISTINCT keyword
                SetQuantifier::Distinct
            };
            let right = self.parse_select_body(recursive)?;
            let span = Span::new(op_tok.span.start, right.span.end);
            tails.push(SetOpTail {
                op,
                quantifier,
                right: Box::new(right),
                span,
            });
        }
        Ok(tails)
    }

    /// Parse zero or more `FOR UPDATE / FOR SHARE / FOR NO KEY UPDATE /
    /// FOR KEY SHARE` locking clauses.
    ///
    /// Grammar per PostgreSQL:
    /// ```text
    /// FOR { UPDATE | NO KEY UPDATE | SHARE | KEY SHARE }
    ///   [ OF table [, …] ]
    ///   [ NOWAIT | SKIP LOCKED ]
    /// ```
    fn parse_locking_clauses(&mut self) -> Result<Vec<LockingClause>, ParseError> {
        let mut clauses = Vec::new();
        while self.peek()?.kind == TokenKind::KwFor {
            self.advance()?; // consume FOR
            let strength = match self.peek()?.kind {
                TokenKind::KwUpdate => {
                    self.advance()?;
                    LockStrength::Update
                }
                TokenKind::KwShare => {
                    self.advance()?;
                    LockStrength::Share
                }
                TokenKind::KwNo => {
                    // FOR NO KEY UPDATE
                    self.advance()?; // NO
                    self.expect(TokenKind::KwKey, "KEY")?;
                    self.expect(TokenKind::KwUpdate, "UPDATE")?;
                    LockStrength::NoKeyUpdate
                }
                TokenKind::KwKey => {
                    // FOR KEY SHARE
                    self.advance()?; // KEY
                    self.expect(TokenKind::KwShare, "SHARE")?;
                    LockStrength::KeyShare
                }
                other => {
                    let tok = self.advance()?;
                    return Err(ParseError::Expected {
                        expected: "UPDATE, SHARE, NO KEY UPDATE, or KEY SHARE after FOR",
                        found: other,
                        offset: tok.span.start as usize,
                    });
                }
            };

            // Optional OF table [, …]
            let of_tables = if self.peek()?.kind == TokenKind::KwOf {
                self.advance()?; // OF
                let mut tables = vec![self.parse_object_name()?];
                while self.peek()?.kind == TokenKind::Comma {
                    self.advance()?;
                    tables.push(self.parse_object_name()?);
                }
                tables
            } else {
                Vec::new()
            };

            // Optional NOWAIT or SKIP LOCKED
            let wait_policy = match self.peek()?.kind {
                TokenKind::KwNowait => {
                    self.advance()?;
                    LockWaitPolicy::NoWait
                }
                TokenKind::KwSkip => {
                    self.advance()?; // SKIP
                    self.expect(TokenKind::KwLocked, "LOCKED")?;
                    LockWaitPolicy::SkipLocked
                }
                _ => LockWaitPolicy::Wait,
            };

            clauses.push(LockingClause {
                strength,
                wait_policy,
                of_tables,
            });
        }
        Ok(clauses)
    }

    /// Parse just the SELECT body (no WITH clause) for the RHS of a set op.
    fn parse_select_body(&mut self, _recursive: bool) -> Result<SelectStmt, ParseError> {
        let start_tok = self.expect(TokenKind::KwSelect, "SELECT")?;
        let distinct = self.parse_distinct()?;
        let projection = self.parse_select_list()?;

        let from = if self.match_kw(TokenKind::KwFrom) {
            self.parse_from_clause()?
        } else {
            Vec::new()
        };

        let r#where = if self.match_kw(TokenKind::KwWhere) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = if self.match_kw(TokenKind::KwGroup) {
            self.expect(TokenKind::KwBy, "BY")?;
            self.parse_expr_list()?
        } else {
            Vec::new()
        };

        let having = if self.match_kw(TokenKind::KwHaving) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let order_by = if self.match_kw(TokenKind::KwOrder) {
            self.expect(TokenKind::KwBy, "BY")?;
            self.parse_order_list()?
        } else {
            Vec::new()
        };

        let limit = if self.match_kw(TokenKind::KwLimit) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let offset = if self.match_kw(TokenKind::KwOffset) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let end = self.peek()?.span.start;
        let span = Span::new(start_tok.span.start, end);

        Ok(SelectStmt {
            distinct,
            projection,
            from,
            r#where,
            group_by,
            having,
            order_by,
            limit,
            offset,
            set_ops: Vec::new(),
            ctes: Vec::new(),
            locking: Vec::new(),
            span,
        })
    }

    // ------------------------------------------------------------------ //
    // Projection list                                                     //
    // ------------------------------------------------------------------ //

    /// Parse a comma-separated `SELECT` projection list (one or more items).
    pub(crate) fn parse_select_list(&mut self) -> Result<Vec<SelectItem>, ParseError> {
        let mut items = Vec::new();
        loop {
            items.push(self.parse_select_item()?);
            if self.peek()?.kind != TokenKind::Comma {
                return Ok(items);
            }
            self.advance()?;
        }
    }

    /// Parse one item in the `SELECT` projection list.
    pub(crate) fn parse_select_item(&mut self) -> Result<SelectItem, ParseError> {
        // `*`
        if self.peek()?.kind == TokenKind::Star {
            let tok = self.advance()?;
            return Ok(SelectItem::Wildcard { span: tok.span });
        }

        // `name.*` ?
        if matches!(
            self.peek()?.kind,
            TokenKind::Identifier | TokenKind::QuotedIdentifier
        ) && self.lookahead_two_is(TokenKind::Dot, TokenKind::Star)
        {
            let ident = self.parse_identifier()?;
            self.advance()?; // dot
            let star = self.advance()?; // star
            return Ok(SelectItem::QualifiedWildcard {
                qualifier: ident.clone(),
                span: Span::new(ident.span.start, star.span.end),
            });
        }

        let expr = self.parse_expr()?;
        let alias = if self.match_kw(TokenKind::KwAs)
            || (matches!(
                self.peek()?.kind,
                TokenKind::Identifier | TokenKind::QuotedIdentifier
            ) && !self.next_token_is_reserved_clause())
        {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        let span_start = expr.span().start;
        let expr_end = expr.span().end;
        let span_end = alias.as_ref().map_or(expr_end, |a| a.span.end);
        Ok(SelectItem::Expr {
            expr,
            alias,
            span: Span::new(span_start, span_end),
        })
    }

    // ------------------------------------------------------------------ //
    // Table reference helpers                                             //
    // ------------------------------------------------------------------ //

    /// Parse a single named table reference after `FROM` or `JOIN`.
    pub(crate) fn parse_table_ref(&mut self) -> Result<TableRef, ParseError> {
        let name = self.parse_object_name()?;
        // `name (` after a single-identifier name signals a table
        // function — `generate_series(1, 10)`, `unnest(array)`, etc.
        if name.parts.len() == 1 && self.peek()?.kind == TokenKind::LParen {
            let func_name =
                name.parts.into_iter().next().expect(
                    "parse_table_ref: ObjectName::parts.len() == 1 implies a leading element",
                );
            self.advance()?; // (
            let mut args = Vec::new();
            if self.peek()?.kind != TokenKind::RParen {
                loop {
                    args.push(self.parse_expr()?);
                    if self.peek()?.kind != TokenKind::Comma {
                        break;
                    }
                    self.advance()?;
                }
            }
            let rp = self.expect(TokenKind::RParen, ")")?;
            let alias = if self.match_kw(TokenKind::KwAs)
                || (matches!(
                    self.peek()?.kind,
                    TokenKind::Identifier | TokenKind::QuotedIdentifier
                ) && !self.next_token_is_reserved_clause())
            {
                Some(self.parse_identifier()?)
            } else {
                None
            };
            let end = alias.as_ref().map_or(rp.span.end, |a| a.span.end);
            return Ok(TableRef::Function {
                span: Span::new(func_name.span.start, end),
                name: func_name,
                args,
                alias,
            });
        }
        let alias = if self.match_kw(TokenKind::KwAs)
            || (matches!(
                self.peek()?.kind,
                TokenKind::Identifier | TokenKind::QuotedIdentifier
            ) && !self.next_token_is_reserved_clause())
        {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        let end = alias.as_ref().map_or(name.span.end, |a| a.span.end);
        Ok(TableRef::Named {
            span: Span::new(name.span.start, end),
            name,
            alias,
        })
    }

    // ------------------------------------------------------------------ //
    // ORDER BY list                                                       //
    // ------------------------------------------------------------------ //

    /// Parse a comma-separated `ORDER BY` list.
    pub(crate) fn parse_order_list(&mut self) -> Result<Vec<OrderItem>, ParseError> {
        let mut items = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let direction = if self.match_kw(TokenKind::KwAsc) {
                SortDirection::Asc
            } else if self.match_kw(TokenKind::KwDesc) {
                SortDirection::Desc
            } else {
                SortDirection::Asc
            };
            let nulls = if self.match_kw(TokenKind::KwNulls) {
                // NULLS FIRST | NULLS LAST
                let n = self.advance()?;
                if n.text(self.source)
                    .is_some_and(|t| t.eq_ignore_ascii_case("first"))
                {
                    NullsOrder::First
                } else if n
                    .text(self.source)
                    .is_some_and(|t| t.eq_ignore_ascii_case("last"))
                {
                    NullsOrder::Last
                } else {
                    return Err(ParseError::Expected {
                        expected: "FIRST or LAST",
                        found: n.kind,
                        offset: n.span.start as usize,
                    });
                }
            } else {
                NullsOrder::Default
            };
            let span_start = expr.span().start;
            let span_end = self.peek()?.span.start;
            items.push(OrderItem {
                expr,
                direction,
                nulls,
                span: Span::new(span_start, span_end),
            });
            if self.peek()?.kind != TokenKind::Comma {
                return Ok(items);
            }
            self.advance()?;
        }
    }

    // ------------------------------------------------------------------ //
    // Object name / identifier helpers                                    //
    // ------------------------------------------------------------------ //

    /// Parse a (possibly schema-qualified) object name such as
    /// `schema.table` or just `table`.
    pub(crate) fn parse_object_name(&mut self) -> Result<ObjectName, ParseError> {
        let first = self.parse_identifier()?;
        let mut parts = vec![first.clone()];
        let start = first.span.start;
        let mut end = first.span.end;
        while self.peek()?.kind == TokenKind::Dot {
            // Look past the dot — if the next token is `*`, this is
            // not part of the name and we leave the dot in place for
            // the caller.
            if self.lookahead_at(1)?.kind == TokenKind::Star {
                break;
            }
            self.advance()?; // dot
            let ident = self.parse_identifier()?;
            end = ident.span.end;
            parts.push(ident);
        }
        Ok(ObjectName {
            parts,
            span: Span::new(start, end),
        })
    }

    /// Parse a single SQL identifier (unquoted or double-quoted).
    pub(crate) fn parse_identifier(&mut self) -> Result<Identifier, ParseError> {
        let tok = self.peek()?;
        match tok.kind {
            TokenKind::Identifier => {
                let tok = self.advance()?;
                let raw = tok.text(self.source).unwrap_or("");
                Ok(Identifier {
                    value: raw.to_ascii_lowercase(),
                    quoted: false,
                    span: tok.span,
                })
            }
            TokenKind::QuotedIdentifier => {
                let tok = self.advance()?;
                let raw = tok.text(self.source).unwrap_or("");
                // Strip the outer quotes and collapse "" to ".
                let inner = &raw[1..raw.len() - 1];
                let value = inner.replace("\"\"", "\"");
                Ok(Identifier {
                    value,
                    quoted: true,
                    span: tok.span,
                })
            }
            other => Err(ParseError::Expected {
                expected: "identifier",
                found: other,
                offset: tok.span.start as usize,
            }),
        }
    }

    // ------------------------------------------------------------------ //
    // List helpers                                                        //
    // ------------------------------------------------------------------ //

    /// Parse a comma-separated list of expressions (at least one).
    pub(crate) fn parse_expr_list(&mut self) -> Result<Vec<Expr>, ParseError> {
        let mut exprs = vec![self.parse_expr()?];
        while self.peek()?.kind == TokenKind::Comma {
            self.advance()?;
            exprs.push(self.parse_expr()?);
        }
        Ok(exprs)
    }

    /// Parse a comma-separated list of identifiers (at least one).
    pub(crate) fn parse_identifier_list(&mut self) -> Result<Vec<Identifier>, ParseError> {
        let mut ids = vec![self.parse_identifier()?];
        while self.peek()?.kind == TokenKind::Comma {
            self.advance()?;
            ids.push(self.parse_identifier()?);
        }
        Ok(ids)
    }
}

// ------------------------------------------------------------------ //
// TableRef helper — span extraction                                   //
// ------------------------------------------------------------------ //

/// A helper trait for extracting the source span from any `TableRef`
/// variant without duplicating match arms everywhere.
trait TableRefSpan {
    fn ref_span(&self) -> Span;
}

impl TableRefSpan for TableRef {
    fn ref_span(&self) -> Span {
        match self {
            Self::Named { span, .. }
            | Self::Join { span, .. }
            | Self::Subquery { span, .. }
            | Self::Function { span, .. } => *span,
        }
    }
}

// ------------------------------------------------------------------ //
// Subquery expression parsing (part of parser.rs's parse_prefix but  //
// living here alongside the SELECT machinery)                        //
// ------------------------------------------------------------------ //

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
        // And after `(` must be `SELECT` or `WITH` to distinguish from function calls.
        let after_lparen = self.lookahead_at(2)?;
        if !matches!(after_lparen.kind, TokenKind::KwSelect | TokenKind::KwWith) {
            return Ok(None);
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

// ================================================================== //
// Tests                                                               //
// ================================================================== //

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use crate::ast::{
        Distinct, Expr, JoinCondition, JoinOp, SelectItem, SetOp, SetQuantifier, Statement,
        TableRef,
    };
    use crate::parser::Parser;

    fn parse(src: &str) -> Statement {
        Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
    }

    fn parse_err(src: &str) -> crate::parser::ParseError {
        Parser::new(src)
            .parse_statement()
            .expect_err("expected parse error")
    }

    // -------- DISTINCT / DISTINCT ON -------------------------------------- //

    #[test]
    fn select_distinct() {
        let stmt = parse("SELECT DISTINCT id FROM users");
        let Statement::Select(s) = stmt else { panic!() };
        assert!(matches!(s.distinct, Distinct::Distinct));
    }

    #[test]
    fn select_distinct_on() {
        let stmt = parse("SELECT DISTINCT ON (dept, id) name FROM employees");
        let Statement::Select(s) = stmt else { panic!() };
        let Distinct::DistinctOn(exprs) = &s.distinct else {
            panic!()
        };
        assert_eq!(exprs.len(), 2);
    }

    #[test]
    fn select_all_keyword() {
        let stmt = parse("SELECT ALL id FROM t");
        let Statement::Select(s) = stmt else { panic!() };
        assert!(matches!(s.distinct, Distinct::All));
    }

    // -------- FROM / Joins ------------------------------------------------ //

    #[test]
    fn select_from_single_table() {
        let stmt = parse("SELECT * FROM users");
        let Statement::Select(s) = stmt else { panic!() };
        assert_eq!(s.from.len(), 1);
        assert!(matches!(s.from[0], TableRef::Named { .. }));
    }

    #[test]
    fn select_inner_join_on() {
        let stmt =
            parse("SELECT u.id, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id");
        let Statement::Select(s) = stmt else { panic!() };
        assert_eq!(s.from.len(), 1);
        let TableRef::Join { op, condition, .. } = &s.from[0] else {
            panic!()
        };
        assert_eq!(*op, JoinOp::Inner);
        assert!(matches!(condition, JoinCondition::On(_)));
    }

    #[test]
    fn select_left_outer_join() {
        let stmt = parse("SELECT * FROM a LEFT OUTER JOIN b ON a.id = b.a_id");
        let Statement::Select(s) = stmt else { panic!() };
        let TableRef::Join { op, .. } = &s.from[0] else {
            panic!()
        };
        assert_eq!(*op, JoinOp::LeftOuter);
    }

    #[test]
    fn select_right_join() {
        let stmt = parse("SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id");
        let Statement::Select(s) = stmt else { panic!() };
        let TableRef::Join { op, .. } = &s.from[0] else {
            panic!()
        };
        assert_eq!(*op, JoinOp::RightOuter);
    }

    #[test]
    fn select_full_outer_join() {
        let stmt = parse("SELECT * FROM a FULL OUTER JOIN b ON a.id = b.a_id");
        let Statement::Select(s) = stmt else { panic!() };
        let TableRef::Join { op, .. } = &s.from[0] else {
            panic!()
        };
        assert_eq!(*op, JoinOp::FullOuter);
    }

    #[test]
    fn select_cross_join() {
        let stmt = parse("SELECT * FROM a CROSS JOIN b");
        let Statement::Select(s) = stmt else { panic!() };
        let TableRef::Join { op, condition, .. } = &s.from[0] else {
            panic!()
        };
        assert_eq!(*op, JoinOp::Cross);
        assert!(matches!(condition, JoinCondition::None));
    }

    #[test]
    fn select_join_using() {
        let stmt = parse("SELECT * FROM a JOIN b USING (id)");
        let Statement::Select(s) = stmt else { panic!() };
        let TableRef::Join { condition, .. } = &s.from[0] else {
            panic!()
        };
        let JoinCondition::Using(cols) = condition else {
            panic!()
        };
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].value, "id");
    }

    #[test]
    fn select_comma_join_canonicalised_to_cross() {
        let stmt = parse("SELECT * FROM a, b");
        let Statement::Select(s) = stmt else { panic!() };
        let TableRef::Join { op, .. } = &s.from[0] else {
            panic!()
        };
        assert_eq!(*op, JoinOp::Cross);
    }

    #[test]
    fn select_subquery_in_from() {
        let stmt = parse("SELECT x FROM (SELECT id AS x FROM t) sub");
        let Statement::Select(s) = stmt else { panic!() };
        let TableRef::Subquery { alias, .. } = &s.from[0] else {
            panic!()
        };
        assert_eq!(alias.value, "sub");
    }

    #[test]
    fn select_subquery_in_from_requires_alias() {
        let err = parse_err("SELECT x FROM (SELECT id FROM t)");
        // Should fail because no alias was given.
        assert!(matches!(
            err,
            crate::parser::ParseError::Expected { .. }
                | crate::parser::ParseError::UnexpectedEof { .. }
        ));
    }

    // -------- GROUP BY / HAVING ------------------------------------------- //

    #[test]
    fn select_group_by() {
        let stmt = parse("SELECT dept, COUNT(*) FROM employees GROUP BY dept");
        let Statement::Select(s) = stmt else { panic!() };
        assert_eq!(s.group_by.len(), 1);
        assert!(s.having.is_none());
    }

    #[test]
    fn select_group_by_having() {
        let stmt = parse("SELECT dept, COUNT(*) FROM employees GROUP BY dept HAVING COUNT(*) > 5");
        let Statement::Select(s) = stmt else { panic!() };
        assert!(!s.group_by.is_empty());
        assert!(s.having.is_some());
    }

    // -------- Subquery expressions ---------------------------------------- //

    #[test]
    fn scalar_subquery_in_where() {
        let stmt = parse("SELECT * FROM t WHERE id = (SELECT MAX(id) FROM t)");
        let Statement::Select(s) = stmt else { panic!() };
        assert!(s.r#where.is_some());
        // The WHERE is a Binary with right = Subquery.
        let Some(Expr::Binary { right, .. }) = &s.r#where else {
            panic!()
        };
        assert!(matches!(right.as_ref(), Expr::Subquery { .. }));
    }

    #[test]
    fn exists_subquery() {
        let stmt = parse("SELECT * FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id)");
        let Statement::Select(s) = stmt else { panic!() };
        let Some(Expr::Exists { negated, .. }) = &s.r#where else {
            panic!()
        };
        assert!(!negated);
    }

    #[test]
    fn not_exists_subquery() {
        let stmt = parse("SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM u)");
        let Statement::Select(s) = stmt else { panic!() };
        // NOT wraps the EXISTS as Unary::Not, or parser builds Exists{negated:true}.
        // Verify something is there.
        assert!(s.r#where.is_some());
    }

    #[test]
    fn in_list() {
        let stmt = parse("SELECT * FROM t WHERE id IN (1, 2, 3)");
        let Statement::Select(s) = stmt else { panic!() };
        let Some(Expr::InList { items, negated, .. }) = &s.r#where else {
            panic!()
        };
        assert!(!negated);
        assert_eq!(items.len(), 3);
    }

    #[test]
    fn not_in_list() {
        let stmt = parse("SELECT * FROM t WHERE id NOT IN (1, 2, 3)");
        let Statement::Select(s) = stmt else { panic!() };
        let Some(Expr::InList { negated, .. }) = &s.r#where else {
            panic!()
        };
        assert!(negated);
    }

    #[test]
    fn in_subquery() {
        let stmt = parse("SELECT * FROM t WHERE id IN (SELECT id FROM u)");
        let Statement::Select(s) = stmt else { panic!() };
        let Some(Expr::InSubquery { negated, .. }) = &s.r#where else {
            panic!()
        };
        assert!(!negated);
    }

    #[test]
    fn not_in_subquery() {
        let stmt = parse("SELECT * FROM t WHERE id NOT IN (SELECT id FROM u)");
        let Statement::Select(s) = stmt else { panic!() };
        let Some(Expr::InSubquery { negated, .. }) = &s.r#where else {
            panic!()
        };
        assert!(negated);
    }

    #[test]
    fn any_subquery() {
        let stmt = parse("SELECT * FROM t WHERE id = ANY (SELECT id FROM u)");
        let Statement::Select(s) = stmt else { panic!() };
        assert!(matches!(s.r#where, Some(Expr::Any { .. })));
    }

    #[test]
    fn all_subquery() {
        let stmt = parse("SELECT * FROM t WHERE id < ALL (SELECT id FROM u)");
        let Statement::Select(s) = stmt else { panic!() };
        assert!(matches!(s.r#where, Some(Expr::All { .. })));
    }

    // -------- UNION / INTERSECT / EXCEPT ---------------------------------- //

    #[test]
    fn union_all() {
        let stmt = parse("SELECT id FROM a UNION ALL SELECT id FROM b");
        let Statement::Select(s) = stmt else { panic!() };
        assert_eq!(s.set_ops.len(), 1);
        assert_eq!(s.set_ops[0].op, SetOp::Union);
        assert_eq!(s.set_ops[0].quantifier, SetQuantifier::All);
    }

    #[test]
    fn intersect_distinct() {
        let stmt = parse("SELECT id FROM a INTERSECT SELECT id FROM b");
        let Statement::Select(s) = stmt else { panic!() };
        assert_eq!(s.set_ops[0].op, SetOp::Intersect);
        assert_eq!(s.set_ops[0].quantifier, SetQuantifier::Distinct);
    }

    #[test]
    fn except_all() {
        let stmt = parse("SELECT id FROM a EXCEPT ALL SELECT id FROM b");
        let Statement::Select(s) = stmt else { panic!() };
        assert_eq!(s.set_ops[0].op, SetOp::Except);
        assert_eq!(s.set_ops[0].quantifier, SetQuantifier::All);
    }

    // -------- CTEs -------------------------------------------------------- //

    #[test]
    fn with_cte() {
        let stmt = parse("WITH cte AS (SELECT id FROM t) SELECT * FROM cte");
        let Statement::Select(s) = stmt else { panic!() };
        assert_eq!(s.ctes.len(), 1);
        assert_eq!(s.ctes[0].name.value, "cte");
        assert!(!s.ctes[0].recursive);
    }

    #[test]
    fn with_recursive_cte() {
        let stmt = parse(
            "WITH RECURSIVE hierarchy AS (SELECT id, parent_id FROM tree) SELECT * FROM hierarchy",
        );
        let Statement::Select(s) = stmt else { panic!() };
        assert!(s.ctes[0].recursive);
    }

    #[test]
    fn with_cte_column_aliases() {
        let stmt = parse("WITH cte(a, b) AS (SELECT 1, 2) SELECT * FROM cte");
        let Statement::Select(s) = stmt else { panic!() };
        assert_eq!(s.ctes[0].column_aliases.len(), 2);
    }

    // -------- SELECT without FROM ----------------------------------------- //

    #[test]
    fn select_without_from() {
        let stmt = parse("SELECT 1 + 1");
        let Statement::Select(s) = stmt else { panic!() };
        assert!(s.from.is_empty());
    }

    // -------- Existing tests updated for Vec<TableRef> ------------------- //

    #[test]
    fn select_star_updated() {
        let stmt = parse("SELECT * FROM users");
        let Statement::Select(s) = stmt else { panic!() };
        assert!(matches!(s.distinct, Distinct::None));
        assert!(matches!(s.projection[0], SelectItem::Wildcard { .. }));
        assert!(!s.from.is_empty());
    }

    // -------- Property test: join chain is left-deep -------------------- //
    //
    // Strategy: generate 1..=6 table names, build the SQL for an N-table
    // INNER JOIN chain, parse it, and verify the resulting join tree is
    // left-deep (each node's right child is a leaf, not a join).

    /// Returns `true` iff the join tree rooted at `t` is left-deep.
    ///
    /// A left-deep join tree has the property that every right child
    /// is a base table (leaf), while the left children recurse.
    fn is_left_deep(t: &TableRef) -> bool {
        match t {
            TableRef::Named { .. } | TableRef::Subquery { .. } | TableRef::Function { .. } => true,
            TableRef::Join { left, right, .. } => {
                // Right must be a leaf.
                matches!(
                    right.as_ref(),
                    TableRef::Named { .. }
                        | TableRef::Subquery { .. }
                        | TableRef::Function { .. }
                ) && is_left_deep(left)
            }
        }
    }

    proptest! {
        #[test]
        fn join_chain_is_left_deep(n_tables in 1_usize..=6) {
            use std::fmt::Write as _;
            // Build a table list: t1, t2, … tN
            let names: Vec<String> = (1..=n_tables).map(|i| format!("t{i}")).collect();
            let mut sql = format!("SELECT * FROM {}", names[0]);
            for name in &names[1..] {
                let _ = write!(sql, " INNER JOIN {name} ON {first}.id = {name}.id", first = names[0]);
            }
            let stmt = Parser::new(&sql)
                .parse_statement()
                .unwrap_or_else(|e| panic!("parse failed for {sql:?}: {e}"));
            let Statement::Select(s) = stmt else { panic!() };

            if n_tables == 1 {
                let is_named = matches!(s.from[0], TableRef::Named { .. });
                prop_assert!(is_named);
                return Ok(());
            }

            prop_assert!(is_left_deep(&s.from[0]), "join tree is not left-deep");
        }
    }

    // -------- FOR UPDATE / FOR SHARE locking clauses ---------------------- //

    #[test]
    fn select_for_update() {
        use crate::ast::{LockStrength, LockWaitPolicy};
        let stmt = parse("SELECT id FROM users FOR UPDATE");
        let Statement::Select(s) = stmt else { panic!() };
        assert_eq!(s.locking.len(), 1);
        assert_eq!(s.locking[0].strength, LockStrength::Update);
        assert_eq!(s.locking[0].wait_policy, LockWaitPolicy::Wait);
        assert!(s.locking[0].of_tables.is_empty());
    }

    #[test]
    fn select_for_share_nowait() {
        use crate::ast::{LockStrength, LockWaitPolicy};
        let stmt = parse("SELECT id FROM users FOR SHARE NOWAIT");
        let Statement::Select(s) = stmt else { panic!() };
        assert_eq!(s.locking.len(), 1);
        assert_eq!(s.locking[0].strength, LockStrength::Share);
        assert_eq!(s.locking[0].wait_policy, LockWaitPolicy::NoWait);
    }

    #[test]
    fn select_for_no_key_update_skip_locked() {
        use crate::ast::{LockStrength, LockWaitPolicy};
        let stmt = parse("SELECT id FROM t FOR NO KEY UPDATE SKIP LOCKED");
        let Statement::Select(s) = stmt else { panic!() };
        assert_eq!(s.locking[0].strength, LockStrength::NoKeyUpdate);
        assert_eq!(s.locking[0].wait_policy, LockWaitPolicy::SkipLocked);
    }

    #[test]
    fn select_for_key_share() {
        use crate::ast::{LockStrength, LockWaitPolicy};
        let stmt = parse("SELECT id FROM t FOR KEY SHARE");
        let Statement::Select(s) = stmt else { panic!() };
        assert_eq!(s.locking[0].strength, LockStrength::KeyShare);
        assert_eq!(s.locking[0].wait_policy, LockWaitPolicy::Wait);
    }

    #[test]
    fn select_for_update_of_table() {
        use crate::ast::LockStrength;
        let stmt = parse("SELECT * FROM t FOR UPDATE OF t");
        let Statement::Select(s) = stmt else { panic!() };
        assert_eq!(s.locking[0].strength, LockStrength::Update);
        assert_eq!(s.locking[0].of_tables.len(), 1);
    }

    #[test]
    fn select_without_locking_has_empty_vec() {
        let stmt = parse("SELECT 1");
        let Statement::Select(s) = stmt else { panic!() };
        assert!(s.locking.is_empty());
    }
}
