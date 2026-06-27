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
    Cte, Distinct, LockStrength, LockWaitPolicy, LockingClause, SelectStmt, SetOp, SetOpTail,
    SetQuantifier, TableRef,
};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

mod from_clause;
mod helpers;
mod subquery;
mod table_ref;

#[cfg(test)]
mod tests;

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

        // LIMIT / OFFSET, in either textual order.
        //
        // PostgreSQL accepts `LIMIT m OFFSET n` and `OFFSET n LIMIT m`
        // interchangeably and applies both regardless of order. We loop,
        // consuming whichever clause appears next; each may appear at most
        // once. A repeated clause is a parse error so `LIMIT 1 LIMIT 2`
        // (ambiguous) is rejected rather than silently dropping one bound.
        let mut limit = None;
        let mut offset = None;
        loop {
            if self.match_kw(TokenKind::KwLimit) {
                if limit.is_some() {
                    return Err(ParseError::Unsupported {
                        what: "duplicate LIMIT clause",
                        offset: self.peek()?.span.start_usize(),
                    });
                }
                limit = Some(self.parse_expr()?);
            } else if self.match_kw(TokenKind::KwOffset) {
                if offset.is_some() {
                    return Err(ParseError::Unsupported {
                        what: "duplicate OFFSET clause",
                        offset: self.peek()?.span.start_usize(),
                    });
                }
                offset = Some(self.parse_expr()?);
            } else {
                break;
            }
        }

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
                        offset: tok.span.start_usize(),
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
}

// ------------------------------------------------------------------ //
// TableRef helper — span extraction                                   //
// ------------------------------------------------------------------ //

/// A helper trait for extracting the source span from any `TableRef`
/// variant without duplicating match arms everywhere.
pub(super) trait TableRefSpan {
    fn ref_span(&self) -> Span;
}

impl TableRefSpan for TableRef {
    fn ref_span(&self) -> Span {
        match self {
            Self::Named { span, .. }
            | Self::Join { span, .. }
            | Self::Subquery { span, .. }
            | Self::Function { span, .. }
            | Self::JsonTable { span, .. }
            | Self::Pivot { span, .. }
            | Self::Unpivot { span, .. }
            | Self::XmlTable { span, .. } => *span,
        }
    }
}
