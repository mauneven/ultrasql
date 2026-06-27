//! Parser methods for `INSERT` statements.
//!
//! Handles the full PostgreSQL `INSERT` syntax:
//! - `INSERT INTO t (cols) VALUES (...), (...)`
//! - `INSERT INTO t SELECT ...`
//! - `INSERT INTO t DEFAULT VALUES`
//! - `ON CONFLICT DO NOTHING / DO UPDATE SET ...`
//! - `RETURNING ...`

use crate::ast::{
    Assignment, ConflictTarget, Identifier, InsertSource, InsertStmt, OnConflict, SelectItem,
};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse a complete `INSERT` statement, starting from the `INSERT`
    /// keyword.
    pub(crate) fn parse_insert(&mut self) -> Result<InsertStmt, ParseError> {
        let start_tok = self.expect(TokenKind::KwInsert, "INSERT")?;
        self.expect(TokenKind::KwInto, "INTO")?;
        let table = self.parse_object_name()?;

        // Optional column list: `(col1, col2, ...)`.
        let columns = if self.peek()?.kind == TokenKind::LParen {
            self.parse_insert_column_list()?
        } else {
            Vec::new()
        };

        // Determine source: DEFAULT VALUES | VALUES (...) | SELECT.
        let source = if self.peek()?.kind == TokenKind::KwDefault {
            // DEFAULT VALUES
            self.advance()?; // DEFAULT
            self.expect(TokenKind::KwValues, "VALUES")?;
            InsertSource::DefaultValues
        } else if self.peek()?.kind == TokenKind::KwValues {
            self.advance()?; // VALUES
            InsertSource::Values(self.parse_values_rows()?)
        } else if self.peek()?.kind == TokenKind::KwSelect {
            InsertSource::Select(Box::new(self.parse_select()?))
        } else {
            let tok = self.peek()?;
            return Err(ParseError::Expected {
                expected: "VALUES, DEFAULT VALUES, or SELECT",
                found: tok.kind,
                offset: tok.span.start_usize(),
            });
        };

        // Optional ON CONFLICT clause.
        let on_conflict = if self.peek()?.kind == TokenKind::KwOn {
            let next = self.lookahead_at(1)?;
            if next.kind == TokenKind::KwConflict {
                Some(self.parse_on_conflict()?)
            } else {
                None
            }
        } else {
            None
        };

        // Optional RETURNING clause.
        let returning = if self.match_kw(TokenKind::KwReturning) {
            self.parse_select_list()?
        } else {
            Vec::new()
        };

        let end = self.peek()?.span.start;
        Ok(InsertStmt {
            table,
            columns,
            source,
            on_conflict,
            returning,
            span: Span::new(start_tok.span.start, end),
        })
    }

    /// Parse the parenthesised column list `(col1, col2, ...)` in an
    /// `INSERT` statement.
    pub(crate) fn parse_insert_column_list(&mut self) -> Result<Vec<Identifier>, ParseError> {
        self.expect(TokenKind::LParen, "(")?;
        let mut cols = Vec::new();
        loop {
            cols.push(self.parse_identifier()?);
            match self.peek()?.kind {
                TokenKind::Comma => {
                    self.advance()?;
                }
                TokenKind::RParen => {
                    self.advance()?;
                    break;
                }
                other => {
                    return Err(ParseError::Expected {
                        expected: "',' or ')'",
                        found: other,
                        offset: self.peek()?.span.start_usize(),
                    });
                }
            }
        }
        Ok(cols)
    }

    /// Parse one or more value row(s): `(expr, ...), (expr, ...), ...`.
    /// Returns a `Vec` of rows, each row being a `Vec<Expr>`.
    fn parse_values_rows(&mut self) -> Result<Vec<Vec<crate::ast::Expr>>, ParseError> {
        let mut rows = Vec::new();
        loop {
            rows.push(self.parse_values_row()?);
            if self.peek()?.kind == TokenKind::Comma {
                // Peek one further to decide if the next token is `(`
                // (another row) or something else (e.g., an ON CONFLICT
                // keyword); if it's `(` consume the comma and continue.
                let after_comma = self.lookahead_at(1)?;
                if after_comma.kind == TokenKind::LParen {
                    self.advance()?; // comma
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(rows)
    }

    /// Parse a single parenthesised values row `(expr, expr, ...)`.
    pub(crate) fn parse_values_row(&mut self) -> Result<Vec<crate::ast::Expr>, ParseError> {
        self.expect(TokenKind::LParen, "(")?;
        let mut exprs = Vec::new();
        loop {
            // `DEFAULT` is only legal as a whole VALUES cell (PostgreSQL),
            // so we accept it here rather than in the general expression
            // grammar — keeping it an error in arbitrary expressions.
            if self.peek()?.kind == TokenKind::KwDefault {
                let tok = self.advance()?;
                exprs.push(crate::ast::Expr::Default { span: tok.span });
            } else {
                exprs.push(self.parse_expr()?);
            }
            match self.peek()?.kind {
                TokenKind::Comma => {
                    self.advance()?;
                }
                TokenKind::RParen => {
                    self.advance()?;
                    break;
                }
                other => {
                    return Err(ParseError::Expected {
                        expected: "',' or ')'",
                        found: other,
                        offset: self.peek()?.span.start_usize(),
                    });
                }
            }
        }
        Ok(exprs)
    }

    /// Parse an `ON CONFLICT` clause. The `ON` keyword has already been
    /// peeked but not consumed.
    fn parse_on_conflict(&mut self) -> Result<OnConflict, ParseError> {
        let on_tok = self.advance()?; // ON
        self.expect(TokenKind::KwConflict, "CONFLICT")?;

        // Optional conflict target: `(col, ...)`.
        let target = if self.peek()?.kind == TokenKind::LParen {
            Some(self.parse_conflict_target()?)
        } else {
            None
        };

        // DO NOTHING | DO UPDATE SET ...
        self.expect(TokenKind::KwDo, "DO")?;

        if self.peek()?.kind == TokenKind::KwNothing {
            let end = self.advance()?.span.end; // NOTHING
            return Ok(OnConflict::DoNothing {
                target,
                span: Span::new(on_tok.span.start, end),
            });
        }

        // DO UPDATE
        self.expect(TokenKind::KwUpdate, "UPDATE")?;
        self.expect(TokenKind::KwSet, "SET")?;
        let set = self.parse_assignment_list()?;

        let r#where = if self.match_kw(TokenKind::KwWhere) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        // Require a non-empty conflict target for DO UPDATE.
        let target = target.ok_or(ParseError::Expected {
            expected: "conflict target (column list) before DO UPDATE",
            found: TokenKind::KwDo,
            offset: on_tok.span.start_usize(),
        })?;

        let end = self.peek()?.span.start;
        Ok(OnConflict::DoUpdate {
            target,
            set,
            r#where,
            span: Span::new(on_tok.span.start, end),
        })
    }

    /// Parse a conflict target column list `(col1, col2, ...)`.
    fn parse_conflict_target(&mut self) -> Result<ConflictTarget, ParseError> {
        let lp = self.expect(TokenKind::LParen, "(")?;
        let mut cols = Vec::new();
        loop {
            cols.push(self.parse_identifier()?);
            match self.peek()?.kind {
                TokenKind::Comma => {
                    self.advance()?;
                }
                TokenKind::RParen => break,
                other => {
                    return Err(ParseError::Expected {
                        expected: "',' or ')'",
                        found: other,
                        offset: self.peek()?.span.start_usize(),
                    });
                }
            }
        }
        let rp = self.expect(TokenKind::RParen, ")")?;
        Ok(ConflictTarget {
            columns: cols,
            span: Span::new(lp.span.start, rp.span.end),
        })
    }

    /// Parse a comma-separated list of `col = expr` assignments.
    pub(crate) fn parse_assignment_list(&mut self) -> Result<Vec<Assignment>, ParseError> {
        let mut list = Vec::new();
        loop {
            let target = self.parse_identifier()?;
            self.expect(TokenKind::Eq, "=")?;
            let value = self.parse_expr()?;
            let span = Span::new(target.span.start, value.span().end);
            list.push(Assignment {
                target,
                value,
                span,
            });
            if self.peek()?.kind == TokenKind::Comma {
                // Check if the next token after the comma is an identifier
                // that could be a column name (not a clause keyword).
                let after = self.lookahead_at(1)?;
                if matches!(
                    after.kind,
                    TokenKind::Identifier | TokenKind::QuotedIdentifier
                ) {
                    self.advance()?; // comma
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(list)
    }

    /// Parse a comma-separated list of table references (for `FROM` and
    /// `USING` clauses in `UPDATE` and `DELETE`).
    pub(crate) fn parse_table_ref_list(&mut self) -> Result<Vec<crate::ast::TableRef>, ParseError> {
        let mut refs = Vec::new();
        loop {
            refs.push(self.parse_table_ref()?);
            if self.peek()?.kind == TokenKind::Comma {
                self.advance()?;
            } else {
                break;
            }
        }
        Ok(refs)
    }

    /// Convenience: parse an optional `RETURNING` clause as a
    /// [`SelectItem`] list. Returns an empty `Vec` if the keyword is
    /// absent.
    pub(crate) fn parse_optional_returning(&mut self) -> Result<Vec<SelectItem>, ParseError> {
        if self.match_kw(TokenKind::KwReturning) {
            self.parse_select_list()
        } else {
            Ok(Vec::new())
        }
    }

    /// Parse an optional `ObjectName` alias that may follow a table
    /// reference in `UPDATE` or `DELETE` (`AS alias` or bare alias).
    pub(crate) fn parse_optional_alias(
        &mut self,
        reserved_check: bool,
    ) -> Result<Option<Identifier>, ParseError> {
        if self.match_kw(TokenKind::KwAs) {
            return Ok(Some(self.parse_identifier()?));
        }
        if reserved_check
            && matches!(
                self.peek()?.kind,
                TokenKind::Identifier | TokenKind::QuotedIdentifier
            )
            && !self.next_token_is_reserved_clause()
        {
            return Ok(Some(self.parse_identifier()?));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Expr, InsertSource, Literal, Statement};
    use crate::parser::Parser;
    use proptest::prelude::*;

    fn parse_insert(src: &str) -> InsertStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::Insert(s) => *s,
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    // ---- happy-path -------------------------------------------------------

    #[test]
    fn insert_values_basic() {
        let stmt = parse_insert("INSERT INTO users (id, name) VALUES (1, 'alice')");
        assert_eq!(stmt.table.to_string(), "users");
        assert_eq!(stmt.columns.len(), 2);
        assert_eq!(stmt.columns[0].value, "id");
        assert_eq!(stmt.columns[1].value, "name");
        let InsertSource::Values(rows) = &stmt.source else {
            panic!("expected Values source")
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 2);
        assert!(stmt.on_conflict.is_none());
        assert!(stmt.returning.is_empty());
    }

    #[test]
    fn insert_multirow_values() {
        let stmt = parse_insert("INSERT INTO t (a, b) VALUES (1, 2), (3, 4), (5, 6)");
        let InsertSource::Values(rows) = &stmt.source else {
            panic!("expected Values source")
        };
        assert_eq!(rows.len(), 3);
        for row in rows {
            assert_eq!(row.len(), 2);
        }
    }

    #[test]
    fn insert_values_default_cell() {
        let stmt = parse_insert("INSERT INTO t (a, b) VALUES (1, DEFAULT), (2, 5)");
        let InsertSource::Values(rows) = &stmt.source else {
            panic!("expected Values source")
        };
        assert_eq!(rows.len(), 2);
        // First row's second cell is the DEFAULT sentinel.
        assert!(matches!(rows[0][1], Expr::Default { .. }));
        // Second row's second cell is a plain literal.
        assert!(matches!(rows[1][1], Expr::Literal(Literal::Integer { .. })));
    }

    #[test]
    fn default_keyword_rejected_outside_values() {
        // `DEFAULT` is not a general expression; the WHERE parser must
        // refuse it.
        let err = Parser::new("SELECT * FROM t WHERE a = DEFAULT").parse_statement();
        assert!(
            err.is_err(),
            "DEFAULT in an expression must be a parse error"
        );
    }

    #[test]
    fn insert_default_values() {
        let stmt = parse_insert("INSERT INTO t DEFAULT VALUES");
        assert!(matches!(stmt.source, InsertSource::DefaultValues));
        assert!(stmt.columns.is_empty());
    }

    #[test]
    fn insert_select() {
        let stmt = parse_insert("INSERT INTO dst SELECT id, name FROM src WHERE id > 0");
        assert!(matches!(stmt.source, InsertSource::Select(_)));
    }

    #[test]
    fn insert_on_conflict_do_nothing() {
        let stmt = parse_insert("INSERT INTO t (id) VALUES (1) ON CONFLICT DO NOTHING");
        assert!(matches!(
            stmt.on_conflict,
            Some(crate::ast::OnConflict::DoNothing { target: None, .. })
        ));
    }

    #[test]
    fn insert_on_conflict_with_target_do_nothing() {
        let stmt = parse_insert("INSERT INTO t (id) VALUES (1) ON CONFLICT (id) DO NOTHING");
        let Some(crate::ast::OnConflict::DoNothing {
            target: Some(ct), ..
        }) = &stmt.on_conflict
        else {
            panic!("expected DoNothing with target")
        };
        assert_eq!(ct.columns[0].value, "id");
    }

    #[test]
    fn insert_on_conflict_do_update() {
        let stmt = parse_insert(
            "INSERT INTO t (id, val) VALUES (1, 42) ON CONFLICT (id) DO UPDATE SET val = 99",
        );
        let Some(crate::ast::OnConflict::DoUpdate { set, .. }) = &stmt.on_conflict else {
            panic!("expected DoUpdate")
        };
        assert_eq!(set.len(), 1);
        assert_eq!(set[0].target.value, "val");
    }

    #[test]
    fn insert_returning() {
        let stmt = parse_insert("INSERT INTO t (x) VALUES (1) RETURNING id, x");
        assert_eq!(stmt.returning.len(), 2);
    }

    // ---- edge cases -------------------------------------------------------

    #[test]
    fn insert_no_column_list() {
        let stmt = parse_insert("INSERT INTO t VALUES (1, 2, 3)");
        assert!(stmt.columns.is_empty());
        let InsertSource::Values(rows) = &stmt.source else {
            panic!()
        };
        assert_eq!(rows[0].len(), 3);
    }

    #[test]
    fn insert_select_with_returning() {
        let stmt = parse_insert("INSERT INTO dst SELECT id FROM src RETURNING id");
        assert!(matches!(stmt.source, InsertSource::Select(_)));
        assert_eq!(stmt.returning.len(), 1);
    }

    #[test]
    fn insert_on_conflict_do_update_where() {
        let stmt = parse_insert(
            "INSERT INTO t (id, v) VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET v = 99 WHERE t.v < 99",
        );
        let Some(crate::ast::OnConflict::DoUpdate { r#where, .. }) = &stmt.on_conflict else {
            panic!()
        };
        assert!(r#where.is_some());
    }

    // ---- negative cases ---------------------------------------------------

    #[test]
    fn insert_missing_values_or_select_errors() {
        let err = Parser::new("INSERT INTO t (x)")
            .parse_statement()
            .unwrap_err();
        assert!(
            matches!(
                err,
                ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
            ),
            "unexpected error kind: {err:?}"
        );
    }

    #[test]
    fn insert_missing_table_name_errors() {
        let err = Parser::new("INSERT INTO VALUES (1)")
            .parse_statement()
            .unwrap_err();
        assert!(matches!(err, ParseError::Expected { .. }));
    }

    #[test]
    fn insert_empty_values_list_errors() {
        // VALUES () — empty row — should fail.
        let err = Parser::new("INSERT INTO t VALUES ()")
            .parse_statement()
            .unwrap_err();
        assert!(matches!(err, ParseError::Expected { .. }));
    }

    // ---- property test ----------------------------------------------------

    proptest! {
        /// For any vector of i32 values, an INSERT parsing round-trip must
        /// preserve all literals in order.
        ///
        /// Non-negative values are verified to produce an
        /// `Expr::Literal(Literal::Integer)` with the expected text.
        /// Negative values are parsed as `Expr::Unary { op: Neg, expr: Literal::Integer }`
        /// so we extract and verify the absolute-value text for those.
        #[test]
        fn prop_integer_literals_round_trip(values in proptest::collection::vec(any::<i32>(), 1..=16)) {
            let cols: Vec<String> = values.iter().enumerate().map(|(i, _)| format!("c{i}")).collect();
            let vals: Vec<String> = values.iter().map(std::string::ToString::to_string).collect();
            let sql = format!(
                "INSERT INTO t ({}) VALUES ({})",
                cols.join(", "),
                vals.join(", ")
            );
            let stmt = Parser::new(&sql).parse_statement().expect("must parse");
            let Statement::Insert(ins) = stmt else { panic!("expected Insert") };
            let InsertSource::Values(rows) = &ins.source else { panic!("expected Values") };
            prop_assert_eq!(rows.len(), 1);
            prop_assert_eq!(rows[0].len(), values.len());
            for (i, v) in values.iter().enumerate() {
                if *v >= 0 {
                    // Non-negative: directly a literal.
                    let Expr::Literal(Literal::Integer { text, .. }) = &rows[0][i] else {
                        return Err(proptest::test_runner::TestCaseError::fail(
                            format!("expected integer literal at position {i}, got {:?}", &rows[0][i])
                        ));
                    };
                    let expected = v.to_string();
                    prop_assert_eq!(text.as_str(), expected.as_str());
                } else {
                    // Negative: parsed as Unary(Neg, Literal(Integer)).
                    let Expr::Unary { op: crate::ast::UnaryOp::Neg, expr, .. } = &rows[0][i] else {
                        return Err(proptest::test_runner::TestCaseError::fail(
                            format!("expected Unary(Neg, integer) at position {i}, got {:?}", &rows[0][i])
                        ));
                    };
                    let Expr::Literal(Literal::Integer { text, .. }) = expr.as_ref() else {
                        return Err(proptest::test_runner::TestCaseError::fail(
                            format!("inner of Unary is not integer literal at position {i}")
                        ));
                    };
                    // The text should be the absolute value.
                    let abs_text = v.unsigned_abs().to_string();
                    prop_assert_eq!(text.as_str(), abs_text.as_str());
                }
            }
        }
    }
}
