//! Parser methods for `CREATE INDEX` statements.
//!
//! Handles:
//! - `CREATE INDEX name ON t (col [ASC|DESC] [NULLS FIRST|LAST])`
//! - `CREATE UNIQUE INDEX …`
//! - `CREATE INDEX IF NOT EXISTS …`
//! - `CREATE INDEX … USING method`
//! - `CREATE INDEX … INCLUDE (col, …)`
//! - `CREATE INDEX … WHERE expr` (partial index)

use crate::ast::{CreateIndexStmt, IndexColumn, NullsOrder, SortDirection};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse `CREATE [UNIQUE] INDEX …`, consuming the `INDEX` keyword.
    ///
    /// The `CREATE` keyword must already have been consumed by the caller.
    /// `unique` indicates whether `UNIQUE` was already seen.
    pub(crate) fn parse_create_index(
        &mut self,
        create_start: u32,
        unique: bool,
    ) -> Result<CreateIndexStmt, ParseError> {
        self.expect(TokenKind::KwIndex, "INDEX")?;
        let if_not_exists = self.parse_if_not_exists()?;

        // Optional index name — if the next token is `ON` there is no name.
        let name = if self.peek()?.kind == TokenKind::KwOn {
            None
        } else {
            Some(self.parse_identifier()?)
        };

        self.expect(TokenKind::KwOn, "ON")?;
        let table = self.parse_object_name()?;

        // Optional USING method
        let method = if self.match_kw(TokenKind::KwUsing) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        // Column list
        self.expect(TokenKind::LParen, "(")?;
        let columns = self.parse_index_column_list()?;
        self.expect(TokenKind::RParen, ")")?;

        // Optional INCLUDE (col, …)
        let include = if self.match_kw(TokenKind::KwInclude) {
            self.parse_ident_list_paren()?
        } else {
            Vec::new()
        };

        // Optional WHERE predicate (partial index)
        let r#where = if self.match_kw(TokenKind::KwWhere) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let end = self.peek()?.span.start;
        Ok(CreateIndexStmt {
            unique,
            if_not_exists,
            name,
            table,
            method,
            columns,
            r#where,
            include,
            span: Span::new(create_start, end),
        })
    }

    /// Parse the index column list (no outer parens — those are consumed
    /// by the caller).
    fn parse_index_column_list(&mut self) -> Result<Vec<IndexColumn>, ParseError> {
        let mut cols = Vec::new();
        loop {
            cols.push(self.parse_index_column()?);
            if self.peek()?.kind == TokenKind::Comma {
                self.advance()?;
            } else {
                break;
            }
        }
        Ok(cols)
    }

    /// Parse one entry in an index column list: `expr [ASC|DESC] [NULLS FIRST|LAST]`.
    pub(crate) fn parse_index_column(&mut self) -> Result<IndexColumn, ParseError> {
        let expr = self.parse_expr()?;
        let start = expr.span().start;

        let direction = if self.match_kw(TokenKind::KwAsc) {
            SortDirection::Asc
        } else if self.match_kw(TokenKind::KwDesc) {
            SortDirection::Desc
        } else {
            SortDirection::Asc
        };

        let nulls = if self.match_kw(TokenKind::KwNulls) {
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

        let end = self.peek()?.span.start;
        Ok(IndexColumn {
            expr,
            direction,
            nulls,
            span: Span::new(start, end),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{NullsOrder, SortDirection, Statement};
    use crate::parser::Parser;

    fn parse_create_index(src: &str) -> CreateIndexStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::CreateIndex(s) => *s,
            other => panic!("expected CreateIndex, got {other:?}"),
        }
    }

    // ---- happy-path -------------------------------------------------------

    #[test]
    fn create_index_basic() {
        let stmt = parse_create_index("CREATE INDEX idx_name ON users (name)");
        assert!(!stmt.unique);
        assert!(!stmt.if_not_exists);
        assert_eq!(stmt.name.as_ref().unwrap().value, "idx_name");
        assert_eq!(stmt.table.to_string(), "users");
        assert_eq!(stmt.columns.len(), 1);
        assert!(stmt.method.is_none());
        assert!(stmt.r#where.is_none());
        assert!(stmt.include.is_empty());
    }

    #[test]
    fn create_unique_index_if_not_exists() {
        let stmt =
            parse_create_index("CREATE UNIQUE INDEX IF NOT EXISTS ux_email ON users (email ASC)");
        assert!(stmt.unique);
        assert!(stmt.if_not_exists);
        assert_eq!(stmt.columns[0].direction, SortDirection::Asc);
    }

    #[test]
    fn create_index_using_hash_with_include() {
        let stmt =
            parse_create_index("CREATE INDEX idx ON t USING hash (id) INCLUDE (name, status)");
        assert_eq!(stmt.method.as_ref().unwrap().value, "hash");
        assert_eq!(stmt.include.len(), 2);
    }

    #[test]
    fn create_index_partial_where_and_nulls_last() {
        let stmt =
            parse_create_index("CREATE INDEX idx ON t (col DESC NULLS LAST) WHERE col IS NOT NULL");
        assert_eq!(stmt.columns[0].direction, SortDirection::Desc);
        assert_eq!(stmt.columns[0].nulls, NullsOrder::Last);
        assert!(stmt.r#where.is_some());
    }

    // ---- negative case ----------------------------------------------------

    #[test]
    fn create_index_missing_on_errors() {
        let err = Parser::new("CREATE INDEX idx users (col)")
            .parse_statement()
            .unwrap_err();
        assert!(matches!(err, ParseError::Expected { .. }));
    }
}
