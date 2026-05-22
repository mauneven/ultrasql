//! Parser methods for `CREATE INDEX` statements.
//!
//! Handles:
//! - `CREATE INDEX name ON t (col [opclass] [ASC|DESC] [NULLS FIRST|LAST])`
//! - `CREATE UNIQUE INDEX …`
//! - `CREATE AGGREGATING INDEX …`
//! - `CREATE INDEX IF NOT EXISTS …`
//! - `CREATE INDEX … USING method`
//! - `CREATE INDEX … INCLUDE (col, …)`
//! - `CREATE INDEX … WITH (option = value, …)`
//! - `CREATE INDEX … WHERE expr` (partial index)

use crate::ast::{CreateIndexStmt, IndexColumn, IndexOption, NullsOrder, SortDirection};
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
        aggregating: bool,
    ) -> Result<CreateIndexStmt, ParseError> {
        self.expect(TokenKind::KwIndex, "INDEX")?;
        let concurrently = self.match_kw(TokenKind::KwConcurrently);
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

        // Optional WITH (option = value, …)
        let options = if self.match_kw(TokenKind::KwWith) {
            self.parse_index_options()?
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
            aggregating,
            concurrently,
            if_not_exists,
            name,
            table,
            method,
            columns,
            r#where,
            include,
            options,
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

    /// Parse one entry in an index column list:
    /// `expr [opclass] [ASC|DESC] [NULLS FIRST|LAST]`.
    pub(crate) fn parse_index_column(&mut self) -> Result<IndexColumn, ParseError> {
        let expr = self.parse_expr()?;
        let start = expr.span().start;

        let opclass = if matches!(
            self.peek()?.kind,
            TokenKind::Identifier | TokenKind::QuotedIdentifier
        ) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

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
            opclass,
            direction,
            nulls,
            span: Span::new(start, end),
        })
    }

    pub(crate) fn parse_index_options(&mut self) -> Result<Vec<IndexOption>, ParseError> {
        self.expect(TokenKind::LParen, "(")?;
        let mut options = Vec::new();
        loop {
            let name = self.parse_identifier()?;
            let start = name.span.start;
            self.expect(TokenKind::Eq, "=")?;
            let value = self.parse_expr()?;
            let end = value.span().end;
            options.push(IndexOption {
                name,
                value,
                span: Span::new(start, end),
            });
            if self.peek()?.kind == TokenKind::Comma {
                self.advance()?;
            } else {
                break;
            }
        }
        self.expect(TokenKind::RParen, ")")?;
        Ok(options)
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
        assert!(!stmt.concurrently);
        assert!(stmt.if_not_exists);
        assert_eq!(stmt.columns[0].direction, SortDirection::Asc);
    }

    #[test]
    fn create_index_concurrently() {
        let stmt = parse_create_index("CREATE INDEX CONCURRENTLY idx ON users (email)");
        assert!(stmt.concurrently);
        assert_eq!(stmt.name.as_ref().unwrap().value, "idx");
    }

    #[test]
    fn create_index_using_hash_with_include() {
        let stmt =
            parse_create_index("CREATE INDEX idx ON t USING hash (id) INCLUDE (name, status)");
        assert_eq!(stmt.method.as_ref().unwrap().value, "hash");
        assert_eq!(stmt.include.len(), 2);
    }

    #[test]
    fn create_index_parses_vector_opclass() {
        let stmt =
            parse_create_index("CREATE INDEX idx ON docs USING hnsw (embedding vector_l2_ops)");
        assert_eq!(stmt.method.as_ref().unwrap().value, "hnsw");
        assert_eq!(
            stmt.columns[0].opclass.as_ref().expect("opclass").value,
            "vector_l2_ops"
        );
    }

    #[test]
    fn create_index_parses_ivfflat_with_lists_and_probes() {
        let stmt = parse_create_index(
            "CREATE INDEX idx ON docs USING ivfflat (embedding vector_l2_ops) \
             WITH (lists = 4, probes = 2)",
        );
        assert_eq!(stmt.method.as_ref().unwrap().value, "ivfflat");
        assert_eq!(stmt.options.len(), 2);
        assert_eq!(stmt.options[0].name.value, "lists");
        assert_eq!(stmt.options[1].name.value, "probes");
    }

    #[test]
    fn create_aggregating_index_parses_group_and_aggregate_keys() {
        let stmt = parse_create_index(
            "CREATE AGGREGATING INDEX fact_rollup ON fact_events \
             (tenant_id, bucket, sum(amount), count(*))",
        );
        assert!(stmt.aggregating);
        assert_eq!(stmt.name.as_ref().unwrap().value, "fact_rollup");
        assert_eq!(stmt.table.to_string(), "fact_events");
        assert_eq!(stmt.columns.len(), 4);
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
