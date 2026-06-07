//! Parser methods for `DROP INDEX` and `REINDEX` statements.
//!
//! Handles:
//! - `DROP INDEX name [, …]`
//! - `DROP INDEX IF EXISTS name [CASCADE|RESTRICT]`
//! - `REINDEX INDEX name`
//! - `REINDEX TABLE name`

use crate::ast::{DropIndexStmt, ReindexKind, ReindexStmt};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse `DROP INDEX …`, consuming the `INDEX` keyword.
    ///
    /// The `DROP` keyword must already have been consumed by the caller.
    pub(crate) fn parse_drop_index(
        &mut self,
        drop_start: u32,
    ) -> Result<DropIndexStmt, ParseError> {
        self.expect(TokenKind::KwIndex, "INDEX")?;
        let if_exists = self.parse_if_exists()?;
        let names = self.parse_object_name_list()?;
        let cascade = self.parse_cascade_restrict();
        let end = self.peek()?.span.start;
        Ok(DropIndexStmt {
            if_exists,
            names,
            cascade,
            span: Span::new(drop_start, end),
        })
    }

    /// Parse `REINDEX { INDEX | TABLE } name`.
    ///
    /// The `REINDEX` keyword has been peeked but not consumed.
    pub(crate) fn parse_reindex(&mut self) -> Result<ReindexStmt, ParseError> {
        let start_tok = self.advance()?; // REINDEX
        let start = start_tok.span.start;

        let tok = self.peek()?;
        let kind = match tok.kind {
            TokenKind::KwIndex => {
                self.advance()?;
                ReindexKind::Index
            }
            TokenKind::KwTable => {
                self.advance()?;
                ReindexKind::Table
            }
            other => {
                return Err(ParseError::Expected {
                    expected: "INDEX or TABLE",
                    found: other,
                    offset: tok.span.start_usize(),
                });
            }
        };

        let name = self.parse_object_name()?;
        let end = self.peek()?.span.start;
        Ok(ReindexStmt {
            kind,
            name,
            span: Span::new(start, end),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{ReindexKind, Statement};
    use crate::parser::Parser;

    fn parse_drop_index(src: &str) -> DropIndexStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::DropIndex(s) => s,
            other => panic!("expected DropIndex, got {other:?}"),
        }
    }

    fn parse_reindex(src: &str) -> ReindexStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::Reindex(s) => s,
            other => panic!("expected Reindex, got {other:?}"),
        }
    }

    // ---- happy-path -------------------------------------------------------

    #[test]
    fn drop_index_basic() {
        let stmt = parse_drop_index("DROP INDEX idx_name");
        assert!(!stmt.if_exists);
        assert_eq!(stmt.names[0].to_string(), "idx_name");
        assert!(!stmt.cascade);
    }

    #[test]
    fn drop_index_if_exists_cascade() {
        let stmt = parse_drop_index("DROP INDEX IF EXISTS idx1, idx2 CASCADE");
        assert!(stmt.if_exists);
        assert_eq!(stmt.names.len(), 2);
        assert!(stmt.cascade);
    }

    #[test]
    fn reindex_index() {
        let stmt = parse_reindex("REINDEX INDEX idx_users_email");
        assert_eq!(stmt.kind, ReindexKind::Index);
        assert_eq!(stmt.name.to_string(), "idx_users_email");
    }

    #[test]
    fn reindex_table() {
        let stmt = parse_reindex("REINDEX TABLE users");
        assert_eq!(stmt.kind, ReindexKind::Table);
        assert_eq!(stmt.name.to_string(), "users");
    }

    // ---- negative case ----------------------------------------------------

    #[test]
    fn reindex_unknown_kind_errors() {
        let err = Parser::new("REINDEX SCHEMA foo")
            .parse_statement()
            .unwrap_err();
        assert!(matches!(err, ParseError::Expected { .. }));
    }
}
