//! Parser methods for `TRUNCATE TABLE` statements.
//!
//! Handles the PostgreSQL `TRUNCATE TABLE` syntax:
//! - `TRUNCATE TABLE t`
//! - `TRUNCATE TABLE a, b, c`
//! - `TRUNCATE TABLE t RESTART IDENTITY`
//! - `TRUNCATE TABLE t CASCADE`

use crate::ast::TruncateStmt;
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse a complete `TRUNCATE` statement, starting from the
    /// `TRUNCATE` keyword.
    pub(crate) fn parse_truncate(&mut self) -> Result<TruncateStmt, ParseError> {
        let start_tok = self.expect(TokenKind::KwTruncate, "TRUNCATE")?;
        // Optional TABLE keyword.
        let _ = self.match_kw(TokenKind::KwTable);

        // One or more comma-separated table names.
        let mut tables = Vec::new();
        tables.push(self.parse_object_name()?);
        while self.peek()?.kind == TokenKind::Comma {
            self.advance()?;
            tables.push(self.parse_object_name()?);
        }

        // Optional RESTART IDENTITY.
        let restart_identity = if self.peek()?.kind == TokenKind::KwRestart {
            let next = self.lookahead_at(1)?;
            if next.kind == TokenKind::KwIdentity {
                self.advance()?; // RESTART
                self.advance()?; // IDENTITY
                true
            } else {
                false
            }
        } else {
            false
        };

        // Optional CASCADE.
        let cascade = self.match_kw(TokenKind::KwCascade);

        let end = self.peek()?.span.start;
        Ok(TruncateStmt {
            tables,
            restart_identity,
            cascade,
            span: Span::new(start_tok.span.start, end),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::{Statement, TruncateStmt};
    use crate::parser::{ParseError, Parser};

    fn parse_truncate(src: &str) -> TruncateStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::Truncate(s) => s,
            other => panic!("expected Truncate, got {other:?}"),
        }
    }

    // ---- happy-path -------------------------------------------------------

    #[test]
    fn truncate_basic() {
        let stmt = parse_truncate("TRUNCATE TABLE users");
        assert_eq!(stmt.tables.len(), 1);
        assert_eq!(stmt.tables[0].to_string(), "users");
        assert!(!stmt.restart_identity);
        assert!(!stmt.cascade);
    }

    #[test]
    fn truncate_without_table_keyword() {
        let stmt = parse_truncate("TRUNCATE events");
        assert_eq!(stmt.tables.len(), 1);
        assert_eq!(stmt.tables[0].to_string(), "events");
    }

    #[test]
    fn truncate_multiple_tables() {
        let stmt = parse_truncate("TRUNCATE TABLE a, b, c");
        assert_eq!(stmt.tables.len(), 3);
        assert_eq!(stmt.tables[0].to_string(), "a");
        assert_eq!(stmt.tables[1].to_string(), "b");
        assert_eq!(stmt.tables[2].to_string(), "c");
    }

    #[test]
    fn truncate_restart_identity() {
        let stmt = parse_truncate("TRUNCATE TABLE t RESTART IDENTITY");
        assert!(stmt.restart_identity);
        assert!(!stmt.cascade);
    }

    #[test]
    fn truncate_cascade() {
        let stmt = parse_truncate("TRUNCATE TABLE t CASCADE");
        assert!(!stmt.restart_identity);
        assert!(stmt.cascade);
    }

    #[test]
    fn truncate_restart_identity_and_cascade() {
        let stmt = parse_truncate("TRUNCATE TABLE t RESTART IDENTITY CASCADE");
        assert!(stmt.restart_identity);
        assert!(stmt.cascade);
    }

    // ---- edge cases -------------------------------------------------------

    #[test]
    fn truncate_schema_qualified() {
        let stmt = parse_truncate("TRUNCATE TABLE public.logs");
        assert_eq!(stmt.tables[0].parts.len(), 2);
    }

    #[test]
    fn truncate_multiple_tables_with_options() {
        let stmt = parse_truncate("TRUNCATE a, b RESTART IDENTITY CASCADE");
        assert_eq!(stmt.tables.len(), 2);
        assert!(stmt.restart_identity);
        assert!(stmt.cascade);
    }

    #[test]
    fn truncate_in_statement_batch() {
        let mut p = Parser::new("TRUNCATE t; TRUNCATE s");
        let stmts = p.parse_statements().unwrap();
        assert_eq!(stmts.len(), 2);
        assert!(matches!(stmts[0], Statement::Truncate(_)));
        assert!(matches!(stmts[1], Statement::Truncate(_)));
    }

    // ---- negative cases ---------------------------------------------------

    #[test]
    fn truncate_missing_table_name_errors() {
        let err = Parser::new("TRUNCATE TABLE").parse_statement().unwrap_err();
        assert!(matches!(
            err,
            ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
        ));
    }

    #[test]
    fn truncate_empty_input_after_keyword_errors() {
        let err = Parser::new("TRUNCATE").parse_statement().unwrap_err();
        assert!(matches!(
            err,
            ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
        ));
    }

    #[test]
    fn truncate_bare_keyword_errors() {
        let err = Parser::new("TRUNCATE ;").parse_statement().unwrap_err();
        assert!(matches!(err, ParseError::Expected { .. }));
    }
}
