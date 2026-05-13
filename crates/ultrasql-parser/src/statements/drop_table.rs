//! Parser methods for `DROP TABLE` statements.
//!
//! Handles:
//! - `DROP TABLE name`
//! - `DROP TABLE IF EXISTS name`
//! - `DROP TABLE name, name2` (multi-table drop)
//! - `DROP TABLE name CASCADE | RESTRICT`

use crate::ast::DropTableStmt;
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse `DROP TABLE …`, consuming the `TABLE` keyword.
    ///
    /// The `DROP` keyword must already have been consumed by the caller.
    pub(crate) fn parse_drop_table(
        &mut self,
        drop_start: u32,
    ) -> Result<DropTableStmt, ParseError> {
        self.expect(TokenKind::KwTable, "TABLE")?;
        let if_exists = self.parse_if_exists()?;
        let names = self.parse_object_name_list()?;
        let cascade = self.parse_cascade_restrict();
        let end = self.peek()?.span.start;
        Ok(DropTableStmt {
            if_exists,
            names,
            cascade,
            span: Span::new(drop_start, end),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Statement;
    use crate::parser::Parser;

    fn parse_drop_table(src: &str) -> DropTableStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::DropTable(s) => s,
            other => panic!("expected DropTable, got {other:?}"),
        }
    }

    // ---- happy-path -------------------------------------------------------

    #[test]
    fn drop_table_basic() {
        let stmt = parse_drop_table("DROP TABLE users");
        assert!(!stmt.if_exists);
        assert_eq!(stmt.names.len(), 1);
        assert_eq!(stmt.names[0].to_string(), "users");
        assert!(!stmt.cascade);
    }

    #[test]
    fn drop_table_if_exists_cascade() {
        let stmt = parse_drop_table("DROP TABLE IF EXISTS orders CASCADE");
        assert!(stmt.if_exists);
        assert_eq!(stmt.names[0].to_string(), "orders");
        assert!(stmt.cascade);
    }

    #[test]
    fn drop_table_multi_table() {
        let stmt = parse_drop_table("DROP TABLE a, b, c");
        assert_eq!(stmt.names.len(), 3);
        assert_eq!(stmt.names[0].to_string(), "a");
        assert_eq!(stmt.names[1].to_string(), "b");
        assert_eq!(stmt.names[2].to_string(), "c");
    }

    #[test]
    fn drop_table_restrict() {
        let stmt = parse_drop_table("DROP TABLE t RESTRICT");
        assert!(!stmt.cascade); // RESTRICT = cascade:false
    }

    // ---- negative case ----------------------------------------------------

    #[test]
    fn drop_table_missing_name_errors() {
        let err = Parser::new("DROP TABLE").parse_statement().unwrap_err();
        assert!(matches!(
            err,
            ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
        ));
    }
}
