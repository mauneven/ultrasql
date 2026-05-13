//! Parser methods for `DELETE` statements.
//!
//! Handles the full PostgreSQL `DELETE` syntax:
//! - `DELETE FROM t WHERE ...`
//! - `DELETE FROM t USING other WHERE ...`
//! - `DELETE ... RETURNING ...`

use crate::ast::DeleteStmt;
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse a complete `DELETE` statement, starting from the `DELETE`
    /// keyword.
    pub(crate) fn parse_delete(&mut self) -> Result<DeleteStmt, ParseError> {
        let start_tok = self.expect(TokenKind::KwDelete, "DELETE")?;
        self.expect(TokenKind::KwFrom, "FROM")?;
        let table = self.parse_object_name()?;

        // Optional alias (AS alias | bare alias).
        let alias = self.parse_optional_alias(true)?;

        // Optional USING clause.
        let using = if self.match_kw(TokenKind::KwUsing) {
            self.parse_table_ref_list()?
        } else {
            Vec::new()
        };

        let r#where = if self.match_kw(TokenKind::KwWhere) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let returning = self.parse_optional_returning()?;

        let end = self.peek()?.span.start;
        Ok(DeleteStmt {
            table,
            alias,
            using,
            r#where,
            returning,
            span: Span::new(start_tok.span.start, end),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::{DeleteStmt, Statement};
    use crate::parser::{ParseError, Parser};

    fn parse_delete(src: &str) -> DeleteStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::Delete(s) => *s,
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    // ---- happy-path -------------------------------------------------------

    #[test]
    fn delete_basic() {
        let stmt = parse_delete("DELETE FROM users WHERE id = 5");
        assert_eq!(stmt.table.to_string(), "users");
        assert!(stmt.r#where.is_some());
        assert!(stmt.using.is_empty());
        assert!(stmt.returning.is_empty());
    }

    #[test]
    fn delete_no_where() {
        let stmt = parse_delete("DELETE FROM t");
        assert!(stmt.r#where.is_none());
        assert!(stmt.using.is_empty());
    }

    #[test]
    fn delete_with_using() {
        let stmt =
            parse_delete("DELETE FROM orders o USING customers c WHERE o.customer_id = c.id");
        assert_eq!(stmt.using.len(), 1);
        assert!(stmt.r#where.is_some());
    }

    #[test]
    fn delete_with_returning() {
        let stmt = parse_delete("DELETE FROM t WHERE id = 1 RETURNING id, name");
        assert_eq!(stmt.returning.len(), 2);
    }

    // ---- edge cases -------------------------------------------------------

    #[test]
    fn delete_with_alias() {
        let stmt = parse_delete("DELETE FROM users AS u WHERE u.id = 99");
        assert!(stmt.alias.is_some());
        assert_eq!(stmt.alias.as_ref().unwrap().value, "u");
    }

    #[test]
    fn delete_using_and_returning() {
        let stmt = parse_delete("DELETE FROM a USING b WHERE a.id = b.id RETURNING a.id");
        assert_eq!(stmt.using.len(), 1);
        assert_eq!(stmt.returning.len(), 1);
    }

    #[test]
    fn delete_schema_qualified_table() {
        let stmt = parse_delete("DELETE FROM public.events WHERE id = 0");
        assert_eq!(stmt.table.parts.len(), 2);
        assert_eq!(stmt.table.parts[0].value, "public");
        assert_eq!(stmt.table.parts[1].value, "events");
    }

    // ---- negative cases ---------------------------------------------------

    #[test]
    fn delete_missing_from_errors() {
        let err = Parser::new("DELETE users WHERE id = 1")
            .parse_statement()
            .unwrap_err();
        assert!(matches!(err, ParseError::Expected { .. }));
    }

    #[test]
    fn delete_missing_table_errors() {
        let err = Parser::new("DELETE FROM WHERE id = 1")
            .parse_statement()
            .unwrap_err();
        assert!(matches!(err, ParseError::Expected { .. }));
    }

    #[test]
    fn delete_truncated_input_errors() {
        let err = Parser::new("DELETE FROM").parse_statement().unwrap_err();
        assert!(matches!(
            err,
            ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
        ));
    }
}
