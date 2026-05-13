//! Parser methods for `UPDATE` statements.
//!
//! Handles the full PostgreSQL `UPDATE` syntax:
//! - `UPDATE t SET col = expr WHERE ...`
//! - `UPDATE t SET col = expr FROM other WHERE ...`
//! - `UPDATE ... RETURNING ...`

use crate::ast::UpdateStmt;
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse a complete `UPDATE` statement, starting from the `UPDATE`
    /// keyword.
    pub(crate) fn parse_update(&mut self) -> Result<UpdateStmt, ParseError> {
        let start_tok = self.expect(TokenKind::KwUpdate, "UPDATE")?;
        let table = self.parse_object_name()?;

        // Optional alias (AS alias | bare alias).
        let alias = self.parse_optional_alias(false)?;

        self.expect(TokenKind::KwSet, "SET")?;
        let set = self.parse_assignment_list()?;

        // Optional FROM clause.
        let from = if self.match_kw(TokenKind::KwFrom) {
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
        Ok(UpdateStmt {
            table,
            alias,
            set,
            from,
            r#where,
            returning,
            span: Span::new(start_tok.span.start, end),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::{Statement, UpdateStmt};
    use crate::parser::{ParseError, Parser};

    fn parse_update(src: &str) -> UpdateStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::Update(s) => *s,
            other => panic!("expected Update, got {other:?}"),
        }
    }

    // ---- happy-path -------------------------------------------------------

    #[test]
    fn update_basic() {
        let stmt = parse_update("UPDATE users SET name = 'bob' WHERE id = 1");
        assert_eq!(stmt.table.to_string(), "users");
        assert_eq!(stmt.set.len(), 1);
        assert_eq!(stmt.set[0].target.value, "name");
        assert!(stmt.r#where.is_some());
        assert!(stmt.from.is_empty());
        assert!(stmt.returning.is_empty());
    }

    #[test]
    fn update_multiple_assignments() {
        let stmt = parse_update("UPDATE t SET a = 1, b = 2, c = 3 WHERE id = 0");
        assert_eq!(stmt.set.len(), 3);
        assert_eq!(stmt.set[0].target.value, "a");
        assert_eq!(stmt.set[1].target.value, "b");
        assert_eq!(stmt.set[2].target.value, "c");
    }

    #[test]
    fn update_with_from_clause() {
        let stmt = parse_update("UPDATE t SET val = o.val FROM other o WHERE t.id = o.id");
        assert_eq!(stmt.from.len(), 1);
        assert!(stmt.r#where.is_some());
    }

    #[test]
    fn update_with_returning() {
        let stmt = parse_update("UPDATE t SET x = 5 RETURNING id, x");
        assert_eq!(stmt.returning.len(), 2);
    }

    // ---- edge cases -------------------------------------------------------

    #[test]
    fn update_no_where() {
        let stmt = parse_update("UPDATE t SET x = 42");
        assert!(stmt.r#where.is_none());
    }

    #[test]
    fn update_with_alias() {
        let stmt = parse_update("UPDATE users AS u SET active = FALSE WHERE u.id = 7");
        assert!(stmt.alias.is_some());
        assert_eq!(stmt.alias.unwrap().value, "u");
    }

    #[test]
    fn update_from_and_returning() {
        let stmt = parse_update("UPDATE a SET v = b.v FROM b WHERE a.id = b.id RETURNING a.id");
        assert_eq!(stmt.from.len(), 1);
        assert_eq!(stmt.returning.len(), 1);
    }

    // ---- negative cases ---------------------------------------------------

    #[test]
    fn update_missing_set_errors() {
        let err = Parser::new("UPDATE t WHERE x = 1")
            .parse_statement()
            .unwrap_err();
        assert!(matches!(err, ParseError::Expected { .. }));
    }

    #[test]
    fn update_missing_table_name_errors() {
        let err = Parser::new("UPDATE SET x = 1")
            .parse_statement()
            .unwrap_err();
        assert!(matches!(err, ParseError::Expected { .. }));
    }

    #[test]
    fn update_empty_input_errors() {
        let err = Parser::new("UPDATE").parse_statement().unwrap_err();
        assert!(matches!(
            err,
            ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
        ));
    }
}
