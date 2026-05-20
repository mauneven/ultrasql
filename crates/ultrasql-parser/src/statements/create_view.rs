//! Parser methods for `CREATE MATERIALIZED VIEW`.

use crate::ast::CreateMaterializedViewStmt;
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::{Token, TokenKind};

impl Parser<'_> {
    /// Parse `CREATE MATERIALIZED VIEW … AS SELECT …`.
    ///
    /// `MATERIALIZED` and `VIEW` are accepted as identifier-shaped
    /// keywords so existing unquoted identifiers named `view` keep
    /// parsing in non-DDL positions.
    pub(crate) fn parse_create_materialized_view(
        &mut self,
        create_start: u32,
    ) -> Result<CreateMaterializedViewStmt, ParseError> {
        self.expect_identifier_word("materialized", "MATERIALIZED after CREATE")?;
        self.expect_identifier_word("view", "VIEW after CREATE MATERIALIZED")?;

        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_object_name()?;
        let columns = if self.peek()?.kind == TokenKind::LParen {
            self.parse_ident_list_paren()?
        } else {
            Vec::new()
        };
        self.expect(TokenKind::KwAs, "AS")?;
        let source = Box::new(self.parse_select()?);
        let end = self.peek()?.span.start;

        Ok(CreateMaterializedViewStmt {
            if_not_exists,
            name,
            columns,
            source,
            span: Span::new(create_start, end),
        })
    }

    fn expect_identifier_word(
        &mut self,
        word: &'static str,
        expected: &'static str,
    ) -> Result<Token, ParseError> {
        let tok = *self.peek()?;
        let matches = tok.kind == TokenKind::Identifier
            && tok
                .text(self.source)
                .is_some_and(|text| text.eq_ignore_ascii_case(word));
        if matches {
            self.advance()
        } else {
            Err(ParseError::Expected {
                expected,
                found: tok.kind,
                offset: tok.span.start as usize,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Statement;

    fn parse_create_materialized_view(src: &str) -> CreateMaterializedViewStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::CreateMaterializedView(s) => *s,
            other => panic!("expected CreateMaterializedView, got {other:?}"),
        }
    }

    #[test]
    fn create_materialized_view_parses_source_query() {
        let stmt = parse_create_materialized_view(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_copy (x, y) AS \
             SELECT id, amount FROM mv_src",
        );
        assert!(stmt.if_not_exists);
        assert_eq!(stmt.name.to_string(), "mv_copy");
        assert_eq!(stmt.columns.len(), 2);
        assert_eq!(stmt.columns[0].value, "x");
        assert_eq!(stmt.columns[1].value, "y");
    }
}
