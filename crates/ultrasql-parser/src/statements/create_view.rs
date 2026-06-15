//! Parser methods for `CREATE VIEW`, `CREATE MATERIALIZED VIEW`, and
//! `ALTER VIEW`.

use crate::ast::{AlterViewAction, AlterViewStmt, CreateMaterializedViewStmt, CreateViewStmt};
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

    /// Parse `CREATE [OR REPLACE] VIEW … AS SELECT …`.
    pub(crate) fn parse_create_view(
        &mut self,
        create_start: u32,
        or_replace: bool,
    ) -> Result<CreateViewStmt, ParseError> {
        self.expect_identifier_word("view", "VIEW after CREATE")?;
        let name = self.parse_object_name()?;
        let columns = if self.peek()?.kind == TokenKind::LParen {
            self.parse_ident_list_paren()?
        } else {
            Vec::new()
        };
        self.expect(TokenKind::KwAs, "AS")?;
        let source_start = self.peek()?.span.start_usize();
        let source = Box::new(self.parse_select()?);
        let end = self.peek()?.span.start;
        let source_end = usize::try_from(end).unwrap_or(self.source.len());
        let source_sql = self.source[source_start..source_end].trim().to_owned();

        Ok(CreateViewStmt {
            or_replace,
            name,
            columns,
            source,
            source_sql,
            span: Span::new(create_start, end),
        })
    }

    /// Parse `ALTER VIEW …`.
    pub(crate) fn parse_alter_view(
        &mut self,
        alter_start: u32,
    ) -> Result<AlterViewStmt, ParseError> {
        self.expect_identifier_word("view", "VIEW after ALTER")?;
        let name = self.parse_object_name()?;
        let action = self.parse_alter_view_action()?;
        let end = self.peek()?.span.start;
        Ok(AlterViewStmt {
            name,
            action,
            span: Span::new(alter_start, end),
        })
    }

    fn parse_alter_view_action(&mut self) -> Result<AlterViewAction, ParseError> {
        let tok = *self.peek()?;
        let start = tok.span.start;
        match tok.kind {
            TokenKind::KwRename => {
                self.advance()?; // RENAME
                self.expect(TokenKind::KwTo, "TO")?;
                let new_name = self.parse_identifier()?;
                let end = self.peek()?.span.start;
                Ok(AlterViewAction::RenameView {
                    new_name,
                    span: Span::new(start, end),
                })
            }
            TokenKind::KwSet => {
                self.advance()?; // SET
                self.expect(TokenKind::KwSchema, "SCHEMA")?;
                let schema_name = self.parse_identifier()?;
                let end = self.peek()?.span.start;
                Ok(AlterViewAction::SetSchema {
                    schema_name,
                    span: Span::new(start, end),
                })
            }
            TokenKind::KwAs => {
                self.advance()?; // AS
                let source_start = self.peek()?.span.start_usize();
                let source = Box::new(self.parse_select()?);
                let end = self.peek()?.span.start;
                let source_end = usize::try_from(end).unwrap_or(self.source.len());
                let source_sql = self.source[source_start..source_end].trim().to_owned();
                Ok(AlterViewAction::ReplaceDefinition {
                    source,
                    source_sql,
                    span: Span::new(start, end),
                })
            }
            other => Err(ParseError::Expected {
                expected: "RENAME, SET SCHEMA, or AS",
                found: other,
                offset: tok.span.start_usize(),
            }),
        }
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
                offset: tok.span.start_usize(),
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

    #[test]
    fn create_view_parses_source_query() {
        let Statement::CreateView(stmt) =
            Parser::new("CREATE VIEW v_copy (x, y) AS SELECT id, amount FROM src")
                .parse_statement()
                .expect("CREATE VIEW should parse")
        else {
            panic!("expected CreateView")
        };
        assert!(!stmt.or_replace);
        assert_eq!(stmt.name.to_string(), "v_copy");
        assert_eq!(stmt.columns.len(), 2);
        assert_eq!(stmt.source_sql, "SELECT id, amount FROM src");
    }

    #[test]
    fn create_or_replace_view_parses_source_query() {
        let Statement::CreateView(stmt) =
            Parser::new("CREATE OR REPLACE VIEW v_copy AS SELECT id FROM src")
                .parse_statement()
                .expect("CREATE OR REPLACE VIEW should parse")
        else {
            panic!("expected CreateView")
        };
        assert!(stmt.or_replace);
        assert_eq!(stmt.source_sql, "SELECT id FROM src");
    }

    #[test]
    fn alter_view_rename_and_set_schema_parse() {
        let Statement::AlterView(rename) = Parser::new("ALTER VIEW old_v RENAME TO new_v")
            .parse_statement()
            .expect("ALTER VIEW RENAME should parse")
        else {
            panic!("expected AlterView")
        };
        assert_eq!(rename.name.to_string(), "old_v");
        let AlterViewAction::RenameView { new_name, .. } = rename.action else {
            panic!("expected RenameView")
        };
        assert_eq!(new_name.value, "new_v");

        let Statement::AlterView(set_schema) = Parser::new("ALTER VIEW old_v SET SCHEMA app")
            .parse_statement()
            .expect("ALTER VIEW SET SCHEMA should parse")
        else {
            panic!("expected AlterView")
        };
        let AlterViewAction::SetSchema { schema_name, .. } = set_schema.action else {
            panic!("expected SetSchema")
        };
        assert_eq!(schema_name.value, "app");
    }
}
