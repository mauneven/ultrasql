//! Parser methods for `CREATE SCHEMA` and `DROP SCHEMA` statements.
//!
//! Handles:
//! - `CREATE SCHEMA name`
//! - `CREATE SCHEMA IF NOT EXISTS name`
//! - `DROP SCHEMA name`
//! - `DROP SCHEMA IF EXISTS name [, …] [CASCADE|RESTRICT]`

use crate::ast::{CreateSchemaStmt, DropSchemaStmt};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse `CREATE SCHEMA …`, consuming the `SCHEMA` keyword.
    ///
    /// The `CREATE` keyword must already have been consumed by the caller.
    pub(crate) fn parse_create_schema(
        &mut self,
        create_start: u32,
    ) -> Result<CreateSchemaStmt, ParseError> {
        self.expect(TokenKind::KwSchema, "SCHEMA")?;
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_identifier()?;
        let end = self.peek()?.span.start;
        Ok(CreateSchemaStmt {
            if_not_exists,
            name,
            span: Span::new(create_start, end),
        })
    }

    /// Parse `DROP SCHEMA …`, consuming the `SCHEMA` keyword.
    ///
    /// The `DROP` keyword must already have been consumed by the caller.
    pub(crate) fn parse_drop_schema(
        &mut self,
        drop_start: u32,
    ) -> Result<DropSchemaStmt, ParseError> {
        self.expect(TokenKind::KwSchema, "SCHEMA")?;
        let if_exists = self.parse_if_exists()?;
        // Parse one or more schema names
        let mut names = Vec::new();
        loop {
            names.push(self.parse_identifier()?);
            if self.peek()?.kind == TokenKind::Comma {
                self.advance()?;
            } else {
                break;
            }
        }
        let cascade = self.parse_cascade_restrict();
        let end = self.peek()?.span.start;
        Ok(DropSchemaStmt {
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

    fn parse_create_schema(src: &str) -> CreateSchemaStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::CreateSchema(s) => s,
            other => panic!("expected CreateSchema, got {other:?}"),
        }
    }

    fn parse_drop_schema(src: &str) -> DropSchemaStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::DropSchema(s) => s,
            other => panic!("expected DropSchema, got {other:?}"),
        }
    }

    // ---- happy-path -------------------------------------------------------

    #[test]
    fn create_schema_basic() {
        let stmt = parse_create_schema("CREATE SCHEMA myschema");
        assert!(!stmt.if_not_exists);
        assert_eq!(stmt.name.value, "myschema");
    }

    #[test]
    fn create_schema_if_not_exists() {
        let stmt = parse_create_schema("CREATE SCHEMA IF NOT EXISTS myschema");
        assert!(stmt.if_not_exists);
        assert_eq!(stmt.name.value, "myschema");
    }

    #[test]
    fn drop_schema_basic() {
        let stmt = parse_drop_schema("DROP SCHEMA myschema");
        assert!(!stmt.if_exists);
        assert_eq!(stmt.names.len(), 1);
        assert_eq!(stmt.names[0].value, "myschema");
        assert!(!stmt.cascade);
    }

    #[test]
    fn drop_schema_if_exists_cascade() {
        let stmt = parse_drop_schema("DROP SCHEMA IF EXISTS s1, s2 CASCADE");
        assert!(stmt.if_exists);
        assert_eq!(stmt.names.len(), 2);
        assert!(stmt.cascade);
    }

    // ---- negative case ----------------------------------------------------

    #[test]
    fn create_schema_missing_name_errors() {
        let err = Parser::new("CREATE SCHEMA").parse_statement().unwrap_err();
        assert!(matches!(
            err,
            ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
        ));
    }
}
