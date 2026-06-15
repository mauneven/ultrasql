//! Parser methods for database portability/admin statements.

use crate::ast::{ExportDatabaseStmt, ImportDatabaseStmt};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse `EXPORT DATABASE TO 'path'`.
    ///
    /// `EXPORT` is accepted as an identifier-shaped command word so existing
    /// non-command uses remain available.
    pub(crate) fn parse_export_database(&mut self) -> Result<ExportDatabaseStmt, ParseError> {
        let export = self.expect_identifier_keyword("export", "EXPORT")?;
        let start = export.span.start;
        self.expect_admin_word("database", "DATABASE after EXPORT")?;
        self.expect_admin_word("to", "TO after EXPORT DATABASE")?;
        let (path, end) = self.parse_admin_path_literal("EXPORT DATABASE path")?;
        Ok(ExportDatabaseStmt {
            path,
            span: Span::new(start, end),
        })
    }

    /// Parse `IMPORT DATABASE FROM 'path'`.
    ///
    /// `IMPORT` is accepted as an identifier-shaped command word so existing
    /// non-command uses remain available.
    pub(crate) fn parse_import_database(&mut self) -> Result<ImportDatabaseStmt, ParseError> {
        let import = self.expect_identifier_keyword("import", "IMPORT")?;
        let start = import.span.start;
        self.expect_admin_word("database", "DATABASE after IMPORT")?;
        self.expect_admin_word("from", "FROM after IMPORT DATABASE")?;
        let (path, end) = self.parse_admin_path_literal("IMPORT DATABASE path")?;
        Ok(ImportDatabaseStmt {
            path,
            span: Span::new(start, end),
        })
    }

    fn parse_admin_path_literal(
        &mut self,
        expected: &'static str,
    ) -> Result<(String, u32), ParseError> {
        let tok = *self.peek()?;
        match tok.kind {
            TokenKind::String => {
                let tok = self.advance()?;
                let raw = tok.text(self.source).unwrap_or("''");
                let inner = if raw.len() >= 2 {
                    &raw[1..raw.len() - 1]
                } else {
                    ""
                };
                Ok((inner.replace("''", "'"), tok.span.end))
            }
            other => Err(ParseError::Expected {
                expected,
                found: other,
                offset: tok.span.start_usize(),
            }),
        }
    }

    fn expect_admin_word(
        &mut self,
        word: &'static str,
        expected: &'static str,
    ) -> Result<(), ParseError> {
        let tok = *self.peek()?;
        let matches = tok
            .text(self.source)
            .is_some_and(|text| text.eq_ignore_ascii_case(word));
        if matches {
            self.advance()?;
            Ok(())
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

    #[test]
    fn export_database_parses_path() {
        let stmt = Parser::new("EXPORT DATABASE TO '/tmp/ultra-dump'")
            .parse_statement()
            .expect("export parses");
        let Statement::ExportDatabase(stmt) = stmt else {
            panic!("expected ExportDatabase");
        };
        assert_eq!(stmt.path, "/tmp/ultra-dump");
    }

    #[test]
    fn import_database_parses_path() {
        let stmt = Parser::new("IMPORT DATABASE FROM '/tmp/ultra-dump'")
            .parse_statement()
            .expect("import parses");
        let Statement::ImportDatabase(stmt) = stmt else {
            panic!("expected ImportDatabase");
        };
        assert_eq!(stmt.path, "/tmp/ultra-dump");
    }

    #[test]
    fn export_database_requires_database_keyword() {
        let err = Parser::new("EXPORT TABLE TO '/tmp/dump'")
            .parse_statement()
            .expect_err("EXPORT TABLE is unsupported");
        assert!(matches!(err, ParseError::Expected { .. }));
    }

    #[test]
    fn import_database_requires_string_path() {
        let err = Parser::new("IMPORT DATABASE FROM ident")
            .parse_statement()
            .expect_err("path must be string");
        assert!(matches!(err, ParseError::Expected { .. }));
    }
}
