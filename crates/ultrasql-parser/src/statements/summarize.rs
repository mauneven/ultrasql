//! Parser methods for `SUMMARIZE` statements.

use crate::ast::SummarizeStmt;
use crate::parser::{ParseError, Parser};
use crate::span::Span;

impl Parser<'_> {
    /// Parse `SUMMARIZE table_name`.
    ///
    /// `SUMMARIZE` is accepted as an identifier-shaped command word so
    /// non-command identifier use remains available.
    pub(crate) fn parse_summarize(&mut self) -> Result<SummarizeStmt, ParseError> {
        let summarize = self.expect_identifier_keyword("summarize", "SUMMARIZE")?;
        let name = self.parse_object_name()?;
        let end = self.peek()?.span.start;
        Ok(SummarizeStmt {
            name,
            span: Span::new(summarize.span.start, end),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Statement;

    fn parse_summarize(src: &str) -> SummarizeStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::Summarize(s) => s,
            other => panic!("expected Summarize, got {other:?}"),
        }
    }

    #[test]
    fn summarize_table_object() {
        let stmt = parse_summarize("SUMMARIZE public.users");
        assert_eq!(stmt.name.to_string(), "public.users");
    }

    #[test]
    fn summarize_without_target_errors() {
        let err = Parser::new("SUMMARIZE").parse_statement().unwrap_err();
        assert!(matches!(
            err,
            ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
        ));
    }
}
