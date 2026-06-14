//! Parser methods for `DESCRIBE` statements.

use crate::ast::{DescribeObjectKind, DescribeStmt, DescribeTarget};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse `DESCRIBE [TABLE|VIEW] name` or `DESCRIBE SELECT ...`.
    ///
    /// `DESCRIBE` is accepted as an identifier-shaped command word so existing
    /// non-command uses of `describe` remain available.
    pub(crate) fn parse_describe(&mut self) -> Result<DescribeStmt, ParseError> {
        let describe = self.expect_identifier_keyword("describe", "DESCRIBE")?;
        let start = describe.span.start;

        let tok = *self.peek()?;
        let target = match tok.kind {
            TokenKind::KwSelect | TokenKind::KwWith => {
                DescribeTarget::Query(Box::new(self.parse_select()?))
            }
            TokenKind::KwTable => {
                self.advance()?; // TABLE
                let name = self.parse_object_name()?;
                DescribeTarget::Object {
                    kind: DescribeObjectKind::Table,
                    name,
                }
            }
            TokenKind::Identifier
                if tok
                    .text(self.source)
                    .is_some_and(|text| text.eq_ignore_ascii_case("view")) =>
            {
                self.advance()?; // VIEW
                let name = self.parse_object_name()?;
                DescribeTarget::Object {
                    kind: DescribeObjectKind::View,
                    name,
                }
            }
            _ => {
                let name = self.parse_object_name()?;
                DescribeTarget::Object {
                    kind: DescribeObjectKind::Any,
                    name,
                }
            }
        };

        let end = self.peek()?.span.start;
        Ok(DescribeStmt {
            target,
            span: Span::new(start, end),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Statement;
    use crate::parser::Parser;

    fn parse_describe(src: &str) -> DescribeStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::Describe(s) => s,
            other => panic!("expected Describe, got {other:?}"),
        }
    }

    #[test]
    fn describe_table_object() {
        let stmt = parse_describe("DESCRIBE TABLE public.users");
        let DescribeTarget::Object { kind, name } = stmt.target else {
            panic!("expected object target");
        };
        assert_eq!(kind, DescribeObjectKind::Table);
        assert_eq!(name.to_string(), "public.users");
    }

    #[test]
    fn describe_view_object() {
        let stmt = parse_describe("DESCRIBE VIEW active_users");
        let DescribeTarget::Object { kind, name } = stmt.target else {
            panic!("expected object target");
        };
        assert_eq!(kind, DescribeObjectKind::View);
        assert_eq!(name.to_string(), "active_users");
    }

    #[test]
    fn describe_unqualified_object() {
        let stmt = parse_describe("DESCRIBE users");
        let DescribeTarget::Object { kind, name } = stmt.target else {
            panic!("expected object target");
        };
        assert_eq!(kind, DescribeObjectKind::Any);
        assert_eq!(name.to_string(), "users");
    }

    #[test]
    fn describe_select_query() {
        let stmt = parse_describe("DESCRIBE SELECT 1 AS answer");
        let DescribeTarget::Query(query) = stmt.target else {
            panic!("expected query target");
        };
        assert_eq!(query.projection.len(), 1);
    }

    #[test]
    fn describe_without_target_errors() {
        let err = Parser::new("DESCRIBE").parse_statement().unwrap_err();
        assert!(matches!(
            err,
            ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
        ));
    }
}
