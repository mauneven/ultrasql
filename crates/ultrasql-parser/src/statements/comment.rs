//! Parser methods for `COMMENT ON` statements.

use crate::ast::{CommentStmt, CommentTarget, Statement};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse `COMMENT ON TABLE/INDEX/COLUMN name IS 'text'`.
    pub(crate) fn parse_comment(&mut self) -> Result<Statement, ParseError> {
        let start = self.expect(TokenKind::KwComment, "COMMENT")?.span.start;
        self.expect(TokenKind::KwOn, "ON")?;
        let target_tok = *self.peek()?;
        let target = match target_tok.kind {
            TokenKind::KwTable => {
                self.advance()?;
                CommentTarget::Table(self.parse_object_name()?)
            }
            TokenKind::KwIndex => {
                self.advance()?;
                CommentTarget::Index(self.parse_object_name()?)
            }
            TokenKind::KwColumn => {
                self.advance()?;
                CommentTarget::Column(self.parse_object_name()?)
            }
            other => {
                return Err(ParseError::Expected {
                    expected: "TABLE, INDEX, or COLUMN after COMMENT ON",
                    found: other,
                    offset: target_tok.span.start_usize(),
                });
            }
        };
        self.expect(TokenKind::KwIs, "IS")?;
        let (comment, end) = self.parse_comment_value()?;
        Ok(Statement::Comment(CommentStmt {
            target,
            comment,
            span: Span::new(start, end),
        }))
    }

    fn parse_comment_value(&mut self) -> Result<(Option<String>, u32), ParseError> {
        let tok = *self.peek()?;
        match tok.kind {
            TokenKind::KwNull => {
                let tok = self.advance()?;
                Ok((None, tok.span.end))
            }
            TokenKind::String => {
                let tok = self.advance()?;
                let raw = tok.text(self.source).unwrap_or("");
                let value = if raw.len() >= 2 {
                    raw[1..raw.len() - 1].replace("''", "'")
                } else {
                    String::new()
                };
                Ok((Some(value), tok.span.end))
            }
            TokenKind::EscapedString | TokenKind::DollarString => {
                let tok = self.advance()?;
                Ok((
                    Some(tok.text(self.source).unwrap_or("").to_owned()),
                    tok.span.end,
                ))
            }
            other => Err(ParseError::Expected {
                expected: "string literal or NULL after COMMENT ... IS",
                found: other,
                offset: tok.span.start_usize(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::{CommentTarget, Statement};
    use crate::parser::Parser;

    #[test]
    fn parses_comment_on_table() {
        let mut parser = Parser::new("COMMENT ON TABLE public.t IS 'hello'");
        let stmt = parser.parse_statement().expect("comment parses");
        let Statement::Comment(comment) = stmt else {
            panic!("expected COMMENT");
        };
        assert_eq!(comment.comment.as_deref(), Some("hello"));
        assert!(matches!(comment.target, CommentTarget::Table(_)));
    }

    #[test]
    fn parses_comment_on_column_null() {
        let mut parser = Parser::new("COMMENT ON COLUMN t.c IS NULL");
        let stmt = parser.parse_statement().expect("comment parses");
        let Statement::Comment(comment) = stmt else {
            panic!("expected COMMENT");
        };
        assert!(comment.comment.is_none());
        assert!(matches!(comment.target, CommentTarget::Column(_)));
    }

    #[test]
    fn parses_comment_on_index() {
        let mut parser = Parser::new("COMMENT ON INDEX t_idx IS 'index docs'");
        let stmt = parser.parse_statement().expect("comment parses");
        let Statement::Comment(comment) = stmt else {
            panic!("expected COMMENT");
        };
        assert_eq!(comment.comment.as_deref(), Some("index docs"));
        assert!(matches!(comment.target, CommentTarget::Index(_)));
    }
}
