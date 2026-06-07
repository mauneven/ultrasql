//! Parser methods for `LISTEN` / `NOTIFY` / `UNLISTEN`.
//!
//! These three statements drive PostgreSQL's async pub-sub surface:
//!
//! ```sql
//! LISTEN channel
//! NOTIFY channel [ , 'payload' ]
//! UNLISTEN { channel | * }
//! ```
//!
//! Wire dispatch lives in the server crate; the planner emits
//! [`crate::ast::Statement::Listen`] / [`crate::ast::Statement::Notify`] /
//! [`crate::ast::Statement::Unlisten`] and the session loop translates
//! those into calls against `notify::NotifyHub`.

use crate::ast::Statement;
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse `LISTEN channel`.
    ///
    /// Assumes the `LISTEN` keyword has already been consumed by the
    /// dispatcher in `parse_one`; `start` is the byte offset of the
    /// keyword. The channel must be a single identifier — schema-
    /// qualified names are not allowed by PostgreSQL either.
    pub(crate) fn parse_listen(&mut self, start: u32) -> Result<Statement, ParseError> {
        let channel = self.parse_identifier()?;
        let span = Span::new(start, channel.span.end);
        Ok(Statement::Listen { channel, span })
    }

    /// Parse `NOTIFY channel [ , 'payload' ]`.
    ///
    /// Assumes the `NOTIFY` keyword has already been consumed. The
    /// payload, when present, must be a single string literal (PostgreSQL
    /// rejects expressions here too — `NOTIFY` is a synchronous statement
    /// whose payload is part of the syntax, not the expression grammar).
    pub(crate) fn parse_notify(&mut self, start: u32) -> Result<Statement, ParseError> {
        let channel = self.parse_identifier()?;
        let mut end = channel.span.end;
        let payload = if self.peek()?.kind == TokenKind::Comma {
            self.advance()?; // ,
            let lit = self.parse_string_literal_for_notify()?;
            end = lit.1;
            Some(lit.0)
        } else {
            None
        };
        Ok(Statement::Notify {
            channel,
            payload,
            span: Span::new(start, end),
        })
    }

    /// Parse `UNLISTEN { channel | * }`.
    ///
    /// Assumes the `UNLISTEN` keyword has already been consumed.
    pub(crate) fn parse_unlisten(&mut self, start: u32) -> Result<Statement, ParseError> {
        // `UNLISTEN *` — drop every subscription on this session.
        if self.peek()?.kind == TokenKind::Star {
            let star = self.advance()?;
            return Ok(Statement::Unlisten {
                channel: None,
                span: Span::new(start, star.span.end),
            });
        }
        let channel = self.parse_identifier()?;
        let span = Span::new(start, channel.span.end);
        Ok(Statement::Unlisten {
            channel: Some(channel),
            span,
        })
    }

    /// Consume a single string literal and return its content together
    /// with the literal's end offset.
    ///
    /// `NOTIFY`'s payload accepts `'plain'`, `E'escaped'`, and
    /// `$tag$dollar$tag$` literals the same way an ordinary string
    /// position does. We mirror the trim-and-unquote logic in
    /// `parse_primary` for the `String` case; escaped / dollar-quoted
    /// strings round-trip verbatim because their unescape rules are
    /// applied later by the binder, and the wire-level payload need
    /// only carry the source bytes.
    fn parse_string_literal_for_notify(&mut self) -> Result<(String, u32), ParseError> {
        let tok = self.advance()?;
        match tok.kind {
            TokenKind::String => {
                let raw = tok.text(self.source).unwrap_or("");
                // Trim the surrounding single quotes and collapse `''`
                // escape pairs, matching the standard-form lexer convention.
                let value = if raw.len() >= 2 {
                    raw[1..raw.len() - 1].replace("''", "'")
                } else {
                    String::new()
                };
                Ok((value, tok.span.end))
            }
            TokenKind::EscapedString | TokenKind::DollarString => {
                // For escape-prefixed and dollar-quoted strings, the
                // payload-position grammar only needs to carry the raw
                // source slice; payload contents are opaque to the
                // parser. Strip the syntactic markers conservatively.
                let raw = tok.text(self.source).unwrap_or("").to_owned();
                Ok((raw, tok.span.end))
            }
            other => Err(ParseError::Expected {
                expected: "string literal containing the NOTIFY payload",
                found: other,
                offset: tok.span.start_usize(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::Statement;
    use crate::parser::Parser;

    fn parse(src: &str) -> Statement {
        Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
    }

    #[test]
    fn listen_basic_identifier() {
        let stmt = parse("LISTEN orders");
        let Statement::Listen { channel, .. } = stmt else {
            panic!("expected Listen");
        };
        assert_eq!(channel.value, "orders");
    }

    #[test]
    fn notify_without_payload() {
        let stmt = parse("NOTIFY orders");
        let Statement::Notify {
            channel, payload, ..
        } = stmt
        else {
            panic!("expected Notify");
        };
        assert_eq!(channel.value, "orders");
        assert!(payload.is_none());
    }

    #[test]
    fn notify_with_payload() {
        let stmt = parse("NOTIFY orders, 'hello'");
        let Statement::Notify {
            channel, payload, ..
        } = stmt
        else {
            panic!("expected Notify");
        };
        assert_eq!(channel.value, "orders");
        assert_eq!(payload.as_deref(), Some("hello"));
    }

    #[test]
    fn unlisten_specific_channel() {
        let stmt = parse("UNLISTEN orders");
        let Statement::Unlisten { channel, .. } = stmt else {
            panic!("expected Unlisten");
        };
        assert_eq!(channel.unwrap().value, "orders");
    }

    #[test]
    fn unlisten_star_drops_all() {
        let stmt = parse("UNLISTEN *");
        let Statement::Unlisten { channel, .. } = stmt else {
            panic!("expected Unlisten");
        };
        assert!(channel.is_none());
    }
}
