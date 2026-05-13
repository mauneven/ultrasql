//! Parser methods for `SET`, `SHOW`, and `RESET` statements.
//!
//! Handles:
//! - `SET var = expr [, expr …]`
//! - `SET var TO expr [, expr …]`
//! - `SET SESSION var = expr`
//! - `SET LOCAL var = expr`
//! - `SHOW var`
//! - `RESET var`

use crate::ast::{SetScope, SetValue, SetVarStmt};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse a `SET`, `SHOW`, or `RESET` statement.
    ///
    /// The leading keyword (`SET`, `SHOW`, or `RESET`) has been peeked but
    /// not consumed. The `start` byte offset must be supplied by the caller.
    pub(crate) fn parse_set_stmt(&mut self) -> Result<SetVarStmt, ParseError> {
        let tok = self.advance()?;
        let start = tok.span.start;

        match tok.kind {
            TokenKind::KwShow => {
                let name = self.parse_identifier()?;
                let end = self.peek()?.span.start;
                return Ok(SetVarStmt {
                    scope: SetScope::Show,
                    name,
                    value: SetValue::Default,
                    span: Span::new(start, end),
                });
            }
            TokenKind::KwReset => {
                let name = self.parse_identifier()?;
                let end = self.peek()?.span.start;
                return Ok(SetVarStmt {
                    scope: SetScope::Reset,
                    name,
                    value: SetValue::Default,
                    span: Span::new(start, end),
                });
            }
            _ => {}
        }

        // SET [SESSION | LOCAL] var [= | TO] value
        let scope = if self.peek()?.kind == TokenKind::KwSession {
            self.advance()?;
            SetScope::Session
        } else if self.peek()?.kind == TokenKind::KwLocal {
            self.advance()?;
            SetScope::Local
        } else {
            SetScope::Session
        };

        let name = self.parse_identifier()?;

        // `=` or `TO`
        match self.peek()?.kind {
            TokenKind::Eq | TokenKind::KwTo => {
                self.advance()?;
            }
            other => {
                return Err(ParseError::Expected {
                    expected: "'=' or 'TO'",
                    found: other,
                    offset: self.peek()?.span.start as usize,
                });
            }
        }

        // DEFAULT or expression list
        let value = if self.peek()?.kind == TokenKind::KwDefault {
            self.advance()?;
            SetValue::Default
        } else {
            let mut exprs = Vec::new();
            loop {
                exprs.push(self.parse_expr()?);
                if self.peek()?.kind == TokenKind::Comma {
                    self.advance()?;
                } else {
                    break;
                }
            }
            SetValue::Values(exprs)
        };

        let end = self.peek()?.span.start;
        Ok(SetVarStmt {
            scope,
            name,
            value,
            span: Span::new(start, end),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{SetScope, SetValue, Statement};
    use crate::parser::Parser;

    fn parse_set(src: &str) -> SetVarStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::SetVar(s) => s,
            other => panic!("expected SetVar, got {other:?}"),
        }
    }

    // ---- happy-path -------------------------------------------------------

    #[test]
    fn set_session_eq() {
        let stmt = parse_set("SET statement_timeout = 5000");
        assert_eq!(stmt.scope, SetScope::Session);
        assert_eq!(stmt.name.value, "statement_timeout");
        assert!(matches!(stmt.value, SetValue::Values(_)));
    }

    #[test]
    fn set_search_path_to_multiple() {
        let stmt = parse_set("SET search_path TO myschema, public");
        assert_eq!(stmt.scope, SetScope::Session);
        assert_eq!(stmt.name.value, "search_path");
        let SetValue::Values(vals) = &stmt.value else {
            panic!("expected Values")
        };
        assert_eq!(vals.len(), 2);
    }

    #[test]
    fn set_local() {
        let stmt = parse_set("SET LOCAL work_mem = '64MB'");
        assert_eq!(stmt.scope, SetScope::Local);
    }

    #[test]
    fn show_var() {
        let stmt = parse_set("SHOW search_path");
        assert_eq!(stmt.scope, SetScope::Show);
        assert_eq!(stmt.name.value, "search_path");
        assert!(matches!(stmt.value, SetValue::Default));
    }

    #[test]
    fn reset_var() {
        let stmt = parse_set("RESET search_path");
        assert_eq!(stmt.scope, SetScope::Reset);
    }

    #[test]
    fn set_default() {
        let stmt = parse_set("SET work_mem = DEFAULT");
        assert!(matches!(stmt.value, SetValue::Default));
    }

    // ---- negative case ----------------------------------------------------

    #[test]
    fn set_missing_value_errors() {
        let err = Parser::new("SET x").parse_statement().unwrap_err();
        assert!(matches!(
            err,
            ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
        ));
    }
}
