//! Parser methods for `SET`, `SHOW`, and `RESET` statements.
//!
//! Handles:
//! - `SET var = expr [, expr …]`
//! - `SET var TO expr [, expr …]`
//! - `SET SESSION var = expr`
//! - `SET LOCAL var = expr`
//! - `SHOW var`
//! - `RESET var`

use crate::ast::{Expr, Identifier, ObjectName, SetRoleStmt, SetScope, SetValue, SetVarStmt};
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
                let name = self.parse_set_name()?;
                let end = self.peek()?.span.start;
                return Ok(SetVarStmt {
                    scope: SetScope::Show,
                    name,
                    value: SetValue::Default,
                    span: Span::new(start, end),
                });
            }
            TokenKind::KwReset => {
                let name = self.parse_set_name()?;
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

        let name = self.parse_set_name()?;

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
                if self.peek()?.kind == TokenKind::KwOn {
                    let tok = self.advance()?;
                    exprs.push(Expr::Column {
                        name: ObjectName {
                            parts: vec![Identifier {
                                value: "on".to_owned(),
                                quoted: false,
                                span: tok.span,
                            }],
                            span: tok.span,
                        },
                    });
                } else {
                    exprs.push(self.parse_expr()?);
                }
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

    /// Parse `SET ROLE role` / `SET ROLE NONE` / `RESET ROLE`.
    pub(crate) fn parse_set_role(&mut self) -> Result<SetRoleStmt, ParseError> {
        let tok = self.advance()?;
        let start = tok.span.start;
        self.expect_identifier_keyword("role", "ROLE")?;
        if tok.kind == TokenKind::KwReset {
            let end = self.peek()?.span.start;
            return Ok(SetRoleStmt {
                role: None,
                span: Span::new(start, end),
            });
        }
        let next = *self.peek()?;
        let role = match next.kind {
            TokenKind::KwDefault => {
                self.advance()?;
                None
            }
            TokenKind::Identifier
                if next
                    .text(self.source)
                    .is_some_and(|text| text.eq_ignore_ascii_case("none")) =>
            {
                self.advance()?;
                None
            }
            _ => Some(self.parse_identifier()?),
        };
        let end = self.peek()?.span.start;
        Ok(SetRoleStmt {
            role,
            span: Span::new(start, end),
        })
    }

    fn parse_set_name(&mut self) -> Result<Identifier, ParseError> {
        let first = self.parse_identifier()?;
        let mut value = first.value.clone();
        let mut end = first.span.end;
        while self.peek()?.kind == TokenKind::Dot {
            self.advance()?; // dot
            let next = self.parse_identifier()?;
            value.push('.');
            value.push_str(&next.value);
            end = next.span.end;
        }
        Ok(Identifier {
            value,
            quoted: first.quoted,
            span: Span::new(first.span.start, end),
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

    fn parse_set_role(src: &str) -> SetRoleStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::SetRole(s) => s,
            other => panic!("expected SetRole, got {other:?}"),
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
    fn set_role_parses() {
        let stmt = parse_set_role("SET ROLE support");
        assert_eq!(stmt.role.expect("role").value, "support");
    }

    #[test]
    fn set_role_none_and_reset_role_parse_as_reset() {
        assert!(parse_set_role("SET ROLE NONE").role.is_none());
        assert!(parse_set_role("RESET ROLE").role.is_none());
    }

    #[test]
    fn set_default() {
        let stmt = parse_set("SET work_mem = DEFAULT");
        assert!(matches!(stmt.value, SetValue::Default));
    }

    #[test]
    fn set_keyword_on_value() {
        let stmt = parse_set("SET jit = on");
        let SetValue::Values(vals) = &stmt.value else {
            panic!("expected Values")
        };
        assert_eq!(vals.len(), 1);
        assert!(matches!(&vals[0], Expr::Column { name } if name.to_string() == "on"));
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
