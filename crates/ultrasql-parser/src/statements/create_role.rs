//! Parser methods for role-management DDL.
//!
//! Handles:
//! - `CREATE ROLE [IF NOT EXISTS] name [WITH] [options]`
//! - `CREATE USER [IF NOT EXISTS] name [WITH] [options]`
//! - `ALTER ROLE name [WITH] [options]`
//! - `ALTER USER name [WITH] [options]`
//! - `DROP ROLE [IF EXISTS] name [, ...] [CASCADE|RESTRICT]`
//! - `DROP USER [IF EXISTS] name [, ...] [CASCADE|RESTRICT]`

use crate::ast::{
    AlterRoleStmt, CreateRoleStmt, DropRoleStmt, Expr, Literal, RoleOption, RoleStmtKind,
};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse `CREATE ROLE …` / `CREATE USER …`.
    ///
    /// The `CREATE` keyword must already have been consumed by the caller.
    pub(crate) fn parse_create_role(
        &mut self,
        create_start: u32,
        kind: RoleStmtKind,
    ) -> Result<CreateRoleStmt, ParseError> {
        self.expect_role_kind(kind)?;
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_identifier()?;
        self.parse_optional_with()?;
        let options = self.parse_role_options()?;
        let end = self.peek()?.span.start;
        Ok(CreateRoleStmt {
            kind,
            if_not_exists,
            name,
            options,
            span: Span::new(create_start, end),
        })
    }

    /// Parse `ALTER ROLE …` / `ALTER USER …`.
    ///
    /// The `ALTER` keyword must already have been consumed by the caller.
    pub(crate) fn parse_alter_role(
        &mut self,
        alter_start: u32,
        kind: RoleStmtKind,
    ) -> Result<AlterRoleStmt, ParseError> {
        self.expect_role_kind(kind)?;
        let name = self.parse_identifier()?;
        self.parse_optional_with()?;
        let options = self.parse_role_options()?;
        let end = self.peek()?.span.start;
        Ok(AlterRoleStmt {
            kind,
            name,
            options,
            span: Span::new(alter_start, end),
        })
    }

    /// Parse `DROP ROLE …` / `DROP USER …`.
    ///
    /// The `DROP` keyword must already have been consumed by the caller.
    pub(crate) fn parse_drop_role(
        &mut self,
        drop_start: u32,
        kind: RoleStmtKind,
    ) -> Result<DropRoleStmt, ParseError> {
        self.expect_role_kind(kind)?;
        let if_exists = self.parse_if_exists()?;
        let mut names = vec![self.parse_identifier()?];
        while self.peek()?.kind == TokenKind::Comma {
            self.advance()?;
            names.push(self.parse_identifier()?);
        }
        let cascade = self.parse_cascade_restrict();
        let end = self.peek()?.span.start;
        Ok(DropRoleStmt {
            kind,
            if_exists,
            names,
            cascade,
            span: Span::new(drop_start, end),
        })
    }

    fn expect_role_kind(&mut self, kind: RoleStmtKind) -> Result<(), ParseError> {
        match kind {
            RoleStmtKind::Role => {
                self.expect_identifier_keyword("role", "ROLE")?;
            }
            RoleStmtKind::User => {
                self.expect_identifier_keyword("user", "USER")?;
            }
        }
        Ok(())
    }

    fn parse_optional_with(&mut self) -> Result<(), ParseError> {
        if self.peek()?.kind == TokenKind::KwWith {
            self.advance()?;
        }
        Ok(())
    }

    fn parse_role_options(&mut self) -> Result<Vec<RoleOption>, ParseError> {
        let mut options = Vec::new();
        loop {
            let tok = *self.peek()?;
            if tok.kind == TokenKind::Semicolon || tok.kind == TokenKind::Eof {
                break;
            }
            if tok.kind == TokenKind::KwNo {
                self.advance()?;
                options.push(self.parse_no_role_option()?);
                continue;
            }
            let Some(text) = tok.text(self.source) else {
                break;
            };
            if tok.kind != TokenKind::Identifier {
                break;
            }
            match text.to_ascii_lowercase().as_str() {
                "superuser" => {
                    self.advance()?;
                    options.push(RoleOption::Superuser(true));
                }
                "nosuperuser" => {
                    self.advance()?;
                    options.push(RoleOption::Superuser(false));
                }
                "inherit" => {
                    self.advance()?;
                    options.push(RoleOption::Inherit(true));
                }
                "noinherit" => {
                    self.advance()?;
                    options.push(RoleOption::Inherit(false));
                }
                "createrole" => {
                    self.advance()?;
                    options.push(RoleOption::CreateRole(true));
                }
                "nocreaterole" => {
                    self.advance()?;
                    options.push(RoleOption::CreateRole(false));
                }
                "createdb" => {
                    self.advance()?;
                    options.push(RoleOption::CreateDb(true));
                }
                "nocreatedb" => {
                    self.advance()?;
                    options.push(RoleOption::CreateDb(false));
                }
                "login" => {
                    self.advance()?;
                    options.push(RoleOption::Login(true));
                }
                "nologin" => {
                    self.advance()?;
                    options.push(RoleOption::Login(false));
                }
                "replication" => {
                    self.advance()?;
                    options.push(RoleOption::Replication(true));
                }
                "noreplication" => {
                    self.advance()?;
                    options.push(RoleOption::Replication(false));
                }
                "bypassrls" => {
                    self.advance()?;
                    options.push(RoleOption::BypassRls(true));
                }
                "nobypassrls" => {
                    self.advance()?;
                    options.push(RoleOption::BypassRls(false));
                }
                "password" => {
                    self.advance()?;
                    options.push(RoleOption::Password(self.parse_optional_password()?));
                }
                "encrypted" | "unencrypted" => {
                    self.advance()?;
                    self.expect_identifier_keyword("password", "PASSWORD")?;
                    options.push(RoleOption::Password(self.parse_optional_password()?));
                }
                "connection" => {
                    self.advance()?;
                    self.expect_identifier_keyword("limit", "LIMIT")?;
                    options.push(RoleOption::ConnectionLimit(self.parse_role_i32()?));
                }
                "valid" => {
                    self.advance()?;
                    self.expect_identifier_keyword("until", "UNTIL")?;
                    options.push(RoleOption::ValidUntil(
                        self.parse_string_literal("VALID UNTIL")?,
                    ));
                }
                _ => break,
            }
        }
        Ok(options)
    }

    fn parse_no_role_option(&mut self) -> Result<RoleOption, ParseError> {
        let tok = *self.peek()?;
        let Some(text) = tok.text(self.source) else {
            return Err(ParseError::Expected {
                expected: "role option after NO",
                found: tok.kind,
                offset: tok.span.start as usize,
            });
        };
        if tok.kind != TokenKind::Identifier {
            return Err(ParseError::Expected {
                expected: "role option after NO",
                found: tok.kind,
                offset: tok.span.start as usize,
            });
        }
        let option = match text.to_ascii_lowercase().as_str() {
            "superuser" => RoleOption::Superuser(false),
            "inherit" => RoleOption::Inherit(false),
            "createrole" => RoleOption::CreateRole(false),
            "createdb" => RoleOption::CreateDb(false),
            "login" => RoleOption::Login(false),
            "replication" => RoleOption::Replication(false),
            "bypassrls" => RoleOption::BypassRls(false),
            _ => {
                return Err(ParseError::Expected {
                    expected: "SUPERUSER, INHERIT, CREATEROLE, CREATEDB, LOGIN, REPLICATION, or BYPASSRLS after NO",
                    found: tok.kind,
                    offset: tok.span.start as usize,
                });
            }
        };
        self.advance()?;
        Ok(option)
    }

    fn parse_optional_password(&mut self) -> Result<Option<String>, ParseError> {
        if self.peek()?.kind == TokenKind::KwNull {
            self.advance()?;
            return Ok(None);
        }
        Ok(Some(self.parse_string_literal("PASSWORD")?))
    }

    fn parse_string_literal(&mut self, expected: &'static str) -> Result<String, ParseError> {
        let expr = self.parse_expr()?;
        match expr {
            Expr::Literal(Literal::String { value, .. }) => Ok(value),
            other => Err(ParseError::Expected {
                expected,
                found: self.peek()?.kind,
                offset: other.span().start as usize,
            }),
        }
    }

    fn parse_role_i32(&mut self) -> Result<i32, ParseError> {
        let negative = self.peek()?.kind == TokenKind::Minus;
        if negative {
            self.advance()?;
        }
        let tok = *self.peek()?;
        if tok.kind != TokenKind::Integer {
            return Err(ParseError::Expected {
                expected: "integer",
                found: tok.kind,
                offset: tok.span.start as usize,
            });
        }
        let token = self.advance()?;
        let text = token.text(self.source).unwrap_or("0");
        let parsed = text
            .parse::<i32>()
            .map_err(|_| ParseError::InvalidInteger {
                text: text.to_owned(),
                offset: token.span.start as usize,
            })?;
        Ok(if negative { -parsed } else { parsed })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Statement;

    fn parse_create(src: &str) -> CreateRoleStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::CreateRole(s) => *s,
            other => panic!("expected CreateRole, got {other:?}"),
        }
    }

    fn parse_alter(src: &str) -> AlterRoleStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::AlterRole(s) => *s,
            other => panic!("expected AlterRole, got {other:?}"),
        }
    }

    #[test]
    fn create_role_options_parse() {
        let stmt = parse_create("CREATE ROLE analytics NOLOGIN CREATEDB CREATEROLE");
        assert_eq!(stmt.kind, RoleStmtKind::Role);
        assert_eq!(stmt.name.value, "analytics");
        assert_eq!(
            stmt.options,
            vec![
                RoleOption::Login(false),
                RoleOption::CreateDb(true),
                RoleOption::CreateRole(true)
            ]
        );
    }

    #[test]
    fn create_user_password_parse() {
        let stmt = parse_create("CREATE USER app PASSWORD 's''ecret' NOSUPERUSER");
        assert_eq!(stmt.kind, RoleStmtKind::User);
        assert_eq!(
            stmt.options,
            vec![
                RoleOption::Password(Some("s'ecret".to_owned())),
                RoleOption::Superuser(false)
            ]
        );
    }

    #[test]
    fn alter_role_options_parse() {
        let stmt = parse_alter("ALTER ROLE analytics LOGIN NOCREATEDB");
        assert_eq!(stmt.kind, RoleStmtKind::Role);
        assert_eq!(stmt.name.value, "analytics");
        assert_eq!(
            stmt.options,
            vec![RoleOption::Login(true), RoleOption::CreateDb(false)]
        );
    }
}
