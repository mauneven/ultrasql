//! Parser methods for object privilege DDL.
//!
//! Handles the supported privilege statement subset:
//! - `GRANT privileges ON {TABLE|SCHEMA|DATABASE|SEQUENCE|FUNCTION} objects TO roles`
//! - `REVOKE privileges ON {TABLE|SCHEMA|DATABASE|SEQUENCE|FUNCTION} objects FROM roles`

use crate::ast::{
    AlterDefaultPrivilegesStmt, DefaultPrivilegeAction, GrantRoleStmt, GrantStmt, PrivilegeKind,
    PrivilegeObjectKind, PrivilegeSpec, RevokeRoleStmt, RevokeStmt, Statement,
};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse either object-privilege or role-membership `GRANT`.
    pub(crate) fn parse_grant_statement(&mut self) -> Result<Statement, ParseError> {
        if self.is_role_membership_grant()? {
            self.parse_grant_role()
                .map(|s| Statement::GrantRole(Box::new(s)))
        } else {
            self.parse_grant().map(|s| Statement::Grant(Box::new(s)))
        }
    }

    /// Parse either object-privilege or role-membership `REVOKE`.
    pub(crate) fn parse_revoke_statement(&mut self) -> Result<Statement, ParseError> {
        if self.is_role_membership_revoke()? {
            self.parse_revoke_role()
                .map(|s| Statement::RevokeRole(Box::new(s)))
        } else {
            self.parse_revoke().map(|s| Statement::Revoke(Box::new(s)))
        }
    }

    /// Parse `GRANT ... ON ... TO ...`.
    pub(crate) fn parse_grant(&mut self) -> Result<GrantStmt, ParseError> {
        let start = self.expect(TokenKind::KwGrant, "GRANT")?.span.start;
        let privileges = self.parse_privilege_list()?;
        self.expect(TokenKind::KwOn, "ON")?;
        let object_kind = self.parse_privilege_object_kind()?;
        let objects = self.parse_privilege_objects(object_kind)?;
        self.expect(TokenKind::KwTo, "TO")?;
        let grantees = self.parse_identifier_list()?;
        let grant_option = if self.peek()?.kind == TokenKind::KwWith {
            self.advance()?;
            self.expect(TokenKind::KwGrant, "GRANT")?;
            self.expect_identifier_keyword("option", "OPTION")?;
            true
        } else {
            false
        };
        let end = self.peek()?.span.start;
        Ok(GrantStmt {
            privileges,
            object_kind,
            objects,
            grantees,
            grant_option,
            span: Span::new(start, end),
        })
    }

    /// Parse `GRANT role [, ...] TO role [, ...]`.
    fn parse_grant_role(&mut self) -> Result<GrantRoleStmt, ParseError> {
        let start = self.expect(TokenKind::KwGrant, "GRANT")?.span.start;
        let roles = self.parse_identifier_list()?;
        self.expect(TokenKind::KwTo, "TO")?;
        let grantees = self.parse_identifier_list()?;
        let admin_option = if self.peek()?.kind == TokenKind::KwWith {
            self.advance()?;
            self.expect_identifier_keyword("admin", "ADMIN")?;
            self.expect_identifier_keyword("option", "OPTION")?;
            true
        } else {
            false
        };
        let end = self.peek()?.span.start;
        Ok(GrantRoleStmt {
            roles,
            grantees,
            admin_option,
            span: Span::new(start, end),
        })
    }

    /// Parse `REVOKE ... ON ... FROM ...`.
    pub(crate) fn parse_revoke(&mut self) -> Result<RevokeStmt, ParseError> {
        let start = self.expect(TokenKind::KwRevoke, "REVOKE")?.span.start;
        let grant_option_for = if self.peek()?.kind == TokenKind::KwGrant {
            self.advance()?;
            self.expect_identifier_keyword("option", "OPTION")?;
            self.expect(TokenKind::KwFor, "FOR")?;
            true
        } else {
            false
        };
        let privileges = self.parse_privilege_list()?;
        self.expect(TokenKind::KwOn, "ON")?;
        let object_kind = self.parse_privilege_object_kind()?;
        let objects = self.parse_privilege_objects(object_kind)?;
        self.expect(TokenKind::KwFrom, "FROM")?;
        let grantees = self.parse_identifier_list()?;
        let cascade = self.parse_cascade_restrict();
        let end = self.peek()?.span.start;
        Ok(RevokeStmt {
            grant_option_for,
            privileges,
            object_kind,
            objects,
            grantees,
            cascade,
            span: Span::new(start, end),
        })
    }

    /// Parse `REVOKE role [, ...] FROM role [, ...]`.
    fn parse_revoke_role(&mut self) -> Result<RevokeRoleStmt, ParseError> {
        let start = self.expect(TokenKind::KwRevoke, "REVOKE")?.span.start;
        let next = *self.peek()?;
        let admin_option_for = if next.kind == TokenKind::Identifier
            && next
                .text(self.source)
                .is_some_and(|text| text.eq_ignore_ascii_case("admin"))
        {
            self.advance()?;
            self.expect_identifier_keyword("option", "OPTION")?;
            self.expect(TokenKind::KwFor, "FOR")?;
            true
        } else {
            false
        };
        let roles = self.parse_identifier_list()?;
        self.expect(TokenKind::KwFrom, "FROM")?;
        let grantees = self.parse_identifier_list()?;
        let cascade = self.parse_cascade_restrict();
        let end = self.peek()?.span.start;
        Ok(RevokeRoleStmt {
            admin_option_for,
            roles,
            grantees,
            cascade,
            span: Span::new(start, end),
        })
    }

    /// Parse `ALTER DEFAULT PRIVILEGES ...`.
    pub(crate) fn parse_alter_default_privileges(
        &mut self,
        start: u32,
    ) -> Result<AlterDefaultPrivilegesStmt, ParseError> {
        self.expect(TokenKind::KwDefault, "DEFAULT")?;
        self.expect_identifier_keyword("privileges", "PRIVILEGES")?;
        let target_roles = if self.peek()?.kind == TokenKind::KwFor {
            self.advance()?;
            let role_word = *self.peek()?;
            let role_ok = role_word.text(self.source).is_some_and(|word| {
                word.eq_ignore_ascii_case("role") || word.eq_ignore_ascii_case("user")
            });
            if !role_ok {
                return Err(ParseError::Expected {
                    expected: "ROLE or USER after FOR",
                    found: role_word.kind,
                    offset: role_word.span.start_usize(),
                });
            }
            self.advance()?;
            self.parse_identifier_list()?
        } else {
            Vec::new()
        };
        let schemas = if self.peek()?.kind == TokenKind::KwIn {
            self.advance()?;
            self.expect(TokenKind::KwSchema, "SCHEMA")?;
            self.parse_identifier_list()?
        } else {
            Vec::new()
        };
        let action = match self.peek()?.kind {
            TokenKind::KwGrant => self.parse_default_privilege_grant_action()?,
            TokenKind::KwRevoke => self.parse_default_privilege_revoke_action()?,
            found => {
                return Err(ParseError::Expected {
                    expected: "GRANT or REVOKE in ALTER DEFAULT PRIVILEGES",
                    found,
                    offset: self.peek()?.span.start_usize(),
                });
            }
        };
        let end = self.peek()?.span.start;
        Ok(AlterDefaultPrivilegesStmt {
            target_roles,
            schemas,
            action,
            span: Span::new(start, end),
        })
    }

    fn parse_default_privilege_grant_action(
        &mut self,
    ) -> Result<DefaultPrivilegeAction, ParseError> {
        self.expect(TokenKind::KwGrant, "GRANT")?;
        let privileges = self.parse_privilege_list()?;
        self.expect(TokenKind::KwOn, "ON")?;
        let object_kind = self.parse_privilege_object_kind()?;
        self.expect(TokenKind::KwTo, "TO")?;
        let grantees = self.parse_identifier_list()?;
        let grant_option = if self.peek()?.kind == TokenKind::KwWith {
            self.advance()?;
            self.expect(TokenKind::KwGrant, "GRANT")?;
            self.expect_identifier_keyword("option", "OPTION")?;
            true
        } else {
            false
        };
        Ok(DefaultPrivilegeAction::Grant {
            privileges,
            object_kind,
            grantees,
            grant_option,
        })
    }

    fn parse_default_privilege_revoke_action(
        &mut self,
    ) -> Result<DefaultPrivilegeAction, ParseError> {
        self.expect(TokenKind::KwRevoke, "REVOKE")?;
        let grant_option_for = if self.peek()?.kind == TokenKind::KwGrant {
            self.advance()?;
            self.expect_identifier_keyword("option", "OPTION")?;
            self.expect(TokenKind::KwFor, "FOR")?;
            true
        } else {
            false
        };
        let privileges = self.parse_privilege_list()?;
        self.expect(TokenKind::KwOn, "ON")?;
        let object_kind = self.parse_privilege_object_kind()?;
        self.expect(TokenKind::KwFrom, "FROM")?;
        let grantees = self.parse_identifier_list()?;
        let cascade = self.parse_cascade_restrict();
        Ok(DefaultPrivilegeAction::Revoke {
            grant_option_for,
            privileges,
            object_kind,
            grantees,
            cascade,
        })
    }

    fn is_role_membership_grant(&mut self) -> Result<bool, ParseError> {
        let mut offset = 1;
        loop {
            let tok = self.lookahead_at(offset)?;
            match tok.kind {
                TokenKind::KwOn => return Ok(false),
                TokenKind::KwTo => return Ok(true),
                TokenKind::Semicolon | TokenKind::Eof => return Ok(false),
                _ => offset += 1,
            }
        }
    }

    fn is_role_membership_revoke(&mut self) -> Result<bool, ParseError> {
        let mut offset = 1;
        loop {
            let tok = self.lookahead_at(offset)?;
            match tok.kind {
                TokenKind::KwOn => return Ok(false),
                TokenKind::KwFrom => return Ok(true),
                TokenKind::Semicolon | TokenKind::Eof => return Ok(false),
                _ => offset += 1,
            }
        }
    }

    fn parse_privilege_list(&mut self) -> Result<Vec<PrivilegeSpec>, ParseError> {
        let first = self.parse_privilege_kind()?;
        if first == PrivilegeKind::All {
            if self.peek_word_is("privileges")? {
                self.advance()?;
            }
            return Ok(vec![PrivilegeSpec {
                kind: first,
                columns: Vec::new(),
            }]);
        }
        let mut privileges = vec![PrivilegeSpec {
            kind: first,
            columns: self.parse_optional_privilege_columns()?,
        }];
        while self.peek()?.kind == TokenKind::Comma {
            self.advance()?;
            let kind = self.parse_privilege_kind()?;
            privileges.push(PrivilegeSpec {
                kind,
                columns: self.parse_optional_privilege_columns()?,
            });
        }
        Ok(privileges)
    }

    fn parse_optional_privilege_columns(
        &mut self,
    ) -> Result<Vec<crate::ast::Identifier>, ParseError> {
        if self.peek()?.kind != TokenKind::LParen {
            return Ok(Vec::new());
        }
        self.advance()?;
        let columns = self.parse_identifier_list()?;
        self.expect(TokenKind::RParen, ")")?;
        Ok(columns)
    }

    fn parse_privilege_kind(&mut self) -> Result<PrivilegeKind, ParseError> {
        let tok = *self.peek()?;
        let Some(word) = tok.text(self.source).map(str::to_ascii_lowercase) else {
            return Err(ParseError::Expected {
                expected: "privilege keyword",
                found: tok.kind,
                offset: tok.span.start_usize(),
            });
        };
        let kind = match word.as_str() {
            "all" => PrivilegeKind::All,
            "select" => PrivilegeKind::Select,
            "insert" => PrivilegeKind::Insert,
            "update" => PrivilegeKind::Update,
            "delete" => PrivilegeKind::Delete,
            "truncate" => PrivilegeKind::Truncate,
            "references" => PrivilegeKind::References,
            "trigger" => PrivilegeKind::Trigger,
            "usage" => PrivilegeKind::Usage,
            "create" => PrivilegeKind::Create,
            "connect" => PrivilegeKind::Connect,
            "temporary" | "temp" => PrivilegeKind::Temporary,
            "execute" => PrivilegeKind::Execute,
            _ => {
                return Err(ParseError::Expected {
                    expected: "privilege keyword",
                    found: tok.kind,
                    offset: tok.span.start_usize(),
                });
            }
        };
        self.advance()?;
        Ok(kind)
    }

    fn parse_privilege_object_kind(&mut self) -> Result<PrivilegeObjectKind, ParseError> {
        let tok = *self.peek()?;
        let Some(word) = tok.text(self.source).map(str::to_ascii_lowercase) else {
            return Err(ParseError::Expected {
                expected: "privilege object kind",
                found: tok.kind,
                offset: tok.span.start_usize(),
            });
        };
        let kind = match word.as_str() {
            "table" | "tables" => PrivilegeObjectKind::Table,
            "schema" | "schemas" => PrivilegeObjectKind::Schema,
            "database" | "databases" => PrivilegeObjectKind::Database,
            "sequence" | "sequences" => PrivilegeObjectKind::Sequence,
            "function" | "functions" | "routine" | "routines" => PrivilegeObjectKind::Function,
            _ => {
                return Err(ParseError::Expected {
                    expected: "TABLE, SCHEMA, DATABASE, SEQUENCE, or FUNCTION",
                    found: tok.kind,
                    offset: tok.span.start_usize(),
                });
            }
        };
        self.advance()?;
        Ok(kind)
    }

    fn parse_privilege_objects(
        &mut self,
        object_kind: PrivilegeObjectKind,
    ) -> Result<Vec<crate::ast::ObjectName>, ParseError> {
        let mut objects = vec![self.parse_privilege_object(object_kind)?];
        while self.peek()?.kind == TokenKind::Comma {
            self.advance()?;
            objects.push(self.parse_privilege_object(object_kind)?);
        }
        Ok(objects)
    }

    fn parse_privilege_object(
        &mut self,
        object_kind: PrivilegeObjectKind,
    ) -> Result<crate::ast::ObjectName, ParseError> {
        let name = self.parse_object_name()?;
        if object_kind == PrivilegeObjectKind::Function && self.peek()?.kind == TokenKind::LParen {
            self.skip_parenthesized_function_signature()?;
        }
        Ok(name)
    }

    fn skip_parenthesized_function_signature(&mut self) -> Result<(), ParseError> {
        self.expect(TokenKind::LParen, "(")?;
        let mut depth = 1_u32;
        while depth > 0 {
            let tok = *self.peek()?;
            match tok.kind {
                TokenKind::LParen => {
                    self.advance()?;
                    depth += 1;
                }
                TokenKind::RParen => {
                    self.advance()?;
                    depth -= 1;
                }
                TokenKind::Eof => {
                    return Err(ParseError::UnexpectedEof {
                        expected: "function signature closing ')'",
                    });
                }
                _ => {
                    self.advance()?;
                }
            }
        }
        Ok(())
    }

    fn peek_word_is(&mut self, expected: &str) -> Result<bool, ParseError> {
        let tok = *self.peek()?;
        Ok(tok
            .text(self.source)
            .is_some_and(|word| word.eq_ignore_ascii_case(expected)))
    }
}

#[cfg(test)]
mod tests {
    use crate::Parser;
    use crate::ast::{PrivilegeKind, PrivilegeObjectKind, Statement};

    #[test]
    fn grant_table_privileges_parses() {
        let stmt = Parser::new("GRANT SELECT, INSERT ON TABLE public.t TO analyst")
            .parse_statement()
            .expect("grant parses");
        let Statement::Grant(stmt) = stmt else {
            panic!("expected GRANT");
        };
        assert_eq!(stmt.privileges[0].kind, PrivilegeKind::Select);
        assert!(stmt.privileges[0].columns.is_empty());
        assert_eq!(stmt.privileges[1].kind, PrivilegeKind::Insert);
        assert!(stmt.privileges[1].columns.is_empty());
        assert_eq!(stmt.object_kind, PrivilegeObjectKind::Table);
        assert_eq!(stmt.objects[0].to_string(), "public.t");
        assert_eq!(stmt.grantees[0].value, "analyst");
    }

    #[test]
    fn grant_column_privileges_parse() {
        let stmt = Parser::new("GRANT SELECT(id), UPDATE(secret) ON TABLE t TO analyst")
            .parse_statement()
            .expect("grant parses");
        let Statement::Grant(stmt) = stmt else {
            panic!("expected GRANT");
        };
        assert_eq!(stmt.privileges[0].kind, PrivilegeKind::Select);
        assert_eq!(stmt.privileges[0].columns[0].value, "id");
        assert_eq!(stmt.privileges[1].kind, PrivilegeKind::Update);
        assert_eq!(stmt.privileges[1].columns[0].value, "secret");
    }

    #[test]
    fn revoke_function_privilege_parses_signature() {
        let stmt = Parser::new("REVOKE EXECUTE ON FUNCTION current_database() FROM analyst")
            .parse_statement()
            .expect("revoke parses");
        let Statement::Revoke(stmt) = stmt else {
            panic!("expected REVOKE");
        };
        assert_eq!(stmt.privileges[0].kind, PrivilegeKind::Execute);
        assert_eq!(stmt.object_kind, PrivilegeObjectKind::Function);
        assert_eq!(stmt.objects[0].to_string(), "current_database");
        assert_eq!(stmt.grantees[0].value, "analyst");
    }

    #[test]
    fn grant_role_membership_parses() {
        let stmt = Parser::new("GRANT app_group TO app_user, support WITH ADMIN OPTION")
            .parse_statement()
            .expect("role grant parses");
        let Statement::GrantRole(stmt) = stmt else {
            panic!("expected role GRANT");
        };
        assert_eq!(stmt.roles[0].value, "app_group");
        assert_eq!(stmt.grantees[0].value, "app_user");
        assert_eq!(stmt.grantees[1].value, "support");
        assert!(stmt.admin_option);
    }

    #[test]
    fn revoke_role_membership_parses() {
        let stmt = Parser::new("REVOKE ADMIN OPTION FOR app_group FROM app_user CASCADE")
            .parse_statement()
            .expect("role revoke parses");
        let Statement::RevokeRole(stmt) = stmt else {
            panic!("expected role REVOKE");
        };
        assert!(stmt.admin_option_for);
        assert_eq!(stmt.roles[0].value, "app_group");
        assert_eq!(stmt.grantees[0].value, "app_user");
        assert!(stmt.cascade);
    }

    #[test]
    fn alter_default_privileges_parses_scope_and_action() {
        let stmt = Parser::new(
            "ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app \
             GRANT SELECT ON TABLES TO analyst WITH GRANT OPTION",
        )
        .parse_statement()
        .expect("default privilege grant parses");
        let Statement::AlterDefaultPrivileges(stmt) = stmt else {
            panic!("expected ALTER DEFAULT PRIVILEGES");
        };
        assert_eq!(stmt.target_roles[0].value, "app_owner");
        assert_eq!(stmt.schemas[0].value, "app");
        let crate::ast::DefaultPrivilegeAction::Grant {
            privileges,
            object_kind,
            grantees,
            grant_option,
        } = &stmt.action
        else {
            panic!("expected default privilege grant action");
        };
        assert_eq!(privileges[0].kind, PrivilegeKind::Select);
        assert_eq!(*object_kind, PrivilegeObjectKind::Table);
        assert_eq!(grantees[0].value, "analyst");
        assert!(*grant_option);

        let stmt = Parser::new(
            "ALTER DEFAULT PRIVILEGES REVOKE GRANT OPTION FOR USAGE ON SEQUENCES FROM analyst CASCADE",
        )
        .parse_statement()
        .expect("default privilege revoke parses");
        let Statement::AlterDefaultPrivileges(stmt) = stmt else {
            panic!("expected ALTER DEFAULT PRIVILEGES");
        };
        let crate::ast::DefaultPrivilegeAction::Revoke {
            grant_option_for,
            object_kind,
            grantees,
            cascade,
            ..
        } = &stmt.action
        else {
            panic!("expected default privilege revoke action");
        };
        assert!(stmt.target_roles.is_empty());
        assert!(*grant_option_for);
        assert_eq!(*object_kind, PrivilegeObjectKind::Sequence);
        assert_eq!(grantees[0].value, "analyst");
        assert!(*cascade);
    }
}
