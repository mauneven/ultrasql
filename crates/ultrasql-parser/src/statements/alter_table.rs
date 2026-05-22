//! Parser methods for `ALTER TABLE` statements.
//!
//! Handles the following action clauses:
//! - `ADD [COLUMN] col type [constraints]`
//! - `DROP [COLUMN] col [CASCADE|RESTRICT]`
//! - `RENAME COLUMN old TO new`
//! - `RENAME TO new_name`
//! - `ADD CONSTRAINT name constraint`
//! - `DROP CONSTRAINT name [CASCADE|RESTRICT]`
//! - `ENABLE ROW LEVEL SECURITY`
//! - `SET (option = value, ...)`

use crate::ast::{AlterTableAction, AlterTableStmt};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse `ALTER TABLE …`, consuming the `TABLE` keyword.
    ///
    /// The `ALTER` keyword must already have been consumed by the caller.
    pub(crate) fn parse_alter_table(
        &mut self,
        alter_start: u32,
    ) -> Result<AlterTableStmt, ParseError> {
        self.expect(TokenKind::KwTable, "TABLE")?;
        let name = self.parse_object_name()?;
        let action = self.parse_alter_table_action()?;
        let end = self.peek()?.span.start;
        Ok(AlterTableStmt {
            name,
            action,
            span: Span::new(alter_start, end),
        })
    }

    fn parse_alter_table_action(&mut self) -> Result<AlterTableAction, ParseError> {
        let tok = *self.peek()?;
        let start = tok.span.start;
        match tok.kind {
            TokenKind::KwAdd => {
                self.advance()?; // ADD
                if self.peek()?.kind == TokenKind::KwConstraint {
                    // ADD CONSTRAINT
                    let constraint = self.parse_table_constraint()?;
                    let end = self.peek()?.span.start;
                    return Ok(AlterTableAction::AddConstraint {
                        constraint,
                        span: Span::new(start, end),
                    });
                }
                // ADD [COLUMN]
                self.match_kw(TokenKind::KwColumn);
                let column = self.parse_column_def()?;
                let end = self.peek()?.span.start;
                Ok(AlterTableAction::AddColumn {
                    column,
                    span: Span::new(start, end),
                })
            }
            TokenKind::KwDrop => {
                self.advance()?; // DROP
                if self.peek()?.kind == TokenKind::KwConstraint {
                    // DROP CONSTRAINT name
                    self.advance()?; // CONSTRAINT
                    let name = self.parse_identifier()?;
                    let cascade = self.parse_cascade_restrict();
                    let end = self.peek()?.span.start;
                    return Ok(AlterTableAction::DropConstraint {
                        name,
                        cascade,
                        span: Span::new(start, end),
                    });
                }
                // DROP [COLUMN] name
                self.match_kw(TokenKind::KwColumn);
                let name = self.parse_identifier()?;
                let cascade = self.parse_cascade_restrict();
                let end = self.peek()?.span.start;
                Ok(AlterTableAction::DropColumn {
                    name,
                    cascade,
                    span: Span::new(start, end),
                })
            }
            TokenKind::KwRename => {
                self.advance()?; // RENAME
                if self.peek()?.kind == TokenKind::KwTo {
                    // RENAME TO new_name
                    self.advance()?; // TO
                    let new_name = self.parse_identifier()?;
                    let end = self.peek()?.span.start;
                    return Ok(AlterTableAction::RenameTable {
                        new_name,
                        span: Span::new(start, end),
                    });
                }
                // RENAME COLUMN old TO new
                self.match_kw(TokenKind::KwColumn);
                let old = self.parse_identifier()?;
                self.expect(TokenKind::KwTo, "TO")?;
                let new = self.parse_identifier()?;
                let end = self.peek()?.span.start;
                Ok(AlterTableAction::RenameColumn {
                    old,
                    new,
                    span: Span::new(start, end),
                })
            }
            TokenKind::KwSet => {
                self.advance()?; // SET
                let options = self.parse_index_options()?;
                let end = self.peek()?.span.start;
                Ok(AlterTableAction::SetOptions {
                    options,
                    span: Span::new(start, end),
                })
            }
            TokenKind::Identifier
                if tok
                    .text(self.source)
                    .is_some_and(|text| text.eq_ignore_ascii_case("enable")) =>
            {
                self.expect_identifier_keyword("enable", "ENABLE")?;
                self.expect(TokenKind::KwRow, "ROW")?;
                self.expect(TokenKind::KwLevel, "LEVEL")?;
                self.expect_identifier_keyword("security", "SECURITY")?;
                let end = self.peek()?.span.start;
                Ok(AlterTableAction::EnableRowLevelSecurity {
                    span: Span::new(start, end),
                })
            }
            other => Err(ParseError::Expected {
                expected: "ADD, DROP, RENAME, or SET",
                found: other,
                offset: tok.span.start as usize,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{AlterTableAction, Statement};
    use crate::parser::Parser;

    fn parse_alter(src: &str) -> AlterTableStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::AlterTable(s) => *s,
            other => panic!("expected AlterTable, got {other:?}"),
        }
    }

    // ---- happy-path -------------------------------------------------------

    #[test]
    fn alter_table_add_column() {
        let stmt = parse_alter("ALTER TABLE users ADD COLUMN email varchar(255) NOT NULL");
        assert_eq!(stmt.name.to_string(), "users");
        let AlterTableAction::AddColumn { column, .. } = stmt.action else {
            panic!("expected AddColumn")
        };
        assert_eq!(column.name.value, "email");
        assert_eq!(column.data_type.name.value, "varchar");
    }

    #[test]
    fn alter_table_drop_column_cascade() {
        let stmt = parse_alter("ALTER TABLE t DROP COLUMN old_col CASCADE");
        let AlterTableAction::DropColumn { name, cascade, .. } = stmt.action else {
            panic!("expected DropColumn")
        };
        assert_eq!(name.value, "old_col");
        assert!(cascade);
    }

    #[test]
    fn alter_table_rename_column() {
        let stmt = parse_alter("ALTER TABLE t RENAME COLUMN first_name TO given_name");
        let AlterTableAction::RenameColumn { old, new, .. } = stmt.action else {
            panic!("expected RenameColumn")
        };
        assert_eq!(old.value, "first_name");
        assert_eq!(new.value, "given_name");
    }

    #[test]
    fn alter_table_rename_to() {
        let stmt = parse_alter("ALTER TABLE old_name RENAME TO new_name");
        let AlterTableAction::RenameTable { new_name, .. } = stmt.action else {
            panic!("expected RenameTable")
        };
        assert_eq!(new_name.value, "new_name");
    }

    #[test]
    fn alter_table_add_constraint() {
        let stmt = parse_alter(
            "ALTER TABLE t ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users (id)",
        );
        let AlterTableAction::AddConstraint { constraint, .. } = stmt.action else {
            panic!("expected AddConstraint")
        };
        // The constraint name from `CONSTRAINT fk_user` must be preserved.
        let crate::ast::TableConstraint::ForeignKey { ref name, .. } = constraint else {
            panic!("expected ForeignKey, got {constraint:?}");
        };
        assert_eq!(
            name.as_ref().map(|n| n.value.as_str()),
            Some("fk_user"),
            "constraint name must be stored in the AST"
        );
    }

    #[test]
    fn alter_table_drop_constraint() {
        let stmt = parse_alter("ALTER TABLE t DROP CONSTRAINT fk_user CASCADE");
        let AlterTableAction::DropConstraint { name, cascade, .. } = stmt.action else {
            panic!("expected DropConstraint")
        };
        assert_eq!(name.value, "fk_user");
        assert!(cascade);
    }

    #[test]
    fn alter_table_set_options() {
        let stmt = parse_alter(
            "ALTER TABLE t SET (autovacuum_vacuum_threshold = 7, autovacuum_analyze_scale_factor = 0.05)",
        );
        let AlterTableAction::SetOptions { options, .. } = stmt.action else {
            panic!("expected SetOptions")
        };
        assert_eq!(options.len(), 2);
        assert_eq!(options[0].name.value, "autovacuum_vacuum_threshold");
        assert_eq!(options[1].name.value, "autovacuum_analyze_scale_factor");
    }

    // ---- negative case ----------------------------------------------------

    #[test]
    fn alter_table_unknown_action_errors() {
        let err = Parser::new("ALTER TABLE t TRUNCATE")
            .parse_statement()
            .unwrap_err();
        assert!(matches!(err, ParseError::Expected { .. }));
    }
}
