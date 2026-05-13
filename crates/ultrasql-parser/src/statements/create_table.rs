//! Parser methods for `CREATE TABLE` statements.
//!
//! Handles the following forms:
//! - `CREATE TABLE t (col type [constraints], …)`
//! - `CREATE TABLE IF NOT EXISTS t (…)`
//! - `CREATE TABLE t AS SELECT …` (dispatched from here after name parsing)
//!
//! # Public helpers exposed for sibling modules
//!
//! - [`Parser::parse_column_def`] — one column definition
//! - [`Parser::parse_table_constraint`] — one table-level constraint
//! - [`Parser::parse_ddl_type_name`] — a typed `TypeName` with modifiers

use crate::ast::{
    ColumnConstraint, ColumnDef, CreateTableAsStmt, CreateTableStmt, Identifier, ObjectName,
    TableConstraint, TypeName,
};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse `CREATE TABLE …`, consuming the `TABLE` keyword.
    ///
    /// The `CREATE` keyword must already have been consumed by the caller
    /// (the top-level dispatch). Dispatches to either `CreateTable` or
    /// `CreateTableAs` depending on whether `AS SELECT` follows the table
    /// name.
    pub(crate) fn parse_create_table(
        &mut self,
        create_start: u32,
    ) -> Result<crate::ast::Statement, ParseError> {
        self.expect(TokenKind::KwTable, "TABLE")?;

        // IF NOT EXISTS
        let if_not_exists = self.parse_if_not_exists()?;

        let name = self.parse_object_name()?;

        // Detect `CREATE TABLE t AS SELECT …`
        if self.peek()?.kind == TokenKind::KwAs {
            self.advance()?; // AS
            // Optional column name list
            let columns = if self.peek()?.kind == TokenKind::LParen {
                self.parse_ident_list_paren()?
            } else {
                Vec::new()
            };
            let source = Box::new(self.parse_select()?);
            let end = self.peek()?.span.start;
            return Ok(crate::ast::Statement::CreateTableAs(Box::new(
                CreateTableAsStmt {
                    if_not_exists,
                    name,
                    columns,
                    source,
                    span: Span::new(create_start, end),
                },
            )));
        }

        // Standard form: column list
        self.expect(TokenKind::LParen, "(")?;
        let (columns, table_constraints) = self.parse_column_and_constraint_list()?;
        let rp = self.expect(TokenKind::RParen, ")")?;

        Ok(crate::ast::Statement::CreateTable(Box::new(
            CreateTableStmt {
                if_not_exists,
                name,
                columns,
                table_constraints,
                span: Span::new(create_start, rp.span.end),
            },
        )))
    }

    /// Parse `IF NOT EXISTS` as three tokens.  Returns `true` if found.
    pub(crate) fn parse_if_not_exists(&mut self) -> Result<bool, ParseError> {
        if self.peek()?.kind == TokenKind::KwIf {
            let after_if = self.lookahead_at(1)?;
            let after_not = self.lookahead_at(2)?;
            if after_if.kind == TokenKind::KwNot && after_not.kind == TokenKind::KwExists {
                self.advance()?; // IF
                self.advance()?; // NOT
                self.advance()?; // EXISTS
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Parse `IF EXISTS` as two tokens.  Returns `true` if found.
    pub(crate) fn parse_if_exists(&mut self) -> Result<bool, ParseError> {
        if self.peek()?.kind == TokenKind::KwIf {
            let after_if = self.lookahead_at(1)?;
            if after_if.kind == TokenKind::KwExists {
                self.advance()?; // IF
                self.advance()?; // EXISTS
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Parse `CASCADE` or `RESTRICT` if present; returns `true` for `CASCADE`.
    pub(crate) fn parse_cascade_restrict(&mut self) -> bool {
        match self.peek().map(|t| t.kind) {
            Ok(TokenKind::KwCascade) => {
                let _ = self.advance();
                true
            }
            Ok(TokenKind::KwRestrict) => {
                let _ = self.advance();
                false
            }
            _ => false,
        }
    }

    /// Parse the body of a `CREATE TABLE` column list: zero or more
    /// column definitions and table constraints, separated by commas.
    fn parse_column_and_constraint_list(
        &mut self,
    ) -> Result<(Vec<ColumnDef>, Vec<TableConstraint>), ParseError> {
        let mut columns = Vec::new();
        let mut constraints = Vec::new();

        loop {
            let kind = self.peek()?.kind;
            if kind == TokenKind::RParen {
                break;
            }
            // Table-level constraints start with CONSTRAINT, PRIMARY, UNIQUE,
            // FOREIGN, or CHECK.
            if matches!(
                kind,
                TokenKind::KwConstraint
                    | TokenKind::KwPrimary
                    | TokenKind::KwUnique
                    | TokenKind::KwForeign
                    | TokenKind::KwCheck
            ) {
                constraints.push(self.parse_table_constraint()?);
            } else {
                columns.push(self.parse_column_def()?);
            }
            if self.peek()?.kind == TokenKind::Comma {
                self.advance()?;
            } else {
                break;
            }
        }

        Ok((columns, constraints))
    }

    /// Parse one column definition: `name type [constraint …]`.
    pub(crate) fn parse_column_def(&mut self) -> Result<ColumnDef, ParseError> {
        let name = self.parse_identifier()?;
        let data_type = self.parse_ddl_type_name()?;
        let mut constraint_list = Vec::new();

        // Consume any column-level constraints.
        loop {
            match self.peek()?.kind {
                TokenKind::KwNot => {
                    let not_tok = self.advance()?;
                    self.expect(TokenKind::KwNull, "NULL")?;
                    constraint_list.push(ColumnConstraint::NotNull {
                        span: Span::new(not_tok.span.start, self.peek()?.span.start),
                    });
                }
                TokenKind::KwNull => {
                    let tok = self.advance()?;
                    constraint_list.push(ColumnConstraint::Null { span: tok.span });
                }
                TokenKind::KwDefault => {
                    let def_tok = self.advance()?;
                    let expr = self.parse_expr()?;
                    let span = Span::new(def_tok.span.start, expr.span().end);
                    constraint_list.push(ColumnConstraint::Default { expr, span });
                }
                TokenKind::KwPrimary => {
                    let pk_tok = self.advance()?;
                    self.expect(TokenKind::KwKey, "KEY")?;
                    constraint_list.push(ColumnConstraint::PrimaryKey {
                        span: Span::new(pk_tok.span.start, self.peek()?.span.start),
                    });
                }
                TokenKind::KwUnique => {
                    let tok = self.advance()?;
                    constraint_list.push(ColumnConstraint::Unique { span: tok.span });
                }
                TokenKind::KwCheck => {
                    let chk_tok = self.advance()?;
                    self.expect(TokenKind::LParen, "(")?;
                    let expr = self.parse_expr()?;
                    let rp = self.expect(TokenKind::RParen, ")")?;
                    constraint_list.push(ColumnConstraint::Check {
                        expr,
                        span: Span::new(chk_tok.span.start, rp.span.end),
                    });
                }
                TokenKind::KwReferences => {
                    let ref_tok = self.advance()?;
                    let target_table = self.parse_object_name()?;
                    let target_columns = if self.peek()?.kind == TokenKind::LParen {
                        self.parse_ident_list_paren()?
                    } else {
                        Vec::new()
                    };
                    let end = self.peek()?.span.start;
                    constraint_list.push(ColumnConstraint::References {
                        target_table,
                        target_columns,
                        span: Span::new(ref_tok.span.start, end),
                    });
                }
                _ => break,
            }
        }

        let end = self.peek()?.span.start;
        Ok(ColumnDef {
            span: Span::new(name.span.start, end),
            name,
            data_type,
            constraints: constraint_list,
        })
    }

    /// Parse one table-level constraint.
    ///
    /// Accepts an optional leading `CONSTRAINT name` label (consumed and
    /// discarded — the name is not yet stored in the AST).
    pub(crate) fn parse_table_constraint(&mut self) -> Result<TableConstraint, ParseError> {
        let start = self.peek()?.span.start;
        // Optional CONSTRAINT name
        if self.peek()?.kind == TokenKind::KwConstraint {
            self.advance()?; // CONSTRAINT
            self.parse_identifier()?; // name (discard for now)
        }

        match self.peek()?.kind {
            TokenKind::KwPrimary => {
                self.advance()?; // PRIMARY
                self.expect(TokenKind::KwKey, "KEY")?;
                let cols = self.parse_ident_list_paren()?;
                let end = self.peek()?.span.start;
                Ok(TableConstraint::PrimaryKey {
                    columns: cols,
                    span: Span::new(start, end),
                })
            }
            TokenKind::KwUnique => {
                self.advance()?; // UNIQUE
                let cols = self.parse_ident_list_paren()?;
                let end = self.peek()?.span.start;
                Ok(TableConstraint::Unique {
                    columns: cols,
                    span: Span::new(start, end),
                })
            }
            TokenKind::KwForeign => {
                self.advance()?; // FOREIGN
                self.expect(TokenKind::KwKey, "KEY")?;
                let cols = self.parse_ident_list_paren()?;
                self.expect(TokenKind::KwReferences, "REFERENCES")?;
                let target_table = self.parse_object_name()?;
                let target_columns = if self.peek()?.kind == TokenKind::LParen {
                    self.parse_ident_list_paren()?
                } else {
                    Vec::new()
                };
                let end = self.peek()?.span.start;
                Ok(TableConstraint::ForeignKey {
                    columns: cols,
                    target_table,
                    target_columns,
                    span: Span::new(start, end),
                })
            }
            TokenKind::KwCheck => {
                self.advance()?; // CHECK
                self.expect(TokenKind::LParen, "(")?;
                let expr = self.parse_expr()?;
                let rp = self.expect(TokenKind::RParen, ")")?;
                Ok(TableConstraint::Check {
                    expr,
                    span: Span::new(start, rp.span.end),
                })
            }
            other => Err(ParseError::Expected {
                expected: "PRIMARY KEY, UNIQUE, FOREIGN KEY, or CHECK",
                found: other,
                offset: self.peek()?.span.start as usize,
            }),
        }
    }

    /// Parse a DDL type name: identifier + optional `(modifiers)` +
    /// optional `[]` array suffix.
    ///
    /// This is richer than the CAST-target parser because column
    /// definitions carry modifiers like `VARCHAR(255)` and array types
    /// like `integer[]`.
    pub(crate) fn parse_ddl_type_name(&mut self) -> Result<TypeName, ParseError> {
        let tok = self.peek()?;
        let start = tok.span.start;

        // Accept keyword type names (integer, varchar, …) or identifiers.
        let name = self.parse_type_name()?;

        // Optional type modifiers: `(255)`, `(10, 2)`, etc.
        let type_modifiers = if self.peek()?.kind == TokenKind::LParen {
            self.advance()?; // (
            let mut mods = Vec::new();
            loop {
                let n_tok = self.peek()?;
                match n_tok.kind {
                    TokenKind::Integer => {
                        let t = self.advance()?;
                        let text = t.text(self.source).unwrap_or("0");
                        let n: u32 = text.parse().map_err(|_| ParseError::InvalidInteger {
                            text: text.to_owned(),
                            offset: t.span.start as usize,
                        })?;
                        mods.push(n);
                    }
                    other => {
                        return Err(ParseError::Expected {
                            expected: "integer type modifier",
                            found: other,
                            offset: n_tok.span.start as usize,
                        });
                    }
                }
                if self.peek()?.kind == TokenKind::Comma {
                    self.advance()?;
                } else {
                    break;
                }
            }
            self.expect(TokenKind::RParen, ")")?;
            mods
        } else {
            Vec::new()
        };

        // Optional array suffix `[]`
        let is_array = if self.peek()?.kind == TokenKind::LBracket {
            self.advance()?; // [
            self.expect(TokenKind::RBracket, "]")?;
            true
        } else {
            false
        };

        let end = self.peek()?.span.start;
        Ok(TypeName {
            name,
            type_modifiers,
            is_array,
            span: Span::new(start, end),
        })
    }

    /// Parse a parenthesised, comma-separated identifier list `(a, b, c)`.
    pub(crate) fn parse_ident_list_paren(&mut self) -> Result<Vec<Identifier>, ParseError> {
        self.expect(TokenKind::LParen, "(")?;
        let mut list = Vec::new();
        loop {
            list.push(self.parse_identifier()?);
            match self.peek()?.kind {
                TokenKind::Comma => {
                    self.advance()?;
                }
                TokenKind::RParen => {
                    self.advance()?;
                    break;
                }
                other => {
                    return Err(ParseError::Expected {
                        expected: "',' or ')'",
                        found: other,
                        offset: self.peek()?.span.start as usize,
                    });
                }
            }
        }
        Ok(list)
    }

    /// Parse a comma-separated list of `ObjectName`s.
    pub(crate) fn parse_object_name_list(&mut self) -> Result<Vec<ObjectName>, ParseError> {
        let mut list = Vec::new();
        loop {
            list.push(self.parse_object_name()?);
            if self.peek()?.kind == TokenKind::Comma {
                self.advance()?;
            } else {
                break;
            }
        }
        Ok(list)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{ColumnConstraint, Statement, TableConstraint};
    use crate::parser::Parser;
    use proptest::prelude::*;

    fn parse_create_table(src: &str) -> CreateTableStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::CreateTable(s) => *s,
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    fn parse_create_table_as(src: &str) -> CreateTableAsStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::CreateTableAs(s) => *s,
            other => panic!("expected CreateTableAs, got {other:?}"),
        }
    }

    // ---- happy-path -------------------------------------------------------

    #[test]
    fn create_table_basic() {
        let stmt = parse_create_table(
            "CREATE TABLE users (id integer NOT NULL, name varchar(255), PRIMARY KEY (id))",
        );
        assert_eq!(stmt.name.to_string(), "users");
        assert!(!stmt.if_not_exists);
        assert_eq!(stmt.columns.len(), 2);
        assert_eq!(stmt.columns[0].name.value, "id");
        assert_eq!(stmt.columns[0].data_type.name.value, "integer");
        assert!(matches!(
            stmt.columns[0].constraints[0],
            ColumnConstraint::NotNull { .. }
        ));
        assert_eq!(stmt.columns[1].data_type.type_modifiers, vec![255]);
        assert_eq!(stmt.table_constraints.len(), 1);
        assert!(matches!(
            stmt.table_constraints[0],
            TableConstraint::PrimaryKey { .. }
        ));
    }

    #[test]
    fn create_table_if_not_exists() {
        let stmt = parse_create_table("CREATE TABLE IF NOT EXISTS t (x integer)");
        assert!(stmt.if_not_exists);
        assert_eq!(stmt.name.to_string(), "t");
    }

    #[test]
    fn create_table_as_select() {
        let stmt = parse_create_table_as("CREATE TABLE dst AS SELECT id, name FROM src");
        assert_eq!(stmt.name.to_string(), "dst");
        assert!(!stmt.if_not_exists);
        assert!(stmt.columns.is_empty());
    }

    #[test]
    fn create_table_full_constraints() {
        let stmt = parse_create_table(
            "CREATE TABLE orders ( \
               id bigint PRIMARY KEY, \
               user_id integer NOT NULL REFERENCES users (id), \
               total numeric(10,2) DEFAULT 0, \
               status varchar(20) UNIQUE CHECK (status > 0), \
               tags integer[], \
               UNIQUE (user_id, status), \
               FOREIGN KEY (user_id) REFERENCES users (id) \
             )",
        );
        assert_eq!(stmt.columns.len(), 5);
        // Check array type
        assert!(stmt.columns[4].data_type.is_array);
        // Check table constraints
        assert_eq!(stmt.table_constraints.len(), 2);
        assert!(matches!(
            stmt.table_constraints[0],
            TableConstraint::Unique { .. }
        ));
        assert!(matches!(
            stmt.table_constraints[1],
            TableConstraint::ForeignKey { .. }
        ));
    }

    #[test]
    fn create_table_negative_missing_rparen() {
        let err = Parser::new("CREATE TABLE t (id integer")
            .parse_statement()
            .unwrap_err();
        assert!(matches!(
            err,
            ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
        ));
    }

    // ---- property test ----------------------------------------------------

    proptest! {
        /// For a small set of SQL type names, CREATE TABLE parsing must
        /// preserve the type name text in the ColumnDef.
        #[test]
        fn prop_column_type_names_round_trip(
            type_name in prop_oneof![
                Just("integer"),
                Just("bigint"),
                Just("text"),
                Just("boolean"),
                Just("real"),
            ]
        ) {
            let sql = format!("CREATE TABLE t (col {type_name})");
            let stmt = Parser::new(&sql).parse_statement().expect("must parse");
            let Statement::CreateTable(ct) = stmt else { panic!("expected CreateTable") };
            prop_assert_eq!(&ct.columns[0].data_type.name.value, type_name);
        }
    }
}
