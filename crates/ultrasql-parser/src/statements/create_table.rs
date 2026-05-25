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
    BinaryOp, ColumnConstraint, ColumnDef, CreateTableAsStmt, CreateTableStmt, ExclusionElement,
    Identifier, ObjectName, ReferentialAction, TableConstraint, TablePartitionKind,
    TablePartitionSpec, TypeName,
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
        let partition_by = self.parse_optional_table_partition()?;
        let end = partition_by.as_ref().map_or(rp.span.end, |p| p.span.end);

        Ok(crate::ast::Statement::CreateTable(Box::new(
            CreateTableStmt {
                if_not_exists,
                name,
                columns,
                table_constraints,
                partition_by,
                span: Span::new(create_start, end),
            },
        )))
    }

    fn parse_optional_table_partition(&mut self) -> Result<Option<TablePartitionSpec>, ParseError> {
        if self.peek()?.kind != TokenKind::KwPartition {
            return Ok(None);
        }
        let start = self.advance()?.span.start;
        self.expect(TokenKind::KwBy, "BY")?;
        let strategy = self.advance()?;
        let strategy_text = strategy.text(self.source).unwrap_or("");
        if !strategy_text.eq_ignore_ascii_case("range") {
            return Err(ParseError::Expected {
                expected: "RANGE",
                found: strategy.kind,
                offset: strategy.span.start as usize,
            });
        }
        self.expect(TokenKind::LParen, "(")?;
        let column = self.parse_identifier()?;
        let rp = self.expect(TokenKind::RParen, ")")?;
        Ok(Some(TablePartitionSpec {
            kind: TablePartitionKind::Range,
            column,
            span: Span::new(start, rp.span.end),
        }))
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

    fn parse_referential_action(&mut self) -> Result<ReferentialAction, ParseError> {
        match self.peek()?.kind {
            TokenKind::KwCascade => {
                self.advance()?;
                Ok(ReferentialAction::Cascade)
            }
            TokenKind::KwRestrict => {
                self.advance()?;
                Ok(ReferentialAction::Restrict)
            }
            TokenKind::KwSet => {
                self.advance()?;
                match self.peek()?.kind {
                    TokenKind::KwNull => {
                        self.advance()?;
                        Ok(ReferentialAction::SetNull)
                    }
                    TokenKind::KwDefault => {
                        self.advance()?;
                        Ok(ReferentialAction::SetDefault)
                    }
                    found => Err(ParseError::Expected {
                        expected: "NULL or DEFAULT after SET",
                        found,
                        offset: self.peek()?.span.start as usize,
                    }),
                }
            }
            TokenKind::KwNo => {
                self.advance()?;
                self.expect(TokenKind::KwAction, "ACTION")?;
                Ok(ReferentialAction::NoAction)
            }
            found => Err(ParseError::Expected {
                expected: "CASCADE, RESTRICT, SET NULL, SET DEFAULT, or NO ACTION",
                found,
                offset: self.peek()?.span.start as usize,
            }),
        }
    }

    fn parse_referential_actions(
        &mut self,
    ) -> Result<(ReferentialAction, ReferentialAction), ParseError> {
        let mut on_delete = ReferentialAction::NoAction;
        let mut on_update = ReferentialAction::NoAction;
        while self.peek()?.kind == TokenKind::KwOn {
            self.advance()?;
            match self.peek()?.kind {
                TokenKind::KwDelete => {
                    self.advance()?;
                    on_delete = self.parse_referential_action()?;
                }
                TokenKind::KwUpdate => {
                    self.advance()?;
                    on_update = self.parse_referential_action()?;
                }
                found => {
                    return Err(ParseError::Expected {
                        expected: "DELETE or UPDATE after ON",
                        found,
                        offset: self.peek()?.span.start as usize,
                    });
                }
            }
        }
        Ok((on_delete, on_update))
    }

    fn parse_constraint_timing(&mut self) -> Result<(bool, bool), ParseError> {
        let mut deferrable = false;
        let mut initially_deferred = false;
        if self.peek()?.kind == TokenKind::KwDeferrable {
            self.advance()?;
            deferrable = true;
        } else if self.peek()?.kind == TokenKind::KwNot
            && self.lookahead_at(1)?.kind == TokenKind::KwDeferrable
        {
            self.advance()?;
            self.expect(TokenKind::KwDeferrable, "DEFERRABLE")?;
        }
        if self.peek()?.kind == TokenKind::KwInitially {
            self.advance()?;
            match self.peek()?.kind {
                TokenKind::KwDeferred => {
                    self.advance()?;
                    initially_deferred = true;
                    deferrable = true;
                }
                TokenKind::KwImmediate => {
                    self.advance()?;
                }
                found => {
                    return Err(ParseError::Expected {
                        expected: "DEFERRED or IMMEDIATE after INITIALLY",
                        found,
                        offset: self.peek()?.span.start as usize,
                    });
                }
            }
        }
        Ok((deferrable, initially_deferred))
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
                    | TokenKind::KwExclude
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
            // Optional CONSTRAINT name prefix before any column constraint.
            let constraint_name: Option<Identifier> =
                if self.peek()?.kind == TokenKind::KwConstraint {
                    self.advance()?; // CONSTRAINT
                    Some(self.parse_identifier()?)
                } else {
                    None
                };

            match self.peek()?.kind {
                TokenKind::KwNot => {
                    let not_tok = self.advance()?;
                    self.expect(TokenKind::KwNull, "NULL")?;
                    constraint_list.push(ColumnConstraint::NotNull {
                        name: constraint_name,
                        span: Span::new(not_tok.span.start, self.peek()?.span.start),
                    });
                }
                TokenKind::KwNull => {
                    let tok = self.advance()?;
                    constraint_list.push(ColumnConstraint::Null {
                        name: constraint_name,
                        span: tok.span,
                    });
                }
                TokenKind::KwDefault => {
                    let def_tok = self.advance()?;
                    let expr = self.parse_expr()?;
                    let span = Span::new(def_tok.span.start, expr.span().end);
                    constraint_list.push(ColumnConstraint::Default {
                        name: constraint_name,
                        expr,
                        span,
                    });
                }
                TokenKind::KwPrimary => {
                    let pk_tok = self.advance()?;
                    self.expect(TokenKind::KwKey, "KEY")?;
                    constraint_list.push(ColumnConstraint::PrimaryKey {
                        name: constraint_name,
                        span: Span::new(pk_tok.span.start, self.peek()?.span.start),
                    });
                }
                TokenKind::KwUnique => {
                    let tok = self.advance()?;
                    constraint_list.push(ColumnConstraint::Unique {
                        name: constraint_name,
                        span: tok.span,
                    });
                }
                TokenKind::KwCheck => {
                    let chk_tok = self.advance()?;
                    self.expect(TokenKind::LParen, "(")?;
                    let expr = self.parse_expr()?;
                    let rp = self.expect(TokenKind::RParen, ")")?;
                    constraint_list.push(ColumnConstraint::Check {
                        name: constraint_name,
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
                    let (on_delete, on_update) = self.parse_referential_actions()?;
                    let (deferrable, initially_deferred) = self.parse_constraint_timing()?;
                    let end = self.peek()?.span.start;
                    constraint_list.push(ColumnConstraint::References {
                        name: constraint_name,
                        target_table,
                        target_columns,
                        on_delete,
                        on_update,
                        deferrable,
                        initially_deferred,
                        span: Span::new(ref_tok.span.start, end),
                    });
                }
                TokenKind::KwGenerated => {
                    let gen_tok = self.advance()?;
                    let always = match self.peek()?.kind {
                        TokenKind::KwAlways => {
                            self.advance()?;
                            true
                        }
                        TokenKind::KwBy => {
                            self.advance()?;
                            self.expect(TokenKind::KwDefault, "DEFAULT")?;
                            false
                        }
                        found => {
                            return Err(ParseError::Expected {
                                expected: "ALWAYS or BY DEFAULT",
                                found,
                                offset: self.peek()?.span.start as usize,
                            });
                        }
                    };
                    self.expect(TokenKind::KwAs, "AS")?;
                    if self.peek()?.kind == TokenKind::KwIdentity {
                        self.advance()?;
                        let (options, end) = if self.peek()?.kind == TokenKind::LParen {
                            self.advance()?;
                            let options = self.parse_sequence_options()?;
                            let rp = self.expect(TokenKind::RParen, ")")?;
                            (options, rp.span.end)
                        } else {
                            (Vec::new(), self.peek()?.span.start)
                        };
                        constraint_list.push(ColumnConstraint::GeneratedIdentity {
                            name: constraint_name,
                            always,
                            options,
                            span: Span::new(gen_tok.span.start, end),
                        });
                    } else if always && self.peek()?.kind == TokenKind::LParen {
                        self.advance()?;
                        let expr = self.parse_expr()?;
                        self.expect(TokenKind::RParen, ")")?;
                        let stored = self.expect(TokenKind::KwStored, "STORED")?;
                        constraint_list.push(ColumnConstraint::GeneratedStored {
                            name: constraint_name,
                            expr,
                            span: Span::new(gen_tok.span.start, stored.span.end),
                        });
                    } else {
                        let tok = self.peek()?;
                        return Err(ParseError::Expected {
                            expected: "IDENTITY or (expr) STORED",
                            found: tok.kind,
                            offset: tok.span.start as usize,
                        });
                    }
                }
                _ => {
                    // If we consumed a CONSTRAINT keyword but found no
                    // recognisable constraint body, that is a parse error.
                    if constraint_name.is_some() {
                        let tok = self.peek()?;
                        return Err(ParseError::Expected {
                            expected: "NOT NULL, NULL, DEFAULT, PRIMARY KEY, UNIQUE, CHECK, REFERENCES, or GENERATED after CONSTRAINT name",
                            found: tok.kind,
                            offset: tok.span.start as usize,
                        });
                    }
                    break;
                }
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
    /// Accepts an optional leading `CONSTRAINT name` label. When present the
    /// name is stored in the resulting [`TableConstraint`] variant so that
    /// `ALTER TABLE … DROP CONSTRAINT name` can identify the constraint.
    pub(crate) fn parse_table_constraint(&mut self) -> Result<TableConstraint, ParseError> {
        let start = self.peek()?.span.start;
        // Optional CONSTRAINT name — stored rather than discarded so that
        // DROP CONSTRAINT can identify constraints by name.
        let constraint_name: Option<Identifier> = if self.peek()?.kind == TokenKind::KwConstraint {
            self.advance()?; // CONSTRAINT
            Some(self.parse_identifier()?)
        } else {
            None
        };

        match self.peek()?.kind {
            TokenKind::KwPrimary => {
                self.advance()?; // PRIMARY
                self.expect(TokenKind::KwKey, "KEY")?;
                let cols = self.parse_ident_list_paren()?;
                let end = self.peek()?.span.start;
                Ok(TableConstraint::PrimaryKey {
                    name: constraint_name,
                    columns: cols,
                    span: Span::new(start, end),
                })
            }
            TokenKind::KwUnique => {
                self.advance()?; // UNIQUE
                let cols = self.parse_ident_list_paren()?;
                let end = self.peek()?.span.start;
                Ok(TableConstraint::Unique {
                    name: constraint_name,
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
                let (on_delete, on_update) = self.parse_referential_actions()?;
                let (deferrable, initially_deferred) = self.parse_constraint_timing()?;
                let end = self.peek()?.span.start;
                Ok(TableConstraint::ForeignKey {
                    name: constraint_name,
                    columns: cols,
                    target_table,
                    target_columns,
                    on_delete,
                    on_update,
                    deferrable,
                    initially_deferred,
                    span: Span::new(start, end),
                })
            }
            TokenKind::KwCheck => {
                self.advance()?; // CHECK
                self.expect(TokenKind::LParen, "(")?;
                let expr = self.parse_expr()?;
                let rp = self.expect(TokenKind::RParen, ")")?;
                Ok(TableConstraint::Check {
                    name: constraint_name,
                    expr,
                    span: Span::new(start, rp.span.end),
                })
            }
            TokenKind::KwExclude => {
                self.advance()?; // EXCLUDE
                self.expect(TokenKind::KwUsing, "USING")?;
                let method = self.parse_identifier()?;
                self.expect(TokenKind::LParen, "(")?;
                let mut elements = Vec::new();
                loop {
                    let col_start = self.peek()?.span.start;
                    let column = self.parse_identifier()?;
                    self.expect(TokenKind::KwWith, "WITH")?;
                    let op = self.parse_exclusion_operator()?;
                    let end = self.peek()?.span.start;
                    elements.push(ExclusionElement {
                        column,
                        op,
                        span: Span::new(col_start, end),
                    });
                    match self.peek()?.kind {
                        TokenKind::Comma => {
                            self.advance()?;
                        }
                        TokenKind::RParen => {
                            let rp = self.advance()?;
                            return Ok(TableConstraint::Exclude {
                                name: constraint_name,
                                method,
                                elements,
                                span: Span::new(start, rp.span.end),
                            });
                        }
                        other => {
                            return Err(ParseError::Expected {
                                expected: "',' or ')' in EXCLUDE element list",
                                found: other,
                                offset: self.peek()?.span.start as usize,
                            });
                        }
                    }
                }
            }
            other => Err(ParseError::Expected {
                expected: "PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK, or EXCLUDE",
                found: other,
                offset: self.peek()?.span.start as usize,
            }),
        }
    }

    fn parse_exclusion_operator(&mut self) -> Result<BinaryOp, ParseError> {
        let tok = self.peek()?;
        let op = match tok.kind {
            TokenKind::Eq => BinaryOp::Eq,
            TokenKind::Overlap => BinaryOp::Overlap,
            TokenKind::AtArrow => BinaryOp::JsonContains,
            TokenKind::ArrowAt => BinaryOp::JsonContained,
            other => {
                return Err(ParseError::Expected {
                    expected: "=, &&, @>, or <@",
                    found: other,
                    offset: tok.span.start as usize,
                });
            }
        };
        self.advance()?;
        Ok(op)
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
        let mut name = self.parse_type_name()?;
        if name.value == "double" && self.peek()?.kind == TokenKind::KwPrecision {
            let precision = self.advance()?;
            name.value = "double precision".to_owned();
            name.span = Span::new(name.span.start, precision.span.end);
        } else if name.value == "bit" && self.next_identifier_is("varying")? {
            let varying = self.advance()?;
            name.value = "bit varying".to_owned();
            name.span = Span::new(name.span.start, varying.span.end);
        } else if matches!(name.value.as_str(), "time" | "timestamp")
            && self.peek()?.kind == TokenKind::KwWith
        {
            self.advance()?;
            self.expect(TokenKind::KwTime, "TIME")?;
            let zone = self.expect(TokenKind::KwZone, "ZONE")?;
            name.value = format!("{} with time zone", name.value);
            name.span = Span::new(name.span.start, zone.span.end);
        } else if matches!(name.value.as_str(), "time" | "timestamp")
            && self.next_identifier_is("without")?
        {
            self.advance()?;
            self.expect(TokenKind::KwTime, "TIME")?;
            let zone = self.expect(TokenKind::KwZone, "ZONE")?;
            name.value = format!("{} without time zone", name.value);
            name.span = Span::new(name.span.start, zone.span.end);
        }

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

        // Optional repeated array suffixes: `[]`, `[][]`, ...
        let mut array_dimensions = 0_u32;
        while self.peek()?.kind == TokenKind::LBracket {
            let bracket = self.advance()?; // [
            self.expect(TokenKind::RBracket, "]")?;
            array_dimensions =
                array_dimensions
                    .checked_add(1)
                    .ok_or(ParseError::InvalidInteger {
                        text: "array dimension count".to_owned(),
                        offset: bracket.span.start as usize,
                    })?;
        }
        let is_array = array_dimensions > 0;

        let end = self.peek()?.span.start;
        Ok(TypeName {
            name,
            type_modifiers,
            is_array,
            array_dimensions,
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
        // Unnamed NOT NULL constraint.
        assert!(
            matches!(
                stmt.columns[0].constraints[0],
                ColumnConstraint::NotNull { name: None, .. }
            ),
            "got {:?}",
            stmt.columns[0].constraints[0]
        );
        assert_eq!(stmt.columns[1].data_type.type_modifiers, vec![255]);
        assert_eq!(stmt.table_constraints.len(), 1);
        // Unnamed table-level PRIMARY KEY.
        assert!(
            matches!(
                stmt.table_constraints[0],
                TableConstraint::PrimaryKey { name: None, .. }
            ),
            "got {:?}",
            stmt.table_constraints[0]
        );
    }

    #[test]
    fn create_table_parses_vector_type_modifier() {
        let stmt = parse_create_table("CREATE TABLE items (embedding VECTOR(3))");
        assert_eq!(stmt.columns[0].data_type.name.value, "vector");
        assert_eq!(stmt.columns[0].data_type.type_modifiers, vec![3]);
        assert!(!stmt.columns[0].data_type.is_array);
    }

    #[test]
    fn create_table_parses_multi_dimensional_array_type_suffixes() {
        let stmt = parse_create_table("CREATE TABLE matrices (id INT, cells INT[][])");
        assert!(stmt.columns[1].data_type.is_array);
        assert_eq!(stmt.columns[1].data_type.array_dimensions, 2);
    }

    #[test]
    fn create_table_parses_range_partition_clause() {
        let stmt = parse_create_table(
            "CREATE TABLE metrics (ts TIMESTAMP NOT NULL, v INT) PARTITION BY RANGE (ts)",
        );
        let partition = stmt.partition_by.expect("partition spec");
        assert_eq!(partition.kind, TablePartitionKind::Range);
        assert_eq!(partition.column.value, "ts");
    }

    #[test]
    fn create_table_parses_timestamp_without_time_zone() {
        let stmt = parse_create_table(
            "CREATE TABLE databasechangelog (dateexecuted TIMESTAMP WITHOUT TIME ZONE NOT NULL)",
        );
        assert_eq!(
            stmt.columns[0].data_type.name.value,
            "timestamp without time zone"
        );
        assert!(matches!(
            stmt.columns[0].constraints[0],
            ColumnConstraint::NotNull { .. }
        ));
    }

    #[test]
    fn create_table_accepts_locked_as_column_name() {
        let stmt = parse_create_table("CREATE TABLE lock_table (LOCKED BOOLEAN NOT NULL)");
        assert_eq!(stmt.columns[0].name.value, "locked");
        assert_eq!(stmt.columns[0].data_type.name.value, "boolean");
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
            TableConstraint::Unique { name: None, .. }
        ));
        assert!(matches!(
            stmt.table_constraints[1],
            TableConstraint::ForeignKey { name: None, .. }
        ));
    }

    #[test]
    fn create_table_parses_exclude_using_gist() {
        let stmt = parse_create_table(
            "CREATE TABLE bookings (room int, during int4range, \
             EXCLUDE USING gist (room WITH =, during WITH &&))",
        );
        assert_eq!(stmt.table_constraints.len(), 1);
        let TableConstraint::Exclude {
            method, elements, ..
        } = &stmt.table_constraints[0]
        else {
            panic!("expected Exclude, got {:?}", stmt.table_constraints[0]);
        };
        assert_eq!(method.value, "gist");
        assert_eq!(elements.len(), 2);
        assert_eq!(elements[0].column.value, "room");
        assert_eq!(elements[0].op, BinaryOp::Eq);
        assert_eq!(elements[1].column.value, "during");
        assert_eq!(elements[1].op, BinaryOp::Overlap);
    }

    #[test]
    fn parses_named_table_constraint_preserves_name() {
        // CONSTRAINT <name> precedes each constraint body; the name must be
        // stored in the AST rather than discarded so DROP CONSTRAINT can
        // identify constraints by name.
        let stmt = parse_create_table(
            "CREATE TABLE orders ( \
               id integer, \
               user_id integer, \
               CONSTRAINT pk_orders PRIMARY KEY (id), \
               CONSTRAINT uq_user UNIQUE (user_id), \
               CONSTRAINT chk_id CHECK (id > 0), \
               CONSTRAINT fk_users FOREIGN KEY (user_id) REFERENCES users (id) \
             )",
        );
        assert_eq!(stmt.table_constraints.len(), 4);
        let TableConstraint::PrimaryKey { ref name, .. } = stmt.table_constraints[0] else {
            panic!("expected PrimaryKey, got {:?}", stmt.table_constraints[0]);
        };
        assert_eq!(
            name.as_ref().map(|n| n.value.as_str()),
            Some("pk_orders"),
            "PrimaryKey constraint name must be preserved"
        );
        let TableConstraint::Unique { ref name, .. } = stmt.table_constraints[1] else {
            panic!("expected Unique, got {:?}", stmt.table_constraints[1]);
        };
        assert_eq!(name.as_ref().map(|n| n.value.as_str()), Some("uq_user"));
        let TableConstraint::Check { ref name, .. } = stmt.table_constraints[2] else {
            panic!("expected Check, got {:?}", stmt.table_constraints[2]);
        };
        assert_eq!(name.as_ref().map(|n| n.value.as_str()), Some("chk_id"));
        let TableConstraint::ForeignKey { ref name, .. } = stmt.table_constraints[3] else {
            panic!("expected ForeignKey, got {:?}", stmt.table_constraints[3]);
        };
        assert_eq!(name.as_ref().map(|n| n.value.as_str()), Some("fk_users"));
    }

    #[test]
    fn parses_named_column_constraint_preserves_name() {
        // Column-level CONSTRAINT name prefix must also be preserved.
        let stmt = parse_create_table(
            "CREATE TABLE t ( \
               id integer CONSTRAINT pk_t PRIMARY KEY, \
               score float CONSTRAINT chk_score CHECK (score >= 0) \
             )",
        );
        assert_eq!(stmt.columns.len(), 2);
        let ColumnConstraint::PrimaryKey { ref name, .. } = stmt.columns[0].constraints[0] else {
            panic!(
                "expected PrimaryKey, got {:?}",
                stmt.columns[0].constraints[0]
            );
        };
        assert_eq!(name.as_ref().map(|n| n.value.as_str()), Some("pk_t"));
        let ColumnConstraint::Check { ref name, .. } = stmt.columns[1].constraints[0] else {
            panic!("expected Check, got {:?}", stmt.columns[1].constraints[0]);
        };
        assert_eq!(name.as_ref().map(|n| n.value.as_str()), Some("chk_score"));
    }

    #[test]
    fn parses_foreign_key_referential_actions() {
        let stmt = parse_create_table(
            "CREATE TABLE child (\
             parent_id integer REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT, \
             FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET NULL)",
        );
        let ColumnConstraint::References {
            on_delete,
            on_update,
            ..
        } = stmt.columns[0].constraints[0]
        else {
            panic!("expected References");
        };
        assert_eq!(on_delete, ReferentialAction::Cascade);
        assert_eq!(on_update, ReferentialAction::Restrict);

        let TableConstraint::ForeignKey { on_delete, .. } = stmt.table_constraints[0] else {
            panic!("expected ForeignKey");
        };
        assert_eq!(on_delete, ReferentialAction::SetNull);
    }

    #[test]
    fn parses_deferrable_foreign_keys() {
        let stmt = parse_create_table(
            "CREATE TABLE child (\
             parent_id integer REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED, \
             FOREIGN KEY (parent_id) REFERENCES parent(id) NOT DEFERRABLE INITIALLY IMMEDIATE)",
        );
        let ColumnConstraint::References {
            deferrable,
            initially_deferred,
            ..
        } = stmt.columns[0].constraints[0]
        else {
            panic!("expected References");
        };
        assert!(deferrable);
        assert!(initially_deferred);

        let TableConstraint::ForeignKey {
            deferrable,
            initially_deferred,
            ..
        } = stmt.table_constraints[0]
        else {
            panic!("expected ForeignKey");
        };
        assert!(!deferrable);
        assert!(!initially_deferred);
    }

    #[test]
    fn parses_generated_identity_column_constraint() {
        let stmt = parse_create_table(
            "CREATE TABLE t (id bigint GENERATED ALWAYS AS IDENTITY (START WITH 10 INCREMENT BY 5))",
        );
        let ColumnConstraint::GeneratedIdentity {
            always, options, ..
        } = &stmt.columns[0].constraints[0]
        else {
            panic!(
                "expected GeneratedIdentity, got {:?}",
                stmt.columns[0].constraints[0]
            );
        };
        assert!(*always);
        assert_eq!(options.len(), 2);
    }

    #[test]
    fn parses_generated_stored_column_constraint() {
        let stmt =
            parse_create_table("CREATE TABLE t (a int, b int GENERATED ALWAYS AS (a + 1) STORED)");
        let ColumnConstraint::GeneratedStored { expr, .. } = &stmt.columns[1].constraints[0] else {
            panic!(
                "expected GeneratedStored, got {:?}",
                stmt.columns[1].constraints[0]
            );
        };
        assert!(matches!(expr, crate::ast::Expr::Binary { .. }));
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
