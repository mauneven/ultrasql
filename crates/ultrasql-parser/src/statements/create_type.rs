//! Parser methods for `CREATE TYPE` statements.
//!
//! This module covers PostgreSQL-style enum and composite declarations:
//! `CREATE TYPE name AS ENUM ('label', ...)` and
//! `CREATE TYPE name AS (field type, ...)`.

use crate::ast::{
    CompositeTypeAttribute, CreateDomainStmt, CreateTypeKind, CreateTypeStmt, DomainConstraint,
};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse `CREATE TYPE name AS ...`, consuming the `TYPE` word.
    ///
    /// The `CREATE` keyword must already have been consumed by the caller.
    pub(crate) fn parse_create_type(
        &mut self,
        create_start: u32,
    ) -> Result<CreateTypeStmt, ParseError> {
        self.expect_identifier_keyword("type", "TYPE")?;
        let name = self.parse_object_name()?;
        self.expect(TokenKind::KwAs, "AS")?;
        let kind = if self.next_identifier_is("enum")? {
            self.advance()?;
            self.parse_create_type_enum()?
        } else if self.peek()?.kind == TokenKind::LParen {
            self.parse_create_type_composite()?
        } else {
            let tok = self.peek()?;
            return Err(ParseError::Expected {
                expected: "ENUM or '(' after CREATE TYPE name AS",
                found: tok.kind,
                offset: tok.span.start as usize,
            });
        };
        let end = self.expect(TokenKind::RParen, ")")?.span.end;
        Ok(CreateTypeStmt {
            name,
            kind,
            span: Span::new(create_start, end),
        })
    }

    fn parse_create_type_enum(&mut self) -> Result<CreateTypeKind, ParseError> {
        self.expect(TokenKind::LParen, "(")?;
        let mut labels = Vec::new();
        loop {
            labels.push(self.parse_enum_label()?);
            match self.peek()?.kind {
                TokenKind::Comma => {
                    self.advance()?;
                }
                TokenKind::RParen => break,
                other => {
                    return Err(ParseError::Expected {
                        expected: "',' or ')' after enum label",
                        found: other,
                        offset: self.peek()?.span.start as usize,
                    });
                }
            }
        }
        Ok(CreateTypeKind::Enum { labels })
    }

    fn parse_create_type_composite(&mut self) -> Result<CreateTypeKind, ParseError> {
        self.expect(TokenKind::LParen, "(")?;
        let mut attributes = Vec::new();
        loop {
            let name = self.parse_identifier()?;
            let data_type = self.parse_ddl_type_name()?;
            let span = Span::new(name.span.start, data_type.span.end);
            attributes.push(CompositeTypeAttribute {
                name,
                data_type,
                span,
            });
            match self.peek()?.kind {
                TokenKind::Comma => {
                    self.advance()?;
                }
                TokenKind::RParen => break,
                other => {
                    return Err(ParseError::Expected {
                        expected: "',' or ')' after composite attribute",
                        found: other,
                        offset: self.peek()?.span.start as usize,
                    });
                }
            }
        }
        Ok(CreateTypeKind::Composite { attributes })
    }

    fn parse_enum_label(&mut self) -> Result<String, ParseError> {
        let tok = self.advance()?;
        match tok.kind {
            TokenKind::String => {
                let raw = tok.text(self.source).unwrap_or("");
                if raw.len() >= 2 {
                    Ok(raw[1..raw.len() - 1].replace("''", "'"))
                } else {
                    Ok(String::new())
                }
            }
            other => Err(ParseError::Expected {
                expected: "string literal enum label",
                found: other,
                offset: tok.span.start as usize,
            }),
        }
    }

    /// Parse `CREATE DOMAIN name AS base_type [constraints...]`, consuming
    /// the `DOMAIN` word.
    ///
    /// The `CREATE` keyword must already have been consumed by the caller.
    pub(crate) fn parse_create_domain(
        &mut self,
        create_start: u32,
    ) -> Result<CreateDomainStmt, ParseError> {
        self.expect_identifier_keyword("domain", "DOMAIN")?;
        let name = self.parse_object_name()?;
        self.expect(TokenKind::KwAs, "AS")?;
        let data_type = self.parse_ddl_type_name()?;
        let mut constraints = Vec::new();
        loop {
            let constraint_name = if self.peek()?.kind == TokenKind::KwConstraint {
                self.advance()?;
                Some(self.parse_identifier()?)
            } else {
                None
            };
            match self.peek()?.kind {
                TokenKind::KwNot => {
                    let not_tok = self.advance()?;
                    self.expect(TokenKind::KwNull, "NULL")?;
                    constraints.push(DomainConstraint::NotNull {
                        name: constraint_name,
                        span: Span::new(not_tok.span.start, self.peek()?.span.start),
                    });
                }
                TokenKind::KwNull => {
                    let tok = self.advance()?;
                    constraints.push(DomainConstraint::Null {
                        name: constraint_name,
                        span: tok.span,
                    });
                }
                TokenKind::KwCheck => {
                    let chk_tok = self.advance()?;
                    self.expect(TokenKind::LParen, "(")?;
                    let expr = self.parse_expr()?;
                    let rp = self.expect(TokenKind::RParen, ")")?;
                    constraints.push(DomainConstraint::Check {
                        name: constraint_name,
                        expr,
                        span: Span::new(chk_tok.span.start, rp.span.end),
                    });
                }
                TokenKind::Semicolon | TokenKind::Eof => {
                    if constraint_name.is_some() {
                        let tok = *self.peek()?;
                        return Err(ParseError::Expected {
                            expected: "domain constraint after CONSTRAINT name",
                            found: tok.kind,
                            offset: tok.span.start as usize,
                        });
                    }
                    break;
                }
                other => {
                    if constraint_name.is_some() {
                        let tok = *self.peek()?;
                        return Err(ParseError::Expected {
                            expected: "domain constraint after CONSTRAINT name",
                            found: other,
                            offset: tok.span.start as usize,
                        });
                    }
                    break;
                }
            }
        }
        let end = self.peek()?.span.start;
        Ok(CreateDomainStmt {
            name,
            data_type,
            constraints,
            span: Span::new(create_start, end),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Statement;

    fn parse_create_type(src: &str) -> CreateTypeStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::CreateType(s) => *s,
            other => panic!("expected CreateType, got {other:?}"),
        }
    }

    #[test]
    fn create_type_enum_basic() {
        let stmt = parse_create_type("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')");
        assert_eq!(stmt.name.parts[0].value, "mood");
        match stmt.kind {
            CreateTypeKind::Enum { labels } => assert_eq!(labels, ["sad", "ok", "happy"]),
            other => panic!("expected enum, got {other:?}"),
        }
    }

    #[test]
    fn create_type_enum_unescapes_standard_strings() {
        let stmt = parse_create_type("CREATE TYPE quote_state AS ENUM ('can''t')");
        match stmt.kind {
            CreateTypeKind::Enum { labels } => assert_eq!(labels, ["can't"]),
            other => panic!("expected enum, got {other:?}"),
        }
    }

    #[test]
    fn create_type_composite_basic() {
        let stmt = parse_create_type("CREATE TYPE postal_address AS (street TEXT, zip INT)");
        assert_eq!(stmt.name.parts[0].value, "postal_address");
        let CreateTypeKind::Composite { attributes } = stmt.kind else {
            panic!("expected composite");
        };
        assert_eq!(attributes[0].name.value, "street");
        assert_eq!(attributes[0].data_type.name.value, "text");
        assert_eq!(attributes[1].name.value, "zip");
        assert_eq!(attributes[1].data_type.name.value, "int");
    }

    #[test]
    fn create_domain_not_null_check() {
        let stmt = match Parser::new(
            "CREATE DOMAIN positive_int AS INT CONSTRAINT positive CHECK (VALUE > 0) NOT NULL",
        )
        .parse_statement()
        .expect("parse domain")
        {
            Statement::CreateDomain(s) => *s,
            other => panic!("expected CreateDomain, got {other:?}"),
        };
        assert_eq!(stmt.name.parts[0].value, "positive_int");
        assert_eq!(stmt.data_type.name.value, "int");
        assert_eq!(stmt.constraints.len(), 2);
        let DomainConstraint::Check { name, .. } = &stmt.constraints[0] else {
            panic!("expected check constraint");
        };
        assert_eq!(name.as_ref().map(|n| n.value.as_str()), Some("positive"));
        assert!(matches!(
            stmt.constraints[1],
            DomainConstraint::NotNull { name: None, .. }
        ));
    }
}
