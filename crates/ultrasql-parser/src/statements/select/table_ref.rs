//! Parser methods for named table references and the table-function-shaped
//! `JSON_TABLE` / `XMLTABLE` constructs, plus the small identifier-keyword
//! helpers they rely on.

use crate::ast::{
    Expr, Identifier, JsonTableColumn, JsonTableColumnKind, Literal, TableRef, XmlTableColumn,
    XmlTableColumnKind,
};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    // ------------------------------------------------------------------ //
    // Table reference helpers                                             //
    // ------------------------------------------------------------------ //

    /// Parse a single named table reference after `FROM` or `JOIN`.
    pub(crate) fn parse_table_ref(&mut self) -> Result<TableRef, ParseError> {
        let name = self.parse_object_name()?;
        // `name (` after a single-identifier name signals a table
        // function — `generate_series(1, 10)`, `unnest(array)`, etc.
        if name.parts.len() == 1 && self.peek()?.kind == TokenKind::LParen {
            let Some(func_name) = name.parts.into_iter().next() else {
                return Err(ParseError::UnexpectedEof {
                    expected: "table function name",
                });
            };
            if func_name.value.eq_ignore_ascii_case("json_table") {
                return self.parse_json_table_ref(func_name);
            }
            if func_name.value.eq_ignore_ascii_case("xmltable") {
                return self.parse_xml_table_ref(func_name);
            }
            self.advance()?; // (
            let mut args = Vec::new();
            if self.peek()?.kind != TokenKind::RParen {
                loop {
                    args.push(self.parse_expr()?);
                    if self.peek()?.kind != TokenKind::Comma {
                        break;
                    }
                    self.advance()?;
                }
            }
            let rp = self.expect(TokenKind::RParen, ")")?;
            let alias = if self.match_kw(TokenKind::KwAs)
                || (matches!(
                    self.peek()?.kind,
                    TokenKind::Identifier | TokenKind::QuotedIdentifier
                ) && !self.next_token_is_reserved_clause())
            {
                Some(self.parse_identifier()?)
            } else {
                None
            };
            let end = alias.as_ref().map_or(rp.span.end, |a| a.span.end);
            return Ok(TableRef::Function {
                span: Span::new(func_name.span.start, end),
                name: func_name,
                args,
                alias,
            });
        }
        let alias = if self.match_kw(TokenKind::KwAs)
            || (matches!(
                self.peek()?.kind,
                TokenKind::Identifier | TokenKind::QuotedIdentifier
            ) && !self.next_token_is_reserved_clause())
        {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        let end = alias.as_ref().map_or(name.span.end, |a| a.span.end);
        Ok(TableRef::Named {
            span: Span::new(name.span.start, end),
            name,
            alias,
        })
    }

    fn parse_json_table_ref(&mut self, name: Identifier) -> Result<TableRef, ParseError> {
        self.expect(TokenKind::LParen, "(")?;
        let context = self.parse_expr()?;
        self.expect(TokenKind::Comma, ",")?;
        let row_path = self.parse_table_function_string_literal("JSON_TABLE row path")?;

        if self.match_kw(TokenKind::KwAs) {
            let _ = self.parse_identifier()?;
        }
        self.expect_ident_keyword("COLUMNS")?;
        self.expect(TokenKind::LParen, "(")?;
        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_json_table_column()?);
            if self.peek()?.kind == TokenKind::Comma {
                self.advance()?;
            } else {
                break;
            }
        }
        self.expect(TokenKind::RParen, ")")?;
        let rp = self.expect(TokenKind::RParen, ")")?;
        let alias = if self.match_kw(TokenKind::KwAs)
            || (matches!(
                self.peek()?.kind,
                TokenKind::Identifier | TokenKind::QuotedIdentifier
            ) && !self.next_token_is_reserved_clause())
        {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        let end = alias.as_ref().map_or(rp.span.end, |a| a.span.end);
        Ok(TableRef::JsonTable {
            context,
            row_path,
            columns,
            alias,
            span: Span::new(name.span.start, end),
        })
    }

    fn parse_json_table_column(&mut self) -> Result<JsonTableColumn, ParseError> {
        let name = self.parse_identifier()?;
        if self.match_kw(TokenKind::KwFor) {
            self.expect_ident_keyword("ORDINALITY")?;
            let end = self.peek()?.span.start;
            return Ok(JsonTableColumn {
                span: Span::new(name.span.start, end),
                name,
                kind: JsonTableColumnKind::Ordinality,
            });
        }

        let data_type = self.parse_ddl_type_name()?;
        let kind = if self.match_kw(TokenKind::KwExists) {
            JsonTableColumnKind::Exists {
                data_type,
                path: self.parse_optional_json_table_path()?,
            }
        } else {
            JsonTableColumnKind::Value {
                data_type,
                path: self.parse_optional_json_table_path()?,
            }
        };
        let end = self.peek()?.span.start;
        Ok(JsonTableColumn {
            span: Span::new(name.span.start, end),
            name,
            kind,
        })
    }

    fn parse_xml_table_ref(&mut self, name: Identifier) -> Result<TableRef, ParseError> {
        self.expect(TokenKind::LParen, "(")?;
        let row_path = self.parse_table_function_string_literal("XMLTABLE row path")?;
        self.expect_ident_keyword("PASSING")?;
        let context = self.parse_expr()?;
        self.expect_ident_keyword("COLUMNS")?;
        self.expect(TokenKind::LParen, "(")?;
        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_xml_table_column()?);
            if self.peek()?.kind == TokenKind::Comma {
                self.advance()?;
            } else {
                break;
            }
        }
        self.expect(TokenKind::RParen, ")")?;
        let rp = self.expect(TokenKind::RParen, ")")?;
        let alias = if self.match_kw(TokenKind::KwAs)
            || (matches!(
                self.peek()?.kind,
                TokenKind::Identifier | TokenKind::QuotedIdentifier
            ) && !self.next_token_is_reserved_clause())
        {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        let end = alias.as_ref().map_or(rp.span.end, |a| a.span.end);
        Ok(TableRef::XmlTable {
            context,
            row_path,
            columns,
            alias,
            span: Span::new(name.span.start, end),
        })
    }

    fn parse_xml_table_column(&mut self) -> Result<XmlTableColumn, ParseError> {
        let name = self.parse_identifier()?;
        if self.match_kw(TokenKind::KwFor) {
            self.expect_ident_keyword("ORDINALITY")?;
            let end = self.peek()?.span.start;
            return Ok(XmlTableColumn {
                span: Span::new(name.span.start, end),
                name,
                kind: XmlTableColumnKind::Ordinality,
            });
        }

        let data_type = self.parse_ddl_type_name()?;
        let path = if self.peek_is_ident_keyword("PATH")? {
            self.advance()?;
            Some(self.parse_table_function_string_literal("XMLTABLE column path")?)
        } else {
            None
        };
        let default = if self.match_kw(TokenKind::KwDefault) {
            Some(self.parse_table_function_string_literal("XMLTABLE column default")?)
        } else {
            None
        };
        let end = self.peek()?.span.start;
        Ok(XmlTableColumn {
            span: Span::new(name.span.start, end),
            name,
            kind: XmlTableColumnKind::Value {
                data_type,
                path,
                default,
            },
        })
    }

    fn parse_optional_json_table_path(&mut self) -> Result<Option<String>, ParseError> {
        if self.peek_is_ident_keyword("PATH")? {
            self.advance()?;
            Ok(Some(self.parse_table_function_string_literal(
                "JSON_TABLE column path",
            )?))
        } else {
            Ok(None)
        }
    }

    fn parse_table_function_string_literal(
        &mut self,
        expected: &'static str,
    ) -> Result<String, ParseError> {
        let expr = self.parse_expr()?;
        let span = expr.span();
        match expr {
            Expr::Literal(Literal::String { value, .. }) => Ok(value),
            _ => Err(ParseError::Expected {
                expected,
                found: self.peek()?.kind,
                offset: span.start_usize(),
            }),
        }
    }

    fn expect_ident_keyword(&mut self, expected: &'static str) -> Result<Span, ParseError> {
        let tok = *self.peek()?;
        if tok.kind == TokenKind::Identifier
            && tok
                .text(self.source)
                .is_some_and(|text| text.eq_ignore_ascii_case(expected))
        {
            return Ok(self.advance()?.span);
        }
        Err(ParseError::Expected {
            expected,
            found: tok.kind,
            offset: tok.span.start_usize(),
        })
    }

    fn peek_is_ident_keyword(&mut self, expected: &str) -> Result<bool, ParseError> {
        let tok = *self.peek()?;
        Ok(tok.kind == TokenKind::Identifier
            && tok
                .text(self.source)
                .is_some_and(|text| text.eq_ignore_ascii_case(expected)))
    }
}
