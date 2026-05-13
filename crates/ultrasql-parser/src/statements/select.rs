//! Parser methods for `SELECT` statements.
//!
//! This module contains `impl<'src> Parser<'src>` blocks that parse the
//! `SELECT` statement and its sub-clauses (projection list, table
//! references, ORDER BY list, object names, and identifiers). All other
//! statement kinds live in sibling modules.

use crate::ast::{
    Identifier, NullsOrder, ObjectName, OrderItem, SelectItem, SelectStmt, SortDirection, TableRef,
};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse a complete `SELECT` statement, starting from the `SELECT`
    /// keyword.
    pub(crate) fn parse_select(&mut self) -> Result<SelectStmt, ParseError> {
        let start = self.expect(TokenKind::KwSelect, "SELECT")?;
        let distinct = self.match_kw(TokenKind::KwDistinct);
        let projection = self.parse_select_list()?;

        let from = if self.match_kw(TokenKind::KwFrom) {
            Some(self.parse_table_ref()?)
        } else {
            None
        };

        let r#where = if self.match_kw(TokenKind::KwWhere) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let order_by = if self.match_kw(TokenKind::KwOrder) {
            self.expect(TokenKind::KwBy, "BY")?;
            self.parse_order_list()?
        } else {
            Vec::new()
        };

        let limit = if self.match_kw(TokenKind::KwLimit) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let offset = if self.match_kw(TokenKind::KwOffset) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let end_tok = self.peek()?;
        let end = end_tok.span.start;
        let span = Span::new(start.span.start, end);

        Ok(SelectStmt {
            distinct,
            projection,
            from,
            r#where,
            order_by,
            limit,
            offset,
            span,
        })
    }

    /// Parse a comma-separated `SELECT` projection list (one or more
    /// items).
    pub(crate) fn parse_select_list(&mut self) -> Result<Vec<SelectItem>, ParseError> {
        let mut items = Vec::new();
        loop {
            items.push(self.parse_select_item()?);
            if self.peek()?.kind != TokenKind::Comma {
                return Ok(items);
            }
            self.advance()?;
        }
    }

    /// Parse one item in the `SELECT` projection list.
    pub(crate) fn parse_select_item(&mut self) -> Result<SelectItem, ParseError> {
        // `*`
        if self.peek()?.kind == TokenKind::Star {
            let tok = self.advance()?;
            return Ok(SelectItem::Wildcard { span: tok.span });
        }

        // `name.*` ?
        if matches!(
            self.peek()?.kind,
            TokenKind::Identifier | TokenKind::QuotedIdentifier
        ) && self.lookahead_two_is(TokenKind::Dot, TokenKind::Star)
        {
            let ident = self.parse_identifier()?;
            self.advance()?; // dot
            let star = self.advance()?; // star
            return Ok(SelectItem::QualifiedWildcard {
                qualifier: ident.clone(),
                span: Span::new(ident.span.start, star.span.end),
            });
        }

        let expr = self.parse_expr()?;
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
        let span_start = expr.span().start;
        let expr_end = expr.span().end;
        let span_end = alias.as_ref().map_or(expr_end, |a| a.span.end);
        Ok(SelectItem::Expr {
            expr,
            alias,
            span: Span::new(span_start, span_end),
        })
    }

    /// Parse a single table reference after `FROM` or `JOIN`.
    pub(crate) fn parse_table_ref(&mut self) -> Result<TableRef, ParseError> {
        let name = self.parse_object_name()?;
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

    /// Parse a comma-separated `ORDER BY` list.
    pub(crate) fn parse_order_list(&mut self) -> Result<Vec<OrderItem>, ParseError> {
        let mut items = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let direction = if self.match_kw(TokenKind::KwAsc) {
                SortDirection::Asc
            } else if self.match_kw(TokenKind::KwDesc) {
                SortDirection::Desc
            } else {
                SortDirection::Asc
            };
            let nulls = if self.match_kw(TokenKind::KwNulls) {
                // NULLS FIRST | NULLS LAST
                let n = self.advance()?;
                if n.text(self.source)
                    .is_some_and(|t| t.eq_ignore_ascii_case("first"))
                {
                    NullsOrder::First
                } else if n
                    .text(self.source)
                    .is_some_and(|t| t.eq_ignore_ascii_case("last"))
                {
                    NullsOrder::Last
                } else {
                    return Err(ParseError::Expected {
                        expected: "FIRST or LAST",
                        found: n.kind,
                        offset: n.span.start as usize,
                    });
                }
            } else {
                NullsOrder::Default
            };
            let span_start = expr.span().start;
            let span_end = self.peek()?.span.start;
            items.push(OrderItem {
                expr,
                direction,
                nulls,
                span: Span::new(span_start, span_end),
            });
            if self.peek()?.kind != TokenKind::Comma {
                return Ok(items);
            }
            self.advance()?;
        }
    }

    /// Parse a (possibly schema-qualified) object name such as
    /// `schema.table` or just `table`.
    pub(crate) fn parse_object_name(&mut self) -> Result<ObjectName, ParseError> {
        let first = self.parse_identifier()?;
        let mut parts = vec![first.clone()];
        let start = first.span.start;
        let mut end = first.span.end;
        while self.peek()?.kind == TokenKind::Dot {
            // Look past the dot — if the next token is `*`, this is
            // not part of the name and we leave the dot in place for
            // the caller.
            if self.lookahead_at(1)?.kind == TokenKind::Star {
                break;
            }
            self.advance()?; // dot
            let ident = self.parse_identifier()?;
            end = ident.span.end;
            parts.push(ident);
        }
        Ok(ObjectName {
            parts,
            span: Span::new(start, end),
        })
    }

    /// Parse a single SQL identifier (unquoted or double-quoted).
    pub(crate) fn parse_identifier(&mut self) -> Result<Identifier, ParseError> {
        let tok = self.peek()?;
        match tok.kind {
            TokenKind::Identifier => {
                let tok = self.advance()?;
                let raw = tok.text(self.source).unwrap_or("");
                Ok(Identifier {
                    value: raw.to_ascii_lowercase(),
                    quoted: false,
                    span: tok.span,
                })
            }
            TokenKind::QuotedIdentifier => {
                let tok = self.advance()?;
                let raw = tok.text(self.source).unwrap_or("");
                // Strip the outer quotes and collapse "" to ".
                let inner = &raw[1..raw.len() - 1];
                let value = inner.replace("\"\"", "\"");
                Ok(Identifier {
                    value,
                    quoted: true,
                    span: tok.span,
                })
            }
            other => Err(ParseError::Expected {
                expected: "identifier",
                found: other,
                offset: tok.span.start as usize,
            }),
        }
    }
}
