//! Parser helpers shared across the `SELECT` machinery: the projection list,
//! the `ORDER BY` list, object-name / identifier parsing, and the small
//! comma-separated list helpers.

use crate::ast::{
    Expr, Identifier, NullsOrder, ObjectName, OrderItem, SelectItem, SortDirection,
};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    // ------------------------------------------------------------------ //
    // Projection list                                                     //
    // ------------------------------------------------------------------ //

    /// Parse a comma-separated `SELECT` projection list (one or more items).
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
        let alias = if self.match_kw(TokenKind::KwAs) {
            Some(self.parse_alias_identifier()?)
        } else if matches!(
            self.peek()?.kind,
            TokenKind::Identifier | TokenKind::QuotedIdentifier
        ) && !self.next_token_is_reserved_clause()
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

    // ------------------------------------------------------------------ //
    // ORDER BY list                                                       //
    // ------------------------------------------------------------------ //

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
                        offset: n.span.start_usize(),
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

    // ------------------------------------------------------------------ //
    // Object name / identifier helpers                                    //
    // ------------------------------------------------------------------ //

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
            TokenKind::Identifier | TokenKind::KwLocked => {
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
                offset: tok.span.start_usize(),
            }),
        }
    }

    // ------------------------------------------------------------------ //
    // List helpers                                                        //
    // ------------------------------------------------------------------ //

    /// Parse a comma-separated list of expressions (at least one).
    pub(crate) fn parse_expr_list(&mut self) -> Result<Vec<Expr>, ParseError> {
        let mut exprs = vec![self.parse_expr()?];
        while self.peek()?.kind == TokenKind::Comma {
            self.advance()?;
            exprs.push(self.parse_expr()?);
        }
        Ok(exprs)
    }

    /// Parse a comma-separated list of identifiers (at least one).
    pub(crate) fn parse_identifier_list(&mut self) -> Result<Vec<Identifier>, ParseError> {
        let mut ids = vec![self.parse_identifier()?];
        while self.peek()?.kind == TokenKind::Comma {
            self.advance()?;
            ids.push(self.parse_identifier()?);
        }
        Ok(ids)
    }

    pub(crate) fn parse_alias_identifier(&mut self) -> Result<Identifier, ParseError> {
        if self.peek()?.kind == TokenKind::KwDelimiter {
            let tok = self.advance()?;
            return Ok(Identifier {
                value: "delimiter".to_owned(),
                quoted: false,
                span: tok.span,
            });
        }
        if self.peek()?.kind == TokenKind::KwComment {
            let tok = self.advance()?;
            return Ok(Identifier {
                value: "comment".to_owned(),
                quoted: false,
                span: tok.span,
            });
        }
        if self.peek()?.kind == TokenKind::KwIdentity {
            let tok = self.advance()?;
            return Ok(Identifier {
                value: "identity".to_owned(),
                quoted: false,
                span: tok.span,
            });
        }
        self.parse_identifier()
    }
}
