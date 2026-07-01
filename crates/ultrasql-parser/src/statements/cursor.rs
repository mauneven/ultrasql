//! Parser methods for the SQL cursor statements.
//!
//! ```sql
//! DECLARE name [BINARY] [ASENSITIVE | INSENSITIVE] [[NO] SCROLL]
//!     CURSOR [{WITH | WITHOUT} HOLD] FOR select
//! FETCH [direction] [FROM | IN] cursor
//! MOVE  [direction] [FROM | IN] cursor
//! CLOSE { name | ALL }
//! ```
//!
//! The full PostgreSQL surface is *parsed* here — including the forms
//! UltraSQL cannot execute yet (`WITH HOLD`, `SCROLL`, backward
//! directions, `MOVE`) — so the server can reject them with SQLSTATE
//! `0A000` (`feature_not_supported`) plus a hint, instead of a
//! misleading syntax error. `DECLARE`, `CLOSE`, and `MOVE` are
//! dispatched on identifier text (like `MERGE` / `CHECKPOINT`) so the
//! words stay usable as ordinary identifiers elsewhere; `FETCH` is
//! already a lexer keyword.

use crate::ast::{CloseStmt, DeclareCursorStmt, FetchDirection, FetchStmt, Statement};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Consume the next token when it is the (case-insensitive) word
    /// `word` — either a bare identifier or any keyword token. Returns
    /// whether it was consumed. The non-erroring twin of
    /// [`Parser::expect_identifier_keyword`].
    fn match_identifier_keyword(&mut self, word: &str) -> Result<bool, ParseError> {
        let tok = *self.peek()?;
        if tok
            .text(self.source)
            .is_some_and(|text| text.eq_ignore_ascii_case(word))
            && (tok.kind == TokenKind::Identifier || tok.kind.is_keyword())
        {
            self.advance()?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Parse `DECLARE name [options] CURSOR [{WITH|WITHOUT} HOLD] FOR
    /// select`. Assumes `DECLARE` has already been consumed; `start` is
    /// its byte offset.
    pub(crate) fn parse_declare_cursor(&mut self, start: u32) -> Result<Statement, ParseError> {
        let name = self.parse_identifier()?;

        // BINARY / INSENSITIVE / ASENSITIVE / [NO] SCROLL may appear in
        // any order (PostgreSQL grammar). INSENSITIVE/ASENSITIVE are
        // accepted and ignored: every UltraSQL cursor is insensitive
        // (materialized at DECLARE time).
        let mut binary = false;
        let mut scroll = false;
        loop {
            if self.match_identifier_keyword("binary")? {
                binary = true;
            } else if self.match_identifier_keyword("insensitive")?
                || self.match_identifier_keyword("asensitive")?
            {
                // Accepted, no semantic effect.
            } else if self.match_identifier_keyword("scroll")? {
                scroll = true;
            } else if self.peek()?.kind == TokenKind::KwNo {
                self.advance()?; // NO
                self.expect_identifier_keyword("scroll", "SCROLL after NO")?;
                scroll = false;
            } else {
                break;
            }
        }

        self.expect_identifier_keyword("cursor", "CURSOR")?;

        // [{WITH | WITHOUT} HOLD]
        let mut hold = false;
        if self.peek()?.kind == TokenKind::KwWith {
            self.advance()?; // WITH
            self.expect_identifier_keyword("hold", "HOLD after WITH")?;
            hold = true;
        } else if self.match_identifier_keyword("without")? {
            self.expect_identifier_keyword("hold", "HOLD after WITHOUT")?;
        }

        self.expect(TokenKind::KwFor, "FOR")?;

        let head = *self.peek()?;
        if !matches!(head.kind, TokenKind::KwSelect | TokenKind::KwWith) {
            return Err(ParseError::Expected {
                expected: "SELECT or WITH after DECLARE … CURSOR FOR",
                found: head.kind,
                offset: head.span.start_usize(),
            });
        }
        let select = self.parse_select()?;
        let span = Span::new(start, select.span.end);
        Ok(Statement::DeclareCursor(Box::new(DeclareCursorStmt {
            name,
            binary,
            scroll,
            hold,
            select: Box::new(select),
            span,
        })))
    }

    /// Parse `FETCH [direction] [FROM|IN] cursor` (or the `MOVE` twin
    /// when `is_move`). Assumes the leading keyword has been consumed;
    /// `start` is its byte offset.
    pub(crate) fn parse_fetch(
        &mut self,
        start: u32,
        is_move: bool,
    ) -> Result<Statement, ParseError> {
        let direction = self.parse_fetch_direction()?;
        // Optional FROM | IN before the cursor name.
        if self.peek()?.kind == TokenKind::KwFrom || self.peek()?.kind == TokenKind::KwIn {
            self.advance()?;
        }
        let cursor = self.parse_identifier()?;
        let span = Span::new(start, cursor.span.end);
        Ok(Statement::Fetch(FetchStmt {
            direction,
            cursor,
            is_move,
            span,
        }))
    }

    /// Parse the optional direction clause of `FETCH` / `MOVE`. A bare
    /// cursor name (no direction) means one row forward, PostgreSQL's
    /// `NEXT`.
    fn parse_fetch_direction(&mut self) -> Result<FetchDirection, ParseError> {
        let tok = *self.peek()?;
        // ALL / FORWARD [n | ALL]
        if tok.kind == TokenKind::KwAll {
            self.advance()?;
            return Ok(FetchDirection::Forward { count: None });
        }
        if self.match_identifier_keyword("next")? {
            return Ok(FetchDirection::Forward { count: Some(1) });
        }
        if self.match_identifier_keyword("forward")? {
            if self.peek()?.kind == TokenKind::KwAll {
                self.advance()?;
                return Ok(FetchDirection::Forward { count: None });
            }
            if let Some(count) = self.parse_optional_signed_count()? {
                return Ok(if count < 0 {
                    FetchDirection::Scrollable
                } else {
                    FetchDirection::Forward { count: Some(count) }
                });
            }
            return Ok(FetchDirection::Forward { count: Some(1) });
        }
        // Scroll-only directions: parsed so the server can reject them
        // with 0A000 rather than a syntax error.
        if self.match_identifier_keyword("prior")?
            || self.match_identifier_keyword("first")?
            || self.match_identifier_keyword("last")?
        {
            return Ok(FetchDirection::Scrollable);
        }
        if self.match_identifier_keyword("backward")? {
            if self.peek()?.kind == TokenKind::KwAll {
                self.advance()?;
            } else {
                let _ = self.parse_optional_signed_count()?;
            }
            return Ok(FetchDirection::Scrollable);
        }
        if self.match_identifier_keyword("absolute")?
            || self.match_identifier_keyword("relative")?
        {
            let _ = self.parse_optional_signed_count()?;
            return Ok(FetchDirection::Scrollable);
        }
        // Bare [-]count.
        if let Some(count) = self.parse_optional_signed_count()? {
            return Ok(if count < 0 {
                FetchDirection::Scrollable
            } else {
                FetchDirection::Forward { count: Some(count) }
            });
        }
        // No direction clause: one row forward.
        Ok(FetchDirection::Forward { count: Some(1) })
    }

    /// Parse an optional `[-] integer` row count. Returns `Ok(None)`
    /// when the next token is not a count.
    fn parse_optional_signed_count(&mut self) -> Result<Option<i64>, ParseError> {
        let negative = if self.peek()?.kind == TokenKind::Minus {
            self.advance()?;
            true
        } else {
            false
        };
        let tok = *self.peek()?;
        if tok.kind != TokenKind::Integer {
            if negative {
                return Err(ParseError::Expected {
                    expected: "row count after '-'",
                    found: tok.kind,
                    offset: tok.span.start_usize(),
                });
            }
            return Ok(None);
        }
        self.advance()?;
        let magnitude: i64 = tok
            .text(self.source)
            .and_then(|text| text.parse().ok())
            .ok_or(ParseError::Expected {
                expected: "row count in the i64 range",
                found: TokenKind::Integer,
                offset: tok.span.start_usize(),
            })?;
        Ok(Some(if negative { -magnitude } else { magnitude }))
    }

    /// Parse `CLOSE { name | ALL }`. Assumes `CLOSE` has been consumed;
    /// `start` is its byte offset.
    pub(crate) fn parse_close_cursor(&mut self, start: u32) -> Result<Statement, ParseError> {
        if self.peek()?.kind == TokenKind::KwAll {
            let all_tok = self.advance()?;
            let span = Span::new(start, all_tok.span.end);
            return Ok(Statement::Close(CloseStmt { cursor: None, span }));
        }
        let cursor = self.parse_identifier()?;
        let span = Span::new(start, cursor.span.end);
        Ok(Statement::Close(CloseStmt {
            cursor: Some(cursor),
            span,
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::Parser;
    use crate::ast::{FetchDirection, Statement};

    fn parse_one(sql: &str) -> Statement {
        Parser::new(sql)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse {sql:?}: {e}"))
    }

    #[test]
    fn declare_plain_cursor_defaults() {
        let Statement::DeclareCursor(d) = parse_one("DECLARE c CURSOR FOR SELECT 1") else {
            panic!("expected DeclareCursor");
        };
        assert_eq!(d.name.value, "c");
        assert!(!d.binary && !d.scroll && !d.hold);
    }

    #[test]
    fn declare_accepts_full_option_surface() {
        let Statement::DeclareCursor(d) =
            parse_one("DECLARE C2 BINARY INSENSITIVE SCROLL CURSOR WITH HOLD FOR SELECT 1")
        else {
            panic!("expected DeclareCursor");
        };
        // Unquoted names case-fold, like every identifier.
        assert_eq!(d.name.value, "c2");
        assert!(d.binary && d.scroll && d.hold);

        let Statement::DeclareCursor(d) =
            parse_one("DECLARE c NO SCROLL CURSOR WITHOUT HOLD FOR SELECT 1")
        else {
            panic!("expected DeclareCursor");
        };
        assert!(!d.scroll && !d.hold);
    }

    #[test]
    fn declare_requires_select_body() {
        assert!(
            Parser::new("DECLARE c CURSOR FOR INSERT INTO t VALUES (1)")
                .parse_statement()
                .is_err(),
            "non-SELECT cursor bodies are a parse error"
        );
    }

    #[test]
    fn fetch_direction_forms() {
        for (sql, expected) in [
            ("FETCH c", FetchDirection::Forward { count: Some(1) }),
            (
                "FETCH NEXT FROM c",
                FetchDirection::Forward { count: Some(1) },
            ),
            ("FETCH 5 FROM c", FetchDirection::Forward { count: Some(5) }),
            (
                "FETCH FORWARD 5 IN c",
                FetchDirection::Forward { count: Some(5) },
            ),
            (
                "FETCH FORWARD FROM c",
                FetchDirection::Forward { count: Some(1) },
            ),
            ("FETCH ALL FROM c", FetchDirection::Forward { count: None }),
            (
                "FETCH FORWARD ALL FROM c",
                FetchDirection::Forward { count: None },
            ),
            ("FETCH PRIOR FROM c", FetchDirection::Scrollable),
            ("FETCH BACKWARD 3 FROM c", FetchDirection::Scrollable),
            ("FETCH ABSOLUTE 7 FROM c", FetchDirection::Scrollable),
            ("FETCH -2 FROM c", FetchDirection::Scrollable),
        ] {
            let Statement::Fetch(f) = parse_one(sql) else {
                panic!("expected Fetch for {sql:?}");
            };
            assert_eq!(f.direction, expected, "direction for {sql:?}");
            assert_eq!(f.cursor.value, "c", "cursor name for {sql:?}");
            assert!(!f.is_move);
        }
    }

    #[test]
    fn move_parses_as_fetch_twin() {
        let Statement::Fetch(f) = parse_one("MOVE FORWARD 5 FROM c") else {
            panic!("expected Fetch (MOVE)");
        };
        assert!(f.is_move);
        assert_eq!(f.direction, FetchDirection::Forward { count: Some(5) });
    }

    #[test]
    fn close_name_and_all() {
        let Statement::Close(c) = parse_one("CLOSE c") else {
            panic!("expected Close");
        };
        assert_eq!(c.cursor.as_ref().map(|i| i.value.as_str()), Some("c"));

        let Statement::Close(c) = parse_one("CLOSE ALL") else {
            panic!("expected Close");
        };
        assert!(c.cursor.is_none());
    }

    #[test]
    fn cursor_words_stay_usable_as_identifiers() {
        // `close`, `declare`, and `move` are dispatched on identifier
        // text, so they remain valid as ordinary object names.
        let Statement::Select(_) = parse_one("SELECT close FROM declare") else {
            panic!("expected Select");
        };
    }
}
