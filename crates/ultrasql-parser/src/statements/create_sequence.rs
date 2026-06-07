//! Parser methods for `CREATE SEQUENCE`, `ALTER SEQUENCE`, and
//! `DROP SEQUENCE` statements.
//!
//! Handles:
//! - `CREATE SEQUENCE [IF NOT EXISTS] name [options]`
//! - `ALTER SEQUENCE name [options]`
//! - `DROP SEQUENCE [IF EXISTS] name [, …] [CASCADE|RESTRICT]`
//!
//! Sequence options: `START [WITH] n`, `INCREMENT [BY] n`,
//! `MINVALUE n | NO MINVALUE`, `MAXVALUE n | NO MAXVALUE`,
//! `RESTART [[WITH] n]`, `CACHE n`, `CYCLE | NO CYCLE`.

use crate::ast::{AlterSequenceStmt, CreateSequenceStmt, DropSequenceStmt, SequenceOption};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse `CREATE SEQUENCE …`, consuming the `SEQUENCE` keyword.
    ///
    /// The `CREATE` keyword must already have been consumed by the caller.
    pub(crate) fn parse_create_sequence(
        &mut self,
        create_start: u32,
    ) -> Result<CreateSequenceStmt, ParseError> {
        self.expect(TokenKind::KwSequence, "SEQUENCE")?;
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_object_name()?;
        let options = self.parse_sequence_options()?;
        let end = self.peek()?.span.start;
        Ok(CreateSequenceStmt {
            if_not_exists,
            name,
            options,
            span: Span::new(create_start, end),
        })
    }

    /// Parse `ALTER SEQUENCE …`, consuming the `SEQUENCE` keyword.
    ///
    /// The `ALTER` keyword must already have been consumed by the caller.
    pub(crate) fn parse_alter_sequence(
        &mut self,
        alter_start: u32,
    ) -> Result<AlterSequenceStmt, ParseError> {
        self.expect(TokenKind::KwSequence, "SEQUENCE")?;
        let name = self.parse_object_name()?;
        let options = self.parse_sequence_options()?;
        let end = self.peek()?.span.start;
        Ok(AlterSequenceStmt {
            name,
            options,
            span: Span::new(alter_start, end),
        })
    }

    /// Parse `DROP SEQUENCE …`, consuming the `SEQUENCE` keyword.
    ///
    /// The `DROP` keyword must already have been consumed by the caller.
    pub(crate) fn parse_drop_sequence(
        &mut self,
        drop_start: u32,
    ) -> Result<DropSequenceStmt, ParseError> {
        self.expect(TokenKind::KwSequence, "SEQUENCE")?;
        let if_exists = self.parse_if_exists()?;
        let names = self.parse_object_name_list()?;
        let cascade = self.parse_cascade_restrict();
        let end = self.peek()?.span.start;
        Ok(DropSequenceStmt {
            if_exists,
            names,
            cascade,
            span: Span::new(drop_start, end),
        })
    }

    /// Parse zero or more sequence option clauses.
    pub(crate) fn parse_sequence_options(&mut self) -> Result<Vec<SequenceOption>, ParseError> {
        let mut opts = Vec::new();
        loop {
            match self.peek()?.kind {
                TokenKind::KwStart => {
                    self.advance()?; // START
                    self.match_kw(TokenKind::KwWith); // optional WITH
                    opts.push(SequenceOption::Start(self.parse_signed_integer()?));
                }
                TokenKind::KwRestart => {
                    self.advance()?; // RESTART
                    let had_with = self.match_kw(TokenKind::KwWith);
                    let value = if had_with
                        || matches!(self.peek()?.kind, TokenKind::Integer | TokenKind::Minus)
                    {
                        Some(self.parse_signed_integer()?)
                    } else {
                        None
                    };
                    opts.push(SequenceOption::Restart(value));
                }
                TokenKind::KwIncrement => {
                    self.advance()?; // INCREMENT
                    self.match_kw(TokenKind::KwBy); // optional BY
                    opts.push(SequenceOption::Increment(self.parse_signed_integer()?));
                }
                TokenKind::KwMinvalue => {
                    self.advance()?; // MINVALUE
                    opts.push(SequenceOption::MinValue(Some(self.parse_signed_integer()?)));
                }
                TokenKind::KwMaxvalue => {
                    self.advance()?; // MAXVALUE
                    opts.push(SequenceOption::MaxValue(Some(self.parse_signed_integer()?)));
                }
                TokenKind::KwNo => {
                    self.advance()?; // NO
                    match self.peek()?.kind {
                        TokenKind::KwMinvalue => {
                            self.advance()?;
                            opts.push(SequenceOption::MinValue(None));
                        }
                        TokenKind::KwMaxvalue => {
                            self.advance()?;
                            opts.push(SequenceOption::MaxValue(None));
                        }
                        TokenKind::KwCycle => {
                            self.advance()?;
                            opts.push(SequenceOption::Cycle(false));
                        }
                        other => {
                            return Err(ParseError::Expected {
                                expected: "MINVALUE, MAXVALUE, or CYCLE after NO",
                                found: other,
                                offset: self.peek()?.span.start_usize(),
                            });
                        }
                    }
                }
                TokenKind::KwCache => {
                    self.advance()?; // CACHE
                    let n = self.parse_unsigned_integer()?;
                    opts.push(SequenceOption::Cache(n));
                }
                TokenKind::KwCycle => {
                    self.advance()?; // CYCLE
                    opts.push(SequenceOption::Cycle(true));
                }
                _ => break,
            }
        }
        Ok(opts)
    }

    /// Parse a signed integer literal (optionally preceded by `-`).
    fn parse_signed_integer(&mut self) -> Result<i64, ParseError> {
        let negative = self.peek()?.kind == TokenKind::Minus;
        if negative {
            self.advance()?;
        }
        let tok = self.peek()?;
        if tok.kind != TokenKind::Integer {
            return Err(ParseError::Expected {
                expected: "integer",
                found: tok.kind,
                offset: tok.span.start_usize(),
            });
        }
        let t = self.advance()?;
        let text = t.text(self.source).unwrap_or("0");
        let n: i64 = text.parse().map_err(|_| ParseError::InvalidInteger {
            text: text.to_owned(),
            offset: t.span.start_usize(),
        })?;
        Ok(if negative { -n } else { n })
    }

    /// Parse an unsigned integer literal.
    fn parse_unsigned_integer(&mut self) -> Result<u64, ParseError> {
        let tok = self.peek()?;
        if tok.kind != TokenKind::Integer {
            return Err(ParseError::Expected {
                expected: "integer",
                found: tok.kind,
                offset: tok.span.start_usize(),
            });
        }
        let t = self.advance()?;
        let text = t.text(self.source).unwrap_or("0");
        let n: u64 = text.parse().map_err(|_| ParseError::InvalidInteger {
            text: text.to_owned(),
            offset: t.span.start_usize(),
        })?;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{SequenceOption, Statement};
    use crate::parser::Parser;

    fn parse_create_seq(src: &str) -> CreateSequenceStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::CreateSequence(s) => *s,
            other => panic!("expected CreateSequence, got {other:?}"),
        }
    }

    fn parse_alter_seq(src: &str) -> AlterSequenceStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::AlterSequence(s) => *s,
            other => panic!("expected AlterSequence, got {other:?}"),
        }
    }

    fn parse_drop_seq(src: &str) -> DropSequenceStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::DropSequence(s) => s,
            other => panic!("expected DropSequence, got {other:?}"),
        }
    }

    // ---- happy-path -------------------------------------------------------

    #[test]
    fn create_sequence_basic() {
        let stmt = parse_create_seq("CREATE SEQUENCE myseq");
        assert_eq!(stmt.name.to_string(), "myseq");
        assert!(!stmt.if_not_exists);
        assert!(stmt.options.is_empty());
    }

    #[test]
    fn create_sequence_all_options() {
        let stmt = parse_create_seq(
            "CREATE SEQUENCE seq START WITH 10 INCREMENT BY 5 MINVALUE 1 MAXVALUE 1000 CACHE 20 CYCLE",
        );
        assert_eq!(stmt.options.len(), 6);
        assert!(matches!(stmt.options[0], SequenceOption::Start(10)));
        assert!(matches!(stmt.options[1], SequenceOption::Increment(5)));
        assert!(matches!(stmt.options[2], SequenceOption::MinValue(Some(1))));
        assert!(matches!(
            stmt.options[3],
            SequenceOption::MaxValue(Some(1000))
        ));
        assert!(matches!(stmt.options[4], SequenceOption::Cache(20)));
        assert!(matches!(stmt.options[5], SequenceOption::Cycle(true)));
    }

    #[test]
    fn create_sequence_no_minvalue_no_cycle() {
        let stmt = parse_create_seq("CREATE SEQUENCE s NO MINVALUE NO MAXVALUE NO CYCLE");
        assert!(matches!(stmt.options[0], SequenceOption::MinValue(None)));
        assert!(matches!(stmt.options[1], SequenceOption::MaxValue(None)));
        assert!(matches!(stmt.options[2], SequenceOption::Cycle(false)));
    }

    #[test]
    fn alter_sequence_restart() {
        let stmt = parse_alter_seq("ALTER SEQUENCE myseq RESTART WITH 1 INCREMENT BY 1");
        assert_eq!(stmt.name.to_string(), "myseq");
        assert_eq!(stmt.options.len(), 2);
        assert!(matches!(stmt.options[0], SequenceOption::Restart(Some(1))));
    }

    #[test]
    fn alter_sequence_restart_without_value() {
        let stmt = parse_alter_seq("ALTER SEQUENCE myseq RESTART");
        assert_eq!(stmt.options.len(), 1);
        assert!(matches!(stmt.options[0], SequenceOption::Restart(None)));
    }

    #[test]
    fn drop_sequence_if_exists_cascade() {
        let stmt = parse_drop_seq("DROP SEQUENCE IF EXISTS s1, s2 CASCADE");
        assert!(stmt.if_exists);
        assert_eq!(stmt.names.len(), 2);
        assert!(stmt.cascade);
    }

    // ---- negative case ----------------------------------------------------

    #[test]
    fn create_sequence_no_unknown_errors() {
        let err = Parser::new("CREATE SEQUENCE s NO COLUMN")
            .parse_statement()
            .unwrap_err();
        assert!(matches!(err, ParseError::Expected { .. }));
    }
}
