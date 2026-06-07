//! Parser methods for `EXPLAIN` statements.
//!
//! Syntax supported:
//!
//! ```sql
//! EXPLAIN [ANALYZE] [VERBOSE] [(FORMAT { TEXT | JSON })] statement
//! ```
//!
//! The inner statement can be any DML/DQL statement (`SELECT`, `INSERT`,
//! `UPDATE`, `DELETE`). Parsing is delegated back to `parse_one` so
//! the full grammar is available.

use crate::ast::{ExplainFormat, ExplainStmt, Statement};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse `EXPLAIN [ANALYZE] [VERBOSE] [(FORMAT TEXT|JSON)] stmt`.
    ///
    /// Assumes the `EXPLAIN` keyword has already been consumed by the
    /// dispatcher in `parse_one`. `start` is the byte offset of the
    /// `EXPLAIN` token.
    pub(crate) fn parse_explain(&mut self, start: u32) -> Result<Statement, ParseError> {
        // ANALYZE
        let analyze = self.match_kw(TokenKind::KwAnalyze);

        // VERBOSE
        let verbose = self.match_kw(TokenKind::KwVerbose);

        // Optional option list: `(FORMAT TEXT|JSON)`.
        let mut format = ExplainFormat::Text;
        if self.peek()?.kind == TokenKind::LParen {
            self.advance()?; // (
            // Parse comma-separated options — only FORMAT is supported.
            loop {
                let opt_tok = self.advance()?;
                match opt_tok.kind {
                    TokenKind::KwFormat => {
                        let fmt_tok = self.advance()?;
                        format = match fmt_tok.kind {
                            TokenKind::KwJson => ExplainFormat::Json,
                            // TEXT or bare identifier "text"
                            _ if fmt_tok
                                .text(self.source)
                                .is_some_and(|t| t.eq_ignore_ascii_case("text")) =>
                            {
                                ExplainFormat::Text
                            }
                            other => {
                                return Err(ParseError::Expected {
                                    expected: "TEXT or JSON",
                                    found: other,
                                    offset: fmt_tok.span.start_usize(),
                                });
                            }
                        };
                    }
                    other => {
                        return Err(ParseError::Expected {
                            expected: "FORMAT",
                            found: other,
                            offset: opt_tok.span.start_usize(),
                        });
                    }
                }
                if self.peek()?.kind != TokenKind::Comma {
                    break;
                }
                self.advance()?; // ,
            }
            self.expect(TokenKind::RParen, ")")?;
        }

        // Inner statement.
        let statement = self.parse_one()?;
        let end = statement.span().end;

        Ok(Statement::Explain(Box::new(ExplainStmt {
            analyze,
            verbose,
            format,
            statement: Box::new(statement),
            span: Span::new(start, end),
        })))
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::{ExplainFormat, Statement};
    use crate::parser::Parser;

    fn parse(src: &str) -> Statement {
        Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
    }

    #[test]
    fn explain_select() {
        let stmt = parse("EXPLAIN SELECT * FROM t");
        let Statement::Explain(inner) = stmt else {
            panic!()
        };
        assert!(!inner.analyze);
        assert!(!inner.verbose);
        assert_eq!(inner.format, ExplainFormat::Text);
    }

    #[test]
    fn explain_analyze() {
        let stmt = parse("EXPLAIN ANALYZE SELECT id FROM users WHERE id = 1");
        let Statement::Explain(inner) = stmt else {
            panic!()
        };
        assert!(inner.analyze);
    }

    #[test]
    fn explain_verbose() {
        let stmt = parse("EXPLAIN VERBOSE SELECT id FROM users");
        let Statement::Explain(inner) = stmt else {
            panic!()
        };
        assert!(inner.verbose);
    }

    #[test]
    fn explain_format_json() {
        let stmt = parse("EXPLAIN (FORMAT JSON) SELECT * FROM t");
        let Statement::Explain(inner) = stmt else {
            panic!()
        };
        assert_eq!(inner.format, ExplainFormat::Json);
    }

    #[test]
    fn explain_analyze_verbose() {
        let stmt = parse("EXPLAIN ANALYZE VERBOSE SELECT 1");
        let Statement::Explain(inner) = stmt else {
            panic!()
        };
        assert!(inner.analyze);
        assert!(inner.verbose);
    }

    #[test]
    fn explain_inner_statement_is_select() {
        let stmt = parse("EXPLAIN SELECT id FROM t WHERE id > 5");
        let Statement::Explain(inner) = stmt else {
            panic!()
        };
        assert!(matches!(*inner.statement, Statement::Select(_)));
    }
}
