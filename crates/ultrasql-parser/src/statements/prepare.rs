//! Parser methods for `PREPARE` / `EXECUTE` / `DEALLOCATE` statements.
//!
//! These three statements support server-side prepared statements.
//!
//! ```sql
//! PREPARE name [ ( type [, …] ) ] AS statement
//! EXECUTE name [ ( arg [, …] ) ]
//! DEALLOCATE [ PREPARE ] { ALL | name }
//! ```

use crate::ast::{DeallocateStmt, ExecuteStmt, PrepareStmt, Statement};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse `PREPARE name [(type, …)] AS stmt`.
    ///
    /// Assumes the `PREPARE` keyword has already been consumed. `start` is
    /// its byte offset.
    pub(crate) fn parse_prepare(&mut self, start: u32) -> Result<Statement, ParseError> {
        // `PREPARE TRANSACTION 'gid'` — phase 1 of two-phase commit.
        // Disambiguate from `PREPARE name [(types)] AS stmt` on the
        // next token.
        if self.peek()?.kind == TokenKind::KwTransaction {
            self.advance()?; // TRANSACTION
            let gid_tok = self.advance()?;
            let gid = match gid_tok.kind {
                TokenKind::String => gid_tok
                    .text(self.source)
                    .unwrap_or("")
                    .trim_matches('\'')
                    .to_owned(),
                other => {
                    return Err(ParseError::Expected {
                        expected: "string literal containing the global transaction id",
                        found: other,
                        offset: gid_tok.span.start as usize,
                    });
                }
            };
            let span = Span::new(start, gid_tok.span.end);
            return Ok(Statement::PrepareTransaction { gid, span });
        }
        let name = self.parse_identifier()?;

        // Optional parameter-type list.
        let param_types = if self.peek()?.kind == TokenKind::LParen {
            self.advance()?; // (
            let mut types = Vec::new();
            loop {
                types.push(self.parse_type_name()?);
                if self.peek()?.kind != TokenKind::Comma {
                    break;
                }
                self.advance()?; // ,
            }
            self.expect(TokenKind::RParen, ")")?;
            types
        } else {
            Vec::new()
        };

        self.expect(TokenKind::KwAs, "AS")?;
        let statement = self.parse_one()?;
        let end = statement.span().end;

        Ok(Statement::Prepare(Box::new(PrepareStmt {
            name,
            param_types,
            statement: Box::new(statement),
            span: Span::new(start, end),
        })))
    }

    /// Parse `COMMIT PREPARED 'gid'` — phase 2 of two-phase commit.
    ///
    /// Assumes `COMMIT` has been consumed and the next token is
    /// `PREPARED`. `start` is the byte offset of `COMMIT`.
    pub(crate) fn parse_commit_prepared(&mut self, start: u32) -> Result<Statement, ParseError> {
        let _ = self.advance()?; // PREPARED
        let gid_tok = self.advance()?;
        let gid = match gid_tok.kind {
            TokenKind::String => gid_tok
                .text(self.source)
                .unwrap_or("")
                .trim_matches('\'')
                .to_owned(),
            other => {
                return Err(ParseError::Expected {
                    expected: "string literal containing the global transaction id",
                    found: other,
                    offset: gid_tok.span.start as usize,
                });
            }
        };
        let span = Span::new(start, gid_tok.span.end);
        Ok(Statement::CommitPrepared { gid, span })
    }

    /// Parse `ROLLBACK PREPARED 'gid'` — phase 2 abort of 2PC.
    ///
    /// Assumes `ROLLBACK` has been consumed and the next token is
    /// `PREPARED`. `start` is the byte offset of `ROLLBACK`.
    pub(crate) fn parse_rollback_prepared(&mut self, start: u32) -> Result<Statement, ParseError> {
        let _ = self.advance()?; // PREPARED
        let gid_tok = self.advance()?;
        let gid = match gid_tok.kind {
            TokenKind::String => gid_tok
                .text(self.source)
                .unwrap_or("")
                .trim_matches('\'')
                .to_owned(),
            other => {
                return Err(ParseError::Expected {
                    expected: "string literal containing the global transaction id",
                    found: other,
                    offset: gid_tok.span.start as usize,
                });
            }
        };
        let span = Span::new(start, gid_tok.span.end);
        Ok(Statement::RollbackPrepared { gid, span })
    }

    /// Parse `EXECUTE name [(arg, …)]`.
    ///
    /// Assumes the `EXECUTE` keyword has already been consumed. `start` is
    /// its byte offset.
    pub(crate) fn parse_execute(&mut self, start: u32) -> Result<Statement, ParseError> {
        let name = self.parse_identifier()?;

        let args = if self.peek()?.kind == TokenKind::LParen {
            self.advance()?; // (
            if self.peek()?.kind == TokenKind::RParen {
                self.advance()?; // )
                Vec::new()
            } else {
                let mut args = Vec::new();
                loop {
                    args.push(self.parse_expr()?);
                    if self.peek()?.kind != TokenKind::Comma {
                        break;
                    }
                    self.advance()?; // ,
                }
                self.expect(TokenKind::RParen, ")")?;
                args
            }
        } else {
            Vec::new()
        };

        let end = args.last().map_or(name.span.end, |e| e.span().end);
        let span = Span::new(start, end);

        Ok(Statement::Execute(ExecuteStmt { name, args, span }))
    }

    /// Parse `DEALLOCATE [PREPARE] { ALL | name }`.
    ///
    /// Assumes the `DEALLOCATE` keyword has already been consumed. `start` is
    /// its byte offset.
    pub(crate) fn parse_deallocate(&mut self, start: u32) -> Result<Statement, ParseError> {
        // Optional PREPARE keyword.
        self.match_kw(TokenKind::KwPrepare);

        if self.match_kw(TokenKind::KwAll) {
            // DEALLOCATE ALL
            let span = Span::new(start, self.peek()?.span.start);
            return Ok(Statement::Deallocate(DeallocateStmt {
                name: None,
                all: true,
                span,
            }));
        }

        let name = self.parse_identifier()?;
        let span = Span::new(start, name.span.end);
        Ok(Statement::Deallocate(DeallocateStmt {
            name: Some(name),
            all: false,
            span,
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::{DeallocateStmt, ExecuteStmt, Statement};
    use crate::parser::Parser;

    fn parse(src: &str) -> Statement {
        Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
    }

    #[test]
    fn prepare_no_params() {
        let stmt = parse("PREPARE my_plan AS SELECT * FROM t WHERE id = $1");
        let Statement::Prepare(inner) = stmt else {
            panic!()
        };
        assert_eq!(inner.name.value, "my_plan");
        assert!(inner.param_types.is_empty());
    }

    #[test]
    fn prepare_with_param_types() {
        let stmt = parse("PREPARE my_plan (integer, text) AS SELECT * FROM t WHERE id = $1");
        let Statement::Prepare(inner) = stmt else {
            panic!()
        };
        assert_eq!(inner.param_types.len(), 2);
        assert_eq!(inner.param_types[0].value, "integer");
        assert_eq!(inner.param_types[1].value, "text");
    }

    #[test]
    fn execute_no_args() {
        let stmt = parse("EXECUTE my_plan");
        let Statement::Execute(ExecuteStmt { name, args, .. }) = stmt else {
            panic!()
        };
        assert_eq!(name.value, "my_plan");
        assert!(args.is_empty());
    }

    #[test]
    fn execute_with_args() {
        let stmt = parse("EXECUTE my_plan (42, 'hello')");
        let Statement::Execute(ExecuteStmt { args, .. }) = stmt else {
            panic!()
        };
        assert_eq!(args.len(), 2);
    }

    #[test]
    fn deallocate_by_name() {
        let stmt = parse("DEALLOCATE my_plan");
        let Statement::Deallocate(DeallocateStmt { name, all, .. }) = stmt else {
            panic!()
        };
        assert!(!all);
        assert_eq!(name.unwrap().value, "my_plan");
    }

    #[test]
    fn deallocate_all() {
        let stmt = parse("DEALLOCATE ALL");
        let Statement::Deallocate(DeallocateStmt { name, all, .. }) = stmt else {
            panic!()
        };
        assert!(all);
        assert!(name.is_none());
    }

    #[test]
    fn deallocate_prepare_name() {
        let stmt = parse("DEALLOCATE PREPARE my_plan");
        let Statement::Deallocate(DeallocateStmt { name, all, .. }) = stmt else {
            panic!()
        };
        assert!(!all);
        assert_eq!(name.unwrap().value, "my_plan");
    }
}
