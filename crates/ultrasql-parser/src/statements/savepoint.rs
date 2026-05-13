//! Parser methods for savepoint management statements.
//!
//! Covers three forms:
//!
//! * `SAVEPOINT name`
//! * `ROLLBACK TO [SAVEPOINT] name`
//! * `RELEASE [SAVEPOINT] name`
//!
//! These are part of the v0.2 Parser completeness milestone. The execution
//! side (subtransaction tracking) lands in v0.4.

use crate::ast::{ReleaseSavepointStmt, RollbackToSavepointStmt, SavepointStmt, Statement};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse `SAVEPOINT name`.
    ///
    /// Assumes the `SAVEPOINT` keyword has already been consumed by the
    /// dispatcher in `parse_one`.
    pub(crate) fn parse_savepoint(&mut self, start: u32) -> Result<Statement, ParseError> {
        let name = self.parse_identifier()?;
        let span = Span::new(start, name.span.end);
        Ok(Statement::Savepoint(SavepointStmt { name, span }))
    }

    /// Parse `TO [SAVEPOINT] name` — the sub-clause after `ROLLBACK TO`.
    ///
    /// Called from the `ROLLBACK` arm of `parse_one` after `TO` has been
    /// confirmed. The optional `SAVEPOINT` keyword after `TO` is consumed
    /// if present.
    pub(crate) fn parse_rollback_to_savepoint(
        &mut self,
        rollback_start: u32,
    ) -> Result<Statement, ParseError> {
        self.expect(TokenKind::KwTo, "TO")?;
        // SAVEPOINT keyword is optional per PostgreSQL syntax.
        self.match_kw(TokenKind::KwSavepoint);
        let name = self.parse_identifier()?;
        let span = Span::new(rollback_start, name.span.end);
        Ok(Statement::RollbackToSavepoint(RollbackToSavepointStmt {
            name,
            span,
        }))
    }

    /// Parse `RELEASE [SAVEPOINT] name`.
    ///
    /// Assumes the `RELEASE` keyword has already been consumed by the
    /// dispatcher in `parse_one`.
    pub(crate) fn parse_release_savepoint(&mut self, start: u32) -> Result<Statement, ParseError> {
        // SAVEPOINT keyword is optional per PostgreSQL syntax.
        self.match_kw(TokenKind::KwSavepoint);
        let name = self.parse_identifier()?;
        let span = Span::new(start, name.span.end);
        Ok(Statement::ReleaseSavepoint(ReleaseSavepointStmt {
            name,
            span,
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::{ReleaseSavepointStmt, RollbackToSavepointStmt, SavepointStmt, Statement};
    use crate::parser::Parser;

    fn parse(src: &str) -> Statement {
        Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
    }

    #[test]
    fn savepoint_basic() {
        let stmt = parse("SAVEPOINT my_sp");
        let Statement::Savepoint(SavepointStmt { name, .. }) = stmt else {
            panic!()
        };
        assert_eq!(name.value, "my_sp");
    }

    #[test]
    fn rollback_to_savepoint_with_keyword() {
        let stmt = parse("ROLLBACK TO SAVEPOINT my_sp");
        let Statement::RollbackToSavepoint(RollbackToSavepointStmt { name, .. }) = stmt else {
            panic!()
        };
        assert_eq!(name.value, "my_sp");
    }

    #[test]
    fn rollback_to_savepoint_without_keyword() {
        let stmt = parse("ROLLBACK TO my_sp");
        let Statement::RollbackToSavepoint(RollbackToSavepointStmt { name, .. }) = stmt else {
            panic!()
        };
        assert_eq!(name.value, "my_sp");
    }

    #[test]
    fn release_savepoint_with_keyword() {
        let stmt = parse("RELEASE SAVEPOINT my_sp");
        let Statement::ReleaseSavepoint(ReleaseSavepointStmt { name, .. }) = stmt else {
            panic!()
        };
        assert_eq!(name.value, "my_sp");
    }

    #[test]
    fn release_savepoint_without_keyword() {
        let stmt = parse("RELEASE my_sp");
        let Statement::ReleaseSavepoint(ReleaseSavepointStmt { name, .. }) = stmt else {
            panic!()
        };
        assert_eq!(name.value, "my_sp");
    }

    #[test]
    fn savepoint_then_rollback_sequence() {
        let mut p = Parser::new("SAVEPOINT sp1; ROLLBACK TO sp1; RELEASE sp1");
        let stmts = p.parse_statements().expect("should parse");
        assert_eq!(stmts.len(), 3);
        assert!(matches!(stmts[0], Statement::Savepoint(_)));
        assert!(matches!(stmts[1], Statement::RollbackToSavepoint(_)));
        assert!(matches!(stmts[2], Statement::ReleaseSavepoint(_)));
    }
}
