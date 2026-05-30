//! Recursive-descent + Pratt-style SQL parser.
//!
//! The parser consumes tokens from a [`Lexer`] and produces a
//! [`Statement`] tree. Statement-level structure is parsed by
//! recursive descent (one function per non-terminal); expressions go
//! through a Pratt parser keyed off [`crate::ast::BinaryOp::precedence`]
//! so adding a new operator costs one match-arm.
//!
//! The parser keeps a one-token lookahead via a buffered next-token
//! function. On EOF, every grammar rule that requires a terminator
//! either succeeds with the so-far-built node or reports a tagged
//! error.
//!
//! # Module layout
//!
//! - This file holds the [`Parser`] struct, its top-level statement
//!   dispatcher `Parser::parse_one`, the CREATE/DROP/ALTER routing,
//!   the token buffering helpers (`Parser::peek`, `Parser::advance`,
//!   …) and the recursion-depth guard.
//! - `expr` holds the Pratt expression parser proper
//!   (`parse_expr_with_precedence`, `parse_prefix`, identifier/call
//!   parsing, `CAST`, type names).
//! - `expr_postfix` holds postfix operators that decorate an already-
//!   parsed expression: `BETWEEN`, `IS`, array subscript / slice,
//!   `AT TIME ZONE`.
//! - `expr_special` holds the syntactic-shape expressions whose
//!   first token is a keyword: `CASE`, `COALESCE`, `NULLIF`,
//!   `GREATEST`/`LEAST`, `ROW`.
//! - `binary_ops` holds the binary-operator detection helpers used
//!   by the Pratt loop (`peek_binary_op` and `consume_binary_op`).

use crate::ast::{
    CreatePolicyStmt, Expr, PolicyCommand, PolicyPermissiveness, RoleStmtKind, Statement,
};
use crate::lexer::{Lexer, LexerError};
use crate::token::{Token, TokenKind};

mod binary_ops;
mod expr;
mod expr_postfix;
mod expr_special;

#[cfg(test)]
mod tests;

/// Errors surfaced by the parser.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ParseError {
    /// The lexer rejected the input before parsing completed.
    #[error("lexer error: {0}")]
    Lex(#[from] LexerError),

    /// An expected token was missing.
    #[error("expected {expected} at offset {offset}, found {found:?}")]
    Expected {
        /// What the parser was looking for.
        expected: &'static str,
        /// What it saw.
        found: TokenKind,
        /// Byte offset of the encountered token.
        offset: usize,
    },

    /// The parser reached EOF while it was still expecting tokens.
    #[error("unexpected end of input; expected {expected}")]
    UnexpectedEof {
        /// What the parser was looking for.
        expected: &'static str,
    },

    /// An integer literal could not be parsed.
    #[error("invalid integer literal '{text}' at offset {offset}")]
    InvalidInteger {
        /// Original text.
        text: String,
        /// Byte offset.
        offset: usize,
    },

    /// A parameter (`$N`) referenced a number that does not fit `u32`.
    #[error("parameter {text} at offset {offset} out of range")]
    ParameterOutOfRange {
        /// Original text.
        text: String,
        /// Byte offset.
        offset: usize,
    },

    /// A construct that is not yet supported.
    #[error("unsupported syntax at offset {offset}: {what}")]
    Unsupported {
        /// What the parser refused.
        what: &'static str,
        /// Byte offset.
        offset: usize,
    },

    /// Expression nesting exceeded the configured depth limit. The
    /// bound guards the call stack against adversarial inputs such as
    /// `((((((...))))))` that would otherwise blow the stack on the
    /// server before any other validation runs.
    #[error("expression depth exceeded {limit} at offset {offset}")]
    DepthExceeded {
        /// Configured maximum nesting depth.
        limit: u32,
        /// Byte offset of the deepest token reached.
        offset: usize,
    },
}

/// Maximum recursive expression depth accepted by the parser.
///
/// The bound is a fixed-size circuit breaker against adversarial inputs
/// (a million nested parentheses would otherwise crash the server
/// before any other validation runs); legitimate SQL never approaches
/// this depth.
///
/// The value is intentionally conservative: the expression parser supports
/// many constructs (CASE, BETWEEN, postfix casts, etc.) so each nesting level
/// consumes a larger stack frame, especially under sanitizers. 128 is still
/// far above any reasonable real-world SQL nesting depth and low enough for
/// the guard to fire before sanitizer-instrumented test threads exhaust stack.
pub const MAX_PARSE_DEPTH: u32 = 128;

/// SQL parser.
#[derive(Debug)]
pub struct Parser<'src> {
    pub(crate) source: &'src str,
    lexer: Lexer<'src>,
    /// One-token lookahead. `None` means "not yet read".
    peeked: Option<Token>,
    /// Current expression-recursion depth. Bounded by
    /// [`MAX_PARSE_DEPTH`]. Reset implicitly between statements because
    /// statement-level entry points construct fresh expression scopes.
    depth: u32,
}

impl<'src> Parser<'src> {
    /// Build a parser over a SQL source string.
    #[must_use]
    pub const fn new(source: &'src str) -> Self {
        Self {
            source,
            lexer: Lexer::new(source),
            peeked: None,
            depth: 0,
        }
    }

    /// Parse a single statement (with optional trailing `;`).
    pub fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        let stmt = self.parse_one()?;
        // Allow an optional trailing semicolon.
        if self.peek()?.kind == TokenKind::Semicolon {
            self.advance()?;
        }
        Ok(stmt)
    }

    /// Parse the entire input as a sequence of `;`-delimited
    /// statements. Trailing `;` is optional.
    pub fn parse_statements(&mut self) -> Result<Vec<Statement>, ParseError> {
        let mut out = Vec::new();
        loop {
            if self.peek()?.kind == TokenKind::Eof {
                return Ok(out);
            }
            let s = self.parse_one()?;
            out.push(s);
            match self.peek()?.kind {
                TokenKind::Semicolon => {
                    self.advance()?;
                }
                TokenKind::Eof => return Ok(out),
                other => {
                    return Err(ParseError::Expected {
                        expected: "';' or end of input",
                        found: other,
                        offset: self.peek()?.span.start as usize,
                    });
                }
            }
        }
    }

    /// Parse the entire input and return source slices for each
    /// `;`-delimited statement.
    ///
    /// The parser still validates each statement structurally; the
    /// returned slices preserve the original text so callers can reuse
    /// existing single-statement execution paths without ad hoc SQL
    /// splitting.
    pub fn parse_statement_slices(&mut self) -> Result<Vec<&'src str>, ParseError> {
        let mut out = Vec::new();
        loop {
            let start = self.peek()?.span.start as usize;
            if self.peek()?.kind == TokenKind::Eof {
                return Ok(out);
            }
            let _ = self.parse_one()?;
            let end = self.peek()?.span.start as usize;
            let slice = self.source[start..end].trim();
            if !slice.is_empty() {
                out.push(slice);
            }
            match self.peek()?.kind {
                TokenKind::Semicolon => {
                    self.advance()?;
                }
                TokenKind::Eof => return Ok(out),
                other => {
                    return Err(ParseError::Expected {
                        expected: "';' or end of input",
                        found: other,
                        offset: self.peek()?.span.start as usize,
                    });
                }
            }
        }
    }

    // ---------------- statement-level ------------------------------------

    pub(crate) fn parse_one(&mut self) -> Result<Statement, ParseError> {
        let head = self.peek()?;
        match head.kind {
            // SELECT or WITH … SELECT
            TokenKind::KwSelect | TokenKind::KwWith => {
                self.parse_select().map(|s| Statement::Select(Box::new(s)))
            }
            TokenKind::KwInsert => self.parse_insert().map(|s| Statement::Insert(Box::new(s))),
            TokenKind::KwUpdate => self.parse_update().map(|s| Statement::Update(Box::new(s))),
            TokenKind::KwDelete => self.parse_delete().map(|s| Statement::Delete(Box::new(s))),
            TokenKind::KwTruncate => self.parse_truncate().map(Statement::Truncate),
            TokenKind::KwBegin => {
                let tok = self.advance()?;
                // Optional TRANSACTION.
                if self.peek()?.kind == TokenKind::KwTransaction {
                    self.advance()?;
                }
                // Optional ISOLATION LEVEL {READ COMMITTED | REPEATABLE READ | SERIALIZABLE}
                let isolation_level = self.parse_opt_isolation_level()?;
                Ok(Statement::Begin {
                    isolation_level,
                    span: tok.span,
                })
            }
            TokenKind::KwCommit => {
                let tok = self.advance()?;
                if self.peek()?.kind == TokenKind::KwTransaction {
                    self.advance()?;
                }
                // COMMIT PREPARED 'gid' — phase 2 of 2PC.
                if self.peek()?.kind == TokenKind::KwPrepared {
                    return self.parse_commit_prepared(tok.span.start);
                }
                Ok(Statement::Commit { span: tok.span })
            }
            TokenKind::KwRollback => {
                let tok = self.advance()?;
                if self.peek()?.kind == TokenKind::KwTransaction {
                    self.advance()?;
                }
                // ROLLBACK PREPARED 'gid' — phase 2 abort of 2PC.
                if self.peek()?.kind == TokenKind::KwPrepared {
                    return self.parse_rollback_prepared(tok.span.start);
                }
                // ROLLBACK TO [SAVEPOINT] name
                if self.peek()?.kind == TokenKind::KwTo {
                    return self.parse_rollback_to_savepoint(tok.span.start);
                }
                Ok(Statement::Rollback { span: tok.span })
            }
            // ---- DDL --------------------------------------------------------
            TokenKind::KwCreate => self.parse_create(),
            TokenKind::KwDrop => self.parse_drop(),
            TokenKind::KwAlter => self.parse_alter(),
            TokenKind::KwComment => self.parse_comment(),
            TokenKind::KwReindex => self.parse_reindex().map(Statement::Reindex),
            TokenKind::KwSet | TokenKind::KwShow | TokenKind::KwReset => {
                let head_kind = head.kind;
                let next_kind = self.lookahead_at(1).map(|t| t.kind).ok();
                let next_is_role = self
                    .lookahead_text_eq_ignore_ascii_case(1, "role")
                    .unwrap_or(false);
                if head_kind == TokenKind::KwSet && next_kind == Some(TokenKind::KwTransaction) {
                    let set_tok = self.advance()?; // SET
                    self.advance()?; // TRANSACTION
                    let next_tok = *self.peek()?;
                    let isolation_level =
                        self.parse_opt_isolation_level()?
                            .ok_or(ParseError::Expected {
                                expected: "ISOLATION LEVEL after SET TRANSACTION",
                                found: next_tok.kind,
                                offset: next_tok.span.start as usize,
                            })?;
                    Ok(Statement::SetTransaction {
                        isolation_level,
                        span: set_tok.span,
                    })
                } else if (head_kind == TokenKind::KwSet || head_kind == TokenKind::KwReset)
                    && next_is_role
                {
                    self.parse_set_role().map(Statement::SetRole)
                } else {
                    self.parse_set_stmt().map(Statement::SetVar)
                }
            }
            // ---- Savepoints -------------------------------------------------
            TokenKind::KwSavepoint => {
                let tok = self.advance()?;
                self.parse_savepoint(tok.span.start)
            }
            TokenKind::KwRelease => {
                let tok = self.advance()?;
                self.parse_release_savepoint(tok.span.start)
            }
            // ---- EXPLAIN ----------------------------------------------------
            TokenKind::KwExplain => {
                let tok = self.advance()?;
                self.parse_explain(tok.span.start)
            }
            // ---- PREPARE / EXECUTE / DEALLOCATE ----------------------------
            TokenKind::KwPrepare => {
                let tok = self.advance()?;
                self.parse_prepare(tok.span.start)
            }
            TokenKind::KwExecute => {
                let tok = self.advance()?;
                self.parse_execute(tok.span.start)
            }
            TokenKind::KwGrant => self.parse_grant_statement(),
            TokenKind::KwRevoke => self.parse_revoke_statement(),
            TokenKind::KwDeallocate => {
                let tok = self.advance()?;
                self.parse_deallocate(tok.span.start)
            }
            // ---- LISTEN / NOTIFY / UNLISTEN --------------------------------
            TokenKind::KwListen => {
                let tok = self.advance()?;
                self.parse_listen(tok.span.start)
            }
            TokenKind::KwNotify => {
                let tok = self.advance()?;
                self.parse_notify(tok.span.start)
            }
            TokenKind::KwUnlisten => {
                let tok = self.advance()?;
                self.parse_unlisten(tok.span.start)
            }
            TokenKind::KwCopy => self.parse_copy().map(|s| Statement::Copy(Box::new(s))),
            other => Err(ParseError::Expected {
                expected: "SELECT, INSERT, UPDATE, DELETE, TRUNCATE, CREATE, DROP, ALTER, \
                           COMMENT, REINDEX, SET, SHOW, RESET, BEGIN, COMMIT, ROLLBACK, SAVEPOINT, \
                           RELEASE, EXPLAIN, PREPARE, EXECUTE, GRANT, REVOKE, DEALLOCATE, \
                           LISTEN, NOTIFY, UNLISTEN, or COPY",
                found: other,
                offset: head.span.start as usize,
            }),
        }
    }

    /// Dispatch `CREATE …` to the appropriate sub-parser based on the
    /// keyword that follows `CREATE`.
    fn parse_create(&mut self) -> Result<Statement, ParseError> {
        let create_tok = self.advance()?; // CREATE
        let start = create_tok.span.start;

        let tok = *self.peek()?;
        match tok.kind {
            TokenKind::KwTable => self.parse_create_table(start),
            TokenKind::KwSchema => self.parse_create_schema(start).map(Statement::CreateSchema),
            TokenKind::Identifier
                if tok
                    .text(self.source)
                    .is_some_and(|text| text.eq_ignore_ascii_case("type")) =>
            {
                self.parse_create_type(start)
                    .map(|s| Statement::CreateType(Box::new(s)))
            }
            TokenKind::Identifier
                if tok
                    .text(self.source)
                    .is_some_and(|text| text.eq_ignore_ascii_case("domain")) =>
            {
                self.parse_create_domain(start)
                    .map(|s| Statement::CreateDomain(Box::new(s)))
            }
            TokenKind::Identifier
                if tok
                    .text(self.source)
                    .is_some_and(|text| text.eq_ignore_ascii_case("materialized")) =>
            {
                self.parse_create_materialized_view(start)
                    .map(|s| Statement::CreateMaterializedView(Box::new(s)))
            }
            TokenKind::KwIndex => self
                .parse_create_index(start, false, false)
                .map(|s| Statement::CreateIndex(Box::new(s))),
            TokenKind::KwUnique => {
                // CREATE UNIQUE INDEX …
                self.advance()?; // UNIQUE
                self.parse_create_index(start, true, false)
                    .map(|s| Statement::CreateIndex(Box::new(s)))
            }
            TokenKind::Identifier
                if tok
                    .text(self.source)
                    .is_some_and(|text| text.eq_ignore_ascii_case("aggregating")) =>
            {
                self.advance()?; // AGGREGATING
                self.parse_create_index(start, false, true)
                    .map(|s| Statement::CreateIndex(Box::new(s)))
            }
            TokenKind::Identifier
                if tok
                    .text(self.source)
                    .is_some_and(|text| text.eq_ignore_ascii_case("policy")) =>
            {
                self.parse_create_policy(start)
                    .map(|s| Statement::CreatePolicy(Box::new(s)))
            }
            TokenKind::Identifier
                if tok
                    .text(self.source)
                    .is_some_and(|text| text.eq_ignore_ascii_case("role")) =>
            {
                self.parse_create_role(start, RoleStmtKind::Role)
                    .map(|s| Statement::CreateRole(Box::new(s)))
            }
            TokenKind::Identifier
                if tok
                    .text(self.source)
                    .is_some_and(|text| text.eq_ignore_ascii_case("user")) =>
            {
                self.parse_create_role(start, RoleStmtKind::User)
                    .map(|s| Statement::CreateRole(Box::new(s)))
            }
            TokenKind::KwSequence => self
                .parse_create_sequence(start)
                .map(|s| Statement::CreateSequence(Box::new(s))),
            other => Err(ParseError::Expected {
                expected: "TABLE, TYPE, DOMAIN, MATERIALIZED VIEW, SCHEMA, INDEX, UNIQUE, AGGREGATING, POLICY, ROLE, USER, or SEQUENCE after CREATE",
                found: other,
                offset: tok.span.start as usize,
            }),
        }
    }

    fn parse_create_policy(&mut self, create_start: u32) -> Result<CreatePolicyStmt, ParseError> {
        self.expect_identifier_keyword("policy", "POLICY")?;
        let name = self.parse_identifier()?;
        self.expect(TokenKind::KwOn, "ON")?;
        let table = self.parse_object_name()?;
        let permissiveness = if self.match_kw(TokenKind::KwAs) {
            self.parse_policy_permissiveness()?
        } else {
            PolicyPermissiveness::Permissive
        };
        let command = if self.match_kw(TokenKind::KwFor) {
            self.parse_policy_command()?
        } else {
            PolicyCommand::All
        };
        let mut using = None;
        let mut with_check = None;
        loop {
            match self.peek()?.kind {
                TokenKind::KwUsing => {
                    self.advance()?; // USING
                    using = Some(self.parse_parenthesized_policy_expr()?);
                }
                TokenKind::KwWith => {
                    self.advance()?; // WITH
                    self.expect(TokenKind::KwCheck, "CHECK")?;
                    with_check = Some(self.parse_parenthesized_policy_expr()?);
                }
                _ => break,
            }
        }
        let end = self.peek()?.span.start;
        Ok(CreatePolicyStmt {
            name,
            table,
            permissiveness,
            command,
            using,
            with_check,
            span: crate::span::Span::new(create_start, end),
        })
    }

    fn parse_policy_permissiveness(&mut self) -> Result<PolicyPermissiveness, ParseError> {
        let tok = *self.peek()?;
        if tok.kind == TokenKind::Identifier {
            if tok
                .text(self.source)
                .is_some_and(|text| text.eq_ignore_ascii_case("permissive"))
            {
                self.advance()?;
                return Ok(PolicyPermissiveness::Permissive);
            }
            if tok
                .text(self.source)
                .is_some_and(|text| text.eq_ignore_ascii_case("restrictive"))
            {
                self.advance()?;
                return Ok(PolicyPermissiveness::Restrictive);
            }
        }
        Err(ParseError::Expected {
            expected: "PERMISSIVE or RESTRICTIVE",
            found: tok.kind,
            offset: tok.span.start as usize,
        })
    }

    fn parse_policy_command(&mut self) -> Result<PolicyCommand, ParseError> {
        let tok = *self.peek()?;
        let command = match tok.kind {
            TokenKind::KwAll => PolicyCommand::All,
            TokenKind::KwSelect => PolicyCommand::Select,
            TokenKind::KwInsert => PolicyCommand::Insert,
            TokenKind::KwUpdate => PolicyCommand::Update,
            TokenKind::KwDelete => PolicyCommand::Delete,
            other => {
                return Err(ParseError::Expected {
                    expected: "ALL, SELECT, INSERT, UPDATE, or DELETE",
                    found: other,
                    offset: tok.span.start as usize,
                });
            }
        };
        self.advance()?;
        Ok(command)
    }

    fn parse_parenthesized_policy_expr(&mut self) -> Result<Expr, ParseError> {
        self.expect(TokenKind::LParen, "(")?;
        let expr = self.parse_expr()?;
        self.expect(TokenKind::RParen, ")")?;
        Ok(expr)
    }

    pub(crate) fn expect_identifier_keyword(
        &mut self,
        word: &'static str,
        expected: &'static str,
    ) -> Result<Token, ParseError> {
        let tok = *self.peek()?;
        if tok.kind == TokenKind::Identifier
            && tok
                .text(self.source)
                .is_some_and(|text| text.eq_ignore_ascii_case(word))
        {
            return self.advance();
        }
        Err(ParseError::Expected {
            expected,
            found: tok.kind,
            offset: tok.span.start as usize,
        })
    }

    /// Dispatch `DROP …` to the appropriate sub-parser based on the
    /// keyword that follows `DROP`.
    fn parse_drop(&mut self) -> Result<Statement, ParseError> {
        let drop_tok = self.advance()?; // DROP
        let start = drop_tok.span.start;

        let tok = *self.peek()?;
        match tok.kind {
            TokenKind::KwTable => self.parse_drop_table(start).map(Statement::DropTable),
            TokenKind::KwSchema => self.parse_drop_schema(start).map(Statement::DropSchema),
            TokenKind::KwIndex => self.parse_drop_index(start).map(Statement::DropIndex),
            TokenKind::KwSequence => self.parse_drop_sequence(start).map(Statement::DropSequence),
            TokenKind::Identifier
                if tok
                    .text(self.source)
                    .is_some_and(|text| text.eq_ignore_ascii_case("role")) =>
            {
                self.parse_drop_role(start, RoleStmtKind::Role)
                    .map(Statement::DropRole)
            }
            TokenKind::Identifier
                if tok
                    .text(self.source)
                    .is_some_and(|text| text.eq_ignore_ascii_case("user")) =>
            {
                self.parse_drop_role(start, RoleStmtKind::User)
                    .map(Statement::DropRole)
            }
            other => Err(ParseError::Expected {
                expected: "TABLE, SCHEMA, INDEX, SEQUENCE, ROLE, or USER after DROP",
                found: other,
                offset: tok.span.start as usize,
            }),
        }
    }

    /// Dispatch `ALTER …` to the appropriate sub-parser based on the
    /// keyword that follows `ALTER`.
    fn parse_alter(&mut self) -> Result<Statement, ParseError> {
        let alter_tok = self.advance()?; // ALTER
        let start = alter_tok.span.start;

        let tok = *self.peek()?;
        match tok.kind {
            TokenKind::KwDefault => self
                .parse_alter_default_privileges(start)
                .map(|s| Statement::AlterDefaultPrivileges(Box::new(s))),
            TokenKind::KwTable => self
                .parse_alter_table(start)
                .map(|s| Statement::AlterTable(Box::new(s))),
            TokenKind::KwSequence => self
                .parse_alter_sequence(start)
                .map(|s| Statement::AlterSequence(Box::new(s))),
            TokenKind::Identifier
                if tok
                    .text(self.source)
                    .is_some_and(|text| text.eq_ignore_ascii_case("role")) =>
            {
                self.parse_alter_role(start, RoleStmtKind::Role)
                    .map(|s| Statement::AlterRole(Box::new(s)))
            }
            TokenKind::Identifier
                if tok
                    .text(self.source)
                    .is_some_and(|text| text.eq_ignore_ascii_case("user")) =>
            {
                self.parse_alter_role(start, RoleStmtKind::User)
                    .map(|s| Statement::AlterRole(Box::new(s)))
            }
            other => Err(ParseError::Expected {
                expected: "DEFAULT PRIVILEGES, TABLE, SEQUENCE, ROLE, or USER after ALTER",
                found: other,
                offset: tok.span.start as usize,
            }),
        }
    }

    // ---------------- recursion guard ------------------------------------

    /// Increment the expression-recursion depth counter, returning a
    /// [`ParseError::DepthExceeded`] when the configured limit is
    /// reached. Every entry into [`expr::Parser::parse_expr_with_precedence`]
    /// pairs an `enter_depth` with a matching `leave_depth`.
    pub(crate) fn enter_depth(&mut self) -> Result<(), ParseError> {
        if self.depth >= MAX_PARSE_DEPTH {
            let offset = self
                .peeked
                .as_ref()
                .map_or_else(|| self.lexer.offset(), |t| t.span.start as usize);
            return Err(ParseError::DepthExceeded {
                limit: MAX_PARSE_DEPTH,
                offset,
            });
        }
        self.depth += 1;
        Ok(())
    }

    pub(crate) fn leave_depth(&mut self) {
        debug_assert!(self.depth > 0, "leave_depth without matching enter_depth");
        self.depth = self.depth.saturating_sub(1);
    }

    // ---------------- token helpers --------------------------------------

    pub(crate) fn peek(&mut self) -> Result<&Token, ParseError> {
        if self.peeked.is_none() {
            let t = self.lexer.next_token()?;
            self.peeked = Some(t);
        }
        self.peeked
            .as_ref()
            .ok_or(ParseError::UnexpectedEof { expected: "token" })
    }

    /// Look `distance` tokens past the buffered peek. `distance == 1`
    /// returns the token immediately after the one [`Self::peek`]
    /// would return. Callers in this parser never ask for more than
    /// two tokens of lookahead, so the linear re-tokenization cost is
    /// negligible.
    pub(crate) fn lookahead_at(&mut self, distance: usize) -> Result<Token, ParseError> {
        debug_assert!(distance >= 1);
        // Ensure we have a buffered peeked token; this fixes the
        // lexer offset to "just past that token's end".
        let _ = self.peek()?;
        let remainder = &self.source[self.lexer.offset()..];
        let mut tmp = Lexer::new(remainder);
        let mut tok = tmp.next_token()?;
        for _ in 1..distance {
            tok = tmp.next_token()?;
        }
        Ok(tok)
    }

    pub(crate) fn lookahead_text_eq_ignore_ascii_case(
        &mut self,
        distance: usize,
        expected: &str,
    ) -> Result<bool, ParseError> {
        debug_assert!(distance >= 1);
        let _ = self.peek()?;
        let remainder = &self.source[self.lexer.offset()..];
        let mut tmp = Lexer::new(remainder);
        let mut tok = tmp.next_token()?;
        for _ in 1..distance {
            tok = tmp.next_token()?;
        }
        Ok(tok
            .text(remainder)
            .is_some_and(|text| text.eq_ignore_ascii_case(expected)))
    }

    pub(crate) fn lookahead_two_is(&mut self, a: TokenKind, b: TokenKind) -> bool {
        // We only ever check from after the *current* peeked token, so
        // this looks one past peek and one past that.
        let Ok(first) = self.lookahead_at(1) else {
            return false;
        };
        if first.kind != a {
            return false;
        }
        let Ok(second) = self.lookahead_at(2) else {
            return false;
        };
        second.kind == b
    }

    pub(crate) fn advance(&mut self) -> Result<Token, ParseError> {
        if let Some(t) = self.peeked.take() {
            return Ok(t);
        }
        self.lexer.next_token().map_err(ParseError::from)
    }

    pub(crate) fn expect(
        &mut self,
        kind: TokenKind,
        name: &'static str,
    ) -> Result<Token, ParseError> {
        let head = self.peek()?;
        if head.kind == kind {
            return self.advance();
        }
        if head.kind == TokenKind::Eof {
            return Err(ParseError::UnexpectedEof { expected: name });
        }
        Err(ParseError::Expected {
            expected: name,
            found: head.kind,
            offset: head.span.start as usize,
        })
    }

    pub(crate) fn match_kw(&mut self, kind: TokenKind) -> bool {
        if matches!(self.peek().map(|t| t.kind), Ok(k) if k == kind) {
            let _ = self.advance();
            return true;
        }
        false
    }

    /// Parse an optional `ISOLATION LEVEL {READ COMMITTED | REPEATABLE READ | SERIALIZABLE}`
    /// clause. Returns `None` if the next token is not `ISOLATION`.
    pub(crate) fn parse_opt_isolation_level(
        &mut self,
    ) -> Result<Option<crate::ast::AstIsolationLevel>, ParseError> {
        if self.peek()?.kind != TokenKind::KwIsolation {
            return Ok(None);
        }
        self.advance()?; // ISOLATION
        self.expect(TokenKind::KwLevel, "LEVEL")?;
        let tok = self.peek()?;
        match tok.kind {
            TokenKind::KwRead => {
                self.advance()?; // READ
                let next = self.peek()?;
                match next.kind {
                    TokenKind::KwCommitted => {
                        self.advance()?;
                        Ok(Some(crate::ast::AstIsolationLevel::ReadCommitted))
                    }
                    TokenKind::KwUncommitted => {
                        // READ UNCOMMITTED — alias to READ COMMITTED per PostgreSQL
                        self.advance()?;
                        Ok(Some(crate::ast::AstIsolationLevel::ReadCommitted))
                    }
                    other => Err(ParseError::Expected {
                        expected: "COMMITTED or UNCOMMITTED after READ",
                        found: other,
                        offset: next.span.start as usize,
                    }),
                }
            }
            TokenKind::KwRepeatable => {
                self.advance()?; // REPEATABLE
                self.expect(TokenKind::KwRead, "READ")?;
                Ok(Some(crate::ast::AstIsolationLevel::RepeatableRead))
            }
            TokenKind::KwSerializable => {
                self.advance()?;
                Ok(Some(crate::ast::AstIsolationLevel::Serializable))
            }
            other => Err(ParseError::Expected {
                expected: "READ COMMITTED, REPEATABLE READ, or SERIALIZABLE",
                found: other,
                offset: tok.span.start as usize,
            }),
        }
    }

    pub(crate) fn next_token_is_reserved_clause(&mut self) -> bool {
        let kind = self.peek().map_or(TokenKind::Eof, |t| t.kind);
        matches!(
            kind,
            TokenKind::KwFrom
                | TokenKind::KwWhere
                | TokenKind::KwGroup
                | TokenKind::KwHaving
                | TokenKind::KwOrder
                | TokenKind::KwLimit
                | TokenKind::KwOffset
                | TokenKind::KwUnion
                | TokenKind::KwIntersect
                | TokenKind::KwExcept
                | TokenKind::Semicolon
                | TokenKind::Eof
                | TokenKind::Comma
        )
    }
}

pub(crate) const fn is_type_name_keyword(kind: TokenKind) -> bool {
    matches!(
        kind,
        TokenKind::KwBoolean
            | TokenKind::KwInt
            | TokenKind::KwInteger
            | TokenKind::KwBigint
            | TokenKind::KwSmallint
            | TokenKind::KwReal
            | TokenKind::KwFloat
            | TokenKind::KwDouble
            | TokenKind::KwPrecision
            | TokenKind::KwNumeric
            | TokenKind::KwDecimal
            | TokenKind::KwText
            | TokenKind::KwVarchar
            | TokenKind::KwChar
            | TokenKind::KwCharacter
            | TokenKind::KwDate
            | TokenKind::KwTime
            | TokenKind::KwTimestamp
            | TokenKind::KwInterval
            | TokenKind::KwJson
    )
}
