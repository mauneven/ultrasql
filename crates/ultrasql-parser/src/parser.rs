//! Recursive-descent + Pratt-style SQL parser.
//!
//! The parser consumes tokens from a [`Lexer`] and produces a
//! [`Statement`] tree. Statement-level structure is parsed by
//! recursive descent (one function per non-terminal); expressions go
//! through a Pratt parser keyed off [`BinaryOp::precedence`] so adding
//! a new operator costs one match-arm.
//!
//! The parser keeps a one-token lookahead via a buffered next-token
//! function. On EOF, every grammar rule that requires a terminator
//! either succeeds with the so-far-built node or reports a tagged
//! error.

use crate::ast::{BinaryOp, Expr, Identifier, Literal, Statement, UnaryOp};
use crate::lexer::{Lexer, LexerError};
use crate::span::Span;
use crate::token::{Token, TokenKind};

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
/// The value is intentionally conservative: the expression parser now
/// supports many more constructs (CASE, BETWEEN, postfix casts, etc.) so
/// each nesting level consumes a larger stack frame. 512 is still far above
/// any reasonable real-world SQL nesting depth.
pub const MAX_PARSE_DEPTH: u32 = 512;

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
                Ok(Statement::Begin { isolation_level, span: tok.span })
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
            TokenKind::KwReindex => self.parse_reindex().map(Statement::Reindex),
            TokenKind::KwSet | TokenKind::KwShow | TokenKind::KwReset => {
                let head_kind = head.kind;
                let next_kind = self.lookahead_at(1).map(|t| t.kind).ok();
                if head_kind == TokenKind::KwSet && next_kind == Some(TokenKind::KwTransaction) {
                    let set_tok = self.advance()?; // SET
                    self.advance()?; // TRANSACTION
                    let next_tok = *self.peek()?;
                    let isolation_level =
                        self.parse_opt_isolation_level()?.ok_or(ParseError::Expected {
                            expected: "ISOLATION LEVEL after SET TRANSACTION",
                            found: next_tok.kind,
                            offset: next_tok.span.start as usize,
                        })?;
                    Ok(Statement::SetTransaction {
                        isolation_level,
                        span: set_tok.span,
                    })
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
            TokenKind::KwDeallocate => {
                let tok = self.advance()?;
                self.parse_deallocate(tok.span.start)
            }
            other => Err(ParseError::Expected {
                expected: "SELECT, INSERT, UPDATE, DELETE, TRUNCATE, CREATE, DROP, ALTER, \
                           REINDEX, SET, SHOW, RESET, BEGIN, COMMIT, ROLLBACK, SAVEPOINT, \
                           RELEASE, EXPLAIN, PREPARE, EXECUTE, or DEALLOCATE",
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

        let tok = self.peek()?;
        match tok.kind {
            TokenKind::KwTable => self.parse_create_table(start),
            TokenKind::KwSchema => self.parse_create_schema(start).map(Statement::CreateSchema),
            TokenKind::KwIndex => self
                .parse_create_index(start, false)
                .map(|s| Statement::CreateIndex(Box::new(s))),
            TokenKind::KwUnique => {
                // CREATE UNIQUE INDEX …
                self.advance()?; // UNIQUE
                self.parse_create_index(start, true)
                    .map(|s| Statement::CreateIndex(Box::new(s)))
            }
            TokenKind::KwSequence => self
                .parse_create_sequence(start)
                .map(|s| Statement::CreateSequence(Box::new(s))),
            other => Err(ParseError::Expected {
                expected: "TABLE, SCHEMA, INDEX, UNIQUE, or SEQUENCE after CREATE",
                found: other,
                offset: tok.span.start as usize,
            }),
        }
    }

    /// Dispatch `DROP …` to the appropriate sub-parser based on the
    /// keyword that follows `DROP`.
    fn parse_drop(&mut self) -> Result<Statement, ParseError> {
        let drop_tok = self.advance()?; // DROP
        let start = drop_tok.span.start;

        let tok = self.peek()?;
        match tok.kind {
            TokenKind::KwTable => self.parse_drop_table(start).map(Statement::DropTable),
            TokenKind::KwSchema => self.parse_drop_schema(start).map(Statement::DropSchema),
            TokenKind::KwIndex => self.parse_drop_index(start).map(Statement::DropIndex),
            TokenKind::KwSequence => self.parse_drop_sequence(start).map(Statement::DropSequence),
            other => Err(ParseError::Expected {
                expected: "TABLE, SCHEMA, INDEX, or SEQUENCE after DROP",
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

        let tok = self.peek()?;
        match tok.kind {
            TokenKind::KwTable => self
                .parse_alter_table(start)
                .map(|s| Statement::AlterTable(Box::new(s))),
            TokenKind::KwSequence => self
                .parse_alter_sequence(start)
                .map(|s| Statement::AlterSequence(Box::new(s))),
            other => Err(ParseError::Expected {
                expected: "TABLE or SEQUENCE after ALTER",
                found: other,
                offset: tok.span.start as usize,
            }),
        }
    }

    // ---------------- expressions ----------------------------------------

    pub(crate) fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_expr_with_precedence(0)
    }

    fn parse_expr_with_precedence(&mut self, min_prec: u8) -> Result<Expr, ParseError> {
        self.enter_depth()?;
        let result = self.parse_expr_with_precedence_inner(min_prec);
        self.leave_depth();
        result
    }

    #[allow(clippy::too_many_lines)]
    fn parse_expr_with_precedence_inner(&mut self, min_prec: u8) -> Result<Expr, ParseError> {
        let mut left = self.parse_prefix()?;

        // Pratt binary-operator loop with integrated postfix constructs.
        'outer: loop {
            // ----------------------------------------------------------------
            // Postfix operators evaluated before the next binary-op check.
            // Ordering: :: (cast) > [] (subscript) > AT TIME ZONE > BETWEEN >
            //           IS > NOT BETWEEN/IN > IN.
            // ----------------------------------------------------------------

            // postfix `::type` — may chain (e.g. `x::int::text`).
            while self.peek()?.kind == TokenKind::ColonColon {
                self.advance()?; // ::
                let target = self.parse_type_name()?;
                let span = Span::new(left.span().start, target.span.end);
                left = Expr::PostfixCast {
                    expr: Box::new(left),
                    target,
                    span,
                };
            }

            // postfix `[index]` or `[lower:upper]` — may chain.
            while self.peek()?.kind == TokenKind::LBracket {
                left = self.parse_subscript_or_slice(left)?;
            }

            // `expr AT TIME ZONE zone`
            if self.peek()?.kind == TokenKind::KwAt
                && self.lookahead_two_is(TokenKind::KwTime, TokenKind::KwZone)
            {
                left = self.parse_at_time_zone(left)?;
                continue 'outer;
            }

            // `expr [NOT] BETWEEN [SYMMETRIC] low AND high`
            if self.peek()?.kind == TokenKind::KwBetween {
                left = self.parse_between_body(left, false)?;
                continue 'outer;
            }
            if self.peek()?.kind == TokenKind::KwNot {
                // Peek: NOT BETWEEN
                if self.lookahead_at(1)?.kind == TokenKind::KwBetween {
                    self.advance()?; // NOT
                    left = self.parse_between_body(left, true)?;
                    continue 'outer;
                }
            }

            // `expr IS [NOT] NULL / TRUE / FALSE / UNKNOWN / DISTINCT FROM`
            if self.peek()?.kind == TokenKind::KwIs {
                left = self.parse_is_postfix(left)?;
                continue 'outer;
            }

            // `expr [NOT] IN (…)` — consumed here rather than the binary loop.
            if self.peek()?.kind == TokenKind::KwIn {
                self.advance()?; // IN
                return self.parse_in_expr(left, false);
            }
            if self.peek()?.kind == TokenKind::KwNot
                && self.lookahead_at(1)?.kind == TokenKind::KwIn
            {
                self.advance()?; // NOT
                self.advance()?; // IN
                return self.parse_in_expr(left, true);
            }

            // ----------------------------------------------------------------
            // Standard Pratt binary-op check.
            // ----------------------------------------------------------------
            let Some((op, op_span)) = self.peek_binary_op()? else {
                break 'outer;
            };
            let prec = op.precedence();
            if prec < min_prec {
                break 'outer;
            }
            self.consume_binary_op(op)?;

            let next_min = if op.is_right_associative() {
                prec
            } else {
                prec + 1
            };

            // Special post-infix: `<op> ANY/ALL (SELECT …)`.
            if let Some(any_all) = self.parse_any_all_expr(left.clone(), op)? {
                left = any_all;
                continue 'outer;
            }

            let right = self.parse_expr_with_precedence(next_min)?;
            let span = Span::new(left.span().start, right.span().end);
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
                span: Span::new(span.start, span.end.max(op_span.end)),
            };
        }

        Ok(left)
    }

    /// Parse `BETWEEN [SYMMETRIC] low AND high` after `expr` and the (optional
    /// `NOT` and the) `BETWEEN` keyword have been consumed by the caller.
    ///
    /// The AND inside `BETWEEN … AND …` is not a boolean AND: we parse the
    /// lower and upper bounds at precedence 4 (one above comparison) so that
    /// a bare `AND` terminates the bound expression without consuming it.
    fn parse_between_body(&mut self, expr: Expr, negated: bool) -> Result<Expr, ParseError> {
        let start = expr.span().start;
        self.advance()?; // BETWEEN
        let symmetric = self.match_kw(TokenKind::KwSymmetric);
        // Precedence 4 stops before AND (prec 2) but allows arithmetic.
        let low = self.parse_expr_with_precedence(4)?;
        self.expect(TokenKind::KwAnd, "AND")?;
        let high = self.parse_expr_with_precedence(4)?;
        let span = Span::new(start, high.span().end);
        Ok(Expr::Between {
            expr: Box::new(expr),
            low: Box::new(low),
            high: Box::new(high),
            negated,
            symmetric,
            span,
        })
    }

    /// Parse `IS [NOT] NULL / TRUE / FALSE / UNKNOWN / DISTINCT FROM`.
    ///
    /// The `IS` keyword is the current token and has not been consumed.
    fn parse_is_postfix(&mut self, expr: Expr) -> Result<Expr, ParseError> {
        let _is_tok = self.advance()?; // IS
        let start = expr.span().start;
        let negated = self.match_kw(TokenKind::KwNot);

        let tok = self.peek()?;
        match tok.kind {
            TokenKind::KwNull => {
                let end = self.advance()?.span.end;
                Ok(Expr::IsNull {
                    expr: Box::new(expr),
                    negated,
                    span: Span::new(start, end),
                })
            }
            TokenKind::KwTrue => {
                let end = self.advance()?.span.end;
                Ok(Expr::IsBoolean {
                    expr: Box::new(expr),
                    value: true,
                    is_unknown: false,
                    negated,
                    span: Span::new(start, end),
                })
            }
            TokenKind::KwFalse => {
                let end = self.advance()?.span.end;
                Ok(Expr::IsBoolean {
                    expr: Box::new(expr),
                    value: false,
                    is_unknown: false,
                    negated,
                    span: Span::new(start, end),
                })
            }
            TokenKind::KwUnknown => {
                let end = self.advance()?.span.end;
                Ok(Expr::IsBoolean {
                    expr: Box::new(expr),
                    // `value` is unused when `is_unknown` is true; use `true`
                    // as a neutral placeholder.
                    value: true,
                    is_unknown: true,
                    negated,
                    span: Span::new(start, end),
                })
            }
            TokenKind::KwDistinct => {
                self.advance()?; // DISTINCT
                self.expect(TokenKind::KwFrom, "FROM")?;
                let right = self.parse_expr_with_precedence(4)?;
                let span = Span::new(start, right.span().end);
                Ok(Expr::IsDistinctFrom {
                    left: Box::new(expr),
                    right: Box::new(right),
                    negated,
                    span,
                })
            }
            other => Err(ParseError::Expected {
                expected: "NULL, TRUE, FALSE, UNKNOWN, or DISTINCT FROM after IS",
                found: other,
                offset: tok.span.start as usize,
            }),
        }
    }

    /// Parse `expr[index]` or `expr[lower:upper]` after identifying `[`.
    fn parse_subscript_or_slice(&mut self, expr: Expr) -> Result<Expr, ParseError> {
        let start = expr.span().start;
        self.advance()?; // [

        // `[:upper]` — lower absent, colon present immediately.
        if self.peek()?.kind == TokenKind::Colon {
            self.advance()?; // :
            let upper = if self.peek()?.kind == TokenKind::RBracket {
                None
            } else {
                Some(Box::new(self.parse_expr()?))
            };
            let end = self.expect(TokenKind::RBracket, "]")?.span.end;
            return Ok(Expr::ArraySlice {
                expr: Box::new(expr),
                lower: None,
                upper,
                span: Span::new(start, end),
            });
        }

        // Parse an expression; decide slice vs subscript by the next token.
        let inner = self.parse_expr()?;

        if self.peek()?.kind == TokenKind::Colon {
            // Slice: `[lower:upper]` or `[lower:]`.
            self.advance()?; // :
            let upper = if self.peek()?.kind == TokenKind::RBracket {
                None
            } else {
                Some(Box::new(self.parse_expr()?))
            };
            let end = self.expect(TokenKind::RBracket, "]")?.span.end;
            Ok(Expr::ArraySlice {
                expr: Box::new(expr),
                lower: Some(Box::new(inner)),
                upper,
                span: Span::new(start, end),
            })
        } else {
            // Subscript: `[index]`.
            let end = self.expect(TokenKind::RBracket, "]")?.span.end;
            Ok(Expr::ArraySubscript {
                expr: Box::new(expr),
                index: Box::new(inner),
                span: Span::new(start, end),
            })
        }
    }

    /// Parse `expr AT TIME ZONE zone_expr`.
    ///
    /// The `AT` keyword is the current token (not yet consumed).
    fn parse_at_time_zone(&mut self, expr: Expr) -> Result<Expr, ParseError> {
        let start = expr.span().start;
        self.advance()?; // AT
        self.expect(TokenKind::KwTime, "TIME")?;
        self.expect(TokenKind::KwZone, "ZONE")?;
        // Zone is a high-precedence expression (e.g. string literal or ident).
        let zone = self.parse_expr_with_precedence(8)?;
        let span = Span::new(start, zone.span().end);
        Ok(Expr::AtTimeZone {
            expr: Box::new(expr),
            zone: Box::new(zone),
            span,
        })
    }

    #[allow(clippy::too_many_lines)]
    fn parse_prefix(&mut self) -> Result<Expr, ParseError> {
        let tok = self.peek()?;
        match tok.kind {
            TokenKind::Plus | TokenKind::Minus | TokenKind::KwNot | TokenKind::Tilde => {
                let op_tok = self.advance()?;
                let op = match op_tok.kind {
                    TokenKind::Plus => UnaryOp::Pos,
                    TokenKind::Minus => UnaryOp::Neg,
                    TokenKind::KwNot => UnaryOp::Not,
                    TokenKind::Tilde => UnaryOp::BitNot,
                    _ => unreachable!(),
                };
                // Special case: NOT EXISTS
                if op == UnaryOp::Not && self.peek()?.kind == TokenKind::KwExists {
                    return self.parse_exists_expr(true);
                }
                // Unary operators bind tighter than any binary operator.
                let rhs = self.parse_expr_with_precedence(9)?;
                let span = Span::new(op_tok.span.start, rhs.span().end);
                Ok(Expr::Unary {
                    op,
                    expr: Box::new(rhs),
                    span,
                })
            }

            TokenKind::Integer => {
                let t = self.advance()?;
                Ok(Expr::Literal(Literal::Integer {
                    text: t.text(self.source).unwrap_or("").to_owned(),
                    span: t.span,
                }))
            }
            TokenKind::Float => {
                let t = self.advance()?;
                Ok(Expr::Literal(Literal::Float {
                    text: t.text(self.source).unwrap_or("").to_owned(),
                    span: t.span,
                }))
            }
            TokenKind::String | TokenKind::EscapedString | TokenKind::DollarString => {
                let t = self.advance()?;
                let raw = t.text(self.source).unwrap_or("");
                // Strip the surrounding quotes for the standard form;
                // escape and dollar-quoted strings are left as-is for
                // the binder to interpret.
                let value = if matches!(t.kind, TokenKind::String) {
                    raw[1..raw.len() - 1].replace("''", "'")
                } else {
                    raw.to_owned()
                };
                Ok(Expr::Literal(Literal::String {
                    value,
                    span: t.span,
                }))
            }
            TokenKind::KwNull => {
                let t = self.advance()?;
                Ok(Expr::Literal(Literal::Null { span: t.span }))
            }
            TokenKind::KwTrue => {
                let t = self.advance()?;
                Ok(Expr::Literal(Literal::Bool {
                    value: true,
                    span: t.span,
                }))
            }
            TokenKind::KwFalse => {
                let t = self.advance()?;
                Ok(Expr::Literal(Literal::Bool {
                    value: false,
                    span: t.span,
                }))
            }

            TokenKind::Parameter => {
                let t = self.advance()?;
                let raw = t.text(self.source).unwrap_or("");
                let n: u32 = raw[1..]
                    .parse()
                    .map_err(|_| ParseError::ParameterOutOfRange {
                        text: raw.to_owned(),
                        offset: t.span.start as usize,
                    })?;
                Ok(Expr::Parameter {
                    index: n,
                    span: t.span,
                })
            }

            TokenKind::LParen => {
                // Consume `(` first, then peek to determine if it's a
                // subquery or a ROW/OVERLAPS paren list.
                let lp = self.advance()?;
                if let Some(subq) = self.try_parse_subquery_after_lparen(lp.span)? {
                    return Ok(subq);
                }
                // Parse a parenthesised expression list: could be a single
                // paren-expr, a ROW, or the LHS of OVERLAPS.
                let first = self.parse_expr()?;
                if self.peek()?.kind == TokenKind::Comma {
                    // Multiple expressions inside parens — candidate ROW or OVERLAPS LHS.
                    let mut fields = vec![first];
                    while self.peek()?.kind == TokenKind::Comma {
                        self.advance()?;
                        fields.push(self.parse_expr()?);
                    }
                    let rp = self.expect(TokenKind::RParen, ")")?;
                    let paren_span = Span::new(lp.span.start, rp.span.end);

                    // If `OVERLAPS` follows, this is the LHS period.
                    if self.peek()?.kind == TokenKind::KwOverlaps {
                        if fields.len() != 2 {
                            return Err(ParseError::Expected {
                                expected: "exactly two expressions before OVERLAPS",
                                found: TokenKind::KwOverlaps,
                                offset: self.peek()?.span.start as usize,
                            });
                        }
                        self.advance()?; // OVERLAPS
                        self.expect(TokenKind::LParen, "(")?;
                        let rs = self.parse_expr()?;
                        self.expect(TokenKind::Comma, ",")?;
                        let re = self.parse_expr()?;
                        let rp2 = self.expect(TokenKind::RParen, ")")?;
                        let mut iter = fields.into_iter();
                        let ls = iter.next().expect("len checked above");
                        let le = iter.next().expect("len checked above");
                        return Ok(Expr::Overlaps {
                            left_start: Box::new(ls),
                            left_end: Box::new(le),
                            right_start: Box::new(rs),
                            right_end: Box::new(re),
                            span: Span::new(lp.span.start, rp2.span.end),
                        });
                    }

                    // Otherwise emit a Row expression.
                    return Ok(Expr::Row {
                        fields,
                        span: paren_span,
                    });
                }

                let rp = self.expect(TokenKind::RParen, ")")?;
                Ok(Expr::Paren {
                    expr: Box::new(first),
                    span: Span::new(lp.span.start, rp.span.end),
                })
            }

            TokenKind::KwExists => self.parse_exists_expr(false),

            TokenKind::KwCast => self.parse_cast_expr(),

            TokenKind::KwCase => self.parse_case_expr(),

            TokenKind::KwCoalesce => self.parse_coalesce_expr(),

            TokenKind::KwNullif => self.parse_nullif_expr(),

            TokenKind::KwGreatest => self.parse_greatest_least_expr(true),

            TokenKind::KwLeast => self.parse_greatest_least_expr(false),

            TokenKind::KwRow => self.parse_row_expr(),

            TokenKind::Identifier | TokenKind::QuotedIdentifier => self.parse_ident_or_call(),

            other => Err(ParseError::Expected {
                expected: "expression",
                found: other,
                offset: tok.span.start as usize,
            }),
        }
    }

    /// Parse `CASE [operand] WHEN … THEN … [ELSE …] END`.
    fn parse_case_expr(&mut self) -> Result<Expr, ParseError> {
        let case_tok = self.advance()?; // CASE
        let start = case_tok.span.start;

        // Optional operand (simple CASE): absent when the next token is WHEN,
        // ELSE, or END (i.e. this is a searched CASE).
        let operand = if matches!(
            self.peek()?.kind,
            TokenKind::KwWhen | TokenKind::KwElse | TokenKind::KwEnd
        ) {
            None
        } else {
            Some(Box::new(self.parse_expr()?))
        };

        // One or more WHEN … THEN … branches.
        let mut branches = Vec::new();
        while self.peek()?.kind == TokenKind::KwWhen {
            self.advance()?; // WHEN
            let when_expr = self.parse_expr()?;
            self.expect(TokenKind::KwThen, "THEN")?;
            let then_expr = self.parse_expr()?;
            branches.push((when_expr, then_expr));
        }
        if branches.is_empty() {
            return Err(ParseError::Expected {
                expected: "WHEN clause in CASE expression",
                found: self.peek()?.kind,
                offset: self.peek()?.span.start as usize,
            });
        }

        // Optional ELSE.
        let else_expr = if self.match_kw(TokenKind::KwElse) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        let end_tok = self.expect(TokenKind::KwEnd, "END")?;
        Ok(Expr::Case {
            operand,
            branches,
            else_expr,
            span: Span::new(start, end_tok.span.end),
        })
    }

    /// Parse `COALESCE(a, b, …)`.
    fn parse_coalesce_expr(&mut self) -> Result<Expr, ParseError> {
        let kw = self.advance()?; // COALESCE
        self.expect(TokenKind::LParen, "(")?;
        let args = self.parse_expr_list_inner()?;
        let rp = self.expect(TokenKind::RParen, ")")?;
        Ok(Expr::Coalesce {
            args,
            span: Span::new(kw.span.start, rp.span.end),
        })
    }

    /// Parse `NULLIF(a, b)`.
    fn parse_nullif_expr(&mut self) -> Result<Expr, ParseError> {
        let kw = self.advance()?; // NULLIF
        self.expect(TokenKind::LParen, "(")?;
        let a = self.parse_expr()?;
        self.expect(TokenKind::Comma, ",")?;
        let b = self.parse_expr()?;
        let rp = self.expect(TokenKind::RParen, ")")?;
        Ok(Expr::NullIf {
            a: Box::new(a),
            b: Box::new(b),
            span: Span::new(kw.span.start, rp.span.end),
        })
    }

    /// Parse `GREATEST(…)` or `LEAST(…)`.
    fn parse_greatest_least_expr(&mut self, is_greatest: bool) -> Result<Expr, ParseError> {
        let kw = self.advance()?; // GREATEST / LEAST
        self.expect(TokenKind::LParen, "(")?;
        let args = self.parse_expr_list_inner()?;
        let rp = self.expect(TokenKind::RParen, ")")?;
        let span = Span::new(kw.span.start, rp.span.end);
        if is_greatest {
            Ok(Expr::Greatest { args, span })
        } else {
            Ok(Expr::Least { args, span })
        }
    }

    /// Parse `ROW(a, b, …)`.
    fn parse_row_expr(&mut self) -> Result<Expr, ParseError> {
        let kw = self.advance()?; // ROW
        self.expect(TokenKind::LParen, "(")?;
        let fields = if self.peek()?.kind == TokenKind::RParen {
            Vec::new()
        } else {
            self.parse_expr_list_inner()?
        };
        let rp = self.expect(TokenKind::RParen, ")")?;
        Ok(Expr::Row {
            fields,
            span: Span::new(kw.span.start, rp.span.end),
        })
    }

    /// Parse a comma-separated expression list for function argument lists.
    ///
    /// Unlike [`parse_expr_list`] in `select.rs`, this helper is used
    /// exclusively inside parentheses and always returns at least one
    /// expression.
    fn parse_expr_list_inner(&mut self) -> Result<Vec<Expr>, ParseError> {
        let mut args = vec![self.parse_expr()?];
        while self.peek()?.kind == TokenKind::Comma {
            self.advance()?;
            args.push(self.parse_expr()?);
        }
        Ok(args)
    }

    pub(crate) fn parse_cast_expr(&mut self) -> Result<Expr, ParseError> {
        let cast = self.advance()?; // CAST
        self.expect(TokenKind::LParen, "(")?;
        let expr = self.parse_expr()?;
        self.expect(TokenKind::KwAs, "AS")?;
        let target = self.parse_type_name()?;
        let rp = self.expect(TokenKind::RParen, ")")?;
        Ok(Expr::Cast {
            expr: Box::new(expr),
            target,
            span: Span::new(cast.span.start, rp.span.end),
        })
    }

    /// Parse a type name. Type names may be either ordinary
    /// identifiers (`my_domain`) or the SQL reserved type-name
    /// keywords (`integer`, `varchar`, `timestamp`, ...). This helper
    /// accepts both.
    pub(crate) fn parse_type_name(&mut self) -> Result<Identifier, ParseError> {
        let tok = self.peek()?;
        match tok.kind {
            TokenKind::Identifier | TokenKind::QuotedIdentifier => self.parse_identifier(),
            kind if is_type_name_keyword(kind) => {
                let tok = self.advance()?;
                let raw = tok.text(self.source).unwrap_or("");
                Ok(Identifier {
                    value: raw.to_ascii_lowercase(),
                    quoted: false,
                    span: tok.span,
                })
            }
            other => Err(ParseError::Expected {
                expected: "type name",
                found: other,
                offset: tok.span.start as usize,
            }),
        }
    }

    fn parse_ident_or_call(&mut self) -> Result<Expr, ParseError> {
        let name = self.parse_object_name()?;
        if self.peek()?.kind == TokenKind::LParen {
            self.advance()?;
            // Optional DISTINCT.
            let distinct = self.match_kw(TokenKind::KwDistinct);
            let mut args = Vec::new();
            if self.peek()?.kind != TokenKind::RParen {
                loop {
                    // Special case: `COUNT(*)` and similar aggregate
                    // wildcard forms. Represent `*` as a `Column` with
                    // a single-part name `*` — the binder turns this
                    // into an aggregate wildcard.
                    if self.peek()?.kind == TokenKind::Star {
                        let star = self.advance()?;
                        args.push(Expr::Column {
                            name: crate::ast::ObjectName {
                                parts: vec![crate::ast::Identifier {
                                    value: "*".to_owned(),
                                    quoted: false,
                                    span: star.span,
                                }],
                                span: star.span,
                            },
                        });
                    } else {
                        args.push(self.parse_expr()?);
                    }
                    if self.peek()?.kind != TokenKind::Comma {
                        break;
                    }
                    self.advance()?;
                }
            }
            let rp = self.expect(TokenKind::RParen, ")")?;
            Ok(Expr::Call {
                args,
                distinct,
                span: Span::new(name.span.start, rp.span.end),
                name,
            })
        } else {
            Ok(Expr::Column { name })
        }
    }

    // ---------------- precedence helpers ---------------------------------

    fn peek_binary_op(&mut self) -> Result<Option<(BinaryOp, Span)>, ParseError> {
        // Snapshot the peek values so we can release the borrow before
        // calling lookahead_at for two-token operators such as NOT LIKE.
        let (kind, span) = {
            let tok = self.peek()?;
            (tok.kind, tok.span)
        };
        let op = match kind {
            TokenKind::Plus => BinaryOp::Add,
            TokenKind::Minus => BinaryOp::Sub,
            TokenKind::Star => BinaryOp::Mul,
            TokenKind::Slash => BinaryOp::Div,
            TokenKind::Percent => BinaryOp::Mod,
            TokenKind::Caret => BinaryOp::Pow,
            TokenKind::Concat => BinaryOp::Concat,
            TokenKind::Eq => BinaryOp::Eq,
            TokenKind::NotEq => BinaryOp::NotEq,
            TokenKind::Lt => BinaryOp::Lt,
            TokenKind::LtEq => BinaryOp::LtEq,
            TokenKind::Gt => BinaryOp::Gt,
            TokenKind::GtEq => BinaryOp::GtEq,
            TokenKind::KwAnd => BinaryOp::And,
            TokenKind::KwOr => BinaryOp::Or,
            TokenKind::KwLike => BinaryOp::Like,
            TokenKind::KwIlike => BinaryOp::Ilike,
            // Regex operators (produced by the lexer as distinct token kinds).
            TokenKind::Tilde => BinaryOp::RegexMatch,
            TokenKind::TildeStar => BinaryOp::RegexIMatch,
            TokenKind::NotTilde => BinaryOp::RegexNotMatch,
            TokenKind::NotTildeStar => BinaryOp::RegexNotIMatch,
            // Bitwise operators.
            TokenKind::Ampersand => BinaryOp::BitAnd,
            TokenKind::Pipe => BinaryOp::BitOr,
            TokenKind::Hash => BinaryOp::BitXor,
            TokenKind::ShiftLeft => BinaryOp::ShiftLeft,
            TokenKind::ShiftRight => BinaryOp::ShiftRight,
            // JSON operators.
            TokenKind::Arrow => BinaryOp::JsonGet,
            TokenKind::ArrowDouble => BinaryOp::JsonGetText,
            TokenKind::HashArrow => BinaryOp::JsonGetPath,
            TokenKind::HashArrowDouble => BinaryOp::JsonGetPathText,
            TokenKind::AtArrow => BinaryOp::JsonContains,
            TokenKind::ArrowAt => BinaryOp::JsonContained,
            TokenKind::QuestionMark => BinaryOp::JsonHasKey,
            TokenKind::QuestionPipe => BinaryOp::JsonHasAnyKey,
            TokenKind::QuestionAmpersand => BinaryOp::JsonHasAllKeys,
            TokenKind::KwNot => {
                // NOT LIKE / NOT ILIKE — the only two-keyword binary operators.
                let next = self.lookahead_at(1)?;
                return match next.kind {
                    TokenKind::KwLike => Ok(Some((BinaryOp::NotLike, span))),
                    TokenKind::KwIlike => Ok(Some((BinaryOp::NotIlike, span))),
                    _ => Ok(None),
                };
            }
            _ => return Ok(None),
        };
        Ok(Some((op, span)))
    }

    fn consume_binary_op(&mut self, op: BinaryOp) -> Result<(), ParseError> {
        match op {
            BinaryOp::NotLike => {
                self.expect(TokenKind::KwNot, "NOT")?;
                self.expect(TokenKind::KwLike, "LIKE")?;
            }
            BinaryOp::NotIlike => {
                self.expect(TokenKind::KwNot, "NOT")?;
                self.expect(TokenKind::KwIlike, "ILIKE")?;
            }
            _ => {
                // All other operators are a single token.
                self.advance()?;
            }
        }
        Ok(())
    }

    // ---------------- recursion guard ------------------------------------

    /// Increment the expression-recursion depth counter, returning a
    /// [`ParseError::DepthExceeded`] when the configured limit is
    /// reached. Every entry into [`Self::parse_expr_with_precedence`]
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
        Ok(self.peeked.as_ref().expect("just set"))
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

const fn is_type_name_keyword(kind: TokenKind) -> bool {
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
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{BinaryOp, Distinct, Expr, SelectItem, SortDirection, Statement, UnaryOp};

    fn parse(src: &str) -> Statement {
        Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
    }

    #[test]
    fn select_star() {
        let stmt = parse("SELECT * FROM users");
        let Statement::Select(s) = stmt else { panic!() };
        assert!(matches!(s.distinct, Distinct::None));
        assert!(matches!(s.projection[0], SelectItem::Wildcard { .. }));
        assert!(!s.from.is_empty());
    }

    #[test]
    fn select_columns_and_alias() {
        let stmt = parse("SELECT id, name AS n FROM users");
        let Statement::Select(s) = stmt else { panic!() };
        assert_eq!(s.projection.len(), 2);
        if let SelectItem::Expr { alias, .. } = &s.projection[1] {
            assert_eq!(alias.as_ref().unwrap().value, "n");
        } else {
            panic!("expected aliased item");
        }
    }

    #[test]
    fn select_with_where_clause() {
        let stmt = parse("SELECT id FROM users WHERE age >= 18 AND active = TRUE");
        let Statement::Select(s) = stmt else { panic!() };
        assert!(s.r#where.is_some());
    }

    #[test]
    fn select_with_order_limit_offset() {
        let stmt = parse("SELECT id FROM users ORDER BY id DESC LIMIT 10 OFFSET 5");
        let Statement::Select(s) = stmt else { panic!() };
        assert_eq!(s.order_by.len(), 1);
        assert_eq!(s.order_by[0].direction, SortDirection::Desc);
        assert!(s.limit.is_some());
        assert!(s.offset.is_some());
    }

    #[test]
    fn qualified_wildcard() {
        let stmt = parse("SELECT u.* FROM users u");
        let Statement::Select(s) = stmt else { panic!() };
        assert!(matches!(
            s.projection[0],
            SelectItem::QualifiedWildcard { .. }
        ));
    }

    #[test]
    fn expression_precedence() {
        let stmt = parse("SELECT 1 + 2 * 3 = 7 FROM x");
        let Statement::Select(s) = stmt else { panic!() };
        // (1 + (2 * 3)) = 7  → top operator is Eq.
        let SelectItem::Expr { expr, .. } = &s.projection[0] else {
            panic!()
        };
        assert!(matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::Eq,
                ..
            }
        ));
    }

    #[test]
    fn function_call_with_distinct() {
        let stmt = parse("SELECT count(DISTINCT id) FROM users");
        let Statement::Select(s) = stmt else { panic!() };
        let SelectItem::Expr { expr, .. } = &s.projection[0] else {
            panic!()
        };
        let Expr::Call {
            distinct,
            args,
            name,
            ..
        } = expr
        else {
            panic!()
        };
        assert!(distinct);
        assert_eq!(args.len(), 1);
        assert_eq!(name.parts[0].value, "count");
    }

    #[test]
    fn cast_expression() {
        let stmt = parse("SELECT CAST(x AS integer) FROM t");
        let Statement::Select(s) = stmt else { panic!() };
        let SelectItem::Expr { expr, .. } = &s.projection[0] else {
            panic!()
        };
        assert!(matches!(expr, Expr::Cast { .. }));
    }

    #[test]
    fn begin_commit_rollback_transactions() {
        assert!(matches!(parse("BEGIN"), Statement::Begin { .. }));
        assert!(matches!(
            parse("BEGIN TRANSACTION"),
            Statement::Begin { .. }
        ));
        assert!(matches!(parse("COMMIT"), Statement::Commit { .. }));
        assert!(matches!(parse("ROLLBACK"), Statement::Rollback { .. }));
    }

    #[test]
    fn set_transaction_isolation_level() {
        use crate::ast::AstIsolationLevel;
        let stmt = parse("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
        let Statement::SetTransaction { isolation_level, .. } = stmt else { panic!() };
        assert_eq!(isolation_level, AstIsolationLevel::ReadCommitted);

        let stmt = parse("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
        let Statement::SetTransaction { isolation_level, .. } = stmt else { panic!() };
        assert_eq!(isolation_level, AstIsolationLevel::RepeatableRead);

        let stmt = parse("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
        let Statement::SetTransaction { isolation_level, .. } = stmt else { panic!() };
        assert_eq!(isolation_level, AstIsolationLevel::Serializable);

        // SET <var> = … must still parse as SetVar (not SetTransaction).
        let stmt = parse("SET search_path TO public");
        assert!(matches!(stmt, Statement::SetVar(_)));
    }

    #[test]
    fn begin_isolation_level() {
        use crate::ast::AstIsolationLevel;
        let stmt = parse("BEGIN ISOLATION LEVEL READ COMMITTED");
        let Statement::Begin { isolation_level, .. } = stmt else { panic!() };
        assert_eq!(isolation_level, Some(AstIsolationLevel::ReadCommitted));

        let stmt = parse("BEGIN ISOLATION LEVEL READ UNCOMMITTED");
        let Statement::Begin { isolation_level, .. } = stmt else { panic!() };
        assert_eq!(isolation_level, Some(AstIsolationLevel::ReadCommitted));

        let stmt = parse("BEGIN ISOLATION LEVEL REPEATABLE READ");
        let Statement::Begin { isolation_level, .. } = stmt else { panic!() };
        assert_eq!(isolation_level, Some(AstIsolationLevel::RepeatableRead));

        let stmt = parse("BEGIN ISOLATION LEVEL SERIALIZABLE");
        let Statement::Begin { isolation_level, .. } = stmt else { panic!() };
        assert_eq!(isolation_level, Some(AstIsolationLevel::Serializable));

        let stmt = parse("BEGIN");
        let Statement::Begin { isolation_level, .. } = stmt else { panic!() };
        assert_eq!(isolation_level, None);
    }

    #[test]
    fn is_null_chain() {
        let stmt = parse("SELECT x IS NOT NULL FROM t");
        let Statement::Select(s) = stmt else { panic!() };
        let SelectItem::Expr { expr, .. } = &s.projection[0] else {
            panic!()
        };
        assert!(matches!(expr, Expr::IsNull { negated: true, .. }));
    }

    #[test]
    fn parameter_token() {
        let stmt = parse("SELECT $1 FROM t WHERE x = $2");
        let Statement::Select(s) = stmt else { panic!() };
        let SelectItem::Expr { expr, .. } = &s.projection[0] else {
            panic!()
        };
        assert!(matches!(expr, Expr::Parameter { index: 1, .. }));
    }

    #[test]
    fn parse_two_statements_separated_by_semicolons() {
        let mut p = Parser::new("BEGIN; SELECT 1 FROM t; COMMIT");
        let stmts = p.parse_statements().unwrap();
        assert_eq!(stmts.len(), 3);
        assert!(matches!(stmts[0], Statement::Begin { .. }));
        assert!(matches!(stmts[1], Statement::Select(_)));
        assert!(matches!(stmts[2], Statement::Commit { .. }));
    }

    #[test]
    fn missing_from_returns_select_without_from() {
        let stmt = parse("SELECT 1 + 1");
        let Statement::Select(s) = stmt else { panic!() };
        assert!(s.from.is_empty());
    }

    #[test]
    fn unexpected_eof_in_where_errors() {
        let err = Parser::new("SELECT x FROM t WHERE")
            .parse_statement()
            .unwrap_err();
        assert!(matches!(
            err,
            ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
        ));
    }

    #[test]
    fn unsupported_statement_rejected() {
        // A truly unknown statement keyword should produce an error.
        let err = Parser::new("VACUUM t").parse_statement().unwrap_err();
        assert!(matches!(err, ParseError::Expected { .. }));
    }

    /// Adversarial input: deeply-nested parentheses must be rejected
    /// with a `DepthExceeded` error rather than overflow the call
    /// stack. The depth bound is [`MAX_PARSE_DEPTH`]; we craft input
    /// that comfortably exceeds it.
    #[test]
    fn deeply_nested_parens_rejected_without_overflow() {
        let depth = (MAX_PARSE_DEPTH as usize) + 64;
        let mut sql = String::with_capacity(depth * 2 + 16);
        sql.push_str("SELECT ");
        for _ in 0..depth {
            sql.push('(');
        }
        sql.push('1');
        for _ in 0..depth {
            sql.push(')');
        }
        let err = Parser::new(&sql).parse_statement().unwrap_err();
        assert!(
            matches!(err, ParseError::DepthExceeded { .. }),
            "expected DepthExceeded, got {err:?}"
        );
    }

    /// A query at a depth comfortably below the limit must still
    /// succeed; the bound exists to refuse pathological inputs, not
    /// reasonable ones.
    #[test]
    fn parens_below_limit_succeed() {
        let depth = 256_usize;
        let mut sql = String::with_capacity(depth * 2 + 16);
        sql.push_str("SELECT ");
        for _ in 0..depth {
            sql.push('(');
        }
        sql.push('1');
        for _ in 0..depth {
            sql.push(')');
        }
        let stmt = Parser::new(&sql).parse_statement().expect("must parse");
        assert!(matches!(stmt, Statement::Select(_)));
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    /// Parse a bare expression from `SELECT <expr>` and return it.
    fn parse_expr(src: &str) -> Expr {
        let sql = format!("SELECT {src}");
        let stmt = Parser::new(&sql)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse_expr failed for {src:?}: {e}"));
        let Statement::Select(s) = stmt else { panic!() };
        let SelectItem::Expr { expr, .. } = s.projection.into_iter().next().unwrap() else {
            panic!()
        };
        expr
    }

    /// Expect parsing `SELECT <src>` to produce a [`ParseError`].
    fn parse_err(src: &str) -> ParseError {
        let sql = format!("SELECT {src}");
        Parser::new(&sql)
            .parse_statement()
            .expect_err("expected parse error")
    }

    // ── CASE expressions ────────────────────────────────────────────────────

    #[test]
    fn searched_case_basic() {
        let expr = parse_expr("CASE WHEN x > 0 THEN 'pos' WHEN x < 0 THEN 'neg' ELSE 'zero' END");
        let Expr::Case {
            operand,
            branches,
            else_expr,
            ..
        } = expr
        else {
            panic!()
        };
        assert!(operand.is_none(), "searched CASE has no operand");
        assert_eq!(branches.len(), 2);
        assert!(else_expr.is_some());
    }

    #[test]
    fn simple_case_basic() {
        let expr = parse_expr("CASE x WHEN 1 THEN 'one' WHEN 2 THEN 'two' END");
        let Expr::Case {
            operand,
            branches,
            else_expr,
            ..
        } = expr
        else {
            panic!()
        };
        assert!(operand.is_some(), "simple CASE has operand");
        assert_eq!(branches.len(), 2);
        assert!(else_expr.is_none());
    }

    #[test]
    fn case_no_when_is_error() {
        // CASE END without at least one WHEN clause is a parse error.
        let err = parse_err("CASE END");
        assert!(matches!(
            err,
            ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
        ));
    }

    // ── COALESCE ────────────────────────────────────────────────────────────

    #[test]
    fn coalesce_two_args() {
        let expr = parse_expr("COALESCE(a, 0)");
        let Expr::Coalesce { args, .. } = expr else {
            panic!()
        };
        assert_eq!(args.len(), 2);
    }

    #[test]
    fn coalesce_many_args() {
        let expr = parse_expr("COALESCE(a, b, c, d)");
        let Expr::Coalesce { args, .. } = expr else {
            panic!()
        };
        assert_eq!(args.len(), 4);
    }

    #[test]
    fn coalesce_empty_is_error() {
        let err = parse_err("COALESCE()");
        assert!(matches!(
            err,
            ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
        ));
    }

    // ── NULLIF ──────────────────────────────────────────────────────────────

    #[test]
    fn nullif_basic() {
        let expr = parse_expr("NULLIF(x, 0)");
        assert!(matches!(expr, Expr::NullIf { .. }));
    }

    #[test]
    fn nullif_with_string() {
        let expr = parse_expr("NULLIF(name, '')");
        let Expr::NullIf { a, b, .. } = expr else {
            panic!()
        };
        assert!(matches!(*a, Expr::Column { .. }));
        assert!(matches!(*b, Expr::Literal(_)));
    }

    #[test]
    fn nullif_too_few_args_is_error() {
        let err = parse_err("NULLIF(x)");
        assert!(matches!(
            err,
            ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
        ));
    }

    // ── GREATEST / LEAST ────────────────────────────────────────────────────

    #[test]
    fn greatest_two_args() {
        let expr = parse_expr("GREATEST(a, b)");
        let Expr::Greatest { args, .. } = expr else {
            panic!()
        };
        assert_eq!(args.len(), 2);
    }

    #[test]
    fn least_many_args() {
        let expr = parse_expr("LEAST(1, 2, 3, 4)");
        let Expr::Least { args, .. } = expr else {
            panic!()
        };
        assert_eq!(args.len(), 4);
    }

    #[test]
    fn greatest_empty_is_error() {
        let err = parse_err("GREATEST()");
        assert!(matches!(
            err,
            ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
        ));
    }

    // ── BETWEEN ─────────────────────────────────────────────────────────────

    #[test]
    fn between_basic() {
        let expr = parse_expr("x BETWEEN 1 AND 10");
        let Expr::Between {
            negated, symmetric, ..
        } = expr
        else {
            panic!()
        };
        assert!(!negated);
        assert!(!symmetric);
    }

    #[test]
    fn not_between() {
        let expr = parse_expr("x NOT BETWEEN 1 AND 10");
        let Expr::Between { negated, .. } = expr else {
            panic!()
        };
        assert!(negated);
    }

    #[test]
    fn between_symmetric() {
        let expr = parse_expr("x BETWEEN SYMMETRIC 10 AND 1");
        let Expr::Between { symmetric, .. } = expr else {
            panic!()
        };
        assert!(symmetric);
    }

    #[test]
    fn between_missing_and_is_error() {
        let err = parse_err("x BETWEEN 1 10");
        assert!(matches!(err, ParseError::Expected { .. }));
    }

    #[test]
    fn between_and_does_not_consume_outer_and() {
        // The AND inside BETWEEN must not eat the outer boolean AND.
        let expr = parse_expr("x BETWEEN 1 AND 10 AND y = 2");
        // Top-level should be a boolean AND of (Between, Binary{Eq}).
        assert!(matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::And,
                ..
            }
        ));
    }

    // ── IS DISTINCT FROM ────────────────────────────────────────────────────

    #[test]
    fn is_distinct_from_basic() {
        let expr = parse_expr("x IS DISTINCT FROM NULL");
        assert!(matches!(expr, Expr::IsDistinctFrom { negated: false, .. }));
    }

    #[test]
    fn is_not_distinct_from() {
        let expr = parse_expr("x IS NOT DISTINCT FROM NULL");
        assert!(matches!(expr, Expr::IsDistinctFrom { negated: true, .. }));
    }

    #[test]
    fn is_distinct_from_missing_from_is_error() {
        let err = parse_err("x IS DISTINCT NULL");
        assert!(matches!(err, ParseError::Expected { .. }));
    }

    // ── IS TRUE / FALSE / UNKNOWN ────────────────────────────────────────────

    #[test]
    fn is_true() {
        let expr = parse_expr("x IS TRUE");
        assert!(matches!(
            expr,
            Expr::IsBoolean {
                value: true,
                is_unknown: false,
                negated: false,
                ..
            }
        ));
    }

    #[test]
    fn is_not_false() {
        let expr = parse_expr("x IS NOT FALSE");
        assert!(matches!(
            expr,
            Expr::IsBoolean {
                value: false,
                negated: true,
                ..
            }
        ));
    }

    #[test]
    fn is_unknown() {
        let expr = parse_expr("x IS UNKNOWN");
        assert!(matches!(
            expr,
            Expr::IsBoolean {
                is_unknown: true,
                negated: false,
                ..
            }
        ));
    }

    #[test]
    fn is_not_unknown() {
        let expr = parse_expr("x IS NOT UNKNOWN");
        assert!(matches!(
            expr,
            Expr::IsBoolean {
                is_unknown: true,
                negated: true,
                ..
            }
        ));
    }

    // ── postfix cast `::` ────────────────────────────────────────────────────

    #[test]
    fn postfix_cast_integer() {
        let expr = parse_expr("x::integer");
        let Expr::PostfixCast { target, .. } = expr else {
            panic!()
        };
        assert_eq!(target.value, "integer");
    }

    #[test]
    fn postfix_cast_chain() {
        // x::text::varchar — two successive casts.
        let expr = parse_expr("x::text::varchar");
        let Expr::PostfixCast {
            expr: inner,
            target: outer_target,
            ..
        } = expr
        else {
            panic!()
        };
        assert_eq!(outer_target.value, "varchar");
        assert!(matches!(*inner, Expr::PostfixCast { .. }));
    }

    #[test]
    fn postfix_cast_missing_type_is_error() {
        let err = parse_err("x::");
        assert!(matches!(
            err,
            ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
        ));
    }

    // ── array subscript `[]` ─────────────────────────────────────────────────

    #[test]
    fn array_subscript_basic() {
        let expr = parse_expr("arr[1]");
        assert!(matches!(expr, Expr::ArraySubscript { .. }));
    }

    #[test]
    fn array_subscript_expression_index() {
        let expr = parse_expr("arr[i + 1]");
        let Expr::ArraySubscript { index, .. } = expr else {
            panic!()
        };
        assert!(matches!(
            *index,
            Expr::Binary {
                op: BinaryOp::Add,
                ..
            }
        ));
    }

    #[test]
    fn array_subscript_unclosed_is_error() {
        let err = parse_err("arr[1");
        assert!(matches!(
            err,
            ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
        ));
    }

    // ── array slice `[:]` ────────────────────────────────────────────────────

    #[test]
    fn array_slice_both_bounds() {
        let expr = parse_expr("arr[2:5]");
        let Expr::ArraySlice { lower, upper, .. } = expr else {
            panic!()
        };
        assert!(lower.is_some());
        assert!(upper.is_some());
    }

    #[test]
    fn array_slice_lower_only() {
        let expr = parse_expr("arr[2:]");
        let Expr::ArraySlice { lower, upper, .. } = expr else {
            panic!()
        };
        assert!(lower.is_some());
        assert!(upper.is_none());
    }

    #[test]
    fn array_slice_upper_only() {
        let expr = parse_expr("arr[:5]");
        let Expr::ArraySlice { lower, upper, .. } = expr else {
            panic!()
        };
        assert!(lower.is_none());
        assert!(upper.is_some());
    }

    // ── AT TIME ZONE ─────────────────────────────────────────────────────────

    #[test]
    fn at_time_zone_string_literal() {
        let expr = parse_expr("now() AT TIME ZONE 'UTC'");
        assert!(matches!(expr, Expr::AtTimeZone { .. }));
    }

    #[test]
    fn at_time_zone_identifier() {
        let expr = parse_expr("ts AT TIME ZONE tz_col");
        assert!(matches!(expr, Expr::AtTimeZone { .. }));
    }

    #[test]
    fn at_time_zone_missing_zone_expr_is_error() {
        // The zone expression is mandatory after AT TIME ZONE.
        let err = parse_err("ts AT TIME ZONE");
        assert!(matches!(
            err,
            ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
        ));
    }

    // ── OVERLAPS ─────────────────────────────────────────────────────────────

    #[test]
    fn overlaps_basic() {
        let expr = parse_expr("(a, b) OVERLAPS (c, d)");
        assert!(matches!(expr, Expr::Overlaps { .. }));
    }

    #[test]
    fn overlaps_fields_are_captured() {
        let expr = parse_expr("(t1, t2) OVERLAPS (t3, t4)");
        let Expr::Overlaps {
            left_start,
            left_end,
            right_start,
            right_end,
            ..
        } = expr
        else {
            panic!()
        };
        // Check all four fields were parsed as column references.
        assert!(matches!(*left_start, Expr::Column { .. }));
        assert!(matches!(*left_end, Expr::Column { .. }));
        assert!(matches!(*right_start, Expr::Column { .. }));
        assert!(matches!(*right_end, Expr::Column { .. }));
    }

    #[test]
    fn overlaps_missing_second_pair_is_error() {
        let err = parse_err("(a, b) OVERLAPS c");
        assert!(matches!(err, ParseError::Expected { .. }));
    }

    // ── ROW constructor ──────────────────────────────────────────────────────

    #[test]
    fn row_explicit_keyword() {
        let expr = parse_expr("ROW(1, 2, 3)");
        let Expr::Row { fields, .. } = expr else {
            panic!()
        };
        assert_eq!(fields.len(), 3);
    }

    #[test]
    fn row_single_field() {
        let expr = parse_expr("ROW(42)");
        let Expr::Row { fields, .. } = expr else {
            panic!()
        };
        assert_eq!(fields.len(), 1);
    }

    #[test]
    fn row_empty_is_accepted() {
        // PostgreSQL accepts ROW() as a zero-element row constructor.
        let expr = parse_expr("ROW()");
        let Expr::Row { fields, .. } = expr else {
            panic!()
        };
        assert_eq!(fields.len(), 0);
    }

    #[test]
    fn row_unclosed_paren_is_error() {
        let err = parse_err("ROW(1, 2");
        assert!(matches!(
            err,
            ParseError::Expected { .. } | ParseError::UnexpectedEof { .. }
        ));
    }

    // ── regex operators ─────────────────────────────────────────────────────

    #[test]
    fn regex_match_operator() {
        let expr = parse_expr("name ~ '^A'");
        assert!(matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::RegexMatch,
                ..
            }
        ));
    }

    #[test]
    fn regex_imatch_operator() {
        let expr = parse_expr("name ~* '^a'");
        assert!(matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::RegexIMatch,
                ..
            }
        ));
    }

    #[test]
    fn regex_not_match_operator() {
        let expr = parse_expr("name !~ '^A'");
        assert!(matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::RegexNotMatch,
                ..
            }
        ));
    }

    #[test]
    fn regex_not_imatch_operator() {
        let expr = parse_expr("name !~* '^a'");
        assert!(matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::RegexNotIMatch,
                ..
            }
        ));
    }

    // ── bitwise operators ────────────────────────────────────────────────────

    #[test]
    fn bitwise_and_operator() {
        let expr = parse_expr("x & 0xff");
        assert!(matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::BitAnd,
                ..
            }
        ));
    }

    #[test]
    fn bitwise_or_operator() {
        let expr = parse_expr("x | 0x01");
        assert!(matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::BitOr,
                ..
            }
        ));
    }

    #[test]
    fn bitwise_xor_operator() {
        let expr = parse_expr("x # y");
        assert!(matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::BitXor,
                ..
            }
        ));
    }

    #[test]
    fn shift_left_operator() {
        let expr = parse_expr("x << 2");
        assert!(matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::ShiftLeft,
                ..
            }
        ));
    }

    #[test]
    fn shift_right_operator() {
        let expr = parse_expr("x >> 2");
        assert!(matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::ShiftRight,
                ..
            }
        ));
    }

    #[test]
    fn unary_bitnot_operator() {
        let expr = parse_expr("~x");
        assert!(matches!(
            expr,
            Expr::Unary {
                op: UnaryOp::BitNot,
                ..
            }
        ));
    }

    #[test]
    fn bitwise_precedence_tighter_than_comparison() {
        // `x & mask = 0` should parse as `(x & mask) = 0` not `x & (mask = 0)`.
        let expr = parse_expr("x & 255 = 0");
        assert!(matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::Eq,
                ..
            }
        ));
    }

    #[test]
    fn shift_lower_precedence_than_add() {
        // Level 5 (<<) is *lower* than level 6 (+), so `a + b << 3`
        // parses as `(a + b) << 3` — top-level operator is ShiftLeft.
        let expr = parse_expr("a + b << 3");
        assert!(matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::ShiftLeft,
                ..
            }
        ));
    }

    // ── JSON operators ───────────────────────────────────────────────────────

    #[test]
    fn json_get_by_key() {
        let expr = parse_expr("doc -> 'key'");
        assert!(matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::JsonGet,
                ..
            }
        ));
    }

    #[test]
    fn json_get_text() {
        let expr = parse_expr("doc ->> 'key'");
        assert!(matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::JsonGetText,
                ..
            }
        ));
    }

    #[test]
    fn json_get_path() {
        let expr = parse_expr("doc #> '{a,b}'");
        assert!(matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::JsonGetPath,
                ..
            }
        ));
    }

    #[test]
    fn json_get_path_text() {
        let expr = parse_expr("doc #>> '{a,b}'");
        assert!(matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::JsonGetPathText,
                ..
            }
        ));
    }

    #[test]
    fn json_contains() {
        let expr = parse_expr("doc @> '{\"a\":1}'");
        assert!(matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::JsonContains,
                ..
            }
        ));
    }

    #[test]
    fn json_contained_by() {
        let expr = parse_expr("doc <@ '{\"a\":1}'");
        assert!(matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::JsonContained,
                ..
            }
        ));
    }

    #[test]
    fn json_has_key() {
        let expr = parse_expr("doc ? 'key'");
        assert!(matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::JsonHasKey,
                ..
            }
        ));
    }

    #[test]
    fn json_has_any_key() {
        let expr = parse_expr("doc ?| keys");
        assert!(matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::JsonHasAnyKey,
                ..
            }
        ));
    }

    #[test]
    fn json_has_all_keys() {
        let expr = parse_expr("doc ?& keys");
        assert!(matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::JsonHasAllKeys,
                ..
            }
        ));
    }

    /// JSON operators bind tighter than comparison: `doc -> 'k' = 'v'`
    /// parses as `(doc -> 'k') = 'v'`.
    #[test]
    fn json_get_tighter_than_eq() {
        let expr = parse_expr("doc -> 'k' = 'v'");
        assert!(matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::Eq,
                ..
            }
        ));
    }

    // ── operator precedence property test ────────────────────────────────────

    /// A table-driven precedence check: build an expression `a OP1 b OP2 c`
    /// and assert the parse tree reflects the correct associativity.
    ///
    /// For each pair `(low_op, high_op)` where `high_op` binds more tightly,
    /// `a LOW b HIGH c` must parse as `a LOW (b HIGH c)` — i.e. the top-level
    /// operator is `low_op`.
    #[test]
    fn binary_op_precedence_pairs() {
        let cases: &[(&str, BinaryOp, &str, BinaryOp)] = &[
            // low_expr, low_op, high_expr, high_op
            ("a OR b AND c", BinaryOp::Or, "b AND c", BinaryOp::And),
            ("a AND b = c", BinaryOp::And, "b = c", BinaryOp::Eq),
            ("a = b + c", BinaryOp::Eq, "b + c", BinaryOp::Add),
            ("a + b * c", BinaryOp::Add, "b * c", BinaryOp::Mul),
            ("a * b ^ c", BinaryOp::Mul, "b ^ c", BinaryOp::Pow),
            ("a << b + c", BinaryOp::ShiftLeft, "b + c", BinaryOp::Add),
            ("a = b & c", BinaryOp::Eq, "b & c", BinaryOp::BitAnd),
        ];

        for (src, expected_top, _rhs_src, expected_rhs) in cases {
            let expr = parse_expr(src);
            let Expr::Binary {
                op: top_op, right, ..
            } = expr
            else {
                panic!("expected Binary for {src:?}, got {expr:?}");
            };
            assert_eq!(top_op, *expected_top, "top op mismatch for {src:?}");
            // The right operand should carry the tighter operator.
            let Expr::Binary { op: rhs_op, .. } = *right else {
                panic!("expected Binary rhs for {src:?}");
            };
            assert_eq!(rhs_op, *expected_rhs, "rhs op mismatch for {src:?}");
        }
    }

    /// Right-associativity of `^`: `a ^ b ^ c` must parse as `a ^ (b ^ c)`.
    #[test]
    fn pow_is_right_associative() {
        let expr = parse_expr("a ^ b ^ c");
        let Expr::Binary {
            op: BinaryOp::Pow,
            right,
            ..
        } = expr
        else {
            panic!()
        };
        assert!(matches!(
            *right,
            Expr::Binary {
                op: BinaryOp::Pow,
                ..
            }
        ));
    }
}
