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
pub const MAX_PARSE_DEPTH: u32 = 1024;

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
            TokenKind::KwSelect => self.parse_select().map(|s| Statement::Select(Box::new(s))),
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
                Ok(Statement::Begin { span: tok.span })
            }
            TokenKind::KwCommit => {
                let tok = self.advance()?;
                if self.peek()?.kind == TokenKind::KwTransaction {
                    self.advance()?;
                }
                Ok(Statement::Commit { span: tok.span })
            }
            TokenKind::KwRollback => {
                let tok = self.advance()?;
                if self.peek()?.kind == TokenKind::KwTransaction {
                    self.advance()?;
                }
                Ok(Statement::Rollback { span: tok.span })
            }
            other => Err(ParseError::Expected {
                expected: "SELECT, INSERT, UPDATE, DELETE, TRUNCATE, BEGIN, COMMIT, or ROLLBACK",
                found: other,
                offset: head.span.start as usize,
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

    fn parse_expr_with_precedence_inner(&mut self, min_prec: u8) -> Result<Expr, ParseError> {
        let mut left = self.parse_prefix()?;

        while let Some((op, op_span)) = self.peek_binary_op()? {
            let prec = op.precedence();
            if prec < min_prec {
                break;
            }
            // Consume the operator tokens (may be 1 or 2 lexer
            // tokens — e.g. `NOT LIKE`).
            self.consume_binary_op(op)?;

            let next_min = if op.is_right_associative() {
                prec
            } else {
                prec + 1
            };

            // Special case: IS NULL / IS NOT NULL — parsed by the
            // prefix-handler path, not as a binary op. But we handle
            // a bare `IS NULL` here by ignoring it; the prefix code
            // covers it.
            let right = self.parse_expr_with_precedence(next_min)?;
            let span = Span::new(left.span().start, right.span().end);
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
                span: Span::new(span.start, span.end.max(op_span.end)),
            };
        }
        // (`op_span` is the position of the operator within the
        // current iteration; we keep it so future error messages can
        // point at the operator, not the right-hand operand.)

        // Trailing IS NULL / IS NOT NULL.
        if self.peek()?.kind == TokenKind::KwIs {
            let is_tok = self.advance()?;
            let negated = self.match_kw(TokenKind::KwNot);
            self.expect(TokenKind::KwNull, "NULL")?;
            let span = Span::new(left.span().start, is_tok.span.end);
            left = Expr::IsNull {
                expr: Box::new(left),
                negated,
                span,
            };
        }

        Ok(left)
    }

    fn parse_prefix(&mut self) -> Result<Expr, ParseError> {
        let tok = self.peek()?;
        match tok.kind {
            TokenKind::Plus | TokenKind::Minus | TokenKind::KwNot => {
                let op_tok = self.advance()?;
                let op = match op_tok.kind {
                    TokenKind::Plus => UnaryOp::Pos,
                    TokenKind::Minus => UnaryOp::Neg,
                    TokenKind::KwNot => UnaryOp::Not,
                    _ => unreachable!(),
                };
                let rhs = self.parse_expr_with_precedence(7)?;
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
                let lp = self.advance()?;
                let inner = self.parse_expr()?;
                let rp = self.expect(TokenKind::RParen, ")")?;
                Ok(Expr::Paren {
                    expr: Box::new(inner),
                    span: Span::new(lp.span.start, rp.span.end),
                })
            }

            TokenKind::KwCast => self.parse_cast_expr(),

            TokenKind::Identifier | TokenKind::QuotedIdentifier => self.parse_ident_or_call(),

            other => Err(ParseError::Expected {
                expected: "expression",
                found: other,
                offset: tok.span.start as usize,
            }),
        }
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
                    args.push(self.parse_expr()?);
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
        // calling lookahead_at for NOT LIKE / NOT ILIKE.
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
            TokenKind::KwNot => {
                // NOT LIKE / NOT ILIKE
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
    use crate::ast::{BinaryOp, Expr, SelectItem, SortDirection, Statement};

    fn parse(src: &str) -> Statement {
        Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
    }

    #[test]
    fn select_star() {
        let stmt = parse("SELECT * FROM users");
        let Statement::Select(s) = stmt else { panic!() };
        assert!(!s.distinct);
        assert!(matches!(s.projection[0], SelectItem::Wildcard { .. }));
        assert!(s.from.is_some());
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
        assert!(s.from.is_none());
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
        let err = Parser::new("DROP TABLE t").parse_statement().unwrap_err();
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
}
