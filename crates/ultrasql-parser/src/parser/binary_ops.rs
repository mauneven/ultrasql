//! Binary-operator detection helpers used by the Pratt expression loop.
//!
//! [`Parser::peek_binary_op`] returns `Some((BinaryOp, Span))` if the
//! next token (or pair of tokens for `NOT LIKE` / `NOT ILIKE`) names a
//! binary operator, and `None` otherwise — that `None` is how the
//! Pratt loop in [`super::expr`] detects the end of an expression.
//! [`Parser::consume_binary_op`] then advances past the operator's
//! token(s).

use super::{ParseError, Parser};
use crate::ast::BinaryOp;
use crate::span::Span;
use crate::token::TokenKind;

impl<'src> Parser<'src> {
    pub(super) fn peek_binary_op(&mut self) -> Result<Option<(BinaryOp, Span)>, ParseError> {
        // Snapshot the peek values so we can release the borrow before
        // calling lookahead_at for two-token operators such as NOT LIKE.
        let (kind, span) = {
            let tok = self.peek()?;
            (tok.kind, tok.span)
        };
        if self.peek_is_operator_keyword()? {
            return self.peek_schema_qualified_operator(span);
        }
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
            TokenKind::VectorL2Distance => BinaryOp::VectorL2Distance,
            TokenKind::VectorNegativeInnerProduct => BinaryOp::VectorNegativeInnerProduct,
            TokenKind::VectorCosineDistance => BinaryOp::VectorCosineDistance,
            TokenKind::VectorL1Distance => BinaryOp::VectorL1Distance,
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
            TokenKind::ShiftLeftEq => BinaryOp::NetworkContainedEq,
            TokenKind::ShiftRightEq => BinaryOp::NetworkContainsEq,
            // JSON operators.
            TokenKind::Arrow => BinaryOp::JsonGet,
            TokenKind::ArrowDouble => BinaryOp::JsonGetText,
            TokenKind::HashArrow => BinaryOp::JsonGetPath,
            TokenKind::HashArrowDouble => BinaryOp::JsonGetPathText,
            TokenKind::AtArrow => BinaryOp::JsonContains,
            TokenKind::AtAt => BinaryOp::TextSearchMatch,
            TokenKind::ArrowAt => BinaryOp::JsonContained,
            TokenKind::Overlap => BinaryOp::Overlap,
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

    pub(super) fn consume_binary_op(&mut self, op: BinaryOp) -> Result<(), ParseError> {
        if self.peek_is_operator_keyword()? {
            return self.consume_schema_qualified_operator(op);
        }
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

    fn peek_is_operator_keyword(&mut self) -> Result<bool, ParseError> {
        let tok = *self.peek()?;
        Ok(tok.kind == TokenKind::Identifier
            && tok
                .text(self.source)
                .is_some_and(|text| text.eq_ignore_ascii_case("operator")))
    }

    fn peek_schema_qualified_operator(
        &mut self,
        span: Span,
    ) -> Result<Option<(BinaryOp, Span)>, ParseError> {
        if self.lookahead_at(1)?.kind != TokenKind::LParen {
            return Ok(None);
        }
        let Some((op, _operator_distance)) = self.operator_syntax_target()? else {
            return Ok(None);
        };
        Ok(Some((op, span)))
    }

    fn consume_schema_qualified_operator(&mut self, expected: BinaryOp) -> Result<(), ParseError> {
        self.expect_identifier_keyword("operator", "OPERATOR")?;
        self.expect(TokenKind::LParen, "(")?;
        while matches!(
            self.peek()?.kind,
            TokenKind::Identifier | TokenKind::QuotedIdentifier
        ) && self.lookahead_at(1)?.kind == TokenKind::Dot
        {
            self.advance()?;
            self.expect(TokenKind::Dot, ".")?;
        }
        let tok = self.advance()?;
        let Some(op) = operator_token_to_binary_op(tok.kind) else {
            return Err(ParseError::Expected {
                expected: "operator token inside OPERATOR(...)",
                found: tok.kind,
                offset: tok.span.start_usize(),
            });
        };
        if op != expected {
            return Err(ParseError::Expected {
                expected: "matching OPERATOR(...) token",
                found: tok.kind,
                offset: tok.span.start_usize(),
            });
        }
        self.expect(TokenKind::RParen, ")")?;
        Ok(())
    }

    fn operator_syntax_target(&mut self) -> Result<Option<(BinaryOp, usize)>, ParseError> {
        let mut distance = 2;
        loop {
            let tok = self.lookahead_at(distance)?;
            if matches!(
                tok.kind,
                TokenKind::Identifier | TokenKind::QuotedIdentifier
            ) && self.lookahead_at(distance + 1)?.kind == TokenKind::Dot
            {
                distance += 2;
                continue;
            }
            let Some(op) = operator_token_to_binary_op(tok.kind) else {
                return Ok(None);
            };
            if self.lookahead_at(distance + 1)?.kind != TokenKind::RParen {
                return Ok(None);
            }
            return Ok(Some((op, distance)));
        }
    }
}

fn operator_token_to_binary_op(kind: TokenKind) -> Option<BinaryOp> {
    Some(match kind {
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
        TokenKind::VectorL2Distance => BinaryOp::VectorL2Distance,
        TokenKind::VectorNegativeInnerProduct => BinaryOp::VectorNegativeInnerProduct,
        TokenKind::VectorCosineDistance => BinaryOp::VectorCosineDistance,
        TokenKind::VectorL1Distance => BinaryOp::VectorL1Distance,
        TokenKind::Tilde => BinaryOp::RegexMatch,
        TokenKind::TildeStar => BinaryOp::RegexIMatch,
        TokenKind::NotTilde => BinaryOp::RegexNotMatch,
        TokenKind::NotTildeStar => BinaryOp::RegexNotIMatch,
        TokenKind::Ampersand => BinaryOp::BitAnd,
        TokenKind::Pipe => BinaryOp::BitOr,
        TokenKind::Hash => BinaryOp::BitXor,
        TokenKind::ShiftLeft => BinaryOp::ShiftLeft,
        TokenKind::ShiftRight => BinaryOp::ShiftRight,
        TokenKind::ShiftLeftEq => BinaryOp::NetworkContainedEq,
        TokenKind::ShiftRightEq => BinaryOp::NetworkContainsEq,
        TokenKind::Arrow => BinaryOp::JsonGet,
        TokenKind::ArrowDouble => BinaryOp::JsonGetText,
        TokenKind::HashArrow => BinaryOp::JsonGetPath,
        TokenKind::HashArrowDouble => BinaryOp::JsonGetPathText,
        TokenKind::AtArrow => BinaryOp::JsonContains,
        TokenKind::AtAt => BinaryOp::TextSearchMatch,
        TokenKind::ArrowAt => BinaryOp::JsonContained,
        TokenKind::Overlap => BinaryOp::Overlap,
        TokenKind::QuestionMark => BinaryOp::JsonHasKey,
        TokenKind::QuestionPipe => BinaryOp::JsonHasAnyKey,
        TokenKind::QuestionAmpersand => BinaryOp::JsonHasAllKeys,
        _ => return None,
    })
}
