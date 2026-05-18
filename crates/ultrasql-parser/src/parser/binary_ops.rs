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
}
