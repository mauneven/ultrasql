use crate::ast::CreateOperatorStmt;
use crate::parser::{ParseError, Parser};
use crate::token::TokenKind;

impl<'src> Parser<'src> {
    /// Parse `CREATE OPERATOR name (...)`.
    pub(crate) fn parse_create_operator(
        &mut self,
        create_start: u32,
    ) -> Result<CreateOperatorStmt, ParseError> {
        self.expect_identifier_keyword("operator", "OPERATOR")?;
        let name = self.parse_create_operator_name()?;
        self.expect(TokenKind::LParen, "(")?;

        let mut left_arg = None;
        let mut right_arg = None;
        let mut procedure = None;
        loop {
            let option = self.parse_identifier()?;
            self.expect(TokenKind::Eq, "=")?;
            match option.value.as_str() {
                "leftarg" => left_arg = Some(self.parse_ddl_type_name()?),
                "rightarg" => right_arg = Some(self.parse_ddl_type_name()?),
                "procedure" | "function" => procedure = Some(self.parse_object_name()?),
                _ => {
                    return Err(ParseError::Unsupported {
                        what: "CREATE OPERATOR option",
                        offset: option.span.start as usize,
                    });
                }
            }
            if self.peek()?.kind == TokenKind::Comma {
                self.advance()?;
            } else {
                break;
            }
        }
        let rp = self.expect(TokenKind::RParen, ")")?;
        let procedure = match procedure {
            Some(procedure) => procedure,
            None => {
                return Err(ParseError::Expected {
                    expected: "PROCEDURE option",
                    found: TokenKind::RParen,
                    offset: rp.span.start as usize,
                });
            }
        };
        Ok(CreateOperatorStmt {
            name,
            left_arg,
            right_arg,
            procedure,
            span: crate::span::Span::new(create_start, rp.span.end),
        })
    }

    fn parse_create_operator_name(&mut self) -> Result<String, ParseError> {
        let first = *self.peek()?;
        if !is_create_operator_name_token(first.kind) {
            return Err(ParseError::Expected {
                expected: "operator name",
                found: first.kind,
                offset: first.span.start as usize,
            });
        }
        let mut end = first.span.start;
        let mut name = String::new();
        loop {
            let tok = *self.peek()?;
            if !is_create_operator_name_token(tok.kind) {
                break;
            }
            if end != tok.span.start {
                return Err(ParseError::Expected {
                    expected: "contiguous operator name",
                    found: tok.kind,
                    offset: tok.span.start as usize,
                });
            }
            let raw = tok.text(self.source).unwrap_or("");
            name.push_str(raw);
            end = tok.span.end;
            self.advance()?;
            if self.peek()?.kind == TokenKind::LParen {
                break;
            }
        }
        Ok(name)
    }
}

fn is_create_operator_name_token(kind: TokenKind) -> bool {
    matches!(
        kind,
        TokenKind::Plus
            | TokenKind::Minus
            | TokenKind::Star
            | TokenKind::Slash
            | TokenKind::Percent
            | TokenKind::Caret
            | TokenKind::Eq
            | TokenKind::NotEq
            | TokenKind::Lt
            | TokenKind::LtEq
            | TokenKind::Gt
            | TokenKind::GtEq
            | TokenKind::VectorL2Distance
            | TokenKind::VectorNegativeInnerProduct
            | TokenKind::VectorCosineDistance
            | TokenKind::VectorL1Distance
            | TokenKind::Concat
            | TokenKind::Arrow
            | TokenKind::ArrowDouble
            | TokenKind::HashArrow
            | TokenKind::HashArrowDouble
            | TokenKind::AtArrow
            | TokenKind::AtAt
            | TokenKind::ArrowAt
            | TokenKind::Overlap
            | TokenKind::Tilde
            | TokenKind::NotTilde
            | TokenKind::TildeStar
            | TokenKind::NotTildeStar
            | TokenKind::Ampersand
            | TokenKind::Pipe
            | TokenKind::Hash
            | TokenKind::ShiftLeft
            | TokenKind::ShiftRight
            | TokenKind::ShiftLeftEq
            | TokenKind::ShiftRightEq
            | TokenKind::QuestionMark
            | TokenKind::QuestionPipe
            | TokenKind::QuestionAmpersand
    )
}
