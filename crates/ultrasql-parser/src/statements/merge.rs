//! Parser methods for `MERGE INTO` statements.

use crate::ast::{MergeAction, MergeClause, MergeMatchKind, MergeStmt};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse a complete `MERGE INTO` statement.
    pub(crate) fn parse_merge(&mut self) -> Result<MergeStmt, ParseError> {
        let merge = self.expect_identifier_keyword("merge", "MERGE")?;
        self.expect(TokenKind::KwInto, "INTO")?;
        let target = self.parse_object_name()?;
        let target_alias = self.parse_optional_alias(true)?;
        self.expect(TokenKind::KwUsing, "USING")?;
        let source = self.parse_table_factor()?;
        self.expect(TokenKind::KwOn, "ON")?;
        let on = self.parse_expr()?;

        if self.peek()?.kind != TokenKind::KwWhen {
            let tok = *self.peek()?;
            return Err(ParseError::Expected {
                expected: "WHEN MATCHED or WHEN NOT MATCHED",
                found: tok.kind,
                offset: tok.span.start_usize(),
            });
        }

        let mut clauses = Vec::new();
        while self.peek()?.kind == TokenKind::KwWhen {
            clauses.push(self.parse_merge_clause()?);
        }

        let end = self.peek()?.span.start;
        Ok(MergeStmt {
            target,
            target_alias,
            source,
            on,
            clauses,
            span: Span::new(merge.span.start, end),
        })
    }

    fn parse_merge_clause(&mut self) -> Result<MergeClause, ParseError> {
        let when = self.expect(TokenKind::KwWhen, "WHEN")?;
        let kind = if self.peek()?.kind == TokenKind::KwNot {
            self.advance()?;
            self.expect_identifier_keyword("matched", "MATCHED")?;
            MergeMatchKind::NotMatched
        } else {
            self.expect_identifier_keyword("matched", "MATCHED")?;
            MergeMatchKind::Matched
        };

        let condition = if self.match_kw(TokenKind::KwAnd) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        self.expect(TokenKind::KwThen, "THEN")?;
        let action = self.parse_merge_action(kind)?;
        let end = self.peek()?.span.start;
        Ok(MergeClause {
            kind,
            condition,
            action,
            span: Span::new(when.span.start, end),
        })
    }

    fn parse_merge_action(&mut self, kind: MergeMatchKind) -> Result<MergeAction, ParseError> {
        match (kind, self.peek()?.kind) {
            (MergeMatchKind::Matched, TokenKind::KwUpdate) => {
                self.advance()?;
                self.expect(TokenKind::KwSet, "SET")?;
                Ok(MergeAction::Update {
                    set: self.parse_assignment_list()?,
                })
            }
            (MergeMatchKind::Matched, TokenKind::KwDelete) => {
                self.advance()?;
                Ok(MergeAction::Delete)
            }
            (MergeMatchKind::NotMatched, TokenKind::KwInsert) => {
                self.advance()?;
                let columns = if self.peek()?.kind == TokenKind::LParen {
                    self.parse_insert_column_list()?
                } else {
                    Vec::new()
                };
                self.expect(TokenKind::KwValues, "VALUES")?;
                let values = self.parse_values_row()?;
                Ok(MergeAction::Insert { columns, values })
            }
            (MergeMatchKind::Matched, TokenKind::KwInsert) => {
                let tok = *self.peek()?;
                Err(ParseError::Expected {
                    expected: "UPDATE or DELETE after WHEN MATCHED THEN",
                    found: tok.kind,
                    offset: tok.span.start_usize(),
                })
            }
            (MergeMatchKind::NotMatched, TokenKind::KwUpdate | TokenKind::KwDelete) => {
                let tok = *self.peek()?;
                Err(ParseError::Expected {
                    expected: "INSERT after WHEN NOT MATCHED THEN",
                    found: tok.kind,
                    offset: tok.span.start_usize(),
                })
            }
            (_, other) => Err(ParseError::Expected {
                expected: "UPDATE, DELETE, or INSERT after MERGE THEN",
                found: other,
                offset: self.peek()?.span.start_usize(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::{MergeAction, MergeMatchKind, Statement};
    use crate::parser::{ParseError, Parser};

    fn parse_merge(src: &str) -> crate::ast::MergeStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::Merge(s) => *s,
            other => panic!("expected Merge, got {other:?}"),
        }
    }

    #[test]
    fn merge_update_delete_insert() {
        let stmt = parse_merge(
            "MERGE INTO target AS t \
             USING source AS s \
             ON t.id = s.id \
             WHEN MATCHED AND s.deleted THEN DELETE \
             WHEN MATCHED THEN UPDATE SET value = s.value \
             WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)",
        );
        assert_eq!(stmt.target.to_string(), "target");
        assert!(stmt.target_alias.is_some());
        assert_eq!(stmt.clauses.len(), 3);
        assert_eq!(stmt.clauses[0].kind, MergeMatchKind::Matched);
        assert!(stmt.clauses[0].condition.is_some());
        assert!(matches!(stmt.clauses[0].action, MergeAction::Delete));
        assert!(matches!(stmt.clauses[1].action, MergeAction::Update { .. }));
        assert!(matches!(stmt.clauses[2].action, MergeAction::Insert { .. }));
    }

    #[test]
    fn merge_source_can_be_derived_table() {
        let stmt = parse_merge(
            "MERGE INTO target \
             USING (SELECT id, value FROM staging) AS s \
             ON target.id = s.id \
             WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.value)",
        );
        assert_eq!(stmt.clauses.len(), 1);
        assert_eq!(stmt.clauses[0].kind, MergeMatchKind::NotMatched);
    }

    #[test]
    fn merge_requires_at_least_one_when_clause() {
        let err = Parser::new("MERGE INTO t USING s ON t.id = s.id")
            .parse_statement()
            .unwrap_err();
        assert!(matches!(err, ParseError::Expected { .. }));
    }

    #[test]
    fn merge_rejects_unsupported_do_nothing_action() {
        let err = Parser::new("MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DO NOTHING")
            .parse_statement()
            .unwrap_err();
        assert!(
            err.to_string().contains("UPDATE, DELETE, or INSERT"),
            "unexpected error: {err}"
        );
    }
}
