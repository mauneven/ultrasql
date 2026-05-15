//! Parser methods for `COPY` statements.
//!
//! Covers the v0.5 surface:
//!
//! ```text
//! COPY table [(col_list)] { FROM | TO } { STDIN | STDOUT }
//!     [WITH (FORMAT { TEXT | CSV }, DELIMITER 'c', HEADER [bool], NULL 'string')]
//! ```
//!
//! File-path sources, the older non-`WITH` option syntax, and binary
//! format are explicitly out of scope; the ROADMAP tracks them under
//! `COPY & Bulk Operations`.

use crate::ast::{CopyDirection, CopyFormat, CopyOption, CopySource, CopyStmt, Identifier};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

impl Parser<'_> {
    /// Parse a complete `COPY` statement, starting from the `COPY` keyword.
    ///
    /// # Errors
    ///
    /// Returns [`ParseError`] for any of:
    /// - unrecognised endpoint (must be `STDIN` for `FROM` or `STDOUT` for `TO`),
    /// - duplicate `WITH` options of the same kind,
    /// - `FORMAT` value other than `TEXT` or `CSV`,
    /// - `DELIMITER` value that is not a single character.
    pub(crate) fn parse_copy(&mut self) -> Result<CopyStmt, ParseError> {
        let start_tok = self.expect(TokenKind::KwCopy, "COPY")?;
        let table = self.parse_object_name()?;

        let columns = if self.peek()?.kind == TokenKind::LParen {
            self.parse_copy_column_list()?
        } else {
            Vec::new()
        };

        let dir_tok = *self.peek()?;
        let direction = match dir_tok.kind {
            TokenKind::KwFrom => {
                self.advance()?;
                CopyDirection::From
            }
            TokenKind::KwTo => {
                self.advance()?;
                CopyDirection::To
            }
            other => {
                return Err(ParseError::Expected {
                    expected: "FROM or TO after COPY target",
                    found: other,
                    offset: dir_tok.span.start as usize,
                });
            }
        };

        let src_tok = *self.peek()?;
        let source = match (direction, src_tok.kind) {
            (CopyDirection::From, TokenKind::KwStdin) => {
                self.advance()?;
                CopySource::Stdin
            }
            (CopyDirection::To, TokenKind::KwStdout) => {
                self.advance()?;
                CopySource::Stdout
            }
            (CopyDirection::From, _) => {
                return Err(ParseError::Expected {
                    expected: "STDIN after COPY ... FROM (v0.5 supports STDIN only)",
                    found: src_tok.kind,
                    offset: src_tok.span.start as usize,
                });
            }
            (CopyDirection::To, _) => {
                return Err(ParseError::Expected {
                    expected: "STDOUT after COPY ... TO (v0.5 supports STDOUT only)",
                    found: src_tok.kind,
                    offset: src_tok.span.start as usize,
                });
            }
        };

        let options = if self.peek()?.kind == TokenKind::KwWith {
            self.advance()?;
            self.parse_copy_options()?
        } else {
            Vec::new()
        };

        let mut format = CopyFormat::Text;
        let mut saw_format = false;
        for opt in &options {
            if let CopyOption::Format(f) = opt {
                if saw_format {
                    return Err(ParseError::Expected {
                        expected: "at most one FORMAT option in COPY WITH",
                        found: TokenKind::KwFormat,
                        offset: start_tok.span.start as usize,
                    });
                }
                format = *f;
                saw_format = true;
            }
        }

        let end = self.peek()?.span.start;
        Ok(CopyStmt {
            table,
            columns,
            direction,
            source,
            format,
            options,
            span: Span::new(start_tok.span.start, end),
        })
    }

    fn parse_copy_column_list(&mut self) -> Result<Vec<Identifier>, ParseError> {
        self.expect(TokenKind::LParen, "(")?;
        let mut cols = Vec::new();
        loop {
            cols.push(self.parse_identifier()?);
            match self.peek()?.kind {
                TokenKind::Comma => {
                    self.advance()?;
                }
                TokenKind::RParen => {
                    self.advance()?;
                    break;
                }
                other => {
                    return Err(ParseError::Expected {
                        expected: "',' or ')' in COPY column list",
                        found: other,
                        offset: self.peek()?.span.start as usize,
                    });
                }
            }
        }
        Ok(cols)
    }

    fn parse_copy_options(&mut self) -> Result<Vec<CopyOption>, ParseError> {
        self.expect(TokenKind::LParen, "(")?;
        let mut opts = Vec::new();
        loop {
            opts.push(self.parse_copy_option()?);
            match self.peek()?.kind {
                TokenKind::Comma => {
                    self.advance()?;
                }
                TokenKind::RParen => {
                    self.advance()?;
                    break;
                }
                other => {
                    return Err(ParseError::Expected {
                        expected: "',' or ')' in COPY WITH options",
                        found: other,
                        offset: self.peek()?.span.start as usize,
                    });
                }
            }
        }
        Ok(opts)
    }

    fn parse_copy_option(&mut self) -> Result<CopyOption, ParseError> {
        let tok = *self.peek()?;
        match tok.kind {
            TokenKind::KwFormat => {
                self.advance()?;
                let fmt_tok = *self.peek()?;
                let fmt = match fmt_tok.kind {
                    TokenKind::KwText | TokenKind::Identifier => {
                        let raw = fmt_tok.text(self.source).unwrap_or("");
                        let lower = raw.to_ascii_lowercase();
                        match lower.as_str() {
                            "text" => CopyFormat::Text,
                            "csv" => CopyFormat::Csv,
                            "binary" => {
                                return Err(ParseError::Expected {
                                    expected: "FORMAT TEXT or CSV (binary not supported in v0.5)",
                                    found: fmt_tok.kind,
                                    offset: fmt_tok.span.start as usize,
                                });
                            }
                            _ => {
                                return Err(ParseError::Expected {
                                    expected: "TEXT or CSV after FORMAT",
                                    found: fmt_tok.kind,
                                    offset: fmt_tok.span.start as usize,
                                });
                            }
                        }
                    }
                    TokenKind::KwCsv => CopyFormat::Csv,
                    _ => {
                        return Err(ParseError::Expected {
                            expected: "TEXT or CSV after FORMAT",
                            found: fmt_tok.kind,
                            offset: fmt_tok.span.start as usize,
                        });
                    }
                };
                self.advance()?;
                Ok(CopyOption::Format(fmt))
            }
            TokenKind::KwDelimiter => {
                self.advance()?;
                let value = self.parse_copy_string_literal("DELIMITER")?;
                let mut chars = value.chars();
                let first = chars.next().ok_or(ParseError::Expected {
                    expected: "single-character DELIMITER value",
                    found: TokenKind::String,
                    offset: tok.span.start as usize,
                })?;
                if chars.next().is_some() {
                    return Err(ParseError::Expected {
                        expected: "single-character DELIMITER value",
                        found: TokenKind::String,
                        offset: tok.span.start as usize,
                    });
                }
                Ok(CopyOption::Delimiter(first))
            }
            TokenKind::KwHeader => {
                self.advance()?;
                let header_value = match self.peek()?.kind {
                    TokenKind::KwTrue => {
                        self.advance()?;
                        true
                    }
                    TokenKind::KwFalse => {
                        self.advance()?;
                        false
                    }
                    _ => true,
                };
                Ok(CopyOption::Header(header_value))
            }
            TokenKind::KwNull => {
                self.advance()?;
                let value = self.parse_copy_string_literal("NULL")?;
                Ok(CopyOption::Null(value))
            }
            other => Err(ParseError::Expected {
                expected: "FORMAT, DELIMITER, HEADER, or NULL inside COPY WITH",
                found: other,
                offset: tok.span.start as usize,
            }),
        }
    }

    fn parse_copy_string_literal(&mut self, label: &'static str) -> Result<String, ParseError> {
        let tok = *self.peek()?;
        match tok.kind {
            TokenKind::String => {
                let tok = self.advance()?;
                let raw = tok.text(self.source).unwrap_or("''");
                let inner = if raw.len() >= 2 {
                    &raw[1..raw.len() - 1]
                } else {
                    ""
                };
                Ok(inner.replace("''", "'"))
            }
            other => Err(ParseError::Expected {
                expected: label,
                found: other,
                offset: tok.span.start as usize,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::{CopyDirection, CopyFormat, CopyOption, CopySource, CopyStmt, Statement};
    use crate::parser::Parser;

    fn parse_copy(src: &str) -> CopyStmt {
        match Parser::new(src)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
        {
            Statement::Copy(s) => *s,
            other => panic!("expected Copy, got {other:?}"),
        }
    }

    #[test]
    fn copy_from_stdin_text_default() {
        let stmt = parse_copy("COPY users FROM STDIN");
        assert_eq!(stmt.table.to_string(), "users");
        assert!(stmt.columns.is_empty());
        assert_eq!(stmt.direction, CopyDirection::From);
        assert_eq!(stmt.source, CopySource::Stdin);
        assert_eq!(stmt.format, CopyFormat::Text);
        assert!(stmt.options.is_empty());
    }

    #[test]
    fn copy_to_stdout_text_default() {
        let stmt = parse_copy("COPY events TO STDOUT");
        assert_eq!(stmt.direction, CopyDirection::To);
        assert_eq!(stmt.source, CopySource::Stdout);
        assert_eq!(stmt.format, CopyFormat::Text);
    }

    #[test]
    fn copy_with_column_list() {
        let stmt = parse_copy("COPY users (id, name) FROM STDIN");
        assert_eq!(stmt.columns.len(), 2);
        assert_eq!(stmt.columns[0].value, "id");
        assert_eq!(stmt.columns[1].value, "name");
    }

    #[test]
    fn copy_with_format_csv() {
        let stmt = parse_copy("COPY t TO STDOUT WITH (FORMAT CSV)");
        assert_eq!(stmt.format, CopyFormat::Csv);
    }

    #[test]
    fn copy_with_delimiter_pipe() {
        let stmt = parse_copy("COPY t FROM STDIN WITH (DELIMITER '|')");
        let delim = stmt
            .options
            .iter()
            .find_map(|o| match o {
                CopyOption::Delimiter(c) => Some(*c),
                _ => None,
            })
            .expect("delimiter option present");
        assert_eq!(delim, '|');
    }

    #[test]
    fn copy_with_header_default_true() {
        let stmt = parse_copy("COPY t FROM STDIN WITH (HEADER)");
        let hdr = stmt
            .options
            .iter()
            .find_map(|o| match o {
                CopyOption::Header(v) => Some(*v),
                _ => None,
            })
            .expect("header option present");
        assert!(hdr);
    }

    #[test]
    fn copy_with_null_marker() {
        let stmt = parse_copy("COPY t FROM STDIN WITH (NULL 'NULL')");
        let null_str = stmt
            .options
            .iter()
            .find_map(|o| match o {
                CopyOption::Null(s) => Some(s.clone()),
                _ => None,
            })
            .expect("null option present");
        assert_eq!(null_str, "NULL");
    }

    #[test]
    fn copy_from_filename_is_rejected() {
        let err = Parser::new("COPY t FROM 'file.csv'")
            .parse_statement()
            .unwrap_err();
        assert!(format!("{err}").contains("STDIN"));
    }

    #[test]
    fn copy_binary_format_is_rejected() {
        let err = Parser::new("COPY t FROM STDIN WITH (FORMAT binary)")
            .parse_statement()
            .unwrap_err();
        assert!(format!("{err}").contains("binary"));
    }
}
