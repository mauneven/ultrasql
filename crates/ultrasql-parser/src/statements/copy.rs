//! Parser methods for `COPY` statements.
//!
//! Covers the v0.9 surface:
//!
//! ```text
//! COPY table [(col_list)] { FROM | TO } { STDIN | STDOUT | 'file' }
//! COPY (SELECT ...) TO { STDOUT | 'file' }
//!     [WITH (FORMAT { TEXT | CSV | BINARY }, DELIMITER 'c',
//!            HEADER [bool], AUTO_DETECT [bool], IGNORE_ERRORS [bool],
//!            MAX_ERRORS n, REJECT_TABLE 'table', NULL 'string')]
//! ```

use crate::ast::{CopyDirection, CopyFormat, CopyOption, CopySource, CopyStmt, Identifier};
use crate::parser::{ParseError, Parser};
use crate::span::Span;
use crate::token::TokenKind;

const COPY_WITH_OPTION_EXPECTED: &str = "FORMAT, DELIMITER, HEADER, AUTO_DETECT, IGNORE_ERRORS, MAX_ERRORS, REJECT_TABLE, or NULL inside COPY WITH";

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
        let (table, query, columns) = if self.peek()?.kind == TokenKind::LParen {
            self.advance()?;
            if self.peek()?.kind == TokenKind::KwSelect {
                let query = self.parse_select()?;
                self.expect(TokenKind::RParen, ") after COPY query")?;
                (None, Some(Box::new(query)), Vec::new())
            } else {
                return Err(ParseError::Expected {
                    expected: "SELECT inside COPY (...)",
                    found: self.peek()?.kind,
                    offset: self.peek()?.span.start as usize,
                });
            }
        } else {
            let table = self.parse_object_name()?;
            let columns = if self.peek()?.kind == TokenKind::LParen {
                self.parse_copy_column_list()?
            } else {
                Vec::new()
            };
            (Some(table), None, columns)
        };

        let columns = if table.is_some() && self.peek()?.kind == TokenKind::LParen {
            self.parse_copy_column_list()?
        } else {
            columns
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
            (_, TokenKind::String) => {
                let path = self.parse_copy_string_literal("COPY file path")?;
                CopySource::File(path)
            }
            (CopyDirection::From, _) => {
                return Err(ParseError::Expected {
                    expected: "STDIN or server-side file path after COPY ... FROM",
                    found: src_tok.kind,
                    offset: src_tok.span.start as usize,
                });
            }
            (CopyDirection::To, _) => {
                return Err(ParseError::Expected {
                    expected: "STDOUT or server-side file path after COPY ... TO",
                    found: src_tok.kind,
                    offset: src_tok.span.start as usize,
                });
            }
        };

        if query.is_some() && direction == CopyDirection::From {
            return Err(ParseError::Expected {
                expected: "COPY (SELECT ...) TO ...",
                found: TokenKind::KwFrom,
                offset: dir_tok.span.start as usize,
            });
        }

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
            query,
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
                self.parse_copy_optional_equals()?;
                let fmt_tok = *self.peek()?;
                let fmt = match fmt_tok.kind {
                    TokenKind::KwText | TokenKind::Identifier => {
                        let raw = fmt_tok.text(self.source).unwrap_or("");
                        let lower = raw.to_ascii_lowercase();
                        match lower.as_str() {
                            "text" => CopyFormat::Text,
                            "csv" => CopyFormat::Csv,
                            "binary" => CopyFormat::Binary,
                            "parquet" => CopyFormat::Parquet,
                            _ => {
                                return Err(ParseError::Expected {
                                    expected: "TEXT, CSV, BINARY, or PARQUET after FORMAT",
                                    found: fmt_tok.kind,
                                    offset: fmt_tok.span.start as usize,
                                });
                            }
                        }
                    }
                    TokenKind::KwCsv => CopyFormat::Csv,
                    _ => {
                        return Err(ParseError::Expected {
                            expected: "TEXT, CSV, BINARY, or PARQUET after FORMAT",
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
                self.parse_copy_optional_equals()?;
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
                self.parse_copy_optional_equals()?;
                let header_value = self.parse_copy_optional_bool()?;
                Ok(CopyOption::Header(header_value))
            }
            TokenKind::Identifier => {
                let raw = tok.text(self.source).unwrap_or("");
                self.advance()?;
                self.parse_copy_optional_equals()?;
                if raw.eq_ignore_ascii_case("auto_detect") {
                    let auto_detect = self.parse_copy_optional_bool()?;
                    return Ok(CopyOption::AutoDetect(auto_detect));
                }
                if raw.eq_ignore_ascii_case("ignore_errors") {
                    let ignore_errors = self.parse_copy_optional_bool()?;
                    return Ok(CopyOption::IgnoreErrors(ignore_errors));
                }
                if raw.eq_ignore_ascii_case("max_errors") {
                    let max_errors = self.parse_copy_u64_literal("MAX_ERRORS")?;
                    return Ok(CopyOption::MaxErrors(max_errors));
                }
                if raw.eq_ignore_ascii_case("reject_table") {
                    let table = self.parse_copy_string_literal("REJECT_TABLE")?;
                    return Ok(CopyOption::RejectTable(table));
                }
                Err(ParseError::Expected {
                    expected: COPY_WITH_OPTION_EXPECTED,
                    found: tok.kind,
                    offset: tok.span.start as usize,
                })
            }
            TokenKind::KwNull => {
                self.advance()?;
                self.parse_copy_optional_equals()?;
                let value = self.parse_copy_string_literal("NULL")?;
                Ok(CopyOption::Null(value))
            }
            other => Err(ParseError::Expected {
                expected: COPY_WITH_OPTION_EXPECTED,
                found: other,
                offset: tok.span.start as usize,
            }),
        }
    }

    fn parse_copy_optional_equals(&mut self) -> Result<(), ParseError> {
        if self.peek()?.kind == TokenKind::Eq {
            self.advance()?;
        }
        Ok(())
    }

    fn parse_copy_optional_bool(&mut self) -> Result<bool, ParseError> {
        match self.peek()?.kind {
            TokenKind::KwTrue => {
                self.advance()?;
                Ok(true)
            }
            TokenKind::KwFalse => {
                self.advance()?;
                Ok(false)
            }
            _ => Ok(true),
        }
    }

    fn parse_copy_u64_literal(&mut self, label: &'static str) -> Result<u64, ParseError> {
        let tok = *self.peek()?;
        if tok.kind != TokenKind::Integer {
            return Err(ParseError::Expected {
                expected: "non-negative integer COPY option value",
                found: tok.kind,
                offset: tok.span.start as usize,
            });
        }
        let raw = tok.text(self.source).unwrap_or("");
        let value = raw.parse::<u64>().map_err(|_| ParseError::Expected {
            expected: label,
            found: tok.kind,
            offset: tok.span.start as usize,
        })?;
        self.advance()?;
        Ok(value)
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
        assert_eq!(stmt.table.as_ref().expect("table").to_string(), "users");
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
    fn copy_with_auto_detect() {
        let stmt = parse_copy("COPY t FROM 'file.csv' WITH (FORMAT csv, AUTO_DETECT true)");
        let auto_detect = stmt
            .options
            .iter()
            .find_map(|o| match o {
                CopyOption::AutoDetect(v) => Some(*v),
                _ => None,
            })
            .expect("auto_detect option present");
        assert!(auto_detect);
    }

    #[test]
    fn copy_with_quarantine_options() {
        let stmt = parse_copy(
            "COPY t FROM 'file.csv' WITH \
             (FORMAT = csv, IGNORE_ERRORS = true, MAX_ERRORS = 1000, REJECT_TABLE = 'bad_rows')",
        );
        assert!(
            stmt.options
                .iter()
                .any(|o| { matches!(o, CopyOption::IgnoreErrors(v) if *v) })
        );
        assert!(
            stmt.options
                .iter()
                .any(|o| { matches!(o, CopyOption::MaxErrors(1000)) })
        );
        assert!(
            stmt.options
                .iter()
                .any(|o| { matches!(o, CopyOption::RejectTable(table) if table == "bad_rows") })
        );
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
    fn copy_from_filename_is_accepted() {
        let stmt = parse_copy("COPY t FROM 'file.csv'");
        assert_eq!(stmt.source, CopySource::File("file.csv".to_string()));
    }

    #[test]
    fn copy_binary_format_is_accepted() {
        let stmt = parse_copy("COPY t FROM STDIN WITH (FORMAT binary)");
        assert_eq!(stmt.format, CopyFormat::Binary);
    }

    #[test]
    fn copy_parquet_format_is_accepted() {
        let stmt = parse_copy("COPY t FROM 'file.parquet' WITH (FORMAT parquet)");
        assert_eq!(stmt.format, CopyFormat::Parquet);
    }

    #[test]
    fn copy_query_to_stdout_is_accepted() {
        let stmt = parse_copy("COPY (SELECT id FROM users) TO STDOUT");
        assert!(stmt.table.is_none());
        assert!(stmt.query.is_some());
        assert_eq!(stmt.source, CopySource::Stdout);
    }
}
