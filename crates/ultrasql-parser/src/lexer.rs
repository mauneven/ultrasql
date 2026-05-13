//! Hand-written SQL lexer.
//!
//! The lexer is single-pass, byte-oriented, allocation-free in its hot
//! path, and produces tokens with byte-offset spans into the original
//! source. Identifiers are case-folded only at keyword-lookup time, so
//! the lexer never allocates a temporary lowercase string for ordinary
//! identifiers.
//!
//! Grammar summary
//! ===============
//!
//! - **Whitespace**: ASCII space, tab, CR, LF. Skipped silently.
//! - **Line comments**: `-- ... \n`. Skipped silently.
//! - **Block comments**: `/* ... */`. Nestable to arbitrary depth.
//! - **Identifiers**: `[A-Za-z_][A-Za-z0-9_$]*` (PostgreSQL allows `$`
//!   in continuation characters but not in the first character).
//! - **Quoted identifiers**: `"..."`. Embedded `""` is a literal `"`.
//! - **Integer literals**: `[0-9]+`, `0x[0-9a-fA-F]+`, `0o[0-7]+`,
//!   `0b[01]+`. Underscores between digits are accepted as separators
//!   (matching Rust and recent SQL dialects).
//! - **Float literals**: `[0-9]+\.[0-9]+([eE][+-]?[0-9]+)?` and the
//!   degenerate forms with a leading or trailing `.`. Exponent-only
//!   floats (`1e10`) are also allowed.
//! - **String literals**: `'...'`. Embedded `''` is a literal `'`.
//! - **Escaped strings**: `E'...'` with C-style escapes (`\n`, `\t`,
//!   `\xHH`, `\uHHHH`, `\\`, `\'`).
//! - **Dollar-quoted strings**: `$tag$ ... $tag$` where `tag` may be
//!   empty (`$$ ... $$`).
//! - **Parameters**: `$1`, `$2`, ..., decimal digits only.
//! - **Operators**: standard arithmetic, comparison, logical, plus
//!   PostgreSQL idioms (`||`, `->`, `->>`, `#>`, `#>>`, `@>`, `<@`,
//!   `~`, `~*`, `!~`, `!~*`).

use crate::keywords;
use crate::span::Span;
use crate::token::{Token, TokenKind};

/// Lexer errors surfaced to callers.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LexerError {
    /// An illegal byte appeared in the input stream.
    #[error("unexpected character '{ch}' at offset {offset}")]
    UnexpectedChar {
        /// The unexpected character.
        ch: char,
        /// Byte offset into the source.
        offset: usize,
    },
    /// A string literal was never closed.
    #[error("unterminated string literal starting at offset {offset}")]
    UnterminatedString {
        /// Byte offset of the opening quote.
        offset: usize,
    },
    /// A block comment was never closed.
    #[error("unterminated block comment starting at offset {offset}")]
    UnterminatedBlockComment {
        /// Byte offset of the opening `/*`.
        offset: usize,
    },
    /// A dollar-quoted string was never closed.
    #[error("unterminated dollar-quoted string starting at offset {offset}")]
    UnterminatedDollarString {
        /// Byte offset of the opening `$tag$`.
        offset: usize,
    },
    /// A quoted identifier was never closed.
    #[error("unterminated quoted identifier starting at offset {offset}")]
    UnterminatedIdentifier {
        /// Byte offset of the opening quote.
        offset: usize,
    },
    /// A numeric literal is malformed (e.g. `0x` with no hex digits).
    #[error("invalid number at offset {offset}: {message}")]
    InvalidNumber {
        /// Byte offset of the first character of the number.
        offset: usize,
        /// Human-readable diagnostic.
        message: &'static str,
    },
    /// A parameter (`$N`) was malformed (e.g. `$` followed by non-digit
    /// when not introducing a dollar-quoted string).
    #[error("invalid parameter at offset {offset}")]
    InvalidParameter {
        /// Byte offset of the `$`.
        offset: usize,
    },
}

/// Streaming SQL lexer.
///
/// Construct with [`Lexer::new`], then call [`Lexer::next_token`] until
/// the returned token's kind is [`TokenKind::Eof`].
#[derive(Debug)]
pub struct Lexer<'src> {
    source: &'src str,
    bytes: &'src [u8],
    pos: usize,
}

impl<'src> Lexer<'src> {
    /// Build a lexer over a SQL source string.
    #[inline]
    #[must_use]
    pub const fn new(source: &'src str) -> Self {
        Self {
            source,
            bytes: source.as_bytes(),
            pos: 0,
        }
    }

    /// The underlying source string. Useful for resolving token spans
    /// back to text.
    #[inline]
    #[must_use]
    pub const fn source(&self) -> &'src str {
        self.source
    }

    /// Current byte offset.
    #[inline]
    #[must_use]
    pub const fn offset(&self) -> usize {
        self.pos
    }

    /// Whether the lexer has consumed all of its input.
    #[inline]
    #[must_use]
    pub const fn at_eof(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    /// Lex a single token. Returns [`TokenKind::Eof`] once the input is
    /// exhausted.
    #[allow(clippy::too_many_lines)]
    pub fn next_token(&mut self) -> Result<Token, LexerError> {
        self.skip_trivia()?;

        let start = self.pos;

        let Some(b) = self.peek() else {
            return Ok(Token::new(TokenKind::Eof, Span::from_usize(start, start)));
        };

        // Identifier or keyword (also: E'...' escape strings; B'...' /
        // X'...' bit and hex strings are folded into the String token
        // kind for now).
        if is_ident_start(b) {
            return self.lex_word(start);
        }

        if b.is_ascii_digit() {
            return self.lex_number(start);
        }

        // .number — e.g. ".5"
        if b == b'.' && self.peek_at(1).is_some_and(|c| c.is_ascii_digit()) {
            return self.lex_number(start);
        }

        match b {
            b'\'' => self.lex_string_literal(start),
            b'"' => self.lex_quoted_identifier(start),
            b'$' => self.lex_dollar(start),
            _ => self.lex_punct_or_operator(start),
        }
    }

    /// Collect every token in the source into a `Vec`. Convenience for
    /// tests; production code uses [`Self::next_token`] directly so it
    /// can stream.
    pub fn tokenize(mut self) -> Result<Vec<Token>, LexerError> {
        let mut out = Vec::new();
        loop {
            let t = self.next_token()?;
            let eof = t.kind == TokenKind::Eof;
            out.push(t);
            if eof {
                return Ok(out);
            }
        }
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    #[inline]
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    #[inline]
    fn peek_at(&self, n: usize) -> Option<u8> {
        self.bytes.get(self.pos + n).copied()
    }

    #[inline]
    const fn advance(&mut self) {
        self.pos += 1;
    }

    #[inline]
    const fn advance_n(&mut self, n: usize) {
        self.pos += n;
    }

    fn skip_trivia(&mut self) -> Result<(), LexerError> {
        loop {
            match self.peek() {
                Some(b' ' | b'\t' | b'\r' | b'\n') => {
                    self.advance();
                }
                Some(b'-') if self.peek_at(1) == Some(b'-') => {
                    // Line comment: consume to newline or EOF.
                    self.advance_n(2);
                    while let Some(c) = self.peek() {
                        self.advance();
                        if c == b'\n' {
                            break;
                        }
                    }
                }
                Some(b'/') if self.peek_at(1) == Some(b'*') => {
                    let start = self.pos;
                    self.advance_n(2);
                    let mut depth: usize = 1;
                    while depth > 0 {
                        match self.peek() {
                            Some(b'/') if self.peek_at(1) == Some(b'*') => {
                                self.advance_n(2);
                                depth += 1;
                            }
                            Some(b'*') if self.peek_at(1) == Some(b'/') => {
                                self.advance_n(2);
                                depth -= 1;
                            }
                            Some(_) => {
                                self.advance();
                            }
                            None => {
                                return Err(LexerError::UnterminatedBlockComment { offset: start });
                            }
                        }
                    }
                }
                _ => return Ok(()),
            }
        }
    }

    fn lex_word(&mut self, start: usize) -> Result<Token, LexerError> {
        // Special case: E'...' is an escaped string, not an identifier.
        // Same for e'...'. We detect this by looking at exactly two
        // characters; longer prefixes (e.g. `EXISTS`) take the normal
        // path because the second char is not a single quote.
        if (self.peek() == Some(b'E') || self.peek() == Some(b'e'))
            && self.peek_at(1) == Some(b'\'')
        {
            self.advance(); // consume the E/e
            return self.lex_escaped_string(start);
        }

        // Walk identifier continuation bytes.
        while let Some(b) = self.peek() {
            if is_ident_continue(b) {
                self.advance();
            } else {
                break;
            }
        }
        let end = self.pos;
        let span = Span::from_usize(start, end);
        let raw = &self.source[start..end];

        // Keyword lookup: lowercase only when necessary. For all-ASCII
        // identifiers (the common case) we can do this in-place via a
        // small stack buffer; for now we accept a single allocation
        // for keyword lookup, which the keyword table dedups.
        let mut buf: [u8; 64] = [0; 64];
        let lower_str: &str = if raw.len() <= buf.len()
            && raw.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            for (i, b) in raw.bytes().enumerate() {
                buf[i] = b.to_ascii_lowercase();
            }
            // SAFETY: we wrote only ASCII (lowercased letters, digits,
            // underscores) into `buf`, so the slice is valid UTF-8.
            unsafe { std::str::from_utf8_unchecked(&buf[..raw.len()]) }
        } else {
            // Out-of-line slow path: identifier is too long for the
            // stack buffer or contains non-ASCII bytes. Allocate a
            // temporary string to lowercase into.
            // We deliberately do not short-circuit here because long
            // identifiers are valid SQL.
            let lowered = raw.to_ascii_lowercase();
            return Ok(Token::new(
                keywords::lookup(&lowered).unwrap_or(TokenKind::Identifier),
                span,
            ));
        };

        let kind = keywords::lookup(lower_str).unwrap_or(TokenKind::Identifier);
        Ok(Token::new(kind, span))
    }

    fn lex_number(&mut self, start: usize) -> Result<Token, LexerError> {
        let first = self.peek().unwrap_or(0);
        // Radix prefix?
        if first == b'0' {
            match self.peek_at(1) {
                Some(b'x' | b'X') => return self.lex_radix_number(start, 16),
                Some(b'o' | b'O') => return self.lex_radix_number(start, 8),
                Some(b'b' | b'B') => return self.lex_radix_number(start, 2),
                _ => {}
            }
        }

        let mut saw_digit = false;
        let mut saw_dot = false;
        let mut saw_exp = false;
        // Leading dot (".5") — already in the float path; consume it.
        if first == b'.' {
            saw_dot = true;
            self.advance();
        }

        while let Some(b) = self.peek() {
            match b {
                b'0'..=b'9' => {
                    saw_digit = true;
                    self.advance();
                }
                b'_' if saw_digit => self.advance(),
                b'.' if !saw_dot && !saw_exp => {
                    // Two-dot range `..` is *not* a float. PostgreSQL
                    // does not have one; we conservatively accept `.`
                    // followed by a digit only.
                    if self.peek_at(1).is_some_and(|c| c.is_ascii_digit()) {
                        saw_dot = true;
                        self.advance();
                    } else {
                        break;
                    }
                }
                b'e' | b'E' if !saw_exp => {
                    saw_exp = true;
                    saw_dot = true; // force float-ness; an exp without a dot is still a float
                    self.advance();
                    if matches!(self.peek(), Some(b'+' | b'-')) {
                        self.advance();
                    }
                    if !matches!(self.peek(), Some(b'0'..=b'9')) {
                        return Err(LexerError::InvalidNumber {
                            offset: start,
                            message: "exponent must be followed by digit(s)",
                        });
                    }
                }
                _ => break,
            }
        }

        if !saw_digit {
            return Err(LexerError::InvalidNumber {
                offset: start,
                message: "no digits in numeric literal",
            });
        }

        let end = self.pos;
        let kind = if saw_dot || saw_exp {
            TokenKind::Float
        } else {
            TokenKind::Integer
        };
        Ok(Token::new(kind, Span::from_usize(start, end)))
    }

    fn lex_radix_number(&mut self, start: usize, radix: u32) -> Result<Token, LexerError> {
        self.advance_n(2); // skip 0x / 0o / 0b
        let mut saw_digit = false;
        while let Some(b) = self.peek() {
            let is_digit = match radix {
                2 => b == b'0' || b == b'1',
                8 => (b'0'..=b'7').contains(&b),
                16 => b.is_ascii_hexdigit(),
                _ => unreachable!(),
            };
            if is_digit || (saw_digit && b == b'_') {
                if is_digit {
                    saw_digit = true;
                }
                self.advance();
            } else {
                break;
            }
        }
        if !saw_digit {
            return Err(LexerError::InvalidNumber {
                offset: start,
                message: "radix prefix must be followed by digits",
            });
        }
        Ok(Token::new(
            TokenKind::Integer,
            Span::from_usize(start, self.pos),
        ))
    }

    fn lex_string_literal(&mut self, start: usize) -> Result<Token, LexerError> {
        self.advance(); // consume opening quote
        loop {
            match self.peek() {
                Some(b'\'') => {
                    // Doubled single-quote is an escaped quote.
                    if self.peek_at(1) == Some(b'\'') {
                        self.advance_n(2);
                    } else {
                        self.advance();
                        return Ok(Token::new(
                            TokenKind::String,
                            Span::from_usize(start, self.pos),
                        ));
                    }
                }
                Some(_) => self.advance(),
                None => return Err(LexerError::UnterminatedString { offset: start }),
            }
        }
    }

    fn lex_escaped_string(&mut self, start: usize) -> Result<Token, LexerError> {
        // We're at the opening quote.
        self.advance();
        loop {
            match self.peek() {
                Some(b'\\') => {
                    // Skip the backslash and the following byte
                    // unconditionally; semantic interpretation happens
                    // at the AST builder, not here.
                    self.advance();
                    if self.peek().is_none() {
                        return Err(LexerError::UnterminatedString { offset: start });
                    }
                    self.advance();
                }
                Some(b'\'') => {
                    // Doubled quote rule applies inside E'...' too.
                    if self.peek_at(1) == Some(b'\'') {
                        self.advance_n(2);
                    } else {
                        self.advance();
                        return Ok(Token::new(
                            TokenKind::EscapedString,
                            Span::from_usize(start, self.pos),
                        ));
                    }
                }
                Some(_) => self.advance(),
                None => return Err(LexerError::UnterminatedString { offset: start }),
            }
        }
    }

    fn lex_quoted_identifier(&mut self, start: usize) -> Result<Token, LexerError> {
        self.advance(); // consume opening "
        loop {
            match self.peek() {
                Some(b'"') => {
                    if self.peek_at(1) == Some(b'"') {
                        self.advance_n(2);
                    } else {
                        self.advance();
                        return Ok(Token::new(
                            TokenKind::QuotedIdentifier,
                            Span::from_usize(start, self.pos),
                        ));
                    }
                }
                Some(_) => self.advance(),
                None => return Err(LexerError::UnterminatedIdentifier { offset: start }),
            }
        }
    }

    fn lex_dollar(&mut self, start: usize) -> Result<Token, LexerError> {
        // Three possibilities:
        //   $tag$ ... $tag$    — dollar-quoted string
        //   $$ ... $$          — same with empty tag
        //   $123               — positional parameter
        //
        // The disambiguation is: after the `$`, if we see a digit, it's
        // a parameter; otherwise it's a dollar-quoted string.
        match self.peek_at(1) {
            Some(b'0'..=b'9') => {
                self.advance(); // consume $
                while let Some(b) = self.peek() {
                    if b.is_ascii_digit() {
                        self.advance();
                    } else {
                        break;
                    }
                }
                // $0 is valid in PostgreSQL (no semantic meaning) — we
                // accept it; the binder catches misuse.
                Ok(Token::new(
                    TokenKind::Parameter,
                    Span::from_usize(start, self.pos),
                ))
            }
            _ => self.lex_dollar_string(start),
        }
    }

    fn lex_dollar_string(&mut self, start: usize) -> Result<Token, LexerError> {
        // Tag is `[A-Za-z_][A-Za-z0-9_]*` or empty. NB: unlike a regular
        // identifier, a dollar-quote tag does *not* admit `$` in the
        // continuation alphabet — that byte is the tag terminator.
        self.advance(); // consume opening $
        let tag_start = self.pos;
        if let Some(c) = self.peek() {
            if is_ident_start(c) {
                self.advance();
                while let Some(c2) = self.peek() {
                    if c2.is_ascii_alphanumeric() || c2 == b'_' {
                        self.advance();
                    } else {
                        break;
                    }
                }
            }
        }
        let tag_end = self.pos;
        // We must now see a `$`.
        if self.peek() != Some(b'$') {
            return Err(LexerError::InvalidParameter { offset: start });
        }
        self.advance(); // consume $ closing the opening tag

        let tag_bytes = &self.bytes[tag_start..tag_end];

        // Consume body until we see `$tag$`.
        loop {
            match self.peek() {
                None => return Err(LexerError::UnterminatedDollarString { offset: start }),
                Some(b'$') => {
                    // Try to match the closing tag.
                    let after_dollar = self.pos + 1;
                    let closing_end = after_dollar + tag_bytes.len();
                    if closing_end < self.bytes.len()
                        && &self.bytes[after_dollar..closing_end] == tag_bytes
                        && self.bytes.get(closing_end) == Some(&b'$')
                    {
                        // Consume the full closing $tag$.
                        self.pos = closing_end + 1;
                        return Ok(Token::new(
                            TokenKind::DollarString,
                            Span::from_usize(start, self.pos),
                        ));
                    }
                    self.advance();
                }
                Some(_) => self.advance(),
            }
        }
    }

    fn lex_punct_or_operator(&mut self, start: usize) -> Result<Token, LexerError> {
        let one = |s: &mut Self, kind: TokenKind| {
            s.advance();
            Token::new(kind, Span::from_usize(start, s.pos))
        };

        let two = |s: &mut Self, kind: TokenKind| {
            s.advance_n(2);
            Token::new(kind, Span::from_usize(start, s.pos))
        };

        let three = |s: &mut Self, kind: TokenKind| {
            s.advance_n(3);
            Token::new(kind, Span::from_usize(start, s.pos))
        };

        let b0 = self.peek().expect("checked by caller");
        let b1 = self.peek_at(1);
        let b2 = self.peek_at(2);

        let token = match (b0, b1, b2) {
            // 3-byte operators first.
            (b'!', Some(b'~'), Some(b'*')) => three(self, TokenKind::NotTildeStar),
            (b'-', Some(b'>'), Some(b'>')) => three(self, TokenKind::ArrowDouble),
            (b'#', Some(b'>'), Some(b'>')) => three(self, TokenKind::HashArrowDouble),

            // 2-byte operators.
            (b'<', Some(b'='), _) => two(self, TokenKind::LtEq),
            (b'>', Some(b'='), _) => two(self, TokenKind::GtEq),
            (b'<', Some(b'>'), _) | (b'!', Some(b'='), _) => two(self, TokenKind::NotEq),
            (b'!', Some(b'~'), _) => two(self, TokenKind::NotTilde),
            (b'~', Some(b'*'), _) => two(self, TokenKind::TildeStar),
            (b'|', Some(b'|'), _) => two(self, TokenKind::Concat),
            (b'-', Some(b'>'), _) => two(self, TokenKind::Arrow),
            (b'#', Some(b'>'), _) => two(self, TokenKind::HashArrow),
            (b'@', Some(b'>'), _) => two(self, TokenKind::AtArrow),
            (b'<', Some(b'@'), _) => two(self, TokenKind::ArrowAt),
            (b':', Some(b':'), _) => two(self, TokenKind::ColonColon),

            // 1-byte operators / punctuation.
            (b'(', _, _) => one(self, TokenKind::LParen),
            (b')', _, _) => one(self, TokenKind::RParen),
            (b'[', _, _) => one(self, TokenKind::LBracket),
            (b']', _, _) => one(self, TokenKind::RBracket),
            (b',', _, _) => one(self, TokenKind::Comma),
            (b';', _, _) => one(self, TokenKind::Semicolon),
            (b'.', _, _) => one(self, TokenKind::Dot),
            (b'*', _, _) => one(self, TokenKind::Star),
            (b'+', _, _) => one(self, TokenKind::Plus),
            (b'-', _, _) => one(self, TokenKind::Minus),
            (b'/', _, _) => one(self, TokenKind::Slash),
            (b'%', _, _) => one(self, TokenKind::Percent),
            (b'^', _, _) => one(self, TokenKind::Caret),
            (b'=', _, _) => one(self, TokenKind::Eq),
            (b'<', _, _) => one(self, TokenKind::Lt),
            (b'>', _, _) => one(self, TokenKind::Gt),
            (b'~', _, _) => one(self, TokenKind::Tilde),
            (b':', _, _) => one(self, TokenKind::Colon),
            (b'?', _, _) => one(self, TokenKind::QuestionMark),

            (other, _, _) => {
                // Carve out an error including the actual char. For
                // multi-byte UTF-8, decode just enough to format the
                // diagnostic; we leave the cursor at the bad byte so
                // higher layers can resume.
                let ch = self.source[self.pos..]
                    .chars()
                    .next()
                    .unwrap_or(other as char);
                return Err(LexerError::UnexpectedChar {
                    ch,
                    offset: self.pos,
                });
            }
        };

        Ok(token)
    }
}

#[inline]
const fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

#[inline]
const fn is_ident_continue(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$'
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(src: &str) -> Vec<TokenKind> {
        Lexer::new(src)
            .tokenize()
            .expect("lex ok")
            .into_iter()
            .map(|t| t.kind)
            .collect()
    }

    fn first_kind(src: &str) -> TokenKind {
        Lexer::new(src).next_token().expect("lex ok").kind
    }

    #[test]
    fn empty_input_yields_eof() {
        assert_eq!(kinds(""), vec![TokenKind::Eof]);
        assert_eq!(kinds("    \n  \t"), vec![TokenKind::Eof]);
    }

    #[test]
    fn select_keyword_is_a_keyword() {
        assert_eq!(first_kind("SELECT"), TokenKind::KwSelect);
        assert_eq!(first_kind("select"), TokenKind::KwSelect);
        assert_eq!(first_kind("SeLeCt"), TokenKind::KwSelect);
    }

    #[test]
    fn identifier_versus_keyword() {
        assert_eq!(first_kind("hello"), TokenKind::Identifier);
        assert_eq!(first_kind("_underscore_first"), TokenKind::Identifier);
        assert_eq!(first_kind("id$dollar"), TokenKind::Identifier);
    }

    #[test]
    fn quoted_identifier_preserves_case() {
        let src = r#""SeLeCt""#;
        let toks = Lexer::new(src).tokenize().unwrap();
        assert_eq!(toks[0].kind, TokenKind::QuotedIdentifier);
        assert_eq!(toks[0].text(src).unwrap(), r#""SeLeCt""#);
    }

    #[test]
    fn quoted_identifier_handles_doubled_quotes() {
        let src = r#""a""b""#;
        let toks = Lexer::new(src).tokenize().unwrap();
        assert_eq!(toks[0].kind, TokenKind::QuotedIdentifier);
        assert_eq!(toks[0].text(src).unwrap(), r#""a""b""#);
    }

    #[test]
    fn integer_literal() {
        assert_eq!(first_kind("0"), TokenKind::Integer);
        assert_eq!(first_kind("42"), TokenKind::Integer);
        assert_eq!(first_kind("1_000_000"), TokenKind::Integer);
    }

    #[test]
    fn radix_literals() {
        assert_eq!(first_kind("0x1A"), TokenKind::Integer);
        assert_eq!(first_kind("0b1010"), TokenKind::Integer);
        assert_eq!(first_kind("0o755"), TokenKind::Integer);
    }

    #[test]
    fn invalid_radix_literal() {
        let err = Lexer::new("0x").next_token().unwrap_err();
        assert!(matches!(err, LexerError::InvalidNumber { .. }));
    }

    #[test]
    fn float_literal() {
        assert_eq!(first_kind("1.5"), TokenKind::Float);
        assert_eq!(first_kind(".5"), TokenKind::Float);
        assert_eq!(first_kind("1e10"), TokenKind::Float);
        assert_eq!(first_kind("1.5e-3"), TokenKind::Float);
        assert_eq!(first_kind("1.5E+3"), TokenKind::Float);
    }

    #[test]
    fn invalid_exponent_is_rejected() {
        let err = Lexer::new("1e").next_token().unwrap_err();
        assert!(matches!(err, LexerError::InvalidNumber { .. }));
    }

    #[test]
    fn string_literal_simple() {
        let src = "'hello world'";
        let toks = Lexer::new(src).tokenize().unwrap();
        assert_eq!(toks[0].kind, TokenKind::String);
        assert_eq!(toks[0].text(src).unwrap(), "'hello world'");
    }

    #[test]
    fn string_literal_doubled_quote() {
        let src = "'it''s ok'";
        let toks = Lexer::new(src).tokenize().unwrap();
        assert_eq!(toks[0].kind, TokenKind::String);
        assert_eq!(toks[0].text(src).unwrap(), "'it''s ok'");
    }

    #[test]
    fn unterminated_string_errors() {
        let err = Lexer::new("'oops").next_token().unwrap_err();
        assert!(matches!(err, LexerError::UnterminatedString { .. }));
    }

    #[test]
    fn escape_string_e_prefix() {
        let src = r"E'\n\t\\'";
        let toks = Lexer::new(src).tokenize().unwrap();
        assert_eq!(toks[0].kind, TokenKind::EscapedString);
    }

    #[test]
    fn dollar_quoted_string_empty_tag() {
        let src = "$$body$$";
        let toks = Lexer::new(src).tokenize().unwrap();
        assert_eq!(toks[0].kind, TokenKind::DollarString);
        assert_eq!(toks[0].text(src).unwrap(), "$$body$$");
    }

    #[test]
    fn dollar_quoted_string_named_tag() {
        let src = "$tag$body $$ with dollars $tag$";
        let toks = Lexer::new(src).tokenize().unwrap();
        assert_eq!(toks[0].kind, TokenKind::DollarString);
    }

    #[test]
    fn unterminated_dollar_string_errors() {
        let err = Lexer::new("$tag$body").next_token().unwrap_err();
        assert!(matches!(err, LexerError::UnterminatedDollarString { .. }));
    }

    #[test]
    fn parameter_token() {
        let src = "$1 $42";
        let toks = Lexer::new(src).tokenize().unwrap();
        assert_eq!(toks[0].kind, TokenKind::Parameter);
        assert_eq!(toks[0].text(src).unwrap(), "$1");
        assert_eq!(toks[1].kind, TokenKind::Parameter);
        assert_eq!(toks[1].text(src).unwrap(), "$42");
    }

    #[test]
    fn line_comment_skipped() {
        let src = "SELECT -- comment\n42";
        let k = kinds(src);
        assert_eq!(
            k,
            vec![TokenKind::KwSelect, TokenKind::Integer, TokenKind::Eof]
        );
    }

    #[test]
    fn block_comment_skipped() {
        let src = "SELECT /* hi */ 42";
        let k = kinds(src);
        assert_eq!(
            k,
            vec![TokenKind::KwSelect, TokenKind::Integer, TokenKind::Eof]
        );
    }

    #[test]
    fn nested_block_comments() {
        let src = "SELECT /* outer /* inner */ tail */ 42";
        let k = kinds(src);
        assert_eq!(
            k,
            vec![TokenKind::KwSelect, TokenKind::Integer, TokenKind::Eof]
        );
    }

    #[test]
    fn unterminated_block_comment_errors() {
        let err = Lexer::new("/* unclosed").next_token().unwrap_err();
        assert!(matches!(err, LexerError::UnterminatedBlockComment { .. }));
    }

    #[test]
    fn punctuation_tokens() {
        let src = "( ) [ ] , ; . :: : * ?";
        let k = kinds(src);
        assert_eq!(
            k,
            vec![
                TokenKind::LParen,
                TokenKind::RParen,
                TokenKind::LBracket,
                TokenKind::RBracket,
                TokenKind::Comma,
                TokenKind::Semicolon,
                TokenKind::Dot,
                TokenKind::ColonColon,
                TokenKind::Colon,
                TokenKind::Star,
                TokenKind::QuestionMark,
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn arithmetic_and_comparison_operators() {
        let src = "+ - / % ^ = <> != < <= > >=";
        let k = kinds(src);
        assert_eq!(
            k,
            vec![
                TokenKind::Plus,
                TokenKind::Minus,
                TokenKind::Slash,
                TokenKind::Percent,
                TokenKind::Caret,
                TokenKind::Eq,
                TokenKind::NotEq,
                TokenKind::NotEq,
                TokenKind::Lt,
                TokenKind::LtEq,
                TokenKind::Gt,
                TokenKind::GtEq,
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn postgres_specific_operators() {
        let src = "|| -> ->> #> #>> @> <@ ~ ~* !~ !~*";
        let k = kinds(src);
        assert_eq!(
            k,
            vec![
                TokenKind::Concat,
                TokenKind::Arrow,
                TokenKind::ArrowDouble,
                TokenKind::HashArrow,
                TokenKind::HashArrowDouble,
                TokenKind::AtArrow,
                TokenKind::ArrowAt,
                TokenKind::Tilde,
                TokenKind::TildeStar,
                TokenKind::NotTilde,
                TokenKind::NotTildeStar,
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn span_positions_are_correct() {
        let src = "SELECT 1";
        let toks = Lexer::new(src).tokenize().unwrap();
        assert_eq!(toks[0].span, Span::new(0, 6));
        assert_eq!(toks[1].span, Span::new(7, 8));
    }

    #[test]
    fn full_select_statement() {
        let src = "SELECT id, name FROM users WHERE age >= 18;";
        let k = kinds(src);
        assert_eq!(
            k,
            vec![
                TokenKind::KwSelect,
                TokenKind::Identifier,
                TokenKind::Comma,
                TokenKind::Identifier,
                TokenKind::KwFrom,
                TokenKind::Identifier,
                TokenKind::KwWhere,
                TokenKind::Identifier,
                TokenKind::GtEq,
                TokenKind::Integer,
                TokenKind::Semicolon,
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn unexpected_character_errors() {
        let err = Lexer::new("`").next_token().unwrap_err();
        assert!(matches!(err, LexerError::UnexpectedChar { .. }));
    }
}
