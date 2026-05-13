//! UltraSQL SQL parser.
//!
//! Pipeline: source text → lexer → token stream → recursive-descent parser
//! with Pratt-style expression precedence → typed AST. The grammar tracks
//! the PostgreSQL dialect; deviations are documented in
//! `docs/dialect.md`.

#![forbid(unsafe_op_in_unsafe_fn)]

pub mod ast;
pub mod keywords;
pub mod lexer;
pub mod parser;
pub mod span;
pub mod token;

pub use lexer::Lexer;
pub use parser::{ParseError, Parser};
pub use span::Span;
pub use token::{Token, TokenKind};
