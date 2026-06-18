//! UltraSQL SQL parser.
//!
//! Pipeline: source text → lexer → token stream → recursive-descent parser
//! with Pratt-style expression precedence → typed AST. The grammar tracks
//! the PostgreSQL dialect; deviations are documented in
//! `docs/dialect.md`.

#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    clippy::cast_possible_wrap
)]
// Panic hardening: production (non-test) parser code must not `.unwrap()`,
// `.expect()`, or `panic!`. Fallible sites propagate errors; proven invariants
// carry a per-site `#[allow]` with an `// INVARIANT:` justification.
// `#[cfg(test)]` modules are exempt.
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

pub mod ast;
pub mod keywords;
pub mod lexer;
pub mod parser;
pub mod span;
pub mod statements;
pub mod token;

pub use lexer::Lexer;
pub use parser::{ParseError, Parser};
pub use span::Span;
pub use token::{Token, TokenKind};
