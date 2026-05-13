//! UltraSQL SQL parser.
//!
//! Pipeline: source text → lexer → token stream → recursive-descent parser
//! with Pratt-style expression precedence → typed AST. The grammar tracks
//! the PostgreSQL dialect; deviations are documented in
//! `docs/dialect.md`.

#![forbid(unsafe_op_in_unsafe_fn)]
