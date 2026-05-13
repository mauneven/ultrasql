//! Per-statement parser modules.
//!
//! Each sub-module implements an `impl<'src> Parser<'src>` block covering
//! one SQL statement family. The top-level dispatch in `parser.rs` routes
//! to these methods based on the leading keyword.

pub(crate) mod delete;
pub(crate) mod insert;
pub(crate) mod select;
pub(crate) mod truncate;
pub(crate) mod update;
