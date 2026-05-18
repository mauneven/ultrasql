//! Per-statement parser modules.
//!
//! Each sub-module implements an `impl<'src> Parser<'src>` block covering
//! one SQL statement family. The top-level dispatch in `parser.rs` routes
//! to these methods based on the leading keyword.

pub(crate) mod alter_table;
pub(crate) mod comment;
pub(crate) mod copy;
pub(crate) mod create_index;
pub(crate) mod create_schema;
pub(crate) mod create_sequence;
pub(crate) mod create_table;
pub(crate) mod delete;
pub(crate) mod drop_index;
pub(crate) mod drop_table;
pub(crate) mod explain;
pub(crate) mod insert;
pub(crate) mod listen;
pub(crate) mod prepare;
pub(crate) mod savepoint;
pub(crate) mod select;
pub(crate) mod set_stmt;
pub(crate) mod truncate;
pub(crate) mod update;
