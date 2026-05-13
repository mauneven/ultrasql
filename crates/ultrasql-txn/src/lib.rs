//! UltraSQL transaction manager.
//!
//! Coordinates xact lifecycle, snapshot acquisition, lock acquisition, and
//! WAL flushes. The lock manager uses a fastpath for relation-level locks
//! and a heavier wait-for graph for row-level conflicts.

#![forbid(unsafe_op_in_unsafe_fn)]
