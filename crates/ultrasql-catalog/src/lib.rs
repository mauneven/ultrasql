//! UltraSQL system catalog.
//!
//! Catalog entries are versioned with the same MVCC machinery as user data,
//! so schema changes participate in transactions. The in-memory cache is an
//! arc-swap of an immutable snapshot — readers are wait-free.

#![forbid(unsafe_op_in_unsafe_fn)]
