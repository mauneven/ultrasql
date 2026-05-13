//! UltraSQL PostgreSQL wire protocol v3.
//!
//! Implements startup negotiation, SASL/SCRAM-SHA-256 auth, simple query,
//! extended query (Parse/Bind/Describe/Execute/Sync), copy protocol, and
//! notification streams. The protocol layer is pure data shuffling; query
//! semantics live in higher crates.

#![forbid(unsafe_op_in_unsafe_fn)]
