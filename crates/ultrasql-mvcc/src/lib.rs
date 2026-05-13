//! UltraSQL multi-version concurrency control.
//!
//! Tuples carry `xmin`/`xmax`/`cmin`/`cmax` headers. Snapshots are computed
//! by sampling the active transaction set. Visibility predicates implement
//! PostgreSQL `HeapTupleSatisfiesMVCC` semantics, generalized to support
//! both snapshot isolation and serializable isolation.

#![forbid(unsafe_op_in_unsafe_fn)]
