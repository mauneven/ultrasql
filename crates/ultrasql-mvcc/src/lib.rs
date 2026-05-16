//! UltraSQL multi-version concurrency control.
//!
//! Tuples carry `xmin`/`xmax`/`cmin`/`cmax` headers. Snapshots are computed
//! by sampling the active transaction set. Visibility predicates implement
//! PostgreSQL `HeapTupleSatisfiesMVCC` semantics, generalized to support
//! both snapshot isolation and serializable isolation.

#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    clippy::cast_possible_wrap
)]

pub mod snapshot;
pub mod status;
pub mod tuple_header;
pub mod visibility;

pub use snapshot::Snapshot;
pub use status::{XidStatus, XidStatusOracle};
pub use tuple_header::{InfoMask, TupleHeader};
pub use visibility::{NoSubxacts, SubxactOracle, Visibility, is_visible, is_visible_ext};
