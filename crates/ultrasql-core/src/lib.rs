//! UltraSQL core — foundational primitives shared across every subsystem.
//!
//! Nothing in this crate depends on any other UltraSQL crate. It is the
//! lowest layer: error type, primitive identifiers, scalar types, datum
//! representation, schema descriptors, endian helpers, and shared
//! constants.
//!
//! Stability: items here are part of the cross-crate ABI; breaking changes
//! must go through the RFC process.

#![forbid(unsafe_op_in_unsafe_fn)]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod cache;
pub mod constants;
pub mod endian;
pub mod error;
pub mod id;
pub mod schema;
pub mod types;
pub mod value;

pub use error::{Error, Result};
pub use id::{
    BlockNumber, CommandId, Lsn, Oid, PageId, RelationId, SegmentId, TableId, TupleId, Xid,
};
pub use schema::{Field, Schema};
pub use types::DataType;
pub use value::{Datum, Value};

/// Version of the on-disk page format. Bumping this is an RFC-level change.
pub const ON_DISK_FORMAT_VERSION: u32 = 1;

/// Crate version. Pinned at compile time from Cargo.
pub const CRATE_VERSION: &str = env!("CARGO_PKG_VERSION");
