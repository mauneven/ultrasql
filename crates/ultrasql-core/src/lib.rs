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
// AGENTS.md §3.3: deny `as` integer-width casts at the crate boundary.
// Use `try_from` + propagate, `From::from` for lossless widening, or a
// `#[allow(...)]` with a justification comment. This crate is at the
// foot of the dependency graph so the surface here is the easiest to
// keep clean.
#![deny(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    clippy::cast_possible_wrap
)]
// Panic hardening: production (non-test) core code must not `.unwrap()`,
// `.expect()`, or `panic!`. Fallible sites propagate errors; proven invariants
// carry a per-site `#[allow]` with an `// INVARIANT:` justification.
// `#[cfg(test)]` modules are exempt.
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

pub mod bit_string;
pub mod bpchar;
pub mod cache;
pub mod constants;
pub mod csv;
pub mod decimal;
pub mod endian;
pub mod error;
pub mod fsync;
pub mod id;
pub mod money;
pub mod network;
pub mod schema;
pub mod types;
pub mod value;

pub use bit_string::BitString;
pub use bpchar::{BpCharError, bpchar_semantic_text, coerce_bpchar_text};
pub use decimal::{
    DecimalError, decode_pg_numeric_binary, encode_pg_numeric_binary, parse_decimal_text,
};
pub use error::{Error, Result};
pub use id::{
    BlockNumber, CommandId, Lsn, Oid, PageId, RelationId, SegmentId, TableId, TupleId, Xid,
};
pub use money::{
    MoneyError, decode_pg_money_binary, encode_pg_money_binary, format_money_text,
    format_money_text_with_locale, parse_money_text,
};
pub use network::{InetAddr, MacAddr, MacAddr8, NetworkValue};
pub use schema::{Field, Schema};
pub use types::{
    DataType, GeometryType, MAX_VECTOR_DIMS, RangeType, composite_text_arity,
    composite_text_matches_arity,
};
pub use value::{
    BoundingBox, Datum, GeometryValue, MICROS_PER_DAY, RangeValue, SparseVector,
    TimestampTzDisplay, Value, date_parts_from_days, format_date_days, format_interval_pg,
    format_time_micros, format_timestamp_micros, format_timestamptz_micros_in_timezone,
    format_timestamptz_micros_utc, format_timetz, format_timezone_offset_seconds, pack_timetz,
    parse_date_text, parse_interval_pg, parse_time_text, parse_timestamp_text,
    parse_timestamptz_text, parse_timetz_text, timestamp_micros_at_timezone,
    timestamp_parts_from_micros, timestamptz_display_in_timezone, timetz_at_timezone,
    timetz_utc_micros, unpack_timetz, xml_content_is_well_formed, xml_document_is_well_formed,
    xml_xpath_element_fragments, xml_xpath_element_fragments_with_namespaces,
};

/// Version of the on-disk page format. Bumping this is an RFC-level change.
pub const ON_DISK_FORMAT_VERSION: u32 = 1;

/// Crate version. Pinned at compile time from Cargo.
pub const CRATE_VERSION: &str = env!("CARGO_PKG_VERSION");
