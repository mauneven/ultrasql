//! UltraSQL system catalog.
//!
//! Owns the canonical metadata for every relation, index, type, and
//! namespace the engine knows about. The public surface is two traits
//! and a default implementation:
//!
//! - [`Catalog`] — read-only lookups. Called on every bound SQL
//!   statement; must stay cheap.
//! - [`MutableCatalog`] — DDL operations. Called by CREATE / DROP /
//!   ANALYZE.
//! - [`InMemoryCatalog`] — sharded-map-backed implementation used
//!   today by the planner, the REPL, and the test harness. A future
//!   persistent implementation will satisfy the same traits by reading
//!   from heap pages of system catalog tables; the field layout of
//!   [`TableEntry`] and [`IndexEntry`] already matches the column set
//!   of those rows so the persistent adapter is a thin decoder. The
//!   migration anchor points are tagged `TODO(catalog-persistent)` in
//!   the source.
//!
//! # Coexistence with the planner's local catalog
//!
//! The planner currently carries a local `Catalog` / `InMemoryCatalog`
//! / `TableMeta` triple sufficient for the binder's needs. That
//! abstraction is *not* removed here: rewriting the planner is a
//! separate, larger commit. This crate ships the persistent-catalog
//! interface; a follow-up RFC will rebind the planner against it.
//!
//! Stability: items here are part of the cross-crate ABI; breaking
//! changes go through the RFC process.

#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    clippy::cast_possible_wrap
)]

pub mod bootstrap;
pub mod encoding;
mod entry;
mod error;
pub mod information_schema;
mod memory;
pub mod persistent;
pub mod rag;
mod traits;
pub mod views;

pub use bootstrap::initial_snapshot;
pub use encoding::{
    DecodeError as RowDecodeError, EncodeError as RowEncodeError, decode_attribute_row,
    encode_attribute_row, schema_from_attributes,
};
pub use entry::{
    CompositeTypeEntry, DomainTypeEntry, EnumLabelEntry, EnumTypeEntry, IndexEntry, TableEntry,
};
pub use error::CatalogError;
pub use memory::{FIRST_USER_OID, InMemoryCatalog};
pub use persistent::{
    CatalogSnapshot, CatalogStats, PersistentCatalog, StatisticExtRow, StatisticRow,
};
pub use traits::{Catalog, MutableCatalog};
