//! Access method trait and implementations for index backends.
//!
//! [`AccessMethod`] is the common interface that every index backend
//! (B-tree, Hash, GIN, `GiST`, BRIN) must satisfy. The trait keeps the
//! surface deliberately narrow so the executor can drive inserts and
//! lookups without knowing which backend is underneath.
//!
//! # Layered position
//!
//! Access methods sit above the buffer pool and below the executor.
//! They own no schema knowledge — the caller supplies pre-encoded keys
//! and receives back [`TupleId`] values.
//!
//! # Status
//!
//! - [`BTreeAccessMethod`]: wraps the existing [`crate::btree::BTree`];
//!   this is the primary persistent B-tree backend and has restart,
//!   concurrency, uniqueness, range-scan, and WAL-failure coverage.
//! - [`HashIndex`]: static hashing with fixed primary bucket pages and
//!   overflow-page chains.
//! - [`HnswIndex`]: runtime ANN graph; [`PageBackedHnswIndex`] adds the
//!   persistent page arena, WAL replay, and VACUUM reclamation path.
//! - [`IvfFlatIndex`]: runtime inverted-list ANN; [`PageBackedIvfFlatIndex`]
//!   adds persistent centroid/list pages and logical WAL replay.
//! - [`GinIndex`], [`GistIndex`], [`BrinIndex`]: provide the trait shape with
//!   happy-path insert/lookup so the catalog and executor can reference the
//!   concrete types. Full type-specific operator-class implementations are
//!   deferred to v1.x.

#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::type_complexity)]

use thiserror::Error;
use ultrasql_core::TupleId;

mod ann;
mod brin;
mod btree;
mod gin;
mod gist;
mod hash;
mod hnsw;
mod hnsw_build;
mod hnsw_page;
mod ivfflat;

#[cfg(test)]
mod tests;

pub use ann::{AnnPayloadKind, AnnRerankPolicy, AnnVectorPayload, HnswMetric, HnswSearchResult};
pub use brin::BrinIndex;
pub use btree::BTreeAccessMethod;
pub use gin::GinIndex;
pub use gist::GistIndex;
pub use hash::HashIndex;
pub use hnsw::HnswIndex;
pub use hnsw_page::{PageBackedHnswIndex, PageBackedHnswPageImage, PageBackedHnswStats};
pub use ivfflat::{
    IvfFlatIndex, IvfFlatSearchResult, PageBackedIvfFlatIndex, PageBackedIvfFlatStats,
};

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors returned by every access method implementation.
#[derive(Debug, Error)]
pub enum AccessMethodError {
    /// The requested key was not found (delete / lookup).
    #[error("key not found")]
    NotFound,

    /// The key is already present and uniqueness is enforced.
    #[error("duplicate key")]
    DuplicateKey,

    /// An internal storage error.
    #[error("storage error: {0}")]
    Storage(String),

    /// The operation is not yet implemented.
    #[error("not implemented: {0}")]
    NotImplemented(&'static str),
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Narrow interface every index backend must implement.
///
/// Keys are pre-encoded byte slices; all type knowledge lives at the
/// caller's boundary. Implementations decide their own internal key
/// comparison semantics (binary, lexicographic, …).
///
/// # Thread safety
///
/// `Send + Sync` is required so a single method handle can be shared
/// across worker threads via `Arc`. Implementations must use interior
/// mutability (e.g. `Mutex`, `RwLock`, atomics) for writable state.
pub trait AccessMethod: Send + Sync + std::fmt::Debug {
    /// Short name of this access method (e.g. `"btree"`, `"hash"`).
    fn name(&self) -> &'static str;

    /// Insert `(key, tid)` into the index.
    ///
    /// Returns [`AccessMethodError::DuplicateKey`] when the index
    /// enforces uniqueness and the key is already present.
    fn insert(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError>;

    /// Look up all [`TupleId`]s matching `key`.
    ///
    /// Returns an empty `Vec` when the key is absent rather than
    /// an error, consistent with how the executor processes misses.
    fn lookup(&self, key: &[u8]) -> Result<Vec<TupleId>, AccessMethodError>;

    /// Remove the specific `(key, tid)` pair from the index.
    ///
    /// Returns [`AccessMethodError::NotFound`] when no matching entry
    /// exists.
    fn delete(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError>;
}
