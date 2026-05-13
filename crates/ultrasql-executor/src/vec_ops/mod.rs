//! Vectorized operator implementations for the push-based pipeline.
//!
//! Each operator here implements `VectorizedOperator` and processes data in
//! 4096-row batches. Operators are wired together via `VectorizedSink`.
//!
//! ## Operators
//!
//! - [`VectorizedSeqScan`]       — emits 4096-row batches from a `MemTableScan`.
//! - [`VectorizedFilter`]        — applies a SIMD selection vector.
//! - [`VectorizedProject`]       — column-wise projection.
//! - [`VectorizedHashJoin`]      — hash build + probe over batches.
//! - [`VectorizedHashAggregate`] — batch-wise hash aggregate.
//! - [`VectorizedSort`]          — in-memory sort over all batches.

pub mod hash_aggregate;
pub mod hash_join;
pub mod project;
pub mod scan;
pub mod sort;
pub mod vec_filter;

pub use hash_aggregate::VectorizedHashAggregate;
pub use hash_join::VectorizedHashJoin;
pub use project::VectorizedProject;
pub use scan::VectorizedSeqScan;
pub use sort::VectorizedSort;
pub use vec_filter::VectorizedFilter;
