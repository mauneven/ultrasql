//! UltraSQL vectorized execution primitives.
//!
//! Column-oriented in-memory format with explicit null bitmaps,
//! length-prefixed varbinary buffers, and aligned numeric storage.
//! Kernels are auto-vectorized; hot paths have hand-written NEON
//! intrinsics for ARM64 and AVX2 / AVX-512 for `x86_64`.
//!
//! Crate layout
//! ------------
//!
//! - [`bitmap`] — packed null bitmap with set/get and a popcount
//!   primitive.
//! - [`mod@column`] — typed columnar buffers (`Int32`, `Int64`, `Float64`,
//!   `Bool`, `Utf8`). Each variant exposes a slice accessor and a
//!   nulls bitmap.
//! - [`batch`] — `Batch`: an ordered set of `Column`s with a uniform
//!   length. The batch is the input and output unit of every
//!   vectorized operator.
//! - [`kernels`] — `filter`, `compare`, `arithmetic`, `aggregate`,
//!   plus a scaffold for SIMD specializations.

#![forbid(unsafe_op_in_unsafe_fn)]

pub mod batch;
pub mod bitmap;
pub mod column;
pub mod kernels;

pub use batch::{Batch, BatchError};
pub use bitmap::Bitmap;
pub use column::{Column, ColumnError};
pub use kernels::{
    cmp_gt_i64, count_i64, eq_i32, max_i64, min_f64, min_i64, range_mask_i64, select_i32, sum_i64,
    sum_i64_with_mask,
};
