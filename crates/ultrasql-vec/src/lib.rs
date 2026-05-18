//! UltraSQL vectorized execution primitives.
//!
//! Column-oriented in-memory format with explicit null bitmaps,
//! length-prefixed varbinary buffers, and aligned numeric storage.
//! Kernels are auto-vectorized; selected hot paths have hand-written
//! NEON intrinsics for ARM64 and AVX2 intrinsics for `x86_64`.
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
#![deny(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    clippy::cast_possible_wrap
)]

pub mod batch;
pub mod bitmap;
pub mod column;
pub mod dict;
pub mod dict_i64;
pub mod kernels;

pub use batch::{Batch, BatchError};
pub use bitmap::Bitmap;
pub use column::{Column, ColumnError};
pub use dict::{
    DictionaryColumn, DictionaryEncodingPolicy, StringEncoding, encode_strings_auto,
    filter_eq_dict_code, group_by_dict,
};
pub use kernels::{
    CmpOp, cmp_gt_i64, cmp_gt_i64_scalar, cmp_i32_scalar, cmp_i64_scalar, count_i64, eq_i32,
    max_i64, min_f64, min_i64, range_mask_i64, select_i32, sum_i64, sum_i64_with_mask,
};
pub use kernels::{
    add_f32, add_f32_scalar, add_f32_scalar_lit, add_f32_scalar_lit_scalar, add_f64,
    add_f64_scalar, add_f64_scalar_lit, add_f64_scalar_lit_scalar, add_i32, add_i32_scalar,
    add_i32_scalar_lit, add_i32_scalar_lit_scalar, add_i64, add_i64_scalar, add_i64_scalar_lit,
    add_i64_scalar_lit_scalar, compare_f32, compare_f32_scalar, compare_f32_scalar_lit,
    compare_f32_scalar_lit_scalar, compare_f64, compare_f64_scalar, compare_f64_scalar_lit,
    compare_f64_scalar_lit_scalar, compare_i32, compare_i32_scalar, compare_i32_scalar_lit,
    compare_i32_scalar_lit_scalar, compare_i64, compare_i64_scalar, compare_i64_scalar_lit,
    compare_i64_scalar_lit_scalar, mul_f32, mul_f32_scalar, mul_f32_scalar_lit,
    mul_f32_scalar_lit_scalar, mul_f64, mul_f64_scalar, mul_f64_scalar_lit,
    mul_f64_scalar_lit_scalar, mul_i32, mul_i32_scalar, mul_i32_scalar_lit,
    mul_i32_scalar_lit_scalar, mul_i64, mul_i64_scalar, mul_i64_scalar_lit,
    mul_i64_scalar_lit_scalar, neg_f32, neg_f32_scalar, neg_f64, neg_f64_scalar, neg_i32,
    neg_i32_scalar, neg_i64, neg_i64_scalar, not_bool, not_bool_scalar, sub_f32, sub_f32_scalar,
    sub_f32_scalar_lit, sub_f32_scalar_lit_scalar, sub_f64, sub_f64_scalar, sub_f64_scalar_lit,
    sub_f64_scalar_lit_scalar, sub_i32, sub_i32_scalar, sub_i32_scalar_lit,
    sub_i32_scalar_lit_scalar, sub_i64, sub_i64_scalar, sub_i64_scalar_lit,
    sub_i64_scalar_lit_scalar,
};
pub use kernels::{
    filter_eq_f64, filter_eq_f64_scalar, filter_eq_i32, filter_eq_i32_scalar, filter_eq_i64,
    filter_eq_i64_scalar, filter_gt_i32, filter_gt_i32_scalar, filter_lt_i32, filter_lt_i32_scalar,
};
pub use kernels::{
    filter_sum_i64_where_gt_zero, filter_sum_i64_where_gt_zero_scalar,
    filter_sum_i64_where_gt_zero_with_validity, filter_sum_par_auto_i64_where_gt_zero,
    filter_sum_par_i64_where_gt_zero,
};
pub use kernels::{hash_i64, hash_i64_scalar, hash_text_bytes, hash_text_bytes_scalar};
pub use kernels::{
    len_text, len_text_scalar, lower_text, lower_text_scalar, upper_text, upper_text_scalar,
};

pub use dict_i64::{
    DictI64U8, DictI64U16, PredicateMask16, PredicateMask256, PredicateMask65536,
    filter_sum_i64_where_dict_predicate, filter_sum_i64_where_dict_predicate_scalar,
    filter_sum_i64_where_dict_predicate_tbl, filter_sum_i64_where_dict_predicate_u16,
    filter_sum_par_auto_i64_where_dict_predicate, filter_sum_par_i64_where_dict_predicate,
};
