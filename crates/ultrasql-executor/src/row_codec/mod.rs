//! Row-level binary codec used by the storage path of the executor.
//!
//! Encodes a `Vec<Value>` matching a `Schema` to a tightly-packed byte
//! buffer suitable for use as the `payload` of a heap tuple. The codec
//! is the inverse of `decode` and is bound to the workspace on-disk
//! format version.
//!
//! Streaming decode (v0.6)
//! -----------------------
//!
//! [`RowCodec::decode_into_builders`] decodes a tuple's bytes
//! directly into a parallel slice of [`ColumnBuilder`]s, skipping the
//! `Vec<Value>` row intermediate.

mod column_builder;
mod decode;
mod decode_builders;
mod decode_value;
mod encode;
mod error;
mod fast_path;
mod numeric;
mod varlena;

#[cfg(test)]
mod tests;

pub use error::RowCodecError;

pub(crate) use column_builder::{ColumnBuilder, finish_builders};
pub(crate) use numeric::*;
pub(crate) use varlena::*;

use ultrasql_core::Schema;

// Stable VECTOR payload layout: u32 little-endian dimension count,
// followed by that many f32 little-endian elements.
pub(crate) const VECTOR_DIMS_WIDTH: usize = std::mem::size_of::<u32>();
pub(crate) const VECTOR_ELEMENT_WIDTH: usize = std::mem::size_of::<f32>();
pub(crate) const NUMERIC_NBASE: u16 = 10_000;
pub(crate) const NUMERIC_DEC_DIGITS: i32 = 4;
pub(crate) const NUMERIC_DEC_DIGITS_USIZE: usize = 4;
pub(crate) const NUMERIC_DSCALE_MAX: i32 = 0x3fff;
pub(crate) const NUMERIC_POS: u16 = 0x0000;
pub(crate) const NUMERIC_NEG: u16 = 0x4000;
pub(crate) const NUMERIC_BINARY_HEADER_WIDTH: usize = 8;
pub(crate) const NUMERIC_DIGIT_WIDTH: usize = std::mem::size_of::<u16>();

pub(crate) fn u32_payload_len_to_usize(len: u32) -> Result<usize, RowCodecError> {
    usize::try_from(len).map_err(|_| RowCodecError::LengthOverflow { len })
}

pub(crate) fn checked_payload_end(
    cursor: usize,
    len: usize,
    have: usize,
) -> Result<usize, RowCodecError> {
    cursor.checked_add(len).ok_or(RowCodecError::Truncated {
        needed: usize::MAX,
        have,
    })
}

pub(crate) fn checked_fixed_end(
    cursor: usize,
    width: usize,
    have: usize,
) -> Result<usize, RowCodecError> {
    cursor.checked_add(width).ok_or(RowCodecError::Truncated {
        needed: usize::MAX,
        have,
    })
}

/// Binary codec bound to a fixed [`Schema`].
///
/// Caches a `fixed_width_lower_bound` and a `decode_shape` tag
/// precomputed at construction. The shape tag dispatches
/// `Self::decode_into_builders` to a specialised tight inline
/// loop for common fixed-width schemas (the scans on the
/// `cross_compare_sql` analytic and OLTP shapes) — bypassing the
/// generic column-loop match-dispatch.
#[derive(Clone, Debug)]
pub struct RowCodec {
    schema: Schema,
    /// Cached `Vec::with_capacity` hint for `encode`. Computed once.
    fixed_width_lower_bound: usize,
    /// Fast-path discriminant for [`Self::decode_into_builders`].
    decode_shape: DecodeShape,
}

/// Fast-path discriminant for [`RowCodec::decode_into_builders`].
///
/// At codec construction we detect the most common all-fixed-width
/// schemas and stash an enum tag here. At decode time we dispatch on
/// the tag and run a tight inline loop that skips:
///
/// - the per-column `(DataType, &mut ColumnBuilder)` match-arm
///   dispatch and its embedded bounds checks;
/// - the per-column `try_into::<[u8; N]>::?` re-validation
///   (the bytes-len check is folded into a single payload-length
///   check at the head of the fast path);
/// - the null-bitmap byte parse + per-column bit extract when the
///   byte is 0 (i.e. every column is non-null).
///
/// `Generic` is the universal fallback used for any schema not
/// covered by a specialised shape, including the mixed-NULL slow
/// path of the specialised shapes themselves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DecodeShape {
    /// Universal fallback. Always correct; never faster than the
    /// specialised paths but handles every supported schema.
    Generic,
    /// `[Int32]`.
    I32x1,
    /// `[Int32, Int32]` (the most common analytic preload — bench
    /// tables `(id INT, x INT)` / `(id INT, val INT)`).
    I32x2,
    /// `[Int32, Int32, Int32]` (the TID-prefixed shape `SeqScan` emits
    /// for UPDATE / DELETE over an `(id, val)` heap).
    I32x3,
    /// `[Int64]`.
    I64x1,
    /// `[Int64, Int64]`.
    I64x2,
}
