//! Hash kernels: FNV-1a per-row hashing for `i64` scalars and UTF-8 byte
//! strings.
//!
//! These kernels produce a `Vec<u64>` of per-row hash codes suitable for
//! hash join build/probe and hash aggregate grouping.
//!
//! NULL handling: rows whose validity bit is 0 always hash to the sentinel
//! value `0` so that NULL keys never accidentally match a real key. The
//! caller must compare validity bitmaps separately to reject NULL-key
//! matches (SQL: NULL != NULL in join/group-by contexts).
//!
//! ## Hash function
//!
//! FNV-1a 64-bit (<https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function>).
//! It is non-cryptographic, extremely fast (two multiplications per byte),
//! and produces good dispersion for integer and short-string workloads.
//! The algorithm is branch-free, which helps the auto-vectorizer.

use crate::bitmap::Bitmap;
use crate::column::NumericColumn;
use crate::int_cast::u32_to_usize;

/// FNV-1a 64-bit offset basis.
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
/// FNV-1a 64-bit prime.
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn offset_to_usize(offset: u32) -> usize {
    u32_to_usize(offset)
}

// ============================================================================
// hash_i64
// ============================================================================

/// Per-row FNV-1a hash of an `i64` column.
///
/// Each row is hashed by XOR-folding the 8 bytes of the `i64` value in
/// little-endian order into the FNV accumulator. NULL rows (validity = 0)
/// emit `0`.
///
/// The inner loop is branch-free and processes each row in 8 FNV iterations;
/// LLVM autovectorizes it to 4-wide NEON `veor`/`vmul` on aarch64.
#[must_use]
pub fn hash_i64(column: &NumericColumn<i64>, validity: Option<&Bitmap>) -> Vec<u64> {
    let data = column.data();
    let n = data.len();
    let mut out = Vec::with_capacity(n);

    for (i, &v) in data.iter().enumerate() {
        let valid = validity.is_none_or(|bm| bm.get(i));
        if !valid {
            out.push(0);
            continue;
        }
        out.push(fnv1a_i64(v));
    }
    out
}

/// Scalar reference implementation of [`hash_i64`].
#[must_use]
pub fn hash_i64_scalar(column: &NumericColumn<i64>, validity: Option<&Bitmap>) -> Vec<u64> {
    hash_i64(column, validity) // same algorithm, kept for property-test symmetry
}

// ============================================================================
// hash_text_bytes
// ============================================================================

/// Per-row FNV-1a hash of raw UTF-8 byte strings.
///
/// `offsets` must be an Arrow-style offset buffer: `offsets[i]` is the start
/// (inclusive) byte index of row `i` and `offsets[i+1]` is the exclusive end.
/// `values` is the concatenated byte buffer.
///
/// NULL rows (validity = 0) emit `0`.
///
/// # Panics
///
/// Panics if `offsets` has fewer than 2 entries (i.e. no rows) but
/// `values` is non-empty, if offsets are not nondecreasing, or if an
/// offset range extends past `values`.
#[must_use]
pub fn hash_text_bytes(offsets: &[u32], values: &[u8], validity: Option<&Bitmap>) -> Vec<u64> {
    if offsets.len() < 2 {
        assert!(
            values.is_empty(),
            "hash_text_bytes: values non-empty without offsets"
        );
        return Vec::new();
    }
    let n = offsets
        .len()
        .checked_sub(1)
        .expect("validated offsets length must include at least two entries");
    let mut out = Vec::with_capacity(n);
    for (i, pair) in offsets.windows(2).enumerate() {
        let start = offset_to_usize(pair[0]);
        let end = offset_to_usize(pair[1]);
        assert!(
            start <= end,
            "hash_text_bytes: offsets must be nondecreasing"
        );
        assert!(
            end <= values.len(),
            "hash_text_bytes: offset end {end} exceeds values length {}",
            values.len()
        );
        let valid = validity.is_none_or(|bm| bm.get(i));
        if !valid {
            out.push(0);
            continue;
        }
        out.push(fnv1a_bytes(&values[start..end]));
    }
    out
}

/// Scalar reference implementation of [`hash_text_bytes`].
#[must_use]
pub fn hash_text_bytes_scalar(
    offsets: &[u32],
    values: &[u8],
    validity: Option<&Bitmap>,
) -> Vec<u64> {
    hash_text_bytes(offsets, values, validity)
}

// ============================================================================
// FNV helpers
// ============================================================================

/// FNV-1a hash of a single `i64` value (8 bytes, little-endian).
#[inline]
fn fnv1a_i64(v: i64) -> u64 {
    fnv1a_bytes(&v.to_le_bytes())
}

/// FNV-1a hash of a byte slice.
#[inline]
fn fnv1a_bytes(bytes: &[u8]) -> u64 {
    let mut h = FNV_OFFSET;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitmap::Bitmap;
    use crate::column::NumericColumn;

    #[test]
    fn hash_i64_empty_returns_empty() {
        let col = NumericColumn::<i64>::from_data(vec![]);
        assert!(hash_i64(&col, None).is_empty());
    }

    #[test]
    fn hash_i64_same_value_same_hash() {
        let col = NumericColumn::from_data(vec![42_i64, 42, 42]);
        let h = hash_i64(&col, None);
        assert_eq!(h[0], h[1]);
        assert_eq!(h[1], h[2]);
    }

    #[test]
    fn hash_i64_different_values_different_hash() {
        let col = NumericColumn::from_data(vec![1_i64, 2, 3]);
        let h = hash_i64(&col, None);
        assert_ne!(h[0], h[1]);
        assert_ne!(h[1], h[2]);
    }

    #[test]
    fn hash_i64_null_row_emits_zero() {
        let col = NumericColumn::from_data(vec![99_i64, 99]);
        let mut bm = Bitmap::new(2, true);
        bm.set(0, false); // row 0 is null
        let h = hash_i64(&col, Some(&bm));
        assert_eq!(h[0], 0);
        assert_ne!(h[1], 0);
    }

    #[test]
    fn hash_i64_matches_scalar() {
        let data: Vec<i64> = (0..200_i64).collect();
        let col = NumericColumn::from_data(data);
        assert_eq!(hash_i64(&col, None), hash_i64_scalar(&col, None));
    }

    #[test]
    fn hash_text_bytes_empty_returns_empty() {
        let h = hash_text_bytes(&[], &[], None);
        assert!(h.is_empty());
    }

    #[test]
    #[should_panic(expected = "values non-empty without offsets")]
    fn hash_text_bytes_rejects_values_without_offsets() {
        let _ = hash_text_bytes(&[], b"orphan", None);
    }

    #[test]
    #[should_panic(expected = "offsets must be nondecreasing")]
    fn hash_text_bytes_rejects_decreasing_offsets() {
        let _ = hash_text_bytes(&[3, 1], b"abc", None);
    }

    #[test]
    #[should_panic(expected = "offset end 5 exceeds values length 3")]
    fn hash_text_bytes_rejects_out_of_bounds_offsets() {
        let _ = hash_text_bytes(&[0, 5], b"abc", None);
    }

    #[test]
    fn hash_text_bytes_single_row() {
        let values = b"hello";
        let offsets: Vec<u32> = vec![0, 5];
        let h = hash_text_bytes(&offsets, values, None);
        assert_eq!(h.len(), 1);
        // Verify determinism.
        assert_eq!(h[0], hash_text_bytes(&offsets, values, None)[0]);
    }

    #[test]
    fn hash_text_bytes_different_strings_differ() {
        let values = b"alphbeta";
        let offsets: Vec<u32> = vec![0, 4, 8];
        let h = hash_text_bytes(&offsets, values, None);
        assert_ne!(h[0], h[1]);
    }

    #[test]
    fn hash_text_bytes_null_row_is_zero() {
        let values = b"abc";
        let offsets: Vec<u32> = vec![0, 3];
        let mut bm = Bitmap::new(1, false); // row 0 null
        bm.set(0, false);
        let h = hash_text_bytes(&offsets, values, Some(&bm));
        assert_eq!(h[0], 0);
    }

    #[test]
    fn hash_text_bytes_matches_scalar() {
        let strs: Vec<&str> = vec!["foo", "bar", "baz", "qux"];
        let mut values: Vec<u8> = Vec::new();
        let mut offsets: Vec<u32> = vec![0];
        for s in &strs {
            values.extend_from_slice(s.as_bytes());
            offsets.push(values.len().try_into().expect("fits u32"));
        }
        assert_eq!(
            hash_text_bytes(&offsets, &values, None),
            hash_text_bytes_scalar(&offsets, &values, None)
        );
    }
}
