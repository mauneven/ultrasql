//! Zero-allocation `DataRow` wire-encoder.
//!
//! The legacy result path materialised every row as
//! `BackendMessage::DataRow { columns: Vec<Option<Vec<u8>>> }`, then ran it
//! through `encode_backend`. For a 10 000-row `SELECT id, val FROM t` that
//! is ~20 000 per-cell `Vec<u8>` allocations plus 10 000 enum heap moves —
//! the dominant cost of the wire path on the `select_scan_10k` bench
//! workload (Wave C profile: 8.57 ms median against a 449 µs target).
//!
//! This module exposes a hand-rolled `DataRow` writer that emits the wire
//! bytes for one row directly into a `bytes::BytesMut` sink, with:
//!
//! - **No per-cell allocation.** Integer cells are formatted with the
//!   inline [`itoa`-style](https://en.wikipedia.org/wiki/Itoa) routines in
//!   [`write_int32_text`] / [`write_int64_text`]; the bytes land straight
//!   in the sink. Boolean cells write a single byte. Float and text cells
//!   fall back to the legacy `encode_text_value` allocator — those types
//!   are out of scope for the `select_scan_10k` workload but we keep
//!   semantics bit-identical with the legacy path so the rest of the
//!   bench matrix does not regress.
//!
//! - **No `BackendMessage` materialisation.** Callers drive
//!   [`write_data_row`] in a tight loop over the operator's batches and
//!   then emit a trailing `CommandComplete` via the regular
//!   `encode_backend` path. The session loop drains the sink with a
//!   single `write_all` + `flush` rather than one per row.
//!
//! The wire-format invariants enforced here mirror
//! `ultrasql_protocol::codec::encode_backend`'s `DataRow` arm exactly; the
//! unit tests below assert byte-for-byte equality against the canonical
//! encoder.

use bytes::{BufMut, BytesMut};
use ultrasql_vec::Bitmap;
use ultrasql_vec::column::Column;

use crate::result_encoder::encode_text_value;

/// PostgreSQL `DataRow` message type tag (`'D'`).
const DATA_ROW_TAG: u8 = b'D';

/// Saturating `i32`-from-`usize` for length fields. The wire-length field
/// is signed 32-bit; messages larger than `i32::MAX` cannot be expressed.
/// We never construct one in practice.
fn i32_from_usize(value: usize) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

/// Same logic for `i16`-sized column counts.
fn i16_from_usize(value: usize) -> i16 {
    i16::try_from(value).unwrap_or(i16::MAX)
}

/// Emit one `DataRow` for `row` of `batch_columns` into `sink`.
///
/// Layout (per PostgreSQL wire spec):
///
/// ```text
///     1 byte    type tag        'D'
///     4 bytes   length            (includes the 4 length bytes themselves)
///     2 bytes   ncols
///     for each column:
///       4 bytes value length      (-1 for SQL NULL)
///       N bytes value bytes       (text format)
/// ```
///
/// The length placeholder is back-filled after the payload is fully
/// written so the writer does not need to know value widths up front.
pub(crate) fn write_data_row(sink: &mut BytesMut, batch_columns: &[Column], row: usize) {
    sink.put_u8(DATA_ROW_TAG);
    let length_index = sink.len();
    sink.put_i32(0); // length placeholder
    let payload_start = sink.len();
    sink.put_i16(i16_from_usize(batch_columns.len()));
    for col in batch_columns {
        write_cell(sink, col, row);
    }
    let payload_end = sink.len();
    let payload_len = payload_end - payload_start;
    let length = i32_from_usize(payload_len + 4);
    sink[length_index..length_index + 4].copy_from_slice(&length.to_be_bytes());
}

/// Fast bulk DataRow writer for the common `(Int32, Int32)` shape.
///
/// Streams every row of a two-column non-nullable `Int32` batch into
/// `sink` with no per-row enum dispatch and no per-cell length-
/// placeholder back-fill. Both columns' bytes share a single
/// `BytesMut::reserve` call sized to the worst-case wire footprint
/// for this batch. For a 10 000-row scan this drops ~10 µs of
/// `bytes::BytesMut::reserve` re-resize work plus the per-row enum
/// match in `write_cell`.
///
/// Caller verifies the shape upfront (`fast_int32_pair_data_rows`).
pub(crate) fn write_int32_pair_data_rows(
    sink: &mut BytesMut,
    a: &[i32],
    b: &[i32],
) {
    debug_assert_eq!(a.len(), b.len());
    let n = a.len();
    if n == 0 {
        return;
    }
    // Worst-case wire size per row: 1 + 4 + 2 + (4 + 11) + (4 + 11) = 37
    // bytes ("-2147483648" is the widest i32 text). Reserve the
    // worst case once to skip every mid-loop resize.
    sink.reserve(n * 37);
    // Cast SAFETY: we just reserved `n * 37` contiguous capacity, so
    // the spare region has at least that many writable bytes. We
    // write into the raw slice and advance the BytesMut length once
    // at the end; this collapses every per-row `put_*` call into a
    // straight memcpy + index store.
    let base_len = sink.len();
    let cap = sink.capacity();
    let writable = cap - base_len;
    let raw_ptr: *mut u8 = unsafe { sink.as_mut_ptr().add(base_len) };
    let raw: &mut [u8] = unsafe { std::slice::from_raw_parts_mut(raw_ptr, writable) };

    let mut off: usize = 0;
    let mut scratch_a = [0u8; 12];
    let mut scratch_b = [0u8; 12];
    for row in 0..n {
        // DataRow tag.
        raw[off] = DATA_ROW_TAG;
        off += 1;
        let length_index = off;
        off += 4; // length placeholder
        let payload_start = off;
        // ncols = 2 (big-endian i16).
        raw[off] = 0;
        raw[off + 1] = 2;
        off += 2;

        let a_text = format_i32_into(&mut scratch_a, a[row]);
        let a_len = a_text.len();
        raw[off..off + 4].copy_from_slice(&(i32_from_usize(a_len)).to_be_bytes());
        off += 4;
        raw[off..off + a_len].copy_from_slice(a_text);
        off += a_len;

        let b_text = format_i32_into(&mut scratch_b, b[row]);
        let b_len = b_text.len();
        raw[off..off + 4].copy_from_slice(&(i32_from_usize(b_len)).to_be_bytes());
        off += 4;
        raw[off..off + b_len].copy_from_slice(b_text);
        off += b_len;

        let payload_len = off - payload_start;
        let length = i32_from_usize(payload_len + 4);
        raw[length_index..length_index + 4].copy_from_slice(&length.to_be_bytes());
    }
    // SAFETY: `off` ≤ `writable` because the worst-case-37-bytes
    // reserve covers every row's maximum width; `raw` is the spare
    // region of `sink` we reserved above.
    unsafe {
        sink.set_len(base_len + off);
    }
}

/// Emit one column cell. NULL is encoded as length `-1` with no value
/// bytes; everything else gets a length-prefixed text-format payload.
fn write_cell(sink: &mut BytesMut, col: &Column, row: usize) {
    if is_null(col, row) {
        sink.put_i32(-1);
        return;
    }
    match col {
        Column::Int32(c) => write_length_prefixed_int32(sink, c.data()[row]),
        Column::Int64(c) => write_length_prefixed_int64(sink, c.data()[row]),
        Column::Bool(c) => {
            sink.put_i32(1);
            sink.put_u8(if c.value(row) { b't' } else { b'f' });
        }
        // Floats and text fall back to the legacy allocator. Floats are
        // dominated by `format!` formatting in any case (no integer fast
        // path), and the `Utf8` case copies an existing `&str` into a
        // fresh `Vec<u8>` — the allocator can be removed in a follow-up
        // but is not on the `select_scan_10k` critical path.
        Column::Float32(_) | Column::Float64(_) | Column::Utf8(_) => {
            // Safe to expect-unwrap: the null branch above already
            // handled the `None` case.
            let bytes =
                encode_text_value(col, row).expect("non-null cell must encode to Some(bytes)");
            sink.put_i32(i32_from_usize(bytes.len()));
            sink.put_slice(&bytes);
        }
    }
}

/// Whether the given row is SQL NULL per the column's optional null
/// bitmap. Mirrors `result_encoder::column_nulls`.
fn is_null(col: &Column, row: usize) -> bool {
    column_nulls(col).is_some_and(|nulls| !nulls.get(row))
}

const fn column_nulls(col: &Column) -> Option<&Bitmap> {
    match col {
        Column::Int32(c) => c.nulls(),
        Column::Int64(c) => c.nulls(),
        Column::Float32(c) => c.nulls(),
        Column::Float64(c) => c.nulls(),
        Column::Bool(c) => c.nulls(),
        Column::Utf8(c) => c.nulls(),
    }
}

/// Write a length-prefixed text-format `int4`.
fn write_length_prefixed_int32(sink: &mut BytesMut, value: i32) {
    let length_index = sink.len();
    sink.put_i32(0); // placeholder
    let payload_start = sink.len();
    write_int32_text(sink, value);
    let len = sink.len() - payload_start;
    sink[length_index..length_index + 4].copy_from_slice(&i32_from_usize(len).to_be_bytes());
}

/// Write a length-prefixed text-format `int8`.
fn write_length_prefixed_int64(sink: &mut BytesMut, value: i64) {
    let length_index = sink.len();
    sink.put_i32(0); // placeholder
    let payload_start = sink.len();
    write_int64_text(sink, value);
    let len = sink.len() - payload_start;
    sink[length_index..length_index + 4].copy_from_slice(&i32_from_usize(len).to_be_bytes());
}

/// Write the decimal text representation of `value` directly into
/// `sink`.
///
/// Equivalent to `sink.put_slice(value.to_string().as_bytes())` but with
/// zero heap allocation: we format into a 12-byte stack scratch then
/// copy to the sink. The 12-byte upper bound covers `-2147483648` plus
/// its sign.
pub(crate) fn write_int32_text(sink: &mut BytesMut, value: i32) {
    let mut scratch = [0u8; 12];
    let written = format_i32_into(&mut scratch, value);
    sink.put_slice(written);
}

/// Same for `i64`. 20-byte upper bound covers `-9223372036854775808`.
pub(crate) fn write_int64_text(sink: &mut BytesMut, value: i64) {
    let mut scratch = [0u8; 20];
    let written = format_i64_into(&mut scratch, value);
    sink.put_slice(written);
}

/// Format an `i32` into the trailing bytes of `scratch` and return the
/// filled sub-slice (left-to-right reading order).
///
/// Standard textbook integer-to-decimal: write digits from least to most
/// significant into the back of the buffer, then optionally prepend the
/// `'-'` sign. The returned slice points into `scratch` and lives as
/// long as the caller's borrow.
fn format_i32_into(scratch: &mut [u8; 12], value: i32) -> &[u8] {
    if value == 0 {
        scratch[0] = b'0';
        return &scratch[..1];
    }
    // Work with the absolute value as `u32` to handle `i32::MIN` without
    // overflow. `i32::MIN.unsigned_abs()` is the standard idiom.
    let negative = value < 0;
    let mut n = u64::from(value.unsigned_abs());
    let mut idx = scratch.len();
    while n > 0 {
        idx -= 1;
        // `n % 10` is in 0..=9; `b'0' + d` fits in `u8` without overflow.
        // `u8::try_from` is the explicit, no-`as`-cast idiom for the
        // narrowing the rule (`AGENTS.md §3.3`) requires.
        let digit = u8::try_from(n % 10).expect("n % 10 < 10");
        scratch[idx] = b'0' + digit;
        n /= 10;
    }
    if negative {
        idx -= 1;
        scratch[idx] = b'-';
    }
    &scratch[idx..]
}

/// Same routine for `i64`.
fn format_i64_into(scratch: &mut [u8; 20], value: i64) -> &[u8] {
    if value == 0 {
        scratch[0] = b'0';
        return &scratch[..1];
    }
    let negative = value < 0;
    let mut n = value.unsigned_abs();
    let mut idx = scratch.len();
    while n > 0 {
        idx -= 1;
        let digit = u8::try_from(n % 10).expect("n % 10 < 10");
        scratch[idx] = b'0' + digit;
        n /= 10;
    }
    if negative {
        idx -= 1;
        scratch[idx] = b'-';
    }
    &scratch[idx..]
}

#[cfg(test)]
mod tests {
    use super::*;
    use ultrasql_protocol::{BackendMessage, encode_backend};
    use ultrasql_vec::column::{Column, NumericColumn};

    /// The hand-rolled writer must produce bit-identical bytes to the
    /// canonical `encode_backend(BackendMessage::DataRow { .. })` for
    /// every supported column type.
    #[test]
    fn write_data_row_matches_canonical_encoder_int32() {
        let cols = vec![Column::Int32(NumericColumn::from_data(vec![42, -1, 0]))];

        // Canonical bytes for row 0:
        let canonical_msg = BackendMessage::DataRow {
            columns: vec![Some(b"42".to_vec())],
        };
        let mut canonical = BytesMut::new();
        encode_backend(&canonical_msg, &mut canonical);

        let mut actual = BytesMut::new();
        write_data_row(&mut actual, &cols, 0);

        assert_eq!(&actual[..], &canonical[..]);
    }

    #[test]
    fn write_data_row_handles_negative_and_min() {
        // i32::MIN exercises `unsigned_abs` and the 12-byte scratch bound.
        let cols = vec![Column::Int32(NumericColumn::from_data(vec![i32::MIN]))];
        let canonical_msg = BackendMessage::DataRow {
            columns: vec![Some(b"-2147483648".to_vec())],
        };
        let mut canonical = BytesMut::new();
        encode_backend(&canonical_msg, &mut canonical);
        let mut actual = BytesMut::new();
        write_data_row(&mut actual, &cols, 0);
        assert_eq!(&actual[..], &canonical[..]);
    }

    #[test]
    fn write_data_row_int64_min_value() {
        // i64::MIN exercises the 20-byte scratch bound.
        let cols = vec![Column::Int64(NumericColumn::from_data(vec![i64::MIN]))];
        let canonical_msg = BackendMessage::DataRow {
            columns: vec![Some(b"-9223372036854775808".to_vec())],
        };
        let mut canonical = BytesMut::new();
        encode_backend(&canonical_msg, &mut canonical);
        let mut actual = BytesMut::new();
        write_data_row(&mut actual, &cols, 0);
        assert_eq!(&actual[..], &canonical[..]);
    }

    #[test]
    fn write_data_row_handles_nulls() {
        // A null-bearing column: 1=valid, 0=null per Arrow convention.
        let mut nulls = Bitmap::new(2, false);
        nulls.set(0, true);
        nulls.set(1, false);
        let col =
            Column::Int32(NumericColumn::with_nulls(vec![17, 0], nulls).expect("matching lengths"));
        let cols = vec![col];

        // Row 0 = Some(17).
        let canonical_msg_0 = BackendMessage::DataRow {
            columns: vec![Some(b"17".to_vec())],
        };
        let mut canonical_0 = BytesMut::new();
        encode_backend(&canonical_msg_0, &mut canonical_0);
        let mut actual_0 = BytesMut::new();
        write_data_row(&mut actual_0, &cols, 0);
        assert_eq!(&actual_0[..], &canonical_0[..]);

        // Row 1 = None.
        let canonical_msg_1 = BackendMessage::DataRow {
            columns: vec![None],
        };
        let mut canonical_1 = BytesMut::new();
        encode_backend(&canonical_msg_1, &mut canonical_1);
        let mut actual_1 = BytesMut::new();
        write_data_row(&mut actual_1, &cols, 1);
        assert_eq!(&actual_1[..], &canonical_1[..]);
    }

    #[test]
    fn write_data_row_multi_column_select_scan_shape() {
        // The shape the bench exercises: two i32 columns per row.
        let cols = vec![
            Column::Int32(NumericColumn::from_data(vec![0, 1, 2])),
            Column::Int32(NumericColumn::from_data(vec![0, 10, 20])),
        ];

        for row in 0..3 {
            let id_text = (row as i32).to_string().into_bytes();
            let val_text = ((row as i32) * 10).to_string().into_bytes();
            let canonical_msg = BackendMessage::DataRow {
                columns: vec![Some(id_text), Some(val_text)],
            };
            let mut canonical = BytesMut::new();
            encode_backend(&canonical_msg, &mut canonical);
            let mut actual = BytesMut::new();
            write_data_row(&mut actual, &cols, row);
            assert_eq!(&actual[..], &canonical[..], "row {row} mismatch");
        }
    }

    #[test]
    fn format_i32_into_covers_zero_negative_positive_and_min() {
        let mut scratch = [0u8; 12];
        assert_eq!(format_i32_into(&mut scratch, 0), b"0");
        let mut scratch = [0u8; 12];
        assert_eq!(format_i32_into(&mut scratch, 1), b"1");
        let mut scratch = [0u8; 12];
        assert_eq!(format_i32_into(&mut scratch, -1), b"-1");
        let mut scratch = [0u8; 12];
        assert_eq!(format_i32_into(&mut scratch, 12345), b"12345");
        let mut scratch = [0u8; 12];
        assert_eq!(format_i32_into(&mut scratch, -98765), b"-98765");
        let mut scratch = [0u8; 12];
        assert_eq!(format_i32_into(&mut scratch, i32::MAX), b"2147483647");
        let mut scratch = [0u8; 12];
        assert_eq!(format_i32_into(&mut scratch, i32::MIN), b"-2147483648");
    }

    #[test]
    fn format_i64_into_covers_zero_negative_positive_and_min() {
        let mut scratch = [0u8; 20];
        assert_eq!(format_i64_into(&mut scratch, 0), b"0");
        let mut scratch = [0u8; 20];
        assert_eq!(format_i64_into(&mut scratch, -1), b"-1");
        let mut scratch = [0u8; 20];
        assert_eq!(format_i64_into(&mut scratch, 9_876_543_210), b"9876543210");
        let mut scratch = [0u8; 20];
        assert_eq!(
            format_i64_into(&mut scratch, i64::MAX),
            b"9223372036854775807"
        );
        let mut scratch = [0u8; 20];
        assert_eq!(
            format_i64_into(&mut scratch, i64::MIN),
            b"-9223372036854775808"
        );
    }

    /// `write_int32_text` and `write_int64_text` write zero allocations.
    /// We can't directly observe heap allocations from a test, but we can
    /// check that the output matches `to_string()` (which is the
    /// allocation-bearing baseline we're replacing).
    #[test]
    fn write_int32_text_matches_to_string() {
        for &v in &[0_i32, 1, -1, 12345, -98765, i32::MAX, i32::MIN] {
            let mut sink = BytesMut::new();
            write_int32_text(&mut sink, v);
            assert_eq!(&sink[..], v.to_string().as_bytes(), "value {v} mismatch");
        }
    }
}
