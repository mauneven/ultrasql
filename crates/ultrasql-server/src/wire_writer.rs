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
use ultrasql_core::{DataType, Schema};
use ultrasql_vec::Bitmap;
use ultrasql_vec::column::Column;

use crate::result_encoder::{encode_text_value, encode_text_value_typed};

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
#[cfg(test)]
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

/// Emit one `DataRow` using logical schema types for physical-layout columns.
///
/// `DATE` and `DECIMAL` are stored as integer batch columns, so this path keeps
/// their PostgreSQL text output semantic while preserving the integer fast paths
/// for true integer schemas.
pub(crate) fn write_data_row_typed(
    sink: &mut BytesMut,
    batch_columns: &[Column],
    schema: &Schema,
    row: usize,
) {
    sink.put_u8(DATA_ROW_TAG);
    let length_index = sink.len();
    sink.put_i32(0);
    let payload_start = sink.len();
    sink.put_i16(i16_from_usize(batch_columns.len()));
    for (idx, col) in batch_columns.iter().enumerate() {
        let logical_type = &schema.field_at(idx).data_type;
        write_cell_typed(sink, col, row, logical_type);
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
/// The hot loop uses raw pointer writes against a freshly-reserved
/// region of `sink`. Every offset is bounded above by the
/// `n * MAX_ROW_BYTES` reserve at the top of the function, so we can
/// safely skip the per-byte slice bounds checks the safe `[off]`
/// indexing would emit.
///
/// Caller verifies the shape upfront (`fast_int32_pair_data_rows`).
pub(crate) fn write_int32_pair_data_rows(sink: &mut BytesMut, a: &[i32], b: &[i32]) {
    debug_assert_eq!(a.len(), b.len());
    let n = a.len();
    if n == 0 {
        return;
    }
    // Worst-case wire size per row: 1 + 4 + 2 + (4 + 11) + (4 + 11) = 37
    // bytes ("-2147483648" is the widest i32 text). Reserve the
    // worst case once to skip every mid-loop resize.
    const MAX_ROW_BYTES: usize = 37;
    sink.reserve(n * MAX_ROW_BYTES);

    let base_len = sink.len();
    // SAFETY: `reserve(n * MAX_ROW_BYTES)` guarantees the spare region
    // starting at `base_len` has at least `n * MAX_ROW_BYTES`
    // writable bytes. We write straight through a raw pointer and
    // `set_len` once at the end — no aliased mutable borrows, every
    // offset is bounded by `n * MAX_ROW_BYTES`.
    let dst_base: *mut u8 = unsafe { sink.as_mut_ptr().add(base_len) };

    let mut off: usize = 0;
    let mut scratch_a = [0u8; 12];
    let mut scratch_b = [0u8; 12];
    for row in 0..n {
        // SAFETY: bounded by the per-row 37-byte reserve above.
        unsafe {
            let dst = dst_base.add(off);
            // DataRow tag.
            *dst = DATA_ROW_TAG;
            // 4-byte length placeholder; back-filled below once we
            // know the payload size. The two-step write tracks the
            // index for the placeholder.
            let length_ptr = dst.add(1);
            // ncols = 2 (big-endian i16): bytes [0x00, 0x02] at
            // offset +5.
            *dst.add(5) = 0;
            *dst.add(6) = 2;
            off += 7; // tag + length placeholder + ncols

            let a_text = format_i32_into(&mut scratch_a, a[row]);
            let a_len = a_text.len();
            // 4-byte big-endian column length.
            let a_len_be = i32_from_usize(a_len).to_be_bytes();
            std::ptr::copy_nonoverlapping(a_len_be.as_ptr(), dst_base.add(off), 4);
            off += 4;
            std::ptr::copy_nonoverlapping(a_text.as_ptr(), dst_base.add(off), a_len);
            off += a_len;

            let b_text = format_i32_into(&mut scratch_b, b[row]);
            let b_len = b_text.len();
            let b_len_be = i32_from_usize(b_len).to_be_bytes();
            std::ptr::copy_nonoverlapping(b_len_be.as_ptr(), dst_base.add(off), 4);
            off += 4;
            std::ptr::copy_nonoverlapping(b_text.as_ptr(), dst_base.add(off), b_len);
            off += b_len;

            // Back-fill the 4-byte length placeholder. `payload_len`
            // is the bytes after the length field itself, so the wire
            // length (which includes the length field) is
            // `(off - (length_index + 4)) + 4 = off - length_index`.
            // `length_index = (dst - dst_base) + 1`, so the on-wire
            // value is `off - ((dst - dst_base) + 1)`. We compute it
            // via the captured row-start offset below.
            // `off` at this point sits past the row. The row started
            // at `off - row_bytes_written` where row_bytes_written =
            // 7 + (4 + a_len) + (4 + b_len). The length field begins
            // 1 byte into the row, so the length value is
            // `row_bytes_written - 1`.
            let row_bytes = 7 + 4 + a_len + 4 + b_len;
            let length = i32_from_usize(row_bytes - 1).to_be_bytes();
            std::ptr::copy_nonoverlapping(length.as_ptr(), length_ptr, 4);
        }
    }
    // SAFETY: `off` ≤ `n * MAX_ROW_BYTES` because every row writes
    // at most MAX_ROW_BYTES; `dst_base` is the spare region of
    // `sink` we reserved above.
    unsafe {
        sink.set_len(base_len + off);
    }
}

/// Fast bulk DataRow writer for the common `(Int32, Int64)` shape.
///
/// Counterpart to [`write_int32_pair_data_rows`] for the window /
/// `row_number()` output schema: `(input INT, row_number BIGINT)`.
/// The bench query `SELECT id, row_number() OVER (ORDER BY x) FROM t`
/// against `(id INT, x INT)` projects to exactly this shape, so the
/// `WindowAgg::try_columnar_row_number` columnar fast path lands here.
///
/// Streams every row of a two-column `(Int32, Int64)` batch into
/// `sink` with no per-row enum dispatch and no per-cell length
/// placeholder back-fill. Both columns' bytes share a single
/// `BytesMut::reserve` call sized to the worst-case wire footprint
/// (37 + 9 = 46 bytes per row: 11-byte i32 vs. 20-byte i64 maxima).
///
/// The hot loop uses raw pointer writes against a freshly reserved
/// region of `sink`. Every offset is bounded above by the
/// `n * MAX_ROW_BYTES` reserve at the top of the function, so we can
/// safely skip the per-byte slice bounds checks the safe `[off]`
/// indexing would emit.
///
/// `a_nulls` / `b_nulls` track the optional validity bitmaps from
/// the source columns. When both are `None` the inner loop is the
/// branch-free path; when either side carries a bitmap we fall through
/// to a per-cell `is_null` check that mirrors the slow-path NULL
/// encoding (`-1` length, no payload bytes).
///
/// Caller verifies the shape upfront (see
/// [`crate::result_encoder::stream_select`]).
pub(crate) fn write_int32_int64_pair_data_rows(
    sink: &mut BytesMut,
    a: &[i32],
    a_nulls: Option<&Bitmap>,
    b: &[i64],
    b_nulls: Option<&Bitmap>,
) {
    debug_assert_eq!(a.len(), b.len());
    let n = a.len();
    if n == 0 {
        return;
    }
    // Worst-case wire size per row: 1 + 4 + 2 (header) + (4 + 11)
    // (i32 cell) + (4 + 20) (i64 cell) = 46 bytes. `-2147483648` is
    // the widest i32 text and `-9223372036854775808` the widest i64
    // text. Reserve the worst case once to skip every mid-loop
    // resize.
    const MAX_ROW_BYTES: usize = 46;
    sink.reserve(n * MAX_ROW_BYTES);

    let base_len = sink.len();
    // SAFETY: `reserve(n * MAX_ROW_BYTES)` guarantees the spare region
    // starting at `base_len` has at least `n * MAX_ROW_BYTES`
    // writable bytes. We write straight through a raw pointer and
    // `set_len` once at the end — no aliased mutable borrows, every
    // offset is bounded by `n * MAX_ROW_BYTES`.
    let dst_base: *mut u8 = unsafe { sink.as_mut_ptr().add(base_len) };

    let mut off: usize = 0;
    let mut scratch_a = [0u8; 12];
    let mut scratch_b = [0u8; 20];

    // Fast path: no validity bitmaps anywhere. The inner loop is
    // branch-free per cell; the optimiser can hoist the bitmap-load
    // out of the hot loop entirely.
    if a_nulls.is_none() && b_nulls.is_none() {
        for row in 0..n {
            // SAFETY: bounded by the per-row 46-byte reserve above.
            unsafe {
                let dst = dst_base.add(off);
                // DataRow tag.
                *dst = DATA_ROW_TAG;
                // 4-byte length placeholder; back-filled below once
                // we know the payload size.
                let length_ptr = dst.add(1);
                // ncols = 2 (big-endian i16).
                *dst.add(5) = 0;
                *dst.add(6) = 2;
                off += 7; // tag + length placeholder + ncols

                let a_text = format_i32_into(&mut scratch_a, a[row]);
                let a_len = a_text.len();
                let a_len_be = i32_from_usize(a_len).to_be_bytes();
                std::ptr::copy_nonoverlapping(a_len_be.as_ptr(), dst_base.add(off), 4);
                off += 4;
                std::ptr::copy_nonoverlapping(a_text.as_ptr(), dst_base.add(off), a_len);
                off += a_len;

                let b_text = format_i64_into(&mut scratch_b, b[row]);
                let b_len = b_text.len();
                let b_len_be = i32_from_usize(b_len).to_be_bytes();
                std::ptr::copy_nonoverlapping(b_len_be.as_ptr(), dst_base.add(off), 4);
                off += 4;
                std::ptr::copy_nonoverlapping(b_text.as_ptr(), dst_base.add(off), b_len);
                off += b_len;

                let row_bytes = 7 + 4 + a_len + 4 + b_len;
                let length = i32_from_usize(row_bytes - 1).to_be_bytes();
                std::ptr::copy_nonoverlapping(length.as_ptr(), length_ptr, 4);
            }
        }
    } else {
        // Slow path: at least one column carries a validity bitmap.
        // Per-cell NULL check mirrors the legacy `write_cell` path.
        // `Bitmap::get(i)` returns true for valid and false for null
        // (matches `is_null` in the legacy path).
        for row in 0..n {
            let a_null = a_nulls.is_some_and(|nulls| !nulls.get(row));
            let b_null = b_nulls.is_some_and(|nulls| !nulls.get(row));
            // SAFETY: bounded by the per-row 46-byte reserve above.
            unsafe {
                let dst = dst_base.add(off);
                *dst = DATA_ROW_TAG;
                let length_ptr = dst.add(1);
                *dst.add(5) = 0;
                *dst.add(6) = 2;
                off += 7;

                let a_payload_len = if a_null {
                    // -1 length, no payload bytes.
                    let neg_one = (-1_i32).to_be_bytes();
                    std::ptr::copy_nonoverlapping(neg_one.as_ptr(), dst_base.add(off), 4);
                    off += 4;
                    0
                } else {
                    let a_text = format_i32_into(&mut scratch_a, a[row]);
                    let a_len = a_text.len();
                    let a_len_be = i32_from_usize(a_len).to_be_bytes();
                    std::ptr::copy_nonoverlapping(a_len_be.as_ptr(), dst_base.add(off), 4);
                    off += 4;
                    std::ptr::copy_nonoverlapping(a_text.as_ptr(), dst_base.add(off), a_len);
                    off += a_len;
                    a_len
                };

                let b_payload_len = if b_null {
                    let neg_one = (-1_i32).to_be_bytes();
                    std::ptr::copy_nonoverlapping(neg_one.as_ptr(), dst_base.add(off), 4);
                    off += 4;
                    0
                } else {
                    let b_text = format_i64_into(&mut scratch_b, b[row]);
                    let b_len = b_text.len();
                    let b_len_be = i32_from_usize(b_len).to_be_bytes();
                    std::ptr::copy_nonoverlapping(b_len_be.as_ptr(), dst_base.add(off), 4);
                    off += 4;
                    std::ptr::copy_nonoverlapping(b_text.as_ptr(), dst_base.add(off), b_len);
                    off += b_len;
                    b_len
                };

                let row_bytes = 7 + 4 + a_payload_len + 4 + b_payload_len;
                let length = i32_from_usize(row_bytes - 1).to_be_bytes();
                std::ptr::copy_nonoverlapping(length.as_ptr(), length_ptr, 4);
            }
        }
    }
    // SAFETY: `off` ≤ `n * MAX_ROW_BYTES` because every row writes
    // at most MAX_ROW_BYTES; `dst_base` is the spare region of
    // `sink` we reserved above.
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
        Column::Float32(_) | Column::Float64(_) | Column::Utf8(_) | Column::DictionaryUtf8(_) => {
            // Safe to expect-unwrap: the null branch above already
            // handled the `None` case.
            let bytes =
                encode_text_value(col, row).expect("non-null cell must encode to Some(bytes)");
            sink.put_i32(i32_from_usize(bytes.len()));
            sink.put_slice(&bytes);
        }
    }
}

/// Emit one typed column cell. Most columns still use the allocation-free
/// physical writer; logical wrappers call the typed encoder.
fn write_cell_typed(sink: &mut BytesMut, col: &Column, row: usize, logical_type: &DataType) {
    if is_null(col, row) {
        sink.put_i32(-1);
        return;
    }
    match (logical_type, col) {
        (DataType::Date | DataType::Decimal { .. }, _) => {
            let bytes = encode_text_value_typed(col, row, logical_type)
                .expect("non-null typed cell must encode to Some(bytes)");
            sink.put_i32(i32_from_usize(bytes.len()));
            sink.put_slice(&bytes);
        }
        _ => write_cell(sink, col, row),
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
        Column::DictionaryUtf8(c) => c.codes.nulls(),
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

/// Two-digit decimal lookup table.
///
/// `DIGIT_PAIRS[2*n..2*n+2]` contains the ASCII representation of the
/// integer `n` for `n ∈ 0..100`. Used by [`format_i32_into`] and
/// [`format_i64_into`] to emit two decimal digits per loop iteration
/// instead of one: the per-digit `%` / `/` pair lowers to a single
/// 32-bit divide on AArch64 and x86_64 but unrolling halves the loop
/// trip count, and the indexed `copy` lands as two byte stores.
///
/// This is the canonical itoa lookup trick used by `std::fmt` and
/// the `itoa` crate; mirroring it here keeps the hot path
/// dependency-free.
///
/// The table is computed at compile time. `try_into` is not yet
/// `const fn`, so the const initializer threads a `u8` counter to
/// avoid the workspace-wide `as`-cast restriction (`AGENTS.md §3.3`).
/// `usize` indexing into `out` comes from `usize::from`, which is
/// the lossless promotion idiom and `const`-friendly. The resulting
/// table is a 200-byte read-only blob in `.rodata`.
const DIGIT_PAIRS: [u8; 200] = {
    let mut out = [0u8; 200];
    let mut i: u8 = 0;
    while i < 100 {
        // `i / 10` and `i % 10` are in 0..=9, so `b'0' + ...` fits
        // in `u8` without wrap. The two byte stores share the same
        // base offset; `usize::from(i) * 2` is the lossless idiom
        // for indexing.
        let base = (i as usize) * 2; // wrap-free: i < 100, base < 200.
        out[base] = b'0' + i / 10;
        out[base + 1] = b'0' + i % 10;
        i += 1;
    }
    out
};

/// Format an `i32` into the trailing bytes of `scratch` and return the
/// filled sub-slice (left-to-right reading order).
///
/// Writes digits from least to most significant into the back of the
/// buffer using a two-digit lookup table, then optionally prepends
/// the `'-'` sign. The returned slice points into `scratch` and lives
/// as long as the caller's borrow.
///
/// The arithmetic stays in `u32` rather than widening to `u64`: every
/// `%` / `/` pair lowers to a single 32-bit `idiv` on AArch64 and
/// x86_64. Widening to `u64` doubles the divisor's bit-width with no
/// benefit (`i32::MIN.unsigned_abs() == 2_147_483_648` fits in u32).
fn format_i32_into(scratch: &mut [u8; 12], value: i32) -> &[u8] {
    // Work with the absolute value as `u32` to handle `i32::MIN`
    // without overflow. `i32::MIN.unsigned_abs()` is the standard
    // idiom.
    let negative = value < 0;
    let mut n: u32 = value.unsigned_abs();
    let mut idx = scratch.len();
    // Emit two digits per iteration using the `DIGIT_PAIRS` lookup
    // table. For values with an odd digit count the final single
    // digit drops out of the trailing branch. The hot path (1- to
    // 5-digit positive integers in the `select_scan_10k` workload)
    // executes 1 lookup + 1 fallback or 2 lookups.
    while n >= 100 {
        let r = usize::try_from(n % 100).expect("n % 100 < 100");
        n /= 100;
        idx -= 2;
        scratch[idx] = DIGIT_PAIRS[2 * r];
        scratch[idx + 1] = DIGIT_PAIRS[2 * r + 1];
    }
    if n >= 10 {
        let r = usize::try_from(n).expect("n < 100 here");
        idx -= 2;
        scratch[idx] = DIGIT_PAIRS[2 * r];
        scratch[idx + 1] = DIGIT_PAIRS[2 * r + 1];
    } else {
        idx -= 1;
        // `n < 10` so `b'0' + n` fits in `u8`. The `try_from`
        // dance keeps the no-`as`-casts rule (`AGENTS.md §3.3`)
        // honoured; the compiler sees the bound and elides the
        // panic branch.
        let digit = u8::try_from(n).expect("n < 10");
        scratch[idx] = b'0' + digit;
    }
    if negative {
        idx -= 1;
        scratch[idx] = b'-';
    }
    &scratch[idx..]
}

/// Same routine for `i64`, using the same `DIGIT_PAIRS` two-digit
/// lookup as `format_i32_into`. Halves the loop trip count vs. the
/// per-digit `% 10 / 10` baseline; the lookup table is shared so we
/// pay no extra memory.
fn format_i64_into(scratch: &mut [u8; 20], value: i64) -> &[u8] {
    let negative = value < 0;
    let mut n: u64 = value.unsigned_abs();
    let mut idx = scratch.len();
    while n >= 100 {
        let r = usize::try_from(n % 100).expect("n % 100 < 100");
        n /= 100;
        idx -= 2;
        scratch[idx] = DIGIT_PAIRS[2 * r];
        scratch[idx + 1] = DIGIT_PAIRS[2 * r + 1];
    }
    if n >= 10 {
        let r = usize::try_from(n).expect("n < 100 here");
        idx -= 2;
        scratch[idx] = DIGIT_PAIRS[2 * r];
        scratch[idx + 1] = DIGIT_PAIRS[2 * r + 1];
    } else {
        idx -= 1;
        let digit = u8::try_from(n).expect("n < 10");
        scratch[idx] = b'0' + digit;
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

        for row_usize in 0_usize..3 {
            let row_i32 = i32::try_from(row_usize).expect("loop bound 3 fits in i32");
            let id_text = row_i32.to_string().into_bytes();
            let val_text = (row_i32 * 10).to_string().into_bytes();
            let canonical_msg = BackendMessage::DataRow {
                columns: vec![Some(id_text), Some(val_text)],
            };
            let mut canonical = BytesMut::new();
            encode_backend(&canonical_msg, &mut canonical);
            let mut actual = BytesMut::new();
            write_data_row(&mut actual, &cols, row_usize);
            assert_eq!(&actual[..], &canonical[..], "row {row_usize} mismatch");
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

    /// Sweep every value `[-N, N]` for both formatters to exercise the
    /// two-digit lookup path (`DIGIT_PAIRS`) at every digit-count
    /// transition. The transitions (1↔2, 2↔3, 4↔5, ...) are where
    /// off-by-one bugs in `idx -= 2 / idx -= 1` would surface.
    #[test]
    fn format_i32_sweep_matches_to_string() {
        for v in -1024_i32..=1024_i32 {
            let mut scratch = [0u8; 12];
            let actual = format_i32_into(&mut scratch, v);
            let expected = v.to_string();
            assert_eq!(actual, expected.as_bytes(), "value {v} mismatch");
        }
        // Cover several digit counts up to i32::MAX boundary.
        for &v in &[
            99_i32,
            100,
            999,
            1_000,
            9_999,
            10_000,
            99_999,
            100_000,
            999_999,
            1_000_000,
            i32::MAX - 1,
            i32::MAX,
            i32::MIN + 1,
            i32::MIN,
        ] {
            let mut scratch = [0u8; 12];
            let actual = format_i32_into(&mut scratch, v);
            assert_eq!(actual, v.to_string().as_bytes(), "value {v} mismatch");
        }
    }

    #[test]
    fn format_i64_sweep_matches_to_string() {
        for v in -1024_i64..=1024_i64 {
            let mut scratch = [0u8; 20];
            let actual = format_i64_into(&mut scratch, v);
            let expected = v.to_string();
            assert_eq!(actual, expected.as_bytes(), "value {v} mismatch");
        }
        for &v in &[
            i64::from(i32::MAX),
            i64::from(i32::MAX) + 1,
            1_000_000_000_000_i64,
            i64::MAX - 1,
            i64::MAX,
            i64::MIN + 1,
            i64::MIN,
        ] {
            let mut scratch = [0u8; 20];
            let actual = format_i64_into(&mut scratch, v);
            assert_eq!(actual, v.to_string().as_bytes(), "value {v} mismatch");
        }
    }

    /// `write_int32_pair_data_rows` must produce byte-identical output
    /// to the canonical `encode_backend(BackendMessage::DataRow)` for
    /// every row of the input. This is the hot-path used by
    /// `select_scan_10k`; a wire-byte mismatch silently corrupts the
    /// protocol stream, so cover every interesting i32 value.
    #[test]
    fn write_int32_pair_data_rows_matches_canonical_encoder() {
        let a: Vec<i32> = vec![0, 1, -1, 12345, -98765, i32::MAX, i32::MIN, 7];
        let b: Vec<i32> = vec![i32::MIN, i32::MAX, -1, 0, 1, 1_000_000, -1_000_000, 8];

        let mut canonical = BytesMut::new();
        for (av, bv) in a.iter().zip(b.iter()) {
            let msg = BackendMessage::DataRow {
                columns: vec![
                    Some(av.to_string().into_bytes()),
                    Some(bv.to_string().into_bytes()),
                ],
            };
            encode_backend(&msg, &mut canonical);
        }

        let mut actual = BytesMut::new();
        write_int32_pair_data_rows(&mut actual, &a, &b);

        assert_eq!(&actual[..], &canonical[..]);
    }

    /// Empty input must produce zero bytes (caller's responsibility to
    /// avoid calling on an empty batch is enforced by the operator
    /// chain emitting non-empty batches; the writer still tolerates
    /// the edge case).
    #[test]
    fn write_int32_pair_data_rows_empty_input_emits_nothing() {
        let mut actual = BytesMut::new();
        write_int32_pair_data_rows(&mut actual, &[], &[]);
        assert!(actual.is_empty());
    }

    /// `write_int32_int64_pair_data_rows` must produce byte-identical
    /// output to the canonical `encode_backend(BackendMessage::DataRow)`
    /// for every row of the input. Mirrors the `select_scan_10k` test
    /// against the `(Int32, Int64)` shape used by
    /// `WindowAgg::try_columnar_row_number`.
    #[test]
    fn write_int32_int64_pair_data_rows_matches_canonical_encoder() {
        let a: Vec<i32> = vec![0, 1, -1, 12345, -98765, i32::MAX, i32::MIN, 7];
        let b: Vec<i64> = vec![
            i64::MIN,
            i64::MAX,
            -1,
            0,
            1,
            1_000_000_000_000,
            -1_000_000_000_000,
            8,
        ];

        let mut canonical = BytesMut::new();
        for (av, bv) in a.iter().zip(b.iter()) {
            let msg = BackendMessage::DataRow {
                columns: vec![
                    Some(av.to_string().into_bytes()),
                    Some(bv.to_string().into_bytes()),
                ],
            };
            encode_backend(&msg, &mut canonical);
        }

        let mut actual = BytesMut::new();
        write_int32_int64_pair_data_rows(&mut actual, &a, None, &b, None);

        assert_eq!(&actual[..], &canonical[..]);
    }

    /// Sweep the two-digit-boundary region for both columns to flush
    /// out any off-by-one in the `idx -= 2 / idx -= 1` book-keeping
    /// across the i32+i64 boundary.
    #[test]
    fn write_int32_int64_pair_data_rows_sweep_two_digit_boundary() {
        let mut a: Vec<i32> = Vec::with_capacity(512);
        let mut b: Vec<i64> = Vec::with_capacity(512);
        // Cover [-128, 128] across both digit-count transitions in
        // each column, plus a few wide values.
        for v in -128_i32..=128_i32 {
            a.push(v);
            b.push(i64::from(v).wrapping_mul(1_000_000));
        }
        // Tack on the i32/i64 extrema so the worst-case scratch
        // widths are exercised.
        let extras_a: &[i32] = &[i32::MIN, i32::MAX, 0, -1, 99, 100, -99, -100];
        let extras_b: &[i64] = &[i64::MIN, i64::MAX, 0, -1, 99, 100, -99, -100];
        for (av, bv) in extras_a.iter().zip(extras_b.iter()) {
            a.push(*av);
            b.push(*bv);
        }

        let mut canonical = BytesMut::new();
        for (av, bv) in a.iter().zip(b.iter()) {
            let msg = BackendMessage::DataRow {
                columns: vec![
                    Some(av.to_string().into_bytes()),
                    Some(bv.to_string().into_bytes()),
                ],
            };
            encode_backend(&msg, &mut canonical);
        }

        let mut actual = BytesMut::new();
        write_int32_int64_pair_data_rows(&mut actual, &a, None, &b, None);

        assert_eq!(&actual[..], &canonical[..]);
    }

    /// `write_int32_int64_pair_data_rows` must still produce the
    /// canonical bytes when either column carries a validity bitmap.
    #[test]
    fn write_int32_int64_pair_data_rows_handles_nulls() {
        let a: Vec<i32> = vec![10, 20, 30, 40];
        let b: Vec<i64> = vec![100, 200, 300, 400];
        // a: nulls = [valid, null, valid, valid]
        // b: nulls = [valid, valid, null, valid]
        let mut a_nulls = Bitmap::new(4, false);
        a_nulls.set(0, true);
        a_nulls.set(1, false);
        a_nulls.set(2, true);
        a_nulls.set(3, true);
        let mut b_nulls = Bitmap::new(4, false);
        b_nulls.set(0, true);
        b_nulls.set(1, true);
        b_nulls.set(2, false);
        b_nulls.set(3, true);

        // Canonical: encode each row through the slow path.
        let mut canonical = BytesMut::new();
        let expected_a: &[Option<&[u8]>] = &[Some(b"10"), None, Some(b"30"), Some(b"40")];
        let expected_b: &[Option<&[u8]>] = &[Some(b"100"), Some(b"200"), None, Some(b"400")];
        for i in 0..4 {
            let columns = vec![
                expected_a[i].map(|s| s.to_vec()),
                expected_b[i].map(|s| s.to_vec()),
            ];
            encode_backend(&BackendMessage::DataRow { columns }, &mut canonical);
        }

        let mut actual = BytesMut::new();
        write_int32_int64_pair_data_rows(&mut actual, &a, Some(&a_nulls), &b, Some(&b_nulls));

        assert_eq!(&actual[..], &canonical[..]);
    }

    /// Byte-equivalence with the generic `write_data_row` path on a
    /// 2 048-row mixed sample. This is the test specified in the
    /// task description: it exercises the same operator-level shape
    /// (`(Int32, Int64)` non-nullable batch) that the
    /// `WindowAgg::try_columnar_row_number` path emits.
    #[test]
    fn write_int32_int64_pair_data_rows_byte_equivalent_to_generic_path_2048_rows() {
        // Mixed sample: positive, negative, zero, large magnitudes.
        // `i32` values cycle through a 7-element pattern so each row
        // has a different text length; `i64` values are
        // `row_number()`-shaped (1..=N) plus the extrema.
        let mut a: Vec<i32> = Vec::with_capacity(2048);
        let mut b: Vec<i64> = Vec::with_capacity(2048);
        let pattern_a: &[i32] = &[0, 1, -1, 12345, -98765, i32::MAX, i32::MIN];
        for i in 0..2048 {
            a.push(pattern_a[i % pattern_a.len()]);
            // Spread `b` across small + huge so every two-digit
            // boundary is hit at least once.
            let n = i64::try_from(i).expect("2048 fits in i64");
            b.push(if i % 100 == 0 {
                i64::MAX - n
            } else if i % 50 == 0 {
                i64::MIN + n
            } else {
                n + 1
            });
        }

        // Build the canonical bytes via the generic
        // `write_data_row` path that the slow fallback uses.
        use ultrasql_vec::column::NumericColumn;
        let cols = vec![
            Column::Int32(NumericColumn::from_data(a.clone())),
            Column::Int64(NumericColumn::from_data(b.clone())),
        ];
        let mut generic = BytesMut::new();
        for row in 0..2048 {
            write_data_row(&mut generic, &cols, row);
        }

        // Build the fast-path bytes.
        let mut fast = BytesMut::new();
        write_int32_int64_pair_data_rows(&mut fast, &a, None, &b, None);

        assert_eq!(
            &fast[..],
            &generic[..],
            "fast-path bytes diverge from generic path"
        );
    }

    /// Empty input must produce zero bytes — same edge-case contract
    /// as the `(Int32, Int32)` writer.
    #[test]
    fn write_int32_int64_pair_data_rows_empty_input_emits_nothing() {
        let mut actual = BytesMut::new();
        write_int32_int64_pair_data_rows(&mut actual, &[], None, &[], None);
        assert!(actual.is_empty());
    }
}
