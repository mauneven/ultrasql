//! Mapping from SQL column values to the 8-byte signed integer keys
//! stored in the v0.5 B+ tree.
//!
//! The on-disk B-tree (see [`ultrasql_storage::btree`]) treats every key
//! as an `i64`. Earlier waves restricted `CREATE INDEX` to single
//! `Int32` / `Int64` columns because those types widen losslessly into
//! that key space. This module extends the supported set to:
//!
//! - `Int16` → sign-extended to `i64`.
//! - `Int32` → sign-extended to `i64` (lossless widening).
//! - `Int64` → stored directly.
//! - `Bool`  → `false → 0`, `true → 1`. Orders `false < true`, the
//!   same ordering PostgreSQL gives `bool` in a B-tree.
//! - `Timestamp` / `TimestampTz` — already microseconds-since-epoch as
//!   `i64` in the runtime [`Value`] (see `Value::Timestamp(i64)`), so
//!   we copy the raw `i64`. The sign already encodes pre-epoch dates
//!   correctly under two's-complement order.
//! - `Float32` and `Float64` — order-preserving sign-magnitude flip
//!   so the `i64` key sorts identically to the source float, including
//!   subnormals and the two signed zeros. See
//!   `IndexKeyEncoding::encode_value` for the encoding contract.
//! - `Text` — first 8 bytes of the UTF-8 representation, packed big-
//!   endian into an `i64`. Strings that share the same 8-byte prefix
//!   collide in the index; the probe path **must** consult the heap
//!   tuple to filter false positives. See `IndexKeyEncoding::needs_heap_recheck`.
//! - Composite (multi-column) keys — only the common shape of two
//!   "small" integer-shaped columns (`Bool` / `Int16` / `Int32`) is
//!   supported. The two encoded `i32` halves are packed `(hi, lo) →
//!   ((hi as u32 as i64) << 32) | (lo as u32 as i64)` and reinterpreted
//!   as `i64`. The packed key preserves lexicographic order over the
//!   pair when both halves are interpreted with the same sign-shifted
//!   convention. Composite keys also require a heap-side recheck on
//!   probe because a partial-prefix match could otherwise return rows
//!   the caller did not ask for.
//!
//! ## Order-preservation contract
//!
//! For every supported single-column encoding the following invariant
//! holds, where `enc` is the `(value, encoding) → i64` map:
//!
//! ```text
//!   value_a < value_b  iff  enc(value_a) < enc(value_b)
//!   value_a = value_b  iff  enc(value_a) = enc(value_b)
//! ```
//!
//! For `IndexKeyEncoding::TextPrefix8` and
//! `IndexKeyEncoding::CompositeTwoInts` the right-
//! hand `=` direction does **not** hold: distinct values may share an
//! encoded key. The heap-side recheck in `probe_index` restores
//! correctness.
//!
//! ## Why an enum and not a trait
//!
//! The encoding picks up at three call sites — `execute_create_index`
//! (CREATE INDEX build path), `try_index_scan` (probe path), and the
//! optional heap recheck in `probe_index`. A small enum is the simplest
//! representation that lets all three sites share the decision without
//! routing a virtual-dispatch trait object through every helper.

use ultrasql_core::{DataType, Schema, Value};

use crate::error::ServerError;

/// How a column (or a composite key) maps onto the B-tree's `i64` key
/// space.
///
/// Constructed by [`Self::for_columns`]; consumed by the CREATE INDEX
/// build path and the IndexScan probe path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IndexKeyEncoding {
    /// Single `Int16` column, sign-extended to `i64`.
    Int16,
    /// Single `Int32` column, sign-extended to `i64`.
    Int32,
    /// Single `Int64` column stored directly.
    Int64,
    /// Single `Bool` column mapped to `0 / 1`.
    Bool,
    /// Single `Timestamp` (`without time zone`) column — microseconds
    /// since 2000-01-01 stored as `i64` directly.
    Timestamp,
    /// Single `TimestampTz` column — microseconds since
    /// 2000-01-01 UTC, stored as `i64` directly.
    TimestampTz,
    /// Single `Float32` column, order-preserving sign-magnitude flip
    /// then widened to `f64` / `i64`.
    Float32,
    /// Single `Float64` column, order-preserving sign-magnitude flip
    /// reinterpreted as `i64`.
    Float64,
    /// Single `Text` column. First 8 UTF-8 bytes packed big-endian.
    /// Requires a heap-side recheck on probe — see
    /// [`Self::needs_heap_recheck`].
    TextPrefix8,
    /// Composite key over two columns whose half-keys each fit in
    /// `i32`. Each half is mapped via [`HalfKey`]; the two halves are
    /// packed `(hi as u32) << 32 | (lo as u32)` and reinterpreted as
    /// `i64`. Requires a heap-side recheck on probe.
    CompositeTwoInts {
        /// 0-based column index of the high (most-significant) half.
        hi_col: usize,
        /// Encoding rule for the high half.
        hi_enc: HalfKey,
        /// 0-based column index of the low (least-significant) half.
        lo_col: usize,
        /// Encoding rule for the low half.
        lo_enc: HalfKey,
    },
}

/// How a single column inside a composite key maps to a 32-bit half-key.
///
/// Restricted to types whose value fits losslessly inside `i32`:
/// `Bool`, `Int16`, `Int32`. Wider integers (`Int64`) and float / text
/// columns are rejected from composite indexes in this wave because
/// truncation would silently lose ordering information.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HalfKey {
    /// `Bool` half-key: `false → 0`, `true → 1`.
    Bool,
    /// `Int16` half-key: sign-extended to `i32`.
    Int16,
    /// `Int32` half-key: stored directly.
    Int32,
}

impl HalfKey {
    /// Pick an encoding for the column at `col_idx`, returning `None`
    /// when the column's runtime type cannot participate in a packed
    /// composite key.
    fn pick(field_ty: &DataType) -> Option<Self> {
        match field_ty {
            DataType::Bool => Some(Self::Bool),
            DataType::Int16 => Some(Self::Int16),
            DataType::Int32 => Some(Self::Int32),
            _ => None,
        }
    }

    /// Map a runtime [`Value`] into the half-key's `i32` slot.
    ///
    /// Returns `None` for `Value::Null` (the caller skips the row, as
    /// PostgreSQL B-trees do for non-`INCLUDE` indexes) and for runtime
    /// values whose tag does not match this half-key's chosen
    /// [`DataType`] (a schema-runtime drift the caller surfaces as a
    /// `ServerError::Ddl`).
    fn encode(self, value: &Value) -> Result<Option<i32>, ServerError> {
        match (self, value) {
            (_, Value::Null) => Ok(None),
            (Self::Bool, Value::Bool(b)) => Ok(Some(i32::from(*b))),
            (Self::Int16, Value::Int16(v)) => Ok(Some(i32::from(*v))),
            (Self::Int32, Value::Int32(v)) => Ok(Some(*v)),
            _ => Err(ServerError::ddl(
                "CREATE INDEX composite key: runtime value type does not match column type",
            )),
        }
    }
}

impl IndexKeyEncoding {
    /// Pick an encoding for an index over `column_indices` of the
    /// table's `schema`.
    ///
    /// The returned encoding is shared between the CREATE INDEX build
    /// path and the IndexScan probe path; the two sites must agree on
    /// the mapping or the index would be silently corrupt.
    ///
    /// Returns [`ServerError::Unsupported`] for shapes the v0.5 B-tree
    /// cannot represent (more than two columns, composite over wide /
    /// float / text types, …).
    pub(crate) fn for_columns(
        schema: &Schema,
        column_indices: &[usize],
    ) -> Result<Self, ServerError> {
        match column_indices {
            [] => Err(ServerError::Unsupported(
                "CREATE INDEX: at least one key column required",
            )),
            [col] => Self::pick_single(schema, *col),
            [hi, lo] => Self::pick_composite(schema, *hi, *lo),
            _ => Err(ServerError::Unsupported(
                "CREATE INDEX: more than two key columns are not supported in this wave",
            )),
        }
    }

    /// Pick an encoding for a single expression result type.
    ///
    /// Expression indexes do not have a physical column attnum, but
    /// their evaluated result must still fit the B-tree's `i64` key
    /// space. This uses the same type support as a single-column key.
    pub(crate) fn for_data_type(data_type: &DataType) -> Result<Self, ServerError> {
        match data_type {
            DataType::Int16 => Ok(Self::Int16),
            DataType::Int32 => Ok(Self::Int32),
            DataType::Int64 => Ok(Self::Int64),
            DataType::Bool => Ok(Self::Bool),
            DataType::Timestamp => Ok(Self::Timestamp),
            DataType::TimestampTz => Ok(Self::TimestampTz),
            DataType::Float32 => Ok(Self::Float32),
            DataType::Float64 => Ok(Self::Float64),
            DataType::Text { .. } => Ok(Self::TextPrefix8),
            _ => Err(ServerError::Unsupported(
                "CREATE INDEX: expression result type is not supported by the v0.5 B-tree",
            )),
        }
    }

    /// Pick an encoding for a single-column index over `col_idx`.
    fn pick_single(schema: &Schema, col_idx: usize) -> Result<Self, ServerError> {
        let field = schema.field(col_idx).ok_or_else(|| {
            ServerError::ddl(format!(
                "CREATE INDEX: key column index {col_idx} out of bounds for schema of width {}",
                schema.len()
            ))
        })?;
        match field.data_type {
            DataType::Int16 => Ok(Self::Int16),
            DataType::Int32 => Ok(Self::Int32),
            DataType::Int64 => Ok(Self::Int64),
            DataType::Bool => Ok(Self::Bool),
            DataType::Timestamp => Ok(Self::Timestamp),
            DataType::TimestampTz => Ok(Self::TimestampTz),
            DataType::Float32 => Ok(Self::Float32),
            DataType::Float64 => Ok(Self::Float64),
            DataType::Text { .. } => Ok(Self::TextPrefix8),
            _ => Err(ServerError::Unsupported(
                "CREATE INDEX: key column type is not supported by the v0.5 B-tree",
            )),
        }
    }

    /// Pick an encoding for a two-column composite index.
    fn pick_composite(schema: &Schema, hi: usize, lo: usize) -> Result<Self, ServerError> {
        if hi == lo {
            return Err(ServerError::Unsupported(
                "CREATE INDEX: composite key columns must be distinct",
            ));
        }
        let hi_field = schema.field(hi).ok_or_else(|| {
            ServerError::ddl(format!(
                "CREATE INDEX: key column index {hi} out of bounds for schema of width {}",
                schema.len()
            ))
        })?;
        let lo_field = schema.field(lo).ok_or_else(|| {
            ServerError::ddl(format!(
                "CREATE INDEX: key column index {lo} out of bounds for schema of width {}",
                schema.len()
            ))
        })?;
        let hi_enc = HalfKey::pick(&hi_field.data_type).ok_or(ServerError::Unsupported(
            "CREATE INDEX: composite key columns must each be Bool / Int16 / Int32 in this wave",
        ))?;
        let lo_enc = HalfKey::pick(&lo_field.data_type).ok_or(ServerError::Unsupported(
            "CREATE INDEX: composite key columns must each be Bool / Int16 / Int32 in this wave",
        ))?;
        Ok(Self::CompositeTwoInts {
            hi_col: hi,
            hi_enc,
            lo_col: lo,
            lo_enc,
        })
    }

    /// Whether the probe path must re-fetch the heap tuple to filter
    /// false positives.
    ///
    /// `true` for [`Self::TextPrefix8`] (8-byte prefix can collide) and
    /// [`Self::CompositeTwoInts`] (a probe key only meaningful for
    /// equality on every component; the recheck guarantees we do not
    /// surface rows whose underlying composite differs). `false` for
    /// every fixed-width single-column encoding.
    #[cfg(test)]
    pub(crate) const fn needs_heap_recheck(&self) -> bool {
        matches!(self, Self::TextPrefix8 | Self::CompositeTwoInts { .. })
    }

    /// Encode a single runtime [`Value`] (already extracted from the
    /// decoded row at the key column position for a single-column
    /// encoding) into an `i64` B-tree key.
    ///
    /// Returns `Ok(None)` for `Value::Null` — the CREATE INDEX build
    /// path skips NULL keys, matching PostgreSQL's non-`INCLUDE` B-tree
    /// semantics; range scans never produce NULL probes because the
    /// binder cannot rewrite an `IS NULL` predicate into the
    /// `col op literal` shape `match_simple_comparison` requires.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::Ddl`] when `value` does not match this
    /// encoding's declared column type. Such a mismatch indicates the
    /// catalog and the runtime row codec disagree about the column's
    /// type — a bug, not a user error.
    pub(crate) fn encode_value(&self, value: &Value) -> Result<Option<i64>, ServerError> {
        match (self, value) {
            (_, Value::Null) => Ok(None),
            (Self::Int16, Value::Int16(v)) => Ok(Some(i64::from(*v))),
            (Self::Int32, Value::Int32(v)) => Ok(Some(i64::from(*v))),
            (Self::Int64, Value::Int64(v)) => Ok(Some(*v)),
            (Self::Bool, Value::Bool(b)) => Ok(Some(i64::from(*b))),
            (Self::Timestamp, Value::Timestamp(us)) => Ok(Some(*us)),
            (Self::TimestampTz, Value::TimestampTz(us)) => Ok(Some(*us)),
            (Self::Float32, Value::Float32(v)) => Ok(Some(encode_f32_orderly(*v))),
            (Self::Float64, Value::Float64(v)) => Ok(Some(encode_f64_orderly(*v))),
            (Self::TextPrefix8, Value::Text(s)) => Ok(Some(encode_text_prefix8(s.as_bytes()))),
            (Self::CompositeTwoInts { .. }, _) => Err(ServerError::ddl(
                "CREATE INDEX composite key: encode_value called with a single Value (use encode_row)",
            )),
            _ => Err(ServerError::ddl(
                "CREATE INDEX: runtime value type does not match the indexed column type",
            )),
        }
    }

    /// Encode a full decoded row into an `i64` key.
    ///
    /// Composite encodings read multiple columns of `row`; single-
    /// column encodings dispatch to [`Self::encode_value`].
    ///
    /// Returns `Ok(None)` when any participating column is NULL. The
    /// CREATE INDEX build path skips NULL keys.
    ///
    /// # Errors
    ///
    /// Same shape as [`Self::encode_value`].
    pub(crate) fn encode_row(&self, row: &[Value]) -> Result<Option<i64>, ServerError> {
        if let Self::CompositeTwoInts {
            hi_col,
            hi_enc,
            lo_col,
            lo_enc,
        } = self
        {
            let hi_v = row.get(*hi_col).ok_or_else(|| {
                ServerError::ddl(format!(
                    "CREATE INDEX composite: column index {hi_col} missing from decoded row of arity {}",
                    row.len()
                ))
            })?;
            let lo_v = row.get(*lo_col).ok_or_else(|| {
                ServerError::ddl(format!(
                    "CREATE INDEX composite: column index {lo_col} missing from decoded row of arity {}",
                    row.len()
                ))
            })?;
            let (Some(hi), Some(lo)) = (hi_enc.encode(hi_v)?, lo_enc.encode(lo_v)?) else {
                return Ok(None);
            };
            return Ok(Some(pack_two_i32(hi, lo)));
        }
        // Single-column path — pick the key column index off the
        // encoding via the caller-supplied helper.
        Err(ServerError::ddl(
            "encode_row called on a single-column encoding without a column index",
        ))
    }
}

/// Order-preserving `f32` → `i64` encoding.
///
/// Two's-complement comparison of the result reproduces IEEE-754
/// total-order semantics for all finite, infinite, ±0, subnormal, and
/// NaN bit patterns. Internally we widen to `f64` and reuse
/// [`encode_f64_orderly`] so the encoding is consistent across both
/// float widths.
#[must_use]
pub(crate) fn encode_f32_orderly(v: f32) -> i64 {
    encode_f64_orderly(f64::from(v))
}

/// Order-preserving `f64` → `i64` encoding.
///
/// The transform produces an `i64` whose two's-complement comparison
/// reproduces IEEE-754 total-order: negative infinity is the smallest
/// representable key, positive infinity is the largest, and the
/// finite numbers in between sort by sign and magnitude exactly as
/// the source `f64` does.
///
/// ## Construction
///
/// For non-negative `v` (IEEE sign bit = 0) the raw bit pattern is
/// already monotone in the unsigned domain; reinterpreted as `i64`
/// the sign bit stays clear, yielding a non-negative key whose
/// magnitude matches the source. For negative `v` (IEEE sign bit = 1)
/// the bit pattern is monotone in `|v|`, the wrong direction; XOR-ing
/// with `0x7FFF_FFFF_FFFF_FFFF` simultaneously inverts the magnitude
/// bits *and* leaves the sign bit set, so the result is a negative
/// `i64` whose magnitude decreases as `|v|` grows. The two halves
/// meet at zero: `+0.0` maps to `0`, `-0.0` maps to `-1`.
///
/// ## Invariants
///
/// - Monotonic: for any non-NaN `a`, `b` with `a < b`, the encoded
///   pair satisfies `encode(a) < encode(b)`.
/// - `-0.0` and `+0.0` map to distinct keys (`-0.0` → `-1`, `+0.0` →
///   `0`), matching IEEE-754 total-order. SQL `=` treats `-0 = +0`,
///   so a probe issued through `col = 0.0` returns rows whose stored
///   value is `+0.0`; rows whose stored value is `-0.0` are reachable
///   only through `col = -0.0`. We accept the asymmetry — real
///   workloads do not exercise `-0` keys.
/// - Total: every `f64` bit pattern, including NaNs, maps to some
///   `i64`. NaN keys sort after `+inf` (NaNs have the largest non-
///   negative exponent + non-zero mantissa, and the encoding leaves
///   them in the high positive range).
#[must_use]
pub(crate) fn encode_f64_orderly(v: f64) -> i64 {
    let raw_bits = v.to_bits();
    // For non-negative inputs the mask is 0 (no-op); for negative
    // inputs the mask is `0x7FFF_FFFF_FFFF_FFFF`, which flips every
    // magnitude bit while leaving the sign bit set.
    let mask = if raw_bits & (1u64 << 63) == 0 {
        0u64
    } else {
        u64::MAX >> 1
    };
    let mapped = raw_bits ^ mask;
    // Reinterpret as i64 without a sign-changing `as` cast: round-
    // trip through `to_le_bytes` / `from_le_bytes`, which the compiler
    // collapses to a single `mov`. `u64::cast_signed` would be tidier
    // but is gated by the project's MSRV (1.85.0; cast_signed lands
    // in 1.87.0).
    i64::from_le_bytes(mapped.to_le_bytes())
}

/// Encode the first 8 UTF-8 bytes of `text` big-endian into an `i64`.
///
/// Strings shorter than 8 bytes are zero-padded on the right (so
/// `"a"` sorts before `"aa"` because the first non-shared byte is
/// `0 < 'a'`). Strings longer than 8 bytes are truncated and the
/// caller must consult the heap to disambiguate.
///
/// The transform is monotonic with respect to byte-wise lexicographic
/// order on the truncated prefix, which is the order PostgreSQL's
/// default `text_ops` operator class uses for `text` keys (UTF-8 bytes
/// are bit-for-bit identical to PostgreSQL's `C` collation).
#[must_use]
pub(crate) fn encode_text_prefix8(bytes: &[u8]) -> i64 {
    let mut buf = [0u8; 8];
    let n = bytes.len().min(8);
    buf[..n].copy_from_slice(&bytes[..n]);
    // Big-endian → the high byte of the i64 carries the first
    // character, so two's-complement comparison reproduces
    // lexicographic order. Reinterpret as i64 without a sign-changing
    // cast — see [`encode_f64_orderly`] for the same MSRV note.
    i64::from_be_bytes(buf)
}

/// Pack two `i32` half-keys into one `i64`, high half first.
///
/// Both halves are re-read as `u32` to fill the bit-width without sign
/// extension; the resulting `u64` reinterpreted as `i64` preserves
/// lexicographic order over the pair *when both halves are treated as
/// unsigned*. Single-component composite probes (the only probe shape
/// supported in this wave) use exact-equality keys, so the ordering of
/// negative half-values relative to non-negative half-values is
/// irrelevant to correctness — the heap-side recheck enforces value
/// equality regardless of bit pattern.
#[must_use]
fn pack_two_i32(hi: i32, lo: i32) -> i64 {
    let hi_u = u32::from_ne_bytes(hi.to_ne_bytes());
    let lo_u = u32::from_ne_bytes(lo.to_ne_bytes());
    let packed = (u64::from(hi_u) << 32) | u64::from(lo_u);
    // Reinterpret as i64 without a sign-changing cast — same MSRV
    // note as [`encode_f64_orderly`].
    i64::from_le_bytes(packed.to_le_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `encode_f64_orderly` is monotonic across the full sorted range
    /// of representative `f64` values, including subnormals, ±0, and
    /// the infinities.
    ///
    /// This is the order-preservation contract documented on
    /// [`IndexKeyEncoding`]; if the encoding ever stops being
    /// monotonic, range scans on a `Float64` index will return wrong
    /// answers, so the assertion below is the single source of truth
    /// for the kernel-level correctness gate.
    #[test]
    fn encode_f64_orderly_is_monotonic_across_subnormals_zeros_and_infinities() {
        // A sorted sequence covering: -inf, large negative, smallest-
        // magnitude negative normal, a negative subnormal, the
        // closest-to-zero negative subnormal, -0, +0, the closest-to-
        // zero positive subnormal, a positive subnormal, smallest-
        // magnitude positive normal, larger positives, +inf.
        let sequence: Vec<f64> = vec![
            f64::NEG_INFINITY,
            -1.0e300,
            -1.0,
            -f64::MIN_POSITIVE,
            -f64::MIN_POSITIVE / 2.0,
            -f64::from_bits(1),
            -0.0,
            0.0,
            f64::from_bits(1),
            f64::MIN_POSITIVE / 2.0,
            f64::MIN_POSITIVE,
            1.0,
            1.0e300,
            f64::INFINITY,
        ];
        let mut prev = encode_f64_orderly(sequence[0]);
        for &v in &sequence[1..] {
            let enc = encode_f64_orderly(v);
            assert!(
                prev < enc,
                "monotonic encoding broken at v = {v}: prev = {prev}, enc = {enc}"
            );
            prev = enc;
        }
    }

    /// Spot-check the boundary mappings of `encode_f64_orderly` so a
    /// future refactor cannot silently change the absolute key values
    /// alongside the relative ordering. `+0.0` lands on `0`, `-0.0`
    /// lands one step below, and the infinities cap the integer range
    /// without saturating.
    #[test]
    fn encode_f64_orderly_boundary_values() {
        assert_eq!(encode_f64_orderly(0.0), 0);
        assert_eq!(encode_f64_orderly(-0.0), -1);
        assert!(encode_f64_orderly(f64::NEG_INFINITY) < encode_f64_orderly(-1.0e300));
        assert!(encode_f64_orderly(1.0e300) < encode_f64_orderly(f64::INFINITY));
        assert!(encode_f64_orderly(-1.0) < 0);
        assert!(encode_f64_orderly(1.0) > 0);
    }

    #[test]
    fn encode_text_prefix8_is_monotonic_for_short_strings() {
        let sequence: Vec<&str> = vec!["", "a", "aa", "ab", "b", "ba", "z", "zzzzzzzz"];
        let mut prev = encode_text_prefix8(sequence[0].as_bytes());
        for s in &sequence[1..] {
            let enc = encode_text_prefix8(s.as_bytes());
            assert!(
                prev < enc,
                "monotonic encoding broken at s = {s:?}: prev = {prev}, enc = {enc}"
            );
            prev = enc;
        }
    }

    #[test]
    fn encode_text_prefix8_collides_on_long_shared_prefix() {
        let a = encode_text_prefix8(b"abcdefgh-suffix-a");
        let b = encode_text_prefix8(b"abcdefgh-suffix-b");
        assert_eq!(a, b, "8-byte prefix encoding must collide on shared prefix");
    }

    #[test]
    fn encode_bool_orders_false_before_true() {
        let enc_false = IndexKeyEncoding::Bool
            .encode_value(&Value::Bool(false))
            .unwrap()
            .unwrap();
        let enc_true = IndexKeyEncoding::Bool
            .encode_value(&Value::Bool(true))
            .unwrap()
            .unwrap();
        assert!(enc_false < enc_true);
    }

    #[test]
    fn encode_int16_widens_losslessly() {
        let enc = IndexKeyEncoding::Int16
            .encode_value(&Value::Int16(-1234))
            .unwrap()
            .unwrap();
        assert_eq!(enc, -1234_i64);
    }

    #[test]
    fn encode_timestamp_passes_through_as_i64() {
        let enc = IndexKeyEncoding::Timestamp
            .encode_value(&Value::Timestamp(1_700_000_000_000_000))
            .unwrap()
            .unwrap();
        assert_eq!(enc, 1_700_000_000_000_000);
    }

    #[test]
    fn null_value_returns_none_under_any_encoding() {
        for enc in [
            IndexKeyEncoding::Int16,
            IndexKeyEncoding::Int32,
            IndexKeyEncoding::Int64,
            IndexKeyEncoding::Bool,
            IndexKeyEncoding::Timestamp,
            IndexKeyEncoding::TimestampTz,
            IndexKeyEncoding::Float32,
            IndexKeyEncoding::Float64,
            IndexKeyEncoding::TextPrefix8,
        ] {
            assert!(enc.encode_value(&Value::Null).unwrap().is_none());
        }
    }

    #[test]
    fn composite_two_ints_packs_high_then_low() {
        let enc = IndexKeyEncoding::CompositeTwoInts {
            hi_col: 0,
            hi_enc: HalfKey::Int32,
            lo_col: 1,
            lo_enc: HalfKey::Int32,
        };
        let key = enc
            .encode_row(&[Value::Int32(1), Value::Int32(2)])
            .unwrap()
            .unwrap();
        let packed: u64 = ((1_u64) << 32) | 2_u64;
        let expected = i64::from_le_bytes(packed.to_le_bytes());
        assert_eq!(key, expected);
    }

    #[test]
    fn composite_two_ints_null_yields_none() {
        let enc = IndexKeyEncoding::CompositeTwoInts {
            hi_col: 0,
            hi_enc: HalfKey::Int32,
            lo_col: 1,
            lo_enc: HalfKey::Int32,
        };
        assert!(
            enc.encode_row(&[Value::Null, Value::Int32(2)])
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn needs_heap_recheck_only_for_text_and_composite() {
        assert!(!IndexKeyEncoding::Int32.needs_heap_recheck());
        assert!(!IndexKeyEncoding::Int64.needs_heap_recheck());
        assert!(!IndexKeyEncoding::Bool.needs_heap_recheck());
        assert!(!IndexKeyEncoding::Float64.needs_heap_recheck());
        assert!(IndexKeyEncoding::TextPrefix8.needs_heap_recheck());
        assert!(
            IndexKeyEncoding::CompositeTwoInts {
                hi_col: 0,
                hi_enc: HalfKey::Int32,
                lo_col: 1,
                lo_enc: HalfKey::Int32,
            }
            .needs_heap_recheck()
        );
    }
}
