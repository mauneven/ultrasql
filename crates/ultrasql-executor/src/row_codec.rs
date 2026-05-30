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

use ultrasql_core::{
    DataType, GeometryValue, Lsn, MAX_VECTOR_DIMS, Oid, RangeValue, Schema, Value,
    coerce_bpchar_text, composite_text_matches_arity, pack_timetz, unpack_timetz,
};
use ultrasql_vec::bitmap::Bitmap;
use ultrasql_vec::column::{BoolColumn, Column, NumericColumn};
use ultrasql_vec::{Batch, DictionaryEncodingPolicy, StringEncoding, encode_strings_auto};

// Stable VECTOR payload layout: u32 little-endian dimension count,
// followed by that many f32 little-endian elements.
const VECTOR_DIMS_WIDTH: usize = std::mem::size_of::<u32>();
const VECTOR_ELEMENT_WIDTH: usize = std::mem::size_of::<f32>();
const NUMERIC_NBASE: u16 = 10_000;
const NUMERIC_DEC_DIGITS: i32 = 4;
const NUMERIC_DEC_DIGITS_USIZE: usize = 4;
const NUMERIC_DSCALE_MAX: i32 = 0x3fff;
const NUMERIC_POS: u16 = 0x0000;
const NUMERIC_NEG: u16 = 0x4000;
const NUMERIC_BINARY_HEADER_WIDTH: usize = 8;
const NUMERIC_DIGIT_WIDTH: usize = std::mem::size_of::<u16>();

fn u32_payload_len_to_usize(len: u32) -> Result<usize, RowCodecError> {
    usize::try_from(len).map_err(|_| RowCodecError::LengthOverflow { len })
}

fn checked_payload_end(cursor: usize, len: usize, have: usize) -> Result<usize, RowCodecError> {
    cursor.checked_add(len).ok_or(RowCodecError::Truncated {
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

impl RowCodec {
    /// Bind a codec to `schema`.
    #[must_use]
    pub fn new(schema: Schema) -> Self {
        let bound = Self::compute_fixed_width_lower_bound(&schema);
        let decode_shape = Self::detect_decode_shape(&schema);
        Self {
            schema,
            fixed_width_lower_bound: bound,
            decode_shape,
        }
    }

    /// Determine which fast-path decode loop applies to `schema`.
    /// Falls back to [`DecodeShape::Generic`] for any schema not
    /// covered by a hand-rolled inline path.
    fn detect_decode_shape(schema: &Schema) -> DecodeShape {
        let fields = schema.fields();
        let types: Vec<&DataType> = fields.iter().map(|f| f.data_type.storage_type()).collect();
        match types.as_slice() {
            [DataType::Int32] => DecodeShape::I32x1,
            [DataType::Int32, DataType::Int32] => DecodeShape::I32x2,
            [DataType::Int32, DataType::Int32, DataType::Int32] => DecodeShape::I32x3,
            [DataType::Int64] => DecodeShape::I64x1,
            [DataType::Int64, DataType::Int64] => DecodeShape::I64x2,
            _ => DecodeShape::Generic,
        }
    }

    /// Dispatch a fast-path decode for the all-non-null branch of the
    /// detected [`DecodeShape`]. Returns `Some(())` on a hit,
    /// `None` to fall through to the generic path (NULL present,
    /// truncated tuple, or schema not covered by a fast path).
    ///
    /// The fast path:
    ///
    /// - Reads the leading null-bitmap byte and bails to the generic
    ///   path if any of the schema's NULL bits are set.
    /// - Confirms the payload has the exact fixed width for the
    ///   shape (single bounds check, not one per column).
    /// - Inline-decodes each fixed-width column via
    ///   `i32::from_le_bytes` / `i64::from_le_bytes` on stack-resident
    ///   4- / 8-byte arrays — no `try_into` round trips, no
    ///   `&mut ColumnBuilder` match dispatch.
    /// - Marks every position valid via `nulls.push_valid()`.
    #[inline]
    #[allow(clippy::too_many_lines)]
    fn try_decode_fast_path(
        shape: DecodeShape,
        bytes: &[u8],
        builders: &mut [ColumnBuilder],
    ) -> Result<Option<()>, RowCodecError> {
        // Common preamble: 1 bitmap byte for any schema with ≤ 8
        // columns. All specialised shapes are 1-, 2-, or 3-column,
        // so the bitmap byte count is always 1.
        if bytes.is_empty() {
            return Ok(None);
        }
        let bitmap0 = bytes[0];
        if bitmap0 != 0 {
            // Any column is NULL — defer to the generic path which
            // emits `push_null()` correctly.
            return Ok(None);
        }
        match shape {
            DecodeShape::Generic => Ok(None),
            DecodeShape::I32x1 => {
                if bytes.len() < 1 + 4 {
                    return Err(RowCodecError::Truncated {
                        needed: 1 + 4,
                        have: bytes.len(),
                    });
                }
                let v0 = i32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
                if let ColumnBuilder::Int32 { data, nulls } = &mut builders[0] {
                    data.push(v0);
                    nulls.push_valid();
                    Ok(Some(()))
                } else {
                    // Builder mismatch: caller built the wrong
                    // builder type for this codec. Defer to generic
                    // path which surfaces a clearer error.
                    Ok(None)
                }
            }
            DecodeShape::I32x2 => {
                if bytes.len() < 1 + 8 {
                    return Err(RowCodecError::Truncated {
                        needed: 1 + 8,
                        have: bytes.len(),
                    });
                }
                let v0 = i32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
                let v1 = i32::from_le_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);
                let (head, tail) = builders.split_at_mut(1);
                if let (
                    ColumnBuilder::Int32 {
                        data: d0,
                        nulls: n0,
                    },
                    ColumnBuilder::Int32 {
                        data: d1,
                        nulls: n1,
                    },
                ) = (&mut head[0], &mut tail[0])
                {
                    d0.push(v0);
                    n0.push_valid();
                    d1.push(v1);
                    n1.push_valid();
                    Ok(Some(()))
                } else {
                    Ok(None)
                }
            }
            DecodeShape::I32x3 => {
                if bytes.len() < 1 + 12 {
                    return Err(RowCodecError::Truncated {
                        needed: 1 + 12,
                        have: bytes.len(),
                    });
                }
                let v0 = i32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
                let v1 = i32::from_le_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);
                let v2 = i32::from_le_bytes([bytes[9], bytes[10], bytes[11], bytes[12]]);
                let (head, rest) = builders.split_at_mut(1);
                let (mid, tail) = rest.split_at_mut(1);
                if let (
                    ColumnBuilder::Int32 {
                        data: d0,
                        nulls: n0,
                    },
                    ColumnBuilder::Int32 {
                        data: d1,
                        nulls: n1,
                    },
                    ColumnBuilder::Int32 {
                        data: d2,
                        nulls: n2,
                    },
                ) = (&mut head[0], &mut mid[0], &mut tail[0])
                {
                    d0.push(v0);
                    n0.push_valid();
                    d1.push(v1);
                    n1.push_valid();
                    d2.push(v2);
                    n2.push_valid();
                    Ok(Some(()))
                } else {
                    Ok(None)
                }
            }
            DecodeShape::I64x1 => {
                if bytes.len() < 1 + 8 {
                    return Err(RowCodecError::Truncated {
                        needed: 1 + 8,
                        have: bytes.len(),
                    });
                }
                let mut cursor = 1;
                let raw = read_fixed::<8>(bytes, &mut cursor)?;
                let v0 = i64::from_le_bytes(raw);
                if let ColumnBuilder::Int64 { data, nulls } = &mut builders[0] {
                    data.push(v0);
                    nulls.push_valid();
                    Ok(Some(()))
                } else {
                    Ok(None)
                }
            }
            DecodeShape::I64x2 => {
                if bytes.len() < 1 + 16 {
                    return Err(RowCodecError::Truncated {
                        needed: 1 + 16,
                        have: bytes.len(),
                    });
                }
                let mut cursor = 1;
                let r0 = read_fixed::<8>(bytes, &mut cursor)?;
                let r1 = read_fixed::<8>(bytes, &mut cursor)?;
                let v0 = i64::from_le_bytes(r0);
                let v1 = i64::from_le_bytes(r1);
                let (head, tail) = builders.split_at_mut(1);
                if let (
                    ColumnBuilder::Int64 {
                        data: d0,
                        nulls: n0,
                    },
                    ColumnBuilder::Int64 {
                        data: d1,
                        nulls: n1,
                    },
                ) = (&mut head[0], &mut tail[0])
                {
                    d0.push(v0);
                    n0.push_valid();
                    d1.push(v1);
                    n1.push_valid();
                    Ok(Some(()))
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// The schema this codec was bound to.
    #[must_use]
    pub const fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Lower bound on the encoded byte length, computed once at
    /// construction time. Used as the initial capacity for the
    /// `Vec<u8>` returned by [`Self::encode`] so the first push does
    /// not reallocate for typical fixed-width payloads.
    #[must_use]
    pub const fn fixed_width_lower_bound(&self) -> usize {
        self.fixed_width_lower_bound
    }

    /// Compute the encoded-payload lower bound: null-bitmap bytes
    /// plus the sum of each column's fixed size. Variable-width columns
    /// add zero — the encoder will grow past the lower bound for those.
    fn compute_fixed_width_lower_bound(schema: &Schema) -> usize {
        let n = schema.len();
        let bitmap_bytes = n.div_ceil(8);
        let mut acc = bitmap_bytes;
        for field in schema.fields() {
            if let Some(sz) = field.data_type.fixed_size() {
                acc = acc.saturating_add(sz);
            }
        }
        acc
    }

    /// Encode `row` into a byte payload.
    ///
    /// # Errors
    ///
    /// - [`RowCodecError::Arity`] — `row.len() != schema.len()`.
    /// - [`RowCodecError::Type`] — runtime/schema type mismatch.
    /// - [`RowCodecError::UnsupportedType`] — unsupported `DataType`.
    pub fn encode(&self, row: &[Value]) -> Result<Vec<u8>, RowCodecError> {
        let n = self.schema.len();
        if row.len() != n {
            return Err(RowCodecError::Arity {
                schema: n,
                row: row.len(),
            });
        }
        let bitmap_bytes = n.div_ceil(8);
        let mut bitmap = vec![0_u8; bitmap_bytes];
        // Pre-size to the cached fixed-width lower bound. For
        // fixed-width schemas this is the exact final length; varlena
        // schemas grow past it but the first push never reallocates.
        let payload_cap = self.fixed_width_lower_bound.saturating_sub(bitmap_bytes);
        let mut payload: Vec<u8> = Vec::with_capacity(payload_cap);

        for (col_idx, (value, field)) in row.iter().zip(self.schema.fields().iter()).enumerate() {
            if matches!(value, Value::Null) {
                let byte = col_idx / 8;
                let bit = col_idx % 8;
                bitmap[byte] |= 1 << bit;
                continue;
            }
            match (field.data_type.storage_type(), value) {
                (DataType::Bool, Value::Bool(v)) => payload.push(u8::from(*v)),
                (DataType::Int16, Value::Int16(v)) => {
                    payload.extend_from_slice(&v.to_le_bytes());
                }
                (DataType::Int32, Value::Int32(v)) => {
                    payload.extend_from_slice(&v.to_le_bytes());
                }
                (DataType::Int64, Value::Int64(v)) => {
                    payload.extend_from_slice(&v.to_le_bytes());
                }
                (DataType::Money, Value::Money(v)) => {
                    payload.extend_from_slice(&v.to_le_bytes());
                }
                (DataType::Oid, Value::Oid(v))
                | (DataType::RegClass, Value::RegClass(v))
                | (DataType::RegType, Value::RegType(v)) => {
                    payload.extend_from_slice(&v.raw().to_le_bytes());
                }
                (DataType::PgLsn, Value::PgLsn(v)) => {
                    payload.extend_from_slice(&v.raw().to_le_bytes());
                }
                (DataType::Float32, Value::Float32(v)) => {
                    payload.extend_from_slice(&v.to_le_bytes());
                }
                (DataType::Float64, Value::Float64(v)) => {
                    payload.extend_from_slice(&v.to_le_bytes());
                }
                (DataType::Date, Value::Date(v)) => {
                    // `Date` stores days since 2000-01-01 in an `i32`.
                    // Same wire shape as `Int32`; the column-builder
                    // path reuses the `Int32` arm for storage. Schema
                    // type tags carry the date semantics for the
                    // surrounding executor.
                    payload.extend_from_slice(&v.to_le_bytes());
                }
                (
                    DataType::Decimal {
                        precision,
                        scale: declared_scale,
                    },
                    Value::Decimal { value, scale },
                ) => {
                    validate_decimal_precision(
                        *value,
                        *scale,
                        *precision,
                        *declared_scale,
                        col_idx,
                        &field.data_type,
                    )?;
                    encode_numeric_value_payload(
                        &mut payload,
                        *value,
                        *scale,
                        col_idx,
                        &field.data_type,
                    )?;
                }
                (DataType::Timestamp, Value::Timestamp(v))
                | (DataType::TimestampTz, Value::TimestampTz(v))
                | (DataType::Time, Value::Time(v)) => {
                    // Microsecond-precision temporal: 8 bytes LE i64
                    // (microseconds since 2000-01-01 for Timestamp/Tz,
                    // microseconds since midnight for Time).
                    payload.extend_from_slice(&v.to_le_bytes());
                }
                (
                    DataType::TimeTz,
                    Value::TimeTz {
                        micros,
                        offset_seconds,
                    },
                ) => {
                    let packed = pack_timetz(*micros, *offset_seconds).ok_or_else(|| {
                        RowCodecError::Type {
                            column: col_idx,
                            expected: field.data_type.clone(),
                            got: value.data_type().to_string(),
                        }
                    })?;
                    payload.extend_from_slice(&packed.to_le_bytes());
                }
                (
                    DataType::Interval,
                    Value::Interval {
                        months,
                        days,
                        microseconds,
                    },
                ) => {
                    payload.extend_from_slice(&microseconds.to_le_bytes());
                    payload.extend_from_slice(&days.to_le_bytes());
                    payload.extend_from_slice(&months.to_le_bytes());
                }
                (DataType::Uuid, Value::Uuid(bytes)) => {
                    payload.extend_from_slice(bytes);
                }
                (DataType::Bytea, Value::Bytea(bytes)) => {
                    let len =
                        u32::try_from(bytes.len()).map_err(|_| RowCodecError::UnsupportedType {
                            column: col_idx,
                            ty: field.data_type.clone(),
                        })?;
                    payload.extend_from_slice(&len.to_le_bytes());
                    payload.extend_from_slice(bytes);
                }
                (DataType::Bit { .. } | DataType::VarBit { .. }, Value::BitString(bits)) => {
                    let coerced = bits.coerce_to(&field.data_type, false).ok_or_else(|| {
                        RowCodecError::StringDataRightTruncation {
                            column: col_idx,
                            ty: field.data_type.clone(),
                            detail: format!(
                                "bit string length {} does not match type {}",
                                bits.len(),
                                field.data_type
                            ),
                        }
                    })?;
                    encode_varlena_text(
                        &mut payload,
                        &coerced.to_string(),
                        col_idx,
                        &field.data_type,
                    )?;
                }
                (
                    DataType::Inet | DataType::Cidr | DataType::MacAddr | DataType::MacAddr8,
                    Value::Network(network),
                ) if network.data_type() == field.data_type => {
                    encode_varlena_text(
                        &mut payload,
                        &network.to_string(),
                        col_idx,
                        &field.data_type,
                    )?;
                }
                (DataType::Text { max_len }, Value::Text(s)) => {
                    validate_varchar_storage_text(s, *max_len, col_idx, &field.data_type)?;
                    encode_varlena_text(&mut payload, s, col_idx, &field.data_type)?;
                }
                (DataType::Enum { labels, .. }, Value::Text(s))
                    if enum_label_is_valid(labels, s) =>
                {
                    encode_varlena_text(&mut payload, s, col_idx, &field.data_type)?;
                }
                (DataType::Composite { fields, .. }, Value::Text(s))
                    if composite_text_matches_arity(s, fields.len()) =>
                {
                    encode_varlena_text(&mut payload, s, col_idx, &field.data_type)?;
                }
                (DataType::Char { len }, Value::Char(s) | Value::Text(s)) => {
                    let coerced = coerce_bpchar_text(s, *len, false).map_err(|err| {
                        RowCodecError::StringDataRightTruncation {
                            column: col_idx,
                            ty: field.data_type.clone(),
                            detail: err.to_string(),
                        }
                    })?;
                    encode_varlena_text(&mut payload, &coerced, col_idx, &field.data_type)?;
                }
                (DataType::Json, Value::Json(s)) => {
                    validate_json_storage_text(s, col_idx, &field.data_type)?;
                    encode_varlena_text(&mut payload, s, col_idx, &field.data_type)?;
                }
                (DataType::Jsonb, Value::Jsonb(s)) => {
                    let canonical = normalize_jsonb_storage_text(s, col_idx, &field.data_type)?;
                    encode_varlena_text(&mut payload, &canonical, col_idx, &field.data_type)?;
                }
                (DataType::Xml, Value::Xml(s)) => {
                    validate_xml_storage_text(s, col_idx, &field.data_type)?;
                    encode_varlena_text(&mut payload, s, col_idx, &field.data_type)?;
                }
                (DataType::Vector { dims }, Value::Vector(values)) => {
                    encode_vector_payload(&mut payload, values, *dims, col_idx, &field.data_type)?;
                }
                (DataType::HalfVec { dims }, Value::HalfVec(values)) => {
                    encode_dense_vector_family_text(
                        &mut payload,
                        value,
                        values,
                        *dims,
                        col_idx,
                        &field.data_type,
                    )?;
                }
                (DataType::SparseVec { dims }, Value::SparseVec(values)) => {
                    encode_dimensioned_value_text(
                        &mut payload,
                        value,
                        values.dims,
                        *dims,
                        col_idx,
                        &field.data_type,
                    )?;
                }
                (
                    DataType::BitVec { dims },
                    Value::BitVec {
                        dims: actual_dims, ..
                    },
                ) => {
                    encode_dimensioned_value_text(
                        &mut payload,
                        value,
                        *actual_dims,
                        *dims,
                        col_idx,
                        &field.data_type,
                    )?;
                }
                (DataType::Array(expected), Value::Array { element_type, .. })
                    if expected.as_ref() == element_type =>
                {
                    encode_varlena_text(
                        &mut payload,
                        &value.to_string(),
                        col_idx,
                        &field.data_type,
                    )?;
                }
                (DataType::Range(expected), Value::Range(v)) if expected == &v.range_type => {
                    encode_varlena_text(&mut payload, &v.to_string(), col_idx, &field.data_type)?;
                }
                (DataType::Geometry(expected), Value::Geometry(v))
                    if expected == &v.geometry_type =>
                {
                    encode_varlena_text(&mut payload, &v.to_string(), col_idx, &field.data_type)?;
                }
                (DataType::Null, _) => {
                    return Err(RowCodecError::Type {
                        column: col_idx,
                        expected: field.data_type.clone(),
                        got: value.data_type().to_string(),
                    });
                }
                (expected, got) => {
                    if !is_supported_type(expected) {
                        return Err(RowCodecError::UnsupportedType {
                            column: col_idx,
                            ty: expected.clone(),
                        });
                    }
                    return Err(RowCodecError::Type {
                        column: col_idx,
                        expected: expected.clone(),
                        got: got.data_type().to_string(),
                    });
                }
            }
        }
        let mut out = Vec::with_capacity(bitmap.len() + payload.len());
        out.extend_from_slice(&bitmap);
        out.extend_from_slice(&payload);
        Ok(out)
    }

    /// Decode a byte payload previously produced by [`Self::encode`].
    ///
    /// # Errors
    ///
    /// - [`RowCodecError::Truncated`] — buffer too short.
    /// - [`RowCodecError::UnsupportedType`] — unsupported `DataType`.
    /// - [`RowCodecError::InvalidUtf8`] — invalid UTF-8 in a Text.
    #[allow(clippy::too_many_lines)]
    pub fn decode(&self, bytes: &[u8]) -> Result<Vec<Value>, RowCodecError> {
        let n = self.schema.len();
        let bitmap_bytes = n.div_ceil(8);
        if bytes.len() < bitmap_bytes {
            return Err(RowCodecError::Truncated {
                needed: bitmap_bytes,
                have: bytes.len(),
            });
        }
        let bitmap = &bytes[..bitmap_bytes];
        let mut cursor = bitmap_bytes;
        let mut row: Vec<Value> = Vec::with_capacity(n);

        for (col_idx, field) in self.schema.fields().iter().enumerate() {
            let null_bit = (bitmap[col_idx / 8] >> (col_idx % 8)) & 1;
            if null_bit != 0 {
                row.push(Value::Null);
                continue;
            }
            let storage_type = field.data_type.storage_type();
            let value = match storage_type {
                DataType::Bool => {
                    let needed = cursor + 1;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let v = bytes[cursor] != 0;
                    cursor += 1;
                    Value::Bool(v)
                }
                DataType::Int16 => {
                    let needed = cursor + 2;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let raw: [u8; 2] = bytes[cursor..cursor + 2].try_into().map_err(|_| {
                        RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        }
                    })?;
                    cursor += 2;
                    Value::Int16(i16::from_le_bytes(raw))
                }
                DataType::Int32 => {
                    let needed = cursor + 4;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let raw: [u8; 4] = bytes[cursor..cursor + 4].try_into().map_err(|_| {
                        RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        }
                    })?;
                    cursor += 4;
                    Value::Int32(i32::from_le_bytes(raw))
                }
                DataType::Int64 => {
                    let needed = cursor + 8;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let raw: [u8; 8] = bytes[cursor..cursor + 8].try_into().map_err(|_| {
                        RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        }
                    })?;
                    cursor += 8;
                    Value::Int64(i64::from_le_bytes(raw))
                }
                DataType::Money => {
                    let needed = cursor + 8;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let raw: [u8; 8] = bytes[cursor..cursor + 8].try_into().map_err(|_| {
                        RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        }
                    })?;
                    cursor += 8;
                    Value::Money(i64::from_le_bytes(raw))
                }
                DataType::Oid | DataType::RegClass | DataType::RegType => {
                    let needed = cursor + 4;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let raw: [u8; 4] = bytes[cursor..cursor + 4].try_into().map_err(|_| {
                        RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        }
                    })?;
                    cursor += 4;
                    let oid = Oid::new(u32::from_le_bytes(raw));
                    match storage_type {
                        DataType::Oid => Value::Oid(oid),
                        DataType::RegClass => Value::RegClass(oid),
                        DataType::RegType => Value::RegType(oid),
                        _ => unreachable!(),
                    }
                }
                DataType::PgLsn => {
                    let needed = cursor + 8;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let raw: [u8; 8] = bytes[cursor..cursor + 8].try_into().map_err(|_| {
                        RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        }
                    })?;
                    cursor += 8;
                    Value::PgLsn(Lsn::new(u64::from_le_bytes(raw)))
                }
                DataType::Float32 => {
                    let needed = cursor + 4;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let raw: [u8; 4] = bytes[cursor..cursor + 4].try_into().map_err(|_| {
                        RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        }
                    })?;
                    cursor += 4;
                    Value::Float32(f32::from_le_bytes(raw))
                }
                DataType::Float64 => {
                    let needed = cursor + 8;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let raw: [u8; 8] = bytes[cursor..cursor + 8].try_into().map_err(|_| {
                        RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        }
                    })?;
                    cursor += 8;
                    Value::Float64(f64::from_le_bytes(raw))
                }
                DataType::Date => {
                    // `Date` storage: 4-byte little-endian i32 days
                    // since 2000-01-01 (same wire shape as Int32).
                    let needed = cursor + 4;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let raw: [u8; 4] = bytes[cursor..cursor + 4].try_into().map_err(|_| {
                        RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        }
                    })?;
                    cursor += 4;
                    Value::Date(i32::from_le_bytes(raw))
                }
                DataType::Decimal { .. } => {
                    decode_numeric_value(bytes, &mut cursor, col_idx, &field.data_type)?
                }
                DataType::Timestamp | DataType::TimestampTz | DataType::Time | DataType::TimeTz => {
                    // Microsecond temporal: 8-byte little-endian i64.
                    let needed = cursor + 8;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let raw: [u8; 8] = bytes[cursor..cursor + 8].try_into().map_err(|_| {
                        RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        }
                    })?;
                    cursor += 8;
                    let v = i64::from_le_bytes(raw);
                    match storage_type {
                        DataType::Timestamp => Value::Timestamp(v),
                        DataType::TimestampTz => Value::TimestampTz(v),
                        DataType::Time => Value::Time(v),
                        DataType::TimeTz => {
                            let (micros, offset_seconds) =
                                unpack_timetz(v).ok_or_else(|| RowCodecError::Type {
                                    column: col_idx,
                                    expected: field.data_type.clone(),
                                    got: "invalid timetz payload".to_owned(),
                                })?;
                            Value::TimeTz {
                                micros,
                                offset_seconds,
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                DataType::Interval => {
                    let needed = cursor + 16;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let micros_raw: [u8; 8] =
                        bytes[cursor..cursor + 8].try_into().map_err(|_| {
                            RowCodecError::Truncated {
                                needed,
                                have: bytes.len(),
                            }
                        })?;
                    let days_raw: [u8; 4] =
                        bytes[cursor + 8..cursor + 12].try_into().map_err(|_| {
                            RowCodecError::Truncated {
                                needed,
                                have: bytes.len(),
                            }
                        })?;
                    let months_raw: [u8; 4] =
                        bytes[cursor + 12..cursor + 16].try_into().map_err(|_| {
                            RowCodecError::Truncated {
                                needed,
                                have: bytes.len(),
                            }
                        })?;
                    cursor = needed;
                    Value::Interval {
                        months: i32::from_le_bytes(months_raw),
                        days: i32::from_le_bytes(days_raw),
                        microseconds: i64::from_le_bytes(micros_raw),
                    }
                }
                DataType::Uuid => {
                    let needed = cursor + 16;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let raw: [u8; 16] =
                        bytes[cursor..needed]
                            .try_into()
                            .map_err(|_| RowCodecError::Truncated {
                                needed,
                                have: bytes.len(),
                            })?;
                    cursor = needed;
                    Value::Uuid(raw)
                }
                DataType::Text { .. }
                | DataType::Enum { .. }
                | DataType::Composite { .. }
                | DataType::Char { .. }
                | DataType::Bit { .. }
                | DataType::VarBit { .. }
                | DataType::Inet
                | DataType::Cidr
                | DataType::MacAddr
                | DataType::MacAddr8 => {
                    let len_end = cursor + 4;
                    if bytes.len() < len_end {
                        return Err(RowCodecError::Truncated {
                            needed: len_end,
                            have: bytes.len(),
                        });
                    }
                    let len_raw: [u8; 4] = bytes[cursor..cursor + 4].try_into().map_err(|_| {
                        RowCodecError::Truncated {
                            needed: len_end,
                            have: bytes.len(),
                        }
                    })?;
                    let str_len = u32_payload_len_to_usize(u32::from_le_bytes(len_raw))?;
                    cursor += 4;
                    let str_end = checked_payload_end(cursor, str_len, bytes.len())?;
                    if bytes.len() < str_end {
                        return Err(RowCodecError::Truncated {
                            needed: str_end,
                            have: bytes.len(),
                        });
                    }
                    let s = String::from_utf8(bytes[cursor..str_end].to_vec())
                        .map_err(|e| RowCodecError::InvalidUtf8(e, "text column"))?;
                    cursor += str_len;
                    match storage_type {
                        DataType::Char { .. } => Value::Char(s),
                        DataType::Bit { .. } | DataType::VarBit { .. } => {
                            decode_bit_string_value(&s, &field.data_type, col_idx)?
                        }
                        DataType::Inet
                        | DataType::Cidr
                        | DataType::MacAddr
                        | DataType::MacAddr8 => {
                            decode_network_value(&s, &field.data_type, col_idx)?
                        }
                        _ => Value::Text(s),
                    }
                }
                DataType::Bytea => {
                    let len_end = cursor + 4;
                    if bytes.len() < len_end {
                        return Err(RowCodecError::Truncated {
                            needed: len_end,
                            have: bytes.len(),
                        });
                    }
                    let len_raw: [u8; 4] = bytes[cursor..len_end].try_into().map_err(|_| {
                        RowCodecError::Truncated {
                            needed: len_end,
                            have: bytes.len(),
                        }
                    })?;
                    let byte_len = u32_payload_len_to_usize(u32::from_le_bytes(len_raw))?;
                    cursor = len_end;
                    let byte_end = checked_payload_end(cursor, byte_len, bytes.len())?;
                    if bytes.len() < byte_end {
                        return Err(RowCodecError::Truncated {
                            needed: byte_end,
                            have: bytes.len(),
                        });
                    }
                    let value = bytes[cursor..byte_end].to_vec();
                    cursor = byte_end;
                    Value::Bytea(value)
                }
                DataType::Range(range_type) => {
                    let s = decode_varlena_text(bytes, &mut cursor, "range column")?;
                    Value::Range(RangeValue::parse(*range_type, &s).ok_or_else(|| {
                        RowCodecError::Type {
                            column: col_idx,
                            expected: field.data_type.clone(),
                            got: "invalid range literal".to_owned(),
                        }
                    })?)
                }
                DataType::Json => {
                    let s = decode_varlena_text(bytes, &mut cursor, "json column")?;
                    Value::Json(s)
                }
                DataType::Jsonb => {
                    let s = decode_varlena_text(bytes, &mut cursor, "jsonb column")?;
                    Value::Jsonb(s)
                }
                DataType::Xml => {
                    let s = decode_varlena_text(bytes, &mut cursor, "xml column")?;
                    Value::Xml(s)
                }
                DataType::Vector { dims } => {
                    decode_vector_value(bytes, &mut cursor, *dims, col_idx, &field.data_type)?
                }
                DataType::HalfVec { dims } => {
                    let s = decode_varlena_text(bytes, &mut cursor, "halfvec column")?;
                    decode_text_vector_family_value(
                        Value::parse_halfvec(&s),
                        *dims,
                        col_idx,
                        &field.data_type,
                    )?
                }
                DataType::SparseVec { dims } => {
                    let s = decode_varlena_text(bytes, &mut cursor, "sparsevec column")?;
                    decode_text_vector_family_value(
                        Value::parse_sparsevec(&s),
                        *dims,
                        col_idx,
                        &field.data_type,
                    )?
                }
                DataType::BitVec { dims } => {
                    let s = decode_varlena_text(bytes, &mut cursor, "bitvec column")?;
                    decode_text_vector_family_value(
                        Value::parse_bitvec(&s),
                        *dims,
                        col_idx,
                        &field.data_type,
                    )?
                }
                DataType::Array(element_type) => {
                    let s = decode_varlena_text(bytes, &mut cursor, "array column")?;
                    Value::parse_array((**element_type).clone(), &s).ok_or_else(|| {
                        RowCodecError::Type {
                            column: col_idx,
                            expected: field.data_type.clone(),
                            got: "invalid array literal".to_owned(),
                        }
                    })?
                }
                DataType::Geometry(geometry_type) => {
                    let s = decode_varlena_text(bytes, &mut cursor, "geometry column")?;
                    Value::Geometry(GeometryValue::parse(*geometry_type, &s).ok_or_else(|| {
                        RowCodecError::Type {
                            column: col_idx,
                            expected: field.data_type.clone(),
                            got: "invalid geometry literal".to_owned(),
                        }
                    })?)
                }
                DataType::Null => {
                    return Err(RowCodecError::UnsupportedType {
                        column: col_idx,
                        ty: DataType::Null,
                    });
                }
                other => {
                    return Err(RowCodecError::UnsupportedType {
                        column: col_idx,
                        ty: other.clone(),
                    });
                }
            };
            row.push(value);
        }
        Ok(row)
    }

    /// Decode only `projection` columns from a stored row payload.
    ///
    /// The row layout is still row-oriented, so the decoder must scan
    /// earlier columns to advance offsets. It skips unprojected values
    /// without constructing [`Value`] objects for them, which is the
    /// payload phase late materialization needs for wide rows.
    pub fn decode_projected(
        &self,
        bytes: &[u8],
        projection: &[usize],
    ) -> Result<Vec<Value>, RowCodecError> {
        let n = self.schema.len();
        let bitmap_bytes = n.div_ceil(8);
        if bytes.len() < bitmap_bytes {
            return Err(RowCodecError::Truncated {
                needed: bitmap_bytes,
                have: bytes.len(),
            });
        }
        let mut targets = vec![Vec::new(); n];
        for (out_idx, &col_idx) in projection.iter().enumerate() {
            if col_idx >= n {
                return Err(RowCodecError::Arity {
                    schema: n,
                    row: col_idx.saturating_add(1),
                });
            }
            targets[col_idx].push(out_idx);
        }

        let bitmap = &bytes[..bitmap_bytes];
        let mut cursor = bitmap_bytes;
        let mut projected = vec![Value::Null; projection.len()];

        for (col_idx, field) in self.schema.fields().iter().enumerate() {
            let null_bit = (bitmap[col_idx / 8] >> (col_idx % 8)) & 1;
            if null_bit != 0 {
                continue;
            }
            if targets[col_idx].is_empty() {
                Self::skip_one_value(bytes, &mut cursor, col_idx, field.data_type.storage_type())?;
                continue;
            }
            let value = Self::decode_one_value(
                bytes,
                &mut cursor,
                col_idx,
                field.data_type.storage_type(),
            )?;
            for &out_idx in &targets[col_idx] {
                projected[out_idx] = value.clone();
            }
        }

        Ok(projected)
    }

    fn decode_one_value(
        bytes: &[u8],
        cursor: &mut usize,
        col_idx: usize,
        data_type: &DataType,
    ) -> Result<Value, RowCodecError> {
        match data_type {
            DataType::Bool => {
                let needed = cursor.saturating_add(1);
                if bytes.len() < needed {
                    return Err(RowCodecError::Truncated {
                        needed,
                        have: bytes.len(),
                    });
                }
                let value = Value::Bool(bytes[*cursor] != 0);
                *cursor = needed;
                Ok(value)
            }
            DataType::Int16 => {
                let raw = read_fixed::<2>(bytes, cursor)?;
                Ok(Value::Int16(i16::from_le_bytes(raw)))
            }
            DataType::Int32 => {
                let raw = read_fixed::<4>(bytes, cursor)?;
                Ok(Value::Int32(i32::from_le_bytes(raw)))
            }
            DataType::Int64 => {
                let raw = read_fixed::<8>(bytes, cursor)?;
                Ok(Value::Int64(i64::from_le_bytes(raw)))
            }
            DataType::Money => {
                let raw = read_fixed::<8>(bytes, cursor)?;
                Ok(Value::Money(i64::from_le_bytes(raw)))
            }
            DataType::Oid | DataType::RegClass | DataType::RegType => {
                let raw = read_fixed::<4>(bytes, cursor)?;
                let oid = Oid::new(u32::from_le_bytes(raw));
                match data_type {
                    DataType::Oid => Ok(Value::Oid(oid)),
                    DataType::RegClass => Ok(Value::RegClass(oid)),
                    DataType::RegType => Ok(Value::RegType(oid)),
                    _ => unreachable!(),
                }
            }
            DataType::PgLsn => {
                let raw = read_fixed::<8>(bytes, cursor)?;
                Ok(Value::PgLsn(Lsn::new(u64::from_le_bytes(raw))))
            }
            DataType::Float32 => {
                let raw = read_fixed::<4>(bytes, cursor)?;
                Ok(Value::Float32(f32::from_le_bytes(raw)))
            }
            DataType::Float64 => {
                let raw = read_fixed::<8>(bytes, cursor)?;
                Ok(Value::Float64(f64::from_le_bytes(raw)))
            }
            DataType::Date => {
                let raw = read_fixed::<4>(bytes, cursor)?;
                Ok(Value::Date(i32::from_le_bytes(raw)))
            }
            DataType::Decimal { .. } => decode_numeric_value(bytes, cursor, col_idx, data_type),
            DataType::Timestamp | DataType::TimestampTz | DataType::Time | DataType::TimeTz => {
                let raw = read_fixed::<8>(bytes, cursor)?;
                let value = i64::from_le_bytes(raw);
                match data_type {
                    DataType::Timestamp => Ok(Value::Timestamp(value)),
                    DataType::TimestampTz => Ok(Value::TimestampTz(value)),
                    DataType::Time => Ok(Value::Time(value)),
                    DataType::TimeTz => {
                        let (micros, offset_seconds) =
                            unpack_timetz(value).ok_or_else(|| RowCodecError::Type {
                                column: col_idx,
                                expected: data_type.clone(),
                                got: "invalid timetz payload".to_owned(),
                            })?;
                        Ok(Value::TimeTz {
                            micros,
                            offset_seconds,
                        })
                    }
                    _ => unreachable!(),
                }
            }
            DataType::Interval => {
                let micros = i64::from_le_bytes(read_fixed::<8>(bytes, cursor)?);
                let days = i32::from_le_bytes(read_fixed::<4>(bytes, cursor)?);
                let months = i32::from_le_bytes(read_fixed::<4>(bytes, cursor)?);
                Ok(Value::Interval {
                    months,
                    days,
                    microseconds: micros,
                })
            }
            DataType::Uuid => {
                let raw = read_fixed::<16>(bytes, cursor)?;
                Ok(Value::Uuid(raw))
            }
            DataType::Bytea => Ok(Value::Bytea(decode_varlena_bytes(bytes, cursor)?)),
            DataType::Text { .. } => Ok(Value::Text(decode_varlena_text(
                bytes,
                cursor,
                "text column",
            )?)),
            DataType::Enum { .. } => Ok(Value::Text(decode_varlena_text(
                bytes,
                cursor,
                "enum column",
            )?)),
            DataType::Composite { .. } => Ok(Value::Text(decode_varlena_text(
                bytes,
                cursor,
                "composite column",
            )?)),
            DataType::Char { .. } => Ok(Value::Char(decode_varlena_text(
                bytes,
                cursor,
                "bpchar column",
            )?)),
            DataType::Bit { .. } | DataType::VarBit { .. } => {
                let s = decode_varlena_text(bytes, cursor, "bit string column")?;
                decode_bit_string_value(&s, data_type, col_idx)
            }
            DataType::Inet | DataType::Cidr | DataType::MacAddr | DataType::MacAddr8 => {
                let s = decode_varlena_text(bytes, cursor, "network column")?;
                decode_network_value(&s, data_type, col_idx)
            }
            DataType::Range(range_type) => {
                let s = decode_varlena_text(bytes, cursor, "range column")?;
                Ok(Value::Range(
                    RangeValue::parse(*range_type, &s).ok_or_else(|| RowCodecError::Type {
                        column: col_idx,
                        expected: data_type.clone(),
                        got: "invalid range literal".to_owned(),
                    })?,
                ))
            }
            DataType::Json => Ok(Value::Json(decode_varlena_text(
                bytes,
                cursor,
                "json column",
            )?)),
            DataType::Jsonb => Ok(Value::Jsonb(decode_varlena_text(
                bytes,
                cursor,
                "jsonb column",
            )?)),
            DataType::Xml => Ok(Value::Xml(decode_varlena_text(
                bytes,
                cursor,
                "xml column",
            )?)),
            DataType::Vector { dims } => {
                decode_vector_value(bytes, cursor, *dims, col_idx, data_type)
            }
            DataType::HalfVec { dims } => {
                let s = decode_varlena_text(bytes, cursor, "halfvec column")?;
                decode_text_vector_family_value(Value::parse_halfvec(&s), *dims, col_idx, data_type)
            }
            DataType::SparseVec { dims } => {
                let s = decode_varlena_text(bytes, cursor, "sparsevec column")?;
                decode_text_vector_family_value(
                    Value::parse_sparsevec(&s),
                    *dims,
                    col_idx,
                    data_type,
                )
            }
            DataType::BitVec { dims } => {
                let s = decode_varlena_text(bytes, cursor, "bitvec column")?;
                decode_text_vector_family_value(Value::parse_bitvec(&s), *dims, col_idx, data_type)
            }
            DataType::Array(element_type) => {
                let s = decode_varlena_text(bytes, cursor, "array column")?;
                Value::parse_array((**element_type).clone(), &s).ok_or_else(|| {
                    RowCodecError::Type {
                        column: col_idx,
                        expected: data_type.clone(),
                        got: "invalid array literal".to_owned(),
                    }
                })
            }
            DataType::Geometry(geometry_type) => {
                let s = decode_varlena_text(bytes, cursor, "geometry column")?;
                Ok(Value::Geometry(
                    GeometryValue::parse(*geometry_type, &s).ok_or_else(|| {
                        RowCodecError::Type {
                            column: col_idx,
                            expected: data_type.clone(),
                            got: "invalid geometry literal".to_owned(),
                        }
                    })?,
                ))
            }
            DataType::Null => Err(RowCodecError::UnsupportedType {
                column: col_idx,
                ty: DataType::Null,
            }),
            other => Err(RowCodecError::UnsupportedType {
                column: col_idx,
                ty: other.clone(),
            }),
        }
    }

    fn skip_one_value(
        bytes: &[u8],
        cursor: &mut usize,
        col_idx: usize,
        data_type: &DataType,
    ) -> Result<(), RowCodecError> {
        match data_type {
            DataType::Bool => skip_fixed(bytes, cursor, 1),
            DataType::Int16 => skip_fixed(bytes, cursor, 2),
            DataType::Int32
            | DataType::Float32
            | DataType::Date
            | DataType::Oid
            | DataType::RegClass
            | DataType::RegType => skip_fixed(bytes, cursor, 4),
            DataType::Int64
            | DataType::Money
            | DataType::Float64
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::Time
            | DataType::TimeTz
            | DataType::PgLsn => skip_fixed(bytes, cursor, 8),
            DataType::Interval => skip_fixed(bytes, cursor, 16),
            DataType::Decimal { .. } => skip_varlena_payload(bytes, cursor),
            DataType::Uuid => skip_fixed(bytes, cursor, 16),
            DataType::Bytea
            | DataType::Text { .. }
            | DataType::Enum { .. }
            | DataType::Composite { .. }
            | DataType::Char { .. }
            | DataType::Bit { .. }
            | DataType::VarBit { .. }
            | DataType::Json
            | DataType::Inet
            | DataType::Cidr
            | DataType::MacAddr
            | DataType::MacAddr8
            | DataType::Range(_)
            | DataType::Jsonb
            | DataType::Xml
            | DataType::HalfVec { .. }
            | DataType::SparseVec { .. }
            | DataType::BitVec { .. }
            | DataType::Array(_)
            | DataType::Geometry(_) => skip_varlena_payload(bytes, cursor),
            DataType::Vector { dims } => skip_vector_value(bytes, cursor, *dims, col_idx),
            DataType::Null => Err(RowCodecError::UnsupportedType {
                column: col_idx,
                ty: DataType::Null,
            }),
            other => Err(RowCodecError::UnsupportedType {
                column: col_idx,
                ty: other.clone(),
            }),
        }
    }

    /// Initialise a `Vec<ColumnBuilder>` matching this codec's schema.
    ///
    /// # Errors
    ///
    /// [`RowCodecError::UnsupportedType`] for unsupported types.
    pub(crate) fn new_builders(
        &self,
        capacity: usize,
    ) -> Result<Vec<ColumnBuilder>, RowCodecError> {
        let mut out: Vec<ColumnBuilder> = Vec::with_capacity(self.schema.len());
        for (idx, field) in self.schema.fields().iter().enumerate() {
            out.push(ColumnBuilder::new(
                field.data_type.storage_type(),
                capacity,
                idx,
            )?);
        }
        Ok(out)
    }

    /// Decode one tuple's `bytes` directly into `builders`.
    ///
    /// # Errors
    ///
    /// Same shape as [`Self::decode`].
    ///
    /// # Panics
    ///
    /// Panics if `builders.len() != self.schema.len()`.
    #[allow(clippy::too_many_lines)]
    pub(crate) fn decode_into_builders(
        &self,
        bytes: &[u8],
        builders: &mut [ColumnBuilder],
    ) -> Result<(), RowCodecError> {
        let n = self.schema.len();
        assert_eq!(builders.len(), n, "builders.len() must equal schema.len()");

        // Fast-path dispatch: the all-non-null branch of the most
        // common all-fixed-width schemas skips the per-column match
        // dispatch and `try_into` round-trip entirely. If the null
        // bitmap byte is non-zero (any NULL present) we fall through
        // to the generic path which handles bit-by-bit nulls.
        if Self::try_decode_fast_path(self.decode_shape, bytes, builders)? == Some(()) {
            return Ok(());
        }

        let bitmap_bytes = n.div_ceil(8);
        if bytes.len() < bitmap_bytes {
            return Err(RowCodecError::Truncated {
                needed: bitmap_bytes,
                have: bytes.len(),
            });
        }
        let bitmap = &bytes[..bitmap_bytes];
        let mut cursor = bitmap_bytes;

        for (col_idx, field) in self.schema.fields().iter().enumerate() {
            let null_bit = (bitmap[col_idx / 8] >> (col_idx % 8)) & 1;
            if null_bit != 0 {
                builders[col_idx].push_null();
                continue;
            }
            match (field.data_type.storage_type(), &mut builders[col_idx]) {
                (DataType::Bool, ColumnBuilder::Bool { data, nulls }) => {
                    let needed = cursor + 1;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    data.push(u8::from(bytes[cursor] != 0));
                    cursor += 1;
                    nulls.push_valid();
                }
                (DataType::Int16, ColumnBuilder::Int16 { data, nulls }) => {
                    let needed = cursor + 2;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let raw: [u8; 2] = bytes[cursor..cursor + 2].try_into().map_err(|_| {
                        RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        }
                    })?;
                    cursor += 2;
                    data.push(i32::from(i16::from_le_bytes(raw)));
                    nulls.push_valid();
                }
                (DataType::Int32, ColumnBuilder::Int32 { data, nulls }) => {
                    let needed = cursor + 4;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let raw: [u8; 4] = bytes[cursor..cursor + 4].try_into().map_err(|_| {
                        RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        }
                    })?;
                    cursor += 4;
                    data.push(i32::from_le_bytes(raw));
                    nulls.push_valid();
                }
                (DataType::Int64, ColumnBuilder::Int64 { data, nulls }) => {
                    let needed = cursor + 8;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let raw: [u8; 8] = bytes[cursor..cursor + 8].try_into().map_err(|_| {
                        RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        }
                    })?;
                    cursor += 8;
                    data.push(i64::from_le_bytes(raw));
                    nulls.push_valid();
                }
                (DataType::Money, ColumnBuilder::Int64 { data, nulls }) => {
                    let needed = cursor + 8;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let raw: [u8; 8] = bytes[cursor..cursor + 8].try_into().map_err(|_| {
                        RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        }
                    })?;
                    cursor += 8;
                    data.push(i64::from_le_bytes(raw));
                    nulls.push_valid();
                }
                (
                    DataType::Oid | DataType::RegClass | DataType::RegType,
                    ColumnBuilder::Int64 { data, nulls },
                ) => {
                    let needed = cursor + 4;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let raw: [u8; 4] = bytes[cursor..cursor + 4].try_into().map_err(|_| {
                        RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        }
                    })?;
                    cursor += 4;
                    data.push(i64::from(u32::from_le_bytes(raw)));
                    nulls.push_valid();
                }
                (
                    DataType::PgLsn,
                    ColumnBuilder::Utf8 {
                        offsets,
                        values,
                        nulls,
                    },
                ) => {
                    let needed = cursor + 8;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let raw: [u8; 8] = bytes[cursor..cursor + 8].try_into().map_err(|_| {
                        RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        }
                    })?;
                    cursor += 8;
                    let text = Value::PgLsn(Lsn::new(u64::from_le_bytes(raw))).to_string();
                    values.extend_from_slice(text.as_bytes());
                    let new_end = u32::try_from(values.len()).map_err(|_| {
                        RowCodecError::UnsupportedType {
                            column: col_idx,
                            ty: field.data_type.clone(),
                        }
                    })?;
                    offsets.push(new_end);
                    nulls.push_valid();
                }
                (DataType::Float32, ColumnBuilder::Float32 { data, nulls }) => {
                    let needed = cursor + 4;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let raw: [u8; 4] = bytes[cursor..cursor + 4].try_into().map_err(|_| {
                        RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        }
                    })?;
                    cursor += 4;
                    data.push(f32::from_le_bytes(raw));
                    nulls.push_valid();
                }
                (DataType::Float64, ColumnBuilder::Float64 { data, nulls }) => {
                    let needed = cursor + 8;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let raw: [u8; 8] = bytes[cursor..cursor + 8].try_into().map_err(|_| {
                        RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        }
                    })?;
                    cursor += 8;
                    data.push(f64::from_le_bytes(raw));
                    nulls.push_valid();
                }
                (DataType::Date, ColumnBuilder::Int32 { data, nulls }) => {
                    // Date values share the Int32 builder; the column
                    // is reported as Int32-typed to downstream batches
                    // and the schema carries the date semantics.
                    let needed = cursor + 4;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let raw: [u8; 4] = bytes[cursor..cursor + 4].try_into().map_err(|_| {
                        RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        }
                    })?;
                    cursor += 4;
                    data.push(i32::from_le_bytes(raw));
                    nulls.push_valid();
                }
                (DataType::Decimal { .. }, ColumnBuilder::Int64 { data, nulls }) => {
                    let value =
                        decode_numeric_scaled_i64(bytes, &mut cursor, col_idx, &field.data_type)?;
                    data.push(value);
                    nulls.push_valid();
                }
                (DataType::Timestamp, ColumnBuilder::Int64 { data, nulls })
                | (DataType::TimestampTz, ColumnBuilder::Int64 { data, nulls })
                | (DataType::Time, ColumnBuilder::Int64 { data, nulls })
                | (DataType::TimeTz, ColumnBuilder::Int64 { data, nulls }) => {
                    let needed = cursor + 8;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let raw: [u8; 8] = bytes[cursor..cursor + 8].try_into().map_err(|_| {
                        RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        }
                    })?;
                    cursor += 8;
                    data.push(i64::from_le_bytes(raw));
                    nulls.push_valid();
                }
                (
                    DataType::Interval,
                    ColumnBuilder::Utf8 {
                        offsets,
                        values,
                        nulls,
                    },
                ) => {
                    let needed = cursor + 16;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let micros_raw: [u8; 8] =
                        bytes[cursor..cursor + 8].try_into().map_err(|_| {
                            RowCodecError::Truncated {
                                needed,
                                have: bytes.len(),
                            }
                        })?;
                    let days_raw: [u8; 4] =
                        bytes[cursor + 8..cursor + 12].try_into().map_err(|_| {
                            RowCodecError::Truncated {
                                needed,
                                have: bytes.len(),
                            }
                        })?;
                    let months_raw: [u8; 4] =
                        bytes[cursor + 12..cursor + 16].try_into().map_err(|_| {
                            RowCodecError::Truncated {
                                needed,
                                have: bytes.len(),
                            }
                        })?;
                    cursor = needed;
                    let text = Value::Interval {
                        months: i32::from_le_bytes(months_raw),
                        days: i32::from_le_bytes(days_raw),
                        microseconds: i64::from_le_bytes(micros_raw),
                    }
                    .to_string();
                    values.extend_from_slice(text.as_bytes());
                    let new_end = u32::try_from(values.len()).map_err(|_| {
                        RowCodecError::UnsupportedType {
                            column: col_idx,
                            ty: field.data_type.clone(),
                        }
                    })?;
                    offsets.push(new_end);
                    nulls.push_valid();
                }
                (
                    DataType::Uuid,
                    ColumnBuilder::Utf8 {
                        offsets,
                        values,
                        nulls,
                    },
                ) => {
                    let needed = cursor + 16;
                    if bytes.len() < needed {
                        return Err(RowCodecError::Truncated {
                            needed,
                            have: bytes.len(),
                        });
                    }
                    let raw: [u8; 16] =
                        bytes[cursor..needed]
                            .try_into()
                            .map_err(|_| RowCodecError::Truncated {
                                needed,
                                have: bytes.len(),
                            })?;
                    cursor = needed;
                    let text = Value::Uuid(raw).to_string();
                    values.extend_from_slice(text.as_bytes());
                    let new_end = u32::try_from(values.len()).map_err(|_| {
                        RowCodecError::UnsupportedType {
                            column: col_idx,
                            ty: field.data_type.clone(),
                        }
                    })?;
                    offsets.push(new_end);
                    nulls.push_valid();
                }
                (
                    DataType::Bytea,
                    ColumnBuilder::Utf8 {
                        offsets,
                        values,
                        nulls,
                    },
                ) => {
                    let len_end = cursor + 4;
                    if bytes.len() < len_end {
                        return Err(RowCodecError::Truncated {
                            needed: len_end,
                            have: bytes.len(),
                        });
                    }
                    let len_raw: [u8; 4] = bytes[cursor..len_end].try_into().map_err(|_| {
                        RowCodecError::Truncated {
                            needed: len_end,
                            have: bytes.len(),
                        }
                    })?;
                    let byte_len = u32_payload_len_to_usize(u32::from_le_bytes(len_raw))?;
                    cursor = len_end;
                    let byte_end = checked_payload_end(cursor, byte_len, bytes.len())?;
                    if bytes.len() < byte_end {
                        return Err(RowCodecError::Truncated {
                            needed: byte_end,
                            have: bytes.len(),
                        });
                    }
                    let text = Value::Bytea(bytes[cursor..byte_end].to_vec()).to_string();
                    cursor = byte_end;
                    values.extend_from_slice(text.as_bytes());
                    let new_end = u32::try_from(values.len()).map_err(|_| {
                        RowCodecError::UnsupportedType {
                            column: col_idx,
                            ty: field.data_type.clone(),
                        }
                    })?;
                    offsets.push(new_end);
                    nulls.push_valid();
                }
                (
                    DataType::Vector { dims },
                    ColumnBuilder::Utf8 {
                        offsets,
                        values,
                        nulls,
                    },
                ) => {
                    let value =
                        decode_vector_value(bytes, &mut cursor, *dims, col_idx, &field.data_type)?;
                    let text = value.to_string();
                    values.extend_from_slice(text.as_bytes());
                    let new_end = u32::try_from(values.len()).map_err(|_| {
                        RowCodecError::UnsupportedType {
                            column: col_idx,
                            ty: field.data_type.clone(),
                        }
                    })?;
                    offsets.push(new_end);
                    nulls.push_valid();
                }
                (
                    DataType::Text { .. }
                    | DataType::Enum { .. }
                    | DataType::Composite { .. }
                    | DataType::Char { .. }
                    | DataType::Bit { .. }
                    | DataType::VarBit { .. }
                    | DataType::Inet
                    | DataType::Cidr
                    | DataType::MacAddr
                    | DataType::MacAddr8
                    | DataType::Json
                    | DataType::Jsonb
                    | DataType::Xml
                    | DataType::Range(_)
                    | DataType::Geometry(_)
                    | DataType::Array(_)
                    | DataType::HalfVec { .. }
                    | DataType::SparseVec { .. }
                    | DataType::BitVec { .. },
                    ColumnBuilder::Utf8 {
                        offsets,
                        values,
                        nulls,
                    },
                ) => {
                    let len_end = cursor + 4;
                    if bytes.len() < len_end {
                        return Err(RowCodecError::Truncated {
                            needed: len_end,
                            have: bytes.len(),
                        });
                    }
                    let len_raw: [u8; 4] = bytes[cursor..cursor + 4].try_into().map_err(|_| {
                        RowCodecError::Truncated {
                            needed: len_end,
                            have: bytes.len(),
                        }
                    })?;
                    let str_len = u32_payload_len_to_usize(u32::from_le_bytes(len_raw))?;
                    cursor += 4;
                    let str_end = checked_payload_end(cursor, str_len, bytes.len())?;
                    if bytes.len() < str_end {
                        return Err(RowCodecError::Truncated {
                            needed: str_end,
                            have: bytes.len(),
                        });
                    }
                    std::str::from_utf8(&bytes[cursor..str_end])
                        .map_err(|error| RowCodecError::InvalidUtf8Slice(error, "text column"))?;
                    values.extend_from_slice(&bytes[cursor..str_end]);
                    cursor += str_len;
                    let new_end = u32::try_from(values.len()).map_err(|_| {
                        RowCodecError::UnsupportedType {
                            column: col_idx,
                            ty: field.data_type.clone(),
                        }
                    })?;
                    offsets.push(new_end);
                    nulls.push_valid();
                }
                (DataType::Null, _) => {
                    return Err(RowCodecError::UnsupportedType {
                        column: col_idx,
                        ty: DataType::Null,
                    });
                }
                (other, _) => {
                    return Err(RowCodecError::UnsupportedType {
                        column: col_idx,
                        ty: other.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Finalise a row of builders into a `Batch`.
    ///
    /// # Errors
    ///
    /// [`ultrasql_vec::BatchError`] if builders disagree on length.
    pub(crate) fn finish_batch(
        builders: Vec<ColumnBuilder>,
    ) -> Result<Batch, ultrasql_vec::BatchError> {
        Batch::new(finish_builders(builders))
    }

    /// Inject an `Int32` into `builders[col_idx]`. Used to prepend TID
    /// columns in the scan operator.
    pub(crate) fn push_i32_into(builders: &mut [ColumnBuilder], col_idx: usize, v: i32) {
        builders[col_idx].push_i32(v);
    }
}

// ---------------------------------------------------------------------------
// Streaming column builders
// ---------------------------------------------------------------------------

/// Lazy packed null tracker that grows by one bit per row at O(1)
/// amortised cost. The [`Bitmap`] is materialised lazily on first
/// observed null and finalised at `finish` time.
#[derive(Debug, Default)]
#[allow(clippy::redundant_pub_crate)]
pub(crate) struct NullTracker {
    words: Vec<u64>,
    len: usize,
    active: bool,
}

impl NullTracker {
    /// Mark a previously-pushed row as valid.
    #[inline]
    pub(crate) fn push_valid(&mut self) {
        if !self.active {
            return;
        }
        let bit_idx = self.len;
        let word_idx = bit_idx / 64;
        if word_idx >= self.words.len() {
            self.words.push(0);
        }
        self.words[word_idx] |= 1_u64 << (bit_idx % 64);
        self.len += 1;
    }

    /// Mark a previously-pushed row as null, activating the tracker
    /// if necessary.
    #[inline]
    fn push_null(&mut self, prior_rows: usize) {
        if !self.active {
            self.activate(prior_rows);
        }
        let bit_idx = self.len;
        let word_idx = bit_idx / 64;
        if word_idx >= self.words.len() {
            self.words.push(0);
        }
        self.len += 1;
    }

    #[cold]
    fn activate(&mut self, prior_rows: usize) {
        debug_assert!(!self.active);
        self.active = true;
        let words = prior_rows.div_ceil(64);
        self.words = vec![u64::MAX; words];
        if prior_rows % 64 != 0 {
            let mask = (1_u64 << (prior_rows % 64)) - 1;
            if let Some(last) = self.words.last_mut() {
                *last &= mask;
            }
        }
        self.len = prior_rows;
    }

    fn finish(self) -> Option<Bitmap> {
        if self.active {
            Some(Bitmap::from_words(self.words, self.len))
        } else {
            None
        }
    }
}

/// Per-column accumulator owning a typed `Vec<T>` plus a null tracker.
#[derive(Debug)]
#[allow(clippy::redundant_pub_crate)]
pub(crate) enum ColumnBuilder {
    Bool {
        data: Vec<u8>,
        nulls: NullTracker,
    },
    Int16 {
        data: Vec<i32>,
        nulls: NullTracker,
    },
    Int32 {
        data: Vec<i32>,
        nulls: NullTracker,
    },
    Int64 {
        data: Vec<i64>,
        nulls: NullTracker,
    },
    Float32 {
        data: Vec<f32>,
        nulls: NullTracker,
    },
    Float64 {
        data: Vec<f64>,
        nulls: NullTracker,
    },
    Utf8 {
        offsets: Vec<u32>,
        values: Vec<u8>,
        nulls: NullTracker,
    },
}

impl ColumnBuilder {
    fn new(ty: &DataType, capacity: usize, col_idx: usize) -> Result<Self, RowCodecError> {
        Ok(match ty {
            DataType::Bool => Self::Bool {
                data: Vec::with_capacity(capacity),
                nulls: NullTracker::default(),
            },
            DataType::Int16 => Self::Int16 {
                data: Vec::with_capacity(capacity),
                nulls: NullTracker::default(),
            },
            DataType::Int32 => Self::Int32 {
                data: Vec::with_capacity(capacity),
                nulls: NullTracker::default(),
            },
            DataType::Int64 => Self::Int64 {
                data: Vec::with_capacity(capacity),
                nulls: NullTracker::default(),
            },
            DataType::Float32 => Self::Float32 {
                data: Vec::with_capacity(capacity),
                nulls: NullTracker::default(),
            },
            DataType::Float64 => Self::Float64 {
                data: Vec::with_capacity(capacity),
                nulls: NullTracker::default(),
            },
            DataType::Date => Self::Int32 {
                // `Date` storage shares the `Int32` builder: days
                // since 2000-01-01 are i32 by definition. Schema
                // tags carry the date semantics so downstream
                // operators that care about date comparisons (range
                // filters, sort) still see a `DataType::Date` field.
                data: Vec::with_capacity(capacity),
                nulls: NullTracker::default(),
            },
            DataType::Decimal { .. }
            | DataType::Money
            | DataType::Oid
            | DataType::RegClass
            | DataType::RegType
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::Time
            | DataType::TimeTz => Self::Int64 {
                // `Decimal` / `Timestamp` / `Time` storage shares the
                // `Int64` builder; the schema field carries the
                // semantic tag and (for Decimal) the scale.
                data: Vec::with_capacity(capacity),
                nulls: NullTracker::default(),
            },
            DataType::Text { .. }
            | DataType::Enum { .. }
            | DataType::Composite { .. }
            | DataType::Char { .. }
            | DataType::Bit { .. }
            | DataType::VarBit { .. }
            | DataType::Inet
            | DataType::Cidr
            | DataType::MacAddr
            | DataType::MacAddr8
            | DataType::Json
            | DataType::Jsonb
            | DataType::Xml
            | DataType::Vector { .. }
            | DataType::HalfVec { .. }
            | DataType::SparseVec { .. }
            | DataType::BitVec { .. }
            | DataType::Range(_)
            | DataType::Geometry(_)
            | DataType::Array(_)
            | DataType::Uuid
            | DataType::Bytea
            | DataType::Interval
            | DataType::PgLsn => Self::Utf8 {
                offsets: {
                    let mut o = Vec::with_capacity(capacity + 1);
                    o.push(0);
                    o
                },
                values: Vec::with_capacity(capacity.saturating_mul(16)),
                nulls: NullTracker::default(),
            },
            other => {
                return Err(RowCodecError::UnsupportedType {
                    column: col_idx,
                    ty: other.clone(),
                });
            }
        })
    }

    fn push_null(&mut self) {
        match self {
            Self::Bool { data, nulls } => {
                let prior = data.len();
                nulls.push_null(prior);
                data.push(0);
            }
            Self::Int16 { data, nulls } | Self::Int32 { data, nulls } => {
                let prior = data.len();
                nulls.push_null(prior);
                data.push(0);
            }
            Self::Int64 { data, nulls } => {
                let prior = data.len();
                nulls.push_null(prior);
                data.push(0);
            }
            Self::Float32 { data, nulls } => {
                let prior = data.len();
                nulls.push_null(prior);
                data.push(0.0);
            }
            Self::Float64 { data, nulls } => {
                let prior = data.len();
                nulls.push_null(prior);
                data.push(0.0);
            }
            Self::Utf8 {
                offsets,
                values,
                nulls,
            } => {
                let prior = offsets.len().saturating_sub(1);
                nulls.push_null(prior);
                offsets.push(u32::try_from(values.len()).unwrap_or(u32::MAX));
            }
        }
    }

    fn push_i32(&mut self, v: i32) {
        match self {
            Self::Int32 { data, nulls } | Self::Int16 { data, nulls } => {
                data.push(v);
                nulls.push_valid();
            }
            _ => unreachable!("push_i32 called on non-Int32/Int16 builder"),
        }
    }
}

fn finish_builders(builders: Vec<ColumnBuilder>) -> Vec<Column> {
    let mut out: Vec<Column> = Vec::with_capacity(builders.len());
    for b in builders {
        let col = match b {
            ColumnBuilder::Bool { data, nulls } => {
                let bools: Vec<bool> = data.iter().map(|&b| b != 0).collect();
                match nulls.finish() {
                    Some(bm) => Column::Bool(
                        BoolColumn::with_nulls(bools, bm).expect("builder length invariant"),
                    ),
                    None => Column::Bool(BoolColumn::from_data(bools)),
                }
            }
            ColumnBuilder::Int16 { data, nulls } | ColumnBuilder::Int32 { data, nulls } => {
                match nulls.finish() {
                    Some(bm) => Column::Int32(
                        NumericColumn::with_nulls(data, bm).expect("builder length invariant"),
                    ),
                    None => Column::Int32(NumericColumn::from_data(data)),
                }
            }
            ColumnBuilder::Int64 { data, nulls } => match nulls.finish() {
                Some(bm) => Column::Int64(
                    NumericColumn::with_nulls(data, bm).expect("builder length invariant"),
                ),
                None => Column::Int64(NumericColumn::from_data(data)),
            },
            ColumnBuilder::Float32 { data, nulls } => match nulls.finish() {
                Some(bm) => Column::Float32(
                    NumericColumn::with_nulls(data, bm).expect("builder length invariant"),
                ),
                None => Column::Float32(NumericColumn::from_data(data)),
            },
            ColumnBuilder::Float64 { data, nulls } => match nulls.finish() {
                Some(bm) => Column::Float64(
                    NumericColumn::with_nulls(data, bm).expect("builder length invariant"),
                ),
                None => Column::Float64(NumericColumn::from_data(data)),
            },
            ColumnBuilder::Utf8 {
                offsets,
                values,
                nulls,
            } => text_column_from_parts(&offsets, &values, nulls.finish()),
        };
        out.push(col);
    }
    out
}

fn text_column_from_parts(offsets: &[u32], values: &[u8], nulls: Option<Bitmap>) -> Column {
    let n = offsets.len().saturating_sub(1);
    let mut rows: Vec<Option<String>> = Vec::with_capacity(n);
    for i in 0..n {
        if nulls.as_ref().is_some_and(|bm| !bm.get(i)) {
            rows.push(None);
        } else {
            let start = offsets[i] as usize;
            let end = offsets[i + 1] as usize;
            let s = String::from_utf8(values[start..end].to_vec())
                .expect("StringColumn builder invariant: values are validated UTF-8");
            rows.push(Some(s));
        }
    }
    match encode_strings_auto(
        rows.iter().map(|v| v.as_deref()),
        DictionaryEncodingPolicy::default(),
    ) {
        StringEncoding::Raw(c) => Column::Utf8(c),
        StringEncoding::Dictionary(c) => Column::DictionaryUtf8(c),
    }
}

fn decode_bit_string_value(
    text: &str,
    expected_type: &DataType,
    col_idx: usize,
) -> Result<Value, RowCodecError> {
    let Some(Value::BitString(bits)) = Value::parse_bit_string(text) else {
        return Err(RowCodecError::Type {
            column: col_idx,
            expected: expected_type.clone(),
            got: "invalid bit string literal".to_owned(),
        });
    };
    let coerced = bits
        .coerce_to(expected_type, false)
        .ok_or_else(|| RowCodecError::Type {
            column: col_idx,
            expected: expected_type.clone(),
            got: format!("bit string length {}", bits.len()),
        })?;
    Ok(Value::BitString(coerced))
}

fn decode_network_value(
    text: &str,
    expected_type: &DataType,
    col_idx: usize,
) -> Result<Value, RowCodecError> {
    Value::parse_network(expected_type, text).ok_or_else(|| RowCodecError::Type {
        column: col_idx,
        expected: expected_type.clone(),
        got: "invalid network address literal".to_owned(),
    })
}

fn enum_label_is_valid(labels: &[String], value: &str) -> bool {
    labels.iter().any(|label| label == value)
}

const fn is_supported_type(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Bool
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Text { .. }
            | DataType::Enum { .. }
            | DataType::Composite { .. }
            | DataType::Char { .. }
            | DataType::Json
            | DataType::Jsonb
            | DataType::Xml
            | DataType::Vector { .. }
            | DataType::HalfVec { .. }
            | DataType::SparseVec { .. }
            | DataType::BitVec { .. }
            | DataType::Bit { .. }
            | DataType::VarBit { .. }
            | DataType::Inet
            | DataType::Cidr
            | DataType::MacAddr
            | DataType::MacAddr8
            | DataType::Date
            | DataType::Oid
            | DataType::RegClass
            | DataType::RegType
            | DataType::Time
            | DataType::TimeTz
            | DataType::PgLsn
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::Decimal { .. }
            | DataType::Money
            | DataType::Uuid
            | DataType::Bytea
            | DataType::Range(_)
            | DataType::Geometry(_)
            | DataType::Array(_)
            | DataType::Null
    )
}

fn encode_vector_payload(
    payload: &mut Vec<u8>,
    values: &[f32],
    expected_dims: Option<u32>,
    column: usize,
    ty: &DataType,
) -> Result<(), RowCodecError> {
    let actual_dims = u32::try_from(values.len()).map_err(|_| RowCodecError::UnsupportedType {
        column,
        ty: ty.clone(),
    })?;
    if actual_dims == 0
        || actual_dims > MAX_VECTOR_DIMS
        || expected_dims.is_some_and(|dims| dims != actual_dims)
        || values.iter().any(|value| !value.is_finite())
    {
        return Err(RowCodecError::Type {
            column,
            expected: ty.clone(),
            got: format!("vector({actual_dims})"),
        });
    }
    payload.extend_from_slice(&actual_dims.to_le_bytes());
    for value in values {
        payload.extend_from_slice(&value.to_le_bytes());
    }
    Ok(())
}

fn encode_dense_vector_family_text(
    payload: &mut Vec<u8>,
    value: &Value,
    values: &[f32],
    expected_dims: Option<u32>,
    column: usize,
    ty: &DataType,
) -> Result<(), RowCodecError> {
    let actual_dims = u32::try_from(values.len()).map_err(|_| RowCodecError::UnsupportedType {
        column,
        ty: ty.clone(),
    })?;
    if actual_dims == 0
        || actual_dims > MAX_VECTOR_DIMS
        || expected_dims.is_some_and(|dims| dims != actual_dims)
        || values.iter().any(|value| !value.is_finite())
    {
        return Err(RowCodecError::Type {
            column,
            expected: ty.clone(),
            got: format!("{}({actual_dims})", ty_name_for_dimension_error(ty)),
        });
    }
    encode_varlena_text(payload, &value.to_string(), column, ty)
}

fn encode_dimensioned_value_text(
    payload: &mut Vec<u8>,
    value: &Value,
    actual_dims: u32,
    expected_dims: Option<u32>,
    column: usize,
    ty: &DataType,
) -> Result<(), RowCodecError> {
    if actual_dims == 0
        || actual_dims > MAX_VECTOR_DIMS
        || expected_dims.is_some_and(|dims| dims != actual_dims)
    {
        return Err(RowCodecError::Type {
            column,
            expected: ty.clone(),
            got: format!("{}({actual_dims})", ty_name_for_dimension_error(ty)),
        });
    }
    encode_varlena_text(payload, &value.to_string(), column, ty)
}

fn decode_text_vector_family_value(
    parsed: Option<Value>,
    expected_dims: Option<u32>,
    column: usize,
    ty: &DataType,
) -> Result<Value, RowCodecError> {
    let value = parsed.ok_or_else(|| RowCodecError::Type {
        column,
        expected: ty.clone(),
        got: format!("invalid {ty} literal"),
    })?;
    let actual_dims = value.data_type().vector_dims().flatten();
    if actual_dims.is_none_or(|dims| {
        dims == 0
            || dims > MAX_VECTOR_DIMS
            || expected_dims.is_some_and(|expected| expected != dims)
    }) {
        return Err(RowCodecError::Type {
            column,
            expected: ty.clone(),
            got: value.data_type().to_string(),
        });
    }
    Ok(value)
}

#[derive(Debug)]
struct NumericBinaryParts {
    weight: i16,
    sign: u16,
    dscale: i16,
    digits: Vec<u16>,
}

fn encode_numeric_value_payload(
    payload: &mut Vec<u8>,
    value: i64,
    scale: i32,
    column: usize,
    ty: &DataType,
) -> Result<(), RowCodecError> {
    let parts = decimal_to_numeric_parts(value, scale, column, ty)?;
    let payload_len = NUMERIC_BINARY_HEADER_WIDTH
        .checked_add(
            parts
                .digits
                .len()
                .checked_mul(NUMERIC_DIGIT_WIDTH)
                .ok_or_else(|| numeric_type_error(column, ty, "numeric payload too large"))?,
        )
        .ok_or_else(|| numeric_type_error(column, ty, "numeric payload too large"))?;
    let payload_len_u32 = u32::try_from(payload_len)
        .map_err(|_| numeric_type_error(column, ty, "numeric payload too large"))?;
    let ndigits = i16::try_from(parts.digits.len())
        .map_err(|_| numeric_type_error(column, ty, "numeric has too many digit groups"))?;

    payload.extend_from_slice(&payload_len_u32.to_le_bytes());
    payload.extend_from_slice(&ndigits.to_be_bytes());
    payload.extend_from_slice(&parts.weight.to_be_bytes());
    payload.extend_from_slice(&parts.sign.to_be_bytes());
    payload.extend_from_slice(&parts.dscale.to_be_bytes());
    for digit in parts.digits {
        payload.extend_from_slice(&digit.to_be_bytes());
    }
    Ok(())
}

fn decimal_to_numeric_parts(
    value: i64,
    scale: i32,
    column: usize,
    ty: &DataType,
) -> Result<NumericBinaryParts, RowCodecError> {
    let sign = if value < 0 { NUMERIC_NEG } else { NUMERIC_POS };
    let mut magnitude = i128::from(value)
        .checked_abs()
        .ok_or_else(|| numeric_type_error(column, ty, "numeric magnitude overflow"))?;
    let dscale_i32 = if scale < 0 {
        let exp = scale
            .checked_neg()
            .and_then(|v| u32::try_from(v).ok())
            .ok_or_else(|| numeric_type_error(column, ty, "numeric scale out of range"))?;
        magnitude =
            magnitude
                .checked_mul(pow10_i128(exp).ok_or_else(|| {
                    numeric_type_error(column, ty, "numeric negative scale overflow")
                })?)
                .ok_or_else(|| numeric_type_error(column, ty, "numeric negative scale overflow"))?;
        0
    } else {
        scale
    };
    if dscale_i32 > NUMERIC_DSCALE_MAX {
        return Err(numeric_type_error(
            column,
            ty,
            "numeric display scale out of range",
        ));
    }
    let dscale = i16::try_from(dscale_i32)
        .map_err(|_| numeric_type_error(column, ty, "numeric display scale out of range"))?;
    if magnitude == 0 {
        return Ok(NumericBinaryParts {
            weight: 0,
            sign: NUMERIC_POS,
            dscale,
            digits: Vec::new(),
        });
    }

    let magnitude_digits = magnitude.to_string();
    let dscale_usize = usize::try_from(dscale_i32)
        .map_err(|_| numeric_type_error(column, ty, "numeric display scale out of range"))?;
    let digit_len = magnitude_digits.len();
    let integer_digits = digit_len.saturating_sub(dscale_usize);
    let groups_before_decimal = integer_digits.div_ceil(NUMERIC_DEC_DIGITS_USIZE);
    let mut grouped = String::new();

    if groups_before_decimal > 0 {
        let padded_integer_digits = groups_before_decimal
            .checked_mul(NUMERIC_DEC_DIGITS_USIZE)
            .ok_or_else(|| numeric_type_error(column, ty, "numeric payload too large"))?;
        for _ in 0..padded_integer_digits.saturating_sub(integer_digits) {
            grouped.push('0');
        }
        grouped.push_str(&magnitude_digits[..integer_digits]);
    }

    if dscale_usize > 0 {
        if dscale_usize > digit_len {
            for _ in 0..dscale_usize - digit_len {
                grouped.push('0');
            }
            grouped.push_str(&magnitude_digits);
        } else {
            grouped.push_str(&magnitude_digits[digit_len - dscale_usize..]);
        }
        let rem = grouped.len() % NUMERIC_DEC_DIGITS_USIZE;
        if rem != 0 {
            for _ in 0..NUMERIC_DEC_DIGITS_USIZE - rem {
                grouped.push('0');
            }
        }
    }

    let mut digits = grouped
        .as_bytes()
        .chunks_exact(NUMERIC_DEC_DIGITS_USIZE)
        .map(decimal_group_to_u16)
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| numeric_type_error(column, ty, "invalid numeric digit group"))?;
    let mut weight = i32::try_from(groups_before_decimal)
        .map_err(|_| numeric_type_error(column, ty, "numeric weight out of range"))?
        - 1;

    let leading_zeroes = digits.iter().take_while(|digit| **digit == 0).count();
    if leading_zeroes > 0 {
        digits.drain(..leading_zeroes);
        weight -= i32::try_from(leading_zeroes)
            .map_err(|_| numeric_type_error(column, ty, "numeric weight out of range"))?;
    }
    while digits.last().is_some_and(|digit| *digit == 0) {
        digits.pop();
    }
    if digits.is_empty() {
        weight = 0;
    }

    Ok(NumericBinaryParts {
        weight: i16::try_from(weight)
            .map_err(|_| numeric_type_error(column, ty, "numeric weight out of range"))?,
        sign,
        dscale,
        digits,
    })
}

fn validate_decimal_precision(
    value: i64,
    value_scale: i32,
    precision: Option<u32>,
    declared_scale: Option<i32>,
    column: usize,
    ty: &DataType,
) -> Result<(), RowCodecError> {
    let Some(precision) = precision else {
        return Ok(());
    };
    let precision = usize::try_from(precision)
        .map_err(|_| numeric_field_overflow(column, ty, "numeric precision out of range"))?;
    let actual_scale = usize::try_from(value_scale.max(0))
        .map_err(|_| numeric_field_overflow(column, ty, "numeric scale out of range"))?;
    let declared_scale = usize::try_from(declared_scale.unwrap_or(0).max(0))
        .map_err(|_| numeric_field_overflow(column, ty, "numeric scale out of range"))?;

    let magnitude = i128::from(value)
        .checked_abs()
        .ok_or_else(|| numeric_field_overflow(column, ty, "numeric magnitude overflow"))?;
    let total_digits = decimal_magnitude_digits(magnitude);
    let integer_digits = total_digits.saturating_sub(actual_scale);
    let max_integer_digits = precision.saturating_sub(declared_scale);

    if total_digits > precision || integer_digits > max_integer_digits {
        return Err(numeric_field_overflow(column, ty, "numeric field overflow"));
    }
    Ok(())
}

fn decimal_magnitude_digits(mut magnitude: i128) -> usize {
    let mut digits = 1;
    while magnitude >= 10 {
        magnitude /= 10;
        digits += 1;
    }
    digits
}

fn numeric_field_overflow(column: usize, ty: &DataType, detail: &str) -> RowCodecError {
    RowCodecError::NumericFieldOverflow {
        column,
        ty: ty.clone(),
        detail: detail.to_owned(),
    }
}

fn decimal_group_to_u16(group: &[u8]) -> Option<u16> {
    let mut value = 0_u16;
    for byte in group {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value
            .checked_mul(10)?
            .checked_add(u16::from(*byte - b'0'))?;
    }
    Some(value)
}

fn decode_numeric_value(
    bytes: &[u8],
    cursor: &mut usize,
    column: usize,
    ty: &DataType,
) -> Result<Value, RowCodecError> {
    let payload = read_varlena_slice(bytes, cursor)?;
    decode_numeric_payload(payload, column, ty)
}

fn decode_numeric_scaled_i64(
    bytes: &[u8],
    cursor: &mut usize,
    column: usize,
    ty: &DataType,
) -> Result<i64, RowCodecError> {
    match decode_numeric_value(bytes, cursor, column, ty)? {
        Value::Decimal { value, .. } => Ok(value),
        _ => unreachable!("decode_numeric_value always returns Decimal"),
    }
}

fn decode_numeric_payload(
    payload: &[u8],
    column: usize,
    ty: &DataType,
) -> Result<Value, RowCodecError> {
    if payload.len() < NUMERIC_BINARY_HEADER_WIDTH {
        return Err(RowCodecError::Truncated {
            needed: NUMERIC_BINARY_HEADER_WIDTH,
            have: payload.len(),
        });
    }
    let ndigits = i16::from_be_bytes([payload[0], payload[1]]);
    if ndigits < 0 {
        return Err(numeric_type_error(
            column,
            ty,
            "negative numeric digit count",
        ));
    }
    let ndigits_usize = usize::try_from(ndigits)
        .map_err(|_| numeric_type_error(column, ty, "invalid numeric digit count"))?;
    let weight = i16::from_be_bytes([payload[2], payload[3]]);
    let sign = u16::from_be_bytes([payload[4], payload[5]]);
    if !matches!(sign, NUMERIC_POS | NUMERIC_NEG) {
        return Err(numeric_type_error(column, ty, "unsupported numeric sign"));
    }
    let dscale = i16::from_be_bytes([payload[6], payload[7]]);
    if dscale < 0 {
        return Err(numeric_type_error(
            column,
            ty,
            "negative numeric display scale",
        ));
    }
    let expected_len = NUMERIC_BINARY_HEADER_WIDTH
        .checked_add(
            ndigits_usize
                .checked_mul(NUMERIC_DIGIT_WIDTH)
                .ok_or_else(|| numeric_type_error(column, ty, "numeric payload too large"))?,
        )
        .ok_or_else(|| numeric_type_error(column, ty, "numeric payload too large"))?;
    if payload.len() != expected_len {
        return Err(numeric_type_error(
            column,
            ty,
            "numeric payload length mismatch",
        ));
    }

    let mut digits = Vec::with_capacity(ndigits_usize);
    for raw in payload[NUMERIC_BINARY_HEADER_WIDTH..].chunks_exact(NUMERIC_DIGIT_WIDTH) {
        let digit = u16::from_be_bytes([raw[0], raw[1]]);
        if digit >= NUMERIC_NBASE {
            return Err(numeric_type_error(
                column,
                ty,
                "numeric digit outside base-10000",
            ));
        }
        digits.push(digit);
    }
    numeric_parts_to_value(&digits, weight, sign, dscale, column, ty)
}

fn numeric_parts_to_value(
    digits: &[u16],
    weight: i16,
    sign: u16,
    dscale: i16,
    column: usize,
    ty: &DataType,
) -> Result<Value, RowCodecError> {
    if digits.is_empty() {
        return Ok(Value::Decimal {
            value: 0,
            scale: i32::from(dscale),
        });
    }
    let mut acc = 0_i128;
    for (idx, digit) in digits.iter().enumerate() {
        if *digit == 0 {
            continue;
        }
        let idx_i32 = i32::try_from(idx)
            .map_err(|_| numeric_type_error(column, ty, "numeric payload too large"))?;
        let base_exp = i32::from(weight)
            .checked_sub(idx_i32)
            .ok_or_else(|| numeric_type_error(column, ty, "numeric exponent underflow"))?;
        let decimal_exp = base_exp
            .checked_mul(NUMERIC_DEC_DIGITS)
            .and_then(|exp| exp.checked_add(i32::from(dscale)))
            .ok_or_else(|| numeric_type_error(column, ty, "numeric exponent overflow"))?;
        let term = if decimal_exp < 0 {
            let divisor = pow10_i128(
                decimal_exp
                    .checked_neg()
                    .and_then(|exp| u32::try_from(exp).ok())
                    .ok_or_else(|| numeric_type_error(column, ty, "numeric exponent overflow"))?,
            )
            .ok_or_else(|| numeric_type_error(column, ty, "numeric exponent overflow"))?;
            let digit = i128::from(*digit);
            if digit % divisor != 0 {
                return Err(numeric_type_error(
                    column,
                    ty,
                    "numeric stores more fractional digits than display scale",
                ));
            }
            digit / divisor
        } else {
            let pow = pow10_i128(
                u32::try_from(decimal_exp)
                    .map_err(|_| numeric_type_error(column, ty, "numeric exponent overflow"))?,
            )
            .ok_or_else(|| numeric_type_error(column, ty, "numeric exponent overflow"))?;
            i128::from(*digit)
                .checked_mul(pow)
                .ok_or_else(|| numeric_type_error(column, ty, "numeric value overflow"))?
        };
        acc = acc
            .checked_add(term)
            .ok_or_else(|| numeric_type_error(column, ty, "numeric value overflow"))?;
    }
    if sign == NUMERIC_NEG {
        acc = acc
            .checked_neg()
            .ok_or_else(|| numeric_type_error(column, ty, "numeric value overflow"))?;
    }
    Ok(Value::Decimal {
        value: i64::try_from(acc)
            .map_err(|_| numeric_type_error(column, ty, "numeric value overflows i64 runtime"))?,
        scale: i32::from(dscale),
    })
}

fn pow10_i128(exp: u32) -> Option<i128> {
    (0..exp).try_fold(1_i128, |acc, _| acc.checked_mul(10))
}

fn numeric_type_error(column: usize, ty: &DataType, got: &str) -> RowCodecError {
    RowCodecError::Type {
        column,
        expected: ty.clone(),
        got: got.to_owned(),
    }
}

fn read_fixed<const N: usize>(bytes: &[u8], cursor: &mut usize) -> Result<[u8; N], RowCodecError> {
    let needed = cursor.saturating_add(N);
    if bytes.len() < needed {
        return Err(RowCodecError::Truncated {
            needed,
            have: bytes.len(),
        });
    }
    let raw: [u8; N] = bytes[*cursor..needed]
        .try_into()
        .map_err(|_| RowCodecError::Truncated {
            needed,
            have: bytes.len(),
        })?;
    *cursor = needed;
    Ok(raw)
}

fn skip_fixed(bytes: &[u8], cursor: &mut usize, width: usize) -> Result<(), RowCodecError> {
    let needed = cursor.saturating_add(width);
    if bytes.len() < needed {
        return Err(RowCodecError::Truncated {
            needed,
            have: bytes.len(),
        });
    }
    *cursor = needed;
    Ok(())
}

fn decode_varlena_bytes(bytes: &[u8], cursor: &mut usize) -> Result<Vec<u8>, RowCodecError> {
    let payload = read_varlena_slice(bytes, cursor)?;
    Ok(payload.to_vec())
}

fn skip_varlena_payload(bytes: &[u8], cursor: &mut usize) -> Result<(), RowCodecError> {
    let _ = read_varlena_slice(bytes, cursor)?;
    Ok(())
}

fn read_varlena_slice<'a>(bytes: &'a [u8], cursor: &mut usize) -> Result<&'a [u8], RowCodecError> {
    let len_raw = read_fixed::<4>(bytes, cursor)?;
    let value_len = u32_payload_len_to_usize(u32::from_le_bytes(len_raw))?;
    let end = checked_payload_end(*cursor, value_len, bytes.len())?;
    if bytes.len() < end {
        return Err(RowCodecError::Truncated {
            needed: end,
            have: bytes.len(),
        });
    }
    let payload = &bytes[*cursor..end];
    *cursor = end;
    Ok(payload)
}

fn skip_vector_value(
    bytes: &[u8],
    cursor: &mut usize,
    expected_dims: Option<u32>,
    column: usize,
) -> Result<(), RowCodecError> {
    let dims_raw = read_fixed::<VECTOR_DIMS_WIDTH>(bytes, cursor)?;
    let dims = u32::from_le_bytes(dims_raw);
    if dims == 0 || dims > MAX_VECTOR_DIMS || expected_dims.is_some_and(|expected| expected != dims)
    {
        return Err(RowCodecError::Type {
            column,
            expected: DataType::Vector {
                dims: expected_dims,
            },
            got: format!("vector({dims})"),
        });
    }
    let dims_usize = u32_payload_len_to_usize(dims)?;
    let byte_len =
        dims_usize
            .checked_mul(VECTOR_ELEMENT_WIDTH)
            .ok_or(RowCodecError::UnsupportedType {
                column,
                ty: DataType::Vector {
                    dims: expected_dims,
                },
            })?;
    skip_fixed(bytes, cursor, byte_len)
}

const fn ty_name_for_dimension_error(ty: &DataType) -> &'static str {
    match ty {
        DataType::Vector { .. } => "vector",
        DataType::HalfVec { .. } => "halfvec",
        DataType::SparseVec { .. } => "sparsevec",
        DataType::BitVec { .. } => "bitvec",
        _ => "value",
    }
}

fn decode_vector_value(
    bytes: &[u8],
    cursor: &mut usize,
    expected_dims: Option<u32>,
    column: usize,
    ty: &DataType,
) -> Result<Value, RowCodecError> {
    let dims_end = cursor.saturating_add(VECTOR_DIMS_WIDTH);
    if bytes.len() < dims_end {
        return Err(RowCodecError::Truncated {
            needed: dims_end,
            have: bytes.len(),
        });
    }
    let dims_raw: [u8; VECTOR_DIMS_WIDTH] =
        bytes[*cursor..dims_end]
            .try_into()
            .map_err(|_| RowCodecError::Truncated {
                needed: dims_end,
                have: bytes.len(),
            })?;
    *cursor = dims_end;
    let dims = u32::from_le_bytes(dims_raw);
    if dims == 0 || dims > MAX_VECTOR_DIMS || expected_dims.is_some_and(|expected| expected != dims)
    {
        return Err(RowCodecError::Type {
            column,
            expected: ty.clone(),
            got: format!("vector({dims})"),
        });
    }
    let dims_usize = u32_payload_len_to_usize(dims)?;
    let byte_len = dims_usize
        .checked_mul(VECTOR_ELEMENT_WIDTH)
        .ok_or_else(|| RowCodecError::UnsupportedType {
            column,
            ty: ty.clone(),
        })?;
    let values_end =
        cursor
            .checked_add(byte_len)
            .ok_or_else(|| RowCodecError::UnsupportedType {
                column,
                ty: ty.clone(),
            })?;
    if bytes.len() < values_end {
        return Err(RowCodecError::Truncated {
            needed: values_end,
            have: bytes.len(),
        });
    }
    let mut values = Vec::with_capacity(dims_usize);
    for chunk in bytes[*cursor..values_end].chunks_exact(VECTOR_ELEMENT_WIDTH) {
        let raw: [u8; VECTOR_ELEMENT_WIDTH] = chunk
            .try_into()
            .expect("chunks_exact(VECTOR_ELEMENT_WIDTH)");
        let value = f32::from_le_bytes(raw);
        if !value.is_finite() {
            return Err(RowCodecError::Type {
                column,
                expected: ty.clone(),
                got: "non-finite vector element".to_owned(),
            });
        }
        values.push(value);
    }
    *cursor = values_end;
    Ok(Value::Vector(values))
}

fn encode_varlena_text(
    payload: &mut Vec<u8>,
    text: &str,
    column: usize,
    ty: &DataType,
) -> Result<(), RowCodecError> {
    let bytes = text.as_bytes();
    let len = u32::try_from(bytes.len()).map_err(|_| RowCodecError::UnsupportedType {
        column,
        ty: ty.clone(),
    })?;
    payload.extend_from_slice(&len.to_le_bytes());
    payload.extend_from_slice(bytes);
    Ok(())
}

fn validate_varchar_storage_text(
    text: &str,
    max_len: Option<u32>,
    column: usize,
    ty: &DataType,
) -> Result<(), RowCodecError> {
    let Some(max_len) = max_len else {
        return Ok(());
    };
    let max_len_usize = usize::try_from(max_len).map_err(|_| RowCodecError::UnsupportedType {
        column,
        ty: ty.clone(),
    })?;
    if text.chars().nth(max_len_usize).is_some() {
        return Err(RowCodecError::StringDataRightTruncation {
            column,
            ty: ty.clone(),
            detail: format!("value too long for type character varying({max_len})"),
        });
    }
    Ok(())
}

fn normalize_jsonb_storage_text(
    text: &str,
    column: usize,
    ty: &DataType,
) -> Result<String, RowCodecError> {
    let parsed =
        serde_json::from_str::<serde_json::Value>(text).map_err(|err| RowCodecError::Type {
            column,
            expected: ty.clone(),
            got: format!("invalid jsonb: {err}"),
        })?;
    serde_json::to_string(&parsed).map_err(|err| RowCodecError::Type {
        column,
        expected: ty.clone(),
        got: format!("cannot encode jsonb: {err}"),
    })
}

fn validate_json_storage_text(
    text: &str,
    column: usize,
    ty: &DataType,
) -> Result<(), RowCodecError> {
    serde_json::from_str::<serde_json::Value>(text)
        .map(|_| ())
        .map_err(|err| RowCodecError::Type {
            column,
            expected: ty.clone(),
            got: format!("invalid json: {err}"),
        })
}

fn validate_xml_storage_text(
    text: &str,
    column: usize,
    ty: &DataType,
) -> Result<(), RowCodecError> {
    Value::validate_xml_text(text)
        .map(|_| ())
        .ok_or_else(|| RowCodecError::Type {
            column,
            expected: ty.clone(),
            got: "invalid xml".to_owned(),
        })
}

fn decode_varlena_text(
    bytes: &[u8],
    cursor: &mut usize,
    context: &'static str,
) -> Result<String, RowCodecError> {
    let len_end = *cursor + 4;
    if bytes.len() < len_end {
        return Err(RowCodecError::Truncated {
            needed: len_end,
            have: bytes.len(),
        });
    }
    let len_raw: [u8; 4] =
        bytes[*cursor..*cursor + 4]
            .try_into()
            .map_err(|_| RowCodecError::Truncated {
                needed: len_end,
                have: bytes.len(),
            })?;
    let str_len = u32_payload_len_to_usize(u32::from_le_bytes(len_raw))?;
    *cursor += 4;
    let str_end = checked_payload_end(*cursor, str_len, bytes.len())?;
    if bytes.len() < str_end {
        return Err(RowCodecError::Truncated {
            needed: str_end,
            have: bytes.len(),
        });
    }
    let s = String::from_utf8(bytes[*cursor..str_end].to_vec())
        .map_err(|e| RowCodecError::InvalidUtf8(e, context))?;
    *cursor = str_end;
    Ok(s)
}

/// Errors raised by [`RowCodec`].
#[derive(Debug, thiserror::Error)]
pub enum RowCodecError {
    /// Arity mismatch.
    #[error("arity mismatch: schema has {schema}, row has {row}")]
    Arity {
        /// Schema arity.
        schema: usize,
        /// Caller-supplied row arity.
        row: usize,
    },
    /// Type mismatch.
    #[error("type mismatch at column {column}: expected {expected}, got {got}")]
    Type {
        /// Column index.
        column: usize,
        /// Expected schema type.
        expected: DataType,
        /// Runtime type name.
        got: String,
    },
    /// A character value exceeds its declared length.
    #[error("{detail}")]
    StringDataRightTruncation {
        /// Column index.
        column: usize,
        /// Expected schema type.
        ty: DataType,
        /// User-facing error detail.
        detail: String,
    },
    /// A numeric value exceeds declared precision.
    #[error("{detail}")]
    NumericFieldOverflow {
        /// Column index.
        column: usize,
        /// Expected schema type.
        ty: DataType,
        /// User-facing error detail.
        detail: String,
    },
    /// Truncated payload.
    #[error("payload truncated: needed {needed}, have {have}")]
    Truncated {
        /// Required byte count.
        needed: usize,
        /// Actual byte count.
        have: usize,
    },
    /// A length prefix does not fit the host address space.
    #[error("payload length prefix does not fit usize: {len}")]
    LengthOverflow {
        /// The raw little-endian `u32` length prefix.
        len: u32,
    },
    /// Unsupported type.
    #[error("unsupported type at column {column}: {ty}")]
    UnsupportedType {
        /// Column index.
        column: usize,
        /// Unsupported `DataType`.
        ty: DataType,
    },
    /// Invalid UTF-8 in a Text column.
    #[error("invalid utf8 at column {1}: {0}")]
    InvalidUtf8(#[source] std::string::FromUtf8Error, &'static str),
    /// Invalid UTF-8 in a borrowed Text column payload.
    #[error("invalid utf8 at column {1}: {0}")]
    InvalidUtf8Slice(#[source] std::str::Utf8Error, &'static str),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use proptest::prelude::*;
    use ultrasql_core::{
        BitString, DataType, Field, GeometryType, GeometryValue, Lsn, NetworkValue, Oid, RangeType,
        RangeValue, Schema, SparseVector, Value,
    };

    use super::{ColumnBuilder, RowCodec, RowCodecError, VECTOR_DIMS_WIDTH, VECTOR_ELEMENT_WIDTH};
    use ultrasql_vec::column::Column;

    fn schema_bool() -> Schema {
        Schema::new([Field::required("b", DataType::Bool)]).unwrap()
    }
    fn schema_i16() -> Schema {
        Schema::new([Field::required("n", DataType::Int16)]).unwrap()
    }
    fn schema_i32() -> Schema {
        Schema::new([Field::required("n", DataType::Int32)]).unwrap()
    }
    fn schema_i64() -> Schema {
        Schema::new([Field::required("n", DataType::Int64)]).unwrap()
    }
    fn schema_f32() -> Schema {
        Schema::new([Field::required("f", DataType::Float32)]).unwrap()
    }
    fn schema_f64() -> Schema {
        Schema::new([Field::required("f", DataType::Float64)]).unwrap()
    }
    fn schema_text() -> Schema {
        Schema::new([Field::required("s", DataType::Text { max_len: None })]).unwrap()
    }
    fn schema_varchar3() -> Schema {
        Schema::new([Field::required("s", DataType::Text { max_len: Some(3) })]).unwrap()
    }
    fn schema_char4() -> Schema {
        Schema::new([Field::required("c", DataType::Char { len: Some(4) })]).unwrap()
    }
    fn schema_decimal(scale: Option<i32>) -> Schema {
        Schema::new([Field::required(
            "n",
            DataType::Decimal {
                precision: None,
                scale,
            },
        )])
        .unwrap()
    }
    fn schema_money() -> Schema {
        Schema::new([Field::required("amount", DataType::Money)]).unwrap()
    }
    fn schema_mixed() -> Schema {
        Schema::new([
            Field::nullable("id", DataType::Int32),
            Field::required("name", DataType::Text { max_len: None }),
            Field::nullable("score", DataType::Float64),
        ])
        .unwrap()
    }
    fn schema_all_nullable() -> Schema {
        Schema::new([
            Field::nullable("a", DataType::Int32),
            Field::nullable("b", DataType::Text { max_len: None }),
        ])
        .unwrap()
    }

    #[test]
    fn round_trip_bool_true() {
        let codec = RowCodec::new(schema_bool());
        let row = vec![Value::Bool(true)];
        let bytes = codec.encode(&row).unwrap();
        assert_eq!(codec.decode(&bytes).unwrap(), row);
    }
    #[test]
    fn round_trip_bool_false() {
        let codec = RowCodec::new(schema_bool());
        let row = vec![Value::Bool(false)];
        let bytes = codec.encode(&row).unwrap();
        assert_eq!(codec.decode(&bytes).unwrap(), row);
    }
    #[test]
    fn round_trip_int16() {
        let codec = RowCodec::new(schema_i16());
        for v in [i16::MIN, -1, 0, 1, i16::MAX] {
            let row = vec![Value::Int16(v)];
            assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
        }
    }
    #[test]
    fn round_trip_int32() {
        let codec = RowCodec::new(schema_i32());
        for v in [i32::MIN, -42, 0, 42, i32::MAX] {
            let row = vec![Value::Int32(v)];
            assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
        }
    }
    #[test]
    fn round_trip_int64() {
        let codec = RowCodec::new(schema_i64());
        for v in [i64::MIN, -1, 0, 1, i64::MAX] {
            let row = vec![Value::Int64(v)];
            assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
        }
    }
    #[test]
    fn round_trip_float32() {
        let codec = RowCodec::new(schema_f32());
        for v in [f32::NEG_INFINITY, -1.5, 0.0, 1.5, f32::INFINITY] {
            let row = vec![Value::Float32(v)];
            assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
        }
    }
    #[test]
    fn round_trip_float64() {
        let codec = RowCodec::new(schema_f64());
        for v in [f64::NEG_INFINITY, -1.5, 0.0, 1.5, f64::INFINITY] {
            let row = vec![Value::Float64(v)];
            assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
        }
    }
    #[test]
    fn round_trip_text() {
        let codec = RowCodec::new(schema_text());
        for s in ["", "hello", "unicode: \u{1F600}", &"x".repeat(1024)] {
            let row = vec![Value::Text(s.to_owned())];
            assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
        }
    }

    #[test]
    fn bounded_varchar_rejects_overlength_text_assignment() {
        let codec = RowCodec::new(schema_varchar3());
        assert!(matches!(
            codec.encode(&[Value::Text("abcd".to_owned())]),
            Err(RowCodecError::StringDataRightTruncation { column: 0, .. })
        ));
    }

    #[test]
    fn round_trip_bpchar_pads_text_assignment() {
        let codec = RowCodec::new(schema_char4());
        let encoded = codec.encode(&[Value::Text("ok".to_owned())]).unwrap();
        assert_eq!(
            codec.decode(&encoded).unwrap(),
            vec![Value::Char("ok  ".to_owned())]
        );
        assert!(matches!(
            codec.encode(&[Value::Text("toolong".to_owned())]),
            Err(RowCodecError::StringDataRightTruncation { column: 0, .. })
        ));
    }

    #[test]
    fn decimal_binary_layout_uses_postgres_numeric_groups() {
        let codec = RowCodec::new(schema_decimal(Some(4)));
        let encoded = codec
            .encode(&[Value::Decimal {
                value: 1_234_567_890_123,
                scale: 4,
            }])
            .unwrap();

        assert_eq!(
            encoded,
            vec![
                0x00, // null bitmap: one non-null column
                0x10, 0x00, 0x00, 0x00, // numeric payload length: 16 bytes
                0x00, 0x04, // ndigits
                0x00, 0x02, // weight
                0x00, 0x00, // sign: NUMERIC_POS
                0x00, 0x04, // dscale
                0x00, 0x01, // 1
                0x09, 0x29, // 2345
                0x1a, 0x85, // 6789
                0x00, 0x7b, // 0123
            ]
        );
    }

    #[test]
    fn decimal_precision_rejects_integer_overflow() {
        let schema = Schema::new([Field::required(
            "n",
            DataType::Decimal {
                precision: Some(4),
                scale: Some(2),
            },
        )])
        .unwrap();
        let codec = RowCodec::new(schema);

        assert!(
            codec
                .encode(&[Value::Decimal {
                    value: 1_234,
                    scale: 2,
                }])
                .is_ok()
        );
        assert!(matches!(
            codec.encode(&[Value::Decimal {
                value: 12_345,
                scale: 2,
            }]),
            Err(RowCodecError::NumericFieldOverflow { column: 0, .. })
        ));
    }

    #[test]
    fn decimal_round_trip_preserves_fractional_weight() {
        let codec = RowCodec::new(schema_decimal(Some(6)));
        let row = vec![Value::Decimal {
            value: -12,
            scale: 6,
        }];
        let encoded = codec.encode(&row).unwrap();

        assert_eq!(
            encoded,
            vec![
                0x00, // null bitmap
                0x0a, 0x00, 0x00, 0x00, // payload length: header + one digit
                0x00, 0x01, // ndigits
                0xff, 0xfe, // weight: -2
                0x40, 0x00, // sign: NUMERIC_NEG
                0x00, 0x06, // dscale
                0x04, 0xb0, // 1200
            ]
        );
        assert_eq!(codec.decode(&encoded).unwrap(), row);
    }

    #[test]
    fn decimal_decode_rejects_digit_outside_nbase() {
        let codec = RowCodec::new(schema_decimal(Some(0)));
        let encoded = vec![
            0x00, // null bitmap
            0x0a, 0x00, 0x00, 0x00, // payload length
            0x00, 0x01, // ndigits
            0x00, 0x00, // weight
            0x00, 0x00, // sign
            0x00, 0x00, // dscale
            0x27, 0x10, // 10000, invalid in base-10000
        ];

        let err = codec.decode(&encoded).expect_err("invalid numeric digit");
        assert!(matches!(err, RowCodecError::Type { column: 0, .. }));
    }

    #[test]
    fn money_round_trip_uses_i64_cash_storage() {
        let codec = RowCodec::new(schema_money());
        let row = vec![Value::Money(123_456)];
        let encoded = codec.encode(&row).unwrap();

        assert_eq!(
            encoded,
            vec![
                0x00, // null bitmap
                0x40, 0xe2, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
            ]
        );
        assert_eq!(codec.decode(&encoded).unwrap(), row);
    }

    #[test]
    fn decode_into_builders_reads_money_cash_payload() {
        let schema = schema_money();
        let codec = RowCodec::new(schema.clone());
        let encoded = codec.encode(&[Value::Money(-123)]).unwrap();
        let mut builders = vec![ColumnBuilder::new(&schema.field_at(0).data_type, 1, 0).unwrap()];

        codec.decode_into_builders(&encoded, &mut builders).unwrap();
        let batch = RowCodec::finish_batch(builders).unwrap();
        match &batch.columns()[0] {
            Column::Int64(c) => assert_eq!(c.data()[0], -123),
            other => panic!("expected money Int64 builder output, got {other:?}"),
        }
    }

    #[test]
    fn decode_projected_skips_decimal_varlena_payload() {
        let schema = Schema::new([
            Field::required(
                "n",
                DataType::Decimal {
                    precision: None,
                    scale: Some(4),
                },
            ),
            Field::required("id", DataType::Int32),
        ])
        .unwrap();
        let codec = RowCodec::new(schema);
        let encoded = codec
            .encode(&[
                Value::Decimal {
                    value: 1_234_567_890_123,
                    scale: 4,
                },
                Value::Int32(7),
            ])
            .unwrap();

        assert_eq!(
            codec.decode_projected(&encoded, &[1]).unwrap(),
            vec![Value::Int32(7)]
        );
    }

    #[test]
    fn decode_into_builders_reads_decimal_numeric_payload() {
        let schema = schema_decimal(Some(4));
        let codec = RowCodec::new(schema.clone());
        let encoded = codec
            .encode(&[Value::Decimal {
                value: 1_234_567_890_123,
                scale: 4,
            }])
            .unwrap();
        let mut builders = vec![ColumnBuilder::new(&schema.field_at(0).data_type, 1, 0).unwrap()];

        codec.decode_into_builders(&encoded, &mut builders).unwrap();
        let batch = RowCodec::finish_batch(builders).unwrap();
        match &batch.columns()[0] {
            Column::Int64(c) => assert_eq!(c.data()[0], 1_234_567_890_123),
            other => panic!("expected decimal Int64 builder output, got {other:?}"),
        }
    }

    #[test]
    fn decode_into_builders_fast_paths_cover_fixed_width_shapes() {
        for (schema, row, expected_width) in [
            (
                Schema::new([Field::required("a", DataType::Int32)]).unwrap(),
                vec![Value::Int32(1)],
                1,
            ),
            (
                Schema::new([
                    Field::required("a", DataType::Int32),
                    Field::required("b", DataType::Int32),
                ])
                .unwrap(),
                vec![Value::Int32(1), Value::Int32(2)],
                2,
            ),
            (
                Schema::new([
                    Field::required("a", DataType::Int32),
                    Field::required("b", DataType::Int32),
                    Field::required("c", DataType::Int32),
                ])
                .unwrap(),
                vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)],
                3,
            ),
            (
                Schema::new([Field::required("a", DataType::Int64)]).unwrap(),
                vec![Value::Int64(4)],
                1,
            ),
            (
                Schema::new([
                    Field::required("a", DataType::Int64),
                    Field::required("b", DataType::Int64),
                ])
                .unwrap(),
                vec![Value::Int64(4), Value::Int64(5)],
                2,
            ),
        ] {
            let codec = RowCodec::new(schema.clone());
            let encoded = codec.encode(&row).expect("encode");
            let mut builders = schema
                .fields()
                .iter()
                .map(|field| ColumnBuilder::new(&field.data_type, 1, 0).expect("builder"))
                .collect::<Vec<_>>();

            codec
                .decode_into_builders(&encoded, &mut builders)
                .expect("fast decode");
            let batch = RowCodec::finish_batch(builders).expect("finish");
            assert_eq!(batch.width(), expected_width);
            assert_eq!(batch.rows(), 1);
            assert_eq!(codec.decode(&encoded).expect("decode"), row);
        }
    }

    #[test]
    fn decode_into_builders_fast_path_falls_back_for_nulls_and_mismatched_builders() {
        let schema = Schema::new([
            Field::required("a", DataType::Int32),
            Field::required("b", DataType::Int32),
        ])
        .unwrap();
        let codec = RowCodec::new(schema.clone());
        let encoded = codec
            .encode(&[Value::Null, Value::Int32(7)])
            .expect("encode");
        let mut builders = codec.new_builders(1).expect("builders");
        codec
            .decode_into_builders(&encoded, &mut builders)
            .expect("generic decode");
        let batch = RowCodec::finish_batch(builders).expect("finish");
        match &batch.columns()[0] {
            Column::Int32(c) => assert!(c.nulls().is_some_and(|n| !n.get(0))),
            other => panic!("expected int32 column, got {other:?}"),
        }
        match &batch.columns()[1] {
            Column::Int32(c) => assert_eq!(c.data()[0], 7),
            other => panic!("expected int32 column, got {other:?}"),
        }

        let mut wrong_builders = vec![
            ColumnBuilder::new(&DataType::Int64, 1, 0).expect("wrong builder"),
            ColumnBuilder::new(&DataType::Int64, 1, 1).expect("wrong builder"),
        ];
        let err = codec
            .decode_into_builders(
                &codec
                    .encode(&[Value::Int32(1), Value::Int32(2)])
                    .expect("encode"),
                &mut wrong_builders,
            )
            .expect_err("builder mismatch");
        assert!(matches!(
            err,
            RowCodecError::UnsupportedType { column: 0, .. }
        ));
    }

    #[test]
    fn decode_into_builders_fast_path_reports_truncated_fixed_width_payloads() {
        for schema in [
            Schema::new([Field::required("a", DataType::Int32)]).unwrap(),
            Schema::new([
                Field::required("a", DataType::Int32),
                Field::required("b", DataType::Int32),
            ])
            .unwrap(),
            Schema::new([
                Field::required("a", DataType::Int32),
                Field::required("b", DataType::Int32),
                Field::required("c", DataType::Int32),
            ])
            .unwrap(),
            Schema::new([Field::required("a", DataType::Int64)]).unwrap(),
            Schema::new([
                Field::required("a", DataType::Int64),
                Field::required("b", DataType::Int64),
            ])
            .unwrap(),
        ] {
            let codec = RowCodec::new(schema);
            let mut builders = codec.new_builders(1).expect("builders");
            let err = codec
                .decode_into_builders(&[0], &mut builders)
                .expect_err("truncated fixed payload");
            assert!(matches!(err, RowCodecError::Truncated { .. }));
        }
    }

    #[test]
    fn decode_into_builders_rejects_invalid_utf8_without_owned_error_roundtrip() {
        let codec = RowCodec::new(schema_text());
        let mut builders = codec.new_builders(1).expect("builders");
        let err = codec
            .decode_into_builders(&[0x00, 0x01, 0x00, 0x00, 0x00, 0xff], &mut builders)
            .expect_err("invalid utf8");
        assert!(matches!(
            err,
            RowCodecError::InvalidUtf8Slice(_, "text column")
        ));
    }

    #[test]
    fn decode_into_builders_generic_covers_bool_smallint_float_and_nulls() {
        let schema = Schema::new([
            Field::nullable("b", DataType::Bool),
            Field::nullable("s", DataType::Int16),
            Field::nullable("f4", DataType::Float32),
            Field::nullable("f8", DataType::Float64),
        ])
        .unwrap();
        let codec = RowCodec::new(schema.clone());
        assert_eq!(codec.fixed_width_lower_bound(), 1 + 1 + 2 + 4 + 8);
        let rows = [
            vec![
                Value::Bool(true),
                Value::Int16(-7),
                Value::Float32(1.5),
                Value::Float64(-2.25),
            ],
            vec![Value::Null, Value::Null, Value::Null, Value::Null],
        ];
        let mut builders = codec.new_builders(rows.len()).expect("builders");
        for row in rows {
            let encoded = codec.encode(&row).expect("encode");
            codec
                .decode_into_builders(&encoded, &mut builders)
                .expect("decode builders");
        }

        let batch = RowCodec::finish_batch(builders).expect("finish");
        assert_eq!(batch.rows(), 2);
        match &batch.columns()[0] {
            Column::Bool(c) => {
                assert_eq!(c.data(), &[1, 0]);
                let nulls = c.nulls().expect("bool nulls");
                assert!(nulls.get(0));
                assert!(!nulls.get(1));
            }
            other => panic!("expected bool column, got {other:?}"),
        }
        match &batch.columns()[1] {
            Column::Int32(c) => {
                assert_eq!(c.data(), &[-7, 0]);
                let nulls = c.nulls().expect("int16 nulls");
                assert!(nulls.get(0));
                assert!(!nulls.get(1));
            }
            other => panic!("expected int32-backed int16 column, got {other:?}"),
        }
        match &batch.columns()[2] {
            Column::Float32(c) => {
                assert_eq!(c.data(), &[1.5, 0.0]);
                let nulls = c.nulls().expect("float32 nulls");
                assert!(nulls.get(0));
                assert!(!nulls.get(1));
            }
            other => panic!("expected float32 column, got {other:?}"),
        }
        match &batch.columns()[3] {
            Column::Float64(c) => {
                assert_eq!(c.data(), &[-2.25, 0.0]);
                let nulls = c.nulls().expect("float64 nulls");
                assert!(nulls.get(0));
                assert!(!nulls.get(1));
            }
            other => panic!("expected float64 column, got {other:?}"),
        }
    }

    #[test]
    fn round_trip_int_array() {
        let schema = Schema::new([Field::required(
            "xs",
            DataType::Array(Box::new(DataType::Int32)),
        )])
        .unwrap();
        let codec = RowCodec::new(schema);
        let row = vec![Value::Array {
            element_type: DataType::Int32,
            elements: vec![Value::Int32(1), Value::Int32(2), Value::Null],
        }];
        assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
    }

    #[test]
    fn round_trip_json_preserves_text() {
        let schema = Schema::new([Field::required("doc", DataType::Json)]).unwrap();
        let codec = RowCodec::new(schema);
        let row = vec![Value::Json(r#"{"b": 2, "a": 1}"#.into())];
        assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
    }

    #[test]
    fn round_trip_jsonb() {
        let schema = Schema::new([Field::required("doc", DataType::Jsonb)]).unwrap();
        let codec = RowCodec::new(schema);
        let row = vec![Value::Jsonb(r#"{"b":"x","a":1}"#.into())];
        assert_eq!(
            codec.decode(&codec.encode(&row).unwrap()).unwrap(),
            vec![Value::Jsonb(r#"{"a":1,"b":"x"}"#.into())]
        );
    }

    #[test]
    fn round_trip_xml_preserves_text_and_rejects_unbalanced_input() {
        let schema = Schema::new([Field::required("doc", DataType::Xml)]).unwrap();
        let codec = RowCodec::new(schema);
        let row = vec![Value::Xml(
            r#"<root attr="v"><child>text</child></root>"#.into(),
        )];
        assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
        assert!(codec.encode(&[Value::Xml("<root>".into())]).is_err());
    }

    #[test]
    fn round_trip_vector() {
        let schema = Schema::new([Field::required(
            "embedding",
            DataType::Vector { dims: Some(3) },
        )])
        .unwrap();
        let codec = RowCodec::new(schema);
        let row = vec![Value::Vector(vec![1.0, 2.5, -3.0])];
        assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
    }

    #[test]
    fn vector_binary_layout_is_stable() {
        let schema = Schema::new([Field::required(
            "embedding",
            DataType::Vector { dims: Some(3) },
        )])
        .unwrap();
        let codec = RowCodec::new(schema);
        let encoded = codec
            .encode(&[Value::Vector(vec![1.0, 2.5, -3.0])])
            .unwrap();

        assert_eq!(
            encoded.len(),
            1 + VECTOR_DIMS_WIDTH + 3 * VECTOR_ELEMENT_WIDTH
        );
        assert_eq!(
            encoded,
            vec![
                0x00, // null bitmap: one non-null column
                0x03, 0x00, 0x00, 0x00, // dims: u32 little-endian
                0x00, 0x00, 0x80, 0x3f, // 1.0f32 little-endian
                0x00, 0x00, 0x20, 0x40, // 2.5f32 little-endian
                0x00, 0x00, 0x40, 0xc0, // -3.0f32 little-endian
            ]
        );
    }

    #[test]
    fn vector_decode_rejects_truncated_payload() {
        let schema = Schema::new([Field::required(
            "embedding",
            DataType::Vector { dims: Some(3) },
        )])
        .unwrap();
        let codec = RowCodec::new(schema);
        let mut encoded = vec![0x00];
        encoded.extend_from_slice(&3_u32.to_le_bytes());
        encoded.extend_from_slice(&1.0_f32.to_le_bytes());

        let err = codec
            .decode(&encoded)
            .expect_err("truncated vector payload");
        assert!(matches!(
            err,
            RowCodecError::Truncated { needed, have }
                if needed == 1 + VECTOR_DIMS_WIDTH + 3 * VECTOR_ELEMENT_WIDTH
                    && have == encoded.len()
        ));
    }

    #[test]
    fn vector_decode_rejects_non_finite_payload() {
        let schema = Schema::new([Field::required(
            "embedding",
            DataType::Vector { dims: Some(1) },
        )])
        .unwrap();
        let codec = RowCodec::new(schema);
        let mut encoded = vec![0x00];
        encoded.extend_from_slice(&1_u32.to_le_bytes());
        encoded.extend_from_slice(&f32::NAN.to_le_bytes());

        let err = codec
            .decode(&encoded)
            .expect_err("non-finite vector payload");
        assert!(matches!(err, RowCodecError::Type { column: 0, .. }));
    }

    #[test]
    fn round_trip_vector_family_values() {
        let schema = Schema::new([
            Field::required("h", DataType::HalfVec { dims: Some(3) }),
            Field::required("s", DataType::SparseVec { dims: Some(5) }),
            Field::required("b", DataType::BitVec { dims: Some(6) }),
        ])
        .unwrap();
        let codec = RowCodec::new(schema);
        let row = vec![
            Value::HalfVec(vec![1.0, 2.5, -3.0]),
            Value::SparseVec(SparseVector::new(5, vec![(1, 1.0), (3, 2.5)]).unwrap()),
            Value::BitVec {
                dims: 6,
                bytes: vec![0b1010_0100],
            },
        ];
        assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
    }

    #[test]
    fn round_trip_temporal_oid_binary_network_range_and_geometry_values() {
        let schema = Schema::new([
            Field::required("oid", DataType::Oid),
            Field::required("regclass", DataType::RegClass),
            Field::required("regtype", DataType::RegType),
            Field::required("lsn", DataType::PgLsn),
            Field::required("date", DataType::Date),
            Field::required("ts", DataType::Timestamp),
            Field::required("tstz", DataType::TimestampTz),
            Field::required("time", DataType::Time),
            Field::required("timetz", DataType::TimeTz),
            Field::required("interval", DataType::Interval),
            Field::required("uuid", DataType::Uuid),
            Field::required("bytea", DataType::Bytea),
            Field::required("bits", DataType::Bit { len: Some(4) }),
            Field::required("varbits", DataType::VarBit { max_len: Some(8) }),
            Field::required("inet", DataType::Inet),
            Field::required("range", DataType::Range(RangeType::Int4)),
            Field::required("geom", DataType::Geometry(GeometryType::Box)),
        ])
        .unwrap();
        let codec = RowCodec::new(schema.clone());
        let row = vec![
            Value::Oid(Oid::new(1)),
            Value::RegClass(Oid::new(2)),
            Value::RegType(Oid::new(3)),
            Value::PgLsn(Lsn::new(0x1_0000_0002)),
            Value::Date(42),
            Value::Timestamp(123),
            Value::TimestampTz(456),
            Value::Time(789),
            Value::TimeTz {
                micros: 1_000,
                offset_seconds: -18_000,
            },
            Value::Interval {
                months: 2,
                days: 3,
                microseconds: 4,
            },
            Value::Uuid([7; 16]),
            Value::Bytea(vec![0, 1, 2, 255]),
            Value::BitString(BitString::parse("1010").expect("bit")),
            Value::BitString(BitString::parse("101011").expect("varbit")),
            Value::Network(
                NetworkValue::parse_for_type(&DataType::Inet, "192.168.1.10").expect("inet"),
            ),
            Value::Range(RangeValue::parse(RangeType::Int4, "[1,4)").expect("range")),
            Value::Geometry(GeometryValue::parse(GeometryType::Box, "((0,0),(2,3))").expect("box")),
        ];
        let encoded = codec.encode(&row).expect("encode");
        assert_eq!(codec.decode(&encoded).expect("decode"), row);
        assert_eq!(
            codec
                .decode_projected(&encoded, &[14, 0, 12])
                .expect("project"),
            vec![
                Value::Network(
                    NetworkValue::parse_for_type(&DataType::Inet, "192.168.1.10").expect("inet")
                ),
                Value::Oid(Oid::new(1)),
                Value::BitString(BitString::parse("1010").expect("bit")),
            ]
        );

        let mut builders = schema
            .fields()
            .iter()
            .map(|field| ColumnBuilder::new(&field.data_type, 1, 0).expect("builder"))
            .collect::<Vec<_>>();
        codec
            .decode_into_builders(&encoded, &mut builders)
            .expect("decode builders");
        let batch = RowCodec::finish_batch(builders).expect("finish");
        assert_eq!(batch.width(), schema.len());
        assert_eq!(batch.rows(), 1);
    }

    #[test]
    fn decode_projected_covers_varlena_catalog_network_and_vector_families() {
        let enum_type = DataType::Enum {
            oid: Oid::new(8_001),
            name: Arc::<str>::from("mood"),
            labels: Arc::from(vec!["happy".to_owned(), "sad".to_owned()].into_boxed_slice()),
        };
        let composite_type = DataType::Composite {
            oid: Oid::new(8_002),
            name: Arc::<str>::from("pair"),
            fields: Arc::from(
                vec![
                    ("id".to_owned(), DataType::Int32),
                    ("name".to_owned(), DataType::Text { max_len: None }),
                ]
                .into_boxed_slice(),
            ),
        };
        let schema = Schema::new([
            Field::required("b", DataType::Bool),
            Field::required("s", DataType::Int16),
            Field::required("f4", DataType::Float32),
            Field::required("f8", DataType::Float64),
            Field::required("enumv", enum_type.clone()),
            Field::required("comp", composite_type.clone()),
            Field::required("charv", DataType::Char { len: Some(4) }),
            Field::required("cidr", DataType::Cidr),
            Field::required("mac", DataType::MacAddr),
            Field::required("mac8", DataType::MacAddr8),
            Field::required("json", DataType::Json),
            Field::required("jsonb", DataType::Jsonb),
            Field::required("xml", DataType::Xml),
            Field::required("bytea", DataType::Bytea),
            Field::required("vector", DataType::Vector { dims: Some(2) }),
            Field::required("halfvec", DataType::HalfVec { dims: Some(2) }),
            Field::required("sparse", DataType::SparseVec { dims: Some(4) }),
            Field::required("bitvec", DataType::BitVec { dims: Some(8) }),
            Field::required(
                "array",
                DataType::Array(Box::new(DataType::Text { max_len: None })),
            ),
            Field::required("geom", DataType::Geometry(GeometryType::Point)),
        ])
        .unwrap();
        let row = vec![
            Value::Bool(false),
            Value::Int16(12),
            Value::Float32(3.5),
            Value::Float64(-4.5),
            Value::Text("happy".to_owned()),
            Value::Text("(1,foo)".to_owned()),
            Value::Char("xy  ".to_owned()),
            Value::Network(
                NetworkValue::parse_for_type(&DataType::Cidr, "192.168.0.0/24").expect("cidr"),
            ),
            Value::Network(
                NetworkValue::parse_for_type(&DataType::MacAddr, "08:00:2b:01:02:03").expect("mac"),
            ),
            Value::Network(
                NetworkValue::parse_for_type(&DataType::MacAddr8, "08:00:2b:01:02:03:04:05")
                    .expect("mac8"),
            ),
            Value::Json(r#"{"z":0}"#.to_owned()),
            Value::Jsonb(r#"{"a":1}"#.to_owned()),
            Value::Xml("<root/>".to_owned()),
            Value::Bytea(vec![1, 2, 3]),
            Value::Vector(vec![1.0, -1.0]),
            Value::HalfVec(vec![0.5, 2.0]),
            Value::SparseVec(SparseVector::new(4, vec![(1, 1.0), (3, -2.0)]).unwrap()),
            Value::BitVec {
                dims: 8,
                bytes: vec![0b1010_1100],
            },
            Value::Array {
                element_type: DataType::Text { max_len: None },
                elements: vec![Value::Text("a".to_owned()), Value::Text("b".to_owned())],
            },
            Value::Geometry(GeometryValue::parse(GeometryType::Point, "(1,2)").expect("point")),
        ];
        let codec = RowCodec::new(schema.clone());
        let encoded = codec.encode(&row).expect("encode");
        let all_columns = (0..schema.len()).collect::<Vec<_>>();
        assert_eq!(
            codec
                .decode_projected(&encoded, &all_columns)
                .expect("project all"),
            row
        );
        assert_eq!(
            codec.decode_projected(&encoded, &[19]).expect("skip all"),
            vec![Value::Geometry(
                GeometryValue::parse(GeometryType::Point, "(1,2)").expect("point")
            )]
        );
        assert!(codec.encode(&[Value::Text("angry".to_owned())]).is_err());
        let null_schema = Schema::new([Field::required("n", DataType::Null)]).expect("null");
        assert!(matches!(
            RowCodec::new(null_schema).encode(&[Value::Int32(1)]),
            Err(RowCodecError::Type { column: 0, .. })
                | Err(RowCodecError::UnsupportedType { column: 0, .. })
        ));
    }

    #[test]
    fn vector_family_encode_rejects_wrong_dimension() {
        for (data_type, value) in [
            (
                DataType::HalfVec { dims: Some(3) },
                Value::HalfVec(vec![1.0, 2.0]),
            ),
            (
                DataType::SparseVec { dims: Some(5) },
                Value::SparseVec(SparseVector::new(4, vec![(1, 1.0)]).unwrap()),
            ),
            (
                DataType::BitVec { dims: Some(8) },
                Value::BitVec {
                    dims: 7,
                    bytes: vec![0b1010_1010],
                },
            ),
        ] {
            let schema = Schema::new([Field::required("v", data_type)]).unwrap();
            let codec = RowCodec::new(schema);
            let err = codec.encode(&[value]).expect_err("dimension mismatch");
            assert!(matches!(err, RowCodecError::Type { column: 0, .. }));
        }
    }

    #[test]
    fn all_null_row() {
        let codec = RowCodec::new(schema_all_nullable());
        let row = vec![Value::Null, Value::Null];
        assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
    }
    #[test]
    fn mixed_nulls() {
        let codec = RowCodec::new(schema_mixed());
        let row = vec![Value::Null, Value::Text("alice".into()), Value::Null];
        assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
    }
    #[test]
    fn no_nulls_in_mixed_schema() {
        let codec = RowCodec::new(schema_mixed());
        let row = vec![
            Value::Int32(1),
            Value::Text("bob".into()),
            Value::Float64(9.9),
        ];
        assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
    }

    #[test]
    fn decode_projected_returns_requested_columns_in_output_order() {
        let codec = RowCodec::new(schema_mixed());
        let row = vec![Value::Int32(7), Value::Text("payload".into()), Value::Null];
        let bytes = codec.encode(&row).unwrap();

        assert_eq!(
            codec.decode_projected(&bytes, &[2, 0, 1, 1]).unwrap(),
            vec![
                Value::Null,
                Value::Int32(7),
                Value::Text("payload".into()),
                Value::Text("payload".into())
            ]
        );
    }

    #[test]
    fn finish_batch_auto_dictionary_encodes_low_cardinality_text() {
        let schema = schema_text();
        let codec = RowCodec::new(schema.clone());
        let mut builders =
            vec![ColumnBuilder::new(&schema.field_at(0).data_type, 2048, 0).unwrap()];

        for i in 0..2048 {
            let row = vec![Value::Text(format!("region{}", i % 4))];
            let bytes = codec.encode(&row).unwrap();
            codec.decode_into_builders(&bytes, &mut builders).unwrap();
        }

        let batch = RowCodec::finish_batch(builders).unwrap();
        match &batch.columns()[0] {
            Column::DictionaryUtf8(c) => {
                assert_eq!(c.len(), 2048);
                assert_eq!(c.dict.len(), 4);
                assert_eq!(c.decode_at(5), "region1");
            }
            other => panic!("expected dictionary text column, got {other:?}"),
        }
    }

    #[test]
    fn finish_batch_dictionary_text_preserves_nulls() {
        let schema = Schema::new([Field::nullable("s", DataType::Text { max_len: None })]).unwrap();
        let codec = RowCodec::new(schema.clone());
        let mut builders =
            vec![ColumnBuilder::new(&schema.field_at(0).data_type, 2048, 0).unwrap()];

        for i in 0..2048 {
            let row = if i % 8 == 0 {
                vec![Value::Null]
            } else {
                vec![Value::Text(format!("code{}", i % 3))]
            };
            let bytes = codec.encode(&row).unwrap();
            codec.decode_into_builders(&bytes, &mut builders).unwrap();
        }

        let batch = RowCodec::finish_batch(builders).unwrap();
        match &batch.columns()[0] {
            Column::DictionaryUtf8(c) => {
                let nulls = c.codes.nulls().expect("dictionary text should be nullable");
                assert!(!nulls.get(0));
                assert!(nulls.get(1));
                assert_eq!(c.decode_at(1), "code1");
            }
            other => panic!("expected nullable dictionary text column, got {other:?}"),
        }
    }

    #[test]
    fn arity_mismatch_on_encode_returns_arity_error() {
        let codec = RowCodec::new(schema_i32());
        let err = codec
            .encode(&[Value::Int32(1), Value::Int32(2)])
            .expect_err("arity mismatch");
        assert!(matches!(err, RowCodecError::Arity { schema: 1, row: 2 }));
    }
    #[test]
    fn arity_mismatch_empty_row_on_nonempty_schema() {
        let codec = RowCodec::new(schema_i32());
        let err = codec.encode(&[]).expect_err("arity mismatch");
        assert!(matches!(err, RowCodecError::Arity { schema: 1, row: 0 }));
    }
    #[test]
    fn truncated_payload_on_decode_returns_truncated_error() {
        let codec = RowCodec::new(schema_i32());
        let err = codec.decode(&[0x00, 0x01, 0x02]).expect_err("truncated");
        assert!(matches!(err, RowCodecError::Truncated { .. }));
    }
    #[test]
    fn empty_payload_on_nonempty_schema_returns_truncated() {
        let codec = RowCodec::new(schema_i32());
        let err = codec.decode(&[]).expect_err("truncated");
        assert!(matches!(err, RowCodecError::Truncated { .. }));
    }

    proptest! {
        #[test]
        fn prop_round_trip_i32(v: i32) {
            let codec = RowCodec::new(schema_i32());
            let row = vec![Value::Int32(v)];
            prop_assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
        }
        #[test]
        fn prop_round_trip_i64(v: i64) {
            let codec = RowCodec::new(schema_i64());
            let row = vec![Value::Int64(v)];
            prop_assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
        }
        #[test]
        fn prop_round_trip_text(s in ".*") {
            let codec = RowCodec::new(schema_text());
            let row = vec![Value::Text(s)];
            prop_assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
        }
        #[test]
        fn prop_round_trip_mixed(id: i32, name in "[a-zA-Z0-9]{0,32}", score: f64) {
            let codec = RowCodec::new(schema_mixed());
            let row = vec![Value::Int32(id), Value::Text(name), Value::Float64(score)];
            prop_assert_eq!(codec.decode(&codec.encode(&row).unwrap()).unwrap(), row);
        }
    }
}
