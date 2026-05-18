//! Row-level binary codec used by the storage path of the executor.
//!
//! Encodes a `Vec<Value>` matching a `Schema` to a tightly-packed byte
//! buffer suitable for use as the `payload` of a heap tuple. The codec
//! is the inverse of `decode` and is stable for v0.5.
//!
//! Streaming decode (v0.6)
//! -----------------------
//!
//! [`RowCodec::decode_into_builders`] decodes a tuple's bytes
//! directly into a parallel slice of [`ColumnBuilder`]s, skipping the
//! `Vec<Value>` row intermediate.

use ultrasql_core::{DataType, GeometryValue, RangeValue, Schema, Value};
use ultrasql_vec::bitmap::Bitmap;
use ultrasql_vec::column::{BoolColumn, Column, NumericColumn};
use ultrasql_vec::{Batch, DictionaryEncodingPolicy, StringEncoding, encode_strings_auto};

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
        let types: Vec<&DataType> = fields.iter().map(|f| &f.data_type).collect();
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
                let raw: [u8; 8] = bytes[1..9].try_into().expect("len checked above");
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
                let r0: [u8; 8] = bytes[1..9].try_into().expect("len checked above");
                let r1: [u8; 8] = bytes[9..17].try_into().expect("len checked above");
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
            match (&field.data_type, value) {
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
                (DataType::Decimal { .. }, Value::Decimal { value, .. }) => {
                    // Decimal storage: scaled i64 value, 8 bytes LE.
                    // Per-column scale lives in the schema; the value
                    // payload is the scaled integer.
                    payload.extend_from_slice(&value.to_le_bytes());
                }
                (DataType::Timestamp, Value::Timestamp(v))
                | (DataType::TimestampTz, Value::TimestampTz(v))
                | (DataType::Time, Value::Time(v)) => {
                    // Microsecond-precision temporal: 8 bytes LE i64
                    // (microseconds since 2000-01-01 for Timestamp/Tz,
                    // microseconds since midnight for Time).
                    payload.extend_from_slice(&v.to_le_bytes());
                }
                (DataType::Text { .. }, Value::Text(s)) => {
                    let bytes = s.as_bytes();
                    let len =
                        u32::try_from(bytes.len()).map_err(|_| RowCodecError::UnsupportedType {
                            column: col_idx,
                            ty: field.data_type.clone(),
                        })?;
                    payload.extend_from_slice(&len.to_le_bytes());
                    payload.extend_from_slice(bytes);
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
            let value = match &field.data_type {
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
                DataType::Decimal { scale, .. } => {
                    // Decimal storage: 8-byte little-endian scaled
                    // i64 payload. Per-column scale lives in the
                    // schema field; the codec reads it back here.
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
                    Value::Decimal {
                        value: i64::from_le_bytes(raw),
                        scale: scale.unwrap_or(0),
                    }
                }
                DataType::Timestamp | DataType::TimestampTz | DataType::Time => {
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
                    match field.data_type {
                        DataType::Timestamp => Value::Timestamp(v),
                        DataType::TimestampTz => Value::TimestampTz(v),
                        DataType::Time => Value::Time(v),
                        _ => unreachable!(),
                    }
                }
                DataType::Text { .. } => {
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
                    let str_len = usize::try_from(u32::from_le_bytes(len_raw))
                        .expect("u32 fits in usize on all supported targets");
                    cursor += 4;
                    let str_end = cursor + str_len;
                    if bytes.len() < str_end {
                        return Err(RowCodecError::Truncated {
                            needed: str_end,
                            have: bytes.len(),
                        });
                    }
                    let s = String::from_utf8(bytes[cursor..str_end].to_vec())
                        .map_err(|e| RowCodecError::InvalidUtf8(e, "text column"))?;
                    cursor += str_len;
                    Value::Text(s)
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
            out.push(ColumnBuilder::new(&field.data_type, capacity, idx)?);
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
            match (&field.data_type, &mut builders[col_idx]) {
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
                (DataType::Decimal { .. }, ColumnBuilder::Int64 { data, nulls })
                | (DataType::Timestamp, ColumnBuilder::Int64 { data, nulls })
                | (DataType::TimestampTz, ColumnBuilder::Int64 { data, nulls })
                | (DataType::Time, ColumnBuilder::Int64 { data, nulls }) => {
                    // Decimal / Timestamp / Time share the Int64
                    // builder; the schema carries the scale (for
                    // Decimal) and the semantic tag for the
                    // surrounding executor.
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
                    DataType::Text { .. } | DataType::Range(_) | DataType::Geometry(_),
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
                    let str_len = usize::try_from(u32::from_le_bytes(len_raw))
                        .expect("u32 fits in usize on all supported targets");
                    cursor += 4;
                    let str_end = cursor + str_len;
                    if bytes.len() < str_end {
                        return Err(RowCodecError::Truncated {
                            needed: str_end,
                            have: bytes.len(),
                        });
                    }
                    if std::str::from_utf8(&bytes[cursor..str_end]).is_err() {
                        return Err(RowCodecError::InvalidUtf8(
                            String::from_utf8(bytes[cursor..str_end].to_vec())
                                .expect_err("just observed invalid utf8"),
                            "text column",
                        ));
                    }
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
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::Time => Self::Int64 {
                // `Decimal` / `Timestamp` / `Time` storage shares the
                // `Int64` builder; the schema field carries the
                // semantic tag and (for Decimal) the scale.
                data: Vec::with_capacity(capacity),
                nulls: NullTracker::default(),
            },
            DataType::Text { .. } | DataType::Range(_) | DataType::Geometry(_) => Self::Utf8 {
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
            ColumnBuilder::Bool { data, nulls: _ } => {
                let bools: Vec<bool> = data.iter().map(|&b| b != 0).collect();
                Column::Bool(BoolColumn::from_data(bools))
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
            | DataType::Date
            | DataType::Time
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::Decimal { .. }
            | DataType::Range(_)
            | DataType::Geometry(_)
            | DataType::Null
    )
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
    let str_len =
        usize::try_from(u32::from_le_bytes(len_raw)).expect("u32 fits in usize on all targets");
    *cursor += 4;
    let str_end = *cursor + str_len;
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
    /// Truncated payload.
    #[error("payload truncated: needed {needed}, have {have}")]
    Truncated {
        /// Required byte count.
        needed: usize,
        /// Actual byte count.
        have: usize,
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
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use ultrasql_core::{DataType, Field, Schema, Value};

    use super::{ColumnBuilder, RowCodec, RowCodecError};
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
