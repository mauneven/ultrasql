//! Row-level binary codec used by the storage path of the executor.
//!
//! Encodes a `Vec<Value>` matching a `Schema` to a tightly-packed byte
//! buffer suitable for use as the `payload` of a heap tuple. The codec
//! is the inverse of `decode` and is stable for v0.5.
//!
//! Wire format
//! -----------
//!
//! ```text
//!  null_bitmap (ceil(n / 8) bytes, LSB-first per column)
//!  field_0 …    fixed-width fields in schema order, little-endian
//!  field_n      variable-width fields: u32 length prefix + bytes
//! ```
//!
//! Only NOT-NULL fields' bytes appear (a column whose bitmap bit is set
//! contributes zero bytes). Nullability is determined per-row, not
//! per-schema.
//!
//! Supported types in v0.5
//! -----------------------
//!
//! - `DataType::Bool`         — 1 byte
//! - `DataType::Int16`        — 2 bytes (LE)
//! - `DataType::Int32`        — 4 bytes (LE)
//! - `DataType::Int64`        — 8 bytes (LE)
//! - `DataType::Float32`      — 4 bytes (LE)
//! - `DataType::Float64`      — 8 bytes (LE)
//! - `DataType::Text { .. }`  — u32 length-prefixed UTF-8
//! - `DataType::Null`         — column-level marker; rows always encode the bitmap bit
//!
//! Unsupported types return [`RowCodecError::UnsupportedType`] in encode
//! and decode. Adding a type is straightforward; do not silently accept
//! a type the codec can't round-trip.

use ultrasql_core::{DataType, Schema, Value};

/// Binary codec bound to a fixed [`Schema`].
///
/// The schema determines the expected column count and types. A single
/// `RowCodec` instance is safe to reuse for many rows — the internal
/// encoding/decoding state is entirely stack-allocated.
#[derive(Clone, Debug)]
pub struct RowCodec {
    schema: Schema,
}

impl RowCodec {
    /// Bind a codec to `schema`.
    ///
    /// Every subsequent `encode`/`decode` call is checked against this schema.
    #[must_use]
    pub const fn new(schema: Schema) -> Self {
        Self { schema }
    }

    /// The schema this codec was bound to.
    #[must_use]
    pub const fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Encode `row` into a byte payload.
    ///
    /// The row must have the same arity as the codec's schema and each
    /// value must be type-compatible with the corresponding field.
    ///
    /// # Errors
    ///
    /// - [`RowCodecError::Arity`] — `row.len() != schema.len()`.
    /// - [`RowCodecError::Type`] — a value's runtime type does not
    ///   match the schema field's declared type.
    /// - [`RowCodecError::UnsupportedType`] — the field's `DataType`
    ///   is not in the v0.5 supported set.
    pub fn encode(&self, row: &[Value]) -> Result<Vec<u8>, RowCodecError> {
        let n = self.schema.len();
        if row.len() != n {
            return Err(RowCodecError::Arity {
                schema: n,
                row: row.len(),
            });
        }

        // Null bitmap: ceil(n / 8) bytes, LSB = column 0.
        let bitmap_bytes = n.div_ceil(8);
        let mut bitmap = vec![0_u8; bitmap_bytes];
        let mut payload: Vec<u8> = Vec::new();

        for (col_idx, (value, field)) in row.iter().zip(self.schema.fields().iter()).enumerate() {
            if matches!(value, Value::Null) {
                // Set the null bit; no data bytes contributed.
                let byte = col_idx / 8;
                let bit = col_idx % 8;
                bitmap[byte] |= 1 << bit;
                continue;
            }

            // Non-null: check type compatibility and append data bytes.
            match (&field.data_type, value) {
                (DataType::Bool, Value::Bool(v)) => {
                    payload.push(u8::from(*v));
                }
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
                (DataType::Null, _) => {
                    // DataType::Null columns are always encoded as null;
                    // if we reach here the value is not Value::Null which
                    // is a type mismatch.
                    return Err(RowCodecError::Type {
                        column: col_idx,
                        expected: field.data_type.clone(),
                        got: value.data_type().to_string(),
                    });
                }
                (expected, got) => {
                    // Check for unsupported type first (takes priority over
                    // type-mismatch so the caller sees a clear message).
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

        // Prepend the bitmap.
        let mut out = bitmap;
        out.extend_from_slice(&payload);
        Ok(out)
    }

    /// Decode a byte payload previously produced by [`Self::encode`] back
    /// into a `Vec<Value>`.
    ///
    /// # Errors
    ///
    /// - [`RowCodecError::Truncated`] — the buffer is shorter than the
    ///   bitmap or any field's data.
    /// - [`RowCodecError::UnsupportedType`] — a field's `DataType` is not
    ///   in the v0.5 supported set.
    /// - [`RowCodecError::InvalidUtf8`] — a `Text` field contains
    ///   invalid UTF-8.
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
            // Check null bit.
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
                DataType::Text { .. } => {
                    // u32 length prefix
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
                DataType::Null => {
                    // DataType::Null columns are always null in the bitmap;
                    // if the null bit was zero something is wrong. Treat as
                    // an unsupported-type decode (the bitmap should have had
                    // the bit set).
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
}

/// Whether `ty` is in the v0.5 supported set for encoding.
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
            | DataType::Null
    )
}

/// Errors raised by [`RowCodec`].
#[derive(Debug, thiserror::Error)]
pub enum RowCodecError {
    /// The row's column count does not match the schema.
    #[error("arity mismatch: schema has {schema}, row has {row}")]
    Arity {
        /// Number of columns declared in the schema.
        schema: usize,
        /// Number of values in the row supplied by the caller.
        row: usize,
    },

    /// A value's runtime type does not match the schema field's declared
    /// type.
    #[error("type mismatch at column {column}: expected {expected}, got {got}")]
    Type {
        /// Zero-based column index.
        column: usize,
        /// The schema's declared type for this column.
        expected: DataType,
        /// Human-readable name of the actual value type.
        got: String,
    },

    /// The payload is shorter than the codec's minimum expectation.
    #[error("payload truncated: needed {needed}, have {have}")]
    Truncated {
        /// Minimum number of bytes required at the current cursor position.
        needed: usize,
        /// Actual length of the supplied buffer.
        have: usize,
    },

    /// The schema contains a type this codec version cannot handle.
    #[error("unsupported type at column {column}: {ty}")]
    UnsupportedType {
        /// Zero-based column index.
        column: usize,
        /// The unsupported type.
        ty: DataType,
    },

    /// A `Text` field contained bytes that are not valid UTF-8.
    #[error("invalid utf8 at column {1}: {0}")]
    InvalidUtf8(#[source] std::string::FromUtf8Error, &'static str),
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use ultrasql_core::{DataType, Field, Schema, Value};

    use super::{RowCodec, RowCodecError};

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn schema_bool() -> Schema {
        Schema::new([Field::required("b", DataType::Bool)]).expect("schema ok")
    }

    fn schema_i16() -> Schema {
        Schema::new([Field::required("n", DataType::Int16)]).expect("schema ok")
    }

    fn schema_i32() -> Schema {
        Schema::new([Field::required("n", DataType::Int32)]).expect("schema ok")
    }

    fn schema_i64() -> Schema {
        Schema::new([Field::required("n", DataType::Int64)]).expect("schema ok")
    }

    fn schema_f32() -> Schema {
        Schema::new([Field::required("f", DataType::Float32)]).expect("schema ok")
    }

    fn schema_f64() -> Schema {
        Schema::new([Field::required("f", DataType::Float64)]).expect("schema ok")
    }

    fn schema_text() -> Schema {
        Schema::new([Field::required("s", DataType::Text { max_len: None })]).expect("schema ok")
    }

    fn schema_mixed() -> Schema {
        Schema::new([
            Field::nullable("id", DataType::Int32),
            Field::required("name", DataType::Text { max_len: None }),
            Field::nullable("score", DataType::Float64),
        ])
        .expect("schema ok")
    }

    fn schema_all_nullable() -> Schema {
        Schema::new([
            Field::nullable("a", DataType::Int32),
            Field::nullable("b", DataType::Text { max_len: None }),
        ])
        .expect("schema ok")
    }

    // -----------------------------------------------------------------------
    // Round-trip tests for each supported type
    // -----------------------------------------------------------------------

    #[test]
    fn round_trip_bool_true() {
        let codec = RowCodec::new(schema_bool());
        let row = vec![Value::Bool(true)];
        let bytes = codec.encode(&row).expect("encode");
        let decoded = codec.decode(&bytes).expect("decode");
        assert_eq!(decoded, row);
    }

    #[test]
    fn round_trip_bool_false() {
        let codec = RowCodec::new(schema_bool());
        let row = vec![Value::Bool(false)];
        let bytes = codec.encode(&row).expect("encode");
        let decoded = codec.decode(&bytes).expect("decode");
        assert_eq!(decoded, row);
    }

    #[test]
    fn round_trip_int16() {
        let codec = RowCodec::new(schema_i16());
        for v in [i16::MIN, -1, 0, 1, i16::MAX] {
            let row = vec![Value::Int16(v)];
            let bytes = codec.encode(&row).expect("encode");
            let decoded = codec.decode(&bytes).expect("decode");
            assert_eq!(decoded, row, "round-trip failed for i16={v}");
        }
    }

    #[test]
    fn round_trip_int32() {
        let codec = RowCodec::new(schema_i32());
        for v in [i32::MIN, -42, 0, 42, i32::MAX] {
            let row = vec![Value::Int32(v)];
            let bytes = codec.encode(&row).expect("encode");
            let decoded = codec.decode(&bytes).expect("decode");
            assert_eq!(decoded, row, "round-trip failed for i32={v}");
        }
    }

    #[test]
    fn round_trip_int64() {
        let codec = RowCodec::new(schema_i64());
        for v in [i64::MIN, -1, 0, 1, i64::MAX] {
            let row = vec![Value::Int64(v)];
            let bytes = codec.encode(&row).expect("encode");
            let decoded = codec.decode(&bytes).expect("decode");
            assert_eq!(decoded, row, "round-trip failed for i64={v}");
        }
    }

    #[test]
    fn round_trip_float32() {
        let codec = RowCodec::new(schema_f32());
        for v in [f32::NEG_INFINITY, -1.5, 0.0, 1.5, f32::INFINITY] {
            let row = vec![Value::Float32(v)];
            let bytes = codec.encode(&row).expect("encode");
            let decoded = codec.decode(&bytes).expect("decode");
            assert_eq!(decoded, row, "round-trip failed for f32={v}");
        }
    }

    #[test]
    fn round_trip_float64() {
        let codec = RowCodec::new(schema_f64());
        for v in [f64::NEG_INFINITY, -1.5, 0.0, 1.5, f64::INFINITY] {
            let row = vec![Value::Float64(v)];
            let bytes = codec.encode(&row).expect("encode");
            let decoded = codec.decode(&bytes).expect("decode");
            assert_eq!(decoded, row, "round-trip failed for f64={v}");
        }
    }

    #[test]
    fn round_trip_text() {
        let codec = RowCodec::new(schema_text());
        for s in ["", "hello", "unicode: \u{1F600}", &"x".repeat(1024)] {
            let row = vec![Value::Text(s.to_owned())];
            let bytes = codec.encode(&row).expect("encode");
            let decoded = codec.decode(&bytes).expect("decode");
            assert_eq!(decoded, row, "round-trip failed for text={s:?}");
        }
    }

    // -----------------------------------------------------------------------
    // Null handling
    // -----------------------------------------------------------------------

    #[test]
    fn all_null_row() {
        let codec = RowCodec::new(schema_all_nullable());
        let row = vec![Value::Null, Value::Null];
        let bytes = codec.encode(&row).expect("encode");
        let decoded = codec.decode(&bytes).expect("decode");
        assert_eq!(decoded, row);
    }

    #[test]
    fn mixed_nulls() {
        let codec = RowCodec::new(schema_mixed());
        let row = vec![Value::Null, Value::Text("alice".into()), Value::Null];
        let bytes = codec.encode(&row).expect("encode");
        let decoded = codec.decode(&bytes).expect("decode");
        assert_eq!(decoded, row);
    }

    #[test]
    fn no_nulls_in_mixed_schema() {
        let codec = RowCodec::new(schema_mixed());
        let row = vec![
            Value::Int32(1),
            Value::Text("bob".into()),
            Value::Float64(9.9),
        ];
        let bytes = codec.encode(&row).expect("encode");
        let decoded = codec.decode(&bytes).expect("decode");
        assert_eq!(decoded, row);
    }

    // -----------------------------------------------------------------------
    // Negative: arity mismatch
    // -----------------------------------------------------------------------

    #[test]
    fn arity_mismatch_on_encode_returns_arity_error() {
        let codec = RowCodec::new(schema_i32());
        let row = vec![Value::Int32(1), Value::Int32(2)]; // too many
        let err = codec.encode(&row).expect_err("arity mismatch must fail");
        assert!(
            matches!(err, RowCodecError::Arity { schema: 1, row: 2 }),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn arity_mismatch_empty_row_on_nonempty_schema() {
        let codec = RowCodec::new(schema_i32());
        let err = codec.encode(&[]).expect_err("arity mismatch must fail");
        assert!(
            matches!(err, RowCodecError::Arity { schema: 1, row: 0 }),
            "unexpected error: {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Negative: truncated payload on decode
    // -----------------------------------------------------------------------

    #[test]
    fn truncated_payload_on_decode_returns_truncated_error() {
        let codec = RowCodec::new(schema_i32());
        // A valid payload has 1 bitmap byte + 4 data bytes = 5 bytes.
        // Pass only 3 bytes — too short for the data.
        let err = codec
            .decode(&[0x00, 0x01, 0x02])
            .expect_err("truncated must fail");
        assert!(
            matches!(err, RowCodecError::Truncated { .. }),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn empty_payload_on_nonempty_schema_returns_truncated() {
        let codec = RowCodec::new(schema_i32());
        let err = codec.decode(&[]).expect_err("truncated must fail");
        assert!(
            matches!(err, RowCodecError::Truncated { .. }),
            "unexpected error: {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Property test: encode → decode identity
    // -----------------------------------------------------------------------

    proptest! {
        #[test]
        fn prop_round_trip_i32(v: i32) {
            let codec = RowCodec::new(schema_i32());
            let row = vec![Value::Int32(v)];
            let bytes = codec.encode(&row).expect("encode");
            let decoded = codec.decode(&bytes).expect("decode");
            prop_assert_eq!(decoded, row);
        }

        #[test]
        fn prop_round_trip_i64(v: i64) {
            let codec = RowCodec::new(schema_i64());
            let row = vec![Value::Int64(v)];
            let bytes = codec.encode(&row).expect("encode");
            let decoded = codec.decode(&bytes).expect("decode");
            prop_assert_eq!(decoded, row);
        }

        #[test]
        fn prop_round_trip_text(s in ".*") {
            let codec = RowCodec::new(schema_text());
            let row = vec![Value::Text(s)];
            let bytes = codec.encode(&row).expect("encode");
            let decoded = codec.decode(&bytes).expect("decode");
            prop_assert_eq!(decoded, row);
        }

        #[test]
        fn prop_round_trip_mixed(id: i32, name in "[a-zA-Z0-9]{0,32}", score: f64) {
            let codec = RowCodec::new(schema_mixed());
            let row = vec![
                Value::Int32(id),
                Value::Text(name),
                Value::Float64(score),
            ];
            let bytes = codec.encode(&row).expect("encode");
            let decoded = codec.decode(&bytes).expect("decode");
            prop_assert_eq!(decoded, row);
        }
    }
}
