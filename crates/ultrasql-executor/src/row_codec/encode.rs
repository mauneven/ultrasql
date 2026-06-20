//! Codec construction, schema accessors, and row encoding.

use super::*;
use ultrasql_core::{
    DataType, Schema, Value, coerce_bpchar_text, composite_text_matches_arity, pack_timetz,
};

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
}
