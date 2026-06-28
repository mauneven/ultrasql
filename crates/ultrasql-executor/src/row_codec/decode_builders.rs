//! Streaming decode directly into column builders.

use super::*;
use ultrasql_core::{DataType, Lsn, Value, format_interval_pg};
use ultrasql_vec::Batch;

impl RowCodec {
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
                (
                    DataType::Decimal { .. },
                    ColumnBuilder::Utf8 {
                        offsets,
                        values,
                        nulls,
                    },
                ) => {
                    // Decimal columns materialise as decimal text so the
                    // full i128-backed mantissa round-trips losslessly.
                    let value =
                        decode_numeric_value(bytes, &mut cursor, col_idx, &field.data_type)?;
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
                    // Materialize PostgreSQL-canonical interval text so the
                    // streaming path matches the batch path and the result
                    // encoder emits libpq-readable interval for OID 1186.
                    let text = format_interval_pg(
                        i32::from_le_bytes(months_raw),
                        i32::from_le_bytes(days_raw),
                        i64::from_le_bytes(micros_raw),
                    );
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
    /// [`RowCodecError`] if builder null bitmaps, text offsets, or final
    /// batch columns violate their length invariants.
    pub(crate) fn finish_batch(builders: Vec<ColumnBuilder>) -> Result<Batch, RowCodecError> {
        Batch::new(finish_builders(builders)?).map_err(RowCodecError::from)
    }

    /// Inject an `Int32` into `builders[col_idx]`. Used to prepend TID
    /// columns in the scan operator.
    pub(crate) fn push_i32_into(builders: &mut [ColumnBuilder], col_idx: usize, v: i32) {
        builders[col_idx].push_i32(v);
    }
}
