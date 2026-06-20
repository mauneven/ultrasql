//! Row decoding into `Vec<Value>`.

use super::*;
use ultrasql_core::{DataType, GeometryValue, Lsn, Oid, RangeValue, Value, unpack_timetz};

impl RowCodec {
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
}
