//! Per-value decode and skip helpers for the generic decode path.

use super::*;
use ultrasql_core::{DataType, GeometryValue, Lsn, Oid, RangeValue, Value, unpack_timetz};

impl RowCodec {
    pub(super) fn decode_one_value(
        bytes: &[u8],
        cursor: &mut usize,
        col_idx: usize,
        data_type: &DataType,
    ) -> Result<Value, RowCodecError> {
        match data_type {
            DataType::Bool => {
                let needed = checked_fixed_end(*cursor, 1, bytes.len())?;
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

    pub(super) fn skip_one_value(
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
}
