//! Variable-length, vector, text, and per-type value codec helpers.

use super::{
    RowCodecError, VECTOR_DIMS_WIDTH, VECTOR_ELEMENT_WIDTH, checked_fixed_end, checked_payload_end,
    u32_payload_len_to_usize,
};
use ultrasql_core::{DataType, MAX_VECTOR_DIMS, Value};

pub(crate) fn decode_bit_string_value(
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

pub(crate) fn decode_network_value(
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

pub(crate) fn enum_label_is_valid(labels: &[String], value: &str) -> bool {
    labels.iter().any(|label| label == value)
}

pub(crate) const fn is_supported_type(ty: &DataType) -> bool {
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

pub(crate) fn encode_vector_payload(
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

pub(crate) fn encode_dense_vector_family_text(
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

pub(crate) fn encode_dimensioned_value_text(
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

pub(crate) fn decode_text_vector_family_value(
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

pub(crate) fn read_fixed<const N: usize>(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<[u8; N], RowCodecError> {
    let needed = checked_fixed_end(*cursor, N, bytes.len())?;
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

pub(crate) fn skip_fixed(
    bytes: &[u8],
    cursor: &mut usize,
    width: usize,
) -> Result<(), RowCodecError> {
    let needed = checked_fixed_end(*cursor, width, bytes.len())?;
    if bytes.len() < needed {
        return Err(RowCodecError::Truncated {
            needed,
            have: bytes.len(),
        });
    }
    *cursor = needed;
    Ok(())
}

pub(crate) fn decode_varlena_bytes(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<Vec<u8>, RowCodecError> {
    let payload = read_varlena_slice(bytes, cursor)?;
    Ok(payload.to_vec())
}

pub(crate) fn skip_varlena_payload(bytes: &[u8], cursor: &mut usize) -> Result<(), RowCodecError> {
    let _ = read_varlena_slice(bytes, cursor)?;
    Ok(())
}

pub(crate) fn read_varlena_slice<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
) -> Result<&'a [u8], RowCodecError> {
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

pub(crate) fn skip_vector_value(
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

pub(crate) const fn ty_name_for_dimension_error(ty: &DataType) -> &'static str {
    match ty {
        DataType::Vector { .. } => "vector",
        DataType::HalfVec { .. } => "halfvec",
        DataType::SparseVec { .. } => "sparsevec",
        DataType::BitVec { .. } => "bitvec",
        _ => "value",
    }
}

pub(crate) fn decode_vector_value(
    bytes: &[u8],
    cursor: &mut usize,
    expected_dims: Option<u32>,
    column: usize,
    ty: &DataType,
) -> Result<Value, RowCodecError> {
    let dims_end = checked_fixed_end(*cursor, VECTOR_DIMS_WIDTH, bytes.len())?;
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
        let raw: [u8; VECTOR_ELEMENT_WIDTH] =
            chunk.try_into().map_err(|_| RowCodecError::Type {
                column,
                expected: ty.clone(),
                got: "invalid vector element width".to_owned(),
            })?;
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

pub(crate) fn encode_varlena_text(
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

pub(crate) fn validate_varchar_storage_text(
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

pub(crate) fn normalize_jsonb_storage_text(
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

pub(crate) fn validate_json_storage_text(
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

pub(crate) fn validate_xml_storage_text(
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

pub(crate) fn decode_varlena_text(
    bytes: &[u8],
    cursor: &mut usize,
    context: &'static str,
) -> Result<String, RowCodecError> {
    let len_raw = read_fixed::<4>(bytes, cursor)?;
    let str_len = u32_payload_len_to_usize(u32::from_le_bytes(len_raw))?;
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
