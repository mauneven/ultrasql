//! PostgreSQL binary COPY (`PGCOPY`) wire encoding and decoding.
//!
//! Serializes table rows into the binary COPY stream format and parses the
//! reverse direction back into typed [`Value`]s, including the file header,
//! per-row field framing, and per-type binary cell codecs.

use ultrasql_catalog::TableEntry;
use ultrasql_core::{
    BitString, DataType, NetworkValue, Schema, Value, coerce_bpchar_text, decode_pg_money_binary,
    decode_pg_numeric_binary, encode_pg_money_binary, encode_pg_numeric_binary,
};
use ultrasql_executor::RowCodec;

use super::super::jsonb_ingest::{JsonbShapeCache, encode_pg_binary_jsonb, parse_json_text};
use super::ServerError;
use super::decode::{decode_copy_cell, parse_xml_text};

pub(super) fn append_binary_copy_header(out: &mut Vec<u8>) {
    out.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    out.extend_from_slice(&0_i32.to_be_bytes());
    out.extend_from_slice(&0_i32.to_be_bytes());
}

pub(super) fn append_binary_copy_row(
    out: &mut Vec<u8>,
    row: &[Value],
    table_schema: &Schema,
    columns: &[usize],
    stream_schema: &Schema,
) -> Result<(), ServerError> {
    append_i16_be(
        out,
        i16::try_from(stream_schema.len())
            .map_err(|_| ServerError::CopyFormat("too many COPY columns".to_string()))?,
    );
    if columns.is_empty() {
        for (idx, value) in row.iter().enumerate() {
            append_binary_copy_cell(out, value, &table_schema.field_at(idx).data_type)?;
        }
    } else {
        for &idx in columns {
            let value = row.get(idx).unwrap_or(&Value::Null);
            append_binary_copy_cell(out, value, &table_schema.field_at(idx).data_type)?;
        }
    }
    Ok(())
}

fn append_binary_copy_cell(
    out: &mut Vec<u8>,
    value: &Value,
    dtype: &DataType,
) -> Result<(), ServerError> {
    if matches!(value, Value::Null) {
        out.extend_from_slice(&(-1_i32).to_be_bytes());
        return Ok(());
    }
    let bytes = binary_copy_cell_bytes(value, dtype)?;
    out.extend_from_slice(
        &i32::try_from(bytes.len())
            .map_err(|_| ServerError::CopyFormat("binary COPY cell too large".to_string()))?
            .to_be_bytes(),
    );
    out.extend_from_slice(&bytes);
    Ok(())
}

pub(super) fn binary_copy_cell_bytes(
    value: &Value,
    dtype: &DataType,
) -> Result<Vec<u8>, ServerError> {
    let bytes = match (dtype, value) {
        (DataType::Bool, Value::Bool(v)) => vec![u8::from(*v)],
        (DataType::Int16, Value::Int16(v)) => v.to_be_bytes().to_vec(),
        (DataType::Int32, Value::Int32(v)) => v.to_be_bytes().to_vec(),
        (DataType::Int64, Value::Int64(v)) => v.to_be_bytes().to_vec(),
        (DataType::Money, Value::Money(v) | Value::Int64(v)) => encode_pg_money_binary(*v).to_vec(),
        (DataType::Float32, Value::Float32(v)) => v.to_bits().to_be_bytes().to_vec(),
        (DataType::Float64, Value::Float64(v)) => v.to_bits().to_be_bytes().to_vec(),
        (DataType::Date, Value::Date(v) | Value::Int32(v)) => v.to_be_bytes().to_vec(),
        (DataType::Time, Value::Time(v) | Value::Int64(v))
        | (DataType::Timestamp, Value::Timestamp(v) | Value::Int64(v))
        | (DataType::TimestampTz, Value::TimestampTz(v) | Value::Int64(v)) => {
            v.to_be_bytes().to_vec()
        }
        (
            DataType::TimeTz,
            Value::TimeTz {
                micros,
                offset_seconds,
            },
        ) => {
            let mut out = Vec::with_capacity(12);
            out.extend_from_slice(&micros.to_be_bytes());
            // PostgreSQL's binary timetz encodes the zone as seconds WEST of
            // UTC; our internal `offset_seconds` is east-positive, so negate
            // to match the wire convention.
            out.extend_from_slice(&offset_seconds.wrapping_neg().to_be_bytes());
            out
        }
        (DataType::Decimal { .. }, Value::Decimal { value, scale }) => {
            encode_pg_numeric_binary(*value, *scale)
                .map_err(|err| ServerError::CopyFormat(format!("binary COPY numeric: {err}")))?
        }
        (DataType::Decimal { scale, .. }, Value::Int64(v)) => {
            encode_pg_numeric_binary(*v, scale.unwrap_or(0))
                .map_err(|err| ServerError::CopyFormat(format!("binary COPY numeric: {err}")))?
        }
        (DataType::Text { .. } | DataType::TsVector | DataType::TsQuery, Value::Text(v))
        | (DataType::Char { .. }, Value::Char(v)) => v.as_bytes().to_vec(),
        (DataType::Bit { .. } | DataType::VarBit { .. }, Value::BitString(bits)) => {
            bits.to_pg_binary()
        }
        (
            DataType::Inet | DataType::Cidr | DataType::MacAddr | DataType::MacAddr8,
            Value::Network(network),
        ) if network.data_type() == dtype.clone() => network.to_pg_binary(),
        (DataType::Json, Value::Json(v)) => v.as_bytes().to_vec(),
        (DataType::Jsonb, Value::Jsonb(v)) => encode_pg_binary_jsonb(v),
        (DataType::Xml, Value::Xml(v)) => v.as_bytes().to_vec(),
        (DataType::Bytea, Value::Bytea(v)) => v.clone(),
        (DataType::Uuid, Value::Uuid(v)) => v.to_vec(),
        (_, other) => other.to_string().into_bytes(),
    };
    Ok(bytes)
}

pub(super) fn append_i16_be(out: &mut Vec<u8>, v: i16) {
    out.extend_from_slice(&v.to_be_bytes());
}

const BINARY_COPY_CRITICAL_FLAGS_MASK: u32 = 0xFFFF_0000;

pub(super) fn decode_binary_copy_payload(
    bytes: &[u8],
    entry: &TableEntry,
    columns: &[usize],
    schema: &Schema,
    codec: &RowCodec,
    jsonb_shape_cache: &mut JsonbShapeCache,
) -> Result<Vec<Vec<u8>>, ServerError> {
    const MAGIC: &[u8] = b"PGCOPY\n\xff\r\n\0";
    if bytes.len() < MAGIC.len() + 8 || &bytes[..MAGIC.len()] != MAGIC {
        return Err(ServerError::CopyFormat(
            "invalid binary COPY header".to_string(),
        ));
    }
    let mut pos = MAGIC.len();
    let flags = u32::from_be_bytes(read_i32_be(bytes, &mut pos)?.to_be_bytes());
    if flags & BINARY_COPY_CRITICAL_FLAGS_MASK != 0 {
        return Err(ServerError::CopyFormat(format!(
            "unsupported binary COPY critical flags: {flags:#010x}"
        )));
    }
    let ext_len = read_i32_be(bytes, &mut pos)?;
    if ext_len < 0 {
        return Err(ServerError::CopyFormat(
            "invalid binary COPY extension length".to_string(),
        ));
    }
    let ext_len = usize::try_from(ext_len)
        .map_err(|_| ServerError::CopyFormat("invalid binary COPY extension".to_string()))?;
    pos = binary_copy_end(pos, ext_len, bytes.len(), "binary COPY extension")?;

    let mut payloads = Vec::new();
    loop {
        let field_count = read_i16_be(bytes, &mut pos)?;
        if field_count == -1 {
            break;
        }
        let expected = i16::try_from(schema.len())
            .map_err(|_| ServerError::CopyFormat("too many COPY columns".to_string()))?;
        if field_count != expected {
            return Err(ServerError::CopyFormat(format!(
                "binary COPY expected {expected} columns, got {field_count}"
            )));
        }
        let mut row = vec![Value::Null; entry.schema.len()];
        for stream_idx in 0..usize::try_from(field_count).unwrap_or(0) {
            let len = read_i32_be(bytes, &mut pos)?;
            let value = if len == -1 {
                Value::Null
            } else {
                if len < 0 {
                    return Err(ServerError::CopyFormat(
                        "invalid binary COPY field length".to_string(),
                    ));
                }
                let len = usize::try_from(len).map_err(|_| {
                    ServerError::CopyFormat("invalid binary COPY field length".to_string())
                })?;
                let end = binary_copy_end(pos, len, bytes.len(), "binary COPY field")?;
                let target_idx = columns.get(stream_idx).copied().unwrap_or(stream_idx);
                let dtype = &entry.schema.field_at(target_idx).data_type;
                let value = decode_binary_copy_cell(
                    &bytes[pos..end],
                    dtype,
                    stream_idx,
                    jsonb_shape_cache,
                )?;
                pos = end;
                value
            };
            let target_idx = columns.get(stream_idx).copied().unwrap_or(stream_idx);
            row[target_idx] = value;
        }
        payloads.push(
            codec
                .encode(&row)
                .map_err(|e| ServerError::CopyFormat(format!("binary COPY row encode: {e}")))?,
        );
    }
    Ok(payloads)
}

pub(super) fn read_i16_be(bytes: &[u8], pos: &mut usize) -> Result<i16, ServerError> {
    let end = binary_copy_end(*pos, 2, bytes.len(), "binary COPY")?;
    let out = i16::from_be_bytes([bytes[*pos], bytes[*pos + 1]]);
    *pos = end;
    Ok(out)
}

pub(super) fn read_i32_be(bytes: &[u8], pos: &mut usize) -> Result<i32, ServerError> {
    let end = binary_copy_end(*pos, 4, bytes.len(), "binary COPY")?;
    let out = i32::from_be_bytes([
        bytes[*pos],
        bytes[*pos + 1],
        bytes[*pos + 2],
        bytes[*pos + 3],
    ]);
    *pos = end;
    Ok(out)
}

pub(super) fn binary_copy_end(
    pos: usize,
    len: usize,
    total: usize,
    context: &str,
) -> Result<usize, ServerError> {
    let end = pos.checked_add(len).ok_or_else(|| {
        ServerError::CopyFormat(format!("{context} offset overflow: pos={pos} len={len}"))
    })?;
    if end > total {
        return Err(ServerError::CopyFormat(format!("truncated {context}")));
    }
    Ok(end)
}

pub(super) fn decode_binary_copy_cell(
    bytes: &[u8],
    dtype: &DataType,
    column_idx: usize,
    jsonb_shape_cache: &mut JsonbShapeCache,
) -> Result<Value, ServerError> {
    let exact = |n: usize| {
        if bytes.len() == n {
            Ok(())
        } else {
            Err(ServerError::CopyFormat(format!(
                "column {column_idx}: binary length {}, expected {n}",
                bytes.len()
            )))
        }
    };
    match dtype {
        DataType::Bool => {
            exact(1)?;
            Ok(Value::Bool(bytes[0] != 0))
        }
        DataType::Int16 => {
            exact(2)?;
            Ok(Value::Int16(i16::from_be_bytes([bytes[0], bytes[1]])))
        }
        DataType::Int32 => {
            exact(4)?;
            Ok(Value::Int32(i32::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
            ])))
        }
        DataType::Int64 => {
            exact(8)?;
            Ok(Value::Int64(i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ])))
        }
        DataType::Float32 => {
            exact(4)?;
            Ok(Value::Float32(f32::from_bits(u32::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
            ]))))
        }
        DataType::Float64 => {
            exact(8)?;
            Ok(Value::Float64(f64::from_bits(u64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]))))
        }
        DataType::Date => {
            exact(4)?;
            Ok(Value::Date(i32::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
            ])))
        }
        DataType::Time => {
            exact(8)?;
            Ok(Value::Time(i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ])))
        }
        DataType::Timestamp => {
            exact(8)?;
            Ok(Value::Timestamp(i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ])))
        }
        DataType::TimestampTz => {
            exact(8)?;
            Ok(Value::TimestampTz(i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ])))
        }
        DataType::TimeTz => {
            exact(12)?;
            // PostgreSQL's binary timetz encodes the zone as seconds WEST of
            // UTC, whereas our internal `offset_seconds` is east-positive.
            // Negate the wire field to convert back to the internal sign.
            let wire_zone = i32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
            Ok(Value::TimeTz {
                micros: i64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]),
                offset_seconds: wire_zone.wrapping_neg(),
            })
        }
        DataType::Decimal { .. } => decode_pg_numeric_binary(bytes)
            .map_err(|err| ServerError::CopyFormat(format!("column {column_idx}: {err}"))),
        DataType::Money => decode_pg_money_binary(bytes)
            .map_err(|err| ServerError::CopyFormat(format!("column {column_idx}: {err}"))),
        DataType::Bit { .. } | DataType::VarBit { .. } => BitString::from_pg_binary(bytes)
            .and_then(|bits| bits.coerce_to(dtype, false))
            .map(Value::BitString)
            .ok_or_else(|| {
                ServerError::CopyFormat(format!("column {column_idx}: invalid {dtype} binary"))
            }),
        DataType::Inet | DataType::Cidr | DataType::MacAddr | DataType::MacAddr8 => {
            NetworkValue::from_pg_binary(dtype, bytes)
                .map(Value::Network)
                .ok_or_else(|| {
                    ServerError::CopyFormat(format!("column {column_idx}: invalid {dtype} binary"))
                })
        }
        DataType::Text { .. } | DataType::TsVector | DataType::TsQuery => {
            std::str::from_utf8(bytes)
                .map(|s| Value::Text(s.to_string()))
                .map_err(|e| ServerError::CopyFormat(format!("column {column_idx}: {e}")))
        }
        DataType::Char { len } => std::str::from_utf8(bytes)
            .map_err(|e| ServerError::CopyFormat(format!("column {column_idx}: {e}")))
            .and_then(|s| {
                coerce_bpchar_text(s, *len, false)
                    .map(Value::Char)
                    .map_err(|err| ServerError::CopyFormat(format!("column {column_idx}: {err}")))
            }),
        DataType::Json => parse_json_text(bytes, column_idx).map(Value::Json),
        DataType::Jsonb => jsonb_shape_cache
            .parse_pg_binary(bytes, column_idx)
            .map(Value::Jsonb),
        DataType::Xml => parse_xml_text(bytes, column_idx).map(Value::Xml),
        DataType::Bytea => Ok(Value::Bytea(bytes.to_vec())),
        DataType::Uuid => {
            exact(16)?;
            let raw: [u8; 16] = bytes.try_into().map_err(|_| {
                ServerError::CopyFormat(format!("column {column_idx}: binary UUID length invalid"))
            })?;
            Ok(Value::Uuid(raw))
        }
        other => decode_copy_cell(Some(bytes), other, column_idx, jsonb_shape_cache),
    }
}
