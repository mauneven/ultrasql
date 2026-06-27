//! Textual COPY cell decoding and encoding helpers.
//!
//! Converts between COPY text/CSV cell bytes and typed runtime [`Value`]s,
//! including the per-type text parsers (dates, timestamps, numerics, vectors)
//! and the reverse `Value -> CopyData` cell encoders.

use ultrasql_core::{
    DataType, Schema, Value, coerce_bpchar_text, parse_decimal_text, parse_money_text,
    parse_time_text, parse_timestamptz_text, parse_timetz_text,
};
use ultrasql_protocol::BackendMessage;

use super::super::jsonb_ingest::{JsonbShapeCache, parse_json_text};
use super::{
    CopyOptions, CopyRowDecodeContext, MICROS_PER_DAY, ServerCopyFormat, ServerError,
    parse_csv_row, parse_text_row, parse_unquoted_csv_row_slices,
};

pub(super) fn copy_cells_from_row_with_options(
    row: &[Value],
    schema: &Schema,
    columns: &[usize],
    text_options: &crate::result_encoder::TextEncodingOptions,
) -> Vec<Option<Vec<u8>>> {
    if columns.is_empty() {
        row.iter()
            .zip(schema.fields())
            .map(|(value, field)| {
                value_to_copy_cell_with_options(value, &field.data_type, text_options)
            })
            .collect()
    } else {
        columns
            .iter()
            .map(|&i| {
                let field = schema.field_at(i);
                row.get(i).and_then(|value| {
                    value_to_copy_cell_with_options(value, &field.data_type, text_options)
                })
            })
            .collect()
    }
}

pub(super) fn copy_rows_from_select_result(
    result: &crate::result_encoder::SelectResult,
    schema: &Schema,
    opts: &CopyOptions,
) -> Result<(Vec<u8>, u64), ServerError> {
    let mut out = Vec::new();
    if opts.header {
        let header_cells: Vec<Option<Vec<u8>>> = schema
            .fields()
            .iter()
            .map(|f| Some(f.name.as_bytes().to_vec()))
            .collect();
        match opts.format {
            ServerCopyFormat::Text => {
                out.extend_from_slice(&super::encode_text_row(&header_cells, opts))
            }
            ServerCopyFormat::Csv => {
                out.extend_from_slice(&super::encode_csv_row(&header_cells, opts))
            }
            ServerCopyFormat::Binary | ServerCopyFormat::Parquet => {}
        }
    }
    let mut rows = 0_u64;
    for msg in &result.messages {
        if let BackendMessage::DataRow { columns } = msg {
            match opts.format {
                ServerCopyFormat::Text => {
                    out.extend_from_slice(&super::encode_text_row(columns, opts))
                }
                ServerCopyFormat::Csv => {
                    out.extend_from_slice(&super::encode_csv_row(columns, opts))
                }
                ServerCopyFormat::Binary => {
                    return Err(ServerError::Unsupported(
                        "binary COPY for query targets is not yet supported",
                    ));
                }
                ServerCopyFormat::Parquet => {
                    return Err(ServerError::Unsupported(
                        "parquet COPY for query targets is not yet supported",
                    ));
                }
            }
            super::increment_copy_rows(&mut rows, "COPY query")?;
        }
    }
    Ok((out, rows))
}

pub(super) fn decode_one_copy_row(
    line: &[u8],
    opts: &CopyOptions,
    mut context: CopyRowDecodeContext<'_>,
) -> Result<Vec<u8>, ServerError> {
    match opts.format {
        ServerCopyFormat::Csv if !line.contains(&b'"') => {
            let raw_cells = parse_unquoted_csv_row_slices(line, opts)?;
            return decode_copy_cells_to_payload(&raw_cells, &mut context);
        }
        ServerCopyFormat::Text | ServerCopyFormat::Csv => {}
        ServerCopyFormat::Binary | ServerCopyFormat::Parquet => {
            return Err(ServerError::CopyFormat(
                "binary COPY rows are decoded by binary parser".to_string(),
            ));
        }
    };
    let owned_cells = match opts.format {
        ServerCopyFormat::Text => parse_text_row(line, opts)?,
        ServerCopyFormat::Csv => parse_csv_row(line, opts)?,
        ServerCopyFormat::Binary | ServerCopyFormat::Parquet => unreachable!(),
    };
    let raw_cells = owned_cells.iter().map(Option::as_deref).collect::<Vec<_>>();
    decode_copy_cells_to_payload(&raw_cells, &mut context)
}

pub(super) fn decode_copy_cells_to_payload(
    raw_cells: &[Option<&[u8]>],
    context: &mut CopyRowDecodeContext<'_>,
) -> Result<Vec<u8>, ServerError> {
    let entry = context.entry;
    let columns = context.columns;
    let schema = context.schema;
    let codec = context.codec;
    if raw_cells.len() != schema.len() {
        return Err(ServerError::CopyFormat(format!(
            "COPY FROM expected {} columns, got {}",
            schema.len(),
            raw_cells.len()
        )));
    }

    // Default-applying column-list path: encode a NARROW row holding only the
    // streamed columns (in stream order, typed by `schema`), and leave default
    // filling, generated-column evaluation, and the NOT NULL check to the
    // downstream INSERT operator (`flush_copy_insert_batch_with_defaults`),
    // exactly like a normal `INSERT t(col-list)`. We must NOT enforce NOT NULL
    // here: an omitted NOT NULL column with a DEFAULT is valid in PostgreSQL,
    // and the operator re-checks NOT NULL after defaults are applied.
    if context.apply_defaults {
        let mut narrow_row = Vec::with_capacity(raw_cells.len());
        for (stream_idx, (table_col_idx, raw)) in columns.iter().zip(raw_cells.iter()).enumerate() {
            let field = entry.schema.field_at(*table_col_idx);
            narrow_row.push(decode_copy_cell(
                *raw,
                &field.data_type,
                stream_idx,
                context.jsonb_shape_cache,
            )?);
        }
        return codec
            .encode(&narrow_row)
            .map_err(|e| ServerError::CopyFormat(format!("COPY FROM row encode: {e}")));
    }

    let mut row = vec![Value::Null; entry.schema.len()];
    if columns.is_empty() {
        for (col_idx, raw) in raw_cells.iter().enumerate() {
            let field = entry.schema.field_at(col_idx);
            row[col_idx] =
                decode_copy_cell(*raw, &field.data_type, col_idx, context.jsonb_shape_cache)?;
        }
    } else {
        for (stream_idx, (table_col_idx, raw)) in columns.iter().zip(raw_cells.iter()).enumerate() {
            let field = entry.schema.field_at(*table_col_idx);
            row[*table_col_idx] = decode_copy_cell(
                *raw,
                &field.data_type,
                stream_idx,
                context.jsonb_shape_cache,
            )?;
        }
    }

    for (value, field) in row.iter().zip(entry.schema.fields()) {
        if matches!(value, Value::Null) && !field.nullable {
            // A NULL into a NOT NULL column is a constraint violation, not a
            // file-format error. Emit the SAME variant the INSERT/ModifyTable
            // path surfaces (`ExecError::NotNullViolation`, which carries the
            // column name and maps to SQLSTATE 23502 `not_null_violation`) so
            // COPY matches PostgreSQL — and so it matches INSERT, CHECK (23514),
            // UNIQUE (23505), FK (23503) and EXCLUDE (23P01) parity. The check
            // stays at the decode layer because the bulk fast path
            // (`copy_table_needs_maintained_insert == false`) never reaches the
            // operator, so this is the sole NOT NULL enforcement there. It
            // still flows through the COPY abort/take-and-park path verbatim:
            // `Execute(_)` is query-scoped and is not matched on by any COPY
            // error handler, so the whole COPY aborts atomically and the wire
            // is drained / the explicit block parked Failed identically to the
            // former `CopyFormat` error. Genuine format/parse errors stay
            // `CopyFormat` -> 22P04.
            return Err(ServerError::Execute(
                ultrasql_executor::ExecError::NotNullViolation(field.name.clone()),
            ));
        }
    }

    codec
        .encode(&row)
        .map_err(|e| ServerError::CopyFormat(format!("COPY FROM row encode: {e}")))
}

/// Encode a runtime [`Value`] as a `CopyData` cell (`None` is SQL NULL).
pub(super) fn value_to_copy_cell_with_options(
    value: &Value,
    dtype: &DataType,
    text_options: &crate::result_encoder::TextEncodingOptions,
) -> Option<Vec<u8>> {
    match (dtype, value) {
        (_, Value::Null) => None,
        (DataType::Date, Value::Int32(v) | Value::Date(v)) => {
            Some(text_options.format_date(*v).into_bytes())
        }
        (DataType::Decimal { scale, .. }, Value::Int64(v)) => Some(
            Value::Decimal {
                value: i128::from(*v),
                scale: scale.unwrap_or(0),
            }
            .to_string()
            .into_bytes(),
        ),
        (DataType::Money, Value::Int64(v) | Value::Money(v)) => {
            Some(text_options.format_money(*v).into_bytes())
        }
        (DataType::Time, Value::Int64(v) | Value::Time(v)) => {
            Some(Value::Time(*v).to_string().into_bytes())
        }
        (DataType::Timestamp, Value::Int64(v) | Value::Timestamp(v)) => {
            Some(text_options.format_timestamp(*v).into_bytes())
        }
        (DataType::TimestampTz, Value::Int64(v) | Value::TimestampTz(v)) => {
            Some(text_options.format_timestamptz(*v).into_bytes())
        }
        (
            DataType::TimeTz,
            Value::TimeTz {
                micros,
                offset_seconds,
            },
        ) => Some(
            Value::TimeTz {
                micros: *micros,
                offset_seconds: *offset_seconds,
            }
            .to_string()
            .into_bytes(),
        ),
        (_, value) => value_to_copy_cell_by_value(value),
    }
}

fn value_to_copy_cell_by_value(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::Null => None,
        Value::Bool(b) => Some(if *b { b"t".to_vec() } else { b"f".to_vec() }),
        Value::Int16(v) => Some(v.to_string().into_bytes()),
        Value::Int32(v) => Some(v.to_string().into_bytes()),
        Value::Int64(v) => Some(v.to_string().into_bytes()),
        Value::Oid(v) | Value::RegClass(v) | Value::RegType(v) => {
            Some(v.raw().to_string().into_bytes())
        }
        Value::PgLsn(v) => Some(v.to_string().into_bytes()),
        Value::Float32(v) => Some(format_float_f32(*v)),
        Value::Float64(v) => Some(format_float_f64(*v)),
        Value::Text(s) | Value::Char(s) => Some(s.as_bytes().to_vec()),
        Value::Bytea(b) => Some(b.clone()),
        Value::Timestamp(v) | Value::TimestampTz(v) | Value::Time(v) => {
            Some(v.to_string().into_bytes())
        }
        Value::TimeTz { .. } => Some(value.to_string().into_bytes()),
        Value::Date(v) => Some(v.to_string().into_bytes()),
        Value::Uuid(bytes) => Some(Value::Uuid(*bytes).to_string().into_bytes()),
        Value::Decimal { .. }
        | Value::Money(_)
        | Value::BitString(_)
        | Value::Network(_)
        | Value::Interval { .. }
        | Value::Range(_)
        | Value::Geometry(_)
        | Value::Json(_)
        | Value::Jsonb(_)
        | Value::Xml(_)
        | Value::Vector(_)
        | Value::HalfVec(_)
        | Value::SparseVec(_)
        | Value::BitVec { .. }
        | Value::Array { .. }
        | Value::Record(_) => Some(value.to_string().into_bytes()),
    }
}

pub(super) fn parse_xml_text(bytes: &[u8], column_idx: usize) -> Result<String, ServerError> {
    let text = std::str::from_utf8(bytes).map_err(|_| {
        ServerError::CopyFormat(format!("column {column_idx}: invalid UTF-8 in xml"))
    })?;
    Value::validate_xml_text(text)
        .ok_or_else(|| ServerError::CopyFormat(format!("column {column_idx}: invalid xml")))
}

/// Decode a single COPY cell into a typed [`Value`] consistent with the
/// target column's [`DataType`].
pub(super) fn decode_copy_cell(
    raw: Option<&[u8]>,
    dtype: &DataType,
    column_idx: usize,
    jsonb_shape_cache: &mut JsonbShapeCache,
) -> Result<Value, ServerError> {
    let Some(bytes) = raw else {
        return Ok(Value::Null);
    };
    let s = std::str::from_utf8(bytes).map_err(|_| {
        ServerError::CopyFormat(format!("column {column_idx}: invalid UTF-8 in COPY input"))
    })?;
    match dtype {
        DataType::Bool => parse_copy_bool(s, column_idx).map(Value::Bool),
        DataType::Int16 => s
            .parse::<i16>()
            .map(Value::Int16)
            .map_err(|e| ServerError::CopyFormat(format!("column {column_idx}: {e}"))),
        DataType::Int32 => s
            .parse::<i32>()
            .map(Value::Int32)
            .map_err(|e| ServerError::CopyFormat(format!("column {column_idx}: {e}"))),
        DataType::Int64 => s
            .parse::<i64>()
            .map(Value::Int64)
            .map_err(|e| ServerError::CopyFormat(format!("column {column_idx}: {e}"))),
        DataType::Oid | DataType::RegClass | DataType::RegType => {
            let oid = Value::parse_oid_text(s).ok_or_else(|| {
                ServerError::CopyFormat(format!("column {column_idx}: invalid {dtype} literal"))
            })?;
            Ok(match dtype {
                DataType::Oid => Value::Oid(oid),
                DataType::RegClass => Value::RegClass(oid),
                DataType::RegType => Value::RegType(oid),
                _ => unreachable!(),
            })
        }
        DataType::PgLsn => Value::parse_pg_lsn_text(s)
            .map(Value::PgLsn)
            .ok_or_else(|| {
                ServerError::CopyFormat(format!("column {column_idx}: invalid {dtype} literal"))
            }),
        DataType::Float32 => s
            .parse::<f32>()
            .map(Value::Float32)
            .map_err(|e| ServerError::CopyFormat(format!("column {column_idx}: {e}"))),
        DataType::Float64 => s
            .parse::<f64>()
            .map(Value::Float64)
            .map_err(|e| ServerError::CopyFormat(format!("column {column_idx}: {e}"))),
        DataType::Decimal { scale, .. } => parse_copy_decimal(s, *scale, column_idx),
        DataType::Money => parse_money_text(s)
            .map_err(|err| ServerError::CopyFormat(format!("column {column_idx}: {err}"))),
        DataType::Date => parse_copy_date(s, column_idx).map(Value::Date),
        DataType::Time => parse_copy_time(s, column_idx).map(Value::Time),
        DataType::TimeTz => parse_copy_timetz(s, column_idx),
        DataType::Timestamp => parse_copy_timestamp(s, column_idx).map(Value::Timestamp),
        DataType::TimestampTz => parse_copy_timestamptz(s, column_idx).map(Value::TimestampTz),
        DataType::Text { .. } | DataType::TsVector | DataType::TsQuery => {
            Ok(Value::Text(s.to_string()))
        }
        DataType::Char { len } => coerce_bpchar_text(s, *len, false)
            .map(Value::Char)
            .map_err(|err| ServerError::CopyFormat(format!("column {column_idx}: {err}"))),
        DataType::Bit { .. } | DataType::VarBit { .. } => {
            parse_copy_bit_string(s, dtype, column_idx)
        }
        DataType::Inet | DataType::Cidr | DataType::MacAddr | DataType::MacAddr8 => {
            Value::parse_network(dtype, s).ok_or_else(|| {
                ServerError::CopyFormat(format!("column {column_idx}: invalid {dtype} literal"))
            })
        }
        DataType::Json => parse_json_text(bytes, column_idx).map(Value::Json),
        DataType::Jsonb => jsonb_shape_cache
            .parse_text(bytes, column_idx)
            .map(Value::Jsonb),
        DataType::Xml => parse_xml_text(bytes, column_idx).map(Value::Xml),
        DataType::Bytea => {
            if s.starts_with("\\x") {
                Value::parse_bytea(s).map(Value::Bytea).ok_or_else(|| {
                    ServerError::CopyFormat(format!("column {column_idx}: invalid bytea literal"))
                })
            } else {
                Ok(Value::Bytea(bytes.to_vec()))
            }
        }
        DataType::Uuid => Value::parse_uuid(s).map(Value::Uuid).ok_or_else(|| {
            ServerError::CopyFormat(format!("column {column_idx}: invalid uuid literal"))
        }),
        DataType::Vector { dims } => match Value::parse_vector(s) {
            Some(Value::Vector(values))
                if dims.is_none() || u32::try_from(values.len()).ok() == *dims =>
            {
                Ok(Value::Vector(values))
            }
            _ => Err(ServerError::CopyFormat(format!(
                "column {column_idx}: invalid {dtype} literal"
            ))),
        },
        DataType::HalfVec { dims } => {
            parse_copy_vector_family(Value::parse_halfvec(s), *dims, dtype, column_idx)
        }
        DataType::SparseVec { dims } => {
            parse_copy_vector_family(Value::parse_sparsevec(s), *dims, dtype, column_idx)
        }
        DataType::BitVec { dims } => {
            parse_copy_vector_family(Value::parse_bitvec(s), *dims, dtype, column_idx)
        }
        DataType::Range(range_type) => ultrasql_core::RangeValue::parse(*range_type, s)
            .map(Value::Range)
            .ok_or_else(|| {
                ServerError::CopyFormat(format!("column {column_idx}: invalid {dtype} literal"))
            }),
        DataType::Geometry(geometry_type) => ultrasql_core::GeometryValue::parse(*geometry_type, s)
            .map(Value::Geometry)
            .ok_or_else(|| {
                ServerError::CopyFormat(format!("column {column_idx}: invalid {dtype} literal"))
            }),
        other => Err(ServerError::CopyFormat(format!(
            "column {column_idx}: unsupported COPY target type {other}"
        ))),
    }
}

fn parse_copy_vector_family(
    parsed: Option<Value>,
    expected_dims: Option<u32>,
    dtype: &DataType,
    column_idx: usize,
) -> Result<Value, ServerError> {
    let value = parsed.ok_or_else(|| {
        ServerError::CopyFormat(format!("column {column_idx}: invalid {dtype} literal"))
    })?;
    let actual_dims = value.data_type().vector_dims().flatten();
    if actual_dims.is_some_and(|dims| expected_dims.is_none_or(|expected| expected == dims)) {
        Ok(value)
    } else {
        Err(ServerError::CopyFormat(format!(
            "column {column_idx}: invalid {dtype} literal"
        )))
    }
}

fn parse_copy_bit_string(
    text: &str,
    dtype: &DataType,
    column_idx: usize,
) -> Result<Value, ServerError> {
    let bits = Value::parse_bit_string(text).ok_or_else(|| {
        ServerError::CopyFormat(format!("column {column_idx}: invalid {dtype} literal"))
    })?;
    match bits {
        Value::BitString(bit_string) if bit_string.matches_type(dtype) => {
            Ok(Value::BitString(bit_string))
        }
        Value::BitString(_) => Err(ServerError::CopyFormat(format!(
            "column {column_idx}: invalid {dtype} length"
        ))),
        _ => Err(ServerError::CopyFormat(format!(
            "column {column_idx}: invalid {dtype} literal"
        ))),
    }
}

/// PostgreSQL-style boolean accept rules used by COPY text input.
fn parse_copy_bool(s: &str, column_idx: usize) -> Result<bool, ServerError> {
    match s {
        "t" | "true" | "TRUE" | "T" | "1" | "y" | "Y" | "yes" | "YES" => Ok(true),
        "f" | "false" | "FALSE" | "F" | "0" | "n" | "N" | "no" | "NO" => Ok(false),
        other => Err(ServerError::CopyFormat(format!(
            "column {column_idx}: not a boolean ({other:?})"
        ))),
    }
}

fn parse_copy_decimal(
    s: &str,
    scale: Option<i32>,
    column_idx: usize,
) -> Result<Value, ServerError> {
    parse_decimal_text(s, scale)
        .map_err(|err| ServerError::CopyFormat(format!("column {column_idx}: {err}")))
}

pub(super) fn parse_copy_date(s: &str, column_idx: usize) -> Result<i32, ServerError> {
    let raw = s.trim();
    if raw.len() != 10 {
        return Err(ServerError::CopyFormat(format!(
            "column {column_idx}: invalid date literal {raw:?}"
        )));
    }
    let bytes = raw.as_bytes();
    if bytes[4] != b'-' || bytes[7] != b'-' {
        return Err(ServerError::CopyFormat(format!(
            "column {column_idx}: invalid date literal {raw:?}"
        )));
    }
    let year = raw[..4].parse::<i32>().map_err(|e| {
        ServerError::CopyFormat(format!("column {column_idx}: invalid date year: {e}"))
    })?;
    let month = raw[5..7].parse::<u32>().map_err(|e| {
        ServerError::CopyFormat(format!("column {column_idx}: invalid date month: {e}"))
    })?;
    let day = raw[8..10].parse::<u32>().map_err(|e| {
        ServerError::CopyFormat(format!("column {column_idx}: invalid date day: {e}"))
    })?;
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return Err(ServerError::CopyFormat(format!(
            "column {column_idx}: invalid date literal {raw:?}"
        )));
    }
    days_since_epoch(year, month, day)
        .ok_or_else(|| ServerError::CopyFormat(format!("column {column_idx}: date overflow")))
}

pub(super) fn parse_copy_timestamp(s: &str, column_idx: usize) -> Result<i64, ServerError> {
    let raw = s.trim();
    let split = raw.find(' ').or_else(|| raw.find('T')).ok_or_else(|| {
        ServerError::CopyFormat(format!(
            "column {column_idx}: invalid timestamp literal {raw:?}"
        ))
    })?;
    let date_micros = i64::from(parse_copy_date(&raw[..split], column_idx)?)
        .checked_mul(MICROS_PER_DAY)
        .ok_or_else(|| {
            ServerError::CopyFormat(format!("column {column_idx}: timestamp overflow"))
        })?;
    let time_micros = parse_copy_time(&raw[split + 1..], column_idx)?;
    date_micros
        .checked_add(time_micros)
        .ok_or_else(|| ServerError::CopyFormat(format!("column {column_idx}: timestamp overflow")))
}

pub(super) fn parse_copy_timestamptz(s: &str, column_idx: usize) -> Result<i64, ServerError> {
    let raw = s.trim();
    parse_timestamptz_text(raw).ok_or_else(|| {
        ServerError::CopyFormat(format!(
            "column {column_idx}: invalid timestamptz literal {raw:?}"
        ))
    })
}

pub(super) fn parse_copy_time(s: &str, column_idx: usize) -> Result<i64, ServerError> {
    parse_time_text(s).ok_or_else(|| {
        ServerError::CopyFormat(format!(
            "column {column_idx}: invalid time literal {:?}",
            s.trim()
        ))
    })
}

pub(super) fn parse_copy_timetz(s: &str, column_idx: usize) -> Result<Value, ServerError> {
    parse_timetz_text(s)
        .map(|(micros, offset_seconds)| Value::TimeTz {
            micros,
            offset_seconds,
        })
        .ok_or_else(|| {
            ServerError::CopyFormat(format!(
                "column {column_idx}: invalid timetz literal {:?}",
                s.trim()
            ))
        })
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

pub(super) fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn days_since_epoch(year: i32, month: u32, day: u32) -> Option<i32> {
    let y = if month <= 2 {
        year.checked_sub(1)?
    } else {
        year
    };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let month_i32 = i32::try_from(month).ok()?;
    let day_i32 = i32::try_from(day).ok()?;
    let month_offset = if month > 2 {
        month_i32 - 3
    } else {
        month_i32 + 9
    };
    let doy = (153 * month_offset + 2) / 5 + day_i32 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days_from_1970_03_01 = i64::from(era)
        .checked_mul(146_097)?
        .checked_add(i64::from(doe))?
        .checked_sub(719_468)?;
    let days_since_2000_01_01 = days_from_1970_03_01.checked_sub(10_957)?;
    i32::try_from(days_since_2000_01_01).ok()
}

pub(super) fn format_float_f32(v: f32) -> Vec<u8> {
    if v.is_nan() {
        b"NaN".to_vec()
    } else if v.is_infinite() {
        if v > 0.0 {
            b"Infinity".to_vec()
        } else {
            b"-Infinity".to_vec()
        }
    } else {
        format!("{v}").into_bytes()
    }
}

pub(super) fn format_float_f64(v: f64) -> Vec<u8> {
    if v.is_nan() {
        b"NaN".to_vec()
    } else if v.is_infinite() {
        if v > 0.0 {
            b"Infinity".to_vec()
        } else {
            b"-Infinity".to_vec()
        }
    } else {
        format!("{v}").into_bytes()
    }
}
