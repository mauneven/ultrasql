//! Wire encoders and decoders used by the Extended Query protocol path:
//! parameter decode (text + binary), result-column binary encode,
//! `RowDescription` synthesis from a logical plan, and the SELECT/INSERT/
//! UPDATE/DELETE command-complete tag formatter.

use ultrasql_core::{
    BitString, DataType, Lsn, NetworkValue, Oid, Value, decode_pg_money_binary,
    decode_pg_numeric_binary, encode_pg_money_binary, encode_pg_numeric_binary, parse_decimal_text,
    parse_money_text, parse_time_text, parse_timestamp_text, parse_timestamptz_text,
    parse_timetz_text, unpack_timetz,
};
use ultrasql_planner::LogicalPlan;
use ultrasql_protocol::{BackendMessage, FieldDescription};

use super::{
    PG_OID_BIT, PG_OID_BIT_ARRAY, PG_OID_BOOL, PG_OID_BOOL_ARRAY, PG_OID_BPCHAR,
    PG_OID_BPCHAR_ARRAY, PG_OID_BYTEA, PG_OID_BYTEA_ARRAY, PG_OID_CIDR, PG_OID_CIDR_ARRAY,
    PG_OID_DATE, PG_OID_DATE_ARRAY, PG_OID_FLOAT4, PG_OID_FLOAT4_ARRAY, PG_OID_FLOAT8,
    PG_OID_FLOAT8_ARRAY, PG_OID_INET, PG_OID_INET_ARRAY, PG_OID_INT2, PG_OID_INT2_ARRAY,
    PG_OID_INT4, PG_OID_INT4_ARRAY, PG_OID_INT8, PG_OID_INT8_ARRAY, PG_OID_JSON, PG_OID_JSON_ARRAY,
    PG_OID_JSONB, PG_OID_JSONB_ARRAY, PG_OID_MACADDR, PG_OID_MACADDR_ARRAY, PG_OID_MACADDR8,
    PG_OID_MACADDR8_ARRAY, PG_OID_MONEY, PG_OID_MONEY_ARRAY, PG_OID_NUMERIC, PG_OID_NUMERIC_ARRAY,
    PG_OID_OID, PG_OID_OID_ARRAY, PG_OID_PG_LSN, PG_OID_PG_LSN_ARRAY, PG_OID_REGCLASS,
    PG_OID_REGCLASS_ARRAY, PG_OID_REGTYPE, PG_OID_REGTYPE_ARRAY, PG_OID_TEXT, PG_OID_TEXT_ARRAY,
    PG_OID_TIME, PG_OID_TIME_ARRAY, PG_OID_TIMESTAMP, PG_OID_TIMESTAMP_ARRAY, PG_OID_TIMESTAMPTZ,
    PG_OID_TIMESTAMPTZ_ARRAY, PG_OID_TIMETZ, PG_OID_TIMETZ_ARRAY, PG_OID_TSQUERY,
    PG_OID_TSQUERY_ARRAY, PG_OID_TSVECTOR, PG_OID_TSVECTOR_ARRAY, PG_OID_UUID, PG_OID_UUID_ARRAY,
    PG_OID_VARBIT, PG_OID_VARBIT_ARRAY, PG_OID_VARCHAR, PG_OID_XML, PG_OID_XML_ARRAY,
};

const JSONB_BINARY_VERSION: u8 = 1;

// ---------------------------------------------------------------------------
// Parameter byte-decoder.
// ---------------------------------------------------------------------------

/// Errors raised while decoding a single Bind parameter.
#[derive(Debug)]
pub(super) enum DecodeError {
    /// Format code other than `0` (text) or `1` (binary).
    BadFormat,
    /// Bytes do not match the declared type (length mismatch, invalid
    /// UTF-8, unparseable numeric, etc.).
    BadBytes,
}

/// Decode one Bind parameter into a [`Value`].
///
/// `raw = None` → SQL NULL (`Value::Null`). Otherwise, `format` is
/// the per-parameter format code (`0` = text, `1` = binary). `oid` is
/// the declared parameter type OID from `Parse`; an absent OID is
/// treated as text-default.
///
/// Text-format decoding parses the UTF-8 bytes through Rust's std
/// parsers. Binary-format decoding uses the type-specific big-endian
/// layout from `pg_type.dat`.
pub(super) fn decode_param(
    raw: Option<&[u8]>,
    format: i16,
    oid: Option<u32>,
) -> Result<Value, DecodeError> {
    let Some(bytes) = raw else {
        return Ok(Value::Null);
    };
    match format {
        0 => decode_param_text(bytes, oid),
        1 => decode_param_binary(bytes, oid),
        _ => Err(DecodeError::BadFormat),
    }
}

/// Decode a parameter in text format.
fn decode_param_text(bytes: &[u8], oid: Option<u32>) -> Result<Value, DecodeError> {
    let s = std::str::from_utf8(bytes).map_err(|_| DecodeError::BadBytes)?;
    // PG treats an empty oid (0) as "unspecified"; default to text-ish
    // and let the binder/runtime coerce.
    match oid.unwrap_or(0) {
        PG_OID_BOOL => match s {
            "t" | "true" | "1" | "TRUE" | "T" | "yes" | "YES" | "y" | "Y" | "on" | "ON" => {
                Ok(Value::Bool(true))
            }
            "f" | "false" | "0" | "FALSE" | "F" | "no" | "NO" | "n" | "N" | "off" | "OFF" => {
                Ok(Value::Bool(false))
            }
            _ => Err(DecodeError::BadBytes),
        },
        PG_OID_INT2 => s
            .parse::<i16>()
            .map(Value::Int16)
            .map_err(|_| DecodeError::BadBytes),
        PG_OID_INT4 => s
            .parse::<i32>()
            .map(Value::Int32)
            .map_err(|_| DecodeError::BadBytes),
        PG_OID_INT8 => s
            .parse::<i64>()
            .map(Value::Int64)
            .map_err(|_| DecodeError::BadBytes),
        PG_OID_OID => Value::parse_oid_text(s)
            .map(Value::Oid)
            .ok_or(DecodeError::BadBytes),
        PG_OID_REGCLASS => {
            Ok(Value::parse_oid_text(s).map_or_else(|| Value::Text(s.to_owned()), Value::RegClass))
        }
        PG_OID_REGTYPE => {
            Ok(Value::parse_oid_text(s).map_or_else(|| Value::Text(s.to_owned()), Value::RegType))
        }
        PG_OID_PG_LSN => Value::parse_pg_lsn_text(s)
            .map(Value::PgLsn)
            .ok_or(DecodeError::BadBytes),
        PG_OID_FLOAT4 => s
            .parse::<f32>()
            .map(Value::Float32)
            .map_err(|_| DecodeError::BadBytes),
        PG_OID_FLOAT8 => s
            .parse::<f64>()
            .map(Value::Float64)
            .map_err(|_| DecodeError::BadBytes),
        PG_OID_MONEY => parse_money_text(s).map_err(|_| DecodeError::BadBytes),
        PG_OID_NUMERIC => parse_decimal_text(s, None).map_err(|_| DecodeError::BadBytes),
        PG_OID_BIT | PG_OID_VARBIT => Value::parse_bit_string(s).ok_or(DecodeError::BadBytes),
        PG_OID_INET => Value::parse_network(&DataType::Inet, s).ok_or(DecodeError::BadBytes),
        PG_OID_CIDR => Value::parse_network(&DataType::Cidr, s).ok_or(DecodeError::BadBytes),
        PG_OID_MACADDR => Value::parse_network(&DataType::MacAddr, s).ok_or(DecodeError::BadBytes),
        PG_OID_MACADDR8 => {
            Value::parse_network(&DataType::MacAddr8, s).ok_or(DecodeError::BadBytes)
        }
        PG_OID_JSON => validate_json_text_param(s).map(Value::Json),
        PG_OID_JSONB => normalize_jsonb_param(s).map(Value::Jsonb),
        PG_OID_XML => validate_xml_text_param(s).map(Value::Xml),
        PG_OID_TIME => parse_time_text(s)
            .map(Value::Time)
            .ok_or(DecodeError::BadBytes),
        PG_OID_TIMESTAMP => parse_timestamp_text(s)
            .map(Value::Timestamp)
            .ok_or(DecodeError::BadBytes),
        PG_OID_TIMESTAMPTZ => parse_timestamptz_text(s)
            .map(Value::TimestampTz)
            .ok_or(DecodeError::BadBytes),
        PG_OID_TIMETZ => parse_timetz_text(s)
            .map(|(micros, offset_seconds)| Value::TimeTz {
                micros,
                offset_seconds,
            })
            .ok_or(DecodeError::BadBytes),
        PG_OID_TEXT | PG_OID_VARCHAR | PG_OID_BPCHAR | PG_OID_TSVECTOR | PG_OID_TSQUERY => {
            Ok(Value::Text(s.to_string()))
        }
        PG_OID_BYTEA => Ok(Value::Bytea(bytes.to_vec())),
        // No declared OID, or an OID we don't decode specially: best-effort
        // numeric-then-text fallback so libpq's `text` default still works
        // for "WHERE id = $1" with $1='42'.
        _ => Ok(s.parse::<i32>().map_or_else(
            |_| {
                s.parse::<i64>().map_or_else(
                    |_| {
                        s.parse::<f64>()
                            .map_or_else(|_| Value::Text(s.to_string()), Value::Float64)
                    },
                    Value::Int64,
                )
            },
            Value::Int32,
        )),
    }
}

/// Decode a parameter in binary format.
fn decode_param_binary(bytes: &[u8], oid: Option<u32>) -> Result<Value, DecodeError> {
    match oid.unwrap_or(0) {
        PG_OID_BOOL => {
            if bytes.len() != 1 {
                return Err(DecodeError::BadBytes);
            }
            Ok(Value::Bool(bytes[0] != 0))
        }
        PG_OID_INT2 => {
            let arr: [u8; 2] = bytes.try_into().map_err(|_| DecodeError::BadBytes)?;
            Ok(Value::Int16(i16::from_be_bytes(arr)))
        }
        PG_OID_INT4 => {
            let arr: [u8; 4] = bytes.try_into().map_err(|_| DecodeError::BadBytes)?;
            Ok(Value::Int32(i32::from_be_bytes(arr)))
        }
        PG_OID_INT8 => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| DecodeError::BadBytes)?;
            Ok(Value::Int64(i64::from_be_bytes(arr)))
        }
        PG_OID_OID | PG_OID_REGCLASS | PG_OID_REGTYPE => {
            let arr: [u8; 4] = bytes.try_into().map_err(|_| DecodeError::BadBytes)?;
            let decoded_oid = Oid::new(u32::from_be_bytes(arr));
            match oid.unwrap_or(0) {
                PG_OID_OID => Ok(Value::Oid(decoded_oid)),
                PG_OID_REGCLASS => Ok(Value::RegClass(decoded_oid)),
                PG_OID_REGTYPE => Ok(Value::RegType(decoded_oid)),
                _ => unreachable!(),
            }
        }
        PG_OID_PG_LSN => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| DecodeError::BadBytes)?;
            Ok(Value::PgLsn(Lsn::new(u64::from_be_bytes(arr))))
        }
        PG_OID_FLOAT4 => {
            let arr: [u8; 4] = bytes.try_into().map_err(|_| DecodeError::BadBytes)?;
            Ok(Value::Float32(f32::from_be_bytes(arr)))
        }
        PG_OID_FLOAT8 => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| DecodeError::BadBytes)?;
            Ok(Value::Float64(f64::from_be_bytes(arr)))
        }
        PG_OID_MONEY => decode_pg_money_binary(bytes).map_err(|_| DecodeError::BadBytes),
        PG_OID_NUMERIC => decode_pg_numeric_binary(bytes).map_err(|_| DecodeError::BadBytes),
        PG_OID_BIT | PG_OID_VARBIT => BitString::from_pg_binary(bytes)
            .map(Value::BitString)
            .ok_or(DecodeError::BadBytes),
        PG_OID_INET => NetworkValue::from_pg_binary(&DataType::Inet, bytes)
            .map(Value::Network)
            .ok_or(DecodeError::BadBytes),
        PG_OID_CIDR => NetworkValue::from_pg_binary(&DataType::Cidr, bytes)
            .map(Value::Network)
            .ok_or(DecodeError::BadBytes),
        PG_OID_MACADDR => NetworkValue::from_pg_binary(&DataType::MacAddr, bytes)
            .map(Value::Network)
            .ok_or(DecodeError::BadBytes),
        PG_OID_MACADDR8 => NetworkValue::from_pg_binary(&DataType::MacAddr8, bytes)
            .map(Value::Network)
            .ok_or(DecodeError::BadBytes),
        PG_OID_JSON => {
            let s = std::str::from_utf8(bytes).map_err(|_| DecodeError::BadBytes)?;
            validate_json_text_param(s).map(Value::Json)
        }
        PG_OID_JSONB => {
            let Some((&version, payload)) = bytes.split_first() else {
                return Err(DecodeError::BadBytes);
            };
            if version != JSONB_BINARY_VERSION {
                return Err(DecodeError::BadBytes);
            }
            let s = std::str::from_utf8(payload).map_err(|_| DecodeError::BadBytes)?;
            normalize_jsonb_param(s).map(Value::Jsonb)
        }
        PG_OID_XML => {
            let s = std::str::from_utf8(bytes).map_err(|_| DecodeError::BadBytes)?;
            validate_xml_text_param(s).map(Value::Xml)
        }
        PG_OID_DATE => {
            let arr: [u8; 4] = bytes.try_into().map_err(|_| DecodeError::BadBytes)?;
            Ok(Value::Date(i32::from_be_bytes(arr)))
        }
        PG_OID_TIME => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| DecodeError::BadBytes)?;
            Ok(Value::Time(i64::from_be_bytes(arr)))
        }
        PG_OID_TIMESTAMP => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| DecodeError::BadBytes)?;
            Ok(Value::Timestamp(i64::from_be_bytes(arr)))
        }
        PG_OID_TIMESTAMPTZ => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| DecodeError::BadBytes)?;
            Ok(Value::TimestampTz(i64::from_be_bytes(arr)))
        }
        PG_OID_TIMETZ => {
            if bytes.len() != 12 {
                return Err(DecodeError::BadBytes);
            }
            Ok(Value::TimeTz {
                micros: i64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]),
                offset_seconds: i32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            })
        }
        PG_OID_TEXT | PG_OID_VARCHAR | PG_OID_BPCHAR | PG_OID_TSVECTOR | PG_OID_TSQUERY => {
            let s = std::str::from_utf8(bytes).map_err(|_| DecodeError::BadBytes)?;
            Ok(Value::Text(s.to_string()))
        }
        PG_OID_BYTEA => Ok(Value::Bytea(bytes.to_vec())),
        // Unknown OID with binary format: fall back to widths we can
        // disambiguate by length.
        _ => match bytes.len() {
            1 => Ok(Value::Bool(bytes[0] != 0)),
            2 => Ok(Value::Int16(i16::from_be_bytes([bytes[0], bytes[1]]))),
            4 => Ok(Value::Int32(i32::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
            ]))),
            8 => Ok(Value::Int64(i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]))),
            _ => Ok(Value::Bytea(bytes.to_vec())),
        },
    }
}

fn validate_json_text_param(text: &str) -> Result<String, DecodeError> {
    serde_json::from_str::<serde_json::Value>(text).map_err(|_| DecodeError::BadBytes)?;
    Ok(text.to_owned())
}

fn normalize_jsonb_param(text: &str) -> Result<String, DecodeError> {
    let value =
        serde_json::from_str::<serde_json::Value>(text).map_err(|_| DecodeError::BadBytes)?;
    serde_json::to_string(&value).map_err(|_| DecodeError::BadBytes)
}

fn validate_xml_text_param(text: &str) -> Result<String, DecodeError> {
    Value::validate_xml_text(text).ok_or(DecodeError::BadBytes)
}

// ---------------------------------------------------------------------------
// Result-column binary encoder.
// ---------------------------------------------------------------------------

/// Encode column row `row` of `col` in binary format. Falls back to
/// the text encoder for value types whose binary layout is not yet
/// implemented in v0.5 — float types, dates/times, etc. The fallback
/// is conservative (returning the text form) so the client sees a
/// well-formed `DataRow` even if the format code says binary; libpq
/// does not validate that the wire format matches its requested
/// format byte-for-byte.
pub(super) fn encode_binary_value(
    col: &ultrasql_vec::column::Column,
    row: usize,
) -> Option<Vec<u8>> {
    use ultrasql_vec::column::Column;
    let nulls = match col {
        Column::Int32(c) => c.nulls(),
        Column::Int64(c) => c.nulls(),
        Column::Float32(c) => c.nulls(),
        Column::Float64(c) => c.nulls(),
        Column::Bool(c) => c.nulls(),
        Column::Utf8(c) => c.nulls(),
        Column::DictionaryUtf8(c) => c.codes.nulls(),
    };
    if let Some(b) = nulls {
        if !b.get(row) {
            return None;
        }
    }
    match col {
        Column::Bool(c) => Some(vec![u8::from(c.value(row))]),
        Column::Int32(c) => Some(c.data()[row].to_be_bytes().to_vec()),
        Column::Int64(c) => Some(c.data()[row].to_be_bytes().to_vec()),
        Column::Float32(c) => Some(c.data()[row].to_be_bytes().to_vec()),
        Column::Float64(c) => Some(c.data()[row].to_be_bytes().to_vec()),
        Column::Utf8(c) => Some(c.value(row).as_bytes().to_vec()),
        Column::DictionaryUtf8(c) => c.try_decode_at(row).map(|value| value.as_bytes().to_vec()),
    }
}

/// Encode one result column in binary format using the logical schema type.
///
/// Some physical vectors are widened (`Int16` is stored as `Int32`), so
/// extended-query binary output must narrow to the PostgreSQL type OID
/// advertised in `RowDescription`.
pub(super) fn encode_binary_value_typed(
    col: &ultrasql_vec::column::Column,
    row: usize,
    logical_type: &DataType,
) -> Option<Vec<u8>> {
    use ultrasql_vec::column::Column;
    let nulls = match col {
        Column::Int32(c) => c.nulls(),
        Column::Int64(c) => c.nulls(),
        Column::Float32(c) => c.nulls(),
        Column::Float64(c) => c.nulls(),
        Column::Bool(c) => c.nulls(),
        Column::Utf8(c) => c.nulls(),
        Column::DictionaryUtf8(c) => c.codes.nulls(),
    };
    if let Some(b) = nulls
        && !b.get(row)
    {
        return None;
    }
    match (logical_type, col) {
        (DataType::Int16, Column::Int32(c)) => i16::try_from(c.data()[row])
            .ok()
            .map(|v| v.to_be_bytes().to_vec()),
        (DataType::Decimal { scale, .. }, Column::Int64(c)) => {
            encode_pg_numeric_binary(c.data()[row], scale.unwrap_or(0)).ok()
        }
        (DataType::Decimal { .. }, Column::Utf8(_) | Column::DictionaryUtf8(_)) => {
            let Value::Decimal { value, scale } =
                parse_decimal_text(col.text_value(row)?, None).ok()?
            else {
                return None;
            };
            encode_pg_numeric_binary(value, scale).ok()
        }
        (DataType::Money, Column::Int64(c)) => Some(encode_pg_money_binary(c.data()[row]).to_vec()),
        (DataType::Oid | DataType::RegClass | DataType::RegType, Column::Int64(c)) => {
            u32::try_from(c.data()[row])
                .ok()
                .map(|raw| raw.to_be_bytes().to_vec())
        }
        (DataType::PgLsn, Column::Utf8(_) | Column::DictionaryUtf8(_)) => col
            .text_value(row)
            .and_then(Value::parse_pg_lsn_text)
            .map(|lsn| lsn.raw().to_be_bytes().to_vec()),
        (DataType::TimeTz, Column::Int64(c)) => {
            unpack_timetz(c.data()[row]).map(|(micros, offset_seconds)| {
                let mut out = Vec::with_capacity(12);
                out.extend_from_slice(&micros.to_be_bytes());
                out.extend_from_slice(&offset_seconds.to_be_bytes());
                out
            })
        }
        (DataType::Bit { .. } | DataType::VarBit { .. }, Column::Utf8(_))
        | (DataType::Bit { .. } | DataType::VarBit { .. }, Column::DictionaryUtf8(_)) => col
            .text_value(row)
            .and_then(Value::parse_bit_string)
            .and_then(|value| match value {
                Value::BitString(bits) => Some(bits.to_pg_binary()),
                _ => None,
            }),
        (
            DataType::Inet | DataType::Cidr | DataType::MacAddr | DataType::MacAddr8,
            Column::Utf8(_) | Column::DictionaryUtf8(_),
        ) => col
            .text_value(row)
            .and_then(|text| Value::parse_network(logical_type, text))
            .and_then(|value| match value {
                Value::Network(network) => Some(network.to_pg_binary()),
                _ => None,
            }),
        (DataType::Jsonb, Column::Utf8(_) | Column::DictionaryUtf8(_)) => {
            col.text_value(row).map(encode_pg_binary_jsonb)
        }
        _ => encode_binary_value(col, row),
    }
}

fn encode_pg_binary_jsonb(text: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(text.len() + 1);
    out.push(JSONB_BINARY_VERSION);
    out.extend_from_slice(text.as_bytes());
    out
}

// ---------------------------------------------------------------------------
// RowDescription builder.
// ---------------------------------------------------------------------------

/// Build a `RowDescription` for the output schema of `plan`, or
/// `NoData` for plans that yield no rows.
pub(crate) fn row_description_for_plan(plan: &LogicalPlan) -> BackendMessage {
    row_description_for_plan_with_formats(plan, &[])
}

/// Build a `RowDescription` for the output schema of `plan`, using the
/// result format codes negotiated for a bound portal.
pub(crate) fn row_description_for_plan_with_formats(
    plan: &LogicalPlan,
    result_formats: &[i16],
) -> BackendMessage {
    // DDL, transaction-control, and modify-without-returning produce no row data.
    let no_rows = matches!(
        plan,
        LogicalPlan::CreateTable { .. }
            | LogicalPlan::CreateMaterializedView { .. }
            | LogicalPlan::CreateTypeEnum { .. }
            | LogicalPlan::CreateTypeComposite { .. }
            | LogicalPlan::CreateDomain { .. }
            | LogicalPlan::CreateOperator { .. }
            | LogicalPlan::CreateIndex { .. }
            | LogicalPlan::DropIndex { .. }
            | LogicalPlan::CreateRole { .. }
            | LogicalPlan::AlterRole { .. }
            | LogicalPlan::DropRole { .. }
            | LogicalPlan::CreateSchema { .. }
            | LogicalPlan::DropSchema { .. }
            | LogicalPlan::DropTable { .. }
            | LogicalPlan::AlterTable { .. }
            | LogicalPlan::CreateSequence { .. }
            | LogicalPlan::AlterSequence { .. }
            | LogicalPlan::DropSequence { .. }
            | LogicalPlan::Comment { .. }
            | LogicalPlan::Truncate { .. }
            | LogicalPlan::Begin { .. }
            | LogicalPlan::Commit { .. }
            | LogicalPlan::Rollback { .. }
            | LogicalPlan::Savepoint { .. }
            | LogicalPlan::RollbackToSavepoint { .. }
            | LogicalPlan::ReleaseSavepoint { .. }
            | LogicalPlan::PrepareTransaction { .. }
            | LogicalPlan::CommitPrepared { .. }
            | LogicalPlan::RollbackPrepared { .. }
            | LogicalPlan::SetTransaction { .. }
            | LogicalPlan::Listen { .. }
            | LogicalPlan::Notify { .. }
            | LogicalPlan::Unlisten { .. }
    ) || matches!(plan, LogicalPlan::Insert { returning, .. } if returning.is_empty())
        || matches!(plan, LogicalPlan::Update { returning, .. } if returning.is_empty())
        || matches!(plan, LogicalPlan::Delete { returning, .. } if returning.is_empty());
    if no_rows {
        return BackendMessage::NoData;
    }
    let schema = plan.schema();
    let fields = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, f)| {
            let format_code = result_format_for_column(&f.data_type, result_formats, idx);
            FieldDescription {
                name: f.name.clone(),
                table_oid: 0,
                col_attnum: 0,
                type_oid: pg_type_oid(&f.data_type),
                type_size: pg_type_size(&f.data_type),
                type_modifier: -1,
                format_code,
            }
        })
        .collect();
    BackendMessage::RowDescription { fields }
}

const fn result_format_for_column(ty: &DataType, result_formats: &[i16], idx: usize) -> i16 {
    match result_formats.len() {
        0 => match ty {
            DataType::Float32 | DataType::Float64 => 1,
            _ => 0,
        },
        1 => result_formats[0],
        _ => {
            if idx < result_formats.len() {
                result_formats[idx]
            } else {
                0
            }
        }
    }
}

pub(super) fn pg_type_oid(ty: &DataType) -> u32 {
    match ty {
        DataType::Bool => PG_OID_BOOL,
        DataType::Int16 => PG_OID_INT2,
        DataType::Int32 => PG_OID_INT4,
        DataType::Int64 => PG_OID_INT8,
        DataType::Float32 => PG_OID_FLOAT4,
        DataType::Float64 => PG_OID_FLOAT8,
        DataType::Decimal { .. } => PG_OID_NUMERIC,
        DataType::Money => PG_OID_MONEY,
        DataType::Oid => PG_OID_OID,
        DataType::RegClass => PG_OID_REGCLASS,
        DataType::RegType => PG_OID_REGTYPE,
        DataType::PgLsn => PG_OID_PG_LSN,
        DataType::Char { .. } => PG_OID_BPCHAR,
        DataType::Bit { .. } => PG_OID_BIT,
        DataType::VarBit { .. } => PG_OID_VARBIT,
        DataType::Inet => PG_OID_INET,
        DataType::Cidr => PG_OID_CIDR,
        DataType::MacAddr => PG_OID_MACADDR,
        DataType::MacAddr8 => PG_OID_MACADDR8,
        DataType::Date => PG_OID_DATE,
        DataType::Time => PG_OID_TIME,
        DataType::Timestamp => PG_OID_TIMESTAMP,
        DataType::TimeTz => PG_OID_TIMETZ,
        DataType::TimestampTz => PG_OID_TIMESTAMPTZ,
        DataType::Bytea => PG_OID_BYTEA,
        DataType::Uuid => PG_OID_UUID,
        DataType::Json => PG_OID_JSON,
        DataType::Jsonb => PG_OID_JSONB,
        DataType::Xml => PG_OID_XML,
        DataType::TsVector => PG_OID_TSVECTOR,
        DataType::TsQuery => PG_OID_TSQUERY,
        DataType::Enum { oid, .. }
        | DataType::Composite { oid, .. }
        | DataType::Domain { oid, .. } => oid.raw(),
        DataType::Array(element) => pg_array_type_oid(element),
        DataType::Vector { .. } => PG_OID_TEXT,
        _ => PG_OID_TEXT,
    }
}

fn pg_array_type_oid(element: &DataType) -> u32 {
    match element {
        DataType::Bool => PG_OID_BOOL_ARRAY,
        DataType::Int16 => PG_OID_INT2_ARRAY,
        DataType::Int32 => PG_OID_INT4_ARRAY,
        DataType::Int64 => PG_OID_INT8_ARRAY,
        DataType::Float32 => PG_OID_FLOAT4_ARRAY,
        DataType::Float64 => PG_OID_FLOAT8_ARRAY,
        DataType::Decimal { .. } => PG_OID_NUMERIC_ARRAY,
        DataType::Money => PG_OID_MONEY_ARRAY,
        DataType::Oid => PG_OID_OID_ARRAY,
        DataType::RegClass => PG_OID_REGCLASS_ARRAY,
        DataType::RegType => PG_OID_REGTYPE_ARRAY,
        DataType::PgLsn => PG_OID_PG_LSN_ARRAY,
        DataType::Text { .. } => PG_OID_TEXT_ARRAY,
        DataType::Char { .. } => PG_OID_BPCHAR_ARRAY,
        DataType::Bit { .. } => PG_OID_BIT_ARRAY,
        DataType::VarBit { .. } => PG_OID_VARBIT_ARRAY,
        DataType::Inet => PG_OID_INET_ARRAY,
        DataType::Cidr => PG_OID_CIDR_ARRAY,
        DataType::MacAddr => PG_OID_MACADDR_ARRAY,
        DataType::MacAddr8 => PG_OID_MACADDR8_ARRAY,
        DataType::Date => PG_OID_DATE_ARRAY,
        DataType::Time => PG_OID_TIME_ARRAY,
        DataType::Timestamp => PG_OID_TIMESTAMP_ARRAY,
        DataType::TimeTz => PG_OID_TIMETZ_ARRAY,
        DataType::TimestampTz => PG_OID_TIMESTAMPTZ_ARRAY,
        DataType::Bytea => PG_OID_BYTEA_ARRAY,
        DataType::Uuid => PG_OID_UUID_ARRAY,
        DataType::Json => PG_OID_JSON_ARRAY,
        DataType::Jsonb => PG_OID_JSONB_ARRAY,
        DataType::Xml => PG_OID_XML_ARRAY,
        DataType::TsVector => PG_OID_TSVECTOR_ARRAY,
        DataType::TsQuery => PG_OID_TSQUERY_ARRAY,
        DataType::Array(inner) => pg_array_type_oid(inner),
        _ => PG_OID_TEXT_ARRAY,
    }
}

const fn pg_type_size(ty: &DataType) -> i16 {
    match ty {
        DataType::Bool => 1,
        DataType::Int16 => 2,
        DataType::Int32
        | DataType::Float32
        | DataType::Date
        | DataType::Oid
        | DataType::RegClass
        | DataType::RegType => 4,
        DataType::Int64
        | DataType::Money
        | DataType::Float64
        | DataType::Time
        | DataType::Timestamp
        | DataType::TimestampTz
        | DataType::PgLsn => 8,
        DataType::TimeTz => 12,
        _ => -1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ultrasql_vec::column::{Column, NumericColumn, StringColumn};
    use ultrasql_vec::dict::DictionaryColumn;

    fn pg_numeric_12_340() -> Vec<u8> {
        vec![
            0x00, 0x02, // ndigits
            0x00, 0x00, // weight
            0x00, 0x00, // sign
            0x00, 0x03, // dscale
            0x00, 0x0c, // 12
            0x0d, 0x48, // 3400
        ]
    }

    #[test]
    fn numeric_oid_maps_to_postgres_numeric() {
        assert_eq!(
            pg_type_oid(&DataType::Decimal {
                precision: Some(12),
                scale: Some(3)
            }),
            1700
        );
    }

    #[test]
    fn money_oid_maps_to_postgres_money() {
        assert_eq!(pg_type_oid(&DataType::Money), 790);
    }

    #[test]
    fn bit_oids_map_to_postgres_bit_families() {
        assert_eq!(pg_type_oid(&DataType::Bit { len: Some(4) }), 1560);
        assert_eq!(pg_type_oid(&DataType::VarBit { max_len: Some(6) }), 1562);
    }

    #[test]
    fn network_oids_map_to_postgres_network_families() {
        assert_eq!(pg_type_oid(&DataType::Inet), 869);
        assert_eq!(pg_type_oid(&DataType::Cidr), 650);
        assert_eq!(pg_type_oid(&DataType::MacAddr), 829);
        assert_eq!(pg_type_oid(&DataType::MacAddr8), 774);
    }

    #[test]
    fn json_oids_map_to_postgres_json_families() {
        assert_eq!(pg_type_oid(&DataType::Json), 114);
        assert_eq!(pg_type_oid(&DataType::Jsonb), 3802);
        assert_eq!(pg_type_oid(&DataType::Xml), 142);
    }

    #[test]
    fn array_oids_map_nested_arrays_to_postgres_base_array_family() {
        assert_eq!(
            pg_type_oid(&DataType::Array(Box::new(DataType::Array(Box::new(
                DataType::Int32
            ))))),
            1007
        );
        assert_eq!(
            pg_type_oid(&DataType::Array(Box::new(DataType::Jsonb))),
            3807
        );
        assert_eq!(pg_type_oid(&DataType::Array(Box::new(DataType::Xml))), 143);
    }

    #[test]
    fn text_json_parameters_validate_and_split_storage() {
        assert_eq!(
            decode_param_text(br#"{"b": 2, "a": 1}"#, Some(114)).unwrap(),
            Value::Json(r#"{"b": 2, "a": 1}"#.to_owned())
        );
        assert_eq!(
            decode_param_text(br#"{"b": 2, "a": 1}"#, Some(3802)).unwrap(),
            Value::Jsonb(r#"{"a":1,"b":2}"#.to_owned())
        );
        assert_eq!(
            decode_param_text(br#"<root><copy/></root>"#, Some(142)).unwrap(),
            Value::Xml("<root><copy/></root>".to_owned())
        );
        assert!(decode_param_text(br#"<root>"#, Some(142)).is_err());
        assert_eq!(
            decode_param_text(b"2000-07-01 00:00:00 America/New_York", Some(1184)).unwrap(),
            Value::TimestampTz(15_739_200_000_000)
        );
    }

    #[test]
    fn binary_jsonb_result_uses_pg_versioned_payload() {
        let column = Column::Utf8(StringColumn::from_data([r#"{"a":1}"#.to_owned()]));
        assert_eq!(
            encode_binary_value_typed(&column, 0, &DataType::Jsonb).unwrap(),
            [vec![1_u8], br#"{"a":1}"#.to_vec()].concat()
        );
    }

    #[test]
    fn binary_encoding_rejects_invalid_dictionary_code_without_panic() {
        let column = Column::DictionaryUtf8(DictionaryColumn {
            dict: vec!["ok".to_owned()],
            codes: NumericColumn::from_data(vec![7]),
        });

        assert_eq!(encode_binary_value(&column, 0), None);
        assert_eq!(
            encode_binary_value_typed(&column, 0, &DataType::Text { max_len: None }),
            None
        );
    }

    #[test]
    fn text_money_parameter_decodes_currency_format() {
        assert_eq!(
            decode_param_text(b"$1,234.56", Some(790)).unwrap(),
            Value::Money(123_456)
        );
    }

    #[test]
    fn binary_money_parameter_decodes_cash_i64() {
        assert_eq!(
            decode_param_binary(&123_456_i64.to_be_bytes(), Some(790)).unwrap(),
            Value::Money(123_456)
        );
    }

    #[test]
    fn binary_money_result_encodes_cash_i64() {
        let column = Column::Int64(NumericColumn::from_data(vec![123_456]));
        assert_eq!(
            encode_binary_value_typed(&column, 0, &DataType::Money).unwrap(),
            123_456_i64.to_be_bytes().to_vec()
        );
    }

    #[test]
    fn binary_bit_parameter_decodes_pg_bit_payload() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&4_i32.to_be_bytes());
        payload.push(0xa0);
        assert_eq!(
            decode_param_binary(&payload, Some(1560)).unwrap(),
            Value::parse_bit_string("1010").unwrap()
        );
    }

    #[test]
    fn binary_bit_result_encodes_pg_bit_payload() {
        let column = Column::Utf8(StringColumn::from_data(["1010".to_owned()]));
        let mut expected = Vec::new();
        expected.extend_from_slice(&4_i32.to_be_bytes());
        expected.push(0xa0);
        assert_eq!(
            encode_binary_value_typed(&column, 0, &DataType::Bit { len: Some(4) }).unwrap(),
            expected
        );
    }

    #[test]
    fn binary_inet_parameter_decodes_pg_network_payload() {
        let payload = [2, 24, 0, 4, 192, 168, 1, 5];
        assert_eq!(
            decode_param_binary(&payload, Some(869)).unwrap(),
            Value::parse_network(&DataType::Inet, "192.168.1.5/24").unwrap()
        );
    }

    #[test]
    fn binary_macaddr8_result_encodes_pg_network_payload() {
        let column = Column::Utf8(StringColumn::from_data([
            "08:00:2b:ff:fe:01:02:03".to_owned()
        ]));
        assert_eq!(
            encode_binary_value_typed(&column, 0, &DataType::MacAddr8).unwrap(),
            vec![0x08, 0x00, 0x2b, 0xff, 0xfe, 0x01, 0x02, 0x03]
        );
    }

    #[test]
    fn binary_numeric_parameter_decodes_pg_numeric_payload() {
        assert_eq!(
            decode_param_binary(&pg_numeric_12_340(), Some(1700)).unwrap(),
            Value::Decimal {
                value: 12_340,
                scale: 3
            }
        );
    }

    #[test]
    fn binary_numeric_result_encodes_pg_numeric_payload() {
        let column = Column::Int64(NumericColumn::from_data(vec![12_340]));
        assert_eq!(
            encode_binary_value_typed(
                &column,
                0,
                &DataType::Decimal {
                    precision: Some(12),
                    scale: Some(3)
                }
            )
            .unwrap(),
            pg_numeric_12_340()
        );
    }

    #[test]
    fn binary_dynamic_numeric_result_encodes_text_backed_scale() {
        let column = Column::Utf8(StringColumn::from_data(["12.340".to_owned()]));
        assert_eq!(
            encode_binary_value_typed(
                &column,
                0,
                &DataType::Decimal {
                    precision: None,
                    scale: None,
                }
            )
            .unwrap(),
            pg_numeric_12_340()
        );
    }
}
