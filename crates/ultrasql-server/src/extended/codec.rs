//! Wire encoders and decoders used by the Extended Query protocol path:
//! parameter decode (text + binary), result-column binary encode,
//! `RowDescription` synthesis from a logical plan, and the SELECT/INSERT/
//! UPDATE/DELETE command-complete tag formatter.

use ultrasql_core::{DataType, Value};
use ultrasql_planner::LogicalPlan;
use ultrasql_protocol::{BackendMessage, FieldDescription};

use super::{
    PG_OID_BOOL, PG_OID_BPCHAR, PG_OID_BYTEA, PG_OID_FLOAT4, PG_OID_FLOAT8, PG_OID_INT2,
    PG_OID_INT4, PG_OID_INT8, PG_OID_OID, PG_OID_TEXT, PG_OID_VARCHAR,
};

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
        PG_OID_INT8 | PG_OID_OID => s
            .parse::<i64>()
            .map(Value::Int64)
            .map_err(|_| DecodeError::BadBytes),
        PG_OID_FLOAT4 => s
            .parse::<f32>()
            .map(Value::Float32)
            .map_err(|_| DecodeError::BadBytes),
        PG_OID_FLOAT8 => s
            .parse::<f64>()
            .map(Value::Float64)
            .map_err(|_| DecodeError::BadBytes),
        PG_OID_TEXT | PG_OID_VARCHAR | PG_OID_BPCHAR => Ok(Value::Text(s.to_string())),
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
        PG_OID_INT8 | PG_OID_OID => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| DecodeError::BadBytes)?;
            Ok(Value::Int64(i64::from_be_bytes(arr)))
        }
        PG_OID_FLOAT4 => {
            let arr: [u8; 4] = bytes.try_into().map_err(|_| DecodeError::BadBytes)?;
            Ok(Value::Float32(f32::from_be_bytes(arr)))
        }
        PG_OID_FLOAT8 => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| DecodeError::BadBytes)?;
            Ok(Value::Float64(f64::from_be_bytes(arr)))
        }
        PG_OID_TEXT | PG_OID_VARCHAR | PG_OID_BPCHAR => {
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
        Column::DictionaryUtf8(c) => Some(c.decode_at(row).as_bytes().to_vec()),
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
        _ => encode_binary_value(col, row),
    }
}

// ---------------------------------------------------------------------------
// RowDescription builder.
// ---------------------------------------------------------------------------

/// Build a `RowDescription` for the output schema of `plan`, or
/// `NoData` for plans that yield no rows.
pub(crate) fn row_description_for_plan(plan: &LogicalPlan) -> BackendMessage {
    // DDL, transaction-control, and modify-without-returning produce no row data.
    let no_rows = matches!(
        plan,
        LogicalPlan::CreateTable { .. }
            | LogicalPlan::CreateIndex { .. }
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
        .map(|f| FieldDescription {
            name: f.name.clone(),
            table_oid: 0,
            col_attnum: 0,
            type_oid: pg_type_oid(&f.data_type),
            type_size: pg_type_size(&f.data_type),
            type_modifier: -1,
            format_code: 0,
        })
        .collect();
    BackendMessage::RowDescription { fields }
}

pub(super) const fn pg_type_oid(ty: &DataType) -> u32 {
    match ty {
        DataType::Bool => PG_OID_BOOL,
        DataType::Int16 => PG_OID_INT2,
        DataType::Int32 => PG_OID_INT4,
        DataType::Int64 => PG_OID_INT8,
        DataType::Float32 => PG_OID_FLOAT4,
        DataType::Float64 => PG_OID_FLOAT8,
        DataType::Bytea => PG_OID_BYTEA,
        DataType::Vector { .. } => PG_OID_TEXT,
        _ => PG_OID_TEXT,
    }
}

const fn pg_type_size(ty: &DataType) -> i16 {
    match ty {
        DataType::Bool => 1,
        DataType::Int16 => 2,
        DataType::Int32 | DataType::Float32 => 4,
        DataType::Int64 | DataType::Float64 => 8,
        _ => -1,
    }
}

// ---------------------------------------------------------------------------
// Tag inference for the CommandComplete message.
// ---------------------------------------------------------------------------

/// Compute the `CommandComplete` tag for a plan. Used only when the
/// plan is a SELECT-like shape (Insert/Update/Delete have their own
/// tag-emitting paths through `run_modify_command`).
#[allow(dead_code)] // Kept for future use when Execute paths grow.
pub(super) fn select_tag(rows: u64) -> String {
    format!("SELECT {rows}")
}
