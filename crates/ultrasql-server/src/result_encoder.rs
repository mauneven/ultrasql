//! Operator output -> PostgreSQL wire-protocol row encoding.
//!
//! The server drives a root [`Operator`] to completion, collecting
//! every emitted [`Batch`] and translating each column value into a
//! PostgreSQL text-format byte string. We use text format throughout
//! v0.5 because it round-trips trivially through psql and tokio
//! clients without requiring per-type binary encodings.
//!
//! A successful drain produces:
//!
//! 1. one [`BackendMessage::RowDescription`] with the operator's
//!    schema translated into per-column [`FieldDescription`]s,
//! 2. zero or more [`BackendMessage::DataRow`] messages,
//! 3. one [`BackendMessage::CommandComplete`] tagged
//!    `SELECT <count>`.
//!
//! The caller is responsible for emitting `ReadyForQuery` after the
//! [`run_select`] result (or after a converted error).
//!
//! [`Batch`]: ultrasql_vec::Batch

use ultrasql_core::{DataType, Schema};
use ultrasql_executor::Operator;
use ultrasql_protocol::{BackendMessage, FieldDescription};
use ultrasql_vec::column::Column;

use crate::error::ServerError;

/// PostgreSQL type OID for `bool`. Pulled from `pg_type.dat`.
const PG_OID_BOOL: u32 = 16;
/// PostgreSQL type OID for `int2`.
const PG_OID_INT2: u32 = 21;
/// PostgreSQL type OID for `int4`.
const PG_OID_INT4: u32 = 23;
/// PostgreSQL type OID for `int8`.
const PG_OID_INT8: u32 = 20;
/// PostgreSQL type OID for `float4`.
const PG_OID_FLOAT4: u32 = 700;
/// PostgreSQL type OID for `float8`.
const PG_OID_FLOAT8: u32 = 701;
/// PostgreSQL type OID for `text`.
const PG_OID_TEXT: u32 = 25;
/// PostgreSQL type OID for `bytea`.
const PG_OID_BYTEA: u32 = 17;
/// PostgreSQL format code 0 = text.
const FORMAT_TEXT: i16 = 0;

/// Outcome of draining a single `SELECT` execution: the messages to
/// send to the client, in transmission order.
#[derive(Debug)]
pub struct SelectResult {
    /// Ordered list of backend messages to emit, from `RowDescription`
    /// through `CommandComplete`.
    pub messages: Vec<BackendMessage>,
    /// Number of rows produced. Mirrors the value embedded in the
    /// trailing `CommandComplete` tag.
    pub rows: u64,
}

/// Wrap a DDL execution result as the wire messages PostgreSQL would
/// emit: a single `CommandComplete` tagged with the DDL command, no
/// `RowDescription` and no `DataRow`.
///
/// `tag` is the tag literal — `"CREATE TABLE"`, `"DROP TABLE"`,
/// `"CREATE INDEX"`, etc. The caller is responsible for emitting the
/// trailing `ReadyForQuery`.
#[must_use]
pub fn run_ddl_command(tag: &str) -> SelectResult {
    SelectResult {
        messages: vec![BackendMessage::CommandComplete {
            tag: tag.to_string(),
        }],
        rows: 0,
    }
}

/// Drive `op` to completion and produce the corresponding wire
/// messages.
///
/// The function buffers every output batch and translates row-by-row.
/// Streaming straight to the socket is an optimization for after the
/// connection loop matures — at v0.5, batches are small (3 rows in the
/// sample table) so memory pressure is negligible.
pub fn run_select(op: &mut dyn Operator) -> Result<SelectResult, ServerError> {
    let row_desc = build_row_description(op.schema());
    let mut messages = Vec::with_capacity(8);
    messages.push(row_desc);

    let mut rows: u64 = 0;
    loop {
        let Some(batch) = op.next_batch()? else { break };
        let row_count = batch.rows();
        for row in 0..row_count {
            let mut columns = Vec::with_capacity(batch.width());
            for col in batch.columns() {
                columns.push(encode_value(col, row));
            }
            messages.push(BackendMessage::DataRow { columns });
        }
        rows = rows.saturating_add(row_count as u64);
    }

    messages.push(BackendMessage::CommandComplete {
        tag: format!("SELECT {rows}"),
    });
    Ok(SelectResult { messages, rows })
}

/// Translate a [`Schema`] into a [`BackendMessage::RowDescription`].
fn build_row_description(schema: &Schema) -> BackendMessage {
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
            format_code: FORMAT_TEXT,
        })
        .collect();
    BackendMessage::RowDescription { fields }
}

/// Encode column row `i` as a PostgreSQL text-format value.
///
/// The protocol crate already encodes `None` as SQL `NULL` (length -1
/// on the wire), so we represent SQL NULL as `None`. Other values are
/// serialized into UTF-8 bytes using their natural display form.
fn encode_value(col: &Column, row: usize) -> Option<Vec<u8>> {
    if let Some(nulls) = column_nulls(col) {
        // Convention from `ultrasql-vec`: 1 bit = valid, 0 bit = null.
        if !nulls.get(row) {
            return None;
        }
    }
    match col {
        Column::Int32(c) => Some(c.data()[row].to_string().into_bytes()),
        Column::Int64(c) => Some(c.data()[row].to_string().into_bytes()),
        Column::Float32(c) => Some(format_f32(c.data()[row])),
        Column::Float64(c) => Some(format_f64(c.data()[row])),
        Column::Bool(c) => Some(if c.value(row) {
            b"t".to_vec()
        } else {
            b"f".to_vec()
        }),
        Column::Utf8(c) => Some(c.value(row).as_bytes().to_vec()),
    }
}

const fn column_nulls(col: &Column) -> Option<&ultrasql_vec::Bitmap> {
    match col {
        Column::Int32(c) => c.nulls(),
        Column::Int64(c) => c.nulls(),
        Column::Float32(c) => c.nulls(),
        Column::Float64(c) => c.nulls(),
        Column::Bool(c) => c.nulls(),
        Column::Utf8(c) => c.nulls(),
    }
}

/// Text-format float emission. PostgreSQL uses a `%g`-style format
/// with `"NaN"`, `"Infinity"`, `"-Infinity"` for the special values.
/// Rust's default `Display` is close enough for v0.5; richer
/// rounding-mode handling is on the follow-up list.
fn format_f32(v: f32) -> Vec<u8> {
    if v.is_nan() {
        return b"NaN".to_vec();
    }
    if v.is_infinite() {
        return if v > 0.0 {
            b"Infinity".to_vec()
        } else {
            b"-Infinity".to_vec()
        };
    }
    format!("{v}").into_bytes()
}

fn format_f64(v: f64) -> Vec<u8> {
    if v.is_nan() {
        return b"NaN".to_vec();
    }
    if v.is_infinite() {
        return if v > 0.0 {
            b"Infinity".to_vec()
        } else {
            b"-Infinity".to_vec()
        };
    }
    format!("{v}").into_bytes()
}

/// Map an UltraSQL [`DataType`] to a PostgreSQL type OID. Types that
/// have no representation yet fall back to `text`; that keeps the
/// driver happy until proper coverage lands.
const fn pg_type_oid(ty: &DataType) -> u32 {
    match ty {
        DataType::Bool => PG_OID_BOOL,
        DataType::Int16 => PG_OID_INT2,
        DataType::Int32 => PG_OID_INT4,
        DataType::Int64 => PG_OID_INT8,
        DataType::Float32 => PG_OID_FLOAT4,
        DataType::Float64 => PG_OID_FLOAT8,
        DataType::Bytea => PG_OID_BYTEA,
        _ => PG_OID_TEXT,
    }
}

/// Map a [`DataType`] to the wire-protocol `type_size` field. Negative
/// values denote a variable-length type.
const fn pg_type_size(ty: &DataType) -> i16 {
    match ty {
        DataType::Bool => 1,
        DataType::Int16 => 2,
        DataType::Int32 | DataType::Float32 => 4,
        DataType::Int64 | DataType::Float64 => 8,
        _ => -1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ultrasql_core::{Field, Schema};
    use ultrasql_executor::MemTableScan;
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    #[test]
    fn run_select_produces_row_description_then_data_rows() {
        let schema = Schema::new([Field::required("id", DataType::Int32)]).unwrap();
        let batch = Batch::new([Column::Int32(NumericColumn::from_data(vec![1, 2, 3]))]).unwrap();
        let mut scan = MemTableScan::new(schema, vec![batch]);
        let result = run_select(&mut scan).expect("ok");

        assert_eq!(result.rows, 3);
        assert!(matches!(
            result.messages[0],
            BackendMessage::RowDescription { .. }
        ));
        for msg in &result.messages[1..result.messages.len() - 1] {
            assert!(matches!(msg, BackendMessage::DataRow { .. }));
        }
        match result.messages.last().unwrap() {
            BackendMessage::CommandComplete { tag } => assert_eq!(tag, "SELECT 3"),
            other => panic!("expected CommandComplete, got {other:?}"),
        }
    }

    #[test]
    fn empty_result_produces_command_complete_with_zero() {
        let schema = Schema::new([Field::required("id", DataType::Int32)]).unwrap();
        let mut scan = MemTableScan::new(schema, vec![]);
        let result = run_select(&mut scan).expect("ok");
        assert_eq!(result.rows, 0);
        match result.messages.last().unwrap() {
            BackendMessage::CommandComplete { tag } => assert_eq!(tag, "SELECT 0"),
            other => panic!("expected CommandComplete, got {other:?}"),
        }
    }

    #[test]
    fn type_oid_mapping_uses_postgres_codes() {
        assert_eq!(pg_type_oid(&DataType::Int32), 23);
        assert_eq!(pg_type_oid(&DataType::Int64), 20);
        assert_eq!(pg_type_oid(&DataType::Float64), 701);
        assert_eq!(pg_type_oid(&DataType::Bool), 16);
        assert_eq!(pg_type_oid(&DataType::Text { max_len: None }), 25);
    }
}
