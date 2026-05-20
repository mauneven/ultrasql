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

use bytes::BytesMut;
use ultrasql_core::{DataType, Schema, Value};
use ultrasql_executor::Operator;
use ultrasql_protocol::{BackendMessage, FieldDescription, encode_backend};
use ultrasql_vec::column::Column;

use crate::error::ServerError;
use crate::wire_writer::{
    write_data_row_typed, write_int32_int64_pair_data_rows, write_int32_pair_data_rows,
};

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
/// PostgreSQL type OID for `uuid`.
const PG_OID_UUID: u32 = 2950;
/// PostgreSQL type OID for `jsonb`.
const PG_OID_JSONB: u32 = 3802;
/// PostgreSQL format code 0 = text.
const FORMAT_TEXT: i16 = 0;

/// Outcome of draining a single `SELECT` execution: the messages to
/// send to the client, in transmission order.
///
/// The result is dispatched by `Session::send_query_result`,
/// which prefers `streamed_body` when present (the SELECT hot-path
/// streams its own wire bytes) and otherwise coalesces every entry of
/// `messages` into a single `write_all` + `flush`.
#[derive(Debug)]
pub struct SelectResult {
    /// Ordered list of backend messages to emit, from `RowDescription`
    /// through `CommandComplete`. Used by every non-SELECT path
    /// (DDL/DML tags, txn-control notices and tags, error envelopes)
    /// and by the legacy `run_select` for callers that still want the
    /// fully-materialised `BackendMessage` shape (tests, fallbacks).
    pub messages: Vec<BackendMessage>,
    /// Optional pre-encoded wire-bytes blob produced by
    /// [`stream_select`]. When `Some(_)`, the session sends this body
    /// directly and ignores `messages`. The body contains
    /// `RowDescription` + N `DataRow` + `CommandComplete` in
    /// transmission order — the same sequence the legacy path would
    /// have built as enum values and then encoded.
    pub streamed_body: Option<BytesMut>,
    /// Optional immutable pre-encoded body shared across repeated
    /// executions of the same cached scan shape. When `Some(_)`, the
    /// session writes this body directly and only appends the dynamic
    /// trailer bytes (`NotificationResponse`s, `ReadyForQuery`).
    pub shared_streamed_body: Option<std::sync::Arc<[u8]>>,
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
        streamed_body: None,
        shared_streamed_body: None,
        rows: 0,
    }
}

/// Drive a `ModifyTable` operator to completion and emit the
/// PostgreSQL-compatible row-count `CommandComplete` tag.
///
/// `command` is one of `"INSERT"`, `"UPDATE"`, `"DELETE"`. `INSERT`
/// uses the legacy `INSERT 0 N` shape (the `0` is the historical OID
/// slot for the inserted row); the others use `UPDATE N` / `DELETE N`.
///
/// The operator's output schema is the single-column
/// `affected_rows: Int64` produced by `ModifyTable`; this function
/// reads that column and folds it into the row-count.
pub fn run_modify_command(
    op: &mut dyn Operator,
    command: &str,
) -> Result<SelectResult, ServerError> {
    let mut affected: i64 = 0;
    while let Some(batch) = op.next_batch()? {
        if batch.rows() == 0 {
            continue;
        }
        if let Some(Column::Int64(c)) = batch.columns().first() {
            let data = c.data();
            if !data.is_empty() {
                affected = affected.saturating_add(data[0]);
            }
        }
    }
    let tag = modify_command_tag(command, u64::try_from(affected.max(0)).unwrap_or(0));
    let rows = u64::try_from(affected.max(0)).unwrap_or(0);
    Ok(SelectResult {
        messages: vec![BackendMessage::CommandComplete { tag }],
        streamed_body: None,
        shared_streamed_body: None,
        rows,
    })
}

/// Drive a DML operator that emits `RETURNING` rows and rewrite the
/// trailing `CommandComplete` tag from `SELECT n` to the PostgreSQL DML
/// tag shape (`INSERT 0 n`, `UPDATE n`, `DELETE n`).
pub fn run_modify_returning(
    op: &mut dyn Operator,
    command: &str,
) -> Result<SelectResult, ServerError> {
    let mut result = run_select(op)?;
    let tag = modify_command_tag(command, result.rows);
    let Some(BackendMessage::CommandComplete { tag: current_tag }) = result.messages.last_mut()
    else {
        return Err(ServerError::Unsupported(
            "RETURNING result missing trailing CommandComplete",
        ));
    };
    *current_tag = tag;
    Ok(result)
}

/// Drive `op` to completion and produce the corresponding wire
/// messages.
///
/// The function buffers every output batch and translates row-by-row.
/// Streaming straight to the socket is an optimization for after the
/// connection loop matures — at v0.5, batches are small (3 rows in the
/// sample table) so memory pressure is negligible.
///
/// Retained for callers that need the full `Vec<BackendMessage>` shape
/// (tests, the Extended Query path before its own streaming refactor,
/// and the txn-error / fallback paths in `lib.rs`). The hot path for
/// Simple Query SELECT now goes through [`stream_select`].
pub fn run_select(op: &mut dyn Operator) -> Result<SelectResult, ServerError> {
    let schema = op.schema().clone();
    let row_desc = build_row_description(&schema);
    let mut messages = Vec::with_capacity(8);
    messages.push(row_desc);

    let mut rows: u64 = 0;
    loop {
        let Some(batch) = op.next_batch()? else { break };
        let row_count = batch.rows();
        for row in 0..row_count {
            let mut columns = Vec::with_capacity(batch.width());
            for (idx, col) in batch.columns().iter().enumerate() {
                columns.push(encode_text_value_typed(
                    col,
                    row,
                    &schema.field_at(idx).data_type,
                ));
            }
            messages.push(BackendMessage::DataRow { columns });
        }
        rows = rows.saturating_add(u64::try_from(row_count).unwrap_or(u64::MAX));
    }

    messages.push(BackendMessage::CommandComplete {
        tag: format!("SELECT {rows}"),
    });
    Ok(SelectResult {
        messages,
        streamed_body: None,
        shared_streamed_body: None,
        rows,
    })
}

fn modify_command_tag(command: &str, affected: u64) -> String {
    if command.eq_ignore_ascii_case("INSERT") {
        format!("INSERT 0 {affected}")
    } else {
        format!("{} {affected}", command.to_uppercase())
    }
}

/// Drain `op` to completion, encoding every wire byte (`RowDescription`
/// + N `DataRow` + `CommandComplete`) directly into `sink`. The caller
/// is responsible for issuing the single `write_all` + `flush` after
/// this returns and for emitting the trailing `ReadyForQuery`.
///
/// This is the production hot path for `SELECT ...` Simple Query
/// execution. Compared to [`run_select`] it eliminates:
///
/// - the per-row `Vec<Option<Vec<u8>>>` allocation for the
///   `BackendMessage::DataRow` payload (one `Vec` heap allocation, plus
///   one per cell);
/// - the per-cell text-format integer allocation (`i.to_string().into_bytes()`
///   would heap-allocate twice per cell: once for the `String` body,
///   once when `into_bytes` re-owns the underlying buffer);
/// - the per-message encode-then-send loop in `Session::handle_query`,
///   which previously issued one `write_all` + `flush` per `DataRow`
///   (i.e. ~10 000 short writes for `select_scan_10k`).
///
/// Wire format is bit-identical to what `encode_backend` produces for
/// the equivalent `BackendMessage::DataRow { columns }` (asserted in
/// `wire_writer::tests`).
///
/// Returns the row count so the caller can update session bookkeeping;
/// the same count is embedded in the `CommandComplete` tag that this
/// function already wrote into `sink`.
pub fn stream_select(op: &mut dyn Operator, sink: &mut BytesMut) -> Result<u64, ServerError> {
    let schema = op.schema().clone();
    let row_desc = build_row_description(&schema);
    encode_backend(&row_desc, sink);

    let mut rows: u64 = 0;
    loop {
        let Some(batch) = op.next_batch()? else { break };
        let row_count = batch.rows();
        let columns = batch.columns();
        // Fast path: when the batch is exactly two non-nullable
        // `Int32` columns (the `select_scan_10k` shape), use the
        // specialised bulk writer that pre-reserves the buffer once
        // and emits every row without per-cell enum dispatch.
        if schema_is_int32_pair(&schema)
            && let [Column::Int32(a), Column::Int32(b)] = columns
            && a.nulls().is_none()
            && b.nulls().is_none()
        {
            write_int32_pair_data_rows(sink, a.data(), b.data());
            rows = rows.saturating_add(u64::try_from(row_count).unwrap_or(u64::MAX));
            continue;
        }
        // Fast path: `(Int32, Int64)` is the
        // `WindowAgg::try_columnar_row_number` output shape used by
        // `SELECT id, row_number() OVER (ORDER BY x) FROM t`. The
        // writer accepts optional validity bitmaps so it stays
        // correct when either side carries NULLs.
        if schema_is_int32_int64_pair(&schema)
            && let [Column::Int32(a), Column::Int64(b)] = columns
        {
            write_int32_int64_pair_data_rows(sink, a.data(), a.nulls(), b.data(), b.nulls());
            rows = rows.saturating_add(u64::try_from(row_count).unwrap_or(u64::MAX));
            continue;
        }
        for row in 0..row_count {
            write_data_row_typed(sink, columns, &schema, row);
        }
        rows = rows.saturating_add(u64::try_from(row_count).unwrap_or(u64::MAX));
    }

    let tag = format!("SELECT {rows}");
    encode_backend(&BackendMessage::CommandComplete { tag }, sink);
    Ok(rows)
}

// Session-owned wire-buffer reuse. The SELECT streaming path writes
// directly into the caller-provided `BytesMut`, which the session then
// keeps across queries. Reusing the buffer at the connection level is
// stable under Tokio task migration and avoids any shared-pool
// contention between sessions.
fn prepare_stream_sink(sink: &mut BytesMut, initial_cap: usize) {
    sink.clear();
    if sink.capacity() < initial_cap {
        sink.reserve(initial_cap - sink.capacity());
    }
}

/// Convenience wrapper around [`stream_select`] that returns a
/// [`SelectResult`] whose `streamed_body` field carries the encoded
/// wire bytes. Drop-in replacement for [`run_select`] at the SELECT
/// dispatch site in `run_plan_in_txn`.
///
/// The `messages` field is left empty by design: the session sends
/// the streamed body verbatim and never iterates `messages` when one
/// is present. The row-count is propagated so callers (e.g. autocommit
/// finalisation) keep their behaviour unchanged.
pub fn run_select_streamed(
    op: &mut dyn Operator,
    sink: &mut BytesMut,
) -> Result<SelectResult, ServerError> {
    // Initial capacity: when the operator advertises its row count
    // (column-cache replay, materialised CTE, LIMIT n) we can size
    // the buffer to the exact wire-byte budget upfront and skip
    // every `BytesMut::reserve` reallocation. The width estimate
    // assumes each column expands to a 5-cell-overhead text-format
    // datum averaging 8 bytes — generous enough for typical int /
    // small-string scans without wasting more than one growth cycle
    // when the relation is wider. Without a hint, fall back to the
    // 32 KiB starting size so small queries stay on one allocation.
    // DataRow wire layout per row: 1B tag + 4B length + 2B ncols +
    // per column (4B length + ascii text). For a typical int column
    // the ascii text is ~5-10 bytes; varchar columns can be wider.
    // We size to 8B per column to stay tight on the common
    // narrow-int case (the bench's `(id INT, val INT)` lands at
    // ~25 B/row, and an 8 B/col bound puts us at 24 B/row + 7 B
    // overhead = 31 B/row, just over the actual width).
    //
    // One extra growth cycle is cheap; over-allocating doubles the
    // initial syscall cost (page-fault the pre-touched bytes), so
    // we lean tight rather than generous.
    const PER_ROW_OVERHEAD_BYTES: usize = 7; // tag + length + ncols
    const PER_CELL_BYTES_ESTIMATE: usize = 12; // 4B length + ~8B text
    const ROWDESC_AND_TAG_BYTES: usize = 256;
    let initial_cap = match op.estimated_row_count() {
        Some(rows) => {
            let cols = op.schema().len().max(1);
            let body = rows.saturating_mul(PER_ROW_OVERHEAD_BYTES + cols * PER_CELL_BYTES_ESTIMATE);
            body.saturating_add(ROWDESC_AND_TAG_BYTES)
        }
        None => 32 * 1024,
    };
    let mut body = std::mem::take(sink);
    prepare_stream_sink(&mut body, initial_cap);
    match stream_select(op, &mut body) {
        Ok(rows) => Ok(SelectResult {
            messages: Vec::new(),
            streamed_body: Some(body),
            shared_streamed_body: None,
            rows,
        }),
        Err(e) => {
            body.clear();
            *sink = body;
            Err(e)
        }
    }
}

/// Fast path for a cached full-table `(Int32, Int32)` scan.
///
/// Unlike [`run_select_streamed`], this helper skips operator batch
/// materialisation entirely: callers provide the cached column slices
/// directly and the wire encoder writes `RowDescription`, every
/// `DataRow`, and the trailing `CommandComplete` into one pooled
/// buffer. Used by the `select_scan_10k` hot path once the relation's
/// column cache is warm.
#[must_use]
pub fn run_cached_int32_pair_select_streamed(
    schema: &Schema,
    left: &[i32],
    right: &[i32],
    sink: &mut BytesMut,
) -> SelectResult {
    debug_assert_eq!(left.len(), right.len());

    const MAX_ROW_BYTES: usize = 37;
    const ROWDESC_AND_TAG_BYTES: usize = 256;
    let initial_cap =
        ROWDESC_AND_TAG_BYTES.saturating_add(left.len().saturating_mul(MAX_ROW_BYTES));
    let mut body = std::mem::take(sink);
    prepare_stream_sink(&mut body, initial_cap);
    let row_desc = build_row_description(schema);
    encode_backend(&row_desc, &mut body);
    write_int32_pair_data_rows(&mut body, left, right);
    let rows = u64::try_from(left.len()).unwrap_or(u64::MAX);
    encode_backend(
        &BackendMessage::CommandComplete {
            tag: format!("SELECT {rows}"),
        },
        &mut body,
    );
    SelectResult {
        messages: Vec::new(),
        streamed_body: Some(body),
        shared_streamed_body: None,
        rows,
    }
}

/// Reuse a previously encoded SELECT wire body by copying it into the
/// session-owned stream buffer.
#[must_use]
pub fn run_preencoded_select_streamed(
    encoded_body: &[u8],
    rows: u64,
    sink: &mut BytesMut,
) -> SelectResult {
    let mut body = std::mem::take(sink);
    prepare_stream_sink(&mut body, encoded_body.len());
    body.extend_from_slice(encoded_body);
    SelectResult {
        messages: Vec::new(),
        streamed_body: Some(body),
        shared_streamed_body: None,
        rows,
    }
}

/// Reuse a previously encoded SELECT wire body without copying it into
/// a session-local buffer.
#[must_use]
pub fn run_shared_preencoded_select_streamed(
    encoded_body: std::sync::Arc<[u8]>,
    rows: u64,
) -> SelectResult {
    SelectResult {
        messages: Vec::new(),
        streamed_body: None,
        shared_streamed_body: Some(encoded_body),
        rows,
    }
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

/// Encode column row `row` of `col` as a PostgreSQL text-format value.
///
/// The protocol crate already encodes `None` as SQL `NULL` (length -1
/// on the wire), so we represent SQL NULL as `None`. Other values are
/// serialized into UTF-8 bytes using their natural display form.
///
/// `pub(crate)` because the Extended Query dispatcher in `extended.rs`
/// shares this encoder for any result column the client requested in
/// text format (the default per the protocol spec).
pub(crate) fn encode_text_value(col: &Column, row: usize) -> Option<Vec<u8>> {
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
        Column::DictionaryUtf8(c) => Some(c.decode_at(row).as_bytes().to_vec()),
    }
}

/// Encode a physical column cell using the logical schema type.
///
/// Batch columns use compact physical layouts: `DATE` shares `Int32`,
/// `DECIMAL` shares `Int64`. Wire text must expose SQL values, not storage
/// integers.
pub(crate) fn encode_text_value_typed(
    col: &Column,
    row: usize,
    logical_type: &DataType,
) -> Option<Vec<u8>> {
    if let Some(nulls) = column_nulls(col) {
        if !nulls.get(row) {
            return None;
        }
    }
    match (logical_type, col) {
        (DataType::Date, Column::Int32(c)) => Some(Value::Date(c.data()[row]).to_string().into()),
        (DataType::Decimal { scale, .. }, Column::Int64(c)) => Some(
            Value::Decimal {
                value: c.data()[row],
                scale: scale.unwrap_or(0),
            }
            .to_string()
            .into(),
        ),
        (ty, Column::Utf8(_) | Column::DictionaryUtf8(_)) if ty.is_vector_family() => col
            .text_value(row)
            .map(|text| encode_vector_family_text_value(text, ty)),
        _ => encode_text_value(col, row),
    }
}

fn encode_vector_family_text_value(text: &str, expected_type: &DataType) -> Vec<u8> {
    let parsed = match expected_type {
        DataType::Vector { .. } => Value::parse_vector(text),
        DataType::HalfVec { .. } => Value::parse_halfvec(text),
        DataType::SparseVec { .. } => Value::parse_sparsevec(text),
        DataType::BitVec { .. } => Value::parse_bitvec(text),
        _ => None,
    };
    if let Some(value) = parsed
        && vector_family_value_matches(expected_type, &value)
    {
        return value.to_string().into_bytes();
    }
    text.as_bytes().to_vec()
}

fn vector_family_value_matches(expected: &DataType, value: &Value) -> bool {
    let actual = value.data_type();
    vector_family_kind(expected) == vector_family_kind(&actual)
        && dims_compatible(
            expected.vector_dims().flatten(),
            actual.vector_dims().flatten(),
        )
}

fn vector_family_kind(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Vector { .. } => Some(0),
        DataType::HalfVec { .. } => Some(1),
        DataType::SparseVec { .. } => Some(2),
        DataType::BitVec { .. } => Some(3),
        _ => None,
    }
}

const fn dims_compatible(left: Option<u32>, right: Option<u32>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left == right,
        _ => true,
    }
}

fn schema_is_int32_pair(schema: &Schema) -> bool {
    schema.len() == 2
        && matches!(&schema.field_at(0).data_type, DataType::Int32)
        && matches!(&schema.field_at(1).data_type, DataType::Int32)
}

fn schema_is_int32_int64_pair(schema: &Schema) -> bool {
    schema.len() == 2
        && matches!(&schema.field_at(0).data_type, DataType::Int32)
        && matches!(&schema.field_at(1).data_type, DataType::Int64)
}

const fn column_nulls(col: &Column) -> Option<&ultrasql_vec::Bitmap> {
    match col {
        Column::Int32(c) => c.nulls(),
        Column::Int64(c) => c.nulls(),
        Column::Float32(c) => c.nulls(),
        Column::Float64(c) => c.nulls(),
        Column::Bool(c) => c.nulls(),
        Column::Utf8(c) => c.nulls(),
        Column::DictionaryUtf8(c) => c.codes.nulls(),
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
        DataType::Uuid => PG_OID_UUID,
        DataType::Jsonb => PG_OID_JSONB,
        DataType::Vector { .. } => PG_OID_TEXT,
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
        DataType::Uuid => 16,
        _ => -1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ultrasql_core::{Field, Schema};
    use ultrasql_executor::MemTableScan;
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn, StringColumn};

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
        assert_eq!(pg_type_oid(&DataType::Jsonb), 3802);
        assert_eq!(pg_type_oid(&DataType::Vector { dims: Some(3) }), 25);
    }

    #[test]
    fn run_select_encodes_logical_date_and_decimal_text() {
        let schema = Schema::new([
            Field::required("d", DataType::Date),
            Field::required(
                "price",
                DataType::Decimal {
                    precision: Some(15),
                    scale: Some(2),
                },
            ),
        ])
        .unwrap();
        let batch = Batch::new([
            Column::Int32(NumericColumn::from_data(vec![0])),
            Column::Int64(NumericColumn::from_data(vec![17_366_547])),
        ])
        .unwrap();
        let mut scan = MemTableScan::new(schema, vec![batch]);
        let result = run_select(&mut scan).expect("ok");

        match &result.messages[1] {
            BackendMessage::DataRow { columns } => {
                assert_eq!(columns[0], Some(b"2000-01-01".to_vec()));
                assert_eq!(columns[1], Some(b"173665.47".to_vec()));
            }
            other => panic!("expected DataRow, got {other:?}"),
        }
    }

    #[test]
    fn run_select_encodes_logical_vector_text() {
        let schema = Schema::new([Field::required(
            "embedding",
            DataType::Vector { dims: Some(3) },
        )])
        .unwrap();
        let batch = Batch::new([Column::Utf8(StringColumn::from_data([
            "[1, 2.500, -3]".to_owned()
        ]))])
        .unwrap();
        let mut scan = MemTableScan::new(schema, vec![batch]);
        let result = run_select(&mut scan).expect("ok");

        match &result.messages[0] {
            BackendMessage::RowDescription { fields } => {
                assert_eq!(fields[0].type_oid, PG_OID_TEXT);
                assert_eq!(fields[0].type_size, -1);
            }
            other => panic!("expected RowDescription, got {other:?}"),
        }
        match &result.messages[1] {
            BackendMessage::DataRow { columns } => {
                assert_eq!(columns[0], Some(b"[1,2.5,-3]".to_vec()));
            }
            other => panic!("expected DataRow, got {other:?}"),
        }
    }
}
