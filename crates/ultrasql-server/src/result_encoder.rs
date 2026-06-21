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
use chrono::{Datelike, NaiveDate};
use std::collections::HashMap;
use ultrasql_core::{
    DataType, Schema, Value, date_parts_from_days, format_date_days, format_money_text,
    format_money_text_with_locale, format_time_micros, format_timestamp_micros,
    format_timestamptz_micros_in_timezone, format_timestamptz_micros_utc, format_timetz,
    format_timezone_offset_seconds, timestamp_parts_from_micros, timestamptz_display_in_timezone,
    unpack_timetz,
};
use ultrasql_executor::Operator;
use ultrasql_protocol::{BackendMessage, FieldDescription, encode_backend};
use ultrasql_txn::Transaction;
use ultrasql_vec::column::Column;

use crate::error::ServerError;
use crate::wire_writer::{
    write_data_row_typed_with_options, write_int32_int64_pair_data_rows, write_int32_pair_data_rows,
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
/// PostgreSQL type OID for `numeric`.
const PG_OID_NUMERIC: u32 = 1700;
/// PostgreSQL type OID for `money`.
const PG_OID_MONEY: u32 = 790;
/// PostgreSQL type OID for `oid`.
const PG_OID_OID: u32 = 26;
/// PostgreSQL type OID for `regclass`.
const PG_OID_REGCLASS: u32 = 2205;
/// PostgreSQL type OID for `regtype`.
const PG_OID_REGTYPE: u32 = 2206;
/// PostgreSQL type OID for `pg_lsn`.
const PG_OID_PG_LSN: u32 = 3220;
/// PostgreSQL type OID for `text`.
const PG_OID_TEXT: u32 = 25;
/// PostgreSQL type OID for `bpchar` (`CHAR(n)`).
const PG_OID_BPCHAR: u32 = 1042;
/// PostgreSQL type OID for `bit`.
const PG_OID_BIT: u32 = 1560;
/// PostgreSQL type OID for `varbit`.
const PG_OID_VARBIT: u32 = 1562;
/// PostgreSQL type OID for `cidr`.
const PG_OID_CIDR: u32 = 650;
/// PostgreSQL type OID for `inet`.
const PG_OID_INET: u32 = 869;
/// PostgreSQL type OID for `macaddr`.
const PG_OID_MACADDR: u32 = 829;
/// PostgreSQL type OID for `macaddr8`.
const PG_OID_MACADDR8: u32 = 774;
/// PostgreSQL type OID for `bytea`.
const PG_OID_BYTEA: u32 = 17;
/// PostgreSQL type OID for `uuid`.
const PG_OID_UUID: u32 = 2950;
/// PostgreSQL type OID for `json`.
const PG_OID_JSON: u32 = 114;
/// PostgreSQL type OID for `jsonb`.
const PG_OID_JSONB: u32 = 3802;
/// PostgreSQL type OID for `xml`.
const PG_OID_XML: u32 = 142;
/// PostgreSQL type OID for `tsvector`.
const PG_OID_TSVECTOR: u32 = 3614;
/// PostgreSQL type OID for `tsquery`.
const PG_OID_TSQUERY: u32 = 3615;
/// PostgreSQL type OID for `date`.
const PG_OID_DATE: u32 = 1082;
/// PostgreSQL type OID for `time`.
const PG_OID_TIME: u32 = 1083;
/// PostgreSQL type OID for `timestamp`.
const PG_OID_TIMESTAMP: u32 = 1114;
/// PostgreSQL type OID for `timetz`.
const PG_OID_TIMETZ: u32 = 1266;
/// PostgreSQL type OID for `timestamptz`.
const PG_OID_TIMESTAMPTZ: u32 = 1184;
/// PostgreSQL type OID for `bool[]`.
const PG_OID_BOOL_ARRAY: u32 = 1000;
/// PostgreSQL type OID for `int2[]`.
const PG_OID_INT2_ARRAY: u32 = 1005;
/// PostgreSQL type OID for `int4[]`.
const PG_OID_INT4_ARRAY: u32 = 1007;
/// PostgreSQL type OID for `int8[]`.
const PG_OID_INT8_ARRAY: u32 = 1016;
/// PostgreSQL type OID for `float4[]`.
const PG_OID_FLOAT4_ARRAY: u32 = 1021;
/// PostgreSQL type OID for `float8[]`.
const PG_OID_FLOAT8_ARRAY: u32 = 1022;
/// PostgreSQL type OID for `numeric[]`.
const PG_OID_NUMERIC_ARRAY: u32 = 1231;
/// PostgreSQL type OID for `money[]`.
const PG_OID_MONEY_ARRAY: u32 = 791;
/// PostgreSQL type OID for `oid[]`.
const PG_OID_OID_ARRAY: u32 = 1028;
/// PostgreSQL type OID for `regclass[]`.
const PG_OID_REGCLASS_ARRAY: u32 = 2210;
/// PostgreSQL type OID for `regtype[]`.
const PG_OID_REGTYPE_ARRAY: u32 = 2211;
/// PostgreSQL type OID for `pg_lsn[]`.
const PG_OID_PG_LSN_ARRAY: u32 = 3221;
/// PostgreSQL type OID for `text[]`.
const PG_OID_TEXT_ARRAY: u32 = 1009;
/// PostgreSQL type OID for `bpchar[]`.
const PG_OID_BPCHAR_ARRAY: u32 = 1014;
/// PostgreSQL type OID for `bit[]`.
const PG_OID_BIT_ARRAY: u32 = 1561;
/// PostgreSQL type OID for `varbit[]`.
const PG_OID_VARBIT_ARRAY: u32 = 1563;
/// PostgreSQL type OID for `cidr[]`.
const PG_OID_CIDR_ARRAY: u32 = 651;
/// PostgreSQL type OID for `inet[]`.
const PG_OID_INET_ARRAY: u32 = 1041;
/// PostgreSQL type OID for `macaddr[]`.
const PG_OID_MACADDR_ARRAY: u32 = 1040;
/// PostgreSQL type OID for `macaddr8[]`.
const PG_OID_MACADDR8_ARRAY: u32 = 775;
/// PostgreSQL type OID for `bytea[]`.
const PG_OID_BYTEA_ARRAY: u32 = 1001;
/// PostgreSQL type OID for `uuid[]`.
const PG_OID_UUID_ARRAY: u32 = 2951;
/// PostgreSQL type OID for `json[]`.
const PG_OID_JSON_ARRAY: u32 = 199;
/// PostgreSQL type OID for `jsonb[]`.
const PG_OID_JSONB_ARRAY: u32 = 3807;
/// PostgreSQL type OID for `xml[]`.
const PG_OID_XML_ARRAY: u32 = 143;
/// PostgreSQL type OID for `tsvector[]`.
const PG_OID_TSVECTOR_ARRAY: u32 = 3643;
/// PostgreSQL type OID for `tsquery[]`.
const PG_OID_TSQUERY_ARRAY: u32 = 3645;
/// PostgreSQL type OID for `date[]`.
const PG_OID_DATE_ARRAY: u32 = 1182;
/// PostgreSQL type OID for `time[]`.
const PG_OID_TIME_ARRAY: u32 = 1183;
/// PostgreSQL type OID for `timestamp[]`.
const PG_OID_TIMESTAMP_ARRAY: u32 = 1115;
/// PostgreSQL type OID for `timetz[]`.
const PG_OID_TIMETZ_ARRAY: u32 = 1270;
/// PostgreSQL type OID for `timestamptz[]`.
const PG_OID_TIMESTAMPTZ_ARRAY: u32 = 1185;
/// PostgreSQL format code 0 = text.
const FORMAT_TEXT: i16 = 0;

/// Text-format display settings owned by one query execution.
#[derive(Clone, Debug, Default)]
pub(crate) struct TextEncodingOptions {
    timezone: Option<String>,
    lc_monetary: Option<String>,
    datestyle: DateStyleOptions,
}

impl TextEncodingOptions {
    /// Build text display settings from session GUC values.
    #[must_use]
    pub(crate) fn from_session_settings(settings: &HashMap<String, String>) -> Self {
        let timezone = settings
            .get("timezone")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        let datestyle = settings
            .get("datestyle")
            .map(String::as_str)
            .map(DateStyleOptions::parse)
            .unwrap_or_default();
        let lc_monetary = settings
            .get("lc_monetary")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        Self {
            timezone,
            lc_monetary,
            datestyle,
        }
    }

    pub(crate) fn format_date(&self, days: i32) -> String {
        self.datestyle.format_date(days)
    }

    pub(crate) fn format_timestamp(&self, micros: i64) -> String {
        self.datestyle.format_timestamp(micros)
    }

    pub(crate) fn format_timestamptz(&self, micros: i64) -> String {
        if self.datestyle.style == DateStyle::Iso {
            return self
                .timezone
                .as_deref()
                .and_then(|timezone| format_timestamptz_micros_in_timezone(micros, timezone))
                .unwrap_or_else(|| format_timestamptz_micros_utc(micros));
        }
        let timezone = self.timezone.as_deref().unwrap_or("UTC");
        let Some(display) = timestamptz_display_in_timezone(micros, timezone)
            .or_else(|| timestamptz_display_in_timezone(micros, "UTC"))
        else {
            return format_timestamptz_micros_utc(micros);
        };
        let zone = display
            .zone_name
            .unwrap_or_else(|| format_timezone_offset_seconds(display.offset_seconds));
        format!(
            "{} {}",
            self.datestyle.format_timestamp(display.local_micros),
            zone
        )
    }

    pub(crate) fn format_money(&self, cents: i64) -> String {
        self.lc_monetary.as_deref().map_or_else(
            || format_money_text(cents),
            |locale| format_money_text_with_locale(cents, locale),
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DateStyle {
    Iso,
    Sql,
    Postgres,
    German,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DateOrder {
    Mdy,
    Dmy,
    Ymd,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DateStyleOptions {
    style: DateStyle,
    order: DateOrder,
}

impl Default for DateStyleOptions {
    fn default() -> Self {
        Self {
            style: DateStyle::Iso,
            order: DateOrder::Mdy,
        }
    }
}

impl DateStyleOptions {
    fn parse(value: &str) -> Self {
        let mut options = Self::default();
        for part in value
            .split(|ch: char| ch == ',' || ch.is_ascii_whitespace())
            .filter(|part| !part.is_empty())
        {
            match part.to_ascii_lowercase().as_str() {
                "iso" => options.style = DateStyle::Iso,
                "sql" => options.style = DateStyle::Sql,
                "postgres" => options.style = DateStyle::Postgres,
                "german" => options.style = DateStyle::German,
                "mdy" => options.order = DateOrder::Mdy,
                "dmy" => options.order = DateOrder::Dmy,
                "ymd" => options.order = DateOrder::Ymd,
                _ => {}
            }
        }
        if options.style == DateStyle::German {
            options.order = DateOrder::Dmy;
        }
        options
    }

    fn format_date(&self, days: i32) -> String {
        let Some((year, month, day)) = date_parts_from_days(days) else {
            return format_date_days(days);
        };
        self.format_date_parts(year, month, day)
    }

    fn format_timestamp(&self, micros: i64) -> String {
        let Some((year, month, day, time_micros)) = timestamp_parts_from_micros(micros) else {
            return format_timestamp_micros(micros);
        };
        let time = format_time_micros(time_micros);
        match self.style {
            DateStyle::Iso | DateStyle::Sql | DateStyle::German => {
                format!("{} {}", self.format_date_parts(year, month, day), time)
            }
            DateStyle::Postgres => {
                let weekday = weekday_abbrev(year, month, day);
                let month_name = month_abbrev(month);
                if self.order == DateOrder::Dmy {
                    format!("{weekday} {day:02} {month_name} {time} {year:04}")
                } else {
                    format!("{weekday} {month_name} {day:02} {time} {year:04}")
                }
            }
        }
    }

    fn format_date_parts(&self, year: i32, month: u32, day: u32) -> String {
        match self.style {
            DateStyle::Iso => format!("{year:04}-{month:02}-{day:02}"),
            DateStyle::German => format!("{day:02}.{month:02}.{year:04}"),
            DateStyle::Sql => {
                if self.order == DateOrder::Dmy {
                    format!("{day:02}/{month:02}/{year:04}")
                } else {
                    format!("{month:02}/{day:02}/{year:04}")
                }
            }
            DateStyle::Postgres => {
                if self.order == DateOrder::Dmy {
                    format!("{day:02}-{month:02}-{year:04}")
                } else {
                    format!("{month:02}-{day:02}-{year:04}")
                }
            }
        }
    }
}

fn weekday_abbrev(year: i32, month: u32, day: u32) -> &'static str {
    let Some(date) = NaiveDate::from_ymd_opt(year, month, day) else {
        return "";
    };
    match date.weekday() {
        chrono::Weekday::Mon => "Mon",
        chrono::Weekday::Tue => "Tue",
        chrono::Weekday::Wed => "Wed",
        chrono::Weekday::Thu => "Thu",
        chrono::Weekday::Fri => "Fri",
        chrono::Weekday::Sat => "Sat",
        chrono::Weekday::Sun => "Sun",
    }
}

const fn month_abbrev(month: u32) -> &'static str {
    match month {
        1 => "Jan",
        2 => "Feb",
        3 => "Mar",
        4 => "Apr",
        5 => "May",
        6 => "Jun",
        7 => "Jul",
        8 => "Aug",
        9 => "Sep",
        10 => "Oct",
        11 => "Nov",
        12 => "Dec",
        _ => "",
    }
}

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
    /// Optional live streaming handle for a large top-level Simple-Query
    /// SELECT whose body exceeded `STREAM_WINDOW_HIGH_WATER_BYTES`. When
    /// `Some(_)`, the session ships `streamed_body` (window 0) first, then
    /// drives the handle to drain the remaining windows to the socket with
    /// backpressure. Mutually exclusive with the other body fields by
    /// construction (only the top-level SELECT arm ever sets it). Boxed to
    /// keep `SelectResult` small — the handle owns the root operator and an
    /// optional `Transaction`.
    pub streaming: Option<Box<StreamingSelect>>,
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
        streaming: None,
        rows: 0,
    }
}

fn row_count_overflow(context: &str) -> ServerError {
    ServerError::Execute(ultrasql_executor::ExecError::NumericFieldOverflow(format!(
        "{context} row count overflow"
    )))
}

fn checked_row_count_delta(row_count: usize, context: &str) -> Result<u64, ServerError> {
    u64::try_from(row_count).map_err(|_| row_count_overflow(context))
}

fn checked_add_rows(rows: &mut u64, row_count: usize, context: &str) -> Result<(), ServerError> {
    let delta = checked_row_count_delta(row_count, context)?;
    *rows = rows
        .checked_add(delta)
        .ok_or_else(|| row_count_overflow(context))?;
    Ok(())
}

fn checked_add_affected(affected: &mut i64, delta: i64) -> Result<(), ServerError> {
    if delta < 0 {
        return Err(row_count_overflow("DML affected"));
    }
    *affected = affected
        .checked_add(delta)
        .ok_or_else(|| row_count_overflow("DML affected"))?;
    Ok(())
}

/// Drive a `ModifyTable` operator to completion and emit the
/// Row-count `CommandComplete` tag.
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
                checked_add_affected(&mut affected, data[0])?;
            }
        }
    }
    let rows = u64::try_from(affected).map_err(|_| row_count_overflow("DML affected"))?;
    let tag = modify_command_tag(command, rows);
    Ok(SelectResult {
        messages: vec![BackendMessage::CommandComplete { tag }],
        streamed_body: None,
        shared_streamed_body: None,
        streaming: None,
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
    run_modify_returning_with_options(op, command, &TextEncodingOptions::default())
}

/// Session-aware variant of [`run_modify_returning`].
pub(crate) fn run_modify_returning_with_options(
    op: &mut dyn Operator,
    command: &str,
    options: &TextEncodingOptions,
) -> Result<SelectResult, ServerError> {
    let mut result = run_select_with_options(op, options)?;
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
    run_select_with_options(op, &TextEncodingOptions::default())
}

/// Session-aware variant of [`run_select`].
pub(crate) fn run_select_with_options(
    op: &mut dyn Operator,
    options: &TextEncodingOptions,
) -> Result<SelectResult, ServerError> {
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
                columns.push(checked_encode_text_value_typed_with_options(
                    col,
                    row,
                    &schema.field_at(idx).data_type,
                    options,
                )?);
            }
            messages.push(BackendMessage::DataRow { columns });
        }
        checked_add_rows(&mut rows, row_count, "SELECT")?;
    }

    messages.push(BackendMessage::CommandComplete {
        tag: format!("SELECT {rows}"),
    });
    Ok(SelectResult {
        messages,
        streamed_body: None,
        shared_streamed_body: None,
        streaming: None,
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
    stream_select_with_options(op, sink, &TextEncodingOptions::default())
}

/// Session-aware variant of [`stream_select`].
pub(crate) fn stream_select_with_options(
    op: &mut dyn Operator,
    sink: &mut BytesMut,
    options: &TextEncodingOptions,
) -> Result<u64, ServerError> {
    let schema = op.schema().clone();
    let row_desc = build_row_description(&schema);
    encode_backend(&row_desc, sink);

    let mut rows: u64 = 0;
    loop {
        let Some(batch) = op.next_batch()? else { break };
        encode_batch_into(sink, &batch, &schema, options, &mut rows)?;
    }

    let tag = format!("SELECT {rows}");
    encode_backend(&BackendMessage::CommandComplete { tag }, sink);
    Ok(rows)
}

/// Encode a single output [`Batch`] into `out` as a run of `DataRow`
/// wire frames and fold its row count into `rows`.
///
/// Lifted verbatim from the per-batch body of
/// [`stream_select_with_options`] so the whole-buffer SELECT path and
/// the windowed streaming path (`encode_window`) share one encoder —
/// the bytes are therefore identical regardless of how the caller
/// chunks batches into windows. The fast-path bulk writers
/// (`write_int32_pair_data_rows`, `write_int32_int64_pair_data_rows`)
/// and the general per-row path emit whole frames only, so a caller
/// may flush after any batch and always lands on a frame boundary.
pub(crate) fn encode_batch_into(
    out: &mut BytesMut,
    batch: &ultrasql_vec::Batch,
    schema: &Schema,
    options: &TextEncodingOptions,
    rows: &mut u64,
) -> Result<(), ServerError> {
    let row_count = batch.rows();
    let columns = batch.columns();
    // Fast path: when the batch is exactly two non-nullable
    // `Int32` columns (the `select_scan_10k` shape), use the
    // specialised bulk writer that pre-reserves the buffer once
    // and emits every row without per-cell enum dispatch.
    if schema_is_int32_pair(schema)
        && let [Column::Int32(a), Column::Int32(b)] = columns
        && a.nulls().is_none()
        && b.nulls().is_none()
    {
        write_int32_pair_data_rows(out, a.data(), b.data());
        checked_add_rows(rows, row_count, "SELECT")?;
        return Ok(());
    }
    // Fast path: `(Int32, Int64)` is the
    // `WindowAgg::try_columnar_row_number` output shape used by
    // `SELECT id, row_number() OVER (ORDER BY x) FROM t`. The
    // writer accepts optional validity bitmaps so it stays
    // correct when either side carries NULLs.
    if schema_is_int32_int64_pair(schema)
        && let [Column::Int32(a), Column::Int64(b)] = columns
    {
        write_int32_int64_pair_data_rows(out, a.data(), a.nulls(), b.data(), b.nulls());
        checked_add_rows(rows, row_count, "SELECT")?;
        return Ok(());
    }
    for row in 0..row_count {
        write_data_row_typed_with_options(out, columns, schema, row, options)?;
    }
    checked_add_rows(rows, row_count, "SELECT")?;
    Ok(())
}

/// High-water mark (in bytes) for one streamed SELECT window.
///
/// When a top-level Simple-Query SELECT body grows past this many bytes
/// before the operator reaches EOF, the result is shipped to the client
/// in bounded windows (flush, clear, refill) instead of being buffered
/// whole. Set to 256 KiB so the `select_scan_10k` bench body (~250 KiB)
/// stays a single window (one `write_all`, no behaviour change there)
/// while a 100 MB scan ships in ~400 windows rather than one 100 MB
/// allocation. A window may overshoot by at most one batch (the loop
/// stops *after* the batch that crosses the mark), so peak wire-buffer
/// memory is bounded by `STREAM_WINDOW_HIGH_WATER_BYTES + one batch`,
/// independent of result cardinality.
pub(crate) const STREAM_WINDOW_HIGH_WATER_BYTES: usize = 256 * 1024;

/// A lazily-driven SELECT result: the still-live root operator plus the
/// running state needed to encode the remainder of the wire body one
/// window at a time.
///
/// Produced only by the top-level Simple-Query SELECT arm when the body
/// crosses `STREAM_WINDOW_HIGH_WATER_BYTES` before EOF (see
/// `run_plan_in_txn`). The async dispatcher
/// (`Session::send_query_result_with_ready`) drives `encode_window` in
/// a loop, flushing each window to the socket with backpressure. The
/// `RowDescription` and the rows that filled window 0 are *not* held
/// here — they were already encoded into the session buffer when the
/// handle was built and ship first; this handle resumes pulling from the
/// operator where window 0 left off.
pub struct StreamingSelect {
    /// Live root operator, drained one window's worth of batches per
    /// [`encode_window`] call. Owns its inputs by `Arc`/value, so it is
    /// `'static` and moves cleanly into the async frame.
    op: Box<dyn Operator>,
    /// Cached output schema (the operator's schema never changes).
    schema: Schema,
    /// Text-format display settings; identical to the buffered path so
    /// the encoded bytes match byte-for-byte.
    options: TextEncodingOptions,
    /// Running row count. Seeded with the rows encoded into window 0 and
    /// folded forward by every subsequent batch via `checked_add_rows`,
    /// so it equals the `CommandComplete` tag count at EOF and
    /// `SelectResult.rows` after the drain.
    rows: u64,
    /// `Some(txn)` when the drive loop must commit this autocommit
    /// transaction after a successful drain (cursor semantics: the rows
    /// are read under the snapshot, the commit finalises once the
    /// operator is exhausted). `None` when the statement runs inside an
    /// explicit transaction block whose handle stays in the session's
    /// `TxnState` and is committed later by COMMIT.
    commit_txn: Option<Transaction>,
}

impl std::fmt::Debug for StreamingSelect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingSelect")
            .field("schema", &self.schema)
            .field("options", &self.options)
            .field("rows", &self.rows)
            .field("commit_txn", &self.commit_txn)
            .finish_non_exhaustive()
    }
}

impl StreamingSelect {
    /// Running row count produced so far (== final `CommandComplete`
    /// count once the drain completes). Used by the streaming harness to
    /// assert the counter agrees with the emitted tag.
    #[cfg(test)]
    pub(crate) fn rows(&self) -> u64 {
        self.rows
    }

    /// Take the autocommit transaction that must be committed after a
    /// successful drain, leaving `None` so it is committed at most once.
    pub(crate) fn take_commit_txn(&mut self) -> Option<Transaction> {
        self.commit_txn.take()
    }

    /// Test-only constructor: wrap an operator in a streaming handle with
    /// no autocommit transaction, so the byte-identity / bounded-memory /
    /// mid-stream-error harness can drive [`encode_window`] directly.
    #[cfg(test)]
    pub(crate) fn for_test(op: Box<dyn Operator>, options: TextEncodingOptions) -> Self {
        let schema = op.schema().clone();
        Self {
            op,
            schema,
            options,
            rows: 0,
            commit_txn: None,
        }
    }

    /// Test-only accessor for the cached schema (used to emit the
    /// reference `RowDescription` in the harness).
    #[cfg(test)]
    pub(crate) fn schema(&self) -> &Schema {
        &self.schema
    }
}

/// Encode the next window of a streaming SELECT body into `out`.
///
/// `out` must be cleared by the caller before each call. The function
/// pulls operator batches and appends whole `DataRow` frames via the
/// shared [`encode_batch_into`] (byte-identical to the buffered path)
/// until either:
///
/// - the running body reaches `high_water` — returns `Ok(true)` (window
///   full at a whole-frame boundary; call again for the next window), or
/// - the operator reaches EOF — writes the trailing `CommandComplete`
///   with the accumulated row count into `out` and returns `Ok(false)`
///   (final window; the body is complete).
///
/// The window check is evaluated only *between* batches, so a window
/// never ends mid-row. The concatenation of all windows is byte-for-byte
/// equal to the single body [`stream_select_with_options`] would have
/// produced (minus the `RowDescription`, which window 0 already shipped).
///
/// On operator error the partial `out` is left untouched and the error
/// is propagated; the caller decides between the pre-flush (reuse
/// today's error path) and post-flush (inline `ErrorResponse`) handling.
pub(crate) fn encode_window(
    s: &mut StreamingSelect,
    out: &mut BytesMut,
    high_water: usize,
) -> Result<bool, ServerError> {
    while out.len() < high_water {
        let Some(batch) = s.op.next_batch()? else {
            let tag = format!("SELECT {}", s.rows);
            encode_backend(&BackendMessage::CommandComplete { tag }, out);
            return Ok(false);
        };
        encode_batch_into(out, &batch, &s.schema, &s.options, &mut s.rows)?;
    }
    Ok(true)
}

/// Begin a streaming SELECT: encode the `RowDescription` and as many
/// `DataRow` frames as fit in window 0 into `sink`, then decide whether
/// the result is small (buffered) or large (streamed).
///
/// Mirrors [`run_select_streamed_with_options`] for the small case so a
/// SELECT whose body fits in one window is byte- and syscall-identical
/// to today (single `streamed_body`, no streaming handle). When window 0
/// fills before EOF, returns the still-live operator wrapped in a
/// [`StreamingSelect`] alongside the already-encoded window-0 body.
///
/// `commit_txn` is threaded into the returned handle unchanged so the
/// caller's transaction is committed after the drain (autocommit) or
/// kept open (explicit block); it is consumed only on the streaming
/// branch — the buffered branch leaves the caller to finalise the txn as
/// it does today.
pub(crate) fn begin_streaming_select(
    op: Box<dyn Operator>,
    sink: &mut BytesMut,
    options: &TextEncodingOptions,
    commit_txn: Option<Transaction>,
) -> Result<StreamingSelectStart, ServerError> {
    let schema = op.schema().clone();
    let mut handle = StreamingSelect {
        op,
        schema,
        options: options.clone(),
        rows: 0,
        commit_txn,
    };

    let initial_cap = streamed_initial_cap(handle.op.estimated_row_count(), handle.schema.len());
    let mut body = std::mem::take(sink);
    prepare_stream_sink(&mut body, initial_cap);

    let row_desc = build_row_description(&handle.schema);
    encode_backend(&row_desc, &mut body);

    match encode_window(&mut handle, &mut body, STREAM_WINDOW_HIGH_WATER_BYTES) {
        Ok(false) => {
            // EOF inside window 0: small result. The body already holds
            // `RowDescription` + every `DataRow` + `CommandComplete`,
            // exactly like the buffered path; return it as `streamed_body`.
            let rows = handle.rows;
            Ok(StreamingSelectStart::Buffered(SelectResult {
                messages: Vec::new(),
                streamed_body: Some(body),
                shared_streamed_body: None,
                streaming: None,
                rows,
            }))
        }
        Ok(true) => {
            // Window 0 full, more rows remain: stream the rest. Window 0
            // bytes ride along in `streamed_body` so the dispatcher ships
            // them before pulling the next window.
            let rows = handle.rows;
            Ok(StreamingSelectStart::Streaming {
                window0: body,
                handle: Box::new(handle),
                rows,
            })
        }
        Err(e) => {
            // No window has been flushed yet; reuse today's error path.
            // Park the buffer back so the session keeps its allocation.
            body.clear();
            *sink = body;
            Err(e)
        }
    }
}

/// Outcome of [`begin_streaming_select`]: either a fully-buffered small
/// result (identical to the non-streaming path) or a streaming handle
/// plus the already-encoded window-0 bytes.
pub(crate) enum StreamingSelectStart {
    /// Body fit in window 0; ship it as today's buffered `streamed_body`.
    Buffered(SelectResult),
    /// Body exceeded window 0; stream the remainder from `handle` after
    /// shipping `window0`.
    Streaming {
        window0: BytesMut,
        handle: Box<StreamingSelect>,
        rows: u64,
    },
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
/// wire bytes. Alternative for [`run_select`] at the SELECT
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
    run_select_streamed_with_options(op, sink, &TextEncodingOptions::default())
}

/// Up-front buffer reservation (in bytes) for a streamed SELECT body.
///
/// When the operator advertises its row count (column-cache replay,
/// materialised CTE, `LIMIT n`) we size the buffer to the estimated wire-byte
/// budget so the encode loop skips repeated `BytesMut::reserve` reallocations.
/// The width estimate assumes each column expands to a text-format datum of
/// ~8 bytes plus a 4-byte length prefix — generous enough for typical int /
/// small-string scans without wasting more than one growth cycle on wider
/// relations. Wire layout per row: 1B tag + 4B length + 2B ncols + per column
/// (4B length + ascii text); the bench's `(id INT, val INT)` lands at ~25 B/row
/// and this bound puts us just over that. Without a hint we fall back to a
/// 32 KiB start so small queries stay on one allocation.
///
/// The reservation is **capped** at [`MAX_INITIAL_CAP_BYTES`]:
/// `estimated_row_count` is only an estimate (and on some paths is
/// influenceable by a crafted plan/`LIMIT`), so an inflated count must never
/// pre-allocate gigabytes before the first row is encoded — that is a
/// server-side OOM/DoS vector. The buffer still grows (amortized) past the cap
/// for genuinely large results; this only bounds the speculative pre-touch.
/// The full mid-result streaming fix that bounds *peak* memory is tracked
/// separately.
fn streamed_initial_cap(estimated_rows: Option<usize>, cols: usize) -> usize {
    const PER_ROW_OVERHEAD_BYTES: usize = 7; // tag + length + ncols
    const PER_CELL_BYTES_ESTIMATE: usize = 12; // 4B length + ~8B text
    const ROWDESC_AND_TAG_BYTES: usize = 256;
    match estimated_rows {
        Some(rows) => {
            let cols = cols.max(1);
            let per_row =
                PER_ROW_OVERHEAD_BYTES.saturating_add(cols.saturating_mul(PER_CELL_BYTES_ESTIMATE));
            rows.saturating_mul(per_row)
                .saturating_add(ROWDESC_AND_TAG_BYTES)
        }
        None => 32 * 1024,
    }
    .min(MAX_INITIAL_CAP_BYTES)
}

/// Hard ceiling on the speculative up-front SELECT-body reservation. See
/// [`streamed_initial_cap`] for why an estimate must never drive an unbounded
/// allocation.
const MAX_INITIAL_CAP_BYTES: usize = 16 * 1024 * 1024; // 16 MiB

/// Session-aware variant of [`run_select_streamed`].
pub(crate) fn run_select_streamed_with_options(
    op: &mut dyn Operator,
    sink: &mut BytesMut,
    options: &TextEncodingOptions,
) -> Result<SelectResult, ServerError> {
    let initial_cap = streamed_initial_cap(op.estimated_row_count(), op.schema().len());
    let mut body = std::mem::take(sink);
    prepare_stream_sink(&mut body, initial_cap);
    match stream_select_with_options(op, &mut body, options) {
        Ok(rows) => Ok(SelectResult {
            messages: Vec::new(),
            streamed_body: Some(body),
            shared_streamed_body: None,
            streaming: None,
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
        streaming: None,
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
        streaming: None,
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
        streaming: None,
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
        Column::DictionaryUtf8(c) => c.try_decode_at(row).map(|value| value.as_bytes().to_vec()),
    }
}

/// Encode a physical column cell using the logical schema type.
///
/// Batch columns use compact physical layouts: `DATE` shares `Int32`,
/// `DECIMAL` shares `Int64`. Wire text must expose SQL values, not storage
/// integers.
#[cfg(test)]
pub(crate) fn encode_text_value_typed(
    col: &Column,
    row: usize,
    logical_type: &DataType,
) -> Option<Vec<u8>> {
    encode_text_value_typed_with_options(col, row, logical_type, &TextEncodingOptions::default())
}

/// Session-aware variant of [`encode_text_value_typed`].
pub(crate) fn encode_text_value_typed_with_options(
    col: &Column,
    row: usize,
    logical_type: &DataType,
    options: &TextEncodingOptions,
) -> Option<Vec<u8>> {
    if let Some(nulls) = column_nulls(col) {
        if !nulls.get(row) {
            return None;
        }
    }
    match (logical_type, col) {
        (DataType::Date, Column::Int32(c)) => Some(options.format_date(c.data()[row]).into()),
        (DataType::Decimal { scale, .. }, Column::Int64(c)) => Some(
            Value::Decimal {
                value: c.data()[row],
                scale: scale.unwrap_or(0),
            }
            .to_string()
            .into(),
        ),
        (DataType::Money, Column::Int64(c)) => Some(options.format_money(c.data()[row]).into()),
        (DataType::Oid | DataType::RegClass | DataType::RegType, Column::Int64(c)) => {
            u32::try_from(c.data()[row])
                .ok()
                .map(|raw| raw.to_string().into_bytes())
        }
        (DataType::Time, Column::Int64(c)) => Some(format_time_micros(c.data()[row]).into_bytes()),
        (DataType::Timestamp, Column::Int64(c)) => {
            Some(options.format_timestamp(c.data()[row]).into_bytes())
        }
        (DataType::TimestampTz, Column::Int64(c)) => {
            Some(options.format_timestamptz(c.data()[row]).into_bytes())
        }
        (DataType::TimeTz, Column::Int64(c)) => unpack_timetz(c.data()[row])
            .map(|(micros, offset_seconds)| format_timetz(micros, offset_seconds).into_bytes()),
        (ty, Column::Utf8(_) | Column::DictionaryUtf8(_)) if ty.is_vector_family() => col
            .text_value(row)
            .map(|text| encode_vector_family_text_value(text, ty)),
        _ => encode_text_value(col, row),
    }
}

fn checked_encode_text_value_typed_with_options(
    col: &Column,
    row: usize,
    logical_type: &DataType,
    options: &TextEncodingOptions,
) -> Result<Option<Vec<u8>>, ServerError> {
    let encoded = encode_text_value_typed_with_options(col, row, logical_type, options);
    if encoded.is_none() && !column_row_is_null(col, row) {
        return Err(ServerError::Execute(
            ultrasql_executor::ExecError::Internal("result text cell encoding failed"),
        ));
    }
    Ok(encoded)
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

fn column_row_is_null(col: &Column, row: usize) -> bool {
    column_nulls(col).is_some_and(|nulls| !nulls.get(row))
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
fn pg_type_oid(ty: &DataType) -> u32 {
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

/// Map a [`DataType`] to the wire-protocol `type_size` field. Negative
/// values denote a variable-length type.
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
        DataType::Uuid => 16,
        _ => -1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use ultrasql_core::{Field, Schema, parse_timestamptz_text};
    use ultrasql_executor::MemTableScan;
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn, StringColumn};
    use ultrasql_vec::dict::DictionaryColumn;

    #[test]
    fn streamed_initial_cap_bounds_inflated_row_estimates() {
        // No row hint: small fixed start so tiny queries stay on one alloc.
        assert_eq!(streamed_initial_cap(None, 4), 32 * 1024);

        // Modest estimate: sized to the estimate, comfortably under the cap.
        let modest = streamed_initial_cap(Some(100), 2);
        assert!(modest < MAX_INITIAL_CAP_BYTES);
        assert!(modest >= 100 * 7, "at least the per-row overhead budget");

        // Inflated / adversarial estimate must NOT pre-allocate gigabytes:
        // it is clamped to the hard ceiling regardless of count or width.
        assert_eq!(
            streamed_initial_cap(Some(usize::MAX), 64),
            MAX_INITIAL_CAP_BYTES
        );
        assert_eq!(
            streamed_initial_cap(Some(1_000_000_000), 16),
            MAX_INITIAL_CAP_BYTES
        );

        // Zero columns is treated as one: no overflow/panic, still bounded.
        assert_eq!(
            streamed_initial_cap(Some(usize::MAX), 0),
            MAX_INITIAL_CAP_BYTES
        );
    }

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
        assert_eq!(pg_type_oid(&DataType::Money), 790);
        assert_eq!(pg_type_oid(&DataType::Json), 114);
        assert_eq!(pg_type_oid(&DataType::Jsonb), 3802);
        assert_eq!(pg_type_oid(&DataType::Xml), 142);
        assert_eq!(pg_type_oid(&DataType::Oid), 26);
        assert_eq!(pg_type_oid(&DataType::RegClass), 2205);
        assert_eq!(pg_type_oid(&DataType::RegType), 2206);
        assert_eq!(pg_type_oid(&DataType::PgLsn), 3220);
        assert_eq!(
            pg_type_oid(&DataType::Array(Box::new(DataType::Array(Box::new(
                DataType::Int32
            ))))),
            1007
        );
        assert_eq!(
            pg_type_oid(&DataType::Decimal {
                precision: Some(12),
                scale: Some(3)
            }),
            1700
        );
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
    fn run_select_encodes_logical_money_text_and_oid() {
        let schema = Schema::new([Field::required("amount", DataType::Money)]).unwrap();
        let batch = Batch::new([Column::Int64(NumericColumn::from_data(vec![123_456]))]).unwrap();
        let mut scan = MemTableScan::new(schema, vec![batch]);
        let result = run_select(&mut scan).expect("ok");

        match &result.messages[0] {
            BackendMessage::RowDescription { fields } => {
                assert_eq!(fields[0].type_oid, 790);
                assert_eq!(fields[0].type_size, 8);
            }
            other => panic!("expected RowDescription, got {other:?}"),
        }
        match &result.messages[1] {
            BackendMessage::DataRow { columns } => {
                assert_eq!(columns[0], Some(b"$1,234.56".to_vec()));
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

    #[test]
    fn run_select_with_options_applies_session_timezone() {
        let schema = Schema::new([Field::required("observed_at", DataType::TimestampTz)]).unwrap();
        let micros = parse_timestamptz_text("2000-07-01 00:00:00+00").unwrap();
        let batch = Batch::new([Column::Int64(NumericColumn::from_data(vec![micros]))]).unwrap();
        let mut scan = MemTableScan::new(schema, vec![batch]);
        let mut settings = HashMap::new();
        settings.insert("timezone".to_owned(), "America/New_York".to_owned());
        let options = TextEncodingOptions::from_session_settings(&settings);
        let result = run_select_with_options(&mut scan, &options).expect("ok");

        match &result.messages[1] {
            BackendMessage::DataRow { columns } => {
                assert_eq!(columns[0], Some(b"2000-06-30 20:00:00-04".to_vec()));
            }
            other => panic!("expected DataRow, got {other:?}"),
        }
    }

    #[test]
    fn dml_and_streaming_helpers_emit_postgres_command_shapes() {
        let ddl = run_ddl_command("CREATE TABLE");
        assert_eq!(ddl.rows, 0);
        assert!(matches!(
            ddl.messages.last(),
            Some(BackendMessage::CommandComplete { tag }) if tag == "CREATE TABLE"
        ));

        let affected_schema = Schema::new([Field::required("affected", DataType::Int64)]).unwrap();
        let affected_batches = vec![
            Batch::new([Column::Int64(NumericColumn::from_data(vec![2]))]).unwrap(),
            Batch::new([Column::Int64(NumericColumn::from_data(vec![3]))]).unwrap(),
        ];
        let mut modify_scan = MemTableScan::new(affected_schema, affected_batches);
        let modify = run_modify_command(&mut modify_scan, "update").expect("modify");
        assert_eq!(modify.rows, 5);
        assert!(matches!(
            modify.messages.last(),
            Some(BackendMessage::CommandComplete { tag }) if tag == "UPDATE 5"
        ));
        let overflow_schema = Schema::new([Field::required("affected", DataType::Int64)]).unwrap();
        let overflow_batches = vec![
            Batch::new([Column::Int64(NumericColumn::from_data(vec![i64::MAX]))]).unwrap(),
            Batch::new([Column::Int64(NumericColumn::from_data(vec![1]))]).unwrap(),
        ];
        let mut overflow_scan = MemTableScan::new(overflow_schema, overflow_batches);
        let err = run_modify_command(&mut overflow_scan, "update")
            .expect_err("DML command tag row count overflow must not clamp");
        assert_eq!(err.sqlstate(), "22003");

        let returning_schema = Schema::new([Field::required("id", DataType::Int32)]).unwrap();
        let returning_batch =
            Batch::new([Column::Int32(NumericColumn::from_data(vec![1, 2]))]).unwrap();
        let mut returning_scan = MemTableScan::new(returning_schema, vec![returning_batch]);
        let returning = run_modify_returning(&mut returning_scan, "insert").expect("returning");
        assert_eq!(returning.rows, 2);
        assert!(matches!(
            returning.messages.last(),
            Some(BackendMessage::CommandComplete { tag }) if tag == "INSERT 0 2"
        ));
    }

    #[test]
    fn streamed_select_helpers_reuse_buffers_and_shared_bodies() {
        let schema = Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("value", DataType::Int32),
        ])
        .unwrap();
        let batch = Batch::new([
            Column::Int32(NumericColumn::from_data(vec![1, 2])),
            Column::Int32(NumericColumn::from_data(vec![10, 20])),
        ])
        .unwrap();
        let mut scan = MemTableScan::new(schema.clone(), vec![batch]);
        let mut sink = BytesMut::with_capacity(1);
        let streamed = run_select_streamed(&mut scan, &mut sink).expect("streamed");
        assert_eq!(streamed.rows, 2);
        assert!(streamed.messages.is_empty());
        let body = streamed.streamed_body.expect("body");
        assert!(body.windows(b"SELECT 2".len()).any(|w| w == b"SELECT 2"));

        let mut cached_sink = BytesMut::new();
        let cached =
            run_cached_int32_pair_select_streamed(&schema, &[1, 2], &[10, 20], &mut cached_sink);
        assert_eq!(cached.rows, 2);
        let cached_body = cached.streamed_body.as_ref().expect("cached body");
        assert!(
            cached_body
                .windows(b"SELECT 2".len())
                .any(|w| w == b"SELECT 2")
        );

        let mut pre_sink = BytesMut::from(&b"old bytes"[..]);
        let preencoded = run_preencoded_select_streamed(cached_body, 2, &mut pre_sink);
        assert_eq!(preencoded.rows, 2);
        assert_eq!(
            preencoded.streamed_body.as_deref(),
            Some(cached_body.as_ref())
        );

        let shared = run_shared_preencoded_select_streamed(std::sync::Arc::from(&b"abc"[..]), 7);
        assert_eq!(shared.rows, 7);
        assert!(shared.streamed_body.is_none());
        assert_eq!(shared.shared_streamed_body.as_deref(), Some(&b"abc"[..]));
    }

    #[test]
    fn typed_text_encoding_covers_temporal_oids_vectors_and_special_floats() {
        assert_eq!(format_f32(f32::NAN), b"NaN");
        assert_eq!(format_f32(f32::INFINITY), b"Infinity");
        assert_eq!(format_f32(f32::NEG_INFINITY), b"-Infinity");
        assert_eq!(format_f64(f64::NAN), b"NaN");
        assert_eq!(format_f64(f64::INFINITY), b"Infinity");
        assert_eq!(format_f64(f64::NEG_INFINITY), b"-Infinity");

        let times = Column::Int64(NumericColumn::from_data(vec![
            3_600_000_000,
            0,
            ultrasql_core::pack_timetz(3_600_000_000, -18_000).expect("timetz"),
            i64::from(u32::MAX) + 1,
        ]));
        assert_eq!(
            encode_text_value_typed(&times, 0, &DataType::Time),
            Some(b"01:00:00".to_vec())
        );
        assert_eq!(
            encode_text_value_typed(&times, 1, &DataType::TimestampTz),
            Some(b"2000-01-01 00:00:00+00".to_vec())
        );
        assert_eq!(
            encode_text_value_typed(&times, 2, &DataType::TimeTz),
            Some(b"01:00:00-05".to_vec())
        );
        assert_eq!(encode_text_value_typed(&times, 3, &DataType::Oid), None);

        let vectors = Column::Utf8(StringColumn::from_data([
            "[1.0,2.50]".to_owned(),
            "{1:0.5}/3".to_owned(),
            "not-a-vector".to_owned(),
        ]));
        assert_eq!(
            encode_text_value_typed(&vectors, 0, &DataType::Vector { dims: Some(2) }),
            Some(b"[1,2.5]".to_vec())
        );
        assert_eq!(
            encode_text_value_typed(&vectors, 1, &DataType::SparseVec { dims: Some(3) }),
            Some(b"{1:0.5}/3".to_vec())
        );
        assert_eq!(
            encode_text_value_typed(&vectors, 2, &DataType::Vector { dims: Some(2) }),
            Some(b"not-a-vector".to_vec())
        );
    }

    #[test]
    fn text_encoding_rejects_invalid_dictionary_code_without_panic() {
        let column = Column::DictionaryUtf8(DictionaryColumn {
            dict: vec!["ok".to_owned()],
            codes: NumericColumn::from_data(vec![7]),
        });

        assert_eq!(encode_text_value(&column, 0), None);
        assert_eq!(
            encode_text_value_typed(&column, 0, &DataType::Text { max_len: None }),
            None
        );
    }

    #[test]
    fn run_select_rejects_invalid_dictionary_code_without_null_substitution() {
        let schema = Schema::new([Field::required("label", DataType::Text { max_len: None })])
            .expect("schema");
        let batch = Batch::new([Column::DictionaryUtf8(DictionaryColumn {
            dict: vec!["ok".to_owned()],
            codes: NumericColumn::from_data(vec![7]),
        })])
        .expect("batch");
        let mut scan = MemTableScan::new(schema, vec![batch]);

        assert!(run_select(&mut scan).is_err());
    }

    #[test]
    fn array_oids_and_type_sizes_cover_extended_type_surface() {
        for (ty, oid) in [
            (DataType::Bool, PG_OID_BOOL_ARRAY),
            (DataType::Int16, PG_OID_INT2_ARRAY),
            (DataType::Int64, PG_OID_INT8_ARRAY),
            (DataType::Float32, PG_OID_FLOAT4_ARRAY),
            (DataType::Float64, PG_OID_FLOAT8_ARRAY),
            (
                DataType::Decimal {
                    precision: None,
                    scale: None,
                },
                PG_OID_NUMERIC_ARRAY,
            ),
            (DataType::Money, PG_OID_MONEY_ARRAY),
            (DataType::Oid, PG_OID_OID_ARRAY),
            (DataType::RegClass, PG_OID_REGCLASS_ARRAY),
            (DataType::RegType, PG_OID_REGTYPE_ARRAY),
            (DataType::PgLsn, PG_OID_PG_LSN_ARRAY),
            (DataType::Text { max_len: None }, PG_OID_TEXT_ARRAY),
            (DataType::Char { len: Some(4) }, PG_OID_BPCHAR_ARRAY),
            (DataType::Bit { len: Some(4) }, PG_OID_BIT_ARRAY),
            (DataType::VarBit { max_len: None }, PG_OID_VARBIT_ARRAY),
            (DataType::Inet, PG_OID_INET_ARRAY),
            (DataType::Cidr, PG_OID_CIDR_ARRAY),
            (DataType::MacAddr, PG_OID_MACADDR_ARRAY),
            (DataType::MacAddr8, PG_OID_MACADDR8_ARRAY),
            (DataType::Date, PG_OID_DATE_ARRAY),
            (DataType::Time, PG_OID_TIME_ARRAY),
            (DataType::Timestamp, PG_OID_TIMESTAMP_ARRAY),
            (DataType::TimeTz, PG_OID_TIMETZ_ARRAY),
            (DataType::TimestampTz, PG_OID_TIMESTAMPTZ_ARRAY),
            (DataType::Bytea, PG_OID_BYTEA_ARRAY),
            (DataType::Uuid, PG_OID_UUID_ARRAY),
            (DataType::Json, PG_OID_JSON_ARRAY),
            (DataType::Jsonb, PG_OID_JSONB_ARRAY),
            (DataType::Xml, PG_OID_XML_ARRAY),
        ] {
            assert_eq!(pg_array_type_oid(&ty), oid);
        }
        assert_eq!(
            pg_array_type_oid(&DataType::Array(Box::new(DataType::Uuid))),
            PG_OID_UUID_ARRAY
        );
        assert_eq!(pg_array_type_oid(&DataType::Null), PG_OID_TEXT_ARRAY);
        assert_eq!(pg_type_size(&DataType::Bool), 1);
        assert_eq!(pg_type_size(&DataType::Int16), 2);
        assert_eq!(pg_type_size(&DataType::Uuid), 16);
        assert_eq!(pg_type_size(&DataType::TimeTz), 12);
        assert_eq!(pg_type_size(&DataType::Jsonb), -1);
    }
}
