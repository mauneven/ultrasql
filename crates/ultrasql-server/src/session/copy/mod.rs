//! Session-level dispatch for the `COPY` statement.
//!
//! The synchronous `execute_query` path cannot drive `COPY`'s wire flow
//! because every `CopyData` frame is an async read/write. This module
//! reopens the `impl<RW> Session<RW>` block and handles `COPY` end-to-end
//! against the async I/O surface.
//!
//! ## Protocol
//!
//! ### `COPY t TO STDOUT`
//!
//! ```text
//! Server: CopyOutResponse { overall_format: 0, column_formats: [0; n] }
//! Server: CopyData(row_bytes)  ×N
//! Server: CopyDone
//! Server: CommandComplete { tag: "COPY N" }
//! Server: ReadyForQuery
//! ```
//!
//! ### `COPY t FROM STDIN`
//!
//! ```text
//! Server: CopyInResponse { overall_format: 0, column_formats: [0; n] }
//! Client: CopyData(chunk)  ×N
//! Client: CopyDone   -or-   CopyFail
//! Server: CommandComplete { tag: "COPY N" }    (on CopyDone)
//! Server: ErrorResponse                         (on CopyFail or row error)
//! Server: ReadyForQuery
//! ```
//!
//! The implementation is split across submodules:
//! - [`dispatch`]: parse/bind probe and direction/source routing.
//! - [`stdio`]: `STDOUT`/`STDIN` streaming over the client connection.
//! - [`file_ops`]: server-side file COPY and `COPY (query) TO ...`.
//! - [`binary`]: the binary `PGCOPY` wire codec.
//! - [`decode`]: textual cell decode/encode and per-type parsers.
//! - [`fs_io`]: file access, CSV framing, and format helpers.

use std::io::BufRead;

use ultrasql_catalog::{TableEntry, table_lookup_key};
use ultrasql_core::Schema;
use ultrasql_executor::{ExecError, RowCodec};
use ultrasql_txn::Transaction;

use super::jsonb_ingest::JsonbShapeCache;
// These re-exports are reached by the COPY submodules through `super::`. A
// private `use` here is sufficient: child modules may access private items of
// their ancestor modules.
use crate::copy::{
    CopyFormat as ServerCopyFormat, CopyOptions, copy_in_response_with_format,
    copy_out_response_with_format, encode_csv_row, encode_text_row, parse_csv_row, parse_text_row,
    parse_unquoted_csv_row_slices,
};
use crate::error::ServerError;

mod binary;
mod decode;
mod dispatch;
mod file_ops;
mod fs_io;
mod maintain;
mod stdio;
#[cfg(test)]
mod tests;

const COPY_INSERT_BATCH_ROWS: usize = 4096;
const COPY_AUTODETECT_SAMPLE_BYTES: usize = 64 * 1024;
const DEFAULT_COPY_BINARY_FILE_LIMIT_BYTES: u64 = 128 * 1024 * 1024;
const MICROS_PER_DAY: i64 = 86_400_000_000;

fn copy_row_count_overflow(context: &str) -> ServerError {
    ServerError::Execute(ExecError::NumericFieldOverflow(format!(
        "{context} row count overflow"
    )))
}

fn copy_rows_from_usize(rows: usize, context: &str) -> Result<u64, ServerError> {
    u64::try_from(rows).map_err(|_| copy_row_count_overflow(context))
}

pub(in crate::session) fn copy_table_key(entry: &TableEntry) -> String {
    table_lookup_key(&entry.schema_name, &entry.name)
}

pub(super) fn add_copy_rows(rows: &mut u64, delta: u64, context: &str) -> Result<(), ServerError> {
    *rows = rows
        .checked_add(delta)
        .ok_or_else(|| copy_row_count_overflow(context))?;
    Ok(())
}

pub(super) fn add_copy_batch_rows(
    rows: &mut u64,
    batch_len: usize,
    context: &str,
) -> Result<(), ServerError> {
    let delta = copy_rows_from_usize(batch_len, context)?;
    add_copy_rows(rows, delta, context)
}

pub(super) fn increment_copy_rows(rows: &mut u64, context: &str) -> Result<(), ServerError> {
    add_copy_rows(rows, 1, context)
}

fn copy_add_row_counts(left: u64, right: u64, context: &str) -> Result<u64, ServerError> {
    left.checked_add(right)
        .ok_or_else(|| copy_row_count_overflow(context))
}

struct CopyRejectTarget {
    entry: TableEntry,
    codec: RowCodec,
    payload_batch: Vec<Vec<u8>>,
    rows: u64,
}

struct CopyRejectState {
    max_errors: u64,
    bad_rows: u64,
    target: Option<CopyRejectTarget>,
}

struct CopyTextFileStreamArgs<'a> {
    entry: &'a TableEntry,
    columns: &'a [usize],
    schema: &'a Schema,
    opts: &'a CopyOptions,
    codec: &'a RowCodec,
    txn: &'a Transaction,
    reader: &'a mut dyn BufRead,
    payload_batch: &'a mut Vec<Vec<u8>>,
    reject_state: Option<&'a mut CopyRejectState>,
    path: &'a str,
    /// Whether freshly bulk-filled pages may be stamped all-visible. `true`
    /// only for autocommit COPY (the implicit txn commits immediately); `false`
    /// when riding the open session txn, whose uncommitted rows must stay
    /// MVCC-governed so a ROLLBACK still discards them.
    mark_all_visible: bool,
}

struct CopyRowDecodeContext<'a> {
    entry: &'a TableEntry,
    columns: &'a [usize],
    schema: &'a Schema,
    codec: &'a RowCodec,
    jsonb_shape_cache: &'a mut JsonbShapeCache,
}

/// The transaction a COPY batch inserts under, plus whether freshly bulk-filled
/// pages may be stamped all-visible (autocommit only — see
/// [`Session::flush_copy_insert_batch`]). The two always travel together, so
/// they ride in one bundle to keep the reject-row helpers under the argument
/// budget.
#[derive(Clone, Copy)]
struct CopyInsertTxn<'a> {
    txn: &'a Transaction,
    mark_all_visible: bool,
}
