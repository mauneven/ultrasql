//! Server-side file access, CSV record framing, and small format helpers for
//! the `COPY ... TO/FROM '<path>'` variants.
//!
//! Holds the filesystem entry points (open/read/write with symlink and size
//! guards), the streaming CSV record-completeness checks, COPY reject-table
//! schema validation, and assorted format/projection helpers shared across the
//! COPY implementation.

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;

use ultrasql_catalog::TableEntry;
#[cfg(test)]
use ultrasql_core::Value;
use ultrasql_core::{DataType, Schema};

use super::{
    COPY_AUTODETECT_SAMPLE_BYTES, CopyOptions, DEFAULT_COPY_BINARY_FILE_LIMIT_BYTES,
    ServerCopyFormat, ServerError,
};

pub(super) fn validate_copy_reject_table(entry: &TableEntry) -> Result<(), ServerError> {
    let fields = entry.schema.fields();
    if fields.len() != 4 {
        return Err(ServerError::CopyFormat(format!(
            "COPY reject_table {} must have columns filename TEXT, line_number BIGINT, raw_row TEXT, error TEXT",
            entry.name
        )));
    }
    let expected = [
        ("filename", RejectColumnType::Text),
        ("line_number", RejectColumnType::Int64),
        ("raw_row", RejectColumnType::Text),
        ("error", RejectColumnType::Text),
    ];
    for (field, (name, ty)) in fields.iter().zip(expected) {
        if !field.name.eq_ignore_ascii_case(name)
            || !reject_column_type_matches(&field.data_type, ty)
        {
            return Err(ServerError::CopyFormat(format!(
                "COPY reject_table {} must have columns filename TEXT, line_number BIGINT, raw_row TEXT, error TEXT",
                entry.name
            )));
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(super) enum RejectColumnType {
    Text,
    Int64,
}

pub(super) fn reject_column_type_matches(data_type: &DataType, expected: RejectColumnType) -> bool {
    match expected {
        RejectColumnType::Text => {
            matches!(data_type, DataType::Text { .. } | DataType::Char { .. })
        }
        RejectColumnType::Int64 => *data_type == DataType::Int64,
    }
}

pub(super) fn read_copy_file_sample(path: &str) -> Result<String, ServerError> {
    let file = open_copy_input_file(path)?;
    let mut reader = BufReader::new(file);
    let mut sample = Vec::new();
    let mut line = Vec::new();
    loop {
        line.clear();
        let bytes_read = reader
            .read_until(b'\n', &mut line)
            .map_err(|e| ServerError::Io(std::io::Error::other(format!("{path}: {e}"))))?;
        if bytes_read == 0 {
            break;
        }
        sample.extend_from_slice(&line);
        if sample.len() >= COPY_AUTODETECT_SAMPLE_BYTES && csv_sample_record_complete(&sample) {
            break;
        }
    }
    String::from_utf8(sample).map_err(|e| {
        ServerError::CopyFormat(format!(
            "COPY AUTO_DETECT {path}: invalid UTF-8 sample: {e}"
        ))
    })
}

pub(super) fn open_copy_input_file(path: &str) -> Result<File, ServerError> {
    ensure_regular_copy_input(path)?;
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW);
    }
    options
        .open(path)
        .map_err(|e| ServerError::Io(std::io::Error::other(format!("{path}: {e}"))))
}

pub(super) fn read_copy_input_file(path: &str) -> Result<Vec<u8>, ServerError> {
    let file = open_copy_input_file(path)?;
    let limit = copy_binary_file_limit_bytes();
    let len = file
        .metadata()
        .map_err(|e| ServerError::Io(std::io::Error::other(format!("{path}: {e}"))))?
        .len();
    if len > limit {
        return Err(ServerError::CopyFormat(format!(
            "COPY binary file exceeds limit: {path} size={len} limit={limit}"
        )));
    }
    let mut bytes = Vec::new();
    let mut limited = file.take(copy_binary_take_limit(limit)?);
    limited
        .read_to_end(&mut bytes)
        .map_err(|e| ServerError::Io(std::io::Error::other(format!("{path}: {e}"))))?;
    let read_len = copy_binary_bytes_read_len(bytes.len())?;
    if read_len > limit {
        return Err(ServerError::CopyFormat(format!(
            "COPY binary file exceeds limit: {path} size={read_len} limit={limit}"
        )));
    }
    Ok(bytes)
}

pub(super) fn copy_binary_file_limit_bytes() -> u64 {
    std::env::var("ULTRASQL_COPY_BINARY_FILE_LIMIT_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_COPY_BINARY_FILE_LIMIT_BYTES)
}

pub(super) fn copy_binary_take_limit(limit: u64) -> Result<u64, ServerError> {
    limit.checked_add(1).ok_or_else(|| {
        ServerError::CopyFormat(format!(
            "COPY binary file read limit is too large: limit={limit}"
        ))
    })
}

fn copy_binary_bytes_read_len(len: usize) -> Result<u64, ServerError> {
    u64::try_from(len).map_err(|_| {
        ServerError::CopyFormat(format!(
            "COPY binary file byte count exceeds u64: bytes={len}"
        ))
    })
}

/// Reject a binary `COPY FROM STDIN` chunk that would push the cumulative
/// stream past `limit`. Pulled out of the streaming loop so the cumulative
/// bound is unit-testable without driving the wire protocol.
pub(super) fn check_copy_stdin_within_limit(
    current_len: usize,
    chunk_len: usize,
    limit: u64,
) -> Result<(), ServerError> {
    let projected = current_len.saturating_add(chunk_len);
    if copy_binary_bytes_read_len(projected)? > limit {
        return Err(ServerError::CopyFormat(format!(
            "COPY FROM STDIN binary stream exceeds limit: size>={projected} limit={limit}"
        )));
    }
    Ok(())
}

fn ensure_regular_copy_input(path: &str) -> Result<(), ServerError> {
    let metadata = fs::symlink_metadata(Path::new(path))
        .map_err(|e| ServerError::Io(std::io::Error::other(format!("{path}: {e}"))))?;
    if metadata.file_type().is_file() {
        Ok(())
    } else {
        Err(ServerError::CopyFormat(format!(
            "COPY file is not a regular file: {path}"
        )))
    }
}

pub(super) fn write_copy_output_file(path: &str, bytes: &[u8]) -> Result<(), ServerError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| ServerError::Io(std::io::Error::other(format!("{path}: {e}"))))?;
    file.write_all(bytes)
        .map_err(|e| ServerError::Io(std::io::Error::other(format!("{path}: {e}"))))
}

pub(super) fn csv_record_complete(record: &[u8], opts: &CopyOptions) -> Result<bool, ServerError> {
    let delimiter = single_byte_delimiter(opts.delimiter)?;
    let mut in_quotes = false;
    let mut at_field_start = true;
    let mut i = 0;
    while i < record.len() {
        let b = record[i];
        if in_quotes {
            if b == b'"' {
                if i + 1 < record.len() && record[i + 1] == b'"' {
                    i += 2;
                    continue;
                }
                in_quotes = false;
            }
            i += 1;
            continue;
        }
        if b == b'"' && at_field_start {
            in_quotes = true;
            at_field_start = false;
        } else {
            at_field_start = b == delimiter || b == b'\n' || b == b'\r';
        }
        i += 1;
    }
    Ok(!in_quotes)
}

pub(super) fn csv_sample_record_complete(sample: &[u8]) -> bool {
    let mut in_quotes = false;
    let mut i = 0;
    while i < sample.len() {
        if sample[i] == b'"' {
            if in_quotes && i + 1 < sample.len() && sample[i + 1] == b'"' {
                i += 2;
                continue;
            }
            in_quotes = !in_quotes;
        }
        i += 1;
    }
    !in_quotes
}

pub(super) fn single_byte_delimiter(delimiter: char) -> Result<u8, ServerError> {
    let mut bytes = [0_u8; 4];
    let encoded = delimiter.encode_utf8(&mut bytes).as_bytes();
    if encoded.len() != 1 {
        return Err(ServerError::CopyFormat(
            "COPY delimiter must be one byte for streaming CSV".to_string(),
        ));
    }
    Ok(encoded[0])
}

pub(super) fn copy_format_code(format: ServerCopyFormat) -> u8 {
    match format {
        ServerCopyFormat::Text | ServerCopyFormat::Csv => 0,
        ServerCopyFormat::Binary => 1,
        ServerCopyFormat::Parquet => 0,
    }
}

pub(super) fn projected_schema(
    entry: &TableEntry,
    columns: &[usize],
) -> Result<Schema, ServerError> {
    if columns.is_empty() {
        return Ok(entry.schema.clone());
    }
    let fields = columns
        .iter()
        .map(|&i| entry.schema.fields()[i].clone())
        .collect::<Vec<_>>();
    Schema::new(fields).map_err(|e| ServerError::CopyFormat(format!("COPY schema: {e}")))
}

#[cfg(test)]
pub(super) fn copy_cells_from_row(
    row: &[Value],
    schema: &Schema,
    columns: &[usize],
) -> Vec<Option<Vec<u8>>> {
    super::decode::copy_cells_from_row_with_options(
        row,
        schema,
        columns,
        &crate::result_encoder::TextEncodingOptions::default(),
    )
}
