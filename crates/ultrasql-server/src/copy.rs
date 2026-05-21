//! COPY wire format — `COPY TO STDOUT` and `COPY FROM STDIN`.
//!
//! ## Protocol flow
//!
//! ### COPY TO STDOUT (server → client)
//!
//! ```text
//! Server: CopyOutResponse { overall_format: 0 (text), column_formats: [0, …] }
//! Server: CopyData(row_bytes)  ×N
//! Server: CopyDone
//! Server: CommandComplete { tag: "COPY N" }
//! Server: ReadyForQuery
//! ```
//!
//! ### COPY FROM STDIN (client → server)
//!
//! ```text
//! Server: CopyInResponse { overall_format: 0 (text), column_formats: [0, …] }
//! Client: CopyData(chunk)  ×N
//! Client: CopyDone  -or-  CopyFail
//! Server: CommandComplete or ErrorResponse
//! Server: ReadyForQuery
//! ```
//!
//! ## v0.5 format support
//!
//! Only `text` format is required for v0.5. CSV format is a v0.6
//! follow-up (tracked in ROADMAP.md).
//!
//! ## Text format
//!
//! Columns are separated by `\t`; rows end with `\n`. `NULL` is encoded
//! as `\N`. Backslash-escape sequences (`\\`, `\t`, `\n`, `\r`) are
//! recognised during import.

use ultrasql_protocol::BackendMessage;

use crate::error::ServerError;

/// COPY format negotiated for a session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopyFormat {
    /// PostgreSQL text format: tab-separated columns, newline rows.
    Text,
    /// PostgreSQL CSV format: comma-separated, double-quoted strings,
    /// configurable NULL marker (empty string by default).
    Csv,
    /// PostgreSQL binary COPY format.
    Binary,
    /// Apache Parquet file format for server-side file COPY.
    Parquet,
}

/// Options parsed from a `COPY … WITH (…)` clause.
#[derive(Clone, Debug)]
pub struct CopyOptions {
    /// Wire format.
    pub format: CopyFormat,
    /// Column delimiter. Default `\t` for text, `,` for CSV.
    pub delimiter: char,
    /// Representation of SQL NULL. Default `\N` for text.
    pub null_str: String,
    /// Whether the first row is a header. Default `false`.
    pub header: bool,
    /// Whether CSV COPY should sniff delimiter/header before reading rows.
    pub auto_detect: bool,
    /// Whether bad COPY FROM rows should be skipped/quarantined.
    pub ignore_errors: bool,
    /// Maximum bad rows tolerated while `ignore_errors` is enabled.
    pub max_errors: u64,
    /// Optional table receiving quarantined rows.
    pub reject_table: Option<String>,
}

impl Default for CopyOptions {
    fn default() -> Self {
        Self {
            format: CopyFormat::Text,
            delimiter: '\t',
            null_str: r"\N".to_string(),
            header: false,
            auto_detect: false,
            ignore_errors: false,
            max_errors: 0,
            reject_table: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Text-format helpers
// ---------------------------------------------------------------------------

/// Encode a row as a PostgreSQL text-format COPY line.
///
/// Each column is separated by `opts.delimiter`; NULL values are rendered
/// as `opts.null_str`. The line is terminated with `\n`.
///
/// Backslash, tab, newline, and carriage-return characters in string
/// values are backslash-escaped so they are not confused with delimiters.
pub fn encode_text_row(columns: &[Option<Vec<u8>>], opts: &CopyOptions) -> Vec<u8> {
    let mut out = Vec::new();
    for (i, col) in columns.iter().enumerate() {
        if i > 0 {
            let delim_bytes = {
                let mut b = [0u8; 4];
                let s = opts.delimiter.encode_utf8(&mut b);
                s.as_bytes().to_vec()
            };
            out.extend_from_slice(&delim_bytes);
        }
        match col {
            None => out.extend_from_slice(opts.null_str.as_bytes()),
            Some(bytes) => {
                for &b in bytes {
                    match b {
                        b'\\' => out.extend_from_slice(b"\\\\"),
                        b'\t' => out.extend_from_slice(b"\\t"),
                        b'\n' => out.extend_from_slice(b"\\n"),
                        b'\r' => out.extend_from_slice(b"\\r"),
                        other => out.push(other),
                    }
                }
            }
        }
    }
    out.push(b'\n');
    out
}

/// Parse a text-format COPY line into column byte-strings.
///
/// The trailing `\n` is stripped if present. Backslash escape sequences
/// `\\`, `\t`, `\n`, `\r` are decoded. A column equal to `null_str` is
/// returned as `None` (SQL NULL).
pub fn parse_text_row(
    line: &[u8],
    opts: &CopyOptions,
) -> Result<Vec<Option<Vec<u8>>>, ServerError> {
    // Strip trailing newline.
    let line = if line.ends_with(b"\n") {
        &line[..line.len() - 1]
    } else {
        line
    };
    // Strip CR too.
    let line = if line.ends_with(b"\r") {
        &line[..line.len() - 1]
    } else {
        line
    };

    let delim_byte = {
        let mut b = [0u8; 4];
        opts.delimiter.encode_utf8(&mut b);
        b[0]
    };

    let mut columns = Vec::new();
    let mut current: Vec<u8> = Vec::new();
    let mut i = 0;

    while i < line.len() {
        let b = line[i];
        if b == b'\\' && i + 1 < line.len() {
            i += 1;
            match line[i] {
                b'\\' => current.push(b'\\'),
                b't' => current.push(b'\t'),
                b'n' => current.push(b'\n'),
                b'r' => current.push(b'\r'),
                other => {
                    current.push(b'\\');
                    current.push(other);
                }
            }
        } else if b == delim_byte {
            columns.push(decode_null(&current, opts));
            current.clear();
        } else {
            current.push(b);
        }
        i += 1;
    }
    columns.push(decode_null(&current, opts));

    Ok(columns)
}

/// Return `None` if `bytes` equals `null_str`, otherwise `Some(bytes.to_vec())`.
fn decode_null(bytes: &[u8], opts: &CopyOptions) -> Option<Vec<u8>> {
    if bytes == opts.null_str.as_bytes() {
        None
    } else {
        Some(bytes.to_vec())
    }
}

// ---------------------------------------------------------------------------
// CSV-format helpers
// ---------------------------------------------------------------------------

/// Encode a row as a PostgreSQL CSV-format COPY line.
///
/// Fields are separated by `opts.delimiter` (default `,`). A value
/// containing the delimiter, double-quote, CR, or LF is wrapped in
/// double-quotes; embedded double-quotes are doubled (`""`). NULL
/// columns are rendered as `opts.null_str` *unquoted* — matching
/// PostgreSQL's CSV semantics.
#[must_use]
pub fn encode_csv_row(columns: &[Option<Vec<u8>>], opts: &CopyOptions) -> Vec<u8> {
    let mut out = Vec::new();
    let delim_bytes = {
        let mut b = [0u8; 4];
        let s = opts.delimiter.encode_utf8(&mut b);
        s.as_bytes().to_vec()
    };
    for (i, col) in columns.iter().enumerate() {
        if i > 0 {
            out.extend_from_slice(&delim_bytes);
        }
        match col {
            None => out.extend_from_slice(opts.null_str.as_bytes()),
            Some(bytes) => {
                let needs_quote = bytes
                    .iter()
                    .any(|&b| b == b'"' || b == b'\n' || b == b'\r' || delim_bytes.contains(&b));
                if needs_quote {
                    out.push(b'"');
                    for &b in bytes {
                        if b == b'"' {
                            out.extend_from_slice(b"\"\"");
                        } else {
                            out.push(b);
                        }
                    }
                    out.push(b'"');
                } else {
                    out.extend_from_slice(bytes);
                }
            }
        }
    }
    out.push(b'\n');
    out
}

/// Parse a CSV-format COPY line into column byte-strings.
///
/// Handles double-quoted fields with embedded delimiters, newlines, and
/// escaped double-quotes (`""` → `"`). The trailing newline is stripped
/// if present. A bare (unquoted) column equal to `opts.null_str` is
/// returned as `None` (SQL NULL); a *quoted* empty string is always a
/// real empty string. This matches PostgreSQL's CSV semantics.
///
/// # Errors
///
/// Returns [`ServerError::CopyFormat`] if a quoted field is never closed.
pub fn parse_csv_row(line: &[u8], opts: &CopyOptions) -> Result<Vec<Option<Vec<u8>>>, ServerError> {
    let line = strip_copy_line_ending(line);
    if !line.contains(&b'"') {
        return parse_unquoted_csv_row(line, opts);
    }
    let delim_byte = {
        let mut b = [0u8; 4];
        opts.delimiter.encode_utf8(&mut b);
        b[0]
    };
    let mut columns: Vec<Option<Vec<u8>>> = Vec::new();
    let mut current: Vec<u8> = Vec::new();
    let mut quoted_field = false;
    let mut field_was_quoted = false;
    let mut i = 0;
    while i < line.len() {
        let b = line[i];
        if quoted_field {
            if b == b'"' {
                if i + 1 < line.len() && line[i + 1] == b'"' {
                    current.push(b'"');
                    i += 2;
                    continue;
                }
                quoted_field = false;
                i += 1;
                continue;
            }
            current.push(b);
            i += 1;
        } else if b == b'"' && current.is_empty() {
            quoted_field = true;
            field_was_quoted = true;
            i += 1;
        } else if b == delim_byte {
            columns.push(finalise_csv_column(&current, field_was_quoted, opts));
            current.clear();
            field_was_quoted = false;
            i += 1;
        } else {
            current.push(b);
            i += 1;
        }
    }
    if quoted_field {
        return Err(ServerError::CopyFormat(
            "unterminated quoted field in CSV input".into(),
        ));
    }
    columns.push(finalise_csv_column(&current, field_was_quoted, opts));
    Ok(columns)
}

fn parse_unquoted_csv_row(
    line: &[u8],
    opts: &CopyOptions,
) -> Result<Vec<Option<Vec<u8>>>, ServerError> {
    parse_unquoted_csv_row_slices(line, opts).map(|cells| {
        cells
            .into_iter()
            .map(|cell| cell.map(<[u8]>::to_vec))
            .collect()
    })
}

/// Parse an unquoted CSV row into borrowed cell slices.
///
/// This is the COPY hot path for machine-generated CSV without quoted
/// fields: numeric casts can read directly from the input line and avoid a
/// per-field `Vec<u8>` allocation before row encoding.
pub(crate) fn parse_unquoted_csv_row_slices<'a>(
    line: &'a [u8],
    opts: &CopyOptions,
) -> Result<Vec<Option<&'a [u8]>>, ServerError> {
    let line = if line.ends_with(b"\n") {
        &line[..line.len() - 1]
    } else {
        line
    };
    let line = if line.ends_with(b"\r") {
        &line[..line.len() - 1]
    } else {
        line
    };
    if line.contains(&b'"') {
        return Err(ServerError::CopyFormat(
            "unquoted CSV fast path received quoted field".to_owned(),
        ));
    }
    let delim_byte = {
        let mut b = [0u8; 4];
        opts.delimiter.encode_utf8(&mut b);
        b[0]
    };
    Ok(parse_unquoted_csv_row_inner(line, delim_byte, opts))
}

fn parse_unquoted_csv_row_inner<'a>(
    line: &'a [u8],
    delim_byte: u8,
    opts: &CopyOptions,
) -> Vec<Option<&'a [u8]>> {
    let mut columns = Vec::with_capacity(line.iter().filter(|&&b| b == delim_byte).count() + 1);
    let mut start = 0_usize;
    for (idx, &b) in line.iter().enumerate() {
        if b == delim_byte {
            columns.push(finalise_csv_column_slice(&line[start..idx], false, opts));
            start = idx.saturating_add(1);
        }
    }
    columns.push(finalise_csv_column_slice(&line[start..], false, opts));
    columns
}

fn strip_copy_line_ending(line: &[u8]) -> &[u8] {
    let line = if line.ends_with(b"\n") {
        &line[..line.len() - 1]
    } else {
        line
    };
    if line.ends_with(b"\r") {
        &line[..line.len() - 1]
    } else {
        line
    }
}

/// Decide whether a finished CSV column is SQL NULL or a real value.
fn finalise_csv_column(
    bytes: &[u8],
    field_was_quoted: bool,
    opts: &CopyOptions,
) -> Option<Vec<u8>> {
    if !field_was_quoted && bytes == opts.null_str.as_bytes() {
        None
    } else {
        Some(bytes.to_vec())
    }
}

fn finalise_csv_column_slice<'a>(
    bytes: &'a [u8],
    field_was_quoted: bool,
    opts: &CopyOptions,
) -> Option<&'a [u8]> {
    if !field_was_quoted && bytes == opts.null_str.as_bytes() {
        None
    } else {
        Some(bytes)
    }
}

// ---------------------------------------------------------------------------
// Protocol message builders
// ---------------------------------------------------------------------------

/// Build the `CopyOutResponse` message for a COPY TO STDOUT operation.
///
/// `n_columns` is the number of output columns; all use `format_code`.
#[must_use]
pub fn copy_out_response(n_columns: usize) -> BackendMessage {
    copy_out_response_with_format(n_columns, 0)
}

#[must_use]
pub fn copy_out_response_with_format(n_columns: usize, format_code: u8) -> BackendMessage {
    BackendMessage::CopyOutResponse {
        overall_format: format_code,
        column_formats: vec![u16::from(format_code); n_columns],
    }
}

/// Build the `CopyInResponse` message for a COPY FROM STDIN operation.
///
/// `n_columns` is the number of expected input columns; all use text format.
#[must_use]
pub fn copy_in_response(n_columns: usize) -> BackendMessage {
    copy_in_response_with_format(n_columns, 0)
}

#[must_use]
pub fn copy_in_response_with_format(n_columns: usize, format_code: u8) -> BackendMessage {
    BackendMessage::CopyInResponse {
        overall_format: format_code,
        column_formats: vec![u16::from(format_code); n_columns],
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_opts() -> CopyOptions {
        CopyOptions::default()
    }

    // ── Text encode ─────────────────────────────────────────────────────────

    #[test]
    fn encode_text_row_basic_values() {
        let opts = default_opts();
        let row = vec![
            Some(b"Alice".to_vec()),
            Some(b"42".to_vec()),
            Some(b"0.9".to_vec()),
        ];
        let encoded = encode_text_row(&row, &opts);
        assert_eq!(encoded, b"Alice\t42\t0.9\n");
    }

    #[test]
    fn encode_text_row_null_column() {
        let opts = default_opts();
        let row = vec![Some(b"x".to_vec()), None, Some(b"y".to_vec())];
        let encoded = encode_text_row(&row, &opts);
        assert_eq!(encoded, b"x\t\\N\ty\n");
    }

    #[test]
    fn encode_text_row_escapes_special_chars() {
        let opts = default_opts();
        let row = vec![Some(b"a\\b\tc\nd".to_vec())];
        let encoded = encode_text_row(&row, &opts);
        assert_eq!(encoded, b"a\\\\b\\tc\\nd\n");
    }

    // ── Text parse ──────────────────────────────────────────────────────────

    #[test]
    fn parse_text_row_basic() {
        let opts = default_opts();
        let line = b"Alice\t42\t0.9\n";
        let cols = parse_text_row(line, &opts).expect("parse ok");
        assert_eq!(
            cols,
            vec![
                Some(b"Alice".to_vec()),
                Some(b"42".to_vec()),
                Some(b"0.9".to_vec()),
            ]
        );
    }

    #[test]
    fn parse_text_row_null() {
        let opts = default_opts();
        let cols = parse_text_row(b"x\t\\N\ty\n", &opts).expect("parse ok");
        assert_eq!(cols, vec![Some(b"x".to_vec()), None, Some(b"y".to_vec())]);
    }

    #[test]
    fn parse_text_row_escape_decode() {
        let opts = default_opts();
        let cols = parse_text_row(b"a\\\\b\\tc\\nd\n", &opts).expect("parse ok");
        assert_eq!(cols, vec![Some(b"a\\b\tc\nd".to_vec())]);
    }

    // ── Round-trip ──────────────────────────────────────────────────────────

    #[test]
    fn encode_then_parse_round_trip() {
        let opts = default_opts();
        let original = vec![
            Some(b"hello\tworld".to_vec()),
            None,
            Some(b"line1\nline2".to_vec()),
            Some(b"back\\slash".to_vec()),
        ];
        let line = encode_text_row(&original, &opts);
        let parsed = parse_text_row(&line, &opts).expect("parse ok");
        assert_eq!(parsed, original);
    }

    #[test]
    fn parse_unquoted_csv_row_fast_path_preserves_nulls_and_crlf() {
        let opts = CopyOptions {
            format: CopyFormat::Csv,
            delimiter: ',',
            null_str: "NULL".to_owned(),
            ..CopyOptions::default()
        };

        let cols = parse_unquoted_csv_row(b"1,NULL,plain\r\n", &opts).expect("parse csv");

        assert_eq!(
            cols,
            vec![Some(b"1".to_vec()), None, Some(b"plain".to_vec())]
        );
    }

    #[test]
    fn parse_unquoted_csv_row_slices_borrows_cells() {
        let opts = CopyOptions {
            format: CopyFormat::Csv,
            delimiter: ',',
            null_str: "NULL".to_owned(),
            ..CopyOptions::default()
        };

        let cells = parse_unquoted_csv_row_slices(b"42,NULL,delta\n", &opts).expect("parse slices");

        assert_eq!(cells, vec![Some(&b"42"[..]), None, Some(&b"delta"[..])]);
    }

    // ── Protocol message builders ────────────────────────────────────────────

    #[test]
    fn copy_out_response_has_correct_column_count() {
        let msg = copy_out_response(3);
        match msg {
            BackendMessage::CopyOutResponse {
                overall_format,
                column_formats,
            } => {
                assert_eq!(overall_format, 0);
                assert_eq!(column_formats.len(), 3);
                assert!(column_formats.iter().all(|&c| c == 0));
            }
            _ => panic!("expected CopyOutResponse"),
        }
    }

    #[test]
    fn copy_in_response_has_correct_column_count() {
        let msg = copy_in_response(2);
        match msg {
            BackendMessage::CopyInResponse {
                overall_format,
                column_formats,
            } => {
                assert_eq!(overall_format, 0);
                assert_eq!(column_formats.len(), 2);
            }
            _ => panic!("expected CopyInResponse"),
        }
    }
}
