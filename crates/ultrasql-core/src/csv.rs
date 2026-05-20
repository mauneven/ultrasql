//! Small CSV parser shared by file-backed SQL surfaces.
//!
//! The parser implements the RFC-4180 pieces UltraSQL needs for local CSV
//! table functions: comma delimiter, double-quoted fields, doubled quote
//! escapes, CRLF/LF records, and quoted newlines. Type inference lives above
//! this layer; parsed cells are UTF-8 strings.

use std::error::Error as StdError;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

/// Error returned when CSV input is malformed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CsvError {
    message: String,
}

impl CsvError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for CsvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl StdError for CsvError {}

/// Parse UTF-8 CSV text into records.
///
/// Empty physical lines are skipped. Non-empty records keep empty cells as
/// empty strings; this parser does not infer SQL NULLs.
///
/// # Errors
///
/// Returns [`CsvError`] when a quoted field is not closed by end-of-input.
pub fn parse_csv_records(input: &str) -> Result<Vec<Vec<String>>, CsvError> {
    let mut records = Vec::new();
    let mut row = Vec::new();
    let mut field = String::new();
    let mut in_quotes = false;
    let mut field_started = false;
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if in_quotes {
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    field.push('"');
                } else {
                    in_quotes = false;
                }
            } else {
                field.push(ch);
            }
            continue;
        }

        match ch {
            '"' if !field_started => {
                in_quotes = true;
                field_started = true;
            }
            ',' => finish_field(&mut row, &mut field, &mut field_started),
            '\n' => finish_record(&mut records, &mut row, &mut field, &mut field_started),
            '\r' => {
                if chars.peek() == Some(&'\n') {
                    chars.next();
                }
                finish_record(&mut records, &mut row, &mut field, &mut field_started);
            }
            other => {
                field_started = true;
                field.push(other);
            }
        }
    }

    if in_quotes {
        return Err(CsvError::new("unterminated quoted field"));
    }
    if field_started || !field.is_empty() || !row.is_empty() {
        finish_record(&mut records, &mut row, &mut field, &mut field_started);
    }
    Ok(records)
}

/// Expand a `read_csv` path or single-directory wildcard pattern.
///
/// Literal paths return one entry. Patterns support `*` and `?` in the file
/// name component, e.g. `/tmp/*.csv`; wildcarded parent directories are not
/// expanded. Returned paths are sorted lexicographically for stable query
/// output.
///
/// # Errors
///
/// Returns [`CsvError`] when the directory cannot be read or the pattern
/// matches no files.
pub fn expand_csv_paths(pattern: &str) -> Result<Vec<PathBuf>, CsvError> {
    let path = Path::new(pattern);
    let file_pattern = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            CsvError::new(format!(
                "read_csv path must name a file or wildcard: {pattern}"
            ))
        })?;
    if !contains_wildcard(file_pattern) {
        return Ok(vec![path.to_path_buf()]);
    }

    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut paths = Vec::new();
    for entry in fs::read_dir(parent).map_err(|err| {
        CsvError::new(format!(
            "read_csv cannot read directory {}: {err}",
            parent.display()
        ))
    })? {
        let entry = entry
            .map_err(|err| CsvError::new(format!("read_csv directory entry failed: {err}")))?;
        let Some(name) = entry.file_name().to_str().map(ToOwned::to_owned) else {
            continue;
        };
        if wildcard_match(file_pattern, &name) {
            paths.push(entry.path());
        }
    }
    paths.sort();
    if paths.is_empty() {
        return Err(CsvError::new(format!(
            "read_csv pattern matched no files: {pattern}"
        )));
    }
    Ok(paths)
}

/// Read and parse one UTF-8 CSV file.
///
/// # Errors
///
/// Returns [`CsvError`] on file I/O, UTF-8, or CSV syntax failure.
pub fn read_csv_records_from_path(path: &Path) -> Result<Vec<Vec<String>>, CsvError> {
    let text = fs::read_to_string(path)
        .map_err(|err| CsvError::new(format!("read_csv cannot read {}: {err}", path.display())))?;
    parse_csv_records(&text)
        .map_err(|err| CsvError::new(format!("read_csv parse {}: {err}", path.display())))
}

/// Read the header row from the first file matched by a `read_csv` pattern.
///
/// # Errors
///
/// Returns [`CsvError`] when no files match or the first file has no header.
pub fn read_csv_header(pattern: &str) -> Result<Vec<String>, CsvError> {
    let paths = expand_csv_paths(pattern)?;
    let first = paths
        .first()
        .expect("expand_csv_paths returns non-empty paths");
    let records = read_csv_records_from_path(first)?;
    let header = records.first().ok_or_else(|| {
        CsvError::new(format!(
            "read_csv file has no header row: {}",
            first.display()
        ))
    })?;
    if header.is_empty() || header.iter().any(String::is_empty) {
        return Err(CsvError::new(format!(
            "read_csv header contains an empty column name: {}",
            first.display()
        )));
    }
    Ok(header.clone())
}

fn finish_field(row: &mut Vec<String>, field: &mut String, field_started: &mut bool) {
    row.push(std::mem::take(field));
    *field_started = false;
}

fn finish_record(
    records: &mut Vec<Vec<String>>,
    row: &mut Vec<String>,
    field: &mut String,
    field_started: &mut bool,
) {
    if *field_started || !field.is_empty() || !row.is_empty() {
        row.push(std::mem::take(field));
        records.push(std::mem::take(row));
    }
    *field_started = false;
}

fn contains_wildcard(s: &str) -> bool {
    s.chars().any(|ch| matches!(ch, '*' | '?'))
}

fn wildcard_match(pattern: &str, text: &str) -> bool {
    let pattern = pattern.chars().collect::<Vec<_>>();
    let text = text.chars().collect::<Vec<_>>();
    let mut dp = vec![vec![false; text.len() + 1]; pattern.len() + 1];
    dp[0][0] = true;
    for (i, ch) in pattern.iter().enumerate() {
        if *ch == '*' {
            dp[i + 1][0] = dp[i][0];
        }
    }
    for (i, pattern_ch) in pattern.iter().enumerate() {
        for (j, text_ch) in text.iter().enumerate() {
            dp[i + 1][j + 1] = match pattern_ch {
                '*' => dp[i][j + 1] || dp[i + 1][j],
                '?' => dp[i][j],
                ch => dp[i][j] && ch == text_ch,
            };
        }
    }
    dp[pattern.len()][text.len()]
}

#[cfg(test)]
mod tests {
    use super::parse_csv_records;

    #[test]
    fn parses_quoted_commas_and_quotes() {
        let rows = parse_csv_records("id,name\n1,\"Grace, Jr.\"\n2,\"Ada \"\"L\"\"\"\n")
            .expect("csv parses");
        assert_eq!(
            rows,
            vec![
                vec!["id".to_string(), "name".to_string()],
                vec!["1".to_string(), "Grace, Jr.".to_string()],
                vec!["2".to_string(), "Ada \"L\"".to_string()],
            ]
        );
    }

    #[test]
    fn parses_quoted_newlines() {
        let rows = parse_csv_records("id,body\r\n1,\"hello\nworld\"\r\n").expect("csv parses");
        assert_eq!(
            rows,
            vec![
                vec!["id".to_string(), "body".to_string()],
                vec!["1".to_string(), "hello\nworld".to_string()],
            ]
        );
    }

    #[test]
    fn rejects_unclosed_quote() {
        let err = parse_csv_records("id,name\n1,\"Ada").expect_err("quote is unclosed");
        assert!(err.to_string().contains("unterminated"));
    }

    #[test]
    fn wildcard_match_supports_star_and_question_mark() {
        assert!(super::wildcard_match("*.csv", "a.csv"));
        assert!(super::wildcard_match("part-?.csv", "part-a.csv"));
        assert!(!super::wildcard_match("part-?.csv", "part-ab.csv"));
    }
}
