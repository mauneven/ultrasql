//! `ultrasql-local` — run UltraSQL queries without a server process.
//!
//! This binary is the file-query entry point: it uses the server crate's
//! in-process local runner instead of the PostgreSQL wire protocol.

use std::fmt::Write as _;
use std::io::Read as _;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;
use ultrasql_server::{LocalQueryOutput, Server};

const DEFAULT_LOCAL_SQL_LIMIT_BYTES: u64 = 128 * 1024 * 1024;

/// Query local files through UltraSQL without starting `ultrasqld`.
#[derive(Debug, Parser)]
#[command(name = "ultrasql-local", about, version)]
struct Cli {
    /// Execute one SQL query and exit.
    #[arg(short = 'q', long, conflicts_with = "file")]
    query: Option<String>,

    /// Read SQL from this file and execute each statement.
    #[arg(short = 'f', long)]
    file: Option<PathBuf>,
}

fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();
    let cli = Cli::parse();
    let sql = read_sql(&cli)?;
    let server = Arc::new(Server::with_sample_database());
    for statement in split_statements(&sql) {
        let output = server.execute_local_query(statement)?;
        print_output(&output)?;
    }
    Ok(())
}

fn read_sql(cli: &Cli) -> Result<String> {
    if let Some(query) = &cli.query {
        return Ok(query.clone());
    }
    let limit = local_sql_limit_bytes();
    if let Some(file) = &cli.file {
        let file_handle = open_sql_file(file)?;
        return read_limited_utf8(file_handle, "SQL file", file.display().to_string(), limit);
    }
    read_limited_utf8(std::io::stdin(), "SQL stdin", "stdin".to_owned(), limit)
}

fn local_sql_limit_bytes() -> u64 {
    std::env::var("ULTRASQL_LOCAL_SQL_LIMIT_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_LOCAL_SQL_LIMIT_BYTES)
}

fn open_sql_file(path: &PathBuf) -> Result<std::fs::File> {
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("cannot inspect SQL file: {}", path.display()))?;
    if !metadata.file_type().is_file() {
        anyhow::bail!("SQL file is not a regular file: {}", path.display());
    }
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW);
    }
    options
        .open(path)
        .with_context(|| format!("cannot read SQL file: {}", path.display()))
}

fn read_limited_utf8<R: std::io::Read>(
    reader: R,
    context: &str,
    display: String,
    limit: u64,
) -> Result<String> {
    let mut bytes = Vec::new();
    let mut limited = reader.take(limit.saturating_add(1));
    limited
        .read_to_end(&mut bytes)
        .with_context(|| format!("cannot read {context}: {display}"))?;
    let read_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if read_len > limit {
        anyhow::bail!("{context} exceeds read limit: {display} size={read_len} limit={limit}");
    }
    String::from_utf8(bytes).with_context(|| format!("{context} is not UTF-8: {display}"))
}

fn print_output(output: &LocalQueryOutput) -> Result<()> {
    if output.columns.is_empty() {
        if !output.command_tag.is_empty() {
            println!("{}", output.command_tag);
        }
        return Ok(());
    }
    if output.rows.is_empty() {
        println!("(0 rows)");
        return Ok(());
    }

    let headers = output
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    let cells = output
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|cell| cell.clone().unwrap_or_else(|| "NULL".to_owned()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let widths = headers
        .iter()
        .enumerate()
        .map(|(idx, header)| {
            let max_cell = cells.iter().map(|row| row[idx].len()).max().unwrap_or(0);
            header.len().max(max_cell)
        })
        .collect::<Vec<_>>();

    let sep = build_separator(&widths);
    println!("{sep}");
    let mut header_row = String::from("|");
    for (header, width) in headers.iter().zip(&widths) {
        write!(header_row, " {header:<width$} |")?;
    }
    println!("{header_row}");
    println!("{sep}");
    for row in &cells {
        let mut line = String::from("|");
        for (cell, width) in row.iter().zip(&widths) {
            write!(line, " {cell:<width$} |")?;
        }
        println!("{line}");
    }
    println!("{sep}");
    let n = output.rows.len();
    println!("({n} row{})", if n == 1 { "" } else { "s" });
    Ok(())
}

fn build_separator(widths: &[usize]) -> String {
    let mut s = String::from("+");
    for width in widths {
        for _ in 0..width + 2 {
            s.push('-');
        }
        s.push('+');
    }
    s
}

fn split_statements(input: &str) -> Vec<&str> {
    let mut statements = Vec::new();
    let mut start = 0usize;
    let mut in_single_quote = false;
    let bytes = input.as_bytes();
    let mut idx = 0usize;

    while idx < bytes.len() {
        match bytes[idx] {
            b'\'' if !in_single_quote => {
                in_single_quote = true;
                idx += 1;
            }
            b'\'' if in_single_quote => {
                if idx + 1 < bytes.len() && bytes[idx + 1] == b'\'' {
                    idx += 2;
                } else {
                    in_single_quote = false;
                    idx += 1;
                }
            }
            b'-' if !in_single_quote && idx + 1 < bytes.len() && bytes[idx + 1] == b'-' => {
                while idx < bytes.len() && bytes[idx] != b'\n' {
                    idx += 1;
                }
            }
            b';' if !in_single_quote => {
                let stmt = input[start..idx].trim();
                if !stmt.is_empty() {
                    statements.push(stmt);
                }
                idx += 1;
                start = idx;
            }
            _ => idx += 1,
        }
    }

    let remainder = input[start..].trim();
    if !remainder.is_empty() {
        statements.push(remainder);
    }
    statements
}

#[cfg(test)]
mod tests {
    use super::*;
    use ultrasql_server::LocalResultColumn;

    #[test]
    fn split_statements_respects_quoted_semicolon() {
        assert_eq!(
            split_statements("SELECT ';'; SELECT 2"),
            vec!["SELECT ';'", "SELECT 2"]
        );
    }

    #[test]
    fn read_sql_prefers_query_then_file() {
        let _env_guard = local_env_test_lock();
        // SAFETY: local_env_test_lock serializes process-env mutation in this
        // module's tests.
        unsafe {
            std::env::remove_var("ULTRASQL_LOCAL_SQL_LIMIT_BYTES");
        }
        let query_cli = Cli {
            query: Some("SELECT 1".to_owned()),
            file: None,
        };
        assert_eq!(read_sql(&query_cli).expect("query sql"), "SELECT 1");

        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("script.sql");
        std::fs::write(&file, "SELECT 2").expect("write SQL file");
        let file_cli = Cli {
            query: None,
            file: Some(file),
        };
        assert_eq!(read_sql(&file_cli).expect("file sql"), "SELECT 2");
    }

    #[test]
    fn read_sql_rejects_oversized_file() {
        let _env_guard = local_env_test_lock();
        // SAFETY: local_env_test_lock serializes process-env mutation in this
        // module's tests.
        unsafe {
            std::env::set_var("ULTRASQL_LOCAL_SQL_LIMIT_BYTES", "3");
        }
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("script.sql");
        std::fs::write(&file, "SELECT 1").expect("write SQL file");
        let file_cli = Cli {
            query: None,
            file: Some(file),
        };

        let err = read_sql(&file_cli).expect_err("oversized SQL file rejected");

        assert!(err.to_string().contains("exceeds read limit"), "{err}");
        // SAFETY: local_env_test_lock serializes process-env mutation in this
        // module's tests.
        unsafe {
            std::env::remove_var("ULTRASQL_LOCAL_SQL_LIMIT_BYTES");
        }
    }

    fn local_env_test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        LOCK.lock().expect("local env test lock")
    }

    #[test]
    fn print_output_handles_command_empty_and_null_cells() {
        let command = LocalQueryOutput {
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: "CREATE TABLE".to_owned(),
        };
        print_output(&command).expect("print command tag");

        let empty_rows = LocalQueryOutput {
            columns: vec![LocalResultColumn {
                name: "id".to_owned(),
                type_oid: 23,
            }],
            rows: Vec::new(),
            command_tag: "SELECT 0".to_owned(),
        };
        print_output(&empty_rows).expect("print empty result");

        let rows = LocalQueryOutput {
            columns: vec![
                LocalResultColumn {
                    name: "id".to_owned(),
                    type_oid: 23,
                },
                LocalResultColumn {
                    name: "note".to_owned(),
                    type_oid: 25,
                },
            ],
            rows: vec![
                vec![Some("1".to_owned()), None],
                vec![Some("200".to_owned()), Some("ok".to_owned())],
            ],
            command_tag: "SELECT 2".to_owned(),
        };
        print_output(&rows).expect("print rows");
    }

    #[test]
    fn local_splitter_skips_semicolons_inside_line_comments() {
        assert_eq!(
            split_statements("SELECT 1 -- ; comment\n; SELECT 2"),
            vec!["SELECT 1 -- ; comment", "SELECT 2"]
        );
    }
}
