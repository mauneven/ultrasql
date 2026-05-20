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
    if let Some(file) = &cli.file {
        return std::fs::read_to_string(file)
            .with_context(|| format!("cannot read SQL file: {}", file.display()));
    }
    let mut sql = String::new();
    std::io::stdin()
        .read_to_string(&mut sql)
        .context("cannot read SQL from stdin")?;
    Ok(sql)
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
    use super::split_statements;

    #[test]
    fn split_statements_respects_quoted_semicolon() {
        assert_eq!(
            split_statements("SELECT ';'; SELECT 2"),
            vec!["SELECT ';'", "SELECT 2"]
        );
    }
}
