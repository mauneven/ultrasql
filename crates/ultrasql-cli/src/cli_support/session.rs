//! Interactive REPL session state, result formatting, statement splitting,
//! and catalog meta-query builders.

use std::fmt::Write as _;
use std::path::Path;

use anyhow::Result;
use tokio_postgres::{Client, Column, Row};

use super::cli_args::ConnParams;
use super::fileio::read_sql_script_file;

// ---------------------------------------------------------------------------
// REPL state
// ---------------------------------------------------------------------------

/// Per-session REPL state.
pub(crate) struct Session {
    pub(crate) client: Client,
    pub(crate) params: ConnParams,
    /// Whether we print timing for each query.
    pub(crate) timing: bool,
    /// Whether we print the number of rows affected.
    pub(crate) row_count: bool,
    /// Toggle for expanded (`\x`) display.
    pub(crate) expanded: bool,
    /// `\pset format` value. Today only `aligned` is honoured but
    /// the field is held for future formatting variants.
    pub(crate) format: String,
}

impl Session {
    pub(crate) fn new(client: Client, params: ConnParams) -> Self {
        Self {
            client,
            params,
            timing: false,
            row_count: true,
            expanded: false,
            format: "aligned".to_string(),
        }
    }

    /// Execute a SQL statement and print the result table.
    pub(crate) async fn exec_sql(&self, sql: &str) -> Result<()> {
        let start = std::time::Instant::now();
        let rows = self.client.query(sql, &[]).await;
        match rows {
            Ok(rows) => {
                if rows.is_empty() {
                    if self.row_count {
                        println!("(0 rows)");
                    }
                } else {
                    print_table(&rows)?;
                    if self.row_count {
                        let n = rows.len();
                        println!("({n} row{})", if n == 1 { "" } else { "s" });
                    }
                }
                if self.timing {
                    println!("Time: {:.3} ms", start.elapsed().as_secs_f64() * 1000.0);
                }
            }
            Err(e) => {
                eprintln!("ERROR: {e}");
            }
        }
        Ok(())
    }

    /// Handle a backslash command. Returns `true` if the session should exit.
    pub(crate) async fn handle_meta(&mut self, cmd: &str) -> Result<bool> {
        let cmd = cmd.trim();
        if cmd == "\\q" || cmd == "\\quit" {
            return Ok(true);
        }
        if cmd == "\\?" || cmd == "\\h" {
            println!("{META_HELP}");
            return Ok(false);
        }
        if cmd == "\\timing" {
            self.timing = !self.timing;
            println!("Timing is {}.", if self.timing { "on" } else { "off" });
            return Ok(false);
        }
        if cmd == "\\conninfo" {
            let p = &self.params;
            println!(
                "You are connected to database \"{}\" as user \"{}\" on host \"{}\" (port {}).",
                p.dbname, p.user, p.host, p.port
            );
            return Ok(false);
        }
        if cmd == "\\dt" || cmd.starts_with("\\dt ") {
            let pattern = cmd.strip_prefix("\\dt").unwrap_or("").trim();
            let sql = list_tables_sql(pattern);
            self.exec_sql(&sql).await?;
            return Ok(false);
        }
        if cmd == "\\d" || cmd.starts_with("\\d ") {
            let table = cmd.strip_prefix("\\d").unwrap_or("").trim();
            if table.is_empty() {
                // List all tables/views/sequences.
                self.exec_sql(LIST_OBJECTS_SQL).await?;
            } else {
                // Describe specific table.
                let sql = describe_table_sql(table);
                self.exec_sql(&sql).await?;
            }
            return Ok(false);
        }
        if cmd == "\\di" || cmd.starts_with("\\di ") {
            let pattern = cmd.strip_prefix("\\di").unwrap_or("").trim();
            let sql = list_indexes_sql(pattern);
            self.exec_sql(&sql).await?;
            return Ok(false);
        }
        if cmd == "\\dn" || cmd.starts_with("\\dn ") {
            self.exec_sql(LIST_SCHEMAS_SQL).await?;
            return Ok(false);
        }
        if cmd == "\\l" || cmd.starts_with("\\l ") {
            self.exec_sql(LIST_DATABASES_SQL).await?;
            return Ok(false);
        }
        if cmd == "\\du" || cmd.starts_with("\\du ") {
            self.exec_sql(LIST_ROLES_SQL).await?;
            return Ok(false);
        }
        if cmd == "\\df" || cmd.starts_with("\\df ") {
            self.exec_sql(LIST_FUNCTIONS_SQL).await?;
            return Ok(false);
        }
        if cmd == "\\dv" || cmd.starts_with("\\dv ") {
            self.exec_sql(LIST_VIEWS_SQL).await?;
            return Ok(false);
        }
        if cmd == "\\ds" || cmd.starts_with("\\ds ") {
            self.exec_sql(LIST_SEQUENCES_SQL).await?;
            return Ok(false);
        }
        if cmd == "\\x" || cmd.starts_with("\\x ") {
            // Toggle expanded output mode. PostgreSQL's psql also
            // accepts an explicit `on` / `off` argument; we match.
            let arg = cmd.strip_prefix("\\x").unwrap_or("").trim();
            self.expanded = match arg {
                "on" => true,
                "off" => false,
                "" => !self.expanded,
                other => {
                    eprintln!("\\x: invalid argument '{other}', expected 'on' or 'off'");
                    return Ok(false);
                }
            };
            println!(
                "Expanded display is {}.",
                if self.expanded { "on" } else { "off" }
            );
            return Ok(false);
        }
        if cmd == "\\pset" || cmd.starts_with("\\pset ") {
            // PostgreSQL psql's \pset takes a `name [value]` pair and
            // tweaks one of dozens of output settings. UltraSQL today
            // supports only `expanded` and `format`; everything else
            // is acknowledged with a notice so scripts do not abort.
            let args = cmd.strip_prefix("\\pset").unwrap_or("").trim();
            let mut parts = args.splitn(2, char::is_whitespace);
            let key = parts.next().unwrap_or("");
            let value = parts.next().map(str::trim).unwrap_or("");
            match key {
                "" => println!("\\pset: expanded={}", self.expanded),
                "expanded" => {
                    self.expanded = matches!(value, "on" | "1" | "true" | "");
                    println!(
                        "Expanded display is {}.",
                        if self.expanded { "on" } else { "off" }
                    );
                }
                "format" => {
                    // Only `aligned` is meaningful in v0.5; unaligned /
                    // wrapped / html / csv are accepted but treated as
                    // aligned. The setting is held in `format` for
                    // future use.
                    self.format = value.to_string();
                    println!("Output format is \"{}\".", self.format);
                }
                other => {
                    println!("\\pset: option '{other}' not yet supported; ignored.");
                }
            }
            return Ok(false);
        }
        if cmd == "\\c" || cmd.starts_with("\\c ") {
            // \c [dbname [user [host [port]]]] — psql's reconnect.
            // Implementing the reconnect requires tearing down the
            // tokio-postgres client and rebuilding it from
            // ConnParams; for v0.5 we acknowledge the command but
            // stay on the existing connection so scripts that issue
            // \c against the same dbname do not abort.
            let args = cmd.strip_prefix("\\c").unwrap_or("").trim();
            if args.is_empty() {
                let p = &self.params;
                println!(
                    "You are connected to database \"{}\" as user \"{}\" on host \"{}\" (port {}).",
                    p.dbname, p.user, p.host, p.port
                );
            } else {
                println!(
                    "\\c: reconnection across sessions is deferred; staying on the current connection."
                );
            }
            return Ok(false);
        }
        if cmd.starts_with("\\i ") {
            let path = cmd.strip_prefix("\\i ").unwrap_or("").trim();
            let content = read_sql_script_file(Path::new(path))?;
            // Execute each SQL statement from the file directly (no meta-
            // recursion: \i inside an \i file is not supported).
            for stmt in split_statements(&content) {
                let stmt = stmt.trim();
                if stmt.is_empty() || stmt.starts_with('\\') {
                    continue;
                }
                self.exec_sql(stmt).await?;
            }
            return Ok(false);
        }
        // Unknown meta command — warn rather than silently drop.
        eprintln!("Unknown command: {cmd}. Try \\? for help.");
        Ok(false)
    }

    /// Execute multiple semicolon-separated statements (or meta commands).
    pub(crate) async fn exec_batch(&mut self, text: &str) -> Result<()> {
        for stmt in split_statements(text) {
            let stmt = stmt.trim();
            if stmt.is_empty() {
                continue;
            }
            if stmt.starts_with('\\') {
                let quit = self.handle_meta(stmt).await?;
                if quit {
                    break;
                }
            } else {
                self.exec_sql(stmt).await?;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Result formatting
// ---------------------------------------------------------------------------

/// Print a table of rows to stdout using the psql box-drawing style.
pub(crate) fn print_table(rows: &[Row]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let cols: Vec<&Column> = rows[0].columns().iter().collect();
    let headers: Vec<&str> = cols.iter().map(|c| c.name()).collect();

    // Stringify all cells.
    let cells: Vec<Vec<String>> = rows
        .iter()
        .map(|row| (0..cols.len()).map(|i| cell_string(row, i)).collect())
        .collect();

    // Column widths = max(header_len, max_cell_len).
    let widths: Vec<usize> = headers
        .iter()
        .enumerate()
        .map(|(i, h)| {
            let max_cell = cells.iter().map(|r| r[i].len()).max().unwrap_or(0);
            h.len().max(max_cell)
        })
        .collect();

    let sep = build_separator(&widths);
    println!("{sep}");

    // Header row.
    let mut header_row = String::from("|");
    for (h, w) in headers.iter().zip(&widths) {
        write!(header_row, " {h:<w$} |")?;
    }
    println!("{header_row}");
    println!("{sep}");

    // Data rows.
    for row_cells in &cells {
        let mut line = String::from("|");
        for (cell, w) in row_cells.iter().zip(&widths) {
            write!(line, " {cell:<w$} |")?;
        }
        println!("{line}");
    }
    println!("{sep}");

    Ok(())
}

/// Build a `+---+---+` separator.
pub(crate) fn build_separator(widths: &[usize]) -> String {
    let mut s = String::from("+");
    for w in widths {
        for _ in 0..w + 2 {
            s.push('-');
        }
        s.push('+');
    }
    s
}

/// Extract a cell as a displayable string from a tokio-postgres `Row`.
pub(crate) fn cell_string(row: &Row, col: usize) -> String {
    // Try the common types. tokio-postgres uses the Rust type system for
    // decoding; we attempt each type and fall back to NULL display.
    if let Ok(v) = row.try_get::<_, Option<String>>(col) {
        return v.unwrap_or_else(|| "NULL".to_owned());
    }
    if let Ok(v) = row.try_get::<_, Option<i64>>(col) {
        return v.map_or_else(|| "NULL".to_owned(), |n| n.to_string());
    }
    if let Ok(v) = row.try_get::<_, Option<i32>>(col) {
        return v.map_or_else(|| "NULL".to_owned(), |n| n.to_string());
    }
    if let Ok(v) = row.try_get::<_, Option<i16>>(col) {
        return v.map_or_else(|| "NULL".to_owned(), |n| n.to_string());
    }
    if let Ok(v) = row.try_get::<_, Option<bool>>(col) {
        return v.map_or_else(|| "NULL".to_owned(), |b| b.to_string());
    }
    if let Ok(v) = row.try_get::<_, Option<f64>>(col) {
        return v.map_or_else(|| "NULL".to_owned(), |f| f.to_string());
    }
    if let Ok(v) = row.try_get::<_, Option<f32>>(col) {
        return v.map_or_else(|| "NULL".to_owned(), |f| f.to_string());
    }
    // Fallback.
    "?".to_owned()
}

// ---------------------------------------------------------------------------
// Statement splitter
// ---------------------------------------------------------------------------

/// Split a SQL text buffer into individual statements on `;` boundaries,
/// respecting single-quoted strings and `--` line comments.
pub(crate) fn split_statements(input: &str) -> Vec<&str> {
    let mut statements = Vec::new();
    let mut start = 0usize;
    let mut in_single_quote = false;
    let bytes = input.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' if !in_single_quote => {
                in_single_quote = true;
                i += 1;
            }
            b'\'' if in_single_quote => {
                // Handle doubled single-quote escape.
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                } else {
                    in_single_quote = false;
                    i += 1;
                }
            }
            b'-' if !in_single_quote && i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                // Skip to end of line.
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b';' if !in_single_quote => {
                let stmt = input[start..i].trim();
                if !stmt.is_empty() {
                    statements.push(stmt);
                }
                i += 1;
                start = i;
            }
            _ => {
                i += 1;
            }
        }
    }

    // Trailing statement without semicolon.
    let remainder = input[start..].trim();
    if !remainder.is_empty() {
        statements.push(remainder);
    }

    statements
}

// ---------------------------------------------------------------------------
// Catalog meta-queries
// ---------------------------------------------------------------------------

const LIST_OBJECTS_SQL: &str = "
SELECT schemaname, tablename AS name, 'table' AS type
FROM   pg_catalog.pg_tables
WHERE  schemaname NOT IN ('pg_catalog','information_schema')
UNION ALL
SELECT schemaname, viewname AS name, 'view' AS type
FROM   pg_catalog.pg_views
WHERE  schemaname NOT IN ('pg_catalog','information_schema')
ORDER  BY 1, 2;
";

const LIST_SCHEMAS_SQL: &str = "
SELECT nspname AS name, pg_catalog.pg_get_userbyid(nspowner) AS owner
FROM   pg_catalog.pg_namespace
ORDER  BY 1;
";

const LIST_DATABASES_SQL: &str = "
SELECT datname AS name, pg_catalog.pg_get_userbyid(datdba) AS owner
FROM   pg_catalog.pg_database
ORDER  BY 1;
";

const LIST_ROLES_SQL: &str = "
SELECT rolname AS role, rolsuper AS superuser, rolcreatedb AS create_db
FROM   pg_catalog.pg_roles
ORDER  BY 1;
";

const LIST_FUNCTIONS_SQL: &str = "
SELECT n.nspname AS schemaname, p.proname AS name
FROM   pg_catalog.pg_proc p
JOIN   pg_catalog.pg_namespace n ON p.pronamespace = n.oid
WHERE  n.nspname NOT IN ('pg_catalog','information_schema')
ORDER  BY 1, 2;
";

const LIST_VIEWS_SQL: &str = "
SELECT schemaname, viewname AS name
FROM   pg_catalog.pg_views
WHERE  schemaname NOT IN ('pg_catalog','information_schema')
ORDER  BY 1, 2;
";

const LIST_SEQUENCES_SQL: &str = "
SELECT schemaname, sequencename AS name
FROM   pg_catalog.pg_sequences
WHERE  schemaname NOT IN ('pg_catalog','information_schema')
ORDER  BY 1, 2;
";

pub(crate) fn list_tables_sql(pattern: &str) -> String {
    if pattern.is_empty() {
        "SELECT schemaname, tablename FROM pg_catalog.pg_tables \
         WHERE schemaname NOT IN ('pg_catalog','information_schema') ORDER BY 1,2;"
            .to_owned()
    } else {
        // Sanitise: only allow identifier chars and wildcards.
        let safe: String = pattern
            .chars()
            .filter(|c| c.is_alphanumeric() || matches!(*c, '_' | '%' | '.'))
            .collect();
        format!(
            "SELECT schemaname, tablename FROM pg_catalog.pg_tables \
             WHERE tablename LIKE '{safe}' ORDER BY 1,2;"
        )
    }
}

pub(crate) fn describe_table_sql(table: &str) -> String {
    // Sanitise the table name.
    let safe: String = table
        .chars()
        .filter(|c| c.is_alphanumeric() || matches!(*c, '_' | '.'))
        .collect();
    format!(
        "SELECT column_name, data_type, character_maximum_length, \
                is_nullable, column_default \
         FROM   information_schema.columns \
         WHERE  table_name = '{safe}' \
         ORDER  BY ordinal_position;"
    )
}

pub(crate) fn list_indexes_sql(pattern: &str) -> String {
    if pattern.is_empty() {
        "SELECT schemaname, tablename, indexname FROM pg_catalog.pg_indexes \
         WHERE schemaname NOT IN ('pg_catalog','information_schema') ORDER BY 1,2,3;"
            .to_owned()
    } else {
        let safe: String = pattern
            .chars()
            .filter(|c| c.is_alphanumeric() || matches!(*c, '_' | '%'))
            .collect();
        format!(
            "SELECT schemaname, tablename, indexname FROM pg_catalog.pg_indexes \
             WHERE indexname LIKE '{safe}' ORDER BY 1,2,3;"
        )
    }
}

// ---------------------------------------------------------------------------
// Help text
// ---------------------------------------------------------------------------

const META_HELP: &str = "\
General
  \\q, \\quit         Quit the client
  \\? , \\h           Show this help

Informational
  \\d  [table]       Describe table or list all objects
  \\dt [pattern]     List tables
  \\di [pattern]     List indexes
  \\dv [pattern]     List views
  \\ds [pattern]     List sequences
  \\df [pattern]     List functions
  \\dn               List schemas
  \\du               List roles
  \\l                List databases

Connection
  \\conninfo         Display current connection info
  \\c  [dbname]      Reconnect (acknowledged; cross-session reconnect deferred)

Formatting
  \\timing           Toggle query timing display
  \\x   [on|off]     Toggle expanded display
  \\pset name [val]  Set / show formatting option (expanded, format)

Input
  \\i file           Execute SQL from file";
