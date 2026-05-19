//! `ultrasql` — UltraSQL command-line client.
//!
//! Connects to an `ultrasqld` instance over the PostgreSQL wire protocol
//! and provides an interactive REPL plus a script-execution mode. Backslash
//! commands are compatible with a useful subset of psql.
//!
//! # Connection precedence
//!
//! 1. Explicit flags (`--host`, `--port`, `--user`, `--dbname`, `--password`).
//! 2. `postgresql://` URL supplied via `--url` or as the first positional.
//! 3. `PGHOST`, `PGPORT`, `PGUSER`, `PGDATABASE`, `PGPASSWORD` environment variables.
//! 4. `~/.pgpass` file (host:port:database:user:password lines, `*` wildcards).
//! 5. Built-in defaults: localhost:5432, username = current OS user, dbname = username.

use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use rustyline::DefaultEditor;
use tokio_postgres::{Client, Column, NoTls, Row};
use tracing_subscriber::EnvFilter;

// ---------------------------------------------------------------------------
// CLI argument definitions
// ---------------------------------------------------------------------------

/// UltraSQL command-line client — connects to ultrasqld or any PostgreSQL
/// compatible server.
#[derive(Debug, Parser)]
#[command(name = "ultrasql", about, version)]
struct Cli {
    /// Server hostname or IP address.
    #[arg(short = 'H', long, env = "PGHOST")]
    host: Option<String>,

    /// Server port number.
    #[arg(short, long, env = "PGPORT", value_parser = clap::value_parser!(u16))]
    port: Option<u16>,

    /// Database name to connect to.
    #[arg(short = 'd', long, env = "PGDATABASE")]
    dbname: Option<String>,

    /// Username to connect as.
    #[arg(short = 'U', long, env = "PGUSER")]
    username: Option<String>,

    /// Connection password (prefer PGPASSWORD env or ~/.pgpass over this flag).
    #[arg(short = 'W', long, env = "PGPASSWORD")]
    password: Option<String>,

    /// Full postgresql:// connection URL. Takes precedence over individual
    /// flags where both are provided.
    #[arg(long)]
    url: Option<String>,

    /// Execute a single SQL statement (or backslash command) and exit.
    #[arg(short = 'c', long, conflicts_with = "file")]
    command: Option<String>,

    /// Read SQL from `file` and execute, then exit.
    #[arg(short = 'f', long)]
    file: Option<PathBuf>,

    /// Check server readiness and exit like `pg_isready`.
    #[arg(long)]
    isready: bool,

    /// Optional HTTP ops endpoint for readiness, e.g. `127.0.0.1:8080`.
    #[arg(long, env = "ULTRASQL_OPS_ENDPOINT")]
    ops_endpoint: Option<String>,

    /// Dump a WAL segment or WAL file in a human-readable hex format.
    #[arg(long, value_name = "PATH")]
    waldump: Option<PathBuf>,

    /// Lightweight `pg_ctl`-style action.
    #[arg(long, value_enum)]
    ctl: Option<CtlCommand>,

    /// Copy a data directory into a base-backup directory and write a manifest.
    #[arg(long, value_name = "DEST")]
    basebackup: Option<PathBuf>,

    /// Archive one WAL file into this directory.
    #[arg(long, value_name = "WAL_PATH")]
    archive_wal: Option<PathBuf>,

    /// Restore one WAL filename from `--archive-dir` into this output path.
    #[arg(long, value_name = "WAL_NAME")]
    restore_wal: Option<String>,

    /// WAL archive directory used by `--archive-wal` and `--restore-wal`.
    #[arg(long, default_value = "target/ultrasql-archive")]
    archive_dir: PathBuf,

    /// Output path for `--restore-wal`.
    #[arg(long, value_name = "PATH")]
    restore_output: Option<PathBuf>,

    /// Recovery target time written by `--ctl recovery`.
    #[arg(long)]
    recovery_target_time: Option<String>,

    /// Recovery target LSN written by `--ctl recovery`.
    #[arg(long)]
    recovery_target_lsn: Option<String>,

    /// Recovery target XID written by `--ctl recovery`.
    #[arg(long)]
    recovery_target_xid: Option<String>,

    /// Data directory used by `--ctl initdb|status|promote`.
    #[arg(long, default_value = "target/ultrasql-data")]
    data_dir: PathBuf,

    /// Positional URL — postgresql:// or host shortcut.
    #[arg(hide = true)]
    positional_url: Option<String>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CtlCommand {
    Initdb,
    Start,
    Status,
    Reload,
    Promote,
    Standby,
    Recovery,
    Stop,
}

// ---------------------------------------------------------------------------
// Connection parameters
// ---------------------------------------------------------------------------

/// Resolved connection parameters after merging all sources.
#[derive(Debug, Clone)]
struct ConnParams {
    host: String,
    port: u16,
    dbname: String,
    user: String,
    password: Option<String>,
}

impl Default for ConnParams {
    fn default() -> Self {
        let user = std::env::var("USER")
            .or_else(|_| std::env::var("LOGNAME"))
            .unwrap_or_else(|_| "postgres".to_owned());
        Self {
            host: "localhost".to_owned(),
            port: 5432,
            dbname: user.clone(),
            user,
            password: None,
        }
    }
}

impl ConnParams {
    /// Parse a `postgresql://[user[:pass]@][host[:port]][/dbname]` URL into
    /// a partial set of overrides. Returns only the fields present in the URL.
    fn from_url(url: &str) -> Result<Self> {
        // Strip the scheme.
        let rest = url
            .strip_prefix("postgresql://")
            .or_else(|| url.strip_prefix("postgres://"))
            .context("URL must start with postgresql:// or postgres://")?;

        let mut params = Self::default();

        // Split off query string (ignored for now).
        let rest = rest.split('?').next().unwrap_or(rest);

        // Split off path (dbname).
        let (authority, path) = rest.find('/').map_or((rest, ""), |slash| {
            let (a, p) = rest.split_at(slash);
            (a, &p[1..]) // skip leading /
        });

        if !path.is_empty() {
            path.clone_into(&mut params.dbname);
        }

        // Split userinfo from host.
        let (userinfo, hostpart) = authority.rfind('@').map_or(("", authority), |at| {
            (&authority[..at], &authority[at + 1..])
        });

        if !userinfo.is_empty() {
            if let Some(colon) = userinfo.find(':') {
                userinfo[..colon].clone_into(&mut params.user);
                params.password = Some(userinfo[colon + 1..].to_owned());
            } else {
                userinfo.clone_into(&mut params.user);
            }
        }

        if !hostpart.is_empty() {
            if let Some(colon) = hostpart.rfind(':') {
                hostpart[..colon].clone_into(&mut params.host);
                params.port = hostpart[colon + 1..]
                    .parse::<u16>()
                    .context("invalid port in URL")?;
            } else {
                hostpart.clone_into(&mut params.host);
            }
        }

        Ok(params)
    }

    /// Apply overrides from another `ConnParams`, keeping `self`'s value
    /// only where `other` holds the default sentinel.
    fn merge_from(&mut self, other: &Self) {
        // Merge host if other differs from localhost (i.e. was explicitly set).
        if other.host != "localhost" {
            other.host.clone_into(&mut self.host);
        }
        if other.port != 5432 {
            self.port = other.port;
        }
        if other.dbname != other.user {
            other.dbname.clone_into(&mut self.dbname);
        }
        if other.user != "postgres" {
            other.user.clone_into(&mut self.user);
        }
        if other.password.is_some() {
            self.password.clone_from(&other.password);
        }
    }

    /// Apply individual overrides supplied as `Option<String>` values.
    fn apply_overrides(
        &mut self,
        host: Option<String>,
        port: Option<u16>,
        dbname: Option<String>,
        user: Option<String>,
        password: Option<String>,
    ) {
        if let Some(h) = host {
            self.host = h;
        }
        if let Some(p) = port {
            self.port = p;
        }
        if let Some(d) = dbname {
            self.dbname = d;
        }
        if let Some(u) = user {
            self.user = u;
        }
        if let Some(pw) = password {
            self.password = Some(pw);
        }
    }
}

// ---------------------------------------------------------------------------
// ~/.pgpass reader
// ---------------------------------------------------------------------------

/// Look up a password from `~/.pgpass`.
///
/// Each line has the form `host:port:database:user:password`. Wildcards
/// (`*`) match any value.
fn pgpass_lookup(host: &str, port: u16, dbname: &str, user: &str) -> Option<String> {
    let home = std::env::var("HOME").ok()?;
    let pgpass = PathBuf::from(home).join(".pgpass");
    let content = fs::read_to_string(&pgpass).ok()?;

    let port_str = port.to_string();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.splitn(5, ':').collect();
        if parts.len() != 5 {
            continue;
        }
        let matches = |pat: &str, val: &str| pat == "*" || pat == val;
        if matches(parts[0], host)
            && matches(parts[1], &port_str)
            && matches(parts[2], dbname)
            && matches(parts[3], user)
        {
            return Some(parts[4].to_owned());
        }
    }
    None
}

// ---------------------------------------------------------------------------
// REPL state
// ---------------------------------------------------------------------------

/// Per-session REPL state.
struct Session {
    client: Client,
    params: ConnParams,
    /// Whether we print timing for each query.
    timing: bool,
    /// Whether we print the number of rows affected.
    row_count: bool,
    /// Toggle for expanded (`\x`) display.
    expanded: bool,
    /// `\pset format` value. Today only `aligned` is honoured but
    /// the field is held for future formatting variants.
    format: String,
}

impl Session {
    fn new(client: Client, params: ConnParams) -> Self {
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
    async fn exec_sql(&self, sql: &str) -> Result<()> {
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
    async fn handle_meta(&mut self, cmd: &str) -> Result<bool> {
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
            // UltraSQL has no `pg_proc` yet (functions are deferred to
            // v0.8); emit an empty result with the standard column
            // headers so scripts that pipe through \df see the
            // expected shape.
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
            let content =
                fs::read_to_string(path).with_context(|| format!("cannot read file: {path}"))?;
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
    async fn exec_batch(&mut self, text: &str) -> Result<()> {
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
fn print_table(rows: &[Row]) -> Result<()> {
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
fn build_separator(widths: &[usize]) -> String {
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
fn cell_string(row: &Row, col: usize) -> String {
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
fn split_statements(input: &str) -> Vec<&str> {
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

fn list_tables_sql(pattern: &str) -> String {
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

fn describe_table_sql(table: &str) -> String {
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

fn list_indexes_sql(pattern: &str) -> String {
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

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Build the `tokio-postgres` connection string from resolved parameters.
fn build_conn_string(p: &ConnParams) -> String {
    let mut parts = vec![
        format!("host={}", p.host),
        format!("port={}", p.port),
        format!("dbname={}", p.dbname),
        format!("user={}", p.user),
    ];
    if let Some(pw) = &p.password {
        parts.push(format!("password={pw}"));
    }
    parts.join(" ")
}

/// Collect all connection parameters from the various sources.
fn resolve_params(cli: &Cli) -> Result<ConnParams> {
    // Start from defaults.
    let mut params = ConnParams::default();

    // URL from --url flag.
    if let Some(url) = &cli.url {
        let from_url = ConnParams::from_url(url)?;
        params.merge_from(&from_url);
    }

    // Positional URL argument.
    if let Some(pos) = &cli.positional_url {
        if pos.contains("://") {
            let from_url = ConnParams::from_url(pos)?;
            params.merge_from(&from_url);
        } else {
            // Treat as host shorthand.
            pos.clone_into(&mut params.host);
        }
    }

    // Individual CLI flags override URL.
    params.apply_overrides(
        cli.host.clone(),
        cli.port,
        cli.dbname.clone(),
        cli.username.clone(),
        cli.password.clone(),
    );

    // If still no password, try ~/.pgpass.
    if params.password.is_none() {
        params.password = pgpass_lookup(&params.host, params.port, &params.dbname, &params.user);
    }

    Ok(params)
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    // Initialise tracing from RUST_LOG (default: off).
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let cli = Cli::parse();

    match run(cli).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    let params = resolve_params(&cli)?;

    if cli.isready {
        run_isready(&params, cli.ops_endpoint.as_deref()).await?;
        return Ok(());
    }

    if let Some(path) = &cli.waldump {
        run_waldump(path)?;
        return Ok(());
    }

    if let Some(cmd) = cli.ctl {
        let targets = RecoveryTargets {
            time: cli.recovery_target_time.clone(),
            lsn: cli.recovery_target_lsn.clone(),
            xid: cli.recovery_target_xid.clone(),
        };
        run_ctl(
            cmd,
            &cli.data_dir,
            &params,
            cli.ops_endpoint.as_deref(),
            &targets,
        )
        .await?;
        return Ok(());
    }

    if let Some(dest) = &cli.basebackup {
        run_basebackup(&cli.data_dir, dest)?;
        return Ok(());
    }

    if let Some(wal_path) = &cli.archive_wal {
        run_archive_wal(wal_path, &cli.archive_dir)?;
        return Ok(());
    }

    if let Some(wal_name) = &cli.restore_wal {
        let output = cli
            .restore_output
            .as_ref()
            .context("--restore-wal requires --restore-output PATH")?;
        run_restore_wal(wal_name, &cli.archive_dir, output)?;
        return Ok(());
    }

    // Build connection string and connect.
    let conn_str = build_conn_string(&params);
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .with_context(|| {
            format!(
                "failed to connect to {}:{} as {}",
                params.host, params.port, params.user
            )
        })?;

    // Drive the connection on a background task.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("postgres connection error: {e}");
        }
    });

    let mut session = Session::new(client, params.clone());

    // -c / --command: execute one statement and exit.
    if let Some(cmd) = cli.command {
        session.exec_batch(&cmd).await?;
        return Ok(());
    }

    // -f / --file: execute from file and exit.
    if let Some(path) = cli.file {
        let content = fs::read_to_string(&path)
            .with_context(|| format!("cannot read file: {}", path.display()))?;
        session.exec_batch(&content).await?;
        return Ok(());
    }

    // Interactive REPL.
    run_repl(&mut session).await
}

/// Run the interactive REPL loop.
async fn run_repl(session: &mut Session) -> Result<()> {
    let mut rl = DefaultEditor::new().context("failed to initialise readline")?;

    // Load history from ~/.ultrasql_history if available.
    let history_path = history_path();
    if let Some(p) = &history_path {
        let _ = rl.load_history(p);
    }

    let p = &session.params;
    println!(
        "ultrasql {} — connected to {} as {} on {}:{} (type \\? for help, \\q to quit)",
        env!("CARGO_PKG_VERSION"),
        p.dbname,
        p.user,
        p.host,
        p.port
    );

    let mut buf = String::new();

    loop {
        let prompt = if buf.is_empty() { "=> " } else { "-> " };
        let line = match rl.readline(prompt) {
            Ok(l) => l,
            Err(rustyline::error::ReadlineError::Eof) => break,
            Err(rustyline::error::ReadlineError::Interrupted) => {
                buf.clear();
                continue;
            }
            Err(e) => return Err(e.into()),
        };

        let _ = rl.add_history_entry(&line);

        let trimmed = line.trim();

        // Backslash commands are dispatched immediately.
        if trimmed.starts_with('\\') {
            let quit = session.handle_meta(trimmed).await?;
            if quit {
                break;
            }
            buf.clear();
            continue;
        }

        // Accumulate into multi-line buffer.
        if !trimmed.is_empty() {
            if !buf.is_empty() {
                buf.push('\n');
            }
            buf.push_str(trimmed);
        }

        // Execute on semicolon or when the buffer ends with one.
        if buf.trim_end().ends_with(';') {
            let sql = std::mem::take(&mut buf);
            session.exec_sql(&sql).await?;
        }
    }

    // Save history.
    if let Some(p) = &history_path {
        let _ = rl.save_history(p);
    }

    println!("Bye!");
    Ok(())
}

/// Return the path to the readline history file, or `None` if HOME is not set.
fn history_path() -> Option<PathBuf> {
    let home = std::env::var("HOME").ok()?;
    Some(PathBuf::from(home).join(".ultrasql_history"))
}

async fn run_isready(params: &ConnParams, ops_endpoint: Option<&str>) -> Result<()> {
    if let Some(endpoint) = ops_endpoint {
        let ready = check_http_ready(endpoint).await?;
        if ready {
            println!("{endpoint} - accepting connections");
            return Ok(());
        }
        anyhow::bail!("{endpoint} - no response");
    }

    let addr = format!("{}:{}", params.host, params.port);
    tokio::net::TcpStream::connect(&addr)
        .await
        .with_context(|| format!("{addr} - no response"))?;
    println!("{addr} - accepting connections");
    Ok(())
}

async fn check_http_ready(endpoint: &str) -> Result<bool> {
    let endpoint = endpoint
        .strip_prefix("http://")
        .unwrap_or(endpoint)
        .trim_end_matches('/');
    let (host_port, path) = endpoint
        .split_once('/')
        .map_or((endpoint, "/ready"), |(host, path)| (host, path));
    let path = if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{path}")
    };
    let mut stream = tokio::net::TcpStream::connect(host_port)
        .await
        .with_context(|| format!("{host_port} - no response"))?;
    let request = format!(
        "GET {path} HTTP/1.1\r\nhost: {host_port}\r\nconnection: close\r\n\r\n"
    );
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    stream.write_all(request.as_bytes()).await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    Ok(response.starts_with(b"HTTP/1.1 200") || response.starts_with(b"HTTP/1.0 200"))
}

fn run_waldump(path: &PathBuf) -> Result<()> {
    let bytes = fs::read(path).with_context(|| format!("cannot read WAL file: {}", path.display()))?;
    println!("file: {}", path.display());
    println!("bytes: {}", bytes.len());
    for (offset, chunk) in bytes.chunks(32).enumerate() {
        let absolute = offset * 32;
        let hex = chunk
            .iter()
            .map(|b| format!("{:02x}", *b))
            .collect::<Vec<_>>()
            .join(" ");
        println!("{absolute:08x}: {hex}");
    }
    Ok(())
}

fn run_basebackup(data_dir: &PathBuf, dest: &PathBuf) -> Result<()> {
    if dest.exists() {
        anyhow::bail!("basebackup destination already exists: {}", dest.display());
    }
    fs::create_dir_all(dest)?;
    let mut manifest = Vec::new();
    copy_tree_with_manifest(data_dir, data_dir, dest, &mut manifest)?;
    manifest.sort_by(|a, b| a.0.cmp(&b.0));
    let mut text = String::from("{\n  \"files\": [\n");
    for (idx, (path, bytes, checksum)) in manifest.iter().enumerate() {
        let comma = if idx + 1 == manifest.len() { "" } else { "," };
        text.push_str(&format!(
            "    {{\"path\":\"{}\",\"bytes\":{},\"checksum\":\"{}\"}}{}\n",
            path.replace('\\', "\\\\").replace('"', "\\\""),
            bytes,
            checksum,
            comma
        ));
    }
    text.push_str("  ]\n}\n");
    fs::write(dest.join("backup_manifest.json"), text)?;
    println!("base backup copied {} files to {}", manifest.len(), dest.display());
    Ok(())
}

fn copy_tree_with_manifest(
    root: &PathBuf,
    current: &PathBuf,
    dest_root: &PathBuf,
    manifest: &mut Vec<(String, u64, String)>,
) -> Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        let rel = path.strip_prefix(root)?.to_path_buf();
        let dest = dest_root.join(&rel);
        if path.is_dir() {
            fs::create_dir_all(&dest)?;
            copy_tree_with_manifest(root, &path, dest_root, manifest)?;
        } else if path.is_file() {
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(&path, &dest)?;
            let bytes = fs::read(&path)?;
            let checksum = checksum_hex(&bytes);
            let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            manifest.push((rel.display().to_string(), len, checksum));
        }
    }
    Ok(())
}

fn checksum_hex(bytes: &[u8]) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn run_archive_wal(wal_path: &PathBuf, archive_dir: &PathBuf) -> Result<()> {
    fs::create_dir_all(archive_dir)?;
    let name = wal_path
        .file_name()
        .context("WAL path must have a filename")?;
    let dest = archive_dir.join(name);
    fs::copy(wal_path, &dest)?;
    println!("archived {} to {}", wal_path.display(), dest.display());
    Ok(())
}

fn run_restore_wal(wal_name: &str, archive_dir: &PathBuf, output: &PathBuf) -> Result<()> {
    let source = archive_dir.join(wal_name);
    fs::copy(&source, output)
        .with_context(|| format!("restore WAL {} to {}", source.display(), output.display()))?;
    println!("restored {} to {}", source.display(), output.display());
    Ok(())
}

async fn run_ctl(
    cmd: CtlCommand,
    data_dir: &PathBuf,
    params: &ConnParams,
    ops_endpoint: Option<&str>,
    targets: &RecoveryTargets,
) -> Result<()> {
    match cmd {
        CtlCommand::Initdb => {
            fs::create_dir_all(data_dir.join("base"))?;
            fs::create_dir_all(data_dir.join("pg_wal"))?;
            fs::create_dir_all(data_dir.join("global"))?;
            fs::write(
                data_dir.join("ultrasql.control"),
                format!("version={}\nstate=initialized\n", env!("CARGO_PKG_VERSION")),
            )?;
            println!("initialized UltraSQL data directory at {}", data_dir.display());
        }
        CtlCommand::Start => {
            println!(
                "start command: ultrasqld --data-dir {} --listen {}:{}",
                data_dir.display(),
                params.host,
                params.port
            );
        }
        CtlCommand::Status => {
            run_isready(params, ops_endpoint).await?;
        }
        CtlCommand::Reload => {
            println!("reload requested; send SIGHUP to ultrasqld process manager");
        }
        CtlCommand::Promote => {
            fs::write(data_dir.join("promote.signal"), b"promote\n")?;
            println!("created {}", data_dir.join("promote.signal").display());
        }
        CtlCommand::Standby => {
            fs::create_dir_all(data_dir)?;
            fs::write(data_dir.join("standby.signal"), b"standby\n")?;
            println!("created {}", data_dir.join("standby.signal").display());
        }
        CtlCommand::Recovery => {
            fs::create_dir_all(data_dir)?;
            fs::write(data_dir.join("recovery.signal"), b"recovery\n")?;
            let mut conf = String::new();
            if let Some(value) = &targets.time {
                conf.push_str(&format!("recovery_target_time = '{}'\n", escape_conf(value)));
            }
            if let Some(value) = &targets.lsn {
                conf.push_str(&format!("recovery_target_lsn = '{}'\n", escape_conf(value)));
            }
            if let Some(value) = &targets.xid {
                conf.push_str(&format!("recovery_target_xid = '{}'\n", escape_conf(value)));
            }
            fs::write(data_dir.join("recovery.targets"), conf)?;
            println!("created {}", data_dir.join("recovery.signal").display());
        }
        CtlCommand::Stop => {
            println!("stop requested; send SIGTERM through service manager");
        }
    }
    Ok(())
}

#[derive(Debug)]
struct RecoveryTargets {
    time: Option<String>,
    lsn: Option<String>,
    xid: Option<String>,
}

fn escape_conf(value: &str) -> String {
    value.replace('\'', "''")
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- URL parsing ---

    #[test]
    fn url_full_parse() {
        let p = ConnParams::from_url("postgresql://alice:s3cr3t@db.example.com:5433/mydb")
            .expect("valid URL");
        assert_eq!(p.host, "db.example.com");
        assert_eq!(p.port, 5433);
        assert_eq!(p.user, "alice");
        assert_eq!(p.password.as_deref(), Some("s3cr3t"));
        assert_eq!(p.dbname, "mydb");
    }

    #[test]
    fn url_minimal_parse() {
        let p = ConnParams::from_url("postgres://localhost/testdb").expect("valid URL");
        assert_eq!(p.host, "localhost");
        assert_eq!(p.dbname, "testdb");
        assert!(p.password.is_none());
    }

    #[test]
    fn url_without_path_uses_default_dbname() {
        // No path component — dbname stays as whatever the default was.
        let p = ConnParams::from_url("postgresql://myhost:5432").expect("valid URL");
        assert_eq!(p.host, "myhost");
        assert_eq!(p.port, 5432);
    }

    #[test]
    fn url_invalid_scheme_rejects() {
        let err = ConnParams::from_url("mysql://localhost/db");
        assert!(err.is_err(), "non-pg URL must fail");
    }

    // --- ~/.pgpass ---

    #[test]
    fn pgpass_wildcard_host_matches() {
        // Build a temp pgpass file.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(".pgpass");
        fs::write(&path, "*:5432:mydb:bob:hunter2\n").expect("write");

        // Point HOME at our temp dir.
        let old_home = std::env::var("HOME");
        // SAFETY: modifying env is not thread-safe in general, but this test
        // is single-threaded within its own process image and the change is
        // reverted before the function returns.
        unsafe {
            std::env::set_var("HOME", dir.path());
        }

        let pw = pgpass_lookup("anyhost", 5432, "mydb", "bob");
        assert_eq!(pw.as_deref(), Some("hunter2"));

        unsafe {
            match old_home {
                Ok(v) => std::env::set_var("HOME", v),
                Err(_) => std::env::remove_var("HOME"),
            }
        }
    }

    #[test]
    fn pgpass_wrong_user_no_match() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(".pgpass");
        fs::write(&path, "localhost:5432:mydb:alice:pw\n").expect("write");

        let old_home = std::env::var("HOME");
        unsafe {
            std::env::set_var("HOME", dir.path());
        }

        let pw = pgpass_lookup("localhost", 5432, "mydb", "bob");
        assert!(pw.is_none(), "wrong user must not match");

        unsafe {
            match old_home {
                Ok(v) => std::env::set_var("HOME", v),
                Err(_) => std::env::remove_var("HOME"),
            }
        }
    }

    // --- Statement splitter ---

    #[test]
    fn split_single_stmt() {
        let stmts = split_statements("SELECT 1;");
        assert_eq!(stmts, vec!["SELECT 1"]);
    }

    #[test]
    fn split_multiple_stmts() {
        let stmts = split_statements("SELECT 1; SELECT 2; SELECT 3;");
        assert_eq!(stmts, vec!["SELECT 1", "SELECT 2", "SELECT 3"]);
    }

    #[test]
    fn split_respects_quoted_semicolon() {
        let stmts = split_statements("SELECT ';' AS c;");
        assert_eq!(stmts, vec!["SELECT ';' AS c"]);
    }

    #[test]
    fn split_comment_skipped_for_semicolon_detection() {
        // The splitter skips `--` comments when searching for `;`, so the
        // semicolon on the next line terminates the statement. The comment
        // text is retained in the slice (the SQL engine will ignore it).
        let stmts = split_statements("SELECT 1 -- comment\n;");
        assert_eq!(stmts, vec!["SELECT 1 -- comment"]);
    }

    #[test]
    fn split_no_trailing_semicolon() {
        let stmts = split_statements("SELECT 1");
        assert_eq!(stmts, vec!["SELECT 1"]);
    }

    // --- Formatting helpers ---

    #[test]
    fn build_separator_correct_width() {
        let sep = build_separator(&[3, 5]);
        // Each column: width + 2 spaces + border
        // "+-----+-------+"
        assert_eq!(sep, "+-----+-------+");
    }

    // --- pgpass lookup with missing file ---

    #[test]
    fn pgpass_missing_file_returns_none() {
        let dir = tempfile::tempdir().expect("tempdir");
        // No .pgpass file in dir.
        let old_home = std::env::var("HOME");
        unsafe {
            std::env::set_var("HOME", dir.path());
        }
        let pw = pgpass_lookup("localhost", 5432, "db", "user");
        assert!(pw.is_none());
        unsafe {
            match old_home {
                Ok(v) => std::env::set_var("HOME", v),
                Err(_) => std::env::remove_var("HOME"),
            }
        }
    }
}
