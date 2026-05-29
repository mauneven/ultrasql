//! `ultrasql` — UltraSQL command-line client.
//!
//! Connects to an `ultrasqld` instance over the PostgreSQL wire protocol
//! and provides an interactive REPL plus a script-execution mode. Backslash
//! commands cover a useful subset of psql.
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
use std::path::{Component, Path, PathBuf};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use rustyline::DefaultEditor;
use tokio_postgres::{Client, Column, NoTls, Row};
use tracing_subscriber::EnvFilter;
use ultrasql_server::replication::{WalReceiver, WalSender};
use ultrasql_server::{Server, ValidationReport};

// ---------------------------------------------------------------------------
// CLI argument definitions
// ---------------------------------------------------------------------------

/// UltraSQL command-line client — connects to ultrasqld or any PostgreSQL
/// UltraSQL server.
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

    /// Write a pg_dump-style UltraSQL archive from `--data-dir`.
    #[arg(long, value_name = "DEST")]
    pg_dump: Option<PathBuf>,

    /// Dump archive format for `--pg-dump`.
    #[arg(long, value_enum, default_value = "custom")]
    dump_format: DumpFormat,

    /// Restore a `--pg-dump` archive or directory into `--data-dir`.
    #[arg(long, value_name = "SOURCE")]
    pg_restore: Option<PathBuf>,

    /// Archive one WAL file into this directory.
    #[arg(long, value_name = "WAL_PATH")]
    archive_wal: Option<PathBuf>,

    /// Restore one WAL filename from `--archive-dir` into this output path.
    #[arg(long, value_name = "WAL_NAME")]
    restore_wal: Option<String>,

    /// Ship archived WAL files once from `--archive-dir` into this directory.
    #[arg(long, value_name = "DEST")]
    wal_send_once: Option<PathBuf>,

    /// Repeat `--wal-send-once` every N milliseconds. Zero means run once.
    #[arg(long, default_value_t = 0)]
    wal_send_interval_ms: u64,

    /// Receive shipped WAL files once from this source directory into `--data-dir/pg_wal`.
    #[arg(long, value_name = "SOURCE")]
    wal_receive_once: Option<PathBuf>,

    /// Repeat `--wal-receive-once` every N milliseconds. Zero means run once.
    #[arg(long, default_value_t = 0)]
    wal_receive_interval_ms: u64,

    /// Also copy received WAL into this archive directory so this standby can
    /// cascade physical WAL to downstream receivers.
    #[arg(long, value_name = "DIR")]
    wal_receive_cascade_archive: Option<PathBuf>,

    /// Replication slot name used by WAL sender.
    #[arg(long, default_value = "standby")]
    replication_slot: String,

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

    /// Admin subcommand.
    #[command(subcommand)]
    subcommand: Option<CliSubcommand>,

    /// Positional URL — postgresql:// or host shortcut.
    #[arg(hide = true)]
    positional_url: Option<String>,
}

#[derive(Clone, Copy, Debug, Subcommand)]
enum CliSubcommand {
    /// Validate catalog, indexes, WAL, heap visibility, and ANN tombstones.
    Validate,
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

#[derive(Clone, Copy, Debug, ValueEnum)]
enum DumpFormat {
    Plain,
    Directory,
    Custom,
    Tar,
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
    pgpass_lookup_in_home(&PathBuf::from(home), host, port, dbname, user)
}

fn pgpass_lookup_in_home(
    home: &std::path::Path,
    host: &str,
    port: u16,
    dbname: &str,
    user: &str,
) -> Option<String> {
    let pgpass = home.join(".pgpass");
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
    if matches!(cli.subcommand, Some(CliSubcommand::Validate)) {
        run_validate(&cli.data_dir)?;
        return Ok(());
    }

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
        run_basebackup(&cli.data_dir, dest, cli.ops_endpoint.as_deref()).await?;
        return Ok(());
    }

    if let Some(dest) = &cli.pg_dump {
        run_pg_dump(&cli.data_dir, dest, cli.dump_format)?;
        return Ok(());
    }

    if let Some(source) = &cli.pg_restore {
        run_pg_restore(source, &cli.data_dir)?;
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

    if let Some(dest) = &cli.wal_send_once {
        let slots_dir = cli.data_dir.join("pg_replslot");
        let sender = WalSender::new(&cli.archive_dir, slots_dir)?;
        if cli.wal_send_interval_ms == 0 {
            let copied = sender.send_once(&cli.replication_slot, dest)?;
            println!("sent {copied} WAL file(s) to {}", dest.display());
        } else {
            run_wal_send_loop(
                &sender,
                &cli.replication_slot,
                dest,
                cli.wal_send_interval_ms,
            )?;
        }
        return Ok(());
    }

    if let Some(source) = &cli.wal_receive_once {
        let receiver = WalReceiver::new(source);
        let wal_dir = cli.data_dir.join("pg_wal");
        if cli.wal_receive_interval_ms == 0 {
            let copied = receive_wal_once(
                &receiver,
                &wal_dir,
                cli.wal_receive_cascade_archive.as_deref(),
            )?;
            fs::write(cli.data_dir.join("standby.signal"), b"standby\n")?;
            println!("received {copied} WAL file(s) into {}", wal_dir.display());
        } else {
            run_wal_receive_loop(
                &receiver,
                &cli.data_dir,
                cli.wal_receive_cascade_archive.as_deref(),
                cli.wal_receive_interval_ms,
            )?;
        }
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
    Ok(http_get_ops_endpoint(endpoint, "/ready").await?.ok)
}

struct OpsHttpResponse {
    ok: bool,
    body: String,
}

async fn http_get_ops_endpoint(endpoint: &str, path: &str) -> Result<OpsHttpResponse> {
    let endpoint = endpoint
        .strip_prefix("http://")
        .unwrap_or(endpoint)
        .trim_end_matches('/');
    let host_port = endpoint
        .split_once('/')
        .map_or(endpoint, |(host, _path)| host);
    let path = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    let mut stream = tokio::net::TcpStream::connect(host_port)
        .await
        .with_context(|| format!("{host_port} - no response"))?;
    let request = format!("GET {path} HTTP/1.1\r\nhost: {host_port}\r\nconnection: close\r\n\r\n");
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    stream.write_all(request.as_bytes()).await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let ok = response.starts_with(b"HTTP/1.1 200") || response.starts_with(b"HTTP/1.0 200");
    let body = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map_or(&response[..], |idx| &response[idx + 4..]);
    Ok(OpsHttpResponse {
        ok,
        body: String::from_utf8_lossy(body).into_owned(),
    })
}

fn run_waldump(path: &PathBuf) -> Result<()> {
    let bytes =
        fs::read(path).with_context(|| format!("cannot read WAL file: {}", path.display()))?;
    println!("file: {}", path.display());
    println!("bytes: {}", bytes.len());
    println!("records:");
    for line in waldump_record_lines(&bytes) {
        println!("{line}");
    }
    println!("hex:");
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

fn waldump_record_lines(bytes: &[u8]) -> Vec<String> {
    let mut lines = Vec::new();
    let mut offset = 0_usize;
    while offset < bytes.len() {
        match ultrasql_wal::WalRecord::decode(&bytes[offset..]) {
            Ok((record, used)) => {
                let decoded = decode_wal_payload(&record);
                lines.push(format!(
                    "{offset:08x}: type={:?} xid={:?} prev_lsn={:?} flags={} len={} payload_len={} {decoded}",
                    record.header.record_type,
                    record.header.xid,
                    record.header.prev_lsn,
                    record.header.flags,
                    record.header.total_length,
                    record.payload.len()
                ));
                offset = offset.saturating_add(used);
            }
            Err(err) => {
                lines.push(format!("{offset:08x}: record_error={err}"));
                break;
            }
        }
    }
    if lines.is_empty() {
        lines.push("00000000: empty".to_string());
    }
    lines
}

fn decode_wal_payload(record: &ultrasql_wal::WalRecord) -> String {
    use ultrasql_wal::RecordType;

    match record.header.record_type {
        RecordType::HeapInsert => {
            format_decoded(ultrasql_wal::HeapInsertPayload::decode(&record.payload))
        }
        RecordType::HeapUpdate => {
            format_decoded(ultrasql_wal::HeapUpdatePayload::decode(&record.payload))
        }
        RecordType::HeapDelete => {
            format_decoded(ultrasql_wal::HeapDeletePayload::decode(&record.payload))
        }
        RecordType::FullPageWrite => {
            format_decoded(ultrasql_wal::FullPageWritePayload::decode(&record.payload))
        }
        RecordType::Commit => format_decoded(ultrasql_wal::CommitPayload::decode(&record.payload)),
        RecordType::Abort => format_decoded(ultrasql_wal::AbortPayload::decode(&record.payload)),
        RecordType::Checkpoint => {
            format_decoded(ultrasql_wal::CheckpointPayload::decode(&record.payload))
        }
        RecordType::BTreeOp => {
            format_decoded(ultrasql_wal::BTreeOpPayload::decode(&record.payload))
        }
        RecordType::HeapUpdateInPlace => format_decoded(
            ultrasql_wal::HeapUpdateInPlacePayload::decode(&record.payload),
        ),
        RecordType::HeapUpdateInPlaceBatch => format_decoded(
            ultrasql_wal::HeapUpdateInPlaceBatchPayload::decode(&record.payload),
        ),
        RecordType::HeapDeleteInPlace => format_decoded(
            ultrasql_wal::HeapDeleteInPlacePayload::decode(&record.payload),
        ),
        RecordType::SequenceOp => {
            format_decoded(ultrasql_wal::SequenceOpPayload::decode(&record.payload))
        }
        RecordType::HashOp => format_decoded(ultrasql_wal::HashOpPayload::decode(&record.payload)),
        RecordType::HnswOp => format_decoded(ultrasql_wal::HnswOpPayload::decode(&record.payload)),
        RecordType::IvfFlatOp => {
            format_decoded(ultrasql_wal::IvfFlatOpPayload::decode(&record.payload))
        }
        RecordType::Nop => "decoded=Nop".to_string(),
    }
}

fn format_decoded<T: std::fmt::Debug>(decoded: Result<T, ultrasql_wal::PayloadError>) -> String {
    match decoded {
        Ok(payload) => format!("decoded={payload:?}"),
        Err(err) => format!("payload_error={err}"),
    }
}

async fn run_basebackup(
    data_dir: &PathBuf,
    dest: &PathBuf,
    ops_endpoint: Option<&str>,
) -> Result<()> {
    let checkpoint_fence = if let Some(endpoint) = ops_endpoint {
        let response = http_get_ops_endpoint(endpoint, "/backup/start").await?;
        if !response.ok {
            anyhow::bail!("backup fence start failed: {}", response.body.trim());
        }
        Some(response.body)
    } else {
        None
    };

    let backup_result = run_basebackup_copy(data_dir, dest, checkpoint_fence.as_deref());
    if let Some(endpoint) = ops_endpoint {
        let stop_result = http_get_ops_endpoint(endpoint, "/backup/stop").await;
        if backup_result.is_ok() {
            let response = stop_result?;
            if !response.ok {
                anyhow::bail!("backup fence stop failed: {}", response.body.trim());
            }
        } else {
            let _ = stop_result;
        }
    }
    backup_result
}

fn run_basebackup_copy(
    data_dir: &PathBuf,
    dest: &PathBuf,
    checkpoint_fence: Option<&str>,
) -> Result<()> {
    if dest.exists() {
        anyhow::bail!("basebackup destination already exists: {}", dest.display());
    }
    fs::create_dir_all(dest)?;
    let mut manifest = Vec::new();
    copy_tree_with_manifest(data_dir, data_dir, dest, &mut manifest)?;
    if let Some(fence) = checkpoint_fence {
        let label = backup_label_text(fence);
        fs::write(dest.join("backup_label"), label.as_bytes())?;
        let len = u64::try_from(label.len()).unwrap_or(u64::MAX);
        manifest.push((
            "backup_label".to_string(),
            len,
            checksum_hex(label.as_bytes()),
        ));
    }
    manifest.sort_by(|a, b| a.0.cmp(&b.0));
    let text = basebackup_manifest_text(&manifest, checkpoint_fence);
    fs::write(dest.join("backup_manifest.json"), text)?;
    println!(
        "base backup copied {} files to {}",
        manifest.len(),
        dest.display()
    );
    Ok(())
}

fn backup_label_text(checkpoint_fence: &str) -> String {
    format!("ULTRASQL BACKUP FENCE\n{checkpoint_fence}")
}

fn basebackup_manifest_text(
    manifest: &[(String, u64, String)],
    checkpoint_fence: Option<&str>,
) -> String {
    let mut text = String::from("{\n");
    if let Some(fence) = checkpoint_fence {
        text.push_str(&format!(
            "  \"checkpoint_fence\":\"{}\",\n",
            json_escape(fence)
        ));
    }
    text.push_str("  \"files\": [\n");
    for (idx, (path, bytes, checksum)) in manifest.iter().enumerate() {
        let comma = if idx + 1 == manifest.len() { "" } else { "," };
        text.push_str(&format!(
            "    {{\"path\":\"{}\",\"bytes\":{},\"checksum\":\"{}\"}}{}\n",
            json_escape(path),
            bytes,
            checksum,
            comma
        ));
    }
    text.push_str("  ]\n}\n");
    text
}

fn json_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn run_pg_dump(data_dir: &Path, dest: &Path, format: DumpFormat) -> Result<()> {
    match format {
        DumpFormat::Directory => {
            if dest.exists() {
                anyhow::bail!("dump destination already exists: {}", dest.display());
            }
            fs::create_dir_all(dest)?;
            let mut manifest = Vec::new();
            copy_tree_with_manifest(
                &data_dir.to_path_buf(),
                &data_dir.to_path_buf(),
                &dest.to_path_buf(),
                &mut manifest,
            )?;
            manifest.sort_by(|a, b| a.0.cmp(&b.0));
            fs::write(
                dest.join("ultrasql_dump.manifest"),
                dump_manifest_text(&manifest),
            )?;
            println!(
                "directory dump wrote {} files to {}",
                manifest.len(),
                dest.display()
            );
        }
        DumpFormat::Plain | DumpFormat::Custom | DumpFormat::Tar => {
            if dest.exists() {
                anyhow::bail!("dump destination already exists: {}", dest.display());
            }
            let mut entries = Vec::new();
            collect_dump_entries(data_dir, data_dir, &mut entries)?;
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            let mut out = String::new();
            writeln!(&mut out, "ULTRASQL_DUMP_V1 format={format:?}")?;
            for (path, bytes) in &entries {
                if path.contains('\n') {
                    anyhow::bail!("cannot dump path containing newline: {path}");
                }
                writeln!(&mut out, "FILE {} {}", bytes.len(), path)?;
                writeln!(&mut out, "{}", hex_bytes(bytes))?;
                writeln!(&mut out, "END")?;
            }
            fs::write(dest, out)?;
            println!(
                "{format:?} dump wrote {} files to {}",
                entries.len(),
                dest.display()
            );
        }
    }
    Ok(())
}

fn run_pg_restore(source: &Path, data_dir: &Path) -> Result<()> {
    fs::create_dir_all(data_dir)?;
    if source.is_dir() {
        restore_dump_directory(source, source, data_dir)?;
        println!("restored directory dump into {}", data_dir.display());
        return Ok(());
    }
    let text = fs::read_to_string(source)
        .with_context(|| format!("cannot read dump archive: {}", source.display()))?;
    let mut lines = text.lines();
    let header = lines.next().context("empty dump archive")?;
    if !header.starts_with("ULTRASQL_DUMP_V1 ") {
        anyhow::bail!("unsupported dump archive header: {header}");
    }
    while let Some(line) = lines.next() {
        if line.trim().is_empty() {
            continue;
        }
        let Some(rest) = line.strip_prefix("FILE ") else {
            anyhow::bail!("malformed dump archive line: {line}");
        };
        let (len_text, rel_path) = rest
            .split_once(' ')
            .context("malformed FILE header in dump archive")?;
        let expected_len = len_text.parse::<usize>()?;
        let hex = lines.next().context("missing FILE payload")?;
        let bytes = decode_hex(hex)?;
        if bytes.len() != expected_len {
            anyhow::bail!(
                "dump payload length mismatch for {rel_path}: expected {expected_len}, got {}",
                bytes.len()
            );
        }
        let end = lines.next().context("missing FILE terminator")?;
        if end != "END" {
            anyhow::bail!("malformed dump archive terminator: {end}");
        }
        let dest = data_dir.join(validate_restore_manifest_path(rel_path)?);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(dest, bytes)?;
    }
    println!("restored archive dump into {}", data_dir.display());
    Ok(())
}

fn validate_restore_manifest_path(rel_path: &str) -> Result<PathBuf> {
    let path = Path::new(rel_path);
    let mut clean = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => clean.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                anyhow::bail!("dump archive path escapes restore directory: {rel_path}");
            }
        }
    }
    if clean.as_os_str().is_empty() {
        anyhow::bail!("dump archive path is empty");
    }
    Ok(clean)
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

fn dump_manifest_text(manifest: &[(String, u64, String)]) -> String {
    let mut text = String::from("{\n  \"files\": [\n");
    for (idx, (path, bytes, checksum)) in manifest.iter().enumerate() {
        let comma = if idx + 1 == manifest.len() { "" } else { "," };
        let escaped = path.replace('\\', "\\\\").replace('"', "\\\"");
        text.push_str(&format!(
            "    {{\"path\":\"{escaped}\",\"bytes\":{bytes},\"checksum\":\"{checksum}\"}}{comma}\n"
        ));
    }
    text.push_str("  ]\n}\n");
    text
}

fn collect_dump_entries(
    root: &Path,
    current: &Path,
    entries: &mut Vec<(String, Vec<u8>)>,
) -> Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_dump_entries(root, &path, entries)?;
        } else if path.is_file() {
            let rel = path.strip_prefix(root)?.display().to_string();
            entries.push((rel, fs::read(&path)?));
        }
    }
    Ok(())
}

fn restore_dump_directory(root: &Path, current: &Path, data_dir: &Path) -> Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        let rel = path.strip_prefix(root)?;
        if rel == Path::new("ultrasql_dump.manifest") {
            continue;
        }
        let dest = data_dir.join(rel);
        if path.is_dir() {
            fs::create_dir_all(&dest)?;
            restore_dump_directory(root, &path, data_dir)?;
        } else if path.is_file() {
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(&path, &dest)?;
        }
    }
    Ok(())
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn decode_hex(hex: &str) -> Result<Vec<u8>> {
    if hex.len() % 2 != 0 {
        anyhow::bail!("hex payload has odd length");
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    for idx in (0..hex.len()).step_by(2) {
        let byte = u8::from_str_radix(&hex[idx..idx + 2], 16)
            .with_context(|| format!("invalid hex payload at byte offset {idx}"))?;
        out.push(byte);
    }
    Ok(out)
}

fn run_archive_wal(wal_path: &Path, archive_dir: &Path) -> Result<()> {
    fs::create_dir_all(archive_dir)?;
    let name = wal_path
        .file_name()
        .context("WAL path must have a filename")?;
    let dest = archive_dir.join(name);
    fs::copy(wal_path, &dest)?;
    println!("archived {} to {}", wal_path.display(), dest.display());
    Ok(())
}

fn run_restore_wal(wal_name: &str, archive_dir: &Path, output: &Path) -> Result<()> {
    let source = archive_dir.join(wal_name);
    fs::copy(&source, output)
        .with_context(|| format!("restore WAL {} to {}", source.display(), output.display()))?;
    println!("restored {} to {}", source.display(), output.display());
    Ok(())
}

fn run_wal_send_loop(sender: &WalSender, slot: &str, dest: &Path, interval_ms: u64) -> Result<()> {
    let interval = Duration::from_millis(interval_ms);
    println!(
        "shipping WAL from archive every {interval_ms}ms to {}",
        dest.display()
    );
    loop {
        let copied = sender.send_once(slot, dest)?;
        if copied > 0 {
            println!("sent {copied} WAL file(s) to {}", dest.display());
        }
        thread::sleep(interval);
    }
}

fn run_wal_receive_loop(
    receiver: &WalReceiver,
    data_dir: &Path,
    cascade_archive_dir: Option<&Path>,
    interval_ms: u64,
) -> Result<()> {
    let interval = Duration::from_millis(interval_ms);
    let wal_dir = data_dir.join("pg_wal");
    fs::write(data_dir.join("standby.signal"), b"standby\n")?;
    println!(
        "receiving WAL every {interval_ms}ms into {}",
        wal_dir.display()
    );
    loop {
        let copied = receive_wal_once(receiver, &wal_dir, cascade_archive_dir)?;
        if copied > 0 {
            println!("received {copied} WAL file(s) into {}", wal_dir.display());
        }
        thread::sleep(interval);
    }
}

fn receive_wal_once(
    receiver: &WalReceiver,
    wal_dir: &Path,
    cascade_archive_dir: Option<&Path>,
) -> Result<usize> {
    match cascade_archive_dir {
        Some(archive_dir) => receiver
            .receive_once_cascading(wal_dir, archive_dir)
            .map_err(Into::into),
        None => receiver.receive_once(wal_dir).map_err(Into::into),
    }
}

fn run_validate(data_dir: &Path) -> Result<()> {
    let server = Server::init(data_dir)
        .with_context(|| format!("validate data directory {}", data_dir.display()))?;
    let report = server.validate();
    print_validation_report(&report);
    if report.is_ok() {
        Ok(())
    } else {
        anyhow::bail!("validation failed")
    }
}

fn print_validation_report(report: &ValidationReport) {
    if report.is_ok() {
        println!("validation ok");
    } else {
        println!("validation failed");
    }
    for check in &report.checks {
        println!(
            "{}: {} - {}",
            check.name,
            check.status.as_str(),
            check.detail
        );
    }
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
            println!(
                "initialized UltraSQL data directory at {}",
                data_dir.display()
            );
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
                conf.push_str(&format!(
                    "recovery_target_time = '{}'\n",
                    escape_conf(value)
                ));
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

        let pw = pgpass_lookup_in_home(dir.path(), "anyhost", 5432, "mydb", "bob");
        assert_eq!(pw.as_deref(), Some("hunter2"));
    }

    #[test]
    fn pgpass_wrong_user_no_match() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(".pgpass");
        fs::write(&path, "localhost:5432:mydb:alice:pw\n").expect("write");

        let pw = pgpass_lookup_in_home(dir.path(), "localhost", 5432, "mydb", "bob");
        assert!(pw.is_none(), "wrong user must not match");
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

    #[test]
    fn basebackup_manifest_records_checkpoint_fence_metadata() {
        let manifest = vec![(
            "pg_wal/segment_0000000000".to_string(),
            3,
            "abc".to_string(),
        )];
        let text = basebackup_manifest_text(
            &manifest,
            Some("{\"status\":\"backup_started\",\"flushed_lsn\":7}\n"),
        );

        assert!(text.contains("\"checkpoint_fence\""));
        assert!(text.contains("backup_started"));
        assert!(text.contains("\"files\""));
    }

    #[test]
    fn waldump_decodes_heap_insert_payload() {
        use ultrasql_core::{BlockNumber, Lsn, PageId, RelationId, TupleId, Xid};
        use ultrasql_wal::{HeapInsertPayload, RecordType, WalRecord};

        let tid = TupleId::new(PageId::new(RelationId::new(7), BlockNumber::new(3)), 2);
        let payload = HeapInsertPayload {
            tid,
            tuple_bytes: vec![1, 2, 3],
        }
        .encode()
        .expect("heap insert payload encodes");
        let record = WalRecord::new(RecordType::HeapInsert, Xid::new(42), Lsn::ZERO, 0, payload);

        let lines = waldump_record_lines(&record.encode());

        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("type=HeapInsert"));
        assert!(lines[0].contains("decoded=HeapInsertPayload"));
        assert!(lines[0].contains("tuple_bytes: [1, 2, 3]"));
    }

    #[test]
    fn waldump_reports_malformed_tail() {
        let lines = waldump_record_lines(&[0, 1, 2]);

        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("record_error="));
    }

    // --- pgpass lookup with missing file ---

    #[test]
    fn pgpass_missing_file_returns_none() {
        let dir = tempfile::tempdir().expect("tempdir");
        // No .pgpass file in dir.
        let pw = pgpass_lookup_in_home(dir.path(), "localhost", 5432, "db", "user");
        assert!(pw.is_none());
    }

    fn test_cli() -> Cli {
        Cli {
            host: None,
            port: None,
            dbname: None,
            username: None,
            password: None,
            url: None,
            command: None,
            file: None,
            isready: false,
            ops_endpoint: None,
            waldump: None,
            ctl: None,
            basebackup: None,
            pg_dump: None,
            dump_format: DumpFormat::Custom,
            pg_restore: None,
            archive_wal: None,
            restore_wal: None,
            wal_send_once: None,
            wal_send_interval_ms: 0,
            wal_receive_once: None,
            wal_receive_interval_ms: 0,
            wal_receive_cascade_archive: None,
            replication_slot: "standby".to_owned(),
            archive_dir: PathBuf::from("archive"),
            restore_output: None,
            recovery_target_time: None,
            recovery_target_lsn: None,
            recovery_target_xid: None,
            data_dir: PathBuf::from("data"),
            subcommand: None,
            positional_url: None,
        }
    }

    #[test]
    fn conn_params_merge_overrides_and_connection_string_are_stable() {
        let mut params = ConnParams::default();
        params.merge_from(
            &ConnParams::from_url("postgresql://bob:pw@db.internal:15432/app").expect("valid URL"),
        );
        params.apply_overrides(
            Some("override.internal".to_owned()),
            Some(25432),
            Some("prod".to_owned()),
            Some("alice".to_owned()),
            Some("secret".to_owned()),
        );

        assert_eq!(params.host, "override.internal");
        assert_eq!(params.port, 25432);
        assert_eq!(params.dbname, "prod");
        assert_eq!(params.user, "alice");
        assert_eq!(params.password.as_deref(), Some("secret"));
        assert_eq!(
            build_conn_string(&params),
            "host=override.internal port=25432 dbname=prod user=alice password=secret"
        );

        let err = ConnParams::from_url("postgresql://host:notaport/db")
            .expect_err("invalid URL port fails");
        assert!(format!("{err:#}").contains("invalid port in URL"));

        let p = ConnParams::from_url("postgresql://carol@/db").expect("empty host accepted");
        assert_eq!(p.user, "carol");
        assert_eq!(p.dbname, "db");
    }

    #[test]
    fn resolve_params_honors_url_position_and_flags() {
        let mut cli = test_cli();
        cli.url = Some("postgresql://u1:p1@url-host:5555/url_db".to_owned());
        cli.positional_url = Some("pos-host".to_owned());
        cli.host = Some("flag-host".to_owned());
        cli.port = Some(7777);
        cli.dbname = Some("flag_db".to_owned());
        cli.username = Some("flag_user".to_owned());
        cli.password = Some("flag_pw".to_owned());

        let params = resolve_params(&cli).expect("resolve params");

        assert_eq!(params.host, "flag-host");
        assert_eq!(params.port, 7777);
        assert_eq!(params.dbname, "flag_db");
        assert_eq!(params.user, "flag_user");
        assert_eq!(params.password.as_deref(), Some("flag_pw"));

        let mut positional = test_cli();
        positional.positional_url = Some("postgresql://pos_user@pos-host/pos_db".to_owned());
        let params = resolve_params(&positional).expect("resolve positional URL");
        assert_eq!(params.host, "pos-host");
        assert_eq!(params.user, "pos_user");
        assert_eq!(params.dbname, "pos_db");
    }

    #[test]
    fn pgpass_ignores_comments_malformed_and_non_matching_lines() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(
            dir.path().join(".pgpass"),
            "# comment\nbad-line\nlocalhost:9999:db:user:nope\nlocalhost:5432:db:user:pw\n",
        )
        .expect("write pgpass");

        let pw = pgpass_lookup_in_home(dir.path(), "localhost", 5432, "db", "user");
        assert_eq!(pw.as_deref(), Some("pw"));
    }

    #[test]
    fn meta_query_builders_sanitize_patterns() {
        assert!(list_tables_sql("").contains("pg_catalog.pg_tables"));
        let tables = list_tables_sql("foo';DROP%bar");
        assert!(tables.contains("LIKE 'fooDROP%bar'"));
        assert!(!tables.contains("foo'"));

        let describe = describe_table_sql("public.users;DELETE");
        assert!(describe.contains("table_name = 'public.usersDELETE'"));

        let indexes = list_indexes_sql("idx_%';");
        assert!(indexes.contains("LIKE 'idx_%'"));
        assert!(!indexes.contains("idx_%';"));
    }

    #[tokio::test]
    async fn session_meta_batch_and_sql_paths_execute_against_in_process_server() {
        let addr: std::net::SocketAddr = "127.0.0.1:0".parse().expect("socket literal");
        let (listener, bound) = ultrasql_server::bind_listener(addr)
            .await
            .expect("bind in-process listener");
        let server = std::sync::Arc::new(Server::with_sample_database());
        let handle = tokio::spawn(ultrasql_server::serve_listener(listener, server));
        let conn = format!(
            "host={} port={} user=ultrasql_cli application_name=ultrasql_cli_test",
            bound.ip(),
            bound.port()
        );
        let (client, connection) = tokio_postgres::connect(&conn, NoTls)
            .await
            .expect("connect in-process server");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let params = ConnParams {
            host: bound.ip().to_string(),
            port: bound.port(),
            dbname: "ultrasql".to_owned(),
            user: "ultrasql_cli".to_owned(),
            password: None,
        };
        let mut session = Session::new(client, params);

        session
            .exec_sql("SELECT 1 AS one")
            .await
            .expect("select row");
        session
            .exec_sql("SELECT 1 AS one WHERE false")
            .await
            .expect("empty select");
        session
            .exec_sql("SELECT no_such_column")
            .await
            .expect("error path");

        for cmd in [
            "\\?",
            "\\timing",
            "\\conninfo",
            "\\dt",
            "\\dt users",
            "\\d",
            "\\d users",
            "\\di",
            "\\dn",
            "\\l",
            "\\du",
            "\\df",
            "\\dv",
            "\\ds",
            "\\x",
            "\\x on",
            "\\x off",
            "\\pset",
            "\\pset expanded off",
            "\\pset format aligned",
            "\\pset unknown value",
            "\\c",
            "\\c otherdb",
            "\\unknown",
        ] {
            assert!(!session.handle_meta(cmd).await.expect("meta command"));
        }
        assert!(!session.handle_meta("\\x bad").await.expect("invalid x"));

        let dir = tempfile::tempdir().expect("tempdir");
        let script = dir.path().join("script.sql");
        fs::write(&script, "SELECT 2 AS two;\\ignored\nSELECT 3 AS three;")
            .expect("write include script");
        let include_cmd = format!("\\i {}", script.display());
        assert!(
            !session
                .handle_meta(&include_cmd)
                .await
                .expect("include command")
        );

        session
            .exec_batch("\\timing; SELECT 4 AS four; \\q; SELECT 5 AS five;")
            .await
            .expect("batch execution");
        assert!(session.handle_meta("\\q").await.expect("quit command"));

        handle.abort();
    }

    #[tokio::test]
    async fn ops_http_readiness_handles_ok_and_failure_statuses() {
        let ok_endpoint =
            spawn_one_shot_http("HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nOK").await;
        let response = http_get_ops_endpoint(&format!("http://{ok_endpoint}/ops"), "ready")
            .await
            .expect("http ready");
        assert!(response.ok);
        assert_eq!(response.body, "OK");
        let ready_endpoint =
            spawn_one_shot_http("HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nOK").await;
        assert!(
            check_http_ready(&ready_endpoint.to_string())
                .await
                .expect("ready true")
        );

        let fail_endpoint = spawn_one_shot_http(
            "HTTP/1.1 503 Service Unavailable\r\ncontent-length: 4\r\n\r\nDOWN",
        )
        .await;
        assert!(
            !check_http_ready(&fail_endpoint.to_string())
                .await
                .expect("ready false")
        );

        let run_endpoint =
            spawn_one_shot_http("HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nOK").await;
        let params = ConnParams::default();
        run_isready(&params, Some(&run_endpoint.to_string()))
            .await
            .expect("ops isready");
    }

    async fn spawn_one_shot_http(response: &'static str) -> std::net::SocketAddr {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test http listener");
        let addr = listener.local_addr().expect("listener addr");
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept test HTTP");
            let mut request = [0_u8; 512];
            let _ = socket.read(&mut request).await;
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write test HTTP response");
        });
        addr
    }

    #[test]
    fn wal_dump_archive_restore_and_hex_helpers_cover_success_and_errors() {
        use ultrasql_core::{Lsn, Xid};
        use ultrasql_wal::{RecordType, WalRecord};

        let dir = tempfile::tempdir().expect("tempdir");
        let wal = dir.path().join("000000010000000000000001");
        let record = WalRecord::new(RecordType::Nop, Xid::new(1), Lsn::ZERO, 0, Vec::new());
        fs::write(&wal, record.encode()).expect("write WAL");

        run_waldump(&wal).expect("waldump");
        assert!(
            waldump_record_lines(&[])
                .first()
                .is_some_and(|line| line.contains("empty"))
        );
        assert!(decode_wal_payload(&record).contains("Nop"));
        assert_eq!(
            format_decoded::<()>(Err(ultrasql_wal::PayloadError::Malformed("bad"))),
            "payload_error=payload malformed: bad"
        );

        let archive = dir.path().join("archive");
        run_archive_wal(&wal, &archive).expect("archive WAL");
        let restored = dir.path().join("restored.wal");
        run_restore_wal("000000010000000000000001", &archive, &restored).expect("restore WAL");
        assert_eq!(
            fs::read(&wal).expect("read wal"),
            fs::read(restored).expect("read restored")
        );

        assert_eq!(hex_bytes(&[0, 1, 255]), "0001ff");
        assert_eq!(decode_hex("0001ff").expect("decode hex"), vec![0, 1, 255]);
        assert!(
            decode_hex("0")
                .expect_err("odd hex")
                .to_string()
                .contains("odd length")
        );
        assert!(
            format!("{:#}", decode_hex("zz").expect_err("invalid hex")).contains("invalid hex")
        );
    }

    #[tokio::test]
    async fn ctl_commands_write_expected_signal_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data_dir = dir.path().join("data");
        let data_dir = data_dir.to_path_buf();
        let params = ConnParams::default();
        let targets = RecoveryTargets {
            time: Some("2026-05-29 00:00:00 O'Hara".to_owned()),
            lsn: Some("0/16B6C50".to_owned()),
            xid: Some("42".to_owned()),
        };

        run_ctl(CtlCommand::Initdb, &data_dir, &params, None, &targets)
            .await
            .expect("initdb");
        assert!(data_dir.join("base").is_dir());
        assert!(data_dir.join("pg_wal").is_dir());
        assert!(data_dir.join("global").is_dir());

        run_ctl(CtlCommand::Start, &data_dir, &params, None, &targets)
            .await
            .expect("start");
        run_ctl(CtlCommand::Reload, &data_dir, &params, None, &targets)
            .await
            .expect("reload");
        run_ctl(CtlCommand::Promote, &data_dir, &params, None, &targets)
            .await
            .expect("promote");
        run_ctl(CtlCommand::Standby, &data_dir, &params, None, &targets)
            .await
            .expect("standby");
        run_ctl(CtlCommand::Recovery, &data_dir, &params, None, &targets)
            .await
            .expect("recovery");
        run_ctl(CtlCommand::Stop, &data_dir, &params, None, &targets)
            .await
            .expect("stop");

        assert_eq!(
            fs::read_to_string(data_dir.join("promote.signal")).expect("promote"),
            "promote\n"
        );
        assert_eq!(
            fs::read_to_string(data_dir.join("standby.signal")).expect("standby"),
            "standby\n"
        );
        let recovery = fs::read_to_string(data_dir.join("recovery.targets")).expect("targets");
        assert!(recovery.contains("O''Hara"));
        assert!(recovery.contains("recovery_target_lsn = '0/16B6C50'"));
        assert!(recovery.contains("recovery_target_xid = '42'"));
    }

    #[test]
    fn basebackup_dump_restore_and_manifest_helpers_round_trip_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data = dir.path().join("data");
        fs::create_dir_all(data.join("base/1")).expect("create data dir");
        fs::write(data.join("base/1/heap"), b"rows").expect("write heap");
        fs::write(data.join("pg_version"), b"1").expect("write version");

        let backup = dir.path().join("backup");
        run_basebackup_copy(
            &data.to_path_buf(),
            &backup.to_path_buf(),
            Some("{\"flushed_lsn\":7}\n"),
        )
        .expect("basebackup copy");
        assert_eq!(
            fs::read(backup.join("base/1/heap")).expect("backup heap"),
            b"rows"
        );
        assert!(
            fs::read_to_string(backup.join("backup_label"))
                .expect("backup label")
                .contains("ULTRASQL BACKUP FENCE")
        );
        assert!(run_basebackup_copy(&data.to_path_buf(), &backup.to_path_buf(), None).is_err());

        let directory_dump = dir.path().join("dumpdir");
        run_pg_dump(&data, &directory_dump, DumpFormat::Directory).expect("directory dump");
        assert!(directory_dump.join("ultrasql_dump.manifest").is_file());

        for format in [DumpFormat::Plain, DumpFormat::Custom, DumpFormat::Tar] {
            let archive = dir.path().join(format!("dump-{format:?}.ultra"));
            run_pg_dump(&data, &archive, format).expect("archive dump");
            let restored = dir.path().join(format!("restore-{format:?}"));
            run_pg_restore(&archive, &restored).expect("archive restore");
            assert_eq!(
                fs::read(restored.join("base/1/heap")).expect("restored heap"),
                b"rows"
            );
        }

        let restored_dir = dir.path().join("restore-dir");
        run_pg_restore(&directory_dump, &restored_dir).expect("directory restore");
        assert_eq!(
            fs::read(restored_dir.join("base/1/heap")).expect("dir restore"),
            b"rows"
        );

        assert!(dump_manifest_text(&[("a\"b".to_owned(), 3, "abc".to_owned())]).contains("a\\\"b"));
        assert_eq!(json_escape("\"\\\n"), "\\\"\\\\\\n");
        assert_eq!(checksum_hex(b"same"), checksum_hex(b"same"));
        assert!(run_pg_restore(&dir.path().join("missing.dump"), &dir.path().join("bad")).is_err());
    }

    #[test]
    fn pg_restore_rejects_archive_paths_outside_data_dir() {
        let dir = tempfile::tempdir().expect("tempdir");
        let archive = dir.path().join("escape.dump");
        let data_dir = dir.path().join("restore");
        let escaped = dir.path().join("escaped");

        fs::write(
            &archive,
            "ULTRASQL_DUMP_V1 format=Plain\nFILE 5 ../escaped\n68656c6c6f\nEND\n",
        )
        .expect("write archive");

        assert!(run_pg_restore(&archive, &data_dir).is_err());
        assert!(!escaped.exists());
    }

    #[test]
    fn wal_receiver_wrapper_copies_and_cascades_archived_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let source = dir.path().join("source");
        let standby = dir.path().join("standby");
        let cascade = dir.path().join("cascade");
        fs::create_dir_all(&source).expect("source");
        fs::write(source.join("000000010000000000000001"), b"wal1").expect("wal1");

        let receiver = WalReceiver::new(&source);
        assert_eq!(
            receive_wal_once(&receiver, &standby, None).expect("receive"),
            1
        );
        assert_eq!(
            fs::read(standby.join("000000010000000000000001")).expect("standby wal"),
            b"wal1"
        );
        assert_eq!(
            receive_wal_once(&receiver, &standby, Some(&cascade)).expect("cascade receive"),
            1
        );
        assert_eq!(
            fs::read(cascade.join("000000010000000000000001")).expect("cascade wal"),
            b"wal1"
        );
    }

    #[test]
    fn validation_report_prints_failure_and_escape_conf_quotes() {
        let report = ValidationReport {
            checks: vec![ultrasql_server::ValidationCheck {
                name: "catalog",
                status: ultrasql_server::ValidationStatus::Failed,
                detail: "broken".to_owned(),
            }],
        };
        assert!(!report.is_ok());
        print_validation_report(&report);
        assert_eq!(escape_conf("O'Hara"), "O''Hara");
    }
}
