//! Command-line interface definition for `ultrasqld`.
//!
//! Holds the [`Cli`] argument struct parsed by `clap`, the log-format
//! and log-statement enums it references, and the long `--help`
//! description. Pure data + parsing surface: no runtime behaviour.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use ultrasql_server::LogStatementMode;

/// `ultrasqld`: a PostgreSQL-wire-compatible SQL database server.
///
/// With `--data-dir` it boots WAL-backed durable storage. Without one it serves
/// an in-memory sample database for quick local experiments — a single
/// pre-populated table:
///
/// ```text
///     users(id INT, name TEXT, score DOUBLE PRECISION)  -- 3 rows: Ada/Grace/Linus
/// ```
///
/// Connect with any PostgreSQL v3 client (`psql`, libpq, language drivers) and
/// run ordinary SQL.
#[derive(Debug, Parser)]
#[command(
    name = "ultrasqld",
    version,
    about = "UltraSQL database server",
    long_about = LONG_ABOUT
)]
pub(crate) struct Cli {
    /// Address to bind the PostgreSQL-wire listener on.
    ///
    /// Defaults to loopback so an out-of-the-box start is never publicly
    /// exposed. Settable via `ULTRASQL_LISTEN` so service managers and
    /// containers can configure the bind declaratively (an explicit CLI value
    /// still wins). Listening on a non-loopback address with no authentication
    /// is refused (see `--auth-user` / `--hba-file` / `--insecure-no-auth`).
    #[arg(long, env = "ULTRASQL_LISTEN", default_value = "127.0.0.1:5433")]
    pub(crate) listen: SocketAddr,

    /// Permit trust-auth PostgreSQL-wire listener on non-loopback addresses.
    ///
    /// `--insecure-no-auth` is the preferred spelling; `--allow-insecure-listen`
    /// is kept as a backward-compatible alias. Either flag explicitly opts the
    /// operator into accept-all "trust" authentication on a public bind (for
    /// deployments behind a separately trusted network boundary).
    #[arg(
        long = "insecure-no-auth",
        alias = "allow-insecure-listen",
        default_value_t = false
    )]
    pub(crate) allow_insecure_listen: bool,

    /// Optional data directory. When set, server boots WAL-backed storage.
    #[arg(long, env = "ULTRASQL_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,

    /// Optional HTTP operations endpoint for `/health`, `/ready`, `/metrics`,
    /// and token-protected backup fencing.
    #[arg(long, env = "ULTRASQL_OPS_LISTEN")]
    pub(crate) ops_listen: Option<SocketAddr>,

    /// Bearer token required for mutating ops routes such as backup fencing.
    #[arg(long, env = "ULTRASQL_OPS_TOKEN")]
    pub(crate) ops_token: Option<String>,

    /// PostgreSQL startup user that must authenticate with MD5.
    #[arg(long, env = "ULTRASQL_AUTH_USER")]
    pub(crate) auth_user: Option<String>,

    /// File containing the authentication password for `--auth-user`.
    #[arg(long, env = "ULTRASQL_AUTH_PASSWORD_FILE")]
    pub(crate) auth_password_file: Option<PathBuf>,

    /// Password authentication method for `--auth-user`: `scram`
    /// (SCRAM-SHA-256, recommended — the password never crosses the wire and
    /// the server stores only a derived verifier) or `md5` (legacy).
    #[arg(
        long,
        env = "ULTRASQL_AUTH_METHOD",
        default_value = "scram",
        value_parser = ["scram", "md5"],
    )]
    pub(crate) auth_method: String,

    /// Path to a `pg_hba.conf`-style rules file. When set, each connection is
    /// authenticated per its matching rule (`trust` / `reject` /
    /// `scram-sha-256`, the last verified against the role's own catalog
    /// password). Mutually exclusive with `--auth-user`.
    #[arg(long, env = "ULTRASQL_HBA_FILE")]
    pub(crate) hba_file: Option<PathBuf>,

    /// Path to the server's PEM-encoded TLS certificate. When set (together with
    /// `--tls-key`), a client `SSLRequest` upgrades the connection to TLS.
    #[arg(long, env = "ULTRASQL_TLS_CERT", requires = "tls_key")]
    pub(crate) tls_cert: Option<PathBuf>,

    /// Path to the server's PKCS#8 PEM-encoded TLS private key (paired with
    /// `--tls-cert`).
    #[arg(long, env = "ULTRASQL_TLS_KEY", requires = "tls_cert")]
    pub(crate) tls_key: Option<PathBuf>,

    /// Tracing level filter, e.g. `info`, `debug`, `ultrasqld=trace`.
    #[arg(long, default_value = "info")]
    pub(crate) log_level: String,

    /// Log output format.
    #[arg(long, value_enum, default_value_t = LogFormat::Text)]
    pub(crate) log_format: LogFormat,

    /// Log each successful connection after authentication.
    #[arg(long, default_value_t = false)]
    pub(crate) log_connections: bool,

    /// Minimum statement duration to log in milliseconds; -1 disables.
    #[arg(long, default_value_t = -1)]
    pub(crate) log_min_duration_statement_ms: i64,

    /// Statement classes logged regardless of duration.
    #[arg(long, value_enum, default_value_t = CliLogStatementMode::None)]
    pub(crate) log_statement: CliLogStatementMode,

    /// Close idle sessions after this many milliseconds; 0 disables.
    #[arg(long, default_value_t = 0)]
    pub(crate) idle_session_timeout_ms: u64,

    /// Server-wide default `statement_timeout` in milliseconds applied to
    /// every session; 0 disables. Any session may override with
    /// `SET statement_timeout` (including back to 0). The bounded default
    /// keeps one runaway query from occupying a connection forever.
    #[arg(
        long,
        env = "ULTRASQL_STATEMENT_TIMEOUT_MS",
        default_value_t = ultrasql_server::DEFAULT_STATEMENT_TIMEOUT_MS
    )]
    pub(crate) statement_timeout_ms: u64,

    /// On SIGTERM/SIGINT, stop accepting and drain in-flight sessions for
    /// this many milliseconds before aborting the rest. A second signal
    /// forces an immediate shutdown.
    #[arg(long, default_value_t = 5000)]
    pub(crate) shutdown_drain_timeout_ms: u64,

    /// Background autovacuum/analyze maintenance interval in milliseconds.
    #[arg(long, default_value_t = 1000)]
    pub(crate) autovacuum_interval_ms: u64,

    /// Minimum tuple changes before autovacuum considers VACUUM work.
    #[arg(long, default_value_t = 50)]
    pub(crate) autovacuum_vacuum_threshold: u64,

    /// Fraction of estimated table rows added to the VACUUM threshold.
    #[arg(long, default_value_t = 0.2)]
    pub(crate) autovacuum_vacuum_scale_factor: f64,

    /// Minimum tuple changes before autovacuum considers ANALYZE work.
    #[arg(long, default_value_t = 50)]
    pub(crate) autovacuum_analyze_threshold: u64,

    /// Fraction of estimated table rows added to the ANALYZE threshold.
    #[arg(long, default_value_t = 0.1)]
    pub(crate) autovacuum_analyze_scale_factor: f64,

    /// Automatic checkpoint interval in milliseconds. Each cycle flushes dirty
    /// pages, fsyncs the data segments, writes per-index/commit-log snapshots,
    /// and recycles WAL segments below the safe floor (so the WAL and restart
    /// time stay bounded instead of growing with total history). 0 disables;
    /// then the floor only advances on an explicit `CHECKPOINT`. Persistent
    /// (data-dir) mode only.
    #[arg(long, default_value_t = 300_000)]
    pub(crate) checkpoint_interval_ms: u64,

    /// WAL segment size in bytes; 0 uses the built-in default (16 MiB). Smaller
    /// segments give finer WAL-retention granularity (segments are recycled
    /// whole at checkpoints). Persistent (data-dir) mode only.
    #[arg(long, default_value_t = 0)]
    pub(crate) wal_segment_size_bytes: u64,

    /// Durability sync primitive for WAL/data flushes, mirroring PostgreSQL's
    /// `wal_sync_method`. `fsync` (default) issues `fsync(2)` — the durability
    /// class PostgreSQL and SQLite defaults provide on every platform.
    /// `fsync_writethrough` additionally forces the drive's own write cache to
    /// stable media (`fcntl(F_FULLFSYNC)` on macOS, like PostgreSQL's
    /// `fsync_writethrough`; identical to `fsync` elsewhere) so commits also
    /// survive sudden power loss on drives with volatile caches.
    #[arg(
        long,
        env = "ULTRASQL_WAL_SYNC_METHOD",
        default_value = "fsync",
        value_parser = ["fsync", "fsync_writethrough"],
    )]
    pub(crate) wal_sync_method: String,

    /// Standby streaming source: libpq-style `host=... port=... user=...`
    /// (optional `slot=...`). Only used when the data dir carries a
    /// `standby.signal`/`recovery.signal` marker; the standby then connects,
    /// streams physical WAL from the primary, and applies it continuously so
    /// read-only sessions stay current. A `primary_conninfo` file inside the
    /// data dir is used when this flag/env is unset.
    #[arg(long, env = "ULTRASQL_PRIMARY_CONNINFO")]
    pub(crate) primary_conninfo: Option<String>,

    /// Shell command used to archive completed WAL files. `%p` expands to the
    /// source path and `%f` expands to the WAL filename.
    #[arg(long, env = "ULTRASQL_ARCHIVE_COMMAND")]
    pub(crate) archive_command: Option<String>,

    /// Shell command used to restore archived WAL files before startup
    /// recovery. `%p` expands to the destination path and `%f` expands to the
    /// WAL filename.
    #[arg(long, env = "ULTRASQL_RESTORE_COMMAND")]
    pub(crate) restore_command: Option<String>,

    /// Maximum number of WAL segment names to probe with `restore_command`.
    /// Zero disables server-side startup restore.
    #[arg(long, default_value_t = 0)]
    pub(crate) restore_max_segments: u32,

    /// Background WAL archive scan interval in milliseconds.
    #[arg(long, default_value_t = 1000)]
    pub(crate) archive_interval_ms: u64,

    /// Kill `archive_command` after this many milliseconds; 0 disables.
    #[arg(
        long,
        env = "ULTRASQL_ARCHIVE_COMMAND_TIMEOUT_MS",
        default_value_t = 60_000
    )]
    pub(crate) archive_command_timeout_ms: u64,

    /// Kill `restore_command` after this many milliseconds; 0 disables.
    #[arg(
        long,
        env = "ULTRASQL_RESTORE_COMMAND_TIMEOUT_MS",
        default_value_t = 60_000
    )]
    pub(crate) restore_command_timeout_ms: u64,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum LogFormat {
    Text,
    Json,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum CliLogStatementMode {
    None,
    Ddl,
    Mod,
    All,
}

impl From<CliLogStatementMode> for LogStatementMode {
    fn from(value: CliLogStatementMode) -> Self {
        match value {
            CliLogStatementMode::None => Self::None,
            CliLogStatementMode::Ddl => Self::Ddl,
            CliLogStatementMode::Mod => Self::Mod,
            CliLogStatementMode::All => Self::All,
        }
    }
}

/// Long description shown by `--help`. Kept as a separate constant so
/// rustfmt does not split it across lines that mangle the indentation.
const LONG_ABOUT: &str = "UltraSQL database server.

Speaks the PostgreSQL wire protocol v3. With --data-dir it boots WAL-backed
durable storage; without one it serves an in-memory sample database for quick
local experiments:

    users(id INT, name TEXT, score DOUBLE PRECISION)
    -- 3 rows: Ada/Grace/Linus

Connect with any libpq-style client and run ordinary SQL. Example session:

    psql -h 127.0.0.1 -p 5433 -d ultrasql -c 'SELECT id, name FROM users;'

Production-oriented flags:
  - --data-dir DIR      boot WAL-backed storage
  - --insecure-no-auth  explicitly permit trust-auth listener outside loopback
                        (alias: --allow-insecure-listen)
  - --auth-user USER    require password auth for this PostgreSQL user
  - --auth-password-file PATH  read the auth password from a private local secret file
  - --auth-method METHOD  scram (SCRAM-SHA-256, default) or md5 (legacy)
  - --hba-file PATH     pg_hba.conf-style per-role rules (trust/reject/scram-sha-256)
  - --ops-listen ADDR   serve /health, /ready, /metrics, and backup routes
  - --ops-token TOKEN   require bearer token for /backup/start and /backup/stop
  - --log-format json   emit structured logs
  - --log-min-duration-statement-ms N
  - --log-statement none|ddl|mod|all
  - --wal-sync-method fsync|fsync_writethrough  durability flush primitive
  - --statement-timeout-ms N  default per-session statement_timeout (0 disables)
  - --idle-session-timeout-ms N
  - --archive-command CMD  archive completed WAL files; %p=path, %f=name
  - --restore-command CMD  restore archived WAL before recovery; %p=path, %f=name
  - --archive-command-timeout-ms N  kill hung archive commands; 0 disables
  - --restore-command-timeout-ms N  kill hung restore commands; 0 disables
";
