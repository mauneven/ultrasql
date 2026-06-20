//! CLI argument definitions, connection parameters, and `~/.pgpass` lookup.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};

// ---------------------------------------------------------------------------
// CLI argument definitions
// ---------------------------------------------------------------------------

/// UltraSQL command-line client — connects to ultrasqld or any PostgreSQL
/// UltraSQL server.
#[derive(Debug, Parser)]
#[command(name = "ultrasql", about, version)]
pub(crate) struct Cli {
    /// Server hostname or IP address.
    #[arg(short = 'H', long, env = "PGHOST")]
    pub(crate) host: Option<String>,

    /// Server port number.
    #[arg(short, long, env = "PGPORT", value_parser = clap::value_parser!(u16))]
    pub(crate) port: Option<u16>,

    /// Database name to connect to.
    #[arg(short = 'd', long, env = "PGDATABASE")]
    pub(crate) dbname: Option<String>,

    /// Username to connect as.
    #[arg(short = 'U', long, env = "PGUSER")]
    pub(crate) username: Option<String>,

    /// Connection password (prefer PGPASSWORD env or ~/.pgpass over this flag).
    #[arg(short = 'W', long, env = "PGPASSWORD")]
    pub(crate) password: Option<String>,

    /// Full postgresql:// connection URL. Takes precedence over individual
    /// flags where both are provided.
    #[arg(long)]
    pub(crate) url: Option<String>,

    /// Execute a single SQL statement (or backslash command) and exit.
    #[arg(short = 'c', long, conflicts_with = "file")]
    pub(crate) command: Option<String>,

    /// Read SQL from `file` and execute, then exit.
    #[arg(short = 'f', long)]
    pub(crate) file: Option<PathBuf>,

    /// Check server readiness and exit like `pg_isready`.
    #[arg(long)]
    pub(crate) isready: bool,

    /// Optional HTTP ops endpoint for readiness, e.g. `127.0.0.1:8080`.
    #[arg(long, env = "ULTRASQL_OPS_ENDPOINT")]
    pub(crate) ops_endpoint: Option<String>,

    /// Dump a WAL segment or WAL file in a human-readable hex format.
    #[arg(long, value_name = "PATH")]
    pub(crate) waldump: Option<PathBuf>,

    /// Lightweight `pg_ctl`-style action.
    #[arg(long, value_enum)]
    pub(crate) ctl: Option<CtlCommand>,

    /// Copy a data directory into a base-backup directory and write a manifest.
    #[arg(long, value_name = "DEST")]
    pub(crate) basebackup: Option<PathBuf>,

    /// Write a pg_dump-style UltraSQL archive from `--data-dir`.
    #[arg(long, value_name = "DEST")]
    pub(crate) pg_dump: Option<PathBuf>,

    /// Dump archive format for `--pg-dump`.
    #[arg(long, value_enum, default_value = "custom")]
    pub(crate) dump_format: DumpFormat,

    /// Restore a `--pg-dump` archive or directory into `--data-dir`.
    #[arg(long, value_name = "SOURCE")]
    pub(crate) pg_restore: Option<PathBuf>,

    /// Archive one WAL file into this directory.
    #[arg(long, value_name = "WAL_PATH")]
    pub(crate) archive_wal: Option<PathBuf>,

    /// Restore one WAL filename from `--archive-dir` into this output path.
    #[arg(long, value_name = "WAL_NAME")]
    pub(crate) restore_wal: Option<String>,

    /// Ship archived WAL files once from `--archive-dir` into this directory.
    #[arg(long, value_name = "DEST")]
    pub(crate) wal_send_once: Option<PathBuf>,

    /// Repeat `--wal-send-once` every N milliseconds. Zero means run once.
    #[arg(long, default_value_t = 0)]
    pub(crate) wal_send_interval_ms: u64,

    /// Receive shipped WAL files once from this source directory into `--data-dir/pg_wal`.
    #[arg(long, value_name = "SOURCE")]
    pub(crate) wal_receive_once: Option<PathBuf>,

    /// Repeat `--wal-receive-once` every N milliseconds. Zero means run once.
    #[arg(long, default_value_t = 0)]
    pub(crate) wal_receive_interval_ms: u64,

    /// Also copy received WAL into this archive directory so this standby can
    /// cascade physical WAL to downstream receivers.
    #[arg(long, value_name = "DIR")]
    pub(crate) wal_receive_cascade_archive: Option<PathBuf>,

    /// Replication slot name used by WAL sender.
    #[arg(long, default_value = "standby")]
    pub(crate) replication_slot: String,

    /// WAL archive directory used by `--archive-wal` and `--restore-wal`.
    #[arg(long, default_value = "target/ultrasql-archive")]
    pub(crate) archive_dir: PathBuf,

    /// Output path for `--restore-wal`.
    #[arg(long, value_name = "PATH")]
    pub(crate) restore_output: Option<PathBuf>,

    /// Recovery target time written by `--ctl recovery`.
    #[arg(long)]
    pub(crate) recovery_target_time: Option<String>,

    /// Recovery target LSN written by `--ctl recovery`.
    #[arg(long)]
    pub(crate) recovery_target_lsn: Option<String>,

    /// Recovery target XID written by `--ctl recovery`.
    #[arg(long)]
    pub(crate) recovery_target_xid: Option<String>,

    /// Data directory used by `--ctl initdb|status|promote`.
    #[arg(long, default_value = "target/ultrasql-data")]
    pub(crate) data_dir: PathBuf,

    /// Admin subcommand.
    #[command(subcommand)]
    pub(crate) subcommand: Option<CliSubcommand>,

    /// Positional URL — postgresql:// or host shortcut.
    #[arg(hide = true)]
    pub(crate) positional_url: Option<String>,
}

#[derive(Clone, Copy, Debug, Subcommand)]
pub(crate) enum CliSubcommand {
    /// Validate catalog, indexes, WAL, heap visibility, and ANN tombstones.
    Validate,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum CtlCommand {
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
pub(crate) enum DumpFormat {
    Plain,
    Directory,
    Custom,
    Tar,
}

#[derive(Debug)]
pub(crate) struct RecoveryTargets {
    pub(crate) time: Option<String>,
    pub(crate) lsn: Option<String>,
    pub(crate) xid: Option<String>,
}

// ---------------------------------------------------------------------------
// Connection parameters
// ---------------------------------------------------------------------------

/// Resolved connection parameters after merging all sources.
#[derive(Debug, Clone)]
pub(crate) struct ConnParams {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) dbname: String,
    pub(crate) user: String,
    pub(crate) password: Option<String>,
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
    pub(crate) fn from_url(url: &str) -> Result<Self> {
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
    pub(crate) fn merge_from(&mut self, other: &Self) {
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
    pub(crate) fn apply_overrides(
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

pub(crate) const PGPASS_FILE_LIMIT_BYTES: usize = 64 * 1024;
pub(crate) const PGPASS_FILE_READ_LIMIT_BYTES: u64 = 64 * 1024 + 1;

/// Look up a password from `~/.pgpass`.
///
/// Each line has the form `host:port:database:user:password`. Wildcards
/// (`*`) match any value.
pub(crate) fn pgpass_lookup(host: &str, port: u16, dbname: &str, user: &str) -> Option<String> {
    let home = std::env::var("HOME").ok()?;
    pgpass_lookup_in_home(&PathBuf::from(home), host, port, dbname, user)
}

pub(crate) fn pgpass_lookup_in_home(
    home: &std::path::Path,
    host: &str,
    port: u16,
    dbname: &str,
    user: &str,
) -> Option<String> {
    let pgpass = home.join(".pgpass");
    if !pgpass_permissions_are_private(&pgpass) {
        return None;
    }
    let content = read_pgpass_file(&pgpass)?;

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

fn read_pgpass_file(path: &Path) -> Option<String> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path).ok()?;
    let mut limited = std::io::Read::take(file, PGPASS_FILE_READ_LIMIT_BYTES);
    let mut content = String::new();
    std::io::Read::read_to_string(&mut limited, &mut content).ok()?;
    if content.len() > PGPASS_FILE_LIMIT_BYTES {
        return None;
    }
    Some(content)
}

#[cfg(unix)]
fn pgpass_permissions_are_private(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;

    fs::metadata(path)
        .map(|metadata| metadata.permissions().mode() & 0o077 == 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn pgpass_permissions_are_private(_path: &Path) -> bool {
    true
}
