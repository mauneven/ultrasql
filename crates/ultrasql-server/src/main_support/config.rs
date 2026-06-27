//! CLI-to-configuration translation and startup wiring for `ultrasqld`.
//!
//! Validates the parsed [`Cli`] into the typed config
//! structs the [`Server`] consumes (autovacuum, logging, auth, TLS),
//! enforces listener-security and secret-file hardening rules, derives
//! the SCRAM verifier so the plaintext password is never stored, and
//! applies hot-standby signal files at boot.

// Panic hardening: production (non-test) server-binary code must not
// `.unwrap()`, `.expect()`, or `panic!`. Fallible sites propagate errors;
// proven invariants carry a per-site `#[allow]` with an `// INVARIANT:`
// justification.
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

use std::fs;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use tracing_subscriber::EnvFilter;
use ultrasql_server::{AuthConfig, AutovacuumConfig, LoggingConfig, Server};

use crate::cli::{Cli, LogFormat};

const AUTH_PASSWORD_FILE_MAX_BYTES: u64 = 1024;
const HBA_FILE_MAX_BYTES: u64 = 256 * 1024;

pub(crate) fn init_tracing(
    filter: &str,
    format: LogFormat,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let env_filter = EnvFilter::try_new(filter)?;
    match format {
        LogFormat::Text => tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_target(true)
            .try_init()?,
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_env_filter(env_filter)
            .with_target(true)
            .try_init()?,
    }
    Ok(())
}

pub(crate) fn autovacuum_config_from_cli(cli: &Cli) -> Result<AutovacuumConfig, String> {
    Ok(AutovacuumConfig {
        vacuum_threshold: cli.autovacuum_vacuum_threshold,
        vacuum_scale_factor_ppm: AutovacuumConfig::scale_factor_to_ppm(
            "autovacuum_vacuum_scale_factor",
            cli.autovacuum_vacuum_scale_factor,
        )?,
        analyze_threshold: cli.autovacuum_analyze_threshold,
        analyze_scale_factor_ppm: AutovacuumConfig::scale_factor_to_ppm(
            "autovacuum_analyze_scale_factor",
            cli.autovacuum_analyze_scale_factor,
        )?,
    })
}

pub(crate) fn ops_token_from_cli(cli: &Cli) -> Result<Option<Arc<str>>, String> {
    let Some(token) = cli.ops_token.as_deref() else {
        return Ok(None);
    };
    if token.len() < 16 {
        return Err("ops_token must be at least 16 bytes".to_string());
    }
    if token
        .bytes()
        .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
    {
        return Err("ops_token must not contain whitespace or control bytes".to_string());
    }
    Ok(Some(Arc::<str>::from(token)))
}

fn read_hba_file(path: &Path) -> Result<String, String> {
    let metadata = std::fs::metadata(path)
        .map_err(|err| format!("inspect hba_file {}: {err}", path.display()))?;
    if metadata.len() > HBA_FILE_MAX_BYTES {
        return Err(format!(
            "hba_file {} must be at most {HBA_FILE_MAX_BYTES} bytes",
            path.display()
        ));
    }
    std::fs::read_to_string(path).map_err(|err| format!("read hba_file {}: {err}", path.display()))
}

pub(crate) fn auth_config_from_cli(cli: &Cli) -> Result<Option<AuthConfig>, String> {
    if let Some(hba_file) = cli.hba_file.as_deref() {
        if cli.auth_user.is_some() || cli.auth_password_file.is_some() {
            return Err(
                "--hba-file is mutually exclusive with --auth-user / --auth-password-file"
                    .to_string(),
            );
        }
        let text = read_hba_file(hba_file)?;
        let config = ultrasql_server::auth::HbaConfig::parse(&text)
            .map_err(|e| format!("parse hba_file {}: {e}", hba_file.display()))?;
        return Ok(Some(AuthConfig::Hba(config)));
    }
    match (cli.auth_user.as_deref(), cli.auth_password_file.as_deref()) {
        (None, None) => Ok(None),
        (Some(_), None) => Err("auth_password_file is required when auth_user is set".to_string()),
        (None, Some(_)) => Err("auth_user is required when auth_password_file is set".to_string()),
        (Some(user), Some(password_file)) => {
            validate_auth_user(user)?;
            let raw = read_auth_password_file(password_file)?;
            let password = raw.strip_suffix('\n').unwrap_or(raw.as_str());
            validate_auth_password(password)?;
            let auth = match cli.auth_method.as_str() {
                "md5" => AuthConfig::Md5 {
                    username: user.to_owned(),
                    password: password.to_owned(),
                },
                "scram" => {
                    // Derive the SCRAM verifier once, here, so the plaintext
                    // password is never stored on the server.
                    let salt = ultrasql_server::auth::PasswordHash::random_salt();
                    let verifier = ultrasql_server::auth::PasswordHash::hash_password(
                        password,
                        &salt,
                        ultrasql_server::auth::scram::DEFAULT_ITERATIONS,
                    )
                    .map_err(|e| format!("derive SCRAM verifier: {e}"))?;
                    AuthConfig::Scram {
                        username: user.to_owned(),
                        verifier,
                    }
                }
                other => {
                    return Err(format!(
                        "unknown auth method '{other}'; expected 'scram' or 'md5'"
                    ));
                }
            };
            Ok(Some(auth))
        }
    }
}

fn read_auth_password_file(password_file: &Path) -> Result<String, String> {
    reject_auth_password_file_symlink(password_file)?;
    let file = open_auth_password_file(password_file)?;
    let metadata = file.metadata().map_err(|err| {
        format!(
            "inspect auth_password_file {}: {err}",
            password_file.display()
        )
    })?;
    let file_type = metadata.file_type();
    if !file_type.is_file() {
        return Err(format!(
            "auth_password_file {} must be a regular file",
            password_file.display()
        ));
    }
    validate_auth_password_file_permissions(password_file, &metadata)?;
    if metadata.len() > AUTH_PASSWORD_FILE_MAX_BYTES {
        return Err(format!(
            "auth_password_file {} must be at most {AUTH_PASSWORD_FILE_MAX_BYTES} bytes",
            password_file.display()
        ));
    }
    let mut raw = String::new();
    let mut limited = file.take(AUTH_PASSWORD_FILE_MAX_BYTES.saturating_add(1));
    limited
        .read_to_string(&mut raw)
        .map_err(|err| format!("read auth_password_file {}: {err}", password_file.display()))?;
    if u64::try_from(raw.len()).unwrap_or(u64::MAX) > AUTH_PASSWORD_FILE_MAX_BYTES {
        return Err(format!(
            "auth_password_file {} must be at most {AUTH_PASSWORD_FILE_MAX_BYTES} bytes",
            password_file.display()
        ));
    }
    Ok(raw)
}

fn reject_auth_password_file_symlink(password_file: &Path) -> Result<(), String> {
    let metadata = fs::symlink_metadata(password_file).map_err(|err| {
        format!(
            "inspect auth_password_file {}: {err}",
            password_file.display()
        )
    })?;
    if metadata.file_type().is_symlink() {
        return Err(format!(
            "auth_password_file {} must not be a symlink",
            password_file.display()
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn open_auth_password_file(password_file: &Path) -> Result<fs::File, String> {
    use std::os::unix::fs::OpenOptionsExt;

    fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(password_file)
        .map_err(|err| {
            if err.raw_os_error() == Some(libc::ELOOP) {
                format!(
                    "auth_password_file {} must not be a symlink",
                    password_file.display()
                )
            } else {
                format!("open auth_password_file {}: {err}", password_file.display())
            }
        })
}

#[cfg(not(unix))]
fn open_auth_password_file(password_file: &Path) -> Result<fs::File, String> {
    fs::File::open(password_file)
        .map_err(|err| format!("open auth_password_file {}: {err}", password_file.display()))
}

#[cfg(unix)]
fn validate_auth_password_file_permissions(
    password_file: &Path,
    metadata: &fs::Metadata,
) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;

    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(format!(
            "auth_password_file {} must not be group- or world-accessible",
            password_file.display()
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_auth_password_file_permissions(
    _password_file: &Path,
    _metadata: &fs::Metadata,
) -> Result<(), String> {
    Ok(())
}

fn validate_auth_user(user: &str) -> Result<(), String> {
    if user.is_empty() {
        return Err("auth_user must not be empty".to_string());
    }
    if user
        .bytes()
        .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
    {
        return Err("auth_user must not contain whitespace or control bytes".to_string());
    }
    Ok(())
}

fn validate_auth_password(password: &str) -> Result<(), String> {
    if password.len() < 12 {
        return Err("auth password must be at least 12 bytes".to_string());
    }
    if password
        .bytes()
        .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
    {
        return Err("auth password must not contain whitespace or control bytes".to_string());
    }
    Ok(())
}

pub(crate) fn apply_auth_config(mut server: Server, auth_config: &Option<AuthConfig>) -> Server {
    if let Some(auth) = auth_config {
        server = server.with_auth(auth.clone());
    }
    server
}

pub(crate) fn tls_config_from_cli(
    cli: &Cli,
) -> Result<Option<std::sync::Arc<rustls::ServerConfig>>, String> {
    // clap's `requires` guarantees cert and key are both set or both absent.
    let (Some(cert_file), Some(key_file)) = (cli.tls_cert.as_deref(), cli.tls_key.as_deref())
    else {
        return Ok(None);
    };
    let tls_config = ultrasql_server::tls::TlsConfig {
        cert_file: cert_file.to_path_buf(),
        key_file: key_file.to_path_buf(),
        ca_file: None,
    };
    let server_config = ultrasql_server::tls::TlsHandshake::build_server_config(&tls_config)
        .map_err(|e| format!("load TLS certificate/key: {e}"))?;
    Ok(Some(server_config))
}

pub(crate) fn apply_tls_config(
    mut server: Server,
    tls_config: &Option<std::sync::Arc<rustls::ServerConfig>>,
) -> Server {
    if let Some(config) = tls_config {
        server = server.with_tls(config.clone());
    }
    server
}

pub(crate) fn logging_config_from_cli(cli: &Cli) -> Result<LoggingConfig, String> {
    if cli.log_min_duration_statement_ms < -1 {
        return Err("log_min_duration_statement_ms must be -1 or greater".to_string());
    }
    Ok(LoggingConfig {
        log_connections: cli.log_connections,
        log_min_duration_statement_ms: cli.log_min_duration_statement_ms,
        log_statement: cli.log_statement.into(),
    })
}

pub(crate) fn listen_security_from_cli(cli: &Cli) -> Result<(), String> {
    require_auth_or_refuse(
        &cli.listen,
        cli_has_explicit_auth(cli),
        cli.allow_insecure_listen,
    )
}

/// Refuse to start an accept-all "trust" listener on a public interface
/// unless the operator explicitly opted in.
///
/// Decision matrix:
/// - loopback bind (127.0.0.0/8, `::1`)            → always OK (local dev).
/// - explicit auth configured                      → always OK (any bind).
/// - non-loopback + no auth + `insecure_no_auth`   → OK (explicit opt-in).
/// - non-loopback + no auth + no opt-in            → hard error.
///
/// Pulled out as a pure function (no `Cli`, no I/O) so the security
/// decision is unit-testable in isolation. `bind_addr.ip().is_loopback()`
/// covers the entire IPv4 `127.0.0.0/8` range and IPv6 `::1`; `0.0.0.0`
/// and any routable address report `false` and so are treated as public.
pub(crate) fn require_auth_or_refuse(
    bind_addr: &std::net::SocketAddr,
    auth_configured: bool,
    insecure_no_auth: bool,
) -> Result<(), String> {
    if bind_addr.ip().is_loopback() || auth_configured || insecure_no_auth {
        return Ok(());
    }
    Err(format!(
        "refusing to listen on a non-loopback address ({bind_addr}) with no authentication: \
         the server would silently accept all clients with trust authentication. \
         Configure auth (--auth-user/--auth-password-file with SCRAM, or --hba-file), \
         bind to a loopback address (127.0.0.1/::1), or pass --insecure-no-auth to \
         explicitly allow trust on this bind"
    ))
}

/// `true` when the operator explicitly configured an authentication
/// mechanism: either password auth (`--auth-user` + `--auth-password-file`)
/// or a `pg_hba.conf`-style rules file. A configured mechanism is treated
/// as an explicit, deliberate choice regardless of bind address.
fn cli_has_explicit_auth(cli: &Cli) -> bool {
    cli.hba_file.is_some() || (cli.auth_user.is_some() && cli.auth_password_file.is_some())
}

pub(crate) fn apply_startup_signal_files(state: &Server, data_dir: &Path) -> bool {
    let enabled = startup_signal_file_present(&data_dir.join("standby.signal"))
        || startup_signal_file_present(&data_dir.join("recovery.signal"));
    if enabled {
        state.set_standby_mode(true);
    }
    enabled
}

fn startup_signal_file_present(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_file())
}
