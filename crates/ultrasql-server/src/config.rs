//! Server configuration types: auth policy, autovacuum, logging, and WAL
//! archive knobs.
//!
//! Moved verbatim from the crate root; behavior unchanged.
use super::*;

/// Authentication policy for incoming connections.
#[derive(Clone, Debug)]
pub enum AuthConfig {
    /// Accept every connection without challenge. Used by the
    /// in-process tests and the v0.5 default REPL.
    Trust,
    /// Require an MD5 password matching the stored
    /// `(username, password)` pair. The password is held in plain
    /// text inside the server because MD5 is a per-challenge hash —
    /// PostgreSQL stores the same way (or the equivalent
    /// `md5(password+username)` digest).
    Md5 {
        /// Required role name presented in `StartupMessage.user`.
        username: String,
        /// Plain-text password used to recompute the expected MD5
        /// hash on every challenge.
        password: String,
    },
    /// Require a SCRAM-SHA-256 password exchange (RFC 7677, PostgreSQL's
    /// default since PG 10). Unlike [`AuthConfig::Md5`] the server holds only
    /// the derived verifier ([`crate::auth::PasswordHash`]: salt, iterations,
    /// `StoredKey`, `ServerKey`) — never the plaintext password — and the
    /// password never crosses the wire.
    Scram {
        /// Required role name presented in `StartupMessage.user`.
        username: String,
        /// Pre-derived SCRAM verifier for the role's password.
        verifier: crate::auth::PasswordHash,
    },
    /// `pg_hba`-style per-connection authentication. Each connection is matched
    /// against the rules by `(connection kind, database, role, client IP)`; the
    /// first matching rule's method decides the outcome: `trust` admits,
    /// `reject` denies, and `scram-sha-256` runs a SCRAM exchange against the
    /// role's own stored verifier in the role catalog. A connection with no
    /// matching rule is rejected (PostgreSQL semantics).
    Hba(crate::auth::HbaConfig),
}

/// Run undo-log GC every `UNDO_GC_INTERVAL_COMMITS` successful
/// commits. The trim itself is `O(total live undo entries)` so we
/// keep it out of the per-commit critical path.
pub const UNDO_GC_INTERVAL_COMMITS: u64 = 64;

/// Fixed-point denominator used by autovacuum scale-factor settings.
pub const AUTOVACUUM_SCALE_DENOMINATOR: u64 = 1_000_000;

/// Runtime autovacuum threshold configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AutovacuumConfig {
    /// Minimum modified/dead tuple count before VACUUM work is considered.
    pub vacuum_threshold: u64,
    /// VACUUM scale factor in parts per million.
    pub vacuum_scale_factor_ppm: u64,
    /// Minimum modified tuple count before ANALYZE work is considered.
    pub analyze_threshold: u64,
    /// ANALYZE scale factor in parts per million.
    pub analyze_scale_factor_ppm: u64,
}

/// Statement classes accepted by `log_statement`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LogStatementMode {
    /// Do not log statements by class.
    #[default]
    None,
    /// Log DDL statements.
    Ddl,
    /// Log DDL and data-modifying statements.
    Mod,
    /// Log every statement.
    All,
}

impl LogStatementMode {
    /// Return the setting string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Ddl => "ddl",
            Self::Mod => "mod",
            Self::All => "all",
        }
    }
}

/// Runtime statement logging configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LoggingConfig {
    /// Log each successful connection after authentication.
    pub log_connections: bool,
    /// `log_min_duration_statement` in milliseconds; `-1` disables duration
    /// logging, matching PostgreSQL's user-facing convention.
    pub log_min_duration_statement_ms: i64,
    /// Statement-class logging mode.
    pub log_statement: LogStatementMode,
}

/// Runtime WAL archive/restore configuration.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WalArchiveConfig {
    /// Shell command used to archive completed WAL files; empty means off.
    pub archive_command: String,
    /// Shell command used to restore archived WAL files; empty means off.
    pub restore_command: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            log_connections: false,
            log_min_duration_statement_ms: -1,
            log_statement: LogStatementMode::None,
        }
    }
}

impl Default for AutovacuumConfig {
    fn default() -> Self {
        Self {
            vacuum_threshold: 50,
            vacuum_scale_factor_ppm: 200_000,
            analyze_threshold: 50,
            analyze_scale_factor_ppm: 100_000,
        }
    }
}

impl AutovacuumConfig {
    /// Convert a user-facing floating-point scale factor into fixed-point ppm.
    ///
    /// # Errors
    ///
    /// Returns an error when the value is NaN, infinite, negative, or too large
    /// to represent in the fixed-point counter space.
    pub fn scale_factor_to_ppm(name: &str, value: f64) -> Result<u64, String> {
        if !value.is_finite() || value < 0.0 {
            return Err(format!("{name} must be a non-negative finite number"));
        }
        let scaled = (value * u64_to_f64_saturating(AUTOVACUUM_SCALE_DENOMINATOR)).round();
        if scaled > u64_to_f64_saturating(u64::MAX) {
            return Err(format!("{name} is too large"));
        }
        format!("{scaled:.0}")
            .parse::<u64>()
            .map_err(|_| format!("{name} is too large"))
    }

    /// Return the configured VACUUM scale factor as a user-facing decimal.
    #[must_use]
    pub fn vacuum_scale_factor(self) -> f64 {
        u64_to_f64_saturating(self.vacuum_scale_factor_ppm)
            / u64_to_f64_saturating(AUTOVACUUM_SCALE_DENOMINATOR)
    }

    /// Return the configured ANALYZE scale factor as a user-facing decimal.
    #[must_use]
    pub fn analyze_scale_factor(self) -> f64 {
        u64_to_f64_saturating(self.analyze_scale_factor_ppm)
            / u64_to_f64_saturating(AUTOVACUUM_SCALE_DENOMINATOR)
    }

    pub(crate) fn vacuum_threshold_for_rows(self, estimated_rows: u64) -> u64 {
        scaled_threshold(
            self.vacuum_threshold,
            self.vacuum_scale_factor_ppm,
            estimated_rows,
        )
    }

    pub(crate) fn analyze_threshold_for_rows(self, estimated_rows: u64) -> u64 {
        scaled_threshold(
            self.analyze_threshold,
            self.analyze_scale_factor_ppm,
            estimated_rows,
        )
    }
}

pub(crate) fn validate_autovacuum_reloptions(
    options: &[(String, String)],
) -> Result<(), ServerError> {
    let mut config = AutovacuumConfig::default();
    apply_autovacuum_reloptions(&mut config, options)?;
    Ok(())
}

pub(crate) fn autovacuum_config_for_table(base: AutovacuumConfig, entry: &TableEntry) -> AutovacuumConfig {
    let mut config = base;
    if let Err(error) = apply_autovacuum_reloptions(&mut config, &entry.options) {
        tracing::warn!(
            table = %entry.name,
            error = %error,
            "ignoring invalid autovacuum reloptions",
        );
        return base;
    }
    config
}

pub(crate) fn apply_autovacuum_reloptions(
    config: &mut AutovacuumConfig,
    options: &[(String, String)],
) -> Result<(), ServerError> {
    for (name, value) in options {
        match name.as_str() {
            "autovacuum_vacuum_threshold" => {
                config.vacuum_threshold = parse_autovacuum_u64(name, value)?;
            }
            "autovacuum_vacuum_scale_factor" => {
                config.vacuum_scale_factor_ppm = parse_autovacuum_scale(name, value)?;
            }
            "autovacuum_analyze_threshold" => {
                config.analyze_threshold = parse_autovacuum_u64(name, value)?;
            }
            "autovacuum_analyze_scale_factor" => {
                config.analyze_scale_factor_ppm = parse_autovacuum_scale(name, value)?;
            }
            _ => {
                return Err(ServerError::Ddl(format!(
                    "unsupported autovacuum reloption: {name}",
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn parse_autovacuum_u64(name: &str, value: &str) -> Result<u64, ServerError> {
    value
        .parse::<u64>()
        .map_err(|_| ServerError::Ddl(format!("{name} must be a non-negative integer")))
}

pub(crate) fn parse_autovacuum_scale(name: &str, value: &str) -> Result<u64, ServerError> {
    let parsed = value
        .parse::<f64>()
        .map_err(|_| ServerError::Ddl(format!("{name} must be a non-negative finite number")))?;
    AutovacuumConfig::scale_factor_to_ppm(name, parsed).map_err(ServerError::Ddl)
}

pub(crate) fn scaled_threshold(base: u64, scale_factor_ppm: u64, estimated_rows: u64) -> u64 {
    let scaled = (u128::from(estimated_rows) * u128::from(scale_factor_ppm))
        / u128::from(AUTOVACUUM_SCALE_DENOMINATOR);
    base.saturating_add(u64::try_from(scaled).unwrap_or(u64::MAX))
}

pub(crate) fn u64_to_f64_saturating(value: u64) -> f64 {
    value.to_f64().unwrap_or(f64::MAX)
}
