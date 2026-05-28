//! `pg_hba.conf`-style host-based access control.
//!
//! Parses a subset of PostgreSQL's `pg_hba.conf` syntax and evaluates
//! rules against incoming connection attributes.
//!
//! # Syntax
//!
//! Each non-blank, non-comment line is a rule with whitespace-separated
//! columns:
//!
//! ```text
//! <connection-type>  <database>  <user>  [<address>]  <method>
//! ```
//!
//! | Field | Values |
//! |---|---|
//! | connection-type | `local`, `host`, `hostssl`, `hostnossl` |
//! | database | `all`, `replication`, or a comma-separated list of names |
//! | user | `all` or a comma-separated list of role names |
//! | address | an IPv4/IPv6 address or CIDR range; absent for `local` |
//! | method | `trust`, `reject`, `md5`, `scram-sha-256`, `password` |
//!
//! Lines starting with `#` (after optional leading whitespace) and blank
//! lines are ignored. Inline comments (`# ...` after fields) are not
//! supported by this parser.
//!
//! # Example
//!
//! ```rust
//! use ultrasql_server::auth::hba::{HbaConfig, HbaConnectionKind};
//! use std::net::IpAddr;
//!
//! let cfg = HbaConfig::parse(
//!     "# local connections\n\
//!      local  all  all  trust\n\
//!      host   all  all  127.0.0.1/32  scram-sha-256\n"
//! ).expect("parse ok");
//!
//! let rule = cfg.match_rule(HbaConnectionKind::Local, "mydb", "alice", None);
//! assert!(rule.is_some());
//! ```

use std::net::IpAddr;

use ipnet::IpNet;
use thiserror::Error;

// ── Error ─────────────────────────────────────────────────────────────────────

/// Errors returned when parsing a `pg_hba.conf`-format string.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum HbaParseError {
    /// A rule line did not have the expected number of fields.
    #[error("line {line}: expected {expected} fields, found {found}")]
    WrongFieldCount {
        /// 1-based line number in the source text.
        line: usize,
        /// Number of fields the parser expected.
        expected: usize,
        /// Number of fields found.
        found: usize,
    },

    /// The `connection-type` field was not recognised.
    #[error("line {line}: unknown connection type {value:?}")]
    UnknownConnectionType {
        /// 1-based line number.
        line: usize,
        /// The unrecognised value.
        value: String,
    },

    /// The `method` field was not recognised.
    #[error("line {line}: unknown auth method {value:?}")]
    UnknownMethod {
        /// 1-based line number.
        line: usize,
        /// The unrecognised value.
        value: String,
    },

    /// The `address` field could not be parsed as an IP network.
    #[error("line {line}: invalid address {value:?}: {reason}")]
    InvalidAddress {
        /// 1-based line number.
        line: usize,
        /// The value that failed to parse.
        value: String,
        /// The underlying parse error message.
        reason: String,
    },
}

// ── Types ──────────────────────────────────────────────────────────────────────

/// Connection type column of an HBA rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HbaConnectionKind {
    /// Unix-domain socket connection (`local`).
    Local,
    /// Any TCP connection (`host`).
    Host,
    /// TCP connection with TLS required (`hostssl`).
    HostSsl,
    /// TCP connection without TLS (`hostnossl`).
    HostNoSsl,
}

/// Database match column of an HBA rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HbaDatabaseMatch {
    /// Matches every database (`all`).
    All,
    /// The replication pseudo-database (`replication`).
    Replication,
    /// An explicit list of database names.
    List(Vec<String>),
}

impl HbaDatabaseMatch {
    /// Returns `true` if this match accepts `db`.
    fn matches(&self, db: &str) -> bool {
        match self {
            Self::All => true,
            Self::Replication => db == "replication",
            Self::List(names) => names.iter().any(|n| n == db),
        }
    }
}

/// User match column of an HBA rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HbaUserMatch {
    /// Matches every role (`all`).
    All,
    /// An explicit list of role names.
    List(Vec<String>),
}

impl HbaUserMatch {
    /// Returns `true` if this match accepts `user`.
    fn matches(&self, user: &str) -> bool {
        match self {
            Self::All => true,
            Self::List(names) => names.iter().any(|n| n == user),
        }
    }
}

/// Authentication method column of an HBA rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HbaMethod {
    /// No password required (`trust`).
    Trust,
    /// Always deny (`reject`).
    Reject,
    /// MD5-hashed password (`md5`). Legacy; gated behind a config flag.
    Md5,
    /// SCRAM-SHA-256 (`scram-sha-256`).
    ScramSha256,
    /// Cleartext password (`password`).
    Password,
}

/// A single parsed HBA rule.
///
/// Rules are evaluated in order by [`HbaConfig::match_rule`]; the first
/// matching rule wins.
#[derive(Debug, Clone)]
pub struct HbaRule {
    /// Connection type this rule applies to.
    pub kind: HbaConnectionKind,
    /// Database(s) this rule applies to.
    pub database: HbaDatabaseMatch,
    /// User(s) this rule applies to.
    pub user: HbaUserMatch,
    /// Network address this rule applies to. `None` for `local` rules.
    pub address: Option<IpNet>,
    /// Authentication method to apply when this rule matches.
    pub method: HbaMethod,
}

/// An ordered collection of HBA rules parsed from a `pg_hba.conf`-format
/// string.
///
/// Rules are evaluated in source order; the first matching rule is
/// returned by [`HbaConfig::match_rule`].
#[derive(Debug, Default)]
pub struct HbaConfig {
    rules: Vec<HbaRule>,
}

impl HbaConfig {
    /// Parse a `pg_hba.conf`-format string and return an ordered
    /// [`HbaConfig`].
    ///
    /// # Errors
    ///
    /// Returns [`HbaParseError`] if any rule line is malformed.
    pub fn parse(text: &str) -> Result<Self, HbaParseError> {
        let mut rules = Vec::new();
        for (idx, line) in text.lines().enumerate() {
            let line_no = idx + 1;
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let rule = parse_rule(trimmed, line_no)?;
            rules.push(rule);
        }
        Ok(Self { rules })
    }

    /// Find the first rule that matches the connection attributes, or
    /// `None` if no rule matches.
    ///
    /// `kind` is the connection type. `db` is the target database name.
    /// `user` is the role name. `peer` is the client IP (not present for
    /// `local` connections).
    pub fn match_rule(
        &self,
        kind: HbaConnectionKind,
        db: &str,
        user: &str,
        peer: Option<IpAddr>,
    ) -> Option<&HbaRule> {
        for rule in &self.rules {
            if !rule_kind_matches(rule.kind, kind) {
                continue;
            }
            if !rule.database.matches(db) {
                continue;
            }
            if !rule.user.matches(user) {
                continue;
            }
            if !address_matches(rule.address.as_ref(), peer) {
                continue;
            }
            return Some(rule);
        }
        None
    }

    /// Returns the number of rules in the configuration.
    #[must_use]
    pub fn rule_count(&self) -> usize {
        self.rules.len()
    }

    /// Returns an iterator over all rules.
    pub fn rules(&self) -> impl Iterator<Item = &HbaRule> {
        self.rules.iter()
    }
}

// ── Private helpers ────────────────────────────────────────────────────────────

/// Parse a single non-blank, non-comment rule line.
fn parse_rule(line: &str, line_no: usize) -> Result<HbaRule, HbaParseError> {
    let fields: Vec<&str> = line.split_whitespace().collect();

    // local: 4 fields (type db user method)
    // host*: 5 fields (type db user address method)
    if fields.len() < 4 {
        return Err(HbaParseError::WrongFieldCount {
            line: line_no,
            expected: 4,
            found: fields.len(),
        });
    }

    let kind = parse_kind(fields[0], line_no)?;
    let database = parse_database(fields[1]);
    let user = parse_user(fields[2]);

    let (address, method_str) = if kind == HbaConnectionKind::Local {
        // local rules have no address column.
        if fields.len() < 4 {
            return Err(HbaParseError::WrongFieldCount {
                line: line_no,
                expected: 4,
                found: fields.len(),
            });
        }
        (None, fields[3])
    } else {
        // host/hostssl/hostnossl rules have an address column.
        if fields.len() < 5 {
            return Err(HbaParseError::WrongFieldCount {
                line: line_no,
                expected: 5,
                found: fields.len(),
            });
        }
        let addr_str = fields[3];
        let net: IpNet =
            addr_str
                .parse()
                .map_err(|e: ipnet::AddrParseError| HbaParseError::InvalidAddress {
                    line: line_no,
                    value: addr_str.to_owned(),
                    reason: e.to_string(),
                })?;
        (Some(net), fields[4])
    };

    let method = parse_method(method_str, line_no)?;

    Ok(HbaRule {
        kind,
        database,
        user,
        address,
        method,
    })
}

fn parse_kind(s: &str, line_no: usize) -> Result<HbaConnectionKind, HbaParseError> {
    match s {
        "local" => Ok(HbaConnectionKind::Local),
        "host" => Ok(HbaConnectionKind::Host),
        "hostssl" => Ok(HbaConnectionKind::HostSsl),
        "hostnossl" => Ok(HbaConnectionKind::HostNoSsl),
        other => Err(HbaParseError::UnknownConnectionType {
            line: line_no,
            value: other.to_owned(),
        }),
    }
}

fn parse_database(s: &str) -> HbaDatabaseMatch {
    match s {
        "all" => HbaDatabaseMatch::All,
        "replication" => HbaDatabaseMatch::Replication,
        other => HbaDatabaseMatch::List(other.split(',').map(|p| p.trim().to_owned()).collect()),
    }
}

fn parse_user(s: &str) -> HbaUserMatch {
    match s {
        "all" => HbaUserMatch::All,
        other => HbaUserMatch::List(other.split(',').map(|p| p.trim().to_owned()).collect()),
    }
}

fn parse_method(s: &str, line_no: usize) -> Result<HbaMethod, HbaParseError> {
    match s {
        "trust" => Ok(HbaMethod::Trust),
        "reject" => Ok(HbaMethod::Reject),
        "md5" => Ok(HbaMethod::Md5),
        "scram-sha-256" => Ok(HbaMethod::ScramSha256),
        "password" => Ok(HbaMethod::Password),
        other => Err(HbaParseError::UnknownMethod {
            line: line_no,
            value: other.to_owned(),
        }),
    }
}

/// Returns `true` if the rule's connection kind is compatible with the
/// incoming connection kind.
const fn rule_kind_matches(rule_kind: HbaConnectionKind, conn_kind: HbaConnectionKind) -> bool {
    matches!(
        (rule_kind, conn_kind),
        // Exact matches.
        (HbaConnectionKind::Local, HbaConnectionKind::Local)
        | (HbaConnectionKind::HostSsl, HbaConnectionKind::HostSsl)
        | (HbaConnectionKind::HostNoSsl, HbaConnectionKind::HostNoSsl)
        // `host` matches both ssl and non-ssl TCP connections.
        | (
            HbaConnectionKind::Host,
            HbaConnectionKind::Host | HbaConnectionKind::HostSsl | HbaConnectionKind::HostNoSsl,
        )
    )
}

/// Returns `true` if the rule's address (if any) contains `peer`.
fn address_matches(rule_addr: Option<&IpNet>, peer: Option<IpAddr>) -> bool {
    match (rule_addr, peer) {
        (None, None) => true,                       // local rule, local conn
        (None, Some(_)) | (Some(_), None) => false, // kind mismatch
        (Some(net), Some(ip)) => net.contains(&ip),
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;

    const SAMPLE_CONFIG: &str = "
# Allow local superuser access via trust.
local   all             all                             trust
# IPv4 loopback via SCRAM.
host    all             all             127.0.0.1/32    scram-sha-256
# IPv6 loopback via SCRAM.
host    all             all             ::1/128         scram-sha-256
# Reject everyone else from a specific subnet.
host    all             all             10.0.0.0/8      reject
";

    #[test]
    fn parse_returns_correct_rule_count() {
        let cfg = HbaConfig::parse(SAMPLE_CONFIG).expect("parse ok");
        assert_eq!(cfg.rule_count(), 4);
    }

    #[test]
    fn first_rule_is_local_trust() {
        let cfg = HbaConfig::parse(SAMPLE_CONFIG).expect("parse ok");
        let rules: Vec<_> = cfg.rules().collect();
        let r = &rules[0];
        assert_eq!(r.kind, HbaConnectionKind::Local);
        assert_eq!(r.database, HbaDatabaseMatch::All);
        assert_eq!(r.user, HbaUserMatch::All);
        assert!(r.address.is_none());
        assert_eq!(r.method, HbaMethod::Trust);
    }

    #[test]
    fn second_rule_is_host_scram_loopback() {
        let cfg = HbaConfig::parse(SAMPLE_CONFIG).expect("parse ok");
        let rules: Vec<_> = cfg.rules().collect();
        let r = &rules[1];
        assert_eq!(r.kind, HbaConnectionKind::Host);
        assert_eq!(r.method, HbaMethod::ScramSha256);
        let net = r.address.as_ref().expect("address present");
        assert!(net.contains(&"127.0.0.1".parse::<IpAddr>().unwrap()));
    }

    #[test]
    fn match_local_connection_returns_trust() {
        let cfg = HbaConfig::parse(SAMPLE_CONFIG).expect("parse ok");
        let rule = cfg
            .match_rule(HbaConnectionKind::Local, "mydb", "alice", None)
            .expect("match found");
        assert_eq!(rule.method, HbaMethod::Trust);
    }

    #[test]
    fn match_loopback_connection_returns_scram() {
        let cfg = HbaConfig::parse(SAMPLE_CONFIG).expect("parse ok");
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        let rule = cfg
            .match_rule(HbaConnectionKind::Host, "mydb", "alice", Some(ip))
            .expect("match found");
        assert_eq!(rule.method, HbaMethod::ScramSha256);
    }

    #[test]
    fn match_private_network_returns_reject() {
        let cfg = HbaConfig::parse(SAMPLE_CONFIG).expect("parse ok");
        let ip: IpAddr = "10.1.2.3".parse().unwrap();
        let rule = cfg
            .match_rule(HbaConnectionKind::Host, "mydb", "bob", Some(ip))
            .expect("match found");
        assert_eq!(rule.method, HbaMethod::Reject);
    }

    #[test]
    fn public_ip_not_in_any_rule_returns_none() {
        let cfg = HbaConfig::parse(SAMPLE_CONFIG).expect("parse ok");
        let ip: IpAddr = "8.8.8.8".parse().unwrap();
        let result = cfg.match_rule(HbaConnectionKind::Host, "mydb", "alice", Some(ip));
        assert!(result.is_none());
    }

    #[test]
    fn blank_lines_and_comments_are_skipped() {
        let cfg = HbaConfig::parse("# comment\n\nlocal all all trust\n\n").expect("parse ok");
        assert_eq!(cfg.rule_count(), 1);
    }

    #[test]
    fn all_methods_parse_correctly() {
        let input = "local all all trust\n\
                     host all all 0.0.0.0/0 reject\n\
                     host all all 0.0.0.0/0 md5\n\
                     host all all 0.0.0.0/0 scram-sha-256\n\
                     host all all 0.0.0.0/0 password\n";
        let cfg = HbaConfig::parse(input).expect("parse ok");
        let methods: Vec<_> = cfg.rules().map(|r| r.method).collect();
        assert_eq!(
            methods,
            vec![
                HbaMethod::Trust,
                HbaMethod::Reject,
                HbaMethod::Md5,
                HbaMethod::ScramSha256,
                HbaMethod::Password,
            ]
        );
    }

    #[test]
    fn unknown_connection_type_returns_error() {
        let err = HbaConfig::parse("unix all all trust").expect_err("must fail");
        assert!(matches!(err, HbaParseError::UnknownConnectionType { .. }));
    }

    #[test]
    fn unknown_method_returns_error() {
        let err = HbaConfig::parse("local all all gssapi").expect_err("must fail");
        assert!(matches!(err, HbaParseError::UnknownMethod { .. }));
    }

    #[test]
    fn too_few_fields_returns_error() {
        // "local all" — only 2 fields
        let err = HbaConfig::parse("local all").expect_err("must fail");
        assert!(matches!(err, HbaParseError::WrongFieldCount { .. }));
    }

    #[test]
    fn invalid_address_returns_error() {
        let err = HbaConfig::parse("host all all not-an-ip scram-sha-256").expect_err("must fail");
        assert!(matches!(err, HbaParseError::InvalidAddress { .. }));
    }

    #[test]
    fn ipv6_loopback_matches_correctly() {
        let cfg = HbaConfig::parse("host all all ::1/128 trust\n").expect("parse ok");
        let ip: IpAddr = "::1".parse().unwrap();
        let rule = cfg
            .match_rule(HbaConnectionKind::Host, "db", "user", Some(ip))
            .expect("match");
        assert_eq!(rule.method, HbaMethod::Trust);
        let other: IpAddr = "::2".parse().unwrap();
        assert!(
            cfg.match_rule(HbaConnectionKind::Host, "db", "user", Some(other))
                .is_none()
        );
    }

    #[test]
    fn database_list_matches_only_named_databases() {
        let cfg = HbaConfig::parse("local db1,db2 all trust\n").expect("parse ok");
        assert!(
            cfg.match_rule(HbaConnectionKind::Local, "db1", "u", None)
                .is_some()
        );
        assert!(
            cfg.match_rule(HbaConnectionKind::Local, "db2", "u", None)
                .is_some()
        );
        assert!(
            cfg.match_rule(HbaConnectionKind::Local, "db3", "u", None)
                .is_none()
        );
    }

    #[test]
    fn user_list_matches_only_named_users() {
        let cfg = HbaConfig::parse("local all alice,bob trust\n").expect("parse ok");
        assert!(
            cfg.match_rule(HbaConnectionKind::Local, "db", "alice", None)
                .is_some()
        );
        assert!(
            cfg.match_rule(HbaConnectionKind::Local, "db", "bob", None)
                .is_some()
        );
        assert!(
            cfg.match_rule(HbaConnectionKind::Local, "db", "charlie", None)
                .is_none()
        );
    }

    #[test]
    fn host_rule_matches_hostssl_and_hostnossl() {
        let cfg = HbaConfig::parse("host all all 0.0.0.0/0 trust\n").expect("parse ok");
        let ip: IpAddr = "1.2.3.4".parse().unwrap();
        // `host` rule matches both HostSsl and HostNoSsl.
        assert!(
            cfg.match_rule(HbaConnectionKind::HostSsl, "db", "u", Some(ip))
                .is_some()
        );
        assert!(
            cfg.match_rule(HbaConnectionKind::HostNoSsl, "db", "u", Some(ip))
                .is_some()
        );
    }

    #[test]
    fn hostssl_rule_does_not_match_plain_host() {
        let cfg = HbaConfig::parse("hostssl all all 0.0.0.0/0 trust\n").expect("parse ok");
        let ip: IpAddr = "1.2.3.4".parse().unwrap();
        // `hostssl` rule must NOT match a plain `Host` connection kind.
        let result = cfg.match_rule(HbaConnectionKind::Host, "db", "u", Some(ip));
        assert!(
            result.is_none(),
            "hostssl rule must not match plain Host kind"
        );
    }
}
