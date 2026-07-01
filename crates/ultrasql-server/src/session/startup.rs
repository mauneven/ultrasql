//! Part of the `session` module split. The
//! `impl<RW> Session<RW>` block is reopened here to add a handful
//! of methods to the type defined in `session/mod.rs`. Splitting
//! across files keeps every unit under the 600-line ceiling without
//! changing semantics.

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::{debug, info};
use ultrasql_protocol::{BackendMessage, FrontendMessage, error_fields};

use super::Session;
use crate::auth::pg_authid::AuthCatalog;
use crate::error::ServerError;

/// Build a `FATAL` `ErrorResponse` for the startup/authentication phase
/// with the structured `S`/`V`/`C`/`M` field set. Startup failures never
/// carry a detail or hint; the connection closes right after the reply.
fn fatal_error(code: &str, message: &str) -> BackendMessage {
    BackendMessage::ErrorResponse {
        fields: error_fields("FATAL", code, message, None, None),
    }
}

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin + Send,
{
    /// Read the startup message and emit the canonical handshake.
    pub(crate) async fn startup(&mut self) -> Result<(), ServerError> {
        // Loop so an SSLRequest / GSSENCRequest (which precede the real
        // StartupMessage) can be declined and the handshake continued.
        // PostgreSQL clients send at most one of each (GSS then SSL), so the
        // attempt count is capped to refuse an abusive pre-startup flood.
        let mut negotiation_attempts: u8 = 0;
        let (major, minor, params) = loop {
            let msg = self.read_frontend().await?;
            match msg {
                FrontendMessage::StartupMessage {
                    protocol_major,
                    protocol_minor,
                    params,
                } => break (protocol_major, protocol_minor, params),
                // A `CancelRequest` rides on the startup-packet framing and
                // is a legitimate non-`StartupMessage` first message. Look up
                // `(pid, secret)` in the server's registry, flip the target
                // session's `CancelFlag` on match, and close this connection
                // without further dialogue (PostgreSQL behaviour: never reply,
                // never error — a mismatched secret silently fails so a probe
                // cannot distinguish "unknown pid" from "wrong secret").
                FrontendMessage::CancelRequest {
                    process_id,
                    secret_key,
                } => {
                    let pid = u32::from_le_bytes(process_id.to_le_bytes());
                    let secret = u32::from_le_bytes(secret_key.to_le_bytes());
                    let _ = self.state.cancel_registry.request_cancel(pid, secret);
                    return Ok(());
                }
                // SSLRequest / GSSENCRequest precede the real StartupMessage
                // (default `sslmode=prefer` sends SSLRequest). PostgreSQL answers
                // with a single byte: `'S'` to upgrade or `'N'` to decline. We
                // answer SSLRequest with `'S'` and upgrade the stream to TLS in
                // place when a server certificate is configured — the real
                // StartupMessage then arrives encrypted — otherwise decline with
                // `'N'` (a `prefer` client continues in plaintext). GSS
                // encryption is always declined.
                FrontendMessage::SslRequest => {
                    negotiation_attempts += 1;
                    if negotiation_attempts > 2 {
                        debug!(target: "ultrasqld", "too many pre-startup negotiation requests");
                        return Err(ServerError::UnexpectedEof);
                    }
                    if let Some(config) = self.state.tls_server_config.clone() {
                        // A client must send nothing between the SSLRequest and
                        // the TLS handshake. Any buffered bytes here are
                        // plaintext that arrived before encryption and would
                        // otherwise be processed as if they came over TLS — a
                        // protocol-injection vector (cf. CVE-2021-23214). Reject.
                        if !self.read_buf.is_empty() {
                            debug!(
                                target: "ultrasqld",
                                "unexpected data after SSLRequest; rejecting before TLS upgrade"
                            );
                            return Err(ServerError::UnexpectedEof);
                        }
                        self.io.write_all(b"S").await?;
                        self.io.flush().await?;
                        if let Err(err) = self.io.upgrade(config).await {
                            debug!(target: "ultrasqld", error = %err, "TLS handshake failed");
                            return Err(ServerError::UnexpectedEof);
                        }
                    } else {
                        self.io.write_all(b"N").await?;
                        self.io.flush().await?;
                    }
                    continue;
                }
                FrontendMessage::GssEncRequest => {
                    negotiation_attempts += 1;
                    if negotiation_attempts > 2 {
                        debug!(target: "ultrasqld", "too many pre-startup negotiation requests");
                        return Err(ServerError::UnexpectedEof);
                    }
                    self.io.write_all(b"N").await?;
                    self.io.flush().await?;
                    continue;
                }
                other => {
                    debug!(target: "ultrasqld", ?other, "expected startup, got other");
                    return Err(ServerError::UnexpectedEof);
                }
            }
        };
        if major != 3 {
            // Reply with an `ErrorResponse` so a libpq client that
            // happens to advertise a future protocol version sees a
            // proper SQLSTATE and a human-readable message before the
            // socket closes; without this the client sees only EOF and
            // reports a confusing "connection closed before handshake"
            // error. The reply is best-effort — if it fails we still
            // propagate the original UnsupportedProtocol error.
            let _ = self
                .send(&fatal_error(
                    "08P01",
                    &format!("unsupported frontend protocol {major}.{minor}; server supports 3.0"),
                ))
                .await;
            return Err(ServerError::UnsupportedProtocol { major, minor });
        }
        let startup_user = params
            .iter()
            .find(|(key, _)| key == "user")
            .map_or("tester", |(_, value)| value.as_str());
        self.auth_user = startup_user.to_ascii_lowercase();
        self.current_user = self.auth_user.clone();
        self.state
            .workload_recorder
            .update_session_user(self.pid, self.auth_user.clone());
        let startup_application_name = params
            .iter()
            .find(|(key, _)| key == "application_name")
            .map_or("", |(_, value)| value.as_str());
        if !startup_application_name.is_empty() {
            self.session_settings.insert(
                "application_name".to_owned(),
                startup_application_name.to_owned(),
            );
            self.state
                .workload_recorder
                .update_session_application_name(
                    self.pid,
                    Some(startup_application_name.to_owned()),
                );
        }

        // A `replication` startup parameter routes this connection to the
        // physical-replication walsender command loop instead of the SQL
        // engine (see [`Session::run_replication`]). PostgreSQL treats
        // `true`/`on`/`yes`/`1`/`database` as replication mode and
        // `false`/`off`/`no`/`0` (or absent) as a normal connection.
        self.is_replication = params.iter().any(|(key, value)| {
            key == "replication"
                && !matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "" | "false" | "off" | "no" | "0"
                )
        });

        // Authentication. The default `Trust` policy short-circuits to
        // `AuthenticationOk`. The `Md5` policy runs the standard
        // PostgreSQL MD5 challenge: send `AuthenticationMD5Password`
        // with a random 4-byte salt, read the client's `Password`
        // message, then verify with `auth::md5::verify_md5_response`.
        // Any failure responds with an `ErrorResponse` and closes.
        match &self.state.auth.clone() {
            crate::AuthConfig::Trust => {}
            crate::AuthConfig::Md5 { username, password } => {
                let presented_user = params
                    .iter()
                    .find(|(k, _)| k == "user")
                    .map(|(_, v)| v.as_str())
                    .unwrap_or("");
                if presented_user != username {
                    let _ = self
                        .send(&fatal_error("28P01", "password authentication failed"))
                        .await;
                    return Err(ServerError::AuthFailed);
                }
                let salt = crate::auth::md5::random_salt();
                self.send(&BackendMessage::AuthenticationMD5Password { salt })
                    .await?;
                let reply = self.read_frontend().await?;
                let supplied = match reply {
                    FrontendMessage::Password { password: p } => p,
                    other => {
                        debug!(target: "ultrasqld", ?other, "expected Password message");
                        let _ = self
                            .send(&fatal_error("08P01", "expected Password message"))
                            .await;
                        return Err(ServerError::AuthFailed);
                    }
                };
                let expected = crate::auth::md5::compute_md5_response(password, username, &salt);
                if !crate::auth::md5::verify_md5_response(&expected, &supplied) {
                    let _ = self
                        .send(&fatal_error("28P01", "password authentication failed"))
                        .await;
                    return Err(ServerError::AuthFailed);
                }
            }
            crate::AuthConfig::Scram { username, verifier } => {
                let presented_user = params
                    .iter()
                    .find(|(k, _)| k == "user")
                    .map(|(_, v)| v.as_str())
                    .unwrap_or("");
                // The username check and the SCRAM proof must both pass. A
                // failure at any SASL step returns `Ok(false)` so a single
                // error path runs; an IO/protocol error propagates via `?`.
                let authenticated =
                    presented_user == username && self.authenticate_scram(verifier).await?;
                if !authenticated {
                    let _ = self
                        .send(&fatal_error("28P01", "password authentication failed"))
                        .await;
                    return Err(ServerError::AuthFailed);
                }
            }
            crate::AuthConfig::Hba(hba) => {
                // PostgreSQL defaults the target database to the role name.
                let database = params
                    .iter()
                    .find(|(k, _)| k == "database")
                    .map(|(_, v)| v.as_str())
                    .unwrap_or(self.auth_user.as_str())
                    .to_owned();
                if !self.authenticate_hba(hba, &database).await? {
                    let _ = self
                        .send(&fatal_error("28P01", "authentication failed"))
                        .await;
                    return Err(ServerError::AuthFailed);
                }
            }
        }
        // Reserve the `ultrasql_` role namespace for the system (mirrors
        // PostgreSQL's `pg_` reservation). A login under this prefix that is
        // not a persisted role would be accepted by the `Trust` policy yet
        // never created via role DDL; once it owns objects, the metadata
        // persistence materializes it as an implicit login role — squatting
        // a system name that a future release may need. Refuse the login
        // instead. The bootstrap role `ultrasql` has no trailing underscore,
        // so it is unaffected, and a role that was explicitly created (hence
        // persisted) still logs in.
        if crate::auth::is_reserved_role_name(&self.auth_user)
            && self
                .state
                .role_catalog
                .lookup_role(&self.auth_user)
                .is_none()
        {
            let _ = self
                .send(&fatal_error(
                    "42939",
                    &format!(
                        "role name \"{}\" is reserved; the \"{}\" prefix is for system roles",
                        self.auth_user,
                        crate::auth::RESERVED_ROLE_PREFIX
                    ),
                ))
                .await;
            return Err(ServerError::AuthFailed);
        }
        if let Some(role) = self.state.role_catalog.lookup_role(&self.auth_user) {
            if !role.can_login {
                let _ = self
                    .send(&fatal_error(
                        "28000",
                        &format!("role {} is not permitted to log in", self.auth_user),
                    ))
                    .await;
                return Err(ServerError::AuthFailed);
            }
            if role
                .valid_until
                .is_some_and(|valid_until| valid_until <= chrono::Utc::now().timestamp_micros())
            {
                let _ = self
                    .send(&fatal_error(
                        "28000",
                        &format!("role {} password has expired", self.auth_user),
                    ))
                    .await;
                return Err(ServerError::AuthFailed);
            }
            if let Err(err) = self
                .state
                .role_connection_limiter
                .try_acquire(&role.name, role.connection_limit)
            {
                let _ = self
                    .send(&fatal_error(
                        "53300",
                        &format!(
                            "role {} connection limit {} exceeded; {} sessions already active",
                            err.role, err.limit, err.active
                        ),
                    ))
                    .await;
                return Err(ServerError::AuthFailed);
            }
            self.connection_limit_role = Some(role.name);
        }
        if self.state.logging_config.log_connections {
            let user = params
                .iter()
                .find(|(key, _)| key == "user")
                .map_or("", |(_, value)| value.as_str());
            let database = params
                .iter()
                .find(|(key, _)| key == "database")
                .map_or("", |(_, value)| value.as_str());
            info!(
                target: "ultrasqld",
                pid = self.pid,
                user,
                database,
                "connection authorized"
            );
        }
        self.send(&BackendMessage::AuthenticationOk).await?;
        // Send the full set of `ParameterStatus` messages that
        // PostgreSQL emits at startup. Several PostgreSQL drivers
        // (psycopg2, JDBC) cache or branch on these values and behave
        // unpredictably if any standard one is missing. The values
        // chosen are PostgreSQL's defaults.
        let params: [(&str, &str); 13] = [
            ("server_version", crate::REPORTED_SERVER_VERSION),
            ("server_encoding", "UTF8"),
            ("client_encoding", "UTF8"),
            ("DateStyle", "ISO, MDY"),
            ("IntervalStyle", "postgres"),
            ("TimeZone", "UTC"),
            ("integer_datetimes", "on"),
            ("standard_conforming_strings", "on"),
            ("extra_float_digits", "1"),
            ("application_name", startup_application_name),
            ("is_superuser", "off"),
            ("session_authorization", startup_user),
            ("in_hot_standby", "off"),
        ];
        for (name, value) in params {
            self.send(&BackendMessage::ParameterStatus {
                name: name.to_string(),
                value: value.to_string(),
            })
            .await?;
        }
        // BackendKeyData — cancellation handle. The session has already
        // registered (pid, secret) with the server's `CancelRegistry`
        // during `Session::new`; emit those values so a peer's
        // `CancelRequest { process_id, secret_key }` round-trips against
        // the same entry. PostgreSQL encodes both fields as signed `i32`
        // on the wire; cast from the registry's `u32` keyspace.
        self.send(&BackendMessage::BackendKeyData {
            process_id: i32::from_le_bytes(self.pid.to_le_bytes()),
            secret_key: i32::from_le_bytes(self.secret.to_le_bytes()),
        })
        .await?;
        self.send(&BackendMessage::ReadyForQuery { status: b'I' })
            .await?;
        Ok(())
    }

    /// Drive the server side of a SCRAM-SHA-256 exchange (RFC 7677).
    ///
    /// Offers `SCRAM-SHA-256`, reads `client-first` (a `SASLInitialResponse`),
    /// replies with `server-first`, reads `client-final` (a `SASLResponse`),
    /// and verifies the client proof. Returns `Ok(true)` when the proof
    /// verifies, `Ok(false)` on any authentication failure (bad mechanism,
    /// malformed SASL, proof mismatch — the caller emits the error and closes),
    /// or `Err` on an IO/connection error.
    async fn authenticate_scram(
        &mut self,
        verifier: &crate::auth::PasswordHash,
    ) -> Result<bool, ServerError> {
        self.send(&BackendMessage::AuthenticationSASL {
            mechanisms: vec![crate::auth::SCRAM_SHA_256.to_owned()],
        })
        .await?;

        // client-first arrives as a `SASLInitialResponse` on the shared `'p'`
        // tag, so read it as a raw frame and parse it ourselves.
        let (tag, payload) = self.read_raw_frontend_frame().await?;
        if tag != b'p' {
            return Ok(false);
        }
        let Ok((mechanism, client_first)) = crate::auth::parse_sasl_initial_response(&payload)
        else {
            return Ok(false);
        };
        if mechanism != crate::auth::SCRAM_SHA_256 {
            return Ok(false);
        }

        let mut scram = crate::auth::ScramSha256Server::new(
            verifier.stored_key,
            verifier.server_key,
            verifier.salt.clone(),
            verifier.iterations,
        );
        let Ok(server_first) = scram.server_first(&client_first) else {
            return Ok(false);
        };
        self.send(&BackendMessage::AuthenticationSASLContinue { data: server_first })
            .await?;

        // client-final arrives as a `SASLResponse`; the whole payload is the
        // SCRAM message bytes.
        let (tag, client_final) = self.read_raw_frontend_frame().await?;
        if tag != b'p' {
            return Ok(false);
        }
        let Ok(server_final) = scram.server_final(&client_final) else {
            return Ok(false);
        };
        self.send(&BackendMessage::AuthenticationSASLFinal { data: server_final })
            .await?;
        Ok(true)
    }

    /// Resolve the authentication outcome for this connection from the `pg_hba`
    /// rules.
    ///
    /// Matches `(connection kind, database, role, client IP)` against the rules
    /// and applies the first match's method. Returns `Ok(true)` to admit,
    /// `Ok(false)` to deny (caller emits the error), or `Err` on IO. `trust`
    /// admits unconditionally; `reject` and no-matching-rule deny;
    /// `scram-sha-256` runs SCRAM against the role's own stored verifier;
    /// `md5`/`password` are rejected because role credentials are stored only as
    /// SCRAM verifiers.
    async fn authenticate_hba(
        &mut self,
        hba: &crate::auth::HbaConfig,
        database: &str,
    ) -> Result<bool, ServerError> {
        use crate::auth::{HbaConnectionKind, HbaMethod};

        // No client IP means an in-process / Unix-socket connection, which
        // matches `local` rules. A TCP peer matches `host` rules plus either
        // `hostssl` or `hostnossl` depending on whether the SSLRequest upgraded
        // the connection to TLS by this point.
        let conn_kind = if self.peer_ip.is_none() {
            HbaConnectionKind::Local
        } else if self.io.is_tls() {
            HbaConnectionKind::HostSsl
        } else {
            HbaConnectionKind::HostNoSsl
        };
        // Canonicalize an IPv4-mapped IPv6 peer (e.g. `::ffff:127.0.0.1`, which
        // a dual-stack listener reports for an IPv4 client) to its IPv4 form so
        // it matches IPv4 CIDR rules like `127.0.0.1/32` as the operator
        // intends, rather than silently failing the address check.
        let peer = self.peer_ip.map(|ip| ip.to_canonical());
        let Some(rule) = hba.match_rule(conn_kind, database, &self.auth_user, peer) else {
            // No matching pg_hba entry → deny (PostgreSQL default).
            return Ok(false);
        };
        match rule.method {
            HbaMethod::Trust => Ok(true),
            HbaMethod::Reject => Ok(false),
            HbaMethod::ScramSha256 => {
                let Some(verifier) = self
                    .state
                    .role_catalog
                    .lookup_role(&self.auth_user)
                    .and_then(|role| role.password)
                else {
                    // Unknown role, or a role with no password set.
                    return Ok(false);
                };
                self.authenticate_scram(&verifier).await
            }
            HbaMethod::Md5 | HbaMethod::Password => {
                tracing::warn!(
                    target: "ultrasqld",
                    method = ?rule.method,
                    role = %self.auth_user,
                    "pg_hba method is unsupported with SCRAM-only role verifiers; rejecting",
                );
                Ok(false)
            }
        }
    }
}
