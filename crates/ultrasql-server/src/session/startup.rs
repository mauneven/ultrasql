//! Part of the `session` module split. The
//! `impl<RW> Session<RW>` block is reopened here to add a handful
//! of methods to the type defined in `session/mod.rs`. Splitting
//! across files keeps every unit under the 600-line ceiling without
//! changing semantics.

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::{debug, info};
use ultrasql_protocol::{BackendMessage, FrontendMessage};

use super::Session;
use crate::auth::pg_authid::AuthCatalog;
use crate::error::ServerError;

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
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
                // SSLRequest / GSSENCRequest: `libpq`/`psql` send these as the
                // very first message (default `sslmode=prefer` sends
                // SSLRequest). PostgreSQL must answer with a single byte: `'S'`
                // to upgrade or `'N'` to decline. We do not negotiate TLS/GSS
                // yet, so we decline with `'N'`; a `prefer` client then
                // continues in plaintext (previously the server dropped the
                // socket with no reply, so stock clients failed to connect).
                FrontendMessage::SslRequest | FrontendMessage::GssEncRequest => {
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
                .send(&BackendMessage::ErrorResponse {
                    fields: vec![
                        (b'S', "FATAL".to_string()),
                        (b'C', "08P01".to_string()),
                        (
                            b'M',
                            format!(
                                "unsupported frontend protocol {major}.{minor}; server supports 3.0"
                            ),
                        ),
                    ],
                })
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
                        .send(&BackendMessage::ErrorResponse {
                            fields: vec![
                                (b'S', "FATAL".to_string()),
                                (b'C', "28P01".to_string()),
                                (b'M', "password authentication failed".to_string()),
                            ],
                        })
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
                            .send(&BackendMessage::ErrorResponse {
                                fields: vec![
                                    (b'S', "FATAL".to_string()),
                                    (b'C', "08P01".to_string()),
                                    (b'M', "expected Password message".to_string()),
                                ],
                            })
                            .await;
                        return Err(ServerError::AuthFailed);
                    }
                };
                let expected = crate::auth::md5::compute_md5_response(password, username, &salt);
                if !crate::auth::md5::verify_md5_response(&expected, &supplied) {
                    let _ = self
                        .send(&BackendMessage::ErrorResponse {
                            fields: vec![
                                (b'S', "FATAL".to_string()),
                                (b'C', "28P01".to_string()),
                                (b'M', "password authentication failed".to_string()),
                            ],
                        })
                        .await;
                    return Err(ServerError::AuthFailed);
                }
            }
        }
        // Reserve the `ultrasql_` role namespace for the system (mirrors
        // PostgreSQL's `pg_` reservation). A login under this prefix that is
        // not a persisted role would be accepted by the `Trust` policy yet
        // never recorded in the role catalog; once it owns objects, the
        // catalog/RLS/sequence/schema sidecar replay on the next restart
        // rejects the unknown owner and the database refuses to start —
        // total data loss. Refuse the login instead. The bootstrap role
        // `ultrasql` has no trailing underscore, so it is unaffected, and a
        // role that was explicitly created (hence persisted) still logs in.
        if crate::auth::is_reserved_role_name(&self.auth_user)
            && self
                .state
                .role_catalog
                .lookup_role(&self.auth_user)
                .is_none()
        {
            let _ = self
                .send(&BackendMessage::ErrorResponse {
                    fields: vec![
                        (b'S', "FATAL".to_string()),
                        (b'C', "42939".to_string()),
                        (
                            b'M',
                            format!(
                                "role name \"{}\" is reserved; the \"{}\" prefix is for system roles",
                                self.auth_user,
                                crate::auth::RESERVED_ROLE_PREFIX
                            ),
                        ),
                    ],
                })
                .await;
            return Err(ServerError::AuthFailed);
        }
        if let Some(role) = self.state.role_catalog.lookup_role(&self.auth_user) {
            if !role.can_login {
                let _ = self
                    .send(&BackendMessage::ErrorResponse {
                        fields: vec![
                            (b'S', "FATAL".to_string()),
                            (b'C', "28000".to_string()),
                            (
                                b'M',
                                format!("role {} is not permitted to log in", self.auth_user),
                            ),
                        ],
                    })
                    .await;
                return Err(ServerError::AuthFailed);
            }
            if role
                .valid_until
                .is_some_and(|valid_until| valid_until <= chrono::Utc::now().timestamp_micros())
            {
                let _ = self
                    .send(&BackendMessage::ErrorResponse {
                        fields: vec![
                            (b'S', "FATAL".to_string()),
                            (b'C', "28000".to_string()),
                            (
                                b'M',
                                format!("role {} password has expired", self.auth_user),
                            ),
                        ],
                    })
                    .await;
                return Err(ServerError::AuthFailed);
            }
            if let Err(err) = self
                .state
                .role_connection_limiter
                .try_acquire(&role.name, role.connection_limit)
            {
                let _ = self
                    .send(&BackendMessage::ErrorResponse {
                        fields: vec![
                            (b'S', "FATAL".to_string()),
                            (b'C', "53300".to_string()),
                            (
                                b'M',
                                format!(
                                    "role {} connection limit {} exceeded; {} sessions already active",
                                    err.role, err.limit, err.active
                                ),
                            ),
                        ],
                    })
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
}
