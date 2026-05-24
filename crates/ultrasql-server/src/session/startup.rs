//! Part of the `session` module split. The
//! `impl<RW> Session<RW>` block is reopened here to add a handful
//! of methods to the type defined in `session/mod.rs`. Splitting
//! across files keeps every unit under the 600-line ceiling without
//! changing semantics.

#![allow(unused_imports)]

use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, error, info, warn};
use ultrasql_catalog::{
    CatalogSnapshot, IndexEntry, MutableCatalog, PersistentCatalog, TableEntry,
};
use ultrasql_core::{DataType, PageId, RelationId, Value};
use ultrasql_optimizer::{NoStats, PlanCache, PlanCacheConfig, PlanCacheKey, StatsSource};
use ultrasql_parser::Parser;
use ultrasql_planner::{
    Catalog as PlannerCatalog, InMemoryCatalog, LogicalAlterTableAction, LogicalPlan, TableMeta,
    bind,
};
use ultrasql_protocol::{BackendMessage, FrontendMessage, decode_frontend, encode_backend};
use ultrasql_storage::btree::BTree;
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::heap::{DeleteOptions, HeapAccess, UpdateOptions};
use ultrasql_storage::page::Page;
use ultrasql_txn::{IsolationLevel, Transaction, TransactionManager};

use super::Session;
use crate::error::ServerError;
use crate::extended;
use crate::pipeline::{self, LowerCtx, SampleTables};
use crate::result_encoder::{
    self, SelectResult, run_ddl_command, run_modify_command, run_select, run_select_streamed,
};
use crate::{
    BlankPageLoader, CombinedCatalog, Server, TxnState, decode_key_column, notice_warning,
    run_plan_in_txn,
};

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Read the startup message and emit the canonical handshake.
    pub(crate) async fn startup(&mut self) -> Result<(), ServerError> {
        let msg = self.read_frontend().await?;
        let (major, minor, params) = match msg {
            FrontendMessage::StartupMessage {
                protocol_major,
                protocol_minor,
                params,
            } => (protocol_major, protocol_minor, params),
            // A `CancelRequest` rides on the startup-packet framing and
            // is the only legitimate non-`StartupMessage` first message.
            // Look up `(pid, secret)` in the server's registry, flip the
            // target session's `CancelFlag` on match, and close this
            // connection without further dialogue (PostgreSQL behaviour:
            // never reply, never error — a mismatched secret silently
            // fails so a probe cannot distinguish "unknown pid" from
            // "wrong secret").
            FrontendMessage::CancelRequest {
                process_id,
                secret_key,
            } => {
                let pid = u32::from_le_bytes(process_id.to_le_bytes());
                let secret = u32::from_le_bytes(secret_key.to_le_bytes());
                let _ = self.state.cancel_registry.request_cancel(pid, secret);
                return Ok(());
            }
            // The spec allows an SSLRequest as the very first message
            // (which decodes to a startup-shaped payload); v0.5 does
            // not negotiate TLS yet. We treat anything else as a
            // protocol violation.
            other => {
                debug!(target: "ultrasqld", ?other, "expected startup, got other");
                return Err(ServerError::UnexpectedEof);
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
        let server_version = format!("ultrasql-{}", env!("CARGO_PKG_VERSION"));
        let params: [(&str, &str); 13] = [
            ("server_version", &server_version),
            ("server_encoding", "UTF8"),
            ("client_encoding", "UTF8"),
            ("DateStyle", "ISO, MDY"),
            ("IntervalStyle", "postgres"),
            ("TimeZone", "UTC"),
            ("integer_datetimes", "on"),
            ("standard_conforming_strings", "on"),
            ("extra_float_digits", "1"),
            ("application_name", ""),
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
