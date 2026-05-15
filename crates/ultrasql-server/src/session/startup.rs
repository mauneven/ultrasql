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
        let (major, minor) = match msg {
            FrontendMessage::StartupMessage {
                protocol_major,
                protocol_minor,
                ..
            } => (protocol_major, protocol_minor),
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

        // AuthenticationOk — v0.5 has no real auth.
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
            ("session_authorization", "tester"),
            ("in_hot_standby", "off"),
        ];
        for (name, value) in params {
            self.send(&BackendMessage::ParameterStatus {
                name: name.to_string(),
                value: value.to_string(),
            })
            .await?;
        }
        // BackendKeyData — cancellation handle. Zeroed until we wire
        // an actual cancel-request handler.
        self.send(&BackendMessage::BackendKeyData {
            process_id: 0,
            secret_key: 0,
        })
        .await?;
        self.send(&BackendMessage::ReadyForQuery { status: b'I' })
            .await?;
        Ok(())
    }
}
