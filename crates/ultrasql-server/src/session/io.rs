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
    pub(crate) async fn read_frontend(&mut self) -> Result<FrontendMessage, ServerError> {
        loop {
            if let Some(msg) = decode_frontend(&mut self.read_buf)? {
                return Ok(msg);
            }
            // Pull more bytes from the socket.
            let n = self.io.read_buf(&mut self.read_buf).await?;
            if n == 0 {
                return Err(ServerError::UnexpectedEof);
            }
        }
    }

    /// Encode and flush a single backend message.
    pub(crate) async fn send(&mut self, msg: &BackendMessage) -> Result<(), ServerError> {
        self.write_buf.clear();
        encode_backend(msg, &mut self.write_buf);
        self.io.write_all(&self.write_buf).await?;
        self.io.flush().await?;
        Ok(())
    }

    /// Encode every message in `msgs` into the connection's write
    /// buffer and dispatch it in a single `write_all` + `flush`.
    ///
    /// The naïve `for msg in msgs { self.send(msg).await? }` loop
    /// issues one `write_all` + one `flush` per message. For a SELECT
    /// that emits a `RowDescription`, N `DataRow`s, and a
    /// `CommandComplete`, that is N+2 syscall round-trips per query
    /// (or N+2 reactor wake-ups on the loopback path used by the
    /// bench harness) — which dominates wall-clock time on
    /// `select_scan_10k`. Coalescing collapses the dispatch to a
    /// single round-trip without changing wire semantics, since
    /// PostgreSQL's protocol does not require message-boundary flushes
    /// between `RowDescription` / `DataRow` / `CommandComplete`.
    pub(crate) async fn send_messages_coalesced(
        &mut self,
        msgs: &[BackendMessage],
    ) -> Result<(), ServerError> {
        self.write_buf.clear();
        for msg in msgs {
            encode_backend(msg, &mut self.write_buf);
        }
        if !self.write_buf.is_empty() {
            self.io.write_all(&self.write_buf).await?;
            self.io.flush().await?;
        }
        Ok(())
    }

    /// Write the raw bytes of one or more already-encoded backend
    /// messages to the socket in a single `write_all` + `flush`.
    ///
    /// Used by the SELECT streaming path
    /// ([`result_encoder::stream_select`]) which builds the wire bytes
    /// directly into a scratch `BytesMut` to avoid materialising
    /// `BackendMessage::DataRow` enums for every row of a large scan.
    pub(crate) async fn send_raw(&mut self, bytes: &[u8]) -> Result<(), ServerError> {
        if !bytes.is_empty() {
            self.io.write_all(bytes).await?;
            self.io.flush().await?;
        }
        Ok(())
    }

    /// Send a PostgreSQL-compatible `ErrorResponse`. The fields are
    /// the minimal set every libpq client expects: severity, code,
    /// message.
    pub(crate) async fn send_error(
        &mut self,
        message: &str,
        sqlstate: &str,
    ) -> Result<(), ServerError> {
        let msg = BackendMessage::ErrorResponse {
            fields: vec![
                (b'S', "ERROR".to_string()),
                (b'C', sqlstate.to_string()),
                (b'M', message.to_string()),
            ],
        };
        self.send(&msg).await
    }
}
