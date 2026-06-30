//! Physical streaming-replication command loop — the walsender side.
//!
//! A connection whose startup packet set the `replication` parameter is routed
//! here ([`Session::run_replication`]) instead of the SQL [`run`](Session::run)
//! loop. Phase 1 (the control plane) answers the replication-protocol commands a
//! standby issues before it streams:
//!
//! - `IDENTIFY_SYSTEM` — reports the system identifier, timeline, current WAL
//!   position, and database name as a one-row result set.
//! - `CREATE_REPLICATION_SLOT <name> PHYSICAL` — persists a physical
//!   [`ReplicationSlot`] whose `restart_lsn` pins the WAL recycle floor
//!   (`Server::maybe_recycle_wal`), so a lagging standby never loses WAL.
//! - `DROP_REPLICATION_SLOT <name>` — removes a slot, releasing its WAL pin.
//!
//! `START_REPLICATION` (the WAL byte stream over `CopyBoth`/`XLogData`) lands in
//! Phase 1b; until then it returns a defined `feature_not_supported` error so a
//! standby gets a clear answer rather than hanging. The commands arrive as
//! Simple Query (`'Q'`) messages and are parsed here directly, never through the
//! SQL parser/executor. See `docs/streaming-replication-design.md`.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_core::Lsn;
use ultrasql_protocol::{BackendMessage, FieldDescription, FrontendMessage};

use super::Session;
use crate::error::ServerError;
use crate::replication::{
    ReplicationSlot, ReplicationSlotStore, format_pg_lsn, validate_replication_slot_name,
};

/// PostgreSQL `text` type OID.
const TEXT_OID: u32 = 25;
/// PostgreSQL `int4` type OID.
const INT4_OID: u32 = 23;

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Physical-replication command loop, entered after startup for a
    /// connection opened with the `replication` parameter. `startup` has
    /// already sent the post-auth `ReadyForQuery`, so the standby drives the
    /// exchange; this loop sends a `ReadyForQuery` after each command.
    pub(crate) async fn run_replication(&mut self) -> Result<(), ServerError> {
        loop {
            let msg = match self.read_frontend().await {
                Ok(msg) => msg,
                // A standby closing the socket is an ordinary end of stream.
                Err(ServerError::UnexpectedEof) => return Ok(()),
                Err(e) => return Err(e),
            };
            match msg {
                FrontendMessage::Query { sql } => self.dispatch_replication_query(&sql).await?,
                FrontendMessage::Terminate => return Ok(()),
                // The extended-query protocol is not used on replication
                // connections in Phase 1; answer clearly and keep the session.
                _ => {
                    self.replication_error(
                        "unexpected message on a replication connection",
                        "08P01",
                    )
                    .await?;
                }
            }
        }
    }

    /// Parse and dispatch one replication command (a Simple Query).
    async fn dispatch_replication_query(&mut self, sql: &str) -> Result<(), ServerError> {
        let command = sql.trim().trim_end_matches(';').trim();
        let verb = command
            .split_whitespace()
            .next()
            .unwrap_or("")
            .to_ascii_uppercase();
        match verb.as_str() {
            "IDENTIFY_SYSTEM" => self.reply_identify_system().await,
            "CREATE_REPLICATION_SLOT" => self.reply_create_replication_slot(command).await,
            "DROP_REPLICATION_SLOT" => self.reply_drop_replication_slot(command).await,
            "START_REPLICATION" => {
                // Phase 1b. Defined error so a standby gets a clear response.
                self.replication_error(
                    "START_REPLICATION (physical WAL streaming) is not yet enabled on this server",
                    "0A000",
                )
                .await
            }
            "" => {
                self.send(&BackendMessage::EmptyQueryResponse).await?;
                self.finish_command_ready().await
            }
            other => {
                self.replication_error(
                    &format!("unsupported replication command: {other}"),
                    "0A000",
                )
                .await
            }
        }
    }

    /// `IDENTIFY_SYSTEM`: one row of (systemid, timeline, xlogpos, dbname).
    async fn reply_identify_system(&mut self) -> Result<(), ServerError> {
        let systemid = self.system_identifier();
        let xlogpos = format_pg_lsn(self.current_wal_lsn());
        self.send(&BackendMessage::RowDescription {
            fields: vec![
                text_field("systemid"),
                int4_field("timeline"),
                text_field("xlogpos"),
                text_field("dbname"),
            ],
        })
        .await?;
        self.send(&BackendMessage::DataRow {
            columns: vec![
                Some(systemid.into_bytes()),
                Some(b"1".to_vec()), // single timeline until Phase 5 (failover)
                Some(xlogpos.into_bytes()),
                None, // dbname is NULL for a physical replication connection
            ],
        })
        .await?;
        self.send(&BackendMessage::CommandComplete {
            tag: "IDENTIFY_SYSTEM".to_owned(),
        })
        .await?;
        self.finish_command_ready().await
    }

    /// `CREATE_REPLICATION_SLOT <name> [TEMPORARY] PHYSICAL [...]`: persist a
    /// physical slot pinned at the current WAL position, then return the
    /// (slot_name, consistent_point, snapshot_name, output_plugin) row.
    async fn reply_create_replication_slot(&mut self, command: &str) -> Result<(), ServerError> {
        let mut tokens = command.split_whitespace();
        let _verb = tokens.next();
        let Some(raw_name) = tokens.next() else {
            return self
                .replication_error("CREATE_REPLICATION_SLOT requires a slot name", "42601")
                .await;
        };
        let name = unquote_ident(raw_name);
        if let Err(e) = validate_replication_slot_name(&name) {
            return self.replication_error(&e.to_string(), "42602").await;
        }
        let upper = command.to_ascii_uppercase();
        if upper.contains(" LOGICAL") {
            return self
                .replication_error(
                    "logical replication slots are not supported (physical only)",
                    "0A000",
                )
                .await;
        }
        if !upper.split_whitespace().any(|t| t == "PHYSICAL") {
            return self
                .replication_error("CREATE_REPLICATION_SLOT requires PHYSICAL", "42601")
                .await;
        }
        let Some(store) = self.replication_slot_store() else {
            return self
                .replication_error(
                    "replication slots require a persistent data directory",
                    "55000",
                )
                .await;
        };
        match store.list() {
            Ok(slots) if slots.iter().any(|s| s.name == name) => {
                return self
                    .replication_error(
                        &format!("replication slot \"{name}\" already exists"),
                        "42710",
                    )
                    .await;
            }
            Ok(_) => {}
            Err(e) => {
                return self
                    .replication_error(&format!("could not read replication slots: {e}"), "58000")
                    .await;
            }
        }

        let consistent_point = self.current_wal_lsn();
        let mut slot = ReplicationSlot::new(&name);
        slot.restart_lsn = Some(format_pg_lsn(consistent_point));
        if let Err(e) = store.save(&slot) {
            return self
                .replication_error(&format!("could not create replication slot: {e}"), "58000")
                .await;
        }

        self.send(&BackendMessage::RowDescription {
            fields: vec![
                text_field("slot_name"),
                text_field("consistent_point"),
                text_field("snapshot_name"),
                text_field("output_plugin"),
            ],
        })
        .await?;
        self.send(&BackendMessage::DataRow {
            columns: vec![
                Some(name.into_bytes()),
                Some(format_pg_lsn(consistent_point).into_bytes()),
                None, // snapshot_name: physical slots export no snapshot
                None, // output_plugin: NULL for physical slots
            ],
        })
        .await?;
        self.send(&BackendMessage::CommandComplete {
            tag: "CREATE_REPLICATION_SLOT".to_owned(),
        })
        .await?;
        self.finish_command_ready().await
    }

    /// `DROP_REPLICATION_SLOT <name> [WAIT]`: remove a slot, releasing its WAL
    /// pin. Errors if the slot does not exist.
    async fn reply_drop_replication_slot(&mut self, command: &str) -> Result<(), ServerError> {
        let mut tokens = command.split_whitespace();
        let _verb = tokens.next();
        let Some(raw_name) = tokens.next() else {
            return self
                .replication_error("DROP_REPLICATION_SLOT requires a slot name", "42601")
                .await;
        };
        let name = unquote_ident(raw_name);
        let Some(store) = self.replication_slot_store() else {
            return self
                .replication_error(
                    "replication slots require a persistent data directory",
                    "55000",
                )
                .await;
        };
        match store.drop_slot(&name) {
            Ok(true) => {
                self.send(&BackendMessage::CommandComplete {
                    tag: "DROP_REPLICATION_SLOT".to_owned(),
                })
                .await?;
                self.finish_command_ready().await
            }
            Ok(false) => {
                self.replication_error(
                    &format!("replication slot \"{name}\" does not exist"),
                    "42704",
                )
                .await
            }
            Err(e) => {
                self.replication_error(&format!("could not drop replication slot: {e}"), "58000")
                    .await
            }
        }
    }

    /// The physical replication slot store under `<data_dir>/pg_replslot`, or
    /// `None` in in-memory mode (no data directory).
    fn replication_slot_store(&self) -> Option<ReplicationSlotStore> {
        let dir = self.state.data_dir.as_ref()?.join("pg_replslot");
        ReplicationSlotStore::open(dir).ok()
    }

    /// Current durable WAL position, or `Lsn::ZERO` in in-memory mode.
    fn current_wal_lsn(&self) -> Lsn {
        self.state.runtime_wal_flushed_lsn().unwrap_or(Lsn::ZERO)
    }

    /// A stable 64-bit system identifier (decimal text, like PostgreSQL). It is
    /// derived from the data directory so it is constant across restarts of the
    /// same instance; in-memory mode uses a fixed sentinel.
    fn system_identifier(&self) -> String {
        let mut hasher = DefaultHasher::new();
        match &self.state.data_dir {
            Some(dir) => dir.hash(&mut hasher),
            None => "ultrasql-in-memory".hash(&mut hasher),
        }
        hasher.finish().to_string()
    }

    /// Send a wire error then the trailing `ReadyForQuery`, mirroring the
    /// simple-query error recovery so the standby can issue the next command.
    async fn replication_error(
        &mut self,
        message: &str,
        sqlstate: &str,
    ) -> Result<(), ServerError> {
        self.send_error(message, sqlstate).await?;
        self.finish_command_ready().await
    }

    /// Send `ReadyForQuery` with idle status, ending one command's reply.
    async fn finish_command_ready(&mut self) -> Result<(), ServerError> {
        self.send(&BackendMessage::ReadyForQuery { status: b'I' })
            .await
    }
}

/// Strip surrounding double quotes from a replication identifier and unescape
/// any doubled inner quotes (PostgreSQL quoted-identifier rules).
fn unquote_ident(raw: &str) -> String {
    let raw = raw.trim();
    if raw.len() >= 2 && raw.starts_with('"') && raw.ends_with('"') {
        raw[1..raw.len() - 1].replace("\"\"", "\"")
    } else {
        raw.to_owned()
    }
}

fn text_field(name: &str) -> FieldDescription {
    FieldDescription {
        name: name.to_owned(),
        table_oid: 0,
        col_attnum: 0,
        type_oid: TEXT_OID,
        type_size: -1,
        type_modifier: -1,
        format_code: 0,
    }
}

fn int4_field(name: &str) -> FieldDescription {
    FieldDescription {
        name: name.to_owned(),
        table_oid: 0,
        col_attnum: 0,
        type_oid: INT4_OID,
        type_size: 4,
        type_modifier: -1,
        format_code: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::unquote_ident;

    #[test]
    fn unquote_ident_strips_and_unescapes() {
        assert_eq!(unquote_ident("slot1"), "slot1");
        assert_eq!(unquote_ident("  slot1  "), "slot1");
        assert_eq!(unquote_ident("\"My Slot\""), "My Slot");
        assert_eq!(unquote_ident("\"a\"\"b\""), "a\"b");
        assert_eq!(unquote_ident("\"\""), "");
    }
}
