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
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_core::Lsn;
use ultrasql_protocol::{BackendMessage, FieldDescription, FrontendMessage};
use ultrasql_wal::reader::read_wal_range;

use super::Session;
use crate::error::ServerError;
use crate::replication::{
    ReplicationSlot, ReplicationSlotStore, format_pg_lsn, parse_pg_lsn,
    validate_replication_slot_name,
};

/// PostgreSQL `text` type OID.
const TEXT_OID: u32 = 25;
/// PostgreSQL `int4` type OID.
const INT4_OID: u32 = 23;

/// How long the walsender waits for a standby message before shipping any
/// newly-durable WAL and sending a keepalive. Bounds idle keepalive cadence;
/// when WAL is flowing or the standby replies, the loop reacts immediately.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);

/// Seconds between the Unix epoch and the PostgreSQL epoch (2000-01-01 UTC).
const PG_EPOCH_UNIX_SECS: i64 = 946_684_800;

/// Maximum WAL bytes per `XLogData` frame. A single WAL record can reach
/// `ultrasql_wal::record::MAX_RECORD_BYTES` (64 MiB), which exceeds the wire
/// `MAX_MESSAGE_BYTES` (16 MiB) decoders enforce, so records are split into
/// frames bounded well under that limit. `XLogData` carries arbitrary WAL byte
/// ranges, so a record may legitimately span several frames.
const MAX_XLOGDATA_PAYLOAD: usize = 1024 * 1024;

/// Outcome of an attempt to ship WAL: a WAL-read failure becomes a wire
/// `ErrorResponse` (the standby is told and the copy ends), whereas a socket
/// error tears the connection down.
enum StreamError {
    /// WAL could not be read (e.g. requested start is below the recycle floor).
    Wal(String),
    /// Underlying socket I/O failure.
    Io(ServerError),
}

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
            "START_REPLICATION" => self.reply_start_replication(command).await,
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

    /// `START_REPLICATION [SLOT <name>] PHYSICAL <lsn> [TIMELINE <n>]`: begin a
    /// `CopyBoth` stream of WAL from `<lsn>` framed as `XLogData`, shipping
    /// newly-durable WAL on a timer, sending keepalives when idle, and advancing
    /// a named slot's `restart_lsn` from the standby's flush acknowledgements.
    /// Ends on `CopyDone`, `Terminate`, or the standby closing the connection.
    async fn reply_start_replication(&mut self, command: &str) -> Result<(), ServerError> {
        let (slot_name, start_lsn) = match parse_start_replication(command) {
            Ok(parsed) => parsed,
            Err(msg) => return self.replication_error(&msg, "42601").await,
        };
        let Some(wal_dir) = self.state.wal_dir.clone() else {
            return self
                .replication_error(
                    "WAL streaming requires a persistent data directory",
                    "55000",
                )
                .await;
        };
        // A SLOT-based stream requires the slot to exist (PostgreSQL semantics).
        if let Some(name) = &slot_name {
            let exists = self
                .replication_slot_store()
                .and_then(|store| store.list().ok())
                .is_some_and(|slots| slots.iter().any(|slot| &slot.name == name));
            if !exists {
                return self
                    .replication_error(
                        &format!("replication slot \"{name}\" does not exist"),
                        "42704",
                    )
                    .await;
            }
        }

        // Enter the bidirectional copy stream.
        self.send(&BackendMessage::CopyBothResponse {
            overall_format: 0,
            column_formats: Vec::new(),
        })
        .await?;

        let mut sent = start_lsn;
        // Initial catch-up: ship everything durable from the requested LSN.
        match self.ship_wal(&wal_dir, sent, self.current_wal_lsn()).await {
            Ok(next) => sent = next,
            Err(StreamError::Wal(msg)) => return self.replication_error(&msg, "XX000").await,
            Err(StreamError::Io(e)) => return Err(e),
        }

        // Service loop: react to standby messages, otherwise ship new WAL and
        // send a keepalive once the idle interval elapses.
        loop {
            match tokio::time::timeout(KEEPALIVE_INTERVAL, self.read_frontend()).await {
                Ok(Ok(FrontendMessage::CopyData(payload))) => {
                    if let (Some(name), Some(flush)) =
                        (slot_name.as_deref(), parse_standby_flush_lsn(&payload))
                    {
                        self.advance_slot(name, flush);
                    }
                    match self.ship_wal(&wal_dir, sent, self.current_wal_lsn()).await {
                        Ok(next) => sent = next,
                        Err(StreamError::Wal(msg)) => {
                            return self.replication_error(&msg, "XX000").await;
                        }
                        Err(StreamError::Io(e)) => return Err(e),
                    }
                }
                Ok(Ok(FrontendMessage::CopyDone)) => {
                    self.send(&BackendMessage::CopyDone).await?;
                    self.send(&BackendMessage::CommandComplete {
                        tag: "START_REPLICATION".to_owned(),
                    })
                    .await?;
                    return self.finish_command_ready().await;
                }
                Ok(Ok(FrontendMessage::Terminate)) => return Ok(()),
                // Any other message mid-stream is ignored (a tolerant standby
                // may send Sync/Flush); keep streaming.
                Ok(Ok(_)) => {}
                Ok(Err(ServerError::UnexpectedEof)) => return Ok(()),
                Ok(Err(e)) => return Err(e),
                Err(_elapsed) => {
                    let durable = self.current_wal_lsn();
                    match self.ship_wal(&wal_dir, sent, durable).await {
                        Ok(next) => sent = next,
                        Err(StreamError::Wal(msg)) => {
                            return self.replication_error(&msg, "XX000").await;
                        }
                        Err(StreamError::Io(e)) => return Err(e),
                    }
                    self.send(&BackendMessage::CopyData(build_keepalive(durable, true)))
                        .await?;
                }
            }
        }
    }

    /// Ship WAL records in `[from, to)` as `XLogData` `CopyData` frames. Each
    /// record's raw on-disk bytes are sent verbatim, split into chunks no larger
    /// than [`MAX_XLOGDATA_PAYLOAD`] so a large record cannot produce a frame
    /// that exceeds the wire message limit. Records are contiguous, so a chunk's
    /// `dataStart` is its first byte's LSN (`record.lsn + offset`). Returns the
    /// LSN to resume from. Reads only already-durable bytes.
    async fn ship_wal(&mut self, wal_dir: &Path, from: Lsn, to: Lsn) -> Result<Lsn, StreamError> {
        if to.raw() <= from.raw() {
            return Ok(from);
        }
        let stream = read_wal_range(wal_dir, from, to)
            .map_err(|e| StreamError::Wal(format!("WAL read failed: {e}")))?;
        for record in &stream.records {
            let base = record.lsn.raw();
            for (offset, end) in chunk_ranges(record.bytes.len(), MAX_XLOGDATA_PAYLOAD) {
                let data_start = Lsn::new(base + offset as u64);
                let frame = build_xlogdata(data_start, to, &record.bytes[offset..end]);
                self.send(&BackendMessage::CopyData(frame))
                    .await
                    .map_err(StreamError::Io)?;
            }
        }
        Ok(stream.next_lsn)
    }

    /// Advance a physical slot's `restart_lsn`/`confirmed_flush_lsn` to the
    /// standby's acknowledged flush position, letting the WAL recycle floor move
    /// up as the standby catches up.
    ///
    /// Advancement is monotonic: a report at or below the current position is
    /// ignored, matching PostgreSQL's `PhysicalConfirmReceivedLocation`. (A
    /// backward report is not itself unsafe — it would only lower the recycle
    /// floor and *over*-retain WAL — but keeping `restart_lsn` monotonic avoids
    /// needless floor churn from a replayed or out-of-order status update.)
    /// Best-effort: a persistence failure only over-retains WAL, never loses it,
    /// so it is logged rather than fatal.
    fn advance_slot(&self, name: &str, flush: Lsn) {
        let Some(store) = self.replication_slot_store() else {
            return;
        };
        let Ok(mut slot) = store.get_or_create(name) else {
            return;
        };
        let current = slot
            .restart_lsn
            .as_deref()
            .and_then(parse_pg_lsn)
            .map_or(0, |lsn| lsn.raw());
        if flush.raw() <= current {
            return;
        }
        let text = format_pg_lsn(flush);
        slot.restart_lsn = Some(text.clone());
        slot.confirmed_flush_lsn = Some(text);
        if let Err(e) = store.save(&slot) {
            tracing::warn!(slot = name, error = %e, "failed to persist standby flush position");
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

/// Parse `START_REPLICATION [SLOT <name>] PHYSICAL <lsn> [TIMELINE <n>]`,
/// returning the optional slot name and the start LSN.
fn parse_start_replication(command: &str) -> Result<(Option<String>, Lsn), String> {
    let tokens: Vec<&str> = command.split_whitespace().collect();
    let mut i = 1; // skip the START_REPLICATION verb
    let mut slot = None;
    if tokens
        .get(i)
        .is_some_and(|t| t.eq_ignore_ascii_case("SLOT"))
    {
        let name = tokens
            .get(i + 1)
            .ok_or_else(|| "START_REPLICATION SLOT requires a slot name".to_owned())?;
        slot = Some(unquote_ident(name));
        i += 2;
    }
    match tokens.get(i) {
        Some(t) if t.eq_ignore_ascii_case("PHYSICAL") => i += 1,
        Some(t) if t.eq_ignore_ascii_case("LOGICAL") => {
            return Err("logical replication is not supported (physical only)".to_owned());
        }
        _ => return Err("START_REPLICATION requires PHYSICAL".to_owned()),
    }
    let lsn = tokens
        .get(i)
        .ok_or_else(|| "START_REPLICATION requires a start LSN".to_owned())
        .and_then(|t| parse_pg_lsn(t).ok_or_else(|| format!("invalid start LSN: {t}")))?;
    Ok((slot, lsn))
}

/// Split a `[0, len)` byte span into consecutive `(start, end)` ranges no larger
/// than `max`. Empty input yields no ranges; `max` must be non-zero.
fn chunk_ranges(len: usize, max: usize) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut offset = 0;
    while offset < len {
        let end = (offset + max).min(len);
        ranges.push((offset, end));
        offset = end;
    }
    ranges
}

/// Microseconds since the PostgreSQL epoch (2000-01-01 UTC), for the `XLogData`
/// / keepalive `sendTime` field. Clamps to 0 if the clock predates the epoch.
fn pg_send_timestamp() -> i64 {
    let Ok(since_unix) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    let unix_micros = i64::try_from(since_unix.as_micros()).unwrap_or(i64::MAX);
    unix_micros.saturating_sub(PG_EPOCH_UNIX_SECS.saturating_mul(1_000_000))
}

/// Build an `XLogData` (`'w'`) CopyData payload: `'w'`, Int64 dataStart, Int64
/// walEnd, Int64 sendTime, then the raw WAL record bytes.
fn build_xlogdata(data_start: Lsn, wal_end: Lsn, wal_bytes: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(25 + wal_bytes.len());
    payload.push(b'w');
    payload.extend_from_slice(&data_start.raw().to_be_bytes());
    payload.extend_from_slice(&wal_end.raw().to_be_bytes());
    payload.extend_from_slice(&pg_send_timestamp().to_be_bytes());
    payload.extend_from_slice(wal_bytes);
    payload
}

/// Build a keepalive (`'k'`) CopyData payload: `'k'`, Int64 walEnd, Int64
/// sendTime, Byte1 replyRequested.
fn build_keepalive(wal_end: Lsn, reply_requested: bool) -> Vec<u8> {
    let mut payload = Vec::with_capacity(18);
    payload.push(b'k');
    payload.extend_from_slice(&wal_end.raw().to_be_bytes());
    payload.extend_from_slice(&pg_send_timestamp().to_be_bytes());
    payload.push(u8::from(reply_requested));
    payload
}

/// Extract the flush LSN from a standby status update (`'r'`) CopyData payload:
/// `'r'`, Int64 writeLSN, Int64 flushLSN, Int64 applyLSN, Int64 clientTime,
/// Byte1 replyRequested. Returns `None` for any other client message shape.
fn parse_standby_flush_lsn(payload: &[u8]) -> Option<Lsn> {
    if payload.first() != Some(&b'r') || payload.len() < 1 + 8 * 4 + 1 {
        return None;
    }
    let flush = u64::from_be_bytes(payload[9..17].try_into().ok()?);
    Some(Lsn::new(flush))
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
    use ultrasql_core::Lsn;

    use super::{
        build_keepalive, build_xlogdata, chunk_ranges, parse_standby_flush_lsn,
        parse_start_replication, unquote_ident,
    };

    #[test]
    fn chunk_ranges_splits_at_the_max_and_covers_exactly() {
        assert_eq!(chunk_ranges(0, 4), Vec::<(usize, usize)>::new());
        assert_eq!(chunk_ranges(3, 4), vec![(0, 3)]); // smaller than max → one chunk
        assert_eq!(chunk_ranges(4, 4), vec![(0, 4)]); // exactly max → one chunk
        assert_eq!(chunk_ranges(10, 4), vec![(0, 4), (4, 8), (8, 10)]); // splits, gap-free
        // The ranges tile [0, len) contiguously with no overlap or gap.
        let ranges = chunk_ranges(10, 4);
        assert_eq!(ranges.first().unwrap().0, 0);
        assert_eq!(ranges.last().unwrap().1, 10);
        for pair in ranges.windows(2) {
            assert_eq!(pair[0].1, pair[1].0);
        }
    }

    #[test]
    fn unquote_ident_strips_and_unescapes() {
        assert_eq!(unquote_ident("slot1"), "slot1");
        assert_eq!(unquote_ident("  slot1  "), "slot1");
        assert_eq!(unquote_ident("\"My Slot\""), "My Slot");
        assert_eq!(unquote_ident("\"a\"\"b\""), "a\"b");
        assert_eq!(unquote_ident("\"\""), "");
    }

    #[test]
    fn parse_start_replication_handles_slot_and_physical() {
        let (slot, lsn) =
            parse_start_replication("START_REPLICATION SLOT s1 PHYSICAL 0/0").unwrap();
        assert_eq!(slot.as_deref(), Some("s1"));
        assert_eq!(lsn, Lsn::new(0));

        let (slot, lsn) = parse_start_replication("START_REPLICATION PHYSICAL 1/A2B").unwrap();
        assert_eq!(slot, None);
        assert_eq!(lsn, Lsn::new((1u64 << 32) | 0xA2B));

        // A trailing TIMELINE clause is accepted and ignored in Phase 1.
        let (slot, _) =
            parse_start_replication("START_REPLICATION SLOT s1 PHYSICAL 0/10 TIMELINE 1").unwrap();
        assert_eq!(slot.as_deref(), Some("s1"));
    }

    #[test]
    fn parse_start_replication_rejects_logical_and_malformed() {
        assert!(parse_start_replication("START_REPLICATION SLOT s1 LOGICAL 0/0").is_err());
        assert!(parse_start_replication("START_REPLICATION 0/0").is_err()); // missing PHYSICAL
        assert!(parse_start_replication("START_REPLICATION PHYSICAL").is_err()); // missing LSN
        assert!(parse_start_replication("START_REPLICATION PHYSICAL zzz").is_err()); // bad LSN
        assert!(parse_start_replication("START_REPLICATION SLOT").is_err()); // missing name
    }

    #[test]
    fn build_xlogdata_has_w_tag_and_25_byte_header() {
        let wal = [1_u8, 2, 3, 4];
        let frame = build_xlogdata(Lsn::new(0x10), Lsn::new(0x20), &wal);
        assert_eq!(frame[0], b'w');
        assert_eq!(frame.len(), 25 + wal.len());
        assert_eq!(&frame[1..9], 0x10_u64.to_be_bytes().as_slice()); // dataStart
        assert_eq!(&frame[9..17], 0x20_u64.to_be_bytes().as_slice()); // walEnd
        assert_eq!(&frame[25..], wal.as_slice()); // trailing WAL bytes verbatim
    }

    #[test]
    fn build_keepalive_has_k_tag_and_reply_flag() {
        let frame = build_keepalive(Lsn::new(0x99), true);
        assert_eq!(frame[0], b'k');
        assert_eq!(&frame[1..9], 0x99_u64.to_be_bytes().as_slice());
        assert_eq!(frame.len(), 18);
        assert_eq!(frame[17], 1); // reply requested
        assert_eq!(build_keepalive(Lsn::new(0), false)[17], 0);
    }

    #[test]
    fn parse_standby_flush_lsn_reads_the_flush_field() {
        let mut payload = vec![b'r'];
        payload.extend_from_slice(&0x111_u64.to_be_bytes()); // write
        payload.extend_from_slice(&0x222_u64.to_be_bytes()); // flush
        payload.extend_from_slice(&0x333_u64.to_be_bytes()); // apply
        payload.extend_from_slice(&0_i64.to_be_bytes()); // client time
        payload.push(0); // reply requested
        assert_eq!(parse_standby_flush_lsn(&payload), Some(Lsn::new(0x222)));
        // Non-'r' or too-short payloads are ignored.
        assert_eq!(parse_standby_flush_lsn(b"k------"), None);
        assert_eq!(parse_standby_flush_lsn(&[b'r', 1, 2]), None);
    }
}
