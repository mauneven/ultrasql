//! File-backed physical replication and same-process logical CDC primitives.
//!
//! These helpers implement deterministic WAL shipping against archived WAL
//! files. They are intentionally small and synchronous so the CLI, tests, and
//! future network WAL sender can share the same slot-state rules.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::error::ServerError;

const DEFAULT_REPLICATION_METADATA_FILE_LIMIT_BYTES: u64 = 1024 * 1024;
const REPLICATION_METADATA_FILE_LIMIT_ENV: &str = "ULTRASQL_REPLICATION_METADATA_FILE_LIMIT_BYTES";

/// Persistent replication slot state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicationSlot {
    /// Slot name.
    pub name: String,
    /// Last WAL filename shipped from this slot.
    pub restart_lsn: Option<String>,
    /// Last WAL filename acknowledged by the receiver.
    pub confirmed_flush_lsn: Option<String>,
}

impl ReplicationSlot {
    /// Create a new empty physical slot.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            restart_lsn: None,
            confirmed_flush_lsn: None,
        }
    }
}

/// Operation class carried by a logical CDC record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalChangeKind {
    /// Rows inserted into a published table.
    Insert,
    /// Rows updated in a published table.
    Update,
    /// Rows deleted from a published table.
    Delete,
}

/// `CREATE PUBLICATION` metadata kept by the in-process server runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Publication {
    /// Case-folded publication name.
    pub name: String,
    tables: BTreeSet<String>,
}

/// `CREATE SUBSCRIPTION` metadata kept by the in-process server runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subscription {
    /// Case-folded subscription name.
    pub name: String,
    /// Connection string supplied by SQL.
    pub conninfo: String,
    /// Replication slot name.
    pub slot_name: String,
    /// Publication names subscribed to, in deterministic order.
    pub publications: Vec<String>,
    /// Whether subscription apply is enabled.
    pub enabled: bool,
}

impl Publication {
    /// Return `true` when this publication includes `table`.
    #[must_use]
    pub fn publishes_table(&self, table: &str) -> bool {
        self.tables.contains(&fold_identifier(table))
    }

    /// Return published table names in deterministic order.
    pub fn tables(&self) -> impl Iterator<Item = &str> {
        self.tables.iter().map(String::as_str)
    }
}

/// One committed logical change emitted for a published table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalChange {
    /// Monotonic in-process logical sequence number.
    pub lsn: u64,
    /// Publication that emitted this change.
    pub publication: String,
    /// Case-folded table name.
    pub table: String,
    /// DML operation class.
    pub kind: LogicalChangeKind,
    /// Number of committed rows affected by the statement.
    pub rows_affected: u64,
}

/// Durable logical decoding slot progress.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalReplicationSlot {
    /// Slot name.
    pub name: String,
    /// Last logical change LSN consumed by this slot.
    pub confirmed_lsn: u64,
}

/// Same-process logical replication runtime.
///
/// This is the first CDC layer: it records committed statement-level DML
/// changes for tables named by `CREATE PUBLICATION ... FOR TABLE ...`.
/// Row-image decoding and external replication slots can layer on this
/// commit-gated stream without changing transaction semantics.
#[derive(Debug)]
pub struct LogicalReplicationRuntime {
    publications: dashmap::DashMap<String, Publication>,
    has_publications: AtomicBool,
    subscriptions: dashmap::DashMap<String, Subscription>,
    logical_slots: dashmap::DashMap<String, LogicalReplicationSlot>,
    changes: parking_lot::Mutex<Vec<LogicalChange>>,
    next_lsn: AtomicU64,
    metadata_dir: Option<PathBuf>,
}

impl Default for LogicalReplicationRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalReplicationRuntime {
    /// Create an empty logical replication runtime.
    #[must_use]
    pub fn new() -> Self {
        Self {
            publications: dashmap::DashMap::new(),
            has_publications: AtomicBool::new(false),
            subscriptions: dashmap::DashMap::new(),
            logical_slots: dashmap::DashMap::new(),
            changes: parking_lot::Mutex::new(Vec::new()),
            next_lsn: AtomicU64::new(1),
            metadata_dir: None,
        }
    }

    /// Open a logical replication metadata store rooted at `metadata_dir`.
    pub fn open_metadata(metadata_dir: impl Into<PathBuf>) -> Result<Self, ServerError> {
        let runtime = Self {
            metadata_dir: Some(metadata_dir.into()),
            ..Self::new()
        };
        runtime.load_metadata()?;
        Ok(runtime)
    }

    /// Register a publication for explicit table names.
    pub fn create_publication(
        &self,
        name: &str,
        tables: Vec<String>,
    ) -> Result<Publication, ServerError> {
        let name = fold_identifier(name);
        if name.is_empty() {
            return Err(ServerError::ddl("CREATE PUBLICATION requires a name"));
        }
        if self.publications.contains_key(&name) {
            return Err(ServerError::ddl(format!(
                "publication \"{name}\" already exists"
            )));
        }
        let tables = tables
            .into_iter()
            .map(|table| fold_identifier(&table))
            .filter(|table| !table.is_empty())
            .collect::<BTreeSet<_>>();
        if tables.is_empty() {
            return Err(ServerError::ddl(
                "CREATE PUBLICATION requires at least one table",
            ));
        }
        let publication = Publication {
            name: name.clone(),
            tables,
        };
        self.persist_publication(&publication)?;
        self.publications.insert(name, publication.clone());
        self.has_publications.store(true, Ordering::Release);
        Ok(publication)
    }

    /// Remove a publication, returning whether it existed.
    ///
    /// Metadata-backed runtimes delete durable state before mutating in-memory
    /// state so a failed drop cannot resurrect after restart.
    pub fn drop_publication(&self, name: &str) -> Result<bool, ServerError> {
        let name = fold_identifier(name);
        if self.publications.get(&name).is_none() {
            return Ok(false);
        }
        self.remove_metadata_file("publications", &name)?;
        let removed = self.publications.remove(&name).is_some();
        if removed && self.publications.is_empty() {
            self.has_publications.store(false, Ordering::Release);
        }
        Ok(removed)
    }

    /// Look up a publication by name.
    #[must_use]
    pub fn publication(&self, name: &str) -> Option<Publication> {
        self.publications
            .get(&fold_identifier(name))
            .map(|entry| entry.value().clone())
    }

    /// Return `true` if any publication exists.
    #[must_use]
    pub fn has_publications(&self) -> bool {
        self.has_publications.load(Ordering::Acquire)
    }

    /// Return publications in deterministic name order.
    #[must_use]
    pub fn publications(&self) -> Vec<Publication> {
        let mut publications = self
            .publications
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        publications.sort_by(|left, right| left.name.cmp(&right.name));
        publications
    }

    /// Register a subscription for explicit publication names.
    pub fn create_subscription(
        &self,
        name: &str,
        conninfo: &str,
        publications: Vec<String>,
        slot_name: Option<String>,
    ) -> Result<Subscription, ServerError> {
        let name = fold_identifier(name);
        if name.is_empty() {
            return Err(ServerError::ddl("CREATE SUBSCRIPTION requires a name"));
        }
        if self.subscriptions.contains_key(&name) {
            return Err(ServerError::ddl(format!(
                "subscription \"{name}\" already exists"
            )));
        }
        let mut publications = publications
            .into_iter()
            .map(|publication| fold_identifier(&publication))
            .filter(|publication| !publication.is_empty())
            .collect::<Vec<_>>();
        publications.sort();
        publications.dedup();
        if publications.is_empty() {
            return Err(ServerError::ddl(
                "CREATE SUBSCRIPTION requires at least one publication",
            ));
        }
        for publication in &publications {
            if !self.publications.contains_key(publication) {
                return Err(ServerError::ddl(format!(
                    "publication \"{publication}\" does not exist"
                )));
            }
        }
        let slot_name = slot_name
            .map(|slot| fold_identifier(&slot))
            .filter(|slot| !slot.is_empty())
            .unwrap_or_else(|| name.clone());
        let subscription = Subscription {
            name: name.clone(),
            conninfo: conninfo.to_string(),
            slot_name,
            publications,
            enabled: true,
        };
        self.persist_subscription(&subscription)?;
        self.subscriptions.insert(name, subscription.clone());
        Ok(subscription)
    }

    /// Remove a subscription, returning whether it existed.
    ///
    /// Metadata-backed runtimes delete durable state before mutating in-memory
    /// state so a failed drop cannot resurrect after restart.
    pub fn drop_subscription(&self, name: &str) -> Result<bool, ServerError> {
        let name = fold_identifier(name);
        if self.subscriptions.get(&name).is_none() {
            return Ok(false);
        }
        self.remove_metadata_file("subscriptions", &name)?;
        Ok(self.subscriptions.remove(&name).is_some())
    }

    /// Return subscriptions in deterministic name order.
    #[must_use]
    pub fn subscriptions(&self) -> Vec<Subscription> {
        let mut subscriptions = self
            .subscriptions
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        subscriptions.sort_by(|left, right| left.name.cmp(&right.name));
        subscriptions
    }

    /// Create a durable logical decoding slot over the statement CDC stream.
    pub fn create_logical_slot(&self, name: &str) -> Result<LogicalReplicationSlot, ServerError> {
        let name = fold_identifier(name);
        if name.is_empty() {
            return Err(ServerError::ddl("logical replication slot requires a name"));
        }
        if self.logical_slots.contains_key(&name) {
            return Err(ServerError::ddl(format!(
                "logical replication slot \"{name}\" already exists"
            )));
        }
        let slot = LogicalReplicationSlot {
            name: name.clone(),
            confirmed_lsn: 0,
        };
        self.persist_logical_slot(&slot)?;
        self.logical_slots.insert(name, slot.clone());
        Ok(slot)
    }

    /// Return a logical decoding slot by name.
    #[must_use]
    pub fn logical_slot(&self, name: &str) -> Option<LogicalReplicationSlot> {
        self.logical_slots
            .get(&fold_identifier(name))
            .map(|entry| entry.value().clone())
    }

    /// Decode changes after a slot's confirmed LSN and advance the slot.
    pub fn decode_slot(&self, name: &str) -> Result<Vec<LogicalChange>, ServerError> {
        let name = fold_identifier(name);
        let Some(entry) = self.logical_slots.get(&name) else {
            return Err(ServerError::ddl(format!(
                "logical replication slot \"{name}\" does not exist"
            )));
        };
        let mut slot = entry.value().clone();
        drop(entry);
        let changes = self.changes_since(slot.confirmed_lsn);
        if let Some(last) = changes.last() {
            slot.confirmed_lsn = last.lsn;
            self.persist_logical_slot(&slot)?;
            self.logical_slots.insert(name, slot);
        }
        Ok(changes)
    }

    /// Return committed logical changes with `lsn` greater than `after_lsn`.
    #[must_use]
    pub fn changes_since(&self, after_lsn: u64) -> Vec<LogicalChange> {
        self.changes
            .lock()
            .iter()
            .filter(|change| change.lsn > after_lsn)
            .cloned()
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn set_next_lsn_for_test(&self, lsn: u64) {
        self.next_lsn.store(lsn, Ordering::Release);
    }

    /// Emit a committed statement-level DML change for matching publications.
    pub fn record_committed_dml(
        &self,
        table: &str,
        kind: LogicalChangeKind,
        rows_affected: u64,
    ) -> Result<(), ServerError> {
        if rows_affected == 0 {
            return Ok(());
        }
        let table = fold_identifier(table);
        if table.is_empty() {
            return Ok(());
        }
        let mut publications = self
            .publications
            .iter()
            .filter(|publication| publication.value().publishes_table(&table))
            .map(|publication| publication.key().clone())
            .collect::<Vec<_>>();
        if publications.is_empty() {
            return Ok(());
        }
        publications.sort();
        let mut changes = self.changes.lock();
        for publication in publications {
            let Ok(lsn) =
                self.next_lsn
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                        current.checked_add(1)
                    })
            else {
                return Err(ServerError::ddl(
                    "logical replication LSN space exhausted while recording committed DML",
                ));
            };
            changes.push(LogicalChange {
                lsn,
                publication,
                table: table.clone(),
                kind,
                rows_affected,
            });
        }
        Ok(())
    }

    fn load_metadata(&self) -> Result<(), ServerError> {
        let Some(root) = &self.metadata_dir else {
            return Ok(());
        };
        ensure_directory(root, "logical replication metadata directory")?;
        let publications_dir = root.join("publications");
        let subscriptions_dir = root.join("subscriptions");
        let logical_slots_dir = root.join("logical_slots");
        ensure_directory(&publications_dir, "logical publication metadata directory")?;
        ensure_directory(
            &subscriptions_dir,
            "logical subscription metadata directory",
        )?;
        ensure_directory(&logical_slots_dir, "logical slot metadata directory")?;
        for entry in fs::read_dir(&publications_dir).map_err(ServerError::Io)? {
            let entry = entry.map_err(ServerError::Io)?;
            if !entry.file_type().map_err(ServerError::Io)?.is_file() {
                continue;
            }
            let path = entry.path();
            let text = read_regular_text_file(&path, "logical publication metadata file")?;
            let publication = parse_publication_metadata(&text)?;
            let name = publication.name.clone();
            if self
                .publications
                .insert(name.clone(), publication)
                .is_some()
            {
                return Err(ServerError::ddl(format!(
                    "duplicate logical publication metadata: {name}"
                )));
            }
            self.has_publications.store(true, Ordering::Release);
        }
        for entry in fs::read_dir(&subscriptions_dir).map_err(ServerError::Io)? {
            let entry = entry.map_err(ServerError::Io)?;
            if !entry.file_type().map_err(ServerError::Io)?.is_file() {
                continue;
            }
            let path = entry.path();
            let text = read_regular_text_file(&path, "logical subscription metadata file")?;
            let subscription = parse_subscription_metadata(&text)?;
            let name = subscription.name.clone();
            if self
                .subscriptions
                .insert(name.clone(), subscription)
                .is_some()
            {
                return Err(ServerError::ddl(format!(
                    "duplicate logical subscription metadata: {name}"
                )));
            }
        }
        let mut max_confirmed = 0_u64;
        for entry in fs::read_dir(&logical_slots_dir).map_err(ServerError::Io)? {
            let entry = entry.map_err(ServerError::Io)?;
            if !entry.file_type().map_err(ServerError::Io)?.is_file() {
                continue;
            }
            let path = entry.path();
            let text = read_regular_text_file(&path, "logical slot metadata file")?;
            let slot = parse_logical_slot_metadata(&text)?;
            max_confirmed = max_confirmed.max(slot.confirmed_lsn);
            let name = slot.name.clone();
            if self.logical_slots.insert(name.clone(), slot).is_some() {
                return Err(ServerError::ddl(format!(
                    "duplicate logical slot metadata: {name}"
                )));
            }
        }
        let next_lsn = max_confirmed
            .checked_add(1)
            .ok_or(ServerError::ddl("logical replication LSN space exhausted"))?
            .max(1);
        self.next_lsn.store(next_lsn, Ordering::Release);
        Ok(())
    }

    fn persist_publication(&self, publication: &Publication) -> Result<(), ServerError> {
        let Some(root) = &self.metadata_dir else {
            return Ok(());
        };
        let dir = root.join("publications");
        ensure_directory(&dir, "logical publication metadata directory")?;
        let tables = publication.tables().collect::<Vec<_>>().join(",");
        let body = format!("name={}\ntables={tables}\n", publication.name);
        write_regular_text_file(&metadata_path(&dir, &publication.name), &body)
    }

    fn persist_subscription(&self, subscription: &Subscription) -> Result<(), ServerError> {
        let Some(root) = &self.metadata_dir else {
            return Ok(());
        };
        let dir = root.join("subscriptions");
        ensure_directory(&dir, "logical subscription metadata directory")?;
        let body = format!(
            "name={}\nconninfo={}\nslot_name={}\npublications={}\nenabled={}\n",
            subscription.name,
            subscription.conninfo.replace('\n', " "),
            subscription.slot_name,
            subscription.publications.join(","),
            subscription.enabled
        );
        write_regular_text_file(&metadata_path(&dir, &subscription.name), &body)
    }

    fn persist_logical_slot(&self, slot: &LogicalReplicationSlot) -> Result<(), ServerError> {
        let Some(root) = &self.metadata_dir else {
            return Ok(());
        };
        let dir = root.join("logical_slots");
        ensure_directory(&dir, "logical slot metadata directory")?;
        let body = format!("name={}\nconfirmed_lsn={}\n", slot.name, slot.confirmed_lsn);
        write_regular_text_file(&metadata_path(&dir, &slot.name), &body)
    }

    fn remove_metadata_file(&self, kind: &str, name: &str) -> Result<(), ServerError> {
        let Some(root) = &self.metadata_dir else {
            return Ok(());
        };
        let dir = root.join(kind);
        ensure_directory(&dir, "logical replication metadata directory")?;
        match fs::remove_file(metadata_path(&dir, name)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(ServerError::Io(e)),
        }
    }
}

fn write_regular_text_file(path: &Path, body: &str) -> Result<(), ServerError> {
    ensure_regular_metadata_write_slot(path)?;
    let tmp = path.with_extension("meta.tmp");
    ensure_regular_metadata_write_slot(&tmp)?;

    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    let mut file = options.open(&tmp).map_err(ServerError::Io)?;
    file.write_all(body.as_bytes()).map_err(ServerError::Io)?;
    file.sync_all().map_err(ServerError::Io)?;
    drop(file);
    fs::rename(&tmp, path).map_err(ServerError::Io)?;
    sync_metadata_parent(path)
}

fn ensure_regular_metadata_write_slot(path: &Path) -> Result<(), ServerError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.file_type().is_file() => {
            return Err(ServerError::ddl(format!(
                "refusing to write non-regular metadata file {}",
                path.display()
            )));
        }
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(ServerError::Io(err)),
    }
    Ok(())
}

fn sync_metadata_parent(path: &Path) -> Result<(), ServerError> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    sync_metadata_dir(parent)
}

#[cfg(unix)]
fn sync_metadata_dir(path: &Path) -> Result<(), ServerError> {
    let dir = File::open(path).map_err(ServerError::Io)?;
    match dir.sync_all() {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::InvalidInput => Ok(()),
        Err(err) => Err(ServerError::Io(err)),
    }
}

#[cfg(not(unix))]
fn sync_metadata_dir(_path: &Path) -> Result<(), ServerError> {
    Ok(())
}

fn read_regular_text_file(path: &Path, context: &str) -> Result<String, ServerError> {
    let limit = replication_metadata_file_limit_bytes();
    let file = open_regular_metadata_file(path)?;
    let metadata = file.metadata().map_err(ServerError::Io)?;
    if !metadata.file_type().is_file() {
        return Err(ServerError::ddl(format!(
            "{context} is not a regular file: {}",
            path.display()
        )));
    }
    if metadata.len() > limit {
        return Err(ServerError::Io(replication_metadata_limit_error(
            path,
            metadata.len(),
            limit,
        )));
    }

    let mut text = String::new();
    let mut limited = file.take(replication_metadata_take_limit(limit)?);
    limited.read_to_string(&mut text).map_err(ServerError::Io)?;
    let bytes_read = replication_metadata_bytes_read_len(text.len())?;
    if bytes_read > limit {
        return Err(ServerError::Io(replication_metadata_limit_error(
            path, bytes_read, limit,
        )));
    }
    Ok(text)
}

fn replication_metadata_file_limit_bytes() -> u64 {
    std::env::var(REPLICATION_METADATA_FILE_LIMIT_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|&limit| limit > 0)
        .unwrap_or(DEFAULT_REPLICATION_METADATA_FILE_LIMIT_BYTES)
}

fn replication_metadata_take_limit(limit: u64) -> Result<u64, ServerError> {
    limit.checked_add(1).ok_or_else(|| {
        ServerError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("replication metadata read limit is too large: limit={limit}"),
        ))
    })
}

fn replication_metadata_bytes_read_len(len: usize) -> Result<u64, ServerError> {
    u64::try_from(len).map_err(|_| {
        ServerError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("replication metadata byte count exceeds u64: bytes={len}"),
        ))
    })
}

fn replication_metadata_limit_error(path: &Path, bytes: u64, limit: u64) -> std::io::Error {
    std::io::Error::new(
        ErrorKind::InvalidData,
        format!(
            "replication metadata file exceeds read limit: path={} bytes={} limit={} env={}",
            path.display(),
            bytes,
            limit,
            REPLICATION_METADATA_FILE_LIMIT_ENV
        ),
    )
}

fn open_regular_metadata_file(path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW);
    options.open(path)
}

fn fold_identifier(ident: &str) -> String {
    ident
        .trim()
        .trim_matches('"')
        .trim_end_matches(';')
        .to_ascii_lowercase()
}

fn metadata_path(dir: &Path, name: &str) -> PathBuf {
    dir.join(format!("{}.meta", hex_name(name)))
}

fn hex_name(value: &str) -> String {
    let mut out = String::with_capacity(value.len().saturating_mul(2));
    for byte in value.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn metadata_field<'a>(text: &'a str, field: &str) -> Option<&'a str> {
    let prefix = format!("{field}=");
    text.lines()
        .find_map(|line| line.strip_prefix(prefix.as_str()))
}

fn metadata_required_field<'a>(
    text: &'a str,
    field: &str,
    kind: &str,
) -> Result<&'a str, ServerError> {
    metadata_field(text, field).ok_or_else(|| {
        ServerError::ddl(format!("malformed {kind} metadata: missing {field} field"))
    })
}

fn parse_publication_metadata(text: &str) -> Result<Publication, ServerError> {
    let name = fold_identifier(metadata_required_field(
        text,
        "name",
        "logical publication",
    )?);
    let tables = metadata_required_field(text, "tables", "logical publication")?
        .split(',')
        .map(fold_identifier)
        .filter(|table| !table.is_empty())
        .collect::<BTreeSet<_>>();
    if name.is_empty() || tables.is_empty() {
        return Err(ServerError::ddl(
            "malformed logical publication metadata: empty name or tables",
        ));
    }
    Ok(Publication { name, tables })
}

fn parse_subscription_metadata(text: &str) -> Result<Subscription, ServerError> {
    let name = fold_identifier(metadata_required_field(
        text,
        "name",
        "logical subscription",
    )?);
    let conninfo = metadata_required_field(text, "conninfo", "logical subscription")?.to_string();
    let slot_name = fold_identifier(metadata_required_field(
        text,
        "slot_name",
        "logical subscription",
    )?);
    let publications = metadata_required_field(text, "publications", "logical subscription")?
        .split(',')
        .map(fold_identifier)
        .filter(|publication| !publication.is_empty())
        .collect::<Vec<_>>();
    if name.is_empty() || slot_name.is_empty() || publications.is_empty() {
        return Err(ServerError::ddl(
            "malformed logical subscription metadata: empty name, slot, or publications",
        ));
    }
    let enabled = match metadata_field(text, "enabled") {
        Some("true") | None => true,
        Some("false") => false,
        Some(_) => {
            return Err(ServerError::ddl(
                "malformed logical subscription metadata: enabled must be true or false",
            ));
        }
    };
    Ok(Subscription {
        name,
        conninfo,
        slot_name,
        publications,
        enabled,
    })
}

fn parse_logical_slot_metadata(text: &str) -> Result<LogicalReplicationSlot, ServerError> {
    let name = fold_identifier(metadata_required_field(text, "name", "logical slot")?);
    let confirmed_lsn = metadata_required_field(text, "confirmed_lsn", "logical slot")?
        .parse::<u64>()
        .map_err(|_| {
            ServerError::ddl("malformed logical slot metadata: confirmed_lsn must be a u64")
        })?;
    if name.is_empty() {
        return Err(ServerError::ddl(
            "malformed logical slot metadata: empty name",
        ));
    }
    Ok(LogicalReplicationSlot {
        name,
        confirmed_lsn,
    })
}

/// File-backed replication slot store under `pg_replslot`.
#[derive(Clone, Debug)]
pub struct ReplicationSlotStore {
    root: PathBuf,
}

impl ReplicationSlotStore {
    /// Open or create a slot store.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, ServerError> {
        let root = root.into();
        ensure_directory(&root, "replication slot directory")?;
        Ok(Self { root })
    }

    /// Load a slot, creating it if absent.
    pub fn get_or_create(&self, name: &str) -> Result<ReplicationSlot, ServerError> {
        validate_replication_slot_name(name)?;
        let path = self.slot_path(name);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_file() => {}
            Ok(_) => {
                return Err(ServerError::ddl(format!(
                    "replication slot state is not a regular file: {}",
                    path.display()
                )));
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let slot = ReplicationSlot::new(name);
                self.save(&slot)?;
                return Ok(slot);
            }
            Err(err) => return Err(ServerError::Io(err)),
        }
        let text = read_regular_text_file(&path, "replication slot state file")?;
        Ok(parse_slot(name, &text))
    }

    /// Save a slot atomically enough for single-process tools.
    pub fn save(&self, slot: &ReplicationSlot) -> Result<(), ServerError> {
        validate_replication_slot_name(&slot.name)?;
        ensure_directory(&self.root, "replication slot directory")?;
        let body = format!(
            "name={}\nrestart_lsn={}\nconfirmed_flush_lsn={}\n",
            slot.name,
            slot.restart_lsn.clone().unwrap_or_default(),
            slot.confirmed_flush_lsn.clone().unwrap_or_default()
        );
        write_regular_text_file(&self.slot_path(&slot.name), &body)
    }

    /// Return all persisted slots in deterministic slot-name order.
    pub fn list(&self) -> Result<Vec<ReplicationSlot>, ServerError> {
        let mut slots = Vec::new();
        if !directory_exists(&self.root, "replication slot directory")? {
            return Ok(slots);
        }
        for entry in fs::read_dir(&self.root).map_err(ServerError::Io)? {
            let entry = entry.map_err(ServerError::Io)?;
            if !entry.file_type().map_err(ServerError::Io)?.is_file() {
                continue;
            }
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("slot") {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|name| name.to_str()) else {
                continue;
            };
            let text = read_regular_text_file(&path, "replication slot state file")?;
            slots.push(parse_slot(stem, &text));
        }
        slots.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(slots)
    }

    fn slot_path(&self, name: &str) -> PathBuf {
        self.root.join(format!("{name}.slot"))
    }
}

fn validate_replication_slot_name(name: &str) -> Result<(), ServerError> {
    let valid = !name.is_empty()
        && name.len() <= 63
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
    if valid {
        Ok(())
    } else {
        Err(ServerError::ddl(
            "invalid replication slot name; use ASCII letters, digits, and underscore",
        ))
    }
}

/// File-backed WAL sender.
#[derive(Clone, Debug)]
pub struct WalSender {
    archive_dir: PathBuf,
    slots: ReplicationSlotStore,
}

impl WalSender {
    /// Create a WAL sender over an archive directory and slot store.
    pub fn new(
        archive_dir: impl Into<PathBuf>,
        slots_dir: impl Into<PathBuf>,
    ) -> Result<Self, ServerError> {
        Ok(Self {
            archive_dir: archive_dir.into(),
            slots: ReplicationSlotStore::open(slots_dir)?,
        })
    }

    /// Ship all archived WAL files after the slot's last restart filename.
    pub fn send_once(&self, slot_name: &str, dest_dir: &Path) -> Result<usize, ServerError> {
        ensure_directory(dest_dir, "WAL send destination directory")?;
        let mut slot = self.slots.get_or_create(slot_name)?;
        let mut files = wal_files(&self.archive_dir)?;
        if let Some(restart) = &slot.restart_lsn {
            files.retain(|path| path.file_name().and_then(|s| s.to_str()) > Some(restart.as_str()));
        }
        let mut copied = 0_usize;
        for file in files {
            let Some(name) = file.file_name() else {
                continue;
            };
            let changed = copy_if_changed(&file, &dest_dir.join(name))?;
            slot.restart_lsn = Some(name.to_string_lossy().to_string());
            slot.confirmed_flush_lsn.clone_from(&slot.restart_lsn);
            if changed {
                copied = copied.saturating_add(1);
            }
        }
        self.slots.save(&slot)?;
        Ok(copied)
    }
}

/// File-backed WAL receiver.
#[derive(Clone, Debug)]
pub struct WalReceiver {
    source_dir: PathBuf,
}

impl WalReceiver {
    /// Create a receiver reading shipped WAL files from `source_dir`.
    #[must_use]
    pub fn new(source_dir: impl Into<PathBuf>) -> Self {
        Self {
            source_dir: source_dir.into(),
        }
    }

    /// Copy all received WAL files into standby `pg_wal`.
    pub fn receive_once(&self, standby_wal_dir: &Path) -> Result<usize, ServerError> {
        ensure_directory(standby_wal_dir, "WAL receive destination directory")?;
        let files = wal_files(&self.source_dir)?;
        let mut copied = 0_usize;
        for file in files {
            let Some(name) = file.file_name() else {
                continue;
            };
            if copy_if_changed(&file, &standby_wal_dir.join(name))? {
                copied = copied.saturating_add(1);
            }
        }
        Ok(copied)
    }

    /// Copy all received WAL files into standby `pg_wal` and into a local
    /// archive directory that can feed a downstream [`WalSender`].
    pub fn receive_once_cascading(
        &self,
        standby_wal_dir: &Path,
        cascade_archive_dir: &Path,
    ) -> Result<usize, ServerError> {
        ensure_directory(standby_wal_dir, "WAL receive destination directory")?;
        ensure_directory(cascade_archive_dir, "WAL cascade archive directory")?;
        let files = wal_files(&self.source_dir)?;
        let mut copied = 0_usize;
        for file in files {
            let Some(name) = file.file_name() else {
                continue;
            };
            let standby_changed = copy_if_changed(&file, &standby_wal_dir.join(name))?;
            let archive_changed = copy_if_changed(&file, &cascade_archive_dir.join(name))?;
            if standby_changed || archive_changed {
                copied = copied.saturating_add(1);
            }
        }
        Ok(copied)
    }
}

fn copy_if_changed(source: &Path, dest: &Path) -> Result<bool, ServerError> {
    ensure_file(source, "WAL source file")?;
    match fs::symlink_metadata(dest) {
        Ok(metadata) if metadata.file_type().is_file() => {
            let source_len = fs::symlink_metadata(source).map_err(ServerError::Io)?.len();
            let dest_len = metadata.len();
            if source_len == dest_len {
                return Ok(false);
            }
        }
        Ok(_) => {
            return Err(ServerError::ddl(format!(
                "refusing to overwrite non-regular WAL file {}",
                dest.display()
            )));
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(ServerError::Io(err)),
    }
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent).map_err(ServerError::Io)?;
    }
    fs::copy(source, dest).map_err(ServerError::Io)?;
    Ok(true)
}

fn wal_files(dir: &Path) -> Result<Vec<PathBuf>, ServerError> {
    let mut files = Vec::new();
    if !directory_exists(dir, "WAL source directory")? {
        return Ok(files);
    }
    for entry in fs::read_dir(dir).map_err(ServerError::Io)? {
        let entry = entry.map_err(ServerError::Io)?;
        let file_type = entry.file_type().map_err(ServerError::Io)?;
        let path = entry.path();
        let safe_name = entry
            .file_name()
            .to_str()
            .is_some_and(is_safe_wal_file_name);
        if file_type.is_file() && safe_name {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn ensure_file(path: &Path, context: &str) -> Result<(), ServerError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(()),
        Ok(_) => Err(ServerError::ddl(format!(
            "{context} is not a regular file: {}",
            path.display()
        ))),
        Err(err) => Err(ServerError::Io(err)),
    }
}

fn ensure_directory(path: &Path, context: &str) -> Result<(), ServerError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(()),
        Ok(_) => Err(ServerError::ddl(format!(
            "{context} is not a non-symlink directory: {}",
            path.display()
        ))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(path).map_err(ServerError::Io)?;
            match fs::symlink_metadata(path) {
                Ok(metadata) if metadata.file_type().is_dir() => Ok(()),
                Ok(_) => Err(ServerError::ddl(format!(
                    "{context} is not a non-symlink directory: {}",
                    path.display()
                ))),
                Err(err) => Err(ServerError::Io(err)),
            }
        }
        Err(err) => Err(ServerError::Io(err)),
    }
}

fn directory_exists(path: &Path, context: &str) -> Result<bool, ServerError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(true),
        Ok(_) => Err(ServerError::ddl(format!(
            "{context} is not a non-symlink directory: {}",
            path.display()
        ))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(ServerError::Io(err)),
    }
}

fn is_safe_wal_file_name(name: &str) -> bool {
    let ultrasql_segment = name
        .strip_prefix("segment_")
        .is_some_and(|suffix| suffix.len() == 10 && suffix.bytes().all(|b| b.is_ascii_digit()));
    let pg_segment = name.len() == 24 && name.bytes().all(|b| b.is_ascii_hexdigit());
    ultrasql_segment || pg_segment
}

fn parse_slot(name: &str, text: &str) -> ReplicationSlot {
    let mut slot = ReplicationSlot::new(name);
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("restart_lsn=") {
            if !value.is_empty() {
                slot.restart_lsn = Some(value.to_string());
            }
        } else if let Some(value) = line.strip_prefix("confirmed_flush_lsn=") {
            if !value.is_empty() {
                slot.confirmed_flush_lsn = Some(value.to_string());
            }
        }
    }
    slot
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_store_lists_persisted_slots_in_name_order() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let store = ReplicationSlotStore::open(dir.path()).expect("slot store");
        store
            .save(&ReplicationSlot {
                name: "standby_b".to_string(),
                restart_lsn: Some("0000000200000000".to_string()),
                confirmed_flush_lsn: Some("0000000200000000".to_string()),
            })
            .expect("save standby_b");
        store
            .save(&ReplicationSlot {
                name: "standby_a".to_string(),
                restart_lsn: None,
                confirmed_flush_lsn: Some("0000000100000000".to_string()),
            })
            .expect("save standby_a");

        let slots = store.list().expect("list slots");

        assert_eq!(slots.len(), 2);
        assert_eq!(slots[0].name, "standby_a");
        assert_eq!(slots[0].restart_lsn, None);
        assert_eq!(
            slots[0].confirmed_flush_lsn.as_deref(),
            Some("0000000100000000")
        );
        assert_eq!(slots[1].name, "standby_b");
    }

    #[test]
    fn slot_store_rejects_path_traversal_slot_names() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let slots_dir = dir.path().join("pg_replslot");
        let store = ReplicationSlotStore::open(&slots_dir).expect("slot store");

        let err = store
            .get_or_create("../escaped")
            .expect_err("traversal slot rejected");

        assert!(err.to_string().contains("invalid replication slot name"));
        assert!(!dir.path().join("escaped.slot").exists());
    }

    #[cfg(unix)]
    #[test]
    fn slot_store_rejects_symlinked_slot_state_files() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::TempDir::new().expect("temp dir");
        let slots_dir = dir.path().join("pg_replslot");
        let store = ReplicationSlotStore::open(&slots_dir).expect("slot store");
        let outside = dir.path().join("outside.slot");
        fs::write(&outside, "keep").expect("outside slot");
        symlink(&outside, slots_dir.join("standby.slot")).expect("slot symlink");

        assert!(store.get_or_create("standby").is_err());
        assert!(store.save(&ReplicationSlot::new("standby")).is_err());
        assert!(store.list().expect("list slots").is_empty());
        assert_eq!(
            fs::read_to_string(&outside).expect("outside unchanged"),
            "keep"
        );
    }

    #[cfg(unix)]
    #[test]
    fn slot_store_rejects_symlinked_root_directory() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::TempDir::new().expect("temp dir");
        let outside = tempfile::TempDir::new().expect("outside dir");
        fs::write(
            outside.path().join("standby.slot"),
            "name=standby\nrestart_lsn=segment_0000000001\n",
        )
        .expect("outside slot");
        let slots_dir = dir.path().join("pg_replslot");
        symlink(outside.path(), &slots_dir).expect("slot root symlink");

        let err = ReplicationSlotStore::open(&slots_dir).expect_err("symlinked root rejected");

        assert!(err.to_string().contains("directory"));
    }

    #[cfg(unix)]
    #[test]
    fn logical_metadata_ignores_and_rejects_symlinked_files() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::TempDir::new().expect("metadata dir");
        let publications = dir.path().join("publications");
        fs::create_dir_all(&publications).expect("publications dir");
        let outside = dir.path().join("outside.meta");
        fs::write(&outside, "name=pub_events\ntables=events\n").expect("outside metadata");
        symlink(
            &outside,
            publications.join(format!("{}.meta", hex_name("pub_events"))),
        )
        .expect("metadata symlink");

        let runtime = LogicalReplicationRuntime::open_metadata(dir.path()).expect("runtime");
        assert!(runtime.publication("pub_events").is_none());
        assert!(
            runtime
                .create_publication("pub_events", vec!["events".to_string()])
                .is_err()
        );
        assert_eq!(
            fs::read_to_string(&outside).expect("outside unchanged"),
            "name=pub_events\ntables=events\n"
        );
    }

    #[cfg(unix)]
    #[test]
    fn logical_metadata_rejects_symlinked_root_directory() {
        use std::os::unix::fs::symlink;

        let parent = tempfile::TempDir::new().expect("parent dir");
        let outside = tempfile::TempDir::new().expect("outside dir");
        let metadata_link = parent.path().join("pg_logical");
        symlink(outside.path(), &metadata_link).expect("metadata root symlink");

        let err =
            LogicalReplicationRuntime::open_metadata(&metadata_link).expect_err("root rejected");

        assert!(err.to_string().contains("directory"));
    }

    #[cfg(unix)]
    #[test]
    fn logical_metadata_rejects_symlinked_kind_directory() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::TempDir::new().expect("metadata dir");
        let outside = tempfile::TempDir::new().expect("outside dir");
        symlink(outside.path(), dir.path().join("publications")).expect("publication dir symlink");

        let err =
            LogicalReplicationRuntime::open_metadata(dir.path()).expect_err("subdir rejected");

        assert!(err.to_string().contains("directory"));
    }

    #[test]
    fn logical_metadata_rejects_oversized_metadata_files() {
        let dir = tempfile::TempDir::new().expect("metadata dir");
        let publications = dir.path().join("publications");
        fs::create_dir_all(&publications).expect("publications dir");
        let mut body = String::from("name=pub_events\ntables=events\n");
        body.push_str(&" ".repeat(1024 * 1024 + 1));
        fs::write(metadata_path(&publications, "pub_events"), body).expect("metadata");

        let err = LogicalReplicationRuntime::open_metadata(dir.path())
            .expect_err("oversized metadata rejected");

        assert!(err.to_string().contains("exceeds read limit"));
    }

    #[test]
    fn logical_metadata_rejects_malformed_publication_files() {
        let dir = tempfile::TempDir::new().expect("metadata dir");
        let publications = dir.path().join("publications");
        fs::create_dir_all(&publications).expect("publications dir");
        fs::write(
            metadata_path(&publications, "pub_events"),
            "name=pub_events\n",
        )
        .expect("metadata");

        let err = LogicalReplicationRuntime::open_metadata(dir.path())
            .expect_err("malformed publication metadata rejected");

        assert!(
            err.to_string()
                .contains("malformed logical publication metadata"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn logical_metadata_rejects_malformed_subscription_files() {
        let dir = tempfile::TempDir::new().expect("metadata dir");
        let subscriptions = dir.path().join("subscriptions");
        fs::create_dir_all(&subscriptions).expect("subscriptions dir");
        fs::write(
            metadata_path(&subscriptions, "sub_events"),
            "name=sub_events\nslot_name=sub_events\npublications=pub_events\nenabled=maybe\n",
        )
        .expect("metadata");

        let err = LogicalReplicationRuntime::open_metadata(dir.path())
            .expect_err("malformed subscription metadata rejected");

        assert!(
            err.to_string()
                .contains("malformed logical subscription metadata"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn logical_metadata_rejects_malformed_slot_files() {
        let dir = tempfile::TempDir::new().expect("metadata dir");
        let slots = dir.path().join("logical_slots");
        fs::create_dir_all(&slots).expect("slots dir");
        fs::write(
            metadata_path(&slots, "slot_events"),
            "name=slot_events\nconfirmed_lsn=not-a-number\n",
        )
        .expect("metadata");

        let err = LogicalReplicationRuntime::open_metadata(dir.path())
            .expect_err("malformed slot metadata rejected");

        assert!(
            err.to_string().contains("malformed logical slot metadata"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn logical_metadata_rejects_duplicate_publication_files() {
        let dir = tempfile::TempDir::new().expect("metadata dir");
        let publications = dir.path().join("publications");
        fs::create_dir_all(&publications).expect("publications dir");
        fs::write(
            metadata_path(&publications, "pub_events"),
            "name=pub_events\ntables=events\n",
        )
        .expect("metadata");
        fs::write(
            publications.join("duplicate.meta"),
            "name=pub_events\ntables=audit\n",
        )
        .expect("duplicate metadata");

        let err = LogicalReplicationRuntime::open_metadata(dir.path())
            .expect_err("duplicate publication metadata rejected");

        assert!(
            err.to_string()
                .contains("duplicate logical publication metadata"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn logical_metadata_rejects_duplicate_subscription_files() {
        let dir = tempfile::TempDir::new().expect("metadata dir");
        let subscriptions = dir.path().join("subscriptions");
        fs::create_dir_all(&subscriptions).expect("subscriptions dir");
        fs::write(
            metadata_path(&subscriptions, "sub_events"),
            "name=sub_events\nconninfo=host=localhost\nslot_name=s1\npublications=pub_events\n",
        )
        .expect("metadata");
        fs::write(
            subscriptions.join("duplicate.meta"),
            "name=sub_events\nconninfo=host=localhost\nslot_name=s2\npublications=pub_events\n",
        )
        .expect("duplicate metadata");

        let err = LogicalReplicationRuntime::open_metadata(dir.path())
            .expect_err("duplicate subscription metadata rejected");

        assert!(
            err.to_string()
                .contains("duplicate logical subscription metadata"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn logical_metadata_rejects_duplicate_slot_files() {
        let dir = tempfile::TempDir::new().expect("metadata dir");
        let slots = dir.path().join("logical_slots");
        fs::create_dir_all(&slots).expect("slots dir");
        fs::write(
            metadata_path(&slots, "slot_events"),
            "name=slot_events\nconfirmed_lsn=1\n",
        )
        .expect("metadata");
        fs::write(
            slots.join("duplicate.meta"),
            "name=slot_events\nconfirmed_lsn=2\n",
        )
        .expect("duplicate metadata");

        let err = LogicalReplicationRuntime::open_metadata(dir.path())
            .expect_err("duplicate slot metadata rejected");

        assert!(
            err.to_string().contains("duplicate logical slot metadata"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn logical_metadata_rejects_unbounded_read_limit() {
        let err = replication_metadata_take_limit(u64::MAX)
            .expect_err("unbounded metadata limit rejected");

        assert!(err.to_string().contains("metadata read limit is too large"));
    }

    #[cfg(unix)]
    #[test]
    fn logical_metadata_persist_rejects_swapped_symlinked_kind_directory() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::TempDir::new().expect("metadata dir");
        let outside = tempfile::TempDir::new().expect("outside dir");
        let runtime = LogicalReplicationRuntime::open_metadata(dir.path()).expect("runtime");
        fs::remove_dir(dir.path().join("publications")).expect("remove publications dir");
        symlink(outside.path(), dir.path().join("publications")).expect("publication dir symlink");

        assert!(
            runtime
                .create_publication("pub_events", vec!["events".to_string()])
                .is_err()
        );
        assert!(
            fs::read_dir(outside.path())
                .expect("outside read")
                .next()
                .is_none()
        );
    }

    #[cfg(unix)]
    #[test]
    fn logical_metadata_drop_does_not_follow_swapped_symlinked_kind_directory() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::TempDir::new().expect("metadata dir");
        let outside = tempfile::TempDir::new().expect("outside dir");
        let runtime = LogicalReplicationRuntime::open_metadata(dir.path()).expect("runtime");
        runtime
            .create_publication("pub_events", vec!["events".to_string()])
            .expect("publication");
        fs::remove_file(metadata_path(
            &dir.path().join("publications"),
            "pub_events",
        ))
        .expect("remove local metadata");
        fs::remove_dir(dir.path().join("publications")).expect("remove publications dir");
        let outside_file = metadata_path(outside.path(), "pub_events");
        fs::write(&outside_file, "name=pub_events\ntables=secret\n").expect("outside metadata");
        symlink(outside.path(), dir.path().join("publications")).expect("publication dir symlink");

        let err = runtime
            .drop_publication("pub_events")
            .expect_err("metadata deletion failure should abort drop");
        assert!(
            err.to_string()
                .contains("logical replication metadata directory")
        );

        assert!(outside_file.exists());
        assert!(runtime.publication("pub_events").is_some());
    }

    #[cfg(unix)]
    #[test]
    fn logical_metadata_persist_rejects_symlinked_temp_file_without_truncating_target() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::TempDir::new().expect("metadata dir");
        let publications = dir.path().join("publications");
        fs::create_dir_all(&publications).expect("publications dir");
        let metadata = metadata_path(&publications, "pub_events");
        let outside = dir.path().join("outside.tmp");
        fs::write(&metadata, "name=pub_events\ntables=events\n").expect("metadata");
        fs::write(&outside, "keep").expect("outside");
        symlink(&outside, metadata.with_extension("meta.tmp")).expect("temp symlink");

        let err = write_regular_text_file(&metadata, "name=pub_events\ntables=audit\n")
            .expect_err("symlinked temp metadata rejected");

        assert!(
            err.to_string().contains("non-regular metadata file"),
            "unexpected error: {err}"
        );
        assert_eq!(
            fs::read_to_string(&metadata).expect("metadata unchanged"),
            "name=pub_events\ntables=events\n"
        );
        assert_eq!(
            fs::read_to_string(&outside).expect("outside unchanged"),
            "keep"
        );
    }

    #[cfg(unix)]
    #[test]
    fn logical_subscription_drop_does_not_follow_swapped_symlinked_kind_directory() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::TempDir::new().expect("metadata dir");
        let outside = tempfile::TempDir::new().expect("outside dir");
        let runtime = LogicalReplicationRuntime::open_metadata(dir.path()).expect("runtime");
        runtime
            .create_publication("pub_events", vec!["events".to_string()])
            .expect("publication");
        runtime
            .create_subscription(
                "sub_events",
                "host=localhost dbname=ultrasql",
                vec!["pub_events".to_string()],
                None,
            )
            .expect("subscription");
        fs::remove_file(metadata_path(
            &dir.path().join("subscriptions"),
            "sub_events",
        ))
        .expect("remove local metadata");
        fs::remove_dir(dir.path().join("subscriptions")).expect("remove subscriptions dir");
        let outside_file = metadata_path(outside.path(), "sub_events");
        fs::write(&outside_file, "name=sub_events\npublications=secret\n")
            .expect("outside metadata");
        symlink(outside.path(), dir.path().join("subscriptions"))
            .expect("subscription dir symlink");

        let err = runtime
            .drop_subscription("sub_events")
            .expect_err("metadata deletion failure should abort drop");
        assert!(
            err.to_string()
                .contains("logical replication metadata directory")
        );

        assert!(outside_file.exists());
        assert!(
            runtime
                .subscriptions()
                .iter()
                .any(|subscription| subscription.name == "sub_events")
        );
    }

    #[test]
    fn logical_replication_metadata_survives_reopen() {
        let dir = tempfile::TempDir::new().expect("metadata dir");
        {
            let runtime =
                LogicalReplicationRuntime::open_metadata(dir.path()).expect("metadata runtime");
            runtime
                .create_publication("pub_events", vec!["events".to_string()])
                .expect("create publication");
            runtime
                .create_subscription(
                    "sub_events",
                    "host=127.0.0.1 port=5433",
                    vec!["pub_events".to_string()],
                    Some("sub_slot".to_string()),
                )
                .expect("create subscription");
        }

        let reopened = LogicalReplicationRuntime::open_metadata(dir.path()).expect("reopen");
        let publication = reopened.publication("pub_events").expect("publication");
        assert!(publication.publishes_table("events"));
        let subscriptions = reopened.subscriptions();
        assert_eq!(subscriptions.len(), 1);
        assert_eq!(subscriptions[0].name, "sub_events");
        assert_eq!(subscriptions[0].slot_name, "sub_slot");
        assert_eq!(subscriptions[0].publications, vec!["pub_events"]);
    }

    #[test]
    fn logical_decoding_slot_persists_confirmed_lsn() {
        let dir = tempfile::TempDir::new().expect("metadata dir");
        {
            let runtime =
                LogicalReplicationRuntime::open_metadata(dir.path()).expect("metadata runtime");
            runtime
                .create_publication("pub_events", vec!["events".to_string()])
                .expect("publication");
            runtime.create_logical_slot("slot_events").expect("slot");
            runtime
                .record_committed_dml("events", LogicalChangeKind::Insert, 2)
                .expect("record insert");
            let changes = runtime.decode_slot("slot_events").expect("decode");
            assert_eq!(changes.len(), 1);
            assert_eq!(changes[0].lsn, 1);
            assert_eq!(
                runtime
                    .logical_slot("slot_events")
                    .expect("slot")
                    .confirmed_lsn,
                1
            );
        }

        let runtime = LogicalReplicationRuntime::open_metadata(dir.path()).expect("reopen");
        assert_eq!(
            runtime
                .logical_slot("slot_events")
                .expect("slot")
                .confirmed_lsn,
            1
        );
        runtime
            .record_committed_dml("events", LogicalChangeKind::Update, 1)
            .expect("record update");
        let changes = runtime
            .decode_slot("slot_events")
            .expect("decode after reopen");
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].lsn, 2);
    }

    #[test]
    fn logical_metadata_rejects_exhausted_confirmed_lsn() {
        let dir = tempfile::TempDir::new().expect("metadata dir");
        let slots_dir = dir.path().join("logical_slots");
        fs::create_dir_all(&slots_dir).expect("slot dir");
        fs::write(
            slots_dir.join("slot_events"),
            "name=slot_events\nconfirmed_lsn=18446744073709551615\n",
        )
        .expect("slot metadata");

        let err = LogicalReplicationRuntime::open_metadata(dir.path())
            .expect_err("exhausted LSN metadata must be rejected");

        assert!(
            err.to_string()
                .contains("logical replication LSN space exhausted"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn logical_recording_does_not_wrap_exhausted_next_lsn() {
        let runtime = LogicalReplicationRuntime::new();
        runtime
            .create_publication("pub_events", vec!["events".to_string()])
            .expect("publication");
        runtime.next_lsn.store(u64::MAX, Ordering::Release);

        let err = runtime
            .record_committed_dml("events", LogicalChangeKind::Insert, 1)
            .expect_err("exhausted logical LSN must be visible to caller");

        assert!(
            err.to_string()
                .contains("logical replication LSN space exhausted"),
            "unexpected error: {err}"
        );
        assert!(runtime.changes_since(0).is_empty());
        assert_eq!(runtime.next_lsn.load(Ordering::Acquire), u64::MAX);
    }

    #[test]
    fn wal_receiver_skips_files_already_present_on_standby() {
        let source = tempfile::TempDir::new().expect("source dir");
        let standby = tempfile::TempDir::new().expect("standby dir");
        fs::write(source.path().join("000000010000000000000001"), b"wal-a").expect("wal a");
        fs::write(source.path().join("000000010000000000000002"), b"wal-b").expect("wal b");

        let receiver = WalReceiver::new(source.path());
        assert_eq!(
            receiver
                .receive_once(standby.path())
                .expect("first receive"),
            2
        );
        assert_eq!(
            receiver
                .receive_once(standby.path())
                .expect("second receive"),
            0
        );

        fs::write(source.path().join("000000010000000000000003"), b"wal-c").expect("wal c");
        assert_eq!(
            receiver
                .receive_once(standby.path())
                .expect("third receive"),
            1
        );
    }

    #[cfg(unix)]
    #[test]
    fn wal_receiver_rejects_symlinked_source_files() {
        use std::os::unix::fs::symlink;

        let source = tempfile::TempDir::new().expect("source dir");
        let standby = tempfile::TempDir::new().expect("standby dir");
        let secret = tempfile::NamedTempFile::new().expect("secret file");
        fs::write(secret.path(), b"not-wal").expect("secret contents");
        symlink(
            secret.path(),
            source.path().join("000000010000000000000001"),
        )
        .expect("symlink wal");

        let receiver = WalReceiver::new(source.path());
        assert_eq!(
            receiver
                .receive_once(standby.path())
                .expect("receive skips symlink"),
            0
        );
        assert!(!standby.path().join("000000010000000000000001").exists());
    }

    #[cfg(unix)]
    #[test]
    fn wal_copy_rejects_symlinked_sources() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::TempDir::new().expect("temp dir");
        let outside = tempfile::NamedTempFile::new().expect("outside file");
        fs::write(outside.path(), b"not-wal").expect("outside contents");
        let source = dir.path().join("000000010000000000000001");
        let dest = dir.path().join("standby/000000010000000000000001");
        symlink(outside.path(), &source).expect("source symlink");

        assert!(copy_if_changed(&source, &dest).is_err());
        assert!(!dest.exists());
    }

    #[cfg(unix)]
    #[test]
    fn wal_receiver_rejects_symlinked_source_directory() {
        use std::os::unix::fs::symlink;

        let real_source = tempfile::TempDir::new().expect("real source dir");
        let link_parent = tempfile::TempDir::new().expect("link parent");
        let standby = tempfile::TempDir::new().expect("standby dir");
        fs::write(
            real_source.path().join("000000010000000000000001"),
            b"wal-a",
        )
        .expect("wal a");
        let source_link = link_parent.path().join("source-link");
        symlink(real_source.path(), &source_link).expect("source dir symlink");

        let receiver = WalReceiver::new(&source_link);
        assert!(receiver.receive_once(standby.path()).is_err());
        assert!(!standby.path().join("000000010000000000000001").exists());
    }

    #[cfg(unix)]
    #[test]
    fn wal_receiver_rejects_symlinked_destination_files() {
        use std::os::unix::fs::symlink;

        let source = tempfile::TempDir::new().expect("source dir");
        let standby = tempfile::TempDir::new().expect("standby dir");
        let outside = tempfile::NamedTempFile::new().expect("outside file");
        fs::write(source.path().join("000000010000000000000001"), b"wal-a").expect("wal a");
        fs::write(outside.path(), b"keep").expect("outside contents");
        symlink(
            outside.path(),
            standby.path().join("000000010000000000000001"),
        )
        .expect("dest symlink");

        let receiver = WalReceiver::new(source.path());
        assert!(receiver.receive_once(standby.path()).is_err());
        assert_eq!(
            fs::read_to_string(outside.path()).expect("outside unchanged"),
            "keep"
        );
    }

    #[cfg(unix)]
    #[test]
    fn wal_receiver_rejects_symlinked_destination_directory() {
        use std::os::unix::fs::symlink;

        let source = tempfile::TempDir::new().expect("source dir");
        let outside = tempfile::TempDir::new().expect("outside dir");
        let link_parent = tempfile::TempDir::new().expect("link parent");
        fs::write(source.path().join("000000010000000000000001"), b"wal-a").expect("wal a");
        let standby_link = link_parent.path().join("standby-link");
        symlink(outside.path(), &standby_link).expect("standby dir symlink");

        let receiver = WalReceiver::new(source.path());
        assert!(receiver.receive_once(&standby_link).is_err());
        assert!(!outside.path().join("000000010000000000000001").exists());
    }

    #[cfg(unix)]
    #[test]
    fn wal_sender_rejects_symlinked_destination_files() {
        use std::os::unix::fs::symlink;

        let archive = tempfile::TempDir::new().expect("archive dir");
        let slots = tempfile::TempDir::new().expect("slots dir");
        let downstream = tempfile::TempDir::new().expect("downstream dir");
        let outside = tempfile::NamedTempFile::new().expect("outside file");
        fs::write(archive.path().join("000000010000000000000001"), b"wal-a").expect("wal a");
        fs::write(outside.path(), b"keep").expect("outside contents");
        symlink(
            outside.path(),
            downstream.path().join("000000010000000000000001"),
        )
        .expect("dest symlink");

        let sender = WalSender::new(archive.path(), slots.path()).expect("sender");
        assert!(sender.send_once("standby_a", downstream.path()).is_err());
        assert_eq!(
            fs::read_to_string(outside.path()).expect("outside unchanged"),
            "keep"
        );
    }

    #[test]
    fn wal_receiver_cascade_archive_can_feed_downstream_sender() {
        let upstream = tempfile::TempDir::new().expect("upstream dir");
        let standby_wal = tempfile::TempDir::new().expect("standby wal dir");
        let standby_archive = tempfile::TempDir::new().expect("standby archive dir");
        let slots = tempfile::TempDir::new().expect("slots dir");
        let downstream = tempfile::TempDir::new().expect("downstream dir");
        fs::write(upstream.path().join("000000010000000000000001"), b"wal-a").expect("wal a");
        fs::write(upstream.path().join("000000010000000000000002"), b"wal-b").expect("wal b");

        let receiver = WalReceiver::new(upstream.path());
        assert_eq!(
            receiver
                .receive_once_cascading(standby_wal.path(), standby_archive.path())
                .expect("cascade receive"),
            2
        );
        assert!(standby_wal.path().join("000000010000000000000001").exists());
        assert!(
            standby_archive
                .path()
                .join("000000010000000000000001")
                .exists()
        );

        let sender = WalSender::new(standby_archive.path(), slots.path()).expect("sender");
        assert_eq!(
            sender
                .send_once("cascade", downstream.path())
                .expect("downstream send"),
            2
        );
        assert!(downstream.path().join("000000010000000000000002").exists());
        assert_eq!(
            receiver
                .receive_once_cascading(standby_wal.path(), standby_archive.path())
                .expect("second cascade receive"),
            0
        );
    }
}
