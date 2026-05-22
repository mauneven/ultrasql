//! File-backed physical replication and same-process logical CDC primitives.
//!
//! These helpers implement deterministic WAL shipping against archived WAL
//! files. They are intentionally small and synchronous so the CLI, tests, and
//! future network WAL sender can share the same slot-state rules.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::ServerError;

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

/// Same-process logical replication runtime.
///
/// This is the first CDC layer: it records committed statement-level DML
/// changes for tables named by `CREATE PUBLICATION ... FOR TABLE ...`.
/// Row-image decoding and external replication slots can layer on this
/// commit-gated stream without changing transaction semantics.
#[derive(Debug)]
pub struct LogicalReplicationRuntime {
    publications: dashmap::DashMap<String, Publication>,
    subscriptions: dashmap::DashMap<String, Subscription>,
    changes: parking_lot::Mutex<Vec<LogicalChange>>,
    next_lsn: AtomicU64,
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
            subscriptions: dashmap::DashMap::new(),
            changes: parking_lot::Mutex::new(Vec::new()),
            next_lsn: AtomicU64::new(1),
        }
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
        self.publications.insert(name, publication.clone());
        Ok(publication)
    }

    /// Remove a publication, returning whether it existed.
    #[must_use]
    pub fn drop_publication(&self, name: &str) -> bool {
        self.publications.remove(&fold_identifier(name)).is_some()
    }

    /// Look up a publication by name.
    #[must_use]
    pub fn publication(&self, name: &str) -> Option<Publication> {
        self.publications
            .get(&fold_identifier(name))
            .map(|entry| entry.value().clone())
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
        self.subscriptions.insert(name, subscription.clone());
        Ok(subscription)
    }

    /// Remove a subscription, returning whether it existed.
    #[must_use]
    pub fn drop_subscription(&self, name: &str) -> bool {
        self.subscriptions.remove(&fold_identifier(name)).is_some()
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

    /// Emit a committed statement-level DML change for matching publications.
    pub fn record_committed_dml(&self, table: &str, kind: LogicalChangeKind, rows_affected: u64) {
        if rows_affected == 0 {
            return;
        }
        let table = fold_identifier(table);
        if table.is_empty() {
            return;
        }
        let mut publications = self
            .publications
            .iter()
            .filter(|publication| publication.value().publishes_table(&table))
            .map(|publication| publication.key().clone())
            .collect::<Vec<_>>();
        if publications.is_empty() {
            return;
        }
        publications.sort();
        let mut changes = self.changes.lock();
        for publication in publications {
            let lsn = self.next_lsn.fetch_add(1, Ordering::AcqRel);
            changes.push(LogicalChange {
                lsn,
                publication,
                table: table.clone(),
                kind,
                rows_affected,
            });
        }
    }
}

fn fold_identifier(ident: &str) -> String {
    ident
        .trim()
        .trim_matches('"')
        .trim_end_matches(';')
        .to_ascii_lowercase()
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
        fs::create_dir_all(&root).map_err(ServerError::Io)?;
        Ok(Self { root })
    }

    /// Load a slot, creating it if absent.
    pub fn get_or_create(&self, name: &str) -> Result<ReplicationSlot, ServerError> {
        let path = self.slot_path(name);
        if !path.exists() {
            let slot = ReplicationSlot::new(name);
            self.save(&slot)?;
            return Ok(slot);
        }
        let text = fs::read_to_string(path).map_err(ServerError::Io)?;
        Ok(parse_slot(name, &text))
    }

    /// Save a slot atomically enough for single-process tools.
    pub fn save(&self, slot: &ReplicationSlot) -> Result<(), ServerError> {
        fs::create_dir_all(&self.root).map_err(ServerError::Io)?;
        let body = format!(
            "name={}\nrestart_lsn={}\nconfirmed_flush_lsn={}\n",
            slot.name,
            slot.restart_lsn.clone().unwrap_or_default(),
            slot.confirmed_flush_lsn.clone().unwrap_or_default()
        );
        fs::write(self.slot_path(&slot.name), body).map_err(ServerError::Io)
    }

    /// Return all persisted slots in deterministic slot-name order.
    pub fn list(&self) -> Result<Vec<ReplicationSlot>, ServerError> {
        let mut slots = Vec::new();
        if !self.root.exists() {
            return Ok(slots);
        }
        for entry in fs::read_dir(&self.root).map_err(ServerError::Io)? {
            let entry = entry.map_err(ServerError::Io)?;
            let path = entry.path();
            if !path.is_file() || path.extension().and_then(|ext| ext.to_str()) != Some("slot") {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|name| name.to_str()) else {
                continue;
            };
            let text = fs::read_to_string(&path).map_err(ServerError::Io)?;
            slots.push(parse_slot(stem, &text));
        }
        slots.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(slots)
    }

    fn slot_path(&self, name: &str) -> PathBuf {
        self.root.join(format!("{name}.slot"))
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
        fs::create_dir_all(dest_dir).map_err(ServerError::Io)?;
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
            fs::copy(&file, dest_dir.join(name)).map_err(ServerError::Io)?;
            slot.restart_lsn = Some(name.to_string_lossy().to_string());
            slot.confirmed_flush_lsn.clone_from(&slot.restart_lsn);
            copied = copied.saturating_add(1);
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
        fs::create_dir_all(standby_wal_dir).map_err(ServerError::Io)?;
        let files = wal_files(&self.source_dir)?;
        let mut copied = 0_usize;
        for file in files {
            let Some(name) = file.file_name() else {
                continue;
            };
            fs::copy(&file, standby_wal_dir.join(name)).map_err(ServerError::Io)?;
            copied = copied.saturating_add(1);
        }
        Ok(copied)
    }
}

fn wal_files(dir: &Path) -> Result<Vec<PathBuf>, ServerError> {
    let mut files = Vec::new();
    if !dir.exists() {
        return Ok(files);
    }
    for entry in fs::read_dir(dir).map_err(ServerError::Io)? {
        let entry = entry.map_err(ServerError::Io)?;
        let path = entry.path();
        if path.is_file() {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
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
}
