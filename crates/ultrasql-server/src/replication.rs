//! File-backed replication primitives for v0.9 operations.
//!
//! These helpers implement deterministic WAL shipping against archived WAL
//! files. They are intentionally small and synchronous so the CLI, tests, and
//! future network WAL sender can share the same slot-state rules.

use std::fs;
use std::path::{Path, PathBuf};

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
    pub fn new(archive_dir: impl Into<PathBuf>, slots_dir: impl Into<PathBuf>) -> Result<Self, ServerError> {
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
            let Some(name) = file.file_name() else { continue };
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
            let Some(name) = file.file_name() else { continue };
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
