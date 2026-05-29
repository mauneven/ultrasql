//! Two-phase commit (2PC) coordinator.
//!
//! Implements `PREPARE TRANSACTION`, `COMMIT PREPARED`, and
//! `ROLLBACK PREPARED` for distributed transaction scenarios (XA, federated
//! queries, or any caller that needs atomic commit across multiple resource
//! managers).
//!
//! # Protocol
//!
//! 1. **Phase 1 — prepare:** The application calls `PREPARE TRANSACTION 'gid'`.
//!    The coordinator durably records the prepared transaction to a state file
//!    in `state_dir` and enters it into `prepared`.  The originating
//!    [`crate::manager::Transaction`] is consumed; its XID remains "in flight"
//!    in the CLOG (neither committed nor aborted).
//!
//! 2. **Phase 2 — resolve:** The application calls `COMMIT PREPARED 'gid'` or
//!    `ROLLBACK PREPARED 'gid'` after all resource managers have voted.  The
//!    coordinator removes the state file and returns the XID so the caller can
//!    update the CLOG.
//!
//! # State file format
//!
//! One file per prepared transaction, named `<sanitized_gid>.txn` under
//! `state_dir`.  The file contains a minimal JSON object:
//!
//! ```json
//! {"gid":"<gid>","xid":<xid_raw>,"prepared_at_secs":<unix_secs>}
//! ```
//!
//! On [`TwoPhaseCoordinator::recover_from_disk`] every file is read and parsed,
//! and the prepared set is repopulated.  This makes durability survive a process
//! restart without a full WAL replay path.
//!
//! # Concurrency
//!
//! All shared state lives in a [`DashMap`]; operations on distinct GIDs never
//! contend.  Operations on the same GID (e.g. a concurrent `COMMIT PREPARED`
//! and `ROLLBACK PREPARED`) are serialised by the [`DashMap`] entry's shard lock
//! via the `entry()` API.

use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use ultrasql_core::Xid;

const DEFAULT_STATE_FILE_LIMIT_BYTES: u64 = 1024 * 1024;
const STATE_FILE_LIMIT_ENV: &str = "ULTRASQL_2PC_STATE_FILE_LIMIT_BYTES";

/// Errors returned by 2PC operations.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TwoPhaseError {
    /// A prepared transaction with this GID already exists.
    #[error("transaction \"{gid}\" is already prepared")]
    DuplicateGid {
        /// The conflicting global identifier.
        gid: String,
    },
    /// No prepared transaction with this GID was found.
    #[error("prepared transaction \"{gid}\" not found")]
    NotFound {
        /// The GID that was looked up.
        gid: String,
    },
    /// An I/O error occurred while writing or reading a state file.
    #[error("state file I/O error for \"{gid}\": {detail}")]
    Io {
        /// The GID associated with the failing operation.
        gid: String,
        /// Human-readable I/O error description.
        detail: String,
    },
    /// The state file contained malformed data.
    #[error("state file for \"{gid}\" is corrupt: {detail}")]
    Corrupt {
        /// The GID (or filename) associated with the corrupt file.
        gid: String,
        /// Description of the parse error.
        detail: String,
    },
}

/// A durably recorded prepared transaction.
///
/// Instances are created by [`TwoPhaseCoordinator::prepare`] and removed by
/// [`TwoPhaseCoordinator::commit_prepared`] or
/// [`TwoPhaseCoordinator::rollback_prepared`].
#[derive(Clone, Debug)]
pub struct PreparedTxn {
    /// The global identifier supplied by the client.
    pub gid: String,
    /// The XID of the original transaction.
    pub xid: Xid,
    /// Wall-clock time at which the transaction was prepared.
    pub prepared_at: SystemTime,
    /// Path to the durable state file on disk.
    pub state_file: PathBuf,
}

/// Two-phase commit coordinator.
///
/// Owns the in-memory map of prepared transactions and the directory where
/// state files are stored.  One instance per server; share via `Arc`.
///
/// # Send + Sync
///
/// [`TwoPhaseCoordinator`] is `Send + Sync` because [`DashMap`] and
/// [`PathBuf`] are `Send + Sync`.
#[derive(Debug)]
pub struct TwoPhaseCoordinator {
    /// In-memory index of prepared transactions, keyed by GID.
    prepared: DashMap<String, PreparedTxn>,
    /// Directory in which state files are stored.
    state_dir: PathBuf,
}

impl TwoPhaseCoordinator {
    /// Create a new coordinator that writes state files to `state_dir`.
    ///
    /// The directory must already exist when the coordinator is created
    /// (it is the server's responsibility to create `pg_twophase`-equivalent
    /// directory during `initdb`).
    #[must_use]
    pub fn new(state_dir: PathBuf) -> Self {
        Self {
            prepared: DashMap::new(),
            state_dir,
        }
    }

    /// Durably prepare the transaction identified by `xid` under global
    /// identifier `gid`.
    ///
    /// Writes a state file to disk before inserting into the in-memory map.
    /// If a transaction with the same `gid` is already prepared, returns
    /// [`TwoPhaseError::DuplicateGid`].
    ///
    /// The caller must have already flushed the transaction's WAL records
    /// (if applicable) before calling this method.
    pub fn prepare(&self, gid: &str, xid: Xid) -> Result<(), TwoPhaseError> {
        // Reject duplicate GIDs.
        if self.prepared.contains_key(gid) {
            return Err(TwoPhaseError::DuplicateGid {
                gid: gid.to_owned(),
            });
        }

        let prepared_at = SystemTime::now();
        let state_file = self.state_file_path(gid);
        write_state_file(&state_file, gid, xid, prepared_at)?;

        // Insert using the entry API for a final race-free duplicate check.
        // If another thread raced us and inserted first, we roll back by
        // removing the file we just wrote and returning DuplicateGid.
        let occupied = self
            .prepared
            .entry(gid.to_owned())
            .or_insert_with(|| PreparedTxn {
                gid: gid.to_owned(),
                xid,
                prepared_at,
                state_file: state_file.clone(),
            })
            .xid
            != xid;

        if occupied {
            // A concurrent prepare raced us.  Clean up our orphaned state file.
            let _ = fs::remove_file(&state_file);
            return Err(TwoPhaseError::DuplicateGid {
                gid: gid.to_owned(),
            });
        }

        Ok(())
    }

    /// Commit the prepared transaction with global identifier `gid`.
    ///
    /// Removes the state file and in-memory entry and returns the `Xid` so
    /// the caller can mark it committed in the CLOG.
    pub fn commit_prepared(&self, gid: &str) -> Result<Xid, TwoPhaseError> {
        self.resolve(gid)
    }

    /// Roll back the prepared transaction with global identifier `gid`.
    ///
    /// Removes the state file and in-memory entry and returns the `Xid` so
    /// the caller can mark it aborted in the CLOG.
    pub fn rollback_prepared(&self, gid: &str) -> Result<Xid, TwoPhaseError> {
        self.resolve(gid)
    }

    /// Scan the state directory and repopulate `prepared` from disk.
    ///
    /// Called during crash recovery / restart.  Any file that cannot be
    /// parsed is skipped (logged as a warning in production; silently skipped
    /// here since the tracing dependency is present but callers can layer it
    /// on).
    ///
    /// Returns the count of successfully restored prepared transactions.
    pub fn recover_from_disk(&self) -> Result<usize, TwoPhaseError> {
        let entries = fs::read_dir(&self.state_dir).map_err(|e| TwoPhaseError::Io {
            gid: "<state_dir>".to_owned(),
            detail: e.to_string(),
        })?;

        let mut count: usize = 0;

        for dir_entry_result in entries {
            let Ok(dir_entry) = dir_entry_result else {
                continue; // skip unreadable entries
            };
            let Ok(file_type) = dir_entry.file_type() else {
                continue;
            };
            if !file_type.is_file() {
                continue;
            }

            let path = dir_entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("txn") {
                continue;
            }

            let Ok(content) = read_state_file_text(&path) else {
                tracing::warn!(
                    path = %path.display(),
                    "skipping unreadable 2PC state file during recovery"
                );
                continue;
            };

            // Derive a placeholder GID from the filename for error messages.
            let filename_gid = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("<unknown>")
                .to_owned();

            let record = match parse_state_file(&content) {
                Ok(r) => r,
                Err(detail) => {
                    // Skip corrupt files during recovery; they will be
                    // investigated by the operator.
                    tracing::warn!(
                        path = %path.display(),
                        %detail,
                        "skipping corrupt 2PC state file during recovery"
                    );
                    let _ = filename_gid; // suppress lint
                    continue;
                }
            };

            // Re-insert only if not already present (idempotent recovery).
            self.prepared
                .entry(record.gid.clone())
                .or_insert(PreparedTxn {
                    gid: record.gid,
                    xid: record.xid,
                    prepared_at: record.prepared_at,
                    state_file: path,
                });
            count += 1;
        }

        Ok(count)
    }

    /// Return a snapshot of all currently prepared transactions.
    ///
    /// This is the backing data for the `pg_prepared_xacts` system view.
    /// The result is a cloned snapshot; modifications to the coordinator
    /// after this call are not reflected.
    pub fn list_prepared(&self) -> Vec<PreparedTxn> {
        self.prepared
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    // ── private helpers ───────────────────────────────────────────────────────

    /// Common resolution path for both commit and rollback.
    fn resolve(&self, gid: &str) -> Result<Xid, TwoPhaseError> {
        let (_, txn) = self
            .prepared
            .remove(gid)
            .ok_or_else(|| TwoPhaseError::NotFound {
                gid: gid.to_owned(),
            })?;

        // Best-effort file removal.  If the file is missing (e.g. after
        // crash-recovery where we recovered from disk but the file was already
        // cleaned up), that is not an error.
        let _ = fs::remove_file(&txn.state_file);

        Ok(txn.xid)
    }

    /// Construct the canonical state file path for a GID.
    fn state_file_path(&self, gid: &str) -> PathBuf {
        let sanitized = sanitize_gid(gid);
        self.state_dir.join(format!("{sanitized}.txn"))
    }
}

// ─── state file I/O ──────────────────────────────────────────────────────────

/// Write a minimal JSON state file for a prepared transaction.
fn write_state_file(
    path: &PathBuf,
    gid: &str,
    xid: Xid,
    prepared_at: SystemTime,
) -> Result<(), TwoPhaseError> {
    let secs = prepared_at
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs();

    // Manual JSON; no serde dependency required.
    let content = format!(
        "{{\"gid\":\"{}\",\"xid\":{},\"prepared_at_secs\":{}}}",
        escape_json_string(gid),
        xid.raw(),
        secs
    );

    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| TwoPhaseError::Io {
            gid: gid.to_owned(),
            detail: e.to_string(),
        })?;
    file.write_all(content.as_bytes())
        .map_err(|e| TwoPhaseError::Io {
            gid: gid.to_owned(),
            detail: e.to_string(),
        })
}

fn read_state_file_text(path: &Path) -> std::io::Result<String> {
    let limit = state_file_limit_bytes();
    let file = open_state_file_for_read(path)?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() {
        return Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            format!("2PC state path is not a regular file: {}", path.display()),
        ));
    }
    if metadata.len() > limit {
        return Err(state_file_limit_error(path, metadata.len(), limit));
    }

    let mut content = String::new();
    let mut limited = file.take(limit.saturating_add(1));
    limited.read_to_string(&mut content)?;
    let bytes_read = u64::try_from(content.len()).unwrap_or(u64::MAX);
    if bytes_read > limit {
        return Err(state_file_limit_error(path, bytes_read, limit));
    }
    Ok(content)
}

fn state_file_limit_bytes() -> u64 {
    std::env::var(STATE_FILE_LIMIT_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|&limit| limit > 0)
        .unwrap_or(DEFAULT_STATE_FILE_LIMIT_BYTES)
}

fn state_file_limit_error(path: &Path, bytes: u64, limit: u64) -> std::io::Error {
    std::io::Error::new(
        ErrorKind::InvalidData,
        format!(
            "2PC state file exceeds read limit: path={} bytes={} limit={} env={}",
            path.display(),
            bytes,
            limit,
            STATE_FILE_LIMIT_ENV
        ),
    )
}

#[cfg_attr(not(unix), allow(unused_variables))]
fn open_state_file_for_read(path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW);
    options.open(path)
}

/// Parsed record from a state file.
struct ParsedRecord {
    gid: String,
    xid: Xid,
    prepared_at: SystemTime,
}

/// Parse the minimal JSON written by [`write_state_file`].
///
/// Accepts only the exact format produced by the writer; this is not a
/// general JSON parser.  Returns an error string on parse failure.
fn parse_state_file(content: &str) -> Result<ParsedRecord, String> {
    let content = content.trim();

    // Extract `"gid":"<value>"`.
    let gid = extract_json_string(content, "gid")
        .ok_or_else(|| "missing or invalid \"gid\" field".to_owned())?;

    // Extract `"xid":<number>`.
    let xid_raw = extract_json_number(content, "xid")
        .ok_or_else(|| "missing or invalid \"xid\" field".to_owned())?;

    // Extract `"prepared_at_secs":<number>`.
    let secs = extract_json_number(content, "prepared_at_secs")
        .ok_or_else(|| "missing or invalid \"prepared_at_secs\" field".to_owned())?;

    let prepared_at = UNIX_EPOCH
        .checked_add(Duration::from_secs(secs))
        .ok_or_else(|| "prepared_at_secs overflows SystemTime".to_owned())?;

    Ok(ParsedRecord {
        gid,
        xid: Xid::new(xid_raw),
        prepared_at,
    })
}

/// Extract the string value of `"key":"<value>"` from a JSON fragment.
///
/// Handles simple JSON string escaping (only `\\` and `\"`).
fn extract_json_string(json: &str, key: &str) -> Option<String> {
    let needle = format!("\"{key}\":\"");
    let start = json.find(&needle)? + needle.len();
    let rest = &json[start..];

    let mut result = String::new();
    let mut chars = rest.chars();
    loop {
        match chars.next()? {
            '"' => break,
            '\\' => match chars.next()? {
                '"' => result.push('"'),
                '\\' => result.push('\\'),
                c => {
                    result.push('\\');
                    result.push(c);
                }
            },
            c => result.push(c),
        }
    }
    Some(result)
}

/// Extract the numeric value of `"key":<number>` from a JSON fragment.
fn extract_json_number(json: &str, key: &str) -> Option<u64> {
    let needle = format!("\"{key}\":");
    let start = json.find(&needle)? + needle.len();
    let rest = json[start..].trim_start();
    let end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

/// Escape special characters in a JSON string value.
fn escape_json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c => out.push(c),
        }
    }
    out
}

/// Replace filename-hostile characters with underscores.
///
/// This keeps GIDs that are ASCII identifiers intact while making path
/// traversal and control-character attacks impossible.
fn sanitize_gid(gid: &str) -> String {
    gid.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

// ─── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use ultrasql_core::Xid;

    use super::*;

    fn xid(n: u64) -> Xid {
        Xid::new(n)
    }

    fn make_coordinator() -> (TwoPhaseCoordinator, TempDir) {
        let dir = TempDir::new().expect("failed to create tempdir");
        let coord = TwoPhaseCoordinator::new(dir.path().to_path_buf());
        (coord, dir)
    }

    // ── prepare → commit ────────────────────────────────────────────────────

    #[test]
    fn prepare_then_commit_prepared_succeeds() {
        let (coord, _dir) = make_coordinator();

        coord.prepare("txn-1", xid(100)).unwrap();

        // State file must exist on disk.
        let listed = coord.list_prepared();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].gid, "txn-1");
        assert_eq!(listed[0].xid, xid(100));
        assert!(listed[0].state_file.exists(), "state file must exist");

        // Commit.
        let committed_xid = coord.commit_prepared("txn-1").unwrap();
        assert_eq!(committed_xid, xid(100));

        // State file removed, in-memory entry gone.
        assert!(
            !listed[0].state_file.exists(),
            "state file should be removed"
        );
        assert!(coord.list_prepared().is_empty());
    }

    // ── prepare → rollback ───────────────────────────────────────────────────

    #[test]
    fn prepare_then_rollback_prepared_succeeds() {
        let (coord, _dir) = make_coordinator();

        coord.prepare("txn-2", xid(200)).unwrap();
        let rolled_xid = coord.rollback_prepared("txn-2").unwrap();
        assert_eq!(rolled_xid, xid(200));
        assert!(coord.list_prepared().is_empty());
    }

    // ── duplicate GID rejected ───────────────────────────────────────────────

    #[test]
    fn duplicate_gid_rejected() {
        let (coord, _dir) = make_coordinator();

        coord.prepare("my-gid", xid(300)).unwrap();
        let err = coord.prepare("my-gid", xid(301)).unwrap_err();
        assert_eq!(
            err,
            TwoPhaseError::DuplicateGid {
                gid: "my-gid".to_owned(),
            }
        );
    }

    // ── commit_prepared on unknown GID ───────────────────────────────────────

    #[test]
    fn commit_prepared_unknown_gid_returns_not_found() {
        let (coord, _dir) = make_coordinator();
        let err = coord.commit_prepared("no-such-gid").unwrap_err();
        assert_eq!(
            err,
            TwoPhaseError::NotFound {
                gid: "no-such-gid".to_owned(),
            }
        );
    }

    // ── rollback_prepared on unknown GID ─────────────────────────────────────

    #[test]
    fn rollback_prepared_unknown_gid_returns_not_found() {
        let (coord, _dir) = make_coordinator();
        let err = coord.rollback_prepared("ghost").unwrap_err();
        assert_eq!(
            err,
            TwoPhaseError::NotFound {
                gid: "ghost".to_owned(),
            }
        );
    }

    // ── restart → recover_from_disk ──────────────────────────────────────────

    #[test]
    fn recover_from_disk_restores_prepared_transactions() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().to_path_buf();

        // First coordinator: prepare two transactions.
        {
            let coord = TwoPhaseCoordinator::new(path.clone());
            coord.prepare("recover-1", xid(400)).unwrap();
            coord.prepare("recover-2", xid(401)).unwrap();
            // Drop without resolving — simulates crash.
        }

        // Second coordinator: recover from disk.
        let coord2 = TwoPhaseCoordinator::new(path);
        let count = coord2.recover_from_disk().unwrap();
        assert_eq!(count, 2);

        let listed = coord2.list_prepared();
        let gids: std::collections::HashSet<_> = listed.iter().map(|t| t.gid.as_str()).collect();
        assert!(gids.contains("recover-1"));
        assert!(gids.contains("recover-2"));

        // Can still commit after recovery.
        let xid_back = coord2.commit_prepared("recover-1").unwrap();
        assert_eq!(xid_back, xid(400));
    }

    // ── list_prepared ────────────────────────────────────────────────────────

    #[test]
    fn list_prepared_returns_all_entries() {
        let (coord, _dir) = make_coordinator();

        coord.prepare("a", xid(1)).unwrap();
        coord.prepare("b", xid(2)).unwrap();
        coord.prepare("c", xid(3)).unwrap();

        let mut gids: Vec<_> = coord
            .list_prepared()
            .iter()
            .map(|t| t.gid.clone())
            .collect();
        gids.sort();
        assert_eq!(gids, ["a", "b", "c"]);
    }

    // ── state file JSON round-trip ────────────────────────────────────────────

    #[test]
    fn state_file_round_trips_gid_and_xid() {
        let (coord, _dir) = make_coordinator();
        let gid = "round-trip-test";
        coord.prepare(gid, xid(9999)).unwrap();

        let listed = coord.list_prepared();
        let file_content =
            std::fs::read_to_string(&listed[0].state_file).expect("state file must be readable");

        let parsed = parse_state_file(&file_content).expect("must parse");
        assert_eq!(parsed.gid, gid);
        assert_eq!(parsed.xid, xid(9999));
    }

    // ── GID with special characters sanitizes correctly ───────────────────────

    #[test]
    fn gid_with_special_chars_is_sanitized_in_filename() {
        let (coord, _dir) = make_coordinator();
        // A GID with path-hostile characters.
        coord.prepare("my/gid:with spaces", xid(500)).unwrap();

        let listed = coord.list_prepared();
        let file_name = listed[0]
            .state_file
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("");
        // Path separators and colons should be sanitized away.
        assert!(!file_name.contains('/'));
        assert!(!file_name.contains(':'));
    }

    // ── recover_from_disk is idempotent ──────────────────────────────────────

    #[test]
    fn recover_from_disk_is_idempotent() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().to_path_buf();

        {
            let coord = TwoPhaseCoordinator::new(path.clone());
            coord.prepare("idem", xid(600)).unwrap();
        }

        let coord = TwoPhaseCoordinator::new(path);
        let c1 = coord.recover_from_disk().unwrap();
        let c2 = coord.recover_from_disk().unwrap();
        // Second call finds the same files; entries already present so the
        // `or_insert` in recover does nothing.  Count still reports 1 for
        // the number of files processed.
        assert_eq!(c1, 1);
        assert_eq!(c2, 1);
        assert_eq!(coord.list_prepared().len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn prepare_refuses_symlinked_state_file() {
        use std::os::unix::fs::symlink;

        let dir = TempDir::new().expect("tempdir");
        let target = dir.path().join("target");
        std::fs::write(&target, b"keep").expect("target");
        symlink(&target, dir.path().join("evil.txn")).expect("state symlink");

        let coord = TwoPhaseCoordinator::new(dir.path().to_path_buf());
        assert!(coord.prepare("evil", xid(700)).is_err());
        assert_eq!(std::fs::read(&target).expect("target unchanged"), b"keep");
        assert!(coord.list_prepared().is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn recover_from_disk_skips_symlinked_state_files() {
        use std::os::unix::fs::symlink;

        let dir = TempDir::new().expect("tempdir");
        let outside = TempDir::new().expect("outside tempdir");
        let target = outside.path().join("target.txn");
        std::fs::write(
            &target,
            "{\"gid\":\"from-link\",\"xid\":701,\"prepared_at_secs\":0}",
        )
        .expect("target state");
        symlink(&target, dir.path().join("from-link.txn")).expect("state symlink");

        let coord = TwoPhaseCoordinator::new(dir.path().to_path_buf());
        assert_eq!(coord.recover_from_disk().expect("recover"), 0);
        assert!(coord.list_prepared().is_empty());
    }

    #[test]
    fn recover_from_disk_skips_configured_oversized_state_files() {
        let dir = TempDir::new().expect("tempdir");
        let mut state = String::from("{\"gid\":\"oversized\",\"xid\":702,\"prepared_at_secs\":0}");
        let padding = usize::try_from(DEFAULT_STATE_FILE_LIMIT_BYTES).unwrap() + 1;
        state.push_str(&" ".repeat(padding));
        std::fs::write(dir.path().join("oversized.txn"), state).expect("state file");

        let coord = TwoPhaseCoordinator::new(dir.path().to_path_buf());
        assert_eq!(coord.recover_from_disk().expect("recover"), 0);
        assert!(coord.list_prepared().is_empty());
    }
}
