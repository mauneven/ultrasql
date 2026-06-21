//! WAL recovery target tests.

use std::fs;
use std::sync::Arc;

use ultrasql_core::{BlockNumber, Lsn, PageId, RelationId, Xid};
use ultrasql_mvcc::{XidStatus, XidStatusOracle};
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::heap::HeapAccess;
use ultrasql_storage::page::Page;
use ultrasql_wal::payload::{AbortPayload, CommitPayload, SequenceOpKind, SequenceOpPayload};
use ultrasql_wal::{HeapTarget, RecordType, WalRecord};

#[cfg(unix)]
use super::super::prepare_secure_data_dir;
use super::super::{
    BlankPageLoader, Server, ServerRecoveryTarget, capped_text_take_limit, decode_clog_snapshot,
    encode_clog_snapshot, recovery_replay_target_from_data_dir,
};

#[test]
fn clog_snapshot_round_trips_and_rejects_corruption() {
    let lsn = Lsn::new(12_345);
    let next_xid = 99;
    let entries = vec![
        (Xid::new(10), XidStatus::Committed),
        (Xid::new(11), XidStatus::Aborted),
        (Xid::new(12), XidStatus::Committed),
        (Xid::new(13), XidStatus::Frozen),
    ];
    let bytes = encode_clog_snapshot(lsn, next_xid, &entries);
    let (got_lsn, got_next, got_entries) = decode_clog_snapshot(&bytes).expect("decode");
    assert_eq!(got_lsn, lsn);
    assert_eq!(got_next, next_xid);
    assert_eq!(got_entries, entries);

    // An empty commit log round-trips.
    let empty = encode_clog_snapshot(Lsn::ZERO, 3, &[]);
    let (_, n, e) = decode_clog_snapshot(&empty).expect("decode empty");
    assert_eq!(n, 3);
    assert!(e.is_empty());

    // Corruption is rejected with an error, never a panic.
    let mut flipped = bytes.clone();
    let mid = flipped.len() / 2;
    flipped[mid] ^= 0xFF;
    assert!(decode_clog_snapshot(&flipped).is_err(), "flipped byte");
    assert!(
        decode_clog_snapshot(&bytes[..bytes.len() - 3]).is_err(),
        "truncated"
    );
    let mut bad_magic = bytes.clone();
    bad_magic[0] = b'X';
    assert!(decode_clog_snapshot(&bad_magic).is_err(), "bad magic");
}

fn recovery_target() -> ServerRecoveryTarget {
    let pool = Arc::new(BufferPool::new(16, BlankPageLoader::new()));
    let heap = Arc::new(HeapAccess::new(pool));
    ServerRecoveryTarget {
        heap,
        sequences: Arc::new(dashmap::DashMap::new()),
    }
}

#[cfg(unix)]
fn make_data_dir_private(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}

#[test]
fn recovery_target_replays_sequence_state_and_drop() {
    let target = recovery_target();
    let payload = SequenceOpPayload {
        op: SequenceOpKind::Advance,
        seqrelid: RelationId::new(42),
        name: "orders_id_seq".to_owned(),
        start_value: 1,
        last_value: 41,
        min_value: 1,
        max_value: i64::MAX,
        increment: 1,
        cache_size: 1,
        is_called: true,
        cycle: false,
    };

    target.apply_sequence_op(&payload).unwrap();
    let seq = target.sequences.get("orders_id_seq").unwrap().clone();
    assert_eq!(seq.nextval().unwrap(), 42);

    let drop_payload = SequenceOpPayload {
        op: SequenceOpKind::Drop,
        ..payload
    };
    target.apply_sequence_op(&drop_payload).unwrap();
    assert!(target.sequences.get("orders_id_seq").is_none());
}

#[test]
fn server_init_retains_wal_writer_and_flushes_on_drop() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let wal_dir = data_dir.path().join("pg_wal");

    let appended_lsn = {
        let server = Server::init(data_dir.path()).unwrap();
        assert_eq!(server.runtime_wal_flushed_lsn(), Some(Lsn::ZERO));

        let pool = server.heap.buffer_pool();
        let sink = pool
            .wal_sink()
            .expect("WAL-backed server must install buffer-pool WAL sink");
        let record = WalRecord::new(RecordType::Nop, Xid::FIRST_USER, Lsn::ZERO, 0, Vec::new())
            .expect("test WAL record should fit size limits");
        sink.append(record).unwrap()
    };

    let mut seen_nop = 0_u64;
    let recovered_lsn = ultrasql_wal::recover(&wal_dir, |record| {
        if record.header.record_type == RecordType::Nop {
            seen_nop = seen_nop.saturating_add(1);
        }
        Ok(())
    })
    .unwrap();

    assert_eq!(seen_nop, 1);
    assert!(recovered_lsn.raw() > appended_lsn.raw());
}

#[test]
fn server_init_shares_checkpoint_lsn_with_checkpointer() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let server = Server::init(data_dir.path()).unwrap();

    assert!(
        Arc::strong_count(&server.heap.last_checkpoint_lsn) >= 2,
        "persistent heap checkpoint LSN must be shared with checkpointer"
    );
}

#[cfg(unix)]
#[test]
fn server_init_refuses_symlinked_data_dir() {
    use std::os::unix::fs::symlink;

    let real = tempfile::TempDir::new().unwrap();
    let link_parent = tempfile::TempDir::new().unwrap();
    let link = link_parent.path().join("alias-data");
    symlink(real.path(), &link).unwrap();

    let err = Server::init(&link).expect_err("symlinked data dir must be rejected");
    assert!(
        err.to_string().contains("symlink"),
        "expected symlink rejection, got {err}"
    );
}

#[cfg(unix)]
#[test]
fn server_init_refuses_symlinked_runtime_metadata_sidecar() {
    use std::os::unix::fs::symlink;

    let data_dir = tempfile::TempDir::new().unwrap();
    let outside = data_dir.path().join("outside.meta");
    fs::write(&outside, "# outside\n").unwrap();
    symlink(&outside, data_dir.path().join("pg_domain_runtime.meta")).unwrap();
    make_data_dir_private(data_dir.path());

    let err = Server::init(data_dir.path()).expect_err("symlinked sidecar rejected");

    assert!(
        err.to_string().contains("runtime metadata"),
        "expected metadata rejection, got {err}"
    );
}

#[cfg(unix)]
#[test]
fn runtime_metadata_persist_refuses_symlinked_temp_file() {
    use std::os::unix::fs::symlink;

    let data_dir = tempfile::TempDir::new().unwrap();
    let server = Server::init(data_dir.path()).unwrap();
    let outside = data_dir.path().join("outside.tmp");
    fs::write(&outside, "keep").unwrap();
    symlink(&outside, data_dir.path().join("pg_domain_runtime.meta.tmp")).unwrap();

    let err = server
        .persist_domain_runtime_constraints_metadata()
        .expect_err("symlinked temp sidecar rejected");

    assert!(
        err.to_string().contains("runtime metadata"),
        "expected metadata rejection, got {err}"
    );
    assert_eq!(fs::read_to_string(&outside).unwrap(), "keep");
}

#[test]
fn runtime_metadata_read_limit_rejects_overflow() {
    let err = capped_text_take_limit("runtime metadata file", u64::MAX).unwrap_err();
    assert!(err.to_string().contains("read limit is too large"));
}

#[cfg(unix)]
#[test]
fn backup_markers_refuse_symlinked_targets() {
    use std::os::unix::fs::symlink;

    let data_dir = tempfile::TempDir::new().unwrap();
    let server = Server::init(data_dir.path()).unwrap();
    let outside = data_dir.path().join("outside-label");
    fs::write(&outside, "keep").unwrap();
    symlink(&outside, data_dir.path().join("backup_label")).unwrap();

    let err = server
        .record_backup_marker("pg_start_backup")
        .expect_err("symlinked backup marker rejected");

    assert!(
        err.to_string().contains("backup marker"),
        "expected backup marker rejection, got {err}"
    );
    assert_eq!(fs::read_to_string(&outside).unwrap(), "keep");
}

#[cfg(unix)]
#[test]
fn server_init_refuses_symlinked_recovery_targets() {
    use std::os::unix::fs::symlink;

    let data_dir = tempfile::TempDir::new().unwrap();
    let outside = data_dir.path().join("outside.targets");
    fs::write(&outside, "recovery_target_xid = '42'\n").unwrap();
    symlink(&outside, data_dir.path().join("recovery.targets")).unwrap();
    make_data_dir_private(data_dir.path());

    let err = Server::init(data_dir.path()).expect_err("symlinked recovery targets rejected");

    assert!(
        err.to_string().contains("recovery targets"),
        "expected recovery target rejection, got {err}"
    );
}

#[cfg(unix)]
#[test]
fn data_dir_owner_check_rejects_unexpected_uid() {
    use std::os::unix::fs::MetadataExt;

    let data_dir = tempfile::TempDir::new().unwrap();
    let actual_uid = fs::metadata(data_dir.path()).unwrap().uid();
    let unexpected_uid = if actual_uid == u32::MAX {
        actual_uid - 1
    } else {
        actual_uid + 1
    };

    let err = super::super::validate_data_dir_owner(data_dir.path(), unexpected_uid)
        .expect_err("uid mismatch must be rejected");
    assert!(
        err.to_string().contains("owned by uid"),
        "expected owner rejection, got {err}"
    );
}

#[cfg(unix)]
#[test]
fn prepare_secure_data_dir_tightens_new_directory_permissions() {
    use std::os::unix::fs::MetadataExt;

    let parent = tempfile::TempDir::new().unwrap();
    let data_dir = parent.path().join("data");

    let canonical = prepare_secure_data_dir(&data_dir).expect("fresh data dir");
    let mode = fs::metadata(canonical).expect("data dir metadata").mode() & 0o777;

    assert_eq!(mode & 0o077, 0);
}

#[cfg(unix)]
#[test]
fn prepare_secure_data_dir_rejects_existing_group_world_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let parent = tempfile::TempDir::new().unwrap();
    let data_dir = parent.path().join("data");
    fs::create_dir(&data_dir).expect("data dir");
    fs::write(data_dir.join("PG_VERSION"), b"0").expect("marker");
    fs::set_permissions(&data_dir, fs::Permissions::from_mode(0o755)).expect("chmod data dir");

    let err = prepare_secure_data_dir(&data_dir).expect_err("open mode must be rejected");

    assert!(
        err.to_string().contains("group/world permissions"),
        "expected permissions rejection, got {err}"
    );
}

#[test]
fn server_init_stores_canonical_data_dir() {
    let root = tempfile::TempDir::new().unwrap();
    let child = root.path().join("child");
    fs::create_dir(&child).unwrap();
    #[cfg(unix)]
    make_data_dir_private(root.path());
    let aliased = child.join("..");

    let server = Server::init(&aliased).unwrap();
    let stored = server.data_dir.as_ref().expect("persistent server path");
    assert_eq!(stored, &root.path().canonicalize().unwrap());
}

#[test]
fn persistent_server_can_force_commit_marker_durable() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let server = Server::init(data_dir.path()).unwrap();

    let commit_lsn = server
        .append_commit_record(Xid::FIRST_USER)
        .unwrap()
        .expect("persistent server must append a commit marker");
    server.wait_for_wal_durable(commit_lsn).unwrap();

    let flushed_lsn = server
        .runtime_wal_flushed_lsn()
        .expect("persistent server must own a WAL writer");
    assert!(flushed_lsn.raw() >= commit_lsn.raw());
}

#[test]
fn server_init_restores_aborted_xids_from_wal() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let wal_dir = data_dir.path().join("pg_wal");
    fs::create_dir_all(&wal_dir).unwrap();
    let xid = Xid::new(42);
    let payload = AbortPayload {
        abort_lsn: Lsn::new(100),
    };
    let record = WalRecord::new(RecordType::Abort, xid, Lsn::ZERO, 0, payload.encode())
        .expect("test WAL abort record should fit size limits");
    fs::write(wal_dir.join("segment_0000000000"), record.encode()).unwrap();
    #[cfg(unix)]
    make_data_dir_private(data_dir.path());

    let server = Server::init(data_dir.path()).unwrap();

    assert_eq!(server.txn_manager.status(xid), XidStatus::Aborted);
}

#[test]
fn server_init_treats_observed_xids_without_terminal_record_as_aborted() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let wal_dir = data_dir.path().join("pg_wal");
    fs::create_dir_all(&wal_dir).unwrap();
    let xid = Xid::new(43);
    let record = WalRecord::new(RecordType::Nop, xid, Lsn::ZERO, 0, Vec::new())
        .expect("test WAL nop record should fit size limits");
    fs::write(wal_dir.join("segment_0000000000"), record.encode()).unwrap();
    #[cfg(unix)]
    make_data_dir_private(data_dir.path());

    let server = Server::init(data_dir.path()).unwrap();

    assert_eq!(server.txn_manager.status(xid), XidStatus::Aborted);
}

#[test]
fn server_init_reopens_base_heap_pages_from_data_dir() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let page_id = PageId::new(RelationId::new(4242), BlockNumber::new(0));

    {
        let server = Server::init(data_dir.path()).unwrap();
        let mut page = Page::new_heap();
        page.set_lsn(777);
        server.page_loader.store(page_id, &page).unwrap();
    }

    let server = Server::init(data_dir.path()).unwrap();
    let page = server.page_loader.load(page_id).unwrap();
    assert_eq!(page.header().lsn, 777);
}

#[test]
fn server_init_reopens_logical_replication_metadata() {
    let data_dir = tempfile::TempDir::new().unwrap();
    {
        let server = Server::init(data_dir.path()).unwrap();
        server
            .logical_replication
            .create_publication("pub_events", vec!["events".to_string()])
            .unwrap();
        server
            .logical_replication
            .create_subscription(
                "sub_events",
                "host=127.0.0.1 port=5433",
                vec!["pub_events".to_string()],
                Some("sub_slot".to_string()),
            )
            .unwrap();
    }

    let server = Server::init(data_dir.path()).unwrap();
    let publication = server
        .logical_replication
        .publication("pub_events")
        .expect("publication survives restart");
    assert!(publication.publishes_table("events"));
    let subscriptions = server.logical_replication.subscriptions();
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0].name, "sub_events");
    assert_eq!(subscriptions[0].slot_name, "sub_slot");
}

#[test]
fn recovery_target_lsn_file_parses_postgres_lsn() {
    let data_dir = tempfile::TempDir::new().unwrap();
    fs::write(
        data_dir.path().join("recovery.targets"),
        "recovery_target_lsn = '0/0000002A'\n",
    )
    .unwrap();

    let target = recovery_replay_target_from_data_dir(data_dir.path()).unwrap();

    assert_eq!(target.target_lsn, Some(Lsn::new(42)));
}

#[test]
fn recovery_target_time_file_parses_rfc3339_timestamp() {
    let data_dir = tempfile::TempDir::new().unwrap();
    fs::write(
        data_dir.path().join("recovery.targets"),
        "recovery_target_time = '1970-01-01T00:00:01Z'\n",
    )
    .unwrap();

    let target = recovery_replay_target_from_data_dir(data_dir.path()).unwrap();

    assert_eq!(target.target_time_micros, Some(1_000_000));
}

#[test]
fn recovery_target_xid_file_parses_decimal_xid() {
    let data_dir = tempfile::TempDir::new().unwrap();
    fs::write(
        data_dir.path().join("recovery.targets"),
        "recovery_target_xid = '42'\n",
    )
    .unwrap();

    let target = recovery_replay_target_from_data_dir(data_dir.path()).unwrap();

    assert_eq!(target.target_xid, Some(Xid::new(42)));
}

#[test]
fn oversized_recovery_targets_file_is_refused() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let mut text = String::from("recovery_target_xid = '42'\n");
    text.push_str(&" ".repeat(70 * 1024));
    fs::write(data_dir.path().join("recovery.targets"), text).unwrap();

    let err = recovery_replay_target_from_data_dir(data_dir.path())
        .expect_err("oversized recovery targets rejected");

    assert!(err.to_string().contains("exceeds read limit"), "{err}");
}

#[test]
fn server_init_honors_recovery_target_lsn_before_installing_wal_writer() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let wal_dir = data_dir.path().join("pg_wal");
    fs::create_dir_all(&wal_dir).unwrap();

    let first = WalRecord::new(RecordType::Nop, Xid::new(10), Lsn::ZERO, 0, Vec::new())
        .expect("test WAL record should fit size limits");
    let first_bytes = first.encode();
    let second = WalRecord::new(RecordType::Nop, Xid::new(11), Lsn::ZERO, 0, Vec::new())
        .expect("test WAL record should fit size limits");
    let target = Lsn::new(u64::try_from(first_bytes.len()).unwrap());
    let mut segment = first_bytes;
    segment.extend_from_slice(&second.encode());
    fs::write(wal_dir.join("segment_0000000000"), segment).unwrap();
    fs::write(
        data_dir.path().join("recovery.targets"),
        format!("recovery_target_lsn = '{}'\n", target.raw()),
    )
    .unwrap();
    #[cfg(unix)]
    make_data_dir_private(data_dir.path());

    let server = Server::init(data_dir.path()).unwrap();
    let sink = server
        .heap
        .buffer_pool()
        .wal_sink()
        .expect("recovered persistent server must install WAL sink");
    let appended = sink
        .append(
            WalRecord::new(RecordType::Nop, Xid::new(12), Lsn::ZERO, 0, Vec::new())
                .expect("test WAL record should fit size limits"),
        )
        .unwrap();

    assert_eq!(appended, target);
}

#[test]
fn server_init_does_not_restore_commit_status_after_recovery_target_lsn() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let wal_dir = data_dir.path().join("pg_wal");
    fs::create_dir_all(&wal_dir).unwrap();

    let first = WalRecord::new(RecordType::Nop, Xid::new(10), Lsn::ZERO, 0, Vec::new())
        .expect("test WAL record should fit size limits");
    let first_bytes = first.encode();
    let target = Lsn::new(u64::try_from(first_bytes.len()).unwrap());
    let xid_after_target = Xid::new(42);
    let commit = CommitPayload {
        commit_lsn: Lsn::new(200),
        commit_timestamp_micros: 1_700_000_000_000_000,
    };
    let second = WalRecord::new(
        RecordType::Commit,
        xid_after_target,
        Lsn::ZERO,
        0,
        commit.encode(),
    )
    .expect("test WAL commit record should fit size limits");
    let mut segment = first_bytes;
    segment.extend_from_slice(&second.encode());
    fs::write(wal_dir.join("segment_0000000000"), segment).unwrap();
    fs::write(
        data_dir.path().join("recovery.targets"),
        format!("recovery_target_lsn = '{}'\n", target.raw()),
    )
    .unwrap();
    #[cfg(unix)]
    make_data_dir_private(data_dir.path());

    let server = Server::init(data_dir.path()).unwrap();

    assert_eq!(
        server.txn_manager.status(xid_after_target),
        XidStatus::InProgress
    );
}

/// Count `segment_*` files currently present in a WAL directory.
fn count_wal_segments(wal_dir: &std::path::Path) -> usize {
    fs::read_dir(wal_dir)
        .map(|rd| {
            rd.filter_map(Result::ok)
                .filter(|e| {
                    e.file_name()
                        .to_str()
                        .is_some_and(|n| n.starts_with("segment_"))
                })
                .count()
        })
        .unwrap_or(0)
}

/// A tiny segment size so a few hundred small records span many segments,
/// exercising multi-segment recycling without writing tens of MiB.
fn small_wal_config() -> ultrasql_wal::WalWriterConfig {
    ultrasql_wal::WalWriterConfig {
        segment_size_bytes: 4096,
        fsync_window_us: 1000,
        fsync_batch_bytes: 4096,
    }
}

/// Append `count` records, each under its own freshly-committed transaction, so
/// none of them stays in progress to pin the truncation floor.
fn append_resolved_records(server: &Server, count: usize) {
    use ultrasql_txn::IsolationLevel;
    let pool = server.heap.buffer_pool();
    let sink = pool.wal_sink().expect("WAL-backed server installs a sink");
    for _ in 0..count {
        let txn = server.txn_manager.begin(IsolationLevel::ReadCommitted);
        let record = WalRecord::new(RecordType::Nop, txn.xid, Lsn::ZERO, 0, vec![0u8; 64])
            .expect("nop record fits size limits");
        sink.append(record).unwrap();
        server.txn_manager.commit(txn).unwrap();
    }
}

#[test]
fn fold_min_nonzero_lsn_skips_zero_and_keeps_minimum() {
    use super::super::fold_min_nonzero_lsn;
    // ZERO never constrains the floor: an index with no logged ops has no WAL
    // records of its own, so it must not pin the floor (which would block all
    // recycling for an empty or never-written vector index).
    assert_eq!(fold_min_nonzero_lsn(None, Lsn::ZERO), None);
    assert_eq!(
        fold_min_nonzero_lsn(Some(Lsn::new(500)), Lsn::ZERO),
        Some(Lsn::new(500))
    );
    // The first non-zero seeds the accumulator; later folds keep the minimum.
    assert_eq!(
        fold_min_nonzero_lsn(None, Lsn::new(500)),
        Some(Lsn::new(500))
    );
    assert_eq!(
        fold_min_nonzero_lsn(Some(Lsn::new(500)), Lsn::new(300)),
        Some(Lsn::new(300))
    );
    assert_eq!(
        fold_min_nonzero_lsn(Some(Lsn::new(300)), Lsn::new(900)),
        Some(Lsn::new(300))
    );
}

#[test]
fn run_checkpoint_cycle_recycles_wal_like_an_explicit_checkpoint() {
    // The background-timer entry point must do a full checkpoint — including WAL
    // recycling — exactly like an explicit CHECKPOINT.
    let data_dir = tempfile::TempDir::new().unwrap();
    let wal_dir = data_dir.path().join("pg_wal");
    let cfg = small_wal_config();
    {
        let server = Server::init_with_wal_writer_config(data_dir.path(), cfg).unwrap();
        append_resolved_records(&server, 600);

        // WAL recycling unlinks rolled-past segment files. `maybe_recycle_wal`
        // intentionally *retains* a segment whose unlink fails and retries on
        // the next cycle (it logs "will retry next interval"). On Windows a
        // segment whose handle was just dropped at roll time can be
        // transiently undeletable, so a single cycle may not advance the floor.
        // Assert the product's real contract — *eventual* recycling — rather
        // than racing on one cycle (this test was flaky on Windows otherwise).
        let mut floor = ultrasql_wal::read_floor(&wal_dir).unwrap();
        for _ in 0..100 {
            server.run_checkpoint_cycle();
            floor = ultrasql_wal::read_floor(&wal_dir).unwrap();
            if floor.segment_index > 0 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(
            floor.segment_index > 0,
            "the automatic checkpoint cycle must recycle low WAL segments"
        );
        // Once the floor advanced past it, segment_0 has been unlinked.
        assert!(!wal_dir.join("segment_0000000000").exists());
    }
    // The advanced floor is durable: a reopen recovers cleanly from it.
    let reopened = Server::init_with_wal_writer_config(data_dir.path(), cfg).unwrap();
    assert!(reopened.runtime_wal_flushed_lsn().is_some());
}

#[test]
fn run_checkpoint_cycle_is_a_safe_noop_without_a_wal() {
    // In-memory (sample) mode installs no WAL sink; the cycle must be a quiet
    // no-op rather than erroring or panicking.
    let server = Server::with_sample_database();
    server.run_checkpoint_cycle();
}

#[test]
fn checkpoint_recycles_wal_segments_below_the_floor() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let wal_dir = data_dir.path().join("pg_wal");
    let cfg = small_wal_config();

    {
        let server = Server::init_with_wal_writer_config(data_dir.path(), cfg).unwrap();
        append_resolved_records(&server, 600);

        // The checkpoint forces every record durable (writing the segments) and
        // then recycles those below the floor — so the post-checkpoint state is
        // deterministic, unlike a pre-checkpoint count that races the writer.
        server.perform_checkpoint().unwrap();

        // A floor at segment N>0 is itself proof that segments 0..N existed and
        // were recycled (the floor only advances over whole removed segments).
        let floor = ultrasql_wal::read_floor(&wal_dir).unwrap();
        assert!(
            floor.segment_index > 0,
            "checkpoint must recycle low WAL segments (floor still at segment {})",
            floor.segment_index
        );
        assert!(
            !wal_dir.join("segment_0000000000").exists(),
            "the original head segment must be recycled"
        );
        // The active segment (and any kept tail) always survives.
        assert!(
            count_wal_segments(&wal_dir) >= 1,
            "the active segment must never be recycled"
        );
    }

    // Reopen: recovery seeds from the advanced floor and succeeds. An absent or
    // mis-seeded floor would make recovery fail or reconstruct shifted LSNs.
    let reopened = Server::init_with_wal_writer_config(data_dir.path(), cfg).unwrap();
    assert!(
        reopened.runtime_wal_flushed_lsn().is_some(),
        "reopened WAL-backed server must report a flushed LSN"
    );
}

#[test]
fn checkpoint_keeps_an_in_progress_transactions_segment() {
    use ultrasql_txn::IsolationLevel;

    let data_dir = tempfile::TempDir::new().unwrap();
    let wal_dir = data_dir.path().join("pg_wal");
    let cfg = small_wal_config();
    let server = Server::init_with_wal_writer_config(data_dir.path(), cfg).unwrap();

    // A long-running transaction writes an EARLY record and stays in progress.
    let long = server.txn_manager.begin(IsolationLevel::ReadCommitted);
    let long_first_lsn = {
        let pool = server.heap.buffer_pool();
        let sink = pool.wal_sink().expect("sink");
        sink.append(WalRecord::new(RecordType::Nop, long.xid, Lsn::ZERO, 0, vec![0u8; 64]).unwrap())
            .unwrap()
    };

    // Many resolved transactions then span many later segments.
    append_resolved_records(&server, 600);

    server.perform_checkpoint().unwrap();

    // The floor must not pass the in-progress transaction's first record: its
    // records must survive so a crash recovery can still mark it aborted (an
    // unknown XID would otherwise default to InProgress forever).
    let pinned = ultrasql_wal::read_floor(&wal_dir).unwrap();
    assert!(
        pinned.floor_lsn.raw() <= long_first_lsn.raw(),
        "floor {} must not pass the in-progress txn's first LSN {}",
        pinned.floor_lsn.raw(),
        long_first_lsn.raw()
    );

    // Once it resolves, the next checkpoint advances past it.
    server.txn_manager.commit(long).unwrap();
    server.perform_checkpoint().unwrap();
    let advanced = ultrasql_wal::read_floor(&wal_dir).unwrap();
    assert!(
        advanced.floor_lsn.raw() > long_first_lsn.raw(),
        "once the transaction resolved, the floor ({}) should advance past its \
         former first LSN ({})",
        advanced.floor_lsn.raw(),
        long_first_lsn.raw()
    );
}
