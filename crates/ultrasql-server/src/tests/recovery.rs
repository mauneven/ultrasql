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
    BlankPageLoader, Server, ServerRecoveryTarget, capped_text_take_limit,
    recovery_replay_target_from_data_dir,
};

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
