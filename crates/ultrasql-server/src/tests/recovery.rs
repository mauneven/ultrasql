//! WAL recovery target tests.

use std::fs;
use std::sync::Arc;

use ultrasql_core::{BlockNumber, Lsn, PageId, RelationId, Xid};
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::heap::HeapAccess;
use ultrasql_storage::page::Page;
use ultrasql_wal::payload::{SequenceOpKind, SequenceOpPayload};
use ultrasql_wal::{HeapTarget, RecordType, WalRecord};

use super::super::{
    BlankPageLoader, Server, ServerRecoveryTarget, recovery_replay_target_from_data_dir,
};

fn recovery_target() -> ServerRecoveryTarget {
    let pool = Arc::new(BufferPool::new(16, BlankPageLoader::new()));
    let heap = Arc::new(HeapAccess::new(pool));
    ServerRecoveryTarget {
        heap,
        sequences: Arc::new(dashmap::DashMap::new()),
    }
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
        let record = WalRecord::new(RecordType::Nop, Xid::FIRST_USER, Lsn::ZERO, 0, Vec::new());
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

#[test]
fn server_init_stores_canonical_data_dir() {
    let root = tempfile::TempDir::new().unwrap();
    let child = root.path().join("child");
    fs::create_dir(&child).unwrap();
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
fn server_init_honors_recovery_target_lsn_before_installing_wal_writer() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let wal_dir = data_dir.path().join("pg_wal");
    fs::create_dir_all(&wal_dir).unwrap();

    let first = WalRecord::new(RecordType::Nop, Xid::new(10), Lsn::ZERO, 0, Vec::new());
    let first_bytes = first.encode();
    let second = WalRecord::new(RecordType::Nop, Xid::new(11), Lsn::ZERO, 0, Vec::new());
    let target = Lsn::new(u64::try_from(first_bytes.len()).unwrap());
    let mut segment = first_bytes;
    segment.extend_from_slice(&second.encode());
    fs::write(wal_dir.join("segment_0000000000"), segment).unwrap();
    fs::write(
        data_dir.path().join("recovery.targets"),
        format!("recovery_target_lsn = '{}'\n", target.raw()),
    )
    .unwrap();

    let server = Server::init(data_dir.path()).unwrap();
    let sink = server
        .heap
        .buffer_pool()
        .wal_sink()
        .expect("recovered persistent server must install WAL sink");
    let appended = sink
        .append(WalRecord::new(
            RecordType::Nop,
            Xid::new(12),
            Lsn::ZERO,
            0,
            Vec::new(),
        ))
        .unwrap();

    assert_eq!(appended, target);
}
