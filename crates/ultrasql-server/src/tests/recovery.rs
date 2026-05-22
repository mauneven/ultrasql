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
fn recovery_target_time_is_rejected_until_transaction_aware_replay_exists() {
    let data_dir = tempfile::TempDir::new().unwrap();
    fs::write(
        data_dir.path().join("recovery.targets"),
        "recovery_target_time = '2026-05-22 00:00:00Z'\n",
    )
    .unwrap();

    let err = recovery_replay_target_from_data_dir(data_dir.path()).unwrap_err();

    assert!(
        err.to_string()
            .contains("recovery_target_time/recovery_target_xid")
    );
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
