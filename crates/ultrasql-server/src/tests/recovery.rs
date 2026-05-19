//! WAL recovery target tests.

use std::sync::Arc;

use ultrasql_core::{Lsn, RelationId, Xid};
use ultrasql_storage::buffer_pool::BufferPool;
use ultrasql_storage::heap::HeapAccess;
use ultrasql_wal::payload::{SequenceOpKind, SequenceOpPayload};
use ultrasql_wal::{HeapTarget, RecordType, WalRecord};

use super::super::{BlankPageLoader, Server, ServerRecoveryTarget};

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
