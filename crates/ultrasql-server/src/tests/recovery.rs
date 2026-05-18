//! WAL recovery target tests.

use std::sync::Arc;

use ultrasql_core::RelationId;
use ultrasql_storage::buffer_pool::BufferPool;
use ultrasql_storage::heap::HeapAccess;
use ultrasql_wal::HeapTarget;
use ultrasql_wal::payload::{SequenceOpKind, SequenceOpPayload};

use super::super::{BlankPageLoader, ServerRecoveryTarget};

fn recovery_target() -> ServerRecoveryTarget {
    let pool = Arc::new(BufferPool::new(16, BlankPageLoader));
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
