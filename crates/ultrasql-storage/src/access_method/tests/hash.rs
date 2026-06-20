//! hash access-method unit tests.

use super::*;
use crate::wal_sink::test_support::InMemoryWalSink;
use ultrasql_core::{RelationId, Xid};
use ultrasql_wal::payload::{HashOpKind, HashOpPayload};
use ultrasql_wal::record::RecordType;

// --- HashIndex ---

#[test]
fn hash_insert_then_lookup_happy_path() {
    let am = HashIndex::new(64);
    let key = b"token";
    am.insert(key, tid(7, 0)).expect("hash insert");
    let results = am.lookup(key).expect("hash lookup");
    assert!(results.contains(&tid(7, 0)));
}

#[test]
fn hash_delete_removes_entry() {
    let am = HashIndex::new(64);
    let key = b"rm";
    am.insert(key, tid(1, 0)).expect("insert");
    am.delete(key, tid(1, 0)).expect("delete");
    assert!(am.lookup(key).expect("lookup").is_empty());
}

#[test]
fn hash_delete_nonexistent_returns_not_found() {
    let am = HashIndex::new(64);
    let err = am.delete(b"ghost", tid(0, 0)).expect_err("not found");
    assert!(matches!(err, AccessMethodError::NotFound));
}

#[test]
fn hash_static_bucket_allocates_overflow_pages() {
    let am = HashIndex::with_page_capacity(1, 2);
    am.insert(b"a", tid(1, 0)).expect("insert a");
    am.insert(b"b", tid(1, 1)).expect("insert b");
    am.insert(b"c", tid(1, 2)).expect("insert c");

    assert_eq!(am.overflow_page_count(), 1);
    assert_eq!(am.lookup(b"c").expect("lookup c"), vec![tid(1, 2)]);
}

#[test]
fn hash_insert_logged_emits_hash_wal_record() {
    let am = HashIndex::new(64);
    let sink = InMemoryWalSink::new();
    let index_rel = RelationId::new(1234);
    let key = b"logged";
    let tuple = tid(7, 3);

    am.insert_logged(index_rel, key, tuple, Xid::new(44), Some(&sink))
        .expect("logged insert");

    let records = sink.records();
    assert_eq!(records.len(), 1);
    let record = &records[0].1;
    assert_eq!(record.header.record_type, RecordType::HashOp);
    let payload = HashOpPayload::decode(&record.payload).expect("decode hash WAL");
    assert_eq!(payload.op, HashOpKind::Insert);
    assert_eq!(payload.index_rel, index_rel);
    assert_eq!(payload.key_bytes, key);
    assert_eq!(payload.value_bytes, HashIndex::tuple_id_bytes(tuple));
}

#[test]
fn hash_delete_logged_emits_hash_wal_record() {
    let am = HashIndex::new(64);
    let sink = InMemoryWalSink::new();
    let index_rel = RelationId::new(4321);
    let key = b"delete";
    let tuple = tid(8, 4);

    am.insert_logged(index_rel, key, tuple, Xid::new(55), Some(&sink))
        .expect("logged insert");
    am.delete_logged(index_rel, key, tuple, Xid::new(55), Some(&sink))
        .expect("logged delete");

    let records = sink.records();
    assert_eq!(records.len(), 2);
    let payload = HashOpPayload::decode(&records[1].1.payload).expect("decode hash WAL");
    assert_eq!(payload.op, HashOpKind::Delete);
    assert_eq!(payload.index_rel, index_rel);
    assert_eq!(payload.key_bytes, key);
}
