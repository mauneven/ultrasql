//! btree access-method unit tests.

use super::*;

// --- BTreeAccessMethod ---

#[test]
fn btree_insert_then_lookup_round_trip() {
    let am = BTreeAccessMethod::new(true);
    let key = b"hello";
    am.insert(key, tid(1, 0)).expect("insert succeeds");
    let results = am.lookup(key).expect("lookup succeeds");
    assert_eq!(results, vec![tid(1, 0)]);
}

#[test]
fn btree_unique_rejects_duplicate() {
    let am = BTreeAccessMethod::new(true);
    let key = b"key";
    am.insert(key, tid(1, 0)).expect("first insert succeeds");
    let err = am.insert(key, tid(2, 0)).expect_err("duplicate rejected");
    assert!(matches!(err, AccessMethodError::DuplicateKey));
}

#[test]
fn btree_non_unique_allows_duplicate_keys() {
    let am = BTreeAccessMethod::new(false);
    let key = b"key";
    am.insert(key, tid(1, 0)).expect("first insert");
    am.insert(key, tid(2, 0)).expect("second insert same key");
    let results = am.lookup(key).expect("lookup");
    assert_eq!(results.len(), 2);
}

#[test]
fn btree_delete_removes_entry() {
    let am = BTreeAccessMethod::new(false);
    let key = b"del";
    am.insert(key, tid(3, 1)).expect("insert");
    am.delete(key, tid(3, 1)).expect("delete");
    assert!(am.lookup(key).expect("lookup after delete").is_empty());
}

#[test]
fn btree_lookup_missing_key_returns_empty() {
    let am = BTreeAccessMethod::new(true);
    assert!(am.lookup(b"missing").expect("lookup").is_empty());
}
