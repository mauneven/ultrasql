//! gist access-method unit tests.

use super::*;

// --- GistIndex ---

#[test]
fn gist_insert_then_lookup_happy_path() {
    let am = GistIndex::new();
    let key = b"\x00\x00\x00\x0a\x00\x00\x00\x14"; // bbox [10, 20]
    am.insert(key, tid(3, 0)).expect("gist insert");
    let results = am.lookup(key).expect("gist lookup");
    assert!(results.contains(&tid(3, 0)));
}

#[test]
fn gist_delete_entry() {
    let am = GistIndex::new();
    let key = b"bbox";
    am.insert(key, tid(4, 0)).expect("insert");
    am.delete(key, tid(4, 0)).expect("delete");
    assert!(am.lookup(key).expect("lookup").is_empty());
}
