//! gin access-method unit tests.

use super::*;

// --- GinIndex ---

#[test]
fn gin_insert_then_lookup_happy_path() {
    let am = GinIndex::new();
    let token = b"rust";
    am.insert(token, tid(5, 2)).expect("gin insert");
    let posting = am.lookup(token).expect("gin lookup");
    assert!(posting.contains(&tid(5, 2)));
}

#[test]
fn gin_multiple_tokens_per_document() {
    let am = GinIndex::new();
    am.insert(b"cat", tid(1, 0)).expect("insert cat");
    am.insert(b"dog", tid(1, 0)).expect("insert dog");
    assert!(am.lookup(b"cat").expect("cat").contains(&tid(1, 0)));
    assert!(am.lookup(b"dog").expect("dog").contains(&tid(1, 0)));
    assert!(am.lookup(b"bird").expect("bird").is_empty());
}

#[test]
fn gin_fast_update_drains_pending_list() {
    let am = GinIndex::new();
    am.insert(b"json-key", tid(2, 0)).expect("insert");
    am.insert(b"json-key", tid(2, 1)).expect("insert");

    assert_eq!(am.pending_len(), 2);
    assert_eq!(am.drain_pending_list(), 2);
    assert_eq!(am.pending_len(), 0);
    assert_eq!(
        am.lookup(b"json-key").expect("lookup"),
        vec![tid(2, 0), tid(2, 1)]
    );
}

#[test]
fn gin_delete_removes_posting() {
    let am = GinIndex::new();
    am.insert(b"tok", tid(2, 0)).expect("insert");
    am.delete(b"tok", tid(2, 0)).expect("delete");
    assert!(am.lookup(b"tok").expect("lookup").is_empty());
}

#[test]
fn gin_jsonb_operator_tokens_cover_contains_and_keys() {
    let am = GinIndex::new();
    am.insert_jsonb_document(r#"{"a":1,"b":"two"}"#, tid(9, 0))
        .expect("insert jsonb");
    am.insert_jsonb_document(r#"{"a":2,"c":3}"#, tid(9, 1))
        .expect("insert jsonb");

    assert_eq!(
        am.lookup_jsonb_contains(r#"{"a":1}"#)
            .expect("jsonb contains"),
        vec![tid(9, 0)]
    );
    assert_eq!(
        am.lookup_jsonb_has_any_key(&["b".to_owned(), "z".to_owned()])
            .expect("jsonb any key"),
        vec![tid(9, 0)]
    );
    assert_eq!(
        am.lookup_jsonb_has_all_keys(&["a".to_owned(), "c".to_owned()])
            .expect("jsonb all keys"),
        vec![tid(9, 1)]
    );
}

#[test]
fn gin_array_operator_tokens_cover_contains_and_overlap() {
    let am = GinIndex::new();
    am.insert_array_value("{red,green}", tid(10, 0))
        .expect("insert array");
    am.insert_array_value("{blue,green}", tid(10, 1))
        .expect("insert array");

    assert_eq!(
        am.lookup_array_contains("{red,green}")
            .expect("array contains"),
        vec![tid(10, 0)]
    );
    assert_eq!(
        am.lookup_array_overlap("{green}").expect("array overlap"),
        vec![tid(10, 0), tid(10, 1)]
    );
}

#[test]
fn gin_tsvector_operator_tokens_cover_match() {
    let am = GinIndex::new();
    am.insert_tsvector("quick brown fox", tid(11, 0))
        .expect("insert tsvector");
    am.insert_tsvector("slow green turtle", tid(11, 1))
        .expect("insert tsvector");

    assert_eq!(
        am.lookup_tsquery_match("quick & fox").expect("tsquery"),
        vec![tid(11, 0)]
    );
}
