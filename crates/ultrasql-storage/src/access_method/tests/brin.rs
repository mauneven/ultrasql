//! brin access-method unit tests.

use super::*;

// --- BrinIndex ---

#[test]
fn brin_insert_builds_summary() {
    let am = BrinIndex::new(128);
    // Insert a tuple in block 0 with key [42].
    am.insert(b"\x2a", tid(0, 0)).expect("brin insert");
    assert_eq!(am.summary_count(), 1);
    assert_eq!(am.candidate_ranges_for_key(b"\x2a"), vec![(0, 127)]);
    assert!(am.candidate_ranges_for_key(b"\x2b").is_empty());
    // Trait lookup still returns empty because callers need ranges.
    let _ = am.lookup(b"\x2a").expect("brin lookup");
}

#[test]
fn brin_summarize_range_stores_minmax() {
    let am = BrinIndex::new(128);
    am.summarize_range(0, 127, b"\x01".to_vec(), b"\xff".to_vec());
    assert_eq!(
        am.candidate_ranges_for_bounds(Some(b"\x80"), Some(b"\x90")),
        vec![(0, 127)]
    );
    assert!(
        am.candidate_ranges_for_bounds(Some(b"\xff\x00"), None)
            .is_empty()
    );
    let _ = am.lookup(b"\x80").expect("lookup in range");
}

#[test]
fn brin_i64_encoding_preserves_signed_order() {
    let keys = [
        BrinIndex::encode_i64_key(i64::MIN),
        BrinIndex::encode_i64_key(-1),
        BrinIndex::encode_i64_key(0),
        BrinIndex::encode_i64_key(1),
        BrinIndex::encode_i64_key(i64::MAX),
    ];
    assert!(keys.windows(2).all(|w| w[0] < w[1]));
}

#[test]
fn brin_delete_is_no_op() {
    let am = BrinIndex::new(128);
    am.insert(b"k", tid(0, 0)).expect("insert");
    // BRIN delete is always Ok — no per-tuple tracking.
    am.delete(b"k", tid(0, 0)).expect("brin delete no-op");
}
