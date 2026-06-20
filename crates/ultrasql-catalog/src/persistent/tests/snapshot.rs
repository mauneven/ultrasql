//! Bootstrap, wait-free snapshot, and concurrency tests.

use super::*;

/// `bootstrap_from_heap` on a fresh database (empty heap) installs the
/// initial snapshot that contains the 13 system relations.
#[test]
fn bootstrap_from_empty_heap_installs_initial_snapshot() {
    let cat = PersistentCatalog::new();
    let heap = blank_heap();
    let stats = cat
        .bootstrap_from_heap(&heap)
        .expect("bootstrap must not fail on empty heap");

    // Stats reflect the initial snapshot counts.
    assert_eq!(stats.namespaces, 3);
    assert_eq!(stats.relations, 13);

    // The snapshot contains all 13 system relations.
    let snap = cat.snapshot();
    assert_eq!(snap.tables.len(), 13);
    assert!(snap.tables.contains_key("pg_class"));
    assert!(snap.tables.contains_key("pg_attribute"));
    assert!(snap.tables.contains_key("pg_attrdef"));
    assert!(snap.tables.contains_key("pg_type"));
    assert!(snap.tables.contains_key("pg_enum"));
    assert!(snap.tables.contains_key("pg_namespace"));
}

/// `snapshot()` returns an `Arc<CatalogSnapshot>` via `arc_swap` `load_full`
/// — a wait-free operation. We verify the Arc is stable across a
/// concurrent write.
#[test]
fn snapshot_returns_wait_free_arc_load() {
    let cat = PersistentCatalog::new();
    let heap = blank_heap();
    cat.bootstrap_from_heap(&heap).expect("bootstrap");

    // Capture snapshot before any mutation.
    let snap_before = cat.snapshot();
    assert_eq!(snap_before.tables.len(), 13);

    // Add a table — this swaps in a new snapshot.
    cat.create_table(make_table(&cat, "user_orders"))
        .expect("create");

    // The old snapshot reference is still valid and unchanged.
    assert_eq!(snap_before.tables.len(), 13);

    // A fresh snapshot call reflects the new state.
    let snap_after = cat.snapshot();
    assert_eq!(snap_after.tables.len(), 14);
}

/// N threads each take a snapshot concurrently; all must see the same
/// data and none must deadlock or panic.
#[test]
fn multiple_concurrent_snapshots_consistent() {
    use std::thread;
    const THREADS: usize = 16;

    let cat = std::sync::Arc::new(PersistentCatalog::new());
    let heap = blank_heap();
    cat.bootstrap_from_heap(&heap).expect("bootstrap");

    let counts: Vec<usize> = (0..THREADS)
        .map(|_| {
            let cat = std::sync::Arc::clone(&cat);
            thread::spawn(move || {
                let snap = cat.snapshot();
                snap.tables.len()
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|h| h.join().expect("thread panicked"))
        .collect();

    // Every thread must see the same count.
    let first = counts[0];
    assert!(counts.iter().all(|&c| c == first));
    assert_eq!(first, 13);
}

/// After installing a new snapshot via `install_snapshot`, the very next
/// `snapshot()` call must return the new state.
#[test]
fn install_snapshot_after_ddl_is_observable_on_next_snapshot() {
    let cat = PersistentCatalog::new();
    let heap = blank_heap();
    cat.bootstrap_from_heap(&heap).expect("bootstrap");

    // Snapshot A: 13 system tables.
    let snap_a = cat.snapshot();
    assert_eq!(snap_a.tables.len(), 13);

    // Build a richer snapshot with an additional table.
    let mut tables = snap_a.tables.clone();
    let mut tables_by_oid = snap_a.tables_by_oid.clone();
    let entry = make_table(&cat, "extra_table");
    tables.insert("extra_table".to_owned(), entry.clone());
    tables_by_oid.insert(entry.oid, entry);
    let snap_b = CatalogSnapshot {
        tables,
        tables_by_oid,
        indexes: snap_a.indexes.clone(),
        indexes_by_table: snap_a.indexes_by_table.clone(),
        enum_types: snap_a.enum_types.clone(),
        enum_types_by_oid: snap_a.enum_types_by_oid.clone(),
        composite_types: snap_a.composite_types.clone(),
        composite_types_by_oid: snap_a.composite_types_by_oid.clone(),
        domain_types: snap_a.domain_types.clone(),
        domain_types_by_oid: snap_a.domain_types_by_oid.clone(),
        constraints: snap_a.constraints.clone(),
        descriptions: snap_a.descriptions.clone(),
        statistics: snap_a.statistics.clone(),
        statistic_ext: snap_a.statistic_ext.clone(),
    };
    cat.install_snapshot(snap_b).expect("install snapshot");

    // Snapshot B must be visible immediately.
    let snap_after = cat.snapshot();
    assert_eq!(snap_after.tables.len(), 14);
    assert!(snap_after.tables.contains_key("extra_table"));
}
