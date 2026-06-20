//! Bootstrap heap round-trip preservation tests.

use super::*;

/// Round-trip a user-defined table through `persist_table_rows`
/// → `bootstrap_from_heap`. The relation must survive the round-
/// trip with its full schema (column names, types, nullability).
#[test]
fn bootstrap_round_trip_preserves_known_relation() {
    use std::sync::Arc;
    use ultrasql_core::{CommandId, DataType, Field, PageId, Schema, Xid};
    use ultrasql_storage::buffer_pool::BufferPool;
    use ultrasql_storage::heap::HeapAccess;
    use ultrasql_storage::page::Page;

    let pool = Arc::new(BufferPool::new(64, |_: PageId| Ok(Page::new_heap())));
    let heap = HeapAccess::new(pool);

    // Build a representative user table and persist its rows.
    let cat = PersistentCatalog::new();
    let oid = cat.next_oid();
    let entry = TableEntry::new(
        oid,
        "orders".to_owned(),
        "public".to_owned(),
        Schema::new(vec![
            Field {
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "amount".into(),
                data_type: DataType::Int64,
                nullable: true,
            },
        ])
        .expect("schema"),
    );
    cat.create_table(entry.clone()).expect("create_table");
    cat.persist_table_rows(&entry, &heap, Xid::new(1), CommandId::new(0))
        .expect("persist_table_rows");

    // Reset to a clean catalog and bootstrap from the heap pages
    // that the previous step wrote.
    let cat2 = PersistentCatalog::new();
    let stats = cat2
        .bootstrap_from_heap(&heap)
        .expect("bootstrap must succeed");
    // Initial system relations plus the one user table.
    assert!(stats.relations >= 11);
    assert_eq!(stats.attributes, 2);

    let snap = cat2.snapshot();
    let restored = snap
        .tables
        .get("orders")
        .expect("user relation present after bootstrap");
    assert_eq!(restored.oid, oid);
    assert_eq!(restored.schema.fields().len(), 2);
    assert_eq!(restored.schema.fields()[0].name, "id");
    assert_eq!(restored.schema.fields()[0].data_type, DataType::Int32);
    assert!(!restored.schema.fields()[0].nullable);
    assert_eq!(restored.schema.fields()[1].name, "amount");
    assert_eq!(restored.schema.fields()[1].data_type, DataType::Int64);
    assert!(restored.schema.fields()[1].nullable);
}

#[test]
fn bootstrap_rejects_max_oid_relation_without_successor() {
    use ultrasql_core::{CommandId, Xid};

    let heap = blank_heap();
    let cat = PersistentCatalog::new();
    let entry = TableEntry::new(
        Oid::new(u32::MAX),
        "max_oid_table".to_owned(),
        "public".to_owned(),
        sample_schema(),
    );
    cat.create_table(entry.clone()).expect("create table");
    cat.persist_table_rows(&entry, &heap, Xid::new(1), CommandId::new(0))
        .expect("persist table rows");

    let cat2 = PersistentCatalog::new();
    let err = cat2
        .bootstrap_from_heap(&heap)
        .expect_err("max oid row should reject restart");

    assert!(
        matches!(err, CatalogError::SchemaConflict(message) if message.contains("catalog OID space exhausted"))
    );
}

#[test]
fn bootstrap_round_trip_preserves_atthasdef_metadata() {
    use std::sync::Arc;
    use ultrasql_core::{CommandId, DataType, Field, PageId, Schema, Xid};
    use ultrasql_storage::buffer_pool::BufferPool;
    use ultrasql_storage::heap::HeapAccess;
    use ultrasql_storage::page::Page;

    let pool = Arc::new(BufferPool::new(64, |_: PageId| Ok(Page::new_heap())));
    let heap = HeapAccess::new(pool);
    let cat = PersistentCatalog::new();
    let oid = cat.next_oid();
    let entry = TableEntry::new(
        oid,
        "defaults_demo".to_owned(),
        "public".to_owned(),
        Schema::new(vec![
            Field::required("id", DataType::Int64),
            Field::nullable("note", DataType::Text { max_len: None }),
        ])
        .expect("schema"),
    );

    cat.persist_table_rows_with_defaults(
        &entry,
        &[true, false],
        &heap,
        Xid::new(1),
        CommandId::new(0),
    )
    .expect("persist table rows with defaults");

    let cat2 = PersistentCatalog::new();
    cat2.bootstrap_from_heap(&heap).expect("bootstrap");

    assert!(cat2.pg_attribute.get(&(oid, 1)).expect("id attr").atthasdef);
    assert!(
        !cat2
            .pg_attribute
            .get(&(oid, 2))
            .expect("note attr")
            .atthasdef
    );
}

#[test]
fn bootstrap_round_trip_preserves_index_entry() {
    use std::sync::Arc;
    use ultrasql_core::{CommandId, DataType, Field, PageId, Schema, Xid};
    use ultrasql_storage::buffer_pool::BufferPool;
    use ultrasql_storage::heap::HeapAccess;
    use ultrasql_storage::page::Page;

    let pool = Arc::new(BufferPool::new(64, |_: PageId| Ok(Page::new_heap())));
    let heap = HeapAccess::new(pool);

    let cat = PersistentCatalog::new();
    let table_oid = cat.next_oid();
    let table = TableEntry::new(
        table_oid,
        "orders".to_owned(),
        "public".to_owned(),
        Schema::new(vec![
            Field::required("id", DataType::Int64),
            Field::nullable("note", DataType::Text { max_len: None }),
        ])
        .expect("schema"),
    );
    cat.persist_table_rows(&table, &heap, Xid::new(1), CommandId::new(0))
        .expect("persist table");

    let mut index = IndexEntry::new(cat.next_oid(), "orders_id_idx", table_oid, vec![0], false);
    index.root_block = BlockNumber::new(7);
    cat.persist_index_rows(&index, &heap, Xid::new(2), CommandId::new(0))
        .expect("persist index");

    let cat2 = PersistentCatalog::new();
    let stats = cat2.bootstrap_from_heap(&heap).expect("bootstrap");
    assert_eq!(stats.indexes, 1);

    let snap = cat2.snapshot();
    let restored = snap.indexes.get("orders_id_idx").expect("index restored");
    assert_eq!(restored.oid, index.oid);
    assert_eq!(restored.table_oid, table_oid);
    assert_eq!(restored.columns, vec![0]);
    assert_eq!(restored.root_block, BlockNumber::new(7));
    assert!(!restored.is_unique);
    assert_eq!(snap.indexes_by_table[&table_oid], vec![restored.clone()]);
}

#[test]
fn indisprimary_follows_is_primary_not_the_pkey_name_heuristic() {
    use std::sync::Arc;
    use ultrasql_core::{CommandId, DataType, Field, PageId, Schema, Xid};
    use ultrasql_storage::buffer_pool::BufferPool;
    use ultrasql_storage::heap::HeapAccess;
    use ultrasql_storage::page::Page;

    let pool = Arc::new(BufferPool::new(64, |_: PageId| Ok(Page::new_heap())));
    let heap = HeapAccess::new(pool);

    let cat = PersistentCatalog::new();
    let table_oid = cat.next_oid();
    let table = TableEntry::new(
        table_oid,
        "orders".to_owned(),
        "public".to_owned(),
        Schema::new(vec![Field::required("id", DataType::Int64)]).expect("schema"),
    );
    cat.persist_table_rows(&table, &heap, Xid::new(1), CommandId::new(0))
        .expect("persist table");

    // A primary-key index whose name does NOT follow the `_pkey`
    // convention must still persist `indisprimary = true`.
    let primary = IndexEntry::new(cat.next_oid(), "orders_primary", table_oid, vec![0], true)
        .with_primary(true);
    cat.persist_index_rows(&primary, &heap, Xid::new(2), CommandId::new(0))
        .expect("persist primary index");

    // A user index that merely happens to be named `*_pkey` is not
    // primary and must persist `indisprimary = false`.
    let decoy = IndexEntry::new(cat.next_oid(), "orders_pkey", table_oid, vec![0], false);
    cat.persist_index_rows(&decoy, &heap, Xid::new(3), CommandId::new(0))
        .expect("persist decoy index");

    // Reload from the heap so the assertions read back the persisted
    // `pg_index.indisprimary` value (bootstrap copies it into
    // `IndexEntry::is_primary`), exercising the full round trip.
    let cat2 = PersistentCatalog::new();
    cat2.bootstrap_from_heap(&heap).expect("bootstrap");
    let snap = cat2.snapshot();

    let restored_primary = snap
        .indexes
        .get("orders_primary")
        .expect("primary index restored");
    assert!(
        restored_primary.is_primary,
        "non-_pkey-named primary index must report indisprimary = true"
    );

    let restored_decoy = snap
        .indexes
        .get("orders_pkey")
        .expect("decoy index restored");
    assert!(
        !restored_decoy.is_primary,
        "user index named *_pkey must report indisprimary = false"
    );
}

#[test]
fn bootstrap_round_trip_preserves_index_method_opclass_and_options() {
    use std::sync::Arc;
    use ultrasql_core::{CommandId, DataType, Field, PageId, Schema, Xid};
    use ultrasql_storage::buffer_pool::BufferPool;
    use ultrasql_storage::heap::HeapAccess;
    use ultrasql_storage::page::Page;

    let pool = Arc::new(BufferPool::new(64, |_: PageId| Ok(Page::new_heap())));
    let heap = HeapAccess::new(pool);

    let cat = PersistentCatalog::new();
    let table_oid = cat.next_oid();
    let table = TableEntry::new(
        table_oid,
        "embeddings".to_owned(),
        "public".to_owned(),
        Schema::new(vec![
            Field::required("id", DataType::Int64),
            Field::required("embedding", DataType::Vector { dims: Some(3) }),
        ])
        .expect("schema"),
    );
    cat.persist_table_rows(&table, &heap, Xid::new(1), CommandId::new(0))
        .expect("persist table");

    let mut index = IndexEntry::new(
        cat.next_oid(),
        "embeddings_hnsw_idx",
        table_oid,
        vec![1],
        false,
    );
    index.access_method = "hnsw".to_owned();
    index.opclasses = vec![Some("vector_l2_ops".to_owned())];
    index.options = vec![
        ("m".to_owned(), "16".to_owned()),
        ("ef_search".to_owned(), "64".to_owned()),
    ];
    cat.persist_index_rows(&index, &heap, Xid::new(2), CommandId::new(0))
        .expect("persist index");

    let cat2 = PersistentCatalog::new();
    cat2.bootstrap_from_heap(&heap).expect("bootstrap");

    let snap = cat2.snapshot();
    let restored = snap
        .indexes
        .get("embeddings_hnsw_idx")
        .expect("index restored");
    assert_eq!(restored.access_method, "hnsw");
    assert_eq!(restored.opclasses, vec![Some("vector_l2_ops".to_owned())]);
    assert_eq!(
        restored.options,
        vec![
            ("m".to_owned(), "16".to_owned()),
            ("ef_search".to_owned(), "64".to_owned()),
        ]
    );
}

#[test]
fn bootstrap_round_trip_preserves_pg_statistic_rows() {
    use std::sync::Arc;
    use ultrasql_core::{CommandId, DataType, Field, PageId, Schema, Xid};
    use ultrasql_storage::buffer_pool::BufferPool;
    use ultrasql_storage::heap::HeapAccess;
    use ultrasql_storage::page::Page;

    let pool = Arc::new(BufferPool::new(64, |_: PageId| Ok(Page::new_heap())));
    let heap = HeapAccess::new(pool);

    let cat = PersistentCatalog::new();
    let oid = cat.next_oid();
    let entry = TableEntry::new(
        oid,
        "orders".to_owned(),
        "public".to_owned(),
        Schema::new(vec![
            Field::required("id", DataType::Int32),
            Field::nullable("note", DataType::Text { max_len: None }),
        ])
        .expect("schema"),
    );
    cat.persist_table_rows(&entry, &heap, Xid::new(1), CommandId::new(0))
        .expect("persist table");
    cat.persist_statistic_rows(
        &[
            StatisticRow {
                starelid: oid,
                staattnum: 1,
                stanullfrac: 0.5,
                stadistinct: -0.25,
            },
            StatisticRow {
                starelid: oid,
                staattnum: 1,
                stanullfrac: 0.0,
                stadistinct: 10.0,
            },
            StatisticRow {
                starelid: oid,
                staattnum: 2,
                stanullfrac: 0.75,
                stadistinct: 2.0,
            },
            StatisticRow {
                starelid: Oid::new(999_999),
                staattnum: 1,
                stanullfrac: 0.0,
                stadistinct: 1.0,
            },
        ],
        &heap,
        Xid::new(2),
        CommandId::new(0),
    )
    .expect("persist statistics");

    let cat2 = PersistentCatalog::new();
    let stats = cat2.bootstrap_from_heap(&heap).expect("bootstrap");
    assert_eq!(stats.statistics, 2);

    let snap = cat2.snapshot();
    assert_eq!(snap.statistics.len(), 2);
    assert_eq!(
        snap.statistics
            .get(&(oid, 1))
            .expect("latest att1 row")
            .stadistinct,
        10.0
    );
    assert_eq!(
        snap.statistics
            .get(&(oid, 2))
            .expect("att2 row")
            .stanullfrac,
        0.75
    );
}

#[test]
fn bootstrap_round_trip_preserves_pg_statistic_ext_rows() {
    use std::sync::Arc;
    use ultrasql_core::{CommandId, DataType, Field, PageId, Schema, Xid};
    use ultrasql_storage::buffer_pool::BufferPool;
    use ultrasql_storage::heap::HeapAccess;
    use ultrasql_storage::page::Page;

    let pool = Arc::new(BufferPool::new(64, |_: PageId| Ok(Page::new_heap())));
    let heap = HeapAccess::new(pool);

    let cat = PersistentCatalog::new();
    let table_oid = cat.next_oid();
    let entry = TableEntry::new(
        table_oid,
        "orders".to_owned(),
        "public".to_owned(),
        Schema::new(vec![
            Field::required("id", DataType::Int32),
            Field::required("region", DataType::Int32),
        ])
        .expect("schema"),
    );
    cat.persist_table_rows(&entry, &heap, Xid::new(1), CommandId::new(0))
        .expect("persist table");

    let row = StatisticExtRow {
        oid: cat.next_oid(),
        stxname: "orders_stats".to_owned(),
        stxrelid: table_oid,
        stxkeys: vec![1, 2],
        stxkind: vec!['d', 'f', 'm'],
    };
    cat.persist_statistic_ext_row(&row, &heap, Xid::new(2), CommandId::new(0))
        .expect("persist statistic ext");

    let cat2 = PersistentCatalog::new();
    let stats = cat2.bootstrap_from_heap(&heap).expect("bootstrap");
    assert_eq!(stats.statistic_ext, 1);
    assert_eq!(
        cat2.snapshot()
            .statistic_ext
            .get(&row.oid)
            .expect("statistic ext row"),
        &row
    );
}
