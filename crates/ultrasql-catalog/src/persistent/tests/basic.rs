//! CRUD, trait lookups, schema moves, index, and overflow tests.

use super::*;

    #[test]
    fn create_and_lookup_via_snapshot() {
        let cat = PersistentCatalog::new();
        let entry = make_table(&cat, "orders");
        let oid = entry.oid;
        cat.create_table(entry.clone()).expect("create");

        let snap = cat.snapshot();
        assert!(snap.tables.contains_key("orders"));
        assert_eq!(snap.tables_by_oid[&oid], entry);
    }

    #[test]
    fn drop_removes_from_snapshot() {
        let cat = PersistentCatalog::new();
        cat.create_table(make_table(&cat, "users")).expect("create");
        cat.drop_table("users").expect("drop");
        let snap = cat.snapshot();
        assert!(!snap.tables.contains_key("users"));
    }

    // --- Catalog trait delegation ---

    #[test]
    fn catalog_trait_lookup_table_by_name() {
        let cat = PersistentCatalog::new();
        let entry = make_table(&cat, "products");
        let oid = entry.oid;
        cat.create_table(entry).expect("create");
        assert!(cat.lookup_table("products").is_some());
        assert!(cat.lookup_table_by_oid(oid).is_some());
    }

    #[test]
    fn catalog_trait_list_tables() {
        let cat = PersistentCatalog::new();
        cat.create_table(make_table(&cat, "a")).expect("a");
        cat.create_table(make_table(&cat, "b")).expect("b");
        assert_eq!(cat.list_tables().len(), 2);
    }

    #[test]
    fn alter_table_set_schema_folds_target_and_rebuilds_snapshot() {
        let cat = PersistentCatalog::new();
        let entry = make_table(&cat, "users");
        let oid = entry.oid;
        cat.create_table(entry).expect("create");

        let updated = cat
            .alter_table_set_schema("USERS", "App")
            .expect("schema move succeeds");
        assert_eq!(updated.oid, oid);
        assert_eq!(updated.schema_name, "app");
        assert!(cat.lookup_table("users").is_none());
        assert_eq!(
            cat.lookup_table_in_schema("APP", "users")
                .expect("moved table reachable by schema")
                .oid,
            oid
        );
        assert_eq!(
            cat.snapshot()
                .tables_by_oid
                .get(&oid)
                .expect("snapshot rebuilt")
                .schema_name,
            "app"
        );

        let same_schema = cat
            .alter_table_set_schema("app.users", "APP")
            .expect("same-schema move is idempotent");
        assert_eq!(same_schema.oid, oid);
        assert_eq!(same_schema.schema_name, "app");
    }

    #[test]
    fn alter_table_set_schema_rejects_target_collision_without_rebuilding_source() {
        let cat = PersistentCatalog::new();
        let source = make_table(&cat, "users");
        let source_oid = source.oid;
        cat.create_table(source).expect("create public source");
        cat.create_table(make_table_in_schema(&cat, "app", "users"))
            .expect("create app collision target");

        let err = cat
            .alter_table_set_schema("users", "app")
            .expect_err("schema collision must fail");
        assert!(matches!(err, CatalogError::AlreadyExists(_)));
        assert_eq!(
            cat.lookup_table("users")
                .expect("source remains in public")
                .oid,
            source_oid
        );
    }

    // --- Index management ---

    #[test]
    fn index_create_and_list() {
        let cat = PersistentCatalog::new();
        let tbl = make_table(&cat, "items");
        let toid = tbl.oid;
        cat.create_table(tbl).expect("create");
        let idx = IndexEntry::new(cat.next_oid(), "items_pk", toid, vec![0], true);
        cat.create_index(idx).expect("idx create");
        let snap = cat.snapshot();
        assert!(snap.indexes.contains_key("items_pk"));
        assert!(!snap.indexes_by_table[&toid].is_empty());
    }

    // --- pg_class insert ---

    #[test]
    fn pg_class_row_can_be_inserted() {
        let cat = PersistentCatalog::new();
        let oid = cat.next_oid();
        cat.pg_class.insert(
            oid,
            ClassRow {
                oid,
                relname: "widgets".into(),
                relnamespace: Oid::new(2200),
                relkind: RelKind::Table,
                relpages: 0,
                reltuples: 0.0,
                relfilenode: 0,
                relhasindex: false,
                reloptions: Vec::new(),
            },
        );
        assert!(cat.pg_class.contains_key(&oid));
        assert_eq!(cat.pg_class.get(&oid).unwrap().relname, "widgets");
    }

    // --- Update table size ---

    #[test]
    fn update_table_size_reflects_in_snapshot() {
        let cat = PersistentCatalog::new();
        let entry = make_table(&cat, "logs");
        let oid = entry.oid;
        cat.create_table(entry).expect("create");
        cat.update_table_size(oid, 42).expect("update");
        let snap = cat.snapshot();
        assert_eq!(snap.tables_by_oid[&oid].n_blocks, 42);
    }

    #[test]
    fn attnum_overflow_returns_catalog_error() {
        let overflowing_index =
            usize::try_from(i64::from(i16::MAX)).expect("usize stores i16::MAX");
        let err =
            attnum_for_index(overflowing_index, "composite type c").expect_err("attnum overflow");
        assert!(
            matches!(err, CatalogError::SchemaConflict(message) if message.contains("too many attributes"))
        );
    }

    #[test]
    fn install_snapshot_attnum_overflow_preserves_existing_snapshot() {
        let cat = PersistentCatalog::new();
        let heap = blank_heap();
        cat.bootstrap_from_heap(&heap).expect("bootstrap");
        let before = cat.snapshot();
        let mut snap = (*before).clone();
        let field_count =
            usize::try_from(i64::from(i16::MAX) + 1).expect("usize stores overflow field count");
        let fields = (0..field_count)
            .map(|idx| Field::required(format!("c{idx}"), DataType::Int32))
            .collect::<Vec<_>>();
        let schema = Schema::new(fields).expect("many unique fields");
        let entry = CompositeTypeEntry {
            oid: cat.next_oid(),
            name: "too_wide".to_owned(),
            schema_name: "public".to_owned(),
            schema,
        };
        snap.composite_types
            .insert(type_entry_key(&entry), entry.clone());
        snap.composite_types_by_oid.insert(entry.oid, entry.clone());

        let err = cat
            .install_snapshot(snap)
            .expect_err("attnum overflow rejects snapshot");
        assert!(
            matches!(err, CatalogError::SchemaConflict(message) if message.contains("too many attributes"))
        );
        let after = cat.snapshot();
        assert_eq!(after.tables.len(), before.tables.len());
        assert!(!after.composite_types.contains_key(&type_entry_key(&entry)));
    }

    // -----------------------------------------------------------------------
    // Bootstrap tests (E)
    // -----------------------------------------------------------------------

