//! Description, statistics, and ALTER TABLE persistence tests.

use super::*;

    #[test]
    fn set_description_updates_snapshot_and_clear_removes_rows() {
        let cat = PersistentCatalog::new();
        let heap = blank_heap();
        cat.bootstrap_from_heap(&heap).expect("bootstrap");

        let objoid = Oid::new(42_000);
        let classoid = Oid::new(crate::bootstrap::PG_CLASS_OID);
        cat.set_description(objoid, classoid, 0, Some("table docs".to_owned()));
        let snap = cat.snapshot();
        let row = snap
            .descriptions
            .get(&(objoid, classoid, 0))
            .expect("description row present");
        assert_eq!(row.description, "table docs");

        cat.set_description(objoid, classoid, 1, Some("column docs".to_owned()));
        assert_eq!(cat.snapshot().descriptions.len(), 2);

        cat.clear_descriptions_for_object(objoid);
        assert!(cat.snapshot().descriptions.is_empty());
    }

    #[test]
    fn statistics_updates_publish_snapshot_rows() {
        let cat = PersistentCatalog::new();
        let table_oid = Oid::new(42_001);
        cat.replace_statistics(
            table_oid,
            [
                StatisticRow {
                    starelid: table_oid,
                    staattnum: 1,
                    stanullfrac: 0.25,
                    stadistinct: -0.75,
                },
                StatisticRow {
                    starelid: table_oid,
                    staattnum: 2,
                    stanullfrac: 0.0,
                    stadistinct: 3.0,
                },
            ],
        );
        assert_eq!(cat.snapshot().statistics.len(), 2);
        cat.replace_statistics(
            table_oid,
            [StatisticRow {
                starelid: table_oid,
                staattnum: 1,
                stanullfrac: 0.0,
                stadistinct: 1.0,
            }],
        );
        let snap = cat.snapshot();
        assert_eq!(snap.statistics.len(), 1);
        assert_eq!(
            snap.statistics
                .get(&(table_oid, 1))
                .expect("stat row")
                .stadistinct,
            1.0
        );
    }

    #[test]
    fn statistic_ext_create_publishes_snapshot_row() {
        let cat = PersistentCatalog::new();
        let oid = Oid::new(42_002);
        cat.create_statistic_ext(StatisticExtRow {
            oid,
            stxname: "s_ab".to_owned(),
            stxrelid: Oid::new(42_001),
            stxkeys: vec![1, 2],
            stxkind: vec!['d', 'f', 'm'],
        })
        .expect("create statistic ext");
        let snap = cat.snapshot();
        let row = snap.statistic_ext.get(&oid).expect("statistic ext row");
        assert_eq!(row.stxname, "s_ab");
        assert_eq!(row.stxkeys, vec![1, 2]);
    }

    #[test]
    fn statistic_ext_remove_by_relation_updates_snapshot() {
        let cat = PersistentCatalog::new();
        let table_oid = Oid::new(42_001);
        let keep_oid = Oid::new(42_099);
        for (oid, name, stxrelid) in [
            (Oid::new(42_002), "s_ab", table_oid),
            (Oid::new(42_003), "s_bc", table_oid),
            (keep_oid, "s_keep", Oid::new(42_098)),
        ] {
            cat.create_statistic_ext(StatisticExtRow {
                oid,
                stxname: name.to_owned(),
                stxrelid,
                stxkeys: vec![1, 2],
                stxkind: vec!['d'],
            })
            .expect("create statistic ext");
        }

        assert_eq!(cat.remove_statistic_ext_for_relation(table_oid), 2);
        let snap = cat.snapshot();
        assert_eq!(snap.statistic_ext.len(), 1);
        assert!(snap.statistic_ext.contains_key(&keep_oid));
        assert_eq!(cat.remove_statistic_ext_for_relation(table_oid), 0);
    }

    /// `alter_table_add_column` on the persistent catalog extends the
    /// schema, preserves the OID, and the new entry is reflected in the
    /// next snapshot taken via `ArcSwap`.
    #[test]
    fn alter_table_add_column_persistent_updates_snapshot() {
        use ultrasql_core::{DataType, Field};

        let cat = PersistentCatalog::new();
        let entry = make_table(&cat, "items");
        let oid = entry.oid;
        cat.create_table(entry).expect("create");

        let new_col = Field::nullable("note", DataType::Text { max_len: None });
        let updated = cat
            .alter_table_add_column("items", new_col.clone())
            .expect("ALTER ADD COLUMN");
        assert_eq!(updated.oid, oid);
        assert_eq!(updated.schema.len(), 3);
        assert_eq!(updated.schema.field_at(2), &new_col);

        // Fresh snapshot reflects the wider schema.
        let snap = cat.snapshot();
        let snap_entry = snap.tables.get("items").expect("present");
        assert_eq!(snap_entry.schema.len(), 3);
        assert_eq!(snap_entry.oid, oid);
    }

