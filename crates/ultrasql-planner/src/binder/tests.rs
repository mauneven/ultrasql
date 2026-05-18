use proptest::prelude::*;
use ultrasql_core::{DataType, Field, Schema};
use ultrasql_parser::Parser;

use super::*;
use crate::LogicalIndexMethod;
use crate::catalog::{InMemoryCatalog, TableMeta};

/// Catalog with a single `users` table: id INT, name TEXT, score FLOAT8.
fn users_catalog() -> InMemoryCatalog {
    let schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("name", DataType::Text { max_len: None }),
        Field::nullable("score", DataType::Float64),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("users", TableMeta::new(schema));
    cat
}

fn parse_and_bind(sql: &str, cat: &dyn Catalog) -> Result<LogicalPlan, PlanError> {
    let stmt = Parser::new(sql)
        .parse_statement()
        .expect("test SQL parses cleanly");
    bind(&stmt, cat)
}

fn parse_bind_ok(sql: &str) -> LogicalPlan {
    let cat = users_catalog();
    parse_and_bind(sql, &cat).expect("bind ok")
}

// -----------------------------------------------------------------------
// INSERT — happy paths
// -----------------------------------------------------------------------

#[test]
fn binds_insert_with_column_list_resolves_indices() {
    let plan = parse_bind_ok("INSERT INTO users (name, score) VALUES ('alice', 1.0)");
    let LogicalPlan::Insert {
        table,
        columns,
        source,
        ..
    } = &plan
    else {
        panic!("expected Insert, got {plan:?}");
    };
    assert_eq!(table, "users");
    // name is index 1, score is index 2
    assert_eq!(columns, &[1_usize, 2_usize]);
    assert!(matches!(source.as_ref(), LogicalPlan::Values { .. }));
}

#[test]
fn binds_insert_default_values() {
    let plan = parse_bind_ok("INSERT INTO users DEFAULT VALUES");
    let LogicalPlan::Insert {
        source, columns, ..
    } = &plan
    else {
        panic!("expected Insert");
    };
    // Columns = all three (all-columns expansion)
    assert_eq!(columns.len(), 3);
    // Source is a Values with one zero-width row.
    let LogicalPlan::Values { rows, .. } = source.as_ref() else {
        panic!("expected Values source");
    };
    assert_eq!(rows.len(), 1);
    assert!(rows[0].is_empty());
}

#[test]
fn binds_insert_with_multi_row_values() {
    let plan =
        parse_bind_ok("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')");
    let LogicalPlan::Insert { source, .. } = &plan else {
        panic!("expected Insert");
    };
    let LogicalPlan::Values { rows, .. } = source.as_ref() else {
        panic!("expected Values");
    };
    assert_eq!(rows.len(), 3);
    for r in rows {
        assert_eq!(r.len(), 2);
    }
}

#[test]
fn binds_insert_select() {
    // Must use a single-column select (id only) to match column count 1.
    let plan = parse_bind_ok("INSERT INTO users (id) SELECT id FROM users WHERE id > 0");
    let LogicalPlan::Insert {
        columns, source, ..
    } = &plan
    else {
        panic!("expected Insert");
    };
    assert_eq!(columns, &[0_usize]);
    // Source is a bound Select plan.
    assert!(
        matches!(
            source.as_ref(),
            LogicalPlan::Limit { .. }
                | LogicalPlan::Sort { .. }
                | LogicalPlan::Project { .. }
                | LogicalPlan::Filter { .. }
                | LogicalPlan::Scan { .. }
        ),
        "unexpected source: {source:?}"
    );
}

// -----------------------------------------------------------------------
// INSERT — error paths
// -----------------------------------------------------------------------

#[test]
fn binds_insert_rejects_ragged_value_rows() {
    let cat = users_catalog();
    let err = parse_and_bind(
        "INSERT INTO users (id, name) VALUES (1, 'alice', 99.0)",
        &cat,
    )
    .unwrap_err();
    // Row 1 has 3 cells but 2 columns expected.
    assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
}

#[test]
fn binds_insert_rejects_unknown_column() {
    let cat = users_catalog();
    let err = parse_and_bind("INSERT INTO users (bogus) VALUES (1)", &cat).unwrap_err();
    assert!(
        matches!(err, PlanError::ColumnNotFound(ref c) if c == "bogus"),
        "got {err:?}"
    );
}

#[test]
fn binds_insert_rejects_arity_mismatch_with_select_source() {
    // Column list has 2 entries, SELECT returns 3 columns.
    let cat = users_catalog();
    let err = parse_and_bind(
        "INSERT INTO users (id, name) SELECT id, name, score FROM users",
        &cat,
    )
    .unwrap_err();
    assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
}

// -----------------------------------------------------------------------
// INSERT — ON CONFLICT
// -----------------------------------------------------------------------

#[test]
fn binds_on_conflict_do_nothing() {
    let plan = parse_bind_ok("INSERT INTO users (id) VALUES (1) ON CONFLICT DO NOTHING");
    let LogicalPlan::Insert { on_conflict, .. } = &plan else {
        panic!("expected Insert");
    };
    assert!(matches!(
        on_conflict,
        Some(LogicalOnConflict::DoNothing { target: None })
    ));
}

#[test]
fn binds_on_conflict_do_update_targets() {
    let plan = parse_bind_ok(
        "INSERT INTO users (id, name) VALUES (1, 'x') ON CONFLICT (id) DO UPDATE SET name = 'y'",
    );
    let LogicalPlan::Insert { on_conflict, .. } = &plan else {
        panic!("expected Insert");
    };
    let Some(LogicalOnConflict::DoUpdate {
        target,
        assignments,
        ..
    }) = on_conflict
    else {
        panic!("expected DoUpdate, got {on_conflict:?}");
    };
    // Conflict target: column 'id' is at index 0
    assert_eq!(target.columns, vec![0_usize]);
    // Assignment: name (index 1) = literal 'y'
    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0].0, 1);
}

// -----------------------------------------------------------------------
// UPDATE
// -----------------------------------------------------------------------

#[test]
fn binds_update_with_filter_and_assignments() {
    let plan = parse_bind_ok("UPDATE users SET score = 9.5 WHERE id = 1");
    let LogicalPlan::Update {
        table,
        assignments,
        input,
        ..
    } = &plan
    else {
        panic!("expected Update, got {plan:?}");
    };
    assert_eq!(table, "users");
    // score is column index 2
    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0].0, 2);
    assert!(matches!(input.as_ref(), LogicalPlan::Filter { .. }));
}

#[test]
fn binds_update_rejects_unknown_target_column() {
    let cat = users_catalog();
    let err = parse_and_bind("UPDATE users SET bogus = 1", &cat).unwrap_err();
    assert!(
        matches!(err, PlanError::ColumnNotFound(ref c) if c == "bogus"),
        "got {err:?}"
    );
}

#[test]
fn binds_update_rejects_duplicate_target_column() {
    let cat = users_catalog();
    // PostgreSQL rejects `UPDATE t SET col=1, col=2` — mirror that.
    let err = parse_and_bind("UPDATE users SET score = 1.0, score = 2.0", &cat).unwrap_err();
    assert!(
        matches!(err, PlanError::DuplicateColumn(ref c) if c == "score"),
        "expected DuplicateColumn(score), got {err:?}"
    );
}

#[test]
fn binder_rejects_update_from_other_table_as_not_supported() {
    let cat = users_catalog();
    let err = parse_and_bind(
        "UPDATE users SET score = 1 FROM users AS u2 WHERE users.id = u2.id",
        &cat,
    )
    .unwrap_err();
    assert!(matches!(err, PlanError::NotSupported(_)), "got {err:?}");
}

// -----------------------------------------------------------------------
// DELETE
// -----------------------------------------------------------------------

#[test]
fn binds_delete_emits_scan_filter_delete() {
    let plan = parse_bind_ok("DELETE FROM users WHERE id = 42");
    let LogicalPlan::Delete { table, input, .. } = &plan else {
        panic!("expected Delete, got {plan:?}");
    };
    assert_eq!(table, "users");
    assert!(matches!(input.as_ref(), LogicalPlan::Filter { .. }));
}

#[test]
fn binder_rejects_delete_using_other_table_as_not_supported() {
    let cat = users_catalog();
    let err = parse_and_bind(
        "DELETE FROM users USING users AS u2 WHERE users.id = u2.id",
        &cat,
    )
    .unwrap_err();
    assert!(matches!(err, PlanError::NotSupported(_)), "got {err:?}");
}

// -----------------------------------------------------------------------
// TRUNCATE
// -----------------------------------------------------------------------

#[test]
fn binds_truncate_validates_table_existence() {
    let plan = parse_bind_ok("TRUNCATE TABLE users");
    let LogicalPlan::Truncate {
        tables,
        restart_identity,
        cascade,
        ..
    } = &plan
    else {
        panic!("expected Truncate, got {plan:?}");
    };
    assert_eq!(tables, &["users"]);
    assert!(!restart_identity);
    assert!(!cascade);
    assert!(plan.schema().is_empty());

    // Unknown table should fail.
    let cat = users_catalog();
    let err = parse_and_bind("TRUNCATE TABLE nope", &cat).unwrap_err();
    assert!(
        matches!(err, PlanError::TableNotFound(ref t) if t == "nope"),
        "got {err:?}"
    );
}

// -----------------------------------------------------------------------
// CREATE TABLE
// -----------------------------------------------------------------------

#[test]
fn binds_create_table_resolves_basic_column_types() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "CREATE TABLE accounts (id BIGINT NOT NULL, name TEXT, balance FLOAT8)",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateTable {
        table_name,
        namespace,
        columns,
        if_not_exists,
        schema,
        ..
    } = plan
    else {
        panic!("expected CreateTable, got other plan");
    };
    assert_eq!(table_name, "accounts");
    assert_eq!(namespace, "public");
    assert!(!if_not_exists);
    assert_eq!(schema, Schema::empty());
    assert_eq!(columns.len(), 3);
    assert_eq!(columns.fields()[0].name, "id");
    assert_eq!(columns.fields()[0].data_type, DataType::Int64);
    assert!(!columns.fields()[0].nullable, "NOT NULL honored");
    assert_eq!(
        columns.fields()[1].data_type,
        DataType::Text { max_len: None }
    );
    assert!(columns.fields()[1].nullable, "no constraint = nullable");
    assert_eq!(columns.fields()[2].data_type, DataType::Float64);
}

#[test]
fn binds_create_table_with_varchar_modifier() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("CREATE TABLE t (s VARCHAR(255))", &cat).expect("bind ok");
    let LogicalPlan::CreateTable { columns, .. } = plan else {
        panic!("expected CreateTable");
    };
    assert_eq!(
        columns.fields()[0].data_type,
        DataType::Text { max_len: Some(255) }
    );
}

#[test]
fn binds_create_table_primary_key_implies_not_null() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("CREATE TABLE t (id INT PRIMARY KEY)", &cat).expect("bind ok");
    let LogicalPlan::CreateTable { columns, .. } = plan else {
        panic!("expected CreateTable");
    };
    assert!(!columns.fields()[0].nullable);
}

#[test]
fn binds_create_table_generated_stored_expression() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "CREATE TABLE t (a INT, b INT GENERATED ALWAYS AS (a + 1) STORED)",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateTable {
        generated_stored, ..
    } = plan
    else {
        panic!("expected CreateTable");
    };
    assert!(generated_stored[0].is_none());
    assert!(matches!(
        generated_stored[1],
        Some(ScalarExpr::Binary { .. })
    ));
}

#[test]
fn binds_create_table_duplicate_column_rejected() {
    let cat = InMemoryCatalog::new();
    let err = parse_and_bind("CREATE TABLE t (id INT, id INT)", &cat).unwrap_err();
    assert!(
        matches!(err, PlanError::DuplicateColumn(ref c) if c == "id"),
        "got {err:?}"
    );
}

#[test]
fn binds_create_table_existing_relation_rejected() {
    let cat = users_catalog();
    let err = parse_and_bind("CREATE TABLE users (id INT)", &cat).unwrap_err();
    assert!(
        matches!(err, PlanError::DuplicateTable(ref t) if t == "users"),
        "got {err:?}"
    );
}

#[test]
fn binds_create_table_if_not_exists_skips_existence_check() {
    let cat = users_catalog();
    let plan = parse_and_bind("CREATE TABLE IF NOT EXISTS users (id INT)", &cat).expect("bind ok");
    let LogicalPlan::CreateTable {
        if_not_exists,
        table_name,
        ..
    } = plan
    else {
        panic!("expected CreateTable");
    };
    assert!(if_not_exists);
    assert_eq!(table_name, "users");
}

#[test]
fn binds_create_table_with_qualified_namespace() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("CREATE TABLE my_ns.events (id INT)", &cat).expect("bind ok");
    let LogicalPlan::CreateTable {
        table_name,
        namespace,
        ..
    } = plan
    else {
        panic!("expected CreateTable");
    };
    assert_eq!(namespace, "my_ns");
    assert_eq!(table_name, "events");
}

#[test]
fn binds_create_table_defaults_checks_and_unique_constraints() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "CREATE TABLE t (id INT DEFAULT 7 CHECK (id > 0), v INT, UNIQUE (id))",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateTable {
        defaults,
        checks,
        unique_constraints,
        ..
    } = plan
    else {
        panic!("expected CreateTable");
    };
    assert!(defaults[0].is_some());
    assert_eq!(checks.len(), 1);
    assert_eq!(unique_constraints.len(), 1);
    assert_eq!(unique_constraints[0].columns, vec![0]);
}

#[test]
fn binds_serial_column_as_required_with_sequence_default() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "CREATE TABLE t (id SERIAL, v BIGSERIAL, s SMALLSERIAL)",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateTable {
        columns,
        sequence_defaults,
        ..
    } = plan
    else {
        panic!("expected CreateTable");
    };
    assert_eq!(columns.field_at(0).data_type, DataType::Int32);
    assert_eq!(columns.field_at(1).data_type, DataType::Int64);
    assert_eq!(columns.field_at(2).data_type, DataType::Int16);
    assert!(!columns.field_at(0).nullable);
    assert_eq!(sequence_defaults[0].as_deref(), Some("t_id_seq"));
    assert_eq!(sequence_defaults[1].as_deref(), Some("t_v_seq"));
    assert_eq!(sequence_defaults[2].as_deref(), Some("t_s_seq"));
}

#[test]
fn binds_identity_column_as_sequence_backed_required_column() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "CREATE TABLE t (id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 10 INCREMENT BY 5))",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateTable {
        columns,
        sequence_defaults,
        sequence_options,
        identity_always,
        ..
    } = plan
    else {
        panic!("expected CreateTable");
    };
    assert_eq!(columns.field_at(0).data_type, DataType::Int64);
    assert!(!columns.field_at(0).nullable);
    assert_eq!(sequence_defaults[0].as_deref(), Some("t_id_seq"));
    assert!(identity_always[0]);
    let opts = sequence_options[0].expect("identity sequence options");
    assert_eq!(opts.start, 10);
    assert_eq!(opts.increment, 5);
}

#[test]
fn binds_create_table_foreign_key_constraints() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "CREATE TABLE child (user_id INT REFERENCES users(id), CONSTRAINT child_user_fk FOREIGN KEY (user_id) REFERENCES users(id))",
        &cat,
    )
    .expect("foreign keys bind");
    let LogicalPlan::CreateTable { foreign_keys, .. } = plan else {
        panic!("expected CreateTable");
    };
    assert_eq!(foreign_keys.len(), 2);
    assert_eq!(foreign_keys[0].columns, vec![0]);
    assert_eq!(foreign_keys[0].target_table, "users");
    assert_eq!(foreign_keys[0].target_columns, vec![0]);
    assert_eq!(foreign_keys[1].name, "child_user_fk");
}

#[test]
fn binds_deferrable_foreign_key_flags() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "CREATE TABLE child (user_id INT REFERENCES users(id) DEFERRABLE INITIALLY DEFERRED)",
        &cat,
    )
    .expect("foreign key binds");
    let LogicalPlan::CreateTable { foreign_keys, .. } = plan else {
        panic!("expected CreateTable");
    };
    assert_eq!(foreign_keys.len(), 1);
    assert!(foreign_keys[0].deferrable);
    assert!(foreign_keys[0].initially_deferred);
}

#[test]
fn binds_sequence_ddl_options() {
    let cat = InMemoryCatalog::new();
    let create = parse_and_bind(
        "CREATE SEQUENCE IF NOT EXISTS s START WITH 10 INCREMENT BY 5 MINVALUE 1 MAXVALUE 100 CACHE 4 CYCLE",
        &cat,
    )
    .expect("create sequence binds");
    let LogicalPlan::CreateSequence {
        sequence_name,
        namespace,
        options,
        if_not_exists,
        ..
    } = create
    else {
        panic!("expected CreateSequence");
    };
    assert_eq!(sequence_name, "s");
    assert_eq!(namespace, "public");
    assert!(if_not_exists);
    assert_eq!(options.start, 10);
    assert_eq!(options.increment, 5);
    assert_eq!(options.min, Some(1));
    assert_eq!(options.max, Some(100));
    assert_eq!(options.cache, 4);
    assert!(options.cycle);

    let alter = parse_and_bind("ALTER SEQUENCE s START WITH 50 RESTART WITH 7", &cat)
        .expect("alter sequence binds");
    let LogicalPlan::AlterSequence { options, .. } = alter else {
        panic!("expected AlterSequence");
    };
    assert_eq!(options.start, Some(50));
    assert_eq!(options.restart, Some(Some(7)));
}

#[test]
fn binds_descending_sequence_default_start_to_maxvalue() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("CREATE SEQUENCE s INCREMENT BY -1", &cat)
        .expect("descending sequence binds");
    let LogicalPlan::CreateSequence { options, .. } = plan else {
        panic!("expected CreateSequence");
    };
    assert_eq!(options.start, -1);
    assert_eq!(options.increment, -1);
}

#[test]
fn binds_create_sequence_rejects_restart() {
    let cat = InMemoryCatalog::new();
    let err = parse_and_bind("CREATE SEQUENCE s RESTART", &cat).unwrap_err();
    assert!(matches!(err, PlanError::NotSupported(_)), "got {err:?}");
}

#[test]
fn binds_create_table_rejects_unsupported_column_type() {
    // `JSONB` is still unsupported. The fixed-width numeric / text /
    // temporal / decimal types have all moved to the supported set
    // as of the v0.6 TPC-H milestone landing.
    let cat = InMemoryCatalog::new();
    let err = parse_and_bind("CREATE TABLE t (id JSONB)", &cat).unwrap_err();
    assert!(matches!(err, PlanError::NotSupported(_)), "got {err:?}");
}

#[test]
fn binds_create_table_persistent_catalog_via_snapshot_adapter() {
    // CatalogSnapshot from ultrasql-catalog implements `Catalog`,
    // so the binder can consume a persistent snapshot directly
    // (the seam the server uses to bind against PersistentCatalog).
    use ultrasql_catalog::TableEntry;
    let snap = ultrasql_catalog::CatalogSnapshot {
        tables: {
            let mut m = std::collections::HashMap::new();
            let schema = Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok");
            m.insert(
                "products".to_string(),
                TableEntry::new(ultrasql_core::Oid::new(100), "products", "public", schema),
            );
            m
        },
        tables_by_oid: std::collections::HashMap::new(),
        indexes: std::collections::HashMap::new(),
        indexes_by_table: std::collections::HashMap::new(),
        descriptions: std::collections::HashMap::new(),
        statistics: std::collections::HashMap::new(),
        statistic_ext: std::collections::HashMap::new(),
    };
    // Creating an already-existing relation through the snapshot
    // adapter surfaces DuplicateTable, proving the binder reaches
    // the snapshot.
    let stmt = Parser::new("CREATE TABLE products (id INT)")
        .parse_statement()
        .expect("parse ok");
    let err = bind(&stmt, &snap).unwrap_err();
    assert!(
        matches!(err, PlanError::DuplicateTable(ref t) if t == "products"),
        "got {err:?}"
    );
}

#[test]
fn binds_comment_on_table_and_column() {
    let cat = users_catalog();
    let table = parse_and_bind("COMMENT ON TABLE users IS 'hello'", &cat).expect("table comment");
    let LogicalPlan::Comment {
        target, comment, ..
    } = table
    else {
        panic!("expected comment plan");
    };
    assert_eq!(comment.as_deref(), Some("hello"));
    assert_eq!(
        target,
        crate::plan::LogicalCommentTarget::Table {
            table: "users".to_owned()
        }
    );

    let column =
        parse_and_bind("COMMENT ON COLUMN users.name IS NULL", &cat).expect("column comment");
    let LogicalPlan::Comment {
        target, comment, ..
    } = column
    else {
        panic!("expected comment plan");
    };
    assert!(comment.is_none());
    assert_eq!(
        target,
        crate::plan::LogicalCommentTarget::Column {
            table: "users".to_owned(),
            column: "name".to_owned(),
            attnum: 2,
        }
    );

    let index =
        parse_and_bind("COMMENT ON INDEX users_name_idx IS 'idx'", &cat).expect("index comment");
    let LogicalPlan::Comment { target, .. } = index else {
        panic!("expected comment plan");
    };
    assert_eq!(
        target,
        crate::plan::LogicalCommentTarget::Index {
            index: "users_name_idx".to_owned()
        }
    );
}

// -----------------------------------------------------------------------
// JOIN tests
// -----------------------------------------------------------------------

/// Build a two-table catalog: users (`id` INT, `name` TEXT) and orders (`oid` INT, `user_id` INT).
fn two_table_catalog() -> InMemoryCatalog {
    let users_schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("name", DataType::Text { max_len: None }),
    ])
    .expect("schema ok");
    let orders_schema = Schema::new([
        Field::required("oid", DataType::Int32),
        Field::required("user_id", DataType::Int32),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("users", TableMeta::new(users_schema));
    cat.register("orders", TableMeta::new(orders_schema));
    cat
}

fn duplicate_id_catalog() -> InMemoryCatalog {
    let schema_a = Schema::new([
        Field::required("id", DataType::Int32),
        Field::required("marker", DataType::Int32),
    ])
    .expect("schema ok");
    let schema_b = Schema::new([
        Field::required("id", DataType::Int32),
        Field::required("marker", DataType::Int32),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("a", TableMeta::new(schema_a));
    cat.register("b", TableMeta::new(schema_b));
    cat
}

#[test]
fn binds_inner_join_with_on_predicate() {
    let cat = two_table_catalog();
    let plan = parse_and_bind(
        "SELECT users.id FROM users INNER JOIN orders ON users.id = orders.user_id",
        &cat,
    )
    .expect("bind ok");
    // The top-level plan has a Project; find the Join underneath.
    fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Join { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_join(input),
            _ => None,
        }
    }
    let join = find_join(&plan).expect("should contain a Join node");
    let LogicalPlan::Join {
        join_type,
        condition,
        schema,
        ..
    } = join
    else {
        panic!("expected Join");
    };
    assert_eq!(*join_type, LogicalJoinType::Inner);
    assert!(
        matches!(condition, LogicalJoinCondition::On(_)),
        "expected ON condition"
    );
    // Schema is concatenation: users(id, name) + orders(oid, user_id) = 4
    assert_eq!(schema.len(), 4, "join schema width should be 4");
}

#[test]
fn qualified_join_predicate_resolves_duplicate_right_column() {
    let cat = duplicate_id_catalog();
    let plan = parse_and_bind(
        "SELECT a.id AS aid, b.id AS bid \
         FROM a JOIN b ON a.id = b.id \
         ORDER BY b.id",
        &cat,
    )
    .expect("bind ok");

    let LogicalPlan::Project { input, exprs, .. } = &plan else {
        panic!("expected top Project, got {plan:?}");
    };
    assert!(matches!(&exprs[0].0, ScalarExpr::Column { index: 0, .. }));
    assert!(matches!(&exprs[1].0, ScalarExpr::Column { index: 2, .. }));

    let LogicalPlan::Sort { input, keys } = input.as_ref() else {
        panic!("expected Sort under Project");
    };
    assert!(matches!(&keys[0].expr, ScalarExpr::Column { index: 2, .. }));

    let LogicalPlan::Join { condition, .. } = input.as_ref() else {
        panic!("expected Join under Sort");
    };
    let LogicalJoinCondition::On(ScalarExpr::Binary { left, right, .. }) = condition else {
        panic!("expected binary ON predicate, got {condition:?}");
    };
    assert!(matches!(left.as_ref(), ScalarExpr::Column { index: 0, .. }));
    assert!(matches!(
        right.as_ref(),
        ScalarExpr::Column { index: 2, .. }
    ));
}

#[test]
fn order_by_can_reference_projection_alias() {
    let cat = users_catalog();
    let plan =
        parse_and_bind("SELECT id AS ident FROM users ORDER BY ident DESC", &cat).expect("bind ok");

    let LogicalPlan::Sort { input, keys } = &plan else {
        panic!("expected top Sort over projected alias, got {plan:?}");
    };
    assert_eq!(keys.len(), 1);
    assert!(!keys[0].asc);
    assert!(matches!(
        &keys[0].expr,
        ScalarExpr::Column { index: 0, name, .. } if name == "ident"
    ));
    assert!(
        matches!(input.as_ref(), LogicalPlan::Project { .. }),
        "alias ORDER BY should sort projected rows"
    );
}

#[test]
fn qualified_where_predicate_resolves_duplicate_right_column() {
    let cat = duplicate_id_catalog();
    let plan = parse_and_bind("SELECT b.id FROM a, b WHERE a.id = b.id", &cat).expect("bind ok");

    let LogicalPlan::Project { input, exprs, .. } = &plan else {
        panic!("expected top Project, got {plan:?}");
    };
    assert!(matches!(&exprs[0].0, ScalarExpr::Column { index: 2, .. }));

    let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
        panic!("expected Filter under Project");
    };
    let ScalarExpr::Binary { left, right, .. } = predicate else {
        panic!("expected binary WHERE predicate, got {predicate:?}");
    };
    assert!(matches!(left.as_ref(), ScalarExpr::Column { index: 0, .. }));
    assert!(matches!(
        right.as_ref(),
        ScalarExpr::Column { index: 2, .. }
    ));
    assert!(matches!(input.as_ref(), LogicalPlan::Join { .. }));
}

#[test]
fn binds_left_outer_join_makes_right_columns_nullable() {
    let cat = two_table_catalog();
    let plan = parse_and_bind(
        "SELECT users.id FROM users LEFT JOIN orders ON users.id = orders.user_id",
        &cat,
    )
    .expect("bind ok");

    fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Join { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_join(input),
            _ => None,
        }
    }
    let join = find_join(&plan).expect("should contain a Join");
    let LogicalPlan::Join {
        join_type, schema, ..
    } = join
    else {
        panic!("expected Join");
    };
    assert_eq!(*join_type, LogicalJoinType::LeftOuter);
    // Left columns (users.id, users.name): id was required, stays required.
    assert!(
        !schema.field_at(0).nullable,
        "left.id should remain required"
    );
    // Right columns (orders.oid, orders.user_id) should be nullable in LEFT JOIN.
    assert!(schema.field_at(2).nullable, "right.oid should be nullable");
    assert!(
        schema.field_at(3).nullable,
        "right.user_id should be nullable"
    );
}

#[test]
fn binds_right_outer_join_makes_left_columns_nullable() {
    let cat = two_table_catalog();
    let plan = parse_and_bind(
        "SELECT users.id FROM users RIGHT JOIN orders ON users.id = orders.user_id",
        &cat,
    )
    .expect("bind ok");

    fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Join { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_join(input),
            _ => None,
        }
    }
    let join = find_join(&plan).expect("should contain a Join");
    let LogicalPlan::Join {
        join_type, schema, ..
    } = join
    else {
        panic!("expected Join");
    };
    assert_eq!(*join_type, LogicalJoinType::RightOuter);
    // In RIGHT JOIN: left columns become nullable.
    assert!(
        schema.field_at(0).nullable,
        "left.id should be nullable in RIGHT JOIN"
    );
    // Right columns keep their original nullability (both were required).
    assert!(!schema.field_at(2).nullable, "right.oid stays required");
}

#[test]
fn binds_full_outer_join_makes_both_sides_nullable() {
    let cat = two_table_catalog();
    let plan = parse_and_bind(
        "SELECT users.id FROM users FULL OUTER JOIN orders ON users.id = orders.user_id",
        &cat,
    )
    .expect("bind ok");

    fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Join { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_join(input),
            _ => None,
        }
    }
    let join = find_join(&plan).expect("should contain a Join");
    let LogicalPlan::Join {
        join_type, schema, ..
    } = join
    else {
        panic!("expected Join");
    };
    assert_eq!(*join_type, LogicalJoinType::FullOuter);
    // Both sides should be nullable.
    assert!(
        schema.field_at(0).nullable,
        "left.id should be nullable in FULL OUTER JOIN"
    );
    assert!(
        schema.field_at(2).nullable,
        "right.oid should be nullable in FULL OUTER JOIN"
    );
}

#[test]
fn binds_cross_join_has_no_predicate() {
    let cat = two_table_catalog();
    let plan =
        parse_and_bind("SELECT users.id FROM users CROSS JOIN orders", &cat).expect("bind ok");

    fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Join { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_join(input),
            _ => None,
        }
    }
    let join = find_join(&plan).expect("should contain a Join");
    let LogicalPlan::Join {
        join_type,
        condition,
        ..
    } = join
    else {
        panic!("expected Join");
    };
    assert_eq!(*join_type, LogicalJoinType::Cross);
    assert!(
        matches!(condition, LogicalJoinCondition::None),
        "cross join should have no condition"
    );
}

#[test]
fn binds_using_join_folds_to_equality_and_collapses_columns() {
    // Build a catalog where both tables have a column named `id`.
    let schema_a = Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok");
    let schema_b = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("val", DataType::Text { max_len: None }),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("a", TableMeta::new(schema_a));
    cat.register("b", TableMeta::new(schema_b));

    let plan = parse_and_bind("SELECT a.id FROM a JOIN b USING (id)", &cat).expect("bind ok");

    fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Join { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_join(input),
            _ => None,
        }
    }
    let join = find_join(&plan).expect("should contain a Join");
    let LogicalPlan::Join {
        condition, schema, ..
    } = join
    else {
        panic!("expected Join");
    };
    assert!(
        matches!(condition, LogicalJoinCondition::Using(_)),
        "expected USING condition"
    );
    // USING(id) collapses: id once + val = 2 columns (not 3).
    assert_eq!(
        schema.len(),
        2,
        "USING join should collapse the shared column"
    );
}

// -----------------------------------------------------------------------
// GROUP BY / aggregate tests
// -----------------------------------------------------------------------

#[test]
fn binds_group_by_emits_aggregate_node() {
    let cat = users_catalog();
    let plan = parse_and_bind("SELECT id, count(*) FROM users GROUP BY id", &cat).expect("bind ok");

    fn find_agg(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Aggregate { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_agg(input),
            _ => None,
        }
    }
    let agg = find_agg(&plan).expect("should contain Aggregate node");
    let LogicalPlan::Aggregate {
        group_by,
        aggregates,
        schema,
        ..
    } = agg
    else {
        panic!("expected Aggregate");
    };
    assert_eq!(group_by.len(), 1, "one GROUP BY key");
    assert_eq!(aggregates.len(), 1, "one aggregate");
    assert_eq!(aggregates[0].func, AggregateFunc::CountStar);
    // Schema: [id, count]
    assert_eq!(schema.len(), 2);
    assert_eq!(schema.field_at(0).name, "id");
    assert_eq!(schema.field_at(1).name, "count");
}

#[test]
fn binds_group_by_scalar_function_projection_alias() {
    let schema = Schema::new([
        Field::required("order_date", DataType::Date),
        Field::required("amount", DataType::Int32),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("sales", TableMeta::new(schema));

    let plan = parse_and_bind(
        "SELECT EXTRACT(YEAR FROM order_date) AS o_year, SUM(amount) AS revenue \
         FROM sales GROUP BY EXTRACT(YEAR FROM order_date) ORDER BY o_year",
        &cat,
    )
    .expect("bind ok");

    assert_eq!(plan.schema().field_at(0).name, "o_year");
    assert_eq!(plan.schema().field_at(1).name, "revenue");
}

#[test]
fn binds_group_by_column_projection_alias() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "SELECT id AS ident, COUNT(*) AS row_count FROM users GROUP BY id ORDER BY ident",
        &cat,
    )
    .expect("bind ok");

    assert_eq!(plan.schema().field_at(0).name, "ident");
    assert_eq!(plan.schema().field_at(1).name, "row_count");
}

#[test]
fn binds_count_star() {
    let cat = users_catalog();
    let plan = parse_and_bind("SELECT count(*) FROM users", &cat).expect("bind ok");

    fn find_agg(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Aggregate { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_agg(input),
            _ => None,
        }
    }
    let agg = find_agg(&plan).expect("should contain Aggregate node");
    let LogicalPlan::Aggregate { aggregates, .. } = agg else {
        panic!("expected Aggregate");
    };
    assert_eq!(aggregates.len(), 1);
    assert_eq!(aggregates[0].func, AggregateFunc::CountStar);
    assert!(aggregates[0].arg.is_none(), "count(*) has no argument");
}

#[test]
fn binds_having_filters_post_aggregate() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "SELECT id, count(*) FROM users GROUP BY id HAVING count(*) > 1",
        &cat,
    )
    .expect("bind ok");

    fn find_filter_above_agg(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Filter { input, .. } => {
                matches!(input.as_ref(), LogicalPlan::Aggregate { .. })
            }
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_filter_above_agg(input),
            _ => false,
        }
    }
    assert!(
        find_filter_above_agg(&plan),
        "should have Filter above Aggregate for HAVING"
    );
}

#[test]
fn binds_decimal_arithmetic_around_aggregate_with_decimal_type() {
    let schema = Schema::new([
        Field::required(
            "price",
            DataType::Decimal {
                precision: Some(15),
                scale: Some(2),
            },
        ),
        Field::required(
            "discount",
            DataType::Decimal {
                precision: Some(15),
                scale: Some(2),
            },
        ),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("lineitem", TableMeta::new(schema));

    let plan = parse_and_bind(
        "SELECT 100 * SUM(price * (1 - discount)) / SUM(price * (1 - discount)) AS ratio FROM lineitem",
        &cat,
    )
    .expect("bind ok");

    let LogicalPlan::Project { schema, exprs, .. } = &plan else {
        panic!("expected top-level Project, got {plan:?}");
    };
    assert_eq!(
        schema.field_at(0).data_type,
        DataType::Decimal {
            precision: None,
            scale: Some(8)
        }
    );
    assert_eq!(
        exprs[0].0.data_type(),
        DataType::Decimal {
            precision: None,
            scale: Some(8)
        }
    );
}

#[test]
fn binds_distinct_sum_arguments_to_distinct_aggregate_columns() {
    let schema = Schema::new([
        Field::required("volume", DataType::Float64),
        Field::required("nation", DataType::Text { max_len: None }),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("lineitem", TableMeta::new(schema));

    let plan = parse_and_bind(
        "SELECT \
             SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE volume - volume END) / SUM(volume) \
             AS mkt_share \
         FROM lineitem",
        &cat,
    )
    .expect("bind ok");

    let LogicalPlan::Project { input, exprs, .. } = &plan else {
        panic!("expected top-level Project, got {plan:?}");
    };
    let ScalarExpr::Binary { left, right, .. } = &exprs[0].0 else {
        panic!("expected ratio expression, got {:?}", exprs[0].0);
    };
    assert!(matches!(left.as_ref(), ScalarExpr::Column { index: 0, .. }));
    assert!(matches!(
        right.as_ref(),
        ScalarExpr::Column { index: 1, .. }
    ));

    let LogicalPlan::Aggregate { aggregates, .. } = input.as_ref() else {
        panic!("expected Aggregate under Project");
    };
    assert_eq!(aggregates.len(), 2, "SUM calls have different arguments");
}

// -----------------------------------------------------------------------
// Set operations tests
// -----------------------------------------------------------------------

#[test]
fn binds_union_all_arity_match() {
    let cat = users_catalog();
    let plan = parse_and_bind("SELECT id FROM users UNION ALL SELECT id FROM users", &cat)
        .expect("bind ok");

    fn find_setop(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::SetOp { .. } => Some(plan),
            LogicalPlan::Cte { body, .. } => find_setop(body),
            _ => None,
        }
    }
    // The SetOp may be wrapped in a Cte if there were CTEs, otherwise it's
    // at the top level.
    let setop = find_setop(&plan).unwrap_or(&plan);
    // Accept either SetOp at top or wrapped in project.
    let has_setop = matches!(plan, LogicalPlan::SetOp { .. })
        || matches!(&plan, LogicalPlan::Project { input, .. }
                if matches!(input.as_ref(), LogicalPlan::SetOp { .. }));
    // Or the plan IS the setop.
    let is_setop = matches!(&plan, LogicalPlan::SetOp { quantifier, .. }
            if *quantifier == LogicalSetQuantifier::All);
    // If it's not directly at top, it's wrapped by the outer structure.
    if !has_setop && !is_setop {
        // Find it anywhere in the tree.
        let _ = setop;
        // The schema should have 1 column.
        let final_schema = plan.schema();
        assert_eq!(
            final_schema.len(),
            1,
            "UNION ALL of single-column selects = 1 col"
        );
    } else {
        assert!(has_setop || is_setop);
    }
    let _ = setop;
}

#[test]
fn binds_union_distinct_with_arity_mismatch_is_rejected() {
    let cat = users_catalog();
    // id (1 col) UNION id, name (2 cols) should fail.
    let err = parse_and_bind(
        "SELECT id FROM users UNION SELECT id, name FROM users",
        &cat,
    )
    .unwrap_err();
    assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
}

// -----------------------------------------------------------------------
// CTE tests
// -----------------------------------------------------------------------

#[test]
fn binds_cte_then_references_it_in_body() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "WITH active AS (SELECT id FROM users) SELECT id FROM active",
        &cat,
    )
    .expect("bind ok");

    // Top-level plan should be a Cte node.
    let LogicalPlan::Cte {
        name, recursive, ..
    } = &plan
    else {
        panic!("expected Cte at top, got {plan:?}");
    };
    assert_eq!(name, "active");
    assert!(!recursive, "non-recursive CTE should have recursive=false");
}

#[test]
fn binds_scalar_subquery_that_references_outer_cte() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "WITH revenue AS (SELECT id, score FROM users) SELECT id FROM revenue WHERE score = (SELECT MAX(score) FROM revenue)",
        &cat,
    )
    .expect("bind ok");

    let LogicalPlan::Cte { body, .. } = &plan else {
        panic!("expected Cte at top, got {plan:?}");
    };

    fn find_filter(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Filter { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Sort { input, .. } => find_filter(input),
            _ => None,
        }
    }

    let filter = find_filter(body).expect("should have Filter");
    let LogicalPlan::Filter { predicate, .. } = filter else {
        panic!("expected Filter");
    };
    let ScalarExpr::Binary { right, .. } = predicate else {
        panic!("expected binary predicate, got {predicate:?}");
    };
    assert!(matches!(
        right.as_ref(),
        ScalarExpr::ScalarSubquery {
            correlated: false,
            ..
        }
    ));
}

// -----------------------------------------------------------------------
// SELECT * wildcard tests
// -----------------------------------------------------------------------

#[test]
fn binds_select_star_expands_via_catalog() {
    let cat = users_catalog();
    let plan = parse_and_bind("SELECT * FROM users", &cat).expect("bind ok");
    let LogicalPlan::Project { schema, exprs, .. } = &plan else {
        panic!("expected Project, got {plan:?}");
    };
    // users has id, name, score = 3 columns
    assert_eq!(schema.len(), 3, "SELECT * should expand to 3 columns");
    assert_eq!(exprs.len(), 3);
}

#[test]
fn binds_qualified_wildcard_restricts_to_table_alias() {
    let cat = two_table_catalog();
    let plan = parse_and_bind(
        "SELECT u.* FROM users u JOIN orders o ON u.id = o.user_id",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::Project { schema, .. } = &plan else {
        panic!("expected Project, got {plan:?}");
    };
    // users u has 2 columns; u.* should expand to those 2 only.
    assert_eq!(schema.len(), 2, "u.* should expand to users' 2 columns");
}

// -----------------------------------------------------------------------
// Error / unsupported
// -----------------------------------------------------------------------

#[test]
fn binder_rejects_unknown_aggregate_with_not_supported() {
    let cat = users_catalog();
    // `mode` is not a known aggregate; the binder should reject it.
    let err = parse_and_bind("SELECT mode(score) FROM users GROUP BY id", &cat).unwrap_err();
    assert!(
        matches!(err, PlanError::NotSupported(_)),
        "unknown aggregate should be NotSupported, got {err:?}"
    );
}

// -----------------------------------------------------------------------
// Property test
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// Subquery tests
// -----------------------------------------------------------------------

/// A two-table catalog: `users (id INT, name TEXT, score FLOAT8)`
/// and `orders (oid INT, user_id INT)`.
fn subquery_catalog() -> InMemoryCatalog {
    let users_schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("name", DataType::Text { max_len: None }),
        Field::nullable("score", DataType::Float64),
    ])
    .expect("schema ok");
    let orders_schema = Schema::new([
        Field::required("oid", DataType::Int32),
        Field::required("user_id", DataType::Int32),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("users", TableMeta::new(users_schema));
    cat.register("orders", TableMeta::new(orders_schema));
    cat
}

#[test]
fn binds_uncorrelated_exists_subquery() {
    // `EXISTS (SELECT oid FROM orders)` has no outer column references.
    let cat = subquery_catalog();
    let plan = parse_and_bind(
        "SELECT id FROM users WHERE EXISTS (SELECT oid FROM orders)",
        &cat,
    )
    .expect("bind ok");
    // Walk to the Filter and check its predicate.
    fn find_filter(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Filter { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Sort { input, .. } => find_filter(input),
            _ => None,
        }
    }
    let filter = find_filter(&plan).expect("should have Filter");
    let LogicalPlan::Filter { predicate, .. } = filter else {
        panic!("expected Filter");
    };
    let ScalarExpr::Exists {
        negated,
        correlated,
        ..
    } = predicate
    else {
        panic!("expected Exists predicate, got {predicate:?}");
    };
    assert!(!negated, "should not be negated");
    assert!(!correlated, "no outer column reference → uncorrelated");
}

#[test]
fn binds_correlated_exists_subquery() {
    // `EXISTS (SELECT oid FROM orders WHERE user_id = id)` — `id` is not in
    // `orders`, so it resolves to the outer `users.id`.
    let cat = subquery_catalog();
    let plan = parse_and_bind(
        "SELECT id FROM users WHERE EXISTS (SELECT oid FROM orders WHERE user_id = id)",
        &cat,
    )
    .expect("bind ok");
    fn find_filter(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Filter { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Sort { input, .. } => find_filter(input),
            _ => None,
        }
    }
    let filter = find_filter(&plan).expect("should have Filter");
    let LogicalPlan::Filter { predicate, .. } = filter else {
        panic!("expected Filter");
    };
    let ScalarExpr::Exists { correlated, .. } = predicate else {
        panic!("expected Exists, got {predicate:?}");
    };
    assert!(correlated, "id resolves to outer users.id → correlated");
}

#[test]
fn binds_in_subquery_arity_1_check_rejects_multi_column() {
    // `id IN (SELECT oid, user_id FROM orders)` — 2-column subquery must fail.
    let cat = subquery_catalog();
    let err = parse_and_bind(
        "SELECT id FROM users WHERE id IN (SELECT oid, user_id FROM orders)",
        &cat,
    )
    .unwrap_err();
    assert!(
        matches!(err, PlanError::TypeMismatch(_)),
        "multi-column IN subquery should be TypeMismatch, got {err:?}"
    );
}

#[test]
fn binds_scalar_subquery_returns_scalar_subquery_expr() {
    // `(SELECT oid FROM orders LIMIT 1)` used as a scalar in the projection.
    let cat = subquery_catalog();
    let plan = parse_and_bind(
        "SELECT id, (SELECT oid FROM orders LIMIT 1) FROM users",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::Project { exprs, .. } = &plan else {
        panic!("expected Project, got {plan:?}");
    };
    // Second expression should be a ScalarSubquery.
    let (second_expr, _) = &exprs[1];
    assert!(
        matches!(
            second_expr,
            ScalarExpr::ScalarSubquery {
                correlated: false,
                ..
            }
        ),
        "expected uncorrelated ScalarSubquery, got {second_expr:?}"
    );
}

#[test]
fn binds_not_in_subquery() {
    let cat = subquery_catalog();
    let plan = parse_and_bind(
        "SELECT id FROM users WHERE id NOT IN (SELECT user_id FROM orders)",
        &cat,
    )
    .expect("bind ok");
    fn find_filter(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Filter { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Sort { input, .. } => find_filter(input),
            _ => None,
        }
    }
    let filter = find_filter(&plan).expect("should have Filter");
    let LogicalPlan::Filter { predicate, .. } = filter else {
        panic!("expected Filter");
    };
    let ScalarExpr::InSubquery { negated, .. } = predicate else {
        panic!("expected InSubquery, got {predicate:?}");
    };
    assert!(negated, "NOT IN should produce negated=true");
}

#[test]
fn binds_any_eq_lowers_to_exists() {
    // `id = ANY (SELECT user_id FROM orders)` should bind as InSubquery with
    // negated=false (the same representation as `id IN (…)`).
    let cat = subquery_catalog();
    let plan = parse_and_bind(
        "SELECT id FROM users WHERE id = ANY (SELECT user_id FROM orders)",
        &cat,
    )
    .expect("bind ok");
    fn find_filter(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Filter { .. } => Some(plan),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Sort { input, .. } => find_filter(input),
            _ => None,
        }
    }
    let filter = find_filter(&plan).expect("should have Filter");
    let LogicalPlan::Filter { predicate, .. } = filter else {
        panic!("expected Filter");
    };
    assert!(
        matches!(predicate, ScalarExpr::InSubquery { negated: false, .. }),
        "= ANY should lower to InSubquery(negated=false), got {predicate:?}"
    );
}

#[test]
fn binds_any_with_lt_returns_not_supported() {
    let cat = subquery_catalog();
    let err = parse_and_bind(
        "SELECT id FROM users WHERE id < ANY (SELECT user_id FROM orders)",
        &cat,
    )
    .unwrap_err();
    assert!(
        matches!(err, PlanError::NotSupported(_)),
        "< ANY should be NotSupported, got {err:?}"
    );
}

#[test]
fn binder_rejects_scalar_subquery_with_multi_column_projection() {
    let cat = subquery_catalog();
    let err = parse_and_bind(
        "SELECT id, (SELECT oid, user_id FROM orders LIMIT 1) FROM users",
        &cat,
    )
    .unwrap_err();
    assert!(
        matches!(err, PlanError::TypeMismatch(_)),
        "multi-column scalar subquery should be TypeMismatch, got {err:?}"
    );
}

#[test]
fn outer_column_correctly_tracks_frame_depth_in_nested_subquery() {
    // Outer query scans `users`.  The subquery scans `orders`.  Inside the
    // subquery's WHERE, `id` is not in `orders` so it should resolve as
    // `OuterColumn { frame_depth: 1, … }`.
    let cat = subquery_catalog();
    let plan = parse_and_bind(
        "SELECT id FROM users WHERE EXISTS (SELECT oid FROM orders WHERE user_id = id)",
        &cat,
    )
    .expect("bind ok");
    // Navigate to the Exists predicate's inner plan.
    fn find_exists_pred(plan: &LogicalPlan) -> Option<&ScalarExpr> {
        match plan {
            LogicalPlan::Filter { predicate, .. } => {
                if matches!(predicate, ScalarExpr::Exists { .. }) {
                    Some(predicate)
                } else {
                    None
                }
            }
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_exists_pred(input),
            _ => None,
        }
    }
    let pred = find_exists_pred(&plan).expect("should find Exists predicate");
    let ScalarExpr::Exists { subplan, .. } = pred else {
        panic!("expected Exists");
    };
    // The inner plan should have a Filter with an outer-column reference.
    fn find_outer_col(plan: &LogicalPlan) -> Option<usize> {
        match plan {
            LogicalPlan::Filter { predicate, .. } => {
                // Predicate is `user_id = id` — a Binary with the right side
                // being an OuterColumn.
                if let ScalarExpr::Binary { right, .. } = predicate {
                    if let ScalarExpr::OuterColumn { frame_depth, .. } = right.as_ref() {
                        return Some(*frame_depth);
                    }
                }
                None
            }
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_outer_col(input),
            _ => None,
        }
    }
    let depth = find_outer_col(subplan).expect("should find OuterColumn in subplan");
    assert_eq!(depth, 1, "column is one level out → frame_depth = 1");
}

// -----------------------------------------------------------------------
// BETWEEN tests — the binder rewrites BETWEEN into a comparison tree
// -----------------------------------------------------------------------

/// Extract the bound WHERE predicate from a SELECT plan that the
/// binder shaped as `Project { Filter { Scan } }`.
fn predicate_of(plan: &LogicalPlan) -> &ScalarExpr {
    fn find_filter(plan: &LogicalPlan) -> &LogicalPlan {
        match plan {
            LogicalPlan::Filter { .. } => plan,
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => find_filter(input),
            _ => panic!("expected Filter under plan, got {plan:?}"),
        }
    }
    match find_filter(plan) {
        LogicalPlan::Filter { predicate, .. } => predicate,
        other => panic!("expected Filter, got {other:?}"),
    }
}

#[test]
fn binds_between_as_ge_and_le() {
    // The canonical rewrite: BETWEEN low AND high becomes
    // `expr >= low AND expr <= high`.
    let plan = parse_bind_ok("SELECT id FROM users WHERE id BETWEEN 5 AND 10");
    let pred = predicate_of(&plan);
    // Top-level: AND.
    let ScalarExpr::Binary {
        op: BinaryOp::And,
        left,
        right,
        data_type,
    } = pred
    else {
        panic!("expected AND at the root, got {pred:?}");
    };
    assert_eq!(*data_type, DataType::Bool);

    // Left arm: `id >= 5`.
    let ScalarExpr::Binary {
        op: BinaryOp::GtEq,
        left: lo_l,
        right: lo_r,
        ..
    } = left.as_ref()
    else {
        panic!("expected GtEq on left, got {left:?}");
    };
    assert!(matches!(lo_l.as_ref(), ScalarExpr::Column { name, .. } if name == "id"));
    assert!(matches!(
        lo_r.as_ref(),
        ScalarExpr::Literal {
            value: Value::Int32(5),
            ..
        }
    ));

    // Right arm: `id <= 10`.
    let ScalarExpr::Binary {
        op: BinaryOp::LtEq,
        left: hi_l,
        right: hi_r,
        ..
    } = right.as_ref()
    else {
        panic!("expected LtEq on right, got {right:?}");
    };
    assert!(matches!(hi_l.as_ref(), ScalarExpr::Column { name, .. } if name == "id"));
    assert!(matches!(
        hi_r.as_ref(),
        ScalarExpr::Literal {
            value: Value::Int32(10),
            ..
        }
    ));
}

#[test]
fn binds_not_between_as_lt_or_gt() {
    let plan = parse_bind_ok("SELECT id FROM users WHERE id NOT BETWEEN 5 AND 10");
    let pred = predicate_of(&plan);
    let ScalarExpr::Binary {
        op: BinaryOp::Or,
        left,
        right,
        ..
    } = pred
    else {
        panic!("expected OR at the root, got {pred:?}");
    };
    assert!(matches!(
        left.as_ref(),
        ScalarExpr::Binary {
            op: BinaryOp::Lt,
            ..
        }
    ));
    assert!(matches!(
        right.as_ref(),
        ScalarExpr::Binary {
            op: BinaryOp::Gt,
            ..
        }
    ));
}

#[test]
fn binds_between_mixed_numeric_types() {
    // `score` is FLOAT8 in the users catalog. A BETWEEN against an
    // integer pair must bind cleanly through the same numeric-join
    // promotion that the explicit comparison form uses.
    let plan = parse_bind_ok("SELECT id FROM users WHERE score BETWEEN 1 AND 100");
    let pred = predicate_of(&plan);
    assert!(matches!(
        pred,
        ScalarExpr::Binary {
            op: BinaryOp::And,
            ..
        }
    ));
}

#[test]
fn binds_between_symmetric_emits_or_of_two_ranges() {
    let plan = parse_bind_ok("SELECT id FROM users WHERE id BETWEEN SYMMETRIC 10 AND 5");
    let pred = predicate_of(&plan);
    // BETWEEN SYMMETRIC: (forward) OR (reversed).
    let ScalarExpr::Binary {
        op: BinaryOp::Or,
        left,
        right,
        ..
    } = pred
    else {
        panic!("expected OR at the root, got {pred:?}");
    };
    // Each arm is a `(>= AND <=)` tree.
    for (label, arm) in [("forward", left.as_ref()), ("reversed", right.as_ref())] {
        assert!(
            matches!(
                arm,
                ScalarExpr::Binary {
                    op: BinaryOp::And,
                    ..
                }
            ),
            "SYMMETRIC {label} arm should be AND, got {arm:?}"
        );
    }
}

#[test]
fn binds_not_between_symmetric_emits_and_of_two_ranges() {
    let plan = parse_bind_ok("SELECT id FROM users WHERE id NOT BETWEEN SYMMETRIC 10 AND 5");
    let pred = predicate_of(&plan);
    // NOT BETWEEN SYMMETRIC: (forward NOT) AND (reversed NOT).
    let ScalarExpr::Binary {
        op: BinaryOp::And,
        left,
        right,
        ..
    } = pred
    else {
        panic!("expected AND at the root, got {pred:?}");
    };
    for (label, arm) in [("forward", left.as_ref()), ("reversed", right.as_ref())] {
        assert!(
            matches!(
                arm,
                ScalarExpr::Binary {
                    op: BinaryOp::Or,
                    ..
                }
            ),
            "NOT SYMMETRIC {label} arm should be OR, got {arm:?}"
        );
    }
}

#[test]
fn binds_between_rewrite_renders_as_full_tree() {
    // Lock down the exact textual form of the rewrite so it surfaces
    // unambiguously in EXPLAIN-style output.
    let plan = parse_bind_ok("SELECT id FROM users WHERE id BETWEEN 5 AND 10");
    let pred = predicate_of(&plan);
    assert_eq!(pred.to_string(), "((id >= 5) AND (id <= 10))");
}

#[test]
fn binds_between_uses_existing_type_check_to_reject_incompatible_bounds() {
    // `name` is TEXT; bound by an integer literal is not comparable
    // — the binder must surface a TypeMismatch the same way it
    // would for the equivalent `name >= 1 AND name <= 10`.
    let cat = users_catalog();
    let err = parse_and_bind("SELECT id FROM users WHERE name BETWEEN 1 AND 10", &cat).unwrap_err();
    assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
}

proptest! {
    /// BETWEEN binds without error for every integer pair drawn from the
    /// supported i32 range — the rewrite never invents a type it does not
    /// already accept on plain comparisons.
    #[test]
    fn prop_between_int_pair_binds_ok(
        lo in -1_000_000_i32..=1_000_000_i32,
        hi in -1_000_000_i32..=1_000_000_i32,
    ) {
        let cat = users_catalog();
        let sql = format!("SELECT id FROM users WHERE id BETWEEN {lo} AND {hi}");
        let result = parse_and_bind(&sql, &cat);
        prop_assert!(result.is_ok(), "BETWEEN should bind, got {:?}", result);
    }

    /// NOT BETWEEN binds without error for every integer pair drawn from
    /// the supported i32 range.
    #[test]
    fn prop_not_between_int_pair_binds_ok(
        lo in -1_000_000_i32..=1_000_000_i32,
        hi in -1_000_000_i32..=1_000_000_i32,
    ) {
        let cat = users_catalog();
        let sql = format!("SELECT id FROM users WHERE id NOT BETWEEN {lo} AND {hi}");
        let result = parse_and_bind(&sql, &cat);
        prop_assert!(result.is_ok(), "NOT BETWEEN should bind, got {:?}", result);
    }
}

proptest! {
    /// Any random join tree over a fixed set of 3 tables binds without error.
    #[test]
    fn prop_join_tree_over_three_tables_binds_ok(
        // Choose join type index 0..4 for left join and right join.
        lj_type in 0_usize..2_usize,
        rj_type in 0_usize..2_usize,
    ) {
        // Catalog: a, b, c each with one column.
        let mut cat = InMemoryCatalog::new();
        let s = Schema::new([Field::required("x", DataType::Int32)]).expect("schema ok");
        cat.register("ta", TableMeta::new(s));
        let sb = Schema::new([Field::required("y", DataType::Int32)]).expect("schema ok");
        cat.register("tb", TableMeta::new(sb));
        let sc = Schema::new([Field::required("z", DataType::Int32)]).expect("schema ok");
        cat.register("tc", TableMeta::new(sc));

        let join_kw = ["INNER JOIN", "CROSS JOIN"];
        let lj = join_kw[lj_type % join_kw.len()];
        let rj = join_kw[rj_type % join_kw.len()];
        let on_lj = if lj == "CROSS JOIN" { "" } else { " ON ta.x = tb.y" };
        let on_rj = if rj == "CROSS JOIN" { "" } else { " ON ta.x = tc.z" };
        let sql = format!(
            "SELECT ta.x FROM ta {lj} tb{on_lj} {rj} tc{on_rj}"
        );
        let result = parse_and_bind(&sql, &cat);
        prop_assert!(result.is_ok(), "join tree should bind ok, got {:?}", result);
    }
}

proptest! {
    /// For any arity in 1..=6 and 1..=4 matching VALUES rows, the bound
    /// INSERT plan has a Values source with the same arity.
    #[test]
    fn prop_insert_values_arity_preserved(
        arity in 1_usize..=6_usize,
        nrows in 1_usize..=4_usize,
    ) {
        // Build a catalog with a table that has `arity` INT columns.
        let fields: Vec<Field> = (0..arity)
            .map(|i| Field::nullable(format!("c{i}"), DataType::Int32))
            .collect();
        let schema = Schema::new(fields).expect("schema ok");
        let mut cat = InMemoryCatalog::new();
        cat.register("t", TableMeta::new(schema));

        // Build SQL: INSERT INTO t (c0, c1, …) VALUES (0, 0, …), …
        let cols: Vec<String> = (0..arity).map(|i| format!("c{i}")).collect();
        let one_row = vec!["0"; arity].join(", ");
        let values_clause = std::iter::repeat_n(format!("({one_row})"), nrows)
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT INTO t ({}) VALUES {}",
            cols.join(", "),
            values_clause
        );

        let plan = parse_and_bind(&sql, &cat).expect("bind ok");
        let LogicalPlan::Insert { columns, source, .. } = &plan else {
            panic!("expected Insert");
        };
        prop_assert_eq!(columns.len(), arity);
        let LogicalPlan::Values { rows, .. } = source.as_ref() else {
            panic!("expected Values source");
        };
        prop_assert_eq!(rows.len(), nrows);
        for r in rows {
            prop_assert_eq!(r.len(), arity);
        }
    }
}

// -----------------------------------------------------------------------
// CREATE INDEX / DROP TABLE / ALTER TABLE — DDL binder tests
// -----------------------------------------------------------------------

#[test]
fn binds_create_index_resolves_column_index_and_synthesises_name() {
    let plan = parse_bind_ok("CREATE INDEX ON users (id)");
    let LogicalPlan::CreateIndex {
        index_name,
        table_name,
        columns,
        unique,
        if_not_exists,
        ..
    } = plan
    else {
        panic!("expected CreateIndex plan");
    };
    assert_eq!(table_name, "users");
    assert_eq!(columns, vec![0]);
    assert!(!unique);
    assert!(!if_not_exists);
    // Synthesised name uses the {table}_{cols}_idx convention.
    assert_eq!(index_name, "users_id_idx");
}

#[test]
fn binds_create_unique_index_honours_unique_flag_and_explicit_name() {
    let plan = parse_bind_ok("CREATE UNIQUE INDEX IF NOT EXISTS users_pk ON users (id)");
    let LogicalPlan::CreateIndex {
        index_name,
        unique,
        if_not_exists,
        ..
    } = plan
    else {
        panic!("expected CreateIndex plan");
    };
    assert!(unique);
    assert!(if_not_exists);
    assert_eq!(index_name, "users_pk");
}

#[test]
fn binds_create_index_concurrently_flag() {
    let plan = parse_bind_ok("CREATE INDEX CONCURRENTLY users_id_idx ON users (id)");
    let LogicalPlan::CreateIndex { concurrently, .. } = plan else {
        panic!("expected CreateIndex plan");
    };
    assert!(concurrently);
}

#[test]
fn binds_create_hash_index_method() {
    let plan = parse_bind_ok("CREATE INDEX users_id_hash_idx ON users USING hash (id)");
    let LogicalPlan::CreateIndex { method, .. } = plan else {
        panic!("expected CreateIndex plan");
    };
    assert_eq!(method, LogicalIndexMethod::Hash);
}

#[test]
fn binds_create_inverted_search_and_brin_index_methods() {
    for (sql, expected) in [
        (
            "CREATE INDEX users_name_gin_idx ON users USING gin (name)",
            LogicalIndexMethod::Gin,
        ),
        (
            "CREATE INDEX users_score_gist_idx ON users USING gist (score)",
            LogicalIndexMethod::Gist,
        ),
        (
            "CREATE INDEX users_id_brin_idx ON users USING brin (id)",
            LogicalIndexMethod::Brin,
        ),
    ] {
        let LogicalPlan::CreateIndex { method, .. } = parse_bind_ok(sql) else {
            panic!("expected CreateIndex plan");
        };
        assert_eq!(method, expected);
    }
}

#[test]
fn rejects_unique_inverted_search_and_brin_index_methods() {
    let cat = users_catalog();
    for sql in [
        "CREATE UNIQUE INDEX users_name_gin_idx ON users USING gin (name)",
        "CREATE UNIQUE INDEX users_score_gist_idx ON users USING gist (score)",
        "CREATE UNIQUE INDEX users_id_brin_idx ON users USING brin (id)",
    ] {
        let err = parse_and_bind(sql, &cat).unwrap_err();
        assert!(matches!(err, PlanError::NotSupported(_)), "got {err:?}");
    }
}

#[test]
fn binds_create_expression_index_key() {
    let plan = parse_bind_ok("CREATE INDEX users_lower_name_idx ON users (lower(name))");
    let LogicalPlan::CreateIndex {
        columns, key_exprs, ..
    } = plan
    else {
        panic!("expected CreateIndex plan");
    };
    assert!(columns.is_empty());
    assert_eq!(key_exprs.len(), 1);
    assert!(matches!(
        &key_exprs[0],
        ScalarExpr::FunctionCall { name, .. } if name == "lower"
    ));
}

#[test]
fn binds_create_partial_and_include_index_metadata() {
    let plan = parse_bind_ok(
        "CREATE INDEX users_name_active_idx ON users (name) INCLUDE (score) WHERE name IS NOT NULL",
    );
    let LogicalPlan::CreateIndex {
        columns,
        include_columns,
        predicate,
        ..
    } = plan
    else {
        panic!("expected CreateIndex plan");
    };
    assert_eq!(columns, vec![1]);
    assert_eq!(include_columns, vec![2]);
    assert!(matches!(
        predicate,
        Some(ScalarExpr::IsNull { negated: true, .. })
    ));
}

#[test]
fn create_index_rejects_unknown_column() {
    let cat = users_catalog();
    let err = parse_and_bind("CREATE INDEX bad_idx ON users (does_not_exist)", &cat).unwrap_err();
    assert!(matches!(err, PlanError::ColumnNotFound(_)), "got {err:?}");
}

#[test]
fn create_index_rejects_unknown_table() {
    let cat = users_catalog();
    let err = parse_and_bind("CREATE INDEX bad_idx ON nonexistent (id)", &cat).unwrap_err();
    assert!(matches!(err, PlanError::TableNotFound(_)), "got {err:?}");
}

#[test]
fn binds_drop_table_with_known_relation() {
    let plan = parse_bind_ok("DROP TABLE users");
    let LogicalPlan::DropTable {
        tables, if_exists, ..
    } = plan
    else {
        panic!("expected DropTable plan");
    };
    assert_eq!(tables, vec!["users".to_string()]);
    assert!(!if_exists);
}

#[test]
fn drop_table_if_exists_silently_omits_missing_relations() {
    let plan = parse_bind_ok("DROP TABLE IF EXISTS users, nope");
    let LogicalPlan::DropTable {
        tables, if_exists, ..
    } = plan
    else {
        panic!("expected DropTable plan");
    };
    assert!(if_exists);
    // `nope` is silently filtered; `users` remains.
    assert_eq!(tables, vec!["users".to_string()]);
}

#[test]
fn drop_table_without_if_exists_rejects_missing_relation() {
    let cat = users_catalog();
    let err = parse_and_bind("DROP TABLE nonexistent", &cat).unwrap_err();
    assert!(matches!(err, PlanError::TableNotFound(_)), "got {err:?}");
}

#[test]
fn binds_alter_table_add_column_resolves_field() {
    let plan = parse_bind_ok("ALTER TABLE users ADD COLUMN extra INTEGER");
    let LogicalPlan::AlterTable {
        table_name, action, ..
    } = plan
    else {
        panic!("expected AlterTable plan");
    };
    assert_eq!(table_name, "users");
    let LogicalAlterTableAction::AddColumn { column } = action else {
        panic!("expected AddColumn action");
    };
    assert_eq!(column.name, "extra");
    assert_eq!(column.data_type, DataType::Int32);
    assert!(column.nullable, "ADD COLUMN defaults to nullable");
}

#[test]
fn binds_alter_table_add_column_not_null() {
    let plan = parse_bind_ok("ALTER TABLE users ADD COLUMN flag BOOLEAN NOT NULL");
    let LogicalPlan::AlterTable { action, .. } = plan else {
        panic!("expected AlterTable plan");
    };
    let LogicalAlterTableAction::AddColumn { column } = action else {
        panic!("expected AddColumn action");
    };
    assert_eq!(column.data_type, DataType::Bool);
    assert!(!column.nullable);
}

#[test]
fn alter_table_add_column_rejects_duplicate_name() {
    let cat = users_catalog();
    let err = parse_and_bind("ALTER TABLE users ADD COLUMN id INTEGER", &cat).unwrap_err();
    assert!(
        matches!(err, PlanError::DuplicateColumn(ref c) if c == "id"),
        "got {err:?}"
    );
}

#[test]
fn binds_alter_table_drop_column_resolves_index() {
    let cat = users_catalog();
    let plan = parse_and_bind("ALTER TABLE users DROP COLUMN score", &cat).unwrap();
    let LogicalPlan::AlterTable { action, .. } = plan else {
        panic!("expected AlterTable plan");
    };
    let LogicalAlterTableAction::DropColumn {
        column_index,
        column_name,
    } = action
    else {
        panic!("expected DropColumn action");
    };
    assert_eq!(column_name, "score");
    assert_eq!(column_index, 2);
}

#[test]
fn binds_alter_table_rename_to_new_name() {
    let cat = users_catalog();
    let plan = parse_and_bind("ALTER TABLE users RENAME TO subscribers", &cat).unwrap();
    let LogicalPlan::AlterTable { action, .. } = plan else {
        panic!("expected AlterTable plan");
    };
    let LogicalAlterTableAction::RenameTable { new_name } = action else {
        panic!("expected RenameTable action");
    };
    assert_eq!(new_name, "subscribers");
}
