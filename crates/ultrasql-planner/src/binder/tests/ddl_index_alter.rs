//! Binder tests for CREATE/DROP INDEX, DROP TABLE and ALTER TABLE/VIEW.

use ultrasql_core::{DataType, Field, Schema, Value};

use super::*;
use crate::LogicalIndexMethod;
use crate::catalog::TableMeta;

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
fn binds_create_index_namespace_from_target_table() {
    let cat = app_users_catalog();
    let plan = parse_and_bind("CREATE INDEX same_idx ON app.users (id)", &cat)
        .expect("qualified app table binds");
    let LogicalPlan::CreateIndex {
        index_name,
        index_namespace,
        table_name,
        ..
    } = plan
    else {
        panic!("expected CreateIndex plan");
    };
    assert_eq!(index_name, "same_idx");
    assert_eq!(index_namespace, "app");
    assert_eq!(table_name, "app.users");
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
fn binds_create_aggregating_index_metadata() {
    let cat = fact_events_catalog();
    let LogicalPlan::CreateIndex {
        method,
        columns,
        aggregating,
        ..
    } = parse_and_bind(
        "CREATE AGGREGATING INDEX fact_rollup ON fact_events \
         (tenant_id, bucket, sum(amount), count(*))",
        &cat,
    )
    .expect("bind aggregating index")
    else {
        panic!("expected CreateIndex plan");
    };
    assert_eq!(method, LogicalIndexMethod::Aggregating);
    assert_eq!(columns, vec![0, 1]);
    let aggregating = aggregating.expect("aggregating metadata");
    assert_eq!(aggregating.group_columns, vec![0, 1]);
    assert_eq!(aggregating.aggregates.len(), 2);
    assert_eq!(aggregating.aggregates[0].func, AggregateFunc::Sum);
    assert_eq!(aggregating.aggregates[0].arg_column, Some(2));
    assert_eq!(aggregating.aggregates[1].func, AggregateFunc::CountStar);
    assert_eq!(aggregating.aggregates[1].arg_column, None);
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
fn binds_create_hnsw_index_method_for_vector_column() {
    let cat = embeddings_catalog();
    let LogicalPlan::CreateIndex {
        method, columns, ..
    } = parse_and_bind(
        "CREATE INDEX embeddings_hnsw_idx ON embeddings USING hnsw (embedding)",
        &cat,
    )
    .expect("bind hnsw")
    else {
        panic!("expected CreateIndex plan");
    };
    assert_eq!(method, LogicalIndexMethod::Hnsw);
    assert_eq!(columns, vec![1]);
}

#[test]
fn binds_create_hnsw_vector_opclass() {
    let cat = embeddings_catalog();
    let LogicalPlan::CreateIndex {
        method,
        columns,
        opclasses,
        ..
    } = parse_and_bind(
        "CREATE INDEX embeddings_hnsw_idx ON embeddings USING hnsw (embedding vector_l2_ops)",
        &cat,
    )
    .expect("bind hnsw opclass")
    else {
        panic!("expected CreateIndex plan");
    };
    assert_eq!(method, LogicalIndexMethod::Hnsw);
    assert_eq!(columns, vec![1]);
    assert_eq!(opclasses, vec![Some("vector_l2_ops".to_owned())]);
}

#[test]
fn binds_create_ivfflat_with_lists_and_probes() {
    let cat = embeddings_catalog();
    let LogicalPlan::CreateIndex {
        method,
        columns,
        opclasses,
        index_options,
        ..
    } = parse_and_bind(
        "CREATE INDEX embeddings_ivf_idx ON embeddings \
         USING ivfflat (embedding vector_l2_ops) WITH (lists = 8, probes = 3)",
        &cat,
    )
    .expect("bind ivfflat")
    else {
        panic!("expected CreateIndex plan");
    };
    assert_eq!(method, LogicalIndexMethod::IvfFlat);
    assert_eq!(columns, vec![1]);
    assert_eq!(opclasses, vec![Some("vector_l2_ops".to_owned())]);
    assert_eq!(index_options.len(), 2);
    assert_eq!(index_options[0].name, "lists");
    assert_eq!(index_options[0].value, "8");
    assert_eq!(index_options[1].name, "probes");
    assert_eq!(index_options[1].value, "3");
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
fn rejects_unique_hnsw_index() {
    let cat = embeddings_catalog();
    let err = parse_and_bind(
        "CREATE UNIQUE INDEX embeddings_hnsw_idx ON embeddings USING hnsw (embedding)",
        &cat,
    )
    .unwrap_err();
    assert!(matches!(err, PlanError::NotSupported(_)), "got {err:?}");
}

#[test]
fn rejects_hnsw_index_on_non_vector_column() {
    let cat = users_catalog();
    let err =
        parse_and_bind("CREATE INDEX users_hnsw_idx ON users USING hnsw (id)", &cat).unwrap_err();
    assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
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
fn binds_drop_index_with_known_index() {
    let cat = users_index_catalog();
    let plan = parse_and_bind("DROP INDEX users_id_idx", &cat).expect("drop index binds");
    let LogicalPlan::DropIndex {
        indexes, if_exists, ..
    } = plan
    else {
        panic!("expected DropIndex plan");
    };
    assert_eq!(indexes, vec!["users_id_idx".to_string()]);
    assert!(!if_exists);
}

#[test]
fn binds_drop_index_preserves_explicit_namespace() {
    let mut cat = users_index_catalog();
    cat.register_index_in_schema("app", "users_id_idx");
    let plan = parse_and_bind("DROP INDEX app.users_id_idx", &cat).expect("drop index binds");
    let LogicalPlan::DropIndex {
        indexes,
        index_namespaces,
        ..
    } = plan
    else {
        panic!("expected DropIndex plan");
    };
    assert_eq!(indexes, vec!["users_id_idx".to_string()]);
    assert_eq!(index_namespaces, vec![Some("app".to_string())]);
}

#[test]
fn binds_drop_index_preserves_quoted_dotted_public_name() {
    let mut cat = users_index_catalog();
    cat.register_index("idx.dotted");
    let plan = parse_and_bind("DROP INDEX \"idx.dotted\"", &cat).expect("drop index binds");
    let LogicalPlan::DropIndex {
        indexes,
        index_namespaces,
        ..
    } = plan
    else {
        panic!("expected DropIndex plan");
    };
    assert_eq!(indexes, vec!["idx.dotted".to_string()]);
    assert_eq!(index_namespaces, vec![None]);
}

#[test]
fn drop_index_if_exists_silently_omits_missing_indexes() {
    let cat = users_index_catalog();
    let plan = parse_and_bind("DROP INDEX IF EXISTS users_id_idx, nope", &cat)
        .expect("drop index if exists binds");
    let LogicalPlan::DropIndex {
        indexes, if_exists, ..
    } = plan
    else {
        panic!("expected DropIndex plan");
    };
    assert!(if_exists);
    assert_eq!(indexes, vec!["users_id_idx".to_string()]);
}

#[test]
fn drop_index_without_if_exists_rejects_missing_index() {
    let cat = users_catalog();
    let err = parse_and_bind("DROP INDEX nonexistent_idx", &cat).unwrap_err();
    assert!(matches!(err, PlanError::IndexNotFound(_)), "got {err:?}");
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
    let LogicalAlterTableAction::AddColumn { column, default } = action else {
        panic!("expected AddColumn action");
    };
    assert_eq!(column.name, "extra");
    assert_eq!(column.data_type, DataType::Int32);
    assert!(column.nullable, "ADD COLUMN defaults to nullable");
    assert!(default.is_none());
}

#[test]
fn binds_alter_table_add_column_not_null() {
    let plan = parse_bind_ok("ALTER TABLE users ADD COLUMN flag BOOLEAN NOT NULL");
    let LogicalPlan::AlterTable { action, .. } = plan else {
        panic!("expected AlterTable plan");
    };
    let LogicalAlterTableAction::AddColumn { column, default } = action else {
        panic!("expected AddColumn action");
    };
    assert_eq!(column.data_type, DataType::Bool);
    assert!(!column.nullable);
    assert!(default.is_none());
}

#[test]
fn binds_alter_table_add_column_default() {
    let plan = parse_bind_ok("ALTER TABLE users ADD COLUMN extra INTEGER DEFAULT 7");
    let LogicalPlan::AlterTable { action, .. } = plan else {
        panic!("expected AlterTable plan");
    };
    let LogicalAlterTableAction::AddColumn { column, default } = action else {
        panic!("expected AddColumn action");
    };
    assert_eq!(column.name, "extra");
    assert_eq!(column.data_type, DataType::Int32);
    let Some(ScalarExpr::Literal {
        value: Value::Int32(7),
        ..
    }) = default
    else {
        panic!("expected integer literal default");
    };
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

#[test]
fn binds_alter_table_add_check_constraint() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "ALTER TABLE users ADD CONSTRAINT score_positive CHECK (score > 0)",
        &cat,
    )
    .unwrap();
    let LogicalPlan::AlterTable { action, .. } = plan else {
        panic!("expected AlterTable plan");
    };
    let LogicalAlterTableAction::AddCheckConstraint { constraint } = action else {
        panic!("expected AddCheckConstraint action");
    };
    assert_eq!(constraint.name, "score_positive");
    // The bound predicate must resolve `score` to its column index.
    assert!(
        matches!(constraint.expr.data_type(), DataType::Bool | DataType::Null),
        "CHECK predicate must be boolean, got {:?}",
        constraint.expr.data_type()
    );
}

#[test]
fn binds_alter_table_add_unnamed_check_uses_pg_default_name() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "ALTER TABLE users ADD CONSTRAINT score_chk CHECK (score >= 0)",
        &cat,
    )
    .unwrap();
    let LogicalPlan::AlterTable { action, .. } = plan else {
        panic!("expected AlterTable plan");
    };
    let LogicalAlterTableAction::AddCheckConstraint { constraint } = action else {
        panic!("expected AddCheckConstraint action");
    };
    assert_eq!(constraint.name, "score_chk");
}

#[test]
fn alter_table_add_check_rejects_non_boolean() {
    let cat = users_catalog();
    let err =
        parse_and_bind("ALTER TABLE users ADD CONSTRAINT bad CHECK (id + 1)", &cat).unwrap_err();
    assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
}

#[test]
fn alter_table_add_check_rejects_unknown_column() {
    let cat = users_catalog();
    let err = parse_and_bind(
        "ALTER TABLE users ADD CONSTRAINT bad CHECK (missing_col > 0)",
        &cat,
    )
    .unwrap_err();
    assert!(matches!(err, PlanError::ColumnNotFound(_)), "got {err:?}");
}

#[test]
fn binds_alter_table_drop_constraint() {
    let cat = users_catalog();
    let plan = parse_and_bind(
        "ALTER TABLE users DROP CONSTRAINT IF EXISTS some_con CASCADE",
        &cat,
    )
    .unwrap();
    let LogicalPlan::AlterTable { action, .. } = plan else {
        panic!("expected AlterTable plan");
    };
    let LogicalAlterTableAction::DropConstraint {
        name,
        if_exists,
        cascade,
    } = action
    else {
        panic!("expected DropConstraint action");
    };
    assert_eq!(name, "some_con");
    assert!(if_exists);
    assert!(cascade);
}

#[test]
fn alter_table_add_foreign_key_constraint_is_rejected() {
    let cat = users_catalog();
    let err = parse_and_bind(
        "ALTER TABLE users ADD CONSTRAINT fk FOREIGN KEY (id) REFERENCES other (id)",
        &cat,
    )
    .unwrap_err();
    assert!(
        matches!(
            err,
            PlanError::NotSupported(_) | PlanError::NotSupportedOwned(_)
        ),
        "ADD FOREIGN KEY must be honestly deferred, got {err:?}"
    );
}

#[test]
fn binds_alter_view_rename_and_set_schema() {
    let mut cat = users_catalog();
    cat.register(
        "user_v",
        TableMeta::new(
            Schema::new([
                Field::required("id", DataType::Int32),
                Field::nullable("name", DataType::Text { max_len: None }),
            ])
            .expect("schema ok"),
        ),
    );
    let plan = parse_and_bind("ALTER VIEW user_v RENAME TO user_v2", &cat).unwrap();
    let LogicalPlan::AlterView { action, .. } = plan else {
        panic!("expected AlterView plan");
    };
    let LogicalAlterViewAction::RenameView { new_name } = action else {
        panic!("expected RenameView action");
    };
    assert_eq!(new_name, "user_v2");

    let plan = parse_and_bind("ALTER VIEW user_v SET SCHEMA app", &cat).unwrap();
    let LogicalPlan::AlterView { action, .. } = plan else {
        panic!("expected AlterView plan");
    };
    let LogicalAlterViewAction::SetSchema { new_schema } = action else {
        panic!("expected SetSchema action");
    };
    assert_eq!(new_schema, "app");
}

#[test]
fn alter_view_replace_definition_is_explicitly_unsupported() {
    let mut cat = users_catalog();
    cat.register(
        "user_v",
        TableMeta::new(Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok")),
    );
    let err = parse_and_bind("ALTER VIEW user_v AS SELECT id FROM users", &cat)
        .expect_err("replacement unsupported");
    assert_eq!(
        err,
        PlanError::NotSupported(
            "ALTER VIEW ... AS SELECT is not supported until dependency-safe view replacement lands"
        )
    );
}

// ALTER TABLE ALTER COLUMN SET/DROP NOT NULL and SET/DROP DEFAULT
// -----------------------------------------------------------------------

#[test]
fn binds_alter_column_set_not_null_resolves_index() {
    let plan = parse_bind_ok("ALTER TABLE users ALTER COLUMN name SET NOT NULL");
    let LogicalPlan::AlterTable { action, .. } = plan else {
        panic!("expected AlterTable plan");
    };
    let LogicalAlterTableAction::AlterColumnSetNotNull {
        column_index,
        column_name,
    } = action
    else {
        panic!("expected AlterColumnSetNotNull action");
    };
    assert_eq!(column_index, 1);
    assert_eq!(column_name, "name");
}

#[test]
fn binds_alter_column_drop_not_null_without_column_keyword() {
    let plan = parse_bind_ok("ALTER TABLE users ALTER name DROP NOT NULL");
    let LogicalPlan::AlterTable { action, .. } = plan else {
        panic!("expected AlterTable plan");
    };
    let LogicalAlterTableAction::AlterColumnDropNotNull { column_index, .. } = action else {
        panic!("expected AlterColumnDropNotNull action");
    };
    assert_eq!(column_index, 1);
}

#[test]
fn binds_alter_column_set_default_type_checks_and_coerces() {
    // `score` is Float64; an integer literal default must coerce.
    let plan = parse_bind_ok("ALTER TABLE users ALTER COLUMN score SET DEFAULT 1");
    let LogicalPlan::AlterTable { action, .. } = plan else {
        panic!("expected AlterTable plan");
    };
    let LogicalAlterTableAction::AlterColumnSetDefault {
        column_index,
        default,
        ..
    } = action
    else {
        panic!("expected AlterColumnSetDefault action");
    };
    assert_eq!(column_index, 2);
    assert_eq!(default.data_type(), DataType::Float64);
}

#[test]
fn binds_alter_column_drop_default_resolves_index() {
    let plan = parse_bind_ok("ALTER TABLE users ALTER COLUMN score DROP DEFAULT");
    let LogicalPlan::AlterTable { action, .. } = plan else {
        panic!("expected AlterTable plan");
    };
    let LogicalAlterTableAction::AlterColumnDropDefault { column_index, .. } = action else {
        panic!("expected AlterColumnDropDefault action");
    };
    assert_eq!(column_index, 2);
}

#[test]
fn alter_column_unknown_column_is_column_not_found() {
    let cat = users_catalog();
    let err = parse_and_bind("ALTER TABLE users ALTER COLUMN bogus SET NOT NULL", &cat)
        .expect_err("unknown column");
    assert!(matches!(err, PlanError::ColumnNotFound(ref s) if s == "bogus"));
}

#[test]
fn alter_column_set_default_rejects_uncoercible_type() {
    // `id` is Int32; a text default cannot coerce.
    let cat = users_catalog();
    let err = parse_and_bind("ALTER TABLE users ALTER COLUMN id SET DEFAULT 'abc'", &cat)
        .expect_err("uncoercible default");
    assert!(matches!(err, PlanError::TypeMismatch(_)));
}

#[test]
fn alter_column_set_default_rejects_volatile_expression() {
    // A default referencing a parameter is not a constant-folded literal.
    let cat = users_catalog();
    let err = parse_and_bind("ALTER TABLE users ALTER COLUMN id SET DEFAULT $1", &cat)
        .expect_err("non-safe default");
    assert!(matches!(err, PlanError::NotSupported(_)));
}
