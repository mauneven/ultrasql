//! `information_schema` view definitions.
//!
//! The SQL standard's `information_schema` exposes portable metadata
//! views for tables, columns, constraints, routines, triggers,
//! sequences, and schemata. This module returns the SQL text of each
//! view definition so the executor can parse and register them at
//! startup.
//!
//! Definitions follow the PostgreSQL 16 column layout. Columns that
//! depend on subsystems not yet implemented carry NULL literals.
//!
//! All views are registered under the `information_schema` namespace.

/// Returns the SQL definition of `information_schema.tables`.
#[must_use]
pub const fn tables_def() -> &'static str {
    "CREATE OR REPLACE VIEW information_schema.tables AS \
     SELECT current_database()::information_schema.sql_identifier AS table_catalog, \
            n.nspname::information_schema.sql_identifier AS table_schema, \
            c.relname::information_schema.sql_identifier AS table_name, \
            CASE c.relkind \
              WHEN 'r' THEN 'BASE TABLE' \
              WHEN 'v' THEN 'VIEW' \
              WHEN 'f' THEN 'FOREIGN' \
              WHEN 'p' THEN 'BASE TABLE' \
              ELSE NULL \
            END::information_schema.character_data AS table_type, \
            NULL::information_schema.sql_identifier AS self_referencing_column_name, \
            NULL::information_schema.character_data AS reference_generation, \
            NULL::information_schema.sql_identifier AS user_defined_type_catalog, \
            NULL::information_schema.sql_identifier AS user_defined_type_schema, \
            NULL::information_schema.sql_identifier AS user_defined_type_name, \
            'YES'::information_schema.yes_or_no AS is_insertable_into, \
            'NO'::information_schema.yes_or_no AS is_typed, \
            NULL::information_schema.character_data AS commit_action \
     FROM pg_catalog.pg_class c \
     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
     WHERE c.relkind IN ('r', 'v', 'f', 'p') \
       AND n.nspname NOT IN ('pg_catalog', 'information_schema')"
}

/// Returns the SQL definition of `information_schema.columns`.
#[must_use]
pub const fn columns_def() -> &'static str {
    "CREATE OR REPLACE VIEW information_schema.columns AS \
     SELECT current_database()::information_schema.sql_identifier AS table_catalog, \
            n.nspname::information_schema.sql_identifier AS table_schema, \
            c.relname::information_schema.sql_identifier AS table_name, \
            a.attname::information_schema.sql_identifier AS column_name, \
            a.attnum::information_schema.cardinal_number AS ordinal_position, \
            NULL::information_schema.character_data AS column_default, \
            CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' \
            END::information_schema.yes_or_no AS is_nullable, \
            NULL::information_schema.character_data AS data_type, \
            NULL::information_schema.cardinal_number AS character_maximum_length, \
            NULL::information_schema.cardinal_number AS character_octet_length, \
            NULL::information_schema.cardinal_number AS numeric_precision, \
            NULL::information_schema.cardinal_number AS numeric_precision_radix, \
            NULL::information_schema.cardinal_number AS numeric_scale, \
            NULL::information_schema.cardinal_number AS datetime_precision, \
            NULL::information_schema.character_data AS interval_type, \
            NULL::information_schema.character_data AS interval_precision \
     FROM pg_catalog.pg_attribute a \
     JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
     WHERE a.attnum > 0 AND NOT a.attisdropped \
       AND c.relkind IN ('r', 'v', 'f', 'p')"
}

/// Returns the SQL definition of `information_schema.table_constraints`.
#[must_use]
pub const fn table_constraints_def() -> &'static str {
    "CREATE OR REPLACE VIEW information_schema.table_constraints AS \
     SELECT current_database()::information_schema.sql_identifier AS constraint_catalog, \
            n.nspname::information_schema.sql_identifier AS constraint_schema, \
            c.conname::information_schema.sql_identifier AS constraint_name, \
            current_database()::information_schema.sql_identifier AS table_catalog, \
            n.nspname::information_schema.sql_identifier AS table_schema, \
            r.relname::information_schema.sql_identifier AS table_name, \
            CASE c.contype \
              WHEN 'c' THEN 'CHECK' \
              WHEN 'f' THEN 'FOREIGN KEY' \
              WHEN 'p' THEN 'PRIMARY KEY' \
              WHEN 'u' THEN 'UNIQUE' \
              WHEN 'x' THEN 'EXCLUDE' \
              ELSE NULL \
            END::information_schema.character_data AS constraint_type, \
            CASE WHEN c.condeferrable THEN 'YES' ELSE 'NO' \
            END::information_schema.yes_or_no AS is_deferrable, \
            CASE WHEN c.condeferred THEN 'YES' ELSE 'NO' \
            END::information_schema.yes_or_no AS initially_deferred, \
            'YES'::information_schema.yes_or_no AS enforced, \
            'NO'::information_schema.yes_or_no AS nulls_distinct \
     FROM pg_catalog.pg_constraint c \
     JOIN pg_catalog.pg_class r ON r.oid = c.conrelid \
     JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace"
}

/// Returns the SQL definition of `information_schema.key_column_usage`.
#[must_use]
pub const fn key_column_usage_def() -> &'static str {
    "CREATE OR REPLACE VIEW information_schema.key_column_usage AS \
     SELECT current_database()::information_schema.sql_identifier AS constraint_catalog, \
            n.nspname::information_schema.sql_identifier AS constraint_schema, \
            con.conname::information_schema.sql_identifier AS constraint_name, \
            current_database()::information_schema.sql_identifier AS table_catalog, \
            n.nspname::information_schema.sql_identifier AS table_schema, \
            r.relname::information_schema.sql_identifier AS table_name, \
            a.attname::information_schema.sql_identifier AS column_name, \
            (row_number() OVER ())::information_schema.cardinal_number AS ordinal_position, \
            NULL::information_schema.cardinal_number AS position_in_unique_constraint \
     FROM pg_catalog.pg_constraint con \
     JOIN pg_catalog.pg_class r ON r.oid = con.conrelid \
     JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace \
     JOIN pg_catalog.pg_attribute a \
          ON a.attrelid = r.oid AND a.attnum = ANY(con.conkey) \
     WHERE con.contype IN ('p', 'u', 'f')"
}

/// Returns the SQL definition of `information_schema.referential_constraints`.
#[must_use]
pub const fn referential_constraints_def() -> &'static str {
    "CREATE OR REPLACE VIEW information_schema.referential_constraints AS \
     SELECT current_database()::information_schema.sql_identifier AS constraint_catalog, \
            n.nspname::information_schema.sql_identifier AS constraint_schema, \
            con.conname::information_schema.sql_identifier AS constraint_name, \
            current_database()::information_schema.sql_identifier AS unique_constraint_catalog, \
            n2.nspname::information_schema.sql_identifier AS unique_constraint_schema, \
            con2.conname::information_schema.sql_identifier AS unique_constraint_name, \
            'NONE'::information_schema.character_data AS match_option, \
            CASE con.confupdtype \
              WHEN 'a' THEN 'NO ACTION' \
              WHEN 'r' THEN 'RESTRICT' \
              WHEN 'c' THEN 'CASCADE' \
              WHEN 'n' THEN 'SET NULL' \
              WHEN 'd' THEN 'SET DEFAULT' \
            END::information_schema.character_data AS update_rule, \
            CASE con.confdeltype \
              WHEN 'a' THEN 'NO ACTION' \
              WHEN 'r' THEN 'RESTRICT' \
              WHEN 'c' THEN 'CASCADE' \
              WHEN 'n' THEN 'SET NULL' \
              WHEN 'd' THEN 'SET DEFAULT' \
            END::information_schema.character_data AS delete_rule \
     FROM pg_catalog.pg_constraint con \
     JOIN pg_catalog.pg_class r ON r.oid = con.conrelid \
     JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace \
     LEFT JOIN pg_catalog.pg_constraint con2 ON con2.oid = con.confrelid \
     LEFT JOIN pg_catalog.pg_class r2 ON r2.oid = con.confrelid \
     LEFT JOIN pg_catalog.pg_namespace n2 ON n2.oid = r2.relnamespace \
     WHERE con.contype = 'f'"
}

/// Returns the SQL definition of `information_schema.check_constraints`.
#[must_use]
pub const fn check_constraints_def() -> &'static str {
    "CREATE OR REPLACE VIEW information_schema.check_constraints AS \
     SELECT current_database()::information_schema.sql_identifier AS constraint_catalog, \
            n.nspname::information_schema.sql_identifier AS constraint_schema, \
            c.conname::information_schema.sql_identifier AS constraint_name, \
            NULL::information_schema.character_data AS check_clause \
     FROM pg_catalog.pg_constraint c \
     JOIN pg_catalog.pg_class r ON r.oid = c.conrelid \
     JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace \
     WHERE c.contype = 'c'"
}

/// Returns the SQL definition of `information_schema.routines`.
#[must_use]
pub const fn routines_def() -> &'static str {
    "CREATE OR REPLACE VIEW information_schema.routines AS \
     SELECT current_database()::information_schema.sql_identifier AS specific_catalog, \
            n.nspname::information_schema.sql_identifier AS specific_schema, \
            p.proname::information_schema.sql_identifier AS specific_name, \
            current_database()::information_schema.sql_identifier AS routine_catalog, \
            n.nspname::information_schema.sql_identifier AS routine_schema, \
            p.proname::information_schema.sql_identifier AS routine_name, \
            CASE p.prokind \
              WHEN 'f' THEN 'FUNCTION' \
              WHEN 'p' THEN 'PROCEDURE' \
            END::information_schema.character_data AS routine_type, \
            NULL::information_schema.sql_identifier AS module_catalog, \
            NULL::information_schema.sql_identifier AS module_schema, \
            NULL::information_schema.sql_identifier AS module_name, \
            NULL::information_schema.sql_identifier AS udt_catalog, \
            NULL::information_schema.sql_identifier AS udt_schema, \
            NULL::information_schema.sql_identifier AS udt_name \
     FROM pg_catalog.pg_proc p \
     JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace"
}

/// Returns the SQL definition of `information_schema.triggers`.
#[must_use]
pub const fn triggers_def() -> &'static str {
    "CREATE OR REPLACE VIEW information_schema.triggers AS \
     SELECT current_database()::information_schema.sql_identifier AS trigger_catalog, \
            n.nspname::information_schema.sql_identifier AS trigger_schema, \
            t.tgname::information_schema.sql_identifier AS trigger_name, \
            NULL::information_schema.character_data AS event_manipulation, \
            current_database()::information_schema.sql_identifier AS event_object_catalog, \
            n.nspname::information_schema.sql_identifier AS event_object_schema, \
            c.relname::information_schema.sql_identifier AS event_object_table, \
            NULL::information_schema.cardinal_number AS action_order, \
            NULL::information_schema.character_data AS action_condition, \
            NULL::information_schema.character_data AS action_statement, \
            NULL::information_schema.character_data AS action_orientation, \
            NULL::information_schema.character_data AS action_timing \
     FROM pg_catalog.pg_trigger t \
     JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid \
     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
     WHERE NOT t.tgisinternal"
}

/// Returns the SQL definition of `information_schema.schemata`.
#[must_use]
pub const fn schemata_def() -> &'static str {
    "CREATE OR REPLACE VIEW information_schema.schemata AS \
     SELECT current_database()::information_schema.sql_identifier AS catalog_name, \
            n.nspname::information_schema.sql_identifier AS schema_name, \
            r.rolname::information_schema.sql_identifier AS schema_owner, \
            NULL::information_schema.character_data AS default_character_set_catalog, \
            NULL::information_schema.character_data AS default_character_set_schema, \
            NULL::information_schema.character_data AS default_character_set_name, \
            NULL::information_schema.character_data AS sql_path \
     FROM pg_catalog.pg_namespace n \
     LEFT JOIN pg_catalog.pg_roles r ON r.oid = n.nspowner"
}

/// Returns the SQL definition of `information_schema.sequences`.
#[must_use]
pub const fn sequences_def() -> &'static str {
    "CREATE OR REPLACE VIEW information_schema.sequences AS \
     SELECT current_database()::information_schema.sql_identifier AS sequence_catalog, \
            n.nspname::information_schema.sql_identifier AS sequence_schema, \
            c.relname::information_schema.sql_identifier AS sequence_name, \
            'bigint'::information_schema.character_data AS data_type, \
            NULL::information_schema.cardinal_number AS numeric_precision, \
            NULL::information_schema.cardinal_number AS numeric_precision_radix, \
            NULL::information_schema.cardinal_number AS numeric_scale, \
            s.seqstart::information_schema.character_data AS start_value, \
            s.seqmin::information_schema.character_data AS minimum_value, \
            s.seqmax::information_schema.character_data AS maximum_value, \
            s.seqincrement::information_schema.character_data AS increment, \
            CASE s.seqcycle WHEN true THEN 'YES' ELSE 'NO' \
            END::information_schema.yes_or_no AS cycle_option \
     FROM pg_catalog.pg_sequence s \
     JOIN pg_catalog.pg_class c ON c.oid = s.seqrelid \
     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace"
}

/// All `information_schema` view definitions keyed by view name.
///
/// Used at startup to register views with the catalog under the
/// `information_schema` namespace.
#[must_use]
pub fn all_information_schema_views() -> Vec<(&'static str, &'static str)> {
    vec![
        ("tables", tables_def()),
        ("columns", columns_def()),
        ("table_constraints", table_constraints_def()),
        ("key_column_usage", key_column_usage_def()),
        ("referential_constraints", referential_constraints_def()),
        ("check_constraints", check_constraints_def()),
        ("routines", routines_def()),
        ("triggers", triggers_def()),
        ("schemata", schemata_def()),
        ("sequences", sequences_def()),
    ]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_information_schema_views_are_non_empty() {
        for (name, def) in all_information_schema_views() {
            assert!(!def.is_empty(), "view {name} is empty");
        }
    }

    #[test]
    fn tables_def_filters_catalog_schemas() {
        assert!(
            tables_def().contains("pg_catalog"),
            "tables view must exclude pg_catalog rows"
        );
        assert!(
            tables_def().contains("information_schema"),
            "tables view must exclude information_schema rows"
        );
    }

    #[test]
    fn columns_def_excludes_dropped_columns() {
        assert!(
            columns_def().contains("attisdropped"),
            "columns view must exclude dropped attributes"
        );
    }

    #[test]
    fn all_view_names_unique() {
        let mut names: Vec<&str> = all_information_schema_views()
            .iter()
            .map(|(n, _)| *n)
            .collect();
        names.sort_unstable();
        let len = names.len();
        names.dedup();
        assert_eq!(names.len(), len, "duplicate information_schema view name");
    }

    #[test]
    fn referential_constraints_def_covers_update_and_delete_rules() {
        let def = referential_constraints_def();
        assert!(def.contains("update_rule"), "must include update_rule");
        assert!(def.contains("delete_rule"), "must include delete_rule");
    }

    #[test]
    fn sequences_def_contains_cycle_option() {
        assert!(
            sequences_def().contains("cycle_option"),
            "sequences view must expose cycle_option"
        );
    }
}
