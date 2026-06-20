//! `pg_database` and the `information_schema` views.

use ultrasql_core::{DataType, Field, Oid, Schema, Value};
use ultrasql_planner::LogicalReferentialAction;

use crate::pipeline::LowerCtx;

use super::common::*;
use super::indexes::virtual_constraints;
use super::objects::sequence_display_name;
use super::pgproc::{pg_proc_builtins, pg_proc_oid, pg_type_name_from_oid};

pub(super) fn schema_pg_database() -> Schema {
    schema([
        Field::required("datname", text()),
        Field::required("datdba", DataType::Int64),
        Field::required("encoding", DataType::Int32),
        Field::required("datallowconn", DataType::Bool),
        Field::required("datcollate", text()),
        Field::required("datctype", text()),
        Field::nullable(
            "datacl",
            DataType::Array(Box::new(DataType::Text { max_len: None })),
        ),
    ])
}

pub(super) fn rows_pg_database() -> Vec<Vec<Value>> {
    vec![vec![
        v_text("ultrasql"),
        Value::Int64(10),
        Value::Int32(6),
        Value::Bool(true),
        v_text("C"),
        v_text("C"),
        Value::Null,
    ]]
}

pub(super) fn schema_information_schema_tables() -> Schema {
    schema([
        Field::required("table_catalog", text()),
        Field::required("table_schema", text()),
        Field::required("table_name", text()),
        Field::required("table_type", text()),
        Field::nullable("self_referencing_column_name", text()),
        Field::nullable("reference_generation", text()),
        Field::nullable("user_defined_type_catalog", text()),
        Field::nullable("user_defined_type_schema", text()),
        Field::nullable("user_defined_type_name", text()),
        Field::required("is_insertable_into", text()),
        Field::required("is_typed", text()),
        Field::nullable("commit_action", text()),
    ])
}

pub(super) fn rows_information_schema_tables(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    table_entries(ctx)
        .into_iter()
        .filter(|entry| {
            entry.schema_name != "pg_catalog"
                && entry.schema_name != "information_schema"
                && !is_materialized_view_entry(entry)
        })
        .map(|entry| {
            let is_view = is_regular_view_entry(&entry);
            vec![
                v_text("ultrasql"),
                v_text(entry.schema_name.clone()),
                v_text(entry.name.clone()),
                v_text(if is_view { "VIEW" } else { "BASE TABLE" }),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                v_text(if is_view { "NO" } else { "YES" }),
                v_text("NO"),
                Value::Null,
            ]
        })
        .collect()
}

pub(super) fn schema_information_schema_columns() -> Schema {
    schema([
        Field::required("table_catalog", text()),
        Field::required("table_schema", text()),
        Field::required("table_name", text()),
        Field::required("column_name", text()),
        Field::required("ordinal_position", DataType::Int32),
        Field::nullable("column_default", text()),
        Field::required("is_nullable", text()),
        Field::required("data_type", text()),
        Field::nullable("character_maximum_length", DataType::Int32),
        Field::nullable("character_octet_length", DataType::Int32),
        Field::nullable("numeric_precision", DataType::Int32),
        Field::nullable("numeric_precision_radix", DataType::Int32),
        Field::nullable("numeric_scale", DataType::Int32),
        Field::nullable("datetime_precision", DataType::Int32),
        Field::nullable("interval_type", text()),
        Field::nullable("interval_precision", DataType::Int32),
    ])
}

pub(super) fn rows_information_schema_columns(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for entry in table_entries(ctx) {
        if entry.schema_name == "pg_catalog" || entry.schema_name == "information_schema" {
            continue;
        }
        for (idx, field) in entry.schema.fields().iter().enumerate() {
            rows.push(vec![
                v_text("ultrasql"),
                v_text(entry.schema_name.clone()),
                v_text(entry.name.clone()),
                v_text(field.name.clone()),
                Value::Int32(i32::try_from(idx + 1).unwrap_or(i32::MAX)),
                Value::Null,
                v_text(if field.nullable { "YES" } else { "NO" }),
                v_text(data_type_name(&field.data_type)),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
            ]);
        }
    }
    rows
}

pub(super) fn constraint_type_name(kind: &str) -> &'static str {
    match kind {
        "p" => "PRIMARY KEY",
        "u" => "UNIQUE",
        "f" => "FOREIGN KEY",
        "c" => "CHECK",
        _ => "CHECK",
    }
}

pub(super) fn schema_information_schema_table_constraints() -> Schema {
    schema([
        Field::required("constraint_catalog", text()),
        Field::required("constraint_schema", text()),
        Field::required("constraint_name", text()),
        Field::required("table_schema", text()),
        Field::required("table_name", text()),
        Field::required("constraint_type", text()),
        Field::required("is_deferrable", text()),
        Field::required("initially_deferred", text()),
        Field::required("enforced", text()),
        Field::required("nulls_distinct", text()),
    ])
}

pub(super) fn rows_information_schema_table_constraints(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    virtual_constraints(ctx)
        .into_iter()
        .map(|c| {
            vec![
                v_text("ultrasql"),
                v_text(c.table_schema.clone()),
                v_text(c.name),
                v_text(c.table_schema),
                v_text(c.table_name),
                v_text(constraint_type_name(c.kind)),
                v_text(if c.deferrable { "YES" } else { "NO" }),
                v_text(if c.initially_deferred { "YES" } else { "NO" }),
                v_text("YES"),
                v_text("YES"),
            ]
        })
        .collect()
}

pub(super) fn field_name_for_attnum(
    ctx: &LowerCtx<'_>,
    table_oid: Oid,
    col_idx: usize,
) -> Option<String> {
    let table = ctx.catalog_snapshot.tables_by_oid.get(&table_oid)?;
    Some(table.schema.field(col_idx)?.name.clone())
}

pub(super) fn schema_information_schema_key_column_usage() -> Schema {
    schema([
        Field::required("constraint_catalog", text()),
        Field::required("constraint_schema", text()),
        Field::required("constraint_name", text()),
        Field::required("table_catalog", text()),
        Field::required("table_schema", text()),
        Field::required("table_name", text()),
        Field::required("column_name", text()),
        Field::required("ordinal_position", DataType::Int32),
        Field::nullable("position_in_unique_constraint", DataType::Int32),
    ])
}

pub(super) fn rows_information_schema_key_column_usage(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for c in virtual_constraints(ctx) {
        if !matches!(c.kind, "p" | "u" | "f") {
            continue;
        }
        for (idx, col_idx) in c.columns.iter().copied().enumerate() {
            let Some(column_name) = field_name_for_attnum(ctx, c.table_oid, col_idx) else {
                continue;
            };
            rows.push(vec![
                v_text("ultrasql"),
                v_text(c.table_schema.clone()),
                v_text(c.name.clone()),
                v_text("ultrasql"),
                v_text(c.table_schema.clone()),
                v_text(c.table_name.clone()),
                v_text(column_name),
                Value::Int32(i32::try_from(idx + 1).unwrap_or(i32::MAX)),
                if c.kind == "f" {
                    Value::Int32(i32::try_from(idx + 1).unwrap_or(i32::MAX))
                } else {
                    Value::Null
                },
            ]);
        }
    }
    rows
}

pub(super) fn referenced_constraint_name(ctx: &LowerCtx<'_>, table_oid: Oid) -> String {
    ctx.catalog_snapshot
        .indexes_by_table
        .get(&table_oid)
        .and_then(|indexes| {
            indexes
                .iter()
                .find(|idx| idx.is_primary)
                .or_else(|| indexes.iter().find(|idx| idx.is_unique))
        })
        .map(|idx| idx.name.clone())
        .unwrap_or_else(|| format!("{}_key", table_oid.raw()))
}

pub(super) const fn referential_action_name(action: LogicalReferentialAction) -> &'static str {
    match action {
        LogicalReferentialAction::NoAction => "NO ACTION",
        LogicalReferentialAction::Restrict => "RESTRICT",
        LogicalReferentialAction::Cascade => "CASCADE",
        LogicalReferentialAction::SetNull => "SET NULL",
        LogicalReferentialAction::SetDefault => "SET DEFAULT",
    }
}

pub(super) fn schema_information_schema_referential_constraints() -> Schema {
    schema([
        Field::required("constraint_catalog", text()),
        Field::required("constraint_schema", text()),
        Field::required("constraint_name", text()),
        Field::required("unique_constraint_catalog", text()),
        Field::required("unique_constraint_schema", text()),
        Field::required("unique_constraint_name", text()),
        Field::required("match_option", text()),
        Field::required("update_rule", text()),
        Field::required("delete_rule", text()),
    ])
}

pub(super) fn rows_information_schema_referential_constraints(
    ctx: &LowerCtx<'_>,
) -> Vec<Vec<Value>> {
    virtual_constraints(ctx)
        .into_iter()
        .filter(|c| c.kind == "f")
        .map(|c| {
            let target_oid = c.foreign_table_oid.unwrap_or(c.table_oid);
            let unique_name = referenced_constraint_name(ctx, target_oid);
            let target_schema = ctx
                .catalog_snapshot
                .tables_by_oid
                .get(&target_oid)
                .map(|table| table.schema_name.clone())
                .unwrap_or_else(|| "public".to_owned());
            vec![
                v_text("ultrasql"),
                v_text(c.table_schema),
                v_text(c.name),
                v_text("ultrasql"),
                v_text(target_schema),
                v_text(unique_name),
                v_text("NONE"),
                v_text(referential_action_name(c.on_update)),
                v_text(referential_action_name(c.on_delete)),
            ]
        })
        .collect()
}

pub(super) fn schema_information_schema_check_constraints() -> Schema {
    schema([
        Field::required("constraint_catalog", text()),
        Field::required("constraint_schema", text()),
        Field::required("constraint_name", text()),
        Field::nullable("check_clause", text()),
    ])
}

pub(super) fn rows_information_schema_check_constraints(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    virtual_constraints(ctx)
        .into_iter()
        .filter(|c| c.kind == "c")
        .map(|c| {
            vec![
                v_text("ultrasql"),
                v_text(c.table_schema),
                v_text(c.name),
                c.check_clause.map_or(Value::Null, v_text),
            ]
        })
        .collect()
}

pub(super) fn schema_information_schema_schemata() -> Schema {
    schema([
        Field::required("catalog_name", text()),
        Field::required("schema_name", text()),
        Field::required("schema_owner", text()),
        Field::nullable("default_character_set_catalog", text()),
        Field::nullable("default_character_set_schema", text()),
        Field::nullable("default_character_set_name", text()),
        Field::nullable("sql_path", text()),
    ])
}

pub(super) fn rows_information_schema_schemata(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows = vec![
        vec![
            v_text("ultrasql"),
            v_text("pg_catalog"),
            v_text("ultrasql"),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ],
        vec![
            v_text("ultrasql"),
            v_text("information_schema"),
            v_text("ultrasql"),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ],
        vec![
            v_text("ultrasql"),
            v_text("public"),
            v_text("ultrasql"),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ],
    ];
    rows.extend(
        runtime_schema_rows(ctx)
            .into_iter()
            .map(|(name, owner_role, _)| {
                vec![
                    v_text("ultrasql"),
                    v_text(name),
                    v_text(owner_role),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ]
            }),
    );
    rows
}

pub(super) fn schema_information_schema_sequences() -> Schema {
    schema([
        Field::required("sequence_catalog", text()),
        Field::required("sequence_schema", text()),
        Field::required("sequence_name", text()),
        Field::required("data_type", text()),
        Field::nullable("numeric_precision", DataType::Int32),
        Field::nullable("numeric_precision_radix", DataType::Int32),
        Field::nullable("numeric_scale", DataType::Int32),
        Field::required("start_value", text()),
        Field::required("minimum_value", text()),
        Field::required("maximum_value", text()),
        Field::required("increment", text()),
        Field::required("cycle_option", text()),
    ])
}

pub(super) fn rows_information_schema_sequences(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut seqs: Vec<_> = ctx
        .sequences
        .iter()
        .map(|e| (e.key().clone(), e.value().clone()))
        .collect();
    seqs.sort_by(|a, b| a.0.cmp(&b.0));
    seqs.into_iter()
        .map(|(name, seq)| {
            let opts = seq.options_snapshot();
            let namespace = ctx
                .sequence_namespaces
                .get(&name)
                .map_or_else(|| "public".to_owned(), |entry| entry.value().clone());
            let display_name = sequence_display_name(&name, &namespace);
            vec![
                v_text("ultrasql"),
                v_text(namespace),
                v_text(display_name),
                v_text("bigint"),
                Value::Null,
                Value::Null,
                Value::Null,
                v_text(opts.start.to_string()),
                v_text(seq.min_value().to_string()),
                v_text(seq.max_value().to_string()),
                v_text(opts.increment.to_string()),
                v_text(if opts.cycle { "YES" } else { "NO" }),
            ]
        })
        .collect()
}

pub(super) fn schema_information_schema_routines() -> Schema {
    schema([
        Field::required("specific_catalog", text()),
        Field::required("specific_schema", text()),
        Field::required("specific_name", text()),
        Field::required("routine_catalog", text()),
        Field::required("routine_schema", text()),
        Field::required("routine_name", text()),
        Field::required("routine_type", text()),
        Field::nullable("data_type", text()),
        Field::nullable("type_udt_catalog", text()),
        Field::nullable("type_udt_schema", text()),
        Field::nullable("type_udt_name", text()),
        Field::required("is_deterministic", text()),
        Field::required("sql_data_access", text()),
        Field::required("security_type", text()),
    ])
}

pub(super) fn rows_information_schema_routines() -> Vec<Vec<Value>> {
    pg_proc_builtins()
        .iter()
        .enumerate()
        .filter_map(|(offset, builtin)| {
            let oid = pg_proc_oid(offset)?;
            Some(vec![
                v_text("ultrasql"),
                v_text("pg_catalog"),
                v_text(format!("{}_{}", builtin.name, oid)),
                v_text("ultrasql"),
                v_text("pg_catalog"),
                v_text(builtin.name),
                v_text("FUNCTION"),
                v_text(pg_type_name_from_oid(builtin.return_type_oid)),
                Value::Null,
                Value::Null,
                Value::Null,
                v_text("NO"),
                v_text("READS SQL DATA"),
                v_text("INVOKER"),
            ])
        })
        .collect()
}

pub(super) fn schema_information_schema_triggers() -> Schema {
    schema([
        Field::required("trigger_catalog", text()),
        Field::required("trigger_schema", text()),
        Field::required("trigger_name", text()),
        Field::required("event_manipulation", text()),
        Field::required("event_object_catalog", text()),
        Field::required("event_object_schema", text()),
        Field::required("event_object_table", text()),
        Field::required("action_order", DataType::Int32),
        Field::nullable("action_condition", text()),
        Field::required("action_statement", text()),
        Field::required("action_orientation", text()),
        Field::required("action_timing", text()),
    ])
}
