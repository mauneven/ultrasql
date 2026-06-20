//! `pg_namespace`, `pg_class`, `pg_attribute`, and `pg_attrdef` scans.

use ultrasql_core::{DataType, Field, Oid, Schema, Value};

use crate::pipeline::LowerCtx;

use super::common::*;
use super::pgtype::column_default_expr;

pub(super) fn schema_pg_namespace() -> Schema {
    schema([
        Field::required("oid", DataType::Int64),
        Field::required("nspname", text()),
        Field::required("nspowner", DataType::Int64),
        Field::nullable("nspacl", text_array()),
    ])
}

pub(super) fn rows_pg_namespace(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows = vec![
        vec![
            Value::Int64(PG_CATALOG_OID),
            v_text("pg_catalog"),
            Value::Int64(10),
            Value::Null,
        ],
        vec![
            Value::Int64(INFORMATION_SCHEMA_OID),
            v_text("information_schema"),
            Value::Int64(10),
            Value::Null,
        ],
        vec![
            Value::Int64(PUBLIC_OID),
            v_text("public"),
            Value::Int64(10),
            Value::Null,
        ],
    ];
    rows.extend(
        runtime_schema_rows(ctx)
            .into_iter()
            .map(|(name, owner_role, oid)| {
                vec![
                    Value::Int64(oid),
                    v_text(name),
                    Value::Int64(namespace_owner_oid(ctx, &owner_role)),
                    Value::Null,
                ]
            }),
    );
    rows
}

pub(super) fn schema_pg_class() -> Schema {
    schema([
        Field::required("oid", DataType::Int64),
        Field::required("relname", text()),
        Field::required("relnamespace", DataType::Int64),
        Field::required("reltype", DataType::Oid),
        Field::required("relkind", DataType::Text { max_len: Some(1) }),
        Field::required("relpages", DataType::Int32),
        Field::required("reltuples", DataType::Float64),
        Field::required("relfilenode", DataType::Int32),
        Field::required("relhasindex", DataType::Bool),
        Field::required("relchecks", DataType::Int32),
        Field::required("relhasrules", DataType::Bool),
        Field::required("relhastriggers", DataType::Bool),
        Field::required("relrowsecurity", DataType::Bool),
        Field::required("relforcerowsecurity", DataType::Bool),
        Field::required("relispartition", DataType::Bool),
        Field::required("reltablespace", DataType::Int64),
        Field::required("reloftype", DataType::Int64),
        Field::required("relpersistence", DataType::Text { max_len: Some(1) }),
        Field::required("relreplident", DataType::Text { max_len: Some(1) }),
        Field::required("reltoastrelid", DataType::Int64),
        Field::required("relam", DataType::Int64),
        Field::nullable("relpartbound", text()),
        Field::required("relowner", DataType::Int64),
        Field::nullable("relacl", text_array()),
        Field::nullable("reloptions", text_array()),
    ])
}

pub(super) fn rows_pg_class(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for entry in table_entries(ctx) {
        let relkind = if is_materialized_view_entry(&entry) {
            "m"
        } else if is_regular_view_entry(&entry) {
            "v"
        } else {
            "r"
        };
        rows.push(pg_class_row(VirtualClassRow {
            oid: entry.oid.raw(),
            relname: entry.name.clone(),
            relnamespace: namespace_oid(&entry.schema_name),
            reltype: relation_type_oid(entry.oid),
            relkind,
            relpages: i32::try_from(entry.n_blocks).unwrap_or(i32::MAX),
            relfilenode: i32::try_from(entry.root_block.raw()).unwrap_or(i32::MAX),
            relhasindex: ctx
                .catalog_snapshot
                .indexes_by_table
                .contains_key(&entry.oid),
        }));
    }
    let mut indexes: Vec<_> = ctx.catalog_snapshot.indexes.values().collect();
    indexes.sort_by(|a, b| a.name.cmp(&b.name));
    for index in indexes {
        rows.push(pg_class_row(VirtualClassRow {
            oid: index.oid.raw(),
            relname: index.name.clone(),
            relnamespace: namespace_oid(&index.schema_name),
            reltype: 0,
            relkind: "i",
            relpages: 0,
            relfilenode: i32::try_from(index.root_block.raw()).unwrap_or(i32::MAX),
            relhasindex: false,
        }));
    }
    let mut composites = ctx
        .catalog_snapshot
        .composite_types_by_oid
        .values()
        .collect::<Vec<_>>();
    composites.sort_by_key(|entry| entry.oid.raw());
    for entry in composites {
        rows.push(pg_class_row(VirtualClassRow {
            oid: entry.oid.raw(),
            relname: entry.name.clone(),
            relnamespace: namespace_oid(&entry.schema_name),
            reltype: entry.oid.raw(),
            relkind: "c",
            relpages: 0,
            relfilenode: 0,
            relhasindex: false,
        }));
    }
    rows
}

pub(super) struct VirtualClassRow {
    oid: u32,
    relname: String,
    relnamespace: i64,
    reltype: u32,
    relkind: &'static str,
    relpages: i32,
    relfilenode: i32,
    relhasindex: bool,
}

pub(super) fn pg_class_row(row: VirtualClassRow) -> Vec<Value> {
    vec![
        v_i64(row.oid),
        v_text(row.relname),
        Value::Int64(row.relnamespace),
        v_oid(row.reltype),
        v_text(row.relkind),
        Value::Int32(row.relpages),
        Value::Float64(0.0),
        Value::Int32(row.relfilenode),
        Value::Bool(row.relhasindex),
        Value::Int32(0),
        Value::Bool(false),
        Value::Bool(false),
        Value::Bool(false),
        Value::Bool(false),
        Value::Bool(false),
        Value::Int64(0),
        Value::Int64(0),
        v_text("p"),
        v_text("d"),
        Value::Int64(0),
        Value::Int64(2),
        Value::Null,
        Value::Int64(10),
        Value::Null,
        Value::Null,
    ]
}

pub(super) fn schema_pg_attribute() -> Schema {
    schema([
        Field::required("attrelid", DataType::Int64),
        Field::required("attname", text()),
        Field::required("atttypid", DataType::Oid),
        Field::required("attnum", DataType::Int16),
        Field::required("attnotnull", DataType::Bool),
        Field::required("atthasdef", DataType::Bool),
        Field::required("attisdropped", DataType::Bool),
        Field::required("atttypmod", DataType::Int32),
        Field::required("attcollation", DataType::Oid),
        Field::required("attidentity", DataType::Text { max_len: Some(1) }),
        Field::required("attgenerated", DataType::Text { max_len: Some(1) }),
        Field::nullable("attacl", text_array()),
        Field::nullable("attoptions", text_array()),
    ])
}

pub(super) fn rows_pg_attribute(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for entry in table_entries(ctx) {
        for (idx, field) in entry.schema.fields().iter().enumerate() {
            rows.push(vec![
                v_i64(entry.oid.raw()),
                v_text(field.name.clone()),
                v_oid_i32(type_oid(&field.data_type)),
                Value::Int16(i16::try_from(idx + 1).unwrap_or(i16::MAX)),
                Value::Bool(!field.nullable),
                Value::Bool(column_default_expr(ctx, entry.oid, idx).is_some()),
                Value::Bool(false),
                Value::Int32(type_modifier(&field.data_type)),
                v_oid(attribute_collation_oid(&entry, idx)),
                v_text(""),
                v_text(""),
                Value::Null,
                Value::Null,
            ]);
        }
    }
    let mut composites = ctx
        .catalog_snapshot
        .composite_types_by_oid
        .values()
        .collect::<Vec<_>>();
    composites.sort_by_key(|entry| entry.oid.raw());
    for entry in composites {
        for (idx, field) in entry.schema.fields().iter().enumerate() {
            rows.push(vec![
                v_i64(entry.oid.raw()),
                v_text(field.name.clone()),
                v_oid_i32(type_oid(&field.data_type)),
                Value::Int16(i16::try_from(idx + 1).unwrap_or(i16::MAX)),
                Value::Bool(!field.nullable),
                Value::Bool(false),
                Value::Bool(false),
                Value::Int32(type_modifier(&field.data_type)),
                v_oid(type_collation_oid(&field.data_type)),
                v_text(""),
                v_text(""),
                Value::Null,
                Value::Null,
            ]);
        }
    }
    rows
}

pub(super) fn schema_pg_attrdef() -> Schema {
    schema([
        Field::required("oid", DataType::Int64),
        Field::required("adrelid", DataType::Int64),
        Field::required("adnum", DataType::Int16),
        Field::required("adbin", text()),
    ])
}

pub(super) fn type_modifier(data_type: &DataType) -> i32 {
    match data_type {
        DataType::Decimal {
            precision: Some(precision),
            scale,
        } => numeric_type_modifier(*precision, scale.unwrap_or(0)).unwrap_or(-1),
        DataType::Char { len: Some(len) } => len
            .checked_add(4)
            .and_then(|typmod| i32::try_from(typmod).ok())
            .unwrap_or(-1),
        _ => -1,
    }
}

pub(super) fn numeric_type_modifier(precision: u32, scale: i32) -> Option<i32> {
    if !(0..=i32::from(u16::MAX)).contains(&scale) {
        return None;
    }
    let precision = i32::try_from(precision).ok()?;
    precision
        .checked_shl(16)?
        .checked_add(scale)?
        .checked_add(4)
}

pub(super) fn rows_pg_attrdef(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for entry in table_entries(ctx) {
        for idx in 0..entry.schema.fields().len() {
            let Some(expr) = column_default_expr(ctx, entry.oid, idx) else {
                continue;
            };
            let attnum = i16::try_from(idx + 1).unwrap_or(i16::MAX);
            rows.push(vec![
                Value::Int64(attrdef_oid(entry.oid, attnum)),
                v_i64(entry.oid.raw()),
                Value::Int16(attnum),
                v_text(expr),
            ]);
        }
    }
    rows
}

pub(super) fn attrdef_oid(relid: Oid, attnum: i16) -> i64 {
    i64::from(relid.raw())
        .saturating_mul(1_000)
        .saturating_add(i64::from(attnum))
}

