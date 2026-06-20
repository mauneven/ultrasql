//! Catalog object scans: `pg_sequence`, `pg_operator`, `pg_depend`,
//! `pg_description`, `pg_statistic[_ext]`, and the `pg_tables`/`pg_indexes`/
//! `pg_views`/`pg_matviews`/`pg_sequences` listing views.

use ultrasql_core::{DataType, Field, Oid, Schema, Value};

use crate::pipeline::LowerCtx;

use super::common::*;
use super::indexes::virtual_constraints;
use super::pgproc::{pg_proc_oid_by_name, pg_type_oid_for_data_type};

pub(super) fn schema_pg_sequence() -> Schema {
    schema([
        Field::required("seqrelid", DataType::Int64),
        Field::required("seqtypid", DataType::Int32),
        Field::required("seqstart", DataType::Int64),
        Field::required("seqincrement", DataType::Int64),
        Field::required("seqmax", DataType::Int64),
        Field::required("seqmin", DataType::Int64),
        Field::required("seqcache", DataType::Int64),
        Field::required("seqcycle", DataType::Bool),
    ])
}

pub(super) fn rows_pg_sequence(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    let mut seqs: Vec<_> = ctx
        .sequences
        .iter()
        .map(|e| (e.key().clone(), e.value().clone()))
        .collect();
    seqs.sort_by(|a, b| a.0.cmp(&b.0));
    for (idx, (_name, seq)) in seqs.into_iter().enumerate() {
        let opts = seq.options_snapshot();
        rows.push(vec![
            Value::Int64(20_000 + i64::try_from(idx).unwrap_or(i64::MAX)),
            Value::Int32(PG_TYPE_INT8),
            Value::Int64(opts.start),
            Value::Int64(opts.increment),
            Value::Int64(seq.max_value()),
            Value::Int64(seq.min_value()),
            Value::Int64(i64::from(opts.cache)),
            Value::Bool(opts.cycle),
        ]);
    }
    rows
}

pub(super) fn schema_pg_operator() -> Schema {
    schema([
        Field::required("oid", DataType::Oid),
        Field::required("oprname", text()),
        Field::required("oprnamespace", DataType::Oid),
        Field::required("oprowner", DataType::Oid),
        Field::required("oprkind", DataType::Text { max_len: Some(1) }),
        Field::required("oprcanmerge", DataType::Bool),
        Field::required("oprcanhash", DataType::Bool),
        Field::required("oprleft", DataType::Oid),
        Field::required("oprright", DataType::Oid),
        Field::required("oprresult", DataType::Oid),
        Field::required("oprcom", DataType::Oid),
        Field::required("oprnegate", DataType::Oid),
        Field::required("oprcode", DataType::Oid),
        Field::required("oprrest", DataType::Oid),
        Field::required("oprjoin", DataType::Oid),
    ])
}

pub(super) fn rows_pg_operator(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut operators: Vec<crate::RuntimeOperator> = ctx
        .operators
        .iter()
        .map(|entry| entry.value().as_ref().clone())
        .collect();
    operators.sort_by(|a, b| a.oid.cmp(&b.oid).then_with(|| a.name.cmp(&b.name)));
    operators
        .into_iter()
        .map(|operator| {
            let kind = match (&operator.left_type, &operator.right_type) {
                (Some(_), Some(_)) => "b",
                (None, Some(_)) => "l",
                (Some(_), None) => "r",
                (None, None) => "b",
            };
            vec![
                Value::Oid(Oid::new(operator.oid)),
                v_text(&operator.name),
                Value::Oid(Oid::new(namespace_oid_u32(&operator.namespace))),
                Value::Oid(Oid::new(10)),
                v_text(kind),
                Value::Bool(false),
                Value::Bool(false),
                Value::Oid(Oid::new(
                    operator
                        .left_type
                        .as_ref()
                        .map_or(0, pg_type_oid_for_data_type),
                )),
                Value::Oid(Oid::new(
                    operator
                        .right_type
                        .as_ref()
                        .map_or(0, pg_type_oid_for_data_type),
                )),
                Value::Oid(Oid::new(pg_type_oid_for_data_type(&operator.result_type))),
                Value::Oid(Oid::new(0)),
                Value::Oid(Oid::new(0)),
                Value::Oid(Oid::new(
                    pg_proc_oid_by_name(&operator.procedure).unwrap_or(0),
                )),
                Value::Oid(Oid::new(0)),
                Value::Oid(Oid::new(0)),
            ]
        })
        .collect()
}

pub(super) fn schema_pg_depend() -> Schema {
    schema([
        Field::required("classid", DataType::Int64),
        Field::required("objid", DataType::Int64),
        Field::required("objsubid", DataType::Int32),
        Field::required("refclassid", DataType::Int64),
        Field::required("refobjid", DataType::Int64),
        Field::required("refobjsubid", DataType::Int32),
        Field::required("deptype", DataType::Text { max_len: Some(1) }),
    ])
}

pub(super) fn rows_pg_depend(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for c in virtual_constraints(ctx) {
        rows.push(vec![
            Value::Int64(PG_CONSTRAINT_OID),
            Value::Int64(c.oid),
            Value::Int32(0),
            Value::Int64(PG_CLASS_OID),
            v_i64(c.table_oid.raw()),
            Value::Int32(0),
            v_text("a"),
        ]);
        if let Some(foreign_table_oid) = c.foreign_table_oid {
            rows.push(vec![
                Value::Int64(PG_CONSTRAINT_OID),
                Value::Int64(c.oid),
                Value::Int32(0),
                Value::Int64(PG_CLASS_OID),
                v_i64(foreign_table_oid.raw()),
                Value::Int32(0),
                v_text("n"),
            ]);
        }
    }
    rows.sort_by(|a, b| {
        let key = |row: &[Value]| match (&row[1], &row[4], &row[6]) {
            (Value::Int64(objid), Value::Int64(refobjid), Value::Text(deptype)) => {
                (*objid, *refobjid, deptype.clone())
            }
            _ => (0, 0, String::new()),
        };
        key(a).cmp(&key(b))
    });
    rows
}

pub(super) fn schema_pg_description() -> Schema {
    schema([
        Field::required("objoid", DataType::Int64),
        Field::required("classoid", DataType::Int64),
        Field::required("objsubid", DataType::Int32),
        Field::required("description", text()),
    ])
}

pub(super) fn rows_pg_description(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut descriptions: Vec<_> = ctx
        .catalog_snapshot
        .descriptions
        .values()
        .cloned()
        .collect();
    descriptions.sort_by_key(|row| (row.objoid.raw(), row.objsubid, row.description.clone()));
    descriptions
        .into_iter()
        .map(|row| {
            vec![
                v_i64(row.objoid.raw()),
                v_i64(row.classoid.raw()),
                Value::Int32(row.objsubid),
                v_text(row.description),
            ]
        })
        .collect()
}

pub(super) fn schema_pg_statistic() -> Schema {
    schema([
        Field::required("starelid", DataType::Int64),
        Field::required("staattnum", DataType::Int16),
        Field::required("stanullfrac", DataType::Float32),
        Field::required("stadistinct", DataType::Float32),
    ])
}

pub(super) fn rows_pg_statistic(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows: Vec<_> = ctx.catalog_snapshot.statistics.values().cloned().collect();
    rows.sort_by_key(|row| (row.starelid.raw(), row.staattnum));
    rows.into_iter()
        .map(|row| {
            vec![
                v_i64(row.starelid.raw()),
                Value::Int16(row.staattnum),
                Value::Float32(row.stanullfrac),
                Value::Float32(row.stadistinct),
            ]
        })
        .collect()
}

pub(super) fn schema_pg_statistic_ext() -> Schema {
    schema([
        Field::required("oid", DataType::Int64),
        Field::required("stxname", text()),
        Field::required("stxrelid", DataType::Int64),
        Field::required("stxnamespace", DataType::Int64),
        Field::required("stxkeys", text()),
        Field::required(
            "stxkind",
            DataType::Array(Box::new(DataType::Text { max_len: None })),
        ),
        Field::required("stxstattarget", DataType::Int32),
    ])
}

pub(super) fn rows_pg_statistic_ext(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows: Vec<_> = ctx
        .catalog_snapshot
        .statistic_ext
        .values()
        .cloned()
        .collect();
    rows.sort_by(|a, b| a.stxname.cmp(&b.stxname));
    rows.into_iter()
        .map(|row| {
            let stxkeys = row
                .stxkeys
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(" ");
            let stxkind = row.stxkind.into_iter().collect::<String>();
            vec![
                v_i64(row.oid.raw()),
                v_text(row.stxname),
                v_i64(row.stxrelid.raw()),
                Value::Int64(namespace_oid("public")),
                v_text(stxkeys),
                Value::Array {
                    element_type: DataType::Text { max_len: None },
                    elements: stxkind
                        .chars()
                        .map(|kind| v_text(kind.to_string()))
                        .collect(),
                },
                Value::Int32(-1),
            ]
        })
        .collect()
}

pub(super) fn schema_pg_tables() -> Schema {
    schema([
        Field::required("schemaname", text()),
        Field::required("tablename", text()),
        Field::required("tableowner", text()),
        Field::nullable("tablespace", text()),
        Field::required("hasindexes", DataType::Bool),
        Field::required("hasrules", DataType::Bool),
        Field::required("hastriggers", DataType::Bool),
        Field::required("rowsecurity", DataType::Bool),
    ])
}

pub(super) fn rows_pg_tables(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    table_entries(ctx)
        .into_iter()
        .filter(|entry| {
            entry.schema_name != "pg_catalog"
                && entry.schema_name != "information_schema"
                && !is_materialized_view_entry(entry)
                && !is_regular_view_entry(entry)
        })
        .map(|entry| {
            vec![
                v_text(entry.schema_name.clone()),
                v_text(entry.name.clone()),
                v_text("ultrasql"),
                Value::Null,
                Value::Bool(
                    ctx.catalog_snapshot
                        .indexes_by_table
                        .contains_key(&entry.oid),
                ),
                Value::Bool(false),
                Value::Bool(false),
                Value::Bool(false),
            ]
        })
        .collect()
}

pub(super) fn schema_pg_indexes() -> Schema {
    schema([
        Field::required("schemaname", text()),
        Field::required("tablename", text()),
        Field::required("indexname", text()),
        Field::nullable("tablespace", text()),
        Field::nullable("indexdef", text()),
    ])
}

pub(super) fn rows_pg_indexes(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    let mut indexes: Vec<_> = ctx.catalog_snapshot.indexes.values().collect();
    indexes.sort_by(|a, b| a.name.cmp(&b.name));
    for idx in indexes {
        let table = ctx.catalog_snapshot.tables_by_oid.get(&idx.table_oid);
        let table_name = table
            .map(|entry| entry.name.clone())
            .unwrap_or_else(|| idx.table_oid.raw().to_string());
        rows.push(vec![
            v_text(idx.schema_name.clone()),
            v_text(table_name),
            v_text(idx.name.clone()),
            Value::Null,
            Value::Null,
        ]);
    }
    rows
}

pub(super) fn schema_pg_views() -> Schema {
    schema([
        Field::required("schemaname", text()),
        Field::required("viewname", text()),
        Field::required("viewowner", text()),
        Field::nullable("definition", text()),
    ])
}

pub(super) fn rows_pg_views(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    table_entries(ctx)
        .into_iter()
        .filter(is_regular_view_entry)
        .map(|entry| {
            vec![
                v_text(entry.schema_name.clone()),
                v_text(entry.name.clone()),
                v_text("ultrasql"),
                Value::Null,
            ]
        })
        .collect()
}

pub(super) fn schema_pg_matviews() -> Schema {
    schema([
        Field::required("schemaname", text()),
        Field::required("matviewname", text()),
        Field::required("matviewowner", text()),
        Field::nullable("tablespace", text()),
        Field::required("hasindexes", DataType::Bool),
        Field::required("ispopulated", DataType::Bool),
        Field::nullable("definition", text()),
    ])
}

pub(super) fn rows_pg_matviews(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    table_entries(ctx)
        .into_iter()
        .filter(is_materialized_view_entry)
        .map(|entry| {
            vec![
                v_text(entry.schema_name.clone()),
                v_text(entry.name.clone()),
                v_text("ultrasql"),
                Value::Null,
                Value::Bool(
                    ctx.catalog_snapshot
                        .indexes_by_table
                        .contains_key(&entry.oid),
                ),
                Value::Bool(true),
                Value::Null,
            ]
        })
        .collect()
}

pub(super) fn schema_pg_sequences() -> Schema {
    schema([
        Field::required("schemaname", text()),
        Field::required("sequencename", text()),
        Field::required("sequenceowner", text()),
        Field::required("data_type", text()),
        Field::required("start_value", DataType::Int64),
        Field::required("min_value", DataType::Int64),
        Field::required("max_value", DataType::Int64),
        Field::required("increment_by", DataType::Int64),
        Field::required("cycle", DataType::Bool),
        Field::required("cache_size", DataType::Int64),
        Field::nullable("last_value", DataType::Int64),
    ])
}

pub(super) fn rows_pg_sequences(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut seqs: Vec<_> = ctx
        .sequences
        .iter()
        .map(|e| (e.key().clone(), e.value().clone()))
        .collect();
    seqs.sort_by(|a, b| a.0.cmp(&b.0));
    seqs.into_iter()
        .map(|(name, seq)| {
            let opts = seq.options_snapshot();
            let owner = ctx
                .sequence_owners
                .get(&name)
                .map_or_else(|| "ultrasql".to_owned(), |entry| entry.value().clone());
            let namespace = ctx
                .sequence_namespaces
                .get(&name)
                .map_or_else(|| "public".to_owned(), |entry| entry.value().clone());
            let display_name = sequence_display_name(&name, &namespace);
            vec![
                v_text(namespace),
                v_text(display_name),
                v_text(owner),
                v_text("bigint"),
                Value::Int64(opts.start),
                Value::Int64(seq.min_value()),
                Value::Int64(seq.max_value()),
                Value::Int64(opts.increment),
                Value::Bool(opts.cycle),
                Value::Int64(i64::from(opts.cache)),
                Value::Null,
            ]
        })
        .collect()
}

pub(super) fn sequence_display_name(key: &str, namespace: &str) -> String {
    let namespace = namespace.to_ascii_lowercase();
    if let Some((schema, relation)) = ultrasql_catalog::decode_table_lookup_key(key)
        && schema.eq_ignore_ascii_case(&namespace)
    {
        return relation.to_owned();
    }
    let prefix = format!("{namespace}.");
    let visible = key.strip_prefix(&prefix).unwrap_or(key);
    visible
        .rsplit_once('.')
        .map_or_else(|| visible.to_owned(), |(_, name)| name.to_owned())
}

