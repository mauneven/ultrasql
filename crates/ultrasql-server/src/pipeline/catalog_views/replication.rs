//! Replication and publication scans: `pg_replication_slots`,
//! `pg_stat_replication`, `pg_stat_subscription`, `pg_publication`,
//! `pg_subscription`, `pg_publication_rel`, and `pg_publication_tables`.

use std::collections::HashMap;

use ultrasql_core::{DataType, Field, Schema, Value};

use crate::pipeline::LowerCtx;

use super::common::*;

pub(super) fn schema_pg_replication_slots() -> Schema {
    schema([
        Field::required("slot_name", text()),
        Field::required("plugin", text()),
        Field::required("slot_type", text()),
        Field::required("datoid", DataType::Int64),
        Field::required("database", text()),
        Field::required("active", DataType::Bool),
        Field::nullable("restart_lsn", text()),
        Field::nullable("confirmed_flush_lsn", text()),
    ])
}

pub(super) fn rows_pg_replication_slots(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let Some(data_dir) = &ctx.data_dir else {
        return Vec::new();
    };
    let Ok(store) = crate::replication::ReplicationSlotStore::open(data_dir.join("pg_replslot"))
    else {
        return Vec::new();
    };
    let Ok(slots) = store.list() else {
        return Vec::new();
    };
    slots
        .into_iter()
        .map(|slot| {
            vec![
                v_text(slot.name),
                v_text(""),
                v_text("physical"),
                Value::Int64(1),
                v_text("ultrasql"),
                Value::Bool(false),
                slot.restart_lsn.map_or(Value::Null, v_text),
                slot.confirmed_flush_lsn.map_or(Value::Null, v_text),
            ]
        })
        .collect()
}

pub(super) fn schema_pg_stat_replication() -> Schema {
    schema([
        Field::required("pid", DataType::Int32),
        Field::required("usename", text()),
        Field::required("application_name", text()),
        Field::required("client_addr", text()),
        Field::required("state", text()),
        Field::nullable("sent_lsn", text()),
        Field::nullable("write_lsn", text()),
        Field::nullable("flush_lsn", text()),
        Field::nullable("replay_lsn", text()),
        Field::required("sync_state", text()),
    ])
}

pub(super) fn rows_pg_stat_replication(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let Some(data_dir) = &ctx.data_dir else {
        return Vec::new();
    };
    let Ok(store) = crate::replication::ReplicationSlotStore::open(data_dir.join("pg_replslot"))
    else {
        return Vec::new();
    };
    let Ok(slots) = store.list() else {
        return Vec::new();
    };

    slots
        .into_iter()
        .map(|slot| {
            let sent_lsn = slot.restart_lsn.clone().map_or(Value::Null, v_text);
            let confirmed_flush_lsn = slot.confirmed_flush_lsn.clone();
            let write_lsn = confirmed_flush_lsn.clone().map_or(Value::Null, v_text);
            let flush_lsn = confirmed_flush_lsn.clone().map_or(Value::Null, v_text);
            let replay_lsn = confirmed_flush_lsn.map_or(Value::Null, v_text);
            let state = match (&slot.restart_lsn, &slot.confirmed_flush_lsn) {
                (Some(_), Some(_)) => "streaming",
                (Some(_), None) => "catchup",
                (None, _) => "startup",
            };

            vec![
                Value::Int32(0),
                v_text("ultrasql"),
                v_text(slot.name),
                v_text(""),
                v_text(state),
                sent_lsn,
                write_lsn,
                flush_lsn,
                replay_lsn,
                v_text("async"),
            ]
        })
        .collect()
}

pub(super) fn schema_pg_stat_subscription() -> Schema {
    schema([
        Field::required("subid", DataType::Int64),
        Field::required("subname", text()),
        Field::required("pid", DataType::Int32),
        Field::required("relid", DataType::Int64),
        Field::nullable("received_lsn", text()),
        Field::nullable("latest_end_lsn", text()),
    ])
}

pub(super) fn rows_pg_stat_subscription(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    ctx.logical_replication
        .subscriptions()
        .into_iter()
        .enumerate()
        .map(|(idx, subscription)| {
            vec![
                Value::Int64(subscription_oid(idx)),
                v_text(subscription.name),
                Value::Int32(0),
                Value::Int64(0),
                Value::Null,
                Value::Null,
            ]
        })
        .collect()
}

pub(super) fn schema_pg_publication() -> Schema {
    schema([
        Field::required("oid", DataType::Int64),
        Field::required("pubname", text()),
        Field::required("pubowner", DataType::Int64),
        Field::required("puballtables", DataType::Bool),
        Field::required("pubinsert", DataType::Bool),
        Field::required("pubupdate", DataType::Bool),
        Field::required("pubdelete", DataType::Bool),
        Field::required("pubtruncate", DataType::Bool),
    ])
}

pub(super) fn rows_pg_publication(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    ctx.logical_replication
        .publications()
        .into_iter()
        .enumerate()
        .map(|(idx, publication)| {
            vec![
                Value::Int64(publication_oid(idx)),
                v_text(publication.name),
                Value::Int64(10),
                Value::Bool(false),
                Value::Bool(true),
                Value::Bool(true),
                Value::Bool(true),
                Value::Bool(false),
            ]
        })
        .collect()
}

pub(super) fn publication_oid(idx: usize) -> i64 {
    90_000 + i64::try_from(idx).unwrap_or(i64::MAX)
}

pub(super) fn schema_pg_subscription() -> Schema {
    schema([
        Field::required("oid", DataType::Int64),
        Field::required("subdbid", DataType::Int64),
        Field::required("subname", text()),
        Field::required("subowner", DataType::Int64),
        Field::required("subenabled", DataType::Bool),
        Field::nullable("subconninfo", text()),
        Field::required("subslotname", text()),
        Field::required("subpublications", text()),
    ])
}

pub(super) fn rows_pg_subscription(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    ctx.logical_replication
        .subscriptions()
        .into_iter()
        .enumerate()
        .map(|(idx, subscription)| {
            vec![
                Value::Int64(subscription_oid(idx)),
                Value::Int64(1),
                v_text(subscription.name),
                Value::Int64(10),
                Value::Bool(subscription.enabled),
                v_text(subscription.conninfo),
                v_text(subscription.slot_name),
                v_text(subscription.publications.join(",")),
            ]
        })
        .collect()
}

pub(super) fn subscription_oid(idx: usize) -> i64 {
    91_000 + i64::try_from(idx).unwrap_or(i64::MAX)
}

pub(super) fn schema_pg_publication_rel() -> Schema {
    schema([
        Field::required("prpubid", DataType::Int64),
        Field::required("prrelid", DataType::Int64),
    ])
}

pub(super) fn rows_pg_publication_rel(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let public_table_oids = table_entries(ctx)
        .into_iter()
        .filter(|entry| entry.schema_name == "public")
        .map(|entry| (entry.name, i64::from(entry.oid.raw())))
        .collect::<HashMap<_, _>>();

    ctx.logical_replication
        .publications()
        .into_iter()
        .enumerate()
        .flat_map(|(idx, publication)| {
            let prpubid = publication_oid(idx);
            let table_oids = publication
                .tables()
                .filter_map(|table| public_table_oids.get(table).copied())
                .collect::<Vec<_>>();
            table_oids
                .into_iter()
                .map(move |prrelid| vec![Value::Int64(prpubid), Value::Int64(prrelid)])
        })
        .collect()
}

pub(super) fn schema_pg_publication_tables() -> Schema {
    schema([
        Field::required("pubname", text()),
        Field::required("schemaname", text()),
        Field::required("tablename", text()),
        Field::nullable("attnames", text()),
        Field::nullable("rowfilter", text()),
    ])
}

pub(super) fn rows_pg_publication_tables(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    ctx.logical_replication
        .publications()
        .into_iter()
        .flat_map(|publication| {
            let pubname = publication.name.clone();
            let tables = publication.tables().map(str::to_owned).collect::<Vec<_>>();
            tables.into_iter().map(move |table| {
                vec![
                    v_text(pubname.clone()),
                    v_text("public"),
                    v_text(table),
                    Value::Null,
                    Value::Null,
                ]
            })
        })
        .collect()
}

