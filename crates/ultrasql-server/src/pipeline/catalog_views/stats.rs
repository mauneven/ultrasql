//! Statistics and lock-introspection scans: `pg_locks`, `pg_stat_activity`,
//! the per-table/index stat views, database/bgwriter/wal stats, and the
//! progress views.

use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_mvcc::{Visibility, XidStatusOracle, is_visible};
use ultrasql_txn::{LockMode, LockTag};

use crate::pipeline::LowerCtx;

use super::common::*;

pub(super) fn schema_pg_stat_statements() -> Schema {
    schema([
        Field::required("queryid", DataType::Int64),
        Field::required("query", text()),
        Field::required("calls", DataType::Int64),
        Field::required("total_exec_time", DataType::Float64),
        Field::required("min_exec_time", DataType::Float64),
        Field::required("max_exec_time", DataType::Float64),
        Field::required("rows", DataType::Int64),
        Field::required("errors", DataType::Int64),
        Field::required("plan_hash", DataType::Int64),
        Field::required("bind_param_count", DataType::Int32),
        Field::required("bind_params_redacted", DataType::Bool),
        Field::nullable("last_error", text()),
    ])
}

pub(super) fn rows_pg_stat_statements(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    ctx.workload_recorder
        .snapshot()
        .into_iter()
        .map(|stat| {
            vec![
                Value::Int64(u64_to_i64_saturating(stat.query_id)),
                v_text(stat.query),
                Value::Int64(u64_to_i64_saturating(stat.calls)),
                Value::Float64(duration_ms(stat.total_exec_time)),
                Value::Float64(duration_ms(stat.min_exec_time)),
                Value::Float64(duration_ms(stat.max_exec_time)),
                Value::Int64(u64_to_i64_saturating(stat.rows)),
                Value::Int64(u64_to_i64_saturating(stat.errors)),
                Value::Int64(u64_to_i64_saturating(stat.plan_hash)),
                Value::Int32(i32::try_from(stat.bind_param_count).unwrap_or(i32::MAX)),
                Value::Bool(stat.bind_params_redacted),
                stat.last_error.map_or(Value::Null, v_text),
            ]
        })
        .collect()
}

pub(super) fn duration_ms(duration: std::time::Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

pub(super) fn u64_to_i64_saturating(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

pub(super) fn schema_pg_locks() -> Schema {
    schema([
        Field::nullable("locktype", text()),
        Field::nullable("database", DataType::Int64),
        Field::nullable("relation", DataType::Int64),
        Field::nullable("page", DataType::Int32),
        Field::nullable("tuple", DataType::Int16),
        Field::nullable("virtualxid", text()),
        Field::nullable("transactionid", DataType::Int64),
        Field::nullable("classid", DataType::Int64),
        Field::nullable("objid", DataType::Int64),
        Field::nullable("objsubid", DataType::Int16),
        Field::nullable("virtualtransaction", text()),
        Field::nullable("pid", DataType::Int32),
        Field::nullable("mode", text()),
        Field::required("granted", DataType::Bool),
        Field::required("fastpath", DataType::Bool),
        Field::nullable("waitstart", DataType::TimestampTz),
    ])
}

pub(super) fn rows_pg_locks(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for (tag, snapshot) in ctx.oracle.lock_manager.snapshot() {
        for (xid, mode) in snapshot.grants {
            rows.push(pg_lock_row(tag, xid.raw(), mode, true));
        }
        for (xid, mode) in snapshot.waiters {
            rows.push(pg_lock_row(tag, xid.raw(), mode, false));
        }
    }
    rows
}

pub(super) fn pg_lock_row(tag: LockTag, owner_xid: u64, mode: LockMode, granted: bool) -> Vec<Value> {
    let pid = advisory_owner_pid(owner_xid)
        .map(Value::Int32)
        .unwrap_or(Value::Null);
    let virtualtransaction = v_text(format!("0/{owner_xid}"));
    let mode = v_text(lock_mode_name(mode));
    let granted = Value::Bool(granted);
    let fastpath = Value::Bool(false);
    let waitstart = Value::Null;
    match tag {
        LockTag::Relation(relation) => vec![
            v_text("relation"),
            Value::Int64(1),
            Value::Int64(i64::from(relation.oid().raw())),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            virtualtransaction,
            pid,
            mode,
            granted,
            fastpath,
            waitstart,
        ],
        LockTag::Tuple(tid) => vec![
            v_text("tuple"),
            Value::Int64(1),
            Value::Int64(i64::from(tid.page.relation.oid().raw())),
            Value::Int32(i32::try_from(tid.page.block.raw()).unwrap_or(i32::MAX)),
            Value::Int16(i16::try_from(tid.slot).unwrap_or(i16::MAX)),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            virtualtransaction,
            pid,
            mode,
            granted,
            fastpath,
            waitstart,
        ],
        LockTag::Advisory { classid, objid } => vec![
            v_text("advisory"),
            Value::Int64(1),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Int64(i64::from(classid)),
            Value::Int64(i64::from(objid)),
            Value::Int16(1),
            virtualtransaction,
            pid,
            mode,
            granted,
            fastpath,
            waitstart,
        ],
    }
}

pub(super) fn advisory_owner_pid(owner_xid: u64) -> Option<i32> {
    let pid = u64::MAX.checked_sub(owner_xid)?;
    i32::try_from(pid).ok()
}

pub(super) fn lock_mode_name(mode: LockMode) -> &'static str {
    match mode {
        LockMode::AccessShare => "AccessShareLock",
        LockMode::RowShare => "RowShareLock",
        LockMode::RowExclusive => "RowExclusiveLock",
        LockMode::ShareUpdateExclusive => "ShareUpdateExclusiveLock",
        LockMode::Share => "ShareLock",
        LockMode::ShareRowExclusive => "ShareRowExclusiveLock",
        LockMode::Exclusive => "ExclusiveLock",
        LockMode::AccessExclusive => "AccessExclusiveLock",
    }
}

pub(super) fn schema_pg_stat_activity() -> Schema {
    schema([
        Field::required("datid", DataType::Int64),
        Field::required("datname", text()),
        Field::required("pid", DataType::Int32),
        Field::required("usename", text()),
        Field::nullable("application_name", text()),
        Field::required("state", text()),
        Field::nullable("query", text()),
        Field::nullable("backend_start", DataType::TimestampTz),
        Field::nullable("xact_start", DataType::TimestampTz),
        Field::nullable("query_start", DataType::TimestampTz),
        Field::nullable("state_change", DataType::TimestampTz),
        Field::nullable("wait_event_type", text()),
        Field::nullable("wait_event", text()),
    ])
}

pub(super) fn rows_pg_stat_activity(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let sessions = ctx.workload_recorder.active_sessions();
    if !sessions.is_empty() {
        return sessions
            .into_iter()
            .map(|session| {
                vec![
                    Value::Int64(session.datid),
                    v_text(session.datname),
                    Value::Int32(session.pid),
                    v_text(session.usename),
                    session.application_name.map_or(Value::Null, v_text),
                    v_text(session.state),
                    session.query.map_or(Value::Null, v_text),
                    Value::TimestampTz(session.backend_start),
                    session.xact_start.map_or(Value::Null, Value::TimestampTz),
                    session.query_start.map_or(Value::Null, Value::TimestampTz),
                    Value::TimestampTz(session.state_change),
                    session.wait_event_type.map_or(Value::Null, v_text),
                    session.wait_event.map_or(Value::Null, v_text),
                ]
            })
            .collect();
    }

    let application_name = ctx
        .session_settings
        .get("application_name")
        .cloned()
        .map_or(Value::Null, v_text);
    vec![vec![
        Value::Int64(1),
        v_text("ultrasql"),
        Value::Int32(0),
        v_text(ctx.current_user.clone()),
        application_name,
        v_text("active"),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
    ]]
}

pub(super) fn schema_pg_stat_user_tables() -> Schema {
    schema([
        Field::required("relid", DataType::Int64),
        Field::required("schemaname", text()),
        Field::required("relname", text()),
        Field::required("seq_scan", DataType::Int64),
        Field::required("idx_scan", DataType::Int64),
        Field::required("n_live_tup", DataType::Int64),
        Field::required("n_dead_tup", DataType::Int64),
        Field::nullable("last_vacuum", DataType::TimestampTz),
        Field::nullable("last_autovacuum", DataType::TimestampTz),
        Field::nullable("last_analyze", DataType::TimestampTz),
        Field::nullable("last_autoanalyze", DataType::TimestampTz),
        Field::required("vacuum_count", DataType::Int64),
        Field::required("autovacuum_count", DataType::Int64),
        Field::required("analyze_count", DataType::Int64),
        Field::required("autoanalyze_count", DataType::Int64),
    ])
}

pub(super) fn rows_pg_stat_user_tables(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    table_entries(ctx)
        .into_iter()
        .filter(|entry| {
            entry.schema_name != "pg_catalog" && entry.schema_name != "information_schema"
        })
        .map(|entry| {
            let (live_tuples, dead_tuples) = table_tuple_counts(ctx, &entry);
            let maintenance = ctx
                .workload_recorder
                .table_maintenance_stats(entry.oid.raw());
            vec![
                v_i64(entry.oid.raw()),
                v_text(entry.schema_name),
                v_text(entry.name),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(live_tuples),
                Value::Int64(dead_tuples),
                maintenance
                    .last_vacuum
                    .map_or(Value::Null, Value::TimestampTz),
                maintenance
                    .last_autovacuum
                    .map_or(Value::Null, Value::TimestampTz),
                maintenance
                    .last_analyze
                    .map_or(Value::Null, Value::TimestampTz),
                maintenance
                    .last_autoanalyze
                    .map_or(Value::Null, Value::TimestampTz),
                Value::Int64(u64_to_i64_saturating(maintenance.vacuum_count)),
                Value::Int64(u64_to_i64_saturating(maintenance.autovacuum_count)),
                Value::Int64(u64_to_i64_saturating(maintenance.analyze_count)),
                Value::Int64(u64_to_i64_saturating(maintenance.autoanalyze_count)),
            ]
        })
        .collect()
}

pub(super) fn table_tuple_counts(ctx: &LowerCtx<'_>, entry: &ultrasql_catalog::TableEntry) -> (i64, i64) {
    let rel = ultrasql_core::RelationId(entry.oid);
    let block_count = ctx.heap.block_count(rel).max(entry.n_blocks);
    if block_count == 0 {
        return (0, 0);
    }

    let oldest = ctx.oracle.oldest_in_progress();
    let mut live = 0_u64;
    let mut dead = 0_u64;
    for tuple in ctx.heap.scan(rel, block_count) {
        let tuple = match tuple {
            Ok(tuple) => tuple,
            Err(err) => {
                tracing::warn!(
                    table = %entry.name,
                    error = %err,
                    "pg_stat_user_tables tuple-count scan failed",
                );
                return (0, 0);
            }
        };
        if matches!(
            is_visible(&tuple.header, &ctx.snapshot, ctx.oracle.as_ref()),
            Visibility::Visible | Visibility::VisiblePreImage
        ) {
            live = live.saturating_add(1);
        }
        let xmax = tuple.header.xmax;
        if !xmax.is_invalid() && xmax < oldest && ctx.oracle.is_committed(xmax) {
            dead = dead.saturating_add(1);
        }
    }
    (u64_to_i64_saturating(live), u64_to_i64_saturating(dead))
}

pub(super) fn schema_pg_stat_user_indexes() -> Schema {
    schema([
        Field::required("relid", DataType::Int64),
        Field::required("indexrelid", DataType::Int64),
        Field::required("schemaname", text()),
        Field::required("relname", text()),
        Field::required("indexrelname", text()),
        Field::required("idx_scan", DataType::Int64),
        Field::required("idx_tup_read", DataType::Int64),
        Field::required("idx_tup_fetch", DataType::Int64),
    ])
}

pub(super) fn rows_pg_stat_user_indexes(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut indexes: Vec<_> = ctx.catalog_snapshot.indexes.values().collect();
    indexes.sort_by(|a, b| a.name.cmp(&b.name));
    indexes
        .into_iter()
        .filter_map(|idx| {
            let table = ctx.catalog_snapshot.tables_by_oid.get(&idx.table_oid)?;
            let usage = ctx.workload_recorder.index_usage_for(idx.oid.raw());
            Some(vec![
                v_i64(table.oid.raw()),
                v_i64(idx.oid.raw()),
                v_text(idx.schema_name.clone()),
                v_text(table.name.clone()),
                v_text(idx.name.clone()),
                Value::Int64(u64_to_i64_saturating(usage.idx_scan)),
                Value::Int64(u64_to_i64_saturating(usage.idx_tup_read)),
                Value::Int64(u64_to_i64_saturating(usage.idx_tup_fetch)),
            ])
        })
        .collect()
}

pub(super) fn schema_pg_statio_user_tables() -> Schema {
    schema([
        Field::required("relid", DataType::Int64),
        Field::required("schemaname", text()),
        Field::required("relname", text()),
        Field::required("heap_blks_read", DataType::Int64),
        Field::required("heap_blks_hit", DataType::Int64),
        Field::required("idx_blks_read", DataType::Int64),
        Field::required("idx_blks_hit", DataType::Int64),
        Field::required("toast_blks_read", DataType::Int64),
        Field::required("toast_blks_hit", DataType::Int64),
        Field::required("tidx_blks_read", DataType::Int64),
        Field::required("tidx_blks_hit", DataType::Int64),
    ])
}

pub(super) fn rows_pg_statio_user_tables(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    table_entries(ctx)
        .into_iter()
        .filter(|entry| {
            entry.schema_name != "pg_catalog" && entry.schema_name != "information_schema"
        })
        .map(|entry| {
            let heap_io = ctx
                .heap
                .buffer_pool()
                .relation_stats(ultrasql_core::RelationId(entry.oid));
            vec![
                v_i64(entry.oid.raw()),
                v_text(entry.schema_name),
                v_text(entry.name),
                Value::Int64(u64_to_i64_saturating(heap_io.reads)),
                Value::Int64(u64_to_i64_saturating(heap_io.hits)),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
            ]
        })
        .collect()
}

pub(super) fn schema_pg_statio_user_indexes() -> Schema {
    schema([
        Field::required("relid", DataType::Int64),
        Field::required("indexrelid", DataType::Int64),
        Field::required("schemaname", text()),
        Field::required("relname", text()),
        Field::required("indexrelname", text()),
        Field::required("idx_blks_read", DataType::Int64),
        Field::required("idx_blks_hit", DataType::Int64),
    ])
}

pub(super) fn rows_pg_statio_user_indexes(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut indexes: Vec<_> = ctx.catalog_snapshot.indexes.values().collect();
    indexes.sort_by(|a, b| a.name.cmp(&b.name));
    indexes
        .into_iter()
        .filter_map(|idx| {
            let table = ctx.catalog_snapshot.tables_by_oid.get(&idx.table_oid)?;
            let index_io = ctx
                .heap
                .buffer_pool()
                .relation_stats(ultrasql_core::RelationId(idx.oid));
            Some(vec![
                v_i64(table.oid.raw()),
                v_i64(idx.oid.raw()),
                v_text(idx.schema_name.clone()),
                v_text(table.name.clone()),
                v_text(idx.name.clone()),
                Value::Int64(u64_to_i64_saturating(index_io.reads)),
                Value::Int64(u64_to_i64_saturating(index_io.hits)),
            ])
        })
        .collect()
}

pub(super) fn schema_pg_stat_database() -> Schema {
    schema([
        Field::required("datid", DataType::Int64),
        Field::required("datname", text()),
        Field::required("numbackends", DataType::Int32),
        Field::required("xact_commit", DataType::Int64),
        Field::required("xact_rollback", DataType::Int64),
        Field::required("deadlocks", DataType::Int64),
    ])
}

pub(super) fn rows_pg_stat_database(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut calls = 0_u64;
    let mut errors = 0_u64;
    for stat in ctx.workload_recorder.snapshot() {
        calls = calls.saturating_add(stat.calls);
        errors = errors.saturating_add(stat.errors);
    }
    let commits = calls.saturating_sub(errors);
    vec![vec![
        Value::Int64(1),
        v_text("ultrasql"),
        Value::Int32(1),
        Value::Int64(u64_to_i64_saturating(commits)),
        Value::Int64(u64_to_i64_saturating(errors)),
        Value::Int64(0),
    ]]
}

pub(super) fn schema_pg_stat_bgwriter() -> Schema {
    schema([
        Field::required("checkpoints_timed", DataType::Int64),
        Field::required("checkpoints_req", DataType::Int64),
        Field::required("checkpoint_write_time", DataType::Float64),
        Field::required("checkpoint_sync_time", DataType::Float64),
        Field::required("buffers_checkpoint", DataType::Int64),
        Field::required("buffers_clean", DataType::Int64),
        Field::required("maxwritten_clean", DataType::Int64),
        Field::required("buffers_backend", DataType::Int64),
        Field::required("buffers_alloc", DataType::Int64),
    ])
}

pub(super) fn rows_pg_stat_bgwriter(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let pool = ctx.heap.buffer_pool().stats();
    vec![vec![
        Value::Int64(0),
        Value::Int64(0),
        Value::Float64(0.0),
        Value::Float64(0.0),
        Value::Int64(0),
        Value::Int64(u64_to_i64_saturating(pool.evictions)),
        Value::Int64(0),
        Value::Int64(u64_to_i64_saturating(pool.gets)),
        Value::Int64(u64_to_i64_saturating(pool.misses)),
    ]]
}

pub(super) fn schema_pg_stat_wal() -> Schema {
    schema([
        Field::required("wal_records", DataType::Int64),
        Field::required("wal_fpi", DataType::Int64),
        Field::required("wal_bytes", DataType::Int64),
        Field::required("wal_sync", DataType::Int64),
        Field::required("wal_write", DataType::Int64),
    ])
}

pub(super) fn rows_pg_stat_wal(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let stats = ctx
        .heap
        .wal_sink()
        .map(|sink| sink.stats())
        .unwrap_or_default();
    vec![vec![
        Value::Int64(u64_to_i64_saturating(stats.wal_records)),
        Value::Int64(u64_to_i64_saturating(stats.wal_fpi)),
        Value::Int64(u64_to_i64_saturating(stats.wal_bytes)),
        Value::Int64(0),
        Value::Int64(u64_to_i64_saturating(stats.wal_write)),
    ]]
}

pub(super) fn schema_pg_stat_progress_vacuum() -> Schema {
    schema([
        Field::required("pid", DataType::Int32),
        Field::required("datid", DataType::Int64),
        Field::required("datname", text()),
        Field::required("relid", DataType::Int64),
        Field::required("phase", text()),
        Field::required("heap_blks_total", DataType::Int64),
        Field::required("heap_blks_scanned", DataType::Int64),
        Field::required("heap_blks_vacuumed", DataType::Int64),
    ])
}

pub(super) fn rows_pg_stat_progress_vacuum(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    ctx.workload_recorder
        .vacuum_progress()
        .into_iter()
        .map(|row| {
            vec![
                Value::Int32(row.pid),
                Value::Int64(row.datid),
                v_text(row.datname),
                Value::Int64(row.relid),
                v_text(row.phase),
                Value::Int64(row.heap_blks_total),
                Value::Int64(row.heap_blks_scanned),
                Value::Int64(row.heap_blks_vacuumed),
            ]
        })
        .collect()
}

pub(super) fn schema_pg_stat_progress_analyze() -> Schema {
    schema([
        Field::required("pid", DataType::Int32),
        Field::required("datid", DataType::Int64),
        Field::required("datname", text()),
        Field::required("relid", DataType::Int64),
        Field::required("phase", text()),
        Field::required("sample_blks_total", DataType::Int64),
        Field::required("sample_blks_scanned", DataType::Int64),
    ])
}

pub(super) fn rows_pg_stat_progress_analyze(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    ctx.workload_recorder
        .analyze_progress()
        .into_iter()
        .map(|row| {
            vec![
                Value::Int32(row.pid),
                Value::Int64(row.datid),
                v_text(row.datname),
                Value::Int64(row.relid),
                v_text(row.phase),
                Value::Int64(row.sample_blks_total),
                Value::Int64(row.sample_blks_scanned),
            ]
        })
        .collect()
}

pub(super) fn schema_pg_stat_progress_create_index() -> Schema {
    schema([
        Field::required("pid", DataType::Int32),
        Field::required("datid", DataType::Int64),
        Field::required("datname", text()),
        Field::required("relid", DataType::Int64),
        Field::required("index_relid", DataType::Int64),
        Field::required("phase", text()),
        Field::required("blocks_total", DataType::Int64),
        Field::required("blocks_done", DataType::Int64),
    ])
}

pub(super) fn rows_pg_stat_progress_create_index(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    ctx.workload_recorder
        .create_index_progress()
        .into_iter()
        .map(|row| {
            vec![
                Value::Int32(row.pid),
                Value::Int64(row.datid),
                v_text(row.datname),
                Value::Int64(row.relid),
                Value::Int64(row.index_relid),
                v_text(row.phase),
                Value::Int64(row.blocks_total),
                Value::Int64(row.blocks_done),
            ]
        })
        .collect()
}

