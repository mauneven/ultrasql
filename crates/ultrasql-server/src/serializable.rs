//! Serializable isolation conflict plumbing.
//!
//! This module maps bound logical plans into SSI predicate-lock tags. The
//! supported precise subset uses scalar column ranges; unsupported or unsafe
//! shapes fall back to relation-wide tags so correctness stays conservative.

use ultrasql_catalog::{CatalogSnapshot, TableEntry};
use ultrasql_core::{DataType, RelationId};
use ultrasql_planner::{LogicalPlan, ScalarExpr};
use ultrasql_txn::{IsolationLevel, PredicateLockTag, Transaction, TransactionManager};

use crate::pipeline;

const SERIALIZABLE_POINT_LOCK_LIMIT: usize = 1024;

pub(crate) fn record_serializable_predicate_locks(
    plan: &LogicalPlan,
    txn: &Transaction,
    catalog_snapshot: &CatalogSnapshot,
    oracle: &TransactionManager,
) {
    if txn.isolation != IsolationLevel::Serializable {
        return;
    }
    oracle.register_serializable(txn.xid);
    let mut tags = Vec::new();
    collect_serializable_read_locks(plan, catalog_snapshot, &mut tags);
    dedup_predicate_tags(&mut tags);
    for tag in tags {
        oracle.record_predicate_lock(txn.xid, tag);
    }
}

fn collect_serializable_read_locks(
    plan: &LogicalPlan,
    catalog_snapshot: &CatalogSnapshot,
    out: &mut Vec<PredicateLockTag>,
) {
    match plan {
        LogicalPlan::Scan { table, .. } => {
            if let Some(entry) = catalog_snapshot.tables.get(table) {
                out.push(relation_predicate_tag(entry));
            }
        }
        LogicalPlan::Filter { input, predicate } => {
            if let Some(tag) = column_range_tag_for_filter(input, predicate, catalog_snapshot) {
                out.push(tag);
            } else {
                collect_serializable_read_locks(input, catalog_snapshot, out);
            }
        }
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::LockRows { input, .. } => {
            collect_serializable_read_locks(input, catalog_snapshot, out);
        }
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOp { left, right, .. } => {
            collect_serializable_read_locks(left, catalog_snapshot, out);
            collect_serializable_read_locks(right, catalog_snapshot, out);
        }
        LogicalPlan::Insert { source, .. } => {
            collect_serializable_read_locks(source, catalog_snapshot, out);
        }
        LogicalPlan::Update { input, .. } | LogicalPlan::Delete { input, .. } => {
            collect_serializable_read_locks(input, catalog_snapshot, out);
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => {
            collect_serializable_read_locks(definition, catalog_snapshot, out);
            collect_serializable_read_locks(body, catalog_snapshot, out);
        }
        _ => {}
    }
}

pub(crate) fn record_serializable_write_conflicts(
    plan: &LogicalPlan,
    txn: &Transaction,
    catalog_snapshot: &CatalogSnapshot,
    oracle: &TransactionManager,
) {
    if txn.isolation != IsolationLevel::Serializable {
        return;
    }
    oracle.register_serializable(txn.xid);
    for tag in serializable_write_conflict_tags(plan, catalog_snapshot) {
        let readers = oracle.record_write_conflicts(txn.xid, &tag);
        if readers.is_empty() {
            continue;
        }
        tracing::debug!(
            writer = ?txn.xid,
            tag = ?tag,
            readers = ?readers,
            "SSI recorded write conflicts",
        );
    }
}

fn serializable_write_conflict_tags(
    plan: &LogicalPlan,
    catalog_snapshot: &CatalogSnapshot,
) -> Vec<PredicateLockTag> {
    match plan {
        LogicalPlan::Insert {
            table,
            columns,
            source,
            ..
        } => catalog_snapshot
            .tables
            .get(table)
            .map_or_else(Vec::new, |entry| {
                insert_write_conflict_tags(entry, columns, source)
            }),
        LogicalPlan::Update {
            table,
            assignments,
            input,
            ..
        } => catalog_snapshot
            .tables
            .get(table)
            .map_or_else(Vec::new, |entry| {
                update_write_conflict_tags(entry, assignments, input, catalog_snapshot)
            }),
        LogicalPlan::Delete { table, input, .. } => catalog_snapshot
            .tables
            .get(table)
            .map_or_else(Vec::new, |entry| {
                delete_write_conflict_tags(entry, input, catalog_snapshot)
            }),
        _ => Vec::new(),
    }
}

fn insert_write_conflict_tags(
    entry: &TableEntry,
    columns: &[usize],
    source: &LogicalPlan,
) -> Vec<PredicateLockTag> {
    let LogicalPlan::Values { rows, .. } = source else {
        return table_column_range_write_tags(entry);
    };
    let field_count = entry.schema.fields().len();
    if columns.is_empty() && rows.iter().any(|row| row.len() != field_count) {
        return table_column_range_write_tags(entry);
    }
    if !columns.is_empty() && columns.len() != field_count {
        return table_column_range_write_tags(entry);
    }

    let mut tags = Vec::new();
    for row in rows {
        for (source_idx, expr) in row.iter().enumerate() {
            let target_idx = columns.get(source_idx).copied().unwrap_or(source_idx);
            if !column_supports_range_lock(entry, target_idx) {
                continue;
            }
            if let Some(key) = pipeline::literal_as_i64(expr) {
                if let Some(tag) = column_range_tag(entry, target_idx, Some(key), Some(key)) {
                    tags.push(tag);
                }
            } else if let Some(tag) = column_range_tag(entry, target_idx, None, None) {
                tags.push(tag);
            }
            if tags.len() > SERIALIZABLE_POINT_LOCK_LIMIT {
                return table_column_range_write_tags(entry);
            }
        }
    }
    finish_write_tags(entry, tags)
}

fn update_write_conflict_tags(
    entry: &TableEntry,
    assignments: &[(usize, ScalarExpr)],
    input: &LogicalPlan,
    catalog_snapshot: &CatalogSnapshot,
) -> Vec<PredicateLockTag> {
    let mut tags = input_write_conflict_tags(entry, input, catalog_snapshot)
        .unwrap_or_else(|| table_column_range_write_tags(entry));
    for (column, _expr) in assignments {
        if !column_supports_range_lock(entry, *column) {
            continue;
        }
        if let Some(tag) = column_range_tag(entry, *column, None, None) {
            tags.push(tag);
        }
    }
    finish_write_tags(entry, tags)
}

fn delete_write_conflict_tags(
    entry: &TableEntry,
    input: &LogicalPlan,
    catalog_snapshot: &CatalogSnapshot,
) -> Vec<PredicateLockTag> {
    let tags = input_write_conflict_tags(entry, input, catalog_snapshot)
        .unwrap_or_else(|| table_column_range_write_tags(entry));
    finish_write_tags(entry, tags)
}

fn input_write_conflict_tags(
    entry: &TableEntry,
    input: &LogicalPlan,
    catalog_snapshot: &CatalogSnapshot,
) -> Option<Vec<PredicateLockTag>> {
    let tag = match input {
        LogicalPlan::Filter { input, predicate } => {
            column_range_tag_for_filter(input, predicate, catalog_snapshot)?
        }
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::LockRows { input, .. } => {
            return input_write_conflict_tags(entry, input, catalog_snapshot);
        }
        LogicalPlan::Scan { table, .. } if table.eq_ignore_ascii_case(&entry.name) => {
            return None;
        }
        _ => return None,
    };
    if !tag_matches_entry(&tag, entry) {
        return None;
    }
    let mut tags = vec![tag.clone()];
    if let Some(locked_column) = column_range_tag_column(&tag) {
        tags.extend(table_column_range_write_tags_except(
            entry,
            Some(locked_column),
        ));
    }
    Some(tags)
}

fn column_range_tag_for_filter(
    input: &LogicalPlan,
    predicate: &ScalarExpr,
    catalog_snapshot: &CatalogSnapshot,
) -> Option<PredicateLockTag> {
    let LogicalPlan::Scan { table, .. } = input else {
        return None;
    };
    let entry = catalog_snapshot.tables.get(table)?;
    let (column, range) = pipeline::match_indexable_predicate(predicate)?;
    column_range_tag(entry, column, range.low, range.high)
}

fn table_column_range_write_tags(entry: &TableEntry) -> Vec<PredicateLockTag> {
    finish_write_tags(entry, table_column_range_write_tags_except(entry, None))
}

fn table_column_range_write_tags_except(
    entry: &TableEntry,
    excluded_column: Option<u16>,
) -> Vec<PredicateLockTag> {
    let mut tags = entry
        .schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(idx, _)| {
            let column = u16::try_from(idx).ok()?;
            (Some(column) != excluded_column).then(|| column_range_tag(entry, idx, None, None))?
        })
        .collect::<Vec<_>>();
    tags.retain(|tag| matches!(tag, PredicateLockTag::ColumnRange { .. }));
    dedup_predicate_tags(&mut tags);
    tags
}

fn finish_write_tags(entry: &TableEntry, mut tags: Vec<PredicateLockTag>) -> Vec<PredicateLockTag> {
    dedup_predicate_tags(&mut tags);
    if tags.is_empty() {
        vec![relation_predicate_tag(entry)]
    } else {
        tags
    }
}

fn dedup_predicate_tags(tags: &mut Vec<PredicateLockTag>) {
    let mut seen = std::collections::HashSet::new();
    tags.retain(|tag| seen.insert(tag.clone()));
}

fn relation_predicate_tag(entry: &TableEntry) -> PredicateLockTag {
    PredicateLockTag::Relation(RelationId(entry.oid))
}

fn tag_matches_entry(tag: &PredicateLockTag, entry: &TableEntry) -> bool {
    match tag {
        PredicateLockTag::Relation(relation) => relation.0 == entry.oid,
        PredicateLockTag::ColumnRange { relation, .. } => relation.0 == entry.oid,
        PredicateLockTag::Page(page) => page.relation.0 == entry.oid,
        PredicateLockTag::Tuple(tuple) => tuple.page.relation.0 == entry.oid,
    }
}

fn column_range_tag_column(tag: &PredicateLockTag) -> Option<u16> {
    match tag {
        PredicateLockTag::ColumnRange { column, .. } => Some(*column),
        _ => None,
    }
}

fn column_range_tag(
    entry: &TableEntry,
    column: usize,
    low: Option<i64>,
    high: Option<i64>,
) -> Option<PredicateLockTag> {
    if !column_supports_range_lock(entry, column) {
        return None;
    }
    let column = u16::try_from(column).ok()?;
    Some(PredicateLockTag::ColumnRange {
        relation: RelationId(entry.oid),
        column,
        low,
        high,
    })
}

fn column_supports_range_lock(entry: &TableEntry, column: usize) -> bool {
    entry
        .schema
        .fields()
        .get(column)
        .is_some_and(|field| data_type_supports_range_lock(&field.data_type))
}

const fn data_type_supports_range_lock(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Bool
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Timestamp
            | DataType::TimestampTz
    )
}
