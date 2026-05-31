//! Serializable isolation conflict plumbing.
//!
//! This module maps bound logical plans into SSI predicate-lock tags. The
//! supported precise subset uses scalar column ranges; unsupported or unsafe
//! shapes fall back to relation-wide tags so correctness stays conservative.

use ultrasql_catalog::{CatalogSnapshot, TableEntry};
use ultrasql_core::{DataType, RelationId};
use ultrasql_planner::{BinaryOp, LogicalPlan, ScalarExpr};
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
            if let Some(tags) = column_range_tags_for_filter(input, predicate, catalog_snapshot) {
                out.extend(tags);
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
    let Some(mut tags) = input_write_conflict_tags(entry, input, catalog_snapshot) else {
        return table_column_range_write_tags(entry);
    };
    if tags.is_empty() {
        return Vec::new();
    }
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
    let Some(tags) = input_write_conflict_tags(entry, input, catalog_snapshot) else {
        return table_column_range_write_tags(entry);
    };
    if tags.is_empty() {
        return Vec::new();
    }
    finish_write_tags(entry, tags)
}

fn input_write_conflict_tags(
    entry: &TableEntry,
    input: &LogicalPlan,
    catalog_snapshot: &CatalogSnapshot,
) -> Option<Vec<PredicateLockTag>> {
    match input {
        LogicalPlan::Filter { input, predicate } => {
            let tags = column_range_tags_for_filter(input, predicate, catalog_snapshot)?;
            if tags.iter().any(|tag| !tag_matches_entry(tag, entry)) {
                return None;
            }
            if tags.is_empty() {
                return Some(Vec::new());
            }
            let locked_columns = tags
                .iter()
                .filter_map(column_range_tag_column)
                .collect::<Vec<_>>();
            let mut out = tags;
            for tag in table_column_range_write_tags_except_any(entry, &locked_columns) {
                out.push(tag);
            }
            Some(out)
        }
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::LockRows { input, .. } => {
            input_write_conflict_tags(entry, input, catalog_snapshot)
        }
        LogicalPlan::Scan { table, .. } if table.eq_ignore_ascii_case(&entry.name) => None,
        _ => None,
    }
}

fn column_range_tags_for_filter(
    input: &LogicalPlan,
    predicate: &ScalarExpr,
    catalog_snapshot: &CatalogSnapshot,
) -> Option<Vec<PredicateLockTag>> {
    let LogicalPlan::Scan { table, .. } = input else {
        return None;
    };
    let entry = catalog_snapshot.tables.get(table)?;
    let mut tags = column_range_tags_for_predicate_expr(entry, predicate)?;
    dedup_predicate_tags(&mut tags);
    Some(tags)
}

fn column_range_tags_for_predicate_expr(
    entry: &TableEntry,
    predicate: &ScalarExpr,
) -> Option<Vec<PredicateLockTag>> {
    if let Some((column, range)) = pipeline::match_indexable_predicate(predicate) {
        let tag = column_range_tag(entry, column, range.low, range.high)?;
        return if column_range_tag_is_empty(&tag) {
            Some(Vec::new())
        } else {
            Some(vec![tag])
        };
    }
    match predicate {
        ScalarExpr::Binary {
            op: BinaryOp::And,
            left,
            right,
            ..
        } => {
            let mut left_tags = column_range_tags_for_predicate_expr(entry, left)?;
            if left_tags.is_empty() {
                return Some(Vec::new());
            }
            let right_tags = column_range_tags_for_predicate_expr(entry, right)?;
            if right_tags.is_empty() {
                return Some(Vec::new());
            }
            left_tags.extend(right_tags);
            Some(left_tags)
        }
        ScalarExpr::Binary {
            op: BinaryOp::Or,
            left,
            right,
            ..
        } => {
            let mut left_tags = column_range_tags_for_predicate_expr(entry, left)?;
            let right_tags = column_range_tags_for_predicate_expr(entry, right)?;
            left_tags.extend(right_tags);
            Some(left_tags)
        }
        _ => None,
    }
}

fn table_column_range_write_tags(entry: &TableEntry) -> Vec<PredicateLockTag> {
    finish_write_tags(entry, table_column_range_write_tags_except(entry, None))
}

fn table_column_range_write_tags_except(
    entry: &TableEntry,
    excluded_column: Option<u16>,
) -> Vec<PredicateLockTag> {
    table_column_range_write_tags_except_matching(entry, |column| Some(column) == excluded_column)
}

fn table_column_range_write_tags_except_any(
    entry: &TableEntry,
    excluded_columns: &[u16],
) -> Vec<PredicateLockTag> {
    table_column_range_write_tags_except_matching(entry, |column| {
        excluded_columns.contains(&column)
    })
}

fn table_column_range_write_tags_except_matching(
    entry: &TableEntry,
    is_excluded: impl Fn(u16) -> bool,
) -> Vec<PredicateLockTag> {
    let mut tags = entry
        .schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(idx, _)| {
            let column = u16::try_from(idx).ok()?;
            (!is_excluded(column)).then(|| column_range_tag(entry, idx, None, None))?
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

fn column_range_tag_is_empty(tag: &PredicateLockTag) -> bool {
    match tag {
        PredicateLockTag::ColumnRange { low, high, .. } => {
            matches!((low, high), (Some(low), Some(high)) if low > high)
        }
        _ => false,
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use ultrasql_catalog::CatalogSnapshot;
    use ultrasql_core::{Field, Oid, Schema, Value};
    use ultrasql_planner::{BinaryOp, LogicalPlan, ScalarExpr};
    use ultrasql_txn::PredicateLockTag;

    use super::*;

    fn test_schema() -> Schema {
        Schema::new([
            Field::required("a", DataType::Int32),
            Field::required("b", DataType::Int32),
        ])
        .expect("test schema is valid")
    }

    fn test_snapshot() -> CatalogSnapshot {
        let entry = TableEntry::new(Oid::new(42), "t", "public", test_schema());
        let mut tables = HashMap::new();
        tables.insert("t".to_owned(), entry.clone());
        let mut tables_by_oid = HashMap::new();
        tables_by_oid.insert(entry.oid, entry);
        CatalogSnapshot {
            tables,
            tables_by_oid,
            indexes: HashMap::new(),
            indexes_by_table: HashMap::new(),
            enum_types: HashMap::new(),
            enum_types_by_oid: HashMap::new(),
            composite_types: HashMap::new(),
            composite_types_by_oid: HashMap::new(),
            domain_types: HashMap::new(),
            domain_types_by_oid: HashMap::new(),
            constraints: HashMap::new(),
            descriptions: HashMap::new(),
            statistics: HashMap::new(),
            statistic_ext: HashMap::new(),
        }
    }

    fn scan() -> LogicalPlan {
        LogicalPlan::Scan {
            table: "t".to_owned(),
            schema: test_schema(),
            projection: None,
        }
    }

    fn col(index: usize, name: &str) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.to_owned(),
            index,
            data_type: DataType::Int32,
        }
    }

    fn lit_i32(value: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(value),
            data_type: DataType::Int32,
        }
    }

    fn binary(op: BinaryOp, left: ScalarExpr, right: ScalarExpr) -> ScalarExpr {
        ScalarExpr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
            data_type: DataType::Bool,
        }
    }

    #[test]
    fn serializable_read_locks_split_supported_multi_column_conjunctions() {
        let predicate = binary(
            BinaryOp::And,
            binary(BinaryOp::Eq, col(0, "a"), lit_i32(7)),
            binary(BinaryOp::Eq, col(1, "b"), lit_i32(9)),
        );
        let plan = LogicalPlan::Filter {
            input: Box::new(scan()),
            predicate,
        };
        let snapshot = test_snapshot();
        let mut tags = Vec::new();

        collect_serializable_read_locks(&plan, &snapshot, &mut tags);
        dedup_predicate_tags(&mut tags);

        assert_eq!(
            tags,
            vec![
                PredicateLockTag::ColumnRange {
                    relation: RelationId(Oid::new(42)),
                    column: 0,
                    low: Some(7),
                    high: Some(7),
                },
                PredicateLockTag::ColumnRange {
                    relation: RelationId(Oid::new(42)),
                    column: 1,
                    low: Some(9),
                    high: Some(9),
                },
            ]
        );
    }

    #[test]
    fn serializable_delete_conflicts_split_supported_multi_column_conjunctions() {
        let predicate = binary(
            BinaryOp::And,
            binary(BinaryOp::Eq, col(0, "a"), lit_i32(7)),
            binary(BinaryOp::Eq, col(1, "b"), lit_i32(9)),
        );
        let input = LogicalPlan::Filter {
            input: Box::new(scan()),
            predicate,
        };
        let snapshot = test_snapshot();
        let entry = snapshot.tables.get("t").expect("table exists");

        assert_eq!(
            delete_write_conflict_tags(entry, &input, &snapshot),
            vec![
                PredicateLockTag::ColumnRange {
                    relation: RelationId(Oid::new(42)),
                    column: 0,
                    low: Some(7),
                    high: Some(7),
                },
                PredicateLockTag::ColumnRange {
                    relation: RelationId(Oid::new(42)),
                    column: 1,
                    low: Some(9),
                    high: Some(9),
                },
            ]
        );
    }

    #[test]
    fn serializable_read_locks_split_supported_disjunctions() {
        let predicate = binary(
            BinaryOp::Or,
            binary(BinaryOp::Eq, col(0, "a"), lit_i32(7)),
            binary(BinaryOp::Eq, col(0, "a"), lit_i32(9)),
        );
        let plan = LogicalPlan::Filter {
            input: Box::new(scan()),
            predicate,
        };
        let snapshot = test_snapshot();
        let mut tags = Vec::new();

        collect_serializable_read_locks(&plan, &snapshot, &mut tags);
        dedup_predicate_tags(&mut tags);

        assert_eq!(
            tags,
            vec![
                PredicateLockTag::ColumnRange {
                    relation: RelationId(Oid::new(42)),
                    column: 0,
                    low: Some(7),
                    high: Some(7),
                },
                PredicateLockTag::ColumnRange {
                    relation: RelationId(Oid::new(42)),
                    column: 0,
                    low: Some(9),
                    high: Some(9),
                },
            ]
        );
    }

    #[test]
    fn serializable_delete_conflicts_split_supported_disjunctions() {
        let predicate = binary(
            BinaryOp::Or,
            binary(BinaryOp::Eq, col(0, "a"), lit_i32(7)),
            binary(BinaryOp::Eq, col(0, "a"), lit_i32(9)),
        );
        let input = LogicalPlan::Filter {
            input: Box::new(scan()),
            predicate,
        };
        let snapshot = test_snapshot();
        let entry = snapshot.tables.get("t").expect("table exists");

        assert_eq!(
            delete_write_conflict_tags(entry, &input, &snapshot),
            vec![
                PredicateLockTag::ColumnRange {
                    relation: RelationId(Oid::new(42)),
                    column: 0,
                    low: Some(7),
                    high: Some(7),
                },
                PredicateLockTag::ColumnRange {
                    relation: RelationId(Oid::new(42)),
                    column: 0,
                    low: Some(9),
                    high: Some(9),
                },
                PredicateLockTag::ColumnRange {
                    relation: RelationId(Oid::new(42)),
                    column: 1,
                    low: None,
                    high: None,
                },
            ]
        );
    }

    #[test]
    fn serializable_read_locks_skip_empty_supported_ranges() {
        let predicate = binary(
            BinaryOp::And,
            binary(BinaryOp::Gt, col(0, "a"), lit_i32(10)),
            binary(BinaryOp::Lt, col(0, "a"), lit_i32(5)),
        );
        let plan = LogicalPlan::Filter {
            input: Box::new(scan()),
            predicate,
        };
        let snapshot = test_snapshot();
        let mut tags = Vec::new();

        collect_serializable_read_locks(&plan, &snapshot, &mut tags);
        dedup_predicate_tags(&mut tags);

        assert!(tags.is_empty(), "empty predicate must not lock relation");
    }

    #[test]
    fn serializable_delete_conflicts_skip_empty_supported_ranges() {
        let predicate = binary(
            BinaryOp::And,
            binary(BinaryOp::Gt, col(0, "a"), lit_i32(10)),
            binary(BinaryOp::Lt, col(0, "a"), lit_i32(5)),
        );
        let input = LogicalPlan::Filter {
            input: Box::new(scan()),
            predicate,
        };
        let snapshot = test_snapshot();
        let entry = snapshot.tables.get("t").expect("table exists");

        assert!(
            delete_write_conflict_tags(entry, &input, &snapshot).is_empty(),
            "empty write predicate must not report write-conflict tags"
        );
    }

    #[test]
    fn serializable_read_locks_keep_empty_and_semantics() {
        let empty_a = binary(
            BinaryOp::And,
            binary(BinaryOp::Gt, col(0, "a"), lit_i32(10)),
            binary(BinaryOp::Lt, col(0, "a"), lit_i32(5)),
        );
        let predicate = binary(
            BinaryOp::And,
            empty_a,
            binary(BinaryOp::Eq, col(1, "b"), lit_i32(1)),
        );
        let plan = LogicalPlan::Filter {
            input: Box::new(scan()),
            predicate,
        };
        let snapshot = test_snapshot();
        let mut tags = Vec::new();

        collect_serializable_read_locks(&plan, &snapshot, &mut tags);
        dedup_predicate_tags(&mut tags);

        assert!(
            tags.is_empty(),
            "empty conjunction must not retain sibling range tags"
        );
    }

    #[test]
    fn serializable_read_locks_keep_relation_fallback_for_unsupported_conjunctions() {
        let unsupported_rhs = binary(
            BinaryOp::Eq,
            binary(BinaryOp::Add, col(1, "b"), lit_i32(1)),
            lit_i32(9),
        );
        let predicate = binary(
            BinaryOp::And,
            binary(BinaryOp::Eq, col(0, "a"), lit_i32(7)),
            unsupported_rhs,
        );
        let plan = LogicalPlan::Filter {
            input: Box::new(scan()),
            predicate,
        };
        let snapshot = test_snapshot();
        let mut tags = Vec::new();

        collect_serializable_read_locks(&plan, &snapshot, &mut tags);
        dedup_predicate_tags(&mut tags);

        assert_eq!(
            tags,
            vec![PredicateLockTag::Relation(RelationId(Oid::new(42)))]
        );
    }
}
