//! Serializable isolation conflict plumbing.
//!
//! This module maps bound logical plans into SSI predicate-lock tags. The
//! supported precise subset uses scalar column ranges; unsupported or unsafe
//! shapes fall back to relation-wide tags so correctness stays conservative.

use ultrasql_catalog::{CatalogSnapshot, TableEntry};
use ultrasql_core::{DataType, RelationId};
use ultrasql_planner::{BinaryOp, LogicalMergeAction, LogicalPlan, ScalarExpr};
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
        LogicalPlan::Summarize {
            table, namespace, ..
        } => {
            let key = ultrasql_catalog::table_lookup_key(namespace, table);
            if let Some(entry) = catalog_snapshot.tables.get(&key) {
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
        | LogicalPlan::SingleRowAssert { input, .. }
        // DISTINCT ON / PIVOT / UNPIVOT are non-leaf table factors: their
        // `input` can be a `Scan`, so we must descend to take the read-lock.
        // Skipping them would silently drop the predicate lock on the scanned
        // relation and miss read-write conflicts (an SSI serialization hole).
        | LogicalPlan::DistinctOn { input, .. }
        | LogicalPlan::Pivot { input, .. }
        | LogicalPlan::Unpivot { input, .. }
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
        // MERGE reads its `source` relation; descend so its scans take read
        // locks too (the target write conflicts are tracked separately).
        LogicalPlan::Merge { source, .. } => {
            collect_serializable_read_locks(source, catalog_snapshot, out);
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => {
            collect_serializable_read_locks(definition, catalog_snapshot, out);
            collect_serializable_read_locks(body, catalog_snapshot, out);
        }
        // Remaining shapes carry no readable base-table subtree that isn't
        // covered above: leaves with no scan (`Values`, `Empty`,
        // `FunctionScan`), and control/DDL plans that never run as a
        // serializable read. They legitimately need no predicate lock.
        _ => {}
    }
    // A node's own *expressions* can embed a [`LogicalPlan`] subquery
    // (uncorrelated `EXISTS` / `IN` / scalar / `= ANY`). Such a subplan is
    // NOT decorrelated to a join, so the match above — which only descends
    // child plan nodes — never reaches the relation it scans. Descend into
    // every embedded subplan so its scans acquire predicate read-locks too;
    // otherwise a SERIALIZABLE reader probing a relation only through a
    // subquery would take no lock and miss a read-write conflict (the SSI
    // mirror of the RLS-bypass hole).
    collect_node_expr_subplan_read_locks(plan, catalog_snapshot, out);
}

/// Descend into every [`LogicalPlan`] embedded in `plan`'s own expressions and
/// collect its serializable read-locks. Mirrors the RLS walker's
/// `apply_row_security_embedded_subplans`: it visits exactly the expression
/// positions that can carry a subquery plan and re-enters
/// [`collect_serializable_read_locks`] on each.
fn collect_node_expr_subplan_read_locks(
    plan: &LogicalPlan,
    catalog_snapshot: &CatalogSnapshot,
    out: &mut Vec<PredicateLockTag>,
) {
    let mut visit = |subplan: &LogicalPlan| {
        collect_serializable_read_locks(subplan, catalog_snapshot, out);
    };
    match plan {
        LogicalPlan::Filter { predicate, .. } => predicate.for_each_subplan(&mut visit),
        LogicalPlan::Project { exprs, .. } => {
            for (expr, _) in exprs {
                expr.for_each_subplan(&mut visit);
            }
        }
        LogicalPlan::Join {
            condition: ultrasql_planner::LogicalJoinCondition::On(on_expr),
            ..
        } => on_expr.for_each_subplan(&mut visit),
        LogicalPlan::Sort { keys, .. } => {
            for key in keys {
                key.expr.for_each_subplan(&mut visit);
            }
        }
        LogicalPlan::DistinctOn { on_keys, .. } => {
            for key in on_keys {
                key.for_each_subplan(&mut visit);
            }
        }
        LogicalPlan::Window {
            partition_by,
            order_by,
            ..
        } => {
            for expr in partition_by {
                expr.for_each_subplan(&mut visit);
            }
            for key in order_by {
                key.expr.for_each_subplan(&mut visit);
            }
        }
        LogicalPlan::Aggregate {
            group_by,
            aggregates,
            ..
        } => {
            for expr in group_by {
                expr.for_each_subplan(&mut visit);
            }
            for agg in aggregates {
                if let Some(arg) = &agg.arg {
                    arg.for_each_subplan(&mut visit);
                }
                if let Some(direct) = &agg.direct_arg {
                    direct.for_each_subplan(&mut visit);
                }
                if let Some(key) = &agg.order_by {
                    key.expr.for_each_subplan(&mut visit);
                }
            }
        }
        LogicalPlan::Update {
            assignments,
            returning,
            ..
        } => {
            for (_, expr) in assignments {
                expr.for_each_subplan(&mut visit);
            }
            for (expr, _) in returning {
                expr.for_each_subplan(&mut visit);
            }
        }
        LogicalPlan::Insert { returning, .. } | LogicalPlan::Delete { returning, .. } => {
            for (expr, _) in returning {
                expr.for_each_subplan(&mut visit);
            }
        }
        // MERGE's `source` child is descended by the main match; here we cover
        // the node's own expressions — the `ON` predicate, each `WHEN` clause's
        // `AND` condition, and each action's expressions (UPDATE assignment
        // RHS, INSERT `VALUES`). A subquery embedded in any of these is NOT
        // decorrelated to a join, so without descending here a SERIALIZABLE
        // reader probing a relation only through such a subquery would take no
        // predicate lock and miss a read-write conflict (the SSI mirror of the
        // RLS-bypass hole).
        LogicalPlan::Merge { on, clauses, .. } => {
            on.for_each_subplan(&mut visit);
            for clause in clauses {
                if let Some(condition) = &clause.condition {
                    condition.for_each_subplan(&mut visit);
                }
                match &clause.action {
                    LogicalMergeAction::Update { assignments } => {
                        for (_, expr) in assignments {
                            expr.for_each_subplan(&mut visit);
                        }
                    }
                    LogicalMergeAction::Insert { values, .. } => {
                        for expr in values {
                            expr.for_each_subplan(&mut visit);
                        }
                    }
                    LogicalMergeAction::Delete => {}
                }
            }
        }
        // Other shapes carry no expression position that can embed a subquery
        // plan reachable from here. (`Join { condition: Using | None }` has no
        // scalar predicate; child-plan subqueries are covered by the main
        // match's recursion.)
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
            // Only take a tight point lock when the inserted literal is in
            // the same i64 unit-class as the target column; a cross-class
            // literal (e.g. a Timestamp value into a Date column) would
            // lock a mis-scaled key and silently miss conflicts, so fall
            // back to the safe full-range lock for that column.
            if let Some(key) = pipeline::literal_as_i64(expr)
                .filter(|_| literal_matches_column_unit_class(entry, target_idx, expr))
            {
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
            let left_tags = column_range_tags_for_predicate_expr(entry, left);
            let right_tags = column_range_tags_for_predicate_expr(entry, right);
            match (left_tags, right_tags) {
                (Some(tags), _) if tags.is_empty() => Some(Vec::new()),
                (_, Some(tags)) if tags.is_empty() => Some(Vec::new()),
                (Some(mut left_tags), Some(right_tags)) => {
                    left_tags.extend(right_tags);
                    Some(left_tags)
                }
                (Some(tags), None) | (None, Some(tags)) => Some(tags),
                (None, None) => None,
            }
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

/// Whether `expr`'s literal value shares the i64 unit-class of column
/// `column` — the precondition for an `INSERT` to take a tight point
/// lock instead of degrading to the safe full-range lock. Returns `false`
/// for non-literals or cross-class temporal pairs.
fn literal_matches_column_unit_class(entry: &TableEntry, column: usize, expr: &ScalarExpr) -> bool {
    let Some(field) = entry.schema.fields().get(column) else {
        return false;
    };
    let ScalarExpr::Literal { value, .. } = expr else {
        return false;
    };
    pipeline::literal_in_same_unit_class_as_column(&field.data_type, value)
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
            | DataType::Date
            | DataType::Time
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use ultrasql_catalog::CatalogSnapshot;
    use ultrasql_core::{Field, Oid, Schema, Value};
    use ultrasql_planner::{
        BinaryOp, LogicalMergeAction, LogicalMergeClause, LogicalMergeMatchKind, LogicalPlan,
        ScalarExpr,
    };
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
        snapshot_for_schema(test_schema())
    }

    /// Build a single-table catalog snapshot named `t` (oid 42) over an
    /// arbitrary schema, so Date/Time tests can reuse the same plumbing
    /// the Int32 tests use without hard-coding the column types.
    fn snapshot_for_schema(schema: Schema) -> CatalogSnapshot {
        let entry = TableEntry::new(Oid::new(42), "t", "public", schema);
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
        let unsupported_lhs = binary(
            BinaryOp::Eq,
            binary(BinaryOp::Add, col(0, "a"), lit_i32(1)),
            lit_i32(7),
        );
        let unsupported_rhs = binary(
            BinaryOp::Eq,
            binary(BinaryOp::Add, col(1, "b"), lit_i32(1)),
            lit_i32(9),
        );
        let predicate = binary(BinaryOp::And, unsupported_lhs, unsupported_rhs);
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

    #[test]
    fn serializable_read_locks_keep_supported_conjunct_with_unsupported_peer() {
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
            vec![PredicateLockTag::ColumnRange {
                relation: RelationId(Oid::new(42)),
                column: 0,
                low: Some(7),
                high: Some(7),
            }]
        );
    }

    // ── Date / Time predicate-range precision ────────────────────────────────
    //
    // These tests pin the i64 ranges the SSI lock computes for `Date` and
    // `Time` predicates. The conflict-detection machinery
    // (`ranges_overlap` in the txn crate) is type-agnostic and already
    // proven for the integer family; what these tests guard is that a
    // Date/Time predicate now produces the *tight* `ColumnRange` tag
    // (column-scoped, exact i64 bounds) instead of the relation-wide
    // fallback — and that the strict-bound `±1` normalisation is
    // integer-exact for the discrete day / microsecond domains.

    /// Two-column schema: a `Date` column and a `Time` column, mirroring
    /// the Int32 `test_schema` so the Date/Time tests reuse `scan()`-style
    /// plumbing through `snapshot_for_schema`.
    fn date_time_schema() -> Schema {
        Schema::new([
            Field::required("d", DataType::Date),
            Field::required("t", DataType::Time),
        ])
        .expect("date/time test schema is valid")
    }

    fn date_time_scan() -> LogicalPlan {
        LogicalPlan::Scan {
            table: "t".to_owned(),
            schema: date_time_schema(),
            projection: None,
        }
    }

    fn date_col(index: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: "d".to_owned(),
            index,
            data_type: DataType::Date,
        }
    }

    fn time_col(index: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: "t".to_owned(),
            index,
            data_type: DataType::Time,
        }
    }

    /// `Value::Date` is `i32` days since 2000-01-01.
    fn lit_date(days: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Date(days),
            data_type: DataType::Date,
        }
    }

    /// `Value::Time` is `i64` microseconds since midnight.
    fn lit_time(micros: i64) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Time(micros),
            data_type: DataType::Time,
        }
    }

    fn read_locks(plan: &LogicalPlan, snapshot: &CatalogSnapshot) -> Vec<PredicateLockTag> {
        let mut tags = Vec::new();
        collect_serializable_read_locks(plan, snapshot, &mut tags);
        dedup_predicate_tags(&mut tags);
        tags
    }

    /// `WHERE d = DATE '...'` now takes a tight point ColumnRange on the
    /// date column — not a relation-wide lock. Proves the tightening
    /// benefit: a date-equality reader no longer blankets the relation.
    #[test]
    fn serializable_read_lock_date_equality_is_point_range() {
        let plan = LogicalPlan::Filter {
            input: Box::new(date_time_scan()),
            predicate: binary(BinaryOp::Eq, date_col(0), lit_date(8821)),
        };
        let snapshot = snapshot_for_schema(date_time_schema());

        assert_eq!(
            read_locks(&plan, &snapshot),
            vec![PredicateLockTag::ColumnRange {
                relation: RelationId(Oid::new(42)),
                column: 0,
                low: Some(8821),
                high: Some(8821),
            }],
            "date equality must yield a tight column-point lock"
        );
    }

    /// Strict upper bound on a `Date`: `d < DATE '2000-01-02'` (day 1)
    /// must normalise to the inclusive `high = 0` (i.e. exactly
    /// '2000-01-01' and earlier). The `-1` adjustment is integer-exact
    /// because the date domain is whole days.
    #[test]
    fn serializable_read_lock_date_strict_upper_bound_is_integer_exact() {
        let plan = LogicalPlan::Filter {
            input: Box::new(date_time_scan()),
            predicate: binary(BinaryOp::Lt, date_col(0), lit_date(1)),
        };
        let snapshot = snapshot_for_schema(date_time_schema());

        assert_eq!(
            read_locks(&plan, &snapshot),
            vec![PredicateLockTag::ColumnRange {
                relation: RelationId(Oid::new(42)),
                column: 0,
                low: None,
                high: Some(0),
            }],
            "d < day 1 must cover exactly day 0 and earlier (high = 0)"
        );
    }

    /// Negative / pre-2000 `Date` values sign-extend correctly: a
    /// half-open `d >= DATE '1999-12-31'` (day -1) locks `low = Some(-1)`
    /// with no upper bound, the same way a negative `Int32` would.
    #[test]
    fn serializable_read_lock_date_negative_pre_2000_sign_extends() {
        let plan = LogicalPlan::Filter {
            input: Box::new(date_time_scan()),
            predicate: binary(BinaryOp::GtEq, date_col(0), lit_date(-1)),
        };
        let snapshot = snapshot_for_schema(date_time_schema());

        assert_eq!(
            read_locks(&plan, &snapshot),
            vec![PredicateLockTag::ColumnRange {
                relation: RelationId(Oid::new(42)),
                column: 0,
                low: Some(-1),
                high: None,
            }],
            "pre-2000 date must sign-extend to a negative i64 bound"
        );
    }

    /// `Time` at microsecond boundaries: `t > TIME '00:00:00.000001'`
    /// (1 µs) normalises to the inclusive `low = 2` — the `+1` adjustment
    /// is integer-exact at µs granularity.
    #[test]
    fn serializable_read_lock_time_strict_lower_bound_at_micro_boundary() {
        let plan = LogicalPlan::Filter {
            input: Box::new(date_time_scan()),
            predicate: binary(BinaryOp::Gt, time_col(1), lit_time(1)),
        };
        let snapshot = snapshot_for_schema(date_time_schema());

        assert_eq!(
            read_locks(&plan, &snapshot),
            vec![PredicateLockTag::ColumnRange {
                relation: RelationId(Oid::new(42)),
                column: 1,
                low: Some(2),
                high: None,
            }],
            "t > 1µs must cover exactly 2µs and later (low = 2)"
        );
    }

    /// A `Date` BETWEEN-shaped conjunction (`d >= lo AND d <= hi`) folds
    /// into a single bounded ColumnRange — the canonical tightening for a
    /// date-range serializable read.
    #[test]
    fn serializable_read_lock_date_between_folds_to_bounded_range() {
        let plan = LogicalPlan::Filter {
            input: Box::new(date_time_scan()),
            predicate: binary(
                BinaryOp::And,
                binary(BinaryOp::GtEq, date_col(0), lit_date(8800)),
                binary(BinaryOp::LtEq, date_col(0), lit_date(8810)),
            ),
        };
        let snapshot = snapshot_for_schema(date_time_schema());

        assert_eq!(
            read_locks(&plan, &snapshot),
            vec![PredicateLockTag::ColumnRange {
                relation: RelationId(Oid::new(42)),
                column: 0,
                low: Some(8800),
                high: Some(8810),
            }],
            "date BETWEEN must fold into one bounded column range"
        );
    }

    /// The write side tightens symmetrically: a DELETE keyed on a tight
    /// `Date` range locks the precise date column range *plus* a
    /// full-range tag for every other column — preserving the write-skew
    /// safety net. This is the same shape the Int32 disjunction test
    /// asserts, now driven by a Date predicate.
    #[test]
    fn serializable_delete_date_range_tightens_with_full_range_safety_net() {
        let input = LogicalPlan::Filter {
            input: Box::new(date_time_scan()),
            predicate: binary(BinaryOp::Eq, date_col(0), lit_date(8821)),
        };
        let snapshot = snapshot_for_schema(date_time_schema());
        let entry = snapshot.tables.get("t").expect("table exists");

        assert_eq!(
            delete_write_conflict_tags(entry, &input, &snapshot),
            vec![
                PredicateLockTag::ColumnRange {
                    relation: RelationId(Oid::new(42)),
                    column: 0,
                    low: Some(8821),
                    high: Some(8821),
                },
                PredicateLockTag::ColumnRange {
                    relation: RelationId(Oid::new(42)),
                    column: 1,
                    low: None,
                    high: None,
                },
            ],
            "date-keyed delete locks the tight date range plus a \
             full-range tag on the other column"
        );
    }

    // ── Cross-unit-class temporal guard (missed-conflict / silent corruption) ──
    //
    // A temporal column compared against a literal of a *different* i64
    // unit-class (e.g. a `Date` column — days — vs a `Timestamp` literal —
    // microseconds) must NOT take a tight, mis-scaled `ColumnRange` lock.
    // The binder allows any temporal-vs-temporal comparison without
    // coercing the literal (`comparable` in expr_type.rs), so such a plan
    // really reaches us. A tight micro-space lock on a Date column would
    // never overlap a concurrent writer's day-space Date lock, so the real
    // rw-conflict would be silently missed (non-serializable schedule).
    // The matcher therefore returns None for cross-class pairs and SSI
    // falls back to the safe relation-wide lock, which DOES overlap.

    fn ts_date_schema() -> Schema {
        Schema::new([
            Field::required("d", DataType::Date),
            Field::required("ts", DataType::Timestamp),
        ])
        .expect("ts/date test schema is valid")
    }

    fn ts_date_scan() -> LogicalPlan {
        LogicalPlan::Scan {
            table: "t".to_owned(),
            schema: ts_date_schema(),
            projection: None,
        }
    }

    fn ts_date_col(index: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: "d".to_owned(),
            index,
            data_type: DataType::Date,
        }
    }

    fn ts_col(index: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: "ts".to_owned(),
            index,
            data_type: DataType::Timestamp,
        }
    }

    /// `Value::Timestamp` is `i64` microseconds since the epoch.
    fn lit_timestamp(micros: i64) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Timestamp(micros),
            data_type: DataType::Timestamp,
        }
    }

    /// `Value::TimestampTz` is `i64` microseconds since the epoch.
    fn lit_timestamptz(micros: i64) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::TimestampTz(micros),
            data_type: DataType::TimestampTz,
        }
    }

    fn time_only_col(index: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: "t".to_owned(),
            index,
            data_type: DataType::Time,
        }
    }

    fn lit_time_value(micros: i64) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Time(micros),
            data_type: DataType::Time,
        }
    }

    /// The gate's repro (READ side): `WHERE d = TIMESTAMP '...'` on a Date
    /// column. The literal is in microseconds, the column in days — a
    /// cross-class pair. The tight micro-space lock would miss a concurrent
    /// day-space Date writer, so the matcher MUST fall back to a
    /// relation-wide lock (which overlaps and catches the conflict).
    #[test]
    fn serializable_read_lock_date_col_vs_timestamp_literal_falls_back_to_relation() {
        // 2000-01-02 00:00 as micros = 86_400_000_000; as a tight Date
        // lock this would (wrongly) read day 86_400_000_000.
        let plan = LogicalPlan::Filter {
            input: Box::new(ts_date_scan()),
            predicate: binary(BinaryOp::Eq, ts_date_col(0), lit_timestamp(86_400_000_000)),
        };
        let snapshot = snapshot_for_schema(ts_date_schema());

        assert_eq!(
            read_locks(&plan, &snapshot),
            vec![PredicateLockTag::Relation(RelationId(Oid::new(42)))],
            "Date column vs Timestamp literal must fall back to the \
             relation-wide lock, not a mis-scaled micro-space range"
        );
    }

    /// The gate's repro (WRITE side): a DELETE keyed on `d = TIMESTAMP '...'`
    /// on a Date column must NOT take a tight, mis-scaled Date lock. The
    /// matcher returns None, so the write path degrades to the safe
    /// table-wide fallback: a *full-range* ColumnRange on every column
    /// (`low/high = None`). A full-range column lock overlaps a concurrent
    /// reader's day-space Date lock on the same column, so the rw-conflict
    /// is still caught — exactly the missing safety the bug removed. Proves
    /// the write path is guarded too. (The key property is that NO tight
    /// micro-space Date range is produced for column 0.)
    #[test]
    fn serializable_delete_date_col_vs_timestamp_literal_falls_back_to_full_range() {
        let input = LogicalPlan::Filter {
            input: Box::new(ts_date_scan()),
            predicate: binary(BinaryOp::Eq, ts_date_col(0), lit_timestamp(86_400_000_000)),
        };
        let snapshot = snapshot_for_schema(ts_date_schema());
        let entry = snapshot.tables.get("t").expect("table exists");

        let tags = delete_write_conflict_tags(entry, &input, &snapshot);

        // Safe fallback: every column locked full-range; specifically the
        // Date column (0) is full-range, NOT the mis-scaled micro point.
        assert_eq!(
            tags,
            vec![
                PredicateLockTag::ColumnRange {
                    relation: RelationId(Oid::new(42)),
                    column: 0,
                    low: None,
                    high: None,
                },
                PredicateLockTag::ColumnRange {
                    relation: RelationId(Oid::new(42)),
                    column: 1,
                    low: None,
                    high: None,
                },
            ],
            "cross-class Date-vs-Timestamp delete must degrade to a \
             full-range write lock on every column, never a tight \
             micro-space Date range"
        );
        assert!(
            !tags.iter().any(|tag| matches!(
                tag,
                PredicateLockTag::ColumnRange {
                    column: 0,
                    low: Some(_),
                    ..
                }
            )),
            "the Date column must never carry a tight (mis-scaled) bound"
        );
    }

    /// Reverse direction: a `Timestamp` column (micros) compared against a
    /// `Date` literal (days) is also cross-class and must fall back.
    #[test]
    fn serializable_read_lock_timestamp_col_vs_date_literal_falls_back_to_relation() {
        let plan = LogicalPlan::Filter {
            input: Box::new(ts_date_scan()),
            predicate: binary(BinaryOp::Eq, ts_col(1), lit_date(1)),
        };
        let snapshot = snapshot_for_schema(ts_date_schema());

        assert_eq!(
            read_locks(&plan, &snapshot),
            vec![PredicateLockTag::Relation(RelationId(Oid::new(42)))],
            "Timestamp column vs Date literal must fall back to the \
             relation-wide lock"
        );
    }

    /// `Time` column (micros since midnight) vs `Timestamp` literal (micros
    /// since epoch) is cross-class — same units numerically but a different
    /// origin, so the lock would still be wrong. Must fall back.
    #[test]
    fn serializable_read_lock_time_col_vs_timestamp_literal_falls_back_to_relation() {
        let schema = Schema::new([
            Field::required("t", DataType::Time),
            Field::required("v", DataType::Int32),
        ])
        .expect("time schema valid");
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: "t".to_owned(),
                schema: schema.clone(),
                projection: None,
            }),
            predicate: binary(BinaryOp::Eq, time_only_col(0), lit_timestamp(3_600_000_000)),
        };
        let snapshot = snapshot_for_schema(schema);

        assert_eq!(
            read_locks(&plan, &snapshot),
            vec![PredicateLockTag::Relation(RelationId(Oid::new(42)))],
            "Time column vs Timestamp literal must fall back to the \
             relation-wide lock"
        );
    }

    /// No-regression: a `Timestamp` column vs a `TimestampTz` literal stays
    /// in the same unit-class (both micros since epoch, interchangeable), so
    /// it MUST still take the tight column-point lock. Proves TS cross-compat
    /// is preserved by the guard.
    #[test]
    fn serializable_read_lock_timestamp_col_vs_timestamptz_literal_stays_tight() {
        let plan = LogicalPlan::Filter {
            input: Box::new(ts_date_scan()),
            predicate: binary(BinaryOp::Eq, ts_col(1), lit_timestamptz(86_400_000_000)),
        };
        let snapshot = snapshot_for_schema(ts_date_schema());

        assert_eq!(
            read_locks(&plan, &snapshot),
            vec![PredicateLockTag::ColumnRange {
                relation: RelationId(Oid::new(42)),
                column: 1,
                low: Some(86_400_000_000),
                high: Some(86_400_000_000),
            }],
            "Timestamp/TimestampTz share a unit-class and must stay tight"
        );
    }

    /// No-regression: same-class `Time` column vs `Time` literal stays tight.
    #[test]
    fn serializable_read_lock_time_col_vs_time_literal_stays_tight() {
        let schema = Schema::new([
            Field::required("t", DataType::Time),
            Field::required("v", DataType::Int32),
        ])
        .expect("time schema valid");
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: "t".to_owned(),
                schema: schema.clone(),
                projection: None,
            }),
            predicate: binary(
                BinaryOp::Eq,
                time_only_col(0),
                lit_time_value(3_600_000_000),
            ),
        };
        let snapshot = snapshot_for_schema(schema);

        assert_eq!(
            read_locks(&plan, &snapshot),
            vec![PredicateLockTag::ColumnRange {
                relation: RelationId(Oid::new(42)),
                column: 0,
                low: Some(3_600_000_000),
                high: Some(3_600_000_000),
            }],
            "same-class Time/Time must stay tight"
        );
    }

    /// No-regression: INT-class width-crossing stays tight — an `Int32`
    /// column compared against an `Int64` literal shares the integer
    /// unit-class (raw integer value), so it keeps the tight lock.
    #[test]
    fn serializable_read_lock_int32_col_vs_int64_literal_stays_tight() {
        let plan = LogicalPlan::Filter {
            input: Box::new(scan()),
            predicate: binary(
                BinaryOp::Eq,
                col(0, "a"),
                ScalarExpr::Literal {
                    value: Value::Int64(7),
                    data_type: DataType::Int64,
                },
            ),
        };
        let snapshot = test_snapshot();

        assert_eq!(
            read_locks(&plan, &snapshot),
            vec![PredicateLockTag::ColumnRange {
                relation: RelationId(Oid::new(42)),
                column: 0,
                low: Some(7),
                high: Some(7),
            }],
            "Int32 column vs Int64 literal shares the INT unit-class and \
             must stay tight (width-crossing allowed)"
        );
    }

    // ── MERGE clause-expression subplan read locks ───────────────────────────
    //
    // A relation scanned only through a subquery embedded in a MERGE `ON`
    // predicate, a `WHEN` clause condition, an UPDATE assignment RHS, or a
    // NOT-MATCHED INSERT `VALUES` expression is reachable by neither the main
    // match's child-plan recursion nor a decorrelated join. A SERIALIZABLE
    // reader probing such a relation must still take a predicate read-lock, or
    // a concurrent writer's read-write conflict is silently missed. These tests
    // pin that `collect_serializable_read_locks` descends into every MERGE
    // clause-expression position.

    /// `EXISTS (SELECT … FROM t)` — boolean subquery scanning `t` (oid 42),
    /// for the `ON` / `WHEN` condition positions.
    fn exists_scan_t() -> ScalarExpr {
        ScalarExpr::Exists {
            subplan: Box::new(scan()),
            negated: false,
            correlated: false,
        }
    }

    /// `(SELECT a FROM t)` — scalar subquery scanning `t` (oid 42), for the
    /// value positions (UPDATE assignment RHS / INSERT `VALUES`).
    fn scalar_scan_t() -> ScalarExpr {
        ScalarExpr::ScalarSubquery {
            subplan: Box::new(scan()),
            correlated: false,
            data_type: DataType::Int32,
        }
    }

    fn bool_true() -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Bool(true),
            data_type: DataType::Bool,
        }
    }

    fn t_relation_tag() -> PredicateLockTag {
        PredicateLockTag::Relation(RelationId(Oid::new(42)))
    }

    /// Build a MERGE plan with a table-free source so any collected read-lock
    /// must come from descending into the clause expressions under test.
    fn merge_plan(on: ScalarExpr, clauses: Vec<LogicalMergeClause>) -> LogicalPlan {
        LogicalPlan::Merge {
            target: "merge_target".to_owned(),
            target_alias: None,
            target_schema: test_schema(),
            source: Box::new(LogicalPlan::Values {
                rows: Vec::new(),
                schema: Schema::empty(),
            }),
            on,
            clauses,
            schema: Schema::empty(),
        }
    }

    #[test]
    fn serializable_read_locks_descend_into_merge_on_subquery() {
        let snapshot = test_snapshot();
        let plan = merge_plan(exists_scan_t(), Vec::new());
        assert_eq!(
            read_locks(&plan, &snapshot),
            vec![t_relation_tag()],
            "a relation scanned only through a MERGE ON subquery must take a read lock"
        );
    }

    #[test]
    fn serializable_read_locks_descend_into_merge_when_condition_subquery() {
        let snapshot = test_snapshot();
        let plan = merge_plan(
            bool_true(),
            vec![LogicalMergeClause {
                kind: LogicalMergeMatchKind::Matched,
                condition: Some(exists_scan_t()),
                action: LogicalMergeAction::Delete,
            }],
        );
        assert_eq!(
            read_locks(&plan, &snapshot),
            vec![t_relation_tag()],
            "a relation scanned only through a MERGE WHEN condition subquery must take a read lock"
        );
    }

    #[test]
    fn serializable_read_locks_descend_into_merge_update_assignment_subquery() {
        let snapshot = test_snapshot();
        let plan = merge_plan(
            bool_true(),
            vec![LogicalMergeClause {
                kind: LogicalMergeMatchKind::Matched,
                condition: None,
                action: LogicalMergeAction::Update {
                    assignments: vec![(0, scalar_scan_t())],
                },
            }],
        );
        assert_eq!(
            read_locks(&plan, &snapshot),
            vec![t_relation_tag()],
            "a relation scanned only through a MERGE UPDATE assignment subquery must take a \
             read lock"
        );
    }

    #[test]
    fn serializable_read_locks_descend_into_merge_insert_values_subquery() {
        let snapshot = test_snapshot();
        let plan = merge_plan(
            bool_true(),
            vec![LogicalMergeClause {
                kind: LogicalMergeMatchKind::NotMatched,
                condition: None,
                action: LogicalMergeAction::Insert {
                    columns: vec![0],
                    values: vec![scalar_scan_t()],
                },
            }],
        );
        assert_eq!(
            read_locks(&plan, &snapshot),
            vec![t_relation_tag()],
            "a relation scanned only through a MERGE INSERT VALUES subquery must take a read lock"
        );
    }

    /// A MERGE whose clause expressions embed no subquery and whose source is
    /// table-free must collect no read locks — the descent arm must not
    /// over-collect.
    #[test]
    fn serializable_read_locks_merge_without_subqueries_is_empty() {
        let snapshot = test_snapshot();
        let plan = merge_plan(
            bool_true(),
            vec![LogicalMergeClause {
                kind: LogicalMergeMatchKind::Matched,
                condition: None,
                action: LogicalMergeAction::Delete,
            }],
        );
        assert!(
            read_locks(&plan, &snapshot).is_empty(),
            "subquery-free MERGE over a table-free source needs no predicate lock"
        );
    }
}
