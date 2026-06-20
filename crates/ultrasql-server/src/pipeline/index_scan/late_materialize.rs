//! Late-materialization prototype: a two-phase B-tree TID probe
//! followed by deferred MVCC-visible heap payload fetch.

use std::sync::Arc;

use ultrasql_catalog::{IndexEntry, TableEntry};
use ultrasql_core::{Schema, TupleId, Value};
use ultrasql_executor::{Operator, RowCodec};
use ultrasql_mvcc::{Visibility, is_visible};
use ultrasql_planner::{LogicalPlan, ScalarExpr, SortKey};
use ultrasql_txn::TransactionManager;

use crate::BlankPageLoader;
use crate::error::ServerError;
use ultrasql_storage::heap::HeapAccess;

use super::LowerCtx;
use super::btree_probe::{
    fetch_visible_index_payload, probe_index_entries_ordered, updated_ctid_target,
};
use super::catalog_lookup::{find_single_column_index, key_type_for_btree};
use super::predicate::{IndexKeyRange, match_indexable_predicate};

const LATE_MATERIALIZATION_MIN_TABLE_WIDTH: usize = 8;
const LATE_MATERIALIZATION_MAX_PROJECTED_COLUMNS: usize = 3;

type LateMaterializationProjectShape<'a> =
    (&'a LogicalPlan, &'a [(ScalarExpr, String)], Option<u64>);

/// Counters reported by the late-materialization prototype.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct LateMaterializationSummary {
    /// TIDs emitted by the index probe before visibility checks.
    pub(crate) candidate_tids: u64,
    /// MVCC-visible heap rows fetched by the payload phase.
    pub(crate) fetched_rows: u64,
    /// Candidate TIDs skipped because the heap tuple was not visible.
    pub(crate) skipped_invisible: u64,
    /// Human-readable EXPLAIN note.
    pub(crate) note: String,
}

impl LateMaterializationSummary {
    fn not_applicable(reason: impl Into<String>) -> Self {
        Self {
            note: reason.into(),
            ..Self::default()
        }
    }
}

/// Try to lower `Project(Filter(Scan), payload_cols)` into a two-phase
/// B-tree TID probe followed by deferred heap payload fetch.
pub(crate) fn try_late_materialization_project(
    input: &LogicalPlan,
    exprs: &[(ScalarExpr, String)],
    output_schema: &Schema,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    let Some(shape) = late_materialization_shape(input, exprs, ctx)? else {
        return Ok(None);
    };
    let entries =
        probe_index_entries_ordered(shape.index_entry, shape.range, shape.ascending, ctx)?;
    let tids = entries.into_iter().map(|(_, tid)| tid).collect();
    let codec = RowCodec::new(shape.table_entry.schema.clone());
    Ok(Some(Box::new(LateMaterializeScan::new(
        tids,
        codec,
        shape.projected_cols,
        output_schema.clone(),
        Arc::clone(&ctx.heap),
        ctx.snapshot.clone(),
        Arc::clone(&ctx.oracle),
    ))))
}

/// Return the same counters printed by `EXPLAIN ANALYZE`.
pub(crate) fn late_materialization_summary_for_plan(
    plan: &LogicalPlan,
    ctx: &LowerCtx<'_>,
) -> Result<LateMaterializationSummary, ServerError> {
    let Some((input, exprs, visible_cap)) = late_materialization_project_shape(plan) else {
        return Ok(LateMaterializationSummary::not_applicable(
            "not applicable (no Project(Filter(Scan)) shape)",
        ));
    };
    let Some(shape) = late_materialization_shape(input, exprs, ctx)? else {
        return Ok(LateMaterializationSummary::not_applicable(
            "not selected (shape, index, or projection not eligible)",
        ));
    };
    let entries =
        probe_index_entries_ordered(shape.index_entry, shape.range, shape.ascending, ctx)?;
    let mut fetched_rows = 0_u64;
    let mut skipped_invisible = 0_u64;
    let mut candidate_tids = 0_u64;
    for (_, tid) in &entries {
        candidate_tids = candidate_tids.saturating_add(1);
        if fetch_visible_index_payload(*tid, ctx)?.is_some() {
            fetched_rows = fetched_rows.saturating_add(1);
            if visible_cap.is_some_and(|cap| fetched_rows >= cap) {
                break;
            }
        } else {
            skipped_invisible = skipped_invisible.saturating_add(1);
        }
    }
    Ok(LateMaterializationSummary {
        candidate_tids,
        fetched_rows,
        skipped_invisible,
        note: format!(
            "selected {} on {}: candidates={} fetched={} skipped={} via index TID probe then deferred heap payload fetch",
            shape.index_entry.name,
            shape.table_name,
            candidate_tids,
            fetched_rows,
            skipped_invisible
        ),
    })
}

fn late_materialization_project_shape(
    plan: &LogicalPlan,
) -> Option<LateMaterializationProjectShape<'_>> {
    match plan {
        LogicalPlan::Project { input, exprs, .. } => Some((input, exprs, None)),
        LogicalPlan::Limit { input, n, offset } => {
            let LogicalPlan::Project {
                input: project_input,
                exprs,
                ..
            } = input.as_ref()
            else {
                return None;
            };
            let visible_cap = n.checked_add(*offset).or(Some(u64::MAX));
            Some((project_input, exprs, visible_cap))
        }
        _ => None,
    }
}

struct LateMaterializationShape<'a> {
    table_name: &'a str,
    table_entry: &'a TableEntry,
    index_entry: &'a IndexEntry,
    range: IndexKeyRange,
    projected_cols: Vec<usize>,
    ascending: bool,
}

fn late_materialization_shape<'a>(
    input: &'a LogicalPlan,
    exprs: &[(ScalarExpr, String)],
    ctx: &'a LowerCtx<'_>,
) -> Result<Option<LateMaterializationShape<'a>>, ServerError> {
    if exprs.is_empty() {
        return Ok(None);
    }
    let (input, sort_keys) = match input {
        LogicalPlan::Sort { input, keys } => (input.as_ref(), Some(keys.as_slice())),
        other => (other, None),
    };
    let LogicalPlan::Filter {
        input: filter_input,
        predicate,
    } = input
    else {
        return Ok(None);
    };
    let LogicalPlan::Scan { table, .. } = filter_input.as_ref() else {
        return Ok(None);
    };
    let Some(table_entry) = ctx.catalog_snapshot.tables.get(&table.to_ascii_lowercase()) else {
        return Ok(None);
    };
    let Some((predicate_col_idx, range)) = match_indexable_predicate(predicate) else {
        return Ok(None);
    };
    let Some(index_entry) =
        find_single_column_index(&ctx.catalog_snapshot, table_entry, predicate_col_idx, ctx)
    else {
        return Ok(None);
    };
    let Some(_widen) = key_type_for_btree(table_entry, predicate_col_idx) else {
        return Ok(None);
    };
    let Some(projected_cols) = simple_projected_columns(exprs, table_entry.schema.len()) else {
        return Ok(None);
    };
    if projected_cols.iter().all(|col| *col == predicate_col_idx) {
        return Ok(None);
    }
    if !late_materialization_is_worthwhile(table_entry.schema.len(), projected_cols.len()) {
        return Ok(None);
    }
    let ascending = if let Some(keys) = sort_keys {
        let Some(ascending) = sort_keys_preserve_index_order(keys, predicate_col_idx) else {
            return Ok(None);
        };
        ascending
    } else {
        true
    };
    Ok(Some(LateMaterializationShape {
        table_name: table.as_str(),
        table_entry,
        index_entry,
        range,
        projected_cols,
        ascending,
    }))
}

fn late_materialization_is_worthwhile(table_width: usize, projected_width: usize) -> bool {
    table_width >= LATE_MATERIALIZATION_MIN_TABLE_WIDTH
        && projected_width <= LATE_MATERIALIZATION_MAX_PROJECTED_COLUMNS
        && projected_width.saturating_mul(4) <= table_width
}

fn sort_keys_preserve_index_order(keys: &[SortKey], predicate_col_idx: usize) -> Option<bool> {
    let [key] = keys else {
        return None;
    };
    let ScalarExpr::Column { index, .. } = &key.expr else {
        return None;
    };
    (*index == predicate_col_idx).then_some(key.asc)
}

fn simple_projected_columns(
    exprs: &[(ScalarExpr, String)],
    table_width: usize,
) -> Option<Vec<usize>> {
    let mut projected_cols = Vec::with_capacity(exprs.len());
    for (expr, _) in exprs {
        let ScalarExpr::Column { index, .. } = expr else {
            return None;
        };
        if *index >= table_width {
            return None;
        }
        projected_cols.push(*index);
    }
    Some(projected_cols)
}

struct LateMaterializeScan {
    tids: std::vec::IntoIter<TupleId>,
    codec: RowCodec,
    projection: Vec<usize>,
    output_schema: Schema,
    heap: Arc<HeapAccess<BlankPageLoader>>,
    snapshot: ultrasql_mvcc::Snapshot,
    oracle: Arc<TransactionManager>,
    eof: bool,
    candidate_tids: u64,
    fetched_rows: u64,
    skipped_invisible: u64,
}

impl std::fmt::Debug for LateMaterializeScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LateMaterializeScan")
            .field("remaining_tids", &self.tids.len())
            .field("projection", &self.projection)
            .field("candidate_tids", &self.candidate_tids)
            .field("fetched_rows", &self.fetched_rows)
            .field("skipped_invisible", &self.skipped_invisible)
            .finish()
    }
}

impl LateMaterializeScan {
    fn new(
        tids: Vec<TupleId>,
        codec: RowCodec,
        projection: Vec<usize>,
        output_schema: Schema,
        heap: Arc<HeapAccess<BlankPageLoader>>,
        snapshot: ultrasql_mvcc::Snapshot,
        oracle: Arc<TransactionManager>,
    ) -> Self {
        let candidate_tids = u64::try_from(tids.len()).unwrap_or(u64::MAX);
        Self {
            tids: tids.into_iter(),
            codec,
            projection,
            output_schema,
            heap,
            snapshot,
            oracle,
            eof: false,
            candidate_tids,
            fetched_rows: 0,
            skipped_invisible: 0,
        }
    }
}

impl Operator for LateMaterializeScan {
    fn next_batch(&mut self) -> Result<Option<ultrasql_vec::Batch>, ultrasql_executor::ExecError> {
        if self.eof {
            return Ok(None);
        }
        let mut rows: Vec<Vec<Value>> = Vec::with_capacity(4096);
        while rows.len() < 4096 {
            let Some(tid) = self.tids.next() else {
                self.eof = true;
                break;
            };
            let Some(payload) = self.fetch_visible_payload(tid)? else {
                self.skipped_invisible = self.skipped_invisible.saturating_add(1);
                continue;
            };
            let row = self
                .codec
                .decode_projected(&payload, &self.projection)
                .map_err(|e| ultrasql_executor::ExecError::TypeMismatch(e.to_string()))?;
            self.fetched_rows = self.fetched_rows.saturating_add(1);
            rows.push(row);
        }
        if rows.is_empty() {
            return Ok(None);
        }
        ultrasql_executor::build_batch(&rows, &self.output_schema).map(Some)
    }

    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    fn estimated_row_count(&self) -> Option<usize> {
        Some(self.tids.len())
    }
}

impl LateMaterializeScan {
    fn fetch_visible_payload(
        &self,
        tid: TupleId,
    ) -> Result<Option<Vec<u8>>, ultrasql_executor::ExecError> {
        let mut current = tid;
        for _ in 0..64 {
            let tuple = self.heap.fetch(current).map_err(|_| {
                ultrasql_executor::ExecError::Internal("LateMaterializeScan heap fetch failed")
            })?;
            let visibility = is_visible(&tuple.header, &self.snapshot, self.oracle.as_ref());
            match visibility {
                Visibility::Visible => return Ok(Some(tuple.data)),
                Visibility::Invisible | Visibility::DeletedByOwn => {
                    if let Some(next) = updated_ctid_target(&tuple.header, current) {
                        current = next;
                        continue;
                    }
                    return Ok(None);
                }
                Visibility::VisiblePreImage => return Ok(None),
            }
        }
        Err(ultrasql_executor::ExecError::Internal(
            "LateMaterializeScan update ctid chain exceeded 64 hops",
        ))
    }
}
