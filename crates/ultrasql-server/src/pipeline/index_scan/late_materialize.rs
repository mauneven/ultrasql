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
    let codec = RowCodec::new(shape.table_entry.schema.clone());
    // Index key encoding + columns for the Option-A stale-entry recheck.
    // `None` (an unsupported encoding shape) skips the recheck rather than
    // dropping rows — the safe direction.
    let columns: Vec<usize> = shape
        .index_entry
        .columns
        .iter()
        .map(|&attnum| usize::from(attnum))
        .collect();
    // Only B-tree indexes store the encoded value as the leaf key; skip the
    // value-encoding recheck for other access methods (e.g. hash).
    let key_recheck = if shape
        .index_entry
        .access_method
        .eq_ignore_ascii_case("btree")
    {
        crate::index_key::IndexKeyEncoding::for_columns(&shape.table_entry.schema, &columns)
            .ok()
            .map(|encoding| (encoding, columns))
    } else {
        None
    };
    Ok(Some(Box::new(LateMaterializeScan::new(
        entries,
        codec,
        shape.projected_cols,
        key_recheck,
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
        // Diagnostic counter only; the key recheck (Option-A) is applied by
        // the real `LateMaterializeScan` fetch path. Passing `None` here may
        // over-count a stale-entry candidate as fetched, which only affects
        // the EXPLAIN ANALYZE estimate, never query results.
        if fetch_visible_index_payload(*tid, ctx, None)?.is_some() {
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
    /// `(index key, candidate TID)` pairs in B-tree order. The key drives
    /// the Option-A stale-entry recheck (see [`Self::fetch_visible_payload`]).
    entries: std::vec::IntoIter<(i64, TupleId)>,
    codec: RowCodec,
    projection: Vec<usize>,
    /// The index's key encoding + key columns, used to recompute a resolved
    /// row's key for the Option-A stale-entry recheck (correct for every
    /// supported key type, not just integers). `None` skips the recheck.
    key_recheck: Option<(crate::index_key::IndexKeyEncoding, Vec<usize>)>,
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
            .field("remaining_tids", &self.entries.len())
            .field("projection", &self.projection)
            .field("candidate_tids", &self.candidate_tids)
            .field("fetched_rows", &self.fetched_rows)
            .field("skipped_invisible", &self.skipped_invisible)
            .finish()
    }
}

impl LateMaterializeScan {
    #[allow(
        clippy::too_many_arguments,
        reason = "private constructor; each arg is a distinct field with no natural grouping"
    )]
    fn new(
        entries: Vec<(i64, TupleId)>,
        codec: RowCodec,
        projection: Vec<usize>,
        key_recheck: Option<(crate::index_key::IndexKeyEncoding, Vec<usize>)>,
        output_schema: Schema,
        heap: Arc<HeapAccess<BlankPageLoader>>,
        snapshot: ultrasql_mvcc::Snapshot,
        oracle: Arc<TransactionManager>,
    ) -> Self {
        let candidate_tids = u64::try_from(entries.len()).unwrap_or(u64::MAX);
        Self {
            entries: entries.into_iter(),
            codec,
            projection,
            key_recheck,
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
            let Some((key, tid)) = self.entries.next() else {
                self.eof = true;
                break;
            };
            let Some(payload) = self.fetch_visible_payload(key, tid)? else {
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
        Some(self.entries.len())
    }
}

impl LateMaterializeScan {
    fn fetch_visible_payload(
        &self,
        key: i64,
        tid: TupleId,
    ) -> Result<Option<Vec<u8>>, ultrasql_executor::ExecError> {
        let mut current = tid;
        for _ in 0..64 {
            let tuple = self.heap.fetch(current).map_err(|_| {
                ultrasql_executor::ExecError::Internal("LateMaterializeScan heap fetch failed")
            })?;
            let visibility = is_visible(&tuple.header, &self.snapshot, self.oracle.as_ref());
            match visibility {
                Visibility::Visible => {
                    // Option-A stale-entry recheck: the resolved row must
                    // still carry the index key this entry was stored under
                    // (a key-changing UPDATE leaves the old entry behind).
                    if !self.payload_matches_key(&tuple.data, key) {
                        return Ok(None);
                    }
                    return Ok(Some(tuple.data));
                }
                Visibility::Invisible | Visibility::DeletedByOwn => {
                    if let Some(next) = updated_ctid_target(&tuple.header, current) {
                        current = next;
                        continue;
                    }
                    return Ok(None);
                }
                Visibility::VisibleMaybePreImage => {
                    // Visible with in-place undo history: pre-image when an
                    // earlier writer is invisible to this snapshot, slot
                    // bytes otherwise; recheck the entry key either way.
                    let pre = self
                        .heap
                        .fetch_visible_pre_image(current, &self.snapshot, self.oracle.as_ref())
                        .map_err(|_| {
                            ultrasql_executor::ExecError::Internal(
                                "LateMaterializeScan pre-image fetch failed",
                            )
                        })?;
                    let payload = pre.unwrap_or(tuple.data);
                    if !self.payload_matches_key(&payload, key) {
                        return Ok(None);
                    }
                    return Ok(Some(payload));
                }
                Visibility::VisiblePreImage => {
                    // Surface the pre-image (design §3 R6) so a late-
                    // materialized index scan agrees with a seq scan, after
                    // rechecking it still carries the entry's key.
                    let pre = self
                        .heap
                        .fetch_visible_pre_image(current, &self.snapshot, self.oracle.as_ref())
                        .map_err(|_| {
                            ultrasql_executor::ExecError::Internal(
                                "LateMaterializeScan pre-image fetch failed",
                            )
                        })?;
                    return Ok(match pre {
                        Some(payload) if self.payload_matches_key(&payload, key) => Some(payload),
                        _ => None,
                    });
                }
            }
        }
        Err(ultrasql_executor::ExecError::Internal(
            "LateMaterializeScan update ctid chain exceeded 64 hops",
        ))
    }

    /// `true` iff `payload` decodes to a row whose recomputed index key
    /// equals `key`. A decode/encode failure rejects (safe direction). When
    /// no encoding is wired the recheck is skipped (returns `true`).
    fn payload_matches_key(&self, payload: &[u8], key: i64) -> bool {
        let Some((encoding, columns)) = &self.key_recheck else {
            return true;
        };
        let Ok(row) = self.codec.decode(payload) else {
            return false;
        };
        matches!(
            super::btree_probe::encode_recheck_key(encoding, columns, &row),
            Ok(Some(k)) if k == key
        )
    }
}
