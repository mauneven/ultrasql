//! B-tree probing, MVCC-visible heap payload fetch, point-lookup
//! fallback, and BRIN candidate-range scanning.

use std::sync::Arc;

use ultrasql_catalog::{IndexEntry, TableEntry};
use ultrasql_core::{RelationId, TupleId, Value};
use ultrasql_executor::RowCodec;
use ultrasql_mvcc::{InfoMask, TupleHeader, Visibility, is_visible};
use ultrasql_storage::btree::BTree;

use crate::BlankPageLoader;
use crate::error::ServerError;

use super::LowerCtx;
use super::predicate::IndexKeyRange;

/// Probe the B-tree for every tuple satisfying `range` and return the
/// (visible) heap payloads in B-tree-order.
///
/// Visibility is enforced inline: a tuple whose MVCC header is not
/// visible to `ctx.snapshot` under `ctx.oracle` is silently dropped.
/// This means the `IndexScan` operator never sees a tuple a `SeqScan`
/// would hide; the user observes the same row set whether or not the
/// index is consulted.
///
/// # Errors
///
/// Returns [`ServerError::Ddl`] when the B-tree probe or heap fetch
/// fails. The `Ddl` variant carries a dynamic message and is the
/// appropriate channel for runtime storage faults; the simpler
/// `Unsupported` channel is reserved for shape-level rejections that
/// the caller can recover from by falling back to `SeqScan`.
pub(super) fn probe_index(
    index_entry: &IndexEntry,
    range: IndexKeyRange,
    ctx: &LowerCtx<'_>,
) -> Result<Vec<Vec<u8>>, ServerError> {
    probe_index_ordered(index_entry, range, true, ctx)
}

pub(super) fn probe_index_ordered(
    index_entry: &IndexEntry,
    range: IndexKeyRange,
    ascending: bool,
    ctx: &LowerCtx<'_>,
) -> Result<Vec<Vec<u8>>, ServerError> {
    if range.is_empty() {
        ctx.workload_recorder
            .record_index_usage(index_entry.oid.raw(), 0, 0);
        return Ok(Vec::new());
    }
    let entries = probe_index_entries_ordered(index_entry, range, ascending, ctx)?;
    let tuples_read = usize_to_u64_saturating(entries.len());
    let mut payloads = fetch_visible_index_payloads(entries.into_iter().map(|(_, tid)| tid), ctx)?;
    if payloads.is_empty()
        && let (Some(lo), Some(hi)) = (range.low, range.high)
        && lo == hi
    {
        let fallback_limit = if index_entry.is_unique { 1 } else { usize::MAX };
        payloads = fallback_point_payloads(index_entry, lo, fallback_limit, ctx)?;
    }
    ctx.workload_recorder.record_index_usage(
        index_entry.oid.raw(),
        tuples_read,
        usize_to_u64_saturating(payloads.len()),
    );
    Ok(payloads)
}

pub(super) fn probe_index_ordered_limited(
    index_entry: &IndexEntry,
    range: IndexKeyRange,
    ascending: bool,
    limit: usize,
    ctx: &LowerCtx<'_>,
) -> Result<Vec<Vec<u8>>, ServerError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    if range.is_empty() {
        ctx.workload_recorder
            .record_index_usage(index_entry.oid.raw(), 0, 0);
        return Ok(Vec::new());
    }
    let index_rel = RelationId::new(index_entry.oid.raw());
    let pool = ctx.heap.buffer_pool();
    let btree: BTree<BlankPageLoader> =
        BTree::open(Arc::clone(pool), index_rel, index_entry.root_block);
    let mut payloads = Vec::new();
    let mut tuples_read = 0_u64;

    match (range.low, range.high, ascending) {
        (Some(lo), Some(hi), true) if lo == hi => {
            if index_entry.is_unique {
                if let Some(tid) = btree
                    .lookup::<i64>(lo)
                    .map_err(|e| ServerError::ddl(format!("IndexScan btree lookup: {e}")))?
                {
                    tuples_read = tuples_read.saturating_add(1);
                    push_visible_index_payload(&mut payloads, tid, ctx, limit)?;
                }
            } else {
                for tid in btree
                    .lookup_all::<i64>(lo)
                    .map_err(|e| ServerError::ddl(format!("IndexScan btree lookup: {e}")))?
                {
                    tuples_read = tuples_read.saturating_add(1);
                    if push_visible_index_payload(&mut payloads, tid, ctx, limit)? {
                        break;
                    }
                }
            }
            if payloads.is_empty() {
                payloads = fallback_point_payloads(index_entry, lo, limit, ctx)?;
            }
        }
        (low, high, true) => {
            let start = low.unwrap_or(i64::MIN);
            let end_exclusive = high.and_then(|h| h.checked_add(1));
            for entry in btree.range_scan::<i64>(start, end_exclusive) {
                let (_key, tid) =
                    entry.map_err(|e| ServerError::ddl(format!("IndexScan btree scan: {e}")))?;
                tuples_read = tuples_read.saturating_add(1);
                if push_visible_index_payload(&mut payloads, tid, ctx, limit)? {
                    break;
                }
            }
        }
        (low, high, false) => {
            let start = high.unwrap_or(i64::MAX);
            let end = low;
            for entry in btree
                .backward_scan::<i64>(start, end)
                .map_err(|e| ServerError::ddl(format!("IndexScan btree backward scan: {e}")))?
            {
                let (_key, tid) = entry
                    .map_err(|e| ServerError::ddl(format!("IndexScan btree backward scan: {e}")))?;
                tuples_read = tuples_read.saturating_add(1);
                if push_visible_index_payload(&mut payloads, tid, ctx, limit)? {
                    break;
                }
            }
        }
    }
    ctx.workload_recorder.record_index_usage(
        index_entry.oid.raw(),
        tuples_read,
        usize_to_u64_saturating(payloads.len()),
    );
    Ok(payloads)
}

fn fallback_point_payloads(
    index_entry: &IndexEntry,
    key: i64,
    limit: usize,
    ctx: &LowerCtx<'_>,
) -> Result<Vec<Vec<u8>>, ServerError> {
    let Some(&attnum) = index_entry.columns.first() else {
        return Ok(Vec::new());
    };
    let Some(table_entry) = ctx
        .catalog_snapshot
        .tables
        .values()
        .find(|entry| entry.oid == index_entry.table_oid)
    else {
        return Ok(Vec::new());
    };
    let col_idx = usize::from(attnum);
    if col_idx >= table_entry.schema.len() {
        return Ok(Vec::new());
    }
    let codec = RowCodec::new(table_entry.schema.clone());
    let rel = RelationId(table_entry.oid);
    let block_count = ctx.heap.block_count(rel).max(table_entry.n_blocks);
    let mut walker =
        ctx.heap
            .scan_visible_walker(rel, block_count, &ctx.snapshot, ctx.oracle.as_ref());
    let mut payloads = Vec::new();
    while let Some((_tid, _header, payload)) = walker
        .try_next()
        .map_err(|e| ServerError::ddl(format!("IndexScan fallback heap scan: {e}")))?
    {
        let row = codec
            .decode(payload)
            .map_err(|e| ServerError::ddl(format!("IndexScan fallback row decode: {e}")))?;
        if row
            .get(col_idx)
            .is_some_and(|value| value_matches_i64(value, key))
        {
            payloads.push(payload.to_vec());
            if payloads.len() >= limit {
                break;
            }
        }
    }
    Ok(payloads)
}

fn value_matches_i64(value: &Value, key: i64) -> bool {
    match value {
        Value::Int16(v) => i64::from(*v) == key,
        Value::Int32(v) => i64::from(*v) == key,
        Value::Int64(v) => *v == key,
        _ => false,
    }
}

pub(crate) fn probe_index_entries_ordered(
    index_entry: &IndexEntry,
    range: IndexKeyRange,
    ascending: bool,
    ctx: &LowerCtx<'_>,
) -> Result<Vec<(i64, TupleId)>, ServerError> {
    if range.is_empty() {
        return Ok(Vec::new());
    }
    let index_rel = RelationId::new(index_entry.oid.raw());
    let pool = ctx.heap.buffer_pool();
    let btree: BTree<BlankPageLoader> =
        BTree::open(Arc::clone(pool), index_rel, index_entry.root_block);

    // Collect the matching TupleIds. A point lookup uses the cheap
    // `lookup` path; everything else walks the leaf chain via
    // `range_scan` between `[low, high+1)` (half-open). `range_scan`'s
    // upper bound is exclusive, so we add 1 to `high` to keep the
    // inclusive contract — overflowing to `None` (i.e., scan to the
    // end of the leaf chain) when `high == i64::MAX`.
    let mut entries_out: Vec<(i64, TupleId)> = Vec::new();
    match (range.low, range.high, ascending) {
        (Some(lo), Some(hi), true) if lo == hi => {
            if index_entry.is_unique {
                if let Some(tid) = btree
                    .lookup::<i64>(lo)
                    .map_err(|e| ServerError::ddl(format!("IndexScan btree lookup: {e}")))?
                {
                    entries_out.push((lo, tid));
                }
            } else {
                for tid in btree
                    .lookup_all::<i64>(lo)
                    .map_err(|e| ServerError::ddl(format!("IndexScan btree lookup: {e}")))?
                {
                    entries_out.push((lo, tid));
                }
            }
        }
        (low, high, true) => {
            // Walk the half-open `[start, end_exclusive)`. `start =
            // low.unwrap_or(i64::MIN)` and `end_exclusive =
            // high.map(|h| h.checked_add(1))` — when the +1 overflows we
            // pass `None` to mean "scan to the end of the leaf chain".
            let start = low.unwrap_or(i64::MIN);
            // `i64::MAX + 1` overflows to `None`, which `range_scan`
            // treats as "unbounded above" — exactly the contract we want.
            let end_exclusive: Option<i64> = high.and_then(|h| h.checked_add(1));
            for entry in btree.range_scan::<i64>(start, end_exclusive) {
                let (key, tid) =
                    entry.map_err(|e| ServerError::ddl(format!("IndexScan btree scan: {e}")))?;
                entries_out.push((key, tid));
            }
        }
        (low, high, false) => {
            let start = high.unwrap_or(i64::MAX);
            let end = low;
            for entry in btree
                .backward_scan::<i64>(start, end)
                .map_err(|e| ServerError::ddl(format!("IndexScan btree backward scan: {e}")))?
            {
                let (key, tid) = entry
                    .map_err(|e| ServerError::ddl(format!("IndexScan btree backward scan: {e}")))?;
                entries_out.push((key, tid));
            }
        }
    }
    Ok(entries_out)
}

fn fetch_visible_index_payloads<I>(tids: I, ctx: &LowerCtx<'_>) -> Result<Vec<Vec<u8>>, ServerError>
where
    I: IntoIterator<Item = TupleId>,
{
    // Fetch the heap tuples in B-tree order and apply MVCC visibility
    // inline. An index entry whose heap tuple is invisible to the
    // statement's snapshot is silently dropped — the same outcome a
    // SeqScan would deliver. We use [`HeapAccess::fetch`] (no
    // visibility check) plus an explicit `is_visible` call rather than
    // chaining through `scan_visible` because the latter walks a
    // block-by-block iterator we cannot project onto an arbitrary
    // TupleId list.
    let iter = tids.into_iter();
    let (lower, _) = iter.size_hint();
    let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(lower);
    for tid in iter {
        if let Some(payload) = fetch_visible_index_payload(tid, ctx)? {
            payloads.push(payload);
        }
    }
    Ok(payloads)
}

fn push_visible_index_payload(
    payloads: &mut Vec<Vec<u8>>,
    tid: TupleId,
    ctx: &LowerCtx<'_>,
    limit: usize,
) -> Result<bool, ServerError> {
    if let Some(payload) = fetch_visible_index_payload(tid, ctx)? {
        payloads.push(payload);
    }
    Ok(payloads.len() >= limit)
}

pub(super) fn fetch_visible_index_payload(
    tid: TupleId,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Vec<u8>>, ServerError> {
    let mut current = tid;
    for _ in 0..64 {
        let tuple = ctx
            .heap
            .fetch(current)
            .map_err(|e| ServerError::ddl(format!("IndexScan heap fetch: {e}")))?;
        let visibility = is_visible(&tuple.header, &ctx.snapshot, ctx.oracle.as_ref());
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
    Err(ServerError::ddl(
        "IndexScan heap fetch: update ctid chain exceeded 64 hops",
    ))
}

pub(super) fn updated_ctid_target(header: &TupleHeader, current: TupleId) -> Option<TupleId> {
    if header.ctid == current {
        return None;
    }
    let redirects = header.infomask.contains(InfoMask::UPDATED)
        || header.infomask.contains(InfoMask::HOT_UPDATED);
    redirects.then_some(header.ctid)
}

pub(super) fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub(super) fn scan_brin_candidate_ranges(
    table_entry: &TableEntry,
    ranges: &[(u32, u32)],
    ctx: &LowerCtx<'_>,
) -> Result<Vec<Vec<u8>>, ServerError> {
    let table_rel = RelationId(table_entry.oid);
    let block_count = ctx.heap.block_count(table_rel).max(table_entry.n_blocks);
    let ranges = normalize_brin_ranges(ranges, block_count);
    let mut payloads = Vec::new();
    for (start_block, end_block_inclusive) in ranges {
        let end_exclusive = end_block_inclusive.saturating_add(1);
        let mut walker = ctx.heap.scan_visible_walker_range_with_vm(
            table_rel,
            start_block,
            end_exclusive,
            &ctx.snapshot,
            ctx.oracle.as_ref(),
            ctx.vm.as_ref(),
        );
        while let Some((_tid, _header, payload)) = walker
            .try_next()
            .map_err(|e| ServerError::ddl(format!("BRIN heap range scan: {e}")))?
        {
            payloads.push(payload.to_vec());
        }
    }
    Ok(payloads)
}

fn normalize_brin_ranges(ranges: &[(u32, u32)], block_count: u32) -> Vec<(u32, u32)> {
    if block_count == 0 {
        return Vec::new();
    }
    let last_block = block_count - 1;
    let mut ranges: Vec<(u32, u32)> = ranges
        .iter()
        .filter_map(|(start, end)| {
            if *start > last_block {
                return None;
            }
            let end = (*end).min(last_block);
            if *start > end {
                return None;
            }
            Some((*start, end))
        })
        .collect();
    ranges.sort_unstable_by_key(|(start, _)| *start);
    let mut merged: Vec<(u32, u32)> = Vec::with_capacity(ranges.len());
    for (start, end) in ranges {
        if let Some((_, current_end)) = merged.last_mut()
            && start <= current_end.saturating_add(1)
        {
            *current_end = (*current_end).max(end);
            continue;
        }
        merged.push((start, end));
    }
    merged
}
