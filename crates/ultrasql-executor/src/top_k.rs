//! Exact top-k retrieval operator.
//!
//! [`TopK`] is the bounded-memory sibling of [`crate::Sort`] for
//! `ORDER BY ... LIMIT k` shapes. It drains the child once, evaluates
//! the order keys per row, keeps only the best `k` annotated rows, then
//! emits those rows in sort order. This is exact retrieval: every input
//! row is considered, but memory is bounded by `k` instead of total
//! cardinality.

use std::cmp::Ordering;

use ultrasql_core::{Schema, Value};
use ultrasql_planner::SortKey;
use ultrasql_vec::Batch;

use crate::eval::Eval;
use crate::filter_op::batch_to_rows;
use crate::seq_scan::build_batch;
use crate::sort::compare_values_nullable;
use crate::{ExecError, Operator};

const BATCH_TARGET_ROWS: usize = 4096;

/// Bounded exact `ORDER BY ... LIMIT k` operator.
///
/// The operator preserves its child's schema and row payloads. Sort keys
/// are evaluated with the row-at-a-time [`Eval`] interpreter, so vector
/// distance expressions use the same semantics as scalar projection and
/// full in-memory sort.
#[derive(Debug)]
pub struct TopK {
    child: Box<dyn Operator>,
    keys: Vec<CompiledKey>,
    schema: Schema,
    cap: usize,
    sorted: Option<std::vec::IntoIter<Vec<Value>>>,
    eof: bool,
}

#[derive(Debug)]
struct CompiledKey {
    eval: Eval,
    asc: bool,
    nulls_first: bool,
}

impl TopK {
    /// Construct a top-k operator.
    ///
    /// `cap` is the maximum number of sorted rows retained and emitted.
    /// A `cap` of zero returns EOF without draining the child.
    #[must_use]
    pub fn new(child: Box<dyn Operator>, keys: Vec<SortKey>, schema: Schema, cap: usize) -> Self {
        let compiled = keys
            .into_iter()
            .map(|k| CompiledKey {
                eval: Eval::new(k.expr),
                asc: k.asc,
                nulls_first: k.nulls_first,
            })
            .collect();
        Self {
            child,
            keys: compiled,
            schema,
            cap,
            sorted: None,
            eof: false,
        }
    }
}

impl Operator for TopK {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }
        if self.cap == 0 {
            self.eof = true;
            return Ok(None);
        }

        if self.sorted.is_none() {
            let mut kept: Vec<(Vec<Value>, Vec<Value>)> = Vec::new();
            loop {
                match self.child.next_batch()? {
                    None => break,
                    Some(batch) => {
                        let rows = batch_to_rows(&batch, &self.schema)?;
                        for row in rows {
                            let key_vals: Vec<Value> = self
                                .keys
                                .iter()
                                .map(|k| k.eval.eval(&row).unwrap_or(Value::Null))
                                .collect();
                            keep_if_top_k(&mut kept, row, key_vals, &self.keys, self.cap);
                        }
                    }
                }
            }

            kept.sort_by(|(_, ak), (_, bk)| compare_key_vecs(ak, bk, &self.keys));
            #[allow(clippy::needless_collect)] // IntoIter needed for stored output cursor.
            let rows: Vec<Vec<Value>> = kept.into_iter().map(|(row, _)| row).collect();
            self.sorted = Some(rows.into_iter());
        }

        let iter = self.sorted.as_mut().expect("just-set above");
        let chunk: Vec<Vec<Value>> = iter.by_ref().take(BATCH_TARGET_ROWS).collect();
        if chunk.is_empty() {
            self.eof = true;
            return Ok(None);
        }
        build_batch(&chunk, &self.schema).map(Some)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn estimated_row_count(&self) -> Option<usize> {
        Some(self.cap)
    }
}

fn keep_if_top_k(
    kept: &mut Vec<(Vec<Value>, Vec<Value>)>,
    row: Vec<Value>,
    key_vals: Vec<Value>,
    keys: &[CompiledKey],
    cap: usize,
) {
    if kept.len() < cap {
        kept.push((row, key_vals));
        return;
    }

    let Some(worst_idx) = worst_index(kept, keys) else {
        return;
    };
    if compare_key_vecs(&key_vals, &kept[worst_idx].1, keys) == Ordering::Less {
        kept[worst_idx] = (row, key_vals);
    }
}

fn worst_index(kept: &[(Vec<Value>, Vec<Value>)], keys: &[CompiledKey]) -> Option<usize> {
    let mut worst = 0usize;
    for idx in 1..kept.len() {
        if compare_key_vecs(&kept[idx].1, &kept[worst].1, keys) == Ordering::Greater {
            worst = idx;
        }
    }
    Some(worst)
}

fn compare_key_vecs(ak: &[Value], bk: &[Value], keys: &[CompiledKey]) -> Ordering {
    for (i, key) in keys.iter().enumerate() {
        let av = &ak[i];
        let bv = &bk[i];
        let ord = compare_values_nullable(av, bv, key.nulls_first);
        let ord = if key.asc { ord } else { ord.reverse() };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}
