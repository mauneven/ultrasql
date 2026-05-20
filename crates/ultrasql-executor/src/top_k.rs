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
use ultrasql_planner::{BinaryOp, ScalarExpr, SortKey};
use ultrasql_vec::Batch;
use ultrasql_vec::kernels::vector::{VectorMetric, exact_top_k_f32};

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
    exact_vector_key: Option<ExactVectorTopKKey>,
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

#[derive(Clone, Debug)]
struct ExactVectorTopKKey {
    column_idx: usize,
    probe: Vec<f32>,
    metric: VectorMetric,
}

impl TopK {
    /// Construct a top-k operator.
    ///
    /// `cap` is the maximum number of sorted rows retained and emitted.
    /// A `cap` of zero returns EOF without draining the child.
    #[must_use]
    pub fn new(child: Box<dyn Operator>, keys: Vec<SortKey>, schema: Schema, cap: usize) -> Self {
        let exact_vector_key = match_exact_vector_top_k_key(&keys);
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
            exact_vector_key,
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
                        drain_top_k_batch(
                            &mut kept,
                            rows,
                            &self.keys,
                            self.exact_vector_key.as_ref(),
                            self.cap,
                        );
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

fn drain_top_k_batch(
    kept: &mut Vec<(Vec<Value>, Vec<Value>)>,
    rows: Vec<Vec<Value>>,
    keys: &[CompiledKey],
    exact_vector_key: Option<&ExactVectorTopKKey>,
    cap: usize,
) {
    if let Some(exact) = exact_vector_key
        && drain_exact_vector_top_k_batch(kept, &rows, keys, exact, cap)
    {
        return;
    }
    drain_generic_top_k_batch(kept, rows, keys, cap);
}

fn drain_generic_top_k_batch(
    kept: &mut Vec<(Vec<Value>, Vec<Value>)>,
    rows: Vec<Vec<Value>>,
    keys: &[CompiledKey],
    cap: usize,
) {
    for row in rows {
        let key_vals: Vec<Value> = keys
            .iter()
            .map(|k| k.eval.eval(&row).unwrap_or(Value::Null))
            .collect();
        keep_if_top_k(kept, row, key_vals, keys, cap);
    }
}

fn drain_exact_vector_top_k_batch(
    kept: &mut Vec<(Vec<Value>, Vec<Value>)>,
    rows: &[Vec<Value>],
    keys: &[CompiledKey],
    exact: &ExactVectorTopKKey,
    cap: usize,
) -> bool {
    let mut vectors: Vec<&[f32]> = Vec::with_capacity(rows.len());
    for row in rows {
        let Some(Value::Vector(vector)) = row.get(exact.column_idx) else {
            return false;
        };
        vectors.push(vector);
    }
    for hit in exact_top_k_f32(&vectors, &exact.probe, exact.metric, cap) {
        let row = rows[hit.row].clone();
        let key_vals = vec![Value::Float64(f64::from(hit.distance))];
        keep_if_top_k(kept, row, key_vals, keys, cap);
    }
    true
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

fn match_exact_vector_top_k_key(keys: &[SortKey]) -> Option<ExactVectorTopKKey> {
    let [key] = keys else {
        return None;
    };
    if !key.asc || key.nulls_first {
        return None;
    }
    let ScalarExpr::Binary {
        op, left, right, ..
    } = &key.expr
    else {
        return None;
    };
    let metric = vector_metric_for_op(*op)?;
    vector_column_probe(left, right, metric).or_else(|| vector_column_probe(right, left, metric))
}

fn vector_metric_for_op(op: BinaryOp) -> Option<VectorMetric> {
    match op {
        BinaryOp::VectorL2Distance => Some(VectorMetric::L2),
        BinaryOp::VectorCosineDistance => Some(VectorMetric::Cosine),
        BinaryOp::VectorNegativeInnerProduct => Some(VectorMetric::NegativeInnerProduct),
        BinaryOp::VectorL1Distance => Some(VectorMetric::L1),
        _ => None,
    }
}

fn vector_column_probe(
    column: &ScalarExpr,
    probe: &ScalarExpr,
    metric: VectorMetric,
) -> Option<ExactVectorTopKKey> {
    let ScalarExpr::Column {
        index,
        data_type: ultrasql_core::DataType::Vector { .. },
        ..
    } = column
    else {
        return None;
    };
    let ScalarExpr::Literal {
        value: Value::Vector(values),
        ..
    } = probe
    else {
        return None;
    };
    Some(ExactVectorTopKKey {
        column_idx: *index,
        probe: values.clone(),
        metric,
    })
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Value};
    use ultrasql_planner::{BinaryOp, ScalarExpr, SortKey};
    use ultrasql_vec::kernels::vector::VectorMetric;

    use super::match_exact_vector_top_k_key;

    #[test]
    fn exact_vector_top_k_key_matches_single_ascending_distance_key() {
        let key = SortKey {
            expr: ScalarExpr::Binary {
                op: BinaryOp::VectorL2Distance,
                left: Box::new(ScalarExpr::Column {
                    name: "embedding".to_owned(),
                    index: 2,
                    data_type: DataType::Vector { dims: Some(3) },
                }),
                right: Box::new(ScalarExpr::Literal {
                    value: Value::Vector(vec![1.0, 2.0, 3.0]),
                    data_type: DataType::Vector { dims: Some(3) },
                }),
                data_type: DataType::Float64,
            },
            asc: true,
            nulls_first: false,
        };

        let matched = match_exact_vector_top_k_key(&[key]).expect("fast path should match");
        assert_eq!(matched.column_idx, 2);
        assert_eq!(matched.probe, vec![1.0, 2.0, 3.0]);
        assert_eq!(matched.metric, VectorMetric::L2);
    }

    #[test]
    fn exact_vector_top_k_key_declines_descending_or_nullable_shape() {
        let expr = ScalarExpr::Binary {
            op: BinaryOp::VectorCosineDistance,
            left: Box::new(ScalarExpr::Column {
                name: "embedding".to_owned(),
                index: 1,
                data_type: DataType::Vector { dims: Some(2) },
            }),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Vector(vec![1.0, 0.0]),
                data_type: DataType::Vector { dims: Some(2) },
            }),
            data_type: DataType::Float64,
        };

        let desc = SortKey {
            expr: expr.clone(),
            asc: false,
            nulls_first: false,
        };
        let nulls_first = SortKey {
            expr,
            asc: true,
            nulls_first: true,
        };

        assert!(match_exact_vector_top_k_key(&[desc]).is_none());
        assert!(match_exact_vector_top_k_key(&[nulls_first]).is_none());
    }
}
