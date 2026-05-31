//! Exact top-k retrieval operator.
//!
//! [`TopK`] is the bounded-memory sibling of [`crate::Sort`] for
//! `ORDER BY ... LIMIT k` shapes. It drains the child once, evaluates
//! the order keys per row, keeps only the best `k` annotated rows, then
//! emits those rows in sort order. This is exact retrieval: every input
//! row is considered, but memory is bounded by `k` instead of total
//! cardinality. With a finite [`crate::WorkMemBudget`], generic top-k routes
//! through the spillable [`crate::Sort`] path; exact dense-vector top-k stays
//! on the bounded kernel path so `ORDER BY embedding <op> probe LIMIT k` never
//! turns back into a full physical sort.

use std::cmp::Ordering;
use std::sync::Arc;

use ultrasql_core::{Schema, Value};
use ultrasql_planner::{BinaryOp, ScalarExpr, SortKey};
use ultrasql_vec::Batch;
use ultrasql_vec::kernels::vector::{VectorMetric, exact_top_k_f32};

use crate::eval::Eval;
use crate::filter_op::batch_to_rows;
use crate::seq_scan::build_batch;
use crate::sort::{Sort, try_compare_values_nullable};
use crate::work_mem::WorkMemBudget;
use crate::{ExecError, Operator, OperatorSpillProfile};

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
    original_keys: Vec<SortKey>,
    keys: Vec<CompiledKey>,
    exact_vector_key: Option<ExactVectorTopKKey>,
    schema: Schema,
    cap: usize,
    presorted_input: bool,
    presorted_emitted: usize,
    work_mem: Option<Arc<WorkMemBudget>>,
    spilled_to_disk: bool,
    exact_vector_kernel_batches: usize,
    exact_vector_generic_fallback_batches: usize,
    spill_delegate: Option<TopKSpillDelegate>,
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
            .iter()
            .cloned()
            .map(|k| CompiledKey {
                eval: Eval::new(k.expr),
                asc: k.asc,
                nulls_first: k.nulls_first,
            })
            .collect();
        Self {
            child,
            original_keys: keys,
            keys: compiled,
            exact_vector_key,
            schema,
            cap,
            presorted_input: false,
            presorted_emitted: 0,
            work_mem: None,
            spilled_to_disk: false,
            exact_vector_kernel_batches: 0,
            exact_vector_generic_fallback_batches: 0,
            spill_delegate: None,
            sorted: None,
            eof: false,
        }
    }

    /// Construct a top-k operator over an input that is already sorted in
    /// the requested order.
    ///
    /// This is the adaptive early-stop path for plans that discover a
    /// cheaper ordered source at execution time, such as an index scan. The
    /// operator emits at most `cap` rows and stops pulling its child as soon
    /// as that cap is reached.
    #[must_use]
    pub fn new_presorted(child: Box<dyn Operator>, schema: Schema, cap: usize) -> Self {
        Self {
            child,
            original_keys: Vec::new(),
            keys: Vec::new(),
            exact_vector_key: None,
            schema,
            cap,
            presorted_input: true,
            presorted_emitted: 0,
            work_mem: None,
            spilled_to_disk: false,
            exact_vector_kernel_batches: 0,
            exact_vector_generic_fallback_batches: 0,
            spill_delegate: None,
            sorted: None,
            eof: false,
        }
    }

    /// Attach a per-query work-memory budget.
    #[must_use]
    pub fn with_work_mem_budget(mut self, budget: Arc<WorkMemBudget>) -> Self {
        self.work_mem = Some(budget);
        self
    }

    /// Whether this execution routed through the spillable sort path.
    #[must_use]
    pub const fn spilled_to_disk(&self) -> bool {
        self.spilled_to_disk
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
        if self.presorted_input {
            return self.next_presorted_batch();
        }
        if self.spill_delegate.is_some() {
            return self.next_spill_delegate_batch();
        }
        if self.should_use_spill_delegate() {
            self.install_spill_delegate();
            return self.next_spill_delegate_batch();
        }

        if self.sorted.is_none() {
            let mut kept: Vec<(Vec<Value>, Vec<Value>)> = Vec::new();
            loop {
                match self.child.next_batch()? {
                    None => break,
                    Some(batch) => {
                        let rows = batch_to_rows(&batch, &self.schema)?;
                        match drain_top_k_batch(
                            &mut kept,
                            rows,
                            &self.keys,
                            self.exact_vector_key.as_ref(),
                            self.cap,
                        )? {
                            TopKDrainMode::ExactVector => {
                                self.exact_vector_kernel_batches =
                                    self.exact_vector_kernel_batches.saturating_add(1);
                            }
                            TopKDrainMode::GenericFallback => {
                                self.exact_vector_generic_fallback_batches =
                                    self.exact_vector_generic_fallback_batches.saturating_add(1);
                            }
                            TopKDrainMode::Generic => {}
                        }
                    }
                }
            }

            kept.sort_by(|(_, ak), (_, bk)| compare_key_vecs(ak, bk, &self.keys));
            #[allow(clippy::needless_collect)] // IntoIter needed for stored output cursor.
            let rows: Vec<Vec<Value>> = kept.into_iter().map(|(row, _)| row).collect();
            self.sorted = Some(rows.into_iter());
        }

        let iter = self
            .sorted
            .as_mut()
            .ok_or(ExecError::Internal("top-k output iterator missing"))?;
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

    fn profile_children(&self) -> Vec<&dyn Operator> {
        if let Some(delegate) = &self.spill_delegate {
            delegate.sort.profile_children()
        } else {
            vec![self.child.as_ref()]
        }
    }

    fn spill_profile(&self) -> OperatorSpillProfile {
        self.spill_delegate
            .as_ref()
            .map_or_else(OperatorSpillProfile::default, |delegate| {
                delegate.sort.spill_profile()
            })
    }

    fn io_bytes(&self) -> u64 {
        self.spill_delegate
            .as_ref()
            .map_or(0, |delegate| delegate.sort.io_bytes())
    }

    fn pruning_stats(&self) -> Vec<String> {
        if self.exact_vector_key.is_none() {
            return Vec::new();
        }
        vec![format!(
            "kernel=exact_top_k_f32,full_sort=false,exact_batches={},generic_fallback_batches={}",
            self.exact_vector_kernel_batches, self.exact_vector_generic_fallback_batches
        )]
    }
}

impl TopK {
    fn should_use_spill_delegate(&self) -> bool {
        if self.exact_vector_key.is_some() {
            return false;
        }
        self.work_mem
            .as_ref()
            .is_some_and(|budget| budget.limit_bytes() != u64::MAX)
    }

    fn install_spill_delegate(&mut self) {
        let child = std::mem::replace(
            &mut self.child,
            Box::new(EmptyTopKChild {
                schema: self.schema.clone(),
            }),
        );
        let mut sort = Sort::new(child, self.original_keys.clone(), self.schema.clone());
        if let Some(budget) = &self.work_mem {
            sort = sort.with_work_mem_budget(Arc::clone(budget));
        }
        self.spill_delegate = Some(TopKSpillDelegate {
            sort,
            remaining: self.cap,
        });
    }

    fn next_spill_delegate_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        let Some(delegate) = self.spill_delegate.as_mut() else {
            return Err(ExecError::Internal("top-k spill delegate missing"));
        };
        if delegate.remaining == 0 {
            self.spilled_to_disk |= delegate.sort.spilled_to_disk();
            self.eof = true;
            return Ok(None);
        }
        let Some(batch) = delegate.sort.next_batch()? else {
            self.spilled_to_disk |= delegate.sort.spilled_to_disk();
            self.eof = true;
            return Ok(None);
        };
        self.spilled_to_disk |= delegate.sort.spilled_to_disk();
        let mut rows = batch_to_rows(&batch, &self.schema)?;
        let take = rows.len().min(delegate.remaining);
        rows.truncate(take);
        delegate.remaining -= take;
        if rows.is_empty() {
            self.eof = true;
            return Ok(None);
        }
        build_batch(&rows, &self.schema).map(Some)
    }

    fn next_presorted_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.presorted_emitted >= self.cap {
            self.eof = true;
            return Ok(None);
        }

        let mut chunk: Vec<Vec<Value>> = Vec::new();
        while chunk.len() < BATCH_TARGET_ROWS && self.presorted_emitted < self.cap {
            let Some(batch) = self.child.next_batch()? else {
                break;
            };
            let remaining = self.cap - self.presorted_emitted;
            let rows = batch_to_rows(&batch, &self.schema)?;
            for row in rows.into_iter().take(remaining) {
                chunk.push(row);
                self.presorted_emitted += 1;
                if chunk.len() == BATCH_TARGET_ROWS || self.presorted_emitted == self.cap {
                    break;
                }
            }
        }

        if chunk.is_empty() {
            self.eof = true;
            return Ok(None);
        }
        build_batch(&chunk, &self.schema).map(Some)
    }
}

#[derive(Debug)]
struct EmptyTopKChild {
    schema: Schema,
}

#[derive(Debug)]
struct TopKSpillDelegate {
    sort: Sort,
    remaining: usize,
}

impl Operator for EmptyTopKChild {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        Ok(None)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TopKDrainMode {
    ExactVector,
    GenericFallback,
    Generic,
}

fn drain_top_k_batch(
    kept: &mut Vec<(Vec<Value>, Vec<Value>)>,
    rows: Vec<Vec<Value>>,
    keys: &[CompiledKey],
    exact_vector_key: Option<&ExactVectorTopKKey>,
    cap: usize,
) -> Result<TopKDrainMode, ExecError> {
    if let Some(exact) = exact_vector_key {
        if drain_exact_vector_top_k_batch(kept, &rows, keys, exact, cap)? {
            return Ok(TopKDrainMode::ExactVector);
        }
    }
    let mode = if exact_vector_key.is_some() {
        TopKDrainMode::GenericFallback
    } else {
        TopKDrainMode::Generic
    };
    drain_generic_top_k_batch(kept, rows, keys, cap)?;
    Ok(mode)
}

fn drain_generic_top_k_batch(
    kept: &mut Vec<(Vec<Value>, Vec<Value>)>,
    rows: Vec<Vec<Value>>,
    keys: &[CompiledKey],
    cap: usize,
) -> Result<(), ExecError> {
    for row in rows {
        let key_vals: Vec<Value> = keys
            .iter()
            .map(|k| {
                k.eval
                    .eval(&row)
                    .map_err(|err| ExecError::TypeMismatch(err.to_string()))
            })
            .collect::<Result<_, _>>()?;
        keep_if_top_k(kept, row, key_vals, keys, cap)?;
    }
    Ok(())
}

fn drain_exact_vector_top_k_batch(
    kept: &mut Vec<(Vec<Value>, Vec<Value>)>,
    rows: &[Vec<Value>],
    keys: &[CompiledKey],
    exact: &ExactVectorTopKKey,
    cap: usize,
) -> Result<bool, ExecError> {
    let mut vectors: Vec<&[f32]> = Vec::with_capacity(rows.len());
    for row in rows {
        let Some(Value::Vector(vector) | Value::HalfVec(vector)) = row.get(exact.column_idx) else {
            return Ok(false);
        };
        vectors.push(vector);
    }
    for hit in exact_top_k_f32(&vectors, &exact.probe, exact.metric, cap) {
        let row = rows[hit.row].clone();
        let key_vals = vec![Value::Float64(f64::from(hit.distance))];
        keep_if_top_k(kept, row, key_vals, keys, cap)?;
    }
    Ok(true)
}

fn keep_if_top_k(
    kept: &mut Vec<(Vec<Value>, Vec<Value>)>,
    row: Vec<Value>,
    key_vals: Vec<Value>,
    keys: &[CompiledKey],
    cap: usize,
) -> Result<(), ExecError> {
    validate_key_values(kept, &key_vals, keys)?;
    if kept.len() < cap {
        kept.push((row, key_vals));
        return Ok(());
    }

    let Some(worst_idx) = worst_index(kept, keys)? else {
        return Ok(());
    };
    if compare_key_vecs_checked(&key_vals, &kept[worst_idx].1, keys)? == Ordering::Less {
        kept[worst_idx] = (row, key_vals);
    }
    Ok(())
}

fn worst_index(
    kept: &[(Vec<Value>, Vec<Value>)],
    keys: &[CompiledKey],
) -> Result<Option<usize>, ExecError> {
    let mut worst = 0usize;
    for idx in 1..kept.len() {
        if compare_key_vecs_checked(&kept[idx].1, &kept[worst].1, keys)? == Ordering::Greater {
            worst = idx;
        }
    }
    Ok(Some(worst))
}

fn compare_key_vecs(ak: &[Value], bk: &[Value], keys: &[CompiledKey]) -> Ordering {
    compare_key_vecs_checked(ak, bk, keys).unwrap_or(Ordering::Equal)
}

fn compare_key_vecs_checked(
    ak: &[Value],
    bk: &[Value],
    keys: &[CompiledKey],
) -> Result<Ordering, ExecError> {
    for (i, key) in keys.iter().enumerate() {
        let av = &ak[i];
        let bv = &bk[i];
        let ord = try_compare_values_nullable(av, bv, key.nulls_first)?;
        let ord = if key.asc { ord } else { ord.reverse() };
        if ord != Ordering::Equal {
            return Ok(ord);
        }
    }
    Ok(Ordering::Equal)
}

fn validate_key_values(
    kept: &[(Vec<Value>, Vec<Value>)],
    key_vals: &[Value],
    keys: &[CompiledKey],
) -> Result<(), ExecError> {
    for (idx, value) in key_vals.iter().enumerate() {
        if !value.is_null() {
            try_compare_values_nullable(value, value, keys[idx].nulls_first)?;
        }
        if let Some(first) = kept
            .iter()
            .filter_map(|(_, existing)| existing.get(idx))
            .find(|existing| !existing.is_null())
        {
            try_compare_values_nullable(first, value, keys[idx].nulls_first)?;
        }
    }
    Ok(())
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
        data_type: ultrasql_core::DataType::Vector { .. } | ultrasql_core::DataType::HalfVec { .. },
        ..
    } = column
    else {
        return None;
    };
    let ScalarExpr::Literal {
        value: Value::Vector(values) | Value::HalfVec(values),
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
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use ultrasql_core::{DataType, Value};
    use ultrasql_core::{Field, Schema};
    use ultrasql_planner::{BinaryOp, ScalarExpr, SortKey};
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};
    use ultrasql_vec::kernels::vector::VectorMetric;

    use super::TopK;
    use super::match_exact_vector_top_k_key;
    use crate::{ExecError, Operator, WorkMemBudget};

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

    #[test]
    fn presorted_top_k_stops_after_cap_without_draining_child() {
        let schema = Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok");
        let pulls = Arc::new(AtomicUsize::new(0));
        let child = CountingScan {
            schema: schema.clone(),
            batches: vec![i32_batch(&[1, 2]), i32_batch(&[3, 4]), i32_batch(&[5, 6])],
            next: 0,
            pulls: Arc::clone(&pulls),
        };
        let mut top_k = TopK::new_presorted(Box::new(child), schema.clone(), 3);
        let mut rows = Vec::new();
        while let Some(batch) = top_k.next_batch().expect("top-k ok") {
            let decoded = crate::filter_op::batch_to_rows(&batch, top_k.schema()).expect("decode");
            rows.extend(decoded.into_iter().map(|row| row[0].clone()));
        }

        assert_eq!(
            rows,
            vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]
        );
        assert_eq!(
            pulls.load(Ordering::SeqCst),
            2,
            "presorted top-k must stop as soon as cap rows are available"
        );
    }

    #[test]
    fn generic_top_k_key_eval_error_propagates() {
        let schema = Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok");
        let child = CountingScan {
            schema: schema.clone(),
            batches: vec![i32_batch(&[1, 2, 3])],
            next: 0,
            pulls: Arc::new(AtomicUsize::new(0)),
        };
        let key = SortKey {
            expr: ScalarExpr::Binary {
                op: BinaryOp::Div,
                left: Box::new(ScalarExpr::Column {
                    name: "id".to_owned(),
                    index: 0,
                    data_type: DataType::Int32,
                }),
                right: Box::new(ScalarExpr::Literal {
                    value: Value::Int32(0),
                    data_type: DataType::Int32,
                }),
                data_type: DataType::Int32,
            },
            asc: true,
            nulls_first: false,
        };
        let mut top_k = TopK::new(Box::new(child), vec![key], schema, 2);
        let err = top_k
            .next_batch()
            .expect_err("top-k key error must surface");
        assert!(
            err.to_string().contains("division by zero"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn generic_top_k_rejects_unsupported_order_value() {
        let schema = Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok");
        let child = CountingScan {
            schema: schema.clone(),
            batches: vec![i32_batch(&[1, 2, 3])],
            next: 0,
            pulls: Arc::new(AtomicUsize::new(0)),
        };
        let key = SortKey {
            expr: ScalarExpr::Literal {
                value: Value::Array {
                    element_type: DataType::Int32,
                    elements: vec![Value::Int32(1)],
                },
                data_type: DataType::Array(Box::new(DataType::Int32)),
            },
            asc: true,
            nulls_first: false,
        };
        let mut top_k = TopK::new(Box::new(child), vec![key], schema, 2);
        let err = top_k
            .next_batch()
            .expect_err("unsupported top-k key must surface");
        assert!(
            err.to_string().contains("not orderable"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn generic_top_k_spills_to_disk_when_work_mem_is_too_small() {
        let schema = Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("embedding", DataType::Vector { dims: Some(2) }),
        ])
        .expect("schema ok");
        let rows = vec![
            vec![Value::Int32(10), Value::Vector(vec![3.0, 0.0])],
            vec![Value::Int32(20), Value::Vector(vec![0.0, 1.0])],
            vec![Value::Int32(30), Value::Vector(vec![0.2, 0.0])],
            vec![Value::Int32(40), Value::Vector(vec![0.0, 2.0])],
        ];
        let batch = crate::seq_scan::build_batch(&rows, &schema).expect("batch ok");
        let child = CountingScan {
            schema: schema.clone(),
            batches: vec![batch],
            next: 0,
            pulls: Arc::new(AtomicUsize::new(0)),
        };
        let key = SortKey {
            expr: ScalarExpr::Column {
                name: "id".into(),
                index: 0,
                data_type: DataType::Int32,
            },
            asc: true,
            nulls_first: false,
        };
        let mut top_k = TopK::new(Box::new(child), vec![key], schema.clone(), 2)
            .with_work_mem_budget(std::sync::Arc::new(WorkMemBudget::new(1)));

        let mut out = Vec::new();
        while let Some(batch) = top_k.next_batch().expect("top-k ok") {
            out.extend(crate::filter_op::batch_to_rows(&batch, top_k.schema()).expect("decode"));
        }
        let ids: Vec<Value> = out.into_iter().map(|row| row[0].clone()).collect();

        assert_eq!(ids, vec![Value::Int32(10), Value::Int32(20)]);
        assert!(
            top_k.spilled_to_disk(),
            "generic top-k must keep spillable external sort fallback"
        );
    }

    #[test]
    fn exact_vector_top_k_uses_bounded_kernel_under_tiny_work_mem() {
        let schema = Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("embedding", DataType::Vector { dims: Some(2) }),
        ])
        .expect("schema ok");
        let rows = vec![
            vec![Value::Int32(10), Value::Vector(vec![3.0, 0.0])],
            vec![Value::Int32(20), Value::Vector(vec![0.0, 1.0])],
            vec![Value::Int32(30), Value::Vector(vec![0.2, 0.0])],
            vec![Value::Int32(40), Value::Vector(vec![0.0, 2.0])],
        ];
        let batch = crate::seq_scan::build_batch(&rows, &schema).expect("batch ok");
        let child = CountingScan {
            schema: schema.clone(),
            batches: vec![batch],
            next: 0,
            pulls: Arc::new(AtomicUsize::new(0)),
        };
        let key = SortKey {
            expr: ScalarExpr::Binary {
                op: BinaryOp::VectorL2Distance,
                left: Box::new(ScalarExpr::Column {
                    name: "embedding".into(),
                    index: 1,
                    data_type: DataType::Vector { dims: Some(2) },
                }),
                right: Box::new(ScalarExpr::Literal {
                    value: Value::Vector(vec![0.0, 0.0]),
                    data_type: DataType::Vector { dims: Some(2) },
                }),
                data_type: DataType::Float64,
            },
            asc: true,
            nulls_first: false,
        };
        let mut top_k = TopK::new(Box::new(child), vec![key], schema.clone(), 2)
            .with_work_mem_budget(std::sync::Arc::new(WorkMemBudget::new(1)));

        let mut out = Vec::new();
        while let Some(batch) = top_k.next_batch().expect("top-k ok") {
            out.extend(crate::filter_op::batch_to_rows(&batch, top_k.schema()).expect("decode"));
        }
        let ids: Vec<Value> = out.into_iter().map(|row| row[0].clone()).collect();

        assert_eq!(ids, vec![Value::Int32(30), Value::Int32(20)]);
        assert!(
            !top_k.spilled_to_disk(),
            "exact vector top-k must stay bounded instead of falling back to full Sort"
        );
        assert!(
            top_k
                .pruning_stats()
                .iter()
                .any(|note| note.contains("kernel=exact_top_k_f32")
                    && note.contains("full_sort=false")),
            "exact vector top-k must expose kernel/no-sort note"
        );
    }

    #[derive(Debug)]
    struct CountingScan {
        schema: Schema,
        batches: Vec<Batch>,
        next: usize,
        pulls: Arc<AtomicUsize>,
    }

    impl Operator for CountingScan {
        fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
            if self.next >= self.batches.len() {
                return Ok(None);
            }
            self.pulls.fetch_add(1, Ordering::SeqCst);
            let batch = self.batches[self.next].clone();
            self.next += 1;
            Ok(Some(batch))
        }

        fn schema(&self) -> &Schema {
            &self.schema
        }
    }

    fn i32_batch(values: &[i32]) -> Batch {
        Batch::new([Column::Int32(NumericColumn::from_data(values.to_vec()))]).expect("batch ok")
    }
}
