//! Hash-based GROUP BY and aggregate operator.
//!
//! # Two-phase execution
//!
//! **Build phase** (first `next_batch` call): the child is drained completely.
//! For each input row the group-key expressions are evaluated to produce a
//! `Vec<Value>` tuple. A `HashMap<GroupKey, Vec<AggState>>` accumulates
//! one aggregate state per group. After the child is exhausted, the groups
//! are collected into an output row buffer.
//!
//! **Probe phase**: the output buffer is emitted in 4096-row chunks until
//! empty.
//!
//! # NULL semantics
//!
//! - `COUNT(expr)` skips NULL argument values; `COUNT(*)` counts all rows.
//! - `SUM`, `AVG`, `MIN`, `MAX` skip NULL argument values; they return NULL
//!   for a group where all values were NULL (or the group was empty).
//! - `BOOL_AND` / `BOOL_OR` skip NULL values.
//! - `STRING_AGG` and `ARRAY_AGG` skip NULL values.
//!
//! # Empty-input rule (SQL standard)
//!
//! If the input is empty and there are **no** group keys, a single row is
//! emitted with all aggregates at their identity (COUNT = 0, SUM/AVG/MIN/MAX
//! = NULL, etc.). This matches `SELECT COUNT(*) FROM empty_table = 0`.
//! If the input is empty and there **are** group keys, no rows are emitted.

mod arith;
mod json_agg;
mod key;
mod state;
mod vec_plan;
mod vec_step;

use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::Arc;

use ultrasql_core::{Schema, Value};
use ultrasql_planner::{LogicalAggregateExpr, ScalarExpr};
use ultrasql_vec::Batch;

use crate::eval::Eval;
use crate::filter_op::batch_to_rows;
use crate::row_codec::RowCodec;
use crate::row_spill::RowSpillFile;
use crate::seq_scan::build_batch;
use crate::work_mem::WorkMemBudget;
use crate::{CancelFlag, ExecError, Operator, OperatorSpillProfile, eval_error_to_exec_error};

use self::key::{GroupKey, hash_value};
use self::state::{AggState, accumulate, finalise, init_state_for, init_states};
use self::vec_plan::{
    GroupedKey, GroupedVecPlan, build_grouped_vectorized_plan, build_vectorized_plan,
    finalize_grouped_sum, grouped_vectorized_step,
};
use self::vec_step::vectorized_step;

/// Maximum rows per emitted batch, matching the `ARCHITECTURE.md` section 9 contract.
const BATCH_TARGET_ROWS: usize = 4096;
const HASH_AGG_SPILL_PARTITIONS: usize = 64;

// ---------------------------------------------------------------------------
// Operator
// ---------------------------------------------------------------------------

/// Hash-based group-by and aggregate operator.
///
/// Evaluates `group_keys` per row to determine the group, then accumulates
/// one `AggState` per aggregate per group. On the probe phase, emits one
/// output row per group consisting of the group key values followed by the
/// finalised aggregate values.
///
/// The operator is `Send` because `HashMap`, `Vec`, `Schema`, and the
/// `Box<dyn Operator>` child are all `Send`.
#[derive(Debug)]
pub struct HashAggregate {
    child: Box<dyn Operator>,
    /// Original GROUP BY expressions. Retained alongside the compiled
    /// evaluators so the build phase can detect columnar fast paths.
    group_keys: Vec<ScalarExpr>,
    /// Compiled evaluators for the GROUP BY expressions.
    group_key_evals: Vec<Eval>,
    /// Aggregate function descriptors.
    aggregates: Vec<LogicalAggregateExpr>,
    schema: Schema,
    /// Output row buffer built during the build phase.
    /// `None` until the build phase completes.
    output: Option<std::vec::IntoIter<Vec<Value>>>,
    /// `true` after `Ok(None)` has been returned.
    eof: bool,
    /// Per-query cancel signal. Polled between batches in the build
    /// phase so a long aggregation surfaces
    /// [`ExecError::Cancelled`] mid-drain. `None` when no cancellation
    /// is wired (tests, bench harnesses).
    cancel_flag: Option<CancelFlag>,
    /// Optional per-query memory budget.
    work_mem: Option<Arc<WorkMemBudget>>,
    /// Whether this execution wrote input partitions to temp storage.
    spilled_to_disk: bool,
    /// Number of hash partitions spilled during the build phase.
    spill_partition_count: u64,
    /// Bytes written to hash-aggregate spill partitions.
    spill_bytes: u64,
    /// When `true`, the build phase skips the column-oriented fast path
    /// and uses the row-at-a-time scalar loop. Test-only knob used by
    /// the cross-validation tests; production callers leave it `false`.
    #[cfg(test)]
    force_scalar_path: bool,
}

impl HashAggregate {
    /// Construct a hash aggregate operator.
    ///
    /// - `child` — the input operator.
    /// - `group_keys` — expressions evaluated per row to form the group key.
    ///   An empty slice means no GROUP BY (whole-relation aggregate).
    /// - `aggregates` — aggregate function descriptors from the planner.
    /// - `schema` — output schema: group key columns followed by aggregate
    ///   columns. Must have `group_keys.len() + aggregates.len()` fields.
    #[must_use]
    pub fn new(
        child: Box<dyn Operator>,
        group_keys: Vec<ScalarExpr>,
        aggregates: Vec<LogicalAggregateExpr>,
        schema: Schema,
    ) -> Self {
        let group_key_evals = group_keys.iter().cloned().map(Eval::new).collect();
        Self {
            child,
            group_keys,
            group_key_evals,
            aggregates,
            schema,
            output: None,
            eof: false,
            cancel_flag: None,
            work_mem: None,
            spilled_to_disk: false,
            spill_partition_count: 0,
            spill_bytes: 0,
            #[cfg(test)]
            force_scalar_path: false,
        }
    }

    /// Attach a [`CancelFlag`] to this aggregate.
    ///
    /// Once set, the build phase polls the flag at every child-batch
    /// boundary and returns [`ExecError::Cancelled`] mid-drain. Use a
    /// builder method (rather than an extra `new_with_cancel_flag`
    /// constructor) so callers that do not need cancellation keep the
    /// existing two-line construction shape.
    #[must_use]
    pub fn with_cancel_flag(mut self, flag: CancelFlag) -> Self {
        self.cancel_flag = Some(flag);
        self
    }

    /// Attach a per-query work-memory budget.
    #[must_use]
    pub fn with_work_mem_budget(mut self, budget: Arc<WorkMemBudget>) -> Self {
        self.work_mem = Some(budget);
        self
    }

    /// Whether this execution wrote hash aggregate partitions to disk.
    #[must_use]
    pub const fn spilled_to_disk(&self) -> bool {
        self.spilled_to_disk
    }

    /// Test-only: disable the vectorised fast path. Used by cross-validation
    /// tests that need to exercise the row-at-a-time loop on inputs that
    /// would otherwise be eligible for the column-oriented kernels.
    #[cfg(test)]
    pub(crate) const fn force_scalar_path(&mut self) {
        self.force_scalar_path = true;
    }
}

impl Operator for HashAggregate {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }
        // Cancellation poll at output-batch boundary. Catches a cancel
        // that arrives during the probe phase between batches.
        if let Some(flag) = self.cancel_flag.as_ref()
            && flag.is_set()
        {
            return Err(ExecError::Cancelled);
        }

        if self.output.is_none() {
            let output_rows = self.build()?;
            self.output = Some(output_rows.into_iter());
        }

        let iter = self.output.as_mut().ok_or(ExecError::Internal(
            "hash aggregate output iterator missing",
        ))?;
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

    fn profile_children(&self) -> Vec<&dyn Operator> {
        vec![self.child.as_ref()]
    }

    fn spill_profile(&self) -> OperatorSpillProfile {
        OperatorSpillProfile {
            spills: self.spill_partition_count,
            bytes: self.spill_bytes,
        }
    }

    fn io_bytes(&self) -> u64 {
        self.spill_bytes.saturating_mul(2)
    }
}

impl HashAggregate {
    /// Execute the build phase: drain child, accumulate aggregates, finalise.
    fn build(&mut self) -> Result<Vec<Vec<Value>>, ExecError> {
        let has_group_keys = !self.group_key_evals.is_empty();
        if has_group_keys && self.should_partition_spill() {
            return self.build_spilled_partitioned();
        }

        // Scalar-aggregate (no GROUP BY) vectorised fast path.
        //
        // When the operator has no GROUP BY columns and every aggregate is
        // expressible as a column-oriented kernel (`SUM`, `COUNT`, `COUNT(*)`,
        // `MIN`, `MAX`, `AVG`) over a single column reference, we accumulate
        // running state directly from the typed columnar buffer using the
        // `ultrasql_vec` kernels. This bypasses the per-row `batch_to_rows`
        // materialisation and the per-row scalar dispatch, which together
        // dominate the cost of scalar aggregates on wide tables.
        #[cfg(test)]
        let allow_vectorized = !self.force_scalar_path;
        #[cfg(not(test))]
        let allow_vectorized = true;
        if allow_vectorized
            && !has_group_keys
            && let Some(plan) = build_vectorized_plan(&self.aggregates)
        {
            let mut states = init_states(&self.aggregates);
            let mut saw_any_row = false;
            loop {
                // Cancellation poll between child batches.
                if let Some(flag) = self.cancel_flag.as_ref()
                    && flag.is_set()
                {
                    return Err(ExecError::Cancelled);
                }
                let Some(batch) = self.child.next_batch()? else {
                    break;
                };
                if batch.rows() == 0 {
                    continue;
                }
                saw_any_row = true;
                vectorized_step(&plan, &batch, &mut states)?;
            }
            if !saw_any_row {
                // Empty input + no GROUP BY → identity row.
                let identity: Vec<Value> = self
                    .aggregates
                    .iter()
                    .map(|agg| finalise(&init_state_for(agg)))
                    .collect::<Result<_, _>>()?;
                return Ok(vec![identity]);
            }
            let row: Vec<Value> = states.iter().map(finalise).collect::<Result<_, _>>()?;
            return Ok(vec![row]);
        }

        if allow_vectorized
            && has_group_keys
            && let Some(plan) = build_grouped_vectorized_plan(
                &self.group_keys,
                &self.aggregates,
                self.child.schema(),
            )
        {
            return self.build_grouped_vectorized(plan);
        }

        // General path: row-at-a-time evaluation against a `HashMap<GroupKey, …>`.
        let mut table: HashMap<GroupKey, Vec<AggState>> = HashMap::new();
        let child_schema = self.child.schema().clone();
        let mut saw_any_row = false;

        loop {
            // Cancellation poll between child batches. Mirrors the
            // vectorised fast-path loop above so neither code path can
            // ignore an in-flight CancelRequest.
            if let Some(flag) = self.cancel_flag.as_ref()
                && flag.is_set()
            {
                return Err(ExecError::Cancelled);
            }
            let Some(batch) = self.child.next_batch()? else {
                break;
            };
            let rows = batch_to_rows(&batch, &child_schema).map_err(|error| {
                ExecError::TypeMismatch(format!(
                    "HashAggregate child decode failed (rows={}, width={}): {error}",
                    batch.rows(),
                    batch.width()
                ))
            })?;
            for row in &rows {
                saw_any_row = true;
                // Evaluate group keys.
                let key_values = eval_group_key_values(&self.group_key_evals, row)?;
                let key = GroupKey::from_values(key_values);

                // Get or insert agg states for this group.
                let states = table
                    .entry(key)
                    .or_insert_with(|| init_states(&self.aggregates));

                // Accumulate.
                for (state, agg) in states.iter_mut().zip(self.aggregates.iter()) {
                    accumulate(state, agg, row)?;
                }
            }
        }

        // Empty input: if no group keys, emit a single identity row.
        if !saw_any_row && !has_group_keys {
            let identity: Vec<Value> = self
                .aggregates
                .iter()
                .map(|agg| finalise(&init_state_for(agg)))
                .collect::<Result<_, _>>()?;
            return Ok(vec![identity]);
        }

        // Collect groups into output rows.
        let mut output: Vec<Vec<Value>> = Vec::with_capacity(table.len());
        for (key, states) in table {
            let mut row = key.into_values(); // group key values first
            for state in &states {
                row.push(finalise(state)?);
            }
            output.push(row);
        }
        Ok(output)
    }

    fn build_grouped_vectorized(
        &mut self,
        plan: GroupedVecPlan,
    ) -> Result<Vec<Vec<Value>>, ExecError> {
        let mut table: HashMap<Option<i64>, Option<i64>> = HashMap::new();
        loop {
            if let Some(flag) = self.cancel_flag.as_ref()
                && flag.is_set()
            {
                return Err(ExecError::Cancelled);
            }
            let Some(batch) = self.child.next_batch()? else {
                break;
            };
            grouped_vectorized_step(&plan, &batch, &mut table)?;
        }

        let mut output = Vec::with_capacity(table.len());
        for (key, sum) in table {
            let key_value = match (plan.key, key) {
                (_, None) => Value::Null,
                (GroupedKey::Int32, Some(v)) => Value::Int32(
                    i32::try_from(v)
                        .map_err(|_| ExecError::TypeMismatch("group key overflow".to_owned()))?,
                ),
                (GroupedKey::Int64, Some(v)) => Value::Int64(v),
            };
            let sum_value = match sum {
                Some(sum) => finalize_grouped_sum(sum, &plan.result_type)?,
                None => Value::Null,
            };
            output.push(vec![key_value, sum_value]);
        }
        Ok(output)
    }

    fn should_partition_spill(&self) -> bool {
        self.work_mem
            .as_ref()
            .is_some_and(|budget| budget.limit_bytes() != u64::MAX)
    }

    fn build_spilled_partitioned(&mut self) -> Result<Vec<Vec<Value>>, ExecError> {
        let child_schema = self.child.schema().clone();
        let codec = RowCodec::new(child_schema.clone());
        let mut partitions: Vec<Option<RowSpillFile>> =
            (0..HASH_AGG_SPILL_PARTITIONS).map(|_| None).collect();
        let mut saw_any_row = false;

        loop {
            if let Some(flag) = self.cancel_flag.as_ref()
                && flag.is_set()
            {
                return Err(ExecError::Cancelled);
            }
            let Some(batch) = self.child.next_batch()? else {
                break;
            };
            let rows = batch_to_rows(&batch, &child_schema).map_err(|error| {
                ExecError::TypeMismatch(format!(
                    "HashAggregate child decode failed (rows={}, width={}): {error}",
                    batch.rows(),
                    batch.width()
                ))
            })?;
            for row in rows {
                saw_any_row = true;
                let key_values = eval_group_key_values(&self.group_key_evals, &row)?;
                let partition = partition_for_group_key_values(&key_values)?;
                if partitions[partition].is_none() {
                    partitions[partition] = Some(RowSpillFile::new("hash aggregate")?);
                }
                partitions[partition]
                    .as_mut()
                    .ok_or(ExecError::Internal("hash aggregate partition missing"))?
                    .append_row(&codec, &row)?;
                self.spilled_to_disk = true;
            }
        }

        if !saw_any_row {
            return Ok(Vec::new());
        }

        self.spill_partition_count =
            u64::try_from(partitions.iter().filter(|p| p.is_some()).count()).unwrap_or(u64::MAX);
        self.spill_bytes = partitions
            .iter()
            .filter_map(std::option::Option::as_ref)
            .fold(0_u64, |acc, spill| acc.saturating_add(spill.bytes()));

        let mut output = Vec::new();
        for partition in partitions.into_iter().flatten() {
            let mut spill = partition;
            let mut table: HashMap<GroupKey, Vec<AggState>> = HashMap::new();
            spill.scan_rows(&codec, |row| {
                let key_values = eval_group_key_values(&self.group_key_evals, &row)?;
                let key = GroupKey::from_values(key_values);
                let states = table
                    .entry(key)
                    .or_insert_with(|| init_states(&self.aggregates));
                for (state, agg) in states.iter_mut().zip(self.aggregates.iter()) {
                    accumulate(state, agg, &row)?;
                }
                Ok(())
            })?;

            for (key, states) in table {
                let mut row = key.into_values();
                for state in &states {
                    row.push(finalise(state)?);
                }
                output.push(row);
            }
        }
        Ok(output)
    }
}

fn partition_for_group_key_values(values: &[Value]) -> Result<usize, ExecError> {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for value in values {
        hash_value(value, &mut hasher);
    }
    let partitions = u64::try_from(HASH_AGG_SPILL_PARTITIONS)
        .map_err(|_| ExecError::Internal("hash aggregate partition count exceeds u64"))?;
    let idx = hasher.finish() % partitions;
    usize::try_from(idx)
        .map_err(|_| ExecError::Internal("hash aggregate partition index exceeds usize"))
}

fn eval_group_key_values(evals: &[Eval], row: &[Value]) -> Result<Vec<Value>, ExecError> {
    evals
        .iter()
        .map(|eval| eval.eval(row).map_err(eval_error_to_exec_error))
        .collect()
}

#[cfg(test)]
mod tests;
