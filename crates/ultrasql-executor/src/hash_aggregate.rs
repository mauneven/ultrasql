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

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use serde_json::{Number as JsonNumber, Value as JsonValue};
use ultrasql_core::{DataType, Schema, Value};
use ultrasql_planner::{AggregateFunc, LogicalAggregateExpr, ScalarExpr};
use ultrasql_vec::column::{Column, NumericColumn};
use ultrasql_vec::{Batch, count_i64, max_i64, min_i64, sum_i64};

use crate::eval::Eval;
use crate::filter_op::batch_to_rows;
use crate::row_codec::RowCodec;
use crate::row_spill::RowSpillFile;
use crate::seq_scan::build_batch;
use crate::work_mem::WorkMemBudget;
use crate::{CancelFlag, ExecError, Operator, OperatorSpillProfile};

/// Maximum rows per emitted batch, matching the `ARCHITECTURE.md` section 9 contract.
const BATCH_TARGET_ROWS: usize = 4096;
const HASH_AGG_SPILL_PARTITIONS: usize = 64;

// ---------------------------------------------------------------------------
// Hash-map key wrapper
// ---------------------------------------------------------------------------

/// A wrapper around [`Value`] that implements `Hash + Eq` so it can serve
/// as a component of a hash-map key.
///
/// [`Value`] derives only `PartialEq` (not `Eq`) because `f32`/`f64` are not
/// `Eq`. We implement `Eq` manually: for floating-point values we use
/// bitwise equality (NaN == NaN in this context, consistent with join
/// semantics for floating-point GROUP BY keys).
#[derive(Debug)]
struct KeyValue(Value);

impl PartialEq for KeyValue {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Value::Float32(a), Value::Float32(b)) => a.to_bits() == b.to_bits(),
            (Value::Float64(a), Value::Float64(b)) => a.to_bits() == b.to_bits(),
            (Value::Vector(a), Value::Vector(b)) | (Value::HalfVec(a), Value::HalfVec(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(l, r)| l.to_bits() == r.to_bits())
            }
            _ => self.0 == other.0,
        }
    }
}

impl Eq for KeyValue {}

impl Hash for KeyValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_value(&self.0, state);
    }
}

/// Hash a [`Value`] by discriminant + bit-pattern for floating-point types.
fn hash_value<H: Hasher>(v: &Value, state: &mut H) {
    match v {
        Value::Null => state.write_u8(0),
        Value::Bool(b) => {
            state.write_u8(1);
            b.hash(state);
        }
        Value::Int16(x) => {
            state.write_u8(2);
            x.hash(state);
        }
        Value::Int32(x) => {
            state.write_u8(3);
            x.hash(state);
        }
        Value::Int64(x) => {
            state.write_u8(4);
            x.hash(state);
        }
        Value::Float32(x) => {
            state.write_u8(5);
            x.to_bits().hash(state);
        }
        Value::Float64(x) => {
            state.write_u8(6);
            x.to_bits().hash(state);
        }
        Value::Text(s) => {
            state.write_u8(7);
            s.hash(state);
        }
        Value::Jsonb(s) => {
            state.write_u8(17);
            s.hash(state);
        }
        Value::Bytea(b) => {
            state.write_u8(8);
            b.hash(state);
        }
        Value::Timestamp(x) | Value::TimestampTz(x) | Value::Time(x) => {
            state.write_u8(9);
            x.hash(state);
        }
        Value::Date(x) => {
            state.write_u8(10);
            x.hash(state);
        }
        Value::Uuid(u) => {
            state.write_u8(11);
            u.hash(state);
        }
        Value::Decimal { value, scale } => {
            state.write_u8(12);
            value.hash(state);
            scale.hash(state);
        }
        Value::Interval {
            months,
            days,
            microseconds,
        } => {
            state.write_u8(13);
            months.hash(state);
            days.hash(state);
            microseconds.hash(state);
        }
        Value::Range(v) => {
            state.write_u8(14);
            v.hash(state);
        }
        Value::Geometry(v) => {
            state.write_u8(15);
            v.hash(state);
        }
        Value::Array {
            element_type,
            elements,
        } => {
            state.write_u8(18);
            element_type.hash(state);
            elements.hash(state);
        }
        Value::Vector(values) | Value::HalfVec(values) => {
            state.write_u8(19);
            for value in values {
                value.to_bits().hash(state);
            }
        }
        Value::SparseVec(value) => {
            state.write_u8(20);
            value.hash(state);
        }
        Value::BitVec { dims, bytes } => {
            state.write_u8(21);
            dims.hash(state);
            bytes.hash(state);
        }
        Value::Record(fields) => {
            state.write_u8(22);
            fields.hash(state);
        }
    }
}

/// A group key — a sequence of zero or more [`KeyValue`]s.
///
/// Uses a newtype wrapper so we can implement `Hash + Eq` for a
/// `Vec<Value>` without a coherence violation.
#[derive(Debug, PartialEq, Eq)]
struct GroupKey(Vec<KeyValue>);

impl Hash for GroupKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for kv in &self.0 {
            kv.hash(state);
        }
    }
}

impl GroupKey {
    fn from_values(values: Vec<Value>) -> Self {
        Self(values.into_iter().map(KeyValue).collect())
    }

    fn into_values(self) -> Vec<Value> {
        self.0.into_iter().map(|kv| kv.0).collect()
    }
}

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

        let iter = self.output.as_mut().expect("just-set");
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
                    .collect();
                return Ok(vec![identity]);
            }
            let row: Vec<Value> = states.iter().map(finalise).collect();
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
                let key_values: Vec<Value> = self
                    .group_key_evals
                    .iter()
                    .map(|ev| ev.eval(row).unwrap_or(Value::Null))
                    .collect();
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
                .collect();
            return Ok(vec![identity]);
        }

        // Collect groups into output rows.
        let mut output: Vec<Vec<Value>> = Vec::with_capacity(table.len());
        for (key, states) in table {
            let mut row = key.into_values(); // group key values first
            for state in &states {
                row.push(finalise(state));
            }
            output.push(row);
        }
        Ok(output)
    }

    fn build_grouped_vectorized(
        &mut self,
        plan: GroupedVecPlan,
    ) -> Result<Vec<Vec<Value>>, ExecError> {
        let mut table: HashMap<Option<i64>, i64> = HashMap::new();
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
            output.push(vec![
                key_value,
                finalize_grouped_sum(sum, &plan.result_type),
            ]);
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
                let key_values: Vec<Value> = self
                    .group_key_evals
                    .iter()
                    .map(|ev| ev.eval(&row).unwrap_or(Value::Null))
                    .collect();
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
                let key_values: Vec<Value> = self
                    .group_key_evals
                    .iter()
                    .map(|ev| ev.eval(&row).unwrap_or(Value::Null))
                    .collect();
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
                    row.push(finalise(state));
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

// ---------------------------------------------------------------------------
// Vectorised scalar-aggregate fast path
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum GroupedKey {
    Int32,
    Int64,
}

#[derive(Clone, Copy, Debug)]
enum GroupedAgg {
    SumColumn {
        index: usize,
    },
    SumMul {
        left_index: usize,
        right_index: usize,
    },
}

#[derive(Clone, Debug)]
struct GroupedVecPlan {
    key: GroupedKey,
    key_index: usize,
    agg: GroupedAgg,
    result_type: DataType,
}

/// One slot of the vectorised plan: which kernel to run and which column it
/// reads. `CountStar` is the only slot without a column reference.
#[derive(Debug, Clone)]
enum VecAggSlot {
    CountStar,
    Count(usize),
    Sum(usize),
    Avg(usize),
    Min(usize),
    Max(usize),
}

/// Build a [`VecAggSlot`] plan when every aggregate is in the supported
/// scalar fast set and references a simple column. Returns `None` if any
/// aggregate falls outside the fast set (e.g. `STRING_AGG`, `BOOL_AND`,
/// `DISTINCT`, or a non-`Column` argument expression).
fn build_vectorized_plan(aggregates: &[LogicalAggregateExpr]) -> Option<Vec<VecAggSlot>> {
    let mut plan = Vec::with_capacity(aggregates.len());
    for agg in aggregates {
        if agg.distinct {
            return None;
        }
        match agg.func {
            AggregateFunc::CountStar => {
                if agg.arg.is_some() {
                    return None;
                }
                plan.push(VecAggSlot::CountStar);
            }
            AggregateFunc::Count
            | AggregateFunc::Sum
            | AggregateFunc::Avg
            | AggregateFunc::Min
            | AggregateFunc::Max => {
                let arg = agg.arg.as_ref()?;
                plan.push(match agg.func {
                    AggregateFunc::Count
                    | AggregateFunc::Avg
                    | AggregateFunc::Min
                    | AggregateFunc::Max => {
                        let (idx, data_type) = column_ref(arg)?;
                        if !matches!(
                            data_type,
                            DataType::Int32
                                | DataType::Int64
                                | DataType::Float32
                                | DataType::Float64
                        ) {
                            return None;
                        }
                        match agg.func {
                            AggregateFunc::Count => VecAggSlot::Count(idx),
                            AggregateFunc::Avg => VecAggSlot::Avg(idx),
                            AggregateFunc::Min => VecAggSlot::Min(idx),
                            AggregateFunc::Max => VecAggSlot::Max(idx),
                            _ => unreachable!(),
                        }
                    }
                    AggregateFunc::Sum => {
                        if let Some((idx, data_type)) = column_ref(arg) {
                            if !matches!(
                                data_type,
                                DataType::Int32
                                    | DataType::Int64
                                    | DataType::Float32
                                    | DataType::Float64
                            ) {
                                return None;
                            }
                            VecAggSlot::Sum(idx)
                        } else {
                            return None;
                        }
                    }
                    _ => unreachable!(),
                });
            }
            _ => return None,
        }
    }
    Some(plan)
}

fn build_grouped_vectorized_plan(
    group_keys: &[ScalarExpr],
    aggregates: &[LogicalAggregateExpr],
    child_schema: &Schema,
) -> Option<GroupedVecPlan> {
    if group_keys.len() != 1 || aggregates.len() != 1 {
        return None;
    }
    let (key_index, key) = match &group_keys[0] {
        ScalarExpr::Column {
            index,
            data_type: DataType::Int32,
            ..
        }
        | ScalarExpr::Column {
            index,
            data_type: DataType::Date,
            ..
        } => (*index, GroupedKey::Int32),
        ScalarExpr::Column {
            index,
            data_type: DataType::Int64,
            ..
        } => (*index, GroupedKey::Int64),
        _ => return None,
    };
    let agg = aggregates.first()?;
    if agg.distinct || agg.func != AggregateFunc::Sum {
        return None;
    }
    let arg = agg.arg.as_ref()?;
    let grouped_agg = match arg {
        ScalarExpr::Column { index, .. }
            if numeric_storage_kind(&child_schema.field_at(*index).data_type) =>
        {
            GroupedAgg::SumColumn { index: *index }
        }
        ScalarExpr::Binary {
            op: ultrasql_planner::BinaryOp::Mul,
            left,
            right,
            ..
        } => {
            let (left_index, right_index) = match (&**left, &**right) {
                (
                    ScalarExpr::Column {
                        index: left_index, ..
                    },
                    ScalarExpr::Column {
                        index: right_index, ..
                    },
                ) => (*left_index, *right_index),
                _ => return None,
            };
            if !numeric_storage_kind(&child_schema.field_at(left_index).data_type)
                || !numeric_storage_kind(&child_schema.field_at(right_index).data_type)
            {
                return None;
            }
            GroupedAgg::SumMul {
                left_index,
                right_index,
            }
        }
        _ => return None,
    };
    Some(GroupedVecPlan {
        key,
        key_index,
        agg: grouped_agg,
        result_type: agg.data_type.clone(),
    })
}

fn numeric_storage_kind(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int32
            | DataType::Int64
            | DataType::Decimal { .. }
            | DataType::Date
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::Time
    )
}

fn grouped_vectorized_step(
    plan: &GroupedVecPlan,
    batch: &Batch,
    table: &mut HashMap<Option<i64>, i64>,
) -> Result<(), ExecError> {
    let cols = batch.columns();
    for row in 0..batch.rows() {
        let key = match plan.key {
            GroupedKey::Int32 => read_i32_key(cols.get(plan.key_index), row)?,
            GroupedKey::Int64 => read_i64_key(cols.get(plan.key_index), row)?,
        };
        let Some(delta) = (match plan.agg {
            GroupedAgg::SumColumn { index } => read_numeric_value(cols.get(index), row)?,
            GroupedAgg::SumMul {
                left_index,
                right_index,
            } => {
                let left = read_numeric_value(cols.get(left_index), row)?;
                let right = read_numeric_value(cols.get(right_index), row)?;
                match (left, right) {
                    (Some(left), Some(right)) => {
                        Some(left.checked_mul(right).ok_or_else(|| {
                            ExecError::TypeMismatch(
                                "grouped aggregate multiply overflow".to_owned(),
                            )
                        })?)
                    }
                    _ => None,
                }
            }
        }) else {
            continue;
        };
        let entry = table.entry(key).or_insert(0);
        *entry = entry
            .checked_add(delta)
            .ok_or_else(|| ExecError::TypeMismatch("grouped aggregate sum overflow".to_owned()))?;
    }
    Ok(())
}

fn read_i32_key(column: Option<&Column>, row: usize) -> Result<Option<i64>, ExecError> {
    match column {
        Some(Column::Int32(c)) => {
            if c.nulls().is_some_and(|nulls| !nulls.get(row)) {
                Ok(None)
            } else {
                Ok(Some(i64::from(c.data()[row])))
            }
        }
        Some(other) => Err(ExecError::TypeMismatch(format!(
            "grouped aggregate Int32 key requires Int32 column, got {:?}",
            other.data_type()
        ))),
        None => Err(ExecError::Internal(
            "grouped aggregate key column out of range",
        )),
    }
}

fn read_i64_key(column: Option<&Column>, row: usize) -> Result<Option<i64>, ExecError> {
    match column {
        Some(Column::Int64(c)) => {
            if c.nulls().is_some_and(|nulls| !nulls.get(row)) {
                Ok(None)
            } else {
                Ok(Some(c.data()[row]))
            }
        }
        Some(other) => Err(ExecError::TypeMismatch(format!(
            "grouped aggregate Int64 key requires Int64 column, got {:?}",
            other.data_type()
        ))),
        None => Err(ExecError::Internal(
            "grouped aggregate key column out of range",
        )),
    }
}

fn read_numeric_value(column: Option<&Column>, row: usize) -> Result<Option<i64>, ExecError> {
    match column {
        Some(Column::Int32(c)) => {
            if c.nulls().is_some_and(|nulls| !nulls.get(row)) {
                Ok(None)
            } else {
                Ok(Some(i64::from(c.data()[row])))
            }
        }
        Some(Column::Int64(c)) => {
            if c.nulls().is_some_and(|nulls| !nulls.get(row)) {
                Ok(None)
            } else {
                Ok(Some(c.data()[row]))
            }
        }
        Some(other) => Err(ExecError::TypeMismatch(format!(
            "grouped aggregate numeric input requires Int32/Int64 column, got {:?}",
            other.data_type()
        ))),
        None => Err(ExecError::Internal(
            "grouped aggregate numeric column out of range",
        )),
    }
}

fn finalize_grouped_sum(sum: i64, data_type: &DataType) -> Value {
    match data_type {
        DataType::Decimal { scale, .. } => Value::Decimal {
            value: sum,
            scale: scale.unwrap_or(0),
        },
        DataType::Int64 => Value::Int64(sum),
        DataType::Int32 => Value::Int32(i32::try_from(sum).unwrap_or(i32::MAX)),
        _ => Value::Int64(sum),
    }
}

/// Extract the column index and type from a `ScalarExpr::Column`. Returns
/// `None` for anything else (literals, binary ops, casts, …) — those go
/// through the scalar row loop.
fn column_ref(expr: &ScalarExpr) -> Option<(usize, DataType)> {
    match expr {
        ScalarExpr::Column {
            index, data_type, ..
        } => Some((*index, data_type.clone())),
        _ => None,
    }
}

/// Apply one vectorised batch step to the single-group `states` vector.
///
/// Each slot dispatches to a kernel that matches the column's runtime
/// variant. Bit-identical results against the scalar path are guaranteed for
/// the supported aggregate set:
/// * `Sum`/`Avg` on integer columns keep an `i64` accumulator with wrapping
///   semantics, matching [`add_values`] (which folds Int16/Int32/Int64
///   through `wrapping_add`).
/// * `Sum`/`Avg` on float columns keep an `f64` accumulator, matching the
///   widening that `add_values` performs for Float32/Float64.
/// * `Count(expr)` counts non-null entries via the column's optional bitmap.
/// * `CountStar` increments by `batch.rows()`.
/// * `Min`/`Max` defer to [`min_i64`] / [`max_i64`] for `Int64`, and use
///   tight per-type folds for the remaining numeric widths.
fn vectorized_step(
    plan: &[VecAggSlot],
    batch: &Batch,
    states: &mut [AggState],
) -> Result<(), ExecError> {
    let cols = batch.columns();
    let n = batch.rows();
    for (slot, state) in plan.iter().zip(states.iter_mut()) {
        match (slot, state) {
            (VecAggSlot::CountStar, AggState::CountStar(acc)) => {
                *acc = acc.saturating_add(i64::try_from(n).unwrap_or(i64::MAX));
            }
            (VecAggSlot::Count(ci), AggState::Count(acc)) => {
                *acc = acc.saturating_add(column_non_null_count(&cols[*ci]));
            }
            (VecAggSlot::Sum(ci), AggState::Sum(acc)) => {
                accumulate_sum(acc, &cols[*ci])?;
            }
            (VecAggSlot::Avg(ci), AggState::Avg(acc, cnt)) => {
                accumulate_sum(acc, &cols[*ci])?;
                *cnt = cnt.saturating_add(column_non_null_count(&cols[*ci]));
            }
            (VecAggSlot::Min(ci), AggState::Min(acc)) => {
                update_extremum(acc, &cols[*ci], /* take_min = */ true)?;
            }
            (VecAggSlot::Max(ci), AggState::Max(acc)) => {
                update_extremum(acc, &cols[*ci], /* take_min = */ false)?;
            }
            // The plan and the states are zipped in the same order and
            // `build_vectorized_plan` only emits slots that correspond to
            // their state variants, so a mismatch here is a logic bug.
            (slot, state) => {
                return Err(ExecError::TypeMismatch(format!(
                    "vectorized aggregate plan/state mismatch: {slot:?} vs {state:?}"
                )));
            }
        }
    }
    Ok(())
}

/// Count of non-null rows in `col`, as `i64` (saturating).
fn column_non_null_count(col: &Column) -> i64 {
    let total = col.len();
    let valid = match col {
        Column::Int32(c) => c.nulls().map_or(total, ultrasql_vec::Bitmap::count_ones),
        Column::Int64(c) => count_i64(c),
        Column::Float32(c) => c.nulls().map_or(total, ultrasql_vec::Bitmap::count_ones),
        Column::Float64(c) => c.nulls().map_or(total, ultrasql_vec::Bitmap::count_ones),
        Column::Bool(c) => c.nulls().map_or(total, ultrasql_vec::Bitmap::count_ones),
        Column::Utf8(c) => c.nulls().map_or(total, ultrasql_vec::Bitmap::count_ones),
        Column::DictionaryUtf8(c) => c
            .codes
            .nulls()
            .map_or(total, ultrasql_vec::Bitmap::count_ones),
    };
    i64::try_from(valid).unwrap_or(i64::MAX)
}

/// Accumulate `SUM(col)` into the running `Value` accumulator. NULL entries
/// are skipped. The accumulator stays `None` until at least one non-null row
/// has been observed, matching the scalar SUM contract.
fn accumulate_sum(acc: &mut Option<Value>, col: &Column) -> Result<(), ExecError> {
    match col {
        Column::Int64(c) => {
            if c.is_empty() {
                return Ok(());
            }
            let (delta, saw) = sum_i64_nullable(c);
            if !saw {
                return Ok(());
            }
            *acc = Some(match acc.take() {
                None => Value::Int64(delta),
                Some(Value::Int64(prev)) => Value::Int64(prev.wrapping_add(delta)),
                Some(other) => {
                    return Err(ExecError::TypeMismatch(format!(
                        "vectorized SUM accumulator/column type mismatch: {other:?} vs Int64"
                    )));
                }
            });
        }
        Column::Int32(c) => {
            if c.is_empty() {
                return Ok(());
            }
            let (delta, saw) = sum_i32_nullable_widened(c);
            if !saw {
                return Ok(());
            }
            *acc = Some(match acc.take() {
                None => Value::Int64(delta),
                Some(Value::Int64(prev)) => Value::Int64(prev.wrapping_add(delta)),
                Some(other) => {
                    return Err(ExecError::TypeMismatch(format!(
                        "vectorized SUM accumulator/column type mismatch: {other:?} vs Int32"
                    )));
                }
            });
        }
        Column::Float64(c) => {
            if c.is_empty() {
                return Ok(());
            }
            let (delta, saw) = sum_f64_nullable(c);
            if !saw {
                return Ok(());
            }
            *acc = Some(match acc.take() {
                None => Value::Float64(delta),
                Some(Value::Float64(prev)) => Value::Float64(prev + delta),
                Some(other) => {
                    return Err(ExecError::TypeMismatch(format!(
                        "vectorized SUM accumulator/column type mismatch: {other:?} vs Float64"
                    )));
                }
            });
        }
        Column::Float32(c) => {
            if c.is_empty() {
                return Ok(());
            }
            let (delta, saw) = sum_f32_nullable_widened(c);
            if !saw {
                return Ok(());
            }
            *acc = Some(match acc.take() {
                None => Value::Float64(delta),
                Some(Value::Float64(prev)) => Value::Float64(prev + delta),
                Some(other) => {
                    return Err(ExecError::TypeMismatch(format!(
                        "vectorized SUM accumulator/column type mismatch: {other:?} vs Float32"
                    )));
                }
            });
        }
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "vectorized SUM not supported for column: {other:?}"
            )));
        }
    }
    Ok(())
}

/// Sum non-null entries of an `i64` column. Returns `(sum, saw_non_null)`.
///
/// The two arms compile to different shapes — the dense path autovectorises
/// to a single NEON / AVX2 fold; the null-aware path keeps a branch per row
/// — so we keep them as a `match`. Clippy's `map_or_else` suggestion would
/// hide that distinction inside a closure body.
#[allow(clippy::option_if_let_else)]
fn sum_i64_nullable(c: &NumericColumn<i64>) -> (i64, bool) {
    match c.nulls() {
        None => (sum_i64(c), !c.is_empty()),
        Some(nulls) => {
            let mut s: i64 = 0;
            let mut saw = false;
            for (i, v) in c.data().iter().enumerate() {
                if nulls.get(i) {
                    s = s.wrapping_add(*v);
                    saw = true;
                }
            }
            (s, saw)
        }
    }
}

/// Sum non-null entries of an `i32` column, widening to `i64`. Dispatches
/// to the hand-NEON [`ultrasql_vec::kernels::sum_i32_widening`] on
/// aarch64 and to the scalar fold on every other target.
#[allow(clippy::option_if_let_else)]
fn sum_i32_nullable_widened(c: &NumericColumn<i32>) -> (i64, bool) {
    match c.nulls() {
        None => {
            let s = ultrasql_vec::kernels::sum_i32_widening(c);
            (s, !c.is_empty())
        }
        Some(nulls) => {
            let mut s: i64 = 0;
            let mut saw = false;
            for (i, &v) in c.data().iter().enumerate() {
                if nulls.get(i) {
                    s = s.wrapping_add(i64::from(v));
                    saw = true;
                }
            }
            (s, saw)
        }
    }
}

/// Sum non-null entries of an `f64` column. Returns `(sum, saw_non_null)`.
#[allow(clippy::option_if_let_else)]
fn sum_f64_nullable(c: &NumericColumn<f64>) -> (f64, bool) {
    match c.nulls() {
        None => (c.data().iter().sum(), !c.is_empty()),
        Some(nulls) => {
            let mut s = 0.0_f64;
            let mut saw = false;
            for (i, &v) in c.data().iter().enumerate() {
                if nulls.get(i) {
                    s += v;
                    saw = true;
                }
            }
            (s, saw)
        }
    }
}

/// Sum non-null entries of an `f32` column, widening to `f64` (matching
/// the scalar `add_values` widening of Float32 → Float64).
#[allow(clippy::option_if_let_else)]
fn sum_f32_nullable_widened(c: &NumericColumn<f32>) -> (f64, bool) {
    match c.nulls() {
        None => {
            let s = c.data().iter().fold(0.0_f64, |a, &b| a + f64::from(b));
            (s, !c.is_empty())
        }
        Some(nulls) => {
            let mut s = 0.0_f64;
            let mut saw = false;
            for (i, &v) in c.data().iter().enumerate() {
                if nulls.get(i) {
                    s += f64::from(v);
                    saw = true;
                }
            }
            (s, saw)
        }
    }
}

/// Update a running MIN/MAX accumulator from a column. `take_min = true`
/// for MIN; `false` for MAX. NULLs are skipped.
#[allow(clippy::option_if_let_else)]
fn update_extremum(acc: &mut Option<Value>, col: &Column, take_min: bool) -> Result<(), ExecError> {
    let candidate = match col {
        Column::Int64(c) => {
            if take_min {
                min_i64(c).map(Value::Int64)
            } else {
                max_i64(c).map(Value::Int64)
            }
        }
        Column::Int32(c) => extremum_i32(c, take_min).map(Value::Int32),
        Column::Float64(c) => extremum_f64(c, take_min).map(Value::Float64),
        Column::Float32(c) => extremum_f32(c, take_min).map(Value::Float32),
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "vectorized MIN/MAX not supported for column: {other:?}"
            )));
        }
    };
    if let Some(v) = candidate {
        *acc = Some(match acc.take() {
            None => v,
            Some(existing) => {
                let pick_new = if take_min {
                    value_lt(&v, &existing)
                } else {
                    value_lt(&existing, &v)
                };
                if pick_new { v } else { existing }
            }
        });
    }
    Ok(())
}

#[allow(clippy::option_if_let_else)]
fn extremum_i32(c: &NumericColumn<i32>, take_min: bool) -> Option<i32> {
    let mut best: Option<i32> = None;
    if let Some(nulls) = c.nulls() {
        for (i, &v) in c.data().iter().enumerate() {
            if !nulls.get(i) {
                continue;
            }
            best = Some(match best {
                None => v,
                Some(b) => {
                    if take_min {
                        if v < b { v } else { b }
                    } else if v > b {
                        v
                    } else {
                        b
                    }
                }
            });
        }
    } else {
        for &v in c.data() {
            best = Some(match best {
                None => v,
                Some(b) => {
                    if take_min {
                        if v < b { v } else { b }
                    } else if v > b {
                        v
                    } else {
                        b
                    }
                }
            });
        }
    }
    best
}

#[allow(clippy::option_if_let_else)]
fn extremum_f64(c: &NumericColumn<f64>, take_min: bool) -> Option<f64> {
    let mut best: Option<f64> = None;
    let consider = |best: Option<f64>, v: f64| -> Option<f64> {
        if v.is_nan() {
            return best;
        }
        Some(match best {
            None => v,
            Some(b) => {
                if take_min {
                    b.min(v)
                } else {
                    b.max(v)
                }
            }
        })
    };
    if let Some(nulls) = c.nulls() {
        for (i, &v) in c.data().iter().enumerate() {
            if !nulls.get(i) {
                continue;
            }
            best = consider(best, v);
        }
    } else {
        for &v in c.data() {
            best = consider(best, v);
        }
    }
    best
}

#[allow(clippy::option_if_let_else)]
fn extremum_f32(c: &NumericColumn<f32>, take_min: bool) -> Option<f32> {
    let mut best: Option<f32> = None;
    let consider = |best: Option<f32>, v: f32| -> Option<f32> {
        if v.is_nan() {
            return best;
        }
        Some(match best {
            None => v,
            Some(b) => {
                if take_min {
                    b.min(v)
                } else {
                    b.max(v)
                }
            }
        })
    };
    if let Some(nulls) = c.nulls() {
        for (i, &v) in c.data().iter().enumerate() {
            if !nulls.get(i) {
                continue;
            }
            best = consider(best, v);
        }
    } else {
        for &v in c.data() {
            best = consider(best, v);
        }
    }
    best
}

// ---------------------------------------------------------------------------
// Aggregate state machine
// ---------------------------------------------------------------------------

/// Per-row accumulator for a single aggregate function instance.
#[derive(Debug)]
enum AggState {
    /// DISTINCT wrapper: filters duplicate non-NULL aggregate inputs before
    /// forwarding them into the wrapped aggregate state.
    Distinct {
        inner: Box<AggState>,
        seen: HashSet<KeyValue>,
    },
    /// `COUNT(*)` — counts all rows regardless of NULLs.
    CountStar(i64),
    /// `COUNT(expr)` — counts non-NULL values.
    Count(i64),
    /// `SUM(expr)` — running sum, `None` if all values were NULL.
    Sum(Option<Value>),
    /// `AVG(expr)` — (running sum, count of non-NULLs).
    Avg(Option<Value>, i64),
    /// `MIN(expr)` — current minimum, `None` if no non-NULL seen yet.
    Min(Option<Value>),
    /// `MAX(expr)` — current maximum, `None` if no non-NULL seen yet.
    Max(Option<Value>),
    /// `BOOL_AND(expr)` — `None` until a non-NULL is seen.
    BoolAnd(Option<bool>),
    /// `BOOL_OR(expr)` — `None` until a non-NULL is seen.
    BoolOr(Option<bool>),
    /// `STRING_AGG(expr, sep)` — accumulated (values, separator).
    StringAgg(Vec<String>, String),
    /// `ARRAY_AGG(expr)` — accumulated non-NULL values.
    ArrayAgg(Vec<Value>),
    /// `JSON_AGG(expr)` — accumulated values, preserving SQL NULL.
    JsonAgg(Vec<Value>),
    /// Welford running aggregate for STDDEV / VARIANCE: `(count,
    /// mean, M2)` where `M2` is the running sum of squared
    /// differences from the mean. Shared between `STDDEV_SAMP`,
    /// `STDDEV_POP`, `VAR_SAMP`, `VAR_POP`; the variant carries
    /// the requested final shape so `finalise` knows whether to
    /// divide by `n` or `n - 1` and whether to take the square
    /// root.
    Welford {
        count: i64,
        mean: f64,
        m2: f64,
        sample: bool,
        sqrt: bool,
    },
}

/// Initialise one [`AggState`] for the given aggregate descriptor.
#[allow(clippy::missing_const_for_fn)] // not const due to Vec::new() in variants
fn init_state_for(agg: &LogicalAggregateExpr) -> AggState {
    if agg.distinct {
        return AggState::Distinct {
            inner: Box::new(init_state_for_func(agg.func)),
            seen: HashSet::new(),
        };
    }
    init_state_for_func(agg.func)
}

#[allow(clippy::missing_const_for_fn)] // not const due to Vec::new() in variants
fn init_state_for_func(func: AggregateFunc) -> AggState {
    match func {
        AggregateFunc::CountStar => AggState::CountStar(0),
        AggregateFunc::Count => AggState::Count(0),
        AggregateFunc::Sum => AggState::Sum(None),
        AggregateFunc::Avg => AggState::Avg(None, 0),
        AggregateFunc::Min => AggState::Min(None),
        AggregateFunc::Max => AggState::Max(None),
        AggregateFunc::BoolAnd => AggState::BoolAnd(None),
        AggregateFunc::BoolOr => AggState::BoolOr(None),
        AggregateFunc::StringAgg => AggState::StringAgg(Vec::new(), String::new()),
        AggregateFunc::ArrayAgg => AggState::ArrayAgg(Vec::new()),
        AggregateFunc::JsonAgg => AggState::JsonAgg(Vec::new()),
        AggregateFunc::StddevSamp => AggState::Welford {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            sample: true,
            sqrt: true,
        },
        AggregateFunc::StddevPop => AggState::Welford {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            sample: false,
            sqrt: true,
        },
        AggregateFunc::VarSamp => AggState::Welford {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            sample: true,
            sqrt: false,
        },
        AggregateFunc::VarPop => AggState::Welford {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            sample: false,
            sqrt: false,
        },
    }
}

/// Initialise all aggregate states for a new group.
fn init_states(aggregates: &[LogicalAggregateExpr]) -> Vec<AggState> {
    aggregates.iter().map(init_state_for).collect()
}

/// Feed one input `row` into `state` using the aggregate descriptor `agg`.
fn accumulate(
    state: &mut AggState,
    agg: &LogicalAggregateExpr,
    row: &[Value],
) -> Result<(), ExecError> {
    // Evaluate the argument expression (if any).
    let arg_val: Option<Value> = agg
        .arg
        .as_ref()
        .map(|expr| Eval::new(expr.clone()).eval(row).unwrap_or(Value::Null));

    if let AggState::Distinct { inner, seen } = state {
        let Some(v) = arg_val else {
            return Ok(());
        };
        if v.is_null() || !seen.insert(KeyValue(v.clone())) {
            return Ok(());
        }
        return accumulate_value(inner, Some(v));
    }

    accumulate_value(state, arg_val)
}

fn accumulate_value(state: &mut AggState, arg_val: Option<Value>) -> Result<(), ExecError> {
    match state {
        AggState::Distinct { .. } => unreachable!("distinct wrapper handled before dispatch"),
        AggState::CountStar(n) => {
            *n = n.saturating_add(1);
        }
        AggState::Count(n) => {
            if !matches!(arg_val, Some(Value::Null) | None) {
                *n = n.saturating_add(1);
            }
        }
        AggState::Sum(acc) => {
            if let Some(v) = arg_val {
                if !v.is_null() {
                    *acc = Some(match acc.take() {
                        None => v,
                        Some(existing) => add_values(existing, v)?,
                    });
                }
            }
        }
        AggState::Avg(sum, cnt) => {
            if let Some(v) = arg_val {
                if !v.is_null() {
                    *sum = Some(match sum.take() {
                        None => v,
                        Some(existing) => add_values(existing, v)?,
                    });
                    *cnt = cnt.saturating_add(1);
                }
            }
        }
        AggState::Min(current) => {
            if let Some(v) = arg_val {
                if !v.is_null() {
                    *current = Some(match current.take() {
                        None => v,
                        Some(existing) => {
                            if value_lt(&v, &existing) {
                                v
                            } else {
                                existing
                            }
                        }
                    });
                }
            }
        }
        AggState::Max(current) => {
            if let Some(v) = arg_val {
                if !v.is_null() {
                    *current = Some(match current.take() {
                        None => v,
                        Some(existing) => {
                            if value_lt(&existing, &v) {
                                v
                            } else {
                                existing
                            }
                        }
                    });
                }
            }
        }
        AggState::BoolAnd(acc) => {
            if let Some(Value::Bool(b)) = arg_val {
                *acc = Some(acc.unwrap_or(true) && b);
            }
        }
        AggState::BoolOr(acc) => {
            if let Some(Value::Bool(b)) = arg_val {
                *acc = Some(acc.unwrap_or(false) || b);
            }
        }
        AggState::StringAgg(parts, _sep) => {
            if let Some(v) = arg_val {
                if !v.is_null() {
                    match v {
                        Value::Text(s) => parts.push(s),
                        other => parts.push(other.to_string()),
                    }
                }
            }
        }
        AggState::ArrayAgg(items) => {
            if let Some(v) = arg_val {
                if !v.is_null() {
                    items.push(v);
                }
            }
        }
        AggState::JsonAgg(items) => {
            if let Some(v) = arg_val {
                items.push(v);
            }
        }
        AggState::Welford {
            count, mean, m2, ..
        } => {
            if let Some(v) = arg_val {
                if let Some(x) = value_as_f64(&v) {
                    // Welford's online algorithm. Numerically stable
                    // even when `count` is large; avoids the
                    // catastrophic cancellation of the naive
                    // sum-of-squares minus square-of-sum recipe.
                    *count = count.saturating_add(1);
                    let delta = x - *mean;
                    *mean += delta / *count as f64;
                    let delta2 = x - *mean;
                    *m2 += delta * delta2;
                }
            }
        }
    }
    Ok(())
}

/// Coerce a numeric `Value` to `f64` for floating-point folds.
fn value_as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Int16(x) => Some(f64::from(*x)),
        Value::Int32(x) => Some(f64::from(*x)),
        Value::Int64(x) => Some(*x as f64),
        Value::Float32(x) => Some(f64::from(*x)),
        Value::Float64(x) => Some(*x),
        _ => None,
    }
}

/// Finalise an [`AggState`] into its result [`Value`].
fn finalise(state: &AggState) -> Value {
    match state {
        AggState::Distinct { inner, .. } => finalise(inner),
        AggState::CountStar(n) | AggState::Count(n) => Value::Int64(*n),
        AggState::Sum(acc) | AggState::Min(acc) | AggState::Max(acc) => {
            acc.clone().unwrap_or(Value::Null)
        }
        AggState::Avg(sum, cnt) => {
            if *cnt == 0 {
                return Value::Null;
            }
            sum.as_ref()
                .map_or(Value::Null, |s| divide_value(s.clone(), *cnt))
        }
        AggState::BoolAnd(b) | AggState::BoolOr(b) => b.map_or(Value::Null, Value::Bool),
        AggState::StringAgg(parts, sep) => {
            if parts.is_empty() {
                Value::Null
            } else {
                Value::Text(parts.join(sep))
            }
        }
        AggState::ArrayAgg(items) => {
            if items.is_empty() {
                Value::Null
            } else {
                let element_type = items
                    .iter()
                    .find(|v| !v.is_null())
                    .map(Value::data_type)
                    .unwrap_or(DataType::Null);
                Value::Array {
                    element_type,
                    elements: items.clone(),
                }
            }
        }
        AggState::JsonAgg(items) => {
            if items.is_empty() {
                Value::Null
            } else {
                Value::Jsonb(json_agg_text(items))
            }
        }
        AggState::Welford {
            count,
            m2,
            sample,
            sqrt,
            ..
        } => {
            // Sample variance/stddev needs n - 1 in the denominator
            // and is undefined for fewer than two non-NULL inputs.
            // Population variance/stddev is defined for any non-zero
            // count.
            let n = *count;
            let denom = if *sample { n - 1 } else { n };
            if denom <= 0 {
                return Value::Null;
            }
            let var = m2 / denom as f64;
            Value::Float64(if *sqrt { var.sqrt() } else { var })
        }
    }
}

fn json_agg_text(items: &[Value]) -> String {
    let values = JsonValue::Array(items.iter().map(sql_value_to_json).collect());
    serde_json::to_string(&values).unwrap_or_else(|_| "[]".to_owned())
}

fn sql_value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Bool(v) => JsonValue::Bool(*v),
        Value::Int16(v) => JsonValue::Number(JsonNumber::from(i64::from(*v))),
        Value::Int32(v) => JsonValue::Number(JsonNumber::from(i64::from(*v))),
        Value::Int64(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Float32(v) => {
            JsonNumber::from_f64(f64::from(*v)).map_or(JsonValue::Null, JsonValue::Number)
        }
        Value::Float64(v) => JsonNumber::from_f64(*v).map_or(JsonValue::Null, JsonValue::Number),
        Value::Text(v) => JsonValue::String(v.clone()),
        Value::Jsonb(v) => serde_json::from_str(v).unwrap_or_else(|_| JsonValue::String(v.clone())),
        Value::Vector(values) | Value::HalfVec(values) => JsonValue::Array(
            values
                .iter()
                .map(|v| {
                    JsonNumber::from_f64(f64::from(*v)).map_or(JsonValue::Null, JsonValue::Number)
                })
                .collect(),
        ),
        Value::Array { elements, .. } => {
            JsonValue::Array(elements.iter().map(sql_value_to_json).collect())
        }
        other => JsonValue::String(other.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Arithmetic helpers
// ---------------------------------------------------------------------------

/// Add two numeric values, widening to Int64 or Float64 as appropriate.
///
/// The `Sum` and `Avg` accumulators store the running total as the
/// widened type (Int64 for integers, Float64 for floats) after the
/// first non-null input — but the *new* row arrives unwidened from
/// the child operator, so this helper must accept any mix of
/// narrower-on-the-right and widened-on-the-left integer and float
/// types. The output type is always the widened type to match.
fn add_values(a: Value, b: Value) -> Result<Value, ExecError> {
    match (a, b) {
        (
            Value::Decimal {
                value: x,
                scale: xs,
            },
            Value::Decimal {
                value: y,
                scale: ys,
            },
        ) => add_decimal_values(x, xs, y, ys),
        // Pure narrow-narrow promotions (first-step folding).
        (Value::Int16(x), Value::Int16(y)) => Ok(Value::Int64(i64::from(x) + i64::from(y))),
        (Value::Int32(x), Value::Int32(y)) => Ok(Value::Int64(i64::from(x) + i64::from(y))),
        (Value::Int64(x), Value::Int64(y)) => Ok(Value::Int64(x.wrapping_add(y))),
        // Widened accumulator + narrower fresh row (the common case in
        // SUM / AVG once the accumulator has stepped through one input).
        (Value::Int64(x), Value::Int16(y)) | (Value::Int16(y), Value::Int64(x)) => {
            Ok(Value::Int64(x.wrapping_add(i64::from(y))))
        }
        (Value::Int64(x), Value::Int32(y)) | (Value::Int32(y), Value::Int64(x)) => {
            Ok(Value::Int64(x.wrapping_add(i64::from(y))))
        }
        (Value::Float32(x), Value::Float32(y)) => Ok(Value::Float64(f64::from(x) + f64::from(y))),
        (Value::Float64(x), Value::Float64(y)) => Ok(Value::Float64(x + y)),
        (Value::Float64(x), Value::Float32(y)) | (Value::Float32(y), Value::Float64(x)) => {
            Ok(Value::Float64(x + f64::from(y)))
        }
        (a, b) => Err(ExecError::TypeMismatch(format!(
            "sum type mismatch: {a:?} and {b:?}"
        ))),
    }
}

/// Divide a running sum by the count to produce an average.
fn divide_value(sum: Value, count: i64) -> Value {
    match sum {
        Value::Int64(s) => Value::Float64(s as f64 / count as f64),
        Value::Float64(s) => Value::Float64(s / count as f64),
        Value::Decimal { value, scale } => {
            Value::Float64(decimal_to_f64(value, scale) / count as f64)
        }
        other => other,
    }
}

fn add_decimal_values(
    left_value: i64,
    left_scale: i32,
    right_value: i64,
    right_scale: i32,
) -> Result<Value, ExecError> {
    let common_scale = left_scale.max(right_scale);
    let left = rescale_decimal_value(left_value, left_scale, common_scale)?;
    let right = rescale_decimal_value(right_value, right_scale, common_scale)?;
    let sum = left
        .checked_add(right)
        .ok_or_else(|| ExecError::TypeMismatch("decimal sum overflow".to_owned()))?;
    let value = i64::try_from(sum)
        .map_err(|_| ExecError::TypeMismatch("decimal sum overflow".to_owned()))?;
    Ok(Value::Decimal {
        value,
        scale: common_scale,
    })
}

fn rescale_decimal_value(
    value: i64,
    current_scale: i32,
    target_scale: i32,
) -> Result<i128, ExecError> {
    let scale_delta = target_scale - current_scale;
    if scale_delta < 0 {
        return Err(ExecError::TypeMismatch(
            "decimal rescale underflow".to_owned(),
        ));
    }
    let factor = pow10_i128(
        u32::try_from(scale_delta)
            .map_err(|_| ExecError::TypeMismatch("decimal rescale overflow".to_owned()))?,
    )
    .ok_or_else(|| ExecError::TypeMismatch("decimal rescale overflow".to_owned()))?;
    i128::from(value)
        .checked_mul(factor)
        .ok_or_else(|| ExecError::TypeMismatch("decimal rescale overflow".to_owned()))
}

fn pow10_i128(exp: u32) -> Option<i128> {
    (0..exp).try_fold(1_i128, |acc, _| acc.checked_mul(10))
}

fn decimal_to_f64(value: i64, scale: i32) -> f64 {
    #[allow(clippy::cast_precision_loss)]
    let raw = value as f64;
    raw / 10_f64.powi(scale)
}

/// Returns `true` if `a < b` under the natural total order.
fn value_lt(a: &Value, b: &Value) -> bool {
    use crate::sort::compare_values_nullable;
    matches!(
        compare_values_nullable(a, b, false),
        std::cmp::Ordering::Less
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    reason = "tests: index arithmetic against compile-time-known loop bounds"
)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::{AggregateFunc, LogicalAggregateExpr, ScalarExpr};
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn, StringColumn};

    use super::{AggState, HashAggregate, accumulate_sum, finalise};
    use crate::mem_table_scan::MemTableScan;
    use crate::{Operator, WorkMemBudget};

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    fn col(name: &str, index: usize, data_type: DataType) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.into(),
            index,
            data_type,
        }
    }

    fn count_star_agg() -> LogicalAggregateExpr {
        LogicalAggregateExpr {
            func: AggregateFunc::CountStar,
            arg: None,
            distinct: false,
            output_name: "cnt".into(),
            data_type: DataType::Int64,
        }
    }

    fn sum_agg(name: &str, index: usize) -> LogicalAggregateExpr {
        LogicalAggregateExpr {
            func: AggregateFunc::Sum,
            arg: Some(col(name, index, DataType::Int64)),
            distinct: false,
            output_name: "total".into(),
            data_type: DataType::Int64,
        }
    }

    fn count_distinct_agg(name: &str, index: usize, data_type: DataType) -> LogicalAggregateExpr {
        LogicalAggregateExpr {
            func: AggregateFunc::Count,
            arg: Some(col(name, index, data_type)),
            distinct: true,
            output_name: "distinct_count".into(),
            data_type: DataType::Int64,
        }
    }

    fn min_agg(name: &str, index: usize) -> LogicalAggregateExpr {
        LogicalAggregateExpr {
            func: AggregateFunc::Min,
            arg: Some(col(name, index, DataType::Int64)),
            distinct: false,
            output_name: "mn".into(),
            data_type: DataType::Int64,
        }
    }

    fn max_agg(name: &str, index: usize) -> LogicalAggregateExpr {
        LogicalAggregateExpr {
            func: AggregateFunc::Max,
            arg: Some(col(name, index, DataType::Int64)),
            distinct: false,
            output_name: "mx".into(),
            data_type: DataType::Int64,
        }
    }

    /// Schema: (group i32, val i64)
    fn schema_group_val() -> Schema {
        Schema::new([
            Field::required("group", DataType::Int32),
            Field::required("val", DataType::Int64),
        ])
        .expect("schema ok")
    }

    fn make_batch_i32_i64(rows: &[(i32, i64)]) -> Batch {
        Batch::new([
            Column::Int32(NumericColumn::from_data(
                rows.iter().map(|(a, _)| *a).collect(),
            )),
            Column::Int64(NumericColumn::from_data(
                rows.iter().map(|(_, b)| *b).collect(),
            )),
        ])
        .expect("batch ok")
    }

    fn sum_decimal_mul_i32_agg() -> LogicalAggregateExpr {
        LogicalAggregateExpr {
            func: AggregateFunc::Sum,
            arg: Some(ScalarExpr::Binary {
                op: ultrasql_planner::BinaryOp::Mul,
                left: Box::new(col(
                    "cost",
                    1,
                    DataType::Decimal {
                        precision: Some(15),
                        scale: Some(2),
                    },
                )),
                right: Box::new(col("qty", 2, DataType::Int32)),
                data_type: DataType::Decimal {
                    precision: Some(15),
                    scale: Some(2),
                },
            }),
            distinct: false,
            output_name: "value".into(),
            data_type: DataType::Decimal {
                precision: Some(15),
                scale: Some(2),
            },
        }
    }

    fn drain_all(op: &mut dyn Operator) -> Vec<Vec<Value>> {
        let schema = op.schema().clone();
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().expect("no error") {
            let rows = crate::filter_op::batch_to_rows(&b, &schema).expect("decode ok");
            out.extend(rows);
        }
        out
    }

    // -------------------------------------------------------------------------
    // Test 1: COUNT(*) with no GROUP BY
    // -------------------------------------------------------------------------

    #[test]
    fn hash_agg_count_star_no_group() {
        let schema = schema_group_val();
        let scan = MemTableScan::new(
            schema,
            vec![make_batch_i32_i64(&[(1, 10), (2, 20), (3, 30)])],
        );
        let out_schema = Schema::new([Field::required("cnt", DataType::Int64)]).expect("schema ok");
        let mut op = HashAggregate::new(Box::new(scan), vec![], vec![count_star_agg()], out_schema);
        let rows = drain_all(&mut op);
        assert_eq!(rows.len(), 1, "scalar aggregate emits exactly one row");
        assert_eq!(rows[0][0], Value::Int64(3), "COUNT(*) = 3");
    }

    // -------------------------------------------------------------------------
    // Test 2: empty input, no group keys → single COUNT=0 row
    // -------------------------------------------------------------------------

    #[test]
    fn hash_agg_empty_input_no_group_emits_identity_row() {
        let schema = schema_group_val();
        let scan = MemTableScan::new(schema, vec![]);
        let out_schema = Schema::new([Field::required("cnt", DataType::Int64)]).expect("schema ok");
        let mut op = HashAggregate::new(Box::new(scan), vec![], vec![count_star_agg()], out_schema);
        let rows = drain_all(&mut op);
        assert_eq!(rows.len(), 1, "empty table + no group keys = one row");
        assert_eq!(rows[0][0], Value::Int64(0), "COUNT(*) = 0");
    }

    // -------------------------------------------------------------------------
    // Test 3: empty input with group keys → no rows
    // -------------------------------------------------------------------------

    #[test]
    fn hash_agg_empty_input_with_group_keys_emits_nothing() {
        let schema = schema_group_val();
        let scan = MemTableScan::new(schema, vec![]);
        let out_schema = Schema::new([
            Field::required("group", DataType::Int32),
            Field::required("cnt", DataType::Int64),
        ])
        .expect("schema ok");
        let mut op = HashAggregate::new(
            Box::new(scan),
            vec![col("group", 0, DataType::Int32)],
            vec![count_star_agg()],
            out_schema,
        );
        let rows = drain_all(&mut op);
        assert!(rows.is_empty(), "empty table + group keys = no rows");
    }

    // -------------------------------------------------------------------------
    // Test 4: GROUP BY with SUM, MIN, MAX
    // -------------------------------------------------------------------------

    #[test]
    fn hash_agg_group_by_sum_min_max() {
        let schema = schema_group_val();
        // group=1 has val: 10, 30; group=2 has val: 20
        let scan = MemTableScan::new(
            schema,
            vec![make_batch_i32_i64(&[(1, 10), (2, 20), (1, 30)])],
        );
        let out_schema = Schema::new([
            Field::required("group", DataType::Int32),
            Field::required("total", DataType::Int64),
            Field::required("mn", DataType::Int64),
            Field::required("mx", DataType::Int64),
        ])
        .expect("schema ok");
        let mut op = HashAggregate::new(
            Box::new(scan),
            vec![col("group", 0, DataType::Int32)],
            vec![sum_agg("val", 1), min_agg("val", 1), max_agg("val", 1)],
            out_schema,
        );
        let mut rows = drain_all(&mut op);
        // Sort by group key for deterministic comparison.
        rows.sort_by_key(|r| match &r[0] {
            Value::Int32(v) => *v,
            _ => i32::MAX,
        });
        assert_eq!(rows.len(), 2);
        // group=1: sum=40 (10+30), min=10, max=30
        assert_eq!(rows[0][0], Value::Int32(1));
        assert_eq!(rows[0][1], Value::Int64(40));
        assert_eq!(rows[0][2], Value::Int64(10));
        assert_eq!(rows[0][3], Value::Int64(30));
        // group=2: sum=20, min=20, max=20
        assert_eq!(rows[1][0], Value::Int32(2));
        assert_eq!(rows[1][1], Value::Int64(20));
    }

    #[test]
    fn hash_agg_spills_grouped_input_when_work_mem_is_too_small() {
        let schema = schema_group_val();
        let scan = MemTableScan::new(
            schema,
            vec![
                make_batch_i32_i64(&[(1, 10), (2, 20), (1, 7)]),
                make_batch_i32_i64(&[(3, 30), (2, 5), (3, 4)]),
            ],
        );
        let out_schema = Schema::new([
            Field::required("group", DataType::Int32),
            Field::required("total", DataType::Int64),
        ])
        .expect("schema ok");
        let mut op = HashAggregate::new(
            Box::new(scan),
            vec![col("group", 0, DataType::Int32)],
            vec![sum_agg("val", 1)],
            out_schema,
        )
        .with_work_mem_budget(std::sync::Arc::new(WorkMemBudget::new(1)));

        let mut rows = drain_all(&mut op);
        rows.sort_by_key(|row| match row[0] {
            Value::Int32(v) => v,
            _ => panic!("unexpected group key"),
        });

        assert_eq!(
            rows,
            vec![
                vec![Value::Int32(1), Value::Int64(17)],
                vec![Value::Int32(2), Value::Int64(25)],
                vec![Value::Int32(3), Value::Int64(34)],
            ]
        );
        assert!(
            op.spilled_to_disk(),
            "grouped hash aggregate must partition-spill"
        );
    }

    #[test]
    fn hash_agg_count_distinct_per_group() {
        let schema = schema_group_val();
        let scan = MemTableScan::new(
            schema,
            vec![make_batch_i32_i64(&[
                (1, 10),
                (1, 10),
                (1, 20),
                (2, 30),
                (2, 30),
            ])],
        );
        let out_schema = Schema::new([
            Field::required("group", DataType::Int32),
            Field::required("distinct_count", DataType::Int64),
        ])
        .expect("schema ok");
        let mut op = HashAggregate::new(
            Box::new(scan),
            vec![col("group", 0, DataType::Int32)],
            vec![count_distinct_agg("val", 1, DataType::Int64)],
            out_schema,
        );
        let mut rows = drain_all(&mut op);
        rows.sort_by_key(|r| match &r[0] {
            Value::Int32(v) => *v,
            _ => i32::MAX,
        });
        assert_eq!(rows[0], vec![Value::Int32(1), Value::Int64(2)]);
        assert_eq!(rows[1], vec![Value::Int32(2), Value::Int64(1)]);
    }

    // -------------------------------------------------------------------------
    // Test 5: COUNT(expr) counts non-null values
    // -------------------------------------------------------------------------

    #[test]
    fn hash_agg_count_expr_counts_non_null_values() {
        let schema = Schema::new([Field::nullable("v", DataType::Text { max_len: None })])
            .expect("schema ok");
        let scan = MemTableScan::new(
            schema,
            vec![
                Batch::new([Column::Utf8(StringColumn::from_data(vec![
                    "a".to_string(),
                    "b".to_string(),
                    "c".to_string(),
                ]))])
                .expect("batch ok"),
            ],
        );
        let out_schema = Schema::new([Field::required("cnt", DataType::Int64)]).expect("schema ok");
        let count_expr_agg = LogicalAggregateExpr {
            func: AggregateFunc::Count,
            arg: Some(col("v", 0, DataType::Text { max_len: None })),
            distinct: false,
            output_name: "cnt".into(),
            data_type: DataType::Int64,
        };
        let mut op = HashAggregate::new(Box::new(scan), vec![], vec![count_expr_agg], out_schema);
        let rows = drain_all(&mut op);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0][0],
            Value::Int64(3),
            "COUNT(v) counts all non-null values"
        );
    }

    #[test]
    fn hash_agg_array_agg_returns_native_array() {
        let schema = schema_group_val();
        let scan = MemTableScan::new(
            schema,
            vec![make_batch_i32_i64(&[(1, 10), (1, 20), (1, 30)])],
        );
        let agg = LogicalAggregateExpr {
            func: AggregateFunc::ArrayAgg,
            arg: Some(col("val", 1, DataType::Int64)),
            distinct: false,
            output_name: "vals".into(),
            data_type: DataType::Array(Box::new(DataType::Int64)),
        };
        let out_schema = Schema::new([Field::required(
            "vals",
            DataType::Array(Box::new(DataType::Int64)),
        )])
        .expect("schema ok");
        let mut op = HashAggregate::new(Box::new(scan), vec![], vec![agg], out_schema);
        let rows = drain_all(&mut op);
        assert_eq!(
            rows,
            vec![vec![Value::Array {
                element_type: DataType::Int64,
                elements: vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)]
            }]]
        );
    }

    // -------------------------------------------------------------------------
    // Test 6: multi-row group (duplicate hash keys handled correctly)
    // -------------------------------------------------------------------------

    #[test]
    fn hash_agg_many_groups_with_duplicates() {
        let schema = schema_group_val();
        // 100 rows, 10 groups of 10 rows each.
        let row_data: Vec<(i32, i64)> = (0_i32..100).map(|i| (i % 10, i64::from(i))).collect();
        let scan = MemTableScan::new(schema, vec![make_batch_i32_i64(&row_data)]);
        let out_schema = Schema::new([
            Field::required("group", DataType::Int32),
            Field::required("cnt", DataType::Int64),
        ])
        .expect("schema ok");
        let mut op = HashAggregate::new(
            Box::new(scan),
            vec![col("group", 0, DataType::Int32)],
            vec![count_star_agg()],
            out_schema,
        );
        let rows = drain_all(&mut op);
        assert_eq!(rows.len(), 10, "expected 10 groups");
        for row in &rows {
            assert_eq!(row[1], Value::Int64(10), "each group has 10 rows");
        }
    }

    // -------------------------------------------------------------------------
    // Vectorised fast-path cross-validation
    // -------------------------------------------------------------------------

    /// Build a single `(val i64 NULL)` batch with the given values and an
    /// optional null bitmap. Used by the vectorised-path cross-checks.
    fn make_i64_batch(values: Vec<i64>, nulls: Option<Vec<bool>>) -> (Schema, Batch) {
        use ultrasql_vec::Bitmap;
        let n = values.len();
        let schema = Schema::new([Field::nullable("val", DataType::Int64)]).expect("schema ok");
        let col = match nulls {
            None => Column::Int64(NumericColumn::from_data(values)),
            Some(pat) => {
                assert_eq!(pat.len(), n);
                let mut bm = Bitmap::new(n, false);
                for (i, &v) in pat.iter().enumerate() {
                    if v {
                        bm.set(i, true);
                    }
                }
                Column::Int64(NumericColumn::with_nulls(values, bm).expect("col ok"))
            }
        };
        let batch = Batch::new([col]).expect("batch ok");
        (schema, batch)
    }

    /// Build a single `(v i32 NULL)` batch with the given values and an
    /// optional null bitmap.
    fn make_i32_batch(values: Vec<i32>, nulls: Option<Vec<bool>>) -> (Schema, Batch) {
        use ultrasql_vec::Bitmap;
        let n = values.len();
        let schema = Schema::new([Field::nullable("v", DataType::Int32)]).expect("schema ok");
        let col = match nulls {
            None => Column::Int32(NumericColumn::from_data(values)),
            Some(pat) => {
                assert_eq!(pat.len(), n);
                let mut bm = Bitmap::new(n, false);
                for (i, &v) in pat.iter().enumerate() {
                    if v {
                        bm.set(i, true);
                    }
                }
                Column::Int32(NumericColumn::with_nulls(values, bm).expect("col ok"))
            }
        };
        let batch = Batch::new([col]).expect("batch ok");
        (schema, batch)
    }

    /// Test 1: SUM(i64) over 4096 rows. The vectorised column path and the
    /// row-at-a-time scalar path must produce bit-identical results on
    /// dense (non-null) data. NULL-aware behaviour is exercised separately
    /// because the v0.5 `batch_to_rows` decoder does not yet honour the
    /// column validity bitmap, so the row-loop reference is only meaningful
    /// when every row is valid.
    #[test]
    fn vectorized_sum_i64_matches_scalar() {
        let n = 4096_i64;
        // Deterministic LCG-style values.
        let values: Vec<i64> = (0..n)
            .map(|i| {
                i.wrapping_mul(2_862_933_555_777_941_757)
                    .wrapping_add(0x1234_5678)
            })
            .collect();
        let (schema, batch) = make_i64_batch(values.clone(), None);
        let out_schema =
            Schema::new([Field::nullable("total", DataType::Int64)]).expect("schema ok");

        let sum_val = LogicalAggregateExpr {
            func: AggregateFunc::Sum,
            arg: Some(col("val", 0, DataType::Int64)),
            distinct: false,
            output_name: "total".into(),
            data_type: DataType::Int64,
        };

        // Vectorised path.
        let scan_vec = MemTableScan::new(schema.clone(), vec![batch.clone()]);
        let mut op_vec = HashAggregate::new(
            Box::new(scan_vec),
            vec![],
            vec![sum_val.clone()],
            out_schema.clone(),
        );
        let rows_vec = drain_all(&mut op_vec);

        // Scalar path (forced off the fast path).
        let scan_sca = MemTableScan::new(schema, vec![batch]);
        let mut op_sca = HashAggregate::new(Box::new(scan_sca), vec![], vec![sum_val], out_schema);
        op_sca.force_scalar_path();
        let rows_sca = drain_all(&mut op_sca);

        assert_eq!(rows_vec.len(), 1);
        assert_eq!(rows_sca.len(), 1);
        assert_eq!(
            rows_vec[0], rows_sca[0],
            "vectorised SUM must equal scalar SUM bit-for-bit"
        );

        // Independent reference.
        let want: i64 = values.iter().fold(0_i64, |a, b| a.wrapping_add(*b));
        assert_eq!(rows_vec[0][0], Value::Int64(want));
    }

    /// Companion NULL-handling check for the vectorised SUM path. The row-
    /// loop reference is computed in Rust directly because v0.5's
    /// `batch_to_rows` does not yet honour the column validity bitmap. The
    /// kernel under test must (a) skip NULL rows, (b) return `Value::Null`
    /// when every row is NULL.
    #[test]
    fn vectorized_sum_i64_honours_nulls() {
        let n = 1024_i64;
        let values: Vec<i64> = (0..n).collect();
        let nulls_pat: Vec<bool> = (0..n as usize).map(|i| !i.is_multiple_of(17)).collect();
        let (schema, batch) = make_i64_batch(values.clone(), Some(nulls_pat.clone()));
        let out_schema =
            Schema::new([Field::nullable("total", DataType::Int64)]).expect("schema ok");

        let sum_val = LogicalAggregateExpr {
            func: AggregateFunc::Sum,
            arg: Some(col("val", 0, DataType::Int64)),
            distinct: false,
            output_name: "total".into(),
            data_type: DataType::Int64,
        };

        let scan = MemTableScan::new(schema, vec![batch]);
        let mut op = HashAggregate::new(Box::new(scan), vec![], vec![sum_val], out_schema);
        let rows = drain_all(&mut op);

        let want: i64 = values
            .iter()
            .zip(nulls_pat.iter())
            .filter_map(|(v, valid)| valid.then_some(*v))
            .fold(0_i64, i64::wrapping_add);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int64(want));

        // Independently verify the internal NULL semantics by exercising
        // the SUM accumulator on an all-NULL column: the accumulator must
        // stay `None`, which `finalise` returns as `Value::Null`. (The
        // operator's outbound `build_batch` does not yet preserve nulls in
        // v0.5, so we observe the NULL via the kernel directly rather than
        // through the round-trip.)
        let nulls_all = ultrasql_vec::Bitmap::new(32, false);
        let all_null_col =
            Column::Int64(NumericColumn::with_nulls(vec![42_i64; 32], nulls_all).expect("col ok"));
        let mut acc: Option<Value> = None;
        accumulate_sum(&mut acc, &all_null_col).expect("sum ok");
        assert!(acc.is_none(), "all-NULL accumulator must stay None");
        let state = AggState::Sum(acc);
        assert_eq!(finalise(&state), Value::Null);
    }

    /// Test 2: AVG(i32) over 4096 rows. The vectorised path widens the i32
    /// accumulator to i64 (matching the scalar `add_values` widening), and
    /// the final divide produces Float64. Dense (non-null) so the v0.5
    /// `batch_to_rows` row-loop reference is well-defined.
    #[test]
    fn vectorized_avg_i32_matches_scalar() {
        // Use a range that fits well in i32 to keep the i64 sum unambiguous.
        let values: Vec<i32> = (0_i32..4096).map(|i| i - 2048).collect();

        let (schema, batch) = make_i32_batch(values, None);
        let out_schema =
            Schema::new([Field::nullable("avg_v", DataType::Float64)]).expect("schema ok");

        let avg_v = LogicalAggregateExpr {
            func: AggregateFunc::Avg,
            arg: Some(col("v", 0, DataType::Int32)),
            distinct: false,
            output_name: "avg_v".into(),
            data_type: DataType::Float64,
        };

        // Vectorised path.
        let scan_vec = MemTableScan::new(schema.clone(), vec![batch.clone()]);
        let mut op_vec = HashAggregate::new(
            Box::new(scan_vec),
            vec![],
            vec![avg_v.clone()],
            out_schema.clone(),
        );
        let rows_vec = drain_all(&mut op_vec);

        // Scalar path.
        let scan_sca = MemTableScan::new(schema, vec![batch]);
        let mut op_sca = HashAggregate::new(Box::new(scan_sca), vec![], vec![avg_v], out_schema);
        op_sca.force_scalar_path();
        let rows_sca = drain_all(&mut op_sca);

        assert_eq!(rows_vec.len(), 1);
        assert_eq!(rows_sca.len(), 1);

        // The result is Float64; compare via bit pattern for exact equality.
        match (&rows_vec[0][0], &rows_sca[0][0]) {
            (Value::Float64(a), Value::Float64(b)) => {
                assert_eq!(
                    a.to_bits(),
                    b.to_bits(),
                    "vectorised AVG bits must equal scalar AVG bits"
                );
            }
            other => panic!("expected Float64 results, got {other:?}"),
        }
    }

    #[test]
    fn grouped_vectorized_sum_i64_matches_scalar() {
        let schema = schema_group_val();
        let batch = make_batch_i32_i64(&[(1, 10), (2, 20), (1, 7), (2, 3), (3, 9)]);
        let out_schema = Schema::new([
            Field::required("group", DataType::Int32),
            Field::nullable("total", DataType::Int64),
        ])
        .expect("schema ok");

        let scan_vec = MemTableScan::new(schema.clone(), vec![batch.clone()]);
        let mut op_vec = HashAggregate::new(
            Box::new(scan_vec),
            vec![col("group", 0, DataType::Int32)],
            vec![sum_agg("val", 1)],
            out_schema.clone(),
        );
        let mut rows_vec = drain_all(&mut op_vec);

        let scan_sca = MemTableScan::new(schema, vec![batch]);
        let mut op_sca = HashAggregate::new(
            Box::new(scan_sca),
            vec![col("group", 0, DataType::Int32)],
            vec![sum_agg("val", 1)],
            out_schema,
        );
        op_sca.force_scalar_path();
        let mut rows_sca = drain_all(&mut op_sca);

        rows_vec.sort_by_key(|row| match row[0] {
            Value::Int32(v) => v,
            ref other => panic!("expected Int32 group key, got {other:?}"),
        });
        rows_sca.sort_by_key(|row| match row[0] {
            Value::Int32(v) => v,
            ref other => panic!("expected Int32 group key, got {other:?}"),
        });
        assert_eq!(rows_vec, rows_sca);
    }

    #[test]
    fn grouped_vectorized_sum_mul_matches_scalar() {
        let schema = Schema::new([
            Field::required("partkey", DataType::Int32),
            Field::required(
                "cost",
                DataType::Decimal {
                    precision: Some(15),
                    scale: Some(2),
                },
            ),
            Field::required("qty", DataType::Int32),
        ])
        .expect("schema ok");
        let batch = Batch::new([
            Column::Int32(NumericColumn::from_data(vec![1_i32, 2, 1, 3])),
            Column::Int64(NumericColumn::from_data(vec![150_i64, 200, 25, 400])),
            Column::Int32(NumericColumn::from_data(vec![2_i32, 5, 4, 1])),
        ])
        .expect("batch ok");
        let out_schema = Schema::new([
            Field::required("partkey", DataType::Int32),
            Field::nullable(
                "value",
                DataType::Decimal {
                    precision: Some(15),
                    scale: Some(2),
                },
            ),
        ])
        .expect("schema ok");

        let scan_vec = MemTableScan::new(schema.clone(), vec![batch.clone()]);
        let mut op_vec = HashAggregate::new(
            Box::new(scan_vec),
            vec![col("partkey", 0, DataType::Int32)],
            vec![sum_decimal_mul_i32_agg()],
            out_schema.clone(),
        );
        let mut rows_vec = drain_all(&mut op_vec);

        let scan_sca = MemTableScan::new(schema, vec![batch]);
        let mut op_sca = HashAggregate::new(
            Box::new(scan_sca),
            vec![col("partkey", 0, DataType::Int32)],
            vec![sum_decimal_mul_i32_agg()],
            out_schema,
        );
        op_sca.force_scalar_path();
        let mut rows_sca = drain_all(&mut op_sca);

        rows_vec.sort_by_key(|row| match row[0] {
            Value::Int32(v) => v,
            ref other => panic!("expected Int32 group key, got {other:?}"),
        });
        rows_sca.sort_by_key(|row| match row[0] {
            Value::Int32(v) => v,
            ref other => panic!("expected Int32 group key, got {other:?}"),
        });
        assert_eq!(rows_vec, rows_sca);
    }

    /// Test 3: COUNT(*) over a 100-row batch returns exactly 100 via the
    /// vectorised path (no rows skipped, no null handling involved).
    #[test]
    fn vectorized_count_star_returns_batch_rows() {
        let values: Vec<i64> = (0_i64..100).collect();
        let (schema, batch) = make_i64_batch(values, None);
        let out_schema = Schema::new([Field::required("cnt", DataType::Int64)]).expect("schema ok");

        let scan = MemTableScan::new(schema, vec![batch]);
        let mut op = HashAggregate::new(Box::new(scan), vec![], vec![count_star_agg()], out_schema);
        let rows = drain_all(&mut op);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int64(100));
    }
}
