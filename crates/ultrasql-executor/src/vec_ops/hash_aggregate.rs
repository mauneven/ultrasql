//! Vectorized hash aggregate operator.
//!
//! [`VectorizedHashAggregate`] accumulates SUM and COUNT aggregates over
//! 4096-row batches, maintaining one accumulator entry per group key.
//!
//! ## Two phases
//!
//! **Accumulate phase** — the child is driven completely. For every batch,
//! the group-key columns are extracted and used to look up per-group
//! accumulators in a `HashMap`.
//!
//! **Emit phase** — after the child is exhausted, `finalize()` emits the
//! accumulated result as a single batch.
//!
//! ## Supported aggregates
//!
//! - `COUNT(*)` — counts all rows per group.
//! - `SUM(col)` — running `i64` sum per group.
//!
//! Only `Int64` group keys are supported in this first vectorized variant.

use std::collections::HashMap;
use std::hash::Hash;

use ultrasql_core::Schema;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};

use crate::ExecError;
use crate::push_pipeline::{SinkVerdict, VectorizedOperator, VectorizedSink};

// ============================================================================
// AggSpec
// ============================================================================

/// Describes a single aggregate to compute.
#[derive(Clone, Debug)]
pub enum AggSpec {
    /// `COUNT(*)` — counts all rows.
    CountStar,
    /// `SUM(column_index)` — running sum of an `Int64` column.
    Sum(usize),
}

// ============================================================================
// VectorizedHashAggregate
// ============================================================================

/// Vectorized hash aggregate operator.
///
/// Groups rows by `group_key_col` (an `Int64` column) and computes one or
/// more aggregates per group.
///
/// The output schema is: group key column, then one column per aggregate in
/// the order they appear in `aggregates`.
#[derive(Debug)]
pub struct VectorizedHashAggregate {
    child: Box<dyn VectorizedOperator>,
    group_key_col: usize,
    aggregates: Vec<AggSpec>,
    schema: Schema,
}

impl VectorizedHashAggregate {
    /// Construct a vectorized hash aggregate.
    ///
    /// - `child`         — upstream push operator.
    /// - `group_key_col` — 0-based index of the `Int64` group-key column.
    /// - `aggregates`    — list of aggregates to compute.
    /// - `schema`        — output schema (key col + one col per aggregate).
    #[must_use]
    pub fn new(
        child: Box<dyn VectorizedOperator>,
        group_key_col: usize,
        aggregates: Vec<AggSpec>,
        schema: Schema,
    ) -> Self {
        Self {
            child,
            group_key_col,
            aggregates,
            schema,
        }
    }
}

impl VectorizedOperator for VectorizedHashAggregate {
    fn drive(&mut self, sink: &mut dyn VectorizedSink) -> Result<(), ExecError> {
        let group_key_col = self.group_key_col;
        let aggregates = self.aggregates.clone();
        let schema = self.schema.clone();

        let mut acc_sink = AccumulateSink {
            table: HashMap::new(),
            group_key_col,
            aggregates: aggregates.clone(),
        };

        self.child.drive(&mut acc_sink)?;

        // Finalise and emit.
        let result_batch = acc_sink.finalise(&schema, &aggregates)?;
        if let Some(b) = result_batch {
            sink.consume(b)?;
        }
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

// ============================================================================
// AccumulateSink
// ============================================================================

/// Per-group accumulator state.
#[derive(Debug, Default)]
struct GroupState {
    count: i64,
    sums: Vec<i64>,
}

/// Key wrapper for i64 that implements `Hash + Eq`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct GroupKey(i64);

struct AccumulateSink {
    table: HashMap<GroupKey, GroupState>,
    group_key_col: usize,
    aggregates: Vec<AggSpec>,
}

impl AccumulateSink {
    fn finalise(self, schema: &Schema, aggregates: &[AggSpec]) -> Result<Option<Batch>, ExecError> {
        if self.table.is_empty() {
            return Ok(None);
        }

        let n = self.table.len();
        let mut keys: Vec<i64> = Vec::with_capacity(n);
        let mut counts: Vec<i64> = Vec::with_capacity(n);
        let n_sums = aggregates
            .iter()
            .filter(|a| matches!(a, AggSpec::Sum(_)))
            .count();
        let mut sums: Vec<Vec<i64>> = vec![Vec::with_capacity(n); n_sums];

        let mut pairs: Vec<(i64, GroupState)> =
            self.table.into_iter().map(|(k, v)| (k.0, v)).collect();
        pairs.sort_by_key(|(k, _)| *k);

        let mut sum_idx_per_agg: Vec<Option<usize>> = Vec::new();
        let mut sum_counter = 0;
        for agg in aggregates {
            match agg {
                AggSpec::CountStar => sum_idx_per_agg.push(None),
                AggSpec::Sum(_) => {
                    sum_idx_per_agg.push(Some(sum_counter));
                    sum_counter += 1;
                }
            }
        }

        for (k, state) in pairs {
            keys.push(k);
            counts.push(state.count);
            for (agg_i, idx_opt) in sum_idx_per_agg.iter().enumerate() {
                if let Some(si) = idx_opt {
                    sums[*si].push(*state.sums.get(agg_i).unwrap_or(&0));
                }
            }
        }

        let _ = schema;
        let mut cols: Vec<Column> = vec![Column::Int64(NumericColumn::from_data(keys))];
        let mut sum_cursor = 0;
        for agg in aggregates {
            match agg {
                AggSpec::CountStar => {
                    cols.push(Column::Int64(NumericColumn::from_data(counts.clone())));
                }
                AggSpec::Sum(_) => {
                    cols.push(Column::Int64(NumericColumn::from_data(
                        sums[sum_cursor].clone(),
                    )));
                    sum_cursor += 1;
                }
            }
        }

        Ok(Some(Batch::new(cols).map_err(ExecError::from)?))
    }
}

impl VectorizedSink for AccumulateSink {
    fn consume(&mut self, batch: Batch) -> Result<SinkVerdict, ExecError> {
        let cols = batch.columns();
        let n = batch.rows();

        // Extract group key column
        let key_data: Vec<i64> = match cols.get(self.group_key_col) {
            Some(Column::Int64(c)) => c.data().to_vec(),
            Some(other) => {
                return Err(ExecError::TypeMismatch(format!(
                    "group key must be Int64, got {:?}",
                    other.data_type()
                )));
            }
            None => return Err(ExecError::Internal("group key column out of range")),
        };

        // Pre-extract sum column data
        let mut sum_sources: Vec<Option<Vec<i64>>> = Vec::new();
        for agg in &self.aggregates {
            match agg {
                AggSpec::CountStar => sum_sources.push(None),
                AggSpec::Sum(ci) => {
                    let data = match cols.get(*ci) {
                        Some(Column::Int64(c)) => c.data().to_vec(),
                        _ => return Err(ExecError::Unsupported("SUM requires Int64 column")),
                    };
                    sum_sources.push(Some(data));
                }
            }
        }

        for row in 0..n {
            let key = GroupKey(key_data[row]);
            let state = self.table.entry(key).or_insert_with(|| GroupState {
                count: 0,
                sums: vec![0; self.aggregates.len()],
            });
            state.count += 1;
            for (agg_i, src) in sum_sources.iter().enumerate() {
                if let Some(data) = src {
                    state.sums[agg_i] = state.sums[agg_i].wrapping_add(data[row]);
                }
            }
        }

        Ok(SinkVerdict::Continue)
    }

    fn finalize(&mut self) -> Result<Option<Batch>, ExecError> {
        Ok(None)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::*;
    use crate::mem_table_scan::MemTableScan;
    use crate::push_pipeline::CollectSink;
    use crate::vec_ops::scan::VectorizedSeqScan;

    fn schema_key_val() -> Schema {
        Schema::new([
            Field::required("key", DataType::Int64),
            Field::required("val", DataType::Int64),
        ])
        .expect("schema ok")
    }

    fn batch_kv(rows: &[(i64, i64)]) -> Batch {
        Batch::new([
            Column::Int64(NumericColumn::from_data(
                rows.iter().map(|(k, _)| *k).collect(),
            )),
            Column::Int64(NumericColumn::from_data(
                rows.iter().map(|(_, v)| *v).collect(),
            )),
        ])
        .unwrap()
    }

    fn agg_schema(n_aggs: usize) -> Schema {
        let mut fields = vec![Field::required("key", DataType::Int64)];
        for i in 0..n_aggs {
            fields.push(Field::required(format!("a{i}"), DataType::Int64));
        }
        Schema::new(fields).expect("schema ok")
    }

    fn drain_i64(batches: Vec<Batch>, col: usize) -> Vec<i64> {
        let mut out = Vec::new();
        for b in batches {
            match &b.columns()[col] {
                Column::Int64(c) => out.extend_from_slice(c.data()),
                other => panic!("unexpected {other:?}"),
            }
        }
        out
    }

    #[test]
    fn count_star_per_group() {
        let scan = MemTableScan::new(
            schema_key_val(),
            vec![batch_kv(&[(1, 10), (2, 20), (1, 30), (2, 40), (1, 50)])],
        );
        let child = VectorizedSeqScan::new(Box::new(scan));
        let mut agg = VectorizedHashAggregate::new(
            Box::new(child),
            0,
            vec![AggSpec::CountStar],
            agg_schema(1),
        );
        let mut sink = CollectSink::new();
        agg.drive(&mut sink).unwrap();
        let batches = sink.finish();
        let keys = drain_i64(batches.clone(), 0);
        let counts = drain_i64(batches, 1);
        // key=1 → count 3, key=2 → count 2
        let idx1 = keys.iter().position(|&k| k == 1).unwrap();
        let idx2 = keys.iter().position(|&k| k == 2).unwrap();
        assert_eq!(counts[idx1], 3);
        assert_eq!(counts[idx2], 2);
    }

    #[test]
    fn sum_per_group() {
        let scan = MemTableScan::new(
            schema_key_val(),
            vec![batch_kv(&[(1, 10), (2, 20), (1, 30)])],
        );
        let child = VectorizedSeqScan::new(Box::new(scan));
        let mut agg =
            VectorizedHashAggregate::new(Box::new(child), 0, vec![AggSpec::Sum(1)], agg_schema(1));
        let mut sink = CollectSink::new();
        agg.drive(&mut sink).unwrap();
        let batches = sink.finish();
        let keys = drain_i64(batches.clone(), 0);
        let sums = drain_i64(batches, 1);
        let idx1 = keys.iter().position(|&k| k == 1).unwrap();
        let idx2 = keys.iter().position(|&k| k == 2).unwrap();
        assert_eq!(sums[idx1], 40); // 10 + 30
        assert_eq!(sums[idx2], 20);
    }

    #[test]
    fn empty_input_returns_nothing() {
        let scan = MemTableScan::new(schema_key_val(), vec![]);
        let child = VectorizedSeqScan::new(Box::new(scan));
        let mut agg = VectorizedHashAggregate::new(
            Box::new(child),
            0,
            vec![AggSpec::CountStar],
            agg_schema(1),
        );
        let mut sink = CollectSink::new();
        agg.drive(&mut sink).unwrap();
        let total: usize = sink.finish().iter().map(Batch::rows).sum();
        assert_eq!(total, 0);
    }
}
