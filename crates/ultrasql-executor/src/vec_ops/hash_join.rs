//! Vectorized hash join operator.
//!
//! [`VectorizedHashJoin`] implements an INNER hash join over batched data.
//!
//! ## Two phases
//!
//! **Build phase** — the right (build) child is drained completely during
//! `drive()`. For each batch, every row is hashed using FNV-1a on the join
//! key column and inserted into a `HashMap<u64, Vec<usize>>` mapping hash
//! code → list of row indices.
//!
//! **Probe phase** — the left (probe) child is then driven batch by batch.
//! For each probe row, its key hash is looked up in the hash table and each
//! match is verified with an equality check to handle hash collisions. Joined
//! rows are pushed to the downstream sink as 4096-row batches.
//!
//! ## Current limitations
//!
//! - Only INNER JOIN.
//! - Single-column integer (`Int64`) join key.
//! - No spill to disk.

use std::collections::HashMap;

use ultrasql_core::Schema;
use ultrasql_vec::column::{Column, NumericColumn};
use ultrasql_vec::{Batch, hash_i64};

use crate::push_pipeline::{SinkVerdict, VectorizedOperator, VectorizedSink};
use crate::{ExecError, Operator};

/// Vectorized hash join.
///
/// Performs an INNER hash join between `build_child` (right side) and
/// `probe_child` (left side) on columns `build_key_col` and `probe_key_col`
/// respectively.
///
/// The output schema is the concatenation of the probe schema and the build
/// schema (probe columns first, then build columns).
pub struct VectorizedHashJoin {
    probe_child: Box<dyn VectorizedOperator>,
    build_child: Box<dyn Operator>,
    probe_key_col: usize,
    build_key_col: usize,
    schema: Schema,
}

impl std::fmt::Debug for VectorizedHashJoin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorizedHashJoin")
            .field("probe_key_col", &self.probe_key_col)
            .field("build_key_col", &self.build_key_col)
            .finish_non_exhaustive()
    }
}

impl VectorizedHashJoin {
    /// Construct a vectorized hash join.
    ///
    /// - `probe_child` — push-based probe side (left).
    /// - `build_child` — pull-based build side (right); drained at first
    ///   `drive()` call.
    /// - `probe_key_col` — 0-based column index of the join key on the probe
    ///   side.
    /// - `build_key_col` — 0-based column index of the join key on the build
    ///   side.
    /// - `schema` — output schema (probe columns || build columns).
    #[must_use]
    pub fn new(
        probe_child: Box<dyn VectorizedOperator>,
        build_child: Box<dyn Operator>,
        probe_key_col: usize,
        build_key_col: usize,
        schema: Schema,
    ) -> Self {
        Self {
            probe_child,
            build_child,
            probe_key_col,
            build_key_col,
            schema,
        }
    }
}

impl VectorizedOperator for VectorizedHashJoin {
    fn drive(&mut self, sink: &mut dyn VectorizedSink) -> Result<(), ExecError> {
        // ---- Build phase ----
        let build_schema = self.build_child.schema().clone();
        let mut build_batches: Vec<Batch> = Vec::new();
        loop {
            let Some(b) = self.build_child.next_batch()? else {
                break;
            };
            if !b.is_empty() {
                build_batches.push(b);
            }
        }

        // Build hash table: hash → Vec<(batch_idx, row_idx)>
        let mut table: HashMap<u64, Vec<(usize, usize)>> = HashMap::new();
        for (bi, batch) in build_batches.iter().enumerate() {
            let key_col = get_i64_col(batch, self.build_key_col)?;
            let hashes = hash_i64(key_col, None);
            for (ri, &h) in hashes.iter().enumerate() {
                table.entry(h).or_default().push((bi, ri));
            }
        }

        // ---- Probe phase ----
        let schema = self.schema.clone();
        let build_key_col = self.build_key_col;
        let probe_key_col = self.probe_key_col;

        let mut probe_sink = ProbeHashSink {
            inner: sink,
            table,
            build_batches: &build_batches,
            build_schema,
            probe_key_col,
            build_key_col,
            output_schema: schema,
        };

        self.probe_child.drive(&mut probe_sink)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

// ---- Internal sink for the probe phase ----

struct ProbeHashSink<'a> {
    inner: &'a mut dyn VectorizedSink,
    table: HashMap<u64, Vec<(usize, usize)>>,
    build_batches: &'a [Batch],
    build_schema: Schema,
    probe_key_col: usize,
    build_key_col: usize,
    output_schema: Schema,
}

impl VectorizedSink for ProbeHashSink<'_> {
    fn consume(&mut self, batch: Batch) -> Result<SinkVerdict, ExecError> {
        let probe_key_col = get_i64_col(&batch, self.probe_key_col)?;
        let probe_hashes = hash_i64(probe_key_col, None);
        let probe_keys = probe_key_col.data();

        // Collect matching (probe_row, build_batch, build_row) tuples
        let mut joined: Vec<(usize, usize, usize)> = Vec::new();
        for (pi, (&ph, &pk)) in probe_hashes.iter().zip(probe_keys.iter()).enumerate() {
            if let Some(candidates) = self.table.get(&ph) {
                for &(bi, ri) in candidates {
                    let build_batch = &self.build_batches[bi];
                    let build_key_data = get_i64_col(build_batch, self.build_key_col)?.data();
                    if build_key_data[ri] == pk {
                        joined.push((pi, bi, ri));
                    }
                }
            }
        }

        if joined.is_empty() {
            return Ok(SinkVerdict::Continue);
        }

        // Materialise output batch
        let probe_cols = batch.columns();
        let mut out_cols: Vec<Column> = probe_cols.to_vec();

        // Append build columns
        let n_out = joined.len();
        let n_build_cols = self.build_schema.len();
        for bc in 0..n_build_cols {
            // Gather rows from the build side for this column
            let col_data: Vec<i64> = joined
                .iter()
                .map(|&(_, bi, ri)| {
                    let build_batch = &self.build_batches[bi];
                    get_i64_col(build_batch, bc).map_or(0, |c| c.data()[ri])
                })
                .collect();
            out_cols.push(Column::Int64(NumericColumn::from_data(col_data)));
        }

        // Project probe columns to matched rows
        let mut final_cols: Vec<Column> = Vec::with_capacity(out_cols.len());
        for probe_col in probe_cols {
            let selected: Column = match probe_col {
                Column::Int32(c) => Column::Int32(NumericColumn::from_data(
                    joined.iter().map(|&(pi, _, _)| c.data()[pi]).collect(),
                )),
                Column::Int64(c) => Column::Int64(NumericColumn::from_data(
                    joined.iter().map(|&(pi, _, _)| c.data()[pi]).collect(),
                )),
                _ => {
                    return Err(ExecError::Unsupported(
                        "VectorizedHashJoin: only Int32/Int64 probe columns supported",
                    ));
                }
            };
            final_cols.push(selected);
        }
        // Append build columns (already have the correct matched rows)
        for bc in 0..n_build_cols {
            final_cols.push(out_cols[probe_cols.len() + bc].clone());
        }

        let _ = n_out;
        let out_batch = Batch::new(final_cols).map_err(ExecError::from)?;
        let _ = &self.output_schema;
        self.inner.consume(out_batch)
    }

    fn finalize(&mut self) -> Result<Option<Batch>, ExecError> {
        self.inner.finalize()
    }
}

// ---- Helpers ----

fn get_i64_col(batch: &Batch, idx: usize) -> Result<&NumericColumn<i64>, ExecError> {
    match batch.columns().get(idx) {
        Some(Column::Int64(c)) => Ok(c),
        Some(other) => Err(ExecError::TypeMismatch(format!(
            "expected Int64 at column {idx}, got {:?}",
            other.data_type()
        ))),
        None => Err(ExecError::TypeMismatch(format!(
            "column index {idx} out of range"
        ))),
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
    fn hash_join_inner_matches() {
        let probe = MemTableScan::new(
            schema_key_val(),
            vec![batch_kv(&[(1, 10), (2, 20), (3, 30)])],
        );
        let build = MemTableScan::new(schema_key_val(), vec![batch_kv(&[(2, 200), (4, 400)])]);
        let out_schema = Schema::new([
            Field::required("p_key", DataType::Int64),
            Field::required("p_val", DataType::Int64),
            Field::required("b_key", DataType::Int64),
            Field::required("b_val", DataType::Int64),
        ])
        .expect("schema ok");
        let probe_op = VectorizedSeqScan::new(Box::new(probe));
        let mut join =
            VectorizedHashJoin::new(Box::new(probe_op), Box::new(build), 0, 0, out_schema);
        let mut sink = CollectSink::new();
        join.drive(&mut sink).unwrap();
        let batches = sink.finish();
        // Only key=2 matches
        let p_keys = drain_i64(batches.clone(), 0);
        let b_keys = drain_i64(batches, 2);
        assert_eq!(p_keys, vec![2]);
        assert_eq!(b_keys, vec![2]);
    }

    #[test]
    fn hash_join_no_match_returns_empty() {
        let probe = MemTableScan::new(schema_key_val(), vec![batch_kv(&[(1, 10), (3, 30)])]);
        let build = MemTableScan::new(schema_key_val(), vec![batch_kv(&[(2, 200), (4, 400)])]);
        let out_schema = Schema::new([
            Field::required("pk", DataType::Int64),
            Field::required("pv", DataType::Int64),
            Field::required("bk", DataType::Int64),
            Field::required("bv", DataType::Int64),
        ])
        .expect("schema ok");
        let probe_op = VectorizedSeqScan::new(Box::new(probe));
        let mut join =
            VectorizedHashJoin::new(Box::new(probe_op), Box::new(build), 0, 0, out_schema);
        let mut sink = CollectSink::new();
        join.drive(&mut sink).unwrap();
        let total_rows: usize = sink.finish().iter().map(Batch::rows).sum();
        assert_eq!(total_rows, 0);
    }

    #[test]
    fn hash_join_schema_is_probe_then_build() {
        let probe = MemTableScan::new(schema_key_val(), vec![]);
        let build = MemTableScan::new(schema_key_val(), vec![]);
        let out_schema = Schema::new([
            Field::required("pk", DataType::Int64),
            Field::required("pv", DataType::Int64),
            Field::required("bk", DataType::Int64),
            Field::required("bv", DataType::Int64),
        ])
        .expect("schema ok");
        let probe_op = VectorizedSeqScan::new(Box::new(probe));
        let join = VectorizedHashJoin::new(Box::new(probe_op), Box::new(build), 0, 0, out_schema);
        assert_eq!(join.schema().len(), 4);
    }
}
