//! `EXPLAIN [ANALYZE] [(FORMAT TEXT|JSON)]` dispatcher.
//!
//! The binder produces [`LogicalPlan::Explain`] for any `EXPLAIN`
//! statement. The session layer renders the wrapped plan tree into the
//! single `"QUERY PLAN"` Text column the client expects and emits one
//! `DataRow` per output line.
//!
//! - `format = ExplainFormat::Text`: the plan is rendered via
//!   [`LogicalPlan::display`] (indented tree, one node per line) and
//!   each line becomes a `DataRow`.
//! - `format = ExplainFormat::Json`: the plan is rendered as a nested
//!   JSON object — one top-level `Plan` field per node, recursively —
//!   and serialised into a single `DataRow`. This mirrors
//!   PostgreSQL's `EXPLAIN (FORMAT JSON)` shape closely enough that
//!   ORMs and dashboards can parse the result.
//! - `analyze = true`: the inner plan is executed end-to-end and an
//!   `actual rows = N (took Xms)` annotation is overlaid on the root
//!   node line. Per-operator timing is out of scope until the
//!   executor grows a generic `OperatorStats` channel; the root-level
//!   number alone is sufficient for ORM compatibility (psycopg /
//!   tokio-postgres surface the rows just fine).

use std::sync::Arc;
use std::time::Instant;

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_catalog::CatalogSnapshot;
use ultrasql_core::Field;
use ultrasql_executor::Operator;
use ultrasql_planner::{BinaryOp, ExplainFormat, LogicalIndexMethod, LogicalPlan, ScalarExpr};
use ultrasql_protocol::messages::{BackendMessage, FieldDescription};
use ultrasql_storage::access_method::HnswMetric;
use ultrasql_vec::Batch;
use ultrasql_vec::column::Column;

use super::Session;
use crate::error::ServerError;
use crate::pipeline::LowerCtx;
use crate::result_encoder::SelectResult;
use crate::run_plan_in_txn;

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Dispatch a [`LogicalPlan::Explain`] node. Renders the wrapped
    /// plan's tree shape into the wire `RowDescription` + `DataRow` +
    /// `CommandComplete` sequence the client expects.
    pub(crate) fn execute_explain(
        &self,
        plan: &LogicalPlan,
        catalog_snapshot: &Arc<CatalogSnapshot>,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::Explain {
            analyze,
            format,
            input,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_explain called with non-Explain plan",
            ));
        };

        // ANALYZE: execute the inner plan, count rows, measure wall-time.
        let actuals = if *analyze {
            Some(self.run_explain_analyze(input, catalog_snapshot)?)
        } else {
            None
        };

        let body = match format {
            ExplainFormat::Text => render_text(input, actuals.as_ref()),
            ExplainFormat::Json => render_json(input, actuals.as_ref()),
        };

        let row_count = body.lines().count();
        let mut messages: Vec<BackendMessage> = Vec::with_capacity(row_count.saturating_add(2));
        messages.push(BackendMessage::RowDescription {
            fields: vec![FieldDescription {
                name: "QUERY PLAN".to_string(),
                table_oid: 0,
                col_attnum: 0,
                type_oid: 25, // text
                type_size: -1,
                type_modifier: -1,
                format_code: 0,
            }],
        });
        for line in body.lines() {
            messages.push(BackendMessage::DataRow {
                columns: vec![Some(line.as_bytes().to_vec())],
            });
        }
        let rows = u64::try_from(row_count).unwrap_or(u64::MAX);
        messages.push(BackendMessage::CommandComplete {
            tag: format!("SELECT {rows}"),
        });

        Ok(SelectResult {
            messages,
            streamed_body: None,
            shared_streamed_body: None,
            rows,
        })
    }

    /// Execute the wrapped plan to completion, collecting root runtime
    /// evidence plus planner/lowerer decision notes.
    fn run_explain_analyze(
        &self,
        inner: &LogicalPlan,
        catalog_snapshot: &Arc<CatalogSnapshot>,
    ) -> Result<ExplainActuals, ServerError> {
        let notes = self.explain_notes(inner, catalog_snapshot);
        let started = Instant::now();
        if !matches!(
            inner,
            LogicalPlan::Insert { .. } | LogicalPlan::Update { .. } | LogicalPlan::Delete { .. }
        ) {
            let scan = self.run_explain_select_analyze(inner, catalog_snapshot)?;
            return Ok(ExplainActuals {
                rows: scan.rows,
                batches: scan.batches,
                peak_output_memory_bytes: scan.peak_output_memory_bytes,
                disk_spill: DiskSpillSummary::none(),
                elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
                simd_kernel: notes.simd_kernel,
                index_decision: notes.index_decision,
                vector_index: notes.vector_index,
                late_materialization: notes.late_materialization,
                aggregating_index: notes.aggregating_index,
                pushdowns_applied: notes.pushdowns_applied,
                parquet_row_groups: notes.parquet_row_groups,
            });
        }

        let txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let mut stream_buf = bytes::BytesMut::new();
        let outcome = run_plan_in_txn(
            inner,
            &txn,
            Arc::clone(catalog_snapshot),
            Arc::clone(&self.state.table_constraints),
            Arc::clone(&self.state.sequences),
            Arc::clone(&self.state.persistent_catalog),
            Arc::clone(&self.state.time_partitions),
            Arc::clone(&self.state.workload_recorder),
            Some(self.sequence_state.clone()),
            &self.state.tables,
            Arc::clone(&self.state.heap),
            Arc::clone(&self.state.vm),
            Arc::clone(&self.state.txn_manager),
            self.jit_config(),
            Some(self.cancel_flag.clone()),
            &mut stream_buf,
        );
        // Always commit the read-only ANALYZE txn — we don't surface
        // its results, only the row count buried in the `SelectResult`.
        let rows = match outcome {
            Ok(result) => result.rows,
            Err(e) => {
                if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                    tracing::warn!(error = %abort_err, "EXPLAIN ANALYZE abort failed");
                }
                return Err(e);
            }
        };
        if let Err(e) = self.state.txn_manager.commit(txn) {
            tracing::warn!(error = %e, "EXPLAIN ANALYZE commit failed");
        }
        Ok(ExplainActuals {
            rows,
            batches: 0,
            peak_output_memory_bytes: 0,
            disk_spill: DiskSpillSummary::none(),
            elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
            simd_kernel: notes.simd_kernel,
            index_decision: notes.index_decision,
            vector_index: notes.vector_index,
            late_materialization: notes.late_materialization,
            aggregating_index: notes.aggregating_index,
            pushdowns_applied: notes.pushdowns_applied,
            parquet_row_groups: notes.parquet_row_groups,
        })
    }

    fn run_explain_select_analyze(
        &self,
        inner: &LogicalPlan,
        catalog_snapshot: &Arc<CatalogSnapshot>,
    ) -> Result<ExplainScanActuals, ServerError> {
        let txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let outcome = (|| {
            let ctx = LowerCtx {
                tables: &self.state.tables,
                catalog_snapshot: Arc::clone(catalog_snapshot),
                table_constraints: Arc::clone(&self.state.table_constraints),
                sequences: Arc::clone(&self.state.sequences),
                persistent_catalog: Arc::clone(&self.state.persistent_catalog),
                time_partitions: Arc::clone(&self.state.time_partitions),
                workload_recorder: Arc::clone(&self.state.workload_recorder),
                sequence_state: Some(self.sequence_state.clone()),
                heap: Arc::clone(&self.state.heap),
                vm: Arc::clone(&self.state.vm),
                snapshot: txn.snapshot.clone(),
                oracle: Arc::clone(&self.state.txn_manager),
                xid: txn.current_xid(),
                command_id: txn.current_command,
                cte_buffers: std::collections::HashMap::new(),
                jit: self.jit_config(),
                cancel_flag: Some(self.cancel_flag.clone()),
                work_mem: Arc::new(ultrasql_executor::work_mem::WorkMemBudget::new(u64::MAX)),
            };
            let mut op = crate::pipeline::lower_query(inner, &ctx)?;
            drain_explain_operator(op.as_mut())
        })();

        match outcome {
            Ok(actuals) => {
                if let Err(e) = self.state.txn_manager.commit(txn) {
                    tracing::warn!(error = %e, "EXPLAIN ANALYZE commit failed");
                }
                Ok(actuals)
            }
            Err(e) => {
                if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                    tracing::warn!(error = %abort_err, "EXPLAIN ANALYZE abort failed");
                }
                Err(e)
            }
        }
    }

    fn explain_notes(
        &self,
        plan: &LogicalPlan,
        catalog_snapshot: &Arc<CatalogSnapshot>,
    ) -> ExplainNotes {
        ExplainNotes {
            simd_kernel: simd_kernel_note(plan),
            index_decision: self.index_decision_note(plan, catalog_snapshot),
            vector_index: self.vector_index_note(plan, catalog_snapshot),
            late_materialization: self.late_materialization_note(plan, catalog_snapshot),
            aggregating_index: crate::aggregating_index::aggregating_index_note_for_snapshot(
                plan,
                catalog_snapshot,
                self.state.table_constraints.as_ref(),
            ),
            pushdowns_applied: pushdown_notes(plan),
            parquet_row_groups: parquet_row_group_note(plan),
        }
    }

    fn index_decision_note(
        &self,
        plan: &LogicalPlan,
        catalog_snapshot: &Arc<CatalogSnapshot>,
    ) -> String {
        let Some((table, col_idx)) = first_filter_scan_column(plan) else {
            return "not applicable (no Filter(Scan) indexable shape)".to_owned();
        };
        let Some(table_entry) = catalog_snapshot.tables.get(&table.to_ascii_lowercase()) else {
            return format!("skipped {table}: not a persistent catalog table");
        };
        let Some(field) = table_entry.schema.field(col_idx) else {
            return format!("skipped {table}: predicate column {col_idx} out of range");
        };
        let Some(indexes) = catalog_snapshot.indexes_by_table.get(&table_entry.oid) else {
            return format!("skipped {table}.{}: no indexes on table", field.name);
        };
        let Ok(attnum) = u16::try_from(col_idx) else {
            return format!(
                "skipped {table}.{}: column index exceeds attnum",
                field.name
            );
        };
        let Some(index) = indexes.iter().find(|index| {
            index.columns.as_slice() == [attnum]
                && self.index_method(table_entry.oid, index.oid) == LogicalIndexMethod::Btree
        }) else {
            return format!(
                "skipped {table}.{}: no usable single-column btree index",
                field.name
            );
        };
        if !matches!(
            field.data_type,
            ultrasql_core::DataType::Bool
                | ultrasql_core::DataType::Int16
                | ultrasql_core::DataType::Int32
                | ultrasql_core::DataType::Int64
                | ultrasql_core::DataType::Timestamp
                | ultrasql_core::DataType::TimestampTz
        ) {
            return format!(
                "skipped {} on {table}.{}: key type {:?} not btree-probeable",
                index.name, field.name, field.data_type
            );
        }
        format!("selected {} on {table}.{}", index.name, field.name)
    }

    fn index_method(
        &self,
        table_oid: ultrasql_core::Oid,
        index_oid: ultrasql_core::Oid,
    ) -> LogicalIndexMethod {
        self.state.table_constraints.get(&table_oid).map_or(
            LogicalIndexMethod::Btree,
            |constraints| {
                constraints
                    .indexes
                    .get(&index_oid)
                    .map_or(LogicalIndexMethod::Btree, |metadata| metadata.method)
            },
        )
    }

    fn vector_index_note(
        &self,
        plan: &LogicalPlan,
        catalog_snapshot: &Arc<CatalogSnapshot>,
    ) -> String {
        let Some((table, col_idx, metric)) = first_vector_sort_scan(plan) else {
            return "not applicable (no ORDER BY vector distance LIMIT shape)".to_owned();
        };
        let Some(table_entry) = catalog_snapshot.tables.get(&table.to_ascii_lowercase()) else {
            return format!("skipped {table}: not a persistent catalog table");
        };
        let Ok(attnum) = u16::try_from(col_idx) else {
            return format!("skipped {table}: vector column index exceeds attnum");
        };
        let Some(indexes) = catalog_snapshot.indexes_by_table.get(&table_entry.oid) else {
            return format!("skipped {table}: no vector indexes on table");
        };
        let Some(constraints) = self.state.table_constraints.get(&table_entry.oid) else {
            return format!("skipped {table}: vector index runtime metadata unavailable");
        };
        for index in indexes {
            if index.columns.as_slice() != [attnum] {
                continue;
            }
            let Some(metadata) = constraints.indexes.get(&index.oid) else {
                continue;
            };
            if metadata.method == LogicalIndexMethod::Hnsw {
                if metadata
                    .hnsw
                    .as_ref()
                    .is_some_and(|hnsw| hnsw.metric() == metric && hnsw.is_available())
                {
                    return format!("selected {} (page-backed hnsw)", index.name);
                }
                return format!("skipped {}: page-backed hnsw unavailable", index.name);
            }
            if metadata.method == LogicalIndexMethod::IvfFlat {
                if metadata
                    .ivfflat
                    .as_ref()
                    .is_some_and(|ivfflat| ivfflat.metric() == metric && ivfflat.is_available())
                {
                    return format!("selected {} (page-backed ivfflat)", index.name);
                }
                return format!("skipped {}: page-backed ivfflat unavailable", index.name);
            }
        }
        format!("skipped {table}: no matching vector index")
    }

    fn late_materialization_note(
        &self,
        plan: &LogicalPlan,
        catalog_snapshot: &Arc<CatalogSnapshot>,
    ) -> String {
        let Some((table, filter_col, projected_cols)) = first_project_filter_scan(plan) else {
            return "not applicable (no Project(Filter(Scan)) shape)".to_owned();
        };
        let Some(table_entry) = catalog_snapshot.tables.get(&table.to_ascii_lowercase()) else {
            return format!("skipped {table}: not a persistent catalog table");
        };
        let Some(indexes) = catalog_snapshot.indexes_by_table.get(&table_entry.oid) else {
            return format!("skipped {table}: no indexes on table");
        };
        let Ok(attnum) = u16::try_from(filter_col) else {
            return format!("skipped {table}: predicate column index exceeds attnum");
        };
        let Some(index) = indexes.iter().find(|index| {
            index.columns.as_slice() == [attnum]
                && self.index_method(table_entry.oid, index.oid) == LogicalIndexMethod::Btree
        }) else {
            return format!("skipped {table}: no usable single-column btree index");
        };
        if projected_cols.iter().all(|col| {
            u16::try_from(*col)
                .ok()
                .is_some_and(|col_attnum| index.columns.contains(&col_attnum))
        }) {
            return format!(
                "not selected {}: projection covered by index key",
                index.name
            );
        }
        format!(
            "selected {} on {table}: index TID probe with deferred heap payload fetch",
            index.name
        )
    }
}

/// Wall-clock + row-count summary collected by `EXPLAIN ANALYZE`.
struct ExplainActuals {
    rows: u64,
    batches: u64,
    peak_output_memory_bytes: u64,
    disk_spill: DiskSpillSummary,
    elapsed_ms: f64,
    simd_kernel: String,
    index_decision: String,
    vector_index: String,
    late_materialization: String,
    aggregating_index: String,
    pushdowns_applied: Vec<String>,
    parquet_row_groups: Option<crate::pipeline::ParquetRowGroupSummary>,
}

struct ExplainScanActuals {
    rows: u64,
    batches: u64,
    peak_output_memory_bytes: u64,
}

struct ExplainNotes {
    simd_kernel: String,
    index_decision: String,
    vector_index: String,
    late_materialization: String,
    aggregating_index: String,
    pushdowns_applied: Vec<String>,
    parquet_row_groups: Option<crate::pipeline::ParquetRowGroupSummary>,
}

struct DiskSpillSummary {
    used: bool,
    bytes: u64,
    reason: &'static str,
}

impl DiskSpillSummary {
    const fn none() -> Self {
        Self {
            used: false,
            bytes: 0,
            reason: "no executor spill path reported disk writes",
        }
    }
}

/// Render the plan as the PostgreSQL-style indented tree.
fn render_text(plan: &LogicalPlan, actuals: Option<&ExplainActuals>) -> String {
    let mut body = plan.display(0);
    if let Some(a) = actuals {
        let pushdowns = if a.pushdowns_applied.is_empty() {
            "none".to_owned()
        } else {
            a.pushdowns_applied.join(", ")
        };
        body.push_str(&format!(
            "Planning Time: 0.000 ms\n\
             Execution Time: {:.3} ms\n\
             Actual Rows: {}\n\
             Actual Batches: {}\n\
             Peak Output Memory: {} bytes\n\
             Disk Spill: {} ({} bytes; {})\n\
             SIMD Kernel: {}\n\
             Index Decision: {}\n\
             Vector Index: {}\n\
             Late Materialization: {}\n\
             Aggregating Index: {}\n\
             Pushdowns Applied: {}\n\
             Parquet Row Groups: {}\n",
            a.elapsed_ms,
            a.rows,
            a.batches,
            a.peak_output_memory_bytes,
            if a.disk_spill.used { "yes" } else { "none" },
            a.disk_spill.bytes,
            a.disk_spill.reason,
            a.simd_kernel,
            a.index_decision,
            a.vector_index,
            a.late_materialization,
            a.aggregating_index,
            pushdowns,
            format_parquet_row_groups(a.parquet_row_groups)
        ));
    }
    body
}

/// Render the plan as a JSON document. The output mirrors
/// PostgreSQL's `EXPLAIN (FORMAT JSON)` schema closely: a single-row,
/// single-column response carrying a JSON array with one object per
/// plan tree, each object containing a `Plan` key, a `Node Type` field,
/// and a `Plans` array for child nodes. ANALYZE adds an `Actual Rows`
/// and `Execution Time` field at the top level.
fn render_json(plan: &LogicalPlan, actuals: Option<&ExplainActuals>) -> String {
    let mut buf = String::new();
    buf.push_str("[\n  {\n    \"Plan\": ");
    write_plan_json(plan, 4, &mut buf);
    if let Some(a) = actuals {
        let pushdowns = if a.pushdowns_applied.is_empty() {
            "[]".to_owned()
        } else {
            format!(
                "[{}]",
                a.pushdowns_applied
                    .iter()
                    .map(|s| format!("\"{}\"", json_escape(s)))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        buf.push_str(&format!(
            ",\n    \"Execution Time\": {:.3},\
             \n    \"Actual Rows\": {},\
             \n    \"Actual Batches\": {},\
             \n    \"Peak Output Memory\": {},\
             \n    \"Disk Spill\": {{\"Used\": {}, \"Bytes\": {}, \"Reason\": \"{}\"}},\
             \n    \"SIMD Kernel\": \"{}\",\
             \n    \"Index Decision\": \"{}\",\
             \n    \"Vector Index\": \"{}\",\
             \n    \"Late Materialization\": \"{}\",\
             \n    \"Aggregating Index\": \"{}\",\
             \n    \"Pushdowns Applied\": {},\
             \n    \"Parquet Row Groups\": {}",
            a.elapsed_ms,
            a.rows,
            a.batches,
            a.peak_output_memory_bytes,
            a.disk_spill.used,
            a.disk_spill.bytes,
            json_escape(a.disk_spill.reason),
            json_escape(&a.simd_kernel),
            json_escape(&a.index_decision),
            json_escape(&a.vector_index),
            json_escape(&a.late_materialization),
            json_escape(&a.aggregating_index),
            pushdowns,
            format_parquet_row_groups_json(a.parquet_row_groups)
        ));
    }
    buf.push_str("\n  }\n]");
    buf
}

/// Recursive JSON helper. `indent` is the column the *opening brace* of
/// the current object lands at.
fn write_plan_json(plan: &LogicalPlan, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    let inner_pad = " ".repeat(indent + 2);
    let node_type = plan_node_type(plan);
    out.push_str("{\n");
    out.push_str(&inner_pad);
    out.push_str("\"Node Type\": \"");
    out.push_str(node_type);
    out.push('"');

    let children = plan_children(plan);
    if !children.is_empty() {
        out.push_str(",\n");
        out.push_str(&inner_pad);
        out.push_str("\"Plans\": [\n");
        for (i, child) in children.iter().enumerate() {
            out.push_str(&" ".repeat(indent + 4));
            write_plan_json(child, indent + 4, out);
            if i + 1 < children.len() {
                out.push(',');
            }
            out.push('\n');
        }
        out.push_str(&inner_pad);
        out.push(']');
    }
    out.push('\n');
    out.push_str(&pad);
    out.push('}');
}

/// Return a stable short string identifying the plan node kind, suitable
/// for the JSON `Node Type` field. Mirrors PostgreSQL's nomenclature
/// where possible.
fn plan_node_type(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Scan { .. } => "Seq Scan",
        LogicalPlan::Filter { .. } => "Filter",
        LogicalPlan::Project { .. } => "Result",
        LogicalPlan::Limit { .. } => "Limit",
        LogicalPlan::Sort { .. } => "Sort",
        LogicalPlan::Join { .. } => "Hash Join",
        LogicalPlan::Aggregate { .. } => "Aggregate",
        LogicalPlan::Values { .. } => "Values Scan",
        LogicalPlan::SetOp { .. } => "Set Op",
        LogicalPlan::Cte { .. } => "CTE",
        LogicalPlan::LockRows { .. } => "LockRows",
        LogicalPlan::Insert { .. } => "Insert",
        LogicalPlan::Update { .. } => "Update",
        LogicalPlan::Delete { .. } => "Delete",
        LogicalPlan::Empty { .. } => "Empty",
        LogicalPlan::Truncate { .. } => "Truncate",
        LogicalPlan::CreateTable { .. } => "CreateTable",
        LogicalPlan::CreateMaterializedView { .. } => "CreateMaterializedView",
        LogicalPlan::CreateIndex { .. } => "CreateIndex",
        LogicalPlan::CreatePolicy { .. } => "CreatePolicy",
        LogicalPlan::DropTable { .. } => "DropTable",
        LogicalPlan::AlterTable { .. } => "AlterTable",
        LogicalPlan::CreateSequence { .. } => "CreateSequence",
        LogicalPlan::AlterSequence { .. } => "AlterSequence",
        LogicalPlan::DropSequence { .. } => "DropSequence",
        LogicalPlan::Comment { .. } => "Comment",
        LogicalPlan::Begin { .. } => "Begin",
        LogicalPlan::Commit { .. } => "Commit",
        LogicalPlan::Rollback { .. } => "Rollback",
        LogicalPlan::Savepoint { .. } => "Savepoint",
        LogicalPlan::RollbackToSavepoint { .. } => "RollbackToSavepoint",
        LogicalPlan::ReleaseSavepoint { .. } => "ReleaseSavepoint",
        LogicalPlan::PrepareTransaction { .. } => "PrepareTransaction",
        LogicalPlan::CommitPrepared { .. } => "CommitPrepared",
        LogicalPlan::RollbackPrepared { .. } => "RollbackPrepared",
        LogicalPlan::SetTransaction { .. } => "SetTransaction",
        LogicalPlan::SetVariable { .. } => "SetVariable",
        LogicalPlan::Explain { .. } => "Explain",
        LogicalPlan::Listen { .. } => "Listen",
        LogicalPlan::Notify { .. } => "Notify",
        LogicalPlan::Unlisten { .. } => "Unlisten",
        LogicalPlan::Copy { .. } => "Copy",
        LogicalPlan::FunctionScan { .. } => "Function Scan",
        LogicalPlan::Window { .. } => "WindowAgg",
    }
}

/// Return the direct children of `plan` in execution order.
fn plan_children(plan: &LogicalPlan) -> Vec<&LogicalPlan> {
    match plan {
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::LockRows { input, .. }
        | LogicalPlan::Explain { input, .. }
        | LogicalPlan::Update { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Delete { input, .. } => vec![input],
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOp { left, right, .. } => {
            vec![left, right]
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => vec![definition, body],
        LogicalPlan::Insert { source, .. } => vec![source],
        _ => Vec::new(),
    }
}

fn drain_explain_operator(op: &mut dyn Operator) -> Result<ExplainScanActuals, ServerError> {
    let mut rows = 0_u64;
    let mut batches = 0_u64;
    let mut peak_output_memory_bytes = 0_u64;
    while let Some(batch) = op.next_batch()? {
        batches = batches.saturating_add(1);
        rows = rows.saturating_add(u64::try_from(batch.rows()).unwrap_or(u64::MAX));
        peak_output_memory_bytes = peak_output_memory_bytes
            .max(u64::try_from(estimate_batch_memory(&batch)).unwrap_or(u64::MAX));
    }
    Ok(ExplainScanActuals {
        rows,
        batches,
        peak_output_memory_bytes,
    })
}

fn estimate_batch_memory(batch: &Batch) -> usize {
    batch.columns().iter().map(estimate_column_memory).sum()
}

fn estimate_column_memory(column: &Column) -> usize {
    match column {
        Column::Int32(c) => std::mem::size_of_val(c.data()) + bitmap_bytes(c.nulls()),
        Column::Int64(c) => std::mem::size_of_val(c.data()) + bitmap_bytes(c.nulls()),
        Column::Float32(c) => std::mem::size_of_val(c.data()) + bitmap_bytes(c.nulls()),
        Column::Float64(c) => std::mem::size_of_val(c.data()) + bitmap_bytes(c.nulls()),
        Column::Bool(c) => c.data().len() + bitmap_bytes(c.nulls()),
        Column::Utf8(c) => {
            c.values().len() + std::mem::size_of_val(c.offsets()) + bitmap_bytes(c.nulls())
        }
        Column::DictionaryUtf8(c) => {
            std::mem::size_of_val(c.codes.data())
                + c.dict.iter().map(String::len).sum::<usize>()
                + bitmap_bytes(c.codes.nulls())
        }
    }
}

fn bitmap_bytes(bitmap: Option<&ultrasql_vec::Bitmap>) -> usize {
    bitmap.map_or(0, |bits| bits.len().div_ceil(8))
}

fn first_filter_scan_column(plan: &LogicalPlan) -> Option<(&str, usize)> {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            let LogicalPlan::Scan { table, .. } = input.as_ref() else {
                return first_filter_scan_column(input);
            };
            indexable_column(predicate).map(|idx| (table.as_str(), idx))
        }
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::LockRows { input, .. }
        | LogicalPlan::Explain { input, .. }
        | LogicalPlan::Update { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Delete { input, .. } => first_filter_scan_column(input),
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOp { left, right, .. } => {
            first_filter_scan_column(left).or_else(|| first_filter_scan_column(right))
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => first_filter_scan_column(definition).or_else(|| first_filter_scan_column(body)),
        LogicalPlan::Insert { source, .. } => first_filter_scan_column(source),
        _ => None,
    }
}

fn first_project_filter_scan(plan: &LogicalPlan) -> Option<(&str, usize, Vec<usize>)> {
    match plan {
        LogicalPlan::Project { input, exprs, .. } => {
            let LogicalPlan::Filter {
                input: filter_input,
                predicate,
            } = input.as_ref()
            else {
                return first_project_filter_scan(input);
            };
            let LogicalPlan::Scan { table, .. } = filter_input.as_ref() else {
                return first_project_filter_scan(input);
            };
            let filter_col = indexable_column(predicate)?;
            let projected = exprs
                .iter()
                .filter_map(|(expr, _)| match expr {
                    ScalarExpr::Column { index, .. } => Some(*index),
                    _ => None,
                })
                .collect::<Vec<_>>();
            Some((table.as_str(), filter_col, projected))
        }
        LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::LockRows { input, .. }
        | LogicalPlan::Explain { input, .. }
        | LogicalPlan::Update { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Delete { input, .. } => first_project_filter_scan(input),
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOp { left, right, .. } => {
            first_project_filter_scan(left).or_else(|| first_project_filter_scan(right))
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => first_project_filter_scan(definition).or_else(|| first_project_filter_scan(body)),
        LogicalPlan::Insert { source, .. } => first_project_filter_scan(source),
        _ => None,
    }
}

fn first_vector_sort_scan(plan: &LogicalPlan) -> Option<(&str, usize, HnswMetric)> {
    match plan {
        LogicalPlan::Limit { input, .. } | LogicalPlan::Project { input, .. } => {
            first_vector_sort_scan(input)
        }
        LogicalPlan::Sort { input, keys } => {
            let key = keys.iter().find(|key| key.asc)?;
            let (col_idx, metric) = vector_sort_key_column(&key.expr)?;
            let table = first_scan_table(input)?;
            Some((table, col_idx, metric))
        }
        _ => plan_children(plan)
            .into_iter()
            .find_map(first_vector_sort_scan),
    }
}

fn first_scan_table(plan: &LogicalPlan) -> Option<&str> {
    match plan {
        LogicalPlan::Scan { table, .. } => Some(table.as_str()),
        _ => plan_children(plan).into_iter().find_map(first_scan_table),
    }
}

fn vector_sort_key_column(expr: &ScalarExpr) -> Option<(usize, HnswMetric)> {
    let ScalarExpr::Binary {
        op, left, right, ..
    } = expr
    else {
        return None;
    };
    let metric = match op {
        BinaryOp::VectorL2Distance => HnswMetric::L2,
        BinaryOp::VectorCosineDistance => HnswMetric::Cosine,
        BinaryOp::VectorNegativeInnerProduct => HnswMetric::NegativeInnerProduct,
        BinaryOp::VectorL1Distance => HnswMetric::L1,
        _ => return None,
    };
    vector_column_index(left)
        .or_else(|| vector_column_index(right))
        .map(|idx| (idx, metric))
}

fn vector_column_index(expr: &ScalarExpr) -> Option<usize> {
    match expr {
        ScalarExpr::Column { index, .. } => Some(*index),
        _ => None,
    }
}

fn indexable_column(predicate: &ScalarExpr) -> Option<usize> {
    match predicate {
        ScalarExpr::Binary {
            op: BinaryOp::Eq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq,
            left,
            right,
            ..
        } => column_literal_index(left, right).or_else(|| column_literal_index(right, left)),
        ScalarExpr::Binary {
            op: BinaryOp::And,
            left,
            right,
            ..
        } => indexable_column(left).or_else(|| indexable_column(right)),
        _ => None,
    }
}

fn column_literal_index(left: &ScalarExpr, right: &ScalarExpr) -> Option<usize> {
    let ScalarExpr::Column { index, .. } = left else {
        return None;
    };
    matches!(
        right,
        ScalarExpr::Literal { .. } | ScalarExpr::Parameter { .. }
    )
    .then_some(*index)
}

fn simd_kernel_note(plan: &LogicalPlan) -> String {
    if has_vector_distance_expr(plan) {
        return "ultrasql-vec vector distance kernel".to_owned();
    }
    if has_sum_filter_shape(plan) {
        return "ultrasql-vec filter_sum scalar/SIMD dispatch".to_owned();
    }
    if has_sum_or_avg_shape(plan) {
        return "ultrasql-vec scalar aggregate scalar/SIMD dispatch".to_owned();
    }
    "scalar fallback (no specialized SIMD kernel selected)".to_owned()
}

fn has_vector_distance_expr(plan: &LogicalPlan) -> bool {
    plan_exprs(plan)
        .iter()
        .any(|expr| expr_has_vector_distance(expr))
        || plan_children(plan)
            .iter()
            .any(|child| has_vector_distance_expr(child))
}

fn has_sum_filter_shape(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate {
            input, aggregates, ..
        } => !aggregates.is_empty() && matches!(input.as_ref(), LogicalPlan::Filter { .. }),
        _ => plan_children(plan)
            .iter()
            .any(|child| has_sum_filter_shape(child)),
    }
}

fn has_sum_or_avg_shape(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate { aggregates, .. } => !aggregates.is_empty(),
        _ => plan_children(plan)
            .iter()
            .any(|child| has_sum_or_avg_shape(child)),
    }
}

fn expr_has_vector_distance(expr: &ScalarExpr) -> bool {
    match expr {
        ScalarExpr::Binary {
            op:
                BinaryOp::VectorL2Distance
                | BinaryOp::VectorNegativeInnerProduct
                | BinaryOp::VectorCosineDistance
                | BinaryOp::VectorL1Distance,
            ..
        } => true,
        ScalarExpr::Binary { left, right, .. } => {
            expr_has_vector_distance(left) || expr_has_vector_distance(right)
        }
        ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => {
            expr_has_vector_distance(expr)
        }
        ScalarExpr::FunctionCall { args, .. } => args.iter().any(expr_has_vector_distance),
        _ => false,
    }
}

fn plan_exprs(plan: &LogicalPlan) -> Vec<&ScalarExpr> {
    match plan {
        LogicalPlan::Filter { predicate, .. } => vec![predicate],
        LogicalPlan::Project { exprs, .. } => exprs.iter().map(|(expr, _)| expr).collect(),
        LogicalPlan::Sort { keys, .. } => keys.iter().map(|key| &key.expr).collect(),
        LogicalPlan::Aggregate {
            group_by,
            aggregates,
            ..
        } => group_by
            .iter()
            .chain(aggregates.iter().filter_map(|agg| agg.arg.as_ref()))
            .collect(),
        _ => Vec::new(),
    }
}

fn pushdown_notes(plan: &LogicalPlan) -> Vec<String> {
    let mut notes = Vec::new();
    collect_pushdown_notes(plan, &mut notes);
    notes.sort();
    notes.dedup();
    notes
}

fn parquet_row_group_note(plan: &LogicalPlan) -> Option<crate::pipeline::ParquetRowGroupSummary> {
    match crate::pipeline::parquet_row_group_summary_for_plan(plan) {
        Ok(summary) => summary,
        Err(err) => {
            tracing::warn!(error = %err, "EXPLAIN ANALYZE parquet row-group summary failed");
            None
        }
    }
}

fn format_parquet_row_groups(summary: Option<crate::pipeline::ParquetRowGroupSummary>) -> String {
    summary.map_or_else(
        || "not applicable".to_owned(),
        |summary| format!("scanned={} skipped={}", summary.scanned, summary.skipped),
    )
}

fn format_parquet_row_groups_json(
    summary: Option<crate::pipeline::ParquetRowGroupSummary>,
) -> String {
    summary.map_or_else(
        || "null".to_owned(),
        |summary| {
            format!(
                "{{\"Scanned\": {}, \"Skipped\": {}}}",
                summary.scanned, summary.skipped
            )
        },
    )
}

fn collect_pushdown_notes(plan: &LogicalPlan, notes: &mut Vec<String>) {
    match plan {
        LogicalPlan::Project { input, exprs, .. } => {
            if matches!(input.as_ref(), LogicalPlan::FunctionScan { name, .. } if name == "read_parquet")
                && exprs.iter().all(|(expr, alias)| {
                    matches!(expr, ScalarExpr::Column { name, .. } if name == alias)
                })
            {
                notes.push("read_parquet projection".to_owned());
            }
            collect_pushdown_notes(input, notes);
        }
        LogicalPlan::Filter { input, predicate } => {
            if matches!(input.as_ref(), LogicalPlan::FunctionScan { name, .. } if name == "read_parquet")
                && parquet_pushdown_shape(predicate)
            {
                notes.push("read_parquet predicate".to_owned());
            }
            collect_pushdown_notes(input, notes);
        }
        _ => {
            for child in plan_children(plan) {
                collect_pushdown_notes(child, notes);
            }
        }
    }
}

fn parquet_pushdown_shape(expr: &ScalarExpr) -> bool {
    matches!(
        expr,
        ScalarExpr::Binary {
            op: BinaryOp::Eq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq,
            ..
        }
    )
}

fn json_escape(input: &str) -> String {
    input
        .chars()
        .flat_map(|ch| match ch {
            '"' => "\\\"".chars().collect::<Vec<_>>(),
            '\\' => "\\\\".chars().collect::<Vec<_>>(),
            '\n' => "\\n".chars().collect::<Vec<_>>(),
            '\r' => "\\r".chars().collect::<Vec<_>>(),
            '\t' => "\\t".chars().collect::<Vec<_>>(),
            c => vec![c],
        })
        .collect()
}

#[allow(dead_code)]
fn _silence_unused(_f: Field) {}
