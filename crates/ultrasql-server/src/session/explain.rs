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
//!   number alone is sufficient for ORM behavior (psycopg /
//!   tokio-postgres surface the rows just fine).

use std::sync::Arc;
use std::time::Instant;

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_catalog::CatalogSnapshot;
use ultrasql_core::{DataType, Value};
use ultrasql_executor::{ExecError, Operator};
use ultrasql_planner::{BinaryOp, ExplainFormat, LogicalIndexMethod, LogicalPlan, ScalarExpr};
use ultrasql_protocol::messages::{BackendMessage, FieldDescription};
use ultrasql_storage::access_method::HnswMetric;
use ultrasql_vec::Batch;
use ultrasql_vec::column::Column;

use super::Session;
use crate::error::ServerError;
use crate::pipeline::LowerCtx;
use crate::result_encoder::SelectResult;
use crate::{RunPlanInTxnArgs, run_plan_in_txn};

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
        let started = Instant::now();
        if !matches!(
            inner,
            LogicalPlan::Insert { .. } | LogicalPlan::Update { .. } | LogicalPlan::Delete { .. }
        ) {
            let scan = self.run_explain_select_analyze(inner, catalog_snapshot)?;
            let notes = self.explain_notes(inner, catalog_snapshot);
            return Ok(ExplainActuals {
                rows: scan.rows,
                batches: scan.batches,
                peak_output_memory_bytes: scan.peak_output_memory_bytes,
                disk_spill: DiskSpillSummary::from_profile(scan.operator_profile.as_ref()),
                elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
                simd_kernel: notes.simd_kernel,
                index_decision: notes.index_decision,
                vector_index: notes.vector_index,
                late_materialization: notes.late_materialization,
                aggregating_index: notes.aggregating_index,
                pushdowns_applied: notes.pushdowns_applied,
                parquet_row_groups: notes.parquet_row_groups,
                parquet_columns_read: notes.parquet_columns_read,
                operator_profile: scan.operator_profile,
            });
        }

        let notes = self.explain_notes(inner, catalog_snapshot);
        let txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let mut stream_buf = bytes::BytesMut::new();
        let outcome = run_plan_in_txn(RunPlanInTxnArgs {
            plan: inner,
            txn: &txn,
            catalog_snapshot: Arc::clone(catalog_snapshot),
            table_constraints: Arc::clone(&self.state.table_constraints),
            sequences: Arc::clone(&self.state.sequences),
            sequence_owners: Arc::clone(&self.state.sequence_owners),
            sequence_namespaces: Arc::clone(&self.state.sequence_namespaces),
            schemas: Arc::clone(&self.state.schemas),
            operators: Arc::clone(&self.state.operators),
            role_catalog: Arc::clone(&self.state.role_catalog),
            privilege_catalog: Arc::clone(&self.state.privilege_catalog),
            row_security: Arc::clone(&self.state.row_security),
            session_settings: Arc::new(self.session_settings.clone()),
            current_user: self.current_user.clone(),
            session_user: self.auth_user.clone(),
            persistent_catalog: Arc::clone(&self.state.persistent_catalog),
            time_partitions: Arc::clone(&self.state.time_partitions),
            workload_recorder: Arc::clone(&self.state.workload_recorder),
            autovacuum_config: self.state.autovacuum_config(),
            logging_config: self.state.logging_config(),
            wal_archive_config: self.state.wal_archive_config(),
            data_dir: self.state.data_dir.clone(),
            logical_replication: Arc::clone(&self.state.logical_replication),
            sequence_state: Some(self.sequence_state.clone()),
            advisory_state: Some(self.advisory_state.clone()),
            tables: &self.state.tables,
            heap: Arc::clone(&self.state.heap),
            vm: Arc::clone(&self.state.vm),
            oracle: Arc::clone(&self.state.txn_manager),
            jit: self.jit_config(),
            cancel_flag: Some(self.cancel_flag.clone()),
            stream_buf: &mut stream_buf,
        });
        // Always commit the read-only ANALYZE txn — we don't surface
        // its results, only the row count buried in the `SelectResult`.
        let rows = match outcome {
            Ok(result) => result.rows,
            Err(e) => {
                return Err(self.rollback_explain_analyze_transaction_after_error(
                    txn,
                    e,
                    "EXPLAIN ANALYZE rollback after execution error",
                ));
            }
        };
        self.finalise_read_transaction(txn, "EXPLAIN ANALYZE commit")?;
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
            parquet_columns_read: notes.parquet_columns_read,
            operator_profile: None,
        })
    }

    pub(crate) fn rollback_explain_analyze_transaction_after_error(
        &self,
        txn: ultrasql_txn::Transaction,
        original: ServerError,
        context: &'static str,
    ) -> ServerError {
        self.rollback_transaction_after_error(txn, original, context)
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
                sequence_owners: Arc::clone(&self.state.sequence_owners),
                sequence_namespaces: Arc::clone(&self.state.sequence_namespaces),
                schemas: Arc::clone(&self.state.schemas),
                operators: Arc::clone(&self.state.operators),
                role_catalog: Arc::clone(&self.state.role_catalog),
                privilege_catalog: Arc::clone(&self.state.privilege_catalog),
                row_security: Arc::clone(&self.state.row_security),
                session_settings: Arc::new(self.session_settings.clone()),
                current_user: self.current_user.clone(),
                session_user: self.auth_user.clone(),
                persistent_catalog: Arc::clone(&self.state.persistent_catalog),
                time_partitions: Arc::clone(&self.state.time_partitions),
                workload_recorder: Arc::clone(&self.state.workload_recorder),
                autovacuum_config: self.state.autovacuum_config(),
                logging_config: self.state.logging_config(),
                wal_archive_config: self.state.wal_archive_config(),
                data_dir: self.state.data_dir.clone(),
                logical_replication: Arc::clone(&self.state.logical_replication),
                sequence_state: Some(self.sequence_state.clone()),
                advisory_state: Some(self.advisory_state.clone()),
                heap: Arc::clone(&self.state.heap),
                vm: Arc::clone(&self.state.vm),
                snapshot: txn.snapshot.clone(),
                isolation: txn.isolation,
                oracle: Arc::clone(&self.state.txn_manager),
                xid: txn.current_xid(),
                command_id: txn.current_command,
                cte_buffers: std::collections::HashMap::new(),
                jit: self.jit_config(),
                cancel_flag: Some(self.cancel_flag.clone()),
                work_mem: Arc::new(ultrasql_executor::work_mem::WorkMemBudget::new(u64::MAX)),
                profile_operators: true,
            };
            let mut op = crate::pipeline::lower_query(inner, &ctx)?;
            let mut actuals = drain_explain_operator(op.as_mut())?;
            actuals.operator_profile = op.runtime_profile();
            Ok(actuals)
        })();

        match outcome {
            Ok(actuals) => {
                self.finalise_read_transaction(txn, "EXPLAIN ANALYZE select commit")?;
                Ok(actuals)
            }
            Err(e) => Err(self.rollback_explain_analyze_transaction_after_error(
                txn,
                e,
                "EXPLAIN ANALYZE select rollback after execution error",
            )),
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
            parquet_columns_read: parquet_columns_read_note(plan),
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
        let Some((table, col_idx, metric, has_filter)) = first_vector_sort_scan(plan) else {
            return "not applicable (no ORDER BY vector distance LIMIT shape)".to_owned();
        };
        let Some(table_entry) = catalog_snapshot.tables.get(&table.to_ascii_lowercase()) else {
            return format!("skipped {table}: not a persistent catalog table");
        };
        let Ok(attnum) = u16::try_from(col_idx) else {
            return format!("skipped {table}: vector column index exceeds attnum");
        };
        let Some(indexes) = catalog_snapshot.indexes_by_table.get(&table_entry.oid) else {
            return exact_vector_fallback_note("no matching vector index");
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
                let Some(hnsw) = metadata.hnsw.as_ref() else {
                    return format!("skipped {}: page-backed hnsw unavailable", index.name);
                };
                if hnsw.metric() != metric || !hnsw.is_available() {
                    return format!("skipped {}: page-backed hnsw unavailable", index.name);
                }
                if has_filter {
                    return format!(
                        "method=exact index={} fallback_used=true fallback_reason=filtered vector top-k requires exact recheck recall_mode=n/a",
                        index.name
                    );
                }
                let stats = hnsw.page_stats();
                return format!(
                    "selected {} (page-backed hnsw); method=hnsw candidates_scanned={} exact_rerank_count={} recall_mode=n/a fallback_used=false deleted_candidates_skipped={}",
                    index.name, stats.live_nodes, stats.live_nodes, stats.tombstones
                );
            }
            if metadata.method == LogicalIndexMethod::IvfFlat {
                let Some(ivfflat) = metadata.ivfflat.as_ref() else {
                    return format!("skipped {}: page-backed ivfflat unavailable", index.name);
                };
                if ivfflat.metric() != metric || !ivfflat.is_available() {
                    return format!("skipped {}: page-backed ivfflat unavailable", index.name);
                }
                if has_filter {
                    return format!(
                        "method=exact index={} fallback_used=true fallback_reason=filtered vector top-k requires exact recheck recall_mode=n/a",
                        index.name
                    );
                }
                let stats = ivfflat.page_stats();
                return format!(
                    "selected {} (page-backed ivfflat); method=ivfflat candidates_scanned={} exact_rerank_count={} recall_mode=n/a fallback_used=false deleted_candidates_skipped={}",
                    index.name, stats.live_entries, stats.live_entries, stats.tombstones
                );
            }
        }
        exact_vector_fallback_note("no matching vector index")
    }

    fn late_materialization_note(
        &self,
        plan: &LogicalPlan,
        catalog_snapshot: &Arc<CatalogSnapshot>,
    ) -> String {
        let txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let outcome = {
            let ctx = LowerCtx {
                tables: &self.state.tables,
                catalog_snapshot: Arc::clone(catalog_snapshot),
                table_constraints: Arc::clone(&self.state.table_constraints),
                sequences: Arc::clone(&self.state.sequences),
                sequence_owners: Arc::clone(&self.state.sequence_owners),
                sequence_namespaces: Arc::clone(&self.state.sequence_namespaces),
                schemas: Arc::clone(&self.state.schemas),
                operators: Arc::clone(&self.state.operators),
                role_catalog: Arc::clone(&self.state.role_catalog),
                privilege_catalog: Arc::clone(&self.state.privilege_catalog),
                row_security: Arc::clone(&self.state.row_security),
                session_settings: Arc::new(self.session_settings.clone()),
                current_user: self.current_user.clone(),
                session_user: self.auth_user.clone(),
                persistent_catalog: Arc::clone(&self.state.persistent_catalog),
                time_partitions: Arc::clone(&self.state.time_partitions),
                workload_recorder: Arc::clone(&self.state.workload_recorder),
                autovacuum_config: self.state.autovacuum_config(),
                logging_config: self.state.logging_config(),
                wal_archive_config: self.state.wal_archive_config(),
                data_dir: self.state.data_dir.clone(),
                logical_replication: Arc::clone(&self.state.logical_replication),
                sequence_state: Some(self.sequence_state.clone()),
                advisory_state: Some(self.advisory_state.clone()),
                heap: Arc::clone(&self.state.heap),
                vm: Arc::clone(&self.state.vm),
                snapshot: txn.snapshot.clone(),
                isolation: txn.isolation,
                oracle: Arc::clone(&self.state.txn_manager),
                xid: txn.current_xid(),
                command_id: txn.current_command,
                cte_buffers: std::collections::HashMap::new(),
                jit: self.jit_config(),
                cancel_flag: Some(self.cancel_flag.clone()),
                work_mem: Arc::new(ultrasql_executor::work_mem::WorkMemBudget::new(u64::MAX)),
                profile_operators: false,
            };
            crate::pipeline::late_materialization_summary_for_plan(plan, &ctx)
        };
        match outcome {
            Ok(summary) => {
                match self.finalise_read_transaction(
                    txn,
                    "EXPLAIN ANALYZE late materialization note commit",
                ) {
                    Ok(()) => summary.note,
                    Err(err) => format!("skipped: {err}"),
                }
            }
            Err(e) => {
                let err = self.rollback_explain_analyze_transaction_after_error(
                    txn,
                    e,
                    "EXPLAIN ANALYZE late materialization note rollback after summary error",
                );
                format!("skipped: {err}")
            }
        }
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
    parquet_columns_read: Option<Vec<String>>,
    operator_profile: Option<ultrasql_executor::OperatorRuntimeProfile>,
}

struct ExplainScanActuals {
    rows: u64,
    batches: u64,
    peak_output_memory_bytes: u64,
    operator_profile: Option<ultrasql_executor::OperatorRuntimeProfile>,
}

struct ExplainNotes {
    simd_kernel: String,
    index_decision: String,
    vector_index: String,
    late_materialization: String,
    aggregating_index: String,
    pushdowns_applied: Vec<String>,
    parquet_row_groups: Option<crate::pipeline::ParquetRowGroupSummary>,
    parquet_columns_read: Option<Vec<String>>,
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

    fn from_profile(profile: Option<&ultrasql_executor::OperatorRuntimeProfile>) -> Self {
        let Some(profile) = profile else {
            return Self::none();
        };
        let (spills, bytes) = profile_spill_totals(profile);
        if spills == 0 {
            Self::none()
        } else {
            Self {
                used: true,
                bytes,
                reason: "operator profile reported spill files",
            }
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
             Parquet Row Groups: {}\n\
             Parquet Columns Read: {}\n",
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
            format_parquet_row_groups(a.parquet_row_groups),
            format_parquet_columns_read(a.parquet_columns_read.as_deref())
        ));
        if let Some(profile) = &a.operator_profile {
            body.push_str("Operator Metrics:\n");
            write_operator_metrics_text(profile, 1, &mut body);
        }
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
             \n    \"Parquet Row Groups\": {},\
             \n    \"Parquet Columns Read\": {},\
             \n    \"Operator Metrics\": {}",
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
            format_parquet_row_groups_json(a.parquet_row_groups),
            format_parquet_columns_read_json(a.parquet_columns_read.as_deref()),
            format_operator_metrics_json(a.operator_profile.as_ref())
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
        LogicalPlan::Merge { .. } => "Merge",
        LogicalPlan::Empty { .. } => "Empty",
        LogicalPlan::Truncate { .. } => "Truncate",
        LogicalPlan::CreateTable { .. } => "CreateTable",
        LogicalPlan::CreateMaterializedView { .. } => "CreateMaterializedView",
        LogicalPlan::CreateTypeEnum { .. } => "CreateTypeEnum",
        LogicalPlan::CreateTypeComposite { .. } => "CreateTypeComposite",
        LogicalPlan::CreateDomain { .. } => "CreateDomain",
        LogicalPlan::CreateOperator { .. } => "CreateOperator",
        LogicalPlan::CreateIndex { .. } => "CreateIndex",
        LogicalPlan::DropIndex { .. } => "DropIndex",
        LogicalPlan::CreatePolicy { .. } => "CreatePolicy",
        LogicalPlan::CreateRole { .. } => "CreateRole",
        LogicalPlan::AlterRole { .. } => "AlterRole",
        LogicalPlan::DropRole { .. } => "DropRole",
        LogicalPlan::GrantPrivileges { .. } => "GrantPrivileges",
        LogicalPlan::RevokePrivileges { .. } => "RevokePrivileges",
        LogicalPlan::AlterDefaultPrivileges { .. } => "AlterDefaultPrivileges",
        LogicalPlan::GrantRole { .. } => "GrantRole",
        LogicalPlan::RevokeRole { .. } => "RevokeRole",
        LogicalPlan::CreateSchema { .. } => "CreateSchema",
        LogicalPlan::DropSchema { .. } => "DropSchema",
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
        LogicalPlan::Describe { .. } => "Describe",
        LogicalPlan::Checkpoint { .. } => "Checkpoint",
        LogicalPlan::SetRole { .. } => "SetRole",
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
        LogicalPlan::Merge { source, .. } => vec![source],
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
        batches = checked_explain_counter_add(batches, 1, "batches")?;
        rows = checked_explain_counter_add(rows, batch.rows(), "rows")?;
        peak_output_memory_bytes = peak_output_memory_bytes.max(usize_to_explain_u64(
            estimate_batch_memory(&batch),
            "memory bytes",
        )?);
    }
    Ok(ExplainScanActuals {
        rows,
        batches,
        peak_output_memory_bytes,
        operator_profile: None,
    })
}

fn checked_explain_counter_add(
    current: u64,
    delta: usize,
    label: &'static str,
) -> Result<u64, ServerError> {
    let delta = usize_to_explain_u64(delta, label)?;
    current.checked_add(delta).ok_or_else(|| {
        ServerError::Execute(ExecError::TypeMismatch(format!(
            "EXPLAIN ANALYZE {label} counter overflow"
        )))
    })
}

fn usize_to_explain_u64(value: usize, label: &'static str) -> Result<u64, ServerError> {
    u64::try_from(value).map_err(|_| {
        ServerError::Execute(ExecError::TypeMismatch(format!(
            "EXPLAIN ANALYZE {label} value exceeds u64"
        )))
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

fn write_operator_metrics_text(
    profile: &ultrasql_executor::OperatorRuntimeProfile,
    depth: usize,
    out: &mut String,
) {
    let indent = "  ".repeat(depth);
    let pruning = if profile.pruning.is_empty() {
        "none".to_owned()
    } else {
        profile.pruning.join(",")
    };
    out.push_str(&format!(
        "{indent}operator={} rows_in={} rows_out={} batches={} time_us={} \
         memory_bytes={} spills={} spill_bytes={} io_bytes={} pruning={}\n",
        profile.operator,
        profile.rows_in,
        profile.rows_out,
        profile.batches,
        profile.time_us,
        profile.memory_bytes,
        profile.spills,
        profile.spill_bytes,
        profile.io_bytes,
        pruning
    ));
    for child in &profile.children {
        write_operator_metrics_text(child, depth + 1, out);
    }
}

fn format_operator_metrics_json(
    profile: Option<&ultrasql_executor::OperatorRuntimeProfile>,
) -> String {
    profile.map_or_else(|| "null".to_owned(), operator_profile_json)
}

fn operator_profile_json(profile: &ultrasql_executor::OperatorRuntimeProfile) -> String {
    let pruning = profile
        .pruning
        .iter()
        .map(|entry| format!("\"{}\"", json_escape(entry)))
        .collect::<Vec<_>>()
        .join(", ");
    let children = profile
        .children
        .iter()
        .map(operator_profile_json)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "{{\"Operator\":\"{}\",\"Rows In\":{},\"Rows Out\":{},\"Batches\":{},\
         \"Time Us\":{},\"Memory Bytes\":{},\"Spills\":{},\"Spill Bytes\":{},\
         \"IO Bytes\":{},\"Pruning\":[{}],\"Children\":[{}]}}",
        json_escape(&profile.operator),
        profile.rows_in,
        profile.rows_out,
        profile.batches,
        profile.time_us,
        profile.memory_bytes,
        profile.spills,
        profile.spill_bytes,
        profile.io_bytes,
        pruning,
        children
    )
}

fn profile_spill_totals(profile: &ultrasql_executor::OperatorRuntimeProfile) -> (u64, u64) {
    profile.children.iter().fold(
        (profile.spills, profile.spill_bytes),
        |(spills, bytes), child| {
            let (child_spills, child_bytes) = profile_spill_totals(child);
            (
                spills.saturating_add(child_spills),
                bytes.saturating_add(child_bytes),
            )
        },
    )
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

fn first_vector_sort_scan(plan: &LogicalPlan) -> Option<(&str, usize, HnswMetric, bool)> {
    match plan {
        LogicalPlan::Limit { input, .. } | LogicalPlan::Project { input, .. } => {
            first_vector_sort_scan(input)
        }
        LogicalPlan::Sort { input, keys } => {
            let key = keys.iter().find(|key| key.asc)?;
            let (col_idx, metric) = vector_sort_key_column(&key.expr)?;
            let table = first_scan_table(input)?;
            Some((table, col_idx, metric, contains_filter(input)))
        }
        _ => plan_children(plan)
            .into_iter()
            .find_map(first_vector_sort_scan),
    }
}

fn contains_filter(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Filter { .. } => true,
        _ => plan_children(plan).into_iter().any(contains_filter),
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
    if has_exact_vector_top_k_shape(plan) {
        return "ultrasql-vec exact_top_k_f32 kernel".to_owned();
    }
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

fn exact_vector_fallback_note(reason: &str) -> String {
    format!("method=exact fallback_used=true fallback_reason={reason} recall_mode=n/a")
}

fn has_exact_vector_top_k_shape(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Limit {
            input, n, offset, ..
        } => *n != 0 && *n != u64::MAX && *offset == 0 && exact_vector_top_k_input(input),
        _ => plan_children(plan)
            .iter()
            .any(|child| has_exact_vector_top_k_shape(child)),
    }
}

fn exact_vector_top_k_input(input: &LogicalPlan) -> bool {
    match input {
        LogicalPlan::Sort { keys, .. } => exact_vector_top_k_keys(keys),
        LogicalPlan::Project { input, .. } => {
            let LogicalPlan::Sort { keys, .. } = input.as_ref() else {
                return false;
            };
            exact_vector_top_k_keys(keys)
        }
        _ => false,
    }
}

fn exact_vector_top_k_keys(keys: &[ultrasql_planner::SortKey]) -> bool {
    let [key] = keys else {
        return false;
    };
    if !key.asc || key.nulls_first {
        return false;
    }
    let ScalarExpr::Binary {
        op, left, right, ..
    } = &key.expr
    else {
        return false;
    };
    matches!(
        op,
        BinaryOp::VectorL2Distance
            | BinaryOp::VectorCosineDistance
            | BinaryOp::VectorNegativeInnerProduct
            | BinaryOp::VectorL1Distance
    ) && (exact_dense_vector_column_probe(left, right)
        || exact_dense_vector_column_probe(right, left))
}

fn exact_dense_vector_column_probe(column: &ScalarExpr, probe: &ScalarExpr) -> bool {
    let ScalarExpr::Column {
        data_type: DataType::Vector { .. } | DataType::HalfVec { .. },
        ..
    } = column
    else {
        return false;
    };
    matches!(
        probe,
        ScalarExpr::Literal {
            value: Value::Vector(_) | Value::HalfVec(_),
            ..
        }
    )
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

fn parquet_columns_read_note(plan: &LogicalPlan) -> Option<Vec<String>> {
    match crate::pipeline::parquet_columns_read_for_plan(plan) {
        Ok(columns) => columns,
        Err(err) => {
            tracing::warn!(error = %err, "EXPLAIN ANALYZE parquet columns_read summary failed");
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

fn format_parquet_columns_read(columns: Option<&[String]>) -> String {
    columns.map_or_else(
        || "not applicable".to_owned(),
        |columns| format!("columns_read={} count={}", columns.join(","), columns.len()),
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

fn format_parquet_columns_read_json(columns: Option<&[String]>) -> String {
    columns.map_or_else(
        || "null".to_owned(),
        |columns| {
            let quoted = columns
                .iter()
                .map(|column| format!("\"{}\"", json_escape(column)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{{\"Columns\": [{quoted}], \"Count\": {}}}", columns.len())
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use tokio::io::{DuplexStream, duplex};
    use ultrasql_core::{Field, Schema};
    use ultrasql_executor::MemTableScan;
    use ultrasql_planner::{
        AggregateFunc, LogicalAggregateExpr, LogicalJoinCondition, LogicalJoinType, LogicalSetOp,
        LogicalSetQuantifier,
    };
    use ultrasql_txn::IsolationLevel;
    use ultrasql_vec::column::{Column, NumericColumn, StringColumn};

    use crate::Server;

    fn schema() -> Schema {
        Schema::new([Field::required("id", DataType::Int32)]).expect("schema")
    }

    fn scan(name: &str) -> LogicalPlan {
        LogicalPlan::Scan {
            table: name.to_owned(),
            schema: schema(),
            projection: None,
        }
    }

    fn col(name: &str, index: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.to_owned(),
            index,
            data_type: DataType::Int32,
        }
    }

    fn lit_i32(value: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(value),
            data_type: DataType::Int32,
        }
    }

    fn test_session() -> Session<DuplexStream> {
        let (io, _peer) = duplex(64);
        Session::new(io, Arc::new(Server::with_sample_database()))
    }

    #[test]
    fn explain_analyze_cleanup_reports_abort_failure_with_original_error() {
        let session = test_session();
        let txn = session
            .state
            .txn_manager
            .begin(IsolationLevel::ReadCommitted);
        let stale = txn.clone();
        session.state.txn_manager.abort(txn).expect("pre-abort");

        let err = session.rollback_explain_analyze_transaction_after_error(
            stale,
            ServerError::Unsupported("analyze boom"),
            "EXPLAIN ANALYZE rollback after execution error",
        );
        let msg = err.to_string();
        assert!(
            msg.contains("EXPLAIN ANALYZE rollback after execution error"),
            "unexpected error: {err}"
        );
        assert!(msg.contains("analyze boom"), "original error lost: {err}");
        assert!(
            msg.contains("transaction abort failed"),
            "abort failure hidden: {err}"
        );
    }

    #[test]
    fn render_text_and_json_include_analyze_evidence_and_escape_metadata() {
        let plan = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan("items")),
                predicate: ScalarExpr::Binary {
                    op: BinaryOp::Gt,
                    left: Box::new(col("id", 0)),
                    right: Box::new(lit_i32(10)),
                    data_type: DataType::Bool,
                },
            }),
            n: 5,
            offset: 1,
        };
        let actuals = ExplainActuals {
            rows: 4,
            batches: 1,
            peak_output_memory_bytes: 64,
            disk_spill: DiskSpillSummary {
                used: true,
                bytes: 128,
                reason: "quoted \"spill\" path",
            },
            elapsed_ms: 1.25,
            simd_kernel: "scalar".to_owned(),
            index_decision: "seq".to_owned(),
            vector_index: "none".to_owned(),
            late_materialization: "not applicable".to_owned(),
            aggregating_index: "none".to_owned(),
            pushdowns_applied: vec!["read_parquet projection".to_owned()],
            parquet_row_groups: Some(crate::pipeline::ParquetRowGroupSummary {
                scanned: 2,
                skipped: 3,
            }),
            parquet_columns_read: Some(vec!["a\"b".to_owned(), "c\\d".to_owned()]),
            operator_profile: None,
        };

        let text = render_text(&plan, Some(&actuals));
        assert!(text.contains("Actual Rows: 4"));
        assert!(text.contains("Disk Spill: yes"));
        assert!(text.contains("Parquet Row Groups: scanned=2 skipped=3"));
        let json = render_json(&plan, Some(&actuals));
        assert!(json.contains("\"Node Type\": \"Limit\""));
        assert!(json.contains("\"Disk Spill\": {\"Used\": true"));
        assert!(json.contains(r#""a\"b""#));
        assert!(json.contains(r#""c\\d""#));
        assert_eq!(
            format_parquet_columns_read_json(Some(&["x".to_owned()])),
            r#"{"Columns": ["x"], "Count": 1}"#
        );
        assert_eq!(json_escape("a\nb\rc\td\\e\"f"), "a\\nb\\rc\\td\\\\e\\\"f");
    }

    #[test]
    fn plan_children_node_types_and_pushdown_notes_cover_nested_shapes() {
        let function = LogicalPlan::FunctionScan {
            name: "read_parquet".to_owned(),
            args: vec![],
            schema: schema(),
        };
        let project = LogicalPlan::Project {
            input: Box::new(function.clone()),
            exprs: vec![(col("id", 0), "id".to_owned())],
            schema: schema(),
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(function),
            predicate: ScalarExpr::Binary {
                op: BinaryOp::LtEq,
                left: Box::new(col("id", 0)),
                right: Box::new(lit_i32(99)),
                data_type: DataType::Bool,
            },
        };
        let join = LogicalPlan::Join {
            left: Box::new(project),
            right: Box::new(filter),
            join_type: LogicalJoinType::Inner,
            condition: LogicalJoinCondition::None,
            schema: Schema::new([
                Field::required("l", DataType::Int32),
                Field::required("r", DataType::Int32),
            ])
            .expect("join schema"),
        };
        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(join),
            group_by: vec![col("l", 0)],
            aggregates: vec![LogicalAggregateExpr {
                func: AggregateFunc::Sum,
                arg: Some(col("r", 1)),
                direct_arg: None,
                order_by: None,
                distinct: false,
                output_name: "sum".to_owned(),
                data_type: DataType::Int64,
            }],
            schema: schema(),
        };
        let set_op = LogicalPlan::SetOp {
            op: LogicalSetOp::Union,
            quantifier: LogicalSetQuantifier::Distinct,
            left: Box::new(aggregate),
            right: Box::new(scan("fallback")),
            schema: schema(),
        };

        assert_eq!(plan_node_type(&set_op), "Set Op");
        assert_eq!(plan_children(&set_op).len(), 2);
        let notes = pushdown_notes(&set_op);
        assert!(notes.contains(&"read_parquet projection".to_owned()));
        assert!(notes.contains(&"read_parquet predicate".to_owned()));
        assert!(parquet_pushdown_shape(&ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(col("id", 0)),
            right: Box::new(lit_i32(1)),
            data_type: DataType::Bool,
        }));
        assert!(!parquet_pushdown_shape(&ScalarExpr::Binary {
            op: BinaryOp::Add,
            left: Box::new(col("id", 0)),
            right: Box::new(lit_i32(1)),
            data_type: DataType::Int32,
        }));
    }

    #[test]
    fn drain_explain_operator_counts_rows_batches_and_memory() {
        let batch = Batch::new(vec![
            Column::Int32(NumericColumn::from_data(vec![1, 2, 3])),
            Column::Utf8(StringColumn::from_data([
                "a".to_owned(),
                "bb".to_owned(),
                "ccc".to_owned(),
            ])),
        ])
        .expect("batch");
        let schema = Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("name", DataType::Text { max_len: None }),
        ])
        .expect("schema");
        let mut op = MemTableScan::new(schema, vec![batch]);
        let actuals = drain_explain_operator(&mut op).expect("drain");
        assert_eq!(actuals.rows, 3);
        assert_eq!(actuals.batches, 1);
        assert!(actuals.peak_output_memory_bytes >= 12);
        assert!(actuals.operator_profile.is_none());

        assert_eq!(format_parquet_row_groups(None), "not applicable");
        assert_eq!(format_parquet_row_groups_json(None), "null");
        assert_eq!(format_parquet_columns_read(None), "not applicable");
        assert_eq!(format_parquet_columns_read_json(None), "null");
    }

    #[test]
    fn explain_counter_add_rejects_overflow() {
        let err = checked_explain_counter_add(u64::MAX, 1, "rows").unwrap_err();
        assert!(matches!(err, ServerError::Execute(_)));
    }
}
