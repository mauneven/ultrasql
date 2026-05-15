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
use ultrasql_planner::{ExplainFormat, LogicalPlan};
use ultrasql_protocol::messages::{BackendMessage, FieldDescription};

use super::Session;
use crate::error::ServerError;
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
            rows,
        })
    }

    /// Execute the wrapped plan to completion, counting rows and timing
    /// wall-clock. Returns `(rows, elapsed_ms)`.
    fn run_explain_analyze(
        &self,
        inner: &LogicalPlan,
        catalog_snapshot: &Arc<CatalogSnapshot>,
    ) -> Result<ExplainActuals, ServerError> {
        let started = Instant::now();
        let txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let outcome = run_plan_in_txn(
            inner,
            &txn,
            Arc::clone(catalog_snapshot),
            &self.state.tables,
            Arc::clone(&self.state.heap),
            Arc::clone(&self.state.txn_manager),
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
            elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
        })
    }
}

/// Wall-clock + row-count summary collected by `EXPLAIN ANALYZE`.
struct ExplainActuals {
    rows: u64,
    elapsed_ms: f64,
}

/// Render the plan as the PostgreSQL-style indented tree.
fn render_text(plan: &LogicalPlan, actuals: Option<&ExplainActuals>) -> String {
    let mut body = plan.display(0);
    if let Some(a) = actuals {
        body.push_str(&format!(
            "Planning Time: 0.000 ms\nExecution Time: {:.3} ms\nActual Rows: {}\n",
            a.elapsed_ms, a.rows
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
        buf.push_str(&format!(
            ",\n    \"Execution Time\": {:.3},\n    \"Actual Rows\": {}",
            a.elapsed_ms, a.rows
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
        LogicalPlan::CreateIndex { .. } => "CreateIndex",
        LogicalPlan::DropTable { .. } => "DropTable",
        LogicalPlan::AlterTable { .. } => "AlterTable",
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
        LogicalPlan::Explain { .. } => "Explain",
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

#[allow(dead_code)]
fn _silence_unused(_f: Field) {}
