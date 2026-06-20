//! MERGE lowering: the `MergeRowSource` operator that pairs target and
//! source rows by the ON predicate, plus `lower_real_merge`.

use std::collections::HashSet;
use std::sync::Arc;

use ultrasql_core::{DataType, Field, RelationId, Schema, Value};
use ultrasql_executor::{
    Eval, ExecError, MergeAction as ExecMergeAction, MergeClause as ExecMergeClause, ModifyKind,
    ModifyTable, ModifyTableStamps, Operator, ValuesScan, batch_to_rows, build_batch,
    eval_error_to_exec_error,
};
use ultrasql_planner::{LogicalMergeAction, LogicalMergeClause, LogicalMergeMatchKind, LogicalPlan, ScalarExpr};

use crate::error::ServerError;
use crate::pipeline::LowerCtx;
use crate::pipeline::lower_query::lower_query;

use super::constraints::{
    build_exclusion_insert_checks, build_exclusion_update_checks, build_foreign_key_checks,
    build_referenced_by_delete_checks,
};
use super::indexes::{build_insert_index_maintainers, build_tid_seq_scan, build_vector_index_maintainers};
use super::insert::{build_rls_insert_checks, build_rls_update_checks, build_sequence_defaults};
use super::referential::build_referenced_by_update_checks;

#[derive(Debug)]
struct MergeRowSource {
    target: Box<dyn Operator>,
    source: Box<dyn Operator>,
    schema: Schema,
    target_width: usize,
    on: Eval,
    clauses: Vec<MergeSourceClause>,
    done: bool,
}

#[derive(Clone, Debug)]
struct MergeSourceClause {
    kind: LogicalMergeMatchKind,
    condition: Option<Eval>,
}

impl MergeRowSource {
    fn new(
        target: Box<dyn Operator>,
        source: Box<dyn Operator>,
        schema: Schema,
        target_width: usize,
        on: Eval,
        clauses: Vec<MergeSourceClause>,
    ) -> Self {
        Self {
            target,
            source,
            schema,
            target_width,
            on,
            clauses,
            done: false,
        }
    }

    fn materialize(child: &mut dyn Operator) -> Result<Vec<Vec<Value>>, ExecError> {
        let schema = child.schema().clone();
        let mut rows = Vec::new();
        while let Some(batch) = child.next_batch()? {
            if batch.rows() == 0 {
                continue;
            }
            rows.extend(batch_to_rows(&batch, &schema)?);
        }
        Ok(rows)
    }

    fn condition_matches(condition: Option<&Eval>, row: &[Value]) -> Result<bool, ExecError> {
        let Some(condition) = condition else {
            return Ok(true);
        };
        match condition.eval(row).map_err(eval_error_to_exec_error)? {
            Value::Bool(true) => Ok(true),
            Value::Bool(false) | Value::Null => Ok(false),
            other => Err(ExecError::TypeMismatch(format!(
                "MERGE WHEN predicate returned {:?}, expected Bool",
                other.data_type()
            ))),
        }
    }

    fn on_matches(&self, row: &[Value]) -> Result<bool, ExecError> {
        match self.on.eval(row).map_err(eval_error_to_exec_error)? {
            Value::Bool(true) => Ok(true),
            Value::Bool(false) | Value::Null => Ok(false),
            other => Err(ExecError::TypeMismatch(format!(
                "MERGE ON predicate returned {:?}, expected Bool",
                other.data_type()
            ))),
        }
    }

    fn choose_clause(
        &self,
        kind: LogicalMergeMatchKind,
        row: &[Value],
    ) -> Result<Option<usize>, ExecError> {
        for (idx, clause) in self.clauses.iter().enumerate() {
            if clause.kind == kind && Self::condition_matches(clause.condition.as_ref(), row)? {
                return Ok(Some(idx));
            }
        }
        Ok(None)
    }

    fn push_merge_row(
        out: &mut Vec<Vec<Value>>,
        clause_idx: usize,
        tid_target_row: &[Value],
        source_row: &[Value],
    ) -> Result<(), ExecError> {
        let clause_i32 = i32::try_from(clause_idx)
            .map_err(|_| ExecError::Internal("MERGE clause index exceeds i32"))?;
        let mut row = Vec::with_capacity(1 + tid_target_row.len() + source_row.len());
        row.push(Value::Int32(clause_i32));
        row.extend_from_slice(tid_target_row);
        row.extend_from_slice(source_row);
        out.push(row);
        Ok(())
    }

    fn target_tid_key(row: &[Value]) -> Result<(i32, i32), ExecError> {
        if row.len() < 2 {
            return Err(ExecError::TypeMismatch(
                "MERGE target row missing TID columns".to_owned(),
            ));
        }
        let Value::Int32(block) = row[0] else {
            return Err(ExecError::TypeMismatch(
                "MERGE target TID block must be Int32".to_owned(),
            ));
        };
        let Value::Int32(slot) = row[1] else {
            return Err(ExecError::TypeMismatch(
                "MERGE target TID slot must be Int32".to_owned(),
            ));
        };
        Ok((block, slot))
    }
}

impl Operator for MergeRowSource {
    fn next_batch(&mut self) -> Result<Option<ultrasql_vec::Batch>, ExecError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let target_rows = Self::materialize(self.target.as_mut())?;
        let source_rows = Self::materialize(self.source.as_mut())?;
        let mut matched_tids = HashSet::new();
        let mut out = Vec::new();
        for source_row in &source_rows {
            let mut source_matched = false;
            for target_row in &target_rows {
                if target_row.len() < 2 + self.target_width {
                    return Err(ExecError::TypeMismatch(format!(
                        "MERGE target row has {} columns, expected at least {}",
                        target_row.len(),
                        2 + self.target_width
                    )));
                }
                let mut eval_row = Vec::with_capacity(self.target_width + source_row.len());
                eval_row.extend_from_slice(&target_row[2..2 + self.target_width]);
                eval_row.extend_from_slice(source_row);
                if !self.on_matches(&eval_row)? {
                    continue;
                }
                source_matched = true;
                let tid_key = Self::target_tid_key(target_row)?;
                if !matched_tids.insert(tid_key) {
                    return Err(ExecError::TypeMismatch(
                        "MERGE source rows matched the same target row more than once".to_owned(),
                    ));
                }
                if let Some(clause_idx) =
                    self.choose_clause(LogicalMergeMatchKind::Matched, &eval_row)?
                {
                    Self::push_merge_row(&mut out, clause_idx, target_row, source_row)?;
                }
            }
            if !source_matched {
                let mut eval_row = vec![Value::Null; self.target_width];
                eval_row.extend_from_slice(source_row);
                if let Some(clause_idx) =
                    self.choose_clause(LogicalMergeMatchKind::NotMatched, &eval_row)?
                {
                    let mut fake_target = Vec::with_capacity(2 + self.target_width);
                    fake_target.push(Value::Int32(-1));
                    fake_target.push(Value::Int32(-1));
                    fake_target.extend((0..self.target_width).map(|_| Value::Null));
                    Self::push_merge_row(&mut out, clause_idx, &fake_target, source_row)?;
                }
            }
        }
        build_batch(&out, &self.schema).map(Some)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn profile_children(&self) -> Vec<&dyn Operator> {
        vec![self.target.as_ref(), self.source.as_ref()]
    }
}

pub(crate) fn lower_real_merge(
    target: &str,
    source: &LogicalPlan,
    on: &ScalarExpr,
    clauses: &[LogicalMergeClause],
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    let entry = ctx
        .catalog_snapshot
        .tables
        .get(&target.to_ascii_lowercase())
        .ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                target.to_string(),
            ))
        })?;
    if ctx
        .time_partitions
        .contains_key(&target.to_ascii_lowercase())
    {
        return Err(ServerError::Unsupported(
            "MERGE on partitioned tables is not yet routed to chunks",
        ));
    }
    if !build_rls_insert_checks(entry, ctx).is_empty()
        || !build_rls_update_checks(entry, ctx).is_empty()
    {
        return Err(ServerError::Unsupported(
            "MERGE does not yet support row-level security checks",
        ));
    }

    let target_scan = build_tid_seq_scan(entry, ctx);
    let source_op = match source {
        LogicalPlan::Values { rows, schema } => {
            Box::new(ValuesScan::new(rows.clone(), schema.clone())) as Box<dyn Operator>
        }
        other => lower_query(other, ctx)?,
    };
    let merge_schema = merge_row_schema(&entry.schema, source.schema())?;
    let source_clauses = clauses
        .iter()
        .map(|clause| MergeSourceClause {
            kind: clause.kind,
            condition: clause.condition.clone().map(Eval::new),
        })
        .collect();
    let merge_source = Box::new(MergeRowSource::new(
        target_scan,
        source_op,
        merge_schema,
        entry.schema.len(),
        Eval::new(on.clone()),
        source_clauses,
    ));

    crate::aggregating_index::mark_aggregating_indexes_dirty(entry, ctx);
    let rel = RelationId(entry.oid);
    let runtime_clauses = clauses
        .iter()
        .map(|clause| ExecMergeClause {
            action: match &clause.action {
                LogicalMergeAction::Update { assignments } => ExecMergeAction::Update {
                    assignments: assignments
                        .iter()
                        .map(|(column, expr)| (*column, Eval::new(expr.clone())))
                        .collect(),
                },
                LogicalMergeAction::Delete => ExecMergeAction::Delete,
                LogicalMergeAction::Insert { columns, values } => ExecMergeAction::Insert {
                    columns: columns.clone(),
                    values: values.iter().cloned().map(Eval::new).collect(),
                },
            },
        })
        .collect();
    let constraints = ctx.table_constraints.get(&entry.oid).map(|c| c.clone());
    let mut modify = ModifyTable::new(
        Arc::clone(&ctx.heap),
        rel,
        entry.schema.clone(),
        ModifyKind::Merge {
            clauses: runtime_clauses,
        },
        ModifyTableStamps::new(ctx.xid, ctx.command_id, ctx.xid, ctx.command_id),
        ctx.heap.wal_sink().cloned(),
        merge_source,
    )
    .with_visibility_map(Arc::clone(&ctx.vm))
    .with_update_extra_eval_columns()
    .with_insert_indexes(build_insert_index_maintainers(entry, ctx)?)
    .with_update_indexes(build_insert_index_maintainers(entry, ctx)?)
    .with_delete_indexes(build_insert_index_maintainers(entry, ctx)?)
    .with_insert_vector_indexes(build_vector_index_maintainers(entry, ctx)?)
    .with_update_vector_indexes(build_vector_index_maintainers(entry, ctx)?)
    .with_delete_vector_indexes(build_vector_index_maintainers(entry, ctx)?);

    if let Some(constraints) = constraints {
        if !constraints.checks.is_empty() {
            modify = modify.with_check_constraints(
                constraints
                    .checks
                    .iter()
                    .map(|check| (check.name.clone(), check.expr.clone()))
                    .collect(),
            );
        }
        modify = modify
            .with_column_defaults(constraints.defaults.clone())
            .with_sequence_defaults(build_sequence_defaults(
                &constraints.sequence_defaults,
                ctx,
            )?)
            .with_identity_always(constraints.identity_always.clone())
            .with_generated_stored(constraints.generated_stored.clone())
            .with_foreign_key_checks(build_foreign_key_checks(&constraints.foreign_keys, ctx)?)
            .with_exclusion_checks(build_exclusion_insert_checks(
                entry,
                &constraints.exclusion_constraints,
                ctx,
            )?)
            .with_exclusion_update_checks(build_exclusion_update_checks(
                entry,
                &constraints.exclusion_constraints,
                ctx,
            )?);
    }
    modify =
        modify.with_referenced_by_update_checks(build_referenced_by_update_checks(entry.oid, ctx)?);
    modify =
        modify.with_referenced_by_delete_checks(build_referenced_by_delete_checks(entry.oid, ctx)?);
    Ok(Box::new(modify))
}

fn merge_row_schema(target_schema: &Schema, source_schema: &Schema) -> Result<Schema, ServerError> {
    let mut fields = Vec::with_capacity(3 + target_schema.len() + source_schema.len());
    fields.push(Field::required("merge_clause", DataType::Int32));
    fields.push(Field::required("tid_block", DataType::Int32));
    fields.push(Field::required("tid_slot", DataType::Int32));
    fields.extend(
        target_schema.fields().iter().map(|field| {
            Field::nullable(format!("target.{}", field.name), field.data_type.clone())
        }),
    );
    fields.extend(
        source_schema.fields().iter().map(|field| {
            Field::nullable(format!("source.{}", field.name), field.data_type.clone())
        }),
    );
    Ok(Schema::new_with_duplicate_names(fields))
}
