//! Plan-cache invalidation and row-level security predicate application.

use ultrasql_planner::{LogicalAggregateExpr, LogicalJoinCondition, SortKey};

use super::*;

/// Apply `rewrite_expr` to a projection's `(expr, alias)` list, returning a
/// new list when any expression changed and `None` otherwise.
fn rewrite_projection_exprs<F>(
    exprs: &[(ScalarExpr, String)],
    rewrite_expr: &mut F,
) -> Result<Option<Vec<(ScalarExpr, String)>>, ServerError>
where
    F: FnMut(&ScalarExpr) -> Result<Option<ScalarExpr>, ServerError>,
{
    let mut changed = false;
    let mut out = Vec::with_capacity(exprs.len());
    for (expr, alias) in exprs {
        match rewrite_expr(expr)? {
            Some(new_expr) => {
                changed = true;
                out.push((new_expr, alias.clone()));
            }
            None => out.push((expr.clone(), alias.clone())),
        }
    }
    Ok(changed.then_some(out))
}

/// Apply `rewrite_expr` to a bare expression list, returning a new list when
/// any expression changed and `None` otherwise.
fn rewrite_expr_list<F>(
    exprs: &[ScalarExpr],
    rewrite_expr: &mut F,
) -> Result<Option<Vec<ScalarExpr>>, ServerError>
where
    F: FnMut(&ScalarExpr) -> Result<Option<ScalarExpr>, ServerError>,
{
    let mut changed = false;
    let mut out = Vec::with_capacity(exprs.len());
    for expr in exprs {
        match rewrite_expr(expr)? {
            Some(new_expr) => {
                changed = true;
                out.push(new_expr);
            }
            None => out.push(expr.clone()),
        }
    }
    Ok(changed.then_some(out))
}

/// Apply `rewrite_expr` to each [`SortKey`]'s expression, returning a new list
/// when any changed and `None` otherwise.
fn rewrite_sort_keys<F>(
    keys: &[SortKey],
    rewrite_expr: &mut F,
) -> Result<Option<Vec<SortKey>>, ServerError>
where
    F: FnMut(&ScalarExpr) -> Result<Option<ScalarExpr>, ServerError>,
{
    let mut changed = false;
    let mut out = Vec::with_capacity(keys.len());
    for key in keys {
        match rewrite_expr(&key.expr)? {
            Some(expr) => {
                changed = true;
                out.push(SortKey {
                    expr,
                    asc: key.asc,
                    nulls_first: key.nulls_first,
                });
            }
            None => out.push(key.clone()),
        }
    }
    Ok(changed.then_some(out))
}

/// Apply `rewrite_expr` to every expression an aggregate call carries (the
/// argument, the direct argument, and a `WITHIN GROUP` sort key), returning a
/// new list when any changed and `None` otherwise.
fn rewrite_aggregate_exprs<F>(
    aggregates: &[LogicalAggregateExpr],
    rewrite_expr: &mut F,
) -> Result<Option<Vec<LogicalAggregateExpr>>, ServerError>
where
    F: FnMut(&ScalarExpr) -> Result<Option<ScalarExpr>, ServerError>,
{
    let mut changed = false;
    let mut out = Vec::with_capacity(aggregates.len());
    for agg in aggregates {
        let new_arg = rewrite_optional_expr(agg.arg.as_ref(), rewrite_expr)?;
        let new_direct = rewrite_optional_expr(agg.direct_arg.as_ref(), rewrite_expr)?;
        let new_order = match agg.order_by.as_ref() {
            Some(key) => rewrite_expr(&key.expr)?.map(|expr| SortKey {
                expr,
                asc: key.asc,
                nulls_first: key.nulls_first,
            }),
            None => None,
        };
        if new_arg.is_none() && new_direct.is_none() && new_order.is_none() {
            out.push(agg.clone());
            continue;
        }
        changed = true;
        out.push(LogicalAggregateExpr {
            func: agg.func,
            arg: new_arg.or_else(|| agg.arg.clone()),
            direct_arg: new_direct.or_else(|| agg.direct_arg.clone()),
            order_by: new_order.or_else(|| agg.order_by.clone()),
            distinct: agg.distinct,
            output_name: agg.output_name.clone(),
            data_type: agg.data_type.clone(),
        });
    }
    Ok(changed.then_some(out))
}

/// Apply `rewrite_expr` to each `(column, expr)` assignment in an UPDATE,
/// returning a new list when any expression changed and `None` otherwise.
fn rewrite_assignments<F>(
    assignments: &[(usize, ScalarExpr)],
    rewrite_expr: &mut F,
) -> Result<Option<Vec<(usize, ScalarExpr)>>, ServerError>
where
    F: FnMut(&ScalarExpr) -> Result<Option<ScalarExpr>, ServerError>,
{
    let mut changed = false;
    let mut out = Vec::with_capacity(assignments.len());
    for (column, expr) in assignments {
        match rewrite_expr(expr)? {
            Some(new_expr) => {
                changed = true;
                out.push((*column, new_expr));
            }
            None => out.push((*column, expr.clone())),
        }
    }
    Ok(changed.then_some(out))
}

/// Apply `rewrite_expr` to an optional expression, preserving the `None`/`Some`
/// distinction (`Ok(None)` means "no change", which the caller folds back to
/// the original `Option`).
fn rewrite_optional_expr<F>(
    expr: Option<&ScalarExpr>,
    rewrite_expr: &mut F,
) -> Result<Option<ScalarExpr>, ServerError>
where
    F: FnMut(&ScalarExpr) -> Result<Option<ScalarExpr>, ServerError>,
{
    match expr {
        Some(expr) => rewrite_expr(expr),
        None => Ok(None),
    }
}

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Clear the shared plan cache.
    ///
    /// Called from every DDL path after a successful catalog mutation
    /// so the next DML/SELECT statement re-plans against the new schema.
    /// The cache is keyed on SQL text, which has no relationship to the
    /// OIDs the DDL touched, so we invalidate everything; a finer-grained
    /// per-relation invalidation is a v0.7 follow-up.
    pub(crate) fn plan_cache_invalidate(&self) {
        self.state.plan_cache.invalidate_all();
        self.stmt_cache.borrow_mut().clear();
        self.prechecked_fast_dml.borrow_mut().clear();
        self.simple_batch_cache.borrow_mut().clear();
    }

    /// Apply row-level security to `plan`, returning the policy-wrapped plan
    /// (`Some`) or `None` when no rewrite was needed.
    ///
    /// Two independent rewrites compose here:
    ///
    /// 1. [`Self::apply_row_security_node`] descends the `LogicalPlan` tree
    ///    and injects each base-table scan's policy `Filter` (the historical
    ///    behaviour, plus the fail-closed default for unenumerated shapes).
    /// 2. [`Self::apply_row_security_embedded_subplans`] descends the
    ///    *expressions* the node carries (Filter predicate, Project list,
    ///    Join ON, HAVING, Sort keys, …) and recursively applies RLS to every
    ///    [`LogicalPlan`] embedded in a subquery expression
    ///    (`EXISTS` / `IN` / scalar / `= ANY`). This closes the
    ///    uncorrelated-`EXISTS` bypass: such a subquery is NOT decorrelated to
    ///    a join, so its raw subplan would otherwise reach the executor with
    ///    no policy applied and leak RLS-hidden rows.
    ///
    /// Correlated subqueries are decorrelated to joins *before* this walker
    /// runs (RLS executes on the optimised plan), so they no longer carry an
    /// embedded subplan here and are handled purely by step 1 — no double
    /// filtering.
    pub(crate) fn apply_row_security(
        &self,
        plan: &LogicalPlan,
        catalog_snapshot: &CatalogSnapshot,
        command: crate::RuntimeRlsCommand,
    ) -> Result<Option<LogicalPlan>, ServerError> {
        // Step 1: rewrite the plan-node tree (inject scan-site policy filters).
        let node_rewritten = self.apply_row_security_node(plan, catalog_snapshot, command)?;
        // Step 2: rewrite subplans embedded in the (possibly already
        // node-rewritten) plan's own expressions. Operate on whichever plan
        // step 1 produced so both rewrites compose.
        let basis = node_rewritten.as_ref().unwrap_or(plan);
        let expr_rewritten = self.apply_row_security_embedded_subplans(basis, catalog_snapshot)?;
        Ok(expr_rewritten.or(node_rewritten))
    }

    fn apply_row_security_node(
        &self,
        plan: &LogicalPlan,
        catalog_snapshot: &CatalogSnapshot,
        command: crate::RuntimeRlsCommand,
    ) -> Result<Option<LogicalPlan>, ServerError> {
        match plan {
            LogicalPlan::Scan {
                table,
                schema,
                projection,
            } => self.rls_scan_plan(
                table,
                schema,
                projection.as_deref(),
                catalog_snapshot,
                command,
            ),
            LogicalPlan::Filter { input, predicate } => self
                .apply_row_security(input, catalog_snapshot, command)?
                .map(|input| LogicalPlan::Filter {
                    input: Box::new(input),
                    predicate: predicate.clone(),
                })
                .transpose_ok(),
            LogicalPlan::Project {
                input,
                exprs,
                schema,
            } => self
                .apply_row_security(input, catalog_snapshot, command)?
                .map(|input| LogicalPlan::Project {
                    input: Box::new(input),
                    exprs: exprs.clone(),
                    schema: schema.clone(),
                })
                .transpose_ok(),
            LogicalPlan::Limit { input, n, offset } => self
                .apply_row_security(input, catalog_snapshot, command)?
                .map(|input| LogicalPlan::Limit {
                    input: Box::new(input),
                    n: *n,
                    offset: *offset,
                })
                .transpose_ok(),
            LogicalPlan::Sort { input, keys } => self
                .apply_row_security(input, catalog_snapshot, command)?
                .map(|input| LogicalPlan::Sort {
                    input: Box::new(input),
                    keys: keys.clone(),
                })
                .transpose_ok(),
            LogicalPlan::Window {
                input,
                partition_by,
                order_by,
                func,
                frame,
                output_name,
                schema,
            } => self
                .apply_row_security(input, catalog_snapshot, command)?
                .map(|input| LogicalPlan::Window {
                    input: Box::new(input),
                    partition_by: partition_by.clone(),
                    order_by: order_by.clone(),
                    func: func.clone(),
                    frame: frame.clone(),
                    output_name: output_name.clone(),
                    schema: schema.clone(),
                })
                .transpose_ok(),
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                schema,
            } => self
                .apply_row_security(input, catalog_snapshot, command)?
                .map(|input| LogicalPlan::Aggregate {
                    input: Box::new(input),
                    group_by: group_by.clone(),
                    aggregates: aggregates.clone(),
                    schema: schema.clone(),
                })
                .transpose_ok(),
            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
                schema,
            } => {
                let new_left = self.apply_row_security(left, catalog_snapshot, command)?;
                let new_right = self.apply_row_security(right, catalog_snapshot, command)?;
                if new_left.is_none() && new_right.is_none() {
                    return Ok(None);
                }
                Ok(Some(LogicalPlan::Join {
                    left: Box::new(new_left.unwrap_or_else(|| left.as_ref().clone())),
                    right: Box::new(new_right.unwrap_or_else(|| right.as_ref().clone())),
                    join_type: *join_type,
                    condition: condition.clone(),
                    schema: schema.clone(),
                }))
            }
            LogicalPlan::SetOp {
                op,
                quantifier,
                left,
                right,
                schema,
            } => {
                let new_left = self.apply_row_security(left, catalog_snapshot, command)?;
                let new_right = self.apply_row_security(right, catalog_snapshot, command)?;
                if new_left.is_none() && new_right.is_none() {
                    return Ok(None);
                }
                Ok(Some(LogicalPlan::SetOp {
                    op: *op,
                    quantifier: *quantifier,
                    left: Box::new(new_left.unwrap_or_else(|| left.as_ref().clone())),
                    right: Box::new(new_right.unwrap_or_else(|| right.as_ref().clone())),
                    schema: schema.clone(),
                }))
            }
            LogicalPlan::Cte {
                name,
                recursive,
                definition,
                body,
                schema,
            } => {
                let new_definition =
                    self.apply_row_security(definition, catalog_snapshot, command)?;
                let new_body = self.apply_row_security(body, catalog_snapshot, command)?;
                if new_definition.is_none() && new_body.is_none() {
                    return Ok(None);
                }
                Ok(Some(LogicalPlan::Cte {
                    name: name.clone(),
                    recursive: *recursive,
                    definition: Box::new(
                        new_definition.unwrap_or_else(|| definition.as_ref().clone()),
                    ),
                    body: Box::new(new_body.unwrap_or_else(|| body.as_ref().clone())),
                    schema: schema.clone(),
                }))
            }
            LogicalPlan::LockRows {
                input,
                strength,
                wait_policy,
                schema,
            } => self
                .apply_row_security(input, catalog_snapshot, command)?
                .map(|input| LogicalPlan::LockRows {
                    input: Box::new(input),
                    strength: *strength,
                    wait_policy: *wait_policy,
                    schema: schema.clone(),
                })
                .transpose_ok(),
            LogicalPlan::Insert {
                table,
                columns,
                source,
                on_conflict,
                returning,
                schema,
            } => self
                .apply_row_security(source, catalog_snapshot, crate::RuntimeRlsCommand::Select)?
                .map(|source| LogicalPlan::Insert {
                    table: table.clone(),
                    columns: columns.clone(),
                    source: Box::new(source),
                    on_conflict: on_conflict.clone(),
                    returning: returning.clone(),
                    schema: schema.clone(),
                })
                .transpose_ok(),
            LogicalPlan::Update {
                table,
                assignments,
                input,
                returning,
                schema,
            } => self
                .apply_row_security(input, catalog_snapshot, crate::RuntimeRlsCommand::Update)?
                .map(|input| LogicalPlan::Update {
                    table: table.clone(),
                    assignments: assignments.clone(),
                    input: Box::new(input),
                    returning: returning.clone(),
                    schema: schema.clone(),
                })
                .transpose_ok(),
            LogicalPlan::Delete {
                table,
                input,
                returning,
                schema,
            } => self
                .apply_row_security(input, catalog_snapshot, crate::RuntimeRlsCommand::Delete)?
                .map(|input| LogicalPlan::Delete {
                    table: table.clone(),
                    input: Box::new(input),
                    returning: returning.clone(),
                    schema: schema.clone(),
                })
                .transpose_ok(),
            LogicalPlan::Explain {
                analyze,
                format,
                input,
                schema,
            } => self
                .apply_row_security(input, catalog_snapshot, command)?
                .map(|input| LogicalPlan::Explain {
                    analyze: *analyze,
                    format: *format,
                    input: Box::new(input),
                    schema: schema.clone(),
                })
                .transpose_ok(),
            LogicalPlan::Copy {
                relation,
                input,
                columns,
                direction,
                source,
                format,
                delimiter,
                null_str,
                header,
                auto_detect,
                ignore_errors,
                max_errors,
                reject_table,
                schema,
            } => {
                let Some(input) = input else {
                    return Ok(None);
                };
                self.apply_row_security(input, catalog_snapshot, command)?
                    .map(|input| LogicalPlan::Copy {
                        relation: relation.clone(),
                        input: Some(Box::new(input)),
                        columns: columns.clone(),
                        direction: *direction,
                        source: source.clone(),
                        format: *format,
                        delimiter: *delimiter,
                        null_str: null_str.clone(),
                        header: *header,
                        auto_detect: *auto_detect,
                        ignore_errors: *ignore_errors,
                        max_errors: *max_errors,
                        reject_table: reject_table.clone(),
                        schema: schema.clone(),
                    })
                    .transpose_ok()
            }
            // Unary cardinality guard the scalar-subquery decorrelation rule
            // inserts around the subquery's right side before the CROSS join.
            // It is non-projecting, so descend into `input` to reach the inner
            // scan and inject its RLS predicate; otherwise the subquery's rows
            // bypass row-level security entirely.
            LogicalPlan::SingleRowAssert { input } => self
                .apply_row_security(input, catalog_snapshot, command)?
                .map(|input| LogicalPlan::SingleRowAssert {
                    input: Box::new(input),
                })
                .transpose_ok(),
            // `SELECT DISTINCT ON (...)` dedup. Non-projecting unary node that
            // wraps the (sorted) input subtree, which can be a `Scan` over an
            // RLS table. Descend so the policy predicate reaches the scan;
            // otherwise DISTINCT ON over a protected table would leak rows.
            LogicalPlan::DistinctOn { input, on_keys } => self
                .apply_row_security(input, catalog_snapshot, command)?
                .map(|input| LogicalPlan::DistinctOn {
                    input: Box::new(input),
                    on_keys: on_keys.clone(),
                })
                .transpose_ok(),
            // `PIVOT` table factor. Unary node over an input that can be a
            // `Scan` of an RLS table; descend to inject the predicate.
            LogicalPlan::Pivot {
                input,
                group_columns,
                pivot_column,
                aggregate,
                pivot_values,
                schema,
            } => self
                .apply_row_security(input, catalog_snapshot, command)?
                .map(|input| LogicalPlan::Pivot {
                    input: Box::new(input),
                    group_columns: group_columns.clone(),
                    pivot_column: *pivot_column,
                    aggregate: aggregate.clone(),
                    pivot_values: pivot_values.clone(),
                    schema: schema.clone(),
                })
                .transpose_ok(),
            // `UNPIVOT` table factor. Unary node over an RLS-bearing input;
            // descend to inject the predicate.
            LogicalPlan::Unpivot {
                input,
                passthrough_columns,
                columns,
                name_column,
                value_column,
                include_nulls,
                schema,
            } => self
                .apply_row_security(input, catalog_snapshot, command)?
                .map(|input| LogicalPlan::Unpivot {
                    input: Box::new(input),
                    passthrough_columns: passthrough_columns.clone(),
                    columns: columns.clone(),
                    name_column: name_column.clone(),
                    value_column: value_column.clone(),
                    include_nulls: *include_nulls,
                    schema: schema.clone(),
                })
                .transpose_ok(),
            // `MERGE INTO target USING source`. The `source` child is a read
            // path that can be (or contain) a `Scan` over an RLS table, so it
            // must have the SELECT-side policy applied. The target table's own
            // RLS WITH CHECK enforcement is handled elsewhere (the executor's
            // merge path); here we only rewrite the readable source subtree.
            LogicalPlan::Merge {
                target,
                target_alias,
                target_schema,
                source,
                on,
                clauses,
                schema,
            } => self
                .apply_row_security(source, catalog_snapshot, crate::RuntimeRlsCommand::Select)?
                .map(|source| LogicalPlan::Merge {
                    target: target.clone(),
                    target_alias: target_alias.clone(),
                    target_schema: target_schema.clone(),
                    source: Box::new(source),
                    on: on.clone(),
                    clauses: clauses.clone(),
                    schema: schema.clone(),
                })
                .transpose_ok(),
            // Leaves with no base-table reference: a constant row set
            // (`VALUES`), the empty/no-FROM source, and a set-returning
            // function scan (its args are scalars, never a catalog relation).
            // RLS does not apply — return `None` explicitly rather than via
            // the (now fail-closed) catch-all.
            LogicalPlan::Values { .. }
            | LogicalPlan::Empty { .. }
            | LogicalPlan::FunctionScan { .. } => Ok(None),
            // `SUMMARIZE table` reads a base table directly. It has no child
            // plan to inject a `Filter` into, and the SUMMARIZE pipeline
            // refuses to run over an RLS-enabled table at its own scan site
            // (`pipeline::scan` returns SQLSTATE 0A000 before reading any
            // row). So there is nothing for this walker to rewrite; returning
            // `None` here keeps the original plan, which the SUMMARIZE
            // executor then rejects fail-closed. Enumerated explicitly (not
            // via the catch-all) so this reasoning is auditable.
            LogicalPlan::Summarize { .. } => Ok(None),
            // Fail closed. Every data-plane plan shape that can reach this
            // walker is enumerated above; control/DDL plans are dispatched
            // before `apply_row_security` is ever called. An unenumerated
            // shape here means a NEW node kind whose RLS handling has not
            // been audited — it might carry a base-table reference below it,
            // and silently returning `Ok(None)` would use the un-rewritten
            // plan (RLS skipped). Refuse the query instead of risking a
            // cross-tenant read. SQLSTATE 0A000 (feature_not_supported).
            other => Err(ServerError::UnsupportedOwned(format!(
                "row-level security cannot be verified for this plan shape ({:?}); refusing to \
                 execute to avoid a possible policy bypass",
                std::mem::discriminant(other),
            ))),
        }
    }

    /// Recursively apply RLS to every [`LogicalPlan`] embedded in `plan`'s own
    /// expressions (subqueries in a Filter predicate, Project list, Join ON,
    /// HAVING, Sort keys, etc.), substituting the policy-wrapped subplan back.
    ///
    /// Returns `Some(rewritten_plan)` when at least one embedded subplan was
    /// rewritten, `None` when none were. A subquery is always a *read*, so the
    /// recursion uses [`crate::RuntimeRlsCommand::Select`] regardless of the
    /// enclosing statement's command.
    ///
    /// This is the second half of [`Self::apply_row_security`]; it never
    /// descends into child *plan* nodes (step 1 does that) — only into the
    /// expressions attached to this single node.
    fn apply_row_security_embedded_subplans(
        &self,
        plan: &LogicalPlan,
        catalog_snapshot: &CatalogSnapshot,
    ) -> Result<Option<LogicalPlan>, ServerError> {
        // Rewrite a single expression's embedded subplans, recursing through
        // the full `apply_row_security` so a subplan that itself embeds
        // further subqueries (or scans an RLS table) is fully protected.
        let mut rewrite_expr = |expr: &ScalarExpr| -> Result<Option<ScalarExpr>, ServerError> {
            expr.try_rewrite_subplans(&mut |subplan: &LogicalPlan| {
                self.apply_row_security(subplan, catalog_snapshot, crate::RuntimeRlsCommand::Select)
            })
        };

        match plan {
            LogicalPlan::Filter { input, predicate } => Ok(rewrite_expr(predicate)?.map(
                |predicate| LogicalPlan::Filter {
                    input: input.clone(),
                    predicate,
                },
            )),
            LogicalPlan::Project {
                input,
                exprs,
                schema,
            } => Ok(
                rewrite_projection_exprs(exprs, &mut rewrite_expr)?.map(|exprs| {
                    LogicalPlan::Project {
                        input: input.clone(),
                        exprs,
                        schema: schema.clone(),
                    }
                }),
            ),
            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
                schema,
            } => {
                let LogicalJoinCondition::On(on_expr) = condition else {
                    return Ok(None);
                };
                Ok(rewrite_expr(on_expr)?.map(|on_expr| LogicalPlan::Join {
                    left: left.clone(),
                    right: right.clone(),
                    join_type: *join_type,
                    condition: LogicalJoinCondition::On(on_expr),
                    schema: schema.clone(),
                }))
            }
            LogicalPlan::Sort { input, keys } => Ok(rewrite_sort_keys(keys, &mut rewrite_expr)?
                .map(|keys| LogicalPlan::Sort {
                    input: input.clone(),
                    keys,
                })),
            LogicalPlan::Window {
                input,
                partition_by,
                order_by,
                func,
                frame,
                output_name,
                schema,
            } => {
                let new_partition = rewrite_expr_list(partition_by, &mut rewrite_expr)?;
                let new_order = rewrite_sort_keys(order_by, &mut rewrite_expr)?;
                if new_partition.is_none() && new_order.is_none() {
                    return Ok(None);
                }
                Ok(Some(LogicalPlan::Window {
                    input: input.clone(),
                    partition_by: new_partition.unwrap_or_else(|| partition_by.clone()),
                    order_by: new_order.unwrap_or_else(|| order_by.clone()),
                    func: func.clone(),
                    frame: frame.clone(),
                    output_name: output_name.clone(),
                    schema: schema.clone(),
                }))
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                schema,
            } => {
                let new_group = rewrite_expr_list(group_by, &mut rewrite_expr)?;
                let new_aggs = rewrite_aggregate_exprs(aggregates, &mut rewrite_expr)?;
                if new_group.is_none() && new_aggs.is_none() {
                    return Ok(None);
                }
                Ok(Some(LogicalPlan::Aggregate {
                    input: input.clone(),
                    group_by: new_group.unwrap_or_else(|| group_by.clone()),
                    aggregates: new_aggs.unwrap_or_else(|| aggregates.clone()),
                    schema: schema.clone(),
                }))
            }
            LogicalPlan::DistinctOn { input, on_keys } => Ok(rewrite_expr_list(
                on_keys,
                &mut rewrite_expr,
            )?
            .map(|on_keys| LogicalPlan::DistinctOn {
                input: input.clone(),
                on_keys,
            })),
            LogicalPlan::Update {
                table,
                assignments,
                input,
                returning,
                schema,
            } => {
                let new_assignments = rewrite_assignments(assignments, &mut rewrite_expr)?;
                let new_returning = rewrite_projection_exprs(returning, &mut rewrite_expr)?;
                if new_assignments.is_none() && new_returning.is_none() {
                    return Ok(None);
                }
                Ok(Some(LogicalPlan::Update {
                    table: table.clone(),
                    assignments: new_assignments.unwrap_or_else(|| assignments.clone()),
                    input: input.clone(),
                    returning: new_returning.unwrap_or_else(|| returning.clone()),
                    schema: schema.clone(),
                }))
            }
            // INSERT/DELETE: the data-read subtree (`source` / `input`) is
            // rewritten by step 1's recursion; here we only need their
            // `RETURNING` expressions, which can embed a subquery scanning an
            // RLS table.
            LogicalPlan::Insert {
                table,
                columns,
                source,
                on_conflict,
                returning,
                schema,
            } => Ok(
                rewrite_projection_exprs(returning, &mut rewrite_expr)?.map(|returning| {
                    LogicalPlan::Insert {
                        table: table.clone(),
                        columns: columns.clone(),
                        source: source.clone(),
                        on_conflict: on_conflict.clone(),
                        returning,
                        schema: schema.clone(),
                    }
                }),
            ),
            LogicalPlan::Delete {
                table,
                input,
                returning,
                schema,
            } => Ok(
                rewrite_projection_exprs(returning, &mut rewrite_expr)?.map(|returning| {
                    LogicalPlan::Delete {
                        table: table.clone(),
                        input: input.clone(),
                        returning,
                        schema: schema.clone(),
                    }
                }),
            ),
            // Remaining shapes carry no expression position that can embed a
            // subquery plan reachable from here: leaves (`Scan`, `Values`,
            // `Empty`, `FunctionScan`, `Summarize`), pure pass-through unary
            // nodes whose only expressions are column references
            // (`Limit`, `LockRows`, `SingleRowAssert`, `Pivot`, `Unpivot`),
            // wrappers handled via their child plan (`SetOp`, `Cte`,
            // `Explain`, `Copy`, `Merge` source), and control/DDL plans
            // dispatched before the walker. The node-tree walker (step 1)
            // already enumerates and fail-closes on unknown shapes, so
            // returning `None` here is safe: any subquery plan they *did*
            // carry would have to live under a child plan node, which step
            // 1's recursion (back through the full `apply_row_security`)
            // covers.
            _ => Ok(None),
        }
    }

    fn rls_scan_plan(
        &self,
        table: &str,
        schema: &ultrasql_core::Schema,
        projection: Option<&[usize]>,
        catalog_snapshot: &CatalogSnapshot,
        command: crate::RuntimeRlsCommand,
    ) -> Result<Option<LogicalPlan>, ServerError> {
        let Some(entry) = catalog_snapshot.tables.get(table) else {
            return Ok(None);
        };
        let Some(runtime) = self.enabled_row_security(entry.oid) else {
            return Ok(None);
        };
        let predicate = self.rls_using_predicate(&runtime, command)?;
        let full_scan = LogicalPlan::Scan {
            table: table.to_owned(),
            schema: entry.schema.clone(),
            projection: None,
        };
        let filtered = LogicalPlan::Filter {
            input: Box::new(full_scan),
            predicate,
        };
        let Some(projection) = projection else {
            return Ok(Some(filtered));
        };
        let exprs = projection
            .iter()
            .map(|idx| {
                let field = entry.schema.fields().get(*idx).ok_or_else(|| {
                    ServerError::ddl(format!("RLS projection index {idx} out of bounds"))
                })?;
                Ok((
                    ScalarExpr::Column {
                        name: field.name.clone(),
                        index: *idx,
                        data_type: field.data_type.clone(),
                    },
                    field.name.clone(),
                ))
            })
            .collect::<Result<Vec<_>, ServerError>>()?;
        Ok(Some(LogicalPlan::Project {
            input: Box::new(filtered),
            exprs,
            schema: schema.clone(),
        }))
    }

    pub(crate) fn enabled_row_security(
        &self,
        table_oid: ultrasql_core::Oid,
    ) -> Option<Arc<crate::TableRowSecurity>> {
        let guard = self.state.row_security.get(&table_oid)?;
        let runtime = Arc::clone(guard.value());
        if runtime.enabled && !self.bypasses_row_security(&runtime) {
            Some(runtime)
        } else {
            None
        }
    }

    fn bypasses_row_security(&self, runtime: &crate::TableRowSecurity) -> bool {
        let current_user = self.current_user.to_ascii_lowercase();
        let Some(role) = self.state.role_catalog.lookup_role(&current_user) else {
            return false;
        };
        role.is_superuser
            || role.bypass_rls
            || (!runtime.owner_role.is_empty()
                && runtime.owner_role.eq_ignore_ascii_case(&current_user))
    }

    fn rls_using_predicate(
        &self,
        runtime: &crate::TableRowSecurity,
        command: crate::RuntimeRlsCommand,
    ) -> Result<ScalarExpr, ServerError> {
        let inherited_roles = self
            .state
            .role_catalog
            .inherited_role_names(&self.current_user);
        let mut permissive = Vec::new();
        let mut restrictive = Vec::new();
        for policy in runtime.policies.iter().filter(|policy| {
            policy.command.applies_to(command) && policy.applies_to_roles(&inherited_roles)
        }) {
            let Some(expr) = policy.using.as_ref() else {
                continue;
            };
            match policy.permissiveness {
                crate::RuntimeRlsPermissiveness::Permissive => {
                    permissive.push(self.rls_tenant_predicate(expr)?);
                }
                crate::RuntimeRlsPermissiveness::Restrictive => {
                    restrictive.push(self.rls_tenant_predicate(expr)?);
                }
            }
        }
        let Some(mut predicate) = combine_rls_predicates(permissive, BinaryOp::Or) else {
            return Ok(bool_literal(false));
        };
        if let Some(restrictive) = combine_rls_predicates(restrictive, BinaryOp::And) {
            predicate = ScalarExpr::Binary {
                op: BinaryOp::And,
                left: Box::new(predicate),
                right: Box::new(restrictive),
                data_type: DataType::Bool,
            };
        }
        Ok(predicate)
    }

    fn rls_tenant_predicate(
        &self,
        expr: &crate::RuntimeTenantPolicyExpr,
    ) -> Result<ScalarExpr, ServerError> {
        let Some(value) = self
            .session_settings
            .get(&expr.setting_name.to_ascii_lowercase())
        else {
            return Ok(bool_literal(false));
        };
        Ok(ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(ScalarExpr::Column {
                name: expr.column_name.clone(),
                index: expr.column_index,
                data_type: DataType::Text { max_len: None },
            }),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Text(value.clone()),
                data_type: DataType::Text { max_len: None },
            }),
            data_type: DataType::Bool,
        })
    }

    pub(crate) fn check_rls_insert_values(
        &self,
        plan: &LogicalPlan,
        catalog_snapshot: &CatalogSnapshot,
    ) -> Result<(), ServerError> {
        let LogicalPlan::Insert {
            table,
            columns,
            source,
            ..
        } = plan
        else {
            return Ok(());
        };
        let Some(entry) = catalog_snapshot.tables.get(table) else {
            return Ok(());
        };
        let Some(runtime) = self.enabled_row_security(entry.oid) else {
            return Ok(());
        };
        let inherited_roles = self
            .state
            .role_catalog
            .inherited_role_names(&self.current_user);
        let mut permissive_checks = Vec::new();
        let mut restrictive_checks = Vec::new();
        for policy in runtime.policies.iter().filter(|policy| {
            policy.command.applies_to(crate::RuntimeRlsCommand::Insert)
                && policy.applies_to_roles(&inherited_roles)
        }) {
            let Some(check) = policy.with_check.as_ref().or(policy.using.as_ref()) else {
                continue;
            };
            match policy.permissiveness {
                crate::RuntimeRlsPermissiveness::Permissive => permissive_checks.push(check),
                crate::RuntimeRlsPermissiveness::Restrictive => restrictive_checks.push(check),
            }
        }
        let LogicalPlan::Values { rows, .. } = source.as_ref() else {
            return Ok(());
        };
        for row in rows {
            let mut accepted = false;
            for check in &permissive_checks {
                if self.rls_insert_row_matches(check, columns, row)? {
                    accepted = true;
                    break;
                }
            }
            if accepted {
                for check in &restrictive_checks {
                    if !self.rls_insert_row_matches(check, columns, row)? {
                        accepted = false;
                        break;
                    }
                }
            }
            if !accepted {
                return Err(ultrasql_executor::ExecError::CheckViolation(
                    "row-level security policy".to_owned(),
                )
                .into());
            }
        }
        Ok(())
    }

    fn rls_insert_row_matches(
        &self,
        check: &crate::RuntimeTenantPolicyExpr,
        columns: &[usize],
        row: &[ScalarExpr],
    ) -> Result<bool, ServerError> {
        let Some(expected) = self
            .session_settings
            .get(&check.setting_name.to_ascii_lowercase())
        else {
            return Ok(false);
        };
        let row_idx = if columns.is_empty() {
            check.column_index
        } else {
            let Some(idx) = columns.iter().position(|col| *col == check.column_index) else {
                return Ok(false);
            };
            idx
        };
        let Some(expr) = row.get(row_idx) else {
            return Ok(false);
        };
        match expr {
            ScalarExpr::Literal {
                value: Value::Text(actual),
                ..
            } => Ok(actual == expected),
            ScalarExpr::Literal {
                value: Value::Null, ..
            } => Ok(false),
            _ => Err(ServerError::Unsupported(
                "RLS WITH CHECK currently requires literal tenant values",
            )),
        }
    }
}

#[cfg(test)]
mod fail_closed_tests {
    use std::sync::Arc;

    use tokio::io::{DuplexStream, duplex};
    use ultrasql_core::Schema;
    use ultrasql_planner::LogicalPlan;

    use crate::session::Session;
    use crate::{RuntimeRlsCommand, Server};

    fn test_session() -> Session<DuplexStream> {
        let (io, _peer) = duplex(64);
        Session::new(io, Arc::new(Server::with_sample_database()), None)
    }

    /// The `apply_row_security` walker must FAIL CLOSED on any plan shape it
    /// does not explicitly enumerate: a silent `Ok(None)` would use the
    /// un-rewritten plan and could skip RLS on a base-table scan buried in the
    /// unknown shape. We use `Commit` only as a stand-in for "a node kind the
    /// walker does not enumerate" (control plans never reach the walker in
    /// production) — the point is that the catch-all errors rather than
    /// returning `Ok(None)`.
    #[test]
    fn apply_row_security_fails_closed_on_unenumerated_shape() {
        let session = test_session();
        let snapshot = session.state.catalog_snapshot();
        let plan = LogicalPlan::Commit {
            schema: Schema::empty(),
        };
        let err = session
            .apply_row_security(&plan, &snapshot, RuntimeRlsCommand::Select)
            .expect_err("unenumerated plan shape must fail closed, not return Ok(None)");
        let message = err.to_string();
        assert!(
            message.contains("row-level security cannot be verified"),
            "fail-closed error should name the RLS verification limitation: {message}"
        );
        assert_eq!(err.sqlstate(), "0A000", "feature_not_supported expected");
    }

    /// Enumerated leaves that provably hold no base-table reference
    /// (`VALUES`, the empty no-FROM source) must return `Ok(None)` — no RLS to
    /// apply, and they must NOT trip the new fail-closed default.
    #[test]
    fn apply_row_security_passes_through_table_free_leaves() {
        let session = test_session();
        let snapshot = session.state.catalog_snapshot();

        let empty = LogicalPlan::Empty {
            schema: Schema::empty(),
        };
        assert!(
            session
                .apply_row_security(&empty, &snapshot, RuntimeRlsCommand::Select)
                .expect("Empty is a table-free leaf")
                .is_none(),
            "Empty must need no RLS rewrite"
        );

        let values = LogicalPlan::Values {
            rows: Vec::new(),
            schema: Schema::empty(),
        };
        assert!(
            session
                .apply_row_security(&values, &snapshot, RuntimeRlsCommand::Select)
                .expect("Values is a table-free leaf")
                .is_none(),
            "Values must need no RLS rewrite"
        );
    }
}
