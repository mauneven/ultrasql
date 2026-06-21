//! Trivial-insert detection, DML plan optimization, and regular-view expansion.

use super::*;

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// `true` iff `plan` is `Insert { source: Values { .. }, .. }`
    /// with no `ON CONFLICT` / `RETURNING` — see the call site for
    /// why this bypasses the optimizer + plan-cache lookup.
    pub(crate) fn is_trivial_insert_values(plan: &LogicalPlan) -> bool {
        let LogicalPlan::Insert {
            source,
            on_conflict,
            returning,
            ..
        } = plan
        else {
            return false;
        };
        if on_conflict.is_some() || !returning.is_empty() {
            return false;
        }
        matches!(source.as_ref(), LogicalPlan::Values { .. })
    }

    /// Apply the cost-based optimizer to a DML/SELECT plan and return
    /// the result.
    ///
    /// The optimised plan is cached in [`Server::plan_cache`] keyed on
    /// the raw `sql` text. A cache hit skips the rule-rewrite loop and
    /// returns the previously-optimised plan; a cache miss runs
    /// [`ultrasql_optimizer::optimize`] against the bound plan and
    /// stores the result. The cache is cleared whole-cloth by every DDL
    /// path (see [`Self::plan_cache_invalidate`]), so concurrent DDL
    /// cannot serve a stale plan.
    ///
    /// # Errors
    ///
    /// Wraps [`OptimizeError`] into [`ServerError::Plan`] via a synthetic
    /// `PlanError::Type` message because the optimizer's failure modes
    /// are all bind-time-quality (the binder already type-checked the
    /// plan, so a rule failure is an internal-invariant violation). The
    /// caller forwards the wrapped error through the normal
    /// `fail_if_in_transaction` machinery.
    pub(crate) fn optimize_dml_plan(
        &self,
        sql: &str,
        plan: LogicalPlan,
        catalog_snapshot: &Arc<CatalogSnapshot>,
    ) -> Result<LogicalPlan, ServerError> {
        let key = PlanCacheKey::named(sql.to_owned());
        let stats = ServerStatsSource {
            stats_catalog: &self.state.stats_catalog,
        };
        let snapshot = Arc::clone(catalog_snapshot);
        // The closure is invoked only on cache miss; on a hit the cached
        // plan is returned and the plan we received here is dropped.
        // The closure consumes the plan via move because `FnOnce` does
        // not require `Clone` even though the underlying signature of
        // `PlanCache::get_or_plan` declares `FnOnce(&[Value])`.
        self.state
            .plan_cache
            .get_or_plan(&key, &[], move |_params| {
                ultrasql_optimizer::optimize(plan, &snapshot, &stats as &dyn StatsSource)
            })
            .map_err(|e| {
                ServerError::Plan(ultrasql_planner::PlanError::TypeMismatch(format!(
                    "optimizer failed: {e}"
                )))
            })
    }

    pub(crate) fn prepare_regular_view_plan(
        &self,
        plan: &LogicalPlan,
        catalog_snapshot: &Arc<CatalogSnapshot>,
    ) -> Result<LogicalPlan, ServerError> {
        if let Some(table) = Self::dml_target_table(plan) {
            let key = table.to_ascii_lowercase();
            if self.state.regular_views.contains_key(&key)
                || catalog_snapshot
                    .tables
                    .get(&key)
                    .is_some_and(crate::is_regular_view_entry)
            {
                return Err(ServerError::UnsupportedOwned(format!(
                    "cannot modify view {table}"
                )));
            }
        }
        self.expand_regular_views_in_plan(plan, catalog_snapshot, &mut Vec::new())
    }

    fn expand_regular_views_in_plan(
        &self,
        plan: &LogicalPlan,
        catalog_snapshot: &CatalogSnapshot,
        stack: &mut Vec<String>,
    ) -> Result<LogicalPlan, ServerError> {
        match plan {
            LogicalPlan::Scan {
                table,
                schema,
                projection,
            } => self.expand_regular_view_scan(table, schema, projection, catalog_snapshot, stack),
            LogicalPlan::Filter { input, predicate } => Ok(LogicalPlan::Filter {
                input: Box::new(self.expand_regular_views_in_plan(
                    input,
                    catalog_snapshot,
                    stack,
                )?),
                predicate: predicate.clone(),
            }),
            LogicalPlan::Project {
                input,
                exprs,
                schema,
            } => Ok(LogicalPlan::Project {
                input: Box::new(self.expand_regular_views_in_plan(
                    input,
                    catalog_snapshot,
                    stack,
                )?),
                exprs: exprs.clone(),
                schema: schema.clone(),
            }),
            LogicalPlan::Limit { input, n, offset } => Ok(LogicalPlan::Limit {
                input: Box::new(self.expand_regular_views_in_plan(
                    input,
                    catalog_snapshot,
                    stack,
                )?),
                n: *n,
                offset: *offset,
            }),
            LogicalPlan::Sort { input, keys } => Ok(LogicalPlan::Sort {
                input: Box::new(self.expand_regular_views_in_plan(
                    input,
                    catalog_snapshot,
                    stack,
                )?),
                keys: keys.clone(),
            }),
            LogicalPlan::Window {
                input,
                partition_by,
                order_by,
                func,
                frame,
                output_name,
                schema,
            } => Ok(LogicalPlan::Window {
                input: Box::new(self.expand_regular_views_in_plan(
                    input,
                    catalog_snapshot,
                    stack,
                )?),
                partition_by: partition_by.clone(),
                order_by: order_by.clone(),
                func: func.clone(),
                frame: frame.clone(),
                output_name: output_name.clone(),
                schema: schema.clone(),
            }),
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                schema,
            } => Ok(LogicalPlan::Aggregate {
                input: Box::new(self.expand_regular_views_in_plan(
                    input,
                    catalog_snapshot,
                    stack,
                )?),
                group_by: group_by.clone(),
                aggregates: aggregates.clone(),
                schema: schema.clone(),
            }),
            LogicalPlan::Pivot {
                input,
                group_columns,
                pivot_column,
                aggregate,
                pivot_values,
                schema,
            } => Ok(LogicalPlan::Pivot {
                input: Box::new(self.expand_regular_views_in_plan(
                    input,
                    catalog_snapshot,
                    stack,
                )?),
                group_columns: group_columns.clone(),
                pivot_column: *pivot_column,
                aggregate: aggregate.clone(),
                pivot_values: pivot_values.clone(),
                schema: schema.clone(),
            }),
            LogicalPlan::Unpivot {
                input,
                passthrough_columns,
                columns,
                name_column,
                value_column,
                include_nulls,
                schema,
            } => Ok(LogicalPlan::Unpivot {
                input: Box::new(self.expand_regular_views_in_plan(
                    input,
                    catalog_snapshot,
                    stack,
                )?),
                passthrough_columns: passthrough_columns.clone(),
                columns: columns.clone(),
                name_column: name_column.clone(),
                value_column: value_column.clone(),
                include_nulls: *include_nulls,
                schema: schema.clone(),
            }),
            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
                schema,
            } => Ok(LogicalPlan::Join {
                left: Box::new(self.expand_regular_views_in_plan(left, catalog_snapshot, stack)?),
                right: Box::new(self.expand_regular_views_in_plan(
                    right,
                    catalog_snapshot,
                    stack,
                )?),
                join_type: *join_type,
                condition: condition.clone(),
                schema: schema.clone(),
            }),
            LogicalPlan::SetOp {
                op,
                quantifier,
                left,
                right,
                schema,
            } => Ok(LogicalPlan::SetOp {
                op: *op,
                quantifier: *quantifier,
                left: Box::new(self.expand_regular_views_in_plan(left, catalog_snapshot, stack)?),
                right: Box::new(self.expand_regular_views_in_plan(
                    right,
                    catalog_snapshot,
                    stack,
                )?),
                schema: schema.clone(),
            }),
            LogicalPlan::Cte {
                name,
                recursive,
                definition,
                body,
                schema,
            } => Ok(LogicalPlan::Cte {
                name: name.clone(),
                recursive: *recursive,
                definition: Box::new(self.expand_regular_views_in_plan(
                    definition,
                    catalog_snapshot,
                    stack,
                )?),
                body: Box::new(self.expand_regular_views_in_plan(body, catalog_snapshot, stack)?),
                schema: schema.clone(),
            }),
            LogicalPlan::LockRows {
                input,
                strength,
                wait_policy,
                schema,
            } => Ok(LogicalPlan::LockRows {
                input: Box::new(self.expand_regular_views_in_plan(
                    input,
                    catalog_snapshot,
                    stack,
                )?),
                strength: *strength,
                wait_policy: *wait_policy,
                schema: schema.clone(),
            }),
            LogicalPlan::Insert {
                table,
                columns,
                source,
                on_conflict,
                returning,
                schema,
            } => Ok(LogicalPlan::Insert {
                table: table.clone(),
                columns: columns.clone(),
                source: Box::new(self.expand_regular_views_in_plan(
                    source,
                    catalog_snapshot,
                    stack,
                )?),
                on_conflict: on_conflict.clone(),
                returning: returning.clone(),
                schema: schema.clone(),
            }),
            LogicalPlan::Update {
                table,
                assignments,
                input,
                returning,
                schema,
            } => Ok(LogicalPlan::Update {
                table: table.clone(),
                assignments: assignments.clone(),
                input: Box::new(self.expand_regular_views_in_plan(
                    input,
                    catalog_snapshot,
                    stack,
                )?),
                returning: returning.clone(),
                schema: schema.clone(),
            }),
            LogicalPlan::Delete {
                table,
                input,
                returning,
                schema,
            } => Ok(LogicalPlan::Delete {
                table: table.clone(),
                input: Box::new(self.expand_regular_views_in_plan(
                    input,
                    catalog_snapshot,
                    stack,
                )?),
                returning: returning.clone(),
                schema: schema.clone(),
            }),
            LogicalPlan::Merge {
                target,
                target_alias,
                target_schema,
                source,
                on,
                clauses,
                schema,
            } => Ok(LogicalPlan::Merge {
                target: target.clone(),
                target_alias: target_alias.clone(),
                target_schema: target_schema.clone(),
                source: Box::new(self.expand_regular_views_in_plan(
                    source,
                    catalog_snapshot,
                    stack,
                )?),
                on: on.clone(),
                clauses: clauses.clone(),
                schema: schema.clone(),
            }),
            LogicalPlan::Explain {
                input,
                analyze,
                format,
                schema,
            } => Ok(LogicalPlan::Explain {
                analyze: *analyze,
                format: *format,
                input: Box::new(self.expand_regular_views_in_plan(
                    input,
                    catalog_snapshot,
                    stack,
                )?),
                schema: schema.clone(),
            }),
            other => Ok(other.clone()),
        }
    }

    fn expand_regular_view_scan(
        &self,
        table: &str,
        schema: &Schema,
        projection: &Option<Vec<usize>>,
        catalog_snapshot: &CatalogSnapshot,
        stack: &mut Vec<String>,
    ) -> Result<LogicalPlan, ServerError> {
        let key = table.to_ascii_lowercase();
        let Some(runtime) = self
            .state
            .regular_views
            .get(&key)
            .map(|guard| Arc::clone(guard.value()))
        else {
            if catalog_snapshot
                .tables
                .get(&key)
                .is_some_and(crate::is_regular_view_entry)
            {
                return Err(ServerError::ddl(format!(
                    "missing runtime metadata for view {table}"
                )));
            }
            return Ok(LogicalPlan::Scan {
                table: table.to_owned(),
                schema: schema.clone(),
                projection: projection.clone(),
            });
        };
        if stack.iter().any(|seen| seen == &key) {
            return Err(ServerError::ddl(format!(
                "recursive view expansion for {table}"
            )));
        }
        if projection.is_some() {
            return Err(ServerError::Unsupported(
                "projected regular-view scans are not supported before view expansion",
            ));
        }
        stack.push(key);
        let source = self.expand_regular_views_in_plan(&runtime.source, catalog_snapshot, stack)?;
        stack.pop();
        if !crate::view_source_shape_matches(source.schema(), &runtime.columns) {
            return Err(ServerError::ddl(format!(
                "view {} source schema no longer matches stored view schema",
                runtime.view_table
            )));
        }
        let exprs = runtime
            .columns
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                (
                    ScalarExpr::Column {
                        name: field.name.clone(),
                        index,
                        data_type: field.data_type.clone(),
                    },
                    field.name.clone(),
                )
            })
            .collect();
        Ok(LogicalPlan::Project {
            input: Box::new(source),
            exprs,
            schema: runtime.columns.clone(),
        })
    }
}
