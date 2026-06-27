//! Plan-cache invalidation and row-level security predicate application.

use super::*;

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

    pub(crate) fn apply_row_security(
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
