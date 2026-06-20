//! View-family DDL: `CREATE [MATERIALIZED] VIEW`, `ALTER VIEW`, and
//! `CREATE POLICY` (tenant-equality row-level-security predicates).

use ultrasql_core::Schema;
use ultrasql_parser::ast::{
    AlterViewAction, AlterViewStmt, BinaryOp, CreateMaterializedViewStmt, CreatePolicyStmt,
    CreateViewStmt, Expr, Identifier, Literal, PolicyCommand as AstPolicyCommand,
    PolicyPermissiveness as AstPolicyPermissiveness,
};

use super::super::{
    Catalog, LogicalAlterViewAction, LogicalPlan, PlanError, ScopeStack, bind_select,
    lookup_table_reference, object_name_simple,
};
use super::shared::{object_name_namespace, unparen_expr};
use crate::plan::{
    LogicalRlsCommand, LogicalRlsPermissiveness, LogicalRlsPolicy, LogicalTenantPolicyExpr,
};

pub(in crate::binder) fn bind_create_policy(
    s: &CreatePolicyStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    let resolved = lookup_table_reference(catalog, &s.table)?;
    let table_name = resolved.plan_name;
    let meta = resolved.meta;
    let using = s
        .using
        .as_ref()
        .map(|expr| bind_tenant_policy_expr(expr, &meta.schema))
        .transpose()?;
    let with_check = s
        .with_check
        .as_ref()
        .map(|expr| bind_tenant_policy_expr(expr, &meta.schema))
        .transpose()?;
    if using.is_none() && with_check.is_none() {
        return Err(PlanError::NotSupported(
            "CREATE POLICY requires USING or WITH CHECK",
        ));
    }
    Ok(LogicalPlan::CreatePolicy {
        policy: LogicalRlsPolicy {
            policy_name: s.name.value.to_ascii_lowercase(),
            table_name,
            permissiveness: match s.permissiveness {
                AstPolicyPermissiveness::Permissive => LogicalRlsPermissiveness::Permissive,
                AstPolicyPermissiveness::Restrictive => LogicalRlsPermissiveness::Restrictive,
            },
            command: match s.command {
                AstPolicyCommand::All => LogicalRlsCommand::All,
                AstPolicyCommand::Select => LogicalRlsCommand::Select,
                AstPolicyCommand::Insert => LogicalRlsCommand::Insert,
                AstPolicyCommand::Update => LogicalRlsCommand::Update,
                AstPolicyCommand::Delete => LogicalRlsCommand::Delete,
            },
            roles: s
                .roles
                .iter()
                .map(|role| role.value.to_ascii_lowercase())
                .collect(),
            using,
            with_check,
        },
        schema: Schema::empty(),
    })
}

fn bind_tenant_policy_expr(
    expr: &Expr,
    table_schema: &Schema,
) -> Result<LogicalTenantPolicyExpr, PlanError> {
    let expr = unparen_expr(expr);
    let Expr::Binary {
        op: BinaryOp::Eq,
        left,
        right,
        ..
    } = expr
    else {
        return Err(PlanError::NotSupported(
            "CREATE POLICY currently supports tenant equality predicates",
        ));
    };
    bind_tenant_policy_pair(left, right, table_schema)
        .or_else(|| bind_tenant_policy_pair(right, left, table_schema))
        .ok_or(PlanError::NotSupported(
            "CREATE POLICY predicate must compare a column to current_setting(setting, true)",
        ))
}

fn bind_tenant_policy_pair(
    column_expr: &Expr,
    setting_expr: &Expr,
    table_schema: &Schema,
) -> Option<LogicalTenantPolicyExpr> {
    let Expr::Column { name } = unparen_expr(column_expr) else {
        return None;
    };
    if name.parts.len() != 1 {
        return None;
    }
    let column_name = name.parts[0].value.to_ascii_lowercase();
    let (column_index, field) = table_schema.find(&column_name)?;
    if !field.data_type.is_textlike() {
        return None;
    }
    let setting_name = extract_current_setting_name(setting_expr)?;
    Some(LogicalTenantPolicyExpr {
        column_index,
        column_name,
        setting_name,
    })
}

fn extract_current_setting_name(expr: &Expr) -> Option<String> {
    let Expr::Call {
        name,
        args,
        distinct: false,
        over: None,
        ..
    } = unparen_expr(expr)
    else {
        return None;
    };
    if object_name_simple(name) != "current_setting" || args.len() != 2 {
        return None;
    }
    let Expr::Literal(Literal::String { value, .. }) = unparen_expr(&args[0]) else {
        return None;
    };
    let Expr::Literal(Literal::Bool { value: true, .. }) = unparen_expr(&args[1]) else {
        return None;
    };
    Some(value.to_ascii_lowercase())
}

pub(in crate::binder) fn bind_create_materialized_view(
    s: &CreateMaterializedViewStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    let table_name = object_name_simple(&s.name);
    let namespace = object_name_namespace(&s.name);
    if !s.if_not_exists
        && catalog
            .lookup_table_in_schema(&namespace, &table_name)
            .is_some()
    {
        return Err(PlanError::DuplicateTable(table_name));
    }

    let mut scope = ScopeStack::new();
    let source = bind_select(&s.source, catalog, &mut scope)?;
    let columns = materialized_view_schema(source.schema(), &s.columns)?;
    if columns.is_empty() {
        return Err(PlanError::NotSupported(
            "CREATE MATERIALIZED VIEW: zero columns",
        ));
    }

    Ok(LogicalPlan::CreateMaterializedView {
        table_name,
        namespace,
        columns,
        source: Box::new(source),
        if_not_exists: s.if_not_exists,
        schema: Schema::empty(),
    })
}

pub(in crate::binder) fn bind_create_view(
    s: &CreateViewStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    let table_name = object_name_simple(&s.name);
    let namespace = object_name_namespace(&s.name);
    if !s.or_replace
        && catalog
            .lookup_table_in_schema(&namespace, &table_name)
            .is_some()
    {
        return Err(PlanError::DuplicateTable(table_name));
    }

    let mut scope = ScopeStack::new();
    let source = bind_select(&s.source, catalog, &mut scope)?;
    let columns = materialized_view_schema(source.schema(), &s.columns)?;
    if columns.is_empty() {
        return Err(PlanError::NotSupported("CREATE VIEW: zero columns"));
    }

    Ok(LogicalPlan::CreateView {
        table_name,
        namespace,
        columns,
        source: Box::new(source),
        source_sql: s.source_sql.clone(),
        or_replace: s.or_replace,
        schema: Schema::empty(),
    })
}

pub(in crate::binder) fn bind_alter_view(
    s: &AlterViewStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    let resolved = lookup_table_reference(catalog, &s.name)?;
    let action = match &s.action {
        AlterViewAction::RenameView { new_name, .. } => LogicalAlterViewAction::RenameView {
            new_name: new_name.value.to_ascii_lowercase(),
        },
        AlterViewAction::SetSchema { schema_name, .. } => LogicalAlterViewAction::SetSchema {
            new_schema: schema_name.value.to_ascii_lowercase(),
        },
        AlterViewAction::ReplaceDefinition { .. } => {
            return Err(PlanError::NotSupported(
                "ALTER VIEW ... AS SELECT is not supported until dependency-safe view replacement lands",
            ));
        }
    };
    Ok(LogicalPlan::AlterView {
        view_name: resolved.plan_name,
        action,
        schema: Schema::empty(),
    })
}

fn materialized_view_schema(
    source_schema: &Schema,
    aliases: &[Identifier],
) -> Result<Schema, PlanError> {
    if aliases.is_empty() {
        return Ok(source_schema.clone());
    }
    if aliases.len() != source_schema.len() {
        return Err(PlanError::TypeMismatch(format!(
            "CREATE MATERIALIZED VIEW column list has {} names but query returns {} columns",
            aliases.len(),
            source_schema.len()
        )));
    }
    let mut fields = Vec::with_capacity(source_schema.len());
    for (field, alias) in source_schema.fields().iter().zip(aliases) {
        let mut renamed = field.clone();
        renamed.name = alias.value.clone();
        fields.push(renamed);
    }
    Schema::new(fields).map_err(|e| PlanError::TypeMismatch(e.to_string()))
}
