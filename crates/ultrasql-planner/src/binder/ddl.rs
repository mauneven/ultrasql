//! DDL binders. Split out of `binder/mod.rs` to keep each
//! production source file under the 600-line ceiling.
//!
//! Public entry points are `pub(super)` so the dispatch in
//! `binder::bind` can route to them; internal helpers
//! (`object_name_namespace`, `resolve_type_name`,
//! `resolve_column_nullability`, `synthesise_index_name`) stay
//! private to this module.

use ultrasql_core::{DataType, Field, GeometryType, MAX_VECTOR_DIMS, RangeType, Schema};
use ultrasql_parser::ast::{
    AlterRoleStmt, AlterSequenceStmt, AlterTableAction, AlterTableStmt, BinaryOp, ColumnConstraint,
    CommentStmt, CommentTarget, CopyDirection as AstCopyDirection, CopyFormat as AstCopyFormat,
    CopyOption, CopySource as AstCopySource, CopyStmt, CreateDomainStmt, CreateIndexStmt,
    CreateMaterializedViewStmt, CreatePolicyStmt, CreateRoleStmt, CreateSequenceStmt,
    CreateTableStmt, CreateTypeKind, CreateTypeStmt, DomainConstraint, DropIndexStmt, DropRoleStmt,
    DropSequenceStmt, DropTableStmt, Expr, Identifier, Literal, ObjectName,
    PolicyCommand as AstPolicyCommand, PolicyPermissiveness as AstPolicyPermissiveness,
    ReferentialAction as AstReferentialAction, RoleOption as AstRoleOption,
    RoleStmtKind as AstRoleStmtKind, SequenceOption, TableConstraint, TruncateStmt, TypeName,
};

use super::expr_bind::coerce_literal_to_type;
use super::{
    Catalog, LogicalAlterTableAction, LogicalPlan, PlanError, ScalarExpr, ScopeStack, bind_expr,
    bind_select, object_name_simple,
};
use crate::catalog::TableMeta;
use crate::plan::{
    AggregateFunc, CopyDirection, CopyFormat, CopySource, LogicalAggregatingIndex,
    LogicalAggregatingIndexExpr, LogicalCheckConstraint, LogicalCommentTarget,
    LogicalExclusionConstraint, LogicalExclusionElement, LogicalForeignKeyConstraint,
    LogicalIndexMethod, LogicalIndexOption, LogicalReferentialAction, LogicalRlsCommand,
    LogicalRlsPermissiveness, LogicalRlsPolicy, LogicalRoleKind, LogicalRoleOptions,
    LogicalSequenceChange, LogicalSequenceOptions, LogicalTableOption, LogicalTenantPolicyExpr,
    LogicalTimePartition, LogicalUniqueConstraint,
};

const MAX_NUMERIC_PRECISION: u32 = 131_072;

struct RawUniqueConstraint {
    name: String,
    columns: Vec<String>,
    primary_key: bool,
}

struct RawCheckConstraint<'a> {
    name: String,
    expr: &'a Expr,
}

struct RawForeignKeyConstraint {
    name: String,
    columns: Vec<String>,
    target_table: String,
    target_columns: Vec<String>,
    on_delete: LogicalReferentialAction,
    on_update: LogicalReferentialAction,
    deferrable: bool,
    initially_deferred: bool,
}

struct RawExclusionConstraint {
    name: String,
    method: LogicalIndexMethod,
    elements: Vec<RawExclusionElement>,
}

struct RawExclusionElement {
    column: String,
    op: ultrasql_parser::ast::BinaryOp,
}

pub(super) fn bind_create_table(
    s: &CreateTableStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    let table_name = object_name_simple(&s.name);
    let namespace = object_name_namespace(&s.name);
    if !s.if_not_exists && catalog.lookup_table(&table_name).is_some() {
        return Err(PlanError::DuplicateTable(table_name));
    }
    if s.columns.is_empty() {
        return Err(PlanError::NotSupported("CREATE TABLE: zero columns"));
    }
    let mut fields: Vec<Field> = Vec::with_capacity(s.columns.len());
    let mut defaults: Vec<Option<&Expr>> = Vec::with_capacity(s.columns.len());
    let mut sequence_defaults: Vec<Option<String>> = Vec::with_capacity(s.columns.len());
    let mut sequence_options: Vec<Option<LogicalSequenceOptions>> =
        Vec::with_capacity(s.columns.len());
    let mut identity_always: Vec<bool> = Vec::with_capacity(s.columns.len());
    let mut generated_stored_raw: Vec<Option<&Expr>> = Vec::with_capacity(s.columns.len());
    let mut raw_checks: Vec<RawCheckConstraint<'_>> = Vec::new();
    let mut raw_uniques: Vec<RawUniqueConstraint> = Vec::new();
    let mut raw_foreign_keys: Vec<RawForeignKeyConstraint> = Vec::new();
    let mut raw_exclusions: Vec<RawExclusionConstraint> = Vec::new();
    let mut primary_key_seen = false;
    for col in &s.columns {
        let name = col.name.value.clone();
        let folded = name.to_ascii_lowercase();
        if fields.iter().any(|f| f.name.to_ascii_lowercase() == folded) {
            return Err(PlanError::DuplicateColumn(name));
        }
        let (dtype, serial_default) =
            resolve_column_type(&table_name, &name, &col.data_type, catalog)?;
        let identity = column_identity(&col.constraints)?;
        let generated_stored = column_generated_stored(&col.constraints)?;
        if identity.is_some() && serial_default.is_some() {
            return Err(PlanError::NotSupported(
                "CREATE TABLE: SERIAL column may not also declare IDENTITY",
            ));
        }
        if generated_stored.is_some() && (identity.is_some() || serial_default.is_some()) {
            return Err(PlanError::NotSupported(
                "CREATE TABLE: generated stored column may not also declare SERIAL or IDENTITY",
            ));
        }
        if identity.is_some()
            && !matches!(dtype, DataType::Int16 | DataType::Int32 | DataType::Int64)
        {
            return Err(PlanError::TypeMismatch(format!(
                "IDENTITY column '{name}' must be SMALLINT, INTEGER, or BIGINT"
            )));
        }
        let (sequence_default, sequence_option, identity_is_always) =
            if let Some((always, options)) = identity {
                (
                    Some(format!(
                        "{}_{}_seq",
                        table_name.to_ascii_lowercase(),
                        name.to_ascii_lowercase()
                    )),
                    Some(options),
                    always,
                )
            } else {
                (
                    serial_default.clone(),
                    serial_default
                        .as_ref()
                        .map(|_| LogicalSequenceOptions::default()),
                    false,
                )
            };
        let nullable = resolve_column_nullability(&col.constraints)?;
        let nullable = nullable
            && sequence_default.is_none()
            && !matches!(dtype, DataType::Domain { not_null: true, .. });
        let field = if nullable {
            Field::nullable(name, dtype)
        } else {
            Field::required(name, dtype)
        };
        let default = column_default(&col.constraints)?;
        if generated_stored.is_some() && default.is_some() {
            return Err(PlanError::NotSupported(
                "CREATE TABLE: generated stored column may not also declare DEFAULT",
            ));
        }
        if sequence_default.is_some() && default.is_some() {
            return Err(PlanError::NotSupported(
                "CREATE TABLE: sequence-backed column may not also declare DEFAULT",
            ));
        }
        defaults.push(default);
        sequence_defaults.push(sequence_default);
        sequence_options.push(sequence_option);
        identity_always.push(identity_is_always);
        generated_stored_raw.push(generated_stored);
        collect_column_constraints(
            &table_name,
            &col.name,
            &col.constraints,
            &mut raw_checks,
            &mut raw_uniques,
            &mut raw_foreign_keys,
            &mut primary_key_seen,
        )?;
        fields.push(field);
    }
    collect_table_constraints(
        &table_name,
        &s.table_constraints,
        &mut raw_checks,
        &mut raw_uniques,
        &mut raw_foreign_keys,
        &mut raw_exclusions,
        &mut primary_key_seen,
    )?;
    for raw in &raw_uniques {
        if raw.primary_key {
            for col_name in &raw.columns {
                let Some(field) = fields
                    .iter_mut()
                    .find(|f| f.name.eq_ignore_ascii_case(col_name))
                else {
                    return Err(PlanError::ColumnNotFound(col_name.clone()));
                };
                field.nullable = false;
            }
        }
    }
    let columns = Schema::new(fields).map_err(|err| PlanError::TypeMismatch(err.to_string()))?;
    let mut bound_defaults = Vec::with_capacity(defaults.len());
    for (idx, default) in defaults.into_iter().enumerate() {
        let Some(expr) = default else {
            bound_defaults.push(None);
            continue;
        };
        let mut scope = ScopeStack::new();
        let mut bound = bind_expr(expr, &Schema::empty(), catalog, &mut scope)?;
        if !is_default_safe(&bound) {
            return Err(PlanError::NotSupported(
                "CREATE TABLE: DEFAULT may not refer to rows, parameters, or subqueries",
            ));
        }
        coerce_default_expr_to_type(&mut bound, &columns.field_at(idx).data_type);
        let target = &columns.field_at(idx).data_type;
        let actual = bound.data_type();
        if actual != target.clone() && actual != DataType::Null {
            return Err(PlanError::TypeMismatch(format!(
                "DEFAULT for column '{}' has type {:?}, expected {:?}",
                columns.field_at(idx).name,
                actual,
                target,
            )));
        }
        bound_defaults.push(Some(bound));
    }
    let mut generated_stored = Vec::with_capacity(generated_stored_raw.len());
    let generated_columns: Vec<bool> = generated_stored_raw.iter().map(Option::is_some).collect();
    for (idx, generated) in generated_stored_raw.into_iter().enumerate() {
        let Some(expr) = generated else {
            generated_stored.push(None);
            continue;
        };
        let mut scope = ScopeStack::new();
        let mut bound = bind_expr(expr, &columns, catalog, &mut scope)?;
        if !is_generated_stored_safe(&bound) {
            return Err(PlanError::NotSupported(
                "CREATE TABLE: generated stored expression may not contain parameters or subqueries",
            ));
        }
        if expr_references_generated_column(&bound, &generated_columns) {
            return Err(PlanError::NotSupported(
                "CREATE TABLE: generated stored expression may not reference generated columns",
            ));
        }
        coerce_default_expr_to_type(&mut bound, &columns.field_at(idx).data_type);
        let target = &columns.field_at(idx).data_type;
        let actual = bound.data_type();
        if actual != target.clone() && actual != DataType::Null {
            return Err(PlanError::TypeMismatch(format!(
                "generated expression for column '{}' has type {:?}, expected {:?}",
                columns.field_at(idx).name,
                actual,
                target,
            )));
        }
        generated_stored.push(Some(bound));
    }
    let mut checks = Vec::with_capacity(raw_checks.len());
    for raw in raw_checks {
        let mut scope = ScopeStack::new();
        let bound = bind_expr(raw.expr, &columns, catalog, &mut scope)?;
        let ty = bound.data_type();
        if ty != DataType::Bool && ty != DataType::Null {
            return Err(PlanError::TypeMismatch(format!(
                "CHECK constraint '{}' has type {:?}, expected Bool",
                raw.name, ty,
            )));
        }
        checks.push(LogicalCheckConstraint {
            name: raw.name,
            expr: bound,
        });
    }
    let unique_constraints = bind_unique_constraints(&columns, raw_uniques)?;
    let foreign_keys = bind_foreign_key_constraints(&columns, raw_foreign_keys, catalog)?;
    let exclusion_constraints = bind_exclusion_constraints(&columns, raw_exclusions)?;
    let partition = bind_time_partition(&columns, s.partition_by.as_ref())?;
    Ok(LogicalPlan::CreateTable {
        table_name,
        namespace,
        columns,
        defaults: bound_defaults,
        sequence_defaults,
        sequence_options,
        identity_always,
        generated_stored,
        checks,
        unique_constraints,
        foreign_keys,
        exclusion_constraints,
        partition,
        if_not_exists: s.if_not_exists,
        schema: Schema::empty(),
    })
}

pub(super) fn bind_create_type(
    s: &CreateTypeStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    let type_name = object_name_simple(&s.name);
    let namespace = object_name_namespace(&s.name);
    if catalog.lookup_type(&type_name).is_some() {
        return Err(PlanError::TypeMismatch(format!(
            "type '{type_name}' already exists"
        )));
    }
    match &s.kind {
        CreateTypeKind::Enum { labels } => {
            if labels.is_empty() {
                return Err(PlanError::TypeMismatch(format!(
                    "enum type '{type_name}' must have at least one label"
                )));
            }
            let mut seen = std::collections::HashSet::with_capacity(labels.len());
            for label in labels {
                if !seen.insert(label) {
                    return Err(PlanError::TypeMismatch(format!(
                        "enum type '{type_name}' repeats label '{label}'"
                    )));
                }
            }
            Ok(LogicalPlan::CreateTypeEnum {
                type_name,
                namespace,
                labels: labels.clone(),
                schema: Schema::empty(),
            })
        }
        CreateTypeKind::Composite { attributes } => {
            if attributes.is_empty() {
                return Err(PlanError::TypeMismatch(format!(
                    "composite type '{type_name}' must have at least one attribute"
                )));
            }
            let fields = attributes
                .iter()
                .map(|attr| {
                    let data_type = resolve_type_name_with_catalog(&attr.data_type, catalog)?;
                    Ok(Field::nullable(attr.name.value.clone(), data_type))
                })
                .collect::<Result<Vec<_>, PlanError>>()?;
            let attributes =
                Schema::new(fields).map_err(|e| PlanError::TypeMismatch(e.to_string()))?;
            Ok(LogicalPlan::CreateTypeComposite {
                type_name,
                namespace,
                attributes,
                schema: Schema::empty(),
            })
        }
    }
}

pub(super) fn bind_create_domain(
    s: &CreateDomainStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    let domain_name = object_name_simple(&s.name);
    let namespace = object_name_namespace(&s.name);
    if catalog.lookup_type(&domain_name).is_some() {
        return Err(PlanError::TypeMismatch(format!(
            "type '{domain_name}' already exists"
        )));
    }
    let base_type = resolve_type_name_with_catalog(&s.data_type, catalog)?;
    if matches!(base_type, DataType::Domain { .. }) {
        return Err(PlanError::NotSupported(
            "CREATE DOMAIN over another domain is not implemented",
        ));
    }
    let mut not_null = false;
    let mut check_ordinal = 0usize;
    let check_scope = Schema::new([Field::nullable("value", base_type.clone())])
        .map_err(|err| PlanError::TypeMismatch(err.to_string()))?;
    let mut checks = Vec::new();
    for constraint in &s.constraints {
        match constraint {
            DomainConstraint::NotNull { .. } => not_null = true,
            DomainConstraint::Null { .. } => not_null = false,
            DomainConstraint::Check { name, expr, .. } => {
                check_ordinal += 1;
                let mut scope = ScopeStack::new();
                let bound = bind_expr(expr, &check_scope, catalog, &mut scope)?;
                let ty = bound.data_type();
                if ty != DataType::Bool && ty != DataType::Null {
                    return Err(PlanError::TypeMismatch(format!(
                        "CHECK constraint on domain '{domain_name}' has type {:?}, expected Bool",
                        ty
                    )));
                }
                checks.push(LogicalCheckConstraint {
                    name: named_or(name.as_ref(), || {
                        format!("{domain_name}_check_{check_ordinal}")
                    }),
                    expr: bound,
                });
            }
        }
    }
    Ok(LogicalPlan::CreateDomain {
        domain_name,
        namespace,
        base_type,
        not_null,
        checks,
        schema: Schema::empty(),
    })
}

fn bind_time_partition(
    columns: &Schema,
    spec: Option<&ultrasql_parser::ast::TablePartitionSpec>,
) -> Result<Option<LogicalTimePartition>, PlanError> {
    let Some(spec) = spec else {
        return Ok(None);
    };
    let column = spec.column.value.clone();
    let (column_index, field) = columns
        .find(&column)
        .ok_or_else(|| PlanError::ColumnNotFound(column.clone()))?;
    if !matches!(field.data_type, DataType::Timestamp | DataType::TimestampTz) {
        return Err(PlanError::TypeMismatch(format!(
            "PARTITION BY RANGE column '{}' must be TIMESTAMP or TIMESTAMPTZ",
            field.name
        )));
    }
    Ok(Some(LogicalTimePartition {
        column: field.name.clone(),
        column_index,
    }))
}

fn column_default(constraints: &[ColumnConstraint]) -> Result<Option<&Expr>, PlanError> {
    let mut out = None;
    for c in constraints {
        if let ColumnConstraint::Default { expr, .. } = c {
            if out.is_some() {
                return Err(PlanError::NotSupported(
                    "CREATE TABLE: multiple DEFAULT clauses on one column",
                ));
            }
            out = Some(expr);
        }
    }
    Ok(out)
}

fn column_identity(
    constraints: &[ColumnConstraint],
) -> Result<Option<(bool, LogicalSequenceOptions)>, PlanError> {
    let mut out = None;
    for c in constraints {
        if let ColumnConstraint::GeneratedIdentity {
            always, options, ..
        } = c
        {
            if out.is_some() {
                return Err(PlanError::NotSupported(
                    "CREATE TABLE: multiple IDENTITY clauses on one column",
                ));
            }
            out = Some((*always, bind_sequence_options(options)?));
        }
    }
    Ok(out)
}

fn column_generated_stored(constraints: &[ColumnConstraint]) -> Result<Option<&Expr>, PlanError> {
    let mut out = None;
    for c in constraints {
        if let ColumnConstraint::GeneratedStored { expr, .. } = c {
            if out.is_some() {
                return Err(PlanError::NotSupported(
                    "CREATE TABLE: multiple generated stored clauses on one column",
                ));
            }
            out = Some(expr);
        }
    }
    Ok(out)
}

pub(super) fn bind_create_policy(
    s: &CreatePolicyStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    let table_name = object_name_simple(&s.table);
    let meta = catalog
        .lookup_table(&table_name)
        .ok_or_else(|| PlanError::TableNotFound(table_name.clone()))?;
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

fn unparen_expr(expr: &Expr) -> &Expr {
    match expr {
        Expr::Paren { expr, .. } => unparen_expr(expr),
        _ => expr,
    }
}

pub(super) fn bind_create_materialized_view(
    s: &CreateMaterializedViewStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    let table_name = object_name_simple(&s.name);
    let namespace = object_name_namespace(&s.name);
    if !s.if_not_exists && catalog.lookup_table(&table_name).is_some() {
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

fn collect_column_constraints<'a>(
    table: &str,
    column: &Identifier,
    constraints: &'a [ColumnConstraint],
    checks: &mut Vec<RawCheckConstraint<'a>>,
    uniques: &mut Vec<RawUniqueConstraint>,
    foreign_keys: &mut Vec<RawForeignKeyConstraint>,
    primary_key_seen: &mut bool,
) -> Result<(), PlanError> {
    let col = column.value.to_ascii_lowercase();
    for c in constraints {
        match c {
            ColumnConstraint::Check { name, expr, .. } => checks.push(RawCheckConstraint {
                name: named_or(name.as_ref(), || format!("{table}_{col}_check")),
                expr,
            }),
            ColumnConstraint::Unique { name, .. } => uniques.push(RawUniqueConstraint {
                name: named_or(name.as_ref(), || format!("{table}_{col}_key")),
                columns: vec![col.clone()],
                primary_key: false,
            }),
            ColumnConstraint::PrimaryKey { name, .. } => {
                if *primary_key_seen {
                    return Err(PlanError::NotSupported(
                        "CREATE TABLE: multiple PRIMARY KEY constraints",
                    ));
                }
                *primary_key_seen = true;
                uniques.push(RawUniqueConstraint {
                    name: named_or(name.as_ref(), || format!("{table}_pkey")),
                    columns: vec![col.clone()],
                    primary_key: true,
                });
            }
            ColumnConstraint::References {
                name,
                target_table,
                target_columns,
                on_delete,
                on_update,
                deferrable,
                initially_deferred,
                ..
            } => {
                if target_columns.is_empty() {
                    return Err(PlanError::NotSupported(
                        "CREATE TABLE: REFERENCES without target columns",
                    ));
                }
                foreign_keys.push(RawForeignKeyConstraint {
                    name: named_or(name.as_ref(), || format!("{table}_{col}_fkey")),
                    columns: vec![col.clone()],
                    target_table: object_name_simple(target_table),
                    target_columns: target_columns
                        .iter()
                        .map(|c| c.value.to_ascii_lowercase())
                        .collect(),
                    on_delete: bind_referential_action(*on_delete),
                    on_update: bind_referential_action(*on_update),
                    deferrable: *deferrable,
                    initially_deferred: *initially_deferred,
                });
            }
            ColumnConstraint::NotNull { .. }
            | ColumnConstraint::Null { .. }
            | ColumnConstraint::Default { .. }
            | ColumnConstraint::GeneratedIdentity { .. }
            | ColumnConstraint::GeneratedStored { .. } => {}
        }
    }
    Ok(())
}

fn collect_table_constraints<'a>(
    table: &str,
    constraints: &'a [TableConstraint],
    checks: &mut Vec<RawCheckConstraint<'a>>,
    uniques: &mut Vec<RawUniqueConstraint>,
    foreign_keys: &mut Vec<RawForeignKeyConstraint>,
    exclusions: &mut Vec<RawExclusionConstraint>,
    primary_key_seen: &mut bool,
) -> Result<(), PlanError> {
    let mut check_ordinal = checks.len();
    for c in constraints {
        match c {
            TableConstraint::Check { name, expr, .. } => {
                check_ordinal += 1;
                checks.push(RawCheckConstraint {
                    name: named_or(name.as_ref(), || format!("{table}_check_{check_ordinal}")),
                    expr,
                });
            }
            TableConstraint::Unique { name, columns, .. } => {
                let cols = columns
                    .iter()
                    .map(|c| c.value.to_ascii_lowercase())
                    .collect::<Vec<_>>();
                uniques.push(RawUniqueConstraint {
                    name: named_or(name.as_ref(), || unique_name(table, &cols, false)),
                    columns: cols,
                    primary_key: false,
                });
            }
            TableConstraint::PrimaryKey { name, columns, .. } => {
                if *primary_key_seen {
                    return Err(PlanError::NotSupported(
                        "CREATE TABLE: multiple PRIMARY KEY constraints",
                    ));
                }
                *primary_key_seen = true;
                let cols = columns
                    .iter()
                    .map(|c| c.value.to_ascii_lowercase())
                    .collect::<Vec<_>>();
                uniques.push(RawUniqueConstraint {
                    name: named_or(name.as_ref(), || unique_name(table, &cols, true)),
                    columns: cols,
                    primary_key: true,
                });
            }
            TableConstraint::ForeignKey {
                name,
                columns,
                target_table,
                target_columns,
                on_delete,
                on_update,
                deferrable,
                initially_deferred,
                ..
            } => {
                if target_columns.is_empty() {
                    return Err(PlanError::NotSupported(
                        "CREATE TABLE: REFERENCES without target columns",
                    ));
                }
                let cols = columns
                    .iter()
                    .map(|c| c.value.to_ascii_lowercase())
                    .collect::<Vec<_>>();
                foreign_keys.push(RawForeignKeyConstraint {
                    name: named_or(name.as_ref(), || format!("{}_fkey", cols.join("_"))),
                    columns: cols,
                    target_table: object_name_simple(target_table),
                    target_columns: target_columns
                        .iter()
                        .map(|c| c.value.to_ascii_lowercase())
                        .collect(),
                    on_delete: bind_referential_action(*on_delete),
                    on_update: bind_referential_action(*on_update),
                    deferrable: *deferrable,
                    initially_deferred: *initially_deferred,
                });
            }
            TableConstraint::Exclude {
                name,
                method,
                elements,
                ..
            } => {
                let method = bind_index_method(&method.value)?;
                if method != LogicalIndexMethod::Gist {
                    return Err(PlanError::NotSupported(
                        "EXCLUDE constraints currently require USING gist",
                    ));
                }
                let exclusion_ordinal = exclusions.len() + 1;
                exclusions.push(RawExclusionConstraint {
                    name: named_or(name.as_ref(), || {
                        format!("{table}_excl_{exclusion_ordinal}")
                    }),
                    method,
                    elements: elements
                        .iter()
                        .map(|element| RawExclusionElement {
                            column: element.column.value.to_ascii_lowercase(),
                            op: element.op,
                        })
                        .collect(),
                });
            }
        }
    }
    Ok(())
}

fn bind_exclusion_constraints(
    schema: &Schema,
    raw_exclusions: Vec<RawExclusionConstraint>,
) -> Result<Vec<LogicalExclusionConstraint>, PlanError> {
    let mut out = Vec::with_capacity(raw_exclusions.len());
    for raw in raw_exclusions {
        if raw.elements.is_empty() {
            return Err(PlanError::NotSupported(
                "CREATE TABLE: empty EXCLUDE element list",
            ));
        }
        let mut seen = std::collections::HashSet::with_capacity(raw.elements.len());
        let mut elements = Vec::with_capacity(raw.elements.len());
        for element in raw.elements {
            if !seen.insert(element.column.clone()) {
                return Err(PlanError::DuplicateColumn(element.column));
            }
            let (idx, field) = schema
                .find(&element.column)
                .ok_or_else(|| PlanError::ColumnNotFound(element.column.clone()))?;
            validate_exclusion_operator(&raw.name, &field.data_type, element.op)?;
            elements.push(LogicalExclusionElement {
                column: idx,
                op: element.op,
            });
        }
        out.push(LogicalExclusionConstraint {
            name: raw.name,
            method: raw.method,
            elements,
        });
    }
    Ok(out)
}

fn validate_exclusion_operator(
    name: &str,
    data_type: &DataType,
    op: ultrasql_parser::ast::BinaryOp,
) -> Result<(), PlanError> {
    match op {
        ultrasql_parser::ast::BinaryOp::Eq => Ok(()),
        ultrasql_parser::ast::BinaryOp::Overlap
        | ultrasql_parser::ast::BinaryOp::JsonContains
        | ultrasql_parser::ast::BinaryOp::JsonContained
            if matches!(data_type, DataType::Range(_) | DataType::Geometry(_)) =>
        {
            Ok(())
        }
        _ => Err(PlanError::TypeMismatch(format!(
            "EXCLUDE constraint '{name}' operator {} is not supported for {data_type}",
            super::expr_type::display_binary(op),
        ))),
    }
}

fn bind_index_method(method: &str) -> Result<LogicalIndexMethod, PlanError> {
    match method.to_ascii_lowercase().as_str() {
        "btree" => Ok(LogicalIndexMethod::Btree),
        "hash" => Ok(LogicalIndexMethod::Hash),
        "gin" => Ok(LogicalIndexMethod::Gin),
        "gist" => Ok(LogicalIndexMethod::Gist),
        "brin" => Ok(LogicalIndexMethod::Brin),
        "hnsw" => Ok(LogicalIndexMethod::Hnsw),
        "ivfflat" => Ok(LogicalIndexMethod::IvfFlat),
        _ => Err(PlanError::NotSupported(
            "only btree, hash, gin, gist, brin, hnsw, and ivfflat methods are supported",
        )),
    }
}

fn bind_unique_constraints(
    schema: &Schema,
    raw_uniques: Vec<RawUniqueConstraint>,
) -> Result<Vec<LogicalUniqueConstraint>, PlanError> {
    let mut out = Vec::with_capacity(raw_uniques.len());
    for raw in raw_uniques {
        if raw.columns.is_empty() {
            return Err(PlanError::NotSupported(
                "CREATE TABLE: empty UNIQUE / PRIMARY KEY column list",
            ));
        }
        let mut seen = std::collections::HashSet::with_capacity(raw.columns.len());
        let mut cols = Vec::with_capacity(raw.columns.len());
        for col in raw.columns {
            if !seen.insert(col.clone()) {
                return Err(PlanError::DuplicateColumn(col));
            }
            let (idx, _) = schema
                .find(&col)
                .ok_or_else(|| PlanError::ColumnNotFound(col.clone()))?;
            cols.push(idx);
        }
        out.push(LogicalUniqueConstraint {
            name: raw.name,
            columns: cols,
            primary_key: raw.primary_key,
        });
    }
    Ok(out)
}

fn bind_foreign_key_constraints(
    schema: &Schema,
    raw_foreign_keys: Vec<RawForeignKeyConstraint>,
    catalog: &dyn Catalog,
) -> Result<Vec<LogicalForeignKeyConstraint>, PlanError> {
    let mut out = Vec::with_capacity(raw_foreign_keys.len());
    for raw in raw_foreign_keys {
        if raw.columns.len() != raw.target_columns.len() {
            return Err(PlanError::TypeMismatch(format!(
                "FOREIGN KEY '{}' has {} referencing columns but {} referenced columns",
                raw.name,
                raw.columns.len(),
                raw.target_columns.len()
            )));
        }
        let target = catalog
            .lookup_table(&raw.target_table)
            .ok_or_else(|| PlanError::TableNotFound(raw.target_table.clone()))?;
        let mut columns = Vec::with_capacity(raw.columns.len());
        let mut target_columns = Vec::with_capacity(raw.target_columns.len());
        for (src, dst) in raw.columns.iter().zip(raw.target_columns.iter()) {
            let (src_idx, src_field) = schema
                .find(src)
                .ok_or_else(|| PlanError::ColumnNotFound(src.clone()))?;
            let (dst_idx, dst_field) = target
                .schema
                .find(dst)
                .ok_or_else(|| PlanError::ColumnNotFound(dst.clone()))?;
            if src_field.data_type != dst_field.data_type {
                return Err(PlanError::TypeMismatch(format!(
                    "FOREIGN KEY '{}' type mismatch: {} {:?} references {} {:?}",
                    raw.name, src, src_field.data_type, dst, dst_field.data_type
                )));
            }
            columns.push(src_idx);
            target_columns.push(dst_idx);
        }
        out.push(LogicalForeignKeyConstraint {
            name: raw.name,
            columns,
            target_table: raw.target_table,
            target_columns,
            on_delete: raw.on_delete,
            on_update: raw.on_update,
            deferrable: raw.deferrable,
            initially_deferred: raw.initially_deferred,
        });
    }
    Ok(out)
}

const fn bind_referential_action(action: AstReferentialAction) -> LogicalReferentialAction {
    match action {
        AstReferentialAction::NoAction => LogicalReferentialAction::NoAction,
        AstReferentialAction::Restrict => LogicalReferentialAction::Restrict,
        AstReferentialAction::Cascade => LogicalReferentialAction::Cascade,
        AstReferentialAction::SetNull => LogicalReferentialAction::SetNull,
        AstReferentialAction::SetDefault => LogicalReferentialAction::SetDefault,
    }
}

fn named_or<F>(name: Option<&Identifier>, fallback: F) -> String
where
    F: FnOnce() -> String,
{
    name.map_or_else(fallback, |n| n.value.to_ascii_lowercase())
}

fn unique_name(table: &str, columns: &[String], primary_key: bool) -> String {
    if primary_key {
        return format!("{table}_pkey");
    }
    let mut s = String::from(table);
    for col in columns {
        s.push('_');
        s.push_str(col);
    }
    s.push_str("_key");
    s
}

fn is_default_safe(expr: &ScalarExpr) -> bool {
    match expr {
        ScalarExpr::Literal { .. } => true,
        ScalarExpr::Column { .. }
        | ScalarExpr::Parameter { .. }
        | ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => false,
        ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => is_default_safe(expr),
        ScalarExpr::Binary { left, right, .. } => is_default_safe(left) && is_default_safe(right),
        ScalarExpr::FunctionCall { args, .. } => args.iter().all(is_default_safe),
    }
}

fn coerce_default_expr_to_type(expr: &mut ScalarExpr, target: &DataType) {
    coerce_literal_to_type(expr, target);
    if let (
        DataType::Timestamp,
        ScalarExpr::FunctionCall {
            name, data_type, ..
        },
    ) = (target, expr)
        && matches!(name.as_str(), "now" | "current_timestamp")
        && matches!(data_type, DataType::TimestampTz)
    {
        *data_type = DataType::Timestamp;
    }
}

fn is_generated_stored_safe(expr: &ScalarExpr) -> bool {
    match expr {
        ScalarExpr::Literal { .. } | ScalarExpr::Column { .. } => true,
        ScalarExpr::Parameter { .. }
        | ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => false,
        ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => {
            is_generated_stored_safe(expr)
        }
        ScalarExpr::Binary { left, right, .. } => {
            is_generated_stored_safe(left) && is_generated_stored_safe(right)
        }
        ScalarExpr::FunctionCall { args, .. } => args.iter().all(is_generated_stored_safe),
    }
}

fn expr_references_generated_column(expr: &ScalarExpr, generated_columns: &[bool]) -> bool {
    match expr {
        ScalarExpr::Column { index, .. } => generated_columns.get(*index).copied().unwrap_or(false),
        ScalarExpr::Literal { .. } | ScalarExpr::Parameter { .. } => false,
        ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => {
            expr_references_generated_column(expr, generated_columns)
        }
        ScalarExpr::Binary { left, right, .. } => {
            expr_references_generated_column(left, generated_columns)
                || expr_references_generated_column(right, generated_columns)
        }
        ScalarExpr::OuterColumn { .. } => false,
        ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => false,
        ScalarExpr::FunctionCall { args, .. } => args
            .iter()
            .any(|arg| expr_references_generated_column(arg, generated_columns)),
    }
}

/// Pull the namespace component out of a possibly-qualified relation
/// name. `t` → `"public"`; `s.t` → `"s"`; `c.s.t` → `"s"`.
fn object_name_namespace(name: &ObjectName) -> String {
    if name.parts.len() >= 2 {
        let idx = name.parts.len() - 2;
        name.parts[idx].value.to_ascii_lowercase()
    } else {
        String::from("public")
    }
}

/// Resolve a parser [`TypeName`] to an UltraSQL [`DataType`].
///
/// The v0.5 type surface is intentionally narrow; types outside the
/// listed set return [`PlanError::NotSupported`]. Length modifiers
/// (e.g. `VARCHAR(255)`) are honored where the target [`DataType`]
/// carries a `max_len` slot.
pub(super) fn resolve_type_name(t: &TypeName) -> Result<DataType, PlanError> {
    if t.is_array {
        let mut inner = t.clone();
        inner.is_array = false;
        inner.array_dimensions = 0;
        let mut ty = resolve_type_name(&inner)?;
        for _ in 0..t.array_dimensions.max(1) {
            ty = DataType::Array(Box::new(ty));
        }
        return Ok(ty);
    }
    let max_len_modifier = || t.type_modifiers.first().copied();
    match t.name.value.as_str() {
        "int" | "integer" | "int4" => Ok(DataType::Int32),
        "bigint" | "int8" => Ok(DataType::Int64),
        "smallint" | "int2" => Ok(DataType::Int16),
        "bool" | "boolean" => Ok(DataType::Bool),
        "real" | "float4" => Ok(DataType::Float32),
        "double" | "double precision" | "float" | "float8" => Ok(DataType::Float64),
        "text" => Ok(DataType::Text { max_len: None }),
        "varchar" | "character varying" => Ok(DataType::Text {
            max_len: max_len_modifier(),
        }),
        "char" | "character" => resolve_bpchar_type(max_len_modifier().or(Some(1))),
        "bpchar" => resolve_bpchar_type(max_len_modifier()),
        "bit" => resolve_bit_type(max_len_modifier().or(Some(1))),
        "varbit" | "bit varying" => resolve_varbit_type(max_len_modifier()),
        "json" => Ok(DataType::Json),
        "jsonb" => Ok(DataType::Jsonb),
        "xml" => Ok(DataType::Xml),
        "vector" => resolve_vector_family_type("VECTOR", t, |dims| DataType::Vector { dims }),
        "halfvec" => resolve_vector_family_type("HALFVEC", t, |dims| DataType::HalfVec { dims }),
        "sparsevec" => {
            resolve_vector_family_type("SPARSEVEC", t, |dims| DataType::SparseVec { dims })
        }
        "bitvec" => resolve_vector_family_type("BITVEC", t, |dims| DataType::BitVec { dims }),
        "bytea" => Ok(DataType::Bytea),
        // `DATE` columns are encoded by the row codec as 4-byte
        // little-endian i32 days since 2000-01-01 (see
        // `crates/ultrasql-executor/src/row_codec.rs`); the SQL
        // surface is enabled.
        "date" => Ok(DataType::Date),
        // Bare `NUMERIC` is unconstrained. `NUMERIC(p)` gets scale
        // zero, and `NUMERIC(p, s)` carries its declared display scale.
        // The row codec stores values in PostgreSQL's base-10000 numeric
        // payload shape; executor arithmetic still narrows runtime values
        // to the current Decimal representation.
        "decimal" | "numeric" => resolve_decimal_type(t),
        "money" => Ok(DataType::Money),
        "oid" => Ok(DataType::Oid),
        "regclass" => Ok(DataType::RegClass),
        "regtype" => Ok(DataType::RegType),
        "pg_lsn" => Ok(DataType::PgLsn),
        "inet" => Ok(DataType::Inet),
        "cidr" => Ok(DataType::Cidr),
        "macaddr" => Ok(DataType::MacAddr),
        "macaddr8" => Ok(DataType::MacAddr8),
        "time" | "time without time zone" => Ok(DataType::Time),
        "timetz" | "time with time zone" => Ok(DataType::TimeTz),
        "timestamp" | "timestamp without time zone" => Ok(DataType::Timestamp),
        "timestamptz" | "timestamp with time zone" => Ok(DataType::TimestampTz),
        "uuid" => Ok(DataType::Uuid),
        "int4range" => Ok(DataType::Range(RangeType::Int4)),
        "int8range" => Ok(DataType::Range(RangeType::Int8)),
        "numrange" => Ok(DataType::Range(RangeType::Num)),
        "daterange" => Ok(DataType::Range(RangeType::Date)),
        "tsrange" => Ok(DataType::Range(RangeType::Timestamp)),
        "tstzrange" => Ok(DataType::Range(RangeType::TimestampTz)),
        "point" => Ok(DataType::Geometry(GeometryType::Point)),
        "box" => Ok(DataType::Geometry(GeometryType::Box)),
        "circle" => Ok(DataType::Geometry(GeometryType::Circle)),
        "line" => Ok(DataType::Geometry(GeometryType::Line)),
        "lseg" => Ok(DataType::Geometry(GeometryType::Lseg)),
        "path" => Ok(DataType::Geometry(GeometryType::Path)),
        "polygon" => Ok(DataType::Geometry(GeometryType::Polygon)),
        _ => Err(PlanError::NotSupported(
            "CREATE TABLE: column type not implemented in v0.5",
        )),
    }
}

fn resolve_type_name_with_catalog(
    t: &TypeName,
    catalog: &dyn Catalog,
) -> Result<DataType, PlanError> {
    if t.is_array {
        let mut inner = t.clone();
        inner.is_array = false;
        inner.array_dimensions = 0;
        let mut ty = resolve_type_name_with_catalog(&inner, catalog)?;
        for _ in 0..t.array_dimensions.max(1) {
            ty = DataType::Array(Box::new(ty));
        }
        return Ok(ty);
    }
    match resolve_type_name(t) {
        Ok(dtype) => Ok(dtype),
        Err(PlanError::NotSupported(_)) => catalog.lookup_type(&t.name.value).ok_or({
            PlanError::NotSupported("CREATE TABLE: column type not implemented in v0.5")
        }),
        Err(err) => Err(err),
    }
}

fn resolve_bpchar_type(len: Option<u32>) -> Result<DataType, PlanError> {
    if matches!(len, Some(0)) {
        return Err(PlanError::TypeMismatch(
            "length for type character must be at least 1".to_owned(),
        ));
    }
    Ok(DataType::Char { len })
}

fn resolve_bit_type(len: Option<u32>) -> Result<DataType, PlanError> {
    if matches!(len, Some(0)) {
        return Err(PlanError::TypeMismatch(
            "length for type bit must be at least 1".to_owned(),
        ));
    }
    Ok(DataType::Bit { len })
}

fn resolve_varbit_type(max_len: Option<u32>) -> Result<DataType, PlanError> {
    if matches!(max_len, Some(0)) {
        return Err(PlanError::TypeMismatch(
            "length for type bit varying must be at least 1".to_owned(),
        ));
    }
    Ok(DataType::VarBit { max_len })
}

fn resolve_decimal_type(t: &TypeName) -> Result<DataType, PlanError> {
    if t.type_modifiers.len() > 2 {
        return Err(PlanError::TypeMismatch(
            "NUMERIC accepts at most precision and scale modifiers".to_owned(),
        ));
    }
    let precision = t.type_modifiers.first().copied();
    if matches!(precision, Some(0)) {
        return Err(PlanError::TypeMismatch(
            "NUMERIC precision must be at least 1".to_owned(),
        ));
    }
    if matches!(precision, Some(p) if p > MAX_NUMERIC_PRECISION) {
        return Err(PlanError::TypeMismatch(format!(
            "NUMERIC precision must be at most {MAX_NUMERIC_PRECISION}"
        )));
    }
    let modifiers = t.type_modifiers.as_slice();
    let scale = match modifiers {
        [] => None,
        [_] => Some(0),
        [_, s] => {
            let scale = i32::try_from(*s).map_err(|_| {
                PlanError::TypeMismatch("NUMERIC scale does not fit int32".to_owned())
            })?;
            Some(scale)
        }
        _ => unreachable!("modifier length checked above"),
    };
    Ok(DataType::Decimal { precision, scale })
}

fn resolve_vector_family_type(
    sql_name: &str,
    t: &TypeName,
    build: fn(Option<u32>) -> DataType,
) -> Result<DataType, PlanError> {
    if t.type_modifiers.len() > 1 {
        return Err(PlanError::TypeMismatch(format!(
            "{sql_name} accepts at most one dimension modifier"
        )));
    }
    let dims = t.type_modifiers.first().copied();
    if matches!(dims, Some(0)) {
        return Err(PlanError::TypeMismatch(format!(
            "{sql_name} dimension must be at least 1"
        )));
    }
    if matches!(dims, Some(n) if n > MAX_VECTOR_DIMS) {
        return Err(PlanError::TypeMismatch(format!(
            "{sql_name} dimension must be at most {MAX_VECTOR_DIMS}"
        )));
    }
    Ok(build(dims))
}

/// Determine whether a column is nullable from its constraint list.
///
/// Returns `true` (nullable) when no `NOT NULL` or `PRIMARY KEY`
/// constraint is present. `PRIMARY KEY` implies `NOT NULL`. Other
/// constraint kinds (DEFAULT, UNIQUE, CHECK, REFERENCES) return
/// [`PlanError::NotSupported`].
fn resolve_column_nullability(constraints: &[ColumnConstraint]) -> Result<bool, PlanError> {
    let mut nullable = true;
    for c in constraints {
        match c {
            ColumnConstraint::NotNull { .. } | ColumnConstraint::PrimaryKey { .. } => {
                nullable = false;
            }
            ColumnConstraint::Null { .. } => nullable = true,
            ColumnConstraint::Default { .. }
            | ColumnConstraint::Unique { .. }
            | ColumnConstraint::Check { .. }
            | ColumnConstraint::References { .. }
            | ColumnConstraint::GeneratedIdentity { .. }
            | ColumnConstraint::GeneratedStored { .. } => {}
        }
    }
    Ok(nullable)
}

fn resolve_column_type(
    table_name: &str,
    column_name: &str,
    t: &TypeName,
    catalog: &dyn Catalog,
) -> Result<(DataType, Option<String>), PlanError> {
    if !t.type_modifiers.is_empty() {
        match t.name.value.as_str() {
            "serial" | "serial4" | "bigserial" | "serial8" | "smallserial" | "serial2" => {
                return Err(PlanError::NotSupported(
                    "CREATE TABLE: SERIAL type modifiers",
                ));
            }
            _ => {}
        }
    }
    let dtype = match t.name.value.as_str() {
        "serial" | "serial4" => DataType::Int32,
        "bigserial" | "serial8" => DataType::Int64,
        "smallserial" | "serial2" => DataType::Int16,
        _ => return resolve_type_name_with_catalog(t, catalog).map(|dtype| (dtype, None)),
    };
    Ok((
        dtype,
        Some(format!(
            "{}_{}_seq",
            table_name.to_ascii_lowercase(),
            column_name.to_ascii_lowercase()
        )),
    ))
}

pub(super) fn bind_create_sequence(s: &CreateSequenceStmt) -> Result<LogicalPlan, PlanError> {
    let sequence_name = object_name_simple(&s.name);
    let namespace = object_name_namespace(&s.name);
    let options = bind_sequence_options(&s.options)?;
    Ok(LogicalPlan::CreateSequence {
        sequence_name,
        namespace,
        options,
        if_not_exists: s.if_not_exists,
        schema: Schema::empty(),
    })
}

pub(super) fn bind_alter_sequence(s: &AlterSequenceStmt) -> Result<LogicalPlan, PlanError> {
    let sequence_name = object_name_simple(&s.name);
    let options = bind_sequence_change(&s.options)?;
    Ok(LogicalPlan::AlterSequence {
        sequence_name,
        options,
        schema: Schema::empty(),
    })
}

pub(super) fn bind_drop_sequence(s: &DropSequenceStmt) -> Result<LogicalPlan, PlanError> {
    let sequences = s.names.iter().map(object_name_simple).collect();
    Ok(LogicalPlan::DropSequence {
        sequences,
        if_exists: s.if_exists,
        cascade: s.cascade,
        schema: Schema::empty(),
    })
}

pub(super) fn bind_create_role(s: &CreateRoleStmt) -> Result<LogicalPlan, PlanError> {
    Ok(LogicalPlan::CreateRole {
        kind: bind_role_kind(s.kind),
        role_name: s.name.value.to_ascii_lowercase(),
        options: bind_role_options(&s.options),
        if_not_exists: s.if_not_exists,
        schema: Schema::empty(),
    })
}

pub(super) fn bind_alter_role(s: &AlterRoleStmt) -> Result<LogicalPlan, PlanError> {
    Ok(LogicalPlan::AlterRole {
        kind: bind_role_kind(s.kind),
        role_name: s.name.value.to_ascii_lowercase(),
        options: bind_role_options(&s.options),
        schema: Schema::empty(),
    })
}

pub(super) fn bind_drop_role(s: &DropRoleStmt) -> Result<LogicalPlan, PlanError> {
    Ok(LogicalPlan::DropRole {
        kind: bind_role_kind(s.kind),
        roles: s
            .names
            .iter()
            .map(|name| name.value.to_ascii_lowercase())
            .collect(),
        if_exists: s.if_exists,
        cascade: s.cascade,
        schema: Schema::empty(),
    })
}

fn bind_role_kind(kind: AstRoleStmtKind) -> LogicalRoleKind {
    match kind {
        AstRoleStmtKind::Role => LogicalRoleKind::Role,
        AstRoleStmtKind::User => LogicalRoleKind::User,
    }
}

fn bind_role_options(options: &[AstRoleOption]) -> LogicalRoleOptions {
    let mut out = LogicalRoleOptions::default();
    for option in options {
        match option {
            AstRoleOption::Superuser(value) => out.superuser = Some(*value),
            AstRoleOption::Inherit(value) => out.inherit = Some(*value),
            AstRoleOption::CreateRole(value) => out.create_role = Some(*value),
            AstRoleOption::CreateDb(value) => out.create_db = Some(*value),
            AstRoleOption::Login(value) => out.can_login = Some(*value),
            AstRoleOption::Replication(value) => out.replication = Some(*value),
            AstRoleOption::BypassRls(value) => out.bypass_rls = Some(*value),
            AstRoleOption::ConnectionLimit(value) => out.connection_limit = Some(*value),
            AstRoleOption::Password(value) => out.password = Some(value.clone()),
            AstRoleOption::ValidUntil(value) => out.valid_until = Some(value.clone()),
        }
    }
    out
}

pub(super) fn bind_comment(
    s: &CommentStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    let target = match &s.target {
        CommentTarget::Table(name) => {
            let table = object_name_simple(name);
            if catalog.lookup_table(&table).is_none() {
                return Err(PlanError::TableNotFound(table));
            }
            LogicalCommentTarget::Table { table }
        }
        CommentTarget::Index(name) => LogicalCommentTarget::Index {
            index: object_name_simple(name),
        },
        CommentTarget::Column(name) => bind_comment_column_target(name, catalog)?,
    };
    Ok(LogicalPlan::Comment {
        target,
        comment: s.comment.clone(),
        schema: Schema::empty(),
    })
}

fn bind_comment_column_target(
    name: &ObjectName,
    catalog: &dyn Catalog,
) -> Result<LogicalCommentTarget, PlanError> {
    if name.parts.len() < 2 {
        return Err(PlanError::NotSupported(
            "COMMENT ON COLUMN requires table.column",
        ));
    }
    let column = name
        .parts
        .last()
        .map_or_else(String::new, |p| p.value.to_ascii_lowercase());
    let table = name.parts[name.parts.len() - 2].value.to_ascii_lowercase();
    let meta = catalog
        .lookup_table(&table)
        .ok_or_else(|| PlanError::TableNotFound(table.clone()))?;
    let Some(idx) = meta
        .schema
        .fields()
        .iter()
        .position(|f| f.name.eq_ignore_ascii_case(&column))
    else {
        return Err(PlanError::ColumnNotFound(column));
    };
    let attnum = i32::try_from(idx + 1)
        .map_err(|_| PlanError::NotSupported("COMMENT ON COLUMN attnum overflow"))?;
    Ok(LogicalCommentTarget::Column {
        table,
        column,
        attnum,
    })
}

fn bind_sequence_options(options: &[SequenceOption]) -> Result<LogicalSequenceOptions, PlanError> {
    let mut out = LogicalSequenceOptions::default();
    let mut explicit_start = None;
    for option in options {
        match *option {
            SequenceOption::Start(v) => explicit_start = Some(v),
            SequenceOption::Restart(_) => {
                return Err(PlanError::NotSupported(
                    "CREATE SEQUENCE: RESTART is only valid in ALTER SEQUENCE",
                ));
            }
            SequenceOption::Increment(v) => out.increment = v,
            SequenceOption::MinValue(v) => out.min = v,
            SequenceOption::MaxValue(v) => out.max = v,
            SequenceOption::Cache(v) => {
                out.cache = u32::try_from(v).map_err(|_| {
                    PlanError::TypeMismatch("sequence CACHE does not fit u32".to_owned())
                })?;
            }
            SequenceOption::Cycle(v) => out.cycle = v,
        }
    }
    out.start = explicit_start.unwrap_or_else(|| default_sequence_start(out));
    validate_sequence_options(out)?;
    Ok(out)
}

fn bind_sequence_change(options: &[SequenceOption]) -> Result<LogicalSequenceChange, PlanError> {
    let mut out = LogicalSequenceChange::default();
    for option in options {
        match *option {
            SequenceOption::Start(v) => out.start = Some(v),
            SequenceOption::Restart(v) => out.restart = Some(v),
            SequenceOption::Increment(v) => out.increment = Some(v),
            SequenceOption::MinValue(v) => out.min = Some(v),
            SequenceOption::MaxValue(v) => out.max = Some(v),
            SequenceOption::Cache(v) => {
                out.cache = Some(u32::try_from(v).map_err(|_| {
                    PlanError::TypeMismatch("sequence CACHE does not fit u32".to_owned())
                })?);
            }
            SequenceOption::Cycle(v) => out.cycle = Some(v),
        }
    }
    Ok(out)
}

fn validate_sequence_options(options: LogicalSequenceOptions) -> Result<(), PlanError> {
    if options.increment == 0 {
        return Err(PlanError::TypeMismatch(
            "sequence INCREMENT must not be zero".to_owned(),
        ));
    }
    if options.cache == 0 {
        return Err(PlanError::TypeMismatch(
            "sequence CACHE must be greater than zero".to_owned(),
        ));
    }
    let ascending = options.increment > 0;
    let min = options.min.unwrap_or(if ascending { 1 } else { i64::MIN });
    let max = options.max.unwrap_or(if ascending { i64::MAX } else { -1 });
    if min >= max {
        return Err(PlanError::TypeMismatch(
            "sequence MINVALUE must be less than MAXVALUE".to_owned(),
        ));
    }
    if options.start < min || options.start > max {
        return Err(PlanError::TypeMismatch(
            "sequence START is outside MINVALUE/MAXVALUE".to_owned(),
        ));
    }
    Ok(())
}

fn default_sequence_start(options: LogicalSequenceOptions) -> i64 {
    if options.increment > 0 {
        options.min.unwrap_or(1)
    } else {
        options.max.unwrap_or(-1)
    }
}

// ---------------------------------------------------------------------------
// CREATE INDEX
// ---------------------------------------------------------------------------

/// Bind a `CREATE [UNIQUE] INDEX [IF NOT EXISTS] [name] ON table (cols)`.
///
/// Accepted shapes for this wave:
///
/// - bare column-reference keys (`(col1, col2, ...)`) and single
///   expression keys (`(lower(col))`) for B-tree storage.
/// - `USING hash`, `USING gin`, `USING gist`, `USING brin`, and `USING hnsw`
///   are preserved in the logical plan so catalog/runtime metadata can route
///   maintenance to the requested access method.
/// - `INCLUDE` covering columns and `WHERE` partial-index predicates
///   are bound into runtime metadata; they do not change the key
///   encoding.
/// - per-key direction / nulls ordering is parsed but not actionable
///   until [`crate::plan::LogicalPlan`] carries order metadata through.
///
/// The binder synthesises a default index name `"{table}_{c1}_{c2}_..._idx"`
/// when one was not supplied so the executor always has a stable
/// catalog key to write.
pub(super) fn bind_create_index(
    s: &CreateIndexStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    // Resolve the target table.
    let table_name = object_name_simple(&s.table);
    let meta = catalog
        .lookup_table(&table_name)
        .ok_or_else(|| PlanError::TableNotFound(table_name.clone()))?;
    let table_schema = &meta.schema;

    let method = if s.aggregating {
        if s.method.is_some() {
            return Err(PlanError::NotSupported(
                "CREATE AGGREGATING INDEX may not also specify USING",
            ));
        }
        LogicalIndexMethod::Aggregating
    } else {
        match s.method.as_ref().map(|m| m.value.to_ascii_lowercase()) {
            None => LogicalIndexMethod::Btree,
            Some(method) if method == "btree" => LogicalIndexMethod::Btree,
            Some(method) if method == "hash" => LogicalIndexMethod::Hash,
            Some(method) if method == "gin" => LogicalIndexMethod::Gin,
            Some(method) if method == "gist" => LogicalIndexMethod::Gist,
            Some(method) if method == "brin" => LogicalIndexMethod::Brin,
            Some(method) if method == "hnsw" => LogicalIndexMethod::Hnsw,
            Some(method) if method == "ivfflat" => LogicalIndexMethod::IvfFlat,
            Some(_) => {
                return Err(PlanError::NotSupported(
                    "CREATE INDEX: only btree, hash, gin, gist, brin, hnsw, and ivfflat methods are supported",
                ));
            }
        }
    };

    if s.columns.is_empty() {
        return Err(PlanError::NotSupported("CREATE INDEX: zero key columns"));
    }
    if method == LogicalIndexMethod::Aggregating {
        return bind_create_aggregating_index(s, table_name, table_schema);
    }
    if method == LogicalIndexMethod::Hash && s.columns.len() != 1 {
        return Err(PlanError::NotSupported(
            "CREATE INDEX USING hash: exactly one key is supported in this wave",
        ));
    }
    if method == LogicalIndexMethod::Hash && s.unique {
        return Err(PlanError::NotSupported(
            "CREATE UNIQUE INDEX USING hash: hash indexes do not enforce uniqueness",
        ));
    }
    if matches!(
        method,
        LogicalIndexMethod::Gin | LogicalIndexMethod::Gist | LogicalIndexMethod::Brin
    ) && s.unique
    {
        return Err(PlanError::NotSupported(
            "CREATE UNIQUE INDEX: gin, gist, and brin indexes do not enforce uniqueness",
        ));
    }
    if matches!(
        method,
        LogicalIndexMethod::Hnsw | LogicalIndexMethod::IvfFlat
    ) && s.unique
    {
        return Err(PlanError::NotSupported(
            "CREATE UNIQUE INDEX USING vector ANN: hnsw and ivfflat indexes do not enforce uniqueness",
        ));
    }
    let mut col_indices: Vec<usize> = Vec::with_capacity(s.columns.len());
    let mut col_names: Vec<String> = Vec::with_capacity(s.columns.len());
    let mut key_exprs: Vec<ScalarExpr> = Vec::with_capacity(s.columns.len());
    let mut opclasses: Vec<Option<String>> = Vec::with_capacity(s.columns.len());
    let mut saw_expression_key = false;
    for key in &s.columns {
        let mut scope = ScopeStack::new();
        let bound = bind_expr(&key.expr, table_schema, catalog, &mut scope)?;
        opclasses.push(
            key.opclass
                .as_ref()
                .map(|ident| ident.value.to_ascii_lowercase()),
        );
        match &bound {
            ScalarExpr::Column { name, index, .. } => {
                col_indices.push(*index);
                col_names.push(name.to_ascii_lowercase());
            }
            _ => {
                saw_expression_key = true;
                col_names.push(index_expr_name_part(&bound));
            }
        }
        key_exprs.push(bound);
    }
    if saw_expression_key {
        if s.columns.len() != 1 {
            return Err(PlanError::NotSupported(
                "CREATE INDEX: expression indexes support exactly one key in this wave",
            ));
        }
        col_indices.clear();
    }

    if matches!(
        method,
        LogicalIndexMethod::Hnsw | LogicalIndexMethod::IvfFlat
    ) {
        if s.columns.len() != 1 || col_indices.len() != 1 {
            return Err(PlanError::NotSupported(
                "CREATE INDEX USING vector ANN: exactly one vector column key is supported",
            ));
        }
        let field = table_schema
            .field(col_indices[0])
            .ok_or_else(|| PlanError::ColumnNotFound(format!("column index {}", col_indices[0])))?;
        if !matches!(
            field.data_type,
            DataType::Vector { dims: Some(_) } | DataType::HalfVec { dims: Some(_) }
        ) {
            return Err(PlanError::TypeMismatch(format!(
                "CREATE INDEX USING vector ANN requires a vector(n) or halfvec(n) column, got {}",
                field.data_type
            )));
        }
        if let Some(opclass) = opclasses.first().and_then(Option::as_ref)
            && !matches!(
                opclass.as_str(),
                "vector_l2_ops" | "vector_cosine_ops" | "vector_ip_ops" | "vector_l1_ops"
            )
        {
            return Err(PlanError::NotSupported(
                "CREATE INDEX USING vector ANN: supported vector opclasses are vector_l2_ops, vector_cosine_ops, vector_ip_ops, vector_l1_ops",
            ));
        }
        if !s.include.is_empty() {
            return Err(PlanError::NotSupported(
                "CREATE INDEX USING vector ANN: INCLUDE columns are not supported in this wave",
            ));
        }
        if s.r#where.is_some() {
            return Err(PlanError::NotSupported(
                "CREATE INDEX USING vector ANN: partial indexes are not supported in this wave",
            ));
        }
    }

    let index_options = s
        .options
        .iter()
        .map(|option| {
            let name = option.name.value.to_ascii_lowercase();
            let value = index_option_value_to_string(&option.value)?;
            Ok(LogicalIndexOption { name, value })
        })
        .collect::<Result<Vec<_>, PlanError>>()?;
    if !matches!(
        method,
        LogicalIndexMethod::Hnsw | LogicalIndexMethod::IvfFlat
    ) && !index_options.is_empty()
    {
        return Err(PlanError::NotSupported(
            "CREATE INDEX WITH options are supported only for hnsw and ivfflat in this wave",
        ));
    }
    if method == LogicalIndexMethod::Hnsw {
        for option in &index_options {
            if option.name != "payload" {
                return Err(PlanError::NotSupported(
                    "CREATE INDEX USING hnsw supports only the payload option",
                ));
            }
            validate_ann_payload_option(&option.value)?;
        }
    }
    if method == LogicalIndexMethod::IvfFlat {
        for option in &index_options {
            if !matches!(option.name.as_str(), "lists" | "probes" | "payload") {
                return Err(PlanError::NotSupported(
                    "CREATE INDEX USING ivfflat supports only lists, probes, and payload options",
                ));
            }
            if option.name == "payload" {
                validate_ann_payload_option(&option.value)?;
            }
        }
    }

    let mut include_columns = Vec::with_capacity(s.include.len());
    for ident in &s.include {
        let folded = ident.value.to_ascii_lowercase();
        let (idx, _) = table_schema
            .find(&folded)
            .ok_or_else(|| PlanError::ColumnNotFound(ident.value.clone()))?;
        include_columns.push(idx);
    }

    let predicate = if let Some(pred_ast) = &s.r#where {
        let mut scope = ScopeStack::new();
        let pred = bind_expr(pred_ast, table_schema, catalog, &mut scope)?;
        let pred_ty = pred.data_type();
        if pred_ty != DataType::Bool {
            return Err(PlanError::TypeMismatch(format!(
                "CREATE INDEX WHERE predicate must be boolean, got {pred_ty}"
            )));
        }
        Some(pred)
    } else {
        None
    };

    let index_name = s.name.as_ref().map_or_else(
        || synthesise_index_name(&table_name, &col_names),
        |ident| ident.value.to_ascii_lowercase(),
    );

    Ok(LogicalPlan::CreateIndex {
        index_name,
        table_name,
        columns: col_indices,
        key_exprs,
        opclasses,
        index_options,
        include_columns,
        predicate,
        method,
        aggregating: None,
        unique: s.unique,
        concurrently: s.concurrently,
        if_not_exists: s.if_not_exists,
        schema: Schema::empty(),
    })
}

fn bind_create_aggregating_index(
    s: &CreateIndexStmt,
    table_name: String,
    table_schema: &Schema,
) -> Result<LogicalPlan, PlanError> {
    if s.unique {
        return Err(PlanError::NotSupported(
            "CREATE UNIQUE AGGREGATING INDEX is not supported",
        ));
    }
    if s.concurrently {
        return Err(PlanError::NotSupported(
            "CREATE AGGREGATING INDEX CONCURRENTLY is not supported",
        ));
    }
    if !s.include.is_empty() {
        return Err(PlanError::NotSupported(
            "CREATE AGGREGATING INDEX does not support INCLUDE",
        ));
    }
    if s.r#where.is_some() {
        return Err(PlanError::NotSupported(
            "CREATE AGGREGATING INDEX does not support partial predicates in this wave",
        ));
    }
    if !s.options.is_empty() {
        return Err(PlanError::NotSupported(
            "CREATE AGGREGATING INDEX does not support WITH options in this wave",
        ));
    }

    let mut group_columns = Vec::new();
    let mut group_exprs = Vec::new();
    let mut col_names = Vec::new();
    let mut aggregates = Vec::new();
    let mut saw_aggregate = false;

    for key in &s.columns {
        if key.opclass.is_some() {
            return Err(PlanError::NotSupported(
                "CREATE AGGREGATING INDEX does not support operator classes",
            ));
        }
        match &key.expr {
            Expr::Call { .. } => {
                saw_aggregate = true;
                let aggregate = bind_aggregating_index_call(&key.expr, table_schema)?;
                col_names.push(aggregate.output_name.clone());
                aggregates.push(aggregate);
            }
            _ => {
                if saw_aggregate {
                    return Err(PlanError::NotSupported(
                        "CREATE AGGREGATING INDEX group columns must precede aggregates",
                    ));
                }
                let mut scope = ScopeStack::new();
                let bound = bind_expr(&key.expr, table_schema, &NoopCatalog, &mut scope)?;
                let ScalarExpr::Column { name, index, .. } = bound else {
                    return Err(PlanError::NotSupported(
                        "CREATE AGGREGATING INDEX group keys must be bare columns",
                    ));
                };
                group_columns.push(index);
                group_exprs.push(ScalarExpr::Column {
                    name: name.clone(),
                    index,
                    data_type: table_schema.field_at(index).data_type.clone(),
                });
                col_names.push(name.to_ascii_lowercase());
            }
        }
    }

    if group_columns.is_empty() || aggregates.is_empty() {
        return Err(PlanError::NotSupported(
            "CREATE AGGREGATING INDEX requires at least one group column and one aggregate",
        ));
    }

    let index_name = s.name.as_ref().map_or_else(
        || synthesise_index_name(&table_name, &col_names),
        |ident| ident.value.to_ascii_lowercase(),
    );

    Ok(LogicalPlan::CreateIndex {
        index_name,
        table_name,
        columns: group_columns.clone(),
        key_exprs: group_exprs,
        opclasses: vec![None; group_columns.len()],
        index_options: Vec::new(),
        include_columns: Vec::new(),
        predicate: None,
        method: LogicalIndexMethod::Aggregating,
        aggregating: Some(LogicalAggregatingIndex {
            group_columns,
            aggregates,
        }),
        unique: false,
        concurrently: false,
        if_not_exists: s.if_not_exists,
        schema: Schema::empty(),
    })
}

struct NoopCatalog;

impl Catalog for NoopCatalog {
    fn lookup_table(&self, _name: &str) -> Option<TableMeta> {
        None
    }
}

fn bind_aggregating_index_call(
    expr: &Expr,
    table_schema: &Schema,
) -> Result<LogicalAggregatingIndexExpr, PlanError> {
    let Expr::Call {
        name,
        args,
        distinct,
        over,
        ..
    } = expr
    else {
        return Err(PlanError::NotSupported(
            "CREATE AGGREGATING INDEX aggregate key must be a function call",
        ));
    };
    if *distinct || over.is_some() {
        return Err(PlanError::NotSupported(
            "CREATE AGGREGATING INDEX does not support DISTINCT or window aggregates",
        ));
    }
    let func_name = name
        .parts
        .last()
        .map_or("", |part| part.value.as_str())
        .to_ascii_lowercase();
    let is_star_arg = args.len() == 1
        && matches!(&args[0], Expr::Column { name }
            if name.parts.len() == 1 && name.parts[0].value == "*");
    match func_name.as_str() {
        "count" if args.is_empty() || is_star_arg => Ok(LogicalAggregatingIndexExpr {
            func: AggregateFunc::CountStar,
            arg_column: None,
            output_name: "count".to_owned(),
            data_type: DataType::Int64,
        }),
        "sum" if args.len() == 1 => {
            let mut scope = ScopeStack::new();
            let bound = bind_expr(&args[0], table_schema, &NoopCatalog, &mut scope)?;
            let ScalarExpr::Column {
                name,
                index,
                data_type,
            } = bound
            else {
                return Err(PlanError::NotSupported(
                    "CREATE AGGREGATING INDEX sum() argument must be a bare column",
                ));
            };
            if !data_type.is_numeric() {
                return Err(PlanError::TypeMismatch(format!(
                    "CREATE AGGREGATING INDEX sum({name}) requires numeric input, got {data_type}"
                )));
            }
            let out_ty = match data_type {
                DataType::Float32 | DataType::Float64 => DataType::Float64,
                _ => DataType::Int64,
            };
            Ok(LogicalAggregatingIndexExpr {
                func: AggregateFunc::Sum,
                arg_column: Some(index),
                output_name: format!("sum({})", name.to_ascii_lowercase()),
                data_type: out_ty,
            })
        }
        _ => Err(PlanError::NotSupported(
            "CREATE AGGREGATING INDEX supports sum(column) and count(*) in this wave",
        )),
    }
}

fn index_expr_name_part(expr: &ScalarExpr) -> String {
    expr.to_string()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_owned()
}

fn index_option_value_to_string(expr: &Expr) -> Result<String, PlanError> {
    match expr {
        Expr::Literal(Literal::Integer { text, .. })
        | Expr::Literal(Literal::Float { text, .. }) => Ok(text.clone()),
        Expr::Literal(Literal::String { value, .. })
        | Expr::Literal(Literal::Typed { value, .. }) => Ok(value.clone()),
        Expr::Literal(Literal::Bool { value, .. }) => {
            Ok(if *value { "true" } else { "false" }.to_owned())
        }
        Expr::Column { name } if name.parts.len() == 1 => Ok(name.parts[0].value.clone()),
        _ => Err(PlanError::NotSupported("CREATE INDEX WITH option value")),
    }
}

fn validate_ann_payload_option(value: &str) -> Result<(), PlanError> {
    match value.to_ascii_lowercase().as_str() {
        "f32" | "float32" | "bf16" | "bfloat16" | "int8" | "i8" => Ok(()),
        _ => Err(PlanError::NotSupported(
            "CREATE INDEX USING vector ANN payload supports f32, bf16, and int8",
        )),
    }
}

/// Build a stable default index name when the user did not supply one:
/// `{table}_{col1}_{col2}_..._idx`. Matches PostgreSQL's
/// `ChooseIndexName` for the common single-column / multi-column case
/// closely enough that EXPLAIN-style output stays familiar.
fn synthesise_index_name(table: &str, columns: &[String]) -> String {
    let mut s = String::with_capacity(table.len() + 16);
    s.push_str(table);
    for c in columns {
        s.push('_');
        s.push_str(c);
    }
    s.push_str("_idx");
    s
}

// ---------------------------------------------------------------------------
// DROP TABLE
// ---------------------------------------------------------------------------

/// Bind a `DROP TABLE [IF EXISTS] name [, ...] [CASCADE|RESTRICT]`.
///
/// Each name is folded to lowercase and resolved against the catalog.
/// Without `IF EXISTS`, a missing relation is rejected with
/// [`PlanError::TableNotFound`]; with `IF EXISTS`, missing relations
/// are silently dropped from the resulting plan so the executor never
/// has to re-check the catalog.
pub(super) fn bind_drop_table(
    s: &DropTableStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    let mut tables: Vec<String> = Vec::with_capacity(s.names.len());
    for obj in &s.names {
        let name = object_name_simple(obj);
        if catalog.lookup_table(&name).is_some() {
            tables.push(name);
        } else if !s.if_exists {
            return Err(PlanError::TableNotFound(name));
        }
    }
    Ok(LogicalPlan::DropTable {
        tables,
        if_exists: s.if_exists,
        cascade: s.cascade,
        schema: Schema::empty(),
    })
}

/// Bind `DROP INDEX [IF EXISTS] name [, ...]`.
pub(super) fn bind_drop_index(
    s: &DropIndexStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    let mut indexes = Vec::with_capacity(s.names.len());
    for obj in &s.names {
        let name = object_name_simple(obj);
        if catalog.lookup_index(&name) {
            indexes.push(name);
        } else if !s.if_exists {
            return Err(PlanError::IndexNotFound(name));
        }
    }
    Ok(LogicalPlan::DropIndex {
        indexes,
        if_exists: s.if_exists,
        cascade: s.cascade,
        schema: Schema::empty(),
    })
}

// ---------------------------------------------------------------------------
// ALTER TABLE
// ---------------------------------------------------------------------------

/// Bind an `ALTER TABLE` statement.
///
/// This wave supports `ADD COLUMN`, `DROP COLUMN`, renames, storage
/// options, row security enablement, and PostgreSQL migration-tool
/// `ADD CONSTRAINT ... PRIMARY KEY/UNIQUE`. Other constraint kinds
/// are rejected with [`PlanError::NotSupported`] so the dispatcher
/// contract stays honest.
///
/// For `ADD COLUMN` the binder resolves the column's data type and
/// nullability against the same v0.5 column-constraint matrix used by
/// `CREATE TABLE` and rejects duplicate column names up front
/// ([`PlanError::DuplicateColumn`]).
pub(super) fn bind_alter_table(
    s: &AlterTableStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    let table_name = object_name_simple(&s.name);
    let meta = catalog
        .lookup_table(&table_name)
        .ok_or_else(|| PlanError::TableNotFound(table_name.clone()))?;
    let table_schema = &meta.schema;

    let action = match &s.action {
        AlterTableAction::AddColumn { column, .. } => {
            let new_name = column.name.value.clone();
            if table_schema.find(&new_name.to_ascii_lowercase()).is_some() {
                return Err(PlanError::DuplicateColumn(new_name));
            }
            let dtype = resolve_type_name_with_catalog(&column.data_type, catalog)?;
            let nullable = resolve_column_nullability(&column.constraints)?;
            let field = if nullable {
                Field::nullable(new_name, dtype)
            } else {
                Field::required(new_name, dtype)
            };
            LogicalAlterTableAction::AddColumn { column: field }
        }
        AlterTableAction::DropColumn { name, .. } => {
            let raw = name.value.to_ascii_lowercase();
            let (idx, _) = table_schema
                .find(&raw)
                .ok_or_else(|| PlanError::ColumnNotFound(name.value.clone()))?;
            if table_schema.len() == 1 {
                return Err(PlanError::NotSupported(
                    "ALTER TABLE: cannot drop the last column of a table",
                ));
            }
            LogicalAlterTableAction::DropColumn {
                column_index: idx,
                column_name: name.value.clone(),
            }
        }
        AlterTableAction::RenameColumn { old, new, .. } => {
            let old_raw = old.value.to_ascii_lowercase();
            let new_raw = new.value.to_ascii_lowercase();
            let (idx, _) = table_schema
                .find(&old_raw)
                .ok_or_else(|| PlanError::ColumnNotFound(old.value.clone()))?;
            if table_schema.find(&new_raw).is_some() {
                return Err(PlanError::DuplicateColumn(new.value.clone()));
            }
            LogicalAlterTableAction::RenameColumn {
                column_index: idx,
                old_name: old.value.clone(),
                new_name: new.value.clone(),
            }
        }
        AlterTableAction::RenameTable { new_name, .. } => {
            let new = new_name.value.clone();
            if catalog.lookup_table(&new.to_ascii_lowercase()).is_some() {
                return Err(PlanError::DuplicateTable(new));
            }
            LogicalAlterTableAction::RenameTable { new_name: new }
        }
        AlterTableAction::EnableRowLevelSecurity { .. } => {
            LogicalAlterTableAction::EnableRowLevelSecurity
        }
        AlterTableAction::SetOptions { options, .. } => {
            let options = options
                .iter()
                .map(|option| {
                    let name = option.name.value.to_ascii_lowercase();
                    validate_table_option_name(&name)?;
                    let value = index_option_value_to_string(&option.value)?;
                    Ok(LogicalTableOption { name, value })
                })
                .collect::<Result<Vec<_>, PlanError>>()?;
            LogicalAlterTableAction::SetOptions { options }
        }
        AlterTableAction::AddConstraint { constraint, .. } => {
            LogicalAlterTableAction::AddUniqueConstraint {
                constraint: bind_alter_add_unique_constraint(
                    &table_name,
                    table_schema,
                    constraint,
                )?,
            }
        }
        AlterTableAction::DropConstraint { .. } => {
            return Err(PlanError::NotSupported(
                "ALTER TABLE: DROP CONSTRAINT not yet supported",
            ));
        }
    };

    Ok(LogicalPlan::AlterTable {
        table_name,
        action,
        schema: Schema::empty(),
    })
}

fn bind_alter_add_unique_constraint(
    table_name: &str,
    table_schema: &Schema,
    constraint: &TableConstraint,
) -> Result<LogicalUniqueConstraint, PlanError> {
    let (name, raw_columns, primary_key) = match constraint {
        TableConstraint::PrimaryKey { name, columns, .. } => (
            named_or(name.as_ref(), || {
                unique_name(
                    table_name,
                    &columns
                        .iter()
                        .map(|column| column.value.to_ascii_lowercase())
                        .collect::<Vec<_>>(),
                    true,
                )
            }),
            columns,
            true,
        ),
        TableConstraint::Unique { name, columns, .. } => (
            named_or(name.as_ref(), || {
                unique_name(
                    table_name,
                    &columns
                        .iter()
                        .map(|column| column.value.to_ascii_lowercase())
                        .collect::<Vec<_>>(),
                    false,
                )
            }),
            columns,
            false,
        ),
        TableConstraint::Check { .. }
        | TableConstraint::ForeignKey { .. }
        | TableConstraint::Exclude { .. } => {
            return Err(PlanError::NotSupported(
                "ALTER TABLE: ADD CONSTRAINT supports only PRIMARY KEY and UNIQUE",
            ));
        }
    };
    if raw_columns.is_empty() {
        return Err(PlanError::NotSupported(
            "ALTER TABLE: empty unique constraints are not supported",
        ));
    }
    let mut columns = Vec::with_capacity(raw_columns.len());
    for column in raw_columns {
        let raw = column.value.to_ascii_lowercase();
        let (idx, field) = table_schema
            .find(&raw)
            .ok_or_else(|| PlanError::ColumnNotFound(column.value.clone()))?;
        if primary_key && field.nullable {
            return Err(PlanError::NotSupported(
                "ALTER TABLE: ADD PRIMARY KEY currently requires NOT NULL columns",
            ));
        }
        columns.push(idx);
    }
    Ok(LogicalUniqueConstraint {
        name,
        columns,
        primary_key,
    })
}

fn validate_table_option_name(name: &str) -> Result<(), PlanError> {
    match name {
        "autovacuum_vacuum_threshold"
        | "autovacuum_vacuum_scale_factor"
        | "autovacuum_analyze_threshold"
        | "autovacuum_analyze_scale_factor" => Ok(()),
        _ => Err(PlanError::NotSupported(
            "ALTER TABLE SET supports autovacuum reloptions only",
        )),
    }
}

// ---------------------------------------------------------------------------
// TRUNCATE
// ---------------------------------------------------------------------------

/// Bind a `TRUNCATE` statement.
///
/// Validates every table name against the catalog; returns
/// [`PlanError::TableNotFound`] on the first missing name.
pub(super) fn bind_truncate(
    s: &TruncateStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    let mut table_names: Vec<String> = Vec::with_capacity(s.tables.len());
    for obj in &s.tables {
        let name = object_name_simple(obj);
        catalog
            .lookup_table(&name)
            .ok_or_else(|| PlanError::TableNotFound(name.clone()))?;
        table_names.push(name);
    }
    Ok(LogicalPlan::Truncate {
        tables: table_names,
        restart_identity: s.restart_identity,
        cascade: s.cascade,
        schema: Schema::empty(),
    })
}

// ---------------------------------------------------------------------------
// COPY
// ---------------------------------------------------------------------------

/// Bind a `COPY` statement.
///
/// Validates the target table, resolves every column name in the optional
/// `(col_list)` against the table's schema, and folds the parsed
/// `WITH (…)` options into the format-appropriate defaults (`\t` delimiter
/// + `\N` NULL marker for TEXT; `,` delimiter + empty-string NULL marker
/// for CSV). The produced [`LogicalPlan::Copy`] carries the row-shape
/// schema the server's session dispatcher needs to encode `CopyOutResponse`
/// / `CopyInResponse` frames.
pub(super) fn bind_copy(s: &CopyStmt, catalog: &dyn Catalog) -> Result<LogicalPlan, PlanError> {
    if let Some(query) = &s.query {
        if !s.columns.is_empty() {
            return Err(PlanError::NotSupported(
                "COPY query target cannot specify a column list",
            ));
        }
        let mut scope = ScopeStack::new();
        let input = bind_select(query, catalog, &mut scope)?;
        let schema = input.schema().clone();
        let direction = match s.direction {
            AstCopyDirection::From => {
                return Err(PlanError::NotSupported(
                    "COPY (SELECT ...) supports TO only",
                ));
            }
            AstCopyDirection::To => CopyDirection::To,
        };
        let source = match &s.source {
            AstCopySource::Stdout => CopySource::Stdout,
            AstCopySource::File(path) => CopySource::File(path.clone()),
            AstCopySource::Stdin => {
                return Err(PlanError::NotSupported(
                    "COPY query target cannot use STDIN",
                ));
            }
        };
        let format = match s.format {
            AstCopyFormat::Text => CopyFormat::Text,
            AstCopyFormat::Csv => CopyFormat::Csv,
            AstCopyFormat::Binary => CopyFormat::Binary,
            AstCopyFormat::Parquet => CopyFormat::Parquet,
        };
        let (mut delimiter, mut null_str) = match format {
            CopyFormat::Text | CopyFormat::Binary | CopyFormat::Parquet => {
                ('\t', String::from(r"\N"))
            }
            CopyFormat::Csv => (',', String::new()),
        };
        let mut header = false;
        let mut auto_detect = false;
        let mut ignore_errors = false;
        let mut max_errors = 0_u64;
        let mut reject_table = None;
        for opt in &s.options {
            match opt {
                CopyOption::Format(_) => {}
                CopyOption::Delimiter(c) => delimiter = *c,
                CopyOption::Header(v) => header = *v,
                CopyOption::AutoDetect(v) => auto_detect = *v,
                CopyOption::IgnoreErrors(v) => ignore_errors = *v,
                CopyOption::MaxErrors(v) => max_errors = *v,
                CopyOption::RejectTable(v) => reject_table = Some(v.to_ascii_lowercase()),
                CopyOption::Null(v) => null_str.clone_from(v),
            }
        }
        return Ok(LogicalPlan::Copy {
            relation: None,
            input: Some(Box::new(input)),
            columns: Vec::new(),
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
        });
    }

    let table_name = s.table.as_ref().ok_or(PlanError::NotSupported(
        "COPY requires table or query target",
    ))?;
    let relation = object_name_simple(table_name);
    let table_meta = catalog
        .lookup_table(&relation)
        .ok_or_else(|| PlanError::TableNotFound(relation.clone()))?;

    let columns: Vec<usize> = if s.columns.is_empty() {
        Vec::new()
    } else {
        let mut indices = Vec::with_capacity(s.columns.len());
        for ident in &s.columns {
            let folded = ident.value.to_ascii_lowercase();
            let idx = table_meta
                .schema
                .fields()
                .iter()
                .position(|f| f.name.eq_ignore_ascii_case(&folded))
                .ok_or_else(|| PlanError::ColumnNotFound(ident.value.clone()))?;
            indices.push(idx);
        }
        indices
    };

    let stream_schema = if columns.is_empty() {
        table_meta.schema.clone()
    } else {
        let fields: Vec<Field> = columns
            .iter()
            .map(|&i| table_meta.schema.fields()[i].clone())
            .collect();
        Schema::new(fields)
            .map_err(|e| PlanError::TypeMismatch(format!("COPY column projection: {e}")))?
    };

    let direction = match s.direction {
        AstCopyDirection::From => CopyDirection::From,
        AstCopyDirection::To => CopyDirection::To,
    };
    let source = match &s.source {
        AstCopySource::Stdin => CopySource::Stdin,
        AstCopySource::Stdout => CopySource::Stdout,
        AstCopySource::File(path) => CopySource::File(path.clone()),
    };
    let mut format = match s.format {
        AstCopyFormat::Text => CopyFormat::Text,
        AstCopyFormat::Csv => CopyFormat::Csv,
        AstCopyFormat::Binary => CopyFormat::Binary,
        AstCopyFormat::Parquet => CopyFormat::Parquet,
    };
    if !copy_has_explicit_format(&s.options) {
        if let AstCopySource::File(path) = &s.source {
            if copy_file_extension_is(path, "parquet") {
                format = CopyFormat::Parquet;
            }
        }
    }

    let (mut delimiter, mut null_str) = match format {
        CopyFormat::Text | CopyFormat::Binary | CopyFormat::Parquet => ('\t', String::from(r"\N")),
        CopyFormat::Csv => (',', String::new()),
    };
    let mut header = false;
    let mut auto_detect = false;
    let mut ignore_errors = false;
    let mut max_errors = 0_u64;
    let mut reject_table = None;
    for opt in &s.options {
        match opt {
            CopyOption::Format(_) => { /* applied above */ }
            CopyOption::Delimiter(c) => delimiter = *c,
            CopyOption::Header(v) => header = *v,
            CopyOption::AutoDetect(v) => auto_detect = *v,
            CopyOption::IgnoreErrors(v) => ignore_errors = *v,
            CopyOption::MaxErrors(v) => max_errors = *v,
            CopyOption::RejectTable(v) => reject_table = Some(v.to_ascii_lowercase()),
            CopyOption::Null(v) => null_str.clone_from(v),
        }
    }

    Ok(LogicalPlan::Copy {
        relation: Some(relation),
        input: None,
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
        schema: stream_schema,
    })
}

fn copy_has_explicit_format(options: &[CopyOption]) -> bool {
    options
        .iter()
        .any(|option| matches!(option, CopyOption::Format(_)))
}

fn copy_file_extension_is(path: &str, extension: &str) -> bool {
    std::path::Path::new(path)
        .extension()
        .and_then(std::ffi::OsStr::to_str)
        .is_some_and(|ext| ext.eq_ignore_ascii_case(extension))
}
