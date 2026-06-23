//! `ALTER TABLE` and `TRUNCATE` binding.

use ultrasql_core::{DataType, Field, Schema};
use ultrasql_parser::ast::{
    AlterTableAction, AlterTableStmt, ColumnConstraint, TableConstraint, TruncateStmt,
};

use super::super::{
    Catalog, LogicalAlterTableAction, LogicalPlan, PlanError, ScopeStack, bind_expr,
    lookup_table_reference, object_name_simple,
};
use super::shared::{
    coerce_default_expr_to_type, column_default, index_option_value_to_string, is_default_safe,
    named_or, resolve_column_nullability, unique_name,
};
use super::types::resolve_type_name_with_catalog;
use crate::plan::{LogicalCheckConstraint, LogicalTableOption, LogicalUniqueConstraint};

// ---------------------------------------------------------------------------
// ALTER TABLE
// ---------------------------------------------------------------------------

/// Bind an `ALTER TABLE` statement.
///
/// This wave supports `ADD COLUMN`, `DROP COLUMN`, renames, storage
/// options, row security enablement, and the PostgreSQL migration-tool
/// constraint lifecycle: `ADD CONSTRAINT ... PRIMARY KEY/UNIQUE/CHECK`
/// and `DROP CONSTRAINT [IF EXISTS] ... [CASCADE|RESTRICT]`. `ADD
/// CONSTRAINT ... FOREIGN KEY` and `EXCLUDE` are still rejected with
/// [`PlanError::NotSupported`] so the dispatcher contract stays honest.
///
/// For `ADD COLUMN` the binder resolves the column's data type and
/// nullability, rejects unsupported constraints before they can be
/// silently discarded, and rejects duplicate column names up front
/// ([`PlanError::DuplicateColumn`]).
pub(in crate::binder) fn bind_alter_table(
    s: &AlterTableStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    let raw_table_name = object_name_simple(&s.name);
    let resolved = lookup_table_reference(catalog, &s.name)?;
    let table_name = resolved.plan_name;
    let meta = resolved.meta;
    let table_schema = &meta.schema;

    let action = match &s.action {
        AlterTableAction::AddColumn { column, .. } => {
            let new_name = column.name.value.clone();
            if table_schema.find(&new_name.to_ascii_lowercase()).is_some() {
                return Err(PlanError::DuplicateColumn(new_name));
            }
            reject_unsupported_alter_add_column_constraints(&column.constraints)?;
            let dtype = resolve_type_name_with_catalog(&column.data_type, catalog)?;
            let nullable = resolve_column_nullability(&column.constraints)?;
            let field = if nullable {
                Field::nullable(new_name, dtype.clone())
            } else {
                Field::required(new_name, dtype.clone())
            };
            let default = column_default(&column.constraints)?;
            let default = if let Some(expr) = default {
                let mut scope = ScopeStack::new();
                let mut bound = bind_expr(expr, &Schema::empty(), catalog, &mut scope)?;
                if !is_default_safe(&bound) {
                    return Err(PlanError::NotSupported(
                        "ALTER TABLE ADD COLUMN: DEFAULT may not refer to rows, parameters, or subqueries",
                    ));
                }
                coerce_default_expr_to_type(&mut bound, &dtype);
                let actual = bound.data_type();
                if actual != dtype.clone() && actual != DataType::Null {
                    return Err(PlanError::TypeMismatch(format!(
                        "DEFAULT for column '{}' has type {:?}, expected {:?}",
                        field.name, actual, dtype,
                    )));
                }
                Some(bound)
            } else {
                None
            };
            LogicalAlterTableAction::AddColumn {
                column: field,
                default,
            }
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
            if catalog
                .lookup_table_in_schema(&meta.schema_name, &new)
                .is_some()
            {
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
        AlterTableAction::AddConstraint { constraint, .. } => match constraint {
            TableConstraint::PrimaryKey { .. } | TableConstraint::Unique { .. } => {
                LogicalAlterTableAction::AddUniqueConstraint {
                    constraint: bind_alter_add_unique_constraint(
                        &raw_table_name,
                        table_schema,
                        constraint,
                    )?,
                }
            }
            TableConstraint::Check { .. } => LogicalAlterTableAction::AddCheckConstraint {
                constraint: bind_alter_add_check_constraint(
                    &raw_table_name,
                    table_schema,
                    constraint,
                    catalog,
                )?,
            },
            TableConstraint::ForeignKey { .. } => {
                return Err(PlanError::NotSupported(
                    "ALTER TABLE: ADD CONSTRAINT FOREIGN KEY is not yet supported; \
                     declare the foreign key in CREATE TABLE instead",
                ));
            }
            TableConstraint::Exclude { .. } => {
                return Err(PlanError::NotSupported(
                    "ALTER TABLE: ADD CONSTRAINT supports PRIMARY KEY, UNIQUE, and CHECK",
                ));
            }
        },
        AlterTableAction::DropConstraint {
            name,
            if_exists,
            cascade,
            ..
        } => LogicalAlterTableAction::DropConstraint {
            name: name.value.to_ascii_lowercase(),
            if_exists: *if_exists,
            cascade: *cascade,
        },
        AlterTableAction::AlterColumnSetNotNull { column, .. } => {
            let (idx, _) = resolve_alter_column(table_schema, column)?;
            LogicalAlterTableAction::AlterColumnSetNotNull {
                column_index: idx,
                column_name: column.value.clone(),
            }
        }
        AlterTableAction::AlterColumnDropNotNull { column, .. } => {
            let (idx, _) = resolve_alter_column(table_schema, column)?;
            LogicalAlterTableAction::AlterColumnDropNotNull {
                column_index: idx,
                column_name: column.value.clone(),
            }
        }
        AlterTableAction::AlterColumnSetDefault { column, expr, .. } => {
            let (idx, field) = resolve_alter_column(table_schema, column)?;
            let dtype = field.data_type.clone();
            let mut scope = ScopeStack::new();
            let mut bound = bind_expr(expr, &Schema::empty(), catalog, &mut scope)?;
            if !is_default_safe(&bound) {
                return Err(PlanError::NotSupported(
                    "ALTER TABLE ALTER COLUMN SET DEFAULT: DEFAULT may not refer to rows, parameters, or subqueries",
                ));
            }
            coerce_default_expr_to_type(&mut bound, &dtype);
            let actual = bound.data_type();
            if actual != dtype && actual != DataType::Null {
                return Err(PlanError::TypeMismatch(format!(
                    "DEFAULT for column '{}' has type {actual:?}, expected {dtype:?}",
                    column.value,
                )));
            }
            LogicalAlterTableAction::AlterColumnSetDefault {
                column_index: idx,
                column_name: column.value.clone(),
                default: bound,
            }
        }
        AlterTableAction::AlterColumnDropDefault { column, .. } => {
            let (idx, _) = resolve_alter_column(table_schema, column)?;
            LogicalAlterTableAction::AlterColumnDropDefault {
                column_index: idx,
                column_name: column.value.clone(),
            }
        }
    };

    Ok(LogicalPlan::AlterTable {
        table_name,
        action,
        schema: Schema::empty(),
    })
}

/// Resolve a target column for an `ALTER COLUMN` sub-action, returning
/// its 0-based index and resolved [`Field`].
///
/// Mirrors PostgreSQL: an unknown column raises
/// [`PlanError::ColumnNotFound`] (SQLSTATE `42703`).
fn resolve_alter_column<'s>(
    table_schema: &'s Schema,
    column: &ultrasql_parser::ast::Identifier,
) -> Result<(usize, &'s Field), PlanError> {
    let raw = column.value.to_ascii_lowercase();
    table_schema
        .find(&raw)
        .ok_or_else(|| PlanError::ColumnNotFound(column.value.clone()))
}

fn reject_unsupported_alter_add_column_constraints(
    constraints: &[ColumnConstraint],
) -> Result<(), PlanError> {
    for constraint in constraints {
        match constraint {
            ColumnConstraint::Null { .. }
            | ColumnConstraint::NotNull { .. }
            | ColumnConstraint::Default { .. } => {}
            ColumnConstraint::PrimaryKey { .. }
            | ColumnConstraint::Unique { .. }
            | ColumnConstraint::Check { .. }
            | ColumnConstraint::References { .. }
            | ColumnConstraint::GeneratedIdentity { .. }
            | ColumnConstraint::GeneratedStored { .. } => {
                return Err(PlanError::NotSupported(
                    "ALTER TABLE ADD COLUMN currently supports only NULL, NOT NULL, and DEFAULT column constraints",
                ));
            }
        }
    }
    Ok(())
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
            // The dispatcher in `bind_alter_table` routes these elsewhere;
            // reaching this arm would be a logic error.
            return Err(PlanError::NotSupported(
                "ALTER TABLE: ADD CONSTRAINT supports only PRIMARY KEY and UNIQUE here",
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

fn bind_alter_add_check_constraint(
    table_name: &str,
    table_schema: &Schema,
    constraint: &TableConstraint,
    catalog: &dyn Catalog,
) -> Result<LogicalCheckConstraint, PlanError> {
    let TableConstraint::Check { name, expr, .. } = constraint else {
        return Err(PlanError::NotSupported(
            "ALTER TABLE: ADD CONSTRAINT CHECK binder reached with non-CHECK constraint",
        ));
    };
    // PostgreSQL names an unnamed table-level CHECK `<table>_check`.
    let constraint_name = named_or(name.as_ref(), || format!("{table_name}_check"));
    let mut scope = ScopeStack::new();
    let bound = bind_expr(expr, table_schema, catalog, &mut scope)?;
    let ty = bound.data_type();
    if ty != DataType::Bool && ty != DataType::Null {
        return Err(PlanError::TypeMismatch(format!(
            "CHECK constraint '{constraint_name}' has type {ty:?}, expected Bool",
        )));
    }
    Ok(LogicalCheckConstraint {
        name: constraint_name,
        expr: bound,
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
pub(in crate::binder) fn bind_truncate(
    s: &TruncateStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    let mut table_names: Vec<String> = Vec::with_capacity(s.tables.len());
    for obj in &s.tables {
        let name = object_name_simple(obj);
        let resolved = lookup_table_reference(catalog, obj)
            .map_err(|_| PlanError::TableNotFound(name.clone()))?;
        table_names.push(resolved.plan_name);
    }
    Ok(LogicalPlan::Truncate {
        tables: table_names,
        restart_identity: s.restart_identity,
        cascade: s.cascade,
        schema: Schema::empty(),
    })
}
