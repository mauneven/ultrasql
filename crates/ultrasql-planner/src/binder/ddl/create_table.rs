//! `CREATE TABLE` binding: column/type resolution, constraint
//! collection, and constraint binding (unique, foreign-key, exclusion,
//! time-partition).

use ultrasql_core::{DataType, Field, Schema};
use ultrasql_parser::ast::{
    ColumnConstraint, CreateTableStmt, Expr, Identifier, ObjectName, TableConstraint,
};

use super::super::{
    Catalog, LogicalPlan, PlanError, ScopeStack, bind_expr, lookup_table_reference,
    object_name_simple,
};
use super::sequence::bind_sequence_options;
use super::shared::{
    bind_index_method, bind_referential_action, coerce_default_expr_to_type, column_default,
    expr_references_generated_column, is_default_safe, is_generated_stored_safe, named_or,
    object_name_namespace, resolve_column_nullability, unique_name,
};
use super::types::{bind_column_collation, resolve_column_type};
use crate::plan::{
    LogicalCheckConstraint, LogicalExclusionConstraint, LogicalExclusionElement,
    LogicalForeignKeyConstraint, LogicalIndexMethod, LogicalReferentialAction,
    LogicalSequenceOptions, LogicalTimePartition, LogicalUniqueConstraint,
};

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
    target_object: ObjectName,
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

pub(in crate::binder) fn bind_create_table(
    s: &CreateTableStmt,
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
    if s.columns.is_empty() {
        return Err(PlanError::NotSupported("CREATE TABLE: zero columns"));
    }
    let mut fields: Vec<Field> = Vec::with_capacity(s.columns.len());
    let mut column_collations: Vec<Option<u32>> = Vec::with_capacity(s.columns.len());
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
        let collation = bind_column_collation(&name, &dtype, col.collation.as_ref())?;
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
        column_collations.push(collation);
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
        column_collations,
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
                    target_object: target_table.clone(),
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
                    target_object: target_table.clone(),
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
            super::super::expr_type::display_binary(op),
        ))),
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
        let resolved = lookup_table_reference(catalog, &raw.target_object)?;
        let target_table = resolved.plan_name;
        let target = resolved.meta;
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
            target_table,
            target_columns,
            on_delete: raw.on_delete,
            on_update: raw.on_update,
            deferrable: raw.deferrable,
            initially_deferred: raw.initially_deferred,
        });
    }
    Ok(out)
}
