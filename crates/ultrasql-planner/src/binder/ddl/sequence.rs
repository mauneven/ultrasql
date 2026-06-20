//! Sequence, schema, role, and comment DDL binders.

use ultrasql_core::Schema;
use ultrasql_parser::ast::{
    AlterRoleStmt, AlterSequenceStmt, CommentStmt, CommentTarget, CreateRoleStmt, CreateSchemaStmt,
    CreateSequenceStmt, DropRoleStmt, DropSchemaStmt, DropSequenceStmt, ObjectName,
    RoleOption as AstRoleOption, RoleStmtKind as AstRoleStmtKind, SequenceOption,
};

use super::super::{Catalog, LogicalPlan, PlanError, lookup_table_reference, object_name_simple};
use super::shared::{object_name_explicit_namespace, object_name_namespace};
use crate::plan::{
    LogicalCommentTarget, LogicalRoleKind, LogicalRoleOptions, LogicalSequenceChange,
    LogicalSequenceOptions,
};

pub(in crate::binder) fn bind_create_sequence(
    s: &CreateSequenceStmt,
) -> Result<LogicalPlan, PlanError> {
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

pub(in crate::binder) fn bind_alter_sequence(
    s: &AlterSequenceStmt,
) -> Result<LogicalPlan, PlanError> {
    let sequence_name = object_name_simple(&s.name);
    let namespace = object_name_explicit_namespace(&s.name);
    let options = bind_sequence_change(&s.options)?;
    Ok(LogicalPlan::AlterSequence {
        sequence_name,
        namespace,
        options,
        schema: Schema::empty(),
    })
}

pub(in crate::binder) fn bind_drop_sequence(
    s: &DropSequenceStmt,
) -> Result<LogicalPlan, PlanError> {
    let sequences = s.names.iter().map(object_name_simple).collect();
    let sequence_namespaces = s.names.iter().map(object_name_explicit_namespace).collect();
    Ok(LogicalPlan::DropSequence {
        sequences,
        sequence_namespaces,
        if_exists: s.if_exists,
        cascade: s.cascade,
        schema: Schema::empty(),
    })
}

pub(in crate::binder) fn bind_create_schema(
    s: &CreateSchemaStmt,
) -> Result<LogicalPlan, PlanError> {
    Ok(LogicalPlan::CreateSchema {
        schema_name: s.name.value.to_ascii_lowercase(),
        if_not_exists: s.if_not_exists,
        schema: Schema::empty(),
    })
}

pub(in crate::binder) fn bind_drop_schema(s: &DropSchemaStmt) -> Result<LogicalPlan, PlanError> {
    Ok(LogicalPlan::DropSchema {
        schemas: s
            .names
            .iter()
            .map(|name| name.value.to_ascii_lowercase())
            .collect(),
        if_exists: s.if_exists,
        cascade: s.cascade,
        schema: Schema::empty(),
    })
}

pub(in crate::binder) fn bind_create_role(s: &CreateRoleStmt) -> Result<LogicalPlan, PlanError> {
    Ok(LogicalPlan::CreateRole {
        kind: bind_role_kind(s.kind),
        role_name: s.name.value.to_ascii_lowercase(),
        options: bind_role_options(&s.options),
        if_not_exists: s.if_not_exists,
        schema: Schema::empty(),
    })
}

pub(in crate::binder) fn bind_alter_role(s: &AlterRoleStmt) -> Result<LogicalPlan, PlanError> {
    Ok(LogicalPlan::AlterRole {
        kind: bind_role_kind(s.kind),
        role_name: s.name.value.to_ascii_lowercase(),
        options: bind_role_options(&s.options),
        schema: Schema::empty(),
    })
}

pub(in crate::binder) fn bind_drop_role(s: &DropRoleStmt) -> Result<LogicalPlan, PlanError> {
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

pub(in crate::binder) fn bind_comment(
    s: &CommentStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    let target = match &s.target {
        CommentTarget::Table(name) => {
            let resolved = lookup_table_reference(catalog, name)?;
            let table = resolved.plan_name;
            LogicalCommentTarget::Table { table }
        }
        CommentTarget::Index(name) => LogicalCommentTarget::Index {
            index: object_name_simple(name),
            namespace: object_name_explicit_namespace(name),
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
    let table_obj = ObjectName {
        parts: name.parts[..name.parts.len() - 1].to_vec(),
        span: name.span,
    };
    let resolved = lookup_table_reference(catalog, &table_obj)?;
    let table = resolved.plan_name;
    let meta = resolved.meta;
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

pub(super) fn bind_sequence_options(
    options: &[SequenceOption],
) -> Result<LogicalSequenceOptions, PlanError> {
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
