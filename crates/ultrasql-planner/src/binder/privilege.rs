//! Binder for privilege-management DDL.

use ultrasql_core::Schema;
use ultrasql_parser::ast::{
    AlterDefaultPrivilegesStmt, DefaultPrivilegeAction, GrantRoleStmt, GrantStmt, Identifier,
    ObjectName, PrivilegeKind as AstPrivilegeKind, PrivilegeObjectKind as AstPrivilegeObjectKind,
    PrivilegeSpec as AstPrivilegeSpec, RevokeRoleStmt, RevokeStmt,
};

use super::{LogicalPlan, PlanError};
use crate::plan::{
    LogicalDefaultPrivilegeOperation, LogicalPrivilegeKind, LogicalPrivilegeObjectKind,
    LogicalPrivilegeSpec,
};

pub(super) fn bind_grant_privileges(s: &GrantStmt) -> Result<LogicalPlan, super::PlanError> {
    let object_kind = bind_object_kind(s.object_kind);
    Ok(LogicalPlan::GrantPrivileges {
        privileges: bind_privileges(&s.privileges, object_kind)?,
        object_kind,
        objects: bind_object_names(&s.objects),
        grantees: bind_role_names(&s.grantees),
        grant_option: s.grant_option,
        schema: Schema::empty(),
    })
}

pub(super) fn bind_revoke_privileges(s: &RevokeStmt) -> Result<LogicalPlan, super::PlanError> {
    let object_kind = bind_object_kind(s.object_kind);
    Ok(LogicalPlan::RevokePrivileges {
        privileges: bind_privileges(&s.privileges, object_kind)?,
        object_kind,
        objects: bind_object_names(&s.objects),
        grantees: bind_role_names(&s.grantees),
        grant_option_for: s.grant_option_for,
        cascade: s.cascade,
        schema: Schema::empty(),
    })
}

pub(super) fn bind_alter_default_privileges(
    s: &AlterDefaultPrivilegesStmt,
) -> Result<LogicalPlan, super::PlanError> {
    let (
        operation,
        privileges,
        object_kind,
        grantees,
        grant_option,
        grant_option_for,
        cascade,
    ) = match &s.action {
        DefaultPrivilegeAction::Grant {
            privileges,
            object_kind,
            grantees,
            grant_option,
        } => (
            LogicalDefaultPrivilegeOperation::Grant,
            privileges,
            *object_kind,
            grantees,
            *grant_option,
            false,
            false,
        ),
        DefaultPrivilegeAction::Revoke {
            grant_option_for,
            privileges,
            object_kind,
            grantees,
            cascade,
        } => (
            LogicalDefaultPrivilegeOperation::Revoke,
            privileges,
            *object_kind,
            grantees,
            false,
            *grant_option_for,
            *cascade,
        ),
    };
    let object_kind = bind_object_kind(object_kind);
    if object_kind == LogicalPrivilegeObjectKind::Database {
        return Err(PlanError::NotSupported(
            "default privileges do not apply to database objects",
        ));
    }
    if object_kind == LogicalPrivilegeObjectKind::Schema && !s.schemas.is_empty() {
        return Err(PlanError::NotSupported(
            "default schema privileges cannot use IN SCHEMA",
        ));
    }
    let privileges = bind_default_privileges(privileges, object_kind)?;
    Ok(LogicalPlan::AlterDefaultPrivileges {
        target_roles: bind_ident_names(&s.target_roles),
        schemas: bind_ident_names(&s.schemas),
        operation,
        privileges,
        object_kind,
        grantees: bind_role_names(grantees),
        grant_option,
        grant_option_for,
        cascade,
        schema: Schema::empty(),
    })
}

pub(super) fn bind_grant_role(s: &GrantRoleStmt) -> Result<LogicalPlan, super::PlanError> {
    Ok(LogicalPlan::GrantRole {
        roles: bind_ident_names(&s.roles),
        grantees: bind_ident_names(&s.grantees),
        admin_option: s.admin_option,
        schema: Schema::empty(),
    })
}

pub(super) fn bind_revoke_role(s: &RevokeRoleStmt) -> Result<LogicalPlan, super::PlanError> {
    Ok(LogicalPlan::RevokeRole {
        roles: bind_ident_names(&s.roles),
        grantees: bind_ident_names(&s.grantees),
        admin_option_for: s.admin_option_for,
        cascade: s.cascade,
        schema: Schema::empty(),
    })
}

fn bind_object_kind(kind: AstPrivilegeObjectKind) -> LogicalPrivilegeObjectKind {
    match kind {
        AstPrivilegeObjectKind::Table => LogicalPrivilegeObjectKind::Table,
        AstPrivilegeObjectKind::Schema => LogicalPrivilegeObjectKind::Schema,
        AstPrivilegeObjectKind::Database => LogicalPrivilegeObjectKind::Database,
        AstPrivilegeObjectKind::Sequence => LogicalPrivilegeObjectKind::Sequence,
        AstPrivilegeObjectKind::Function => LogicalPrivilegeObjectKind::Function,
    }
}

fn bind_privileges(
    privileges: &[AstPrivilegeSpec],
    object_kind: LogicalPrivilegeObjectKind,
) -> Result<Vec<LogicalPrivilegeSpec>, PlanError> {
    let mut bound = Vec::with_capacity(privileges.len());
    for privilege in privileges {
        if !privilege.columns.is_empty() {
            validate_column_privilege(privilege.kind, object_kind)?;
        }
        match privilege.kind {
            AstPrivilegeKind::All => {
                for kind in all_privileges_for(object_kind) {
                    bound.push(LogicalPrivilegeSpec {
                        kind: *kind,
                        columns: Vec::new(),
                    });
                }
            }
            _ => bound.push(LogicalPrivilegeSpec {
                kind: bind_privilege_kind(privilege.kind),
                columns: privilege
                    .columns
                    .iter()
                    .map(|column| column.value.to_ascii_lowercase())
                    .collect(),
            }),
        }
    }
    Ok(bound)
}

fn bind_default_privileges(
    privileges: &[AstPrivilegeSpec],
    object_kind: LogicalPrivilegeObjectKind,
) -> Result<Vec<LogicalPrivilegeSpec>, PlanError> {
    let bound = bind_privileges(privileges, object_kind)?;
    if bound.iter().any(|privilege| !privilege.columns.is_empty()) {
        return Err(PlanError::NotSupported(
            "default privileges do not support column privilege lists",
        ));
    }
    Ok(bound)
}

fn validate_column_privilege(
    kind: AstPrivilegeKind,
    object_kind: LogicalPrivilegeObjectKind,
) -> Result<(), PlanError> {
    if object_kind != LogicalPrivilegeObjectKind::Table {
        return Err(PlanError::NotSupported(
            "column privileges only apply to table objects",
        ));
    }
    match kind {
        AstPrivilegeKind::Select
        | AstPrivilegeKind::Insert
        | AstPrivilegeKind::Update
        | AstPrivilegeKind::References => Ok(()),
        _ => Err(PlanError::NotSupported(
            "column privileges support SELECT, INSERT, UPDATE, and REFERENCES",
        )),
    }
}
fn bind_privilege_kind(kind: AstPrivilegeKind) -> LogicalPrivilegeKind {
    match kind {
        AstPrivilegeKind::All => unreachable!("ALL expands before binding single kind"),
        AstPrivilegeKind::Select => LogicalPrivilegeKind::Select,
        AstPrivilegeKind::Insert => LogicalPrivilegeKind::Insert,
        AstPrivilegeKind::Update => LogicalPrivilegeKind::Update,
        AstPrivilegeKind::Delete => LogicalPrivilegeKind::Delete,
        AstPrivilegeKind::Truncate => LogicalPrivilegeKind::Truncate,
        AstPrivilegeKind::References => LogicalPrivilegeKind::References,
        AstPrivilegeKind::Trigger => LogicalPrivilegeKind::Trigger,
        AstPrivilegeKind::Usage => LogicalPrivilegeKind::Usage,
        AstPrivilegeKind::Create => LogicalPrivilegeKind::Create,
        AstPrivilegeKind::Connect => LogicalPrivilegeKind::Connect,
        AstPrivilegeKind::Temporary => LogicalPrivilegeKind::Temporary,
        AstPrivilegeKind::Execute => LogicalPrivilegeKind::Execute,
    }
}

fn all_privileges_for(object_kind: LogicalPrivilegeObjectKind) -> &'static [LogicalPrivilegeKind] {
    match object_kind {
        LogicalPrivilegeObjectKind::Table => &[
            LogicalPrivilegeKind::Select,
            LogicalPrivilegeKind::Insert,
            LogicalPrivilegeKind::Update,
            LogicalPrivilegeKind::Delete,
            LogicalPrivilegeKind::Truncate,
            LogicalPrivilegeKind::References,
            LogicalPrivilegeKind::Trigger,
        ],
        LogicalPrivilegeObjectKind::Schema => {
            &[LogicalPrivilegeKind::Create, LogicalPrivilegeKind::Usage]
        }
        LogicalPrivilegeObjectKind::Database => &[
            LogicalPrivilegeKind::Create,
            LogicalPrivilegeKind::Connect,
            LogicalPrivilegeKind::Temporary,
        ],
        LogicalPrivilegeObjectKind::Sequence => &[
            LogicalPrivilegeKind::Usage,
            LogicalPrivilegeKind::Select,
            LogicalPrivilegeKind::Update,
        ],
        LogicalPrivilegeObjectKind::Function => &[LogicalPrivilegeKind::Execute],
    }
}

fn bind_object_names(objects: &[ObjectName]) -> Vec<String> {
    objects.iter().map(object_name_key).collect()
}

fn object_name_key(name: &ObjectName) -> String {
    name.parts
        .iter()
        .map(|part| part.value.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(".")
}

fn bind_role_names(names: &[Identifier]) -> Vec<String> {
    bind_ident_names(names)
}

fn bind_ident_names(names: &[Identifier]) -> Vec<String> {
    names
        .iter()
        .map(|name| name.value.to_ascii_lowercase())
        .collect()
}
