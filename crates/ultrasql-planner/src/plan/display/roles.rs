//! EXPLAIN-style rendering for roles, policies, and privilege management.
//!
//! Helper bodies split verbatim out of [`super`]'s exhaustive match; output is
//! byte-for-byte identical.

use std::fmt;

use super::super::ddl_types::{
    LogicalDefaultPrivilegeOperation, LogicalPrivilegeObjectKind, LogicalRlsPolicy,
    LogicalRoleKind,
};

pub(super) fn fmt_create_policy(policy: &LogicalRlsPolicy, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let _ = fmt::write(
        out,
        format_args!(
            "CreatePolicy: {} ON {}\n",
            policy.policy_name, policy.table_name
        ),
    );
}

pub(super) fn fmt_create_role(
    kind: &LogicalRoleKind,
    role_name: &str,
    if_not_exists: bool,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let ine = if if_not_exists { " IF NOT EXISTS" } else { "" };
    let keyword = match kind {
        LogicalRoleKind::Role => "Role",
        LogicalRoleKind::User => "User",
    };
    let _ = fmt::write(out, format_args!("Create{keyword}{ine}: {role_name}\n"));
}

pub(super) fn fmt_alter_role(
    kind: &LogicalRoleKind,
    role_name: &str,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let keyword = match kind {
        LogicalRoleKind::Role => "Role",
        LogicalRoleKind::User => "User",
    };
    let _ = fmt::write(out, format_args!("Alter{keyword}: {role_name}\n"));
}

pub(super) fn fmt_drop_role(
    kind: &LogicalRoleKind,
    roles: &[String],
    if_exists: bool,
    cascade: bool,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let ie = if if_exists { " IF EXISTS" } else { "" };
    let csc = if cascade { " CASCADE" } else { "" };
    let keyword = match kind {
        LogicalRoleKind::Role => "Role",
        LogicalRoleKind::User => "User",
    };
    let _ = fmt::write(
        out,
        format_args!("Drop{keyword}{ie}: roles=[{}]{csc}\n", roles.join(", ")),
    );
}

pub(super) fn fmt_grant_privileges(
    object_kind: &LogicalPrivilegeObjectKind,
    objects: &[String],
    grantees: &[String],
    grant_option: bool,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let opt = if grant_option {
        " WITH GRANT OPTION"
    } else {
        ""
    };
    let _ = fmt::write(
        out,
        format_args!(
            "GrantPrivileges: {:?} objects=[{}] grantees=[{}]{opt}\n",
            object_kind,
            objects.join(", "),
            grantees.join(", ")
        ),
    );
}

pub(super) fn fmt_revoke_privileges(
    object_kind: &LogicalPrivilegeObjectKind,
    objects: &[String],
    grantees: &[String],
    cascade: bool,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let csc = if cascade { " CASCADE" } else { "" };
    let _ = fmt::write(
        out,
        format_args!(
            "RevokePrivileges: {:?} objects=[{}] grantees=[{}]{csc}\n",
            object_kind,
            objects.join(", "),
            grantees.join(", ")
        ),
    );
}

#[allow(clippy::too_many_arguments)]
pub(super) fn fmt_alter_default_privileges(
    target_roles: &[String],
    schemas: &[String],
    operation: &LogicalDefaultPrivilegeOperation,
    object_kind: &LogicalPrivilegeObjectKind,
    grantees: &[String],
    grant_option: bool,
    cascade: bool,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let scope = if target_roles.is_empty() {
        "current role".to_owned()
    } else {
        target_roles.join(", ")
    };
    let schema_scope = if schemas.is_empty() {
        "all schemas".to_owned()
    } else {
        schemas.join(", ")
    };
    let opt = if grant_option {
        " WITH GRANT OPTION"
    } else if cascade {
        " CASCADE"
    } else {
        ""
    };
    let _ = fmt::write(
        out,
        format_args!(
            "AlterDefaultPrivileges: {:?} {:?} roles=[{}] schemas=[{}] grantees=[{}]{opt}\n",
            operation,
            object_kind,
            scope,
            schema_scope,
            grantees.join(", ")
        ),
    );
}

pub(super) fn fmt_grant_role(
    roles: &[String],
    grantees: &[String],
    admin_option: bool,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let opt = if admin_option {
        " WITH ADMIN OPTION"
    } else {
        ""
    };
    let _ = fmt::write(
        out,
        format_args!(
            "GrantRole: roles=[{}] grantees=[{}]{opt}\n",
            roles.join(", "),
            grantees.join(", ")
        ),
    );
}

pub(super) fn fmt_revoke_role(
    roles: &[String],
    grantees: &[String],
    cascade: bool,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let csc = if cascade { " CASCADE" } else { "" };
    let _ = fmt::write(
        out,
        format_args!(
            "RevokeRole: roles=[{}] grantees=[{}]{csc}\n",
            roles.join(", "),
            grantees.join(", ")
        ),
    );
}
