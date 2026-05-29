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
    let (operation, privileges, object_kind, grantees, grant_option, grant_option_for, cascade) =
        match &s.action {
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

#[cfg(test)]
mod tests {
    use super::*;
    use ultrasql_parser::span::Span;

    fn ident(name: &str) -> Identifier {
        Identifier {
            value: name.to_owned(),
            quoted: false,
            span: Span::default(),
        }
    }

    fn object(parts: &[&str]) -> ObjectName {
        ObjectName {
            parts: parts.iter().map(|part| ident(part)).collect(),
            span: Span::default(),
        }
    }

    fn spec(kind: AstPrivilegeKind, columns: &[&str]) -> AstPrivilegeSpec {
        AstPrivilegeSpec {
            kind,
            columns: columns.iter().map(|column| ident(column)).collect(),
        }
    }

    #[test]
    fn all_privileges_expand_per_object_kind() {
        let cases = [
            (LogicalPrivilegeObjectKind::Table, 7),
            (LogicalPrivilegeObjectKind::Schema, 2),
            (LogicalPrivilegeObjectKind::Database, 3),
            (LogicalPrivilegeObjectKind::Sequence, 3),
            (LogicalPrivilegeObjectKind::Function, 1),
        ];

        for (object_kind, expected_len) in cases {
            let privileges = bind_privileges(&[spec(AstPrivilegeKind::All, &[])], object_kind)
                .expect("all privileges bind");
            assert_eq!(privileges.len(), expected_len);
            assert!(
                privileges
                    .iter()
                    .all(|privilege| privilege.columns.is_empty())
            );
        }
    }

    #[test]
    fn column_privileges_validate_object_and_kind() {
        let allowed = [
            AstPrivilegeKind::Select,
            AstPrivilegeKind::Insert,
            AstPrivilegeKind::Update,
            AstPrivilegeKind::References,
        ];
        for kind in allowed {
            let privileges =
                bind_privileges(&[spec(kind, &["ID"])], LogicalPrivilegeObjectKind::Table)
                    .expect("column privilege bind");
            assert_eq!(privileges[0].columns, vec!["id"]);
        }

        let err = bind_privileges(
            &[spec(AstPrivilegeKind::Select, &["id"])],
            LogicalPrivilegeObjectKind::Sequence,
        )
        .expect_err("sequence column privilege rejected");
        assert!(err.to_string().contains("column privileges only apply"));

        let err = bind_privileges(
            &[spec(AstPrivilegeKind::Delete, &["id"])],
            LogicalPrivilegeObjectKind::Table,
        )
        .expect_err("delete column privilege rejected");
        assert!(err.to_string().contains("SELECT, INSERT, UPDATE"));
    }

    #[test]
    fn bind_grant_revoke_and_default_privilege_shapes() {
        let grant = GrantStmt {
            privileges: vec![spec(AstPrivilegeKind::Usage, &[])],
            object_kind: AstPrivilegeObjectKind::Schema,
            objects: vec![object(&["Public"])],
            grantees: vec![ident("Analyst")],
            grant_option: true,
            span: Span::default(),
        };
        match bind_grant_privileges(&grant).expect("grant binds") {
            LogicalPlan::GrantPrivileges {
                privileges,
                object_kind,
                objects,
                grantees,
                grant_option,
                ..
            } => {
                assert_eq!(privileges[0].kind, LogicalPrivilegeKind::Usage);
                assert_eq!(object_kind, LogicalPrivilegeObjectKind::Schema);
                assert_eq!(objects, vec!["public"]);
                assert_eq!(grantees, vec!["analyst"]);
                assert!(grant_option);
            }
            other => panic!("unexpected plan: {other:?}"),
        }

        let revoke = RevokeStmt {
            grant_option_for: true,
            privileges: vec![spec(AstPrivilegeKind::Connect, &[])],
            object_kind: AstPrivilegeObjectKind::Database,
            objects: vec![object(&["MainDb"])],
            grantees: vec![ident("Analyst")],
            cascade: true,
            span: Span::default(),
        };
        assert!(matches!(
            bind_revoke_privileges(&revoke).expect("revoke binds"),
            LogicalPlan::RevokePrivileges {
                object_kind: LogicalPrivilegeObjectKind::Database,
                grant_option_for: true,
                cascade: true,
                ..
            }
        ));

        let default_grant = AlterDefaultPrivilegesStmt {
            target_roles: vec![ident("Owner")],
            schemas: vec![ident("App")],
            action: DefaultPrivilegeAction::Grant {
                privileges: vec![spec(AstPrivilegeKind::Execute, &[])],
                object_kind: AstPrivilegeObjectKind::Function,
                grantees: vec![ident("Analyst")],
                grant_option: true,
            },
            span: Span::default(),
        };
        assert!(matches!(
            bind_alter_default_privileges(&default_grant).expect("default grant binds"),
            LogicalPlan::AlterDefaultPrivileges {
                operation: LogicalDefaultPrivilegeOperation::Grant,
                object_kind: LogicalPrivilegeObjectKind::Function,
                grant_option: true,
                ..
            }
        ));
    }

    #[test]
    fn default_privileges_reject_unsupported_shapes() {
        let default_database = AlterDefaultPrivilegesStmt {
            target_roles: vec![],
            schemas: vec![],
            action: DefaultPrivilegeAction::Grant {
                privileges: vec![spec(AstPrivilegeKind::Connect, &[])],
                object_kind: AstPrivilegeObjectKind::Database,
                grantees: vec![ident("Analyst")],
                grant_option: false,
            },
            span: Span::default(),
        };
        assert!(
            bind_alter_default_privileges(&default_database)
                .expect_err("database default privilege rejected")
                .to_string()
                .contains("database objects")
        );

        let default_schema_in_schema = AlterDefaultPrivilegesStmt {
            target_roles: vec![],
            schemas: vec![ident("app")],
            action: DefaultPrivilegeAction::Grant {
                privileges: vec![spec(AstPrivilegeKind::Usage, &[])],
                object_kind: AstPrivilegeObjectKind::Schema,
                grantees: vec![ident("Analyst")],
                grant_option: false,
            },
            span: Span::default(),
        };
        assert!(
            bind_alter_default_privileges(&default_schema_in_schema)
                .expect_err("schema in schema default privilege rejected")
                .to_string()
                .contains("cannot use IN SCHEMA")
        );

        let default_columns = AlterDefaultPrivilegesStmt {
            target_roles: vec![],
            schemas: vec![],
            action: DefaultPrivilegeAction::Revoke {
                grant_option_for: true,
                privileges: vec![spec(AstPrivilegeKind::Select, &["id"])],
                object_kind: AstPrivilegeObjectKind::Table,
                grantees: vec![ident("Analyst")],
                cascade: true,
            },
            span: Span::default(),
        };
        assert!(
            bind_alter_default_privileges(&default_columns)
                .expect_err("default column privilege rejected")
                .to_string()
                .contains("column privilege lists")
        );
    }

    #[test]
    fn role_grants_and_object_keys_fold_case() {
        assert_eq!(
            bind_object_names(&[object(&["App", "Users"])]),
            vec!["app.users"]
        );
        assert_eq!(
            bind_ident_names(&[ident("Owner"), ident("PUBLIC")]),
            vec!["owner", "public"]
        );

        let grant_role = GrantRoleStmt {
            roles: vec![ident("Reader")],
            grantees: vec![ident("Analyst")],
            admin_option: true,
            span: Span::default(),
        };
        assert!(matches!(
            bind_grant_role(&grant_role).expect("grant role binds"),
            LogicalPlan::GrantRole {
                admin_option: true,
                ..
            }
        ));

        let revoke_role = RevokeRoleStmt {
            admin_option_for: true,
            roles: vec![ident("Reader")],
            grantees: vec![ident("Analyst")],
            cascade: true,
            span: Span::default(),
        };
        assert!(matches!(
            bind_revoke_role(&revoke_role).expect("revoke role binds"),
            LogicalPlan::RevokeRole {
                admin_option_for: true,
                cascade: true,
                ..
            }
        ));
    }
}
