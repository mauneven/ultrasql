//! Broad coverage test exercising `display`/`schema`/`pipeline_mode` across
//! the control-flow and DDL plan variants. Split out of the original
//! monolithic `plan.rs` verbatim.

use ultrasql_core::{DataType, Field, Schema};

use crate::plan::*;

use super::{col, lit_i32, users_schema};

#[test]
#[allow(clippy::too_many_lines)]
fn display_schema_and_pipeline_cover_control_and_ddl_variants() {
    fn empty() -> Schema {
        Schema::empty()
    }

    fn scan(name: &str) -> LogicalPlan {
        LogicalPlan::Scan {
            table: name.to_owned(),
            schema: users_schema(),
            projection: None,
        }
    }

    let check = LogicalCheckConstraint {
        name: "amount_positive".to_owned(),
        expr: col("id", 0, DataType::Int32),
    };
    let unique = LogicalUniqueConstraint {
        name: "users_pkey".to_owned(),
        columns: vec![0],
        primary_key: true,
    };
    let exclusion = LogicalExclusionConstraint {
        name: "users_excl".to_owned(),
        method: LogicalIndexMethod::Gist,
        elements: vec![LogicalExclusionElement {
            column: 0,
            op: ultrasql_parser::ast::BinaryOp::Eq,
        }],
    };

    let mut plans = vec![
        (
            LogicalPlan::Truncate {
                tables: vec!["users".to_owned(), "orders".to_owned()],
                restart_identity: true,
                cascade: true,
                schema: empty(),
            },
            "Truncate:",
        ),
        (
            LogicalPlan::CreateTable {
                table_name: "users".to_owned(),
                namespace: "public".to_owned(),
                columns: users_schema(),
                column_collations: vec![None, None],
                defaults: vec![None, None],
                sequence_defaults: vec![None, None],
                sequence_options: vec![None, None],
                identity_always: vec![false, false],
                generated_stored: vec![None, None],
                checks: vec![check.clone()],
                unique_constraints: vec![unique.clone()],
                foreign_keys: vec![],
                exclusion_constraints: vec![exclusion],
                partition: None,
                if_not_exists: true,
                schema: empty(),
            },
            "CreateTable:",
        ),
        (
            LogicalPlan::CreateMaterializedView {
                table_name: "mv_users".to_owned(),
                namespace: "public".to_owned(),
                columns: users_schema(),
                source: Box::new(scan("users")),
                if_not_exists: true,
                schema: empty(),
            },
            "CreateMaterializedView:",
        ),
        (
            LogicalPlan::CreateTypeEnum {
                type_name: "mood".to_owned(),
                namespace: "public".to_owned(),
                labels: vec!["sad".to_owned(), "ok".to_owned()],
                schema: empty(),
            },
            "CreateTypeEnum:",
        ),
        (
            LogicalPlan::CreateTypeComposite {
                type_name: "pair".to_owned(),
                namespace: "public".to_owned(),
                attributes: users_schema(),
                schema: empty(),
            },
            "CreateTypeComposite:",
        ),
        (
            LogicalPlan::CreateDomain {
                domain_name: "positive_int".to_owned(),
                namespace: "public".to_owned(),
                base_type: DataType::Int32,
                not_null: true,
                checks: vec![check.clone()],
                schema: empty(),
            },
            "CreateDomain:",
        ),
        (
            LogicalPlan::SetOp {
                op: LogicalSetOp::Intersect,
                quantifier: LogicalSetQuantifier::All,
                left: Box::new(scan("left")),
                right: Box::new(scan("right")),
                schema: users_schema(),
            },
            "SetOp[Intersect All]",
        ),
        (
            LogicalPlan::SetOp {
                op: LogicalSetOp::Except,
                quantifier: LogicalSetQuantifier::Distinct,
                left: Box::new(scan("left")),
                right: Box::new(scan("right")),
                schema: users_schema(),
            },
            "SetOp[Except Distinct]",
        ),
        (
            LogicalPlan::Cte {
                name: "cte_users".to_owned(),
                recursive: true,
                definition: Box::new(scan("users")),
                body: Box::new(scan("cte_users")),
                schema: users_schema(),
            },
            "Cte RECURSIVE:",
        ),
        (
            LogicalPlan::LockRows {
                input: Box::new(scan("users")),
                strength: LockStrength::NoKeyUpdate,
                wait_policy: LockWaitPolicy::NoWait,
                schema: users_schema(),
            },
            "LockRows: FOR NO KEY UPDATE NOWAIT",
        ),
        (
            LogicalPlan::LockRows {
                input: Box::new(scan("users")),
                strength: LockStrength::KeyShare,
                wait_policy: LockWaitPolicy::SkipLocked,
                schema: users_schema(),
            },
            "LockRows: FOR KEY SHARE SKIP LOCKED",
        ),
        (
            LogicalPlan::CreateIndex {
                index_name: "users_expr_idx".to_owned(),
                index_namespace: "public".to_owned(),
                table_name: "users".to_owned(),
                columns: vec![],
                key_exprs: vec![col("id", 0, DataType::Int32)],
                opclasses: vec![None],
                index_options: vec![LogicalIndexOption {
                    name: "lists".to_owned(),
                    value: "128".to_owned(),
                }],
                include_columns: vec![],
                predicate: None,
                method: LogicalIndexMethod::IvfFlat,
                aggregating: None,
                unique: true,
                primary_key: false,
                concurrently: true,
                if_not_exists: true,
                schema: empty(),
            },
            "CreateUniqueIndex Concurrently IF NOT EXISTS:",
        ),
        (
            LogicalPlan::DropIndex {
                indexes: vec!["users_expr_idx".to_owned()],
                index_namespaces: vec![None],
                if_exists: true,
                cascade: true,
                schema: empty(),
            },
            "DropIndex IF EXISTS: indexes=[users_expr_idx] CASCADE",
        ),
        (
            LogicalPlan::AlterTable {
                table_name: "users".to_owned(),
                action: LogicalAlterTableAction::RenameColumn {
                    column_index: 0,
                    old_name: "id".to_owned(),
                    new_name: "user_id".to_owned(),
                },
                schema: empty(),
            },
            "RENAME COLUMN",
        ),
        (
            LogicalPlan::AlterTable {
                table_name: "users".to_owned(),
                action: LogicalAlterTableAction::SetOptions {
                    options: vec![LogicalTableOption {
                        name: "fillfactor".to_owned(),
                        value: "90".to_owned(),
                    }],
                },
                schema: empty(),
            },
            "SET (fillfactor=90)",
        ),
        (
            LogicalPlan::AlterTable {
                table_name: "users".to_owned(),
                action: LogicalAlterTableAction::AddUniqueConstraint { constraint: unique },
                schema: empty(),
            },
            "ADD CONSTRAINT",
        ),
        (
            LogicalPlan::CreatePolicy {
                policy: LogicalRlsPolicy {
                    policy_name: "tenant_isolation".to_owned(),
                    table_name: "users".to_owned(),
                    permissiveness: LogicalRlsPermissiveness::Restrictive,
                    command: LogicalRlsCommand::Select,
                    roles: Vec::new(),
                    using: Some(LogicalTenantPolicyExpr {
                        column_index: 0,
                        column_name: "id".to_owned(),
                        setting_name: "app.tenant".to_owned(),
                    }),
                    with_check: None,
                },
                schema: empty(),
            },
            "CreatePolicy:",
        ),
        (
            LogicalPlan::CreateRole {
                kind: LogicalRoleKind::User,
                role_name: "analyst".to_owned(),
                options: LogicalRoleOptions::default(),
                if_not_exists: true,
                schema: empty(),
            },
            "CreateUser IF NOT EXISTS:",
        ),
        (
            LogicalPlan::AlterRole {
                kind: LogicalRoleKind::Role,
                role_name: "analyst".to_owned(),
                options: LogicalRoleOptions::default(),
                schema: empty(),
            },
            "AlterRole:",
        ),
        (
            LogicalPlan::DropRole {
                kind: LogicalRoleKind::User,
                roles: vec!["analyst".to_owned()],
                if_exists: true,
                cascade: true,
                schema: empty(),
            },
            "DropUser IF EXISTS:",
        ),
        (
            LogicalPlan::GrantPrivileges {
                privileges: vec![LogicalPrivilegeSpec {
                    kind: LogicalPrivilegeKind::Select,
                    columns: vec![],
                }],
                object_kind: LogicalPrivilegeObjectKind::Table,
                objects: vec!["users".to_owned()],
                grantees: vec!["analyst".to_owned()],
                grant_option: true,
                schema: empty(),
            },
            "GrantPrivileges:",
        ),
        (
            LogicalPlan::RevokePrivileges {
                privileges: vec![LogicalPrivilegeSpec {
                    kind: LogicalPrivilegeKind::Update,
                    columns: vec!["id".to_owned()],
                }],
                object_kind: LogicalPrivilegeObjectKind::Table,
                objects: vec!["users".to_owned()],
                grantees: vec!["analyst".to_owned()],
                grant_option_for: true,
                cascade: true,
                schema: empty(),
            },
            "RevokePrivileges:",
        ),
        (
            LogicalPlan::AlterDefaultPrivileges {
                target_roles: vec![],
                schemas: vec![],
                operation: LogicalDefaultPrivilegeOperation::Revoke,
                privileges: vec![LogicalPrivilegeSpec {
                    kind: LogicalPrivilegeKind::Execute,
                    columns: vec![],
                }],
                object_kind: LogicalPrivilegeObjectKind::Function,
                grantees: vec!["analyst".to_owned()],
                grant_option: false,
                grant_option_for: true,
                cascade: true,
                schema: empty(),
            },
            "AlterDefaultPrivileges:",
        ),
        (
            LogicalPlan::GrantRole {
                roles: vec!["reader".to_owned()],
                grantees: vec!["analyst".to_owned()],
                admin_option: true,
                schema: empty(),
            },
            "GrantRole:",
        ),
        (
            LogicalPlan::RevokeRole {
                roles: vec!["reader".to_owned()],
                grantees: vec!["analyst".to_owned()],
                admin_option_for: true,
                cascade: true,
                schema: empty(),
            },
            "RevokeRole:",
        ),
        (
            LogicalPlan::CreateSequence {
                sequence_name: "users_id_seq".to_owned(),
                namespace: "public".to_owned(),
                options: LogicalSequenceOptions::default(),
                if_not_exists: true,
                schema: empty(),
            },
            "CreateSequence IF NOT EXISTS:",
        ),
        (
            LogicalPlan::AlterSequence {
                sequence_name: "users_id_seq".to_owned(),
                namespace: None,
                options: LogicalSequenceChange {
                    increment: Some(10),
                    ..LogicalSequenceChange::default()
                },
                schema: empty(),
            },
            "AlterSequence:",
        ),
        (
            LogicalPlan::DropSequence {
                sequences: vec!["users_id_seq".to_owned()],
                sequence_namespaces: vec![None],
                if_exists: true,
                cascade: true,
                schema: empty(),
            },
            "DropSequence IF EXISTS:",
        ),
        (
            LogicalPlan::Comment {
                target: LogicalCommentTarget::Column {
                    table: "users".to_owned(),
                    column: "id".to_owned(),
                    attnum: 1,
                },
                comment: None,
                schema: empty(),
            },
            "Comment: COLUMN",
        ),
        (
            LogicalPlan::PrepareTransaction {
                gid: "g1".to_owned(),
                schema: empty(),
            },
            "PrepareTransaction:",
        ),
        (
            LogicalPlan::CommitPrepared {
                gid: "g1".to_owned(),
                schema: empty(),
            },
            "CommitPrepared:",
        ),
        (
            LogicalPlan::RollbackPrepared {
                gid: "g1".to_owned(),
                schema: empty(),
            },
            "RollbackPrepared:",
        ),
        (
            LogicalPlan::SetTransaction {
                isolation_level: TxnIsolationLevel::Serializable,
                schema: empty(),
            },
            "SetTransaction:",
        ),
        (
            LogicalPlan::SetVariable {
                name: "work_mem".to_owned(),
                action: LogicalSetVariableAction::Reset,
                value: None,
                schema: empty(),
            },
            "SetVariable:",
        ),
        (
            LogicalPlan::SetRole {
                role_name: None,
                schema: empty(),
            },
            "SetRole: NONE",
        ),
        (
            LogicalPlan::Listen {
                channel: "events".to_owned(),
                schema: empty(),
            },
            "Listen:",
        ),
        (
            LogicalPlan::Notify {
                channel: "events".to_owned(),
                payload: Some("payload".to_owned()),
                schema: empty(),
            },
            "Notify:",
        ),
        (
            LogicalPlan::Unlisten {
                channel: None,
                schema: empty(),
            },
            "Unlisten: *",
        ),
        (
            LogicalPlan::Explain {
                analyze: true,
                format: ExplainFormat::Text,
                input: Box::new(scan("users")),
                schema: Schema::new([Field::nullable(
                    "QUERY PLAN",
                    DataType::Text { max_len: None },
                )])
                .expect("explain schema"),
            },
            "Explain ANALYZE (TEXT)",
        ),
        (
            LogicalPlan::Copy {
                relation: Some("users".to_owned()),
                input: None,
                columns: vec![],
                direction: CopyDirection::To,
                source: CopySource::File("/tmp/users.csv".to_owned()),
                format: CopyFormat::Binary,
                delimiter: ',',
                null_str: "\\N".to_owned(),
                header: false,
                auto_detect: false,
                ignore_errors: false,
                max_errors: 0,
                reject_table: None,
                schema: users_schema(),
            },
            "Copy: users (*) TO FILE FORMAT=BINARY",
        ),
        (
            LogicalPlan::FunctionScan {
                name: "generate_series".to_owned(),
                args: vec![lit_i32(1), lit_i32(3)],
                schema: Schema::new([Field::required("generate_series", DataType::Int64)])
                    .expect("function schema"),
            },
            "FunctionScan:",
        ),
    ];

    plans.push((
        LogicalPlan::Join {
            left: Box::new(scan("a")),
            right: Box::new(scan("b")),
            join_type: LogicalJoinType::Anti,
            condition: LogicalJoinCondition::Using(vec![(0, 0)]),
            schema: users_schema(),
        },
        "Join[Anti]: USING",
    ));

    for (plan, expected) in plans {
        let expected_mode = if matches!(
            expected,
            "SetOp[Intersect All]"
                | "SetOp[Except Distinct]"
                | "Cte RECURSIVE:"
                | "LockRows: FOR NO KEY UPDATE NOWAIT"
                | "LockRows: FOR KEY SHARE SKIP LOCKED"
                | "Join[Anti]: USING"
        ) {
            PipelineMode::VectorizedOlap
        } else {
            PipelineMode::ScalarOltp
        };
        assert_eq!(plan.pipeline_mode(), expected_mode, "{expected}");
        let _ = plan.schema();
        let dump = plan.display(0);
        assert!(
            dump.contains(expected),
            "expected {expected:?}, got: {dump}"
        );
    }

    let olap = LogicalPlan::Project {
        input: Box::new(scan("users")),
        exprs: vec![(col("id", 0, DataType::Int32), "id".to_owned())],
        schema: Schema::new([Field::required("id", DataType::Int32)]).expect("project schema"),
    };
    assert_eq!(olap.pipeline_mode(), PipelineMode::VectorizedOlap);
}
