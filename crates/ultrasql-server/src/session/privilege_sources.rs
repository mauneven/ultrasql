//! Column-source mapping helpers for privilege enforcement.

use std::collections::BTreeSet;

use ultrasql_core::Schema;
use ultrasql_planner::{LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr};

use crate::auth::PrivilegeKind;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct ColumnSource {
    pub(super) table: String,
    pub(super) column: String,
}

pub(super) fn plan_sources(plan: &LogicalPlan) -> Vec<Option<ColumnSource>> {
    match plan {
        LogicalPlan::Scan {
            table,
            schema,
            projection,
        } => scan_sources(table, schema, projection.as_deref()),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::LockRows { input, .. } => plan_sources(input),
        LogicalPlan::Project { input, exprs, .. } => {
            let input_sources = plan_sources(input);
            exprs
                .iter()
                .map(|(expr, _)| expr_direct_source(expr, &input_sources))
                .collect()
        }
        LogicalPlan::Window { input, .. } => {
            let mut sources = plan_sources(input);
            sources.push(None);
            sources
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            ..
        } => {
            let input_sources = plan_sources(input);
            let mut sources = Vec::with_capacity(group_by.len() + aggregates.len());
            for expr in group_by {
                sources.push(expr_direct_source(expr, &input_sources));
            }
            sources.extend((0..aggregates.len()).map(|_| None));
            sources
        }
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            schema,
        } => join_sources(
            plan_sources(left),
            plan_sources(right),
            *join_type,
            condition,
            schema,
        ),
        LogicalPlan::SetOp { schema, .. }
        | LogicalPlan::Cte { schema, .. }
        | LogicalPlan::Values { schema, .. }
        | LogicalPlan::Empty { schema }
        | LogicalPlan::FunctionScan { schema, .. }
        | LogicalPlan::Insert { schema, .. }
        | LogicalPlan::Update { schema, .. }
        | LogicalPlan::Delete { schema, .. }
        | LogicalPlan::Truncate { schema, .. }
        | LogicalPlan::CreateTable { schema, .. }
        | LogicalPlan::CreateMaterializedView { schema, .. }
        | LogicalPlan::CreateTypeEnum { schema, .. }
        | LogicalPlan::CreateTypeComposite { schema, .. }
        | LogicalPlan::CreateDomain { schema, .. }
        | LogicalPlan::CreateOperator { schema, .. }
        | LogicalPlan::CreateIndex { schema, .. }
        | LogicalPlan::DropIndex { schema, .. }
        | LogicalPlan::CreatePolicy { schema, .. }
        | LogicalPlan::CreateRole { schema, .. }
        | LogicalPlan::AlterRole { schema, .. }
        | LogicalPlan::DropRole { schema, .. }
        | LogicalPlan::GrantPrivileges { schema, .. }
        | LogicalPlan::RevokePrivileges { schema, .. }
        | LogicalPlan::AlterDefaultPrivileges { schema, .. }
        | LogicalPlan::GrantRole { schema, .. }
        | LogicalPlan::RevokeRole { schema, .. }
        | LogicalPlan::CreateSchema { schema, .. }
        | LogicalPlan::DropSchema { schema, .. }
        | LogicalPlan::DropTable { schema, .. }
        | LogicalPlan::AlterTable { schema, .. }
        | LogicalPlan::CreateSequence { schema, .. }
        | LogicalPlan::AlterSequence { schema, .. }
        | LogicalPlan::DropSequence { schema, .. }
        | LogicalPlan::Comment { schema, .. }
        | LogicalPlan::Begin { schema, .. }
        | LogicalPlan::Commit { schema }
        | LogicalPlan::Rollback { schema }
        | LogicalPlan::Savepoint { schema, .. }
        | LogicalPlan::RollbackToSavepoint { schema, .. }
        | LogicalPlan::ReleaseSavepoint { schema, .. }
        | LogicalPlan::PrepareTransaction { schema, .. }
        | LogicalPlan::CommitPrepared { schema, .. }
        | LogicalPlan::RollbackPrepared { schema, .. }
        | LogicalPlan::SetTransaction { schema, .. }
        | LogicalPlan::SetVariable { schema, .. }
        | LogicalPlan::SetRole { schema, .. }
        | LogicalPlan::Listen { schema, .. }
        | LogicalPlan::Notify { schema, .. }
        | LogicalPlan::Unlisten { schema, .. }
        | LogicalPlan::Copy { schema, .. }
        | LogicalPlan::Explain { schema, .. } => vec![None; schema.len()],
    }
}

pub(super) fn table_sources(table: &str, schema: &Schema) -> Vec<Option<ColumnSource>> {
    schema
        .fields()
        .iter()
        .map(|field| {
            Some(ColumnSource {
                table: table.to_ascii_lowercase(),
                column: field.name.to_ascii_lowercase(),
            })
        })
        .collect()
}

pub(super) fn target_columns(columns: &[usize], schema: &Schema) -> Vec<usize> {
    if columns.is_empty() {
        (0..schema.len()).collect()
    } else {
        columns.to_vec()
    }
}

pub(super) const fn privilege_name(privilege: PrivilegeKind) -> &'static str {
    match privilege {
        PrivilegeKind::Select => "SELECT",
        PrivilegeKind::Insert => "INSERT",
        PrivilegeKind::Update => "UPDATE",
        PrivilegeKind::Delete => "DELETE",
        PrivilegeKind::Truncate => "TRUNCATE",
        PrivilegeKind::References => "REFERENCES",
        PrivilegeKind::Trigger => "TRIGGER",
        PrivilegeKind::Usage => "USAGE",
        PrivilegeKind::Create => "CREATE",
        PrivilegeKind::Connect => "CONNECT",
        PrivilegeKind::Temporary => "TEMPORARY",
        PrivilegeKind::Execute => "EXECUTE",
    }
}

fn scan_sources(
    table: &str,
    schema: &Schema,
    projection: Option<&[usize]>,
) -> Vec<Option<ColumnSource>> {
    match projection {
        Some(projection) => projection
            .iter()
            .enumerate()
            .map(|(output_index, original_index)| {
                let field = if schema.len() == projection.len() {
                    schema.field(output_index)
                } else {
                    schema.field(*original_index)
                }
                .or_else(|| schema.field(output_index))?;
                Some(ColumnSource {
                    table: table.to_ascii_lowercase(),
                    column: field.name.to_ascii_lowercase(),
                })
            })
            .collect(),
        None => table_sources(table, schema),
    }
}

fn expr_direct_source(expr: &ScalarExpr, sources: &[Option<ColumnSource>]) -> Option<ColumnSource> {
    match expr {
        ScalarExpr::Column { index, .. } => sources.get(*index).cloned().flatten(),
        _ => None,
    }
}

fn join_sources(
    left: Vec<Option<ColumnSource>>,
    right: Vec<Option<ColumnSource>>,
    join_type: LogicalJoinType,
    condition: &LogicalJoinCondition,
    schema: &Schema,
) -> Vec<Option<ColumnSource>> {
    if matches!(join_type, LogicalJoinType::Semi | LogicalJoinType::Anti) {
        return left;
    }
    if schema.len() == left.len() + right.len() {
        return left.into_iter().chain(right).collect();
    }
    if let LogicalJoinCondition::Using(pairs) = condition {
        let mut sources = Vec::with_capacity(schema.len());
        let mut used_right = BTreeSet::new();
        for (left_index, right_index) in pairs {
            sources.push(left.get(*left_index).cloned().flatten());
            used_right.insert(*right_index);
        }
        sources.extend(left.into_iter().enumerate().filter_map(|(index, source)| {
            if pairs.iter().any(|(left_index, _)| *left_index == index) {
                None
            } else {
                Some(source)
            }
        }));
        sources.extend(right.into_iter().enumerate().filter_map(|(index, source)| {
            if used_right.contains(&index) {
                None
            } else {
                Some(source)
            }
        }));
        sources.truncate(schema.len());
        sources.resize_with(schema.len(), || None);
        return sources;
    }
    vec![None; schema.len()]
}

#[cfg(test)]
mod tests {
    use super::*;
    use ultrasql_core::{DataType, Field, Value};
    use ultrasql_planner::{
        AggregateFunc, LogicalAggregateExpr, LogicalSetOp, LogicalSetQuantifier, LogicalWindowFunc,
    };

    fn schema(names: &[&str]) -> Schema {
        Schema::new(
            names
                .iter()
                .map(|name| Field::required(*name, DataType::Int32)),
        )
        .expect("schema")
    }

    fn scan(table: &str, fields: &[&str]) -> LogicalPlan {
        LogicalPlan::Scan {
            table: table.to_owned(),
            schema: schema(fields),
            projection: None,
        }
    }

    fn projected_scan(table: &str, fields: &[&str], projection: Vec<usize>) -> LogicalPlan {
        LogicalPlan::Scan {
            table: table.to_owned(),
            schema: schema(fields),
            projection: Some(projection),
        }
    }

    fn col(name: &str, index: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.to_owned(),
            index,
            data_type: DataType::Int32,
        }
    }

    fn lit(value: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(value),
            data_type: DataType::Int32,
        }
    }

    fn source_names(sources: &[Option<ColumnSource>]) -> Vec<Option<String>> {
        sources
            .iter()
            .map(|source| source.as_ref().map(|s| format!("{}.{}", s.table, s.column)))
            .collect()
    }

    #[test]
    fn scan_project_window_and_aggregate_sources_preserve_direct_columns() {
        assert_eq!(
            source_names(&plan_sources(&scan("Users", &["Id", "Score"]))),
            vec![Some("users.id".to_owned()), Some("users.score".to_owned())]
        );
        assert_eq!(
            source_names(&plan_sources(&projected_scan("Users", &["Id"], vec![1]))),
            vec![Some("users.id".to_owned())]
        );
        assert_eq!(
            target_columns(&[], &schema(&["a", "b", "c"])),
            vec![0, 1, 2]
        );
        assert_eq!(
            target_columns(&[2, 0], &schema(&["a", "b", "c"])),
            vec![2, 0]
        );

        let base = scan("Users", &["id", "score"]);
        let project = LogicalPlan::Project {
            input: Box::new(base.clone()),
            exprs: vec![
                (col("score", 1), "score".to_owned()),
                (lit(7), "seven".to_owned()),
            ],
            schema: schema(&["score", "seven"]),
        };
        assert_eq!(
            source_names(&plan_sources(&project)),
            vec![Some("users.score".to_owned()), None]
        );

        let window = LogicalPlan::Window {
            input: Box::new(base.clone()),
            partition_by: vec![col("id", 0)],
            order_by: Vec::new(),
            func: LogicalWindowFunc::RowNumber,
            output_name: "rn".to_owned(),
            schema: schema(&["id", "score", "rn"]),
        };
        assert_eq!(
            source_names(&plan_sources(&window)),
            vec![
                Some("users.id".to_owned()),
                Some("users.score".to_owned()),
                None
            ]
        );

        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(base),
            group_by: vec![col("id", 0), lit(1)],
            aggregates: vec![LogicalAggregateExpr {
                func: AggregateFunc::Sum,
                arg: Some(col("score", 1)),
                direct_arg: None,
                order_by: None,
                distinct: false,
                output_name: "sum_score".to_owned(),
                data_type: DataType::Int64,
            }],
            schema: schema(&["id", "one", "sum_score"]),
        };
        assert_eq!(
            source_names(&plan_sources(&aggregate)),
            vec![Some("users.id".to_owned()), None, None]
        );
    }

    #[test]
    fn join_and_fallback_sources_cover_using_semi_and_unknown_shapes() {
        let left = scan("LeftT", &["id", "lv"]);
        let right = scan("RightT", &["id", "rv"]);
        let concatenated = LogicalPlan::Join {
            left: Box::new(left.clone()),
            right: Box::new(right.clone()),
            join_type: LogicalJoinType::Inner,
            condition: LogicalJoinCondition::None,
            schema: schema(&["id", "lv", "id2", "rv"]),
        };
        assert_eq!(
            source_names(&plan_sources(&concatenated)),
            vec![
                Some("leftt.id".to_owned()),
                Some("leftt.lv".to_owned()),
                Some("rightt.id".to_owned()),
                Some("rightt.rv".to_owned())
            ]
        );

        let using_join = LogicalPlan::Join {
            left: Box::new(left.clone()),
            right: Box::new(right.clone()),
            join_type: LogicalJoinType::Inner,
            condition: LogicalJoinCondition::Using(vec![(0, 0)]),
            schema: schema(&["id", "lv", "rv"]),
        };
        assert_eq!(
            source_names(&plan_sources(&using_join)),
            vec![
                Some("leftt.id".to_owned()),
                Some("leftt.lv".to_owned()),
                Some("rightt.rv".to_owned())
            ]
        );

        let semi = LogicalPlan::Join {
            left: Box::new(left.clone()),
            right: Box::new(right.clone()),
            join_type: LogicalJoinType::Semi,
            condition: LogicalJoinCondition::None,
            schema: schema(&["id", "lv"]),
        };
        assert_eq!(
            source_names(&plan_sources(&semi)),
            vec![Some("leftt.id".to_owned()), Some("leftt.lv".to_owned())]
        );

        let unknown = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: LogicalJoinType::Inner,
            condition: LogicalJoinCondition::None,
            schema: schema(&["mystery"]),
        };
        assert_eq!(source_names(&plan_sources(&unknown)), vec![None]);

        let setop = LogicalPlan::SetOp {
            op: LogicalSetOp::Union,
            quantifier: LogicalSetQuantifier::Distinct,
            left: Box::new(scan("a", &["x"])),
            right: Box::new(scan("b", &["x"])),
            schema: schema(&["x"]),
        };
        assert_eq!(source_names(&plan_sources(&setop)), vec![None]);
    }

    #[test]
    fn privilege_names_cover_all_supported_privilege_kinds() {
        let cases = [
            (PrivilegeKind::Select, "SELECT"),
            (PrivilegeKind::Insert, "INSERT"),
            (PrivilegeKind::Update, "UPDATE"),
            (PrivilegeKind::Delete, "DELETE"),
            (PrivilegeKind::Truncate, "TRUNCATE"),
            (PrivilegeKind::References, "REFERENCES"),
            (PrivilegeKind::Trigger, "TRIGGER"),
            (PrivilegeKind::Usage, "USAGE"),
            (PrivilegeKind::Create, "CREATE"),
            (PrivilegeKind::Connect, "CONNECT"),
            (PrivilegeKind::Temporary, "TEMPORARY"),
            (PrivilegeKind::Execute, "EXECUTE"),
        ];
        for (kind, expected) in cases {
            assert_eq!(privilege_name(kind), expected);
        }
    }
}
