//! Unit tests for the logical plan tree. Split out of the original
//! monolithic `plan.rs`; helpers are shared with the [`coverage`] submodule.

use ultrasql_core::{DataType, Field, Schema, Value};

use crate::expr::ScalarExpr;

use super::*;

mod coverage;

pub(super) fn users_schema() -> Schema {
    Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("score", DataType::Float64),
    ])
    .expect("schema invariants hold for test fixture")
}

pub(super) fn lit_i32(v: i32) -> ScalarExpr {
    ScalarExpr::Literal {
        value: Value::Int32(v),
        data_type: DataType::Int32,
    }
}

pub(super) fn lit_text(s: &str) -> ScalarExpr {
    ScalarExpr::Literal {
        value: Value::Text(s.to_owned()),
        data_type: DataType::Text { max_len: None },
    }
}

pub(super) fn col(name: &str, index: usize, data_type: DataType) -> ScalarExpr {
    ScalarExpr::Column {
        name: name.to_owned(),
        index,
        data_type,
    }
}

#[test]
fn empty_plan_schema_round_trips() {
    let plan = LogicalPlan::Empty {
        schema: Schema::empty(),
    };
    assert!(plan.schema().is_empty());
}

#[test]
fn scan_display_names_table() {
    let plan = LogicalPlan::Scan {
        table: "users".into(),
        schema: users_schema(),
        projection: None,
    };
    assert!(plan.display(0).contains("Scan: users"));
}

/// A `Values` plan's inferred schema columns have the right data types.
#[test]
fn values_schema_infers_column_types() {
    // Two rows: (1, 'alice'), (2, 'bob')
    let schema = Schema::new([
        Field::nullable("column1", DataType::Int32),
        Field::nullable("column2", DataType::Text { max_len: None }),
    ])
    .expect("schema ok");
    let plan = LogicalPlan::Values {
        rows: vec![
            vec![lit_i32(1), lit_text("alice")],
            vec![lit_i32(2), lit_text("bob")],
        ],
        schema,
    };
    assert_eq!(plan.schema().len(), 2);
    assert_eq!(plan.schema().field_at(0).data_type, DataType::Int32);
    assert_eq!(
        plan.schema().field_at(1).data_type,
        DataType::Text { max_len: None }
    );
    let dump = plan.display(0);
    assert!(dump.contains("Values: 2 row(s)"));
}

/// An `Insert` plan's schema matches the `RETURNING` projection.
#[test]
fn insert_plan_schema_matches_returning() {
    let returning_schema = Schema::new([
        Field::nullable("id", DataType::Int32),
        Field::nullable("score", DataType::Float64),
    ])
    .expect("schema ok");
    let source = LogicalPlan::Values {
        rows: vec![vec![lit_i32(42)]],
        schema: Schema::new([Field::nullable("column1", DataType::Int32)]).expect("schema ok"),
    };
    let plan = LogicalPlan::Insert {
        table: "users".into(),
        columns: vec![0],
        source: Box::new(source),
        on_conflict: None,
        returning: vec![
            (col("id", 0, DataType::Int32), "id".into()),
            (col("score", 1, DataType::Float64), "score".into()),
        ],
        schema: returning_schema.clone(),
    };
    assert_eq!(plan.schema(), &returning_schema);
}

/// An `Update` plan with no `RETURNING` has an empty schema.
#[test]
fn update_plan_schema_empty_when_no_returning() {
    let input = LogicalPlan::Scan {
        table: "users".into(),
        schema: users_schema(),
        projection: None,
    };
    let plan = LogicalPlan::Update {
        table: "users".into(),
        assignments: vec![(1, lit_i32(99))],
        input: Box::new(input),
        returning: vec![],
        schema: Schema::empty(),
    };
    assert!(plan.schema().is_empty());
}

/// The `display` for an `Insert` plan includes the table name and column
/// indices.
#[test]
fn display_insert_includes_table_and_columns() {
    let source = LogicalPlan::Values {
        rows: vec![vec![lit_i32(1), lit_text("alice")]],
        schema: Schema::new([
            Field::nullable("column1", DataType::Int32),
            Field::nullable("column2", DataType::Text { max_len: None }),
        ])
        .expect("schema ok"),
    };
    let plan = LogicalPlan::Insert {
        table: "users".into(),
        columns: vec![0, 2, 3],
        source: Box::new(source),
        on_conflict: None,
        returning: vec![],
        schema: Schema::empty(),
    };
    let dump = plan.display(0);
    assert!(dump.contains("Insert:"), "got: {dump}");
    assert!(dump.contains("table=users"), "got: {dump}");
    assert!(dump.contains("cols=[0,2,3]"), "got: {dump}");
}

/// The aggregate output schema lists group-by columns first, then
/// aggregate columns.
#[test]
fn aggregate_schema_orders_group_by_then_aggregates() {
    let input_schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("score", DataType::Float64),
    ])
    .expect("schema ok");
    let input = LogicalPlan::Scan {
        table: "users".into(),
        schema: input_schema,
        projection: None,
    };
    let agg_schema = Schema::new([
        Field::nullable("id", DataType::Int32),
        Field::nullable("cnt", DataType::Int64),
    ])
    .expect("schema ok");
    let plan = LogicalPlan::Aggregate {
        input: Box::new(input),
        group_by: vec![col("id", 0, DataType::Int32)],
        aggregates: vec![LogicalAggregateExpr {
            func: AggregateFunc::CountStar,
            arg: None,
            direct_arg: None,
            order_by: None,
            distinct: false,
            output_name: "cnt".into(),
            data_type: DataType::Int64,
        }],
        schema: agg_schema,
    };
    assert_eq!(plan.schema().len(), 2);
    assert_eq!(plan.schema().field_at(0).name, "id");
    assert_eq!(plan.schema().field_at(1).name, "cnt");
}

/// A Join plan's schema is the concatenation of the left and right schemas
/// under outer-join nullability: right columns become nullable in a LEFT JOIN.
#[test]
fn join_schema_concatenates_under_outer_nullability() {
    let left_schema = Schema::new([Field::required("a", DataType::Int32)]).expect("schema ok");
    let right_schema =
        Schema::new([Field::nullable("b", DataType::Float64)]).expect("schema ok");
    let left = LogicalPlan::Scan {
        table: "t1".into(),
        schema: left_schema,
        projection: None,
    };
    let right = LogicalPlan::Scan {
        table: "t2".into(),
        schema: right_schema,
        projection: None,
    };
    // For a LEFT JOIN the right field 'b' is already nullable; left field
    // 'a' stays required.
    let join_schema = Schema::new([
        Field::required("a", DataType::Int32),   // left: stays required
        Field::nullable("b", DataType::Float64), // right: nullable
    ])
    .expect("schema ok");
    let plan = LogicalPlan::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type: LogicalJoinType::LeftOuter,
        condition: LogicalJoinCondition::None,
        schema: join_schema,
    };
    assert_eq!(plan.schema().len(), 2);
    assert!(
        !plan.schema().field_at(0).nullable,
        "left col should be required"
    );
    assert!(
        plan.schema().field_at(1).nullable,
        "right col should be nullable"
    );
}

/// `display()` renders a nested join tree.
#[test]
fn display_renders_join_tree() {
    let s = Schema::new([Field::required("x", DataType::Int32)]).expect("schema ok");
    let scan_a = LogicalPlan::Scan {
        table: "a".into(),
        schema: s.clone(),
        projection: None,
    };
    let scan_b = LogicalPlan::Scan {
        table: "b".into(),
        schema: s,
        projection: None,
    };
    let join_schema = Schema::new([Field::required("x", DataType::Int32)]).expect("schema ok");
    let join = LogicalPlan::Join {
        left: Box::new(scan_a),
        right: Box::new(scan_b),
        join_type: LogicalJoinType::Inner,
        condition: LogicalJoinCondition::On(col("x", 0, DataType::Int32)),
        schema: join_schema,
    };
    let dump = join.display(0);
    assert!(dump.contains("Join[Inner]"), "got: {dump}");
    assert!(dump.contains("ON x"), "got: {dump}");
    assert!(dump.contains("Scan: a"), "got: {dump}");
    assert!(dump.contains("Scan: b"), "got: {dump}");
}

/// `display()` renders the aggregate node with function names.
#[test]
fn display_renders_aggregate_with_function_names() {
    let input = LogicalPlan::Scan {
        table: "t".into(),
        schema: Schema::new([Field::required("v", DataType::Int32)]).expect("schema ok"),
        projection: None,
    };
    let agg_schema =
        Schema::new([Field::nullable("total", DataType::Int64)]).expect("schema ok");
    let plan = LogicalPlan::Aggregate {
        input: Box::new(input),
        group_by: vec![],
        aggregates: vec![LogicalAggregateExpr {
            func: AggregateFunc::Sum,
            arg: Some(col("v", 0, DataType::Int32)),
            direct_arg: None,
            order_by: None,
            distinct: false,
            output_name: "total".into(),
            data_type: DataType::Int64,
        }],
        schema: agg_schema,
    };
    let dump = plan.display(0);
    assert!(dump.contains("sum"), "got: {dump}");
    assert!(dump.contains("total"), "got: {dump}");
}
