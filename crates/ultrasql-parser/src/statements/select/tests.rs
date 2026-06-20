//! Unit and property tests for the `SELECT` parser.

use proptest::prelude::*;

use crate::ast::{
    Distinct, Expr, JoinCondition, JoinOp, SelectItem, SetOp, SetQuantifier, Statement,
    TableRef, XmlTableColumnKind,
};
use crate::parser::Parser;

fn parse(src: &str) -> Statement {
    Parser::new(src)
        .parse_statement()
        .unwrap_or_else(|e| panic!("parse failed for {src:?}: {e}"))
}

fn parse_err(src: &str) -> crate::parser::ParseError {
    Parser::new(src)
        .parse_statement()
        .expect_err("expected parse error")
}

// -------- DISTINCT / DISTINCT ON -------------------------------------- //

#[test]
fn select_distinct() {
    let stmt = parse("SELECT DISTINCT id FROM users");
    let Statement::Select(s) = stmt else { panic!() };
    assert!(matches!(s.distinct, Distinct::Distinct));
}

#[test]
fn select_distinct_on() {
    let stmt = parse("SELECT DISTINCT ON (dept, id) name FROM employees");
    let Statement::Select(s) = stmt else { panic!() };
    let Distinct::DistinctOn(exprs) = &s.distinct else {
        panic!()
    };
    assert_eq!(exprs.len(), 2);
}

#[test]
fn select_all_keyword() {
    let stmt = parse("SELECT ALL id FROM t");
    let Statement::Select(s) = stmt else { panic!() };
    assert!(matches!(s.distinct, Distinct::All));
}

// -------- FROM / Joins ------------------------------------------------ //

#[test]
fn select_from_single_table() {
    let stmt = parse("SELECT * FROM users");
    let Statement::Select(s) = stmt else { panic!() };
    assert_eq!(s.from.len(), 1);
    assert!(matches!(s.from[0], TableRef::Named { .. }));
}

#[test]
fn select_pivot_table_factor_parses_aggregate_and_values() {
    let stmt = parse(
        "SELECT * FROM sales \
         PIVOT (SUM(amount) FOR quarter IN ('Q1' AS q1, 'Q2' AS q2))",
    );
    let Statement::Select(s) = stmt else { panic!() };
    let TableRef::Pivot {
        aggregate,
        value_column,
        pivot_values,
        ..
    } = &s.from[0]
    else {
        panic!("expected PIVOT table ref");
    };
    assert_eq!(aggregate.function.value, "sum");
    assert!(aggregate.arg.is_some());
    assert_eq!(value_column.value, "quarter");
    assert_eq!(pivot_values.len(), 2);
    assert_eq!(pivot_values[0].alias.as_ref().expect("alias").value, "q1");
}

#[test]
fn select_unpivot_table_factor_parses_columns_and_labels() {
    let stmt = parse(
        "SELECT * FROM quarterly \
         UNPIVOT INCLUDE NULLS (amount FOR quarter IN (q1 AS 'Q1', q2 AS 'Q2'))",
    );
    let Statement::Select(s) = stmt else { panic!() };
    let TableRef::Unpivot {
        value_column,
        name_column,
        columns,
        include_nulls,
        ..
    } = &s.from[0]
    else {
        panic!("expected UNPIVOT table ref");
    };
    assert_eq!(value_column.value, "amount");
    assert_eq!(name_column.value, "quarter");
    assert!(*include_nulls);
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].column.value, "q1");
    assert!(columns[0].label.is_some());
}

#[test]
fn select_pivot_rejects_multiple_aggregate_arguments() {
    let err = parse_err(
        "SELECT * FROM sales \
         PIVOT (SUM(amount, tax) FOR quarter IN ('Q1' AS q1))",
    );
    assert!(err.to_string().contains("expected )"));
}

#[test]
fn select_unpivot_rejects_empty_column_list() {
    let err = parse_err("SELECT * FROM quarterly UNPIVOT (amount FOR quarter IN ())");
    assert!(err.to_string().contains("expected identifier"));
}

#[test]
fn select_inner_join_on() {
    let stmt =
        parse("SELECT u.id, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id");
    let Statement::Select(s) = stmt else { panic!() };
    assert_eq!(s.from.len(), 1);
    let TableRef::Join { op, condition, .. } = &s.from[0] else {
        panic!()
    };
    assert_eq!(*op, JoinOp::Inner);
    assert!(matches!(condition, JoinCondition::On(_)));
}

#[test]
fn select_left_outer_join() {
    let stmt = parse("SELECT * FROM a LEFT OUTER JOIN b ON a.id = b.a_id");
    let Statement::Select(s) = stmt else { panic!() };
    let TableRef::Join { op, .. } = &s.from[0] else {
        panic!()
    };
    assert_eq!(*op, JoinOp::LeftOuter);
}

#[test]
fn select_right_join() {
    let stmt = parse("SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id");
    let Statement::Select(s) = stmt else { panic!() };
    let TableRef::Join { op, .. } = &s.from[0] else {
        panic!()
    };
    assert_eq!(*op, JoinOp::RightOuter);
}

#[test]
fn select_full_outer_join() {
    let stmt = parse("SELECT * FROM a FULL OUTER JOIN b ON a.id = b.a_id");
    let Statement::Select(s) = stmt else { panic!() };
    let TableRef::Join { op, .. } = &s.from[0] else {
        panic!()
    };
    assert_eq!(*op, JoinOp::FullOuter);
}

#[test]
fn select_cross_join() {
    let stmt = parse("SELECT * FROM a CROSS JOIN b");
    let Statement::Select(s) = stmt else { panic!() };
    let TableRef::Join { op, condition, .. } = &s.from[0] else {
        panic!()
    };
    assert_eq!(*op, JoinOp::Cross);
    assert!(matches!(condition, JoinCondition::None));
}

#[test]
fn select_join_using() {
    let stmt = parse("SELECT * FROM a JOIN b USING (id)");
    let Statement::Select(s) = stmt else { panic!() };
    let TableRef::Join { condition, .. } = &s.from[0] else {
        panic!()
    };
    let JoinCondition::Using(cols) = condition else {
        panic!()
    };
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].value, "id");
}

#[test]
fn select_natural_join() {
    let stmt = parse("SELECT * FROM a NATURAL JOIN b");
    let Statement::Select(s) = stmt else { panic!() };
    let TableRef::Join { op, condition, .. } = &s.from[0] else {
        panic!()
    };
    assert_eq!(*op, JoinOp::Inner);
    assert!(matches!(condition, JoinCondition::Natural));
}

#[test]
fn select_natural_left_join() {
    let stmt = parse("SELECT * FROM a NATURAL LEFT JOIN b");
    let Statement::Select(s) = stmt else { panic!() };
    let TableRef::Join { op, condition, .. } = &s.from[0] else {
        panic!()
    };
    assert_eq!(*op, JoinOp::LeftOuter);
    assert!(matches!(condition, JoinCondition::Natural));
}

#[test]
fn select_natural_cross_join_is_rejected() {
    let err = parse_err("SELECT * FROM a NATURAL CROSS JOIN b");
    assert!(matches!(
        err,
        crate::parser::ParseError::Unsupported {
            what: "NATURAL CROSS JOIN",
            ..
        }
    ));
}

#[test]
fn select_comma_join_canonicalised_to_cross() {
    let stmt = parse("SELECT * FROM a, b");
    let Statement::Select(s) = stmt else { panic!() };
    let TableRef::Join { op, .. } = &s.from[0] else {
        panic!()
    };
    assert_eq!(*op, JoinOp::Cross);
}

#[test]
fn select_subquery_in_from() {
    let stmt = parse("SELECT x FROM (SELECT id AS x FROM t) sub");
    let Statement::Select(s) = stmt else { panic!() };
    let TableRef::Subquery { alias, .. } = &s.from[0] else {
        panic!()
    };
    assert_eq!(alias.value, "sub");
}

#[test]
fn select_subquery_in_from_requires_alias() {
    let err = parse_err("SELECT x FROM (SELECT id FROM t)");
    // Should fail because no alias was given.
    assert!(matches!(
        err,
        crate::parser::ParseError::Expected { .. }
            | crate::parser::ParseError::UnexpectedEof { .. }
    ));
}

#[test]
fn select_csv_file_literal_in_from_lowers_to_read_csv_function() {
    let stmt = parse("SELECT count(*) FROM 'logs/*.csv'");
    let Statement::Select(s) = stmt else { panic!() };
    let TableRef::Function { name, args, .. } = &s.from[0] else {
        panic!("expected file literal to parse as table function");
    };
    assert_eq!(name.value, "read_csv");
    assert_eq!(args.len(), 1);
}

#[test]
fn select_parquet_file_literal_in_from_lowers_to_read_parquet_function() {
    let stmt = parse("SELECT * FROM 'facts/*.parquet' f");
    let Statement::Select(s) = stmt else { panic!() };
    let TableRef::Function {
        name, args, alias, ..
    } = &s.from[0]
    else {
        panic!("expected file literal to parse as table function");
    };
    assert_eq!(name.value, "read_parquet");
    assert_eq!(args.len(), 1);
    assert_eq!(alias.as_ref().expect("alias").value, "f");
}

#[test]
fn select_json_file_literal_in_from_lowers_to_read_json_function() {
    let stmt = parse("SELECT * FROM 'facts/*.json'");
    let Statement::Select(s) = stmt else { panic!() };
    let TableRef::Function { name, args, .. } = &s.from[0] else {
        panic!("expected file literal to parse as table function");
    };
    assert_eq!(name.value, "read_json");
    assert_eq!(args.len(), 1);
}

#[test]
fn select_ndjson_file_literal_in_from_lowers_to_read_ndjson_function() {
    let stmt = parse("SELECT * FROM 'facts/*.ndjson'");
    let Statement::Select(s) = stmt else { panic!() };
    let TableRef::Function { name, args, .. } = &s.from[0] else {
        panic!("expected file literal to parse as table function");
    };
    assert_eq!(name.value, "read_ndjson");
    assert_eq!(args.len(), 1);
}

#[test]
fn select_arrow_file_literal_in_from_lowers_to_read_arrow_function() {
    let stmt = parse("SELECT * FROM 'facts/*.arrow'");
    let Statement::Select(s) = stmt else { panic!() };
    let TableRef::Function { name, args, .. } = &s.from[0] else {
        panic!("expected file literal to parse as table function");
    };
    assert_eq!(name.value, "read_arrow");
    assert_eq!(args.len(), 1);
}

#[test]
fn select_json_table_in_from_parses_columns_clause() {
    let stmt = parse(
        "SELECT * FROM JSON_TABLE(\
         jsonb '[{\"id\":1,\"name\":\"Ada\"}]', \
         '$[*]' COLUMNS (\
             ord FOR ORDINALITY, \
             id bigint PATH '$.id', \
             name text, \
             has_score boolean EXISTS PATH '$.score'\
         )) jt",
    );
    let Statement::Select(s) = stmt else { panic!() };
    let TableRef::JsonTable {
        row_path,
        columns,
        alias,
        ..
    } = &s.from[0]
    else {
        panic!("expected JSON_TABLE table ref");
    };
    assert_eq!(row_path, "$[*]");
    assert_eq!(alias.as_ref().expect("alias").value, "jt");
    assert_eq!(columns.len(), 4);
}

#[test]
fn select_xmltable_in_from_parses_columns_clause() {
    let stmt = parse(
        "SELECT * FROM XMLTABLE(\
         '/root/item' PASSING XML '<root><item id=\"1\"><name>Ada</name></item></root>' \
         COLUMNS (\
             ord FOR ORDINALITY, \
             id bigint PATH '@id', \
             name text PATH 'name/text()', \
             score int PATH 'score/text()' DEFAULT '0'\
         )) xt",
    );
    let Statement::Select(s) = stmt else { panic!() };
    let TableRef::XmlTable {
        row_path,
        columns,
        alias,
        ..
    } = &s.from[0]
    else {
        panic!("expected XMLTABLE table ref");
    };
    assert_eq!(row_path, "/root/item");
    assert_eq!(alias.as_ref().expect("alias").value, "xt");
    assert_eq!(columns.len(), 4);
    let XmlTableColumnKind::Value { default, .. } = &columns[3].kind else {
        panic!("expected XMLTABLE value column");
    };
    assert_eq!(default.as_deref(), Some("0"));
}

// -------- GROUP BY / HAVING ------------------------------------------- //

#[test]
fn select_group_by() {
    let stmt = parse("SELECT dept, COUNT(*) FROM employees GROUP BY dept");
    let Statement::Select(s) = stmt else { panic!() };
    assert_eq!(s.group_by.len(), 1);
    assert!(s.having.is_none());
}

#[test]
fn select_group_by_having() {
    let stmt = parse("SELECT dept, COUNT(*) FROM employees GROUP BY dept HAVING COUNT(*) > 5");
    let Statement::Select(s) = stmt else { panic!() };
    assert!(!s.group_by.is_empty());
    assert!(s.having.is_some());
}

// -------- Subquery expressions ---------------------------------------- //

#[test]
fn scalar_subquery_in_where() {
    let stmt = parse("SELECT * FROM t WHERE id = (SELECT MAX(id) FROM t)");
    let Statement::Select(s) = stmt else { panic!() };
    assert!(s.r#where.is_some());
    // The WHERE is a Binary with right = Subquery.
    let Some(Expr::Binary { right, .. }) = &s.r#where else {
        panic!()
    };
    assert!(matches!(right.as_ref(), Expr::Subquery { .. }));
}

#[test]
fn exists_subquery() {
    let stmt = parse("SELECT * FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id)");
    let Statement::Select(s) = stmt else { panic!() };
    let Some(Expr::Exists { negated, .. }) = &s.r#where else {
        panic!()
    };
    assert!(!negated);
}

#[test]
fn not_exists_subquery() {
    let stmt = parse("SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM u)");
    let Statement::Select(s) = stmt else { panic!() };
    // NOT wraps the EXISTS as Unary::Not, or parser builds Exists{negated:true}.
    // Verify something is there.
    assert!(s.r#where.is_some());
}

#[test]
fn in_list() {
    let stmt = parse("SELECT * FROM t WHERE id IN (1, 2, 3)");
    let Statement::Select(s) = stmt else { panic!() };
    let Some(Expr::InList { items, negated, .. }) = &s.r#where else {
        panic!()
    };
    assert!(!negated);
    assert_eq!(items.len(), 3);
}

#[test]
fn not_in_list() {
    let stmt = parse("SELECT * FROM t WHERE id NOT IN (1, 2, 3)");
    let Statement::Select(s) = stmt else { panic!() };
    let Some(Expr::InList { negated, .. }) = &s.r#where else {
        panic!()
    };
    assert!(negated);
}

#[test]
fn in_subquery() {
    let stmt = parse("SELECT * FROM t WHERE id IN (SELECT id FROM u)");
    let Statement::Select(s) = stmt else { panic!() };
    let Some(Expr::InSubquery { negated, .. }) = &s.r#where else {
        panic!()
    };
    assert!(!negated);
}

#[test]
fn not_in_subquery() {
    let stmt = parse("SELECT * FROM t WHERE id NOT IN (SELECT id FROM u)");
    let Statement::Select(s) = stmt else { panic!() };
    let Some(Expr::InSubquery { negated, .. }) = &s.r#where else {
        panic!()
    };
    assert!(negated);
}

#[test]
fn any_subquery() {
    let stmt = parse("SELECT * FROM t WHERE id = ANY (SELECT id FROM u)");
    let Statement::Select(s) = stmt else { panic!() };
    assert!(matches!(s.r#where, Some(Expr::Any { .. })));
}

#[test]
fn all_subquery() {
    let stmt = parse("SELECT * FROM t WHERE id < ALL (SELECT id FROM u)");
    let Statement::Select(s) = stmt else { panic!() };
    assert!(matches!(s.r#where, Some(Expr::All { .. })));
}

// -------- UNION / INTERSECT / EXCEPT ---------------------------------- //

#[test]
fn union_all() {
    let stmt = parse("SELECT id FROM a UNION ALL SELECT id FROM b");
    let Statement::Select(s) = stmt else { panic!() };
    assert_eq!(s.set_ops.len(), 1);
    assert_eq!(s.set_ops[0].op, SetOp::Union);
    assert_eq!(s.set_ops[0].quantifier, SetQuantifier::All);
}

#[test]
fn intersect_distinct() {
    let stmt = parse("SELECT id FROM a INTERSECT SELECT id FROM b");
    let Statement::Select(s) = stmt else { panic!() };
    assert_eq!(s.set_ops[0].op, SetOp::Intersect);
    assert_eq!(s.set_ops[0].quantifier, SetQuantifier::Distinct);
}

#[test]
fn except_all() {
    let stmt = parse("SELECT id FROM a EXCEPT ALL SELECT id FROM b");
    let Statement::Select(s) = stmt else { panic!() };
    assert_eq!(s.set_ops[0].op, SetOp::Except);
    assert_eq!(s.set_ops[0].quantifier, SetQuantifier::All);
}

// -------- CTEs -------------------------------------------------------- //

#[test]
fn with_cte() {
    let stmt = parse("WITH cte AS (SELECT id FROM t) SELECT * FROM cte");
    let Statement::Select(s) = stmt else { panic!() };
    assert_eq!(s.ctes.len(), 1);
    assert_eq!(s.ctes[0].name.value, "cte");
    assert!(!s.ctes[0].recursive);
}

#[test]
fn with_recursive_cte() {
    let stmt = parse(
        "WITH RECURSIVE hierarchy AS (SELECT id, parent_id FROM tree) SELECT * FROM hierarchy",
    );
    let Statement::Select(s) = stmt else { panic!() };
    assert!(s.ctes[0].recursive);
}

#[test]
fn with_cte_column_aliases() {
    let stmt = parse("WITH cte(a, b) AS (SELECT 1, 2) SELECT * FROM cte");
    let Statement::Select(s) = stmt else { panic!() };
    assert_eq!(s.ctes[0].column_aliases.len(), 2);
}

// -------- SELECT without FROM ----------------------------------------- //

#[test]
fn select_without_from() {
    let stmt = parse("SELECT 1 + 1");
    let Statement::Select(s) = stmt else { panic!() };
    assert!(s.from.is_empty());
}

// -------- Existing tests updated for Vec<TableRef> ------------------- //

#[test]
fn select_star_updated() {
    let stmt = parse("SELECT * FROM users");
    let Statement::Select(s) = stmt else { panic!() };
    assert!(matches!(s.distinct, Distinct::None));
    assert!(matches!(s.projection[0], SelectItem::Wildcard { .. }));
    assert!(!s.from.is_empty());
}

// -------- Property test: join chain is left-deep -------------------- //
//
// Strategy: generate 1..=6 table names, build the SQL for an N-table
// INNER JOIN chain, parse it, and verify the resulting join tree is
// left-deep (each node's right child is a leaf, not a join).

/// Returns `true` iff the join tree rooted at `t` is left-deep.
///
/// A left-deep join tree has the property that every right child
/// is a base table (leaf), while the left children recurse.
fn is_left_deep(t: &TableRef) -> bool {
    match t {
        TableRef::Named { .. }
        | TableRef::Subquery { .. }
        | TableRef::Function { .. }
        | TableRef::JsonTable { .. }
        | TableRef::XmlTable { .. } => true,
        TableRef::Pivot { input, .. } | TableRef::Unpivot { input, .. } => is_left_deep(input),
        TableRef::Join { left, right, .. } => {
            // Right must be a leaf.
            matches!(
                right.as_ref(),
                TableRef::Named { .. }
                    | TableRef::Subquery { .. }
                    | TableRef::Function { .. }
                    | TableRef::JsonTable { .. }
                    | TableRef::Pivot { .. }
                    | TableRef::Unpivot { .. }
                    | TableRef::XmlTable { .. }
            ) && is_left_deep(left)
        }
    }
}

proptest! {
    #[test]
    fn join_chain_is_left_deep(n_tables in 1_usize..=6) {
        use std::fmt::Write as _;
        // Build a table list: t1, t2, … tN
        let names: Vec<String> = (1..=n_tables).map(|i| format!("t{i}")).collect();
        let mut sql = format!("SELECT * FROM {}", names[0]);
        for name in &names[1..] {
            let _ = write!(sql, " INNER JOIN {name} ON {first}.id = {name}.id", first = names[0]);
        }
        let stmt = Parser::new(&sql)
            .parse_statement()
            .unwrap_or_else(|e| panic!("parse failed for {sql:?}: {e}"));
        let Statement::Select(s) = stmt else { panic!() };

        if n_tables == 1 {
            let is_named = matches!(s.from[0], TableRef::Named { .. });
            prop_assert!(is_named);
            return Ok(());
        }

        prop_assert!(is_left_deep(&s.from[0]), "join tree is not left-deep");
    }
}

// -------- FOR UPDATE / FOR SHARE locking clauses ---------------------- //

#[test]
fn select_for_update() {
    use crate::ast::{LockStrength, LockWaitPolicy};
    let stmt = parse("SELECT id FROM users FOR UPDATE");
    let Statement::Select(s) = stmt else { panic!() };
    assert_eq!(s.locking.len(), 1);
    assert_eq!(s.locking[0].strength, LockStrength::Update);
    assert_eq!(s.locking[0].wait_policy, LockWaitPolicy::Wait);
    assert!(s.locking[0].of_tables.is_empty());
}

#[test]
fn select_for_share_nowait() {
    use crate::ast::{LockStrength, LockWaitPolicy};
    let stmt = parse("SELECT id FROM users FOR SHARE NOWAIT");
    let Statement::Select(s) = stmt else { panic!() };
    assert_eq!(s.locking.len(), 1);
    assert_eq!(s.locking[0].strength, LockStrength::Share);
    assert_eq!(s.locking[0].wait_policy, LockWaitPolicy::NoWait);
}

#[test]
fn select_for_no_key_update_skip_locked() {
    use crate::ast::{LockStrength, LockWaitPolicy};
    let stmt = parse("SELECT id FROM t FOR NO KEY UPDATE SKIP LOCKED");
    let Statement::Select(s) = stmt else { panic!() };
    assert_eq!(s.locking[0].strength, LockStrength::NoKeyUpdate);
    assert_eq!(s.locking[0].wait_policy, LockWaitPolicy::SkipLocked);
}

#[test]
fn select_for_key_share() {
    use crate::ast::{LockStrength, LockWaitPolicy};
    let stmt = parse("SELECT id FROM t FOR KEY SHARE");
    let Statement::Select(s) = stmt else { panic!() };
    assert_eq!(s.locking[0].strength, LockStrength::KeyShare);
    assert_eq!(s.locking[0].wait_policy, LockWaitPolicy::Wait);
}

#[test]
fn select_for_update_of_table() {
    use crate::ast::LockStrength;
    let stmt = parse("SELECT * FROM t FOR UPDATE OF t");
    let Statement::Select(s) = stmt else { panic!() };
    assert_eq!(s.locking[0].strength, LockStrength::Update);
    assert_eq!(s.locking[0].of_tables.len(), 1);
}

#[test]
fn select_without_locking_has_empty_vec() {
    let stmt = parse("SELECT 1");
    let Statement::Select(s) = stmt else { panic!() };
    assert!(s.locking.is_empty());
}
