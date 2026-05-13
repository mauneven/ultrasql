//! Subquery decorrelation rewrite rule.
//!
//! [`SubqueryDecorrelation`] transforms correlated subqueries (those that
//! reference columns from an outer query) into equivalent join expressions,
//! eliminating the need for repeated inner execution.
//!
//! ## Current status (v0.6 — stub)
//!
//! The rule is scaffolded here but returns `None` on every input. The full
//! implementation requires:
//!
//! 1. A `ScalarExpr::Subquery` or similar variant in the planner that
//!    carries the inner plan and the correlation binding set.
//! 2. Apply-to-join rewriting: a correlated subquery in a `Filter` becomes
//!    a lateral join, which can be further transformed into a regular
//!    (hash or nest-loop) join when the correlation is limited to the
//!    join key.
//! 3. Dependent-join elimination for aggregated subqueries
//!    (`WHERE x = (SELECT MAX(...) FROM ... WHERE t.id = ...)` pattern).
//!
//! This work is deferred to v0.7 together with the binder's subquery support.
//!
//! TODO(v0.7): implement correlated-subquery → join decorrelation pass.

use ultrasql_planner::LogicalPlan;

use crate::error::OptimizeError;
use crate::rules::RewriteRule;

/// Subquery decorrelation — stub returning `None` until v0.7.
#[derive(Debug)]
pub struct SubqueryDecorrelation;

impl RewriteRule for SubqueryDecorrelation {
    fn name(&self) -> &'static str {
        "subquery_decorrelation"
    }

    fn apply(&self, _plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
        // TODO(v0.7): implement subquery decorrelation.
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_planner::LogicalPlan;

    use super::*;
    use crate::rules::RewriteRule;

    fn scan() -> LogicalPlan {
        LogicalPlan::Scan {
            table: "t".into(),
            schema: Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok"),
            projection: None,
        }
    }

    #[test]
    fn stub_returns_none_for_scan() {
        let rule = SubqueryDecorrelation;
        assert!(rule.apply(&scan()).expect("no error").is_none());
    }

    #[test]
    fn stub_returns_none_for_filter() {
        use ultrasql_core::Value;
        use ultrasql_planner::ScalarExpr;

        let plan = LogicalPlan::Filter {
            input: Box::new(scan()),
            predicate: ScalarExpr::Literal {
                value: Value::Bool(true),
                data_type: DataType::Bool,
            },
        };
        let rule = SubqueryDecorrelation;
        assert!(rule.apply(&plan).expect("no error").is_none());
    }

    #[test]
    fn stub_name_is_stable() {
        let rule = SubqueryDecorrelation;
        assert_eq!(rule.name(), "subquery_decorrelation");
    }
}
