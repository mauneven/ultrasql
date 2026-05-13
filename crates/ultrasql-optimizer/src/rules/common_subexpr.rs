//! Common-subexpression elimination (CSE) rewrite rule.
//!
//! [`CommonSubExprElimination`] identifies scalar sub-expressions that appear
//! more than once in the plan and replaces repeated occurrences with a
//! reference to a single computed column.
//!
//! ## Current status (v0.6 — stub)
//!
//! The rule is scaffolded here but returns `None` on every input. A full
//! implementation requires:
//!
//! 1. An expression-identity hash that ignores node identity (pointer
//!    equality) and compares structural equality.
//! 2. A pass that walks the plan tree and counts expression frequencies.
//! 3. A rewrite pass that lifts frequent sub-expressions into a `Project`
//!    node above the first computation site and replaces subsequent
//!    occurrences with `Column { index }` references.
//!
//! This work is deferred to v0.7 where the vectorized execution layer can
//! benefit from it.
//!
//! TODO(v0.7): implement full CSE pass.

use ultrasql_planner::LogicalPlan;

use crate::error::OptimizeError;
use crate::rules::RewriteRule;

/// Common-subexpression elimination — stub returning `None` until v0.7.
#[derive(Debug)]
pub struct CommonSubExprElimination;

impl RewriteRule for CommonSubExprElimination {
    fn name(&self) -> &'static str {
        "common_subexpr_elimination"
    }

    fn apply(&self, _plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
        // TODO(v0.7): implement CSE.
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
            schema: Schema::new([Field::required("x", DataType::Int32)]).expect("schema ok"),
            projection: None,
        }
    }

    #[test]
    fn stub_returns_none_for_scan() {
        let rule = CommonSubExprElimination;
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
        let rule = CommonSubExprElimination;
        assert!(rule.apply(&plan).expect("no error").is_none());
    }

    #[test]
    fn stub_name_is_stable() {
        let rule = CommonSubExprElimination;
        assert_eq!(rule.name(), "common_subexpr_elimination");
    }
}
