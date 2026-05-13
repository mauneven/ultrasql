//! Sort-elimination advisory rule.
//!
//! [`SortElimination`] detects `Sort { input: Scan { table, .. }, keys }` nodes
//! where an available index on `table` covers the sort keys in the same order
//! and direction as the `Sort`.
//!
//! ## Advisory model (v0.6)
//!
//! `LogicalPlan::Scan` does not carry an `index_hint` field, so this rule
//! cannot directly annotate the scan with an index choice. Instead, when
//! elimination is possible the rule returns `Ok(None)` and emits a
//! `tracing::debug!` event with the table name, index name, and sort keys.
//! The physical-plan layer (`enumeration/physical_selection.rs`) already
//! recognises `IndexHint` and can select `IndexScan` independently; this rule's
//! advisory log ensures that the planner's decision is traceable.
//!
//! ## Full implementation note
//!
//! A complete sort-elimination pass would annotate `LogicalPlan::Scan` with the
//! chosen index so that the physical-plan layer can emit a directed `IndexScan`
//! without reconsidering costs. That requires either:
//! - A new `LogicalPlan::Scan { index_hint: Option<IndexHint>, .. }` field, or
//! - A dedicated `LogicalPlan::IndexScan` variant.
//!
//! Both changes require modifying `ultrasql-planner`, which is out of scope for
//! v0.6. This advisory rule is the placeholder that records eliminability and
//! exercises the detection logic.
//!
//! TODO(v0.7): wire the detected index into the scan node when `LogicalPlan`
//! carries the field.

use ultrasql_planner::{LogicalPlan, SortKey};

use crate::enumeration::physical_selection::IndexHint;
use crate::error::OptimizeError;
use crate::rules::RewriteRule;

/// Advisory sort-elimination rule.
///
/// Returns `Ok(None)` (no rewrite) but emits a `tracing::debug!` event when
/// the sort keys can be satisfied by an available index on the scanned table.
///
/// ## Usage
///
/// ```rust
/// use ultrasql_optimizer::rules::SortElimination;
/// use ultrasql_optimizer::enumeration::IndexHint;
///
/// // Inject the available indexes at construction time.
/// let rule = SortElimination::new(vec![
///     IndexHint { name: "idx_score".into(), columns: vec![2], unique: false, method: "btree" },
/// ]);
/// ```
#[derive(Debug)]
pub struct SortElimination {
    /// Index catalog injected at construction time.
    indexes: Vec<IndexHint>,
}

impl SortElimination {
    /// Create a `SortElimination` rule with the given index catalog.
    #[must_use]
    pub const fn new(indexes: Vec<IndexHint>) -> Self {
        Self { indexes }
    }

    /// Returns `true` when the available index `hint` covers `keys` exactly:
    /// same columns in the same order with matching `asc` direction.
    fn index_covers_sort(hint: &IndexHint, keys: &[SortKey]) -> bool {
        if keys.is_empty() || hint.columns.len() < keys.len() {
            return false;
        }
        // The index must cover each sort key in its leading columns, in order.
        for (i, key) in keys.iter().enumerate() {
            let idx_col = hint.columns[i];
            // The sort key must reference this column index directly.
            if let ultrasql_planner::ScalarExpr::Column { index, .. } = &key.expr {
                if *index != idx_col {
                    return false;
                }
            } else {
                // Non-column sort key (expression) — cannot use index.
                return false;
            }
            // ASC is the natural B-tree order. DESC requires a backward scan;
            // we treat it as non-eliminable in v0.6 since `LogicalPlan::Scan`
            // cannot express scan direction.
            if !key.asc {
                return false;
            }
        }
        true
    }
}

impl RewriteRule for SortElimination {
    fn name(&self) -> &'static str {
        "sort_elimination"
    }

    fn apply(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>, OptimizeError> {
        // Match `Sort { input: Scan { table, .. }, keys }`.
        if let LogicalPlan::Sort { input, keys } = plan {
            if let LogicalPlan::Scan { table, .. } = input.as_ref() {
                for hint in &self.indexes {
                    if Self::index_covers_sort(hint, keys) {
                        tracing::debug!(
                            table = %table,
                            index = %hint.name,
                            keys = ?keys.iter().map(|k| format!("{} {}", k.expr, if k.asc { "ASC" } else { "DESC" })).collect::<Vec<_>>(),
                            "sort_elimination: sort on '{}' can be eliminated via index '{}'",
                            table,
                            hint.name,
                        );
                        // Advisory only: no structural rewrite in v0.6.
                        // Return None so the driver continues to the next rule.
                        return Ok(None);
                    }
                }
            }
        }

        // Not applicable — recurse into children for nested Sort nodes.
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                let new_input = self.apply(input)?;
                Ok(new_input.map(|i| LogicalPlan::Filter {
                    input: Box::new(i),
                    predicate: predicate.clone(),
                }))
            }
            LogicalPlan::Project {
                input,
                exprs,
                schema,
            } => {
                let new_input = self.apply(input)?;
                Ok(new_input.map(|i| LogicalPlan::Project {
                    input: Box::new(i),
                    exprs: exprs.clone(),
                    schema: schema.clone(),
                }))
            }
            LogicalPlan::Sort { input, keys } => {
                let new_input = self.apply(input)?;
                Ok(new_input.map(|i| LogicalPlan::Sort {
                    input: Box::new(i),
                    keys: keys.clone(),
                }))
            }
            LogicalPlan::Limit { input, n, offset } => {
                let new_input = self.apply(input)?;
                Ok(new_input.map(|i| LogicalPlan::Limit {
                    input: Box::new(i),
                    n: *n,
                    offset: *offset,
                }))
            }
            _ => Ok(None),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::{LogicalPlan, ScalarExpr, SortKey};

    use super::*;
    use crate::enumeration::physical_selection::IndexHint;
    use crate::rules::RewriteRule;

    fn scan(table: &str) -> LogicalPlan {
        LogicalPlan::Scan {
            table: table.into(),
            schema: Schema::new(vec![
                Field::required("id", DataType::Int32),
                Field::nullable("score", DataType::Float64),
                Field::nullable("name", DataType::Text { max_len: None }),
            ])
            .expect("schema ok"),
            projection: None,
        }
    }

    fn col_key(name: &str, idx: usize, asc: bool) -> SortKey {
        SortKey {
            expr: ScalarExpr::Column {
                name: name.into(),
                index: idx,
                data_type: DataType::Int32,
            },
            asc,
            nulls_first: false,
        }
    }

    fn expr_key() -> SortKey {
        SortKey {
            expr: ScalarExpr::Binary {
                op: ultrasql_planner::BinaryOp::Add,
                left: Box::new(ScalarExpr::Column {
                    name: "id".into(),
                    index: 0,
                    data_type: DataType::Int32,
                }),
                right: Box::new(ScalarExpr::Literal {
                    value: Value::Int32(1),
                    data_type: DataType::Int32,
                }),
                data_type: DataType::Int32,
            },
            asc: true,
            nulls_first: false,
        }
    }

    fn idx(name: &str, columns: Vec<usize>) -> IndexHint {
        IndexHint {
            name: name.into(),
            columns,
            unique: false,
            method: "btree",
        }
    }

    // -----------------------------------------------------------------------
    // Rule name stability
    // -----------------------------------------------------------------------

    #[test]
    fn rule_name_is_stable() {
        let rule = SortElimination::new(vec![]);
        assert_eq!(rule.name(), "sort_elimination");
    }

    // -----------------------------------------------------------------------
    // Recognises eliminable sort
    // -----------------------------------------------------------------------

    #[test]
    fn sort_eliminable_when_index_covers_asc_key() {
        let rule = SortElimination::new(vec![idx("idx_id", vec![0])]);
        let plan = LogicalPlan::Sort {
            input: Box::new(scan("t")),
            keys: vec![col_key("id", 0, true)],
        };
        // Rule should return None (advisory only, no structural rewrite).
        let result = rule.apply(&plan).expect("no error");
        assert!(
            result.is_none(),
            "advisory rule returns None even when eliminable"
        );
    }

    #[test]
    fn sort_eliminable_with_multi_column_index() {
        let rule = SortElimination::new(vec![idx("idx_id_score", vec![0, 1])]);
        let plan = LogicalPlan::Sort {
            input: Box::new(scan("t")),
            keys: vec![col_key("id", 0, true), col_key("score", 1, true)],
        };
        let result = rule.apply(&plan).expect("no error");
        assert!(result.is_none(), "advisory rule returns None");
    }

    // -----------------------------------------------------------------------
    // Does NOT eliminate when index does not cover sort
    // -----------------------------------------------------------------------

    #[test]
    fn sort_not_eliminated_when_no_matching_index() {
        let rule = SortElimination::new(vec![idx("idx_score", vec![1])]);
        let plan = LogicalPlan::Sort {
            input: Box::new(scan("t")),
            keys: vec![col_key("id", 0, true)],
        };
        let result = rule.apply(&plan).expect("no error");
        assert!(result.is_none(), "no index covers sort key[0]=id");
    }

    #[test]
    fn sort_not_eliminated_with_empty_index_catalog() {
        let rule = SortElimination::new(vec![]);
        let plan = LogicalPlan::Sort {
            input: Box::new(scan("t")),
            keys: vec![col_key("id", 0, true)],
        };
        let result = rule.apply(&plan).expect("no error");
        assert!(result.is_none());
    }

    #[test]
    fn sort_not_eliminated_for_desc_key() {
        // B-tree backward scan not supported in v0.6.
        let rule = SortElimination::new(vec![idx("idx_id", vec![0])]);
        let plan = LogicalPlan::Sort {
            input: Box::new(scan("t")),
            keys: vec![col_key("id", 0, false)], // DESC
        };
        let result = rule.apply(&plan).expect("no error");
        assert!(result.is_none());
    }

    #[test]
    fn sort_not_eliminated_for_expression_key() {
        let rule = SortElimination::new(vec![idx("idx_id", vec![0])]);
        let plan = LogicalPlan::Sort {
            input: Box::new(scan("t")),
            keys: vec![expr_key()],
        };
        let result = rule.apply(&plan).expect("no error");
        assert!(result.is_none());
    }

    // -----------------------------------------------------------------------
    // No-op on non-Sort plans
    // -----------------------------------------------------------------------

    #[test]
    fn no_op_on_plain_scan() {
        let rule = SortElimination::new(vec![idx("idx_id", vec![0])]);
        let plan = scan("t");
        let result = rule.apply(&plan).expect("no error");
        assert!(result.is_none());
    }

    // -----------------------------------------------------------------------
    // Index partial-prefix check
    // -----------------------------------------------------------------------

    #[test]
    fn index_prefix_covers_subset_of_sort_keys() {
        // Index covers [id, score]; sort only asks for [id]. Should be eliminable.
        let rule = SortElimination::new(vec![idx("idx_id_score", vec![0, 1])]);
        let plan = LogicalPlan::Sort {
            input: Box::new(scan("t")),
            keys: vec![col_key("id", 0, true)],
        };
        let result = rule.apply(&plan).expect("no error");
        assert!(result.is_none(), "index prefix covers the sort key");
    }

    #[test]
    fn sort_requires_more_cols_than_index_provides() {
        // Sort on [id, score, name] but index only covers [id, score].
        let rule = SortElimination::new(vec![idx("idx_id_score", vec![0, 1])]);
        let plan = LogicalPlan::Sort {
            input: Box::new(scan("t")),
            keys: vec![
                col_key("id", 0, true),
                col_key("score", 1, true),
                col_key("name", 2, true),
            ],
        };
        let result = rule.apply(&plan).expect("no error");
        assert!(
            result.is_none(),
            "index doesn't cover all 3 sort keys — but advisory returns None anyway"
        );
    }
}
