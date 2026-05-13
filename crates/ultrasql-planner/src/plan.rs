//! Logical plan tree.
//!
//! The logical plan is the binder's output and the optimizer's input.
//! It is type-checked but not physical: it names *what* to compute, not
//! *how*. Each variant produces a [`Schema`] queryable through
//! [`LogicalPlan::schema`]; an EXPLAIN-style indented dump is available
//! through [`LogicalPlan::display`].

use std::fmt;

use ultrasql_core::Schema;

use crate::expr::ScalarExpr;

/// A sort key for `ORDER BY`.
#[derive(Clone, Debug, PartialEq)]
pub struct SortKey {
    /// Sort expression (resolved against the input schema).
    pub expr: ScalarExpr,
    /// `true` for `ASC`, `false` for `DESC`.
    pub asc: bool,
    /// Whether NULLs sort first.
    pub nulls_first: bool,
}

/// The bound, type-checked logical plan tree.
#[derive(Clone, Debug, PartialEq)]
pub enum LogicalPlan {
    /// Table scan. The `projection` field is reserved for the
    /// optimizer's projection pushdown; the binder always emits
    /// `None` so the scan returns the table's natural column order.
    Scan {
        /// Case-folded table name.
        table: String,
        /// Output schema (table schema, possibly already projected).
        schema: Schema,
        /// Optional list of column indices to scan. `None` means "all
        /// columns in natural order".
        projection: Option<Vec<usize>>,
    },

    /// Filter rows by a boolean predicate. The input's schema flows
    /// through unchanged.
    Filter {
        /// Input plan.
        input: Box<Self>,
        /// Boolean-valued predicate, bound against `input.schema()`.
        predicate: ScalarExpr,
    },

    /// Project a tuple of expressions out of the input, each with an
    /// output name.
    Project {
        /// Input plan.
        input: Box<Self>,
        /// Output expressions paired with their column names.
        exprs: Vec<(ScalarExpr, String)>,
        /// Output schema, derived from `exprs`.
        schema: Schema,
    },

    /// `LIMIT n OFFSET m`.
    Limit {
        /// Input plan.
        input: Box<Self>,
        /// Maximum number of rows to return.
        n: u64,
        /// Number of rows to skip before counting toward the limit.
        offset: u64,
    },

    /// Sort by a list of keys.
    Sort {
        /// Input plan.
        input: Box<Self>,
        /// Sort keys, evaluated left-to-right.
        keys: Vec<SortKey>,
    },

    /// A no-row source. Used for queries with constant-false predicates
    /// and for the placeholder produced when a statement is a `SELECT`
    /// with no `FROM`.
    Empty {
        /// Output schema (may be empty).
        schema: Schema,
    },
}

impl LogicalPlan {
    /// The schema of rows produced by this plan node.
    #[must_use]
    pub fn schema(&self) -> &Schema {
        match self {
            Self::Scan { schema, .. } | Self::Project { schema, .. } | Self::Empty { schema } => {
                schema
            }
            Self::Filter { input, .. } | Self::Limit { input, .. } | Self::Sort { input, .. } => {
                input.schema()
            }
        }
    }

    /// Render this plan in an indented EXPLAIN-style tree, where every
    /// child line is indented by two additional spaces.
    ///
    /// `indent` is the column the *root* node's text begins at.
    #[must_use]
    pub fn display(&self, indent: usize) -> String {
        let mut out = String::new();
        self.display_into(indent, &mut out);
        out
    }

    fn display_into(&self, indent: usize, out: &mut String) {
        let pad = " ".repeat(indent);
        match self {
            Self::Scan { table, .. } => {
                out.push_str(&pad);
                out.push_str("Scan: ");
                out.push_str(table);
                out.push('\n');
            }
            Self::Filter { input, predicate } => {
                out.push_str(&pad);
                out.push_str("Filter: ");
                let _ = fmt::write(out, format_args!("{predicate}"));
                out.push('\n');
                input.display_into(indent + 2, out);
            }
            Self::Project { input, exprs, .. } => {
                out.push_str(&pad);
                out.push_str("Project: ");
                for (i, (e, n)) in exprs.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    let _ = fmt::write(out, format_args!("{e} AS {n}"));
                }
                out.push('\n');
                input.display_into(indent + 2, out);
            }
            Self::Limit { input, n, offset } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("Limit: n={n}, offset={offset}\n"));
                input.display_into(indent + 2, out);
            }
            Self::Sort { input, keys } => {
                out.push_str(&pad);
                out.push_str("Sort: ");
                for (i, k) in keys.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    let dir = if k.asc { "ASC" } else { "DESC" };
                    let nulls = if k.nulls_first {
                        "NULLS FIRST"
                    } else {
                        "NULLS LAST"
                    };
                    let _ = fmt::write(out, format_args!("{} {dir} {nulls}", k.expr));
                }
                out.push('\n');
                input.display_into(indent + 2, out);
            }
            Self::Empty { .. } => {
                out.push_str(&pad);
                out.push_str("Empty\n");
            }
        }
    }
}

impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display(0))
    }
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field};

    use super::*;

    fn users_schema() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::nullable("score", DataType::Float64),
        ])
        .expect("schema invariants hold for test fixture")
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
}
