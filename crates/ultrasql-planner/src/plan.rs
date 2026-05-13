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

/// Conflict target resolved to column indices in the target table's schema.
///
/// An empty `columns` list means the conflict target was absent (only valid
/// for `DO NOTHING`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConflictTarget {
    /// 0-based indices into the target table's schema.
    pub columns: Vec<usize>,
}

/// The resolved `ON CONFLICT` clause of a logical `Insert` plan node.
///
/// `EXCLUDED` column references inside `DoUpdate::assignments` are not
/// supported in v0.2; the binder rejects them with
/// [`crate::error::PlanError::NotSupported`].
#[derive(Clone, Debug, PartialEq)]
pub enum LogicalOnConflict {
    /// `ON CONFLICT [target] DO NOTHING`.
    DoNothing {
        /// Optional conflict target.
        target: Option<ConflictTarget>,
    },
    /// `ON CONFLICT target DO UPDATE SET …`.
    DoUpdate {
        /// Conflict target (must be non-empty).
        target: ConflictTarget,
        /// `(column-index, new-value-expression)` pairs.
        assignments: Vec<(usize, ScalarExpr)>,
        /// Optional `WHERE` filter applied to the existing row before
        /// performing the update.
        r#where: Option<ScalarExpr>,
    },
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

    /// A literal row set produced by a `VALUES` clause.
    ///
    /// All rows must have the same arity (enforced by the binder). The
    /// output schema uses PostgreSQL-compatible synthetic column names
    /// `column1`, `column2`, … Column types are the `numeric_join` of
    /// all cells in the same column across all rows; columns that are
    /// entirely NULL have type `DataType::Null`.
    Values {
        /// One inner `Vec` per row; all inner `Vec`s have the same length.
        rows: Vec<Vec<ScalarExpr>>,
        /// Output schema inferred from the rows.
        schema: Schema,
    },

    /// Insert rows into a table.
    ///
    /// The `source` child plan produces the rows to insert. The binder
    /// ensures the source's arity matches `columns.len()` (or the full
    /// table schema width when `columns` is empty).
    Insert {
        /// Case-folded target table name.
        table: String,
        /// 0-based indices into the target table's full schema for the
        /// targeted columns. Empty means "all columns in natural order".
        columns: Vec<usize>,
        /// Child plan that supplies the rows (`Values`, `Project` over
        /// `Scan`, etc.).
        source: Box<Self>,
        /// Resolved `ON CONFLICT` action, if any.
        on_conflict: Option<LogicalOnConflict>,
        /// `RETURNING` output expressions paired with their output names.
        returning: Vec<(ScalarExpr, String)>,
        /// Schema of the rows returned by `RETURNING`. Empty when there
        /// is no `RETURNING` clause.
        schema: Schema,
    },

    /// Update existing rows in a table.
    ///
    /// The `input` child plan is a `Scan` (possibly wrapped in `Filter`)
    /// that selects the rows to update.
    ///
    /// `UPDATE … FROM other_table` is not supported in v0.2; the binder
    /// returns `NotSupported` for that form.
    Update {
        /// Case-folded target table name.
        table: String,
        /// `(column-index, new-value-expression)` pairs.
        assignments: Vec<(usize, ScalarExpr)>,
        /// Input plan feeding the rows to update.
        input: Box<Self>,
        /// `RETURNING` output expressions.
        returning: Vec<(ScalarExpr, String)>,
        /// Schema of the rows returned by `RETURNING`. Empty when there
        /// is no `RETURNING` clause.
        schema: Schema,
    },

    /// Delete rows from a table.
    ///
    /// The `input` child plan is a `Scan` (possibly wrapped in `Filter`)
    /// that selects the rows to delete.
    ///
    /// `DELETE … USING other_table` is not supported in v0.2; the binder
    /// returns `NotSupported` for that form.
    Delete {
        /// Case-folded target table name.
        table: String,
        /// Input plan feeding the rows to delete.
        input: Box<Self>,
        /// `RETURNING` output expressions.
        returning: Vec<(ScalarExpr, String)>,
        /// Schema of the rows returned by `RETURNING`. Empty when there
        /// is no `RETURNING` clause.
        schema: Schema,
    },

    /// Truncate one or more tables.
    ///
    /// Every table name is validated against the catalog by the binder.
    Truncate {
        /// Case-folded table names.
        tables: Vec<String>,
        /// Whether `RESTART IDENTITY` was specified.
        restart_identity: bool,
        /// Whether `CASCADE` was specified.
        cascade: bool,
        /// Always an empty schema — `TRUNCATE` returns no rows.
        schema: Schema,
    },
}

impl LogicalPlan {
    /// The schema of rows produced by this plan node.
    #[must_use]
    pub fn schema(&self) -> &Schema {
        match self {
            Self::Scan { schema, .. }
            | Self::Project { schema, .. }
            | Self::Empty { schema }
            | Self::Values { schema, .. }
            | Self::Insert { schema, .. }
            | Self::Update { schema, .. }
            | Self::Delete { schema, .. }
            | Self::Truncate { schema, .. } => schema,
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

    #[allow(clippy::too_many_lines)]
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
            Self::Values { rows, .. } => {
                out.push_str(&pad);
                let _ = fmt::write(out, format_args!("Values: {} row(s)\n", rows.len()));
            }
            Self::Insert {
                table,
                columns,
                source,
                returning,
                ..
            } => {
                out.push_str(&pad);
                out.push_str("Insert: table=");
                out.push_str(table);
                out.push_str(" cols=[");
                for (i, c) in columns.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    let _ = fmt::write(out, format_args!("{c}"));
                }
                out.push(']');
                if !returning.is_empty() {
                    out.push_str(" returning=[");
                    for (i, (e, n)) in returning.iter().enumerate() {
                        if i > 0 {
                            out.push_str(", ");
                        }
                        let _ = fmt::write(out, format_args!("{e} AS {n}"));
                    }
                    out.push(']');
                }
                out.push('\n');
                source.display_into(indent + 2, out);
            }
            Self::Update {
                table,
                assignments,
                input,
                returning,
                ..
            } => {
                out.push_str(&pad);
                out.push_str("Update: table=");
                out.push_str(table);
                out.push_str(" assignments=[");
                for (i, (idx, e)) in assignments.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    let _ = fmt::write(out, format_args!("col{idx}={e}"));
                }
                out.push(']');
                if !returning.is_empty() {
                    out.push_str(" returning=[");
                    for (i, (e, n)) in returning.iter().enumerate() {
                        if i > 0 {
                            out.push_str(", ");
                        }
                        let _ = fmt::write(out, format_args!("{e} AS {n}"));
                    }
                    out.push(']');
                }
                out.push('\n');
                input.display_into(indent + 2, out);
            }
            Self::Delete {
                table,
                input,
                returning,
                ..
            } => {
                out.push_str(&pad);
                out.push_str("Delete: table=");
                out.push_str(table);
                if !returning.is_empty() {
                    out.push_str(" returning=[");
                    for (i, (e, n)) in returning.iter().enumerate() {
                        if i > 0 {
                            out.push_str(", ");
                        }
                        let _ = fmt::write(out, format_args!("{e} AS {n}"));
                    }
                    out.push(']');
                }
                out.push('\n');
                input.display_into(indent + 2, out);
            }
            Self::Truncate {
                tables,
                restart_identity,
                cascade,
                ..
            } => {
                out.push_str(&pad);
                out.push_str("Truncate: tables=[");
                out.push_str(&tables.join(", "));
                out.push(']');
                if *restart_identity {
                    out.push_str(" RESTART IDENTITY");
                }
                if *cascade {
                    out.push_str(" CASCADE");
                }
                out.push('\n');
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
    use ultrasql_core::{DataType, Field, Value};

    use super::*;

    fn users_schema() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::nullable("score", DataType::Float64),
        ])
        .expect("schema invariants hold for test fixture")
    }

    fn lit_i32(v: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            data_type: DataType::Int32,
        }
    }

    fn lit_text(s: &str) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Text(s.to_owned()),
            data_type: DataType::Text { max_len: None },
        }
    }

    fn col(name: &str, index: usize, data_type: DataType) -> ScalarExpr {
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
}
