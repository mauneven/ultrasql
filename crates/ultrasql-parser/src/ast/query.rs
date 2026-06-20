//! Query-level AST nodes.
//!
//! The `SELECT` statement and everything it composes: projection items,
//! table references and joins, CTEs, set operations, locking clauses,
//! pivot/unpivot and JSON/XML table functions, window specs, ordering,
//! and the shared [`ObjectName`] / [`Identifier`] name types.

use std::fmt;

use crate::ast::{Expr, TypeName};
use crate::span::Span;

// ============================================================================
// SELECT-level AST — extended for v0.2
// ============================================================================

/// A `SELECT` statement, including optional WITH clause (CTEs) and set
/// operations (UNION / INTERSECT / EXCEPT).
///
/// # Breaking change (v0.2)
/// `from` is now `Vec<TableRef>` (was `Option<TableRef>`). The join tree is
/// encoded as a left-deep chain of [`TableRef::Join`] nodes; comma-separated
/// tables are canonicalised to `JoinOp::Cross`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectStmt {
    /// DISTINCT / DISTINCT ON / ALL / no keyword.
    pub distinct: Distinct,
    /// Output items.
    pub projection: Vec<SelectItem>,
    /// `FROM` clause. Zero entries means no `FROM` (e.g. `SELECT 1`).
    /// Multiple entries represent a left-deep join tree where comma-joins
    /// have been canonicalised to `JoinOp::Cross`.
    pub from: Vec<TableRef>,
    /// `WHERE` predicate.
    pub r#where: Option<Expr>,
    /// `GROUP BY` expressions.
    pub group_by: Vec<Expr>,
    /// `HAVING` predicate (only meaningful when `group_by` is non-empty).
    pub having: Option<Expr>,
    /// `ORDER BY` items.
    pub order_by: Vec<OrderItem>,
    /// `LIMIT n`.
    pub limit: Option<Expr>,
    /// `OFFSET n`.
    pub offset: Option<Expr>,
    /// Chained UNION / INTERSECT / EXCEPT tails. In PostgreSQL, set ops bind
    /// less tightly than ORDER BY / LIMIT. For v0.2 we represent the chain
    /// here and leave precedence enforcement to the binder.
    pub set_ops: Vec<SetOpTail>,
    /// Leading `WITH [RECURSIVE]` CTEs.
    pub ctes: Vec<Cte>,
    /// `FOR UPDATE` / `FOR SHARE` / `FOR NO KEY UPDATE` / `FOR KEY SHARE`
    /// locking clauses. Multiple clauses are permitted by PostgreSQL when
    /// different `OF table` targets are given; we collect all of them.
    pub locking: Vec<LockingClause>,
    /// Source span.
    pub span: Span,
}

/// `FOR UPDATE` / `FOR SHARE` / `FOR NO KEY UPDATE` / `FOR KEY SHARE`.
///
/// Describes the row-level lock strength requested by a `SELECT` statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockStrength {
    /// `FOR UPDATE` — exclusive row lock; blocks concurrent writers.
    Update,
    /// `FOR NO KEY UPDATE` — like Update but does not block `KeyShare` locks.
    NoKeyUpdate,
    /// `FOR SHARE` — shared lock; blocks concurrent writes, not other reads.
    Share,
    /// `FOR KEY SHARE` — weakest; only blocks `FOR UPDATE`.
    KeyShare,
}

/// What to do when a requested lock is unavailable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum LockWaitPolicy {
    /// Block until the lock can be acquired (default).
    #[default]
    Wait,
    /// Raise an error immediately if any row is locked.
    NoWait,
    /// Silently skip rows that are currently locked.
    SkipLocked,
}

/// One `FOR …` locking clause in a `SELECT` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LockingClause {
    /// Lock strength requested.
    pub strength: LockStrength,
    /// Wait policy when a lock cannot be acquired immediately.
    pub wait_policy: LockWaitPolicy,
    /// Optional `OF table [, …]` targets; empty means all relations in `FROM`.
    pub of_tables: Vec<ObjectName>,
}

/// DISTINCT / DISTINCT ON / ALL / (implicit none).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Distinct {
    /// No DISTINCT keyword (implicit ALL rows).
    None,
    /// `ALL` keyword was explicit.
    All,
    /// `DISTINCT` with no ON clause.
    Distinct,
    /// `DISTINCT ON (expr, …)`.
    DistinctOn(Vec<Expr>),
}

/// One UNION / INTERSECT / EXCEPT tail following a SELECT body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetOpTail {
    /// The set operation.
    pub op: SetOp,
    /// ALL or DISTINCT (default is DISTINCT per SQL standard).
    pub quantifier: SetQuantifier,
    /// Right-hand SELECT of the set operation.
    pub right: Box<SelectStmt>,
    /// Source span of this tail (from the keyword through to end of right).
    pub span: Span,
}

/// Set operation kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetOp {
    /// `UNION`.
    Union,
    /// `INTERSECT`.
    Intersect,
    /// `EXCEPT`.
    Except,
}

/// Set quantifier for a set operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetQuantifier {
    /// `DISTINCT` (default per SQL standard).
    Distinct,
    /// `ALL` — keep duplicates.
    All,
}

/// One entry in the `SELECT` projection list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SelectItem {
    /// `*`.
    Wildcard {
        /// Source span.
        span: Span,
    },
    /// `t.*` (qualified wildcard).
    QualifiedWildcard {
        /// Qualifier (table or alias name).
        qualifier: Identifier,
        /// Source span.
        span: Span,
    },
    /// `expr [AS alias]`.
    Expr {
        /// The expression.
        expr: Expr,
        /// Optional output alias.
        alias: Option<Identifier>,
        /// Source span.
        span: Span,
    },
}

/// A table reference in the `FROM` clause.
///
/// The v0.2 extension adds [`TableRef::Join`] (explicit join syntax and
/// comma-separated table lists canonicalised to CROSS JOIN) and
/// [`TableRef::Subquery`] (derived tables).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableRef {
    /// A bare table name with an optional alias.
    Named {
        /// Schema-qualified name (one or two parts).
        name: ObjectName,
        /// `AS alias` (or implicit alias).
        alias: Option<Identifier>,
        /// Source span.
        span: Span,
    },
    /// An explicit or implicit join between two table references.
    ///
    /// The parser builds a left-deep tree: the first table in the FROM
    /// clause is the leftmost leaf; each subsequent join wraps the
    /// current left side.
    Join {
        /// Left-hand table or sub-tree.
        left: Box<Self>,
        /// Join type.
        op: JoinOp,
        /// Right-hand table factor.
        right: Box<Self>,
        /// Join condition.
        condition: JoinCondition,
        /// Source span.
        span: Span,
    },
    /// A derived table (subquery in FROM).
    ///
    /// PostgreSQL requires an alias on every derived table. Parsing
    /// without an alias is a [`crate::parser::ParseError`].
    Subquery {
        /// The inner SELECT.
        select: Box<SelectStmt>,
        /// Required alias for the derived table.
        alias: Identifier,
        /// Optional column-alias list: `AS t(c1, c2, …)`.
        column_aliases: Vec<Identifier>,
        /// Source span.
        span: Span,
    },
    /// A set-returning function in the `FROM` clause, e.g.
    /// `generate_series(1, 10)`.
    Function {
        /// Function name (e.g. `generate_series`, `unnest`).
        name: Identifier,
        /// Argument expressions in declaration order.
        args: Vec<Expr>,
        /// Optional alias for the produced relation.
        alias: Option<Identifier>,
        /// Source span.
        span: Span,
    },
    /// SQL/JSON `JSON_TABLE(...)` table function.
    JsonTable {
        /// Input JSON expression.
        context: Expr,
        /// Row-pattern SQL/JSON path expression.
        row_path: String,
        /// Declared output columns.
        columns: Vec<JsonTableColumn>,
        /// Optional alias for the produced relation.
        alias: Option<Identifier>,
        /// Source span.
        span: Span,
    },
    /// `table PIVOT (agg(expr) FOR col IN (...))`.
    Pivot {
        /// Input table reference.
        input: Box<Self>,
        /// Aggregate applied to each pivot value.
        aggregate: PivotAggregate,
        /// Column whose values select the pivot bucket.
        value_column: Identifier,
        /// Literal pivot values and optional output aliases.
        pivot_values: Vec<PivotValue>,
        /// Source span.
        span: Span,
    },
    /// `table UNPIVOT ([INCLUDE|EXCLUDE NULLS] value FOR name IN (...))`.
    Unpivot {
        /// Input table reference.
        input: Box<Self>,
        /// Output value column.
        value_column: Identifier,
        /// Output name column holding each unpivoted label.
        name_column: Identifier,
        /// Input columns to unpivot.
        columns: Vec<UnpivotColumn>,
        /// Whether NULL source values are retained.
        include_nulls: bool,
        /// Source span.
        span: Span,
    },
    /// SQL/XML `XMLTABLE(...)` table function.
    XmlTable {
        /// Input XML expression.
        context: Expr,
        /// Row-pattern XPath expression.
        row_path: String,
        /// Declared output columns.
        columns: Vec<XmlTableColumn>,
        /// Optional alias for the produced relation.
        alias: Option<Identifier>,
        /// Source span.
        span: Span,
    },
}

/// Aggregate call inside a `PIVOT` table-factor transform.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PivotAggregate {
    /// Aggregate function name.
    pub function: Identifier,
    /// Aggregate argument, or `None` for `COUNT(*)`.
    pub arg: Option<Expr>,
    /// Source span.
    pub span: Span,
}

/// One value inside a `PIVOT ... IN (...)` list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PivotValue {
    /// Pivot-key value expression.
    pub value: Expr,
    /// Optional output column alias.
    pub alias: Option<Identifier>,
    /// Source span.
    pub span: Span,
}

/// One input column inside an `UNPIVOT ... IN (...)` list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnpivotColumn {
    /// Source column to unpivot.
    pub column: Identifier,
    /// Optional emitted label. Defaults to the column name.
    pub label: Option<Expr>,
    /// Source span.
    pub span: Span,
}

/// One column declared inside a `JSON_TABLE ... COLUMNS (...)` clause.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonTableColumn {
    /// Output column name.
    pub name: Identifier,
    /// Column behavior.
    pub kind: JsonTableColumnKind,
    /// Source span.
    pub span: Span,
}

/// Supported `JSON_TABLE` column forms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JsonTableColumnKind {
    /// `name FOR ORDINALITY`.
    Ordinality,
    /// `name type [PATH path_expression]`.
    Value {
        /// Declared SQL output type.
        data_type: TypeName,
        /// Optional column path. Defaults to `$.name`.
        path: Option<String>,
    },
    /// `name type EXISTS [PATH path_expression]`.
    Exists {
        /// Declared SQL output type, normally boolean.
        data_type: TypeName,
        /// Optional column path. Defaults to `$.name`.
        path: Option<String>,
    },
}

/// One column declared inside an `XMLTABLE ... COLUMNS (...)` clause.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct XmlTableColumn {
    /// Output column name.
    pub name: Identifier,
    /// Column behavior.
    pub kind: XmlTableColumnKind,
    /// Source span.
    pub span: Span,
}

/// Supported `XMLTABLE` column forms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum XmlTableColumnKind {
    /// `name FOR ORDINALITY`.
    Ordinality,
    /// `name type [PATH path_expression]`.
    Value {
        /// Declared SQL output type.
        data_type: TypeName,
        /// Optional XPath expression. Defaults to the column name.
        path: Option<String>,
        /// Optional scalar literal used when the path returns no value.
        default: Option<String>,
    },
}

/// Join operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinOp {
    /// `[INNER] JOIN`.
    Inner,
    /// `LEFT [OUTER] JOIN`.
    LeftOuter,
    /// `RIGHT [OUTER] JOIN`.
    RightOuter,
    /// `FULL [OUTER] JOIN`.
    FullOuter,
    /// `CROSS JOIN` or comma-separated table factor.
    Cross,
}

/// Join condition for an explicit join.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinCondition {
    /// `ON expr`.
    On(Expr),
    /// `USING (col, …)`.
    Using(Vec<Identifier>),
    /// `NATURAL JOIN`; binder resolves common column names to `USING`.
    Natural,
    /// No condition (CROSS JOIN).
    None,
}

/// Common table expression in a `WITH` clause.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cte {
    /// CTE name.
    pub name: Identifier,
    /// Optional column-alias list: `cte_name(c1, c2, …)`.
    pub column_aliases: Vec<Identifier>,
    /// Whether `WITH RECURSIVE` was specified.
    ///
    /// The flag is replicated on every CTE in the same WITH clause.
    pub recursive: bool,
    /// The body SELECT.
    pub query: Box<SelectStmt>,
    /// Source span.
    pub span: Span,
}

/// `OVER (PARTITION BY ... ORDER BY ...)` window specification riding on
/// a function call.
///
/// v0.5 supports an empty / present `PARTITION BY` and an empty /
/// present `ORDER BY`. Frame clauses (`ROWS BETWEEN ... AND ...`,
/// `RANGE BETWEEN ... AND ...`) are recognised at the kernel but the
/// parser does not yet emit them — the default frame is "UNBOUNDED
/// PRECEDING to CURRENT ROW for ORDER BY, UNBOUNDED PRECEDING to
/// UNBOUNDED FOLLOWING otherwise", matching PostgreSQL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowSpec {
    /// `PARTITION BY` expressions.
    pub partition_by: Vec<Expr>,
    /// `ORDER BY` items.
    pub order_by: Vec<OrderItem>,
    /// Source span.
    pub span: Span,
}

/// Order-by item.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderItem {
    /// The sort expression.
    pub expr: Expr,
    /// Sort direction.
    pub direction: SortDirection,
    /// Null ordering.
    pub nulls: NullsOrder,
    /// Source span.
    pub span: Span,
}

/// `ASC` or `DESC`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortDirection {
    /// Ascending (default).
    Asc,
    /// Descending.
    Desc,
}

/// `NULLS FIRST` or `NULLS LAST`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NullsOrder {
    /// Database default (PostgreSQL: `NULLS LAST` for `ASC`,
    /// `NULLS FIRST` for `DESC`).
    Default,
    /// `NULLS FIRST`.
    First,
    /// `NULLS LAST`.
    Last,
}

/// Multi-part object name (`db.schema.table`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectName {
    /// Components in left-to-right order.
    pub parts: Vec<Identifier>,
    /// Source span.
    pub span: Span,
}

impl fmt::Display for ObjectName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, p) in self.parts.iter().enumerate() {
            if i > 0 {
                f.write_str(".")?;
            }
            write!(f, "{}", p.value)?;
        }
        Ok(())
    }
}

/// A SQL identifier — name + quote flag + span.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identifier {
    /// The identifier text. For unquoted identifiers this is the
    /// case-folded (lower-case) form; for quoted identifiers it
    /// preserves the source case.
    pub value: String,
    /// Whether the identifier was double-quoted in the source.
    pub quoted: bool,
    /// Source span.
    pub span: Span,
}
