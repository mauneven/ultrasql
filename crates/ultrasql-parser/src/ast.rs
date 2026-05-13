//! SQL abstract syntax tree.
//!
//! AST nodes are owned (no lifetime parameters tying them to the source
//! string). They carry [`Span`]s so error messages can quote source
//! exactly. Identifiers preserve their original quoting state so a
//! later printer can round-trip a parsed statement.

use std::fmt;

use crate::span::Span;

/// Top-level SQL statement.
///
/// `SelectStmt` and the DML statement types are comparatively large, so
/// they live behind a `Box` to keep the enum's stack size small. Pattern
/// matching looks the same to callers.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Statement {
    /// `SELECT ...`.
    Select(Box<SelectStmt>),
    /// `INSERT INTO ...`.
    Insert(Box<InsertStmt>),
    /// `UPDATE ...`.
    Update(Box<UpdateStmt>),
    /// `DELETE FROM ...`.
    Delete(Box<DeleteStmt>),
    /// `TRUNCATE TABLE ...`.
    Truncate(TruncateStmt),
    /// `BEGIN [TRANSACTION]`.
    Begin {
        /// Source span.
        span: Span,
    },
    /// `COMMIT`.
    Commit {
        /// Source span.
        span: Span,
    },
    /// `ROLLBACK`.
    Rollback {
        /// Source span.
        span: Span,
    },
}

impl Statement {
    /// Source span enclosing this statement.
    #[must_use]
    pub fn span(&self) -> Span {
        match self {
            Self::Select(s) => s.span,
            Self::Insert(s) => s.span,
            Self::Update(s) => s.span,
            Self::Delete(s) => s.span,
            Self::Truncate(s) => s.span,
            Self::Begin { span } | Self::Commit { span } | Self::Rollback { span } => *span,
        }
    }
}

/// An `INSERT` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InsertStmt {
    /// Target table.
    pub table: ObjectName,
    /// Explicit column list (empty means all columns, positional).
    pub columns: Vec<Identifier>,
    /// Source of rows to insert.
    pub source: InsertSource,
    /// Optional `ON CONFLICT` clause.
    pub on_conflict: Option<OnConflict>,
    /// Optional `RETURNING` projection list (empty = no RETURNING).
    pub returning: Vec<SelectItem>,
    /// Source span of the entire statement.
    pub span: Span,
}

/// The source of rows in an `INSERT` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InsertSource {
    /// `VALUES (a, b), (c, d), ...` — one `Vec<Expr>` per row.
    Values(Vec<Vec<Expr>>),
    /// `INSERT ... SELECT ...`.
    Select(Box<SelectStmt>),
    /// `INSERT ... DEFAULT VALUES`.
    DefaultValues,
}

/// `ON CONFLICT` clause of an `INSERT` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OnConflict {
    /// `ON CONFLICT [target] DO NOTHING`.
    DoNothing {
        /// Optional conflict target (columns).
        target: Option<ConflictTarget>,
        /// Source span.
        span: Span,
    },
    /// `ON CONFLICT target DO UPDATE SET ...`.
    DoUpdate {
        /// Conflict target (columns).
        target: ConflictTarget,
        /// `SET` assignments.
        set: Vec<Assignment>,
        /// Optional `WHERE` filter on the update.
        r#where: Option<Expr>,
        /// Source span.
        span: Span,
    },
}

/// Conflict target in an `ON CONFLICT` clause.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConflictTarget {
    /// The indexed columns whose uniqueness constraint was violated.
    pub columns: Vec<Identifier>,
    /// Source span.
    pub span: Span,
}

/// A `col = expr` assignment used in `UPDATE … SET` and
/// `ON CONFLICT … DO UPDATE SET`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Assignment {
    /// Target column name.
    pub target: Identifier,
    /// New value expression.
    pub value: Expr,
    /// Source span.
    pub span: Span,
}

/// An `UPDATE` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateStmt {
    /// Target table.
    pub table: ObjectName,
    /// Optional alias for the target table.
    pub alias: Option<Identifier>,
    /// `SET` assignments (must be non-empty).
    pub set: Vec<Assignment>,
    /// Optional `FROM` clause (additional table references).
    pub from: Vec<TableRef>,
    /// Optional `WHERE` predicate.
    pub r#where: Option<Expr>,
    /// Optional `RETURNING` projection list (empty = no RETURNING).
    pub returning: Vec<SelectItem>,
    /// Source span of the entire statement.
    pub span: Span,
}

/// A `DELETE` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteStmt {
    /// Target table.
    pub table: ObjectName,
    /// Optional alias for the target table.
    pub alias: Option<Identifier>,
    /// Optional `USING` clause (additional table references).
    pub using: Vec<TableRef>,
    /// Optional `WHERE` predicate.
    pub r#where: Option<Expr>,
    /// Optional `RETURNING` projection list (empty = no RETURNING).
    pub returning: Vec<SelectItem>,
    /// Source span of the entire statement.
    pub span: Span,
}

/// A `TRUNCATE TABLE` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TruncateStmt {
    /// Tables to truncate (one or more).
    pub tables: Vec<ObjectName>,
    /// Whether `RESTART IDENTITY` was specified.
    pub restart_identity: bool,
    /// Whether `CASCADE` was specified.
    pub cascade: bool,
    /// Source span of the entire statement.
    pub span: Span,
}

/// A `SELECT` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectStmt {
    /// Whether `DISTINCT` was specified.
    pub distinct: bool,
    /// Output items.
    pub projection: Vec<SelectItem>,
    /// `FROM` clause (zero or one table reference for now; joins land
    /// in a follow-up).
    pub from: Option<TableRef>,
    /// `WHERE` predicate.
    pub r#where: Option<Expr>,
    /// `ORDER BY` items.
    pub order_by: Vec<OrderItem>,
    /// `LIMIT n`.
    pub limit: Option<Expr>,
    /// `OFFSET n`.
    pub offset: Option<Expr>,
    /// Source span.
    pub span: Span,
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
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
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

/// SQL expression.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Expr {
    /// Literal value.
    Literal(Literal),
    /// Column reference, optionally qualified.
    Column {
        /// Name with up to three parts.
        name: ObjectName,
    },
    /// Positional parameter (`$N`).
    Parameter {
        /// 1-based parameter index.
        index: u32,
        /// Source span.
        span: Span,
    },
    /// Unary operator.
    Unary {
        /// Operator.
        op: UnaryOp,
        /// Operand.
        expr: Box<Self>,
        /// Source span.
        span: Span,
    },
    /// Binary operator.
    Binary {
        /// Operator.
        op: BinaryOp,
        /// Left-hand operand.
        left: Box<Self>,
        /// Right-hand operand.
        right: Box<Self>,
        /// Source span.
        span: Span,
    },
    /// Function call.
    Call {
        /// Function name (may be schema-qualified).
        name: ObjectName,
        /// Argument list.
        args: Vec<Self>,
        /// Whether the argument list was `DISTINCT`-prefixed.
        distinct: bool,
        /// Source span.
        span: Span,
    },
    /// `expr IS NULL` / `expr IS NOT NULL`.
    IsNull {
        /// Operand.
        expr: Box<Self>,
        /// `true` for `IS NOT NULL`.
        negated: bool,
        /// Source span.
        span: Span,
    },
    /// Parenthesized expression.
    Paren {
        /// Inner expression.
        expr: Box<Self>,
        /// Source span.
        span: Span,
    },
    /// `CAST(expr AS type)`.
    Cast {
        /// Expression to cast.
        expr: Box<Self>,
        /// Target type identifier (parsed as an identifier; the binder
        /// turns it into a [`ultrasql_core::DataType`]).
        target: Identifier,
        /// Source span.
        span: Span,
    },
}

impl Expr {
    /// Source span.
    #[must_use]
    pub const fn span(&self) -> Span {
        match self {
            Self::Literal(lit) => lit.span(),
            Self::Column { name } => name.span,
            Self::Parameter { span, .. }
            | Self::Unary { span, .. }
            | Self::Binary { span, .. }
            | Self::Call { span, .. }
            | Self::IsNull { span, .. }
            | Self::Paren { span, .. }
            | Self::Cast { span, .. } => *span,
        }
    }
}

/// Literal value.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Literal {
    /// `NULL`.
    Null {
        /// Source span.
        span: Span,
    },
    /// `TRUE` / `FALSE`.
    Bool {
        /// Value.
        value: bool,
        /// Source span.
        span: Span,
    },
    /// Integer literal (kept as a string until the binder picks a
    /// width).
    Integer {
        /// Original text.
        text: String,
        /// Source span.
        span: Span,
    },
    /// Floating-point literal (kept as a string until the binder picks
    /// a width and rounds).
    Float {
        /// Original text.
        text: String,
        /// Source span.
        span: Span,
    },
    /// String literal. The body excludes the surrounding quotes and
    /// has any `''` collapsed.
    String {
        /// Decoded body.
        value: String,
        /// Source span.
        span: Span,
    },
}

impl Literal {
    /// Source span.
    #[must_use]
    pub const fn span(&self) -> Span {
        match self {
            Self::Null { span }
            | Self::Bool { span, .. }
            | Self::Integer { span, .. }
            | Self::Float { span, .. }
            | Self::String { span, .. } => *span,
        }
    }
}

/// Unary operators.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    /// `-x`.
    Neg,
    /// `+x` (rare but accepted).
    Pos,
    /// `NOT x`.
    Not,
}

/// Binary operators recognized by the parser.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryOp {
    /// `+`
    Add,
    /// `-`
    Sub,
    /// `*`
    Mul,
    /// `/`
    Div,
    /// `%`
    Mod,
    /// `^` (exponentiation)
    Pow,
    /// `||` (string concat).
    Concat,
    /// `=`
    Eq,
    /// `<>`/`!=`
    NotEq,
    /// `<`
    Lt,
    /// `<=`
    LtEq,
    /// `>`
    Gt,
    /// `>=`
    GtEq,
    /// `AND`
    And,
    /// `OR`
    Or,
    /// `LIKE`
    Like,
    /// `NOT LIKE`
    NotLike,
    /// `ILIKE`
    Ilike,
    /// `NOT ILIKE`
    NotIlike,
}

impl BinaryOp {
    /// Precedence (higher binds tighter). Roughly mirrors PostgreSQL.
    #[must_use]
    pub const fn precedence(self) -> u8 {
        match self {
            Self::Or => 1,
            Self::And => 2,
            Self::Eq
            | Self::NotEq
            | Self::Lt
            | Self::LtEq
            | Self::Gt
            | Self::GtEq
            | Self::Like
            | Self::NotLike
            | Self::Ilike
            | Self::NotIlike => 3,
            Self::Concat => 4,
            Self::Add | Self::Sub => 5,
            Self::Mul | Self::Div | Self::Mod => 6,
            Self::Pow => 7,
        }
    }

    /// `true` iff this operator is right-associative.
    #[must_use]
    pub const fn is_right_associative(self) -> bool {
        matches!(self, Self::Pow)
    }
}
