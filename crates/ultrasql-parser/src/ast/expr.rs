//! Expression AST nodes.
//!
//! The [`Expr`] tree, scalar [`Literal`]s, and the [`UnaryOp`] /
//! [`BinaryOp`] operator enums (including operator precedence used by the
//! Pratt parser).

use crate::ast::{Identifier, ObjectName, OrderItem, SelectStmt, WindowSpec};
use crate::span::Span;

/// SQL expression.
///
/// The enum is `#[non_exhaustive]` so that downstream crates are not broken
/// when new expression kinds are added (e.g. window functions).
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
        /// Optional ordered-set aggregate sort specification from
        /// `WITHIN GROUP (ORDER BY ...)`.
        within_group: Option<Vec<OrderItem>>,
        /// Optional `OVER (...)` clause turning this into a window
        /// function call. `None` for ordinary scalar / aggregate calls.
        over: Option<WindowSpec>,
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
    /// Array literal, e.g. `['a', 'b']`.
    ArrayLiteral {
        /// Elements in source order.
        elements: Vec<Self>,
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
    /// Scalar subquery: `(SELECT …)`.
    ///
    /// The surrounding operator (`IN`, `EXISTS`, etc.) disambiguates the role.
    Subquery {
        /// The inner SELECT.
        select: Box<SelectStmt>,
        /// Source span.
        span: Span,
    },
    /// `EXISTS (SELECT …)` / `NOT EXISTS (SELECT …)`.
    Exists {
        /// The inner SELECT.
        select: Box<SelectStmt>,
        /// `true` for `NOT EXISTS`.
        negated: bool,
        /// Source span.
        span: Span,
    },
    /// `expr [NOT] IN (val, …)`.
    InList {
        /// The test expression.
        expr: Box<Self>,
        /// The list items (arbitrary expressions per PG).
        items: Vec<Self>,
        /// `true` for `NOT IN`.
        negated: bool,
        /// Source span.
        span: Span,
    },
    /// `expr [NOT] IN (SELECT …)`.
    InSubquery {
        /// The test expression.
        expr: Box<Self>,
        /// The inner SELECT.
        select: Box<SelectStmt>,
        /// `true` for `NOT IN`.
        negated: bool,
        /// Source span.
        span: Span,
    },
    /// `left_expr <op> ANY (SELECT …)`.
    ///
    /// The parser folds `lhs <op> ANY (sub)` into this node. Only
    /// comparison operators (`=`, `<>`, `<`, `<=`, `>`, `>=`) are valid
    /// with `ANY`.
    Any {
        /// Left-hand expression.
        expr: Box<Self>,
        /// Comparison operator.
        op: BinaryOp,
        /// The inner SELECT.
        select: Box<SelectStmt>,
        /// Source span.
        span: Span,
    },
    /// `left_expr <op> ANY (array_expr)`.
    ///
    /// PostgreSQL accepts both array literals and array-producing scalar
    /// expressions here. The parser keeps non-literal array expressions in
    /// this node so the planner can bind them as scalar membership tests.
    AnyArray {
        /// Left-hand expression.
        expr: Box<Self>,
        /// Comparison operator.
        op: BinaryOp,
        /// Array-producing expression.
        array: Box<Self>,
        /// Source span.
        span: Span,
    },
    /// `left_expr <op> ALL (SELECT …)`.
    ///
    /// Like [`Expr::Any`] but with `ALL` semantics.
    All {
        /// Left-hand expression.
        expr: Box<Self>,
        /// Comparison operator.
        op: BinaryOp,
        /// The inner SELECT.
        select: Box<SelectStmt>,
        /// Source span.
        span: Span,
    },
    /// `CASE [operand] WHEN … THEN … [ELSE …] END`.
    ///
    /// When `operand` is `Some` this is a *simple* CASE that compares the
    /// operand against each WHEN value with `=`. When `operand` is `None`
    /// it is a *searched* CASE where each WHEN branch is a boolean condition.
    Case {
        /// Optional operand for a simple CASE (`CASE expr WHEN …`).
        /// `None` for a searched CASE (`CASE WHEN cond …`).
        operand: Option<Box<Self>>,
        /// `(when_expr, then_expr)` branch pairs (at least one).
        branches: Vec<(Self, Self)>,
        /// Optional ELSE expression.
        else_expr: Option<Box<Self>>,
        /// Source span.
        span: Span,
    },
    /// `COALESCE(a, b, …)` — returns the first non-NULL argument.
    Coalesce {
        /// Argument list (at least one per SQL standard).
        args: Vec<Self>,
        /// Source span.
        span: Span,
    },
    /// `NULLIF(a, b)` — returns NULL if `a = b`, otherwise `a`.
    NullIf {
        /// First argument.
        a: Box<Self>,
        /// Second argument.
        b: Box<Self>,
        /// Source span.
        span: Span,
    },
    /// `GREATEST(a, b, …)` — returns the largest non-NULL argument.
    Greatest {
        /// Argument list (at least one).
        args: Vec<Self>,
        /// Source span.
        span: Span,
    },
    /// `LEAST(a, b, …)` — returns the smallest non-NULL argument.
    Least {
        /// Argument list (at least one).
        args: Vec<Self>,
        /// Source span.
        span: Span,
    },
    /// `expr [NOT] BETWEEN [SYMMETRIC] low AND high`.
    Between {
        /// The expression being tested.
        expr: Box<Self>,
        /// Lower bound.
        low: Box<Self>,
        /// Upper bound.
        high: Box<Self>,
        /// `true` for `NOT BETWEEN`.
        negated: bool,
        /// `true` for `BETWEEN SYMMETRIC` (ordering of bounds is irrelevant).
        symmetric: bool,
        /// Source span.
        span: Span,
    },
    /// `expr IS [NOT] DISTINCT FROM other`.
    IsDistinctFrom {
        /// Left-hand expression.
        left: Box<Self>,
        /// Right-hand expression.
        right: Box<Self>,
        /// `true` for `IS NOT DISTINCT FROM`.
        negated: bool,
        /// Source span.
        span: Span,
    },
    /// `expr IS [NOT] TRUE / FALSE / UNKNOWN`.
    IsBoolean {
        /// The expression being tested.
        expr: Box<Self>,
        /// The boolean literal being compared against (`true` = TRUE,
        /// `false` = FALSE). Unused when `is_unknown` is `true`.
        value: bool,
        /// `true` when the test is against `UNKNOWN` rather than a boolean
        /// literal.
        is_unknown: bool,
        /// `true` for `IS NOT …`.
        negated: bool,
        /// Source span.
        span: Span,
    },
    /// `expr::type_name` — PostgreSQL postfix cast syntax.
    PostfixCast {
        /// Expression being cast.
        expr: Box<Self>,
        /// Target type name.
        target: Identifier,
        /// Source span.
        span: Span,
    },
    /// `expr[index]` — single-element array subscript (1-based).
    ArraySubscript {
        /// Array expression.
        expr: Box<Self>,
        /// Index expression.
        index: Box<Self>,
        /// Source span.
        span: Span,
    },
    /// `expr[lower:upper]` — array slice.
    ///
    /// Either bound may be `None` (`arr[:3]`, `arr[2:]`, `arr[:]`).
    ArraySlice {
        /// Array expression.
        expr: Box<Self>,
        /// Optional lower bound (absent in `arr[:n]`).
        lower: Option<Box<Self>>,
        /// Optional upper bound (absent in `arr[n:]`).
        upper: Option<Box<Self>>,
        /// Source span.
        span: Span,
    },
    /// `expr AT TIME ZONE zone` — convert to/from a time zone.
    AtTimeZone {
        /// The timestamp or time expression.
        expr: Box<Self>,
        /// Time zone specifier (string literal or identifier).
        zone: Box<Self>,
        /// Source span.
        span: Span,
    },
    /// `expr COLLATE collation` — attach a collation to text comparison.
    Collate {
        /// Expression being collated.
        expr: Box<Self>,
        /// Collation name, optionally schema-qualified.
        collation: ObjectName,
        /// Source span.
        span: Span,
    },
    /// `(a, b) OVERLAPS (c, d)` — period overlap test.
    Overlaps {
        /// Start of the left period.
        left_start: Box<Self>,
        /// End of the left period.
        left_end: Box<Self>,
        /// Start of the right period.
        right_start: Box<Self>,
        /// End of the right period.
        right_end: Box<Self>,
        /// Source span.
        span: Span,
    },
    /// `ROW(a, b, …)` — explicit row constructor.
    Row {
        /// Field expressions.
        fields: Vec<Self>,
        /// Source span.
        span: Span,
    },
    /// `DEFAULT` placeholder, valid only as a cell of an `INSERT ... VALUES`
    /// row. The binder substitutes the target column's default (or NULL);
    /// it is rejected anywhere else, matching PostgreSQL.
    Default {
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
            | Self::ArrayLiteral { span, .. }
            | Self::Cast { span, .. }
            | Self::Subquery { span, .. }
            | Self::Exists { span, .. }
            | Self::InList { span, .. }
            | Self::InSubquery { span, .. }
            | Self::Any { span, .. }
            | Self::AnyArray { span, .. }
            | Self::All { span, .. }
            | Self::Case { span, .. }
            | Self::Coalesce { span, .. }
            | Self::NullIf { span, .. }
            | Self::Greatest { span, .. }
            | Self::Least { span, .. }
            | Self::Between { span, .. }
            | Self::IsDistinctFrom { span, .. }
            | Self::IsBoolean { span, .. }
            | Self::PostfixCast { span, .. }
            | Self::ArraySubscript { span, .. }
            | Self::ArraySlice { span, .. }
            | Self::AtTimeZone { span, .. }
            | Self::Collate { span, .. }
            | Self::Overlaps { span, .. }
            | Self::Row { span, .. }
            | Self::Default { span, .. } => *span,
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
    /// Typed string constant. SQL syntax: `TYPENAME 'literal-body'`.
    /// Covers `DATE 'YYYY-MM-DD'`, `TIMESTAMP '…'`, `TIME '…'`, and
    /// `INTERVAL '…' [unit]`. The body is the decoded string content;
    /// the binder is responsible for parsing it into the appropriate
    /// `Value` variant.
    Typed {
        /// Lowercase type-name spelling (`"date"`, `"interval"`,
        /// `"vector"`, `"vector(3)"`, ...).
        type_name: String,
        /// Decoded string body (no surrounding quotes).
        value: String,
        /// Optional trailing interval unit (`"year"`, `"month"`, …)
        /// for `INTERVAL '…' unit`. Empty for non-interval typed
        /// literals.
        unit: Option<String>,
        /// Source span covering the whole `TYPENAME '…'` construct.
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
            | Self::String { span, .. }
            | Self::Typed { span, .. } => *span,
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
    /// `~x` — bitwise NOT (prefix position).
    ///
    /// The `~` character is also used as the binary POSIX-regex-match operator
    /// (`BinaryOp::RegexMatch`). The parser disambiguates by position: in the
    /// prefix (expression-start) slot `~` is `BitNot`; in the infix slot it is
    /// `RegexMatch`.
    BitNot,
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
    /// `<->` — vector L2 distance.
    VectorL2Distance,
    /// `<#>` — vector negative inner product.
    VectorNegativeInnerProduct,
    /// `<=>` — vector cosine distance.
    VectorCosineDistance,
    /// `<+>` — vector L1 distance.
    VectorL1Distance,
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
    /// `~` — POSIX regex match (case-sensitive).
    RegexMatch,
    /// `~*` — POSIX regex match (case-insensitive).
    RegexIMatch,
    /// `!~` — POSIX regex non-match (case-sensitive).
    RegexNotMatch,
    /// `!~*` — POSIX regex non-match (case-insensitive).
    RegexNotIMatch,
    /// `&` — bitwise AND.
    BitAnd,
    /// `|` — bitwise OR.
    BitOr,
    /// `#` — bitwise XOR (PostgreSQL syntax).
    BitXor,
    /// `<<` — bitwise shift left.
    ShiftLeft,
    /// `>>` — bitwise shift right.
    ShiftRight,
    /// `<<=` — network contained within or equal.
    NetworkContainedEq,
    /// `>>=` — network contains or equal.
    NetworkContainsEq,
    /// `->` — JSON/JSONB element access by key, returns JSONB.
    JsonGet,
    /// `->>` — JSON/JSONB element access by key, returns text.
    JsonGetText,
    /// `#>` — JSON/JSONB path access, returns JSONB.
    JsonGetPath,
    /// `#>>` — JSON/JSONB path access, returns text.
    JsonGetPathText,
    /// `@>` — JSONB/array contains.
    JsonContains,
    /// `<@` — JSONB/array contained by.
    JsonContained,
    /// `?` — JSONB has key.
    JsonHasKey,
    /// `?|` — JSONB has any of the given keys.
    JsonHasAnyKey,
    /// `?&` — JSONB has all of the given keys.
    JsonHasAllKeys,
    /// `@@` — TSVECTOR matches TSQUERY.
    TextSearchMatch,
    /// `&&` — range/geometric overlap.
    Overlap,
}

impl BinaryOp {
    /// Operator precedence level. Higher values bind more tightly.
    ///
    /// The table mirrors PostgreSQL's operator precedence (Table 4.2) from
    /// loosest to tightest. The numeric levels here are shared with the
    /// non-operator postfix decorators handled in the Pratt loop (`IN`,
    /// `BETWEEN`, `IS`); see the `IN_BETWEEN_PREC` and `IS_PREC` constants.
    ///
    /// ```text
    /// Level 1 — OR
    /// Level 2 — AND
    /// (NOT is a prefix operator, handled in parse_prefix)
    /// Level 3 — IS / ISNULL / NOTNULL band              (Self::IS_PREC)
    /// Level 4 — comparison band: < > = <= >= <>, LIKE, ILIKE,
    ///           regex ops (~, ~*, !~, !~*), network containment
    /// Level 5 — IN / BETWEEN band                       (Self::IN_BETWEEN_PREC)
    /// Level 6 — all other binary operators (one PostgreSQL level):
    ///           JSON ops (-> ->> #> #>> @> <@ ? ?| ?&), concat ||,
    ///           bitwise & | #, bitwise shift << >>,
    ///           vector distance ops (<-> <#> <=> <+>)
    /// Level 7 — addition/subtraction + -
    /// Level 8 — multiplication/division/modulo * / %
    /// Level 9 — exponentiation ^ (left-associative, as in PostgreSQL)
    /// ```
    ///
    /// Per PostgreSQL Table 4.2 every non-arithmetic infix operator
    /// (concat, bitwise, shift, JSON, vector distance, …) shares a single
    /// "all other operators" precedence level that sits just *below*
    /// addition/subtraction. Shift therefore groups with bitwise/concat,
    /// not above add/sub. The comparison band keeps LIKE/ILIKE/regex
    /// alongside `=`/`<`/… so their long-standing grouping is unchanged.
    #[must_use]
    pub const fn precedence(self) -> u8 {
        match self {
            Self::Or => 1,
            Self::And => 2,
            // (Self::IS_PREC == 3 — postfix IS band, no binary op lives here.)
            // Comparison and regex band.
            Self::Eq
            | Self::NotEq
            | Self::Lt
            | Self::LtEq
            | Self::Gt
            | Self::GtEq
            | Self::Like
            | Self::NotLike
            | Self::Ilike
            | Self::NotIlike
            | Self::RegexMatch
            | Self::RegexIMatch
            | Self::RegexNotMatch
            | Self::RegexNotIMatch
            | Self::NetworkContainedEq
            | Self::NetworkContainsEq => 4,
            // (Self::IN_BETWEEN_PREC == 5 — postfix IN/BETWEEN band.)
            // "All other operators": JSON, concat, bitwise and/or/xor,
            // bitwise shift, and vector distance — one PostgreSQL level.
            Self::JsonGet
            | Self::JsonGetText
            | Self::JsonGetPath
            | Self::JsonGetPathText
            | Self::JsonContains
            | Self::JsonContained
            | Self::Overlap
            | Self::JsonHasKey
            | Self::JsonHasAnyKey
            | Self::JsonHasAllKeys
            | Self::TextSearchMatch
            | Self::Concat
            | Self::BitAnd
            | Self::BitOr
            | Self::BitXor
            | Self::ShiftLeft
            | Self::ShiftRight
            | Self::VectorL2Distance
            | Self::VectorNegativeInnerProduct
            | Self::VectorCosineDistance
            | Self::VectorL1Distance => 6,
            Self::Add | Self::Sub => 7,
            Self::Mul | Self::Div | Self::Mod => 8,
            Self::Pow => 9,
        }
    }

    /// Precedence level of the postfix `IS` band (`IS NULL`, `IS TRUE`,
    /// `IS DISTINCT FROM`, …). Per PostgreSQL Table 4.2 this binds *looser*
    /// than the comparison band, so it sits just above `AND`.
    pub(crate) const IS_PREC: u8 = 3;

    /// Precedence level of the postfix `IN` / `BETWEEN` band. Per
    /// PostgreSQL Table 4.2 this binds *tighter* than comparison but
    /// *looser* than the "all other operators" level (concat, bitwise,
    /// shift, …).
    pub(crate) const IN_BETWEEN_PREC: u8 = 5;

    /// `true` iff this operator is right-associative.
    ///
    /// PostgreSQL has no right-associative binary operators. In particular
    /// `^` (exponentiation) is **left**-associative there: `2 ^ 3 ^ 2`
    /// parses as `(2 ^ 3) ^ 2` = 64, not the maths-convention `2 ^ (3 ^ 2)`
    /// = 512. Matching that keeps chained-`^` results identical to Postgres.
    #[must_use]
    pub const fn is_right_associative(self) -> bool {
        // No PostgreSQL binary operator is right-associative.
        false
    }

    /// `true` iff this operator is a comparison operator valid for
    /// `ANY (subquery)` and `ALL (subquery)`.
    #[must_use]
    pub const fn is_comparison(self) -> bool {
        matches!(
            self,
            Self::Eq | Self::NotEq | Self::Lt | Self::LtEq | Self::Gt | Self::GtEq
        )
    }
}
