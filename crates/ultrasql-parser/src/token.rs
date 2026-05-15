//! SQL token kinds and the `Token` type.
//!
//! `TokenKind` enumerates every token the lexer can produce. Most
//! variants are unit (the value is the kind itself); literal-bearing
//! variants reference their text via the `Token`'s span, which is
//! cheaper to copy than the text.

use crate::span::Span;

/// Tokens.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Token {
    /// What kind of token this is.
    pub kind: TokenKind,
    /// Source span — byte offsets into the original SQL text.
    pub span: Span,
}

impl Token {
    /// Construct a token.
    #[inline]
    #[must_use]
    pub const fn new(kind: TokenKind, span: Span) -> Self {
        Self { kind, span }
    }

    /// Convenience: textual slice of the token from the original source.
    #[inline]
    #[must_use]
    pub fn text<'src>(&self, source: &'src str) -> Option<&'src str> {
        self.span.slice(source)
    }
}

/// Kinds of tokens the lexer can produce.
///
/// The enum is `Copy` and used as a hash-map key during keyword lookup.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[allow(clippy::module_name_repetitions, clippy::too_many_lines)]
pub enum TokenKind {
    // ---- structural ----------------------------------------------------
    /// End of input.
    Eof,

    // ---- literals ------------------------------------------------------
    /// Integer literal (decimal, hex `0x`, octal `0o`, binary `0b`).
    Integer,
    /// Floating-point literal: `1.5`, `.5`, `1e10`, `1.5e-3`.
    Float,
    /// Single-quoted string literal: `'hello'`. Embedded `''` becomes
    /// a single `'`.
    String,
    /// E-prefixed string with C-style escapes: `E'\n\t'`.
    EscapedString,
    /// Dollar-quoted string: `$tag$body$tag$`.
    DollarString,
    /// Boolean / NULL keywords come through as their keyword tokens, not
    /// as literal kinds.

    // ---- identifiers ---------------------------------------------------
    /// Unquoted identifier (case-folded to lower case by convention,
    /// though the lexer preserves the raw span; case-folding is done at
    /// keyword lookup time).
    Identifier,
    /// Quoted identifier (`"col Name"`). Case is preserved verbatim.
    QuotedIdentifier,
    /// Positional parameter: `$1`, `$2`, ...
    Parameter,

    // ---- punctuation ---------------------------------------------------
    /// `(`
    LParen,
    /// `)`
    RParen,
    /// `[`
    LBracket,
    /// `]`
    RBracket,
    /// `,`
    Comma,
    /// `;`
    Semicolon,
    /// `.`
    Dot,
    /// `::`
    ColonColon,
    /// `:`
    Colon,
    /// `*` (also an operator; the parser routes by context).
    Star,
    /// `?` — driver-style positional placeholder. PostgreSQL uses `$N`;
    /// we keep `?` for compatibility with ODBC-style clients.
    QuestionMark,

    // ---- operators -----------------------------------------------------
    /// `+`
    Plus,
    /// `-`
    Minus,
    /// `/`
    Slash,
    /// `%`
    Percent,
    /// `^`
    Caret,
    /// `=`
    Eq,
    /// `<>`, `!=`
    NotEq,
    /// `<`
    Lt,
    /// `<=`
    LtEq,
    /// `>`
    Gt,
    /// `>=`
    GtEq,
    /// `||` — string / array concatenation in PostgreSQL.
    Concat,
    /// `->` — JSON object access.
    Arrow,
    /// `->>` — JSON object access returning text.
    ArrowDouble,
    /// `#>` — JSON path access.
    HashArrow,
    /// `#>>` — JSON path access returning text.
    HashArrowDouble,
    /// `@>` — `contains`.
    AtArrow,
    /// `<@` — `contained by`.
    ArrowAt,
    /// `~`  — POSIX regex match (also unary bitwise NOT in prefix position).
    Tilde,
    /// `!~` — POSIX regex non-match.
    NotTilde,
    /// `~*` — case-insensitive POSIX regex match.
    TildeStar,
    /// `!~*` — case-insensitive POSIX regex non-match.
    NotTildeStar,
    /// `&` — bitwise AND.
    Ampersand,
    /// `|` — bitwise OR.
    Pipe,
    /// `#` — bitwise XOR (PostgreSQL).
    Hash,
    /// `<<` — bitwise shift left.
    ShiftLeft,
    /// `>>` — bitwise shift right.
    ShiftRight,
    /// `?|` — JSON has-any-key.
    QuestionPipe,
    /// `?&` — JSON has-all-keys.
    QuestionAmpersand,

    // ---- keywords (sorted alphabetically for sanity) --------------------
    KwAbort,
    KwAll,
    KwAlter,
    KwAnalyze,
    KwAnd,
    KwAny,
    KwArray,
    KwAs,
    KwAsc,
    KwBegin,
    KwBetween,
    KwBigint,
    KwBoolean,
    KwBoth,
    KwBy,
    KwCase,
    /// `CASCADE` — used in `TRUNCATE … CASCADE` and DDL.
    KwCascade,
    KwCast,
    KwChar,
    KwCharacter,
    KwCheck,
    KwCollate,
    KwColumn,
    KwCommit,
    /// `CONFLICT` — used in `ON CONFLICT`.
    KwConflict,
    KwConstraint,
    KwCreate,
    KwCross,
    KwCurrentDate,
    KwCurrentTime,
    KwCurrentTimestamp,
    KwDate,
    KwDecimal,
    KwDefault,
    KwDelete,
    KwDesc,
    KwDistinct,
    KwDo,
    KwDouble,
    KwDrop,
    KwElse,
    KwEnd,
    KwEscape,
    KwExcept,
    KwExists,
    KwExplain,
    KwFalse,
    KwFetch,
    KwFilter,
    KwFloat,
    KwFor,
    KwForeign,
    KwFrom,
    KwFull,
    KwGroup,
    KwGrouping,
    KwHaving,
    KwIlike,
    KwIn,
    KwIndex,
    /// `IDENTITY` — used in `TRUNCATE … RESTART IDENTITY`.
    KwIdentity,
    KwInner,
    KwInsert,
    KwInt,
    KwInteger,
    KwIntersect,
    KwInterval,
    KwInto,
    KwIs,
    KwIsolation,
    KwJoin,
    KwKey,
    KwLateral,
    KwLeading,
    KwLeft,
    KwLike,
    KwLimit,
    KwNatural,
    KwNot,
    /// `NOTHING` — used in `ON CONFLICT DO NOTHING`.
    KwNothing,
    KwNull,
    KwNulls,
    KwNumeric,
    KwOf,
    KwOffset,
    KwOn,
    KwOnly,
    KwOr,
    KwOrder,
    KwOuter,
    KwOver,
    KwPartition,
    KwPlacing,
    KwPrecision,
    KwPrimary,
    KwReal,
    KwReferences,
    /// `RESTART` — used in `TRUNCATE … RESTART IDENTITY`.
    KwRestart,
    KwReturning,
    KwRight,
    KwRollback,
    KwRow,
    KwRows,
    KwSavepoint,
    KwSelect,
    KwSerializable,
    KwSet,
    KwShow,
    KwSimilar,
    KwSmallint,
    KwSome,
    KwSymmetric,
    KwTable,
    KwTemp,
    KwTemporary,
    KwText,
    KwThen,
    KwTime,
    KwTimestamp,
    KwTrailing,
    KwTransaction,
    /// `PREPARED` — used in `COMMIT PREPARED` / `ROLLBACK PREPARED` / `PREPARE TRANSACTION`.
    KwPrepared,
    KwTrue,
    KwTruncate,
    KwUnion,
    KwUnique,
    KwUnknown,
    KwUpdate,
    KwUsing,
    KwValues,
    KwVarchar,
    KwVacuum,
    KwWhen,
    KwWhere,
    KwWindow,
    KwWith,
    KwWithin,
    // ---- DDL keywords added for v0.2 -------------------------------------
    /// `ADD` — used in `ALTER TABLE … ADD COLUMN / ADD CONSTRAINT`.
    KwAdd,
    /// `CACHE` — used in `CREATE SEQUENCE … CACHE n`.
    KwCache,
    /// `CYCLE` — used in `CREATE SEQUENCE … CYCLE / NO CYCLE`.
    KwCycle,
    /// `IF` — used in `CREATE TABLE IF NOT EXISTS` / `DROP TABLE IF EXISTS`.
    KwIf,
    /// `INCLUDE` — used in `CREATE INDEX … INCLUDE (col, …)`.
    KwInclude,
    /// `INCREMENT` — used in `CREATE SEQUENCE … INCREMENT BY n`.
    KwIncrement,
    /// `LOCAL` — used in `SET LOCAL …`.
    KwLocal,
    /// `MAXVALUE` — used in `CREATE SEQUENCE … MAXVALUE n`.
    KwMaxvalue,
    /// `MINVALUE` — used in `CREATE SEQUENCE … MINVALUE n`.
    KwMinvalue,
    /// `NO` — used in `NO CYCLE`, `NO MINVALUE`, `NO MAXVALUE`.
    KwNo,
    /// `REINDEX` — `REINDEX TABLE / INDEX name`.
    KwReindex,
    /// `RENAME` — used in `ALTER TABLE … RENAME COLUMN / RENAME TO`.
    KwRename,
    /// `RESET` — used in `RESET var`.
    KwReset,
    /// `RESTRICT` — used in `DROP TABLE … RESTRICT`.
    KwRestrict,
    /// `SCHEMA` — used in `CREATE SCHEMA` / `DROP SCHEMA`.
    KwSchema,
    /// `SEQUENCE` — used in `CREATE SEQUENCE` / `DROP SEQUENCE`.
    KwSequence,
    /// `SESSION` — used in `SET SESSION …`.
    KwSession,
    /// `START` — used in `CREATE SEQUENCE … START WITH n`.
    KwStart,
    /// `TO` — used in `SET search_path TO …` and `RENAME … TO`.
    KwTo,
    // ---- SELECT completeness keywords added for v0.2 ---------------------
    /// `DEALLOCATE` — `DEALLOCATE [ALL | name]`.
    KwDeallocate,
    /// `EXECUTE` — `EXECUTE name [(args)]`.
    KwExecute,
    /// `FORMAT` — `EXPLAIN (FORMAT TEXT|JSON)`.
    KwFormat,
    /// `JSON` — `EXPLAIN (FORMAT JSON)`.
    KwJson,
    /// `PREPARE` — `PREPARE name AS stmt`.
    KwPrepare,
    /// `RECURSIVE` — `WITH RECURSIVE`.
    KwRecursive,
    /// `RELEASE` — `RELEASE [SAVEPOINT] name`.
    KwRelease,
    /// `VERBOSE` — `EXPLAIN VERBOSE`.
    KwVerbose,
    // ---- Expression-completeness keywords added for v0.2 (wave 4) ----------
    /// `AT` — used in `expr AT TIME ZONE zone`.
    KwAt,
    /// `COALESCE` — `COALESCE(a, b, …)`.
    KwCoalesce,
    /// `GREATEST` — `GREATEST(a, b, …)`.
    KwGreatest,
    /// `LEAST` — `LEAST(a, b, …)`.
    KwLeast,
    /// `NULLIF` — `NULLIF(a, b)`.
    KwNullif,
    /// `OVERLAPS` — `(a, b) OVERLAPS (c, d)`.
    KwOverlaps,
    /// `ZONE` — used in `AT TIME ZONE`.
    KwZone,
    // ---- SELECT locking keywords added for v0.4 ----------------------------
    /// `LOCKED` — `SKIP LOCKED` wait policy.
    KwLocked,
    /// `NOWAIT` — `FOR UPDATE NOWAIT` wait policy.
    KwNowait,
    /// `SHARE` — `FOR SHARE` lock strength.
    KwShare,
    /// `SKIP` — `SKIP LOCKED` wait policy.
    KwSkip,
    // ---- Isolation level keywords added for v0.4 ----------------------------
    /// `COMMITTED` — `READ COMMITTED`.
    KwCommitted,
    /// `LEVEL` — `ISOLATION LEVEL`.
    KwLevel,
    /// `READ` — `READ COMMITTED / READ UNCOMMITTED`.
    KwRead,
    /// `REPEATABLE` — `REPEATABLE READ`.
    KwRepeatable,
    /// `UNCOMMITTED` — `READ UNCOMMITTED` (aliased to READ COMMITTED).
    KwUncommitted,
}

impl TokenKind {
    /// Whether this kind corresponds to a SQL keyword.
    #[must_use]
    pub const fn is_keyword(self) -> bool {
        // Keyword variants begin at KwAbort. We exploit the structural
        // ordering to make this a single integer comparison.
        (self as u32) >= (Self::KwAbort as u32)
    }

    /// Whether this kind is a literal — string or number.
    #[must_use]
    pub const fn is_literal(self) -> bool {
        matches!(
            self,
            Self::Integer | Self::Float | Self::String | Self::EscapedString | Self::DollarString
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyword_classifier() {
        assert!(TokenKind::KwSelect.is_keyword());
        assert!(TokenKind::KwAbort.is_keyword());
        assert!(!TokenKind::Identifier.is_keyword());
        assert!(!TokenKind::Eq.is_keyword());
        assert!(!TokenKind::Integer.is_keyword());
    }

    #[test]
    fn literal_classifier() {
        assert!(TokenKind::Integer.is_literal());
        assert!(TokenKind::Float.is_literal());
        assert!(TokenKind::String.is_literal());
        assert!(!TokenKind::Identifier.is_literal());
        assert!(!TokenKind::KwSelect.is_literal());
    }

    #[test]
    fn token_text_retrieves_slice() {
        let src = "SELECT *";
        let tok = Token::new(TokenKind::KwSelect, Span::new(0, 6));
        assert_eq!(tok.text(src), Some("SELECT"));
    }
}
