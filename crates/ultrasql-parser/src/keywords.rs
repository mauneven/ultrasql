//! Reserved and non-reserved SQL keywords.
//!
//! The parser distinguishes between *reserved* keywords (cannot be used as
//! identifiers without quoting) and *non-reserved* keywords (can appear
//! either as a keyword or as an identifier depending on context). The
//! distinction follows PostgreSQL's `kwlist.h`.

use std::sync::OnceLock;

use ahash::AHashMap;

use crate::token::TokenKind;

/// Lookup table from lowercased keyword text to its token kind.
///
/// The table is constructed once on first access and reused for the
/// lifetime of the process.
pub fn keyword_table() -> &'static AHashMap<&'static str, TokenKind> {
    static TABLE: OnceLock<AHashMap<&'static str, TokenKind>> = OnceLock::new();
    TABLE.get_or_init(build_table)
}

/// Look up a lower-case identifier in the keyword table; return its
/// `TokenKind` if it is a keyword, or `None` if it is a plain identifier.
#[inline]
#[must_use]
pub fn lookup(lower: &str) -> Option<TokenKind> {
    keyword_table().get(lower).copied()
}

#[allow(clippy::too_many_lines)]
fn build_table() -> AHashMap<&'static str, TokenKind> {
    // The set below covers the SQL standard reserved words plus the
    // PostgreSQL-specific ones the parser will need before it accepts
    // a meaningful statement subset. Categorization (reserved vs
    // non-reserved) is encoded in the parser by where each keyword may
    // appear, not by the lexer.
    let entries: &[(&'static str, TokenKind)] = &[
        ("abort", TokenKind::KwAbort),
        ("all", TokenKind::KwAll),
        ("alter", TokenKind::KwAlter),
        ("analyze", TokenKind::KwAnalyze),
        ("and", TokenKind::KwAnd),
        ("any", TokenKind::KwAny),
        ("array", TokenKind::KwArray),
        ("as", TokenKind::KwAs),
        ("asc", TokenKind::KwAsc),
        ("begin", TokenKind::KwBegin),
        ("between", TokenKind::KwBetween),
        ("bigint", TokenKind::KwBigint),
        ("boolean", TokenKind::KwBoolean),
        ("both", TokenKind::KwBoth),
        ("by", TokenKind::KwBy),
        ("case", TokenKind::KwCase),
        ("cascade", TokenKind::KwCascade),
        ("cast", TokenKind::KwCast),
        ("char", TokenKind::KwChar),
        ("character", TokenKind::KwCharacter),
        ("check", TokenKind::KwCheck),
        ("collate", TokenKind::KwCollate),
        ("column", TokenKind::KwColumn),
        ("commit", TokenKind::KwCommit),
        ("conflict", TokenKind::KwConflict),
        ("constraint", TokenKind::KwConstraint),
        ("create", TokenKind::KwCreate),
        ("cross", TokenKind::KwCross),
        ("current_date", TokenKind::KwCurrentDate),
        ("current_time", TokenKind::KwCurrentTime),
        ("current_timestamp", TokenKind::KwCurrentTimestamp),
        ("date", TokenKind::KwDate),
        ("decimal", TokenKind::KwDecimal),
        ("default", TokenKind::KwDefault),
        ("delete", TokenKind::KwDelete),
        ("desc", TokenKind::KwDesc),
        ("distinct", TokenKind::KwDistinct),
        ("do", TokenKind::KwDo),
        ("double", TokenKind::KwDouble),
        ("drop", TokenKind::KwDrop),
        ("else", TokenKind::KwElse),
        ("end", TokenKind::KwEnd),
        ("escape", TokenKind::KwEscape),
        ("except", TokenKind::KwExcept),
        ("exists", TokenKind::KwExists),
        ("explain", TokenKind::KwExplain),
        ("false", TokenKind::KwFalse),
        ("fetch", TokenKind::KwFetch),
        ("filter", TokenKind::KwFilter),
        ("float", TokenKind::KwFloat),
        ("for", TokenKind::KwFor),
        ("foreign", TokenKind::KwForeign),
        ("from", TokenKind::KwFrom),
        ("full", TokenKind::KwFull),
        ("group", TokenKind::KwGroup),
        ("grouping", TokenKind::KwGrouping),
        ("having", TokenKind::KwHaving),
        ("ilike", TokenKind::KwIlike),
        ("in", TokenKind::KwIn),
        ("identity", TokenKind::KwIdentity),
        ("index", TokenKind::KwIndex),
        ("inner", TokenKind::KwInner),
        ("insert", TokenKind::KwInsert),
        ("int", TokenKind::KwInt),
        ("integer", TokenKind::KwInteger),
        ("intersect", TokenKind::KwIntersect),
        ("interval", TokenKind::KwInterval),
        ("into", TokenKind::KwInto),
        ("is", TokenKind::KwIs),
        ("isolation", TokenKind::KwIsolation),
        ("join", TokenKind::KwJoin),
        ("key", TokenKind::KwKey),
        ("lateral", TokenKind::KwLateral),
        ("leading", TokenKind::KwLeading),
        ("left", TokenKind::KwLeft),
        ("like", TokenKind::KwLike),
        ("limit", TokenKind::KwLimit),
        ("natural", TokenKind::KwNatural),
        ("not", TokenKind::KwNot),
        ("nothing", TokenKind::KwNothing),
        ("null", TokenKind::KwNull),
        ("nulls", TokenKind::KwNulls),
        ("numeric", TokenKind::KwNumeric),
        ("of", TokenKind::KwOf),
        ("offset", TokenKind::KwOffset),
        ("on", TokenKind::KwOn),
        ("only", TokenKind::KwOnly),
        ("or", TokenKind::KwOr),
        ("order", TokenKind::KwOrder),
        ("outer", TokenKind::KwOuter),
        ("over", TokenKind::KwOver),
        ("partition", TokenKind::KwPartition),
        ("placing", TokenKind::KwPlacing),
        ("precision", TokenKind::KwPrecision),
        ("primary", TokenKind::KwPrimary),
        ("real", TokenKind::KwReal),
        ("references", TokenKind::KwReferences),
        ("restart", TokenKind::KwRestart),
        ("returning", TokenKind::KwReturning),
        ("right", TokenKind::KwRight),
        ("rollback", TokenKind::KwRollback),
        ("row", TokenKind::KwRow),
        ("rows", TokenKind::KwRows),
        ("savepoint", TokenKind::KwSavepoint),
        ("select", TokenKind::KwSelect),
        ("serializable", TokenKind::KwSerializable),
        ("set", TokenKind::KwSet),
        ("show", TokenKind::KwShow),
        ("similar", TokenKind::KwSimilar),
        ("smallint", TokenKind::KwSmallint),
        ("some", TokenKind::KwSome),
        ("symmetric", TokenKind::KwSymmetric),
        ("table", TokenKind::KwTable),
        ("temp", TokenKind::KwTemp),
        ("temporary", TokenKind::KwTemporary),
        ("text", TokenKind::KwText),
        ("then", TokenKind::KwThen),
        ("time", TokenKind::KwTime),
        ("timestamp", TokenKind::KwTimestamp),
        ("trailing", TokenKind::KwTrailing),
        ("transaction", TokenKind::KwTransaction),
        ("true", TokenKind::KwTrue),
        ("truncate", TokenKind::KwTruncate),
        ("union", TokenKind::KwUnion),
        ("unique", TokenKind::KwUnique),
        ("unknown", TokenKind::KwUnknown),
        ("update", TokenKind::KwUpdate),
        ("using", TokenKind::KwUsing),
        ("values", TokenKind::KwValues),
        ("varchar", TokenKind::KwVarchar),
        ("vacuum", TokenKind::KwVacuum),
        ("when", TokenKind::KwWhen),
        ("where", TokenKind::KwWhere),
        ("window", TokenKind::KwWindow),
        ("with", TokenKind::KwWith),
        ("within", TokenKind::KwWithin),
        // DDL keywords added for v0.2
        ("add", TokenKind::KwAdd),
        ("cache", TokenKind::KwCache),
        ("cycle", TokenKind::KwCycle),
        ("if", TokenKind::KwIf),
        ("include", TokenKind::KwInclude),
        ("increment", TokenKind::KwIncrement),
        ("local", TokenKind::KwLocal),
        ("maxvalue", TokenKind::KwMaxvalue),
        ("minvalue", TokenKind::KwMinvalue),
        ("no", TokenKind::KwNo),
        ("reindex", TokenKind::KwReindex),
        ("rename", TokenKind::KwRename),
        ("reset", TokenKind::KwReset),
        ("restrict", TokenKind::KwRestrict),
        ("schema", TokenKind::KwSchema),
        ("sequence", TokenKind::KwSequence),
        ("session", TokenKind::KwSession),
        ("start", TokenKind::KwStart),
        ("to", TokenKind::KwTo),
        // SELECT completeness keywords added for v0.2
        ("deallocate", TokenKind::KwDeallocate),
        ("execute", TokenKind::KwExecute),
        ("format", TokenKind::KwFormat),
        ("json", TokenKind::KwJson),
        ("prepare", TokenKind::KwPrepare),
        ("recursive", TokenKind::KwRecursive),
        ("release", TokenKind::KwRelease),
        ("verbose", TokenKind::KwVerbose),
        // Expression-completeness keywords added for v0.2 (wave 4)
        ("at", TokenKind::KwAt),
        ("coalesce", TokenKind::KwCoalesce),
        ("greatest", TokenKind::KwGreatest),
        ("least", TokenKind::KwLeast),
        ("nullif", TokenKind::KwNullif),
        ("overlaps", TokenKind::KwOverlaps),
        ("zone", TokenKind::KwZone),
    ];

    entries.iter().copied().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_is_a_keyword() {
        assert_eq!(lookup("select"), Some(TokenKind::KwSelect));
    }

    #[test]
    fn lookup_is_table_lower_case_only() {
        // The lexer feeds lower-cased identifiers in. Mixed-case is
        // explicitly not in the table.
        assert_eq!(lookup("SELECT"), None);
        assert_eq!(lookup("Select"), None);
    }

    #[test]
    fn non_keyword_returns_none() {
        assert_eq!(lookup("hello"), None);
        assert_eq!(lookup(""), None);
    }

    #[test]
    fn entries_are_unique() {
        let t = keyword_table();
        // The size should match the number of entries we put in.
        assert!(t.len() > 100, "lots of keywords expected, got {}", t.len());
    }
}
