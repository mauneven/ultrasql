//! `LIKE` / `ILIKE` pattern matching.
//!
//! Extracted verbatim from the original `eval.rs`; pure code motion.

use std::cell::RefCell;
use std::collections::HashMap;

use super::*;

/// Upper bound on cached compiled LIKE patterns per thread. Mirrors the
/// regex cache bound: large enough for the handful of distinct constant
/// patterns a query holds, small enough to cap memory if patterns vary.
const MAX_LIKE_CACHE_ENTRIES: usize = 256;

thread_local! {
    /// Memoised `compile_like_pattern` results keyed by the pattern text and
    /// the case-insensitivity flag. The common `col LIKE 'literal'` shape
    /// re-presents the same pattern on every row, so this turns a per-row
    /// recompile into a single compile plus a cheap clone of the token Vec.
    static LIKE_PATTERN_CACHE: RefCell<HashMap<(String, bool), Vec<LikeToken>>> =
        RefCell::new(HashMap::new());
}

/// Compile `pattern` to LIKE tokens, reusing a cached compile when the same
/// `(pattern, case_insensitive)` pair was seen before on this thread.
///
/// Behaviourally identical to calling [`compile_like_pattern`] directly:
/// the function is pure, so a cached token Vec is the same value a fresh
/// compile would produce.
fn cached_like_pattern(pattern: &str, case_insensitive: bool) -> Vec<LikeToken> {
    LIKE_PATTERN_CACHE.with(|cache| {
        if let Some(found) = cache.borrow().get(&(pattern.to_owned(), case_insensitive)) {
            return found.clone();
        }
        let compiled = compile_like_pattern(pattern, case_insensitive);
        let mut map = cache.borrow_mut();
        if map.len() >= MAX_LIKE_CACHE_ENTRIES {
            map.clear();
        }
        map.insert((pattern.to_owned(), case_insensitive), compiled.clone());
        compiled
    })
}

// ---------------------------------------------------------------------------
// LIKE / ILIKE pattern matching
// ---------------------------------------------------------------------------

/// Match `haystack` against a SQL LIKE/ILIKE `pattern`.
///
/// `%` matches any sequence of characters (including empty). `_`
/// matches exactly one character. Backslash escapes `%`, `_`, and
/// backslash itself to match PostgreSQL's default `LIKE` escape behavior.
pub(crate) fn like_match(haystack: &str, pattern: &str, case_insensitive: bool) -> bool {
    // Collect to chars so we handle multi-byte UTF-8 correctly.
    let h: Vec<char> = if case_insensitive {
        haystack
            .chars()
            .map(|c| c.to_lowercase().next().unwrap_or(c))
            .collect()
    } else {
        haystack.chars().collect()
    };
    let p = cached_like_pattern(pattern, case_insensitive);
    like_match_tokens(&h, &p)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LikeToken {
    AnySeq,
    AnyOne,
    Literal(char),
}

pub(crate) fn compile_like_pattern(pattern: &str, case_insensitive: bool) -> Vec<LikeToken> {
    let mut tokens = Vec::with_capacity(pattern.chars().count());
    let mut chars = pattern.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '%' => tokens.push(LikeToken::AnySeq),
            '_' => tokens.push(LikeToken::AnyOne),
            '\\' => {
                let literal = chars.next().unwrap_or('\\');
                tokens.push(LikeToken::Literal(fold_like_char(
                    literal,
                    case_insensitive,
                )));
            }
            literal => tokens.push(LikeToken::Literal(fold_like_char(
                literal,
                case_insensitive,
            ))),
        }
    }
    tokens
}

pub(crate) fn fold_like_char(ch: char, case_insensitive: bool) -> char {
    if case_insensitive {
        ch.to_lowercase().next().unwrap_or(ch)
    } else {
        ch
    }
}

pub(crate) fn like_match_tokens(haystack: &[char], pattern: &[LikeToken]) -> bool {
    let mut prev = vec![false; pattern.len() + 1];
    prev[0] = true;
    for (idx, token) in pattern.iter().enumerate() {
        if *token == LikeToken::AnySeq {
            prev[idx + 1] = prev[idx];
        }
    }

    // Reuse two swap buffers across haystack characters instead of
    // allocating a fresh `next` Vec per character. `next` is reset to all
    // `false` at the start of each iteration, matching the original
    // semantics where it began life as `vec![false; ..]`.
    let mut next = vec![false; pattern.len() + 1];
    for &ch in haystack {
        next.iter_mut().for_each(|cell| *cell = false);
        for (idx, token) in pattern.iter().enumerate() {
            let col = idx + 1;
            next[col] = match token {
                LikeToken::AnySeq => next[col - 1] || prev[col],
                LikeToken::AnyOne => prev[col - 1],
                LikeToken::Literal(literal) => prev[col - 1] && ch == *literal,
            }
        }
        std::mem::swap(&mut prev, &mut next);
    }
    prev[pattern.len()]
}

pub(crate) fn regex_match(
    haystack: &str,
    pattern: &str,
    case_insensitive: bool,
) -> Result<bool, EvalError> {
    super::regex_cache::cached_regex(pattern, case_insensitive)
        .map(|regex| regex.is_match(haystack))
        .map_err(|err| EvalError::Type(format!("regex operator: invalid pattern: {err}")))
}
