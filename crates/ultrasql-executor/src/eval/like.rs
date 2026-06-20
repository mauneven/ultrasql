//! `LIKE` / `ILIKE` pattern matching.
//!
//! Extracted verbatim from the original `eval.rs`; pure code motion.

use super::*;

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
    let p = compile_like_pattern(pattern, case_insensitive);
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

    for &ch in haystack {
        let mut next = vec![false; pattern.len() + 1];
        for (idx, token) in pattern.iter().enumerate() {
            let col = idx + 1;
            next[col] = match token {
                LikeToken::AnySeq => next[col - 1] || prev[col],
                LikeToken::AnyOne => prev[col - 1],
                LikeToken::Literal(literal) => prev[col - 1] && ch == *literal,
            }
        }
        prev = next;
    }
    prev[pattern.len()]
}

pub(crate) fn regex_match(
    haystack: &str,
    pattern: &str,
    case_insensitive: bool,
) -> Result<bool, EvalError> {
    regex::RegexBuilder::new(pattern)
        .case_insensitive(case_insensitive)
        .build()
        .map(|regex| regex.is_match(haystack))
        .map_err(|err| EvalError::Type(format!("regex operator: invalid pattern: {err}")))
}
