//! Thread-local cache of compiled regexes.
//!
//! The row-at-a-time interpreter calls the `~` / `~*` operators and
//! `regexp_replace` once per row. When the pattern operand is a constant
//! (the common `col ~ 'literal'` shape), the *same* pattern string is
//! compiled on every row. Compiling a [`regex::Regex`] is comparatively
//! expensive, so we memoise successful compiles keyed by the pattern text
//! and the case-insensitivity flag.
//!
//! Behaviour is identical to compiling inline: a cache hit returns a clone
//! of the previously compiled regex (cloning a `Regex` is cheap — it shares
//! the compiled program behind an `Arc`), and a compile *error* is never
//! cached, so an invalid pattern produces the same error on every row just
//! as before. A non-constant pattern simply misses on each distinct string.
//!
//! The cache is per-thread and bounded; if it would exceed [`MAX_ENTRIES`]
//! it is cleared before inserting, which bounds memory for genuinely
//! varying patterns without changing results.

use std::cell::RefCell;
use std::collections::HashMap;

/// Upper bound on cached compiled regexes per thread. Large enough that a
/// handful of distinct constant patterns in one query all stay resident;
/// small enough that a pathological varying-pattern workload cannot grow
/// memory without bound.
const MAX_ENTRIES: usize = 256;

thread_local! {
    static REGEX_CACHE: RefCell<HashMap<(String, bool), regex::Regex>> =
        RefCell::new(HashMap::new());
}

/// Return a compiled regex for `(pattern, case_insensitive)`, reusing a
/// cached compile when available.
///
/// On a cache miss the pattern is compiled with the same
/// [`regex::RegexBuilder`] settings the callers previously used inline.
/// Successful compiles are cached; compile errors are propagated to the
/// caller (via `build`) and never cached.
pub(crate) fn cached_regex(
    pattern: &str,
    case_insensitive: bool,
) -> Result<regex::Regex, regex::Error> {
    REGEX_CACHE.with(|cache| {
        if let Some(found) = cache.borrow().get(&(pattern.to_owned(), case_insensitive)) {
            return Ok(found.clone());
        }
        let compiled = regex::RegexBuilder::new(pattern)
            .case_insensitive(case_insensitive)
            .build()?;
        let mut map = cache.borrow_mut();
        if map.len() >= MAX_ENTRIES {
            map.clear();
        }
        map.insert((pattern.to_owned(), case_insensitive), compiled.clone());
        Ok(compiled)
    })
}
