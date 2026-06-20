//! GIN (Generalized Inverted Index) scaffold.
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::type_complexity)]

use parking_lot::Mutex;
use ultrasql_core::TupleId;

use super::{AccessMethod, AccessMethodError};

#[derive(Debug, Default)]
struct GinStorage {
    postings: std::collections::BTreeMap<Vec<u8>, Vec<TupleId>>,
    pending: Vec<(Vec<u8>, TupleId)>,
}

impl GinStorage {
    fn drain_pending(&mut self) -> usize {
        let drained = self.pending.len();
        for (token, tid) in self.pending.drain(..) {
            self.postings.entry(token).or_default().push(tid);
        }
        drained
    }
}

// GIN (Generalized Inverted Index) scaffold
// ---------------------------------------------------------------------------

/// GIN (Generalized Inverted Index) scaffold.
///
/// GIN indexes an item (document, array, JSON) as a set of tokens and
/// maintains a per-token posting list. Inserts use fast-update mode by
/// default: tokens first land in a pending list, then [`Self::drain_pending_list`]
/// merges them into the main posting tree.
///
/// # Status
///
/// The current implementation owns posting lists and pending-list draining.
/// Type-specific JSONB/array/TSVECTOR extraction and full posting-tree page
/// storage remain separate operator-class work.
#[derive(Debug)]
pub struct GinIndex {
    /// Posting lists and fast-update pending list.
    storage: Mutex<GinStorage>,
    /// Whether inserts append to the pending list before a drain.
    fast_update: bool,
}

impl Default for GinIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl GinIndex {
    /// Create an empty GIN index with fast-update mode enabled.
    #[must_use]
    pub fn new() -> Self {
        Self {
            storage: Mutex::new(GinStorage::default()),
            fast_update: true,
        }
    }

    /// Merge every pending-list item into the main posting lists.
    ///
    /// Returns the number of pending items drained.
    pub fn drain_pending_list(&self) -> usize {
        self.storage.lock().drain_pending()
    }

    /// Current pending-list length.
    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.storage.lock().pending.len()
    }

    /// Tokenize and insert one JSONB document for GIN containment/key probes.
    pub fn insert_jsonb_document(&self, json: &str, tid: TupleId) -> Result<(), AccessMethodError> {
        for token in gin_jsonb_document_tokens(json) {
            self.insert(&token, tid)?;
        }
        Ok(())
    }

    /// Probe JSONB containment (`@>`) by intersecting query tokens.
    pub fn lookup_jsonb_contains(&self, query: &str) -> Result<Vec<TupleId>, AccessMethodError> {
        self.lookup_all_tokens(&gin_jsonb_document_tokens(query))
    }

    /// Probe JSONB any-key existence (`?|`).
    pub fn lookup_jsonb_has_any_key(
        &self,
        keys: &[String],
    ) -> Result<Vec<TupleId>, AccessMethodError> {
        let tokens: Vec<Vec<u8>> = keys.iter().map(|key| gin_token("json:key", key)).collect();
        self.lookup_any_token(&tokens)
    }

    /// Probe JSONB all-key existence (`?&`).
    pub fn lookup_jsonb_has_all_keys(
        &self,
        keys: &[String],
    ) -> Result<Vec<TupleId>, AccessMethodError> {
        let tokens: Vec<Vec<u8>> = keys.iter().map(|key| gin_token("json:key", key)).collect();
        self.lookup_all_tokens(&tokens)
    }

    /// Tokenize and insert one SQL array value for GIN array probes.
    pub fn insert_array_value(&self, array: &str, tid: TupleId) -> Result<(), AccessMethodError> {
        for token in gin_array_tokens(array) {
            self.insert(&token, tid)?;
        }
        Ok(())
    }

    /// Probe array containment (`@>`) by intersecting member tokens.
    pub fn lookup_array_contains(&self, query: &str) -> Result<Vec<TupleId>, AccessMethodError> {
        self.lookup_all_tokens(&gin_array_tokens(query))
    }

    /// Probe array overlap (`&&`) by unioning member-token postings.
    pub fn lookup_array_overlap(&self, query: &str) -> Result<Vec<TupleId>, AccessMethodError> {
        self.lookup_any_token(&gin_array_tokens(query))
    }

    /// Tokenize and insert one `TSVECTOR` value for GIN full-text probes.
    pub fn insert_tsvector(&self, tsvector: &str, tid: TupleId) -> Result<(), AccessMethodError> {
        for token in gin_tsvector_tokens(tsvector) {
            self.insert(&token, tid)?;
        }
        Ok(())
    }

    /// Probe `TSVECTOR @@ TSQUERY` by intersecting query term tokens.
    pub fn lookup_tsquery_match(&self, tsquery: &str) -> Result<Vec<TupleId>, AccessMethodError> {
        self.lookup_all_tokens(&gin_tsvector_tokens(tsquery))
    }

    fn lookup_all_tokens(&self, tokens: &[Vec<u8>]) -> Result<Vec<TupleId>, AccessMethodError> {
        let Some((first, rest)) = tokens.split_first() else {
            return Ok(Vec::new());
        };
        let mut out = self.lookup(first)?;
        for token in rest {
            let postings = self.lookup(token)?;
            out.retain(|tid| postings.contains(tid));
        }
        out.sort_unstable();
        out.dedup();
        Ok(out)
    }

    fn lookup_any_token(&self, tokens: &[Vec<u8>]) -> Result<Vec<TupleId>, AccessMethodError> {
        let mut out = Vec::new();
        for token in tokens {
            out.extend(self.lookup(token)?);
        }
        out.sort_unstable();
        out.dedup();
        Ok(out)
    }
}

fn gin_token(prefix: &str, value: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(prefix.len() + 1 + value.len());
    out.extend_from_slice(prefix.as_bytes());
    out.push(0);
    out.extend_from_slice(value.as_bytes());
    out
}

fn gin_jsonb_document_tokens(json: &str) -> Vec<Vec<u8>> {
    let mut tokens = Vec::new();
    for (key, value) in gin_json_object_pairs(json) {
        tokens.push(gin_token("json:key", &key));
        let mut pair = gin_token("json:pair", &key);
        pair.push(0);
        pair.extend_from_slice(value.as_bytes());
        tokens.push(pair);
    }
    if tokens.is_empty() {
        tokens.extend(
            gin_split_loose_list(json)
                .into_iter()
                .map(|value| gin_token("json:elem", &value)),
        );
    }
    tokens.sort();
    tokens.dedup();
    tokens
}

fn gin_array_tokens(array: &str) -> Vec<Vec<u8>> {
    let mut tokens: Vec<Vec<u8>> = gin_split_loose_list(array)
        .into_iter()
        .map(|value| gin_token("array:elem", &value))
        .collect();
    tokens.sort();
    tokens.dedup();
    tokens
}

fn gin_tsvector_tokens(text: &str) -> Vec<Vec<u8>> {
    let mut tokens: Vec<Vec<u8>> = text
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|term| !term.is_empty())
        .map(|term| gin_token("ts:term", &term.to_ascii_lowercase()))
        .collect();
    tokens.sort();
    tokens.dedup();
    tokens
}

fn gin_json_object_pairs(text: &str) -> Vec<(String, String)> {
    let trimmed = text.trim();
    let Some(body) = trimmed.strip_prefix('{').and_then(|s| s.strip_suffix('}')) else {
        return Vec::new();
    };
    split_top_level_commas(body)
        .into_iter()
        .filter_map(|part| {
            let (key, value) = part.split_once(':')?;
            Some((unquote_json_scalar(key), unquote_json_scalar(value)))
        })
        .collect()
}

fn gin_split_loose_list(text: &str) -> Vec<String> {
    let trimmed = text.trim();
    let body = trimmed
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .or_else(|| trimmed.strip_prefix('{').and_then(|s| s.strip_suffix('}')))
        .unwrap_or(trimmed);
    split_top_level_commas(body)
        .into_iter()
        .map(unquote_json_scalar)
        .filter(|part| !part.is_empty())
        .collect()
}

fn split_top_level_commas(text: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut in_string = false;
    let mut escaped = false;
    for (idx, ch) in text.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' if in_string => escaped = true,
            '"' => in_string = !in_string,
            ',' if !in_string => {
                parts.push(text[start..idx].trim());
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(text[start..].trim());
    parts
}

fn unquote_json_scalar(text: &str) -> String {
    let trimmed = text.trim();
    if let Some(inner) = trimmed.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        inner.replace("\\\"", "\"").replace("\\\\", "\\")
    } else {
        trimmed.to_owned()
    }
}

impl AccessMethod for GinIndex {
    fn name(&self) -> &'static str {
        "gin"
    }

    fn insert(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        let mut storage = self.storage.lock();
        if self.fast_update {
            storage.pending.push((key.to_vec(), tid));
        } else {
            storage.postings.entry(key.to_vec()).or_default().push(tid);
        }
        Ok(())
    }

    fn lookup(&self, key: &[u8]) -> Result<Vec<TupleId>, AccessMethodError> {
        let mut storage = self.storage.lock();
        if self.fast_update {
            storage.drain_pending();
        }
        Ok(storage.postings.get(key).cloned().unwrap_or_default())
    }

    fn delete(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        let mut storage = self.storage.lock();
        if self.fast_update {
            storage.drain_pending();
        }
        match storage.postings.get_mut(key) {
            None => Err(AccessMethodError::NotFound),
            Some(list) => {
                let before = list.len();
                list.retain(|t| *t != tid);
                if list.len() < before {
                    Ok(())
                } else {
                    Err(AccessMethodError::NotFound)
                }
            }
        }
    }
}
