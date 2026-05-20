//! Fast JSONB ingest helpers for COPY paths.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};

use serde_json::Value as JsonValue;

use crate::error::ServerError;

const JSONB_BINARY_VERSION: u8 = 1;
const JSONB_SHAPE_CACHE_MAX_ENTRIES: usize = 1024;

#[derive(Debug)]
struct JsonbShapeEntry {
    rows_seen: u64,
    last_payload_len: usize,
}

/// Session-local cache of repeated JSONB structural shapes.
#[derive(Debug)]
pub(super) struct JsonbShapeCache {
    entries: HashMap<u64, JsonbShapeEntry>,
    max_entries: usize,
    hits: u64,
    misses: u64,
}

impl Default for JsonbShapeCache {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            max_entries: JSONB_SHAPE_CACHE_MAX_ENTRIES,
            hits: 0,
            misses: 0,
        }
    }
}

impl JsonbShapeCache {
    /// Parse JSONB text through `simd-json`, canonicalise it, and record
    /// its structural shape for repeated COPY rows.
    pub(super) fn parse_text(
        &mut self,
        bytes: &[u8],
        column_idx: usize,
    ) -> Result<String, ServerError> {
        let value = parse_json_value(bytes, column_idx)?;
        self.observe_shape(&value, bytes.len());
        serde_json::to_string(&value).map_err(|err| {
            ServerError::CopyFormat(format!("column {column_idx}: cannot encode jsonb: {err}"))
        })
    }

    /// Decode PostgreSQL's binary JSONB wire shape: one version byte
    /// followed by UTF-8 JSON text.
    pub(super) fn parse_pg_binary(
        &mut self,
        bytes: &[u8],
        column_idx: usize,
    ) -> Result<String, ServerError> {
        let Some((&version, payload)) = bytes.split_first() else {
            return Err(ServerError::CopyFormat(format!(
                "column {column_idx}: empty binary jsonb field"
            )));
        };
        if version != JSONB_BINARY_VERSION {
            return Err(ServerError::CopyFormat(format!(
                "column {column_idx}: unsupported binary jsonb version {version}"
            )));
        }
        self.parse_text(payload, column_idx)
    }

    #[cfg(test)]
    fn stats(&self) -> (u64, u64, usize) {
        (self.hits, self.misses, self.entries.len())
    }

    fn observe_shape(&mut self, value: &JsonValue, payload_len: usize) {
        let fingerprint = json_shape_fingerprint(value);
        if let Some(shape) = self.entries.get_mut(&fingerprint) {
            self.hits = self.hits.saturating_add(1);
            shape.rows_seen = shape.rows_seen.saturating_add(1);
            shape.last_payload_len = payload_len;
            return;
        }
        self.misses = self.misses.saturating_add(1);
        if self.entries.len() >= self.max_entries {
            self.entries.clear();
        }
        self.entries.insert(
            fingerprint,
            JsonbShapeEntry {
                rows_seen: 1,
                last_payload_len: payload_len,
            },
        );
    }
}

pub(super) fn encode_pg_binary_jsonb(text: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(text.len() + 1);
    out.push(JSONB_BINARY_VERSION);
    out.extend_from_slice(text.as_bytes());
    out
}

fn parse_json_value(bytes: &[u8], column_idx: usize) -> Result<JsonValue, ServerError> {
    let mut scratch = bytes.to_vec();
    simd_json::serde::from_slice::<JsonValue>(&mut scratch).map_err(|err| {
        ServerError::CopyFormat(format!("column {column_idx}: invalid jsonb: {err}"))
    })
}

fn json_shape_fingerprint(value: &JsonValue) -> u64 {
    let mut hasher = DefaultHasher::new();
    hash_json_shape(value, &mut hasher);
    hasher.finish()
}

fn hash_json_shape(value: &JsonValue, state: &mut DefaultHasher) {
    match value {
        JsonValue::Null => 0_u8.hash(state),
        JsonValue::Bool(_) => 1_u8.hash(state),
        JsonValue::Number(number) => {
            2_u8.hash(state);
            number.is_i64().hash(state);
            number.is_u64().hash(state);
            number.is_f64().hash(state);
        }
        JsonValue::String(_) => 3_u8.hash(state),
        JsonValue::Array(values) => {
            4_u8.hash(state);
            values.len().hash(state);
            for value in values.iter().take(32) {
                hash_json_shape(value, state);
            }
            if values.len() > 32 {
                0xff_u8.hash(state);
            }
        }
        JsonValue::Object(map) => {
            5_u8.hash(state);
            map.len().hash(state);
            for (key, value) in map {
                key.hash(state);
                hash_json_shape(value, state);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_canonicalises_and_reuses_shape_cache() {
        let mut cache = JsonbShapeCache::default();

        let first = cache
            .parse_text(br#"{"b":"x","a":1}"#, 0)
            .expect("first parse");
        let second = cache
            .parse_text(br#"{"b":"y","a":2}"#, 0)
            .expect("second parse");

        assert_eq!(first, r#"{"a":1,"b":"x"}"#);
        assert_eq!(second, r#"{"a":2,"b":"y"}"#);
        assert_eq!(cache.stats(), (1, 1, 1));
    }

    #[test]
    fn binary_jsonb_requires_supported_version() {
        let mut cache = JsonbShapeCache::default();

        let err = cache
            .parse_pg_binary(&[2, b'{', b'}'], 3)
            .expect_err("unsupported version");

        assert!(
            err.to_string()
                .contains("column 3: unsupported binary jsonb version 2")
        );
    }
}
