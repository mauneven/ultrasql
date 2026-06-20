//! Local path/glob expansion and object-store spec classification.

use std::fs;
use std::path::{Path, PathBuf};

use ultrasql_objectstore::is_object_store_uri;

use crate::error::ServerError;

use super::MAX_LOCAL_WILDCARD_PATTERN_CHARS;

pub(super) fn expand_parquet_path_specs(patterns: &[String]) -> Result<Vec<PathBuf>, ServerError> {
    if patterns.is_empty() {
        return Err(ServerError::CopyFormat(
            "read_parquet path list cannot be empty".to_owned(),
        ));
    }
    let mut paths = Vec::new();
    for pattern in patterns {
        paths.extend(expand_parquet_paths(pattern)?);
    }
    Ok(paths)
}

pub(super) fn path_specs_use_object_store(
    function_name: &str,
    patterns: &[String],
) -> Result<bool, ServerError> {
    let object_count = patterns
        .iter()
        .filter(|pattern| is_object_store_uri(pattern))
        .count();
    if object_count == 0 {
        return Ok(false);
    }
    if object_count == patterns.len() {
        return Ok(true);
    }
    Err(ServerError::CopyFormat(format!(
        "{function_name}: cannot mix local and object-store paths"
    )))
}

pub(super) fn expand_parquet_paths(pattern: &str) -> Result<Vec<PathBuf>, ServerError> {
    let path = Path::new(pattern);
    let file_pattern = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            ServerError::CopyFormat(format!(
                "read_parquet path must name a file or wildcard: {pattern}"
            ))
        })?;
    if !contains_wildcard(file_pattern) {
        return Ok(vec![path.to_path_buf()]);
    }
    validate_wildcard_pattern_len(file_pattern)?;

    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut paths = Vec::new();
    for entry in fs::read_dir(parent).map_err(|err| {
        ServerError::CopyFormat(format!(
            "read_parquet cannot read directory {}: {err}",
            parent.display()
        ))
    })? {
        let entry = entry.map_err(|err| ServerError::CopyFormat(format!("read_parquet: {err}")))?;
        let Some(name) = entry.file_name().to_str().map(ToOwned::to_owned) else {
            continue;
        };
        if wildcard_match(file_pattern, &name) {
            paths.push(entry.path());
        }
    }
    paths.sort();
    if paths.is_empty() {
        return Err(ServerError::CopyFormat(format!(
            "read_parquet pattern matched no files: {pattern}"
        )));
    }
    Ok(paths)
}

fn contains_wildcard(s: &str) -> bool {
    s.chars().any(|ch| matches!(ch, '*' | '?'))
}

fn validate_wildcard_pattern_len(file_pattern: &str) -> Result<(), ServerError> {
    let pattern_chars = file_pattern.chars().count();
    if pattern_chars > MAX_LOCAL_WILDCARD_PATTERN_CHARS {
        return Err(ServerError::CopyFormat(format!(
            "read_parquet wildcard pattern too long: chars={pattern_chars} limit={MAX_LOCAL_WILDCARD_PATTERN_CHARS}"
        )));
    }
    Ok(())
}

fn advance_index(index: &mut usize) -> bool {
    let Some(next) = index.checked_add(1) else {
        return false;
    };
    *index = next;
    true
}

pub(super) fn wildcard_match(pattern: &str, text: &str) -> bool {
    let pattern = pattern.chars().collect::<Vec<_>>();
    let text = text.chars().collect::<Vec<_>>();

    let mut pattern_idx = 0;
    let mut text_idx = 0;
    let mut last_star = None;
    let mut star_text_idx = 0;

    while let Some(&text_ch) = text.get(text_idx) {
        match pattern.get(pattern_idx).copied() {
            Some('?') => {
                if !advance_index(&mut pattern_idx) || !advance_index(&mut text_idx) {
                    return false;
                }
            }
            Some('*') => {
                last_star = Some(pattern_idx);
                if !advance_index(&mut pattern_idx) {
                    return false;
                }
                star_text_idx = text_idx;
            }
            Some(pattern_ch) if pattern_ch == text_ch => {
                if !advance_index(&mut pattern_idx) || !advance_index(&mut text_idx) {
                    return false;
                }
            }
            _ => {
                let Some(star_idx) = last_star else {
                    return false;
                };
                let Some(next_pattern_idx) = star_idx.checked_add(1) else {
                    return false;
                };
                pattern_idx = next_pattern_idx;
                if !advance_index(&mut star_text_idx) {
                    return false;
                }
                text_idx = star_text_idx;
            }
        }
    }

    while matches!(pattern.get(pattern_idx), Some('*')) {
        if !advance_index(&mut pattern_idx) {
            return false;
        }
    }
    pattern_idx == pattern.len()
}
