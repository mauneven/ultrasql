//! Path-spec parsing, glob expansion, and local-file opening shared by the
//! file-reading table functions.

use std::fs::{self, File};
use std::path::{Path, PathBuf};

use ultrasql_core::{DataType, Value};
use ultrasql_objectstore::is_object_store_uri;

use super::{PlanError, ScalarExpr};

pub(super) fn read_file_path_specs(
    function_name: &str,
    arg: &ScalarExpr,
) -> Result<Vec<String>, PlanError> {
    match arg {
        ScalarExpr::Literal {
            value: Value::Text(pattern),
            ..
        } => Ok(vec![pattern.clone()]),
        ScalarExpr::Literal {
            value:
                Value::Array {
                    element_type,
                    elements,
                },
            ..
        } if matches!(element_type, &DataType::Text { max_len: None }) => elements
            .iter()
            .map(|value| match value {
                Value::Text(path) => Ok(path.clone()),
                _ => Err(PlanError::TypeMismatch(format!(
                    "{function_name}: path-list elements must be string literals"
                ))),
            })
            .collect(),
        _ => Err(PlanError::TypeMismatch(format!(
            "{function_name}: argument must be a string literal or text array literal"
        ))),
    }
}

pub(super) fn open_local_regular_file(function_name: &str, path: &Path) -> Result<File, PlanError> {
    let metadata = fs::symlink_metadata(path).map_err(|err| {
        PlanError::TypeMismatch(format!(
            "{function_name} cannot inspect {}: {err}",
            path.display()
        ))
    })?;
    if !metadata.file_type().is_file() {
        return Err(PlanError::TypeMismatch(format!(
            "{function_name} path is not a regular file: {}",
            path.display()
        )));
    }
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW);
    }
    options.open(path).map_err(|err| {
        PlanError::TypeMismatch(format!(
            "{function_name} cannot open {}: {err}",
            path.display()
        ))
    })
}

pub(super) fn path_specs_use_object_store(
    function_name: &str,
    path_specs: &[String],
) -> Result<bool, PlanError> {
    let object_count = path_specs
        .iter()
        .filter(|spec| is_object_store_uri(spec))
        .count();
    if object_count == 0 {
        return Ok(false);
    }
    if object_count == path_specs.len() {
        return Ok(true);
    }
    Err(PlanError::TypeMismatch(format!(
        "{function_name}: cannot mix local and object-store paths"
    )))
}

pub(super) fn expand_file_path_specs(
    function_name: &str,
    patterns: &[String],
) -> Result<Vec<PathBuf>, PlanError> {
    if patterns.is_empty() {
        return Err(PlanError::TypeMismatch(format!(
            "{function_name}: path list cannot be empty"
        )));
    }
    let mut paths = Vec::new();
    for pattern in patterns {
        paths.extend(expand_file_paths(function_name, pattern)?);
    }
    Ok(paths)
}

pub(super) fn first_expanded_file(
    function_name: &str,
    patterns: &[String],
) -> Result<PathBuf, PlanError> {
    expand_file_path_specs(function_name, patterns)?
        .into_iter()
        .next()
        .ok_or_else(|| {
            PlanError::TypeMismatch(format!("{function_name}: path expansion produced no files"))
        })
}

pub(super) fn expand_file_paths(
    function_name: &str,
    pattern: &str,
) -> Result<Vec<PathBuf>, PlanError> {
    let path = Path::new(pattern);
    let file_pattern = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            PlanError::TypeMismatch(format!(
                "{function_name}: path must name a file or wildcard: {pattern}"
            ))
        })?;
    if !contains_wildcard(file_pattern) {
        return Ok(vec![path.to_path_buf()]);
    }

    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(parent).map_err(|err| {
        PlanError::TypeMismatch(format!(
            "{function_name}: cannot read directory {}: {err}",
            parent.display()
        ))
    })? {
        let entry =
            entry.map_err(|err| PlanError::TypeMismatch(format!("{function_name}: {err}")))?;
        let Some(name) = entry.file_name().to_str().map(ToOwned::to_owned) else {
            continue;
        };
        if wildcard_match(file_pattern, &name) {
            paths.push(entry.path());
        }
    }
    paths.sort();
    if paths.is_empty() {
        return Err(PlanError::TypeMismatch(format!(
            "{function_name}: pattern matched no files: {pattern}"
        )));
    }
    Ok(paths)
}

pub(super) fn contains_wildcard(s: &str) -> bool {
    s.chars().any(|ch| matches!(ch, '*' | '?'))
}

pub(super) fn wildcard_match(pattern: &str, text: &str) -> bool {
    let pattern = pattern.chars().collect::<Vec<_>>();
    let text = text.chars().collect::<Vec<_>>();
    let mut dp = vec![vec![false; text.len() + 1]; pattern.len() + 1];
    dp[0][0] = true;
    for (i, ch) in pattern.iter().enumerate() {
        if *ch == '*' {
            dp[i + 1][0] = dp[i][0];
        }
    }
    for (i, pattern_ch) in pattern.iter().enumerate() {
        for (j, text_ch) in text.iter().enumerate() {
            dp[i + 1][j + 1] = match pattern_ch {
                '*' => dp[i][j + 1] || dp[i + 1][j],
                '?' => dp[i][j],
                ch => dp[i][j] && ch == text_ch,
            };
        }
    }
    dp[pattern.len()][text.len()]
}
