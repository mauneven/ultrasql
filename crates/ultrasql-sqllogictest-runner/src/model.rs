//! Parsed SQLLogicTest model: cases, expectations, skip filters, and directives.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StatementExpectation {
    Ok,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SortMode {
    NoSort,
    RowSort,
}

#[derive(Clone, Debug)]
pub(crate) enum TestKind {
    Statement {
        expectation: StatementExpectation,
        sql: String,
    },
    Query {
        type_string: String,
        sort_mode: SortMode,
        sql: String,
        expected: QueryExpectation,
    },
}

#[derive(Clone, Debug)]
pub(crate) enum QueryExpectation {
    Values(Vec<String>),
    Hash { value_count: usize, digest: String },
}

#[derive(Clone, Debug)]
pub(crate) struct TestCase {
    pub(crate) path: PathBuf,
    pub(crate) line: usize,
    pub(crate) kind: TestKind,
    pub(crate) skip_reason: Option<String>,
    pub(crate) requires: Vec<String>,
}

impl TestCase {
    pub(crate) fn sql(&self) -> &str {
        match &self.kind {
            TestKind::Statement { sql, .. } | TestKind::Query { sql, .. } => sql,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SkipPattern {
    pub(crate) pattern: String,
    pub(crate) reason: String,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SkipFilters {
    pub(crate) patterns: Vec<SkipPattern>,
}

impl SkipFilters {
    pub(crate) fn load_all(paths: &[PathBuf]) -> Result<Self> {
        let mut filters = Self::default();
        for path in paths {
            if !path.exists() {
                continue;
            }
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("read skip filter {}", path.display()))?;
            for (idx, raw_line) in text.lines().enumerate() {
                let trimmed = raw_line.trim();
                if trimmed.is_empty() || trimmed.starts_with('#') {
                    continue;
                }
                let Some((pattern, reason)) = raw_line.split_once('\t') else {
                    bail!(
                        "{}:{} skip filter requires `pattern<TAB>reason`",
                        path.display(),
                        idx + 1
                    );
                };
                let pattern = pattern.trim();
                if pattern.is_empty() {
                    bail!("{}:{} empty skip pattern", path.display(), idx + 1);
                }
                let reason = reason.trim();
                if reason.is_empty() {
                    bail!(
                        "{}:{} skip filter requires an explicit reason",
                        path.display(),
                        idx + 1
                    );
                }
                filters.patterns.push(SkipPattern {
                    pattern: pattern.to_owned(),
                    reason: reason.to_owned(),
                });
            }
        }
        Ok(filters)
    }

    pub(crate) fn skip_reason(&self, path: &Path, sql: &str) -> Option<String> {
        let path = path.to_string_lossy();
        self.patterns.iter().find_map(|filter| {
            if sql.contains(&filter.pattern) || path.contains(&filter.pattern) {
                Some(format!("{} ({})", filter.reason, filter.pattern))
            } else {
                None
            }
        })
    }
}

#[derive(Debug, Default)]
pub(crate) struct Directives {
    pub(crate) file_skip: Option<String>,
    pub(crate) file_requires: Vec<String>,
    pub(crate) next_skip: Option<String>,
    pub(crate) next_requires: Vec<String>,
}

impl Directives {
    pub(crate) fn take_for_case(&mut self) -> (Option<String>, Vec<String>) {
        let skip = self.file_skip.clone().or_else(|| self.next_skip.take());
        let mut requires = self.file_requires.clone();
        requires.append(&mut self.next_requires);
        (skip, requires)
    }
}

#[derive(Debug, Default)]
pub(crate) struct Summary {
    pub(crate) files: u64,
    pub(crate) cases: u64,
    pub(crate) passed: u64,
    pub(crate) failed: u64,
    pub(crate) skipped: u64,
}
