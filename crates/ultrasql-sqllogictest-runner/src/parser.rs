//! SQLLogicTest script parsing and input-file collection.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use crate::model::{
    Directives, QueryExpectation, SortMode, StatementExpectation, TestCase, TestKind,
};

pub(crate) fn collect_input_files(paths: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let roots = if paths.is_empty() {
        vec![PathBuf::from("tests/slt")]
    } else {
        paths.to_vec()
    };
    let mut files = Vec::new();
    for root in roots {
        collect_path(&root, &mut files)?;
    }
    files.sort();
    Ok(files)
}

fn collect_path(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    if path.is_file() {
        if is_slt_file(path) {
            files.push(path.to_path_buf());
        }
        return Ok(());
    }
    if !path.is_dir() {
        bail!("test path does not exist: {}", path.display());
    }
    let mut entries = std::fs::read_dir(path)
        .with_context(|| format!("read directory {}", path.display()))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("read directory entry {}", path.display()))?;
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        collect_path(&entry.path(), files)?;
    }
    Ok(())
}

pub(crate) fn is_slt_file(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| matches!(ext, "slt" | "test"))
}

pub(crate) fn parse_script(path: &Path, text: &str) -> Result<Vec<TestCase>> {
    let lines: Vec<&str> = text.lines().collect();
    let mut idx = 0;
    let mut cases = Vec::new();
    let mut directives = Directives::default();

    while idx < lines.len() {
        let line_no = idx.saturating_add(1);
        let line = lines[idx].trim();
        idx = idx.saturating_add(1);
        if line.is_empty() {
            continue;
        }
        if parse_directive(line, &mut directives)? {
            continue;
        }
        if line.starts_with('#') {
            continue;
        }
        if line.starts_with("hash-threshold") {
            continue;
        }

        if let Some(rest) = line.strip_prefix("statement") {
            let expectation = parse_statement_expectation(path, line_no, rest)?;
            let (sql, next_idx) = collect_until_blank(&lines, idx);
            idx = next_idx;
            let (skip_reason, requires) = directives.take_for_case();
            cases.push(TestCase {
                path: path.to_path_buf(),
                line: line_no,
                kind: TestKind::Statement { expectation, sql },
                skip_reason,
                requires,
            });
            continue;
        }

        if let Some(rest) = line.strip_prefix("query") {
            let (type_string, sort_mode) = parse_query_header(path, line_no, rest)?;
            let (sql, expected, next_idx) = collect_query(&lines, idx)
                .with_context(|| format!("{}:{line_no} parse query", path.display()))?;
            idx = next_idx;
            let (skip_reason, requires) = directives.take_for_case();
            cases.push(TestCase {
                path: path.to_path_buf(),
                line: line_no,
                kind: TestKind::Query {
                    type_string,
                    sort_mode,
                    sql,
                    expected,
                },
                skip_reason,
                requires,
            });
            continue;
        }

        bail!(
            "{}:{line_no} unsupported SQLLogicTest directive `{line}`",
            path.display()
        );
    }

    Ok(cases)
}

pub(crate) fn parse_directive(line: &str, directives: &mut Directives) -> Result<bool> {
    let Some(rest) = line.strip_prefix("# ultrasql:") else {
        return Ok(false);
    };
    let rest = rest.trim();
    if rest == "skip" || rest.starts_with("skip ") {
        let reason = rest.strip_prefix("skip").unwrap_or_default().trim();
        if reason.is_empty() {
            bail!("skip directive requires an explicit reason");
        }
        directives.next_skip = Some(reason.to_owned());
        return Ok(true);
    }
    if let Some(feature) = rest.strip_prefix("require ") {
        directives.next_requires.push(feature.trim().to_owned());
        return Ok(true);
    }
    if rest == "file-skip" || rest.starts_with("file-skip ") {
        let reason = rest.strip_prefix("file-skip").unwrap_or_default().trim();
        if reason.is_empty() {
            bail!("file-skip directive requires an explicit reason");
        }
        directives.file_skip = Some(reason.to_owned());
        return Ok(true);
    }
    if let Some(feature) = rest.strip_prefix("file-require ") {
        directives.file_requires.push(feature.trim().to_owned());
        return Ok(true);
    }
    bail!("unknown UltraSQL SLT directive `{rest}`")
}

fn parse_statement_expectation(
    path: &Path,
    line_no: usize,
    rest: &str,
) -> Result<StatementExpectation> {
    match rest.split_whitespace().next() {
        Some("ok") => Ok(StatementExpectation::Ok),
        Some("error") => Ok(StatementExpectation::Error),
        other => bail!(
            "{}:{line_no} statement must declare `ok` or `error`, got {:?}",
            path.display(),
            other
        ),
    }
}

pub(crate) fn parse_query_header(
    path: &Path,
    line_no: usize,
    rest: &str,
) -> Result<(String, SortMode)> {
    let mut tokens = rest.split_whitespace();
    let type_string = tokens
        .next()
        .ok_or_else(|| anyhow::anyhow!("{}:{line_no} query missing type string", path.display()))?
        .to_owned();
    let mut sort_mode = SortMode::NoSort;
    for token in tokens {
        match token {
            "nosort" => sort_mode = SortMode::NoSort,
            "sort" | "rowsort" => sort_mode = SortMode::RowSort,
            _ => {}
        }
    }
    Ok((type_string, sort_mode))
}

pub(crate) fn collect_until_blank(lines: &[&str], mut idx: usize) -> (String, usize) {
    let mut sql = Vec::new();
    while idx < lines.len() {
        let line = lines[idx];
        idx = idx.saturating_add(1);
        if line.trim().is_empty() {
            break;
        }
        sql.push(line);
    }
    (sql.join("\n"), idx)
}

pub(crate) fn collect_query(
    lines: &[&str],
    mut idx: usize,
) -> Result<(String, QueryExpectation, usize)> {
    let mut sql = Vec::new();
    while idx < lines.len() {
        let line = lines[idx];
        idx = idx.saturating_add(1);
        if line.trim() == "----" {
            let (expected, next_idx) = collect_expected(lines, idx);
            return Ok((
                sql.join("\n"),
                parse_query_expectation(&expected)?,
                next_idx,
            ));
        }
        sql.push(line);
    }
    bail!("query missing ---- separator")
}

fn parse_query_expectation(lines: &[String]) -> Result<QueryExpectation> {
    if lines.len() == 1 {
        let line = lines[0].trim();
        if let Some((count, digest)) = parse_hash_expectation(line)? {
            return Ok(QueryExpectation::Hash {
                value_count: count,
                digest,
            });
        }
    }
    Ok(QueryExpectation::Values(lines.to_vec()))
}

fn parse_hash_expectation(line: &str) -> Result<Option<(usize, String)>> {
    let Some((count, digest)) = line.split_once(" values hashing to ") else {
        return Ok(None);
    };
    let value_count = count
        .parse::<usize>()
        .with_context(|| format!("invalid hashed value count `{count}`"))?;
    if digest.len() != 32 || !digest.chars().all(|ch| ch.is_ascii_hexdigit()) {
        bail!("invalid SQLLogicTest MD5 digest `{digest}`");
    }
    Ok(Some((value_count, digest.to_ascii_lowercase())))
}

fn collect_expected(lines: &[&str], mut idx: usize) -> (Vec<String>, usize) {
    let mut expected = Vec::new();
    while idx < lines.len() {
        let line = lines[idx];
        idx = idx.saturating_add(1);
        if line.trim().is_empty() {
            break;
        }
        expected.push(line.trim_end().to_owned());
    }
    (expected, idx)
}
