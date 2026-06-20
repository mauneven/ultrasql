//! Case execution, expectation comparison, and result-row formatting.

use std::collections::BTreeSet;

use anyhow::{Result, bail};
use tokio_postgres::types::Type;
use tokio_postgres::{Client, Row};

use crate::model::{QueryExpectation, SkipFilters, SortMode, StatementExpectation, TestCase, TestKind};
use crate::target::ReferenceTarget;

pub(crate) fn format_cli_reference_rows(
    stdout: &str,
    type_string: &str,
    sort_mode: SortMode,
) -> Result<Vec<String>> {
    let column_count = type_string.chars().count();
    if column_count == 0 {
        bail!("query type string must declare at least one column");
    }
    let values: Vec<String> = stdout
        .lines()
        .map(|line| line.strip_suffix('\r').unwrap_or(line).to_owned())
        .collect();
    if values.len() % column_count != 0 {
        bail!(
            "reference output produced {} values, not divisible by {column_count} column(s)",
            values.len()
        );
    }
    let mut rows: Vec<Vec<String>> = values
        .chunks(column_count)
        .map(<[String]>::to_vec)
        .collect();
    if matches!(sort_mode, SortMode::RowSort) {
        rows.sort();
    }
    Ok(rows.into_iter().flatten().collect())
}

#[derive(Debug)]
pub(crate) enum CaseOutcome {
    Passed,
    Skipped(String),
    Failed(String),
}

pub(crate) async fn run_case(
    client: &Client,
    references: &[ReferenceTarget],
    filters: &SkipFilters,
    enabled_features: &BTreeSet<String>,
    case: &TestCase,
) -> CaseOutcome {
    if let Some(reason) = effective_skip_reason(filters, enabled_features, case) {
        return CaseOutcome::Skipped(reason);
    }

    match &case.kind {
        TestKind::Statement { expectation, sql } => {
            run_statement_case(client, references, *expectation, sql).await
        }
        TestKind::Query {
            type_string,
            sort_mode,
            sql,
            expected,
        } => run_query_case(client, references, type_string, *sort_mode, sql, expected).await,
    }
}

pub(crate) fn effective_skip_reason(
    filters: &SkipFilters,
    enabled_features: &BTreeSet<String>,
    case: &TestCase,
) -> Option<String> {
    if let Some(reason) = &case.skip_reason {
        return Some(reason.clone());
    }
    if let Some(missing) = case
        .requires
        .iter()
        .find(|feature| !enabled_features.contains(feature.as_str()))
    {
        return Some(format!("missing feature `{missing}`"));
    }
    filters.skip_reason(&case.path, case.sql())
}

async fn run_statement_case(
    client: &Client,
    references: &[ReferenceTarget],
    expectation: StatementExpectation,
    sql: &str,
) -> CaseOutcome {
    let actual = client.batch_execute(sql).await;
    let actual_ok = actual.is_ok();
    let expected_ok = matches!(expectation, StatementExpectation::Ok);
    if actual_ok != expected_ok {
        let detail = actual.err().map_or_else(
            || "statement succeeded".to_owned(),
            |err| format_pg_error(&err),
        );
        return CaseOutcome::Failed(format!(
            "statement expectation mismatch: expected {:?}, got {detail}",
            expectation
        ));
    }

    for reference_client in references {
        let reference_ok = reference_client.execute_statement(sql).await.is_ok();
        if reference_ok != actual_ok {
            return CaseOutcome::Failed(format!(
                "reference statement class mismatch: UltraSQL ok={actual_ok}, reference ok={reference_ok}"
            ));
        }
    }

    CaseOutcome::Passed
}

async fn run_query_case(
    client: &Client,
    references: &[ReferenceTarget],
    type_string: &str,
    sort_mode: SortMode,
    sql: &str,
    expected: &QueryExpectation,
) -> CaseOutcome {
    let actual = match execute_query(client, type_string, sort_mode, sql).await {
        Ok(values) => values,
        Err(err) => return CaseOutcome::Failed(format!("query failed: {err}")),
    };

    if let Err(message) = compare_query_expectation(&actual, expected) {
        return CaseOutcome::Failed(format!("{message}\nactual values:\n{}", actual.join("\n")));
    }

    for reference_client in references {
        let reference_values = match reference_client
            .execute_query(type_string, sort_mode, sql)
            .await
        {
            Ok(values) => values,
            Err(err) => return CaseOutcome::Failed(format!("reference query failed: {err}")),
        };
        if reference_values != actual {
            return CaseOutcome::Failed(format!(
                "reference mismatch:\nreference values:\n{}\nactual values:\n{}",
                reference_values.join("\n"),
                actual.join("\n")
            ));
        }
    }

    CaseOutcome::Passed
}

pub(crate) fn compare_query_expectation(
    actual: &[String],
    expected: &QueryExpectation,
) -> Result<()> {
    match expected {
        QueryExpectation::Values(expected_values) => {
            if actual == expected_values {
                Ok(())
            } else {
                bail!("expected values:\n{}", expected_values.join("\n"));
            }
        }
        QueryExpectation::Hash {
            value_count,
            digest,
        } => {
            if actual.len() != *value_count {
                bail!(
                    "expected {value_count} hashed value(s), got {}",
                    actual.len()
                );
            }
            let actual_digest = hash_query_values(actual);
            if actual_digest == *digest {
                Ok(())
            } else {
                bail!("expected hash {digest}, got {actual_digest}");
            }
        }
    }
}

pub(crate) fn hash_query_values(values: &[String]) -> String {
    let mut repr = values.join("\n");
    repr.push('\n');
    format!("{:x}", md5::compute(repr.as_bytes()))
}

pub(crate) async fn execute_query(
    client: &Client,
    type_string: &str,
    sort_mode: SortMode,
    sql: &str,
) -> Result<Vec<String>> {
    let rows = client
        .query(sql, &[])
        .await
        .map_err(|err| anyhow::anyhow!("{}", format_pg_error(&err)))?;
    let expected_columns = type_string.chars().count();
    let mut formatted_rows = Vec::with_capacity(rows.len());
    for row in rows {
        if row.columns().len() != expected_columns {
            bail!(
                "query returned {} columns, type string declares {expected_columns}",
                row.columns().len()
            );
        }
        formatted_rows.push(format_row(&row)?);
    }
    if matches!(sort_mode, SortMode::RowSort) {
        formatted_rows.sort();
    }
    Ok(formatted_rows.into_iter().flatten().collect())
}

fn format_row(row: &Row) -> Result<Vec<String>> {
    let mut out = Vec::with_capacity(row.columns().len());
    for (idx, column) in row.columns().iter().enumerate() {
        out.push(format_cell(row, idx, column.type_())?);
    }
    Ok(out)
}

fn format_cell(row: &Row, idx: usize, ty: &Type) -> Result<String> {
    if *ty == Type::INT2 {
        let value: Option<i16> = row.try_get(idx)?;
        return Ok(format_nullable(value));
    }
    if *ty == Type::INT4 {
        let value: Option<i32> = row.try_get(idx)?;
        return Ok(format_nullable(value));
    }
    if *ty == Type::INT8 {
        let value: Option<i64> = row.try_get(idx)?;
        return Ok(format_nullable(value));
    }
    if *ty == Type::FLOAT4 {
        let value: Option<f32> = row.try_get(idx)?;
        return Ok(format_nullable(value));
    }
    if *ty == Type::FLOAT8 {
        let value: Option<f64> = row.try_get(idx)?;
        return Ok(format_nullable(value));
    }
    if *ty == Type::BOOL {
        let value: Option<bool> = row.try_get(idx)?;
        return Ok(value.map_or_else(|| "NULL".to_owned(), |v| v.to_string()));
    }
    if *ty == Type::TEXT || *ty == Type::VARCHAR || *ty == Type::BPCHAR || *ty == Type::NAME {
        let value: Option<String> = row.try_get(idx)?;
        return Ok(value.unwrap_or_else(|| "NULL".to_owned()));
    }
    bail!(
        "unsupported result type `{}` at column {}",
        ty.name(),
        idx.saturating_add(1)
    )
}

fn format_nullable<T: ToString>(value: Option<T>) -> String {
    value.map_or_else(|| "NULL".to_owned(), |v| v.to_string())
}

pub(crate) fn format_pg_error(err: &tokio_postgres::Error) -> String {
    if let Some(db_error) = err.as_db_error() {
        return format!("{}: {}", db_error.code().code(), db_error.message());
    }
    err.to_string()
}
