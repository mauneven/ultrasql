//! Case execution, expectation comparison, and result-row formatting.

use std::collections::BTreeSet;
use std::error::Error as StdError;

use anyhow::{Result, bail};
use tokio_postgres::types::{FromSql, Type};
use tokio_postgres::{Client, Row};
use ultrasql_core::{Value, decode_pg_numeric_binary, format_interval_pg, parse_interval_pg};

use crate::model::{
    QueryExpectation, SkipFilters, SortMode, StatementExpectation, TestCase, TestKind,
};
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
        formatted_rows.push(format_row(&row, type_string)?);
    }
    if matches!(sort_mode, SortMode::RowSort) {
        formatted_rows.sort();
    }
    Ok(formatted_rows.into_iter().flatten().collect())
}

fn format_row(row: &Row, type_string: &str) -> Result<Vec<String>> {
    let mut out = Vec::with_capacity(row.columns().len());
    for (idx, column) in row.columns().iter().enumerate() {
        // The declared sqllogictest type letter for this column (`I`, `R`,
        // `T`, `B`); drives NUMERIC formatting, which is type-directed in
        // canonical sqllogictest rather than value-directed.
        let declared = type_string.chars().nth(idx);
        out.push(format_cell(row, idx, column.type_(), declared)?);
    }
    Ok(out)
}

fn format_cell(row: &Row, idx: usize, ty: &Type, declared: Option<char>) -> Result<String> {
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
    if *ty == Type::NUMERIC {
        let value: Option<PgNumeric> = row.try_get(idx)?;
        return Ok(value.map_or_else(|| "NULL".to_owned(), |n| n.format_for(declared)));
    }
    if *ty == Type::INTERVAL {
        let value: Option<PgInterval> = row.try_get(idx)?;
        return Ok(value.map_or_else(|| "NULL".to_owned(), |v| v.0));
    }
    bail!(
        "unsupported result type `{}` at column {}",
        ty.name(),
        idx.saturating_add(1)
    )
}

/// A decoded PostgreSQL `numeric` result value (the exact scaled integer and
/// its display scale), formatted per the declared sqllogictest type letter.
///
/// The server advertises decimal columns as `numeric` (OID 1700) and
/// `tokio_postgres` requests them in binary, so the wire payload is PG binary
/// numeric (base-10000 digit groups). The decoder is robust to either form:
/// PG binary numeric or — as a fallback for text-format servers — ASCII text.
struct PgNumeric {
    value: i128,
    scale: i32,
}

impl PgNumeric {
    /// Render this numeric per the declared sqllogictest column type:
    /// - `I` (integer): the truncated integer part, matching the PG-derived
    ///   baselines (e.g. `AVG`-of-int `15`).
    /// - `R` (real): three decimal places, the canonical sqllogictest `R` form.
    /// - `T` / anything else: the canonical PG fixed-point numeric text.
    fn format_for(&self, declared: Option<char>) -> String {
        let decimal = Value::Decimal {
            value: self.value,
            scale: self.scale,
        };
        match declared {
            Some('I') => self.integer_part().to_string(),
            Some('R') => format!("{:.3}", self.as_f64()),
            _ => decimal.to_string(),
        }
    }

    /// Integer part via truncation toward zero (PG `numeric -> int` text under
    /// sqllogictest `query I` matches truncation for these baselines).
    fn integer_part(&self) -> i128 {
        if self.scale <= 0 {
            // Already an integer (possibly with implied trailing zeros).
            let mut v = self.value;
            for _ in 0..(-self.scale) {
                v = v.saturating_mul(10);
            }
            v
        } else {
            let divisor = 10_i128.pow(u32::try_from(self.scale).unwrap_or(u32::MAX));
            self.value / divisor
        }
    }

    fn as_f64(&self) -> f64 {
        (self.value as f64) / 10_f64.powi(self.scale)
    }
}

impl<'a> FromSql<'a> for PgNumeric {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn StdError + Sync + Send>> {
        // Binary PG numeric is the common case (tokio_postgres requests binary
        // results). Fall back to ASCII text for text-format servers.
        let decoded = decode_pg_numeric_binary(raw).or_else(|_| {
            let text = std::str::from_utf8(raw)?;
            ultrasql_core::parse_decimal_text(text, None)
                .map_err(|err| Box::<dyn StdError + Sync + Send>::from(err.to_string()))
        })?;
        let Value::Decimal { value, scale } = decoded else {
            return Err("decoded numeric was not a decimal value".into());
        };
        Ok(Self { value, scale })
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::NUMERIC
    }
}

/// A decoded PostgreSQL `interval` (OID 1186), rendered as canonical interval
/// text. The server advertises OID 1186 and `tokio_postgres` requests binary,
/// so the wire payload is PG binary interval (`int64 microseconds || int32
/// days || int32 months`, network byte order). The decoder also accepts the
/// canonical text form as a fallback, then re-renders it through the shared
/// formatter so the runner sees exactly what a typed libpq client decodes.
struct PgInterval(String);

impl<'a> FromSql<'a> for PgInterval {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn StdError + Sync + Send>> {
        if raw.len() == 16 {
            let micros = i64::from_be_bytes(raw[0..8].try_into()?);
            let days = i32::from_be_bytes(raw[8..12].try_into()?);
            let months = i32::from_be_bytes(raw[12..16].try_into()?);
            return Ok(Self(format_interval_pg(months, days, micros)));
        }
        let text = std::str::from_utf8(raw)?;
        let (months, days, micros) =
            parse_interval_pg(text).ok_or_else(|| format!("invalid interval text {text:?}"))?;
        Ok(Self(format_interval_pg(months, days, micros)))
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::INTERVAL
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use ultrasql_core::encode_pg_numeric_binary;

    #[test]
    fn numeric_query_i_renders_truncated_integer_part() {
        // AVG(int) materialises as numeric `15` (scale 0); `query I` -> "15".
        let n = PgNumeric {
            value: 15,
            scale: 0,
        };
        assert_eq!(n.format_for(Some('I')), "15");

        // A fractional numeric under `query I` truncates toward zero.
        let frac = PgNumeric {
            value: 1599,
            scale: 2,
        };
        assert_eq!(frac.format_for(Some('I')), "15");
    }

    #[test]
    fn numeric_query_r_renders_three_decimals() {
        let n = PgNumeric {
            value: 15,
            scale: 1,
        };
        assert_eq!(n.format_for(Some('R')), "1.500");
    }

    #[test]
    fn numeric_query_t_renders_canonical_text() {
        let n = PgNumeric {
            value: 1230,
            scale: 2,
        };
        assert_eq!(n.format_for(Some('T')), "12.30");
    }

    #[test]
    fn numeric_negative_scale_query_i_appends_trailing_zeros() {
        // Scale -1 means the stored value carries an implied trailing zero.
        let n = PgNumeric {
            value: 15,
            scale: -1,
        };
        assert_eq!(n.format_for(Some('I')), "150");
    }

    #[test]
    fn from_sql_decodes_pg_binary_numeric() {
        let payload = encode_pg_numeric_binary(150, 1).expect("encode 15.0");
        let decoded = PgNumeric::from_sql(&Type::NUMERIC, &payload).expect("decode binary numeric");
        assert_eq!(decoded.value, 150);
        assert_eq!(decoded.scale, 1);
        assert_eq!(decoded.format_for(Some('I')), "15");
        assert_eq!(decoded.format_for(Some('T')), "15.0");
    }

    #[test]
    fn from_sql_falls_back_to_ascii_text() {
        let decoded = PgNumeric::from_sql(&Type::NUMERIC, b"12.30")
            .expect("decode ascii-text numeric fallback");
        assert_eq!(decoded.format_for(Some('T')), "12.30");
        assert_eq!(decoded.format_for(Some('I')), "12");
    }
}
