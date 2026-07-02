//! Literal formatters and the `INSERT ... VALUES` SQL builder used by the
//! wire loader ([`build_ultrasql_insert_sql`] and friends), gated on
//! `any(test, feature = "sql-bench")`.

#[cfg(any(test, feature = "sql-bench"))]
use anyhow::{Context, Result, bail};
#[cfg(any(test, feature = "sql-bench"))]
use std::fmt::Write as _;

#[cfg(any(test, feature = "sql-bench"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ColumnKind {
    Int,
    Text,
    Decimal,
    Date,
}

#[cfg(any(test, feature = "sql-bench"))]
pub(crate) fn column_kinds(table: &str) -> &'static [ColumnKind] {
    use ColumnKind::{Date, Decimal, Int, Text};

    match table {
        "region" => &[Int, Text, Text],
        "nation" => &[Int, Text, Int, Text],
        "supplier" => &[Int, Text, Text, Int, Text, Decimal, Text],
        "customer" => &[Int, Text, Text, Int, Text, Decimal, Text, Text],
        "part" => &[Int, Text, Text, Text, Text, Int, Text, Decimal, Text],
        "partsupp" => &[Int, Int, Int, Decimal, Text],
        "orders" => &[Int, Int, Text, Decimal, Date, Text, Text, Int, Text],
        "lineitem" => &[
            Int, Int, Int, Int, Decimal, Decimal, Decimal, Decimal, Text, Text, Date, Date, Date,
            Text, Text, Text,
        ],
        _ => &[],
    }
}

#[cfg(any(test, feature = "sql-bench"))]
pub(crate) fn escape_sql_text(text: &str) -> String {
    text.replace('\'', "''")
}

#[cfg(any(test, feature = "sql-bench"))]
pub(crate) fn format_ultrasql_literal(kind: ColumnKind, raw: &str) -> Result<String> {
    match kind {
        ColumnKind::Int => {
            raw.parse::<i64>()
                .with_context(|| format!("parse integer literal `{raw}`"))?;
            Ok(raw.to_owned())
        }
        ColumnKind::Decimal => {
            raw.parse::<f64>()
                .with_context(|| format!("parse decimal literal `{raw}`"))?;
            Ok(raw.to_owned())
        }
        ColumnKind::Date => Ok(format!("DATE '{}'", escape_sql_text(raw))),
        ColumnKind::Text => Ok(format!("'{}'", escape_sql_text(raw))),
    }
}

#[cfg(any(test, feature = "sql-bench"))]
pub(crate) fn build_ultrasql_insert_sql(table: &str, rows: &[Vec<String>]) -> Result<String> {
    let kinds = column_kinds(table);
    if kinds.is_empty() {
        bail!("unknown TPC-H table `{table}`");
    }
    let mut sql = String::new();
    write!(&mut sql, "INSERT INTO {table} VALUES ").context("format insert SQL prefix")?;
    for (row_idx, row) in rows.iter().enumerate() {
        if row.len() != kinds.len() {
            bail!(
                "{table}: row {} has {} fields, expected {}",
                row_idx + 1,
                row.len(),
                kinds.len()
            );
        }
        if row_idx > 0 {
            sql.push(',');
        }
        sql.push('(');
        for (col_idx, field) in row.iter().enumerate() {
            if col_idx > 0 {
                sql.push(',');
            }
            sql.push_str(&format_ultrasql_literal(kinds[col_idx], field)?);
        }
        sql.push(')');
    }
    Ok(sql)
}
