//! Part of the `session` module split. The
//! `impl<RW> Session<RW>` block is reopened here to add a handful
//! of methods to the type defined in `session/mod.rs`. Splitting
//! across files keeps every unit under the 600-line ceiling without
//! changing semantics.

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_catalog::{Catalog, CatalogSnapshot, StatisticExtRow, TableEntry};
use ultrasql_core::{
    BlockNumber, DataType, Oid, RelationId, Schema, Value, Xid, timestamptz_display_in_timezone,
};
use ultrasql_mvcc::XidStatusOracle;
use ultrasql_optimizer::{InMemoryStatsCatalog, PlanCacheKey, StatsCatalog, StatsSource};
use ultrasql_parser::Parser;
use ultrasql_planner::{
    BinaryOp, LogicalDescribeObjectKind, LogicalDescribeTarget, LogicalPlan,
    LogicalReferentialAction, LogicalSetVariableAction, ScalarExpr, bind,
};
use ultrasql_protocol::{BackendMessage, FieldDescription};
use ultrasql_storage::access_method::{AccessMethod, BrinIndex};
use ultrasql_storage::btree::BTree;
use ultrasql_storage::heap::{
    DeleteInt32PairScan, DeleteInt32PairStamp, InsertOptions, Int32PairCmp, Int32PairPredicate,
};
use ultrasql_txn::{IsolationLevel, Transaction};

use super::{PendingLogicalChange, Session};
use crate::auth::AuthCatalog;
use crate::error::ServerError;
use crate::replication::LogicalChangeKind;
use crate::result_encoder::{self, SelectResult, run_ddl_command};
use crate::{
    CombinedCatalog, RunPlanInTxnArgs, TxnState, run_plan_in_txn, try_run_cached_int32_pair_select,
    try_run_cached_scalar_aggregate_select,
};

const PG_OID_BOOL: u32 = 16;
const PG_OID_TEXT: u32 = 25;
const FORMAT_TEXT: i16 = 0;

fn mirror_int32_pair_cmp(cmp: Int32PairCmp) -> Int32PairCmp {
    match cmp {
        Int32PairCmp::Eq => Int32PairCmp::Eq,
        Int32PairCmp::Ne => Int32PairCmp::Ne,
        Int32PairCmp::Lt => Int32PairCmp::Gt,
        Int32PairCmp::Le => Int32PairCmp::Ge,
        Int32PairCmp::Gt => Int32PairCmp::Lt,
        Int32PairCmp::Ge => Int32PairCmp::Le,
    }
}

#[derive(Debug)]
pub(crate) struct CreateStatisticsSpec {
    name: String,
    table: String,
    columns: Vec<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct FastInsertInt32PairSql<'a> {
    table: &'a str,
    rows: Vec<(i32, i32)>,
}

fn skip_ascii_ws(bytes: &[u8], mut pos: usize) -> usize {
    while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    pos
}

fn consume_keyword(bytes: &[u8], pos: usize, keyword: &[u8]) -> Option<usize> {
    let end = pos.checked_add(keyword.len())?;
    let got = bytes.get(pos..end)?;
    if got.eq_ignore_ascii_case(keyword) {
        Some(end)
    } else {
        None
    }
}

fn parse_simple_identifier(sql: &str, mut pos: usize) -> Option<(&str, usize)> {
    let bytes = sql.as_bytes();
    let start = pos;
    let first = *bytes.get(pos)?;
    if !(first.is_ascii_alphabetic() || first == b'_') {
        return None;
    }
    pos += 1;
    while pos < bytes.len() {
        let b = bytes[pos];
        if b.is_ascii_alphanumeric() || b == b'_' {
            pos += 1;
        } else {
            break;
        }
    }
    Some((&sql[start..pos], pos))
}

fn parse_i32_literal(bytes: &[u8], mut pos: usize) -> Option<(i32, usize)> {
    let mut negative = false;
    if bytes.get(pos).copied() == Some(b'-') {
        negative = true;
        pos += 1;
    } else if bytes.get(pos).copied() == Some(b'+') {
        pos += 1;
    }
    let start_digits = pos;
    let mut value: i64 = 0;
    while let Some(b) = bytes.get(pos).copied() {
        if !b.is_ascii_digit() {
            break;
        }
        value = value
            .checked_mul(10)?
            .checked_add(i64::from(b.wrapping_sub(b'0')))?;
        let limit = i64::from(i32::MAX) + if negative { 1 } else { 0 };
        if value > limit {
            return None;
        }
        pos += 1;
    }
    if pos == start_digits {
        return None;
    }
    let signed = if negative { -value } else { value };
    let signed = i32::try_from(signed).ok()?;
    Some((signed, pos))
}

fn fast_insert_result(rows: u64) -> SelectResult {
    SelectResult {
        messages: vec![BackendMessage::CommandComplete {
            tag: format!("INSERT 0 {rows}"),
        }],
        streamed_body: None,
        shared_streamed_body: None,
        streaming: None,
        rows,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LogicalReplicationDdl {
    CreatePublication {
        name: String,
        tables: Vec<String>,
    },
    DropPublication {
        name: String,
        if_exists: bool,
    },
    CreateSubscription {
        name: String,
        conninfo: String,
        publications: Vec<String>,
        slot_name: Option<String>,
    },
    DropSubscription {
        name: String,
        if_exists: bool,
    },
}

trait RlsPlanOptionExt {
    fn transpose_ok(self) -> Result<Option<LogicalPlan>, ServerError>;
}

impl RlsPlanOptionExt for Option<LogicalPlan> {
    fn transpose_ok(self) -> Result<Option<LogicalPlan>, ServerError> {
        Ok(self)
    }
}

fn materialized_view_row_count_overflow() -> ServerError {
    ServerError::Execute(ultrasql_executor::ExecError::NumericFieldOverflow(
        "materialized view row count overflow".to_owned(),
    ))
}

fn checked_materialized_view_row_add(left: u64, right: u64) -> Result<u64, ServerError> {
    left.checked_add(right)
        .ok_or_else(materialized_view_row_count_overflow)
}

fn bool_literal(value: bool) -> ScalarExpr {
    ScalarExpr::Literal {
        value: Value::Bool(value),
        data_type: DataType::Bool,
    }
}

fn combine_rls_predicates(mut predicates: Vec<ScalarExpr>, op: BinaryOp) -> Option<ScalarExpr> {
    let first = predicates.pop()?;
    Some(
        predicates
            .into_iter()
            .fold(first, |left, right| ScalarExpr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
                data_type: DataType::Bool,
            }),
    )
}

fn parse_bool_guc(value: &str) -> Result<bool, ServerError> {
    match value.to_ascii_lowercase().as_str() {
        "on" | "true" | "1" | "yes" => Ok(true),
        "off" | "false" | "0" | "no" => Ok(false),
        _ => Err(ServerError::Unsupported(
            "invalid boolean runtime parameter",
        )),
    }
}

fn isolation_level_name(isolation: IsolationLevel) -> &'static str {
    match isolation {
        IsolationLevel::ReadCommitted => "read committed",
        IsolationLevel::RepeatableRead => "repeatable read",
        IsolationLevel::Serializable => "serializable",
    }
}

fn shown_transaction_isolation(txn_state: &TxnState) -> &'static str {
    match txn_state {
        TxnState::Idle => isolation_level_name(IsolationLevel::ReadCommitted),
        TxnState::InTransaction(txn) | TxnState::Failed(txn) => isolation_level_name(txn.isolation),
    }
}

fn parse_statement_timeout_ms(value: &str) -> Result<u64, ServerError> {
    let trimmed = value.trim();
    if let Some(stripped) = trimmed.strip_prefix('-') {
        if !stripped.is_empty() {
            return Err(ServerError::Unsupported("invalid statement_timeout"));
        }
    }
    trimmed
        .parse::<u64>()
        .map_err(|_| ServerError::Unsupported("invalid statement_timeout"))
}

fn normalize_datestyle(value: &str) -> Result<String, ServerError> {
    let mut style = None;
    let mut order = None;
    for part in value
        .split(|ch: char| ch == ',' || ch.is_ascii_whitespace())
        .filter(|part| !part.is_empty())
    {
        match part.to_ascii_lowercase().as_str() {
            "iso" => style = Some("ISO"),
            "postgres" => style = Some("Postgres"),
            "sql" => style = Some("SQL"),
            "german" => style = Some("German"),
            "mdy" => order = Some("MDY"),
            "dmy" => order = Some("DMY"),
            "ymd" => order = Some("YMD"),
            _ => return Err(ServerError::Unsupported("invalid datestyle")),
        }
    }
    let style = style.unwrap_or("ISO");
    let order = order.unwrap_or(if style == "German" { "DMY" } else { "MDY" });
    Ok(format!("{style}, {order}"))
}

fn starts_with_keyword_pair(sql: &str, first: &str, second: &str) -> bool {
    let mut words = sql.split_whitespace();
    words
        .next()
        .is_some_and(|word| word.eq_ignore_ascii_case(first))
        && words
            .next()
            .is_some_and(|word| word.eq_ignore_ascii_case(second))
}

fn split_first_token(input: &str) -> Result<(&str, &str), ServerError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(ServerError::ddl("logical replication DDL requires a name"));
    }
    let end = input.find(char::is_whitespace).unwrap_or(input.len());
    Ok((&input[..end], input[end..].trim()))
}

fn parse_publication_tables(input: &str) -> Result<Vec<String>, ServerError> {
    let input = input.trim();
    if !input
        .get(.."FOR TABLE".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("FOR TABLE"))
    {
        return Err(ServerError::ddl(
            "CREATE PUBLICATION currently supports FOR TABLE only",
        ));
    }
    let table_list = input["FOR TABLE".len()..].trim();
    let tables = table_list
        .split(',')
        .map(|table| table.trim().trim_matches('"').to_string())
        .filter(|table| !table.is_empty())
        .collect::<Vec<_>>();
    if tables.is_empty() {
        return Err(ServerError::ddl(
            "CREATE PUBLICATION requires at least one table",
        ));
    }
    Ok(tables)
}

fn parse_quoted_literal(input: &str) -> Result<(&str, &str), ServerError> {
    let input = input.trim_start();
    let Some(rest) = input.strip_prefix('\'') else {
        return Err(ServerError::ddl(
            "CREATE SUBSCRIPTION requires a quoted literal",
        ));
    };
    let Some(end) = rest.find('\'') else {
        return Err(ServerError::ddl("unterminated quoted literal"));
    };
    Ok((&rest[..end], rest[end + 1..].trim()))
}

fn parse_subscription_publications(input: &str) -> Result<Vec<String>, ServerError> {
    let mut rest = input.trim();
    if !rest
        .get(.."PUBLICATION".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("PUBLICATION"))
    {
        return Err(ServerError::ddl("CREATE SUBSCRIPTION requires PUBLICATION"));
    }
    rest = rest["PUBLICATION".len()..].trim();
    let publication_part = rest
        .split_once("WITH")
        .map_or(rest, |(publications, _)| publications)
        .trim();
    let publications = publication_part
        .split(',')
        .map(|publication| publication.trim().trim_matches('"').to_string())
        .filter(|publication| !publication.is_empty())
        .collect::<Vec<_>>();
    if publications.is_empty() {
        return Err(ServerError::ddl(
            "CREATE SUBSCRIPTION requires at least one publication",
        ));
    }
    Ok(publications)
}

fn parse_subscription_slot_name(input: &str) -> Result<Option<String>, ServerError> {
    let Some((_, options)) = input.split_once("WITH") else {
        return Ok(None);
    };
    let options = options.trim();
    let options = options
        .strip_prefix('(')
        .and_then(|text| text.strip_suffix(')'))
        .unwrap_or(options);
    for option in options.split(',') {
        let Some((name, value)) = option.split_once('=') else {
            continue;
        };
        if name.trim().eq_ignore_ascii_case("slot_name") {
            return Ok(Some(
                value
                    .trim()
                    .trim_matches('\'')
                    .trim_matches('"')
                    .to_string(),
            ));
        }
    }
    Ok(None)
}

fn parse_create_subscription(input: &str) -> Result<LogicalReplicationDdl, ServerError> {
    let (name, rest) = split_first_token(input)?;
    let rest = rest.trim();
    if !rest
        .get(.."CONNECTION".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("CONNECTION"))
    {
        return Err(ServerError::ddl("CREATE SUBSCRIPTION requires CONNECTION"));
    }
    let (conninfo, after_conninfo) = parse_quoted_literal(&rest["CONNECTION".len()..])?;
    let publications = parse_subscription_publications(after_conninfo)?;
    let slot_name = parse_subscription_slot_name(after_conninfo)?;
    Ok(LogicalReplicationDdl::CreateSubscription {
        name: name.to_string(),
        conninfo: conninfo.to_string(),
        publications,
        slot_name,
    })
}

mod bound_plan;
mod describe;
mod dml_txn;
mod effects;
mod maintenance;
mod mvcc_maint;
mod query;
mod row_security;
mod views;

struct ServerStatsSource<'a> {
    stats_catalog: &'a parking_lot::RwLock<InMemoryStatsCatalog>,
}

impl StatsSource for ServerStatsSource<'_> {
    fn row_count(&self, table: &str) -> u64 {
        self.stats_catalog
            .read()
            .lookup_relation(table)
            .map_or(0, |s| s.row_count)
    }

    fn page_count(&self, table: &str) -> u64 {
        self.stats_catalog
            .read()
            .lookup_relation(table)
            .map_or(0, |s| s.page_count)
    }

    fn null_frac(&self, table: &str, column: usize) -> f64 {
        self.stats_catalog
            .read()
            .lookup_relation(table)
            .and_then(|s| s.columns.get(column).map(|c| c.null_frac))
            .unwrap_or(0.0)
    }

    fn n_distinct(&self, table: &str, column: usize) -> f64 {
        self.stats_catalog
            .read()
            .lookup_relation(table)
            .and_then(|s| s.columns.get(column).map(|c| c.n_distinct))
            .unwrap_or(0.0)
    }
}

#[cfg(test)]
mod tests;
