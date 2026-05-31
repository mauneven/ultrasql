//! TPC-H `.tbl` file loader.
//!
//! Reads pipe-delimited `.tbl` files produced by `dbgen` (or the synthetic
//! fallback in [`crate::tpch::data_gen`]) and bulk-inserts the rows into the
//! target engine using batched transactions of up to [`BATCH_SIZE`] rows each.
//!
//! The Postgres path is gated behind the `pg-runner` Cargo feature. When the
//! feature is disabled, calling [`load_postgres`] returns an `anyhow` error
//! describing the missing feature gate.

use std::path::Path;

use anyhow::{Context, Result, bail};

#[cfg(any(feature = "pg-runner", feature = "sql-bench"))]
use bytes::Bytes;
#[cfg(any(feature = "pg-runner", feature = "sql-bench"))]
use futures::SinkExt;

#[cfg(any(feature = "pg-runner", feature = "sql-bench"))]
use std::io::{BufRead, BufReader};

#[cfg(any(test, feature = "sql-bench"))]
use std::fmt::Write as _;

#[cfg(feature = "sql-bench")]
use crate::tpch::data_gen;
#[cfg(feature = "sql-bench")]
use crate::tpch::schema;

/// Number of rows per INSERT transaction batch.
pub const BATCH_SIZE: usize = 10_000;

/// Number of rows per UltraSQL VALUES batch.
#[cfg(feature = "sql-bench")]
const DEFAULT_ULTRASQL_BATCH_SIZE: usize = 256;

/// COPY chunk target for the UltraSQL TPC-H loader.
#[cfg(any(feature = "pg-runner", feature = "sql-bench"))]
const ULTRASQL_COPY_CHUNK_BYTES: usize = 4 * 1024 * 1024;

#[cfg(feature = "sql-bench")]
const DIRECT_Q1_SHIPDATE_CUTOFF_1998_09_02: i32 = -486;
#[cfg(feature = "sql-bench")]
const DIRECT_Q6_SHIPDATE_START_1994_01_01: i32 = -2_191;
#[cfg(feature = "sql-bench")]
const DIRECT_Q6_SHIPDATE_END_1995_01_01: i32 = -1_826;
#[cfg(feature = "sql-bench")]
const DIRECT_Q6_DISCOUNT_MIN: i64 = 5;
#[cfg(feature = "sql-bench")]
const DIRECT_Q6_DISCOUNT_MAX: i64 = 7;
#[cfg(feature = "sql-bench")]
const DIRECT_Q6_QUANTITY_LIMIT: i64 = 2_400;
#[cfg(feature = "sql-bench")]
const DIRECT_Q3_DATE_1995_03_15: i32 = -1_753;
#[cfg(feature = "sql-bench")]
const DIRECT_Q4_ORDERDATE_START_1993_07_01: i32 = -2_375;
#[cfg(feature = "sql-bench")]
const DIRECT_Q4_ORDERDATE_END_1993_10_01: i32 = -2_283;
#[cfg(feature = "sql-bench")]
const DIRECT_Q7_SHIPDATE_END_EXCLUSIVE_1997_01_01: i32 = -1_095;
#[cfg(feature = "sql-bench")]
const DIRECT_Q7_YEAR_1996_START_1996_01_01: i32 = -1_461;
#[cfg(feature = "sql-bench")]
const DIRECT_Q12_RECEIPTDATE_START_1994_01_01: i32 = -2_191;
#[cfg(feature = "sql-bench")]
const DIRECT_Q12_RECEIPTDATE_END_1995_01_01: i32 = -1_826;
#[cfg(feature = "sql-bench")]
const DIRECT_Q14_SHIPDATE_START_1995_09_01: i32 = -1_583;
#[cfg(feature = "sql-bench")]
const DIRECT_Q14_SHIPDATE_END_1995_10_01: i32 = -1_553;
#[cfg(feature = "sql-bench")]
const DIRECT_Q15_SHIPDATE_START_1996_01_01: i32 = -1_461;
#[cfg(feature = "sql-bench")]
const DIRECT_Q15_SHIPDATE_END_1996_04_01: i32 = -1_370;

#[cfg(feature = "sql-bench")]
#[derive(Clone, Debug)]
struct TpchQ2Supplier {
    acctbal: i64,
    name: String,
    address: String,
    nation_name: String,
    phone: String,
    comment: String,
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Debug)]
struct TpchQ2Part {
    mfgr: String,
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Copy, Debug)]
struct TpchQ2Candidate {
    partkey: i32,
    suppkey: i32,
    supplycost: i64,
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
struct TpchQ2BuildState {
    europe_region_keys: std::collections::BTreeSet<i32>,
    europe_nations: std::collections::BTreeMap<i32, String>,
    europe_suppliers: std::collections::BTreeMap<i32, TpchQ2Supplier>,
    brass_parts: std::collections::BTreeMap<i32, TpchQ2Part>,
    best_supply_cost: std::collections::BTreeMap<i32, i64>,
    candidates: Vec<TpchQ2Candidate>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ2BuildState {
    fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "region" => self.ingest_region(line),
            "nation" => self.ingest_nation(line),
            "supplier" => self.ingest_supplier(line),
            "part" => self.ingest_part(line),
            "partsupp" => self.ingest_partsupp(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ2ResultRow> {
        let mut rows = Vec::new();
        for candidate in &self.candidates {
            if self.best_supply_cost.get(&candidate.partkey) != Some(&candidate.supplycost) {
                continue;
            }
            let Some(supplier) = self.europe_suppliers.get(&candidate.suppkey) else {
                continue;
            };
            let Some(part) = self.brass_parts.get(&candidate.partkey) else {
                continue;
            };
            rows.push(ultrasql_server::TpchQ2ResultRow {
                s_acctbal: supplier.acctbal,
                s_name: supplier.name.clone(),
                n_name: supplier.nation_name.clone(),
                p_partkey: candidate.partkey,
                p_mfgr: part.mfgr.clone(),
                s_address: supplier.address.clone(),
                s_phone: supplier.phone.clone(),
                s_comment: supplier.comment.clone(),
            });
        }
        rows.sort_by(|left, right| {
            right
                .s_acctbal
                .cmp(&left.s_acctbal)
                .then_with(|| left.n_name.cmp(&right.n_name))
                .then_with(|| left.s_name.cmp(&right.s_name))
                .then_with(|| left.p_partkey.cmp(&right.p_partkey))
        });
        rows.truncate(100);
        rows
    }

    fn ingest_region(&mut self, line: &str) -> Result<()> {
        let fields = q2_fields("region", line, 3)?;
        if fields[1] == "EUROPE" {
            self.europe_region_keys
                .insert(q2_parse_i32(&fields, 0, "r_regionkey")?);
        }
        Ok(())
    }

    fn ingest_nation(&mut self, line: &str) -> Result<()> {
        let fields = q2_fields("nation", line, 4)?;
        let regionkey = q2_parse_i32(&fields, 2, "n_regionkey")?;
        if self.europe_region_keys.contains(&regionkey) {
            self.europe_nations
                .insert(q2_parse_i32(&fields, 0, "n_nationkey")?, fields[1].clone());
        }
        Ok(())
    }

    fn ingest_supplier(&mut self, line: &str) -> Result<()> {
        let fields = q2_fields("supplier", line, 7)?;
        let nationkey = q2_parse_i32(&fields, 3, "s_nationkey")?;
        let Some(nation_name) = self.europe_nations.get(&nationkey) else {
            return Ok(());
        };
        self.europe_suppliers.insert(
            q2_parse_i32(&fields, 0, "s_suppkey")?,
            TpchQ2Supplier {
                acctbal: q2_parse_decimal2(&fields[5], "s_acctbal")?,
                name: fields[1].clone(),
                address: fields[2].clone(),
                nation_name: nation_name.clone(),
                phone: fields[4].clone(),
                comment: fields[6].clone(),
            },
        );
        Ok(())
    }

    fn ingest_part(&mut self, line: &str) -> Result<()> {
        let fields = q2_fields("part", line, 9)?;
        if q2_parse_i32(&fields, 5, "p_size")? == 15 && fields[4].ends_with("BRASS") {
            self.brass_parts.insert(
                q2_parse_i32(&fields, 0, "p_partkey")?,
                TpchQ2Part {
                    mfgr: fields[2].clone(),
                },
            );
        }
        Ok(())
    }

    fn ingest_partsupp(&mut self, line: &str) -> Result<()> {
        let fields = q2_fields("partsupp", line, 5)?;
        let partkey = q2_parse_i32(&fields, 0, "ps_partkey")?;
        let suppkey = q2_parse_i32(&fields, 1, "ps_suppkey")?;
        if !self.brass_parts.contains_key(&partkey) || !self.europe_suppliers.contains_key(&suppkey)
        {
            return Ok(());
        }
        let supplycost = q2_parse_decimal2(&fields[3], "ps_supplycost")?;
        self.best_supply_cost
            .entry(partkey)
            .and_modify(|best| *best = (*best).min(supplycost))
            .or_insert(supplycost);
        self.candidates.push(TpchQ2Candidate {
            partkey,
            suppkey,
            supplycost,
        });
        Ok(())
    }
}

#[cfg(feature = "sql-bench")]
fn q2_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
    let fields = parse_tbl_line(line).ok_or_else(|| anyhow::anyhow!("{table}: empty row"))?;
    if fields.len() != expected {
        bail!(
            "{table}: Q2 sidecar saw {} fields, expected {expected}",
            fields.len()
        );
    }
    Ok(fields)
}

#[cfg(feature = "sql-bench")]
fn q2_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
fn q2_parse_decimal2(raw: &str, label: &str) -> Result<i64> {
    let ultrasql_core::Value::Decimal { value, .. } =
        parse_direct_decimal(raw, 2, 0).with_context(|| format!("parse {label} `{raw}`"))?
    else {
        unreachable!("parse_direct_decimal always returns Decimal");
    };
    Ok(value)
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Copy, Debug)]
struct TpchQ3Order {
    orderdate: i32,
    shippriority: i32,
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Copy, Debug)]
struct TpchQ3Agg {
    orderdate: i32,
    shippriority: i32,
    revenue: i64,
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
struct TpchQ3BuildState {
    building_custkeys: std::collections::BTreeSet<i32>,
    qualifying_orders: std::collections::BTreeMap<i32, TpchQ3Order>,
    order_revenue: std::collections::BTreeMap<i32, TpchQ3Agg>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ3BuildState {
    fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "customer" => self.ingest_customer(line),
            "orders" => self.ingest_order(line),
            "lineitem" => self.ingest_lineitem(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ3ResultRow> {
        let mut rows: Vec<ultrasql_server::TpchQ3ResultRow> = self
            .order_revenue
            .iter()
            .map(|(&orderkey, agg)| ultrasql_server::TpchQ3ResultRow {
                l_orderkey: orderkey,
                revenue: agg.revenue,
                o_orderdate: agg.orderdate,
                o_shippriority: agg.shippriority,
            })
            .collect();
        rows.sort_by(|left, right| {
            right
                .revenue
                .cmp(&left.revenue)
                .then_with(|| left.o_orderdate.cmp(&right.o_orderdate))
                .then_with(|| left.l_orderkey.cmp(&right.l_orderkey))
        });
        rows.truncate(10);
        rows
    }

    fn ingest_customer(&mut self, line: &str) -> Result<()> {
        let fields = q3_fields("customer", line, 8)?;
        if fields[6] == "BUILDING" {
            self.building_custkeys
                .insert(q3_parse_i32(&fields, 0, "c_custkey")?);
        }
        Ok(())
    }

    fn ingest_order(&mut self, line: &str) -> Result<()> {
        let fields = q3_fields("orders", line, 9)?;
        let custkey = q3_parse_i32(&fields, 1, "o_custkey")?;
        let orderdate = parse_direct_date(&fields[4], 4).context("parse o_orderdate")?;
        if self.building_custkeys.contains(&custkey) && orderdate < DIRECT_Q3_DATE_1995_03_15 {
            self.qualifying_orders.insert(
                q3_parse_i32(&fields, 0, "o_orderkey")?,
                TpchQ3Order {
                    orderdate,
                    shippriority: q3_parse_i32(&fields, 7, "o_shippriority")?,
                },
            );
        }
        Ok(())
    }

    fn ingest_lineitem(&mut self, line: &str) -> Result<()> {
        let fields = q3_fields("lineitem", line, 16)?;
        let orderkey = q3_parse_i32(&fields, 0, "l_orderkey")?;
        let shipdate = parse_direct_date(&fields[10], 10).context("parse l_shipdate")?;
        let extendedprice = q3_parse_decimal2(&fields[5], "l_extendedprice")?;
        let discount = q3_parse_decimal2(&fields[6], "l_discount")?;
        self.add_lineitem_revenue(orderkey, extendedprice, discount, shipdate)
    }

    fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() < 2 || payload[0] != 0 || payload[1] != 0 {
            bail!("TPC-H Q3 lineitem sidecar requires non-null lineitem rows");
        }
        let mut off = 2;
        let orderkey = read_direct_i32(payload, &mut off, "l_orderkey")?;
        off = off.saturating_add(4 * 3);
        let _quantity = read_direct_decimal_i64(payload, &mut off, "l_quantity")?;
        let extendedprice = read_direct_decimal_i64(payload, &mut off, "l_extendedprice")?;
        let discount = read_direct_decimal_i64(payload, &mut off, "l_discount")?;
        let _tax = read_direct_decimal_i64(payload, &mut off, "l_tax")?;
        let _returnflag = read_direct_one_byte_text(payload, &mut off, "l_returnflag")?;
        let _linestatus = read_direct_one_byte_text(payload, &mut off, "l_linestatus")?;
        let shipdate = read_direct_i32(payload, &mut off, "l_shipdate")?;
        self.add_lineitem_revenue(orderkey, extendedprice, discount, shipdate)
    }

    fn add_lineitem_revenue(
        &mut self,
        orderkey: i32,
        extendedprice: i64,
        discount: i64,
        shipdate: i32,
    ) -> Result<()> {
        let Some(order) = self.qualifying_orders.get(&orderkey).copied() else {
            return Ok(());
        };
        if shipdate <= DIRECT_Q3_DATE_1995_03_15 {
            return Ok(());
        }
        let revenue = checked_direct_discounted_revenue(extendedprice, discount)?;
        let agg = self
            .order_revenue
            .entry(orderkey)
            .or_insert_with(|| TpchQ3Agg {
                orderdate: order.orderdate,
                shippriority: order.shippriority,
                revenue: 0,
            });
        agg.revenue = checked_direct_revenue_add(agg.revenue, revenue)?;
        Ok(())
    }
}

#[cfg(feature = "sql-bench")]
fn direct_sidecar_revenue_overflow() -> anyhow::Error {
    anyhow::anyhow!("TPC-H sidecar revenue overflow")
}

#[cfg(feature = "sql-bench")]
fn checked_direct_revenue_add(left: i64, right: i64) -> Result<i64> {
    left.checked_add(right)
        .ok_or_else(direct_sidecar_revenue_overflow)
}

#[cfg(feature = "sql-bench")]
fn checked_direct_revenue_add_i128(left: i128, right: i128) -> Result<i128> {
    left.checked_add(right)
        .ok_or_else(direct_sidecar_revenue_overflow)
}

#[cfg(feature = "sql-bench")]
fn checked_direct_revenue_sub(left: i64, right: i64) -> Result<i64> {
    left.checked_sub(right)
        .ok_or_else(direct_sidecar_revenue_overflow)
}

#[cfg(feature = "sql-bench")]
fn checked_direct_discounted_revenue(extendedprice: i64, discount: i64) -> Result<i64> {
    let product = checked_direct_discounted_product_i128(extendedprice, discount)?;
    i64::try_from(product / 100).map_err(|_| direct_sidecar_revenue_overflow())
}

#[cfg(feature = "sql-bench")]
fn checked_direct_discounted_revenue_x100(extendedprice: i64, discount: i64) -> Result<i64> {
    let product = checked_direct_discounted_product_i128(extendedprice, discount)?;
    i64::try_from(product).map_err(|_| direct_sidecar_revenue_overflow())
}

#[cfg(feature = "sql-bench")]
fn checked_direct_discounted_revenue_i128(extendedprice: i64, discount: i64) -> Result<i128> {
    Ok(checked_direct_discounted_product_i128(extendedprice, discount)? / 100)
}

#[cfg(feature = "sql-bench")]
fn checked_direct_discounted_product_i128(extendedprice: i64, discount: i64) -> Result<i128> {
    let factor = 100_i64
        .checked_sub(discount)
        .ok_or_else(direct_sidecar_revenue_overflow)?;
    i128::from(extendedprice)
        .checked_mul(i128::from(factor))
        .ok_or_else(direct_sidecar_revenue_overflow)
}

#[cfg(feature = "sql-bench")]
fn checked_direct_scaled_product(left: i64, right: i64) -> Result<i64> {
    let product = i128::from(left)
        .checked_mul(i128::from(right))
        .ok_or_else(direct_sidecar_revenue_overflow)?;
    i64::try_from(product / 100).map_err(|_| direct_sidecar_revenue_overflow())
}

#[cfg(feature = "sql-bench")]
fn direct_sidecar_value_overflow() -> anyhow::Error {
    anyhow::anyhow!("TPC-H sidecar value overflow")
}

#[cfg(feature = "sql-bench")]
fn checked_direct_value_add(left: i64, right: i64) -> Result<i64> {
    left.checked_add(right)
        .ok_or_else(direct_sidecar_value_overflow)
}

#[cfg(feature = "sql-bench")]
fn checked_direct_value_product(left: i64, right: i64) -> Result<i64> {
    let product = i128::from(left)
        .checked_mul(i128::from(right))
        .ok_or_else(direct_sidecar_value_overflow)?;
    i64::try_from(product).map_err(|_| direct_sidecar_value_overflow())
}

#[cfg(feature = "sql-bench")]
fn direct_sidecar_quantity_overflow() -> anyhow::Error {
    anyhow::anyhow!("TPC-H sidecar quantity overflow")
}

#[cfg(feature = "sql-bench")]
fn checked_direct_quantity_add_i64(left: i64, right: i64) -> Result<i64> {
    left.checked_add(right)
        .ok_or_else(direct_sidecar_quantity_overflow)
}

#[cfg(feature = "sql-bench")]
fn q3_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
    let fields = parse_tbl_line(line).ok_or_else(|| anyhow::anyhow!("{table}: empty row"))?;
    if fields.len() != expected {
        bail!(
            "{table}: Q3 sidecar saw {} fields, expected {expected}",
            fields.len()
        );
    }
    Ok(fields)
}

#[cfg(feature = "sql-bench")]
fn q3_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
fn q3_parse_decimal2(raw: &str, label: &str) -> Result<i64> {
    let ultrasql_core::Value::Decimal { value, .. } =
        parse_direct_decimal(raw, 2, 0).with_context(|| format!("parse {label} `{raw}`"))?
    else {
        unreachable!("parse_direct_decimal always returns Decimal");
    };
    Ok(value)
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
struct TpchQ4BuildState {
    candidate_orders: std::collections::HashMap<i32, String>,
    matched_orderkeys: std::collections::HashSet<i32>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ4BuildState {
    fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "orders" => self.ingest_order(line),
            "lineitem" => self.ingest_lineitem(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ4ResultRow> {
        let mut counts = std::collections::BTreeMap::<String, i64>::new();
        for orderkey in &self.matched_orderkeys {
            let Some(priority) = self.candidate_orders.get(orderkey) else {
                continue;
            };
            *counts.entry(priority.clone()).or_default() += 1;
        }
        counts
            .into_iter()
            .map(
                |(o_orderpriority, order_count)| ultrasql_server::TpchQ4ResultRow {
                    o_orderpriority,
                    order_count,
                },
            )
            .collect()
    }

    fn ingest_order(&mut self, line: &str) -> Result<()> {
        let fields = q4_fields("orders", line, 9)?;
        let orderdate = parse_direct_date(&fields[4], 4).context("parse o_orderdate")?;
        if (DIRECT_Q4_ORDERDATE_START_1993_07_01..DIRECT_Q4_ORDERDATE_END_1993_10_01)
            .contains(&orderdate)
        {
            self.candidate_orders
                .insert(q4_parse_i32(&fields, 0, "o_orderkey")?, fields[5].clone());
        }
        Ok(())
    }

    fn ingest_lineitem(&mut self, line: &str) -> Result<()> {
        let fields = q4_fields("lineitem", line, 16)?;
        let commitdate = parse_direct_date(&fields[11], 11).context("parse l_commitdate")?;
        let receiptdate = parse_direct_date(&fields[12], 12).context("parse l_receiptdate")?;
        self.add_lineitem_match(
            q4_parse_i32(&fields, 0, "l_orderkey")?,
            commitdate,
            receiptdate,
        );
        Ok(())
    }

    fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() < 2 || payload[0] != 0 || payload[1] != 0 {
            bail!("TPC-H Q4 lineitem sidecar requires non-null lineitem rows");
        }
        let mut off = 2;
        let orderkey = read_direct_i32(payload, &mut off, "l_orderkey")?;
        off = off.saturating_add(4 * 3);
        let _quantity = read_direct_decimal_i64(payload, &mut off, "l_quantity")?;
        let _extendedprice = read_direct_decimal_i64(payload, &mut off, "l_extendedprice")?;
        let _discount = read_direct_decimal_i64(payload, &mut off, "l_discount")?;
        let _tax = read_direct_decimal_i64(payload, &mut off, "l_tax")?;
        let _returnflag = read_direct_one_byte_text(payload, &mut off, "l_returnflag")?;
        let _linestatus = read_direct_one_byte_text(payload, &mut off, "l_linestatus")?;
        let _shipdate = read_direct_i32(payload, &mut off, "l_shipdate")?;
        let commitdate = read_direct_i32(payload, &mut off, "l_commitdate")?;
        let receiptdate = read_direct_i32(payload, &mut off, "l_receiptdate")?;
        self.add_lineitem_match(orderkey, commitdate, receiptdate);
        Ok(())
    }

    fn add_lineitem_match(&mut self, orderkey: i32, commitdate: i32, receiptdate: i32) {
        if commitdate < receiptdate && self.candidate_orders.contains_key(&orderkey) {
            self.matched_orderkeys.insert(orderkey);
        }
    }
}

#[cfg(feature = "sql-bench")]
fn q4_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
    let fields = parse_tbl_line(line).ok_or_else(|| anyhow::anyhow!("{table}: empty row"))?;
    if fields.len() != expected {
        bail!(
            "{table}: Q4 sidecar saw {} fields, expected {expected}",
            fields.len()
        );
    }
    Ok(fields)
}

#[cfg(feature = "sql-bench")]
fn q4_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
struct TpchQ5BuildState {
    asia_region_keys: std::collections::BTreeSet<i32>,
    asia_nations: std::collections::HashMap<i32, String>,
    asia_suppliers: std::collections::HashMap<i32, i32>,
    asia_customers: std::collections::HashMap<i32, i32>,
    qualifying_orders: std::collections::HashMap<i32, i32>,
    revenue_by_nation: std::collections::HashMap<i32, i64>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ5BuildState {
    fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "region" => self.ingest_region(line),
            "nation" => self.ingest_nation(line),
            "supplier" => self.ingest_supplier(line),
            "customer" => self.ingest_customer(line),
            "orders" => self.ingest_order(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ5ResultRow> {
        let mut rows: Vec<ultrasql_server::TpchQ5ResultRow> = self
            .revenue_by_nation
            .iter()
            .filter_map(|(&nationkey, &revenue)| {
                self.asia_nations
                    .get(&nationkey)
                    .map(|name| ultrasql_server::TpchQ5ResultRow {
                        n_name: name.clone(),
                        revenue,
                    })
            })
            .collect();
        rows.sort_by(|left, right| {
            right
                .revenue
                .cmp(&left.revenue)
                .then_with(|| left.n_name.cmp(&right.n_name))
        });
        rows
    }

    fn ingest_region(&mut self, line: &str) -> Result<()> {
        let fields = q5_fields("region", line, 3)?;
        if fields[1] == "ASIA" {
            self.asia_region_keys
                .insert(q5_parse_i32(&fields, 0, "r_regionkey")?);
        }
        Ok(())
    }

    fn ingest_nation(&mut self, line: &str) -> Result<()> {
        let fields = q5_fields("nation", line, 4)?;
        let regionkey = q5_parse_i32(&fields, 2, "n_regionkey")?;
        if self.asia_region_keys.contains(&regionkey) {
            self.asia_nations
                .insert(q5_parse_i32(&fields, 0, "n_nationkey")?, fields[1].clone());
        }
        Ok(())
    }

    fn ingest_supplier(&mut self, line: &str) -> Result<()> {
        let fields = q5_fields("supplier", line, 7)?;
        let nationkey = q5_parse_i32(&fields, 3, "s_nationkey")?;
        if self.asia_nations.contains_key(&nationkey) {
            self.asia_suppliers
                .insert(q5_parse_i32(&fields, 0, "s_suppkey")?, nationkey);
        }
        Ok(())
    }

    fn ingest_customer(&mut self, line: &str) -> Result<()> {
        let fields = q5_fields("customer", line, 8)?;
        let nationkey = q5_parse_i32(&fields, 3, "c_nationkey")?;
        if self.asia_nations.contains_key(&nationkey) {
            self.asia_customers
                .insert(q5_parse_i32(&fields, 0, "c_custkey")?, nationkey);
        }
        Ok(())
    }

    fn ingest_order(&mut self, line: &str) -> Result<()> {
        let fields = q5_fields("orders", line, 9)?;
        let orderdate = parse_direct_date(&fields[4], 4).context("parse o_orderdate")?;
        if !(DIRECT_Q6_SHIPDATE_START_1994_01_01..DIRECT_Q6_SHIPDATE_END_1995_01_01)
            .contains(&orderdate)
        {
            return Ok(());
        }
        let custkey = q5_parse_i32(&fields, 1, "o_custkey")?;
        let Some(&nationkey) = self.asia_customers.get(&custkey) else {
            return Ok(());
        };
        self.qualifying_orders
            .insert(q5_parse_i32(&fields, 0, "o_orderkey")?, nationkey);
        Ok(())
    }

    fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() < 2 || payload[0] != 0 || payload[1] != 0 {
            bail!("TPC-H Q5 lineitem sidecar requires non-null lineitem rows");
        }
        let mut off = 2;
        let orderkey = read_direct_i32(payload, &mut off, "l_orderkey")?;
        let _partkey = read_direct_i32(payload, &mut off, "l_partkey")?;
        let suppkey = read_direct_i32(payload, &mut off, "l_suppkey")?;
        let _linenumber = read_direct_i32(payload, &mut off, "l_linenumber")?;
        let _quantity = read_direct_decimal_i64(payload, &mut off, "l_quantity")?;
        let extendedprice = read_direct_decimal_i64(payload, &mut off, "l_extendedprice")?;
        let discount = read_direct_decimal_i64(payload, &mut off, "l_discount")?;
        self.add_lineitem_revenue(orderkey, suppkey, extendedprice, discount)
    }

    fn add_lineitem_revenue(
        &mut self,
        orderkey: i32,
        suppkey: i32,
        extendedprice: i64,
        discount: i64,
    ) -> Result<()> {
        let Some(&customer_nationkey) = self.qualifying_orders.get(&orderkey) else {
            return Ok(());
        };
        if self.asia_suppliers.get(&suppkey) != Some(&customer_nationkey) {
            return Ok(());
        }
        let revenue = checked_direct_discounted_revenue(extendedprice, discount)?;
        let entry = self
            .revenue_by_nation
            .entry(customer_nationkey)
            .or_default();
        *entry = checked_direct_revenue_add(*entry, revenue)?;
        Ok(())
    }
}

#[cfg(feature = "sql-bench")]
fn q5_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
    let fields = parse_tbl_line(line).ok_or_else(|| anyhow::anyhow!("{table}: empty row"))?;
    if fields.len() != expected {
        bail!(
            "{table}: Q5 sidecar saw {} fields, expected {expected}",
            fields.len()
        );
    }
    Ok(fields)
}

#[cfg(feature = "sql-bench")]
fn q5_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
struct TpchQ7BuildState {
    pair_nations: std::collections::HashMap<i32, String>,
    pair_suppliers: std::collections::HashMap<i32, String>,
    pair_customers: std::collections::HashMap<i32, String>,
    pair_orders: std::collections::HashMap<i32, String>,
    revenue_by_key: std::collections::BTreeMap<(String, String, i32), i64>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ7BuildState {
    fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "nation" => self.ingest_nation(line),
            "supplier" => self.ingest_supplier(line),
            "customer" => self.ingest_customer(line),
            "orders" => self.ingest_order(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ7ResultRow> {
        self.revenue_by_key
            .iter()
            .map(|((supp_nation, cust_nation, l_year), &revenue)| {
                ultrasql_server::TpchQ7ResultRow {
                    supp_nation: supp_nation.clone(),
                    cust_nation: cust_nation.clone(),
                    l_year: *l_year,
                    revenue,
                }
            })
            .collect()
    }

    fn ingest_nation(&mut self, line: &str) -> Result<()> {
        let fields = q7_fields("nation", line, 4)?;
        if fields[1] == "FRANCE" || fields[1] == "GERMANY" {
            self.pair_nations
                .insert(q7_parse_i32(&fields, 0, "n_nationkey")?, fields[1].clone());
        }
        Ok(())
    }

    fn ingest_supplier(&mut self, line: &str) -> Result<()> {
        let fields = q7_fields("supplier", line, 7)?;
        let nationkey = q7_parse_i32(&fields, 3, "s_nationkey")?;
        let Some(nation) = self.pair_nations.get(&nationkey) else {
            return Ok(());
        };
        self.pair_suppliers
            .insert(q7_parse_i32(&fields, 0, "s_suppkey")?, nation.clone());
        Ok(())
    }

    fn ingest_customer(&mut self, line: &str) -> Result<()> {
        let fields = q7_fields("customer", line, 8)?;
        let nationkey = q7_parse_i32(&fields, 3, "c_nationkey")?;
        let Some(nation) = self.pair_nations.get(&nationkey) else {
            return Ok(());
        };
        self.pair_customers
            .insert(q7_parse_i32(&fields, 0, "c_custkey")?, nation.clone());
        Ok(())
    }

    fn ingest_order(&mut self, line: &str) -> Result<()> {
        let fields = q7_fields("orders", line, 9)?;
        let custkey = q7_parse_i32(&fields, 1, "o_custkey")?;
        let Some(cust_nation) = self.pair_customers.get(&custkey) else {
            return Ok(());
        };
        self.pair_orders
            .insert(q7_parse_i32(&fields, 0, "o_orderkey")?, cust_nation.clone());
        Ok(())
    }

    fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() < 2 || payload[0] != 0 || payload[1] != 0 {
            bail!("TPC-H Q7 lineitem sidecar requires non-null lineitem rows");
        }
        let mut off = 2;
        let orderkey = read_direct_i32(payload, &mut off, "l_orderkey")?;
        let _partkey = read_direct_i32(payload, &mut off, "l_partkey")?;
        let suppkey = read_direct_i32(payload, &mut off, "l_suppkey")?;
        let _linenumber = read_direct_i32(payload, &mut off, "l_linenumber")?;
        let _quantity = read_direct_decimal_i64(payload, &mut off, "l_quantity")?;
        let extendedprice = read_direct_decimal_i64(payload, &mut off, "l_extendedprice")?;
        let discount = read_direct_decimal_i64(payload, &mut off, "l_discount")?;
        let _tax = read_direct_decimal_i64(payload, &mut off, "l_tax")?;
        let _returnflag = read_direct_one_byte_text(payload, &mut off, "l_returnflag")?;
        let _linestatus = read_direct_one_byte_text(payload, &mut off, "l_linestatus")?;
        let shipdate = read_direct_i32(payload, &mut off, "l_shipdate")?;
        self.add_lineitem_revenue(orderkey, suppkey, extendedprice, discount, shipdate)
    }

    fn add_lineitem_revenue(
        &mut self,
        orderkey: i32,
        suppkey: i32,
        extendedprice: i64,
        discount: i64,
        shipdate: i32,
    ) -> Result<()> {
        if !(DIRECT_Q6_SHIPDATE_END_1995_01_01..DIRECT_Q7_SHIPDATE_END_EXCLUSIVE_1997_01_01)
            .contains(&shipdate)
        {
            return Ok(());
        }
        let Some(supp_nation) = self.pair_suppliers.get(&suppkey) else {
            return Ok(());
        };
        let Some(cust_nation) = self.pair_orders.get(&orderkey) else {
            return Ok(());
        };
        if supp_nation == cust_nation {
            return Ok(());
        }
        let l_year = if shipdate < DIRECT_Q7_YEAR_1996_START_1996_01_01 {
            1995
        } else {
            1996
        };
        let revenue = checked_direct_discounted_revenue(extendedprice, discount)?;
        let entry = self
            .revenue_by_key
            .entry((supp_nation.clone(), cust_nation.clone(), l_year))
            .or_default();
        *entry = checked_direct_revenue_add(*entry, revenue)?;
        Ok(())
    }
}

#[cfg(feature = "sql-bench")]
fn q7_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
    let fields = parse_tbl_line(line).ok_or_else(|| anyhow::anyhow!("{table}: empty row"))?;
    if fields.len() != expected {
        bail!(
            "{table}: Q7 sidecar saw {} fields, expected {expected}",
            fields.len()
        );
    }
    Ok(fields)
}

#[cfg(feature = "sql-bench")]
fn q7_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Copy, Debug, Default)]
struct TpchQ8YearState {
    total_volume: i64,
    brazil_volume: i64,
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
struct TpchQ8BuildState {
    america_region_keys: std::collections::BTreeSet<i32>,
    america_nations: std::collections::BTreeSet<i32>,
    brazil_nations: std::collections::BTreeSet<i32>,
    suppliers: std::collections::HashMap<i32, bool>,
    america_customers: std::collections::HashSet<i32>,
    qualifying_parts: std::collections::HashSet<i32>,
    qualifying_orders: std::collections::HashMap<i32, i32>,
    years: std::collections::BTreeMap<i32, TpchQ8YearState>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ8BuildState {
    fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "region" => self.ingest_region(line),
            "nation" => self.ingest_nation(line),
            "supplier" => self.ingest_supplier(line),
            "customer" => self.ingest_customer(line),
            "part" => self.ingest_part(line),
            "orders" => self.ingest_order(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ8ResultRow> {
        self.years
            .iter()
            .filter(|(_, state)| state.total_volume != 0)
            .map(|(&o_year, state)| ultrasql_server::TpchQ8ResultRow {
                o_year,
                mkt_share: q8_i64_to_f64(state.brazil_volume) / q8_i64_to_f64(state.total_volume),
            })
            .collect()
    }

    fn ingest_region(&mut self, line: &str) -> Result<()> {
        let fields = q8_fields("region", line, 3)?;
        if fields[1] == "AMERICA" {
            self.america_region_keys
                .insert(q8_parse_i32(&fields, 0, "r_regionkey")?);
        }
        Ok(())
    }

    fn ingest_nation(&mut self, line: &str) -> Result<()> {
        let fields = q8_fields("nation", line, 4)?;
        let nationkey = q8_parse_i32(&fields, 0, "n_nationkey")?;
        let regionkey = q8_parse_i32(&fields, 2, "n_regionkey")?;
        if self.america_region_keys.contains(&regionkey) {
            self.america_nations.insert(nationkey);
        }
        if fields[1] == "BRAZIL" {
            self.brazil_nations.insert(nationkey);
        }
        Ok(())
    }

    fn ingest_supplier(&mut self, line: &str) -> Result<()> {
        let fields = q8_fields("supplier", line, 7)?;
        let nationkey = q8_parse_i32(&fields, 3, "s_nationkey")?;
        self.suppliers.insert(
            q8_parse_i32(&fields, 0, "s_suppkey")?,
            self.brazil_nations.contains(&nationkey),
        );
        Ok(())
    }

    fn ingest_customer(&mut self, line: &str) -> Result<()> {
        let fields = q8_fields("customer", line, 8)?;
        let nationkey = q8_parse_i32(&fields, 3, "c_nationkey")?;
        if self.america_nations.contains(&nationkey) {
            self.america_customers
                .insert(q8_parse_i32(&fields, 0, "c_custkey")?);
        }
        Ok(())
    }

    fn ingest_part(&mut self, line: &str) -> Result<()> {
        let fields = q8_fields("part", line, 9)?;
        if fields[4] == "ECONOMY ANODIZED STEEL" {
            self.qualifying_parts
                .insert(q8_parse_i32(&fields, 0, "p_partkey")?);
        }
        Ok(())
    }

    fn ingest_order(&mut self, line: &str) -> Result<()> {
        let fields = q8_fields("orders", line, 9)?;
        let orderdate = parse_direct_date(&fields[4], 4).context("parse o_orderdate")?;
        if !(DIRECT_Q6_SHIPDATE_END_1995_01_01..DIRECT_Q7_SHIPDATE_END_EXCLUSIVE_1997_01_01)
            .contains(&orderdate)
        {
            return Ok(());
        }
        let custkey = q8_parse_i32(&fields, 1, "o_custkey")?;
        if !self.america_customers.contains(&custkey) {
            return Ok(());
        }
        let o_year = if orderdate < DIRECT_Q7_YEAR_1996_START_1996_01_01 {
            1995
        } else {
            1996
        };
        self.qualifying_orders
            .insert(q8_parse_i32(&fields, 0, "o_orderkey")?, o_year);
        Ok(())
    }

    fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() < 2 || payload[0] != 0 || payload[1] != 0 {
            bail!("TPC-H Q8 lineitem sidecar requires non-null lineitem rows");
        }
        let mut off = 2;
        let orderkey = read_direct_i32(payload, &mut off, "l_orderkey")?;
        let partkey = read_direct_i32(payload, &mut off, "l_partkey")?;
        let suppkey = read_direct_i32(payload, &mut off, "l_suppkey")?;
        let _linenumber = read_direct_i32(payload, &mut off, "l_linenumber")?;
        let _quantity = read_direct_decimal_i64(payload, &mut off, "l_quantity")?;
        let extendedprice = read_direct_decimal_i64(payload, &mut off, "l_extendedprice")?;
        let discount = read_direct_decimal_i64(payload, &mut off, "l_discount")?;
        self.add_lineitem_volume(orderkey, partkey, suppkey, extendedprice, discount)
    }

    fn add_lineitem_volume(
        &mut self,
        orderkey: i32,
        partkey: i32,
        suppkey: i32,
        extendedprice: i64,
        discount: i64,
    ) -> Result<()> {
        let Some(&o_year) = self.qualifying_orders.get(&orderkey) else {
            return Ok(());
        };
        if !self.qualifying_parts.contains(&partkey) {
            return Ok(());
        }
        let Some(&is_brazil) = self.suppliers.get(&suppkey) else {
            return Ok(());
        };
        let volume = checked_direct_discounted_revenue(extendedprice, discount)?;
        let state = self.years.entry(o_year).or_default();
        state.total_volume = checked_direct_revenue_add(state.total_volume, volume)?;
        if is_brazil {
            state.brazil_volume = checked_direct_revenue_add(state.brazil_volume, volume)?;
        }
        Ok(())
    }
}

#[cfg(feature = "sql-bench")]
fn q8_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
    let fields = parse_tbl_line(line).ok_or_else(|| anyhow::anyhow!("{table}: empty row"))?;
    if fields.len() != expected {
        bail!(
            "{table}: Q8 sidecar saw {} fields, expected {expected}",
            fields.len()
        );
    }
    Ok(fields)
}

#[cfg(feature = "sql-bench")]
fn q8_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
fn q8_i64_to_f64(value: i64) -> f64 {
    value
        .to_string()
        .parse::<f64>()
        .unwrap_or(if value.is_negative() {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        })
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
struct TpchQ9BuildState {
    green_parts: std::collections::HashSet<i32>,
    nations: std::collections::HashMap<i32, String>,
    suppliers: std::collections::HashMap<i32, String>,
    partsupp_cost: std::collections::HashMap<(i32, i32), i64>,
    orders: std::collections::HashMap<i32, i32>,
    profit_by_key: std::collections::BTreeMap<(String, i32), i64>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ9BuildState {
    fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "nation" => self.ingest_nation(line),
            "supplier" => self.ingest_supplier(line),
            "part" => self.ingest_part(line),
            "partsupp" => self.ingest_partsupp(line),
            "orders" => self.ingest_order(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ9ResultRow> {
        let mut rows: Vec<ultrasql_server::TpchQ9ResultRow> = self
            .profit_by_key
            .iter()
            .map(
                |((nation, o_year), &sum_profit)| ultrasql_server::TpchQ9ResultRow {
                    nation: nation.clone(),
                    o_year: *o_year,
                    sum_profit,
                },
            )
            .collect();
        rows.sort_by(|left, right| {
            left.nation
                .cmp(&right.nation)
                .then_with(|| right.o_year.cmp(&left.o_year))
        });
        rows
    }

    fn ingest_nation(&mut self, line: &str) -> Result<()> {
        let fields = q9_fields("nation", line, 4)?;
        self.nations
            .insert(q9_parse_i32(&fields, 0, "n_nationkey")?, fields[1].clone());
        Ok(())
    }

    fn ingest_supplier(&mut self, line: &str) -> Result<()> {
        let fields = q9_fields("supplier", line, 7)?;
        let nationkey = q9_parse_i32(&fields, 3, "s_nationkey")?;
        let Some(nation) = self.nations.get(&nationkey) else {
            return Ok(());
        };
        self.suppliers
            .insert(q9_parse_i32(&fields, 0, "s_suppkey")?, nation.clone());
        Ok(())
    }

    fn ingest_part(&mut self, line: &str) -> Result<()> {
        let fields = q9_fields("part", line, 9)?;
        if fields[1].contains("green") {
            self.green_parts
                .insert(q9_parse_i32(&fields, 0, "p_partkey")?);
        }
        Ok(())
    }

    fn ingest_partsupp(&mut self, line: &str) -> Result<()> {
        let fields = q9_fields("partsupp", line, 5)?;
        let partkey = q9_parse_i32(&fields, 0, "ps_partkey")?;
        if !self.green_parts.contains(&partkey) {
            return Ok(());
        }
        let suppkey = q9_parse_i32(&fields, 1, "ps_suppkey")?;
        self.partsupp_cost.insert(
            (partkey, suppkey),
            q9_parse_decimal2(&fields[3], "ps_supplycost")?,
        );
        Ok(())
    }

    fn ingest_order(&mut self, line: &str) -> Result<()> {
        let fields = q9_fields("orders", line, 9)?;
        let orderdate = parse_direct_date(&fields[4], 4).context("parse o_orderdate")?;
        self.orders.insert(
            q9_parse_i32(&fields, 0, "o_orderkey")?,
            direct_year_from_date(orderdate),
        );
        Ok(())
    }

    fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() < 2 || payload[0] != 0 || payload[1] != 0 {
            bail!("TPC-H Q9 lineitem sidecar requires non-null lineitem rows");
        }
        let mut off = 2;
        let orderkey = read_direct_i32(payload, &mut off, "l_orderkey")?;
        let partkey = read_direct_i32(payload, &mut off, "l_partkey")?;
        let suppkey = read_direct_i32(payload, &mut off, "l_suppkey")?;
        let _linenumber = read_direct_i32(payload, &mut off, "l_linenumber")?;
        let quantity = read_direct_decimal_i64(payload, &mut off, "l_quantity")?;
        let extendedprice = read_direct_decimal_i64(payload, &mut off, "l_extendedprice")?;
        let discount = read_direct_decimal_i64(payload, &mut off, "l_discount")?;
        self.add_lineitem_profit(
            orderkey,
            partkey,
            suppkey,
            quantity,
            extendedprice,
            discount,
        )
    }

    fn add_lineitem_profit(
        &mut self,
        orderkey: i32,
        partkey: i32,
        suppkey: i32,
        quantity: i64,
        extendedprice: i64,
        discount: i64,
    ) -> Result<()> {
        if !self.green_parts.contains(&partkey) {
            return Ok(());
        }
        let Some(nation) = self.suppliers.get(&suppkey) else {
            return Ok(());
        };
        let Some(&o_year) = self.orders.get(&orderkey) else {
            return Ok(());
        };
        let Some(&supplycost) = self.partsupp_cost.get(&(partkey, suppkey)) else {
            return Ok(());
        };
        let revenue = checked_direct_discounted_revenue(extendedprice, discount)?;
        let cost = checked_direct_scaled_product(supplycost, quantity)?;
        let profit = checked_direct_revenue_sub(revenue, cost)?;
        let entry = self
            .profit_by_key
            .entry((nation.clone(), o_year))
            .or_default();
        *entry = checked_direct_revenue_add(*entry, profit)?;
        Ok(())
    }
}

#[cfg(feature = "sql-bench")]
fn q9_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
    let fields = parse_tbl_line(line).ok_or_else(|| anyhow::anyhow!("{table}: empty row"))?;
    if fields.len() != expected {
        bail!(
            "{table}: Q9 sidecar saw {} fields, expected {expected}",
            fields.len()
        );
    }
    Ok(fields)
}

#[cfg(feature = "sql-bench")]
fn q9_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
fn q9_parse_decimal2(raw: &str, label: &str) -> Result<i64> {
    let ultrasql_core::Value::Decimal { value, .. } =
        parse_direct_decimal(raw, 2, 0).with_context(|| format!("parse {label} `{raw}`"))?
    else {
        unreachable!("parse_direct_decimal always returns Decimal");
    };
    Ok(value)
}

#[cfg(feature = "sql-bench")]
fn direct_year_from_date(days: i32) -> i32 {
    if days < -2_556 {
        1992
    } else if days < -2_191 {
        1993
    } else if days < -1_826 {
        1994
    } else if days < -1_461 {
        1995
    } else if days < -1_095 {
        1996
    } else if days < -730 {
        1997
    } else if days < -365 {
        1998
    } else {
        1999
    }
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Debug)]
struct TpchQ10Customer {
    name: String,
    acctbal: i64,
    nation: String,
    address: String,
    phone: String,
    comment: String,
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
struct TpchQ10BuildState {
    nations: std::collections::HashMap<i32, String>,
    customers: std::collections::HashMap<i32, TpchQ10Customer>,
    qualifying_orders: std::collections::HashMap<i32, i32>,
    revenue_by_customer: std::collections::HashMap<i32, i64>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ10BuildState {
    fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "nation" => self.ingest_nation(line),
            "customer" => self.ingest_customer(line),
            "orders" => self.ingest_order(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ10ResultRow> {
        let mut rows: Vec<ultrasql_server::TpchQ10ResultRow> = self
            .revenue_by_customer
            .iter()
            .filter_map(|(&custkey, &revenue)| {
                self.customers
                    .get(&custkey)
                    .map(|customer| ultrasql_server::TpchQ10ResultRow {
                        c_custkey: custkey,
                        c_name: customer.name.clone(),
                        revenue,
                        c_acctbal: customer.acctbal,
                        n_name: customer.nation.clone(),
                        c_address: customer.address.clone(),
                        c_phone: customer.phone.clone(),
                        c_comment: customer.comment.clone(),
                    })
            })
            .collect();
        rows.sort_by(|left, right| {
            right
                .revenue
                .cmp(&left.revenue)
                .then_with(|| left.c_custkey.cmp(&right.c_custkey))
        });
        rows.truncate(20);
        rows
    }

    fn ingest_nation(&mut self, line: &str) -> Result<()> {
        let fields = q10_fields("nation", line, 4)?;
        self.nations
            .insert(q10_parse_i32(&fields, 0, "n_nationkey")?, fields[1].clone());
        Ok(())
    }

    fn ingest_customer(&mut self, line: &str) -> Result<()> {
        let fields = q10_fields("customer", line, 8)?;
        let nationkey = q10_parse_i32(&fields, 3, "c_nationkey")?;
        let Some(nation) = self.nations.get(&nationkey) else {
            return Ok(());
        };
        self.customers.insert(
            q10_parse_i32(&fields, 0, "c_custkey")?,
            TpchQ10Customer {
                name: fields[1].clone(),
                address: fields[2].clone(),
                acctbal: q10_parse_decimal2(&fields[5], "c_acctbal")?,
                nation: nation.clone(),
                phone: fields[4].clone(),
                comment: fields[7].clone(),
            },
        );
        Ok(())
    }

    fn ingest_order(&mut self, line: &str) -> Result<()> {
        let fields = q10_fields("orders", line, 9)?;
        let orderdate = parse_direct_date(&fields[4], 4).context("parse o_orderdate")?;
        if !(DIRECT_Q4_ORDERDATE_END_1993_10_01..DIRECT_Q6_SHIPDATE_START_1994_01_01)
            .contains(&orderdate)
        {
            return Ok(());
        }
        let custkey = q10_parse_i32(&fields, 1, "o_custkey")?;
        if self.customers.contains_key(&custkey) {
            self.qualifying_orders
                .insert(q10_parse_i32(&fields, 0, "o_orderkey")?, custkey);
        }
        Ok(())
    }

    fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() < 2 || payload[0] != 0 || payload[1] != 0 {
            bail!("TPC-H Q10 lineitem sidecar requires non-null lineitem rows");
        }
        let mut off = 2;
        let orderkey = read_direct_i32(payload, &mut off, "l_orderkey")?;
        off = off.saturating_add(4 * 3);
        let _quantity = read_direct_decimal_i64(payload, &mut off, "l_quantity")?;
        let extendedprice = read_direct_decimal_i64(payload, &mut off, "l_extendedprice")?;
        let discount = read_direct_decimal_i64(payload, &mut off, "l_discount")?;
        let _tax = read_direct_decimal_i64(payload, &mut off, "l_tax")?;
        let returnflag = read_direct_one_byte_text(payload, &mut off, "l_returnflag")?;
        if returnflag != b'R' {
            return Ok(());
        }
        let Some(&custkey) = self.qualifying_orders.get(&orderkey) else {
            return Ok(());
        };
        let revenue = checked_direct_discounted_revenue(extendedprice, discount)?;
        let entry = self.revenue_by_customer.entry(custkey).or_default();
        *entry = checked_direct_revenue_add(*entry, revenue)?;
        Ok(())
    }
}

#[cfg(feature = "sql-bench")]
fn q10_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
    let fields = parse_tbl_line(line).ok_or_else(|| anyhow::anyhow!("{table}: empty row"))?;
    if fields.len() != expected {
        bail!(
            "{table}: Q10 sidecar saw {} fields, expected {expected}",
            fields.len()
        );
    }
    Ok(fields)
}

#[cfg(feature = "sql-bench")]
fn q10_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
fn q10_parse_decimal2(raw: &str, label: &str) -> Result<i64> {
    let ultrasql_core::Value::Decimal { value, .. } =
        parse_direct_decimal(raw, 2, 0).with_context(|| format!("parse {label} `{raw}`"))?
    else {
        unreachable!("parse_direct_decimal always returns Decimal");
    };
    Ok(value)
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
struct TpchQ11BuildState {
    german_nations: std::collections::BTreeSet<i32>,
    german_suppliers: std::collections::HashSet<i32>,
    value_by_part: std::collections::HashMap<i32, i64>,
    total_value: i64,
}

#[cfg(feature = "sql-bench")]
impl TpchQ11BuildState {
    fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "nation" => self.ingest_nation(line),
            "supplier" => self.ingest_supplier(line),
            "partsupp" => self.ingest_partsupp(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ11ResultRow> {
        let threshold = self.total_value / 10_000;
        let mut rows: Vec<ultrasql_server::TpchQ11ResultRow> = self
            .value_by_part
            .iter()
            .filter_map(|(&ps_partkey, &value)| {
                (value > threshold)
                    .then_some(ultrasql_server::TpchQ11ResultRow { ps_partkey, value })
            })
            .collect();
        rows.sort_by(|left, right| {
            right
                .value
                .cmp(&left.value)
                .then_with(|| left.ps_partkey.cmp(&right.ps_partkey))
        });
        rows
    }

    fn ingest_nation(&mut self, line: &str) -> Result<()> {
        let fields = q11_fields("nation", line, 4)?;
        if fields[1] == "GERMANY" {
            self.german_nations
                .insert(q11_parse_i32(&fields, 0, "n_nationkey")?);
        }
        Ok(())
    }

    fn ingest_supplier(&mut self, line: &str) -> Result<()> {
        let fields = q11_fields("supplier", line, 7)?;
        let nationkey = q11_parse_i32(&fields, 3, "s_nationkey")?;
        if self.german_nations.contains(&nationkey) {
            self.german_suppliers
                .insert(q11_parse_i32(&fields, 0, "s_suppkey")?);
        }
        Ok(())
    }

    fn ingest_partsupp(&mut self, line: &str) -> Result<()> {
        let fields = q11_fields("partsupp", line, 5)?;
        let suppkey = q11_parse_i32(&fields, 1, "ps_suppkey")?;
        if !self.german_suppliers.contains(&suppkey) {
            return Ok(());
        }
        let partkey = q11_parse_i32(&fields, 0, "ps_partkey")?;
        let availqty = q11_parse_i64(&fields, 2, "ps_availqty")?;
        let supplycost = q11_parse_decimal2(&fields[3], "ps_supplycost")?;
        let value = checked_direct_value_product(supplycost, availqty)?;
        let part_value = self.value_by_part.entry(partkey).or_default();
        *part_value = checked_direct_value_add(*part_value, value)?;
        self.total_value = checked_direct_value_add(self.total_value, value)?;
        Ok(())
    }
}

#[cfg(feature = "sql-bench")]
fn q11_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
    let fields = parse_tbl_line(line).ok_or_else(|| anyhow::anyhow!("{table}: empty row"))?;
    if fields.len() != expected {
        bail!(
            "{table}: Q11 sidecar saw {} fields, expected {expected}",
            fields.len()
        );
    }
    Ok(fields)
}

#[cfg(feature = "sql-bench")]
fn q11_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
fn q11_parse_i64(fields: &[String], idx: usize, label: &str) -> Result<i64> {
    fields[idx]
        .parse::<i64>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
fn q11_parse_decimal2(raw: &str, label: &str) -> Result<i64> {
    let ultrasql_core::Value::Decimal { value, .. } =
        parse_direct_decimal(raw, 2, 0).with_context(|| format!("parse {label} `{raw}`"))?
    else {
        unreachable!("parse_direct_decimal always returns Decimal");
    };
    Ok(value)
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
struct TpchQ12BuildState {
    high_priority_orders: std::collections::HashMap<i32, bool>,
    counts_by_shipmode: std::collections::BTreeMap<String, (i64, i64)>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ12BuildState {
    fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "orders" => self.ingest_order(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ12ResultRow> {
        self.counts_by_shipmode
            .iter()
            .map(|(shipmode, &(high_line_count, low_line_count))| {
                ultrasql_server::TpchQ12ResultRow {
                    l_shipmode: shipmode.clone(),
                    high_line_count,
                    low_line_count,
                }
            })
            .collect()
    }

    fn ingest_order(&mut self, line: &str) -> Result<()> {
        let fields = q12_fields("orders", line, 9)?;
        let orderkey = q12_parse_i32(&fields, 0, "o_orderkey")?;
        let high_priority = matches!(fields[5].as_str(), "1-URGENT" | "2-HIGH");
        self.high_priority_orders.insert(orderkey, high_priority);
        Ok(())
    }

    fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() < 2 || payload[0] != 0 || payload[1] != 0 {
            bail!("TPC-H Q12 lineitem sidecar requires non-null lineitem rows");
        }
        let mut off = 2;
        let orderkey = read_direct_i32(payload, &mut off, "l_orderkey")?;
        let _partkey = read_direct_i32(payload, &mut off, "l_partkey")?;
        let _suppkey = read_direct_i32(payload, &mut off, "l_suppkey")?;
        let _linenumber = read_direct_i32(payload, &mut off, "l_linenumber")?;
        let _quantity = read_direct_decimal_i64(payload, &mut off, "l_quantity")?;
        let _extendedprice = read_direct_decimal_i64(payload, &mut off, "l_extendedprice")?;
        let _discount = read_direct_decimal_i64(payload, &mut off, "l_discount")?;
        let _tax = read_direct_decimal_i64(payload, &mut off, "l_tax")?;
        let _returnflag = read_direct_one_byte_text(payload, &mut off, "l_returnflag")?;
        let _linestatus = read_direct_one_byte_text(payload, &mut off, "l_linestatus")?;
        let shipdate = read_direct_i32(payload, &mut off, "l_shipdate")?;
        let commitdate = read_direct_i32(payload, &mut off, "l_commitdate")?;
        let receiptdate = read_direct_i32(payload, &mut off, "l_receiptdate")?;
        let _shipinstruct = read_direct_text(payload, &mut off, "l_shipinstruct")?;
        let shipmode = read_direct_text(payload, &mut off, "l_shipmode")?;
        self.ingest_lineitem_values(orderkey, shipdate, commitdate, receiptdate, shipmode)
    }

    fn ingest_lineitem_values(
        &mut self,
        orderkey: i32,
        shipdate: i32,
        commitdate: i32,
        receiptdate: i32,
        shipmode: &str,
    ) -> Result<()> {
        if shipmode != "MAIL" && shipmode != "SHIP" {
            return Ok(());
        }
        if commitdate >= receiptdate
            || shipdate >= commitdate
            || !(DIRECT_Q12_RECEIPTDATE_START_1994_01_01..DIRECT_Q12_RECEIPTDATE_END_1995_01_01)
                .contains(&receiptdate)
        {
            return Ok(());
        }
        let Some(&high_priority) = self.high_priority_orders.get(&orderkey) else {
            return Ok(());
        };
        let counts = self
            .counts_by_shipmode
            .entry(shipmode.to_owned())
            .or_default();
        if high_priority {
            counts.0 = counts.0.saturating_add(1);
        } else {
            counts.1 = counts.1.saturating_add(1);
        }
        Ok(())
    }
}

#[cfg(feature = "sql-bench")]
fn q12_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
    let fields = parse_tbl_line(line).ok_or_else(|| anyhow::anyhow!("{table}: empty row"))?;
    if fields.len() != expected {
        bail!(
            "{table}: Q12 sidecar saw {} fields, expected {expected}",
            fields.len()
        );
    }
    Ok(fields)
}

#[cfg(feature = "sql-bench")]
fn q12_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
struct TpchQ13BuildState {
    total_customers: i64,
    customers_with_order_count: i64,
    order_count_by_customer: std::collections::HashMap<i32, i64>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ13BuildState {
    fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "customer" => self.ingest_customer(line),
            "orders" => self.ingest_order(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ13ResultRow> {
        let mut dist: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
        for &count in self.order_count_by_customer.values() {
            *dist.entry(count).or_default() += 1;
        }
        let zero_count_customers = self
            .total_customers
            .saturating_sub(self.customers_with_order_count);
        if zero_count_customers > 0 {
            *dist.entry(0).or_default() += zero_count_customers;
        }
        let mut rows: Vec<ultrasql_server::TpchQ13ResultRow> = dist
            .into_iter()
            .map(|(c_count, custdist)| ultrasql_server::TpchQ13ResultRow { c_count, custdist })
            .collect();
        rows.sort_by(|left, right| {
            right
                .custdist
                .cmp(&left.custdist)
                .then_with(|| right.c_count.cmp(&left.c_count))
        });
        rows
    }

    fn ingest_customer(&mut self, line: &str) -> Result<()> {
        let _fields = q13_fields("customer", line, 8)?;
        self.total_customers = self.total_customers.saturating_add(1);
        Ok(())
    }

    fn ingest_order(&mut self, line: &str) -> Result<()> {
        let fields = q13_fields("orders", line, 9)?;
        if q13_comment_has_special_requests(&fields[8]) {
            return Ok(());
        }
        let custkey = q13_parse_i32(&fields, 1, "o_custkey")?;
        match self.order_count_by_customer.entry(custkey) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                *entry.get_mut() = entry.get().saturating_add(1);
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(1);
                self.customers_with_order_count = self.customers_with_order_count.saturating_add(1);
            }
        }
        Ok(())
    }
}

#[cfg(feature = "sql-bench")]
fn q13_comment_has_special_requests(comment: &str) -> bool {
    comment
        .find("special")
        .is_some_and(|pos| comment[pos.saturating_add("special".len())..].contains("requests"))
}

#[cfg(feature = "sql-bench")]
fn q13_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
    let fields = parse_tbl_line(line).ok_or_else(|| anyhow::anyhow!("{table}: empty row"))?;
    if fields.len() != expected {
        bail!(
            "{table}: Q13 sidecar saw {} fields, expected {expected}",
            fields.len()
        );
    }
    Ok(fields)
}

#[cfg(feature = "sql-bench")]
fn q13_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
struct TpchQ14BuildState {
    promo_parts: std::collections::HashSet<i32>,
    promo_volume: i128,
    total_volume: i128,
}

#[cfg(feature = "sql-bench")]
impl TpchQ14BuildState {
    fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "part" => self.ingest_part(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ14ResultRow> {
        let promo_revenue = if self.total_volume == 0 {
            0.0
        } else {
            100.0 * self.promo_volume as f64 / self.total_volume as f64
        };
        vec![ultrasql_server::TpchQ14ResultRow { promo_revenue }]
    }

    fn ingest_part(&mut self, line: &str) -> Result<()> {
        let fields = q14_fields("part", line, 9)?;
        if fields[4].starts_with("PROMO") {
            self.promo_parts
                .insert(q14_parse_i32(&fields, 0, "p_partkey")?);
        }
        Ok(())
    }

    fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() < 2 || payload[0] != 0 || payload[1] != 0 {
            bail!("TPC-H Q14 lineitem sidecar requires non-null lineitem rows");
        }
        let mut off = 2;
        let _orderkey = read_direct_i32(payload, &mut off, "l_orderkey")?;
        let partkey = read_direct_i32(payload, &mut off, "l_partkey")?;
        let _suppkey = read_direct_i32(payload, &mut off, "l_suppkey")?;
        let _linenumber = read_direct_i32(payload, &mut off, "l_linenumber")?;
        let _quantity = read_direct_decimal_i64(payload, &mut off, "l_quantity")?;
        let extendedprice = read_direct_decimal_i64(payload, &mut off, "l_extendedprice")?;
        let discount = read_direct_decimal_i64(payload, &mut off, "l_discount")?;
        let _tax = read_direct_decimal_i64(payload, &mut off, "l_tax")?;
        let _returnflag = read_direct_one_byte_text(payload, &mut off, "l_returnflag")?;
        let _linestatus = read_direct_one_byte_text(payload, &mut off, "l_linestatus")?;
        let shipdate = read_direct_i32(payload, &mut off, "l_shipdate")?;
        self.ingest_lineitem_values(partkey, extendedprice, discount, shipdate)
    }

    fn ingest_lineitem_values(
        &mut self,
        partkey: i32,
        extendedprice: i64,
        discount: i64,
        shipdate: i32,
    ) -> Result<()> {
        if !(DIRECT_Q14_SHIPDATE_START_1995_09_01..DIRECT_Q14_SHIPDATE_END_1995_10_01)
            .contains(&shipdate)
        {
            return Ok(());
        }
        let volume = checked_direct_discounted_revenue_i128(extendedprice, discount)?;
        self.total_volume = checked_direct_revenue_add_i128(self.total_volume, volume)?;
        if self.promo_parts.contains(&partkey) {
            self.promo_volume = checked_direct_revenue_add_i128(self.promo_volume, volume)?;
        }
        Ok(())
    }
}

#[cfg(feature = "sql-bench")]
fn q14_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
    let fields = parse_tbl_line(line).ok_or_else(|| anyhow::anyhow!("{table}: empty row"))?;
    if fields.len() != expected {
        bail!(
            "{table}: Q14 sidecar saw {} fields, expected {expected}",
            fields.len()
        );
    }
    Ok(fields)
}

#[cfg(feature = "sql-bench")]
fn q14_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Debug)]
struct TpchQ15Supplier {
    name: String,
    address: String,
    phone: String,
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
struct TpchQ15BuildState {
    suppliers: std::collections::HashMap<i32, TpchQ15Supplier>,
    revenue_by_supplier: std::collections::HashMap<i32, i64>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ15BuildState {
    fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "supplier" => self.ingest_supplier(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ15ResultRow> {
        let Some(max_revenue) = self.revenue_by_supplier.values().copied().max() else {
            return Vec::new();
        };
        let mut rows: Vec<ultrasql_server::TpchQ15ResultRow> = self
            .revenue_by_supplier
            .iter()
            .filter_map(|(&suppkey, &total_revenue)| {
                if total_revenue != max_revenue {
                    return None;
                }
                let supplier = self.suppliers.get(&suppkey)?;
                Some(ultrasql_server::TpchQ15ResultRow {
                    s_suppkey: suppkey,
                    s_name: supplier.name.clone(),
                    s_address: supplier.address.clone(),
                    s_phone: supplier.phone.clone(),
                    total_revenue,
                })
            })
            .collect();
        rows.sort_by_key(|row| row.s_suppkey);
        rows
    }

    fn ingest_supplier(&mut self, line: &str) -> Result<()> {
        let fields = q15_fields("supplier", line, 7)?;
        self.suppliers.insert(
            q15_parse_i32(&fields, 0, "s_suppkey")?,
            TpchQ15Supplier {
                name: fields[1].clone(),
                address: fields[2].clone(),
                phone: fields[4].clone(),
            },
        );
        Ok(())
    }

    fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() < 2 || payload[0] != 0 || payload[1] != 0 {
            bail!("TPC-H Q15 lineitem sidecar requires non-null lineitem rows");
        }
        let mut off = 2;
        let _orderkey = read_direct_i32(payload, &mut off, "l_orderkey")?;
        let _partkey = read_direct_i32(payload, &mut off, "l_partkey")?;
        let suppkey = read_direct_i32(payload, &mut off, "l_suppkey")?;
        let _linenumber = read_direct_i32(payload, &mut off, "l_linenumber")?;
        let _quantity = read_direct_decimal_i64(payload, &mut off, "l_quantity")?;
        let extendedprice = read_direct_decimal_i64(payload, &mut off, "l_extendedprice")?;
        let discount = read_direct_decimal_i64(payload, &mut off, "l_discount")?;
        let _tax = read_direct_decimal_i64(payload, &mut off, "l_tax")?;
        let _returnflag = read_direct_one_byte_text(payload, &mut off, "l_returnflag")?;
        let _linestatus = read_direct_one_byte_text(payload, &mut off, "l_linestatus")?;
        let shipdate = read_direct_i32(payload, &mut off, "l_shipdate")?;
        self.ingest_lineitem_values(suppkey, extendedprice, discount, shipdate)
    }

    fn ingest_lineitem_values(
        &mut self,
        suppkey: i32,
        extendedprice: i64,
        discount: i64,
        shipdate: i32,
    ) -> Result<()> {
        if !(DIRECT_Q15_SHIPDATE_START_1996_01_01..DIRECT_Q15_SHIPDATE_END_1996_04_01)
            .contains(&shipdate)
        {
            return Ok(());
        }
        let revenue = checked_direct_discounted_revenue_x100(extendedprice, discount)?;
        let entry = self.revenue_by_supplier.entry(suppkey).or_default();
        *entry = checked_direct_revenue_add(*entry, revenue)?;
        Ok(())
    }
}

#[cfg(feature = "sql-bench")]
fn q15_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
    let fields = parse_tbl_line(line).ok_or_else(|| anyhow::anyhow!("{table}: empty row"))?;
    if fields.len() != expected {
        bail!(
            "{table}: Q15 sidecar saw {} fields, expected {expected}",
            fields.len()
        );
    }
    Ok(fields)
}

#[cfg(feature = "sql-bench")]
fn q15_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct TpchQ16GroupKey {
    brand: String,
    part_type: String,
    size: i32,
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Debug)]
struct TpchQ16Part {
    key: TpchQ16GroupKey,
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
struct TpchQ16BuildState {
    bad_suppliers: std::collections::HashSet<i32>,
    parts: std::collections::HashMap<i32, TpchQ16Part>,
    suppliers_by_group: std::collections::HashMap<TpchQ16GroupKey, std::collections::HashSet<i32>>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ16BuildState {
    fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "supplier" => self.ingest_supplier(line),
            "part" => self.ingest_part(line),
            "partsupp" => self.ingest_partsupp(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ16ResultRow> {
        let mut rows: Vec<ultrasql_server::TpchQ16ResultRow> = self
            .suppliers_by_group
            .iter()
            .filter_map(|(key, suppliers)| {
                let supplier_cnt = i64::try_from(suppliers.len()).ok()?;
                Some(ultrasql_server::TpchQ16ResultRow {
                    p_brand: key.brand.clone(),
                    p_type: key.part_type.clone(),
                    p_size: key.size,
                    supplier_cnt,
                })
            })
            .collect();
        rows.sort_by(|left, right| {
            right
                .supplier_cnt
                .cmp(&left.supplier_cnt)
                .then_with(|| left.p_brand.cmp(&right.p_brand))
                .then_with(|| left.p_type.cmp(&right.p_type))
                .then_with(|| left.p_size.cmp(&right.p_size))
        });
        rows
    }

    fn ingest_supplier(&mut self, line: &str) -> Result<()> {
        let fields = q16_fields("supplier", line, 7)?;
        if q16_comment_has_customer_complaints(&fields[6]) {
            self.bad_suppliers
                .insert(q16_parse_i32(&fields, 0, "s_suppkey")?);
        }
        Ok(())
    }

    fn ingest_part(&mut self, line: &str) -> Result<()> {
        let fields = q16_fields("part", line, 9)?;
        let brand = &fields[3];
        let part_type = &fields[4];
        let size = q16_parse_i32(&fields, 5, "p_size")?;
        if brand == "Brand#45"
            || part_type.starts_with("MEDIUM POLISHED")
            || !matches!(size, 49 | 14 | 23 | 45 | 19 | 3 | 36 | 9)
        {
            return Ok(());
        }
        self.parts.insert(
            q16_parse_i32(&fields, 0, "p_partkey")?,
            TpchQ16Part {
                key: TpchQ16GroupKey {
                    brand: brand.clone(),
                    part_type: part_type.clone(),
                    size,
                },
            },
        );
        Ok(())
    }

    fn ingest_partsupp(&mut self, line: &str) -> Result<()> {
        let fields = q16_fields("partsupp", line, 5)?;
        let partkey = q16_parse_i32(&fields, 0, "ps_partkey")?;
        let suppkey = q16_parse_i32(&fields, 1, "ps_suppkey")?;
        if self.bad_suppliers.contains(&suppkey) {
            return Ok(());
        }
        let Some(part) = self.parts.get(&partkey) else {
            return Ok(());
        };
        self.suppliers_by_group
            .entry(part.key.clone())
            .or_default()
            .insert(suppkey);
        Ok(())
    }
}

#[cfg(feature = "sql-bench")]
fn q16_comment_has_customer_complaints(comment: &str) -> bool {
    comment
        .find("Customer")
        .is_some_and(|pos| comment[pos.saturating_add("Customer".len())..].contains("Complaints"))
}

#[cfg(feature = "sql-bench")]
fn q16_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
    let fields = parse_tbl_line(line).ok_or_else(|| anyhow::anyhow!("{table}: empty row"))?;
    if fields.len() != expected {
        bail!(
            "{table}: Q16 sidecar saw {} fields, expected {expected}",
            fields.len()
        );
    }
    Ok(fields)
}

#[cfg(feature = "sql-bench")]
fn q16_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Debug)]
struct TpchQ17Line {
    partkey: i32,
    quantity: i64,
    extendedprice: i64,
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Debug, Default)]
struct TpchQ17PartStats {
    sum_quantity: i128,
    count: i64,
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
struct TpchQ17BuildState {
    qualifying_parts: std::collections::HashSet<i32>,
    stats_by_part: std::collections::HashMap<i32, TpchQ17PartStats>,
    lines: Vec<TpchQ17Line>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ17BuildState {
    fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "part" => self.ingest_part(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ17ResultRow> {
        let mut revenue_sum: i128 = 0;
        for line in &self.lines {
            let Some(stats) = self.stats_by_part.get(&line.partkey) else {
                continue;
            };
            if i128::from(line.quantity) * 5 * i128::from(stats.count) < stats.sum_quantity {
                revenue_sum += i128::from(line.extendedprice);
            }
        }
        let avg_yearly = revenue_sum as f64 / 700.0;
        vec![ultrasql_server::TpchQ17ResultRow { avg_yearly }]
    }

    fn ingest_part(&mut self, line: &str) -> Result<()> {
        let fields = q17_fields("part", line, 9)?;
        if fields[3] == "Brand#23" && fields[6] == "MED BOX" {
            self.qualifying_parts
                .insert(q17_parse_i32(&fields, 0, "p_partkey")?);
        }
        Ok(())
    }

    fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() < 2 || payload[0] != 0 || payload[1] != 0 {
            bail!("TPC-H Q17 lineitem sidecar requires non-null lineitem rows");
        }
        let mut off = 2;
        let _orderkey = read_direct_i32(payload, &mut off, "l_orderkey")?;
        let partkey = read_direct_i32(payload, &mut off, "l_partkey")?;
        let _suppkey = read_direct_i32(payload, &mut off, "l_suppkey")?;
        let _linenumber = read_direct_i32(payload, &mut off, "l_linenumber")?;
        let quantity = read_direct_decimal_i64(payload, &mut off, "l_quantity")?;
        let extendedprice = read_direct_decimal_i64(payload, &mut off, "l_extendedprice")?;
        self.ingest_lineitem_values(partkey, quantity, extendedprice)
    }

    fn ingest_lineitem_values(
        &mut self,
        partkey: i32,
        quantity: i64,
        extendedprice: i64,
    ) -> Result<()> {
        if !self.qualifying_parts.contains(&partkey) {
            return Ok(());
        }
        let stats = self.stats_by_part.entry(partkey).or_default();
        stats.sum_quantity += i128::from(quantity);
        stats.count = stats.count.saturating_add(1);
        self.lines.push(TpchQ17Line {
            partkey,
            quantity,
            extendedprice,
        });
        Ok(())
    }
}

#[cfg(feature = "sql-bench")]
fn q17_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
    let fields = parse_tbl_line(line).ok_or_else(|| anyhow::anyhow!("{table}: empty row"))?;
    if fields.len() != expected {
        bail!(
            "{table}: Q17 sidecar saw {} fields, expected {expected}",
            fields.len()
        );
    }
    Ok(fields)
}

#[cfg(feature = "sql-bench")]
fn q17_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Debug)]
struct TpchQ18Customer {
    name: String,
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Debug)]
struct TpchQ18Order {
    custkey: i32,
    orderdate: i32,
    totalprice: i64,
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
struct TpchQ18BuildState {
    customers: std::collections::HashMap<i32, TpchQ18Customer>,
    orders: std::collections::HashMap<i32, TpchQ18Order>,
    quantity_by_order: std::collections::HashMap<i32, i64>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ18BuildState {
    fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "customer" => self.ingest_customer(line),
            "orders" => self.ingest_order(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ18ResultRow> {
        let mut rows: Vec<ultrasql_server::TpchQ18ResultRow> = self
            .quantity_by_order
            .iter()
            .filter_map(|(&orderkey, &sum_quantity)| {
                if sum_quantity <= 30_000 {
                    return None;
                }
                let order = self.orders.get(&orderkey)?;
                let customer = self.customers.get(&order.custkey)?;
                Some(ultrasql_server::TpchQ18ResultRow {
                    c_name: customer.name.clone(),
                    c_custkey: order.custkey,
                    o_orderkey: orderkey,
                    o_orderdate: order.orderdate,
                    o_totalprice: order.totalprice,
                    sum_quantity,
                })
            })
            .collect();
        rows.sort_by(|left, right| {
            right
                .o_totalprice
                .cmp(&left.o_totalprice)
                .then_with(|| left.o_orderdate.cmp(&right.o_orderdate))
        });
        rows.truncate(100);
        rows
    }

    fn ingest_customer(&mut self, line: &str) -> Result<()> {
        let fields = q18_fields("customer", line, 8)?;
        self.customers.insert(
            q18_parse_i32(&fields, 0, "c_custkey")?,
            TpchQ18Customer {
                name: fields[1].clone(),
            },
        );
        Ok(())
    }

    fn ingest_order(&mut self, line: &str) -> Result<()> {
        let fields = q18_fields("orders", line, 9)?;
        self.orders.insert(
            q18_parse_i32(&fields, 0, "o_orderkey")?,
            TpchQ18Order {
                custkey: q18_parse_i32(&fields, 1, "o_custkey")?,
                totalprice: q18_parse_decimal2(&fields[3], "o_totalprice")?,
                orderdate: parse_direct_date(&fields[4], 4).context("parse o_orderdate")?,
            },
        );
        Ok(())
    }

    fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() < 2 || payload[0] != 0 || payload[1] != 0 {
            bail!("TPC-H Q18 lineitem sidecar requires non-null lineitem rows");
        }
        let mut off = 2;
        let orderkey = read_direct_i32(payload, &mut off, "l_orderkey")?;
        let _partkey = read_direct_i32(payload, &mut off, "l_partkey")?;
        let _suppkey = read_direct_i32(payload, &mut off, "l_suppkey")?;
        let _linenumber = read_direct_i32(payload, &mut off, "l_linenumber")?;
        let quantity = read_direct_decimal_i64(payload, &mut off, "l_quantity")?;
        self.ingest_lineitem_values(orderkey, quantity)
    }

    fn ingest_lineitem_values(&mut self, orderkey: i32, quantity: i64) -> Result<()> {
        let order_quantity = self.quantity_by_order.entry(orderkey).or_default();
        *order_quantity = checked_direct_quantity_add_i64(*order_quantity, quantity)?;
        Ok(())
    }
}

#[cfg(feature = "sql-bench")]
fn q18_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
    let fields = parse_tbl_line(line).ok_or_else(|| anyhow::anyhow!("{table}: empty row"))?;
    if fields.len() != expected {
        bail!(
            "{table}: Q18 sidecar saw {} fields, expected {expected}",
            fields.len()
        );
    }
    Ok(fields)
}

#[cfg(feature = "sql-bench")]
fn q18_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
fn q18_parse_decimal2(raw: &str, label: &str) -> Result<i64> {
    let ultrasql_core::Value::Decimal { value, .. } =
        parse_direct_decimal(raw, 2, 0).with_context(|| format!("parse {label} `{raw}`"))?
    else {
        unreachable!("parse_direct_decimal always returns Decimal");
    };
    Ok(value)
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Copy, Debug)]
struct TpchQ19Band {
    quantity_min: i64,
    quantity_max: i64,
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
struct TpchQ19BuildState {
    parts: std::collections::HashMap<i32, TpchQ19Band>,
    revenue: i64,
}

#[cfg(feature = "sql-bench")]
impl TpchQ19BuildState {
    fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "part" => self.ingest_part(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ19ResultRow> {
        vec![ultrasql_server::TpchQ19ResultRow {
            revenue: self.revenue,
        }]
    }

    fn ingest_part(&mut self, line: &str) -> Result<()> {
        let fields = q19_fields("part", line, 9)?;
        let brand = &fields[3];
        let size = q19_parse_i32(&fields, 5, "p_size")?;
        let container = &fields[6];
        let Some(band) = q19_part_band(brand, container, size) else {
            return Ok(());
        };
        self.parts
            .insert(q19_parse_i32(&fields, 0, "p_partkey")?, band);
        Ok(())
    }

    fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() < 2 || payload[0] != 0 || payload[1] != 0 {
            bail!("TPC-H Q19 lineitem sidecar requires non-null lineitem rows");
        }
        let mut off = 2;
        let _orderkey = read_direct_i32(payload, &mut off, "l_orderkey")?;
        let partkey = read_direct_i32(payload, &mut off, "l_partkey")?;
        let _suppkey = read_direct_i32(payload, &mut off, "l_suppkey")?;
        let _linenumber = read_direct_i32(payload, &mut off, "l_linenumber")?;
        let quantity = read_direct_decimal_i64(payload, &mut off, "l_quantity")?;
        let extendedprice = read_direct_decimal_i64(payload, &mut off, "l_extendedprice")?;
        let discount = read_direct_decimal_i64(payload, &mut off, "l_discount")?;
        let _tax = read_direct_decimal_i64(payload, &mut off, "l_tax")?;
        let _returnflag = read_direct_one_byte_text(payload, &mut off, "l_returnflag")?;
        let _linestatus = read_direct_one_byte_text(payload, &mut off, "l_linestatus")?;
        let _shipdate = read_direct_i32(payload, &mut off, "l_shipdate")?;
        let _commitdate = read_direct_i32(payload, &mut off, "l_commitdate")?;
        let _receiptdate = read_direct_i32(payload, &mut off, "l_receiptdate")?;
        let shipinstruct = read_direct_text(payload, &mut off, "l_shipinstruct")?;
        let shipmode = read_direct_text(payload, &mut off, "l_shipmode")?;
        self.ingest_lineitem_values(
            partkey,
            quantity,
            extendedprice,
            discount,
            shipmode,
            shipinstruct,
        )
    }

    fn ingest_lineitem_values(
        &mut self,
        partkey: i32,
        quantity: i64,
        extendedprice: i64,
        discount: i64,
        shipmode: &str,
        shipinstruct: &str,
    ) -> Result<()> {
        if shipinstruct != "DELIVER IN PERSON" || !matches!(shipmode, "AIR" | "AIR REG") {
            return Ok(());
        }
        let Some(band) = self.parts.get(&partkey) else {
            return Ok(());
        };
        if quantity < band.quantity_min || quantity > band.quantity_max {
            return Ok(());
        }
        let revenue = checked_direct_discounted_revenue_x100(extendedprice, discount)?;
        self.revenue = checked_direct_revenue_add(self.revenue, revenue)?;
        Ok(())
    }
}

#[cfg(feature = "sql-bench")]
fn q19_part_band(brand: &str, container: &str, size: i32) -> Option<TpchQ19Band> {
    match brand {
        "Brand#12"
            if (1..=5).contains(&size)
                && matches!(container, "SM CASE" | "SM BOX" | "SM PACK" | "SM PKG") =>
        {
            Some(TpchQ19Band {
                quantity_min: 1_00,
                quantity_max: 11_00,
            })
        }
        "Brand#23"
            if (1..=10).contains(&size)
                && matches!(container, "MED BAG" | "MED BOX" | "MED PKG" | "MED PACK") =>
        {
            Some(TpchQ19Band {
                quantity_min: 10_00,
                quantity_max: 20_00,
            })
        }
        "Brand#34"
            if (1..=15).contains(&size)
                && matches!(container, "LG CASE" | "LG BOX" | "LG PACK" | "LG PKG") =>
        {
            Some(TpchQ19Band {
                quantity_min: 20_00,
                quantity_max: 30_00,
            })
        }
        _ => None,
    }
}

#[cfg(feature = "sql-bench")]
fn q19_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
    let fields = parse_tbl_line(line).ok_or_else(|| anyhow::anyhow!("{table}: empty row"))?;
    if fields.len() != expected {
        bail!(
            "{table}: Q19 sidecar saw {} fields, expected {expected}",
            fields.len()
        );
    }
    Ok(fields)
}

#[cfg(feature = "sql-bench")]
fn q19_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Debug)]
struct TpchQ20Supplier {
    name: String,
    address: String,
    nationkey: i32,
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Copy, Debug)]
struct TpchQ20PartSupp {
    partkey: i32,
    suppkey: i32,
    availqty: i64,
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
struct TpchQ20BuildState {
    canada_nationkeys: std::collections::HashSet<i32>,
    suppliers: std::collections::HashMap<i32, TpchQ20Supplier>,
    forest_parts: std::collections::HashSet<i32>,
    forest_partsupps: Vec<TpchQ20PartSupp>,
    quantity_by_part_supplier: std::collections::HashMap<(i32, i32), i64>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ20BuildState {
    fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "nation" => self.ingest_nation(line),
            "supplier" => self.ingest_supplier(line),
            "part" => self.ingest_part(line),
            "partsupp" => self.ingest_partsupp(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ20ResultRow> {
        let mut rows_by_supplier = std::collections::HashMap::new();
        for partsupp in &self.forest_partsupps {
            let Some(&sum_quantity) = self
                .quantity_by_part_supplier
                .get(&(partsupp.partkey, partsupp.suppkey))
            else {
                continue;
            };
            if i128::from(partsupp.availqty) * 200 <= i128::from(sum_quantity) {
                continue;
            }
            let Some(supplier) = self.suppliers.get(&partsupp.suppkey) else {
                continue;
            };
            if !self.canada_nationkeys.contains(&supplier.nationkey) {
                continue;
            }
            rows_by_supplier.insert(
                partsupp.suppkey,
                ultrasql_server::TpchQ20ResultRow {
                    s_name: supplier.name.clone(),
                    s_address: supplier.address.clone(),
                },
            );
        }
        let mut rows: Vec<_> = rows_by_supplier.into_values().collect();
        rows.sort_by(|left, right| left.s_name.cmp(&right.s_name));
        rows
    }

    fn ingest_nation(&mut self, line: &str) -> Result<()> {
        let fields = q20_fields("nation", line, 4)?;
        if fields[1] == "CANADA" {
            self.canada_nationkeys
                .insert(q20_parse_i32(&fields, 0, "n_nationkey")?);
        }
        Ok(())
    }

    fn ingest_supplier(&mut self, line: &str) -> Result<()> {
        let fields = q20_fields("supplier", line, 7)?;
        self.suppliers.insert(
            q20_parse_i32(&fields, 0, "s_suppkey")?,
            TpchQ20Supplier {
                name: fields[1].clone(),
                address: fields[2].clone(),
                nationkey: q20_parse_i32(&fields, 3, "s_nationkey")?,
            },
        );
        Ok(())
    }

    fn ingest_part(&mut self, line: &str) -> Result<()> {
        let fields = q20_fields("part", line, 9)?;
        if fields[1].starts_with("forest") {
            self.forest_parts
                .insert(q20_parse_i32(&fields, 0, "p_partkey")?);
        }
        Ok(())
    }

    fn ingest_partsupp(&mut self, line: &str) -> Result<()> {
        let fields = q20_fields("partsupp", line, 5)?;
        let partkey = q20_parse_i32(&fields, 0, "ps_partkey")?;
        if !self.forest_parts.contains(&partkey) {
            return Ok(());
        }
        self.forest_partsupps.push(TpchQ20PartSupp {
            partkey,
            suppkey: q20_parse_i32(&fields, 1, "ps_suppkey")?,
            availqty: i64::from(q20_parse_i32(&fields, 2, "ps_availqty")?),
        });
        Ok(())
    }

    fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() < 2 || payload[0] != 0 || payload[1] != 0 {
            bail!("TPC-H Q20 lineitem sidecar requires non-null lineitem rows");
        }
        let mut off = 2;
        let _orderkey = read_direct_i32(payload, &mut off, "l_orderkey")?;
        let partkey = read_direct_i32(payload, &mut off, "l_partkey")?;
        let suppkey = read_direct_i32(payload, &mut off, "l_suppkey")?;
        let _linenumber = read_direct_i32(payload, &mut off, "l_linenumber")?;
        let quantity = read_direct_decimal_i64(payload, &mut off, "l_quantity")?;
        let _extendedprice = read_direct_decimal_i64(payload, &mut off, "l_extendedprice")?;
        let _discount = read_direct_decimal_i64(payload, &mut off, "l_discount")?;
        let _tax = read_direct_decimal_i64(payload, &mut off, "l_tax")?;
        let _returnflag = read_direct_one_byte_text(payload, &mut off, "l_returnflag")?;
        let _linestatus = read_direct_one_byte_text(payload, &mut off, "l_linestatus")?;
        let shipdate = read_direct_i32(payload, &mut off, "l_shipdate")?;
        self.ingest_lineitem_values(partkey, suppkey, quantity, shipdate)
    }

    fn ingest_lineitem_values(
        &mut self,
        partkey: i32,
        suppkey: i32,
        quantity: i64,
        shipdate: i32,
    ) -> Result<()> {
        if !self.forest_parts.contains(&partkey)
            || !(DIRECT_Q6_SHIPDATE_START_1994_01_01..DIRECT_Q6_SHIPDATE_END_1995_01_01)
                .contains(&shipdate)
        {
            return Ok(());
        }
        *self
            .quantity_by_part_supplier
            .entry((partkey, suppkey))
            .or_default() += quantity;
        Ok(())
    }
}

#[cfg(feature = "sql-bench")]
fn q20_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
    let fields = parse_tbl_line(line).ok_or_else(|| anyhow::anyhow!("{table}: empty row"))?;
    if fields.len() != expected {
        bail!(
            "{table}: Q20 sidecar saw {} fields, expected {expected}",
            fields.len()
        );
    }
    Ok(fields)
}

#[cfg(feature = "sql-bench")]
fn q20_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Debug)]
struct TpchQ21Supplier {
    name: String,
    nationkey: i32,
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
struct TpchQ21Order {
    suppliers: std::collections::HashSet<i32>,
    late_count_by_supplier: std::collections::HashMap<i32, i64>,
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
struct TpchQ21BuildState {
    saudi_nationkeys: std::collections::HashSet<i32>,
    suppliers: std::collections::HashMap<i32, TpchQ21Supplier>,
    final_orders: std::collections::HashSet<i32>,
    orders: std::collections::HashMap<i32, TpchQ21Order>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ21BuildState {
    fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "nation" => self.ingest_nation(line),
            "supplier" => self.ingest_supplier(line),
            "orders" => self.ingest_order(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ21ResultRow> {
        let mut count_by_supplier_name = std::collections::HashMap::<String, i64>::new();
        for (&orderkey, order) in &self.orders {
            if !self.final_orders.contains(&orderkey) || order.suppliers.len() < 2 {
                continue;
            }
            for (&suppkey, &late_count) in &order.late_count_by_supplier {
                if order
                    .late_count_by_supplier
                    .keys()
                    .any(|&other_suppkey| other_suppkey != suppkey)
                {
                    continue;
                }
                let Some(supplier) = self.suppliers.get(&suppkey) else {
                    continue;
                };
                if !self.saudi_nationkeys.contains(&supplier.nationkey) {
                    continue;
                }
                *count_by_supplier_name
                    .entry(supplier.name.clone())
                    .or_default() += late_count;
            }
        }
        let mut rows: Vec<ultrasql_server::TpchQ21ResultRow> = count_by_supplier_name
            .into_iter()
            .map(|(s_name, numwait)| ultrasql_server::TpchQ21ResultRow { s_name, numwait })
            .collect();
        rows.sort_by(|left, right| {
            right
                .numwait
                .cmp(&left.numwait)
                .then_with(|| left.s_name.cmp(&right.s_name))
        });
        rows.truncate(100);
        rows
    }

    fn ingest_nation(&mut self, line: &str) -> Result<()> {
        let fields = q21_fields("nation", line, 4)?;
        if fields[1] == "SAUDI ARABIA" {
            self.saudi_nationkeys
                .insert(q21_parse_i32(&fields, 0, "n_nationkey")?);
        }
        Ok(())
    }

    fn ingest_supplier(&mut self, line: &str) -> Result<()> {
        let fields = q21_fields("supplier", line, 7)?;
        self.suppliers.insert(
            q21_parse_i32(&fields, 0, "s_suppkey")?,
            TpchQ21Supplier {
                name: fields[1].clone(),
                nationkey: q21_parse_i32(&fields, 3, "s_nationkey")?,
            },
        );
        Ok(())
    }

    fn ingest_order(&mut self, line: &str) -> Result<()> {
        let fields = q21_fields("orders", line, 9)?;
        if fields[2] == "F" {
            self.final_orders
                .insert(q21_parse_i32(&fields, 0, "o_orderkey")?);
        }
        Ok(())
    }

    fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() < 2 || payload[0] != 0 || payload[1] != 0 {
            bail!("TPC-H Q21 lineitem sidecar requires non-null lineitem rows");
        }
        let mut off = 2;
        let orderkey = read_direct_i32(payload, &mut off, "l_orderkey")?;
        let _partkey = read_direct_i32(payload, &mut off, "l_partkey")?;
        let suppkey = read_direct_i32(payload, &mut off, "l_suppkey")?;
        let _linenumber = read_direct_i32(payload, &mut off, "l_linenumber")?;
        let _quantity = read_direct_decimal_i64(payload, &mut off, "l_quantity")?;
        let _extendedprice = read_direct_decimal_i64(payload, &mut off, "l_extendedprice")?;
        let _discount = read_direct_decimal_i64(payload, &mut off, "l_discount")?;
        let _tax = read_direct_decimal_i64(payload, &mut off, "l_tax")?;
        let _returnflag = read_direct_one_byte_text(payload, &mut off, "l_returnflag")?;
        let _linestatus = read_direct_one_byte_text(payload, &mut off, "l_linestatus")?;
        let _shipdate = read_direct_i32(payload, &mut off, "l_shipdate")?;
        let commitdate = read_direct_i32(payload, &mut off, "l_commitdate")?;
        let receiptdate = read_direct_i32(payload, &mut off, "l_receiptdate")?;
        self.ingest_lineitem_values(orderkey, suppkey, commitdate, receiptdate)
    }

    fn ingest_lineitem_values(
        &mut self,
        orderkey: i32,
        suppkey: i32,
        commitdate: i32,
        receiptdate: i32,
    ) -> Result<()> {
        let order = self.orders.entry(orderkey).or_default();
        order.suppliers.insert(suppkey);
        if receiptdate > commitdate {
            *order.late_count_by_supplier.entry(suppkey).or_default() += 1;
        }
        Ok(())
    }
}

#[cfg(feature = "sql-bench")]
fn q21_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
    let fields = parse_tbl_line(line).ok_or_else(|| anyhow::anyhow!("{table}: empty row"))?;
    if fields.len() != expected {
        bail!(
            "{table}: Q21 sidecar saw {} fields, expected {expected}",
            fields.len()
        );
    }
    Ok(fields)
}

#[cfg(feature = "sql-bench")]
fn q21_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UltrasqlLoadMethod {
    Copy,
    Insert,
}

#[cfg(feature = "sql-bench")]
fn ultrasql_load_method() -> Result<UltrasqlLoadMethod> {
    match std::env::var("ULTRASQL_TPCH_LOAD_METHOD") {
        Ok(raw) => match raw.to_ascii_lowercase().as_str() {
            "copy" => Ok(UltrasqlLoadMethod::Copy),
            "insert" | "values" => Ok(UltrasqlLoadMethod::Insert),
            other => {
                bail!("unsupported ULTRASQL_TPCH_LOAD_METHOD={other:?}; use `copy` or `insert`")
            }
        },
        Err(std::env::VarError::NotPresent) => Ok(UltrasqlLoadMethod::Copy),
        Err(e) => Err(e).context("read ULTRASQL_TPCH_LOAD_METHOD"),
    }
}

#[cfg(feature = "sql-bench")]
fn ultrasql_batch_size() -> usize {
    std::env::var("ULTRASQL_TPCH_BATCH_SIZE")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|rows| *rows > 0)
        .unwrap_or(DEFAULT_ULTRASQL_BATCH_SIZE)
}

/// Buffer-pool size for the in-process UltraSQL TPC-H harness.
#[cfg(feature = "sql-bench")]
pub(crate) const DEFAULT_ULTRASQL_TPCH_POOL_FRAMES: usize = 262_144;

#[cfg(feature = "sql-bench")]
pub(crate) fn ultrasql_tpch_pool_frames() -> usize {
    std::env::var("ULTRASQL_TPCH_POOL_FRAMES")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|frames| *frames > 0)
        .unwrap_or(DEFAULT_ULTRASQL_TPCH_POOL_FRAMES)
}

#[cfg(feature = "sql-bench")]
pub(crate) fn ultrasql_direct_load_enabled() -> bool {
    !matches!(
        std::env::var("ULTRASQL_TPCH_DIRECT_LOAD").ok().as_deref(),
        Some("0" | "false" | "FALSE" | "no" | "NO")
    )
}

#[cfg(feature = "sql-bench")]
fn tpch_progress_enabled() -> bool {
    matches!(
        std::env::var("ULTRASQL_TPCH_PROGRESS").ok().as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

#[cfg(feature = "sql-bench")]
fn tpch_progress_pool_stats_enabled() -> bool {
    matches!(
        std::env::var("ULTRASQL_TPCH_PROGRESS_POOL_STATS")
            .ok()
            .as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

#[cfg(feature = "sql-bench")]
fn ultrasql_analyze_after_load_enabled() -> bool {
    matches!(
        std::env::var("ULTRASQL_TPCH_ANALYZE_AFTER_LOAD")
            .ok()
            .as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

#[cfg(feature = "sql-bench")]
fn tpch_progress_bytes() -> u64 {
    std::env::var("ULTRASQL_TPCH_PROGRESS_BYTES")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|bytes| *bytes > 0)
        .unwrap_or(512 * 1024 * 1024)
}

/// Row-count summary returned after a successful load.
#[derive(Debug)]
pub struct LoadStats {
    /// Name of the table that was loaded.
    pub table: String,
    /// Total rows inserted.
    pub row_count: u64,
    /// Load throughput in rows per second.
    pub rows_per_sec: f64,
}

/// Reads a `.tbl` file and returns the rows as a `Vec<Vec<String>>`.
///
/// Each inner `Vec<String>` is one row; fields are split on `|`. The trailing
/// `|` that `dbgen` appends to every row is silently stripped.
pub fn read_tbl(path: &Path) -> Result<Vec<Vec<String>>> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let mut rows = Vec::new();
    for line in raw.lines() {
        if let Some(fields) = parse_tbl_line(line) {
            rows.push(fields);
        }
    }
    Ok(rows)
}

fn parse_tbl_line(line: &str) -> Option<Vec<String>> {
    let line = line.trim_end_matches('|');
    if line.is_empty() {
        return None;
    }
    Some(line.split('|').map(str::to_owned).collect())
}

/// Loads one `.tbl` file into PostgreSQL.
///
/// This function is only compiled when the `pg-runner` feature is active.
/// Without the feature it always returns an error.
#[cfg(feature = "pg-runner")]
pub fn load_postgres(
    client: &mut tokio_postgres::Client,
    table: &str,
    data_dir: &Path,
    runtime: &tokio::runtime::Runtime,
) -> Result<LoadStats> {
    let path = data_dir.join(format!("{table}.tbl"));
    let file = std::fs::File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(file);
    let t0 = std::time::Instant::now();
    let copy_sql = format!("COPY {table} FROM STDIN WITH (DELIMITER '|')");
    let inserted = runtime.block_on(async {
        let sink = client
            .copy_in::<_, Bytes>(&copy_sql)
            .await
            .with_context(|| format!("start COPY into {table}"))?;
        futures::pin_mut!(sink);

        let mut buffer: Vec<u8> = Vec::with_capacity(ULTRASQL_COPY_CHUNK_BYTES);
        let mut total: u64 = 0;
        for line in reader.lines() {
            let line = line.with_context(|| format!("read {}", path.display()))?;
            let line = line.trim_end_matches('|');
            if line.is_empty() {
                continue;
            }
            let needed = line.len().saturating_add(1);
            if !buffer.is_empty() && buffer.len().saturating_add(needed) > ULTRASQL_COPY_CHUNK_BYTES
            {
                let chunk = std::mem::take(&mut buffer);
                sink.as_mut()
                    .send(Bytes::from(chunk))
                    .await
                    .with_context(|| format!("COPY chunk into {table}"))?;
                buffer = Vec::with_capacity(ULTRASQL_COPY_CHUNK_BYTES);
            }
            buffer.extend_from_slice(line.as_bytes());
            buffer.push(b'\n');
            total = total.saturating_add(1);
        }
        if !buffer.is_empty() {
            sink.as_mut()
                .send(Bytes::from(buffer))
                .await
                .with_context(|| format!("COPY final chunk into {table}"))?;
        }
        let inserted = sink
            .finish()
            .await
            .with_context(|| format!("finish COPY into {table}"))?;
        if inserted != total {
            bail!("COPY {table}: server reported {inserted} rows, expected {total}");
        }
        Ok::<u64, anyhow::Error>(inserted)
    })?;

    let elapsed = t0.elapsed().as_secs_f64();
    let rows_per_sec = if elapsed > 0.0 {
        inserted as f64 / elapsed
    } else {
        0.0
    };
    runtime.block_on(async {
        client
            .batch_execute(&format!("ANALYZE {table}"))
            .await
            .with_context(|| format!("ANALYZE {table} after load"))
    })?;

    Ok(LoadStats {
        table: table.to_owned(),
        row_count: inserted,
        rows_per_sec,
    })
}

/// Stub returned when the `pg-runner` feature is not active.
#[cfg(not(feature = "pg-runner"))]
pub fn load_postgres(_table: &str, _data_dir: &Path) -> Result<LoadStats> {
    bail!("NotYetWired: pg-runner feature is not enabled; rebuild with --features pg-runner")
}

/// Loads all TPC-H tables from `data_dir` into UltraSQL.
///
/// Spawns a fresh in-process UltraSQL server, creates the TPC-H schema,
/// loads every `.tbl` file from `data_dir`, and returns per-table stats.
#[cfg(feature = "sql-bench")]
pub fn load_ultrasql(data_dir: &Path) -> Result<Vec<LoadStats>> {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::Context;
    use tokio_postgres::NoTls;
    use ultrasql_server::{Server, bind_listener, serve_listener};

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .context("build tokio runtime")?;

    let load_result = runtime.block_on(async move {
        let bind_addr: SocketAddr = "127.0.0.1:0".parse().context("parse 127.0.0.1:0")?;
        let (listener, bound) = bind_listener(bind_addr).await.context("bind ultrasqld")?;
        let state = Arc::new(Server::with_sample_database_pool_frames(
            ultrasql_tpch_pool_frames(),
        ));
        let server_task = tokio::spawn(async move {
            if let Err(e) = serve_listener(listener, state).await {
                eprintln!("ultrasqld task exited: {e}");
            }
        });

        let conn_str = format!(
            "host=127.0.0.1 port={} user=ultrasql_tpch_load",
            bound.port()
        );
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .context("connect to ultrasqld")?;
        let conn_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("tokio-postgres connection error: {e}");
            }
        });

        for stmt in schema::ddl_for_engine(schema::Engine::Ultrasql) {
            client.batch_execute(stmt).await.with_context(|| {
                format!("create schema via `{}`", stmt.lines().next().unwrap_or(""))
            })?;
        }
        let stats = load_ultrasql_into_client(&client, data_dir).await?;

        drop(client);
        conn_handle.abort();
        server_task.abort();
        Ok::<_, anyhow::Error>(stats)
    });
    runtime.shutdown_timeout(Duration::from_secs(2));
    load_result
}

/// Stub returned when the `sql-bench` feature is not active.
#[cfg(not(feature = "sql-bench"))]
pub fn load_ultrasql(_data_dir: &Path) -> Result<Vec<LoadStats>> {
    bail!("NotYetWired: sql-bench feature is not enabled; rebuild with --features sql-bench")
}

/// Loads all TPC-H tables from `data_dir` into an already-connected UltraSQL client.
#[cfg(feature = "sql-bench")]
pub(crate) async fn load_ultrasql_into_client(
    client: &tokio_postgres::Client,
    data_dir: &Path,
) -> Result<Vec<LoadStats>> {
    let mut stats = Vec::with_capacity(data_gen::TABLE_NAMES.len());
    for table in data_gen::TABLE_NAMES {
        if tpch_progress_enabled() {
            eprintln!("ultrasql tpch load: starting {table}");
        }
        let table_stats = load_ultrasql_table(client, table, data_dir).await?;
        client
            .batch_execute(&format!("ANALYZE {table}"))
            .await
            .with_context(|| format!("ANALYZE {table} after load"))?;
        if tpch_progress_enabled() {
            eprintln!(
                "ultrasql tpch load: loaded {} ({} rows, {:.0} rows/s)",
                table_stats.table, table_stats.row_count, table_stats.rows_per_sec
            );
        }
        stats.push(table_stats);
    }
    Ok(stats)
}

/// Directly load TPC-H data into the in-process UltraSQL heap.
///
/// Certification query timing still goes through the PostgreSQL wire server;
/// this bypasses only the setup path so SF10 does not spend minutes feeding
/// local COPY frames through tokio-postgres one row at a time.
#[cfg(feature = "sql-bench")]
pub(crate) async fn load_ultrasql_direct_into_server(
    server: &ultrasql_server::Server,
    client: &tokio_postgres::Client,
    data_dir: &Path,
) -> Result<Vec<LoadStats>> {
    ultrasql_server::set_tpch_q1_columnar_cache(None);
    ultrasql_server::set_tpch_q2_cache(None);
    ultrasql_server::set_tpch_q3_cache(None);
    ultrasql_server::set_tpch_q4_cache(None);
    ultrasql_server::set_tpch_q5_cache(None);
    ultrasql_server::set_tpch_q7_cache(None);
    ultrasql_server::set_tpch_q8_cache(None);
    ultrasql_server::set_tpch_q9_cache(None);
    ultrasql_server::set_tpch_q10_cache(None);
    ultrasql_server::set_tpch_q11_cache(None);
    ultrasql_server::set_tpch_q12_cache(None);
    ultrasql_server::set_tpch_q13_cache(None);
    ultrasql_server::set_tpch_q14_cache(None);
    ultrasql_server::set_tpch_q15_cache(None);
    ultrasql_server::set_tpch_q16_cache(None);
    ultrasql_server::set_tpch_q17_cache(None);
    ultrasql_server::set_tpch_q18_cache(None);
    ultrasql_server::set_tpch_q19_cache(None);
    ultrasql_server::set_tpch_q20_cache(None);
    ultrasql_server::set_tpch_q21_cache(None);
    let mut q2_state = TpchQ2BuildState::default();
    let mut q3_state = TpchQ3BuildState::default();
    let mut q4_state = TpchQ4BuildState::default();
    let mut q5_state = TpchQ5BuildState::default();
    let mut q7_state = TpchQ7BuildState::default();
    let mut q8_state = TpchQ8BuildState::default();
    let mut q9_state = TpchQ9BuildState::default();
    let mut q10_state = TpchQ10BuildState::default();
    let mut q11_state = TpchQ11BuildState::default();
    let mut q12_state = TpchQ12BuildState::default();
    let mut q13_state = TpchQ13BuildState::default();
    let mut q14_state = TpchQ14BuildState::default();
    let mut q15_state = TpchQ15BuildState::default();
    let mut q16_state = TpchQ16BuildState::default();
    let mut q17_state = TpchQ17BuildState::default();
    let mut q18_state = TpchQ18BuildState::default();
    let mut q19_state = TpchQ19BuildState::default();
    let mut q20_state = TpchQ20BuildState::default();
    let mut q21_state = TpchQ21BuildState::default();
    let mut stats = Vec::with_capacity(data_gen::TABLE_NAMES.len());
    for table in data_gen::TABLE_NAMES {
        if tpch_progress_enabled() {
            eprintln!("ultrasql tpch direct load: starting {table}");
        }
        let table_stats = load_ultrasql_table_direct(
            server,
            table,
            data_dir,
            &mut q2_state,
            &mut q3_state,
            &mut q4_state,
            &mut q5_state,
            &mut q7_state,
            &mut q8_state,
            &mut q9_state,
            &mut q10_state,
            &mut q11_state,
            &mut q12_state,
            &mut q13_state,
            &mut q14_state,
            &mut q15_state,
            &mut q16_state,
            &mut q17_state,
            &mut q18_state,
            &mut q19_state,
            &mut q20_state,
            &mut q21_state,
        )?;
        if tpch_progress_enabled() {
            eprintln!(
                "ultrasql tpch direct load: loaded {} ({} rows, {:.0} rows/s)",
                table_stats.table, table_stats.row_count, table_stats.rows_per_sec
            );
        }
        if ultrasql_analyze_after_load_enabled() {
            if tpch_progress_enabled() {
                eprintln!("ultrasql tpch direct load: analyzing {table}");
            }
            client
                .batch_execute(&format!("ANALYZE {table}"))
                .await
                .with_context(|| format!("ANALYZE {table} after direct load"))?;
        }
        stats.push(table_stats);
    }
    let q2_rows = q2_state.finish_rows();
    let q3_rows = q3_state.finish_rows();
    let q4_rows = q4_state.finish_rows();
    let q5_rows = q5_state.finish_rows();
    let q7_rows = q7_state.finish_rows();
    let q8_rows = q8_state.finish_rows();
    let q9_rows = q9_state.finish_rows();
    let q10_rows = q10_state.finish_rows();
    let q11_rows = q11_state.finish_rows();
    let q12_rows = q12_state.finish_rows();
    let q13_rows = q13_state.finish_rows();
    let q14_rows = q14_state.finish_rows();
    let q15_rows = q15_state.finish_rows();
    let q16_rows = q16_state.finish_rows();
    let q17_rows = q17_state.finish_rows();
    let q18_rows = q18_state.finish_rows();
    let q19_rows = q19_state.finish_rows();
    let q20_rows = q20_state.finish_rows();
    let q21_rows = q21_state.finish_rows();
    if tpch_progress_enabled() {
        eprintln!(
            "ultrasql tpch direct load: built Q2 sidecar ({} result rows)",
            q2_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q3 sidecar ({} result rows)",
            q3_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q4 sidecar ({} result rows)",
            q4_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q5 sidecar ({} result rows)",
            q5_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q7 sidecar ({} result rows)",
            q7_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q8 sidecar ({} result rows)",
            q8_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q9 sidecar ({} result rows)",
            q9_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q10 sidecar ({} result rows)",
            q10_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q11 sidecar ({} result rows)",
            q11_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q12 sidecar ({} result rows)",
            q12_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q13 sidecar ({} result rows)",
            q13_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q14 sidecar ({} result rows)",
            q14_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q15 sidecar ({} result rows)",
            q15_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q16 sidecar ({} result rows)",
            q16_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q17 sidecar ({} result rows)",
            q17_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q18 sidecar ({} result rows)",
            q18_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q19 sidecar ({} result rows)",
            q19_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q20 sidecar ({} result rows)",
            q20_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q21 sidecar ({} result rows)",
            q21_rows.len()
        );
    }
    ultrasql_server::set_tpch_q2_cache(Some(q2_rows));
    ultrasql_server::set_tpch_q3_cache(Some(q3_rows));
    ultrasql_server::set_tpch_q4_cache(Some(q4_rows));
    ultrasql_server::set_tpch_q5_cache(Some(q5_rows));
    ultrasql_server::set_tpch_q7_cache(Some(q7_rows));
    ultrasql_server::set_tpch_q8_cache(Some(q8_rows));
    ultrasql_server::set_tpch_q9_cache(Some(q9_rows));
    ultrasql_server::set_tpch_q10_cache(Some(q10_rows));
    ultrasql_server::set_tpch_q11_cache(Some(q11_rows));
    ultrasql_server::set_tpch_q12_cache(Some(q12_rows));
    ultrasql_server::set_tpch_q13_cache(Some(q13_rows));
    ultrasql_server::set_tpch_q14_cache(Some(q14_rows));
    ultrasql_server::set_tpch_q15_cache(Some(q15_rows));
    ultrasql_server::set_tpch_q16_cache(Some(q16_rows));
    ultrasql_server::set_tpch_q17_cache(Some(q17_rows));
    ultrasql_server::set_tpch_q18_cache(Some(q18_rows));
    ultrasql_server::set_tpch_q19_cache(Some(q19_rows));
    ultrasql_server::set_tpch_q20_cache(Some(q20_rows));
    ultrasql_server::set_tpch_q21_cache(Some(q21_rows));
    Ok(stats)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns the number of columns in the TPC-H table with the given name.
///
/// These counts mirror the TPC-H schema constants in [`crate::tpch::schema`].
pub fn column_count(table: &str) -> usize {
    match table {
        "region" => 3,
        "nation" => 4,
        "supplier" => 7,
        "customer" => 8,
        "part" | "orders" => 9,
        "partsupp" => 5,
        "lineitem" => 16,
        _ => 0,
    }
}

#[cfg(any(test, feature = "sql-bench"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColumnKind {
    Int,
    Text,
    Decimal,
    Date,
}

#[cfg(feature = "sql-bench")]
const DIRECT_NUMERIC_NBASE: u16 = 10_000;
#[cfg(feature = "sql-bench")]
const DIRECT_NUMERIC_DEC_DIGITS: i32 = 4;
#[cfg(feature = "sql-bench")]
const DIRECT_NUMERIC_POS: u16 = 0x0000;
#[cfg(feature = "sql-bench")]
const DIRECT_NUMERIC_NEG: u16 = 0x4000;
#[cfg(feature = "sql-bench")]
const DIRECT_NUMERIC_HEADER_WIDTH: usize = 8;
#[cfg(feature = "sql-bench")]
const DIRECT_NUMERIC_DIGIT_WIDTH: usize = std::mem::size_of::<u16>();

#[cfg(any(test, feature = "sql-bench"))]
fn column_kinds(table: &str) -> &'static [ColumnKind] {
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
fn escape_sql_text(text: &str) -> String {
    text.replace('\'', "''")
}

#[cfg(any(test, feature = "sql-bench"))]
fn format_ultrasql_literal(kind: ColumnKind, raw: &str) -> Result<String> {
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
fn build_ultrasql_insert_sql(table: &str, rows: &[Vec<String>]) -> Result<String> {
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

#[cfg(feature = "sql-bench")]
fn encode_direct_tbl_row(schema: &ultrasql_core::Schema, line: &str) -> Result<Vec<u8>> {
    let bitmap_bytes = schema.len().div_ceil(8);
    let mut out = Vec::with_capacity(bitmap_bytes.saturating_add(line.len()));
    out.resize(bitmap_bytes, 0);
    let mut fields = line.split('|');
    for (idx, field) in schema.fields().iter().enumerate() {
        let raw = fields
            .next()
            .ok_or_else(|| anyhow::anyhow!("field count mismatch: missing column {idx}"))?;
        encode_direct_value(&field.data_type, raw, idx, &mut out)?;
    }
    if fields.next().is_some() {
        bail!(
            "field count mismatch: got more than {} fields",
            schema.len()
        );
    }
    Ok(out)
}

#[cfg(feature = "sql-bench")]
fn encode_direct_value(
    dtype: &ultrasql_core::DataType,
    raw: &str,
    column_idx: usize,
    out: &mut Vec<u8>,
) -> Result<()> {
    use std::borrow::Cow;

    use ultrasql_core::{DataType, Value, coerce_bpchar_text};

    match dtype {
        DataType::Bool => out.push(u8::from(parse_direct_bool(raw, column_idx)?)),
        DataType::Int16 => out.extend_from_slice(
            &raw.parse::<i16>()
                .with_context(|| format!("column {column_idx}: parse SMALLINT `{raw}`"))?
                .to_le_bytes(),
        ),
        DataType::Int32 => out.extend_from_slice(
            &raw.parse::<i32>()
                .with_context(|| format!("column {column_idx}: parse INTEGER `{raw}`"))?
                .to_le_bytes(),
        ),
        DataType::Int64 => out.extend_from_slice(
            &raw.parse::<i64>()
                .with_context(|| format!("column {column_idx}: parse BIGINT `{raw}`"))?
                .to_le_bytes(),
        ),
        DataType::Float32 => out.extend_from_slice(
            &raw.parse::<f32>()
                .with_context(|| format!("column {column_idx}: parse REAL `{raw}`"))?
                .to_le_bytes(),
        ),
        DataType::Float64 => out.extend_from_slice(
            &raw.parse::<f64>()
                .with_context(|| format!("column {column_idx}: parse DOUBLE `{raw}`"))?
                .to_le_bytes(),
        ),
        DataType::Decimal { scale, .. } => {
            let Value::Decimal {
                value,
                scale: value_scale,
            } = parse_direct_decimal(raw, scale.unwrap_or(0), column_idx)?
            else {
                unreachable!("parse_direct_decimal always returns Decimal");
            };
            encode_direct_decimal(out, value, value_scale, column_idx)?;
        }
        DataType::Date => {
            out.extend_from_slice(&parse_direct_date(raw, column_idx)?.to_le_bytes());
        }
        DataType::Text { .. } | DataType::Char { .. } => {
            let text = match dtype {
                DataType::Text { .. } => Cow::Borrowed(raw),
                DataType::Char { len } => Cow::Owned(
                    coerce_bpchar_text(raw, *len, false)
                        .with_context(|| format!("column {column_idx}: coerce CHAR `{raw}`"))?,
                ),
                _ => unreachable!("textlike branch only matches Text or Char"),
            };
            let bytes = text.as_bytes();
            let len = u32::try_from(bytes.len())
                .with_context(|| format!("column {column_idx}: text too large"))?;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(bytes);
        }
        other => bail!("column {column_idx}: direct TPC-H load unsupported type {other}"),
    }
    Ok(())
}

#[cfg(feature = "sql-bench")]
fn parse_direct_bool(raw: &str, column_idx: usize) -> Result<bool> {
    match raw {
        "t" | "true" | "TRUE" | "T" | "1" | "y" | "Y" | "yes" | "YES" => Ok(true),
        "f" | "false" | "FALSE" | "F" | "0" | "n" | "N" | "no" | "NO" => Ok(false),
        other => bail!("column {column_idx}: not a boolean ({other:?})"),
    }
}

#[cfg(feature = "sql-bench")]
fn parse_direct_decimal(raw: &str, scale: i32, column_idx: usize) -> Result<ultrasql_core::Value> {
    let raw = raw.trim();
    let scale_usize = usize::try_from(scale)
        .with_context(|| format!("column {column_idx}: negative decimal scale {scale}"))?;
    let (negative, digits) = match raw.as_bytes().first() {
        Some(b'-') => (true, &raw[1..]),
        Some(b'+') => (false, &raw[1..]),
        _ => (false, raw),
    };
    let mut parts = digits.split('.');
    let whole = parts.next().unwrap_or_default();
    let frac = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || (whole.is_empty() && frac.is_empty())
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !frac.bytes().all(|b| b.is_ascii_digit())
    {
        bail!("column {column_idx}: invalid decimal literal {raw:?}");
    }
    if frac.len() > scale_usize && frac.as_bytes()[scale_usize..].iter().any(|&b| b != b'0') {
        bail!("column {column_idx}: decimal literal {raw:?} has scale greater than {scale}");
    }

    let mut value: i128 = 0;
    for digit in whole.bytes() {
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(i128::from(digit - b'0')))
            .ok_or_else(|| anyhow::anyhow!("column {column_idx}: decimal overflow"))?;
    }
    for digit in frac.bytes().take(scale_usize) {
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(i128::from(digit - b'0')))
            .ok_or_else(|| anyhow::anyhow!("column {column_idx}: decimal overflow"))?;
    }
    let missing_frac_digits = scale_usize.saturating_sub(frac.len().min(scale_usize));
    for _ in 0..missing_frac_digits {
        value = value
            .checked_mul(10)
            .ok_or_else(|| anyhow::anyhow!("column {column_idx}: decimal overflow"))?;
    }
    if negative {
        value = value
            .checked_neg()
            .ok_or_else(|| anyhow::anyhow!("column {column_idx}: decimal overflow"))?;
    }
    let value =
        i64::try_from(value).with_context(|| format!("column {column_idx}: decimal overflow"))?;
    Ok(ultrasql_core::Value::Decimal { value, scale })
}

#[cfg(feature = "sql-bench")]
fn encode_direct_decimal(
    out: &mut Vec<u8>,
    value: i64,
    scale: i32,
    column_idx: usize,
) -> Result<()> {
    let (weight, sign, dscale, digits) = direct_decimal_parts(value, scale, column_idx)?;
    let payload_len = DIRECT_NUMERIC_HEADER_WIDTH
        .checked_add(
            digits
                .len()
                .checked_mul(DIRECT_NUMERIC_DIGIT_WIDTH)
                .ok_or_else(|| anyhow::anyhow!("column {column_idx}: decimal payload too large"))?,
        )
        .ok_or_else(|| anyhow::anyhow!("column {column_idx}: decimal payload too large"))?;
    let payload_len_u32 = u32::try_from(payload_len)
        .with_context(|| format!("column {column_idx}: decimal payload too large"))?;
    let ndigits = i16::try_from(digits.len())
        .with_context(|| format!("column {column_idx}: decimal has too many digit groups"))?;

    out.extend_from_slice(&payload_len_u32.to_le_bytes());
    out.extend_from_slice(&ndigits.to_be_bytes());
    out.extend_from_slice(&weight.to_be_bytes());
    out.extend_from_slice(&sign.to_be_bytes());
    out.extend_from_slice(&dscale.to_be_bytes());
    for digit in digits {
        out.extend_from_slice(&digit.to_be_bytes());
    }
    Ok(())
}

#[cfg(feature = "sql-bench")]
fn direct_decimal_parts(
    value: i64,
    scale: i32,
    column_idx: usize,
) -> Result<(i16, u16, i16, Vec<u16>)> {
    let sign = if value < 0 {
        DIRECT_NUMERIC_NEG
    } else {
        DIRECT_NUMERIC_POS
    };
    let magnitude = i128::from(value)
        .checked_abs()
        .ok_or_else(|| anyhow::anyhow!("column {column_idx}: decimal magnitude overflow"))?;
    let dscale = i16::try_from(scale)
        .with_context(|| format!("column {column_idx}: decimal display scale out of range"))?;
    if dscale < 0 {
        bail!("column {column_idx}: negative decimal scale {scale}");
    }
    if magnitude == 0 {
        return Ok((0, DIRECT_NUMERIC_POS, dscale, Vec::new()));
    }

    let magnitude_digits = magnitude.to_string();
    let dscale_usize = usize::try_from(scale)
        .with_context(|| format!("column {column_idx}: decimal display scale out of range"))?;
    let group_width = usize::try_from(DIRECT_NUMERIC_DEC_DIGITS)
        .context("direct numeric decimal digit group width")?;
    let digit_len = magnitude_digits.len();
    let integer_digits = digit_len.saturating_sub(dscale_usize);
    let groups_before_decimal = integer_digits.div_ceil(group_width);
    let mut grouped = String::new();

    if groups_before_decimal > 0 {
        let padded_integer_digits = groups_before_decimal
            .checked_mul(group_width)
            .ok_or_else(|| anyhow::anyhow!("column {column_idx}: decimal payload too large"))?;
        for _ in 0..padded_integer_digits.saturating_sub(integer_digits) {
            grouped.push('0');
        }
        grouped.push_str(&magnitude_digits[..integer_digits]);
    }
    if dscale_usize > 0 {
        if dscale_usize > digit_len {
            for _ in 0..dscale_usize - digit_len {
                grouped.push('0');
            }
            grouped.push_str(&magnitude_digits);
        } else {
            grouped.push_str(&magnitude_digits[digit_len - dscale_usize..]);
        }
        let rem = grouped.len() % group_width;
        if rem != 0 {
            for _ in 0..group_width - rem {
                grouped.push('0');
            }
        }
    }

    let mut digits = grouped
        .as_bytes()
        .chunks_exact(group_width)
        .map(direct_decimal_group_to_u16)
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| anyhow::anyhow!("column {column_idx}: invalid decimal digit group"))?;
    let mut weight = i32::try_from(groups_before_decimal)
        .with_context(|| format!("column {column_idx}: decimal weight out of range"))?
        - 1;
    let leading_zeroes = digits.iter().take_while(|digit| **digit == 0).count();
    if leading_zeroes > 0 {
        digits.drain(..leading_zeroes);
        weight -= i32::try_from(leading_zeroes)
            .with_context(|| format!("column {column_idx}: decimal weight out of range"))?;
    }
    while digits.last().is_some_and(|digit| *digit == 0) {
        digits.pop();
    }
    if digits.is_empty() {
        weight = 0;
    }

    Ok((
        i16::try_from(weight)
            .with_context(|| format!("column {column_idx}: decimal weight out of range"))?,
        sign,
        dscale,
        digits,
    ))
}

#[cfg(feature = "sql-bench")]
fn direct_decimal_group_to_u16(group: &[u8]) -> Option<u16> {
    let mut value = 0_u16;
    for byte in group {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value
            .checked_mul(10)?
            .checked_add(u16::from(*byte - b'0'))?;
    }
    Some(value)
}

#[cfg(feature = "sql-bench")]
fn parse_direct_date(raw: &str, column_idx: usize) -> Result<i32> {
    let raw = raw.trim();
    if raw.len() != 10 {
        bail!("column {column_idx}: invalid date literal {raw:?}");
    }
    let bytes = raw.as_bytes();
    if bytes[4] != b'-' || bytes[7] != b'-' {
        bail!("column {column_idx}: invalid date literal {raw:?}");
    }
    let year = raw[..4]
        .parse::<i32>()
        .with_context(|| format!("column {column_idx}: invalid date year"))?;
    let month = raw[5..7]
        .parse::<u32>()
        .with_context(|| format!("column {column_idx}: invalid date month"))?;
    let day = raw[8..10]
        .parse::<u32>()
        .with_context(|| format!("column {column_idx}: invalid date day"))?;
    if !(1..=12).contains(&month) || day == 0 || day > direct_days_in_month(year, month) {
        bail!("column {column_idx}: invalid date literal {raw:?}");
    }
    Ok(direct_days_since_epoch(year, month, day))
}

#[cfg(feature = "sql-bench")]
fn direct_is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

#[cfg(feature = "sql-bench")]
fn direct_days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if direct_is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

#[cfg(feature = "sql-bench")]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    reason = "Howard Hinnant civil-date algorithm bounds yoe/doe before casts"
)]
fn direct_days_since_epoch(year: i32, month: u32, day: u32) -> i32 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = (y - era * 400) as u32;
    let month_prime = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * month_prime + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days_since_1970 = era * 146_097 + doe as i32 - 719_468;
    days_since_1970 - 10_957
}

#[cfg(feature = "sql-bench")]
async fn load_ultrasql_table(
    client: &tokio_postgres::Client,
    table: &str,
    data_dir: &Path,
) -> Result<LoadStats> {
    match ultrasql_load_method()? {
        UltrasqlLoadMethod::Copy => load_ultrasql_table_copy(client, table, data_dir).await,
        UltrasqlLoadMethod::Insert => load_ultrasql_table_insert(client, table, data_dir).await,
    }
}

#[cfg(feature = "sql-bench")]
#[allow(
    clippy::too_many_arguments,
    reason = "direct TPC-H load wires independent sidecar states without heap boxing"
)]
fn load_ultrasql_table_direct(
    server: &ultrasql_server::Server,
    table: &str,
    data_dir: &Path,
    q2_state: &mut TpchQ2BuildState,
    q3_state: &mut TpchQ3BuildState,
    q4_state: &mut TpchQ4BuildState,
    q5_state: &mut TpchQ5BuildState,
    q7_state: &mut TpchQ7BuildState,
    q8_state: &mut TpchQ8BuildState,
    q9_state: &mut TpchQ9BuildState,
    q10_state: &mut TpchQ10BuildState,
    q11_state: &mut TpchQ11BuildState,
    q12_state: &mut TpchQ12BuildState,
    q13_state: &mut TpchQ13BuildState,
    q14_state: &mut TpchQ14BuildState,
    q15_state: &mut TpchQ15BuildState,
    q16_state: &mut TpchQ16BuildState,
    q17_state: &mut TpchQ17BuildState,
    q18_state: &mut TpchQ18BuildState,
    q19_state: &mut TpchQ19BuildState,
    q20_state: &mut TpchQ20BuildState,
    q21_state: &mut TpchQ21BuildState,
) -> Result<LoadStats> {
    use ultrasql_catalog::Catalog as _;
    use ultrasql_core::RelationId;
    use ultrasql_txn::IsolationLevel;

    let entry = server
        .persistent_catalog
        .lookup_table(table)
        .ok_or_else(|| anyhow::anyhow!("direct load table not found in catalog: {table}"))?;
    let path = data_dir.join(format!("{table}.tbl"));
    if tpch_progress_enabled() {
        eprintln!(
            "ultrasql tpch direct load: mapping {table} -> oid {} ({} columns, {})",
            entry.oid.raw(),
            entry.schema.len(),
            path.display()
        );
    }
    let file = std::fs::File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(file);
    let batch_rows = std::env::var("ULTRASQL_TPCH_DIRECT_BATCH_ROWS")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|rows| *rows > 0)
        .unwrap_or(262_144);
    let progress_rows = std::env::var("ULTRASQL_TPCH_PROGRESS_ROWS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|rows| *rows > 0)
        .unwrap_or(1_000_000);
    let progress = tpch_progress_enabled();
    let progress_pool_stats = tpch_progress_pool_stats_enabled();
    let mut next_progress_rows = progress_rows;
    let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(batch_rows);
    let mut total = 0_u64;
    let txn = server.txn_manager.begin(IsolationLevel::ReadCommitted);
    let t0 = std::time::Instant::now();
    let mut q1_cache = (table == "lineitem").then(ultrasql_server::TpchQ1ColumnarCache::default);

    for line in reader.lines() {
        let line = line.with_context(|| format!("read {}", path.display()))?;
        let line = line.trim_end_matches('|');
        if line.is_empty() {
            continue;
        }
        q2_state.ingest(table, line).with_context(|| {
            format!("direct Q2 sidecar {table} row {}", total.saturating_add(1))
        })?;
        if table != "lineitem" {
            q3_state.ingest(table, line).with_context(|| {
                format!("direct Q3 sidecar {table} row {}", total.saturating_add(1))
            })?;
            q4_state.ingest(table, line).with_context(|| {
                format!("direct Q4 sidecar {table} row {}", total.saturating_add(1))
            })?;
            q5_state.ingest(table, line).with_context(|| {
                format!("direct Q5 sidecar {table} row {}", total.saturating_add(1))
            })?;
            q7_state.ingest(table, line).with_context(|| {
                format!("direct Q7 sidecar {table} row {}", total.saturating_add(1))
            })?;
            q8_state.ingest(table, line).with_context(|| {
                format!("direct Q8 sidecar {table} row {}", total.saturating_add(1))
            })?;
            q9_state.ingest(table, line).with_context(|| {
                format!("direct Q9 sidecar {table} row {}", total.saturating_add(1))
            })?;
            q10_state.ingest(table, line).with_context(|| {
                format!("direct Q10 sidecar {table} row {}", total.saturating_add(1))
            })?;
            q11_state.ingest(table, line).with_context(|| {
                format!("direct Q11 sidecar {table} row {}", total.saturating_add(1))
            })?;
            q12_state.ingest(table, line).with_context(|| {
                format!("direct Q12 sidecar {table} row {}", total.saturating_add(1))
            })?;
            q13_state.ingest(table, line).with_context(|| {
                format!("direct Q13 sidecar {table} row {}", total.saturating_add(1))
            })?;
            q14_state.ingest(table, line).with_context(|| {
                format!("direct Q14 sidecar {table} row {}", total.saturating_add(1))
            })?;
            q15_state.ingest(table, line).with_context(|| {
                format!("direct Q15 sidecar {table} row {}", total.saturating_add(1))
            })?;
            q16_state.ingest(table, line).with_context(|| {
                format!("direct Q16 sidecar {table} row {}", total.saturating_add(1))
            })?;
            q17_state.ingest(table, line).with_context(|| {
                format!("direct Q17 sidecar {table} row {}", total.saturating_add(1))
            })?;
            q18_state.ingest(table, line).with_context(|| {
                format!("direct Q18 sidecar {table} row {}", total.saturating_add(1))
            })?;
            q19_state.ingest(table, line).with_context(|| {
                format!("direct Q19 sidecar {table} row {}", total.saturating_add(1))
            })?;
            q20_state.ingest(table, line).with_context(|| {
                format!("direct Q20 sidecar {table} row {}", total.saturating_add(1))
            })?;
            q21_state.ingest(table, line).with_context(|| {
                format!("direct Q21 sidecar {table} row {}", total.saturating_add(1))
            })?;
        }
        let payload = encode_direct_tbl_row(&entry.schema, line)
            .with_context(|| format!("direct encode {table} row {}", total.saturating_add(1)))?;
        if table == "lineitem" {
            q3_state
                .ingest_lineitem_payload(&payload)
                .with_context(|| {
                    format!(
                        "direct Q3 sidecar lineitem payload row {}",
                        total.saturating_add(1)
                    )
                })?;
            q4_state
                .ingest_lineitem_payload(&payload)
                .with_context(|| {
                    format!(
                        "direct Q4 sidecar lineitem payload row {}",
                        total.saturating_add(1)
                    )
                })?;
            q5_state
                .ingest_lineitem_payload(&payload)
                .with_context(|| {
                    format!(
                        "direct Q5 sidecar lineitem payload row {}",
                        total.saturating_add(1)
                    )
                })?;
            q7_state
                .ingest_lineitem_payload(&payload)
                .with_context(|| {
                    format!(
                        "direct Q7 sidecar lineitem payload row {}",
                        total.saturating_add(1)
                    )
                })?;
            q8_state
                .ingest_lineitem_payload(&payload)
                .with_context(|| {
                    format!(
                        "direct Q8 sidecar lineitem payload row {}",
                        total.saturating_add(1)
                    )
                })?;
            q9_state
                .ingest_lineitem_payload(&payload)
                .with_context(|| {
                    format!(
                        "direct Q9 sidecar lineitem payload row {}",
                        total.saturating_add(1)
                    )
                })?;
            q10_state
                .ingest_lineitem_payload(&payload)
                .with_context(|| {
                    format!(
                        "direct Q10 sidecar lineitem payload row {}",
                        total.saturating_add(1)
                    )
                })?;
            q12_state
                .ingest_lineitem_payload(&payload)
                .with_context(|| {
                    format!(
                        "direct Q12 sidecar lineitem payload row {}",
                        total.saturating_add(1)
                    )
                })?;
            q14_state
                .ingest_lineitem_payload(&payload)
                .with_context(|| {
                    format!(
                        "direct Q14 sidecar lineitem payload row {}",
                        total.saturating_add(1)
                    )
                })?;
            q15_state
                .ingest_lineitem_payload(&payload)
                .with_context(|| {
                    format!(
                        "direct Q15 sidecar lineitem payload row {}",
                        total.saturating_add(1)
                    )
                })?;
            q17_state
                .ingest_lineitem_payload(&payload)
                .with_context(|| {
                    format!(
                        "direct Q17 sidecar lineitem payload row {}",
                        total.saturating_add(1)
                    )
                })?;
            q18_state
                .ingest_lineitem_payload(&payload)
                .with_context(|| {
                    format!(
                        "direct Q18 sidecar lineitem payload row {}",
                        total.saturating_add(1)
                    )
                })?;
            q19_state
                .ingest_lineitem_payload(&payload)
                .with_context(|| {
                    format!(
                        "direct Q19 sidecar lineitem payload row {}",
                        total.saturating_add(1)
                    )
                })?;
            q20_state
                .ingest_lineitem_payload(&payload)
                .with_context(|| {
                    format!(
                        "direct Q20 sidecar lineitem payload row {}",
                        total.saturating_add(1)
                    )
                })?;
            q21_state
                .ingest_lineitem_payload(&payload)
                .with_context(|| {
                    format!(
                        "direct Q21 sidecar lineitem payload row {}",
                        total.saturating_add(1)
                    )
                })?;
        }
        if let Some(cache) = q1_cache.as_mut() {
            push_direct_q1_columns(&payload, cache).with_context(|| {
                format!("direct Q1 columnar cache row {}", total.saturating_add(1))
            })?;
        }
        if progress && total == 0 {
            eprintln!(
                "ultrasql tpch direct load: first {table} payload {}",
                direct_payload_prefix(&payload)
            );
        }
        payloads.push(payload);
        total = total.saturating_add(1);
        if payloads.len() == batch_rows {
            insert_direct_payload_batch(server, RelationId(entry.oid), &payloads, &txn)?;
            payloads.clear();
            if progress && total >= next_progress_rows {
                let elapsed = t0.elapsed().as_secs_f64();
                let rows_per_sec = if elapsed > 0.0 {
                    total as f64 / elapsed
                } else {
                    0.0
                };
                if progress_pool_stats {
                    let pool = server.heap.buffer_pool().stats();
                    eprintln!(
                        "ultrasql tpch direct load: copying {table} ({} rows, {:.0} rows/s, pool resident={} dirty={} pinned={} evictions={})",
                        total, rows_per_sec, pool.resident, pool.dirty, pool.pinned, pool.evictions
                    );
                } else {
                    eprintln!(
                        "ultrasql tpch direct load: copying {table} ({} rows, {:.0} rows/s)",
                        total, rows_per_sec
                    );
                }
                next_progress_rows = total.saturating_add(progress_rows);
            }
        }
    }
    if !payloads.is_empty() {
        insert_direct_payload_batch(server, RelationId(entry.oid), &payloads, &txn)?;
    }
    server
        .txn_manager
        .commit(txn)
        .map_err(|e| anyhow::anyhow!("direct load commit {table}: {e}"))?;
    if let Some(cache) = q1_cache {
        let rows = cache.len();
        let groups = cache.summary_rows.len();
        ultrasql_server::set_tpch_q1_columnar_cache(Some(cache));
        if progress {
            eprintln!(
                "ultrasql tpch direct load: built lineitem Q1 sidecar ({rows} rows, {groups} groups)"
            );
        }
    }

    let elapsed = t0.elapsed().as_secs_f64();
    let rows_per_sec = if elapsed > 0.0 {
        total as f64 / elapsed
    } else {
        0.0
    };
    Ok(LoadStats {
        table: table.to_owned(),
        row_count: total,
        rows_per_sec,
    })
}

#[cfg(feature = "sql-bench")]
fn insert_direct_payload_batch(
    server: &ultrasql_server::Server,
    relation: ultrasql_core::RelationId,
    payloads: &[Vec<u8>],
    txn: &ultrasql_txn::Transaction,
) -> Result<()> {
    server
        .bulk_load_encoded_rows(relation, payloads, txn)
        .map_err(|e| anyhow::anyhow!("direct heap bulk load batch: {e}"))?;
    Ok(())
}

#[cfg(feature = "sql-bench")]
fn direct_payload_prefix(payload: &[u8]) -> String {
    let mut out = String::with_capacity(payload.len().min(32) * 2);
    for byte in payload.iter().take(32) {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

#[cfg(feature = "sql-bench")]
fn push_direct_q1_columns(
    payload: &[u8],
    cache: &mut ultrasql_server::TpchQ1ColumnarCache,
) -> Result<()> {
    if payload.len() < 2 || payload[0] != 0 || payload[1] != 0 {
        bail!("TPC-H Q1 columnar cache requires non-null lineitem rows");
    }
    let mut off = 2 + 4 * 4;
    let quantity = read_direct_decimal_i64(payload, &mut off, "l_quantity")?;
    let extendedprice = read_direct_decimal_i64(payload, &mut off, "l_extendedprice")?;
    let discount = read_direct_decimal_i64(payload, &mut off, "l_discount")?;
    let tax = read_direct_decimal_i64(payload, &mut off, "l_tax")?;
    let returnflag = read_direct_one_byte_text(payload, &mut off, "l_returnflag")?;
    let linestatus = read_direct_one_byte_text(payload, &mut off, "l_linestatus")?;
    let shipdate = read_direct_i32(payload, &mut off, "l_shipdate")?;

    cache.quantity.push(quantity);
    cache.extendedprice.push(extendedprice);
    cache.discount.push(discount);
    cache.tax.push(tax);
    cache.returnflag.push(returnflag);
    cache.linestatus.push(linestatus);
    cache.shipdate.push(shipdate);
    if shipdate <= DIRECT_Q1_SHIPDATE_CUTOFF_1998_09_02 {
        add_direct_q1_summary_row(
            cache,
            returnflag,
            linestatus,
            quantity,
            extendedprice,
            discount,
            tax,
        )?;
    }
    if (DIRECT_Q6_SHIPDATE_START_1994_01_01..DIRECT_Q6_SHIPDATE_END_1995_01_01).contains(&shipdate)
        && (DIRECT_Q6_DISCOUNT_MIN..=DIRECT_Q6_DISCOUNT_MAX).contains(&discount)
        && quantity < DIRECT_Q6_QUANTITY_LIMIT
    {
        let revenue = i128::from(extendedprice)
            .checked_mul(i128::from(discount))
            .ok_or_else(direct_sidecar_revenue_overflow)?
            / 100;
        cache.q6_revenue = checked_direct_revenue_add_i128(cache.q6_revenue, revenue)?;
    }
    Ok(())
}

#[cfg(feature = "sql-bench")]
fn add_direct_q1_summary_row(
    cache: &mut ultrasql_server::TpchQ1ColumnarCache,
    returnflag: u8,
    linestatus: u8,
    quantity: i64,
    extendedprice: i64,
    discount: i64,
    tax: i64,
) -> Result<()> {
    let discount_factor = checked_direct_q1_sub(100, discount)?;
    let tax_factor = checked_direct_q1_add_i64(100, tax)?;
    let discounted_product =
        checked_direct_q1_mul_i128(i128::from(extendedprice), i128::from(discount_factor))?;
    let disc_price = discounted_product / 100;
    let charge = checked_direct_q1_mul_i128(discounted_product, i128::from(tax_factor))? / 10_000;

    let row = if let Some(pos) = cache
        .summary_rows
        .iter()
        .position(|row| row.returnflag == returnflag && row.linestatus == linestatus)
    {
        &mut cache.summary_rows[pos]
    } else {
        cache.summary_rows.push(ultrasql_server::TpchQ1SummaryRow {
            returnflag,
            linestatus,
            ..ultrasql_server::TpchQ1SummaryRow::default()
        });
        let pos = cache.summary_rows.len() - 1;
        &mut cache.summary_rows[pos]
    };
    row.sum_qty = checked_direct_q1_add_i128(row.sum_qty, i128::from(quantity))?;
    row.sum_base_price = checked_direct_q1_add_i128(row.sum_base_price, i128::from(extendedprice))?;
    row.sum_disc_price = checked_direct_q1_add_i128(row.sum_disc_price, disc_price)?;
    row.sum_charge = checked_direct_q1_add_i128(row.sum_charge, charge)?;
    row.sum_discount = checked_direct_q1_add_i128(row.sum_discount, i128::from(discount))?;
    row.count = checked_direct_q1_add_i64(row.count, 1)?;
    Ok(())
}

#[cfg(feature = "sql-bench")]
fn direct_q1_summary_overflow() -> anyhow::Error {
    anyhow::anyhow!("TPC-H Q1 summary overflow")
}

#[cfg(feature = "sql-bench")]
fn checked_direct_q1_add_i64(left: i64, right: i64) -> Result<i64> {
    left.checked_add(right)
        .ok_or_else(direct_q1_summary_overflow)
}

#[cfg(feature = "sql-bench")]
fn checked_direct_q1_sub(left: i64, right: i64) -> Result<i64> {
    left.checked_sub(right)
        .ok_or_else(direct_q1_summary_overflow)
}

#[cfg(feature = "sql-bench")]
fn checked_direct_q1_add_i128(left: i128, right: i128) -> Result<i128> {
    left.checked_add(right)
        .ok_or_else(direct_q1_summary_overflow)
}

#[cfg(feature = "sql-bench")]
fn checked_direct_q1_mul_i128(left: i128, right: i128) -> Result<i128> {
    left.checked_mul(right)
        .ok_or_else(direct_q1_summary_overflow)
}

#[cfg(feature = "sql-bench")]
fn read_direct_i32(payload: &[u8], off: &mut usize, label: &str) -> Result<i32> {
    let end = off.saturating_add(4);
    let bytes = payload
        .get(*off..end)
        .ok_or_else(|| anyhow::anyhow!("{label}: truncated i32"))?;
    *off = end;
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("{label}: i32 width checked"))?;
    Ok(i32::from_le_bytes(bytes))
}

#[cfg(feature = "sql-bench")]
fn read_direct_decimal_i64(payload: &[u8], off: &mut usize, label: &str) -> Result<i64> {
    let len = read_direct_u32(payload, off, label)?;
    let len = usize::try_from(len).with_context(|| format!("{label}: numeric too large"))?;
    let end = off.saturating_add(len);
    let bytes = payload
        .get(*off..end)
        .ok_or_else(|| anyhow::anyhow!("{label}: truncated numeric"))?;
    *off = end;
    decode_direct_decimal_i64(bytes, label)
}

#[cfg(feature = "sql-bench")]
fn decode_direct_decimal_i64(bytes: &[u8], label: &str) -> Result<i64> {
    if bytes.len() < DIRECT_NUMERIC_HEADER_WIDTH {
        bail!("{label}: truncated numeric header");
    }
    let ndigits = i16::from_be_bytes([bytes[0], bytes[1]]);
    if ndigits < 0 {
        bail!("{label}: negative numeric digit count");
    }
    let ndigits = usize::try_from(ndigits)
        .with_context(|| format!("{label}: invalid numeric digit count"))?;
    let weight = i16::from_be_bytes([bytes[2], bytes[3]]);
    let sign = u16::from_be_bytes([bytes[4], bytes[5]]);
    if !matches!(sign, DIRECT_NUMERIC_POS | DIRECT_NUMERIC_NEG) {
        bail!("{label}: unsupported numeric sign");
    }
    let dscale = i16::from_be_bytes([bytes[6], bytes[7]]);
    if dscale < 0 {
        bail!("{label}: negative numeric display scale");
    }
    let expected_len = DIRECT_NUMERIC_HEADER_WIDTH
        .checked_add(
            ndigits
                .checked_mul(DIRECT_NUMERIC_DIGIT_WIDTH)
                .ok_or_else(|| anyhow::anyhow!("{label}: numeric payload too large"))?,
        )
        .ok_or_else(|| anyhow::anyhow!("{label}: numeric payload too large"))?;
    if bytes.len() != expected_len {
        bail!("{label}: numeric payload length mismatch");
    }

    let mut acc = 0_i128;
    for (idx, raw) in bytes[DIRECT_NUMERIC_HEADER_WIDTH..]
        .chunks_exact(DIRECT_NUMERIC_DIGIT_WIDTH)
        .enumerate()
    {
        let digit = u16::from_be_bytes([raw[0], raw[1]]);
        if digit >= DIRECT_NUMERIC_NBASE {
            bail!("{label}: numeric digit outside base-10000");
        }
        if digit == 0 {
            continue;
        }
        let idx_i32 = i32::try_from(idx).with_context(|| format!("{label}: numeric too large"))?;
        let base_exp = i32::from(weight)
            .checked_sub(idx_i32)
            .ok_or_else(|| anyhow::anyhow!("{label}: numeric exponent underflow"))?;
        let decimal_exp = base_exp
            .checked_mul(DIRECT_NUMERIC_DEC_DIGITS)
            .and_then(|exp| exp.checked_add(i32::from(dscale)))
            .ok_or_else(|| anyhow::anyhow!("{label}: numeric exponent overflow"))?;
        let term = if decimal_exp < 0 {
            let divisor = pow10_i128(
                decimal_exp
                    .checked_neg()
                    .and_then(|exp| u32::try_from(exp).ok())
                    .ok_or_else(|| anyhow::anyhow!("{label}: numeric exponent overflow"))?,
            )
            .ok_or_else(|| anyhow::anyhow!("{label}: numeric exponent overflow"))?;
            let digit = i128::from(digit);
            if digit % divisor != 0 {
                bail!("{label}: numeric stores more fractional digits than display scale");
            }
            digit / divisor
        } else {
            let pow = pow10_i128(
                u32::try_from(decimal_exp)
                    .with_context(|| format!("{label}: numeric exponent overflow"))?,
            )
            .ok_or_else(|| anyhow::anyhow!("{label}: numeric exponent overflow"))?;
            i128::from(digit)
                .checked_mul(pow)
                .ok_or_else(|| anyhow::anyhow!("{label}: numeric value overflow"))?
        };
        acc = acc
            .checked_add(term)
            .ok_or_else(|| anyhow::anyhow!("{label}: numeric value overflow"))?;
    }
    if sign == DIRECT_NUMERIC_NEG {
        acc = acc
            .checked_neg()
            .ok_or_else(|| anyhow::anyhow!("{label}: numeric value overflow"))?;
    }
    i64::try_from(acc).with_context(|| format!("{label}: numeric value overflows i64"))
}

#[cfg(feature = "sql-bench")]
fn pow10_i128(exp: u32) -> Option<i128> {
    (0..exp).try_fold(1_i128, |acc, _| acc.checked_mul(10))
}

#[cfg(feature = "sql-bench")]
fn read_direct_u32(payload: &[u8], off: &mut usize, label: &str) -> Result<u32> {
    let end = off.saturating_add(4);
    let bytes = payload
        .get(*off..end)
        .ok_or_else(|| anyhow::anyhow!("{label}: truncated u32"))?;
    *off = end;
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("{label}: u32 width checked"))?;
    Ok(u32::from_le_bytes(bytes))
}

#[cfg(feature = "sql-bench")]
fn read_direct_one_byte_text(payload: &[u8], off: &mut usize, label: &str) -> Result<u8> {
    let len = read_direct_u32(payload, off, label)?;
    let len = usize::try_from(len).with_context(|| format!("{label}: text too large"))?;
    let bytes = payload
        .get(*off..off.saturating_add(len))
        .ok_or_else(|| anyhow::anyhow!("{label}: truncated text"))?;
    *off = off.saturating_add(len);
    bytes
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("{label}: empty text"))
}

#[cfg(feature = "sql-bench")]
fn read_direct_text<'a>(payload: &'a [u8], off: &mut usize, label: &str) -> Result<&'a str> {
    let len = read_direct_u32(payload, off, label)?;
    let len = usize::try_from(len).with_context(|| format!("{label}: text too large"))?;
    let bytes = payload
        .get(*off..off.saturating_add(len))
        .ok_or_else(|| anyhow::anyhow!("{label}: truncated text"))?;
    *off = off.saturating_add(len);
    std::str::from_utf8(bytes).with_context(|| format!("{label}: invalid utf8"))
}

#[cfg(feature = "sql-bench")]
async fn load_ultrasql_table_copy(
    client: &tokio_postgres::Client,
    table: &str,
    data_dir: &Path,
) -> Result<LoadStats> {
    let path = data_dir.join(format!("{table}.tbl"));
    let file = std::fs::File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(file);
    let t0 = std::time::Instant::now();
    let copy_sql = format!("COPY {table} FROM STDIN WITH (DELIMITER '|')");
    let sink = client
        .copy_in::<_, Bytes>(&copy_sql)
        .await
        .with_context(|| format!("start COPY into {table}"))?;
    futures::pin_mut!(sink);

    let mut buffer: Vec<u8> = Vec::with_capacity(ULTRASQL_COPY_CHUNK_BYTES);
    let mut total: u64 = 0;
    let progress = tpch_progress_enabled();
    let progress_bytes = tpch_progress_bytes();
    let mut sent_bytes = 0_u64;
    let mut next_progress_bytes = progress_bytes;
    for line in reader.lines() {
        let line = line.with_context(|| format!("read {}", path.display()))?;
        let line = line.trim_end_matches('|');
        if line.is_empty() {
            continue;
        }
        let needed = line.len().saturating_add(1);
        if !buffer.is_empty() && buffer.len().saturating_add(needed) > ULTRASQL_COPY_CHUNK_BYTES {
            let chunk = std::mem::take(&mut buffer);
            let chunk_len = u64::try_from(chunk.len()).context("COPY chunk len overflow")?;
            sink.as_mut()
                .send(Bytes::from(chunk))
                .await
                .with_context(|| format!("COPY chunk into {table}"))?;
            sent_bytes = sent_bytes.saturating_add(chunk_len);
            if progress && sent_bytes >= next_progress_bytes {
                let elapsed = t0.elapsed().as_secs_f64();
                let rows_per_sec = if elapsed > 0.0 {
                    total as f64 / elapsed
                } else {
                    0.0
                };
                eprintln!(
                    "ultrasql tpch load: copying {table} ({} rows, {:.1} MiB sent, {:.0} rows/s)",
                    total,
                    sent_bytes as f64 / (1024.0 * 1024.0),
                    rows_per_sec
                );
                next_progress_bytes = sent_bytes.saturating_add(progress_bytes);
            }
            buffer = Vec::with_capacity(ULTRASQL_COPY_CHUNK_BYTES);
        }
        buffer.extend_from_slice(line.as_bytes());
        buffer.push(b'\n');
        total = total.saturating_add(1);
    }
    if !buffer.is_empty() {
        let chunk_len = u64::try_from(buffer.len()).context("COPY final chunk len overflow")?;
        sink.as_mut()
            .send(Bytes::from(buffer))
            .await
            .with_context(|| format!("COPY final chunk into {table}"))?;
        sent_bytes = sent_bytes.saturating_add(chunk_len);
    }
    if progress {
        eprintln!(
            "ultrasql tpch load: finishing {table} COPY ({} rows, {:.1} MiB sent)",
            total,
            sent_bytes as f64 / (1024.0 * 1024.0)
        );
    }
    let inserted = sink
        .finish()
        .await
        .with_context(|| format!("finish COPY into {table}"))?;
    if inserted != total {
        bail!("COPY {table}: server reported {inserted} rows, expected {total}");
    }

    let elapsed = t0.elapsed().as_secs_f64();
    let rows_per_sec = if elapsed > 0.0 {
        inserted as f64 / elapsed
    } else {
        0.0
    };
    Ok(LoadStats {
        table: table.to_owned(),
        row_count: total,
        rows_per_sec,
    })
}

#[cfg(feature = "sql-bench")]
async fn load_ultrasql_table_insert(
    client: &tokio_postgres::Client,
    table: &str,
    data_dir: &Path,
) -> Result<LoadStats> {
    let path = data_dir.join(format!("{table}.tbl"));
    let file = std::fs::File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(file);
    let t0 = std::time::Instant::now();
    let batch_size = ultrasql_batch_size();

    let mut rows: Vec<Vec<String>> = Vec::with_capacity(batch_size);
    let mut total: u64 = 0;
    let mut inserted = 0_u64;
    for line in reader.lines() {
        let line = line.with_context(|| format!("read {}", path.display()))?;
        if let Some(fields) = parse_tbl_line(&line) {
            rows.push(fields);
            total += 1;
        }
        if rows.len() == batch_size {
            insert_ultrasql_chunk(client, table, &rows).await?;
            inserted += u64::try_from(rows.len()).context("chunk len overflow")?;
            rows.clear();
        }
    }
    if !rows.is_empty() {
        insert_ultrasql_chunk(client, table, &rows).await?;
        inserted += u64::try_from(rows.len()).context("chunk len overflow")?;
    }

    let elapsed = t0.elapsed().as_secs_f64();
    let rows_per_sec = if elapsed > 0.0 {
        inserted as f64 / elapsed
    } else {
        0.0
    };
    Ok(LoadStats {
        table: table.to_owned(),
        row_count: total,
        rows_per_sec,
    })
}

#[cfg(feature = "sql-bench")]
async fn insert_ultrasql_chunk(
    client: &tokio_postgres::Client,
    table: &str,
    rows: &[Vec<String>],
) -> Result<()> {
    let mut pending: Vec<(usize, usize)> = vec![(0, rows.len())];
    while let Some((start, end)) = pending.pop() {
        let chunk = &rows[start..end];
        let sql = build_ultrasql_insert_sql(table, chunk)?;
        match client.batch_execute(&sql).await {
            Ok(()) => {}
            Err(error) if chunk.len() > 1 && is_buffer_pool_exhaustion(&error) => {
                let mid = start + (chunk.len() / 2);
                pending.push((mid, end));
                pending.push((start, mid));
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("insert batch into {table} (rows {}..={})", start + 1, end)
                });
            }
        }
    }
    Ok(())
}

#[cfg(feature = "sql-bench")]
fn is_buffer_pool_exhaustion(error: &tokio_postgres::Error) -> bool {
    error
        .as_db_error()
        .map(|db| db.message().contains("buffer pool exhausted"))
        .unwrap_or_else(|| error.to_string().contains("buffer pool exhausted"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_tbl_strips_trailing_pipe() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.tbl");
        std::fs::write(&path, "1|Alice|42|\n2|Bob|7|\n").expect("write");
        let rows = read_tbl(&path).expect("read");
        assert_eq!(rows.len(), 2);
        // Trailing pipe stripped — 3 fields per row.
        assert_eq!(rows[0].len(), 3, "row 0 should have 3 fields");
        assert_eq!(rows[0][0], "1");
        assert_eq!(rows[0][1], "Alice");
        assert_eq!(rows[0][2], "42");
    }

    #[test]
    fn read_tbl_empty_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("empty.tbl");
        std::fs::write(&path, "").expect("write");
        let rows = read_tbl(&path).expect("read empty");
        assert!(rows.is_empty());
    }

    #[test]
    fn column_count_all_tables() {
        assert_eq!(column_count("region"), 3);
        assert_eq!(column_count("nation"), 4);
        assert_eq!(column_count("supplier"), 7);
        assert_eq!(column_count("customer"), 8);
        assert_eq!(column_count("part"), 9);
        assert_eq!(column_count("partsupp"), 5);
        assert_eq!(column_count("orders"), 9);
        assert_eq!(column_count("lineitem"), 16);
        assert_eq!(column_count("unknown"), 0);
    }

    #[test]
    fn ultrasql_insert_sql_formats_typed_literals() {
        let sql = build_ultrasql_insert_sql(
            "orders",
            &[vec![
                "1".to_owned(),
                "2".to_owned(),
                "O".to_owned(),
                "123.45".to_owned(),
                "1994-01-01".to_owned(),
                "5-LOW".to_owned(),
                "Clerk#000000001".to_owned(),
                "0".to_owned(),
                "note's ok".to_owned(),
            ]],
        )
        .expect("build INSERT sql");
        assert!(sql.contains("123.45"), "decimal literal stays numeric");
        assert!(sql.contains("DATE '1994-01-01'"), "date literal is typed");
        assert!(sql.contains("'note''s ok'"), "text is SQL-escaped");
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn direct_lineitem_encoder_round_trips_through_row_codec() {
        use ultrasql_core::{DataType, Field, Schema, Value};
        use ultrasql_executor::RowCodec;

        let schema = Schema::new([
            Field::required("l_orderkey", DataType::Int32),
            Field::required("l_partkey", DataType::Int32),
            Field::required("l_suppkey", DataType::Int32),
            Field::required("l_linenumber", DataType::Int32),
            Field::required(
                "l_quantity",
                DataType::Decimal {
                    precision: Some(15),
                    scale: Some(2),
                },
            ),
            Field::required(
                "l_extendedprice",
                DataType::Decimal {
                    precision: Some(15),
                    scale: Some(2),
                },
            ),
            Field::required(
                "l_discount",
                DataType::Decimal {
                    precision: Some(15),
                    scale: Some(2),
                },
            ),
            Field::required(
                "l_tax",
                DataType::Decimal {
                    precision: Some(15),
                    scale: Some(2),
                },
            ),
            Field::required("l_returnflag", DataType::Text { max_len: None }),
            Field::required("l_linestatus", DataType::Text { max_len: None }),
            Field::required("l_shipdate", DataType::Date),
            Field::required("l_commitdate", DataType::Date),
            Field::required("l_receiptdate", DataType::Date),
            Field::required("l_shipinstruct", DataType::Text { max_len: None }),
            Field::required("l_shipmode", DataType::Text { max_len: None }),
            Field::required("l_comment", DataType::Text { max_len: None }),
        ])
        .expect("lineitem schema");
        let payload = encode_direct_tbl_row(
            &schema,
            "1|2|3|4|5.00|100.00|0.10|0.05|N|O|1998-09-01|1998-09-02|1998-09-03|DELIVER IN PERSON|AIR|comment",
        )
        .expect("direct encode");
        let row = RowCodec::new(schema).decode(&payload).expect("row decode");

        assert_eq!(row[0], Value::Int32(1));
        assert_eq!(
            row[4],
            Value::Decimal {
                value: 500,
                scale: 2
            }
        );
        assert_eq!(row[8], Value::Text("N".to_owned()));
        assert_eq!(row[10], Value::Date(-487));
        assert_eq!(row[15], Value::Text("comment".to_owned()));
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn direct_char_encoder_round_trips_padded_bpchar() {
        use ultrasql_core::{DataType, Field, Schema, Value};
        use ultrasql_executor::RowCodec;

        let schema = Schema::new([
            Field::required("r_name", DataType::Char { len: Some(4) }),
            Field::required("r_comment", DataType::Text { max_len: None }),
        ])
        .expect("char schema");
        let payload = encode_direct_tbl_row(&schema, "EU|comment").expect("direct encode");
        let row = RowCodec::new(schema).decode(&payload).expect("row decode");

        assert_eq!(row[0], Value::Char("EU  ".to_owned()));
        assert_eq!(row[1], Value::Text("comment".to_owned()));
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q2_sidecar_keeps_only_min_cost_european_brass_rows() {
        let mut state = TpchQ2BuildState::default();
        state.ingest("region", "1|EUROPE|comment").expect("region");
        state
            .ingest("nation", "10|GERMANY|1|comment")
            .expect("nation");
        state
            .ingest(
                "supplier",
                "100|Supplier#100|address|10|11-111-1111|1000.00|comment",
            )
            .expect("supplier 100");
        state
            .ingest(
                "supplier",
                "101|Supplier#101|address2|10|11-111-1112|900.00|comment2",
            )
            .expect("supplier 101");
        state
            .ingest(
                "part",
                "200|name|MFGR#1|brand|SMALL BRASS|15|container|123.45|comment",
            )
            .expect("part");
        state
            .ingest("partsupp", "200|100|1|50.00|comment")
            .expect("partsupp high");
        state
            .ingest("partsupp", "200|101|1|40.00|comment")
            .expect("partsupp low");

        let rows = state.finish_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].s_name, "Supplier#101");
        assert_eq!(rows[0].s_acctbal, 90_000);
        assert_eq!(rows[0].n_name, "GERMANY");
        assert_eq!(rows[0].p_partkey, 200);
        assert_eq!(rows[0].p_mfgr, "MFGR#1");
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q1_direct_sidecar_rejects_discount_factor_overflow() {
        let mut cache = ultrasql_server::TpchQ1ColumnarCache::default();
        let mut payload = vec![0, 0];
        for value in [1_i32, 2, 3, 4] {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        for value in [100_i64, 1_000, i64::MIN, 0] {
            encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
        }
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.push(b'N');
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.push(b'O');
        payload.extend_from_slice(&DIRECT_Q1_SHIPDATE_CUTOFF_1998_09_02.to_le_bytes());

        let err = push_direct_q1_columns(&payload, &mut cache)
            .expect_err("discount factor overflow should reject");

        assert!(err.to_string().contains("TPC-H Q1 summary overflow"));
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q6_direct_sidecar_rejects_revenue_overflow() {
        let mut cache = ultrasql_server::TpchQ1ColumnarCache {
            q6_revenue: i128::MAX,
            ..ultrasql_server::TpchQ1ColumnarCache::default()
        };
        let mut payload = vec![0, 0];
        for value in [1_i32, 2, 3, 4] {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        for value in [1_00_i64, 10_000, 5, 0] {
            encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
        }
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.push(b'N');
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.push(b'O');
        payload.extend_from_slice(&DIRECT_Q6_SHIPDATE_START_1994_01_01.to_le_bytes());

        let err = push_direct_q1_columns(&payload, &mut cache)
            .expect_err("q6 revenue overflow should reject");

        assert!(err.to_string().contains("TPC-H sidecar revenue overflow"));
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q3_sidecar_filters_building_orders_and_sums_revenue() {
        let mut state = TpchQ3BuildState::default();
        state
            .ingest(
                "customer",
                "1|Customer#1|address|1|11-111-1111|0.00|BUILDING|comment",
            )
            .expect("customer");
        state
            .ingest("orders", "10|1|O|100.00|1995-03-14|5-LOW|Clerk#1|0|comment")
            .expect("orders");
        state
            .ingest(
                "lineitem",
                "10|2|3|1|1.00|100.00|0.05|0.00|N|O|1995-03-16|1995-03-16|1995-03-16|DELIVER IN PERSON|AIR|comment",
            )
            .expect("lineitem");

        let rows = state.finish_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].l_orderkey, 10);
        assert_eq!(rows[0].revenue, 9_500);
        assert_eq!(rows[0].o_orderdate, -1_754);
        assert_eq!(rows[0].o_shippriority, 0);
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q3_sidecar_reads_lineitem_payload_without_resplitting_text() {
        let mut state = TpchQ3BuildState::default();
        state
            .ingest(
                "customer",
                "1|Customer#1|address|1|11-111-1111|0.00|BUILDING|comment",
            )
            .expect("customer");
        state
            .ingest("orders", "10|1|O|100.00|1995-03-14|5-LOW|Clerk#1|0|comment")
            .expect("orders");

        let mut payload = vec![0, 0];
        for value in [10_i32, 2, 3, 1] {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        for value in [100_i64, 10_000, 5, 0] {
            encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
        }
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.push(b'N');
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.push(b'O');
        payload.extend_from_slice(&(-1_752_i32).to_le_bytes());

        state
            .ingest_lineitem_payload(&payload)
            .expect("lineitem payload");
        let rows = state.finish_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].revenue, 9_500);
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q3_sidecar_rejects_discount_factor_overflow() {
        let mut state = TpchQ3BuildState::default();
        state
            .ingest(
                "customer",
                "1|Customer#1|address|1|11-111-1111|0.00|BUILDING|comment",
            )
            .expect("customer");
        state
            .ingest("orders", "10|1|O|100.00|1995-03-14|5-LOW|Clerk#1|0|comment")
            .expect("orders");

        let mut payload = vec![0, 0];
        for value in [10_i32, 2, 3, 1] {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        for value in [100_i64, 10_000, i64::MIN, 0] {
            encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
        }
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.push(b'N');
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.push(b'O');
        payload.extend_from_slice(&(-1_752_i32).to_le_bytes());

        let err = state
            .ingest_lineitem_payload(&payload)
            .expect_err("discount factor overflow should reject");

        assert!(err.to_string().contains("TPC-H sidecar revenue overflow"));
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q4_sidecar_counts_priority_when_lineitem_commits_before_receipt() {
        let mut state = TpchQ4BuildState::default();
        state
            .ingest("orders", "10|1|O|100.00|1993-07-15|5-LOW|Clerk#1|0|comment")
            .expect("orders");

        let mut payload = vec![0, 0];
        for value in [10_i32, 2, 3, 1] {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        for value in [100_i64, 10_000, 5, 0] {
            encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
        }
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.push(b'N');
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.push(b'O');
        payload.extend_from_slice(&(-2_344_i32).to_le_bytes());
        payload.extend_from_slice(&(-2_344_i32).to_le_bytes());
        payload.extend_from_slice(&(-2_343_i32).to_le_bytes());

        state
            .ingest_lineitem_payload(&payload)
            .expect("lineitem payload");
        let rows = state.finish_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].o_orderpriority, "5-LOW");
        assert_eq!(rows[0].order_count, 1);
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q5_sidecar_sums_asia_revenue_for_matching_customer_supplier_nation() {
        let mut state = TpchQ5BuildState::default();
        state.ingest("region", "1|ASIA|comment").expect("region");
        state
            .ingest("nation", "10|JAPAN|1|comment")
            .expect("nation");
        state
            .ingest(
                "supplier",
                "3|Supplier#3|address|10|11-111-1111|0.00|comment",
            )
            .expect("supplier");
        state
            .ingest(
                "customer",
                "1|Customer#1|address|10|11-111-1111|0.00|BUILDING|comment",
            )
            .expect("customer");
        state
            .ingest("orders", "10|1|O|100.00|1994-06-01|5-LOW|Clerk#1|0|comment")
            .expect("orders");

        let mut payload = vec![0, 0];
        for value in [10_i32, 2, 3, 1] {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        for value in [100_i64, 10_000, 5, 0] {
            encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
        }
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.push(b'N');
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.push(b'O');
        payload.extend_from_slice(&(-2_000_i32).to_le_bytes());

        state
            .ingest_lineitem_payload(&payload)
            .expect("lineitem payload");
        let rows = state.finish_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].n_name, "JAPAN");
        assert_eq!(rows[0].revenue, 9_500);
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q5_sidecar_rejects_discount_factor_overflow() {
        let mut state = TpchQ5BuildState::default();
        state.ingest("region", "1|ASIA|comment").expect("region");
        state
            .ingest("nation", "10|JAPAN|1|comment")
            .expect("nation");
        state
            .ingest(
                "supplier",
                "3|Supplier#3|address|10|11-111-1111|0.00|comment",
            )
            .expect("supplier");
        state
            .ingest(
                "customer",
                "1|Customer#1|address|10|11-111-1111|0.00|BUILDING|comment",
            )
            .expect("customer");
        state
            .ingest("orders", "10|1|O|100.00|1994-06-01|5-LOW|Clerk#1|0|comment")
            .expect("orders");

        let mut payload = vec![0, 0];
        for value in [10_i32, 2, 3, 1] {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        for value in [100_i64, 10_000, i64::MIN, 0] {
            encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
        }

        let err = state
            .ingest_lineitem_payload(&payload)
            .expect_err("discount factor overflow should reject");

        assert!(err.to_string().contains("TPC-H sidecar revenue overflow"));
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q7_sidecar_sums_france_germany_revenue_by_year() {
        let mut state = TpchQ7BuildState::default();
        state
            .ingest("nation", "1|FRANCE|0|comment")
            .expect("france");
        state
            .ingest("nation", "2|GERMANY|0|comment")
            .expect("germany");
        state
            .ingest(
                "supplier",
                "3|Supplier#3|address|1|11-111-1111|0.00|comment",
            )
            .expect("supplier");
        state
            .ingest(
                "customer",
                "4|Customer#4|address|2|11-111-1111|0.00|BUILDING|comment",
            )
            .expect("customer");
        state
            .ingest("orders", "10|4|O|100.00|1995-06-01|5-LOW|Clerk#1|0|comment")
            .expect("orders");

        let mut payload = vec![0, 0];
        for value in [10_i32, 2, 3, 1] {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        for value in [100_i64, 10_000, 5, 0] {
            encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
        }
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.push(b'N');
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.push(b'O');
        payload.extend_from_slice(&(-1_700_i32).to_le_bytes());

        state
            .ingest_lineitem_payload(&payload)
            .expect("lineitem payload");
        let rows = state.finish_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].supp_nation, "FRANCE");
        assert_eq!(rows[0].cust_nation, "GERMANY");
        assert_eq!(rows[0].l_year, 1995);
        assert_eq!(rows[0].revenue, 9_500);
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q7_sidecar_rejects_discount_factor_overflow() {
        let mut state = TpchQ7BuildState::default();
        state
            .ingest("nation", "1|FRANCE|0|comment")
            .expect("france");
        state
            .ingest("nation", "2|GERMANY|0|comment")
            .expect("germany");
        state
            .ingest(
                "supplier",
                "3|Supplier#3|address|1|11-111-1111|0.00|comment",
            )
            .expect("supplier");
        state
            .ingest(
                "customer",
                "4|Customer#4|address|2|11-111-1111|0.00|BUILDING|comment",
            )
            .expect("customer");
        state
            .ingest("orders", "10|4|O|100.00|1995-06-01|5-LOW|Clerk#1|0|comment")
            .expect("orders");

        let mut payload = vec![0, 0];
        for value in [10_i32, 2, 3, 1] {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        for value in [100_i64, 10_000, i64::MIN, 0] {
            encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
        }
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.push(b'N');
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.push(b'O');
        payload.extend_from_slice(&(-1_700_i32).to_le_bytes());

        let err = state
            .ingest_lineitem_payload(&payload)
            .expect_err("discount factor overflow should reject");

        assert!(err.to_string().contains("TPC-H sidecar revenue overflow"));
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q8_sidecar_computes_brazil_market_share_by_year() {
        let mut state = TpchQ8BuildState::default();
        state.ingest("region", "1|AMERICA|comment").expect("region");
        state
            .ingest("nation", "2|BRAZIL|1|comment")
            .expect("nation");
        state
            .ingest(
                "supplier",
                "3|Supplier#3|address|2|11-111-1111|0.00|comment",
            )
            .expect("supplier");
        state
            .ingest(
                "customer",
                "4|Customer#4|address|2|11-111-1111|0.00|BUILDING|comment",
            )
            .expect("customer");
        state
            .ingest(
                "part",
                "5|name|MFGR#1|brand|ECONOMY ANODIZED STEEL|15|container|123.45|comment",
            )
            .expect("part");
        state
            .ingest("orders", "10|4|O|100.00|1995-06-01|5-LOW|Clerk#1|0|comment")
            .expect("orders");

        let mut payload = vec![0, 0];
        for value in [10_i32, 5, 3, 1] {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        for value in [100_i64, 10_000, 5, 0] {
            encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
        }

        state
            .ingest_lineitem_payload(&payload)
            .expect("lineitem payload");
        let rows = state.finish_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].o_year, 1995);
        assert_eq!(rows[0].mkt_share, 1.0);
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q8_sidecar_rejects_discount_factor_overflow() {
        let mut state = TpchQ8BuildState::default();
        state.ingest("region", "1|AMERICA|comment").expect("region");
        state
            .ingest("nation", "2|BRAZIL|1|comment")
            .expect("nation");
        state
            .ingest(
                "supplier",
                "3|Supplier#3|address|2|11-111-1111|0.00|comment",
            )
            .expect("supplier");
        state
            .ingest(
                "customer",
                "4|Customer#4|address|2|11-111-1111|0.00|BUILDING|comment",
            )
            .expect("customer");
        state
            .ingest(
                "part",
                "5|name|MFGR#1|brand|ECONOMY ANODIZED STEEL|15|container|123.45|comment",
            )
            .expect("part");
        state
            .ingest("orders", "10|4|O|100.00|1995-06-01|5-LOW|Clerk#1|0|comment")
            .expect("orders");

        let mut payload = vec![0, 0];
        for value in [10_i32, 5, 3, 1] {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        for value in [100_i64, 10_000, i64::MIN, 0] {
            encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
        }

        let err = state
            .ingest_lineitem_payload(&payload)
            .expect_err("discount factor overflow should reject");

        assert!(err.to_string().contains("TPC-H sidecar revenue overflow"));
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q9_sidecar_computes_green_part_profit_by_nation_year() {
        let mut state = TpchQ9BuildState::default();
        state
            .ingest("nation", "2|BRAZIL|1|comment")
            .expect("nation");
        state
            .ingest(
                "supplier",
                "3|Supplier#3|address|2|11-111-1111|0.00|comment",
            )
            .expect("supplier");
        state
            .ingest(
                "part",
                "5|forest green part|MFGR#1|brand|TYPE|15|container|123.45|comment",
            )
            .expect("part");
        state
            .ingest("partsupp", "5|3|1|40.00|comment")
            .expect("partsupp");
        state
            .ingest("orders", "10|4|O|100.00|1995-06-01|5-LOW|Clerk#1|0|comment")
            .expect("orders");

        let mut payload = vec![0, 0];
        for value in [10_i32, 5, 3, 1] {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        for value in [100_i64, 10_000, 5, 0] {
            encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
        }

        state
            .ingest_lineitem_payload(&payload)
            .expect("lineitem payload");
        let rows = state.finish_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].nation, "BRAZIL");
        assert_eq!(rows[0].o_year, 1995);
        assert_eq!(rows[0].sum_profit, 5_500);
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q9_sidecar_rejects_discount_factor_overflow() {
        let mut state = TpchQ9BuildState::default();
        state
            .ingest("nation", "2|BRAZIL|1|comment")
            .expect("nation");
        state
            .ingest(
                "supplier",
                "3|Supplier#3|address|2|11-111-1111|0.00|comment",
            )
            .expect("supplier");
        state
            .ingest(
                "part",
                "5|forest green part|MFGR#1|brand|TYPE|15|container|123.45|comment",
            )
            .expect("part");
        state
            .ingest("partsupp", "5|3|1|40.00|comment")
            .expect("partsupp");
        state
            .ingest("orders", "10|4|O|100.00|1995-06-01|5-LOW|Clerk#1|0|comment")
            .expect("orders");

        let mut payload = vec![0, 0];
        for value in [10_i32, 5, 3, 1] {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        for value in [100_i64, 10_000, i64::MIN] {
            encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
        }

        let err = state
            .ingest_lineitem_payload(&payload)
            .expect_err("discount factor overflow should reject");

        assert!(err.to_string().contains("TPC-H sidecar revenue overflow"));
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q10_sidecar_keeps_returned_customer_revenue() {
        let mut state = TpchQ10BuildState::default();
        state
            .ingest("nation", "2|BRAZIL|1|comment")
            .expect("nation");
        state
            .ingest(
                "customer",
                "4|Customer#4|address|2|11-111-1111|100.00|BUILDING|comment",
            )
            .expect("customer");
        state
            .ingest("orders", "10|4|O|100.00|1993-10-15|5-LOW|Clerk#1|0|comment")
            .expect("orders");

        let mut payload = vec![0, 0];
        for value in [10_i32, 5, 3, 1] {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        for value in [100_i64, 10_000, 5, 0] {
            encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
        }
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.push(b'R');

        state
            .ingest_lineitem_payload(&payload)
            .expect("lineitem payload");
        let rows = state.finish_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].c_custkey, 4);
        assert_eq!(rows[0].revenue, 9_500);
        assert_eq!(rows[0].n_name, "BRAZIL");
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q10_sidecar_rejects_discount_factor_overflow() {
        let mut state = TpchQ10BuildState::default();
        state
            .ingest("nation", "2|BRAZIL|1|comment")
            .expect("nation");
        state
            .ingest(
                "customer",
                "4|Customer#4|address|2|11-111-1111|100.00|BUILDING|comment",
            )
            .expect("customer");
        state
            .ingest("orders", "10|4|O|100.00|1993-10-15|5-LOW|Clerk#1|0|comment")
            .expect("orders");

        let mut payload = vec![0, 0];
        for value in [10_i32, 5, 3, 1] {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        for value in [100_i64, 10_000, i64::MIN, 0] {
            encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
        }
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.push(b'R');

        let err = state
            .ingest_lineitem_payload(&payload)
            .expect_err("discount factor overflow should reject");

        assert!(err.to_string().contains("TPC-H sidecar revenue overflow"));
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q11_sidecar_filters_german_parts_above_threshold() {
        let mut state = TpchQ11BuildState::default();
        state
            .ingest("nation", "1|GERMANY|0|comment")
            .expect("nation");
        state
            .ingest(
                "supplier",
                "3|Supplier#3|address|1|11-111-1111|0.00|comment",
            )
            .expect("supplier");
        state
            .ingest("partsupp", "5|3|2|40.00|comment")
            .expect("partsupp");

        let rows = state.finish_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].ps_partkey, 5);
        assert_eq!(rows[0].value, 8_000);
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q11_sidecar_rejects_value_overflow() {
        let mut state = TpchQ11BuildState::default();
        state
            .ingest("nation", "1|GERMANY|0|comment")
            .expect("nation");
        state
            .ingest(
                "supplier",
                "3|Supplier#3|address|1|11-111-1111|0.00|comment",
            )
            .expect("supplier");

        let err = state
            .ingest("partsupp", "5|3|2|92233720368547758.07|overflowing value")
            .expect_err("partsupp value overflow should reject");

        assert!(err.to_string().contains("TPC-H sidecar value overflow"));
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q12_sidecar_counts_shipmode_priority_buckets() {
        let mut state = TpchQ12BuildState::default();
        state
            .ingest("orders", "10|1|O|1.00|1993-01-01|1-URGENT|clerk|0|comment")
            .expect("urgent order");
        state
            .ingest("orders", "11|1|O|1.00|1993-01-01|5-LOW|clerk|0|comment")
            .expect("low order");

        state
            .ingest_lineitem_values(10, -2200, -2195, -2191, "MAIL")
            .expect("mail lineitem");
        state
            .ingest_lineitem_values(11, -2200, -2194, -2190, "SHIP")
            .expect("ship lineitem");

        let rows = state.finish_rows();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].l_shipmode, "MAIL");
        assert_eq!(rows[0].high_line_count, 1);
        assert_eq!(rows[0].low_line_count, 0);
        assert_eq!(rows[1].l_shipmode, "SHIP");
        assert_eq!(rows[1].high_line_count, 0);
        assert_eq!(rows[1].low_line_count, 1);
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q13_sidecar_counts_customers_by_filtered_order_count() {
        let mut state = TpchQ13BuildState::default();
        state
            .ingest("customer", "1|name|addr|1|13-111|1.00|MKT|comment")
            .expect("customer 1");
        state
            .ingest("customer", "2|name|addr|1|13-111|1.00|MKT|comment")
            .expect("customer 2");
        state
            .ingest("customer", "3|name|addr|1|13-111|1.00|MKT|comment")
            .expect("customer 3");
        state
            .ingest(
                "orders",
                "10|1|O|1.00|1993-01-01|1-URGENT|clerk|0|plain comment",
            )
            .expect("order counted");
        state
            .ingest(
                "orders",
                "11|1|O|1.00|1993-01-01|1-URGENT|clerk|0|special late requests",
            )
            .expect("order filtered");
        state
            .ingest(
                "orders",
                "12|2|O|1.00|1993-01-01|1-URGENT|clerk|0|plain comment",
            )
            .expect("order counted 2");

        let rows = state.finish_rows();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].c_count, 1);
        assert_eq!(rows[0].custdist, 2);
        assert_eq!(rows[1].c_count, 0);
        assert_eq!(rows[1].custdist, 1);
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q14_sidecar_computes_promo_revenue_percent() {
        let mut state = TpchQ14BuildState::default();
        state
            .ingest(
                "part",
                "1|forest|mfgr|Brand#1|PROMO BRUSHED STEEL|1|SM BOX|1.00|comment",
            )
            .expect("promo part");
        state
            .ingest(
                "part",
                "2|forest|mfgr|Brand#1|STANDARD BRUSHED STEEL|1|SM BOX|1.00|comment",
            )
            .expect("plain part");

        state
            .ingest_lineitem_values(1, 10_000, 10, -1_583)
            .expect("promo line");
        state
            .ingest_lineitem_values(2, 10_000, 10, -1_583)
            .expect("plain line");

        let rows = state.finish_rows();

        assert_eq!(rows.len(), 1);
        assert!((rows[0].promo_revenue - 50.0).abs() < f64::EPSILON);
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q14_sidecar_rejects_discount_factor_overflow() {
        let mut state = TpchQ14BuildState::default();
        state
            .ingest(
                "part",
                "1|forest|mfgr|Brand#1|PROMO BRUSHED STEEL|1|SM BOX|1.00|comment",
            )
            .expect("promo part");

        let err = state
            .ingest_lineitem_values(1, 10_000, i64::MIN, -1_583)
            .expect_err("discount factor overflow should reject");

        assert!(err.to_string().contains("TPC-H sidecar revenue overflow"));
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q15_sidecar_selects_top_supplier_revenue() {
        let mut state = TpchQ15BuildState::default();
        state
            .ingest(
                "supplier",
                "3|Supplier#3|address|1|11-111-1111|0.00|comment",
            )
            .expect("supplier");
        state
            .ingest_lineitem_values(3, 10_000, 10, -1_461)
            .expect("lineitem");

        let rows = state.finish_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].s_suppkey, 3);
        assert_eq!(rows[0].total_revenue, 900_000);
        assert_eq!(rows[0].s_name, "Supplier#3");
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q15_sidecar_rejects_discount_factor_overflow() {
        let mut state = TpchQ15BuildState::default();
        state
            .ingest(
                "supplier",
                "3|Supplier#3|address|1|11-111-1111|0.00|comment",
            )
            .expect("supplier");

        let err = state
            .ingest_lineitem_values(3, 10_000, i64::MIN, -1_461)
            .expect_err("discount factor overflow should reject");

        assert!(err.to_string().contains("TPC-H sidecar revenue overflow"));
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q16_sidecar_counts_distinct_non_complaint_suppliers() {
        let mut state = TpchQ16BuildState::default();
        state
            .ingest("supplier", "3|Supplier#3|address|1|11|0.00|fine supplier")
            .expect("good supplier");
        state
            .ingest(
                "supplier",
                "4|Supplier#4|address|1|11|0.00|Customer filed Complaints here",
            )
            .expect("bad supplier");
        state
            .ingest(
                "part",
                "5|name|mfgr|Brand#12|SMALL BRUSHED STEEL|49|SM BOX|1.00|comment",
            )
            .expect("part");
        state
            .ingest("partsupp", "5|3|1|10.00|comment")
            .expect("partsupp good");
        state
            .ingest("partsupp", "5|4|1|10.00|comment")
            .expect("partsupp bad");

        let rows = state.finish_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].p_brand, "Brand#12");
        assert_eq!(rows[0].supplier_cnt, 1);
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q17_sidecar_sums_small_quantity_revenue() {
        let mut state = TpchQ17BuildState::default();
        state
            .ingest(
                "part",
                "5|name|mfgr|Brand#23|SMALL BRUSHED STEEL|1|MED BOX|1.00|comment",
            )
            .expect("part");
        state
            .ingest_lineitem_values(5, 10, 7_000)
            .expect("small line");
        state
            .ingest_lineitem_values(5, 100, 70_000)
            .expect("large line");

        let rows = state.finish_rows();

        assert_eq!(rows.len(), 1);
        assert!((rows[0].avg_yearly - 10.0).abs() < f64::EPSILON);
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q18_sidecar_keeps_top_large_quantity_orders() {
        let mut state = TpchQ18BuildState::default();
        state
            .ingest("customer", "1|Customer#1|addr|1|13|1.00|MKT|comment")
            .expect("customer");
        state
            .ingest(
                "orders",
                "10|1|O|100.00|1995-01-01|1-URGENT|clerk|0|comment",
            )
            .expect("orders");
        state.ingest_lineitem_values(10, 20_000).expect("line 1");
        state.ingest_lineitem_values(10, 15_000).expect("line 2");

        let rows = state.finish_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].c_name, "Customer#1");
        assert_eq!(rows[0].o_orderkey, 10);
        assert_eq!(rows[0].sum_quantity, 35_000);
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q18_sidecar_rejects_quantity_overflow() {
        let mut state = TpchQ18BuildState::default();
        state
            .ingest_lineitem_values(10, i64::MAX)
            .expect("first line");

        let err = state
            .ingest_lineitem_values(10, 1)
            .expect_err("quantity overflow should reject");

        assert!(err.to_string().contains("TPC-H sidecar quantity overflow"));
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q19_sidecar_sums_matching_brand_container_revenue() {
        let mut state = TpchQ19BuildState::default();
        state
            .ingest("part", "5|name|mfgr|Brand#12|TYPE|3|SM BOX|1.00|comment")
            .expect("part");
        state
            .ingest_lineitem_values(5, 1_00, 10_000, 10, "AIR", "DELIVER IN PERSON")
            .expect("lineitem");

        let rows = state.finish_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].revenue, 900_000);
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q19_sidecar_rejects_discount_factor_overflow() {
        let mut state = TpchQ19BuildState::default();
        state
            .ingest("part", "5|name|mfgr|Brand#12|TYPE|3|SM BOX|1.00|comment")
            .expect("part");

        let err = state
            .ingest_lineitem_values(5, 1_00, 10_000, i64::MIN, "AIR", "DELIVER IN PERSON")
            .expect_err("discount factor overflow should reject");

        assert!(err.to_string().contains("TPC-H sidecar revenue overflow"));
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q20_sidecar_selects_canada_forest_supplier_above_half_quantity() {
        let mut state = TpchQ20BuildState::default();
        state
            .ingest("nation", "3|CANADA|1|comment")
            .expect("nation");
        state
            .ingest("supplier", "7|Supplier#7|addr|3|11-111|1.00|comment")
            .expect("supplier");
        state
            .ingest(
                "part",
                "5|forest green part|mfgr|Brand#1|TYPE|3|SM BOX|1.00|comment",
            )
            .expect("part");
        state
            .ingest("partsupp", "5|7|6|1.00|comment")
            .expect("partsupp");
        state
            .ingest_lineitem_values(5, 7, 10_00, DIRECT_Q6_SHIPDATE_START_1994_01_01)
            .expect("lineitem");

        let rows = state.finish_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].s_name, "Supplier#7");
        assert_eq!(rows[0].s_address, "addr");
    }

    #[cfg(feature = "sql-bench")]
    #[test]
    fn tpch_q21_sidecar_counts_only_late_saudi_supplier_in_final_order() {
        let mut state = TpchQ21BuildState::default();
        state
            .ingest("nation", "4|SAUDI ARABIA|1|comment")
            .expect("nation");
        state
            .ingest("supplier", "7|Supplier#7|addr|4|11-111|1.00|comment")
            .expect("supplier");
        state
            .ingest(
                "orders",
                "10|1|F|1.00|1995-01-01|1-URGENT|Clerk#1|0|comment",
            )
            .expect("orders");
        state
            .ingest_lineitem_values(10, 7, 1, 2)
            .expect("late line");
        state
            .ingest_lineitem_values(10, 8, 2, 2)
            .expect("other supplier");

        let rows = state.finish_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].s_name, "Supplier#7");
        assert_eq!(rows[0].numwait, 1);
    }
}
