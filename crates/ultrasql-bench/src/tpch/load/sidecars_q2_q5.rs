//! TPC-H Q2-Q5 direct-load sidecars.
//!
//! Pure code motion from the original `tpch::load` module: TPC-H
//! direct-load sidecar build states that accumulate query results while
//! the `.tbl` files stream through the in-process loader.

use anyhow::{Context, Result, bail};

use super::arith::*;
use super::encode::*;
use super::{
    DIRECT_Q3_DATE_1995_03_15, DIRECT_Q4_ORDERDATE_END_1993_10_01,
    DIRECT_Q4_ORDERDATE_START_1993_07_01, DIRECT_Q6_SHIPDATE_END_1995_01_01,
    DIRECT_Q6_SHIPDATE_START_1994_01_01, parse_tbl_line,
};

#[cfg(feature = "sql-bench")]
#[derive(Clone, Debug)]
pub(crate) struct TpchQ2Supplier {
    pub(crate) acctbal: i64,
    pub(crate) name: String,
    pub(crate) address: String,
    pub(crate) nation_name: String,
    pub(crate) phone: String,
    pub(crate) comment: String,
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Debug)]
pub(crate) struct TpchQ2Part {
    pub(crate) mfgr: String,
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Copy, Debug)]
pub(crate) struct TpchQ2Candidate {
    pub(crate) partkey: i32,
    pub(crate) suppkey: i32,
    pub(crate) supplycost: i64,
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
pub(crate) struct TpchQ2BuildState {
    pub(crate) europe_region_keys: std::collections::BTreeSet<i32>,
    pub(crate) europe_nations: std::collections::BTreeMap<i32, String>,
    pub(crate) europe_suppliers: std::collections::BTreeMap<i32, TpchQ2Supplier>,
    pub(crate) brass_parts: std::collections::BTreeMap<i32, TpchQ2Part>,
    pub(crate) best_supply_cost: std::collections::BTreeMap<i32, i64>,
    pub(crate) candidates: Vec<TpchQ2Candidate>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ2BuildState {
    pub(crate) fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
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
    pub(crate) fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ2ResultRow> {
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

    pub(crate) fn ingest_region(&mut self, line: &str) -> Result<()> {
        let fields = q2_fields("region", line, 3)?;
        if fields[1] == "EUROPE" {
            self.europe_region_keys
                .insert(q2_parse_i32(&fields, 0, "r_regionkey")?);
        }
        Ok(())
    }

    pub(crate) fn ingest_nation(&mut self, line: &str) -> Result<()> {
        let fields = q2_fields("nation", line, 4)?;
        let regionkey = q2_parse_i32(&fields, 2, "n_regionkey")?;
        if self.europe_region_keys.contains(&regionkey) {
            self.europe_nations
                .insert(q2_parse_i32(&fields, 0, "n_nationkey")?, fields[1].clone());
        }
        Ok(())
    }

    pub(crate) fn ingest_supplier(&mut self, line: &str) -> Result<()> {
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

    pub(crate) fn ingest_part(&mut self, line: &str) -> Result<()> {
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

    pub(crate) fn ingest_partsupp(&mut self, line: &str) -> Result<()> {
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
pub(crate) fn q2_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
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
pub(crate) fn q2_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
pub(crate) fn q2_parse_decimal2(raw: &str, label: &str) -> Result<i64> {
    let ultrasql_core::Value::Decimal { value, .. } =
        parse_direct_decimal(raw, 2, 0).with_context(|| format!("parse {label} `{raw}`"))?
    else {
        unreachable!("parse_direct_decimal always returns Decimal");
    };
    Ok(value)
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Copy, Debug)]
pub(crate) struct TpchQ3Order {
    pub(crate) orderdate: i32,
    pub(crate) shippriority: i32,
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Copy, Debug)]
pub(crate) struct TpchQ3Agg {
    pub(crate) orderdate: i32,
    pub(crate) shippriority: i32,
    pub(crate) revenue: i64,
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
pub(crate) struct TpchQ3BuildState {
    pub(crate) building_custkeys: std::collections::BTreeSet<i32>,
    pub(crate) qualifying_orders: std::collections::BTreeMap<i32, TpchQ3Order>,
    pub(crate) order_revenue: std::collections::BTreeMap<i32, TpchQ3Agg>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ3BuildState {
    pub(crate) fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "customer" => self.ingest_customer(line),
            "orders" => self.ingest_order(line),
            "lineitem" => self.ingest_lineitem(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    pub(crate) fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ3ResultRow> {
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

    pub(crate) fn ingest_customer(&mut self, line: &str) -> Result<()> {
        let fields = q3_fields("customer", line, 8)?;
        if fields[6] == "BUILDING" {
            self.building_custkeys
                .insert(q3_parse_i32(&fields, 0, "c_custkey")?);
        }
        Ok(())
    }

    pub(crate) fn ingest_order(&mut self, line: &str) -> Result<()> {
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

    pub(crate) fn ingest_lineitem(&mut self, line: &str) -> Result<()> {
        let fields = q3_fields("lineitem", line, 16)?;
        let orderkey = q3_parse_i32(&fields, 0, "l_orderkey")?;
        let shipdate = parse_direct_date(&fields[10], 10).context("parse l_shipdate")?;
        let extendedprice = q3_parse_decimal2(&fields[5], "l_extendedprice")?;
        let discount = q3_parse_decimal2(&fields[6], "l_discount")?;
        self.add_lineitem_revenue(orderkey, extendedprice, discount, shipdate)
    }

    pub(crate) fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
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

    pub(crate) fn add_lineitem_revenue(
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
pub(crate) fn q3_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
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
pub(crate) fn q3_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
pub(crate) fn q3_parse_decimal2(raw: &str, label: &str) -> Result<i64> {
    let ultrasql_core::Value::Decimal { value, .. } =
        parse_direct_decimal(raw, 2, 0).with_context(|| format!("parse {label} `{raw}`"))?
    else {
        unreachable!("parse_direct_decimal always returns Decimal");
    };
    Ok(value)
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
pub(crate) struct TpchQ4BuildState {
    pub(crate) candidate_orders: std::collections::HashMap<i32, String>,
    pub(crate) matched_orderkeys: std::collections::HashSet<i32>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ4BuildState {
    pub(crate) fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "orders" => self.ingest_order(line),
            "lineitem" => self.ingest_lineitem(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    pub(crate) fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ4ResultRow> {
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

    pub(crate) fn ingest_order(&mut self, line: &str) -> Result<()> {
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

    pub(crate) fn ingest_lineitem(&mut self, line: &str) -> Result<()> {
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

    pub(crate) fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
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

    pub(crate) fn add_lineitem_match(&mut self, orderkey: i32, commitdate: i32, receiptdate: i32) {
        if commitdate < receiptdate && self.candidate_orders.contains_key(&orderkey) {
            self.matched_orderkeys.insert(orderkey);
        }
    }
}

#[cfg(feature = "sql-bench")]
pub(crate) fn q4_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
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
pub(crate) fn q4_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
pub(crate) struct TpchQ5BuildState {
    pub(crate) asia_region_keys: std::collections::BTreeSet<i32>,
    pub(crate) asia_nations: std::collections::HashMap<i32, String>,
    pub(crate) asia_suppliers: std::collections::HashMap<i32, i32>,
    pub(crate) asia_customers: std::collections::HashMap<i32, i32>,
    pub(crate) qualifying_orders: std::collections::HashMap<i32, i32>,
    pub(crate) revenue_by_nation: std::collections::HashMap<i32, i64>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ5BuildState {
    pub(crate) fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
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
    pub(crate) fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ5ResultRow> {
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

    pub(crate) fn ingest_region(&mut self, line: &str) -> Result<()> {
        let fields = q5_fields("region", line, 3)?;
        if fields[1] == "ASIA" {
            self.asia_region_keys
                .insert(q5_parse_i32(&fields, 0, "r_regionkey")?);
        }
        Ok(())
    }

    pub(crate) fn ingest_nation(&mut self, line: &str) -> Result<()> {
        let fields = q5_fields("nation", line, 4)?;
        let regionkey = q5_parse_i32(&fields, 2, "n_regionkey")?;
        if self.asia_region_keys.contains(&regionkey) {
            self.asia_nations
                .insert(q5_parse_i32(&fields, 0, "n_nationkey")?, fields[1].clone());
        }
        Ok(())
    }

    pub(crate) fn ingest_supplier(&mut self, line: &str) -> Result<()> {
        let fields = q5_fields("supplier", line, 7)?;
        let nationkey = q5_parse_i32(&fields, 3, "s_nationkey")?;
        if self.asia_nations.contains_key(&nationkey) {
            self.asia_suppliers
                .insert(q5_parse_i32(&fields, 0, "s_suppkey")?, nationkey);
        }
        Ok(())
    }

    pub(crate) fn ingest_customer(&mut self, line: &str) -> Result<()> {
        let fields = q5_fields("customer", line, 8)?;
        let nationkey = q5_parse_i32(&fields, 3, "c_nationkey")?;
        if self.asia_nations.contains_key(&nationkey) {
            self.asia_customers
                .insert(q5_parse_i32(&fields, 0, "c_custkey")?, nationkey);
        }
        Ok(())
    }

    pub(crate) fn ingest_order(&mut self, line: &str) -> Result<()> {
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

    pub(crate) fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
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

    pub(crate) fn add_lineitem_revenue(
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
pub(crate) fn q5_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
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
pub(crate) fn q5_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}
