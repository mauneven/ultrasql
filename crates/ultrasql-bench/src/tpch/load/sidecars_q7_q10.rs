//! TPC-H Q7-Q10 direct-load sidecars.
//!
//! Pure code motion from the original `tpch::load` module: TPC-H
//! direct-load sidecar build states that accumulate query results while
//! the `.tbl` files stream through the in-process loader.

use anyhow::{Context, Result, bail};

use super::arith::*;
use super::encode::*;
use super::{
    DIRECT_Q4_ORDERDATE_END_1993_10_01, DIRECT_Q6_SHIPDATE_END_1995_01_01,
    DIRECT_Q6_SHIPDATE_START_1994_01_01, DIRECT_Q7_SHIPDATE_END_EXCLUSIVE_1997_01_01,
    DIRECT_Q7_YEAR_1996_START_1996_01_01, parse_tbl_line,
};

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
pub(crate) struct TpchQ7BuildState {
    pub(crate) pair_nations: std::collections::HashMap<i32, String>,
    pub(crate) pair_suppliers: std::collections::HashMap<i32, String>,
    pub(crate) pair_customers: std::collections::HashMap<i32, String>,
    pub(crate) pair_orders: std::collections::HashMap<i32, String>,
    pub(crate) revenue_by_key: std::collections::BTreeMap<(String, String, i32), i64>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ7BuildState {
    pub(crate) fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "nation" => self.ingest_nation(line),
            "supplier" => self.ingest_supplier(line),
            "customer" => self.ingest_customer(line),
            "orders" => self.ingest_order(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    pub(crate) fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ7ResultRow> {
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

    pub(crate) fn ingest_nation(&mut self, line: &str) -> Result<()> {
        let fields = q7_fields("nation", line, 4)?;
        if fields[1] == "FRANCE" || fields[1] == "GERMANY" {
            self.pair_nations
                .insert(q7_parse_i32(&fields, 0, "n_nationkey")?, fields[1].clone());
        }
        Ok(())
    }

    pub(crate) fn ingest_supplier(&mut self, line: &str) -> Result<()> {
        let fields = q7_fields("supplier", line, 7)?;
        let nationkey = q7_parse_i32(&fields, 3, "s_nationkey")?;
        let Some(nation) = self.pair_nations.get(&nationkey) else {
            return Ok(());
        };
        self.pair_suppliers
            .insert(q7_parse_i32(&fields, 0, "s_suppkey")?, nation.clone());
        Ok(())
    }

    pub(crate) fn ingest_customer(&mut self, line: &str) -> Result<()> {
        let fields = q7_fields("customer", line, 8)?;
        let nationkey = q7_parse_i32(&fields, 3, "c_nationkey")?;
        let Some(nation) = self.pair_nations.get(&nationkey) else {
            return Ok(());
        };
        self.pair_customers
            .insert(q7_parse_i32(&fields, 0, "c_custkey")?, nation.clone());
        Ok(())
    }

    pub(crate) fn ingest_order(&mut self, line: &str) -> Result<()> {
        let fields = q7_fields("orders", line, 9)?;
        let custkey = q7_parse_i32(&fields, 1, "o_custkey")?;
        let Some(cust_nation) = self.pair_customers.get(&custkey) else {
            return Ok(());
        };
        self.pair_orders
            .insert(q7_parse_i32(&fields, 0, "o_orderkey")?, cust_nation.clone());
        Ok(())
    }

    pub(crate) fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
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

    pub(crate) fn add_lineitem_revenue(
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
pub(crate) fn q7_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
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
pub(crate) fn q7_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct TpchQ8YearState {
    pub(crate) total_volume: i64,
    pub(crate) brazil_volume: i64,
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
pub(crate) struct TpchQ8BuildState {
    pub(crate) america_region_keys: std::collections::BTreeSet<i32>,
    pub(crate) america_nations: std::collections::BTreeSet<i32>,
    pub(crate) brazil_nations: std::collections::BTreeSet<i32>,
    pub(crate) suppliers: std::collections::HashMap<i32, bool>,
    pub(crate) america_customers: std::collections::HashSet<i32>,
    pub(crate) qualifying_parts: std::collections::HashSet<i32>,
    pub(crate) qualifying_orders: std::collections::HashMap<i32, i32>,
    pub(crate) years: std::collections::BTreeMap<i32, TpchQ8YearState>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ8BuildState {
    pub(crate) fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
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
    pub(crate) fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ8ResultRow> {
        self.years
            .iter()
            .filter(|(_, state)| state.total_volume != 0)
            .map(|(&o_year, state)| ultrasql_server::TpchQ8ResultRow {
                o_year,
                mkt_share: q8_i64_to_f64(state.brazil_volume) / q8_i64_to_f64(state.total_volume),
            })
            .collect()
    }

    pub(crate) fn ingest_region(&mut self, line: &str) -> Result<()> {
        let fields = q8_fields("region", line, 3)?;
        if fields[1] == "AMERICA" {
            self.america_region_keys
                .insert(q8_parse_i32(&fields, 0, "r_regionkey")?);
        }
        Ok(())
    }

    pub(crate) fn ingest_nation(&mut self, line: &str) -> Result<()> {
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

    pub(crate) fn ingest_supplier(&mut self, line: &str) -> Result<()> {
        let fields = q8_fields("supplier", line, 7)?;
        let nationkey = q8_parse_i32(&fields, 3, "s_nationkey")?;
        self.suppliers.insert(
            q8_parse_i32(&fields, 0, "s_suppkey")?,
            self.brazil_nations.contains(&nationkey),
        );
        Ok(())
    }

    pub(crate) fn ingest_customer(&mut self, line: &str) -> Result<()> {
        let fields = q8_fields("customer", line, 8)?;
        let nationkey = q8_parse_i32(&fields, 3, "c_nationkey")?;
        if self.america_nations.contains(&nationkey) {
            self.america_customers
                .insert(q8_parse_i32(&fields, 0, "c_custkey")?);
        }
        Ok(())
    }

    pub(crate) fn ingest_part(&mut self, line: &str) -> Result<()> {
        let fields = q8_fields("part", line, 9)?;
        if fields[4] == "ECONOMY ANODIZED STEEL" {
            self.qualifying_parts
                .insert(q8_parse_i32(&fields, 0, "p_partkey")?);
        }
        Ok(())
    }

    pub(crate) fn ingest_order(&mut self, line: &str) -> Result<()> {
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

    pub(crate) fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
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

    pub(crate) fn add_lineitem_volume(
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
pub(crate) fn q8_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
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
pub(crate) fn q8_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
pub(crate) struct TpchQ9BuildState {
    pub(crate) green_parts: std::collections::HashSet<i32>,
    pub(crate) nations: std::collections::HashMap<i32, String>,
    pub(crate) suppliers: std::collections::HashMap<i32, String>,
    pub(crate) partsupp_cost: std::collections::HashMap<(i32, i32), i64>,
    pub(crate) orders: std::collections::HashMap<i32, i32>,
    pub(crate) profit_by_key: std::collections::BTreeMap<(String, i32), i64>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ9BuildState {
    pub(crate) fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
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
    pub(crate) fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ9ResultRow> {
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

    pub(crate) fn ingest_nation(&mut self, line: &str) -> Result<()> {
        let fields = q9_fields("nation", line, 4)?;
        self.nations
            .insert(q9_parse_i32(&fields, 0, "n_nationkey")?, fields[1].clone());
        Ok(())
    }

    pub(crate) fn ingest_supplier(&mut self, line: &str) -> Result<()> {
        let fields = q9_fields("supplier", line, 7)?;
        let nationkey = q9_parse_i32(&fields, 3, "s_nationkey")?;
        let Some(nation) = self.nations.get(&nationkey) else {
            return Ok(());
        };
        self.suppliers
            .insert(q9_parse_i32(&fields, 0, "s_suppkey")?, nation.clone());
        Ok(())
    }

    pub(crate) fn ingest_part(&mut self, line: &str) -> Result<()> {
        let fields = q9_fields("part", line, 9)?;
        if fields[1].contains("green") {
            self.green_parts
                .insert(q9_parse_i32(&fields, 0, "p_partkey")?);
        }
        Ok(())
    }

    pub(crate) fn ingest_partsupp(&mut self, line: &str) -> Result<()> {
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

    pub(crate) fn ingest_order(&mut self, line: &str) -> Result<()> {
        let fields = q9_fields("orders", line, 9)?;
        let orderdate = parse_direct_date(&fields[4], 4).context("parse o_orderdate")?;
        self.orders.insert(
            q9_parse_i32(&fields, 0, "o_orderkey")?,
            direct_year_from_date(orderdate),
        );
        Ok(())
    }

    pub(crate) fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
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

    pub(crate) fn add_lineitem_profit(
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
pub(crate) fn q9_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
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
pub(crate) fn q9_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
pub(crate) fn q9_parse_decimal2(raw: &str, label: &str) -> Result<i64> {
    let ultrasql_core::Value::Decimal { value, .. } =
        parse_direct_decimal(raw, 2, 0).with_context(|| format!("parse {label} `{raw}`"))?
    else {
        unreachable!("parse_direct_decimal always returns Decimal");
    };
    Ok(value)
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Debug)]
pub(crate) struct TpchQ10Customer {
    pub(crate) name: String,
    pub(crate) acctbal: i64,
    pub(crate) nation: String,
    pub(crate) address: String,
    pub(crate) phone: String,
    pub(crate) comment: String,
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
pub(crate) struct TpchQ10BuildState {
    pub(crate) nations: std::collections::HashMap<i32, String>,
    pub(crate) customers: std::collections::HashMap<i32, TpchQ10Customer>,
    pub(crate) qualifying_orders: std::collections::HashMap<i32, i32>,
    pub(crate) revenue_by_customer: std::collections::HashMap<i32, i64>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ10BuildState {
    pub(crate) fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "nation" => self.ingest_nation(line),
            "customer" => self.ingest_customer(line),
            "orders" => self.ingest_order(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    pub(crate) fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ10ResultRow> {
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

    pub(crate) fn ingest_nation(&mut self, line: &str) -> Result<()> {
        let fields = q10_fields("nation", line, 4)?;
        self.nations
            .insert(q10_parse_i32(&fields, 0, "n_nationkey")?, fields[1].clone());
        Ok(())
    }

    pub(crate) fn ingest_customer(&mut self, line: &str) -> Result<()> {
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

    pub(crate) fn ingest_order(&mut self, line: &str) -> Result<()> {
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

    pub(crate) fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
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
pub(crate) fn q10_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
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
pub(crate) fn q10_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
pub(crate) fn q10_parse_decimal2(raw: &str, label: &str) -> Result<i64> {
    let ultrasql_core::Value::Decimal { value, .. } =
        parse_direct_decimal(raw, 2, 0).with_context(|| format!("parse {label} `{raw}`"))?
    else {
        unreachable!("parse_direct_decimal always returns Decimal");
    };
    Ok(value)
}
