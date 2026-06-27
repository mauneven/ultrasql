//! TPC-H Q11-Q15 direct-load sidecars.
//!
//! Pure code motion from the original `tpch::load` module: TPC-H
//! direct-load sidecar build states that accumulate query results while
//! the `.tbl` files stream through the in-process loader.

use anyhow::{Context, Result, bail};

use super::arith::*;
use super::encode::*;
use super::{
    DIRECT_Q12_RECEIPTDATE_END_1995_01_01, DIRECT_Q12_RECEIPTDATE_START_1994_01_01,
    DIRECT_Q14_SHIPDATE_END_1995_10_01, DIRECT_Q14_SHIPDATE_START_1995_09_01,
    DIRECT_Q15_SHIPDATE_END_1996_04_01, DIRECT_Q15_SHIPDATE_START_1996_01_01, parse_tbl_line,
};

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
pub(crate) struct TpchQ11BuildState {
    pub(crate) german_nations: std::collections::BTreeSet<i32>,
    pub(crate) german_suppliers: std::collections::HashSet<i32>,
    pub(crate) value_by_part: std::collections::HashMap<i32, i64>,
    pub(crate) total_value: i64,
}

#[cfg(feature = "sql-bench")]
impl TpchQ11BuildState {
    pub(crate) fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "nation" => self.ingest_nation(line),
            "supplier" => self.ingest_supplier(line),
            "partsupp" => self.ingest_partsupp(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    pub(crate) fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ11ResultRow> {
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

    pub(crate) fn ingest_nation(&mut self, line: &str) -> Result<()> {
        let fields = q11_fields("nation", line, 4)?;
        if fields[1] == "GERMANY" {
            self.german_nations
                .insert(q11_parse_i32(&fields, 0, "n_nationkey")?);
        }
        Ok(())
    }

    pub(crate) fn ingest_supplier(&mut self, line: &str) -> Result<()> {
        let fields = q11_fields("supplier", line, 7)?;
        let nationkey = q11_parse_i32(&fields, 3, "s_nationkey")?;
        if self.german_nations.contains(&nationkey) {
            self.german_suppliers
                .insert(q11_parse_i32(&fields, 0, "s_suppkey")?);
        }
        Ok(())
    }

    pub(crate) fn ingest_partsupp(&mut self, line: &str) -> Result<()> {
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
pub(crate) fn q11_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
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
pub(crate) fn q11_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
pub(crate) fn q11_parse_i64(fields: &[String], idx: usize, label: &str) -> Result<i64> {
    fields[idx]
        .parse::<i64>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
pub(crate) fn q11_parse_decimal2(raw: &str, label: &str) -> Result<i64> {
    let ultrasql_core::Value::Decimal { value, .. } =
        parse_direct_decimal(raw, 2, 0).with_context(|| format!("parse {label} `{raw}`"))?
    else {
        unreachable!("parse_direct_decimal always returns Decimal");
    };
    i64::try_from(value).with_context(|| format!("{label} `{raw}` out of range"))
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
pub(crate) struct TpchQ12BuildState {
    pub(crate) high_priority_orders: std::collections::HashMap<i32, bool>,
    pub(crate) counts_by_shipmode: std::collections::BTreeMap<String, (i64, i64)>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ12BuildState {
    pub(crate) fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "orders" => self.ingest_order(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    pub(crate) fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ12ResultRow> {
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

    pub(crate) fn ingest_order(&mut self, line: &str) -> Result<()> {
        let fields = q12_fields("orders", line, 9)?;
        let orderkey = q12_parse_i32(&fields, 0, "o_orderkey")?;
        let high_priority = matches!(fields[5].as_str(), "1-URGENT" | "2-HIGH");
        self.high_priority_orders.insert(orderkey, high_priority);
        Ok(())
    }

    pub(crate) fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
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

    pub(crate) fn ingest_lineitem_values(
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
            counts.0 = checked_direct_count_add_i64(counts.0, 1)?;
        } else {
            counts.1 = checked_direct_count_add_i64(counts.1, 1)?;
        }
        Ok(())
    }
}

#[cfg(feature = "sql-bench")]
pub(crate) fn q12_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
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
pub(crate) fn q12_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
pub(crate) struct TpchQ13BuildState {
    pub(crate) total_customers: i64,
    pub(crate) customers_with_order_count: i64,
    pub(crate) order_count_by_customer: std::collections::HashMap<i32, i64>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ13BuildState {
    pub(crate) fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "customer" => self.ingest_customer(line),
            "orders" => self.ingest_order(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    pub(crate) fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ13ResultRow> {
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

    pub(crate) fn ingest_customer(&mut self, line: &str) -> Result<()> {
        let _fields = q13_fields("customer", line, 8)?;
        self.total_customers = checked_direct_count_add_i64(self.total_customers, 1)?;
        Ok(())
    }

    pub(crate) fn ingest_order(&mut self, line: &str) -> Result<()> {
        let fields = q13_fields("orders", line, 9)?;
        if q13_comment_has_special_requests(&fields[8]) {
            return Ok(());
        }
        let custkey = q13_parse_i32(&fields, 1, "o_custkey")?;
        match self.order_count_by_customer.entry(custkey) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                *entry.get_mut() = checked_direct_count_add_i64(*entry.get(), 1)?;
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(1);
                self.customers_with_order_count =
                    checked_direct_count_add_i64(self.customers_with_order_count, 1)?;
            }
        }
        Ok(())
    }
}

#[cfg(feature = "sql-bench")]
pub(crate) fn q13_comment_has_special_requests(comment: &str) -> bool {
    comment
        .find("special")
        .is_some_and(|pos| comment[pos.saturating_add("special".len())..].contains("requests"))
}

#[cfg(feature = "sql-bench")]
pub(crate) fn q13_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
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
pub(crate) fn q13_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
pub(crate) struct TpchQ14BuildState {
    pub(crate) promo_parts: std::collections::HashSet<i32>,
    pub(crate) promo_volume: i128,
    pub(crate) total_volume: i128,
}

#[cfg(feature = "sql-bench")]
impl TpchQ14BuildState {
    pub(crate) fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "part" => self.ingest_part(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    pub(crate) fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ14ResultRow> {
        let promo_revenue = if self.total_volume == 0 {
            0.0
        } else {
            100.0 * tpch_i128_to_f64(self.promo_volume) / tpch_i128_to_f64(self.total_volume)
        };
        vec![ultrasql_server::TpchQ14ResultRow { promo_revenue }]
    }

    pub(crate) fn ingest_part(&mut self, line: &str) -> Result<()> {
        let fields = q14_fields("part", line, 9)?;
        if fields[4].starts_with("PROMO") {
            self.promo_parts
                .insert(q14_parse_i32(&fields, 0, "p_partkey")?);
        }
        Ok(())
    }

    pub(crate) fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
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

    pub(crate) fn ingest_lineitem_values(
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
pub(crate) fn q14_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
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
pub(crate) fn q14_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Debug)]
pub(crate) struct TpchQ15Supplier {
    pub(crate) name: String,
    pub(crate) address: String,
    pub(crate) phone: String,
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
pub(crate) struct TpchQ15BuildState {
    pub(crate) suppliers: std::collections::HashMap<i32, TpchQ15Supplier>,
    pub(crate) revenue_by_supplier: std::collections::HashMap<i32, i64>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ15BuildState {
    pub(crate) fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "supplier" => self.ingest_supplier(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    pub(crate) fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ15ResultRow> {
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

    pub(crate) fn ingest_supplier(&mut self, line: &str) -> Result<()> {
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

    pub(crate) fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
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

    pub(crate) fn ingest_lineitem_values(
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
pub(crate) fn q15_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
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
pub(crate) fn q15_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}
