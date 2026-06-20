//! TPC-H Q16-Q18 direct-load sidecars.
//!
//! Pure code motion from the original `tpch::load` module: TPC-H
//! direct-load sidecar build states that accumulate query results while
//! the `.tbl` files stream through the in-process loader.

use anyhow::{Context, Result, bail};

use super::arith::*;
use super::encode::*;
use super::parse_tbl_line;

#[cfg(feature = "sql-bench")]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct TpchQ16GroupKey {
    pub(crate) brand: String,
    pub(crate) part_type: String,
    pub(crate) size: i32,
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Debug)]
pub(crate) struct TpchQ16Part {
    pub(crate) key: TpchQ16GroupKey,
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
pub(crate) struct TpchQ16BuildState {
    pub(crate) bad_suppliers: std::collections::HashSet<i32>,
    pub(crate) parts: std::collections::HashMap<i32, TpchQ16Part>,
    pub(crate) suppliers_by_group:
        std::collections::HashMap<TpchQ16GroupKey, std::collections::HashSet<i32>>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ16BuildState {
    pub(crate) fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "supplier" => self.ingest_supplier(line),
            "part" => self.ingest_part(line),
            "partsupp" => self.ingest_partsupp(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    pub(crate) fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ16ResultRow> {
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

    pub(crate) fn ingest_supplier(&mut self, line: &str) -> Result<()> {
        let fields = q16_fields("supplier", line, 7)?;
        if q16_comment_has_customer_complaints(&fields[6]) {
            self.bad_suppliers
                .insert(q16_parse_i32(&fields, 0, "s_suppkey")?);
        }
        Ok(())
    }

    pub(crate) fn ingest_part(&mut self, line: &str) -> Result<()> {
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

    pub(crate) fn ingest_partsupp(&mut self, line: &str) -> Result<()> {
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
pub(crate) fn q16_comment_has_customer_complaints(comment: &str) -> bool {
    comment
        .find("Customer")
        .is_some_and(|pos| comment[pos.saturating_add("Customer".len())..].contains("Complaints"))
}

#[cfg(feature = "sql-bench")]
pub(crate) fn q16_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
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
pub(crate) fn q16_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Debug)]
pub(crate) struct TpchQ17Line {
    pub(crate) partkey: i32,
    pub(crate) quantity: i64,
    pub(crate) extendedprice: i64,
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Debug, Default)]
pub(crate) struct TpchQ17PartStats {
    pub(crate) sum_quantity: i128,
    pub(crate) count: i64,
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
pub(crate) struct TpchQ17BuildState {
    pub(crate) qualifying_parts: std::collections::HashSet<i32>,
    pub(crate) stats_by_part: std::collections::HashMap<i32, TpchQ17PartStats>,
    pub(crate) lines: Vec<TpchQ17Line>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ17BuildState {
    pub(crate) fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "part" => self.ingest_part(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    pub(crate) fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ17ResultRow> {
        let mut revenue_sum: i128 = 0;
        for line in &self.lines {
            let Some(stats) = self.stats_by_part.get(&line.partkey) else {
                continue;
            };
            if i128::from(line.quantity) * 5 * i128::from(stats.count) < stats.sum_quantity {
                revenue_sum += i128::from(line.extendedprice);
            }
        }
        let avg_yearly = tpch_i128_to_f64(revenue_sum) / 700.0;
        vec![ultrasql_server::TpchQ17ResultRow { avg_yearly }]
    }

    pub(crate) fn ingest_part(&mut self, line: &str) -> Result<()> {
        let fields = q17_fields("part", line, 9)?;
        if fields[3] == "Brand#23" && fields[6] == "MED BOX" {
            self.qualifying_parts
                .insert(q17_parse_i32(&fields, 0, "p_partkey")?);
        }
        Ok(())
    }

    pub(crate) fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
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

    pub(crate) fn ingest_lineitem_values(
        &mut self,
        partkey: i32,
        quantity: i64,
        extendedprice: i64,
    ) -> Result<()> {
        if !self.qualifying_parts.contains(&partkey) {
            return Ok(());
        }
        let stats = self.stats_by_part.entry(partkey).or_default();
        stats.sum_quantity =
            checked_direct_quantity_add_i128(stats.sum_quantity, i128::from(quantity))?;
        stats.count = checked_direct_count_add_i64(stats.count, 1)?;
        self.lines.push(TpchQ17Line {
            partkey,
            quantity,
            extendedprice,
        });
        Ok(())
    }
}

#[cfg(feature = "sql-bench")]
pub(crate) fn q17_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
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
pub(crate) fn q17_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Debug)]
pub(crate) struct TpchQ18Customer {
    pub(crate) name: String,
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Debug)]
pub(crate) struct TpchQ18Order {
    pub(crate) custkey: i32,
    pub(crate) orderdate: i32,
    pub(crate) totalprice: i64,
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
pub(crate) struct TpchQ18BuildState {
    pub(crate) customers: std::collections::HashMap<i32, TpchQ18Customer>,
    pub(crate) orders: std::collections::HashMap<i32, TpchQ18Order>,
    pub(crate) quantity_by_order: std::collections::HashMap<i32, i64>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ18BuildState {
    pub(crate) fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "customer" => self.ingest_customer(line),
            "orders" => self.ingest_order(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    pub(crate) fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ18ResultRow> {
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

    pub(crate) fn ingest_customer(&mut self, line: &str) -> Result<()> {
        let fields = q18_fields("customer", line, 8)?;
        self.customers.insert(
            q18_parse_i32(&fields, 0, "c_custkey")?,
            TpchQ18Customer {
                name: fields[1].clone(),
            },
        );
        Ok(())
    }

    pub(crate) fn ingest_order(&mut self, line: &str) -> Result<()> {
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

    pub(crate) fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
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

    pub(crate) fn ingest_lineitem_values(&mut self, orderkey: i32, quantity: i64) -> Result<()> {
        let order_quantity = self.quantity_by_order.entry(orderkey).or_default();
        *order_quantity = checked_direct_quantity_add_i64(*order_quantity, quantity)?;
        Ok(())
    }
}

#[cfg(feature = "sql-bench")]
pub(crate) fn q18_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
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
pub(crate) fn q18_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
pub(crate) fn q18_parse_decimal2(raw: &str, label: &str) -> Result<i64> {
    let ultrasql_core::Value::Decimal { value, .. } =
        parse_direct_decimal(raw, 2, 0).with_context(|| format!("parse {label} `{raw}`"))?
    else {
        unreachable!("parse_direct_decimal always returns Decimal");
    };
    Ok(value)
}
