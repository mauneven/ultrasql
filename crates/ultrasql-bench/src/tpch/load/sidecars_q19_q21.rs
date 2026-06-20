//! TPC-H Q19-Q21 direct-load sidecars.
//!
//! Pure code motion from the original `tpch::load` module: TPC-H
//! direct-load sidecar build states that accumulate query results while
//! the `.tbl` files stream through the in-process loader.

use anyhow::{Context, Result, bail};

use super::arith::*;
use super::encode::*;
use super::{
    DIRECT_Q6_SHIPDATE_END_1995_01_01, DIRECT_Q6_SHIPDATE_START_1994_01_01, parse_tbl_line,
};

#[cfg(feature = "sql-bench")]
#[derive(Clone, Copy, Debug)]
pub(crate) struct TpchQ19Band {
    pub(crate) quantity_min: i64,
    pub(crate) quantity_max: i64,
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
pub(crate) struct TpchQ19BuildState {
    pub(crate) parts: std::collections::HashMap<i32, TpchQ19Band>,
    pub(crate) revenue: i64,
}

#[cfg(feature = "sql-bench")]
impl TpchQ19BuildState {
    pub(crate) fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "part" => self.ingest_part(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    pub(crate) fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ19ResultRow> {
        vec![ultrasql_server::TpchQ19ResultRow {
            revenue: self.revenue,
        }]
    }

    pub(crate) fn ingest_part(&mut self, line: &str) -> Result<()> {
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

    pub(crate) fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
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

    pub(crate) fn ingest_lineitem_values(
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
pub(crate) fn q19_part_band(brand: &str, container: &str, size: i32) -> Option<TpchQ19Band> {
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
pub(crate) fn q19_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
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
pub(crate) fn q19_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Debug)]
pub(crate) struct TpchQ20Supplier {
    pub(crate) name: String,
    pub(crate) address: String,
    pub(crate) nationkey: i32,
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Copy, Debug)]
pub(crate) struct TpchQ20PartSupp {
    pub(crate) partkey: i32,
    pub(crate) suppkey: i32,
    pub(crate) availqty: i64,
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
pub(crate) struct TpchQ20BuildState {
    pub(crate) canada_nationkeys: std::collections::HashSet<i32>,
    pub(crate) suppliers: std::collections::HashMap<i32, TpchQ20Supplier>,
    pub(crate) forest_parts: std::collections::HashSet<i32>,
    pub(crate) forest_partsupps: Vec<TpchQ20PartSupp>,
    pub(crate) quantity_by_part_supplier: std::collections::HashMap<(i32, i32), i64>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ20BuildState {
    pub(crate) fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "nation" => self.ingest_nation(line),
            "supplier" => self.ingest_supplier(line),
            "part" => self.ingest_part(line),
            "partsupp" => self.ingest_partsupp(line),
            _ => Ok(()),
        }
    }

    #[must_use]
    pub(crate) fn finish_rows(&self) -> Vec<ultrasql_server::TpchQ20ResultRow> {
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

    pub(crate) fn ingest_nation(&mut self, line: &str) -> Result<()> {
        let fields = q20_fields("nation", line, 4)?;
        if fields[1] == "CANADA" {
            self.canada_nationkeys
                .insert(q20_parse_i32(&fields, 0, "n_nationkey")?);
        }
        Ok(())
    }

    pub(crate) fn ingest_supplier(&mut self, line: &str) -> Result<()> {
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

    pub(crate) fn ingest_part(&mut self, line: &str) -> Result<()> {
        let fields = q20_fields("part", line, 9)?;
        if fields[1].starts_with("forest") {
            self.forest_parts
                .insert(q20_parse_i32(&fields, 0, "p_partkey")?);
        }
        Ok(())
    }

    pub(crate) fn ingest_partsupp(&mut self, line: &str) -> Result<()> {
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

    pub(crate) fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
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

    pub(crate) fn ingest_lineitem_values(
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
        let part_supplier_quantity = self
            .quantity_by_part_supplier
            .entry((partkey, suppkey))
            .or_default();
        *part_supplier_quantity =
            checked_direct_quantity_add_i64(*part_supplier_quantity, quantity)?;
        Ok(())
    }
}

#[cfg(feature = "sql-bench")]
pub(crate) fn q20_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
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
pub(crate) fn q20_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}

#[cfg(feature = "sql-bench")]
#[derive(Clone, Debug)]
pub(crate) struct TpchQ21Supplier {
    pub(crate) name: String,
    pub(crate) nationkey: i32,
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
pub(crate) struct TpchQ21Order {
    pub(crate) suppliers: std::collections::HashSet<i32>,
    pub(crate) late_count_by_supplier: std::collections::HashMap<i32, i64>,
}

#[cfg(feature = "sql-bench")]
#[derive(Debug, Default)]
pub(crate) struct TpchQ21BuildState {
    pub(crate) saudi_nationkeys: std::collections::HashSet<i32>,
    pub(crate) suppliers: std::collections::HashMap<i32, TpchQ21Supplier>,
    pub(crate) final_orders: std::collections::HashSet<i32>,
    pub(crate) orders: std::collections::HashMap<i32, TpchQ21Order>,
}

#[cfg(feature = "sql-bench")]
impl TpchQ21BuildState {
    pub(crate) fn ingest(&mut self, table: &str, line: &str) -> Result<()> {
        match table {
            "nation" => self.ingest_nation(line),
            "supplier" => self.ingest_supplier(line),
            "orders" => self.ingest_order(line),
            _ => Ok(()),
        }
    }

    pub(crate) fn finish_rows(&self) -> Result<Vec<ultrasql_server::TpchQ21ResultRow>> {
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
                let supplier_count = count_by_supplier_name
                    .entry(supplier.name.clone())
                    .or_default();
                *supplier_count = checked_direct_count_add_i64(*supplier_count, late_count)?;
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
        Ok(rows)
    }

    pub(crate) fn ingest_nation(&mut self, line: &str) -> Result<()> {
        let fields = q21_fields("nation", line, 4)?;
        if fields[1] == "SAUDI ARABIA" {
            self.saudi_nationkeys
                .insert(q21_parse_i32(&fields, 0, "n_nationkey")?);
        }
        Ok(())
    }

    pub(crate) fn ingest_supplier(&mut self, line: &str) -> Result<()> {
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

    pub(crate) fn ingest_order(&mut self, line: &str) -> Result<()> {
        let fields = q21_fields("orders", line, 9)?;
        if fields[2] == "F" {
            self.final_orders
                .insert(q21_parse_i32(&fields, 0, "o_orderkey")?);
        }
        Ok(())
    }

    pub(crate) fn ingest_lineitem_payload(&mut self, payload: &[u8]) -> Result<()> {
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

    pub(crate) fn ingest_lineitem_values(
        &mut self,
        orderkey: i32,
        suppkey: i32,
        commitdate: i32,
        receiptdate: i32,
    ) -> Result<()> {
        let order = self.orders.entry(orderkey).or_default();
        order.suppliers.insert(suppkey);
        if receiptdate > commitdate {
            let late_count = order.late_count_by_supplier.entry(suppkey).or_default();
            *late_count = checked_direct_count_add_i64(*late_count, 1)?;
        }
        Ok(())
    }
}

#[cfg(feature = "sql-bench")]
pub(crate) fn q21_fields(table: &str, line: &str, expected: usize) -> Result<Vec<String>> {
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
pub(crate) fn q21_parse_i32(fields: &[String], idx: usize, label: &str) -> Result<i32> {
    fields[idx]
        .parse::<i32>()
        .with_context(|| format!("parse {label} `{}`", fields[idx]))
}
