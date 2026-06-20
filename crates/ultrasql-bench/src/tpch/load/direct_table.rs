//! Per-table direct heap loader.
//!
//! [`load_ultrasql_table_direct`] streams one `.tbl` file, encodes each row to
//! the binary heap layout, bulk-inserts batches into the in-process server
//! heap, and feeds every Q1-Q21 sidecar build state in a single pass. The Q1
//! columnar cache and its overflow-checked accumulators live here too.

use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{Context, Result, bail};

use super::LoadStats;
use super::arith::{
    checked_direct_revenue_add_i128, direct_sidecar_revenue_overflow, tpch_u64_to_f64,
};
use super::encode::{
    encode_direct_tbl_row, read_direct_decimal_i64, read_direct_i32, read_direct_one_byte_text,
};
use super::loader::{tpch_progress_enabled, tpch_progress_pool_stats_enabled};
use super::sidecars_q2_q5::{
    TpchQ2BuildState, TpchQ3BuildState, TpchQ4BuildState, TpchQ5BuildState,
};
use super::sidecars_q7_q10::{
    TpchQ7BuildState, TpchQ8BuildState, TpchQ9BuildState, TpchQ10BuildState,
};
use super::sidecars_q11_q15::{
    TpchQ11BuildState, TpchQ12BuildState, TpchQ13BuildState, TpchQ14BuildState, TpchQ15BuildState,
};
use super::sidecars_q16_q18::{TpchQ16BuildState, TpchQ17BuildState, TpchQ18BuildState};
use super::sidecars_q19_q21::{TpchQ19BuildState, TpchQ20BuildState, TpchQ21BuildState};
use super::{
    DIRECT_Q1_SHIPDATE_CUTOFF_1998_09_02, DIRECT_Q6_DISCOUNT_MAX, DIRECT_Q6_DISCOUNT_MIN,
    DIRECT_Q6_QUANTITY_LIMIT, DIRECT_Q6_SHIPDATE_END_1995_01_01,
    DIRECT_Q6_SHIPDATE_START_1994_01_01,
};

#[cfg(feature = "sql-bench")]
#[allow(
    clippy::too_many_arguments,
    reason = "direct TPC-H load wires independent sidecar states without heap boxing"
)]
pub(crate) fn load_ultrasql_table_direct(
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
                    tpch_u64_to_f64(total) / elapsed
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
        tpch_u64_to_f64(total) / elapsed
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
pub(crate) fn insert_direct_payload_batch(
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
pub(crate) fn direct_payload_prefix(payload: &[u8]) -> String {
    let mut out = String::with_capacity(payload.len().min(32) * 2);
    for byte in payload.iter().take(32) {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

#[cfg(feature = "sql-bench")]
pub(crate) fn push_direct_q1_columns(
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
pub(crate) fn add_direct_q1_summary_row(
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
pub(crate) fn direct_q1_summary_overflow() -> anyhow::Error {
    anyhow::anyhow!("TPC-H Q1 summary overflow")
}

#[cfg(feature = "sql-bench")]
pub(crate) fn checked_direct_q1_add_i64(left: i64, right: i64) -> Result<i64> {
    left.checked_add(right)
        .ok_or_else(direct_q1_summary_overflow)
}

#[cfg(feature = "sql-bench")]
pub(crate) fn checked_direct_q1_sub(left: i64, right: i64) -> Result<i64> {
    left.checked_sub(right)
        .ok_or_else(direct_q1_summary_overflow)
}

#[cfg(feature = "sql-bench")]
pub(crate) fn checked_direct_q1_add_i128(left: i128, right: i128) -> Result<i128> {
    left.checked_add(right)
        .ok_or_else(direct_q1_summary_overflow)
}

#[cfg(feature = "sql-bench")]
pub(crate) fn checked_direct_q1_mul_i128(left: i128, right: i128) -> Result<i128> {
    left.checked_mul(right)
        .ok_or_else(direct_q1_summary_overflow)
}
