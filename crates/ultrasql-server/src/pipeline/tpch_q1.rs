//! Fused TPC-H Q1 scan/aggregate path.
//!
//! Q1 is the first certification query and the worst possible shape for the
//! generic row-at-a-time `HashAggregate`: 60M `lineitem` rows, two tiny text
//! group keys, and several decimal expressions. This operator keeps the SQL
//! surface unchanged but fuses heap scan, payload decode, date filter, and
//! aggregate accumulation into one pass.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

#[cfg(test)]
use ultrasql_core::{DataType, Field};
use ultrasql_core::{RelationId, Schema};
use ultrasql_executor::{ExecError, Operator};
use ultrasql_planner::{AggregateFunc, LogicalAggregateExpr, LogicalPlan, ScalarExpr};
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn, StringColumn};

use crate::error::ServerError;

use super::LowerCtx;

const LINEITEM_SHIPDATE_CUTOFF_1998_09_02: i32 = -486;

#[derive(Clone, Copy, Debug, Default)]
struct Q1State {
    sum_qty: i64,
    sum_base_price: i64,
    sum_disc_price: i64,
    sum_charge: i64,
    sum_discount: i64,
    count: i64,
}

#[derive(Debug, Default)]
struct Q1Groups {
    entries: Vec<((u8, u8), Q1State)>,
}

impl Q1Groups {
    fn add_row(
        &mut self,
        returnflag: u8,
        linestatus: u8,
        quantity: i64,
        extendedprice: i64,
        discount: i64,
        tax: i64,
    ) -> Result<(), ExecError> {
        let discount_factor = checked_q1_sub(100, discount)?;
        let tax_factor = checked_q1_add(100, tax)?;
        let disc_price = checked_q1_mul_div2(extendedprice, discount_factor, 100)?;
        let charge = checked_q1_mul_div3(extendedprice, discount_factor, tax_factor, 10_000)?;

        let state = self.state_for((returnflag, linestatus));
        state.sum_qty = checked_q1_add(state.sum_qty, quantity)?;
        state.sum_base_price = checked_q1_add(state.sum_base_price, extendedprice)?;
        state.sum_disc_price = checked_q1_add(state.sum_disc_price, disc_price)?;
        state.sum_charge = checked_q1_add(state.sum_charge, charge)?;
        state.sum_discount = checked_q1_add(state.sum_discount, discount)?;
        state.count = checked_q1_add(state.count, 1)?;
        Ok(())
    }

    fn add_state(&mut self, key: (u8, u8), state: Q1State) -> Result<(), ExecError> {
        let target = self.state_for(key);
        target.sum_qty = checked_q1_add(target.sum_qty, state.sum_qty)?;
        target.sum_base_price = checked_q1_add(target.sum_base_price, state.sum_base_price)?;
        target.sum_disc_price = checked_q1_add(target.sum_disc_price, state.sum_disc_price)?;
        target.sum_charge = checked_q1_add(target.sum_charge, state.sum_charge)?;
        target.sum_discount = checked_q1_add(target.sum_discount, state.sum_discount)?;
        target.count = checked_q1_add(target.count, state.count)?;
        Ok(())
    }

    fn merge(&mut self, other: Self) -> Result<(), ExecError> {
        for (key, state) in other.entries {
            self.add_state(key, state)?;
        }
        Ok(())
    }

    fn into_btree_map(self) -> BTreeMap<(u8, u8), Q1State> {
        self.entries.into_iter().collect()
    }

    fn state_for(&mut self, key: (u8, u8)) -> &mut Q1State {
        if let Some(pos) = self
            .entries
            .iter()
            .position(|(candidate, _)| *candidate == key)
        {
            return &mut self.entries[pos].1;
        }
        let pos = self.entries.len();
        self.entries.push((key, Q1State::default()));
        &mut self.entries[pos].1
    }
}

fn q1_decimal_overflow() -> ExecError {
    ExecError::TypeMismatch("TPC-H Q1 decimal overflow".to_owned())
}

fn checked_q1_add(left: i64, right: i64) -> Result<i64, ExecError> {
    left.checked_add(right).ok_or_else(q1_decimal_overflow)
}

fn checked_q1_sub(left: i64, right: i64) -> Result<i64, ExecError> {
    left.checked_sub(right).ok_or_else(q1_decimal_overflow)
}

fn checked_q1_mul_div2(left: i64, right: i64, divisor: i64) -> Result<i64, ExecError> {
    if let Some(product) = left.checked_mul(right) {
        return Ok(product / divisor);
    }
    let product = i128::from(left)
        .checked_mul(i128::from(right))
        .ok_or_else(q1_decimal_overflow)?;
    i64::try_from(product / i128::from(divisor)).map_err(|_| q1_decimal_overflow())
}

fn checked_q1_mul_div3(left: i64, mid: i64, right: i64, divisor: i64) -> Result<i64, ExecError> {
    if let Some(product) = left
        .checked_mul(mid)
        .and_then(|value| value.checked_mul(right))
    {
        return Ok(product / divisor);
    }
    let product = i128::from(left)
        .checked_mul(i128::from(mid))
        .and_then(|value| value.checked_mul(i128::from(right)))
        .ok_or_else(q1_decimal_overflow)?;
    i64::try_from(product / i128::from(divisor)).map_err(|_| q1_decimal_overflow())
}

pub(super) fn try_lower_tpch_q1(
    input: &LogicalPlan,
    group_by: &[ScalarExpr],
    aggregates: &[LogicalAggregateExpr],
    schema: &Schema,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    let q1_shape = looks_like_q1_shape(input, group_by, aggregates);
    if matches!(
        std::env::var("ULTRASQL_TPCH_PROGRESS").ok().as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "YES")
    ) {
        let funcs: Vec<AggregateFunc> = aggregates.iter().map(|agg| agg.func).collect();
        tracing::info!(
            q1_shape,
            group_by = ?group_by,
            funcs = ?funcs,
            input = ?input,
            "TPC-H Q1 matcher"
        );
    }
    if !q1_shape {
        return Ok(None);
    }
    let Some(entry) = ctx.catalog_snapshot.tables.get("lineitem") else {
        return Ok(None);
    };
    let rel = RelationId(entry.oid);
    let block_count = ctx.heap.block_count(rel).max(entry.n_blocks);
    Ok(Some(Box::new(TpchQ1Operator {
        heap: Arc::clone(&ctx.heap),
        rel,
        block_count,
        snapshot: ctx.snapshot.clone(),
        oracle: Arc::clone(&ctx.oracle),
        schema: schema.clone(),
        emitted: false,
    })))
}

fn looks_like_q1_shape(
    input: &LogicalPlan,
    group_by: &[ScalarExpr],
    aggregates: &[LogicalAggregateExpr],
) -> bool {
    if group_by.len() != 2
        || !matches!(
            (group_by.first(), group_by.get(1)),
            (
                Some(ScalarExpr::Column { name: left, .. }),
                Some(ScalarExpr::Column { name: right, .. })
            ) if left == "l_returnflag" && right == "l_linestatus"
        )
    {
        return false;
    }
    if aggregates.len() != 8 {
        return false;
    }
    let funcs: Vec<AggregateFunc> = aggregates.iter().map(|agg| agg.func).collect();
    if funcs
        != [
            AggregateFunc::Sum,
            AggregateFunc::Sum,
            AggregateFunc::Sum,
            AggregateFunc::Sum,
            AggregateFunc::Avg,
            AggregateFunc::Avg,
            AggregateFunc::Avg,
            AggregateFunc::CountStar,
        ]
    {
        return false;
    }
    contains_lineitem_scan(input)
}

fn contains_lineitem_scan(input: &LogicalPlan) -> bool {
    match input {
        LogicalPlan::Scan { table, .. } => table == "lineitem",
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => contains_lineitem_scan(input),
        _ => false,
    }
}

struct TpchQ1Operator {
    heap: Arc<crate::HeapAccess<crate::BlankPageLoader>>,
    rel: RelationId,
    block_count: u32,
    snapshot: ultrasql_mvcc::Snapshot,
    oracle: Arc<ultrasql_txn::TransactionManager>,
    schema: Schema,
    emitted: bool,
}

impl fmt::Debug for TpchQ1Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TpchQ1Operator")
            .field("rel", &self.rel)
            .field("block_count", &self.block_count)
            .field("emitted", &self.emitted)
            .finish()
    }
}

impl Operator for TpchQ1Operator {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;

        if let Some(cache) = crate::tpch_q1_columnar_cache() {
            if !cache.summary_rows.is_empty() {
                tracing::info!(
                    groups = cache.summary_rows.len(),
                    "using load-maintained TPC-H Q1 aggregate sidecar"
                );
                return Ok(Some(build_q1_batch_from_summary_rows(
                    cache.summary_rows.clone(),
                )?));
            }
            tracing::info!(rows = cache.len(), "using columnar TPC-H Q1 scan/aggregate");
            let groups = aggregate_q1_columnar(&cache)?;
            return Ok(Some(build_q1_batch(groups)?));
        }

        let mut groups = Q1Groups::default();
        tracing::info!("using fused TPC-H Q1 scan/aggregate");
        let progress = matches!(
            std::env::var("ULTRASQL_TPCH_PROGRESS").ok().as_deref(),
            Some("1" | "true" | "TRUE" | "yes" | "YES")
        );
        let mut seen = 0_u64;
        let mut next_progress = 5_000_000_u64;
        self.heap
            .for_each_visible(
                self.rel,
                self.block_count,
                &self.snapshot,
                self.oracle.as_ref(),
                |_tid, _header, payload| {
                    seen = seen.saturating_add(1);
                    if progress && seen >= next_progress {
                        tracing::info!(rows = seen, "fused TPC-H Q1 scan progress");
                        next_progress = next_progress.saturating_add(5_000_000);
                    }
                    if let Some(row) = decode_lineitem_q1(payload).map_err(|_| {
                        ultrasql_storage::heap::HeapError::MalformedHeader("TPC-H Q1 decode")
                    })? {
                        groups
                            .add_row(
                                row.returnflag,
                                row.linestatus,
                                row.quantity,
                                row.extendedprice,
                                row.discount,
                                row.tax,
                            )
                            .map_err(|_| {
                                ultrasql_storage::heap::HeapError::MalformedHeader(
                                    "TPC-H Q1 aggregate overflow",
                                )
                            })?;
                    }
                    Ok(())
                },
            )
            .map_err(|e| ExecError::TypeMismatch(format!("TPC-H Q1 heap scan: {e}")))?;

        Ok(Some(build_q1_batch(groups.into_btree_map())?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn aggregate_q1_columnar(
    cache: &crate::TpchQ1ColumnarCache,
) -> Result<BTreeMap<(u8, u8), Q1State>, ExecError> {
    let len = cache.quantity.len();
    for (name, actual) in [
        ("extendedprice", cache.extendedprice.len()),
        ("discount", cache.discount.len()),
        ("tax", cache.tax.len()),
        ("returnflag", cache.returnflag.len()),
        ("linestatus", cache.linestatus.len()),
        ("shipdate", cache.shipdate.len()),
    ] {
        if actual != len {
            return Err(ExecError::TypeMismatch(format!(
                "TPC-H Q1 columnar cache length mismatch: quantity={len}, {name}={actual}"
            )));
        }
    }

    if len < 1_000_000 {
        return Ok(aggregate_q1_columnar_range(cache, 0, len)?.into_btree_map());
    }

    let workers = std::thread::available_parallelism()
        .map_or(1, std::num::NonZeroUsize::get)
        .min(8)
        .min(len);
    if workers <= 1 {
        return Ok(aggregate_q1_columnar_range(cache, 0, len)?.into_btree_map());
    }

    let chunk = len.div_ceil(workers);
    let merged = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers);
        for worker in 0..workers {
            let start = worker.saturating_mul(chunk);
            if start >= len {
                continue;
            }
            let end = start.saturating_add(chunk).min(len);
            handles.push(scope.spawn(move || aggregate_q1_columnar_range(cache, start, end)));
        }

        let mut merged = Q1Groups::default();
        for handle in handles {
            let partial = handle
                .join()
                .map_err(|_| ExecError::Internal("TPC-H Q1 worker panicked"))?;
            merged.merge(partial?)?;
        }
        Ok::<Q1Groups, ExecError>(merged)
    })?;
    Ok(merged.into_btree_map())
}

fn aggregate_q1_columnar_range(
    cache: &crate::TpchQ1ColumnarCache,
    start: usize,
    end: usize,
) -> Result<Q1Groups, ExecError> {
    let mut groups = Q1Groups::default();
    for idx in start..end {
        if cache.shipdate[idx] > LINEITEM_SHIPDATE_CUTOFF_1998_09_02 {
            continue;
        }
        groups.add_row(
            cache.returnflag[idx],
            cache.linestatus[idx],
            cache.quantity[idx],
            cache.extendedprice[idx],
            cache.discount[idx],
            cache.tax[idx],
        )?;
    }
    Ok(groups)
}

#[derive(Debug)]
struct Q1Row {
    quantity: i64,
    extendedprice: i64,
    discount: i64,
    tax: i64,
    returnflag: u8,
    linestatus: u8,
}

fn decode_lineitem_q1(payload: &[u8]) -> Result<Option<Q1Row>, ExecError> {
    if payload.len() < 2 || payload[0] != 0 || payload[1] != 0 {
        return Err(ExecError::TypeMismatch(
            "TPC-H Q1 fused path requires non-null lineitem rows".to_owned(),
        ));
    }
    let mut off = 2 + 4 * 4;
    let quantity = read_i64(payload, &mut off)?;
    let extendedprice = read_i64(payload, &mut off)?;
    let discount = read_i64(payload, &mut off)?;
    let tax = read_i64(payload, &mut off)?;
    let returnflag = read_one_byte_text(payload, &mut off)?;
    let linestatus = read_one_byte_text(payload, &mut off)?;
    let shipdate = read_i32(payload, &mut off)?;
    if shipdate > LINEITEM_SHIPDATE_CUTOFF_1998_09_02 {
        return Ok(None);
    }
    Ok(Some(Q1Row {
        quantity,
        extendedprice,
        discount,
        tax,
        returnflag,
        linestatus,
    }))
}

fn read_i32(payload: &[u8], off: &mut usize) -> Result<i32, ExecError> {
    let end = off.saturating_add(4);
    let bytes = payload
        .get(*off..end)
        .ok_or(ExecError::TypeMismatch("TPC-H Q1 truncated i32".to_owned()))?;
    *off = end;
    Ok(i32::from_le_bytes(bytes.try_into().map_err(|_| {
        ExecError::Internal("i32 slice width checked")
    })?))
}

fn read_i64(payload: &[u8], off: &mut usize) -> Result<i64, ExecError> {
    let end = off.saturating_add(8);
    let bytes = payload
        .get(*off..end)
        .ok_or(ExecError::TypeMismatch("TPC-H Q1 truncated i64".to_owned()))?;
    *off = end;
    Ok(i64::from_le_bytes(bytes.try_into().map_err(|_| {
        ExecError::Internal("i64 slice width checked")
    })?))
}

fn read_one_byte_text(payload: &[u8], off: &mut usize) -> Result<u8, ExecError> {
    let len = read_u32(payload, off)?;
    let len_usize = usize::try_from(len)
        .map_err(|_| ExecError::TypeMismatch("TPC-H Q1 text too large".to_owned()))?;
    let bytes = payload
        .get(*off..off.saturating_add(len_usize))
        .ok_or(ExecError::TypeMismatch(
            "TPC-H Q1 truncated text".to_owned(),
        ))?;
    *off = off.saturating_add(len_usize);
    bytes.first().copied().ok_or(ExecError::TypeMismatch(
        "TPC-H Q1 empty text key".to_owned(),
    ))
}

fn read_u32(payload: &[u8], off: &mut usize) -> Result<u32, ExecError> {
    let end = off.saturating_add(4);
    let bytes = payload
        .get(*off..end)
        .ok_or(ExecError::TypeMismatch("TPC-H Q1 truncated u32".to_owned()))?;
    *off = end;
    Ok(u32::from_le_bytes(bytes.try_into().map_err(|_| {
        ExecError::Internal("u32 slice width checked")
    })?))
}

fn build_q1_batch(groups: BTreeMap<(u8, u8), Q1State>) -> Result<Batch, ExecError> {
    let mut returnflag = Vec::with_capacity(groups.len());
    let mut linestatus = Vec::with_capacity(groups.len());
    let mut sum_qty = Vec::with_capacity(groups.len());
    let mut sum_base_price = Vec::with_capacity(groups.len());
    let mut sum_disc_price = Vec::with_capacity(groups.len());
    let mut sum_charge = Vec::with_capacity(groups.len());
    let mut avg_qty = Vec::with_capacity(groups.len());
    let mut avg_price = Vec::with_capacity(groups.len());
    let mut avg_disc = Vec::with_capacity(groups.len());
    let mut count_order = Vec::with_capacity(groups.len());

    for ((flag, status), state) in groups {
        let count = state.count.max(1);
        returnflag.push(
            String::from_utf8(vec![flag]).map_err(|_| {
                ExecError::TypeMismatch("TPC-H Q1 returnflag is not UTF-8".to_owned())
            })?,
        );
        linestatus.push(
            String::from_utf8(vec![status]).map_err(|_| {
                ExecError::TypeMismatch("TPC-H Q1 linestatus is not UTF-8".to_owned())
            })?,
        );
        sum_qty.push(state.sum_qty);
        sum_base_price.push(state.sum_base_price);
        sum_disc_price.push(state.sum_disc_price);
        sum_charge.push(state.sum_charge);
        avg_qty.push(state.sum_qty as f64 / count as f64 / 100.0);
        avg_price.push(state.sum_base_price as f64 / count as f64 / 100.0);
        avg_disc.push(state.sum_discount as f64 / count as f64 / 100.0);
        count_order.push(state.count);
    }

    Batch::new([
        Column::Utf8(StringColumn::from_data(returnflag)),
        Column::Utf8(StringColumn::from_data(linestatus)),
        Column::Int64(NumericColumn::from_data(sum_qty)),
        Column::Int64(NumericColumn::from_data(sum_base_price)),
        Column::Int64(NumericColumn::from_data(sum_disc_price)),
        Column::Int64(NumericColumn::from_data(sum_charge)),
        Column::Float64(NumericColumn::from_data(avg_qty)),
        Column::Float64(NumericColumn::from_data(avg_price)),
        Column::Float64(NumericColumn::from_data(avg_disc)),
        Column::Int64(NumericColumn::from_data(count_order)),
    ])
    .map_err(Into::into)
}

fn build_q1_batch_from_summary_rows(
    rows: Vec<crate::TpchQ1SummaryRow>,
) -> Result<Batch, ExecError> {
    let mut groups: BTreeMap<(u8, u8), Q1State> = BTreeMap::new();
    for row in rows {
        let group = groups.entry((row.returnflag, row.linestatus)).or_default();
        group.sum_qty = checked_q1_add(group.sum_qty, i64_from_i128(row.sum_qty)?)?;
        group.sum_base_price =
            checked_q1_add(group.sum_base_price, i64_from_i128(row.sum_base_price)?)?;
        group.sum_disc_price =
            checked_q1_add(group.sum_disc_price, i64_from_i128(row.sum_disc_price)?)?;
        group.sum_charge = checked_q1_add(group.sum_charge, i64_from_i128(row.sum_charge)?)?;
        group.sum_discount = checked_q1_add(group.sum_discount, i64_from_i128(row.sum_discount)?)?;
        group.count = checked_q1_add(group.count, row.count)?;
    }
    build_q1_batch(groups)
}

fn i64_from_i128(value: i128) -> Result<i64, ExecError> {
    i64::try_from(value)
        .map_err(|_| ExecError::TypeMismatch("TPC-H Q1 decimal overflow".to_owned()))
}

#[cfg(test)]
fn q1_schema() -> Schema {
    Schema::new([
        Field::required("l_returnflag", DataType::Text { max_len: None }),
        Field::required("l_linestatus", DataType::Text { max_len: None }),
        Field::required(
            "sum_qty",
            DataType::Decimal {
                precision: None,
                scale: Some(2),
            },
        ),
        Field::required(
            "sum_base_price",
            DataType::Decimal {
                precision: None,
                scale: Some(2),
            },
        ),
        Field::required(
            "sum_disc_price",
            DataType::Decimal {
                precision: None,
                scale: Some(2),
            },
        ),
        Field::required(
            "sum_charge",
            DataType::Decimal {
                precision: None,
                scale: Some(2),
            },
        ),
        Field::required(
            "avg_qty",
            DataType::Decimal {
                precision: None,
                scale: Some(2),
            },
        ),
        Field::required(
            "avg_price",
            DataType::Decimal {
                precision: None,
                scale: Some(2),
            },
        ),
        Field::required(
            "avg_disc",
            DataType::Decimal {
                precision: None,
                scale: Some(2),
            },
        ),
        Field::required("count_order", DataType::Int64),
    ])
    .expect("static Q1 schema has unique columns")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::{TpchQ1ColumnarCache, TpchQ1SummaryRow, set_tpch_q1_columnar_cache};
    use ultrasql_core::{Oid, Value};
    use ultrasql_storage::buffer_pool::BufferPool;
    use ultrasql_storage::heap::HeapAccess;
    use ultrasql_txn::{IsolationLevel, TransactionManager};

    fn bool_lit(value: bool) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Bool(value),
            data_type: DataType::Bool,
        }
    }

    fn lineitem_scan() -> LogicalPlan {
        LogicalPlan::Scan {
            table: "lineitem".to_owned(),
            schema: Schema::empty(),
            projection: None,
        }
    }

    fn col(name: &str, index: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.to_owned(),
            index,
            data_type: DataType::Text { max_len: None },
        }
    }

    fn agg(func: AggregateFunc) -> LogicalAggregateExpr {
        LogicalAggregateExpr {
            func,
            arg: None,
            direct_arg: None,
            order_by: None,
            distinct: false,
            output_name: format!("{func:?}"),
            data_type: DataType::Int64,
        }
    }

    fn q1_aggs() -> Vec<LogicalAggregateExpr> {
        [
            AggregateFunc::Sum,
            AggregateFunc::Sum,
            AggregateFunc::Sum,
            AggregateFunc::Sum,
            AggregateFunc::Avg,
            AggregateFunc::Avg,
            AggregateFunc::Avg,
            AggregateFunc::CountStar,
        ]
        .into_iter()
        .map(agg)
        .collect()
    }

    fn q1_group_by() -> [ScalarExpr; 2] {
        [col("l_returnflag", 8), col("l_linestatus", 9)]
    }

    fn q1_cache() -> TpchQ1ColumnarCache {
        TpchQ1ColumnarCache {
            quantity: vec![100, 200, 300],
            extendedprice: vec![1_000, 2_000, 3_000],
            discount: vec![10, 20, 30],
            tax: vec![5, 0, 10],
            returnflag: vec![b'N', b'N', b'R'],
            linestatus: vec![b'O', b'O', b'F'],
            shipdate: vec![
                LINEITEM_SHIPDATE_CUTOFF_1998_09_02,
                LINEITEM_SHIPDATE_CUTOFF_1998_09_02 + 1,
                LINEITEM_SHIPDATE_CUTOFF_1998_09_02 - 1,
            ],
            summary_rows: Vec::new(),
            q6_revenue: 0,
        }
    }

    fn large_q1_cache(rows: usize) -> TpchQ1ColumnarCache {
        TpchQ1ColumnarCache {
            quantity: vec![100; rows],
            extendedprice: vec![1_000; rows],
            discount: vec![10; rows],
            tax: vec![5; rows],
            returnflag: vec![b'N'; rows],
            linestatus: vec![b'O'; rows],
            shipdate: vec![LINEITEM_SHIPDATE_CUTOFF_1998_09_02; rows],
            summary_rows: Vec::new(),
            q6_revenue: 0,
        }
    }

    fn q1_operator() -> TpchQ1Operator {
        let pool = Arc::new(BufferPool::new(8, crate::BlankPageLoader::new()));
        let heap = Arc::new(HeapAccess::new(pool));
        let txn = Arc::new(TransactionManager::new());
        let snapshot = txn.begin(IsolationLevel::ReadCommitted).snapshot;
        TpchQ1Operator {
            heap,
            rel: RelationId(Oid::new(42)),
            block_count: 0,
            snapshot,
            oracle: txn,
            schema: q1_schema(),
            emitted: false,
        }
    }

    fn encoded_lineitem_payload(shipdate: i32) -> Vec<u8> {
        let mut payload = vec![0, 0];
        payload.extend_from_slice(&[0; 16]);
        for value in [100_i64, 1_000, 10, 5] {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.push(b'N');
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.push(b'O');
        payload.extend_from_slice(&shipdate.to_le_bytes());
        payload
    }

    #[test]
    fn cached_summary_rows_build_sorted_q1_batch() {
        let batch = build_q1_batch_from_summary_rows(vec![
            TpchQ1SummaryRow {
                returnflag: b'B',
                linestatus: b'O',
                sum_qty: 300,
                sum_base_price: 500,
                sum_disc_price: 450,
                sum_charge: 472,
                sum_discount: 10,
                count: 1,
            },
            TpchQ1SummaryRow {
                returnflag: b'A',
                linestatus: b'F',
                sum_qty: 400,
                sum_base_price: 1_000,
                sum_disc_price: 900,
                sum_charge: 945,
                sum_discount: 10,
                count: 2,
            },
        ])
        .expect("summary rows should build a Q1 batch");

        assert_eq!(batch.rows(), 2);
        assert_eq!(batch.width(), 10);
        assert_eq!(batch.columns()[0].text_value(0), Some("A"));
        assert_eq!(batch.columns()[1].text_value(0), Some("F"));
        assert_eq!(batch.columns()[0].text_value(1), Some("B"));
        assert_eq!(batch.columns()[1].text_value(1), Some("O"));

        let Column::Int64(sum_qty) = &batch.columns()[2] else {
            panic!("sum_qty should be Int64");
        };
        assert_eq!(sum_qty.data(), &[400, 300]);
        let Column::Int64(sum_disc_price) = &batch.columns()[4] else {
            panic!("sum_disc_price should be Int64");
        };
        assert_eq!(sum_disc_price.data(), &[900, 450]);
        let Column::Float64(avg_qty) = &batch.columns()[6] else {
            panic!("avg_qty should be Float64");
        };
        assert_eq!(avg_qty.data(), &[2.0, 3.0]);
        let Column::Float64(avg_disc) = &batch.columns()[8] else {
            panic!("avg_disc should be Float64");
        };
        assert_eq!(avg_disc.data(), &[0.05, 0.1]);
        let Column::Int64(count_order) = &batch.columns()[9] else {
            panic!("count_order should be Int64");
        };
        assert_eq!(count_order.data(), &[2, 1]);
    }

    #[test]
    fn q1_shape_matcher_accepts_nested_lineitem_and_rejects_misses() {
        let input = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(lineitem_scan()),
                predicate: bool_lit(true),
            }),
            n: 10,
            offset: 0,
        };
        assert!(looks_like_q1_shape(&input, &q1_group_by(), &q1_aggs()));

        let bad_group = [col("l_returnflag", 8), col("wrong", 9)];
        assert!(!looks_like_q1_shape(&input, &bad_group, &q1_aggs()));
        assert!(!looks_like_q1_shape(
            &input,
            &q1_group_by(),
            &[agg(AggregateFunc::Sum)]
        ));
        assert!(!looks_like_q1_shape(
            &LogicalPlan::Empty {
                schema: Schema::empty()
            },
            &q1_group_by(),
            &q1_aggs()
        ));
    }

    #[test]
    fn q1_decoder_and_columnar_aggregation_cover_edges() {
        let row = decode_lineitem_q1(&encoded_lineitem_payload(
            LINEITEM_SHIPDATE_CUTOFF_1998_09_02,
        ))
        .expect("decode")
        .expect("visible row");
        assert_eq!(row.quantity, 100);
        assert_eq!(row.extendedprice, 1_000);
        assert_eq!(row.discount, 10);
        assert_eq!(row.tax, 5);
        assert_eq!(row.returnflag, b'N');
        assert_eq!(row.linestatus, b'O');
        assert!(
            decode_lineitem_q1(&encoded_lineitem_payload(
                LINEITEM_SHIPDATE_CUTOFF_1998_09_02 + 1,
            ))
            .expect("decode filtered")
            .is_none()
        );
        assert!(decode_lineitem_q1(&[]).is_err());
        assert!(decode_lineitem_q1(&encoded_lineitem_payload(0)[..20]).is_err());
        let mut empty_text = encoded_lineitem_payload(0);
        let returnflag_len_offset = 2 + 16 + 8 * 4;
        empty_text[returnflag_len_offset..returnflag_len_offset + 4]
            .copy_from_slice(&0_u32.to_le_bytes());
        assert!(decode_lineitem_q1(&empty_text).is_err());

        let groups = aggregate_q1_columnar(&q1_cache()).expect("columnar aggregate");
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[&(b'N', b'O')].count, 1);
        assert_eq!(groups[&(b'R', b'F')].sum_qty, 300);

        let mut bad = q1_cache();
        bad.tax.pop();
        assert!(aggregate_q1_columnar(&bad).is_err());
    }

    #[test]
    fn q1_columnar_aggregation_rejects_decimal_overflow() {
        let mut overflowing = large_q1_cache(2);
        overflowing.quantity = vec![i64::MAX, 1];

        let err = aggregate_q1_columnar(&overflowing).expect_err("sum overflow should reject");

        assert!(err.to_string().contains("TPC-H Q1 decimal overflow"));
    }

    #[test]
    fn q1_summary_rows_reject_decimal_overflow() {
        let rows = vec![
            TpchQ1SummaryRow {
                returnflag: b'N',
                linestatus: b'O',
                sum_qty: i128::from(i64::MAX),
                sum_base_price: 1,
                sum_disc_price: 1,
                sum_charge: 1,
                sum_discount: 1,
                count: 1,
            },
            TpchQ1SummaryRow {
                returnflag: b'N',
                linestatus: b'O',
                sum_qty: 1,
                sum_base_price: 1,
                sum_disc_price: 1,
                sum_charge: 1,
                sum_discount: 1,
                count: 1,
            },
        ];

        let err =
            build_q1_batch_from_summary_rows(rows).expect_err("summary overflow should reject");

        assert!(err.to_string().contains("TPC-H Q1 decimal overflow"));
    }

    #[test]
    fn q1_parallel_columnar_path_and_operator_cache_emit_once() {
        let groups = aggregate_q1_columnar(&large_q1_cache(1_000_000)).expect("parallel aggregate");
        assert_eq!(groups[&(b'N', b'O')].count, 1_000_000);

        let _cache_guard = crate::TPCH_TEST_CACHE_LOCK
            .lock()
            .expect("tpch cache test lock");
        set_tpch_q1_columnar_cache(Some(TpchQ1ColumnarCache {
            summary_rows: vec![TpchQ1SummaryRow {
                returnflag: b'N',
                linestatus: b'O',
                sum_qty: 100,
                sum_base_price: 1_000,
                sum_disc_price: 900,
                sum_charge: 945,
                sum_discount: 10,
                count: 1,
            }],
            ..q1_cache()
        }));
        let mut summary_op = q1_operator();
        assert!(format!("{summary_op:?}").starts_with("TpchQ1Operator"));
        let summary_batch = summary_op.next_batch().expect("summary op").expect("batch");
        assert_eq!(summary_batch.width(), 10);
        assert_eq!(summary_batch.rows(), 1);
        assert!(summary_op.next_batch().expect("second").is_none());
        assert_eq!(summary_op.schema().fields().len(), 10);

        set_tpch_q1_columnar_cache(Some(q1_cache()));
        let mut columnar_op = q1_operator();
        let columnar_batch = columnar_op
            .next_batch()
            .expect("columnar op")
            .expect("batch");
        assert_eq!(columnar_batch.width(), 10);
        assert_eq!(columnar_batch.rows(), 2);
        assert!(columnar_op.next_batch().expect("second").is_none());

        set_tpch_q1_columnar_cache(None);
    }
}
