//! Checked fixed-point arithmetic helpers shared by the TPC-H direct-load
//! sidecars.
//!
//! These wrap `i64`/`i128` `checked_*` operations with descriptive overflow
//! errors so a malformed `.tbl` row surfaces a clear message instead of a
//! silent wrap. The numeric conversion helpers normalize integers to `f64`
//! for the market-share / percentage result rows.

#[cfg(feature = "sql-bench")]
use anyhow::Result;
#[cfg(any(feature = "pg-runner", feature = "sql-bench"))]
use num_traits::ToPrimitive;

#[cfg(feature = "sql-bench")]
pub(crate) fn direct_sidecar_revenue_overflow() -> anyhow::Error {
    anyhow::anyhow!("TPC-H sidecar revenue overflow")
}

#[cfg(feature = "sql-bench")]
pub(crate) fn checked_direct_revenue_add(left: i64, right: i64) -> Result<i64> {
    left.checked_add(right)
        .ok_or_else(direct_sidecar_revenue_overflow)
}

#[cfg(feature = "sql-bench")]
pub(crate) fn checked_direct_revenue_add_i128(left: i128, right: i128) -> Result<i128> {
    left.checked_add(right)
        .ok_or_else(direct_sidecar_revenue_overflow)
}

#[cfg(feature = "sql-bench")]
pub(crate) fn checked_direct_revenue_sub(left: i64, right: i64) -> Result<i64> {
    left.checked_sub(right)
        .ok_or_else(direct_sidecar_revenue_overflow)
}

#[cfg(feature = "sql-bench")]
pub(crate) fn checked_direct_discounted_revenue(extendedprice: i64, discount: i64) -> Result<i64> {
    let product = checked_direct_discounted_product_i128(extendedprice, discount)?;
    i64::try_from(product / 100).map_err(|_| direct_sidecar_revenue_overflow())
}

#[cfg(feature = "sql-bench")]
pub(crate) fn checked_direct_discounted_revenue_x100(
    extendedprice: i64,
    discount: i64,
) -> Result<i64> {
    let product = checked_direct_discounted_product_i128(extendedprice, discount)?;
    i64::try_from(product).map_err(|_| direct_sidecar_revenue_overflow())
}

#[cfg(feature = "sql-bench")]
pub(crate) fn checked_direct_discounted_revenue_i128(
    extendedprice: i64,
    discount: i64,
) -> Result<i128> {
    Ok(checked_direct_discounted_product_i128(extendedprice, discount)? / 100)
}

#[cfg(feature = "sql-bench")]
pub(crate) fn checked_direct_discounted_product_i128(
    extendedprice: i64,
    discount: i64,
) -> Result<i128> {
    let factor = 100_i64
        .checked_sub(discount)
        .ok_or_else(direct_sidecar_revenue_overflow)?;
    i128::from(extendedprice)
        .checked_mul(i128::from(factor))
        .ok_or_else(direct_sidecar_revenue_overflow)
}

#[cfg(feature = "sql-bench")]
pub(crate) fn checked_direct_scaled_product(left: i64, right: i64) -> Result<i64> {
    let product = i128::from(left)
        .checked_mul(i128::from(right))
        .ok_or_else(direct_sidecar_revenue_overflow)?;
    i64::try_from(product / 100).map_err(|_| direct_sidecar_revenue_overflow())
}

#[cfg(feature = "sql-bench")]
pub(crate) fn direct_sidecar_value_overflow() -> anyhow::Error {
    anyhow::anyhow!("TPC-H sidecar value overflow")
}

#[cfg(feature = "sql-bench")]
pub(crate) fn checked_direct_value_add(left: i64, right: i64) -> Result<i64> {
    left.checked_add(right)
        .ok_or_else(direct_sidecar_value_overflow)
}

#[cfg(feature = "sql-bench")]
pub(crate) fn checked_direct_value_product(left: i64, right: i64) -> Result<i64> {
    let product = i128::from(left)
        .checked_mul(i128::from(right))
        .ok_or_else(direct_sidecar_value_overflow)?;
    i64::try_from(product).map_err(|_| direct_sidecar_value_overflow())
}

#[cfg(feature = "sql-bench")]
pub(crate) fn direct_sidecar_quantity_overflow() -> anyhow::Error {
    anyhow::anyhow!("TPC-H sidecar quantity overflow")
}

#[cfg(feature = "sql-bench")]
pub(crate) fn checked_direct_quantity_add_i64(left: i64, right: i64) -> Result<i64> {
    left.checked_add(right)
        .ok_or_else(direct_sidecar_quantity_overflow)
}

#[cfg(feature = "sql-bench")]
pub(crate) fn checked_direct_quantity_add_i128(left: i128, right: i128) -> Result<i128> {
    left.checked_add(right)
        .ok_or_else(direct_sidecar_quantity_overflow)
}

#[cfg(feature = "sql-bench")]
pub(crate) fn direct_sidecar_count_overflow() -> anyhow::Error {
    anyhow::anyhow!("TPC-H sidecar count overflow")
}

#[cfg(feature = "sql-bench")]
pub(crate) fn checked_direct_count_add_i64(left: i64, right: i64) -> Result<i64> {
    left.checked_add(right)
        .ok_or_else(direct_sidecar_count_overflow)
}

#[cfg(feature = "sql-bench")]
pub(crate) fn q8_i64_to_f64(value: i64) -> f64 {
    value
        .to_string()
        .parse::<f64>()
        .unwrap_or(if value.is_negative() {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        })
}

#[cfg(any(feature = "pg-runner", feature = "sql-bench"))]
pub(crate) fn tpch_u64_to_f64(value: u64) -> f64 {
    value.to_f64().unwrap_or(f64::MAX)
}

#[cfg(feature = "sql-bench")]
pub(crate) fn tpch_i128_to_f64(value: i128) -> f64 {
    value.to_f64().unwrap_or_else(|| {
        if value.is_negative() {
            f64::MIN
        } else {
            f64::MAX
        }
    })
}

#[cfg(feature = "sql-bench")]
pub(crate) fn direct_year_from_date(days: i32) -> i32 {
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
