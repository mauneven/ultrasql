//! Predicate selectivity estimation.
//!
//! Selectivity is the fraction of rows that satisfy a predicate, in `[0.0,
//! 1.0]`. The functions here follow PostgreSQL's simple heuristics for v0.6;
//! they are intended to be replaced by histogram / MCV lookups once the
//! statistics subsystem is complete.
//!
//! ## Design note
//!
//! This module is deliberately `pub(crate)` within the cost subsystem.
//! Callers outside `cost/` should go through [`super::CostModel::estimate`].

use ultrasql_planner::{BinaryOp, ScalarExpr, UnaryOp};

use crate::cost::StatsSource;

// Default selectivities matching PostgreSQL's DEFAULT_EQ_SEL / DEFAULT_RANGE_SEL
// when no statistics are available.
const DEFAULT_RANGE_SEL: f64 = 0.333;
const DEFAULT_LIKE_SEL: f64 = 0.005;
const DEFAULT_UNKNOWN_SEL: f64 = 0.333;

/// Estimate the fraction of rows that satisfy `pred` against an input
/// of `input_rows` rows.
///
/// Returns a value in `[0.0, 1.0]`. When statistics are absent the
/// function falls back to PostgreSQL-compatible default selectivities.
///
/// ## Arguments
///
/// - `pred` — the predicate scalar expression to estimate.
/// - `stats` — the statistics source for the table being filtered.
/// - `table` — case-folded name of the table being scanned; used to look up
///   per-column statistics.
/// - `input_rows` — the estimated row count going into the filter; used for
///   cardinality-aware estimates (not currently used by the simple heuristics
///   but kept for the histogram upgrade path).
pub fn selectivity(
    pred: &ScalarExpr,
    stats: &dyn StatsSource,
    table: &str,
    _input_rows: u64,
) -> f64 {
    clamp(sel_inner(pred, stats, table))
}

/// Internal recursive selectivity computation (unclamped).
fn sel_inner(pred: &ScalarExpr, stats: &dyn StatsSource, table: &str) -> f64 {
    match pred {
        // Column = Literal  →  1.0 / max(n_distinct, 1)
        ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
            ..
        } => column_index(left).or_else(|| column_index(right)).map_or(
            DEFAULT_UNKNOWN_SEL,
            |col_idx| {
                let nd = stats.n_distinct(table, col_idx);
                // n_distinct = 0 means "no stats"; treat as 1 distinct value.
                1.0 / nd.max(1.0)
            },
        ),

        // Column <> Literal  →  1 - eq_selectivity
        ScalarExpr::Binary {
            op: BinaryOp::NotEq,
            left,
            right,
            ..
        } => {
            // Reuse the Eq formula: for a column reference compute 1/n_distinct.
            let eq_sel = column_index(left).or_else(|| column_index(right)).map_or(
                DEFAULT_UNKNOWN_SEL,
                |col_idx| {
                    let nd = stats.n_distinct(table, col_idx);
                    1.0 / nd.max(1.0)
                },
            );
            1.0 - eq_sel
        }

        // Range comparisons  →  0.333
        ScalarExpr::Binary {
            op: BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq,
            ..
        } => DEFAULT_RANGE_SEL,

        // LIKE / ILIKE
        ScalarExpr::Binary {
            op: BinaryOp::Like | BinaryOp::Ilike,
            ..
        } => DEFAULT_LIKE_SEL,

        // NOT LIKE / NOT ILIKE
        ScalarExpr::Binary {
            op: BinaryOp::NotLike | BinaryOp::NotIlike,
            ..
        } => 1.0 - DEFAULT_LIKE_SEL,

        // AND  →  sel(l) * sel(r)  (independence assumption)
        ScalarExpr::Binary {
            op: BinaryOp::And,
            left,
            right,
            ..
        } => sel_inner(left, stats, table) * sel_inner(right, stats, table),

        // OR  →  1 - (1 - sel(l)) * (1 - sel(r))
        ScalarExpr::Binary {
            op: BinaryOp::Or,
            left,
            right,
            ..
        } => {
            let l = sel_inner(left, stats, table);
            let r = sel_inner(right, stats, table);
            (1.0 - l).mul_add(-(1.0 - r), 1.0)
        }

        // NOT  →  1 - sel(e)
        ScalarExpr::Unary {
            op: UnaryOp::Not,
            expr,
            ..
        } => 1.0 - sel_inner(expr, stats, table),

        // IS NULL  →  null_frac(column)
        ScalarExpr::IsNull {
            expr,
            negated: false,
        } => column_index(expr).map_or(DEFAULT_UNKNOWN_SEL, |idx| stats.null_frac(table, idx)),

        // IS NOT NULL  →  1 - null_frac(column)
        ScalarExpr::IsNull {
            expr,
            negated: true,
        } => {
            column_index(expr).map_or(DEFAULT_UNKNOWN_SEL, |idx| 1.0 - stats.null_frac(table, idx))
        }

        // Literal true/false
        ScalarExpr::Literal {
            value: ultrasql_core::Value::Bool(b),
            ..
        } => {
            if *b {
                1.0
            } else {
                0.0
            }
        }

        // Unknown expression → PG-compatible default
        _ => DEFAULT_UNKNOWN_SEL,
    }
}

/// Extract the 0-based column index from a `ScalarExpr::Column`, if present.
const fn column_index(expr: &ScalarExpr) -> Option<usize> {
    if let ScalarExpr::Column { index, .. } = expr {
        Some(*index)
    } else {
        None
    }
}

/// Clamp a selectivity value to `[0.0, 1.0]`.
const fn clamp(v: f64) -> f64 {
    // clamp is not yet const-stable for f64 on stable Rust;
    // the #[allow] suppresses the clippy suggestion until it lands.
    if v < 0.0 {
        0.0
    } else if v > 1.0 {
        1.0
    } else {
        v
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Value};
    use ultrasql_planner::{BinaryOp, ScalarExpr, UnaryOp};

    use super::*;
    use crate::cost::NoStats;

    fn col(idx: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: format!("c{idx}"),
            index: idx,
            data_type: DataType::Int32,
        }
    }

    fn lit_int(v: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            data_type: DataType::Int32,
        }
    }

    fn bin(op: BinaryOp, l: ScalarExpr, r: ScalarExpr) -> ScalarExpr {
        ScalarExpr::Binary {
            op,
            left: Box::new(l),
            right: Box::new(r),
            data_type: DataType::Bool,
        }
    }

    // ------------------------------------------------------------------

    /// The `=` selectivity with `n_distinct` = 0 (no stats) falls back to
    /// 1.0 / max(1, 1) = 1.0.
    #[test]
    fn eq_sel_with_no_stats_is_one_over_one() {
        let expr = bin(BinaryOp::Eq, col(0), lit_int(42));
        let sel = selectivity(&expr, &NoStats, "t", 1000);
        assert!((sel - 1.0).abs() < 1e-9, "expected 1.0, got {sel}");
    }

    /// Range selectivity is always `DEFAULT_RANGE_SEL` (0.333).
    #[test]
    fn range_selectivity_returns_default_constant() {
        for op in [BinaryOp::Lt, BinaryOp::LtEq, BinaryOp::Gt, BinaryOp::GtEq] {
            let expr = bin(op, col(0), lit_int(10));
            let sel = selectivity(&expr, &NoStats, "t", 1000);
            assert!(
                (sel - DEFAULT_RANGE_SEL).abs() < 1e-9,
                "expected {DEFAULT_RANGE_SEL} for {op:?}, got {sel}"
            );
        }
    }

    /// AND selectivity is the product of the individual selectivities
    /// (independence assumption).
    #[test]
    fn and_selectivity_is_product_of_children() {
        let left = bin(BinaryOp::Lt, col(0), lit_int(10)); // 0.333
        let right = bin(BinaryOp::Lt, col(1), lit_int(20)); // 0.333
        let and_expr = bin(BinaryOp::And, left, right);
        let sel = selectivity(&and_expr, &NoStats, "t", 1000);
        let expected = DEFAULT_RANGE_SEL * DEFAULT_RANGE_SEL;
        assert!((sel - expected).abs() < 1e-9, "got {sel}");
    }

    /// NOT inverts selectivity.
    #[test]
    fn not_selectivity_inverts() {
        let inner = bin(BinaryOp::Lt, col(0), lit_int(10)); // 0.333
        let not_expr = ScalarExpr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(inner),
            data_type: DataType::Bool,
        };
        let sel = selectivity(&not_expr, &NoStats, "t", 1000);
        let expected = 1.0 - DEFAULT_RANGE_SEL;
        assert!((sel - expected).abs() < 1e-9, "got {sel}");
    }

    /// IS NULL selectivity uses `null_frac` from stats.
    #[test]
    fn is_null_uses_null_frac() {
        struct NullStats;
        impl StatsSource for NullStats {
            fn row_count(&self, _: &str) -> u64 {
                1000
            }
            fn page_count(&self, _: &str) -> u64 {
                10
            }
            fn null_frac(&self, _: &str, _: usize) -> f64 {
                0.15
            }
            fn n_distinct(&self, _: &str, _: usize) -> f64 {
                50.0
            }
        }
        let expr = ScalarExpr::IsNull {
            expr: Box::new(col(0)),
            negated: false,
        };
        let sel = selectivity(&expr, &NullStats, "t", 1000);
        assert!((sel - 0.15).abs() < 1e-9, "expected 0.15, got {sel}");
    }

    /// LIKE selectivity is `DEFAULT_LIKE_SEL`.
    #[test]
    fn like_selectivity_is_default() {
        let col_str = ScalarExpr::Column {
            name: "name".into(),
            index: 0,
            data_type: ultrasql_core::DataType::Text { max_len: None },
        };
        let lit_str = ScalarExpr::Literal {
            value: Value::Text("%foo%".into()),
            data_type: ultrasql_core::DataType::Text { max_len: None },
        };
        let expr = bin(BinaryOp::Like, col_str, lit_str);
        let sel = selectivity(&expr, &NoStats, "t", 1000);
        assert!((sel - DEFAULT_LIKE_SEL).abs() < 1e-9, "got {sel}");
    }

    /// Literal `true` has selectivity 1.0, literal `false` has 0.0.
    #[test]
    fn literal_bool_selectivity() {
        let t_expr = ScalarExpr::Literal {
            value: Value::Bool(true),
            data_type: DataType::Bool,
        };
        let f_expr = ScalarExpr::Literal {
            value: Value::Bool(false),
            data_type: DataType::Bool,
        };
        assert!((selectivity(&t_expr, &NoStats, "t", 1) - 1.0).abs() < 1e-9);
        assert!((selectivity(&f_expr, &NoStats, "t", 1) - 0.0).abs() < 1e-9);
    }
}
