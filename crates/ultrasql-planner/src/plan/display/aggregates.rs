//! EXPLAIN-style rendering for aggregation, pivot, and unpivot nodes.
//!
//! Helper bodies split verbatim out of [`super`]'s exhaustive match; output is
//! byte-for-byte identical.

use std::fmt;

use crate::expr::ScalarExpr;

use super::super::logical_plan::LogicalPlan;
use super::super::node_types::{
    AggregateFunc, LogicalAggregateExpr, LogicalPivotAggregate, LogicalPivotValue,
    LogicalUnpivotColumn,
};

pub(super) fn fmt_aggregate(
    input: &LogicalPlan,
    group_by: &[ScalarExpr],
    aggregates: &[LogicalAggregateExpr],
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    out.push_str("Aggregate: group_by=[");
    for (i, g) in group_by.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        let _ = fmt::write(out, format_args!("{g}"));
    }
    out.push_str("] aggs=[");
    for (i, agg) in aggregates.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        let func_name = match agg.func {
            AggregateFunc::CountStar => "count(*)",
            AggregateFunc::Count => "count",
            AggregateFunc::Sum => "sum",
            AggregateFunc::Avg => "avg",
            AggregateFunc::Min => "min",
            AggregateFunc::Max => "max",
            AggregateFunc::BoolAnd => "bool_and",
            AggregateFunc::BoolOr => "bool_or",
            AggregateFunc::StringAgg => "string_agg",
            AggregateFunc::ArrayAgg => "array_agg",
            AggregateFunc::JsonAgg => "json_agg",
            AggregateFunc::StddevSamp => "stddev_samp",
            AggregateFunc::StddevPop => "stddev_pop",
            AggregateFunc::VarSamp => "var_samp",
            AggregateFunc::VarPop => "var_pop",
            AggregateFunc::Corr => "corr",
            AggregateFunc::PercentileCont => "percentile_cont",
            AggregateFunc::PercentileDisc => "percentile_disc",
        };
        if let Some(arg) = &agg.arg {
            let dist = if agg.distinct { "DISTINCT " } else { "" };
            let _ = fmt::write(
                out,
                format_args!("{func_name}({dist}{arg}) AS {}", agg.output_name),
            );
        } else {
            let _ = fmt::write(out, format_args!("{func_name} AS {}", agg.output_name));
        }
    }
    out.push_str("]\n");
    input.display_into(indent + 2, out);
}

pub(super) fn fmt_pivot(
    input: &LogicalPlan,
    group_columns: &[usize],
    pivot_column: usize,
    aggregate: &LogicalPivotAggregate,
    pivot_values: &[LogicalPivotValue],
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let _ = fmt::write(
        out,
        format_args!(
            "Pivot: groups={group_columns:?} pivot_column={pivot_column} func={:?} values=[{}]\n",
            aggregate.func,
            pivot_values
                .iter()
                .map(|value| value.output_name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    );
    input.display_into(indent + 2, out);
}

#[allow(clippy::too_many_arguments)]
pub(super) fn fmt_unpivot(
    input: &LogicalPlan,
    passthrough_columns: &[usize],
    columns: &[LogicalUnpivotColumn],
    name_column: &str,
    value_column: &str,
    include_nulls: bool,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    let _ = fmt::write(
        out,
        format_args!(
            "Unpivot: passthrough={passthrough_columns:?} name={name_column} value={value_column} include_nulls={include_nulls} columns=[{}]\n",
            columns
                .iter()
                .map(|column| column.label.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    );
    input.display_into(indent + 2, out);
}
