//! SQL lowering for `ORDER BY hybrid_search(...) DESC LIMIT k`.

use ultrasql_core::{DataType, Value};
use ultrasql_executor::{
    HybridSearch, HybridSearchConfig, HybridSearchWeights, HybridTextSpec, HybridVectorSpec,
    Operator,
};
use ultrasql_planner::{LogicalPlan, ScalarExpr, SortKey};
use ultrasql_vec::kernels::vector::VectorMetric;

use crate::error::ServerError;

use super::LowerCtx;
use super::lower_query::lower_query;
use super::modify::lower_project_columns;
use super::saturate_row_count;

/// Try to lower ranked hybrid search SQL into the existing executor node.
///
/// The accepted shape is:
/// `ORDER BY hybrid_search(text_col, query, vector_col, probe) DESC LIMIT k`.
/// Any normal `WHERE` or metadata predicate remains in the child plan and
/// therefore runs before scoring.
pub(super) fn try_lower_hybrid_search_limit(
    input: &LogicalPlan,
    limit: u64,
    offset: u64,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if offset != 0 || limit == 0 || limit == u64::MAX {
        return Ok(None);
    }
    let limit = saturate_row_count(limit);
    match input {
        LogicalPlan::Sort {
            input: sort_input,
            keys,
        } => lower_hybrid_sorted_input(sort_input, keys, limit, ctx),
        LogicalPlan::Project {
            input: project_input,
            exprs,
            ..
        } => {
            let LogicalPlan::Sort {
                input: sort_input,
                keys,
            } = project_input.as_ref()
            else {
                return Ok(None);
            };
            let Some(search) = lower_hybrid_sorted_input(sort_input, keys, limit, ctx)? else {
                return Ok(None);
            };
            lower_project_columns(search, exprs).map(Some)
        }
        _ => Ok(None),
    }
}

fn lower_hybrid_sorted_input(
    sort_input: &LogicalPlan,
    keys: &[SortKey],
    limit: usize,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    let [key] = keys else {
        return Ok(None);
    };
    if key.asc {
        return Ok(None);
    }
    let Some(spec) = match_hybrid_search_key(&key.expr)? else {
        return Ok(None);
    };
    let child = lower_query(sort_input, ctx)?;
    let schema = child.schema().clone();
    let config = HybridSearchConfig {
        text: Some(HybridTextSpec {
            column: spec.text_column,
            query: spec.query,
        }),
        vector: Some(HybridVectorSpec {
            column: spec.vector_column,
            probe: spec.probe,
            metric: VectorMetric::L2,
        }),
        metadata_filter: None,
        where_predicate: None,
        recency_column: None,
        version_column: None,
        limit,
        weights: HybridSearchWeights::DEFAULT,
    };
    Ok(Some(Box::new(HybridSearch::new(child, schema, config))))
}

#[derive(Debug)]
struct HybridSearchKey {
    text_column: usize,
    query: String,
    vector_column: usize,
    probe: Vec<f32>,
}

fn match_hybrid_search_key(expr: &ScalarExpr) -> Result<Option<HybridSearchKey>, ServerError> {
    let ScalarExpr::FunctionCall { name, args, .. } = expr else {
        return Ok(None);
    };
    if name != "hybrid_search" {
        return Ok(None);
    }
    if args.len() != 4 {
        return Err(ServerError::Unsupported(
            "hybrid_search expects four arguments",
        ));
    }
    let Some(text_column) = match_hybrid_text_column(&args[0]) else {
        return Ok(None);
    };
    let Some(query) = match_text_literal(&args[1]) else {
        return Ok(None);
    };
    let Some(vector_column) = match_dense_vector_column(&args[2]) else {
        return Ok(None);
    };
    let Some(probe) = match_dense_vector_literal(&args[3]) else {
        return Ok(None);
    };
    Ok(Some(HybridSearchKey {
        text_column,
        query,
        vector_column,
        probe,
    }))
}

fn match_hybrid_text_column(expr: &ScalarExpr) -> Option<usize> {
    let ScalarExpr::Column {
        index, data_type, ..
    } = expr
    else {
        return None;
    };
    if matches!(data_type, DataType::Text { .. } | DataType::Jsonb) {
        Some(*index)
    } else {
        None
    }
}

fn match_text_literal(expr: &ScalarExpr) -> Option<String> {
    match expr {
        ScalarExpr::Literal {
            value: Value::Text(text),
            ..
        } => Some(text.clone()),
        _ => None,
    }
}

fn match_dense_vector_column(expr: &ScalarExpr) -> Option<usize> {
    let ScalarExpr::Column {
        index, data_type, ..
    } = expr
    else {
        return None;
    };
    if matches!(
        data_type,
        DataType::Vector { .. } | DataType::HalfVec { .. }
    ) {
        Some(*index)
    } else {
        None
    }
}

fn match_dense_vector_literal(expr: &ScalarExpr) -> Option<Vec<f32>> {
    match expr {
        ScalarExpr::Literal {
            value: Value::Vector(values) | Value::HalfVec(values),
            ..
        } => Some(values.clone()),
        _ => None,
    }
}
