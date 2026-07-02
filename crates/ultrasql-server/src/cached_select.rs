//! Fast-path cached scalar-aggregate and int32-pair SELECT execution.
//!
//! Moved verbatim from the crate root; behavior unchanged.
use ultrasql_vec::column::StringColumn;

use super::*;

/// Result-cache *replay* gate.
///
/// The two `try_run_cached_*` entry points below return memoized results —
/// up to pre-encoded wire bytes — for repeated identical reads over a
/// quiescent table. Replay is correct (entries are MVCC-version-gated and
/// invalidated by writes), but a hit is a cache lookup, not query
/// execution. `ULTRASQL_RESULT_CACHE=off|0|false|no` disables replay so
/// benchmarks can compare real compute across engines; the committed scale
/// sweep publishes cache-off numbers and discloses cache-on numbers
/// separately (see the "Result caches" section of BENCHMARKS.md).
pub(crate) fn result_cache_replay_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        !matches!(
            std::env::var("ULTRASQL_RESULT_CACHE").ok().as_deref(),
            Some("off" | "OFF" | "0" | "false" | "FALSE" | "no" | "NO")
        )
    })
}

pub(crate) fn try_run_cached_int32_pair_select(
    plan: &LogicalPlan,
    catalog_snapshot: &Arc<CatalogSnapshot>,
    heap: &HeapAccess<BlankPageLoader>,
    snapshot: &ultrasql_mvcc::Snapshot,
    oracle: &dyn ultrasql_mvcc::XidStatusOracle,
    stream_buf: &mut bytes::BytesMut,
) -> Option<SelectResult> {
    if !result_cache_replay_enabled() {
        return None;
    }
    let (table, output_schema) = match plan {
        LogicalPlan::Scan { table, schema, .. } => (table.as_str(), schema),
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => {
            let LogicalPlan::Scan { table, .. } = input.as_ref() else {
                return None;
            };
            if exprs.len() != 2 {
                return None;
            }
            let is_identity_pair = exprs.iter().enumerate().all(|(idx, (expr, _name))| {
                matches!(expr, ScalarExpr::Column { index, .. } if *index == idx)
            });
            if !is_identity_pair {
                return None;
            }
            (table.as_str(), schema)
        }
        _ => return None,
    };

    if output_schema.len() != 2
        || output_schema.field_at(0).data_type != ultrasql_core::DataType::Int32
        || output_schema.field_at(1).data_type != ultrasql_core::DataType::Int32
    {
        return None;
    }

    let folded = table.to_ascii_lowercase();
    let entry = catalog_snapshot.tables.get(&folded)?;
    let rel = RelationId(entry.oid);
    // Coherence gate: only serve the shared, RAW-replayed projection to a
    // snapshot that reflects exactly the committed state at this version
    // (writer committed per the same oracle the heap visibility path uses,
    // or the reader is the writer itself).
    let cached = heap.column_cache.get_for_snapshot(rel, snapshot, oracle)?;
    let [Column::Int32(left), Column::Int32(right)] = cached.columns.as_slice() else {
        return None;
    };
    if left.nulls().is_some() || right.nulls().is_some() {
        return None;
    }
    let rows = u64::try_from(left.len()).unwrap_or(u64::MAX);

    if output_schema == &cached.schema
        && let Some(encoded) = cached.cached_int32_pair_select_wire.read().clone()
    {
        return Some(result_encoder::run_shared_preencoded_select_streamed(
            encoded, rows,
        ));
    }

    let result = result_encoder::run_cached_int32_pair_select_streamed(
        output_schema,
        left.data(),
        right.data(),
        stream_buf,
    );
    if output_schema == &cached.schema
        && let Some(body) = result.streamed_body.as_ref()
    {
        let mut slot = cached.cached_int32_pair_select_wire.write();
        if slot.is_none() {
            *slot = Some(Arc::<[u8]>::from(body.as_ref()));
        }
    }
    Some(result)
}

pub(crate) fn try_run_cached_scalar_aggregate_select(
    plan: &LogicalPlan,
    catalog_snapshot: &Arc<CatalogSnapshot>,
    heap: &HeapAccess<BlankPageLoader>,
    snapshot: &ultrasql_mvcc::Snapshot,
    oracle: &dyn ultrasql_mvcc::XidStatusOracle,
    stream_buf: &mut bytes::BytesMut,
) -> Option<SelectResult> {
    if !result_cache_replay_enabled() {
        return None;
    }
    let (aggregate_input, group_by, aggregates, output_schema) = match plan {
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => {
            let passthrough = exprs.iter().enumerate().all(|(idx, (expr, _name))| {
                matches!(expr, ScalarExpr::Column { index, .. } if *index == idx)
            });
            if !passthrough {
                return None;
            }
            let LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                ..
            } = input.as_ref()
            else {
                return None;
            };
            (
                input.as_ref(),
                group_by.as_slice(),
                aggregates.as_slice(),
                schema,
            )
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            schema,
        } => (
            input.as_ref(),
            group_by.as_slice(),
            aggregates.as_slice(),
            schema,
        ),
        _ => return None,
    };

    if !group_by.is_empty() || aggregates.len() != 1 || output_schema.len() != 1 {
        return None;
    }

    let agg = &aggregates[0];
    if agg.distinct {
        return None;
    }

    let (table, predicate) = match aggregate_input {
        LogicalPlan::Scan { table, .. } => (table.as_str(), None),
        LogicalPlan::Filter { input, predicate } => {
            let LogicalPlan::Scan { table, .. } = input.as_ref() else {
                return None;
            };
            (table.as_str(), Some(predicate))
        }
        _ => return None,
    };

    let folded = table.to_ascii_lowercase();
    let entry = catalog_snapshot.tables.get(&folded)?;
    let rel = RelationId(entry.oid);
    // Coherence gate: same quiescent-snapshot + committed-writer requirement
    // as the int32-pair fast path — these aggregates fold the shared
    // projection RAW.
    let cached = heap.column_cache.get_for_snapshot(rel, snapshot, oracle)?;

    let cache_key = build_cached_scalar_wire_key(agg, output_schema, predicate)?;
    if let Some(encoded) = cached
        .cached_scalar_aggregate_wire
        .read()
        .get(&cache_key)
        .cloned()
    {
        return Some(result_encoder::run_shared_preencoded_select_streamed(
            encoded, 1,
        ));
    }

    let result_col = match (agg.func, &agg.arg, predicate) {
        (
            AggregateFunc::Sum,
            Some(ScalarExpr::Column {
                index, data_type, ..
            }),
            None,
        ) => build_cached_sum_column(*index, data_type, &cached.columns)?,
        (
            AggregateFunc::Avg,
            Some(ScalarExpr::Column {
                index, data_type, ..
            }),
            None,
        ) => build_cached_avg_column(*index, data_type, &cached.columns)?,
        (
            AggregateFunc::Sum,
            Some(ScalarExpr::Column {
                index, data_type, ..
            }),
            Some(predicate),
        ) => build_cached_filter_sum_column(*index, data_type, predicate, &cached.columns)?,
        _ => return None,
    };

    let batch = Batch::new([result_col]).ok()?;
    let mut op = MemTableScan::new(output_schema.clone(), vec![batch]);
    let result = result_encoder::run_select_streamed(&mut op, stream_buf).ok()?;
    if let Some(body) = result.streamed_body.as_ref() {
        let mut slot = cached.cached_scalar_aggregate_wire.write();
        slot.entry(cache_key)
            .or_insert_with(|| Arc::<[u8]>::from(body.as_ref()));
    }
    Some(result)
}

pub(crate) fn build_cached_scalar_wire_key(
    agg: &ultrasql_planner::LogicalAggregateExpr,
    output_schema: &ultrasql_core::Schema,
    predicate: Option<&ScalarExpr>,
) -> Option<ultrasql_storage::column_cache::CachedScalarAggregateWireKey> {
    let output_name = output_schema.field_at(0).name.clone();
    match (agg.func, &agg.arg, predicate) {
        (
            AggregateFunc::Sum,
            Some(ScalarExpr::Column {
                index, data_type, ..
            }),
            None,
        ) => Some(
            ultrasql_storage::column_cache::CachedScalarAggregateWireKey::Sum {
                output_name,
                input_type_tag: scalar_input_type_tag(data_type)?,
                sum_col: *index,
            },
        ),
        (
            AggregateFunc::Avg,
            Some(ScalarExpr::Column {
                index, data_type, ..
            }),
            None,
        ) => Some(
            ultrasql_storage::column_cache::CachedScalarAggregateWireKey::Avg {
                output_name,
                input_type_tag: scalar_input_type_tag(data_type)?,
                sum_col: *index,
            },
        ),
        (
            AggregateFunc::Sum,
            Some(ScalarExpr::Column {
                index, data_type, ..
            }),
            Some(expr),
        ) => match data_type {
            ultrasql_core::DataType::Int32 => {
                let (predicate_col, predicate_op, predicate_lit) = extract_int32_col_op_lit(expr)?;
                Some(
                    ultrasql_storage::column_cache::CachedScalarAggregateWireKey::FilterSum {
                        output_name,
                        input_type_tag: 0,
                        sum_col: *index,
                        predicate_col,
                        predicate_op_tag: cmp_op_tag(predicate_op),
                        predicate_lit: i64::from(predicate_lit),
                    },
                )
            }
            ultrasql_core::DataType::Int64 => {
                let (predicate_col, predicate_op, predicate_lit) = extract_int64_col_op_lit(expr)?;
                Some(
                    ultrasql_storage::column_cache::CachedScalarAggregateWireKey::FilterSum {
                        output_name,
                        input_type_tag: 1,
                        sum_col: *index,
                        predicate_col,
                        predicate_op_tag: cmp_op_tag(predicate_op),
                        predicate_lit,
                    },
                )
            }
            _ => None,
        },
        _ => None,
    }
}

pub(crate) fn scalar_input_type_tag(data_type: &ultrasql_core::DataType) -> Option<u8> {
    match data_type {
        ultrasql_core::DataType::Int32 => Some(0),
        ultrasql_core::DataType::Int64 => Some(1),
        _ => None,
    }
}

pub(crate) const fn cmp_op_tag(op: CmpOp) -> u8 {
    match op {
        CmpOp::Eq => 0,
        CmpOp::Ne => 1,
        CmpOp::Lt => 2,
        CmpOp::Le => 3,
        CmpOp::Gt => 4,
        CmpOp::Ge => 5,
    }
}

pub(crate) fn build_cached_sum_column(
    sum_col: usize,
    data_type: &ultrasql_core::DataType,
    columns: &[Column],
) -> Option<Column> {
    match data_type {
        ultrasql_core::DataType::Int32 => {
            let Column::Int32(col) = columns.get(sum_col)? else {
                return None;
            };
            if col.nulls().is_some() {
                return None;
            }
            if col.is_empty() {
                null_int64_column()
            } else {
                Some(Column::Int64(NumericColumn::from_data(vec![
                    sum_i32_widening(col),
                ])))
            }
        }
        ultrasql_core::DataType::Int64 => {
            let Column::Int64(col) = columns.get(sum_col)? else {
                return None;
            };
            if col.nulls().is_some() {
                return None;
            }
            if col.is_empty() {
                null_int64_column()
            } else {
                Some(Column::Int64(NumericColumn::from_data(vec![sum_i64(col)])))
            }
        }
        _ => None,
    }
}

pub(crate) fn build_cached_avg_column(
    sum_col: usize,
    data_type: &ultrasql_core::DataType,
    columns: &[Column],
) -> Option<Column> {
    // AVG over integer input returns `numeric` (PG semantics): exact i128
    // division materialised as decimal text. Empty group is SQL NULL.
    match data_type {
        ultrasql_core::DataType::Int32 => {
            let Column::Int32(col) = columns.get(sum_col)? else {
                return None;
            };
            if col.nulls().is_some() {
                return None;
            }
            if col.is_empty() {
                null_decimal_column()
            } else {
                let count = i64::try_from(col.len()).ok()?;
                let text = ultrasql_executor::avg_int_decimal_text(
                    i128::from(sum_i32_widening(col)),
                    count,
                )?;
                Some(Column::Utf8(StringColumn::from_data(vec![text])))
            }
        }
        ultrasql_core::DataType::Int64 => {
            let Column::Int64(col) = columns.get(sum_col)? else {
                return None;
            };
            if col.nulls().is_some() {
                return None;
            }
            if col.is_empty() {
                null_decimal_column()
            } else {
                let count = i64::try_from(col.len()).ok()?;
                let text =
                    ultrasql_executor::avg_int_decimal_text(i128::from(sum_i64(col)), count)?;
                Some(Column::Utf8(StringColumn::from_data(vec![text])))
            }
        }
        _ => None,
    }
}

pub(crate) fn build_cached_filter_sum_column(
    sum_col: usize,
    data_type: &ultrasql_core::DataType,
    predicate: &ScalarExpr,
    columns: &[Column],
) -> Option<Column> {
    match data_type {
        ultrasql_core::DataType::Int32 => {
            let (pred_col, pred_op, pred_lit) = extract_int32_col_op_lit(predicate)?;
            let (Column::Int32(pred), Column::Int32(sum)) =
                (columns.get(pred_col)?, columns.get(sum_col)?)
            else {
                return None;
            };
            if pred.nulls().is_some() || sum.nulls().is_some() {
                return None;
            }
            if sum.is_empty() {
                return null_int64_column();
            }
            let total = if pred_col == sum_col && matches!(pred_op, CmpOp::Gt) {
                filter_sum_i32_widening_gt(sum.data(), pred_lit)
            } else {
                let mask = cmp_i32_scalar(pred, pred_lit, pred_op);
                sum_i32_widening_with_mask(sum, &mask)
            };
            Some(Column::Int64(NumericColumn::from_data(vec![total])))
        }
        ultrasql_core::DataType::Int64 => {
            let (pred_col, pred_op, pred_lit) = extract_int64_col_op_lit(predicate)?;
            let (Column::Int64(pred), Column::Int64(sum)) =
                (columns.get(pred_col)?, columns.get(sum_col)?)
            else {
                return None;
            };
            if pred.nulls().is_some() || sum.nulls().is_some() {
                return None;
            }
            if sum.is_empty() {
                return null_int64_column();
            }
            let total = if pred_col == sum_col && matches!(pred_op, CmpOp::Gt) {
                filter_sum_i64_gt(sum.data(), pred_lit)
            } else {
                let mask = cmp_i64_scalar(pred, pred_lit, pred_op);
                sum_i64_with_mask(sum, &mask)
            };
            Some(Column::Int64(NumericColumn::from_data(vec![total])))
        }
        _ => None,
    }
}

pub(crate) fn extract_int32_col_op_lit(expr: &ScalarExpr) -> Option<(usize, CmpOp, i32)> {
    let ScalarExpr::Binary {
        op, left, right, ..
    } = expr
    else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (
            ScalarExpr::Column {
                index,
                data_type: ultrasql_core::DataType::Int32,
                ..
            },
            ScalarExpr::Literal {
                value: Value::Int32(lit),
                ..
            },
        ) => Some((*index, binary_op_to_cmp(*op)?, *lit)),
        (
            ScalarExpr::Literal {
                value: Value::Int32(lit),
                ..
            },
            ScalarExpr::Column {
                index,
                data_type: ultrasql_core::DataType::Int32,
                ..
            },
        ) => Some((*index, reverse_binary_op_to_cmp(*op)?, *lit)),
        _ => None,
    }
}

pub(crate) fn extract_int64_col_op_lit(expr: &ScalarExpr) -> Option<(usize, CmpOp, i64)> {
    let ScalarExpr::Binary {
        op, left, right, ..
    } = expr
    else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (
            ScalarExpr::Column {
                index,
                data_type: ultrasql_core::DataType::Int64,
                ..
            },
            ScalarExpr::Literal {
                value: Value::Int64(lit),
                ..
            },
        ) => Some((*index, binary_op_to_cmp(*op)?, *lit)),
        (
            ScalarExpr::Literal {
                value: Value::Int64(lit),
                ..
            },
            ScalarExpr::Column {
                index,
                data_type: ultrasql_core::DataType::Int64,
                ..
            },
        ) => Some((*index, reverse_binary_op_to_cmp(*op)?, *lit)),
        _ => None,
    }
}

pub(crate) fn binary_op_to_cmp(op: BinaryOp) -> Option<CmpOp> {
    match op {
        BinaryOp::Eq => Some(CmpOp::Eq),
        BinaryOp::NotEq => Some(CmpOp::Ne),
        BinaryOp::Lt => Some(CmpOp::Lt),
        BinaryOp::LtEq => Some(CmpOp::Le),
        BinaryOp::Gt => Some(CmpOp::Gt),
        BinaryOp::GtEq => Some(CmpOp::Ge),
        _ => None,
    }
}

pub(crate) fn reverse_binary_op_to_cmp(op: BinaryOp) -> Option<CmpOp> {
    match op {
        BinaryOp::Eq => Some(CmpOp::Eq),
        BinaryOp::NotEq => Some(CmpOp::Ne),
        BinaryOp::Lt => Some(CmpOp::Gt),
        BinaryOp::LtEq => Some(CmpOp::Ge),
        BinaryOp::Gt => Some(CmpOp::Lt),
        BinaryOp::GtEq => Some(CmpOp::Le),
        _ => None,
    }
}

pub(crate) fn null_int64_column() -> Option<Column> {
    let mut nulls = ultrasql_vec::Bitmap::new(1, false);
    nulls.set(0, false);
    NumericColumn::with_nulls(vec![0_i64], nulls)
        .ok()
        .map(Column::Int64)
}

/// Single-row Decimal column carrying SQL `NULL` (Decimal materialises as
/// text). Used for an empty-group `AVG`, which returns `numeric` NULL.
pub(crate) fn null_decimal_column() -> Option<Column> {
    let mut nulls = ultrasql_vec::Bitmap::new(1, false);
    nulls.set(0, false);
    StringColumn::with_nulls(vec![String::new()], nulls)
        .ok()
        .map(Column::Utf8)
}

pub(crate) fn decode_key_column(
    bytes: &[u8],
    schema: &ultrasql_core::Schema,
    col_idx: Option<usize>,
    key_exprs: &[ScalarExpr],
    predicate: Option<&ScalarExpr>,
    method: LogicalIndexMethod,
    encoding: &index_key::IndexKeyEncoding,
) -> Result<Option<i64>, ServerError> {
    let codec = ultrasql_executor::RowCodec::new(schema.clone());
    let row = codec
        .decode(bytes)
        .map_err(|e| ServerError::ddl(format!("CREATE INDEX key decode: {e}")))?;
    if let Some(predicate) = predicate {
        match Eval::new(predicate.clone())
            .eval(&row)
            .map_err(|e| ServerError::ddl(format!("CREATE INDEX partial predicate: {e}")))?
        {
            Value::Bool(true) => {}
            Value::Bool(false) | Value::Null => return Ok(None),
            other => {
                return Err(ServerError::ddl(format!(
                    "CREATE INDEX partial predicate returned {:?}, expected bool",
                    other.data_type()
                )));
            }
        }
    }
    if !key_exprs.is_empty() {
        let [expr] = key_exprs else {
            return Err(ServerError::Unsupported(
                "CREATE INDEX: expression indexes support exactly one key in this wave",
            ));
        };
        let value = Eval::new(expr.clone())
            .eval(&row)
            .map_err(|e| ServerError::ddl(format!("CREATE INDEX expression key: {e}")))?;
        if method == LogicalIndexMethod::Hash {
            return Ok(hash_index_value(&value));
        }
        return encoding.encode_value(&value);
    }
    if matches!(
        encoding,
        index_key::IndexKeyEncoding::CompositeTwoInts { .. }
    ) {
        return encoding.encode_row(&row);
    }
    let col_idx = col_idx.ok_or_else(|| {
        ServerError::ddl("CREATE INDEX key column missing for plain column index")
    })?;
    let value = row.get(col_idx).ok_or_else(|| {
        ServerError::ddl(format!(
            "CREATE INDEX key column {col_idx} missing from decoded row of arity {}",
            row.len()
        ))
    })?;
    if method == LogicalIndexMethod::Hash {
        return Ok(hash_index_value(value));
    }
    encoding.encode_value(value)
}

pub(crate) fn hash_index_value(value: &Value) -> Option<i64> {
    if matches!(value, Value::Null) {
        return None;
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(value, &mut hasher);
    Some(i64::from_ne_bytes(
        std::hash::Hasher::finish(&hasher).to_ne_bytes(),
    ))
}
