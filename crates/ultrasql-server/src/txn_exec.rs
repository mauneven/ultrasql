//! Transaction execution glue and row-locking helpers.
//!
//! Moved verbatim from the crate root; behavior unchanged.
use super::*;

/// Decode a single column out of an encoded heap-tuple payload and
/// return its value as an `i64` key.
///
/// `schema` is the relation's full schema; `col_idx` is the 0-based
/// position of the key column inside that schema; `widen_i32` is
/// `true` for `Int32` columns (the value is sign-extended to `i64`)
/// and `false` for `Int64`. `Value::Null` returns `None` so the
/// caller can decide what to do — the CREATE INDEX build path
/// currently skips NULL keys (PostgreSQL semantics for non-`INCLUDE`
/// b-tree indexes).
///
/// Returning `Result<Option<i64>, ServerError>` keeps NULL handling
/// at the call site; using a panic / sentinel value would conflate
/// "schema mismatch" with "missing value", which the catalog wants
/// to keep distinct.
/// Build a PostgreSQL `NoticeResponse` carrying a `WARNING` with the
/// given SQLSTATE and human-readable text.
///
/// `NoticeResponse` is shaped exactly like `ErrorResponse` on the wire
/// (an `'N'` tag instead of `'E'`); a libpq client routes notices to a
/// callback rather than aborting the operation. UltraSQL emits notices
/// where PostgreSQL would emit them — most importantly for
/// `BEGIN`-inside-tx, `COMMIT`-outside-tx, and `ROLLBACK`-outside-tx so
/// drivers see the same behaviour they expect from PostgreSQL.
pub(crate) fn notice_warning(sqlstate: &str, message: &str) -> BackendMessage {
    BackendMessage::NoticeResponse {
        fields: vec![
            (b'S', "WARNING".to_string()),
            (b'C', sqlstate.to_string()),
            (b'M', message.to_string()),
        ],
    }
}

pub(crate) struct RunPlanInTxnArgs<'a> {
    pub(crate) plan: &'a LogicalPlan,
    pub(crate) txn: &'a Transaction,
    pub(crate) catalog_snapshot: Arc<CatalogSnapshot>,
    pub(crate) table_constraints:
        Arc<dashmap::DashMap<ultrasql_core::Oid, Arc<TableRuntimeConstraints>>>,
    pub(crate) sequences: Arc<dashmap::DashMap<String, Arc<ultrasql_storage::sequence::Sequence>>>,
    pub(crate) sequence_owners: Arc<dashmap::DashMap<String, String>>,
    pub(crate) sequence_namespaces: Arc<dashmap::DashMap<String, String>>,
    pub(crate) schemas: Arc<dashmap::DashMap<String, Arc<RuntimeSchema>>>,
    pub(crate) operators: Arc<dashmap::DashMap<String, Arc<RuntimeOperator>>>,
    pub(crate) role_catalog: Arc<auth::InMemoryAuthCatalog>,
    pub(crate) privilege_catalog: Arc<auth::InMemoryPrivilegeCatalog>,
    pub(crate) row_security: Arc<dashmap::DashMap<ultrasql_core::Oid, Arc<TableRowSecurity>>>,
    pub(crate) session_settings: Arc<std::collections::HashMap<String, String>>,
    pub(crate) current_user: String,
    pub(crate) session_user: String,
    pub(crate) persistent_catalog: Arc<PersistentCatalog>,
    pub(crate) time_partitions:
        Arc<dashmap::DashMap<String, Arc<time_partition::TimePartitionRuntime>>>,
    pub(crate) workload_recorder: Arc<workload::WorkloadRecorder>,
    pub(crate) autovacuum_config: AutovacuumConfig,
    pub(crate) logging_config: LoggingConfig,
    pub(crate) wal_archive_config: WalArchiveConfig,
    pub(crate) data_dir: Option<std::path::PathBuf>,
    pub(crate) logical_replication: Arc<replication::LogicalReplicationRuntime>,
    pub(crate) sequence_state: Option<SequenceSessionState>,
    pub(crate) advisory_state: Option<AdvisorySessionState>,
    pub(crate) tables: &'a SampleTables,
    pub(crate) heap: Arc<HeapAccess<BlankPageLoader>>,
    pub(crate) vm: Arc<VisibilityMap>,
    pub(crate) oracle: Arc<TransactionManager>,
    pub(crate) jit: ultrasql_vec::jit::JitConfig,
    pub(crate) cancel_flag: Option<ultrasql_executor::CancelFlag>,
    pub(crate) stream_buf: &'a mut bytes::BytesMut,
    /// When `true`, a large SELECT (body past
    /// [`crate::result_encoder::STREAM_WINDOW_HIGH_WATER_BYTES`]) returns
    /// a streaming handle in `SelectResult::streaming` instead of a fully
    /// buffered body. Set `true` only at the two top-level Simple-Query
    /// dispatch sites; `false` for EXPLAIN ANALYZE, materialized-view
    /// maintenance, and any other nested/local caller that needs a
    /// complete contiguous body. The streaming handle also carries the
    /// autocommit transaction to commit after the drain, so it is only
    /// ever populated when the caller can drive it to completion.
    pub(crate) allow_streaming: bool,
    /// Autocommit transaction to hand to the streaming handle so the
    /// drive loop commits it after the drain. `Some(_)` only on the
    /// top-level autocommit (`Idle`) SELECT path; `None` inside an
    /// explicit transaction block (the handle stays in `TxnState`) and on
    /// every non-streaming caller. Ignored unless `allow_streaming` and
    /// the SELECT body actually streams.
    pub(crate) streaming_commit_txn: Option<Transaction>,
}

/// Run a non-DDL, non-transaction-control plan inside the given
/// transaction and return the assembled wire-message result.
///
/// Owns no state of its own: it captures everything it needs by
/// argument so both the Simple Query and Extended Query paths can call
/// it. The caller is responsible for committing or aborting `txn` based
/// on whether this function returned `Ok` or `Err`.
///
/// `command_id` is taken from `txn.current_command` so each statement
/// inside an explicit transaction sees its own writes via the MVCC
/// `cmin < current_command` rule.
pub(crate) fn run_plan_in_txn(args: RunPlanInTxnArgs<'_>) -> Result<SelectResult, ServerError> {
    let RunPlanInTxnArgs {
        plan,
        txn,
        catalog_snapshot,
        table_constraints,
        sequences,
        sequence_owners,
        sequence_namespaces,
        schemas,
        operators,
        role_catalog,
        privilege_catalog,
        row_security,
        session_settings,
        current_user,
        session_user,
        persistent_catalog,
        time_partitions,
        workload_recorder,
        autovacuum_config,
        logging_config,
        wal_archive_config,
        data_dir,
        logical_replication,
        sequence_state,
        advisory_state,
        tables,
        heap,
        vm,
        oracle,
        jit,
        cancel_flag,
        stream_buf,
        allow_streaming,
        streaming_commit_txn,
    } = args;
    if let Some(result) = try_run_cached_int32_pair_select(
        plan,
        &catalog_snapshot,
        heap.as_ref(),
        &txn.snapshot,
        oracle.as_ref(),
        stream_buf,
    ) {
        return Ok(result);
    }
    let text_options =
        result_encoder::TextEncodingOptions::from_session_settings(session_settings.as_ref());
    record_serializable_predicate_locks(plan, txn, &catalog_snapshot, oracle.as_ref());
    record_serializable_write_conflicts(plan, txn, &catalog_snapshot, oracle.as_ref());
    acquire_simple_lock_rows(
        plan,
        &catalog_snapshot,
        &table_constraints,
        heap.as_ref(),
        oracle.as_ref(),
        txn,
    )?;

    // Mirror the COPY server-file gate: server-LOCAL external-file reads
    // (read_csv/read_parquet/…/sniff_csv) are permitted only for superusers.
    // Computed from the same role_catalog/current_user the lowering uses, so
    // the predicate matches `Session::current_role_is_superuser` exactly.
    let allow_server_files =
        crate::session::role_is_superuser(role_catalog.as_ref(), &current_user);
    let ctx = LowerCtx {
        tables,
        catalog_snapshot,
        table_constraints,
        sequences,
        sequence_owners,
        sequence_namespaces,
        schemas,
        operators,
        role_catalog,
        privilege_catalog,
        row_security,
        session_settings,
        current_user,
        session_user,
        persistent_catalog,
        time_partitions,
        workload_recorder,
        autovacuum_config,
        logging_config,
        wal_archive_config,
        data_dir,
        logical_replication,
        sequence_state,
        advisory_state,
        heap,
        vm,
        snapshot: txn.snapshot.clone(),
        isolation: txn.isolation,
        oracle,
        // Use the *current* effective xid so writes performed inside an
        // active SAVEPOINT carry the subxact xid in their tuple header
        // rather than the parent xid; ROLLBACK TO can then hide them
        // via the standard MVCC visibility rules.
        xid: txn.current_xid(),
        command_id: txn.current_command,
        cte_buffers: std::collections::HashMap::new(),
        jit,
        cancel_flag,
        work_mem: Arc::new(ultrasql_executor::work_mem::WorkMemBudget::new(u64::MAX)),
        profile_operators: false,
        allow_server_files,
    };
    match plan {
        LogicalPlan::Insert { returning, .. } => {
            let mut op = pipeline::lower_query(plan, &ctx)?;
            if returning.is_empty() {
                run_modify_command(op.as_mut(), "INSERT")
            } else {
                result_encoder::run_modify_returning_with_options(
                    op.as_mut(),
                    "INSERT",
                    &text_options,
                )
            }
        }
        LogicalPlan::Update { returning, .. } => {
            let mut op = pipeline::lower_query(plan, &ctx)?;
            if returning.is_empty() {
                run_modify_command(op.as_mut(), "UPDATE")
            } else {
                result_encoder::run_modify_returning_with_options(
                    op.as_mut(),
                    "UPDATE",
                    &text_options,
                )
            }
        }
        LogicalPlan::Delete { returning, .. } => {
            let mut op = pipeline::lower_query(plan, &ctx)?;
            if returning.is_empty() {
                run_modify_command(op.as_mut(), "DELETE")
            } else {
                result_encoder::run_modify_returning_with_options(
                    op.as_mut(),
                    "DELETE",
                    &text_options,
                )
            }
        }
        LogicalPlan::Merge { .. } => {
            let mut op = pipeline::lower_query(plan, &ctx)?;
            run_modify_command(op.as_mut(), "MERGE")
        }
        _ => {
            let op = pipeline::lower_query(plan, &ctx)?;
            if allow_streaming {
                // Top-level Simple-Query SELECT: encode window 0 and
                // decide buffered-vs-streaming empirically. A small body
                // (EOF within the first window) returns today's fully
                // buffered `streamed_body` — byte- and syscall-identical
                // to the legacy path. A large body returns a streaming
                // handle (the still-live operator + window-0 bytes) the
                // async dispatcher drives to drain the rest in bounded
                // windows. `streaming_commit_txn` rides into the handle so
                // the drive loop commits the autocommit txn after the
                // drain (or stays `None` inside an explicit block).
                match result_encoder::begin_streaming_select(
                    op,
                    stream_buf,
                    &text_options,
                    streaming_commit_txn,
                )? {
                    result_encoder::StreamingSelectStart::Buffered(result) => Ok(result),
                    result_encoder::StreamingSelectStart::Streaming {
                        window0,
                        handle,
                        rows,
                    } => Ok(SelectResult {
                        messages: Vec::new(),
                        streamed_body: Some(window0),
                        shared_streamed_body: None,
                        streaming: Some(handle),
                        rows,
                    }),
                }
            } else {
                // Nested / local / EXPLAIN-ANALYZE / maintenance caller:
                // never stream — produce a complete contiguous body via the
                // legacy whole-buffer encoder so `decode_local_result_body`
                // and friends always see whole frames.
                let mut op = op;
                result_encoder::run_select_streamed_with_options(
                    op.as_mut(),
                    stream_buf,
                    &text_options,
                )
            }
        }
    }
}

pub(crate) fn acquire_simple_lock_rows(
    plan: &LogicalPlan,
    catalog_snapshot: &Arc<CatalogSnapshot>,
    table_constraints: &dashmap::DashMap<ultrasql_core::Oid, Arc<TableRuntimeConstraints>>,
    heap: &HeapAccess<BlankPageLoader>,
    oracle: &TransactionManager,
    txn: &Transaction,
) -> Result<(), ServerError> {
    let LogicalPlan::LockRows {
        input,
        strength,
        wait_policy,
        ..
    } = plan
    else {
        return Ok(());
    };
    if *wait_policy != LockWaitPolicy::Wait {
        return Ok(());
    }
    let Some((table, predicate)) = lock_rows_base_filter(input) else {
        return Ok(());
    };
    let Some(entry) = catalog_snapshot.tables.get(&table.to_ascii_lowercase()) else {
        return Ok(());
    };

    let rel = RelationId(entry.oid);
    let mode = row_lock_mode(*strength);
    if let Some(tids) =
        lock_rows_index_tids(predicate, entry, catalog_snapshot, table_constraints, heap)?
    {
        return lock_tuple_ids(&tids, oracle, txn, mode);
    }

    let block_count = heap.block_count(rel).max(entry.n_blocks);
    let codec = RowCodec::new(entry.schema.clone());
    let predicate_eval = predicate.cloned().map(Eval::new);

    for tuple in heap.scan_visible(rel, block_count, &txn.snapshot, oracle) {
        let tuple =
            tuple.map_err(|e| ServerError::Execute(ExecError::TypeMismatch(e.to_string())))?;
        let row = codec
            .decode(&tuple.data)
            .map_err(|e| ServerError::Execute(ExecError::TypeMismatch(e.to_string())))?;
        let matched = match &predicate_eval {
            Some(eval) => match eval
                .eval(&row)
                .map_err(|e| ServerError::Execute(ExecError::TypeMismatch(e.to_string())))?
            {
                Value::Bool(true) => true,
                Value::Bool(false) | Value::Null => false,
                other => {
                    return Err(ServerError::Execute(ExecError::TypeMismatch(format!(
                        "FOR UPDATE predicate returned non-boolean value {other:?}",
                    ))));
                }
            },
            None => true,
        };
        if matched {
            lock_tuple_ids(&[tuple.tid], oracle, txn, mode)?;
        }
    }

    Ok(())
}

pub(crate) fn lock_rows_index_tids(
    predicate: Option<&ScalarExpr>,
    entry: &TableEntry,
    catalog_snapshot: &Arc<CatalogSnapshot>,
    table_constraints: &dashmap::DashMap<ultrasql_core::Oid, Arc<TableRuntimeConstraints>>,
    heap: &HeapAccess<BlankPageLoader>,
) -> Result<Option<Vec<ultrasql_core::TupleId>>, ServerError> {
    let Some(predicate) = predicate else {
        return Ok(None);
    };
    let Some((column, key)) = equality_i64_predicate(predicate) else {
        return Ok(None);
    };
    let Some(attnum) = u16::try_from(column).ok() else {
        return Ok(None);
    };
    let Some(indexes) = catalog_snapshot.indexes_by_table.get(&entry.oid) else {
        return Ok(None);
    };
    let Some(index) = indexes.iter().find(|idx| {
        idx.columns.as_slice() == [attnum]
            && idx.root_block != BlockNumber::INVALID
            && runtime_index_method(table_constraints, entry.oid, idx.oid)
                == LogicalIndexMethod::Btree
    }) else {
        return Ok(None);
    };
    let tree: BTree<BlankPageLoader> = BTree::open(
        Arc::clone(heap.buffer_pool()),
        RelationId::new(index.oid.raw()),
        index.root_block,
    );
    let tids = if index.is_unique {
        tree.lookup::<i64>(key)
            .map(|maybe| maybe.into_iter().collect::<Vec<_>>())
    } else {
        tree.lookup_all::<i64>(key)
    }
    .map_err(|e| ServerError::ddl(format!("FOR UPDATE btree lookup: {e}")))?;
    Ok(Some(tids))
}

pub(crate) fn lock_tuple_ids(
    tids: &[ultrasql_core::TupleId],
    oracle: &TransactionManager,
    txn: &Transaction,
    mode: RowLockMode,
) -> Result<(), ServerError> {
    for tid in tids {
        let acquired = oracle
            .lock_manager
            .try_acquire(LockRequest {
                xid: txn.current_xid(),
                tag: LockTag::Tuple(*tid),
                mode: mode.to_lock_mode(),
            })
            .map_err(|e| ServerError::Execute(ExecError::SerializationFailure(e.to_string())))?;
        if !acquired {
            // serialization_failure (40001) — a concurrent transaction holds
            // a conflicting row lock and this `FOR UPDATE`/`FOR SHARE` request
            // cannot proceed. Mirrors PostgreSQL, which aborts the blocked
            // statement with 40001 so retry-aware clients re-issue the txn.
            return Err(ServerError::Execute(ExecError::SerializationFailure(
                "could not serialize access due to concurrent update".to_string(),
            )));
        }
    }
    Ok(())
}

pub(crate) fn equality_i64_predicate(predicate: &ScalarExpr) -> Option<(usize, i64)> {
    let ScalarExpr::Binary {
        op: BinaryOp::Eq,
        left,
        right,
        ..
    } = predicate
    else {
        return None;
    };
    column_literal_i64(left, right).or_else(|| column_literal_i64(right, left))
}

pub(crate) fn column_literal_i64(
    column: &ScalarExpr,
    literal: &ScalarExpr,
) -> Option<(usize, i64)> {
    let ScalarExpr::Column { index, .. } = column else {
        return None;
    };
    let ScalarExpr::Literal { value, .. } = literal else {
        return None;
    };
    value_i64(value).map(|key| (*index, key))
}

pub(crate) fn value_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Bool(v) => Some(i64::from(*v)),
        Value::Int16(v) => Some(i64::from(*v)),
        Value::Int32(v) => Some(i64::from(*v)),
        Value::Int64(v) => Some(*v),
        _ => None,
    }
}

pub(crate) fn runtime_index_method(
    table_constraints: &dashmap::DashMap<ultrasql_core::Oid, Arc<TableRuntimeConstraints>>,
    table_oid: ultrasql_core::Oid,
    index_oid: ultrasql_core::Oid,
) -> LogicalIndexMethod {
    table_constraints
        .get(&table_oid)
        .and_then(|constraints| constraints.indexes.get(&index_oid).map(|idx| idx.method))
        .unwrap_or(LogicalIndexMethod::Btree)
}

pub(crate) fn logical_index_method_from_name(name: &str) -> LogicalIndexMethod {
    match name {
        "hash" => LogicalIndexMethod::Hash,
        "gin" => LogicalIndexMethod::Gin,
        "gist" => LogicalIndexMethod::Gist,
        "brin" => LogicalIndexMethod::Brin,
        "hnsw" => LogicalIndexMethod::Hnsw,
        "ivfflat" => LogicalIndexMethod::IvfFlat,
        "aggregating" => LogicalIndexMethod::Aggregating,
        _ => LogicalIndexMethod::Btree,
    }
}

pub(crate) fn aggregating_group_key_exprs(
    table: &TableEntry,
    spec: &ultrasql_planner::LogicalAggregatingIndex,
) -> Result<Vec<ScalarExpr>, ServerError> {
    spec.group_columns
        .iter()
        .map(|col| {
            let field = table.schema.field(*col).ok_or_else(|| {
                ServerError::ddl(format!(
                    "aggregating index group column {} missing from table {}",
                    col, table.name
                ))
            })?;
            Ok(ScalarExpr::Column {
                name: field.name.clone(),
                index: *col,
                data_type: field.data_type.clone(),
            })
        })
        .collect()
}

pub(crate) fn hnsw_metric_for_opclass_name(
    opclass: Option<&str>,
) -> Result<HnswMetric, ServerError> {
    match opclass.unwrap_or("vector_l2_ops") {
        "vector_l2_ops" => Ok(HnswMetric::L2),
        "vector_cosine_ops" => Ok(HnswMetric::Cosine),
        "vector_ip_ops" => Ok(HnswMetric::NegativeInnerProduct),
        "vector_l1_ops" => Ok(HnswMetric::L1),
        other => Err(ServerError::ddl(format!(
            "CREATE INDEX USING hnsw: unsupported vector opclass {other}"
        ))),
    }
}

pub(crate) fn ann_dims_and_default_payload(data_type: &DataType) -> Option<(u32, AnnPayloadKind)> {
    match data_type {
        DataType::Vector { dims: Some(dims) } => Some((*dims, AnnPayloadKind::F32)),
        DataType::HalfVec { dims: Some(dims) } => Some((*dims, AnnPayloadKind::Bf16)),
        _ => None,
    }
}

pub(crate) fn ann_payload_option_from_catalog(
    options: &[(String, String)],
) -> Result<Option<AnnPayloadKind>, ServerError> {
    let mut payload = None;
    for (name, value) in options {
        if name == "payload" {
            payload = Some(ann_payload_kind_from_value("rebuild vector ANN", value)?);
        }
    }
    Ok(payload)
}

pub(crate) fn ann_payload_kind_from_value(
    context: &str,
    value: &str,
) -> Result<AnnPayloadKind, ServerError> {
    match value.to_ascii_lowercase().as_str() {
        "f32" | "float32" => Ok(AnnPayloadKind::F32),
        "bf16" | "bfloat16" => Ok(AnnPayloadKind::Bf16),
        "int8" | "i8" => Ok(AnnPayloadKind::Int8),
        other => Err(ServerError::ddl(format!(
            "{context}: unsupported payload {other}; expected f32, bf16, or int8"
        ))),
    }
}

pub(crate) fn ivfflat_options_from_catalog(
    options: &[(String, String)],
) -> Result<(usize, usize, Option<AnnPayloadKind>), ServerError> {
    let mut lists = 100_usize;
    let mut probes = 1_usize;
    let mut payload = None;
    for (name, value) in options {
        match name.as_str() {
            "lists" => lists = parse_positive_ivfflat_catalog_option(name, value)?,
            "probes" => probes = parse_positive_ivfflat_catalog_option(name, value)?,
            "payload" => {
                payload = Some(ann_payload_kind_from_value("rebuild IVFFlat", value)?);
            }
            other => {
                return Err(ServerError::ddl(format!(
                    "rebuild IVFFlat: unsupported option {other}"
                )));
            }
        }
    }
    Ok((lists, probes, payload))
}

pub(crate) fn parse_positive_ivfflat_catalog_option(
    name: &str,
    value: &str,
) -> Result<usize, ServerError> {
    let parsed = value.parse::<usize>().map_err(|_| {
        ServerError::ddl(format!(
            "rebuild IVFFlat: option {name} must be a positive integer"
        ))
    })?;
    if parsed == 0 {
        return Err(ServerError::ddl(format!(
            "rebuild IVFFlat: option {name} must be greater than zero"
        )));
    }
    Ok(parsed)
}

pub(crate) fn unix_timestamp_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_micros().try_into().unwrap_or(u64::MAX))
        .unwrap_or(0)
}

pub(crate) fn lock_rows_base_filter(plan: &LogicalPlan) -> Option<(&str, Option<&ScalarExpr>)> {
    match plan {
        LogicalPlan::Project { input, .. } => lock_rows_base_filter(input),
        LogicalPlan::Filter { input, predicate } => match input.as_ref() {
            LogicalPlan::Scan { table, .. } => Some((table.as_str(), Some(predicate))),
            other => lock_rows_base_filter(other).map(|(table, _)| (table, Some(predicate))),
        },
        LogicalPlan::Scan { table, .. } => Some((table.as_str(), None)),
        _ => None,
    }
}

pub(crate) const fn row_lock_mode(strength: LockStrength) -> RowLockMode {
    match strength {
        LockStrength::Update => RowLockMode::ForUpdate,
        LockStrength::NoKeyUpdate => RowLockMode::ForNoKeyUpdate,
        LockStrength::Share => RowLockMode::ForShare,
        LockStrength::KeyShare => RowLockMode::ForKeyShare,
    }
}
