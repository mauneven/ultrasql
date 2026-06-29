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
    // The int32-pair fast path serves the shared column-cache projection
    // without descending into the executor, so it must NOT short-circuit a
    // SERIALIZABLE read: returning here would skip the
    // `record_serializable_predicate_locks` call below and drop the SIREAD
    // predicate lock for this read, missing a read-write conflict (an SSI
    // serialization hole). Under SERIALIZABLE we fall through to the locking
    // path — correctness over the fast path. READ COMMITTED / REPEATABLE READ
    // record no predicate locks, so the fast path is safe there. Mirrors the
    // `ReadCommitted`-only guard in
    // `can_use_cached_scalar_aggregate_in_explicit_txn`.
    if txn.isolation != ultrasql_txn::IsolationLevel::Serializable
        && let Some(result) = try_run_cached_int32_pair_select(
            plan,
            &catalog_snapshot,
            heap.as_ref(),
            &txn.snapshot,
            oracle.as_ref(),
            stream_buf,
        )
    {
        return Ok(result);
    }
    let text_options =
        result_encoder::TextEncodingOptions::from_session_settings(session_settings.as_ref());
    record_serializable_predicate_locks(plan, txn, &catalog_snapshot, oracle.as_ref());
    record_serializable_write_conflicts(plan, txn, &catalog_snapshot, oracle.as_ref());
    // Acquire the real row locks demanded by a SELECT ... FOR UPDATE /
    // SHARE clause. For SKIP LOCKED this returns a rewritten plan that
    // yields only the rows this txn actually grabbed; otherwise the
    // original plan is executed unchanged.
    let rewritten_lock_plan = acquire_simple_lock_rows(
        plan,
        &catalog_snapshot,
        &table_constraints,
        heap.as_ref(),
        oracle.as_ref(),
        txn,
    )?;
    let plan: &LogicalPlan = rewritten_lock_plan.as_ref().unwrap_or(plan);

    // Mirror the COPY server-file gate: server-LOCAL external-file reads
    // (read_csv/read_parquet/…/sniff_csv) are permitted only for superusers.
    // Computed from the same role_catalog/current_user the lowering uses, so
    // the predicate matches `Session::current_role_is_superuser` exactly.
    let allow_server_files =
        crate::session::role_is_superuser(role_catalog.as_ref(), &current_user);
    // Arm the per-statement work-memory budget from the session's `work_mem`
    // GUC (default 64 MiB) *before* `session_settings` is moved into the
    // `LowerCtx`. Once a sort / GROUP BY / hash-join working set crosses this
    // budget the executor spills to disk instead of growing the heap without
    // bound (OOM DoS).
    let work_mem = crate::session::work_mem_budget_from_settings(session_settings.as_ref());
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
        // Row locks are owned by the TOP-LEVEL xid (released by
        // `release_all` only at txn end, never at `ROLLBACK TO`), so a lock
        // taken inside a savepoint that is later rolled back still releases
        // at commit and a re-lock of the same row in a later statement is a
        // no-op rather than a self-block.
        lock_xid: txn.xid,
        command_id: txn.current_command,
        cte_buffers: std::collections::HashMap::new(),
        jit,
        cancel_flag,
        work_mem,
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

/// One base relation that a `SELECT ... FOR UPDATE / SHARE` must lock,
/// together with the single-relation predicate conjunct (if any) that
/// restricts which of its rows are eligible.
pub(crate) struct LockTarget<'a> {
    /// Lower-cased relation name as written in the plan.
    pub(crate) table: String,
    /// The single-relation filter predicate, if one applies. `None`
    /// means every visible row of the relation is locked.
    pub(crate) predicate: Option<&'a ScalarExpr>,
}

/// Acquire the real row-level locks demanded by a `SELECT ... FOR UPDATE /
/// FOR SHARE / FOR NO KEY UPDATE / FOR KEY SHARE` clause.
///
/// This is the pre-execution lock pass: it runs after the SSI predicate
/// bookkeeping and before the result pipeline is lowered, resolving the
/// base-relation tuple ids each locked relation contributes and acquiring
/// the mapped tuple lock under the session transaction's xid (held until
/// commit/rollback, released by `release_all(xid)` at txn end).
///
/// # Plan-shape support
///
/// Fully supported (locks the exact base-relation rows that match):
/// single-table `Scan` / `Filter(Scan)` / `Project` / `Sort` / `Limit`,
/// and inner/cross `Join`s and comma multi-table selects whose leaves
/// are those shapes. For a join, each base relation's rows matching that
/// relation's own predicate conjuncts are locked (a safe superset of the
/// emitted rows — never silent-no-lock).
///
/// Unsupported shapes (`Aggregate`, `SetOp`, sub-select/derived `Scan`
/// with no catalog entry, CTE bodies, …) raise `feature_not_supported`
/// rather than silently locking nothing.
///
/// # Wait policy
///
/// - `Wait`     — blocking `acquire()` (deadlock-aware); on a deadlock
///   victim raises `40P01`.
/// - `NoWait`   — `try_acquire`; first conflict raises `55P03`.
/// - `SkipLocked` — `try_acquire`; conflicting rows are skipped (the
///   non-conflicting rows are still locked).
/// Returns `Some(rewritten_plan)` when the lock pass must replace the plan
/// the caller executes — this happens only for `SKIP LOCKED`, whose result
/// must exclude rows another transaction has locked (and so cannot be
/// produced by the unmodified plan). For `Wait` / `NoWait` the original
/// plan is executed unchanged and `None` is returned.
pub(crate) fn acquire_simple_lock_rows(
    plan: &LogicalPlan,
    catalog_snapshot: &Arc<CatalogSnapshot>,
    table_constraints: &dashmap::DashMap<ultrasql_core::Oid, Arc<TableRuntimeConstraints>>,
    heap: &HeapAccess<BlankPageLoader>,
    oracle: &TransactionManager,
    txn: &Transaction,
) -> Result<Option<LogicalPlan>, ServerError> {
    let LogicalPlan::LockRows {
        input,
        strength,
        wait_policy,
        schema,
    } = plan
    else {
        return Ok(None);
    };
    let mode = row_lock_mode(*strength);

    // SKIP LOCKED needs the scan-then-skip-then-limit ordering and must
    // filter locked rows out of the result, which the unmodified plan
    // cannot do — handle it on its own single-table path that returns a
    // rewritten Values plan of the grabbed rows.
    if *wait_policy == LockWaitPolicy::SkipLocked {
        return acquire_skip_locked(input, schema, catalog_snapshot, heap, oracle, txn, mode)
            .map(Some);
    }

    // Collect every base relation the FOR UPDATE/SHARE clause must lock.
    // An unrecognised shape errors here instead of silently locking
    // nothing (silent-no-lock is the lost-update corruption).
    let mut targets: Vec<LockTarget<'_>> = Vec::new();
    collect_lock_targets(input, None, &mut targets)?;

    for target in &targets {
        let Some(entry) = catalog_snapshot.tables.get(&target.table) else {
            // Named in the plan but absent from the catalog snapshot:
            // refuse rather than skip. A FOR UPDATE that cannot resolve
            // its base relation must not proceed lock-free.
            return Err(ServerError::unsupported(format!(
                "SELECT ... FOR UPDATE/SHARE: cannot resolve base relation '{}' to lock",
                target.table
            )));
        };

        let tids = resolve_lock_tids(
            target.predicate,
            entry,
            catalog_snapshot,
            table_constraints,
            heap,
            oracle,
            txn,
        )?;
        acquire_row_locks(&tids, oracle, txn, mode, *wait_policy)?;
    }

    Ok(None)
}

/// `SKIP LOCKED` single-table path.
///
/// Scans the base relation in heap order, evaluates the optional `WHERE`
/// predicate, `try_acquire`s the row lock on each match, skips rows whose
/// lock is held by another transaction, and stops once `LIMIT` grabbed
/// rows are collected (`SKIP` happens *before* `LIMIT`, matching
/// PostgreSQL). Returns a `Values` plan of the grabbed rows projected
/// through the original output list so the result excludes skipped rows.
///
/// Supported input shapes: `Scan`, `Filter(Scan)`, optional `Project`,
/// optional `Limit`/`Offset`. `ORDER BY`, joins, aggregates and other
/// shapes raise `feature_not_supported` (never silent-no-lock).
fn acquire_skip_locked(
    input: &LogicalPlan,
    out_schema: &Schema,
    catalog_snapshot: &Arc<CatalogSnapshot>,
    heap: &HeapAccess<BlankPageLoader>,
    oracle: &TransactionManager,
    txn: &Transaction,
    mode: RowLockMode,
) -> Result<LogicalPlan, ServerError> {
    let shape = SkipLockedShape::collect(input)?;
    let Some(entry) = catalog_snapshot.tables.get(&shape.table) else {
        return Err(ServerError::unsupported(format!(
            "SELECT ... FOR UPDATE SKIP LOCKED: cannot resolve base relation '{}' to lock",
            shape.table
        )));
    };

    let rel = RelationId(entry.oid);
    let block_count = heap.block_count(rel).max(entry.n_blocks);
    let codec = RowCodec::new(entry.schema.clone());
    let predicate_eval = shape.predicate.cloned().map(Eval::new);
    // Projection evaluators: `None` means SELECT * (emit the base row).
    let projection: Option<Vec<Eval>> = shape
        .projection
        .as_ref()
        .map(|exprs| exprs.iter().map(|(e, _)| Eval::new(e.clone())).collect());

    let limit = shape.limit.unwrap_or(u64::MAX);
    let offset = shape.offset;
    let mut skipped = 0_u64;
    let mut grabbed: Vec<Vec<ScalarExpr>> = Vec::new();
    // Hold the lock under the stable top-level xid (released at txn end by
    // `release_all`, and a no-op on re-lock) while recording the acquiring
    // subxid as the grant's owner so `ROLLBACK TO` frees locks taken since the
    // savepoint. Mirrors `acquire_row_locks` (the non-SKIP-LOCKED path).
    let xid = txn.xid;
    let owner = txn.current_xid();

    for tuple in heap.scan_visible(rel, block_count, &txn.snapshot, oracle) {
        if grabbed.len() as u64 >= limit {
            break;
        }
        let tuple =
            tuple.map_err(|e| ServerError::Execute(ExecError::TypeMismatch(e.to_string())))?;
        let row = codec
            .decode(&tuple.data)
            .map_err(|e| ServerError::Execute(ExecError::TypeMismatch(e.to_string())))?;
        if let Some(eval) = &predicate_eval {
            match eval
                .eval(&row)
                .map_err(|e| ServerError::Execute(ExecError::TypeMismatch(e.to_string())))?
            {
                Value::Bool(true) => {}
                Value::Bool(false) | Value::Null => continue,
                other => {
                    return Err(ServerError::Execute(ExecError::TypeMismatch(format!(
                        "FOR UPDATE predicate returned non-boolean value {other:?}",
                    ))));
                }
            }
        }
        // Try to grab the lock; a conflict means another txn holds it —
        // skip the row entirely (no block, no error, not in the result).
        let acquired = oracle
            .lock_manager
            .try_acquire_for_owner(
                LockRequest {
                    xid,
                    tag: LockTag::Tuple(tuple.tid),
                    mode: mode.to_lock_mode(),
                },
                owner,
            )
            .map_err(|e| match e {
                LockError::Deadlock { .. } => deadlock_error(),
                other => ServerError::Execute(ExecError::SerializationFailure(other.to_string())),
            })?;
        if !acquired {
            continue;
        }
        // OFFSET applies to grabbed (locked) rows, after SKIP.
        if skipped < offset {
            skipped += 1;
            continue;
        }
        grabbed.push(project_row(&row, projection.as_deref(), out_schema)?);
    }

    Ok(LogicalPlan::Values {
        rows: grabbed,
        schema: out_schema.clone(),
    })
}

/// Project one decoded base row into the output literal list for a
/// `Values` row, applying the SELECT list (or passing the row through for
/// `SELECT *`) and tagging each value with the output schema's type.
fn project_row(
    row: &[Value],
    projection: Option<&[Eval]>,
    out_schema: &Schema,
) -> Result<Vec<ScalarExpr>, ServerError> {
    let values: Vec<Value> =
        match projection {
            Some(evals) => {
                let mut out = Vec::with_capacity(evals.len());
                for eval in evals {
                    out.push(eval.eval(row).map_err(|e| {
                        ServerError::Execute(ExecError::TypeMismatch(e.to_string()))
                    })?);
                }
                out
            }
            None => row.to_vec(),
        };
    if values.len() != out_schema.fields().len() {
        return Err(ServerError::unsupported(
            "SELECT ... FOR UPDATE SKIP LOCKED: projection width does not match output schema",
        ));
    }
    Ok(values
        .into_iter()
        .zip(out_schema.fields())
        .map(|(value, field)| ScalarExpr::Literal {
            value,
            data_type: field.data_type.clone(),
        })
        .collect())
}

/// The decomposed single-table shape under a `SKIP LOCKED` `LockRows`.
struct SkipLockedShape<'a> {
    table: String,
    predicate: Option<&'a ScalarExpr>,
    projection: Option<&'a [(ScalarExpr, String)]>,
    limit: Option<u64>,
    offset: u64,
}

impl<'a> SkipLockedShape<'a> {
    fn collect(plan: &'a LogicalPlan) -> Result<Self, ServerError> {
        let mut shape = Self {
            table: String::new(),
            predicate: None,
            projection: None,
            limit: None,
            offset: 0,
        };
        shape.walk(plan)?;
        if shape.table.is_empty() {
            return Err(ServerError::unsupported(
                "SELECT ... FOR UPDATE SKIP LOCKED: could not identify a single base relation",
            ));
        }
        Ok(shape)
    }

    fn walk(&mut self, plan: &'a LogicalPlan) -> Result<(), ServerError> {
        match plan {
            LogicalPlan::Limit { input, n, offset } => {
                self.limit = Some(*n);
                self.offset = *offset;
                self.walk(input)
            }
            LogicalPlan::Project {
                input,
                exprs,
                schema,
            } => {
                // Only a flat list of column/expression outputs is
                // supported. The projection drives the Values rows.
                let _ = schema;
                self.projection = Some(exprs.as_slice());
                self.walk(input)
            }
            LogicalPlan::Filter { input, predicate } => {
                if let LogicalPlan::Scan { table, .. } = input.as_ref() {
                    self.predicate = Some(predicate);
                    self.table = table.to_ascii_lowercase();
                    Ok(())
                } else {
                    Err(skip_locked_unsupported())
                }
            }
            LogicalPlan::Scan { table, .. } => {
                self.table = table.to_ascii_lowercase();
                Ok(())
            }
            _ => Err(skip_locked_unsupported()),
        }
    }
}

fn skip_locked_unsupported() -> ServerError {
    ServerError::unsupported(
        "SELECT ... FOR UPDATE SKIP LOCKED is supported only for a single base table with an \
         optional WHERE / projection / LIMIT (ORDER BY, joins and aggregates are not supported)",
    )
}

/// Resolve the visible base-relation tuple ids that a lock target's
/// predicate selects, preferring an equality index probe and falling
/// back to a predicate-filtered visible heap scan.
fn resolve_lock_tids(
    predicate: Option<&ScalarExpr>,
    entry: &TableEntry,
    catalog_snapshot: &Arc<CatalogSnapshot>,
    table_constraints: &dashmap::DashMap<ultrasql_core::Oid, Arc<TableRuntimeConstraints>>,
    heap: &HeapAccess<BlankPageLoader>,
    oracle: &TransactionManager,
    txn: &Transaction,
) -> Result<Vec<ultrasql_core::TupleId>, ServerError> {
    if let Some(tids) =
        lock_rows_index_tids(predicate, entry, catalog_snapshot, table_constraints, heap)?
    {
        return Ok(tids);
    }

    let rel = RelationId(entry.oid);
    let block_count = heap.block_count(rel).max(entry.n_blocks);
    let codec = RowCodec::new(entry.schema.clone());
    let predicate_eval = predicate.cloned().map(Eval::new);
    let mut tids = Vec::new();

    for tuple in heap.scan_visible(rel, block_count, &txn.snapshot, oracle) {
        let tuple =
            tuple.map_err(|e| ServerError::Execute(ExecError::TypeMismatch(e.to_string())))?;
        let matched = match &predicate_eval {
            Some(eval) => {
                let row = codec
                    .decode(&tuple.data)
                    .map_err(|e| ServerError::Execute(ExecError::TypeMismatch(e.to_string())))?;
                match eval
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
                }
            }
            None => true,
        };
        if matched {
            tids.push(tuple.tid);
        }
    }

    Ok(tids)
}

/// Recursively collect the base relations a `LockRows` child plan locks.
///
/// `inherited` is the predicate conjunct flowing down from an enclosing
/// `Filter`; it is attached to a leaf `Scan` so single-table predicates
/// restrict the locked set. Errors on any shape that cannot be reduced to
/// concrete base relations (so the caller never silently locks nothing).
pub(crate) fn collect_lock_targets<'a>(
    plan: &'a LogicalPlan,
    inherited: Option<&'a ScalarExpr>,
    out: &mut Vec<LockTarget<'a>>,
) -> Result<(), ServerError> {
    match plan {
        LogicalPlan::Scan { table, .. } => {
            out.push(LockTarget {
                table: table.to_ascii_lowercase(),
                predicate: inherited,
            });
            Ok(())
        }
        // Pass-through wrappers that neither change row identity nor the
        // set of base relations contributing rows.
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => collect_lock_targets(input, inherited, out),
        LogicalPlan::Filter { input, predicate } => {
            // A nested filter combines with any inherited predicate. We
            // keep the *innermost* predicate per relation; an equality on
            // the indexed key (the common `WHERE id = ?` case) survives to
            // drive the index probe. Carrying the filter down to a single
            // Scan keeps single-table locking exact.
            match input.as_ref() {
                LogicalPlan::Scan { .. } => collect_lock_targets(input, Some(predicate), out),
                _ => collect_lock_targets(input, inherited.or(Some(predicate)), out),
            }
        }
        LogicalPlan::Join { left, right, .. } => {
            // Lock both sides' base relations. The inherited single-relation
            // predicate (if any) only soundly restricts one side, so we drop
            // it across the join and lock each relation's full visible set —
            // a safe superset of the emitted rows (never under-locks).
            collect_lock_targets(left, None, out)?;
            collect_lock_targets(right, None, out)?;
            Ok(())
        }
        other => Err(ServerError::unsupported(format!(
            "SELECT ... FOR UPDATE/SHARE over this plan shape is not supported \
             (cannot identify base-relation rows to lock): {}",
            lock_target_plan_label(other)
        ))),
    }
}

/// A short, stable node label for the unsupported-shape error message.
fn lock_target_plan_label(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Aggregate { .. } => "Aggregate",
        LogicalPlan::SetOp { .. } => "SetOp",
        LogicalPlan::Cte { .. } => "CTE",
        LogicalPlan::Values { .. } => "Values",
        LogicalPlan::FunctionScan { .. } => "FunctionScan",
        LogicalPlan::Window { .. } => "Window",
        LogicalPlan::DistinctOn { .. } => "DistinctOn",
        LogicalPlan::Pivot { .. } => "Pivot",
        LogicalPlan::Unpivot { .. } => "Unpivot",
        LogicalPlan::Empty { .. } => "Empty",
        _ => "unsupported",
    }
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

/// Acquire row locks on `tids` under the requested wait policy.
///
/// The lock is held by the **top-level** transaction xid, so
/// `lock_manager.release_all(xid)` frees it at commit/rollback — held to the
/// end of the transaction, matching PostgreSQL. When a savepoint is open the
/// acquiring subxid (`txn.current_xid()`) is recorded as the grant's *owner*
/// so a `ROLLBACK TO` releases exactly the locks taken since that savepoint
/// (via [`TransactionManager::rollback_to_savepoint`] →
/// `release_subxact_locks`), again matching PostgreSQL. Owning the lock under
/// the stable top-level xid (rather than the subxid) is what keeps a re-lock
/// of the same row in a later statement a no-op instead of a self-block, and
/// is what stops the lock from leaking past commit when it was first taken
/// inside a savepoint.
///
/// - `Wait`     — blocking, deadlock-aware `acquire()` (the same path the
///   indexed UPDATE write uses). A conflicting holder makes this block
///   until released; a deadlock victim returns `40P01`.
/// - `NoWait`   — `try_acquire`; the first conflict returns `55P03`.
/// - `SkipLocked` — `try_acquire`; conflicting rows are silently skipped
///   (their non-conflict siblings are still locked).
fn acquire_row_locks(
    tids: &[ultrasql_core::TupleId],
    oracle: &TransactionManager,
    txn: &Transaction,
    mode: RowLockMode,
    wait_policy: LockWaitPolicy,
) -> Result<(), ServerError> {
    let xid = txn.xid;
    let owner = txn.current_xid();
    let lock_manager = &oracle.lock_manager;
    for tid in tids {
        let req = LockRequest {
            xid,
            tag: LockTag::Tuple(*tid),
            mode: mode.to_lock_mode(),
        };
        match wait_policy {
            LockWaitPolicy::Wait => {
                // Fast path: try once non-blocking. On a conflict fall back
                // to the blocking, deadlock-detecting acquire — but never
                // call it twice when this xid already holds the grant
                // (re-locking the same row inside the same txn is a no-op).
                match lock_manager.try_acquire_for_owner(req, owner) {
                    Ok(true) => {}
                    Ok(false) => block_on_lock(lock_manager, req, owner)?,
                    Err(LockError::Deadlock { .. }) => {
                        return Err(deadlock_error());
                    }
                    Err(e) => {
                        return Err(ServerError::Execute(ExecError::SerializationFailure(
                            e.to_string(),
                        )));
                    }
                }
            }
            LockWaitPolicy::NoWait => match lock_manager.try_acquire_for_owner(req, owner) {
                Ok(true) => {}
                Ok(false) => {
                    return Err(ServerError::LockNotAvailable(
                        "could not obtain lock on row in relation".to_string(),
                    ));
                }
                Err(LockError::Deadlock { .. }) => return Err(deadlock_error()),
                Err(e) => {
                    return Err(ServerError::Execute(ExecError::SerializationFailure(
                        e.to_string(),
                    )));
                }
            },
            LockWaitPolicy::SkipLocked => match lock_manager.try_acquire_for_owner(req, owner) {
                // A locked row is skipped: do not block, do not error.
                Ok(_) => {}
                Err(LockError::Deadlock { .. }) => return Err(deadlock_error()),
                Err(e) => {
                    return Err(ServerError::Execute(ExecError::SerializationFailure(
                        e.to_string(),
                    )));
                }
            },
        }
    }
    Ok(())
}

/// Block on a conflicting row lock via the lock manager's blocking,
/// deadlock-aware `acquire()`. Mirrors the indexed-UPDATE write path:
/// when running on a multi-thread tokio runtime the blocking call is
/// wrapped in `block_in_place` so the worker thread is released to the
/// scheduler while parked; otherwise it blocks the current thread
/// directly (single-thread runtime / non-async caller).
fn block_on_lock(
    lock_manager: &LockManager,
    req: LockRequest,
    owner: Xid,
) -> Result<(), ServerError> {
    let acquire = || lock_manager.acquire_for_owner(req, owner);
    let result = if matches!(
        tokio::runtime::Handle::try_current().map(|handle| handle.runtime_flavor()),
        Ok(tokio::runtime::RuntimeFlavor::MultiThread)
    ) {
        tokio::task::block_in_place(acquire)
    } else {
        acquire()
    };
    match result {
        Ok(()) => Ok(()),
        Err(LockError::Deadlock { .. }) => Err(deadlock_error()),
        Err(e) => Err(ServerError::Execute(ExecError::SerializationFailure(
            e.to_string(),
        ))),
    }
}

/// Construct the `40P01 deadlock_detected` error for a row-lock victim.
fn deadlock_error() -> ServerError {
    ServerError::DeadlockDetected(
        "deadlock detected while waiting for a row lock (FOR UPDATE/SHARE)".to_string(),
    )
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

pub(crate) const fn row_lock_mode(strength: LockStrength) -> RowLockMode {
    match strength {
        LockStrength::Update => RowLockMode::ForUpdate,
        LockStrength::NoKeyUpdate => RowLockMode::ForNoKeyUpdate,
        LockStrength::Share => RowLockMode::ForShare,
        LockStrength::KeyShare => RowLockMode::ForKeyShare,
    }
}
