//! Focused production-helper coverage; see `tests/mod.rs`.

#![allow(clippy::too_many_lines)]

use super::*;
use std::sync::atomic::Ordering;
use ultrasql_core::{Field, Oid, Schema};
use ultrasql_planner::LogicalAggregateExpr;

#[test]
fn top_level_local_query_runs_sample_select_and_rejects_writes() {
    let output = execute_local_query("SELECT id, name FROM users ORDER BY id").expect("select");

    assert_eq!(output.command_tag, "SELECT 3");
    assert_eq!(
        output
            .columns
            .iter()
            .map(|column| (column.name.as_str(), column.type_oid))
            .collect::<Vec<_>>(),
        vec![("id", 23), ("name", 25)]
    );
    assert_eq!(
        output.rows,
        vec![
            vec![Some("1".to_owned()), Some("Ada".to_owned())],
            vec![Some("2".to_owned()), Some("Grace".to_owned())],
            vec![Some("3".to_owned()), Some("Linus".to_owned())],
        ]
    );

    let err = execute_local_query("CREATE TABLE local_nope (id int4)")
        .expect_err("local writes are rejected");
    assert!(matches!(err, ServerError::Unsupported(message) if message.contains("read-only")));
}

#[test]
fn local_query_finalisation_reports_commit_failure() {
    let server = Server::with_sample_database();
    let txn = server.txn_manager.begin(IsolationLevel::ReadCommitted);
    let stale = txn.clone();
    server.txn_manager.commit(txn).expect("pre-commit");

    let err = server
        .finalise_local_query_transaction(
            stale,
            Ok(LocalQueryOutput {
                columns: Vec::new(),
                rows: Vec::new(),
                command_tag: "SELECT 0".to_owned(),
            }),
        )
        .expect_err("local read commit failure must be visible");
    let msg = err.to_string();
    assert!(
        msg.contains("ultrasql-local read transaction commit"),
        "context missing: {err}"
    );
    assert!(msg.contains("commit"), "commit failure hidden: {err}");
}

#[test]
fn local_query_finalisation_reports_abort_failure_with_original_error() {
    let server = Server::with_sample_database();
    let txn = server.txn_manager.begin(IsolationLevel::ReadCommitted);
    let stale = txn.clone();
    server.txn_manager.abort(txn).expect("pre-abort");

    let err = server
        .finalise_local_query_transaction(
            stale,
            Err(ServerError::Unsupported("local executor boom")),
        )
        .expect_err("local read rollback failure must be visible");
    let msg = err.to_string();
    assert!(
        msg.contains("ultrasql-local read transaction rollback"),
        "context missing: {err}"
    );
    assert!(
        msg.contains("local executor boom"),
        "original error lost: {err}"
    );
    assert!(
        msg.contains("transaction abort failed"),
        "abort failure hidden: {err}"
    );
}

#[test]
fn restart_rebuild_finalisation_reports_commit_failure() {
    let server = Server::with_sample_database();
    let txn = server.txn_manager.begin(IsolationLevel::ReadCommitted);
    let stale = txn.clone();
    server.txn_manager.commit(txn).expect("pre-commit");

    let err = server
        .finalise_restart_rebuild_transaction(
            stale,
            Ok(1_u64),
            "restart rebuild btree commit",
            "restart rebuild btree rollback",
        )
        .expect_err("restart rebuild commit failure must be visible");
    let msg = err.to_string();
    assert!(
        msg.contains("restart rebuild btree commit"),
        "context missing: {err}"
    );
    assert!(msg.contains("commit"), "commit failure hidden: {err}");
}

#[test]
fn restart_rebuild_finalisation_reports_abort_failure_with_original_error() {
    let server = Server::with_sample_database();
    let txn = server.txn_manager.begin(IsolationLevel::ReadCommitted);
    let stale = txn.clone();
    server.txn_manager.abort(txn).expect("pre-abort");

    let err = server
        .finalise_restart_rebuild_transaction::<u64>(
            stale,
            Err(ServerError::ddl("restart rebuild boom")),
            "restart rebuild btree commit",
            "restart rebuild btree rollback",
        )
        .expect_err("restart rebuild rollback failure must be visible");
    let msg = err.to_string();
    assert!(
        msg.contains("restart rebuild btree rollback"),
        "context missing: {err}"
    );
    assert!(
        msg.contains("restart rebuild boom"),
        "original error lost: {err}"
    );
    assert!(
        msg.contains("transaction abort failed"),
        "abort failure hidden: {err}"
    );
}

#[test]
fn metadata_codecs_cover_escapes_and_rls_tokens() {
    let raw = "tenant\\name\tline\nnext";
    let escaped = metadata_escape(raw);
    assert_eq!(escaped, "tenant\\\\name\\tline\\nnext");
    assert_eq!(metadata_unescape(&escaped).expect("unescape"), raw);
    assert!(metadata_unescape("\\x").is_err());
    assert!(metadata_unescape("\\").is_err());

    assert_eq!(
        rls_permissiveness_name(RuntimeRlsPermissiveness::Restrictive),
        "restrictive"
    );
    assert_eq!(
        parse_rls_permissiveness("restrictive").expect("restrictive"),
        RuntimeRlsPermissiveness::Restrictive
    );
    assert!(parse_rls_permissiveness("bogus").is_err());

    for (command, token) in [
        (RuntimeRlsCommand::All, "all"),
        (RuntimeRlsCommand::Select, "select"),
        (RuntimeRlsCommand::Insert, "insert"),
        (RuntimeRlsCommand::Update, "update"),
        (RuntimeRlsCommand::Delete, "delete"),
    ] {
        assert_eq!(rls_command_name(command), token);
        assert_eq!(parse_rls_command(token).expect("command"), command);
    }
    assert!(parse_rls_command("merge").is_err());

    assert_eq!(
        rls_expr_fields(None),
        (String::new(), String::new(), String::new())
    );
    assert!(parse_rls_expr("", "", "").expect("none").is_none());
    let expr = RuntimeTenantPolicyExpr {
        column_index: 2,
        column_name: "tenant\tid".to_owned(),
        setting_name: "ultrasql.tenant\nid".to_owned(),
    };
    let fields = rls_expr_fields(Some(&expr));
    let decoded = parse_rls_expr(&fields.0, &fields.1, &fields.2)
        .expect("rls expr")
        .expect("some expr");
    assert_eq!(decoded.column_index, 2);
    assert_eq!(decoded.column_name, expr.column_name);
    assert_eq!(decoded.setting_name, expr.setting_name);
    assert!(parse_rls_expr("bad", "", "").is_err());
}

#[test]
fn scalar_metadata_tokens_round_trip_supported_surface() {
    let type_cases = [
        DataType::Bool,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Money,
        DataType::Float32,
        DataType::Float64,
        DataType::Text { max_len: None },
        DataType::Char { len: Some(4) },
        DataType::Char { len: None },
        DataType::Bit { len: Some(3) },
        DataType::Bit { len: None },
        DataType::VarBit { max_len: Some(8) },
        DataType::VarBit { max_len: None },
        DataType::Inet,
        DataType::Cidr,
        DataType::MacAddr,
        DataType::MacAddr8,
        DataType::Date,
        DataType::Time,
        DataType::TimeTz,
        DataType::Timestamp,
        DataType::TimestampTz,
        DataType::Null,
    ];
    for ty in &type_cases {
        let token = data_type_token(ty).expect("type token");
        assert_eq!(data_type_from_token(&token).expect("type decode"), *ty);
    }
    assert!(data_type_from_token("char:bad").is_none());
    assert!(data_type_from_token("nope").is_none());

    let binary_ops = [
        BinaryOp::Add,
        BinaryOp::Sub,
        BinaryOp::Mul,
        BinaryOp::Div,
        BinaryOp::Mod,
        BinaryOp::Pow,
        BinaryOp::Concat,
        BinaryOp::Eq,
        BinaryOp::NotEq,
        BinaryOp::Lt,
        BinaryOp::LtEq,
        BinaryOp::Gt,
        BinaryOp::GtEq,
        BinaryOp::And,
        BinaryOp::Or,
        BinaryOp::Like,
        BinaryOp::NotLike,
        BinaryOp::Ilike,
        BinaryOp::NotIlike,
        BinaryOp::RegexMatch,
        BinaryOp::RegexIMatch,
        BinaryOp::RegexNotMatch,
        BinaryOp::RegexNotIMatch,
        BinaryOp::BitAnd,
        BinaryOp::BitOr,
        BinaryOp::BitXor,
        BinaryOp::ShiftLeft,
        BinaryOp::ShiftRight,
        BinaryOp::NetworkContainedEq,
        BinaryOp::NetworkContainsEq,
        BinaryOp::JsonGet,
        BinaryOp::JsonGetText,
        BinaryOp::JsonGetPath,
        BinaryOp::JsonGetPathText,
        BinaryOp::JsonContains,
        BinaryOp::JsonContained,
        BinaryOp::JsonHasKey,
        BinaryOp::JsonHasAnyKey,
        BinaryOp::JsonHasAllKeys,
        BinaryOp::TextSearchMatch,
        BinaryOp::Overlap,
        BinaryOp::VectorL2Distance,
        BinaryOp::VectorNegativeInnerProduct,
        BinaryOp::VectorCosineDistance,
        BinaryOp::VectorL1Distance,
    ];
    for op in binary_ops {
        assert_eq!(
            binary_op_from_token(binary_op_token(op)).expect("binary op"),
            op
        );
    }
    assert!(binary_op_from_token("not-an-op").is_none());

    for op in [UnaryOp::Neg, UnaryOp::Pos, UnaryOp::Not, UnaryOp::BitNot] {
        assert_eq!(
            unary_op_from_token(unary_op_token(op)).expect("unary op"),
            op
        );
    }
    assert!(unary_op_from_token("not-an-op").is_none());

    let values = vec![
        (DataType::Null, Value::Null),
        (DataType::Bool, Value::Bool(true)),
        (DataType::Int16, Value::Int16(-7)),
        (DataType::Int32, Value::Int32(42)),
        (DataType::Int64, Value::Int64(9_000)),
        (DataType::Money, Value::Money(12_34)),
        (DataType::Float32, Value::Float32(1.5)),
        (DataType::Float64, Value::Float64(-2.25)),
        (
            DataType::Text { max_len: None },
            Value::Text("a\tb".to_owned()),
        ),
        (
            DataType::Char { len: Some(4) },
            Value::Char("xy".to_owned()),
        ),
        (
            DataType::Bit { len: Some(4) },
            Value::parse_bit_string("1010").expect("bit string"),
        ),
        (
            DataType::Inet,
            Value::parse_network(&DataType::Inet, "127.0.0.1").expect("inet"),
        ),
        (
            DataType::Cidr,
            Value::parse_network(&DataType::Cidr, "10.0.0.0/24").expect("cidr"),
        ),
        (
            DataType::MacAddr,
            Value::parse_network(&DataType::MacAddr, "08:00:2b:01:02:03").expect("mac"),
        ),
        (
            DataType::MacAddr8,
            Value::parse_network(&DataType::MacAddr8, "08:00:2b:ff:fe:01:02:03").expect("mac8"),
        ),
        (DataType::Date, Value::Date(123)),
        (DataType::Time, Value::Time(456)),
        (
            DataType::TimeTz,
            Value::TimeTz {
                micros: 789,
                offset_seconds: -18_000,
            },
        ),
        (DataType::Timestamp, Value::Timestamp(1_234)),
        (DataType::TimestampTz, Value::TimestampTz(5_678)),
    ];
    for (ty, value) in values {
        let token = value_token(&value).expect("value token");
        assert_eq!(value_from_token(&ty, &token).expect("value decode"), value);
    }
    assert!(value_from_token(&DataType::Int32, "not-int").is_err());
    assert!(value_from_token(&DataType::TimeTz, "bad").is_err());

    let scalar = ScalarExpr::Binary {
        op: BinaryOp::Add,
        left: Box::new(ScalarExpr::Unary {
            op: UnaryOp::Neg,
            expr: Box::new(ScalarExpr::Column {
                name: "x".to_owned(),
                index: 0,
                data_type: DataType::Int32,
            }),
            data_type: DataType::Int32,
        }),
        right: Box::new(ScalarExpr::Literal {
            value: Value::Int32(7),
            data_type: DataType::Int32,
        }),
        data_type: DataType::Int32,
    };
    let encoded = encode_scalar_expr_field(&scalar).expect("scalar encode");
    assert_eq!(
        decode_scalar_expr_field(&encoded).expect("scalar decode"),
        scalar
    );

    let is_null = ScalarExpr::IsNull {
        expr: Box::new(ScalarExpr::Literal {
            value: Value::Null,
            data_type: DataType::Null,
        }),
        negated: true,
    };
    let encoded = encode_scalar_expr_field(&is_null).expect("isnull encode");
    assert_eq!(
        decode_scalar_expr_field(&encoded).expect("isnull decode"),
        is_null
    );
    assert!(decode_scalar_expr_field("unknown").is_err());
}

#[test]
fn autovacuum_and_recovery_helpers_validate_edges() {
    assert_eq!(
        AutovacuumConfig::scale_factor_to_ppm("x", 0.125).expect("ppm"),
        125_000
    );
    assert!(AutovacuumConfig::scale_factor_to_ppm("x", f64::NAN).is_err());
    assert!(AutovacuumConfig::scale_factor_to_ppm("x", -1.0).is_err());
    assert_eq!(AutovacuumConfig::default().vacuum_scale_factor(), 0.2);
    assert_eq!(AutovacuumConfig::default().analyze_scale_factor(), 0.1);
    assert_eq!(scaled_threshold(50, 100_000, 1_000), 150);

    let mut config = AutovacuumConfig::default();
    apply_autovacuum_reloptions(
        &mut config,
        &[
            ("autovacuum_vacuum_threshold".to_owned(), "7".to_owned()),
            (
                "autovacuum_vacuum_scale_factor".to_owned(),
                "0.05".to_owned(),
            ),
            ("autovacuum_analyze_threshold".to_owned(), "9".to_owned()),
            (
                "autovacuum_analyze_scale_factor".to_owned(),
                "0.025".to_owned(),
            ),
        ],
    )
    .expect("reloptions");
    assert_eq!(config.vacuum_threshold, 7);
    assert_eq!(config.vacuum_scale_factor_ppm, 50_000);
    assert_eq!(config.analyze_threshold, 9);
    assert_eq!(config.analyze_scale_factor_ppm, 25_000);
    assert_eq!(config.vacuum_threshold_for_rows(100), 12);
    assert_eq!(config.analyze_threshold_for_rows(100), 11);
    assert!(validate_autovacuum_reloptions(&[("unknown".to_owned(), "1".to_owned())]).is_err());
    assert!(parse_autovacuum_u64("x", "-1").is_err());
    assert!(parse_autovacuum_scale("x", "inf").is_err());

    assert_eq!(parse_recovery_lsn("0/10").expect("lsn").raw(), 16);
    assert_eq!(parse_recovery_lsn("42").expect("lsn").raw(), 42);
    assert!(parse_recovery_lsn("100000000/0").is_err());
    assert!(parse_recovery_lsn("bad").is_err());
    assert_eq!(
        parse_recovery_time_micros("1970-01-01 00:00:01Z").expect("time"),
        1_000_000
    );
    assert!(parse_recovery_time_micros("1969-12-31T23:59:59Z").is_err());
    assert_eq!(parse_recovery_xid("5").expect("xid").raw(), 5);
    assert!(parse_recovery_xid("0").is_err());
    assert!(parse_recovery_xid("bad").is_err());
}

#[test]
fn runtime_state_helpers_cover_sequences_advisory_and_validation() {
    let sequence_state = SequenceSessionState::default();
    assert!(sequence_state.currval("seq").is_none());
    assert!(sequence_state.lastval().is_none());
    sequence_state.record_nextval("Seq", 10);
    assert_eq!(sequence_state.currval("seq"), Some(10));
    assert_eq!(sequence_state.lastval(), Some(("seq".to_owned(), 10)));
    sequence_state.forget("SEQ");
    assert!(sequence_state.currval("seq").is_none());
    assert!(sequence_state.lastval().is_none());

    let locks = LockManager::new();
    let advisory = AdvisorySessionState::new(77);
    assert_eq!(
        advisory
            .evaluate_function("pg_try_advisory_lock", &[Value::Int64(42)], &locks)
            .expect("try lock"),
        Value::Bool(true)
    );
    assert_eq!(
        advisory
            .evaluate_function(
                "pg_advisory_unlock",
                &[Value::Int32(0), Value::Int32(42)],
                &locks
            )
            .expect("unlock"),
        Value::Bool(true)
    );
    assert_eq!(
        advisory
            .evaluate_function("pg_advisory_unlock", &[Value::Int64(42)], &locks)
            .expect("unlock missing"),
        Value::Bool(false)
    );
    assert!(
        advisory
            .evaluate_function("pg_advisory_lock", &[Value::Text("x".to_owned())], &locks)
            .is_err()
    );
    assert!(
        advisory
            .evaluate_function("pg_advisory_unlock_all", &[Value::Int32(1)], &locks)
            .is_err()
    );
    assert_eq!(
        advisory
            .evaluate_transaction_function(
                "pg_try_advisory_xact_lock",
                &[Value::Int32(1), Value::Int32(2)],
                &locks,
                Xid::new(99),
            )
            .expect("xact lock"),
        Value::Bool(true)
    );

    let ok = validation_check("ok", Vec::new(), "all good".to_owned());
    assert_eq!(ok.status, ValidationStatus::Ok);
    assert_eq!(ok.status.as_str(), "ok");
    let failed = validation_check("bad", vec!["a".to_owned(), "b".to_owned()], String::new());
    assert_eq!(failed.status, ValidationStatus::Failed);
    assert_eq!(failed.status.as_str(), "failed");
    assert_eq!(failed.detail, "a; b");
}

#[test]
fn page_loader_and_local_wire_body_helpers_cover_error_paths() {
    let dir = tempfile::tempdir().expect("tempdir");
    let loader = BlankPageLoader::persistent(dir.path()).expect("persistent loader");
    let page_id = PageId::new(RelationId(Oid::new(9_999)), BlockNumber::new(0));
    let empty = loader.load(page_id).expect("empty page");
    assert_eq!(empty.as_bytes().len(), PAGE_SIZE);
    loader.store(page_id, &empty).expect("store page");
    let loaded = loader.load(page_id).expect("load page");
    assert_eq!(loaded.as_bytes().len(), PAGE_SIZE);

    let messages = local_result_messages(SelectResult {
        messages: vec![BackendMessage::CommandComplete {
            tag: "SELECT 0".to_owned(),
        }],
        streamed_body: None,
        shared_streamed_body: None,
        rows: 0,
    })
    .expect("messages");
    assert!(matches!(
        messages[0],
        BackendMessage::CommandComplete { .. }
    ));

    let err =
        decode_local_result_body(BytesMut::from(&b"T\0\0"[..])).expect_err("partial body rejected");
    assert!(matches!(err, ServerError::CopyFormat(message) if message.contains("partial")));
}

#[test]
fn materialized_view_and_cache_helpers_cover_runtime_sidecars() {
    let schema = Schema::new([
        Field::required("a", DataType::Int32),
        Field::required("b", DataType::Text { max_len: None }),
    ])
    .expect("schema");
    let scan = LogicalPlan::Scan {
        table: "src".to_owned(),
        schema: schema.clone(),
        projection: None,
    };
    assert_eq!(append_only_materialized_source_table(&scan), Some("src"));
    let project = LogicalPlan::Project {
        input: Box::new(scan.clone()),
        exprs: vec![(
            ScalarExpr::Column {
                name: "b".to_owned(),
                index: 1,
                data_type: DataType::Text { max_len: None },
            },
            "b".to_owned(),
        )],
        schema: Schema::new([Field::required("b", DataType::Text { max_len: None })])
            .expect("project schema"),
    };
    assert_eq!(append_only_materialized_source_table(&project), Some("src"));
    assert_eq!(
        materialized_view_projection_indices(&project).expect("projection"),
        vec![1]
    );
    assert!(
        materialized_view_projection_indices(&LogicalPlan::Empty {
            schema: Schema::empty()
        })
        .is_none()
    );

    let source_entry = TableEntry::new(Oid::new(10), "src", "public", schema.clone());
    let view_entry = TableEntry::new(
        Oid::new(11),
        "mv",
        "public",
        Schema::new([Field::required("b", DataType::Text { max_len: None })]).expect("view"),
    );
    let metadata = MaterializedViewMetadataRecord {
        view_table: "mv".to_owned(),
        view_oid: Oid::new(11),
        source_table: "src".to_owned(),
        source_oid: Oid::new(10),
        materialized_rows: 3,
        projection: vec![1],
    };
    let restored =
        materialized_view_source_plan_from_metadata(&source_entry, &view_entry, &metadata)
            .expect("restored plan");
    assert!(matches!(restored, LogicalPlan::Project { .. }));

    let runtime = RuntimeAggregatingIndex::new(
        ultrasql_planner::LogicalAggregatingIndex {
            group_columns: vec![0],
            aggregates: vec![ultrasql_planner::LogicalAggregatingIndexExpr {
                func: AggregateFunc::CountStar,
                arg_column: None,
                output_name: "count".to_owned(),
                data_type: DataType::Int64,
            }],
        },
        vec![vec![Value::Int32(1)]],
    );
    assert!(!runtime.dirty.load(Ordering::Acquire));
    runtime.mark_dirty();
    assert!(runtime.dirty.load(Ordering::Acquire));
    runtime.record_explain_read(true, 4, 8);
    let stats = runtime.explain_stats_snapshot();
    assert!(stats.aggregating_index_used);
    assert!(stats.stale_rebuild_used);
    assert_eq!(stats.summary_rows_read, 4);
    assert_eq!(stats.base_rows_skipped, 8);

    assert_eq!(usize_to_u64_saturated(7), 7);
    assert_eq!(
        pages_to_bytes_saturated(2),
        u64::try_from(PAGE_SIZE * 2).unwrap()
    );
}

#[test]
fn tpch_sidecar_cache_setters_round_trip_all_cached_shapes() {
    let _cache_guard = crate::TPCH_TEST_CACHE_LOCK
        .lock()
        .expect("tpch cache test lock");
    let q1 = TpchQ1ColumnarCache {
        quantity: vec![1],
        extendedprice: vec![2],
        discount: vec![3],
        tax: vec![4],
        returnflag: vec![b'N'],
        linestatus: vec![b'O'],
        shipdate: vec![5],
        summary_rows: vec![TpchQ1SummaryRow {
            returnflag: b'N',
            linestatus: b'O',
            sum_qty: 1,
            sum_base_price: 2,
            sum_disc_price: 3,
            sum_charge: 4,
            sum_discount: 5,
            count: 6,
        }],
        q6_revenue: 7,
    };
    assert_eq!(q1.len(), 1);
    assert!(!q1.is_empty());
    set_tpch_q1_columnar_cache(Some(q1));
    assert_eq!(tpch_q1_columnar_cache().expect("q1").len(), 1);
    set_tpch_q1_columnar_cache(None);
    assert!(tpch_q1_columnar_cache().is_none());

    set_tpch_q2_cache(Some(vec![TpchQ2ResultRow::default()]));
    assert_eq!(tpch_q2_cache().expect("q2").len(), 1);
    set_tpch_q3_cache(Some(vec![TpchQ3ResultRow::default()]));
    assert_eq!(tpch_q3_cache().expect("q3").len(), 1);
    set_tpch_q4_cache(Some(vec![TpchQ4ResultRow::default()]));
    assert_eq!(tpch_q4_cache().expect("q4").len(), 1);
    set_tpch_q5_cache(Some(vec![TpchQ5ResultRow::default()]));
    assert_eq!(tpch_q5_cache().expect("q5").len(), 1);
    set_tpch_q7_cache(Some(vec![TpchQ7ResultRow::default()]));
    assert_eq!(tpch_q7_cache().expect("q7").len(), 1);
    set_tpch_q8_cache(Some(vec![TpchQ8ResultRow::default()]));
    assert_eq!(tpch_q8_cache().expect("q8").len(), 1);
    set_tpch_q9_cache(Some(vec![TpchQ9ResultRow::default()]));
    assert_eq!(tpch_q9_cache().expect("q9").len(), 1);
    set_tpch_q10_cache(Some(vec![TpchQ10ResultRow::default()]));
    assert_eq!(tpch_q10_cache().expect("q10").len(), 1);
    set_tpch_q11_cache(Some(vec![TpchQ11ResultRow::default()]));
    assert_eq!(tpch_q11_cache().expect("q11").len(), 1);
    set_tpch_q12_cache(Some(vec![TpchQ12ResultRow::default()]));
    assert_eq!(tpch_q12_cache().expect("q12").len(), 1);
    set_tpch_q13_cache(Some(vec![TpchQ13ResultRow::default()]));
    assert_eq!(tpch_q13_cache().expect("q13").len(), 1);
    set_tpch_q14_cache(Some(vec![TpchQ14ResultRow::default()]));
    assert_eq!(tpch_q14_cache().expect("q14").len(), 1);
    set_tpch_q15_cache(Some(vec![TpchQ15ResultRow::default()]));
    assert_eq!(tpch_q15_cache().expect("q15").len(), 1);
    set_tpch_q16_cache(Some(vec![TpchQ16ResultRow::default()]));
    assert_eq!(tpch_q16_cache().expect("q16").len(), 1);
    set_tpch_q17_cache(Some(vec![TpchQ17ResultRow::default()]));
    assert_eq!(tpch_q17_cache().expect("q17").len(), 1);
    set_tpch_q18_cache(Some(vec![TpchQ18ResultRow::default()]));
    assert_eq!(tpch_q18_cache().expect("q18").len(), 1);
    set_tpch_q19_cache(Some(vec![TpchQ19ResultRow::default()]));
    assert_eq!(tpch_q19_cache().expect("q19").len(), 1);
    set_tpch_q20_cache(Some(vec![TpchQ20ResultRow::default()]));
    assert_eq!(tpch_q20_cache().expect("q20").len(), 1);
    set_tpch_q21_cache(Some(vec![TpchQ21ResultRow::default()]));
    assert_eq!(tpch_q21_cache().expect("q21").len(), 1);

    set_tpch_q2_cache(None);
    set_tpch_q3_cache(None);
    set_tpch_q4_cache(None);
    set_tpch_q5_cache(None);
    set_tpch_q7_cache(None);
    set_tpch_q8_cache(None);
    set_tpch_q9_cache(None);
    set_tpch_q10_cache(None);
    set_tpch_q11_cache(None);
    set_tpch_q12_cache(None);
    set_tpch_q13_cache(None);
    set_tpch_q14_cache(None);
    set_tpch_q15_cache(None);
    set_tpch_q16_cache(None);
    set_tpch_q17_cache(None);
    set_tpch_q18_cache(None);
    set_tpch_q19_cache(None);
    set_tpch_q20_cache(None);
    set_tpch_q21_cache(None);
    assert!(tpch_q21_cache().is_none());
}

#[test]
fn scalar_cache_index_and_ann_helpers_cover_hot_edges() {
    let i32_columns = vec![
        Column::Int32(NumericColumn::from_data(vec![1, 2, 3, 4])),
        Column::Int32(NumericColumn::from_data(vec![10, 20, 30, 40])),
    ];
    let i64_columns = vec![Column::Int64(NumericColumn::from_data(vec![5, 6, 7]))];

    let Column::Int64(sum_i32) =
        build_cached_sum_column(1, &DataType::Int32, &i32_columns).expect("sum i32")
    else {
        panic!("sum i32 column type");
    };
    assert_eq!(sum_i32.data(), &[100]);
    let Column::Float64(avg_i32) =
        build_cached_avg_column(0, &DataType::Int32, &i32_columns).expect("avg i32")
    else {
        panic!("avg i32 column type");
    };
    assert_eq!(avg_i32.data(), &[2.5]);
    let Column::Int64(sum_i64) =
        build_cached_sum_column(0, &DataType::Int64, &i64_columns).expect("sum i64")
    else {
        panic!("sum i64 column type");
    };
    assert_eq!(sum_i64.data(), &[18]);
    let empty_i32 = vec![Column::Int32(NumericColumn::from_data(Vec::new()))];
    let Column::Int64(null_sum) =
        build_cached_sum_column(0, &DataType::Int32, &empty_i32).expect("empty sum")
    else {
        panic!("empty sum column type");
    };
    assert!(null_sum.nulls().is_some());

    let pred_gt = ScalarExpr::Binary {
        op: BinaryOp::Gt,
        left: Box::new(ScalarExpr::Column {
            name: "id".to_owned(),
            index: 0,
            data_type: DataType::Int32,
        }),
        right: Box::new(ScalarExpr::Literal {
            value: Value::Int32(2),
            data_type: DataType::Int32,
        }),
        data_type: DataType::Bool,
    };
    let Column::Int64(filtered_sum) =
        build_cached_filter_sum_column(1, &DataType::Int32, &pred_gt, &i32_columns)
            .expect("filter sum")
    else {
        panic!("filter sum column type");
    };
    assert_eq!(filtered_sum.data(), &[70]);
    assert_eq!(extract_int32_col_op_lit(&pred_gt), Some((0, CmpOp::Gt, 2)));
    let pred_reverse = ScalarExpr::Binary {
        op: BinaryOp::Lt,
        left: Box::new(ScalarExpr::Literal {
            value: Value::Int64(7),
            data_type: DataType::Int64,
        }),
        right: Box::new(ScalarExpr::Column {
            name: "big".to_owned(),
            index: 0,
            data_type: DataType::Int64,
        }),
        data_type: DataType::Bool,
    };
    assert_eq!(
        extract_int64_col_op_lit(&pred_reverse),
        Some((0, CmpOp::Gt, 7))
    );
    assert_eq!(binary_op_to_cmp(BinaryOp::LtEq), Some(CmpOp::Le));
    assert_eq!(reverse_binary_op_to_cmp(BinaryOp::GtEq), Some(CmpOp::Le));
    assert_eq!(scalar_input_type_tag(&DataType::Int32), Some(0));
    assert_eq!(
        scalar_input_type_tag(&DataType::Text { max_len: None }),
        None
    );
    assert_eq!(cmp_op_tag(CmpOp::Ge), 5);

    let output_schema = Schema::new([Field::required("sum", DataType::Int64)]).expect("schema");
    let agg = LogicalAggregateExpr {
        func: AggregateFunc::Sum,
        arg: Some(ScalarExpr::Column {
            name: "value".to_owned(),
            index: 1,
            data_type: DataType::Int32,
        }),
        direct_arg: None,
        order_by: None,
        distinct: false,
        output_name: "sum".to_owned(),
        data_type: DataType::Int64,
    };
    assert!(build_cached_scalar_wire_key(&agg, &output_schema, Some(&pred_gt)).is_some());

    let schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::required("tenant", DataType::Text { max_len: None }),
    ])
    .expect("schema");
    let codec = RowCodec::new(schema.clone());
    let payload = codec
        .encode(&[Value::Int32(7), Value::Text("acme".to_owned())])
        .expect("payload");
    assert_eq!(
        decode_key_column(
            &payload,
            &schema,
            Some(0),
            &[],
            None,
            LogicalIndexMethod::Btree,
            &index_key::IndexKeyEncoding::Int32,
        )
        .expect("decode key"),
        Some(7)
    );
    assert!(
        decode_key_column(
            &payload,
            &schema,
            None,
            &[],
            None,
            LogicalIndexMethod::Btree,
            &index_key::IndexKeyEncoding::Int32,
        )
        .is_err()
    );
    let false_pred = ScalarExpr::Literal {
        value: Value::Bool(false),
        data_type: DataType::Bool,
    };
    assert_eq!(
        decode_key_column(
            &payload,
            &schema,
            Some(0),
            &[],
            Some(&false_pred),
            LogicalIndexMethod::Btree,
            &index_key::IndexKeyEncoding::Int32,
        )
        .expect("partial skip"),
        None
    );
    let non_bool_pred = ScalarExpr::Literal {
        value: Value::Int32(1),
        data_type: DataType::Int32,
    };
    assert!(
        decode_key_column(
            &payload,
            &schema,
            Some(0),
            &[],
            Some(&non_bool_pred),
            LogicalIndexMethod::Btree,
            &index_key::IndexKeyEncoding::Int32,
        )
        .is_err()
    );
    assert!(hash_index_value(&Value::Text("key".to_owned())).is_some());
    assert_eq!(hash_index_value(&Value::Null), None);

    assert_eq!(
        logical_index_method_from_name("hash"),
        LogicalIndexMethod::Hash
    );
    assert_eq!(
        logical_index_method_from_name("ivfflat"),
        LogicalIndexMethod::IvfFlat
    );
    assert_eq!(
        hnsw_metric_for_opclass_name(Some("vector_cosine_ops")).expect("metric"),
        HnswMetric::Cosine
    );
    assert!(hnsw_metric_for_opclass_name(Some("unknown_ops")).is_err());
    assert_eq!(
        ann_dims_and_default_payload(&DataType::HalfVec { dims: Some(3) }),
        Some((3, AnnPayloadKind::Bf16))
    );
    assert_eq!(
        ann_payload_option_from_catalog(&[("payload".to_owned(), "int8".to_owned())])
            .expect("payload"),
        Some(AnnPayloadKind::Int8)
    );
    assert!(ann_payload_option_from_catalog(&[("payload".to_owned(), "bad".to_owned())]).is_err());
    assert_eq!(
        ivfflat_options_from_catalog(&[
            ("lists".to_owned(), "8".to_owned()),
            ("probes".to_owned(), "2".to_owned()),
            ("payload".to_owned(), "bf16".to_owned()),
        ])
        .expect("ivfflat options"),
        (8, 2, Some(AnnPayloadKind::Bf16))
    );
    assert!(ivfflat_options_from_catalog(&[("lists".to_owned(), "0".to_owned())]).is_err());
    assert!(ivfflat_options_from_catalog(&[("unknown".to_owned(), "1".to_owned())]).is_err());
}

#[test]
fn server_config_backup_and_lock_helpers_cover_admin_edges() {
    let mut server = Server::with_sample_database();
    let autovacuum = AutovacuumConfig {
        vacuum_threshold: 7,
        vacuum_scale_factor_ppm: 10_000,
        analyze_threshold: 9,
        analyze_scale_factor_ppm: 20_000,
    };
    server.set_autovacuum_config(autovacuum);
    assert_eq!(server.autovacuum_config(), autovacuum);
    let logging = LoggingConfig {
        log_connections: true,
        log_min_duration_statement_ms: 5,
        log_statement: LogStatementMode::All,
    };
    server.set_logging_config(logging);
    assert_eq!(server.logging_config(), logging);
    server.set_idle_session_timeout_ms(123);
    assert_eq!(server.idle_session_timeout_ms(), 123);
    let archive = WalArchiveConfig {
        archive_command: "cp %p %f".to_owned(),
        restore_command: "cp %f %p".to_owned(),
    };
    server.set_wal_archive_config(archive.clone());
    assert_eq!(server.wal_archive_config(), archive);
    assert!(server.wal_writer_stats().is_none());
    assert_eq!(server.ann_system_metrics(), AnnSystemMetrics::default());
    assert_eq!(
        server
            .record_backup_marker("pg_start_backup")
            .expect("memory marker"),
        "0/0"
    );

    let dir = tempfile::tempdir().expect("data dir");
    let persistent = Server::init(dir.path()).expect("persistent server");
    assert_eq!(
        persistent
            .record_backup_marker("pg_start_backup")
            .expect("start marker"),
        "0/0"
    );
    assert!(dir.path().join("backup_label").exists());
    assert_eq!(
        persistent
            .record_backup_marker("pg_stop_backup")
            .expect("stop marker"),
        "0/0"
    );
    assert!(dir.path().join("backup_stop").exists());

    let scan = LogicalPlan::Scan {
        table: "t".to_owned(),
        schema: Schema::empty(),
        projection: None,
    };
    assert_eq!(lock_rows_base_filter(&scan), Some(("t", None)));
    let pred = ScalarExpr::Literal {
        value: Value::Bool(true),
        data_type: DataType::Bool,
    };
    let filter = LogicalPlan::Filter {
        input: Box::new(scan.clone()),
        predicate: pred.clone(),
    };
    assert_eq!(
        lock_rows_base_filter(&filter).map(|(table, has_pred)| (table, has_pred.is_some())),
        Some(("t", true))
    );
    let project = LogicalPlan::Project {
        input: Box::new(filter),
        exprs: Vec::new(),
        schema: Schema::empty(),
    };
    assert_eq!(
        lock_rows_base_filter(&project).map(|(table, has_pred)| (table, has_pred.is_some())),
        Some(("t", true))
    );
    assert!(
        lock_rows_base_filter(&LogicalPlan::Empty {
            schema: Schema::empty()
        })
        .is_none()
    );
    assert_eq!(row_lock_mode(LockStrength::Update), RowLockMode::ForUpdate);
    assert_eq!(
        row_lock_mode(LockStrength::NoKeyUpdate),
        RowLockMode::ForNoKeyUpdate
    );
    assert_eq!(row_lock_mode(LockStrength::Share), RowLockMode::ForShare);
    assert_eq!(
        row_lock_mode(LockStrength::KeyShare),
        RowLockMode::ForKeyShare
    );
    assert_eq!(value_i64(&Value::Bool(true)), Some(1));
    assert_eq!(value_i64(&Value::Int16(-2)), Some(-2));
    assert_eq!(value_i64(&Value::Text("x".to_owned())), None);
    assert!(unix_timestamp_micros() > 0);
}

#[cfg(unix)]
#[test]
fn data_dir_rejects_symlink_and_wrong_file_type() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().expect("tempdir");
    let target = dir.path().join("target");
    std::fs::create_dir(&target).expect("target dir");
    let link = dir.path().join("link");
    symlink(&target, &link).expect("symlink");
    assert!(reject_data_dir_symlink(&link).is_err());

    let file = dir.path().join("file");
    std::fs::write(&file, b"x").expect("file");
    assert!(validate_data_dir_owner(&file, effective_uid()).is_err());
    assert!(validate_data_dir_owner(&target, effective_uid()).is_ok());
}
