//! Parser, GUC/session-variable, and fast-insert test coverage.

use super::*;

#[test]
fn fast_dml_precheck_cache_keys_on_arc_identity_not_heap_address() {
    type S = Session<tokio::io::DuplexStream>;
    let session = test_session();

    // Two distinct, independently-allocated DELETE plans of the
    // cacheable shape. In production these are the pointer-stable
    // `stmt_cache` `Arc`s driving repeat executions.
    let cached = Arc::new(cacheable_delete_plan());
    let other = Arc::new(cacheable_delete_plan());

    // Nothing cached yet: neither plan is prechecked, and the cold /
    // view-rewrite `None` path is never prechecked.
    assert!(!session.fast_dml_prechecked(Some(&cached)));
    assert!(!session.fast_dml_prechecked(Some(&other)));
    assert!(!session.fast_dml_prechecked(None));

    // Simulate `cached` having passed its static DML checks, exactly as
    // `run_dml_or_select` does on the stable path (pin the `Arc`).
    let key = S::prechecked_fast_dml_key(&cached).expect("delete plan is cache-eligible");
    session
        .prechecked_fast_dml
        .borrow_mut()
        .insert(key, Arc::clone(&cached));

    // The genuinely-cached `Arc` hits; the distinct sibling does not.
    assert!(session.fast_dml_prechecked(Some(&cached)));
    assert!(!session.fast_dml_prechecked(Some(&other)));

    // Forge the ABA worst case directly: an entry stored under `other`'s
    // current heap address but holding a *different* plan's `Arc`
    // (`cached`). Under the old address-only `HashSet<usize>` this is
    // exactly the false positive that would skip RLS / column-privilege
    // / MV-source checks for `other`; `Arc::ptr_eq` must reject it.
    let collision_key = S::prechecked_fast_dml_key(&other).expect("delete plan is cache-eligible");
    assert_ne!(
        key, collision_key,
        "distinct live Arcs must occupy distinct addresses",
    );
    session
        .prechecked_fast_dml
        .borrow_mut()
        .insert(collision_key, Arc::clone(&cached));
    assert!(
        !session.fast_dml_prechecked(Some(&other)),
        "address collision with a distinct plan must not be a false cache hit",
    );

    // Invalidation drops the pinned `Arc`s, forcing the next execution
    // back through the full checks.
    session.plan_cache_invalidate();
    assert!(session.prechecked_fast_dml.borrow().is_empty());
    assert!(!session.fast_dml_prechecked(Some(&cached)));
}

#[test]
fn logical_replication_and_guc_parsers_cover_success_and_errors() {
    for value in ["on", "TRUE", "1", "yes"] {
        assert!(parse_bool_guc(value).expect("true guc"));
    }
    for value in ["off", "FALSE", "0", "no"] {
        assert!(!parse_bool_guc(value).expect("false guc"));
    }
    assert!(parse_bool_guc("maybe").is_err());
    assert_eq!(parse_statement_timeout_ms(" 250 ").expect("timeout"), 250);
    assert!(parse_statement_timeout_ms("-1").is_err());
    assert!(parse_statement_timeout_ms("abc").is_err());

    // work_mem: a bare integer is kilobytes; explicit units override.
    assert_eq!(
        parse_work_mem_bytes("1024").expect("bare kb"),
        1024 * 1024,
        "bare integer is interpreted as kB"
    );
    assert_eq!(parse_work_mem_bytes("64MB").expect("mb"), 64 * 1024 * 1024);
    assert_eq!(
        parse_work_mem_bytes(" 1 GB ").expect("gb with spaces"),
        1024 * 1024 * 1024
    );
    assert_eq!(parse_work_mem_bytes("4096B").expect("bytes"), 64 * 1024); // clamped to min
    assert_eq!(
        parse_work_mem_bytes("'8MB'").expect("quoted"),
        8 * 1024 * 1024
    );
    // Below the 64 KiB floor clamps up rather than making everything spill.
    assert_eq!(parse_work_mem_bytes("1").expect("tiny"), 64 * 1024);
    assert!(parse_work_mem_bytes("5furlongs").is_err());
    assert!(parse_work_mem_bytes("MB").is_err());
    assert!(parse_work_mem_bytes("").is_err());

    assert!(starts_with_keyword_pair(
        "create publication pub for table t",
        "CREATE",
        "PUBLICATION",
    ));
    assert!(!starts_with_keyword_pair("create", "CREATE", "PUBLICATION"));
    assert_eq!(
        split_first_token("  name rest ").expect("token"),
        ("name", "rest")
    );
    assert!(split_first_token("   ").is_err());
    assert_eq!(
        parse_publication_tables("FOR TABLE users, \"Orders\"").expect("tables"),
        vec!["users".to_owned(), "Orders".to_owned()]
    );
    assert!(parse_publication_tables("FOR ALL TABLES").is_err());
    assert!(parse_publication_tables("FOR TABLE ,").is_err());
    assert_eq!(
        parse_quoted_literal(" 'conn info' PUBLICATION pub").expect("literal"),
        ("conn info", "PUBLICATION pub")
    );
    assert!(parse_quoted_literal("conn").is_err());
    assert!(parse_quoted_literal("'unterminated").is_err());
    assert_eq!(
        parse_subscription_publications("PUBLICATION pub1, \"Pub2\" WITH (slot_name='s')")
            .expect("publications"),
        vec!["pub1".to_owned(), "Pub2".to_owned()]
    );
    assert!(parse_subscription_publications("WITH ()").is_err());
    assert_eq!(
        parse_subscription_slot_name("PUBLICATION pub WITH (copy_data=false, slot_name='slot_a')")
            .expect("slot"),
        Some("slot_a".to_owned())
    );
    assert_eq!(
        parse_subscription_slot_name("PUBLICATION pub").expect("no slot"),
        None
    );

    let subscription = parse_create_subscription(
        "sub CONNECTION 'host=localhost' PUBLICATION pub WITH (slot_name = \"slot_b\")",
    )
    .expect("subscription");
    assert_eq!(
        subscription,
        LogicalReplicationDdl::CreateSubscription {
            name: "sub".to_owned(),
            conninfo: "host=localhost".to_owned(),
            publications: vec!["pub".to_owned()],
            slot_name: Some("slot_b".to_owned()),
        }
    );
    assert!(parse_create_subscription("sub PUBLICATION pub").is_err());

    assert_eq!(
        Session::<tokio::io::DuplexStream>::try_parse_logical_replication_ddl(
            "CREATE PUBLICATION pub FOR TABLE users;"
        )
        .expect("parse publication"),
        Some(LogicalReplicationDdl::CreatePublication {
            name: "pub".to_owned(),
            tables: vec!["users".to_owned()],
        })
    );
    assert_eq!(
        Session::<tokio::io::DuplexStream>::try_parse_logical_replication_ddl(
            "DROP PUBLICATION IF EXISTS pub"
        )
        .expect("drop publication"),
        Some(LogicalReplicationDdl::DropPublication {
            name: "pub".to_owned(),
            if_exists: true,
        })
    );
    assert_eq!(
        Session::<tokio::io::DuplexStream>::try_parse_logical_replication_ddl(
            "DROP SUBSCRIPTION sub"
        )
        .expect("drop subscription"),
        Some(LogicalReplicationDdl::DropSubscription {
            name: "sub".to_owned(),
            if_exists: false,
        })
    );
    assert!(
        Session::<tokio::io::DuplexStream>::try_parse_logical_replication_ddl("SELECT 1")
            .expect("not ddl")
            .is_none()
    );
}

#[test]
fn session_variable_surface_sets_shows_and_resets_supported_gucs() {
    let mut session = test_session();
    session
        .apply_session_variable("jit", "on")
        .expect("set jit");
    assert!(session.jit_enabled);
    session
        .apply_session_variable("jit_above_cost", "123")
        .expect("set jit threshold");
    assert_eq!(session.jit_above_rows, 123);
    session
        .apply_session_variable("statement_timeout", "50")
        .expect("set timeout");
    assert_eq!(session.statement_timeout_ms, 50);
    session
        .apply_session_variable("work_mem", "8MB")
        .expect("set work_mem");
    assert_eq!(
        session.session_settings.get("work_mem").map(String::as_str),
        Some((8 * 1024 * 1024).to_string().as_str())
    );
    session
        .apply_session_variable("extra_float_digits", "3")
        .expect("extra_float_digits");
    session
        .apply_session_variable("application_name", "cert")
        .expect("application name");
    session
        .apply_session_variable("client_min_messages", "WARNING")
        .expect("client min messages");
    session
        .apply_session_variable("client_encoding", "UTF8")
        .expect("encoding");
    session
        .apply_session_variable("datestyle", "SQL, DMY")
        .expect("datestyle");
    session
        .apply_session_variable("search_path", "app, public")
        .expect("search path");
    session
        .apply_session_variable("intervalstyle", "iso_8601")
        .expect("intervalstyle");
    session
        .apply_session_variable("lc_monetary", "C")
        .expect("lc_monetary");
    session
        .apply_session_variable("timezone", "America/Bogota")
        .expect("timezone");
    session
        .apply_session_variable("timezone", "+02:30")
        .expect("fixed timezone");
    session
        .apply_session_variable("standard_conforming_strings", "on")
        .expect("strings");
    session
        .apply_session_variable("synchronous_commit", "remote_write")
        .expect("sync commit");
    session
        .apply_session_variable("ultrasql.tenant", "acme")
        .expect("custom guc");

    assert_eq!(
        first_data_row_text(
            &session
                .show_session_variable("jit", true)
                .expect("show jit")
        ),
        "on"
    );
    assert_eq!(
        first_data_row_text(
            &session
                .show_session_variable("timezone", false)
                .expect("show timezone")
        ),
        "+02:30"
    );
    assert_eq!(
        first_data_row_text(
            &session
                .show_session_variable("ultrasql.tenant", false)
                .expect("show custom")
        ),
        "acme"
    );
    assert_eq!(
        first_data_row_text(
            &session
                .show_session_variable("lc_monetary", false)
                .expect("show lc_monetary")
        ),
        "C"
    );
    assert_eq!(
        first_data_row_text(
            &session
                .show_session_variable("datestyle", false)
                .expect("show datestyle")
        ),
        "SQL, DMY"
    );
    assert_eq!(
        first_data_row_text(
            &session
                .show_session_variable("server_version", true)
                .expect("show version")
        ),
        crate::REPORTED_SERVER_VERSION
    );
    assert_eq!(
        first_data_row_text(
            &session
                .show_session_variable("work_mem", false)
                .expect("show work_mem")
        ),
        (8 * 1024 * 1024).to_string(),
        "SHOW work_mem reflects the session value in bytes"
    );

    assert!(session.apply_session_variable("jit", "maybe").is_err());
    assert!(
        session
            .apply_session_variable("work_mem", "10toads")
            .is_err()
    );
    assert!(
        session
            .apply_session_variable("jit_above_cost", "bad")
            .is_err()
    );
    assert!(
        session
            .apply_session_variable("extra_float_digits", "4")
            .is_err()
    );
    assert!(
        session
            .apply_session_variable("client_min_messages", "loud")
            .is_err()
    );
    assert!(
        session
            .apply_session_variable("client_encoding", "LATIN1")
            .is_err()
    );
    assert!(session.apply_session_variable("datestyle", "moon").is_err());
    assert!(
        session
            .apply_session_variable("timezone", "No/SuchZone")
            .is_err()
    );
    assert!(
        session
            .apply_session_variable("intervalstyle", "bad")
            .is_err()
    );
    assert!(
        session
            .apply_session_variable("standard_conforming_strings", "off")
            .is_err()
    );
    assert!(
        session
            .apply_session_variable("synchronous_commit", "bad")
            .is_err()
    );
    assert!(session.apply_session_variable("unknown", "x").is_err());

    for name in [
        "jit",
        "jit_above_cost",
        "statement_timeout",
        "work_mem",
        "extra_float_digits",
        "application_name",
        "client_min_messages",
        "client_encoding",
        "datestyle",
        "search_path",
        "intervalstyle",
        "lc_monetary",
        "timezone",
        "synchronous_commit",
        "ultrasql.tenant",
    ] {
        session
            .execute_set_variable_reset(name)
            .unwrap_or_else(|_| panic!("reset {name}"));
    }
    assert!(!session.jit_enabled);
    assert_eq!(session.statement_timeout_ms, 0);
    assert!(!session.session_settings.contains_key("ultrasql.tenant"));
    assert!(session.execute_set_variable_reset("unsupported").is_err());

    let show_plan = LogicalPlan::SetVariable {
        name: "client_encoding".to_owned(),
        action: LogicalSetVariableAction::Show,
        value: None,
        schema: Schema::new([Field::required(
            "client_encoding",
            DataType::Text { max_len: None },
        )])
        .expect("show schema"),
    };
    assert_eq!(
        first_data_row_text(
            &session
                .execute_set_variable(&show_plan, true)
                .expect("execute show")
        ),
        "UTF8"
    );
    let wrong = LogicalPlan::Values {
        rows: Vec::new(),
        schema: Schema::empty(),
    };
    assert!(session.execute_set_variable(&wrong, true).is_err());
}

#[test]
fn fast_insert_int32_pair_parser_accepts_simple_values_only() {
    let parsed = Session::<tokio::io::DuplexStream>::parse_fast_insert_int32_pair_sql(
        "INSERT INTO bench_insert_0 VALUES (1, 10),(-2,0)",
    )
    .expect("fast insert should parse");

    assert_eq!(parsed.table, "bench_insert_0");
    assert_eq!(parsed.rows, vec![(1, 10), (-2, 0)]);
    assert!(
        Session::<tokio::io::DuplexStream>::parse_fast_insert_int32_pair_sql(
            "INSERT INTO bench_insert_0 (id, val) VALUES (1, 10)"
        )
        .is_none()
    );
    assert!(
        Session::<tokio::io::DuplexStream>::parse_fast_insert_int32_pair_sql(
            "INSERT INTO bench_insert_0 VALUES (1, 10) RETURNING id"
        )
        .is_none()
    );
    assert!(
        Session::<tokio::io::DuplexStream>::parse_fast_insert_int32_pair_sql(
            "INSERT INTO bench_insert_0 VALUES (2147483648, 10)"
        )
        .is_none()
    );
}

#[test]
fn fast_insert_int32_pair_dispatches_benchmark_table() {
    let mut session = test_session();
    session
        .execute_query(
            "CREATE TABLE bench_insert_0 (id INT NOT NULL, val INT)",
            false,
        )
        .expect("create benchmark table");
    let snapshot = session.state.catalog_snapshot();

    let result = session
        .try_execute_fast_insert_int32_pair_sql(
            "INSERT INTO bench_insert_0 VALUES (1,10),(2,20)",
            &snapshot,
        )
        .expect("fast insert dispatch should not error");

    assert!(result.is_some(), "benchmark INSERT must hit fast path");
}
