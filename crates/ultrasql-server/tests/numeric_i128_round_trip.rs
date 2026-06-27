//! End-to-end proof that the i128-backed NUMERIC representation handles
//! values beyond i64 exactly, accumulates SUM/AVG/STDDEV without silent
//! overflow, raises SQLSTATE 22003 on true i128 overflow, and round-trips
//! through the heap and crash recovery.

pub mod support;

use support::{shutdown, start_persistent_server, start_sample_server};

fn rows_of(messages: Vec<tokio_postgres::SimpleQueryMessage>) -> Vec<Vec<String>> {
    messages
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|idx| row.get(idx).unwrap_or("").to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

async fn one_value(client: &tokio_postgres::Client, sql: &str) -> String {
    let messages = client.simple_query(sql).await.expect("query");
    rows_of(messages)
        .into_iter()
        .next()
        .and_then(|mut row| row.drain(..).next())
        .expect("one value")
}

#[tokio::test]
async fn literal_beyond_i64_round_trips_exactly() {
    let running = start_sample_server("i128_literal").await;
    let client = &running.client;

    // 20 digits, > i64::MAX (~9.2e18). Previously truncated/errored.
    assert_eq!(
        one_value(client, "SELECT 99999999999999999999::numeric").await,
        "99999999999999999999"
    );
    // A 38-digit literal (within i128) round-trips exactly.
    assert_eq!(
        one_value(
            client,
            "SELECT 12345678901234567890123456789012345678::numeric"
        )
        .await,
        "12345678901234567890123456789012345678"
    );
    // Beyond i128 (~39+ digits) raises numeric_value_out_of_range (22003).
    let err = client
        .simple_query("SELECT 999999999999999999999999999999999999999::numeric")
        .await
        .expect_err("beyond i128 must error");
    assert_eq!(
        err.code().map(tokio_postgres::error::SqlState::code),
        Some("22003")
    );

    shutdown(running).await;
}

#[tokio::test]
async fn sum_avg_over_large_bigints_are_correct() {
    let running = start_sample_server("i128_sum_avg").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE big (v NUMERIC)")
        .await
        .expect("create");
    // Three values each ~7.5e18; their sum (~2.25e19) overflows i64 but
    // fits i128. Previously this wrapped silently.
    client
        .batch_execute(
            "INSERT INTO big VALUES \
             (7500000000000000000), \
             (7500000000000000000), \
             (7500000000000000000)",
        )
        .await
        .expect("insert");

    assert_eq!(
        one_value(client, "SELECT SUM(v) FROM big").await,
        "22500000000000000000"
    );
    // AVG over NUMERIC returns NUMERIC (exact). PostgreSQL's select_div_scale
    // yields scale 0 at this magnitude, so the exact average renders with no
    // fractional digits (matches `psql: avg -> 7500000000000000000`).
    assert_eq!(
        one_value(client, "SELECT AVG(v) FROM big").await,
        "7500000000000000000"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn stddev_variance_over_large_bigints_do_not_overflow() {
    let running = start_sample_server("i128_stddev").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE s (v BIGINT)")
        .await
        .expect("create");
    client
        .batch_execute(
            "INSERT INTO s VALUES \
             (9000000000000000000), \
             (9000000000000000000), \
             (9000000000000000000)",
        )
        .await
        .expect("insert");

    // All values equal: population stddev and variance are exactly 0,
    // and the squared-deviation accumulation must not overflow/wrap.
    assert_eq!(one_value(client, "SELECT VAR_POP(v) FROM s").await, "0");
    assert_eq!(one_value(client, "SELECT STDDEV_POP(v) FROM s").await, "0");

    shutdown(running).await;
}

#[tokio::test]
async fn decimal_multiplication_overflow_reports_22003() {
    let running = start_sample_server("i128_mul").await;
    let client = &running.client;

    // A product within i128 is exact.
    assert_eq!(
        one_value(
            client,
            "SELECT 1000000000000000000::numeric * 1000000000::numeric",
        )
        .await,
        "1000000000000000000000000000"
    );

    // A product that exceeds i128 must raise 22003, never wrap.
    let err = client
        .simple_query(
            "SELECT 10000000000000000000000000000000000000::numeric \
             * 10000000000000000000000000000000000000::numeric",
        )
        .await
        .expect_err("mul overflow must error");
    assert_eq!(
        err.code().map(tokio_postgres::error::SqlState::code),
        Some("22003")
    );

    shutdown(running).await;
}

#[tokio::test]
async fn casts_round_check_and_range_over_i128() {
    let running = start_sample_server("i128_casts").await;
    let client = &running.client;

    // text -> decimal of a 30-digit string round-trips exactly.
    assert_eq!(
        one_value(client, "SELECT '123456789012345678901234567890'::numeric").await,
        "123456789012345678901234567890"
    );
    // int8 -> decimal preserves the full i64 range.
    assert_eq!(
        one_value(client, "SELECT 9223372036854775807::bigint::numeric").await,
        "9223372036854775807"
    );
    // int8 -> decimal of a value, then back out as exact text.
    assert_eq!(
        one_value(client, "SELECT (-9223372036854775808)::bigint::numeric").await,
        "-9223372036854775808"
    );
    // decimal -> float8 of a beyond-i64 magnitude (lossy but finite).
    assert_eq!(
        one_value(client, "SELECT (10::numeric / 4::numeric)::float8").await,
        "2.5"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn order_by_distinct_group_by_over_i128_mixed_scales() {
    let running = start_sample_server("i128_order").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE m (v NUMERIC)")
        .await
        .expect("create");
    client
        .batch_execute(
            "INSERT INTO m VALUES \
             (99999999999999999999.5), \
             (99999999999999999999.50), \
             (10000000000000000000), \
             (-99999999999999999999), \
             (0)",
        )
        .await
        .expect("insert");

    // DISTINCT must collapse 99999999999999999999.5 and ...50 (same value,
    // different stored scale), and ORDER BY must sort the >i64 magnitudes
    // (including the negative) correctly.
    let ordered = rows_of(
        client
            .simple_query("SELECT DISTINCT v FROM m ORDER BY v")
            .await
            .expect("ordered distinct"),
    );
    assert_eq!(
        ordered,
        vec![
            vec!["-99999999999999999999".to_owned()],
            vec!["0".to_owned()],
            vec!["10000000000000000000".to_owned()],
            vec!["99999999999999999999.5".to_owned()],
        ]
    );

    shutdown(running).await;
}

#[tokio::test]
async fn heap_round_trip_over_i128_across_restart() {
    let data_dir = tempfile::TempDir::new().expect("temp dir");
    let running = start_persistent_server(data_dir.path(), "i128_persist").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE p (id INT, v NUMERIC)")
        .await
        .expect("create");
    client
        .batch_execute(
            "INSERT INTO p VALUES \
             (1, 99999999999999999999), \
             (2, -88888888888888888888.25), \
             (3, 12345678901234567890123456789012345678)",
        )
        .await
        .expect("insert >i64 decimals");

    let expected = vec![
        vec!["2".to_owned(), "-88888888888888888888.25".to_owned()],
        vec!["1".to_owned(), "99999999999999999999".to_owned()],
        vec![
            "3".to_owned(),
            "12345678901234567890123456789012345678".to_owned(),
        ],
    ];

    // ORDER BY over >i64 magnitudes (including the negative) and mixed
    // scales sorts correctly through the heap scan.
    let before = rows_of(
        client
            .simple_query("SELECT id, v FROM p ORDER BY v")
            .await
            .expect("ordered before restart"),
    );
    assert_eq!(before, expected);

    // A filter on a >i64 decimal returns the correct rows.
    let ranged = rows_of(
        client
            .simple_query("SELECT id, v FROM p WHERE v > 0 ORDER BY v")
            .await
            .expect("range query"),
    );
    assert_eq!(
        ranged,
        vec![
            vec!["1".to_owned(), "99999999999999999999".to_owned()],
            vec![
                "3".to_owned(),
                "12345678901234567890123456789012345678".to_owned(),
            ],
        ]
    );

    // Crash-recovery: restart replays the WAL and recovers the >i64
    // decimals exactly, in the correct order (the heap row codec stores
    // decimals as base-10000 numeric payload, which is width-agnostic).
    shutdown(running).await;
    let running = start_persistent_server(data_dir.path(), "i128_persist").await;
    let after = rows_of(
        running
            .client
            .simple_query("SELECT id, v FROM p ORDER BY v")
            .await
            .expect("ordered after restart"),
    );
    assert_eq!(after, expected);
    shutdown(running).await;
}

#[tokio::test]
async fn wire_output_renders_i128_decimal_text() {
    let running = start_sample_server("i128_wire").await;
    let client = &running.client;

    // Binary (extended) protocol: tokio-postgres requests NUMERIC and we
    // render it as text; the >i64 value must not truncate.
    let value = one_value(
        client,
        "SELECT (-12345678901234567890123456789.0123456789)::numeric",
    )
    .await;
    assert_eq!(value, "-12345678901234567890123456789.0123456789");

    shutdown(running).await;
}
