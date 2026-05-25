//! End-to-end scalar system-function compatibility tests.

mod support;

use support::{shutdown, start_sample_server};

#[tokio::test]
async fn jdbc_startup_runtime_parameters_round_trip() {
    let running = start_sample_server("system_functions_test").await;
    let client = &running.client;

    client
        .batch_execute("SET extra_float_digits = 3")
        .await
        .expect("JDBC startup SET extra_float_digits succeeds");
    let row = client
        .query_one("SHOW extra_float_digits", &[])
        .await
        .expect("SHOW extra_float_digits");
    assert_eq!(row.get::<_, String>(0), "3");
    client
        .batch_execute("RESET extra_float_digits")
        .await
        .expect("RESET extra_float_digits succeeds");
    let row = client
        .query_one("SHOW extra_float_digits", &[])
        .await
        .expect("SHOW reset extra_float_digits");
    assert_eq!(row.get::<_, String>(0), "1");

    client
        .batch_execute("SET application_name = 'driver_cert_jdbc'")
        .await
        .expect("JDBC startup SET application_name succeeds");
    let row = client
        .query_one("SHOW application_name", &[])
        .await
        .expect("SHOW application_name");
    assert_eq!(row.get::<_, String>(0), "driver_cert_jdbc");

    shutdown(running).await;
}

#[tokio::test]
async fn orm_startup_runtime_parameters_round_trip() {
    let running = start_sample_server("system_functions_test").await;
    let client = &running.client;

    client
        .batch_execute("SET client_min_messages = warning")
        .await
        .expect("Rails startup SET client_min_messages succeeds");
    let row = client
        .query_one("SHOW client_min_messages", &[])
        .await
        .expect("SHOW client_min_messages");
    assert_eq!(row.get::<_, String>(0), "warning");

    client
        .batch_execute("SET intervalstyle = iso_8601")
        .await
        .expect("Rails startup SET intervalstyle succeeds");
    let row = client
        .query_one("SHOW intervalstyle", &[])
        .await
        .expect("SHOW intervalstyle");
    assert_eq!(row.get::<_, String>(0), "iso_8601");

    client
        .batch_execute("SET SESSION timezone TO 'UTC'")
        .await
        .expect("Rails startup SET timezone succeeds");
    let row = client
        .query_one("SHOW timezone", &[])
        .await
        .expect("SHOW timezone");
    assert_eq!(row.get::<_, String>(0), "UTC");

    client
        .batch_execute("SET CLIENT_ENCODING TO 'UTF8'")
        .await
        .expect("Diesel startup SET CLIENT_ENCODING succeeds");
    let row = client
        .query_one("SHOW client_encoding", &[])
        .await
        .expect("SHOW client_encoding");
    assert_eq!(row.get::<_, String>(0), "UTF8");

    let row = client
        .query_one("SHOW server_version", &[])
        .await
        .expect("SHOW server_version");
    assert_eq!(row.get::<_, String>(0), "14.0");

    let row = client
        .query_one("SHOW max_identifier_length", &[])
        .await
        .expect("SHOW max_identifier_length");
    assert_eq!(row.get::<_, String>(0), "63");

    let row = client
        .query_one("SHOW search_path", &[])
        .await
        .expect("SHOW search_path");
    assert_eq!(row.get::<_, String>(0), "\"$user\", public");

    client
        .batch_execute("SET LOCAL search_path TO public, \"$user\"")
        .await
        .expect("SET LOCAL search_path list");
    let row = client
        .query_one("SHOW search_path", &[])
        .await
        .expect("SHOW search_path list");
    assert_eq!(row.get::<_, String>(0), "public, \"$user\"");

    let row = client
        .query_one("SHOW transaction isolation level", &[])
        .await
        .expect("SHOW transaction isolation level");
    assert_eq!(row.get::<_, String>(0), "read committed");

    let row = client
        .query_one("SHOW standard_conforming_strings", &[])
        .await
        .expect("SHOW standard_conforming_strings");
    assert_eq!(row.get::<_, String>(0), "on");

    let row = client
        .query_one("SELECT set_config('TimeZone', 'UTC', false)", &[])
        .await
        .expect("Django startup set_config TimeZone");
    assert_eq!(row.get::<_, String>(0), "UTC");

    shutdown(running).await;
}

#[tokio::test]
async fn scalar_system_functions_return_postgres_shaped_values() {
    let running = start_sample_server("system_functions_test").await;
    let client = &running.client;

    let rows = client
        .query(
            "SELECT version(), current_database(), current_schema(), current_user(), pg_typeof(1), pg_size_pretty(2048)",
            &[],
        )
        .await
        .expect("system functions");
    assert_eq!(rows.len(), 1);
    let version = rows[0].get::<_, String>(0);
    assert!(
        version.starts_with("PostgreSQL 14.0"),
        "ORMs require PostgreSQL-shaped version(), got {version:?}"
    );
    assert!(
        version.contains("UltraSQL 0.0.1"),
        "version() should retain UltraSQL identity, got {version:?}"
    );
    assert_eq!(rows[0].get::<_, String>(1), "ultrasql");
    assert_eq!(rows[0].get::<_, String>(2), "public");
    assert_eq!(rows[0].get::<_, String>(3), "tester");
    assert_eq!(rows[0].get::<_, String>(4), "integer");
    assert_eq!(rows[0].get::<_, String>(5), "2 kB");

    let bare = client
        .query("SELECT current_user, session_user, current_catalog", &[])
        .await
        .expect("bare user functions");
    assert_eq!(bare.len(), 1);
    assert_eq!(bare[0].get::<_, String>(0), "tester");
    assert_eq!(bare[0].get::<_, String>(1), "tester");
    assert_eq!(bare[0].get::<_, String>(2), "ultrasql");

    shutdown(running).await;
}

#[tokio::test]
async fn scalar_string_functions_return_postgres_shaped_values() {
    let running = start_sample_server("system_functions_test").await;
    let client = &running.client;

    let row = client
        .query_one(
            "SELECT \
             length('UltraSQL'), \
             left('UltraSQL', 5), \
             right('UltraSQL', 3), \
             substr('UltraSQL', 6, 3), \
             trim('  hi  '), \
             lpad('7', 3, '0'), \
             rpad('x', 3, '-'), \
             position('SQL', 'UltraSQL'), \
             replace('aa-bb', '-', '+'), \
             split_part('a,b,c', ',', 2), \
             concat('AI', NULL, '-', 1), \
             concat_ws('|', 'a', NULL, 'b'), \
             repeat('ha', 3), \
             reverse('abc'), \
             md5('abc'), \
             sha256('abc'), \
             quote_ident('simple_name'), \
             quote_ident('select'), \
             format('hello %s %I %L %%', 'world', 'odd name', 'O''Reilly'), \
             regexp_replace('abc123abc', '[a-z]+', 'X'), \
             regexp_replace('abc123abc', '[a-z]+', 'X', 'g')",
            &[],
        )
        .await
        .expect("string functions");

    assert_eq!(row.get::<_, i32>(0), 8);
    assert_eq!(row.get::<_, String>(1), "Ultra");
    assert_eq!(row.get::<_, String>(2), "SQL");
    assert_eq!(row.get::<_, String>(3), "SQL");
    assert_eq!(row.get::<_, String>(4), "hi");
    assert_eq!(row.get::<_, String>(5), "007");
    assert_eq!(row.get::<_, String>(6), "x--");
    assert_eq!(row.get::<_, i32>(7), 6);
    assert_eq!(row.get::<_, String>(8), "aa+bb");
    assert_eq!(row.get::<_, String>(9), "b");
    assert_eq!(row.get::<_, String>(10), "AI-1");
    assert_eq!(row.get::<_, String>(11), "a|b");
    assert_eq!(row.get::<_, String>(12), "hahaha");
    assert_eq!(row.get::<_, String>(13), "cba");
    assert_eq!(row.get::<_, String>(14), "900150983cd24fb0d6963f7d28e17f72");
    assert_eq!(
        row.get::<_, String>(15),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert_eq!(row.get::<_, String>(16), "simple_name");
    assert_eq!(row.get::<_, String>(17), "\"select\"");
    assert_eq!(
        row.get::<_, String>(18),
        "hello world \"odd name\" 'O''Reilly' %"
    );
    assert_eq!(row.get::<_, String>(19), "X123abc");
    assert_eq!(row.get::<_, String>(20), "X123X");

    shutdown(running).await;
}

#[tokio::test]
async fn scalar_math_functions_return_postgres_shaped_values() {
    let running = start_sample_server("system_functions_test").await;
    let client = &running.client;

    let row = client
        .query_one(
            "SELECT \
             ceil(1.2), floor(1.8), round(1.6), trunc(1.9), \
             mod(7, 3), power(2, 5), sqrt(9), exp(0), ln(1), log(100), \
             pi(), sin(0), cos(0), tan(0), asin(0), acos(1), atan(0), random()",
            &[],
        )
        .await
        .expect("math functions");

    let expected = [
        2.0,
        1.0,
        2.0,
        1.0,
        1.0,
        32.0,
        3.0,
        1.0,
        0.0,
        2.0,
        std::f64::consts::PI,
        0.0,
        1.0,
        0.0,
        0.0,
        0.0,
        0.0,
    ];
    for (idx, expected_value) in expected.into_iter().enumerate() {
        let got: f64 = row.get(idx);
        assert!(
            (got - expected_value).abs() < 1e-12,
            "column {idx}: expected {expected_value}, got {got}"
        );
    }
    let random_value: f64 = row.get(17);
    assert!(
        (0.0..1.0).contains(&random_value),
        "random() out of range: {random_value}"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn scalar_datetime_functions_return_postgres_shaped_values() {
    let running = start_sample_server("system_functions_test").await;
    let client = &running.client;

    let row = client
        .query_one(
            "SELECT \
             extract(year FROM DATE '2024-05-22'), \
             extract(hour FROM TIMESTAMP '2024-05-22 13:14:15'), \
             extract(epoch FROM TIMESTAMP '2000-01-02 00:00:00'), \
             extract(year FROM current_date) >= 2026, \
             extract(epoch FROM to_timestamp(86400)), \
             extract(day FROM make_date(2024, 2, 29)), \
             extract(day FROM date_trunc('month', TIMESTAMP '2024-05-22 13:14:15')), \
             extract(hour FROM date_trunc('day', TIMESTAMP '2024-05-22 13:14:15')), \
             extract(day FROM age(TIMESTAMP '2024-05-22 13:14:15', TIMESTAMP '2024-05-20 12:14:15')), \
             extract(day FROM date_bin(INTERVAL '1' DAY, TIMESTAMP '2000-01-03 15:00:00', TIMESTAMP '2000-01-01 00:00:00'))",
            &[],
        )
        .await
        .expect("datetime functions");

    assert_eq!(row.get::<_, i64>(0), 2024);
    assert_eq!(row.get::<_, i64>(1), 13);
    assert_eq!(row.get::<_, i64>(2), 946_771_200);
    assert!(row.get::<_, bool>(3));
    assert_eq!(row.get::<_, i64>(4), 86_400);
    assert_eq!(row.get::<_, i64>(5), 29);
    assert_eq!(row.get::<_, i64>(6), 1);
    assert_eq!(row.get::<_, i64>(7), 0);
    assert_eq!(row.get::<_, i64>(8), 2);
    assert_eq!(row.get::<_, i64>(9), 3);

    shutdown(running).await;
}

#[tokio::test]
async fn pg_relation_size_reports_heap_pages() {
    let running = start_sample_server("system_functions_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE sized (id INT NOT NULL, name TEXT)")
        .await
        .expect("create sized table");
    client
        .batch_execute("INSERT INTO sized VALUES (1, 'a'), (2, 'b')")
        .await
        .expect("insert sized rows");

    let rows = client
        .query(
            "SELECT pg_relation_size('sized'), pg_size_pretty(pg_relation_size('public.sized'))",
            &[],
        )
        .await
        .expect("relation size");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>(0), 8192);
    assert_eq!(rows[0].get::<_, String>(1), "8 kB");

    shutdown(running).await;
}
