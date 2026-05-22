//! End-to-end scalar system-function compatibility tests.

mod support;

use support::{shutdown, start_sample_server};

#[tokio::test]
async fn scalar_system_functions_return_postgres_shaped_values() {
    let running = start_sample_server("system_functions_test").await;
    let client = &running.client;

    let rows = client
        .query(
            "SELECT version(), current_database(), current_user(), pg_typeof(1), pg_size_pretty(2048)",
            &[],
        )
        .await
        .expect("system functions");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "UltraSQL 0.0.1");
    assert_eq!(rows[0].get::<_, String>(1), "ultrasql");
    assert_eq!(rows[0].get::<_, String>(2), "user");
    assert_eq!(rows[0].get::<_, String>(3), "integer");
    assert_eq!(rows[0].get::<_, String>(4), "2 kB");

    let bare = client
        .query("SELECT current_user, session_user", &[])
        .await
        .expect("bare user functions");
    assert_eq!(bare.len(), 1);
    assert_eq!(bare[0].get::<_, String>(0), "user");
    assert_eq!(bare[0].get::<_, String>(1), "user");

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
