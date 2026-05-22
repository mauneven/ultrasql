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
             sha256('abc')",
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
