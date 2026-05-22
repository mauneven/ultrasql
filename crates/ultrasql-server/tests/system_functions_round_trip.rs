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
