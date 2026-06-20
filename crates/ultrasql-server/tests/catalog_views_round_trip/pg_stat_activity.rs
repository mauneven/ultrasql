//! `pg_stat_activity` views: session identity and listing of open sessions.

use super::*;

#[tokio::test]
async fn pg_stat_activity_reflects_session_identity() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    let startup_app = client
        .query_one("SHOW application_name", &[])
        .await
        .expect("startup application_name");
    assert_eq!(startup_app.get::<_, String>(0), "catalog_views_test");

    client
        .batch_execute("SET application_name = 'activity_probe'")
        .await
        .expect("set application_name");
    let row = client
        .query_one(
            "SELECT usename, application_name, state \
             FROM pg_catalog.pg_stat_activity \
             WHERE datname = 'ultrasql'",
            &[],
        )
        .await
        .expect("pg_stat_activity current session");
    assert_eq!(row.get::<_, String>(0), "tester");
    assert_eq!(row.get::<_, String>(1), "activity_probe");
    assert_eq!(row.get::<_, String>(2), "active");

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn pg_stat_activity_lists_open_sessions() {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());
    let server_handle = tokio::spawn(serve_listener(listener, Arc::clone(&server)));

    let conn_a = format!(
        "host={host} port={port} user=activity_a application_name=activity_a",
        host = bound.ip(),
        port = bound.port()
    );
    let (client_a, connection_a) = tokio_postgres::connect(&conn_a, NoTls)
        .await
        .expect("connect activity_a");
    let conn_handle_a = tokio::spawn(async move {
        if let Err(e) = connection_a.await {
            eprintln!("connection a error: {e}");
        }
    });

    let conn_b = format!(
        "host={host} port={port} user=activity_b application_name=activity_b",
        host = bound.ip(),
        port = bound.port()
    );
    let (client_b, connection_b) = tokio_postgres::connect(&conn_b, NoTls)
        .await
        .expect("connect activity_b");
    let conn_handle_b = tokio::spawn(async move {
        if let Err(e) = connection_b.await {
            eprintln!("connection b error: {e}");
        }
    });

    let rows = client_a
        .query(
            "SELECT usename, application_name, state, query, \
                    backend_start IS NOT NULL, \
                    xact_start IS NOT NULL, \
                    query_start IS NOT NULL, \
                    state_change IS NOT NULL, \
                    wait_event_type, wait_event \
             FROM pg_catalog.pg_stat_activity \
             WHERE application_name IN ('activity_a', 'activity_b') \
             ORDER BY application_name",
            &[],
        )
        .await
        .expect("pg_stat_activity open sessions");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>(0), "activity_a");
    assert_eq!(rows[0].get::<_, String>(1), "activity_a");
    assert_eq!(rows[0].get::<_, String>(2), "active");
    let current_query = rows[0].get::<_, Option<String>>(3);
    assert!(
        current_query
            .as_deref()
            .is_some_and(|query| query.contains("pg_stat_activity")),
        "current activity query should be visible"
    );
    assert!(rows[0].get::<_, bool>(4));
    assert!(rows[0].get::<_, bool>(5));
    assert!(rows[0].get::<_, bool>(6));
    assert!(rows[0].get::<_, bool>(7));
    assert_eq!(rows[0].get::<_, Option<String>>(8), None);
    assert_eq!(rows[0].get::<_, Option<String>>(9), None);
    assert_eq!(rows[1].get::<_, String>(0), "activity_b");
    assert_eq!(rows[1].get::<_, String>(1), "activity_b");
    assert_eq!(rows[1].get::<_, String>(2), "idle");
    assert_eq!(rows[1].get::<_, Option<String>>(3), None);
    assert!(rows[1].get::<_, bool>(4));
    assert!(!rows[1].get::<_, bool>(5));
    assert!(!rows[1].get::<_, bool>(6));
    assert!(rows[1].get::<_, bool>(7));
    assert_eq!(
        rows[1].get::<_, Option<String>>(8).as_deref(),
        Some("Client")
    );
    assert_eq!(
        rows[1].get::<_, Option<String>>(9).as_deref(),
        Some("ClientRead")
    );

    let row = client_a
        .query_one(
            "SELECT xact_start IS NOT NULL \
             FROM pg_catalog.pg_stat_activity \
             WHERE application_name = 'activity_b'",
            &[],
        )
        .await
        .expect("idle activity_b transaction start");
    assert!(!row.get::<_, bool>(0));

    client_b
        .batch_execute("BEGIN")
        .await
        .expect("begin activity_b transaction");
    let row = client_a
        .query_one(
            "SELECT state, xact_start IS NOT NULL \
             FROM pg_catalog.pg_stat_activity \
             WHERE application_name = 'activity_b'",
            &[],
        )
        .await
        .expect("active activity_b transaction start");
    assert_eq!(row.get::<_, String>(0), "idle in transaction");
    assert!(row.get::<_, bool>(1));

    client_b
        .batch_execute("COMMIT")
        .await
        .expect("commit activity_b transaction");
    let row = client_a
        .query_one(
            "SELECT state, xact_start IS NOT NULL \
             FROM pg_catalog.pg_stat_activity \
             WHERE application_name = 'activity_b'",
            &[],
        )
        .await
        .expect("committed activity_b transaction start");
    assert_eq!(row.get::<_, String>(0), "idle");
    assert!(!row.get::<_, bool>(1));

    client_b
        .batch_execute("BEGIN")
        .await
        .expect("begin rollback activity_b transaction");
    let row = client_a
        .query_one(
            "SELECT state, xact_start IS NOT NULL \
             FROM pg_catalog.pg_stat_activity \
             WHERE application_name = 'activity_b'",
            &[],
        )
        .await
        .expect("rollback activity_b transaction start");
    assert_eq!(row.get::<_, String>(0), "idle in transaction");
    assert!(row.get::<_, bool>(1));

    client_b
        .batch_execute("ROLLBACK")
        .await
        .expect("rollback activity_b transaction");
    let row = client_a
        .query_one(
            "SELECT state, xact_start IS NOT NULL \
             FROM pg_catalog.pg_stat_activity \
             WHERE application_name = 'activity_b'",
            &[],
        )
        .await
        .expect("rolled back activity_b transaction start");
    assert_eq!(row.get::<_, String>(0), "idle");
    assert!(!row.get::<_, bool>(1));

    drop(client_b);
    tokio::time::sleep(Duration::from_millis(20)).await;
    let rows = client_a
        .query(
            "SELECT application_name \
             FROM pg_catalog.pg_stat_activity \
             WHERE application_name IN ('activity_a', 'activity_b') \
             ORDER BY application_name",
            &[],
        )
        .await
        .expect("pg_stat_activity after close");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "activity_a");

    drop(client_a);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
    conn_handle_a.abort();
    conn_handle_b.abort();
}
