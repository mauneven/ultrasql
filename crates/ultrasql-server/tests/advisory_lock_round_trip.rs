//! PostgreSQL advisory-lock SQL function round trips.

use tokio_postgres::NoTls;

pub mod support;

use support::{shutdown, start_sample_server};

#[tokio::test]
async fn try_advisory_lock_conflicts_across_sessions_and_unlocks() {
    let running = start_sample_server("advisory_lock_test").await;
    let a = &running.client;
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=advisory_lock_test",
        host = running.bound.ip(),
        port = running.bound.port()
    );
    let (b, b_conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect b");
    let b_handle = tokio::spawn(async move {
        let _ = b_conn.await;
    });

    let row = a
        .query_one("SELECT pg_try_advisory_lock(9001)", &[])
        .await
        .expect("session a try lock");
    assert!(row.get::<_, bool>(0));
    let locks = b
        .query(
            "SELECT locktype, classid, objid, mode, granted \
             FROM pg_catalog.pg_locks \
             WHERE locktype = 'advisory' \
             ORDER BY classid, objid",
            &[],
        )
        .await
        .expect("pg_locks advisory rows");
    assert_eq!(locks.len(), 1);
    assert_eq!(locks[0].get::<_, String>(0), "advisory");
    assert_eq!(locks[0].get::<_, i64>(1), 0);
    assert_eq!(locks[0].get::<_, i64>(2), 9001);
    assert_eq!(locks[0].get::<_, String>(3), "ExclusiveLock");
    assert!(locks[0].get::<_, bool>(4));

    let row = b
        .query_one("SELECT pg_try_advisory_lock(9001)", &[])
        .await
        .expect("session b conflicting try lock");
    assert!(!row.get::<_, bool>(0));

    let row = a
        .query_one("SELECT pg_advisory_unlock(9001)", &[])
        .await
        .expect("session a unlock");
    assert!(row.get::<_, bool>(0));

    let row = b
        .query_one("SELECT pg_try_advisory_lock(9001)", &[])
        .await
        .expect("session b try lock after unlock");
    assert!(row.get::<_, bool>(0));

    let row = b
        .query_one("SELECT pg_advisory_unlock(9001)", &[])
        .await
        .expect("session b unlock");
    assert!(row.get::<_, bool>(0));

    drop(b);
    b_handle.abort();
    shutdown(running).await;
}

#[tokio::test]
async fn blocking_advisory_lock_simple_query_uses_same_lock_table() {
    let running = start_sample_server("advisory_lock_test").await;
    let a = &running.client;
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=advisory_lock_test",
        host = running.bound.ip(),
        port = running.bound.port()
    );
    let (b, b_conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect b");
    let b_handle = tokio::spawn(async move {
        let _ = b_conn.await;
    });

    a.simple_query("SELECT pg_advisory_lock(9002)")
        .await
        .expect("session a blocking lock");

    let row = b
        .query_one("SELECT pg_try_advisory_lock(9002)", &[])
        .await
        .expect("session b conflicting try lock");
    assert!(!row.get::<_, bool>(0));

    let row = a
        .query_one("SELECT pg_advisory_unlock(9002)", &[])
        .await
        .expect("session a unlock");
    assert!(row.get::<_, bool>(0));

    let row = b
        .query_one("SELECT pg_try_advisory_lock(9002)", &[])
        .await
        .expect("session b try lock after unlock");
    assert!(row.get::<_, bool>(0));

    drop(b);
    b_handle.abort();
    shutdown(running).await;
}

#[tokio::test]
async fn transaction_advisory_try_lock_releases_on_commit() {
    let running = start_sample_server("advisory_xact_lock_test").await;
    let a = &running.client;
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=advisory_xact_lock_test",
        host = running.bound.ip(),
        port = running.bound.port()
    );
    let (b, b_conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect b");
    let b_handle = tokio::spawn(async move {
        let _ = b_conn.await;
    });

    a.batch_execute("BEGIN").await.expect("begin transaction");
    let row = a
        .query_one("SELECT pg_try_advisory_xact_lock(9003)", &[])
        .await
        .expect("session a transaction try lock");
    assert_eq!(row.columns()[0].name(), "pg_try_advisory_xact_lock");
    assert!(row.get::<_, bool>(0));

    let row = b
        .query_one("SELECT pg_try_advisory_lock(9003)", &[])
        .await
        .expect("session b conflicting session try lock");
    assert!(!row.get::<_, bool>(0));

    a.batch_execute("COMMIT").await.expect("commit transaction");
    let row = b
        .query_one("SELECT pg_try_advisory_lock(9003)", &[])
        .await
        .expect("session b try lock after transaction commit");
    assert!(row.get::<_, bool>(0));

    drop(b);
    b_handle.abort();
    shutdown(running).await;
}
