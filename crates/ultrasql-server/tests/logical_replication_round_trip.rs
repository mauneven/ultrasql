//! End-to-end logical replication / CDC tests.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::oneshot;
use tokio_postgres::NoTls;
use ultrasql_server::replication::LogicalChangeKind;
use ultrasql_server::{Server, bind_listener, serve_listener_with_shutdown};

async fn start_server_and_connect() -> (
    Arc<Server>,
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
    oneshot::Sender<()>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_handle = tokio::spawn(serve_listener_with_shutdown(
        listener,
        Arc::clone(&server),
        async move {
            let _ = shutdown_rx.await;
        },
    ));
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=logical_replication_test",
        host = bound.ip(),
        port = bound.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("tokio-postgres connect");
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    (server, client, conn_handle, server_handle, shutdown_tx)
}

async fn shutdown(
    client: tokio_postgres::Client,
    conn_handle: tokio::task::JoinHandle<()>,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
    shutdown_tx: oneshot::Sender<()>,
) {
    drop(client);
    let _ = shutdown_tx.send(());
    conn_handle.await.expect("connection task joins");
    server_handle
        .await
        .expect("server task joins")
        .expect("server shuts down cleanly");
}

#[tokio::test]
async fn create_publication_records_committed_dml_stream() {
    let (server, client, conn_handle, server_handle, shutdown_tx) =
        start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE events (id INT NOT NULL, value INT NOT NULL)")
        .await
        .expect("create events table");
    client
        .batch_execute("CREATE TABLE private_events (id INT NOT NULL)")
        .await
        .expect("create private_events table");
    client
        .batch_execute("CREATE PUBLICATION pub_events FOR TABLE events")
        .await
        .expect("create publication");

    let publication = server
        .logical_replication
        .publication("pub_events")
        .expect("publication registered");
    assert!(publication.publishes_table("events"));
    assert!(!publication.publishes_table("private_events"));
    let publication_rows = client
        .query(
            "SELECT pubname, pubinsert, pubupdate, pubdelete \
             FROM pg_catalog.pg_publication \
             WHERE pubname = 'pub_events'",
            &[],
        )
        .await
        .expect("pg_publication row");
    assert_eq!(publication_rows.len(), 1);
    assert_eq!(publication_rows[0].get::<_, String>(0), "pub_events");
    assert!(publication_rows[0].get::<_, bool>(1));
    assert!(publication_rows[0].get::<_, bool>(2));
    assert!(publication_rows[0].get::<_, bool>(3));
    let publication_table_rows = client
        .query(
            "SELECT schemaname, tablename \
             FROM pg_catalog.pg_publication_tables \
             WHERE pubname = 'pub_events'",
            &[],
        )
        .await
        .expect("pg_publication_tables row");
    assert_eq!(publication_table_rows.len(), 1);
    assert_eq!(publication_table_rows[0].get::<_, String>(0), "public");
    assert_eq!(publication_table_rows[0].get::<_, String>(1), "events");

    client
        .batch_execute("INSERT INTO events VALUES (1, 10), (2, 20)")
        .await
        .expect("insert published rows");
    client
        .batch_execute("UPDATE events SET value = 30 WHERE id = 1")
        .await
        .expect("update published row");
    client
        .batch_execute("DELETE FROM events WHERE id = 2")
        .await
        .expect("delete published row");
    client
        .batch_execute("INSERT INTO private_events VALUES (9)")
        .await
        .expect("insert unpublished row");

    let changes = server.logical_replication.changes_since(0);
    assert_eq!(changes.len(), 3);
    assert_eq!(changes[0].publication, "pub_events");
    assert_eq!(changes[0].table, "events");
    assert_eq!(changes[0].kind, LogicalChangeKind::Insert);
    assert_eq!(changes[0].rows_affected, 2);
    assert_eq!(changes[1].kind, LogicalChangeKind::Update);
    assert_eq!(changes[1].rows_affected, 1);
    assert_eq!(changes[2].kind, LogicalChangeKind::Delete);
    assert_eq!(changes[2].rows_affected, 1);
    assert!(changes.windows(2).all(|pair| pair[0].lsn < pair[1].lsn));

    shutdown(client, conn_handle, server_handle, shutdown_tx).await;
}

#[tokio::test]
async fn rollback_does_not_emit_logical_changes() {
    let (server, client, conn_handle, server_handle, shutdown_tx) =
        start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE events (id INT NOT NULL)")
        .await
        .expect("create events table");
    client
        .batch_execute("CREATE PUBLICATION pub_events FOR TABLE events")
        .await
        .expect("create publication");

    client
        .batch_execute("BEGIN")
        .await
        .expect("begin transaction");
    client
        .batch_execute("INSERT INTO events VALUES (1)")
        .await
        .expect("insert inside transaction");
    client
        .batch_execute("ROLLBACK")
        .await
        .expect("rolled-back insert");

    assert!(server.logical_replication.changes_since(0).is_empty());

    shutdown(client, conn_handle, server_handle, shutdown_tx).await;
}

#[tokio::test]
async fn create_subscription_populates_catalog_and_stat_views() {
    let (_server, client, conn_handle, server_handle, shutdown_tx) =
        start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE events (id INT NOT NULL)")
        .await
        .expect("create events table");
    client
        .batch_execute("CREATE PUBLICATION pub_events FOR TABLE events")
        .await
        .expect("create publication");
    client
        .batch_execute(
            "CREATE SUBSCRIPTION sub_events \
             CONNECTION 'host=127.0.0.1 port=5433' \
             PUBLICATION pub_events \
             WITH (slot_name = 'sub_events_slot')",
        )
        .await
        .expect("create subscription");

    let subscription_rows = client
        .query(
            "SELECT subname, subenabled, subslotname, subpublications \
             FROM pg_catalog.pg_subscription \
             WHERE subname = 'sub_events'",
            &[],
        )
        .await
        .expect("pg_subscription rows");
    assert_eq!(subscription_rows.len(), 1);
    assert_eq!(subscription_rows[0].get::<_, String>(0), "sub_events");
    assert!(subscription_rows[0].get::<_, bool>(1));
    assert_eq!(subscription_rows[0].get::<_, String>(2), "sub_events_slot");
    assert_eq!(subscription_rows[0].get::<_, String>(3), "pub_events");

    let stat_rows = client
        .query(
            "SELECT subname, pid, relid \
             FROM pg_catalog.pg_stat_subscription \
             WHERE subname = 'sub_events'",
            &[],
        )
        .await
        .expect("pg_stat_subscription rows");
    assert_eq!(stat_rows.len(), 1);
    assert_eq!(stat_rows[0].get::<_, String>(0), "sub_events");
    assert_eq!(stat_rows[0].get::<_, i32>(1), 0);
    assert_eq!(stat_rows[0].get::<_, i64>(2), 0);

    shutdown(client, conn_handle, server_handle, shutdown_tx).await;
}
