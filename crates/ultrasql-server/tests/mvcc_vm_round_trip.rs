//! Server-owned visibility-map regression.
//!
//! This verifies the production path, not only the storage primitive:
//! autocommit DML clears VM bits, server maintenance certifies pages, and
//! `SeqScan` can read through the VM-aware heap walker without changing SQL
//! results.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::NoTls;
use ultrasql_core::{BlockNumber, RelationId};
use ultrasql_server::{Server, UNDO_GC_INTERVAL_COMMITS, bind_listener, serve_listener};

async fn start_server_and_connect() -> (
    Arc<Server>,
    tokio_postgres::Client,
    String,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());
    let server_handle = tokio::spawn(serve_listener(listener, Arc::clone(&server)));
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=mvcc_vm_round_trip",
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
    (server, client, conn_str, conn_handle, server_handle)
}

async fn shutdown(
    client: tokio_postgres::Client,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    drop(client);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

fn relation_id(server: &Server, table: &str) -> RelationId {
    let snapshot = server.catalog_snapshot();
    let entry = snapshot
        .tables
        .get(table)
        .unwrap_or_else(|| panic!("{table} exists"));
    RelationId(entry.oid)
}

fn force_maintenance(server: &Server) {
    for _ in 0..UNDO_GC_INTERVAL_COMMITS {
        server.note_commit_for_gc();
    }
}

#[tokio::test]
async fn server_vm_certifies_scan_and_mutation_clears() {
    let (server, client, _conn_str, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE vm_t (id INT NOT NULL, val INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO vm_t VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("insert rows");

    let rel = relation_id(&server, "vm_t");
    force_maintenance(&server);
    assert!(server.vm.is_all_visible(rel, BlockNumber::new(0)));

    let rows = client
        .query("SELECT SUM(val) FROM vm_t", &[])
        .await
        .expect("vm-aware seqscan still returns rows");
    let sum: i64 = rows[0].get(0);
    assert_eq!(sum, 60);

    client
        .batch_execute("UPDATE vm_t SET val = val + 1 WHERE id = 2")
        .await
        .expect("update clears vm");
    assert!(!server.vm.is_all_visible(rel, BlockNumber::new(0)));

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn maintenance_preserves_versions_for_live_repeatable_read_snapshot() {
    let (server, writer, conn_str, _writer_conn, server_handle) = start_server_and_connect().await;
    let (reader, reader_connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("reader connect");
    let _reader_conn = tokio::spawn(async move {
        if let Err(e) = reader_connection.await {
            eprintln!("reader connection error: {e}");
        }
    });

    writer
        .batch_execute(
            "CREATE TABLE snapshot_t (id INT NOT NULL, val INT NOT NULL); \
             INSERT INTO snapshot_t VALUES (1, 10), (2, 20)",
        )
        .await
        .expect("create and seed snapshot table");

    // The writer deliberately gets the lower XID and remains in progress when
    // the reader captures its repeatable-read snapshot.
    writer.batch_execute("BEGIN").await.expect("begin writer");
    writer
        .batch_execute(
            "UPDATE snapshot_t SET val = 11 WHERE id = 1; \
             DELETE FROM snapshot_t WHERE id = 2",
        )
        .await
        .expect("mutate rows before reader snapshot");
    reader
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("begin repeatable-read reader");

    let before_commit = reader
        .query("SELECT id, val FROM snapshot_t ORDER BY id", &[])
        .await
        .expect("reader sees pre-update versions");
    assert_eq!(
        before_commit
            .iter()
            .map(|row| (row.get::<_, i32>(0), row.get::<_, i32>(1)))
            .collect::<Vec<_>>(),
        vec![(1, 10), (2, 20)]
    );

    writer.batch_execute("COMMIT").await.expect("commit writer");

    // Exercise both periodic undo/VM maintenance and heap autovacuum while the
    // snapshot that observed the writer as in-progress remains registered.
    force_maintenance(&server);
    server
        .table_modifications
        .insert("snapshot_t".to_owned(), u64::MAX);
    server.run_autovacuum_cycle();

    let after_maintenance = reader
        .query("SELECT id, val FROM snapshot_t ORDER BY id", &[])
        .await
        .expect("maintenance preserves snapshot versions");
    assert_eq!(
        after_maintenance
            .iter()
            .map(|row| (row.get::<_, i32>(0), row.get::<_, i32>(1)))
            .collect::<Vec<_>>(),
        vec![(1, 10), (2, 20)]
    );

    reader
        .batch_execute("COMMIT")
        .await
        .expect("commit repeatable-read reader");
    let current = reader
        .query("SELECT id, val FROM snapshot_t ORDER BY id", &[])
        .await
        .expect("new snapshot sees committed mutations");
    assert_eq!(
        current
            .iter()
            .map(|row| (row.get::<_, i32>(0), row.get::<_, i32>(1)))
            .collect::<Vec<_>>(),
        vec![(1, 11)]
    );

    drop(writer);
    shutdown(reader, server_handle).await;
}
