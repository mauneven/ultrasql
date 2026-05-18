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
    (server, client, conn_handle, server_handle)
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
    let (server, client, _conn, server_handle) = start_server_and_connect().await;

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
