//! End-to-end row-level security tests.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::{NoTls, SimpleQueryMessage};
use ultrasql_server::{Server, bind_listener, serve_listener};

async fn start_server_and_connect() -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());
    let server_handle = tokio::spawn(serve_listener(listener, server));
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=rls_test",
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
    (client, conn_handle, server_handle)
}

async fn shutdown(
    client: tokio_postgres::Client,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    drop(client);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

fn simple_rows(messages: &[SimpleQueryMessage]) -> Vec<Vec<String>> {
    messages
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|idx| row.get(idx).unwrap_or("").to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn rls_tenant_policy_filters_reads_and_checks_inserts() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE tenant_docs (\
                tenant_id TEXT NOT NULL, \
                doc_id TEXT NOT NULL, \
                body TEXT\
             )",
        )
        .await
        .expect("create tenant table");
    client
        .batch_execute(
            "INSERT INTO tenant_docs VALUES \
                ('tenant-a', 'doc-a', 'alpha'), \
                ('tenant-b', 'doc-b', 'bravo')",
        )
        .await
        .expect("insert seed rows");
    client
        .batch_execute(
            "CREATE POLICY tenant_docs_isolation ON tenant_docs \
                USING (tenant_id = current_setting('ultrasql.tenant_id', true)) \
                WITH CHECK (tenant_id = current_setting('ultrasql.tenant_id', true))",
        )
        .await
        .expect("create tenant rls policy");
    client
        .batch_execute("ALTER TABLE tenant_docs ENABLE ROW LEVEL SECURITY")
        .await
        .expect("enable table rls");
    client
        .batch_execute("SET ultrasql.tenant_id = 'tenant-a'")
        .await
        .expect("set tenant guc");

    let rows = simple_rows(
        &client
            .simple_query("SELECT doc_id FROM tenant_docs ORDER BY doc_id")
            .await
            .expect("select tenant-a rows"),
    );
    assert_eq!(rows, vec![vec!["doc-a".to_owned()]]);

    client
        .batch_execute("INSERT INTO tenant_docs VALUES ('tenant-a', 'doc-a-2', 'alpha-2')")
        .await
        .expect("same-tenant insert passes");
    let err = client
        .batch_execute("INSERT INTO tenant_docs VALUES ('tenant-b', 'doc-b-2', 'bravo-2')")
        .await
        .expect_err("cross-tenant insert must fail RLS WITH CHECK");
    assert!(
        err.as_db_error()
            .is_some_and(|db| db.message().contains("row-level security")),
        "unexpected error: {err}"
    );

    client
        .batch_execute("SET ultrasql.tenant_id = 'tenant-b'")
        .await
        .expect("switch tenant setting");
    let rows = simple_rows(
        &client
            .simple_query("SELECT doc_id FROM tenant_docs ORDER BY doc_id")
            .await
            .expect("select tenant-b rows"),
    );
    assert_eq!(rows, vec![vec!["doc-b".to_owned()]]);

    shutdown(client, server_handle).await;
}
