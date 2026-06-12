//! End-to-end append-only materialized view tests.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::oneshot;
use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener_with_shutdown};

pub mod support;

use support::{shutdown as shutdown_persistent, start_persistent_server};

struct RunningServer {
    client: tokio_postgres::Client,
    conn_handle: tokio::task::JoinHandle<()>,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
    shutdown_tx: oneshot::Sender<()>,
}

async fn start_server_and_connect() -> RunningServer {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_handle = tokio::spawn(serve_listener_with_shutdown(listener, server, async move {
        let _ = shutdown_rx.await;
    }));
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=materialized_view_test",
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
    RunningServer {
        client,
        conn_handle,
        server_handle,
        shutdown_tx,
    }
}

async fn shutdown(running: RunningServer) {
    drop(running.client);
    running.conn_handle.await.expect("connection task joins");
    let _ = running.shutdown_tx.send(());
    running
        .server_handle
        .await
        .expect("server task joins")
        .expect("listener exits cleanly");
}

#[tokio::test]
async fn materialized_view_snapshots_then_appends_from_source_inserts() {
    let running = start_server_and_connect().await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE mv_src (id INT NOT NULL, amount INT NOT NULL)")
        .await
        .expect("create source");
    client
        .batch_execute("INSERT INTO mv_src VALUES (1, 10), (2, 20)")
        .await
        .expect("seed source");

    client
        .batch_execute("CREATE MATERIALIZED VIEW mv_copy AS SELECT id, amount FROM mv_src")
        .await
        .expect("create materialized view");

    let catalog_row = client
        .query_one(
            "SELECT c.relkind, m.matviewowner, m.ispopulated \
             FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_matviews m ON m.matviewname = c.relname \
             WHERE c.relname = 'mv_copy'",
            &[],
        )
        .await
        .expect("materialized view catalog row");
    assert_eq!(catalog_row.get::<_, String>(0), "m");
    assert_eq!(catalog_row.get::<_, String>(1), "ultrasql");
    assert!(catalog_row.get::<_, bool>(2));

    let table_rows = client
        .query_one(
            "SELECT COUNT(*) FROM pg_catalog.pg_tables WHERE tablename = 'mv_copy'",
            &[],
        )
        .await
        .expect("materialized view excluded from pg_tables");
    assert_eq!(table_rows.get::<_, i64>(0), 0);

    let initial = client
        .query("SELECT id, amount FROM mv_copy ORDER BY id", &[])
        .await
        .expect("select initial materialized rows");
    assert_eq!(initial.len(), 2);
    assert_eq!(initial[0].get::<_, i32>(0), 1);
    assert_eq!(initial[0].get::<_, i32>(1), 10);
    assert_eq!(initial[1].get::<_, i32>(0), 2);
    assert_eq!(initial[1].get::<_, i32>(1), 20);

    client
        .batch_execute("INSERT INTO mv_src VALUES (3, 30)")
        .await
        .expect("append source");

    let after_append = client
        .query("SELECT id, amount FROM mv_copy ORDER BY id", &[])
        .await
        .expect("select appended materialized rows");
    assert_eq!(after_append.len(), 3);
    assert_eq!(after_append[2].get::<_, i32>(0), 3);
    assert_eq!(after_append[2].get::<_, i32>(1), 30);

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("INSERT INTO mv_src VALUES (4, 40)")
        .await
        .expect("append source in transaction");
    client.batch_execute("COMMIT").await.expect("commit");

    let after_commit = client
        .query("SELECT id, amount FROM mv_copy ORDER BY id", &[])
        .await
        .expect("select committed materialized rows");
    assert_eq!(after_commit.len(), 4);
    assert_eq!(after_commit[3].get::<_, i32>(0), 4);
    assert_eq!(after_commit[3].get::<_, i32>(1), 40);

    let update_err = client
        .batch_execute("UPDATE mv_src SET amount = 99 WHERE id = 1")
        .await
        .expect_err("updates to append-only source must be rejected");
    let db_err = update_err
        .as_db_error()
        .expect("server-sent ErrorResponse for update rejection");
    assert!(
        db_err
            .message()
            .contains("append-only materialized view source"),
        "expected append-only materialized view error, got {:?}",
        db_err.message()
    );

    shutdown(running).await;
}

#[tokio::test]
async fn same_materialized_view_name_is_isolated_by_schema() {
    let running = start_server_and_connect().await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE TABLE same_mv_src (id INT NOT NULL, amount INT NOT NULL); \
             CREATE TABLE app.same_mv_src (id INT NOT NULL, amount INT NOT NULL); \
             INSERT INTO same_mv_src VALUES (1, 10); \
             INSERT INTO app.same_mv_src VALUES (10, 100); \
             CREATE MATERIALIZED VIEW same_mv_copy AS \
                 SELECT id, amount FROM same_mv_src; \
             CREATE MATERIALIZED VIEW app.same_mv_copy AS \
                 SELECT id, amount FROM app.same_mv_src",
        )
        .await
        .expect("create same materialized view names in different schemas");

    client
        .batch_execute(
            "INSERT INTO same_mv_src VALUES (2, 20); \
             INSERT INTO app.same_mv_src VALUES (20, 200)",
        )
        .await
        .expect("append both schema sources");

    let public_rows = client
        .query("SELECT id, amount FROM same_mv_copy ORDER BY id", &[])
        .await
        .expect("public materialized view is maintained");
    assert_eq!(public_rows.len(), 2);
    assert_eq!(public_rows[1].get::<_, i32>(0), 2);
    assert_eq!(public_rows[1].get::<_, i32>(1), 20);

    let app_rows = client
        .query("SELECT id, amount FROM app.same_mv_copy ORDER BY id", &[])
        .await
        .expect("app materialized view is maintained");
    assert_eq!(app_rows.len(), 2);
    assert_eq!(app_rows[1].get::<_, i32>(0), 20);
    assert_eq!(app_rows[1].get::<_, i32>(1), 200);

    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn appended_materialized_view_rows_survive_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "materialized_view_restart_test").await;
    running
        .client
        .batch_execute("CREATE TABLE mv_restart_src (id INT NOT NULL, amount INT NOT NULL)")
        .await
        .expect("create source");
    running
        .client
        .batch_execute("INSERT INTO mv_restart_src VALUES (1, 10), (2, 20)")
        .await
        .expect("seed source");
    running
        .client
        .batch_execute(
            "CREATE MATERIALIZED VIEW mv_restart_copy AS SELECT id, amount FROM mv_restart_src",
        )
        .await
        .expect("create materialized view");
    running
        .client
        .batch_execute("INSERT INTO mv_restart_src VALUES (3, 30)")
        .await
        .expect("append source");
    shutdown_persistent(running).await;

    let running = start_persistent_server(data_dir.path(), "materialized_view_restart_test").await;
    let catalog_row = running
        .client
        .query_one(
            "SELECT c.relkind, m.ispopulated \
             FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_matviews m ON m.matviewname = c.relname \
             WHERE c.relname = 'mv_restart_copy'",
            &[],
        )
        .await
        .expect("materialized view catalog row after restart");
    assert_eq!(catalog_row.get::<_, String>(0), "m");
    assert!(catalog_row.get::<_, bool>(1));
    let rows = running
        .client
        .query("SELECT id, amount FROM mv_restart_copy ORDER BY id", &[])
        .await
        .expect("select materialized view after restart");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[2].get::<_, i32>(0), 3);
    assert_eq!(rows[2].get::<_, i32>(1), 30);
    shutdown_persistent(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_materialized_view_unmaintainable_shape_is_rejected_before_catalog_write() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "materialized_view_shape").await;
    running
        .client
        .batch_execute("CREATE TABLE mv_shape_src (id INT NOT NULL)")
        .await
        .expect("create source");
    let err = running
        .client
        .batch_execute(
            "CREATE MATERIALIZED VIEW mv_shape_bad AS SELECT id + 1 AS id FROM mv_shape_src",
        )
        .await
        .expect_err("persistent materialized view must reject unmaintainable source shape");
    let msg = err
        .as_db_error()
        .expect("server-sent DB error")
        .message()
        .to_owned();
    assert!(
        msg.contains("materialized view") && msg.contains("restart-persistable metadata subset"),
        "unexpected error: {err}"
    );
    let err = running
        .client
        .query("SELECT id FROM mv_shape_bad", &[])
        .await
        .expect_err("failed materialized view must not exist in current session");
    assert_eq!(err.code().expect("SQLSTATE").code(), "42P01");
    shutdown_persistent(running).await;

    let running = start_persistent_server(data_dir.path(), "materialized_view_shape").await;
    let err = running
        .client
        .query("SELECT id FROM mv_shape_bad", &[])
        .await
        .expect_err("failed materialized view must not exist after restart");
    assert_eq!(err.code().expect("SQLSTATE").code(), "42P01");
    shutdown_persistent(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn materialized_view_metadata_escapes_quoted_names_on_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();
    support::make_data_dir_private(data_dir.path());
    let metadata_path = data_dir.path().join("pg_materialized_views.meta");

    let running = start_persistent_server(data_dir.path(), "materialized_view_quoted_setup").await;
    running
        .client
        .batch_execute("CREATE TABLE \"mv\tsrc\" (id INT NOT NULL, amount INT NOT NULL)")
        .await
        .expect("create quoted source");
    running
        .client
        .batch_execute("INSERT INTO \"mv\tsrc\" VALUES (1, 10)")
        .await
        .expect("seed quoted source");
    running
        .client
        .batch_execute("CREATE MATERIALIZED VIEW \"mv\tcopy\" AS SELECT id FROM \"mv\tsrc\"")
        .await
        .expect("create quoted materialized view");
    shutdown_persistent(running).await;

    let metadata = std::fs::read_to_string(&metadata_path).expect("metadata exists");
    assert!(
        metadata.contains(r"mv\tcopy") && metadata.contains(r"mv\tsrc"),
        "materialized-view metadata must escape quoted table names: {metadata:?}"
    );

    let running = start_persistent_server(data_dir.path(), "materialized_view_quoted_verify").await;
    let before: i64 = running
        .client
        .query_one("SELECT COUNT(*) FROM \"mv\tcopy\"", &[])
        .await
        .expect("query quoted materialized view after restart")
        .get(0);
    assert_eq!(before, 1);

    running
        .client
        .batch_execute("INSERT INTO \"mv\tsrc\" VALUES (2, 20)")
        .await
        .expect("append quoted source");
    let after: i64 = running
        .client
        .query_one("SELECT COUNT(*) FROM \"mv\tcopy\"", &[])
        .await
        .expect("query maintained quoted materialized view")
        .get(0);
    assert_eq!(after, 2);
    shutdown_persistent(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn materialized_view_keeps_maintaining_source_after_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "materialized_view_restart_test").await;
    running
        .client
        .batch_execute("CREATE TABLE mv_runtime_src (id INT NOT NULL, amount INT NOT NULL)")
        .await
        .expect("create source");
    running
        .client
        .batch_execute("INSERT INTO mv_runtime_src VALUES (1, 10), (2, 20)")
        .await
        .expect("seed source");
    running
        .client
        .batch_execute(
            "CREATE MATERIALIZED VIEW mv_runtime_copy AS SELECT id, amount FROM mv_runtime_src",
        )
        .await
        .expect("create materialized view");
    shutdown_persistent(running).await;

    let running = start_persistent_server(data_dir.path(), "materialized_view_restart_test").await;
    running
        .client
        .batch_execute("INSERT INTO mv_runtime_src VALUES (3, 30)")
        .await
        .expect("append after restart");
    let rows = running
        .client
        .query("SELECT id, amount FROM mv_runtime_copy ORDER BY id", &[])
        .await
        .expect("select materialized view after restarted append");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[2].get::<_, i32>(0), 3);
    assert_eq!(rows[2].get::<_, i32>(1), 30);
    shutdown_persistent(running).await;
}

#[tokio::test]
async fn drop_source_restricts_and_cascade_drops_materialized_view_dependency() {
    let running = start_server_and_connect().await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE mv_drop_src (id INT NOT NULL, amount INT NOT NULL)")
        .await
        .expect("create source");
    client
        .batch_execute("INSERT INTO mv_drop_src VALUES (1, 10)")
        .await
        .expect("seed source");
    client
        .batch_execute(
            "CREATE MATERIALIZED VIEW mv_drop_copy AS SELECT id, amount FROM mv_drop_src",
        )
        .await
        .expect("create materialized view");

    let restricted = client
        .batch_execute("DROP TABLE mv_drop_src")
        .await
        .expect_err("source drop must be restricted by materialized view");
    assert_eq!(restricted.code().expect("SQLSTATE").code(), "2BP01");

    client
        .batch_execute("DROP TABLE mv_drop_src CASCADE")
        .await
        .expect("cascade drops materialized view dependency");

    let source_err = client
        .batch_execute("SELECT * FROM mv_drop_src")
        .await
        .expect_err("source table dropped");
    assert_eq!(source_err.code().expect("SQLSTATE").code(), "42P01");
    let view_err = client
        .batch_execute("SELECT * FROM mv_drop_copy")
        .await
        .expect_err("dependent materialized view dropped");
    assert_eq!(view_err.code().expect("SQLSTATE").code(), "42P01");

    shutdown(running).await;
}

#[tokio::test]
async fn drop_materialized_view_clears_runtime_dependency() {
    let running = start_server_and_connect().await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE mv_direct_drop_src (id INT NOT NULL, amount INT NOT NULL)")
        .await
        .expect("create source");
    client
        .batch_execute("INSERT INTO mv_direct_drop_src VALUES (1, 10)")
        .await
        .expect("seed source");
    client
        .batch_execute(
            "CREATE MATERIALIZED VIEW mv_direct_drop_copy AS \
             SELECT id, amount FROM mv_direct_drop_src",
        )
        .await
        .expect("create materialized view");

    client
        .batch_execute("DROP TABLE mv_direct_drop_copy")
        .await
        .expect("drop materialized view");
    client
        .batch_execute("INSERT INTO mv_direct_drop_src VALUES (2, 20)")
        .await
        .expect("source insert after view drop must not maintain stale runtime");

    let rows = client
        .query("SELECT id, amount FROM mv_direct_drop_src ORDER BY id", &[])
        .await
        .expect("source remains queryable");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[1].get::<_, i32>(1), 20);

    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_materialized_view_removes_restart_metadata() {
    let data_dir = tempfile::TempDir::new().unwrap();
    support::make_data_dir_private(data_dir.path());
    let metadata_path = data_dir.path().join("pg_materialized_views.meta");

    let running = start_persistent_server(data_dir.path(), "materialized_view_drop_meta").await;
    running
        .client
        .batch_execute("CREATE TABLE mv_meta_src (id INT NOT NULL, amount INT NOT NULL)")
        .await
        .expect("create source");
    running
        .client
        .batch_execute("INSERT INTO mv_meta_src VALUES (1, 10)")
        .await
        .expect("seed source");
    running
        .client
        .batch_execute(
            "CREATE MATERIALIZED VIEW mv_meta_copy AS SELECT id, amount FROM mv_meta_src",
        )
        .await
        .expect("create materialized view");
    let metadata = std::fs::read_to_string(&metadata_path).expect("metadata exists");
    assert!(
        metadata.contains("mv_meta_copy"),
        "materialized-view metadata should record view before drop: {metadata}"
    );

    running
        .client
        .batch_execute("DROP TABLE mv_meta_copy")
        .await
        .expect("drop materialized view");
    shutdown_persistent(running).await;

    let metadata = std::fs::read_to_string(&metadata_path).expect("metadata still exists");
    assert!(
        !metadata.contains("mv_meta_copy"),
        "dropped materialized view must be removed from metadata: {metadata}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn materialized_view_metadata_rejects_duplicate_views_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    support::make_data_dir_private(data_dir.path());
    let metadata_path = data_dir.path().join("pg_materialized_views.meta");

    let running =
        start_persistent_server(data_dir.path(), "materialized_view_duplicate_meta").await;
    running
        .client
        .batch_execute("CREATE TABLE mv_duplicate_src (id INT NOT NULL, amount INT NOT NULL)")
        .await
        .expect("create source");
    running
        .client
        .batch_execute("INSERT INTO mv_duplicate_src VALUES (1, 10)")
        .await
        .expect("seed source");
    running
        .client
        .batch_execute(
            "CREATE MATERIALIZED VIEW mv_duplicate_copy AS SELECT id, amount FROM mv_duplicate_src",
        )
        .await
        .expect("create materialized view");
    shutdown_persistent(running).await;

    let mut metadata = std::fs::read_to_string(&metadata_path).expect("metadata exists");
    let view_line = metadata
        .lines()
        .find(|line| !line.is_empty() && !line.starts_with('#'))
        .expect("materialized-view metadata row")
        .to_owned();
    metadata.push_str(&view_line);
    metadata.push('\n');
    std::fs::write(&metadata_path, metadata).expect("duplicate materialized-view metadata");

    let err =
        Server::init(data_dir.path()).expect_err("duplicate materialized-view metadata rejected");
    assert!(
        err.to_string()
            .contains("duplicate materialized-view metadata"),
        "expected duplicate materialized-view metadata rejection, got {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn materialized_view_metadata_rejects_mismatched_source_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    support::make_data_dir_private(data_dir.path());
    let metadata_path = data_dir.path().join("pg_materialized_views.meta");

    let running =
        start_persistent_server(data_dir.path(), "materialized_view_mismatched_source").await;
    running
        .client
        .batch_execute("CREATE TABLE mv_mismatch_src (id INT NOT NULL, amount INT NOT NULL)")
        .await
        .expect("create source");
    running
        .client
        .batch_execute("INSERT INTO mv_mismatch_src VALUES (1, 10)")
        .await
        .expect("seed source");
    running
        .client
        .batch_execute(
            "CREATE MATERIALIZED VIEW mv_mismatch_copy AS SELECT id, amount FROM mv_mismatch_src",
        )
        .await
        .expect("create materialized view");
    shutdown_persistent(running).await;

    let mut metadata = std::fs::read_to_string(&metadata_path).expect("metadata exists");
    let old_line = metadata
        .lines()
        .find(|line| !line.is_empty() && !line.starts_with('#'))
        .expect("materialized-view metadata row")
        .to_owned();
    let mut parts = old_line.split('\t').collect::<Vec<_>>();
    parts[3] = "424242";
    let new_line = parts.join("\t");
    metadata = metadata.replace(&old_line, &new_line);
    std::fs::write(&metadata_path, metadata).expect("mismatched materialized-view metadata");

    let err =
        Server::init(data_dir.path()).expect_err("mismatched materialized-view metadata rejected");
    assert!(
        err.to_string()
            .contains("invalid materialized-view metadata"),
        "expected invalid materialized-view metadata rejection, got {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn materialized_view_metadata_rejects_projection_type_mismatch_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    support::make_data_dir_private(data_dir.path());
    let metadata_path = data_dir.path().join("pg_materialized_views.meta");

    let running =
        start_persistent_server(data_dir.path(), "materialized_view_projection_mismatch").await;
    running
        .client
        .batch_execute("CREATE TABLE mv_projection_src (id INT NOT NULL, label TEXT NOT NULL)")
        .await
        .expect("create source");
    running
        .client
        .batch_execute("INSERT INTO mv_projection_src VALUES (1, 'one')")
        .await
        .expect("seed source");
    running
        .client
        .batch_execute(
            "CREATE MATERIALIZED VIEW mv_projection_copy AS SELECT id FROM mv_projection_src",
        )
        .await
        .expect("create materialized view");
    shutdown_persistent(running).await;

    let mut metadata = std::fs::read_to_string(&metadata_path).expect("metadata exists");
    let old_line = metadata
        .lines()
        .find(|line| !line.is_empty() && !line.starts_with('#'))
        .expect("materialized-view metadata row")
        .to_owned();
    let mut parts = old_line.split('\t').collect::<Vec<_>>();
    parts[5] = "1";
    let new_line = parts.join("\t");
    metadata = metadata.replace(&old_line, &new_line);
    std::fs::write(&metadata_path, metadata).expect("mismatched projection metadata");

    let err =
        Server::init(data_dir.path()).expect_err("projection type mismatch metadata rejected");
    assert!(
        err.to_string()
            .contains("invalid materialized-view metadata"),
        "expected invalid materialized-view metadata rejection, got {err}"
    );
}
