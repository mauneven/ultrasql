//! End-to-end row-level security tests.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::{NoTls, SimpleQueryMessage};
use ultrasql_server::{Server, bind_listener, serve_listener};

mod support;

use support::{shutdown as graceful_shutdown, start_persistent_server};

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
    let _ = server_handle.await;
}

async fn connect_as(
    bound: SocketAddr,
    user: &str,
    application_name: &str,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let conn_str = format!(
        "host={host} port={port} user={user} application_name={application_name}",
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
    (client, conn_handle)
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
    let policy_rows = client
        .query(
            "SELECT polcmd, polpermissive, polqual, polwithcheck \
             FROM pg_catalog.pg_policy \
             WHERE polname = 'tenant_docs_isolation'",
            &[],
        )
        .await
        .expect("query pg_policy row");
    assert_eq!(policy_rows.len(), 1);
    assert_eq!(policy_rows[0].get::<_, String>(0), "*");
    assert!(policy_rows[0].get::<_, bool>(1));
    let polqual: Option<String> = policy_rows[0].get(2);
    let polwithcheck: Option<String> = policy_rows[0].get(3);
    assert_eq!(
        polqual,
        Some("tenant_id = current_setting('ultrasql.tenant_id', true)".to_owned())
    );
    assert_eq!(
        polwithcheck,
        Some("tenant_id = current_setting('ultrasql.tenant_id', true)".to_owned())
    );
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

#[tokio::test]
async fn rls_owner_and_bypass_roles_skip_policy_filtering() {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());
    let server_handle = tokio::spawn(serve_listener(listener, server));
    let (admin, admin_conn) = connect_as(bound, "tester", "rls_owner_bypass_admin").await;

    for sql in [
        "CREATE ROLE tester SUPERUSER LOGIN",
        "CREATE ROLE app_owner LOGIN",
        "CREATE ROLE tenant_user LOGIN",
        "CREATE ROLE rls_bypass LOGIN BYPASSRLS",
        "SET ROLE app_owner",
        "CREATE TABLE rls_owner_docs (tenant_id TEXT NOT NULL, doc_id TEXT NOT NULL)",
        "RESET ROLE",
        "INSERT INTO rls_owner_docs VALUES ('tenant-a', 'doc-a'), ('tenant-b', 'doc-b')",
        "SET ROLE app_owner",
        "CREATE POLICY rls_owner_docs_tenant ON rls_owner_docs \
            USING (tenant_id = current_setting('ultrasql.tenant_id', true)) \
            WITH CHECK (tenant_id = current_setting('ultrasql.tenant_id', true))",
        "ALTER TABLE rls_owner_docs ENABLE ROW LEVEL SECURITY",
        "RESET ROLE",
        "GRANT SELECT ON TABLE rls_owner_docs TO app_owner, tenant_user, rls_bypass",
    ] {
        admin.batch_execute(sql).await.expect(sql);
    }

    let (tenant, tenant_conn) = connect_as(bound, "tenant_user", "rls_owner_bypass_tenant").await;
    tenant
        .batch_execute("SET ultrasql.tenant_id = 'tenant-a'")
        .await
        .expect("set tenant");
    let rows = simple_rows(
        &tenant
            .simple_query("SELECT doc_id FROM rls_owner_docs ORDER BY doc_id")
            .await
            .expect("tenant sees filtered rows"),
    );
    assert_eq!(rows, vec![vec!["doc-a".to_owned()]]);

    let (owner, owner_conn) = connect_as(bound, "app_owner", "rls_owner_bypass_owner").await;
    let rows = simple_rows(
        &owner
            .simple_query("SELECT doc_id FROM rls_owner_docs ORDER BY doc_id")
            .await
            .expect("owner bypasses RLS"),
    );
    assert_eq!(
        rows,
        vec![vec!["doc-a".to_owned()], vec!["doc-b".to_owned()]]
    );

    let (bypass, bypass_conn) = connect_as(bound, "rls_bypass", "rls_owner_bypass_bypass").await;
    let rows = simple_rows(
        &bypass
            .simple_query("SELECT doc_id FROM rls_owner_docs ORDER BY doc_id")
            .await
            .expect("BYPASSRLS role bypasses RLS"),
    );
    assert_eq!(
        rows,
        vec![vec!["doc-a".to_owned()], vec!["doc-b".to_owned()]]
    );

    drop(tenant);
    drop(owner);
    drop(bypass);
    tenant_conn.await.expect("tenant connection joins");
    owner_conn.await.expect("owner connection joins");
    bypass_conn.await.expect("bypass connection joins");
    drop(admin);
    admin_conn.await.expect("admin connection joins");
    server_handle.abort();
    let _ = server_handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rls_owner_and_bypass_semantics_survive_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "rls_owner_restart_setup").await;
    for sql in [
        "CREATE ROLE tester SUPERUSER LOGIN",
        "CREATE ROLE app_owner LOGIN",
        "SET ROLE app_owner",
        "CREATE TABLE rls_restart_owner_docs (tenant_id TEXT NOT NULL, doc_id TEXT NOT NULL)",
        "RESET ROLE",
        "INSERT INTO rls_restart_owner_docs VALUES ('tenant-a', 'doc-a'), ('tenant-b', 'doc-b')",
        "SET ROLE app_owner",
        "CREATE POLICY rls_restart_owner_docs_tenant ON rls_restart_owner_docs \
            USING (tenant_id = current_setting('ultrasql.tenant_id', true)) \
            WITH CHECK (tenant_id = current_setting('ultrasql.tenant_id', true))",
        "ALTER TABLE rls_restart_owner_docs ENABLE ROW LEVEL SECURITY",
        "RESET ROLE",
    ] {
        running.client.batch_execute(sql).await.expect(sql);
    }
    graceful_shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "rls_owner_restart_verify").await;
    for sql in [
        "CREATE ROLE tenant_user LOGIN",
        "CREATE ROLE rls_bypass LOGIN BYPASSRLS",
        "GRANT SELECT ON TABLE rls_restart_owner_docs TO app_owner, tenant_user, rls_bypass",
    ] {
        running.client.batch_execute(sql).await.expect(sql);
    }

    let (tenant, tenant_conn) =
        connect_as(running.bound, "tenant_user", "rls_owner_restart_tenant").await;
    tenant
        .batch_execute("SET ultrasql.tenant_id = 'tenant-a'")
        .await
        .expect("set tenant");
    let rows = simple_rows(
        &tenant
            .simple_query("SELECT doc_id FROM rls_restart_owner_docs ORDER BY doc_id")
            .await
            .expect("tenant sees restarted policy"),
    );
    assert_eq!(rows, vec![vec!["doc-a".to_owned()]]);

    let (owner, owner_conn) =
        connect_as(running.bound, "app_owner", "rls_owner_restart_owner").await;
    let rows = simple_rows(
        &owner
            .simple_query("SELECT doc_id FROM rls_restart_owner_docs ORDER BY doc_id")
            .await
            .expect("restarted owner bypasses RLS"),
    );
    assert_eq!(
        rows,
        vec![vec!["doc-a".to_owned()], vec!["doc-b".to_owned()]]
    );

    let (bypass, bypass_conn) =
        connect_as(running.bound, "rls_bypass", "rls_owner_restart_bypass").await;
    let rows = simple_rows(
        &bypass
            .simple_query("SELECT doc_id FROM rls_restart_owner_docs ORDER BY doc_id")
            .await
            .expect("restarted BYPASSRLS role bypasses RLS"),
    );
    assert_eq!(
        rows,
        vec![vec!["doc-a".to_owned()], vec!["doc-b".to_owned()]]
    );

    drop(tenant);
    drop(owner);
    drop(bypass);
    tenant_conn.await.expect("tenant connection joins");
    owner_conn.await.expect("owner connection joins");
    bypass_conn.await.expect("bypass connection joins");
    graceful_shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rls_tenant_policy_survives_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "rls_restart_test").await;
    running
        .client
        .batch_execute(
            "CREATE TABLE tenant_docs_restart (\
                tenant_id TEXT NOT NULL, \
                doc_id TEXT NOT NULL, \
                body TEXT\
             )",
        )
        .await
        .expect("create tenant table");
    running
        .client
        .batch_execute(
            "INSERT INTO tenant_docs_restart VALUES \
                ('tenant-a', 'doc-a', 'alpha'), \
                ('tenant-b', 'doc-b', 'bravo')",
        )
        .await
        .expect("insert seed rows");
    running
        .client
        .batch_execute(
            "CREATE POLICY tenant_docs_restart_isolation ON tenant_docs_restart \
                USING (tenant_id = current_setting('ultrasql.tenant_id', true)) \
                WITH CHECK (tenant_id = current_setting('ultrasql.tenant_id', true))",
        )
        .await
        .expect("create tenant rls policy");
    running
        .client
        .batch_execute("ALTER TABLE tenant_docs_restart ENABLE ROW LEVEL SECURITY")
        .await
        .expect("enable table rls");
    graceful_shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "rls_restart_test").await;
    running
        .client
        .batch_execute("SET ultrasql.tenant_id = 'tenant-a'")
        .await
        .expect("set tenant guc");
    let rows = simple_rows(
        &running
            .client
            .simple_query("SELECT doc_id FROM tenant_docs_restart ORDER BY doc_id")
            .await
            .expect("select tenant-a rows after restart"),
    );
    assert_eq!(rows, vec![vec!["doc-a".to_owned()]]);
    let err = running
        .client
        .batch_execute("INSERT INTO tenant_docs_restart VALUES ('tenant-b', 'doc-b-2', 'bravo-2')")
        .await
        .expect_err("cross-tenant insert must fail after restart");
    assert!(
        err.as_db_error()
            .is_some_and(|db| db.message().contains("row-level security")),
        "expected RLS error after restart, got {err:?}"
    );
    graceful_shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_table_removes_rls_restart_metadata() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let metadata_path = data_dir.path().join("pg_row_security.meta");

    let running = start_persistent_server(data_dir.path(), "rls_drop_metadata").await;
    for sql in [
        "CREATE TABLE rls_drop_docs (tenant_id TEXT NOT NULL, doc_id TEXT NOT NULL)",
        "CREATE POLICY rls_drop_docs_tenant ON rls_drop_docs \
            USING (tenant_id = current_setting('ultrasql.tenant_id', true))",
        "ALTER TABLE rls_drop_docs ENABLE ROW LEVEL SECURITY",
    ] {
        running.client.batch_execute(sql).await.expect(sql);
    }
    let metadata = std::fs::read_to_string(&metadata_path).expect("RLS metadata exists");
    assert!(
        metadata.contains("rls_drop_docs"),
        "RLS metadata should record table before drop: {metadata}"
    );

    running
        .client
        .batch_execute("DROP TABLE rls_drop_docs")
        .await
        .expect("drop RLS table");
    graceful_shutdown(running).await;

    let metadata = std::fs::read_to_string(&metadata_path).expect("RLS metadata exists");
    assert!(
        !metadata.contains("rls_drop_docs"),
        "dropped table must be removed from RLS metadata: {metadata}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rls_metadata_rejects_duplicate_policy_names_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let metadata_path = data_dir.path().join("pg_row_security.meta");

    let running = start_persistent_server(data_dir.path(), "rls_duplicate_policy_setup").await;
    for sql in [
        "CREATE TABLE rls_duplicate_policy_docs (tenant_id TEXT NOT NULL, doc_id TEXT NOT NULL)",
        "CREATE POLICY rls_duplicate_policy_docs_tenant ON rls_duplicate_policy_docs \
            USING (tenant_id = current_setting('ultrasql.tenant_id', true))",
        "ALTER TABLE rls_duplicate_policy_docs ENABLE ROW LEVEL SECURITY",
    ] {
        running.client.batch_execute(sql).await.expect(sql);
    }
    graceful_shutdown(running).await;

    let mut metadata = std::fs::read_to_string(&metadata_path).expect("RLS metadata exists");
    let policy_line = metadata
        .lines()
        .find(|line| line.starts_with("policy\t"))
        .expect("policy metadata row")
        .to_owned();
    metadata.push_str(&policy_line);
    metadata.push('\n');
    std::fs::write(&metadata_path, metadata).expect("duplicate policy metadata");

    let err = Server::init(data_dir.path()).expect_err("duplicate RLS policy metadata rejected");
    assert!(
        err.to_string().contains("duplicate RLS policy metadata"),
        "expected duplicate RLS policy metadata rejection, got {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rls_metadata_rejects_duplicate_table_rows_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let metadata_path = data_dir.path().join("pg_row_security.meta");

    let running = start_persistent_server(data_dir.path(), "rls_duplicate_table_setup").await;
    for sql in [
        "CREATE TABLE rls_duplicate_table_docs (tenant_id TEXT NOT NULL, doc_id TEXT NOT NULL)",
        "ALTER TABLE rls_duplicate_table_docs ENABLE ROW LEVEL SECURITY",
    ] {
        running.client.batch_execute(sql).await.expect(sql);
    }
    graceful_shutdown(running).await;

    let mut metadata = std::fs::read_to_string(&metadata_path).expect("RLS metadata exists");
    let table_line = metadata
        .lines()
        .find(|line| line.starts_with("table\t"))
        .expect("table metadata row")
        .to_owned();
    metadata.push_str(&table_line);
    metadata.push('\n');
    std::fs::write(&metadata_path, metadata).expect("duplicate table metadata");

    let err = Server::init(data_dir.path()).expect_err("duplicate RLS table metadata rejected");
    assert!(
        err.to_string().contains("duplicate RLS table metadata"),
        "expected duplicate RLS table metadata rejection, got {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rls_metadata_rejects_unknown_table_rows_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let metadata_path = data_dir.path().join("pg_row_security.meta");

    let running = start_persistent_server(data_dir.path(), "rls_unknown_table_setup").await;
    for sql in [
        "CREATE TABLE rls_known_docs (tenant_id TEXT NOT NULL, doc_id TEXT NOT NULL)",
        "ALTER TABLE rls_known_docs ENABLE ROW LEVEL SECURITY",
    ] {
        running.client.batch_execute(sql).await.expect(sql);
    }
    graceful_shutdown(running).await;

    let mut metadata = std::fs::read_to_string(&metadata_path).expect("RLS metadata exists");
    metadata.push_str("table\tghost_rls\t424242\ttrue\ttester\n");
    std::fs::write(&metadata_path, metadata).expect("unknown table metadata");

    let err = Server::init(data_dir.path()).expect_err("unknown RLS table metadata rejected");
    assert!(
        err.to_string().contains("unknown RLS table metadata"),
        "expected unknown RLS table metadata rejection, got {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rls_policy_roles_scope_visibility_and_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "rls_role_scope_setup").await;
    for sql in [
        "CREATE ROLE tester SUPERUSER LOGIN",
        "CREATE ROLE tenant_group NOLOGIN",
        "CREATE ROLE tenant_reader LOGIN",
        "CREATE ROLE tenant_blocked LOGIN",
        "GRANT tenant_group TO tenant_reader",
        "CREATE TABLE rls_role_docs (tenant_id TEXT NOT NULL, doc_id TEXT NOT NULL)",
        "INSERT INTO rls_role_docs VALUES ('tenant-a', 'doc-a'), ('tenant-b', 'doc-b')",
        "CREATE POLICY rls_role_docs_tenant ON rls_role_docs \
            FOR SELECT TO tenant_group \
            USING (tenant_id = current_setting('ultrasql.tenant_id', true))",
        "ALTER TABLE rls_role_docs ENABLE ROW LEVEL SECURITY",
        "GRANT SELECT ON TABLE rls_role_docs TO tenant_group, tenant_blocked",
    ] {
        running.client.batch_execute(sql).await.expect(sql);
    }
    graceful_shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "rls_role_scope_verify").await;

    let (reader, reader_conn) =
        connect_as(running.bound, "tenant_reader", "rls_role_scope_reader").await;
    reader
        .batch_execute("SET ultrasql.tenant_id = 'tenant-a'")
        .await
        .expect("set reader tenant");
    let rows = simple_rows(
        &reader
            .simple_query("SELECT doc_id FROM rls_role_docs ORDER BY doc_id")
            .await
            .expect("role member sees scoped policy rows"),
    );
    assert_eq!(rows, vec![vec!["doc-a".to_owned()]]);

    let (blocked, blocked_conn) =
        connect_as(running.bound, "tenant_blocked", "rls_role_scope_blocked").await;
    blocked
        .batch_execute("SET ultrasql.tenant_id = 'tenant-a'")
        .await
        .expect("set blocked tenant");
    let rows = simple_rows(
        &blocked
            .simple_query("SELECT doc_id FROM rls_role_docs ORDER BY doc_id")
            .await
            .expect("role outside policy sees no rows"),
    );
    assert!(rows.is_empty(), "policy-scoped role leaked rows: {rows:?}");

    drop(reader);
    drop(blocked);
    reader_conn.await.expect("reader connection joins");
    blocked_conn.await.expect("blocked connection joins");
    graceful_shutdown(running).await;
}

#[tokio::test]
async fn rls_insert_uses_insert_policies_not_select_using_predicates() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE tenant_docs_insert (\
                tenant_id TEXT NOT NULL, \
                doc_id TEXT NOT NULL, \
                body TEXT\
             )",
        )
        .await
        .expect("create tenant table");
    client
        .batch_execute(
            "INSERT INTO tenant_docs_insert VALUES \
                ('tenant-a', 'doc-a', 'alpha'), \
                ('tenant-b', 'doc-b', 'bravo')",
        )
        .await
        .expect("insert seed rows");
    client
        .batch_execute(
            "CREATE POLICY tenant_docs_insert_read ON tenant_docs_insert \
                FOR SELECT \
                USING (tenant_id = current_setting('ultrasql.tenant_id', true))",
        )
        .await
        .expect("create select policy");
    client
        .batch_execute("ALTER TABLE tenant_docs_insert ENABLE ROW LEVEL SECURITY")
        .await
        .expect("enable table rls");
    client
        .batch_execute("SET ultrasql.tenant_id = 'tenant-a'")
        .await
        .expect("set tenant guc");

    let rows = simple_rows(
        &client
            .simple_query("SELECT doc_id FROM tenant_docs_insert ORDER BY doc_id")
            .await
            .expect("select tenant-a rows"),
    );
    assert_eq!(rows, vec![vec!["doc-a".to_owned()]]);

    let err = client
        .batch_execute("INSERT INTO tenant_docs_insert VALUES ('tenant-a', 'doc-a-2', 'alpha-2')")
        .await
        .expect_err("SELECT policy must not authorize INSERT");
    assert!(
        err.as_db_error()
            .is_some_and(|db| db.message().contains("row-level security")),
        "unexpected error: {err}"
    );

    client
        .batch_execute(
            "CREATE POLICY tenant_docs_insert_write ON tenant_docs_insert \
                FOR INSERT \
                WITH CHECK (tenant_id = current_setting('ultrasql.tenant_id', true))",
        )
        .await
        .expect("create insert policy");
    client
        .batch_execute("INSERT INTO tenant_docs_insert VALUES ('tenant-a', 'doc-a-3', 'alpha-3')")
        .await
        .expect("same-tenant insert passes after FOR INSERT policy");
    let err = client
        .batch_execute("INSERT INTO tenant_docs_insert VALUES ('tenant-b', 'doc-b-2', 'bravo-2')")
        .await
        .expect_err("cross-tenant insert must fail INSERT policy");
    assert!(
        err.as_db_error()
            .is_some_and(|db| db.message().contains("row-level security")),
        "unexpected error: {err}"
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn rls_insert_select_checks_source_rows_atomically() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE tenant_docs_insert_select_source (\
                tenant_id TEXT NOT NULL, \
                doc_id TEXT NOT NULL\
             )",
        )
        .await
        .expect("create source table");
    client
        .batch_execute(
            "CREATE TABLE tenant_docs_insert_select_target (\
                tenant_id TEXT NOT NULL, \
                doc_id TEXT NOT NULL\
             )",
        )
        .await
        .expect("create target table");
    client
        .batch_execute(
            "INSERT INTO tenant_docs_insert_select_source VALUES \
                ('tenant-a', 'doc-a'), \
                ('tenant-b', 'doc-b')",
        )
        .await
        .expect("insert source rows");
    client
        .batch_execute(
            "CREATE POLICY tenant_docs_insert_select_write \
                ON tenant_docs_insert_select_target \
                FOR INSERT \
                WITH CHECK (tenant_id = current_setting('ultrasql.tenant_id', true))",
        )
        .await
        .expect("create insert policy");
    client
        .batch_execute(
            "CREATE POLICY tenant_docs_insert_select_read \
                ON tenant_docs_insert_select_target \
                FOR SELECT \
                USING (tenant_id = current_setting('ultrasql.tenant_id', true))",
        )
        .await
        .expect("create select policy");
    client
        .batch_execute("ALTER TABLE tenant_docs_insert_select_target ENABLE ROW LEVEL SECURITY")
        .await
        .expect("enable target rls");
    client
        .batch_execute("SET ultrasql.tenant_id = 'tenant-a'")
        .await
        .expect("set tenant guc");

    client
        .batch_execute(
            "INSERT INTO tenant_docs_insert_select_target \
                SELECT tenant_id, doc_id \
                FROM tenant_docs_insert_select_source \
                WHERE tenant_id = 'tenant-a'",
        )
        .await
        .expect("same-tenant insert-select passes");
    let rows = simple_rows(
        &client
            .simple_query("SELECT doc_id FROM tenant_docs_insert_select_target ORDER BY doc_id")
            .await
            .expect("select inserted same-tenant rows"),
    );
    assert_eq!(rows, vec![vec!["doc-a".to_owned()]]);

    let err = client
        .batch_execute(
            "INSERT INTO tenant_docs_insert_select_target \
                SELECT tenant_id, doc_id \
                FROM tenant_docs_insert_select_source \
                ORDER BY doc_id",
        )
        .await
        .expect_err("cross-tenant insert-select must fail RLS WITH CHECK");
    assert!(
        err.as_db_error()
            .is_some_and(|db| db.message().contains("row-level security")),
        "unexpected error: {err}"
    );
    let rows = simple_rows(
        &client
            .simple_query("SELECT doc_id FROM tenant_docs_insert_select_target ORDER BY doc_id")
            .await
            .expect("select rows after rejected insert-select"),
    );
    assert_eq!(rows, vec![vec!["doc-a".to_owned()]]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn rls_update_checks_new_rows_and_preserves_old_row_on_failure() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE tenant_docs_update_check (\
                tenant_id TEXT NOT NULL, \
                doc_id TEXT NOT NULL, \
                body TEXT\
             )",
        )
        .await
        .expect("create update rls table");
    client
        .batch_execute(
            "INSERT INTO tenant_docs_update_check VALUES \
                ('tenant-a', 'doc-a', 'alpha'), \
                ('tenant-b', 'doc-b', 'bravo')",
        )
        .await
        .expect("insert update rls rows");
    client
        .batch_execute(
            "CREATE POLICY tenant_docs_update_check_read \
                ON tenant_docs_update_check \
                FOR SELECT \
                USING (tenant_id = current_setting('ultrasql.tenant_id', true))",
        )
        .await
        .expect("create select policy");
    client
        .batch_execute(
            "CREATE POLICY tenant_docs_update_check_write \
                ON tenant_docs_update_check \
                FOR UPDATE \
                USING (tenant_id = current_setting('ultrasql.tenant_id', true)) \
                WITH CHECK (tenant_id = current_setting('ultrasql.tenant_id', true))",
        )
        .await
        .expect("create update policy");
    client
        .batch_execute("ALTER TABLE tenant_docs_update_check ENABLE ROW LEVEL SECURITY")
        .await
        .expect("enable update rls");
    client
        .batch_execute("SET ultrasql.tenant_id = 'tenant-a'")
        .await
        .expect("set tenant guc");

    client
        .batch_execute(
            "UPDATE tenant_docs_update_check SET body = 'alpha-2' WHERE doc_id = 'doc-a'",
        )
        .await
        .expect("same-tenant update passes");
    let err = client
        .batch_execute(
            "UPDATE tenant_docs_update_check SET tenant_id = 'tenant-b' WHERE doc_id = 'doc-a'",
        )
        .await
        .expect_err("cross-tenant update must fail RLS WITH CHECK");
    assert!(
        err.as_db_error()
            .is_some_and(|db| db.message().contains("row-level security")),
        "unexpected error: {err}"
    );
    let rows = simple_rows(
        &client
            .simple_query(
                "SELECT tenant_id, doc_id, body \
                 FROM tenant_docs_update_check \
                 ORDER BY doc_id",
            )
            .await
            .expect("select rows after rejected update"),
    );
    assert_eq!(
        rows,
        vec![vec![
            "tenant-a".to_owned(),
            "doc-a".to_owned(),
            "alpha-2".to_owned(),
        ]]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn rls_restrictive_select_policies_narrow_permissive_visibility() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE tenant_docs_restrictive (\
                tenant_id TEXT NOT NULL, \
                visibility TEXT NOT NULL, \
                doc_id TEXT NOT NULL\
             )",
        )
        .await
        .expect("create tenant table");
    client
        .batch_execute(
            "INSERT INTO tenant_docs_restrictive VALUES \
                ('tenant-a', 'public', 'doc-a-public'), \
                ('tenant-a', 'private', 'doc-a-private'), \
                ('tenant-b', 'public', 'doc-b-public')",
        )
        .await
        .expect("insert seed rows");
    client
        .batch_execute(
            "CREATE POLICY tenant_docs_restrictive_tenant ON tenant_docs_restrictive \
                AS PERMISSIVE \
                FOR SELECT \
                USING (tenant_id = current_setting('ultrasql.tenant_id', true))",
        )
        .await
        .expect("create permissive tenant policy");
    client
        .batch_execute(
            "CREATE POLICY tenant_docs_restrictive_visibility ON tenant_docs_restrictive \
                AS RESTRICTIVE \
                FOR SELECT \
                USING (visibility = current_setting('ultrasql.visibility', true))",
        )
        .await
        .expect("create restrictive visibility policy");
    client
        .batch_execute("ALTER TABLE tenant_docs_restrictive ENABLE ROW LEVEL SECURITY")
        .await
        .expect("enable table rls");
    client
        .batch_execute("SET ultrasql.tenant_id = 'tenant-a'")
        .await
        .expect("set tenant guc");
    client
        .batch_execute("SET ultrasql.visibility = 'public'")
        .await
        .expect("set visibility guc");

    let rows = simple_rows(
        &client
            .simple_query("SELECT doc_id FROM tenant_docs_restrictive ORDER BY doc_id")
            .await
            .expect("select narrowed rows"),
    );
    assert_eq!(rows, vec![vec!["doc-a-public".to_owned()]]);

    shutdown(client, server_handle).await;
}
