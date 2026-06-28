//! End-to-end row-level security tests.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::{NoTls, SimpleQueryMessage, error::SqlState};
use ultrasql_server::{Server, bind_listener, serve_listener};

pub mod support;

use support::{shutdown as graceful_shutdown, start_persistent_server, start_sample_server};

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

fn assert_insufficient_privilege(err: tokio_postgres::Error) {
    let db = err.as_db_error().expect("database error");
    assert_eq!(
        db.code(),
        &SqlState::INSUFFICIENT_PRIVILEGE,
        "{}",
        db.message()
    );
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
async fn non_owner_cannot_create_rls_policy() {
    let running = start_sample_server("rls_policy_owner_test").await;
    let client = &running.client;

    for sql in [
        "CREATE ROLE tester SUPERUSER LOGIN",
        "CREATE ROLE app_owner LOGIN",
        "CREATE ROLE rls_attacker LOGIN",
        "SET ROLE app_owner",
        "CREATE TABLE rls_policy_owned_docs (tenant_id TEXT NOT NULL)",
        "RESET ROLE",
    ] {
        client.batch_execute(sql).await.expect(sql);
    }

    let (attacker, attacker_conn) =
        connect_as(running.bound, "rls_attacker", "rls_policy_attacker").await;
    assert_insufficient_privilege(
        attacker
            .batch_execute(
                "CREATE POLICY rls_policy_owned_docs_attack \
                 ON rls_policy_owned_docs \
                 FOR SELECT TO public \
                 USING (tenant_id = current_setting('ultrasql.tenant_id', true))",
            )
            .await
            .expect_err("non-owner cannot create RLS policy"),
    );
    drop(attacker);
    attacker_conn.await.expect("attacker connection joins");

    graceful_shutdown(running).await;
}

#[tokio::test]
async fn create_policy_respects_schema_qualifier() {
    let running = start_sample_server("rls_policy_schema_qualifier_guard").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE TABLE guarded_policy (tenant_id TEXT NOT NULL)",
        )
        .await
        .expect("create public table and separate schema");

    client
        .batch_execute(
            "CREATE POLICY guarded_policy_isolation \
             ON app.guarded_policy \
             USING (tenant_id = current_setting('ultrasql.tenant_id', true))",
        )
        .await
        .expect_err("qualified CREATE POLICY must not resolve public table");

    let rows = client
        .query(
            "SELECT polname \
             FROM pg_catalog.pg_policy \
             WHERE polname = 'guarded_policy_isolation'",
            &[],
        )
        .await
        .expect("query policy catalog after rejected qualified CREATE POLICY");
    assert!(
        rows.is_empty(),
        "wrong-qualified CREATE POLICY must not attach policy to public table"
    );

    client
        .batch_execute("DROP TABLE guarded_policy; DROP SCHEMA app")
        .await
        .expect("cleanup CREATE POLICY qualifier guard");

    graceful_shutdown(running).await;
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
    support::make_data_dir_private(data_dir.path());
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
    support::make_data_dir_private(data_dir.path());
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
    support::make_data_dir_private(data_dir.path());
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
    support::make_data_dir_private(data_dir.path());
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
async fn rls_metadata_rejects_unknown_table_owner_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    support::make_data_dir_private(data_dir.path());
    let metadata_path = data_dir.path().join("pg_row_security.meta");

    let running = start_persistent_server(data_dir.path(), "rls_unknown_owner_setup").await;
    for sql in [
        "CREATE TABLE rls_unknown_owner_docs (tenant_id TEXT NOT NULL, doc_id TEXT NOT NULL)",
        "ALTER TABLE rls_unknown_owner_docs ENABLE ROW LEVEL SECURITY",
    ] {
        running.client.batch_execute(sql).await.expect(sql);
    }
    graceful_shutdown(running).await;

    let metadata = std::fs::read_to_string(&metadata_path).expect("RLS metadata exists");
    let mut changed = false;
    let tampered = metadata
        .lines()
        .map(|line| {
            if changed || !line.starts_with("table\t") {
                return line.to_owned();
            }
            let mut parts = line.split('\t').collect::<Vec<_>>();
            parts[4] = "missing_owner";
            changed = true;
            parts.join("\t")
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(changed, "RLS metadata should include table row: {metadata}");
    std::fs::write(&metadata_path, format!("{tampered}\n")).expect("unknown RLS owner metadata");

    let err = Server::init(data_dir.path()).expect_err("unknown RLS table owner rejected");
    assert!(
        err.to_string().contains("unknown RLS table metadata owner"),
        "expected unknown RLS table owner rejection, got {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rls_metadata_rejects_unknown_policy_roles_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    support::make_data_dir_private(data_dir.path());
    let metadata_path = data_dir.path().join("pg_row_security.meta");

    let running = start_persistent_server(data_dir.path(), "rls_unknown_role_setup").await;
    for sql in [
        "CREATE ROLE tester SUPERUSER LOGIN",
        "CREATE ROLE tenant_group NOLOGIN",
        "CREATE TABLE rls_unknown_role_docs (tenant_id TEXT NOT NULL, doc_id TEXT NOT NULL)",
        "CREATE POLICY rls_unknown_role_docs_tenant ON rls_unknown_role_docs \
            FOR SELECT TO tenant_group \
            USING (tenant_id = current_setting('ultrasql.tenant_id', true))",
        "ALTER TABLE rls_unknown_role_docs ENABLE ROW LEVEL SECURITY",
    ] {
        running.client.batch_execute(sql).await.expect(sql);
    }
    graceful_shutdown(running).await;

    let metadata = std::fs::read_to_string(&metadata_path).expect("RLS metadata exists");
    assert!(
        metadata.contains("tenant_group"),
        "RLS metadata should record scoped role: {metadata}"
    );
    std::fs::write(
        &metadata_path,
        metadata.replace("tenant_group", "missing_role"),
    )
    .expect("unknown RLS role metadata");

    let err = match Server::init(data_dir.path()) {
        Ok(_) => panic!("unknown RLS policy role should be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("unknown RLS policy role"),
        "expected unknown RLS policy role rejection, got {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rls_metadata_rejects_invalid_policy_columns_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    support::make_data_dir_private(data_dir.path());
    let metadata_path = data_dir.path().join("pg_row_security.meta");

    let running = start_persistent_server(data_dir.path(), "rls_bad_column_setup").await;
    for sql in [
        "CREATE TABLE rls_bad_column_docs (tenant_id TEXT NOT NULL, doc_id TEXT NOT NULL)",
        "CREATE POLICY rls_bad_column_docs_tenant ON rls_bad_column_docs \
            USING (tenant_id = current_setting('ultrasql.tenant_id', true))",
        "ALTER TABLE rls_bad_column_docs ENABLE ROW LEVEL SECURITY",
    ] {
        running.client.batch_execute(sql).await.expect(sql);
    }
    graceful_shutdown(running).await;

    let metadata = std::fs::read_to_string(&metadata_path).expect("RLS metadata exists");
    let mut changed = false;
    let tampered = metadata
        .lines()
        .map(|line| {
            if changed || !line.starts_with("policy\t") {
                return line.to_owned();
            }
            let mut parts = line.split('\t').collect::<Vec<_>>();
            parts[5] = "99";
            changed = true;
            parts.join("\t")
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        changed,
        "RLS metadata should include policy row: {metadata}"
    );
    std::fs::write(&metadata_path, format!("{tampered}\n")).expect("bad RLS column metadata");

    let err = match Server::init(data_dir.path()) {
        Ok(_) => panic!("invalid RLS policy column should be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("RLS metadata") && err.to_string().contains("column index"),
        "expected invalid RLS column rejection, got {err}"
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

/// Regression: a row-level-security policy must apply to a table read through
/// an uncorrelated scalar subquery, even after the optimizer decorrelates it
/// and wraps the subquery's right side in a `SingleRowAssert` cardinality
/// guard. Before the fix, `apply_row_security` treated `SingleRowAssert` as a
/// leaf (its `_ => Ok(None)` catch-all), dropping the inner scan subtree, so
/// the policy predicate was never injected and the scalar saw EVERY tenant's
/// rows (`SELECT (SELECT sum(val) FROM secret)` returned 60, not 10).
#[tokio::test]
async fn rls_applies_through_scalar_subquery_single_row_assert() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE secret (\
                tenant_id TEXT NOT NULL, \
                val INT NOT NULL\
             )",
        )
        .await
        .expect("create secret table");
    // tenant-a sums to 10; tenant-b sums to 50. Across all tenants: 60.
    client
        .batch_execute(
            "INSERT INTO secret VALUES \
                ('tenant-a', 10), \
                ('tenant-b', 20), \
                ('tenant-b', 30)",
        )
        .await
        .expect("insert seed rows");
    // A second table so the scalar subquery is uncorrelated (decorrelation
    // hoists it as the right side of a CROSS join wrapped in SingleRowAssert).
    client
        .batch_execute("CREATE TABLE outer_t (k INT NOT NULL)")
        .await
        .expect("create outer table");
    client
        .batch_execute("INSERT INTO outer_t VALUES (1)")
        .await
        .expect("insert outer row");
    client
        .batch_execute(
            "CREATE POLICY secret_tenant_isolation ON secret \
                USING (tenant_id = current_setting('ultrasql.tenant_id', true))",
        )
        .await
        .expect("create tenant rls policy");
    client
        .batch_execute("ALTER TABLE secret ENABLE ROW LEVEL SECURITY")
        .await
        .expect("enable table rls");
    client
        .batch_execute("SET ultrasql.tenant_id = 'tenant-a'")
        .await
        .expect("set tenant guc");

    // Control: a direct aggregate sees only tenant-a (10), never 60.
    let rows = simple_rows(
        &client
            .simple_query("SELECT sum(val) FROM secret")
            .await
            .expect("direct aggregate respects RLS"),
    );
    assert_eq!(
        rows,
        vec![vec!["10".to_owned()]],
        "direct aggregate must reflect only the current tenant"
    );

    // (1) SELECT-list scalar subquery: the bypass site. Must be 10, not 60.
    let rows = simple_rows(
        &client
            .simple_query("SELECT (SELECT sum(val) FROM secret) FROM outer_t")
            .await
            .expect("scalar subquery in SELECT list respects RLS"),
    );
    assert_eq!(
        rows,
        vec![vec!["10".to_owned()]],
        "scalar subquery leaked other tenants' rows (RLS bypassed)"
    );

    // (2) WHERE-position scalar subquery: the tenant-filtered value is 10, so
    // the predicate holds and the outer row survives. If RLS were bypassed the
    // subquery would be 60 and the row would be filtered out.
    let rows = simple_rows(
        &client
            .simple_query("SELECT k FROM outer_t WHERE (SELECT sum(val) FROM secret) = 10")
            .await
            .expect("scalar subquery in WHERE respects RLS"),
    );
    assert_eq!(
        rows,
        vec![vec!["1".to_owned()]],
        "WHERE-clause scalar subquery did not reflect the tenant-filtered value"
    );

    // (3) RLS filters BEFORE the single-row assert: with only tenant-a visible,
    // a row-returning scalar subquery yields exactly one row (no 21000), and it
    // is the current tenant's value (10).
    let rows = simple_rows(
        &client
            .simple_query("SELECT (SELECT val FROM secret) FROM outer_t")
            .await
            .expect("row-returning scalar subquery respects RLS and is single-row"),
    );
    assert_eq!(
        rows,
        vec![vec!["10".to_owned()]],
        "row-returning scalar subquery leaked other tenants' rows"
    );

    // (3b) Multiple visible tenant rows still raise cardinality 21000 AFTER RLS
    // has filtered: switch to tenant-b, which has two surviving rows.
    client
        .batch_execute("SET ultrasql.tenant_id = 'tenant-b'")
        .await
        .expect("switch tenant guc");
    let err = client
        .simple_query("SELECT (SELECT val FROM secret) FROM outer_t")
        .await
        .expect_err("multi-row scalar subquery must raise 21000 after RLS filtering");
    let db = err.as_db_error().expect("database error");
    assert_eq!(
        db.code(),
        &SqlState::CARDINALITY_VIOLATION,
        "expected 21000 cardinality violation, got: {}",
        db.message()
    );
    // And tenant-b's aggregate is 50 (20 + 30), never 60.
    let rows = simple_rows(
        &client
            .simple_query("SELECT (SELECT sum(val) FROM secret) FROM outer_t")
            .await
            .expect("tenant-b scalar aggregate respects RLS"),
    );
    assert_eq!(
        rows,
        vec![vec!["50".to_owned()]],
        "tenant-b scalar subquery must sum only tenant-b rows"
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

/// Regression / no-bypass: a row-level-security policy must apply when the
/// protected table is read through a `DISTINCT ON` table factor. The binder
/// lowers `SELECT DISTINCT ON (...)` to `Project(DistinctOn(Sort(Scan)))`.
/// Before the fail-closed hardening, `apply_row_security` had no `DistinctOn`
/// arm, so it hit the `_ => Ok(None)` catch-all, dropped the RLS-wrapped scan
/// subtree, and the query saw EVERY tenant's rows (fail-open bypass).
#[tokio::test]
async fn rls_applies_through_distinct_on() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE distinct_secret (\
                tenant_id TEXT NOT NULL, \
                grp TEXT NOT NULL, \
                doc_id TEXT NOT NULL\
             )",
        )
        .await
        .expect("create table");
    // Per group, the OTHER tenant's row sorts FIRST by doc_id (b-* < a-*), so
    // `DISTINCT ON (grp) ... ORDER BY grp, doc_id` would emit the tenant-b row
    // per group if RLS were bypassed. With RLS, tenant-b's rows never reach the
    // dedup, so the tenant-a rows are the first (and only) per group.
    client
        .batch_execute(
            "INSERT INTO distinct_secret VALUES \
                ('tenant-a', 'g1', 'a-doc'), \
                ('tenant-a', 'g2', 'a-doc'), \
                ('tenant-b', 'g1', 'b-doc'), \
                ('tenant-b', 'g2', 'b-doc')",
        )
        .await
        .expect("insert seed rows");
    client
        .batch_execute(
            "CREATE POLICY distinct_secret_isolation ON distinct_secret \
                USING (tenant_id = current_setting('ultrasql.tenant_id', true))",
        )
        .await
        .expect("create tenant rls policy");
    client
        .batch_execute("ALTER TABLE distinct_secret ENABLE ROW LEVEL SECURITY")
        .await
        .expect("enable table rls");
    client
        .batch_execute("SET ultrasql.tenant_id = 'tenant-a'")
        .await
        .expect("set tenant guc");

    // DISTINCT ON (grp): if RLS were bypassed, b-doc (which sorts first) would
    // be the chosen row per group. With RLS only tenant-a's rows are visible,
    // so a-doc is emitted for each group.
    let rows = simple_rows(
        &client
            .simple_query(
                "SELECT DISTINCT ON (grp) grp, doc_id FROM distinct_secret ORDER BY grp, doc_id",
            )
            .await
            .expect("distinct on respects RLS"),
    );
    assert_eq!(
        rows,
        vec![
            vec!["g1".to_owned(), "a-doc".to_owned()],
            vec!["g2".to_owned(), "a-doc".to_owned()],
        ],
        "DISTINCT ON leaked other tenants' rows (RLS bypassed)"
    );

    // Decisive bypass detector: with the tenant GUC set to a value matching no
    // row, RLS must filter EVERYTHING. A bypass would still surface rows here.
    client
        .batch_execute("SET ultrasql.tenant_id = 'nonexistent-tenant'")
        .await
        .expect("set nonexistent tenant guc");
    let rows = simple_rows(
        &client
            .simple_query(
                "SELECT DISTINCT ON (grp) grp, doc_id FROM distinct_secret ORDER BY grp, doc_id",
            )
            .await
            .expect("distinct on with no matching tenant"),
    );
    assert!(
        rows.is_empty(),
        "DISTINCT ON returned rows for a tenant that owns none (RLS bypassed): {rows:?}"
    );

    shutdown(client, server_handle).await;
}

/// No-bypass sweep: RLS must be enforced through every common query shape over
/// a protected table — bare scan, ORDER BY, LIMIT, DISTINCT, GROUP BY, window,
/// UNION, CTE, and an IN-subquery. Each must return ONLY the current tenant's
/// rows. This guards against a future plan-shape change re-opening a silent
/// bypass: if any shape stopped applying the policy it would surface a
/// tenant-b row here (or an inflated aggregate).
#[tokio::test]
async fn rls_no_bypass_through_common_shapes() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE sweep_secret (\
                tenant_id TEXT NOT NULL, \
                val INT NOT NULL\
             )",
        )
        .await
        .expect("create table");
    // tenant-a: two rows summing to 30; tenant-b: two rows summing to 70.
    client
        .batch_execute(
            "INSERT INTO sweep_secret VALUES \
                ('tenant-a', 10), \
                ('tenant-a', 20), \
                ('tenant-b', 30), \
                ('tenant-b', 40)",
        )
        .await
        .expect("insert seed rows");
    client
        .batch_execute(
            "CREATE POLICY sweep_secret_isolation ON sweep_secret \
                USING (tenant_id = current_setting('ultrasql.tenant_id', true))",
        )
        .await
        .expect("create tenant rls policy");
    client
        .batch_execute("ALTER TABLE sweep_secret ENABLE ROW LEVEL SECURITY")
        .await
        .expect("enable table rls");
    client
        .batch_execute("SET ultrasql.tenant_id = 'tenant-a'")
        .await
        .expect("set tenant guc");

    // Bare scan + ORDER BY + LIMIT: only tenant-a's two rows.
    let rows = simple_rows(
        &client
            .simple_query("SELECT val FROM sweep_secret ORDER BY val DESC LIMIT 5")
            .await
            .expect("order by / limit respects RLS"),
    );
    assert_eq!(rows, vec![vec!["20".to_owned()], vec!["10".to_owned()]]);

    // DISTINCT.
    let rows = simple_rows(
        &client
            .simple_query("SELECT DISTINCT tenant_id FROM sweep_secret")
            .await
            .expect("distinct respects RLS"),
    );
    assert_eq!(rows, vec![vec!["tenant-a".to_owned()]]);

    // GROUP BY / aggregate: tenant-a sums to 30, never 100.
    let rows = simple_rows(
        &client
            .simple_query("SELECT tenant_id, sum(val) FROM sweep_secret GROUP BY tenant_id")
            .await
            .expect("group by respects RLS"),
    );
    assert_eq!(rows, vec![vec!["tenant-a".to_owned(), "30".to_owned()]]);

    // Window function over the protected scan.
    let rows = simple_rows(
        &client
            .simple_query("SELECT val, sum(val) OVER () AS running FROM sweep_secret ORDER BY val")
            .await
            .expect("window respects RLS"),
    );
    assert_eq!(
        rows,
        vec![
            vec!["10".to_owned(), "30".to_owned()],
            vec!["20".to_owned(), "30".to_owned()],
        ],
        "window saw other tenants' rows"
    );

    // UNION of two reads of the protected table: still only tenant-a.
    let rows = simple_rows(
        &client
            .simple_query(
                "SELECT val FROM sweep_secret UNION SELECT val FROM sweep_secret ORDER BY val",
            )
            .await
            .expect("union respects RLS"),
    );
    assert_eq!(rows, vec![vec!["10".to_owned()], vec!["20".to_owned()]]);

    // CTE wrapping the protected scan.
    let rows = simple_rows(
        &client
            .simple_query("WITH t AS (SELECT val FROM sweep_secret) SELECT sum(val) FROM t")
            .await
            .expect("cte respects RLS"),
    );
    assert_eq!(rows, vec![vec!["30".to_owned()]]);

    // IN-subquery against the protected table.
    let rows = simple_rows(
        &client
            .simple_query(
                "SELECT val FROM sweep_secret \
                 WHERE val IN (SELECT val FROM sweep_secret) ORDER BY val",
            )
            .await
            .expect("in-subquery respects RLS"),
    );
    assert_eq!(rows, vec![vec!["10".to_owned()], vec!["20".to_owned()]]);

    // Decisive bypass detector: a tenant GUC matching no row must yield zero
    // rows through every shape. If any shape silently skipped RLS, a row would
    // leak here regardless of how an aggregate/dedup might mask it above.
    client
        .batch_execute("SET ultrasql.tenant_id = 'nobody'")
        .await
        .expect("set nonexistent tenant guc");
    for sql in [
        "SELECT val FROM sweep_secret ORDER BY val",
        "SELECT DISTINCT tenant_id FROM sweep_secret",
        "SELECT val FROM sweep_secret UNION SELECT val FROM sweep_secret",
        "WITH t AS (SELECT val FROM sweep_secret) SELECT val FROM t",
        "SELECT val FROM sweep_secret WHERE val IN (SELECT val FROM sweep_secret)",
    ] {
        let rows = simple_rows(&client.simple_query(sql).await.expect(sql));
        assert!(
            rows.is_empty(),
            "shape leaked rows for a tenant that owns none (RLS bypassed): {sql} -> {rows:?}"
        );
    }

    shutdown(client, server_handle).await;
}

/// The confirmed uncorrelated-EXISTS RLS bypass and the full matrix of
/// expression-embedded subquery shapes.
///
/// Uncorrelated `EXISTS` / `NOT EXISTS` are NOT decorrelated to a join (the
/// decorrelation rule gates on `correlated: true`), so before the fix their
/// raw subplan reached the executor with no policy filter and saw RLS-hidden
/// rows. The fix makes `apply_row_security` descend into every embedded
/// subplan. Here the OUTER table (`driver`) is deliberately NON-RLS and always
/// visible, so the only thing that can change the result is whether the INNER
/// RLS table (`secret`) is policy-filtered inside the subquery.
#[tokio::test]
async fn rls_applies_through_uncorrelated_exists_and_embedded_subplans() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    for sql in [
        // Non-RLS outer table: a single row that must survive iff the inner
        // subquery, after RLS, is empty/zero for a non-matching tenant.
        "CREATE TABLE driver (id INT NOT NULL)",
        "INSERT INTO driver VALUES (1)",
        // RLS-protected inner table.
        "CREATE TABLE secret (tenant_id TEXT NOT NULL, val INT NOT NULL)",
        "INSERT INTO secret VALUES ('tenant-a', 10), ('tenant-b', 20)",
        "CREATE POLICY secret_isolation ON secret \
            USING (tenant_id = current_setting('ultrasql.tenant_id', true))",
        "ALTER TABLE secret ENABLE ROW LEVEL SECURITY",
    ] {
        client.batch_execute(sql).await.expect(sql);
    }

    // ── Non-matching tenant: every embedded-subplan shape must see ZERO
    // permitted `secret` rows. The control `SELECT * FROM secret` returns 0,
    // so any probe that lets a hidden row influence its result is a leak.
    client
        .batch_execute("SET ultrasql.tenant_id = 'nobody'")
        .await
        .expect("set non-matching tenant guc");

    let control = simple_rows(
        &client
            .simple_query("SELECT val FROM secret")
            .await
            .expect("control direct select"),
    );
    assert!(
        control.is_empty(),
        "control: non-matching tenant must see no secret rows, got {control:?}"
    );

    // THE LEAK CLOSED: uncorrelated EXISTS must see zero permitted rows, so the
    // driver row is filtered out (matches `SELECT * FROM secret` -> 0 rows).
    let leak = simple_rows(
        &client
            .simple_query("SELECT id FROM driver WHERE EXISTS (SELECT 1 FROM secret)")
            .await
            .expect("uncorrelated EXISTS over RLS table"),
    );
    assert!(
        leak.is_empty(),
        "uncorrelated EXISTS leaked RLS-hidden rows: driver row returned ({leak:?})"
    );

    // Each probe: the driver row appears iff the embedded subquery's truth
    // value reflects an EMPTY/zero `secret` (i.e. RLS applied). With RLS the
    // outer row is kept for `NOT EXISTS` / `NOT IN` / `count = 0`, and dropped
    // for `EXISTS` / `IN` / `= ANY`. A leak would flip these.
    let kept_when_inner_empty = [
        "SELECT id FROM driver WHERE NOT EXISTS (SELECT 1 FROM secret)",
        "SELECT id FROM driver WHERE 10 NOT IN (SELECT val FROM secret)",
        "SELECT id FROM driver WHERE (SELECT count(*) FROM secret) = 0",
        // scalar subquery returning no row -> NULL -> coalesce to 0
        "SELECT id FROM driver WHERE coalesce((SELECT val FROM secret LIMIT 1), 0) = 0",
        // nested: subquery inside a subquery, both over the RLS table
        "SELECT id FROM driver WHERE NOT EXISTS \
            (SELECT 1 FROM secret WHERE val IN (SELECT val FROM secret))",
        // EXISTS inside a CTE body
        "WITH d AS (SELECT id FROM driver) \
            SELECT id FROM d WHERE NOT EXISTS (SELECT 1 FROM secret)",
        // subquery in a HAVING-style aggregate over driver
        "SELECT id FROM driver GROUP BY id \
            HAVING (SELECT count(*) FROM secret) = 0",
    ];
    for sql in kept_when_inner_empty {
        let rows = simple_rows(&client.simple_query(sql).await.expect(sql));
        assert_eq!(
            rows,
            vec![vec!["1".to_owned()]],
            "RLS-applied subquery should keep the driver row (inner empty): {sql} -> {rows:?}"
        );
    }

    let dropped_when_inner_empty = [
        "SELECT id FROM driver WHERE EXISTS (SELECT 1 FROM secret)",
        "SELECT id FROM driver WHERE 10 IN (SELECT val FROM secret)",
        "SELECT id FROM driver WHERE (SELECT count(*) FROM secret) > 0",
        "SELECT id FROM driver WHERE 10 = ANY (SELECT val FROM secret)",
        "WITH d AS (SELECT id FROM driver) \
            SELECT id FROM d WHERE EXISTS (SELECT 1 FROM secret)",
    ];
    for sql in dropped_when_inner_empty {
        let rows = simple_rows(&client.simple_query(sql).await.expect(sql));
        assert!(
            rows.is_empty(),
            "embedded subquery leaked an RLS-hidden row: {sql} -> {rows:?}"
        );
    }

    // ── Matching tenant: the same shapes now reflect the visible row
    // (tenant-a owns `val = 10`), proving the fix does not over-filter.
    client
        .batch_execute("SET ultrasql.tenant_id = 'tenant-a'")
        .await
        .expect("set matching tenant guc");

    for sql in [
        "SELECT id FROM driver WHERE EXISTS (SELECT 1 FROM secret)",
        "SELECT id FROM driver WHERE 10 IN (SELECT val FROM secret)",
        "SELECT id FROM driver WHERE (SELECT count(*) FROM secret) = 1",
        "SELECT id FROM driver WHERE 10 = ANY (SELECT val FROM secret)",
    ] {
        let rows = simple_rows(&client.simple_query(sql).await.expect(sql));
        assert_eq!(
            rows,
            vec![vec!["1".to_owned()]],
            "matching tenant should see its row through the subquery: {sql} -> {rows:?}"
        );
    }
    // tenant-a does NOT own val = 20, so an IN over 20 misses.
    let rows = simple_rows(
        &client
            .simple_query("SELECT id FROM driver WHERE 20 IN (SELECT val FROM secret)")
            .await
            .expect("IN over a non-owned value"),
    );
    assert!(
        rows.is_empty(),
        "matching tenant must not match another tenant's value: {rows:?}"
    );

    shutdown(client, server_handle).await;
}

/// Correlated EXISTS/IN are decorrelated to a semi/anti join BEFORE RLS runs,
/// so they carry no embedded subplan at the walker stage and are filtered
/// purely through the join's scanned input. This guards the no-regression /
/// no-double-filter property: the join-side scan still gets exactly one policy
/// filter.
#[tokio::test]
async fn rls_correlated_subqueries_still_enforce_without_double_filter() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    for sql in [
        "CREATE TABLE corr_driver (id INT NOT NULL, tenant_id TEXT NOT NULL)",
        "INSERT INTO corr_driver VALUES (1, 'tenant-a'), (2, 'tenant-b')",
        "CREATE TABLE corr_secret (owner INT NOT NULL, tenant_id TEXT NOT NULL)",
        "INSERT INTO corr_secret VALUES (1, 'tenant-a'), (2, 'tenant-b')",
        "CREATE POLICY corr_secret_isolation ON corr_secret \
            USING (tenant_id = current_setting('ultrasql.tenant_id', true))",
        "ALTER TABLE corr_secret ENABLE ROW LEVEL SECURITY",
        "SET ultrasql.tenant_id = 'tenant-a'",
    ] {
        client.batch_execute(sql).await.expect(sql);
    }

    // Correlated EXISTS: only corr_secret rows that pass RLS (tenant-a) can
    // satisfy the correlation. corr_driver id=1 correlates to owner=1
    // (tenant-a, visible); id=2 correlates to owner=2 (tenant-b, hidden).
    let rows = simple_rows(
        &client
            .simple_query(
                "SELECT id FROM corr_driver d \
                 WHERE EXISTS (SELECT 1 FROM corr_secret s WHERE s.owner = d.id) \
                 ORDER BY id",
            )
            .await
            .expect("correlated EXISTS respects RLS"),
    );
    assert_eq!(
        rows,
        vec![vec!["1".to_owned()]],
        "correlated EXISTS must keep only the row whose RLS-visible match exists"
    );

    // Correlated IN: same expectation via a different decorrelated shape.
    let rows = simple_rows(
        &client
            .simple_query(
                "SELECT id FROM corr_driver d \
                 WHERE d.id IN (SELECT owner FROM corr_secret s WHERE s.tenant_id = d.tenant_id) \
                 ORDER BY id",
            )
            .await
            .expect("correlated IN respects RLS"),
    );
    assert_eq!(
        rows,
        vec![vec!["1".to_owned()]],
        "correlated IN must reflect only RLS-visible inner rows (no double-filter dropping it)"
    );

    shutdown(client, server_handle).await;
}

/// No-regression for non-RLS tables: every embedded-subplan shape returns the
/// correct result when no policy is in play, so the new recursion does not
/// alter ordinary subquery semantics.
#[tokio::test]
async fn non_rls_embedded_subqueries_unaffected() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    for sql in [
        "CREATE TABLE plain_driver (id INT NOT NULL)",
        "INSERT INTO plain_driver VALUES (1), (2), (3)",
        "CREATE TABLE plain_lookup (val INT NOT NULL)",
        "INSERT INTO plain_lookup VALUES (2), (3)",
    ] {
        client.batch_execute(sql).await.expect(sql);
    }

    let rows = simple_rows(
        &client
            .simple_query(
                "SELECT id FROM plain_driver \
                 WHERE id IN (SELECT val FROM plain_lookup) ORDER BY id",
            )
            .await
            .expect("uncorrelated IN over non-RLS table"),
    );
    assert_eq!(rows, vec![vec!["2".to_owned()], vec!["3".to_owned()]]);

    let rows = simple_rows(
        &client
            .simple_query(
                "SELECT id FROM plain_driver \
                 WHERE EXISTS (SELECT 1 FROM plain_lookup) ORDER BY id",
            )
            .await
            .expect("uncorrelated EXISTS over non-RLS table"),
    );
    assert_eq!(
        rows,
        vec![
            vec!["1".to_owned()],
            vec!["2".to_owned()],
            vec!["3".to_owned()]
        ],
        "non-RLS EXISTS must keep all rows (lookup is non-empty)"
    );

    // Scalar subquery in a projection over a 1-row driver (the uncorrelated
    // shape decorrelation supports — a bare no-FROM `SELECT (subquery)` is a
    // separate, pre-existing limitation and not exercised here).
    let rows = simple_rows(
        &client
            .simple_query(
                "SELECT (SELECT count(*) FROM plain_lookup) FROM plain_driver WHERE id = 1",
            )
            .await
            .expect("scalar subquery over non-RLS table"),
    );
    assert_eq!(rows, vec![vec!["2".to_owned()]]);

    shutdown(client, server_handle).await;
}

/// Defense-in-depth: a subquery embedded in a MERGE clause expression (here
/// the `ON` predicate) that probes an RLS-protected table must fail closed.
///
/// Unlike a SELECT/Filter position, a subquery in a MERGE `ON` / `WHEN`
/// condition / INSERT `VALUES` is NOT decorrelated to a join, so it survives
/// to the executor, which rejects it via the eval backstop rather than
/// evaluating it with RLS bypassed. The RLS and SSI walkers do descend into
/// these clause positions (so any embedded subplan is policy-rewritten and
/// takes a predicate lock), but the masking guarantee today is the executor's
/// fail-closed refusal. This test pins that guarantee: if a future change
/// gives MERGE embedded-subquery *execution* and this MERGE starts to succeed,
/// the test trips — forcing a conscious check that RLS is enforced on the
/// executed subquery instead of silently reopening the bypass.
#[tokio::test]
async fn rls_merge_embedded_subquery_fails_closed() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    for sql in [
        "CREATE TABLE merge_target (id INT PRIMARY KEY, v INT NOT NULL)",
        "CREATE TABLE merge_source (id INT NOT NULL, v INT NOT NULL)",
        "INSERT INTO merge_target VALUES (1, 10)",
        "INSERT INTO merge_source VALUES (1, 100)",
        // RLS-protected table the MERGE clause subquery probes.
        "CREATE TABLE secret (tenant_id TEXT NOT NULL, val INT NOT NULL)",
        "INSERT INTO secret VALUES ('tenant-a', 10), ('tenant-b', 20)",
        "CREATE POLICY secret_isolation ON secret \
            USING (tenant_id = current_setting('ultrasql.tenant_id', true))",
        "ALTER TABLE secret ENABLE ROW LEVEL SECURITY",
        // A tenant that owns no `secret` row, so a leak would be observable.
        "SET ultrasql.tenant_id = 'nobody'",
    ] {
        client.batch_execute(sql).await.expect(sql);
    }

    let err = client
        .batch_execute(
            "MERGE INTO merge_target AS t \
             USING merge_source AS s \
             ON t.id = s.id AND EXISTS (SELECT 1 FROM secret) \
             WHEN MATCHED THEN UPDATE SET v = s.v",
        )
        .await
        .expect_err("MERGE with an embedded subquery must fail closed, not leak RLS-hidden rows");
    assert!(
        err.as_db_error()
            .is_some_and(|db| db.message().contains("subquery")),
        "expected the executor's subquery backstop to refuse the MERGE: {err}"
    );

    // Fail-closed means no partial effect: the target row is untouched.
    let rows = simple_rows(
        &client
            .simple_query("SELECT v FROM merge_target WHERE id = 1")
            .await
            .expect("read merge_target after fail-closed MERGE"),
    );
    assert_eq!(
        rows,
        vec![vec!["10".to_owned()]],
        "fail-closed MERGE must not have applied any update"
    );

    shutdown(client, server_handle).await;
}
