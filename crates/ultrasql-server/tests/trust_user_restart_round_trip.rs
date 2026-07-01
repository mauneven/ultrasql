//! Restart round-trip for objects owned by a trust-authenticated username
//! that has no catalog role entry.
//!
//! Trust auth accepts any username, including one never created with
//! `CREATE ROLE`. Such a session can own tables (RLS sidecar owner), schemas,
//! and sequences, and appear as a privilege grantor. A restart must boot and
//! serve that data: refusing to start over a recorded owner bricks the data
//! directory.

pub mod support;

use support::{connect_as, shutdown as graceful_shutdown, start_persistent_server};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trust_user_owned_objects_survive_restart() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    support::make_data_dir_private(data_dir.path());

    let running = start_persistent_server(data_dir.path(), "trust_user_restart_setup").await;
    let (client, conn_handle) =
        connect_as(running.bound, "someuser", "trust_user_restart_someuser").await;
    // Everything below records `someuser` (a username with no catalog role)
    // in a runtime metadata sidecar: RLS table owner, schema owner, sequence
    // owner, and privilege grantor.
    for sql in [
        "CREATE TABLE rt (id INT)",
        "INSERT INTO rt VALUES (1)",
        "CREATE TABLE rt_rls (tenant_id TEXT NOT NULL, doc_id TEXT NOT NULL)",
        "CREATE POLICY rt_rls_tenant ON rt_rls \
            USING (tenant_id = current_setting('ultrasql.tenant_id', true))",
        "ALTER TABLE rt_rls ENABLE ROW LEVEL SECURITY",
        "CREATE SCHEMA rt_app",
        "CREATE SEQUENCE rt_seq",
        "GRANT SELECT ON TABLE rt TO PUBLIC",
    ] {
        client.batch_execute(sql).await.expect(sql);
    }
    drop(client);
    conn_handle.await.expect("someuser connection task joins");
    graceful_shutdown(running).await;

    // Taking ownership must have durably materialized the implicit role, so
    // the restart below can resolve every recorded owner.
    let auth_meta = std::fs::read_to_string(data_dir.path().join("pg_auth.meta"))
        .expect("role metadata exists after trust-user DDL");
    assert!(
        auth_meta.contains("someuser"),
        "trust-user owner must be recorded as a role: {auth_meta}"
    );

    // The restart bricked the data directory before the fix:
    // "DDL failed: unknown RLS table metadata owner 'someuser' on line 2".
    let running = start_persistent_server(data_dir.path(), "trust_user_restart_verify").await;
    let (client, conn_handle) = connect_as(
        running.bound,
        "someuser",
        "trust_user_restart_verify_someuser",
    )
    .await;
    let count: i64 = client
        .query_one("SELECT count(*) FROM rt", &[])
        .await
        .expect("owned table is queryable after restart")
        .get(0);
    assert_eq!(count, 1);
    client
        .batch_execute("INSERT INTO rt VALUES (2)")
        .await
        .expect("owned table accepts writes after restart");
    client
        .batch_execute("INSERT INTO rt_rls VALUES ('t1', 'd1')")
        .await
        .expect("RLS-enabled owned table accepts owner writes after restart");
    let role_rows = client
        .query(
            "SELECT rolname FROM pg_catalog.pg_roles WHERE rolname = 'someuser'",
            &[],
        )
        .await
        .expect("query pg_roles after restart");
    assert_eq!(role_rows.len(), 1, "implicit role is visible in pg_roles");
    drop(client);
    conn_handle.await.expect("someuser connection task joins");
    graceful_shutdown(running).await;
}
