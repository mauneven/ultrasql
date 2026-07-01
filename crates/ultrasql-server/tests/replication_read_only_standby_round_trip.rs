//! Hot-standby read-only enforcement (replication Phase 3): a server in standby
//! mode rejects every write — via the simple, extended, and DDL paths — with
//! SQLSTATE 25006 (`read_only_sql_transaction`), while continuing to serve reads.

pub mod support;

use support::{shutdown, start_persistent_server};

fn sqlstate(err: &tokio_postgres::Error) -> String {
    err.code()
        .expect("server-sent SQLSTATE present")
        .code()
        .to_owned()
}

#[tokio::test]
async fn hot_standby_rejects_writes_with_25006_and_allows_reads() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    let running = start_persistent_server(data_dir.path(), "read_only_standby").await;

    // Seed data while still a primary.
    running
        .client
        .batch_execute("CREATE TABLE ro_t (id INT NOT NULL); INSERT INTO ro_t VALUES (1), (2);")
        .await
        .expect("primary setup");

    // Promote to a read-only standby.
    running.server.set_standby_mode(true);

    // Reads still work (extended-protocol query()).
    let rows = running
        .client
        .query("SELECT id FROM ro_t ORDER BY id", &[])
        .await
        .expect("SELECT is allowed on a standby");
    assert_eq!(rows.len(), 2);

    // Simple-query write → 25006 (the execute_query text-level gate).
    let err = running
        .client
        .batch_execute("INSERT INTO ro_t VALUES (3)")
        .await
        .expect_err("simple INSERT must be rejected on a standby");
    assert_eq!(sqlstate(&err), "25006", "simple INSERT: {err}");

    // Extended-protocol (parameterized) write → 25006. This path bypasses the
    // text-level gate, so it is enforced in `run_portal_routed`.
    let err = running
        .client
        .execute("INSERT INTO ro_t VALUES ($1)", &[&4_i32])
        .await
        .expect_err("extended INSERT must be rejected on a standby");
    assert_eq!(sqlstate(&err), "25006", "extended INSERT: {err}");

    // DDL → 25006.
    let err = running
        .client
        .batch_execute("CREATE TABLE ro_t2 (id INT)")
        .await
        .expect_err("DDL must be rejected on a standby");
    assert_eq!(sqlstate(&err), "25006", "DDL: {err}");

    // Nothing leaked through: the table still has exactly the two seeded rows.
    let rows = running
        .client
        .query("SELECT id FROM ro_t ORDER BY id", &[])
        .await
        .expect("SELECT still works");
    assert_eq!(rows.len(), 2, "no write leaked past the read-only gate");

    shutdown(running).await;
}
