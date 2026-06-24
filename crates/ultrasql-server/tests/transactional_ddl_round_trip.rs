//! Adversarial battery for transactional `CREATE TABLE` (transactional-DDL
//! milestones 1 and 2).
//!
//! A rolled-back `CREATE TABLE` whose catalog row or snapshot entry survives is
//! silent schema corruption — the exact class that got SAVEPOINT reverted once.
//! This battery is the gate: tests #1 (ROLLBACK in-memory), #2 (second-connection
//! isolation), and #4 (crash mid-transaction) are the corruption cases and must
//! pass for both the simple-query and the extended/portal path.
//!
//! Milestone 2 (the `M2 #*` tests at the end) adds in-txn `PRIMARY KEY / UNIQUE`
//! by deferring the implicit constraint-index B-tree build to COMMIT: M2 #2
//! (no segment on ROLLBACK), M2 #3 (crash before commit), and M2 #5 (a duplicate
//! key at the COMMIT build fails 23505 with a FULL rollback) are the new
//! corruption gates.

pub mod support;

use bytes::Bytes;
use futures::SinkExt;
use support::{connect_as, make_data_dir_private, shutdown, start_persistent_server};
use tokio_postgres::types::Type;

/// Whether a `tokio_postgres` error is an "undefined table" (42P01).
fn is_undefined_table(err: &tokio_postgres::Error) -> bool {
    err.code().map(|c| c.code() == "42P01").unwrap_or(false)
}

/// SQLSTATE carried by a wire error, if any.
fn sqlstate(err: &tokio_postgres::Error) -> String {
    err.code()
        .map_or_else(String::new, |c| c.code().to_string())
}

// ───────────────────────────── Battery #1 ─────────────────────────────
// ROLLBACK undoes the in-txn CREATE TABLE — in-memory, same session.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rollback_in_txn_create_table_is_invisible_to_self() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_rollback").await;
    let client = &running.client;

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("CREATE TABLE rb_t (id INT NOT NULL)")
        .await
        .expect("in-txn create table is accepted");
    // Self-visible before commit (battery #3 in miniature).
    client
        .query("SELECT id FROM rb_t", &[])
        .await
        .expect("self sees the table before commit");
    client.batch_execute("ROLLBACK").await.expect("rollback");

    // Same session, fresh statement: the table is gone.
    let err = client
        .query("SELECT id FROM rb_t", &[])
        .await
        .expect_err("rolled-back table must be invisible to self after rollback");
    assert!(is_undefined_table(&err), "expected 42P01, got {err}");

    // And the global committed snapshot has no entry for it.
    assert!(
        !running
            .server
            .catalog_snapshot()
            .tables
            .contains_key("rb_t"),
        "global snapshot must not carry a rolled-back table",
    );

    // The same name can now be created and committed cleanly.
    client.batch_execute("BEGIN").await.expect("begin 2");
    client
        .batch_execute("CREATE TABLE rb_t (id INT NOT NULL)")
        .await
        .expect("recreate after rollback");
    client.batch_execute("COMMIT").await.expect("commit");
    client
        .query("SELECT id FROM rb_t", &[])
        .await
        .expect("committed table is visible");

    shutdown(running).await;
}

// ───────────────────────────── Battery #2 ─────────────────────────────
// ROLLBACK / COMMIT and the second-connection isolation contract.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uncommitted_in_txn_create_table_is_invisible_to_other_connection() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_iso_a").await;
    let (client_b, b_handle) = connect_as(running.bound, "tester", "txddl_iso_b").await;

    // A opens a transaction and creates the table, but does NOT commit.
    running
        .client
        .batch_execute("BEGIN")
        .await
        .expect("A begin");
    running
        .client
        .batch_execute("CREATE TABLE iso_t (id INT NOT NULL)")
        .await
        .expect("A in-txn create");

    // B must NOT see the uncommitted table (others-no isolation).
    let err = client_b
        .query("SELECT id FROM iso_t", &[])
        .await
        .expect_err("B must not see A's uncommitted table");
    assert!(is_undefined_table(&err), "expected 42P01 for B, got {err}");

    // A rolls back; B still does not see it.
    running
        .client
        .batch_execute("ROLLBACK")
        .await
        .expect("A rollback");
    let err = client_b
        .query("SELECT id FROM iso_t", &[])
        .await
        .expect_err("B must not see a rolled-back table");
    assert!(
        is_undefined_table(&err),
        "expected 42P01 after rollback, got {err}"
    );

    drop(client_b);
    let _ = b_handle.await;
    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_in_txn_create_table_becomes_visible_to_other_connection() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_iso2_a").await;
    let (client_b, b_handle) = connect_as(running.bound, "tester", "txddl_iso2_b").await;

    running
        .client
        .batch_execute("BEGIN")
        .await
        .expect("A begin");
    running
        .client
        .batch_execute("CREATE TABLE iso2_t (id INT NOT NULL)")
        .await
        .expect("A in-txn create");

    // Before COMMIT, B cannot see it.
    let err = client_b
        .query("SELECT id FROM iso2_t", &[])
        .await
        .expect_err("B blind before A commits");
    assert!(is_undefined_table(&err), "expected 42P01, got {err}");

    running
        .client
        .batch_execute("COMMIT")
        .await
        .expect("A commit");

    // After COMMIT, B sees it.
    client_b
        .query("SELECT id FROM iso2_t", &[])
        .await
        .expect("B sees the table after A commits");

    drop(client_b);
    let _ = b_handle.await;
    shutdown(running).await;
}

// ───────────────────────────── Battery #3 ─────────────────────────────
// Self-visible before commit: CREATE + INSERT + SELECT in one txn, committed
// together.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_txn_create_insert_select_commit_together() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_self").await;
    let client = &running.client;

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("CREATE TABLE self_t (id INT NOT NULL, v TEXT)")
        .await
        .expect("in-txn create");
    client
        .batch_execute("INSERT INTO self_t VALUES (1, 'a'), (2, 'b')")
        .await
        .expect("in-txn insert into self-created table");
    let rows = client
        .query("SELECT id, v FROM self_t ORDER BY id", &[])
        .await
        .expect("in-txn select from self-created table");
    assert_eq!(rows.len(), 2, "both rows visible to self before commit");
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[1].get::<_, &str>(1), "b");

    client.batch_execute("COMMIT").await.expect("commit");

    // Table + rows commit together; visible to a fresh connection.
    let (client_b, b_handle) = connect_as(running.bound, "tester", "txddl_self_b").await;
    let rows = client_b
        .query("SELECT count(*) FROM self_t", &[])
        .await
        .expect("fresh connection sees committed table");
    assert_eq!(rows[0].get::<_, i64>(0), 2, "rows committed with the table");

    drop(client_b);
    let _ = b_handle.await;
    shutdown(running).await;
}

// ───────────────────────────── Battery #4 ─────────────────────────────
// Crash mid-transaction after DDL, before COMMIT: the table must NOT resurrect
// on restart. Symmetric: after COMMIT it must be present after restart.
//
// "Crash" is modelled by dropping the server while the transaction is still
// open (no COMMIT/ROLLBACK marker on disk for the user xid). `Server::drop`
// flushes the durable heap pages — so the catalog rows ARE on disk under the
// uncommitted user xid — and the reopened server runs WAL recovery + the
// visibility-filtered catalog bootstrap, which must hide them.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_after_in_txn_create_before_commit_does_not_resurrect_table() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "txddl_crash_a").await;
        running.client.batch_execute("BEGIN").await.expect("begin");
        running
            .client
            .batch_execute("CREATE TABLE crash_t (id INT NOT NULL)")
            .await
            .expect("in-txn create (durable rows under user xid, NO commit)");
        // Drop the client and server WITHOUT COMMIT/ROLLBACK — the user xid
        // has no commit record; the durable heap rows are flushed by
        // `Server::drop`.
        shutdown(running).await;
    }

    // Restart: recovery must NOT resurrect the uncommitted table.
    let running = start_persistent_server(data_dir.path(), "txddl_crash_a2").await;
    assert!(
        !running
            .server
            .catalog_snapshot()
            .tables
            .contains_key("crash_t"),
        "crash-before-commit table must not resurrect in the catalog snapshot",
    );
    let err = running
        .client
        .query("SELECT id FROM crash_t", &[])
        .await
        .expect_err("crash-before-commit table must be absent after restart");
    assert!(
        is_undefined_table(&err),
        "expected 42P01 after restart, got {err}"
    );
    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_in_txn_create_survives_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "txddl_survive_a").await;
        running.client.batch_execute("BEGIN").await.expect("begin");
        running
            .client
            .batch_execute("CREATE TABLE survive_t (id INT NOT NULL)")
            .await
            .expect("in-txn create");
        running
            .client
            .batch_execute("INSERT INTO survive_t VALUES (42)")
            .await
            .expect("in-txn insert");
        running
            .client
            .batch_execute("COMMIT")
            .await
            .expect("commit");
        shutdown(running).await;
    }

    let running = start_persistent_server(data_dir.path(), "txddl_survive_a2").await;
    let rows = running
        .client
        .query("SELECT id FROM survive_t", &[])
        .await
        .expect("committed in-txn table present after restart");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get::<_, i32>(0),
        42,
        "committed row survives restart"
    );
    shutdown(running).await;
}

// ───────────────────────────── Battery #5 ─────────────────────────────
// Concurrent CREATE TABLE of the same name: the per-name AccessExclusive lock
// serializes the two transactions so two `pg_class` rows for the same relation
// can never both reach durable commit (which would be a duplicate-name
// corruption on restart). The engine's lock discipline is non-blocking
// (`try_acquire`, never parking a tokio worker on a cross-transaction lock),
// so while A holds the name lock B's same-name CREATE fails immediately with a
// retryable serialization error (40001) instead of blocking. After A commits
// and releases the lock, B can create — but now sees A's committed table and
// fails with already-exists (42P07). Either way: no torn state, exactly one
// table.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_in_txn_create_same_name_serializes_cleanly() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_race_a").await;
    let (client_b, b_handle) = connect_as(running.bound, "tester", "txddl_race_b").await;

    // A opens a transaction and creates the table, taking the name lock.
    running
        .client
        .batch_execute("BEGIN")
        .await
        .expect("A begin");
    running
        .client
        .batch_execute("CREATE TABLE race_t (id INT NOT NULL)")
        .await
        .expect("A in-txn create takes the name lock");

    // B, concurrently, tries to create the SAME table while A still holds the
    // lock. Non-blocking: B fails immediately with a serialization error.
    client_b.batch_execute("BEGIN").await.expect("B begin");
    let err = client_b
        .batch_execute("CREATE TABLE race_t (id INT NOT NULL)")
        .await
        .expect_err("B's same-name CREATE must fail while A holds the name lock");
    assert_eq!(
        sqlstate(&err),
        "40001",
        "concurrent same-name CREATE must report serialization_failure, got {err}"
    );
    client_b
        .batch_execute("ROLLBACK")
        .await
        .expect("B rollback");

    // A commits and wins.
    running
        .client
        .batch_execute("COMMIT")
        .await
        .expect("A commit wins");

    // Now B retries: the table is committed, so B fails with already-exists.
    client_b.batch_execute("BEGIN").await.expect("B begin 2");
    let err = client_b
        .batch_execute("CREATE TABLE race_t (id INT NOT NULL)")
        .await
        .expect_err("B's retry must see A's committed table");
    assert_eq!(
        sqlstate(&err),
        "42P07",
        "retry after winner committed must report duplicate_table, got {err}"
    );
    client_b
        .batch_execute("ROLLBACK")
        .await
        .expect("B rollback 2");

    // Exactly one table exists and is usable.
    running
        .client
        .batch_execute("INSERT INTO race_t VALUES (1)")
        .await
        .expect("the single committed table is usable");
    let rows = running
        .client
        .query("SELECT count(*) FROM race_t", &[])
        .await
        .expect("query the winner");
    assert_eq!(rows[0].get::<_, i64>(0), 1);

    drop(client_b);
    let _ = b_handle.await;
    shutdown(running).await;
}

// ───────────────────────────── Battery #6 ─────────────────────────────
// No orphaned files after ROLLBACK of CREATE TABLE: lazy segment creation
// means no `base/<oid>` segment should exist for the rolled-back relation.

/// Segment files for any user relation (OID ≥ 16384) under `base/`. System
/// catalog heap segments (pg_class = 1259, pg_attribute = 1249, …) are durable
/// and expected; only a *user* relation's segment would be an orphan.
fn user_relation_segments(data_dir: &std::path::Path) -> Vec<String> {
    const FIRST_USER_OID: u32 = 16_384;
    let mut names: Vec<String> = std::fs::read_dir(data_dir.join("base"))
        .map(|rd| {
            rd.filter_map(Result::ok)
                .filter_map(|e| e.file_name().into_string().ok())
                .filter(|name| {
                    // Segment file names start with the relation OID; keep only
                    // those whose leading numeric component is a user OID.
                    name.split(|c: char| !c.is_ascii_digit())
                        .next()
                        .and_then(|n| n.parse::<u32>().ok())
                        .is_some_and(|oid| oid >= FIRST_USER_OID)
                })
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rollback_in_txn_create_table_leaves_no_segment_file() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());
    let running = start_persistent_server(data_dir.path(), "txddl_noorphan").await;

    running.client.batch_execute("BEGIN").await.expect("begin");
    running
        .client
        .batch_execute("CREATE TABLE orphan_t (id INT NOT NULL)")
        .await
        .expect("in-txn create");
    running
        .client
        .batch_execute("ROLLBACK")
        .await
        .expect("rollback");

    // No user-relation segment file: the table was never materialized (lazy
    // creation) and its catalog rows are MVCC-invisible. Only system catalog
    // heap segments (durable, OID < 16384) may exist under base/.
    assert_eq!(
        user_relation_segments(data_dir.path()),
        Vec::<String>::new(),
        "ROLLBACK of CREATE TABLE must not leave a base/<user-oid> segment (lazy creation)",
    );

    shutdown(running).await;
}

// ───────────────────────────── Battery #7 ─────────────────────────────
// Regression guard: every out-of-scope DDL still rejects in-txn with 0A000 and
// transitions the block to Failed (25P02). The relaxed gate must only open
// CREATE TABLE.

async fn assert_ddl_rejected_in_txn(application_name: &str, setup: &[&str], ddl: &str) {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), application_name).await;
    let client = &running.client;
    for stmt in setup {
        client.batch_execute(stmt).await.expect("setup");
    }

    client.batch_execute("BEGIN").await.expect("begin");
    let err = client
        .batch_execute(ddl)
        .await
        .expect_err("out-of-scope DDL must be rejected in a transaction");
    assert_eq!(
        sqlstate(&err),
        "0A000",
        "`{ddl}` in-txn must be feature_not_supported, got {err}"
    );

    // The block is now Failed: a subsequent statement gets 25P02.
    let in_failed = client
        .batch_execute("SELECT 1")
        .await
        .expect_err("statement after rejected DDL must be 25P02");
    assert_eq!(
        sqlstate(&in_failed),
        "25P02",
        "in-failed-block after `{ddl}` must be in_failed_sql_transaction, got {in_failed}"
    );
    client.batch_execute("ROLLBACK").await.expect("rollback");

    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn out_of_scope_ddl_still_rejected_in_transaction() {
    assert_ddl_rejected_in_txn(
        "txddl_reg_drop",
        &["CREATE TABLE reg_drop (id INT)"],
        "DROP TABLE reg_drop",
    )
    .await;
    assert_ddl_rejected_in_txn(
        "txddl_reg_index",
        &["CREATE TABLE reg_idx (id INT)"],
        "CREATE INDEX reg_idx_ix ON reg_idx (id)",
    )
    .await;
    assert_ddl_rejected_in_txn(
        "txddl_reg_alter",
        &["CREATE TABLE reg_alter (id INT)"],
        "ALTER TABLE reg_alter ADD COLUMN v INT",
    )
    .await;
    assert_ddl_rejected_in_txn("txddl_reg_grant", &[], "CREATE ROLE reg_role").await;
    assert_ddl_rejected_in_txn(
        "txddl_reg_comment",
        &["CREATE TABLE reg_comment (id INT)"],
        "COMMENT ON TABLE reg_comment IS 'x'",
    )
    .await;
    assert_ddl_rejected_in_txn("txddl_reg_checkpoint", &[], "CHECKPOINT").await;
    // Serial-bearing CREATE TABLE is out of scope for milestone 1 (non-MVCC
    // sequence-create WAL would resurrect on restart) and must still reject.
    assert_ddl_rejected_in_txn(
        "txddl_reg_serial",
        &[],
        "CREATE TABLE reg_serial (id SERIAL)",
    )
    .await;
}

/// CREATE TABLE while a SAVEPOINT is active is out of scope for milestone 1:
/// the durable rows ride the parent xid and the overlay is whole-transaction
/// scoped, so a `ROLLBACK TO SAVEPOINT` could not undo the table. It must be
/// rejected (0A000), and a CREATE TABLE BEFORE any savepoint stays supported.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_table_with_active_savepoint_is_rejected() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_sp").await;
    let client = &running.client;

    // A CREATE TABLE before any savepoint commits fine.
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("CREATE TABLE sp_before (id INT)")
        .await
        .expect("create before savepoint is supported");
    client
        .batch_execute("SAVEPOINT s1")
        .await
        .expect("savepoint");
    // A CREATE TABLE with the savepoint active is rejected.
    let err = client
        .batch_execute("CREATE TABLE sp_after (id INT)")
        .await
        .expect_err("create with active savepoint must be rejected");
    assert_eq!(
        sqlstate(&err),
        "0A000",
        "savepoint-active CREATE TABLE must be feature_not_supported, got {err}"
    );
    client.batch_execute("ROLLBACK").await.expect("rollback");

    // The whole transaction rolled back: neither table exists.
    for name in ["sp_before", "sp_after"] {
        let err = client
            .query(&format!("SELECT id FROM {name}"), &[])
            .await
            .expect_err("rolled-back tables absent");
        assert!(
            is_undefined_table(&err),
            "expected 42P01 for {name}, got {err}"
        );
    }

    shutdown(running).await;
}

// ────────────────────── Extended/portal path ──────────────────────
// The whole battery must also hold on the extended-query path. A prepared
// `CREATE TABLE` executed inside a transaction, then rolled back, must be
// invisible; committed, it must be visible — driven through Parse/Bind/Execute.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extended_path_in_txn_create_rollback_and_commit() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_ext").await;
    let client = &running.client;

    // ROLLBACK case via the extended path (`client.execute` issues
    // Parse/Bind/Execute).
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .execute("CREATE TABLE ext_t (id INT NOT NULL)", &[])
        .await
        .expect("extended in-txn create");
    // Self-visible: a prepared SELECT inside the txn resolves the table.
    let select = client
        .prepare_typed("SELECT id FROM ext_t", &[])
        .await
        .expect("prepare select against self-created table");
    let rows = client
        .query(&select, &[])
        .await
        .expect("extended self-select");
    assert_eq!(rows.len(), 0, "empty self-created table, no rows yet");
    client.batch_execute("ROLLBACK").await.expect("rollback");
    let err = client
        .prepare_typed("SELECT id FROM ext_t", &[])
        .await
        .expect_err("prepared statement must not resolve a rolled-back table");
    assert!(is_undefined_table(&err), "expected 42P01, got {err}");

    // COMMIT case.
    client.batch_execute("BEGIN").await.expect("begin 2");
    client
        .execute("CREATE TABLE ext_t (id INT NOT NULL)", &[])
        .await
        .expect("extended create in txn 2");
    client
        .execute("INSERT INTO ext_t VALUES ($1)", &[&7_i32])
        .await
        .expect("parametrized insert into self-created table");
    client.batch_execute("COMMIT").await.expect("commit 2");

    let select = client
        .prepare_typed("SELECT id FROM ext_t", &[])
        .await
        .expect("prepare after commit");
    let rows = client
        .query(&select, &[])
        .await
        .expect("query committed table");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 7);
    // The column type round-trips (sanity that the catalog entry is real).
    assert_eq!(select.columns()[0].type_(), &Type::INT4);

    shutdown(running).await;
}

// ───────────────────────────── Battery #8 ─────────────────────────────
// Two-phase commit of a transaction carrying an in-txn CREATE TABLE is out of
// scope for milestone 1 and MUST be rejected at PREPARE TRANSACTION (0A000,
// block Failed) — never silently dropped or committed.
//
// Pre-fix this was corruption-class: PREPARE terminated the txn and handed the
// xid to the 2PC coordinator WITHOUT publishing the overlay, so a subsequent
// COMMIT PREPARED durably committed the catalog rows while losing the table in
// the live process, and the name lock (released at PREPARE) let a second
// same-name CREATE commit too → duplicate-name pg_class corruption.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_transaction_with_in_txn_create_table_is_rejected() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_2pc_reject").await;
    let client = &running.client;

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("CREATE TABLE tpc_t (id INT NOT NULL)")
        .await
        .expect("in-txn create");
    // PREPARE TRANSACTION must reject (transactional DDL cannot be
    // two-phase-committed yet).
    let err = client
        .batch_execute("PREPARE TRANSACTION 'txddl-gid'")
        .await
        .expect_err("PREPARE of a txn carrying CREATE TABLE must be rejected");
    assert_eq!(
        sqlstate(&err),
        "0A000",
        "PREPARE TRANSACTION with in-txn CREATE TABLE must be feature_not_supported, got {err}"
    );

    // The block is now Failed: a subsequent statement gets 25P02.
    let in_failed = client
        .batch_execute("SELECT 1")
        .await
        .expect_err("statement after rejected PREPARE must be 25P02");
    assert_eq!(
        sqlstate(&in_failed),
        "25P02",
        "in-failed-block after rejected PREPARE must be in_failed_sql_transaction, got {in_failed}"
    );

    // No prepared transaction was created, so COMMIT PREPARED finds nothing —
    // the durable-commit-of-a-lost-table corruption path is unreachable.
    let commit_prepared = client
        .batch_execute("COMMIT PREPARED 'txddl-gid'")
        .await
        .expect_err("no prepared transaction named 'txddl-gid' can exist");
    assert_ne!(
        sqlstate(&commit_prepared),
        "00000",
        "COMMIT PREPARED of a never-created gid must error, got success"
    );

    client.batch_execute("ROLLBACK").await.expect("rollback");

    // The rolled-back table is gone everywhere: self, the global snapshot, and
    // a durable pg_class probe (exactly zero rows — no corruption leak).
    let err = client
        .query("SELECT id FROM tpc_t", &[])
        .await
        .expect_err("rejected-PREPARE table must be invisible to self");
    assert!(is_undefined_table(&err), "expected 42P01, got {err}");
    assert!(
        !running
            .server
            .catalog_snapshot()
            .tables
            .contains_key("tpc_t"),
        "global snapshot must not carry the table",
    );
    let count = client
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'tpc_t'",
            &[],
        )
        .await
        .expect("pg_class probe");
    assert_eq!(
        count.get::<_, i64>(0),
        0,
        "no durable pg_class row may exist for the rejected-PREPARE table",
    );

    shutdown(running).await;
}

// ───────────────────────────── Battery #9 ─────────────────────────────
// Autocommit-vs-in-txn same-name race: the per-name AccessExclusive lock is
// taken on BOTH paths, so an autocommit CREATE TABLE and an in-txn CREATE TABLE
// of the same name serialize. Exactly ONE durable pg_class row may exist after
// the in-txn creator commits and the server restarts.
//
// Pre-fix the autocommit path never contended on the name lock (it was taken
// only inside the in-txn branch): the autocommit creator could not see the
// in-txn table (staged in the other session's overlay) and self-committed its
// own durable rows; the in-txn creator then committed too → two same-name
// pg_class rows (count == 2 after restart).

/// Count durable `pg_class` rows for `relname` by reopening the data dir and
/// reading the rebuilt catalog snapshot.
async fn durable_pg_class_count(data_dir: &std::path::Path, relname: &str) -> i64 {
    let running = start_persistent_server(data_dir, "txddl_pgclass_probe").await;
    let count = running
        .client
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = $1",
            &[&relname],
        )
        .await
        .expect("pg_class count probe")
        .get::<_, i64>(0);
    shutdown(running).await;
    count
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn autocommit_vs_in_txn_same_name_create_yields_single_durable_row() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());
    let running = start_persistent_server(data_dir.path(), "txddl_mixrace_a").await;
    let (client_b, b_handle) = connect_as(running.bound, "tester", "txddl_mixrace_b").await;

    // A opens a transaction and creates `dup`, taking the name lock under its
    // user xid (held until A's COMMIT/ROLLBACK).
    running
        .client
        .batch_execute("BEGIN")
        .await
        .expect("A begin");
    running
        .client
        .batch_execute("CREATE TABLE dup (id INT NOT NULL)")
        .await
        .expect("A in-txn create takes the name lock");

    // B, in autocommit, attempts the SAME name while A holds the lock. With the
    // fix the autocommit path contends on the shared name lock and loses
    // immediately (non-blocking try_acquire) → 40001. (Pre-fix B would NOT
    // contend and would self-commit a second durable pg_class row.)
    let err = client_b
        .batch_execute("CREATE TABLE dup (id INT NOT NULL)")
        .await
        .expect_err("B's autocommit same-name CREATE must fail while A holds the name lock");
    assert_eq!(
        sqlstate(&err),
        "40001",
        "autocommit-vs-in-txn same-name race must report serialization_failure, got {err}"
    );

    // A commits and wins.
    running
        .client
        .batch_execute("COMMIT")
        .await
        .expect("A commit wins");

    // B retries in autocommit: A's table is committed, so B now fails
    // already-exists (42P07).
    let err = client_b
        .batch_execute("CREATE TABLE dup (id INT NOT NULL)")
        .await
        .expect_err("B's autocommit retry must see A's committed table");
    assert_eq!(
        sqlstate(&err),
        "42P07",
        "autocommit retry after winner committed must report duplicate_table, got {err}"
    );

    drop(client_b);
    let _ = b_handle.await;
    shutdown(running).await;

    // Exactly one durable pg_class row for `dup` survives the restart.
    assert_eq!(
        durable_pg_class_count(data_dir.path(), "dup").await,
        1,
        "exactly one durable pg_class row for 'dup' (no duplicate-name corruption)",
    );
}

// The mirror order: B autocommit creates first (and self-commits), then A's
// in-txn CREATE of the same name must lose against the already-committed row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn autocommit_then_in_txn_same_name_create_yields_single_durable_row() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());
    let running = start_persistent_server(data_dir.path(), "txddl_mixrace2_a").await;

    // Autocommit creates and commits `dup2` first.
    running
        .client
        .batch_execute("CREATE TABLE dup2 (id INT NOT NULL)")
        .await
        .expect("autocommit create commits durably");

    // An in-txn CREATE of the same name now sees the committed row (existence
    // re-checked under the lock) and fails already-exists (42P07).
    running.client.batch_execute("BEGIN").await.expect("begin");
    let err = running
        .client
        .batch_execute("CREATE TABLE dup2 (id INT NOT NULL)")
        .await
        .expect_err("in-txn create must see the autocommitted table");
    assert_eq!(
        sqlstate(&err),
        "42P07",
        "in-txn create of an already-committed name must report duplicate_table, got {err}"
    );
    running
        .client
        .batch_execute("ROLLBACK")
        .await
        .expect("rollback");

    shutdown(running).await;

    assert_eq!(
        durable_pg_class_count(data_dir.path(), "dup2").await,
        1,
        "exactly one durable pg_class row for 'dup2'",
    );
}

// ───────────────────────────── Battery #10 ─────────────────────────────
// In-txn CREATE TABLE variants that would create a durable artifact the overlay
// cannot transactionally roll back are rejected (0A000, block Failed). The same
// statement is fully supported on the autocommit path.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_side_effect_create_table_variants_rejected_in_txn() {
    // FOREIGN KEY / PARTITION BY / DEFAULT nextval and serial all build (or
    // depend on) a durable artifact the overlay cannot undo: reject in-txn.
    // (PRIMARY KEY / UNIQUE are now supported via the milestone-2 deferred
    // index build — see `in_txn_create_table_primary_key_*` below.)
    assert_ddl_rejected_in_txn(
        "txddl_v_fk",
        &["CREATE TABLE v_parent (id INT PRIMARY KEY)"],
        "CREATE TABLE v_child (id INT, p INT REFERENCES v_parent (id))",
    )
    .await;
    assert_ddl_rejected_in_txn(
        "txddl_v_part",
        &[],
        "CREATE TABLE v_part (ts TIMESTAMP NOT NULL, v INT) PARTITION BY RANGE (ts)",
    )
    .await;
    assert_ddl_rejected_in_txn("txddl_v_serial", &[], "CREATE TABLE v_serial (id SERIAL)").await;
    // CREATE TABLE AS SELECT is not supported by the engine at all (it never
    // produces a plain CreateTable plan, hitting the binder's catch-all), so it
    // rejects with 0A000 regardless of transaction context — assert the in-txn
    // rejection still holds and fails the block.
    assert_ddl_rejected_in_txn(
        "txddl_v_ctas",
        &["CREATE TABLE v_src (id INT)"],
        "CREATE TABLE v_ctas AS SELECT * FROM v_src",
    )
    .await;
    // A column DEFAULT calling nextval() is rejected before the milestone-1
    // overlay guard is reached: the binder does not resolve `nextval` as an
    // allowed builtin (unlike e.g. `lower()`), so it fails to bind in any
    // context. Assert the in-txn statement still rejects and fails the block.
    assert_ddl_rejected_in_txn(
        "txddl_v_nextval",
        &["CREATE SEQUENCE v_seq"],
        "CREATE TABLE v_dn (id INT DEFAULT nextval('v_seq'))",
    )
    .await;
    // CREATE TEMP TABLE is not parsed by the engine (TEMP is not in the
    // CREATE TABLE grammar), so it fails with a syntax error (42601) rather than
    // the feature gate. Assert it still errors in-txn and fails the block.
    assert_create_temp_rejected_in_txn().await;
}

/// `CREATE TEMP TABLE` is unparsed (42601), so it cannot use the
/// `assert_ddl_rejected_in_txn` 0A000 helper. Assert it still errors in-txn and
/// leaves the block Failed (25P02).
async fn assert_create_temp_rejected_in_txn() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_v_temp").await;
    let client = &running.client;

    client.batch_execute("BEGIN").await.expect("begin");
    let err = client
        .batch_execute("CREATE TEMP TABLE v_tmp (id INT)")
        .await
        .expect_err("CREATE TEMP TABLE must be rejected");
    assert_eq!(
        sqlstate(&err),
        "42601",
        "CREATE TEMP TABLE is unparsed → syntax_error, got {err}"
    );
    let in_failed = client
        .batch_execute("SELECT 1")
        .await
        .expect_err("statement after rejected CREATE TEMP must be 25P02");
    assert_eq!(
        sqlstate(&in_failed),
        "25P02",
        "in-failed-block after CREATE TEMP must be in_failed_sql_transaction, got {in_failed}"
    );
    client.batch_execute("ROLLBACK").await.expect("rollback");

    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_side_effect_create_table_variants_still_work_in_autocommit() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_v_autocommit").await;
    let client = &running.client;

    // The same variants the in-txn path rejects all succeed in autocommit.
    client
        .batch_execute("CREATE TABLE ac_pk (id INT PRIMARY KEY)")
        .await
        .expect("autocommit PRIMARY KEY");
    client
        .batch_execute("CREATE TABLE ac_uniq (id INT, u INT UNIQUE)")
        .await
        .expect("autocommit UNIQUE");
    client
        .batch_execute("CREATE TABLE ac_parent (id INT PRIMARY KEY)")
        .await
        .expect("autocommit referenced table");
    client
        .batch_execute("CREATE TABLE ac_child (id INT, p INT REFERENCES ac_parent (id))")
        .await
        .expect("autocommit FOREIGN KEY");
    client
        .batch_execute("CREATE TABLE ac_serial (id SERIAL)")
        .await
        .expect("autocommit SERIAL");

    // Each is real and usable.
    client
        .batch_execute("INSERT INTO ac_pk VALUES (1)")
        .await
        .expect("insert into autocommit PK table");

    shutdown(running).await;
}

// The reject guard must be PRECISE: the allowed in-txn surface (plain columns,
// NOT NULL, a constant or immutable non-nextval DEFAULT, and CHECK constraints
// — all of which persist as pure MVCC catalog rows under the user xid) still
// commits and rolls back cleanly. This proves the milestone-1 gate did not
// over-reject.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn allowed_in_txn_create_table_surface_commits_and_rolls_back() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_allowed").await;
    let client = &running.client;

    // COMMIT case: NOT NULL + constant DEFAULT + immutable function DEFAULT
    // (`lower`, an allowed builtin — distinct from the rejected `nextval`) + a
    // CHECK constraint.
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute(
            "CREATE TABLE allowed_t ( \
                 id INT NOT NULL, \
                 n INT DEFAULT 7, \
                 label TEXT DEFAULT lower('ADA'), \
                 CHECK (id >= 0) \
             )",
        )
        .await
        .expect("allowed in-txn CREATE TABLE surface is accepted");
    client
        .batch_execute("INSERT INTO allowed_t (id) VALUES (1)")
        .await
        .expect("insert exercising the defaults");
    let row = client
        .query_one("SELECT n, label FROM allowed_t WHERE id = 1", &[])
        .await
        .expect("defaults applied in-txn");
    assert_eq!(row.get::<_, i32>(0), 7);
    assert_eq!(row.get::<_, &str>(1), "ada");
    client.batch_execute("COMMIT").await.expect("commit");

    // The CHECK constraint is enforced post-commit (persisted as an MVCC
    // pg_constraint row under the user xid).
    let violation = client
        .batch_execute("INSERT INTO allowed_t (id) VALUES (-1)")
        .await
        .expect_err("CHECK (id >= 0) must reject a negative id");
    assert_eq!(
        sqlstate(&violation),
        "23514",
        "CHECK violation must report check_violation, got {violation}"
    );

    // ROLLBACK case: the same allowed surface rolls back cleanly.
    client.batch_execute("BEGIN").await.expect("begin 2");
    client
        .batch_execute("CREATE TABLE allowed_rb (id INT NOT NULL, n INT DEFAULT 1, CHECK (n > 0))")
        .await
        .expect("allowed in-txn create (rollback case)");
    client.batch_execute("ROLLBACK").await.expect("rollback");
    let err = client
        .query("SELECT id FROM allowed_rb", &[])
        .await
        .expect_err("rolled-back allowed table must be invisible");
    assert!(is_undefined_table(&err), "expected 42P01, got {err}");

    shutdown(running).await;
}

// ───────────────────────────── Battery #11 ─────────────────────────────
// Self-visibility of an in-txn-created table on the COPY and PREPARE/EXECUTE
// read paths (pre-fix these re-fetched the RAW committed snapshot and failed
// 42P01 for the session's own in-txn table).

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_into_in_txn_created_table_is_self_visible() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_copy_self").await;
    let client = &running.client;

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("CREATE TABLE copy_self_t (id INT NOT NULL, v TEXT)")
        .await
        .expect("in-txn create");

    // COPY into the table created earlier in this same transaction must resolve
    // it (self-visibility) — pre-fix this failed 42P01.
    let sink = client
        .copy_in::<_, Bytes>("COPY copy_self_t FROM STDIN")
        .await
        .expect("COPY into self-created in-txn table establishes");
    futures::pin_mut!(sink);
    sink.as_mut()
        .send(Bytes::from_static(b"1\ta\n2\tb\n"))
        .await
        .expect("send CopyData");
    let copied = sink.finish().await.expect("finish copy_in");
    assert_eq!(copied, 2, "COPY reports two rows ingested");

    let rows = client
        .query("SELECT id, v FROM copy_self_t ORDER BY id", &[])
        .await
        .expect("self-select after COPY");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[1].get::<_, &str>(1), "b");

    client.batch_execute("COMMIT").await.expect("commit");

    // Visible to a fresh connection after commit.
    let (client_b, b_handle) = connect_as(running.bound, "tester", "txddl_copy_self_b").await;
    let rows = client_b
        .query("SELECT count(*) FROM copy_self_t", &[])
        .await
        .expect("fresh connection sees committed COPYed rows");
    assert_eq!(rows[0].get::<_, i64>(0), 2);

    drop(client_b);
    let _ = b_handle.await;
    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_execute_resolves_in_txn_created_table() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_prep_exec").await;
    let client = &running.client;

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("CREATE TABLE pe_t (id INT NOT NULL)")
        .await
        .expect("in-txn create");
    client
        .batch_execute("INSERT INTO pe_t VALUES (1), (2), (3)")
        .await
        .expect("in-txn insert");

    // SQL-level PREPARE binds against the overlay (already correct), and
    // EXECUTE re-resolves the plan against the overlay-aware snapshot — pre-fix
    // EXECUTE re-fetched the RAW snapshot and failed 42P01. EXECUTE is a
    // simple-query meta-statement (no LogicalPlan of its own), so it is driven
    // through `simple_query`, not the extended `query` path.
    client
        .batch_execute("PREPARE pe_p AS SELECT id FROM pe_t ORDER BY id")
        .await
        .expect("PREPARE against self-created in-txn table");
    let rows = client
        .simple_query("EXECUTE pe_p")
        .await
        .expect("EXECUTE resolves the in-txn-created table");
    let got: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).and_then(|s| s.parse().ok()),
            _ => None,
        })
        .collect();
    assert_eq!(got, vec![1, 2, 3], "EXECUTE sees the in-txn rows in order");

    client.batch_execute("COMMIT").await.expect("commit");

    shutdown(running).await;
}

// ───────────────────────────── Battery #12 ─────────────────────────────
// EXPLAIN / EXPLAIN ANALYZE over the EXTENDED protocol must resolve a table
// created earlier in the same open transaction.
//
// `EXPLAIN ANALYZE` re-lowers and executes the wrapped plan against a catalog
// snapshot. Over the extended (Parse/Bind/Execute) path the EXPLAIN branch used
// the RAW committed snapshot (`state.catalog_snapshot()`), which does not carry
// the session's in-txn overlay, so the inner `SELECT * FROM t` failed 42P01 for
// the session's own freshly-created table. The fix routes the extended EXPLAIN
// branch through `effective_catalog_snapshot()` (overlay-aware), mirroring every
// other extended read path. `tokio_postgres::query`/`query_one` with an explicit
// params slice drive Parse/Bind/Execute, so this exercises the extended branch
// (not simple-query).

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extended_explain_analyze_resolves_in_txn_created_table() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_ext_explain").await;
    let client = &running.client;

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("CREATE TABLE xa_t (id INT NOT NULL)")
        .await
        .expect("in-txn create");
    client
        .batch_execute("INSERT INTO xa_t VALUES (1), (2)")
        .await
        .expect("in-txn insert");

    // EXPLAIN ANALYZE over the extended path: `query` issues Parse/Bind/Execute.
    // Pre-fix this failed 42P01 because the EXPLAIN branch lowered+executed the
    // inner plan against the raw committed snapshot, which lacks the overlay.
    let rows = client
        .query("EXPLAIN ANALYZE SELECT * FROM xa_t", &[])
        .await
        .expect("extended EXPLAIN ANALYZE resolves the in-txn-created table");
    assert!(
        !rows.is_empty(),
        "EXPLAIN ANALYZE must return a non-empty plan, not 42P01"
    );
    let plan_text: String = rows
        .iter()
        .map(|r| r.get::<_, &str>(0))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan_text.to_lowercase().contains("actual rows"),
        "EXPLAIN ANALYZE plan must carry the executed `actual rows` annotation, got:\n{plan_text}"
    );

    // Plain EXPLAIN (no ANALYZE) over the same extended path must also resolve
    // the table and render a plan.
    let rows = client
        .query("EXPLAIN SELECT * FROM xa_t", &[])
        .await
        .expect("extended EXPLAIN resolves the in-txn-created table");
    assert!(!rows.is_empty(), "EXPLAIN must return a plan, not 42P01");

    // Sanity: the same statement still works after COMMIT (overlay published).
    client.batch_execute("COMMIT").await.expect("commit");
    let rows = client
        .query("EXPLAIN ANALYZE SELECT * FROM xa_t", &[])
        .await
        .expect("EXPLAIN ANALYZE works post-commit too");
    assert!(
        !rows.is_empty(),
        "post-commit EXPLAIN ANALYZE returns a plan"
    );

    shutdown(running).await;
}

// ───────────────────────────── Battery #13 ─────────────────────────────
// PREPARE TRANSACTION issued while the explicit block is ALREADY Failed but
// still carries a pending transactional-DDL overlay.
//
// The 0A000 reject guard (battery #8) fires only when the block is healthy. If a
// runtime error has already flipped the block to Failed while the overlay is
// staged, the Failed-block branch of `execute_prepare_transaction` is reached
// instead: it aborts the txn and returns ROLLBACK. Pre-fix that branch did NOT
// discard the overlay, so the staged in-memory side effects (the CHECK's runtime
// constraint in the GLOBAL `Server::table_constraints` map, keyed by the aborted
// table's OID) leaked for the process lifetime. The fix calls
// `discard_pending_catalog_ddl()` on both the abort-error and success paths,
// mirroring the COMMIT/ROLLBACK Failed branches.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_block_prepare_discards_in_txn_ddl_overlay() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_failed_prepare").await;
    let client = &running.client;

    // Server-side observable: the count of runtime-constraint entries in the
    // global map before any in-txn DDL.
    let baseline_constraints = running.server.table_constraints.len();

    client.batch_execute("BEGIN").await.expect("begin");
    // Allowed in-txn surface (plain column + CHECK). The CHECK stages a runtime
    // constraint into the GLOBAL Server::table_constraints map keyed by the new
    // table's OID, alongside the catalog overlay.
    client
        .batch_execute("CREATE TABLE fb_t (id INT NOT NULL, CHECK (id >= 0))")
        .await
        .expect("in-txn create with CHECK");
    assert_eq!(
        running.server.table_constraints.len(),
        baseline_constraints + 1,
        "the in-txn CHECK must stage a runtime constraint into the global map",
    );

    // Flip the block to Failed WITHOUT touching the overlay: a plain runtime
    // error (division by zero) trips into the Failed state but leaves
    // `pending_catalog_ddl` set.
    let div = client
        .batch_execute("SELECT 1 / 0")
        .await
        .expect_err("division by zero must fail the block");
    assert_eq!(
        sqlstate(&div),
        "22012",
        "expected division_by_zero, got {div}"
    );

    // PREPARE TRANSACTION while Failed hits the Failed(txn) branch, which aborts
    // the txn and returns ROLLBACK (reachable before the healthy-block 0A000
    // guard). This is the path the fix hardened. tokio-postgres surfaces the
    // server's ROLLBACK as a successful batch.
    client
        .batch_execute("PREPARE TRANSACTION 'fb-gid'")
        .await
        .expect("failed-block PREPARE returns ROLLBACK, terminating the txn");

    // (a) Rollback: the table is gone for this session and the global snapshot,
    //     and a fresh autocommit same-name CREATE works and is the only one — no
    //     stale side-map entry interferes.
    let err = client
        .query("SELECT id FROM fb_t", &[])
        .await
        .expect_err("aborted table must be invisible to self after failed-block PREPARE");
    assert!(is_undefined_table(&err), "expected 42P01, got {err}");
    assert!(
        !running
            .server
            .catalog_snapshot()
            .tables
            .contains_key("fb_t"),
        "global snapshot must not carry the aborted table",
    );

    // Other session also never sees it.
    let (client_b, b_handle) = connect_as(running.bound, "tester", "txddl_failed_prepare_b").await;
    let err = client_b
        .query("SELECT id FROM fb_t", &[])
        .await
        .expect_err("other session must not see the aborted table");
    assert!(is_undefined_table(&err), "expected 42P01 for B, got {err}");

    // (b) Leak-freedom — the staged side effect did NOT leak. The runtime
    //     constraint for the aborted table's OID must be reverted: the global
    //     map is back to baseline. (Pre-fix it remained, +1 forever.)
    assert_eq!(
        running.server.table_constraints.len(),
        baseline_constraints,
        "failed-block PREPARE must discard the overlay's staged runtime constraint \
         (leak: aborted table's OID still in the global table_constraints map)",
    );

    // The same name creates cleanly afterward (autocommit) — no stale state.
    client
        .batch_execute("CREATE TABLE fb_t (id INT NOT NULL, CHECK (id >= 0))")
        .await
        .expect("same-name autocommit CREATE works after the aborted in-txn one");
    client
        .batch_execute("INSERT INTO fb_t VALUES (5)")
        .await
        .expect("the freshly committed table is usable");
    let rows = client
        .query("SELECT count(*) FROM fb_t", &[])
        .await
        .expect("query the committed table");
    assert_eq!(rows[0].get::<_, i64>(0), 1, "exactly the one inserted row");
    // The fresh table re-stakes exactly one global runtime-constraint entry: the
    // map is baseline + 1 (the new table), proving no orphan from the aborted one.
    assert_eq!(
        running.server.table_constraints.len(),
        baseline_constraints + 1,
        "only the committed table's runtime constraint is present (no orphan)",
    );

    drop(client_b);
    let _ = b_handle.await;
    shutdown(running).await;
}

// ═══════════════════ Transactional-DDL milestone 2 ═══════════════════
// In-txn `CREATE TABLE … PRIMARY KEY / UNIQUE` is now supported by deferring the
// implicit constraint-index B-tree build to COMMIT. The index is staged UNBUILT
// (`root_block == INVALID`) so in-txn INSERTs skip its maintenance and a
// ROLLBACK / crash-before-commit leaks no durable index segment; the tree is
// built once, over the table's rows under the user snapshot, at COMMIT. A
// duplicate key found during that build aborts the WHOLE transaction with 23505
// — never a half-committed table.

// ───────────────────────────── M2 #1 ─────────────────────────────
// COMMIT publishes a working index: visible to a 2nd connection, the index
// enforces uniqueness on a post-commit INSERT, and survives restart BUILT
// (a post-restart duplicate still fails 23505 — proves the durable root_block
// was corrected, not left INVALID/unbuilt).

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_txn_create_table_primary_key_commits_working_index() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "txddl_pk_commit_a").await;
        let client = &running.client;

        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("CREATE TABLE pk_t (id INT PRIMARY KEY)")
            .await
            .expect("in-txn CREATE TABLE … PRIMARY KEY is accepted (milestone 2)");
        client.batch_execute("COMMIT").await.expect("commit");

        // Visible to a 2nd connection, with the index on `id`.
        let (client_b, b_handle) = connect_as(running.bound, "tester", "txddl_pk_commit_b").await;
        client_b
            .query("SELECT id FROM pk_t", &[])
            .await
            .expect("2nd connection sees the committed table");
        client_b
            .batch_execute("INSERT INTO pk_t VALUES (1)")
            .await
            .expect("first insert ok");
        // The index enforces uniqueness on a post-commit INSERT.
        let dup = client_b
            .batch_execute("INSERT INTO pk_t VALUES (1)")
            .await
            .expect_err("duplicate id must violate the committed PRIMARY KEY index");
        assert_eq!(
            sqlstate(&dup),
            "23505",
            "post-commit duplicate must report unique_violation, got {dup}"
        );

        drop(client_b);
        let _ = b_handle.await;
        shutdown(running).await;
    }

    // Survives restart BUILT: the table and its row are present, and the index
    // still rejects a duplicate (so the durable root_block was corrected from
    // INVALID to the real built tree — not left unbuilt).
    let running = start_persistent_server(data_dir.path(), "txddl_pk_commit_a2").await;
    let rows = running
        .client
        .query("SELECT id FROM pk_t ORDER BY id", &[])
        .await
        .expect("committed PK table present after restart");
    assert_eq!(rows.len(), 1, "the single row survives restart");
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    let dup = running
        .client
        .batch_execute("INSERT INTO pk_t VALUES (1)")
        .await
        .expect_err("post-restart duplicate must still violate the rebuilt PK index");
    assert_eq!(
        sqlstate(&dup),
        "23505",
        "post-restart duplicate must report unique_violation (index rebuilt BUILT), got {dup}"
    );
    shutdown(running).await;
}

// ───────────────────────────── M2 #2 (corruption gate) ─────────────────────────────
// ROLLBACK of an in-txn CREATE TABLE … PRIMARY KEY leaves NO durable index (or
// table) segment, no catalog entry, and restart-clean. No INSERTs ran, so the
// table is never materialized and the index is never built — the strict
// `user_relation_segments`-empty check applies.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rollback_in_txn_create_table_primary_key_leaves_no_segment() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "txddl_pk_rollback_a").await;
        let client = &running.client;

        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("CREATE TABLE pk_rb (id INT PRIMARY KEY)")
            .await
            .expect("in-txn create with PK");
        client.batch_execute("ROLLBACK").await.expect("rollback");

        // No user-relation segment file: the table was never INSERTed into
        // (lazy materialization) and the deferred index was never built — a
        // ROLLBACK leaks no `base/<user-oid>` segment for either OID.
        assert_eq!(
            user_relation_segments(data_dir.path()),
            Vec::<String>::new(),
            "ROLLBACK of CREATE TABLE … PRIMARY KEY must leave no base/<user-oid> segment \
             (table never materialized, index never built)",
        );

        // Invisible to self and to the global committed snapshot.
        let err = client
            .query("SELECT id FROM pk_rb", &[])
            .await
            .expect_err("rolled-back PK table must be invisible to self");
        assert!(is_undefined_table(&err), "expected 42P01, got {err}");
        assert!(
            !running
                .server
                .catalog_snapshot()
                .tables
                .contains_key("pk_rb"),
            "global snapshot must not carry the rolled-back PK table",
        );

        shutdown(running).await;
    }

    // Restart-clean: no table, no index, no durable pg_class row.
    let running = start_persistent_server(data_dir.path(), "txddl_pk_rollback_a2").await;
    assert!(
        !running
            .server
            .catalog_snapshot()
            .tables
            .contains_key("pk_rb"),
        "rolled-back PK table must not resurrect after restart",
    );
    let err = running
        .client
        .query("SELECT id FROM pk_rb", &[])
        .await
        .expect_err("rolled-back PK table must be absent after restart");
    assert!(is_undefined_table(&err), "expected 42P01, got {err}");
    let count = running
        .client
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'pk_rb'",
            &[],
        )
        .await
        .expect("pg_class probe")
        .get::<_, i64>(0);
    assert_eq!(
        count, 0,
        "no durable pg_class row for the rolled-back PK table"
    );
    // And still no orphaned user segment after the restart's recovery.
    assert_eq!(
        user_relation_segments(data_dir.path()),
        Vec::<String>::new(),
        "no user segment after restart either",
    );
    shutdown(running).await;
}

// ───────────────────────────── M2 #3 (corruption gate) ─────────────────────────────
// Crash mid-transaction after an in-txn CREATE TABLE … PRIMARY KEY, before
// COMMIT: the table AND its index must not resurrect on restart. The deferred
// build never ran (no COMMIT), so no index segment was ever allocated, and the
// catalog rows ride the uncommitted user xid (hidden by the visibility-filtered
// bootstrap).

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_before_commit_in_txn_create_table_primary_key_does_not_resurrect() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "txddl_pk_crash_a").await;
        running.client.batch_execute("BEGIN").await.expect("begin");
        running
            .client
            .batch_execute("CREATE TABLE pk_crash (id INT PRIMARY KEY)")
            .await
            .expect("in-txn create with PK (durable catalog rows under user xid, NO commit)");
        // Drop the server WITHOUT COMMIT/ROLLBACK — the user xid has no commit
        // record; the deferred index was never built.
        shutdown(running).await;
    }

    let running = start_persistent_server(data_dir.path(), "txddl_pk_crash_a2").await;
    assert!(
        !running
            .server
            .catalog_snapshot()
            .tables
            .contains_key("pk_crash"),
        "crash-before-commit PK table must not resurrect in the catalog snapshot",
    );
    let err = running
        .client
        .query("SELECT id FROM pk_crash", &[])
        .await
        .expect_err("crash-before-commit PK table must be absent after restart");
    assert!(is_undefined_table(&err), "expected 42P01, got {err}");
    // No durable pg_class row for the table or its implicit index.
    let count = running
        .client
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_class \
             WHERE relname = 'pk_crash' OR relname LIKE 'pk_crash%pkey%'",
            &[],
        )
        .await
        .expect("pg_class probe")
        .get::<_, i64>(0);
    assert_eq!(
        count, 0,
        "no durable pg_class row for the crash-before-commit table or its index",
    );
    shutdown(running).await;
}

// ───────────────────────────── M2 #4 ─────────────────────────────
// CREATE + INSERT(unique) + COMMIT: the deferred build indexes the existing
// rows; index lookups work; rows + index survive restart.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_txn_create_insert_unique_commit_builds_index_over_rows() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "txddl_pk_rows_a").await;
        let client = &running.client;

        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("CREATE TABLE pk_rows (id INT PRIMARY KEY, v TEXT)")
            .await
            .expect("in-txn create with PK");
        client
            .batch_execute("INSERT INTO pk_rows VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            .await
            .expect("in-txn unique inserts (index unbuilt, maintenance skipped)");
        client
            .batch_execute("COMMIT")
            .await
            .expect("commit builds the index over the rows");

        // An index lookup resolves a specific key (the tree carries the rows).
        let row = client
            .query_one("SELECT v FROM pk_rows WHERE id = 2", &[])
            .await
            .expect("index lookup on the committed PK");
        assert_eq!(row.get::<_, &str>(0), "b");
        // Uniqueness is enforced post-commit against an EXISTING key.
        let dup = client
            .batch_execute("INSERT INTO pk_rows VALUES (2, 'dup')")
            .await
            .expect_err("re-inserting an existing key must violate the built index");
        assert_eq!(sqlstate(&dup), "23505", "expected 23505, got {dup}");

        shutdown(running).await;
    }

    let running = start_persistent_server(data_dir.path(), "txddl_pk_rows_a2").await;
    let rows = running
        .client
        .query("SELECT id FROM pk_rows ORDER BY id", &[])
        .await
        .expect("rows present after restart");
    let ids: Vec<i32> = rows.iter().map(|r| r.get::<_, i32>(0)).collect();
    assert_eq!(ids, vec![1, 2, 3], "all three rows survive restart");
    // Index still enforces uniqueness after restart (rebuilt BUILT).
    let dup = running
        .client
        .batch_execute("INSERT INTO pk_rows VALUES (3, 'x')")
        .await
        .expect_err("post-restart duplicate must violate the rebuilt index");
    assert_eq!(
        sqlstate(&dup),
        "23505",
        "expected 23505 post-restart, got {dup}"
    );
    shutdown(running).await;
}

// ───────────────────────────── M2 #5 (THE CRUX — corruption gate) ─────────────────────────────
// CREATE + INSERT(DUPLICATE) + COMMIT: the duplicate is caught during the
// deferred build at COMMIT, which fails 23505 and rolls back the WHOLE
// transaction. From a 2nd connection AND after restart the table, its rows, and
// its index are ALL absent.
//
// Note on segments: the two INSERTs materialize the table heap before the build
// fails, so a `base/<table_oid>` orphan segment may exist on disk after the
// abort. That is the same bounded, MVCC-safe orphan-file leak the engine already
// tolerates for aborted xids (the rows carry the aborted xmin → invisible; the
// catalog-driven, visibility-filtered bootstrap never resurrects the relation).
// The correctness gate is therefore catalog/query/restart ABSENCE, not a literal
// zero-file count (which only holds for the no-INSERT case, M2 #2).

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_txn_create_insert_duplicate_commit_fails_23505_full_rollback() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    let dup_sqlstate;
    {
        let running = start_persistent_server(data_dir.path(), "txddl_pk_dup_a").await;
        let client = &running.client;

        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("CREATE TABLE pk_dup (id INT PRIMARY KEY)")
            .await
            .expect("in-txn create with PK");
        // Both inserts succeed in-txn (the index is unbuilt — maintenance
        // skipped — so the duplicate is NOT caught here).
        client
            .batch_execute("INSERT INTO pk_dup VALUES (1), (1)")
            .await
            .expect("duplicate inserts succeed in-txn (deferred uniqueness check)");

        // COMMIT runs the deferred build, finds the duplicate, and fails 23505,
        // rolling back the whole transaction.
        let err = client
            .batch_execute("COMMIT")
            .await
            .expect_err("COMMIT must fail when the deferred index build hits a duplicate");
        dup_sqlstate = sqlstate(&err);
        assert_eq!(
            dup_sqlstate, "23505",
            "duplicate at the COMMIT build must report unique_violation, got {err}"
        );

        // FULL rollback: self no longer sees the table (the txn is over and
        // aborted — the table never committed).
        let err = client
            .query("SELECT id FROM pk_dup", &[])
            .await
            .expect_err("table must be absent after the failed COMMIT");
        assert!(is_undefined_table(&err), "expected 42P01, got {err}");
        assert!(
            !running
                .server
                .catalog_snapshot()
                .tables
                .contains_key("pk_dup"),
            "global snapshot must not carry the aborted table",
        );

        // A 2nd connection also never sees it.
        let (client_b, b_handle) = connect_as(running.bound, "tester", "txddl_pk_dup_b").await;
        let err = client_b
            .query("SELECT id FROM pk_dup", &[])
            .await
            .expect_err("2nd connection must not see the aborted table");
        assert!(is_undefined_table(&err), "expected 42P01 for B, got {err}");

        // The same name creates cleanly afterward (no stale state).
        client_b
            .batch_execute("CREATE TABLE pk_dup (id INT PRIMARY KEY)")
            .await
            .expect("same-name autocommit CREATE works after the aborted in-txn one");
        client_b
            .batch_execute("INSERT INTO pk_dup VALUES (9)")
            .await
            .expect("the freshly created table is usable");

        drop(client_b);
        let _ = b_handle.await;
        shutdown(running).await;
    }
    assert_eq!(dup_sqlstate, "23505");

    // After restart: the recreated `pk_dup` (the clean autocommit one with a
    // single row 9) is present — exactly one durable pg_class row — and the
    // aborted in-txn table/rows/index left nothing behind.
    let running = start_persistent_server(data_dir.path(), "txddl_pk_dup_a2").await;
    let rows = running
        .client
        .query("SELECT id FROM pk_dup ORDER BY id", &[])
        .await
        .expect("the clean recreated table is present after restart");
    let ids: Vec<i32> = rows.iter().map(|r| r.get::<_, i32>(0)).collect();
    assert_eq!(
        ids,
        vec![9],
        "only the clean autocommit row survives — the aborted in-txn rows (1),(1) are gone",
    );
    // Exactly one durable pg_class row for the name (no aborted duplicate).
    let count = running
        .client
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'pk_dup' AND relkind = 'r'",
            &[],
        )
        .await
        .expect("pg_class probe")
        .get::<_, i64>(0);
    assert_eq!(
        count, 1,
        "exactly one durable pg_class table row for 'pk_dup' (no aborted-txn leak)",
    );
    // The rebuilt index still enforces uniqueness (the recreated PK is BUILT).
    let dup = running
        .client
        .batch_execute("INSERT INTO pk_dup VALUES (9)")
        .await
        .expect_err("post-restart duplicate on the recreated table must violate its PK");
    assert_eq!(sqlstate(&dup), "23505", "expected 23505, got {dup}");
    shutdown(running).await;
}

// ───────────────────────────── M2 #6 ─────────────────────────────
// Concurrent same-name in-txn CREATE TABLE … PRIMARY KEY still serializes on the
// per-name AccessExclusive lock (unchanged by the deferred build): the loser
// fails immediately with 40001 while the holder is open, and 42P07 after it
// commits.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_in_txn_create_same_name_primary_key_serializes() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_pk_race_a").await;
    let (client_b, b_handle) = connect_as(running.bound, "tester", "txddl_pk_race_b").await;

    running
        .client
        .batch_execute("BEGIN")
        .await
        .expect("A begin");
    running
        .client
        .batch_execute("CREATE TABLE pk_race (id INT PRIMARY KEY)")
        .await
        .expect("A in-txn create takes the name lock");

    client_b.batch_execute("BEGIN").await.expect("B begin");
    let err = client_b
        .batch_execute("CREATE TABLE pk_race (id INT PRIMARY KEY)")
        .await
        .expect_err("B's same-name CREATE must fail while A holds the name lock");
    assert_eq!(
        sqlstate(&err),
        "40001",
        "concurrent same-name PK CREATE must report serialization_failure, got {err}"
    );
    client_b
        .batch_execute("ROLLBACK")
        .await
        .expect("B rollback");

    running
        .client
        .batch_execute("COMMIT")
        .await
        .expect("A commit wins");

    client_b.batch_execute("BEGIN").await.expect("B begin 2");
    let err = client_b
        .batch_execute("CREATE TABLE pk_race (id INT PRIMARY KEY)")
        .await
        .expect_err("B's retry must see A's committed table");
    assert_eq!(
        sqlstate(&err),
        "42P07",
        "retry after winner committed must report duplicate_table, got {err}"
    );
    client_b
        .batch_execute("ROLLBACK")
        .await
        .expect("B rollback 2");

    // Exactly one usable table whose PK enforces uniqueness.
    running
        .client
        .batch_execute("INSERT INTO pk_race VALUES (1)")
        .await
        .expect("the single committed table is usable");
    let dup = running
        .client
        .batch_execute("INSERT INTO pk_race VALUES (1)")
        .await
        .expect_err("the committed table's PK enforces uniqueness");
    assert_eq!(sqlstate(&dup), "23505", "expected 23505, got {dup}");

    drop(client_b);
    let _ = b_handle.await;
    shutdown(running).await;
}

// ───────────────────────────── M2 #7 ─────────────────────────────
// A multi-column UNIQUE constraint is built correctly at COMMIT (composite-key
// encoding must match the insert-time maintainer): a composite duplicate is
// caught post-commit, distinct composites are allowed, and it survives restart.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_txn_create_table_composite_unique_builds_and_enforces() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "txddl_uniq2_a").await;
        let client = &running.client;

        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("CREATE TABLE uniq2 (a INT, b INT, UNIQUE (a, b))")
            .await
            .expect("in-txn create with composite UNIQUE");
        client
            .batch_execute("INSERT INTO uniq2 VALUES (1, 1), (1, 2), (2, 1)")
            .await
            .expect("distinct composite keys insert in-txn");
        client
            .batch_execute("COMMIT")
            .await
            .expect("commit builds the composite index");

        // A composite duplicate of an existing (a,b) pair is rejected; a pair
        // that differs in one column is allowed (proves the build used the same
        // composite encoding as the insert-time maintainer).
        let dup = client
            .batch_execute("INSERT INTO uniq2 VALUES (1, 1)")
            .await
            .expect_err("(1,1) duplicates an existing composite key");
        assert_eq!(sqlstate(&dup), "23505", "expected 23505, got {dup}");
        client
            .batch_execute("INSERT INTO uniq2 VALUES (1, 3)")
            .await
            .expect("(1,3) is a distinct composite key — allowed");

        shutdown(running).await;
    }

    let running = start_persistent_server(data_dir.path(), "txddl_uniq2_a2").await;
    let count = running
        .client
        .query_one("SELECT count(*) FROM uniq2", &[])
        .await
        .expect("rows after restart")
        .get::<_, i64>(0);
    assert_eq!(count, 4, "the four committed rows survive restart");
    let dup = running
        .client
        .batch_execute("INSERT INTO uniq2 VALUES (2, 1)")
        .await
        .expect_err("post-restart composite duplicate must violate the rebuilt UNIQUE");
    assert_eq!(
        sqlstate(&dup),
        "23505",
        "expected 23505 post-restart, got {dup}"
    );
    shutdown(running).await;
}

// ───────────────────────────── M2 #8 (extended path) ─────────────────────────────
// The deferred build holds on the extended (Parse/Bind/Execute) path too: an
// in-txn PRIMARY KEY CREATE + parametrized INSERTs committed via the extended
// path build a working index, and a post-commit prepared duplicate fails 23505.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extended_path_in_txn_create_table_primary_key_commits_working_index() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_pk_ext").await;
    let client = &running.client;

    client.batch_execute("BEGIN").await.expect("begin");
    // `execute` issues Parse/Bind/Execute (extended path).
    client
        .execute("CREATE TABLE pk_ext (id INT PRIMARY KEY)", &[])
        .await
        .expect("extended in-txn CREATE TABLE … PRIMARY KEY");
    client
        .execute("INSERT INTO pk_ext VALUES ($1)", &[&5_i32])
        .await
        .expect("parametrized insert into the unbuilt-index table");
    client
        .batch_execute("COMMIT")
        .await
        .expect("commit builds the index");

    // The committed index enforces uniqueness against the row inserted in-txn.
    let dup = client
        .execute("INSERT INTO pk_ext VALUES ($1)", &[&5_i32])
        .await
        .expect_err("re-inserting the existing key must violate the built PK index");
    assert_eq!(sqlstate(&dup), "23505", "expected 23505, got {dup}");

    // A distinct key is accepted.
    client
        .execute("INSERT INTO pk_ext VALUES ($1)", &[&6_i32])
        .await
        .expect("a distinct key is accepted");
    let rows = client
        .query("SELECT id FROM pk_ext ORDER BY id", &[])
        .await
        .expect("query committed rows");
    let ids: Vec<i32> = rows.iter().map(|r| r.get::<_, i32>(0)).collect();
    assert_eq!(ids, vec![5, 6]);

    shutdown(running).await;
}
