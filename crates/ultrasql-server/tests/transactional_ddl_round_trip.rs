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
    // A plain RESTRICT DROP TABLE is now SUPPORTED in-txn (milestone 5) and is
    // covered by the M5 battery below; only the out-of-scope DROP forms must
    // still reject. CASCADE expands into a transitive closure of dependents the
    // negative-mask overlay cannot model; a sequence-owning (SERIAL) table emits
    // an unconditionally-replayed `SequenceOp::Drop` WAL; a view-dependent table
    // touches non-MVCC view runtime sidecars.
    assert_ddl_rejected_in_txn(
        "txddl_reg_drop_cascade",
        &["CREATE TABLE reg_drop_c (id INT)"],
        "DROP TABLE reg_drop_c CASCADE",
    )
    .await;
    assert_ddl_rejected_in_txn(
        "txddl_reg_drop_serial",
        &["CREATE TABLE reg_drop_s (id SERIAL)"],
        "DROP TABLE reg_drop_s",
    )
    .await;
    assert_ddl_rejected_in_txn(
        "txddl_reg_drop_view",
        &[
            "CREATE TABLE reg_drop_v (id INT)",
            "CREATE VIEW reg_drop_vv AS SELECT id FROM reg_drop_v",
        ],
        "DROP TABLE reg_drop_v",
    )
    .await;
    // A plain B-tree CREATE INDEX is now SUPPORTED in-txn (milestone 3) and is
    // covered by the M3 battery below; only the out-of-scope index FORMS must
    // still reject. CONCURRENTLY is a multi-transaction protocol; a hash index
    // and an expression index both write the non-MVCC RuntimeIndexMetadata
    // sidecar the overlay cannot roll back.
    assert_ddl_rejected_in_txn(
        "txddl_reg_index_conc",
        &["CREATE TABLE reg_idx_c (id INT)"],
        "CREATE INDEX CONCURRENTLY reg_idx_c_ix ON reg_idx_c (id)",
    )
    .await;
    assert_ddl_rejected_in_txn(
        "txddl_reg_index_hash",
        &["CREATE TABLE reg_idx_h (id INT)"],
        "CREATE INDEX reg_idx_h_ix ON reg_idx_h USING hash (id)",
    )
    .await;
    assert_ddl_rejected_in_txn(
        "txddl_reg_index_expr",
        &["CREATE TABLE reg_idx_e (id INT)"],
        "CREATE INDEX reg_idx_e_ix ON reg_idx_e ((id + 1))",
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

// Battery #9 (atomicity): an in-txn `CREATE TABLE` + `COPY` that is then
// ROLLBACK'd must leave neither the table nor the COPYed rows. Pre-fix the COPY
// opened its OWN autocommit txn and durably committed its rows, so the rows
// outlived the ROLLBACK (an ACID violation) even though the table itself was
// discarded — a row surviving ROLLBACK is the bug this guards.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_into_in_txn_created_table_is_discarded_on_rollback() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_copy_rb").await;
    let client = &running.client;

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("CREATE TABLE copy_rb_t (id INT NOT NULL, v TEXT)")
        .await
        .expect("in-txn create");

    let sink = client
        .copy_in::<_, Bytes>("COPY copy_rb_t FROM STDIN")
        .await
        .expect("COPY into self-created in-txn table establishes");
    futures::pin_mut!(sink);
    sink.as_mut()
        .send(Bytes::from_static(b"1\ta\n2\tb\n3\tc\n"))
        .await
        .expect("send CopyData");
    let copied = sink.finish().await.expect("finish copy_in");
    assert_eq!(copied, 3, "COPY reports three rows ingested in-txn");

    // Visible to this session before the rollback (self-visibility). Use a
    // column scan (not COUNT(*)) so the aggregate cache is not seeded with the
    // uncommitted count.
    let rows = client
        .query("SELECT id FROM copy_rb_t ORDER BY id", &[])
        .await
        .expect("self scan before rollback");
    assert_eq!(rows.len(), 3);

    client.batch_execute("ROLLBACK").await.expect("rollback");

    // The table is gone (DDL rolled back) AND its COPYed rows did not survive.
    let err = client
        .query("SELECT count(*) FROM copy_rb_t", &[])
        .await
        .expect_err("rolled-back COPY table must be undefined");
    assert!(is_undefined_table(&err), "expected 42P01, got {err}");

    // Re-create the same table fresh (autocommit) and confirm it is empty — a
    // belt-and-braces check that no rows leaked into a same-named relation.
    client
        .batch_execute("CREATE TABLE copy_rb_t (id INT NOT NULL, v TEXT)")
        .await
        .expect("recreate after rollback");
    let rows = client
        .query("SELECT count(*) FROM copy_rb_t", &[])
        .await
        .expect("count fresh table");
    assert_eq!(
        rows[0].get::<_, i64>(0),
        0,
        "no COPY rows survived ROLLBACK"
    );

    // Fresh connection also sees nothing durable.
    let (client_b, b_handle) = connect_as(running.bound, "tester", "txddl_copy_rb_b").await;
    let rows = client_b
        .query("SELECT count(*) FROM copy_rb_t", &[])
        .await
        .expect("fresh connection sees empty recreated table");
    assert_eq!(rows[0].get::<_, i64>(0), 0);

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

// ═════════════════════════════════════════════════════════════════════════
// Milestone 3: transactional `CREATE INDEX` (plain B-tree) on an EXISTING,
// non-partitioned table already in the global catalog. The index is staged in
// the session overlay UNBUILT and built once at COMMIT over the existing rows
// under the user snapshot; ROLLBACK / crash-before-commit leaves no segment
// and no catalog entry. The corruption gates are M3 #2 (no segment on
// rollback), M3 #4 (crash), and M3 #6 (CREATE UNIQUE INDEX over duplicate rows
// → full rollback). Both the simple and extended paths are exercised.

/// EXPLAIN-ANALYZE text for `query` as a single joined string.
async fn explain_text(client: &tokio_postgres::Client, query: &str) -> String {
    client
        .query(&format!("EXPLAIN ANALYZE {query}"), &[])
        .await
        .expect("explain analyze")
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n")
}

// ───────────────────────────── M3 #1 ─────────────────────────────
// COMMIT publishes a working index: present on a 2nd connection, the issuer's
// EXPLAIN uses it, a post-commit duplicate on a UNIQUE index fails 23505, and
// it survives restart BUILT (a post-restart duplicate still fails 23505).

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_txn_create_index_commits_working_index() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "txddl_ci_commit_a").await;
        let client = &running.client;

        // Existing table with rows, created + committed BEFORE the in-txn index.
        client
            .batch_execute("CREATE TABLE ci_t (id INT NOT NULL, v TEXT)")
            .await
            .expect("create existing table");
        client
            .batch_execute("INSERT INTO ci_t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            .await
            .expect("seed existing rows");

        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("CREATE UNIQUE INDEX ci_ix ON ci_t (id)")
            .await
            .expect("in-txn CREATE UNIQUE INDEX is accepted (milestone 3)");
        client.batch_execute("COMMIT").await.expect("commit");

        // The issuer's EXPLAIN uses the committed index.
        let txt = explain_text(client, "SELECT v FROM ci_t WHERE id = 2").await;
        assert!(
            txt.contains("Index Decision: selected ci_ix on ci_t.id"),
            "issuer EXPLAIN must use the committed index, got: {txt}"
        );

        // Visible to a 2nd connection, probe-able, and uniqueness enforced.
        let (client_b, b_handle) = connect_as(running.bound, "tester", "txddl_ci_commit_b").await;
        let row = client_b
            .query_one("SELECT v FROM ci_t WHERE id = 3", &[])
            .await
            .expect("2nd connection index lookup");
        assert_eq!(row.get::<_, &str>(0), "c");
        let dup = client_b
            .batch_execute("INSERT INTO ci_t VALUES (1, 'dup')")
            .await
            .expect_err("re-inserting an existing key must violate the committed UNIQUE index");
        assert_eq!(sqlstate(&dup), "23505", "expected 23505, got {dup}");

        drop(client_b);
        let _ = b_handle.await;
        shutdown(running).await;
    }

    // Survives restart BUILT: the index still rejects a duplicate (durable
    // root_block was corrected from INVALID to the built tree).
    let running = start_persistent_server(data_dir.path(), "txddl_ci_commit_a2").await;
    let count = running
        .client
        .query_one("SELECT count(*) FROM ci_t", &[])
        .await
        .expect("rows after restart")
        .get::<_, i64>(0);
    assert_eq!(count, 3, "all three rows survive restart");
    let dup = running
        .client
        .batch_execute("INSERT INTO ci_t VALUES (2, 'x')")
        .await
        .expect_err("post-restart duplicate must violate the rebuilt index");
    assert_eq!(
        sqlstate(&dup),
        "23505",
        "post-restart duplicate must report unique_violation (index rebuilt BUILT), got {dup}"
    );
    shutdown(running).await;
}

// ───────────────────────────── M3 #2 (corruption gate) ─────────────────────────────
// ROLLBACK reverts: the index is absent for the issuer, a concurrent connection
// B never saw it, restart leaves it absent, and NO btree segment was created.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rollback_in_txn_create_index_reverts_and_leaves_no_segment() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    // The existing table's heap segment is materialized by the seed INSERT, so
    // capture the set of user segments BEFORE the in-txn index and assert the
    // ROLLBACK adds none (i.e. no NEW index segment).
    let segments_before;
    {
        let running = start_persistent_server(data_dir.path(), "txddl_ci_rb_a").await;
        let client = &running.client;
        let (client_b, b_handle) = connect_as(running.bound, "tester", "txddl_ci_rb_b").await;

        client
            .batch_execute("CREATE TABLE ci_rb (id INT NOT NULL)")
            .await
            .expect("create existing table");
        client
            .batch_execute("INSERT INTO ci_rb VALUES (1), (2), (3)")
            .await
            .expect("seed rows (materializes the table heap segment)");
        segments_before = user_relation_segments(data_dir.path());

        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("CREATE INDEX ci_rb_ix ON ci_rb (id)")
            .await
            .expect("in-txn create index");

        // B never sees the uncommitted index (its plan does not reference it).
        let txt_b = explain_text(&client_b, "SELECT id FROM ci_rb WHERE id = 2").await;
        assert!(
            !txt_b.contains("ci_rb_ix"),
            "connection B must not see the uncommitted index, got: {txt_b}"
        );

        client.batch_execute("ROLLBACK").await.expect("rollback");

        // The index is gone for the issuer (its plan no longer references it).
        let txt = explain_text(client, "SELECT id FROM ci_rb WHERE id = 2").await;
        assert!(
            !txt.contains("ci_rb_ix"),
            "rolled-back index must be invisible to the issuer, got: {txt}"
        );
        assert!(
            !running
                .server
                .catalog_snapshot()
                .indexes
                .contains_key("ci_rb_ix"),
            "global snapshot must not carry the rolled-back index",
        );

        // NO new btree segment: the deferred build never ran, so the only user
        // segment is the pre-existing table heap.
        assert_eq!(
            user_relation_segments(data_dir.path()),
            segments_before,
            "ROLLBACK of CREATE INDEX must not leave a new base/<index-oid> segment",
        );

        drop(client_b);
        let _ = b_handle.await;
        shutdown(running).await;
    }

    // Restart-clean: index still absent, no new segment, the same name can be
    // created cleanly afterward.
    let running = start_persistent_server(data_dir.path(), "txddl_ci_rb_a2").await;
    assert!(
        !running
            .server
            .catalog_snapshot()
            .indexes
            .contains_key("ci_rb_ix"),
        "rolled-back index must not resurrect after restart",
    );
    let count = running
        .client
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'ci_rb_ix'",
            &[],
        )
        .await
        .expect("pg_class probe")
        .get::<_, i64>(0);
    assert_eq!(
        count, 0,
        "no durable pg_class row for the rolled-back index"
    );
    // Recreating it (autocommit) works and the table data is intact.
    running
        .client
        .batch_execute("CREATE UNIQUE INDEX ci_rb_ix ON ci_rb (id)")
        .await
        .expect("recreate index after rollback works");
    let dup = running
        .client
        .batch_execute("INSERT INTO ci_rb VALUES (1)")
        .await
        .expect_err("recreated index enforces uniqueness over the intact rows");
    assert_eq!(sqlstate(&dup), "23505", "expected 23505, got {dup}");
    shutdown(running).await;
}

// ───────────────────────────── M3 #3 ─────────────────────────────
// Self-sees / others-don't mid-txn: after CREATE INDEX in-txn the issuer's
// EXPLAIN can use the index; connection B's plan on the same table never
// references it (isolation, before any COMMIT).

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_txn_create_index_self_sees_others_dont() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_ci_iso_a").await;
    let (client_b, b_handle) = connect_as(running.bound, "tester", "txddl_ci_iso_b").await;

    running
        .client
        .batch_execute("CREATE TABLE ci_iso (id INT NOT NULL)")
        .await
        .expect("create table");
    running
        .client
        .batch_execute("INSERT INTO ci_iso VALUES (10), (20), (30)")
        .await
        .expect("seed rows");

    running.client.batch_execute("BEGIN").await.expect("begin");
    running
        .client
        .batch_execute("CREATE INDEX ci_iso_ix ON ci_iso (id)")
        .await
        .expect("in-txn create index");

    // The issuer can use the (pending) index.
    let txt_a = explain_text(&running.client, "SELECT id FROM ci_iso WHERE id = 20").await;
    assert!(
        txt_a.contains("Index Decision: selected ci_iso_ix on ci_iso.id"),
        "issuer must see its own pending index, got: {txt_a}"
    );
    // B does not — its plan on the same table never references the index.
    let txt_b = explain_text(&client_b, "SELECT id FROM ci_iso WHERE id = 20").await;
    assert!(
        !txt_b.contains("ci_iso_ix"),
        "connection B must not reference the uncommitted index, got: {txt_b}"
    );

    running
        .client
        .batch_execute("COMMIT")
        .await
        .expect("commit");

    drop(client_b);
    let _ = b_handle.await;
    shutdown(running).await;
}

// ───────────────────────────── M3 #4 (corruption gate) ─────────────────────────────
// Crash before COMMIT: after CREATE INDEX ran but before COMMIT, drop the
// server. Restart: no index, no orphaned segment (the deferred build never
// ran). Symmetric: a server that COMMITted has the index present + probe-able
// after restart.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_before_commit_in_txn_create_index_does_not_resurrect() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    let segments_before;
    {
        let running = start_persistent_server(data_dir.path(), "txddl_ci_crash_a").await;
        running
            .client
            .batch_execute("CREATE TABLE ci_crash (id INT NOT NULL)")
            .await
            .expect("create table");
        running
            .client
            .batch_execute("INSERT INTO ci_crash VALUES (1), (2)")
            .await
            .expect("seed rows");
        segments_before = user_relation_segments(data_dir.path());

        running.client.batch_execute("BEGIN").await.expect("begin");
        running
            .client
            .batch_execute("CREATE INDEX ci_crash_ix ON ci_crash (id)")
            .await
            .expect("in-txn create index (pg_index rows under user xid, NO commit)");
        // Drop the server WITHOUT COMMIT/ROLLBACK: the user xid has no commit
        // record and the deferred build never ran.
        shutdown(running).await;
    }

    let running = start_persistent_server(data_dir.path(), "txddl_ci_crash_a2").await;
    assert!(
        !running
            .server
            .catalog_snapshot()
            .indexes
            .contains_key("ci_crash_ix"),
        "crash-before-commit index must not resurrect in the catalog snapshot",
    );
    // The table itself (committed before the txn) is intact.
    let count = running
        .client
        .query_one("SELECT count(*) FROM ci_crash", &[])
        .await
        .expect("table intact")
        .get::<_, i64>(0);
    assert_eq!(count, 2, "the pre-existing table and rows are intact");
    // No durable pg_class row for the index, and no orphaned index segment.
    let idx_rows = running
        .client
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'ci_crash_ix'",
            &[],
        )
        .await
        .expect("pg_class probe")
        .get::<_, i64>(0);
    assert_eq!(idx_rows, 0, "no durable pg_class row for the crashed index");
    assert_eq!(
        user_relation_segments(data_dir.path()),
        segments_before,
        "crash-before-commit must leave no new index segment after restart",
    );
    shutdown(running).await;
}

// ───────────────────────────── M3 #5 ─────────────────────────────
// Concurrent serialized: A holds AccessExclusive on the table via an in-txn
// CREATE INDEX; B's concurrent CREATE INDEX on the same table fails 40001
// immediately (non-blocking try_acquire). After A commits, the index is
// singular and usable.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_in_txn_create_index_same_table_serializes() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_ci_race_a").await;
    let (client_b, b_handle) = connect_as(running.bound, "tester", "txddl_ci_race_b").await;

    running
        .client
        .batch_execute("CREATE TABLE ci_race (id INT NOT NULL)")
        .await
        .expect("create table");
    running
        .client
        .batch_execute("INSERT INTO ci_race VALUES (1), (2), (3)")
        .await
        .expect("seed rows");

    running
        .client
        .batch_execute("BEGIN")
        .await
        .expect("A begin");
    running
        .client
        .batch_execute("CREATE INDEX ci_race_ix_a ON ci_race (id)")
        .await
        .expect("A in-txn create index takes AccessExclusive on the table");

    // B's concurrent in-txn CREATE INDEX on the SAME table fails immediately.
    client_b.batch_execute("BEGIN").await.expect("B begin");
    let err = client_b
        .batch_execute("CREATE INDEX ci_race_ix_b ON ci_race (id)")
        .await
        .expect_err("B's same-table CREATE INDEX must fail while A holds the lock");
    assert_eq!(
        sqlstate(&err),
        "40001",
        "concurrent same-table CREATE INDEX must report serialization_failure, got {err}"
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

    // A's index is present and enforces nothing extra (non-unique); B can now
    // create a different index without contention.
    assert!(
        running
            .server
            .catalog_snapshot()
            .indexes
            .contains_key("ci_race_ix_a"),
        "A's committed index is present",
    );
    client_b
        .batch_execute("CREATE INDEX ci_race_ix_b ON ci_race (id)")
        .await
        .expect("B creates its index after A committed (no torn index set)");

    drop(client_b);
    let _ = b_handle.await;
    shutdown(running).await;
}

// ───────────────────────────── M3 #6 (THE CRUX — corruption gate) ─────────────────────────────
// CREATE UNIQUE INDEX over existing rows that ARE unique → COMMIT builds +
// enforces. Over existing DUPLICATE rows → COMMIT fails 23505, full rollback:
// no index, no segment, table unchanged — verified from a 2nd connection and
// after restart.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_txn_create_unique_index_over_duplicates_fails_23505_full_rollback() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    // ── Part A: unique rows → builds and enforces. ──
    {
        let running = start_persistent_server(data_dir.path(), "txddl_ci_uniq_ok_a").await;
        let client = &running.client;
        client
            .batch_execute("CREATE TABLE ci_uok (id INT NOT NULL)")
            .await
            .expect("create table");
        client
            .batch_execute("INSERT INTO ci_uok VALUES (1), (2), (3)")
            .await
            .expect("seed unique rows");
        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("CREATE UNIQUE INDEX ci_uok_ix ON ci_uok (id)")
            .await
            .expect("in-txn create unique index over unique rows");
        client
            .batch_execute("COMMIT")
            .await
            .expect("commit builds the unique index over the unique rows");
        let dup = client
            .batch_execute("INSERT INTO ci_uok VALUES (2)")
            .await
            .expect_err("post-commit duplicate must violate the built unique index");
        assert_eq!(sqlstate(&dup), "23505", "expected 23505, got {dup}");
        shutdown(running).await;
    }

    // ── Part B: duplicate rows → COMMIT fails 23505, full rollback. ──
    //
    // Note on segments: the deferred build calls `BTree::create` (allocating
    // the index segment) BEFORE scanning the rows that surface the duplicate,
    // so a `base/<index_oid>` orphan segment may exist on disk after the abort.
    // That is the same bounded, MVCC-safe orphan-file leak the engine tolerates
    // for any aborted xid (the index's catalog rows ride the aborted xid →
    // invisible; the visibility-filtered bootstrap never resurrects the index).
    // The correctness gate is therefore catalog/query/restart ABSENCE, not a
    // literal segment count (that strict check holds only for the no-build case,
    // M3 #2 above).
    {
        let running = start_persistent_server(data_dir.path(), "txddl_ci_uniq_dup_a").await;
        let client = &running.client;
        client
            .batch_execute("CREATE TABLE ci_udup (id INT NOT NULL)")
            .await
            .expect("create table");
        // Existing rows contain a DUPLICATE key.
        client
            .batch_execute("INSERT INTO ci_udup VALUES (1), (2), (2), (3)")
            .await
            .expect("seed rows with a duplicate key");

        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("CREATE UNIQUE INDEX ci_udup_ix ON ci_udup (id)")
            .await
            .expect("in-txn create unique index is accepted (build deferred to COMMIT)");
        // COMMIT runs the deferred build, finds the existing duplicate, fails
        // 23505, and rolls back the whole transaction.
        let err = client
            .batch_execute("COMMIT")
            .await
            .expect_err("COMMIT must fail when the deferred unique-index build hits a duplicate");
        assert_eq!(
            sqlstate(&err),
            "23505",
            "duplicate at the COMMIT build must report unique_violation, got {err}"
        );

        // Full rollback: the index is absent; the table is unchanged.
        assert!(
            !running
                .server
                .catalog_snapshot()
                .indexes
                .contains_key("ci_udup_ix"),
            "global snapshot must not carry the aborted index",
        );
        let txt = explain_text(client, "SELECT id FROM ci_udup WHERE id = 2").await;
        assert!(
            !txt.contains("ci_udup_ix"),
            "the aborted index must not be referenced, got: {txt}"
        );
        // The table's rows are unchanged (all four, including the duplicate).
        let count = client
            .query_one("SELECT count(*) FROM ci_udup", &[])
            .await
            .expect("table unchanged")
            .get::<_, i64>(0);
        assert_eq!(count, 4, "the table is unchanged after the aborted index");

        // A 2nd connection also never sees the index.
        let (client_b, b_handle) = connect_as(running.bound, "tester", "txddl_ci_uniq_dup_b").await;
        let txt_b = explain_text(&client_b, "SELECT id FROM ci_udup WHERE id = 2").await;
        assert!(
            !txt_b.contains("ci_udup_ix"),
            "2nd connection must not see the aborted index, got: {txt_b}"
        );
        drop(client_b);
        let _ = b_handle.await;
        shutdown(running).await;
    }

    // After restart: the index left nothing behind; the table is intact; the
    // name can be reused once the duplicate is removed.
    let running = start_persistent_server(data_dir.path(), "txddl_ci_uniq_dup_a2").await;
    assert!(
        !running
            .server
            .catalog_snapshot()
            .indexes
            .contains_key("ci_udup_ix"),
        "aborted index must not resurrect after restart",
    );
    let count = running
        .client
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'ci_udup_ix'",
            &[],
        )
        .await
        .expect("pg_class probe")
        .get::<_, i64>(0);
    assert_eq!(count, 0, "no durable pg_class row for the aborted index");
    let rows = running
        .client
        .query_one("SELECT count(*) FROM ci_udup", &[])
        .await
        .expect("rows after restart")
        .get::<_, i64>(0);
    assert_eq!(rows, 4, "the four rows survive restart unchanged");
    shutdown(running).await;
}

// ───────────────────────────── M3 #7 (UNBLOCKED: same-txn table+index) ─────────────────────────────
// In-txn CREATE INDEX on a SAME-TXN-created table — the M3 scope boundary — is
// now LIFTED. A non-unique index over a freshly created table accumulates and
// builds at COMMIT (resolving its target against the overlay's `created_tables`,
// not the committed snapshot). Self sees the index inside the same txn; after
// ROLLBACK both the table and index are gone. (Out-of-scope index forms — hash /
// expression / CONCURRENTLY — and the other DDL classes still reject; covered by
// `out_of_scope_ddl_still_rejected_in_transaction`.)

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_txn_create_index_on_same_txn_created_table_now_accumulates() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_ci_sametxn").await;
    let client = &running.client;

    // Accumulate-and-COMMIT: a non-unique index over a same-txn table.
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("CREATE TABLE ci_same (id INT NOT NULL)")
        .await
        .expect("in-txn create table (overlay holds the created table)");
    client
        .batch_execute("INSERT INTO ci_same VALUES (1), (2), (2), (3)")
        .await
        .expect("in-txn rows (duplicates allowed: non-unique index)");
    client
        .batch_execute("CREATE INDEX ci_same_ix ON ci_same (id)")
        .await
        .expect("CREATE INDEX on a same-txn-created table now accumulates");
    client
        .batch_execute("COMMIT")
        .await
        .expect("the non-unique index builds over the in-txn rows at COMMIT");
    assert!(
        running
            .server
            .catalog_snapshot()
            .indexes
            .contains_key("ci_same_ix"),
        "the same-txn index must be published after commit",
    );
    // The index carries the (duplicate-tolerant) rows: a key lookup resolves.
    let count = client
        .query_one("SELECT count(*) FROM ci_same WHERE id = 2", &[])
        .await
        .expect("index lookup on the same-txn-created table")
        .get::<_, i64>(0);
    assert_eq!(count, 2, "both id=2 rows are present (non-unique index)");

    // Accumulate-and-ROLLBACK: both the table and index vanish.
    client.batch_execute("BEGIN").await.expect("begin rb");
    client
        .batch_execute("CREATE TABLE ci_rb (id INT NOT NULL)")
        .await
        .expect("create table for rollback");
    client
        .batch_execute("CREATE INDEX ci_rb_ix ON ci_rb (id)")
        .await
        .expect("index on the same-txn table accumulates");
    client.batch_execute("ROLLBACK").await.expect("rollback");
    let err = client
        .query("SELECT id FROM ci_rb", &[])
        .await
        .expect_err("the rolled-back same-txn table must be gone");
    assert!(is_undefined_table(&err), "expected 42P01, got {err}");
    assert!(
        !running
            .server
            .catalog_snapshot()
            .indexes
            .contains_key("ci_rb_ix"),
        "the rolled-back same-txn index must not be in the snapshot",
    );

    shutdown(running).await;
}

// PREPARE TRANSACTION of a txn carrying an in-txn CREATE INDEX must reject
// (the deferred build has no two-phase publish hook), mirroring the CREATE
// TABLE 2PC reject. After ROLLBACK the index left nothing behind.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_transaction_with_in_txn_create_index_is_rejected() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_ci_2pc").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE ci_2pc (id INT NOT NULL)")
        .await
        .expect("create table");
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("CREATE INDEX ci_2pc_ix ON ci_2pc (id)")
        .await
        .expect("in-txn create index");
    let err = client
        .batch_execute("PREPARE TRANSACTION 'txddl-ci-gid'")
        .await
        .expect_err("PREPARE of a txn carrying CREATE INDEX must be rejected");
    assert_eq!(
        sqlstate(&err),
        "0A000",
        "PREPARE with in-txn CREATE INDEX must be feature_not_supported, got {err}"
    );
    client.batch_execute("ROLLBACK").await.expect("rollback");

    assert!(
        !running
            .server
            .catalog_snapshot()
            .indexes
            .contains_key("ci_2pc_ix"),
        "rejected-PREPARE index must not be in the snapshot",
    );

    shutdown(running).await;
}

// ───────────────────────────── M3 #8 (extended path) ─────────────────────────────
// The deferred build holds on the extended (Parse/Bind/Execute) path too.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extended_path_in_txn_create_index_commits_working_index() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_ci_ext").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE ci_ext (id INT NOT NULL)")
        .await
        .expect("create table");
    client
        .execute("INSERT INTO ci_ext VALUES ($1)", &[&7_i32])
        .await
        .expect("seed via extended path");
    client
        .execute("INSERT INTO ci_ext VALUES ($1)", &[&8_i32])
        .await
        .expect("seed via extended path");

    client.batch_execute("BEGIN").await.expect("begin");
    // `execute` issues Parse/Bind/Execute (extended path).
    client
        .execute("CREATE UNIQUE INDEX ci_ext_ix ON ci_ext (id)", &[])
        .await
        .expect("extended in-txn CREATE UNIQUE INDEX");
    client
        .batch_execute("COMMIT")
        .await
        .expect("commit builds the index");

    // The committed index enforces uniqueness against the existing rows.
    let dup = client
        .execute("INSERT INTO ci_ext VALUES ($1)", &[&7_i32])
        .await
        .expect_err("re-inserting an existing key must violate the built unique index");
    assert_eq!(sqlstate(&dup), "23505", "expected 23505, got {dup}");
    client
        .execute("INSERT INTO ci_ext VALUES ($1)", &[&9_i32])
        .await
        .expect("a distinct key is accepted");

    shutdown(running).await;
}

// ═════════════════════════════════════════════════════════════════════════
// Transactional-DDL multi-statement ACCUMULATION (lifting the M3 one-schema-
// statement-per-transaction limit). The session catalog overlay
// (`Session::pending_catalog_ddl`) now ACCUMULATES multiple in-txn
// `CREATE TABLE` / `CREATE INDEX` statements: every producer APPENDS to the
// overlay (`created_tables` / `extra_indexes` / `staged` Vecs) instead of
// clobbering it. All accumulated statements commit atomically — a single
// COMMIT publish, or, if ANY deferred index build hits a duplicate, the WHOLE
// transaction rolls back (every table, row, and index rides the one aborted
// user xid, hidden by the visibility-filtered bootstrap on restart). The four
// `*_rejects_second` cases below are CONVERTED from "second statement rejected
// 0A000" to "both accumulate and commit / both roll back", and a new battery
// (ACC #1..#7) covers the multi-op matrix — esp. the CRUX (a duplicate in a
// later build aborts everything) and the previously-blocked combo
// (CREATE INDEX on a table created earlier in the same transaction).

// ───────────────────────────── ACC: CREATE INDEX then CREATE TABLE (was rejected) ─────────────────────────────
// BEGIN; CREATE UNIQUE INDEX ix ON existing_t; CREATE TABLE u; COMMIT.
// Both now accumulate and commit atomically: the index is BUILT and enforcing,
// the new table exists. After a separate ROLLBACK run, neither survives.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_txn_create_index_then_create_table_accumulates_and_commits() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "txddl_acc_ci_ct_a").await;
        let client = &running.client;

        client
            .batch_execute("CREATE TABLE oc_existing (id INT NOT NULL)")
            .await
            .expect("create existing table");
        client
            .batch_execute("INSERT INTO oc_existing VALUES (1), (2), (3)")
            .await
            .expect("seed unique rows");

        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("CREATE UNIQUE INDEX oc_ix ON oc_existing (id)")
            .await
            .expect("first op: CREATE UNIQUE INDEX accumulates into the overlay");
        client
            .batch_execute("CREATE TABLE oc_u (id INT)")
            .await
            .expect("second op: CREATE TABLE accumulates (no longer rejected)");
        client
            .batch_execute("COMMIT")
            .await
            .expect("both schema changes commit atomically");

        // The unique index is BUILT and enforcing.
        let dup = client
            .batch_execute("INSERT INTO oc_existing VALUES (1)")
            .await
            .expect_err("duplicate must violate the built unique index");
        assert_eq!(sqlstate(&dup), "23505", "expected 23505, got {dup}");
        // The new table exists and is usable.
        client
            .batch_execute("INSERT INTO oc_u VALUES (7)")
            .await
            .expect("the in-txn-created table is usable after commit");

        shutdown(running).await;
    }

    // After restart both survive: the index still enforces uniqueness (rebuilt
    // BUILT) and the second table is present.
    let running = start_persistent_server(data_dir.path(), "txddl_acc_ci_ct_a2").await;
    let client = &running.client;
    let dup = client
        .batch_execute("INSERT INTO oc_existing VALUES (2)")
        .await
        .expect_err("post-restart duplicate must violate the rebuilt unique index");
    assert_eq!(sqlstate(&dup), "23505", "expected 23505, got {dup}");
    let row = client
        .query_one("SELECT count(*) FROM oc_u", &[])
        .await
        .expect("the committed second table is present after restart")
        .get::<_, i64>(0);
    assert_eq!(row, 1, "the in-txn-created table's row survives restart");
    shutdown(running).await;
}

// ───────────────────────────── ACC: two CREATE INDEX (was rejected) ─────────────────────────────
// BEGIN; CREATE INDEX a ON t1; CREATE UNIQUE INDEX b ON t2; COMMIT.
// Both now BUILD and enforce after COMMIT; both survive restart BUILT.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_txn_two_create_index_accumulates_and_builds_both() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "txddl_acc_two_ci_a").await;
        let client = &running.client;

        client
            .batch_execute("CREATE TABLE oc_t1 (id INT NOT NULL)")
            .await
            .expect("create t1");
        client
            .batch_execute("CREATE TABLE oc_t2 (id INT NOT NULL)")
            .await
            .expect("create t2");
        client
            .batch_execute("INSERT INTO oc_t1 VALUES (1), (2)")
            .await
            .expect("seed t1");
        client
            .batch_execute("INSERT INTO oc_t2 VALUES (1), (2)")
            .await
            .expect("seed t2 (distinct keys)");

        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("CREATE INDEX oc_a ON oc_t1 (id)")
            .await
            .expect("first CREATE INDEX accumulates");
        client
            .batch_execute("CREATE UNIQUE INDEX oc_b ON oc_t2 (id)")
            .await
            .expect("second CREATE INDEX accumulates (no longer rejected)");
        client
            .batch_execute("COMMIT")
            .await
            .expect("both indexes build atomically at COMMIT");

        // Both indexes reached the global snapshot.
        for ix in ["oc_a", "oc_b"] {
            assert!(
                running.server.catalog_snapshot().indexes.contains_key(ix),
                "index {ix} must be published after commit",
            );
        }
        // The unique one enforces uniqueness.
        let dup = client
            .batch_execute("INSERT INTO oc_t2 VALUES (1)")
            .await
            .expect_err("duplicate must violate the built unique index oc_b");
        assert_eq!(sqlstate(&dup), "23505", "expected 23505, got {dup}");

        shutdown(running).await;
    }

    // Both survive restart BUILT: the unique index still enforces uniqueness.
    let running = start_persistent_server(data_dir.path(), "txddl_acc_two_ci_a2").await;
    let dup = running
        .client
        .batch_execute("INSERT INTO oc_t2 VALUES (2)")
        .await
        .expect_err("post-restart duplicate must violate the rebuilt unique index");
    assert_eq!(sqlstate(&dup), "23505", "expected 23505, got {dup}");
    shutdown(running).await;
}

// ───────────────────────────── ACC: CREATE TABLE then CREATE INDEX on it (was rejected — THE UNBLOCKED COMBO) ─────────────────────────────
// BEGIN; CREATE TABLE t; INSERT INTO t …; CREATE INDEX ix ON t; COMMIT.
// The M3 same-txn-created-table reject is LIFTED: the index now builds over the
// rows the transaction inserted into the freshly created table (resolved at
// COMMIT against the overlay's `created_tables`, not the committed snapshot). A
// post-restart duplicate fails 23505 (built + durable + rebuilt BUILT).

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_txn_create_table_then_index_on_it_now_builds() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "txddl_acc_t_then_ix_a").await;
        let client = &running.client;

        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("CREATE TABLE oc_ti (id INT NOT NULL, v TEXT)")
            .await
            .expect("first op: in-txn create table");
        client
            .batch_execute("INSERT INTO oc_ti VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            .await
            .expect("in-txn rows into the freshly created table");
        client
            .batch_execute("CREATE UNIQUE INDEX oc_ti_ix ON oc_ti (id)")
            .await
            .expect("CREATE INDEX on a same-txn-created table now accumulates");
        client
            .batch_execute("COMMIT")
            .await
            .expect("the index builds over the in-txn rows at COMMIT");

        // The index resolves a specific key (it carries the in-txn rows).
        let row = client
            .query_one("SELECT v FROM oc_ti WHERE id = 2", &[])
            .await
            .expect("index lookup on the same-txn-created table")
            .get::<_, String>(0);
        assert_eq!(row, "b");
        // Uniqueness is enforced against an EXISTING key.
        let dup = client
            .batch_execute("INSERT INTO oc_ti VALUES (2, 'dup')")
            .await
            .expect_err("re-inserting an existing key must violate the built index");
        assert_eq!(sqlstate(&dup), "23505", "expected 23505, got {dup}");

        shutdown(running).await;
    }

    // After restart the table + its rows + the BUILT index all survive; a
    // post-restart duplicate fails 23505 (rebuilt BUILT, not resurrected
    // UNBUILT).
    let running = start_persistent_server(data_dir.path(), "txddl_acc_t_then_ix_a2").await;
    let rows = running
        .client
        .query("SELECT id FROM oc_ti ORDER BY id", &[])
        .await
        .expect("rows present after restart");
    let ids: Vec<i32> = rows.iter().map(|r| r.get::<_, i32>(0)).collect();
    assert_eq!(ids, vec![1, 2, 3], "all in-txn rows survive restart");
    let dup = running
        .client
        .batch_execute("INSERT INTO oc_ti VALUES (3, 'x')")
        .await
        .expect_err("post-restart duplicate must violate the rebuilt index");
    assert_eq!(
        sqlstate(&dup),
        "23505",
        "expected 23505 post-restart, got {dup}"
    );
    shutdown(running).await;
}

// ───────────────────────────── ACC #1: two CREATE TABLE accumulate + commit ─────────────────────────────
// BEGIN; CREATE TABLE a; CREATE TABLE b; COMMIT → both present (self, 2nd conn,
// restart).

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_txn_two_create_table_accumulates_and_commits() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "txddl_acc_two_ct_a").await;
        let client = &running.client;

        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("CREATE TABLE oc_first (id INT NOT NULL)")
            .await
            .expect("first CREATE TABLE accumulates");
        client
            .batch_execute("CREATE TABLE oc_second (id INT NOT NULL)")
            .await
            .expect("second CREATE TABLE accumulates (no longer rejected)");
        client
            .batch_execute("INSERT INTO oc_first VALUES (10)")
            .await
            .expect("insert into the first in-txn table");
        client
            .batch_execute("INSERT INTO oc_second VALUES (20)")
            .await
            .expect("insert into the second in-txn table");
        client
            .batch_execute("COMMIT")
            .await
            .expect("both tables commit atomically");

        // Self sees both.
        for (name, val) in [("oc_first", 10_i32), ("oc_second", 20)] {
            let got = client
                .query_one(&format!("SELECT id FROM {name}"), &[])
                .await
                .expect("self sees the committed table")
                .get::<_, i32>(0);
            assert_eq!(got, val, "{name} carries its row");
        }
        // A 2nd connection sees both.
        let (client_b, b_handle) = connect_as(running.bound, "tester", "txddl_acc_two_ct_b").await;
        for name in ["oc_first", "oc_second"] {
            client_b
                .query_one(&format!("SELECT count(*) FROM {name}"), &[])
                .await
                .expect("2nd connection sees the committed table");
        }
        drop(client_b);
        let _ = b_handle.await;

        shutdown(running).await;
    }

    // After restart both survive.
    let running = start_persistent_server(data_dir.path(), "txddl_acc_two_ct_a2").await;
    for (name, val) in [("oc_first", 10_i32), ("oc_second", 20)] {
        let got = running
            .client
            .query_one(&format!("SELECT id FROM {name}"), &[])
            .await
            .expect("committed table present after restart")
            .get::<_, i32>(0);
        assert_eq!(got, val, "{name} survives restart with its row");
    }
    shutdown(running).await;
}

// ───────────────────────────── ACC #2: two CREATE TABLE + ROLLBACK ─────────────────────────────
// BEGIN; CREATE TABLE a; CREATE TABLE b; ROLLBACK → both gone (self + global +
// restart-clean, no orphan segment).

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_txn_two_create_table_rollback_leaves_nothing() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "txddl_acc_two_ct_rb_a").await;
        let client = &running.client;

        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("CREATE TABLE rb_first (id INT NOT NULL)")
            .await
            .expect("first CREATE TABLE accumulates");
        client
            .batch_execute("CREATE TABLE rb_second (id INT NOT NULL)")
            .await
            .expect("second CREATE TABLE accumulates");
        client.batch_execute("ROLLBACK").await.expect("rollback");

        // Neither table exists for the issuer or the global snapshot.
        for name in ["rb_first", "rb_second"] {
            let err = client
                .query(&format!("SELECT id FROM {name}"), &[])
                .await
                .expect_err("rolled-back table absent");
            assert!(
                is_undefined_table(&err),
                "expected 42P01 for {name}, got {err}"
            );
            assert!(
                !running.server.catalog_snapshot().tables.contains_key(name),
                "global snapshot must not carry {name}",
            );
        }
        // No user-relation segment leaked (no INSERTs, lazy creation).
        assert_eq!(
            user_relation_segments(data_dir.path()),
            Vec::<String>::new(),
            "ROLLBACK of two CREATE TABLE must not leave a base/<user-oid> segment",
        );

        shutdown(running).await;
    }

    // After restart neither resurrects.
    let running = start_persistent_server(data_dir.path(), "txddl_acc_two_ct_rb_a2").await;
    for name in ["rb_first", "rb_second"] {
        assert!(
            !running.server.catalog_snapshot().tables.contains_key(name),
            "rolled-back table {name} must not resurrect after restart",
        );
        let err = running
            .client
            .query(&format!("SELECT id FROM {name}"), &[])
            .await
            .expect_err("rolled-back table absent after restart");
        assert!(
            is_undefined_table(&err),
            "expected 42P01 for {name}, got {err}"
        );
    }
    shutdown(running).await;
}

// ───────────────────────────── ACC #4 (THE CRUX — corruption gate) ─────────────────────────────
// BEGIN; CREATE TABLE t; INSERT duplicate; CREATE UNIQUE INDEX ON t; COMMIT →
// the deferred build hits a duplicate and fails 23505, rolling back the WHOLE
// transaction. The table, its rows, and the index are ALL absent (self, 2nd
// conn, restart). This is the all-or-nothing crux: a later op's build failure
// undoes the earlier op's CREATE TABLE.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_txn_create_table_insert_dup_then_unique_index_full_rollback() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    let dup_sqlstate;
    {
        let running = start_persistent_server(data_dir.path(), "txddl_acc_crux_a").await;
        let client = &running.client;

        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("CREATE TABLE crux_t (id INT NOT NULL)")
            .await
            .expect("in-txn create table");
        client
            .batch_execute("INSERT INTO crux_t VALUES (1), (1)")
            .await
            .expect("duplicate inserts succeed in-txn (index not yet built)");
        client
            .batch_execute("CREATE UNIQUE INDEX crux_ix ON crux_t (id)")
            .await
            .expect("CREATE INDEX accumulates (build deferred to COMMIT)");

        let err = client
            .batch_execute("COMMIT")
            .await
            .expect_err("COMMIT must fail when the deferred unique build hits a duplicate");
        dup_sqlstate = sqlstate(&err);
        assert_eq!(
            dup_sqlstate, "23505",
            "expected 23505 at COMMIT build, got {err}"
        );

        // FULL rollback: the table created EARLIER in the same txn is also gone.
        let err = client
            .query("SELECT id FROM crux_t", &[])
            .await
            .expect_err("the CREATE TABLE must be undone by the later build failure");
        assert!(is_undefined_table(&err), "expected 42P01, got {err}");
        assert!(
            !running
                .server
                .catalog_snapshot()
                .tables
                .contains_key("crux_t"),
            "global snapshot must not carry the aborted table",
        );
        assert!(
            !running
                .server
                .catalog_snapshot()
                .indexes
                .contains_key("crux_ix"),
            "global snapshot must not carry the aborted index",
        );

        // A 2nd connection never saw any of it.
        let (client_b, b_handle) = connect_as(running.bound, "tester", "txddl_acc_crux_b").await;
        let err = client_b
            .query("SELECT id FROM crux_t", &[])
            .await
            .expect_err("2nd connection must not see the aborted table");
        assert!(is_undefined_table(&err), "expected 42P01 for B, got {err}");
        drop(client_b);
        let _ = b_handle.await;

        shutdown(running).await;
    }
    assert_eq!(dup_sqlstate, "23505");

    // After restart nothing resurrects — no table row and no UNBUILT index row.
    let running = start_persistent_server(data_dir.path(), "txddl_acc_crux_a2").await;
    let client = &running.client;
    assert!(
        !running
            .server
            .catalog_snapshot()
            .tables
            .contains_key("crux_t"),
        "the aborted table must not resurrect after restart",
    );
    assert!(
        !running
            .server
            .catalog_snapshot()
            .indexes
            .contains_key("crux_ix"),
        "the aborted index must not resurrect after restart",
    );
    let table_rows = client
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'crux_t' AND relkind = 'r'",
            &[],
        )
        .await
        .expect("pg_class table probe")
        .get::<_, i64>(0);
    assert_eq!(
        table_rows, 0,
        "no durable pg_class row for the aborted table"
    );
    let idx_rows = client
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'crux_ix'",
            &[],
        )
        .await
        .expect("pg_class index probe")
        .get::<_, i64>(0);
    assert_eq!(idx_rows, 0, "no durable pg_class row for the aborted index");
    shutdown(running).await;
}

// ───────────────────────────── ACC #5: crash mid-multi-op does not resurrect ─────────────────────────────
// BEGIN; CREATE TABLE a; CREATE TABLE b; CREATE INDEX on b; <crash, no COMMIT>.
// Nothing resurrects after restart — and no UNBUILT pg_class index row survives.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_mid_multi_op_in_txn_ddl_does_not_resurrect() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "txddl_acc_crash_a").await;
        let client = &running.client;
        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("CREATE TABLE crash_a (id INT NOT NULL)")
            .await
            .expect("first create table");
        client
            .batch_execute("CREATE TABLE crash_b (id INT NOT NULL)")
            .await
            .expect("second create table");
        client
            .batch_execute("INSERT INTO crash_b VALUES (1), (2)")
            .await
            .expect("rows into crash_b");
        client
            .batch_execute("CREATE INDEX crash_b_ix ON crash_b (id)")
            .await
            .expect("create index accumulates (deferred build)");
        // Drop without COMMIT/ROLLBACK — the user xid has no commit marker.
        shutdown(running).await;
    }

    // Restart: nothing resurrects.
    let running = start_persistent_server(data_dir.path(), "txddl_acc_crash_a2").await;
    let client = &running.client;
    for name in ["crash_a", "crash_b"] {
        assert!(
            !running.server.catalog_snapshot().tables.contains_key(name),
            "crash-before-commit table {name} must not resurrect",
        );
        let err = client
            .query(&format!("SELECT id FROM {name}"), &[])
            .await
            .expect_err("crash-before-commit table absent after restart");
        assert!(
            is_undefined_table(&err),
            "expected 42P01 for {name}, got {err}"
        );
    }
    assert!(
        !running
            .server
            .catalog_snapshot()
            .indexes
            .contains_key("crash_b_ix"),
        "crash-before-commit index must not resurrect in the snapshot",
    );
    // No durable pg_class row for the never-committed index (no UNBUILT leak).
    let idx_rows = client
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'crash_b_ix'",
            &[],
        )
        .await
        .expect("pg_class index probe")
        .get::<_, i64>(0);
    assert_eq!(idx_rows, 0, "no UNBUILT pg_class index row may resurrect");
    shutdown(running).await;
}

// ───────────────────────────── ACC #6 (PARTIAL-BUILD — corruption gate) ─────────────────────────────
// BEGIN; CREATE TABLE a (ok); CREATE TABLE b; INSERT dup into b; CREATE UNIQUE
// INDEX on b; COMMIT → the LATER build fails 23505 → ALL-or-nothing: BOTH a and
// b are gone (no op commits durably when a subsequent build fails). This proves
// an earlier, perfectly-valid CREATE TABLE does not half-commit when a later
// statement's build aborts the transaction.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_txn_partial_build_failure_rolls_back_all_ops() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "txddl_acc_partial_a").await;
        let client = &running.client;

        client.batch_execute("BEGIN").await.expect("begin");
        // (a) A perfectly valid CREATE TABLE — would commit cleanly on its own.
        client
            .batch_execute("CREATE TABLE pb_a (id INT NOT NULL)")
            .await
            .expect("first table (valid)");
        client
            .batch_execute("INSERT INTO pb_a VALUES (100)")
            .await
            .expect("row into pb_a");
        // (b) A second table whose later unique-index build will fail.
        client
            .batch_execute("CREATE TABLE pb_b (id INT NOT NULL)")
            .await
            .expect("second table");
        client
            .batch_execute("INSERT INTO pb_b VALUES (5), (5)")
            .await
            .expect("duplicate rows into pb_b");
        client
            .batch_execute("CREATE UNIQUE INDEX pb_b_ix ON pb_b (id)")
            .await
            .expect("create unique index (build deferred to COMMIT)");

        let err = client
            .batch_execute("COMMIT")
            .await
            .expect_err("COMMIT must fail on the later duplicate build");
        assert_eq!(sqlstate(&err), "23505", "expected 23505, got {err}");

        // BOTH tables are gone — the valid first one did NOT half-commit.
        for name in ["pb_a", "pb_b"] {
            let err = client
                .query(&format!("SELECT id FROM {name}"), &[])
                .await
                .expect_err("all-or-nothing: every op rolls back");
            assert!(
                is_undefined_table(&err),
                "expected 42P01 for {name}, got {err}"
            );
            assert!(
                !running.server.catalog_snapshot().tables.contains_key(name),
                "global snapshot must not carry {name}",
            );
        }
        assert!(
            !running
                .server
                .catalog_snapshot()
                .indexes
                .contains_key("pb_b_ix"),
            "the aborted index must not be in the snapshot",
        );

        shutdown(running).await;
    }

    // After restart neither table (and no index) resurrects — the valid first
    // CREATE TABLE must NOT have committed durably.
    let running = start_persistent_server(data_dir.path(), "txddl_acc_partial_a2").await;
    let client = &running.client;
    for name in ["pb_a", "pb_b"] {
        assert!(
            !running.server.catalog_snapshot().tables.contains_key(name),
            "table {name} must not resurrect after restart",
        );
        let count = client
            .query_one(
                &format!(
                    "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = '{name}' AND relkind = 'r'"
                ),
                &[],
            )
            .await
            .expect("pg_class probe")
            .get::<_, i64>(0);
        assert_eq!(count, 0, "no durable pg_class row for aborted table {name}");
    }
    let idx_rows = client
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'pb_b_ix'",
            &[],
        )
        .await
        .expect("pg_class index probe")
        .get::<_, i64>(0);
    assert_eq!(idx_rows, 0, "no durable pg_class row for the aborted index");
    shutdown(running).await;
}

// ───────────────────────────── ACC #7: concurrent same-name still serializes ─────────────────────────────
// Accumulation does not relax the per-name AccessExclusive serialization: two
// transactions racing to create the same relation still cannot both commit a
// pg_class row. While A holds the name lock (open txn), B's same-name CREATE
// fails immediately with 40001; after A commits, B sees the committed table and
// fails 42P07. (Mirrors Battery #5, re-asserted under the multi-op overlay.)

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_same_name_create_still_serializes_under_accumulation() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_acc_race_a").await;
    let (client_b, b_handle) = connect_as(running.bound, "tester", "txddl_acc_race_b").await;
    let client_a = &running.client;

    // A opens a multi-op transaction and creates `race_one` (taking the name
    // lock on `race_dup`'s precursor) then the contended `race_dup`.
    client_a.batch_execute("BEGIN").await.expect("A begin");
    client_a
        .batch_execute("CREATE TABLE race_one (id INT NOT NULL)")
        .await
        .expect("A first table accumulates");
    client_a
        .batch_execute("CREATE TABLE race_dup (id INT NOT NULL)")
        .await
        .expect("A creates the contended name, holding its AccessExclusive lock");

    // While A's transaction is open, B's same-name autocommit CREATE fails
    // immediately with a retryable serialization error (40001) — the lock is
    // non-blocking.
    let err = client_b
        .batch_execute("CREATE TABLE race_dup (id INT NOT NULL)")
        .await
        .expect_err("B's same-name CREATE must not block; it fails 40001 while A holds the lock");
    assert_eq!(
        sqlstate(&err),
        "40001",
        "expected serialization_failure while A holds the name lock, got {err}"
    );

    // A commits, releasing the lock and publishing both tables.
    client_a.batch_execute("COMMIT").await.expect("A commit");

    // Now B sees A's committed `race_dup` and fails with already-exists (42P07).
    let err = client_b
        .batch_execute("CREATE TABLE race_dup (id INT NOT NULL)")
        .await
        .expect_err("after A commits, B's same-name CREATE must fail 42P07");
    assert_eq!(
        sqlstate(&err),
        "42P07",
        "expected duplicate_table after A committed, got {err}"
    );

    drop(client_b);
    let _ = b_handle.await;
    shutdown(running).await;
}

// ───────────────────────────── No-regression: single schema statement still builds ─────────────────────────────
// The single-statement cases still BUILD their index (one overlay producer):
//   (a) M2: BEGIN; CREATE TABLE t(id INT PRIMARY KEY); INSERT; COMMIT.
//   (b) M3: BEGIN; CREATE INDEX ix ON existing; COMMIT.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_schema_statement_per_txn_still_builds_index() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "txddl_oc_single_a").await;
        let client = &running.client;

        // (a) Within-ONE-statement CREATE TABLE … PRIMARY KEY.
        client.batch_execute("BEGIN").await.expect("begin a");
        client
            .batch_execute("CREATE TABLE oc_pk (id INT PRIMARY KEY)")
            .await
            .expect("in-txn CREATE TABLE … PRIMARY KEY (single statement) is accepted");
        client
            .batch_execute("INSERT INTO oc_pk VALUES (1), (2)")
            .await
            .expect("in-txn inserts");
        client
            .batch_execute("COMMIT")
            .await
            .expect("commit builds the implicit PK index");
        let dup = client
            .batch_execute("INSERT INTO oc_pk VALUES (1)")
            .await
            .expect_err("post-commit duplicate must violate the built PK index");
        assert_eq!(sqlstate(&dup), "23505", "expected 23505, got {dup}");

        // (b) Single CREATE INDEX on an EXISTING table.
        client
            .batch_execute("CREATE TABLE oc_ex (id INT NOT NULL)")
            .await
            .expect("create existing table");
        client
            .batch_execute("INSERT INTO oc_ex VALUES (10), (20)")
            .await
            .expect("seed rows");
        client.batch_execute("BEGIN").await.expect("begin b");
        client
            .batch_execute("CREATE UNIQUE INDEX oc_ex_ix ON oc_ex (id)")
            .await
            .expect("single in-txn CREATE UNIQUE INDEX is accepted");
        client
            .batch_execute("COMMIT")
            .await
            .expect("commit builds the index");
        let dup = client
            .batch_execute("INSERT INTO oc_ex VALUES (10)")
            .await
            .expect_err("post-commit duplicate must violate the built unique index");
        assert_eq!(sqlstate(&dup), "23505", "expected 23505, got {dup}");

        shutdown(running).await;
    }

    // Both indexes survive restart BUILT (post-restart duplicates fail 23505).
    let running = start_persistent_server(data_dir.path(), "txddl_oc_single_a2").await;
    let client = &running.client;
    let dup_pk = client
        .batch_execute("INSERT INTO oc_pk VALUES (2)")
        .await
        .expect_err("post-restart PK duplicate must fail 23505");
    assert_eq!(sqlstate(&dup_pk), "23505", "expected 23505, got {dup_pk}");
    let dup_ix = client
        .batch_execute("INSERT INTO oc_ex VALUES (20)")
        .await
        .expect_err("post-restart unique-index duplicate must fail 23505");
    assert_eq!(sqlstate(&dup_ix), "23505", "expected 23505, got {dup_ix}");
    shutdown(running).await;
}

// ════════════════════════════ Milestone 4 ════════════════════════════
// Transactional ALTER TABLE — the catalog-only sub-action subset
// (RENAME TO / RENAME COLUMN / SET|DROP DEFAULT / SET|DROP NOT NULL / SET opts).
//
// A rolled-back ALTER whose catalog edit survives — or a committed ALTER lost on
// restart — is silent schema corruption (the same class this whole battery
// guards). M4 #1 (ROLLBACK undoes the ALTER), M4 #2 (second-connection
// isolation), and M4 #4 (crash mid-txn-ALTER) are the corruption gates.

/// Whether a `tokio_postgres` error is an "undefined column" (42703).
fn is_undefined_column(err: &tokio_postgres::Error) -> bool {
    err.code().map(|c| c.code() == "42703").unwrap_or(false)
}

// ───────────────────────────── M4 #1 ─────────────────────────────
// ROLLBACK undoes the in-txn ALTER, same session — for every sub-action.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m4_rollback_undoes_in_txn_alter_same_session() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_m4_rb").await;
    let client = &running.client;

    // Seed (autocommit) a two-column table — `id` is always supplied, `c` is the
    // column the ALTERs target. A second column lets an INSERT omit `c` so a
    // column default actually takes effect (a single-column `INSERT DEFAULT
    // VALUES` inserts no row in this engine).
    client
        .batch_execute("CREATE TABLE m4_rb (id INT, c INT)")
        .await
        .expect("seed table");

    // --- RENAME COLUMN c -> d, then ROLLBACK: column is c, not d. ---
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("ALTER TABLE m4_rb RENAME COLUMN c TO d")
        .await
        .expect("in-txn rename column accepted");
    // Self-visible in-txn: d resolves, c does not.
    client
        .query("SELECT d FROM m4_rb", &[])
        .await
        .expect("renamed column visible to self in-txn");
    client.batch_execute("ROLLBACK").await.expect("rollback");
    client
        .query("SELECT c FROM m4_rb", &[])
        .await
        .expect("rolled-back rename: column is c again");
    let err = client
        .query("SELECT d FROM m4_rb", &[])
        .await
        .expect_err("rolled-back rename: d must not exist");
    assert!(is_undefined_column(&err), "expected 42703, got {err}");

    // --- RENAME TO m4_rb2, then ROLLBACK: table is m4_rb, not m4_rb2. ---
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("ALTER TABLE m4_rb RENAME TO m4_rb2")
        .await
        .expect("in-txn rename table accepted");
    client
        .query("SELECT c FROM m4_rb2", &[])
        .await
        .expect("renamed table visible to self in-txn");
    client.batch_execute("ROLLBACK").await.expect("rollback");
    client
        .query("SELECT c FROM m4_rb", &[])
        .await
        .expect("rolled-back rename: table is m4_rb again");
    let err = client
        .query("SELECT c FROM m4_rb2", &[])
        .await
        .expect_err("rolled-back rename: m4_rb2 must not exist");
    assert!(is_undefined_table(&err), "expected 42P01, got {err}");
    assert!(
        running
            .server
            .catalog_snapshot()
            .tables
            .contains_key("m4_rb")
            && !running
                .server
                .catalog_snapshot()
                .tables
                .contains_key("m4_rb2"),
        "global snapshot must keep the pre-ALTER name after rollback",
    );

    // --- SET DEFAULT 5, then ROLLBACK: a later INSERT omitting c gets NULL,
    //     not 5. ---
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("ALTER TABLE m4_rb ALTER COLUMN c SET DEFAULT 5")
        .await
        .expect("in-txn set default accepted");
    client.batch_execute("ROLLBACK").await.expect("rollback");
    client
        .batch_execute("INSERT INTO m4_rb (id) VALUES (1)")
        .await
        .expect("insert after rolled-back default");
    let row = client
        .query_one("SELECT c FROM m4_rb WHERE id = 1", &[])
        .await
        .expect("read back");
    assert!(
        row.try_get::<_, i32>(0).is_err(),
        "rolled-back default must leave the column NULL",
    );
    client
        .batch_execute("DELETE FROM m4_rb")
        .await
        .expect("clean up the NULL row");

    // --- SET NOT NULL, then ROLLBACK: a NULL insert still succeeds after. ---
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("ALTER TABLE m4_rb ALTER COLUMN c SET NOT NULL")
        .await
        .expect("in-txn set not null accepted (table empty)");
    client.batch_execute("ROLLBACK").await.expect("rollback");
    client
        .batch_execute("INSERT INTO m4_rb (id, c) VALUES (2, NULL)")
        .await
        .expect("rolled-back NOT NULL: NULL insert must still be allowed");
    client
        .batch_execute("DELETE FROM m4_rb")
        .await
        .expect("clean up");

    // --- SET (autovacuum_vacuum_threshold = 100), then ROLLBACK: option not
    //     present after. ---
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("ALTER TABLE m4_rb SET (autovacuum_vacuum_threshold = 100)")
        .await
        .expect("in-txn set options accepted");
    client.batch_execute("ROLLBACK").await.expect("rollback");
    let entry = running
        .server
        .catalog_snapshot()
        .tables
        .get("m4_rb")
        .expect("table still present")
        .clone();
    assert!(
        !entry
            .options
            .iter()
            .any(|(k, _)| k == "autovacuum_vacuum_threshold"),
        "rolled-back SET options must leave no autovacuum_vacuum_threshold: {:?}",
        entry.options,
    );

    shutdown(running).await;
}

// ───────────────────────────── M4 #2 ─────────────────────────────
// Second-connection isolation: B never sees A's uncommitted RENAME, and after
// A ROLLBACK B still sees the old name. (The COMMIT-visibility half is covered
// by M4 #4's committed-survives-restart and M4 #7.)

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_uncommitted_in_txn_alter_invisible_to_other_connection() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_m4_iso_a").await;
    let (client_b, b_handle) = connect_as(running.bound, "tester", "txddl_m4_iso_b").await;

    running
        .client
        .batch_execute("CREATE TABLE m4_iso (c INT)")
        .await
        .expect("seed");

    running
        .client
        .batch_execute("BEGIN")
        .await
        .expect("A begin");
    running
        .client
        .batch_execute("ALTER TABLE m4_iso RENAME TO m4_iso2")
        .await
        .expect("A in-txn rename (uncommitted)");

    // B still sees the OLD name and NOT the new one.
    client_b
        .query("SELECT c FROM m4_iso", &[])
        .await
        .expect("B still sees the old name while A's rename is uncommitted");
    let err = client_b
        .query("SELECT c FROM m4_iso2", &[])
        .await
        .expect_err("B must not see A's uncommitted new name");
    assert!(is_undefined_table(&err), "expected 42P01 for B, got {err}");

    // A rolls back; B still sees the old name, never the new one.
    running
        .client
        .batch_execute("ROLLBACK")
        .await
        .expect("A rollback");
    client_b
        .query("SELECT c FROM m4_iso", &[])
        .await
        .expect("B still sees the old name after A rollback");
    let err = client_b
        .query("SELECT c FROM m4_iso2", &[])
        .await
        .expect_err("B must never see the rolled-back new name");
    assert!(is_undefined_table(&err), "expected 42P01 for B, got {err}");

    // A commits a rename on a fresh txn; now B sees the new name.
    running
        .client
        .batch_execute("BEGIN")
        .await
        .expect("A begin 2");
    running
        .client
        .batch_execute("ALTER TABLE m4_iso RENAME TO m4_iso2")
        .await
        .expect("A in-txn rename 2");
    running
        .client
        .batch_execute("COMMIT")
        .await
        .expect("A commit");
    client_b
        .query("SELECT c FROM m4_iso2", &[])
        .await
        .expect("B sees the committed new name");
    let err = client_b
        .query("SELECT c FROM m4_iso", &[])
        .await
        .expect_err("old name gone after commit");
    assert!(is_undefined_table(&err), "expected 42P01 for B, got {err}");

    drop(client_b);
    let _ = b_handle.await;
    shutdown(running).await;
}

// ───────────────────────────── M4 #3 ─────────────────────────────
// Self-visible before commit: SET DEFAULT then INSERT DEFAULT VALUES sees the
// new default in-txn; RENAME COLUMN then SELECT the new name works in-txn.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m4_in_txn_alter_self_visible_before_commit() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_m4_self").await;
    let client = &running.client;

    // Two columns so an INSERT can omit `c` and let its default apply.
    client
        .batch_execute("CREATE TABLE m4_self (id INT, c INT)")
        .await
        .expect("seed");

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("ALTER TABLE m4_self ALTER COLUMN c SET DEFAULT 5")
        .await
        .expect("in-txn set default");
    client
        .batch_execute("INSERT INTO m4_self (id) VALUES (1)")
        .await
        .expect("in-txn insert omitting c");
    let row = client
        .query_one("SELECT c FROM m4_self WHERE id = 1", &[])
        .await
        .expect("in-txn select");
    assert_eq!(
        row.get::<_, i32>(0),
        5,
        "in-txn INSERT omitting c must use the in-txn SET DEFAULT",
    );
    // Now rename the column in the same txn and read it back by the new name.
    client
        .batch_execute("ALTER TABLE m4_self RENAME COLUMN c TO d")
        .await
        .expect("in-txn rename column");
    let row = client
        .query_one("SELECT d FROM m4_self WHERE id = 1", &[])
        .await
        .expect("in-txn select by renamed column");
    assert_eq!(row.get::<_, i32>(0), 5, "renamed column carries the value");
    client.batch_execute("COMMIT").await.expect("commit");

    // After commit the renamed column + default both stand.
    let row = client
        .query_one("SELECT d FROM m4_self WHERE id = 1", &[])
        .await
        .expect("post-commit select by renamed column");
    assert_eq!(row.get::<_, i32>(0), 5);
    client
        .batch_execute("INSERT INTO m4_self (id) VALUES (2)")
        .await
        .expect("post-commit insert omitting the renamed column");
    let row = client
        .query_one("SELECT d FROM m4_self WHERE id = 2", &[])
        .await
        .expect("post-commit select");
    assert_eq!(row.get::<_, i32>(0), 5, "default persists post-commit");

    shutdown(running).await;
}

// ───────────────────────────── M4 #4 ─────────────────────────────
// THE corruption gate: crash mid-txn-ALTER (after the ALTER ran, before COMMIT)
// → on restart the table reverts to its pre-ALTER name/schema/default (the
// aborted post-ALTER rows are bootstrap-hidden; the committed pre-ALTER row
// wins). Symmetric: an ALTER that DID commit durably is present on restart.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m4_crash_mid_txn_alter_reverts_on_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "txddl_m4_crash_a").await;
        running
            .client
            .batch_execute("CREATE TABLE m4_crash (c INT)")
            .await
            .expect("seed (autocommit, committed)");
        running.client.batch_execute("BEGIN").await.expect("begin");
        running
            .client
            .batch_execute("ALTER TABLE m4_crash RENAME TO m4_crash2")
            .await
            .expect("in-txn rename (durable rows under user xid, NO commit)");
        running
            .client
            .batch_execute("ALTER TABLE m4_crash2 RENAME COLUMN c TO d")
            .await
            .expect("in-txn rename column too");
        // Drop server WITHOUT COMMIT/ROLLBACK — the user xid has no commit record.
        shutdown(running).await;
    }

    // Restart: the crash-before-commit ALTER must NOT survive.
    let running = start_persistent_server(data_dir.path(), "txddl_m4_crash_a2").await;
    let snap = running.server.catalog_snapshot();
    assert!(
        snap.tables.contains_key("m4_crash") && !snap.tables.contains_key("m4_crash2"),
        "crash-before-commit rename must revert to the pre-ALTER name on restart",
    );
    // The pre-ALTER column name `c` wins; `d` never existed.
    running
        .client
        .query("SELECT c FROM m4_crash", &[])
        .await
        .expect("pre-ALTER column name present after restart");
    let err = running
        .client
        .query("SELECT d FROM m4_crash", &[])
        .await
        .expect_err("aborted post-ALTER column must be hidden on restart");
    assert!(is_undefined_column(&err), "expected 42703, got {err}");
    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m4_committed_in_txn_alter_survives_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "txddl_m4_survive_a").await;
        running
            .client
            .batch_execute("CREATE TABLE m4_sv (id INT, c INT)")
            .await
            .expect("seed");
        running
            .client
            .batch_execute("INSERT INTO m4_sv (id, c) VALUES (1, 7)")
            .await
            .expect("seed row");
        running.client.batch_execute("BEGIN").await.expect("begin");
        running
            .client
            .batch_execute("ALTER TABLE m4_sv RENAME TO m4_sv2")
            .await
            .expect("in-txn rename table");
        running
            .client
            .batch_execute("ALTER TABLE m4_sv2 RENAME COLUMN c TO d")
            .await
            .expect("in-txn rename column");
        running
            .client
            .batch_execute("ALTER TABLE m4_sv2 ALTER COLUMN d SET DEFAULT 9")
            .await
            .expect("in-txn set default");
        running
            .client
            .batch_execute("COMMIT")
            .await
            .expect("commit");
        shutdown(running).await;
    }

    let running = start_persistent_server(data_dir.path(), "txddl_m4_survive_a2").await;
    // New name + new column name survive.
    let row = running
        .client
        .query_one("SELECT d FROM m4_sv2 WHERE id = 1", &[])
        .await
        .expect("committed ALTER present after restart");
    assert_eq!(row.get::<_, i32>(0), 7, "row data preserved across rename");
    // Old name + old column name gone.
    assert!(
        running
            .client
            .query("SELECT c FROM m4_sv2", &[])
            .await
            .is_err(),
        "old column name must be gone after restart",
    );
    // The default survives restart: an insert omitting d gets 9.
    running
        .client
        .batch_execute("INSERT INTO m4_sv2 (id) VALUES (2)")
        .await
        .expect("default insert post-restart");
    let row = running
        .client
        .query_one("SELECT d FROM m4_sv2 WHERE id = 2", &[])
        .await
        .expect("read back");
    assert_eq!(
        row.get::<_, i32>(0),
        9,
        "committed default survives restart"
    );
    shutdown(running).await;
}

// ───────────────────────────── M4 #5 ─────────────────────────────
// Concurrent serialized: A holds AccessExclusive on BOTH old and new names; B's
// concurrent CREATE TABLE of the new name fails 40001 (no torn rename).

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_concurrent_rename_serializes_on_both_names() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_m4_race_a").await;
    let (client_b, b_handle) = connect_as(running.bound, "tester", "txddl_m4_race_b").await;

    running
        .client
        .batch_execute("CREATE TABLE m4_race (c INT)")
        .await
        .expect("seed");

    running
        .client
        .batch_execute("BEGIN")
        .await
        .expect("A begin");
    running
        .client
        .batch_execute("ALTER TABLE m4_race RENAME TO m4_race2")
        .await
        .expect("A rename takes AccessExclusive on both names");

    // B tries to CREATE the NEW name while A holds its lock → 40001.
    client_b.batch_execute("BEGIN").await.expect("B begin");
    let err = client_b
        .batch_execute("CREATE TABLE m4_race2 (x INT)")
        .await
        .expect_err("B's CREATE of the new name must serialize-fail");
    assert_eq!(sqlstate(&err), "40001", "expected 40001, got {err}");
    client_b
        .batch_execute("ROLLBACK")
        .await
        .expect("B rollback");

    // A commits; exactly one table under the new name, none under the old.
    running
        .client
        .batch_execute("COMMIT")
        .await
        .expect("A commit");
    let snap = running.server.catalog_snapshot();
    assert!(
        snap.tables.contains_key("m4_race2") && !snap.tables.contains_key("m4_race"),
        "exactly one table, under the new name, after the serialized rename",
    );

    drop(client_b);
    let _ = b_handle.await;
    shutdown(running).await;
}

// ───────────────────────────── M4 #6 ─────────────────────────────
// No orphaned runtime state after ROLLBACK: a SET DEFAULT that rolls back must
// not leave a runtime-constraints entry that a later INSERT would honour.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m4_rollback_leaves_no_orphaned_runtime_default() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_m4_orphan").await;
    let client = &running.client;

    // Table starts with NO runtime constraints at all. Two columns so an INSERT
    // can omit `c` and reveal whether a default leaked.
    client
        .batch_execute("CREATE TABLE m4_orphan (id INT, c INT)")
        .await
        .expect("seed");

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("ALTER TABLE m4_orphan ALTER COLUMN c SET DEFAULT 42")
        .await
        .expect("in-txn set default");
    client.batch_execute("ROLLBACK").await.expect("rollback");

    // The runtime side map must have NO entry that survived the rollback: an
    // INSERT that omits c gets NULL, not 42.
    client
        .batch_execute("INSERT INTO m4_orphan (id) VALUES (1)")
        .await
        .expect("insert omitting c");
    let row = client
        .query_one("SELECT c FROM m4_orphan WHERE id = 1", &[])
        .await
        .expect("read back");
    assert!(
        row.try_get::<_, i32>(0).is_err(),
        "rolled-back default must not leak into the runtime side map",
    );

    shutdown(running).await;
}

// ───────────────────────────── M4 #7 ─────────────────────────────
// Accumulation with CREATE: CREATE then ALTER (rename column) then INSERT by the
// new name, all in one txn — present + consistent on a fresh connection AND
// after restart. ROLLBACK variant: table absent before + after restart.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_create_then_alter_accumulates_and_commits() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "txddl_m4_acc_a").await;
        let (client_b, b_handle) = connect_as(running.bound, "tester", "txddl_m4_acc_b").await;

        running.client.batch_execute("BEGIN").await.expect("begin");
        running
            .client
            .batch_execute("CREATE TABLE m4_acc (c INT)")
            .await
            .expect("in-txn create");
        running
            .client
            .batch_execute("ALTER TABLE m4_acc RENAME COLUMN c TO d")
            .await
            .expect("in-txn alter the same-txn-created table");
        running
            .client
            .batch_execute("INSERT INTO m4_acc (d) VALUES (1)")
            .await
            .expect("in-txn insert by the new column name");
        // B sees nothing yet.
        let err = client_b
            .query("SELECT d FROM m4_acc", &[])
            .await
            .expect_err("B must not see the uncommitted create+alter");
        assert!(is_undefined_table(&err), "expected 42P01, got {err}");
        running
            .client
            .batch_execute("COMMIT")
            .await
            .expect("commit");

        // A fresh connection sees the committed table with the renamed column.
        let row = client_b
            .query_one("SELECT d FROM m4_acc", &[])
            .await
            .expect("B sees committed create+alter");
        assert_eq!(row.get::<_, i32>(0), 1);
        let err = client_b
            .query("SELECT c FROM m4_acc", &[])
            .await
            .expect_err("the pre-rename column name must be gone");
        assert!(is_undefined_column(&err), "expected 42703, got {err}");

        drop(client_b);
        let _ = b_handle.await;
        shutdown(running).await;
    }

    // After restart: still present + consistent.
    let running = start_persistent_server(data_dir.path(), "txddl_m4_acc_a2").await;
    let row = running
        .client
        .query_one("SELECT d FROM m4_acc", &[])
        .await
        .expect("create+alter present after restart");
    assert_eq!(row.get::<_, i32>(0), 1);
    assert!(
        running
            .client
            .query("SELECT c FROM m4_acc", &[])
            .await
            .is_err(),
        "renamed-away column must stay gone after restart",
    );
    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m4_create_then_alter_rollback_absent_before_and_after_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "txddl_m4_accrb_a").await;
        running.client.batch_execute("BEGIN").await.expect("begin");
        running
            .client
            .batch_execute("CREATE TABLE m4_accrb (c INT)")
            .await
            .expect("in-txn create");
        running
            .client
            .batch_execute("ALTER TABLE m4_accrb RENAME COLUMN c TO d")
            .await
            .expect("in-txn alter");
        running
            .client
            .batch_execute("ROLLBACK")
            .await
            .expect("rollback");
        // Absent in this session immediately after rollback.
        let err = running
            .client
            .query("SELECT d FROM m4_accrb", &[])
            .await
            .expect_err("rolled-back create+alter must be absent");
        assert!(is_undefined_table(&err), "expected 42P01, got {err}");
        assert!(
            !running
                .server
                .catalog_snapshot()
                .tables
                .contains_key("m4_accrb"),
            "global snapshot must not carry the rolled-back table",
        );
        shutdown(running).await;
    }

    // Absent after restart.
    let running = start_persistent_server(data_dir.path(), "txddl_m4_accrb_a2").await;
    assert!(
        !running
            .server
            .catalog_snapshot()
            .tables
            .contains_key("m4_accrb"),
        "rolled-back create+alter must not resurrect after restart",
    );
    shutdown(running).await;
}

// ───────────────────────────── M4 #8 ─────────────────────────────
// Regression: out-of-scope ALTER sub-actions stay 0A000 (→ Failed) in-txn; an
// in-scope ALTER under an active SAVEPOINT is 0A000; autocommit ALTER unchanged.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m4_out_of_scope_alter_still_rejected_in_txn() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_m4_oos").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE m4_oos (c INT)")
        .await
        .expect("seed");

    // ADD COLUMN inside a txn → 0A000, block goes Failed.
    client.batch_execute("BEGIN").await.expect("begin");
    let err = client
        .batch_execute("ALTER TABLE m4_oos ADD COLUMN e INT")
        .await
        .expect_err("ADD COLUMN in-txn must be rejected");
    assert_eq!(sqlstate(&err), "0A000", "expected 0A000, got {err}");
    client.batch_execute("ROLLBACK").await.expect("rollback");

    // DROP COLUMN inside a txn → 0A000.
    client.batch_execute("BEGIN").await.expect("begin");
    let err = client
        .batch_execute("ALTER TABLE m4_oos DROP COLUMN c")
        .await
        .expect_err("DROP COLUMN in-txn must be rejected");
    assert_eq!(sqlstate(&err), "0A000", "expected 0A000, got {err}");
    client.batch_execute("ROLLBACK").await.expect("rollback");

    // ADD CONSTRAINT (CHECK) inside a txn → 0A000.
    client.batch_execute("BEGIN").await.expect("begin");
    let err = client
        .batch_execute("ALTER TABLE m4_oos ADD CONSTRAINT m4_chk CHECK (c > 0)")
        .await
        .expect_err("ADD CONSTRAINT in-txn must be rejected");
    assert_eq!(sqlstate(&err), "0A000", "expected 0A000, got {err}");
    client.batch_execute("ROLLBACK").await.expect("rollback");

    // ENABLE ROW LEVEL SECURITY inside a txn → 0A000.
    client.batch_execute("BEGIN").await.expect("begin");
    let err = client
        .batch_execute("ALTER TABLE m4_oos ENABLE ROW LEVEL SECURITY")
        .await
        .expect_err("ENABLE RLS in-txn must be rejected");
    assert_eq!(sqlstate(&err), "0A000", "expected 0A000, got {err}");
    client.batch_execute("ROLLBACK").await.expect("rollback");

    // An in-SCOPE ALTER under an active SAVEPOINT → 0A000.
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("SAVEPOINT sp1")
        .await
        .expect("savepoint");
    let err = client
        .batch_execute("ALTER TABLE m4_oos RENAME TO m4_oos2")
        .await
        .expect_err("in-scope ALTER under a SAVEPOINT must be rejected");
    assert_eq!(sqlstate(&err), "0A000", "expected 0A000, got {err}");
    client.batch_execute("ROLLBACK").await.expect("rollback");

    // The table is untouched: still named m4_oos with column c, no e.
    client
        .query("SELECT c FROM m4_oos", &[])
        .await
        .expect("table untouched by the rejected ALTERs");
    assert!(
        client.query("SELECT e FROM m4_oos", &[]).await.is_err(),
        "no leaked ADD COLUMN",
    );

    // Autocommit ALTER (in-scope) still works unchanged.
    client
        .batch_execute("ALTER TABLE m4_oos RENAME TO m4_oos2")
        .await
        .expect("autocommit rename still works");
    client
        .query("SELECT c FROM m4_oos2", &[])
        .await
        .expect("autocommit-renamed table usable");

    shutdown(running).await;
}

// ───────────────────────── M4 #8 (corruption) ─────────────────────────
// In-txn `SET NOT NULL` must see the transaction's OWN uncommitted rows.
//
// CORRUPTION GATE. The DDL dispatch does not `refresh_snapshot` for an ALTER,
// so the in-txn `SET NOT NULL` validate-scan ran under `txn.snapshot` whose
// `current_command` still pointed at the PRIOR statement. A row inserted by the
// immediately-preceding in-txn INSERT carries that same command id, and the
// MVCC self-visibility rule (`cmin >= current_command ⇒ Invisible`) hid it: the
// scan saw 0 rows, a same-txn NULL passed, and COMMIT durably persisted a
// NOT-NULL column over a NULL row — silent schema corruption (PostgreSQL
// rejects the original sequence with 23502).
//
// Repro is deterministic with NO intervening statement: BEGIN; INSERT a NULL;
// ALTER … SET NOT NULL; COMMIT. Pre-fix the ALTER (and COMMIT) wrongly succeed;
// post-fix the ALTER fails 23502 and the column stays nullable.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m4_set_not_null_sees_own_uncommitted_null_row() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "txddl_m4_own_null_a").await;
        let client = &running.client;
        client
            .batch_execute("CREATE TABLE m4_own (id INT, c INT)")
            .await
            .expect("seed table");

        // The deterministic corruption sequence, NO intervening statement
        // between the INSERT and the ALTER.
        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("INSERT INTO m4_own (id, c) VALUES (1, NULL)")
            .await
            .expect("same-txn NULL insert");
        let alter_err = client
            .batch_execute("ALTER TABLE m4_own ALTER COLUMN c SET NOT NULL")
            .await
            .expect_err(
                "in-txn SET NOT NULL must SEE the transaction's own NULL row and reject 23502 \
                 (pre-fix it wrongly succeeded, durably persisting a NOT-NULL column over a NULL)",
            );
        assert_eq!(
            sqlstate(&alter_err),
            "23502",
            "expected 23502 not_null_violation, got {alter_err}",
        );
        // The ALTER failed → the block is aborted (25P02). Roll it back; the
        // staged schema edit must NOT have committed.
        client.batch_execute("ROLLBACK").await.expect("rollback");

        // The column is still nullable: a later NULL insert succeeds, and there
        // is no enforced-but-violated NOT NULL.
        client
            .batch_execute("INSERT INTO m4_own (id, c) VALUES (2, NULL)")
            .await
            .expect("column must remain nullable after the rejected SET NOT NULL");

        // A second connection sees the column as nullable too.
        let (client_b, b_handle) = connect_as(running.bound, "tester", "txddl_m4_own_null_b").await;
        client_b
            .batch_execute("INSERT INTO m4_own (id, c) VALUES (3, NULL)")
            .await
            .expect("2nd connection: column still nullable");
        drop(client_b);
        b_handle.abort();

        shutdown(running).await;
    }

    // After restart the column is still nullable — the rejected ALTER left no
    // durable NOT-NULL marker.
    let running = start_persistent_server(data_dir.path(), "txddl_m4_own_null_a2").await;
    running
        .client
        .batch_execute("INSERT INTO m4_own (id, c) VALUES (4, NULL)")
        .await
        .expect("column still nullable after restart (no durable NOT NULL)");
    shutdown(running).await;
}

// ───────────────────────── M4 #9 (positive) ─────────────────────────
// In-txn `SET NOT NULL` over a same-txn INSERT of only NON-NULL rows COMMITs;
// the column is genuinely NOT NULL afterward (a later NULL insert is rejected
// 23502) and survives restart.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m4_set_not_null_commits_over_own_non_null_rows() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "txddl_m4_own_ok_a").await;
        let client = &running.client;
        client
            .batch_execute("CREATE TABLE m4_ok (id INT, c INT)")
            .await
            .expect("seed table");

        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("INSERT INTO m4_ok (id, c) VALUES (1, 10), (2, 20)")
            .await
            .expect("same-txn non-null inserts");
        client
            .batch_execute("ALTER TABLE m4_ok ALTER COLUMN c SET NOT NULL")
            .await
            .expect("SET NOT NULL must succeed when the txn's own rows are all non-null");
        client.batch_execute("COMMIT").await.expect("commit");

        // The column is genuinely NOT NULL now: a NULL insert is rejected 23502.
        let null_err = client
            .batch_execute("INSERT INTO m4_ok (id, c) VALUES (3, NULL)")
            .await
            .expect_err("the column must be enforced NOT NULL after commit");
        assert_eq!(
            sqlstate(&null_err),
            "23502",
            "expected 23502 after commit, got {null_err}",
        );
        // A non-null insert still works.
        client
            .batch_execute("INSERT INTO m4_ok (id, c) VALUES (4, 40)")
            .await
            .expect("non-null insert ok");
        shutdown(running).await;
    }

    // Survives restart: still NOT NULL.
    let running = start_persistent_server(data_dir.path(), "txddl_m4_own_ok_a2").await;
    let null_err = running
        .client
        .batch_execute("INSERT INTO m4_ok (id, c) VALUES (5, NULL)")
        .await
        .expect_err("NOT NULL must survive restart");
    assert_eq!(
        sqlstate(&null_err),
        "23502",
        "expected 23502 after restart, got {null_err}",
    );
    let count = running
        .client
        .query_one("SELECT count(*) FROM m4_ok", &[])
        .await
        .expect("count survives restart");
    assert_eq!(
        count.get::<_, i64>(0),
        3,
        "the three committed rows survive"
    );
    shutdown(running).await;
}

// ───────────────────────── M4 #10 (committed data) ─────────────────────────
// The previously-working committed-data case still works: a NULL row committed
// by an EARLIER autocommit txn is caught by an in-txn `SET NOT NULL` (the
// refreshed-command-id validate snapshot keeps the same frozen MVCC view, so
// already-committed rows remain visible).

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m4_set_not_null_still_catches_earlier_committed_null() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_m4_committed_null").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE m4_cn (id INT, c INT)")
        .await
        .expect("seed table");
    // Committed (autocommit) NULL row from an EARLIER transaction.
    client
        .batch_execute("INSERT INTO m4_cn (id, c) VALUES (1, NULL)")
        .await
        .expect("earlier committed NULL row");

    client.batch_execute("BEGIN").await.expect("begin");
    let alter_err = client
        .batch_execute("ALTER TABLE m4_cn ALTER COLUMN c SET NOT NULL")
        .await
        .expect_err("in-txn SET NOT NULL must still catch an earlier-committed NULL");
    assert_eq!(
        sqlstate(&alter_err),
        "23502",
        "expected 23502 for the committed NULL, got {alter_err}",
    );
    client.batch_execute("ROLLBACK").await.expect("rollback");

    // Column remains nullable: another NULL insert succeeds.
    client
        .batch_execute("INSERT INTO m4_cn (id, c) VALUES (2, NULL)")
        .await
        .expect("column still nullable after the rejected SET NOT NULL");

    shutdown(running).await;
}

// ═══════════════════ Transactional-DDL milestone 5 ═══════════════════
// In-txn `DROP TABLE` (plain RESTRICT) via a NEGATIVE-MASK catalog overlay.
//
// The in-txn DROP handler mutates NOTHING in the global catalog and emits NO
// `SequenceOp::Drop` WAL: it stages a `RelKind::Dropped` tombstone under the
// USER xid (for a committed-before table) plus a session-local mask that hides
// the table + its indexes + its constraints from the issuing session, while
// other sessions keep seeing it until COMMIT. COMMIT applies the real global
// drop; ROLLBACK/crash discard the overlay (free) and the table resurrects.
//
// The corruption gates: M5 #1 (ROLLBACK / crash RESURRECTS the table WITH its
// data + index — a rolled-back DROP that stays gone is silent loss of a
// committed table), M5 #2 (COMMIT — gone everywhere incl. restart, no orphans),
// M5 #3 (a rolled-back DROP leaves NO half-state).

/// Run an in-txn DROP that must reject with 0A000 + transition to Failed (25P02),
/// asserting the table SURVIVES a ROLLBACK (the reject fired before any durable
/// mutation). Used for the (iv) rejected-set battery.
async fn assert_in_txn_drop_rejected_and_survives(
    application_name: &str,
    setup: &[&str],
    drop_stmt: &str,
    survives_table: &str,
) {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), application_name).await;
    let client = &running.client;
    for stmt in setup {
        client.batch_execute(stmt).await.expect("setup");
    }
    client.batch_execute("BEGIN").await.expect("begin");
    let err = client
        .batch_execute(drop_stmt)
        .await
        .expect_err("out-of-scope in-txn DROP must reject");
    assert_eq!(
        sqlstate(&err),
        "0A000",
        "`{drop_stmt}` in-txn must be feature_not_supported, got {err}"
    );
    let in_failed = client
        .batch_execute("SELECT 1")
        .await
        .expect_err("statement after rejected DROP must be 25P02");
    assert_eq!(
        sqlstate(&in_failed),
        "25P02",
        "in-failed-block after `{drop_stmt}` must be 25P02, got {in_failed}"
    );
    client.batch_execute("ROLLBACK").await.expect("rollback");
    // The target table survives the rejected-then-rolled-back DROP.
    client
        .query(&format!("SELECT * FROM {survives_table}"), &[])
        .await
        .unwrap_or_else(|e| panic!("table `{survives_table}` must survive a rejected DROP: {e}"));
    shutdown(running).await;
}

// ───────────────────────────── M5 #1 ─────────────────────────────
// ROLLBACK RESURRECTS the committed table WITH its rows + secondary index;
// a 2nd connection sees the table live THROUGHOUT (mask is session-local).

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_in_txn_drop_resurrects_table_data_and_index() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "m5_drop_rb_a").await;
    let client = &running.client;
    let (client_b, b_handle) = connect_as(running.bound, "tester", "m5_drop_rb_b").await;

    // Seed a committed table with N rows + a secondary index.
    client
        .batch_execute("CREATE TABLE drb (id INT PRIMARY KEY, v INT)")
        .await
        .expect("seed table");
    client
        .batch_execute("CREATE INDEX drb_v_ix ON drb (v)")
        .await
        .expect("seed secondary index");
    for i in 0..5 {
        client
            .batch_execute(&format!("INSERT INTO drb VALUES ({i}, {})", i * 10))
            .await
            .expect("seed row");
    }

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("DROP TABLE drb")
        .await
        .expect("in-txn DROP accepted (milestone 5)");
    // Same session: the table is now invisible.
    let err = client
        .query("SELECT * FROM drb", &[])
        .await
        .expect_err("issuing session must not see the dropped table");
    assert!(
        is_undefined_table(&err),
        "expected 42P01 for self, got {err}"
    );

    // 2nd connection sees the table LIVE throughout (mask is session-local).
    let rows_b = client_b
        .query("SELECT count(*) FROM drb", &[])
        .await
        .expect("B sees the table live during A's uncommitted DROP");
    assert_eq!(rows_b[0].get::<_, i64>(0), 5, "B sees all rows");

    client.batch_execute("ROLLBACK").await.expect("rollback");

    // After ROLLBACK the table fully RESURRECTS for the issuing session:
    // count == N, the index probe works, the index is present.
    let rows = client
        .query("SELECT count(*) FROM drb", &[])
        .await
        .expect("table resurrects after rollback");
    assert_eq!(rows[0].get::<_, i64>(0), 5, "all rows resurrect");
    let probe = client
        .query("SELECT id FROM drb WHERE v = 30", &[])
        .await
        .expect("secondary index probe works after resurrection");
    assert_eq!(probe.len(), 1);
    assert_eq!(probe[0].get::<_, i32>(0), 3);
    assert!(
        running
            .server
            .catalog_snapshot()
            .indexes
            .contains_key("drb_v_ix"),
        "secondary index resurrects in the committed snapshot",
    );
    // B still sees it live.
    let rows_b = client_b
        .query("SELECT count(*) FROM drb", &[])
        .await
        .expect("B still sees the table after A's rollback");
    assert_eq!(rows_b[0].get::<_, i64>(0), 5);

    drop(client_b);
    let _ = b_handle.await;
    shutdown(running).await;
}

// ───────────────────────────── M5 #1b ─────────────────────────────
// CRASH after DROP before COMMIT → restart → table fully RESURRECTS with N rows
// + index (the tombstone rode the aborted user xid; bootstrap hides it).

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_after_in_txn_drop_before_commit_resurrects_table() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "m5_drop_crash_a").await;
        let client = &running.client;
        client
            .batch_execute("CREATE TABLE dcr (id INT PRIMARY KEY, v INT)")
            .await
            .expect("seed table");
        client
            .batch_execute("CREATE INDEX dcr_v_ix ON dcr (v)")
            .await
            .expect("seed index");
        client
            .batch_execute("INSERT INTO dcr VALUES (1, 100), (2, 200)")
            .await
            .expect("seed rows");
        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("DROP TABLE dcr")
            .await
            .expect("in-txn DROP (tombstone under user xid, NO commit)");
        // Crash: drop the server with the transaction still open.
        shutdown(running).await;
    }

    // Restart: the table must RESURRECT with its rows + index.
    let running = start_persistent_server(data_dir.path(), "m5_drop_crash_a2").await;
    assert!(
        running.server.catalog_snapshot().tables.contains_key("dcr"),
        "crash-before-commit DROP must resurrect the table on restart",
    );
    let rows = running
        .client
        .query("SELECT count(*) FROM dcr", &[])
        .await
        .expect("resurrected table is queryable after restart");
    assert_eq!(
        rows[0].get::<_, i64>(0),
        2,
        "all rows resurrect after restart"
    );
    assert!(
        running
            .server
            .catalog_snapshot()
            .indexes
            .contains_key("dcr_v_ix"),
        "secondary index resurrects after restart",
    );
    let probe = running
        .client
        .query("SELECT id FROM dcr WHERE v = 200", &[])
        .await
        .expect("index probe works after restart resurrection");
    assert_eq!(probe[0].get::<_, i32>(0), 2);
    shutdown(running).await;
}

// ───────────────────────────── M5 #2 ─────────────────────────────
// COMMIT — gone everywhere (both connections + restart), no orphan pg_class /
// index rows.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_in_txn_drop_is_gone_everywhere_and_after_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "m5_drop_commit_a").await;
        let client = &running.client;
        let (client_b, b_handle) = connect_as(running.bound, "tester", "m5_drop_commit_b").await;
        client
            .batch_execute("CREATE TABLE dco (id INT PRIMARY KEY, v INT)")
            .await
            .expect("seed table");
        client
            .batch_execute("INSERT INTO dco VALUES (1, 10)")
            .await
            .expect("seed row");

        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("DROP TABLE dco")
            .await
            .expect("in-txn DROP accepted");
        client.batch_execute("COMMIT").await.expect("commit");

        // Both connections see 42P01 after commit.
        let err_a = client
            .query("SELECT * FROM dco", &[])
            .await
            .expect_err("A sees the table gone after commit");
        assert!(
            is_undefined_table(&err_a),
            "expected 42P01 for A, got {err_a}"
        );
        let err_b = client_b
            .query("SELECT * FROM dco", &[])
            .await
            .expect_err("B sees the table gone after commit");
        assert!(
            is_undefined_table(&err_b),
            "expected 42P01 for B, got {err_b}"
        );
        assert!(
            !running.server.catalog_snapshot().tables.contains_key("dco"),
            "committed DROP removes the table from the global snapshot",
        );

        drop(client_b);
        let _ = b_handle.await;
        shutdown(running).await;
    }

    // Restart: still gone — the RelKind::Dropped tombstone wins latest-per-OID.
    let running = start_persistent_server(data_dir.path(), "m5_drop_commit_a2").await;
    assert!(
        !running.server.catalog_snapshot().tables.contains_key("dco"),
        "committed DROP stays gone after restart",
    );
    let err = running
        .client
        .query("SELECT * FROM dco", &[])
        .await
        .expect_err("dropped table absent after restart");
    assert!(
        is_undefined_table(&err),
        "expected 42P01 after restart, got {err}"
    );
    // No live pg_class row for the table relkind 'r'; no orphan index.
    let live = running
        .client
        .query(
            "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'dco' AND relkind = 'r'",
            &[],
        )
        .await
        .expect("probe pg_class");
    assert_eq!(
        live[0].get::<_, i64>(0),
        0,
        "no live pg_class row for the dropped table"
    );
    shutdown(running).await;
}

// ───────────────────────────── M5 #3 ─────────────────────────────
// A rolled-back DROP leaves NO half-state: index resolves, constraints intact,
// effective snapshot == committed base.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rolled_back_in_txn_drop_leaves_no_half_state() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "m5_drop_half").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE dhs (id INT PRIMARY KEY, v INT UNIQUE)")
        .await
        .expect("seed table with PK + UNIQUE");
    client
        .batch_execute("INSERT INTO dhs VALUES (1, 100)")
        .await
        .expect("seed row");
    let indexes_before = running.server.catalog_snapshot().indexes.len();
    let constraints_before = running.server.catalog_snapshot().constraints.len();

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("DROP TABLE dhs")
        .await
        .expect("in-txn DROP");
    client.batch_execute("ROLLBACK").await.expect("rollback");

    // The committed base is fully intact: same index + constraint counts, the
    // unique index still enforces (a duplicate fails 23505), PK probe works.
    let snap = running.server.catalog_snapshot();
    assert_eq!(
        snap.indexes.len(),
        indexes_before,
        "no index lost or orphaned"
    );
    assert_eq!(
        snap.constraints.len(),
        constraints_before,
        "no constraint lost or orphaned",
    );
    assert!(
        snap.tables.contains_key("dhs"),
        "table back in committed base"
    );
    let dup = client
        .batch_execute("INSERT INTO dhs VALUES (2, 100)")
        .await
        .expect_err("the UNIQUE index still enforces after a rolled-back DROP");
    assert_eq!(
        sqlstate(&dup),
        "23505",
        "expected unique_violation, got {dup}"
    );
    shutdown(running).await;
}

// ───────────────────────────── M5 #4 ─────────────────────────────
// The rejected set stays 0A000 + the table survives ROLLBACK; for the SERIAL
// case, no SequenceOp::Drop WAL was emitted (the sequence survives).

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn out_of_scope_in_txn_drop_rejects_and_survives() {
    // CASCADE.
    assert_in_txn_drop_rejected_and_survives(
        "m5_rej_cascade",
        &["CREATE TABLE rj_c (id INT)"],
        "DROP TABLE rj_c CASCADE",
        "rj_c",
    )
    .await;
    // Sequence-owning (SERIAL).
    assert_in_txn_drop_rejected_and_survives(
        "m5_rej_serial",
        &["CREATE TABLE rj_s (id SERIAL, v INT)"],
        "DROP TABLE rj_s",
        "rj_s",
    )
    .await;
    // Dependent view.
    assert_in_txn_drop_rejected_and_survives(
        "m5_rej_view",
        &[
            "CREATE TABLE rj_v (id INT)",
            "CREATE VIEW rj_vv AS SELECT id FROM rj_v",
        ],
        "DROP TABLE rj_v",
        "rj_v",
    )
    .await;
    // Inbound FK (rj_child references rj_parent).
    assert_in_txn_drop_rejected_and_survives(
        "m5_rej_fk_in",
        &[
            "CREATE TABLE rj_parent (id INT PRIMARY KEY)",
            "CREATE TABLE rj_child (id INT, p INT REFERENCES rj_parent (id))",
        ],
        "DROP TABLE rj_parent",
        "rj_parent",
    )
    .await;
    // Outbound FK (rj_child2 carries an FK).
    assert_in_txn_drop_rejected_and_survives(
        "m5_rej_fk_out",
        &[
            "CREATE TABLE rj_parent2 (id INT PRIMARY KEY)",
            "CREATE TABLE rj_child2 (id INT, p INT REFERENCES rj_parent2 (id))",
        ],
        "DROP TABLE rj_child2",
        "rj_child2",
    )
    .await;
    // System table — rejected (either by the owner/privilege guard with 42501,
    // which fires first for a non-superuser, or by the in-txn handler's
    // system-schema reject with 0A000). Either way it must NOT be tombstoned and
    // must survive.
    {
        let data_dir = tempfile::TempDir::new().unwrap();
        let running = start_persistent_server(data_dir.path(), "m5_rej_system").await;
        let client = &running.client;
        client.batch_execute("BEGIN").await.expect("begin");
        let err = client
            .batch_execute("DROP TABLE pg_catalog.pg_class")
            .await
            .expect_err("in-txn DROP of a system table must reject");
        let code = sqlstate(&err);
        assert!(
            code == "0A000" || code == "42501",
            "system-table DROP must be rejected (0A000 or 42501), got {code}: {err}",
        );
        client.batch_execute("ROLLBACK").await.expect("rollback");
        client
            .query("SELECT count(*) FROM pg_catalog.pg_class", &[])
            .await
            .expect("pg_class survives the rejected DROP");
        shutdown(running).await;
    }
}

// ───────────────────────────── M5 #4b ─────────────────────────────
// The SERIAL reject must not have emitted a SequenceOp::Drop WAL: the sequence
// survives the ROLLBACK and still advances.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_in_txn_drop_of_serial_table_leaves_sequence_intact() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "m5_serial_seq").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE sseq (id SERIAL, v INT)")
        .await
        .expect("seed serial table");
    client
        .batch_execute("INSERT INTO sseq (v) VALUES (1)")
        .await
        .expect("first serial insert");

    client.batch_execute("BEGIN").await.expect("begin");
    let err = client
        .batch_execute("DROP TABLE sseq")
        .await
        .expect_err("serial-owning DROP rejects in-txn");
    assert_eq!(sqlstate(&err), "0A000", "expected 0A000, got {err}");
    client.batch_execute("ROLLBACK").await.expect("rollback");

    // The sequence survived (no WAL Drop emitted): the table is usable and the
    // sequence keeps advancing past its prior value.
    client
        .batch_execute("INSERT INTO sseq (v) VALUES (2)")
        .await
        .expect("serial insert after rejected DROP");
    let rows = client
        .query("SELECT id FROM sseq ORDER BY id", &[])
        .await
        .expect("query serial ids");
    assert_eq!(rows.len(), 2, "both serial rows present");
    let first: i32 = rows[0].get(0);
    let second: i32 = rows[1].get(0);
    assert!(
        second > first,
        "the owned sequence advanced (not vaporized by a WAL Drop): {first} -> {second}",
    );
    shutdown(running).await;
}

// ───────────────────────────── M5 #5 ─────────────────────────────
// Accumulation matrix (§6): every sequence nets correctly in-txn AND durably.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_txn_drop_accumulation_matrix() {
    let data_dir = tempfile::TempDir::new().unwrap();
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "m5_accum_a").await;
        let client = &running.client;

        // (1) CREATE+DROP → nothing committed.
        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("CREATE TABLE acc_cd (id INT)")
            .await
            .expect("create");
        // Self-visible before the drop.
        client
            .query("SELECT * FROM acc_cd", &[])
            .await
            .expect("same-txn-created table is self-visible before the drop");
        client
            .batch_execute("DROP TABLE acc_cd")
            .await
            .expect("drop same-txn-created");
        // NOTE: a masked self-SELECT here would error 42P01 and FAIL the block,
        // turning COMMIT into a ROLLBACK; self-visibility of the mask is covered
        // by M5 #1. Here we COMMIT a genuine CREATE+DROP net-out.
        client.batch_execute("COMMIT").await.expect("commit");
        assert!(
            !running
                .server
                .catalog_snapshot()
                .tables
                .contains_key("acc_cd"),
            "CREATE+DROP publishes nothing",
        );

        // (2) CREATE+INSERT+DROP → gone.
        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("CREATE TABLE acc_cid (id INT)")
            .await
            .expect("create");
        client
            .batch_execute("INSERT INTO acc_cid VALUES (1)")
            .await
            .expect("insert");
        client
            .batch_execute("DROP TABLE acc_cid")
            .await
            .expect("drop");
        client.batch_execute("COMMIT").await.expect("commit");
        assert!(
            !running
                .server
                .catalog_snapshot()
                .tables
                .contains_key("acc_cid"),
            "CREATE+INSERT+DROP publishes nothing",
        );

        // (3) DROP committed + CREATE same name → new table commits, old gone.
        client
            .batch_execute("CREATE TABLE acc_dc (id INT, old_col INT)")
            .await
            .expect("seed committed table");
        client
            .batch_execute("INSERT INTO acc_dc VALUES (1, 1)")
            .await
            .expect("seed row");
        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("DROP TABLE acc_dc")
            .await
            .expect("drop committed");
        client
            .batch_execute("CREATE TABLE acc_dc (id INT, new_col TEXT)")
            .await
            .expect("recreate same name, new shape");
        client.batch_execute("COMMIT").await.expect("commit");
        // The new shape is what survives.
        client
            .batch_execute("INSERT INTO acc_dc (id, new_col) VALUES (2, 'x')")
            .await
            .expect("new column usable");
        let old = client.query("SELECT old_col FROM acc_dc", &[]).await;
        assert!(old.is_err(), "old column must be gone after recreate");

        // (4a) SAME-TXN CREATE + ALTER + DROP → gone, no stale ALTER replay.
        // A same-txn-created table that is renamed then dropped nets out via the
        // un-stage fast path (its altered staging is stripped too).
        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("CREATE TABLE acc_ad (id INT)")
            .await
            .expect("create same-txn");
        client
            .batch_execute("ALTER TABLE acc_ad RENAME TO acc_ad2")
            .await
            .expect("rename in-txn");
        client
            .batch_execute("DROP TABLE acc_ad2")
            .await
            .expect("drop the altered same-txn table");
        client.batch_execute("COMMIT").await.expect("commit");
        assert!(
            !running
                .server
                .catalog_snapshot()
                .tables
                .contains_key("acc_ad")
                && !running
                    .server
                    .catalog_snapshot()
                    .tables
                    .contains_key("acc_ad2"),
            "same-txn CREATE+ALTER+DROP publishes nothing under either name",
        );

        // (4b) COMMITTED-BEFORE ALTER + DROP → rejected (0A000). Dropping a
        // committed table that was ALTERed earlier in the same txn would have to
        // unwind the ALTER's in-memory side-map edits — out of the minimal M5
        // scope. The ALTER and the DROP each work fine in autocommit.
        client
            .batch_execute("CREATE TABLE acc_abd (id INT)")
            .await
            .expect("seed committed table");
        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("ALTER TABLE acc_abd RENAME TO acc_abd2")
            .await
            .expect("rename committed-before table in-txn");
        let alter_drop_err = client
            .batch_execute("DROP TABLE acc_abd2")
            .await
            .expect_err("dropping a committed-before ALTERed table in-txn rejects");
        assert_eq!(
            sqlstate(&alter_drop_err),
            "0A000",
            "committed-before ALTER+DROP must be feature_not_supported, got {alter_drop_err}",
        );
        client.batch_execute("ROLLBACK").await.expect("rollback");
        // The original committed table survives the rejected DROP.
        client
            .query("SELECT * FROM acc_abd", &[])
            .await
            .expect("committed table survives the rejected ALTER+DROP");

        // (5) DROP + DROP bare → 42P01.
        client
            .batch_execute("CREATE TABLE acc_dd (id INT)")
            .await
            .expect("seed");
        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("DROP TABLE acc_dd")
            .await
            .expect("first drop");
        let second = client
            .batch_execute("DROP TABLE acc_dd")
            .await
            .expect_err("second bare DROP of a masked table is 42P01");
        assert!(is_undefined_table(&second), "expected 42P01, got {second}");
        client.batch_execute("ROLLBACK").await.expect("rollback");

        // (6) DROP + DROP IF EXISTS → no-op (the IF EXISTS second drop succeeds).
        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("DROP TABLE acc_dd")
            .await
            .expect("first drop");
        client
            .batch_execute("DROP TABLE IF EXISTS acc_dd")
            .await
            .expect("second IF EXISTS drop is a no-op success");
        client.batch_execute("COMMIT").await.expect("commit");
        assert!(
            !running
                .server
                .catalog_snapshot()
                .tables
                .contains_key("acc_dd"),
            "DROP+DROP-IF-EXISTS commits the single drop",
        );
        shutdown(running).await;
    }

    // Restart: the committed nets survive (nothing resurrects, recreate persists).
    let running = start_persistent_server(data_dir.path(), "m5_accum_a2").await;
    let snap = running.server.catalog_snapshot();
    assert!(
        !snap.tables.contains_key("acc_cd"),
        "CREATE+DROP stays gone after restart"
    );
    assert!(
        !snap.tables.contains_key("acc_cid"),
        "CREATE+INSERT+DROP stays gone after restart"
    );
    assert!(
        !snap.tables.contains_key("acc_dd"),
        "drop stays gone after restart"
    );
    // The DROP+CREATE-same-name recreate persists with the NEW shape.
    let rows = running
        .client
        .query("SELECT id, new_col FROM acc_dc ORDER BY id", &[])
        .await
        .expect("recreated table present after restart with new shape");
    assert_eq!(rows.len(), 1, "only the post-recreate row survives");
    shutdown(running).await;
}

// ───────────────────────────── M5 #6 ─────────────────────────────
// Concurrent serialization: two sessions DROP the same name → loser 40001;
// DROP-then-recreate same name in ONE txn → no self-deadlock, commits.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_in_txn_drop_same_name_serializes() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "m5_drop_race_a").await;
    let client = &running.client;
    let (client_b, b_handle) = connect_as(running.bound, "tester", "m5_drop_race_b").await;
    client
        .batch_execute("CREATE TABLE drace (id INT)")
        .await
        .expect("seed table");

    client.batch_execute("BEGIN").await.expect("A begin");
    client
        .batch_execute("DROP TABLE drace")
        .await
        .expect("A in-txn DROP takes the name lock");

    client_b.batch_execute("BEGIN").await.expect("B begin");
    let err = client_b
        .batch_execute("DROP TABLE drace")
        .await
        .expect_err("B's same-name DROP must fail while A holds the lock");
    assert_eq!(
        sqlstate(&err),
        "40001",
        "concurrent same-name DROP must report serialization_failure, got {err}"
    );
    client_b
        .batch_execute("ROLLBACK")
        .await
        .expect("B rollback");
    client.batch_execute("ROLLBACK").await.expect("A rollback");

    drop(client_b);
    let _ = b_handle.await;
    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_txn_drop_then_recreate_same_name_no_self_deadlock() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "m5_drop_recreate").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE drr (id INT, old_col INT)")
        .await
        .expect("seed table");

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("DROP TABLE drr")
        .await
        .expect("drop takes the name lock keyed on user xid");
    // Re-acquiring the SAME name lock on the same xid must not self-deadlock.
    client
        .batch_execute("CREATE TABLE drr (id INT, new_col TEXT)")
        .await
        .expect("recreate same name (re-entrant lock, no self-deadlock)");
    client.batch_execute("COMMIT").await.expect("commit");

    client
        .batch_execute("INSERT INTO drr (id, new_col) VALUES (1, 'x')")
        .await
        .expect("recreated table usable with new shape");
    let old = client.query("SELECT old_col FROM drr", &[]).await;
    assert!(old.is_err(), "old column gone after recreate");
    shutdown(running).await;
}

// ───────────────────────────── M5 #7 ─────────────────────────────
// IF EXISTS absent → no-op; SAVEPOINT → 0A000; PREPARE over a drop → rejected.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_txn_drop_if_exists_absent_is_noop() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "m5_drop_ifexists").await;
    let client = &running.client;
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("DROP TABLE IF EXISTS never_existed")
        .await
        .expect("DROP IF EXISTS of an absent table is a no-op success in-txn");
    // The block is NOT failed: a subsequent statement runs fine.
    client
        .query("SELECT 1", &[])
        .await
        .expect("block is healthy after the no-op DROP IF EXISTS");
    client.batch_execute("COMMIT").await.expect("commit");
    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_txn_drop_under_savepoint_is_rejected() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "m5_drop_sp").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE dsp (id INT)")
        .await
        .expect("seed");
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("SAVEPOINT s1")
        .await
        .expect("savepoint");
    let err = client
        .batch_execute("DROP TABLE dsp")
        .await
        .expect_err("DROP under an active SAVEPOINT must reject");
    assert_eq!(sqlstate(&err), "0A000", "expected 0A000, got {err}");
    client.batch_execute("ROLLBACK").await.expect("rollback");
    // Table survives.
    client
        .query("SELECT * FROM dsp", &[])
        .await
        .expect("table survives the rejected DROP");
    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_transaction_over_in_txn_drop_is_rejected() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "m5_drop_prepare").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE dpr (id INT)")
        .await
        .expect("seed");
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("DROP TABLE dpr")
        .await
        .expect("in-txn DROP");
    let err = client
        .batch_execute("PREPARE TRANSACTION 'm5dp'")
        .await
        .expect_err("PREPARE over a drop overlay must reject");
    assert_eq!(sqlstate(&err), "0A000", "expected 0A000, got {err}");
    client.batch_execute("ROLLBACK").await.expect("rollback");
    // Table survives the rejected PREPARE + rollback.
    client
        .query("SELECT * FROM dpr", &[])
        .await
        .expect("table survives");
    shutdown(running).await;
}

// ───────────────────────────── M5 #8 ─────────────────────────────
// Extended/portal path: an in-txn DROP staged + committed via the prepared
// (extended-query) protocol behaves identically (the gate is shared).

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_txn_drop_via_extended_protocol_commits() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "m5_drop_ext").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE dext (id INT)")
        .await
        .expect("seed");
    client
        .batch_execute("INSERT INTO dext VALUES (1)")
        .await
        .expect("seed row");
    client.batch_execute("BEGIN").await.expect("begin");
    // `execute` uses the extended (parse/bind/execute) protocol. (A masked
    // self-SELECT would error 42P01 and FAIL the block — turning COMMIT into a
    // ROLLBACK — so self-visibility of the mask is covered by the simple-query
    // batteries; here we only prove the extended-path stage + commit publishes.)
    client
        .execute("DROP TABLE dext", &[])
        .await
        .expect("in-txn DROP via extended protocol");
    client.batch_execute("COMMIT").await.expect("commit");
    assert!(
        !running
            .server
            .catalog_snapshot()
            .tables
            .contains_key("dext"),
        "extended-path DROP commits the drop",
    );
    // Gone for a fresh statement too.
    let err = client
        .query("SELECT * FROM dext", &[])
        .await
        .expect_err("dropped table absent after commit");
    assert!(is_undefined_table(&err), "expected 42P01, got {err}");
    shutdown(running).await;
}

// ──────────────────── Plan-cache cross-session isolation ────────────────────
// The server-wide plan cache is keyed ONLY by raw SQL text, so a plan it stores
// must be valid for EVERY session that runs that text. When a session has an
// active in-txn catalog overlay (an in-txn ALTER/CREATE/DROP), its DML plans are
// built against the UNCOMMITTED overlay schema and must NEVER enter the shared
// cache — else a concurrent session running the identical SQL text would get a
// cache hit and execute the uncommitted schema (a cross-session isolation
// breach). The fix bypasses the shared cache entirely while the overlay is
// active (no read, no write); COMMIT invalidates the whole cache so other
// sessions re-plan against the NEW committed schema.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plan_cache_does_not_leak_in_txn_alter_plan_cross_session() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "plc_leak_a").await;
    let client = &running.client;

    // Committed baseline: table `plc` with column `c`.
    client
        .batch_execute("CREATE TABLE plc(c int)")
        .await
        .expect("autocommit create");
    client
        .batch_execute("INSERT INTO plc VALUES (1)")
        .await
        .expect("seed row");

    // A second connection, never in a transaction.
    let (client_b, b_handle) = connect_as(running.bound, "tester", "plc_leak_b").await;

    // A begins a txn and renames the column in-txn; the overlay is now active.
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("ALTER TABLE plc RENAME COLUMN c TO d")
        .await
        .expect("in-txn rename column");
    // A builds + runs an overlay plan for the renamed column (cache-bypassed).
    let rows = client
        .query("SELECT d FROM plc", &[])
        .await
        .expect("A sees its own in-txn rename");
    assert_eq!(rows.len(), 1, "A sees the seeded row under the new name");

    // CONCURRENTLY: B (no txn) must see the COMMITTED schema, not A's overlay.
    // `SELECT c FROM plc` MUST succeed (committed column `c` still exists) —
    // this is the leak that the bug produced (a cache HIT on A's overlay plan
    // would surface as 42703 for column `c`). And `SELECT d FROM plc` MUST fail
    // 42703 — B must not see A's uncommitted rename.
    let rows_b = client_b
        .query("SELECT c FROM plc", &[])
        .await
        .expect("B sees committed column c (no leak of A's overlay plan)");
    assert_eq!(rows_b.len(), 1, "B reads the committed row");
    let err_b = client_b
        .query("SELECT d FROM plc", &[])
        .await
        .expect_err("B must NOT see A's uncommitted rename to d");
    assert!(
        is_undefined_column(&err_b),
        "expected 42703 for B's SELECT d, got {err_b}"
    );

    // A commits: the rename is now the committed schema; the whole plan cache is
    // invalidated so B re-plans against the new schema.
    client.batch_execute("COMMIT").await.expect("commit");

    // After commit, B sees the new column `d` and `c` is gone — proving the
    // cache was invalidated rather than serving B a plan built over the OLD
    // schema.
    let rows_b = client_b
        .query("SELECT d FROM plc", &[])
        .await
        .expect("after commit B sees renamed column d (cache invalidated)");
    assert_eq!(rows_b.len(), 1, "B reads the row under the committed name");
    let err_b = client_b
        .query("SELECT c FROM plc", &[])
        .await
        .expect_err("after commit the old column c is gone for B");
    assert!(
        is_undefined_column(&err_b),
        "expected 42703 for B's SELECT c after commit, got {err_b}"
    );

    drop(client_b);
    let _ = b_handle.await;
    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_txn_alter_session_sees_own_schema_not_stale_cached_plan() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "plc_stale_a").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE plc(c int)")
        .await
        .expect("autocommit create");
    client
        .batch_execute("INSERT INTO plc VALUES (7)")
        .await
        .expect("seed row");

    // Pre-warm the shared cache with the committed-schema plan for `SELECT c`.
    let rows = client
        .query("SELECT c FROM plc", &[])
        .await
        .expect("pre-warm committed plan for SELECT c");
    assert_eq!(rows.len(), 1);

    // In-txn rename, then the SAME-text SELECT against the NEW name must build a
    // fresh overlay plan (the cache is bypassed while the overlay is active) —
    // A is NOT served the stale committed plan that ignores its own DDL.
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("ALTER TABLE plc RENAME COLUMN c TO d")
        .await
        .expect("in-txn rename column");
    let rows = client
        .query("SELECT d FROM plc", &[])
        .await
        .expect("A sees its own rename (fresh overlay plan, not stale cache)");
    assert_eq!(rows.len(), 1, "A reads the seeded row under the new name");

    // The old name `c` is gone for A in-txn: the pre-warmed `SELECT c` plan must
    // NOT be served from the shared cache while the overlay is active.
    let err = client
        .query("SELECT c FROM plc", &[])
        .await
        .expect_err("in-txn, the pre-rename column c must be gone for A");
    assert!(
        is_undefined_column(&err),
        "expected 42703 for in-txn SELECT c, got {err}"
    );

    client.batch_execute("COMMIT").await.expect("commit");
    shutdown(running).await;
}

// ───────────── ROOT-A: utility / COPY paths route through the overlay ─────────
// COPY-query / VACUUM / ANALYZE resolved through the RAW committed snapshot,
// not the per-txn overlay, so each failed 42P01 (or aborted the txn) for the
// session's OWN in-txn-created table. The fix routes them through
// `effective_catalog_snapshot()`.

/// Drain a `tokio_postgres::CopyOutStream` to a single `Vec<u8>`.
async fn drain_copy_out(stream: tokio_postgres::CopyOutStream) -> Vec<u8> {
    use futures::StreamExt;
    let mut stream = Box::pin(stream);
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk.expect("CopyData chunk"));
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_query_to_over_in_txn_created_table_is_self_visible() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_copyq_self").await;
    let client = &running.client;

    // Seed COMMITTED rows so the COPY query's own ReadCommitted snapshot has
    // visible rows to emit, proving schema resolution + row emission end to end.
    client
        .batch_execute("CREATE TABLE cq (id INT NOT NULL)")
        .await
        .expect("autocommit create");
    client
        .batch_execute("INSERT INTO cq VALUES (1), (2), (3)")
        .await
        .expect("autocommit insert (committed rows)");

    // In-txn ALTER-RENAME, then a COPY query selecting the NEW (overlay) column
    // name must resolve the schema through the overlay — pre-fix this failed
    // 42P01 (the COPY-query path re-fetched the RAW committed snapshot). The
    // rows are the COMMITTED rows under the renamed column.
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("ALTER TABLE cq RENAME COLUMN id TO renamed_id")
        .await
        .expect("in-txn rename column");
    let stream = client
        .copy_out("COPY (SELECT renamed_id FROM cq ORDER BY renamed_id) TO STDOUT")
        .await
        .expect("COPY query resolves the in-txn-renamed column (no 42P01/42703)");
    let bytes = drain_copy_out(stream).await;
    assert_eq!(
        bytes,
        b"1\n2\n3\n".to_vec(),
        "COPY query emits the committed rows under the overlay-renamed column"
    );
    client.batch_execute("COMMIT").await.expect("commit");

    // In-txn CREATE variant: `COPY (SELECT … FROM t) TO …` over an
    // in-txn-CREATED table must resolve the schema (no 42P01). The COPY query
    // runs in its own ReadCommitted txn, so the session's still-uncommitted
    // INSERTs are not visible to it (COPY-query txn-atomicity is a separate,
    // deferred concern); the assertion here is the ROOT-A schema-resolution
    // fix: the table is FOUND and the COPY completes cleanly instead of 42P01.
    client.batch_execute("BEGIN").await.expect("begin 2");
    client
        .batch_execute("CREATE TABLE cq2 (id INT NOT NULL)")
        .await
        .expect("in-txn create");
    client
        .batch_execute("INSERT INTO cq2 VALUES (7), (8)")
        .await
        .expect("in-txn insert");
    let stream = client
        .copy_out("COPY (SELECT id FROM cq2 ORDER BY id) TO STDOUT")
        .await
        .expect("COPY query over self-created in-txn table resolves (no 42P01)");
    // Drains cleanly; row visibility under the session's own xid is the
    // deferred atomicity concern, so we only assert no error here.
    let _ = drain_copy_out(stream).await;
    client.batch_execute("COMMIT").await.expect("commit 2");

    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vacuum_in_txn_created_table_no_42p01() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_vac_self").await;
    let client = &running.client;

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("CREATE TABLE vac_t (id INT NOT NULL)")
        .await
        .expect("in-txn create");
    client
        .batch_execute("INSERT INTO vac_t VALUES (10), (20)")
        .await
        .expect("in-txn insert");

    // `VACUUM <table>` over the in-txn-created table must resolve it — pre-fix
    // this failed 42P01 and aborted the transaction.
    client
        .batch_execute("VACUUM vac_t")
        .await
        .expect("VACUUM resolves the self-created in-txn table");

    // The transaction is still live and the table still readable.
    let rows = client
        .query("SELECT id FROM vac_t ORDER BY id", &[])
        .await
        .expect("table readable after in-txn VACUUM (txn not aborted)");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 10);

    client.batch_execute("COMMIT").await.expect("commit");
    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn analyze_in_txn_created_table_no_42p01() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_ana_self").await;
    let client = &running.client;

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("CREATE TABLE ana_t (id INT NOT NULL)")
        .await
        .expect("in-txn create");
    client
        .batch_execute("INSERT INTO ana_t VALUES (1), (2), (3)")
        .await
        .expect("in-txn insert");

    // `ANALYZE <table>` over the in-txn-created table must resolve it — pre-fix
    // the Server-global resolver missed the overlay and ANALYZE failed 42P01.
    client
        .batch_execute("ANALYZE ana_t")
        .await
        .expect("ANALYZE resolves the self-created in-txn table");

    let rows = client
        .query("SELECT count(*) FROM ana_t", &[])
        .await
        .expect("table readable after in-txn ANALYZE (txn not aborted)");
    assert_eq!(rows[0].get::<_, i64>(0), 3);

    client.batch_execute("COMMIT").await.expect("commit");
    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bare_analyze_in_txn_sees_created_and_skips_dropped() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "txddl_bare_ana").await;
    let client = &running.client;

    // A committed table that will be DROPPED in-txn — bare ANALYZE must NOT
    // touch it through the overlay (the overlay hides it).
    client
        .batch_execute("CREATE TABLE bare_dropped (id INT NOT NULL)")
        .await
        .expect("autocommit create of soon-dropped table");

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("CREATE TABLE bare_created (id INT NOT NULL)")
        .await
        .expect("in-txn create");
    client
        .batch_execute("INSERT INTO bare_created VALUES (1)")
        .await
        .expect("in-txn insert");
    client
        .batch_execute("DROP TABLE bare_dropped")
        .await
        .expect("in-txn drop");

    // Bare `ANALYZE` iterates the overlay snapshot: it sees `bare_created` and
    // skips the overlay-dropped `bare_dropped`, and must not abort the txn.
    client
        .batch_execute("ANALYZE")
        .await
        .expect("bare ANALYZE iterates the overlay snapshot without 42P01");

    // The transaction is still live: the in-txn-created table is still there,
    // the in-txn-dropped one is gone for this session.
    let rows = client
        .query("SELECT count(*) FROM bare_created", &[])
        .await
        .expect("in-txn-created table readable after bare ANALYZE");
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    let err = client
        .query("SELECT * FROM bare_dropped", &[])
        .await
        .expect_err("in-txn-dropped table is gone for this session");
    assert!(is_undefined_table(&err), "expected 42P01, got {err}");

    client.batch_execute("COMMIT").await.expect("commit");
    shutdown(running).await;
}
