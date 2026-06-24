//! Persistent two-phase-commit restart coverage through the PostgreSQL wire path.

pub mod support;

use support::{shutdown, start_persistent_server};

async fn count_rows(client: &tokio_postgres::Client, table: &str) -> i64 {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    client
        .query_one(&sql, &[])
        .await
        .expect("count rows")
        .get(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_prepared_survives_restart_and_makes_rows_visible() {
    let data_dir = tempfile::TempDir::new().expect("data dir");
    support::make_data_dir_private(data_dir.path());

    let running = start_persistent_server(data_dir.path(), "two_phase_restart_prepare").await;
    running
        .client
        .batch_execute("CREATE TABLE two_phase_restart (id INT NOT NULL, note TEXT)")
        .await
        .expect("create table");
    running
        .client
        .batch_execute(
            "BEGIN; \
             INSERT INTO two_phase_restart VALUES (1, 'prepared'); \
             PREPARE TRANSACTION 'restart-gid'",
        )
        .await
        .expect("prepare transaction");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "two_phase_restart_commit").await;
    assert_eq!(count_rows(&running.client, "two_phase_restart").await, 0);
    running
        .client
        .batch_execute("COMMIT PREPARED 'restart-gid'")
        .await
        .expect("commit prepared after restart");
    let row = running
        .client
        .query_one("SELECT note FROM two_phase_restart WHERE id = 1", &[])
        .await
        .expect("prepared row visible after commit");
    let note: &str = row.get(0);
    assert_eq!(note, "prepared");
    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rollback_prepared_survives_restart_and_discards_rows() {
    let data_dir = tempfile::TempDir::new().expect("data dir");
    support::make_data_dir_private(data_dir.path());

    let running = start_persistent_server(data_dir.path(), "two_phase_restart_prepare").await;
    running
        .client
        .batch_execute("CREATE TABLE two_phase_restart_abort (id INT NOT NULL, note TEXT)")
        .await
        .expect("create table");
    running
        .client
        .batch_execute(
            "BEGIN; \
             INSERT INTO two_phase_restart_abort VALUES (1, 'prepared'); \
             PREPARE TRANSACTION 'restart-rollback-gid'",
        )
        .await
        .expect("prepare transaction");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "two_phase_restart_rollback").await;
    running
        .client
        .batch_execute("ROLLBACK PREPARED 'restart-rollback-gid'")
        .await
        .expect("rollback prepared after restart");
    assert_eq!(
        count_rows(&running.client, "two_phase_restart_abort").await,
        0
    );
    shutdown(running).await;
}

/// Regression gate for the 2PC savepoint recovery data-loss bug.
///
/// A row inserted under a `RELEASE`d savepoint inside a transaction that is
/// `PREPARE`d and later `COMMIT PREPARED` must survive a pure-WAL restart. Before
/// the fix the committed-subxid family was not carried through PREPARE / COMMIT
/// PREPARED, so the Commit record listed no subxids, recovery's default-abort
/// sweep marked the savepoint subxid `Aborted`, and the row vanished
/// (`COUNT(*)` came back `0` instead of `1`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_prepared_released_savepoint_row_survives_restart() {
    let data_dir = tempfile::TempDir::new().expect("data dir");
    support::make_data_dir_private(data_dir.path());

    let running = start_persistent_server(data_dir.path(), "2pc_sp_released_prepare").await;
    running
        .client
        .batch_execute("CREATE TABLE sp_released (id INT NOT NULL, note TEXT)")
        .await
        .expect("create table");
    // Row written under a savepoint that is RELEASEd (merged up) before PREPARE.
    running
        .client
        .batch_execute(
            "BEGIN; \
             SAVEPOINT s; \
             INSERT INTO sp_released VALUES (1, 'under-savepoint'); \
             RELEASE SAVEPOINT s; \
             PREPARE TRANSACTION 'sp-released-gid'",
        )
        .await
        .expect("prepare transaction with released savepoint");
    shutdown(running).await;

    // Phase 2 after restart, then a SECOND restart to force pure-WAL replay of
    // the COMMIT PREPARED commit record.
    let running = start_persistent_server(data_dir.path(), "2pc_sp_released_commit").await;
    running
        .client
        .batch_execute("COMMIT PREPARED 'sp-released-gid'")
        .await
        .expect("commit prepared after restart");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "2pc_sp_released_verify").await;
    assert_eq!(
        count_rows(&running.client, "sp_released").await,
        1,
        "row under a RELEASEd savepoint must survive COMMIT PREPARED + restart"
    );
    let note: String = running
        .client
        .query_one("SELECT note FROM sp_released WHERE id = 1", &[])
        .await
        .expect("savepoint row present after restart")
        .get(0);
    assert_eq!(note, "under-savepoint");
    shutdown(running).await;
}

/// Variant: the savepoint is still OPEN at PREPARE (no RELEASE). PostgreSQL
/// implicitly releases an open savepoint at commit, so the subxid is part of
/// the committed family and its row must likewise survive COMMIT PREPARED +
/// restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_prepared_open_savepoint_row_survives_restart() {
    let data_dir = tempfile::TempDir::new().expect("data dir");
    support::make_data_dir_private(data_dir.path());

    let running = start_persistent_server(data_dir.path(), "2pc_sp_open_prepare").await;
    running
        .client
        .batch_execute("CREATE TABLE sp_open (id INT NOT NULL, note TEXT)")
        .await
        .expect("create table");
    running
        .client
        .batch_execute(
            "BEGIN; \
             SAVEPOINT s; \
             INSERT INTO sp_open VALUES (1, 'still-open'); \
             PREPARE TRANSACTION 'sp-open-gid'",
        )
        .await
        .expect("prepare transaction with open savepoint");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "2pc_sp_open_commit").await;
    running
        .client
        .batch_execute("COMMIT PREPARED 'sp-open-gid'")
        .await
        .expect("commit prepared after restart");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "2pc_sp_open_verify").await;
    assert_eq!(
        count_rows(&running.client, "sp_open").await,
        1,
        "row under an open-at-prepare savepoint must survive COMMIT PREPARED + restart"
    );
    shutdown(running).await;
}

/// Proves the committed-subxid family survives PREPARE itself: restart happens
/// BETWEEN `PREPARE TRANSACTION` and `COMMIT PREPARED`, so the family must be
/// reconstructed purely from the durable 2PC state file (the original
/// transaction's in-memory savepoint stack is long gone).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_subxid_family_survives_restart_after_prepare() {
    let data_dir = tempfile::TempDir::new().expect("data dir");
    support::make_data_dir_private(data_dir.path());

    let running = start_persistent_server(data_dir.path(), "2pc_sp_family_prepare").await;
    running
        .client
        .batch_execute("CREATE TABLE sp_family (id INT NOT NULL)")
        .await
        .expect("create table");
    running
        .client
        .batch_execute(
            "BEGIN; \
             SAVEPOINT a; \
             INSERT INTO sp_family VALUES (1); \
             RELEASE SAVEPOINT a; \
             SAVEPOINT b; \
             INSERT INTO sp_family VALUES (2); \
             PREPARE TRANSACTION 'sp-family-gid'",
        )
        .await
        .expect("prepare transaction with released + open savepoints");
    shutdown(running).await;

    // Restart #1: in doubt. The family lives only in the state file now.
    let running = start_persistent_server(data_dir.path(), "2pc_sp_family_restart1").await;
    assert_eq!(count_rows(&running.client, "sp_family").await, 0);
    running
        .client
        .batch_execute("COMMIT PREPARED 'sp-family-gid'")
        .await
        .expect("commit prepared after restart");
    shutdown(running).await;

    // Restart #2: pure-WAL replay of the COMMIT PREPARED commit record.
    let running = start_persistent_server(data_dir.path(), "2pc_sp_family_restart2").await;
    assert_eq!(
        count_rows(&running.client, "sp_family").await,
        2,
        "both savepoint rows (released + open) must survive when the family is \
         reconstructed from the prepared state file across a restart"
    );
    shutdown(running).await;
}

/// In-doubt recovery gate (the missing direction): restart happens BETWEEN
/// `PREPARE TRANSACTION` and `COMMIT PREPARED`, then the row written under a
/// `RELEASE`d savepoint must be visible **in the SAME live process** right
/// after COMMIT PREPARED — with NO further restart.
///
/// Before the fix this failed: at recovery only the PARENT xid was re-seeded
/// `InProgress`; the savepoint subxid had a heap-insert WAL record but no Commit
/// record yet, so the default-abort sweep marked it `Aborted` in memory. COMMIT
/// PREPARED's `terminate_with_subxids` upgraded a subxid only if it was still
/// `InProgress`, so the `Aborted` subxid was skipped and stayed `Aborted`, and
/// the row's xmin pointed at an aborted subxid — invisible (`COUNT(*)` = 0)
/// until a *further* restart re-applied the durable Commit record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_prepared_savepoint_row_visible_in_same_process_after_prepare_restart() {
    let data_dir = tempfile::TempDir::new().expect("data dir");
    support::make_data_dir_private(data_dir.path());

    let running = start_persistent_server(data_dir.path(), "2pc_sp_indoubt_rel_prepare").await;
    running
        .client
        .batch_execute("CREATE TABLE sp_indoubt_rel (id INT NOT NULL, note TEXT)")
        .await
        .expect("create table");
    running
        .client
        .batch_execute(
            "BEGIN; \
             SAVEPOINT s; \
             INSERT INTO sp_indoubt_rel VALUES (1, 'under-savepoint'); \
             RELEASE SAVEPOINT s; \
             PREPARE TRANSACTION 'sp-indoubt-rel-gid'",
        )
        .await
        .expect("prepare transaction with released savepoint");
    shutdown(running).await;

    // Restart #1: in doubt. The family lives only in the state file now and the
    // savepoint subxid is swept to Aborted by recovery's default-abort pass.
    let running = start_persistent_server(data_dir.path(), "2pc_sp_indoubt_rel_commit").await;
    assert_eq!(count_rows(&running.client, "sp_indoubt_rel").await, 0);
    running
        .client
        .batch_execute("COMMIT PREPARED 'sp-indoubt-rel-gid'")
        .await
        .expect("commit prepared after restart");

    // SAME live process, NO further restart: the row must be visible now.
    assert_eq!(
        count_rows(&running.client, "sp_indoubt_rel").await,
        1,
        "row under a RELEASEd savepoint must be visible in the same process \
         immediately after COMMIT PREPARED following a prepare-restart"
    );
    let note: String = running
        .client
        .query_one("SELECT note FROM sp_indoubt_rel WHERE id = 1", &[])
        .await
        .expect("savepoint row present in same process after COMMIT PREPARED")
        .get(0);
    assert_eq!(note, "under-savepoint");
    shutdown(running).await;
}

/// Open-at-prepare variant of the in-doubt recovery gate: the savepoint is still
/// OPEN at PREPARE (implicitly released at commit). The row must be visible in
/// the SAME live process immediately after COMMIT PREPARED following a
/// prepare-restart, with NO further restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_prepared_open_savepoint_row_visible_in_same_process_after_prepare_restart() {
    let data_dir = tempfile::TempDir::new().expect("data dir");
    support::make_data_dir_private(data_dir.path());

    let running = start_persistent_server(data_dir.path(), "2pc_sp_indoubt_open_prepare").await;
    running
        .client
        .batch_execute("CREATE TABLE sp_indoubt_open (id INT NOT NULL, note TEXT)")
        .await
        .expect("create table");
    running
        .client
        .batch_execute(
            "BEGIN; \
             SAVEPOINT s; \
             INSERT INTO sp_indoubt_open VALUES (1, 'still-open'); \
             PREPARE TRANSACTION 'sp-indoubt-open-gid'",
        )
        .await
        .expect("prepare transaction with open savepoint");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "2pc_sp_indoubt_open_commit").await;
    assert_eq!(count_rows(&running.client, "sp_indoubt_open").await, 0);
    running
        .client
        .batch_execute("COMMIT PREPARED 'sp-indoubt-open-gid'")
        .await
        .expect("commit prepared after restart");

    // SAME live process, NO further restart.
    assert_eq!(
        count_rows(&running.client, "sp_indoubt_open").await,
        1,
        "row under an open-at-prepare savepoint must be visible in the same \
         process immediately after COMMIT PREPARED following a prepare-restart"
    );
    shutdown(running).await;
}

/// Negative in-doubt gate: restart BETWEEN PREPARE and ROLLBACK PREPARED, then
/// ROLLBACK PREPARED in the SAME live process — the savepoint row must remain
/// ABSENT (the family stays aborted, matching the recovered default).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rollback_prepared_savepoint_row_invisible_in_same_process_after_prepare_restart() {
    let data_dir = tempfile::TempDir::new().expect("data dir");
    support::make_data_dir_private(data_dir.path());

    let running = start_persistent_server(data_dir.path(), "2pc_sp_indoubt_rb_prepare").await;
    running
        .client
        .batch_execute("CREATE TABLE sp_indoubt_rb (id INT NOT NULL)")
        .await
        .expect("create table");
    running
        .client
        .batch_execute(
            "BEGIN; \
             SAVEPOINT s; \
             INSERT INTO sp_indoubt_rb VALUES (1); \
             RELEASE SAVEPOINT s; \
             PREPARE TRANSACTION 'sp-indoubt-rb-gid'",
        )
        .await
        .expect("prepare transaction with released savepoint");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "2pc_sp_indoubt_rb_abort").await;
    assert_eq!(count_rows(&running.client, "sp_indoubt_rb").await, 0);
    running
        .client
        .batch_execute("ROLLBACK PREPARED 'sp-indoubt-rb-gid'")
        .await
        .expect("rollback prepared after restart");

    // SAME live process, NO further restart: the row must stay absent.
    assert_eq!(
        count_rows(&running.client, "sp_indoubt_rb").await,
        0,
        "a savepoint row in a ROLLBACK PREPARED txn must stay absent in the \
         same process after a prepare-restart"
    );
    shutdown(running).await;
}

/// Negative gate: a savepoint row inside a prepared transaction that is
/// `ROLLBACK PREPARED` must NOT survive — the subxid appears in no committed
/// list, so recovery's default-abort sweep correctly discards it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rollback_prepared_savepoint_row_does_not_survive_restart() {
    let data_dir = tempfile::TempDir::new().expect("data dir");
    support::make_data_dir_private(data_dir.path());

    let running = start_persistent_server(data_dir.path(), "2pc_sp_rollback_prepare").await;
    running
        .client
        .batch_execute("CREATE TABLE sp_rollback (id INT NOT NULL)")
        .await
        .expect("create table");
    running
        .client
        .batch_execute(
            "BEGIN; \
             SAVEPOINT s; \
             INSERT INTO sp_rollback VALUES (1); \
             RELEASE SAVEPOINT s; \
             PREPARE TRANSACTION 'sp-rollback-gid'",
        )
        .await
        .expect("prepare transaction with released savepoint");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "2pc_sp_rollback_abort").await;
    running
        .client
        .batch_execute("ROLLBACK PREPARED 'sp-rollback-gid'")
        .await
        .expect("rollback prepared after restart");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "2pc_sp_rollback_verify").await;
    assert_eq!(
        count_rows(&running.client, "sp_rollback").await,
        0,
        "a savepoint row in a ROLLBACK PREPARED txn must not survive restart"
    );
    shutdown(running).await;
}
