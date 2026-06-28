//! Two-connection regression tests for the savepoint ↔ row-lock interaction.
//!
//! Proves the two PostgreSQL-14 behaviours fixed alongside the
//! ROLLBACK-TO-SAVEPOINT bug batch (both verified against a real PostgreSQL 14
//! server before this file was written):
//!
//! (A) `ROLLBACK TO SAVEPOINT` releases the row locks acquired *since* that
//!     savepoint — a peer `FOR UPDATE NOWAIT` on the row then succeeds
//!     immediately, while the rolled-back transaction stays open. Locks taken
//!     *before* the savepoint stay held until the top-level transaction ends.
//!     Covered for the explicit `SELECT ... FOR UPDATE` path, the `UPDATE`
//!     write path, and the released-then-rolled-back inner-savepoint case.
//!
//! (B) A row lock first taken *inside* a savepoint is released at `COMMIT`; it
//!     does not leak past transaction end. The lock is held under the stable
//!     top-level xid (not the savepoint subxid), so `release_all` at commit
//!     reclaims it.
//!
//! These exercise `acquire_row_locks` / `acquire_skip_locked` (txn_exec.rs),
//! the `UPDATE` lock path (pipeline/modify), and
//! `TransactionManager::rollback_to_savepoint` → `LockManager::release_subxact_locks`
//! end-to-end through a real `tokio-postgres` client.

use tokio_postgres::error::SqlState;

pub mod support;

use support::{connect_as, shutdown, start_sample_server};

/// (A) Explicit `SELECT ... FOR UPDATE` taken inside a savepoint is released by
/// `ROLLBACK TO SAVEPOINT`: a peer `FOR UPDATE NOWAIT` conflicts while the
/// savepoint holds it, then succeeds right after the rollback — the top-level
/// transaction still open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_to_savepoint_releases_for_update_lock() {
    let running = start_sample_server("sp_fu_release").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE t (id INT NOT NULL, v INT NOT NULL);
         INSERT INTO t VALUES (1, 10), (2, 20);
         CREATE INDEX t_id_idx ON t(id);",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "sp_fu_release_b").await;

    a.batch_execute("BEGIN").await.expect("A begin");
    a.batch_execute("SAVEPOINT s1").await.expect("A savepoint");
    a.query("SELECT id FROM t WHERE id = 1 FOR UPDATE", &[])
        .await
        .expect("A locks id=1 under s1");

    // While the savepoint holds the lock, a peer NOWAIT conflicts (55P03).
    let err = b
        .query("SELECT id FROM t WHERE id = 1 FOR UPDATE NOWAIT", &[])
        .await
        .expect_err("row locked under the savepoint");
    assert_eq!(
        err.code(),
        Some(&SqlState::LOCK_NOT_AVAILABLE),
        "expected 55P03 while the lock is held, got {err:?}"
    );

    // ROLLBACK TO releases the lock taken since the savepoint — the txn lives.
    a.batch_execute("ROLLBACK TO SAVEPOINT s1")
        .await
        .expect("A rollback to s1");

    // PG: the peer can now lock the row immediately.
    b.query("SELECT id FROM t WHERE id = 1 FOR UPDATE NOWAIT", &[])
        .await
        .expect("lock released by ROLLBACK TO — peer NOWAIT now succeeds");

    a.batch_execute("ROLLBACK").await.expect("A rollback");
    b_conn.abort();
    shutdown(running).await;
}

/// (A) An `UPDATE`-acquired row lock inside a savepoint is released by
/// `ROLLBACK TO SAVEPOINT`, and the write is reverted: the peer both re-locks
/// the row and observes the pre-update value. This is the exact scenario the
/// bug report verified against PostgreSQL 14.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_to_savepoint_releases_update_row_lock_and_reverts() {
    let running = start_sample_server("sp_upd_release").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE t (id INT NOT NULL, v INT NOT NULL);
         INSERT INTO t VALUES (1, 10), (2, 20);
         CREATE INDEX t_id_idx ON t(id);",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "sp_upd_release_b").await;

    a.batch_execute("BEGIN").await.expect("A begin");
    a.batch_execute("SAVEPOINT s1").await.expect("A savepoint");
    a.batch_execute("UPDATE t SET v = v + 1 WHERE id = 1")
        .await
        .expect("A updates id=1 under s1 (takes the row lock)");

    let err = b
        .query("SELECT id FROM t WHERE id = 1 FOR UPDATE NOWAIT", &[])
        .await
        .expect_err("row locked by the in-savepoint UPDATE");
    assert_eq!(
        err.code(),
        Some(&SqlState::LOCK_NOT_AVAILABLE),
        "expected 55P03 while the UPDATE lock is held, got {err:?}"
    );

    a.batch_execute("ROLLBACK TO SAVEPOINT s1")
        .await
        .expect("A rollback to s1");

    // PG: lock released AND the UPDATE reverted (v back to 10).
    let rows = b
        .query("SELECT v FROM t WHERE id = 1 FOR UPDATE NOWAIT", &[])
        .await
        .expect("lock released by ROLLBACK TO — peer NOWAIT now succeeds");
    assert_eq!(rows.len(), 1);
    let v: i32 = rows[0].get(0);
    assert_eq!(v, 10, "the in-savepoint UPDATE was reverted by ROLLBACK TO");

    a.batch_execute("ROLLBACK").await.expect("A rollback");
    b_conn.abort();
    shutdown(running).await;
}

/// (A) A lock taken *before* the savepoint survives `ROLLBACK TO`, while a lock
/// taken *after* it is released. Distinguishes the two by owner (top-level xid
/// vs savepoint subxid).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lock_before_savepoint_survives_rollback_to() {
    let running = start_sample_server("sp_before_after").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE t (id INT NOT NULL, v INT NOT NULL);
         INSERT INTO t VALUES (1, 10), (2, 20);
         CREATE INDEX t_id_idx ON t(id);",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "sp_before_after_b").await;

    a.batch_execute("BEGIN").await.expect("A begin");
    // id=1 locked BEFORE the savepoint → owned by the top-level xid.
    a.batch_execute("UPDATE t SET v = v + 1 WHERE id = 1")
        .await
        .expect("A updates id=1 before the savepoint");
    a.batch_execute("SAVEPOINT s1").await.expect("A savepoint");
    // id=2 locked AFTER the savepoint → owned by the savepoint subxid.
    a.batch_execute("UPDATE t SET v = v + 1 WHERE id = 2")
        .await
        .expect("A updates id=2 under s1");

    a.batch_execute("ROLLBACK TO SAVEPOINT s1")
        .await
        .expect("A rollback to s1");

    // id=1 (locked before the savepoint) must STILL be locked.
    let err = b
        .query("SELECT id FROM t WHERE id = 1 FOR UPDATE NOWAIT", &[])
        .await
        .expect_err("pre-savepoint lock must survive ROLLBACK TO");
    assert_eq!(
        err.code(),
        Some(&SqlState::LOCK_NOT_AVAILABLE),
        "id=1 lock (taken before the savepoint) must survive, got {err:?}"
    );

    // id=2 (locked under the savepoint) must be released and reverted.
    let rows = b
        .query("SELECT v FROM t WHERE id = 2 FOR UPDATE NOWAIT", &[])
        .await
        .expect("in-savepoint lock on id=2 released by ROLLBACK TO");
    let v2: i32 = rows[0].get(0);
    assert_eq!(v2, 20, "id=2 update reverted by ROLLBACK TO");

    // The pre-savepoint update survives to commit.
    a.batch_execute("COMMIT").await.expect("A commit");
    let v1: i32 = a
        .query_one("SELECT v FROM t WHERE id = 1", &[])
        .await
        .expect("read id=1")
        .get(0);
    assert_eq!(v1, 11, "the pre-savepoint UPDATE committed");

    b_conn.abort();
    shutdown(running).await;
}

/// (A) A lock taken inside an inner savepoint that is then `RELEASE`d (merged
/// up) is still released when an *outer* `ROLLBACK TO` discards the region that
/// contained it. Exercises the merged-up-subxid pruning path of
/// `release_subxact_locks`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_to_outer_releases_lock_from_released_inner_savepoint() {
    let running = start_sample_server("sp_nested_release").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE t (id INT NOT NULL, v INT NOT NULL);
         INSERT INTO t VALUES (1, 10);
         CREATE INDEX t_id_idx ON t(id);",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "sp_nested_release_b").await;

    a.batch_execute("BEGIN").await.expect("A begin");
    a.batch_execute("SAVEPOINT s1").await.expect("A savepoint s1");
    a.batch_execute("SAVEPOINT s2").await.expect("A savepoint s2");
    a.query("SELECT id FROM t WHERE id = 1 FOR UPDATE", &[])
        .await
        .expect("A locks id=1 under s2");
    a.batch_execute("RELEASE SAVEPOINT s2")
        .await
        .expect("A releases s2 (merges into s1's scope)");

    // Still held after RELEASE (the writes merged up, the lock did not drop).
    let err = b
        .query("SELECT id FROM t WHERE id = 1 FOR UPDATE NOWAIT", &[])
        .await
        .expect_err("merged-up lock still held before the outer rollback");
    assert_eq!(err.code(), Some(&SqlState::LOCK_NOT_AVAILABLE));

    // Rolling back to the OUTER savepoint discards the merged-up region too.
    a.batch_execute("ROLLBACK TO SAVEPOINT s1")
        .await
        .expect("A rollback to s1");

    b.query("SELECT id FROM t WHERE id = 1 FOR UPDATE NOWAIT", &[])
        .await
        .expect("lock from released-then-rolled-back inner savepoint is freed");

    a.batch_execute("ROLLBACK").await.expect("A rollback");
    b_conn.abort();
    shutdown(running).await;
}

/// (B) A `FOR UPDATE` first taken inside a savepoint must not leak past the
/// top-level `COMMIT`: it stays held through `RELEASE` and until commit, then a
/// peer `NOWAIT` succeeds. Before the fix the lock was held under the savepoint
/// subxid and `release_all` (keyed on the top-level xid) missed it at commit —
/// a permanent leak.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn for_update_in_savepoint_released_at_commit_not_leaked() {
    let running = start_sample_server("sp_fu_commit").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE t (id INT NOT NULL, v INT NOT NULL);
         INSERT INTO t VALUES (1, 10);
         CREATE INDEX t_id_idx ON t(id);",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "sp_fu_commit_b").await;

    a.batch_execute("BEGIN").await.expect("A begin");
    a.batch_execute("SAVEPOINT s1").await.expect("A savepoint");
    a.query("SELECT id FROM t WHERE id = 1 FOR UPDATE", &[])
        .await
        .expect("A locks id=1 under s1");
    a.batch_execute("RELEASE SAVEPOINT s1")
        .await
        .expect("A releases s1");

    // Still held after RELEASE, before COMMIT.
    let err = b
        .query("SELECT id FROM t WHERE id = 1 FOR UPDATE NOWAIT", &[])
        .await
        .expect_err("lock held until the top-level transaction commits");
    assert_eq!(err.code(), Some(&SqlState::LOCK_NOT_AVAILABLE));

    a.batch_execute("COMMIT").await.expect("A commit");

    // Released at commit — no leak.
    b.query("SELECT id FROM t WHERE id = 1 FOR UPDATE NOWAIT", &[])
        .await
        .expect("savepoint-acquired lock released at COMMIT, not leaked");

    b_conn.abort();
    shutdown(running).await;
}
