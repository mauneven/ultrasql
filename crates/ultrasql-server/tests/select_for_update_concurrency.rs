//! Adversarial two-connection tests for real `SELECT ... FOR UPDATE /
//! FOR SHARE` row locking.
//!
//! These exercise the lock-acquisition wiring in
//! `crates/ultrasql-server/src/txn_exec.rs` (`acquire_simple_lock_rows` /
//! `acquire_row_locks` / `acquire_skip_locked`) end-to-end through a real
//! `tokio-postgres` client, proving:
//!
//! 1. lost update prevented — `FOR UPDATE` then a peer `UPDATE` blocks
//!    until the lock holder ends its transaction;
//! 2. `FOR UPDATE` vs `FOR UPDATE` — Wait blocks, NOWAIT → 55P03,
//!    SKIP LOCKED skips the locked row;
//! 3. any-plan-shape — `FOR UPDATE` over a JOIN / multi-table select
//!    locks the base rows (a peer lock blocks), never silent-no-lock;
//! 4. no spurious 40001 — plain contended `FOR UPDATE` (Wait) blocks
//!    then proceeds, it does not raise serialization_failure;
//! 5. SKIP LOCKED job queue — two workers grab different rows;
//! 6. deadlock — A waits on B's row while B waits on A's → one victim
//!    gets 40P01, the other proceeds;
//! 7. no regression — a plain SELECT takes no locks; `FOR UPDATE` then an
//!    `UPDATE` of the locked row in the same txn works.

use std::time::Duration;

use tokio_postgres::error::SqlState;

pub mod support;

use support::{connect_as, shutdown, start_sample_server};

/// Poll a future to a short deadline, returning `None` if it is still
/// pending — used to assert that a peer statement is blocked.
async fn is_pending<F, T>(fut: std::pin::Pin<&mut F>, ms: u64) -> bool
where
    F: std::future::Future<Output = T>,
{
    tokio::time::timeout(Duration::from_millis(ms), fut)
        .await
        .is_err()
}

/// 1. LOST-UPDATE PREVENTED: A holds `FOR UPDATE` on id=1; B's `UPDATE`
///    of id=1 blocks until A commits, then applies — no lost update.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn for_update_blocks_concurrent_update_no_lost_update() {
    let running = start_sample_server("fu_lost_update").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE t (id INT NOT NULL, v INT NOT NULL);
         INSERT INTO t VALUES (1, 10), (2, 20);
         CREATE INDEX t_id_idx ON t(id);",
    )
    .await
    .expect("setup");

    let (b, b_conn) = connect_as(running.bound, "tester", "fu_lost_update_b").await;

    a.batch_execute("BEGIN").await.expect("A begin");
    let locked = a
        .query("SELECT id, v FROM t WHERE id = 1 FOR UPDATE", &[])
        .await
        .expect("A locks id=1");
    assert_eq!(locked.len(), 1);

    // B's UPDATE of the locked row must block while A holds the lock.
    let mut b_update = Box::pin(b.batch_execute("UPDATE t SET v = v + 100 WHERE id = 1"));
    assert!(
        is_pending(b_update.as_mut(), 300).await,
        "B's UPDATE must block while A holds FOR UPDATE"
    );

    // Release A; B should then complete promptly.
    a.batch_execute("COMMIT").await.expect("A commit");
    tokio::time::timeout(Duration::from_secs(5), b_update)
        .await
        .expect("B's UPDATE unblocks after A commits")
        .expect("B's UPDATE succeeds");

    let v: i32 = a
        .query_one("SELECT v FROM t WHERE id = 1", &[])
        .await
        .expect("read")
        .get(0);
    assert_eq!(v, 110, "B's +100 applied on top of A's committed 10");

    b_conn.abort();
    shutdown(running).await;
}

/// 2. FOR UPDATE / FOR UPDATE — Wait blocks until release.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn for_update_vs_for_update_wait_blocks() {
    let running = start_sample_server("fufu_wait").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE t (id INT NOT NULL, v INT NOT NULL);
         INSERT INTO t VALUES (1, 10);
         CREATE INDEX t_id_idx ON t(id);",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "fufu_wait_b").await;

    a.batch_execute("BEGIN").await.expect("A begin");
    a.query("SELECT id FROM t WHERE id = 1 FOR UPDATE", &[])
        .await
        .expect("A locks");

    b.batch_execute("BEGIN").await.expect("B begin");
    let mut b_lock = Box::pin(b.query("SELECT id FROM t WHERE id = 1 FOR UPDATE", &[]));
    assert!(
        is_pending(b_lock.as_mut(), 300).await,
        "B's FOR UPDATE (Wait) must block, not return 40001"
    );

    a.batch_execute("COMMIT").await.expect("A commit");
    let rows = tokio::time::timeout(Duration::from_secs(5), b_lock)
        .await
        .expect("B unblocks")
        .expect("B's FOR UPDATE succeeds after A commits");
    assert_eq!(rows.len(), 1);
    b.batch_execute("ROLLBACK").await.expect("B rollback");

    b_conn.abort();
    shutdown(running).await;
}

/// 2b. FOR UPDATE NOWAIT against a held lock → SQLSTATE 55P03.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn for_update_nowait_conflict_is_55p03() {
    let running = start_sample_server("fu_nowait").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE t (id INT NOT NULL, v INT NOT NULL);
         INSERT INTO t VALUES (1, 10);
         CREATE INDEX t_id_idx ON t(id);",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "fu_nowait_b").await;

    a.batch_execute("BEGIN").await.expect("A begin");
    a.query("SELECT id FROM t WHERE id = 1 FOR UPDATE", &[])
        .await
        .expect("A locks");

    let err = b
        .query("SELECT id FROM t WHERE id = 1 FOR UPDATE NOWAIT", &[])
        .await
        .expect_err("NOWAIT must fail on a held lock");
    assert_eq!(
        err.code(),
        Some(&SqlState::LOCK_NOT_AVAILABLE),
        "NOWAIT conflict must be 55P03, got {err:?}"
    );

    a.batch_execute("COMMIT").await.expect("A commit");
    // Positive control: after A releases, NOWAIT succeeds.
    b.query("SELECT id FROM t WHERE id = 1 FOR UPDATE NOWAIT", &[])
        .await
        .expect("NOWAIT succeeds once the lock is free");

    b_conn.abort();
    shutdown(running).await;
}

/// 2c. FOR UPDATE SKIP LOCKED returns the non-locked rows (skipping the
///     row held by a peer), without error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn for_update_skip_locked_skips_held_row() {
    let running = start_sample_server("fu_skip").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE t (id INT NOT NULL, v INT NOT NULL);
         INSERT INTO t VALUES (1, 10), (2, 20);
         CREATE INDEX t_id_idx ON t(id);",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "fu_skip_b").await;

    a.batch_execute("BEGIN").await.expect("A begin");
    a.query("SELECT id FROM t WHERE id = 1 FOR UPDATE", &[])
        .await
        .expect("A locks id=1");

    // B asks for id=1 SKIP LOCKED → 0 rows (skipped), no error/no block.
    b.batch_execute("BEGIN").await.expect("B begin");
    let rows = b
        .query("SELECT id FROM t WHERE id = 1 FOR UPDATE SKIP LOCKED", &[])
        .await
        .expect("SKIP LOCKED never errors");
    assert_eq!(rows.len(), 0, "the locked row id=1 is skipped");
    b.batch_execute("ROLLBACK").await.expect("B rollback");

    a.batch_execute("COMMIT").await.expect("A commit");
    b_conn.abort();
    shutdown(running).await;
}

/// 3. ANY-PLAN-SHAPE: `FOR UPDATE` over a two-table comma select locks
///    the base rows of *both* relations — a peer `FOR UPDATE NOWAIT` of
///    either base row then conflicts (55P03). Not silent-no-lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn for_update_over_join_locks_base_rows() {
    let running = start_sample_server("fu_join").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE ja (id INT NOT NULL, v INT NOT NULL);
         CREATE TABLE jb (id INT NOT NULL, v INT NOT NULL);
         INSERT INTO ja VALUES (1, 10);
         INSERT INTO jb VALUES (1, 100);
         CREATE INDEX ja_id_idx ON ja(id);
         CREATE INDEX jb_id_idx ON jb(id);",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "fu_join_b").await;

    a.batch_execute("BEGIN").await.expect("A begin");
    let joined = a
        .query(
            "SELECT ja.id, jb.v FROM ja, jb WHERE ja.id = jb.id FOR UPDATE",
            &[],
        )
        .await
        .expect("A locks the join base rows");
    assert_eq!(joined.len(), 1);

    // Both base rows must be locked: a peer NOWAIT on either conflicts.
    let err_a = b
        .query("SELECT id FROM ja WHERE id = 1 FOR UPDATE NOWAIT", &[])
        .await
        .expect_err("ja base row must be locked");
    assert_eq!(err_a.code(), Some(&SqlState::LOCK_NOT_AVAILABLE));
    let err_b = b
        .query("SELECT id FROM jb WHERE id = 1 FOR UPDATE NOWAIT", &[])
        .await
        .expect_err("jb base row must be locked");
    assert_eq!(err_b.code(), Some(&SqlState::LOCK_NOT_AVAILABLE));

    a.batch_execute("COMMIT").await.expect("A commit");
    b_conn.abort();
    shutdown(running).await;
}

/// 4. NO SPURIOUS 40001: two connections contend for the same row with
///    plain `FOR UPDATE` (Wait). The waiter blocks then proceeds; it must
///    NOT surface 40001.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn contended_for_update_does_not_raise_40001() {
    let running = start_sample_server("fu_no40001").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE t (id INT NOT NULL, v INT NOT NULL);
         INSERT INTO t VALUES (1, 10);
         CREATE INDEX t_id_idx ON t(id);",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "fu_no40001_b").await;

    a.batch_execute("BEGIN").await.expect("A begin");
    a.query("SELECT id FROM t WHERE id = 1 FOR UPDATE", &[])
        .await
        .expect("A locks");

    b.batch_execute("BEGIN").await.expect("B begin");
    let mut b_lock = Box::pin(b.query("SELECT id FROM t WHERE id = 1 FOR UPDATE", &[]));
    assert!(
        is_pending(b_lock.as_mut(), 300).await,
        "B blocks rather than erroring"
    );
    a.batch_execute("COMMIT").await.expect("A commit");
    // The result must be Ok — specifically NOT a 40001 serialization error.
    b_lock
        .await
        .expect("contended FOR UPDATE blocks then succeeds, never 40001");
    b.batch_execute("ROLLBACK").await.expect("B rollback");

    b_conn.abort();
    shutdown(running).await;
}

/// 5. SKIP LOCKED JOB QUEUE: A grabs one row with
///    `FOR UPDATE SKIP LOCKED LIMIT 1`; B (concurrently, A still open)
///    grabs a *different* row — no double-processing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn skip_locked_job_queue_no_double_process() {
    let running = start_sample_server("fu_jobqueue").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE q (id INT NOT NULL, v INT NOT NULL);
         INSERT INTO q VALUES (1, 10), (2, 20);",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "fu_jobqueue_b").await;

    a.batch_execute("BEGIN").await.expect("A begin");
    let a_rows = a
        .query("SELECT id FROM q LIMIT 1 FOR UPDATE SKIP LOCKED", &[])
        .await
        .expect("A grabs one job");
    assert_eq!(a_rows.len(), 1, "A grabs exactly one row");
    let a_id: i32 = a_rows[0].get(0);

    b.batch_execute("BEGIN").await.expect("B begin");
    let b_rows = b
        .query("SELECT id FROM q LIMIT 1 FOR UPDATE SKIP LOCKED", &[])
        .await
        .expect("B grabs one job");
    assert_eq!(b_rows.len(), 1, "B grabs exactly one row");
    let b_id: i32 = b_rows[0].get(0);

    assert_ne!(
        a_id, b_id,
        "A and B must grab different rows (no double-process)"
    );

    a.batch_execute("ROLLBACK").await.expect("A rollback");
    b.batch_execute("ROLLBACK").await.expect("B rollback");
    b_conn.abort();
    shutdown(running).await;
}

/// 6. DEADLOCK: A locks r1 then waits on r2; B locks r2 then waits on r1.
///    The lock manager's detector picks a victim → 40P01; the other side
///    proceeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadlock_one_victim_gets_40p01() {
    let running = start_sample_server("fu_deadlock").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE d (id INT NOT NULL, v INT NOT NULL);
         INSERT INTO d VALUES (1, 10), (2, 20);
         CREATE INDEX d_id_idx ON d(id);",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "fu_deadlock_b").await;

    a.batch_execute("BEGIN").await.expect("A begin");
    a.query("SELECT id FROM d WHERE id = 1 FOR UPDATE", &[])
        .await
        .expect("A locks r1");

    b.batch_execute("BEGIN").await.expect("B begin");
    b.query("SELECT id FROM d WHERE id = 2 FOR UPDATE", &[])
        .await
        .expect("B locks r2");

    // A now waits on r2 (held by B); B waits on r1 (held by A) → cycle.
    // The deadlock detector aborts exactly one waiter's statement with
    // 40P01; the surviving waiter stays blocked on the victim's still-held
    // lock until the victim's transaction rolls back (PostgreSQL
    // semantics — a deadlock victim's *statement* errors but its locks are
    // released only at transaction end).
    let mut a_fut = Box::pin(a.query("SELECT id FROM d WHERE id = 2 FOR UPDATE", &[]));
    let mut b_fut = Box::pin(b.query("SELECT id FROM d WHERE id = 1 FOR UPDATE", &[]));

    // Whichever side resolves first is the deadlock victim (it errored;
    // the other is still parked). Identify it, assert 40P01, roll it back,
    // then confirm the survivor proceeds.
    let deadline = tokio::time::sleep(Duration::from_secs(8));
    tokio::pin!(deadline);
    enum Victim {
        A,
        B,
    }
    let (victim, victim_err) = tokio::select! {
        r = &mut a_fut => (Victim::A, r.expect_err("victim's statement must error")),
        r = &mut b_fut => (Victim::B, r.expect_err("victim's statement must error")),
        () = &mut deadline => panic!("deadlock was not detected within the deadline"),
    };
    assert_eq!(
        victim_err.code(),
        Some(&SqlState::T_R_DEADLOCK_DETECTED),
        "the victim must fail with 40P01, got {victim_err:?}"
    );

    // Roll the victim back to release its held lock, then the survivor's
    // blocked FOR UPDATE must complete.
    match victim {
        Victim::A => {
            drop(a_fut);
            a.batch_execute("ROLLBACK")
                .await
                .expect("victim A rollback");
            tokio::time::timeout(Duration::from_secs(5), &mut b_fut)
                .await
                .expect("survivor B unblocks after victim rolls back")
                .expect("survivor B's FOR UPDATE succeeds");
            drop(b_fut);
            b.batch_execute("ROLLBACK").await.expect("B rollback");
        }
        Victim::B => {
            drop(b_fut);
            b.batch_execute("ROLLBACK")
                .await
                .expect("victim B rollback");
            tokio::time::timeout(Duration::from_secs(5), &mut a_fut)
                .await
                .expect("survivor A unblocks after victim rolls back")
                .expect("survivor A's FOR UPDATE succeeds");
            drop(a_fut);
            a.batch_execute("ROLLBACK").await.expect("A rollback");
        }
    }

    b_conn.abort();
    shutdown(running).await;
}

/// 7a. REGRESSION: a plain SELECT takes no row lock — a peer
///     `FOR UPDATE NOWAIT` of the same row still succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plain_select_takes_no_lock() {
    let running = start_sample_server("fu_plain").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE t (id INT NOT NULL, v INT NOT NULL);
         INSERT INTO t VALUES (1, 10);
         CREATE INDEX t_id_idx ON t(id);",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "fu_plain_b").await;

    a.batch_execute("BEGIN").await.expect("A begin");
    a.query("SELECT id FROM t WHERE id = 1", &[])
        .await
        .expect("plain select");

    // No lock was taken: B's NOWAIT must succeed.
    b.query("SELECT id FROM t WHERE id = 1 FOR UPDATE NOWAIT", &[])
        .await
        .expect("plain SELECT held no lock, NOWAIT succeeds");

    a.batch_execute("ROLLBACK").await.expect("A rollback");
    b_conn.abort();
    shutdown(running).await;
}

/// 7b. REGRESSION: `FOR UPDATE` then an `UPDATE` of the locked row inside
///     the same transaction works (self re-lock is a no-op).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn for_update_then_update_same_txn_works() {
    let running = start_sample_server("fu_self").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE t (id INT NOT NULL, v INT NOT NULL);
         INSERT INTO t VALUES (1, 10);
         CREATE INDEX t_id_idx ON t(id);",
    )
    .await
    .expect("setup");

    a.batch_execute("BEGIN").await.expect("begin");
    a.query("SELECT id FROM t WHERE id = 1 FOR UPDATE", &[])
        .await
        .expect("lock id=1");
    a.batch_execute("UPDATE t SET v = v + 5 WHERE id = 1")
        .await
        .expect("update the self-locked row");
    a.batch_execute("COMMIT").await.expect("commit");

    let v: i32 = a
        .query_one("SELECT v FROM t WHERE id = 1", &[])
        .await
        .expect("read")
        .get(0);
    assert_eq!(v, 15);

    shutdown(running).await;
}
