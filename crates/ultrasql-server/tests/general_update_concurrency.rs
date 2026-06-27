//! Adversarial two-connection tests for the **general** (non-fused)
//! UPDATE / DELETE write path's Exclusive row lock + EvalPlanQual
//! latest-version re-check.
//!
//! These exercise the lock+recheck wiring added to
//! `crates/ultrasql-executor/src/modify/eval_plan_qual.rs` and lowered by
//! `build_eval_plan_qual` in
//! `crates/ultrasql-server/src/pipeline/modify/lowering.rs`. Every table
//! here is shaped so the int32-pair fused fast path does NOT apply — a
//! third `note TEXT` column makes the relation non-`(Int32, Int32)` — so
//! the writes go through the general `ModifyTable(Filter(SeqScan))` path
//! the fix hardens. They prove:
//!
//! 1. lost update prevented on the GENERAL path — two concurrent
//!    `UPDATE ... SET bal = bal + 10` serialize, both apply (100 → 120);
//! 2. FOR UPDATE blocks a general UPDATE — A's `SELECT ... FOR UPDATE`
//!    makes B's general UPDATE block until A ends its txn, then B applies
//!    on top of A's committed version;
//! 3. READ COMMITTED EvalPlanQual re-read — B applies to A's committed
//!    latest version (200 → 210), not its stale snapshot;
//! 4. predicate re-check — B's `UPDATE ... WHERE status='pending'` skips a
//!    row A concurrently moved to `status='done'`;
//! 5. concurrent DELETE — B's UPDATE of a row A committed a DELETE of is a
//!    no-op (0 rows);
//! 6. REPEATABLE READ first-updater-wins — the second concurrent UPDATE
//!    gets 40001;
//! 7. deadlock — cross-row general UPDATE ordering → one victim gets 40P01;
//! 8. no regression — single-connection general UPDATE/DELETE work and
//!    RETURNING reflects the applied latest version.

use std::time::Duration;

use tokio_postgres::error::SqlState;

pub mod support;

use support::{connect_as, shutdown, start_sample_server};

/// Poll a future to a short deadline, returning `true` if it is still
/// pending — used to assert that a peer statement is blocked.
async fn is_pending<F, T>(fut: std::pin::Pin<&mut F>, ms: u64) -> bool
where
    F: std::future::Future<Output = T>,
{
    tokio::time::timeout(Duration::from_millis(ms), fut)
        .await
        .is_err()
}

/// 1. LOST-UPDATE PREVENTED (general path, the headline): two connections
///    each `UPDATE acct SET bal = bal + 10 WHERE id = 1` concurrently.
///    They must serialize on the row's Exclusive lock and both apply
///    (100 → 120); the second must re-read the first's committed 110.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn general_update_no_lost_update() {
    let running = start_sample_server("gen_lost_update").await;
    let a = &running.client;
    // `note TEXT` makes the relation non-(Int32, Int32): general path.
    a.batch_execute(
        "CREATE TABLE acct (id INT NOT NULL, bal INT NOT NULL, note TEXT);
         INSERT INTO acct VALUES (1, 100, 'x');",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "gen_lost_update_b").await;

    // A begins and applies +10 but stays open (uncommitted, holds the lock).
    a.batch_execute("BEGIN; UPDATE acct SET bal = bal + 10 WHERE id = 1;")
        .await
        .expect("A holds general update");

    // B's general UPDATE must block on A's Exclusive row lock.
    let mut b_update = Box::pin(b.batch_execute("UPDATE acct SET bal = bal + 10 WHERE id = 1;"));
    assert!(
        is_pending(b_update.as_mut(), 300).await,
        "B's general UPDATE must block while A holds the row lock"
    );

    a.batch_execute("COMMIT").await.expect("A commit");
    tokio::time::timeout(Duration::from_secs(5), b_update)
        .await
        .expect("B unblocks after A commits")
        .expect("B's general UPDATE succeeds");

    let bal: i32 = a
        .query_one("SELECT bal FROM acct WHERE id = 1", &[])
        .await
        .expect("read")
        .get(0);
    assert_eq!(
        bal, 120,
        "both +10 applied serially on the general path — no lost update"
    );

    b_conn.abort();
    shutdown(running).await;
}

/// 1b. LOST-UPDATE PREVENTED with a multi-assignment general UPDATE
///     (`SET bal = bal + 10, note = 'y'`) — proves it is the general path
///     even on a two-int table, since the extra assignment disqualifies the
///     int32-pair fused fast path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn general_multi_assignment_update_no_lost_update() {
    let running = start_sample_server("gen_multi_assign").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE acct (id INT NOT NULL, bal INT NOT NULL, note TEXT);
         INSERT INTO acct VALUES (1, 100, 'x');",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "gen_multi_assign_b").await;

    a.batch_execute("BEGIN; UPDATE acct SET bal = bal + 10, note = 'a' WHERE id = 1;")
        .await
        .expect("A holds general update");

    let mut b_update =
        Box::pin(b.batch_execute("UPDATE acct SET bal = bal + 10, note = 'b' WHERE id = 1;"));
    assert!(
        is_pending(b_update.as_mut(), 300).await,
        "B's general UPDATE must block while A holds the row lock"
    );

    a.batch_execute("COMMIT").await.expect("A commit");
    tokio::time::timeout(Duration::from_secs(5), b_update)
        .await
        .expect("B unblocks")
        .expect("B succeeds");

    let row = a
        .query_one("SELECT bal, note FROM acct WHERE id = 1", &[])
        .await
        .expect("read");
    let bal: i32 = row.get(0);
    let note: String = row.get(1);
    assert_eq!(bal, 120, "both +10 applied — no lost update");
    assert_eq!(note, "b", "B's note wins (applied last)");

    b_conn.abort();
    shutdown(running).await;
}

/// 2. FOR UPDATE vs general UPDATE: A `SELECT ... FOR UPDATE id=1`; B's
///    general `UPDATE id=1` blocks until A commits, then applies to A's
///    committed version.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn for_update_blocks_general_update() {
    let running = start_sample_server("gen_fu_blocks").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE acct (id INT NOT NULL, bal INT NOT NULL, note TEXT);
         INSERT INTO acct VALUES (1, 100, 'x');",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "gen_fu_blocks_b").await;

    a.batch_execute("BEGIN").await.expect("A begin");
    let locked = a
        .query("SELECT id, bal FROM acct WHERE id = 1 FOR UPDATE", &[])
        .await
        .expect("A locks id=1");
    assert_eq!(locked.len(), 1);

    let mut b_update = Box::pin(b.batch_execute("UPDATE acct SET bal = bal + 10 WHERE id = 1;"));
    assert!(
        is_pending(b_update.as_mut(), 300).await,
        "B's general UPDATE must block while A holds FOR UPDATE"
    );

    a.batch_execute("COMMIT").await.expect("A commit");
    tokio::time::timeout(Duration::from_secs(5), b_update)
        .await
        .expect("B unblocks after A commits")
        .expect("B succeeds");

    let bal: i32 = a
        .query_one("SELECT bal FROM acct WHERE id = 1", &[])
        .await
        .expect("read")
        .get(0);
    assert_eq!(bal, 110, "B's +10 applied on top of A's committed 100");

    b_conn.abort();
    shutdown(running).await;
}

/// 3. READ COMMITTED EvalPlanQual re-read: A commits an UPDATE that sets
///    bal=200 while B is blocked; B (started before A committed) must
///    re-read the latest (200) and apply +10 → 210, NOT its stale snapshot
///    of 100.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_committed_reads_latest_committed_version() {
    let running = start_sample_server("gen_rc_reread").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE acct (id INT NOT NULL, bal INT NOT NULL, note TEXT);
         INSERT INTO acct VALUES (1, 100, 'x');",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "gen_rc_reread_b").await;

    // A holds an uncommitted absolute-set UPDATE (bal = 200).
    a.batch_execute("BEGIN; UPDATE acct SET bal = 200 WHERE id = 1;")
        .await
        .expect("A holds bal=200");

    // B's relative UPDATE must block on the row lock.
    let mut b_update = Box::pin(b.batch_execute("UPDATE acct SET bal = bal + 10 WHERE id = 1;"));
    assert!(
        is_pending(b_update.as_mut(), 300).await,
        "B must block while A holds the row lock"
    );

    a.batch_execute("COMMIT").await.expect("A commit bal=200");
    tokio::time::timeout(Duration::from_secs(5), b_update)
        .await
        .expect("B unblocks")
        .expect("B succeeds");

    let bal: i32 = a
        .query_one("SELECT bal FROM acct WHERE id = 1", &[])
        .await
        .expect("read")
        .get(0);
    assert_eq!(
        bal, 210,
        "B re-read A's committed 200 and applied +10 (EvalPlanQual RC), not 110"
    );

    b_conn.abort();
    shutdown(running).await;
}

/// 4. PREDICATE re-check (RC): B `UPDATE t SET bal = 1 WHERE status =
///    'pending'`; A concurrently commits a move of id=1 to status='done'.
///    B must SKIP id=1 (the latest version no longer matches the WHERE).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_committed_predicate_recheck_skips_moved_row() {
    let running = start_sample_server("gen_rc_pred").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE t (id INT NOT NULL, bal INT NOT NULL, status TEXT NOT NULL);
         INSERT INTO t VALUES (1, 0, 'pending'), (2, 0, 'pending');",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "gen_rc_pred_b").await;

    // A holds an uncommitted move of id=1 out of the 'pending' set.
    a.batch_execute("BEGIN; UPDATE t SET status = 'done' WHERE id = 1;")
        .await
        .expect("A moves id=1 to done");

    // B's predicate-driven UPDATE must block on id=1's row lock.
    let mut b_update = Box::pin(b.execute("UPDATE t SET bal = 1 WHERE status = 'pending'", &[]));
    assert!(
        is_pending(b_update.as_mut(), 300).await,
        "B must block on id=1's row lock"
    );

    a.batch_execute("COMMIT").await.expect("A commit");
    let affected = tokio::time::timeout(Duration::from_secs(5), b_update)
        .await
        .expect("B unblocks")
        .expect("B succeeds");
    // Only id=2 still matches 'pending'; id=1 is skipped on re-check.
    assert_eq!(affected, 1, "B updates only the still-pending row (id=2)");

    let id1_bal: i32 = a
        .query_one("SELECT bal FROM t WHERE id = 1", &[])
        .await
        .expect("read id1")
        .get(0);
    assert_eq!(
        id1_bal, 0,
        "id=1 was skipped (moved to done) — not wrongly updated"
    );
    let id2_bal: i32 = a
        .query_one("SELECT bal FROM t WHERE id = 2", &[])
        .await
        .expect("read id2")
        .get(0);
    assert_eq!(id2_bal, 1, "id=2 (still pending) was updated");

    b_conn.abort();
    shutdown(running).await;
}

/// 5. CONCURRENT DELETE: A commits `DELETE id=1`; B's blocked
///    `UPDATE id=1` must skip the deleted row (0 rows updated).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_skips_concurrently_deleted_row() {
    let running = start_sample_server("gen_concurrent_delete").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE acct (id INT NOT NULL, bal INT NOT NULL, note TEXT);
         INSERT INTO acct VALUES (1, 100, 'x');",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "gen_concurrent_delete_b").await;

    // A holds an uncommitted DELETE of id=1.
    a.batch_execute("BEGIN; DELETE FROM acct WHERE id = 1;")
        .await
        .expect("A holds delete");

    let mut b_update = Box::pin(b.execute("UPDATE acct SET bal = bal + 10 WHERE id = 1", &[]));
    assert!(
        is_pending(b_update.as_mut(), 300).await,
        "B must block on A's delete lock"
    );

    a.batch_execute("COMMIT").await.expect("A commit delete");
    let affected = tokio::time::timeout(Duration::from_secs(5), b_update)
        .await
        .expect("B unblocks")
        .expect("B succeeds");
    assert_eq!(affected, 0, "B skips the concurrently-deleted row");

    let remaining: i64 = a
        .query_one("SELECT count(*) FROM acct WHERE id = 1", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(
        remaining, 0,
        "row stays deleted, not resurrected by B's UPDATE"
    );

    b_conn.abort();
    shutdown(running).await;
}

/// 6. REPEATABLE READ / first-updater-wins: under REPEATABLE READ, two
///    concurrent UPDATEs to id=1 — the second must get 40001, not a stale
///    overwrite.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeatable_read_second_updater_gets_40001() {
    let running = start_sample_server("gen_rr_40001").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE acct (id INT NOT NULL, bal INT NOT NULL, note TEXT);
         INSERT INTO acct VALUES (1, 100, 'x');",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "gen_rr_40001_b").await;

    // Both take a REPEATABLE READ snapshot of bal=100 via an initial read.
    a.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("A begin RR");
    b.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("B begin RR");
    a.query_one("SELECT bal FROM acct WHERE id = 1", &[])
        .await
        .expect("A snapshot read");
    b.query_one("SELECT bal FROM acct WHERE id = 1", &[])
        .await
        .expect("B snapshot read");

    // A updates and commits first.
    a.batch_execute("UPDATE acct SET bal = bal + 10 WHERE id = 1")
        .await
        .expect("A update");
    a.batch_execute("COMMIT").await.expect("A commit");

    // B's UPDATE of the same row is a write-write conflict under RR.
    let err = b
        .batch_execute("UPDATE acct SET bal = bal + 10 WHERE id = 1")
        .await
        .expect_err("B's RR update must conflict with A's committed update");
    assert_eq!(
        err.code(),
        Some(&SqlState::T_R_SERIALIZATION_FAILURE),
        "second updater under RR must get 40001, got {err:?}"
    );
    b.batch_execute("ROLLBACK").await.expect("B rollback");

    // First-updater-wins: only A's +10 applied (no lost update / no double).
    let bal: i32 = a
        .query_one("SELECT bal FROM acct WHERE id = 1", &[])
        .await
        .expect("read")
        .get(0);
    assert_eq!(bal, 110, "only A's committed +10 applied; B aborted");

    b_conn.abort();
    shutdown(running).await;
}

/// 7. DEADLOCK: A updates r1 then waits on r2; B updates r2 then waits on
///    r1. The lock manager's detector aborts exactly one general UPDATE
///    with 40P01; the survivor proceeds after the victim rolls back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn general_update_cross_row_deadlock_40p01() {
    let running = start_sample_server("gen_deadlock").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE acct (id INT NOT NULL, bal INT NOT NULL, note TEXT);
         INSERT INTO acct VALUES (1, 10, 'x'), (2, 20, 'y');",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "gen_deadlock_b").await;

    a.batch_execute("BEGIN").await.expect("A begin");
    a.batch_execute("UPDATE acct SET bal = bal + 1 WHERE id = 1")
        .await
        .expect("A locks r1");

    b.batch_execute("BEGIN").await.expect("B begin");
    b.batch_execute("UPDATE acct SET bal = bal + 1 WHERE id = 2")
        .await
        .expect("B locks r2");

    // A now waits on r2 (held by B); B waits on r1 (held by A) → cycle.
    let mut a_fut = Box::pin(a.batch_execute("UPDATE acct SET bal = bal + 1 WHERE id = 2"));
    let mut b_fut = Box::pin(b.batch_execute("UPDATE acct SET bal = bal + 1 WHERE id = 1"));

    enum Victim {
        A,
        B,
    }
    let deadline = tokio::time::sleep(Duration::from_secs(8));
    tokio::pin!(deadline);
    let (victim, victim_err) = tokio::select! {
        r = &mut a_fut => (Victim::A, r.expect_err("victim's statement must error")),
        r = &mut b_fut => (Victim::B, r.expect_err("victim's statement must error")),
        () = &mut deadline => panic!("deadlock was not detected within the deadline"),
    };
    assert_eq!(
        victim_err.code(),
        Some(&SqlState::T_R_DEADLOCK_DETECTED),
        "the deadlock victim must fail with 40P01, got {victim_err:?}"
    );

    match victim {
        Victim::A => {
            drop(a_fut);
            a.batch_execute("ROLLBACK")
                .await
                .expect("victim A rollback");
            tokio::time::timeout(Duration::from_secs(5), &mut b_fut)
                .await
                .expect("survivor B unblocks")
                .expect("survivor B succeeds");
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
                .expect("survivor A unblocks")
                .expect("survivor A succeeds");
            drop(a_fut);
            a.batch_execute("ROLLBACK").await.expect("A rollback");
        }
    }

    b_conn.abort();
    shutdown(running).await;
}

/// 8a. REGRESSION: a single-connection general UPDATE/DELETE works and
///     RETURNING reflects the applied (latest) version.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_connection_general_update_delete_returning() {
    let running = start_sample_server("gen_single_conn").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE acct (id INT NOT NULL, bal INT NOT NULL, note TEXT);
         INSERT INTO acct VALUES (1, 100, 'x'), (2, 200, 'y');",
    )
    .await
    .expect("setup");

    // UPDATE ... RETURNING reflects the new (applied) value.
    let row = a
        .query_one(
            "UPDATE acct SET bal = bal + 5 WHERE id = 1 RETURNING bal",
            &[],
        )
        .await
        .expect("update returning");
    let bal: i32 = row.get(0);
    assert_eq!(bal, 105, "RETURNING reflects the applied latest version");

    // DELETE ... RETURNING reflects the removed row, and the row is gone.
    let deleted = a
        .query_one("DELETE FROM acct WHERE id = 2 RETURNING id", &[])
        .await
        .expect("delete returning");
    let id: i32 = deleted.get(0);
    assert_eq!(id, 2);
    let remaining: i64 = a
        .query_one("SELECT count(*) FROM acct", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(remaining, 1, "id=2 deleted; id=1 remains");

    shutdown(running).await;
}

/// 9. MULTI-ROW LOST-UPDATE PREVENTED (the adversarial repro the single-row
///    cases missed): rows (1,100),(2,200). A runs
///    `UPDATE acct SET bal = bal + 10 WHERE id IN (1,2)` uncommitted, holding
///    BOTH row locks. B runs the same statement and parks on row 1. A
///    commits, releasing *both* locks at once. B resumes: row 1 re-reads
///    A's committed 110 (it waited), but row 2's grant is now IMMEDIATE
///    (no wait) — the buggy `waited == false` fast path would re-read row 2
///    against B's STALE snapshot (200 → 210), silently dropping A's +10.
///    The fix always re-reads the latest committed version, so both rows
///    land +10 on top of A's commit: bal1 = 120 AND bal2 = 220, and B's
///    affected count is 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_row_general_update_no_lost_update_in_list() {
    let running = start_sample_server("gen_multi_lost_update").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE acct (id INT NOT NULL, bal INT NOT NULL, note TEXT);
         INSERT INTO acct VALUES (1, 100, 'x'), (2, 200, 'y');",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "gen_multi_lost_update_b").await;

    // A holds the multi-row UPDATE uncommitted: both row locks are held.
    a.batch_execute("BEGIN; UPDATE acct SET bal = bal + 10 WHERE id IN (1, 2);")
        .await
        .expect("A holds both row locks");

    // B's identical multi-row UPDATE parks on the first contended row.
    let mut b_update =
        Box::pin(b.execute("UPDATE acct SET bal = bal + 10 WHERE id IN (1, 2)", &[]));
    assert!(
        is_pending(b_update.as_mut(), 300).await,
        "B's multi-row UPDATE must block while A holds the row locks"
    );

    // A commits, releasing BOTH locks; B resumes — row 2's grant is now
    // immediate (the exact waited==false window).
    a.batch_execute("COMMIT").await.expect("A commit");
    let affected = tokio::time::timeout(Duration::from_secs(5), b_update)
        .await
        .expect("B unblocks after A commits")
        .expect("B's multi-row UPDATE succeeds");
    assert_eq!(
        affected, 2,
        "B applied to both targeted rows (none stale-skipped)"
    );

    let bal1: i32 = a
        .query_one("SELECT bal FROM acct WHERE id = 1", &[])
        .await
        .expect("read id1")
        .get(0);
    let bal2: i32 = a
        .query_one("SELECT bal FROM acct WHERE id = 2", &[])
        .await
        .expect("read id2")
        .get(0);
    assert_eq!(bal1, 120, "row 1: both +10 applied (it waited) — no loss");
    assert_eq!(
        bal2, 220,
        "row 2: both +10 applied on the immediate (waited==false) grant — \
         the stale-snapshot lost update is closed"
    );

    b_conn.abort();
    shutdown(running).await;
}

/// 9b. MULTI-ROW LOST-UPDATE PREVENTED, range predicate variant with three
///     rows: `WHERE id BETWEEN 1 AND 3`. Same structure as case 9 but with a
///     wider target set, so at least two rows clear the lock with no wait
///     after A commits — all three must land both increments.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_row_general_update_no_lost_update_between_range() {
    let running = start_sample_server("gen_multi_lost_update_range").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE acct (id INT NOT NULL, bal INT NOT NULL, note TEXT);
         INSERT INTO acct VALUES (1, 100, 'x'), (2, 200, 'y'), (3, 300, 'z');",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "gen_multi_lost_update_range_b").await;

    a.batch_execute("BEGIN; UPDATE acct SET bal = bal + 10 WHERE id BETWEEN 1 AND 3;")
        .await
        .expect("A holds all three row locks");

    let mut b_update = Box::pin(b.execute(
        "UPDATE acct SET bal = bal + 10 WHERE id BETWEEN 1 AND 3",
        &[],
    ));
    assert!(
        is_pending(b_update.as_mut(), 300).await,
        "B's range UPDATE must block while A holds the row locks"
    );

    a.batch_execute("COMMIT").await.expect("A commit");
    let affected = tokio::time::timeout(Duration::from_secs(5), b_update)
        .await
        .expect("B unblocks")
        .expect("B succeeds");
    assert_eq!(affected, 3, "B applied to all three targeted rows");

    for (id, expected) in [(1, 120), (2, 220), (3, 320)] {
        let bal: i32 = a
            .query_one("SELECT bal FROM acct WHERE id = $1", &[&id])
            .await
            .expect("read")
            .get(0);
        assert_eq!(
            bal, expected,
            "row {id}: both +10 applied — no stale overwrite on any row"
        );
    }

    b_conn.abort();
    shutdown(running).await;
}

/// 10. WAITED==false PATH ISOLATED: B does NOT block on the first row it
///     processes, only on a later one. A holds a lock on row 2 ONLY (an
///     uncommitted single-row UPDATE of id=2). B's multi-row statement
///     touches row 1 (free → immediate grant, waited==false) then row 2
///     (blocked). After A commits, row 2 must re-read A's committed version.
///     This proves the non-blocked leading row (row 1) is also re-read from
///     the latest version and the blocked trailing row picks up A's commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_row_update_rereads_latest_when_first_row_not_blocked() {
    let running = start_sample_server("gen_multi_first_free").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE acct (id INT NOT NULL, bal INT NOT NULL, note TEXT);
         INSERT INTO acct VALUES (1, 100, 'x'), (2, 200, 'y');",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "gen_multi_first_free_b").await;

    // A holds row 2's lock ONLY (uncommitted absolute set bal=999).
    a.batch_execute("BEGIN; UPDATE acct SET bal = 999 WHERE id = 2;")
        .await
        .expect("A holds id=2 only");

    // B touches id=1 (free) then id=2 (locked by A): B blocks on id=2.
    let mut b_update =
        Box::pin(b.execute("UPDATE acct SET bal = bal + 10 WHERE id IN (1, 2)", &[]));
    assert!(
        is_pending(b_update.as_mut(), 300).await,
        "B must block on id=2 (held by A) after clearing id=1 with no wait"
    );

    a.batch_execute("COMMIT").await.expect("A commit bal=999");
    let affected = tokio::time::timeout(Duration::from_secs(5), b_update)
        .await
        .expect("B unblocks")
        .expect("B succeeds");
    assert_eq!(affected, 2, "B updated both rows");

    let bal1: i32 = a
        .query_one("SELECT bal FROM acct WHERE id = 1", &[])
        .await
        .expect("read id1")
        .get(0);
    let bal2: i32 = a
        .query_one("SELECT bal FROM acct WHERE id = 2", &[])
        .await
        .expect("read id2")
        .get(0);
    assert_eq!(bal1, 110, "id=1 (never contended) applied +10 over 100");
    assert_eq!(
        bal2, 1009,
        "id=2 re-read A's committed 999 and applied +10 — not its stale 200"
    );

    b_conn.abort();
    shutdown(running).await;
}

/// 10b. REVERSE ORDER: A holds the FIRST processed row's lock, the later row
///      is free. B blocks on row 1, then row 2's grant is immediate
///      (waited==false) — the exact buggy window. Row 2 must still apply over
///      its own (unchanged) value, and row 1 re-reads A's commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_row_update_rereads_latest_when_later_row_not_blocked() {
    let running = start_sample_server("gen_multi_later_free").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE acct (id INT NOT NULL, bal INT NOT NULL, note TEXT);
         INSERT INTO acct VALUES (1, 100, 'x'), (2, 200, 'y');",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "gen_multi_later_free_b").await;

    // A holds row 1's lock ONLY (uncommitted absolute set bal=999).
    a.batch_execute("BEGIN; UPDATE acct SET bal = 999 WHERE id = 1;")
        .await
        .expect("A holds id=1 only");

    // B blocks on id=1; id=2 is free → immediate grant after A commits.
    let mut b_update =
        Box::pin(b.execute("UPDATE acct SET bal = bal + 10 WHERE id IN (1, 2)", &[]));
    assert!(
        is_pending(b_update.as_mut(), 300).await,
        "B must block on id=1 (held by A)"
    );

    a.batch_execute("COMMIT").await.expect("A commit");
    let affected = tokio::time::timeout(Duration::from_secs(5), b_update)
        .await
        .expect("B unblocks")
        .expect("B succeeds");
    assert_eq!(affected, 2, "B updated both rows");

    let bal1: i32 = a
        .query_one("SELECT bal FROM acct WHERE id = 1", &[])
        .await
        .expect("read id1")
        .get(0);
    let bal2: i32 = a
        .query_one("SELECT bal FROM acct WHERE id = 2", &[])
        .await
        .expect("read id2")
        .get(0);
    assert_eq!(bal1, 1009, "id=1 re-read A's committed 999 and applied +10");
    assert_eq!(
        bal2, 210,
        "id=2 (immediate grant, waited==false) applied +10 over its own 200"
    );

    b_conn.abort();
    shutdown(running).await;
}

/// 11. MULTI-ROW DELETE vs a concurrent predicate-moving UPDATE: B deletes
///     `WHERE status='pending'` (rows 1,2,3 all pending); A concurrently
///     commits a move of id=2 to status='done'. B must SKIP id=2 on the
///     re-check (its latest version no longer matches the WHERE) and delete
///     only id=1 and id=3 → affected count 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_row_delete_skips_concurrently_moved_row() {
    let running = start_sample_server("gen_multi_delete_skip").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE t (id INT NOT NULL, bal INT NOT NULL, status TEXT NOT NULL);
         INSERT INTO t VALUES (1, 0, 'pending'), (2, 0, 'pending'), (3, 0, 'pending');",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "gen_multi_delete_skip_b").await;

    // A holds an uncommitted move of id=2 out of the 'pending' set.
    a.batch_execute("BEGIN; UPDATE t SET status = 'done' WHERE id = 2;")
        .await
        .expect("A moves id=2 to done");

    // B's predicate DELETE must block on id=2's row lock.
    let mut b_delete = Box::pin(b.execute("DELETE FROM t WHERE status = 'pending'", &[]));
    assert!(
        is_pending(b_delete.as_mut(), 300).await,
        "B must block on id=2's row lock"
    );

    a.batch_execute("COMMIT").await.expect("A commit");
    let affected = tokio::time::timeout(Duration::from_secs(5), b_delete)
        .await
        .expect("B unblocks")
        .expect("B succeeds");
    assert_eq!(
        affected, 2,
        "B deletes only the still-pending rows (id=1, id=3); id=2 skipped"
    );

    let surviving: Vec<i32> = a
        .query("SELECT id FROM t ORDER BY id", &[])
        .await
        .expect("read survivors")
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(
        surviving,
        vec![2],
        "only id=2 (moved to done) survives the DELETE"
    );

    b_conn.abort();
    shutdown(running).await;
}

/// 12. AFFECTED COUNT with a concurrent DELETE in a multi-row UPDATE: B
///     `UPDATE acct SET bal = bal + 10 WHERE id IN (1,2,3)` (all three
///     targeted); A concurrently commits a DELETE of id=2. B must skip the
///     deleted row and apply to id=1 and id=3 → affected count exactly 2,
///     with both surviving rows carrying +10 over A's committed state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_row_update_affected_count_excludes_concurrently_deleted() {
    let running = start_sample_server("gen_multi_count_delete").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE acct (id INT NOT NULL, bal INT NOT NULL, note TEXT);
         INSERT INTO acct VALUES (1, 100, 'x'), (2, 200, 'y'), (3, 300, 'z');",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "gen_multi_count_delete_b").await;

    // A holds an uncommitted DELETE of id=2 (the middle targeted row).
    a.batch_execute("BEGIN; DELETE FROM acct WHERE id = 2;")
        .await
        .expect("A holds delete of id=2");

    let mut b_update =
        Box::pin(b.execute("UPDATE acct SET bal = bal + 10 WHERE id IN (1, 2, 3)", &[]));
    assert!(
        is_pending(b_update.as_mut(), 300).await,
        "B must block on id=2's delete lock"
    );

    a.batch_execute("COMMIT").await.expect("A commit delete");
    let affected = tokio::time::timeout(Duration::from_secs(5), b_update)
        .await
        .expect("B unblocks")
        .expect("B succeeds");
    assert_eq!(
        affected, 2,
        "3 targeted, 1 concurrently deleted → affected = 2"
    );

    let bal1: i32 = a
        .query_one("SELECT bal FROM acct WHERE id = 1", &[])
        .await
        .expect("read id1")
        .get(0);
    let bal3: i32 = a
        .query_one("SELECT bal FROM acct WHERE id = 3", &[])
        .await
        .expect("read id3")
        .get(0);
    let id2_count: i64 = a
        .query_one("SELECT count(*) FROM acct WHERE id = 2", &[])
        .await
        .expect("count id2")
        .get(0);
    assert_eq!(bal1, 110, "id=1 applied +10");
    assert_eq!(bal3, 310, "id=3 applied +10");
    assert_eq!(
        id2_count, 0,
        "id=2 stays deleted (skipped, not resurrected)"
    );

    b_conn.abort();
    shutdown(running).await;
}

/// 8b. REGRESSION: the int32-pair fused fast path is still chosen and still
///     correct under contention — two concurrent `SET v = v + 1` over a
///     `(id, v)` table serialize and both apply (0 → 2).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fused_fast_path_still_serializes() {
    let running = start_sample_server("gen_fused_regression").await;
    let a = &running.client;
    // Two-int relation with an index on id: the fused indexed-update path.
    a.batch_execute(
        "CREATE TABLE pair (id INT NOT NULL, v INT NOT NULL);
         INSERT INTO pair VALUES (1, 0);
         CREATE INDEX pair_id_idx ON pair(id);",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "gen_fused_regression_b").await;

    a.batch_execute("BEGIN; UPDATE pair SET v = v + 1 WHERE id = 1;")
        .await
        .expect("A holds fused update");
    let mut b_update = Box::pin(b.batch_execute("UPDATE pair SET v = v + 1 WHERE id = 1;"));
    assert!(
        is_pending(b_update.as_mut(), 300).await,
        "B's fused UPDATE must block while A holds the row lock"
    );
    a.batch_execute("COMMIT").await.expect("A commit");
    tokio::time::timeout(Duration::from_secs(5), b_update)
        .await
        .expect("B unblocks")
        .expect("B succeeds");

    let v: i32 = a
        .query_one("SELECT v FROM pair WHERE id = 1", &[])
        .await
        .expect("read")
        .get(0);
    assert_eq!(v, 2, "fused fast path still serializes — both +1 applied");

    b_conn.abort();
    shutdown(running).await;
}
