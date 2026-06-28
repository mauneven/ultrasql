//! Adversarial two-connection tests for the INSERT ... ON CONFLICT DO UPDATE
//! write path's Exclusive row lock + EvalPlanQual latest-version re-check.
//!
//! ON CONFLICT DO UPDATE computes its conflicting row from a unique-index
//! probe and used to mutate it with NO row lock and NO concurrent-update
//! re-check — so a concurrent writer of the conflicting row lost its update,
//! exactly like the general UPDATE / DELETE path did before it was fixed.
//! `lower_real_insert` now wires the SAME
//! `build_eval_plan_qual_no_predicate` lock+recheck the general path uses
//! (`crates/ultrasql-server/src/pipeline/modify/insert.rs`), so the DO UPDATE
//! Exclusive-locks the conflicting tuple and re-checks it against the latest
//! committed version before applying the update.
//!
//! These prove, for the ON CONFLICT DO UPDATE path:
//!
//! 1. lost update prevented — two concurrent upserts `... SET bal = bal + 10`
//!    serialize on the conflicting row's lock and both apply (100 → 120);
//! 2. FOR UPDATE blocks the DO UPDATE until the holder commits;
//! 3. READ COMMITTED re-read — the DO UPDATE applies to the latest committed
//!    version (200 → 210), not the stale snapshot;
//! 4. REPEATABLE READ first-updater-wins — the second concurrent upsert gets
//!    40001;
//! 5. deadlock — cross-row upsert ordering → one victim 40P01;
//! 6. no regression — single-connection upsert insert / update still work.

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

/// `INSERT ... ON CONFLICT (id) DO UPDATE SET bal = bal + 10`. The `excluded`
/// value (999) never lands; the matched row's own `bal` is incremented, so a
/// lost update would be plainly visible.
const UPSERT_ADD: &str =
    "INSERT INTO acct VALUES (1, 999, 'z') ON CONFLICT (id) DO UPDATE SET bal = bal + 10";

async fn setup_acct(a: &tokio_postgres::Client, rows: &str) {
    a.batch_execute(&format!(
        "CREATE TABLE acct (id INT PRIMARY KEY, bal INT NOT NULL, note TEXT);
         INSERT INTO acct VALUES {rows};"
    ))
    .await
    .expect("setup");
}

/// 1. LOST-UPDATE PREVENTED (the headline): two connections each upsert the
///    same conflicting key with `DO UPDATE SET bal = bal + 10` concurrently.
///    They must serialize on the conflicting row's Exclusive lock and both
///    apply (100 → 120); the second must re-read the first's committed 110.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_conflict_do_update_no_lost_update() {
    let running = start_sample_server("oc_lost_update").await;
    let a = &running.client;
    setup_acct(a, "(1, 100, 'x')").await;
    let (b, b_conn) = connect_as(running.bound, "tester", "oc_lost_update_b").await;

    a.batch_execute(&format!("BEGIN; {UPSERT_ADD};"))
        .await
        .expect("A holds upsert update");

    let mut b_upsert = Box::pin(b.batch_execute(UPSERT_ADD));
    assert!(
        is_pending(b_upsert.as_mut(), 300).await,
        "B's ON CONFLICT DO UPDATE must block while A holds the conflicting row lock"
    );

    a.batch_execute("COMMIT").await.expect("A commit");
    tokio::time::timeout(Duration::from_secs(5), b_upsert)
        .await
        .expect("B unblocks after A commits")
        .expect("B's upsert succeeds");

    let bal: i32 = a
        .query_one("SELECT bal FROM acct WHERE id = 1", &[])
        .await
        .expect("read")
        .get(0);
    assert_eq!(
        bal, 120,
        "both +10 applied serially on the DO UPDATE path — no lost update"
    );

    b_conn.abort();
    shutdown(running).await;
}

/// 2. FOR UPDATE vs ON CONFLICT DO UPDATE: A `SELECT ... FOR UPDATE id=1`; B's
///    upsert DO UPDATE blocks until A commits, then applies on top.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn for_update_blocks_on_conflict_do_update() {
    let running = start_sample_server("oc_fu_blocks").await;
    let a = &running.client;
    setup_acct(a, "(1, 100, 'x')").await;
    let (b, b_conn) = connect_as(running.bound, "tester", "oc_fu_blocks_b").await;

    a.batch_execute("BEGIN").await.expect("A begin");
    let locked = a
        .query("SELECT id, bal FROM acct WHERE id = 1 FOR UPDATE", &[])
        .await
        .expect("A locks id=1");
    assert_eq!(locked.len(), 1);

    let mut b_upsert = Box::pin(b.batch_execute(UPSERT_ADD));
    assert!(
        is_pending(b_upsert.as_mut(), 300).await,
        "B's DO UPDATE must block while A holds FOR UPDATE"
    );

    a.batch_execute("COMMIT").await.expect("A commit");
    tokio::time::timeout(Duration::from_secs(5), b_upsert)
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

/// 3. READ COMMITTED re-read: A commits an absolute-set UPDATE (bal=200) while
///    B's upsert is blocked; B must re-read the latest (200) and apply +10 →
///    210, NOT its stale snapshot of 100.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_conflict_read_committed_reads_latest_committed_version() {
    let running = start_sample_server("oc_rc_reread").await;
    let a = &running.client;
    setup_acct(a, "(1, 100, 'x')").await;
    let (b, b_conn) = connect_as(running.bound, "tester", "oc_rc_reread_b").await;

    a.batch_execute("BEGIN; UPDATE acct SET bal = 200 WHERE id = 1;")
        .await
        .expect("A holds bal=200");

    let mut b_upsert = Box::pin(b.batch_execute(UPSERT_ADD));
    assert!(
        is_pending(b_upsert.as_mut(), 300).await,
        "B must block while A holds the row lock"
    );

    a.batch_execute("COMMIT").await.expect("A commit bal=200");
    tokio::time::timeout(Duration::from_secs(5), b_upsert)
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

/// 4. REPEATABLE READ / first-updater-wins: under REPEATABLE READ, two
///    concurrent upserts to id=1 — the second must get 40001.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_conflict_repeatable_read_second_updater_gets_40001() {
    let running = start_sample_server("oc_rr_40001").await;
    let a = &running.client;
    setup_acct(a, "(1, 100, 'x')").await;
    let (b, b_conn) = connect_as(running.bound, "tester", "oc_rr_40001_b").await;

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

    a.batch_execute(UPSERT_ADD).await.expect("A upsert");
    a.batch_execute("COMMIT").await.expect("A commit");

    let err = b
        .batch_execute(UPSERT_ADD)
        .await
        .expect_err("B's RR upsert must conflict with A's committed update");
    assert_eq!(
        err.code(),
        Some(&SqlState::T_R_SERIALIZATION_FAILURE),
        "second upserter under RR must get 40001, got {err:?}"
    );
    b.batch_execute("ROLLBACK").await.expect("B rollback");

    let bal: i32 = a
        .query_one("SELECT bal FROM acct WHERE id = 1", &[])
        .await
        .expect("read")
        .get(0);
    assert_eq!(bal, 110, "only A's committed +10 applied; B aborted");

    b_conn.abort();
    shutdown(running).await;
}

/// 5. DEADLOCK: A's upsert locks r1 then waits on r2; B's upsert locks r2 then
///    waits on r1. The detector aborts exactly one with 40P01.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_conflict_cross_row_deadlock_40p01() {
    let running = start_sample_server("oc_deadlock").await;
    let a = &running.client;
    setup_acct(a, "(1, 10, 'x'), (2, 20, 'y')").await;
    let (b, b_conn) = connect_as(running.bound, "tester", "oc_deadlock_b").await;

    let upsert_id = |id: i32| {
        format!(
            "INSERT INTO acct VALUES ({id}, 999, 'z') ON CONFLICT (id) DO UPDATE SET bal = bal + 1"
        )
    };

    a.batch_execute("BEGIN").await.expect("A begin");
    a.batch_execute(&upsert_id(1)).await.expect("A locks r1");

    b.batch_execute("BEGIN").await.expect("B begin");
    b.batch_execute(&upsert_id(2)).await.expect("B locks r2");

    let a_sql = upsert_id(2);
    let b_sql = upsert_id(1);
    let mut a_fut = Box::pin(a.batch_execute(&a_sql));
    let mut b_fut = Box::pin(b.batch_execute(&b_sql));

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
            a.batch_execute("ROLLBACK").await.expect("victim A rollback");
            tokio::time::timeout(Duration::from_secs(5), &mut b_fut)
                .await
                .expect("survivor B unblocks")
                .expect("survivor B succeeds");
            drop(b_fut);
            b.batch_execute("ROLLBACK").await.expect("B rollback");
        }
        Victim::B => {
            drop(b_fut);
            b.batch_execute("ROLLBACK").await.expect("victim B rollback");
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

/// 6. REGRESSION: single-connection upsert — DO UPDATE on a conflict and a
///    plain INSERT of a new key both work, and RETURNING reflects the applied
///    value now that the path locks and re-checks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_connection_on_conflict_regression() {
    let running = start_sample_server("oc_single_conn").await;
    let a = &running.client;
    setup_acct(a, "(1, 100, 'x')").await;

    // Conflict on id=1 → DO UPDATE; RETURNING reflects the applied value.
    let row = a
        .query_one(
            "INSERT INTO acct VALUES (1, 999, 'z') \
             ON CONFLICT (id) DO UPDATE SET bal = bal + 5 RETURNING bal",
            &[],
        )
        .await
        .expect("upsert update returning");
    let bal: i32 = row.get(0);
    assert_eq!(bal, 105, "RETURNING reflects the applied latest version");

    // No conflict on id=2 → plain INSERT.
    let affected = a
        .execute(
            "INSERT INTO acct VALUES (2, 50, 'new') ON CONFLICT (id) DO UPDATE SET bal = bal + 5",
            &[],
        )
        .await
        .expect("upsert insert");
    assert_eq!(affected, 1, "non-conflicting key inserts a new row");

    let bal2: i32 = a
        .query_one("SELECT bal FROM acct WHERE id = 2", &[])
        .await
        .expect("read id2")
        .get(0);
    assert_eq!(bal2, 50, "id=2 inserted with its own value");

    shutdown(running).await;
}
