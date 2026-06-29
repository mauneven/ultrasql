//! Adversarial two-connection tests for the MERGE WHEN MATCHED UPDATE /
//! DELETE write path's Exclusive row lock + EvalPlanQual latest-version
//! re-check.
//!
//! MERGE computes its matched set from a target/source join probe and used
//! to mutate the matched rows with NO row lock and NO concurrent-update
//! re-check — so a concurrent writer of a matched row lost its update,
//! exactly like the general UPDATE / DELETE path did before it was fixed.
//! `lower_real_merge` now wires the SAME `build_eval_plan_qual_no_predicate`
//! lock+recheck the general path uses
//! (`crates/ultrasql-server/src/pipeline/modify/merge.rs`), so every WHEN
//! MATCHED UPDATE / DELETE action Exclusive-locks its target tuple and
//! re-checks it against the latest committed version before mutating it.
//!
//! These prove, for the MERGE matched path:
//!
//! 1. lost update prevented — two concurrent MERGE `... SET bal = bal + s.amt`
//!    serialize on the row lock and both apply (100 → 120);
//! 2. FOR UPDATE blocks a MERGE matched UPDATE until the holder commits;
//! 3. READ COMMITTED re-read — the matched UPDATE applies to the latest
//!    committed version (200 → 210), not the stale snapshot;
//! 4. concurrent DELETE — a MERGE matched UPDATE / DELETE of a row another
//!    txn committed a DELETE of is a no-op (skipped);
//! 5. REPEATABLE READ first-updater-wins — the second concurrent MERGE
//!    matched UPDATE gets 40001;
//! 6. deadlock — cross-row MERGE matched UPDATE ordering → one victim 40P01;
//! 7. no regression — single-connection MERGE UPDATE / DELETE / INSERT work.

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

/// Create the `acct` target and `src` source tables. `acct` carries a third
/// `note TEXT` column purely to mirror the general-path fixtures; MERGE never
/// uses the int32-pair fused fast path regardless.
async fn setup_merge_tables(a: &tokio_postgres::Client, acct_rows: &str, src_rows: &str) {
    a.batch_execute(&format!(
        "CREATE TABLE acct (id INT NOT NULL, bal INT NOT NULL, note TEXT);
         INSERT INTO acct VALUES {acct_rows};
         CREATE TABLE src (id INT NOT NULL, amt INT NOT NULL);
         INSERT INTO src VALUES {src_rows};"
    ))
    .await
    .expect("setup");
}

const MERGE_ADD: &str = "MERGE INTO acct AS t USING src AS s ON t.id = s.id \
     WHEN MATCHED THEN UPDATE SET bal = bal + s.amt";

/// 1. LOST-UPDATE PREVENTED (the headline): two connections each run
///    `MERGE ... WHEN MATCHED THEN UPDATE SET bal = bal + s.amt` against the
///    same matched row concurrently. They must serialize on the row's
///    Exclusive lock and both apply (100 → 120); the second must re-read the
///    first's committed 110.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_matched_update_no_lost_update() {
    let running = start_sample_server("merge_lost_update").await;
    let a = &running.client;
    setup_merge_tables(a, "(1, 100, 'x')", "(1, 10)").await;
    let (b, b_conn) = connect_as(running.bound, "tester", "merge_lost_update_b").await;

    // A begins and applies +10 but stays open (uncommitted, holds the lock).
    a.batch_execute(&format!("BEGIN; {MERGE_ADD};"))
        .await
        .expect("A holds merge update");

    // B's MERGE matched UPDATE must block on A's Exclusive row lock.
    let mut b_merge = Box::pin(b.batch_execute(MERGE_ADD));
    assert!(
        is_pending(b_merge.as_mut(), 300).await,
        "B's MERGE matched UPDATE must block while A holds the row lock"
    );

    a.batch_execute("COMMIT").await.expect("A commit");
    tokio::time::timeout(Duration::from_secs(5), b_merge)
        .await
        .expect("B unblocks after A commits")
        .expect("B's MERGE succeeds");

    let bal: i32 = a
        .query_one("SELECT bal FROM acct WHERE id = 1", &[])
        .await
        .expect("read")
        .get(0);
    assert_eq!(
        bal, 120,
        "both +10 applied serially on the MERGE matched path — no lost update"
    );

    b_conn.abort();
    shutdown(running).await;
}

/// 2. FOR UPDATE vs MERGE matched UPDATE: A `SELECT ... FOR UPDATE id=1`; B's
///    MERGE matched UPDATE blocks until A commits, then applies on top.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn for_update_blocks_merge_matched_update() {
    let running = start_sample_server("merge_fu_blocks").await;
    let a = &running.client;
    setup_merge_tables(a, "(1, 100, 'x')", "(1, 10)").await;
    let (b, b_conn) = connect_as(running.bound, "tester", "merge_fu_blocks_b").await;

    a.batch_execute("BEGIN").await.expect("A begin");
    let locked = a
        .query("SELECT id, bal FROM acct WHERE id = 1 FOR UPDATE", &[])
        .await
        .expect("A locks id=1");
    assert_eq!(locked.len(), 1);

    let mut b_merge = Box::pin(b.batch_execute(MERGE_ADD));
    assert!(
        is_pending(b_merge.as_mut(), 300).await,
        "B's MERGE matched UPDATE must block while A holds FOR UPDATE"
    );

    a.batch_execute("COMMIT").await.expect("A commit");
    tokio::time::timeout(Duration::from_secs(5), b_merge)
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
///    B's MERGE is blocked; B must re-read the latest (200) and apply +10 →
///    210, NOT its stale snapshot of 100.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_read_committed_reads_latest_committed_version() {
    let running = start_sample_server("merge_rc_reread").await;
    let a = &running.client;
    setup_merge_tables(a, "(1, 100, 'x')", "(1, 10)").await;
    let (b, b_conn) = connect_as(running.bound, "tester", "merge_rc_reread_b").await;

    // A holds an uncommitted absolute-set UPDATE (bal = 200).
    a.batch_execute("BEGIN; UPDATE acct SET bal = 200 WHERE id = 1;")
        .await
        .expect("A holds bal=200");

    let mut b_merge = Box::pin(b.batch_execute(MERGE_ADD));
    assert!(
        is_pending(b_merge.as_mut(), 300).await,
        "B must block while A holds the row lock"
    );

    a.batch_execute("COMMIT").await.expect("A commit bal=200");
    tokio::time::timeout(Duration::from_secs(5), b_merge)
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

/// 4. CONCURRENT DELETE vs MERGE matched UPDATE: A commits `DELETE id=1`; B's
///    blocked MERGE matched UPDATE must skip the deleted row (0 rows).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_matched_update_skips_concurrently_deleted_row() {
    let running = start_sample_server("merge_concurrent_delete").await;
    let a = &running.client;
    setup_merge_tables(a, "(1, 100, 'x')", "(1, 10)").await;
    let (b, b_conn) = connect_as(running.bound, "tester", "merge_concurrent_delete_b").await;

    a.batch_execute("BEGIN; DELETE FROM acct WHERE id = 1;")
        .await
        .expect("A holds delete");

    let mut b_merge = Box::pin(b.batch_execute(MERGE_ADD));
    assert!(
        is_pending(b_merge.as_mut(), 300).await,
        "B must block on A's delete lock"
    );

    a.batch_execute("COMMIT").await.expect("A commit delete");
    tokio::time::timeout(Duration::from_secs(5), b_merge)
        .await
        .expect("B unblocks")
        .expect("B succeeds without resurrecting the deleted row");

    let remaining: i64 = a
        .query_one("SELECT count(*) FROM acct WHERE id = 1", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(
        remaining, 0,
        "B's matched UPDATE skipped the gone row — not resurrected"
    );

    b_conn.abort();
    shutdown(running).await;
}

/// 4b. CONCURRENT DELETE vs MERGE matched DELETE: A commits `DELETE id=1`;
///     B's blocked MERGE WHEN MATCHED THEN DELETE must skip it (0 rows).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_matched_delete_skips_concurrently_deleted_row() {
    let running = start_sample_server("merge_delete_concurrent_delete").await;
    let a = &running.client;
    setup_merge_tables(a, "(1, 100, 'x')", "(1, 10)").await;
    let (b, b_conn) = connect_as(running.bound, "tester", "merge_delete_concurrent_delete_b").await;

    a.batch_execute("BEGIN; DELETE FROM acct WHERE id = 1;")
        .await
        .expect("A holds delete");

    let merge_delete = "MERGE INTO acct AS t USING src AS s ON t.id = s.id \
         WHEN MATCHED THEN DELETE";
    let mut b_merge = Box::pin(b.batch_execute(merge_delete));
    assert!(
        is_pending(b_merge.as_mut(), 300).await,
        "B must block on A's delete lock"
    );

    a.batch_execute("COMMIT").await.expect("A commit delete");
    tokio::time::timeout(Duration::from_secs(5), b_merge)
        .await
        .expect("B unblocks")
        .expect("B's matched DELETE succeeds against the gone row");

    let remaining: i64 = a
        .query_one("SELECT count(*) FROM acct WHERE id = 1", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(remaining, 0, "row stays deleted");

    b_conn.abort();
    shutdown(running).await;
}

/// 5. REPEATABLE READ / first-updater-wins: under REPEATABLE READ, two
///    concurrent MERGE matched UPDATEs to id=1 — the second must get 40001.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_repeatable_read_second_updater_gets_40001() {
    let running = start_sample_server("merge_rr_40001").await;
    let a = &running.client;
    setup_merge_tables(a, "(1, 100, 'x')", "(1, 10)").await;
    let (b, b_conn) = connect_as(running.bound, "tester", "merge_rr_40001_b").await;

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

    a.batch_execute(MERGE_ADD).await.expect("A merge");
    a.batch_execute("COMMIT").await.expect("A commit");

    let err = b
        .batch_execute(MERGE_ADD)
        .await
        .expect_err("B's RR merge must conflict with A's committed update");
    assert_eq!(
        err.code(),
        Some(&SqlState::T_R_SERIALIZATION_FAILURE),
        "second MERGE updater under RR must get 40001, got {err:?}"
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

/// 6. DEADLOCK: A's MERGE locks r1 then waits on r2; B's MERGE locks r2 then
///    waits on r1. The detector aborts exactly one with 40P01.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_cross_row_deadlock_40p01() {
    let running = start_sample_server("merge_deadlock").await;
    let a = &running.client;
    setup_merge_tables(a, "(1, 10, 'x'), (2, 20, 'y')", "(1, 1), (2, 1)").await;
    let (b, b_conn) = connect_as(running.bound, "tester", "merge_deadlock_b").await;

    // Each connection first locks one row via a single-row MERGE, then both
    // attempt the other row, forming a cycle. A single-source-row MERGE keyed
    // to one id touches exactly that target row.
    let merge_id = |id: i32| {
        format!(
            "MERGE INTO acct AS t USING (SELECT id, amt FROM src WHERE id = {id}) AS s \
             ON t.id = s.id WHEN MATCHED THEN UPDATE SET bal = bal + s.amt"
        )
    };

    a.batch_execute("BEGIN").await.expect("A begin");
    a.batch_execute(&merge_id(1)).await.expect("A locks r1");

    b.batch_execute("BEGIN").await.expect("B begin");
    b.batch_execute(&merge_id(2)).await.expect("B locks r2");

    let a_sql = merge_id(2);
    let b_sql = merge_id(1);
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

/// 7. REGRESSION: single-connection MERGE UPDATE / DELETE / INSERT still work
///    end to end now that the matched path locks and re-checks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_connection_merge_regression() {
    let running = start_sample_server("merge_single_conn").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE acct (id INT NOT NULL, bal INT NOT NULL, note TEXT);
         INSERT INTO acct VALUES (1, 100, 'x'), (2, 200, 'y');
         CREATE TABLE src (id INT NOT NULL, amt INT NOT NULL, op TEXT);
         INSERT INTO src VALUES (1, 5, 'upd'), (2, 0, 'del'), (3, 30, 'ins');",
    )
    .await
    .expect("setup");

    a.batch_execute(
        "MERGE INTO acct AS t USING src AS s ON t.id = s.id \
             WHEN MATCHED AND s.op = 'del' THEN DELETE \
             WHEN MATCHED THEN UPDATE SET bal = bal + s.amt \
             WHEN NOT MATCHED THEN INSERT (id, bal, note) VALUES (s.id, s.amt, s.op)",
    )
    .await
    .expect("merge");

    let bal1: i32 = a
        .query_one("SELECT bal FROM acct WHERE id = 1", &[])
        .await
        .expect("read id1")
        .get(0);
    assert_eq!(bal1, 105, "id=1 matched UPDATE applied +5");

    let id2_count: i64 = a
        .query_one("SELECT count(*) FROM acct WHERE id = 2", &[])
        .await
        .expect("count id2")
        .get(0);
    assert_eq!(id2_count, 0, "id=2 matched DELETE removed it");

    let row3 = a
        .query_one("SELECT bal, note FROM acct WHERE id = 3", &[])
        .await
        .expect("read id3");
    let bal3: i32 = row3.get(0);
    let note3: String = row3.get(1);
    assert_eq!(bal3, 30, "id=3 NOT MATCHED INSERT used s.amt");
    assert_eq!(note3, "ins", "id=3 NOT MATCHED INSERT used s.op");

    shutdown(running).await;
}

/// 8. MULTI-ROW LOST-UPDATE PREVENTED: two source rows match two target rows.
///    A holds the MERGE uncommitted (both row locks); B's identical MERGE parks
///    on the first contended row, then resumes after A commits — the row whose
///    lock clears with no wait must still re-read the latest committed version,
///    so both rows land both increments.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_row_merge_no_lost_update() {
    let running = start_sample_server("merge_multi_lost_update").await;
    let a = &running.client;
    setup_merge_tables(a, "(1, 100, 'x'), (2, 200, 'y')", "(1, 10), (2, 10)").await;
    let (b, b_conn) = connect_as(running.bound, "tester", "merge_multi_lost_update_b").await;

    a.batch_execute(&format!("BEGIN; {MERGE_ADD};"))
        .await
        .expect("A holds both row locks");

    let mut b_merge = Box::pin(b.batch_execute(MERGE_ADD));
    assert!(
        is_pending(b_merge.as_mut(), 300).await,
        "B's multi-row MERGE must block while A holds the row locks"
    );

    a.batch_execute("COMMIT").await.expect("A commit");
    tokio::time::timeout(Duration::from_secs(5), b_merge)
        .await
        .expect("B unblocks after A commits")
        .expect("B's multi-row MERGE succeeds");

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
    assert_eq!(bal1, 120, "row 1: both +10 applied — no loss");
    assert_eq!(bal2, 220, "row 2: both +10 applied — no stale overwrite");

    b_conn.abort();
    shutdown(running).await;
}
