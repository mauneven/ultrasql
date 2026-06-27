//! Reproduction: faithful mini-fuzz of the savepoint battery's
//! `z_index_seq_agreement_fuzz` pattern — s1 mutates (general path) under a
//! transaction with savepoints while s2 observes — to pinpoint the EPQ
//! self-block.

use std::time::Duration;

pub mod support;

use support::{connect_as, shutdown, start_sample_server};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_row_updated_twice_in_one_txn_does_not_self_block() {
    let running = start_sample_server("epq_self_block").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE t_idx (id INT NOT NULL, val INT NOT NULL, name TEXT);
         INSERT INTO t_idx VALUES (1, 10, 'n1');
         CREATE INDEX t_idx_val ON t_idx(val);",
    )
    .await
    .expect("setup");

    let fut = async {
        a.batch_execute("BEGIN").await.expect("begin");
        a.batch_execute("UPDATE t_idx SET val = 20 WHERE id = 1")
            .await
            .expect("update 1");
        a.batch_execute("UPDATE t_idx SET val = 30 WHERE id = 1")
            .await
            .expect("update 2");
        a.batch_execute("COMMIT").await.expect("commit");
    };
    tokio::time::timeout(Duration::from_secs(8), fut)
        .await
        .expect("same-row repeated UPDATE must not self-block");

    shutdown(running).await;
}

/// The headline self-block: UPDATE/DELETE under a SAVEPOINT, ROLLBACK TO,
/// then another UPDATE/DELETE of the same row. The EvalPlanQual row lock must
/// be owned by the top-level xid, not the (now rolled-back) subxid, or the
/// re-lock self-blocks forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn savepoint_rollback_then_relock_same_row_does_not_self_block() {
    let running = start_sample_server("epq_sp_relock").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE t_idx (id INT NOT NULL, val INT NOT NULL, name TEXT);
         INSERT INTO t_idx VALUES (1, 10, 'n1');
         CREATE INDEX t_idx_val ON t_idx(val);",
    )
    .await
    .expect("setup");

    let fut = async {
        a.batch_execute("BEGIN").await.expect("begin");
        // UPDATE under savepoint s2 (locks id=1 under the subxid), roll back.
        a.batch_execute("SAVEPOINT s2").await.expect("sp2");
        a.batch_execute("UPDATE t_idx SET val = 999 WHERE id = 1")
            .await
            .expect("update under sp");
        a.batch_execute("ROLLBACK TO SAVEPOINT s2")
            .await
            .expect("rollback to sp2");
        // DELETE under savepoint s3 must re-lock id=1 (top-level xid owns the
        // lock), not block behind the rolled-back subxid.
        a.batch_execute("SAVEPOINT s3").await.expect("sp3");
        a.batch_execute("DELETE FROM t_idx WHERE id = 1")
            .await
            .expect("delete under sp");
        a.batch_execute("ROLLBACK TO SAVEPOINT s3")
            .await
            .expect("rollback to sp3");
        // And a final plain UPDATE of the same row at the top level.
        a.batch_execute("UPDATE t_idx SET val = 42 WHERE id = 1")
            .await
            .expect("final update");
        a.batch_execute("COMMIT").await.expect("commit");
    };
    tokio::time::timeout(Duration::from_secs(8), fut)
        .await
        .expect("savepoint rollback + re-lock must not self-block");

    let val: i32 = a
        .query_one("SELECT val FROM t_idx WHERE id = 1", &[])
        .await
        .expect("read")
        .get(0);
    assert_eq!(
        val, 42,
        "pre-images restored; final top-level update applied"
    );

    shutdown(running).await;
}

/// Faithful mini-fuzz: s1 BEGIN, INSERT/UPDATE/DELETE under savepoints with
/// interleaved SELECTs, COMMIT/ROLLBACK, then s2 (a peer connection) reads
/// the committed state. This mirrors `z_index_seq_agreement_fuzz` on a small
/// scale to reproduce any EPQ-induced stall deterministically.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mini_fuzz_general_dml_with_peer_observer_does_not_stall() {
    let running = start_sample_server("epq_mini_fuzz").await;
    let s1 = &running.client;
    s1.batch_execute(
        "CREATE TABLE t_idx (id INT NOT NULL, val INT NOT NULL, name TEXT);
         CREATE INDEX t_idx_val ON t_idx(val);",
    )
    .await
    .expect("setup");
    let (s2, s2_conn) = connect_as(running.bound, "tester", "epq_mini_fuzz_s2").await;

    let fut = async {
        let mut next_id = 1;
        for round in 0..8 {
            s1.batch_execute("BEGIN").await.expect("begin");
            // INSERT a couple rows.
            for _ in 0..2 {
                let id = next_id;
                next_id += 1;
                s1.batch_execute(&format!(
                    "INSERT INTO t_idx (id, val, name) VALUES ({id}, {id}, 'n{id}')"
                ))
                .await
                .expect("insert");
            }
            // Read live ids inside the txn (plain SELECT, no lock).
            let live = s1
                .query("SELECT id FROM t_idx", &[])
                .await
                .expect("select live");
            // UPDATE then DELETE the first live id (general path).
            if let Some(row) = live.first() {
                let id: i32 = row.get(0);
                s1.batch_execute(&format!("UPDATE t_idx SET val = {round} WHERE id = {id}"))
                    .await
                    .expect("update");
                // Re-read then update the SAME row again (own-write re-scan).
                s1.batch_execute(&format!(
                    "UPDATE t_idx SET val = {} WHERE id = {id}",
                    round + 100
                ))
                .await
                .expect("update again");
                s1.batch_execute(&format!("DELETE FROM t_idx WHERE id = {id}"))
                    .await
                    .expect("delete");
            }
            if round % 3 == 0 {
                s1.batch_execute("ROLLBACK").await.expect("rollback");
            } else {
                s1.batch_execute("COMMIT").await.expect("commit");
            }
            // Peer observes committed state (independent connection).
            let _expected = s2
                .query("SELECT id FROM t_idx", &[])
                .await
                .expect("s2 observe");
        }
    };
    tokio::time::timeout(Duration::from_secs(20), fut)
        .await
        .expect("mini-fuzz must not stall on EPQ locks");

    drop(s2);
    s2_conn.abort();
    shutdown(running).await;
}
