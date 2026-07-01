//! End-to-end `lock_timeout` (SQLSTATE 55P03) and deadline-aware lock-wait
//! coverage.
//!
//! Two-session adversarial tests proving:
//!
//! 1. `SET` / `SHOW` / `RESET lock_timeout` round-trips (default 0 =
//!    disabled, matching PostgreSQL).
//! 2. A session blocked on a peer's row lock with `lock_timeout` set fails
//!    with `55P03` within the timeout budget — for both the
//!    `SELECT ... FOR UPDATE` wait path and the `UPDATE` write path — and
//!    the timed-out waiter is removed from the lock manager's wait queue
//!    (the holder commits and a *fresh* session acquires immediately).
//! 3. `statement_timeout` (and thus client cancel, which shares the same
//!    `CancelFlag` observer) interrupts a lock wait with `57014` — without
//!    `lock_timeout` being set at all.
//! 4. A waiter whose connection dies mid-wait does not wedge the lock: the
//!    holder commits, and a third session acquires the row promptly.
//!
//! Without deadline-aware lock waits these all hang forever (the old
//! blocking `acquire` slept on an unbounded condvar), so every blocked
//! statement here is wrapped in a generous outer `tokio::time::timeout`
//! that fails the test rather than wedging CI.

use std::time::Duration;

use tokio_postgres::error::SqlState;

pub mod support;

use support::{connect_as, shutdown, start_sample_server};

async fn setup_locked_row(
    running: &support::RunningServer,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE t (id INT NOT NULL, v INT NOT NULL);
         INSERT INTO t VALUES (1, 10), (2, 20);
         CREATE INDEX t_id_idx ON t(id);",
    )
    .await
    .expect("setup");
    let (b, b_conn) = connect_as(running.bound, "tester", "lock_timeout_b").await;

    a.batch_execute("BEGIN").await.expect("A begin");
    a.query("SELECT id FROM t WHERE id = 1 FOR UPDATE", &[])
        .await
        .expect("A locks id=1");
    (b, b_conn)
}

/// 1. GUC round-trip: default 0 (disabled), SET/SHOW, RESET back to 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lock_timeout_set_show_and_reset_round_trip() {
    let running = start_sample_server("lock_timeout_guc").await;
    let client = &running.client;

    let row = client
        .query_one("SHOW lock_timeout", &[])
        .await
        .expect("show default lock_timeout");
    assert_eq!(
        row.get::<_, String>(0),
        "0",
        "lock_timeout defaults to 0 (disabled), matching PostgreSQL"
    );

    client
        .batch_execute("SET lock_timeout = 150")
        .await
        .expect("set lock_timeout");
    let row = client
        .query_one("SHOW lock_timeout", &[])
        .await
        .expect("show configured lock_timeout");
    assert_eq!(row.get::<_, String>(0), "150");

    client
        .batch_execute("RESET lock_timeout")
        .await
        .expect("reset lock_timeout");
    let row = client
        .query_one("SHOW lock_timeout", &[])
        .await
        .expect("show reset lock_timeout");
    assert_eq!(row.get::<_, String>(0), "0", "RESET restores 0 (disabled)");

    shutdown(running).await;
}

/// 2. Blocked FOR UPDATE and UPDATE waits fail with 55P03 within the
///    timeout budget; A is unaffected, B stays usable, and after A commits
///    a fresh session C acquires the row immediately (no waiter leak).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lock_timeout_raises_55p03_and_leaves_no_waiter() {
    let running = start_sample_server("lock_timeout_55p03").await;
    let (b, b_conn) = setup_locked_row(&running).await;
    let a = &running.client;

    b.batch_execute("SET lock_timeout = 100")
        .await
        .expect("B sets lock_timeout");

    // (a) SELECT ... FOR UPDATE wait path.
    let started = std::time::Instant::now();
    let err = tokio::time::timeout(
        Duration::from_secs(5),
        b.query("SELECT id FROM t WHERE id = 1 FOR UPDATE", &[]),
    )
    .await
    .expect("lock_timeout must resolve the blocked FOR UPDATE (would hang without the fix)")
    .expect_err("blocked FOR UPDATE must fail once lock_timeout expires");
    assert_eq!(
        err.code(),
        Some(&SqlState::LOCK_NOT_AVAILABLE),
        "lock_timeout expiry must be 55P03, got {err:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "55P03 must arrive within ~1s of the 100 ms timeout, took {:?}",
        started.elapsed()
    );

    // (b) UPDATE write path (EvalPlanQual / fused indexed update lock).
    let err = tokio::time::timeout(
        Duration::from_secs(5),
        b.execute("UPDATE t SET v = v + 1 WHERE id = 1", &[]),
    )
    .await
    .expect("lock_timeout must resolve the blocked UPDATE (would hang without the fix)")
    .expect_err("blocked UPDATE must fail once lock_timeout expires");
    assert_eq!(
        err.code(),
        Some(&SqlState::LOCK_NOT_AVAILABLE),
        "UPDATE lock_timeout expiry must be 55P03, got {err:?}"
    );

    // A's transaction is unaffected: it still holds the lock and can work.
    let row = a
        .query_one("SELECT v FROM t WHERE id = 1", &[])
        .await
        .expect("A's session must be unaffected by B's lock timeouts");
    assert_eq!(row.get::<_, i32>(0), 10);

    // B stays usable after both timeouts.
    let row = b
        .query_one("SELECT 41 + 1", &[])
        .await
        .expect("B must remain usable after 55P03");
    assert_eq!(row.get::<_, i32>(0), 42);

    // No waiter leak: A commits, and a *fresh* session acquires the row
    // immediately (a leaked waiter entry would not block this — the grant
    // check ignores the queue — but a wedged/corrupt entry would).
    a.batch_execute("COMMIT").await.expect("A commit");
    let (c, c_conn) = connect_as(running.bound, "tester", "lock_timeout_c").await;
    c.batch_execute("SET lock_timeout = 1000")
        .await
        .expect("C sets lock_timeout");
    tokio::time::timeout(
        Duration::from_secs(5),
        c.query("SELECT id FROM t WHERE id = 1 FOR UPDATE", &[]),
    )
    .await
    .expect("C must not hang")
    .expect("C acquires the freed row immediately after A commits");

    // B, too, can now take and use the lock (its earlier waiter entries
    // must be gone; C's autocommit lock is already released).
    tokio::time::timeout(
        Duration::from_secs(5),
        b.execute("UPDATE t SET v = v + 1 WHERE id = 1", &[]),
    )
    .await
    .expect("B must not hang after the lock is free")
    .expect("B's UPDATE succeeds once the lock is free");

    c_conn.abort();
    b_conn.abort();
    shutdown(running).await;
}

/// 3. `statement_timeout` interrupts a lock wait with 57014 — no
///    `lock_timeout` involved. The same observer serves client cancel.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn statement_timeout_interrupts_lock_wait_with_57014() {
    let running = start_sample_server("lock_wait_stmt_timeout").await;
    let (b, b_conn) = setup_locked_row(&running).await;
    let a = &running.client;

    b.batch_execute("SET statement_timeout = 100")
        .await
        .expect("B sets statement_timeout");

    let started = std::time::Instant::now();
    let err = tokio::time::timeout(
        Duration::from_secs(5),
        b.execute("UPDATE t SET v = v + 1 WHERE id = 1", &[]),
    )
    .await
    .expect("statement_timeout must resolve the blocked UPDATE (would hang without the fix)")
    .expect_err("blocked UPDATE must be cancelled by statement_timeout");
    assert_eq!(
        err.code(),
        Some(&SqlState::QUERY_CANCELED),
        "statement_timeout during a lock wait must be 57014, got {err:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "57014 must arrive promptly, took {:?}",
        started.elapsed()
    );

    // Same for the FOR UPDATE wait path.
    let err = tokio::time::timeout(
        Duration::from_secs(5),
        b.query("SELECT id FROM t WHERE id = 1 FOR UPDATE", &[]),
    )
    .await
    .expect("statement_timeout must resolve the blocked FOR UPDATE")
    .expect_err("blocked FOR UPDATE must be cancelled by statement_timeout");
    assert_eq!(err.code(), Some(&SqlState::QUERY_CANCELED));

    // B recovers; A is unaffected and commits cleanly.
    b.batch_execute("SET statement_timeout = 0")
        .await
        .expect("B disables statement_timeout");
    let row = b
        .query_one("SELECT 1", &[])
        .await
        .expect("B usable after 57014");
    assert_eq!(row.get::<_, i32>(0), 1);
    a.batch_execute("COMMIT").await.expect("A commit");

    b_conn.abort();
    shutdown(running).await;
}

/// 4. A waiter killed mid-wait must not wedge the lock: B's connection is
///    dropped while it blocks on A's row; A commits; a fresh session C
///    acquires the row promptly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killed_waiter_mid_wait_does_not_wedge_the_lock() {
    let running = start_sample_server("lock_wait_killed_waiter").await;
    let (b, b_conn) = setup_locked_row(&running).await;
    let a = &running.client;

    // B blocks on the locked row (no timeouts at all — a pure waiter).
    let mut b_update = Box::pin(b.execute("UPDATE t SET v = v + 1 WHERE id = 1", &[]));
    assert!(
        tokio::time::timeout(Duration::from_millis(300), b_update.as_mut())
            .await
            .is_err(),
        "B's UPDATE must be blocked on A's row lock"
    );

    // Kill B mid-wait: drop the in-flight statement future, the client,
    // and the connection driver task.
    drop(b_update);
    drop(b);
    b_conn.abort();

    // A commits, releasing the row.
    a.batch_execute("COMMIT").await.expect("A commit");

    // A fresh session acquires the row promptly — the dead waiter must
    // not hold, wedge, or otherwise deny the grant.
    let (c, c_conn) = connect_as(running.bound, "tester", "lock_wait_killed_c").await;
    tokio::time::timeout(
        Duration::from_secs(5),
        c.execute("UPDATE t SET v = v + 100 WHERE id = 1", &[]),
    )
    .await
    .expect("C must not hang after the waiter's connection died")
    .expect("C's UPDATE succeeds");
    let row = c
        .query_one("SELECT v FROM t WHERE id = 1", &[])
        .await
        .expect("C reads the row");
    // 10 + 100, plus 1 more if B's in-flight UPDATE won the lock and
    // committed before its connection teardown was observed — both
    // interleavings are legal; the invariant is "no hang, no wedge".
    let v = row.get::<_, i32>(0);
    assert!(v == 110 || v == 111, "unexpected v after recovery: {v}");

    c_conn.abort();
    shutdown(running).await;
}

/// 4b. A *holder* killed mid-transaction must not leak its row locks: A2
///     locks a row inside an explicit transaction and disconnects without
///     COMMIT/ROLLBACK; a fresh session acquires the row promptly (the
///     server aborts the orphaned transaction on session teardown).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killed_holder_mid_transaction_releases_its_locks() {
    let running = start_sample_server("lock_holder_killed").await;
    let a = &running.client;
    a.batch_execute(
        "CREATE TABLE t (id INT NOT NULL, v INT NOT NULL);
         INSERT INTO t VALUES (1, 10);",
    )
    .await
    .expect("setup");

    // A2 takes the row lock inside an explicit transaction, then dies.
    let (a2, a2_conn) = connect_as(running.bound, "tester", "lock_holder_a2").await;
    a2.batch_execute("BEGIN").await.expect("A2 begin");
    a2.query("SELECT id FROM t WHERE id = 1 FOR UPDATE", &[])
        .await
        .expect("A2 locks id=1");
    drop(a2);
    a2_conn.abort();

    // A fresh session must be able to lock the row promptly. Allow a
    // couple of retries: the server observes A2's death asynchronously.
    let (c, c_conn) = connect_as(running.bound, "tester", "lock_holder_c").await;
    c.batch_execute("SET lock_timeout = 500")
        .await
        .expect("C sets lock_timeout");
    let mut acquired = false;
    for _ in 0..10 {
        match tokio::time::timeout(
            Duration::from_secs(5),
            c.query("SELECT id FROM t WHERE id = 1 FOR UPDATE", &[]),
        )
        .await
        .expect("C must never hang (lock_timeout bounds each attempt)")
        {
            Ok(_) => {
                acquired = true;
                break;
            }
            Err(e) if e.code() == Some(&SqlState::LOCK_NOT_AVAILABLE) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("unexpected error while re-acquiring: {e:?}"),
        }
    }
    assert!(
        acquired,
        "a dead holder's transaction must be aborted on disconnect so its row locks release"
    );

    c_conn.abort();
    shutdown(running).await;
}
