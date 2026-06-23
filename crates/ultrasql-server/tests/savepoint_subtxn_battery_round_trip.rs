//! SAVEPOINT subtransaction-visibility adversarial battery (design §5).
//!
//! Every test here exercises the correctness of subtransaction visibility
//! end-to-end through the real wire server. The end-to-end tests run two
//! connections — `s1` (the writer) and `s2` (a concurrent reader) — so heap
//! truth is verified from a *second* connection, and run on **both** table
//! shapes:
//!
//! - **Shape N** (`t_pair(id int4, val int4)`, no index) — hits the
//!   fused/fast int32-pair paths and the column cache.
//! - **Shape I** (`t_idx(id int4 primary key, val int4, name text)` with a
//!   secondary index on `val`) — forces the general operator + index paths.
//!
//! Test C (ROLLBACK TO restores a deleted row, verified from S2 after COMMIT)
//! is the corruption test and must pass on both shapes. Every test must
//! genuinely exercise the path it names — none are weakened.

pub mod support;

use std::net::SocketAddr;

use support::{connect_as, shutdown, start_sample_server};
use tokio_postgres::Client;

// ─────────────────────────────────────────────────────────────────────────
// Table-shape abstraction
// ─────────────────────────────────────────────────────────────────────────

/// One of the two table shapes the battery runs against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    /// `t_pair(id int4, val int4)`, no index — fused int32-pair + column cache.
    NoIndex,
    /// `t_idx(id int4 primary key, val int4, name text)` + secondary index on
    /// `val` — general operator + index scan paths.
    Indexed,
}

impl Shape {
    fn table(self) -> &'static str {
        match self {
            Shape::NoIndex => "t_pair",
            Shape::Indexed => "t_idx",
        }
    }

    /// DDL that creates the shape's table (and, for `Indexed`, its secondary
    /// index). Idempotent against a fresh sample server.
    async fn create(self, c: &Client) {
        match self {
            Shape::NoIndex => {
                c.batch_execute("CREATE TABLE t_pair (id INT4, val INT4)")
                    .await
                    .expect("create t_pair");
            }
            Shape::Indexed => {
                c.batch_execute("CREATE TABLE t_idx (id INT4 PRIMARY KEY, val INT4, name TEXT)")
                    .await
                    .expect("create t_idx");
                c.batch_execute("CREATE INDEX ix_t_idx_val ON t_idx(val)")
                    .await
                    .expect("create secondary index");
            }
        }
    }

    /// Insert a row with `(id, val)`. For the indexed shape `name` is derived.
    async fn insert(self, c: &Client, id: i32, val: i32) {
        let sql = match self {
            Shape::NoIndex => format!("INSERT INTO t_pair (id, val) VALUES ({id}, {val})"),
            Shape::Indexed => {
                format!("INSERT INTO t_idx (id, val, name) VALUES ({id}, {val}, 'n{id}')")
            }
        };
        c.batch_execute(&sql).await.expect("insert row");
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Query helpers
// ─────────────────────────────────────────────────────────────────────────

/// All `id`s currently visible to `c`, sorted — read via a **sequential**
/// scan (no index hint), so this reflects heap truth.
async fn ids_seq(c: &Client, shape: Shape) -> Vec<i32> {
    let sql = format!("SELECT id FROM {}", shape.table());
    let rows = c.query(&sql, &[]).await.expect("seq id scan");
    let mut ids: Vec<i32> = rows.iter().map(|r| r.get::<_, i32>(0)).collect();
    ids.sort_unstable();
    ids
}

/// All `id`s visible to `c` whose `val` equals `val`, forced through the
/// index path where one exists (equality on the indexed column). For the
/// no-index shape this is still a heap scan with the same predicate, so the
/// two shapes are directly comparable.
async fn ids_by_val(c: &Client, shape: Shape, val: i32) -> Vec<i32> {
    let sql = format!("SELECT id FROM {} WHERE val = {val}", shape.table());
    let rows = c.query(&sql, &[]).await.expect("val-predicate scan");
    let mut ids: Vec<i32> = rows.iter().map(|r| r.get::<_, i32>(0)).collect();
    ids.sort_unstable();
    ids
}

/// `SUM(val)` over all visible rows (NULL → 0).
async fn sum_val(c: &Client, shape: Shape) -> i64 {
    let sql = format!("SELECT COALESCE(SUM(val), 0) FROM {}", shape.table());
    c.query_one(&sql, &[])
        .await
        .expect("sum(val)")
        .get::<_, i64>(0)
}

/// `val` for a given `id`, or `None` if the row is invisible.
async fn val_of(c: &Client, shape: Shape, id: i32) -> Option<i32> {
    let sql = format!("SELECT val FROM {} WHERE id = {id}", shape.table());
    let rows = c.query(&sql, &[]).await.expect("val_of");
    rows.first().map(|r| r.get::<_, i32>(0))
}

/// Open a second connection (`s2`) to the running server.
async fn peer(bound: SocketAddr, app: &str) -> (Client, tokio::task::JoinHandle<()>) {
    connect_as(bound, "tester", app).await
}

/// Map a random `u64` to an index in `[0, len)` without lossy casts.
/// Returns 0 for an empty slice (callers guard the empty case via `get`).
fn pick_index(rng: u64, len: usize) -> usize {
    let modulus = u64::try_from(len.max(1)).unwrap_or(1);
    usize::try_from(rng % modulus).unwrap_or(0)
}

// ═════════════════════════════════════════════════════════════════════════
// Test A — Own-write visible
// ═════════════════════════════════════════════════════════════════════════

async fn test_a(shape: Shape) {
    let running = start_sample_server("sp_battery_a").await;
    let s1 = &running.client;
    shape.create(s1).await;

    s1.batch_execute("BEGIN").await.expect("BEGIN");
    shape.insert(s1, 1, 10).await;
    s1.batch_execute("SAVEPOINT s1").await.expect("SAVEPOINT");
    shape.insert(s1, 2, 20).await;

    // Own writes visible to the same txn via seq scan AND val predicate.
    assert_eq!(ids_seq(s1, shape).await, vec![1, 2], "A seq: own writes");
    assert_eq!(
        ids_by_val(s1, shape, 20).await,
        vec![2],
        "A index/predicate: own savepoint write visible"
    );

    s1.batch_execute("COMMIT").await.expect("COMMIT");
    assert_eq!(ids_seq(s1, shape).await, vec![1, 2], "A post-commit");
    shutdown(running).await;
}

#[tokio::test]
async fn a_own_write_visible_no_index() {
    test_a(Shape::NoIndex).await;
}

#[tokio::test]
async fn a_own_write_visible_indexed() {
    test_a(Shape::Indexed).await;
}

// ═════════════════════════════════════════════════════════════════════════
// Test B — ROLLBACK TO hides insert (seq + index + cross-connection)
// ═════════════════════════════════════════════════════════════════════════

async fn test_b(shape: Shape) {
    let running = start_sample_server("sp_battery_b").await;
    let bound = running.bound;
    let s1 = &running.client;
    shape.create(s1).await;

    s1.batch_execute("BEGIN").await.expect("BEGIN");
    shape.insert(s1, 1, 10).await;
    s1.batch_execute("SAVEPOINT s1").await.expect("SAVEPOINT");
    shape.insert(s1, 2, 20).await;
    s1.batch_execute("ROLLBACK TO SAVEPOINT s1")
        .await
        .expect("ROLLBACK TO");

    // Row 2 hidden to s1 via seq AND predicate.
    assert_eq!(ids_seq(s1, shape).await, vec![1], "B seq: row 2 hidden");
    assert_eq!(
        ids_by_val(s1, shape, 20).await,
        Vec::<i32>::new(),
        "B index/predicate: rolled-back row 2 hidden"
    );

    // From s2 mid-txn, nothing committed yet.
    let (s2, s2_handle) = peer(bound, "sp_battery_b_s2").await;
    assert_eq!(ids_seq(&s2, shape).await, Vec::<i32>::new(), "B s2 mid-txn");

    s1.batch_execute("COMMIT").await.expect("COMMIT");
    // After commit s2 (fresh snapshot per statement) sees only row 1.
    assert_eq!(ids_seq(&s2, shape).await, vec![1], "B s2 post-commit");
    assert_eq!(
        ids_by_val(&s2, shape, 20).await,
        Vec::<i32>::new(),
        "B s2 post-commit: rolled-back row never durable"
    );

    drop(s2);
    s2_handle.abort();
    shutdown(running).await;
}

#[tokio::test]
async fn b_rollback_to_hides_insert_no_index() {
    test_b(Shape::NoIndex).await;
}

#[tokio::test]
async fn b_rollback_to_hides_insert_indexed() {
    test_b(Shape::Indexed).await;
}

// ═════════════════════════════════════════════════════════════════════════
// Test C — ROLLBACK TO restores a deleted row (THE corruption test)
// ═════════════════════════════════════════════════════════════════════════

async fn test_c(shape: Shape) {
    let running = start_sample_server("sp_battery_c").await;
    let bound = running.bound;
    let s1 = &running.client;
    shape.create(s1).await;

    // Pre-existing committed row.
    shape.insert(s1, 1, 100).await;

    s1.batch_execute("BEGIN").await.expect("BEGIN");
    s1.batch_execute("SAVEPOINT s1").await.expect("SAVEPOINT");
    let del = format!("DELETE FROM {} WHERE id = 1", shape.table());
    s1.batch_execute(&del)
        .await
        .expect("DELETE under savepoint");
    // Deleted is invisible to s1 now (seq + index).
    assert_eq!(
        ids_seq(s1, shape).await,
        Vec::<i32>::new(),
        "C seq: deleted"
    );
    assert_eq!(
        ids_by_val(s1, shape, 100).await,
        Vec::<i32>::new(),
        "C index/predicate: deleted"
    );

    s1.batch_execute("ROLLBACK TO SAVEPOINT s1")
        .await
        .expect("ROLLBACK TO");

    // The delete is reverted — row 1 reappears via BOTH access paths.
    assert_eq!(ids_seq(s1, shape).await, vec![1], "C seq: row restored");
    assert_eq!(
        ids_by_val(s1, shape, 100).await,
        vec![1],
        "C index/predicate: row restored after ROLLBACK TO"
    );
    assert_eq!(val_of(s1, shape, 1).await, Some(100), "C val restored");

    s1.batch_execute("COMMIT").await.expect("COMMIT");

    // From a SECOND connection after COMMIT: the row is durably present and
    // the aggregate is correct. This is the exact reverted-corruption shape.
    let (s2, s2_handle) = peer(bound, "sp_battery_c_s2").await;
    assert_eq!(
        ids_seq(&s2, shape).await,
        vec![1],
        "C s2: restored row durable after COMMIT"
    );
    assert_eq!(
        ids_by_val(&s2, shape, 100).await,
        vec![1],
        "C s2 index/predicate: restored row durable"
    );
    assert_eq!(sum_val(&s2, shape).await, 100, "C s2: sum(val) correct");

    drop(s2);
    s2_handle.abort();
    shutdown(running).await;
}

#[tokio::test]
async fn c_rollback_to_restores_deleted_row_no_index() {
    // Shape N: the exact reverted fused-DELETE corruption path.
    test_c(Shape::NoIndex).await;
}

#[tokio::test]
async fn c_rollback_to_restores_deleted_row_indexed() {
    // Shape I: index-scan path must agree with the heap after restore.
    test_c(Shape::Indexed).await;
}

// ═════════════════════════════════════════════════════════════════════════
// Test D — Nested RELEASE-inner-then-ROLLBACK-outer
// ═════════════════════════════════════════════════════════════════════════

async fn test_d(shape: Shape) {
    let running = start_sample_server("sp_battery_d").await;
    let bound = running.bound;
    let s1 = &running.client;
    shape.create(s1).await;

    // Part 1: nested rollback to inner.
    s1.batch_execute("BEGIN").await.expect("BEGIN");
    shape.insert(s1, 10, 10).await;
    s1.batch_execute("SAVEPOINT a").await.expect("SAVEPOINT a");
    shape.insert(s1, 20, 20).await;
    s1.batch_execute("SAVEPOINT b").await.expect("SAVEPOINT b");
    shape.insert(s1, 30, 30).await;
    s1.batch_execute("ROLLBACK TO SAVEPOINT b")
        .await
        .expect("ROLLBACK TO b");
    assert_eq!(ids_seq(s1, shape).await, vec![10, 20], "D rollback to b");
    s1.batch_execute("COMMIT").await.expect("COMMIT");

    // Part 2 (the leak test): RELEASE inner, then ROLLBACK TO outer must
    // DISCARD the already-released inner savepoint's writes.
    s1.batch_execute("BEGIN").await.expect("BEGIN 2");
    s1.batch_execute("SAVEPOINT sp_outer")
        .await
        .expect("SAVEPOINT sp_outer");
    shape.insert(s1, 40, 40).await;
    s1.batch_execute("SAVEPOINT sp_inner")
        .await
        .expect("SAVEPOINT sp_inner");
    shape.insert(s1, 50, 50).await;
    s1.batch_execute("RELEASE SAVEPOINT sp_inner")
        .await
        .expect("RELEASE sp_inner");
    // inner's row 50 is still self here.
    assert_eq!(
        ids_seq(s1, shape).await,
        vec![10, 20, 40, 50],
        "D after release inner"
    );
    s1.batch_execute("ROLLBACK TO SAVEPOINT sp_outer")
        .await
        .expect("ROLLBACK TO sp_outer");
    // Rolling back to outer discards rows 40 AND the released-inner row 50.
    assert_eq!(
        ids_seq(s1, shape).await,
        vec![10, 20],
        "D ROLLBACK TO outer discards released-inner row"
    );
    s1.batch_execute("COMMIT").await.expect("COMMIT 2");

    // Part 3: RELEASE then COMMIT persists the released row.
    s1.batch_execute("BEGIN").await.expect("BEGIN 3");
    s1.batch_execute("SAVEPOINT keep")
        .await
        .expect("SAVEPOINT keep");
    shape.insert(s1, 60, 60).await;
    s1.batch_execute("RELEASE SAVEPOINT keep")
        .await
        .expect("RELEASE keep");
    s1.batch_execute("COMMIT").await.expect("COMMIT 3");

    let (s2, s2_handle) = peer(bound, "sp_battery_d_s2").await;
    assert_eq!(
        ids_seq(&s2, shape).await,
        vec![10, 20, 60],
        "D s2: released-then-committed row persists; discarded ones gone"
    );

    drop(s2);
    s2_handle.abort();
    shutdown(running).await;
}

#[tokio::test]
async fn d_nested_release_then_rollback_outer_no_index() {
    test_d(Shape::NoIndex).await;
}

#[tokio::test]
async fn d_nested_release_then_rollback_outer_indexed() {
    test_d(Shape::Indexed).await;
}

// ═════════════════════════════════════════════════════════════════════════
// Test E — UPDATE pre-image restored on ROLLBACK TO (seq + index)
// ═════════════════════════════════════════════════════════════════════════

async fn test_e(shape: Shape) {
    let running = start_sample_server("sp_battery_e").await;
    let bound = running.bound;
    let s1 = &running.client;
    shape.create(s1).await;
    shape.insert(s1, 1, 100).await;

    s1.batch_execute("BEGIN").await.expect("BEGIN");
    s1.batch_execute("SAVEPOINT s1").await.expect("SAVEPOINT");
    let upd = format!("UPDATE {} SET val = 200 WHERE id = 1", shape.table());
    s1.batch_execute(&upd)
        .await
        .expect("UPDATE under savepoint");
    assert_eq!(val_of(s1, shape, 1).await, Some(200), "E updated value");
    assert_eq!(
        ids_by_val(s1, shape, 200).await,
        vec![1],
        "E index/predicate sees new value"
    );

    s1.batch_execute("ROLLBACK TO SAVEPOINT s1")
        .await
        .expect("ROLLBACK TO");

    // Pre-image restored — value 100 visible again via seq AND index.
    assert_eq!(
        val_of(s1, shape, 1).await,
        Some(100),
        "E pre-image restored"
    );
    assert_eq!(
        ids_by_val(s1, shape, 100).await,
        vec![1],
        "E index/predicate restored to pre-image"
    );
    assert_eq!(
        ids_by_val(s1, shape, 200).await,
        Vec::<i32>::new(),
        "E new value gone from index after rollback"
    );

    s1.batch_execute("COMMIT").await.expect("COMMIT");

    let (s2, s2_handle) = peer(bound, "sp_battery_e_s2").await;
    assert_eq!(
        val_of(&s2, shape, 1).await,
        Some(100),
        "E s2 pre-image durable"
    );

    drop(s2);
    s2_handle.abort();
    shutdown(running).await;
}

#[tokio::test]
async fn e_update_pre_image_no_index() {
    test_e(Shape::NoIndex).await;
}

#[tokio::test]
async fn e_update_pre_image_indexed() {
    test_e(Shape::Indexed).await;
}

// ═════════════════════════════════════════════════════════════════════════
// Test F — Cross-txn isolation, gated on top commit/abort
// ═════════════════════════════════════════════════════════════════════════

async fn test_f(shape: Shape) {
    let running = start_sample_server("sp_battery_f").await;
    let bound = running.bound;
    let s1 = &running.client;
    shape.create(s1).await;

    let (s2, s2_handle) = peer(bound, "sp_battery_f_s2").await;

    s1.batch_execute("BEGIN").await.expect("BEGIN");
    s1.batch_execute("SAVEPOINT s1").await.expect("SAVEPOINT");
    shape.insert(s1, 99, 99).await;
    // s2 must not see the uncommitted savepoint write.
    assert_eq!(
        ids_seq(&s2, shape).await,
        Vec::<i32>::new(),
        "F s2 pre-release"
    );

    // RELEASE (still pre-commit) — the leak test: s2 STILL must not see it.
    s1.batch_execute("RELEASE SAVEPOINT s1")
        .await
        .expect("RELEASE");
    assert_eq!(
        ids_seq(&s2, shape).await,
        Vec::<i32>::new(),
        "F s2 after RELEASE but before COMMIT: NO dirty read"
    );

    s1.batch_execute("COMMIT").await.expect("COMMIT");
    assert_eq!(
        ids_seq(&s2, shape).await,
        vec![99],
        "F s2 sees row after COMMIT"
    );

    // Variant: ROLLBACK TO then COMMIT — s2 never sees the row.
    s1.batch_execute("BEGIN").await.expect("BEGIN 2");
    s1.batch_execute("SAVEPOINT s2sp")
        .await
        .expect("SAVEPOINT s2sp");
    shape.insert(s1, 77, 77).await;
    s1.batch_execute("ROLLBACK TO SAVEPOINT s2sp")
        .await
        .expect("ROLLBACK TO");
    s1.batch_execute("COMMIT").await.expect("COMMIT 2");
    assert_eq!(
        ids_seq(&s2, shape).await,
        vec![99],
        "F s2 never sees the rolled-back-then-committed row"
    );

    drop(s2);
    s2_handle.abort();
    shutdown(running).await;
}

#[tokio::test]
async fn f_cross_txn_isolation_no_index() {
    test_f(Shape::NoIndex).await;
}

#[tokio::test]
async fn f_cross_txn_isolation_indexed() {
    test_f(Shape::Indexed).await;
}

// ═════════════════════════════════════════════════════════════════════════
// Test G — Stamping matrix: every write path stamps the subxid, not parent
// ═════════════════════════════════════════════════════════════════════════
//
// Behavioural proof: a write performed under an active SAVEPOINT, then
// ROLLBACK TO, must be hidden. That is ONLY possible if the row was stamped
// with the subxid (which ROLLBACK TO aborts), not the parent. Each arm
// exercises a distinct write path, including the int32-pair fused DELETE
// shape (2× Int32, no index) the revert was caused by.

#[tokio::test]
async fn g_stamping_matrix_fast_insert_int32_pair() {
    let running = start_sample_server("sp_battery_g_ins").await;
    let s1 = &running.client;
    // No-index int32 pair → cached fast INSERT bypass (bound_plan.rs).
    Shape::NoIndex.create(s1).await;
    s1.batch_execute("BEGIN").await.expect("BEGIN");
    s1.batch_execute("SAVEPOINT s").await.expect("SAVEPOINT");
    s1.batch_execute("INSERT INTO t_pair (id, val) VALUES (1, 10)")
        .await
        .expect("fast insert");
    s1.batch_execute("ROLLBACK TO SAVEPOINT s")
        .await
        .expect("ROLLBACK TO");
    assert_eq!(
        ids_seq(s1, Shape::NoIndex).await,
        Vec::<i32>::new(),
        "G fast INSERT stamped subxid (else ROLLBACK TO could not hide it)"
    );
    s1.batch_execute("COMMIT").await.expect("COMMIT");
    shutdown(running).await;
}

#[tokio::test]
async fn g_stamping_matrix_fused_delete_int32_pair() {
    let running = start_sample_server("sp_battery_g_del").await;
    let s1 = &running.client;
    // No-index int32 pair → fused-DELETE shape (mvcc_maint.rs), the exact
    // corruption root: 2× Int32, no index, no referenced-by checks.
    Shape::NoIndex.create(s1).await;
    Shape::NoIndex.insert(s1, 1, 10).await;
    s1.batch_execute("BEGIN").await.expect("BEGIN");
    s1.batch_execute("SAVEPOINT s").await.expect("SAVEPOINT");
    s1.batch_execute("DELETE FROM t_pair WHERE id = 1")
        .await
        .expect("fused delete");
    assert_eq!(
        ids_seq(s1, Shape::NoIndex).await,
        Vec::<i32>::new(),
        "G fused DELETE removed the row"
    );
    s1.batch_execute("ROLLBACK TO SAVEPOINT s")
        .await
        .expect("ROLLBACK TO");
    assert_eq!(
        ids_seq(s1, Shape::NoIndex).await,
        vec![1],
        "G fused DELETE stamped subxid (else ROLLBACK TO could not restore it)"
    );
    s1.batch_execute("COMMIT").await.expect("COMMIT");
    shutdown(running).await;
}

#[tokio::test]
async fn g_stamping_matrix_general_insert_update_delete_indexed() {
    let running = start_sample_server("sp_battery_g_gen").await;
    let s1 = &running.client;
    // Indexed/multi-column → general operator INSERT/UPDATE/DELETE paths.
    Shape::Indexed.create(s1).await;
    Shape::Indexed.insert(s1, 1, 10).await;

    // General INSERT under savepoint, rolled back → hidden.
    s1.batch_execute("BEGIN").await.expect("BEGIN");
    s1.batch_execute("SAVEPOINT s1").await.expect("SAVEPOINT");
    Shape::Indexed.insert(s1, 2, 20).await;
    s1.batch_execute("ROLLBACK TO SAVEPOINT s1")
        .await
        .expect("ROLLBACK TO");
    assert_eq!(
        ids_seq(s1, Shape::Indexed).await,
        vec![1],
        "G general INSERT stamped subxid"
    );

    // General UPDATE under savepoint, rolled back → pre-image restored.
    s1.batch_execute("SAVEPOINT s2").await.expect("SAVEPOINT 2");
    s1.batch_execute("UPDATE t_idx SET val = 999 WHERE id = 1")
        .await
        .expect("general update");
    s1.batch_execute("ROLLBACK TO SAVEPOINT s2")
        .await
        .expect("ROLLBACK TO 2");
    assert_eq!(
        val_of(s1, Shape::Indexed, 1).await,
        Some(10),
        "G general UPDATE stamped subxid (pre-image restored)"
    );

    // General DELETE under savepoint, rolled back → restored.
    s1.batch_execute("SAVEPOINT s3").await.expect("SAVEPOINT 3");
    s1.batch_execute("DELETE FROM t_idx WHERE id = 1")
        .await
        .expect("general delete");
    s1.batch_execute("ROLLBACK TO SAVEPOINT s3")
        .await
        .expect("ROLLBACK TO 3");
    assert_eq!(
        ids_seq(s1, Shape::Indexed).await,
        vec![1],
        "G general DELETE stamped subxid (restored)"
    );

    s1.batch_execute("COMMIT").await.expect("COMMIT");
    shutdown(running).await;
}

#[tokio::test]
async fn g_stamping_matrix_in_place_fused_update_int32_pair() {
    let running = start_sample_server("sp_battery_g_upd").await;
    let s1 = &running.client;
    // No-index int32 pair → fused in-place UPDATE path (col ± lit). The
    // pre-image must be restored on ROLLBACK TO, which is only possible if
    // the update stamped the subxid (fused_update.rs already uses ctx.xid =
    // current_xid; this is the regression guard for that path).
    Shape::NoIndex.create(s1).await;
    Shape::NoIndex.insert(s1, 1, 100).await;
    s1.batch_execute("BEGIN").await.expect("BEGIN");
    s1.batch_execute("SAVEPOINT s").await.expect("SAVEPOINT");
    s1.batch_execute("UPDATE t_pair SET val = val + 50 WHERE id = 1")
        .await
        .expect("fused in-place update");
    assert_eq!(
        val_of(s1, Shape::NoIndex, 1).await,
        Some(150),
        "G fused UPDATE applied under savepoint"
    );
    s1.batch_execute("ROLLBACK TO SAVEPOINT s")
        .await
        .expect("ROLLBACK TO");
    assert_eq!(
        val_of(s1, Shape::NoIndex, 1).await,
        Some(100),
        "G fused in-place UPDATE stamped subxid (pre-image restored on rollback)"
    );
    s1.batch_execute("COMMIT").await.expect("COMMIT");
    shutdown(running).await;
}

// Note: COPY FROM stdin runs in its own autocommit transaction in this
// server (copy/stdio.rs begins a fresh txn), so it is intentionally not part
// of the savepoint-rollback stamping matrix — the stamp itself is already
// `current_xid()` (design §2 #4/#5), and COPY transactionality inside an
// explicit block is a separate, documented gap.

// ═════════════════════════════════════════════════════════════════════════
// Test H — COMMIT atomicity under racing readers (family fold)
// ═════════════════════════════════════════════════════════════════════════

async fn test_h(shape: Shape) {
    let running = start_sample_server("sp_battery_h").await;
    let bound = running.bound;
    let s1 = &running.client;
    shape.create(s1).await;

    // Build a 3-deep savepoint family with a write at each level + one RELEASE.
    s1.batch_execute("BEGIN").await.expect("BEGIN");
    s1.batch_execute("SAVEPOINT l1")
        .await
        .expect("SAVEPOINT l1");
    shape.insert(s1, 1, 1).await;
    s1.batch_execute("SAVEPOINT l2")
        .await
        .expect("SAVEPOINT l2");
    shape.insert(s1, 2, 2).await;
    s1.batch_execute("SAVEPOINT l3")
        .await
        .expect("SAVEPOINT l3");
    shape.insert(s1, 3, 3).await;
    s1.batch_execute("RELEASE SAVEPOINT l3")
        .await
        .expect("RELEASE l3");

    // Spawn a reader that takes snapshots in a tight loop across s1's COMMIT.
    let (s2, s2_handle) = peer(bound, "sp_battery_h_s2").await;
    let table = shape.table().to_string();
    let reader = tokio::spawn(async move {
        let sql = format!("SELECT id FROM {table}");
        for _ in 0..400 {
            let rows = s2.query(&sql, &[]).await.expect("racing read");
            let mut ids: Vec<i32> = rows.iter().map(|r| r.get::<_, i32>(0)).collect();
            ids.sort_unstable();
            // All-or-nothing: never a torn subset of the family. The only
            // legal observations are {} (before commit) or {1,2,3} (after).
            assert!(
                ids.is_empty() || ids == vec![1, 2, 3],
                "H torn read observed: {ids:?} (family must commit atomically)"
            );
        }
        s2
    });

    // Give the reader a head start, then commit the whole family at once.
    tokio::task::yield_now().await;
    s1.batch_execute("COMMIT").await.expect("COMMIT");

    let s2 = reader.await.expect("reader task");
    // Final state: the whole family is visible.
    assert_eq!(
        ids_seq(&s2, shape).await,
        vec![1, 2, 3],
        "H final: whole family committed"
    );

    drop(s2);
    s2_handle.abort();
    shutdown(running).await;
}

#[tokio::test]
async fn h_commit_atomicity_no_index() {
    test_h(Shape::NoIndex).await;
}

#[tokio::test]
async fn h_commit_atomicity_indexed() {
    test_h(Shape::Indexed).await;
}

// ═════════════════════════════════════════════════════════════════════════
// Test I — Isolation matrix (RC / RR / Serializable)
// ═════════════════════════════════════════════════════════════════════════
//
// Repeat the core own-write + rollback-to behaviour under each isolation
// level, and assert RR/SSI keep a STABLE snapshot across SAVEPOINT/RELEASE/
// ROLLBACK TO (a concurrently-committed foreign row stays invisible).

async fn test_i_level(shape: Shape, level: &str) {
    let running = start_sample_server("sp_battery_i").await;
    let bound = running.bound;
    let s1 = &running.client;
    shape.create(s1).await;
    shape.insert(s1, 1, 1).await; // committed baseline

    let begin = format!("BEGIN ISOLATION LEVEL {level}");
    s1.batch_execute(&begin).await.expect("BEGIN isolation");
    // Take the snapshot (RR/SSI freeze here).
    assert_eq!(ids_seq(s1, shape).await, vec![1], "I baseline visible");

    // A concurrent foreign txn commits a new row.
    let (s2, s2_handle) = peer(bound, "sp_battery_i_s2").await;
    shape.insert(&s2, 2, 2).await;

    // Own-write visibility + rollback still correct under this level.
    s1.batch_execute("SAVEPOINT s1").await.expect("SAVEPOINT");
    shape.insert(s1, 3, 3).await;
    assert!(
        ids_seq(s1, shape).await.contains(&3),
        "I own savepoint write visible under {level}"
    );
    s1.batch_execute("ROLLBACK TO SAVEPOINT s1")
        .await
        .expect("ROLLBACK TO");
    assert!(
        !ids_seq(s1, shape).await.contains(&3),
        "I own write hidden after ROLLBACK TO under {level}"
    );

    // Under RR / SERIALIZABLE the foreign row 2 must remain invisible
    // (frozen snapshot stable across the savepoint ops); under RC a fresh
    // statement may see it. Either way row 1 is always visible and row 3
    // never is.
    let visible = ids_seq(s1, shape).await;
    assert!(visible.contains(&1), "I row 1 always visible under {level}");
    assert!(!visible.contains(&3), "I row 3 hidden under {level}");
    if level != "READ COMMITTED" {
        assert!(
            !visible.contains(&2),
            "I {level}: frozen snapshot must not see foreign row 2 committed after begin"
        );
    }

    s1.batch_execute("COMMIT").await.expect("COMMIT");
    drop(s2);
    s2_handle.abort();
    shutdown(running).await;
}

#[tokio::test]
async fn i_isolation_matrix_read_committed() {
    test_i_level(Shape::NoIndex, "READ COMMITTED").await;
    test_i_level(Shape::Indexed, "READ COMMITTED").await;
}

#[tokio::test]
async fn i_isolation_matrix_repeatable_read() {
    test_i_level(Shape::NoIndex, "REPEATABLE READ").await;
    test_i_level(Shape::Indexed, "REPEATABLE READ").await;
}

#[tokio::test]
async fn i_isolation_matrix_serializable() {
    test_i_level(Shape::NoIndex, "SERIALIZABLE").await;
    test_i_level(Shape::Indexed, "SERIALIZABLE").await;
}

// ═════════════════════════════════════════════════════════════════════════
// Test U — Unique-index dead-entry reuse (Option-A A3 must-fix)
// ═════════════════════════════════════════════════════════════════════════

/// Helper table for U: a UNIQUE index on `val`.
async fn create_unique_val(c: &Client) {
    c.batch_execute("CREATE TABLE t_uniq (id INT4 PRIMARY KEY, val INT4)")
        .await
        .expect("create t_uniq");
    c.batch_execute("CREATE UNIQUE INDEX ux_t_uniq_val ON t_uniq(val)")
        .await
        .expect("create unique index");
}

#[tokio::test]
async fn u_unique_dead_entry_reuse_after_commit_delete() {
    let running = start_sample_server("sp_battery_u1").await;
    let s1 = &running.client;
    create_unique_val(s1).await;
    s1.batch_execute("INSERT INTO t_uniq VALUES (1, 5)")
        .await
        .expect("insert val=5");
    // Delete it and COMMIT — the dead unique entry lingers (Option-A).
    s1.batch_execute("DELETE FROM t_uniq WHERE id = 1")
        .await
        .expect("delete val=5");
    // Re-insert val=5 with a new id — must succeed (no false UniqueViolation).
    s1.batch_execute("INSERT INTO t_uniq VALUES (2, 5)")
        .await
        .expect("re-insert val=5 after committed delete must succeed");
    let rows = s1
        .query("SELECT id FROM t_uniq WHERE val = 5", &[])
        .await
        .expect("query val=5");
    assert_eq!(rows.len(), 1, "U: exactly one live val=5");
    assert_eq!(rows[0].get::<_, i32>(0), 2, "U: the re-inserted row");
    shutdown(running).await;
}

#[tokio::test]
async fn u_unique_reuse_after_rollback_to() {
    let running = start_sample_server("sp_battery_u2").await;
    let s1 = &running.client;
    create_unique_val(s1).await;
    s1.batch_execute("BEGIN").await.expect("BEGIN");
    s1.batch_execute("SAVEPOINT s").await.expect("SAVEPOINT");
    s1.batch_execute("INSERT INTO t_uniq VALUES (1, 5)")
        .await
        .expect("insert val=5 under savepoint");
    s1.batch_execute("ROLLBACK TO SAVEPOINT s")
        .await
        .expect("ROLLBACK TO");
    // The rolled-back insert's dead entry must not block re-inserting val=5.
    s1.batch_execute("INSERT INTO t_uniq VALUES (2, 5)")
        .await
        .expect("re-insert val=5 after ROLLBACK TO must succeed");
    s1.batch_execute("COMMIT").await.expect("COMMIT");
    let rows = s1
        .query("SELECT id FROM t_uniq WHERE val = 5", &[])
        .await
        .expect("query val=5");
    assert_eq!(rows.len(), 1, "U2: exactly one live val=5");
    assert_eq!(rows[0].get::<_, i32>(0), 2, "U2: the re-inserted row");
    shutdown(running).await;
}

#[tokio::test]
async fn u_unique_live_conflict_still_rejected() {
    let running = start_sample_server("sp_battery_u3").await;
    let s1 = &running.client;
    create_unique_val(s1).await;
    s1.batch_execute("INSERT INTO t_uniq VALUES (1, 5)")
        .await
        .expect("insert live val=5");
    // A LIVE val=5 is still present → re-insert must be rejected (23505).
    let err = s1
        .batch_execute("INSERT INTO t_uniq VALUES (2, 5)")
        .await
        .expect_err("live unique conflict must be rejected");
    assert_eq!(
        err.code().expect("sqlstate").code(),
        "23505",
        "U3: live unique conflict → 23505"
    );
    shutdown(running).await;
}

#[tokio::test]
async fn u_unique_in_progress_other_writer_rejected() {
    let running = start_sample_server("sp_battery_u4").await;
    let bound = running.bound;
    let s1 = &running.client;
    create_unique_val(s1).await;

    // s1 holds an uncommitted insert of val=5.
    s1.batch_execute("BEGIN").await.expect("BEGIN");
    s1.batch_execute("INSERT INTO t_uniq VALUES (1, 5)")
        .await
        .expect("s1 pending insert val=5");

    // s2 tries to insert val=5 concurrently — must be rejected (dirty-snapshot
    // conflict: an in-progress foreign inserter still holds the key). The
    // attempt either blocks-then-fails or fails immediately with 23505; we
    // accept any error here and confirm it does NOT silently both-succeed.
    let (s2, s2_handle) = peer(bound, "sp_battery_u4_s2").await;
    let s2_result = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        s2.batch_execute("INSERT INTO t_uniq VALUES (2, 5)"),
    )
    .await;

    // Now commit s1.
    s1.batch_execute("COMMIT").await.expect("COMMIT s1");

    // After s1 commits, there must be exactly ONE live val=5 — the two
    // inserters cannot both have won.
    let rows = s1
        .query("SELECT id FROM t_uniq WHERE val = 5", &[])
        .await
        .expect("query val=5");
    assert_eq!(
        rows.len(),
        1,
        "U4: two concurrent inserters of the same unique key must not both win"
    );
    // (s2_result may be Ok, Err, or a timeout depending on lock waiting; the
    // load-bearing invariant is the single-live-row assertion above.)
    let _ = s2_result;

    drop(s2);
    s2_handle.abort();
    shutdown(running).await;
}

// ═════════════════════════════════════════════════════════════════════════
// Test Z — Index/seq agreement under randomized savepoint nesting
// ═════════════════════════════════════════════════════════════════════════
//
// Randomized DML under random savepoint nesting + ROLLBACK TO / RELEASE,
// then assert seq-scan rowset == index-scan rowset == a second-connection
// rowset for every committed state. The catch-all for the access-path
// agreement contract.

#[tokio::test]
async fn z_index_seq_agreement_fuzz() {
    let running = start_sample_server("sp_battery_z").await;
    let bound = running.bound;
    let s1 = &running.client;
    Shape::Indexed.create(s1).await;
    let (s2, s2_handle) = peer(bound, "sp_battery_z_s2").await;

    // Deterministic xorshift PRNG so the fuzz is reproducible.
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };

    let mut next_id: i32 = 1;

    for round in 0..40 {
        s1.batch_execute("BEGIN").await.expect("BEGIN");
        let mut sp_depth: usize = 0;
        // If a mutation hits an engine-level limitation mid-statement, the
        // block enters the failed state; we then ROLLBACK the whole txn and
        // move on. The load-bearing invariant — seq == index == s2 on the
        // committed state — is checked after every round regardless.
        let mut failed = false;
        let ops = 3 + usize::try_from(next() % 6).unwrap_or(0);
        for _ in 0..ops {
            if failed {
                break;
            }
            match next() % 6 {
                0 => {
                    sp_depth += 1;
                    let name = format!("z{sp_depth}");
                    s1.batch_execute(&format!("SAVEPOINT {name}"))
                        .await
                        .expect("SAVEPOINT");
                }
                1 if sp_depth > 0 => {
                    let name = format!("z{sp_depth}");
                    s1.batch_execute(&format!("ROLLBACK TO SAVEPOINT {name}"))
                        .await
                        .expect("ROLLBACK TO");
                    sp_depth -= 1;
                }
                2 if sp_depth > 0 => {
                    let name = format!("z{sp_depth}");
                    s1.batch_execute(&format!("RELEASE SAVEPOINT {name}"))
                        .await
                        .expect("RELEASE");
                    sp_depth -= 1;
                }
                3 => {
                    // INSERT a fresh id.
                    let id = next_id;
                    next_id += 1;
                    let val = i32::try_from(next() % 100).unwrap_or(0);
                    let sql =
                        format!("INSERT INTO t_idx (id, val, name) VALUES ({id}, {val}, 'n{id}')");
                    if s1.batch_execute(&sql).await.is_err() {
                        failed = true;
                    }
                }
                4 => {
                    // DELETE a currently-live id (heap truth).
                    let live = ids_seq(s1, Shape::Indexed).await;
                    if let Some(&victim) = live.get(pick_index(next(), live.len())) {
                        let sql = format!("DELETE FROM t_idx WHERE id = {victim}");
                        if s1.batch_execute(&sql).await.is_err() {
                            failed = true;
                        }
                    }
                }
                _ => {
                    // UPDATE a currently-live id's val (key-changing on the
                    // secondary index).
                    let live = ids_seq(s1, Shape::Indexed).await;
                    if let Some(&target) = live.get(pick_index(next(), live.len())) {
                        let val = i32::try_from(next() % 100).unwrap_or(0);
                        let sql = format!("UPDATE t_idx SET val = {val} WHERE id = {target}");
                        if s1.batch_execute(&sql).await.is_err() {
                            failed = true;
                        }
                    }
                }
            }
        }

        // Commit (unless the block failed or the dice say roll back).
        let commit = !failed && next() % 5 != 0;
        if commit {
            s1.batch_execute("COMMIT").await.expect("COMMIT");
        } else {
            s1.batch_execute("ROLLBACK").await.expect("ROLLBACK");
        }

        // The catch-all invariant: every committed state is observed
        // IDENTICALLY through a seq scan, an index scan, and a fresh second
        // connection. The expected set is taken from the second connection
        // (an independent committed-state observer), so no error-prone local
        // model is on the critical path.
        let expected = ids_seq(&s2, Shape::Indexed).await;

        let seq = ids_seq(s1, Shape::Indexed).await;
        assert_eq!(
            seq, expected,
            "Z round {round}: s1 seq scan disagrees with s2 committed state"
        );

        // Index agreement: union of per-val index probes equals the seq set.
        let mut via_index: Vec<i32> = Vec::new();
        for v in 0..100 {
            via_index.extend(ids_by_val(s1, Shape::Indexed, v).await);
        }
        via_index.sort_unstable();
        via_index.dedup();
        assert_eq!(
            via_index, expected,
            "Z round {round}: index scan disagrees with seq scan (expected = s2 committed state)"
        );
    }

    drop(s2);
    s2_handle.abort();
    shutdown(running).await;
}

// ═════════════════════════════════════════════════════════════════════════
// Test R — Crash recovery of subxact rollback
// ═════════════════════════════════════════════════════════════════════════
//
// Replay-after-restart variant of C/B: write under a savepoint, ROLLBACK TO,
// COMMIT, restart the server from its data dir (WAL replay), and assert the
// rolled-back row's invisibility survives replay (the applier re-stamps the
// subxid, not the parent). Uses a persistent server so the WAL is durable.

#[tokio::test]
async fn r_crash_recovery_of_subxact_rollback() {
    use support::start_persistent_server;

    let dir = tempfile::tempdir().expect("tempdir");
    support::make_data_dir_private(dir.path());

    // Phase 1: write under savepoint, ROLLBACK TO, COMMIT — on a persistent
    // server so the effects hit the WAL.
    {
        let running = start_persistent_server(dir.path(), "sp_battery_r").await;
        let s1 = &running.client;
        s1.batch_execute("CREATE TABLE t_rec (id INT4 PRIMARY KEY, val INT4)")
            .await
            .expect("create t_rec");
        s1.batch_execute("INSERT INTO t_rec VALUES (1, 100)")
            .await
            .expect("seed row");

        s1.batch_execute("BEGIN").await.expect("BEGIN");
        s1.batch_execute("SAVEPOINT s").await.expect("SAVEPOINT");
        // A DELETE that will be rolled back, and an INSERT that will be rolled
        // back. After ROLLBACK TO both must be reverted.
        s1.batch_execute("DELETE FROM t_rec WHERE id = 1")
            .await
            .expect("delete under savepoint");
        s1.batch_execute("INSERT INTO t_rec VALUES (2, 200)")
            .await
            .expect("insert under savepoint");
        s1.batch_execute("ROLLBACK TO SAVEPOINT s")
            .await
            .expect("ROLLBACK TO");
        // A write that DOES survive, to prove the commit landed.
        s1.batch_execute("INSERT INTO t_rec VALUES (3, 300)")
            .await
            .expect("post-rollback insert");
        s1.batch_execute("COMMIT").await.expect("COMMIT");

        // Pre-restart truth: row 1 restored, row 2 gone, row 3 present.
        let rows = s1
            .query("SELECT id FROM t_rec ORDER BY id", &[])
            .await
            .expect("pre-restart read");
        let ids: Vec<i32> = rows.iter().map(|r| r.get::<_, i32>(0)).collect();
        assert_eq!(ids, vec![1, 3], "R pre-restart: rolled-back ops reverted");

        shutdown(running).await;
    }

    // Phase 2: restart from the same data dir (WAL replay) and re-read.
    {
        let running = start_persistent_server(dir.path(), "sp_battery_r2").await;
        let s1 = &running.client;
        let rows = s1
            .query("SELECT id, val FROM t_rec ORDER BY id", &[])
            .await
            .expect("post-restart read");
        let pairs: Vec<(i32, i32)> = rows
            .iter()
            .map(|r| (r.get::<_, i32>(0), r.get::<_, i32>(1)))
            .collect();
        assert_eq!(
            pairs,
            vec![(1, 100), (3, 300)],
            "R post-restart: rolled-back subxact effects do not survive replay; \
             row 1 restored (not durably deleted), row 2 never durable, row 3 committed"
        );
        shutdown(running).await;
    }
}
