//! Isolation-suite coverage derived from public ACID/Hermitage scenarios.

pub mod support;

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use support::{shutdown, start_sample_server};
use tokio_postgres::{Client, NoTls};

#[test]
fn isolation_suite_keeps_public_provenance_and_ssi_honesty() {
    let root = repo_root();
    let isolation_root = root.join("tests/isolation");
    let acid = fs::read_to_string(isolation_root.join("acid.sql")).expect("read acid.sql");
    let manifest =
        fs::read_to_string(isolation_root.join("IMPORT_MANIFEST.txt")).expect("read manifest");
    let notice =
        fs::read_to_string(isolation_root.join("NOTICE.hermitage.md")).expect("read notice");
    let docs = fs::read_to_string(root.join("docs/testing/isolation-suite.md"))
        .expect("read isolation docs");
    let todo = fs::read_to_string(root.join("TODO.md")).expect("read todo");
    let limitations =
        fs::read_to_string(root.join("docs/known-limitations.md")).expect("read limitations");

    assert!(
        acid.contains("UltraSQL-authored ACID transfer baseline"),
        "acid.sql must be authored or explicitly licensed"
    );
    assert!(
        manifest.contains("hermitage_commit=f029bec8e32af6a9506508638fdf74ef61286225"),
        "manifest:\n{manifest}"
    );
    assert!(
        manifest.contains("license=CC-BY-4.0"),
        "manifest:\n{manifest}"
    );
    assert!(
        manifest.contains("scenario=Hermitage PostgreSQL G1a dirty read"),
        "manifest:\n{manifest}"
    );
    assert!(
        manifest.contains("scenario=Hermitage PostgreSQL PMP repeatable-read phantom"),
        "manifest:\n{manifest}"
    );
    assert!(
        manifest.contains("scenario=Hermitage PostgreSQL G2 serializable write skew"),
        "manifest:\n{manifest}"
    );
    assert!(
        notice.contains("Martin Kleppmann") && notice.contains("CC BY 4.0"),
        "notice:\n{notice}"
    );
    for text in [&docs, &todo, &limitations] {
        let normalized = normalize_ws(text);
        assert!(
            normalized.contains("column-range SSI"),
            "SSI docs must state supported column-range granularity honestly:\n{text}"
        );
        assert!(
            normalized.contains("not fully predicate-precise"),
            "SSI docs must not overclaim full predicate precision:\n{text}"
        );
    }
}

#[tokio::test]
async fn acid_sql_transfer_invariant_survives_commit_and_rollback() {
    let running = start_sample_server("isolation_acid_sql").await;
    let acid_sql =
        fs::read_to_string(repo_root().join("tests/isolation/acid.sql")).expect("read acid.sql");

    run_sql_script(&running.client, &acid_sql).await;

    assert_eq!(
        scalar_i64(
            &running.client,
            "SELECT COUNT(*) FROM isolation_acid_accounts \
             WHERE (id = 1 AND balance = 50) OR (id = 2 AND balance = -50)",
        )
        .await,
        2
    );

    running
        .client
        .batch_execute("DROP TABLE isolation_acid_accounts")
        .await
        .expect("drop acid table");
    shutdown(running).await;
}

#[tokio::test]
async fn hermitage_g1a_read_committed_prevents_dirty_read_wire() {
    let running = start_sample_server("hermitage_g1a").await;
    let (peer, peer_handle) = connect_peer(running.bound, "hermitage_g1a_peer").await;

    setup_hermitage_table(&running.client).await;

    running
        .client
        .batch_execute("BEGIN ISOLATION LEVEL READ COMMITTED")
        .await
        .expect("begin writer");
    peer.batch_execute("BEGIN ISOLATION LEVEL READ COMMITTED")
        .await
        .expect("begin reader");

    running
        .client
        .batch_execute("UPDATE isolation_hermitage SET value = 101 WHERE id = 1")
        .await
        .expect("writer update");
    assert_eq!(
        scalar_i32(&peer, "SELECT value FROM isolation_hermitage WHERE id = 1",).await,
        10
    );

    running
        .client
        .batch_execute("ROLLBACK")
        .await
        .expect("writer rollback");
    assert_eq!(
        scalar_i32(&peer, "SELECT value FROM isolation_hermitage WHERE id = 1",).await,
        10
    );

    peer.batch_execute("COMMIT").await.expect("reader commit");
    close_peer(peer, peer_handle).await;
    shutdown(running).await;
}

#[tokio::test]
async fn hermitage_pmp_repeatable_read_prevents_phantom_wire() {
    let running = start_sample_server("hermitage_pmp").await;
    let (peer, peer_handle) = connect_peer(running.bound, "hermitage_pmp_peer").await;

    setup_hermitage_table(&running.client).await;

    running
        .client
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("begin reader");
    peer.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("begin writer");

    assert_eq!(
        scalar_i64(
            &running.client,
            "SELECT COUNT(*) FROM isolation_hermitage WHERE value = 30",
        )
        .await,
        0
    );
    peer.batch_execute("INSERT INTO isolation_hermitage VALUES (3, 30)")
        .await
        .expect("phantom insert");
    peer.batch_execute("COMMIT").await.expect("writer commit");

    assert_eq!(
        scalar_i64(
            &running.client,
            "SELECT COUNT(*) FROM isolation_hermitage WHERE value = 30",
        )
        .await,
        0
    );

    running
        .client
        .batch_execute("COMMIT")
        .await
        .expect("reader commit");
    close_peer(peer, peer_handle).await;
    shutdown(running).await;
}

#[tokio::test]
async fn hermitage_g2_serializable_write_skew_aborts_one_wire() {
    let running = start_sample_server("hermitage_g2").await;
    let (peer, peer_handle) = connect_peer(running.bound, "hermitage_g2_peer").await;

    running
        .client
        .batch_execute("CREATE TABLE isolation_shift (id INT NOT NULL, on_call INT)")
        .await
        .expect("create shift table");
    running
        .client
        .batch_execute("INSERT INTO isolation_shift VALUES (1, 1), (2, 1)")
        .await
        .expect("seed shift table");

    running
        .client
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin a");
    peer.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin b");

    assert_eq!(
        scalar_i64(
            &running.client,
            "SELECT COUNT(*) FROM isolation_shift WHERE on_call = 1",
        )
        .await,
        2
    );
    assert_eq!(
        scalar_i64(
            &peer,
            "SELECT COUNT(*) FROM isolation_shift WHERE on_call = 1",
        )
        .await,
        2
    );

    running
        .client
        .batch_execute("UPDATE isolation_shift SET on_call = 0 WHERE id = 1")
        .await
        .expect("update a");
    peer.batch_execute("UPDATE isolation_shift SET on_call = 0 WHERE id = 2")
        .await
        .expect("update b");

    let a_commit = running.client.batch_execute("COMMIT").await;
    let b_commit = peer.batch_execute("COMMIT").await;
    assert_eq!(
        [&a_commit, &b_commit]
            .iter()
            .filter(|result| result.is_ok())
            .count(),
        1,
        "one serializable tx must commit: a={a_commit:?}, b={b_commit:?}"
    );
    assert_eq!(
        [&a_commit, &b_commit]
            .iter()
            .filter(|result| is_serialization_failure(result))
            .count(),
        1,
        "one serializable tx must fail with 40001: a={a_commit:?}, b={b_commit:?}"
    );

    close_peer(peer, peer_handle).await;
    shutdown(running).await;
}

#[tokio::test]
async fn serializable_indexed_disjoint_row_updates_both_commit_wire() {
    let running = start_sample_server("serializable_disjoint_indexed_rows").await;
    let (peer, peer_handle) =
        connect_peer(running.bound, "serializable_disjoint_indexed_rows_peer").await;

    running
        .client
        .batch_execute(
            "CREATE TABLE serializable_disjoint (id INT NOT NULL, value INT NOT NULL);\
             INSERT INTO serializable_disjoint VALUES (1, 10), (2, 20);\
             CREATE INDEX serializable_disjoint_id_idx ON serializable_disjoint (id)",
        )
        .await
        .expect("seed indexed serializable table");

    running
        .client
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin a");
    peer.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin b");

    assert_eq!(
        scalar_i32(
            &running.client,
            "SELECT value FROM serializable_disjoint WHERE id = 1",
        )
        .await,
        10
    );
    assert_eq!(
        scalar_i32(
            &peer,
            "SELECT value FROM serializable_disjoint WHERE id = 2",
        )
        .await,
        20
    );

    running
        .client
        .batch_execute("UPDATE serializable_disjoint SET value = value + 1 WHERE id = 1")
        .await
        .expect("update a");
    peer.batch_execute("UPDATE serializable_disjoint SET value = value + 1 WHERE id = 2")
        .await
        .expect("update b");

    let a_commit = running.client.batch_execute("COMMIT").await;
    let b_commit = peer.batch_execute("COMMIT").await;

    assert!(
        a_commit.is_ok() && b_commit.is_ok(),
        "disjoint indexed serializable updates should both commit: a={a_commit:?}, b={b_commit:?}"
    );

    assert_eq!(
        scalar_i64(
            &running.client,
            "SELECT SUM(value) FROM serializable_disjoint",
        )
        .await,
        32
    );

    close_peer(peer, peer_handle).await;
    shutdown(running).await;
}

#[tokio::test]
async fn serializable_disjoint_in_list_ranges_both_commit_wire() {
    let running = start_sample_server("serializable_disjoint_in_ranges").await;
    let (peer, peer_handle) =
        connect_peer(running.bound, "serializable_disjoint_in_ranges_peer").await;

    running
        .client
        .batch_execute(
            "CREATE TABLE serializable_disjoint_in (id INT NOT NULL, value INT NOT NULL);\
             INSERT INTO serializable_disjoint_in VALUES (1, 10), (2, 20), (3, 30), (4, 40);\
             CREATE INDEX serializable_disjoint_in_id_idx ON serializable_disjoint_in (id)",
        )
        .await
        .expect("seed indexed serializable IN-list table");

    running
        .client
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin a");
    peer.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin b");

    assert_eq!(
        scalar_i64(
            &running.client,
            "SELECT SUM(value) FROM serializable_disjoint_in WHERE id IN (1, 2)",
        )
        .await,
        30
    );
    assert_eq!(
        scalar_i64(
            &peer,
            "SELECT SUM(value) FROM serializable_disjoint_in WHERE id IN (3, 4)",
        )
        .await,
        70
    );

    running
        .client
        .batch_execute("UPDATE serializable_disjoint_in SET value = value + 1 WHERE id = 1")
        .await
        .expect("update a");
    peer.batch_execute("UPDATE serializable_disjoint_in SET value = value + 1 WHERE id = 3")
        .await
        .expect("update b");

    let a_commit = running.client.batch_execute("COMMIT").await;
    let b_commit = peer.batch_execute("COMMIT").await;

    assert!(
        a_commit.is_ok() && b_commit.is_ok(),
        "disjoint serializable IN-list ranges should both commit: a={a_commit:?}, b={b_commit:?}"
    );

    assert_eq!(
        scalar_i64(
            &running.client,
            "SELECT SUM(value) FROM serializable_disjoint_in",
        )
        .await,
        102
    );

    close_peer(peer, peer_handle).await;
    shutdown(running).await;
}

#[tokio::test]
async fn serializable_disjoint_supported_and_unsupported_conjuncts_both_commit_wire() {
    let running = start_sample_server("serializable_partial_and_ranges").await;
    let (peer, peer_handle) =
        connect_peer(running.bound, "serializable_partial_and_ranges_peer").await;

    running
        .client
        .batch_execute(
            "CREATE TABLE serializable_partial_and (id INT NOT NULL, value INT NOT NULL);\
             INSERT INTO serializable_partial_and VALUES (1, 10), (2, 20);\
             CREATE INDEX serializable_partial_and_id_idx ON serializable_partial_and (id)",
        )
        .await
        .expect("seed partial AND serializable table");

    running
        .client
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin a");
    peer.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin b");

    assert_eq!(
        scalar_i32(
            &running.client,
            "SELECT value FROM serializable_partial_and WHERE id = 1 AND value + 0 = 10",
        )
        .await,
        10
    );
    assert_eq!(
        scalar_i32(
            &peer,
            "SELECT value FROM serializable_partial_and WHERE id = 2 AND value + 0 = 20",
        )
        .await,
        20
    );

    running
        .client
        .batch_execute("UPDATE serializable_partial_and SET value = value + 1 WHERE id = 1")
        .await
        .expect("update a");
    peer.batch_execute("UPDATE serializable_partial_and SET value = value + 1 WHERE id = 2")
        .await
        .expect("update b");

    let a_commit = running.client.batch_execute("COMMIT").await;
    let b_commit = peer.batch_execute("COMMIT").await;

    assert!(
        a_commit.is_ok() && b_commit.is_ok(),
        "disjoint serializable AND predicates should both commit: a={a_commit:?}, b={b_commit:?}"
    );

    assert_eq!(
        scalar_i64(
            &running.client,
            "SELECT SUM(value) FROM serializable_partial_and",
        )
        .await,
        32
    );

    close_peer(peer, peer_handle).await;
    shutdown(running).await;
}

#[tokio::test]
async fn serializable_empty_strict_range_does_not_false_abort_wire() {
    let running = start_sample_server("serializable_empty_strict_range").await;
    let (peer, peer_handle) =
        connect_peer(running.bound, "serializable_empty_strict_range_peer").await;

    running
        .client
        .batch_execute(
            "CREATE TABLE serializable_empty_range (id BIGINT NOT NULL, value INT NOT NULL);\
             INSERT INTO serializable_empty_range VALUES (1, 10), (2, 20);\
             CREATE INDEX serializable_empty_range_id_idx ON serializable_empty_range (id)",
        )
        .await
        .expect("seed indexed serializable empty-range table");

    running
        .client
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin a");
    peer.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin b");

    assert_eq!(
        scalar_i64(
            &running.client,
            "SELECT COUNT(*) FROM serializable_empty_range WHERE id > 9223372036854775807",
        )
        .await,
        0
    );
    assert_eq!(
        scalar_i64(
            &peer,
            "SELECT COUNT(*) FROM serializable_empty_range WHERE id >= 1",
        )
        .await,
        2
    );

    running
        .client
        .batch_execute("UPDATE serializable_empty_range SET value = value + 1 WHERE id = 2")
        .await
        .expect("update a");
    peer.batch_execute("UPDATE serializable_empty_range SET value = value + 1 WHERE id = 1")
        .await
        .expect("update b");

    let a_commit = running.client.batch_execute("COMMIT").await;
    let b_commit = peer.batch_execute("COMMIT").await;

    assert!(
        a_commit.is_ok() && b_commit.is_ok(),
        "empty strict serializable range must not create a false abort: a={a_commit:?}, b={b_commit:?}"
    );

    assert_eq!(
        scalar_i64(
            &running.client,
            "SELECT SUM(value) FROM serializable_empty_range"
        )
        .await,
        32
    );

    close_peer(peer, peer_handle).await;
    shutdown(running).await;
}

/// DATE-keyed write-skew (missed-conflict direction): two serializable
/// transactions each read a date the other writes into. With the tighter
/// `Date` ColumnRange lock the dangerous structure MUST still be caught —
/// exactly one transaction commits and the other fails with 40001. A
/// regression to "both commit" would mean the tightened lock silently
/// misses a real conflict (unsound).
#[tokio::test]
async fn serializable_date_write_skew_aborts_one_wire() {
    let running = start_sample_server("serializable_date_write_skew").await;
    let (peer, peer_handle) =
        connect_peer(running.bound, "serializable_date_write_skew_peer").await;

    running
        .client
        .batch_execute(
            "CREATE TABLE serializable_date_skew (d DATE NOT NULL, v INT NOT NULL);\
             INSERT INTO serializable_date_skew \
                VALUES (DATE '2024-01-01', 1), (DATE '2024-01-02', 1)",
        )
        .await
        .expect("seed date write-skew table");

    running
        .client
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin a");
    peer.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin b");

    // Each transaction reads the date point the *other* will write into.
    assert_eq!(
        scalar_i64(
            &running.client,
            "SELECT COUNT(*) FROM serializable_date_skew WHERE d = DATE '2024-01-01'",
        )
        .await,
        1
    );
    assert_eq!(
        scalar_i64(
            &peer,
            "SELECT COUNT(*) FROM serializable_date_skew WHERE d = DATE '2024-01-02'",
        )
        .await,
        1
    );

    // Cross writes: A writes the row B read; B writes the row A read.
    running
        .client
        .batch_execute("UPDATE serializable_date_skew SET v = v + 1 WHERE d = DATE '2024-01-02'")
        .await
        .expect("update a");
    peer.batch_execute("UPDATE serializable_date_skew SET v = v + 1 WHERE d = DATE '2024-01-01'")
        .await
        .expect("update b");

    let a_commit = running.client.batch_execute("COMMIT").await;
    let b_commit = peer.batch_execute("COMMIT").await;

    assert_eq!(
        [&a_commit, &b_commit]
            .iter()
            .filter(|result| result.is_ok())
            .count(),
        1,
        "one date-skew serializable tx must commit: a={a_commit:?}, b={b_commit:?}"
    );
    assert_eq!(
        [&a_commit, &b_commit]
            .iter()
            .filter(|result| is_serialization_failure(result))
            .count(),
        1,
        "one date-skew serializable tx must fail with 40001: a={a_commit:?}, b={b_commit:?}"
    );

    close_peer(peer, peer_handle).await;
    shutdown(running).await;
}

/// DATE-keyed disjoint ranges (the precision benefit): two serializable
/// transactions read and write strictly disjoint date ranges. Before
/// `Date` joined the range-lock allowlist these degraded to a
/// relation-wide lock and spuriously aborted; with the tighter
/// per-column-range lock they no longer overlap, so BOTH commit. A
/// `Time`-keyed disjoint pair is checked in the same transaction to cover
/// the microsecond domain.
#[tokio::test]
async fn serializable_date_time_disjoint_ranges_both_commit_wire() {
    let running = start_sample_server("serializable_date_time_disjoint").await;
    let (peer, peer_handle) =
        connect_peer(running.bound, "serializable_date_time_disjoint_peer").await;

    running
        .client
        .batch_execute(
            "CREATE TABLE serializable_dt_disjoint (d DATE NOT NULL, t TIME NOT NULL, v INT NOT NULL);\
             INSERT INTO serializable_dt_disjoint VALUES \
                (DATE '2024-01-01', TIME '01:00:00', 10), \
                (DATE '2024-06-01', TIME '13:00:00', 20)",
        )
        .await
        .expect("seed date/time disjoint table");

    running
        .client
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin a");
    peer.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin b");

    // Disjoint date ranges: A reads the January half, B reads the June half.
    assert_eq!(
        scalar_i32(
            &running.client,
            "SELECT v FROM serializable_dt_disjoint WHERE d < DATE '2024-03-01'",
        )
        .await,
        10
    );
    assert_eq!(
        scalar_i32(
            &peer,
            "SELECT v FROM serializable_dt_disjoint WHERE d >= DATE '2024-03-01'",
        )
        .await,
        20
    );

    // Each writes only inside the date range it read (disjoint from the peer).
    running
        .client
        .batch_execute("UPDATE serializable_dt_disjoint SET v = v + 1 WHERE d < DATE '2024-03-01'")
        .await
        .expect("update a");
    peer.batch_execute(
        "UPDATE serializable_dt_disjoint SET v = v + 1 WHERE d >= DATE '2024-03-01'",
    )
    .await
    .expect("update b");

    let a_commit = running.client.batch_execute("COMMIT").await;
    let b_commit = peer.batch_execute("COMMIT").await;

    assert!(
        a_commit.is_ok() && b_commit.is_ok(),
        "disjoint date-range serializable updates should both commit: a={a_commit:?}, b={b_commit:?}"
    );
    assert_eq!(
        scalar_i64(
            &running.client,
            "SELECT SUM(v) FROM serializable_dt_disjoint"
        )
        .await,
        32,
        "both disjoint date-range updates must have applied"
    );

    close_peer(peer, peer_handle).await;
    shutdown(running).await;
}

/// CROSS-TYPE write-skew (the gate's missed-conflict repro — the
/// load-bearing test). A Date column is READ with a `TIMESTAMP` literal
/// (microseconds) and the *crossed* WRITE keys it with a `DATE` literal
/// (days). The binder allows the temporal-vs-temporal comparison without
/// coercing the literal (`comparable` in expr_type.rs), so the read lock
/// and the crossed write lock land on the same Date column but in
/// different i64 unit-classes.
///
/// Genuine write-skew structure: A reads row 2000-01-02 and writes row
/// 2000-01-01; B reads row 2000-01-01 and writes row 2000-01-02. Each
/// transaction writes the row the *other* read, so a serializable schedule
/// must abort one.
///
/// Pre-fix (commit 31dc3a70) the Date column took a tight *micro-space*
/// read lock (`[86_400_000_000, …]`) and a tight *day-space* write lock
/// (`[0,0]` / `[1,1]`); those ranges never overlap, so the real
/// rw-conflict was MISSED and BOTH transactions committed — a
/// non-serializable schedule (silent corruption). Post-fix the cross-class
/// read falls back to a relation-wide lock, the conflict is caught, and
/// exactly one transaction aborts with SQLSTATE 40001.
#[tokio::test]
async fn serializable_date_col_vs_timestamp_literal_write_skew_aborts_one_wire() {
    let running = start_sample_server("serializable_date_cross_ts").await;
    let (peer, peer_handle) = connect_peer(running.bound, "serializable_date_cross_ts_peer").await;

    running
        .client
        .batch_execute(
            "CREATE TABLE serializable_date_cross (d DATE NOT NULL, v INT NOT NULL);\
             INSERT INTO serializable_date_cross \
                VALUES (DATE '2000-01-01', 10), (DATE '2000-01-02', 20)",
        )
        .await
        .expect("seed cross-type date table");

    running
        .client
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin a");
    peer.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin b");

    // READ a date point keyed with a TIMESTAMP literal (microseconds):
    // A reads 2000-01-02, B reads 2000-01-01.
    assert_eq!(
        scalar_i64(
            &running.client,
            "SELECT COUNT(*) FROM serializable_date_cross \
             WHERE d = TIMESTAMP '2000-01-02 00:00:00'",
        )
        .await,
        1
    );
    assert_eq!(
        scalar_i64(
            &peer,
            "SELECT COUNT(*) FROM serializable_date_cross \
             WHERE d = TIMESTAMP '2000-01-01 00:00:00'",
        )
        .await,
        1
    );

    // CROSS WRITES keyed with a DATE literal (days) into the row the *peer*
    // read: A writes 2000-01-01 (B read it); B writes 2000-01-02 (A read it).
    running
        .client
        .batch_execute("UPDATE serializable_date_cross SET v = v + 1 WHERE d = DATE '2000-01-01'")
        .await
        .expect("update a");
    peer.batch_execute("UPDATE serializable_date_cross SET v = v + 1 WHERE d = DATE '2000-01-02'")
        .await
        .expect("update b");

    let a_commit = running.client.batch_execute("COMMIT").await;
    let b_commit = peer.batch_execute("COMMIT").await;

    assert_eq!(
        [&a_commit, &b_commit]
            .iter()
            .filter(|result| result.is_ok())
            .count(),
        1,
        "exactly one cross-type (Date-col/TIMESTAMP-lit read, DATE-lit write) \
         serializable tx must commit: a={a_commit:?}, b={b_commit:?}"
    );
    assert_eq!(
        [&a_commit, &b_commit]
            .iter()
            .filter(|result| is_serialization_failure(result))
            .count(),
        1,
        "exactly one cross-type serializable tx must fail with 40001 \
         (the missed-conflict must be caught): a={a_commit:?}, b={b_commit:?}"
    );

    close_peer(peer, peer_handle).await;
    shutdown(running).await;
}

/// REVERSE cross-type write-skew: a `Timestamp` column (microseconds) is
/// READ with a `DATE` literal (days) and the crossed WRITE keys it with a
/// `TIMESTAMP` literal — the mirror of the gate's repro. Same genuine
/// write-skew structure (each writes the row the other read). The same
/// cross-unit-class hazard applies, so exactly one transaction must abort
/// with 40001.
#[tokio::test]
async fn serializable_timestamp_col_vs_date_literal_write_skew_aborts_one_wire() {
    let running = start_sample_server("serializable_ts_cross_date").await;
    let (peer, peer_handle) = connect_peer(running.bound, "serializable_ts_cross_date_peer").await;

    running
        .client
        .batch_execute(
            "CREATE TABLE serializable_ts_cross (ts TIMESTAMP NOT NULL, v INT NOT NULL);\
             INSERT INTO serializable_ts_cross \
                VALUES (TIMESTAMP '2000-01-01 00:00:00', 10), \
                       (TIMESTAMP '2000-01-02 00:00:00', 20)",
        )
        .await
        .expect("seed reverse cross-type table");

    running
        .client
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin a");
    peer.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin b");

    // READ keyed with a DATE literal against the TIMESTAMP column:
    // A reads 2000-01-02, B reads 2000-01-01.
    assert_eq!(
        scalar_i64(
            &running.client,
            "SELECT COUNT(*) FROM serializable_ts_cross WHERE ts = DATE '2000-01-02'",
        )
        .await,
        1
    );
    assert_eq!(
        scalar_i64(
            &peer,
            "SELECT COUNT(*) FROM serializable_ts_cross WHERE ts = DATE '2000-01-01'",
        )
        .await,
        1
    );

    // CROSS WRITES keyed with a TIMESTAMP literal into the row the peer read:
    // A writes 2000-01-01 (B read it); B writes 2000-01-02 (A read it).
    running
        .client
        .batch_execute(
            "UPDATE serializable_ts_cross SET v = v + 1 \
             WHERE ts = TIMESTAMP '2000-01-01 00:00:00'",
        )
        .await
        .expect("update a");
    peer.batch_execute(
        "UPDATE serializable_ts_cross SET v = v + 1 \
         WHERE ts = TIMESTAMP '2000-01-02 00:00:00'",
    )
    .await
    .expect("update b");

    let a_commit = running.client.batch_execute("COMMIT").await;
    let b_commit = peer.batch_execute("COMMIT").await;

    assert_eq!(
        [&a_commit, &b_commit]
            .iter()
            .filter(|result| result.is_ok())
            .count(),
        1,
        "exactly one reverse cross-type serializable tx must commit: \
         a={a_commit:?}, b={b_commit:?}"
    );
    assert_eq!(
        [&a_commit, &b_commit]
            .iter()
            .filter(|result| is_serialization_failure(result))
            .count(),
        1,
        "exactly one reverse cross-type serializable tx must fail with 40001: \
         a={a_commit:?}, b={b_commit:?}"
    );

    close_peer(peer, peer_handle).await;
    shutdown(running).await;
}

/// NO-REGRESSION (TS cross-compat preserved): a `Timestamp` column READ
/// with a `TIMESTAMPTZ` literal and the crossed WRITE keyed with a
/// `TIMESTAMP` literal is a *same-unit-class* pair (both micros since
/// epoch, interchangeable). The tight column-range lock is retained, so a
/// genuine write-skew (A writes the row B read; B writes the row A read)
/// is still caught — exactly one transaction aborts with 40001. Proves the
/// unit-class guard did NOT regress the sound Timestamp ↔ TimestampTz
/// cross-compat.
#[tokio::test]
async fn serializable_timestamp_col_vs_timestamptz_literal_write_skew_aborts_one_wire() {
    let running = start_sample_server("serializable_ts_tstz").await;
    let (peer, peer_handle) = connect_peer(running.bound, "serializable_ts_tstz_peer").await;

    running
        .client
        .batch_execute(
            "CREATE TABLE serializable_ts_tstz (ts TIMESTAMP NOT NULL, v INT NOT NULL);\
             INSERT INTO serializable_ts_tstz \
                VALUES (TIMESTAMP '2000-01-01 00:00:00', 10), \
                       (TIMESTAMP '2000-01-02 00:00:00', 20)",
        )
        .await
        .expect("seed ts/tstz table");

    running
        .client
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin a");
    peer.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin b");

    // READ keyed with a TIMESTAMPTZ literal against the TIMESTAMP column
    // (same unit-class — micros since epoch): A reads 2000-01-02, B reads
    // 2000-01-01.
    assert_eq!(
        scalar_i64(
            &running.client,
            "SELECT COUNT(*) FROM serializable_ts_tstz \
             WHERE ts = TIMESTAMPTZ '2000-01-02 00:00:00+00'",
        )
        .await,
        1
    );
    assert_eq!(
        scalar_i64(
            &peer,
            "SELECT COUNT(*) FROM serializable_ts_tstz \
             WHERE ts = TIMESTAMPTZ '2000-01-01 00:00:00+00'",
        )
        .await,
        1
    );

    // CROSS WRITES keyed with a TIMESTAMP literal into the row the peer read:
    // A writes 2000-01-01 (B read it); B writes 2000-01-02 (A read it).
    running
        .client
        .batch_execute(
            "UPDATE serializable_ts_tstz SET v = v + 1 \
             WHERE ts = TIMESTAMP '2000-01-01 00:00:00'",
        )
        .await
        .expect("update a");
    peer.batch_execute(
        "UPDATE serializable_ts_tstz SET v = v + 1 \
         WHERE ts = TIMESTAMP '2000-01-02 00:00:00'",
    )
    .await
    .expect("update b");

    let a_commit = running.client.batch_execute("COMMIT").await;
    let b_commit = peer.batch_execute("COMMIT").await;

    assert_eq!(
        [&a_commit, &b_commit]
            .iter()
            .filter(|result| result.is_ok())
            .count(),
        1,
        "exactly one Timestamp/TimestampTz serializable tx must commit \
         (TS cross-compat tight lock preserved): a={a_commit:?}, b={b_commit:?}"
    );
    assert_eq!(
        [&a_commit, &b_commit]
            .iter()
            .filter(|result| is_serialization_failure(result))
            .count(),
        1,
        "exactly one Timestamp/TimestampTz serializable tx must fail with 40001: \
         a={a_commit:?}, b={b_commit:?}"
    );

    close_peer(peer, peer_handle).await;
    shutdown(running).await;
}

/// NO-REGRESSION (same-class TIME tight lock): a genuine `Time`/`Time`
/// write-skew (each transaction writes the row-of-time the other read)
/// must still abort one transaction with 40001. The `Time` ↔ `Timestamp`
/// cross-class hazard is exercised at the unit level
/// (`serializable_read_lock_time_col_vs_timestamp_literal_falls_back_to_relation`
/// in serializable.rs) because a `Time`-vs-`Timestamp` comparison is not
/// evaluable at execution; here we pin that same-class Time locks still
/// catch real conflicts.
#[tokio::test]
async fn serializable_time_same_class_write_skew_aborts_one_wire() {
    let running = start_sample_server("serializable_time_same").await;
    let (peer, peer_handle) = connect_peer(running.bound, "serializable_time_same_peer").await;

    running
        .client
        .batch_execute(
            "CREATE TABLE serializable_time_same (t TIME NOT NULL, v INT NOT NULL);\
             INSERT INTO serializable_time_same \
                VALUES (TIME '01:00:00', 10), (TIME '02:00:00', 20)",
        )
        .await
        .expect("seed time same-class table");

    running
        .client
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin a");
    peer.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin b");

    // READ keyed with a TIME literal: A reads 02:00, B reads 01:00.
    assert_eq!(
        scalar_i64(
            &running.client,
            "SELECT COUNT(*) FROM serializable_time_same WHERE t = TIME '02:00:00'",
        )
        .await,
        1
    );
    assert_eq!(
        scalar_i64(
            &peer,
            "SELECT COUNT(*) FROM serializable_time_same WHERE t = TIME '01:00:00'",
        )
        .await,
        1
    );

    // CROSS WRITES into the row the peer read: A writes 01:00 (B read it);
    // B writes 02:00 (A read it).
    running
        .client
        .batch_execute("UPDATE serializable_time_same SET v = v + 1 WHERE t = TIME '01:00:00'")
        .await
        .expect("update a");
    peer.batch_execute("UPDATE serializable_time_same SET v = v + 1 WHERE t = TIME '02:00:00'")
        .await
        .expect("update b");

    let a_commit = running.client.batch_execute("COMMIT").await;
    let b_commit = peer.batch_execute("COMMIT").await;

    assert_eq!(
        [&a_commit, &b_commit]
            .iter()
            .filter(|result| result.is_ok())
            .count(),
        1,
        "exactly one same-class Time write-skew tx must commit: \
         a={a_commit:?}, b={b_commit:?}"
    );
    assert_eq!(
        [&a_commit, &b_commit]
            .iter()
            .filter(|result| is_serialization_failure(result))
            .count(),
        1,
        "exactly one same-class Time write-skew tx must fail with 40001: \
         a={a_commit:?}, b={b_commit:?}"
    );

    close_peer(peer, peer_handle).await;
    shutdown(running).await;
}

async fn setup_hermitage_table(client: &Client) {
    client
        .batch_execute("CREATE TABLE isolation_hermitage (id INT NOT NULL, value INT)")
        .await
        .expect("create hermitage table");
    client
        .batch_execute("INSERT INTO isolation_hermitage VALUES (1, 10), (2, 20)")
        .await
        .expect("seed hermitage table");
}

async fn run_sql_script(client: &Client, script: &str) {
    for statement in script
        .split(';')
        .map(str::trim)
        .filter(|sql| !sql.is_empty())
    {
        client
            .batch_execute(&format!("{statement};"))
            .await
            .unwrap_or_else(|err| panic!("run SQL script statement `{statement}`: {err}"));
    }
}

async fn scalar_i64(client: &Client, sql: &str) -> i64 {
    client
        .query_one(sql, &[])
        .await
        .expect("query scalar i64")
        .get::<_, i64>(0)
}

async fn scalar_i32(client: &Client, sql: &str) -> i32 {
    client
        .query_one(sql, &[])
        .await
        .expect("query scalar i32")
        .get::<_, i32>(0)
}

async fn connect_peer(
    bound: SocketAddr,
    application_name: &str,
) -> (Client, tokio::task::JoinHandle<()>) {
    let conn_str = format!(
        "host={host} port={port} user=tester application_name={application_name}",
        host = bound.ip(),
        port = bound.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect peer");
    let handle = tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("peer connection error: {err}");
        }
    });
    (client, handle)
}

async fn close_peer(client: Client, handle: tokio::task::JoinHandle<()>) {
    drop(client);
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("peer connection task exits")
        .expect("peer connection task joins");
}

fn is_serialization_failure<T>(result: &Result<T, tokio_postgres::Error>) -> bool {
    result
        .as_ref()
        .err()
        .and_then(tokio_postgres::Error::code)
        .is_some_and(|code| code.code() == "40001")
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn normalize_ws(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}
