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
    let roadmap = fs::read_to_string(root.join("ROADMAP.md")).expect("read roadmap");
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
    for text in [&docs, &roadmap, &limitations] {
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
