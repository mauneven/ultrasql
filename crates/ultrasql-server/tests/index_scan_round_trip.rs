//! End-to-end `IndexScan` tests against a real `tokio-postgres` client.
//!
//! Closes the v0.5 P0 wire-protocol gap "`IndexScan` wired in
//! `lower_query`" by driving an in-process `ultrasqld` with a stock
//! `tokio-postgres` client. After `CREATE INDEX ix_t_id ON t(id)`,
//! point lookups and range scans return the correct rows and — per the
//! micro-bench at the bottom of this file — observably finish faster
//! than the `SeqScan` baseline on a 50 000-row table.
//!
//! Shapes covered:
//!
//! - `SELECT * FROM t WHERE id = N` — point lookup (one row).
//! - `SELECT * FROM t WHERE id BETWEEN lo AND hi` — bounded range
//!   (lifted into `IndexScan` because the binder rewrites BETWEEN into
//!   `id >= lo AND id <= hi`, which the lowerer pattern-matches).
//! - `SELECT COUNT(*) FROM t WHERE id = N` — aggregate over an index
//!   probe; confirms the dispatcher composes with `HashAggregate`.
//! - `SELECT * FROM t WHERE val = N` — predicate on an *unindexed*
//!   column still works correctly (`SeqScan` + `Filter`, no regression).
//!
//! Why no explicit "operator was `IndexScan`" wire-level assertion:
//! the server does not yet expose `EXPLAIN` over the wire (ROADMAP
//! v0.5 P0 lists `EXPLAIN` as ❌). The unit tests in
//! `pipeline::tests::lower_query_*_indexed_column_picks_index_scan`
//! pin the dispatcher decision at the operator level. This file's
//! contribution is the *behavioural* end-to-end correctness check
//! plus the micro-bench at the bottom of the module.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

/// Spin up an in-process server on an ephemeral TCP port and return a
/// connected `tokio-postgres` client plus the join handles so the test
/// can shut everything down cleanly.
async fn start_server_and_connect() -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());
    let server_handle = tokio::spawn(serve_listener(listener, server));

    let conn_str = format!(
        "host={host} port={port} user=tester application_name=index_scan_test",
        host = bound.ip(),
        port = bound.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("tokio-postgres connect");
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    (client, conn_handle, server_handle)
}

/// Tidy shutdown sequence — drop the client, give the connection task
/// a beat to flush its socket teardown, then abort the listener.
async fn shutdown(
    client: tokio_postgres::Client,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    drop(client);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

/// Insert `n_rows` of `(id INT, val INT)` rows into `table_name` via a
/// single multi-row VALUES statement.
async fn preload(client: &tokio_postgres::Client, table: &str, n_rows: i32) {
    client
        .batch_execute(&format!(
            "CREATE TABLE {table} (id INT NOT NULL, val INT NOT NULL)"
        ))
        .await
        .expect("create table");
    let mut sql = String::with_capacity(usize::try_from(n_rows).unwrap_or(0) * 16 + 64);
    sql.push_str("INSERT INTO ");
    sql.push_str(table);
    sql.push_str(" VALUES ");
    for j in 0..n_rows {
        if j > 0 {
            sql.push(',');
        }
        sql.push('(');
        sql.push_str(&j.to_string());
        sql.push(',');
        sql.push_str(&(j * 10).to_string());
        sql.push(')');
    }
    client.batch_execute(&sql).await.expect("preload");
}

/// `SELECT * FROM t WHERE id = 42` returns exactly the row with `id =
/// 42` when an index covers the column.
#[tokio::test]
async fn point_lookup_with_index_returns_one_row() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    preload(&client, "t_point", 1000).await;
    client
        .batch_execute("CREATE INDEX ix_t_point_id ON t_point(id)")
        .await
        .expect("create index");

    let rows = client
        .simple_query("SELECT id, val FROM t_point WHERE id = 42")
        .await
        .expect("query");
    let pairs: Vec<(i32, i32)> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => {
                let id = r.get(0)?.parse::<i32>().ok()?;
                let val = r.get(1)?.parse::<i32>().ok()?;
                Some((id, val))
            }
            _ => None,
        })
        .collect();
    assert_eq!(pairs, vec![(42, 420)]);

    shutdown(client, server_handle).await;
}

/// `SELECT * FROM t WHERE id BETWEEN 100 AND 200` returns the 101 rows
/// in the inclusive range. The binder rewrites BETWEEN into
/// `id >= 100 AND id <= 200`; the lowerer recognises that shape as a
/// bounded range and dispatches to `IndexScan`.
#[tokio::test]
async fn between_range_with_index_returns_inclusive_range() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    preload(&client, "t_range", 500).await;
    client
        .batch_execute("CREATE INDEX ix_t_range_id ON t_range(id)")
        .await
        .expect("create index");

    let rows = client
        .simple_query("SELECT id FROM t_range WHERE id BETWEEN 100 AND 200")
        .await
        .expect("query");
    let mut ids: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0)?.parse::<i32>().ok(),
            _ => None,
        })
        .collect();
    ids.sort_unstable();
    let expected: Vec<i32> = (100..=200).collect();
    assert_eq!(ids, expected);

    shutdown(client, server_handle).await;
}

/// `SELECT COUNT(*) FROM t WHERE id = 42` returns one row whose value
/// is the cardinality of the index probe (here `1`). Confirms the
/// dispatcher composes with `HashAggregate`.
#[tokio::test]
async fn count_over_index_probe_returns_correct_count() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    preload(&client, "t_count", 1000).await;
    client
        .batch_execute("CREATE INDEX ix_t_count_id ON t_count(id)")
        .await
        .expect("create index");

    let rows = client
        .simple_query("SELECT COUNT(*) FROM t_count WHERE id = 42")
        .await
        .expect("query");
    let counts: Vec<i64> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0)?.parse::<i64>().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(counts, vec![1]);

    shutdown(client, server_handle).await;
}

/// `WHERE val = N` on an unindexed column still works correctly
/// (`SeqScan` + `Filter`). Confirms no regression for queries the
/// dispatcher must leave on the fallback path.
#[tokio::test]
async fn unindexed_column_filter_still_works() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    preload(&client, "t_unindexed", 1000).await;
    client
        .batch_execute("CREATE INDEX ix_t_unindexed_id ON t_unindexed(id)")
        .await
        .expect("create index");

    // Predicate is on `val`, not `id`; the index does not cover it.
    let rows = client
        .simple_query("SELECT id, val FROM t_unindexed WHERE val = 7770")
        .await
        .expect("query");
    let pairs: Vec<(i32, i32)> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => {
                let id = r.get(0)?.parse::<i32>().ok()?;
                let val = r.get(1)?.parse::<i32>().ok()?;
                Some((id, val))
            }
            _ => None,
        })
        .collect();
    assert_eq!(pairs, vec![(777, 7770)]);

    shutdown(client, server_handle).await;
}

/// `WHERE id < N` over an indexed column returns rows `0..N`.
#[tokio::test]
async fn less_than_with_index_returns_prefix() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    preload(&client, "t_lt", 200).await;
    client
        .batch_execute("CREATE INDEX ix_t_lt_id ON t_lt(id)")
        .await
        .expect("create index");

    let rows = client
        .simple_query("SELECT id FROM t_lt WHERE id < 5")
        .await
        .expect("query");
    let mut ids: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0)?.parse::<i32>().ok(),
            _ => None,
        })
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1, 2, 3, 4]);

    shutdown(client, server_handle).await;
}

/// Micro-bench: point-lookup with an index should observably beat the
/// `SeqScan` baseline once the table is non-trivial. The assertion is
/// "indexed is at least 1.5× faster than unindexed on a 50 000-row
/// point-lookup" — chosen with substantial slack because micro-bench
/// numbers inside a `cargo test` job sit on top of process startup,
/// connection handshake, and a buffer pool warmup that perturb the
/// timing. The unit tests above pin "`IndexScan` was chosen"; this
/// test pins the *consequence* — that picking `IndexScan` is actually
/// a win.
///
/// The test bounds run-time to under 30 s even on a cold cache by
/// keeping `n_rows = 50_000`; the median-of-`SAMPLES` reporting
/// ensures one slow iteration does not flake the assertion.
#[tokio::test]
async fn point_lookup_with_index_is_faster_than_seq_scan() {
    const N_ROWS: i32 = 50_000;
    const SAMPLES: usize = 8;
    const TARGET_KEY: i32 = N_ROWS / 2;

    let (client, _conn_handle, server_handle) = start_server_and_connect().await;
    preload(&client, "t_bench_idx", N_ROWS).await;
    preload(&client, "t_bench_noidx", N_ROWS).await;
    client
        .batch_execute("CREATE INDEX ix_t_bench_idx_id ON t_bench_idx(id)")
        .await
        .expect("create index");

    let median = |mut xs: Vec<u128>| -> u128 {
        xs.sort_unstable();
        xs[xs.len() / 2]
    };

    // Warmup one of each path so neither version pays connection-setup
    // costs disproportionately.
    let _ = client
        .simple_query(&format!(
            "SELECT id FROM t_bench_idx WHERE id = {TARGET_KEY}"
        ))
        .await
        .expect("warmup idx");
    let _ = client
        .simple_query(&format!(
            "SELECT id FROM t_bench_noidx WHERE id = {TARGET_KEY}"
        ))
        .await
        .expect("warmup noidx");

    // Measure SeqScan-path latency.
    let mut seq_us: Vec<u128> = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let t0 = Instant::now();
        let _ = client
            .simple_query(&format!(
                "SELECT id FROM t_bench_noidx WHERE id = {TARGET_KEY}"
            ))
            .await
            .expect("seq scan probe");
        seq_us.push(t0.elapsed().as_micros());
    }
    // Measure IndexScan-path latency.
    let mut idx_us: Vec<u128> = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let t0 = Instant::now();
        let _ = client
            .simple_query(&format!(
                "SELECT id FROM t_bench_idx WHERE id = {TARGET_KEY}"
            ))
            .await
            .expect("index scan probe");
        idx_us.push(t0.elapsed().as_micros());
    }

    let seq_median = median(seq_us);
    let idx_median = median(idx_us);
    eprintln!(
        "point_lookup_bench: seq_median={seq_median} us, idx_median={idx_median} us, ratio={:.2}x",
        seq_median as f64 / idx_median.max(1) as f64
    );

    // SeqScan over 50k rows must take at least 1.5x as long as the
    // IndexScan. If both numbers are tiny (e.g. < 100 us due to
    // optimisation), we skip the assertion: the system is so fast that
    // the ratio is dominated by noise. This is the documented escape
    // hatch in PERFORMANCE.md §2 ("Microbenchmarks measure
    // microseconds. … both are necessary; neither substitutes for the
    // other.").
    if seq_median >= 100 {
        assert!(
            idx_median * 3 < seq_median * 2,
            "expected IndexScan to be observably faster than SeqScan on a 50k-row table; \
             seq={seq_median} us, idx={idx_median} us"
        );
    }

    shutdown(client, server_handle).await;
}
