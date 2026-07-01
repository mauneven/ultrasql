//! End-to-end coverage for the process-wide memory-admission ceiling.
//!
//! `work_mem` bounds one statement, but N connections × `work_mem` is
//! unbounded. The server-wide ceiling (`--memory-ceiling-bytes` /
//! `ULTRASQL_MEMORY_CEILING_BYTES`, default 0 = auto = 75 % of physical
//! RAM) caps the effective per-statement budget at
//! `min(session work_mem, ceiling / live connections)`. These tests prove:
//!
//! 1. `SHOW effective_work_mem` reflects the cap: a `SET work_mem = '1GB'`
//!    session on a small-ceiling server gets `ceiling / connections`, and
//!    the divisor tracks live connections.
//! 2. A large ORDER BY on a small-ceiling server with a huge session
//!    `work_mem` still returns correct results — the sort spills under the
//!    admitted budget instead of ballooning the heap toward OOM.
//! 3. At default settings (auto ceiling) a single session keeps the full
//!    64 MiB default `work_mem`, so benchmark-shaped workloads see no
//!    budget change.

use std::time::Duration;

pub mod support;

use support::{connect_as, shutdown, start_configured_server, start_sample_server};
use ultrasql_server::Server;

fn show_value(rows: &[tokio_postgres::SimpleQueryMessage]) -> String {
    rows.iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some(row.get(0).expect("value present").to_owned())
            }
            _ => None,
        })
        .expect("a data row")
}

/// 1. `SHOW effective_work_mem` = min(session work_mem, ceiling / live
///    connections), tracking connect/disconnect.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn effective_work_mem_is_capped_by_ceiling_over_connections() {
    const CEILING: u64 = 8 * 1024 * 1024; // 8 MiB
    let mut server = Server::with_sample_database();
    server.set_memory_ceiling_bytes(CEILING);
    let running = start_configured_server(server, "memory_ceiling_test").await;
    let a = &running.client;

    // One connection: the full ceiling, but the session's 1 GiB request is
    // capped to it.
    a.batch_execute("SET work_mem = '1GB'")
        .await
        .expect("set huge work_mem");
    let rows = a
        .simple_query("SHOW effective_work_mem")
        .await
        .expect("show effective_work_mem");
    assert_eq!(
        show_value(&rows),
        CEILING.to_string(),
        "one connection: effective budget = min(1GB, ceiling/1) = ceiling"
    );
    // SHOW work_mem still reports the session's requested value.
    let rows = a
        .simple_query("SHOW work_mem")
        .await
        .expect("show work_mem");
    assert_eq!(show_value(&rows), (1024u64 * 1024 * 1024).to_string());

    // Second live connection halves the share.
    let (b, b_conn) = connect_as(running.bound, "tester", "memory_ceiling_b").await;
    let rows = a
        .simple_query("SHOW effective_work_mem")
        .await
        .expect("show effective_work_mem with two connections");
    assert_eq!(
        show_value(&rows),
        (CEILING / 2).to_string(),
        "two connections: effective budget = ceiling / 2"
    );

    // A session asking for *less* than its share keeps its own value.
    a.batch_execute("SET work_mem = '1MB'")
        .await
        .expect("set small work_mem");
    let rows = a
        .simple_query("SHOW effective_work_mem")
        .await
        .expect("show effective_work_mem under small work_mem");
    assert_eq!(
        show_value(&rows),
        (1024u64 * 1024).to_string(),
        "a request below the admission share is unchanged"
    );

    // Disconnect B: the share recovers (deregistration is asynchronous —
    // poll briefly).
    drop(b);
    b_conn.abort();
    a.batch_execute("SET work_mem = '1GB'")
        .await
        .expect("set huge work_mem again");
    let mut recovered = false;
    for _ in 0..50 {
        let rows = a
            .simple_query("SHOW effective_work_mem")
            .await
            .expect("show effective_work_mem after disconnect");
        if show_value(&rows) == CEILING.to_string() {
            recovered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        recovered,
        "after B disconnects the effective budget must return to the full ceiling"
    );

    shutdown(running).await;
}

/// 2. Ceiling-capped budget engages the spill path instead of honouring a
///    huge session `work_mem`: a large scrambled ORDER BY on a
///    minimum-ceiling server with `SET work_mem = '1GB'` returns fully
///    sorted, correct results.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_sort_under_small_ceiling_spills_and_stays_correct() {
    let mut server = Server::with_sample_database();
    // Floor-clamped to the 64 KiB minimum effective budget.
    server.set_memory_ceiling_bytes(1);
    let running = start_configured_server(server, "memory_ceiling_spill_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE mc_sort (id INT NOT NULL, val INT NOT NULL)")
        .await
        .expect("create");
    const N: i32 = 5000;
    let mut stmt = String::from("INSERT INTO mc_sort VALUES ");
    for i in 0..N {
        let v = i32::try_from((i64::from(i) * 2_654_435_761).rem_euclid(i64::from(N)))
            .expect("scrambled value fits i32");
        if i > 0 {
            stmt.push(',');
        }
        stmt.push_str(&format!("({i}, {v})"));
    }
    client.batch_execute(&stmt).await.expect("bulk insert");

    // The session asks for 1 GiB; the admission cap overrides it down to
    // the 64 KiB floor, forcing the sort to spill.
    client
        .batch_execute("SET work_mem = '1GB'")
        .await
        .expect("set huge work_mem");
    let rows = client
        .simple_query("SHOW effective_work_mem")
        .await
        .expect("show effective_work_mem");
    assert_eq!(
        show_value(&rows),
        (64 * 1024).to_string(),
        "the admission cap must override the session's 1GB request"
    );

    let rows = client
        .simple_query("SELECT val FROM mc_sort ORDER BY val ASC")
        .await
        .expect("sorted query succeeds under the admission cap");
    let vals: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                row.get(0)
                    .expect("val present")
                    .parse::<i32>()
                    .expect("val parses"),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(vals.len(), N as usize, "all rows returned");
    assert!(
        vals.windows(2).all(|w| w[0] <= w[1]),
        "results are fully sorted despite spilling under the admitted budget"
    );

    shutdown(running).await;
}

/// 3. Defaults are generous: with the auto ceiling (75 % of RAM) a single
///    session keeps the full 64 MiB default `work_mem` — the admission cap
///    must not shrink default-config budgets (benchmark non-regression at
///    the policy level).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_auto_ceiling_leaves_default_work_mem_untouched() {
    let running = start_sample_server("memory_ceiling_default_test").await;
    let client = &running.client;

    let rows = client
        .simple_query("SHOW effective_work_mem")
        .await
        .expect("show effective_work_mem at defaults");
    assert_eq!(
        show_value(&rows),
        (64u64 * 1024 * 1024).to_string(),
        "auto ceiling (75% of RAM) must not cap the 64 MiB default work_mem"
    );

    shutdown(running).await;
}
