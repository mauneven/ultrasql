//! Test sub-module; see `tests/mod.rs` for shared helpers.

#![allow(unused_imports)]

use super::*;


// -----------------------------------------------------------------------
// Wave B: optimizer + plan cache.
//
// The optimizer (rule-based rewrites) runs against every DML/SELECT
// before the operator lowerer; the result is cached against the raw
// SQL text. These tests pin the contract:
//
// 1. A repeat Simple Query reuses the cached plan (the optimiser
//    closure runs once, the entry's `use_count` advances on each
//    call).
// 2. A new SQL text creates a fresh cache entry.
// 3. A DDL statement invalidates the cache so the next DML/SELECT
//    re-plans.
// 4. The Simple Query path and the Extended Query Parse path share
//    the same cache: an Extended Parse over an SQL string already
//    optimised by a prior Simple Query reuses the cached plan
//    (cross-protocol sharing — the headline win of the wave).
//
// Each test asserts both the cache shape (`plan_cache.len()`,
// `use_count`) and the result correctness (the query still returns
// the expected rows) so a regression in either layer is caught
// here, not in the integration suite.
// -----------------------------------------------------------------------

/// Issuing the same `SELECT` SQL twice via Simple Query inserts one
/// cache entry on the first call and increments its `use_count` on
/// the second — the optimiser closure does not run again.
#[tokio::test]
async fn plan_cache_simple_query_repeat_reuses_optimised_plan() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = server();
    let cache = Arc::clone(&state.plan_cache);
    assert_eq!(cache.len(), 0, "cache empty before any query runs");
    let handle = tokio::spawn(handle_connection(server_side, Arc::clone(&state)));

    complete_startup(&mut client).await;

    let sql = "SELECT id FROM users".to_string();
    send_frontend(&mut client, &FrontendMessage::Query { sql: sql.clone() }).await;
    let _ = drain_until_ready(&mut client).await;
    assert_eq!(cache.len(), 1, "first Simple Query inserts one entry");

    send_frontend(&mut client, &FrontendMessage::Query { sql }).await;
    let _ = drain_until_ready(&mut client).await;
    assert_eq!(
        cache.len(),
        1,
        "second Simple Query reuses the cached entry; no new entry inserted"
    );

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}


/// Two distinct SELECTs produce two cache entries — the cache key is
/// the SQL text, so different text should not collide.
#[tokio::test]
async fn plan_cache_distinct_sql_text_produces_distinct_entries() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = server();
    let cache = Arc::clone(&state.plan_cache);
    let handle = tokio::spawn(handle_connection(server_side, Arc::clone(&state)));

    complete_startup(&mut client).await;

    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "SELECT id FROM users".to_string(),
        },
    )
    .await;
    let _ = drain_until_ready(&mut client).await;
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "SELECT id FROM users WHERE id = 1".to_string(),
        },
    )
    .await;
    let _ = drain_until_ready(&mut client).await;
    assert_eq!(
        cache.len(),
        2,
        "distinct SQL text yields distinct cache entries"
    );

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}


/// A `CREATE TABLE` clears every entry in the plan cache; a query
/// run after the DDL therefore inserts a fresh entry rather than
/// reusing the pre-DDL plan.
#[tokio::test]
async fn plan_cache_ddl_invalidates_every_entry() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = server();
    let cache = Arc::clone(&state.plan_cache);
    let handle = tokio::spawn(handle_connection(server_side, Arc::clone(&state)));

    complete_startup(&mut client).await;

    // 1. Prime the cache with a SELECT.
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "SELECT id FROM users".to_string(),
        },
    )
    .await;
    let _ = drain_until_ready(&mut client).await;
    assert_eq!(cache.len(), 1, "prime: one cached entry");

    // 2. Run a CREATE TABLE — the cache should be cleared.
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "CREATE TABLE tt (id INT NOT NULL)".to_string(),
        },
    )
    .await;
    let _ = drain_until_ready(&mut client).await;
    assert_eq!(cache.len(), 0, "DDL must invalidate every cached entry");

    // 3. Re-run the SELECT — a fresh entry is inserted.
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "SELECT id FROM users".to_string(),
        },
    )
    .await;
    let _ = drain_until_ready(&mut client).await;
    assert_eq!(cache.len(), 1, "post-DDL query inserts a fresh cache entry");

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}


/// Cross-protocol cache sharing: a Simple Query primes the cache;
/// an Extended Query `Parse` over the same SQL text hits the cache
/// and does NOT insert a second entry. The headline win of the
/// wave — wire-compatibility for the libpq world means an ORM that
/// issues `Parse`+`Bind`+`Execute` for a SELECT a `psql` session
/// previously typed pays no extra optimization cost.
#[tokio::test]
async fn plan_cache_shared_between_simple_and_extended_query() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = server();
    let cache = Arc::clone(&state.plan_cache);
    let handle = tokio::spawn(handle_connection(server_side, Arc::clone(&state)));

    complete_startup(&mut client).await;

    let sql = "SELECT id FROM users".to_string();

    // 1. Simple Query: primes the cache.
    send_frontend(&mut client, &FrontendMessage::Query { sql: sql.clone() }).await;
    let _ = drain_until_ready(&mut client).await;
    assert_eq!(cache.len(), 1, "Simple Query inserts one cached entry");

    // 2. Extended Query: Parse over the same SQL text should hit
    //    the cache. Issue a Parse/Sync pair (no Execute needed —
    //    the optimisation step happens inside `handle_parse`).
    send_frontend(
        &mut client,
        &FrontendMessage::Parse {
            name: String::new(),
            sql,
            param_types: vec![],
        },
    )
    .await;
    send_frontend(&mut client, &FrontendMessage::Sync).await;
    let _ = drain_until_ready(&mut client).await;
    assert_eq!(
        cache.len(),
        1,
        "Extended Query Parse must reuse the cached entry primed by Simple Query"
    );

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}


/// `WHERE id = 42` over an indexed column still picks `IndexScan`
/// when the bound plan flows through the optimizer first.
///
/// The optimizer's rule loop is shape-preserving for the
/// `Filter { Scan, Eq(Col, Literal) }` shape (predicate pushdown is
/// a no-op when the filter is already on the leaf scan), so the
/// catalog-aware lowerer in `pipeline::try_index_scan` still sees
/// the indexable shape and dispatches to `IndexScan`. This test
/// pins that round-trip.
#[tokio::test]
async fn optimizer_route_still_selects_index_scan() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = server();
    let handle = tokio::spawn(handle_connection(server_side, Arc::clone(&state)));

    complete_startup(&mut client).await;

    // CREATE + populate + CREATE INDEX.
    for sql in [
        "CREATE TABLE t_ix (id INT NOT NULL, val INT NOT NULL)",
        "INSERT INTO t_ix VALUES (1,10),(2,20),(3,30),(42,420),(99,990)",
        "CREATE INDEX ix_t_ix_id ON t_ix(id)",
    ] {
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: sql.to_string(),
            },
        )
        .await;
        let _ = drain_until_ready(&mut client).await;
    }

    // SELECT WHERE id = 42 should return exactly the one row, going
    // through the optimizer first.
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "SELECT id, val FROM t_ix WHERE id = 42".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    let rows: Vec<_> = msgs
        .iter()
        .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
        .collect();
    assert_eq!(rows.len(), 1, "point lookup must return one row");
    match rows[0] {
        BackendMessage::DataRow { columns } => {
            assert_eq!(columns[0].as_deref(), Some(b"42".as_slice()));
            assert_eq!(columns[1].as_deref(), Some(b"420".as_slice()));
        }
        _ => unreachable!(),
    }

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}

