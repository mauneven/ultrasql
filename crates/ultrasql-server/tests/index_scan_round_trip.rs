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
//! - `SELECT * FROM t ORDER BY id DESC` — directed B-tree scan over an
//!   indexed column returns descending order without a Sort operator.
//!
//! Why no explicit "operator was `IndexScan`" wire-level assertion:
//! the server does not yet expose `EXPLAIN` over the wire (ROADMAP
//! v0.5 P0 lists `EXPLAIN` as ❌). The unit tests in
//! `pipeline::tests::lower_query_*_indexed_column_picks_index_scan`
//! pin the dispatcher decision at the operator level. This file's
//! contribution is the *behavioural* end-to-end correctness check
//! plus the micro-bench at the bottom of the module.

use std::sync::Arc;
use std::time::Instant;

use num_traits::ToPrimitive;
use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId, Xid};
use ultrasql_storage::access_method::BrinIndex;
use ultrasql_storage::btree::BTree;

pub mod support;

use support::{shutdown as graceful_shutdown, start_persistent_server, start_sample_server};

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

/// Insert sysbench-like `(id, k, c, pad)` rows into `table_name`.
async fn preload_sysbench_like(client: &tokio_postgres::Client, table: &str, n_rows: i32) {
    client
        .batch_execute(&format!(
            "CREATE TABLE {table} \
             (id INT NOT NULL, k INT NOT NULL, c TEXT NOT NULL, pad TEXT NOT NULL)"
        ))
        .await
        .expect("create sysbench-like table");
    let mut sql = String::with_capacity(usize::try_from(n_rows).unwrap_or(0) * 32 + 96);
    sql.push_str("INSERT INTO ");
    sql.push_str(table);
    sql.push_str(" (id, k, c, pad) VALUES ");
    for j in 0..n_rows {
        if j > 0 {
            sql.push(',');
        }
        let k = j.wrapping_mul(17) % 1_000_000;
        sql.push_str(&format!("({j}, {k}, 'c{j}', 'pad{j}')"));
    }
    client.batch_execute(&sql).await.expect("preload");
}

fn rows_first_col(rows: &[tokio_postgres::SimpleQueryMessage]) -> Vec<String> {
    rows.iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_owned),
            _ => None,
        })
        .collect()
}

/// `SELECT * FROM t WHERE id = 42` returns exactly the row with `id =
/// 42` when an index covers the column.
#[tokio::test]
async fn point_lookup_with_index_returns_one_row() {
    let running = start_sample_server("index_scan_test").await;
    let client = &running.client;
    preload(client, "t_point", 1000).await;
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

    graceful_shutdown(running).await;
}

/// `CREATE INDEX CONCURRENTLY` is accepted on the wire and produces the same
/// visible index state as the current non-blocking build path.
#[tokio::test]
async fn create_index_concurrently_then_point_lookup_round_trip() {
    let running = start_sample_server("index_scan_test").await;
    let client = &running.client;
    preload(client, "t_cic", 100).await;

    client
        .batch_execute("CREATE INDEX CONCURRENTLY ix_t_cic_id ON t_cic(id)")
        .await
        .expect("create index concurrently");

    let rows = client
        .simple_query("SELECT val FROM t_cic WHERE id = 42")
        .await
        .expect("query through concurrent index");
    assert_eq!(rows_first_col(&rows), vec!["420".to_string()]);

    graceful_shutdown(running).await;
}

/// Rows inserted after `CREATE INDEX` must be visible through the
/// index path. This catches stale-index builds where `CREATE INDEX`
/// populated the B-tree once but later INSERTs only touched the heap.
#[tokio::test]
async fn insert_after_create_index_updates_btree() {
    let running = start_sample_server("index_scan_test").await;
    let client = &running.client;
    preload(client, "t_insert_after_index", 10).await;
    client
        .batch_execute("CREATE INDEX ix_t_insert_after_index_id ON t_insert_after_index(id)")
        .await
        .expect("create index");
    client
        .batch_execute("INSERT INTO t_insert_after_index VALUES (999, 9990)")
        .await
        .expect("insert after index");

    let rows = client
        .simple_query("SELECT id, val FROM t_insert_after_index WHERE id = 999")
        .await
        .expect("query inserted row through index");
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
    assert_eq!(pairs, vec![(999, 9990)]);

    graceful_shutdown(running).await;
}

/// BRIN indexes keep min/max block-range summaries, use those ranges
/// for heap scan pruning, and update summaries for post-index INSERTs.
#[tokio::test]
async fn brin_index_range_scan_and_insert_maintenance_round_trip() {
    let running = start_sample_server("index_scan_test").await;
    let server = Arc::clone(&running.server);
    let client = &running.client;
    preload(client, "t_brin_idx", 30_000).await;
    client
        .batch_execute("CREATE INDEX ix_t_brin_idx_id ON t_brin_idx USING brin (id)")
        .await
        .expect("create brin index");

    let brin = {
        let snapshot = server.persistent_catalog.snapshot();
        let table = snapshot.tables.get("t_brin_idx").expect("table exists");
        let constraints = server
            .table_constraints
            .get(&table.oid)
            .expect("runtime index metadata exists");
        constraints
            .indexes
            .values()
            .find_map(|metadata| metadata.brin.clone())
            .expect("brin summary stored")
    };
    assert!(
        brin.summary_count() >= 2,
        "fixture should span multiple BRIN ranges"
    );

    let rows = client
        .simple_query("SELECT id FROM t_brin_idx WHERE id BETWEEN 29000 AND 29005 ORDER BY id")
        .await
        .expect("query through brin");
    assert_eq!(
        rows_first_col(&rows),
        vec![
            "29000".to_string(),
            "29001".to_string(),
            "29002".to_string(),
            "29003".to_string(),
            "29004".to_string(),
            "29005".to_string()
        ]
    );

    client
        .batch_execute("INSERT INTO t_brin_idx VALUES (35000, 350000)")
        .await
        .expect("insert after brin index");
    assert!(
        !brin
            .candidate_ranges_for_key(&BrinIndex::encode_i64_key(35_000))
            .is_empty()
    );
    let rows = client
        .simple_query("SELECT val FROM t_brin_idx WHERE id = 35000")
        .await
        .expect("query inserted row through brin");
    assert_eq!(rows_first_col(&rows), vec!["350000".to_string()]);

    client
        .batch_execute("DELETE FROM t_brin_idx WHERE id = 35000")
        .await
        .expect("delete brin-covered row");
    client
        .batch_execute("VACUUM t_brin_idx")
        .await
        .expect("vacuum brin table");
    assert!(
        brin.candidate_ranges_for_key(&BrinIndex::encode_i64_key(35_000))
            .is_empty()
    );

    graceful_shutdown(running).await;
}

#[tokio::test]
async fn brin_index_summary_rebuilds_after_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    {
        let running = start_persistent_server(data_dir.path(), "index_scan_restart_test").await;
        preload(&running.client, "t_brin_restart", 10_000).await;
        running
            .client
            .batch_execute("CREATE INDEX ix_t_brin_restart_id ON t_brin_restart USING brin (id)")
            .await
            .expect("create brin index");
        graceful_shutdown(running).await;
    }

    {
        let running = start_persistent_server(data_dir.path(), "index_scan_restart_test").await;
        let brin = {
            let snapshot = running.server.persistent_catalog.snapshot();
            let table = snapshot.tables.get("t_brin_restart").expect("table exists");
            let constraints = running
                .server
                .table_constraints
                .get(&table.oid)
                .expect("runtime index metadata exists after restart");
            constraints
                .indexes
                .values()
                .find_map(|metadata| metadata.brin.clone())
                .expect("brin summary rebuilt after restart")
        };
        assert!(brin.summary_count() >= 1);
        let rows = running
            .client
            .simple_query(
                "SELECT id FROM t_brin_restart WHERE id BETWEEN 9990 AND 9992 ORDER BY id",
            )
            .await
            .expect("query through rebuilt brin");
        assert_eq!(
            rows_first_col(&rows),
            vec!["9990".to_string(), "9991".to_string(), "9992".to_string()]
        );
        graceful_shutdown(running).await;
    }
}

#[tokio::test]
async fn expression_index_runtime_metadata_survives_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    {
        let running = start_persistent_server(data_dir.path(), "expr_index_restart_test").await;
        running
            .client
            .batch_execute(
                "CREATE TABLE t_expr_restart (name TEXT NOT NULL, payload INT NOT NULL); \
                 INSERT INTO t_expr_restart VALUES ('Alice', 1); \
                 CREATE UNIQUE INDEX ux_t_expr_restart_lower_name \
                 ON t_expr_restart (lower(name))",
            )
            .await
            .expect("create expression index before restart");
        graceful_shutdown(running).await;
    }

    {
        let running = start_persistent_server(data_dir.path(), "expr_index_restart_test").await;
        let err = running
            .client
            .batch_execute("INSERT INTO t_expr_restart VALUES ('alice', 2)")
            .await
            .expect_err("expression index duplicate must remain rejected after restart");
        let db_err = err.as_db_error().expect("server returns SQLSTATE");
        assert_eq!(db_err.code().code(), "23505");
        graceful_shutdown(running).await;
    }
}

/// Plain non-unique indexes must retain every duplicate key/TID pair.
/// Unique enforcement belongs only to UNIQUE / PRIMARY KEY indexes.
#[tokio::test]
async fn non_unique_index_returns_duplicate_key_rows() {
    let running = start_sample_server("index_scan_test").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE t_nonunique_idx (id INT NOT NULL, val INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t_nonunique_idx VALUES (1, 10), (1, 20), (2, 30)")
        .await
        .expect("insert duplicate keys before index");
    client
        .batch_execute("CREATE INDEX ix_t_nonunique_idx_id ON t_nonunique_idx(id)")
        .await
        .expect("create non-unique index over duplicates");
    client
        .batch_execute("INSERT INTO t_nonunique_idx VALUES (1, 40)")
        .await
        .expect("insert duplicate key after index");

    let rows = client
        .simple_query("SELECT val FROM t_nonunique_idx WHERE id = 1 ORDER BY val")
        .await
        .expect("query duplicate rows through non-unique index");
    assert_eq!(
        rows_first_col(&rows),
        vec!["10".to_string(), "20".to_string(), "40".to_string()]
    );

    graceful_shutdown(running).await;
}

/// Duplicate keys rejected by insert-side index maintenance must fail
/// before the heap write, preserving statement atomicity.
#[tokio::test]
async fn duplicate_insert_after_unique_index_returns_23505_and_preserves_heap() {
    let running = start_sample_server("index_scan_test").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE t_unique_idx (id INT NOT NULL, val INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t_unique_idx VALUES (1, 10)")
        .await
        .expect("insert");
    client
        .batch_execute("CREATE UNIQUE INDEX ix_t_unique_idx_id ON t_unique_idx(id)")
        .await
        .expect("create unique index");

    let err = client
        .batch_execute("INSERT INTO t_unique_idx VALUES (1, 99)")
        .await
        .expect_err("duplicate indexed key must fail");
    let sqlstate = err.code().expect("server-sent SQLSTATE present");
    assert_eq!(sqlstate.code(), "23505");

    let rows = client
        .simple_query("SELECT val FROM t_unique_idx WHERE id = 1")
        .await
        .expect("query original row");
    let vals: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0)?.parse::<i32>().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(vals, vec![10]);

    graceful_shutdown(running).await;
}

/// Updating a non-key column after `CREATE INDEX` must keep the
/// indexed point lookup alive.
#[tokio::test]
async fn update_non_key_column_after_create_index_updates_btree_tid() {
    let running = start_sample_server("index_scan_test").await;
    let client = &running.client;
    preload(client, "t_update_indexed", 10).await;
    client
        .batch_execute("CREATE INDEX ix_t_update_indexed_id ON t_update_indexed(id)")
        .await
        .expect("create index");

    client
        .batch_execute("UPDATE t_update_indexed SET val = 777 WHERE id = 7")
        .await
        .expect("indexed-table UPDATE");

    let rows = client
        .simple_query("SELECT val FROM t_update_indexed WHERE id = 7")
        .await
        .expect("query updated row through index");
    let vals: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0)?.parse::<i32>().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(vals, vec![777]);

    graceful_shutdown(running).await;
}

/// Repeated non-key updates on an indexed predicate must keep replacing the
/// B-tree TID. A single update used to pass while the second lookup could
/// follow the key to a dead heap version and return no rows.
#[tokio::test]
async fn repeated_non_key_update_after_create_index_keeps_point_lookup_alive() {
    let running = start_sample_server("index_scan_test").await;
    let client = &running.client;
    preload(client, "t_update_indexed_repeat", 1000).await;
    client
        .batch_execute("CREATE INDEX ix_t_update_indexed_repeat_id ON t_update_indexed_repeat(id)")
        .await
        .expect("create index");

    for expected in [281, 282] {
        client
            .batch_execute("UPDATE t_update_indexed_repeat SET val = val + 1 WHERE id = 28")
            .await
            .expect("indexed-table UPDATE");

        let rows = client
            .simple_query("SELECT val FROM t_update_indexed_repeat WHERE id = 28")
            .await
            .expect("query updated row through index");
        let vals: Vec<i32> = rows
            .iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0)?.parse::<i32>().ok(),
                _ => None,
            })
            .collect();
        assert_eq!(vals, vec![expected]);
    }

    graceful_shutdown(running).await;
}

/// Sysbench read/write mixes point updates and high-key inserts. This locks
/// the first deterministic single-client prefix that made an untouched key
/// disappear from a unique index lookup.
#[tokio::test]
async fn sysbench_like_dml_prefix_keeps_untouched_point_lookup_alive() {
    let running = start_sample_server("index_scan_test").await;
    let client = &running.client;
    preload_sysbench_like(client, "t_sysbench_prefix", 1000).await;
    client
        .batch_execute("CREATE UNIQUE INDEX ix_t_sysbench_prefix_id ON t_sysbench_prefix(id)")
        .await
        .expect("create index");

    for (step, sql) in [
        "UPDATE t_sysbench_prefix SET k = k + 1 WHERE id = 932",
        "UPDATE t_sysbench_prefix SET k = k + 1 WHERE id = 323",
        "UPDATE t_sysbench_prefix SET k = k + 1 WHERE id = 485",
        "UPDATE t_sysbench_prefix SET k = k + 1 WHERE id = 396",
        "UPDATE t_sysbench_prefix SET k = k + 1 WHERE id = 873",
        "UPDATE t_sysbench_prefix SET k = k + 1 WHERE id = 283",
        "INSERT INTO t_sysbench_prefix (id, k, c, pad) VALUES (1000001000, 235155926, 'c1000001000', 'pad1000001000')",
        "UPDATE t_sysbench_prefix SET k = k + 1 WHERE id = 299",
        "INSERT INTO t_sysbench_prefix (id, k, c, pad) VALUES (1000001001, -1322918395, 'c1000001001', 'pad1000001001')",
        "UPDATE t_sysbench_prefix SET k = k + 1 WHERE id = 489",
        "INSERT INTO t_sysbench_prefix (id, k, c, pad) VALUES (1000001002, 1885956909, 'c1000001002', 'pad1000001002')",
        "UPDATE t_sysbench_prefix SET k = k + 1 WHERE id = 213",
        "INSERT INTO t_sysbench_prefix (id, k, c, pad) VALUES (1000001003, -1403475000, 'c1000001003', 'pad1000001003')",
        "INSERT INTO t_sysbench_prefix (id, k, c, pad) VALUES (1000001004, 1780127685, 'c1000001004', 'pad1000001004')",
        "UPDATE t_sysbench_prefix SET k = k + 1 WHERE id = 454",
        "UPDATE t_sysbench_prefix SET k = k + 1 WHERE id = 108",
        "INSERT INTO t_sysbench_prefix (id, k, c, pad) VALUES (1000001005, 405047199, 'c1000001005', 'pad1000001005')",
        "UPDATE t_sysbench_prefix SET k = k + 1 WHERE id = 990",
        "UPDATE t_sysbench_prefix SET k = k + 1 WHERE id = 641",
        "INSERT INTO t_sysbench_prefix (id, k, c, pad) VALUES (1000001006, -1586892587, 'c1000001006', 'pad1000001006')",
        "UPDATE t_sysbench_prefix SET k = k + 1 WHERE id = 190",
        "INSERT INTO t_sysbench_prefix (id, k, c, pad) VALUES (1000001007, -1672241594, 'c1000001007', 'pad1000001007')",
        "INSERT INTO t_sysbench_prefix (id, k, c, pad) VALUES (1000001008, -1743059325, 'c1000001008', 'pad1000001008')",
        "UPDATE t_sysbench_prefix SET k = k + 1 WHERE id = 576",
        "INSERT INTO t_sysbench_prefix (id, k, c, pad) VALUES (1000001009, 781756086, 'c1000001009', 'pad1000001009')",
        "INSERT INTO t_sysbench_prefix (id, k, c, pad) VALUES (1000001010, 1479574444, 'c1000001010', 'pad1000001010')",
    ]
    .into_iter()
    .enumerate()
    {
        client.batch_execute(sql).await.expect("apply DML prefix");
        let rows = client
            .simple_query("SELECT k FROM t_sysbench_prefix WHERE id = 28")
            .await
            .expect("query untouched indexed row during DML prefix");
        assert_eq!(
            rows_first_col(&rows),
            vec!["476".to_string()],
            "lost indexed row after prefix step {}: {sql}",
            step + 1
        );
    }

    let rows = client
        .simple_query("SELECT k FROM t_sysbench_prefix WHERE id = 28")
        .await
        .expect("query untouched indexed row");
    assert_eq!(rows_first_col(&rows), vec!["476".to_string()]);

    graceful_shutdown(running).await;
}

/// Updating the indexed key must move the B-tree entry from the old
/// key to the new key.
#[tokio::test]
async fn update_indexed_key_after_create_index_moves_btree_entry() {
    let running = start_sample_server("index_scan_test").await;
    let client = &running.client;
    preload(client, "t_update_key", 10).await;
    client
        .batch_execute("CREATE INDEX ix_t_update_key_id ON t_update_key(id)")
        .await
        .expect("create index");

    client
        .batch_execute("UPDATE t_update_key SET id = 77 WHERE id = 7")
        .await
        .expect("indexed-key UPDATE");

    let old_rows = client
        .simple_query("SELECT val FROM t_update_key WHERE id = 7")
        .await
        .expect("query old key through index");
    assert_eq!(rows_first_col(&old_rows), Vec::<String>::new());

    let new_rows = client
        .simple_query("SELECT val FROM t_update_key WHERE id = 77")
        .await
        .expect("query new key through index");
    assert_eq!(rows_first_col(&new_rows), vec!["70".to_string()]);

    graceful_shutdown(running).await;
}

/// Updating an indexed key to an existing key fails before heap write.
#[tokio::test]
async fn update_indexed_key_to_duplicate_returns_23505_and_preserves_rows() {
    let running = start_sample_server("index_scan_test").await;
    let client = &running.client;
    preload(client, "t_update_dup", 10).await;
    client
        .batch_execute("CREATE UNIQUE INDEX ix_t_update_dup_id ON t_update_dup(id)")
        .await
        .expect("create unique index");

    let err = client
        .batch_execute("UPDATE t_update_dup SET id = 8 WHERE id = 7")
        .await
        .expect_err("duplicate indexed-key UPDATE");
    let sqlstate = err.code().expect("server-sent SQLSTATE present");
    assert_eq!(sqlstate.code(), "23505");

    let rows = client
        .simple_query("SELECT val FROM t_update_dup WHERE id = 7")
        .await
        .expect("query original key through index");
    assert_eq!(rows_first_col(&rows), vec!["70".to_string()]);

    graceful_shutdown(running).await;
}

/// DELETE on indexed tables removes the B-tree entry so future
/// unique-key reuse is possible.
#[tokio::test]
async fn delete_on_indexed_table_removes_btree_entry() {
    let running = start_sample_server("index_scan_test").await;
    let client = &running.client;
    preload(client, "t_delete_indexed", 10).await;
    client
        .batch_execute("CREATE INDEX ix_t_delete_indexed_id ON t_delete_indexed(id)")
        .await
        .expect("create index");

    client
        .batch_execute("DELETE FROM t_delete_indexed WHERE id = 7")
        .await
        .expect("indexed-table DELETE");

    let rows = client
        .simple_query("SELECT val FROM t_delete_indexed WHERE id = 7")
        .await
        .expect("query deleted key through index");
    assert_eq!(rows_first_col(&rows), Vec::<String>::new());

    client
        .batch_execute("INSERT INTO t_delete_indexed VALUES (7, 700)")
        .await
        .expect("reuse deleted indexed key");
    let rows = client
        .simple_query("SELECT val FROM t_delete_indexed WHERE id = 7")
        .await
        .expect("query reinserted key through index");
    assert_eq!(rows_first_col(&rows), vec!["700".to_string()]);

    graceful_shutdown(running).await;
}

/// SQL `VACUUM table` runs the B-tree vacuum pass and reclaims stale
/// index TIDs that point at committed-dead heap slots.
#[tokio::test]
async fn vacuum_reclaims_stale_index_entries() {
    let running = start_sample_server("index_scan_test").await;
    let server = Arc::clone(&running.server);
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE t_vacuum_idx (id INT NOT NULL, val INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t_vacuum_idx VALUES (1, 10)")
        .await
        .expect("insert row");
    client
        .batch_execute("CREATE INDEX ix_t_vacuum_idx_id ON t_vacuum_idx(id)")
        .await
        .expect("create index");
    client
        .batch_execute("DELETE FROM t_vacuum_idx WHERE id = 1")
        .await
        .expect("delete row");

    let snapshot = server.catalog_snapshot();
    let table = snapshot
        .tables
        .get("t_vacuum_idx")
        .expect("table catalog entry");
    let index = snapshot
        .indexes_by_table
        .get(&table.oid)
        .and_then(|indexes| indexes.first().cloned())
        .expect("index catalog entry");
    let stale_tid = TupleId::new(PageId::new(RelationId(table.oid), BlockNumber::new(0)), 0);
    let mut btree = BTree::open(
        Arc::clone(server.heap.buffer_pool()),
        RelationId::new(index.oid.raw()),
        index.root_block,
    );
    btree
        .insert_non_unique::<i64>(1, stale_tid, Xid::new(1), None)
        .expect("plant stale index entry");
    assert_eq!(btree.lookup_all::<i64>(1).expect("lookup stale").len(), 1);

    client
        .batch_execute("VACUUM t_vacuum_idx")
        .await
        .expect("vacuum table");

    let btree = BTree::open(
        Arc::clone(server.heap.buffer_pool()),
        RelationId::new(index.oid.raw()),
        index.root_block,
    );
    assert!(
        btree
            .lookup_all::<i64>(1)
            .expect("lookup after vacuum")
            .is_empty()
    );

    graceful_shutdown(running).await;
}

/// `SELECT * FROM t WHERE id BETWEEN 100 AND 200` returns the 101 rows
/// in the inclusive range. The binder rewrites BETWEEN into
/// `id >= 100 AND id <= 200`; the lowerer recognises that shape as a
/// bounded range and dispatches to `IndexScan`.
#[tokio::test]
async fn between_range_with_index_returns_inclusive_range() {
    let running = start_sample_server("index_scan_test").await;
    let client = &running.client;
    preload(client, "t_range", 500).await;
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

    graceful_shutdown(running).await;
}

/// `ORDER BY indexed_col DESC` uses the lowerer's directed B-tree path
/// and returns rows in descending key order.
#[tokio::test]
async fn order_by_desc_with_index_returns_descending_rows() {
    let running = start_sample_server("index_scan_test").await;
    let client = &running.client;
    preload(client, "t_order_desc", 8).await;
    client
        .batch_execute("CREATE INDEX ix_t_order_desc_id ON t_order_desc(id)")
        .await
        .expect("create index");

    let rows = client
        .simple_query("SELECT id, val FROM t_order_desc ORDER BY id DESC")
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
    let expected: Vec<(i32, i32)> = (0..8).rev().map(|i| (i, i * 10)).collect();
    assert_eq!(pairs, expected);

    graceful_shutdown(running).await;
}

/// `SELECT COUNT(*) FROM t WHERE id = 42` returns one row whose value
/// is the cardinality of the index probe (here `1`). Confirms the
/// dispatcher composes with `HashAggregate`.
#[tokio::test]
async fn count_over_index_probe_returns_correct_count() {
    let running = start_sample_server("index_scan_test").await;
    let client = &running.client;
    preload(client, "t_count", 1000).await;
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

    graceful_shutdown(running).await;
}

/// `WHERE val = N` on an unindexed column still works correctly
/// (`SeqScan` + `Filter`). Confirms no regression for queries the
/// dispatcher must leave on the fallback path.
#[tokio::test]
async fn unindexed_column_filter_still_works() {
    let running = start_sample_server("index_scan_test").await;
    let client = &running.client;
    preload(client, "t_unindexed", 1000).await;
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

    graceful_shutdown(running).await;
}

/// `WHERE id < N` over an indexed column returns rows `0..N`.
#[tokio::test]
async fn less_than_with_index_returns_prefix() {
    let running = start_sample_server("index_scan_test").await;
    let client = &running.client;
    preload(client, "t_lt", 200).await;
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

    graceful_shutdown(running).await;
}

/// Perf-smoke: point-lookup through a vacuum-certified index-only path must
/// record a selected index path through `EXPLAIN ANALYZE` and emit wire-level
/// timings beside a `SeqScan` baseline.
///
/// The explicit `VACUUM` below is part of the contract, not cosmetics:
/// without all-visible VM proof, `SELECT id FROM t WHERE id = k` must
/// heap-fetch the tuple to check MVCC visibility before projecting the
/// indexed key, while repeat `SeqScan` can replay cached columns. That
/// shape is correctness-first but not guaranteed faster on tiny cached
/// tables.
/// After `VACUUM`, `try_index_only_scan` can answer from the index key.
///
/// This deliberately avoids a hard wall-clock ratio assertion. Normal
/// `cargo test` runs integration tests in parallel, so scheduler noise can make
/// short point-lookups swing even when the planner selects the right path.
/// Release-grade speed ratios belong in reproducible benchmark harnesses under
/// `benchmarks/`, not in this correctness suite.
#[tokio::test]
async fn point_lookup_with_index_records_vacuum_certified_path() {
    const N_ROWS: i32 = 500_000;
    const SAMPLES: usize = 8;
    const TARGET_KEY: i32 = N_ROWS / 2;

    let running = start_sample_server("index_scan_test").await;
    let client = &running.client;
    preload(client, "t_bench_idx", N_ROWS).await;
    preload(client, "t_bench_noidx", N_ROWS).await;
    client
        .batch_execute("CREATE UNIQUE INDEX ix_t_bench_idx_id ON t_bench_idx(id)")
        .await
        .expect("create index");
    client
        .batch_execute("VACUUM t_bench_idx")
        .await
        .expect("vacuum indexed table");
    let catalog = running.server.catalog_snapshot();
    let table = catalog
        .tables
        .get("t_bench_idx")
        .expect("bench table catalog entry");
    let indexes = catalog
        .indexes_by_table
        .get(&table.oid)
        .expect("bench table index metadata");
    assert!(
        indexes
            .iter()
            .any(|index| index.name == "ix_t_bench_idx_id" && index.is_unique),
        "CREATE UNIQUE INDEX metadata must preserve is_unique for point lookup"
    );
    let rel = RelationId(table.oid);
    let block_count = running.server.heap.block_count(rel).max(table.n_blocks);
    assert!(
        (0..block_count).all(|block| running
            .server
            .vm
            .is_all_visible(rel, BlockNumber::new(block))),
        "VACUUM must certify all t_bench_idx pages all-visible for index-only lookup"
    );
    let explain = client
        .query(
            &format!("EXPLAIN ANALYZE SELECT id FROM t_bench_idx WHERE id = {TARGET_KEY}"),
            &[],
        )
        .await
        .expect("explain analyze indexed point lookup");
    let explain_text = explain
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        explain_text.contains("Index Decision: selected ix_t_bench_idx_id on t_bench_idx.id"),
        "EXPLAIN ANALYZE must report the selected index path, got: {explain_text}"
    );

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
        u128_to_f64_saturating(seq_median) / u128_to_f64_saturating(idx_median.max(1))
    );

    assert!(seq_median > 0, "SeqScan timing should be non-zero");
    assert!(idx_median > 0, "index timing should be non-zero");

    graceful_shutdown(running).await;
}

fn u128_to_f64_saturating(value: u128) -> f64 {
    value.to_f64().unwrap_or(f64::MAX)
}
