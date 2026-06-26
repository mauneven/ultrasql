//! `COPY FROM` index + CHECK/UNIQUE parity coverage.
//!
//! Closes the corruption-class gap where `COPY FROM` maintained no secondary
//! index and enforced only NOT NULL: a COPY into an indexed table left the
//! index STALE (an index scan missed the COPYed rows), admitted duplicate keys
//! into a UNIQUE index, and skipped CHECK constraints. These tests drive the
//! adversarial battery against a real `tokio-postgres` client and compare the
//! COPY outcome to the equivalent INSERT.
//!
//! Battery:
//! 1. DUP primary key within a COPY batch → whole COPY fails 23505, 0 rows.
//! 2. CHECK violation → 23514, whole COPY aborts.
//! 3. Valid rows → a forced index-scan (`WHERE indexed_col = …`) returns
//!    exactly the heap rows (index not stale); values match an INSERT load.
//! 4. Two duplicate keys within one COPY batch → 23505.
//! 5. UNIQUE against a pre-existing committed row → 23505; against the txn's
//!    own prior in-txn INSERT row → 23505.
//! 6. `BEGIN; COPY (valid); ROLLBACK` → the index has no entries for the
//!    rolled-back rows (a later re-INSERT of the same key succeeds and an
//!    index scan is correct); `BEGIN; COPY; COMMIT` → entries durable +
//!    probe-able after restart.
//! 7. No-index/no-constraint table keeps the bulk fast path (regression); a
//!    large COPY (>4096 rows) into an INDEXED table is correct (all rows
//!    index-probe-able) and atomic on ROLLBACK.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use bytes::Bytes;
use futures::SinkExt;
use parquet::arrow::ArrowWriter;

pub mod support;

use support::{shutdown, start_persistent_server, start_sample_server};

fn sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Build a binary `PGCOPY` payload for a two-column `(INT, INT)` table.
fn binary_int_pair_payload(rows: &[(i32, i32)]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    out.extend_from_slice(&0_i32.to_be_bytes()); // flags
    out.extend_from_slice(&0_i32.to_be_bytes()); // header extension length
    for (id, val) in rows {
        out.extend_from_slice(&2_i16.to_be_bytes()); // field count
        out.extend_from_slice(&4_i32.to_be_bytes()); // field length
        out.extend_from_slice(&id.to_be_bytes());
        out.extend_from_slice(&4_i32.to_be_bytes());
        out.extend_from_slice(&val.to_be_bytes());
    }
    out.extend_from_slice(&(-1_i16).to_be_bytes()); // trailer
    out
}

/// Write a two-column `(id BIGINT, val BIGINT)` parquet file for COPY import.
fn write_int_pair_parquet(path: &std::path::Path, rows: &[(i64, i64)]) {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("val", ArrowDataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, val)| *val).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("parquet record batch");
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("parquet writer");
    writer.write(&batch).expect("write parquet batch");
    writer.close().expect("close parquet writer");
}

async fn select_count(client: &tokio_postgres::Client, table: &str) -> i64 {
    let rows = client
        .simple_query(&format!("SELECT COUNT(*) FROM {table}"))
        .await
        .expect("count query");
    rows.into_iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                row.get(0).map(|c| c.parse::<i64>().expect("count parses"))
            }
            _ => None,
        })
        .expect("COUNT(*) returned a row")
}

/// Stream `payload` into `sql` (a `COPY ... FROM STDIN`) and finish, returning
/// the row count on success or the server error on failure.
async fn copy_in_payload_result(
    client: &tokio_postgres::Client,
    sql: &str,
    payload: &[u8],
) -> Result<u64, tokio_postgres::Error> {
    let sink = client
        .copy_in::<_, Bytes>(sql)
        .await
        .expect("copy_in establishes COPY FROM STDIN");
    futures::pin_mut!(sink);
    sink.as_mut()
        .send(Bytes::from(payload.to_vec()))
        .await
        .expect("send CopyData");
    sink.finish().await
}

async fn copy_in_payload(client: &tokio_postgres::Client, sql: &str, payload: &[u8]) -> u64 {
    copy_in_payload_result(client, sql, payload)
        .await
        .expect("finish copy_in")
}

fn sqlstate(err: &tokio_postgres::Error) -> String {
    err.code()
        .map(|c| c.code().to_owned())
        .unwrap_or_else(|| format!("<no sqlstate: {err}>"))
}

/// Force an index scan with a `WHERE indexed_col = …` point lookup and return
/// the matched rows' `(id, val)` pairs. The planner routes an equality on an
/// indexed column through the index path; if the index were stale the COPYed
/// row would be missing here even though it is present in the heap.
async fn index_probe(
    client: &tokio_postgres::Client,
    table: &str,
    col: &str,
    value: i32,
) -> Vec<(i32, i32)> {
    let rows = client
        .query(
            &format!("SELECT id, val FROM {table} WHERE {col} = $1 ORDER BY id"),
            &[&value],
        )
        .await
        .expect("index point lookup");
    rows.iter()
        .map(|r| (r.get::<_, i32>(0), r.get::<_, i32>(1)))
        .collect()
}

// ── #1 + #2: a DUP primary key / a CHECK violation aborts the whole COPY ──
#[tokio::test]
async fn copy_dup_pk_and_check_violation_abort_whole_copy_autocommit() {
    let running = start_sample_server("copy_idx_dup_check").await;
    let client = &running.client;
    client
        .batch_execute(
            "CREATE TABLE t_dc (id INT PRIMARY KEY, c INT CHECK (c > 0), val INT); \
             CREATE INDEX ix_t_dc_val ON t_dc(val)",
        )
        .await
        .expect("create table + secondary index");

    // #1: a DUP id within one COPY batch → 23505, zero rows land.
    let dup_err = copy_in_payload_result(
        client,
        "COPY t_dc (id, c, val) FROM STDIN WITH (FORMAT csv)",
        b"1,1,10\n2,1,20\n1,1,30\n",
    )
    .await
    .expect_err("duplicate primary key must abort the COPY");
    assert_eq!(
        sqlstate(&dup_err),
        "23505",
        "dup PK in COPY → unique_violation"
    );
    assert_eq!(
        select_count(client, "t_dc").await,
        0,
        "a failed COPY must land zero rows (all-or-nothing)"
    );

    // #2: a CHECK violation (c <= 0) → 23514, whole COPY aborts.
    let check_err = copy_in_payload_result(
        client,
        "COPY t_dc (id, c, val) FROM STDIN WITH (FORMAT csv)",
        b"3,5,30\n4,0,40\n",
    )
    .await
    .expect_err("CHECK violation must abort the COPY");
    assert_eq!(
        sqlstate(&check_err),
        "23514",
        "c<=0 in COPY → check_violation"
    );
    assert_eq!(
        select_count(client, "t_dc").await,
        0,
        "a CHECK-failing COPY must land zero rows"
    );

    shutdown(running).await;
}

// ── #3: valid COPY rows are index-probe-able; values match an INSERT load ──
#[tokio::test]
async fn copy_valid_rows_are_index_probeable_and_match_insert() {
    let running = start_sample_server("copy_idx_probe").await;
    let client = &running.client;
    client
        .batch_execute(
            "CREATE TABLE t_copy (id INT PRIMARY KEY, val INT); \
             CREATE INDEX ix_t_copy_val ON t_copy(val); \
             CREATE TABLE t_insert (id INT PRIMARY KEY, val INT); \
             CREATE INDEX ix_t_insert_val ON t_insert(val)",
        )
        .await
        .expect("create indexed tables");

    let copied = copy_in_payload(
        client,
        "COPY t_copy (id, val) FROM STDIN WITH (FORMAT csv)",
        b"1,100\n2,200\n3,300\n",
    )
    .await;
    assert_eq!(copied, 3);
    client
        .batch_execute("INSERT INTO t_insert VALUES (1,100),(2,200),(3,300)")
        .await
        .expect("equivalent insert load");

    // Primary-key index: the COPYed row is found via the unique index.
    assert_eq!(index_probe(client, "t_copy", "id", 2).await, vec![(2, 200)]);
    // Secondary index on val: the COPYed row is found (index not stale).
    assert_eq!(
        index_probe(client, "t_copy", "val", 300).await,
        vec![(3, 300)]
    );
    // The COPY load matches the INSERT load row-for-row on both index paths.
    assert_eq!(
        index_probe(client, "t_copy", "id", 1).await,
        index_probe(client, "t_insert", "id", 1).await
    );
    assert_eq!(
        index_probe(client, "t_copy", "val", 200).await,
        index_probe(client, "t_insert", "val", 200).await
    );
    assert_eq!(select_count(client, "t_copy").await, 3);

    shutdown(running).await;
}

// ── #4: two duplicate keys within one COPY batch → 23505 ──
#[tokio::test]
async fn copy_duplicate_keys_within_batch_rejected() {
    let running = start_sample_server("copy_idx_dup_batch").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE t_dupbatch (id INT, val INT, UNIQUE (id))")
        .await
        .expect("create unique table");

    let err = copy_in_payload_result(
        client,
        "COPY t_dupbatch (id, val) FROM STDIN WITH (FORMAT csv)",
        b"7,1\n7,2\n",
    )
    .await
    .expect_err("two duplicate keys in one COPY batch must be rejected");
    assert_eq!(sqlstate(&err), "23505");
    assert_eq!(select_count(client, "t_dupbatch").await, 0);

    shutdown(running).await;
}

// ── #4 (cross-batch): a duplicate key that straddles a batch flush boundary ──
// A COPY larger than COPY_INSERT_BATCH_ROWS (4096) is flushed in several
// maintained batches under one txn. A key in an early batch and the same key
// in a later batch must still collide — the later batch's uniqueness recheck
// sees the earlier batch's own-xid rows as live. Zero rows survive.
#[tokio::test]
async fn copy_duplicate_key_across_batch_boundary_rejected() {
    let running = start_sample_server("copy_idx_dup_cross_batch").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE t_xb (id INT PRIMARY KEY, val INT)")
        .await
        .expect("create table");

    // 5000 distinct rows (spans 2 maintained batches), then repeat id 0 — the
    // duplicate lands in a later batch than the original.
    let mut payload = String::new();
    for id in 0..5000 {
        payload.push_str(&format!("{id},{id}\n"));
    }
    payload.push_str("0,9999\n");
    let err = copy_in_payload_result(
        client,
        "COPY t_xb (id, val) FROM STDIN WITH (FORMAT csv)",
        payload.as_bytes(),
    )
    .await
    .expect_err("a cross-batch duplicate key must be rejected");
    assert_eq!(sqlstate(&err), "23505");
    assert_eq!(
        select_count(client, "t_xb").await,
        0,
        "a COPY that fails on a later batch must land zero rows"
    );

    shutdown(running).await;
}

// ── #5: UNIQUE against a committed row AND against the txn's own prior row ──
#[tokio::test]
async fn copy_unique_against_committed_and_in_txn_rows() {
    let running = start_sample_server("copy_idx_unique_existing").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE t_uq (id INT PRIMARY KEY, val INT)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t_uq VALUES (1, 10)")
        .await
        .expect("seed committed row");

    // Against a pre-existing committed row.
    let err = copy_in_payload_result(
        client,
        "COPY t_uq (id, val) FROM STDIN WITH (FORMAT csv)",
        b"1,99\n",
    )
    .await
    .expect_err("COPY of an existing committed key must be rejected");
    assert_eq!(sqlstate(&err), "23505");
    assert_eq!(select_count(client, "t_uq").await, 1);

    // Against the txn's own prior in-txn INSERT row.
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("INSERT INTO t_uq VALUES (2, 20)")
        .await
        .expect("in-txn insert");
    let in_txn_err = copy_in_payload_result(
        client,
        "COPY t_uq (id, val) FROM STDIN WITH (FORMAT csv)",
        b"2,21\n",
    )
    .await
    .expect_err("COPY conflicting with the txn's own prior INSERT must be rejected");
    assert_eq!(sqlstate(&in_txn_err), "23505");
    client.batch_execute("ROLLBACK").await.expect("rollback");
    assert_eq!(select_count(client, "t_uq").await, 1);

    shutdown(running).await;
}

// ── #6: in-txn ROLLBACK leaves NO index entries; COMMIT makes them durable ──
#[tokio::test]
async fn copy_rollback_leaves_no_index_entries_then_reinsert_is_correct() {
    let running = start_sample_server("copy_idx_rollback").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE t_rb (id INT PRIMARY KEY, val INT)")
        .await
        .expect("create table");

    client.batch_execute("BEGIN").await.expect("begin");
    let copied = copy_in_payload(
        client,
        "COPY t_rb (id, val) FROM STDIN WITH (FORMAT csv)",
        b"1,10\n2,20\n",
    )
    .await;
    assert_eq!(copied, 2);
    client.batch_execute("ROLLBACK").await.expect("rollback");
    assert_eq!(
        select_count(client, "t_rb").await,
        0,
        "ROLLBACK must discard COPYed rows"
    );

    // The rolled-back index entries must NOT fabricate a unique conflict: a
    // fresh INSERT of the same key succeeds, and the index scan is correct.
    client
        .batch_execute("INSERT INTO t_rb VALUES (1, 111)")
        .await
        .expect("re-insert of rolled-back key must succeed (no stale unique entry)");
    assert_eq!(index_probe(client, "t_rb", "id", 1).await, vec![(1, 111)]);
    assert_eq!(select_count(client, "t_rb").await, 1);

    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_commit_index_entries_durable_after_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "copy_idx_commit_restart").await;
    running
        .client
        .batch_execute(
            "CREATE TABLE t_commit_idx (id INT PRIMARY KEY, val INT); \
             CREATE INDEX ix_t_commit_idx_val ON t_commit_idx(val)",
        )
        .await
        .expect("create indexed table");
    running.client.batch_execute("BEGIN").await.expect("begin");
    let copied = copy_in_payload(
        &running.client,
        "COPY t_commit_idx (id, val) FROM STDIN WITH (FORMAT csv)",
        b"1,10\n2,20\n3,30\n",
    )
    .await;
    assert_eq!(copied, 3);
    running
        .client
        .batch_execute("COMMIT")
        .await
        .expect("commit");
    // Probe-able immediately after commit.
    assert_eq!(
        index_probe(&running.client, "t_commit_idx", "val", 20).await,
        vec![(2, 20)]
    );
    shutdown(running).await;

    // Probe-able after a restart (entries durable).
    let running = start_persistent_server(data_dir.path(), "copy_idx_commit_restart").await;
    assert_eq!(select_count(&running.client, "t_commit_idx").await, 3);
    assert_eq!(
        index_probe(&running.client, "t_commit_idx", "id", 3).await,
        vec![(3, 30)]
    );
    assert_eq!(
        index_probe(&running.client, "t_commit_idx", "val", 10).await,
        vec![(1, 10)]
    );
    shutdown(running).await;
}

// ── #7a: a large (>4096 rows) COPY into an INDEXED table is correct + atomic ──
#[tokio::test]
async fn copy_large_indexed_all_probeable_and_atomic_on_rollback() {
    let running = start_sample_server("copy_idx_large").await;
    let client = &running.client;
    client
        .batch_execute(
            "CREATE TABLE t_big (id INT PRIMARY KEY, val INT); \
             CREATE INDEX ix_t_big_val ON t_big(val)",
        )
        .await
        .expect("create indexed table");

    // 5000 rows > COPY_INSERT_BATCH_ROWS (4096): spans multiple flushed batches.
    const N: i32 = 5000;
    let mut payload = String::with_capacity(N as usize * 10);
    for id in 0..N {
        payload.push_str(&format!("{id},{}\n", id * 2));
    }
    let copied = copy_in_payload(
        client,
        "COPY t_big (id, val) FROM STDIN WITH (FORMAT csv)",
        payload.as_bytes(),
    )
    .await;
    assert_eq!(copied, u64::try_from(N).unwrap());
    assert_eq!(select_count(client, "t_big").await, i64::from(N));
    // Rows from the first batch, a batch boundary, and the last batch are all
    // index-probe-able (the index is not stale across batch flushes).
    assert_eq!(index_probe(client, "t_big", "id", 0).await, vec![(0, 0)]);
    assert_eq!(
        index_probe(client, "t_big", "id", 4096).await,
        vec![(4096, 8192)]
    );
    assert_eq!(
        index_probe(client, "t_big", "id", 4999).await,
        vec![(4999, 9998)]
    );
    assert_eq!(
        index_probe(client, "t_big", "val", 8192).await,
        vec![(4096, 8192)]
    );

    // A large COPY into the SAME indexed table is atomic on ROLLBACK.
    client.batch_execute("BEGIN").await.expect("begin");
    let mut more = String::new();
    for id in N..(N + N) {
        more.push_str(&format!("{id},{}\n", id * 2));
    }
    let copied = copy_in_payload(
        client,
        "COPY t_big (id, val) FROM STDIN WITH (FORMAT csv)",
        more.as_bytes(),
    )
    .await;
    assert_eq!(copied, u64::try_from(N).unwrap());
    client.batch_execute("ROLLBACK").await.expect("rollback");
    assert_eq!(
        select_count(client, "t_big").await,
        i64::from(N),
        "ROLLBACK must discard the second large COPY entirely"
    );
    // A rolled-back key re-inserts cleanly (no stale unique entry).
    client
        .batch_execute(&format!("INSERT INTO t_big VALUES ({N}, 777)"))
        .await
        .expect("re-insert of rolled-back key succeeds");
    assert_eq!(index_probe(client, "t_big", "id", N).await, vec![(N, 777)]);

    shutdown(running).await;
}

// ── #7b: an unindexed/unconstrained table still works (fast-path regression) ──
#[tokio::test]
async fn copy_unindexed_table_still_round_trips() {
    let running = start_sample_server("copy_idx_plain").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE t_plain (id INT, label TEXT)")
        .await
        .expect("create plain table");
    let copied = copy_in_payload(
        client,
        "COPY t_plain (id, label) FROM STDIN WITH (FORMAT csv)",
        b"1,alpha\n2,bravo\n3,charlie\n",
    )
    .await;
    assert_eq!(copied, 3);
    assert_eq!(select_count(client, "t_plain").await, 3);

    shutdown(running).await;
}

// ── #6 (formats): every COPY format reaches the shared maintained flush point ──

// Binary STDIN into an indexed table: rows are index-probe-able and a dup key
// is rejected (23505) — proving the binary decode path is maintained.
#[tokio::test]
async fn copy_binary_stdin_into_indexed_table_is_maintained() {
    let running = start_sample_server("copy_idx_binary").await;
    let client = &running.client;
    client
        .batch_execute(
            "CREATE TABLE t_bin_idx (id INT PRIMARY KEY, val INT); \
             CREATE INDEX ix_t_bin_idx_val ON t_bin_idx(val)",
        )
        .await
        .expect("create indexed table");

    let copied = copy_in_payload(
        client,
        "COPY t_bin_idx (id, val) FROM STDIN WITH (FORMAT binary)",
        &binary_int_pair_payload(&[(1, 10), (2, 20)]),
    )
    .await;
    assert_eq!(copied, 2);
    assert_eq!(
        index_probe(client, "t_bin_idx", "id", 2).await,
        vec![(2, 20)]
    );
    assert_eq!(
        index_probe(client, "t_bin_idx", "val", 10).await,
        vec![(1, 10)]
    );

    // A binary COPY with a dup primary key is rejected, just like text/csv.
    let err = copy_in_payload_result(
        client,
        "COPY t_bin_idx (id, val) FROM STDIN WITH (FORMAT binary)",
        &binary_int_pair_payload(&[(1, 99)]),
    )
    .await
    .expect_err("binary COPY of an existing key must be rejected");
    assert_eq!(sqlstate(&err), "23505");
    assert_eq!(select_count(client, "t_bin_idx").await, 2);

    shutdown(running).await;
}

// Server-side file CSV into an indexed table: maintained + dup-rejected.
#[tokio::test]
async fn copy_file_into_indexed_table_is_maintained() {
    let dir = tempfile::tempdir().expect("tempdir");
    let csv_path = dir.path().join("rows.csv");
    std::fs::write(&csv_path, "1,10\n2,20\n3,30\n").expect("write csv");
    let dup_path = dir.path().join("dup.csv");
    std::fs::write(&dup_path, "2,99\n").expect("write dup csv");

    let running = start_sample_server("copy_idx_file").await;
    let client = &running.client;
    client
        .batch_execute(
            "CREATE TABLE t_file_idx (id INT PRIMARY KEY, val INT); \
             CREATE INDEX ix_t_file_idx_val ON t_file_idx(val)",
        )
        .await
        .expect("create indexed table");

    client
        .batch_execute(&format!(
            "COPY t_file_idx (id, val) FROM {} WITH (FORMAT csv)",
            sql_string(csv_path.to_str().unwrap())
        ))
        .await
        .expect("file COPY into indexed table");
    assert_eq!(select_count(client, "t_file_idx").await, 3);
    assert_eq!(
        index_probe(client, "t_file_idx", "id", 3).await,
        vec![(3, 30)]
    );
    assert_eq!(
        index_probe(client, "t_file_idx", "val", 20).await,
        vec![(2, 20)]
    );

    let err = client
        .batch_execute(&format!(
            "COPY t_file_idx (id, val) FROM {} WITH (FORMAT csv)",
            sql_string(dup_path.to_str().unwrap())
        ))
        .await
        .expect_err("file COPY of an existing key must be rejected");
    assert_eq!(sqlstate(&err), "23505");
    assert_eq!(select_count(client, "t_file_idx").await, 3);

    shutdown(running).await;
}

// Parquet file into an indexed table: maintained + dup-rejected.
#[tokio::test]
async fn copy_parquet_into_indexed_table_is_maintained() {
    let dir = tempfile::tempdir().expect("tempdir");
    let parquet_path = dir.path().join("import.parquet");
    write_int_pair_parquet(&parquet_path, &[(1, 10), (2, 20), (3, 30)]);
    let dup_path = dir.path().join("dup.parquet");
    write_int_pair_parquet(&dup_path, &[(2, 99)]);

    let running = start_sample_server("copy_idx_parquet").await;
    let client = &running.client;
    client
        .batch_execute(
            "CREATE TABLE t_parq_idx (id BIGINT PRIMARY KEY, val BIGINT); \
             CREATE INDEX ix_t_parq_idx_val ON t_parq_idx(val)",
        )
        .await
        .expect("create indexed table");

    client
        .batch_execute(&format!(
            "COPY t_parq_idx FROM {}",
            sql_string(parquet_path.to_str().unwrap())
        ))
        .await
        .expect("parquet COPY into indexed table");
    assert_eq!(select_count(client, "t_parq_idx").await, 3);
    let row = client
        .query_one("SELECT id, val FROM t_parq_idx WHERE id = $1", &[&3_i64])
        .await
        .expect("parquet row index-probe");
    assert_eq!(
        (row.get::<_, i64>(0), row.get::<_, i64>(1)),
        (3_i64, 30_i64)
    );

    let err = client
        .batch_execute(&format!(
            "COPY t_parq_idx FROM {}",
            sql_string(dup_path.to_str().unwrap())
        ))
        .await
        .expect_err("parquet COPY of an existing key must be rejected");
    assert_eq!(sqlstate(&err), "23505");
    assert_eq!(select_count(client, "t_parq_idx").await, 3);

    shutdown(running).await;
}
