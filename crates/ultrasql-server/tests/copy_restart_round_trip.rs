//! Persistent `COPY FROM` restart coverage through the PostgreSQL wire path.

use std::path::Path;
use std::sync::Arc;

use arrow_array::{BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use bytes::Bytes;
use futures::SinkExt;
use parquet::arrow::ArrowWriter;

pub mod support;

use support::{shutdown, start_persistent_server};

fn sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn write_copy_parquet(path: &Path, rows: &[(i64, &str, f64, bool)]) {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("label", ArrowDataType::Utf8, false),
        ArrowField::new("score", ArrowDataType::Float64, false),
        ArrowField::new("active", ArrowDataType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|(id, _, _, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(_, label, _, _)| *label)
                    .collect::<Vec<&str>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter()
                    .map(|(_, _, score, _)| *score)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                rows.iter()
                    .map(|(_, _, _, active)| *active)
                    .collect::<Vec<_>>(),
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
        .find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row
                .get(0)
                .map(|cell| cell.parse::<i64>().expect("count parses")),
            _ => None,
        })
        .expect("COUNT(*) returned a row")
}

async fn copy_in_payload(client: &tokio_postgres::Client, sql: &str, payload: &[u8]) -> u64 {
    let sink = client
        .copy_in::<_, Bytes>(sql)
        .await
        .expect("copy_in establishes COPY FROM STDIN");
    futures::pin_mut!(sink);
    sink.as_mut()
        .send(Bytes::from(payload.to_vec()))
        .await
        .expect("send CopyData");
    sink.finish().await.expect("finish copy_in")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_from_stdin_rows_survive_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "copy_restart_test").await;
    running
        .client
        .simple_query("CREATE TABLE copy_restart (id INT, label TEXT)")
        .await
        .expect("create table");
    let copied = copy_in_payload(
        &running.client,
        "COPY copy_restart (id, label) FROM STDIN WITH (FORMAT csv)",
        b"1,alpha\n2,bravo\n",
    )
    .await;
    assert_eq!(copied, 2);
    assert_eq!(select_count(&running.client, "copy_restart").await, 2);
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "copy_restart_test").await;
    assert_eq!(select_count(&running.client, "copy_restart").await, 2);
    shutdown(running).await;
}

// Battery #8: a crash mid-transaction (COPY issued, no COMMIT) must leave zero
// rows on restart. The COPYed rows were written under the session xid, which
// has no commit record, so WAL recovery treats them as aborted — they never
// become durable. Pre-fix the COPY committed its OWN autocommit txn, so its
// rows WOULD have survived the crash (durability/atomicity violation).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_in_uncommitted_txn_does_not_survive_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "copy_restart_uncommitted").await;
    running
        .client
        .simple_query("CREATE TABLE copy_crash (id INT, label TEXT)")
        .await
        .expect("create table");
    // Open an explicit transaction and COPY rows, but never COMMIT.
    running.client.simple_query("BEGIN").await.expect("begin");
    let copied = copy_in_payload(
        &running.client,
        "COPY copy_crash (id, label) FROM STDIN WITH (FORMAT csv)",
        b"1,alpha\n2,bravo\n3,charlie\n",
    )
    .await;
    assert_eq!(copied, 3);
    assert_eq!(select_count(&running.client, "copy_crash").await, 3);
    // Simulate a crash: tear the server down with the txn still open (no COMMIT).
    shutdown(running).await;

    // On restart the uncommitted COPY rows are gone; the table itself was
    // created+committed by the autocommit DDL before BEGIN, so it survives.
    let running = start_persistent_server(data_dir.path(), "copy_restart_uncommitted").await;
    assert_eq!(
        select_count(&running.client, "copy_crash").await,
        0,
        "uncommitted in-txn COPY rows must not survive a crash"
    );
    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_from_parquet_rows_survive_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let parquet_dir = tempfile::TempDir::new().unwrap();
    let parquet_path = parquet_dir.path().join("import.parquet");
    write_copy_parquet(
        &parquet_path,
        &[(10, "alpha", 1.5, true), (20, "bravo", 2.5, false)],
    );

    let running = start_persistent_server(data_dir.path(), "copy_restart_test").await;
    running
        .client
        .simple_query(
            "CREATE TABLE parquet_restart (
                id BIGINT,
                label TEXT,
                score DOUBLE,
                active BOOL
            )",
        )
        .await
        .expect("create parquet restart table");
    running
        .client
        .simple_query(&format!(
            "COPY parquet_restart FROM {}",
            sql_string(parquet_path.to_str().expect("utf8 parquet path"))
        ))
        .await
        .expect("copy parquet import");
    assert_eq!(select_count(&running.client, "parquet_restart").await, 2);
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "copy_restart_test").await;
    assert_eq!(select_count(&running.client, "parquet_restart").await, 2);
    shutdown(running).await;
}
