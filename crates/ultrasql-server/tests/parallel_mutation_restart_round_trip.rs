//! Restart recovery over the parallel WAL-backed bulk-mutation paths.
//!
//! Bulk fused UPDATE and DELETE now run on multiple worker threads even with
//! a WAL sink, appending per-page delta records through an atomically-linked
//! per-transaction chain — and, after a checkpoint, emitting full-page-write
//! records from the workers themselves (torn-page protection used to force
//! these statements onto the sequential path). This round trip proves the
//! whole WAL stream those workers produce is recoverable: mutate AFTER a
//! CHECKPOINT (so worker-emitted FPWs are in the stream), commit, restart,
//! and verify the exact surviving row set.

pub mod support;

use support::{shutdown, start_persistent_server};

/// Rows spread across enough pages (>=128 blocks) that the parallel WAL
/// paths engage rather than falling back to the sequential loop.
const ROWS: i64 = 200_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_bulk_update_and_delete_survive_restart_after_checkpoint() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("error")
        .try_init();
    let data_dir = tempfile::TempDir::new().expect("data dir");
    {
        let running = start_persistent_server(data_dir.path(), "parallel_mutation_restart").await;
        let client = &running.client;

        client
            .batch_execute("SET statement_timeout = 0")
            .await
            .expect("disable timeout for bulk load");
        client
            .batch_execute("CREATE TABLE pm (id INT NOT NULL, val INT)")
            .await
            .expect("create table");
        let mut start = 0;
        while start < ROWS {
            let end = (start + 10_000).min(ROWS);
            let values: Vec<String> = (start..end).map(|i| format!("({i},{i})")).collect();
            client
                .batch_execute(&format!("INSERT INTO pm VALUES {}", values.join(",")))
                .await
                .expect("preload chunk");
            start = end;
        }

        // The checkpoint advances last_checkpoint_lsn, so the parallel bulk
        // mutations below MUST emit worker-side full-page writes for every
        // page they touch — the exact stream this test exists to recover.
        client
            .batch_execute("CHECKPOINT")
            .await
            .expect("checkpoint");

        client
            .batch_execute("UPDATE pm SET val = val + 7 WHERE id < 1000000")
            .await
            .expect("parallel bulk update");
        client
            .batch_execute(&format!("DELETE FROM pm WHERE id < {}", ROWS / 2))
            .await
            .expect("parallel bulk delete");

        shutdown(running).await;
    }

    // Restart: recovery replays worker-emitted FPWs + linked delta records.
    let running = start_persistent_server(data_dir.path(), "parallel_mutation_restarted").await;
    let client = &running.client;

    let row = client
        .query_one("SELECT COUNT(*), MIN(id), MAX(id) FROM pm", &[])
        .await
        .expect("count after restart");
    assert_eq!(row.get::<_, i64>(0), ROWS / 2, "deleted half survives");
    assert_eq!(row.get::<_, i32>(1), i32::try_from(ROWS / 2).expect("fits"));
    assert_eq!(row.get::<_, i32>(2), i32::try_from(ROWS - 1).expect("fits"));

    // Every surviving row carries the committed parallel UPDATE's delta.
    let row = client
        .query_one("SELECT COUNT(*) FROM pm WHERE val <> id + 7", &[])
        .await
        .expect("delta check after restart");
    assert_eq!(
        row.get::<_, i64>(0),
        0,
        "every surviving row must show the committed parallel update"
    );

    shutdown(running).await;
}
