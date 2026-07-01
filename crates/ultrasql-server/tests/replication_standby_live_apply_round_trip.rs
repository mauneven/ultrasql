//! Continuous hot-standby round trip: base-backup bring-up + streaming
//! walreceiver + live apply, proven over the wire on two nodes.
//!
//! The standby is brought up from a file copy of the primary's data dir
//! (base-backup bring-up: heap segments, WAL, catalog + authz sidecars),
//! then `run_standby_walreceiver` streams WAL the primary writes AFTER the
//! backup and applies it continuously. A read-only client on the standby
//! must observe the primary's post-backup commits without any restart, and
//! writes on the standby must stay rejected.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ultrasql_server::walreceiver::{PrimaryConnInfo, run_standby_walreceiver};

pub mod support;

use support::{shutdown, start_persistent_server_with_segment_size};

const SEG: u64 = 4096; // small segments so streaming crosses file boundaries

fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) {
    std::fs::create_dir_all(dst).expect("create dest dir");
    for entry in std::fs::read_dir(src).expect("read src dir") {
        let entry = entry.expect("dir entry");
        let ty = entry.file_type().expect("file type");
        let to = dst.join(entry.file_name());
        if ty.is_dir() {
            copy_dir_recursive(&entry.path(), &to);
        } else if ty.is_file() {
            std::fs::copy(entry.path(), &to).expect("copy file");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standby_applies_streamed_commits_and_serves_them_over_the_wire() {
    // The server's panic hook reports through `tracing`; without a
    // subscriber a swallowed panic makes failures silent. Surface them.
    let _ = tracing_subscriber::fmt()
        .with_env_filter("error")
        .try_init();
    // --- Primary: table + one pre-backup row ---
    let primary_dir = tempfile::TempDir::new().expect("primary data dir");
    let primary =
        start_persistent_server_with_segment_size(primary_dir.path(), "live-primary", SEG).await;
    primary
        .client
        .batch_execute(
            "CREATE TABLE live_repl (id INT NOT NULL, note TEXT); \
             INSERT INTO live_repl VALUES (1, 'before-backup');",
        )
        .await
        .expect("primary pre-backup write");
    // Make the pre-backup state durable on disk before the file copy.
    primary
        .client
        .batch_execute("CHECKPOINT")
        .await
        .expect("primary checkpoint");

    // --- Base-backup bring-up: copy the data dir (incl. sidecars + WAL) ---
    let standby_root = tempfile::TempDir::new().expect("standby data dir");
    let standby_dir = standby_root.path().join("data");
    copy_dir_recursive(primary_dir.path(), &standby_dir);
    support::make_data_dir_private(&standby_dir);
    std::fs::write(standby_dir.join("standby.signal"), b"").expect("standby signal");

    // Boot the standby from the copy: recovers the pre-backup state.
    let standby = support::start_configured_server(
        {
            let server = ultrasql_server::Server::init_with_wal_segment_size(&standby_dir, SEG)
                .expect("standby init from base backup");
            server.set_standby_mode(true);
            server
        },
        "live-standby",
    )
    .await;
    let row = standby
        .client
        .query_one("SELECT note FROM live_repl WHERE id = 1", &[])
        .await
        .expect("standby serves the base-backup state");
    assert_eq!(row.get::<_, String>(0), "before-backup");

    // --- Launch the continuous walreceiver exactly as ultrasqld does ---
    let conninfo = PrimaryConnInfo {
        host: primary.bound.ip().to_string(),
        port: primary.bound.port(),
        user: "tester".to_owned(),
        slot: None,
    };
    let stop = Arc::new(AtomicBool::new(false));
    let receiver_state = Arc::clone(&standby.server);
    let receiver_stop = Arc::clone(&stop);
    let wal_dir = standby_dir.join("pg_wal");
    let receiver_thread = std::thread::spawn(move || {
        run_standby_walreceiver(receiver_state, &conninfo, wal_dir, SEG, receiver_stop);
    });

    // --- Primary commits AFTER the standby is up and streaming ---
    primary
        .client
        .batch_execute(
            "INSERT INTO live_repl VALUES (2, 'streamed-live'); \
             INSERT INTO live_repl VALUES (3, 'streamed-live');",
        )
        .await
        .expect("primary post-backup writes");

    // --- The standby must observe them without restart ---
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut live_count: i64 = 0;
    while std::time::Instant::now() < deadline {
        let row = standby
            .client
            .query_one(
                "SELECT COUNT(*) FROM live_repl WHERE note = 'streamed-live'",
                &[],
            )
            .await
            .expect("standby read while streaming");
        live_count = row.get::<_, i64>(0);
        if live_count == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        live_count, 2,
        "standby must apply and serve the primary's post-backup commits"
    );

    // --- The standby stays read-only ---
    let err = standby
        .client
        .batch_execute("INSERT INTO live_repl VALUES (99, 'nope')")
        .await
        .expect_err("standby must reject writes");
    let db = err.as_db_error().expect("db error");
    assert_eq!(db.code().code(), "25006", "read_only_sql_transaction");

    stop.store(true, Ordering::Release);
    // The receiver loop notices `stop` at the next frame/keepalive or
    // reconnect; shutting the primary down closes the stream promptly.
    shutdown(primary).await;
    receiver_thread.join().expect("walreceiver thread joins");
    shutdown(standby).await;
}
