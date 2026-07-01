//! Round-trip coverage for the runtime-settable statement-logging GUCs.
//!
//! `log_statement` (`none` | `ddl` | `mod` | `all`) and
//! `log_min_duration_statement` (milliseconds, `-1` disables) start at
//! the server-config values and are settable per session via
//! `SET` / `SHOW` / `RESET`, mirroring how `statement_timeout` inherits
//! a server default. The statement-logging call site consults the
//! session-effective values, so a `SET log_statement = 'all'` takes
//! effect immediately for that session only.

pub mod support;

use std::io::Write;
use std::sync::{Arc, Mutex, OnceLock};

use support::{connect_as, shutdown, start_configured_server};
use tokio_postgres::error::SqlState;
use ultrasql_server::{LogStatementMode, LoggingConfig, Server};

/// A `MakeWriter` that funnels the global tracing output into a shared
/// in-memory buffer so a test can assert on emitted statement-log lines.
#[derive(Clone, Default)]
struct SharedLogBuffer(Arc<Mutex<Vec<u8>>>);

impl SharedLogBuffer {
    fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("log buffer lock")).into_owned()
    }
}

impl Write for SharedLogBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("log buffer lock")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedLogBuffer {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Install (once per test binary) a global tracing subscriber that
/// captures INFO-level output — including the `ultrasqld::statement`
/// target — into a shared buffer.
fn install_log_capture() -> SharedLogBuffer {
    static CAPTURE: OnceLock<SharedLogBuffer> = OnceLock::new();
    CAPTURE
        .get_or_init(|| {
            let buffer = SharedLogBuffer::default();
            let subscriber = tracing_subscriber::fmt()
                .with_max_level(tracing::Level::INFO)
                .with_writer(buffer.clone())
                .finish();
            let _ = tracing::subscriber::set_global_default(subscriber);
            buffer
        })
        .clone()
}

fn server_with_logging_config(data_dir: &std::path::Path, config: LoggingConfig) -> Server {
    let mut server = Server::init(data_dir).expect("persistent server init");
    server.logging_config = config;
    server
}

async fn show_one(client: &tokio_postgres::Client, name: &str) -> String {
    let row = client
        .query_one(&format!("SHOW {name}"), &[])
        .await
        .unwrap_or_else(|e| panic!("SHOW {name}: {e}"));
    row.get::<_, String>(0)
}

/// SET / SHOW / RESET for both logging GUCs: sessions inherit the
/// server-config defaults, overrides are session-scoped, and RESET (or
/// `SET ... = DEFAULT`) restores the server default — not the built-in
/// `none` / `-1`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logging_gucs_set_show_reset_round_trip() {
    let _capture = install_log_capture();
    let data_dir = tempfile::TempDir::new().unwrap();
    let server = server_with_logging_config(
        data_dir.path(),
        LoggingConfig {
            log_connections: false,
            log_min_duration_statement_ms: 250,
            log_statement: LogStatementMode::Ddl,
        },
    );
    let running = start_configured_server(server, "logging_gucs").await;
    let client = &running.client;

    // Sessions start at the server-config values.
    assert_eq!(show_one(client, "log_statement").await, "ddl");
    assert_eq!(show_one(client, "log_min_duration_statement").await, "250");

    // Runtime SET takes effect and SHOW reads it back.
    client
        .batch_execute("SET log_statement = 'all'")
        .await
        .expect("SET log_statement");
    assert_eq!(show_one(client, "log_statement").await, "all");
    client
        .batch_execute("SET log_min_duration_statement = 0")
        .await
        .expect("SET log_min_duration_statement");
    assert_eq!(show_one(client, "log_min_duration_statement").await, "0");

    // The session keeps working with every-statement logging armed.
    let row = client
        .query_one("SELECT 1 + 1", &[])
        .await
        .expect("statement executes with logging armed");
    assert_eq!(row.get::<_, i32>(0), 2);

    // pg_settings reports the session-effective value with PostgreSQL's
    // `superuser` context for both GUCs.
    let row = client
        .query_one(
            "SELECT setting, context FROM pg_settings WHERE name = 'log_statement'",
            &[],
        )
        .await
        .expect("pg_settings row for log_statement");
    assert_eq!(row.get::<_, String>(0), "all");
    assert_eq!(row.get::<_, String>(1), "superuser");

    // Overrides are session-scoped: a second connection still sees the
    // server-config defaults.
    let (other, other_conn) = connect_as(running.bound, "tester", "logging_gucs_other").await;
    assert_eq!(show_one(&other, "log_statement").await, "ddl");
    assert_eq!(show_one(&other, "log_min_duration_statement").await, "250");
    drop(other);
    other_conn.await.expect("other session joins");

    // RESET restores the server-config default, not the built-in default.
    client
        .batch_execute("RESET log_statement")
        .await
        .expect("RESET log_statement");
    assert_eq!(show_one(client, "log_statement").await, "ddl");
    client
        .batch_execute("SET log_min_duration_statement = DEFAULT")
        .await
        .expect("SET log_min_duration_statement DEFAULT");
    assert_eq!(show_one(client, "log_min_duration_statement").await, "250");

    // Invalid values are rejected and leave the session value untouched.
    let err = client
        .batch_execute("SET log_statement = 'verbose'")
        .await
        .expect_err("invalid log_statement class");
    assert_eq!(
        err.as_db_error().expect("db error").code(),
        &SqlState::FEATURE_NOT_SUPPORTED
    );
    let err = client
        .batch_execute("SET log_min_duration_statement = -5")
        .await
        .expect_err("below -1 is invalid");
    assert_eq!(
        err.as_db_error().expect("db error").code(),
        &SqlState::FEATURE_NOT_SUPPORTED
    );
    assert_eq!(show_one(client, "log_statement").await, "ddl");
    assert_eq!(show_one(client, "log_min_duration_statement").await, "250");

    shutdown(running).await;
}

/// With the server default at `none`/`-1` (nothing logged), a session
/// `SET log_statement = 'all'` makes the very next statement appear in
/// the statement log; the marker was absent before the SET.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_log_statement_all_makes_statements_reach_the_log_sink() {
    let capture = install_log_capture();
    let data_dir = tempfile::TempDir::new().unwrap();
    let server = server_with_logging_config(data_dir.path(), LoggingConfig::default());
    let running = start_configured_server(server, "logging_gucs_sink").await;
    let client = &running.client;

    client
        .query_one("SELECT 1 AS logging_probe_before", &[])
        .await
        .expect("probe before SET");
    assert!(
        !capture.contents().contains("logging_probe_before"),
        "server default log_statement=none must not log SELECTs"
    );

    client
        .batch_execute("SET log_statement = 'all'")
        .await
        .expect("SET log_statement");
    client
        .query_one("SELECT 1 AS logging_probe_after", &[])
        .await
        .expect("probe after SET");
    assert!(
        capture.contents().contains("logging_probe_after"),
        "session-level log_statement=all must log the statement; captured: {}",
        capture.contents()
    );

    shutdown(running).await;
}
