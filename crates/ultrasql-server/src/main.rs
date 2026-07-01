//! `ultrasqld` — UltraSQL database server.
//!
//! Binary entry point. Parses CLI arguments, initializes structured
//! logging, builds a Tokio runtime, optionally boots a WAL-backed data
//! directory, and runs the connection accept loop until shutdown.
//!
//! The actual session logic lives in the [`ultrasql_server`] library
//! crate so it can be exercised by unit tests against an in-memory
//! duplex stream as well as by integration tests over a real TCP
//! socket.
//!
//! The binary's own support code is split into sibling modules under
//! `main_support/`: [`cli`] (argument struct), [`config`] (CLI →
//! typed config translation and startup wiring), [`wal_archive`]
//! (background archive/restore orchestration), and [`ops`] (the HTTP
//! operations endpoint).

// Panic hardening: production (non-test) server-binary code must not
// `.unwrap()`, `.expect()`, or `panic!`. Fallible sites propagate errors;
// proven invariants carry a per-site `#[allow]` with an `// INVARIANT:`
// justification.
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

use std::sync::Arc;

use clap::Parser;
use tracing::{error, info, warn};
use ultrasql_server::{
    Server, WalArchiveConfig, bind_listener, serve_listener_with_graceful_shutdown,
};

#[path = "main_support/cli.rs"]
mod cli;
#[path = "main_support/config.rs"]
mod config;
#[path = "main_support/ops.rs"]
mod ops;
#[path = "main_support/wal_archive.rs"]
mod wal_archive;

#[cfg(test)]
#[path = "main_support/tests/mod.rs"]
mod tests;

use cli::Cli;
use config::{
    apply_auth_config, apply_startup_signal_files, apply_tls_config, auth_config_from_cli,
    autovacuum_config_from_cli, init_tracing, listen_security_from_cli, logging_config_from_cli,
    ops_token_from_cli, resolve_primary_conninfo, tls_config_from_cli, wal_sync_method_from_cli,
};
use ops::run_ops_endpoint;
use wal_archive::{command_timeout, restore_wal_once_with_timeout, run_wal_archiver_loop};

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    if let Err(e) = init_tracing(&cli.log_level, cli.log_format) {
        eprintln!("ultrasqld: failed to initialise tracing: {e}");
        return std::process::ExitCode::from(1);
    }
    // Log every panic with full detail before the unwind proceeds. With
    // panic=unwind (Cargo.toml) the per-statement catch_unwind in the session
    // loop then isolates a query panic to its own connection.
    ultrasql_server::install_panic_hook();
    let autovacuum_config = match autovacuum_config_from_cli(&cli) {
        Ok(config) => config,
        Err(e) => {
            error!(target: "ultrasqld", error = %e, "invalid autovacuum configuration");
            return std::process::ExitCode::from(1);
        }
    };
    let logging_config = match logging_config_from_cli(&cli) {
        Ok(config) => config,
        Err(e) => {
            error!(target: "ultrasqld", error = %e, "invalid logging configuration");
            return std::process::ExitCode::from(1);
        }
    };
    let auth_config = match auth_config_from_cli(&cli) {
        Ok(config) => config,
        Err(e) => {
            error!(target: "ultrasqld", error = %e, "invalid auth configuration");
            return std::process::ExitCode::from(1);
        }
    };
    let tls_config = match tls_config_from_cli(&cli) {
        Ok(config) => config,
        Err(e) => {
            error!(target: "ultrasqld", error = %e, "invalid TLS configuration");
            return std::process::ExitCode::from(1);
        }
    };
    if let Err(e) = listen_security_from_cli(&cli) {
        error!(target: "ultrasqld", error = %e, "invalid listener security configuration");
        return std::process::ExitCode::from(1);
    }
    let ops_token = match ops_token_from_cli(&cli) {
        Ok(token) => token,
        Err(e) => {
            error!(target: "ultrasqld", error = %e, "invalid ops token configuration");
            return std::process::ExitCode::from(1);
        }
    };
    // Configure the process-global durability sync primitive before any WAL
    // or storage file is opened so every flush uses the configured method.
    match wal_sync_method_from_cli(&cli) {
        Ok(method) => {
            ultrasql_core::fsync::set_wal_sync_method(method);
            info!(target: "ultrasqld", wal_sync_method = method.as_str(), "durability sync method configured");
        }
        Err(e) => {
            error!(target: "ultrasqld", error = %e, "invalid wal_sync_method configuration");
            return std::process::ExitCode::from(1);
        }
    }
    let wal_archive_config = WalArchiveConfig {
        archive_command: cli.archive_command.clone().unwrap_or_default(),
        restore_command: cli.restore_command.clone().unwrap_or_default(),
    };

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            error!(target: "ultrasqld", error = %e, "failed to build tokio runtime");
            return std::process::ExitCode::from(1);
        }
    };

    let state = match &cli.data_dir {
        Some(path) => {
            if let Some(command) = cli
                .restore_command
                .as_deref()
                .filter(|command| !command.trim().is_empty())
            {
                let timeout = command_timeout(cli.restore_command_timeout_ms);
                match restore_wal_once_with_timeout(
                    path,
                    command,
                    cli.restore_max_segments,
                    timeout,
                ) {
                    Ok(restored) if restored > 0 => {
                        info!(target: "ultrasqld", restored, data_dir = %path.display(), "restored archived WAL before startup recovery");
                    }
                    Ok(_) => {}
                    Err(e) => {
                        error!(target: "ultrasqld", error = %e, data_dir = %path.display(), "WAL restore failed");
                        return std::process::ExitCode::from(1);
                    }
                }
            }
            let init_result = if cli.wal_segment_size_bytes > 0 {
                Server::init_with_wal_segment_size(path, cli.wal_segment_size_bytes)
            } else {
                Server::init(path)
            };
            match init_result {
                Ok(mut server) => {
                    server.set_autovacuum_config(autovacuum_config);
                    server.set_logging_config(logging_config);
                    server.set_idle_session_timeout_ms(cli.idle_session_timeout_ms);
                    server.set_default_statement_timeout_ms(cli.statement_timeout_ms);
                    server.set_memory_ceiling_bytes(cli.memory_ceiling_bytes);
                    server.set_wal_archive_config(wal_archive_config.clone());
                    server = apply_auth_config(server, &auth_config);
                    server = apply_tls_config(server, &tls_config);
                    Arc::new(server)
                }
                Err(e) => {
                    error!(target: "ultrasqld", error = %e, data_dir = %path.display(), "server init failed");
                    return std::process::ExitCode::from(1);
                }
            }
        }
        None => {
            let mut server = Server::with_sample_database();
            server.set_autovacuum_config(autovacuum_config);
            server.set_logging_config(logging_config);
            server.set_idle_session_timeout_ms(cli.idle_session_timeout_ms);
            server.set_default_statement_timeout_ms(cli.statement_timeout_ms);
            server.set_memory_ceiling_bytes(cli.memory_ceiling_bytes);
            server.set_wal_archive_config(wal_archive_config);
            server = apply_auth_config(server, &auth_config);
            server = apply_tls_config(server, &tls_config);
            Arc::new(server)
        }
    };
    if let Some(path) = &cli.data_dir {
        if apply_startup_signal_files(state.as_ref(), path) {
            info!(target: "ultrasqld", data_dir = %path.display(), "hot standby read-only mode enabled");
        }
    }
    // Standby streaming: with standby mode enabled and a primary_conninfo
    // (flag/env, or a `primary_conninfo` file in the data dir), launch the
    // continuous walreceiver so this standby stays current on its own. Runs
    // on a dedicated OS thread: WAL apply is blocking heap/commit-status I/O.
    let walreceiver_shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
    if state.is_standby_mode()
        && let Some(path) = &cli.data_dir
    {
        match resolve_primary_conninfo(&cli, path) {
            Ok(Some(conninfo)) => {
                let receiver_state = Arc::clone(&state);
                let wal_dir = path.join("pg_wal");
                let segment_size_bytes = if cli.wal_segment_size_bytes > 0 {
                    cli.wal_segment_size_bytes
                } else {
                    ultrasql_wal::WalWriterConfig::default().segment_size_bytes
                };
                let shutdown = Arc::clone(&walreceiver_shutdown);
                info!(
                    target: "ultrasqld",
                    primary = %format!("{}:{}", conninfo.host, conninfo.port),
                    "standby walreceiver configured; streaming from primary"
                );
                std::thread::Builder::new()
                    .name("ultrasql-walreceiver".to_owned())
                    .spawn(move || {
                        ultrasql_server::walreceiver::run_standby_walreceiver(
                            receiver_state,
                            &conninfo,
                            wal_dir,
                            segment_size_bytes,
                            shutdown,
                        );
                    })
                    .ok();
            }
            Ok(None) => {
                info!(
                    target: "ultrasqld",
                    "standby has no primary_conninfo; serving the recovered state without streaming"
                );
            }
            Err(e) => {
                error!(target: "ultrasqld", error = %e, "invalid primary_conninfo configuration");
                return std::process::ExitCode::from(1);
            }
        }
    }
    let outcome = runtime.block_on(async move {
        if let Some(ops_addr) = cli.ops_listen {
            let pg_addr = cli.listen;
            let ops_state = Arc::clone(&state);
            let ops_token = ops_token.clone();
            tokio::spawn(async move {
                if let Err(e) = run_ops_endpoint(ops_addr, pg_addr, ops_state, ops_token).await {
                    error!(target: "ultrasqld", error = %e, "ops endpoint terminated");
                }
            });
        }
        if cli.autovacuum_interval_ms > 0 && !state.is_standby_mode() {
            let autovacuum_state = Arc::clone(&state);
            let interval = std::time::Duration::from_millis(cli.autovacuum_interval_ms);
            tokio::spawn(async move {
                // Space cycles by `interval` AFTER each completes (not between
                // starts), so a slow vacuum never queues a back-to-back run.
                loop {
                    tokio::time::sleep(interval).await;
                    let server = Arc::clone(&autovacuum_state);
                    // An autovacuum cycle does blocking heap/buffer/WAL IO; run
                    // it off the async reactor so it never stalls connection
                    // handling.
                    if let Err(e) =
                        tokio::task::spawn_blocking(move || server.run_autovacuum_cycle()).await
                    {
                        error!(target: "ultrasqld", error = %e, "automatic autovacuum task panicked");
                    }
                }
            });
        }
        // A standby must not append LOCAL WAL: a checkpoint barrier record or
        // autovacuum WAL would diverge its byte stream from the primary's and
        // corrupt the landed-WAL invariant the walreceiver relies on. Both
        // background writers are therefore gated off in standby mode
        // (PostgreSQL standbys likewise run neither autovacuum nor normal
        // checkpoints); standby WAL is truncated on promotion/restart instead.
        if cli.checkpoint_interval_ms > 0 && cli.data_dir.is_some() && !state.is_standby_mode() {
            let checkpoint_state = Arc::clone(&state);
            let interval = std::time::Duration::from_millis(cli.checkpoint_interval_ms);
            tokio::spawn(async move {
                // Space cycles by `interval` AFTER each completes (not between
                // starts), so a slow checkpoint never queues a back-to-back run.
                loop {
                    tokio::time::sleep(interval).await;
                    let server = Arc::clone(&checkpoint_state);
                    // A full checkpoint does blocking fsync/file IO; run it off
                    // the async reactor so it never stalls connection handling.
                    if let Err(e) =
                        tokio::task::spawn_blocking(move || server.run_checkpoint_cycle()).await
                    {
                        error!(target: "ultrasqld", error = %e, "automatic checkpoint task panicked");
                    }
                }
            });
        }
        if let (Some(data_dir), Some(command)) = (
            cli.data_dir.clone(),
            cli.archive_command
                .clone()
                .filter(|command| !command.trim().is_empty()),
        ) {
            let interval_ms = cli.archive_interval_ms;
            let timeout = command_timeout(cli.archive_command_timeout_ms);
            tokio::spawn(async move {
                run_wal_archiver_loop(data_dir, command, interval_ms, timeout).await;
            });
        }
        run_server_with_signals(cli.listen, state, cli.shutdown_drain_timeout_ms).await
    });
    // Stop the standby walreceiver thread (if any) before exiting so it does
    // not race process teardown mid-landing.
    walreceiver_shutdown.store(true, std::sync::atomic::Ordering::Release);
    match outcome {
        Ok(()) => std::process::ExitCode::from(0),
        Err(e) => {
            error!(target: "ultrasqld", error = %e, "server terminated with error");
            std::process::ExitCode::from(1)
        }
    }
}

/// Bind the PostgreSQL listener and serve it until a shutdown signal,
/// then drain in-flight sessions gracefully.
///
/// Installs SIGTERM and SIGINT (Ctrl-C) handlers. The first signal stops
/// the accept loop and starts draining; in-flight sessions get up to
/// `drain_timeout_ms` to finish. A second signal (or the deadline) aborts
/// whatever remains so the process exits promptly. On a clean return the
/// `Server` `Arc` is dropped, which flushes the WAL / shuts the
/// checkpointer down.
async fn run_server_with_signals(
    addr: std::net::SocketAddr,
    state: Arc<Server>,
    drain_timeout_ms: u64,
) -> Result<(), ultrasql_server::ServerError> {
    let (listener, bound) = bind_listener(addr).await?;
    info!(target: "ultrasqld", listen = %bound, "ultrasqld is ready");

    // A shared signal pump increments a counter on each SIGTERM/SIGINT and
    // notifies both gates. The first signal trips `begin`; the second trips
    // `force`. Using a `tokio::sync::Notify` per gate keeps the futures
    // cheap to await inside `serve_listener_with_graceful_shutdown`.
    let begin = Arc::new(tokio::sync::Notify::new());
    let force = Arc::new(tokio::sync::Notify::new());
    let begin_pump = Arc::clone(&begin);
    let force_pump = Arc::clone(&force);
    tokio::spawn(async move {
        // SIGINT (Ctrl-C) and SIGTERM (kill / k8s) both initiate shutdown.
        // `ctrl_c` is portable; SIGTERM is registered via the unix-specific
        // signal stream. On non-unix targets only ctrl_c is wired.
        #[cfg(unix)]
        let mut sigterm =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    error!(target: "ultrasqld", error = %e, "failed to install SIGTERM handler");
                    return;
                }
            };
        let mut count: u32 = 0;
        loop {
            #[cfg(unix)]
            {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = sigterm.recv() => {}
                }
            }
            #[cfg(not(unix))]
            {
                if tokio::signal::ctrl_c().await.is_err() {
                    return;
                }
            }
            count += 1;
            match count {
                1 => {
                    info!(target: "ultrasqld", "shutdown signal received; draining connections");
                    // `notify_one` latches a permit even if no waiter is
                    // registered yet, so the gate cannot miss the signal.
                    begin_pump.notify_one();
                }
                _ => {
                    warn!(target: "ultrasqld", "second shutdown signal received; forcing immediate shutdown");
                    force_pump.notify_one();
                    return;
                }
            }
        }
    });

    let begin_shutdown = async move { begin.notified().await };
    let force_shutdown = async move { force.notified().await };
    serve_listener_with_graceful_shutdown(
        listener,
        state,
        begin_shutdown,
        force_shutdown,
        std::time::Duration::from_millis(drain_timeout_ms),
    )
    .await
}
