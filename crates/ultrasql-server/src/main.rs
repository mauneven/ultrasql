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
use tracing::{error, info};
use ultrasql_server::{Server, WalArchiveConfig, run_server};

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
    ops_token_from_cli, tls_config_from_cli,
};
use ops::run_ops_endpoint;
use wal_archive::{command_timeout, restore_wal_once_with_timeout, run_wal_archiver_loop};

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    if let Err(e) = init_tracing(&cli.log_level, cli.log_format) {
        eprintln!("ultrasqld: failed to initialise tracing: {e}");
        return std::process::ExitCode::from(1);
    }
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
        if cli.autovacuum_interval_ms > 0 {
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
        if cli.checkpoint_interval_ms > 0 && cli.data_dir.is_some() {
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
        run_server(cli.listen, state).await
    });
    match outcome {
        Ok(()) => std::process::ExitCode::from(0),
        Err(e) => {
            error!(target: "ultrasqld", error = %e, "server terminated with error");
            std::process::ExitCode::from(1)
        }
    }
}
