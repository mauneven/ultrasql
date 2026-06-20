//! `ultrasql` — UltraSQL command-line client.
//!
//! Connects to an `ultrasqld` instance over the PostgreSQL wire protocol
//! and provides an interactive REPL plus a script-execution mode. Backslash
//! commands cover a useful subset of psql.
//!
//! # Connection precedence
//!
//! 1. Explicit flags (`--host`, `--port`, `--user`, `--dbname`, `--password`).
//! 2. `postgresql://` URL supplied via `--url` or as the first positional.
//! 3. `PGHOST`, `PGPORT`, `PGUSER`, `PGDATABASE`, `PGPASSWORD` environment variables.
//! 4. `~/.pgpass` file (host:port:database:user:password lines, `*` wildcards).
//! 5. Built-in defaults: localhost:5432, username = current OS user, dbname = username.

mod cli_support;

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use rustyline::DefaultEditor;
use tokio_postgres::NoTls;
use tracing_subscriber::EnvFilter;
use ultrasql_server::replication::{WalReceiver, WalSender};

use cli_support::backup::{run_basebackup, run_pg_dump_fenced, run_pg_restore};
use cli_support::cli_args::{Cli, CliSubcommand, ConnParams, RecoveryTargets, pgpass_lookup};
use cli_support::fileio::{read_sql_script_file, write_regular_file};
use cli_support::server_ops::{run_ctl, run_isready, run_validate};
use cli_support::session::Session;
use cli_support::wal_ship::{
    receive_wal_once, run_archive_wal, run_restore_wal, run_wal_receive_loop, run_wal_send_loop,
};
use cli_support::waldump::run_waldump;

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Build the `tokio-postgres` connection string from resolved parameters.
fn build_conn_string(p: &ConnParams) -> String {
    let mut parts = vec![
        format_conn_param("host", &p.host),
        format!("port={}", p.port),
        format_conn_param("dbname", &p.dbname),
        format_conn_param("user", &p.user),
    ];
    if let Some(pw) = &p.password {
        parts.push(format_conn_param("password", pw));
    }
    parts.join(" ")
}

fn format_conn_param(key: &str, value: &str) -> String {
    format!("{key}={}", quote_conn_value(value))
}

fn quote_conn_value(value: &str) -> String {
    if !value.is_empty()
        && !value
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b == b'\'' || b == b'\\')
    {
        return value.to_owned();
    }

    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('\'');
    for ch in value.chars() {
        if ch == '\'' || ch == '\\' {
            quoted.push('\\');
        }
        quoted.push(ch);
    }
    quoted.push('\'');
    quoted
}

/// Collect all connection parameters from the various sources.
fn resolve_params(cli: &Cli) -> Result<ConnParams> {
    // Start from defaults.
    let mut params = ConnParams::default();

    // URL from --url flag.
    if let Some(url) = &cli.url {
        let from_url = ConnParams::from_url(url)?;
        params.merge_from(&from_url);
    }

    // Positional URL argument.
    if let Some(pos) = &cli.positional_url {
        if pos.contains("://") {
            let from_url = ConnParams::from_url(pos)?;
            params.merge_from(&from_url);
        } else {
            // Treat as host shorthand.
            pos.clone_into(&mut params.host);
        }
    }

    // Individual CLI flags override URL.
    params.apply_overrides(
        cli.host.clone(),
        cli.port,
        cli.dbname.clone(),
        cli.username.clone(),
        cli.password.clone(),
    );

    // If still no password, try ~/.pgpass.
    if params.password.is_none() {
        params.password = pgpass_lookup(&params.host, params.port, &params.dbname, &params.user);
    }

    Ok(params)
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    // Initialise tracing from RUST_LOG (default: off).
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let cli = Cli::parse();

    match run(cli).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    if matches!(cli.subcommand, Some(CliSubcommand::Validate)) {
        run_validate(&cli.data_dir)?;
        return Ok(());
    }

    let params = resolve_params(&cli)?;

    if cli.isready {
        run_isready(&params, cli.ops_endpoint.as_deref()).await?;
        return Ok(());
    }

    if let Some(path) = &cli.waldump {
        run_waldump(path)?;
        return Ok(());
    }

    if let Some(cmd) = cli.ctl {
        let targets = RecoveryTargets {
            time: cli.recovery_target_time.clone(),
            lsn: cli.recovery_target_lsn.clone(),
            xid: cli.recovery_target_xid.clone(),
        };
        run_ctl(
            cmd,
            &cli.data_dir,
            &params,
            cli.ops_endpoint.as_deref(),
            &targets,
        )
        .await?;
        return Ok(());
    }

    if let Some(dest) = &cli.basebackup {
        run_basebackup(&cli.data_dir, dest, cli.ops_endpoint.as_deref()).await?;
        return Ok(());
    }

    if let Some(dest) = &cli.pg_dump {
        run_pg_dump_fenced(
            &cli.data_dir,
            dest,
            cli.dump_format,
            cli.ops_endpoint.as_deref(),
        )
        .await?;
        return Ok(());
    }

    if let Some(source) = &cli.pg_restore {
        run_pg_restore(source, &cli.data_dir)?;
        return Ok(());
    }

    if let Some(wal_path) = &cli.archive_wal {
        run_archive_wal(wal_path, &cli.archive_dir)?;
        return Ok(());
    }

    if let Some(wal_name) = &cli.restore_wal {
        let output = cli
            .restore_output
            .as_ref()
            .context("--restore-wal requires --restore-output PATH")?;
        run_restore_wal(wal_name, &cli.archive_dir, output)?;
        return Ok(());
    }

    if let Some(dest) = &cli.wal_send_once {
        let slots_dir = cli.data_dir.join("pg_replslot");
        let sender = WalSender::new(&cli.archive_dir, slots_dir)?;
        if cli.wal_send_interval_ms == 0 {
            let copied = sender.send_once(&cli.replication_slot, dest)?;
            println!("sent {copied} WAL file(s) to {}", dest.display());
        } else {
            run_wal_send_loop(
                &sender,
                &cli.replication_slot,
                dest,
                cli.wal_send_interval_ms,
            )?;
        }
        return Ok(());
    }

    if let Some(source) = &cli.wal_receive_once {
        let receiver = WalReceiver::new(source);
        let wal_dir = cli.data_dir.join("pg_wal");
        if cli.wal_receive_interval_ms == 0 {
            let copied = receive_wal_once(
                &receiver,
                &wal_dir,
                cli.wal_receive_cascade_archive.as_deref(),
            )?;
            write_regular_file(
                &cli.data_dir.join("standby.signal"),
                b"standby\n",
                "standby signal",
            )?;
            println!("received {copied} WAL file(s) into {}", wal_dir.display());
        } else {
            run_wal_receive_loop(
                &receiver,
                &cli.data_dir,
                cli.wal_receive_cascade_archive.as_deref(),
                cli.wal_receive_interval_ms,
            )?;
        }
        return Ok(());
    }

    // Build connection string and connect.
    let conn_str = build_conn_string(&params);
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .with_context(|| {
            format!(
                "failed to connect to {}:{} as {}",
                params.host, params.port, params.user
            )
        })?;

    // Drive the connection on a background task.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("postgres connection error: {e}");
        }
    });

    let mut session = Session::new(client, params.clone());

    // -c / --command: execute one statement and exit.
    if let Some(cmd) = cli.command {
        session.exec_batch(&cmd).await?;
        return Ok(());
    }

    // -f / --file: execute from file and exit.
    if let Some(path) = cli.file {
        let content = read_sql_script_file(&path)?;
        session.exec_batch(&content).await?;
        return Ok(());
    }

    // Interactive REPL.
    run_repl(&mut session).await
}

/// Run the interactive REPL loop.
async fn run_repl(session: &mut Session) -> Result<()> {
    let mut rl = DefaultEditor::new().context("failed to initialise readline")?;

    // Load history from ~/.ultrasql_history if available.
    let history_path = history_path();
    if let Some(p) = &history_path {
        let _ = rl.load_history(p);
    }

    let p = &session.params;
    println!(
        "ultrasql {} — connected to {} as {} on {}:{} (type \\? for help, \\q to quit)",
        env!("CARGO_PKG_VERSION"),
        p.dbname,
        p.user,
        p.host,
        p.port
    );

    let mut buf = String::new();

    loop {
        let prompt = if buf.is_empty() { "=> " } else { "-> " };
        let line = match rl.readline(prompt) {
            Ok(l) => l,
            Err(rustyline::error::ReadlineError::Eof) => break,
            Err(rustyline::error::ReadlineError::Interrupted) => {
                buf.clear();
                continue;
            }
            Err(e) => return Err(e.into()),
        };

        let _ = rl.add_history_entry(&line);

        let trimmed = line.trim();

        // Backslash commands are dispatched immediately.
        if trimmed.starts_with('\\') {
            let quit = session.handle_meta(trimmed).await?;
            if quit {
                break;
            }
            buf.clear();
            continue;
        }

        // Accumulate into multi-line buffer.
        if !trimmed.is_empty() {
            if !buf.is_empty() {
                buf.push('\n');
            }
            buf.push_str(trimmed);
        }

        // Execute on semicolon or when the buffer ends with one.
        if buf.trim_end().ends_with(';') {
            let sql = std::mem::take(&mut buf);
            session.exec_sql(&sql).await?;
        }
    }

    // Save history.
    if let Some(p) = &history_path {
        let _ = rl.save_history(p);
    }

    println!("Bye!");
    Ok(())
}

/// Return the path to the readline history file, or `None` if HOME is not set.
fn history_path() -> Option<PathBuf> {
    let home = std::env::var("HOME").ok()?;
    Some(PathBuf::from(home).join(".ultrasql_history"))
}
