//! UltraSQL and reference connection targets and their setup.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use tokio_postgres::{Client, NoTls};

use crate::cli::{Cli, Mode, ReferenceEngine};
use crate::model::SortMode;
use crate::runner::{execute_query, format_cli_reference_rows};

#[derive(Debug)]
pub(crate) struct InProcessServer {
    handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
}

impl Drop for InProcessServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

pub(crate) async fn connect_ultrasql_target(
    cli: &Cli,
) -> Result<(Client, Option<InProcessServer>)> {
    match cli.mode {
        Mode::Wire => {
            let database_url = cli.database_url.as_deref().ok_or_else(|| {
                anyhow::anyhow!(
                    "missing --database-url or ULTRASQL_SLT_DATABASE_URL for wire-mode execution"
                )
            })?;
            let client = connect_database(database_url, "UltraSQL wire endpoint").await?;
            Ok((client, None))
        }
        Mode::InProcess => {
            let addr = SocketAddr::from(([127, 0, 0, 1], 0));
            let (listener, bound) = ultrasql_server::bind_listener(addr)
                .await
                .context("bind in-process UltraSQL listener")?;
            let server = Arc::new(ultrasql_server::Server::with_sample_database());
            let handle = tokio::spawn(ultrasql_server::serve_listener(listener, server));
            let conn_str = format!(
                "host={host} port={port} user=slt_runner application_name=ultrasql_slt",
                host = bound.ip(),
                port = bound.port()
            );
            let client = connect_database(&conn_str, "in-process UltraSQL wire endpoint").await?;
            Ok((client, Some(InProcessServer { handle })))
        }
    }
}

pub(crate) async fn connect_database(conn_str: &str, label: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(conn_str, NoTls)
        .await
        .with_context(|| format!("connect {label}"))?;
    let label = label.to_owned();
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("ultrasql-slt: {label} connection error: {err}");
        }
    });
    Ok(client)
}

#[derive(Debug)]
pub(crate) enum ReferenceTarget {
    Postgres(Client),
    Cli(CliReference),
}

impl ReferenceTarget {
    pub(crate) async fn execute_statement(&self, sql: &str) -> Result<()> {
        match self {
            Self::Postgres(client) => client
                .batch_execute(sql)
                .await
                .map_err(|err| anyhow::anyhow!("{}", crate::runner::format_pg_error(&err))),
            Self::Cli(reference) => reference.execute_statement(sql),
        }
    }

    pub(crate) async fn execute_query(
        &self,
        type_string: &str,
        sort_mode: SortMode,
        sql: &str,
    ) -> Result<Vec<String>> {
        match self {
            Self::Postgres(client) => execute_query(client, type_string, sort_mode, sql).await,
            Self::Cli(reference) => reference.execute_query(type_string, sort_mode, sql),
        }
    }
}

#[derive(Debug)]
pub(crate) struct CliReference {
    engine: ReferenceEngine,
    db_path: PathBuf,
    remove_on_drop: bool,
}

impl CliReference {
    pub(crate) fn new(engine: ReferenceEngine, db_path: PathBuf, remove_on_drop: bool) -> Self {
        Self {
            engine,
            db_path,
            remove_on_drop,
        }
    }

    fn execute_statement(&self, sql: &str) -> Result<()> {
        self.run_sql(sql).map(|_| ())
    }

    fn execute_query(
        &self,
        type_string: &str,
        sort_mode: SortMode,
        sql: &str,
    ) -> Result<Vec<String>> {
        let stdout = self.run_sql(sql)?;
        format_cli_reference_rows(&stdout, type_string, sort_mode)
    }

    fn run_sql(&self, sql: &str) -> Result<String> {
        let command = self.engine.command().with_context(|| {
            format!(
                "reference engine {} does not expose a CLI command",
                self.engine.suffix()
            )
        })?;
        let output = Command::new(command)
            .arg("-batch")
            .arg("-bail")
            .arg("-noheader")
            .arg("-list")
            .arg("-nullvalue")
            .arg("NULL")
            .arg("-separator")
            .arg("\n")
            .arg(&self.db_path)
            .arg(sql)
            .output()
            .with_context(|| format!("run {command} reference engine"))?;
        if !output.status.success() {
            bail!(
                "{} reference failed with status {}\nstdout:\n{}\nstderr:\n{}",
                command,
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        String::from_utf8(output.stdout).context("reference output is not UTF-8")
    }
}

impl Drop for CliReference {
    fn drop(&mut self) {
        if self.remove_on_drop {
            let _ = std::fs::remove_file(&self.db_path);
        }
    }
}

pub(crate) async fn connect_reference_targets(cli: &Cli) -> Result<Vec<ReferenceTarget>> {
    let engines = selected_reference_engines(cli)?;
    if engines.is_empty() {
        if cli.reference_db.is_some() {
            bail!("--reference-db requires --reference-engine duckdb or sqlite");
        }
        return Ok(Vec::new());
    }

    let mut references = Vec::with_capacity(engines.len());
    for engine in engines {
        match engine {
            ReferenceEngine::Postgres => {
                if cli.reference_db.is_some() {
                    bail!("--reference-db is only valid with duckdb or sqlite comparison engines");
                }
                let reference_url = cli.reference_url.as_deref().ok_or_else(|| {
                    anyhow::anyhow!("--reference-engine postgres requires --reference-url")
                })?;
                references.push(ReferenceTarget::Postgres(
                    connect_database(reference_url, "PostgreSQL reference endpoint").await?,
                ));
            }
            ReferenceEngine::Duckdb | ReferenceEngine::Sqlite => {
                let (db_path, remove_on_drop) = match &cli.reference_db {
                    Some(path) => (path.clone(), false),
                    None => (temp_reference_db_path(engine)?, true),
                };
                references.push(ReferenceTarget::Cli(CliReference::new(
                    engine,
                    db_path,
                    remove_on_drop,
                )));
            }
        }
    }
    Ok(references)
}

pub(crate) fn selected_reference_engines(cli: &Cli) -> Result<Vec<ReferenceEngine>> {
    let mut engines = cli.reference_engine.clone();
    if cli.reference_url.is_some() && !engines.contains(&ReferenceEngine::Postgres) {
        engines.push(ReferenceEngine::Postgres);
    }
    if cli.reference_url.is_some()
        && engines
            .iter()
            .any(|engine| matches!(engine, ReferenceEngine::Duckdb | ReferenceEngine::Sqlite))
    {
        bail!("--reference-url is only valid with postgres reference engine");
    }
    if cli.reference_db.is_some() {
        let cli_engine_count = engines
            .iter()
            .filter(|engine| matches!(engine, ReferenceEngine::Duckdb | ReferenceEngine::Sqlite))
            .count();
        if cli_engine_count != 1 || engines.len() != 1 {
            bail!("--reference-db requires exactly one duckdb or sqlite reference engine");
        }
    }
    let mut deduped = Vec::with_capacity(engines.len());
    for engine in engines {
        if !deduped.contains(&engine) {
            deduped.push(engine);
        }
    }
    Ok(deduped)
}

pub(crate) fn temp_reference_db_path(engine: ReferenceEngine) -> Result<PathBuf> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before Unix epoch")?
        .as_nanos();
    Ok(std::env::temp_dir().join(format!(
        "ultrasql-slt-{}-{nanos}.{}",
        std::process::id(),
        engine.suffix()
    )))
}
