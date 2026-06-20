//! Readiness checks, the ops HTTP client, `--validate`, and `--ctl` actions.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use ultrasql_server::{Server, ValidationReport};

use super::cli_args::{ConnParams, CtlCommand, RecoveryTargets};
use super::fileio::write_regular_file;

pub(crate) async fn run_isready(params: &ConnParams, ops_endpoint: Option<&str>) -> Result<()> {
    if let Some(endpoint) = ops_endpoint {
        let ready = check_http_ready(endpoint).await?;
        if ready {
            println!("{endpoint} - accepting connections");
            return Ok(());
        }
        anyhow::bail!("{endpoint} - no response");
    }

    let addr = format!("{}:{}", params.host, params.port);
    tokio::net::TcpStream::connect(&addr)
        .await
        .with_context(|| format!("{addr} - no response"))?;
    println!("{addr} - accepting connections");
    Ok(())
}

pub(crate) async fn check_http_ready(endpoint: &str) -> Result<bool> {
    Ok(http_get_ops_endpoint(endpoint, "/ready").await?.ok)
}

#[derive(Debug)]
pub(crate) struct OpsHttpResponse {
    pub(crate) ok: bool,
    pub(crate) body: String,
}

const OPS_HTTP_RESPONSE_LIMIT_BYTES: usize = 64 * 1024;

pub(crate) async fn http_get_ops_endpoint(endpoint: &str, path: &str) -> Result<OpsHttpResponse> {
    http_ops_endpoint("GET", endpoint, path).await
}

pub(crate) async fn http_post_ops_endpoint(endpoint: &str, path: &str) -> Result<OpsHttpResponse> {
    http_ops_endpoint("POST", endpoint, path).await
}

async fn http_ops_endpoint(method: &str, endpoint: &str, path: &str) -> Result<OpsHttpResponse> {
    let endpoint = endpoint
        .strip_prefix("http://")
        .unwrap_or(endpoint)
        .trim_end_matches('/');
    let host_port = endpoint
        .split_once('/')
        .map_or(endpoint, |(host, _path)| host);
    let path = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    let mut stream = tokio::net::TcpStream::connect(host_port)
        .await
        .with_context(|| format!("{host_port} - no response"))?;
    let request =
        format!("{method} {path} HTTP/1.1\r\nhost: {host_port}\r\nconnection: close\r\n\r\n");
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    stream.write_all(request.as_bytes()).await?;
    let mut response = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        let next_len = response.len().saturating_add(read);
        if next_len > OPS_HTTP_RESPONSE_LIMIT_BYTES {
            anyhow::bail!(
                "ops endpoint response exceeds read limit: bytes={} limit={}",
                next_len,
                OPS_HTTP_RESPONSE_LIMIT_BYTES
            );
        }
        response.extend_from_slice(&buffer[..read]);
    }
    let ok = response.starts_with(b"HTTP/1.1 200") || response.starts_with(b"HTTP/1.0 200");
    let body = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map_or(&response[..], |idx| &response[idx + 4..]);
    Ok(OpsHttpResponse {
        ok,
        body: String::from_utf8_lossy(body).into_owned(),
    })
}

pub(crate) fn run_validate(data_dir: &Path) -> Result<()> {
    let server = Server::init(data_dir)
        .with_context(|| format!("validate data directory {}", data_dir.display()))?;
    let report = server.validate();
    print_validation_report(&report);
    if report.is_ok() {
        Ok(())
    } else {
        anyhow::bail!("validation failed")
    }
}

pub(crate) fn print_validation_report(report: &ValidationReport) {
    if report.is_ok() {
        println!("validation ok");
    } else {
        println!("validation failed");
    }
    for check in &report.checks {
        println!(
            "{}: {} - {}",
            check.name,
            check.status.as_str(),
            check.detail
        );
    }
}

pub(crate) async fn run_ctl(
    cmd: CtlCommand,
    data_dir: &PathBuf,
    params: &ConnParams,
    ops_endpoint: Option<&str>,
    targets: &RecoveryTargets,
) -> Result<()> {
    match cmd {
        CtlCommand::Initdb => {
            prepare_initdb_data_dir(data_dir)?;
            fs::create_dir_all(data_dir.join("base"))?;
            fs::create_dir_all(data_dir.join("pg_wal"))?;
            fs::create_dir_all(data_dir.join("global"))?;
            write_regular_file(
                &data_dir.join("ultrasql.control"),
                format!("version={}\nstate=initialized\n", env!("CARGO_PKG_VERSION")).as_bytes(),
                "control file",
            )?;
            println!(
                "initialized UltraSQL data directory at {}",
                data_dir.display()
            );
        }
        CtlCommand::Start => {
            println!(
                "start command: ultrasqld --data-dir {} --listen {}:{}",
                data_dir.display(),
                params.host,
                params.port
            );
        }
        CtlCommand::Status => {
            run_isready(params, ops_endpoint).await?;
        }
        CtlCommand::Reload => {
            println!("reload requested; send SIGHUP to ultrasqld process manager");
        }
        CtlCommand::Promote => {
            write_regular_file(
                &data_dir.join("promote.signal"),
                b"promote\n",
                "promote signal",
            )?;
            println!("created {}", data_dir.join("promote.signal").display());
        }
        CtlCommand::Standby => {
            fs::create_dir_all(data_dir)?;
            write_regular_file(
                &data_dir.join("standby.signal"),
                b"standby\n",
                "standby signal",
            )?;
            println!("created {}", data_dir.join("standby.signal").display());
        }
        CtlCommand::Recovery => {
            fs::create_dir_all(data_dir)?;
            write_regular_file(
                &data_dir.join("recovery.signal"),
                b"recovery\n",
                "recovery signal",
            )?;
            let mut conf = String::new();
            if let Some(value) = &targets.time {
                conf.push_str(&format!(
                    "recovery_target_time = '{}'\n",
                    escape_conf(value)
                ));
            }
            if let Some(value) = &targets.lsn {
                conf.push_str(&format!("recovery_target_lsn = '{}'\n", escape_conf(value)));
            }
            if let Some(value) = &targets.xid {
                conf.push_str(&format!("recovery_target_xid = '{}'\n", escape_conf(value)));
            }
            write_regular_file(
                &data_dir.join("recovery.targets"),
                conf.as_bytes(),
                "recovery targets",
            )?;
            println!("created {}", data_dir.join("recovery.signal").display());
        }
        CtlCommand::Stop => {
            println!("stop requested; send SIGTERM through service manager");
        }
    }
    Ok(())
}

fn prepare_initdb_data_dir(data_dir: &Path) -> Result<()> {
    match fs::symlink_metadata(data_dir) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            anyhow::bail!("data directory {} is a symlink", data_dir.display());
        }
        Ok(metadata) if metadata.file_type().is_dir() => {}
        Ok(_) => anyhow::bail!("data directory {} is not a directory", data_dir.display()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => fs::create_dir_all(data_dir)
            .with_context(|| format!("create data directory {}", data_dir.display()))?,
        Err(err) => {
            return Err(err)
                .with_context(|| format!("inspect data directory {}", data_dir.display()));
        }
    }
    set_private_data_dir_permissions(data_dir)
}

#[cfg(unix)]
fn set_private_data_dir_permissions(data_dir: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(data_dir, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("chmod 700 data directory {}", data_dir.display()))
}

#[cfg(not(unix))]
fn set_private_data_dir_permissions(_data_dir: &Path) -> Result<()> {
    Ok(())
}

pub(crate) fn escape_conf(value: &str) -> String {
    value.replace('\'', "''")
}
