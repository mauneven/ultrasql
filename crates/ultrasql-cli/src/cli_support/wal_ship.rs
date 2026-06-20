//! WAL archive/restore and physical WAL shipping/receiving subcommands.

use std::path::Path;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use ultrasql_server::replication::{WalReceiver, WalSender};

use super::fileio::{copy_regular_file, write_regular_file};

pub(crate) fn run_archive_wal(wal_path: &Path, archive_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(archive_dir)?;
    let name = wal_path
        .file_name()
        .and_then(|name| name.to_str())
        .context("WAL path must have a filename")?;
    validate_wal_file_name(name)?;
    let dest = archive_dir.join(name);
    copy_regular_file(wal_path, &dest, "WAL archive")?;
    println!("archived {} to {}", wal_path.display(), dest.display());
    Ok(())
}

pub(crate) fn run_restore_wal(wal_name: &str, archive_dir: &Path, output: &Path) -> Result<()> {
    validate_wal_file_name(wal_name)?;
    let source = archive_dir.join(wal_name);
    copy_regular_file(&source, output, "WAL restore")?;
    println!("restored {} to {}", source.display(), output.display());
    Ok(())
}

fn validate_wal_file_name(name: &str) -> Result<()> {
    let ultrasql_segment = name
        .strip_prefix("segment_")
        .is_some_and(|suffix| suffix.len() == 10 && suffix.bytes().all(|b| b.is_ascii_digit()));
    let pg_segment = name.len() == 24 && name.bytes().all(|b| b.is_ascii_hexdigit());
    if ultrasql_segment || pg_segment {
        Ok(())
    } else {
        anyhow::bail!("unsafe WAL filename: {name}");
    }
}

pub(crate) fn run_wal_send_loop(
    sender: &WalSender,
    slot: &str,
    dest: &Path,
    interval_ms: u64,
) -> Result<()> {
    let interval = Duration::from_millis(interval_ms);
    println!(
        "shipping WAL from archive every {interval_ms}ms to {}",
        dest.display()
    );
    loop {
        let copied = sender.send_once(slot, dest)?;
        if copied > 0 {
            println!("sent {copied} WAL file(s) to {}", dest.display());
        }
        thread::sleep(interval);
    }
}

pub(crate) fn run_wal_receive_loop(
    receiver: &WalReceiver,
    data_dir: &Path,
    cascade_archive_dir: Option<&Path>,
    interval_ms: u64,
) -> Result<()> {
    let interval = Duration::from_millis(interval_ms);
    let wal_dir = data_dir.join("pg_wal");
    write_regular_file(
        &data_dir.join("standby.signal"),
        b"standby\n",
        "standby signal",
    )?;
    println!(
        "receiving WAL every {interval_ms}ms into {}",
        wal_dir.display()
    );
    loop {
        let copied = receive_wal_once(receiver, &wal_dir, cascade_archive_dir)?;
        if copied > 0 {
            println!("received {copied} WAL file(s) into {}", wal_dir.display());
        }
        thread::sleep(interval);
    }
}

pub(crate) fn receive_wal_once(
    receiver: &WalReceiver,
    wal_dir: &Path,
    cascade_archive_dir: Option<&Path>,
) -> Result<usize> {
    match cascade_archive_dir {
        Some(archive_dir) => receiver
            .receive_once_cascading(wal_dir, archive_dir)
            .map_err(Into::into),
        None => receiver.receive_once(wal_dir).map_err(Into::into),
    }
}
