//! Runtime-metadata sidecar file IO: capped reads, atomic writes, fsync,
//! and backup-marker persistence.
//!
//! Moved verbatim from the crate root; behavior unchanged.
use super::*;

pub(crate) fn validation_check(
    name: &'static str,
    errors: Vec<String>,
    ok_detail: String,
) -> ValidationCheck {
    if errors.is_empty() {
        ValidationCheck {
            name,
            status: ValidationStatus::Ok,
            detail: ok_detail,
        }
    } else {
        ValidationCheck {
            name,
            status: ValidationStatus::Failed,
            detail: errors.join("; "),
        }
    }
}

pub(crate) fn read_runtime_metadata_file(path: &Path) -> Result<Option<String>, ServerError> {
    read_capped_regular_text_file(
        path,
        "runtime metadata file",
        RUNTIME_METADATA_FILE_LIMIT_BYTES,
    )
}

pub(crate) fn read_capped_regular_text_file(
    path: &Path,
    context: &str,
    limit: u64,
) -> Result<Option<String>, ServerError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => {
            if metadata.len() > limit {
                return Err(ServerError::ddl(format!(
                    "{context} {} exceeds read limit: bytes={} limit={}",
                    path.display(),
                    metadata.len(),
                    limit
                )));
            }
            let file = open_no_follow_read(path)?;
            let opened = file.metadata().map_err(ServerError::Io)?;
            if !opened.file_type().is_file() {
                return Err(ServerError::ddl(format!(
                    "{context} {} is not a regular file",
                    path.display()
                )));
            }
            if opened.len() > limit {
                return Err(ServerError::ddl(format!(
                    "{context} {} exceeds read limit: bytes={} limit={}",
                    path.display(),
                    opened.len(),
                    limit
                )));
            }
            let mut text = String::new();
            let mut limited = file.take(capped_text_take_limit(context, limit)?);
            limited.read_to_string(&mut text).map_err(ServerError::Io)?;
            let bytes_read = capped_text_bytes_read_len(path, context, text.len())?;
            if bytes_read > limit {
                return Err(ServerError::ddl(format!(
                    "{context} {} exceeds read limit: bytes={} limit={}",
                    path.display(),
                    bytes_read,
                    limit
                )));
            }
            Ok(Some(text))
        }
        Ok(_) => Err(ServerError::ddl(format!(
            "{context} {} is not a regular file",
            path.display()
        ))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(ServerError::Io(err)),
    }
}

pub(crate) fn capped_text_take_limit(context: &str, limit: u64) -> Result<u64, ServerError> {
    limit.checked_add(1).ok_or_else(|| {
        ServerError::ddl(format!("{context} read limit is too large: limit={limit}"))
    })
}

pub(crate) fn capped_text_bytes_read_len(
    path: &Path,
    context: &str,
    len: usize,
) -> Result<u64, ServerError> {
    u64::try_from(len).map_err(|_| {
        ServerError::ddl(format!(
            "{context} {} byte count exceeds u64: bytes={len}",
            path.display()
        ))
    })
}

pub(crate) fn open_no_follow_read(path: &Path) -> Result<std::fs::File, ServerError> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW);
    }
    options.open(path).map_err(ServerError::Io)
}

pub(crate) fn write_runtime_metadata_file(path: &Path, text: &str) -> Result<(), ServerError> {
    ensure_runtime_metadata_write_slots(path)?;
    let tmp = path.with_extension("meta.tmp");
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options.open(&tmp).map_err(|err| {
        #[cfg(unix)]
        if err.raw_os_error() == Some(libc::ELOOP) {
            return ServerError::ddl(format!(
                "runtime metadata file {} is not a regular file",
                tmp.display()
            ));
        }
        ServerError::Io(err)
    })?;
    std::io::Write::write_all(&mut file, text.as_bytes()).map_err(ServerError::Io)?;
    ultrasql_core::fsync::durability_sync(&file).map_err(ServerError::Io)?;
    drop(file);
    std::fs::rename(&tmp, path).map_err(ServerError::Io)?;
    sync_runtime_metadata_parent(path)
}

pub(crate) fn ensure_runtime_metadata_write_slots(path: &Path) -> Result<(), ServerError> {
    ensure_runtime_metadata_file_slot(path)?;
    let tmp = path.with_extension("meta.tmp");
    ensure_runtime_metadata_file_slot(&tmp)
}

pub(crate) fn ensure_optional_runtime_metadata_write_slots(
    path: Option<PathBuf>,
) -> Result<(), ServerError> {
    if let Some(path) = path {
        ensure_runtime_metadata_write_slots(&path)?;
    }
    Ok(())
}

pub(crate) fn ensure_runtime_metadata_file_slot(path: &Path) -> Result<(), ServerError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(()),
        Ok(_) => Err(ServerError::ddl(format!(
            "runtime metadata file {} is not a regular file",
            path.display()
        ))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(ServerError::Io(err)),
    }
}

pub(crate) fn sync_runtime_metadata_parent(path: &Path) -> Result<(), ServerError> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    sync_runtime_metadata_dir(parent)
}

#[cfg(unix)]
pub(crate) fn sync_runtime_metadata_dir(path: &Path) -> Result<(), ServerError> {
    let dir = std::fs::File::open(path).map_err(ServerError::Io)?;
    match dir.sync_all() {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::InvalidInput => Ok(()),
        Err(err) => Err(ServerError::Io(err)),
    }
}

#[cfg(not(unix))]
pub(crate) fn sync_runtime_metadata_dir(_path: &Path) -> Result<(), ServerError> {
    Ok(())
}

pub(crate) fn write_backup_marker_file(path: &Path, payload: &str) -> Result<(), ServerError> {
    ensure_backup_marker_file_slot(path)?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options.open(path).map_err(|err| {
        #[cfg(unix)]
        if err.raw_os_error() == Some(libc::ELOOP) {
            return ServerError::ddl(format!(
                "backup marker file {} is not a regular file",
                path.display()
            ));
        }
        ServerError::Io(err)
    })?;
    std::io::Write::write_all(&mut file, payload.as_bytes()).map_err(ServerError::Io)
}

pub(crate) fn ensure_backup_marker_file_slot(path: &Path) -> Result<(), ServerError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(()),
        Ok(_) => Err(ServerError::ddl(format!(
            "backup marker file {} is not a regular file",
            path.display()
        ))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(ServerError::Io(err)),
    }
}
