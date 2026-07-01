//! `--basebackup`, `--pg-dump`, and `--pg-restore` subcommands, with the
//! manifest format and directory-tree copy/verify helpers.

use std::fmt::Write as _;
use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result};

use super::cli_args::DumpFormat;
use super::fileio::{
    checksum_hex, copy_regular_file, decode_hex, hex_bytes, path_exists_or_symlink,
    read_regular_file, read_regular_text_file, write_regular_file,
};
use super::server_ops::http_post_ops_endpoint;

pub(crate) async fn run_basebackup(
    data_dir: &PathBuf,
    dest: &PathBuf,
    ops_endpoint: Option<&str>,
) -> Result<()> {
    let checkpoint_fence = if let Some(endpoint) = ops_endpoint {
        let response = http_post_ops_endpoint(endpoint, "/backup/start").await?;
        if !response.ok {
            anyhow::bail!("backup fence start failed: {}", response.body.trim());
        }
        Some(response.body)
    } else {
        None
    };

    let backup_result = run_basebackup_copy(data_dir, dest, checkpoint_fence.as_deref());
    if let Some(endpoint) = ops_endpoint {
        let stop_result = http_post_ops_endpoint(endpoint, "/backup/stop").await;
        if backup_result.is_ok() {
            let response = stop_result?;
            if !response.ok {
                anyhow::bail!("backup fence stop failed: {}", response.body.trim());
            }
        } else {
            let _ = stop_result;
        }
    }
    backup_result
}

pub(crate) fn run_basebackup_copy(
    data_dir: &PathBuf,
    dest: &PathBuf,
    checkpoint_fence: Option<&str>,
) -> Result<()> {
    if path_exists_or_symlink(dest)? {
        anyhow::bail!("basebackup destination already exists: {}", dest.display());
    }
    fs::create_dir_all(dest)?;
    // The copy is a bootable data directory (credentials, sidecar metadata),
    // and the server refuses to start from a group/world-readable data dir —
    // hand it over with the same 0700 the server requires so a standby can
    // boot from the backup without a manual chmod.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(dest, fs::Permissions::from_mode(0o700))?;
    }
    let mut manifest = Vec::new();
    copy_tree_with_manifest(data_dir, data_dir, dest, &mut manifest)?;
    if let Some(fence) = checkpoint_fence {
        let label = backup_label_text(fence);
        write_regular_file(
            &dest.join("backup_label"),
            label.as_bytes(),
            "basebackup label",
        )?;
        let len = u64::try_from(label.len()).unwrap_or(u64::MAX);
        manifest.push((
            "backup_label".to_string(),
            len,
            checksum_hex(label.as_bytes()),
        ));
    }
    manifest.sort_by(|a, b| a.0.cmp(&b.0));
    let text = basebackup_manifest_text(&manifest, checkpoint_fence);
    write_regular_file(
        &dest.join("backup_manifest.json"),
        text.as_bytes(),
        "basebackup manifest",
    )?;
    println!(
        "base backup copied {} files to {}",
        manifest.len(),
        dest.display()
    );
    Ok(())
}

fn backup_label_text(checkpoint_fence: &str) -> String {
    format!("ULTRASQL BACKUP FENCE\n{checkpoint_fence}")
}

pub(crate) fn basebackup_manifest_text(
    manifest: &[(String, u64, String)],
    checkpoint_fence: Option<&str>,
) -> String {
    let mut text = String::from("{\n");
    if let Some(fence) = checkpoint_fence {
        text.push_str(&format!(
            "  \"checkpoint_fence\":\"{}\",\n",
            json_escape(fence)
        ));
    }
    text.push_str("  \"files\": [\n");
    for (idx, (path, bytes, checksum)) in manifest.iter().enumerate() {
        let comma = if idx + 1 == manifest.len() { "" } else { "," };
        text.push_str(&format!(
            "    {{\"path\":\"{}\",\"bytes\":{},\"checksum\":\"{}\"}}{}\n",
            json_escape(path),
            bytes,
            checksum,
            comma
        ));
    }
    text.push_str("  ]\n}\n");
    text
}

pub(crate) fn json_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            '\u{08}' => escaped.push_str("\\b"),
            '\u{0c}' => escaped.push_str("\\f"),
            ch if ch.is_control() => {
                write!(&mut escaped, "\\u{:04x}", u32::from(ch))
                    .expect("writing to String cannot fail");
            }
            ch => escaped.push(ch),
        }
    }
    escaped
}

#[cfg(test)]
pub(crate) fn run_pg_dump(data_dir: &Path, dest: &Path, format: DumpFormat) -> Result<()> {
    run_pg_dump_with_fence(data_dir, dest, format, None)
}

pub(crate) async fn run_pg_dump_fenced(
    data_dir: &Path,
    dest: &Path,
    format: DumpFormat,
    ops_endpoint: Option<&str>,
) -> Result<()> {
    let checkpoint_fence = if let Some(endpoint) = ops_endpoint {
        let response = http_post_ops_endpoint(endpoint, "/backup/start").await?;
        if !response.ok {
            anyhow::bail!("dump fence start failed: {}", response.body.trim());
        }
        Some(response.body)
    } else {
        None
    };

    let dump_result = run_pg_dump_with_fence(data_dir, dest, format, checkpoint_fence.as_deref());
    if let Some(endpoint) = ops_endpoint {
        let stop_result = http_post_ops_endpoint(endpoint, "/backup/stop").await;
        if dump_result.is_ok() {
            let response = stop_result?;
            if !response.ok {
                anyhow::bail!("dump fence stop failed: {}", response.body.trim());
            }
        } else {
            let _ = stop_result;
        }
    }
    dump_result
}

fn run_pg_dump_with_fence(
    data_dir: &Path,
    dest: &Path,
    format: DumpFormat,
    checkpoint_fence: Option<&str>,
) -> Result<()> {
    match format {
        DumpFormat::Directory => {
            if path_exists_or_symlink(dest)? {
                anyhow::bail!("dump destination already exists: {}", dest.display());
            }
            fs::create_dir_all(dest)?;
            let mut manifest = Vec::new();
            copy_tree_with_manifest(
                &data_dir.to_path_buf(),
                &data_dir.to_path_buf(),
                &dest.to_path_buf(),
                &mut manifest,
            )?;
            manifest.sort_by(|a, b| a.0.cmp(&b.0));
            write_regular_file(
                &dest.join("ultrasql_dump.manifest"),
                dump_manifest_text_with_fence(&manifest, checkpoint_fence).as_bytes(),
                "dump manifest",
            )?;
            println!(
                "directory dump wrote {} files to {}",
                manifest.len(),
                dest.display()
            );
        }
        DumpFormat::Plain | DumpFormat::Custom | DumpFormat::Tar => {
            if path_exists_or_symlink(dest)? {
                anyhow::bail!("dump destination already exists: {}", dest.display());
            }
            let mut entries = Vec::new();
            collect_dump_entries(data_dir, data_dir, &mut entries)?;
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            let mut out = String::new();
            writeln!(&mut out, "ULTRASQL_DUMP_V1 format={format:?}")?;
            if let Some(fence) = checkpoint_fence {
                writeln!(
                    &mut out,
                    "CHECKPOINT_FENCE_HEX {} {}",
                    hex_bytes(fence.as_bytes()),
                    json_escape(fence)
                )?;
            }
            for (path, bytes) in &entries {
                if path.contains('\n') {
                    anyhow::bail!("cannot dump path containing newline: {path}");
                }
                writeln!(
                    &mut out,
                    "FILE {} sha256:{} {}",
                    bytes.len(),
                    checksum_hex(bytes),
                    path
                )?;
                writeln!(&mut out, "{}", hex_bytes(bytes))?;
                writeln!(&mut out, "END")?;
            }
            write_regular_file(dest, out.as_bytes(), "dump archive")?;
            println!(
                "{format:?} dump wrote {} files to {}",
                entries.len(),
                dest.display()
            );
        }
    }
    Ok(())
}

pub(crate) fn run_pg_restore(source: &Path, data_dir: &Path) -> Result<()> {
    fs::create_dir_all(data_dir)?;
    let source_type = fs::symlink_metadata(source)
        .with_context(|| format!("cannot inspect dump source: {}", source.display()))?
        .file_type();
    if source_type.is_dir() {
        verify_dump_directory_manifest(source)?;
        restore_dump_directory(source, source, data_dir)?;
        println!("restored directory dump into {}", data_dir.display());
        return Ok(());
    }
    if !source_type.is_file() {
        anyhow::bail!("dump source is not a regular file: {}", source.display());
    }
    let text = read_regular_text_file(source, "dump archive")?;
    let mut lines = text.lines();
    let header = lines.next().context("empty dump archive")?;
    if !header.starts_with("ULTRASQL_DUMP_V1 ") {
        anyhow::bail!("unsupported dump archive header: {header}");
    }
    while let Some(line) = lines.next() {
        if line.trim().is_empty() {
            continue;
        }
        if line.starts_with("CHECKPOINT_FENCE_HEX ") {
            continue;
        }
        let Some(rest) = line.strip_prefix("FILE ") else {
            anyhow::bail!("malformed dump archive line: {line}");
        };
        let (len_text, rel_path) = rest
            .split_once(' ')
            .context("malformed FILE header in dump archive")?;
        let (expected_checksum, rel_path) =
            if let Some((maybe_checksum, path)) = rel_path.split_once(' ') {
                if let Some(checksum) = maybe_checksum.strip_prefix("sha256:") {
                    if !is_checksum_hex(checksum) {
                        anyhow::bail!("malformed dump archive checksum: {maybe_checksum}");
                    }
                    (Some(checksum), path)
                } else {
                    (None, rel_path)
                }
            } else {
                (None, rel_path)
            };
        let expected_len = len_text.parse::<usize>()?;
        let hex = lines.next().context("missing FILE payload")?;
        let bytes = decode_hex(hex)?;
        if bytes.len() != expected_len {
            anyhow::bail!(
                "dump payload length mismatch for {rel_path}: expected {expected_len}, got {}",
                bytes.len()
            );
        }
        if let Some(expected_checksum) = expected_checksum {
            let actual_checksum = checksum_hex(&bytes);
            if actual_checksum != expected_checksum {
                anyhow::bail!(
                    "dump archive checksum mismatch for {rel_path}: expected {expected_checksum}, got {actual_checksum}"
                );
            }
        }
        let end = lines.next().context("missing FILE terminator")?;
        if end != "END" {
            anyhow::bail!("malformed dump archive terminator: {end}");
        }
        let dest = data_dir.join(validate_restore_manifest_path(rel_path)?);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        write_regular_file(&dest, &bytes, "dump archive restore")?;
    }
    println!("restored archive dump into {}", data_dir.display());
    Ok(())
}

fn is_checksum_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn validate_restore_manifest_path(rel_path: &str) -> Result<PathBuf> {
    let path = Path::new(rel_path);
    let mut clean = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => clean.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                anyhow::bail!("dump archive path escapes restore directory: {rel_path}");
            }
        }
    }
    if clean.as_os_str().is_empty() {
        anyhow::bail!("dump archive path is empty");
    }
    Ok(clean)
}

fn copy_tree_with_manifest(
    root: &PathBuf,
    current: &PathBuf,
    dest_root: &PathBuf,
    manifest: &mut Vec<(String, u64, String)>,
) -> Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let path = entry.path();
        let rel = path.strip_prefix(root)?.to_path_buf();
        let dest = dest_root.join(&rel);
        if file_type.is_dir() {
            fs::create_dir_all(&dest)?;
            copy_tree_with_manifest(root, &path, dest_root, manifest)?;
        } else if file_type.is_file() {
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent)?;
            }
            copy_regular_file(&path, &dest, "dump source")?;
            let bytes = read_regular_file(&path, "dump source")?;
            let checksum = checksum_hex(&bytes);
            let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            manifest.push((rel.display().to_string(), len, checksum));
        } else {
            anyhow::bail!("dump source is not a regular file: {}", path.display());
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn dump_manifest_text(manifest: &[(String, u64, String)]) -> String {
    dump_manifest_text_with_fence(manifest, None)
}

fn dump_manifest_text_with_fence(
    manifest: &[(String, u64, String)],
    checkpoint_fence: Option<&str>,
) -> String {
    let mut text = String::from("{\n  \"files\": [\n");
    for (idx, (path, bytes, checksum)) in manifest.iter().enumerate() {
        let comma = if idx + 1 == manifest.len() { "" } else { "," };
        let escaped = json_escape(path);
        text.push_str(&format!(
            "    {{\"path\":\"{escaped}\",\"bytes\":{bytes},\"checksum\":\"{checksum}\"}}{comma}\n"
        ));
    }
    text.push_str("  ]");
    if let Some(fence) = checkpoint_fence {
        text.push_str(&format!(
            ",\n  \"checkpoint_fence\":\"{}\"",
            json_escape(fence)
        ));
    }
    text.push_str("\n}\n");
    text
}

fn collect_dump_entries(
    root: &Path,
    current: &Path,
    entries: &mut Vec<(String, Vec<u8>)>,
) -> Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let path = entry.path();
        if file_type.is_dir() {
            collect_dump_entries(root, &path, entries)?;
        } else if file_type.is_file() {
            let rel = path.strip_prefix(root)?.display().to_string();
            entries.push((rel, read_regular_file(&path, "dump source")?));
        } else {
            anyhow::bail!("dump source is not a regular file: {}", path.display());
        }
    }
    Ok(())
}

#[derive(Debug)]
struct DumpManifestEntry {
    bytes: u64,
    checksum: String,
}

fn verify_dump_directory_manifest(root: &Path) -> Result<()> {
    let manifest_path = root.join("ultrasql_dump.manifest");
    let mut expected = read_dump_manifest_entries(&manifest_path)?;
    verify_dump_directory_tree(root, root, &mut expected)?;
    if !expected.is_empty() {
        let missing = expected.keys().next().cloned().unwrap_or_default();
        anyhow::bail!("dump manifest entry missing from directory: {missing}");
    }
    Ok(())
}

fn read_dump_manifest_entries(
    manifest_path: &Path,
) -> Result<std::collections::HashMap<String, DumpManifestEntry>> {
    let text = read_regular_text_file(manifest_path, "dump manifest")?;
    let manifest: serde_json::Value = serde_json::from_str(&text)
        .with_context(|| format!("cannot parse dump manifest: {}", manifest_path.display()))?;
    let files = manifest
        .get("files")
        .and_then(serde_json::Value::as_array)
        .context("dump manifest missing files array")?;
    let mut entries = std::collections::HashMap::with_capacity(files.len());
    for file in files {
        let entry = file
            .as_object()
            .context("dump manifest file entry is not an object")?;
        let path = entry
            .get("path")
            .and_then(serde_json::Value::as_str)
            .context("dump manifest file entry missing path")?
            .to_owned();
        if path == "ultrasql_dump.manifest" {
            anyhow::bail!("dump manifest cannot list itself");
        }
        validate_restore_manifest_path(&path)?;
        let bytes = entry
            .get("bytes")
            .and_then(serde_json::Value::as_u64)
            .context("dump manifest file entry missing bytes")?;
        let checksum = entry
            .get("checksum")
            .and_then(serde_json::Value::as_str)
            .context("dump manifest file entry missing checksum")?
            .to_owned();
        if entries
            .insert(path.clone(), DumpManifestEntry { bytes, checksum })
            .is_some()
        {
            anyhow::bail!("dump manifest lists duplicate path: {path}");
        }
    }
    Ok(entries)
}

fn verify_dump_directory_tree(
    root: &Path,
    current: &Path,
    expected: &mut std::collections::HashMap<String, DumpManifestEntry>,
) -> Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let path = entry.path();
        let rel = path.strip_prefix(root)?;
        if rel == Path::new("ultrasql_dump.manifest") {
            continue;
        }
        if file_type.is_dir() {
            verify_dump_directory_tree(root, &path, expected)?;
        } else if file_type.is_file() {
            let rel_text = rel.display().to_string();
            let expected_entry = expected.remove(&rel_text).with_context(|| {
                format!("dump directory contains unmanifested file: {rel_text}")
            })?;
            let bytes = read_regular_file(&path, "directory dump file")?;
            let actual_len =
                u64::try_from(bytes.len()).context("dump file length does not fit u64")?;
            if actual_len != expected_entry.bytes {
                anyhow::bail!(
                    "dump directory length mismatch for {rel_text}: expected {}, got {actual_len}",
                    expected_entry.bytes
                );
            }
            let actual_checksum = checksum_hex(&bytes);
            if actual_checksum != expected_entry.checksum {
                anyhow::bail!(
                    "dump directory checksum mismatch for {rel_text}: expected {}, got {actual_checksum}",
                    expected_entry.checksum
                );
            }
        } else {
            anyhow::bail!("dump source is not a regular file: {}", path.display());
        }
    }
    Ok(())
}

fn restore_dump_directory(root: &Path, current: &Path, data_dir: &Path) -> Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let path = entry.path();
        let rel = path.strip_prefix(root)?;
        if rel == Path::new("ultrasql_dump.manifest") {
            continue;
        }
        let dest = data_dir.join(rel);
        if file_type.is_dir() {
            fs::create_dir_all(&dest)?;
            restore_dump_directory(root, &path, data_dir)?;
        } else if file_type.is_file() {
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent)?;
            }
            copy_regular_file(&path, &dest, "directory dump restore")?;
        } else {
            anyhow::bail!("dump source is not a regular file: {}", path.display());
        }
    }
    Ok(())
}
