//! Base-backup, dump, and restore tests: round trips, corruption detection,
//! fence metadata, and symlink hardening.

use std::fs;

use super::super::backup::{
    basebackup_manifest_text, dump_manifest_text, json_escape, run_basebackup, run_basebackup_copy,
    run_pg_dump, run_pg_dump_fenced, run_pg_restore,
};
use super::super::cli_args::DumpFormat;
use super::super::fileio::{checksum_hex, hex_bytes};
use super::spawn_recording_http;

#[test]
fn basebackup_manifest_records_checkpoint_fence_metadata() {
    let manifest = vec![(
        "pg_wal/segment_0000000000".to_string(),
        3,
        "abc".to_string(),
    )];
    let text = basebackup_manifest_text(
        &manifest,
        Some("{\"status\":\"backup_started\",\"flushed_lsn\":7}\n"),
    );

    assert!(text.contains("\"checkpoint_fence\""));
    assert!(text.contains("backup_started"));
    assert!(text.contains("\"files\""));
}

#[tokio::test]
async fn basebackup_fence_uses_post_requests() {
    let data_dir = tempfile::tempdir().expect("data dir");
    fs::write(data_dir.path().join("heap"), b"data").expect("data file");
    let dest_parent = tempfile::tempdir().expect("dest parent");
    let dest = dest_parent.path().join("backup");
    let (endpoint, mut requests) = spawn_recording_http(vec![
        "HTTP/1.1 200 OK\r\ncontent-length: 20\r\n\r\n{\"status\":\"start\"}".to_owned(),
        "HTTP/1.1 200 OK\r\ncontent-length: 19\r\n\r\n{\"status\":\"stop\"}".to_owned(),
    ])
    .await;

    run_basebackup(
        &data_dir.path().to_path_buf(),
        &dest,
        Some(&endpoint.to_string()),
    )
    .await
    .expect("basebackup");

    let start = requests.recv().await.expect("start request");
    let stop = requests.recv().await.expect("stop request");
    assert!(start.starts_with("POST /backup/start HTTP/1.1"), "{start}");
    assert!(stop.starts_with("POST /backup/stop HTTP/1.1"), "{stop}");
}

#[tokio::test]
async fn pg_dump_fence_uses_post_requests_and_records_metadata() {
    let data_dir = tempfile::tempdir().expect("data dir");
    fs::create_dir_all(data_dir.path().join("base/1")).expect("data tree");
    fs::write(data_dir.path().join("base/1/heap"), b"rows").expect("data file");
    let dump_parent = tempfile::tempdir().expect("dump parent");
    let archive = dump_parent.path().join("dump.ultra");
    let (endpoint, mut requests) = spawn_recording_http(vec![
        "HTTP/1.1 200 OK\r\ncontent-length: 44\r\n\r\n{\"status\":\"backup_started\",\"flushed_lsn\":7}"
            .to_owned(),
        "HTTP/1.1 200 OK\r\ncontent-length: 19\r\n\r\n{\"status\":\"stop\"}".to_owned(),
    ])
    .await;

    run_pg_dump_fenced(
        data_dir.path(),
        &archive,
        DumpFormat::Custom,
        Some(&endpoint.to_string()),
    )
    .await
    .expect("fenced pg dump");

    let start = requests.recv().await.expect("start request");
    let stop = requests.recv().await.expect("stop request");
    assert!(start.starts_with("POST /backup/start HTTP/1.1"), "{start}");
    assert!(stop.starts_with("POST /backup/stop HTTP/1.1"), "{stop}");
    let text = fs::read_to_string(&archive).expect("dump archive");
    assert!(text.contains("CHECKPOINT_FENCE_HEX"));
    assert!(text.contains("backup_started"));

    let restored = dump_parent.path().join("restored");
    run_pg_restore(&archive, &restored).expect("restore fenced archive");
    assert_eq!(
        fs::read(restored.join("base/1/heap")).expect("restored heap"),
        b"rows"
    );
}

#[test]
fn basebackup_dump_restore_and_manifest_helpers_round_trip_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("data");
    fs::create_dir_all(data.join("base/1")).expect("create data dir");
    fs::write(data.join("base/1/heap"), b"rows").expect("write heap");
    fs::write(data.join("pg_version"), b"1").expect("write version");

    let backup = dir.path().join("backup");
    run_basebackup_copy(
        &data.to_path_buf(),
        &backup.to_path_buf(),
        Some("{\"flushed_lsn\":7}\n"),
    )
    .expect("basebackup copy");
    assert_eq!(
        fs::read(backup.join("base/1/heap")).expect("backup heap"),
        b"rows"
    );
    assert!(
        fs::read_to_string(backup.join("backup_label"))
            .expect("backup label")
            .contains("ULTRASQL BACKUP FENCE")
    );
    assert!(run_basebackup_copy(&data.to_path_buf(), &backup.to_path_buf(), None).is_err());

    let directory_dump = dir.path().join("dumpdir");
    run_pg_dump(&data, &directory_dump, DumpFormat::Directory).expect("directory dump");
    assert!(directory_dump.join("ultrasql_dump.manifest").is_file());

    for format in [DumpFormat::Plain, DumpFormat::Custom, DumpFormat::Tar] {
        let archive = dir.path().join(format!("dump-{format:?}.ultra"));
        run_pg_dump(&data, &archive, format).expect("archive dump");
        let restored = dir.path().join(format!("restore-{format:?}"));
        run_pg_restore(&archive, &restored).expect("archive restore");
        assert_eq!(
            fs::read(restored.join("base/1/heap")).expect("restored heap"),
            b"rows"
        );
    }

    let restored_dir = dir.path().join("restore-dir");
    run_pg_restore(&directory_dump, &restored_dir).expect("directory restore");
    assert_eq!(
        fs::read(restored_dir.join("base/1/heap")).expect("dir restore"),
        b"rows"
    );

    assert!(dump_manifest_text(&[("a\"b".to_owned(), 3, "abc".to_owned())]).contains("a\\\"b"));
    assert_eq!(json_escape("\"\\\n"), "\\\"\\\\\\n");
    assert_eq!(json_escape("\r\t\u{0001}"), "\\r\\t\\u0001");
    assert_eq!(checksum_hex(b"same"), checksum_hex(b"same"));
    assert_eq!(checksum_hex(b"same").len(), 64);
    assert!(run_pg_restore(&dir.path().join("missing.dump"), &dir.path().join("bad")).is_err());
}

#[test]
fn pg_restore_rejects_corrupt_dump_payloads() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("data");
    fs::create_dir_all(data.join("base/1")).expect("create data dir");
    fs::write(data.join("base/1/heap"), b"rows").expect("write heap");

    let directory_dump = dir.path().join("dumpdir");
    run_pg_dump(&data, &directory_dump, DumpFormat::Directory).expect("directory dump");
    fs::write(directory_dump.join("base/1/heap"), b"rowt").expect("corrupt directory dump");
    let restored_dir = dir.path().join("restore-dir-corrupt");
    let err = run_pg_restore(&directory_dump, &restored_dir).expect_err("directory checksum");
    assert!(err.to_string().contains("checksum"), "{err:?}");
    assert!(!restored_dir.join("base/1/heap").exists());

    let archive = dir.path().join("dump.ultra");
    run_pg_dump(&data, &archive, DumpFormat::Plain).expect("archive dump");
    let text = fs::read_to_string(&archive).expect("archive text");
    assert!(text.contains("FILE 4 sha256:"));
    let corrupted = text.replacen("726f7773", "726f7774", 1);
    assert_ne!(text, corrupted);
    fs::write(&archive, corrupted).expect("corrupt archive");
    let restored_archive = dir.path().join("restore-archive-corrupt");
    let err = run_pg_restore(&archive, &restored_archive).expect_err("archive checksum");
    assert!(err.to_string().contains("checksum"), "{err:?}");
    assert!(!restored_archive.join("base/1/heap").exists());
}

#[test]
fn pg_restore_legacy_archive_keeps_checksum_like_path_token() {
    let dir = tempfile::tempdir().expect("tempdir");
    let checksum_like_name = "a".repeat(64);
    let rel_path = format!("{checksum_like_name} file");
    let archive = dir.path().join("legacy.dump");
    fs::write(
        &archive,
        format!(
            "ULTRASQL_DUMP_V1 format=Plain\nFILE 4 {rel_path}\n{}\nEND\n",
            hex_bytes(b"rows")
        ),
    )
    .expect("legacy archive");

    let restored = dir.path().join("restore-legacy");
    run_pg_restore(&archive, &restored).expect("legacy restore");
    assert_eq!(
        fs::read(restored.join(rel_path)).expect("restored legacy file"),
        b"rows"
    );
}

#[test]
fn pg_restore_rejects_archive_paths_outside_data_dir() {
    let dir = tempfile::tempdir().expect("tempdir");
    let archive = dir.path().join("escape.dump");
    let data_dir = dir.path().join("restore");
    let escaped = dir.path().join("escaped");

    fs::write(
        &archive,
        "ULTRASQL_DUMP_V1 format=Plain\nFILE 5 ../escaped\n68656c6c6f\nEND\n",
    )
    .expect("write archive");

    assert!(run_pg_restore(&archive, &data_dir).is_err());
    assert!(!escaped.exists());
}

#[cfg(unix)]
#[test]
fn backup_and_dump_reject_symlinked_source_files() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("data");
    fs::create_dir_all(data.join("base/1")).expect("create data dir");
    let outside = dir.path().join("outside");
    fs::write(&outside, b"secret").expect("outside file");
    symlink(&outside, data.join("base/1/heap")).expect("source symlink");

    assert!(run_basebackup_copy(&data.to_path_buf(), &dir.path().join("backup"), None).is_err());
    assert!(run_pg_dump(&data, &dir.path().join("dumpdir"), DumpFormat::Directory).is_err());
    assert!(run_pg_dump(&data, &dir.path().join("dump.ultra"), DumpFormat::Plain).is_err());
}

#[cfg(unix)]
#[test]
fn pg_dump_rejects_symlinked_archive_outputs() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("data");
    fs::create_dir_all(data.join("base/1")).expect("create data dir");
    fs::write(data.join("base/1/heap"), b"rows").expect("write heap");
    fs::write(data.join("pg_version"), b"1").expect("write version");
    let outside = dir.path().join("outside-dump");
    let dump = dir.path().join("dump.ultra");
    symlink(&outside, &dump).expect("dump symlink");

    assert!(run_pg_dump(&data, &dump, DumpFormat::Plain).is_err());
    assert!(!outside.exists());
}

#[cfg(unix)]
#[test]
fn pg_restore_rejects_symlinked_directory_sources_and_targets() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().expect("tempdir");
    let outside = dir.path().join("outside");
    fs::write(&outside, b"keep").expect("outside file");

    let dump = dir.path().join("dumpdir");
    fs::create_dir_all(dump.join("base/1")).expect("dump dir");
    symlink(&outside, dump.join("base/1/heap")).expect("dump symlink");
    assert!(run_pg_restore(&dump, &dir.path().join("restore-source")).is_err());
    assert!(!dir.path().join("restore-source/base/1/heap").exists());

    let archive = dir.path().join("dump.ultra");
    fs::write(
        &archive,
        "ULTRASQL_DUMP_V1 format=Plain\nFILE 4 base/1/heap\n726f7773\nEND\n",
    )
    .expect("archive");
    let restore = dir.path().join("restore-target");
    fs::create_dir_all(restore.join("base/1")).expect("restore dir");
    symlink(&outside, restore.join("base/1/heap")).expect("target symlink");
    assert!(run_pg_restore(&archive, &restore).is_err());
    assert_eq!(fs::read(&outside).expect("outside unchanged"), b"keep");
}
