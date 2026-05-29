//! Catalog-version startup guard tests.

use ultrasql_server::catalog_version::{
    CATALOG_VERSION_FILE, CURRENT_CATALOG_VERSION, ensure_catalog_version,
};

#[test]
fn fresh_data_dir_gets_v1_catalog_version_marker() {
    let dir = tempfile::tempdir().expect("tempdir");

    let status = ensure_catalog_version(dir.path()).expect("fresh catalog version");

    assert_eq!(status.observed_version, CURRENT_CATALOG_VERSION);
    assert!(status.created);
    let marker = std::fs::read_to_string(dir.path().join(CATALOG_VERSION_FILE))
        .expect("read catalog version marker");
    assert_eq!(marker, format!("{CURRENT_CATALOG_VERSION}\n"));
}

#[test]
fn matching_catalog_version_is_accepted_without_rewrite() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join(CATALOG_VERSION_FILE),
        format!("{CURRENT_CATALOG_VERSION}\n"),
    )
    .expect("write catalog marker");

    let status = ensure_catalog_version(dir.path()).expect("matching catalog version");

    assert_eq!(status.observed_version, CURRENT_CATALOG_VERSION);
    assert!(!status.created);
}

#[test]
fn newer_catalog_version_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let newer = CURRENT_CATALOG_VERSION + 1;
    std::fs::write(dir.path().join(CATALOG_VERSION_FILE), format!("{newer}\n"))
        .expect("write newer catalog marker");

    let err = ensure_catalog_version(dir.path()).expect_err("newer catalog must be refused");
    let message = err.to_string();

    assert!(
        message.contains("newer than this UltraSQL binary"),
        "{message}"
    );
    assert!(message.contains(&newer.to_string()), "{message}");
    assert!(
        message.contains(&CURRENT_CATALOG_VERSION.to_string()),
        "{message}"
    );
}

#[cfg(unix)]
#[test]
fn symlinked_catalog_version_marker_is_refused() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().expect("tempdir");
    let outside = dir.path().join("outside.version");
    std::fs::write(&outside, format!("{CURRENT_CATALOG_VERSION}\n")).expect("outside marker");
    symlink(&outside, dir.path().join(CATALOG_VERSION_FILE)).expect("catalog symlink");

    let err = ensure_catalog_version(dir.path()).expect_err("symlink marker refused");

    assert!(err.to_string().contains("catalog version marker"), "{err}");
}

#[cfg(unix)]
#[test]
fn broken_symlink_catalog_version_marker_does_not_create_target() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().expect("tempdir");
    let outside = dir.path().join("outside.version");
    symlink(&outside, dir.path().join(CATALOG_VERSION_FILE)).expect("catalog symlink");

    let err = ensure_catalog_version(dir.path()).expect_err("broken symlink marker refused");

    assert!(err.to_string().contains("catalog version marker"), "{err}");
    assert!(!outside.exists());
}
