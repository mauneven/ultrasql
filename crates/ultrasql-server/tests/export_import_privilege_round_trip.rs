//! Wire-level privilege checks for `EXPORT DATABASE` / `IMPORT DATABASE`.
//!
//! Both statements read or write a directory tree on the database host using
//! the server process's own filesystem privileges — exactly like a
//! server-side `COPY ... FROM/TO '<path>'`. Without a gate any authenticated
//! non-superuser could write attacker-controlled bytes anywhere the server
//! can write (host takeover / exfiltration) via `EXPORT DATABASE`, or read
//! another tenant's dump directory via `IMPORT DATABASE`. The gate mirrors
//! the COPY server-file gate: it requires SUPERUSER.

pub mod support;

use std::path::Path;

use support::{shutdown, start_persistent_server};
use tokio_postgres::error::SqlState;

fn sql_string(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "''"))
}

/// A non-superuser effective role is denied both `EXPORT DATABASE` and
/// `IMPORT DATABASE`, while the superuser `tester` still passes the gate
/// (export succeeds end-to-end).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_superuser_cannot_export_or_import_database() {
    let data_dir = tempfile::TempDir::new().expect("data dir");
    let dump_root = tempfile::TempDir::new().expect("dump parent");
    let dump_dir = dump_root.path().join("ultrasql-priv-export");
    // A directory the gate must protect from a non-superuser write.
    let denied_dir = dump_root.path().join("ultrasql-denied-export");
    // Some valid-looking source for the IMPORT deny case (need not exist:
    // the gate fires before the path is ever touched).
    let import_src = dump_root.path().join("ultrasql-other-tenant");

    let running = start_persistent_server(data_dir.path(), "export_import_priv").await;
    let client = &running.client;

    // Register `tester` as a real superuser so it may SET ROLE to a
    // non-superuser, mirroring the external-read privilege suite.
    for sql in [
        "CREATE ROLE tester SUPERUSER LOGIN",
        "CREATE ROLE dump_low_priv NOSUPERUSER",
    ] {
        client.batch_execute(sql).await.expect(sql);
    }

    // Under the non-superuser effective role, both statements are denied
    // *before* any filesystem access happens.
    client
        .batch_execute("SET ROLE dump_low_priv")
        .await
        .expect("set role");

    let export_err = client
        .batch_execute(&format!("EXPORT DATABASE TO {}", sql_string(&denied_dir)))
        .await
        .expect_err("non-superuser EXPORT DATABASE must be denied");
    assert_privilege_denied(&export_err);
    assert!(
        !denied_dir.exists(),
        "denied EXPORT must not write anything to the host filesystem"
    );

    let import_err = client
        .batch_execute(&format!("IMPORT DATABASE FROM {}", sql_string(&import_src)))
        .await
        .expect_err("non-superuser IMPORT DATABASE must be denied");
    assert_privilege_denied(&import_err);

    // RESET ROLE returns to the superuser `tester`: EXPORT now passes the
    // gate and runs to completion, writing a real dump.
    client
        .batch_execute("RESET ROLE")
        .await
        .expect("reset role");
    client
        .batch_execute(&format!("EXPORT DATABASE TO {}", sql_string(&dump_dir)))
        .await
        .expect("superuser EXPORT DATABASE succeeds");
    assert!(
        dump_dir.join("manifest.json").exists(),
        "superuser export produced a manifest"
    );

    shutdown(running).await;
}

/// Assert that `err` is the privilege denial: SQLSTATE 42501.
fn assert_privilege_denied(err: &tokio_postgres::Error) {
    let db = err.as_db_error().expect("database error");
    assert_eq!(
        db.code(),
        &SqlState::INSUFFICIENT_PRIVILEGE,
        "expected insufficient_privilege (42501), got: {}",
        db.message()
    );
}
