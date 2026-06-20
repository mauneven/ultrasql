//! Ops-endpoint readiness, `--ctl` actions, `--validate`, and WAL receive
//! wrapper tests.

use std::fs;

use ultrasql_server::replication::WalReceiver;
use ultrasql_server::ValidationReport;

use super::super::cli_args::{ConnParams, CtlCommand, RecoveryTargets};
use super::super::server_ops::{
    check_http_ready, escape_conf, http_get_ops_endpoint, print_validation_report, run_ctl,
    run_isready,
};
use super::super::wal_ship::receive_wal_once;
use super::{spawn_one_shot_http, spawn_recording_http};

#[tokio::test]
async fn ops_http_readiness_handles_ok_and_failure_statuses() {
    let ok_endpoint = spawn_one_shot_http("HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nOK").await;
    let response = http_get_ops_endpoint(&format!("http://{ok_endpoint}/ops"), "ready")
        .await
        .expect("http ready");
    assert!(response.ok);
    assert_eq!(response.body, "OK");
    let ready_endpoint =
        spawn_one_shot_http("HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nOK").await;
    assert!(
        check_http_ready(&ready_endpoint.to_string())
            .await
            .expect("ready true")
    );

    let fail_endpoint =
        spawn_one_shot_http("HTTP/1.1 503 Service Unavailable\r\ncontent-length: 4\r\n\r\nDOWN")
            .await;
    assert!(
        !check_http_ready(&fail_endpoint.to_string())
            .await
            .expect("ready false")
    );

    let run_endpoint = spawn_one_shot_http("HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nOK").await;
    let params = ConnParams::default();
    run_isready(&params, Some(&run_endpoint.to_string()))
        .await
        .expect("ops isready");
}

#[tokio::test]
async fn ops_http_response_body_is_bounded() {
    let body = "x".repeat(70 * 1024);
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    let (endpoint, _requests) = spawn_recording_http(vec![response]).await;

    let err = http_get_ops_endpoint(&endpoint.to_string(), "/ready")
        .await
        .expect_err("oversized ops response rejected");

    assert!(err.to_string().contains("exceeds read limit"), "{err}");
}

#[tokio::test]
async fn ctl_commands_write_expected_signal_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("data");
    let data_dir = data_dir.to_path_buf();
    let params = ConnParams::default();
    let targets = RecoveryTargets {
        time: Some("2026-05-29 00:00:00 O'Hara".to_owned()),
        lsn: Some("0/16B6C50".to_owned()),
        xid: Some("42".to_owned()),
    };

    run_ctl(CtlCommand::Initdb, &data_dir, &params, None, &targets)
        .await
        .expect("initdb");
    assert!(data_dir.join("base").is_dir());
    assert!(data_dir.join("pg_wal").is_dir());
    assert!(data_dir.join("global").is_dir());

    run_ctl(CtlCommand::Start, &data_dir, &params, None, &targets)
        .await
        .expect("start");
    run_ctl(CtlCommand::Reload, &data_dir, &params, None, &targets)
        .await
        .expect("reload");
    run_ctl(CtlCommand::Promote, &data_dir, &params, None, &targets)
        .await
        .expect("promote");
    run_ctl(CtlCommand::Standby, &data_dir, &params, None, &targets)
        .await
        .expect("standby");
    run_ctl(CtlCommand::Recovery, &data_dir, &params, None, &targets)
        .await
        .expect("recovery");
    run_ctl(CtlCommand::Stop, &data_dir, &params, None, &targets)
        .await
        .expect("stop");

    assert_eq!(
        fs::read_to_string(data_dir.join("promote.signal")).expect("promote"),
        "promote\n"
    );
    assert_eq!(
        fs::read_to_string(data_dir.join("standby.signal")).expect("standby"),
        "standby\n"
    );
    let recovery = fs::read_to_string(data_dir.join("recovery.targets")).expect("targets");
    assert!(recovery.contains("O''Hara"));
    assert!(recovery.contains("recovery_target_lsn = '0/16B6C50'"));
    assert!(recovery.contains("recovery_target_xid = '42'"));
}

#[cfg(unix)]
#[tokio::test]
async fn ctl_commands_reject_symlinked_signal_targets() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().expect("tempdir");
    let params = ConnParams::default();
    let targets = RecoveryTargets {
        time: None,
        lsn: None,
        xid: None,
    };

    let promote_dir = dir.path().join("promote-data");
    fs::create_dir_all(&promote_dir).expect("promote data");
    let outside = dir.path().join("outside.signal");
    fs::write(&outside, b"keep").expect("outside signal");
    symlink(&outside, promote_dir.join("promote.signal")).expect("promote symlink");

    assert!(
        run_ctl(CtlCommand::Promote, &promote_dir, &params, None, &targets)
            .await
            .is_err()
    );
    assert_eq!(fs::read(&outside).expect("outside unchanged"), b"keep");

    let recovery_dir = dir.path().join("recovery-data");
    fs::create_dir_all(&recovery_dir).expect("recovery data");
    let outside_targets = dir.path().join("outside.targets");
    fs::write(&outside_targets, b"keep").expect("outside targets");
    symlink(&outside_targets, recovery_dir.join("recovery.targets")).expect("targets symlink");

    assert!(
        run_ctl(CtlCommand::Recovery, &recovery_dir, &params, None, &targets)
            .await
            .is_err()
    );
    assert_eq!(
        fs::read(&outside_targets).expect("outside targets unchanged"),
        b"keep"
    );
}

#[test]
fn wal_receiver_wrapper_copies_and_cascades_archived_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    let source = dir.path().join("source");
    let standby = dir.path().join("standby");
    let cascade = dir.path().join("cascade");
    fs::create_dir_all(&source).expect("source");
    fs::write(source.join("000000010000000000000001"), b"wal1").expect("wal1");

    let receiver = WalReceiver::new(&source);
    assert_eq!(
        receive_wal_once(&receiver, &standby, None).expect("receive"),
        1
    );
    assert_eq!(
        fs::read(standby.join("000000010000000000000001")).expect("standby wal"),
        b"wal1"
    );
    assert_eq!(
        receive_wal_once(&receiver, &standby, Some(&cascade)).expect("cascade receive"),
        1
    );
    assert_eq!(
        fs::read(cascade.join("000000010000000000000001")).expect("cascade wal"),
        b"wal1"
    );
}

#[test]
fn validation_report_prints_failure_and_escape_conf_quotes() {
    let report = ValidationReport {
        checks: vec![ultrasql_server::ValidationCheck {
            name: "catalog",
            status: ultrasql_server::ValidationStatus::Failed,
            detail: "broken".to_owned(),
        }],
    };
    assert!(!report.is_ok());
    print_validation_report(&report);
    assert_eq!(escape_conf("O'Hara"), "O''Hara");
}
