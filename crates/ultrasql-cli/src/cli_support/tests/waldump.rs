//! WAL dump/decode tests and the WAL archive/restore + hex-helper round-trip.

use std::fs;

use super::super::fileio::{decode_hex, hex_bytes};
use super::super::wal_ship::{run_archive_wal, run_restore_wal};
use super::super::waldump::{
    decode_wal_payload, format_decoded, run_waldump, waldump_record_lines,
};
use super::cli_env_test_lock;

#[test]
fn waldump_decodes_heap_insert_payload() {
    use ultrasql_core::{BlockNumber, Lsn, PageId, RelationId, TupleId, Xid};
    use ultrasql_wal::{HeapInsertPayload, RecordType, WalRecord};

    let tid = TupleId::new(PageId::new(RelationId::new(7), BlockNumber::new(3)), 2);
    let payload = HeapInsertPayload {
        tid,
        tuple_bytes: vec![1, 2, 3],
    }
    .encode()
    .expect("heap insert payload encodes");
    let record = WalRecord::new(RecordType::HeapInsert, Xid::new(42), Lsn::ZERO, 0, payload)
        .expect("test WAL record should fit size limits");

    let lines = waldump_record_lines(&record.encode());

    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("type=HeapInsert"));
    assert!(lines[0].contains("decoded=HeapInsertPayload"));
    assert!(lines[0].contains("tuple_bytes: [1, 2, 3]"));
}

#[test]
fn waldump_reports_malformed_tail() {
    let lines = waldump_record_lines(&[0, 1, 2]);

    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("record_error="));
}

#[test]
fn every_record_type_has_a_waldump_decode_arm() {
    // Regression guard for the build-break class where a new
    // ultrasql-wal RecordType variant is added without a matching arm in
    // the CLI's WAL-decode dispatch. Iterating RecordType::ALL (kept
    // exhaustive by a compile-time guard in ultrasql-wal) ensures every
    // current and future variant routes through a typed payload decoder.
    //
    // decode_wal_payload returns "decoded=<Payload>" on a successful parse
    // and "payload_error=<err>" when a typed decoder rejects the (here,
    // empty) payload. Both prove the variant reached a real decode arm. A
    // future wildcard `_ =>` fallback for an unhandled variant would emit
    // neither prefix, failing this test instead of shipping a CLI that
    // cannot describe that record.
    use ultrasql_core::{Lsn, Xid};
    use ultrasql_wal::{RecordType, WalRecord};

    for &rt in RecordType::ALL {
        let record = WalRecord::new(rt, Xid::new(7), Lsn::ZERO, 0, Vec::new())
            .expect("test WAL record should fit size limits");
        let decoded = decode_wal_payload(&record);
        assert!(
            decoded.starts_with("decoded=") || decoded.starts_with("payload_error="),
            "RecordType::{rt:?} has no typed waldump decode arm: {decoded:?}"
        );
    }
}

#[test]
fn wal_dump_archive_restore_and_hex_helpers_cover_success_and_errors() {
    use ultrasql_core::{Lsn, Xid};
    use ultrasql_wal::{RecordType, WalRecord};

    let dir = tempfile::tempdir().expect("tempdir");
    let wal = dir.path().join("000000010000000000000001");
    let record = WalRecord::new(RecordType::Nop, Xid::new(1), Lsn::ZERO, 0, Vec::new())
        .expect("test WAL record should fit size limits");
    fs::write(&wal, record.encode()).expect("write WAL");

    run_waldump(&wal).expect("waldump");
    let _env_guard = cli_env_test_lock();
    // SAFETY: cli_env_test_lock serializes process-env mutation in this
    // module's tests.
    unsafe {
        std::env::set_var("ULTRASQL_WALDUMP_FILE_LIMIT_BYTES", "3");
    }
    let oversized = dir.path().join("oversized-wal");
    fs::write(&oversized, b"abcd").expect("oversized wal");
    let err = run_waldump(&oversized).expect_err("oversized waldump rejected");
    assert!(err.to_string().contains("exceeds read limit"), "{err}");
    // SAFETY: cli_env_test_lock serializes process-env mutation in this
    // module's tests.
    unsafe {
        std::env::remove_var("ULTRASQL_WALDUMP_FILE_LIMIT_BYTES");
    }
    assert!(
        waldump_record_lines(&[])
            .first()
            .is_some_and(|line| line.contains("empty"))
    );
    assert!(decode_wal_payload(&record).contains("Nop"));
    assert_eq!(
        format_decoded::<()>(Err(ultrasql_wal::PayloadError::Malformed("bad"))),
        "payload_error=payload malformed: bad"
    );

    let archive = dir.path().join("archive");
    run_archive_wal(&wal, &archive).expect("archive WAL");
    let restored = dir.path().join("restored.wal");
    run_restore_wal("000000010000000000000001", &archive, &restored).expect("restore WAL");
    assert_eq!(
        fs::read(&wal).expect("read wal"),
        fs::read(restored).expect("read restored")
    );

    let outside = dir.path().join("outside.wal");
    fs::write(&outside, b"outside").expect("outside wal");
    let escaped = dir.path().join("escaped.wal");
    assert!(run_restore_wal("../outside.wal", &archive, &escaped).is_err());
    assert!(!escaped.exists());

    assert_eq!(hex_bytes(&[0, 1, 255]), "0001ff");
    assert_eq!(decode_hex("0001ff").expect("decode hex"), vec![0, 1, 255]);
    assert!(
        decode_hex("0")
            .expect_err("odd hex")
            .to_string()
            .contains("odd length")
    );
    assert!(format!("{:#}", decode_hex("zz").expect_err("invalid hex")).contains("invalid hex"));
}

#[cfg(unix)]
#[test]
fn wal_archive_restore_rejects_symlinked_sources_and_targets() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().expect("tempdir");
    let wal_name = "000000010000000000000001";
    let outside = dir.path().join("outside");
    fs::write(&outside, b"keep").expect("outside file");

    let wal_link = dir.path().join(wal_name);
    symlink(&outside, &wal_link).expect("wal source symlink");
    let archive = dir.path().join("archive");
    assert!(run_archive_wal(&wal_link, &archive).is_err());
    assert!(!archive.join(wal_name).exists());

    let real_wal = dir.path().join("000000010000000000000002");
    fs::write(&real_wal, b"wal").expect("real wal");
    fs::create_dir_all(&archive).expect("archive dir");
    symlink(&outside, archive.join("000000010000000000000002")).expect("archive symlink");
    assert!(run_archive_wal(&real_wal, &archive).is_err());
    assert_eq!(fs::read(&outside).expect("outside unchanged"), b"keep");

    let restore_archive = dir.path().join("restore-archive");
    fs::create_dir_all(&restore_archive).expect("restore archive dir");
    symlink(&outside, restore_archive.join(wal_name)).expect("restore source symlink");
    assert!(run_restore_wal(wal_name, &restore_archive, &dir.path().join("restored")).is_err());

    let real_archive = dir.path().join("real-archive");
    fs::create_dir_all(&real_archive).expect("real archive dir");
    fs::write(real_archive.join(wal_name), b"wal").expect("archive wal");
    let output = dir.path().join("output");
    symlink(&outside, &output).expect("restore output symlink");
    assert!(run_restore_wal(wal_name, &real_archive, &output).is_err());
    assert_eq!(fs::read(&outside).expect("outside unchanged"), b"keep");
}
