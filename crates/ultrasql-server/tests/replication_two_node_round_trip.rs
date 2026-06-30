//! Phase 2b end-to-end: a real walreceiver client streams physical WAL from a
//! live primary over a socket and lands it locally, and the standby's WAL is
//! byte-identical to the primary's (the Phase 2 milestone gate).

pub mod support;

use std::path::Path;

use support::{shutdown, start_persistent_server_with_segment_size};
use ultrasql_core::Lsn;
use ultrasql_server::walreceiver::{StandbyStreamOptions, WalReceiverClient};

/// Read every `segment_*` file in a WAL dir, sorted by name, as `(name, bytes)`.
/// (The standby has no `wal.manifest`, so we compare only the segment files.)
fn read_segments(wal_dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut out: Vec<(String, Vec<u8>)> = std::fs::read_dir(wal_dir)
        .expect("read wal dir")
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let name = entry.file_name().into_string().ok()?;
            if name.starts_with("segment_") {
                Some((name, std::fs::read(entry.path()).expect("read segment")))
            } else {
                None
            }
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

#[tokio::test]
async fn standby_streams_byte_identical_wal_from_a_live_primary() {
    const SEG: u64 = 4096; // small segments so the WAL below spans several files

    // --- Primary: a live server generating committed WAL ---
    let primary_dir = tempfile::TempDir::new().expect("primary data dir");
    let primary =
        start_persistent_server_with_segment_size(primary_dir.path(), "primary", SEG).await;

    // Generate enough WAL to span multiple segments (DDL + many small commits).
    let mut sql = String::from("CREATE TABLE repl_t (id INT NOT NULL, note TEXT);");
    for i in 0..120 {
        sql.push_str(&format!(
            "INSERT INTO repl_t VALUES ({i}, 'row-{i}-streamed-over-the-wire');"
        ));
    }
    primary
        .client
        .batch_execute(&sql)
        .await
        .expect("generate WAL");

    let target = primary
        .server
        .runtime_wal_flushed_lsn()
        .expect("primary has a durable WAL position");
    assert!(target.raw() > 0, "primary produced WAL");

    // --- Standby: a walreceiver client streams from the primary over TCP ---
    let standby_dir = tempfile::TempDir::new().expect("standby data dir");
    let standby_wal = standby_dir.path().join("pg_wal");

    let mut client = WalReceiverClient::connect(primary.bound, "tester")
        .await
        .expect("replication connect + startup");
    let opts = StandbyStreamOptions {
        slot: None,
        start_lsn: Lsn::ZERO,
        wal_dir: standby_wal.clone(),
        segment_size_bytes: SEG,
    };
    // Stream until the standby has received everything durable on the primary.
    let receiver = client
        .stream_into(&opts, |r| r.received_lsn().raw() >= target.raw())
        .await
        .expect("stream WAL into the standby");

    assert_eq!(
        receiver.flushed_lsn(),
        target,
        "standby fsynced up to the target"
    );
    assert_eq!(
        receiver.received_lsn(),
        target,
        "no partial record left over"
    );

    // --- The standby's WAL is a byte-identical prefix of the primary's ---
    let primary_segments = read_segments(&primary_dir.path().join("pg_wal"));
    let standby_segments = read_segments(&standby_wal);
    assert!(
        primary_segments.len() > 1,
        "the workload should span several segments (got {})",
        primary_segments.len()
    );

    let primary_stream: Vec<u8> = primary_segments
        .iter()
        .flat_map(|(_, b)| b.clone())
        .collect();
    let standby_stream: Vec<u8> = standby_segments
        .iter()
        .flat_map(|(_, b)| b.clone())
        .collect();
    assert_eq!(
        standby_stream.len() as u64,
        target.raw(),
        "standby holds exactly the streamed range"
    );
    // The primary may have appended a little more WAL after we sampled `target`
    // (background activity), so compare the standby against the primary prefix.
    assert!(
        primary_stream.len() as u64 >= target.raw(),
        "primary stream covers the target"
    );
    assert_eq!(
        standby_stream,
        primary_stream[..standby_stream.len()],
        "standby WAL is byte-identical to the primary's over the streamed range"
    );

    // Close the replication socket before shutting the primary down.
    drop(client);
    shutdown(primary).await;
}

#[tokio::test]
async fn standby_streams_from_a_named_slot_and_advances_it() {
    const SEG: u64 = 4096;

    let primary_dir = tempfile::TempDir::new().expect("primary data dir");
    let primary =
        start_persistent_server_with_segment_size(primary_dir.path(), "primary_slot", SEG).await;

    let mut sql = String::from("CREATE TABLE slot_t (id INT NOT NULL, note TEXT);");
    for i in 0..120 {
        sql.push_str(&format!("INSERT INTO slot_t VALUES ({i}, 'slot-row-{i}');"));
    }
    primary
        .client
        .batch_execute(&sql)
        .await
        .expect("generate WAL");
    let target = primary
        .server
        .runtime_wal_flushed_lsn()
        .expect("primary durable LSN");

    let standby_dir = tempfile::TempDir::new().expect("standby data dir");
    let standby_wal = standby_dir.path().join("pg_wal");

    let mut client = WalReceiverClient::connect(primary.bound, "tester")
        .await
        .expect("connect");
    // Create the physical slot on the primary, then stream from it.
    client
        .run_command("CREATE_REPLICATION_SLOT standby1 PHYSICAL")
        .await
        .expect("create slot");
    let opts = StandbyStreamOptions {
        slot: Some("standby1".to_owned()),
        start_lsn: Lsn::ZERO,
        wal_dir: standby_wal.clone(),
        segment_size_bytes: SEG,
    };
    let receiver = client
        .stream_into(&opts, |r| r.received_lsn().raw() >= target.raw())
        .await
        .expect("stream from slot");
    assert_eq!(receiver.flushed_lsn(), target);

    // Byte-identical over the streamed range.
    let primary_stream: Vec<u8> = read_segments(&primary_dir.path().join("pg_wal"))
        .iter()
        .flat_map(|(_, b)| b.clone())
        .collect();
    let standby_stream: Vec<u8> = read_segments(&standby_wal)
        .iter()
        .flat_map(|(_, b)| b.clone())
        .collect();
    assert_eq!(standby_stream, primary_stream[..standby_stream.len()]);

    // The standby's flush acknowledgement advanced the slot's restart_lsn (so the
    // primary's WAL recycle floor now follows the standby).
    let raw = target.raw();
    let expected_lsn = format!("{:X}/{:X}", raw >> 32, raw & 0xFFFF_FFFF);
    let slot_body =
        std::fs::read_to_string(primary_dir.path().join("pg_replslot").join("standby1.slot"))
            .expect("read slot file");
    assert!(
        slot_body.contains(&format!("restart_lsn={expected_lsn}")),
        "slot restart_lsn advanced to the standby flush position {expected_lsn}: {slot_body}"
    );

    drop(client);
    shutdown(primary).await;
}
