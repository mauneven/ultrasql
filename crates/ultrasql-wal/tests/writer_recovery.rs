//! End-to-end tests for the WAL writer and recovery driver.
//!
//! These tests bring up a real on-disk WAL directory (via `tempfile`),
//! run real writer threads, and verify the contents the way crash
//! recovery would see them.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use tempfile::TempDir;
use ultrasql_core::{Lsn, Xid};
use ultrasql_wal::buffer::WalBuffer;
use ultrasql_wal::record::{RecordType, WalRecord};
use ultrasql_wal::{WalWriter, WalWriterConfig, recover};

fn build_record(i: u32) -> WalRecord {
    // Use a payload that varies in size with `i` so torn-write tests
    // have something to detect; the first 4 bytes hold the index.
    let mut payload = i.to_le_bytes().to_vec();
    payload.extend_from_slice(b"-- payload contents for record ");
    payload.extend_from_slice(&[i.to_le_bytes()[0]; 8]);
    WalRecord::new(
        RecordType::HeapInsert,
        Xid::new(u64::from(i + 1)),
        Lsn::ZERO,
        0,
        payload,
    )
    .expect("test WAL record should fit size limits")
}

const fn writer_config(segment_size: u64) -> WalWriterConfig {
    WalWriterConfig {
        segment_size_bytes: segment_size,
        // Tight window keeps tests fast.
        fsync_window_us: 200,
        // A small threshold so we don't sit on a single byte forever.
        fsync_batch_bytes: 64,
    }
}

/// Wait for `flushed_lsn` to reach `target` (or longer) with a wall-clock budget.
fn wait_for_durable(writer: &WalWriter, target: Lsn, budget: Duration) {
    let start = Instant::now();
    while writer.flushed_lsn() < target {
        assert!(
            start.elapsed() <= budget,
            "wal writer never reached durable lsn {target:?} (got {:?})",
            writer.flushed_lsn()
        );
        writer.notify();
        thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn round_trip_1k_records_flushed_lsn_covers_buffer() {
    let dir = TempDir::new().unwrap();
    let buffer = Arc::new(WalBuffer::new(8 * 1024 * 1024, Lsn::ZERO));
    let writer =
        WalWriter::open(dir.path(), Arc::clone(&buffer), writer_config(1024 * 1024)).unwrap();

    for i in 0..1000_u32 {
        buffer.append(&build_record(i)).unwrap();
        if i % 64 == 0 {
            writer.notify();
        }
    }
    let end_lsn = buffer.next_lsn();
    writer.notify();
    writer.shutdown().unwrap();

    assert!(buffer.durable_lsn() >= end_lsn);
}

#[test]
fn recovery_observes_all_records_in_order() {
    let dir = TempDir::new().unwrap();
    let buffer = Arc::new(WalBuffer::new(2 * 1024 * 1024, Lsn::ZERO));
    let writer =
        WalWriter::open(dir.path(), Arc::clone(&buffer), writer_config(512 * 1024)).unwrap();

    let mut expected = Vec::with_capacity(250);
    for i in 0..250_u32 {
        let rec = build_record(i);
        buffer.append(&rec).unwrap();
        expected.push(rec);
    }
    writer.shutdown().unwrap();

    let mut seen = Vec::new();
    let _last_lsn = recover(dir.path(), |rec| {
        seen.push(rec.clone());
        Ok(())
    })
    .unwrap();

    assert_eq!(seen.len(), expected.len());
    for (got, want) in seen.iter().zip(expected.iter()) {
        assert_eq!(got.header.record_type, want.header.record_type);
        assert_eq!(got.header.xid, want.header.xid);
        assert_eq!(got.payload, want.payload);
    }
}

#[test]
fn segment_rollover_creates_multiple_files_and_recovers_all() {
    let dir = TempDir::new().unwrap();
    let buffer = Arc::new(WalBuffer::new(2 * 1024 * 1024, Lsn::ZERO));
    // Very small segment so we roll many times.
    let writer = WalWriter::open(dir.path(), Arc::clone(&buffer), writer_config(2048)).unwrap();

    let total = 500_u32;
    for i in 0..total {
        buffer.append(&build_record(i)).unwrap();
    }
    writer.shutdown().unwrap();

    // Count segment files post-shutdown.
    let mut segment_files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(std::result::Result::ok)
        .filter(|e| {
            e.file_name().to_string_lossy().starts_with("segment_")
                && e.file_type().unwrap().is_file()
        })
        .collect();
    segment_files.sort_by_key(std::fs::DirEntry::file_name);
    assert!(
        segment_files.len() >= 2,
        "expected multiple segments after rollover, got {}",
        segment_files.len()
    );

    let mut count = 0_u32;
    recover(dir.path(), |rec| {
        // First 4 bytes of payload encode the record index.
        let i = u32::from_le_bytes([
            rec.payload[0],
            rec.payload[1],
            rec.payload[2],
            rec.payload[3],
        ]);
        assert_eq!(i, count, "records must replay in append order");
        count += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(count, total);
}

#[test]
fn torn_write_at_tail_stops_recovery_cleanly() {
    let dir = TempDir::new().unwrap();
    let buffer = Arc::new(WalBuffer::new(64 * 1024, Lsn::ZERO));
    let writer =
        WalWriter::open(dir.path(), Arc::clone(&buffer), writer_config(64 * 1024)).unwrap();

    // Append 10 records and shutdown so they are all durably written.
    for i in 0..10_u32 {
        buffer.append(&build_record(i)).unwrap();
    }
    writer.shutdown().unwrap();

    // Find the (single) segment and truncate it by 4 bytes to simulate
    // a torn write. We have to grab the path before the writer is
    // dropped so we don't race a parallel agent.
    let segment = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(std::result::Result::ok)
        .find(|e| e.file_name().to_string_lossy().starts_with("segment_"))
        .map(|e| e.path())
        .expect("must have at least one segment");
    let original_len = std::fs::metadata(&segment).unwrap().len();
    let new_len = original_len - 4;
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(&segment)
        .unwrap();
    f.set_len(new_len).unwrap();

    let mut recovered = 0_u32;
    let last_lsn = recover(dir.path(), |_| {
        recovered += 1;
        Ok(())
    })
    .unwrap();

    // We must have recovered fewer records than were appended (the
    // last one is now torn), and the returned LSN must equal the byte
    // count of the recovered prefix.
    assert!(recovered < 10, "torn record at tail must not replay");
    assert!(recovered >= 9, "only the very last record is torn");
    assert!(last_lsn.raw() < original_len);
    assert!(last_lsn.raw() > 0);
}

#[test]
fn recover_empty_dir_returns_zero_lsn() {
    let dir = TempDir::new().unwrap();
    let lsn = recover(dir.path(), |_| Ok(())).unwrap();
    assert_eq!(lsn, Lsn::ZERO);
}

#[test]
fn multi_producer_appends_all_recover() {
    let dir = TempDir::new().unwrap();
    let buffer = Arc::new(WalBuffer::new(4 * 1024 * 1024, Lsn::ZERO));
    let writer =
        WalWriter::open(dir.path(), Arc::clone(&buffer), writer_config(256 * 1024)).unwrap();

    let producers = 8_u32;
    let per_producer = 100_u32;
    let counter = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for _p in 0..producers {
        let buf = Arc::clone(&buffer);
        let c = Arc::clone(&counter);
        handles.push(thread::spawn(move || {
            for _ in 0..per_producer {
                let id = c.fetch_add(1, Ordering::Relaxed);
                let payload = id.to_le_bytes().to_vec();
                let rec =
                    WalRecord::new(RecordType::HeapInsert, Xid::new(1), Lsn::ZERO, 0, payload)
                        .expect("test WAL record should fit size limits");
                // Buffer is sized generously above; appends never fail.
                buf.append(&rec).unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    writer.shutdown().unwrap();

    let mut seen = 0_u32;
    recover(dir.path(), |_| {
        seen += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(seen, producers * per_producer);
}

#[test]
fn durable_lsn_catches_up_to_buffer_next_lsn_on_shutdown() {
    let dir = TempDir::new().unwrap();
    let buffer = Arc::new(WalBuffer::new(256 * 1024, Lsn::ZERO));
    let writer =
        WalWriter::open(dir.path(), Arc::clone(&buffer), writer_config(64 * 1024)).unwrap();

    for i in 0..200_u32 {
        buffer.append(&build_record(i)).unwrap();
    }
    let next = buffer.next_lsn();
    writer.shutdown().unwrap();
    assert_eq!(
        buffer.durable_lsn(),
        next,
        "shutdown must flush every buffered byte"
    );
}

#[test]
fn restart_resumes_and_recovers_all_records() {
    let dir = TempDir::new().unwrap();

    // First lifetime: append 50 records.
    {
        let buffer = Arc::new(WalBuffer::new(256 * 1024, Lsn::ZERO));
        let writer =
            WalWriter::open(dir.path(), Arc::clone(&buffer), writer_config(8 * 1024)).unwrap();
        for i in 0..50_u32 {
            buffer.append(&build_record(i)).unwrap();
        }
        writer.shutdown().unwrap();
    }

    // Compute the LSN where the first lifetime stopped writing by
    // tallying segment sizes — this is the byte offset where the
    // second lifetime must continue.
    let mut existing_bytes = 0_u64;
    for entry in std::fs::read_dir(dir.path()).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name().to_string_lossy().starts_with("segment_") {
            existing_bytes += entry.metadata().unwrap().len();
        }
    }
    assert!(existing_bytes > 0);

    // Second lifetime: a fresh buffer that knows where to resume.
    {
        let buffer = Arc::new(WalBuffer::new(256 * 1024, Lsn::new(existing_bytes)));
        let writer =
            WalWriter::open(dir.path(), Arc::clone(&buffer), writer_config(8 * 1024)).unwrap();
        for i in 50..100_u32 {
            buffer.append(&build_record(i)).unwrap();
        }
        writer.shutdown().unwrap();
    }

    // Recovery must see all 100 records, in order.
    let mut count = 0_u32;
    recover(dir.path(), |rec| {
        let i = u32::from_le_bytes([
            rec.payload[0],
            rec.payload[1],
            rec.payload[2],
            rec.payload[3],
        ]);
        assert_eq!(i, count);
        count += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(count, 100);
}

#[test]
fn writer_drops_cleanly_without_explicit_shutdown() {
    let dir = TempDir::new().unwrap();
    let buffer = Arc::new(WalBuffer::new(64 * 1024, Lsn::ZERO));
    {
        let writer =
            WalWriter::open(dir.path(), Arc::clone(&buffer), writer_config(64 * 1024)).unwrap();
        buffer.append(&build_record(0)).unwrap();
        writer.notify();
        // Wait for the writer to fsync at least one record.
        wait_for_durable(&writer, buffer.next_lsn(), Duration::from_secs(5));
        // Dropped here without calling shutdown().
    }
    // The writer's Drop signals stop and joins. We can still recover.
    let mut seen = 0;
    recover(dir.path(), |_| {
        seen += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(seen, 1);
}
