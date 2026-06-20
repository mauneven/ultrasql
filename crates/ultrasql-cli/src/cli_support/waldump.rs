//! `--waldump` subcommand: decode and hex-dump a WAL segment file.

use std::path::Path;

use anyhow::Result;

use super::fileio::read_regular_file_capped;

pub(crate) fn run_waldump(path: &Path) -> Result<()> {
    let bytes = read_regular_file_capped(path, "WAL file", waldump_file_limit_bytes())?;
    println!("file: {}", path.display());
    println!("bytes: {}", bytes.len());
    println!("records:");
    for line in waldump_record_lines(&bytes) {
        println!("{line}");
    }
    println!("hex:");
    for (offset, chunk) in bytes.chunks(32).enumerate() {
        let absolute = offset * 32;
        let hex = chunk
            .iter()
            .map(|b| format!("{:02x}", *b))
            .collect::<Vec<_>>()
            .join(" ");
        println!("{absolute:08x}: {hex}");
    }
    Ok(())
}

fn waldump_file_limit_bytes() -> u64 {
    std::env::var("ULTRASQL_WALDUMP_FILE_LIMIT_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(256 * 1024 * 1024)
}

pub(crate) fn waldump_record_lines(bytes: &[u8]) -> Vec<String> {
    let mut lines = Vec::new();
    let mut offset = 0_usize;
    while offset < bytes.len() {
        match ultrasql_wal::WalRecord::decode(&bytes[offset..]) {
            Ok((record, used)) => {
                let decoded = decode_wal_payload(&record);
                lines.push(format!(
                    "{offset:08x}: type={:?} xid={:?} prev_lsn={:?} flags={} len={} payload_len={} {decoded}",
                    record.header.record_type,
                    record.header.xid,
                    record.header.prev_lsn,
                    record.header.flags,
                    record.header.total_length,
                    record.payload.len()
                ));
                offset = offset.saturating_add(used);
            }
            Err(err) => {
                lines.push(format!("{offset:08x}: record_error={err}"));
                break;
            }
        }
    }
    if lines.is_empty() {
        lines.push("00000000: empty".to_string());
    }
    lines
}

pub(crate) fn decode_wal_payload(record: &ultrasql_wal::WalRecord) -> String {
    use ultrasql_wal::RecordType;

    match record.header.record_type {
        RecordType::HeapInsert => {
            format_decoded(ultrasql_wal::HeapInsertPayload::decode(&record.payload))
        }
        RecordType::HeapInsertBatch => format_decoded(
            ultrasql_wal::HeapInsertBatchPayload::decode(&record.payload),
        ),
        RecordType::HeapUpdate => {
            format_decoded(ultrasql_wal::HeapUpdatePayload::decode(&record.payload))
        }
        RecordType::HeapDelete => {
            format_decoded(ultrasql_wal::HeapDeletePayload::decode(&record.payload))
        }
        RecordType::FullPageWrite => {
            format_decoded(ultrasql_wal::FullPageWritePayload::decode(&record.payload))
        }
        RecordType::Commit => format_decoded(ultrasql_wal::CommitPayload::decode(&record.payload)),
        RecordType::Abort => format_decoded(ultrasql_wal::AbortPayload::decode(&record.payload)),
        RecordType::Checkpoint => {
            format_decoded(ultrasql_wal::CheckpointPayload::decode(&record.payload))
        }
        RecordType::BTreeOp => {
            format_decoded(ultrasql_wal::BTreeOpPayload::decode(&record.payload))
        }
        RecordType::HeapUpdateInPlace => format_decoded(
            ultrasql_wal::HeapUpdateInPlacePayload::decode(&record.payload),
        ),
        RecordType::HeapUpdateInPlaceBatch => format_decoded(
            ultrasql_wal::HeapUpdateInPlaceBatchPayload::decode(&record.payload),
        ),
        RecordType::HeapUpdateInt32PairDeltaBatch => format_decoded(
            ultrasql_wal::HeapUpdateInt32PairDeltaBatchPayload::decode(&record.payload),
        ),
        RecordType::HeapUpdateInt32PairDeltaRangeBatch => format_decoded(
            ultrasql_wal::HeapUpdateInt32PairDeltaRangeBatchPayload::decode(&record.payload),
        ),
        RecordType::HeapDeleteInPlace => format_decoded(
            ultrasql_wal::HeapDeleteInPlacePayload::decode(&record.payload),
        ),
        RecordType::HeapDeleteInPlaceBatch => format_decoded(
            ultrasql_wal::HeapDeleteInPlaceBatchPayload::decode(&record.payload),
        ),
        RecordType::HeapDeleteInPlaceRangeBatch => format_decoded(
            ultrasql_wal::HeapDeleteInPlaceRangeBatchPayload::decode(&record.payload),
        ),
        RecordType::SequenceOp => {
            format_decoded(ultrasql_wal::SequenceOpPayload::decode(&record.payload))
        }
        RecordType::HashOp => format_decoded(ultrasql_wal::HashOpPayload::decode(&record.payload)),
        RecordType::HnswOp => format_decoded(ultrasql_wal::HnswOpPayload::decode(&record.payload)),
        RecordType::IvfFlatOp => {
            format_decoded(ultrasql_wal::IvfFlatOpPayload::decode(&record.payload))
        }
        RecordType::Nop => "decoded=Nop".to_string(),
    }
}

pub(crate) fn format_decoded<T: std::fmt::Debug>(
    decoded: Result<T, ultrasql_wal::PayloadError>,
) -> String {
    match decoded {
        Ok(payload) => format!("decoded={payload:?}"),
        Err(err) => format!("payload_error={err}"),
    }
}
