//! WAL record decoder fuzz target.
//!
//! Feeds arbitrary bytes into the WAL record decoder. Successfully decoded
//! records are encoded and decoded again to assert local record round-trip
//! stability.

#![no_main]

use libfuzzer_sys::fuzz_target;
use ultrasql_wal::WalRecord;

fuzz_target!(|data: &[u8]| {
    let Ok((record, used)) = WalRecord::decode(data) else {
        return;
    };
    if used > data.len() {
        panic!("WAL decoder consumed beyond input length");
    }
    let encoded = record.encode();
    match WalRecord::decode(&encoded) {
        Ok((round_trip, round_trip_used)) => {
            assert_eq!(round_trip, record);
            assert_eq!(round_trip_used, encoded.len());
        }
        Err(err) => panic!("encoded WAL record failed to decode: {err}"),
    }
});
