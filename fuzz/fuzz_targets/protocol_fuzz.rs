//! PostgreSQL wire-protocol codec fuzz target.
//!
//! Feeds arbitrary bytes into frontend and backend decoders. The decoders
//! must either parse, request more bytes, or return a typed protocol error.

#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use ultrasql_protocol::{decode_backend, decode_frontend};

fuzz_target!(|data: &[u8]| {
    let mut frontend = BytesMut::from(data);
    let _ = decode_frontend(&mut frontend);

    let mut backend = BytesMut::from(data);
    let _ = decode_backend(&mut backend);
});
