//! Parser fuzz target.
//!
//! Feeds arbitrary bytes (interpreted as UTF-8) into the parser. The
//! contract is "no panic on any input": the parser should always
//! return either `Ok(stmts)` or a typed `ParseError`. Any panic or
//! abort surfaces as a libfuzzer crash.
//!
//! Build:
//!   cargo +nightly fuzz build parser_fuzz
//! Run:
//!   cargo +nightly fuzz run parser_fuzz -- -max_total_time=60

#![no_main]
use libfuzzer_sys::fuzz_target;
use ultrasql_parser::Parser;

fuzz_target!(|data: &[u8]| {
    let Ok(s) = std::str::from_utf8(data) else { return; };
    let _ = Parser::new(s).parse_statements();
});
