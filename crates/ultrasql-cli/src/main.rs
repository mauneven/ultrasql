//! `ultrasql` — UltraSQL command-line client.
//!
//! Connects to an `ultrasqld` instance over the PostgreSQL wire protocol
//! and provides an interactive REPL plus a script-execution mode. Backslash
//! commands are compatible with a useful subset of psql.

fn main() -> std::process::ExitCode {
    eprintln!(
        "ultrasql {} — not yet implemented",
        env!("CARGO_PKG_VERSION")
    );
    std::process::ExitCode::from(0)
}
