//! `ultrasqld` — UltraSQL database server.
//!
//! Binary entry point. Parses CLI args, loads configuration, initializes
//! the storage stack, opens the listening socket, and runs the connection
//! accept loop until shutdown.

fn main() -> std::process::ExitCode {
    eprintln!("ultrasqld {} — not yet implemented", env!("CARGO_PKG_VERSION"));
    std::process::ExitCode::from(0)
}
