//! `ultrasqld` — UltraSQL database server.
//!
//! Binary entry point. Parses CLI arguments, initializes structured
//! logging, builds a Tokio runtime, registers the v0.5 sample
//! database, and runs the connection accept loop until shutdown.
//!
//! The actual session logic lives in the [`ultrasql_server`] library
//! crate so it can be exercised by unit tests against an in-memory
//! duplex stream as well as by integration tests over a real TCP
//! socket.

use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use tracing::error;
use tracing_subscriber::EnvFilter;
use ultrasql_server::{Server, run_server};

/// `ultrasqld` v0.5: PostgreSQL-wire-compatible server with an
/// in-memory sample database.
///
/// On startup the server registers a single table:
///
/// ```text
///     users(id INT, name TEXT, score DOUBLE PRECISION)
/// ```
///
/// pre-populated with three rows (Ada, Grace, Linus). Connect with
/// any PostgreSQL v3 client and run:
///
/// ```text
///     SELECT id FROM users;
///     SELECT id FROM users WHERE id = 2;
///     SELECT id FROM users LIMIT 1;
/// ```
#[derive(Debug, Parser)]
#[command(
    name = "ultrasqld",
    version,
    about = "UltraSQL database server",
    long_about = LONG_ABOUT
)]
struct Cli {
    /// Address to bind the PostgreSQL-wire listener on.
    #[arg(long, default_value = "127.0.0.1:5433")]
    listen: SocketAddr,

    /// Tracing level filter, e.g. `info`, `debug`, `ultrasqld=trace`.
    #[arg(long, default_value = "info")]
    log_level: String,
}

/// Long description shown by `--help`. Kept as a separate constant so
/// rustfmt does not split it across lines that mangle the indentation.
const LONG_ABOUT: &str = "UltraSQL database server (v0.5).

Speaks the PostgreSQL wire protocol v3 and serves SELECT queries over an
in-memory sample database. The catalog is pre-populated with:

    users(id INT, name TEXT, score DOUBLE PRECISION)
    -- 3 rows: Ada/Grace/Linus

Connect with any libpq-compatible client. Example session:

    psql -h 127.0.0.1 -p 5433 -d ultrasql -c 'SELECT id FROM users;'

Supported query shapes in v0.5:
  - SELECT col [, col]* FROM users
  - SELECT col FROM users WHERE int_col = literal
  - ... LIMIT n
";

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    if let Err(e) = init_tracing(&cli.log_level) {
        eprintln!("ultrasqld: failed to initialise tracing: {e}");
        return std::process::ExitCode::from(1);
    }

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            error!(target: "ultrasqld", error = %e, "failed to build tokio runtime");
            return std::process::ExitCode::from(1);
        }
    };

    let state = Arc::new(Server::with_sample_database());
    let outcome = runtime.block_on(run_server(cli.listen, state));
    match outcome {
        Ok(()) => std::process::ExitCode::from(0),
        Err(e) => {
            error!(target: "ultrasqld", error = %e, "server terminated with error");
            std::process::ExitCode::from(1)
        }
    }
}

fn init_tracing(filter: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let env_filter = EnvFilter::try_new(filter)?;
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .try_init()?;
    Ok(())
}
