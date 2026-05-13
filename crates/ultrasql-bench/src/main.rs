//! UltraSQL benchmark harness.
//!
//! Drives standardized OLTP and OLAP workloads against any PostgreSQL-wire
//! database — UltraSQL, PostgreSQL, CockroachDB, etc. — and emits machine-
//! readable results into `benchmarks/results/`.

fn main() -> std::process::ExitCode {
    eprintln!("ultrasql-bench {} — not yet implemented", env!("CARGO_PKG_VERSION"));
    std::process::ExitCode::from(0)
}
