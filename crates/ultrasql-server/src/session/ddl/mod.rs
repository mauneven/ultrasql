//! Part of the `session` module split. The
//! `impl<RW> Session<RW>` block is reopened across the submodules here
//! to add a handful of methods to the type defined in `session/mod.rs`.
//! Splitting across files keeps every unit under the 600-line ceiling
//! without changing semantics.

mod create_index;
mod create_index_build;
mod create_index_build_btree;
pub(crate) mod create_table;
mod drop;
mod index_options;
mod types;
mod views;

use ultrasql_catalog::TableEntry;

fn table_entry_lookup_key(entry: &TableEntry) -> String {
    ultrasql_catalog::table_lookup_key(&entry.schema_name, &entry.name)
}

/// Report a best-effort DDL rollback-cleanup failure. These persistent-catalog
/// drops run on the error-recovery path after a half-created object; a failure
/// here cannot lose committed user data but can leave an orphaned catalog
/// entry, so surface it at `warn` rather than silently discarding the result.
fn log_failed_ddl_rollback<T, E: std::fmt::Display>(result: Result<T, E>, what: &str) {
    if let Err(e) = result {
        tracing::warn!(
            error = %e,
            operation = what,
            "best-effort DDL rollback cleanup failed; catalog may retain an orphaned entry"
        );
    }
}

struct CreateIndexProgressGuard<'a> {
    recorder: &'a crate::workload::WorkloadRecorder,
    pid: u32,
}

impl<'a> CreateIndexProgressGuard<'a> {
    fn new(
        recorder: &'a crate::workload::WorkloadRecorder,
        pid: u32,
        relid: u32,
        index_relid: u32,
        blocks_total: u32,
    ) -> Self {
        recorder.begin_create_index(pid, relid, index_relid, blocks_total);
        Self { recorder, pid }
    }

    fn update(&self, phase: &'static str, blocks_done: u32) {
        self.recorder
            .update_create_index(self.pid, phase, blocks_done);
    }
}

impl Drop for CreateIndexProgressGuard<'_> {
    fn drop(&mut self) {
        self.recorder.finish_create_index(self.pid);
    }
}
