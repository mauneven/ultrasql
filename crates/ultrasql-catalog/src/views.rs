//! `pg_catalog` view definitions.
//!
//! Each function in this module returns the SQL text definition of a
//! `pg_catalog` view. The definitions follow PostgreSQL 16's exact
//! column names and semantics; they are registered with the catalog at
//! server startup so client tools (psql, pgAdmin, JDBC drivers) can
//! query them.
//!
//! Views are expressed as `CREATE OR REPLACE VIEW pg_catalog.<name> AS
//! SELECT ...` strings. The executor is responsible for parsing and
//! materializing them; this module owns only the definition text.
//!
//! All definitions are intentionally simplified relative to PostgreSQL:
//! columns that depend on subsystems not yet implemented (e.g.
//! `pg_stat_activity.wait_event_type`) carry a fixed NULL or empty
//! literal until the subsystem ships.

/// Returns the SQL definition of `pg_tables`.
///
/// Columns: `schemaname`, `tablename`, `tableowner`, `tablespace`,
/// `hasindexes`, `hasrules`, `hastriggers`, `rowsecurity`.
#[must_use]
pub const fn pg_tables_def() -> &'static str {
    "CREATE OR REPLACE VIEW pg_catalog.pg_tables AS \
     SELECT n.nspname AS schemaname, \
            c.relname AS tablename, \
            r.rolname AS tableowner, \
            NULL::text AS tablespace, \
            c.relhasindex AS hasindexes, \
            false AS hasrules, \
            false AS hastriggers, \
            false AS rowsecurity \
     FROM pg_catalog.pg_class c \
     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
     LEFT JOIN pg_catalog.pg_roles r ON r.oid = c.relowner \
     WHERE c.relkind = 'r'"
}

/// Returns the SQL definition of `pg_indexes`.
///
/// Columns: `schemaname`, `tablename`, `indexname`, `tablespace`,
/// `indexdef`.
#[must_use]
pub const fn pg_indexes_def() -> &'static str {
    // `tablespace` stays NULL: UltraSQL has no tablespace subsystem, so
    // there is no real source to populate (PostgreSQL also reports NULL for
    // indexes in the default tablespace). `indexdef` delegates to
    // `pg_get_indexdef(i.oid)` — the index relation's oid — so psql `\d`/`\di`,
    // pgAdmin, and ORM schema reflection see a non-NULL definition instead of
    // breaking on a NULL.
    "CREATE OR REPLACE VIEW pg_catalog.pg_indexes AS \
     SELECT n.nspname AS schemaname, \
            t.relname AS tablename, \
            i.relname AS indexname, \
            NULL::text AS tablespace, \
            pg_catalog.pg_get_indexdef(i.oid) AS indexdef \
     FROM pg_catalog.pg_index ix \
     JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid \
     JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid \
     JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace \
     WHERE t.relkind = 'r'"
}

/// Returns the SQL definition of `pg_views`.
///
/// Columns: `schemaname`, `viewname`, `viewowner`, `definition`.
#[must_use]
pub const fn pg_views_def() -> &'static str {
    "CREATE OR REPLACE VIEW pg_catalog.pg_views AS \
     SELECT n.nspname AS schemaname, \
            c.relname AS viewname, \
            r.rolname AS viewowner, \
            NULL::text AS definition \
     FROM pg_catalog.pg_class c \
     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
     LEFT JOIN pg_catalog.pg_roles r ON r.oid = c.relowner \
     WHERE c.relkind = 'v'"
}

/// Returns the SQL definition of `pg_sequences`.
///
/// Columns: `schemaname`, `sequencename`, `sequenceowner`, `data_type`,
/// `start_value`, `min_value`, `max_value`, `increment_by`, `cycle`,
/// `cache_size`, `last_value`.
#[must_use]
pub const fn pg_sequences_def() -> &'static str {
    "CREATE OR REPLACE VIEW pg_catalog.pg_sequences AS \
     SELECT n.nspname AS schemaname, \
            c.relname AS sequencename, \
            r.rolname AS sequenceowner, \
            'bigint'::text AS data_type, \
            s.seqstart AS start_value, \
            s.seqmin AS min_value, \
            s.seqmax AS max_value, \
            s.seqincrement AS increment_by, \
            s.seqcycle AS cycle, \
            s.seqcache AS cache_size, \
            NULL::bigint AS last_value \
     FROM pg_catalog.pg_sequence s \
     JOIN pg_catalog.pg_class c ON c.oid = s.seqrelid \
     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
     LEFT JOIN pg_catalog.pg_roles r ON r.oid = c.relowner"
}

/// Returns the SQL definition of `pg_roles`.
///
/// Columns: `rolname`, `rolsuper`, `rolinherit`, `rolcreaterole`,
/// `rolcreatedb`, `rolcanlogin`, `rolreplication`, `rolbypassrls`,
/// `rolconnlimit`, `rolpassword`, `rolvaliduntil`, `rolconfig`, `oid`.
#[must_use]
pub const fn pg_roles_def() -> &'static str {
    "CREATE OR REPLACE VIEW pg_catalog.pg_roles AS \
     SELECT rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, \
            rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, \
            '********'::text AS rolpassword, rolvaliduntil, rolconfig, oid \
     FROM pg_catalog.pg_authid"
}

/// Returns the SQL definition of `pg_user`.
///
/// Columns: `usename`, `usesysid`, `usecreatedb`, `usesuper`,
/// `userepl`, `usebypassrls`, `passwd`, `valuntil`, `useconfig`.
#[must_use]
pub const fn pg_user_def() -> &'static str {
    "CREATE OR REPLACE VIEW pg_catalog.pg_user AS \
     SELECT usename, usesysid, usecreatedb, usesuper, userepl, \
            usebypassrls, \
            '********'::text AS passwd, \
            valuntil, useconfig \
     FROM pg_catalog.pg_shadow"
}

/// Returns the SQL definition of `pg_settings`.
///
/// Exposes the server configuration parameters. The full column set
/// matches `pg_settings` in PostgreSQL 16; columns derived from the
/// configuration subsystem are `NULLed` until that subsystem ships.
#[must_use]
pub const fn pg_settings_def() -> &'static str {
    "CREATE OR REPLACE VIEW pg_catalog.pg_settings AS \
     SELECT name, setting, unit, category, short_desc, extra_desc, \
            context, vartype, source, min_val, max_val, enumvals, \
            boot_val, reset_val, sourcefile, sourceline, pending_restart \
     FROM pg_catalog.pg_config_settings"
}

/// Returns the SQL definition of `pg_locks`.
///
/// Columns: `locktype`, `database`, `relation`, `page`, `tuple`,
/// `virtualxid`, `transactionid`, `classid`, `objid`, `objsubid`,
/// `virtualtransaction`, `pid`, `mode`, `granted`, `fastpath`,
/// `waitstart`.
#[must_use]
pub const fn pg_locks_def() -> &'static str {
    "CREATE OR REPLACE VIEW pg_catalog.pg_locks AS \
     SELECT locktype, database, relation, page, tuple, virtualxid, \
            transactionid, classid, objid, objsubid, virtualtransaction, \
            pid, mode, granted, fastpath, waitstart \
     FROM pg_catalog.pg_lock_status() AS \
          L(locktype, database, relation, page, tuple, virtualxid, \
            transactionid, classid, objid, objsubid, virtualtransaction, \
            pid, mode, granted, fastpath, waitstart)"
}

/// Returns the SQL definition of `pg_stat_activity`.
///
/// Columns: `datid`, `datname`, `pid`, `leader_pid`, `usesysid`,
/// `usename`, `application_name`, `client_addr`, `client_hostname`,
/// `client_port`, `backend_start`, `xact_start`, `query_start`,
/// `state_change`, `wait_event_type`, `wait_event`, `state`,
/// `backend_xid`, `backend_xmin`, `query_id`, `query`, `backend_type`.
#[must_use]
pub const fn pg_stat_activity_def() -> &'static str {
    "CREATE OR REPLACE VIEW pg_catalog.pg_stat_activity AS \
     SELECT datid, datname, pid, leader_pid, usesysid, usename, \
            application_name, client_addr, client_hostname, client_port, \
            backend_start, xact_start, query_start, state_change, \
            wait_event_type, wait_event, state, backend_xid, backend_xmin, \
            query_id, query, backend_type \
     FROM pg_catalog.pg_stat_get_activity(NULL) AS \
          S(datid, pid, usesysid, application_name, state, query, \
            wait_event_type, wait_event, xact_start, query_start, \
            backend_start, state_change, client_addr, client_hostname, \
            client_port, backend_xid, backend_xmin, leader_pid, \
            query_id, backend_type) \
     JOIN pg_catalog.pg_database ON pg_database.oid = S.datid \
     LEFT JOIN pg_catalog.pg_authid ON pg_authid.oid = S.usesysid"
}

/// All `pg_catalog` view definitions, keyed by view name.
///
/// Used at startup to register views with the catalog.
#[must_use]
pub fn all_pg_catalog_views() -> Vec<(&'static str, &'static str)> {
    vec![
        ("pg_tables", pg_tables_def()),
        ("pg_indexes", pg_indexes_def()),
        ("pg_views", pg_views_def()),
        ("pg_sequences", pg_sequences_def()),
        ("pg_roles", pg_roles_def()),
        ("pg_user", pg_user_def()),
        ("pg_settings", pg_settings_def()),
        ("pg_locks", pg_locks_def()),
        ("pg_stat_activity", pg_stat_activity_def()),
    ]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_views_are_non_empty() {
        for (name, def) in all_pg_catalog_views() {
            assert!(!def.is_empty(), "view {name} has empty definition");
        }
    }

    #[test]
    fn pg_tables_def_contains_relkind_filter() {
        assert!(
            pg_tables_def().contains("relkind"),
            "pg_tables must filter by relkind"
        );
    }

    #[test]
    fn pg_indexes_def_references_pg_index() {
        assert!(
            pg_indexes_def().contains("pg_index"),
            "pg_indexes must join pg_index"
        );
    }

    #[test]
    fn pg_indexes_def_populates_indexdef_via_pg_get_indexdef() {
        let def = pg_indexes_def();
        assert!(
            def.contains("pg_get_indexdef(i.oid) AS indexdef"),
            "pg_indexes.indexdef must delegate to pg_get_indexdef(i.oid), got: {def}"
        );
        assert!(
            !def.contains("NULL::text AS indexdef"),
            "pg_indexes.indexdef must no longer be a NULL literal"
        );
    }

    #[test]
    fn all_view_names_are_unique() {
        let mut names: Vec<&str> = all_pg_catalog_views().iter().map(|(n, _)| *n).collect();
        names.sort_unstable();
        let original_len = names.len();
        names.dedup();
        assert_eq!(
            names.len(),
            original_len,
            "duplicate view name in all_pg_catalog_views"
        );
    }

    #[test]
    fn pg_sequences_def_contains_seqcycle() {
        assert!(
            pg_sequences_def().contains("seqcycle"),
            "pg_sequences must expose seqcycle"
        );
    }
}
