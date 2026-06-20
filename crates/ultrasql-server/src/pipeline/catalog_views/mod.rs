//! Virtual `pg_catalog` and `information_schema` relations.
//!
//! These scans expose metadata from the same catalog snapshot used by the
//! binder. They are deliberately read-only and statement-local: a SELECT sees
//! the snapshot captured at statement start, matching normal catalog lookup.

mod common;
mod indexes;
mod infoschema;
mod objects;
mod pgproc;
mod pgtype;
mod relations;
mod replication;
mod roles;
mod stats;

use ultrasql_core::{Schema, Value};
use ultrasql_executor::{MemTableScan, Operator, build_batch};

use crate::error::ServerError;

use super::LowerCtx;

use self::indexes::*;
use self::infoschema::*;
use self::objects::*;
use self::pgproc::*;
use self::pgtype::*;
use self::relations::*;
use self::replication::*;
use self::roles::*;
use self::stats::*;

pub(crate) use self::pgproc::pg_proc_builtin_exists;

/// Return the schema for a virtual catalog relation or view.
#[must_use]
pub(crate) fn virtual_catalog_schema(name: &str) -> Option<Schema> {
    match normalized_name(name).as_str() {
        "pg_catalog.pg_namespace" => Some(schema_pg_namespace()),
        "pg_catalog.pg_class" => Some(schema_pg_class()),
        "pg_catalog.pg_attribute" => Some(schema_pg_attribute()),
        "pg_catalog.pg_attrdef" => Some(schema_pg_attrdef()),
        "pg_catalog.pg_type" => Some(schema_pg_type()),
        "pg_catalog.pg_am" => Some(schema_pg_am()),
        "pg_catalog.pg_range" => Some(schema_pg_range()),
        "pg_catalog.pg_collation" => Some(schema_pg_collation()),
        "pg_catalog.pg_enum" => Some(schema_pg_enum()),
        "pg_catalog.pg_index" => Some(schema_pg_index()),
        "pg_catalog.pg_inherits" => Some(schema_pg_inherits()),
        "pg_catalog.pg_constraint" => Some(schema_pg_constraint()),
        "pg_catalog.pg_policy" => Some(schema_pg_policy()),
        "pg_catalog.pg_sequence" => Some(schema_pg_sequence()),
        "pg_catalog.pg_operator" => Some(schema_pg_operator()),
        "pg_catalog.pg_depend" => Some(schema_pg_depend()),
        "pg_catalog.pg_description" => Some(schema_pg_description()),
        "pg_catalog.pg_statistic" => Some(schema_pg_statistic()),
        "pg_catalog.pg_statistic_ext" => Some(schema_pg_statistic_ext()),
        "pg_catalog.pg_tables" => Some(schema_pg_tables()),
        "pg_catalog.pg_indexes" => Some(schema_pg_indexes()),
        "pg_catalog.pg_views" => Some(schema_pg_views()),
        "pg_catalog.pg_matviews" => Some(schema_pg_matviews()),
        "pg_catalog.pg_sequences" => Some(schema_pg_sequences()),
        "pg_catalog.pg_roles" => Some(schema_pg_roles()),
        "pg_catalog.pg_auth_members" => Some(schema_pg_auth_members()),
        "pg_catalog.pg_user" => Some(schema_pg_user()),
        "pg_catalog.pg_get_keywords" => Some(schema_pg_get_keywords()),
        "pg_catalog.pg_settings" => Some(schema_pg_settings()),
        "pg_catalog.pg_stat_statements" => Some(schema_pg_stat_statements()),
        "pg_catalog.pg_locks" => Some(schema_pg_locks()),
        "pg_catalog.pg_stat_activity" => Some(schema_pg_stat_activity()),
        "pg_catalog.pg_stat_user_tables" => Some(schema_pg_stat_user_tables()),
        "pg_catalog.pg_stat_user_indexes" => Some(schema_pg_stat_user_indexes()),
        "pg_catalog.pg_statio_user_tables" => Some(schema_pg_statio_user_tables()),
        "pg_catalog.pg_statio_user_indexes" => Some(schema_pg_statio_user_indexes()),
        "pg_catalog.pg_stat_database" => Some(schema_pg_stat_database()),
        "pg_catalog.pg_stat_bgwriter" => Some(schema_pg_stat_bgwriter()),
        "pg_catalog.pg_stat_wal" => Some(schema_pg_stat_wal()),
        "pg_catalog.pg_stat_progress_vacuum" => Some(schema_pg_stat_progress_vacuum()),
        "pg_catalog.pg_stat_progress_analyze" => Some(schema_pg_stat_progress_analyze()),
        "pg_catalog.pg_stat_progress_create_index" => Some(schema_pg_stat_progress_create_index()),
        "pg_catalog.pg_replication_slots" => Some(schema_pg_replication_slots()),
        "pg_catalog.pg_stat_replication" => Some(schema_pg_stat_replication()),
        "pg_catalog.pg_stat_subscription" => Some(schema_pg_stat_subscription()),
        "pg_catalog.pg_publication" => Some(schema_pg_publication()),
        "pg_catalog.pg_subscription" => Some(schema_pg_subscription()),
        "pg_catalog.pg_publication_rel" => Some(schema_pg_publication_rel()),
        "pg_catalog.pg_publication_tables" => Some(schema_pg_publication_tables()),
        "pg_catalog.pg_proc" => Some(schema_pg_proc()),
        "pg_catalog.pg_database" => Some(schema_pg_database()),
        "information_schema.tables" => Some(schema_information_schema_tables()),
        "information_schema.columns" => Some(schema_information_schema_columns()),
        "information_schema.table_constraints" => {
            Some(schema_information_schema_table_constraints())
        }
        "information_schema.key_column_usage" => Some(schema_information_schema_key_column_usage()),
        "information_schema.referential_constraints" => {
            Some(schema_information_schema_referential_constraints())
        }
        "information_schema.check_constraints" => {
            Some(schema_information_schema_check_constraints())
        }
        "information_schema.schemata" => Some(schema_information_schema_schemata()),
        "information_schema.sequences" => Some(schema_information_schema_sequences()),
        "information_schema.routines" => Some(schema_information_schema_routines()),
        "information_schema.triggers" => Some(schema_information_schema_triggers()),
        _ => None,
    }
}

/// Build a scan for a virtual catalog relation when `table` names one.
pub(super) fn try_virtual_catalog_scan(
    table: &str,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    let normalized = normalized_name(table);
    let Some((schema, rows)) = virtual_rows(&normalized, ctx) else {
        return Ok(None);
    };
    let batches = if rows.is_empty() {
        Vec::new()
    } else {
        vec![build_batch(&rows, &schema)?]
    };
    Ok(Some(Box::new(MemTableScan::new(schema, batches))))
}

fn virtual_rows(name: &str, ctx: &LowerCtx<'_>) -> Option<(Schema, Vec<Vec<Value>>)> {
    match name {
        "pg_catalog.pg_namespace" => Some((schema_pg_namespace(), rows_pg_namespace(ctx))),
        "pg_catalog.pg_class" => Some((schema_pg_class(), rows_pg_class(ctx))),
        "pg_catalog.pg_attribute" => Some((schema_pg_attribute(), rows_pg_attribute(ctx))),
        "pg_catalog.pg_attrdef" => Some((schema_pg_attrdef(), rows_pg_attrdef(ctx))),
        "pg_catalog.pg_type" => Some((schema_pg_type(), rows_pg_type(ctx))),
        "pg_catalog.pg_am" => Some((schema_pg_am(), rows_pg_am())),
        "pg_catalog.pg_range" => Some((schema_pg_range(), rows_pg_range())),
        "pg_catalog.pg_collation" => Some((schema_pg_collation(), rows_pg_collation())),
        "pg_catalog.pg_enum" => Some((schema_pg_enum(), rows_pg_enum(ctx))),
        "pg_catalog.pg_index" => Some((schema_pg_index(), rows_pg_index(ctx))),
        "pg_catalog.pg_inherits" => Some((schema_pg_inherits(), Vec::new())),
        "pg_catalog.pg_constraint" => Some((schema_pg_constraint(), rows_pg_constraint(ctx))),
        "pg_catalog.pg_policy" => Some((schema_pg_policy(), rows_pg_policy(ctx))),
        "pg_catalog.pg_sequence" => Some((schema_pg_sequence(), rows_pg_sequence(ctx))),
        "pg_catalog.pg_operator" => Some((schema_pg_operator(), rows_pg_operator(ctx))),
        "pg_catalog.pg_depend" => Some((schema_pg_depend(), rows_pg_depend(ctx))),
        "pg_catalog.pg_description" => Some((schema_pg_description(), rows_pg_description(ctx))),
        "pg_catalog.pg_statistic" => Some((schema_pg_statistic(), rows_pg_statistic(ctx))),
        "pg_catalog.pg_statistic_ext" => {
            Some((schema_pg_statistic_ext(), rows_pg_statistic_ext(ctx)))
        }
        "pg_catalog.pg_tables" => Some((schema_pg_tables(), rows_pg_tables(ctx))),
        "pg_catalog.pg_indexes" => Some((schema_pg_indexes(), rows_pg_indexes(ctx))),
        "pg_catalog.pg_views" => Some((schema_pg_views(), rows_pg_views(ctx))),
        "pg_catalog.pg_matviews" => Some((schema_pg_matviews(), rows_pg_matviews(ctx))),
        "pg_catalog.pg_sequences" => Some((schema_pg_sequences(), rows_pg_sequences(ctx))),
        "pg_catalog.pg_roles" => Some((schema_pg_roles(), rows_pg_roles(ctx))),
        "pg_catalog.pg_auth_members" => Some((schema_pg_auth_members(), rows_pg_auth_members(ctx))),
        "pg_catalog.pg_user" => Some((schema_pg_user(), rows_pg_user(ctx))),
        "pg_catalog.pg_get_keywords" => Some((schema_pg_get_keywords(), rows_pg_get_keywords())),
        "pg_catalog.pg_settings" => Some((schema_pg_settings(), rows_pg_settings(ctx))),
        "pg_catalog.pg_stat_statements" => {
            Some((schema_pg_stat_statements(), rows_pg_stat_statements(ctx)))
        }
        "pg_catalog.pg_locks" => Some((schema_pg_locks(), rows_pg_locks(ctx))),
        "pg_catalog.pg_stat_activity" => {
            Some((schema_pg_stat_activity(), rows_pg_stat_activity(ctx)))
        }
        "pg_catalog.pg_stat_user_tables" => {
            Some((schema_pg_stat_user_tables(), rows_pg_stat_user_tables(ctx)))
        }
        "pg_catalog.pg_stat_user_indexes" => Some((
            schema_pg_stat_user_indexes(),
            rows_pg_stat_user_indexes(ctx),
        )),
        "pg_catalog.pg_statio_user_tables" => Some((
            schema_pg_statio_user_tables(),
            rows_pg_statio_user_tables(ctx),
        )),
        "pg_catalog.pg_statio_user_indexes" => Some((
            schema_pg_statio_user_indexes(),
            rows_pg_statio_user_indexes(ctx),
        )),
        "pg_catalog.pg_stat_database" => {
            Some((schema_pg_stat_database(), rows_pg_stat_database(ctx)))
        }
        "pg_catalog.pg_stat_bgwriter" => {
            Some((schema_pg_stat_bgwriter(), rows_pg_stat_bgwriter(ctx)))
        }
        "pg_catalog.pg_stat_wal" => Some((schema_pg_stat_wal(), rows_pg_stat_wal(ctx))),
        "pg_catalog.pg_stat_progress_vacuum" => Some((
            schema_pg_stat_progress_vacuum(),
            rows_pg_stat_progress_vacuum(ctx),
        )),
        "pg_catalog.pg_stat_progress_analyze" => Some((
            schema_pg_stat_progress_analyze(),
            rows_pg_stat_progress_analyze(ctx),
        )),
        "pg_catalog.pg_stat_progress_create_index" => Some((
            schema_pg_stat_progress_create_index(),
            rows_pg_stat_progress_create_index(ctx),
        )),
        "pg_catalog.pg_replication_slots" => Some((
            schema_pg_replication_slots(),
            rows_pg_replication_slots(ctx),
        )),
        "pg_catalog.pg_stat_replication" => {
            Some((schema_pg_stat_replication(), rows_pg_stat_replication(ctx)))
        }
        "pg_catalog.pg_stat_subscription" => Some((
            schema_pg_stat_subscription(),
            rows_pg_stat_subscription(ctx),
        )),
        "pg_catalog.pg_publication" => Some((schema_pg_publication(), rows_pg_publication(ctx))),
        "pg_catalog.pg_subscription" => Some((schema_pg_subscription(), rows_pg_subscription(ctx))),
        "pg_catalog.pg_publication_rel" => {
            Some((schema_pg_publication_rel(), rows_pg_publication_rel(ctx)))
        }
        "pg_catalog.pg_publication_tables" => Some((
            schema_pg_publication_tables(),
            rows_pg_publication_tables(ctx),
        )),
        "pg_catalog.pg_proc" => Some((schema_pg_proc(), rows_pg_proc())),
        "pg_catalog.pg_database" => Some((schema_pg_database(), rows_pg_database())),
        "information_schema.tables" => Some((
            schema_information_schema_tables(),
            rows_information_schema_tables(ctx),
        )),
        "information_schema.columns" => Some((
            schema_information_schema_columns(),
            rows_information_schema_columns(ctx),
        )),
        "information_schema.table_constraints" => Some((
            schema_information_schema_table_constraints(),
            rows_information_schema_table_constraints(ctx),
        )),
        "information_schema.key_column_usage" => Some((
            schema_information_schema_key_column_usage(),
            rows_information_schema_key_column_usage(ctx),
        )),
        "information_schema.referential_constraints" => Some((
            schema_information_schema_referential_constraints(),
            rows_information_schema_referential_constraints(ctx),
        )),
        "information_schema.check_constraints" => Some((
            schema_information_schema_check_constraints(),
            rows_information_schema_check_constraints(ctx),
        )),
        "information_schema.schemata" => Some((
            schema_information_schema_schemata(),
            rows_information_schema_schemata(ctx),
        )),
        "information_schema.sequences" => Some((
            schema_information_schema_sequences(),
            rows_information_schema_sequences(ctx),
        )),
        "information_schema.routines" => Some((
            schema_information_schema_routines(),
            rows_information_schema_routines(),
        )),
        "information_schema.triggers" => Some((schema_information_schema_triggers(), Vec::new())),
        _ => None,
    }
}

fn normalized_name(name: &str) -> String {
    let folded = name.to_ascii_lowercase();
    if folded.contains('.') {
        return folded;
    }
    match folded.as_str() {
        "pg_namespace"
        | "pg_class"
        | "pg_attribute"
        | "pg_attrdef"
        | "pg_type"
        | "pg_am"
        | "pg_range"
        | "pg_collation"
        | "pg_enum"
        | "pg_index"
        | "pg_inherits"
        | "pg_constraint"
        | "pg_policy"
        | "pg_sequence"
        | "pg_operator"
        | "pg_depend"
        | "pg_description"
        | "pg_tables"
        | "pg_indexes"
        | "pg_statistic"
        | "pg_statistic_ext"
        | "pg_views"
        | "pg_matviews"
        | "pg_sequences"
        | "pg_roles"
        | "pg_auth_members"
        | "pg_user"
        | "pg_get_keywords"
        | "pg_settings"
        | "pg_stat_statements"
        | "pg_locks"
        | "pg_stat_activity"
        | "pg_proc"
        | "pg_stat_user_tables"
        | "pg_stat_user_indexes"
        | "pg_statio_user_tables"
        | "pg_statio_user_indexes"
        | "pg_stat_database"
        | "pg_stat_bgwriter"
        | "pg_stat_wal"
        | "pg_stat_progress_vacuum"
        | "pg_stat_progress_analyze"
        | "pg_stat_progress_create_index"
        | "pg_replication_slots"
        | "pg_stat_replication"
        | "pg_stat_subscription"
        | "pg_publication"
        | "pg_subscription"
        | "pg_publication_rel"
        | "pg_publication_tables"
        | "pg_database" => {
            format!("pg_catalog.{folded}")
        }
        "tables"
        | "columns"
        | "table_constraints"
        | "key_column_usage"
        | "referential_constraints"
        | "check_constraints"
        | "schemata"
        | "sequences"
        | "routines"
        | "triggers" => {
            format!("information_schema.{folded}")
        }
        _ => folded,
    }
}
