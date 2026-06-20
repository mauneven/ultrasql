//! Role, membership, and session-setting scans: `pg_roles`, `pg_auth_members`,
//! `pg_user`, `pg_get_keywords`, and `pg_settings`.

use std::collections::HashMap;

use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_txn::IsolationLevel;

use crate::pipeline::LowerCtx;

use super::common::*;

pub(super) fn schema_pg_roles() -> Schema {
    schema([
        Field::required("rolname", text()),
        Field::required("rolsuper", DataType::Bool),
        Field::required("rolinherit", DataType::Bool),
        Field::required("rolcreaterole", DataType::Bool),
        Field::required("rolcreatedb", DataType::Bool),
        Field::required("rolcanlogin", DataType::Bool),
        Field::required("rolreplication", DataType::Bool),
        Field::required("rolbypassrls", DataType::Bool),
        Field::required("rolconnlimit", DataType::Int32),
        Field::nullable("rolpassword", text()),
        Field::nullable("rolvaliduntil", DataType::TimestampTz),
        Field::nullable("rolconfig", text()),
        Field::required("oid", DataType::Int64),
    ])
}

pub(super) fn rows_pg_roles(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    ctx.role_catalog
        .list_roles()
        .into_iter()
        .map(|role| {
            vec![
                v_text(&role.name),
                Value::Bool(role.is_superuser),
                Value::Bool(role.inherit),
                Value::Bool(role.create_role),
                Value::Bool(role.create_db),
                Value::Bool(role.can_login),
                Value::Bool(role.replication),
                Value::Bool(role.bypass_rls),
                Value::Int32(role.connection_limit),
                masked_password_value(role.password.is_some()),
                role.valid_until.map_or(Value::Null, Value::TimestampTz),
                Value::Null,
                Value::Int64(i64::from(role.oid)),
            ]
        })
        .collect()
}

pub(super) fn schema_pg_auth_members() -> Schema {
    schema([
        Field::required("roleid", DataType::Int64),
        Field::required("member", DataType::Int64),
        Field::required("grantor", DataType::Int64),
        Field::required("admin_option", DataType::Bool),
    ])
}

pub(super) fn rows_pg_auth_members(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let roles = role_oid_map(ctx);
    ctx.role_catalog
        .list_memberships()
        .into_iter()
        .filter_map(|membership| {
            let roleid = roles.get(&membership.role).copied()?;
            let member = roles.get(&membership.member).copied()?;
            let grantor = roles.get(&membership.grantor).copied()?;
            Some(vec![
                Value::Int64(roleid),
                Value::Int64(member),
                Value::Int64(grantor),
                Value::Bool(membership.admin_option),
            ])
        })
        .collect()
}

pub(super) fn schema_pg_user() -> Schema {
    schema([
        Field::required("usename", text()),
        Field::required("usesysid", DataType::Int64),
        Field::required("usecreatedb", DataType::Bool),
        Field::required("usesuper", DataType::Bool),
        Field::required("userepl", DataType::Bool),
        Field::required("usebypassrls", DataType::Bool),
        Field::nullable("passwd", text()),
        Field::nullable("valuntil", DataType::TimestampTz),
        Field::nullable("useconfig", text()),
    ])
}

pub(super) fn rows_pg_user(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    ctx.role_catalog
        .list_roles()
        .into_iter()
        .filter(|role| role.can_login)
        .map(|role| {
            vec![
                v_text(&role.name),
                Value::Int64(i64::from(role.oid)),
                Value::Bool(role.create_db),
                Value::Bool(role.is_superuser),
                Value::Bool(role.replication),
                Value::Bool(role.bypass_rls),
                masked_password_value(role.password.is_some()),
                role.valid_until.map_or(Value::Null, Value::TimestampTz),
                Value::Null,
            ]
        })
        .collect()
}

pub(super) fn role_oid_map(ctx: &LowerCtx<'_>) -> HashMap<String, i64> {
    ctx.role_catalog
        .list_roles()
        .into_iter()
        .map(|role| (role.name, i64::from(role.oid)))
        .collect()
}

pub(super) fn schema_pg_get_keywords() -> Schema {
    schema([
        Field::required("word", text()),
        Field::required("catcode", DataType::Text { max_len: Some(1) }),
        Field::required("barelabel", DataType::Bool),
        Field::required("catdesc", text()),
        Field::required("baredesc", text()),
    ])
}

pub(super) fn rows_pg_get_keywords() -> Vec<Vec<Value>> {
    vec![vec![
        v_text("abort"),
        v_text("U"),
        Value::Bool(true),
        v_text("unreserved"),
        v_text("can be bare label"),
    ]]
}

pub(super) fn masked_password_value(has_password: bool) -> Value {
    if has_password {
        v_text("********")
    } else {
        Value::Null
    }
}

pub(super) fn schema_pg_settings() -> Schema {
    schema([
        Field::required("name", text()),
        Field::required("setting", text()),
        Field::nullable("unit", text()),
        Field::required("category", text()),
        Field::required("short_desc", text()),
        Field::required("vartype", text()),
        Field::required("context", text()),
    ])
}

pub(super) fn rows_pg_settings(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let autovacuum = ctx.autovacuum_config;
    vec![
        vec![
            v_text("server_version"),
            v_text(crate::REPORTED_SERVER_VERSION),
            Value::Null,
            v_text("Preset Options"),
            v_text("Wire version reported to drivers."),
            v_text("string"),
            v_text("internal"),
        ],
        vec![
            v_text("server_version_num"),
            v_text("140000"),
            Value::Null,
            v_text("Preset Options"),
            v_text("Server version number reported to drivers."),
            v_text("integer"),
            v_text("internal"),
        ],
        vec![
            v_text("server_encoding"),
            v_text("UTF8"),
            Value::Null,
            v_text("Client Connection Defaults / Locale and Formatting"),
            v_text("Sets the server character set encoding."),
            v_text("string"),
            v_text("internal"),
        ],
        vec![
            v_text("client_encoding"),
            v_text("UTF8"),
            Value::Null,
            v_text("Client Connection Defaults / Locale and Formatting"),
            v_text("Sets the client character set encoding."),
            v_text("string"),
            v_text("user"),
        ],
        vec![
            v_text("application_name"),
            v_text(session_setting(ctx, "application_name", "")),
            Value::Null,
            v_text("Reporting and Logging / What to Log"),
            v_text("Sets the application name reported in activity views."),
            v_text("string"),
            v_text("user"),
        ],
        vec![
            v_text("client_min_messages"),
            v_text(session_setting(ctx, "client_min_messages", "notice")),
            Value::Null,
            v_text("Client Connection Defaults / Statement Behavior"),
            v_text("Sets the message levels sent to the client."),
            v_text("enum"),
            v_text("user"),
        ],
        vec![
            v_text("DateStyle"),
            v_text(session_setting(ctx, "datestyle", "ISO, MDY")),
            Value::Null,
            v_text("Client Connection Defaults / Locale and Formatting"),
            v_text("Sets the display format for date and time values."),
            v_text("string"),
            v_text("user"),
        ],
        vec![
            v_text("extra_float_digits"),
            v_text(session_setting(ctx, "extra_float_digits", "1")),
            Value::Null,
            v_text("Client Connection Defaults / Locale and Formatting"),
            v_text("Sets the number of digits displayed for floating-point values."),
            v_text("integer"),
            v_text("user"),
        ],
        vec![
            v_text("IntervalStyle"),
            v_text(session_setting(ctx, "intervalstyle", "postgres")),
            Value::Null,
            v_text("Client Connection Defaults / Locale and Formatting"),
            v_text("Sets the display format for interval values."),
            v_text("enum"),
            v_text("user"),
        ],
        vec![
            v_text("lc_monetary"),
            v_text(session_setting(ctx, "lc_monetary", "C")),
            Value::Null,
            v_text("Client Connection Defaults / Locale and Formatting"),
            v_text("Sets the locale for formatting monetary amounts."),
            v_text("string"),
            v_text("user"),
        ],
        vec![
            v_text("max_identifier_length"),
            v_text("63"),
            Value::Null,
            v_text("Preset Options"),
            v_text("Shows the maximum identifier length in bytes."),
            v_text("integer"),
            v_text("internal"),
        ],
        vec![
            v_text("search_path"),
            v_text(session_setting(ctx, "search_path", "\"$user\", public")),
            Value::Null,
            v_text("Client Connection Defaults / Statement Behavior"),
            v_text("Sets the schema search order."),
            v_text("string"),
            v_text("user"),
        ],
        vec![
            v_text("transaction_isolation"),
            v_text(isolation_level_setting(ctx.isolation)),
            Value::Null,
            v_text("Client Connection Defaults / Statement Behavior"),
            v_text("Sets the current transaction isolation level."),
            v_text("enum"),
            v_text("user"),
        ],
        vec![
            v_text("standard_conforming_strings"),
            v_text("on"),
            Value::Null,
            v_text("Version and Platform Compatibility"),
            v_text("Causes string literals to treat backslashes literally."),
            v_text("bool"),
            v_text("user"),
        ],
        vec![
            v_text("statement_timeout"),
            v_text(session_setting(ctx, "statement_timeout", "0")),
            v_text("ms"),
            v_text("Client Connection Defaults / Statement Behavior"),
            v_text("Sets the maximum allowed duration of any statement."),
            v_text("integer"),
            v_text("user"),
        ],
        vec![
            v_text("TimeZone"),
            v_text(session_setting(ctx, "timezone", "UTC")),
            Value::Null,
            v_text("Client Connection Defaults / Locale and Formatting"),
            v_text("Sets the time zone for displaying and interpreting timestamps."),
            v_text("string"),
            v_text("user"),
        ],
        vec![
            v_text("work_mem"),
            v_text("4194304"),
            v_text("B"),
            v_text("Resource Usage / Memory"),
            v_text("Sets the maximum memory to use for query work areas."),
            v_text("integer"),
            v_text("user"),
        ],
        vec![
            v_text("autovacuum"),
            v_text("on"),
            Value::Null,
            v_text("Autovacuum"),
            v_text("Starts the autovacuum launcher."),
            v_text("bool"),
            v_text("sighup"),
        ],
        vec![
            v_text("autovacuum_vacuum_threshold"),
            v_text(autovacuum.vacuum_threshold.to_string()),
            Value::Null,
            v_text("Autovacuum"),
            v_text("Minimum dead tuples before vacuum."),
            v_text("integer"),
            v_text("sighup"),
        ],
        vec![
            v_text("autovacuum_vacuum_scale_factor"),
            v_text(format_scale_factor(autovacuum.vacuum_scale_factor())),
            Value::Null,
            v_text("Autovacuum"),
            v_text("Fraction of table size before vacuum."),
            v_text("real"),
            v_text("sighup"),
        ],
        vec![
            v_text("autovacuum_analyze_threshold"),
            v_text(autovacuum.analyze_threshold.to_string()),
            Value::Null,
            v_text("Autovacuum"),
            v_text("Minimum changed tuples before analyze."),
            v_text("integer"),
            v_text("sighup"),
        ],
        vec![
            v_text("autovacuum_analyze_scale_factor"),
            v_text(format_scale_factor(autovacuum.analyze_scale_factor())),
            Value::Null,
            v_text("Autovacuum"),
            v_text("Fraction of table size before analyze."),
            v_text("real"),
            v_text("sighup"),
        ],
        vec![
            v_text("synchronous_commit"),
            v_text("on"),
            Value::Null,
            v_text("Write-Ahead Log / Settings"),
            v_text("Sets the commit durability level."),
            v_text("enum"),
            v_text("user"),
        ],
        vec![
            v_text("archive_command"),
            sensitive_setting_value(&ctx.wal_archive_config.archive_command),
            Value::Null,
            v_text("Write-Ahead Log / Archiving"),
            v_text("Command to archive completed WAL files."),
            v_text("string"),
            v_text("sighup"),
        ],
        vec![
            v_text("restore_command"),
            sensitive_setting_value(&ctx.wal_archive_config.restore_command),
            Value::Null,
            v_text("Write-Ahead Log / Recovery"),
            v_text("Command to restore archived WAL files."),
            v_text("string"),
            v_text("postmaster"),
        ],
        vec![
            v_text("log_connections"),
            v_text(if ctx.logging_config.log_connections {
                "on"
            } else {
                "off"
            }),
            Value::Null,
            v_text("Reporting and Logging / What to Log"),
            v_text("Logs each successful connection."),
            v_text("bool"),
            v_text("sighup"),
        ],
        vec![
            v_text("log_min_duration_statement"),
            v_text(ctx.logging_config.log_min_duration_statement_ms.to_string()),
            v_text("ms"),
            v_text("Reporting and Logging / When to Log"),
            v_text("Logs statements running at least this long."),
            v_text("integer"),
            v_text("sighup"),
        ],
        vec![
            v_text("log_statement"),
            v_text(ctx.logging_config.log_statement.as_str()),
            Value::Null,
            v_text("Reporting and Logging / What to Log"),
            v_text("Sets the statements logged by class."),
            v_text("enum"),
            v_text("sighup"),
        ],
    ]
}

pub(super) fn isolation_level_setting(isolation: IsolationLevel) -> &'static str {
    match isolation {
        IsolationLevel::ReadCommitted => "read committed",
        IsolationLevel::RepeatableRead => "repeatable read",
        IsolationLevel::Serializable => "serializable",
    }
}

pub(super) fn session_setting(ctx: &LowerCtx<'_>, name: &str, default: &'static str) -> String {
    ctx.session_settings
        .get(name)
        .cloned()
        .unwrap_or_else(|| default.to_owned())
}

pub(super) fn sensitive_setting_value(value: &str) -> Value {
    if value.is_empty() {
        v_text("")
    } else {
        v_text("<redacted>")
    }
}

pub(super) fn format_scale_factor(value: f64) -> String {
    let rendered = format!("{value:.6}");
    rendered
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_owned()
}
