//! Sidecar-metadata token codecs for roles, privileges, RLS, and small
//! scalar lists.
//!
//! Moved verbatim from the crate root; behavior unchanged.
use super::*;

pub(crate) fn bind_regular_view_source_sql(
    source_sql: &str,
    catalog: &dyn PlannerCatalog,
) -> Result<LogicalPlan, ServerError> {
    let stmt = Parser::new(source_sql).parse_statement()?;
    if !matches!(stmt, ultrasql_parser::ast::Statement::Select(_)) {
        return Err(ServerError::Ddl(
            "view metadata source SQL is not a SELECT".to_owned(),
        ));
    }
    bind(&stmt, catalog).map_err(ServerError::Plan)
}

pub(crate) fn metadata_escape(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

pub(crate) fn metadata_encode_list(values: &[String]) -> String {
    let mut out = String::new();
    for value in values {
        out.push_str(&value.len().to_string());
        out.push(':');
        out.push_str(value);
    }
    out
}

pub(crate) fn metadata_decode_list(raw: &str) -> Result<Vec<String>, ServerError> {
    let mut values = Vec::new();
    let mut offset = 0;
    while offset < raw.len() {
        let Some(rel_colon) = raw[offset..].find(':') else {
            return Err(ServerError::Ddl(
                "malformed metadata list length".to_owned(),
            ));
        };
        let len_end = offset + rel_colon;
        let len = raw[offset..len_end]
            .parse::<usize>()
            .map_err(|err| ServerError::Ddl(format!("malformed metadata list length: {err}")))?;
        let value_start = len_end + 1;
        let value_end = value_start
            .checked_add(len)
            .ok_or_else(|| ServerError::Ddl("metadata list value length overflow".to_owned()))?;
        if value_end > raw.len() || !raw.is_char_boundary(value_end) {
            return Err(ServerError::Ddl(
                "metadata list value exceeds field length".to_owned(),
            ));
        }
        values.push(raw[value_start..value_end].to_owned());
        offset = value_end;
    }
    Ok(values)
}

pub(crate) fn metadata_unescape(raw: &str) -> Result<String, ServerError> {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('\\') => out.push('\\'),
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some(other) => {
                return Err(ServerError::Ddl(format!(
                    "invalid escaped metadata byte \\{other}"
                )));
            }
            None => return Err(ServerError::Ddl("trailing metadata escape".to_owned())),
        }
    }
    Ok(out)
}

pub(crate) fn format_password_hash(password: Option<&auth::PasswordHash>) -> String {
    let Some(password) = password else {
        return String::new();
    };
    format!(
        "SCRAM-SHA-256${}${}${}${}",
        password.iterations,
        B64.encode(&password.salt),
        B64.encode(password.stored_key),
        B64.encode(password.server_key)
    )
}

pub(crate) fn parse_password_hash(
    raw: &str,
    line_no: usize,
) -> Result<Option<auth::PasswordHash>, ServerError> {
    if raw.is_empty() {
        return Ok(None);
    }
    let parts = raw.split('$').collect::<Vec<_>>();
    if parts.len() != 5 || parts[0] != "SCRAM-SHA-256" {
        return Err(ServerError::ddl(format!(
            "role metadata line {} has malformed SCRAM password hash",
            line_no + 1
        )));
    }
    let iterations = parse_role_u32(parts[1], line_no, "password iterations")?;
    let salt = B64.decode(parts[2]).map_err(|err| {
        ServerError::ddl(format!(
            "role metadata line {} bad password salt: {err}",
            line_no + 1
        ))
    })?;
    let stored_key = decode_hash_key(parts[3], line_no, "stored key")?;
    let server_key = decode_hash_key(parts[4], line_no, "server key")?;
    Ok(Some(auth::PasswordHash {
        salt,
        iterations,
        stored_key,
        server_key,
    }))
}

pub(crate) fn decode_hash_key(raw: &str, line_no: usize, field: &str) -> Result<[u8; 32], ServerError> {
    let bytes = B64.decode(raw).map_err(|err| {
        ServerError::ddl(format!(
            "role metadata line {} bad password {field}: {err}",
            line_no + 1
        ))
    })?;
    bytes.try_into().map_err(|bytes: Vec<u8>| {
        ServerError::ddl(format!(
            "role metadata line {} password {field} has {} bytes, expected 32",
            line_no + 1,
            bytes.len()
        ))
    })
}

pub(crate) fn parse_role_bool(raw: &str, line_no: usize, field: &str) -> Result<bool, ServerError> {
    raw.parse::<bool>().map_err(|err| {
        ServerError::ddl(format!(
            "role metadata line {} bad {field}: {err}",
            line_no + 1
        ))
    })
}

pub(crate) fn validate_role_metadata_name(name: &str, line_no: usize, field: &str) -> Result<(), ServerError> {
    if !name.trim().is_empty() {
        return Ok(());
    }
    Err(ServerError::ddl(format!(
        "empty role metadata {field} on line {}",
        line_no + 1
    )))
}

pub(crate) fn validate_bootstrap_role_metadata(role: &auth::RoleEntry) -> Result<(), ServerError> {
    if role.is_superuser
        && role.inherit
        && role.create_role
        && role.create_db
        && role.can_login
        && role.connection_limit == -1
        && role.valid_until.is_none()
    {
        return Ok(());
    }
    Err(ServerError::ddl(
        "invalid bootstrap role metadata privileges for ultrasql",
    ))
}

pub(crate) fn parse_role_u32(raw: &str, line_no: usize, field: &str) -> Result<u32, ServerError> {
    raw.parse::<u32>().map_err(|err| {
        ServerError::ddl(format!(
            "role metadata line {} bad {field}: {err}",
            line_no + 1
        ))
    })
}

pub(crate) fn parse_role_i32(raw: &str, line_no: usize, field: &str) -> Result<i32, ServerError> {
    raw.parse::<i32>().map_err(|err| {
        ServerError::ddl(format!(
            "role metadata line {} bad {field}: {err}",
            line_no + 1
        ))
    })
}

pub(crate) fn parse_role_optional_i64(
    raw: &str,
    line_no: usize,
    field: &str,
) -> Result<Option<i64>, ServerError> {
    if raw.is_empty() {
        return Ok(None);
    }
    raw.parse::<i64>().map(Some).map_err(|err| {
        ServerError::ddl(format!(
            "role metadata line {} bad {field}: {err}",
            line_no + 1
        ))
    })
}

pub(crate) fn privilege_object_kind_name(kind: auth::PrivilegeObjectKind) -> &'static str {
    match kind {
        auth::PrivilegeObjectKind::Table => "table",
        auth::PrivilegeObjectKind::Schema => "schema",
        auth::PrivilegeObjectKind::Database => "database",
        auth::PrivilegeObjectKind::Sequence => "sequence",
        auth::PrivilegeObjectKind::Function => "function",
    }
}

pub(crate) fn parse_privilege_object_kind(
    raw: &str,
    line_no: usize,
) -> Result<auth::PrivilegeObjectKind, ServerError> {
    match raw {
        "table" => Ok(auth::PrivilegeObjectKind::Table),
        "schema" => Ok(auth::PrivilegeObjectKind::Schema),
        "database" => Ok(auth::PrivilegeObjectKind::Database),
        "sequence" => Ok(auth::PrivilegeObjectKind::Sequence),
        "function" => Ok(auth::PrivilegeObjectKind::Function),
        _ => Err(ServerError::ddl(format!(
            "privilege metadata line {} bad object kind",
            line_no + 1
        ))),
    }
}

pub(crate) fn privilege_kind_name(kind: auth::PrivilegeKind) -> &'static str {
    match kind {
        auth::PrivilegeKind::Select => "select",
        auth::PrivilegeKind::Insert => "insert",
        auth::PrivilegeKind::Update => "update",
        auth::PrivilegeKind::Delete => "delete",
        auth::PrivilegeKind::Truncate => "truncate",
        auth::PrivilegeKind::References => "references",
        auth::PrivilegeKind::Trigger => "trigger",
        auth::PrivilegeKind::Usage => "usage",
        auth::PrivilegeKind::Create => "create",
        auth::PrivilegeKind::Connect => "connect",
        auth::PrivilegeKind::Temporary => "temporary",
        auth::PrivilegeKind::Execute => "execute",
    }
}

pub(crate) fn parse_privilege_kind(raw: &str, line_no: usize) -> Result<auth::PrivilegeKind, ServerError> {
    match raw {
        "select" => Ok(auth::PrivilegeKind::Select),
        "insert" => Ok(auth::PrivilegeKind::Insert),
        "update" => Ok(auth::PrivilegeKind::Update),
        "delete" => Ok(auth::PrivilegeKind::Delete),
        "truncate" => Ok(auth::PrivilegeKind::Truncate),
        "references" => Ok(auth::PrivilegeKind::References),
        "trigger" => Ok(auth::PrivilegeKind::Trigger),
        "usage" => Ok(auth::PrivilegeKind::Usage),
        "create" => Ok(auth::PrivilegeKind::Create),
        "connect" => Ok(auth::PrivilegeKind::Connect),
        "temporary" => Ok(auth::PrivilegeKind::Temporary),
        "execute" => Ok(auth::PrivilegeKind::Execute),
        _ => Err(ServerError::ddl(format!(
            "privilege metadata line {} bad privilege kind",
            line_no + 1
        ))),
    }
}

pub(crate) fn validate_privilege_metadata_role(
    known_roles: &std::collections::HashSet<String>,
    role: &str,
    line_no: usize,
    field: &str,
) -> Result<(), ServerError> {
    if known_roles.contains(&role.to_ascii_lowercase()) {
        return Ok(());
    }
    Err(ServerError::ddl(format!(
        "unknown privilege metadata role '{role}' in {field} on line {}",
        line_no + 1
    )))
}

pub(crate) fn validate_privilege_metadata_grantee(
    known_roles: &std::collections::HashSet<String>,
    grantee: &str,
    line_no: usize,
) -> Result<(), ServerError> {
    if grantee.eq_ignore_ascii_case("public") {
        return Ok(());
    }
    validate_privilege_metadata_role(known_roles, grantee, line_no, "grantee")
}

pub(crate) fn runtime_metadata_known_role_names(
    role_catalog: &auth::InMemoryAuthCatalog,
) -> std::collections::HashSet<String> {
    let mut roles = role_catalog
        .list_roles()
        .into_iter()
        .map(|role| role.name.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    // Trust-mode tests already treat uncataloged `tester` as an effective superuser.
    roles.insert("tester".to_owned());
    roles
}

pub(crate) fn privilege_metadata_object_key(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

pub(crate) fn privilege_metadata_table_has_column(
    snapshot: &CatalogSnapshot,
    fallback: &InMemoryCatalog,
    object_name: &str,
    column_name: &str,
) -> Option<bool> {
    let table_name = privilege_metadata_object_key(object_name);
    if let Some(table) = snapshot.tables.get(&table_name) {
        return Some(table.schema.find(column_name).is_some());
    }
    PlannerCatalog::lookup_table(fallback, &table_name)
        .map(|table| table.schema.find(column_name).is_some())
}

pub(crate) fn validate_privilege_metadata_column(
    snapshot: &CatalogSnapshot,
    fallback: &InMemoryCatalog,
    grant: &auth::PrivilegeGrant,
    line_no: usize,
) -> Result<(), ServerError> {
    if grant.object_kind != auth::PrivilegeObjectKind::Table {
        return Ok(());
    }
    let Some(column_name) = grant.column_name.as_deref() else {
        return Ok(());
    };
    match privilege_metadata_table_has_column(snapshot, fallback, &grant.object_name, column_name) {
        Some(true) | None => Ok(()),
        Some(false) => Err(ServerError::ddl(format!(
            "unknown privilege metadata column '{column_name}' for table '{}' on line {}",
            grant.object_name,
            line_no + 1
        ))),
    }
}

pub(crate) fn rls_permissiveness_name(value: RuntimeRlsPermissiveness) -> &'static str {
    match value {
        RuntimeRlsPermissiveness::Permissive => "permissive",
        RuntimeRlsPermissiveness::Restrictive => "restrictive",
    }
}

pub(crate) fn parse_rls_permissiveness(value: &str) -> Result<RuntimeRlsPermissiveness, ServerError> {
    match value {
        "permissive" => Ok(RuntimeRlsPermissiveness::Permissive),
        "restrictive" => Ok(RuntimeRlsPermissiveness::Restrictive),
        other => Err(ServerError::Ddl(format!(
            "unknown RLS permissiveness {other}"
        ))),
    }
}

pub(crate) fn rls_command_name(value: RuntimeRlsCommand) -> &'static str {
    match value {
        RuntimeRlsCommand::All => "all",
        RuntimeRlsCommand::Select => "select",
        RuntimeRlsCommand::Insert => "insert",
        RuntimeRlsCommand::Update => "update",
        RuntimeRlsCommand::Delete => "delete",
    }
}

pub(crate) fn parse_rls_command(value: &str) -> Result<RuntimeRlsCommand, ServerError> {
    match value {
        "all" => Ok(RuntimeRlsCommand::All),
        "select" => Ok(RuntimeRlsCommand::Select),
        "insert" => Ok(RuntimeRlsCommand::Insert),
        "update" => Ok(RuntimeRlsCommand::Update),
        "delete" => Ok(RuntimeRlsCommand::Delete),
        other => Err(ServerError::Ddl(format!("unknown RLS command {other}"))),
    }
}

pub(crate) fn validate_rls_metadata_policy_roles(
    known_roles: &std::collections::HashSet<String>,
    roles: &mut [String],
    line_no: usize,
) -> Result<(), ServerError> {
    for role in roles {
        *role = role.to_ascii_lowercase();
        if role == "public" || known_roles.contains(role.as_str()) {
            continue;
        }
        return Err(ServerError::Ddl(format!(
            "unknown RLS policy role '{role}' on line {}",
            line_no + 1
        )));
    }
    Ok(())
}

pub(crate) fn validate_rls_metadata_expr(
    table: &TableEntry,
    expr: Option<&RuntimeTenantPolicyExpr>,
    line_no: usize,
    clause: &str,
) -> Result<(), ServerError> {
    let Some(expr) = expr else {
        return Ok(());
    };
    let Some(field) = table.schema.field(expr.column_index) else {
        return Err(ServerError::Ddl(format!(
            "RLS metadata line {} {clause} column index {} out of bounds for table '{}' with {} columns",
            line_no + 1,
            expr.column_index,
            table.name,
            table.schema.len()
        )));
    };
    if field.name.eq_ignore_ascii_case(&expr.column_name) {
        return Ok(());
    }
    Err(ServerError::Ddl(format!(
        "RLS metadata line {} {clause} column '{}' does not match table column '{}'",
        line_no + 1,
        expr.column_name,
        field.name
    )))
}

pub(crate) fn rls_expr_fields(expr: Option<&RuntimeTenantPolicyExpr>) -> (String, String, String) {
    expr.map_or_else(
        || (String::new(), String::new(), String::new()),
        |expr| {
            (
                expr.column_index.to_string(),
                metadata_escape(&expr.column_name),
                metadata_escape(&expr.setting_name),
            )
        },
    )
}

pub(crate) fn parse_rls_expr(
    index: &str,
    column_name: &str,
    setting_name: &str,
) -> Result<Option<RuntimeTenantPolicyExpr>, ServerError> {
    if index.is_empty() {
        return Ok(None);
    }
    Ok(Some(RuntimeTenantPolicyExpr {
        column_index: index
            .parse::<usize>()
            .map_err(|err| ServerError::Ddl(format!("bad RLS column index: {err}")))?,
        column_name: metadata_unescape(column_name)?,
        setting_name: metadata_unescape(setting_name)?,
    }))
}

pub(crate) fn usize_list_token(values: &[usize]) -> String {
    values
        .iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn parse_usize_list_token(raw: &str) -> Result<Vec<usize>, ServerError> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    raw.split(',')
        .map(|part| {
            part.parse::<usize>()
                .map_err(|err| ServerError::Ddl(format!("bad usize list entry: {err}")))
        })
        .collect()
}

pub(crate) fn referential_action_token(action: LogicalReferentialAction) -> &'static str {
    match action {
        LogicalReferentialAction::NoAction => "no_action",
        LogicalReferentialAction::Restrict => "restrict",
        LogicalReferentialAction::Cascade => "cascade",
        LogicalReferentialAction::SetNull => "set_null",
        LogicalReferentialAction::SetDefault => "set_default",
    }
}

pub(crate) fn index_method_token(method: LogicalIndexMethod) -> &'static str {
    match method {
        LogicalIndexMethod::Btree => "btree",
        LogicalIndexMethod::Hash => "hash",
        LogicalIndexMethod::Gin => "gin",
        LogicalIndexMethod::Gist => "gist",
        LogicalIndexMethod::Brin => "brin",
        LogicalIndexMethod::Hnsw => "hnsw",
        LogicalIndexMethod::IvfFlat => "ivfflat",
        LogicalIndexMethod::Aggregating => "aggregating",
    }
}

pub(crate) fn parse_index_method(raw: &str) -> Result<LogicalIndexMethod, ServerError> {
    match raw {
        "btree" => Ok(LogicalIndexMethod::Btree),
        "hash" => Ok(LogicalIndexMethod::Hash),
        "gin" => Ok(LogicalIndexMethod::Gin),
        "gist" => Ok(LogicalIndexMethod::Gist),
        "brin" => Ok(LogicalIndexMethod::Brin),
        "hnsw" => Ok(LogicalIndexMethod::Hnsw),
        "ivfflat" => Ok(LogicalIndexMethod::IvfFlat),
        "aggregating" => Ok(LogicalIndexMethod::Aggregating),
        other => Err(ServerError::Ddl(format!("unknown index method {other}"))),
    }
}

pub(crate) fn parse_referential_action(raw: &str) -> Result<LogicalReferentialAction, ServerError> {
    match raw {
        "no_action" => Ok(LogicalReferentialAction::NoAction),
        "restrict" => Ok(LogicalReferentialAction::Restrict),
        "cascade" => Ok(LogicalReferentialAction::Cascade),
        "set_null" => Ok(LogicalReferentialAction::SetNull),
        "set_default" => Ok(LogicalReferentialAction::SetDefault),
        other => Err(ServerError::Ddl(format!(
            "unknown referential action {other}"
        ))),
    }
}
