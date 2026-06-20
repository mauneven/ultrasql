//! `impl Server` methods (split out of the crate root): meta_role_priv.
//!
//! Pure code motion from `lib.rs`; behavior unchanged.
use super::*;

impl Server {
    pub(crate) fn rebuild_table_runtime_constraint_sidecars(&self) -> Result<(), ServerError> {
        let Some(path) = self.table_runtime_metadata_path() else {
            return Ok(());
        };
        let Some(text) = read_runtime_metadata_file(&path)? else {
            return Ok(());
        };
        let snapshot = self.catalog_snapshot();
        let mut table_names: std::collections::HashMap<Oid, String> =
            std::collections::HashMap::new();
        let mut sequence_defaults: std::collections::HashMap<Oid, Vec<(usize, String)>> =
            std::collections::HashMap::new();
        let mut defaults: std::collections::HashMap<Oid, Vec<(usize, ScalarExpr)>> =
            std::collections::HashMap::new();
        let mut identity_always: std::collections::HashMap<Oid, Vec<usize>> =
            std::collections::HashMap::new();
        let mut generated_stored: std::collections::HashMap<Oid, Vec<(usize, ScalarExpr)>> =
            std::collections::HashMap::new();
        let mut checks: std::collections::HashMap<Oid, Vec<RuntimeCheckConstraint>> =
            std::collections::HashMap::new();
        let mut foreign_keys: std::collections::HashMap<Oid, Vec<RuntimeForeignKeyConstraint>> =
            std::collections::HashMap::new();
        let mut exclusions: std::collections::HashMap<Oid, Vec<RuntimeExclusionConstraint>> =
            std::collections::HashMap::new();
        let mut indexes: std::collections::HashMap<Oid, Vec<(Oid, RuntimeIndexMetadata)>> =
            std::collections::HashMap::new();
        let mut seen_table_oids = std::collections::HashSet::new();
        let mut seen_sequence_default_keys = std::collections::HashSet::new();
        let mut seen_default_keys = std::collections::HashSet::new();
        let mut seen_identity_keys = std::collections::HashSet::new();
        let mut seen_generated_keys = std::collections::HashSet::new();
        let mut seen_check_keys = std::collections::HashSet::new();
        let mut seen_foreign_key_keys = std::collections::HashSet::new();
        let mut seen_exclusion_keys = std::collections::HashSet::new();
        let mut seen_index_keys = std::collections::HashSet::new();
        let mut skipped_stale_index_metadata = false;
        for (line_no, line) in text.lines().enumerate() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts = line.split('\t').collect::<Vec<_>>();
            match parts.first().copied() {
                Some("table") if parts.len() == 3 => {
                    let oid = Oid::new(parts[2].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    let table_name = metadata_unescape(parts[1])?;
                    if !seen_table_oids.insert(oid) {
                        return Err(ServerError::Ddl(format!(
                            "duplicate table-runtime metadata on line {}",
                            line_no + 1
                        )));
                    }
                    table_names.insert(oid, table_name);
                }
                Some("sequence_default") if parts.len() == 4 => {
                    let oid = Oid::new(parts[1].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    let idx = parts[2].parse::<usize>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad column index: {err}",
                            line_no + 1
                        ))
                    })?;
                    if !seen_sequence_default_keys.insert((oid, idx)) {
                        return Err(ServerError::Ddl(format!(
                            "duplicate table-runtime sequence default metadata on line {}",
                            line_no + 1
                        )));
                    }
                    sequence_defaults
                        .entry(oid)
                        .or_default()
                        .push((idx, metadata_unescape(parts[3])?));
                }
                Some("default") if parts.len() == 4 => {
                    let oid = Oid::new(parts[1].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    let idx = parts[2].parse::<usize>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad column index: {err}",
                            line_no + 1
                        ))
                    })?;
                    if !seen_default_keys.insert((oid, idx)) {
                        return Err(ServerError::Ddl(format!(
                            "duplicate table-runtime default metadata on line {}",
                            line_no + 1
                        )));
                    }
                    defaults.entry(oid).or_default().push((
                        idx,
                        decode_scalar_expr_field(&metadata_unescape(parts[3])?)?,
                    ));
                }
                Some("identity_always") if parts.len() == 3 => {
                    let oid = Oid::new(parts[1].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    let idx = parts[2].parse::<usize>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad column index: {err}",
                            line_no + 1
                        ))
                    })?;
                    if !seen_identity_keys.insert((oid, idx)) {
                        return Err(ServerError::Ddl(format!(
                            "duplicate table-runtime identity metadata on line {}",
                            line_no + 1
                        )));
                    }
                    identity_always.entry(oid).or_default().push(idx);
                }
                Some("generated_stored") if parts.len() == 4 => {
                    let oid = Oid::new(parts[1].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    let idx = parts[2].parse::<usize>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad column index: {err}",
                            line_no + 1
                        ))
                    })?;
                    if !seen_generated_keys.insert((oid, idx)) {
                        return Err(ServerError::Ddl(format!(
                            "duplicate table-runtime generated metadata on line {}",
                            line_no + 1
                        )));
                    }
                    generated_stored.entry(oid).or_default().push((
                        idx,
                        decode_scalar_expr_field(&metadata_unescape(parts[3])?)?,
                    ));
                }
                Some("check") if parts.len() == 4 => {
                    let oid = Oid::new(parts[1].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    let name = metadata_unescape(parts[2])?;
                    if !seen_check_keys.insert((oid, name.to_ascii_lowercase())) {
                        return Err(ServerError::Ddl(format!(
                            "duplicate table-runtime check metadata on line {}",
                            line_no + 1
                        )));
                    }
                    checks.entry(oid).or_default().push(RuntimeCheckConstraint {
                        name,
                        expr: decode_scalar_expr_field(&metadata_unescape(parts[3])?)?,
                    });
                }
                Some("foreign_key") if parts.len() == 11 => {
                    let oid = Oid::new(parts[1].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    let name = metadata_unescape(parts[2])?;
                    if !seen_foreign_key_keys.insert((oid, name.to_ascii_lowercase())) {
                        return Err(ServerError::Ddl(format!(
                            "duplicate table-runtime foreign-key metadata on line {}",
                            line_no + 1
                        )));
                    }
                    foreign_keys
                        .entry(oid)
                        .or_default()
                        .push(RuntimeForeignKeyConstraint {
                        name,
                        columns: parse_usize_list_token(parts[3])?,
                        target_table: metadata_unescape(parts[4])?,
                        target_oid: Oid::new(parts[5].parse::<u32>().map_err(|err| {
                            ServerError::Ddl(format!(
                                "table-runtime metadata line {} bad target oid: {err}",
                                line_no + 1
                            ))
                        })?),
                        target_columns: parse_usize_list_token(parts[6])?,
                        on_delete: parse_referential_action(parts[7])?,
                        on_update: parse_referential_action(parts[8])?,
                        deferrable: parts[9].parse::<bool>().map_err(|err| {
                            ServerError::Ddl(format!(
                                "table-runtime metadata line {} bad deferrable flag: {err}",
                                line_no + 1
                            ))
                        })?,
                        initially_deferred: parts[10].parse::<bool>().map_err(|err| {
                            ServerError::Ddl(format!(
                                "table-runtime metadata line {} bad initially_deferred flag: {err}",
                                line_no + 1
                            ))
                        })?,
                    });
                }
                Some("exclusion") if parts.len() == 5 => {
                    let oid = Oid::new(parts[1].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    let name = metadata_unescape(parts[2])?;
                    if !seen_exclusion_keys.insert((oid, name.to_ascii_lowercase())) {
                        return Err(ServerError::Ddl(format!(
                            "duplicate table-runtime exclusion metadata on line {}",
                            line_no + 1
                        )));
                    }
                    let mut elements = Vec::new();
                    if !parts[4].is_empty() {
                        for raw in parts[4].split(',') {
                            let (column, op) = raw.split_once(':').ok_or_else(|| {
                                ServerError::Ddl(format!(
                                    "table-runtime metadata line {} bad exclusion element",
                                    line_no + 1
                                ))
                            })?;
                            elements.push(RuntimeExclusionElement {
                                column: column.parse::<usize>().map_err(|err| {
                                    ServerError::Ddl(format!(
                                        "table-runtime metadata line {} bad exclusion column: {err}",
                                        line_no + 1
                                    ))
                                })?,
                                op: binary_op_from_token(op).ok_or_else(|| {
                                    ServerError::Ddl(format!(
                                        "table-runtime metadata line {} bad exclusion op",
                                        line_no + 1
                                    ))
                                })?,
                            });
                        }
                    }
                    exclusions
                        .entry(oid)
                        .or_default()
                        .push(RuntimeExclusionConstraint {
                            name,
                            method: parse_index_method(parts[3])?,
                            elements,
                        });
                }
                Some("index") if parts.len() == 7 => {
                    let oid = Oid::new(parts[1].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    let index_oid = Oid::new(parts[2].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "table-runtime metadata line {} bad index oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    if !seen_index_keys.insert((oid, index_oid)) {
                        return Err(ServerError::Ddl(format!(
                            "duplicate table-runtime index metadata on line {}",
                            line_no + 1
                        )));
                    }
                    let method = parse_index_method(parts[3])?;
                    let key_exprs = decode_scalar_expr_list_field(&metadata_unescape(parts[4])?)?;
                    let predicate = {
                        let raw = metadata_unescape(parts[5])?;
                        if raw.is_empty() {
                            None
                        } else {
                            Some(decode_scalar_expr_field(&raw)?)
                        }
                    };
                    indexes.entry(oid).or_default().push((
                        index_oid,
                        RuntimeIndexMetadata {
                            key_exprs,
                            predicate,
                            include_columns: parse_usize_list_token(parts[6])?,
                            method,
                            brin: None,
                            hnsw: None,
                            ivfflat: None,
                            aggregating: None,
                        },
                    ));
                }
                _ => {
                    return Err(ServerError::Ddl(format!(
                        "malformed table-runtime metadata line {}",
                        line_no + 1
                    )));
                }
            }
        }
        for (oid, table_name) in table_names {
            let Some(table) = snapshot.tables_by_oid.get(&oid) else {
                return Err(ServerError::Ddl(format!(
                    "unknown table-runtime metadata table '{}' on oid {}",
                    table_name,
                    oid.raw()
                )));
            };
            let expected_key = table_entry_lookup_key(table);
            if table_name != expected_key && table_name != table.name {
                return Err(ServerError::Ddl(format!(
                    "table-runtime metadata table '{}' does not match catalog table '{}'",
                    table_name, expected_key
                )));
            }
            let width = table.schema.fields().len();
            let mut runtime = self
                .table_constraints
                .get(&oid)
                .map(|existing| existing.as_ref().clone())
                .unwrap_or_default();
            if runtime.defaults.len() < width {
                runtime.defaults.resize(width, None);
            }
            if runtime.sequence_defaults.len() < width {
                runtime.sequence_defaults.resize(width, None);
            }
            if runtime.identity_always.len() < width {
                runtime.identity_always.resize(width, false);
            }
            if runtime.generated_stored.len() < width {
                runtime.generated_stored.resize(width, None);
            }
            if let Some(defaults) = sequence_defaults.remove(&oid) {
                for (idx, seq_name) in defaults {
                    if idx < runtime.sequence_defaults.len() {
                        runtime.sequence_defaults[idx] = Some(seq_name);
                    }
                }
            }
            if let Some(defaults) = defaults.remove(&oid) {
                for (idx, expr) in defaults {
                    if idx < runtime.defaults.len() {
                        runtime.defaults[idx] = Some(expr);
                    }
                }
            }
            if let Some(always_columns) = identity_always.remove(&oid) {
                for idx in always_columns {
                    if idx < runtime.identity_always.len() {
                        runtime.identity_always[idx] = true;
                    }
                }
            }
            if let Some(generated) = generated_stored.remove(&oid) {
                for (idx, expr) in generated {
                    if idx < runtime.generated_stored.len() {
                        runtime.generated_stored[idx] = Some(expr);
                    }
                }
            }
            if let Some(checks) = checks.remove(&oid) {
                runtime.checks = checks;
            }
            if let Some(foreign_keys) = foreign_keys.remove(&oid) {
                let mut validated_foreign_keys = Vec::with_capacity(foreign_keys.len());
                for mut fk in foreign_keys {
                    let Some(target) = snapshot.tables.get(&fk.target_table) else {
                        return Err(ServerError::Ddl(format!(
                            "invalid table-runtime foreign-key target metadata for '{}'",
                            fk.name
                        )));
                    };
                    if target.oid != fk.target_oid {
                        return Err(ServerError::Ddl(format!(
                            "invalid table-runtime foreign-key target metadata for '{}'",
                            fk.name
                        )));
                    }
                    fk.target_oid = target.oid;
                    validated_foreign_keys.push(fk);
                }
                runtime.foreign_keys = validated_foreign_keys;
            }
            if let Some(exclusions) = exclusions.remove(&oid) {
                runtime.exclusion_constraints = exclusions;
            }
            if let Some(indexes) = indexes.remove(&oid) {
                for (index_oid, metadata) in indexes {
                    let index_belongs_to_table = snapshot
                        .indexes_by_table
                        .get(&oid)
                        .is_some_and(|entries| entries.iter().any(|index| index.oid == index_oid));
                    if !index_belongs_to_table {
                        let index_exists = snapshot
                            .indexes_by_table
                            .values()
                            .any(|entries| entries.iter().any(|index| index.oid == index_oid));
                        // CREATE INDEX can crash after the runtime sidecar is written but before
                        // the catalog index row is WAL-durable. That stale sidecar is ignored; a
                        // committed index oid attached to the wrong table is still corrupt.
                        if !index_exists {
                            skipped_stale_index_metadata = true;
                            continue;
                        }
                        return Err(ServerError::Ddl(format!(
                            "invalid table-runtime index metadata on oid {} for table oid {}",
                            index_oid.raw(),
                            oid.raw()
                        )));
                    }
                    runtime.indexes.insert(index_oid, metadata);
                }
            }
            self.table_constraints.insert(oid, Arc::new(runtime));
        }
        if let Some(oid) = sequence_defaults
            .keys()
            .chain(defaults.keys())
            .chain(identity_always.keys())
            .chain(generated_stored.keys())
            .chain(checks.keys())
            .chain(foreign_keys.keys())
            .chain(exclusions.keys())
            .chain(indexes.keys())
            .copied()
            .next()
        {
            return Err(ServerError::Ddl(format!(
                "orphan table-runtime metadata rows on oid {}",
                oid.raw()
            )));
        }
        if skipped_stale_index_metadata {
            self.persist_table_runtime_constraints_metadata()?;
        }
        Ok(())
    }

    pub(crate) fn role_metadata_path(&self) -> Option<std::path::PathBuf> {
        self.data_dir.as_ref().map(|dir| dir.join("pg_auth.meta"))
    }

    pub(crate) fn persist_role_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.role_metadata_path() else {
            return Ok(());
        };
        let mut roles = self.role_catalog.list_roles();
        roles.sort_by_key(|role| role.oid);
        let mut memberships = self.role_catalog.list_memberships();
        memberships.sort_by(|left, right| {
            left.role
                .cmp(&right.role)
                .then_with(|| left.member.cmp(&right.member))
        });

        let mut out = String::from("# ultrasql auth runtime v1\n");
        for role in roles {
            out.push_str(&format!(
                "role\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                metadata_escape(&role.name),
                role.oid,
                metadata_escape(&format_password_hash(role.password.as_ref())),
                role.is_superuser,
                role.inherit,
                role.create_role,
                role.create_db,
                role.can_login,
                role.replication,
                role.bypass_rls,
                role.connection_limit,
                role.valid_until
                    .map_or_else(String::new, |value| value.to_string())
            ));
        }
        for membership in memberships {
            out.push_str(&format!(
                "member\t{}\t{}\t{}\t{}\n",
                metadata_escape(&membership.role),
                metadata_escape(&membership.member),
                metadata_escape(&membership.grantor),
                membership.admin_option
            ));
        }
        write_runtime_metadata_file(&path, &out)
    }

}
