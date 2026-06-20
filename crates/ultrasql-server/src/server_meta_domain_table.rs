//! `impl Server` methods (split out of the crate root): meta_domain_table.
//!
//! Pure code motion from `lib.rs`; behavior unchanged.
use super::*;

impl Server {
    pub(crate) fn persist_domain_runtime_constraints_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.domain_runtime_metadata_path() else {
            return Ok(());
        };
        let snapshot = self.catalog_snapshot();
        let mut entries = snapshot
            .domain_types_by_oid
            .values()
            .map(|entry| {
                let runtime = self.domain_constraints.get(&entry.oid);
                (entry.clone(), runtime.map(|guard| guard.as_ref().clone()))
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|(entry, _)| entry.oid.raw());

        let mut out = String::from("# ultrasql domain runtime constraints v1\n");
        for (entry, runtime) in entries {
            let runtime = runtime.unwrap_or_else(|| DomainRuntimeConstraints {
                base_type: entry.base_type.clone(),
                not_null: entry.not_null,
                checks: Vec::new(),
            });
            let Some(base_token) = data_type_token(&runtime.base_type) else {
                return Err(ServerError::ddl(format!(
                    "domain '{}' base type is outside restart-persistable metadata subset",
                    entry.name
                )));
            };
            out.push_str(&format!(
                "domain\t{}\t{}\t{}\t{}\t{}\n",
                metadata_escape(&entry.name),
                entry.oid.raw(),
                metadata_escape(&entry.schema_name),
                metadata_escape(&base_token),
                runtime.not_null
            ));
            for check in &runtime.checks {
                let Some(expr) = encode_scalar_expr_field(&check.expr) else {
                    return Err(ServerError::ddl(format!(
                        "domain '{}' CHECK '{}' is outside restart-persistable metadata subset",
                        entry.name, check.name
                    )));
                };
                out.push_str(&format!(
                    "check\t{}\t{}\t{}\n",
                    entry.oid.raw(),
                    metadata_escape(&check.name),
                    metadata_escape(&expr)
                ));
            }
        }
        write_runtime_metadata_file(&path, &out)
    }

    pub(crate) fn rebuild_domain_runtime_constraint_sidecars(&self) -> Result<(), ServerError> {
        let Some(path) = self.domain_runtime_metadata_path() else {
            return Ok(());
        };
        let Some(text) = read_runtime_metadata_file(&path)? else {
            return Ok(());
        };
        let mut domains: std::collections::HashMap<Oid, DomainTypeEntry> =
            std::collections::HashMap::new();
        let mut checks: std::collections::HashMap<Oid, Vec<RuntimeCheckConstraint>> =
            std::collections::HashMap::new();
        let mut seen_domain_oids = std::collections::HashSet::new();
        let mut seen_domain_names = std::collections::HashSet::new();
        let mut seen_check_keys = std::collections::HashSet::new();
        for (line_no, line) in text.lines().enumerate() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts = line.split('\t').collect::<Vec<_>>();
            match parts.first().copied() {
                Some("domain") if parts.len() == 6 => {
                    let oid = Oid::new(parts[2].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "domain-runtime metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    let base_token = metadata_unescape(parts[4])?;
                    let base_type = data_type_from_token(&base_token).ok_or_else(|| {
                        ServerError::Ddl(format!(
                            "domain-runtime metadata line {} unknown base type",
                            line_no + 1
                        ))
                    })?;
                    let not_null = parts[5].parse::<bool>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "domain-runtime metadata line {} bad not-null flag: {err}",
                            line_no + 1
                        ))
                    })?;
                    let name = metadata_unescape(parts[1])?;
                    let schema_name = metadata_unescape(parts[3])?;
                    if !seen_domain_oids.insert(oid)
                        || !seen_domain_names
                            .insert((schema_name.to_ascii_lowercase(), name.to_ascii_lowercase()))
                    {
                        return Err(ServerError::Ddl(format!(
                            "duplicate domain-runtime metadata on line {}",
                            line_no + 1
                        )));
                    }
                    domains.insert(
                        oid,
                        DomainTypeEntry {
                            oid,
                            name,
                            schema_name,
                            base_type,
                            not_null,
                        },
                    );
                }
                Some("check") if parts.len() == 4 => {
                    let oid = Oid::new(parts[1].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "domain-runtime metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    let name = metadata_unescape(parts[2])?;
                    if !seen_check_keys.insert((oid, name.to_ascii_lowercase())) {
                        return Err(ServerError::Ddl(format!(
                            "duplicate domain-runtime check metadata on line {}",
                            line_no + 1
                        )));
                    }
                    checks.entry(oid).or_default().push(RuntimeCheckConstraint {
                        name,
                        expr: decode_scalar_expr_field(&metadata_unescape(parts[3])?)?,
                    });
                }
                _ => {
                    return Err(ServerError::Ddl(format!(
                        "malformed domain-runtime metadata line {}",
                        line_no + 1
                    )));
                }
            }
        }
        for (oid, entry) in domains {
            if !self
                .catalog_snapshot()
                .domain_types_by_oid
                .contains_key(&oid)
            {
                self.persistent_catalog.create_domain_type(entry.clone())?;
            }
            self.domain_constraints.insert(
                oid,
                Arc::new(DomainRuntimeConstraints {
                    base_type: entry.base_type,
                    not_null: entry.not_null,
                    checks: checks.remove(&oid).unwrap_or_default(),
                }),
            );
        }
        if let Some(oid) = checks.keys().copied().next() {
            return Err(ServerError::Ddl(format!(
                "orphan domain-runtime check metadata on oid {}",
                oid.raw()
            )));
        }
        Ok(())
    }

    pub(crate) fn table_runtime_metadata_path(&self) -> Option<std::path::PathBuf> {
        self.data_dir
            .as_ref()
            .map(|dir| dir.join("pg_table_runtime.meta"))
    }

    pub(crate) fn ensure_table_runtime_constraints_metadata_slots_persistable(
        &self,
    ) -> Result<(), ServerError> {
        ensure_optional_runtime_metadata_write_slots(self.table_runtime_metadata_path())
    }

    pub(crate) fn ensure_create_table_runtime_metadata_slots_persistable(
        &self,
        writes_sequence_owner_metadata: bool,
    ) -> Result<(), ServerError> {
        self.ensure_table_runtime_constraints_metadata_slots_persistable()?;
        self.ensure_create_relation_metadata_slots_persistable()?;
        if writes_sequence_owner_metadata {
            ensure_optional_runtime_metadata_write_slots(self.sequence_owner_metadata_path())?;
        }
        Ok(())
    }

    pub(crate) fn ensure_create_relation_metadata_slots_persistable(
        &self,
    ) -> Result<(), ServerError> {
        ensure_optional_runtime_metadata_write_slots(self.row_security_metadata_path())?;
        ensure_optional_runtime_metadata_write_slots(self.privilege_metadata_path())
    }

    pub(crate) fn ensure_drop_table_runtime_metadata_slots_persistable(
        &self,
        dropped_tables: &[String],
    ) -> Result<(), ServerError> {
        self.ensure_table_runtime_constraints_metadata_slots_persistable()?;
        ensure_optional_runtime_metadata_write_slots(self.row_security_metadata_path())?;

        let grant_objects = self
            .privilege_catalog
            .list_grants()
            .into_iter()
            .map(|grant| (grant.object_kind, grant.object_name))
            .collect::<std::collections::HashSet<_>>();
        let mut sequence_owner_metadata_changed = false;
        let mut privilege_metadata_changed = false;
        let mut materialized_view_metadata_changed = false;
        let mut regular_view_metadata_changed = false;
        for table_name in dropped_tables {
            if self.materialized_views.contains_key(table_name) {
                materialized_view_metadata_changed = true;
            }
            if self.regular_views.contains_key(table_name) {
                regular_view_metadata_changed = true;
            }
            let Some(entry) = self.persistent_catalog.lookup_table(table_name) else {
                continue;
            };
            let table_key = ultrasql_catalog::table_lookup_key(&entry.schema_name, &entry.name);
            if grant_objects.contains(&(crate::auth::PrivilegeObjectKind::Table, table_key)) {
                privilege_metadata_changed = true;
            }
            let Some(constraints) = self.table_constraints.get(&entry.oid) else {
                continue;
            };
            for sequence_name in constraints.sequence_defaults.iter().flatten() {
                sequence_owner_metadata_changed = true;
                let sequence_key = sequence_name.to_ascii_lowercase();
                let sequence_grant_key =
                    if ultrasql_catalog::decode_table_lookup_key(&sequence_key).is_some() {
                        sequence_key
                    } else {
                        let namespace = self
                            .sequence_namespaces
                            .get(&sequence_key)
                            .map_or_else(|| "public".to_owned(), |entry| entry.value().clone());
                        ultrasql_catalog::table_lookup_key(&namespace, &sequence_key)
                    };
                if grant_objects.contains(&(
                    crate::auth::PrivilegeObjectKind::Sequence,
                    sequence_grant_key,
                )) {
                    privilege_metadata_changed = true;
                }
            }
        }

        if sequence_owner_metadata_changed {
            ensure_optional_runtime_metadata_write_slots(self.sequence_owner_metadata_path())?;
        }
        if privilege_metadata_changed {
            ensure_optional_runtime_metadata_write_slots(self.privilege_metadata_path())?;
        }
        if materialized_view_metadata_changed {
            ensure_optional_runtime_metadata_write_slots(self.materialized_view_metadata_path())?;
        }
        if regular_view_metadata_changed {
            ensure_optional_runtime_metadata_write_slots(self.regular_view_metadata_path())?;
        }
        Ok(())
    }

    pub(crate) fn ensure_schema_metadata_slots_persistable(&self) -> Result<(), ServerError> {
        ensure_optional_runtime_metadata_write_slots(self.schema_metadata_path())
    }

    pub(crate) fn ensure_table_runtime_constraints_metadata_persistable(
        &self,
        table_name: &str,
        constraints: &TableRuntimeConstraints,
    ) -> Result<(), ServerError> {
        if self.table_runtime_metadata_path().is_none() {
            return Ok(());
        }
        for (idx, default_expr) in constraints.defaults.iter().enumerate() {
            if let Some(default_expr) = default_expr {
                encode_table_runtime_scalar_expr(
                    table_name,
                    format!("DEFAULT expression on column {idx}"),
                    default_expr,
                )?;
            }
        }
        for (idx, generated_expr) in constraints.generated_stored.iter().enumerate() {
            if let Some(generated_expr) = generated_expr {
                encode_table_runtime_scalar_expr(
                    table_name,
                    format!("generated stored expression on column {idx}"),
                    generated_expr,
                )?;
            }
        }
        for check in &constraints.checks {
            encode_table_runtime_scalar_expr(
                table_name,
                format!("CHECK '{}' expression", check.name),
                &check.expr,
            )?;
        }
        Ok(())
    }

    pub(crate) fn persist_table_runtime_constraints_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.table_runtime_metadata_path() else {
            return Ok(());
        };
        let snapshot = self.catalog_snapshot();
        let mut entries = self
            .table_constraints
            .iter()
            .filter_map(|entry| {
                let table = snapshot.tables_by_oid.get(entry.key())?;
                Some((
                    *entry.key(),
                    table_entry_lookup_key(table),
                    entry.value().as_ref().clone(),
                ))
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|(oid, _, _)| oid.raw());

        let mut out = String::from("# ultrasql table runtime constraints v1\n");
        for (oid, table_name, constraints) in entries {
            out.push_str(&format!(
                "table\t{}\t{}\n",
                metadata_escape(&table_name),
                oid.raw()
            ));
            for (idx, seq_name) in constraints.sequence_defaults.iter().enumerate() {
                let Some(seq_name) = seq_name else {
                    continue;
                };
                out.push_str(&format!(
                    "sequence_default\t{}\t{}\t{}\n",
                    oid.raw(),
                    idx,
                    metadata_escape(seq_name)
                ));
            }
            for (idx, default_expr) in constraints.defaults.iter().enumerate() {
                let Some(default_expr) = default_expr else {
                    continue;
                };
                let expr = encode_table_runtime_scalar_expr(
                    &table_name,
                    format!("DEFAULT expression on column {idx}"),
                    default_expr,
                )?;
                out.push_str(&format!(
                    "default\t{}\t{}\t{}\n",
                    oid.raw(),
                    idx,
                    metadata_escape(&expr)
                ));
            }
            for (idx, identity_always) in constraints.identity_always.iter().enumerate() {
                if *identity_always {
                    out.push_str(&format!("identity_always\t{}\t{}\n", oid.raw(), idx));
                }
            }
            for (idx, generated_expr) in constraints.generated_stored.iter().enumerate() {
                let Some(generated_expr) = generated_expr else {
                    continue;
                };
                let expr = encode_table_runtime_scalar_expr(
                    &table_name,
                    format!("generated stored expression on column {idx}"),
                    generated_expr,
                )?;
                out.push_str(&format!(
                    "generated_stored\t{}\t{}\t{}\n",
                    oid.raw(),
                    idx,
                    metadata_escape(&expr)
                ));
            }
            for check in &constraints.checks {
                let expr = encode_table_runtime_scalar_expr(
                    &table_name,
                    format!("CHECK '{}' expression", check.name),
                    &check.expr,
                )?;
                out.push_str(&format!(
                    "check\t{}\t{}\t{}\n",
                    oid.raw(),
                    metadata_escape(&check.name),
                    metadata_escape(&expr)
                ));
            }
            for fk in &constraints.foreign_keys {
                out.push_str(&format!(
                    "foreign_key\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                    oid.raw(),
                    metadata_escape(&fk.name),
                    usize_list_token(&fk.columns),
                    metadata_escape(&fk.target_table),
                    fk.target_oid.raw(),
                    usize_list_token(&fk.target_columns),
                    referential_action_token(fk.on_delete),
                    referential_action_token(fk.on_update),
                    fk.deferrable,
                    fk.initially_deferred
                ));
            }
            for exclusion in &constraints.exclusion_constraints {
                let elements = exclusion
                    .elements
                    .iter()
                    .map(|element| format!("{}:{}", element.column, binary_op_token(element.op)))
                    .collect::<Vec<_>>()
                    .join(",");
                out.push_str(&format!(
                    "exclusion\t{}\t{}\t{}\t{}\n",
                    oid.raw(),
                    metadata_escape(&exclusion.name),
                    index_method_token(exclusion.method),
                    elements
                ));
            }
            let mut indexes = constraints.indexes.iter().collect::<Vec<_>>();
            indexes.sort_by_key(|(index_oid, _)| index_oid.raw());
            for (index_oid, metadata) in indexes {
                let key_exprs = encode_table_runtime_scalar_expr_list(
                    &table_name,
                    format!("index {} key", index_oid.raw()),
                    &metadata.key_exprs,
                )?;
                let predicate = metadata
                    .predicate
                    .as_ref()
                    .map(|predicate| {
                        encode_table_runtime_scalar_expr(
                            &table_name,
                            format!("index {} predicate", index_oid.raw()),
                            predicate,
                        )
                    })
                    .transpose()?
                    .unwrap_or_default();
                out.push_str(&format!(
                    "index\t{}\t{}\t{}\t{}\t{}\t{}\n",
                    oid.raw(),
                    index_oid.raw(),
                    index_method_token(metadata.method),
                    metadata_escape(&key_exprs),
                    metadata_escape(&predicate),
                    usize_list_token(&metadata.include_columns)
                ));
            }
        }
        write_runtime_metadata_file(&path, &out)
    }

}
