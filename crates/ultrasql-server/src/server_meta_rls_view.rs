//! `impl Server` methods (split out of the crate root): meta_rls_view.
//!
//! Pure code motion from `lib.rs`; behavior unchanged.
use super::*;

impl Server {
    pub(crate) fn rebuild_operator_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.operator_metadata_path() else {
            return Ok(());
        };
        let Some(text) = read_runtime_metadata_file(&path)? else {
            return Ok(());
        };

        self.operators.clear();
        let mut seen_oids = std::collections::HashSet::new();
        let mut seen_signatures = std::collections::HashSet::new();
        for (line_no, line) in text.lines().enumerate() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts = line.split('\t').collect::<Vec<_>>();
            if parts.len() != 8 || parts.first().copied() != Some("operator") {
                return Err(ServerError::ddl(format!(
                    "malformed operator metadata line {}",
                    line_no + 1
                )));
            }
            let oid = parts[1].parse::<u32>().map_err(|err| {
                ServerError::Ddl(format!(
                    "operator metadata line {} bad oid: {err}",
                    line_no + 1
                ))
            })?;
            if !seen_oids.insert(oid) {
                return Err(ServerError::ddl(format!(
                    "duplicate operator metadata oid {} on line {}",
                    oid,
                    line_no + 1
                )));
            }
            let namespace = metadata_unescape(parts[2])?;
            let name = metadata_unescape(parts[3])?;
            let left_type =
                parse_operator_data_type_token(&metadata_unescape(parts[4])?, line_no, "left")?;
            let right_type =
                parse_operator_data_type_token(&metadata_unescape(parts[5])?, line_no, "right")?;
            let procedure = metadata_unescape(parts[6])?;
            let result_token = metadata_unescape(parts[7])?;
            let result_type = data_type_from_token(&result_token).ok_or_else(|| {
                ServerError::ddl(format!(
                    "operator metadata line {} has unknown result type '{}'",
                    line_no + 1,
                    result_token
                ))
            })?;
            let operator = RuntimeOperator {
                oid,
                name,
                namespace,
                left_type,
                right_type,
                procedure,
                result_type,
            };
            validate_runtime_operator_metadata(&operator, line_no)?;
            let signature = runtime_operator_signature(
                &operator.namespace,
                &operator.name,
                &operator.left_type,
                &operator.right_type,
            );
            if !seen_signatures.insert(signature.clone()) {
                return Err(ServerError::ddl(format!(
                    "duplicate operator metadata signature '{}' on line {}",
                    signature,
                    line_no + 1
                )));
            }
            self.operators.insert(signature, Arc::new(operator));
        }
        Ok(())
    }

    pub(crate) fn row_security_metadata_path(&self) -> Option<std::path::PathBuf> {
        self.data_dir
            .as_ref()
            .map(|dir| dir.join("pg_row_security.meta"))
    }

    pub(crate) fn persist_row_security_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.row_security_metadata_path() else {
            return Ok(());
        };
        let snapshot = self.catalog_snapshot();
        let mut entries = self
            .row_security
            .iter()
            .filter_map(|entry| {
                if !entry.value().enabled
                    && entry.value().policies.is_empty()
                    && entry.value().owner_role.is_empty()
                {
                    return None;
                }
                let table = snapshot.tables_by_oid.get(entry.key())?;
                Some((
                    *entry.key(),
                    table.name.clone(),
                    entry.value().as_ref().clone(),
                ))
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|(oid, _, _)| oid.raw());

        let mut out = String::from("# ultrasql row security v2\n");
        for (oid, table_name, runtime) in entries {
            out.push_str(&format!(
                "table\t{}\t{}\t{}\t{}\n",
                metadata_escape(&table_name),
                oid.raw(),
                runtime.enabled,
                metadata_escape(&runtime.owner_role)
            ));
            for policy in &runtime.policies {
                let (using_idx, using_col, using_setting) = rls_expr_fields(policy.using.as_ref());
                let (check_idx, check_col, check_setting) =
                    rls_expr_fields(policy.with_check.as_ref());
                out.push_str(&format!(
                    "policy\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                    oid.raw(),
                    metadata_escape(&policy.name),
                    rls_permissiveness_name(policy.permissiveness),
                    rls_command_name(policy.command),
                    using_idx,
                    using_col,
                    using_setting,
                    check_idx,
                    check_col,
                    check_setting,
                    metadata_escape(&metadata_encode_list(&policy.roles))
                ));
            }
        }
        write_runtime_metadata_file(&path, &out)
    }

    pub(crate) fn rebuild_row_security_sidecars(&self) -> Result<(), ServerError> {
        let Some(path) = self.row_security_metadata_path() else {
            return Ok(());
        };
        let Some(text) = read_runtime_metadata_file(&path)? else {
            return Ok(());
        };
        let snapshot = self.catalog_snapshot();
        let mut rows: std::collections::HashMap<ultrasql_core::Oid, (String, TableRowSecurity)> =
            std::collections::HashMap::new();
        let mut seen_table_oids = std::collections::HashSet::new();
        let mut seen_policy_keys = std::collections::HashSet::new();
        let known_roles = runtime_metadata_known_role_names(&self.role_catalog);
        for (line_no, line) in text.lines().enumerate() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts = line.split('\t').collect::<Vec<_>>();
            match parts.first().copied() {
                Some("table") if parts.len() == 4 || parts.len() == 5 => {
                    let table_name = metadata_unescape(parts[1])?;
                    let oid = ultrasql_core::Oid::new(parts[2].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "RLS metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    if !seen_table_oids.insert(oid) {
                        return Err(ServerError::Ddl(format!(
                            "duplicate RLS table metadata on line {}",
                            line_no + 1
                        )));
                    }
                    let enabled = parts[3].parse::<bool>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "RLS metadata line {} bad enabled flag: {err}",
                            line_no + 1
                        ))
                    })?;
                    let owner_role = if parts.len() == 5 {
                        metadata_unescape(parts[4])?.to_ascii_lowercase()
                    } else {
                        String::new()
                    };
                    if !owner_role.is_empty() && !known_roles.contains(&owner_role) {
                        return Err(ServerError::Ddl(format!(
                            "unknown RLS table metadata owner '{}' on line {}",
                            owner_role,
                            line_no + 1
                        )));
                    }
                    let entry = rows
                        .entry(oid)
                        .or_insert_with(|| (String::new(), TableRowSecurity::default()));
                    entry.0 = table_name;
                    entry.1.enabled = enabled;
                    entry.1.owner_role = owner_role;
                }
                Some("policy") if parts.len() == 11 || parts.len() == 12 => {
                    let oid = ultrasql_core::Oid::new(parts[1].parse::<u32>().map_err(|err| {
                        ServerError::Ddl(format!(
                            "RLS metadata line {} bad oid: {err}",
                            line_no + 1
                        ))
                    })?);
                    let policy_name = metadata_unescape(parts[2])?;
                    if !seen_policy_keys.insert((oid, policy_name.to_ascii_lowercase())) {
                        return Err(ServerError::Ddl(format!(
                            "duplicate RLS policy metadata '{}' on line {}",
                            policy_name,
                            line_no + 1
                        )));
                    }
                    let mut roles = if parts.len() == 12 {
                        metadata_decode_list(&metadata_unescape(parts[11])?)?
                    } else {
                        Vec::new()
                    };
                    validate_rls_metadata_policy_roles(&known_roles, &mut roles, line_no)?;
                    let using = parse_rls_expr(parts[5], parts[6], parts[7])?;
                    let with_check = parse_rls_expr(parts[8], parts[9], parts[10])?;
                    if let Some(table) = snapshot.tables_by_oid.get(&oid) {
                        validate_rls_metadata_expr(table, using.as_ref(), line_no, "USING")?;
                        validate_rls_metadata_expr(
                            table,
                            with_check.as_ref(),
                            line_no,
                            "WITH CHECK",
                        )?;
                    }
                    let policy = RuntimeRlsPolicy {
                        name: policy_name,
                        permissiveness: parse_rls_permissiveness(parts[3])?,
                        command: parse_rls_command(parts[4])?,
                        roles,
                        using,
                        with_check,
                    };
                    rows.entry(oid)
                        .or_insert_with(|| (String::new(), TableRowSecurity::default()))
                        .1
                        .policies
                        .push(policy);
                }
                _ => {
                    return Err(ServerError::Ddl(format!(
                        "malformed RLS metadata line {}",
                        line_no + 1
                    )));
                }
            }
        }
        for (oid, (table_name, runtime)) in rows {
            let Some(table) = snapshot.tables_by_oid.get(&oid) else {
                return Err(ServerError::Ddl(format!(
                    "unknown RLS table metadata '{}' on oid {}",
                    table_name,
                    oid.raw()
                )));
            };
            if table.name != table_name {
                return Err(ServerError::Ddl(format!(
                    "RLS table metadata '{}' does not match catalog table '{}'",
                    table_name, table.name
                )));
            }
            self.row_security.insert(oid, Arc::new(runtime));
        }
        Ok(())
    }

    pub(crate) fn materialized_view_metadata_path(&self) -> Option<std::path::PathBuf> {
        self.data_dir
            .as_ref()
            .map(|dir| dir.join("pg_materialized_views.meta"))
    }

    pub(crate) fn load_materialized_view_metadata(
        &self,
    ) -> Result<Vec<MaterializedViewMetadataRecord>, ServerError> {
        let Some(path) = self.materialized_view_metadata_path() else {
            return Ok(Vec::new());
        };
        let Some(text) = read_runtime_metadata_file(&path)? else {
            return Ok(Vec::new());
        };
        let mut records = Vec::new();
        let mut seen_view_names = std::collections::HashSet::new();
        let mut seen_view_oids = std::collections::HashSet::new();
        for (line_no, line) in text.lines().enumerate() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts = line.split('\t').collect::<Vec<_>>();
            if parts.len() != 6 {
                return Err(ServerError::Ddl(format!(
                    "materialized-view metadata line {} has {} fields",
                    line_no + 1,
                    parts.len()
                )));
            }
            let view_oid = parts[1].parse::<u32>().map_err(|err| {
                ServerError::Ddl(format!(
                    "materialized-view metadata line {} bad view oid: {err}",
                    line_no + 1
                ))
            })?;
            let view_table = metadata_unescape(parts[0])?;
            if !seen_view_names.insert(view_table.to_ascii_lowercase())
                || !seen_view_oids.insert(view_oid)
            {
                return Err(ServerError::Ddl(format!(
                    "duplicate materialized-view metadata on line {}",
                    line_no + 1
                )));
            }
            let source_oid = parts[3].parse::<u32>().map_err(|err| {
                ServerError::Ddl(format!(
                    "materialized-view metadata line {} bad source oid: {err}",
                    line_no + 1
                ))
            })?;
            let materialized_rows = parts[4].parse::<u64>().map_err(|err| {
                ServerError::Ddl(format!(
                    "materialized-view metadata line {} bad row count: {err}",
                    line_no + 1
                ))
            })?;
            let projection = if parts[5].is_empty() {
                Vec::new()
            } else {
                parts[5]
                    .split(',')
                    .map(|raw| {
                        raw.parse::<usize>().map_err(|err| {
                            ServerError::Ddl(format!(
                                "materialized-view metadata line {} bad projection index: {err}",
                                line_no + 1
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?
            };
            records.push(MaterializedViewMetadataRecord {
                view_table,
                view_oid: ultrasql_core::Oid::new(view_oid),
                source_table: metadata_unescape(parts[2])?,
                source_oid: ultrasql_core::Oid::new(source_oid),
                materialized_rows,
                projection,
            });
        }
        Ok(records)
    }

    pub(crate) fn write_materialized_view_metadata(
        &self,
        records: &[MaterializedViewMetadataRecord],
    ) -> Result<(), ServerError> {
        let Some(path) = self.materialized_view_metadata_path() else {
            return Ok(());
        };
        let mut out = String::from("# ultrasql materialized views v1\n");
        for record in records {
            let projection = record
                .projection
                .iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join(",");
            out.push_str(&format!(
                "{}\t{}\t{}\t{}\t{}\t{}\n",
                metadata_escape(&record.view_table),
                record.view_oid.raw(),
                metadata_escape(&record.source_table),
                record.source_oid.raw(),
                record.materialized_rows,
                projection
            ));
        }
        write_runtime_metadata_file(&path, &out)
    }

    pub(crate) fn ensure_materialized_view_runtime_metadata_persistable(
        &self,
        runtime: &MaterializedViewRuntime,
    ) -> Result<Vec<usize>, ServerError> {
        if self.materialized_view_metadata_path().is_none() {
            return Ok(Vec::new());
        }
        materialized_view_projection_indices(&runtime.source).ok_or_else(|| {
            ServerError::ddl(format!(
                "materialized view '{}' source shape is outside restart-persistable metadata subset",
                runtime.view_table
            ))
        })
    }

    pub(crate) fn persist_materialized_view_runtime_metadata(
        &self,
        runtime: &MaterializedViewRuntime,
        materialized_rows: u64,
    ) -> Result<(), ServerError> {
        if self.materialized_view_metadata_path().is_none() {
            return Ok(());
        }
        let projection = self.ensure_materialized_view_runtime_metadata_persistable(runtime)?;
        let Some(view_entry) = self.persistent_catalog.lookup_table(&runtime.view_table) else {
            return Ok(());
        };
        let Some(source_entry) = self.persistent_catalog.lookup_table(&runtime.source_table) else {
            return Ok(());
        };
        let mut records = self.load_materialized_view_metadata()?;
        records.retain(|record| {
            record.view_table != runtime.view_table && record.view_oid != view_entry.oid
        });
        records.push(MaterializedViewMetadataRecord {
            view_table: runtime.view_table.clone(),
            view_oid: view_entry.oid,
            source_table: runtime.source_table.clone(),
            source_oid: source_entry.oid,
            materialized_rows,
            projection,
        });
        self.write_materialized_view_metadata(&records)
    }

    pub(crate) fn remove_materialized_view_runtime_metadata(
        &self,
        dropped_tables: &[String],
    ) -> Result<(), ServerError> {
        if dropped_tables.is_empty() {
            return Ok(());
        }
        let mut records = self.load_materialized_view_metadata()?;
        let before = records.len();
        records.retain(|record| {
            !dropped_tables
                .iter()
                .any(|table| record.view_table.eq_ignore_ascii_case(table))
        });
        if records.len() != before {
            self.write_materialized_view_metadata(&records)?;
        }
        Ok(())
    }

    pub(crate) fn rebuild_materialized_view_runtime_sidecars(&self) -> Result<(), ServerError> {
        for record in self.load_materialized_view_metadata()? {
            let view_entry = self
                .persistent_catalog
                .lookup_table(&record.view_table)
                .ok_or_else(|| {
                    ServerError::Ddl(format!(
                        "invalid materialized-view metadata for '{}'",
                        record.view_table
                    ))
                })?;
            let source_entry = self
                .persistent_catalog
                .lookup_table(&record.source_table)
                .ok_or_else(|| {
                    ServerError::Ddl(format!(
                        "invalid materialized-view metadata for '{}'",
                        record.view_table
                    ))
                })?;
            if view_entry.oid != record.view_oid || source_entry.oid != record.source_oid {
                return Err(ServerError::Ddl(format!(
                    "invalid materialized-view metadata for '{}'",
                    record.view_table
                )));
            }
            let Some(source) =
                materialized_view_source_plan_from_metadata(&source_entry, &view_entry, &record)
            else {
                return Err(ServerError::Ddl(format!(
                    "invalid materialized-view metadata for '{}'",
                    record.view_table
                )));
            };
            self.materialized_views.insert(
                record.view_table.clone(),
                Arc::new(MaterializedViewRuntime {
                    view_table: record.view_table.clone(),
                    source_table: record.source_table.clone(),
                    source,
                    materialized_rows: std::sync::atomic::AtomicU64::new(record.materialized_rows),
                }),
            );
        }
        Ok(())
    }

    pub(crate) fn regular_view_metadata_path(&self) -> Option<std::path::PathBuf> {
        self.data_dir.as_ref().map(|dir| dir.join("pg_views.meta"))
    }

    pub(crate) fn load_regular_view_metadata(&self) -> Result<Vec<RegularViewMetadataRecord>, ServerError> {
        let Some(path) = self.regular_view_metadata_path() else {
            return Ok(Vec::new());
        };
        let Some(text) = read_runtime_metadata_file(&path)? else {
            return Ok(Vec::new());
        };
        let mut records = Vec::new();
        let mut seen_view_names = std::collections::HashSet::new();
        let mut seen_view_oids = std::collections::HashSet::new();
        for (line_no, line) in text.lines().enumerate() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts = line.split('\t').collect::<Vec<_>>();
            if parts.len() != 4 {
                return Err(ServerError::Ddl(format!(
                    "view metadata line {} has {} fields",
                    line_no + 1,
                    parts.len()
                )));
            }
            let view_table = metadata_unescape(parts[0])?;
            let view_oid = parts[1].parse::<u32>().map_err(|err| {
                ServerError::Ddl(format!(
                    "view metadata line {} bad view oid: {err}",
                    line_no + 1
                ))
            })?;
            if !seen_view_names.insert(view_table.to_ascii_lowercase())
                || !seen_view_oids.insert(view_oid)
            {
                return Err(ServerError::Ddl(format!(
                    "duplicate view metadata on line {}",
                    line_no + 1
                )));
            }
            let source_sql = metadata_unescape(parts[2])?;
            if source_sql.is_empty() {
                return Err(ServerError::Ddl(format!(
                    "view metadata line {} has empty source SQL",
                    line_no + 1
                )));
            }
            let search_path = (!parts[3].is_empty())
                .then(|| metadata_unescape(parts[3]))
                .transpose()?;
            records.push(RegularViewMetadataRecord {
                view_table,
                view_oid: ultrasql_core::Oid::new(view_oid),
                source_sql,
                search_path,
            });
        }
        Ok(records)
    }

    pub(crate) fn write_regular_view_metadata(
        &self,
        records: &[RegularViewMetadataRecord],
    ) -> Result<(), ServerError> {
        let Some(path) = self.regular_view_metadata_path() else {
            return Ok(());
        };
        let mut out = String::from("# ultrasql views v1\n");
        for record in records {
            out.push_str(&format!(
                "{}\t{}\t{}\t{}\n",
                metadata_escape(&record.view_table),
                record.view_oid.raw(),
                metadata_escape(&record.source_sql),
                record
                    .search_path
                    .as_deref()
                    .map(metadata_escape)
                    .unwrap_or_default()
            ));
        }
        write_runtime_metadata_file(&path, &out)
    }

}
