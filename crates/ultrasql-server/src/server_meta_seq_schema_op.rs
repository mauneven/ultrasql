//! `impl Server` methods (split out of the crate root): meta_seq_schema_op.
//!
//! Pure code motion from `lib.rs`; behavior unchanged.
use super::*;

impl Server {
    pub(crate) fn rebuild_role_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.role_metadata_path() else {
            return Ok(());
        };
        let Some(text) = read_runtime_metadata_file(&path)? else {
            return Ok(());
        };

        let mut roles = Vec::new();
        let mut memberships = Vec::new();
        let mut seen_role_names = std::collections::HashSet::new();
        let mut seen_role_oids = std::collections::HashSet::new();
        let mut seen_membership_keys = std::collections::HashSet::new();
        for (line_no, line) in text.lines().enumerate() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts = line.split('\t').collect::<Vec<_>>();
            match parts.first().copied() {
                Some("role") if parts.len() == 13 => {
                    let name = metadata_unescape(parts[1])?;
                    validate_role_metadata_name(&name, line_no, "name")?;
                    if !seen_role_names.insert(name.to_ascii_lowercase()) {
                        return Err(ServerError::ddl(format!(
                            "duplicate role metadata name '{}' on line {}",
                            name,
                            line_no + 1
                        )));
                    }
                    let oid = parse_role_u32(parts[2], line_no, "oid")?;
                    if oid == 0 {
                        return Err(ServerError::ddl(format!(
                            "invalid role metadata oid 0 on line {}",
                            line_no + 1
                        )));
                    }
                    if !seen_role_oids.insert(oid) {
                        return Err(ServerError::ddl(format!(
                            "duplicate role metadata oid {} on line {}",
                            oid,
                            line_no + 1
                        )));
                    }
                    roles.push(auth::RoleEntry {
                        name,
                        oid,
                        password: parse_password_hash(&metadata_unescape(parts[3])?, line_no)?,
                        is_superuser: parse_role_bool(parts[4], line_no, "is_superuser")?,
                        inherit: parse_role_bool(parts[5], line_no, "inherit")?,
                        create_role: parse_role_bool(parts[6], line_no, "create_role")?,
                        create_db: parse_role_bool(parts[7], line_no, "create_db")?,
                        can_login: parse_role_bool(parts[8], line_no, "can_login")?,
                        replication: parse_role_bool(parts[9], line_no, "replication")?,
                        bypass_rls: parse_role_bool(parts[10], line_no, "bypass_rls")?,
                        connection_limit: parse_role_i32(parts[11], line_no, "connection_limit")?,
                        valid_until: parse_role_optional_i64(parts[12], line_no, "valid_until")?,
                    });
                }
                Some("member") if parts.len() == 5 => {
                    let role = metadata_unescape(parts[1])?;
                    let member = metadata_unescape(parts[2])?;
                    validate_role_metadata_name(&role, line_no, "role")?;
                    validate_role_metadata_name(&member, line_no, "member")?;
                    let grantor = metadata_unescape(parts[3])?;
                    validate_role_metadata_name(&grantor, line_no, "grantor")?;
                    let key = (role.to_ascii_lowercase(), member.to_ascii_lowercase());
                    if !seen_membership_keys.insert(key) {
                        return Err(ServerError::ddl(format!(
                            "duplicate role membership metadata on line {}",
                            line_no + 1
                        )));
                    }
                    memberships.push(auth::RoleMembership {
                        role,
                        member,
                        grantor,
                        admin_option: parse_role_bool(parts[4], line_no, "admin_option")?,
                    });
                }
                _ => {
                    return Err(ServerError::ddl(format!(
                        "malformed role metadata line {}",
                        line_no + 1
                    )));
                }
            }
        }
        if roles.is_empty() {
            roles.push(auth::RoleEntry::bootstrap_superuser());
        }
        let role_names = roles
            .iter()
            .map(|role| role.name.to_ascii_lowercase())
            .collect::<std::collections::HashSet<_>>();
        for membership in &memberships {
            for (field, role_name) in [
                ("role", &membership.role),
                ("member", &membership.member),
                ("grantor", &membership.grantor),
            ] {
                if !role_names.contains(&role_name.to_ascii_lowercase()) {
                    return Err(ServerError::ddl(format!(
                        "unknown role membership metadata {field} '{}'",
                        role_name
                    )));
                }
            }
        }
        match roles
            .iter()
            .find(|role| role.name.eq_ignore_ascii_case("ultrasql"))
        {
            Some(role) if role.oid == auth::pg_authid::BOOTSTRAP_ROLE_OID => {
                validate_bootstrap_role_metadata(role)?;
            }
            Some(role) => {
                return Err(ServerError::ddl(format!(
                    "invalid bootstrap role metadata oid {}, expected {}",
                    role.oid,
                    auth::pg_authid::BOOTSTRAP_ROLE_OID
                )));
            }
            None => {
                return Err(ServerError::ddl(
                    "missing bootstrap role metadata 'ultrasql'",
                ));
            }
        }
        self.role_catalog.install_snapshot(roles, memberships);
        Ok(())
    }

    pub(crate) fn privilege_metadata_path(&self) -> Option<std::path::PathBuf> {
        self.data_dir
            .as_ref()
            .map(|dir| dir.join("pg_privileges.meta"))
    }

    pub(crate) fn ensure_privilege_metadata_slots_persistable(&self) -> Result<(), ServerError> {
        ensure_optional_runtime_metadata_write_slots(self.privilege_metadata_path())
    }

    pub(crate) fn persist_privilege_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.privilege_metadata_path() else {
            return Ok(());
        };
        let mut grants = self.privilege_catalog.list_grants();
        grants.sort_by(|left, right| {
            privilege_object_kind_name(left.object_kind)
                .cmp(privilege_object_kind_name(right.object_kind))
                .then_with(|| left.object_name.cmp(&right.object_name))
                .then_with(|| left.grantee.cmp(&right.grantee))
                .then_with(|| {
                    privilege_kind_name(left.privilege).cmp(privilege_kind_name(right.privilege))
                })
                .then_with(|| left.column_name.cmp(&right.column_name))
        });
        let mut default_grants = self.privilege_catalog.list_default_grants();
        default_grants.sort_by(|left, right| {
            left.owner_role
                .cmp(&right.owner_role)
                .then_with(|| left.schema_name.cmp(&right.schema_name))
                .then_with(|| {
                    privilege_object_kind_name(left.object_kind)
                        .cmp(privilege_object_kind_name(right.object_kind))
                })
                .then_with(|| left.grantee.cmp(&right.grantee))
                .then_with(|| {
                    privilege_kind_name(left.privilege).cmp(privilege_kind_name(right.privilege))
                })
        });

        let mut out = String::from("# ultrasql privilege runtime v1\n");
        for grant in grants {
            out.push_str(&format!(
                "grant\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                privilege_object_kind_name(grant.object_kind),
                metadata_escape(&grant.object_name),
                metadata_escape(&grant.grantee),
                privilege_kind_name(grant.privilege),
                metadata_escape(grant.column_name.as_deref().unwrap_or("")),
                metadata_escape(&grant.grantor),
                grant.grant_option
            ));
        }
        for grant in default_grants {
            out.push_str(&format!(
                "default\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                metadata_escape(&grant.owner_role),
                metadata_escape(grant.schema_name.as_deref().unwrap_or("")),
                privilege_object_kind_name(grant.object_kind),
                metadata_escape(&grant.grantee),
                privilege_kind_name(grant.privilege),
                metadata_escape(&grant.grantor),
                grant.grant_option
            ));
        }
        write_runtime_metadata_file(&path, &out)
    }

    pub(crate) fn rebuild_privilege_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.privilege_metadata_path() else {
            return Ok(());
        };
        let Some(text) = read_runtime_metadata_file(&path)? else {
            return Ok(());
        };

        let mut grants = Vec::new();
        let mut default_grants = Vec::new();
        let mut seen_grant_keys = std::collections::HashSet::new();
        let mut seen_default_grant_keys = std::collections::HashSet::new();
        let known_roles = runtime_metadata_known_role_names(&self.role_catalog);
        let snapshot = self.catalog_snapshot();
        for (line_no, line) in text.lines().enumerate() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts = line.split('\t').collect::<Vec<_>>();
            match parts.first().copied() {
                Some("grant") if parts.len() == 8 => {
                    let column_name = metadata_unescape(parts[5])?;
                    let grant = auth::PrivilegeGrant {
                        object_kind: parse_privilege_object_kind(parts[1], line_no)?,
                        object_name: metadata_unescape(parts[2])?,
                        grantee: metadata_unescape(parts[3])?,
                        privilege: parse_privilege_kind(parts[4], line_no)?,
                        column_name: (!column_name.is_empty()).then_some(column_name),
                        grantor: metadata_unescape(parts[6])?,
                        grant_option: parse_role_bool(parts[7], line_no, "grant_option")?,
                    };
                    let key = (
                        grant.object_kind,
                        grant.object_name.to_ascii_lowercase(),
                        grant.grantee.to_ascii_lowercase(),
                        grant.privilege,
                        grant
                            .column_name
                            .as_ref()
                            .map(|column| column.to_ascii_lowercase()),
                    );
                    if !seen_grant_keys.insert(key) {
                        return Err(ServerError::ddl(format!(
                            "duplicate privilege metadata grant on line {}",
                            line_no + 1
                        )));
                    }
                    validate_privilege_metadata_grantee(&known_roles, &grant.grantee, line_no)?;
                    validate_privilege_metadata_role(
                        &known_roles,
                        &grant.grantor,
                        line_no,
                        "grantor",
                    )?;
                    validate_privilege_metadata_column(&snapshot, &self.catalog, &grant, line_no)?;
                    grants.push(grant);
                }
                Some("default") if parts.len() == 8 => {
                    let schema_name = metadata_unescape(parts[2])?;
                    let grant = auth::DefaultPrivilegeGrant {
                        owner_role: metadata_unescape(parts[1])?,
                        schema_name: (!schema_name.is_empty()).then_some(schema_name),
                        object_kind: parse_privilege_object_kind(parts[3], line_no)?,
                        grantee: metadata_unescape(parts[4])?,
                        privilege: parse_privilege_kind(parts[5], line_no)?,
                        grantor: metadata_unescape(parts[6])?,
                        grant_option: parse_role_bool(parts[7], line_no, "grant_option")?,
                    };
                    let key = (
                        grant.owner_role.to_ascii_lowercase(),
                        grant
                            .schema_name
                            .as_ref()
                            .map(|schema| schema.to_ascii_lowercase()),
                        grant.object_kind,
                        grant.grantee.to_ascii_lowercase(),
                        grant.privilege,
                    );
                    if !seen_default_grant_keys.insert(key) {
                        return Err(ServerError::ddl(format!(
                            "duplicate default privilege metadata grant on line {}",
                            line_no + 1
                        )));
                    }
                    validate_privilege_metadata_role(
                        &known_roles,
                        &grant.owner_role,
                        line_no,
                        "owner",
                    )?;
                    validate_privilege_metadata_grantee(&known_roles, &grant.grantee, line_no)?;
                    validate_privilege_metadata_role(
                        &known_roles,
                        &grant.grantor,
                        line_no,
                        "grantor",
                    )?;
                    default_grants.push(grant);
                }
                _ => {
                    return Err(ServerError::ddl(format!(
                        "malformed privilege metadata line {}",
                        line_no + 1
                    )));
                }
            }
        }
        self.privilege_catalog
            .install_snapshot(grants, default_grants);
        Ok(())
    }

    pub(crate) fn sequence_owner_metadata_path(&self) -> Option<std::path::PathBuf> {
        self.data_dir
            .as_ref()
            .map(|dir| dir.join("pg_sequence_owner.meta"))
    }

    pub(crate) fn ensure_sequence_owner_metadata_slots_persistable(
        &self,
    ) -> Result<(), ServerError> {
        ensure_optional_runtime_metadata_write_slots(self.sequence_owner_metadata_path())
    }

    pub(crate) fn ensure_create_sequence_metadata_slots_persistable(
        &self,
    ) -> Result<(), ServerError> {
        self.ensure_sequence_owner_metadata_slots_persistable()?;
        ensure_optional_runtime_metadata_write_slots(self.privilege_metadata_path())
    }

    pub(crate) fn persist_sequence_owner_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.sequence_owner_metadata_path() else {
            return Ok(());
        };
        let mut owners = self
            .sequence_owners
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect::<Vec<_>>();
        owners.sort_by(|left, right| left.0.cmp(&right.0));

        let mut out = String::from("# ultrasql sequence owners v2\n");
        for (sequence_name, owner_role) in owners {
            if self.sequences.contains_key(&sequence_name) {
                let namespace = self
                    .sequence_namespaces
                    .get(&sequence_name)
                    .map_or_else(|| "public".to_owned(), |entry| entry.value().clone());
                out.push_str(&format!(
                    "sequence\t{}\t{}\t{}\n",
                    metadata_escape(&sequence_name),
                    metadata_escape(&owner_role),
                    metadata_escape(&namespace)
                ));
            }
        }
        write_runtime_metadata_file(&path, &out)
    }

    pub(crate) fn rebuild_sequence_owner_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.sequence_owner_metadata_path() else {
            return Ok(());
        };
        let Some(text) = read_runtime_metadata_file(&path)? else {
            return Ok(());
        };

        let mut owners = Vec::new();
        let mut seen_sequences = std::collections::HashSet::new();
        let known_roles = runtime_metadata_known_role_names(&self.role_catalog);
        for (line_no, line) in text.lines().enumerate() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts = line.split('\t').collect::<Vec<_>>();
            if !(parts.len() == 3 || parts.len() == 4) || parts.first().copied() != Some("sequence")
            {
                return Err(ServerError::ddl(format!(
                    "malformed sequence owner metadata line {}",
                    line_no + 1
                )));
            }
            let sequence_name = metadata_unescape(parts[1])?.to_ascii_lowercase();
            let owner_role = metadata_unescape(parts[2])?.to_ascii_lowercase();
            let namespace = parts
                .get(3)
                .map_or_else(|| Ok("public".to_owned()), |part| metadata_unescape(part))
                .map(|schema| schema.to_ascii_lowercase())?;
            if sequence_name.is_empty() || owner_role.is_empty() || namespace.is_empty() {
                return Err(ServerError::ddl(format!(
                    "empty sequence owner metadata field on line {}",
                    line_no + 1
                )));
            }
            if !builtin_schema_name(&namespace) && !self.schemas.contains_key(&namespace) {
                return Err(ServerError::ddl(format!(
                    "sequence owner metadata line {} references missing schema '{}'",
                    line_no + 1,
                    namespace
                )));
            }
            if !seen_sequences.insert(sequence_name.clone()) {
                return Err(ServerError::ddl(format!(
                    "duplicate sequence owner metadata '{}' on line {}",
                    sequence_name,
                    line_no + 1
                )));
            }
            if !self.sequences.contains_key(&sequence_name) {
                return Err(ServerError::ddl(format!(
                    "sequence owner metadata line {} references missing sequence '{}'",
                    line_no + 1,
                    sequence_name
                )));
            }
            if !known_roles.contains(&owner_role) {
                return Err(ServerError::ddl(format!(
                    "unknown sequence owner metadata role '{}' on line {}",
                    owner_role,
                    line_no + 1
                )));
            }
            owners.push((sequence_name, owner_role, namespace));
        }
        self.sequence_owners.clear();
        self.sequence_namespaces.clear();
        for (sequence_name, owner_role, namespace) in owners {
            self.sequence_owners
                .insert(sequence_name.clone(), owner_role);
            self.sequence_namespaces.insert(sequence_name, namespace);
        }
        Ok(())
    }

    pub(crate) fn schema_metadata_path(&self) -> Option<std::path::PathBuf> {
        self.data_dir
            .as_ref()
            .map(|dir| dir.join("pg_schema_runtime.meta"))
    }

    pub(crate) fn persist_schema_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.schema_metadata_path() else {
            return Ok(());
        };
        let mut schemas = self
            .schemas
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().as_ref().clone()))
            .collect::<Vec<_>>();
        schemas.sort_by(|left, right| left.0.cmp(&right.0));

        let mut out = String::from("# ultrasql schemas v1\n");
        for (_, schema) in schemas {
            out.push_str(&format!(
                "schema\t{}\t{}\n",
                metadata_escape(&schema.name),
                metadata_escape(&schema.owner_role)
            ));
        }
        write_runtime_metadata_file(&path, &out)
    }

    pub(crate) fn rebuild_schema_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.schema_metadata_path() else {
            return Ok(());
        };
        let Some(text) = read_runtime_metadata_file(&path)? else {
            return Ok(());
        };

        let mut schemas = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let known_roles = runtime_metadata_known_role_names(&self.role_catalog);
        for (line_no, line) in text.lines().enumerate() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts = line.split('\t').collect::<Vec<_>>();
            if parts.len() != 3 || parts.first().copied() != Some("schema") {
                return Err(ServerError::ddl(format!(
                    "malformed schema metadata line {}",
                    line_no + 1
                )));
            }
            let name = metadata_unescape(parts[1])?.to_ascii_lowercase();
            let owner_role = metadata_unescape(parts[2])?.to_ascii_lowercase();
            if name.is_empty() || owner_role.is_empty() {
                return Err(ServerError::ddl(format!(
                    "empty schema metadata field on line {}",
                    line_no + 1
                )));
            }
            if builtin_schema_name(&name) {
                return Err(ServerError::ddl(format!(
                    "schema metadata line {} attempts to override built-in schema '{}'",
                    line_no + 1,
                    name
                )));
            }
            if !seen.insert(name.clone()) {
                return Err(ServerError::ddl(format!(
                    "duplicate schema metadata '{}' on line {}",
                    name,
                    line_no + 1
                )));
            }
            if !known_roles.contains(&owner_role) {
                return Err(ServerError::ddl(format!(
                    "unknown schema metadata owner '{}' on line {}",
                    owner_role,
                    line_no + 1
                )));
            }
            schemas.push(RuntimeSchema { name, owner_role });
        }
        self.schemas.clear();
        for schema in schemas {
            self.schemas.insert(schema.name.clone(), Arc::new(schema));
        }
        Ok(())
    }

    pub(crate) fn refresh_persistent_catalog_schema_names(&self) {
        let namespace_names = self
            .schemas
            .iter()
            .map(|entry| {
                (
                    ultrasql_core::Oid::new(runtime_schema_oid(entry.key())),
                    entry.key().clone(),
                )
            })
            .collect::<std::collections::HashMap<_, _>>();
        self.persistent_catalog
            .refresh_runtime_schema_names(&namespace_names);
    }

    pub(crate) fn operator_metadata_path(&self) -> Option<std::path::PathBuf> {
        self.data_dir
            .as_ref()
            .map(|dir| dir.join("pg_operator_runtime.meta"))
    }

    pub(crate) fn persist_operator_metadata(&self) -> Result<(), ServerError> {
        let Some(path) = self.operator_metadata_path() else {
            return Ok(());
        };
        let mut operators = self
            .operators
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().as_ref().clone()))
            .collect::<Vec<_>>();
        operators.sort_by(|left, right| left.0.cmp(&right.0));

        let mut out = String::from("# ultrasql operator runtime v1\n");
        for (_, operator) in operators {
            let left = operator_data_type_token(&operator.left_type, &operator.name)?;
            let right = operator_data_type_token(&operator.right_type, &operator.name)?;
            let Some(result) = data_type_token(&operator.result_type) else {
                return Err(ServerError::ddl(format!(
                    "operator '{}' result type is outside restart-persistable metadata subset",
                    operator.name
                )));
            };
            out.push_str(&format!(
                "operator\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                operator.oid,
                metadata_escape(&operator.namespace),
                metadata_escape(&operator.name),
                metadata_escape(&left),
                metadata_escape(&right),
                metadata_escape(&operator.procedure),
                metadata_escape(&result)
            ));
        }
        write_runtime_metadata_file(&path, &out)
    }

}
