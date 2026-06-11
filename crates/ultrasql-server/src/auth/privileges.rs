//! In-memory object privilege catalog.
//!
//! PostgreSQL stores ACL arrays on object catalog rows. UltraSQL does
//! not yet persist those ACL attributes, so this module provides the
//! same-process catalog used by `GRANT` / `REVOKE` and certification
//! helpers such as `has_table_privilege`.

use std::collections::BTreeMap;

use parking_lot::RwLock;

/// Object class addressed by a privilege entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PrivilegeObjectKind {
    /// Table or view privileges.
    Table,
    /// Schema privileges.
    Schema,
    /// Database privileges.
    Database,
    /// Sequence privileges.
    Sequence,
    /// Function or routine privileges.
    Function,
}

/// Concrete object privilege.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PrivilegeKind {
    /// `SELECT`.
    Select,
    /// `INSERT`.
    Insert,
    /// `UPDATE`.
    Update,
    /// `DELETE`.
    Delete,
    /// `TRUNCATE`.
    Truncate,
    /// `REFERENCES`.
    References,
    /// `TRIGGER`.
    Trigger,
    /// `USAGE`.
    Usage,
    /// `CREATE`.
    Create,
    /// `CONNECT`.
    Connect,
    /// `TEMPORARY`.
    Temporary,
    /// `EXECUTE`.
    Execute,
}

/// Snapshot row describing one granted privilege.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrivilegeGrant {
    /// Object class.
    pub object_kind: PrivilegeObjectKind,
    /// Normalized object name.
    pub object_name: String,
    /// Recipient role, or `public`.
    pub grantee: String,
    /// Granted privilege.
    pub privilege: PrivilegeKind,
    /// Folded column name for column-level grants.
    pub column_name: Option<String>,
    /// Granting role.
    pub grantor: String,
    /// Whether the grantee may grant this privilege onward.
    pub grant_option: bool,
}

/// Snapshot row describing one default privilege template.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DefaultPrivilegeGrant {
    /// Role that will own future objects.
    pub owner_role: String,
    /// Optional schema filter. `None` means every schema.
    pub schema_name: Option<String>,
    /// Future object class.
    pub object_kind: PrivilegeObjectKind,
    /// Recipient role, or `public`.
    pub grantee: String,
    /// Privilege granted to matching future objects.
    pub privilege: PrivilegeKind,
    /// Role that changed the default ACL.
    pub grantor: String,
    /// Whether applied grants include grant option.
    pub grant_option: bool,
}

/// Privilege requested by one GRANT/REVOKE item.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrivilegeRequest {
    /// Granted or revoked privilege.
    pub privilege: PrivilegeKind,
    /// Folded column names. Empty means object-level.
    pub columns: Vec<String>,
}

/// Input for adding default privilege templates.
#[derive(Clone, Copy, Debug)]
pub struct DefaultPrivilegeUpdate<'a> {
    /// Role that changed the default ACL.
    pub grantor: &'a str,
    /// Roles whose future objects receive the default ACL.
    pub owner_roles: &'a [String],
    /// Optional schema filters. Empty means every schema.
    pub schemas: &'a [String],
    /// Future object class.
    pub object_kind: PrivilegeObjectKind,
    /// Recipient roles.
    pub grantees: &'a [String],
    /// Privileges to apply to matching future objects.
    pub privileges: &'a [PrivilegeRequest],
    /// Whether applied grants include grant option.
    pub grant_option: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct PrivilegeKey {
    object_kind: PrivilegeObjectKind,
    object_name: String,
    grantee: String,
    privilege: PrivilegeKind,
    column_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct DefaultPrivilegeKey {
    owner_role: String,
    schema_name: Option<String>,
    object_kind: PrivilegeObjectKind,
    grantee: String,
    privilege: PrivilegeKind,
}

/// Same-process privilege catalog backed by a deterministic map.
#[derive(Debug, Default)]
pub struct InMemoryPrivilegeCatalog {
    grants: RwLock<BTreeMap<PrivilegeKey, PrivilegeGrant>>,
    default_grants: RwLock<BTreeMap<DefaultPrivilegeKey, DefaultPrivilegeGrant>>,
}

impl InMemoryPrivilegeCatalog {
    /// Create an empty privilege catalog.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Grant every listed privilege to every grantee for every object.
    pub fn grant_many(
        &self,
        grantor: &str,
        object_kind: PrivilegeObjectKind,
        objects: &[String],
        grantees: &[String],
        privileges: &[PrivilegeRequest],
        grant_option: bool,
    ) {
        let mut grants = self.grants.write();
        for object in objects {
            let object_name = normalize_object_name(object_kind, object);
            for grantee in grantees {
                let grantee = grantee.to_ascii_lowercase();
                for request in privileges {
                    for column_name in privilege_columns(request) {
                        let key = PrivilegeKey {
                            object_kind,
                            object_name: object_name.clone(),
                            grantee: grantee.clone(),
                            privilege: request.privilege,
                            column_name: column_name.clone(),
                        };
                        grants.insert(
                            key,
                            PrivilegeGrant {
                                object_kind,
                                object_name: object_name.clone(),
                                grantee: grantee.clone(),
                                privilege: request.privilege,
                                column_name,
                                grantor: grantor.to_owned(),
                                grant_option,
                            },
                        );
                    }
                }
            }
        }
    }

    /// Revoke every listed privilege from every grantee for every object.
    pub fn revoke_many(
        &self,
        object_kind: PrivilegeObjectKind,
        objects: &[String],
        grantees: &[String],
        privileges: &[PrivilegeRequest],
    ) {
        let mut grants = self.grants.write();
        for object in objects {
            let object_name = normalize_object_name(object_kind, object);
            for grantee in grantees {
                let grantee = grantee.to_ascii_lowercase();
                for request in privileges {
                    for column_name in privilege_columns(request) {
                        grants.remove(&PrivilegeKey {
                            object_kind,
                            object_name: object_name.clone(),
                            grantee: grantee.clone(),
                            privilege: request.privilege,
                            column_name,
                        });
                    }
                }
            }
        }
    }

    /// Remove all explicit grants attached to one object.
    pub fn remove_object_grants(&self, object_kind: PrivilegeObjectKind, object: &str) -> bool {
        let object_name = normalize_object_name(object_kind, object);
        let mut grants = self.grants.write();
        let before = grants.len();
        grants.retain(|key, _| key.object_kind != object_kind || key.object_name != object_name);
        grants.len() != before
    }

    /// Remove every default privilege template scoped to one schema.
    pub fn remove_default_grants_for_schema(&self, schema_name: &str) -> bool {
        let schema_name = schema_name.to_ascii_lowercase();
        let mut default_grants = self.default_grants.write();
        let before = default_grants.len();
        default_grants.retain(|key, _| key.schema_name.as_deref() != Some(schema_name.as_str()));
        default_grants.len() != before
    }

    /// Add default privileges applied to future objects owned by listed roles.
    pub fn grant_default_many(&self, update: DefaultPrivilegeUpdate<'_>) {
        let grantor = update.grantor.to_ascii_lowercase();
        let owner_roles = normalize_names(update.owner_roles);
        let schema_names = normalize_default_schemas(update.schemas);
        let mut default_grants = self.default_grants.write();
        for owner_role in owner_roles {
            for schema_name in &schema_names {
                for grantee in update.grantees {
                    let grantee = grantee.to_ascii_lowercase();
                    for request in update.privileges {
                        if !request.columns.is_empty() {
                            continue;
                        }
                        let key = DefaultPrivilegeKey {
                            owner_role: owner_role.clone(),
                            schema_name: schema_name.clone(),
                            object_kind: update.object_kind,
                            grantee: grantee.clone(),
                            privilege: request.privilege,
                        };
                        default_grants.insert(
                            key,
                            DefaultPrivilegeGrant {
                                owner_role: owner_role.clone(),
                                schema_name: schema_name.clone(),
                                object_kind: update.object_kind,
                                grantee: grantee.clone(),
                                privilege: request.privilege,
                                grantor: grantor.clone(),
                                grant_option: update.grant_option,
                            },
                        );
                    }
                }
            }
        }
    }

    /// Remove default privileges for future objects owned by listed roles.
    pub fn revoke_default_many(
        &self,
        owner_roles: &[String],
        schemas: &[String],
        object_kind: PrivilegeObjectKind,
        grantees: &[String],
        privileges: &[PrivilegeRequest],
    ) {
        let owner_roles = normalize_names(owner_roles);
        let schema_names = normalize_default_schemas(schemas);
        let mut default_grants = self.default_grants.write();
        for owner_role in owner_roles {
            for schema_name in &schema_names {
                for grantee in grantees {
                    let grantee = grantee.to_ascii_lowercase();
                    for request in privileges {
                        if !request.columns.is_empty() {
                            continue;
                        }
                        default_grants.remove(&DefaultPrivilegeKey {
                            owner_role: owner_role.clone(),
                            schema_name: schema_name.clone(),
                            object_kind,
                            grantee: grantee.clone(),
                            privilege: request.privilege,
                        });
                    }
                }
            }
        }
    }

    /// Apply matching default privileges to one newly created object.
    pub fn apply_default_privileges(
        &self,
        owner_role: &str,
        schema_name: &str,
        object_kind: PrivilegeObjectKind,
        object: &str,
    ) {
        let owner_role = owner_role.to_ascii_lowercase();
        let schema_name = schema_name.to_ascii_lowercase();
        let object_name =
            normalize_default_privilege_object_name(object_kind, &schema_name, object);
        let matching = {
            let default_grants = self.default_grants.read();
            default_grants
                .values()
                .filter(|grant| {
                    grant.owner_role == owner_role
                        && grant.object_kind == object_kind
                        && grant
                            .schema_name
                            .as_ref()
                            .is_none_or(|schema| schema == &schema_name)
                })
                .cloned()
                .collect::<Vec<_>>()
        };
        if matching.is_empty() {
            return;
        }
        let mut grants = self.grants.write();
        for default in matching {
            let key = PrivilegeKey {
                object_kind,
                object_name: object_name.clone(),
                grantee: default.grantee.clone(),
                privilege: default.privilege,
                column_name: None,
            };
            grants.insert(
                key,
                PrivilegeGrant {
                    object_kind,
                    object_name: object_name.clone(),
                    grantee: default.grantee,
                    privilege: default.privilege,
                    column_name: None,
                    grantor: default.owner_role,
                    grant_option: default.grant_option,
                },
            );
        }
    }

    /// Return whether `grantee` has `privilege` on `object`.
    #[must_use]
    pub fn has_privilege(
        &self,
        grantee: &str,
        object_kind: PrivilegeObjectKind,
        object: &str,
        privilege: PrivilegeKind,
    ) -> bool {
        let object_name = normalize_object_name(object_kind, object);
        let grantee = grantee.to_ascii_lowercase();
        let grants = self.grants.read();
        grants.contains_key(&PrivilegeKey {
            object_kind,
            object_name: object_name.clone(),
            grantee,
            privilege,
            column_name: None,
        }) || grants.contains_key(&PrivilegeKey {
            object_kind,
            object_name,
            grantee: "public".to_owned(),
            privilege,
            column_name: None,
        })
    }

    /// Return whether any listed role has `privilege` on `object`.
    #[must_use]
    pub fn has_privilege_for_roles(
        &self,
        grantees: &[String],
        object_kind: PrivilegeObjectKind,
        object: &str,
        privilege: PrivilegeKind,
    ) -> bool {
        grantees
            .iter()
            .any(|grantee| self.has_privilege(grantee, object_kind, object, privilege))
    }

    /// Return whether `grantee` has `privilege` on one column.
    #[must_use]
    pub fn has_column_privilege(
        &self,
        grantee: &str,
        object_kind: PrivilegeObjectKind,
        object: &str,
        column: &str,
        privilege: PrivilegeKind,
    ) -> bool {
        let object_name = normalize_object_name(object_kind, object);
        let column_name = Some(column.to_ascii_lowercase());
        let grantee = grantee.to_ascii_lowercase();
        let grants = self.grants.read();
        for subject in [grantee.as_str(), "public"] {
            if grants.contains_key(&PrivilegeKey {
                object_kind,
                object_name: object_name.clone(),
                grantee: subject.to_owned(),
                privilege,
                column_name: None,
            }) || grants.contains_key(&PrivilegeKey {
                object_kind,
                object_name: object_name.clone(),
                grantee: subject.to_owned(),
                privilege,
                column_name: column_name.clone(),
            }) {
                return true;
            }
        }
        false
    }

    /// Return whether any listed role has `privilege` on one column.
    #[must_use]
    pub fn has_column_privilege_for_roles(
        &self,
        grantees: &[String],
        object_kind: PrivilegeObjectKind,
        object: &str,
        column: &str,
        privilege: PrivilegeKind,
    ) -> bool {
        grantees.iter().any(|grantee| {
            self.has_column_privilege(grantee, object_kind, object, column, privilege)
        })
    }

    /// Return a deterministic snapshot of all grants.
    #[must_use]
    pub fn list_grants(&self) -> Vec<PrivilegeGrant> {
        self.grants.read().values().cloned().collect()
    }

    /// Return a deterministic snapshot of all default grants.
    #[must_use]
    pub fn list_default_grants(&self) -> Vec<DefaultPrivilegeGrant> {
        self.default_grants.read().values().cloned().collect()
    }

    /// Replace privilege and default-privilege state from a durable snapshot.
    pub fn install_snapshot(
        &self,
        grants: Vec<PrivilegeGrant>,
        default_grants: Vec<DefaultPrivilegeGrant>,
    ) {
        let mut grant_map = BTreeMap::new();
        for mut grant in grants {
            grant.object_name = normalize_object_name(grant.object_kind, &grant.object_name);
            grant.grantee = grant.grantee.to_ascii_lowercase();
            grant.grantor = grant.grantor.to_ascii_lowercase();
            grant.column_name = grant.column_name.map(|column| column.to_ascii_lowercase());
            grant_map.insert(
                PrivilegeKey {
                    object_kind: grant.object_kind,
                    object_name: grant.object_name.clone(),
                    grantee: grant.grantee.clone(),
                    privilege: grant.privilege,
                    column_name: grant.column_name.clone(),
                },
                grant,
            );
        }

        let mut default_map = BTreeMap::new();
        for mut grant in default_grants {
            grant.owner_role = grant.owner_role.to_ascii_lowercase();
            grant.schema_name = grant.schema_name.map(|schema| schema.to_ascii_lowercase());
            grant.grantee = grant.grantee.to_ascii_lowercase();
            grant.grantor = grant.grantor.to_ascii_lowercase();
            default_map.insert(
                DefaultPrivilegeKey {
                    owner_role: grant.owner_role.clone(),
                    schema_name: grant.schema_name.clone(),
                    object_kind: grant.object_kind,
                    grantee: grant.grantee.clone(),
                    privilege: grant.privilege,
                },
                grant,
            );
        }

        *self.grants.write() = grant_map;
        *self.default_grants.write() = default_map;
    }
}

fn normalize_default_privilege_object_name(
    kind: PrivilegeObjectKind,
    schema_name: &str,
    name: &str,
) -> String {
    let folded = name.trim().to_ascii_lowercase();
    match kind {
        PrivilegeObjectKind::Table | PrivilegeObjectKind::Sequence => {
            if ultrasql_catalog::decode_table_lookup_key(&folded).is_some() {
                folded
            } else {
                ultrasql_catalog::table_lookup_key(schema_name, &folded)
            }
        }
        _ => normalize_object_name(kind, &folded),
    }
}

fn normalize_object_name(kind: PrivilegeObjectKind, name: &str) -> String {
    let folded = name.trim().to_ascii_lowercase();
    match kind {
        PrivilegeObjectKind::Function => {
            let compact = folded
                .chars()
                .filter(|ch| !ch.is_ascii_whitespace())
                .collect::<String>();
            let base = compact
                .split_once('(')
                .map_or(compact.as_str(), |(base, _)| base);
            last_name_part(base).to_owned()
        }
        PrivilegeObjectKind::Table | PrivilegeObjectKind::Sequence => {
            normalize_relation_object_name(&folded)
        }
        PrivilegeObjectKind::Schema | PrivilegeObjectKind::Database => {
            normalize_single_object_name(&folded)
        }
    }
}

fn normalize_relation_object_name(folded: &str) -> String {
    if ultrasql_catalog::decode_table_lookup_key(folded).is_some() {
        return folded.to_owned();
    }
    match crate::parse_pg_identifier_path(folded).as_deref() {
        Some([relation_name]) => ultrasql_catalog::table_lookup_key("public", relation_name),
        Some([schema_name, relation_name]) => {
            ultrasql_catalog::table_lookup_key(schema_name, relation_name)
        }
        _ => folded.to_owned(),
    }
}

fn last_name_part(name: &str) -> &str {
    name.rsplit_once('.').map_or(name, |(_, last)| last)
}

fn normalize_single_object_name(folded: &str) -> String {
    if folded.starts_with('"')
        && let Some(parts) = crate::parse_pg_identifier_path(folded)
        && let [name] = parts.as_slice()
    {
        return name.to_ascii_lowercase();
    }
    folded.to_owned()
}

fn privilege_columns(request: &PrivilegeRequest) -> Vec<Option<String>> {
    if request.columns.is_empty() {
        vec![None]
    } else {
        request
            .columns
            .iter()
            .map(|column| Some(column.to_ascii_lowercase()))
            .collect()
    }
}

fn normalize_names(names: &[String]) -> Vec<String> {
    names.iter().map(|name| name.to_ascii_lowercase()).collect()
}

fn normalize_default_schemas(schemas: &[String]) -> Vec<Option<String>> {
    if schemas.is_empty() {
        vec![None]
    } else {
        schemas
            .iter()
            .map(|schema| Some(schema.to_ascii_lowercase()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grants_and_revokes_are_visible() {
        let catalog = InMemoryPrivilegeCatalog::new();
        catalog.grant_many(
            "ultrasql",
            PrivilegeObjectKind::Table,
            &["public.t".to_owned()],
            &["analyst".to_owned()],
            &[
                PrivilegeRequest {
                    privilege: PrivilegeKind::Select,
                    columns: Vec::new(),
                },
                PrivilegeRequest {
                    privilege: PrivilegeKind::Insert,
                    columns: Vec::new(),
                },
            ],
            false,
        );
        assert!(catalog.has_privilege(
            "analyst",
            PrivilegeObjectKind::Table,
            "t",
            PrivilegeKind::Select
        ));
        catalog.revoke_many(
            PrivilegeObjectKind::Table,
            &["t".to_owned()],
            &["analyst".to_owned()],
            &[PrivilegeRequest {
                privilege: PrivilegeKind::Insert,
                columns: Vec::new(),
            }],
        );
        assert!(!catalog.has_privilege(
            "analyst",
            PrivilegeObjectKind::Table,
            "t",
            PrivilegeKind::Insert
        ));
    }

    #[test]
    fn public_grants_match_any_role() {
        let catalog = InMemoryPrivilegeCatalog::new();
        catalog.grant_many(
            "ultrasql",
            PrivilegeObjectKind::Function,
            &["pg_catalog.current_database".to_owned()],
            &["public".to_owned()],
            &[PrivilegeRequest {
                privilege: PrivilegeKind::Execute,
                columns: Vec::new(),
            }],
            false,
        );
        assert!(catalog.has_privilege(
            "analyst",
            PrivilegeObjectKind::Function,
            "current_database()",
            PrivilegeKind::Execute
        ));
    }

    #[test]
    fn column_grants_apply_only_to_named_column() {
        let catalog = InMemoryPrivilegeCatalog::new();
        catalog.grant_many(
            "ultrasql",
            PrivilegeObjectKind::Table,
            &["t".to_owned()],
            &["analyst".to_owned()],
            &[PrivilegeRequest {
                privilege: PrivilegeKind::Select,
                columns: vec!["id".to_owned()],
            }],
            false,
        );
        assert!(catalog.has_column_privilege(
            "analyst",
            PrivilegeObjectKind::Table,
            "t",
            "id",
            PrivilegeKind::Select
        ));
        assert!(!catalog.has_column_privilege(
            "analyst",
            PrivilegeObjectKind::Table,
            "t",
            "secret",
            PrivilegeKind::Select
        ));
    }

    #[test]
    fn remove_object_grants_clears_object_and_column_entries() {
        let catalog = InMemoryPrivilegeCatalog::new();
        catalog.grant_many(
            "ultrasql",
            PrivilegeObjectKind::Table,
            &["public.t".to_owned()],
            &["analyst".to_owned()],
            &[
                PrivilegeRequest {
                    privilege: PrivilegeKind::Select,
                    columns: Vec::new(),
                },
                PrivilegeRequest {
                    privilege: PrivilegeKind::Update,
                    columns: vec!["id".to_owned()],
                },
            ],
            false,
        );
        catalog.grant_many(
            "ultrasql",
            PrivilegeObjectKind::Sequence,
            &["t".to_owned()],
            &["analyst".to_owned()],
            &[PrivilegeRequest {
                privilege: PrivilegeKind::Usage,
                columns: Vec::new(),
            }],
            false,
        );

        assert!(catalog.remove_object_grants(PrivilegeObjectKind::Table, "t"));
        assert!(!catalog.has_privilege(
            "analyst",
            PrivilegeObjectKind::Table,
            "t",
            PrivilegeKind::Select
        ));
        assert!(!catalog.has_column_privilege(
            "analyst",
            PrivilegeObjectKind::Table,
            "t",
            "id",
            PrivilegeKind::Update
        ));
        assert!(catalog.has_privilege(
            "analyst",
            PrivilegeObjectKind::Sequence,
            "t",
            PrivilegeKind::Usage
        ));
        assert!(!catalog.remove_object_grants(PrivilegeObjectKind::Table, "t"));
    }

    #[test]
    fn default_privileges_apply_by_owner_schema_and_future_object() {
        let catalog = InMemoryPrivilegeCatalog::new();
        let owners = ["owner".to_owned()];
        let schemas = ["tenant".to_owned()];
        let grantees = ["analyst".to_owned()];
        let privileges = [PrivilegeRequest {
            privilege: PrivilegeKind::Select,
            columns: Vec::new(),
        }];
        catalog.grant_default_many(DefaultPrivilegeUpdate {
            grantor: "owner",
            owner_roles: &owners,
            schemas: &schemas,
            object_kind: PrivilegeObjectKind::Table,
            grantees: &grantees,
            privileges: &privileges,
            grant_option: false,
        });

        catalog.apply_default_privileges("owner", "public", PrivilegeObjectKind::Table, "early");
        assert!(!catalog.has_privilege(
            "analyst",
            PrivilegeObjectKind::Table,
            "early",
            PrivilegeKind::Select
        ));

        catalog.apply_default_privileges("owner", "tenant", PrivilegeObjectKind::Table, "future");
        assert!(catalog.has_privilege(
            "analyst",
            PrivilegeObjectKind::Table,
            "tenant.future",
            PrivilegeKind::Select
        ));

        catalog.revoke_default_many(
            &["owner".to_owned()],
            &["tenant".to_owned()],
            PrivilegeObjectKind::Table,
            &["analyst".to_owned()],
            &[PrivilegeRequest {
                privilege: PrivilegeKind::Select,
                columns: Vec::new(),
            }],
        );
        catalog.apply_default_privileges("owner", "tenant", PrivilegeObjectKind::Table, "later");
        assert!(!catalog.has_privilege(
            "analyst",
            PrivilegeObjectKind::Table,
            "later",
            PrivilegeKind::Select
        ));
        assert!(catalog.has_privilege(
            "analyst",
            PrivilegeObjectKind::Table,
            "tenant.future",
            PrivilegeKind::Select
        ));
    }

    #[test]
    fn default_privileges_preserve_dotted_future_relation_name() {
        let catalog = InMemoryPrivilegeCatalog::new();
        let owners = ["owner".to_owned()];
        let grantees = ["analyst".to_owned()];
        let privileges = [PrivilegeRequest {
            privilege: PrivilegeKind::Select,
            columns: Vec::new(),
        }];
        catalog.grant_default_many(DefaultPrivilegeUpdate {
            grantor: "owner",
            owner_roles: &owners,
            schemas: &[],
            object_kind: PrivilegeObjectKind::Table,
            grantees: &grantees,
            privileges: &privileges,
            grant_option: false,
        });

        catalog.apply_default_privileges(
            "owner",
            "public",
            PrivilegeObjectKind::Table,
            "future.dot",
        );

        assert!(catalog.has_privilege(
            "analyst",
            PrivilegeObjectKind::Table,
            "\"future.dot\"",
            PrivilegeKind::Select
        ));
        assert!(!catalog.has_privilege(
            "analyst",
            PrivilegeObjectKind::Table,
            "future.dot",
            PrivilegeKind::Select
        ));
    }
}
