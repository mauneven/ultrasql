//! In-memory stand-in for the `pg_authid` system catalog.
//!
//! PostgreSQL stores role names, hashed passwords, and privilege bits in
//! the shared `pg_authid` catalog table. UltraSQL does not have a
//! persistent catalog yet (that is v0.8 work). This module provides:
//!
//! - [`AuthCatalog`] — the trait the connection handler depends on.
//! - [`RoleEntry`] — the per-role data the connection handler needs.
//! - [`InMemoryAuthCatalog`] — an in-memory implementation backed by
//!   a `HashMap`. Used for integration tests and initial bring-up.
//! - [`PasswordHash`] — re-exported from [`crate::auth::scram`] for
//!   callers that interact with this module alone.
//!
//! Wave 8 will add a `HeapAuthCatalog` that reads real `pg_authid` tuples
//! from the buffer pool, replacing [`InMemoryAuthCatalog`] in
//! production.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU32, Ordering};

pub use crate::auth::scram::PasswordHash;
use parking_lot::RwLock;
use ultrasql_catalog::CatalogError;

/// Bootstrap superuser OID used by virtual `pg_roles` / `pg_user`.
pub const BOOTSTRAP_ROLE_OID: u32 = 10;

const FIRST_USER_ROLE_OID: u32 = 16_384;

/// A single role's authentication data.
///
/// Corresponds to a row in PostgreSQL's `pg_authid` table.
#[derive(Debug, Clone)]
pub struct RoleEntry {
    /// Stable role OID for catalog views.
    pub oid: u32,
    /// The role name (case-sensitive, as in PostgreSQL).
    pub name: String,
    /// The hashed password, or `None` if the role has no password set
    /// (i.e., `rolpassword IS NULL`).
    pub password: Option<PasswordHash>,
    /// Whether this role has `rolsuper = true`.
    pub is_superuser: bool,
    /// Whether this role has `rolinherit = true`.
    pub inherit: bool,
    /// Whether this role has `rolcreaterole = true`.
    pub create_role: bool,
    /// Whether this role has `rolcreatedb = true`.
    pub create_db: bool,
    /// Whether this role has `rolcanlogin = true`.
    pub can_login: bool,
    /// Whether this role has `rolreplication = true`.
    pub replication: bool,
    /// Whether this role has `rolbypassrls = true`.
    pub bypass_rls: bool,
    /// `rolconnlimit`; `-1` means unlimited.
    pub connection_limit: i32,
    /// `rolvaliduntil` as microseconds since Unix epoch.
    pub valid_until: Option<i64>,
}

impl RoleEntry {
    /// Build the bootstrap `ultrasql` superuser role.
    #[must_use]
    pub fn bootstrap_superuser() -> Self {
        Self {
            oid: BOOTSTRAP_ROLE_OID,
            name: "ultrasql".to_owned(),
            password: None,
            is_superuser: true,
            inherit: true,
            create_role: true,
            create_db: true,
            can_login: true,
            replication: false,
            bypass_rls: false,
            connection_limit: -1,
            valid_until: None,
        }
    }
}

/// Partial role mutation used by `ALTER ROLE`.
#[derive(Debug, Clone, Default)]
pub struct RoleEntryChanges {
    /// New SCRAM password. `Some(None)` clears `rolpassword`.
    pub password: Option<Option<PasswordHash>>,
    /// New `rolsuper`.
    pub is_superuser: Option<bool>,
    /// New `rolinherit`.
    pub inherit: Option<bool>,
    /// New `rolcreaterole`.
    pub create_role: Option<bool>,
    /// New `rolcreatedb`.
    pub create_db: Option<bool>,
    /// New `rolcanlogin`.
    pub can_login: Option<bool>,
    /// New `rolreplication`.
    pub replication: Option<bool>,
    /// New `rolbypassrls`.
    pub bypass_rls: Option<bool>,
    /// New `rolconnlimit`.
    pub connection_limit: Option<i32>,
    /// New `rolvaliduntil`. `Some(None)` clears it.
    pub valid_until: Option<Option<i64>>,
}

/// One role-membership edge: `member` has been granted `role`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleMembership {
    /// Granted role name.
    pub role: String,
    /// Recipient role name.
    pub member: String,
    /// Granting role name.
    pub grantor: String,
    /// Whether `WITH ADMIN OPTION` was specified.
    pub admin_option: bool,
}

/// Interface for looking up authentication data for a named role.
///
/// # Contract
///
/// - `lookup_role` must be non-blocking and wait-free for read-heavy
///   workloads. Implementations that need locking use `RwLock` (not
///   `tokio::sync::RwLock`) per AGENTS.md §5.
/// - A return value of `None` means the role does not exist; the caller
///   treats this as an authentication failure.
/// - The returned [`RoleEntry`] is a snapshot; mutations to the catalog
///   after the call are not reflected.
pub trait AuthCatalog: Send + Sync {
    /// Look up the role named `name`. Returns `None` if no such role exists.
    fn lookup_role(&self, name: &str) -> Option<RoleEntry>;
}

/// In-memory [`AuthCatalog`] backed by a `HashMap<String, RoleEntry>`.
///
/// Suitable for integration tests and server bring-up. Not intended for
/// production use; replace with `HeapAuthCatalog` in v0.8.
///
/// Internally guarded by a [`parking_lot::RwLock`] so it is `Send + Sync`
/// and can be mutated (e.g., via [`InMemoryAuthCatalog::add_role`]) after
/// creation.
#[derive(Debug)]
pub struct InMemoryAuthCatalog {
    roles: RwLock<HashMap<String, RoleEntry>>,
    memberships: RwLock<BTreeMap<(String, String), RoleMembership>>,
    next_oid: AtomicU32,
}

impl Default for InMemoryAuthCatalog {
    fn default() -> Self {
        Self {
            roles: RwLock::new(HashMap::new()),
            memberships: RwLock::new(BTreeMap::new()),
            next_oid: AtomicU32::new(FIRST_USER_ROLE_OID),
        }
    }
}

impl InMemoryAuthCatalog {
    /// Create an empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a catalog seeded with the bootstrap `ultrasql` superuser.
    #[must_use]
    pub fn with_bootstrap_superuser() -> Self {
        let catalog = Self::new();
        catalog.add_role(RoleEntry::bootstrap_superuser());
        catalog
    }

    /// Insert or replace a role entry.
    ///
    /// Thread-safe: acquires a write lock, inserts the role, releases
    /// the lock.
    pub fn add_role(&self, mut entry: RoleEntry) {
        entry.name = normalize_role_name(&entry.name);
        self.roles.write().insert(entry.name.clone(), entry);
    }

    /// Create a new role and allocate an OID if the caller supplied `0`.
    pub fn create_role(&self, mut entry: RoleEntry) -> Result<(), CatalogError> {
        entry.name = normalize_role_name(&entry.name);
        let mut roles = self.roles.write();
        if roles.contains_key(&entry.name) {
            return Err(CatalogError::already_exists(entry.name));
        }
        if entry.oid == 0 {
            entry.oid = self.next_oid.fetch_add(1, Ordering::Relaxed);
        }
        roles.insert(entry.name.clone(), entry);
        Ok(())
    }

    /// Apply partial role changes.
    pub fn alter_role(&self, name: &str, changes: RoleEntryChanges) -> Result<(), CatalogError> {
        let name = normalize_role_name(name);
        let mut roles = self.roles.write();
        let entry = roles
            .get_mut(&name)
            .ok_or_else(|| CatalogError::not_found(name.clone()))?;
        if let Some(value) = changes.password {
            entry.password = value;
        }
        if let Some(value) = changes.is_superuser {
            entry.is_superuser = value;
        }
        if let Some(value) = changes.inherit {
            entry.inherit = value;
        }
        if let Some(value) = changes.create_role {
            entry.create_role = value;
        }
        if let Some(value) = changes.create_db {
            entry.create_db = value;
        }
        if let Some(value) = changes.can_login {
            entry.can_login = value;
        }
        if let Some(value) = changes.replication {
            entry.replication = value;
        }
        if let Some(value) = changes.bypass_rls {
            entry.bypass_rls = value;
        }
        if let Some(value) = changes.connection_limit {
            entry.connection_limit = value;
        }
        if let Some(value) = changes.valid_until {
            entry.valid_until = value;
        }
        Ok(())
    }

    /// Drop a role by name.
    pub fn drop_role(&self, name: &str) -> Result<(), CatalogError> {
        let name = normalize_role_name(name);
        if name == "ultrasql" {
            return Err(CatalogError::schema_conflict(
                "cannot drop bootstrap role ultrasql",
            ));
        }
        self.roles
            .write()
            .remove(&name)
            .map(|_| {
                self.memberships
                    .write()
                    .retain(|(role, member), _| role != &name && member != &name);
            })
            .ok_or_else(|| CatalogError::not_found(name))
    }

    /// Grant role memberships.
    pub fn grant_roles(
        &self,
        grantor: &str,
        roles: &[String],
        members: &[String],
        admin_option: bool,
    ) -> Result<(), CatalogError> {
        let roles = roles
            .iter()
            .map(|role| normalize_role_name(role))
            .collect::<Vec<_>>();
        let members = members
            .iter()
            .map(|member| normalize_role_name(member))
            .collect::<Vec<_>>();
        {
            let existing = self.roles.read();
            for role in roles.iter().chain(members.iter()) {
                if !existing.contains_key(role) {
                    return Err(CatalogError::not_found(role.clone()));
                }
            }
        }
        let mut memberships = self.memberships.write();
        for role in &roles {
            for member in &members {
                if role == member || membership_path_exists(&memberships, role, member) {
                    return Err(CatalogError::schema_conflict(format!(
                        "role membership would create a cycle: {member} -> {role}"
                    )));
                }
                memberships.insert(
                    (role.clone(), member.clone()),
                    RoleMembership {
                        role: role.clone(),
                        member: member.clone(),
                        grantor: normalize_role_name(grantor),
                        admin_option,
                    },
                );
            }
        }
        Ok(())
    }

    /// Revoke role memberships.
    pub fn revoke_roles(&self, roles: &[String], members: &[String]) {
        let roles = roles
            .iter()
            .map(|role| normalize_role_name(role))
            .collect::<Vec<_>>();
        let members = members
            .iter()
            .map(|member| normalize_role_name(member))
            .collect::<Vec<_>>();
        let mut memberships = self.memberships.write();
        for role in roles {
            for member in &members {
                memberships.remove(&(role.clone(), member.clone()));
            }
        }
    }

    /// Return whether `member` is transitively a member of `role`.
    #[must_use]
    pub fn is_member_of(&self, member: &str, role: &str) -> bool {
        let member = normalize_role_name(member);
        let role = normalize_role_name(role);
        if member == role {
            return true;
        }
        membership_path_exists(&self.memberships.read(), &member, &role)
    }

    /// Return roles whose privileges apply automatically to `member`.
    #[must_use]
    pub fn inherited_role_names(&self, member: &str) -> Vec<String> {
        let member = normalize_role_name(member);
        let Some(entry) = self.lookup_role(&member) else {
            return vec![member];
        };
        let mut roles = BTreeSet::from([member.clone()]);
        if entry.inherit {
            collect_memberships(&self.memberships.read(), &member, &mut roles);
        }
        roles.into_iter().collect()
    }

    /// Return whether `session_user` may `SET ROLE target`.
    #[must_use]
    pub fn can_set_role(&self, session_user: &str, target: &str) -> bool {
        let session_user = normalize_role_name(session_user);
        let target = normalize_role_name(target);
        let Some(session_entry) = self.lookup_role(&session_user) else {
            return false;
        };
        self.lookup_role(&target).is_some()
            && (session_user == target
                || session_entry.is_superuser
                || self.is_member_of(&session_user, &target))
    }

    /// Return a deterministic snapshot of all role memberships.
    #[must_use]
    pub fn list_memberships(&self) -> Vec<RoleMembership> {
        self.memberships.read().values().cloned().collect()
    }

    /// Return a deterministic snapshot of all roles.
    #[must_use]
    pub fn list_roles(&self) -> Vec<RoleEntry> {
        let mut roles = self.roles.read().values().cloned().collect::<Vec<_>>();
        roles.sort_by_key(|role| role.oid);
        roles
    }
}

impl AuthCatalog for InMemoryAuthCatalog {
    fn lookup_role(&self, name: &str) -> Option<RoleEntry> {
        self.roles.read().get(&normalize_role_name(name)).cloned()
    }
}

fn normalize_role_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

fn membership_path_exists(
    memberships: &BTreeMap<(String, String), RoleMembership>,
    start_member: &str,
    target_role: &str,
) -> bool {
    let mut seen = BTreeSet::new();
    let mut stack = vec![start_member.to_owned()];
    while let Some(member) = stack.pop() {
        if !seen.insert(member.clone()) {
            continue;
        }
        for membership in memberships.values().filter(|edge| edge.member == member) {
            if membership.role == target_role {
                return true;
            }
            stack.push(membership.role.clone());
        }
    }
    false
}

fn collect_memberships(
    memberships: &BTreeMap<(String, String), RoleMembership>,
    member: &str,
    out: &mut BTreeSet<String>,
) {
    for membership in memberships.values().filter(|edge| edge.member == member) {
        if out.insert(membership.role.clone()) {
            collect_memberships(memberships, &membership.role, out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_catalog() -> InMemoryAuthCatalog {
        let cat = InMemoryAuthCatalog::new();
        let salt = PasswordHash::random_salt();
        let ph = PasswordHash::hash_password("secret", &salt, 4096);
        let mut alice = test_role(1, "alice", true);
        alice.password = Some(ph);
        cat.add_role(alice);
        let mut root = test_role(2, "root", true);
        root.is_superuser = true;
        root.create_role = true;
        root.create_db = true;
        cat.add_role(root);
        cat
    }

    fn test_role(oid: u32, name: &str, inherit: bool) -> RoleEntry {
        RoleEntry {
            oid,
            name: name.to_owned(),
            password: None,
            is_superuser: false,
            inherit,
            create_role: false,
            create_db: false,
            can_login: true,
            replication: false,
            bypass_rls: false,
            connection_limit: -1,
            valid_until: None,
        }
    }

    #[test]
    fn lookup_existing_role_returns_entry() {
        let cat = make_catalog();
        let entry = cat.lookup_role("alice").expect("alice exists");
        assert_eq!(entry.name, "alice");
        assert!(!entry.is_superuser);
        assert!(entry.can_login);
        assert!(entry.password.is_some());
    }

    #[test]
    fn lookup_missing_role_returns_none() {
        let cat = make_catalog();
        assert!(cat.lookup_role("nobody").is_none());
    }

    #[test]
    fn superuser_has_no_password_by_default() {
        let cat = make_catalog();
        let root = cat.lookup_role("root").expect("root exists");
        assert!(root.is_superuser);
        assert!(root.password.is_none());
    }

    #[test]
    fn add_role_overwrites_existing() {
        let cat = make_catalog();
        let salt = PasswordHash::random_salt();
        let ph2 = PasswordHash::hash_password("new_secret", &salt, 4096);
        let mut replacement = test_role(1, "alice", true);
        replacement.password = Some(ph2);
        replacement.is_superuser = true;
        replacement.can_login = false;
        cat.add_role(replacement);
        let entry = cat.lookup_role("alice").expect("still exists");
        assert!(entry.is_superuser);
        assert!(!entry.can_login);
        // Stored key changed because password changed.
        assert!(entry.password.is_some());
    }

    #[test]
    fn inherited_roles_respect_role_inherit_flag() {
        let cat = make_catalog();
        cat.add_role(test_role(3, "group_role", true));
        cat.add_role(test_role(4, "inheriting_member", true));
        cat.add_role(test_role(5, "noinherit_member", false));
        cat.grant_roles(
            "root",
            &["group_role".to_owned()],
            &[
                "inheriting_member".to_owned(),
                "noinherit_member".to_owned(),
            ],
            false,
        )
        .expect("grant role membership");

        assert!(
            cat.inherited_role_names("inheriting_member")
                .contains(&"group_role".to_owned())
        );
        assert_eq!(
            cat.inherited_role_names("noinherit_member"),
            vec!["noinherit_member".to_owned()]
        );
        assert!(cat.can_set_role("noinherit_member", "group_role"));
    }

    #[test]
    fn role_membership_cycle_is_rejected() {
        let cat = make_catalog();
        cat.add_role(test_role(3, "left_role", true));
        cat.add_role(test_role(4, "right_role", true));
        cat.grant_roles(
            "root",
            &["left_role".to_owned()],
            &["right_role".to_owned()],
            false,
        )
        .expect("grant first edge");

        assert!(
            cat.grant_roles(
                "root",
                &["right_role".to_owned()],
                &["left_role".to_owned()],
                false,
            )
            .is_err()
        );
    }

    #[test]
    fn catalog_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<InMemoryAuthCatalog>();
    }
}
