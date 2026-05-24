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

use std::collections::HashMap;
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
    next_oid: AtomicU32,
}

impl Default for InMemoryAuthCatalog {
    fn default() -> Self {
        Self {
            roles: RwLock::new(HashMap::new()),
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
    pub fn add_role(&self, entry: RoleEntry) {
        self.roles.write().insert(entry.name.clone(), entry);
    }

    /// Create a new role and allocate an OID if the caller supplied `0`.
    pub fn create_role(&self, mut entry: RoleEntry) -> Result<(), CatalogError> {
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
        let mut roles = self.roles.write();
        let entry = roles
            .get_mut(name)
            .ok_or_else(|| CatalogError::not_found(name.to_owned()))?;
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
        if name == "ultrasql" {
            return Err(CatalogError::schema_conflict(
                "cannot drop bootstrap role ultrasql",
            ));
        }
        self.roles
            .write()
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| CatalogError::not_found(name.to_owned()))
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
        self.roles.read().get(name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_catalog() -> InMemoryAuthCatalog {
        let cat = InMemoryAuthCatalog::new();
        let salt = PasswordHash::random_salt();
        let ph = PasswordHash::hash_password("secret", &salt, 4096);
        cat.add_role(RoleEntry {
            oid: 1,
            name: "alice".to_owned(),
            password: Some(ph),
            is_superuser: false,
            inherit: true,
            create_role: false,
            create_db: false,
            can_login: true,
            replication: false,
            bypass_rls: false,
            connection_limit: -1,
            valid_until: None,
        });
        cat.add_role(RoleEntry {
            oid: 2,
            name: "root".to_owned(),
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
        });
        cat
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
        cat.add_role(RoleEntry {
            oid: 1,
            name: "alice".to_owned(),
            password: Some(ph2),
            is_superuser: true,
            inherit: true,
            create_role: false,
            create_db: false,
            can_login: false,
            replication: false,
            bypass_rls: false,
            connection_limit: -1,
            valid_until: None,
        });
        let entry = cat.lookup_role("alice").expect("still exists");
        assert!(entry.is_superuser);
        assert!(!entry.can_login);
        // Stored key changed because password changed.
        assert!(entry.password.is_some());
    }

    #[test]
    fn catalog_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<InMemoryAuthCatalog>();
    }
}
