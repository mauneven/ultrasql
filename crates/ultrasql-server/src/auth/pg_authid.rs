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

pub use crate::auth::scram::PasswordHash;
use parking_lot::RwLock;

/// A single role's authentication data.
///
/// Corresponds to a row in PostgreSQL's `pg_authid` table.
#[derive(Debug, Clone)]
pub struct RoleEntry {
    /// The role name (case-sensitive, as in PostgreSQL).
    pub name: String,
    /// The hashed password, or `None` if the role has no password set
    /// (i.e., `rolpassword IS NULL`).
    pub password: Option<PasswordHash>,
    /// Whether this role has `rolsuper = true`.
    pub is_superuser: bool,
    /// Whether this role has `rolcanlogin = true`.
    pub can_login: bool,
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
#[derive(Debug, Default)]
pub struct InMemoryAuthCatalog {
    roles: RwLock<HashMap<String, RoleEntry>>,
}

impl InMemoryAuthCatalog {
    /// Create an empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or replace a role entry.
    ///
    /// Thread-safe: acquires a write lock, inserts the role, releases
    /// the lock.
    pub fn add_role(&self, entry: RoleEntry) {
        self.roles.write().insert(entry.name.clone(), entry);
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
            name: "alice".to_owned(),
            password: Some(ph),
            is_superuser: false,
            can_login: true,
        });
        cat.add_role(RoleEntry {
            name: "root".to_owned(),
            password: None,
            is_superuser: true,
            can_login: true,
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
            name: "alice".to_owned(),
            password: Some(ph2),
            is_superuser: true,
            can_login: false,
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
