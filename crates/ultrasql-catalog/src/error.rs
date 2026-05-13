//! Catalog error type.
//!
//! [`CatalogError`] is the failure mode of mutating catalog operations
//! (create/drop a table or index, update a size estimate). Read paths
//! return [`Option`] rather than [`Result`] because a missing entry is
//! the expected miss case — callers route on `None`, not on a string.
//!
//! Errors here are intentionally distinct from [`ultrasql_core::Error`]:
//! a catalog error frequently carries SQL identifiers the caller wants
//! to inspect (for example, the binder turns [`Self::NotFound`] into a
//! syntactic error pointing at the offending identifier).

/// Top-level catalog failure.
///
/// Variants are mutually exclusive — a given catalog operation produces
/// exactly one of them on the failure path.
#[derive(Debug, thiserror::Error, PartialEq, Eq, Clone)]
#[non_exhaustive]
pub enum CatalogError {
    /// An object with the same name already exists. The string carries
    /// the name as the caller supplied it (preserving case so SQL
    /// error messages echo what the user wrote).
    #[error("catalog object already exists: {0}")]
    AlreadyExists(String),

    /// No object by that name (or OID) is registered.
    #[error("catalog object not found: {0}")]
    NotFound(String),

    /// Creating this object would introduce a dependency cycle. Reserved
    /// for the persistent implementation, which has to validate foreign-
    /// key and view chains; the in-memory implementation never produces
    /// this variant today.
    #[error("catalog dependency cycle: {0}")]
    DependencyCycle(String),

    /// The supplied entry conflicts with an existing one in a way that
    /// is not a simple name clash — for example, an index references a
    /// table OID that is not registered, or an index attnum is out of
    /// range for the underlying table's schema.
    #[error("catalog schema conflict: {0}")]
    SchemaConflict(String),
}

impl CatalogError {
    /// Build an [`Self::AlreadyExists`] from any string-like.
    pub fn already_exists<S: Into<String>>(name: S) -> Self {
        Self::AlreadyExists(name.into())
    }

    /// Build a [`Self::NotFound`] from any string-like.
    pub fn not_found<S: Into<String>>(name: S) -> Self {
        Self::NotFound(name.into())
    }

    /// Build a [`Self::DependencyCycle`] from any string-like.
    pub fn dependency_cycle<S: Into<String>>(name: S) -> Self {
        Self::DependencyCycle(name.into())
    }

    /// Build a [`Self::SchemaConflict`] from any string-like.
    pub fn schema_conflict<S: Into<String>>(msg: S) -> Self {
        Self::SchemaConflict(msg.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors_carry_payload() {
        let e = CatalogError::already_exists("users");
        match e {
            CatalogError::AlreadyExists(s) => assert_eq!(s, "users"),
            other => panic!("expected AlreadyExists, got {other:?}"),
        }
    }

    #[test]
    fn display_contains_name() {
        let e = CatalogError::not_found("orders");
        let s = e.to_string();
        assert!(s.contains("orders"), "{s}");
    }

    #[test]
    fn variants_are_distinct() {
        assert_ne!(
            CatalogError::already_exists("a"),
            CatalogError::not_found("a")
        );
    }
}
