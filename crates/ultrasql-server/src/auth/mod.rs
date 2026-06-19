//! Authentication subsystem for UltraSQL.
//!
//! This module provides three distinct components:
//!
//! - **[`scram`]** — RFC 5802 / RFC 7677 SCRAM-SHA-256 server-side state
//!   machine. Converts a stored [`PasswordHash`] into a two-message
//!   challenge/response exchange with a PostgreSQL client.
//! - **[`pg_authid`]** — In-memory stand-in for the `pg_authid` system
//!   catalog. Exposes [`AuthCatalog`] and its default implementation
//!   [`InMemoryAuthCatalog`] so the connection handler can look up
//!   roles without touching persistent storage.
//! - **[`hba`]** — `pg_hba.conf`-syntax host-based access control. Parses
//!   a text rule file into an ordered [`HbaConfig`] and provides
//!   [`HbaConfig::match_rule`] for the connection handler.
//!
//! ## Layering note
//!
//! All three components are intentionally free of Tokio I/O. The
//! connection handler (in `lib.rs`) owns the I/O and drives these
//! components synchronously from an async context. This keeps the crypto
//! and access-control logic unit-testable without a runtime.

pub mod connection_limit;
pub mod hba;
pub mod md5;
pub mod pg_authid;
pub mod privileges;
pub mod scram;

pub use connection_limit::{ConnectionLimitError, RoleConnectionLimiter};
pub use hba::{HbaConfig, HbaConnectionKind, HbaDatabaseMatch, HbaMethod, HbaRule, HbaUserMatch};
pub use pg_authid::{
    AuthCatalog, InMemoryAuthCatalog, PasswordHash, RESERVED_ROLE_PREFIX, RoleEntry,
    RoleEntryChanges, RoleMembership, is_reserved_role_name,
};
pub use privileges::{
    DefaultPrivilegeGrant, DefaultPrivilegeUpdate, InMemoryPrivilegeCatalog, PrivilegeGrant,
    PrivilegeKind, PrivilegeObjectKind, PrivilegeRequest,
};
pub use scram::{AuthError, SCRAM_SHA_256, ScramSha256Server, parse_sasl_initial_response};
