//! Shared ownership checks for session-level DDL.

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_core::Oid;

use super::Session;
use crate::auth::pg_authid::AuthCatalog;
use crate::error::ServerError;

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    pub(super) fn ensure_table_owner_or_superuser(
        &self,
        table_oid: Oid,
        table_name: &str,
    ) -> Result<(), ServerError> {
        let current_user = self.current_user.to_ascii_lowercase();
        if self
            .state
            .role_catalog
            .lookup_role(&current_user)
            .is_some_and(|role| role.is_superuser)
        {
            return Ok(());
        }
        let owns_table = self
            .state
            .row_security
            .get(&table_oid)
            .is_some_and(|runtime| runtime.owner_role.eq_ignore_ascii_case(&current_user));
        if owns_table {
            Ok(())
        } else {
            Err(ServerError::InsufficientPrivilege(format!(
                "permission denied to manage table {table_name}"
            )))
        }
    }
}
