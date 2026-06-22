//! Embedded execution adapter for [`Session`].

use tokio::io::{AsyncRead, AsyncWrite};

use super::Session;
use crate::{LocalQueryOutput, ServerError, TxnState};

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Execute one embedded statement and materialise its result.
    pub(crate) fn execute_embedded_query(
        &mut self,
        sql: &str,
    ) -> Result<LocalQueryOutput, ServerError> {
        // The embedded API materialises the whole result into a
        // `LocalQueryOutput` via `local_output_from_select_result`, which
        // decodes a complete contiguous body — it cannot drive a streaming
        // handle. Request the whole-buffer path (`false`) so a large result
        // returns every row with a correct command_tag and never leaks the
        // streaming handle's XID.
        let result = self.execute_query(sql, false)?;
        if matches!(self.txn_state, TxnState::Idle) {
            self.run_post_response_maintenance();
        }
        crate::local_output_from_select_result(result)
    }
}
