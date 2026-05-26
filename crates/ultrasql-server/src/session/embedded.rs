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
        let result = self.execute_query(sql)?;
        if matches!(self.txn_state, TxnState::Idle) {
            self.run_post_response_maintenance();
        }
        crate::local_output_from_select_result(result)
    }
}
