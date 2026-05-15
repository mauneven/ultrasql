//! Per-connection [`Session`] state machine + impl pieces.
//!
//! The implementation is intentionally fragmented across several
//! files in this directory so no single unit exceeds the 600-line
//! ceiling. `mod.rs` carries the struct definition and the smallest
//! constructor; every other method lives in a sibling file that
//! reopens the same `impl<RW> Session<RW>` block.

#![allow(unused_imports)]

use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::extended::ExtendedConnState;
use crate::{READ_BUFFER_INITIAL, Server, TxnState};

mod alter;
mod ddl;
mod execute;
mod ext;
mod io;
mod meta_stmt;
mod run;
mod startup;
mod txn;

pub(crate) struct Session<RW> {
    pub(super) io: RW,
    pub(super) read_buf: BytesMut,
    pub(super) write_buf: BytesMut,
    pub(super) state: Arc<Server>,
    pub(super) extended: ExtendedConnState,
    pub(super) txn_state: TxnState,
}

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn new(io: RW, state: Arc<Server>) -> Self {
        Self {
            io,
            read_buf: BytesMut::with_capacity(READ_BUFFER_INITIAL),
            write_buf: BytesMut::with_capacity(READ_BUFFER_INITIAL),
            state,
            extended: crate::extended::ExtendedConnState::new(),
            txn_state: TxnState::Idle,
        }
    }
}
