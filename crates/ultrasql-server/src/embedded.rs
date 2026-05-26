//! Embedded in-process database API.

use std::fmt;
use std::path::Path;
use std::sync::Arc;

use tokio::io::{DuplexStream, duplex};

use crate::session::Session;
use crate::{LocalQueryOutput, Server, ServerError};

/// In-process UltraSQL database handle.
///
/// The handle owns normal server session state without binding a socket or
/// speaking the PostgreSQL wire protocol. Statements still use the same parser,
/// binder, planner, executor, transaction manager, catalog, heap, and WAL code
/// paths as a TCP client, then materialise text rows for embedding callers.
pub struct EmbeddedDatabase {
    session: Session<DuplexStream>,
}

impl fmt::Debug for EmbeddedDatabase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EmbeddedDatabase").finish_non_exhaustive()
    }
}

impl EmbeddedDatabase {
    /// Open an in-memory embedded database.
    #[must_use]
    pub fn open_memory() -> Self {
        Self::from_server(Server::with_empty_database())
    }

    /// Open an embedded database from a target.
    ///
    /// `:memory:` creates an in-memory database. Any other target is treated as
    /// a data directory and booted through [`Server::init`].
    pub fn open(target: impl AsRef<Path>) -> Result<Self, ServerError> {
        let path = target.as_ref();
        if path.as_os_str() == ":memory:" {
            return Ok(Self::open_memory());
        }
        Self::open_path(path)
    }

    /// Open a WAL-backed embedded database rooted at `data_dir`.
    pub fn open_path(data_dir: impl AsRef<Path>) -> Result<Self, ServerError> {
        Server::init(data_dir.as_ref()).map(Self::from_server)
    }

    /// Execute one SQL statement and return its materialised result.
    pub fn execute(&mut self, sql: &str) -> Result<LocalQueryOutput, ServerError> {
        self.session.execute_embedded_query(sql)
    }

    fn from_server(server: Server) -> Self {
        let (io, _peer) = duplex(1);
        Self {
            session: Session::new(io, Arc::new(server)),
        }
    }
}
