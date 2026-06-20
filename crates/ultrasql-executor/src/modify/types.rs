//! Implementations for the small runtime descriptor types declared in the
//! parent module: [`SequenceDefault`] and [`ModifyTableStamps`].

use std::sync::Arc;

use ultrasql_core::{CommandId, RelationId, Xid};
use ultrasql_storage::sequence::Sequence;
use ultrasql_storage::wal_sink::WalSink;

use super::{ModifyTableStamps, SequenceDefault, SequenceNextvalObserver};

impl std::fmt::Debug for SequenceDefault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SequenceDefault")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl SequenceDefault {
    /// Build a sequence default that advances `sequence` when the
    /// corresponding INSERT column is omitted.
    #[must_use]
    pub fn new<N: Into<String>>(name: N, sequence: Arc<Sequence>) -> Self {
        Self {
            name: name.into(),
            sequence,
            on_nextval: None,
            wal: None,
            xid: Xid::INVALID,
            seqrelid: RelationId::INVALID,
        }
    }

    /// Attach a session-local observer called with every generated value.
    #[must_use]
    pub fn with_observer(mut self, on_nextval: SequenceNextvalObserver) -> Self {
        self.on_nextval = Some(on_nextval);
        self
    }

    /// Attach WAL context used when this default advances the sequence.
    #[must_use]
    pub fn with_wal(
        mut self,
        wal: Option<Arc<dyn WalSink>>,
        xid: Xid,
        seqrelid: RelationId,
    ) -> Self {
        self.wal = wal;
        self.xid = xid;
        self.seqrelid = seqrelid;
        self
    }
}

impl ModifyTableStamps {
    /// Create MVCC stamp metadata for table mutation.
    #[must_use]
    pub fn new(
        insert_xmin: Xid,
        insert_command_id: CommandId,
        delete_xmax: Xid,
        delete_cmax: CommandId,
    ) -> Self {
        Self {
            insert_xmin,
            insert_command_id,
            delete_xmax,
            delete_cmax,
        }
    }
}
