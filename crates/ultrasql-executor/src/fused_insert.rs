//! Fast fixed-width INSERT operator for literal `(Int32, Int32)` values.

use std::sync::Arc;

use ultrasql_core::{CommandId, DataType, Field, RelationId, Schema, Xid};
use ultrasql_storage::heap::{HeapAccess, InsertOptions};
use ultrasql_storage::{PageLoader, WalSink};
use ultrasql_vec::Batch;

use crate::affected_rows::affected_rows_batch;
use crate::{ExecError, Operator};

/// Inserts already-bound `(Int32, Int32)` literal rows without routing through
/// `ValuesScan`, row materialisation, and `RowCodec` for every row.
pub struct FusedInsertInt32Pair<L: PageLoader> {
    heap: Arc<HeapAccess<L>>,
    relation: RelationId,
    rows: Vec<(i32, i32)>,
    xid: Xid,
    command_id: CommandId,
    wal: Option<Arc<dyn WalSink>>,
    vm: Option<Arc<ultrasql_storage::vm::VisibilityMap>>,
    schema: Schema,
    done: bool,
}

impl<L: PageLoader> std::fmt::Debug for FusedInsertInt32Pair<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FusedInsertInt32Pair")
            .field("relation", &self.relation)
            .field("rows", &self.rows.len())
            .finish()
    }
}

impl<L: PageLoader> FusedInsertInt32Pair<L> {
    /// Build an insert operator for rows that already match the target schema.
    pub fn new(
        heap: Arc<HeapAccess<L>>,
        relation: RelationId,
        rows: Vec<(i32, i32)>,
        xid: Xid,
        command_id: CommandId,
        wal: Option<Arc<dyn WalSink>>,
        vm: Option<Arc<ultrasql_storage::vm::VisibilityMap>>,
    ) -> Self {
        let schema = Schema::new_with_duplicate_names([Field::required("count", DataType::Int64)]);
        Self {
            heap,
            relation,
            rows,
            xid,
            command_id,
            wal,
            vm,
            schema,
            done: false,
        }
    }
}

impl<L: PageLoader + Send + Sync + std::fmt::Debug + 'static> Operator for FusedInsertInt32Pair<L> {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let mut payloads = Vec::with_capacity(self.rows.len());
        for (id, val) in &self.rows {
            let mut payload = [0u8; 9];
            payload[1..5].copy_from_slice(&id.to_le_bytes());
            payload[5..9].copy_from_slice(&val.to_le_bytes());
            payloads.push(payload);
        }
        let payload_refs = payloads
            .iter()
            .map(|payload| payload.as_slice())
            .collect::<Vec<_>>();
        let wal_ref = self.wal.as_deref();
        let tids = self
            .heap
            .insert_batch(
                self.relation,
                &payload_refs,
                InsertOptions {
                    xmin: self.xid,
                    command_id: self.command_id,
                    wal: wal_ref,
                    fsm: None,
                    vm: self.vm.as_deref(),
                },
            )
            .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
        Ok(Some(affected_rows_batch(tids.len(), "fused INSERT")?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
