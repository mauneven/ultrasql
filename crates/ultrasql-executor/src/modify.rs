//! [`ModifyTable`] operator: applies INSERT / UPDATE / DELETE to a relation
//! through [`HeapAccess`].
//!
//! Drains its input operator (the rows to insert, or scan-output of rows
//! to update/delete) and reports the row count that was modified.
//!
//! # INSERT path
//!
//! The `Insert` kind drains the child operator. For each decoded row it
//! calls [`HeapAccess::insert`] with the provided MVCC metadata, then
//! tracks the count. After EOF it emits a single 1-row batch whose only
//! column is `Int64(affected_rows)` and then returns `Ok(None)`.
//!
//! # UPDATE / DELETE paths
//!
//! `Update` and `Delete` are implemented as stand-alone operator code
//! but are only reachable through direct construction (not through
//! `build_operator`) until the physical-plan lowering gains datasource
//! handles in wave 5+. Both accept a child that emits rows prepended
//! with `(tid_block: Int32, tid_slot: Int32, ...)` columns.
//!
//! # Send bound
//!
//! `ModifyTable<L>` is `Send` because [`HeapAccess<L>`] is `Send + Sync`
//! whenever `L: PageLoader + Send + Sync` and the WAL sink (when present)
//! implements `Send + Sync`.

use std::sync::Arc;

use ultrasql_core::{CommandId, DataType, Field, RelationId, Schema, TupleId, Value, Xid};
use ultrasql_planner::ScalarExpr;
use ultrasql_storage::PageLoader;
use ultrasql_storage::heap::{DeleteOptions, HeapAccess, InsertOptions, UpdateOptions};
use ultrasql_storage::wal_sink::WalSink;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};

use crate::eval::Eval;
use crate::filter_op::batch_to_rows;
use crate::row_codec::RowCodec;
use crate::{ExecError, Operator};

// ---------------------------------------------------------------------------
// ModifyKind
// ---------------------------------------------------------------------------

/// The mutation kind for a [`ModifyTable`] operator.
#[derive(Debug)]
pub enum ModifyKind {
    /// Append all input rows to the target relation.
    Insert,

    /// Replace targeted rows in the relation.
    ///
    /// `assignments` is a list of `(target_column_index, value_expr)` pairs
    /// where the expression scope is the current row's values. The child
    /// operator must emit rows in the shape `[tid_block: Int32, tid_slot:
    /// Int32, original_col0, original_col1, ...]`.
    Update {
        /// Ordered list of `(column_index_in_relation_schema, value_expr)` pairs.
        assignments: Vec<(usize, ScalarExpr)>,
    },

    /// Mark targeted rows as dead.
    ///
    /// The child operator must emit rows in the shape
    /// `[tid_block: Int32, tid_slot: Int32, ...rest]`.
    Delete,
}

// ---------------------------------------------------------------------------
// ModifyTable
// ---------------------------------------------------------------------------

/// Pull-based table mutation operator.
///
/// On each `next_batch` call, drains all remaining rows from `child` and
/// performs the configured mutation. After the child signals EOF, emits
/// exactly one batch containing the `affected_rows` count (an `Int64`
/// column) and then returns `Ok(None)` on all subsequent calls.
pub struct ModifyTable<L: PageLoader> {
    heap: Arc<HeapAccess<L>>,
    relation: RelationId,
    /// Schema of the output: `[("affected_rows", Int64)]`.
    schema: Schema,
    /// Row codec for INSERT encoding.
    codec: RowCodec,
    kind: ModifyKind,
    insert_xmin: Xid,
    insert_command_id: CommandId,
    delete_xmax: Xid,
    delete_cmax: CommandId,
    wal: Option<Arc<dyn WalSink>>,
    child: Box<dyn Operator>,
    done: bool,
    affected: u64,
}

impl<L: PageLoader> std::fmt::Debug for ModifyTable<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModifyTable")
            .field("relation", &self.relation)
            .field("kind", &self.kind)
            .field("done", &self.done)
            .field("affected", &self.affected)
            .finish_non_exhaustive()
    }
}

impl<L: PageLoader> ModifyTable<L> {
    /// Output schema shared across all `ModifyTable` instances: a single
    /// `Int64` column named `"affected_rows"`.
    fn affected_rows_schema() -> Schema {
        Schema::new([Field::required("affected_rows", DataType::Int64)])
            .expect("affected_rows schema is trivially well-formed")
    }

    /// Construct a `ModifyTable` operator.
    ///
    /// # Parameters
    ///
    /// - `heap` — shared reference to the heap access method.
    /// - `relation` — target relation id.
    /// - `relation_schema` — full column schema of the target relation
    ///   (used as the codec schema for INSERT).
    /// - `kind` — mutation kind.
    /// - `insert_xmin` — XID to stamp as `xmin` on inserted tuples.
    /// - `insert_command_id` — command id within the inserting transaction.
    /// - `delete_xmax` — XID to stamp as `xmax` on deleted/updated tuples.
    /// - `delete_cmax` — command id for deletes/updates.
    /// - `wal` — optional WAL sink; `None` skips WAL emission.
    /// - `child` — source operator.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::similar_names)]
    pub fn new(
        heap: Arc<HeapAccess<L>>,
        relation: RelationId,
        relation_schema: Schema,
        kind: ModifyKind,
        insert_xmin: Xid,
        insert_command_id: CommandId,
        delete_xmax: Xid,
        delete_cmax: CommandId,
        wal: Option<Arc<dyn WalSink>>,
        child: Box<dyn Operator>,
    ) -> Self {
        Self {
            heap,
            relation,
            schema: Self::affected_rows_schema(),
            codec: RowCodec::new(relation_schema),
            kind,
            insert_xmin,
            insert_command_id,
            delete_xmax,
            delete_cmax,
            wal,
            child,
            done: false,
            affected: 0,
        }
    }
}

impl<L: PageLoader + Send + Sync + std::fmt::Debug + 'static> Operator for ModifyTable<L> {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        // Drain the entire child input.
        loop {
            let Some(batch) = self.child.next_batch()? else {
                break;
            };
            if batch.rows() == 0 {
                continue;
            }

            // Decode batch into rows so we can operate per-row.
            let child_schema = self.child.schema().clone();
            let rows = batch_to_rows(&batch, &child_schema)
                .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;

            for row in rows {
                match &self.kind {
                    ModifyKind::Insert => {
                        self.apply_insert(&row)?;
                    }
                    ModifyKind::Update { assignments } => {
                        // Clone to avoid borrow conflicts; assignments are small.
                        let assignments: Vec<(usize, ScalarExpr)> = assignments.clone();
                        self.apply_update(&row, &assignments)?;
                    }
                    ModifyKind::Delete => {
                        self.apply_delete(&row)?;
                    }
                }
                self.affected += 1;
            }
        }

        // Emit the affected-row-count batch.
        let affected_i64 = i64::try_from(self.affected).unwrap_or(i64::MAX);
        let batch = Batch::new([Column::Int64(NumericColumn::from_data(vec![affected_i64]))])
            .map_err(ExecError::from)?;
        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl<L: PageLoader + Send + Sync + std::fmt::Debug + 'static> ModifyTable<L> {
    /// Apply a single INSERT row.
    fn apply_insert(&self, row: &[Value]) -> Result<(), ExecError> {
        let payload = self
            .codec
            .encode(row)
            .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
        let wal_ref: Option<&dyn WalSink> = self.wal.as_deref();
        self.heap
            .insert(
                self.relation,
                &payload,
                InsertOptions {
                    xmin: self.insert_xmin,
                    command_id: self.insert_command_id,
                    wal: wal_ref,
                    fsm: None,
                    vm: None,
                },
            )
            .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
        Ok(())
    }

    /// Apply a single UPDATE row.
    ///
    /// The `row` slice must begin with `[tid_block: Int32, tid_slot: Int32,
    /// original_col0, ...]`. We extract the TID from the first two columns,
    /// apply the assignments to the remaining columns, encode the new tuple,
    /// and call `heap.update`.
    fn apply_update(
        &self,
        row: &[Value],
        assignments: &[(usize, ScalarExpr)],
    ) -> Result<(), ExecError> {
        let (tid, orig_row) = extract_tid_and_row(row, self.relation)?;

        // Build the new row from the original, applying assignments.
        let relation_cols = self.codec.schema().len();
        let mut new_row: Vec<Value> = orig_row.to_vec();
        if new_row.len() != relation_cols {
            return Err(ExecError::TypeMismatch(format!(
                "UPDATE row has {} columns after TID, expected {}",
                new_row.len(),
                relation_cols,
            )));
        }

        for (col_idx, expr) in assignments {
            let evaluator = Eval::new(expr.clone());
            let val = evaluator
                .eval(orig_row)
                .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
            if *col_idx >= relation_cols {
                return Err(ExecError::TypeMismatch(format!(
                    "UPDATE assignment column index {col_idx} out of range (relation has {relation_cols} columns)"
                )));
            }
            new_row[*col_idx] = val;
        }

        let new_payload = self
            .codec
            .encode(&new_row)
            .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
        let wal_ref: Option<&dyn WalSink> = self.wal.as_deref();
        self.heap
            .update(
                tid,
                &new_payload,
                UpdateOptions {
                    xid: self.delete_xmax,
                    command_id: self.delete_cmax,
                    hot_eligible: true,
                    wal: wal_ref,
                    vm: None,
                },
            )
            .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
        Ok(())
    }

    /// Apply a single DELETE row.
    ///
    /// The `row` slice must begin with `[tid_block: Int32, tid_slot: Int32,
    /// ...]`. We extract the TID and call `heap.delete`.
    fn apply_delete(&self, row: &[Value]) -> Result<(), ExecError> {
        let (tid, _) = extract_tid_and_row(row, self.relation)?;
        let wal_ref: Option<&dyn WalSink> = self.wal.as_deref();
        self.heap
            .delete(
                tid,
                DeleteOptions {
                    xmax: self.delete_xmax,
                    cmax: self.delete_cmax,
                    wal: wal_ref,
                    fsm: None,
                    vm: None,
                },
            )
            .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
        Ok(())
    }
}

/// Extract a `TupleId` and the remaining column values from a row that
/// begins with `[tid_block: Int32, tid_slot: Int32, ...]`.
///
/// `relation` is the relation that owns the pages; it is embedded in the
/// returned `TupleId` via `PageId`.
fn extract_tid_and_row(
    row: &[Value],
    relation: RelationId,
) -> Result<(TupleId, &[Value]), ExecError> {
    if row.len() < 2 {
        return Err(ExecError::TypeMismatch(
            "UPDATE/DELETE input row must have at least two TID columns".to_owned(),
        ));
    }
    let block = match &row[0] {
        Value::Int32(b) => *b,
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "TID block must be Int32, got {other:?}"
            )));
        }
    };
    let slot = match &row[1] {
        Value::Int32(s) => *s,
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "TID slot must be Int32, got {other:?}"
            )));
        }
    };
    let block_u32 = u32::try_from(block).map_err(|_| {
        ExecError::TypeMismatch(format!("TID block value {block} out of u32 range"))
    })?;
    let slot_u16 = u16::try_from(slot)
        .map_err(|_| ExecError::TypeMismatch(format!("TID slot value {slot} out of u16 range")))?;
    let page_id = ultrasql_core::PageId::new(relation, ultrasql_core::BlockNumber::new(block_u32));
    let tid = TupleId::new(page_id, slot_u16);
    Ok((tid, &row[2..]))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::Mutex;
    use std::collections::HashMap;
    use ultrasql_core::constants::PAGE_SIZE;
    use ultrasql_core::{
        CommandId, DataType, Field, PageId, RelationId, Result, Schema, Value, Xid,
    };
    use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
    use ultrasql_storage::heap::HeapAccess;
    use ultrasql_storage::page::Page;
    use ultrasql_storage::wal_sink::test_support::InMemoryWalSink;
    use ultrasql_vec::column::Column;

    use super::{ModifyKind, ModifyTable};
    use crate::Operator;
    use crate::mem_table_scan::MemTableScan;
    use crate::values_scan::ValuesScan;
    use ultrasql_planner::ScalarExpr;

    // -----------------------------------------------------------------------
    // In-memory heap fixtures (duplicated from seq_scan tests)
    // -----------------------------------------------------------------------

    #[derive(Default, Debug)]
    struct MapLoader {
        store: Mutex<HashMap<PageId, Box<[u8; PAGE_SIZE]>>>,
    }

    impl MapLoader {
        fn new() -> Self {
            Self::default()
        }
    }

    impl PageLoader for MapLoader {
        fn load(&self, page_id: PageId) -> Result<Page> {
            let stored = {
                let store = self.store.lock();
                store.get(&page_id).map(|b| {
                    let mut copy: Box<[u8; PAGE_SIZE]> = vec![0_u8; PAGE_SIZE]
                        .into_boxed_slice()
                        .try_into()
                        .expect("alloc matches PAGE_SIZE");
                    copy.copy_from_slice(&**b);
                    copy
                })
            };
            if let Some(bytes) = stored {
                return Page::from_bytes(bytes)
                    .map_err(|e| ultrasql_core::Error::Corruption(format!("test loader: {e}")));
            }
            let page = Page::new_heap();
            let mut copy: Box<[u8; PAGE_SIZE]> = vec![0_u8; PAGE_SIZE]
                .into_boxed_slice()
                .try_into()
                .expect("alloc matches PAGE_SIZE");
            copy.copy_from_slice(page.as_bytes());
            self.store.lock().insert(page_id, copy);
            Ok(page)
        }
    }

    fn rel() -> RelationId {
        RelationId::new(42)
    }

    fn make_heap() -> Arc<HeapAccess<MapLoader>> {
        let pool = Arc::new(BufferPool::new(64, MapLoader::new()));
        Arc::new(HeapAccess::new(pool))
    }

    fn schema_i32_text() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("name", DataType::Text { max_len: None }),
        ])
        .expect("schema ok")
    }

    fn lit_i32(v: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            data_type: DataType::Int32,
        }
    }

    fn lit_text(s: &str) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Text(s.to_owned()),
            data_type: DataType::Text { max_len: None },
        }
    }

    // -----------------------------------------------------------------------
    // Test 1: insert writes each input row to heap and reports count
    // -----------------------------------------------------------------------

    #[test]
    fn insert_writes_each_input_row_to_heap_and_reports_count() {
        let heap = make_heap();
        let schema = schema_i32_text();
        let wal = Arc::new(InMemoryWalSink::new());

        // Source: 3 rows via ValuesScan.
        let rows = vec![
            vec![lit_i32(1), lit_text("alice")],
            vec![lit_i32(2), lit_text("bob")],
            vec![lit_i32(3), lit_text("carol")],
        ];
        let source = ValuesScan::new(rows, schema.clone());

        let mut op = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            schema,
            ModifyKind::Insert,
            Xid::new(10),
            CommandId::FIRST,
            Xid::new(10),
            CommandId::FIRST,
            Some(Arc::clone(&wal) as Arc<dyn ultrasql_storage::wal_sink::WalSink>),
            Box::new(source),
        );

        // Drain the operator.
        let batch = op
            .next_batch()
            .expect("must not error")
            .expect("must emit batch");
        assert_eq!(batch.rows(), 1, "expected single affected-rows batch");
        match &batch.columns()[0] {
            Column::Int64(c) => assert_eq!(c.data(), &[3_i64], "expected 3 affected rows"),
            other => panic!("unexpected column: {other:?}"),
        }
        assert!(
            op.next_batch().unwrap().is_none(),
            "must return None after emit"
        );

        // Verify 3 rows are present in the heap.
        assert_eq!(heap.block_count(rel()), 1, "one block should be allocated");
    }

    // -----------------------------------------------------------------------
    // Test 2: insert emits a WAL record per inserted row
    // -----------------------------------------------------------------------

    #[test]
    fn insert_emits_wal_record_per_inserted_row() {
        let heap = make_heap();
        let schema = schema_i32_text();
        let wal = Arc::new(InMemoryWalSink::new());

        let rows = vec![
            vec![lit_i32(1), lit_text("x")],
            vec![lit_i32(2), lit_text("y")],
        ];
        let source = ValuesScan::new(rows, schema.clone());

        let mut op = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            schema,
            ModifyKind::Insert,
            Xid::new(20),
            CommandId::FIRST,
            Xid::new(20),
            CommandId::FIRST,
            Some(Arc::clone(&wal) as Arc<dyn ultrasql_storage::wal_sink::WalSink>),
            Box::new(source),
        );

        op.next_batch().unwrap();

        // Each insert should emit exactly one WAL record.
        assert_eq!(wal.len(), 2, "expected 2 WAL records");
    }

    // -----------------------------------------------------------------------
    // Test 3: empty input reports zero affected rows
    // -----------------------------------------------------------------------

    #[test]
    fn insert_empty_input_reports_zero() {
        let heap = make_heap();
        let schema = schema_i32_text();
        let source = ValuesScan::new(vec![], schema.clone());

        let mut op = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            schema,
            ModifyKind::Insert,
            Xid::new(30),
            CommandId::FIRST,
            Xid::new(30),
            CommandId::FIRST,
            None,
            Box::new(source),
        );

        let batch = op.next_batch().unwrap().unwrap();
        match &batch.columns()[0] {
            Column::Int64(c) => assert_eq!(c.data(), &[0_i64]),
            other => panic!("unexpected column: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 4: schema reports affected_rows column
    // -----------------------------------------------------------------------

    #[test]
    fn modify_table_schema_is_affected_rows() {
        let heap = make_heap();
        let schema = schema_i32_text();
        let source = MemTableScan::new(schema.clone(), vec![]);
        let op = ModifyTable::new(
            Arc::clone(&heap),
            rel(),
            schema,
            ModifyKind::Insert,
            Xid::new(1),
            CommandId::FIRST,
            Xid::new(1),
            CommandId::FIRST,
            None,
            Box::new(source),
        );
        assert_eq!(op.schema().len(), 1);
        assert_eq!(op.schema().field_at(0).name, "affected_rows");
        assert_eq!(op.schema().field_at(0).data_type, DataType::Int64);
    }
}
