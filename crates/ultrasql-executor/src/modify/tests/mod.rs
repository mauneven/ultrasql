//! Unit tests for the `ModifyTable` operator and its helpers.
//!
//! Shared fixtures (the in-memory `MapLoader`, schema/expression builders,
//! and heap/index constructors) live here; the per-topic `#[test]`
//! functions are split into sibling submodules that pull these in via
//! `use super::*`.

pub(super) use std::collections::{HashMap, HashSet};
pub(super) use std::sync::Arc;

pub(super) use parking_lot::Mutex;
pub(super) use ultrasql_core::constants::PAGE_SIZE;
pub(super) use ultrasql_core::{
    BlockNumber, CommandId, DataType, Field, PageId, RelationId, Result, Schema, TupleId, Value,
    Xid,
};
pub(super) use ultrasql_mvcc::{InfoMask, TupleHeader};
pub(super) use ultrasql_storage::btree::BTree;
pub(super) use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
pub(super) use ultrasql_storage::heap::{HeapAccess, InsertOptions, UpdateOutcome};
pub(super) use ultrasql_storage::page::Page;
pub(super) use ultrasql_storage::sequence::SequenceOptions;
pub(super) use ultrasql_storage::vm::VisibilityMap;
pub(super) use ultrasql_storage::wal_sink::test_support::InMemoryWalSink;
pub(super) use ultrasql_vec::Batch;
pub(super) use ultrasql_vec::column::{Column, NumericColumn};

pub(super) use super::helpers::{
    build_update_edits_int32_pair, check_not_null_violations, columns_match_unordered,
    conflict_target_columns, detect_update_int32_pair_fast_path, expand_insert_row,
    extract_tid_and_row, extract_tids_from_batch, row_codec_error_to_exec, updated_ctid_target,
};
pub(super) use super::{
    DeleteIndexChange, InsertConflict, InsertConflictAction, InsertIndexMaintainer, ModifyKind,
    ModifyTable, ModifyTableStamps, SequenceDefault, UpdateFastPathInt32Pair, UpdateIndexChange,
};
pub(super) use crate::eval::Eval;
pub(super) use crate::mem_table_scan::MemTableScan;
pub(super) use crate::row_codec::{RowCodec, RowCodecError};
pub(super) use crate::values_scan::ValuesScan;
pub(super) use crate::{ExecError, Operator};
pub(super) use ultrasql_planner::{BinaryOp, ScalarExpr};

mod conflict_index_paths;
mod constraints_returning;
mod fast_path_helpers;
mod operator_basic;

pub(super) type RowCheck =
    Arc<dyn Fn(&[Value]) -> std::result::Result<(), ExecError> + Send + Sync>;
pub(super) type UpdateCheck =
    Arc<dyn Fn(&[Value], &[Value]) -> std::result::Result<(), ExecError> + Send + Sync>;

// -----------------------------------------------------------------------
// In-memory heap fixtures (duplicated from seq_scan tests)
// -----------------------------------------------------------------------

#[derive(Default, Debug)]
pub(super) struct MapLoader {
    store: Mutex<HashMap<PageId, Box<[u8; PAGE_SIZE]>>>,
}

impl MapLoader {
    pub(super) fn new() -> Self {
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

pub(super) fn rel() -> RelationId {
    RelationId::new(42)
}

pub(super) fn make_heap() -> Arc<HeapAccess<MapLoader>> {
    let pool = Arc::new(BufferPool::new(64, MapLoader::new()));
    Arc::new(HeapAccess::new(pool))
}

pub(super) fn schema_i32_text() -> Schema {
    Schema::new([
        Field::required("id", DataType::Int32),
        Field::required("name", DataType::Text { max_len: None }),
    ])
    .expect("schema ok")
}

pub(super) fn lit_i32(v: i32) -> ScalarExpr {
    ScalarExpr::Literal {
        value: Value::Int32(v),
        data_type: DataType::Int32,
    }
}

pub(super) fn lit_text(s: &str) -> ScalarExpr {
    ScalarExpr::Literal {
        value: Value::Text(s.to_owned()),
        data_type: DataType::Text { max_len: None },
    }
}

pub(super) fn lit_bool(v: bool) -> ScalarExpr {
    ScalarExpr::Literal {
        value: Value::Bool(v),
        data_type: DataType::Bool,
    }
}

pub(super) fn schema_i32_pair() -> Schema {
    Schema::new([
        Field::required("id", DataType::Int32),
        Field::required("val", DataType::Int32),
    ])
    .expect("schema ok")
}

pub(super) fn col_i32(name: &str, index: usize) -> ScalarExpr {
    ScalarExpr::Column {
        name: name.to_owned(),
        index,
        data_type: DataType::Int32,
    }
}

pub(super) fn col_text(name: &str, index: usize) -> ScalarExpr {
    ScalarExpr::Column {
        name: name.to_owned(),
        index,
        data_type: DataType::Text { max_len: None },
    }
}

pub(super) fn binary_i32(op: BinaryOp, left: ScalarExpr, right: ScalarExpr) -> ScalarExpr {
    ScalarExpr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
        data_type: DataType::Int32,
    }
}

pub(super) fn tid(block: u32, slot: u16) -> TupleId {
    TupleId::new(PageId::new(rel(), BlockNumber::new(block)), slot)
}

pub(super) fn stamps(xid: u64) -> ModifyTableStamps {
    ModifyTableStamps::new(
        Xid::new(xid),
        CommandId::FIRST,
        Xid::new(xid),
        CommandId::FIRST,
    )
}

pub(super) fn tid_row_schema(relation_schema: &Schema) -> Schema {
    let mut fields = vec![
        Field::required("tid_block", DataType::Int32),
        Field::required("tid_slot", DataType::Int32),
    ];
    fields.extend(relation_schema.fields().iter().cloned());
    Schema::new(fields).expect("tid schema")
}

pub(super) fn insert_payload(
    heap: &HeapAccess<MapLoader>,
    schema: &Schema,
    row: &[Value],
) -> TupleId {
    let codec = RowCodec::new(schema.clone());
    let payload = codec.encode(row).expect("payload");
    let tids = heap
        .insert_batch(
            rel(),
            &[payload.as_slice()],
            InsertOptions {
                xmin: Xid::new(1),
                command_id: CommandId::FIRST,
                n_atts: u16::try_from(schema.len()).expect("test schema fits u16"),
                wal: None,
                fsm: None,
                vm: None,
            },
        )
        .expect("insert row");
    tids[0]
}

pub(super) fn btree_index(name: &str, unique: bool) -> InsertIndexMaintainer<MapLoader> {
    let pool = Arc::new(BufferPool::new(64, MapLoader::new()));
    let tree = BTree::create(pool, RelationId::new(100)).expect("btree");
    InsertIndexMaintainer::new(
        name,
        tree,
        Arc::new(|row| match row.first() {
            Some(Value::Int32(v)) => Ok(Some(i64::from(*v))),
            Some(Value::Int64(v)) => Ok(Some(*v)),
            Some(Value::Null) => Ok(None),
            other => Err(ExecError::TypeMismatch(format!("bad key {other:?}"))),
        }),
        unique,
    )
    .with_key_columns(vec![0])
}
