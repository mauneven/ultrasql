//! Secondary-index maintainer builders (B-tree and vector) and the
//! TID-emitting sequential scan used to drive UPDATE / DELETE.

use std::sync::Arc;

use ultrasql_catalog::{IndexEntry, TableEntry};
use ultrasql_core::{BlockNumber, RelationId, Value};
use ultrasql_executor::{
    Eval, InsertIndexEncoder, InsertIndexMaintainer, Operator, RowCodec, SeqScan,
    VectorIndexEncoder, VectorIndexMaintainer, eval_error_to_exec_error,
};
use ultrasql_planner::LogicalIndexMethod;
use ultrasql_storage::btree::BTree;

use crate::error::ServerError;
use crate::pipeline::LowerCtx;
use crate::pipeline::modify::IndexMaintainerDeps;

pub(super) fn build_insert_index_maintainers(
    entry: &TableEntry,
    ctx: &LowerCtx<'_>,
) -> Result<Vec<InsertIndexMaintainer<crate::BlankPageLoader>>, ServerError> {
    build_insert_index_maintainers_from_deps(entry, IndexMaintainerDeps::from_lower_ctx(ctx))
}

/// Build the B-tree/hash insert index maintainers from explicit dependencies.
///
/// This is the reuse seam shared by the INSERT-lowering path (which passes
/// [`IndexMaintainerDeps::from_lower_ctx`]) and `COPY FROM` (which assembles
/// the same dependencies straight off the [`Session`](crate::Session) and the
/// governing transaction). Keeping the body here means COPY maintains indexes
/// with the identical encoder/partial-predicate/BRIN logic as INSERT.
pub(crate) fn build_insert_index_maintainers_from_deps(
    entry: &TableEntry,
    deps: IndexMaintainerDeps<'_>,
) -> Result<Vec<InsertIndexMaintainer<crate::BlankPageLoader>>, ServerError> {
    let Some(indexes) = deps.catalog_snapshot.indexes_by_table.get(&entry.oid) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(indexes.len());
    for index in indexes {
        if let Some(maintainer) = build_one_insert_index_maintainer(entry, index, &deps)? {
            out.push(maintainer);
        }
    }
    Ok(out)
}

fn build_one_insert_index_maintainer(
    entry: &TableEntry,
    index: &IndexEntry,
    deps: &IndexMaintainerDeps<'_>,
) -> Result<Option<InsertIndexMaintainer<crate::BlankPageLoader>>, ServerError> {
    let columns: Vec<usize> = index
        .columns
        .iter()
        .map(|attnum| usize::from(*attnum))
        .collect();
    let key_columns = columns.clone();
    let runtime = deps
        .table_constraints
        .get(&entry.oid)
        .and_then(|constraints| constraints.indexes.get(&index.oid).cloned());
    let key_exprs = runtime
        .as_ref()
        .map(|metadata| metadata.key_exprs.clone())
        .unwrap_or_default();
    let predicate = runtime
        .as_ref()
        .and_then(|metadata| metadata.predicate.clone());
    let method = runtime
        .as_ref()
        .map_or(LogicalIndexMethod::Btree, |metadata| metadata.method);
    if matches!(
        method,
        LogicalIndexMethod::Hnsw | LogicalIndexMethod::IvfFlat
    ) {
        return Ok(None);
    }
    if index.root_block == BlockNumber::INVALID {
        return Ok(None);
    }
    let brin = runtime.as_ref().and_then(|metadata| metadata.brin.clone());
    let encoding = if method == LogicalIndexMethod::Hash {
        crate::index_key::IndexKeyEncoding::Int64
    } else if key_exprs.is_empty() {
        crate::index_key::IndexKeyEncoding::for_columns(&entry.schema, &columns)?
    } else {
        let [expr] = key_exprs.as_slice() else {
            return Err(ServerError::Unsupported(
                "CREATE INDEX: expression indexes support exactly one key in this wave",
            ));
        };
        crate::index_key::IndexKeyEncoding::for_data_type(&expr.data_type())?
    };
    let index_rel = RelationId::new(index.oid.raw());
    let tree = BTree::open(
        Arc::clone(deps.heap.buffer_pool()),
        index_rel,
        index.root_block,
    );
    let index_name = index.name.clone();
    let encoder: InsertIndexEncoder = Arc::new(move |row: &[Value]| {
        if let Some(predicate) = &predicate {
            match Eval::new(predicate.clone())
                .eval(row)
                .map_err(eval_error_to_exec_error)?
            {
                Value::Bool(true) => {}
                Value::Bool(false) | Value::Null => return Ok(None),
                other => {
                    return Err(ultrasql_executor::ExecError::TypeMismatch(format!(
                        "index {index_name} partial predicate returned {:?}, expected bool",
                        other.data_type()
                    )));
                }
            }
        }
        if !key_exprs.is_empty() {
            let value = Eval::new(key_exprs[0].clone())
                .eval(row)
                .map_err(eval_error_to_exec_error)?;
            if method == LogicalIndexMethod::Hash {
                return Ok(crate::hash_index_value(&value));
            }
            return encoding.encode_value(&value).map_err(|e| {
                ultrasql_executor::ExecError::TypeMismatch(format!("index {index_name}: {e}"))
            });
        }
        let encoded = match columns.as_slice() {
            [col] => {
                let value = row.get(*col).ok_or_else(|| {
                    ultrasql_executor::ExecError::TypeMismatch(format!(
                        "index {index_name}: row missing key column {col}"
                    ))
                })?;
                if method == LogicalIndexMethod::Hash {
                    return Ok(crate::hash_index_value(value));
                }
                encoding.encode_value(value).map_err(|e| {
                    ultrasql_executor::ExecError::TypeMismatch(format!("index {index_name}: {e}"))
                })?
            }
            _ => encoding.encode_row(row).map_err(|e| {
                ultrasql_executor::ExecError::TypeMismatch(format!("index {index_name}: {e}"))
            })?,
        };
        Ok(encoded)
    });
    Ok(Some(
        InsertIndexMaintainer::new(index.name.clone(), tree, encoder, index.is_unique)
            .with_key_columns(key_columns)
            .with_brin(brin),
    ))
}

pub(super) fn build_vector_index_maintainers(
    entry: &TableEntry,
    ctx: &LowerCtx<'_>,
) -> Result<Vec<VectorIndexMaintainer>, ServerError> {
    build_vector_index_maintainers_from_deps(entry, IndexMaintainerDeps::from_lower_ctx(ctx))
}

/// Build the HNSW/IVFFlat vector index maintainers from explicit dependencies.
///
/// The COPY-maintenance reuse seam for vector (ANN) indexes; mirrors
/// [`build_insert_index_maintainers_from_deps`].
pub(crate) fn build_vector_index_maintainers_from_deps(
    entry: &TableEntry,
    deps: IndexMaintainerDeps<'_>,
) -> Result<Vec<VectorIndexMaintainer>, ServerError> {
    let Some(indexes) = deps.catalog_snapshot.indexes_by_table.get(&entry.oid) else {
        return Ok(Vec::new());
    };
    let Some(constraints) = deps.table_constraints.get(&entry.oid) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for index in indexes {
        let Some(metadata) = constraints.indexes.get(&index.oid) else {
            continue;
        };
        if !matches!(
            metadata.method,
            LogicalIndexMethod::Hnsw | LogicalIndexMethod::IvfFlat
        ) {
            continue;
        };
        let [attnum] = index.columns.as_slice() else {
            return Err(ServerError::Unsupported(
                "CREATE INDEX USING vector ANN: exactly one vector column key is supported",
            ));
        };
        let col = usize::from(*attnum);
        let index_name = index.name.clone();
        let encoder: VectorIndexEncoder = Arc::new(move |row: &[Value]| {
            let value = row.get(col).ok_or_else(|| {
                ultrasql_executor::ExecError::TypeMismatch(format!(
                    "vector index {index_name}: row missing key column {col}"
                ))
            })?;
            match value {
                Value::Vector(vector) | Value::HalfVec(vector) => Ok(Some(vector.clone())),
                Value::Null => Ok(None),
                other => Err(ultrasql_executor::ExecError::TypeMismatch(format!(
                    "vector index {index_name}: expected vector or halfvec key, got {:?}",
                    other.data_type()
                ))),
            }
        });
        match metadata.method {
            LogicalIndexMethod::Hnsw => {
                let Some(hnsw) = metadata.hnsw.clone() else {
                    continue;
                };
                out.push(VectorIndexMaintainer::new_hnsw(
                    index.name.clone(),
                    hnsw,
                    encoder,
                    deps.xid,
                    deps.heap.wal_sink().cloned(),
                ));
            }
            LogicalIndexMethod::IvfFlat => {
                let Some(ivfflat) = metadata.ivfflat.clone() else {
                    continue;
                };
                out.push(VectorIndexMaintainer::new_ivfflat(
                    index.name.clone(),
                    ivfflat,
                    encoder,
                    deps.xid,
                    deps.heap.wal_sink().cloned(),
                ));
            }
            _ => {}
        }
    }
    Ok(out)
}

/// Build a TID-emitting [`SeqScan`] over a persistent relation.
///
/// The resulting operator emits rows shaped
/// `[tid_block: Int32, tid_slot: Int32, ...payload_cols]`, which is the
/// contract [`ModifyTable`] expects for UPDATE and DELETE.
pub(super) fn build_tid_seq_scan(entry: &TableEntry, ctx: &LowerCtx<'_>) -> Box<dyn Operator> {
    let rel = RelationId(entry.oid);
    let block_count = ctx.heap.block_count(rel).max(entry.n_blocks);
    let codec = RowCodec::new(entry.schema.clone());
    let scan = SeqScan::new_with_tids_and_vm(
        Arc::clone(&ctx.heap),
        rel,
        block_count,
        ctx.snapshot.clone(),
        Arc::clone(&ctx.oracle),
        Arc::clone(&ctx.vm),
        codec,
    );
    Box::new(scan)
}
