//! Unit tests for the access-method backends.

use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId};

use super::*;
use super::hnsw_page::{
    HNSW_PAGE_KIND_NODE, HNSW_SNAPSHOT_VERSION, HnswLevelNeighbors, HnswNodePage,
    HnswPersistentPage, SnapshotCursor, decode_hnsw_page_record, encode_hnsw_page_record, push_len,
    push_opt_block, push_tuple_id,
};
use super::ivfflat::{nearest_vector, nearest_vectors};

mod btree;
mod brin;
mod gin;
mod gist;
mod hash;
mod hnsw;
mod hnsw_page;
mod ivfflat;

fn tid(block: u32, slot: u16) -> TupleId {
    TupleId::new(
        PageId::new(RelationId::new(99), BlockNumber::new(block)),
        slot,
    )
}
