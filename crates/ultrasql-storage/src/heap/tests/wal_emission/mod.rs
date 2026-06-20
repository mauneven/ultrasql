//! Deliverable C: heap WAL emission, LSN stamping, and vacuum_heap tests.

use std::sync::Arc;

use ultrasql_core::RelationId;

use super::MapLoader;
use crate::buffer_pool::BufferPool;
use crate::heap::HeapAccess;
use crate::wal_sink::test_support::InMemoryWalSink;

mod chaining;
mod inplace;
mod lsn_stamping;
mod records;
mod vacuum;

fn make_heap_with_sink(capacity: usize) -> (HeapAccess<MapLoader>, Arc<InMemoryWalSink>) {
    let pool = Arc::new(BufferPool::new(capacity, MapLoader::new()));
    let heap = HeapAccess::new(pool);
    let sink = Arc::new(InMemoryWalSink::new());
    (heap, sink)
}

fn rel() -> RelationId {
    RelationId::new(99)
}
