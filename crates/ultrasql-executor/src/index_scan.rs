//! B-tree index scan operator.
//!
//! [`IndexScan`] performs point lookups and range scans over a B-tree index
//! backed by `ultrasql-storage`. For v0.5 the operator accepts a
//! pre-computed list of row payloads (as returned by the B-tree probe API)
//! and decodes them via [`RowCodec`], emitting 4096-row batches.
//!
//! # Design decision: injected rows
//!
//! At v0.5 the executor does not yet own a direct handle to
//! `ultrasql-storage`'s B-tree; that wiring belongs in the physical-plan
//! lowering layer that arrives in v0.6. Until then, the caller is
//! responsible for probing the B-tree and passing the resulting byte
//! payloads to `IndexScan::new`. This is the same pattern used by
//! `SeqScan` in v0.3 (injecting row iterators rather than holding a live
//! heap handle).
//!
//! # Point lookup vs range scan
//!
//! Both modes are represented by the same `Vec<Vec<u8>>` payload list:
//! a point lookup passes a single-element list, a range scan passes the
//! full range. The operator does not distinguish them at the API level.

use ultrasql_core::{Schema, Value};
use ultrasql_vec::Batch;

use crate::row_codec::RowCodec;
use crate::seq_scan::build_batch;
use crate::{ExecError, Operator};

const BATCH_TARGET_ROWS: usize = 4096;

/// B-tree index scan operator.
///
/// Decodes a pre-probed list of byte payloads and emits them as 4096-row
/// batches. The payload list is typically the output of a B-tree range
/// probe or point lookup.
///
/// # Send
///
/// `Vec<Vec<u8>>`, `RowCodec`, and `Schema` are all `Send`.
#[derive(Debug)]
pub struct IndexScan {
    /// Decoded rows, built lazily on first call.
    payloads: std::vec::IntoIter<Vec<u8>>,
    codec: RowCodec,
    eof: bool,
    /// Decoded row buffer filled in chunks of 4096.
    #[allow(dead_code)]
    pending: Vec<Vec<Value>>,
}

impl IndexScan {
    /// Construct an index scan over the given raw tuple payloads.
    ///
    /// - `payloads` — raw byte payloads as returned by the storage B-tree
    ///   probe. Each element is the `data` field of a `HeapTuple`.
    /// - `codec` — row codec bound to the relation schema.
    #[must_use]
    pub fn new(payloads: Vec<Vec<u8>>, codec: RowCodec) -> Self {
        Self {
            payloads: payloads.into_iter(),
            codec,
            eof: false,
            pending: Vec::new(),
        }
    }
}

impl Operator for IndexScan {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }

        // Decode up to BATCH_TARGET_ROWS payloads.
        let mut chunk: Vec<Vec<Value>> = Vec::with_capacity(BATCH_TARGET_ROWS);
        loop {
            if chunk.len() >= BATCH_TARGET_ROWS {
                break;
            }
            let Some(payload) = self.payloads.next() else {
                self.eof = true;
                break;
            };
            let row = self
                .codec
                .decode(&payload)
                .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
            chunk.push(row);
        }

        if chunk.is_empty() {
            return Ok(None);
        }

        build_batch(&chunk, self.codec.schema()).map(Some)
    }

    fn schema(&self) -> &Schema {
        self.codec.schema()
    }
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};

    use super::IndexScan;
    use crate::Operator;
    use crate::row_codec::RowCodec;

    fn schema_i32() -> Schema {
        Schema::new([Field::required("id", DataType::Int32)]).expect("ok")
    }

    fn encode_i32(v: i32) -> Vec<u8> {
        let codec = RowCodec::new(schema_i32());
        codec.encode(&[Value::Int32(v)]).expect("encode ok")
    }

    fn drain_i32(op: &mut dyn Operator) -> Vec<i32> {
        let schema = op.schema().clone();
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().expect("ok") {
            let rows = crate::filter_op::batch_to_rows(&b, &schema).expect("decode");
            for row in rows {
                if let Value::Int32(v) = &row[0] {
                    out.push(*v);
                }
            }
        }
        out
    }

    #[test]
    fn index_scan_point_lookup_returns_one_row() {
        let payloads = vec![encode_i32(42)];
        let codec = RowCodec::new(schema_i32());
        let mut scan = IndexScan::new(payloads, codec);
        let vals = drain_i32(&mut scan);
        assert_eq!(vals, vec![42]);
    }

    #[test]
    fn index_scan_range_returns_all_rows_in_order() {
        let payloads: Vec<Vec<u8>> = (1_i32..=5).map(encode_i32).collect();
        let codec = RowCodec::new(schema_i32());
        let mut scan = IndexScan::new(payloads, codec);
        let vals = drain_i32(&mut scan);
        assert_eq!(vals, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn index_scan_empty_payloads_returns_none() {
        let codec = RowCodec::new(schema_i32());
        let mut scan = IndexScan::new(vec![], codec);
        assert!(scan.next_batch().expect("ok").is_none());
    }

    #[test]
    fn index_scan_chunks_into_4096_row_batches() {
        let payloads: Vec<Vec<u8>> = (0_i32..4100).map(encode_i32).collect();
        let codec = RowCodec::new(schema_i32());
        let mut scan = IndexScan::new(payloads, codec);
        let mut sizes: Vec<usize> = Vec::new();
        while let Some(b) = scan.next_batch().expect("ok") {
            sizes.push(b.rows());
        }
        assert_eq!(sizes.iter().sum::<usize>(), 4100);
        assert!(sizes.contains(&4096));
    }
}
