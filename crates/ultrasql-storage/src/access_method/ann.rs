//! Shared ANN value types: distance metric, vector payload, search result.
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::type_complexity)]

use num_traits::ToPrimitive;
use ultrasql_core::TupleId;

use super::AccessMethodError;

// ---------------------------------------------------------------------------
// HNSW vector index
// ---------------------------------------------------------------------------

/// Distance metric attached to an HNSW vector index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HnswMetric {
    /// Euclidean distance, matching pgvector's `<->` operator.
    L2,
    /// Cosine distance, matching pgvector's `<=>` operator.
    Cosine,
    /// Negative inner product, matching pgvector's `<#>` ordering.
    NegativeInnerProduct,
    /// Manhattan distance, matching pgvector's `<+>` operator.
    L1,
}

impl HnswMetric {
    pub(crate) fn distance(self, left: &[f32], right: &[f32]) -> f32 {
        match self {
            Self::L2 => ultrasql_vec::kernels::vector::l2_distance_f32(left, right),
            Self::Cosine => ultrasql_vec::kernels::vector::cosine_distance_f32(left, right)
                .unwrap_or(f32::INFINITY),
            Self::NegativeInnerProduct => -ultrasql_vec::kernels::vector::dot_f32(left, right),
            Self::L1 => left
                .iter()
                .zip(right)
                .map(|(l, r)| (l - r).abs())
                .sum::<f32>(),
        }
    }

    pub(crate) fn vector_metric(self) -> ultrasql_vec::kernels::vector::VectorMetric {
        match self {
            Self::L2 => ultrasql_vec::kernels::vector::VectorMetric::L2,
            Self::Cosine => ultrasql_vec::kernels::vector::VectorMetric::Cosine,
            Self::NegativeInnerProduct => {
                ultrasql_vec::kernels::vector::VectorMetric::NegativeInnerProduct
            }
            Self::L1 => ultrasql_vec::kernels::vector::VectorMetric::L1,
        }
    }
}

/// Physical payload family stored by page-backed ANN indexes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnnPayloadKind {
    /// Store single-precision values directly.
    F32,
    /// Store a bfloat16 payload beside exact f32 rerank values.
    Bf16,
    /// Store symmetric int8 quantized payload beside exact f32 rerank values.
    Int8,
}

/// Final rerank policy for quantized ANN candidates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnnRerankPolicy {
    /// Candidate recall may use a quantized payload; final ordering uses exact
    /// f32 values preserved by the index entry.
    ExactF32,
}

/// ANN entry payload with optional quantized storage and exact f32 rerank data.
#[derive(Clone, Debug, PartialEq)]
pub struct AnnVectorPayload {
    pub(crate) kind: AnnPayloadKind,
    pub(crate) exact_f32: Vec<f32>,
    pub(crate) quantized: AnnQuantizedPayload,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum AnnQuantizedPayload {
    F32(Vec<f32>),
    Bf16(Vec<u16>),
    Int8 { scale: f32, values: Vec<i8> },
}

impl AnnVectorPayload {
    /// Build an ANN payload, preserving exact f32 values for final rerank.
    pub fn new(kind: AnnPayloadKind, vector: &[f32]) -> Result<Self, AccessMethodError> {
        if vector.is_empty() {
            return Err(AccessMethodError::Storage(
                "ANN payload vector must be non-empty".to_owned(),
            ));
        }
        if vector.iter().any(|value| !value.is_finite()) {
            return Err(AccessMethodError::Storage(
                "ANN payload vector elements must be finite".to_owned(),
            ));
        }
        let exact_f32 = vector.to_vec();
        let quantized = match kind {
            AnnPayloadKind::F32 => AnnQuantizedPayload::F32(exact_f32.clone()),
            AnnPayloadKind::Bf16 => {
                let values = vector
                    .iter()
                    .map(|value| {
                        u16::try_from(value.to_bits() >> 16).map_err(|_| {
                            AccessMethodError::Storage(
                                "ANN bf16 payload conversion overflow".to_owned(),
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                AnnQuantizedPayload::Bf16(values)
            }
            AnnPayloadKind::Int8 => {
                let max_abs = vector
                    .iter()
                    .map(|value| value.abs())
                    .fold(0.0_f32, f32::max);
                let scale = if max_abs <= f32::EPSILON {
                    1.0
                } else {
                    max_abs / 127.0
                };
                let values = vector
                    .iter()
                    .map(|value| {
                        let quantized = (*value / scale).round().clamp(-127.0, 127.0);
                        quantized.to_i8().ok_or_else(|| {
                            AccessMethodError::Storage(
                                "ANN int8 payload conversion overflow".to_owned(),
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                AnnQuantizedPayload::Int8 { scale, values }
            }
        };
        Ok(Self {
            kind,
            exact_f32,
            quantized,
        })
    }

    /// Return the storage payload family.
    #[must_use]
    pub const fn kind(&self) -> AnnPayloadKind {
        self.kind
    }

    /// Return the candidate rerank policy.
    #[must_use]
    pub const fn rerank_policy(&self) -> AnnRerankPolicy {
        AnnRerankPolicy::ExactF32
    }

    /// Return exact f32 values used for final rerank.
    #[must_use]
    pub fn exact_f32(&self) -> &[f32] {
        &self.exact_f32
    }

    /// Return quantized payload byte length excluding exact rerank values.
    #[must_use]
    pub fn quantized_len_bytes(&self) -> usize {
        match &self.quantized {
            AnnQuantizedPayload::F32(values) => values.len() * std::mem::size_of::<f32>(),
            AnnQuantizedPayload::Bf16(values) => values.len() * std::mem::size_of::<u16>(),
            AnnQuantizedPayload::Int8 { scale, values } => {
                let _ = scale;
                values.len()
            }
        }
    }
}

/// One result from an HNSW search.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct HnswSearchResult {
    /// Heap tuple identifier stored in the index node.
    pub tid: TupleId,
    /// Distance from the search probe under the index metric.
    pub distance: f32,
}
