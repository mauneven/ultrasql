//! Hybrid search ranker.
//!
//! [`HybridSearch`] is a physical executor node for retrieval queries
//! that need lexical, vector, metadata, freshness, version, and normal
//! SQL predicates in one ranking step. Its child supplies candidate rows
//! from any source: a sequential scan, an exact vector scan, a full-text
//! index probe, or an ANN index probe. This node owns final filtering,
//! scoring, tie-breaking, and top-k emission.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use num_traits::ToPrimitive;
use ultrasql_core::{Schema, Value};
use ultrasql_planner::ScalarExpr;
use ultrasql_vec::Batch;
use ultrasql_vec::kernels::vector::{VectorMetric, cosine_distance_f32, dot_f32, l2_distance_f32};

use crate::eval::Eval;
use crate::filter_op::batch_to_rows;
use crate::seq_scan::build_batch;
use crate::{ExecError, Operator, eval_error_to_exec_error};

const BATCH_TARGET_ROWS: usize = 4096;
const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;

/// Physical hybrid retrieval operator.
///
/// The operator drains its child once, applies optional row predicates,
/// computes component scores, keeps the highest-scoring rows, then emits
/// result batches preserving the child's schema. It is exact with
/// respect to the rows its child produces; ANN or full-text access
/// methods can sit below it as candidate producers without changing the
/// ranking contract.
#[derive(Debug)]
pub struct HybridSearch {
    child: Box<dyn Operator>,
    schema: Schema,
    config: HybridSearchConfig,
    metadata_filter: Option<Eval>,
    where_predicate: Option<Eval>,
    sorted: Option<std::vec::IntoIter<Vec<Value>>>,
    eof: bool,
}

/// Score-fusion method used by [`HybridSearch`] to combine its component
/// signals into one ranking.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FusionMethod {
    /// Weighted linear sum of normalized component scores:
    /// `Σ weightₘ · scoreₘ`. BM25 is used as-is; vector similarity, recency,
    /// and version are min/max normalized across the candidate set.
    WeightedLinear,
    /// Reciprocal Rank Fusion. Each component independently ranks the
    /// candidates (best = rank 1); the fused score is
    /// `Σ weightₘ / (k + rankₘ)`. A candidate absent from a component's
    /// ranking (e.g. no lexical match) contributes nothing for that
    /// component. RRF is robust to incomparable score scales, which is why
    /// it is the documented default for the SQL `ai_search` surface.
    Rrf {
        /// Rank-damping constant (Cormack et al. use 60). Larger `k`
        /// flattens the contribution of top ranks.
        k: f64,
    },
}

impl FusionMethod {
    /// The conventional RRF damping constant from the original paper.
    pub const RRF_DEFAULT_K: f64 = 60.0;

    /// RRF with the conventional damping constant.
    #[must_use]
    pub const fn rrf() -> Self {
        Self::Rrf {
            k: Self::RRF_DEFAULT_K,
        }
    }
}

/// Configuration for [`HybridSearch`].
#[derive(Clone, Debug)]
pub struct HybridSearchConfig {
    /// Optional BM25-like text scoring input.
    pub text: Option<HybridTextSpec>,
    /// Optional dense vector scoring input.
    pub vector: Option<HybridVectorSpec>,
    /// Optional metadata predicate, usually a JSONB expression such as
    /// `metadata @> '{"tenant":"acme"}'`.
    pub metadata_filter: Option<ScalarExpr>,
    /// Optional normal SQL `WHERE` predicate evaluated before scoring.
    pub where_predicate: Option<ScalarExpr>,
    /// Optional column whose larger numeric/timestamp value means newer.
    pub recency_column: Option<usize>,
    /// Optional column whose larger numeric value means preferred version.
    pub version_column: Option<usize>,
    /// Maximum number of rows emitted.
    pub limit: usize,
    /// Component weights used for the final score.
    pub weights: HybridSearchWeights,
    /// How component scores are fused into the final ranking.
    pub fusion: FusionMethod,
}

impl HybridSearchConfig {
    /// Build a config with only `limit` set.
    #[must_use]
    pub const fn with_limit(limit: usize) -> Self {
        Self {
            text: None,
            vector: None,
            metadata_filter: None,
            where_predicate: None,
            recency_column: None,
            version_column: None,
            limit,
            weights: HybridSearchWeights::DEFAULT,
            fusion: FusionMethod::WeightedLinear,
        }
    }
}

/// Text component for [`HybridSearch`].
#[derive(Clone, Debug)]
pub struct HybridTextSpec {
    /// 0-based text/tsvector column index in the child schema.
    pub column: usize,
    /// User query text. Tokenized with the same simple ASCII word
    /// splitter as the ranker.
    pub query: String,
}

/// Dense vector component for [`HybridSearch`].
#[derive(Clone, Debug)]
pub struct HybridVectorSpec {
    /// 0-based vector column index in the child schema.
    pub column: usize,
    /// Query embedding.
    pub probe: Vec<f32>,
    /// Distance metric used to compare row vectors against `probe`.
    pub metric: VectorMetric,
}

/// Weighted score components for [`HybridSearch`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct HybridSearchWeights {
    /// BM25-like lexical score multiplier.
    pub bm25: f64,
    /// Dense vector similarity multiplier.
    pub vector: f64,
    /// Normalized recency multiplier.
    pub recency: f64,
    /// Normalized version multiplier.
    pub version: f64,
}

impl HybridSearchWeights {
    /// Default weights: text and vector on, freshness/version off.
    pub const DEFAULT: Self = Self {
        bm25: 1.0,
        vector: 1.0,
        recency: 0.0,
        version: 0.0,
    };
}

impl Default for HybridSearchWeights {
    fn default() -> Self {
        Self::DEFAULT
    }
}

#[derive(Debug)]
struct Candidate {
    row: Vec<Value>,
    ordinal: usize,
    text_terms: Vec<String>,
    vector_similarity: Option<f64>,
    recency: Option<f64>,
    version: Option<f64>,
}

#[derive(Debug)]
struct ScoredRow {
    row: Vec<Value>,
    ordinal: usize,
    score: f64,
}

impl HybridSearch {
    /// Construct a hybrid search node.
    ///
    /// `schema` is the exact output schema. It must match the child row
    /// payload; mismatches surface when batches are decoded or encoded.
    #[must_use]
    pub fn new(child: Box<dyn Operator>, schema: Schema, config: HybridSearchConfig) -> Self {
        let metadata_filter = config.metadata_filter.clone().map(Eval::new);
        let where_predicate = config.where_predicate.clone().map(Eval::new);
        Self {
            child,
            schema,
            config,
            metadata_filter,
            where_predicate,
            sorted: None,
            eof: false,
        }
    }

    fn collect_candidates(&mut self) -> Result<Vec<Candidate>, ExecError> {
        let mut candidates = Vec::new();
        let mut ordinal = 0usize;
        while let Some(batch) = self.child.next_batch()? {
            for row in batch_to_rows(&batch, &self.schema)? {
                let row_ordinal = ordinal;
                ordinal = ordinal.saturating_add(1);
                if !passes_filter(&self.metadata_filter, &row, "metadata filter")? {
                    continue;
                }
                if !passes_filter(&self.where_predicate, &row, "WHERE predicate")? {
                    continue;
                }
                let text_terms = self.text_terms(&row)?;
                let vector_similarity = self.vector_similarity(&row)?;
                let recency = self.numeric_column(&row, self.config.recency_column, "recency")?;
                let version = self.numeric_column(&row, self.config.version_column, "version")?;
                candidates.push(Candidate {
                    row,
                    ordinal: row_ordinal,
                    text_terms,
                    vector_similarity,
                    recency,
                    version,
                });
            }
        }
        Ok(candidates)
    }

    fn text_terms(&self, row: &[Value]) -> Result<Vec<String>, ExecError> {
        let Some(spec) = self.config.text.as_ref() else {
            return Ok(Vec::new());
        };
        match row.get(spec.column) {
            Some(Value::Text(text) | Value::Json(text) | Value::Jsonb(text)) => Ok(tokenize(text)),
            Some(Value::Null) => Ok(Vec::new()),
            Some(other) => Err(ExecError::TypeMismatch(format!(
                "hybrid search text column must be Text, Jsonb, or Null, got {:?}",
                other.data_type()
            ))),
            None => Err(ExecError::TypeMismatch(format!(
                "hybrid search text column index {} out of range",
                spec.column
            ))),
        }
    }

    fn vector_similarity(&self, row: &[Value]) -> Result<Option<f64>, ExecError> {
        let Some(spec) = self.config.vector.as_ref() else {
            return Ok(None);
        };
        match row.get(spec.column) {
            Some(Value::Vector(values) | Value::HalfVec(values)) => {
                let distance = dense_vector_distance(values, &spec.probe, spec.metric)?;
                Ok(Some(distance_to_similarity(distance, spec.metric)))
            }
            Some(Value::Null) => Ok(None),
            Some(other) => Err(ExecError::TypeMismatch(format!(
                "hybrid search vector column must be vector, halfvec, or Null, got {:?}",
                other.data_type()
            ))),
            None => Err(ExecError::TypeMismatch(format!(
                "hybrid search vector column index {} out of range",
                spec.column
            ))),
        }
    }

    fn numeric_column(
        &self,
        row: &[Value],
        column: Option<usize>,
        label: &str,
    ) -> Result<Option<f64>, ExecError> {
        let Some(column) = column else {
            return Ok(None);
        };
        let Some(value) = row.get(column) else {
            return Err(ExecError::TypeMismatch(format!(
                "hybrid search {label} column index {column} out of range"
            )));
        };
        numeric_value(value).map_err(|msg| {
            ExecError::TypeMismatch(format!(
                "hybrid search {label} column must be numeric: {msg}"
            ))
        })
    }

    fn rank_candidates(&self, candidates: Vec<Candidate>) -> Vec<Vec<Value>> {
        let scores = self.component_scores(&candidates);
        let mut scored: Vec<ScoredRow> = candidates
            .into_iter()
            .zip(scores)
            .map(|(candidate, score)| ScoredRow {
                row: candidate.row,
                ordinal: candidate.ordinal,
                score,
            })
            .collect();

        scored.sort_by(compare_scored_rows);
        scored
            .into_iter()
            .take(self.config.limit)
            .map(|scored| scored.row)
            .collect()
    }

    /// Compute the fused score for each candidate, in candidate order.
    ///
    /// The lexical (BM25), vector, recency, and version signals are derived
    /// once, then combined per [`FusionMethod`]. Splitting derivation from
    /// fusion keeps the two fusion strategies — and the explainability hook
    /// in PART 5 — reading from one set of component values.
    fn component_scores(&self, candidates: &[Candidate]) -> Vec<f64> {
        let query_terms = self
            .config
            .text
            .as_ref()
            .map(|spec| unique_terms(tokenize(&spec.query)))
            .unwrap_or_default();
        let doc_freq = document_frequencies(candidates, &query_terms);
        let avg_doc_len = average_document_len(candidates);
        let corpus_docs = candidates.len().to_f64().unwrap_or(0.0);

        let bm25: Vec<f64> = candidates
            .iter()
            .map(|c| bm25_score(c, &query_terms, &doc_freq, avg_doc_len, corpus_docs))
            .collect();
        let vector: Vec<Option<f64>> = candidates.iter().map(|c| c.vector_similarity).collect();
        let recency: Vec<Option<f64>> = candidates.iter().map(|c| c.recency).collect();
        let version: Vec<Option<f64>> = candidates.iter().map(|c| c.version).collect();
        let weights = self.config.weights;

        match self.config.fusion {
            FusionMethod::WeightedLinear => {
                let recency_range = finite_range(recency.iter().copied().flatten());
                let version_range = finite_range(version.iter().copied().flatten());
                (0..candidates.len())
                    .map(|i| {
                        (weights.bm25 * bm25[i])
                            + (weights.vector * vector[i].unwrap_or(0.0))
                            + (weights.recency * normalized(recency[i], recency_range))
                            + (weights.version * normalized(version[i], version_range))
                    })
                    .collect()
            }
            FusionMethod::Rrf { k } => {
                // BM25 only contributes a lexical rank when a query term
                // actually matched (score > 0); a zero score means the row
                // is absent from the lexical result list.
                let bm25_opt: Vec<Option<f64>> =
                    bm25.iter().map(|&s| (s > 0.0).then_some(s)).collect();
                let bm25_rank = rrf_ranks(&bm25_opt);
                let vector_rank = rrf_ranks(&vector);
                let recency_rank = rrf_ranks(&recency);
                let version_rank = rrf_ranks(&version);
                (0..candidates.len())
                    .map(|i| {
                        rrf_term(weights.bm25, bm25_rank[i], k)
                            + rrf_term(weights.vector, vector_rank[i], k)
                            + rrf_term(weights.recency, recency_rank[i], k)
                            + rrf_term(weights.version, version_rank[i], k)
                    })
                    .collect()
            }
        }
    }
}

/// Assign 1-based ranks to each index by descending value (best = rank 1).
///
/// Indices whose value is `None` are absent from this component's ranking
/// and receive `None`. Ties are broken by original index so the ranking is
/// deterministic.
fn rrf_ranks(values: &[Option<f64>]) -> Vec<Option<usize>> {
    let mut order: Vec<usize> = (0..values.len()).filter(|&i| values[i].is_some()).collect();
    order.sort_by(|&a, &b| {
        let va = values[a].unwrap_or(f64::NEG_INFINITY);
        let vb = values[b].unwrap_or(f64::NEG_INFINITY);
        vb.total_cmp(&va).then_with(|| a.cmp(&b))
    });
    let mut ranks = vec![None; values.len()];
    for (rank, &idx) in order.iter().enumerate() {
        ranks[idx] = Some(rank + 1);
    }
    ranks
}

/// One Reciprocal Rank Fusion term: `weight / (k + rank)`, or `0` when the
/// candidate is absent from this component's ranking.
fn rrf_term(weight: f64, rank: Option<usize>, k: f64) -> f64 {
    match rank {
        Some(rank) => weight / (k + rank.to_f64().unwrap_or(f64::INFINITY)),
        None => 0.0,
    }
}

impl Operator for HybridSearch {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }
        if self.config.limit == 0 {
            self.eof = true;
            return Ok(None);
        }

        if self.sorted.is_none() {
            let candidates = self.collect_candidates()?;
            let rows = self.rank_candidates(candidates);
            self.sorted = Some(rows.into_iter());
        }

        let iter = self.sorted.as_mut().ok_or(ExecError::Internal(
            "hybrid search sorted cursor must be initialised",
        ))?;
        let chunk: Vec<Vec<Value>> = iter.by_ref().take(BATCH_TARGET_ROWS).collect();
        if chunk.is_empty() {
            self.eof = true;
            return Ok(None);
        }
        build_batch(&chunk, &self.schema).map(Some)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn estimated_row_count(&self) -> Option<usize> {
        Some(
            self.child
                .estimated_row_count()
                .map_or(self.config.limit, |rows| rows.min(self.config.limit)),
        )
    }
}

fn passes_filter(filter: &Option<Eval>, row: &[Value], label: &str) -> Result<bool, ExecError> {
    let Some(filter) = filter else {
        return Ok(true);
    };
    match filter.eval(row).map_err(eval_error_to_exec_error)? {
        Value::Bool(pass) => Ok(pass),
        Value::Null => Ok(false),
        other => Err(ExecError::TypeMismatch(format!(
            "hybrid search {label} must evaluate to Bool or Null, got {:?}",
            other.data_type()
        ))),
    }
}

fn tokenize(text: &str) -> Vec<String> {
    let mut terms = Vec::new();
    let mut current = String::new();
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() {
            current.push(char::from(byte.to_ascii_lowercase()));
        } else if !current.is_empty() {
            terms.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        terms.push(current);
    }
    terms
}

fn unique_terms(terms: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut unique = Vec::new();
    for term in terms {
        if seen.insert(term.clone()) {
            unique.push(term);
        }
    }
    unique
}

fn document_frequencies(
    candidates: &[Candidate],
    query_terms: &[String],
) -> HashMap<String, usize> {
    let mut frequencies = HashMap::new();
    for term in query_terms {
        let count = candidates
            .iter()
            .filter(|candidate| candidate.text_terms.iter().any(|doc_term| doc_term == term))
            .count();
        if count > 0 {
            frequencies.insert(term.clone(), count);
        }
    }
    frequencies
}

fn average_document_len(candidates: &[Candidate]) -> f64 {
    if candidates.is_empty() {
        return 0.0;
    }
    let total = candidates
        .iter()
        .map(|candidate| candidate.text_terms.len())
        .sum::<usize>()
        .to_f64()
        .unwrap_or(0.0);
    let docs = candidates.len().to_f64().unwrap_or(1.0);
    total / docs
}

fn bm25_score(
    candidate: &Candidate,
    query_terms: &[String],
    doc_freq: &HashMap<String, usize>,
    avg_doc_len: f64,
    corpus_docs: f64,
) -> f64 {
    if query_terms.is_empty() || avg_doc_len <= f64::EPSILON {
        return 0.0;
    }
    let doc_len = candidate.text_terms.len().to_f64().unwrap_or(0.0);
    if doc_len <= f64::EPSILON {
        return 0.0;
    }
    let mut score = 0.0;
    for term in query_terms {
        let tf = candidate
            .text_terms
            .iter()
            .filter(|doc_term| *doc_term == term)
            .count()
            .to_f64()
            .unwrap_or(0.0);
        if tf <= f64::EPSILON {
            continue;
        }
        let Some(df) = doc_freq.get(term).and_then(|count| count.to_f64()) else {
            continue;
        };
        let idf = (((corpus_docs - df + 0.5) / (df + 0.5)) + 1.0).ln();
        let length_norm = 1.0 - BM25_B + (BM25_B * (doc_len / avg_doc_len));
        let denom = tf + (BM25_K1 * length_norm);
        score += idf * ((tf * (BM25_K1 + 1.0)) / denom);
    }
    score
}

fn dense_vector_distance(
    values: &[f32],
    probe: &[f32],
    metric: VectorMetric,
) -> Result<f32, ExecError> {
    if values.len() != probe.len() {
        return Err(ExecError::TypeMismatch(format!(
            "hybrid search vector dimension mismatch: row has {}, probe has {}",
            values.len(),
            probe.len()
        )));
    }
    if values
        .iter()
        .chain(probe.iter())
        .any(|value| !value.is_finite())
    {
        return Err(ExecError::TypeMismatch(
            "hybrid search vector values must be finite".to_owned(),
        ));
    }
    let distance = match metric {
        VectorMetric::L2 => l2_distance_f32(values, probe),
        VectorMetric::Cosine => cosine_distance_f32(values, probe).ok_or_else(|| {
            ExecError::TypeMismatch(
                "hybrid search cosine distance requires non-zero vectors".to_owned(),
            )
        })?,
        VectorMetric::NegativeInnerProduct => -dot_f32(values, probe),
        VectorMetric::L1 => values
            .iter()
            .zip(probe.iter())
            .map(|(left, right)| (*left - *right).abs())
            .sum(),
    };
    Ok(distance)
}

fn distance_to_similarity(distance: f32, metric: VectorMetric) -> f64 {
    match metric {
        VectorMetric::NegativeInnerProduct => -f64::from(distance),
        VectorMetric::L2 | VectorMetric::Cosine | VectorMetric::L1 => {
            1.0 / (1.0 + f64::from(distance.max(0.0)))
        }
    }
}

fn numeric_value(value: &Value) -> Result<Option<f64>, String> {
    let value = match value {
        Value::Null => return Ok(None),
        Value::Int16(v) => v.to_f64(),
        Value::Int32(v) | Value::Date(v) => v.to_f64(),
        Value::Int64(v) | Value::Time(v) | Value::Timestamp(v) | Value::TimestampTz(v) => {
            v.to_f64()
        }
        Value::Float32(v) => Some(f64::from(*v)),
        Value::Float64(v) => Some(*v),
        Value::Decimal { value, .. } => value.to_f64(),
        other => {
            return Err(format!("got {:?}", other.data_type()));
        }
    };
    let Some(value) = value else {
        return Err("conversion to f64 failed".to_owned());
    };
    if !value.is_finite() {
        return Err("value is not finite".to_owned());
    }
    Ok(Some(value))
}

fn finite_range(values: impl Iterator<Item = f64>) -> Option<(f64, f64)> {
    let mut range: Option<(f64, f64)> = None;
    for value in values.filter(|value| value.is_finite()) {
        range = Some(match range {
            Some((min, max)) => (min.min(value), max.max(value)),
            None => (value, value),
        });
    }
    range
}

fn normalized(value: Option<f64>, range: Option<(f64, f64)>) -> f64 {
    let (Some(value), Some((min, max))) = (value, range) else {
        return 0.0;
    };
    let width = max - min;
    if width <= f64::EPSILON {
        return 1.0;
    }
    (value - min) / width
}

fn compare_scored_rows(left: &ScoredRow, right: &ScoredRow) -> Ordering {
    right
        .score
        .total_cmp(&left.score)
        .then_with(|| left.ordinal.cmp(&right.ordinal))
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::{BinaryOp, ScalarExpr};

    use super::*;
    use crate::MemTableScan;

    fn schema() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("content", DataType::Text { max_len: None }),
            Field::required("embedding", DataType::Vector { dims: Some(2) }),
            Field::required("metadata", DataType::Jsonb),
            Field::required("recency", DataType::Int64),
            Field::required("version", DataType::Int64),
            Field::required("published", DataType::Bool),
        ])
        .expect("hybrid search test schema is well-formed")
    }

    fn rows() -> Vec<Vec<Value>> {
        vec![
            vec![
                Value::Int32(1),
                Value::Text("rust sql vector database".to_owned()),
                Value::Vector(vec![0.0, 0.0]),
                Value::Jsonb(r#"{"kind":"guide","tenant":"a"}"#.to_owned()),
                Value::Int64(10),
                Value::Int64(1),
                Value::Bool(true),
            ],
            vec![
                Value::Int32(2),
                Value::Text("rust sql hybrid rag".to_owned()),
                Value::Vector(vec![0.05, 0.0]),
                Value::Jsonb(r#"{"kind":"guide","tenant":"a"}"#.to_owned()),
                Value::Int64(20),
                Value::Int64(2),
                Value::Bool(true),
            ],
            vec![
                Value::Int32(3),
                Value::Text("rust sql vector stale".to_owned()),
                Value::Vector(vec![0.02, 0.0]),
                Value::Jsonb(r#"{"kind":"blog","tenant":"a"}"#.to_owned()),
                Value::Int64(100),
                Value::Int64(9),
                Value::Bool(true),
            ],
            vec![
                Value::Int32(4),
                Value::Text("rust sql old guide".to_owned()),
                Value::Vector(vec![0.35, 0.0]),
                Value::Jsonb(r#"{"kind":"guide","tenant":"a"}"#.to_owned()),
                Value::Int64(1),
                Value::Int64(1),
                Value::Bool(true),
            ],
            vec![
                Value::Int32(5),
                Value::Text("rust sql hidden guide".to_owned()),
                Value::Vector(vec![0.01, 0.0]),
                Value::Jsonb(r#"{"kind":"guide","tenant":"a"}"#.to_owned()),
                Value::Int64(40),
                Value::Int64(5),
                Value::Bool(false),
            ],
        ]
    }

    fn col(name: &str, index: usize, data_type: DataType) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.to_owned(),
            index,
            data_type,
        }
    }

    fn lit(value: Value, data_type: DataType) -> ScalarExpr {
        ScalarExpr::Literal { value, data_type }
    }

    #[test]
    fn hybrid_search_combines_text_vector_json_recency_version_and_where() {
        let schema = schema();
        let batch = build_batch(&rows(), &schema).expect("rows encode");
        let scan = MemTableScan::new(schema.clone(), vec![batch]);
        let metadata_filter = ScalarExpr::Binary {
            op: BinaryOp::JsonContains,
            left: Box::new(col("metadata", 3, DataType::Jsonb)),
            right: Box::new(lit(
                Value::Jsonb(r#"{"kind":"guide"}"#.to_owned()),
                DataType::Jsonb,
            )),
            data_type: DataType::Bool,
        };
        let id_predicate = ScalarExpr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(col("id", 0, DataType::Int32)),
            right: Box::new(lit(Value::Int32(1), DataType::Int32)),
            data_type: DataType::Bool,
        };
        let published_predicate = ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(col("published", 6, DataType::Bool)),
            right: Box::new(lit(Value::Bool(true), DataType::Bool)),
            data_type: DataType::Bool,
        };
        let where_predicate = ScalarExpr::Binary {
            op: BinaryOp::And,
            left: Box::new(id_predicate),
            right: Box::new(published_predicate),
            data_type: DataType::Bool,
        };
        let config = HybridSearchConfig {
            text: Some(HybridTextSpec {
                column: 1,
                query: "rust sql hybrid".to_owned(),
            }),
            vector: Some(HybridVectorSpec {
                column: 2,
                probe: vec![0.0, 0.0],
                metric: VectorMetric::L2,
            }),
            metadata_filter: Some(metadata_filter),
            where_predicate: Some(where_predicate),
            recency_column: Some(4),
            version_column: Some(5),
            limit: 2,
            weights: HybridSearchWeights {
                bm25: 1.0,
                vector: 2.0,
                recency: 0.75,
                version: 0.25,
            },
            fusion: FusionMethod::WeightedLinear,
        };

        let mut op = HybridSearch::new(Box::new(scan), schema.clone(), config);
        let batch = op
            .next_batch()
            .expect("hybrid search runs")
            .expect("hybrid search emits one batch");
        let out = batch_to_rows(&batch, &schema).expect("output decodes");
        let ids: Vec<i32> = out
            .iter()
            .map(|row| match row.first() {
                Some(Value::Int32(id)) => *id,
                other => panic!("expected Int32 id, got {other:?}"),
            })
            .collect();

        assert_eq!(ids, vec![2, 4]);
        assert!(op.next_batch().expect("eof check").is_none());
    }

    /// 1-based rank by descending value (as `f64`), ties broken by index — the
    /// same contract as the operator's `rrf_ranks`, reimplemented independently
    /// so the test is a true reference check rather than a tautology.
    fn reference_rank_desc(values: &[f64]) -> Vec<f64> {
        let mut order: Vec<usize> = (0..values.len()).collect();
        order.sort_by(|&a, &b| values[b].total_cmp(&values[a]).then_with(|| a.cmp(&b)));
        let mut ranks = vec![0.0_f64; values.len()];
        for (rank, &idx) in order.iter().enumerate() {
            ranks[idx] = f64::from(u32::try_from(rank + 1).expect("rank fits u32"));
        }
        ranks
    }

    fn drain_ids(op: &mut HybridSearch, schema: &Schema) -> Vec<i32> {
        let mut ids = Vec::new();
        while let Some(batch) = op.next_batch().expect("hybrid search batch") {
            for row in batch_to_rows(&batch, schema).expect("decode") {
                match row.first() {
                    Some(Value::Int32(id)) => ids.push(*id),
                    other => panic!("expected Int32 id, got {other:?}"),
                }
            }
        }
        ids
    }

    #[test]
    fn hybrid_search_rrf_matches_reference_fusion() {
        // Two components (vector closeness, version) whose rankings disagree,
        // so the fused order is non-trivial and exercises real RRF math.
        let schema = schema();
        let spec = [(1, 0.1_f32, 1_i64), (2, 0.2, 4), (3, 0.3, 3), (4, 0.4, 2)];
        let rows: Vec<Vec<Value>> = spec
            .iter()
            .map(|&(id, dist, version)| {
                vec![
                    Value::Int32(id),
                    Value::Text(String::new()),
                    Value::Vector(vec![dist, 0.0]),
                    Value::Jsonb("{}".to_owned()),
                    Value::Int64(0),
                    Value::Int64(version),
                    Value::Bool(true),
                ]
            })
            .collect();
        let batch = build_batch(&rows, &schema).expect("rows encode");
        let scan = MemTableScan::new(schema.clone(), vec![batch]);
        let config = HybridSearchConfig {
            text: None,
            vector: Some(HybridVectorSpec {
                column: 2,
                probe: vec![0.0, 0.0],
                metric: VectorMetric::L2,
            }),
            metadata_filter: None,
            where_predicate: None,
            recency_column: None,
            version_column: Some(5),
            limit: 4,
            weights: HybridSearchWeights {
                bm25: 0.0,
                vector: 1.0,
                recency: 0.0,
                version: 1.0,
            },
            fusion: FusionMethod::rrf(),
        };
        let mut op = HybridSearch::new(Box::new(scan), schema.clone(), config);
        let got = drain_ids(&mut op, &schema);

        // Independent reference RRF over the same component signals.
        let k = FusionMethod::RRF_DEFAULT_K;
        let vector_sim: Vec<f64> = spec
            .iter()
            .map(|&(_, dist, _)| 1.0 / (1.0 + f64::from(dist)))
            .collect();
        let version: Vec<f64> = spec
            .iter()
            .map(|&(_, _, v)| f64::from(i32::try_from(v).expect("test version fits i32")))
            .collect();
        let vrank = reference_rank_desc(&vector_sim);
        let verrank = reference_rank_desc(&version);
        let mut expected: Vec<(i32, f64)> = spec
            .iter()
            .enumerate()
            .map(|(i, &(id, _, _))| {
                let score = 1.0 / (k + vrank[i]) + 1.0 / (k + verrank[i]);
                (id, score)
            })
            .collect();
        expected.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        let expected_ids: Vec<i32> = expected.into_iter().map(|(id, _)| id).collect();

        assert_eq!(got, expected_ids);
        // Sanity: the disagreeing components make the fused winner id=2, not
        // the vector-closest id=1.
        assert_eq!(got.first(), Some(&2));
    }

    #[test]
    fn hybrid_search_limit_zero_returns_eof_without_draining_child() {
        let schema = schema();
        let batch = build_batch(&rows(), &schema).expect("rows encode");
        let scan = MemTableScan::new(schema.clone(), vec![batch]);
        let config = HybridSearchConfig {
            text: Some(HybridTextSpec {
                column: 1,
                query: "rust sql".to_owned(),
            }),
            vector: None,
            metadata_filter: None,
            where_predicate: None,
            recency_column: None,
            version_column: None,
            limit: 0,
            weights: HybridSearchWeights::default(),
            fusion: FusionMethod::WeightedLinear,
        };

        let mut op = HybridSearch::new(Box::new(scan), schema, config);

        assert!(op.next_batch().expect("zero-limit eof").is_none());
    }

    #[test]
    fn hybrid_search_filter_eval_error_stays_typed() {
        let schema = schema();
        let batch = build_batch(&rows(), &schema).expect("rows encode");
        let scan = MemTableScan::new(schema.clone(), vec![batch]);
        let where_predicate = ScalarExpr::Binary {
            op: BinaryOp::Div,
            left: Box::new(col("id", 0, DataType::Int32)),
            right: Box::new(lit(Value::Int32(0), DataType::Int32)),
            data_type: DataType::Int32,
        };
        let config = HybridSearchConfig {
            text: None,
            vector: None,
            metadata_filter: None,
            where_predicate: Some(where_predicate),
            recency_column: None,
            version_column: None,
            limit: 1,
            weights: HybridSearchWeights::default(),
            fusion: FusionMethod::WeightedLinear,
        };

        let mut op = HybridSearch::new(Box::new(scan), schema, config);
        let err = op
            .next_batch()
            .expect_err("hybrid filter division by zero must surface");
        assert!(matches!(err, ExecError::DivisionByZero(_)), "{err:?}");
    }
}
