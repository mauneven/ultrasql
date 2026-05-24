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
use crate::{ExecError, Operator};

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
        let query_terms = self
            .config
            .text
            .as_ref()
            .map(|spec| unique_terms(tokenize(&spec.query)))
            .unwrap_or_default();
        let doc_freq = document_frequencies(&candidates, &query_terms);
        let avg_doc_len = average_document_len(&candidates);
        let corpus_docs = candidates.len().to_f64().unwrap_or(0.0);
        let recency_range = finite_range(candidates.iter().filter_map(|c| c.recency));
        let version_range = finite_range(candidates.iter().filter_map(|c| c.version));

        let mut scored: Vec<ScoredRow> = candidates
            .into_iter()
            .map(|candidate| {
                let bm25 = bm25_score(
                    &candidate,
                    &query_terms,
                    &doc_freq,
                    avg_doc_len,
                    corpus_docs,
                );
                let vector = candidate.vector_similarity.unwrap_or(0.0);
                let recency = normalized(candidate.recency, recency_range);
                let version = normalized(candidate.version, version_range);
                let score = (self.config.weights.bm25 * bm25)
                    + (self.config.weights.vector * vector)
                    + (self.config.weights.recency * recency)
                    + (self.config.weights.version * version);
                ScoredRow {
                    row: candidate.row,
                    ordinal: candidate.ordinal,
                    score,
                }
            })
            .collect();

        scored.sort_by(compare_scored_rows);
        scored
            .into_iter()
            .take(self.config.limit)
            .map(|scored| scored.row)
            .collect()
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
    match filter
        .eval(row)
        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?
    {
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
        };

        let mut op = HybridSearch::new(Box::new(scan), schema, config);

        assert!(op.next_batch().expect("zero-limit eof").is_none());
    }
}
