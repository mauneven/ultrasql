//! Hash equi-join operator.
//!
//! Implements Inner, Left Outer, Semi, and Anti equi-joins using a classical
//! build+probe hash table.
//!
//! The default path drains the left child first and hashes it by the left key,
//! then streams the right child as the probe side. Semi/anti joins can also
//! flip that orientation and build the right/subquery side instead, which lets
//! the server avoid hashing a large left relation just to test membership
//! against a compact `IN` / `NOT IN` subquery result.
//!
//! # Join type support
//!
//! | Join type   | Status |
//! |-------------|--------|
//! | `Inner`     | Supported. |
//! | `LeftOuter` | Supported: unmatched left rows are emitted with NULL right columns at the end of the probe phase. |
//! | `Semi`     | Supported: matched left rows are emitted once, right columns suppressed. |
//! | `Anti`     | Supported: unmatched left rows are emitted, right columns suppressed. |
//! | `RightOuter`, `FullOuter`, `Cross` | Return [`ExecError::Unsupported`] — pending wave 6. |
//!
//! # NULL key semantics
//!
//! NULL keys never match (SQL standard: `NULL = NULL` is unknown, not true).
//! Rows with a NULL build key are placed in the hash table under a
//! `Value::Null` bucket but are never returned because the probe lookup also
//! skips NULL probe keys.
//!
//! # Duplicate build keys
//!
//! Multiple left rows with the same (non-NULL) key are all stored; the probe
//! emits one output row per (right, left) pair.
//!
//! # Residual predicates
//!
//! The operator can evaluate an optional residual predicate after hash-key
//! equality succeeds. This lets the server lower predicates such as
//! `a.k = b.k AND a.x <> b.x` to a hash probe on `k` plus scalar evaluation
//! on candidate pairs, instead of falling back to a full nested-loop join.

use std::collections::{HashMap, VecDeque};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use ultrasql_core::{Schema, Value, bpchar_semantic_text, timetz_utc_micros};
use ultrasql_planner::{BinaryOp, LogicalJoinType, ScalarExpr};
use ultrasql_vec::Batch;

use crate::eval::Eval;
use crate::filter_op::batch_to_rows;
use crate::row_codec::RowCodec;
use crate::row_spill::{
    checked_spill_bytes_add, checked_temp_spill_total, spill_row_bytes_for_len,
};
use crate::seq_scan::build_batch;
use crate::value_key::{decimal_values_equal, hash_decimal_key};
use crate::work_mem::WorkMemBudget;
use crate::{ExecError, Operator, OperatorSpillProfile, eval_error_to_exec_error};

/// Maximum rows per emitted batch, matching the `ARCHITECTURE.md` section 9 contract.
const BATCH_TARGET_ROWS: usize = 4096;

/// Hash equi-join operator.
///
/// Performs a two-phase hash join:
///
/// 1. **Build phase** — drain `left`, hash each row by `left_key`.
/// 2. **Probe phase** — stream `right`, look up each row's `right_key` in the
///    hash table, emit matching pairs.
///
/// After the probe phase, unmatched left rows are emitted (for `LeftOuter`).
///
/// # Send bound
///
/// All owned fields are `Send`: `Box<dyn Operator>`, `Eval`, `Schema`, and
/// `HashMap`.
#[derive(Debug)]
pub struct HashJoin {
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,
    left_key_evals: Vec<Eval>,
    right_key_evals: Vec<Eval>,
    residual_eval: Option<Eval>,
    residual_fast: Option<FastResidual>,
    join_type: LogicalJoinType,
    schema: Schema,
    left_schema: Schema,
    right_schema: Schema,
    build_side: BuildSide,
    /// Build-side rows retained for hash lookup and output assembly.
    left_rows: Vec<Vec<Value>>,
    /// Build-side rows retained when a semi/anti join hashes the right child.
    right_rows: Vec<Vec<Value>>,
    /// Hash table built from the left side on first execution.
    hash_table: Option<HashMap<JoinKey, Vec<usize>>>,
    /// Optional work-memory budget that can trigger adaptive spill.
    work_mem: Option<Arc<WorkMemBudget>>,
    /// Bytes observed on the build side while constructing the join.
    build_memory_bytes: u64,
    /// Disk-backed build-side spill for inner joins whose build side
    /// exceeds the configured work-memory budget.
    spill: Option<HashJoinSpill>,
    /// Whether this execution switched from in-memory hash join to spill.
    spilled_to_disk: bool,
    /// Whether each left row matched at least one probe row.
    left_matched: Vec<bool>,
    /// Number of build-side rows whose match bit is set.
    matched_left_count: usize,
    /// Joined rows produced from probe batches but not yet emitted.
    pending_output: VecDeque<Vec<Value>>,
    /// `true` once the right/probe side has been fully consumed.
    probe_finished: bool,
    /// Cursor for emitting deferred left rows in `LeftOuter`, `Semi`, or `Anti` mode.
    next_unmatched_left: usize,
    /// `true` after the final `Ok(None)` is returned.
    eof: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BuildSide {
    Left,
    RightForSemiAnti,
}

#[derive(Debug)]
struct HashJoinSpill {
    file: tempfile::NamedTempFile,
    bytes: u64,
}

impl HashJoinSpill {
    fn new() -> Result<Self, ExecError> {
        let file = tempfile::NamedTempFile::new().map_err(|error| {
            ExecError::TypeMismatch(format!("hash join spill create failed: {error}"))
        })?;
        Ok(Self { file, bytes: 0 })
    }

    fn append_row(&mut self, encoded: &[u8]) -> Result<(), ExecError> {
        let len = u32::try_from(encoded.len()).map_err(|_| {
            ExecError::TypeMismatch("hash join spill row exceeds u32 length".to_owned())
        })?;
        let row_bytes = spill_row_bytes_for_len(encoded.len(), "hash join")?;
        let next_bytes = checked_temp_spill_total(self.bytes, row_bytes, "hash join")?;

        let handle = self.file.as_file_mut();
        handle
            .write_all(&len.to_le_bytes())
            .map_err(|error| spill_io_error("write row length", error))?;
        handle
            .write_all(encoded)
            .map_err(|error| spill_io_error("write row", error))?;
        self.bytes = next_bytes;
        Ok(())
    }

    fn scan_rows<F>(&mut self, codec: &RowCodec, mut visit: F) -> Result<(), ExecError>
    where
        F: FnMut(Vec<Value>) -> Result<(), ExecError>,
    {
        let handle = self.file.as_file_mut();
        handle
            .flush()
            .map_err(|error| spill_io_error("flush", error))?;
        handle
            .seek(SeekFrom::Start(0))
            .map_err(|error| spill_io_error("rewind", error))?;

        loop {
            let mut len_buf = [0_u8; 4];
            match handle.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(error) => return Err(spill_io_error("read row length", error)),
            }
            let len = usize::try_from(u32::from_le_bytes(len_buf)).map_err(|_| {
                ExecError::TypeMismatch("hash join spill row length exceeds usize".to_owned())
            })?;
            let mut encoded = vec![0_u8; len];
            handle
                .read_exact(&mut encoded)
                .map_err(|error| spill_io_error("read row", error))?;
            let row = codec.decode(&encoded).map_err(|error| {
                ExecError::TypeMismatch(format!("hash join spill decode failed: {error}"))
            })?;
            visit(row)?;
        }
        Ok(())
    }
}

impl HashJoin {
    /// Construct a hash join operator.
    ///
    /// - `left` — the build side.
    /// - `right` — the probe side.
    /// - `left_key` — expression evaluated over left rows to produce the build key.
    /// - `right_key` — expression evaluated over right rows to produce the probe key.
    /// - `join_type` — must be `Inner`, `LeftOuter`, `Semi`, or `Anti`;
    ///   other variants return `ExecError::Unsupported` at runtime.
    /// - `schema` — output schema (left columns followed by right columns).
    /// - `left_schema` — schema of the left child's output.
    /// - `right_schema` — schema of the right child's output.
    #[must_use]
    #[allow(clippy::too_many_arguments)] // all 8 parameters are distinct logical inputs
    pub fn new(
        left: Box<dyn Operator>,
        right: Box<dyn Operator>,
        left_key: ScalarExpr,
        right_key: ScalarExpr,
        join_type: LogicalJoinType,
        schema: Schema,
        left_schema: Schema,
        right_schema: Schema,
    ) -> Self {
        Self::new_multi(
            left,
            right,
            vec![left_key],
            vec![right_key],
            join_type,
            schema,
            left_schema,
            right_schema,
        )
    }

    /// Construct a hash join with one or more equality keys.
    ///
    /// The key vectors must be non-empty and have matching lengths. Each
    /// key at position `i` forms one equality predicate:
    /// `left_keys[i] = right_keys[i]`. Rows with NULL in any key component
    /// do not match, preserving SQL equality semantics.
    #[must_use]
    #[allow(clippy::too_many_arguments)] // all 8 parameters are distinct logical inputs
    pub fn new_multi(
        left: Box<dyn Operator>,
        right: Box<dyn Operator>,
        left_keys: Vec<ScalarExpr>,
        right_keys: Vec<ScalarExpr>,
        join_type: LogicalJoinType,
        schema: Schema,
        left_schema: Schema,
        right_schema: Schema,
    ) -> Self {
        Self::new_multi_with_residual(
            left,
            right,
            left_keys,
            right_keys,
            None,
            join_type,
            schema,
            left_schema,
            right_schema,
        )
    }

    /// Construct a hash join with equality keys plus an optional residual
    /// predicate evaluated over the joined `left ++ right` row.
    #[must_use]
    #[allow(clippy::too_many_arguments)] // all 9 parameters are distinct logical inputs
    pub fn new_multi_with_residual(
        left: Box<dyn Operator>,
        right: Box<dyn Operator>,
        left_keys: Vec<ScalarExpr>,
        right_keys: Vec<ScalarExpr>,
        residual: Option<ScalarExpr>,
        join_type: LogicalJoinType,
        schema: Schema,
        left_schema: Schema,
        right_schema: Schema,
    ) -> Self {
        debug_assert!(!left_keys.is_empty(), "hash join requires at least one key");
        debug_assert_eq!(
            left_keys.len(),
            right_keys.len(),
            "hash join key vectors must align"
        );
        Self {
            left,
            right,
            left_key_evals: left_keys.into_iter().map(Eval::new).collect(),
            right_key_evals: right_keys.into_iter().map(Eval::new).collect(),
            residual_fast: residual
                .as_ref()
                .and_then(|expr| match_fast_residual(expr, left_schema.len())),
            residual_eval: residual.map(Eval::new),
            join_type,
            schema,
            left_schema,
            right_schema,
            build_side: BuildSide::Left,
            left_rows: Vec::new(),
            right_rows: Vec::new(),
            hash_table: None,
            work_mem: None,
            build_memory_bytes: 0,
            spill: None,
            spilled_to_disk: false,
            left_matched: Vec::new(),
            matched_left_count: 0,
            pending_output: VecDeque::new(),
            probe_finished: false,
            next_unmatched_left: 0,
            eof: false,
        }
    }

    /// Construct a semi/anti hash join that builds the right side and streams
    /// the left/output side as the probe.
    #[must_use]
    #[allow(clippy::too_many_arguments)] // all 9 parameters are distinct logical inputs
    pub fn new_multi_with_residual_build_right(
        left: Box<dyn Operator>,
        right: Box<dyn Operator>,
        left_keys: Vec<ScalarExpr>,
        right_keys: Vec<ScalarExpr>,
        residual: Option<ScalarExpr>,
        join_type: LogicalJoinType,
        schema: Schema,
        left_schema: Schema,
        right_schema: Schema,
    ) -> Self {
        debug_assert!(
            matches!(join_type, LogicalJoinType::Semi | LogicalJoinType::Anti),
            "right-build hash join is only valid for semi/anti joins"
        );
        let mut join = Self::new_multi_with_residual(
            left,
            right,
            left_keys,
            right_keys,
            residual,
            join_type,
            schema,
            left_schema,
            right_schema,
        );
        join.build_side = BuildSide::RightForSemiAnti;
        join
    }

    /// Attach a per-query work-memory budget.
    #[must_use]
    pub fn with_work_mem_budget(mut self, budget: Arc<WorkMemBudget>) -> Self {
        self.work_mem = Some(budget);
        self
    }

    /// Whether this execution switched to the disk-spill path.
    #[must_use]
    pub const fn spilled_to_disk(&self) -> bool {
        self.spilled_to_disk
    }
}

impl Operator for HashJoin {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }

        if self.hash_table.is_none() {
            self.build_phase()?;
        }

        if self.build_side == BuildSide::RightForSemiAnti {
            return self.next_batch_build_right_semi_anti();
        }

        let null_right = vec![Value::Null; self.right_schema.len()];
        let mut chunk: Vec<Vec<Value>> = Vec::with_capacity(BATCH_TARGET_ROWS);

        while chunk.len() < BATCH_TARGET_ROWS {
            if let Some(row) = self.pending_output.pop_front() {
                chunk.push(row);
                continue;
            }

            if !self.probe_finished {
                if self.probe_once()? {
                    continue;
                }
                self.probe_finished = true;
                continue;
            }

            match self.join_type {
                LogicalJoinType::LeftOuter => {
                    while self.next_unmatched_left < self.left_rows.len()
                        && chunk.len() < BATCH_TARGET_ROWS
                    {
                        if !self.left_matched[self.next_unmatched_left] {
                            chunk.push(concat_rows(
                                &self.left_rows[self.next_unmatched_left],
                                &null_right,
                            ));
                        }
                        self.next_unmatched_left += 1;
                    }
                }
                LogicalJoinType::Semi | LogicalJoinType::Anti => {
                    let want_matched = self.join_type == LogicalJoinType::Semi;
                    while self.next_unmatched_left < self.left_rows.len()
                        && chunk.len() < BATCH_TARGET_ROWS
                    {
                        if self.left_matched[self.next_unmatched_left] == want_matched {
                            chunk.push(self.left_rows[self.next_unmatched_left].clone());
                        }
                        self.next_unmatched_left += 1;
                    }
                }
                LogicalJoinType::Inner
                | LogicalJoinType::RightOuter
                | LogicalJoinType::FullOuter
                | LogicalJoinType::Cross => {}
            }

            break;
        }

        if chunk.is_empty() {
            self.eof = true;
            return Ok(None);
        }
        build_batch(&chunk, &self.schema).map(Some)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn profile_children(&self) -> Vec<&dyn Operator> {
        vec![self.left.as_ref(), self.right.as_ref()]
    }

    fn spill_profile(&self) -> OperatorSpillProfile {
        if !self.spilled_to_disk {
            return OperatorSpillProfile::default();
        }
        OperatorSpillProfile {
            spills: 1,
            bytes: self.spill.as_ref().map_or(0, |spill| spill.bytes),
        }
    }

    fn io_bytes(&self) -> u64 {
        self.spill_profile().bytes.saturating_mul(2)
    }
}

impl HashJoin {
    fn next_batch_build_right_semi_anti(&mut self) -> Result<Option<Batch>, ExecError> {
        debug_assert!(
            matches!(
                self.join_type,
                LogicalJoinType::Semi | LogicalJoinType::Anti
            ),
            "right-build path only supports semi/anti joins"
        );

        let mut chunk: Vec<Vec<Value>> = Vec::with_capacity(BATCH_TARGET_ROWS);

        while chunk.len() < BATCH_TARGET_ROWS {
            if let Some(row) = self.pending_output.pop_front() {
                chunk.push(row);
                continue;
            }

            let Some(batch) = self.left.next_batch()? else {
                break;
            };
            let rows = batch_to_rows(&batch, &self.left_schema)?;
            for left_row in rows {
                let matched = self.right_build_matches_left_row(&left_row)?;
                match self.join_type {
                    LogicalJoinType::Semi if matched => self.pending_output.push_back(left_row),
                    LogicalJoinType::Anti if !matched => self.pending_output.push_back(left_row),
                    LogicalJoinType::Semi | LogicalJoinType::Anti => {}
                    LogicalJoinType::Inner
                    | LogicalJoinType::LeftOuter
                    | LogicalJoinType::RightOuter
                    | LogicalJoinType::FullOuter
                    | LogicalJoinType::Cross => unreachable!(
                        "right-build semi/anti path reached with non semi/anti join type"
                    ),
                }
            }
        }

        while chunk.len() < BATCH_TARGET_ROWS {
            let Some(row) = self.pending_output.pop_front() else {
                break;
            };
            chunk.push(row);
        }

        if chunk.is_empty() {
            self.eof = true;
            return Ok(None);
        }
        build_batch(&chunk, &self.schema).map(Some)
    }

    fn build_phase(&mut self) -> Result<(), ExecError> {
        if self.build_side == BuildSide::RightForSemiAnti {
            return self.build_right_phase();
        }

        // Validate join type early so the error surfaces before doing any work.
        match self.join_type {
            LogicalJoinType::Inner
            | LogicalJoinType::LeftOuter
            | LogicalJoinType::Semi
            | LogicalJoinType::Anti => {}
            LogicalJoinType::RightOuter => {
                return Err(ExecError::Unsupported(
                    "hash join outer variant pending: RightOuter",
                ));
            }
            LogicalJoinType::FullOuter => {
                return Err(ExecError::Unsupported(
                    "hash join outer variant pending: FullOuter",
                ));
            }
            LogicalJoinType::Cross => {
                return Err(ExecError::Unsupported(
                    "hash join outer variant pending: Cross (use NestedLoopJoin)",
                ));
            }
        }

        // ----- Build phase -----
        // Key: one or more left key values. Value: row indices into
        // `left_rows`. The row array keeps output assembly contiguous
        // while the hash table stays compact.
        let mut hash_table: HashMap<JoinKey, Vec<usize>> = HashMap::new();
        let left_codec = RowCodec::new(self.left_schema.clone());

        loop {
            let Some(batch) = self.left.next_batch()? else {
                break;
            };
            let rows = batch_to_rows(&batch, &self.left_schema)?;
            for row in rows {
                if let Some(spill) = self.spill.as_mut() {
                    let encoded = encode_spill_row(&left_codec, &row)?;
                    spill.append_row(&encoded)?;
                    continue;
                }

                let encoded = if self.work_mem.is_some() {
                    Some(encode_spill_row(&left_codec, &row)?)
                } else {
                    None
                };
                if let Some(encoded) = encoded.as_ref()
                    && self.should_switch_build_to_spill(encoded.len())?
                {
                    let mut spill = self.spill_existing_build_rows(&mut hash_table, &left_codec)?;
                    spill.append_row(encoded)?;
                    self.spill = Some(spill);
                    continue;
                }

                if let Some(key) = build_join_key(&self.left_key_evals, &row)? {
                    hash_table
                        .entry(key)
                        .or_default()
                        .push(self.left_rows.len());
                }
                self.left_rows.push(row);
            }
        }

        self.left_matched = if self.spill.is_some() {
            Vec::new()
        } else {
            vec![false; self.left_rows.len()]
        };
        self.matched_left_count = 0;
        self.hash_table = Some(hash_table);
        Ok(())
    }

    fn should_switch_build_to_spill(&mut self, encoded_len: usize) -> Result<bool, ExecError> {
        if self.join_type != LogicalJoinType::Inner || self.build_side != BuildSide::Left {
            return Ok(false);
        }
        let Some(budget) = &self.work_mem else {
            return Ok(false);
        };
        let row_bytes = spill_row_bytes_for_len(encoded_len, "hash join build")?;
        self.build_memory_bytes =
            checked_spill_bytes_add(self.build_memory_bytes, row_bytes, "hash join build")?;
        Ok(self.build_memory_bytes > budget.limit_bytes())
    }

    fn spill_existing_build_rows(
        &mut self,
        hash_table: &mut HashMap<JoinKey, Vec<usize>>,
        left_codec: &RowCodec,
    ) -> Result<HashJoinSpill, ExecError> {
        let mut spill = HashJoinSpill::new()?;
        for row in self.left_rows.drain(..) {
            let encoded = encode_spill_row(left_codec, &row)?;
            spill.append_row(&encoded)?;
        }
        hash_table.clear();
        self.left_matched.clear();
        self.matched_left_count = 0;
        self.spilled_to_disk = true;
        Ok(spill)
    }

    fn build_right_phase(&mut self) -> Result<(), ExecError> {
        match self.join_type {
            LogicalJoinType::Semi | LogicalJoinType::Anti => {}
            LogicalJoinType::Inner
            | LogicalJoinType::LeftOuter
            | LogicalJoinType::RightOuter
            | LogicalJoinType::FullOuter
            | LogicalJoinType::Cross => {
                return Err(ExecError::Unsupported(
                    "right-build hash join is only implemented for semi/anti joins",
                ));
            }
        }

        let mut hash_table: HashMap<JoinKey, Vec<usize>> = HashMap::new();

        loop {
            let Some(batch) = self.right.next_batch()? else {
                break;
            };
            let rows = batch_to_rows(&batch, &self.right_schema)?;
            for row in rows {
                if let Some(key) = build_join_key(&self.right_key_evals, &row)? {
                    hash_table
                        .entry(key)
                        .or_default()
                        .push(self.right_rows.len());
                }
                self.right_rows.push(row);
            }
        }

        self.hash_table = Some(hash_table);
        Ok(())
    }

    fn probe_once(&mut self) -> Result<bool, ExecError> {
        if self.spill.is_some() {
            return self.probe_once_spilled();
        }
        if matches!(
            self.join_type,
            LogicalJoinType::Semi | LogicalJoinType::Anti
        ) && self.matched_left_count == self.left_rows.len()
        {
            return Ok(false);
        }
        let Some(batch) = self.right.next_batch()? else {
            return Ok(false);
        };
        let rows = batch_to_rows(&batch, &self.right_schema)?;
        for right_row in rows {
            let Some(probe_key) = build_join_key(&self.right_key_evals, &right_row)? else {
                continue;
            };
            let indices = self
                .hash_table
                .as_ref()
                .and_then(|table| table.get(&probe_key).cloned());
            if let Some(indices) = indices {
                for li in indices {
                    if matches!(
                        self.join_type,
                        LogicalJoinType::Semi | LogicalJoinType::Anti
                    ) && self.left_matched[li]
                    {
                        continue;
                    }
                    match self.join_type {
                        LogicalJoinType::Inner => {
                            let joined = concat_rows(&self.left_rows[li], &right_row);
                            if !self.passes_residual(&joined)? {
                                continue;
                            }
                            self.pending_output.push_back(joined);
                        }
                        LogicalJoinType::LeftOuter => {
                            let joined = concat_rows(&self.left_rows[li], &right_row);
                            if !self.passes_residual(&joined)? {
                                continue;
                            }
                            self.mark_left_matched(li);
                            self.pending_output.push_back(joined);
                        }
                        LogicalJoinType::Semi | LogicalJoinType::Anti => {
                            if !self
                                .passes_semi_anti_residual_rows(&self.left_rows[li], &right_row)?
                            {
                                continue;
                            }
                            self.mark_left_matched(li);
                        }
                        LogicalJoinType::RightOuter
                        | LogicalJoinType::FullOuter
                        | LogicalJoinType::Cross => {}
                    }
                }
            }
        }
        Ok(true)
    }

    fn probe_once_spilled(&mut self) -> Result<bool, ExecError> {
        let Some(batch) = self.right.next_batch()? else {
            return Ok(false);
        };
        let right_rows = batch_to_rows(&batch, &self.right_schema)?;
        let left_codec = RowCodec::new(self.left_schema.clone());
        let mut spill = self
            .spill
            .take()
            .ok_or(ExecError::Internal("hash join spill missing"))?;

        for right_row in right_rows {
            let Some(probe_key) = build_join_key(&self.right_key_evals, &right_row)? else {
                continue;
            };
            spill.scan_rows(&left_codec, |left_row| {
                let Some(build_key) = build_join_key(&self.left_key_evals, &left_row)? else {
                    return Ok(());
                };
                if build_key != probe_key {
                    return Ok(());
                }
                let joined = concat_rows(&left_row, &right_row);
                if self.passes_residual(&joined)? {
                    self.pending_output.push_back(joined);
                }
                Ok(())
            })?;
        }
        self.spill = Some(spill);
        Ok(true)
    }

    fn right_build_matches_left_row(&self, left_row: &[Value]) -> Result<bool, ExecError> {
        let Some(probe_key) = build_join_key(&self.left_key_evals, left_row)? else {
            return Ok(false);
        };
        let Some(indices) = self
            .hash_table
            .as_ref()
            .and_then(|table| table.get(&probe_key))
        else {
            return Ok(false);
        };
        for &ri in indices {
            if self.passes_semi_anti_residual_rows(left_row, &self.right_rows[ri])? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn mark_left_matched(&mut self, idx: usize) {
        if !self.left_matched[idx] {
            self.left_matched[idx] = true;
            self.matched_left_count = self.matched_left_count.saturating_add(1);
        }
    }

    fn passes_residual(&self, joined: &[Value]) -> Result<bool, ExecError> {
        let Some(eval) = &self.residual_eval else {
            return Ok(true);
        };
        match eval.eval(joined) {
            Ok(Value::Bool(true)) => Ok(true),
            Ok(Value::Bool(false) | Value::Null) => Ok(false),
            Ok(other) => Err(ExecError::TypeMismatch(format!(
                "hash join residual must evaluate to Bool or Null, got {:?}",
                other.data_type()
            ))),
            Err(error) => Err(eval_error_to_exec_error(error)),
        }
    }

    fn passes_semi_anti_residual_rows(
        &self,
        left_row: &[Value],
        right_row: &[Value],
    ) -> Result<bool, ExecError> {
        let Some(eval) = &self.residual_eval else {
            return Ok(true);
        };
        if let Some(fast) = &self.residual_fast {
            if let Some(result) = fast.eval(left_row, right_row) {
                return Ok(result);
            }
        }
        let joined = concat_rows(left_row, right_row);
        match eval.eval(&joined) {
            Ok(Value::Bool(true)) => Ok(true),
            Ok(Value::Bool(false) | Value::Null) => Ok(false),
            Ok(other) => Err(ExecError::TypeMismatch(format!(
                "hash join residual must evaluate to Bool or Null, got {:?}",
                other.data_type()
            ))),
            Err(error) => Err(eval_error_to_exec_error(error)),
        }
    }
}

// ---------------------------------------------------------------------------
// Fast residual predicates
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
struct FastResidual {
    left_index: usize,
    right_index: usize,
    op: BinaryOp,
}

impl FastResidual {
    fn eval(&self, left_row: &[Value], right_row: &[Value]) -> Option<bool> {
        let left = left_row.get(self.left_index)?;
        let right = right_row.get(self.right_index)?;
        compare_fast_values(left, right, self.op)
    }
}

fn match_fast_residual(expr: &ScalarExpr, left_width: usize) -> Option<FastResidual> {
    let ScalarExpr::Binary {
        op, left, right, ..
    } = expr
    else {
        return None;
    };
    if !matches!(
        op,
        BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq
    ) {
        return None;
    }
    let (
        ScalarExpr::Column {
            index: left_index, ..
        },
        ScalarExpr::Column {
            index: right_index, ..
        },
    ) = (left.as_ref(), right.as_ref())
    else {
        return None;
    };

    match (*left_index < left_width, *right_index < left_width) {
        (true, false) => Some(FastResidual {
            left_index: *left_index,
            right_index: *right_index - left_width,
            op: *op,
        }),
        (false, true) => Some(FastResidual {
            left_index: *right_index,
            right_index: *left_index - left_width,
            op: flip_binary_cmp(*op)?,
        }),
        _ => None,
    }
}

const fn flip_binary_cmp(op: BinaryOp) -> Option<BinaryOp> {
    match op {
        BinaryOp::Eq => Some(BinaryOp::Eq),
        BinaryOp::NotEq => Some(BinaryOp::NotEq),
        BinaryOp::Lt => Some(BinaryOp::Gt),
        BinaryOp::LtEq => Some(BinaryOp::GtEq),
        BinaryOp::Gt => Some(BinaryOp::Lt),
        BinaryOp::GtEq => Some(BinaryOp::LtEq),
        _ => None,
    }
}

fn compare_fast_values(left: &Value, right: &Value, op: BinaryOp) -> Option<bool> {
    if left.is_null() || right.is_null() {
        return Some(false);
    }
    let ordering = match (left, right) {
        (Value::Int16(l), Value::Int16(r)) => l.cmp(r),
        (Value::Int32(l), Value::Int32(r)) => l.cmp(r),
        (Value::Int64(l), Value::Int64(r)) => l.cmp(r),
        (Value::Oid(l), Value::Oid(r))
        | (Value::RegClass(l), Value::RegClass(r))
        | (Value::RegType(l), Value::RegType(r)) => l.cmp(r),
        (Value::PgLsn(l), Value::PgLsn(r)) => l.cmp(r),
        (Value::Date(l), Value::Date(r)) => l.cmp(r),
        (Value::Time(l), Value::Time(r)) => l.cmp(r),
        (
            Value::TimeTz {
                micros: lm,
                offset_seconds: lo,
            },
            Value::TimeTz {
                micros: rm,
                offset_seconds: ro,
            },
        ) => timetz_utc_micros(*lm, *lo).cmp(&timetz_utc_micros(*rm, *ro)),
        (Value::Timestamp(l), Value::Timestamp(r))
        | (Value::TimestampTz(l), Value::TimestampTz(r))
        | (Value::Timestamp(l), Value::TimestampTz(r))
        | (Value::TimestampTz(l), Value::Timestamp(r)) => l.cmp(r),
        (Value::Text(l), Value::Text(r)) => l.cmp(r),
        (Value::Char(l), Value::Char(r)) => bpchar_semantic_text(l).cmp(bpchar_semantic_text(r)),
        (Value::Char(l), Value::Text(r)) => bpchar_semantic_text(l).cmp(r),
        (Value::Text(l), Value::Char(r)) => l.as_str().cmp(bpchar_semantic_text(r)),
        (Value::BitString(l), Value::BitString(r)) => l.to_bit_text().cmp(&r.to_bit_text()),
        (Value::Network(l), Value::Network(r)) => (*l)
            .cmp_network(*r)
            .unwrap_or_else(|| l.to_string().cmp(&r.to_string())),
        (Value::Bool(l), Value::Bool(r)) => l.cmp(r),
        (
            Value::Decimal {
                value: left_value,
                scale: left_scale,
            },
            Value::Decimal {
                value: right_value,
                scale: right_scale,
            },
        ) if left_scale == right_scale => left_value.cmp(right_value),
        _ => return None,
    };
    Some(match op {
        BinaryOp::Eq => ordering == std::cmp::Ordering::Equal,
        BinaryOp::NotEq => ordering != std::cmp::Ordering::Equal,
        BinaryOp::Lt => ordering == std::cmp::Ordering::Less,
        BinaryOp::LtEq => ordering != std::cmp::Ordering::Greater,
        BinaryOp::Gt => ordering == std::cmp::Ordering::Greater,
        BinaryOp::GtEq => ordering != std::cmp::Ordering::Less,
        _ => return None,
    })
}

// ---------------------------------------------------------------------------
// Hash-map key wrapper
// ---------------------------------------------------------------------------

#[derive(Debug, Eq, PartialEq, Hash)]
enum JoinKey {
    Single(OrderedValue),
    Multi(Vec<OrderedValue>),
}

fn build_join_key(evals: &[Eval], row: &[Value]) -> Result<Option<JoinKey>, ExecError> {
    if let [eval] = evals {
        let value = eval.eval(row).map_err(eval_error_to_exec_error)?;
        if value.is_null() {
            return Ok(None);
        }
        return Ok(Some(JoinKey::Single(OrderedValue(value))));
    }

    let mut values = Vec::with_capacity(evals.len());
    for eval in evals {
        let value = eval.eval(row).map_err(eval_error_to_exec_error)?;
        if value.is_null() {
            return Ok(None);
        }
        values.push(OrderedValue(value));
    }
    Ok(Some(JoinKey::Multi(values)))
}

/// A wrapper around [`Value`] that implements `Hash + Eq` so it can serve
/// as a `HashMap` key.
///
/// `Value` itself does not implement `Hash` because `f32`/`f64` are not
/// `Hash` (NaN != NaN). We implement an approximate hash that is consistent
/// with the join semantics: NaN values compare equal to themselves here.
#[derive(Clone, Debug)]
struct OrderedValue(Value);

impl PartialEq for OrderedValue {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            // Bit-pattern equality for floats so NaN == NaN in hash tables.
            (Value::Float32(a), Value::Float32(b)) => a.to_bits() == b.to_bits(),
            (Value::Float64(a), Value::Float64(b)) => a.to_bits() == b.to_bits(),
            (Value::Vector(a), Value::Vector(b)) | (Value::HalfVec(a), Value::HalfVec(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(l, r)| l.to_bits() == r.to_bits())
            }
            (
                Value::Decimal {
                    value: left_value,
                    scale: left_scale,
                },
                Value::Decimal {
                    value: right_value,
                    scale: right_scale,
                },
            ) => decimal_values_equal(*left_value, *left_scale, *right_value, *right_scale),
            (Value::Char(a), Value::Text(b)) => bpchar_semantic_text(a) == b,
            (Value::Text(a), Value::Char(b)) => a == bpchar_semantic_text(b),
            _ => self.0 == other.0,
        }
    }
}

impl Eq for OrderedValue {}

impl std::hash::Hash for OrderedValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match &self.0 {
            Value::Null => state.write_u8(0),
            Value::Bool(b) => {
                state.write_u8(1);
                b.hash(state);
            }
            Value::Int16(v) => {
                state.write_u8(2);
                v.hash(state);
            }
            Value::Int32(v) => {
                state.write_u8(3);
                v.hash(state);
            }
            Value::Int64(v) => {
                state.write_u8(4);
                v.hash(state);
            }
            Value::Money(v) => {
                state.write_u8(23);
                v.hash(state);
            }
            Value::Oid(v) => {
                state.write_u8(27);
                v.hash(state);
            }
            Value::RegClass(v) => {
                state.write_u8(28);
                v.hash(state);
            }
            Value::RegType(v) => {
                state.write_u8(29);
                v.hash(state);
            }
            Value::PgLsn(v) => {
                state.write_u8(30);
                v.hash(state);
            }
            Value::Float32(v) => {
                state.write_u8(5);
                // Hash the bit pattern so NaN is stable.
                v.to_bits().hash(state);
            }
            Value::Float64(v) => {
                state.write_u8(6);
                v.to_bits().hash(state);
            }
            Value::Text(s) => {
                state.write_u8(7);
                s.hash(state);
            }
            Value::Char(s) => {
                state.write_u8(7);
                bpchar_semantic_text(s).hash(state);
            }
            Value::Json(s) => {
                state.write_u8(16);
                s.hash(state);
            }
            Value::Jsonb(s) => {
                state.write_u8(17);
                s.hash(state);
            }
            Value::Xml(s) => {
                state.write_u8(31);
                s.hash(state);
            }
            Value::Bytea(b) => {
                state.write_u8(8);
                b.hash(state);
            }
            Value::Timestamp(v) | Value::TimestampTz(v) | Value::Time(v) => {
                state.write_u8(9);
                v.hash(state);
            }
            Value::TimeTz {
                micros,
                offset_seconds,
            } => {
                state.write_u8(9);
                timetz_utc_micros(*micros, *offset_seconds).hash(state);
            }
            Value::Date(v) => {
                state.write_u8(10);
                v.hash(state);
            }
            Value::Uuid(u) => {
                state.write_u8(11);
                u.hash(state);
            }
            Value::Decimal { value, scale } => {
                state.write_u8(12);
                hash_decimal_key(state, *value, *scale);
            }
            Value::Interval {
                months,
                days,
                microseconds,
            } => {
                state.write_u8(13);
                months.hash(state);
                days.hash(state);
                microseconds.hash(state);
            }
            Value::Range(v) => {
                state.write_u8(14);
                v.hash(state);
            }
            Value::Geometry(v) => {
                state.write_u8(15);
                v.hash(state);
            }
            Value::Array {
                element_type,
                elements,
            } => {
                state.write_u8(18);
                element_type.hash(state);
                elements.hash(state);
            }
            Value::Vector(values) | Value::HalfVec(values) => {
                state.write_u8(19);
                for value in values {
                    value.to_bits().hash(state);
                }
            }
            Value::SparseVec(value) => {
                state.write_u8(20);
                value.hash(state);
            }
            Value::BitVec { dims, bytes } => {
                state.write_u8(21);
                dims.hash(state);
                bytes.hash(state);
            }
            Value::BitString(bits) => {
                state.write_u8(25);
                bits.hash(state);
            }
            Value::Network(network) => {
                state.write_u8(26);
                network.hash(state);
            }
            Value::Record(fields) => {
                state.write_u8(22);
                fields.hash(state);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

fn concat_rows(left: &[Value], right: &[Value]) -> Vec<Value> {
    let mut row = Vec::with_capacity(left.len() + right.len());
    row.extend_from_slice(left);
    row.extend_from_slice(right);
    row
}

fn encode_spill_row(codec: &RowCodec, row: &[Value]) -> Result<Vec<u8>, ExecError> {
    codec
        .encode(row)
        .map_err(|error| ExecError::TypeMismatch(format!("hash join spill encode failed: {error}")))
}

fn spill_io_error(action: &str, error: std::io::Error) -> ExecError {
    ExecError::TypeMismatch(format!("hash join spill {action} failed: {error}"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::{BinaryOp, LogicalJoinType, ScalarExpr};
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::HashJoin;
    use crate::mem_table_scan::MemTableScan;
    use crate::{ExecError, Operator, WorkMemBudget};

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    fn schema_id() -> Schema {
        Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok")
    }

    fn schema_val() -> Schema {
        Schema::new([Field::required("val", DataType::Int32)]).expect("schema ok")
    }

    fn decimal_type(scale: i32) -> DataType {
        DataType::Decimal {
            precision: None,
            scale: Some(scale),
        }
    }

    fn schema_decimal(name: &str, scale: i32) -> Schema {
        Schema::new([Field::required(name, decimal_type(scale))]).expect("schema ok")
    }

    fn schema_joined_decimals(left_scale: i32, right_scale: i32) -> Schema {
        Schema::new([
            Field::required("id", decimal_type(left_scale)),
            Field::required("val", decimal_type(right_scale)),
        ])
        .expect("schema ok")
    }

    fn schema_id_val() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("val", DataType::Int32),
        ])
        .expect("schema ok")
    }

    fn schema_pair(prefix: &str) -> Schema {
        Schema::new([
            Field::required(format!("{prefix}_part"), DataType::Int32),
            Field::required(format!("{prefix}_supp"), DataType::Int32),
        ])
        .expect("schema ok")
    }

    fn schema_joined_pair() -> Schema {
        Schema::new([
            Field::required("left_part", DataType::Int32),
            Field::required("left_supp", DataType::Int32),
            Field::required("right_part", DataType::Int32),
            Field::required("right_supp", DataType::Int32),
        ])
        .expect("schema ok")
    }

    fn i32_batch(rows: &[i32]) -> Batch {
        Batch::new([Column::Int32(NumericColumn::from_data(rows.to_vec()))]).expect("batch ok")
    }

    fn decimal_batch(rows: &[i64]) -> Batch {
        Batch::new([Column::Int64(NumericColumn::from_data(rows.to_vec()))]).expect("batch ok")
    }

    fn i32_pair_batch(rows: &[(i32, i32)]) -> Batch {
        let first = rows.iter().map(|(a, _)| *a).collect::<Vec<_>>();
        let second = rows.iter().map(|(_, b)| *b).collect::<Vec<_>>();
        Batch::new([
            Column::Int32(NumericColumn::from_data(first)),
            Column::Int32(NumericColumn::from_data(second)),
        ])
        .expect("batch ok")
    }

    fn col_idx0_i32(name: &str) -> ScalarExpr {
        col_i32(name, 0)
    }

    fn col_i32(name: &str, index: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.into(),
            index,
            data_type: DataType::Int32,
        }
    }

    fn col_decimal(name: &str, index: usize, scale: i32) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.into(),
            index,
            data_type: decimal_type(scale),
        }
    }

    fn lit_i32(v: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            data_type: DataType::Int32,
        }
    }

    fn divide_i32_by_zero(name: &str) -> ScalarExpr {
        ScalarExpr::Binary {
            op: BinaryOp::Div,
            left: Box::new(col_idx0_i32(name)),
            right: Box::new(lit_i32(0)),
            data_type: DataType::Int32,
        }
    }

    fn drain_rows(op: &mut dyn Operator) -> Vec<(i32, i32)> {
        let schema = op.schema().clone();
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().expect("no error") {
            let rows = crate::filter_op::batch_to_rows(&b, &schema).expect("decode ok");
            for row in rows {
                // batch_to_rows now reports `Value::Null` for the null
                // probe-side rows produced by LEFT OUTER unmatched
                // padding (the underlying NumericColumn validity bitmap
                // distinguishes them from real zeros). Map back to 0
                // here so the test assertions stay readable.
                let l = match &row[0] {
                    Value::Int32(v) => *v,
                    Value::Null => 0,
                    _ => panic!("unexpected left value: {:?}", row[0]),
                };
                let r = match &row[1] {
                    Value::Int32(v) => *v,
                    Value::Null => 0,
                    _ => panic!("unexpected right value: {:?}", row[1]),
                };
                out.push((l, r));
            }
        }
        out
    }

    fn drain_single_i32(op: &mut dyn Operator) -> Vec<i32> {
        let schema = op.schema().clone();
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().expect("no error") {
            let rows = crate::filter_op::batch_to_rows(&b, &schema).expect("decode ok");
            for row in rows {
                match &row[0] {
                    Value::Int32(v) => out.push(*v),
                    other => panic!("unexpected value: {other:?}"),
                }
            }
        }
        out
    }

    fn drain_pair_i32(op: &mut dyn Operator) -> Vec<(i32, i32)> {
        let schema = op.schema().clone();
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().expect("no error") {
            let rows = crate::filter_op::batch_to_rows(&b, &schema).expect("decode ok");
            for row in rows {
                let left = match &row[0] {
                    Value::Int32(v) => *v,
                    other => panic!("unexpected first value: {other:?}"),
                };
                let right = match &row[1] {
                    Value::Int32(v) => *v,
                    other => panic!("unexpected second value: {other:?}"),
                };
                out.push((left, right));
            }
        }
        out
    }

    fn drain_decimal_pairs(op: &mut dyn Operator) -> Vec<((i64, i32), (i64, i32))> {
        let schema = op.schema().clone();
        let mut out = Vec::new();
        while let Some(batch) = op.next_batch().expect("no error") {
            let rows = crate::filter_op::batch_to_rows(&batch, &schema).expect("decode ok");
            for row in rows {
                let left = match &row[0] {
                    Value::Decimal { value, scale } => (*value, *scale),
                    other => panic!("unexpected left value: {other:?}"),
                };
                let right = match &row[1] {
                    Value::Decimal { value, scale } => (*value, *scale),
                    other => panic!("unexpected right value: {other:?}"),
                };
                out.push((left, right));
            }
        }
        out
    }

    // -------------------------------------------------------------------------
    // Test 1: INNER hash join happy path
    // -------------------------------------------------------------------------

    #[test]
    fn hash_join_inner_happy_path() {
        // left: [1, 2, 3], right: [2, 3, 4]
        // Matches: (2,2), (3,3)
        let left = MemTableScan::new(schema_id(), vec![i32_batch(&[1, 2, 3])]);
        let right = MemTableScan::new(schema_val(), vec![i32_batch(&[2, 3, 4])]);
        let mut op = HashJoin::new(
            Box::new(left),
            Box::new(right),
            col_idx0_i32("id"),
            col_idx0_i32("val"),
            LogicalJoinType::Inner,
            schema_id_val(),
            schema_id(),
            schema_val(),
        );
        let mut rows = drain_rows(&mut op);
        rows.sort_unstable();
        assert_eq!(rows, vec![(2, 2), (3, 3)]);
    }

    #[test]
    fn hash_join_matches_decimal_keys_across_scales() {
        let left_schema = schema_decimal("id", 1);
        let right_schema = schema_decimal("val", 0);
        let left = MemTableScan::new(left_schema.clone(), vec![decimal_batch(&[10, 25])]);
        let right = MemTableScan::new(right_schema.clone(), vec![decimal_batch(&[1, 3])]);
        let mut op = HashJoin::new(
            Box::new(left),
            Box::new(right),
            col_decimal("id", 0, 1),
            col_decimal("val", 0, 0),
            LogicalJoinType::Inner,
            schema_joined_decimals(1, 0),
            left_schema,
            right_schema,
        );

        assert_eq!(drain_decimal_pairs(&mut op), vec![((10, 1), (1, 0))]);
    }

    #[test]
    fn hash_join_build_key_eval_error_propagates() {
        let left = MemTableScan::new(schema_id(), vec![i32_batch(&[1])]);
        let right = MemTableScan::new(schema_val(), vec![i32_batch(&[1])]);
        let mut op = HashJoin::new(
            Box::new(left),
            Box::new(right),
            divide_i32_by_zero("id"),
            col_idx0_i32("val"),
            LogicalJoinType::Inner,
            schema_id_val(),
            schema_id(),
            schema_val(),
        );

        let err = op.next_batch().expect_err("build key division must error");
        assert!(
            err.to_string().contains("division by zero"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn hash_join_probe_key_eval_error_propagates() {
        let left = MemTableScan::new(schema_id(), vec![i32_batch(&[1])]);
        let right = MemTableScan::new(schema_val(), vec![i32_batch(&[1])]);
        let mut op = HashJoin::new(
            Box::new(left),
            Box::new(right),
            col_idx0_i32("id"),
            divide_i32_by_zero("val"),
            LogicalJoinType::Inner,
            schema_id_val(),
            schema_id(),
            schema_val(),
        );

        let err = op.next_batch().expect_err("probe key division must error");
        assert!(
            err.to_string().contains("division by zero"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn hash_join_spills_build_side_when_work_mem_is_too_small() {
        let left = MemTableScan::new(schema_id(), vec![i32_batch(&[1, 2, 3, 4])]);
        let right = MemTableScan::new(schema_val(), vec![i32_batch(&[2, 4, 9])]);
        let mut op = HashJoin::new(
            Box::new(left),
            Box::new(right),
            col_idx0_i32("id"),
            col_idx0_i32("val"),
            LogicalJoinType::Inner,
            schema_id_val(),
            schema_id(),
            schema_val(),
        )
        .with_work_mem_budget(std::sync::Arc::new(WorkMemBudget::new(1)));

        let mut rows = drain_rows(&mut op);
        rows.sort_unstable();
        assert_eq!(rows, vec![(2, 2), (4, 4)]);
        assert!(op.spilled_to_disk(), "build side must switch to spill mode");
    }

    #[test]
    fn hash_join_inner_composite_key() {
        let left_schema = schema_pair("left");
        let right_schema = schema_pair("right");
        let left = MemTableScan::new(
            left_schema.clone(),
            vec![i32_pair_batch(&[(1, 10), (1, 20), (2, 10)])],
        );
        let right = MemTableScan::new(
            right_schema.clone(),
            vec![i32_pair_batch(&[(1, 10), (1, 30), (2, 10)])],
        );
        let mut op = HashJoin::new_multi(
            Box::new(left),
            Box::new(right),
            vec![col_i32("left_part", 0), col_i32("left_supp", 1)],
            vec![col_i32("right_part", 0), col_i32("right_supp", 1)],
            LogicalJoinType::Inner,
            schema_joined_pair(),
            left_schema,
            right_schema,
        );

        let schema = op.schema().clone();
        let mut rows = Vec::new();
        while let Some(batch) = op.next_batch().expect("no error") {
            for row in crate::filter_op::batch_to_rows(&batch, &schema).expect("decode ok") {
                let values = row
                    .into_iter()
                    .map(|value| match value {
                        Value::Int32(v) => v,
                        other => panic!("unexpected value: {other:?}"),
                    })
                    .collect::<Vec<_>>();
                rows.push((values[0], values[1], values[2], values[3]));
            }
        }
        rows.sort_unstable();
        assert_eq!(rows, vec![(1, 10, 1, 10), (2, 10, 2, 10)]);
    }

    #[test]
    fn hash_join_streams_large_output_across_batches() {
        let left = MemTableScan::new(schema_id(), vec![i32_batch(&(0..5000).collect::<Vec<_>>())]);
        let right = MemTableScan::new(
            schema_val(),
            vec![i32_batch(&(0..5000).collect::<Vec<_>>())],
        );
        let mut op = HashJoin::new(
            Box::new(left),
            Box::new(right),
            col_idx0_i32("id"),
            col_idx0_i32("val"),
            LogicalJoinType::Inner,
            schema_id_val(),
            schema_id(),
            schema_val(),
        );

        let rows = drain_rows(&mut op);
        assert_eq!(rows.len(), 5000);
        assert_eq!(rows.first(), Some(&(0, 0)));
        assert_eq!(rows.last(), Some(&(4999, 4999)));
    }

    // -------------------------------------------------------------------------
    // Test 2: empty build side returns no rows
    // -------------------------------------------------------------------------

    #[test]
    fn hash_join_empty_left_returns_no_rows() {
        let left = MemTableScan::new(schema_id(), vec![]);
        let right = MemTableScan::new(schema_val(), vec![i32_batch(&[1, 2, 3])]);
        let mut op = HashJoin::new(
            Box::new(left),
            Box::new(right),
            col_idx0_i32("id"),
            col_idx0_i32("val"),
            LogicalJoinType::Inner,
            schema_id_val(),
            schema_id(),
            schema_val(),
        );
        assert!(drain_rows(&mut op).is_empty());
    }

    // -------------------------------------------------------------------------
    // Test 3: LEFT OUTER — unmatched left rows emit NULL right
    // -------------------------------------------------------------------------

    #[test]
    fn hash_join_left_outer_unmatched_rows() {
        // left: [1, 2], right: [2]
        // Inner match: (2,2). LeftOuter also emits: (1, NULL)
        let left = MemTableScan::new(schema_id(), vec![i32_batch(&[1, 2])]);
        let right = MemTableScan::new(schema_val(), vec![i32_batch(&[2])]);
        let mut op = HashJoin::new(
            Box::new(left),
            Box::new(right),
            col_idx0_i32("id"),
            col_idx0_i32("val"),
            LogicalJoinType::LeftOuter,
            schema_id_val(),
            schema_id(),
            schema_val(),
        );
        let mut rows = drain_rows(&mut op);
        rows.sort_unstable();
        assert!(rows.contains(&(2, 2)), "matched pair present");
        // build_batch encodes NULL as 0 for Int32 columns (v0.5 no-null-bitmap
        // format), so the decoded sentinel is 0, not i32::MIN.
        assert!(
            rows.contains(&(1, 0)),
            "unmatched left row with NULL right (encoded as 0)"
        );
    }

    #[test]
    fn hash_join_semi_emits_each_matching_left_row_once() {
        let left = MemTableScan::new(schema_id(), vec![i32_batch(&[1, 2, 2, 3])]);
        let right = MemTableScan::new(schema_val(), vec![i32_batch(&[2, 2, 4])]);
        let mut op = HashJoin::new(
            Box::new(left),
            Box::new(right),
            col_idx0_i32("id"),
            col_idx0_i32("val"),
            LogicalJoinType::Semi,
            schema_id(),
            schema_id(),
            schema_val(),
        );
        let mut rows = drain_single_i32(&mut op);
        rows.sort_unstable();
        assert_eq!(rows, vec![2, 2]);
    }

    #[test]
    fn hash_join_anti_emits_unmatched_left_rows() {
        let left = MemTableScan::new(schema_id(), vec![i32_batch(&[1, 2, 3])]);
        let right = MemTableScan::new(schema_val(), vec![i32_batch(&[2, 4])]);
        let mut op = HashJoin::new(
            Box::new(left),
            Box::new(right),
            col_idx0_i32("id"),
            col_idx0_i32("val"),
            LogicalJoinType::Anti,
            schema_id(),
            schema_id(),
            schema_val(),
        );
        let mut rows = drain_single_i32(&mut op);
        rows.sort_unstable();
        assert_eq!(rows, vec![1, 3]);
    }

    #[test]
    fn hash_join_semi_residual_filters_candidate_pairs() {
        let left_schema = schema_pair("left");
        let right_schema = schema_pair("right");
        let left = MemTableScan::new(
            left_schema.clone(),
            vec![i32_pair_batch(&[(1, 10), (2, 30)])],
        );
        let right = MemTableScan::new(
            right_schema.clone(),
            vec![i32_pair_batch(&[(1, 10), (1, 20), (2, 30)])],
        );
        let residual = ScalarExpr::Binary {
            op: BinaryOp::NotEq,
            left: Box::new(col_i32("left_supp", 1)),
            right: Box::new(col_i32("right_supp", 3)),
            data_type: DataType::Bool,
        };
        let mut op = HashJoin::new_multi_with_residual(
            Box::new(left),
            Box::new(right),
            vec![col_i32("left_part", 0)],
            vec![col_i32("right_part", 0)],
            Some(residual),
            LogicalJoinType::Semi,
            left_schema.clone(),
            left_schema,
            right_schema,
        );

        assert_eq!(drain_pair_i32(&mut op), vec![(1, 10)]);
    }

    #[test]
    fn hash_join_anti_residual_keeps_rows_without_residual_match() {
        let left_schema = schema_pair("left");
        let right_schema = schema_pair("right");
        let left = MemTableScan::new(
            left_schema.clone(),
            vec![i32_pair_batch(&[(1, 10), (2, 30)])],
        );
        let right = MemTableScan::new(
            right_schema.clone(),
            vec![i32_pair_batch(&[(1, 10), (1, 20), (2, 30)])],
        );
        let residual = ScalarExpr::Binary {
            op: BinaryOp::NotEq,
            left: Box::new(col_i32("left_supp", 1)),
            right: Box::new(col_i32("right_supp", 3)),
            data_type: DataType::Bool,
        };
        let mut op = HashJoin::new_multi_with_residual(
            Box::new(left),
            Box::new(right),
            vec![col_i32("left_part", 0)],
            vec![col_i32("right_part", 0)],
            Some(residual),
            LogicalJoinType::Anti,
            left_schema.clone(),
            left_schema,
            right_schema,
        );

        assert_eq!(drain_pair_i32(&mut op), vec![(2, 30)]);
    }

    #[test]
    fn hash_join_right_build_semi_emits_each_matching_left_row_once() {
        let left = MemTableScan::new(schema_id(), vec![i32_batch(&[1, 2, 2, 3])]);
        let right = MemTableScan::new(schema_val(), vec![i32_batch(&[2, 2, 4])]);
        let mut op = HashJoin::new_multi_with_residual_build_right(
            Box::new(left),
            Box::new(right),
            vec![col_idx0_i32("id")],
            vec![col_idx0_i32("val")],
            None,
            LogicalJoinType::Semi,
            schema_id(),
            schema_id(),
            schema_val(),
        );
        let mut rows = drain_single_i32(&mut op);
        rows.sort_unstable();
        assert_eq!(rows, vec![2, 2]);
    }

    #[test]
    fn hash_join_right_build_anti_residual_keeps_rows_without_match() {
        let left_schema = schema_pair("left");
        let right_schema = schema_pair("right");
        let left = MemTableScan::new(
            left_schema.clone(),
            vec![i32_pair_batch(&[(1, 10), (2, 30)])],
        );
        let right = MemTableScan::new(
            right_schema.clone(),
            vec![i32_pair_batch(&[(1, 10), (1, 20), (2, 30)])],
        );
        let residual = ScalarExpr::Binary {
            op: BinaryOp::NotEq,
            left: Box::new(col_i32("left_supp", 1)),
            right: Box::new(col_i32("right_supp", 3)),
            data_type: DataType::Bool,
        };
        let mut op = HashJoin::new_multi_with_residual_build_right(
            Box::new(left),
            Box::new(right),
            vec![col_i32("left_part", 0)],
            vec![col_i32("right_part", 0)],
            Some(residual),
            LogicalJoinType::Anti,
            left_schema.clone(),
            left_schema,
            right_schema,
        );

        assert_eq!(drain_pair_i32(&mut op), vec![(2, 30)]);
    }

    // -------------------------------------------------------------------------
    // Test 4: duplicate build keys — multiple matches emitted
    // -------------------------------------------------------------------------

    #[test]
    fn hash_join_duplicate_build_keys_all_emitted() {
        // left: [2, 2, 3], right: [2, 3]
        // Matches: (2,2), (2,2) (two from left), (3,3)
        let left = MemTableScan::new(schema_id(), vec![i32_batch(&[2, 2, 3])]);
        let right = MemTableScan::new(schema_val(), vec![i32_batch(&[2, 3])]);
        let mut op = HashJoin::new(
            Box::new(left),
            Box::new(right),
            col_idx0_i32("id"),
            col_idx0_i32("val"),
            LogicalJoinType::Inner,
            schema_id_val(),
            schema_id(),
            schema_val(),
        );
        let mut rows = drain_rows(&mut op);
        rows.sort_unstable();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], (2, 2));
        assert_eq!(rows[1], (2, 2));
        assert_eq!(rows[2], (3, 3));
    }

    // -------------------------------------------------------------------------
    // Test 5: unsupported join types return ExecError::Unsupported
    // -------------------------------------------------------------------------

    #[test]
    fn hash_join_right_outer_returns_unsupported() {
        let left = MemTableScan::new(schema_id(), vec![]);
        let right = MemTableScan::new(schema_val(), vec![]);
        let mut op = HashJoin::new(
            Box::new(left),
            Box::new(right),
            col_idx0_i32("id"),
            col_idx0_i32("val"),
            LogicalJoinType::RightOuter,
            schema_id_val(),
            schema_id(),
            schema_val(),
        );
        let err = op.next_batch().expect_err("RightOuter must error");
        assert!(matches!(err, ExecError::Unsupported(_)), "got {err:?}");
    }
}
