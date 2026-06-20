//! Construction surface for [`ModifyTable`]: the `new` constructor, the
//! fluent `with_*` configuration methods, the affected-row helpers, and
//! the operator's `Debug` implementation.

use std::sync::Arc;

use ultrasql_core::{DataType, Field, RelationId, Schema};
use ultrasql_planner::ScalarExpr;
use ultrasql_storage::PageLoader;
use ultrasql_storage::heap::HeapAccess;
use ultrasql_storage::vm::VisibilityMap;
use ultrasql_storage::wal_sink::WalSink;

use super::{
    CheckEvaluator, InsertConflictAction, InsertIndexMaintainer, ModifyKind, ModifyTable,
    ModifyTableStamps, RowConstraintCheck, RowUpdateConstraintCheck, SequenceDefault,
    VectorIndexMaintainer,
};
use crate::eval::Eval;
use crate::row_codec::RowCodec;
use crate::{ExecError, Operator};

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
        match Schema::new([Field::required("affected_rows", DataType::Int64)]) {
            Ok(schema) => schema,
            Err(err) => {
                tracing::error!(error = %err, "modify affected_rows schema failed");
                Schema::empty()
            }
        }
    }

    pub(crate) fn add_affected_rows(&mut self, rows: usize) -> Result<(), ExecError> {
        let delta = i64::try_from(rows).map_err(|_| {
            ExecError::NumericFieldOverflow("DML affected row count overflow".to_owned())
        })?;
        self.affected = self.affected.checked_add(delta).ok_or_else(|| {
            ExecError::NumericFieldOverflow("DML affected row count overflow".to_owned())
        })?;
        Ok(())
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
    /// - `stamps` — MVCC metadata to stamp on inserted/deleted tuple versions.
    /// - `wal` — optional WAL sink; `None` skips WAL emission.
    /// - `child` — source operator.
    #[must_use]
    pub fn new(
        heap: Arc<HeapAccess<L>>,
        target_relation: RelationId,
        target_schema: Schema,
        kind: ModifyKind,
        stamps: ModifyTableStamps,
        wal: Option<Arc<dyn WalSink>>,
        child: Box<dyn Operator>,
    ) -> Self {
        // Build per-assignment evaluators once at construction so the
        // per-row UPDATE loop does not pay the `ScalarExpr::clone()`
        // and evaluator-allocation cost on every iteration. For
        // INSERT and DELETE this is empty.
        let update_evaluators: Vec<(usize, Eval)> = match &kind {
            ModifyKind::Update { assignments } => assignments
                .iter()
                .map(|(col, expr)| (*col, Eval::new(expr.clone())))
                .collect(),
            ModifyKind::Insert | ModifyKind::Delete | ModifyKind::Merge { .. } => Vec::new(),
        };
        let update_fast_path = match &kind {
            ModifyKind::Update { assignments } => {
                super::helpers::detect_update_int32_pair_fast_path(assignments, &target_schema)
            }
            ModifyKind::Insert | ModifyKind::Delete | ModifyKind::Merge { .. } => None,
        };
        Self {
            heap,
            relation: target_relation,
            schema: Self::affected_rows_schema(),
            codec: RowCodec::new(target_schema),
            kind,
            update_evaluators,
            update_extra_eval_columns: false,
            update_fast_path,
            insert_xmin: stamps.insert_xmin,
            insert_command_id: stamps.insert_command_id,
            delete_xmax: stamps.delete_xmax,
            delete_cmax: stamps.delete_cmax,
            wal,
            vm: None,
            insert_indexes: Vec::new(),
            update_indexes: Vec::new(),
            delete_indexes: Vec::new(),
            insert_vector_indexes: Vec::new(),
            update_vector_indexes: Vec::new(),
            delete_vector_indexes: Vec::new(),
            insert_conflict_action: None,
            insert_column_map: None,
            column_defaults: Vec::new(),
            sequence_defaults: Vec::new(),
            identity_always: Vec::new(),
            generated_stored: Vec::new(),
            check_constraints: Vec::new(),
            foreign_key_checks: Vec::new(),
            exclusion_checks: Vec::new(),
            exclusion_update_checks: Vec::new(),
            referenced_by_delete_checks: Vec::new(),
            referenced_by_update_checks: Vec::new(),
            returning_evaluators: Vec::new(),
            child,
            done: false,
            affected: 0,
        }
    }

    /// Attach the server-owned visibility map so heap mutations clear
    /// all-visible bits for touched pages.
    #[must_use]
    pub fn with_visibility_map(mut self, vm: Arc<VisibilityMap>) -> Self {
        self.vm = Some(vm);
        self
    }

    /// Attach B-tree index maintainers used by the INSERT arm.
    ///
    /// The operator updates these indexes after the heap batch returns
    /// the inserted tuple IDs. Duplicate key checks run before the heap
    /// write so statement-level rejection remains atomic for this path.
    #[must_use]
    pub fn with_insert_indexes(mut self, indexes: Vec<InsertIndexMaintainer<L>>) -> Self {
        self.insert_indexes = indexes;
        self
    }

    /// Attach B-tree index maintainers used by the UPDATE arm.
    #[must_use]
    pub fn with_update_indexes(mut self, indexes: Vec<InsertIndexMaintainer<L>>) -> Self {
        self.update_indexes = indexes;
        self
    }

    /// Allow UPDATE child rows to append expression-only columns after
    /// the target row image.
    ///
    /// The heap update still writes only the target-width prefix. The
    /// full post-TID row is visible to assignment evaluators.
    #[must_use]
    pub fn with_update_extra_eval_columns(mut self) -> Self {
        self.update_extra_eval_columns = true;
        self
    }

    /// Attach B-tree index maintainers used by the DELETE arm.
    #[must_use]
    pub fn with_delete_indexes(mut self, indexes: Vec<InsertIndexMaintainer<L>>) -> Self {
        self.delete_indexes = indexes;
        self
    }

    /// Attach HNSW vector-index maintainers used by the INSERT arm.
    #[must_use]
    pub fn with_insert_vector_indexes(mut self, indexes: Vec<VectorIndexMaintainer>) -> Self {
        self.insert_vector_indexes = indexes;
        self
    }

    /// Attach HNSW vector-index maintainers used by the UPDATE arm.
    #[must_use]
    pub fn with_update_vector_indexes(mut self, indexes: Vec<VectorIndexMaintainer>) -> Self {
        self.update_vector_indexes = indexes;
        self
    }

    /// Attach HNSW vector-index maintainers used by the DELETE arm.
    #[must_use]
    pub fn with_delete_vector_indexes(mut self, indexes: Vec<VectorIndexMaintainer>) -> Self {
        self.delete_vector_indexes = indexes;
        self
    }

    /// Attach `INSERT ... ON CONFLICT` behavior.
    #[must_use]
    pub fn with_insert_conflict_action(mut self, action: InsertConflictAction) -> Self {
        self.insert_conflict_action = Some(action);
        self
    }

    /// Attach a source-to-target column map for INSERT.
    ///
    /// `map[src_idx] = target_idx`. Target columns omitted by `map`
    /// are filled with [`Value::Null`] before NOT NULL checks, index
    /// key encoding, and heap row encoding run.
    #[must_use]
    pub fn with_insert_column_map(mut self, map: Vec<usize>) -> Self {
        self.insert_column_map = Some(map);
        self
    }

    /// Attach per-column DEFAULT expressions evaluated for omitted
    /// INSERT columns.
    #[must_use]
    pub fn with_column_defaults(mut self, defaults: Vec<Option<ScalarExpr>>) -> Self {
        self.column_defaults = defaults
            .into_iter()
            .map(|expr| expr.map(Eval::new))
            .collect();
        self
    }

    /// Attach per-column sequence-backed defaults.
    #[must_use]
    pub fn with_sequence_defaults(mut self, defaults: Vec<Option<SequenceDefault>>) -> Self {
        self.sequence_defaults = defaults;
        self
    }

    /// Attach per-column `GENERATED ALWAYS AS IDENTITY` flags.
    #[must_use]
    pub fn with_identity_always(mut self, identity_always: Vec<bool>) -> Self {
        self.identity_always = identity_always;
        self
    }

    /// Attach per-column stored generated expressions.
    #[must_use]
    pub fn with_generated_stored(mut self, generated: Vec<Option<ScalarExpr>>) -> Self {
        self.generated_stored = generated
            .into_iter()
            .map(|expr| expr.map(Eval::new))
            .collect();
        self
    }

    /// Attach row-level CHECK constraints evaluated for INSERT/UPDATE.
    #[must_use]
    pub fn with_check_constraints(mut self, checks: Vec<(String, ScalarExpr)>) -> Self {
        self.check_constraints = checks
            .into_iter()
            .map(|(name, expr)| CheckEvaluator {
                name,
                evaluator: Eval::new(expr),
            })
            .collect();
        self
    }

    /// Attach FOREIGN KEY checks for rows written by INSERT/UPDATE.
    #[must_use]
    pub fn with_foreign_key_checks(mut self, checks: Vec<RowConstraintCheck>) -> Self {
        self.foreign_key_checks = checks;
        self
    }

    /// Attach EXCLUDE checks for rows written by INSERT.
    #[must_use]
    pub fn with_exclusion_checks(mut self, checks: Vec<RowConstraintCheck>) -> Self {
        self.exclusion_checks = checks;
        self
    }

    /// Attach EXCLUDE checks for rows written by UPDATE.
    #[must_use]
    pub fn with_exclusion_update_checks(mut self, checks: Vec<RowUpdateConstraintCheck>) -> Self {
        self.exclusion_update_checks = checks;
        self
    }

    /// Attach RESTRICT/NO ACTION checks for parent rows deleted by this operator.
    #[must_use]
    pub fn with_referenced_by_delete_checks(mut self, checks: Vec<RowConstraintCheck>) -> Self {
        self.referenced_by_delete_checks = checks;
        self
    }

    /// Attach RESTRICT/NO ACTION checks for parent key UPDATEs.
    #[must_use]
    pub fn with_referenced_by_update_checks(
        mut self,
        checks: Vec<RowUpdateConstraintCheck>,
    ) -> Self {
        self.referenced_by_update_checks = checks;
        self
    }

    /// Replace the default affected-row output with a `RETURNING`
    /// projection evaluated over the row image the mutation exposes:
    /// inserted row for INSERT, updated row for UPDATE, old row for
    /// DELETE.
    #[must_use]
    pub fn with_returning(mut self, exprs: Vec<ScalarExpr>, schema: Schema) -> Self {
        self.returning_evaluators = exprs.into_iter().map(Eval::new).collect();
        self.schema = schema;
        self
    }
}
