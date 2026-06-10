//! Table constraint definitions and the runtime checker.
//!
//! Constraints are the SQL-level integrity rules that every mutation
//! (INSERT, UPDATE, DELETE) must satisfy before committing. This module
//! defines the canonical in-memory representation of every PostgreSQL-
//! compatible constraint kind and a [`ConstraintChecker`] that enforces
//! them at DML time.
//!
//! # Layered position
//!
//! `constraints.rs` sits above the heap and below the executor. The
//! executor calls [`ConstraintChecker::check_insert`],
//! [`ConstraintChecker::check_update`], and
//! [`ConstraintChecker::check_delete`] before committing a row.
//!
//! # Foreign key enforcement
//!
//! FK checks use the [`FkParentLookup`] callback supplied at
//! construction.  In production this callback performs a B-tree key
//! lookup; tests supply a closure over an in-memory set.
//!
//! # Deferred constraints
//!
//! `DEFERRABLE INITIALLY DEFERRED` FK constraints are not checked
//! immediately; the checker skips them on immediate DML checks. Callers
//! that buffer deferred FK events must call
//! [`ConstraintChecker::check_deferred_insert`],
//! [`ConstraintChecker::check_deferred_update`], and
//! [`ConstraintChecker::check_deferred_delete`] during their commit-time
//! deferred pass.

use std::collections::HashSet;
use std::fmt;

use parking_lot::Mutex;
use thiserror::Error;
use ultrasql_core::{Oid, Value};

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors raised when a constraint is violated.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ConstraintError {
    /// A NOT NULL constraint was violated.
    #[error("null value violates NOT NULL constraint on column {column_index}")]
    NotNull {
        /// 0-based column index.
        column_index: usize,
    },

    /// A CHECK constraint expression evaluated to false or NULL.
    #[error("check constraint violated: {name}")]
    CheckFailed {
        /// User-visible constraint name.
        name: String,
    },

    /// A UNIQUE or PRIMARY KEY constraint was violated.
    #[error("unique constraint violated: duplicate key")]
    UniqueViolation,

    /// A FOREIGN KEY constraint was violated on the referencing side.
    #[error("foreign key violation: key does not exist in referenced table")]
    ForeignKeyViolation,

    /// A FOREIGN KEY referential action could not complete.
    #[error("foreign key referential action failed: {0}")]
    ReferentialAction(String),
}

// ---------------------------------------------------------------------------
// Referential action
// ---------------------------------------------------------------------------

/// Action to take when the referenced row is updated or deleted.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ReferentialAction {
    /// Delete / update matching rows in the referencing table.
    Cascade,
    /// Set the referencing columns to NULL.
    SetNull,
    /// Set the referencing columns to their declared DEFAULT.
    SetDefault,
    /// Raise an error if any referencing row exists.
    Restrict,
    /// Like `Restrict` but checked at end of statement.
    NoAction,
}

impl fmt::Display for ReferentialAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Cascade => "CASCADE",
            Self::SetNull => "SET NULL",
            Self::SetDefault => "SET DEFAULT",
            Self::Restrict => "RESTRICT",
            Self::NoAction => "NO ACTION",
        };
        f.write_str(s)
    }
}

// ---------------------------------------------------------------------------
// ScalarExpr: a callable expression over a row
// ---------------------------------------------------------------------------

/// Type alias for an expression evaluation function.
///
/// Receives a row slice and returns `Some(Value)` on success, `None`
/// when the expression evaluates to SQL NULL.
pub type EvalFn = Box<dyn Fn(&[Value]) -> Option<Value> + Send + Sync>;

/// An expression that can be applied to a row.
///
/// Used by `Default`, `Check`, and `GeneratedAlwaysAsStored`
/// constraints. The evaluation function is a closure capturing whatever
/// constants the expression needs; the `display` field carries a
/// human-readable form for EXPLAIN and error messages.
pub struct ScalarExpr {
    /// Evaluation function.
    pub compute: EvalFn,
    /// Human-readable expression text.
    pub display: String,
}

impl fmt::Debug for ScalarExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ScalarExpr({:?})", self.display)
    }
}

impl ScalarExpr {
    /// Construct a `ScalarExpr` from a closure and a display string.
    pub fn new<F>(compute: F, display: impl Into<String>) -> Self
    where
        F: Fn(&[Value]) -> Option<Value> + Send + Sync + 'static,
    {
        Self {
            compute: Box::new(compute),
            display: display.into(),
        }
    }

    /// Run the expression against `row`.
    #[inline]
    pub fn apply(&self, row: &[Value]) -> Option<Value> {
        (self.compute)(row)
    }
}

// ---------------------------------------------------------------------------
// Constraint kinds
// ---------------------------------------------------------------------------

/// A single constraint attached to a relation.
#[derive(Debug)]
pub enum Constraint {
    /// Column must not be SQL NULL.
    NotNull {
        /// 0-based column index.
        column: usize,
    },

    /// Column has a default value expression applied on INSERT when the
    /// column is absent.
    Default {
        /// 0-based column index.
        column: usize,
        /// Expression applied with an empty row context.
        expr: ScalarExpr,
    },

    /// Row-level CHECK expression. Violated when the expression is
    /// `false` or SQL NULL.
    Check {
        /// User-visible constraint name.
        name: String,
        /// Boolean-typed expression.
        expr: ScalarExpr,
    },

    /// UNIQUE key constraint on one or more columns.
    Unique {
        /// 0-based column indices.
        columns: Vec<usize>,
    },

    /// PRIMARY KEY constraint: NOT NULL + UNIQUE on the named columns.
    PrimaryKey {
        /// 0-based column indices.
        columns: Vec<usize>,
    },

    /// FOREIGN KEY constraint.
    ForeignKey {
        /// 0-based column indices on the referencing (child) side.
        columns: Vec<usize>,
        /// OID of the referenced (parent) table.
        target_rel: Oid,
        /// 0-based column indices on the referenced (parent) side.
        target_columns: Vec<usize>,
        /// Action when the referenced row is deleted.
        on_delete: ReferentialAction,
        /// Action when the referenced row is updated.
        on_update: ReferentialAction,
        /// Whether this constraint is deferrable.
        deferrable: bool,
        /// If deferrable, whether it starts in DEFERRED mode.
        initially_deferred: bool,
    },

    /// EXCLUDE constraint (GiST-based). `TODO(exclude-complete)`.
    Exclude,

    /// GENERATED ALWAYS AS IDENTITY.
    GeneratedAlwaysAsIdentity {
        /// 0-based column index.
        column: usize,
    },

    /// GENERATED BY DEFAULT AS IDENTITY.
    GeneratedByDefaultAsIdentity {
        /// 0-based column index.
        column: usize,
    },

    /// GENERATED ALWAYS AS (expr) STORED: computed column value.
    GeneratedAlwaysAsStored {
        /// 0-based column index.
        column: usize,
        /// Expression applied over the rest of the new row.
        expr: ScalarExpr,
    },
}

// ---------------------------------------------------------------------------
// Callback types
// ---------------------------------------------------------------------------

/// Returns `true` when a parent row matching `key` exists in `rel`.
pub type FkParentLookup = Box<dyn Fn(Oid, &[Value]) -> bool + Send + Sync>;

/// Returns all child rows whose FK columns match the given parent `key`.
pub type FkChildLookup = Box<dyn Fn(Oid, &[Value]) -> Vec<Vec<Value>> + Send + Sync>;

// ---------------------------------------------------------------------------
// ConstraintChecker
// ---------------------------------------------------------------------------

/// Runtime constraint enforcement for a single relation.
///
/// # Thread safety
///
/// `Send + Sync`. Unique-key state lives behind a `Mutex`; FK callbacks
/// are required to be `Send + Sync`.
pub struct ConstraintChecker {
    constraints: Vec<Constraint>,
    /// One optional unique-key set per constraint (Some only for
    /// Unique / `PrimaryKey` kinds).
    unique_sets: Vec<Option<Mutex<HashSet<Vec<Value>>>>>,
    fk_lookup: FkParentLookup,
    fk_child_lookup: FkChildLookup,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConstraintCheckPhase {
    Immediate,
    Deferred,
}

impl ConstraintCheckPhase {
    const fn should_check_fk(self, deferrable: bool, initially_deferred: bool) -> bool {
        match self {
            Self::Immediate => !(deferrable && initially_deferred),
            Self::Deferred => deferrable && initially_deferred,
        }
    }
}

impl fmt::Debug for ConstraintChecker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConstraintChecker")
            .field("constraints", &self.constraints)
            .finish_non_exhaustive()
    }
}

impl ConstraintChecker {
    /// Create a new `ConstraintChecker`.
    ///
    /// - `fk_lookup` — returns `true` when the parent row exists.
    /// - `fk_child_lookup` — returns child rows referencing the given
    ///   parent key (used by ON DELETE / ON UPDATE enforcement).
    pub fn new(
        constraints: Vec<Constraint>,
        fk_lookup: impl Fn(Oid, &[Value]) -> bool + Send + Sync + 'static,
        fk_child_lookup: impl Fn(Oid, &[Value]) -> Vec<Vec<Value>> + Send + Sync + 'static,
    ) -> Self {
        let unique_sets = constraints
            .iter()
            .map(|c| match c {
                Constraint::Unique { .. } | Constraint::PrimaryKey { .. } => {
                    Some(Mutex::new(HashSet::new()))
                }
                _ => None,
            })
            .collect();
        Self {
            constraints,
            unique_sets,
            fk_lookup: Box::new(fk_lookup),
            fk_child_lookup: Box::new(fk_child_lookup),
        }
    }

    /// Check all constraints that apply to a newly inserted row.
    pub fn check_insert(&self, row: &[Value]) -> Result<(), ConstraintError> {
        self.check_insert_non_unique_constraints(row)?;
        self.reserve_unique_keys(row)
    }

    fn check_insert_non_unique_constraints(&self, row: &[Value]) -> Result<(), ConstraintError> {
        for c in &self.constraints {
            match c {
                Constraint::NotNull { column } => {
                    if matches!(row.get(*column), Some(Value::Null) | None) {
                        return Err(ConstraintError::NotNull {
                            column_index: *column,
                        });
                    }
                }
                Constraint::Check { name, expr } => match expr.apply(row) {
                    Some(Value::Bool(true)) => {}
                    _ => return Err(ConstraintError::CheckFailed { name: name.clone() }),
                },
                Constraint::Unique { .. } | Constraint::PrimaryKey { .. } => {}
                Constraint::ForeignKey {
                    columns,
                    target_rel,
                    deferrable,
                    initially_deferred,
                    ..
                } => {
                    if ConstraintCheckPhase::Immediate
                        .should_check_fk(*deferrable, *initially_deferred)
                    {
                        self.check_insert_fk(columns, *target_rel, row)?;
                    }
                }
                Constraint::Default { .. }
                | Constraint::Exclude
                | Constraint::GeneratedAlwaysAsIdentity { .. }
                | Constraint::GeneratedByDefaultAsIdentity { .. }
                | Constraint::GeneratedAlwaysAsStored { .. } => {}
            }
        }
        Ok(())
    }

    fn reserve_unique_keys(&self, row: &[Value]) -> Result<(), ConstraintError> {
        let mut inserted_keys: Vec<(usize, Vec<Value>)> = Vec::new();
        for (i, c) in self.constraints.iter().enumerate() {
            if let Constraint::Unique { columns } | Constraint::PrimaryKey { columns } = c {
                let key: Vec<Value> = columns
                    .iter()
                    .map(|&col| row.get(col).cloned().unwrap_or(Value::Null))
                    .collect();
                if key.iter().any(|v| v == &Value::Null) {
                    continue; // NULLs never participate in uniqueness
                }
                if let Some(set_lock) = &self.unique_sets[i] {
                    let inserted = set_lock.lock().insert(key.clone());
                    if !inserted {
                        self.rollback_unique_keys(inserted_keys);
                        return Err(ConstraintError::UniqueViolation);
                    }
                    inserted_keys.push((i, key));
                }
            }
        }
        Ok(())
    }

    fn rollback_unique_keys(&self, inserted_keys: Vec<(usize, Vec<Value>)>) {
        for (i, key) in inserted_keys.into_iter().rev() {
            if let Some(set_lock) = &self.unique_sets[i] {
                set_lock.lock().remove(&key);
            }
        }
    }

    /// Check all constraints for an UPDATE (new row after-image).
    pub fn check_update(&self, _old: &[Value], new: &[Value]) -> Result<(), ConstraintError> {
        self.check_insert(new)
    }

    /// Check deferred constraints for a newly inserted row.
    ///
    /// Only `DEFERRABLE INITIALLY DEFERRED` foreign keys are evaluated
    /// here. Immediate constraints and unique-key state are intentionally
    /// not re-run because they have already been checked by
    /// [`Self::check_insert`].
    pub fn check_deferred_insert(&self, row: &[Value]) -> Result<(), ConstraintError> {
        for c in &self.constraints {
            if let Constraint::ForeignKey {
                columns,
                target_rel,
                deferrable,
                initially_deferred,
                ..
            } = c
            {
                if ConstraintCheckPhase::Deferred.should_check_fk(*deferrable, *initially_deferred)
                {
                    self.check_insert_fk(columns, *target_rel, row)?;
                }
            }
        }
        Ok(())
    }

    /// Check deferred constraints for an UPDATE after-image.
    ///
    /// The current representation stores deferred state only on foreign
    /// keys, so UPDATE validation is equivalent to validating the new
    /// row's referencing key at commit time.
    pub fn check_deferred_update(
        &self,
        _old: &[Value],
        new: &[Value],
    ) -> Result<(), ConstraintError> {
        self.check_deferred_insert(new)
    }

    /// Check referential integrity when a row is deleted.
    ///
    /// `Restrict` / `NoAction` raise an error when child rows exist.
    /// `Cascade` / `SetNull` / `SetDefault` return `Ok(())` — the
    /// executor performs the cascading DML.
    pub fn check_delete(&self, row: &[Value]) -> Result<(), ConstraintError> {
        for c in &self.constraints {
            if let Constraint::ForeignKey {
                columns,
                target_rel,
                on_delete,
                deferrable,
                initially_deferred,
                ..
            } = c
            {
                if ConstraintCheckPhase::Immediate.should_check_fk(*deferrable, *initially_deferred)
                {
                    self.check_delete_fk(columns, *target_rel, on_delete, row)?;
                }
            }
        }
        Ok(())
    }

    /// Check deferred referential integrity for a deleted row.
    ///
    /// Only `DEFERRABLE INITIALLY DEFERRED` foreign keys are evaluated.
    /// Cascading actions are still left to executor-layer DML; this
    /// method reports only deferred `RESTRICT` / `NO ACTION` failures.
    pub fn check_deferred_delete(&self, row: &[Value]) -> Result<(), ConstraintError> {
        for c in &self.constraints {
            if let Constraint::ForeignKey {
                columns,
                target_rel,
                on_delete,
                deferrable,
                initially_deferred,
                ..
            } = c
            {
                if ConstraintCheckPhase::Deferred.should_check_fk(*deferrable, *initially_deferred)
                {
                    self.check_delete_fk(columns, *target_rel, on_delete, row)?;
                }
            }
        }
        Ok(())
    }

    /// Fill NULL column positions with their declared DEFAULT values.
    ///
    /// Mutates `row` in place. Only `None` or `Some(Null)` positions
    /// whose constraint column matches are touched.
    pub fn apply_defaults(&self, row: &mut [Option<Value>]) {
        for c in &self.constraints {
            if let Constraint::Default { column, expr } = c {
                if let Some(slot) = row.get_mut(*column) {
                    if slot.is_none() || *slot == Some(Value::Null) {
                        *slot = expr.apply(&[]);
                    }
                }
            }
        }
    }

    /// Overwrite generated-column positions with computed values.
    ///
    /// For `GENERATED ALWAYS AS (expr) STORED`, the column is always
    /// overwritten regardless of what the caller supplied. Identity
    /// columns are driven by the sequence at the executor layer and are
    /// left untouched here.
    pub fn apply_generated(&self, row: &mut [Value]) {
        for c in &self.constraints {
            if let Constraint::GeneratedAlwaysAsStored { column, expr } = c {
                let value = expr.apply(row).unwrap_or(Value::Null);
                if let Some(slot) = row.get_mut(*column) {
                    *slot = value;
                }
            }
        }
    }

    fn check_insert_fk(
        &self,
        columns: &[usize],
        target_rel: Oid,
        row: &[Value],
    ) -> Result<(), ConstraintError> {
        let key: Vec<Value> = columns
            .iter()
            .map(|&col| row.get(col).cloned().unwrap_or(Value::Null))
            .collect();
        if key.iter().all(|v| v == &Value::Null) {
            return Ok(());
        }
        if !(self.fk_lookup)(target_rel, &key) {
            return Err(ConstraintError::ForeignKeyViolation);
        }
        Ok(())
    }

    fn check_delete_fk(
        &self,
        columns: &[usize],
        target_rel: Oid,
        on_delete: &ReferentialAction,
        row: &[Value],
    ) -> Result<(), ConstraintError> {
        let key: Vec<Value> = columns
            .iter()
            .map(|&col| row.get(col).cloned().unwrap_or(Value::Null))
            .collect();
        let children = (self.fk_child_lookup)(target_rel, &key);
        if !children.is_empty() {
            match on_delete {
                ReferentialAction::Restrict | ReferentialAction::NoAction => {
                    return Err(ConstraintError::ReferentialAction(format!(
                        "ON DELETE {on_delete}: child rows still reference key {key:?}",
                    )));
                }
                ReferentialAction::Cascade
                | ReferentialAction::SetNull
                | ReferentialAction::SetDefault => {}
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::Mutex;
    use ultrasql_core::Oid;

    use super::*;

    type ParentKeySet = Arc<Mutex<HashSet<Vec<Value>>>>;

    fn no_fk(constraints: Vec<Constraint>) -> ConstraintChecker {
        ConstraintChecker::new(constraints, |_, _| true, |_, _| Vec::new())
    }

    // --- NotNull ---

    #[test]
    fn not_null_accepts_non_null() {
        let c = no_fk(vec![Constraint::NotNull { column: 0 }]);
        assert!(c.check_insert(&[Value::Int64(1)]).is_ok());
    }

    #[test]
    fn not_null_rejects_null() {
        let c = no_fk(vec![Constraint::NotNull { column: 0 }]);
        let err = c.check_insert(&[Value::Null]).expect_err("must fail");
        assert!(matches!(err, ConstraintError::NotNull { column_index: 0 }));
    }

    // --- Check ---

    #[test]
    fn check_accepts_when_expression_is_true() {
        let c = no_fk(vec![Constraint::Check {
            name: "positive".into(),
            expr: ScalarExpr::new(
                |row| {
                    if let Some(Value::Int64(v)) = row.first() {
                        Some(Value::Bool(*v > 0))
                    } else {
                        None
                    }
                },
                "col0 > 0",
            ),
        }]);
        assert!(c.check_insert(&[Value::Int64(5)]).is_ok());
    }

    #[test]
    fn check_rejects_when_expression_is_false() {
        let c = no_fk(vec![Constraint::Check {
            name: "positive".into(),
            expr: ScalarExpr::new(
                |row| {
                    if let Some(Value::Int64(v)) = row.first() {
                        Some(Value::Bool(*v > 0))
                    } else {
                        None
                    }
                },
                "col0 > 0",
            ),
        }]);
        let err = c.check_insert(&[Value::Int64(-1)]).expect_err("must fail");
        assert!(
            matches!(err, ConstraintError::CheckFailed { ref name } if name == "positive"),
            "got {err:?}"
        );
    }

    // --- Unique ---

    #[test]
    fn unique_accepts_distinct_keys() {
        let c = no_fk(vec![Constraint::Unique { columns: vec![0] }]);
        assert!(c.check_insert(&[Value::Int64(1)]).is_ok());
        assert!(c.check_insert(&[Value::Int64(2)]).is_ok());
    }

    #[test]
    fn unique_rejects_duplicate() {
        let c = no_fk(vec![Constraint::Unique { columns: vec![0] }]);
        c.check_insert(&[Value::Int64(7)]).unwrap();
        let err = c
            .check_insert(&[Value::Int64(7)])
            .expect_err("dup must fail");
        assert!(matches!(err, ConstraintError::UniqueViolation));
    }

    #[test]
    fn unique_key_is_not_reserved_when_later_constraint_fails() {
        let c = no_fk(vec![
            Constraint::Unique { columns: vec![0] },
            Constraint::Check {
                name: "flag_true".into(),
                expr: ScalarExpr::new(
                    |row| match row.get(1) {
                        Some(Value::Bool(v)) => Some(Value::Bool(*v)),
                        _ => Some(Value::Bool(false)),
                    },
                    "flag_true",
                ),
            },
        ]);

        let err = c
            .check_insert(&[Value::Int64(7), Value::Bool(false)])
            .expect_err("check must fail");
        assert!(matches!(err, ConstraintError::CheckFailed { .. }));

        assert!(
            c.check_insert(&[Value::Int64(7), Value::Bool(true)])
                .is_ok()
        );
    }

    // --- PrimaryKey ---

    #[test]
    fn primary_key_accepts_non_null_distinct() {
        let c = no_fk(vec![Constraint::PrimaryKey { columns: vec![0] }]);
        assert!(c.check_insert(&[Value::Int64(1)]).is_ok());
        assert!(c.check_insert(&[Value::Int64(2)]).is_ok());
    }

    #[test]
    fn primary_key_rejects_null_via_not_null() {
        let c = no_fk(vec![
            Constraint::NotNull { column: 0 },
            Constraint::PrimaryKey { columns: vec![0] },
        ]);
        let err = c
            .check_insert(&[Value::Null])
            .expect_err("null pk must fail");
        assert!(matches!(err, ConstraintError::NotNull { .. }));
    }

    // --- ForeignKey INSERT ---

    #[test]
    fn fk_insert_passes_when_parent_exists() {
        let parent_keys: ParentKeySet = Arc::new(Mutex::new(HashSet::new()));
        parent_keys.lock().insert(vec![Value::Int64(1)]);
        let pk = Arc::clone(&parent_keys);
        let c = ConstraintChecker::new(
            vec![Constraint::ForeignKey {
                columns: vec![1],
                target_rel: Oid::new(100),
                target_columns: vec![0],
                on_delete: ReferentialAction::Restrict,
                on_update: ReferentialAction::NoAction,
                deferrable: false,
                initially_deferred: false,
            }],
            move |_rel, key| pk.lock().contains(key),
            |_, _| Vec::new(),
        );
        assert!(c.check_insert(&[Value::Int64(42), Value::Int64(1)]).is_ok());
    }

    #[test]
    fn fk_insert_fails_when_parent_missing() {
        let c = ConstraintChecker::new(
            vec![Constraint::ForeignKey {
                columns: vec![0],
                target_rel: Oid::new(100),
                target_columns: vec![0],
                on_delete: ReferentialAction::Restrict,
                on_update: ReferentialAction::NoAction,
                deferrable: false,
                initially_deferred: false,
            }],
            |_, _| false,
            |_, _| Vec::new(),
        );
        let err = c.check_insert(&[Value::Int64(999)]).expect_err("must fail");
        assert!(matches!(err, ConstraintError::ForeignKeyViolation));
    }

    #[test]
    fn deferred_fk_insert_skips_immediate_and_fails_deferred_pass() {
        let c = ConstraintChecker::new(
            vec![Constraint::ForeignKey {
                columns: vec![0],
                target_rel: Oid::new(100),
                target_columns: vec![0],
                on_delete: ReferentialAction::Restrict,
                on_update: ReferentialAction::NoAction,
                deferrable: true,
                initially_deferred: true,
            }],
            |_, _| false,
            |_, _| Vec::new(),
        );

        assert!(c.check_insert(&[Value::Int64(999)]).is_ok());
        let err = c
            .check_deferred_insert(&[Value::Int64(999)])
            .expect_err("deferred pass must fail");
        assert!(matches!(err, ConstraintError::ForeignKeyViolation));
    }

    // --- ForeignKey DELETE (Restrict / Cascade / SetNull) ---

    #[test]
    fn fk_delete_restrict_fails_with_children() {
        let c = ConstraintChecker::new(
            vec![Constraint::ForeignKey {
                columns: vec![0],
                target_rel: Oid::new(200),
                target_columns: vec![0],
                on_delete: ReferentialAction::Restrict,
                on_update: ReferentialAction::NoAction,
                deferrable: false,
                initially_deferred: false,
            }],
            |_, _| true,
            |_, _| vec![vec![Value::Int64(7)]],
        );
        let err = c
            .check_delete(&[Value::Int64(1)])
            .expect_err("restrict must fail");
        assert!(matches!(err, ConstraintError::ReferentialAction(_)));
    }

    #[test]
    fn fk_delete_cascade_passes_checker() {
        let c = ConstraintChecker::new(
            vec![Constraint::ForeignKey {
                columns: vec![0],
                target_rel: Oid::new(200),
                target_columns: vec![0],
                on_delete: ReferentialAction::Cascade,
                on_update: ReferentialAction::NoAction,
                deferrable: false,
                initially_deferred: false,
            }],
            |_, _| true,
            |_, _| vec![vec![Value::Int64(99)]],
        );
        assert!(c.check_delete(&[Value::Int64(1)]).is_ok());
    }

    #[test]
    fn fk_delete_set_null_passes_checker() {
        let c = ConstraintChecker::new(
            vec![Constraint::ForeignKey {
                columns: vec![0],
                target_rel: Oid::new(200),
                target_columns: vec![0],
                on_delete: ReferentialAction::SetNull,
                on_update: ReferentialAction::NoAction,
                deferrable: false,
                initially_deferred: false,
            }],
            |_, _| true,
            |_, _| vec![vec![Value::Int64(5)]],
        );
        assert!(c.check_delete(&[Value::Int64(1)]).is_ok());
    }

    #[test]
    fn deferred_fk_delete_skips_immediate_and_fails_deferred_pass() {
        let c = ConstraintChecker::new(
            vec![Constraint::ForeignKey {
                columns: vec![0],
                target_rel: Oid::new(200),
                target_columns: vec![0],
                on_delete: ReferentialAction::Restrict,
                on_update: ReferentialAction::NoAction,
                deferrable: true,
                initially_deferred: true,
            }],
            |_, _| true,
            |_, _| vec![vec![Value::Int64(7)]],
        );

        assert!(c.check_delete(&[Value::Int64(1)]).is_ok());
        let err = c
            .check_deferred_delete(&[Value::Int64(1)])
            .expect_err("deferred pass must fail");
        assert!(matches!(err, ConstraintError::ReferentialAction(_)));
    }

    // --- Defaults ---

    #[test]
    fn apply_defaults_fills_missing() {
        let c = no_fk(vec![Constraint::Default {
            column: 1,
            expr: ScalarExpr::new(|_| Some(Value::Int64(42)), "42"),
        }]);
        let mut row: Vec<Option<Value>> = vec![Some(Value::Int64(1)), None];
        c.apply_defaults(&mut row);
        assert_eq!(row[1], Some(Value::Int64(42)));
    }

    #[test]
    fn apply_defaults_skips_existing_value() {
        let c = no_fk(vec![Constraint::Default {
            column: 0,
            expr: ScalarExpr::new(|_| Some(Value::Int64(0)), "0"),
        }]);
        let mut row: Vec<Option<Value>> = vec![Some(Value::Int64(99))];
        c.apply_defaults(&mut row);
        assert_eq!(row[0], Some(Value::Int64(99)));
    }

    // --- Generated columns ---

    #[test]
    fn apply_generated_computes_stored_column() {
        let c = no_fk(vec![Constraint::GeneratedAlwaysAsStored {
            column: 2,
            expr: ScalarExpr::new(
                |row| {
                    let a = if let Some(Value::Int64(v)) = row.first() {
                        *v
                    } else {
                        0
                    };
                    let b = if let Some(Value::Int64(v)) = row.get(1) {
                        *v
                    } else {
                        0
                    };
                    Some(Value::Int64(a + b))
                },
                "col0 + col1",
            ),
        }]);
        let mut row = vec![Value::Int64(3), Value::Int64(4), Value::Null];
        c.apply_generated(&mut row);
        assert_eq!(row[2], Value::Int64(7));
    }
}
