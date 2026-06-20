//! Logical plan tree.
//!
//! The logical plan is the binder's output and the optimizer's input.
//! It is type-checked but not physical: it names *what* to compute, not
//! *how*. Each variant produces a [`Schema`] queryable through
//! [`LogicalPlan::schema`]; an EXPLAIN-style indented dump is available
//! through [`LogicalPlan::display`].
//!
//! [`Schema`]: ultrasql_core::Schema
//!
//! The plan tree was split out of a single oversized `plan.rs` into
//! cohesive submodules with no behaviour change:
//!
//! - [`logical_plan`] — the central [`LogicalPlan`] enum.
//! - [`node_types`] — supporting types for query / DML / control nodes.
//! - [`ddl_types`] — supporting types for DDL / access-control nodes.
//! - [`analysis`] — [`LogicalPlan`] pipeline-mode and schema methods.
//! - [`display`] — [`LogicalPlan`] EXPLAIN-style rendering.
//!
//! Every previously-public path (`ultrasql_planner::plan::…`) is preserved
//! through the re-exports below.

mod analysis;
mod ddl_types;
mod display;
mod logical_plan;
mod node_types;

#[cfg(test)]
mod tests;

pub use self::ddl_types::{
    CopyDirection, CopyFormat, CopySource, ExplainFormat, LogicalAlterTableAction,
    LogicalAlterViewAction, LogicalCheckConstraint, LogicalCommentTarget,
    LogicalDefaultPrivilegeOperation, LogicalExclusionConstraint, LogicalExclusionElement,
    LogicalForeignKeyConstraint, LogicalPrivilegeKind, LogicalPrivilegeObjectKind,
    LogicalPrivilegeSpec, LogicalReferentialAction, LogicalRlsCommand, LogicalRlsPermissiveness,
    LogicalRlsPolicy, LogicalRoleKind, LogicalRoleOptions, LogicalSequenceChange,
    LogicalSequenceOptions, LogicalTenantPolicyExpr, LogicalTimePartition, LogicalUniqueConstraint,
};
pub use self::logical_plan::LogicalPlan;
pub use self::node_types::{
    AggregateFunc, ConflictTarget, LockStrength, LockWaitPolicy, LogicalAggregateExpr,
    LogicalAggregatingIndex, LogicalAggregatingIndexExpr, LogicalDescribeObjectKind,
    LogicalDescribeTarget, LogicalIndexMethod, LogicalIndexOption, LogicalJoinCondition,
    LogicalJoinType, LogicalMergeAction, LogicalMergeClause, LogicalMergeMatchKind,
    LogicalOnConflict, LogicalPivotAggregate, LogicalPivotValue, LogicalSetOp,
    LogicalSetQuantifier, LogicalSetVariableAction, LogicalTableOption, LogicalUnpivotColumn,
    LogicalWindowFunc, PipelineMode, SortKey, TxnIsolationLevel,
};
