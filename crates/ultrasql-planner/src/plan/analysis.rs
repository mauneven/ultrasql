//! Inherent analysis methods on [`LogicalPlan`]: pipeline-mode classification
//! and schema access. Split out of the original monolithic `plan.rs` verbatim.

use ultrasql_core::Schema;

use super::logical_plan::LogicalPlan;
use super::node_types::PipelineMode;

impl LogicalPlan {
    pub fn pipeline_mode(&self) -> PipelineMode {
        if self.is_mutation_or_control() {
            PipelineMode::ScalarOltp
        } else if self.has_batch_pipeline() {
            PipelineMode::VectorizedOlap
        } else {
            PipelineMode::ScalarOltp
        }
    }

    fn is_mutation_or_control(&self) -> bool {
        matches!(
            self,
            Self::Insert { .. }
                | Self::Update { .. }
                | Self::Delete { .. }
                | Self::Merge { .. }
                | Self::Truncate { .. }
                | Self::CreateTable { .. }
                | Self::CreateMaterializedView { .. }
                | Self::CreateView { .. }
                | Self::CreateTypeEnum { .. }
                | Self::CreateTypeComposite { .. }
                | Self::CreateDomain { .. }
                | Self::CreateOperator { .. }
                | Self::CreateIndex { .. }
                | Self::DropIndex { .. }
                | Self::CreatePolicy { .. }
                | Self::CreateRole { .. }
                | Self::AlterRole { .. }
                | Self::DropRole { .. }
                | Self::GrantPrivileges { .. }
                | Self::RevokePrivileges { .. }
                | Self::AlterDefaultPrivileges { .. }
                | Self::GrantRole { .. }
                | Self::RevokeRole { .. }
                | Self::CreateSchema { .. }
                | Self::DropSchema { .. }
                | Self::DropTable { .. }
                | Self::AlterTable { .. }
                | Self::AlterView { .. }
                | Self::CreateSequence { .. }
                | Self::AlterSequence { .. }
                | Self::DropSequence { .. }
                | Self::Comment { .. }
                | Self::Checkpoint { .. }
                | Self::ExportDatabase { .. }
                | Self::ImportDatabase { .. }
                | Self::Begin { .. }
                | Self::Commit { .. }
                | Self::Rollback { .. }
                | Self::Savepoint { .. }
                | Self::RollbackToSavepoint { .. }
                | Self::ReleaseSavepoint { .. }
                | Self::PrepareTransaction { .. }
                | Self::CommitPrepared { .. }
                | Self::RollbackPrepared { .. }
                | Self::SetTransaction { .. }
                | Self::SetVariable { .. }
                | Self::Describe { .. }
                | Self::Summarize { .. }
                | Self::SetRole { .. }
                | Self::Listen { .. }
                | Self::Notify { .. }
                | Self::Unlisten { .. }
                | Self::Copy { .. }
                | Self::Explain { .. }
        )
    }

    fn has_batch_pipeline(&self) -> bool {
        match self {
            Self::Scan { .. }
            | Self::Sort { .. }
            | Self::Window { .. }
            | Self::Aggregate { .. }
            | Self::Pivot { .. }
            | Self::Unpivot { .. }
            | Self::Join { .. }
            | Self::SetOp { .. }
            | Self::LockRows { .. } => true,
            Self::Filter { input, .. }
            | Self::Project { input, .. }
            | Self::Limit { input, .. } => input.has_batch_pipeline(),
            Self::Cte {
                definition, body, ..
            } => definition.has_batch_pipeline() || body.has_batch_pipeline(),
            Self::Insert { source, .. } => source.has_batch_pipeline(),
            Self::Update { input, .. } | Self::Delete { input, .. } => input.has_batch_pipeline(),
            Self::Merge { source, .. } => source.has_batch_pipeline(),
            Self::Explain { input: plan, .. }
            | Self::Copy {
                input: Some(plan), ..
            } => plan.has_batch_pipeline(),
            Self::Empty { .. }
            | Self::Values { .. }
            | Self::FunctionScan { .. }
            | Self::Truncate { .. }
            | Self::CreateTable { .. }
            | Self::CreateMaterializedView { .. }
            | Self::CreateView { .. }
            | Self::CreateTypeEnum { .. }
            | Self::CreateTypeComposite { .. }
            | Self::CreateDomain { .. }
            | Self::CreateOperator { .. }
            | Self::CreateIndex { .. }
            | Self::DropIndex { .. }
            | Self::CreatePolicy { .. }
            | Self::CreateRole { .. }
            | Self::AlterRole { .. }
            | Self::DropRole { .. }
            | Self::GrantPrivileges { .. }
            | Self::RevokePrivileges { .. }
            | Self::AlterDefaultPrivileges { .. }
            | Self::GrantRole { .. }
            | Self::RevokeRole { .. }
            | Self::CreateSchema { .. }
            | Self::DropSchema { .. }
            | Self::DropTable { .. }
            | Self::AlterTable { .. }
            | Self::AlterView { .. }
            | Self::CreateSequence { .. }
            | Self::AlterSequence { .. }
            | Self::DropSequence { .. }
            | Self::Comment { .. }
            | Self::Checkpoint { .. }
            | Self::ExportDatabase { .. }
            | Self::ImportDatabase { .. }
            | Self::Begin { .. }
            | Self::Commit { .. }
            | Self::Rollback { .. }
            | Self::Savepoint { .. }
            | Self::RollbackToSavepoint { .. }
            | Self::ReleaseSavepoint { .. }
            | Self::PrepareTransaction { .. }
            | Self::CommitPrepared { .. }
            | Self::RollbackPrepared { .. }
            | Self::SetTransaction { .. }
            | Self::SetVariable { .. }
            | Self::Describe { .. }
            | Self::Summarize { .. }
            | Self::SetRole { .. }
            | Self::Listen { .. }
            | Self::Notify { .. }
            | Self::Unlisten { .. }
            | Self::Copy { input: None, .. } => false,
        }
    }

    /// The schema of rows produced by this plan node.
    #[must_use]
    pub fn schema(&self) -> &Schema {
        match self {
            Self::Scan { schema, .. }
            | Self::Project { schema, .. }
            | Self::Empty { schema }
            | Self::Values { schema, .. }
            | Self::Insert { schema, .. }
            | Self::Update { schema, .. }
            | Self::Delete { schema, .. }
            | Self::Merge { schema, .. }
            | Self::Truncate { schema, .. }
            | Self::CreateTable { schema, .. }
            | Self::CreateMaterializedView { schema, .. }
            | Self::CreateView { schema, .. }
            | Self::CreateTypeEnum { schema, .. }
            | Self::CreateTypeComposite { schema, .. }
            | Self::CreateDomain { schema, .. }
            | Self::CreateOperator { schema, .. }
            | Self::Join { schema, .. }
            | Self::Aggregate { schema, .. }
            | Self::Pivot { schema, .. }
            | Self::Unpivot { schema, .. }
            | Self::SetOp { schema, .. }
            | Self::Cte { schema, .. }
            | Self::LockRows { schema, .. }
            | Self::CreateIndex { schema, .. }
            | Self::DropIndex { schema, .. }
            | Self::CreatePolicy { schema, .. }
            | Self::CreateRole { schema, .. }
            | Self::AlterRole { schema, .. }
            | Self::DropRole { schema, .. }
            | Self::GrantPrivileges { schema, .. }
            | Self::RevokePrivileges { schema, .. }
            | Self::AlterDefaultPrivileges { schema, .. }
            | Self::GrantRole { schema, .. }
            | Self::RevokeRole { schema, .. }
            | Self::CreateSchema { schema, .. }
            | Self::DropSchema { schema, .. }
            | Self::DropTable { schema, .. }
            | Self::AlterTable { schema, .. }
            | Self::AlterView { schema, .. }
            | Self::CreateSequence { schema, .. }
            | Self::AlterSequence { schema, .. }
            | Self::DropSequence { schema, .. }
            | Self::Comment { schema, .. }
            | Self::Checkpoint { schema }
            | Self::ExportDatabase { schema, .. }
            | Self::ImportDatabase { schema, .. }
            | Self::Begin { schema, .. }
            | Self::Commit { schema }
            | Self::Rollback { schema }
            | Self::Savepoint { schema, .. }
            | Self::RollbackToSavepoint { schema, .. }
            | Self::ReleaseSavepoint { schema, .. }
            | Self::PrepareTransaction { schema, .. }
            | Self::CommitPrepared { schema, .. }
            | Self::RollbackPrepared { schema, .. }
            | Self::SetTransaction { schema, .. }
            | Self::SetVariable { schema, .. }
            | Self::Describe { schema, .. }
            | Self::Summarize { schema, .. }
            | Self::SetRole { schema, .. }
            | Self::Listen { schema, .. }
            | Self::Notify { schema, .. }
            | Self::Unlisten { schema, .. }
            | Self::Copy { schema, .. }
            | Self::Explain { schema, .. }
            | Self::FunctionScan { schema, .. }
            | Self::Window { schema, .. } => schema,
            Self::Filter { input, .. } | Self::Limit { input, .. } | Self::Sort { input, .. } => {
                input.schema()
            }
        }
    }
}
