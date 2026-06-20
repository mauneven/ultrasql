//! INSERT/UPDATE/DELETE lowering plus the fused-kernel fast paths.

mod constraints;
mod delete;
mod indexes;
mod insert;
mod lowering;
mod merge;
mod referential;
mod update;

pub(super) use delete::lower_real_delete;
pub(super) use insert::lower_real_insert;
pub(super) use lowering::lower_project_columns;
pub(super) use merge::lower_real_merge;
pub(super) use update::lower_real_update;
