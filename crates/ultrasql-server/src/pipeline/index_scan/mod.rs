//! Index-scan lowering: detect `WHERE col op lit` shapes that match an
//! existing B-tree index and lower them to an `IndexScan`.

use super::LowerCtx;
use super::modify;

mod btree_probe;
mod catalog_lookup;
mod dispatch;
mod index_only;
mod late_materialize;
mod ordered;
mod predicate;
mod vector;

pub(super) use dispatch::try_index_scan;
pub(super) use index_only::try_index_only_scan;
pub(super) use late_materialize::try_late_materialization_project;
pub(super) use ordered::{try_ordered_index_scan, try_ordered_index_scan_limit};
pub(super) use vector::{try_hnsw_filtered_top_k_limit, try_hnsw_top_k_limit};

pub(crate) use late_materialize::late_materialization_summary_for_plan;
#[cfg(test)]
pub(crate) use predicate::match_simple_comparison;
pub(crate) use predicate::{
    literal_as_i64, literal_in_same_unit_class_as_column, match_indexable_predicate,
};

pub(super) use btree_probe::probe_index_entries_ordered;
pub(super) use catalog_lookup::{find_single_column_index, key_type_for_btree};
