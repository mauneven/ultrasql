//! Operator-level behaviour tests: row retention under SQL 3VL,
//! batch chaining, schema passthrough, selectivity-scaled row
//! estimates, and empty-input/empty-output handling.

use super::*;

#[test]
fn filter_keeps_rows_where_predicate_true() {
    let scan = MemTableScan::new(
        schema_id_val(),
        vec![pair_batch(&[(7, 10), (1, 20), (7, 30), (2, 40)])],
    );
    let mut filter = Filter::new(Box::new(scan), pred_id_eq_7());
    let rows = drain_id_val(&mut filter);
    assert_eq!(rows, vec![(7, 10), (7, 30)]);
}

#[test]
fn filter_drops_rows_where_predicate_false_or_null() {
    let scan = MemTableScan::new(
        schema_id_val(),
        vec![pair_batch(&[(1, 10), (2, 20), (3, 30)])],
    );
    let mut filter = Filter::new(Box::new(scan), pred_id_eq_7());
    let rows = drain_id_val(&mut filter);
    assert!(rows.is_empty(), "expected no rows, got {rows:?}");
}

#[test]
fn filter_chains_with_mem_table_scan() {
    let schema = schema_id_val();
    let b1 = pair_batch(&[(7, 1), (2, 2), (7, 3)]);
    let b2 = pair_batch(&[(7, 4), (5, 5)]);
    let scan = MemTableScan::new(schema, vec![b1, b2]);
    let mut filter = Filter::new(Box::new(scan), pred_id_eq_7());
    let rows = drain_id_val(&mut filter);
    assert_eq!(rows, vec![(7, 1), (7, 3), (7, 4)]);
}

#[test]
fn filter_schema_matches_child_schema() {
    let scan = MemTableScan::new(schema_id_val(), vec![]);
    let filter = Filter::new(Box::new(scan), pred_id_eq_7());
    assert_eq!(filter.schema().len(), 2);
    assert_eq!(filter.schema().field_at(0).name, "id");
}

#[test]
fn filter_scales_child_row_count_hint_by_selectivity() {
    let child = HintOnlyOp {
        schema: schema_id_val(),
        hint: Some(123),
    };
    let filter = Filter::new(Box::new(child), pred_id_eq_7());
    assert_eq!(filter.estimated_row_count(), Some(13));
}

#[test]
fn filter_empty_input_returns_none() {
    let scan = MemTableScan::new(schema_id_val(), vec![]);
    let mut filter = Filter::new(Box::new(scan), pred_id_eq_7());
    assert!(filter.next_batch().unwrap().is_none());
}

#[test]
fn filter_emits_empty_batch_when_nothing_matches() {
    let scan = MemTableScan::new(schema_id_val(), vec![pair_batch(&[(1, 1), (2, 2)])]);
    let mut filter = Filter::new(Box::new(scan), pred_id_eq_7());
    // The filter emits a batch (possibly empty) per child batch, not None.
    let batch = filter.next_batch().unwrap().unwrap();
    assert_eq!(batch.rows(), 0, "expected empty batch");
    assert!(filter.next_batch().unwrap().is_none());
}
