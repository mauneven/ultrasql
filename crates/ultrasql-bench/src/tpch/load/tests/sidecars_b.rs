//! Direct-load sidecar tests for TPC-H Q10-Q21.

#[cfg(feature = "sql-bench")]
use crate::tpch::load::DIRECT_Q6_SHIPDATE_START_1994_01_01;
#[cfg(feature = "sql-bench")]
use crate::tpch::load::encode::encode_direct_decimal;
#[cfg(feature = "sql-bench")]
use crate::tpch::load::sidecars_q7_q10::TpchQ10BuildState;
#[cfg(feature = "sql-bench")]
use crate::tpch::load::sidecars_q11_q15::{
    TpchQ11BuildState, TpchQ12BuildState, TpchQ13BuildState, TpchQ14BuildState, TpchQ15BuildState,
};
#[cfg(feature = "sql-bench")]
use crate::tpch::load::sidecars_q16_q18::{
    TpchQ16BuildState, TpchQ17BuildState, TpchQ17PartStats, TpchQ18BuildState,
};
#[cfg(feature = "sql-bench")]
use crate::tpch::load::sidecars_q19_q21::{
    TpchQ19BuildState, TpchQ20BuildState, TpchQ21BuildState, TpchQ21Order, TpchQ21Supplier,
};

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q10_sidecar_keeps_returned_customer_revenue() {
    let mut state = TpchQ10BuildState::default();
    state
        .ingest("nation", "2|BRAZIL|1|comment")
        .expect("nation");
    state
        .ingest(
            "customer",
            "4|Customer#4|address|2|11-111-1111|100.00|BUILDING|comment",
        )
        .expect("customer");
    state
        .ingest("orders", "10|4|O|100.00|1993-10-15|5-LOW|Clerk#1|0|comment")
        .expect("orders");

    let mut payload = vec![0, 0];
    for value in [10_i32, 5, 3, 1] {
        payload.extend_from_slice(&value.to_le_bytes());
    }
    for value in [100_i64, 10_000, 5, 0] {
        encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
    }
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.push(b'R');

    state
        .ingest_lineitem_payload(&payload)
        .expect("lineitem payload");
    let rows = state.finish_rows();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].c_custkey, 4);
    assert_eq!(rows[0].revenue, 9_500);
    assert_eq!(rows[0].n_name, "BRAZIL");
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q10_sidecar_rejects_discount_factor_overflow() {
    let mut state = TpchQ10BuildState::default();
    state
        .ingest("nation", "2|BRAZIL|1|comment")
        .expect("nation");
    state
        .ingest(
            "customer",
            "4|Customer#4|address|2|11-111-1111|100.00|BUILDING|comment",
        )
        .expect("customer");
    state
        .ingest("orders", "10|4|O|100.00|1993-10-15|5-LOW|Clerk#1|0|comment")
        .expect("orders");

    let mut payload = vec![0, 0];
    for value in [10_i32, 5, 3, 1] {
        payload.extend_from_slice(&value.to_le_bytes());
    }
    for value in [100_i64, 10_000, i64::MIN, 0] {
        encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
    }
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.push(b'R');

    let err = state
        .ingest_lineitem_payload(&payload)
        .expect_err("discount factor overflow should reject");

    assert!(err.to_string().contains("TPC-H sidecar revenue overflow"));
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q11_sidecar_filters_german_parts_above_threshold() {
    let mut state = TpchQ11BuildState::default();
    state
        .ingest("nation", "1|GERMANY|0|comment")
        .expect("nation");
    state
        .ingest(
            "supplier",
            "3|Supplier#3|address|1|11-111-1111|0.00|comment",
        )
        .expect("supplier");
    state
        .ingest("partsupp", "5|3|2|40.00|comment")
        .expect("partsupp");

    let rows = state.finish_rows();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].ps_partkey, 5);
    assert_eq!(rows[0].value, 8_000);
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q11_sidecar_rejects_value_overflow() {
    let mut state = TpchQ11BuildState::default();
    state
        .ingest("nation", "1|GERMANY|0|comment")
        .expect("nation");
    state
        .ingest(
            "supplier",
            "3|Supplier#3|address|1|11-111-1111|0.00|comment",
        )
        .expect("supplier");

    let err = state
        .ingest("partsupp", "5|3|2|92233720368547758.07|overflowing value")
        .expect_err("partsupp value overflow should reject");

    assert!(err.to_string().contains("TPC-H sidecar value overflow"));
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q12_sidecar_counts_shipmode_priority_buckets() {
    let mut state = TpchQ12BuildState::default();
    state
        .ingest("orders", "10|1|O|1.00|1993-01-01|1-URGENT|clerk|0|comment")
        .expect("urgent order");
    state
        .ingest("orders", "11|1|O|1.00|1993-01-01|5-LOW|clerk|0|comment")
        .expect("low order");

    state
        .ingest_lineitem_values(10, -2200, -2195, -2191, "MAIL")
        .expect("mail lineitem");
    state
        .ingest_lineitem_values(11, -2200, -2194, -2190, "SHIP")
        .expect("ship lineitem");

    let rows = state.finish_rows();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].l_shipmode, "MAIL");
    assert_eq!(rows[0].high_line_count, 1);
    assert_eq!(rows[0].low_line_count, 0);
    assert_eq!(rows[1].l_shipmode, "SHIP");
    assert_eq!(rows[1].high_line_count, 0);
    assert_eq!(rows[1].low_line_count, 1);
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q12_sidecar_rejects_count_overflow() {
    let mut state = TpchQ12BuildState::default();
    state
        .ingest("orders", "10|1|O|1.00|1993-01-01|1-URGENT|clerk|0|comment")
        .expect("urgent order");
    state
        .counts_by_shipmode
        .insert("MAIL".to_owned(), (i64::MAX, 0));

    let err = state
        .ingest_lineitem_values(10, -2200, -2195, -2191, "MAIL")
        .expect_err("count overflow should reject");

    assert!(err.to_string().contains("TPC-H sidecar count overflow"));
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q13_sidecar_counts_customers_by_filtered_order_count() {
    let mut state = TpchQ13BuildState::default();
    state
        .ingest("customer", "1|name|addr|1|13-111|1.00|MKT|comment")
        .expect("customer 1");
    state
        .ingest("customer", "2|name|addr|1|13-111|1.00|MKT|comment")
        .expect("customer 2");
    state
        .ingest("customer", "3|name|addr|1|13-111|1.00|MKT|comment")
        .expect("customer 3");
    state
        .ingest(
            "orders",
            "10|1|O|1.00|1993-01-01|1-URGENT|clerk|0|plain comment",
        )
        .expect("order counted");
    state
        .ingest(
            "orders",
            "11|1|O|1.00|1993-01-01|1-URGENT|clerk|0|special late requests",
        )
        .expect("order filtered");
    state
        .ingest(
            "orders",
            "12|2|O|1.00|1993-01-01|1-URGENT|clerk|0|plain comment",
        )
        .expect("order counted 2");

    let rows = state.finish_rows();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].c_count, 1);
    assert_eq!(rows[0].custdist, 2);
    assert_eq!(rows[1].c_count, 0);
    assert_eq!(rows[1].custdist, 1);
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q13_sidecar_rejects_customer_count_overflow() {
    let mut state = TpchQ13BuildState {
        total_customers: i64::MAX,
        ..TpchQ13BuildState::default()
    };

    let err = state
        .ingest("customer", "1|name|addr|1|13-111|1.00|MKT|comment")
        .expect_err("customer count overflow should reject");

    assert!(err.to_string().contains("TPC-H sidecar count overflow"));
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q13_sidecar_rejects_order_count_overflow() {
    let mut state = TpchQ13BuildState::default();
    state.order_count_by_customer.insert(1, i64::MAX);

    let err = state
        .ingest(
            "orders",
            "10|1|O|1.00|1993-01-01|1-URGENT|clerk|0|plain comment",
        )
        .expect_err("order count overflow should reject");

    assert!(err.to_string().contains("TPC-H sidecar count overflow"));
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q13_sidecar_rejects_customer_with_order_count_overflow() {
    let mut state = TpchQ13BuildState {
        customers_with_order_count: i64::MAX,
        ..TpchQ13BuildState::default()
    };

    let err = state
        .ingest(
            "orders",
            "10|1|O|1.00|1993-01-01|1-URGENT|clerk|0|plain comment",
        )
        .expect_err("customers-with-orders count overflow should reject");

    assert!(err.to_string().contains("TPC-H sidecar count overflow"));
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q14_sidecar_computes_promo_revenue_percent() {
    let mut state = TpchQ14BuildState::default();
    state
        .ingest(
            "part",
            "1|forest|mfgr|Brand#1|PROMO BRUSHED STEEL|1|SM BOX|1.00|comment",
        )
        .expect("promo part");
    state
        .ingest(
            "part",
            "2|forest|mfgr|Brand#1|STANDARD BRUSHED STEEL|1|SM BOX|1.00|comment",
        )
        .expect("plain part");

    state
        .ingest_lineitem_values(1, 10_000, 10, -1_583)
        .expect("promo line");
    state
        .ingest_lineitem_values(2, 10_000, 10, -1_583)
        .expect("plain line");

    let rows = state.finish_rows();

    assert_eq!(rows.len(), 1);
    assert!((rows[0].promo_revenue - 50.0).abs() < f64::EPSILON);
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q14_sidecar_rejects_discount_factor_overflow() {
    let mut state = TpchQ14BuildState::default();
    state
        .ingest(
            "part",
            "1|forest|mfgr|Brand#1|PROMO BRUSHED STEEL|1|SM BOX|1.00|comment",
        )
        .expect("promo part");

    let err = state
        .ingest_lineitem_values(1, 10_000, i64::MIN, -1_583)
        .expect_err("discount factor overflow should reject");

    assert!(err.to_string().contains("TPC-H sidecar revenue overflow"));
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q15_sidecar_selects_top_supplier_revenue() {
    let mut state = TpchQ15BuildState::default();
    state
        .ingest(
            "supplier",
            "3|Supplier#3|address|1|11-111-1111|0.00|comment",
        )
        .expect("supplier");
    state
        .ingest_lineitem_values(3, 10_000, 10, -1_461)
        .expect("lineitem");

    let rows = state.finish_rows();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].s_suppkey, 3);
    assert_eq!(rows[0].total_revenue, 900_000);
    assert_eq!(rows[0].s_name, "Supplier#3");
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q15_sidecar_rejects_discount_factor_overflow() {
    let mut state = TpchQ15BuildState::default();
    state
        .ingest(
            "supplier",
            "3|Supplier#3|address|1|11-111-1111|0.00|comment",
        )
        .expect("supplier");

    let err = state
        .ingest_lineitem_values(3, 10_000, i64::MIN, -1_461)
        .expect_err("discount factor overflow should reject");

    assert!(err.to_string().contains("TPC-H sidecar revenue overflow"));
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q16_sidecar_counts_distinct_non_complaint_suppliers() {
    let mut state = TpchQ16BuildState::default();
    state
        .ingest("supplier", "3|Supplier#3|address|1|11|0.00|fine supplier")
        .expect("good supplier");
    state
        .ingest(
            "supplier",
            "4|Supplier#4|address|1|11|0.00|Customer filed Complaints here",
        )
        .expect("bad supplier");
    state
        .ingest(
            "part",
            "5|name|mfgr|Brand#12|SMALL BRUSHED STEEL|49|SM BOX|1.00|comment",
        )
        .expect("part");
    state
        .ingest("partsupp", "5|3|1|10.00|comment")
        .expect("partsupp good");
    state
        .ingest("partsupp", "5|4|1|10.00|comment")
        .expect("partsupp bad");

    let rows = state.finish_rows();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].p_brand, "Brand#12");
    assert_eq!(rows[0].supplier_cnt, 1);
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q17_sidecar_sums_small_quantity_revenue() {
    let mut state = TpchQ17BuildState::default();
    state
        .ingest(
            "part",
            "5|name|mfgr|Brand#23|SMALL BRUSHED STEEL|1|MED BOX|1.00|comment",
        )
        .expect("part");
    state
        .ingest_lineitem_values(5, 10, 7_000)
        .expect("small line");
    state
        .ingest_lineitem_values(5, 100, 70_000)
        .expect("large line");

    let rows = state.finish_rows();

    assert_eq!(rows.len(), 1);
    assert!((rows[0].avg_yearly - 10.0).abs() < f64::EPSILON);
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q17_sidecar_rejects_stats_count_overflow() {
    let mut state = TpchQ17BuildState::default();
    state.qualifying_parts.insert(5);
    state.stats_by_part.insert(
        5,
        TpchQ17PartStats {
            sum_quantity: 0,
            count: i64::MAX,
        },
    );

    let err = state
        .ingest_lineitem_values(5, 1, 10)
        .expect_err("stats count overflow should reject");

    assert!(err.to_string().contains("TPC-H sidecar count overflow"));
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q17_sidecar_rejects_quantity_sum_overflow() {
    let mut state = TpchQ17BuildState::default();
    state.qualifying_parts.insert(5);
    state.stats_by_part.insert(
        5,
        TpchQ17PartStats {
            sum_quantity: i128::MAX,
            count: 0,
        },
    );

    let err = state
        .ingest_lineitem_values(5, 1, 10)
        .expect_err("quantity sum overflow should reject");

    assert!(err.to_string().contains("TPC-H sidecar quantity overflow"));
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q18_sidecar_keeps_top_large_quantity_orders() {
    let mut state = TpchQ18BuildState::default();
    state
        .ingest("customer", "1|Customer#1|addr|1|13|1.00|MKT|comment")
        .expect("customer");
    state
        .ingest(
            "orders",
            "10|1|O|100.00|1995-01-01|1-URGENT|clerk|0|comment",
        )
        .expect("orders");
    state.ingest_lineitem_values(10, 20_000).expect("line 1");
    state.ingest_lineitem_values(10, 15_000).expect("line 2");

    let rows = state.finish_rows();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].c_name, "Customer#1");
    assert_eq!(rows[0].o_orderkey, 10);
    assert_eq!(rows[0].sum_quantity, 35_000);
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q18_sidecar_rejects_quantity_overflow() {
    let mut state = TpchQ18BuildState::default();
    state
        .ingest_lineitem_values(10, i64::MAX)
        .expect("first line");

    let err = state
        .ingest_lineitem_values(10, 1)
        .expect_err("quantity overflow should reject");

    assert!(err.to_string().contains("TPC-H sidecar quantity overflow"));
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q19_sidecar_sums_matching_brand_container_revenue() {
    let mut state = TpchQ19BuildState::default();
    state
        .ingest("part", "5|name|mfgr|Brand#12|TYPE|3|SM BOX|1.00|comment")
        .expect("part");
    state
        .ingest_lineitem_values(5, 1_00, 10_000, 10, "AIR", "DELIVER IN PERSON")
        .expect("lineitem");

    let rows = state.finish_rows();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].revenue, 900_000);
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q19_sidecar_rejects_discount_factor_overflow() {
    let mut state = TpchQ19BuildState::default();
    state
        .ingest("part", "5|name|mfgr|Brand#12|TYPE|3|SM BOX|1.00|comment")
        .expect("part");

    let err = state
        .ingest_lineitem_values(5, 1_00, 10_000, i64::MIN, "AIR", "DELIVER IN PERSON")
        .expect_err("discount factor overflow should reject");

    assert!(err.to_string().contains("TPC-H sidecar revenue overflow"));
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q20_sidecar_selects_canada_forest_supplier_above_half_quantity() {
    let mut state = TpchQ20BuildState::default();
    state
        .ingest("nation", "3|CANADA|1|comment")
        .expect("nation");
    state
        .ingest("supplier", "7|Supplier#7|addr|3|11-111|1.00|comment")
        .expect("supplier");
    state
        .ingest(
            "part",
            "5|forest green part|mfgr|Brand#1|TYPE|3|SM BOX|1.00|comment",
        )
        .expect("part");
    state
        .ingest("partsupp", "5|7|6|1.00|comment")
        .expect("partsupp");
    state
        .ingest_lineitem_values(5, 7, 10_00, DIRECT_Q6_SHIPDATE_START_1994_01_01)
        .expect("lineitem");

    let rows = state.finish_rows();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].s_name, "Supplier#7");
    assert_eq!(rows[0].s_address, "addr");
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q20_sidecar_rejects_quantity_overflow() {
    let mut state = TpchQ20BuildState::default();
    state
        .ingest(
            "part",
            "5|forest green part|mfgr|Brand#1|TYPE|3|SM BOX|1.00|comment",
        )
        .expect("part");
    state
        .ingest_lineitem_values(5, 7, i64::MAX, DIRECT_Q6_SHIPDATE_START_1994_01_01)
        .expect("first line");

    let err = state
        .ingest_lineitem_values(5, 7, 1, DIRECT_Q6_SHIPDATE_START_1994_01_01)
        .expect_err("quantity overflow should reject");

    assert!(err.to_string().contains("TPC-H sidecar quantity overflow"));
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q21_sidecar_counts_only_late_saudi_supplier_in_final_order() {
    let mut state = TpchQ21BuildState::default();
    state
        .ingest("nation", "4|SAUDI ARABIA|1|comment")
        .expect("nation");
    state
        .ingest("supplier", "7|Supplier#7|addr|4|11-111|1.00|comment")
        .expect("supplier");
    state
        .ingest(
            "orders",
            "10|1|F|1.00|1995-01-01|1-URGENT|Clerk#1|0|comment",
        )
        .expect("orders");
    state
        .ingest_lineitem_values(10, 7, 1, 2)
        .expect("late line");
    state
        .ingest_lineitem_values(10, 8, 2, 2)
        .expect("other supplier");

    let rows = state.finish_rows().expect("finish rows");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].s_name, "Supplier#7");
    assert_eq!(rows[0].numwait, 1);
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q21_sidecar_rejects_late_count_overflow() {
    let mut state = TpchQ21BuildState::default();
    state
        .orders
        .entry(10)
        .or_default()
        .late_count_by_supplier
        .insert(7, i64::MAX);

    let err = state
        .ingest_lineitem_values(10, 7, 1, 2)
        .expect_err("late count overflow should reject");

    assert!(err.to_string().contains("TPC-H sidecar count overflow"));
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q21_sidecar_rejects_final_count_overflow() {
    let mut state = TpchQ21BuildState::default();
    state.saudi_nationkeys.insert(4);
    state.suppliers.insert(
        7,
        TpchQ21Supplier {
            name: "Supplier#7".to_owned(),
            nationkey: 4,
        },
    );
    for (orderkey, other_suppkey, late_count) in [(10, 8, i64::MAX), (11, 9, 1)] {
        state.final_orders.insert(orderkey);
        let mut order = TpchQ21Order::default();
        order.suppliers.insert(7);
        order.suppliers.insert(other_suppkey);
        order.late_count_by_supplier.insert(7, late_count);
        state.orders.insert(orderkey, order);
    }

    let err = state
        .finish_rows()
        .expect_err("final count overflow should reject");

    assert!(err.to_string().contains("TPC-H sidecar count overflow"));
}
