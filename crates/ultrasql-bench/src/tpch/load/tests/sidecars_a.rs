//! Direct-load sidecar tests for TPC-H Q1-Q9.

#[cfg(feature = "sql-bench")]
use crate::tpch::load::direct_table::push_direct_q1_columns;
#[cfg(feature = "sql-bench")]
use crate::tpch::load::encode::encode_direct_decimal;
#[cfg(feature = "sql-bench")]
use crate::tpch::load::sidecars_q2_q5::{
    TpchQ2BuildState, TpchQ3BuildState, TpchQ4BuildState, TpchQ5BuildState,
};
#[cfg(feature = "sql-bench")]
use crate::tpch::load::sidecars_q7_q10::{TpchQ7BuildState, TpchQ8BuildState, TpchQ9BuildState};
#[cfg(feature = "sql-bench")]
use crate::tpch::load::{
    DIRECT_Q1_SHIPDATE_CUTOFF_1998_09_02, DIRECT_Q6_SHIPDATE_START_1994_01_01,
};

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q2_sidecar_keeps_only_min_cost_european_brass_rows() {
    let mut state = TpchQ2BuildState::default();
    state.ingest("region", "1|EUROPE|comment").expect("region");
    state
        .ingest("nation", "10|GERMANY|1|comment")
        .expect("nation");
    state
        .ingest(
            "supplier",
            "100|Supplier#100|address|10|11-111-1111|1000.00|comment",
        )
        .expect("supplier 100");
    state
        .ingest(
            "supplier",
            "101|Supplier#101|address2|10|11-111-1112|900.00|comment2",
        )
        .expect("supplier 101");
    state
        .ingest(
            "part",
            "200|name|MFGR#1|brand|SMALL BRASS|15|container|123.45|comment",
        )
        .expect("part");
    state
        .ingest("partsupp", "200|100|1|50.00|comment")
        .expect("partsupp high");
    state
        .ingest("partsupp", "200|101|1|40.00|comment")
        .expect("partsupp low");

    let rows = state.finish_rows();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].s_name, "Supplier#101");
    assert_eq!(rows[0].s_acctbal, 90_000);
    assert_eq!(rows[0].n_name, "GERMANY");
    assert_eq!(rows[0].p_partkey, 200);
    assert_eq!(rows[0].p_mfgr, "MFGR#1");
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q1_direct_sidecar_rejects_discount_factor_overflow() {
    let mut cache = ultrasql_server::TpchQ1ColumnarCache::default();
    let mut payload = vec![0, 0];
    for value in [1_i32, 2, 3, 4] {
        payload.extend_from_slice(&value.to_le_bytes());
    }
    for value in [100_i64, 1_000, i64::MIN, 0] {
        encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
    }
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.push(b'N');
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.push(b'O');
    payload.extend_from_slice(&DIRECT_Q1_SHIPDATE_CUTOFF_1998_09_02.to_le_bytes());

    let err = push_direct_q1_columns(&payload, &mut cache)
        .expect_err("discount factor overflow should reject");

    assert!(err.to_string().contains("TPC-H Q1 summary overflow"));
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q6_direct_sidecar_rejects_revenue_overflow() {
    let mut cache = ultrasql_server::TpchQ1ColumnarCache {
        q6_revenue: i128::MAX,
        ..ultrasql_server::TpchQ1ColumnarCache::default()
    };
    let mut payload = vec![0, 0];
    for value in [1_i32, 2, 3, 4] {
        payload.extend_from_slice(&value.to_le_bytes());
    }
    for value in [1_00_i64, 10_000, 5, 0] {
        encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
    }
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.push(b'N');
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.push(b'O');
    payload.extend_from_slice(&DIRECT_Q6_SHIPDATE_START_1994_01_01.to_le_bytes());

    let err = push_direct_q1_columns(&payload, &mut cache)
        .expect_err("q6 revenue overflow should reject");

    assert!(err.to_string().contains("TPC-H sidecar revenue overflow"));
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q3_sidecar_filters_building_orders_and_sums_revenue() {
    let mut state = TpchQ3BuildState::default();
    state
        .ingest(
            "customer",
            "1|Customer#1|address|1|11-111-1111|0.00|BUILDING|comment",
        )
        .expect("customer");
    state
        .ingest("orders", "10|1|O|100.00|1995-03-14|5-LOW|Clerk#1|0|comment")
        .expect("orders");
    state
        .ingest(
            "lineitem",
            "10|2|3|1|1.00|100.00|0.05|0.00|N|O|1995-03-16|1995-03-16|1995-03-16|DELIVER IN PERSON|AIR|comment",
        )
        .expect("lineitem");

    let rows = state.finish_rows();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].l_orderkey, 10);
    assert_eq!(rows[0].revenue, 9_500);
    assert_eq!(rows[0].o_orderdate, -1_754);
    assert_eq!(rows[0].o_shippriority, 0);
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q3_sidecar_reads_lineitem_payload_without_resplitting_text() {
    let mut state = TpchQ3BuildState::default();
    state
        .ingest(
            "customer",
            "1|Customer#1|address|1|11-111-1111|0.00|BUILDING|comment",
        )
        .expect("customer");
    state
        .ingest("orders", "10|1|O|100.00|1995-03-14|5-LOW|Clerk#1|0|comment")
        .expect("orders");

    let mut payload = vec![0, 0];
    for value in [10_i32, 2, 3, 1] {
        payload.extend_from_slice(&value.to_le_bytes());
    }
    for value in [100_i64, 10_000, 5, 0] {
        encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
    }
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.push(b'N');
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.push(b'O');
    payload.extend_from_slice(&(-1_752_i32).to_le_bytes());

    state
        .ingest_lineitem_payload(&payload)
        .expect("lineitem payload");
    let rows = state.finish_rows();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].revenue, 9_500);
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q3_sidecar_rejects_discount_factor_overflow() {
    let mut state = TpchQ3BuildState::default();
    state
        .ingest(
            "customer",
            "1|Customer#1|address|1|11-111-1111|0.00|BUILDING|comment",
        )
        .expect("customer");
    state
        .ingest("orders", "10|1|O|100.00|1995-03-14|5-LOW|Clerk#1|0|comment")
        .expect("orders");

    let mut payload = vec![0, 0];
    for value in [10_i32, 2, 3, 1] {
        payload.extend_from_slice(&value.to_le_bytes());
    }
    for value in [100_i64, 10_000, i64::MIN, 0] {
        encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
    }
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.push(b'N');
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.push(b'O');
    payload.extend_from_slice(&(-1_752_i32).to_le_bytes());

    let err = state
        .ingest_lineitem_payload(&payload)
        .expect_err("discount factor overflow should reject");

    assert!(err.to_string().contains("TPC-H sidecar revenue overflow"));
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q4_sidecar_counts_priority_when_lineitem_commits_before_receipt() {
    let mut state = TpchQ4BuildState::default();
    state
        .ingest("orders", "10|1|O|100.00|1993-07-15|5-LOW|Clerk#1|0|comment")
        .expect("orders");

    let mut payload = vec![0, 0];
    for value in [10_i32, 2, 3, 1] {
        payload.extend_from_slice(&value.to_le_bytes());
    }
    for value in [100_i64, 10_000, 5, 0] {
        encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
    }
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.push(b'N');
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.push(b'O');
    payload.extend_from_slice(&(-2_344_i32).to_le_bytes());
    payload.extend_from_slice(&(-2_344_i32).to_le_bytes());
    payload.extend_from_slice(&(-2_343_i32).to_le_bytes());

    state
        .ingest_lineitem_payload(&payload)
        .expect("lineitem payload");
    let rows = state.finish_rows();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].o_orderpriority, "5-LOW");
    assert_eq!(rows[0].order_count, 1);
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q5_sidecar_sums_asia_revenue_for_matching_customer_supplier_nation() {
    let mut state = TpchQ5BuildState::default();
    state.ingest("region", "1|ASIA|comment").expect("region");
    state
        .ingest("nation", "10|JAPAN|1|comment")
        .expect("nation");
    state
        .ingest(
            "supplier",
            "3|Supplier#3|address|10|11-111-1111|0.00|comment",
        )
        .expect("supplier");
    state
        .ingest(
            "customer",
            "1|Customer#1|address|10|11-111-1111|0.00|BUILDING|comment",
        )
        .expect("customer");
    state
        .ingest("orders", "10|1|O|100.00|1994-06-01|5-LOW|Clerk#1|0|comment")
        .expect("orders");

    let mut payload = vec![0, 0];
    for value in [10_i32, 2, 3, 1] {
        payload.extend_from_slice(&value.to_le_bytes());
    }
    for value in [100_i64, 10_000, 5, 0] {
        encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
    }
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.push(b'N');
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.push(b'O');
    payload.extend_from_slice(&(-2_000_i32).to_le_bytes());

    state
        .ingest_lineitem_payload(&payload)
        .expect("lineitem payload");
    let rows = state.finish_rows();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].n_name, "JAPAN");
    assert_eq!(rows[0].revenue, 9_500);
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q5_sidecar_rejects_discount_factor_overflow() {
    let mut state = TpchQ5BuildState::default();
    state.ingest("region", "1|ASIA|comment").expect("region");
    state
        .ingest("nation", "10|JAPAN|1|comment")
        .expect("nation");
    state
        .ingest(
            "supplier",
            "3|Supplier#3|address|10|11-111-1111|0.00|comment",
        )
        .expect("supplier");
    state
        .ingest(
            "customer",
            "1|Customer#1|address|10|11-111-1111|0.00|BUILDING|comment",
        )
        .expect("customer");
    state
        .ingest("orders", "10|1|O|100.00|1994-06-01|5-LOW|Clerk#1|0|comment")
        .expect("orders");

    let mut payload = vec![0, 0];
    for value in [10_i32, 2, 3, 1] {
        payload.extend_from_slice(&value.to_le_bytes());
    }
    for value in [100_i64, 10_000, i64::MIN, 0] {
        encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
    }

    let err = state
        .ingest_lineitem_payload(&payload)
        .expect_err("discount factor overflow should reject");

    assert!(err.to_string().contains("TPC-H sidecar revenue overflow"));
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q7_sidecar_sums_france_germany_revenue_by_year() {
    let mut state = TpchQ7BuildState::default();
    state
        .ingest("nation", "1|FRANCE|0|comment")
        .expect("france");
    state
        .ingest("nation", "2|GERMANY|0|comment")
        .expect("germany");
    state
        .ingest(
            "supplier",
            "3|Supplier#3|address|1|11-111-1111|0.00|comment",
        )
        .expect("supplier");
    state
        .ingest(
            "customer",
            "4|Customer#4|address|2|11-111-1111|0.00|BUILDING|comment",
        )
        .expect("customer");
    state
        .ingest("orders", "10|4|O|100.00|1995-06-01|5-LOW|Clerk#1|0|comment")
        .expect("orders");

    let mut payload = vec![0, 0];
    for value in [10_i32, 2, 3, 1] {
        payload.extend_from_slice(&value.to_le_bytes());
    }
    for value in [100_i64, 10_000, 5, 0] {
        encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
    }
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.push(b'N');
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.push(b'O');
    payload.extend_from_slice(&(-1_700_i32).to_le_bytes());

    state
        .ingest_lineitem_payload(&payload)
        .expect("lineitem payload");
    let rows = state.finish_rows();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].supp_nation, "FRANCE");
    assert_eq!(rows[0].cust_nation, "GERMANY");
    assert_eq!(rows[0].l_year, 1995);
    assert_eq!(rows[0].revenue, 9_500);
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q7_sidecar_rejects_discount_factor_overflow() {
    let mut state = TpchQ7BuildState::default();
    state
        .ingest("nation", "1|FRANCE|0|comment")
        .expect("france");
    state
        .ingest("nation", "2|GERMANY|0|comment")
        .expect("germany");
    state
        .ingest(
            "supplier",
            "3|Supplier#3|address|1|11-111-1111|0.00|comment",
        )
        .expect("supplier");
    state
        .ingest(
            "customer",
            "4|Customer#4|address|2|11-111-1111|0.00|BUILDING|comment",
        )
        .expect("customer");
    state
        .ingest("orders", "10|4|O|100.00|1995-06-01|5-LOW|Clerk#1|0|comment")
        .expect("orders");

    let mut payload = vec![0, 0];
    for value in [10_i32, 2, 3, 1] {
        payload.extend_from_slice(&value.to_le_bytes());
    }
    for value in [100_i64, 10_000, i64::MIN, 0] {
        encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
    }
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.push(b'N');
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.push(b'O');
    payload.extend_from_slice(&(-1_700_i32).to_le_bytes());

    let err = state
        .ingest_lineitem_payload(&payload)
        .expect_err("discount factor overflow should reject");

    assert!(err.to_string().contains("TPC-H sidecar revenue overflow"));
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q8_sidecar_computes_brazil_market_share_by_year() {
    let mut state = TpchQ8BuildState::default();
    state.ingest("region", "1|AMERICA|comment").expect("region");
    state
        .ingest("nation", "2|BRAZIL|1|comment")
        .expect("nation");
    state
        .ingest(
            "supplier",
            "3|Supplier#3|address|2|11-111-1111|0.00|comment",
        )
        .expect("supplier");
    state
        .ingest(
            "customer",
            "4|Customer#4|address|2|11-111-1111|0.00|BUILDING|comment",
        )
        .expect("customer");
    state
        .ingest(
            "part",
            "5|name|MFGR#1|brand|ECONOMY ANODIZED STEEL|15|container|123.45|comment",
        )
        .expect("part");
    state
        .ingest("orders", "10|4|O|100.00|1995-06-01|5-LOW|Clerk#1|0|comment")
        .expect("orders");

    let mut payload = vec![0, 0];
    for value in [10_i32, 5, 3, 1] {
        payload.extend_from_slice(&value.to_le_bytes());
    }
    for value in [100_i64, 10_000, 5, 0] {
        encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
    }

    state
        .ingest_lineitem_payload(&payload)
        .expect("lineitem payload");
    let rows = state.finish_rows();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].o_year, 1995);
    assert_eq!(rows[0].mkt_share, 1.0);
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q8_sidecar_rejects_discount_factor_overflow() {
    let mut state = TpchQ8BuildState::default();
    state.ingest("region", "1|AMERICA|comment").expect("region");
    state
        .ingest("nation", "2|BRAZIL|1|comment")
        .expect("nation");
    state
        .ingest(
            "supplier",
            "3|Supplier#3|address|2|11-111-1111|0.00|comment",
        )
        .expect("supplier");
    state
        .ingest(
            "customer",
            "4|Customer#4|address|2|11-111-1111|0.00|BUILDING|comment",
        )
        .expect("customer");
    state
        .ingest(
            "part",
            "5|name|MFGR#1|brand|ECONOMY ANODIZED STEEL|15|container|123.45|comment",
        )
        .expect("part");
    state
        .ingest("orders", "10|4|O|100.00|1995-06-01|5-LOW|Clerk#1|0|comment")
        .expect("orders");

    let mut payload = vec![0, 0];
    for value in [10_i32, 5, 3, 1] {
        payload.extend_from_slice(&value.to_le_bytes());
    }
    for value in [100_i64, 10_000, i64::MIN, 0] {
        encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
    }

    let err = state
        .ingest_lineitem_payload(&payload)
        .expect_err("discount factor overflow should reject");

    assert!(err.to_string().contains("TPC-H sidecar revenue overflow"));
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q9_sidecar_computes_green_part_profit_by_nation_year() {
    let mut state = TpchQ9BuildState::default();
    state
        .ingest("nation", "2|BRAZIL|1|comment")
        .expect("nation");
    state
        .ingest(
            "supplier",
            "3|Supplier#3|address|2|11-111-1111|0.00|comment",
        )
        .expect("supplier");
    state
        .ingest(
            "part",
            "5|forest green part|MFGR#1|brand|TYPE|15|container|123.45|comment",
        )
        .expect("part");
    state
        .ingest("partsupp", "5|3|1|40.00|comment")
        .expect("partsupp");
    state
        .ingest("orders", "10|4|O|100.00|1995-06-01|5-LOW|Clerk#1|0|comment")
        .expect("orders");

    let mut payload = vec![0, 0];
    for value in [10_i32, 5, 3, 1] {
        payload.extend_from_slice(&value.to_le_bytes());
    }
    for value in [100_i64, 10_000, 5, 0] {
        encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
    }

    state
        .ingest_lineitem_payload(&payload)
        .expect("lineitem payload");
    let rows = state.finish_rows();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].nation, "BRAZIL");
    assert_eq!(rows[0].o_year, 1995);
    assert_eq!(rows[0].sum_profit, 5_500);
}

#[cfg(feature = "sql-bench")]
#[test]
fn tpch_q9_sidecar_rejects_discount_factor_overflow() {
    let mut state = TpchQ9BuildState::default();
    state
        .ingest("nation", "2|BRAZIL|1|comment")
        .expect("nation");
    state
        .ingest(
            "supplier",
            "3|Supplier#3|address|2|11-111-1111|0.00|comment",
        )
        .expect("supplier");
    state
        .ingest(
            "part",
            "5|forest green part|MFGR#1|brand|TYPE|15|container|123.45|comment",
        )
        .expect("part");
    state
        .ingest("partsupp", "5|3|1|40.00|comment")
        .expect("partsupp");
    state
        .ingest("orders", "10|4|O|100.00|1995-06-01|5-LOW|Clerk#1|0|comment")
        .expect("orders");

    let mut payload = vec![0, 0];
    for value in [10_i32, 5, 3, 1] {
        payload.extend_from_slice(&value.to_le_bytes());
    }
    for value in [100_i64, 10_000, i64::MIN] {
        encode_direct_decimal(&mut payload, value, 2, 0).expect("decimal payload");
    }

    let err = state
        .ingest_lineitem_payload(&payload)
        .expect_err("discount factor overflow should reject");

    assert!(err.to_string().contains("TPC-H sidecar revenue overflow"));
}
