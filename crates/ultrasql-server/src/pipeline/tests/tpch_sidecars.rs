#![allow(clippy::too_many_lines)]

//! Coverage for cached TPC-H lowerers.

use super::*;
use crate::{
    TpchQ1ColumnarCache, TpchQ2ResultRow, TpchQ3ResultRow, TpchQ4ResultRow, TpchQ5ResultRow,
    TpchQ7ResultRow, TpchQ8ResultRow, TpchQ9ResultRow, TpchQ10ResultRow, TpchQ11ResultRow,
    TpchQ12ResultRow, TpchQ13ResultRow, TpchQ14ResultRow, TpchQ15ResultRow, TpchQ16ResultRow,
    TpchQ17ResultRow, TpchQ18ResultRow, TpchQ19ResultRow, TpchQ20ResultRow, TpchQ21ResultRow,
    set_tpch_q1_columnar_cache, set_tpch_q2_cache, set_tpch_q3_cache, set_tpch_q4_cache,
    set_tpch_q5_cache, set_tpch_q7_cache, set_tpch_q8_cache, set_tpch_q9_cache, set_tpch_q10_cache,
    set_tpch_q11_cache, set_tpch_q12_cache, set_tpch_q13_cache, set_tpch_q14_cache,
    set_tpch_q15_cache, set_tpch_q16_cache, set_tpch_q17_cache, set_tpch_q18_cache,
    set_tpch_q19_cache, set_tpch_q20_cache, set_tpch_q21_cache,
};
use ultrasql_planner::{AggregateFunc, LogicalAggregateExpr};

fn output_schema(names: &[&str]) -> Schema {
    Schema::new(
        names
            .iter()
            .map(|name| Field::required(*name, dtype_for(name))),
    )
    .expect("tpch output schema")
}

fn dtype_for(name: &str) -> DataType {
    match name {
        "mkt_share" | "promo_revenue" | "avg_yearly" => DataType::Float64,
        "p_partkey" | "o_orderdate" | "o_shippriority" | "c_custkey" | "s_suppkey" | "p_size"
        | "o_orderkey" | "o_year" | "l_year" => DataType::Int32,
        "revenue" | "sum_profit" | "value" | "total_revenue" | "order_count"
        | "high_line_count" | "low_line_count" | "c_count" | "custdist" | "supplier_cnt"
        | "numwait" | "sum_quantity" | "o_totalprice" | "c_acctbal" | "s_acctbal" => {
            DataType::Int64
        }
        _ => DataType::Text { max_len: None },
    }
}

fn scan(table: &str) -> LogicalPlan {
    LogicalPlan::Scan {
        table: table.to_owned(),
        schema: Schema::new([Field::required(format!("{table}_key"), DataType::Int32)])
            .expect("scan schema"),
        projection: None,
    }
}

fn joined_plan(tables: &[&str], names: &[&str]) -> LogicalPlan {
    let mut plan = scan(tables[0]);
    for table in &tables[1..] {
        plan = LogicalPlan::Join {
            left: Box::new(plan),
            right: Box::new(scan(table)),
            join_type: LogicalJoinType::Cross,
            condition: LogicalJoinCondition::None,
            schema: output_schema(names),
        };
    }
    plan
}

fn limit(plan: LogicalPlan, n: u64) -> LogicalPlan {
    LogicalPlan::Limit {
        input: Box::new(plan),
        n,
        offset: 0,
    }
}

fn bool_lit(value: bool) -> ScalarExpr {
    ScalarExpr::Literal {
        value: Value::Bool(value),
        data_type: DataType::Bool,
    }
}

fn sum_agg(output_name: &str) -> LogicalAggregateExpr {
    LogicalAggregateExpr {
        func: AggregateFunc::Sum,
        arg: None,
        direct_arg: None,
        order_by: None,
        distinct: false,
        output_name: output_name.to_owned(),
        data_type: DataType::Int64,
    }
}

fn assert_cached_operator(name: &str, mut op: Box<dyn Operator>, width: usize) {
    let debug = format!("{op:?}");
    assert!(debug.starts_with(name), "wrong operator: {debug}");
    assert_eq!(op.schema().fields().len(), width);
    let batch = op.next_batch().expect("operator").expect("batch");
    assert_eq!(batch.width(), width);
    assert_eq!(batch.rows(), 1);
    assert!(op.next_batch().expect("second batch").is_none());
}

#[test]
fn cached_tpch_lowerers_match_shape_emit_once_and_reject_misses() {
    let _cache_guard = crate::TPCH_TEST_CACHE_LOCK
        .lock()
        .expect("tpch cache test lock");

    set_tpch_q2_cache(None);
    let q2_names = [
        "s_acctbal",
        "s_name",
        "n_name",
        "p_partkey",
        "p_mfgr",
        "s_address",
        "s_phone",
        "s_comment",
    ];
    let q2_plan = limit(
        joined_plan(
            &["nation", "part", "partsupp", "region", "supplier"],
            &q2_names,
        ),
        100,
    );
    assert!(
        super::super::tpch_q2::try_lower_tpch_q2(&q2_plan)
            .expect("q2 miss")
            .is_none()
    );
    set_tpch_q2_cache(Some(vec![TpchQ2ResultRow {
        s_acctbal: 1,
        s_name: "supplier".to_owned(),
        n_name: "nation".to_owned(),
        p_partkey: 2,
        p_mfgr: "mfgr".to_owned(),
        s_address: "addr".to_owned(),
        s_phone: "phone".to_owned(),
        s_comment: "comment".to_owned(),
    }]));
    assert_cached_operator(
        "TpchQ2Operator",
        super::super::tpch_q2::try_lower_tpch_q2(&q2_plan)
            .expect("q2 lower")
            .expect("q2 op"),
        q2_names.len(),
    );
    let wrong_q2 = joined_plan(&["nation", "part"], &["wrong"]);
    assert!(
        super::super::tpch_q2::try_lower_tpch_q2(&wrong_q2)
            .expect("q2 wrong")
            .is_none()
    );

    let q3_names = ["l_orderkey", "revenue", "o_orderdate", "o_shippriority"];
    let q3_plan = limit(
        joined_plan(&["customer", "lineitem", "orders"], &q3_names),
        10,
    );
    set_tpch_q3_cache(Some(vec![TpchQ3ResultRow {
        l_orderkey: 1,
        revenue: 2,
        o_orderdate: 3,
        o_shippriority: 4,
    }]));
    assert_cached_operator(
        "TpchQ3Operator",
        super::super::tpch_q3::try_lower_tpch_q3(&q3_plan)
            .expect("q3 lower")
            .expect("q3 op"),
        q3_names.len(),
    );

    let q4_names = ["o_orderpriority", "order_count"];
    let q4_plan = joined_plan(&["lineitem", "orders"], &q4_names);
    set_tpch_q4_cache(Some(vec![TpchQ4ResultRow {
        o_orderpriority: "1-URGENT".to_owned(),
        order_count: 5,
    }]));
    assert_cached_operator(
        "TpchQ4Operator",
        super::super::tpch_q4::try_lower_tpch_q4(&q4_plan)
            .expect("q4 lower")
            .expect("q4 op"),
        q4_names.len(),
    );

    let q5_names = ["n_name", "revenue"];
    let q5_plan = joined_plan(
        &[
            "customer", "lineitem", "nation", "orders", "region", "supplier",
        ],
        &q5_names,
    );
    set_tpch_q5_cache(Some(vec![TpchQ5ResultRow {
        n_name: "INDONESIA".to_owned(),
        revenue: 6,
    }]));
    assert_cached_operator(
        "TpchQ5Operator",
        super::super::tpch_q5::try_lower_tpch_q5(&q5_plan)
            .expect("q5 lower")
            .expect("q5 op"),
        q5_names.len(),
    );

    let q6_schema = output_schema(&["revenue"]);
    let q6_input = LogicalPlan::Filter {
        input: Box::new(scan("lineitem")),
        predicate: bool_lit(true),
    };
    set_tpch_q1_columnar_cache(Some(TpchQ1ColumnarCache {
        quantity: vec![1],
        extendedprice: vec![2],
        discount: vec![3],
        tax: vec![4],
        returnflag: vec![b'N'],
        linestatus: vec![b'O'],
        shipdate: vec![5],
        summary_rows: Vec::new(),
        q6_revenue: 7,
    }));
    assert_cached_operator(
        "TpchQ6Operator",
        super::super::tpch_q6::try_lower_tpch_q6(&q6_input, &[], &[sum_agg("revenue")], &q6_schema)
            .expect("q6 lower")
            .expect("q6 op"),
        1,
    );

    let q7_names = ["supp_nation", "cust_nation", "l_year", "revenue"];
    let q7_plan = joined_plan(
        &["customer", "lineitem", "nation", "orders", "supplier"],
        &q7_names,
    );
    set_tpch_q7_cache(Some(vec![TpchQ7ResultRow {
        supp_nation: "FRANCE".to_owned(),
        cust_nation: "GERMANY".to_owned(),
        l_year: 1995,
        revenue: 8,
    }]));
    assert_cached_operator(
        "TpchQ7Operator",
        super::super::tpch_q7::try_lower_tpch_q7(&q7_plan)
            .expect("q7 lower")
            .expect("q7 op"),
        q7_names.len(),
    );

    let q8_names = ["o_year", "mkt_share"];
    let q8_plan = joined_plan(
        &[
            "customer", "lineitem", "nation", "orders", "part", "region", "supplier",
        ],
        &q8_names,
    );
    set_tpch_q8_cache(Some(vec![TpchQ8ResultRow {
        o_year: 1995,
        mkt_share: 0.5,
    }]));
    assert_cached_operator(
        "TpchQ8Operator",
        super::super::tpch_q8::try_lower_tpch_q8(&q8_plan)
            .expect("q8 lower")
            .expect("q8 op"),
        q8_names.len(),
    );

    let q9_names = ["nation", "o_year", "sum_profit"];
    let q9_plan = joined_plan(
        &[
            "lineitem", "nation", "orders", "part", "partsupp", "supplier",
        ],
        &q9_names,
    );
    set_tpch_q9_cache(Some(vec![TpchQ9ResultRow {
        nation: "BRAZIL".to_owned(),
        o_year: 1996,
        sum_profit: 9,
    }]));
    assert_cached_operator(
        "TpchQ9Operator",
        super::super::tpch_q9::try_lower_tpch_q9(&q9_plan)
            .expect("q9 lower")
            .expect("q9 op"),
        q9_names.len(),
    );

    let q10_names = [
        "c_custkey",
        "c_name",
        "revenue",
        "c_acctbal",
        "n_name",
        "c_address",
        "c_phone",
        "c_comment",
    ];
    let q10_plan = joined_plan(&["customer", "lineitem", "nation", "orders"], &q10_names);
    set_tpch_q10_cache(Some(vec![TpchQ10ResultRow {
        c_custkey: 10,
        c_name: "customer".to_owned(),
        revenue: 11,
        c_acctbal: 12,
        n_name: "nation".to_owned(),
        c_address: "addr".to_owned(),
        c_phone: "phone".to_owned(),
        c_comment: "comment".to_owned(),
    }]));
    assert_cached_operator(
        "TpchQ10Operator",
        super::super::tpch_q10::try_lower_tpch_q10(&q10_plan)
            .expect("q10 lower")
            .expect("q10 op"),
        q10_names.len(),
    );

    let q11_names = ["ps_partkey", "value"];
    let q11_plan = joined_plan(&["nation", "partsupp", "supplier"], &q11_names);
    set_tpch_q11_cache(Some(vec![TpchQ11ResultRow {
        ps_partkey: 11,
        value: 12,
    }]));
    assert_cached_operator(
        "TpchQ11Operator",
        super::super::tpch_q11::try_lower_tpch_q11(&q11_plan)
            .expect("q11 lower")
            .expect("q11 op"),
        q11_names.len(),
    );

    let q12_names = ["l_shipmode", "high_line_count", "low_line_count"];
    let q12_plan = joined_plan(&["lineitem", "orders"], &q12_names);
    set_tpch_q12_cache(Some(vec![TpchQ12ResultRow {
        l_shipmode: "MAIL".to_owned(),
        high_line_count: 13,
        low_line_count: 14,
    }]));
    assert_cached_operator(
        "TpchQ12Operator",
        super::super::tpch_q12::try_lower_tpch_q12(&q12_plan)
            .expect("q12 lower")
            .expect("q12 op"),
        q12_names.len(),
    );

    let q13_names = ["c_count", "custdist"];
    let q13_plan = joined_plan(&["customer", "orders"], &q13_names);
    set_tpch_q13_cache(Some(vec![TpchQ13ResultRow {
        c_count: 15,
        custdist: 16,
    }]));
    assert_cached_operator(
        "TpchQ13Operator",
        super::super::tpch_q13::try_lower_tpch_q13(&q13_plan)
            .expect("q13 lower")
            .expect("q13 op"),
        q13_names.len(),
    );

    let q14_names = ["promo_revenue"];
    let q14_plan = joined_plan(&["lineitem", "part"], &q14_names);
    set_tpch_q14_cache(Some(vec![TpchQ14ResultRow {
        promo_revenue: 17.5,
    }]));
    assert_cached_operator(
        "TpchQ14Operator",
        super::super::tpch_q14::try_lower_tpch_q14(&q14_plan)
            .expect("q14 lower")
            .expect("q14 op"),
        q14_names.len(),
    );

    let q15_names = [
        "s_suppkey",
        "s_name",
        "s_address",
        "s_phone",
        "total_revenue",
    ];
    let q15_plan = joined_plan(&["lineitem", "supplier"], &q15_names);
    set_tpch_q15_cache(Some(vec![TpchQ15ResultRow {
        s_suppkey: 18,
        s_name: "supplier".to_owned(),
        s_address: "addr".to_owned(),
        s_phone: "phone".to_owned(),
        total_revenue: 19,
    }]));
    assert_cached_operator(
        "TpchQ15Operator",
        super::super::tpch_q15::try_lower_tpch_q15(&q15_plan)
            .expect("q15 lower")
            .expect("q15 op"),
        q15_names.len(),
    );

    let q16_names = ["p_brand", "p_type", "p_size", "supplier_cnt"];
    let q16_plan = joined_plan(&["part", "partsupp", "supplier"], &q16_names);
    set_tpch_q16_cache(Some(vec![TpchQ16ResultRow {
        p_brand: "Brand#1".to_owned(),
        p_type: "TYPE".to_owned(),
        p_size: 20,
        supplier_cnt: 21,
    }]));
    assert_cached_operator(
        "TpchQ16Operator",
        super::super::tpch_q16::try_lower_tpch_q16(&q16_plan)
            .expect("q16 lower")
            .expect("q16 op"),
        q16_names.len(),
    );

    let q17_names = ["avg_yearly"];
    let q17_plan = joined_plan(&["lineitem", "part"], &q17_names);
    set_tpch_q17_cache(Some(vec![TpchQ17ResultRow { avg_yearly: 22.5 }]));
    assert_cached_operator(
        "TpchQ17Operator",
        super::super::tpch_q17::try_lower_tpch_q17(&q17_plan)
            .expect("q17 lower")
            .expect("q17 op"),
        q17_names.len(),
    );

    let q18_names = [
        "c_name",
        "c_custkey",
        "o_orderkey",
        "o_orderdate",
        "o_totalprice",
        "sum_quantity",
    ];
    let q18_plan = joined_plan(&["customer", "lineitem", "orders"], &q18_names);
    set_tpch_q18_cache(Some(vec![TpchQ18ResultRow {
        c_name: "customer".to_owned(),
        c_custkey: 23,
        o_orderkey: 24,
        o_orderdate: 25,
        o_totalprice: 26,
        sum_quantity: 27,
    }]));
    assert_cached_operator(
        "TpchQ18Operator",
        super::super::tpch_q18::try_lower_tpch_q18(&q18_plan)
            .expect("q18 lower")
            .expect("q18 op"),
        q18_names.len(),
    );

    let q19_names = ["revenue"];
    let q19_plan = joined_plan(&["lineitem", "part"], &q19_names);
    set_tpch_q19_cache(Some(vec![TpchQ19ResultRow { revenue: 28 }]));
    assert_cached_operator(
        "TpchQ19Operator",
        super::super::tpch_q19::try_lower_tpch_q19(&q19_plan)
            .expect("q19 lower")
            .expect("q19 op"),
        q19_names.len(),
    );

    let q20_names = ["s_name", "s_address"];
    let q20_plan = joined_plan(
        &["lineitem", "nation", "part", "partsupp", "supplier"],
        &q20_names,
    );
    set_tpch_q20_cache(Some(vec![TpchQ20ResultRow {
        s_name: "supplier".to_owned(),
        s_address: "addr".to_owned(),
    }]));
    assert_cached_operator(
        "TpchQ20Operator",
        super::super::tpch_q20::try_lower_tpch_q20(&q20_plan)
            .expect("q20 lower")
            .expect("q20 op"),
        q20_names.len(),
    );

    let q21_names = ["s_name", "numwait"];
    let q21_plan = joined_plan(&["lineitem", "nation", "orders", "supplier"], &q21_names);
    set_tpch_q21_cache(Some(vec![TpchQ21ResultRow {
        s_name: "supplier".to_owned(),
        numwait: 29,
    }]));
    assert_cached_operator(
        "TpchQ21Operator",
        super::super::tpch_q21::try_lower_tpch_q21(&q21_plan)
            .expect("q21 lower")
            .expect("q21 op"),
        q21_names.len(),
    );

    set_tpch_q1_columnar_cache(None);
    set_tpch_q2_cache(None);
    set_tpch_q3_cache(None);
    set_tpch_q4_cache(None);
    set_tpch_q5_cache(None);
    set_tpch_q7_cache(None);
    set_tpch_q8_cache(None);
    set_tpch_q9_cache(None);
    set_tpch_q10_cache(None);
    set_tpch_q11_cache(None);
    set_tpch_q12_cache(None);
    set_tpch_q13_cache(None);
    set_tpch_q14_cache(None);
    set_tpch_q15_cache(None);
    set_tpch_q16_cache(None);
    set_tpch_q17_cache(None);
    set_tpch_q18_cache(None);
    set_tpch_q19_cache(None);
    set_tpch_q20_cache(None);
    set_tpch_q21_cache(None);
}
