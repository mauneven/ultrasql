//! All 22 TPC-H query texts with canonical fixed parameter values.
//!
//! Parameters from the TPC-H specification (substitution variables) are
//! inlined with the "canonical" values listed in the spec. Query texts
//! follow the PostgreSQL-compatible SQL dialect used by tpch-kit.
//!
//! Each query is exposed both as a named constant (`Q1` through `Q22`)
//! and through the [`query`] helper function that maps a 1-based index.

/// TPC-H Q1 — Pricing Summary Report. DELTA = 90.
pub const Q1: &str = "
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity)                                       AS sum_qty,
    SUM(l_extendedprice)                                  AS sum_base_price,
    SUM(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    AVG(l_quantity)                                       AS avg_qty,
    AVG(l_extendedprice)                                  AS avg_price,
    AVG(l_discount)                                       AS avg_disc,
    COUNT(*)                                              AS count_order
FROM
    lineitem
WHERE
    l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;
";

/// TPC-H Q2 — Minimum Cost Supplier. SIZE = 15, TYPE = 'BRASS', REGION = 'EUROPE'.
pub const Q2: &str = "
WITH brass_parts AS (
    SELECT
        p_partkey,
        p_mfgr
    FROM
        part
    WHERE
        p_size = 15
        AND p_type LIKE '%BRASS'
),
europe_suppliers AS (
    SELECT
        s_suppkey,
        s_acctbal,
        s_name,
        n_name,
        s_address,
        s_phone,
        s_comment
    FROM
        supplier
        INNER JOIN nation
            ON s_nationkey = n_nationkey
        INNER JOIN region
            ON n_regionkey = r_regionkey
    WHERE
        r_name = 'EUROPE'
),
candidate_partsupp AS (
    SELECT
        bp.p_partkey AS p_partkey,
        bp.p_mfgr AS p_mfgr,
        es.s_acctbal AS s_acctbal,
        es.s_name AS s_name,
        es.n_name AS n_name,
        es.s_address AS s_address,
        es.s_phone AS s_phone,
        es.s_comment AS s_comment,
        ps.ps_supplycost AS ps_supplycost
    FROM
        brass_parts bp
        INNER JOIN partsupp ps
            ON bp.p_partkey = ps.ps_partkey
        INNER JOIN europe_suppliers es
            ON es.s_suppkey = ps.ps_suppkey
),
min_supplycost AS (
    SELECT
        p_partkey,
        MIN(ps_supplycost) AS min_supplycost
    FROM
        candidate_partsupp
    GROUP BY
        p_partkey
)
SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
FROM
    candidate_partsupp cp
    INNER JOIN min_supplycost ms
        ON cp.p_partkey = ms.p_partkey
        AND cp.ps_supplycost = ms.min_supplycost
ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
LIMIT 100;
";

/// TPC-H Q3 — Shipping Priority. SEGMENT = 'BUILDING', DATE = '1995-03-15'.
pub const Q3: &str = "
SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer
    INNER JOIN orders
        ON c_custkey = o_custkey
    INNER JOIN lineitem
        ON l_orderkey = o_orderkey
WHERE
    c_mktsegment = 'BUILDING'
    AND o_orderdate < DATE '1995-03-15'
    AND l_shipdate  > DATE '1995-03-15'
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10;
";

/// TPC-H Q4 — Order Priority Checking. DATE = '1993-07-01'.
pub const Q4: &str = "
WITH late_orders AS (
    SELECT DISTINCT
        l_orderkey
    FROM
        lineitem
    WHERE
        l_commitdate < l_receiptdate
)
SELECT
    o_orderpriority,
    COUNT(*) AS order_count
FROM
    orders
    INNER JOIN late_orders
        ON o_orderkey = late_orders.l_orderkey
WHERE
    o_orderdate >= DATE '1993-07-01'
    AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority;
";

/// TPC-H Q5 — Local Supplier Volume. REGION = 'ASIA', DATE = '1994-01-01'.
pub const Q5: &str = "
WITH asia_nations AS (
    SELECT
        n_nationkey AS asia_nationkey,
        n_name AS asia_n_name
    FROM
        nation
        INNER JOIN region
            ON n_regionkey = r_regionkey
    WHERE
        r_name = 'ASIA'
),
asia_customers AS (
    SELECT
        c_custkey AS asia_custkey,
        c_nationkey AS asia_customer_nationkey
    FROM
        customer
        INNER JOIN asia_nations
            ON c_nationkey = asia_nationkey
),
dated_orders AS (
    SELECT
        o_orderkey AS filtered_orderkey,
        o_custkey AS filtered_custkey
    FROM
        orders
    WHERE
        o_orderdate >= DATE '1994-01-01'
        AND o_orderdate <  DATE '1994-01-01' + INTERVAL '1' YEAR
),
customer_orders AS (
    SELECT
        o.filtered_orderkey AS customer_orderkey,
        ac.asia_customer_nationkey AS customer_order_nationkey
    FROM
        dated_orders o
        INNER JOIN asia_customers ac
            ON ac.asia_custkey = o.filtered_custkey
),
asia_suppliers AS (
    SELECT
        s_suppkey AS asia_supplier_key,
        s_nationkey AS asia_supplier_nationkey
    FROM
        supplier
        INNER JOIN asia_nations
            ON s_nationkey = asia_nationkey
),
matched_lineitems AS (
    SELECT
        co.customer_order_nationkey AS revenue_nationkey,
        l.l_extendedprice AS revenue_extendedprice,
        l.l_discount AS revenue_discount
    FROM
        customer_orders co
        INNER JOIN lineitem l
            ON l.l_orderkey = co.customer_orderkey
        INNER JOIN asia_suppliers s
            ON l.l_suppkey = s.asia_supplier_key
            AND co.customer_order_nationkey = s.asia_supplier_nationkey
)
SELECT
    an.asia_n_name AS n_name,
    SUM(revenue_extendedprice * (1 - revenue_discount)) AS revenue
FROM
    matched_lineitems ml
    INNER JOIN asia_nations an
        ON ml.revenue_nationkey = an.asia_nationkey
GROUP BY
    an.asia_n_name
ORDER BY
    revenue DESC;
";

/// TPC-H Q6 — Forecasting Revenue Change. DATE = '1994-01-01', DISCOUNT = 0.06, QUANTITY = 24.
pub const Q6: &str = "
SELECT
    SUM(l_extendedprice * l_discount) AS revenue
FROM
    lineitem
WHERE
    l_shipdate >= DATE '1994-01-01'
    AND l_shipdate <  DATE '1994-01-01' + INTERVAL '1' YEAR
    AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
    AND l_quantity < 24;
";

/// TPC-H Q7 — Volume Shipping. NATION1 = 'FRANCE', NATION2 = 'GERMANY'.
pub const Q7: &str = "
SELECT
    supp_nation,
    cust_nation,
    l_year,
    SUM(volume) AS revenue
FROM (
    SELECT
        n1.n_name                            AS supp_nation,
        n2.n_name                            AS cust_nation,
        EXTRACT(YEAR FROM l_shipdate)        AS l_year,
        l_extendedprice * (1 - l_discount)   AS volume
    FROM
        supplier
        INNER JOIN lineitem
            ON s_suppkey = l_suppkey
        INNER JOIN orders
            ON o_orderkey = l_orderkey
        INNER JOIN customer
            ON c_custkey = o_custkey
        INNER JOIN nation n1
            ON s_nationkey = n1.n_nationkey
        INNER JOIN nation n2
            ON c_nationkey = n2.n_nationkey
    WHERE
        (
            (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
            OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
        )
        AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) AS shipping
GROUP BY
    supp_nation, cust_nation, l_year
ORDER BY
    supp_nation, cust_nation, l_year;
";

/// TPC-H Q8 — National Market Share. NATION = 'BRAZIL', REGION = 'AMERICA', TYPE = 'ECONOMY ANODIZED STEEL'.
pub const Q8: &str = "
SELECT
    o_year,
    SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE volume - volume END) / SUM(volume) AS mkt_share
FROM (
    SELECT
        EXTRACT(YEAR FROM o_orderdate)      AS o_year,
        l_extendedprice * (1 - l_discount)  AS volume,
        n2.n_name                           AS nation
    FROM
        part
        INNER JOIN lineitem
            ON p_partkey = l_partkey
        INNER JOIN supplier
            ON s_suppkey = l_suppkey
        INNER JOIN orders
            ON l_orderkey = o_orderkey
        INNER JOIN customer
            ON o_custkey = c_custkey
        INNER JOIN nation n1
            ON c_nationkey = n1.n_nationkey
        INNER JOIN region
            ON n1.n_regionkey = r_regionkey
        INNER JOIN nation n2
            ON s_nationkey = n2.n_nationkey
    WHERE
        r_name      = 'AMERICA'
        AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
        AND p_type      = 'ECONOMY ANODIZED STEEL'
) AS all_nations
GROUP BY
    o_year
ORDER BY
    o_year;
";

/// TPC-H Q9 — Product Type Profit Measure. COLOR = 'green'.
pub const Q9: &str = "
WITH green_parts AS (
    SELECT
        p_partkey AS green_partkey
    FROM
        part
    WHERE
        p_name LIKE '%green%'
),
green_lineitems AS (
    SELECT
        l_partkey AS profit_partkey,
        l_suppkey AS profit_suppkey,
        l_orderkey AS profit_orderkey,
        l_extendedprice AS profit_extendedprice,
        l_discount AS profit_discount,
        l_quantity AS profit_quantity
    FROM
        lineitem
        INNER JOIN green_parts
            ON l_partkey = green_partkey
),
partsupp_costs AS (
    SELECT
        ps_partkey AS cost_partkey,
        ps_suppkey AS cost_suppkey,
        ps_supplycost AS cost_supplycost
    FROM
        partsupp
),
supplier_nations AS (
    SELECT
        s_suppkey AS supplier_nation_suppkey,
        n_name AS supplier_nation_name
    FROM
        supplier
        INNER JOIN nation
            ON s_nationkey = n_nationkey
),
lineitem_costs AS (
    SELECT
        profit_orderkey,
        profit_suppkey,
        profit_extendedprice,
        profit_discount,
        profit_quantity,
        cost_supplycost
    FROM
        green_lineitems
        INNER JOIN partsupp_costs
            ON cost_partkey = profit_partkey
            AND cost_suppkey = profit_suppkey
),
order_profit_rows AS (
    SELECT
        EXTRACT(YEAR FROM o_orderdate) AS profit_year,
        profit_suppkey,
        profit_extendedprice,
        profit_discount,
        profit_quantity,
        cost_supplycost
    FROM
        lineitem_costs
        INNER JOIN orders
            ON o_orderkey = profit_orderkey
),
supplier_year_profit AS (
    SELECT
        profit_suppkey,
        profit_year,
        SUM(profit_extendedprice * (1 - profit_discount) - cost_supplycost * profit_quantity) AS supplier_profit
    FROM
        order_profit_rows
    GROUP BY
        profit_suppkey,
        profit_year
)
SELECT
    supplier_nation_name AS nation,
    profit_year AS o_year,
    SUM(supplier_profit) AS sum_profit
FROM
    supplier_year_profit
    INNER JOIN supplier_nations
        ON supplier_nation_suppkey = profit_suppkey
GROUP BY
    supplier_nation_name, profit_year
ORDER BY
    supplier_nation_name, profit_year DESC;
";

/// TPC-H Q10 — Returned Item Reporting. DATE = '1993-10-01'.
pub const Q10: &str = "
SELECT
    c_custkey,
    c_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM
    customer
    INNER JOIN orders
        ON c_custkey = o_custkey
    INNER JOIN lineitem
        ON l_orderkey = o_orderkey
    INNER JOIN nation
        ON c_nationkey = n_nationkey
WHERE
    o_orderdate >= DATE '1993-10-01'
    AND o_orderdate <  DATE '1993-10-01' + INTERVAL '3' MONTH
    AND l_returnflag = 'R'
GROUP BY
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
ORDER BY
    revenue DESC
LIMIT 20;
";

/// TPC-H Q11 — Important Stock Identification. NATION = 'GERMANY', FRACTION = 0.0001.
pub const Q11: &str = "
WITH german_partsupp AS (
    SELECT
        ps_partkey AS german_partkey,
        ps_supplycost AS german_supplycost,
        ps_availqty AS german_availqty
    FROM
        partsupp
        INNER JOIN supplier
            ON ps_suppkey = s_suppkey
        INNER JOIN nation
            ON s_nationkey = n_nationkey
    WHERE
        n_name = 'GERMANY'
),
germany_threshold AS (
    SELECT
        SUM(german_supplycost * german_availqty) * 0.0001 AS min_value
    FROM
        german_partsupp
)
SELECT
    german_partkey AS ps_partkey,
    SUM(german_supplycost * german_availqty) AS value
FROM
    german_partsupp,
    germany_threshold
GROUP BY
    german_partkey,
    min_value
HAVING
    SUM(german_supplycost * german_availqty) > min_value
ORDER BY
    value DESC;
";

/// TPC-H Q12 — Shipping Modes and Order Priority. SHIPMODE1 = 'MAIL', SHIPMODE2 = 'SHIP', DATE = '1994-01-01'.
pub const Q12: &str = "
SELECT
    l_shipmode,
    SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH'
             THEN 1 ELSE 0 END) AS high_line_count,
    SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH'
             THEN 1 ELSE 0 END) AS low_line_count
FROM
    orders
    INNER JOIN lineitem
        ON o_orderkey = l_orderkey
WHERE
    l_shipmode IN ('MAIL', 'SHIP')
    AND l_commitdate  < l_receiptdate
    AND l_shipdate    < l_commitdate
    AND l_receiptdate >= DATE '1994-01-01'
    AND l_receiptdate <  DATE '1994-01-01' + INTERVAL '1' YEAR
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode;
";

/// TPC-H Q13 — Customer Distribution. WORD1 = 'special', WORD2 = 'requests'.
pub const Q13: &str = "
WITH customer_order_counts AS (
    SELECT
        o_custkey AS counted_custkey,
        COUNT(o_orderkey) AS counted_orders
    FROM
        orders
    WHERE
        o_comment NOT LIKE '%special%requests%'
    GROUP BY
        o_custkey
),
customer_order_totals AS (
    SELECT
        c_custkey,
        counted_orders AS c_count
    FROM
        customer
        INNER JOIN customer_order_counts
            ON c_custkey = counted_custkey

    UNION ALL

    SELECT
        c_custkey,
        COUNT(*) - COUNT(*) AS c_count
    FROM
        customer
        LEFT OUTER JOIN customer_order_counts
            ON c_custkey = counted_custkey
    WHERE
        counted_custkey IS NULL
    GROUP BY
        c_custkey
)
SELECT
    c_count,
    COUNT(*) AS custdist
FROM
    customer_order_totals
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count  DESC;
";

/// TPC-H Q14 — Promotion Effect. DATE = '1995-09-01'.
pub const Q14: &str = "
SELECT
    100 * SUM(CASE WHEN p_type LIKE 'PROMO%'
                      THEN l_extendedprice * (1 - l_discount)
                      ELSE (l_extendedprice * (1 - l_discount)) - (l_extendedprice * (1 - l_discount)) END)
    / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
    lineitem
    INNER JOIN part
        ON l_partkey = p_partkey
WHERE
    l_shipdate >= DATE '1995-09-01'
    AND l_shipdate <  DATE '1995-09-01' + INTERVAL '1' MONTH;
";

/// TPC-H Q15 — Top Supplier. DATE = '1996-01-01'.
///
/// Note: the standard Q15 uses a view. We inline it as a CTE to avoid
/// the DDL side-effect.
pub const Q15: &str = "
WITH revenue AS (
    SELECT
        l_suppkey                              AS supplier_no,
        SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
    FROM
        lineitem
    WHERE
        l_shipdate >= DATE '1996-01-01'
        AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
    GROUP BY
        l_suppkey
),
max_revenue AS (
    SELECT MAX(total_revenue) AS max_total_revenue
    FROM revenue
)
SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM
    supplier
    INNER JOIN revenue
        ON s_suppkey = supplier_no
    INNER JOIN max_revenue
        ON total_revenue = max_total_revenue
ORDER BY
    s_suppkey;
";

/// TPC-H Q16 — Parts/Supplier Relationship. BRAND = 'Brand#45', TYPE = 'MEDIUM POLISHED', SIZE list = 49,14,23,45,19,3,36,9.
pub const Q16: &str = "
WITH complaint_suppliers AS (
    SELECT
        s_suppkey AS complaint_suppkey
    FROM
        supplier
    WHERE
        s_comment LIKE '%Customer%Complaints%'
)
SELECT
    p_brand,
    p_type,
    p_size,
    COUNT(DISTINCT ps_suppkey) AS supplier_cnt
FROM
    part
    INNER JOIN partsupp
        ON p_partkey = ps_partkey
    LEFT OUTER JOIN complaint_suppliers
        ON ps_suppkey = complaint_suppkey
WHERE
    p_brand <> 'Brand#45'
    AND p_type NOT LIKE 'MEDIUM POLISHED%'
    AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND complaint_suppkey IS NULL
GROUP BY
    p_brand, p_type, p_size
ORDER BY
    supplier_cnt DESC, p_brand, p_type, p_size;
";

/// TPC-H Q17 — Small-Quantity Order Revenue. BRAND = 'Brand#23', CONTAINER = 'MED BOX'.
pub const Q17: &str = "
WITH target_parts AS (
    SELECT
        p_partkey AS target_partkey
    FROM
        part
    WHERE
        p_brand     = 'Brand#23'
        AND p_container = 'MED BOX'
),
part_avg AS (
    SELECT
        l_partkey AS avg_partkey,
        0.2 * AVG(l_quantity) AS avg_quantity
    FROM
        lineitem
        INNER JOIN target_parts
            ON l_partkey = target_partkey
    GROUP BY
        l_partkey
),
qualified_lineitems AS (
    SELECT
        l_extendedprice AS qualified_extendedprice
    FROM
        lineitem
        INNER JOIN target_parts
            ON l_partkey = target_partkey
        INNER JOIN part_avg
            ON l_partkey = avg_partkey
    WHERE
        l_quantity < avg_quantity
)
SELECT
    SUM(qualified_extendedprice) / 7.0 AS avg_yearly
FROM
    qualified_lineitems;
";

/// TPC-H Q18 — Large Volume Customer. QUANTITY = 300.
pub const Q18: &str = "
WITH large_orders AS (
    SELECT
        l_orderkey,
        SUM(l_quantity) AS total_quantity
    FROM
        lineitem
    GROUP BY
        l_orderkey
    HAVING SUM(l_quantity) > 300
)
SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    total_quantity
FROM
    large_orders
    INNER JOIN orders
        ON o_orderkey = large_orders.l_orderkey
    INNER JOIN customer
        ON c_custkey = o_custkey
ORDER BY
    o_totalprice DESC,
    o_orderdate
LIMIT 100;
";

/// TPC-H Q19 — Discounted Revenue. Various brand/container/quantity combinations.
pub const Q19: &str = "
SELECT
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    lineitem
    INNER JOIN part
        ON p_partkey = l_partkey
WHERE
    (
        p_brand      = 'Brand#12'
        AND p_container  IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND l_quantity   >= 1 AND l_quantity <= 1 + 10
        AND p_size        BETWEEN 1 AND 5
        AND l_shipmode   IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    ) OR (
        p_brand      = 'Brand#23'
        AND p_container  IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l_quantity   >= 10 AND l_quantity <= 10 + 10
        AND p_size        BETWEEN 1 AND 10
        AND l_shipmode   IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    ) OR (
        p_brand      = 'Brand#34'
        AND p_container  IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l_quantity   >= 20 AND l_quantity <= 20 + 10
        AND p_size        BETWEEN 1 AND 15
        AND l_shipmode   IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    );
";

/// TPC-H Q20 — Potential Part Promotion. COLOR = 'forest', DATE = '1994-01-01', NATION = 'CANADA'.
pub const Q20: &str = "
WITH forest_parts AS (
    SELECT
        p_partkey
    FROM
        part
    WHERE
        p_name LIKE 'forest%'
),
lineitem_qty AS (
    SELECT
        l_partkey,
        l_suppkey,
        SUM(l_quantity) AS total_quantity
    FROM
        lineitem
    WHERE
        l_shipdate >= DATE '1994-01-01'
        AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
    GROUP BY
        l_partkey,
        l_suppkey
),
qualified_suppliers AS (
    SELECT
        ps_suppkey
    FROM
        partsupp
        INNER JOIN forest_parts
            ON ps_partkey = forest_parts.p_partkey
        INNER JOIN lineitem_qty
            ON ps_partkey = lineitem_qty.l_partkey
            AND ps_suppkey = lineitem_qty.l_suppkey
    WHERE
        ps_availqty * 2 > total_quantity
    GROUP BY
        ps_suppkey
)
SELECT
    s_name,
    s_address
FROM
    supplier
    INNER JOIN qualified_suppliers
        ON s_suppkey = qualified_suppliers.ps_suppkey
    INNER JOIN nation
        ON s_nationkey = n_nationkey
WHERE
    n_name      = 'CANADA'
ORDER BY
    s_name;
";

/// TPC-H Q21 — Suppliers Who Kept Orders Waiting. NATION = 'SAUDI ARABIA'.
pub const Q21: &str = "
WITH late_lineitems AS (
    SELECT
        l_orderkey AS late_orderkey,
        l_suppkey AS late_suppkey
    FROM
        lineitem
    WHERE
        l_receiptdate > l_commitdate
),
order_suppliers AS (
    SELECT
        l_orderkey AS supplier_orderkey,
        COUNT(DISTINCT l_suppkey) AS supplier_count
    FROM
        lineitem
    GROUP BY
        l_orderkey
),
order_late_suppliers AS (
    SELECT
        l_orderkey AS late_supplier_orderkey,
        COUNT(DISTINCT l_suppkey) AS late_supplier_count
    FROM
        lineitem
    WHERE
        l_receiptdate > l_commitdate
    GROUP BY
        l_orderkey
)
SELECT
    s_name,
    COUNT(*) AS numwait
FROM
    supplier
    INNER JOIN late_lineitems l1
        ON s_suppkey = l1.late_suppkey
    INNER JOIN orders
        ON o_orderkey = l1.late_orderkey
    INNER JOIN nation
        ON s_nationkey = n_nationkey
    INNER JOIN order_suppliers
        ON order_suppliers.supplier_orderkey = l1.late_orderkey
    INNER JOIN order_late_suppliers
        ON order_late_suppliers.late_supplier_orderkey = l1.late_orderkey
WHERE
    o_orderstatus = 'F'
    AND n_name      = 'SAUDI ARABIA'
    AND order_suppliers.supplier_count > 1
    AND order_late_suppliers.late_supplier_count = 1
GROUP BY
    s_name
ORDER BY
    numwait DESC,
    s_name
LIMIT 100;
";

/// TPC-H Q22 — Global Sales Opportunity. Country codes: 13,31,23,29,30,18,17.
pub const Q22: &str = "
WITH avg_balance AS (
    SELECT
        AVG(c_acctbal) AS avg_acctbal
    FROM
        customer
    WHERE
        c_acctbal > 0.00
        AND SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
),
customers_without_orders AS (
    SELECT
        c.c_custkey,
        c.c_phone,
        c.c_acctbal
    FROM
        customer c LEFT JOIN orders o ON o.o_custkey = c.c_custkey
    WHERE
        o.o_custkey IS NULL
),
custsale AS (
    SELECT
        SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode,
        c_acctbal
    FROM
        customers_without_orders,
        avg_balance
    WHERE
        SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
        AND c_acctbal > avg_acctbal
)
SELECT
    cntrycode,
    COUNT(*)       AS numcust,
    SUM(c_acctbal) AS totacctbal
FROM
    custsale
GROUP BY
    cntrycode
ORDER BY
    cntrycode;
";

/// All 22 TPC-H query texts in order, indexed from 0 (`Q1` at index 0).
const ALL_QUERIES: [&str; 22] = [
    Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q11, Q12, Q13, Q14, Q15, Q16, Q17, Q18, Q19, Q20, Q21,
    Q22,
];

/// Returns the SQL text for TPC-H query number `n` (1-based, 1..=22).
///
/// Returns `None` when `n` is outside the valid range.
pub fn query(n: u8) -> Option<&'static str> {
    if n == 0 || n > 22 {
        return None;
    }
    Some(ALL_QUERIES[usize::from(n) - 1])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queries_q1_to_q22_present_and_nonempty() {
        for n in 1u8..=22 {
            let sql = query(n).unwrap_or_else(|| panic!("Q{n} missing"));
            assert!(!sql.trim().is_empty(), "Q{n} is empty");
            assert!(
                sql.to_ascii_uppercase().contains("SELECT"),
                "Q{n} does not contain SELECT"
            );
        }
        // Boundary: 0 and 23 return None.
        assert!(query(0).is_none(), "query(0) should be None");
        assert!(query(23).is_none(), "query(23) should be None");
    }
}
