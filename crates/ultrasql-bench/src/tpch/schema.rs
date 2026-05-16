//! TPC-H DDL constants for all eight standard tables.
//!
//! Each public constant holds the `CREATE TABLE` statement for one TPC-H
//! table. The statements are compatible with both PostgreSQL and the target
//! UltraSQL surface. Call [`ddl_for_engine`] to obtain the full ordered
//! schema for a given engine.

/// Engine target for DDL generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    /// Target PostgreSQL (any v14+).
    Postgres,
    /// Target UltraSQL (executor wiring pending).
    Ultrasql,
}

/// `REGION` table DDL.
pub const REGION: &str = "\
CREATE TABLE IF NOT EXISTS region (
    r_regionkey  INTEGER       NOT NULL,
    r_name       CHAR(25)      NOT NULL,
    r_comment    VARCHAR(152),
    PRIMARY KEY (r_regionkey)
);";

/// `NATION` table DDL.
pub const NATION: &str = "\
CREATE TABLE IF NOT EXISTS nation (
    n_nationkey  INTEGER       NOT NULL,
    n_name       CHAR(25)      NOT NULL,
    n_regionkey  INTEGER       NOT NULL,
    n_comment    VARCHAR(152),
    PRIMARY KEY (n_nationkey)
);";

/// `SUPPLIER` table DDL.
pub const SUPPLIER: &str = "\
CREATE TABLE IF NOT EXISTS supplier (
    s_suppkey    INTEGER       NOT NULL,
    s_name       CHAR(25)      NOT NULL,
    s_address    VARCHAR(40)   NOT NULL,
    s_nationkey  INTEGER       NOT NULL,
    s_phone      CHAR(15)      NOT NULL,
    s_acctbal    DECIMAL(15,2) NOT NULL,
    s_comment    VARCHAR(101)  NOT NULL,
    PRIMARY KEY (s_suppkey)
);";

/// `CUSTOMER` table DDL.
pub const CUSTOMER: &str = "\
CREATE TABLE IF NOT EXISTS customer (
    c_custkey    INTEGER       NOT NULL,
    c_name       VARCHAR(25)   NOT NULL,
    c_address    VARCHAR(40)   NOT NULL,
    c_nationkey  INTEGER       NOT NULL,
    c_phone      CHAR(15)      NOT NULL,
    c_acctbal    DECIMAL(15,2) NOT NULL,
    c_mktsegment CHAR(10)      NOT NULL,
    c_comment    VARCHAR(117)  NOT NULL,
    PRIMARY KEY (c_custkey)
);";

/// `PART` table DDL.
pub const PART: &str = "\
CREATE TABLE IF NOT EXISTS part (
    p_partkey    INTEGER       NOT NULL,
    p_name       VARCHAR(55)   NOT NULL,
    p_mfgr       CHAR(25)      NOT NULL,
    p_brand      CHAR(10)      NOT NULL,
    p_type       VARCHAR(25)   NOT NULL,
    p_size       INTEGER       NOT NULL,
    p_container  CHAR(10)      NOT NULL,
    p_retailprice DECIMAL(15,2) NOT NULL,
    p_comment    VARCHAR(23)   NOT NULL,
    PRIMARY KEY (p_partkey)
);";

/// `PARTSUPP` (part-supplier) table DDL.
pub const PARTSUPP: &str = "\
CREATE TABLE IF NOT EXISTS partsupp (
    ps_partkey   INTEGER       NOT NULL,
    ps_suppkey   INTEGER       NOT NULL,
    ps_availqty  INTEGER       NOT NULL,
    ps_supplycost DECIMAL(15,2) NOT NULL,
    ps_comment   VARCHAR(199)  NOT NULL,
    PRIMARY KEY (ps_partkey, ps_suppkey)
);";

/// `ORDERS` table DDL.
pub const ORDERS: &str = "\
CREATE TABLE IF NOT EXISTS orders (
    o_orderkey   INTEGER       NOT NULL,
    o_custkey    INTEGER       NOT NULL,
    o_orderstatus CHAR(1)      NOT NULL,
    o_totalprice DECIMAL(15,2) NOT NULL,
    o_orderdate  DATE          NOT NULL,
    o_orderpriority CHAR(15)   NOT NULL,
    o_clerk      CHAR(15)      NOT NULL,
    o_shippriority INTEGER     NOT NULL,
    o_comment    VARCHAR(79)   NOT NULL,
    PRIMARY KEY (o_orderkey)
);";

/// `LINEITEM` table DDL.
pub const LINEITEM: &str = "\
CREATE TABLE IF NOT EXISTS lineitem (
    l_orderkey   INTEGER       NOT NULL,
    l_partkey    INTEGER       NOT NULL,
    l_suppkey    INTEGER       NOT NULL,
    l_linenumber INTEGER       NOT NULL,
    l_quantity   DECIMAL(15,2) NOT NULL,
    l_extendedprice DECIMAL(15,2) NOT NULL,
    l_discount   DECIMAL(15,2) NOT NULL,
    l_tax        DECIMAL(15,2) NOT NULL,
    l_returnflag CHAR(1)       NOT NULL,
    l_linestatus CHAR(1)       NOT NULL,
    l_shipdate   DATE          NOT NULL,
    l_commitdate DATE          NOT NULL,
    l_receiptdate DATE         NOT NULL,
    l_shipinstruct CHAR(25)    NOT NULL,
    l_shipmode   CHAR(10)      NOT NULL,
    l_comment    VARCHAR(44)   NOT NULL,
    PRIMARY KEY (l_orderkey, l_linenumber)
);";

/// `REGION` table DDL targeted at UltraSQL (no table-level PRIMARY KEY).
pub const REGION_ULTRASQL: &str = "\
CREATE TABLE IF NOT EXISTS region (
    r_regionkey  INTEGER       NOT NULL,
    r_name       CHAR(25)      NOT NULL,
    r_comment    VARCHAR(152)
);";

/// `NATION` table DDL targeted at UltraSQL.
pub const NATION_ULTRASQL: &str = "\
CREATE TABLE IF NOT EXISTS nation (
    n_nationkey  INTEGER       NOT NULL,
    n_name       CHAR(25)      NOT NULL,
    n_regionkey  INTEGER       NOT NULL,
    n_comment    VARCHAR(152)
);";

/// `SUPPLIER` table DDL targeted at UltraSQL.
pub const SUPPLIER_ULTRASQL: &str = "\
CREATE TABLE IF NOT EXISTS supplier (
    s_suppkey    INTEGER       NOT NULL,
    s_name       CHAR(25)      NOT NULL,
    s_address    VARCHAR(40)   NOT NULL,
    s_nationkey  INTEGER       NOT NULL,
    s_phone      CHAR(15)      NOT NULL,
    s_acctbal    DECIMAL(15,2) NOT NULL,
    s_comment    VARCHAR(101)  NOT NULL
);";

/// `CUSTOMER` table DDL targeted at UltraSQL.
pub const CUSTOMER_ULTRASQL: &str = "\
CREATE TABLE IF NOT EXISTS customer (
    c_custkey    INTEGER       NOT NULL,
    c_name       VARCHAR(25)   NOT NULL,
    c_address    VARCHAR(40)   NOT NULL,
    c_nationkey  INTEGER       NOT NULL,
    c_phone      CHAR(15)      NOT NULL,
    c_acctbal    DECIMAL(15,2) NOT NULL,
    c_mktsegment CHAR(10)      NOT NULL,
    c_comment    VARCHAR(117)  NOT NULL
);";

/// `PART` table DDL targeted at UltraSQL.
pub const PART_ULTRASQL: &str = "\
CREATE TABLE IF NOT EXISTS part (
    p_partkey    INTEGER       NOT NULL,
    p_name       VARCHAR(55)   NOT NULL,
    p_mfgr       CHAR(25)      NOT NULL,
    p_brand      CHAR(10)      NOT NULL,
    p_type       VARCHAR(25)   NOT NULL,
    p_size       INTEGER       NOT NULL,
    p_container  CHAR(10)      NOT NULL,
    p_retailprice DECIMAL(15,2) NOT NULL,
    p_comment    VARCHAR(23)   NOT NULL
);";

/// `PARTSUPP` table DDL targeted at UltraSQL.
pub const PARTSUPP_ULTRASQL: &str = "\
CREATE TABLE IF NOT EXISTS partsupp (
    ps_partkey   INTEGER       NOT NULL,
    ps_suppkey   INTEGER       NOT NULL,
    ps_availqty  INTEGER       NOT NULL,
    ps_supplycost DECIMAL(15,2) NOT NULL,
    ps_comment   VARCHAR(199)  NOT NULL
);";

/// `ORDERS` table DDL targeted at UltraSQL.
pub const ORDERS_ULTRASQL: &str = "\
CREATE TABLE IF NOT EXISTS orders (
    o_orderkey   INTEGER       NOT NULL,
    o_custkey    INTEGER       NOT NULL,
    o_orderstatus CHAR(1)      NOT NULL,
    o_totalprice DECIMAL(15,2) NOT NULL,
    o_orderdate  DATE          NOT NULL,
    o_orderpriority CHAR(15)   NOT NULL,
    o_clerk      CHAR(15)      NOT NULL,
    o_shippriority INTEGER     NOT NULL,
    o_comment    VARCHAR(79)   NOT NULL
);";

/// `LINEITEM` table DDL targeted at UltraSQL.
pub const LINEITEM_ULTRASQL: &str = "\
CREATE TABLE IF NOT EXISTS lineitem (
    l_orderkey   INTEGER       NOT NULL,
    l_partkey    INTEGER       NOT NULL,
    l_suppkey    INTEGER       NOT NULL,
    l_linenumber INTEGER       NOT NULL,
    l_quantity   DECIMAL(15,2) NOT NULL,
    l_extendedprice DECIMAL(15,2) NOT NULL,
    l_discount   DECIMAL(15,2) NOT NULL,
    l_tax        DECIMAL(15,2) NOT NULL,
    l_returnflag CHAR(1)       NOT NULL,
    l_linestatus CHAR(1)       NOT NULL,
    l_shipdate   DATE          NOT NULL,
    l_commitdate DATE          NOT NULL,
    l_receiptdate DATE         NOT NULL,
    l_shipinstruct CHAR(25)    NOT NULL,
    l_shipmode   CHAR(10)      NOT NULL,
    l_comment    VARCHAR(44)   NOT NULL
);";

/// Returns the ordered slice of DDL statements for all eight TPC-H tables.
///
/// The tables are ordered so that foreign-key targets precede their
/// dependents (region → nation → supplier/customer/orders → partsupp/lineitem).
///
/// The two engine variants differ today on one axis: table-level
/// `PRIMARY KEY` clauses. PostgreSQL accepts them; the UltraSQL CREATE TABLE
/// path rejects table-level constraints in v0.6, so the `Ultrasql` variant
/// drops the PK clause and relies on uniqueness held by the data generator
/// instead (TPC-H keys are unique by construction). The CHAR/VARCHAR/DECIMAL
/// length annotations are accepted by both paths.
pub const fn ddl_for_engine(engine: Engine) -> &'static [&'static str] {
    match engine {
        Engine::Postgres => &[
            REGION, NATION, SUPPLIER, CUSTOMER, PART, PARTSUPP, ORDERS, LINEITEM,
        ],
        Engine::Ultrasql => &[
            REGION_ULTRASQL,
            NATION_ULTRASQL,
            SUPPLIER_ULTRASQL,
            CUSTOMER_ULTRASQL,
            PART_ULTRASQL,
            PARTSUPP_ULTRASQL,
            ORDERS_ULTRASQL,
            LINEITEM_ULTRASQL,
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_ddl_compiles_for_both_engines() {
        for engine in [Engine::Postgres, Engine::Ultrasql] {
            let stmts = ddl_for_engine(engine);
            assert_eq!(stmts.len(), 8, "expected 8 TPC-H tables");
            for stmt in stmts {
                assert!(
                    stmt.trim_start()
                        .to_ascii_uppercase()
                        .starts_with("CREATE TABLE"),
                    "DDL should start with CREATE TABLE, got: {stmt}"
                );
                assert!(
                    stmt.trim_end().ends_with(");"),
                    "DDL should end with ');', got: ...{:?}",
                    stmt.get(stmt.len().saturating_sub(10)..)
                );
            }
        }
    }
}
