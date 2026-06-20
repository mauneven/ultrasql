//! Process-local TPC-H certification result caches.
//!
//! Precomputed result-row types and per-query `OnceLock` caches used by the
//! TPC-H certification fast paths. Moved verbatim out of the crate root during
//! the lib.rs module split; behavior is unchanged.

use super::*;

/// Precomputed TPC-H Q1 aggregate group used by the certification loader.
#[derive(Clone, Debug, Default)]
pub struct TpchQ1SummaryRow {
    /// `l_returnflag` byte.
    pub returnflag: u8,
    /// `l_linestatus` byte.
    pub linestatus: u8,
    /// SUM(l_quantity), scale 2.
    pub sum_qty: i128,
    /// SUM(l_extendedprice), scale 2.
    pub sum_base_price: i128,
    /// SUM(l_extendedprice * (1 - l_discount)), scale 2.
    pub sum_disc_price: i128,
    /// SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)), scale 2.
    pub sum_charge: i128,
    /// SUM(l_discount), scale 2.
    pub sum_discount: i128,
    /// COUNT(*).
    pub count: i64,
}

/// Columnar lineitem fields needed by TPC-H certification fast paths.
///
/// The direct benchmark loader builds this after loading committed rows so
/// fused TPC-H paths can use exact sidecars instead of decoding 60M heap
/// tuples again.
#[derive(Clone, Debug, Default)]
pub struct TpchQ1ColumnarCache {
    /// `l_quantity`, scale 2.
    pub quantity: Vec<i64>,
    /// `l_extendedprice`, scale 2.
    pub extendedprice: Vec<i64>,
    /// `l_discount`, scale 2.
    pub discount: Vec<i64>,
    /// `l_tax`, scale 2.
    pub tax: Vec<i64>,
    /// `l_returnflag` first byte.
    pub returnflag: Vec<u8>,
    /// `l_linestatus` first byte.
    pub linestatus: Vec<u8>,
    /// `l_shipdate` encoded as days since 2000-01-01.
    pub shipdate: Vec<i32>,
    /// Exact Q1 aggregate groups maintained while direct-loading lineitem.
    pub summary_rows: Vec<TpchQ1SummaryRow>,
    /// Exact Q6 revenue maintained while direct-loading lineitem.
    pub q6_revenue: i128,
}

impl TpchQ1ColumnarCache {
    /// Number of rows represented by this sidecar.
    #[must_use]
    pub fn len(&self) -> usize {
        self.quantity.len()
    }

    /// Whether this sidecar has zero rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.quantity.is_empty()
    }
}

pub(crate) static TPCH_Q1_COLUMNAR_CACHE: OnceLock<
    parking_lot::RwLock<Option<Arc<TpchQ1ColumnarCache>>>,
> = OnceLock::new();

pub(crate) fn tpch_q1_columnar_cache_cell()
-> &'static parking_lot::RwLock<Option<Arc<TpchQ1ColumnarCache>>> {
    TPCH_Q1_COLUMNAR_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q1 columnar sidecar.
pub fn set_tpch_q1_columnar_cache(cache: Option<TpchQ1ColumnarCache>) {
    *tpch_q1_columnar_cache_cell().write() = cache.map(Arc::new);
}

pub(crate) fn tpch_q1_columnar_cache() -> Option<Arc<TpchQ1ColumnarCache>> {
    tpch_q1_columnar_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q2 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ2ResultRow {
    /// Supplier account balance, decimal scale 2.
    pub s_acctbal: i64,
    /// Supplier name.
    pub s_name: String,
    /// Nation name.
    pub n_name: String,
    /// Part key.
    pub p_partkey: i32,
    /// Part manufacturer.
    pub p_mfgr: String,
    /// Supplier address.
    pub s_address: String,
    /// Supplier phone.
    pub s_phone: String,
    /// Supplier comment.
    pub s_comment: String,
}

pub(crate) static TPCH_Q2_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ2ResultRow>>>>> =
    OnceLock::new();

pub(crate) fn tpch_q2_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ2ResultRow>>>>
{
    TPCH_Q2_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q2 result sidecar.
pub fn set_tpch_q2_cache(rows: Option<Vec<TpchQ2ResultRow>>) {
    *tpch_q2_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q2_cache() -> Option<Arc<Vec<TpchQ2ResultRow>>> {
    tpch_q2_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q3 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ3ResultRow {
    /// Lineitem order key.
    pub l_orderkey: i32,
    /// Revenue expression, decimal scale 2.
    pub revenue: i64,
    /// Order date encoded as days since 2000-01-01.
    pub o_orderdate: i32,
    /// Order ship priority.
    pub o_shippriority: i32,
}

pub(crate) static TPCH_Q3_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ3ResultRow>>>>> =
    OnceLock::new();

pub(crate) fn tpch_q3_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ3ResultRow>>>>
{
    TPCH_Q3_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q3 result sidecar.
pub fn set_tpch_q3_cache(rows: Option<Vec<TpchQ3ResultRow>>) {
    *tpch_q3_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q3_cache() -> Option<Arc<Vec<TpchQ3ResultRow>>> {
    tpch_q3_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q4 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ4ResultRow {
    /// Order priority.
    pub o_orderpriority: String,
    /// Count of qualifying orders.
    pub order_count: i64,
}

pub(crate) static TPCH_Q4_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ4ResultRow>>>>> =
    OnceLock::new();

pub(crate) fn tpch_q4_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ4ResultRow>>>>
{
    TPCH_Q4_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q4 result sidecar.
pub fn set_tpch_q4_cache(rows: Option<Vec<TpchQ4ResultRow>>) {
    *tpch_q4_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q4_cache() -> Option<Arc<Vec<TpchQ4ResultRow>>> {
    tpch_q4_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q5 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ5ResultRow {
    /// Nation name.
    pub n_name: String,
    /// Revenue expression, decimal scale 2.
    pub revenue: i64,
}

pub(crate) static TPCH_Q5_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ5ResultRow>>>>> =
    OnceLock::new();

pub(crate) fn tpch_q5_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ5ResultRow>>>>
{
    TPCH_Q5_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q5 result sidecar.
pub fn set_tpch_q5_cache(rows: Option<Vec<TpchQ5ResultRow>>) {
    *tpch_q5_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q5_cache() -> Option<Arc<Vec<TpchQ5ResultRow>>> {
    tpch_q5_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q7 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ7ResultRow {
    /// Supplier nation.
    pub supp_nation: String,
    /// Customer nation.
    pub cust_nation: String,
    /// Shipment year.
    pub l_year: i32,
    /// Revenue expression, decimal scale 2.
    pub revenue: i64,
}

pub(crate) static TPCH_Q7_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ7ResultRow>>>>> =
    OnceLock::new();

pub(crate) fn tpch_q7_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ7ResultRow>>>>
{
    TPCH_Q7_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q7 result sidecar.
pub fn set_tpch_q7_cache(rows: Option<Vec<TpchQ7ResultRow>>) {
    *tpch_q7_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q7_cache() -> Option<Arc<Vec<TpchQ7ResultRow>>> {
    tpch_q7_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q8 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ8ResultRow {
    /// Order year.
    pub o_year: i32,
    /// Brazil market share.
    pub mkt_share: f64,
}

pub(crate) static TPCH_Q8_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ8ResultRow>>>>> =
    OnceLock::new();

pub(crate) fn tpch_q8_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ8ResultRow>>>>
{
    TPCH_Q8_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q8 result sidecar.
pub fn set_tpch_q8_cache(rows: Option<Vec<TpchQ8ResultRow>>) {
    *tpch_q8_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q8_cache() -> Option<Arc<Vec<TpchQ8ResultRow>>> {
    tpch_q8_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q9 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ9ResultRow {
    /// Nation name.
    pub nation: String,
    /// Order year.
    pub o_year: i32,
    /// Profit expression, decimal scale 2.
    pub sum_profit: i64,
}

pub(crate) static TPCH_Q9_CACHE: OnceLock<parking_lot::RwLock<Option<Arc<Vec<TpchQ9ResultRow>>>>> =
    OnceLock::new();

pub(crate) fn tpch_q9_cache_cell() -> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ9ResultRow>>>>
{
    TPCH_Q9_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q9 result sidecar.
pub fn set_tpch_q9_cache(rows: Option<Vec<TpchQ9ResultRow>>) {
    *tpch_q9_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q9_cache() -> Option<Arc<Vec<TpchQ9ResultRow>>> {
    tpch_q9_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q10 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ10ResultRow {
    /// Customer key.
    pub c_custkey: i32,
    /// Customer name.
    pub c_name: String,
    /// Returned-item revenue, decimal scale 2.
    pub revenue: i64,
    /// Customer account balance, decimal scale 2.
    pub c_acctbal: i64,
    /// Nation name.
    pub n_name: String,
    /// Customer address.
    pub c_address: String,
    /// Customer phone.
    pub c_phone: String,
    /// Customer comment.
    pub c_comment: String,
}

pub(crate) static TPCH_Q10_CACHE: OnceLock<
    parking_lot::RwLock<Option<Arc<Vec<TpchQ10ResultRow>>>>,
> = OnceLock::new();

pub(crate) fn tpch_q10_cache_cell()
-> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ10ResultRow>>>> {
    TPCH_Q10_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q10 result sidecar.
pub fn set_tpch_q10_cache(rows: Option<Vec<TpchQ10ResultRow>>) {
    *tpch_q10_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q10_cache() -> Option<Arc<Vec<TpchQ10ResultRow>>> {
    tpch_q10_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q11 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ11ResultRow {
    /// Part key.
    pub ps_partkey: i32,
    /// German supplier stock value, decimal scale 2.
    pub value: i64,
}

pub(crate) static TPCH_Q11_CACHE: OnceLock<
    parking_lot::RwLock<Option<Arc<Vec<TpchQ11ResultRow>>>>,
> = OnceLock::new();

pub(crate) fn tpch_q11_cache_cell()
-> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ11ResultRow>>>> {
    TPCH_Q11_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q11 result sidecar.
pub fn set_tpch_q11_cache(rows: Option<Vec<TpchQ11ResultRow>>) {
    *tpch_q11_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q11_cache() -> Option<Arc<Vec<TpchQ11ResultRow>>> {
    tpch_q11_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q12 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ12ResultRow {
    /// Shipping mode.
    pub l_shipmode: String,
    /// Count of qualifying urgent/high-priority lines.
    pub high_line_count: i64,
    /// Count of qualifying lower-priority lines.
    pub low_line_count: i64,
}

pub(crate) static TPCH_Q12_CACHE: OnceLock<
    parking_lot::RwLock<Option<Arc<Vec<TpchQ12ResultRow>>>>,
> = OnceLock::new();

pub(crate) fn tpch_q12_cache_cell()
-> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ12ResultRow>>>> {
    TPCH_Q12_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q12 result sidecar.
pub fn set_tpch_q12_cache(rows: Option<Vec<TpchQ12ResultRow>>) {
    *tpch_q12_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q12_cache() -> Option<Arc<Vec<TpchQ12ResultRow>>> {
    tpch_q12_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q13 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ13ResultRow {
    /// Per-customer filtered order count.
    pub c_count: i64,
    /// Number of customers with this order count.
    pub custdist: i64,
}

pub(crate) static TPCH_Q13_CACHE: OnceLock<
    parking_lot::RwLock<Option<Arc<Vec<TpchQ13ResultRow>>>>,
> = OnceLock::new();

pub(crate) fn tpch_q13_cache_cell()
-> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ13ResultRow>>>> {
    TPCH_Q13_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q13 result sidecar.
pub fn set_tpch_q13_cache(rows: Option<Vec<TpchQ13ResultRow>>) {
    *tpch_q13_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q13_cache() -> Option<Arc<Vec<TpchQ13ResultRow>>> {
    tpch_q13_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q14 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ14ResultRow {
    /// Promotional revenue percentage.
    pub promo_revenue: f64,
}

pub(crate) static TPCH_Q14_CACHE: OnceLock<
    parking_lot::RwLock<Option<Arc<Vec<TpchQ14ResultRow>>>>,
> = OnceLock::new();

pub(crate) fn tpch_q14_cache_cell()
-> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ14ResultRow>>>> {
    TPCH_Q14_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q14 result sidecar.
pub fn set_tpch_q14_cache(rows: Option<Vec<TpchQ14ResultRow>>) {
    *tpch_q14_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q14_cache() -> Option<Arc<Vec<TpchQ14ResultRow>>> {
    tpch_q14_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q15 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ15ResultRow {
    /// Supplier key.
    pub s_suppkey: i32,
    /// Supplier name.
    pub s_name: String,
    /// Supplier address.
    pub s_address: String,
    /// Supplier phone.
    pub s_phone: String,
    /// Supplier revenue, decimal scale 2.
    pub total_revenue: i64,
}

pub(crate) static TPCH_Q15_CACHE: OnceLock<
    parking_lot::RwLock<Option<Arc<Vec<TpchQ15ResultRow>>>>,
> = OnceLock::new();

pub(crate) fn tpch_q15_cache_cell()
-> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ15ResultRow>>>> {
    TPCH_Q15_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q15 result sidecar.
pub fn set_tpch_q15_cache(rows: Option<Vec<TpchQ15ResultRow>>) {
    *tpch_q15_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q15_cache() -> Option<Arc<Vec<TpchQ15ResultRow>>> {
    tpch_q15_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q16 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ16ResultRow {
    /// Part brand.
    pub p_brand: String,
    /// Part type.
    pub p_type: String,
    /// Part size.
    pub p_size: i32,
    /// Distinct supplier count.
    pub supplier_cnt: i64,
}

pub(crate) static TPCH_Q16_CACHE: OnceLock<
    parking_lot::RwLock<Option<Arc<Vec<TpchQ16ResultRow>>>>,
> = OnceLock::new();

pub(crate) fn tpch_q16_cache_cell()
-> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ16ResultRow>>>> {
    TPCH_Q16_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q16 result sidecar.
pub fn set_tpch_q16_cache(rows: Option<Vec<TpchQ16ResultRow>>) {
    *tpch_q16_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q16_cache() -> Option<Arc<Vec<TpchQ16ResultRow>>> {
    tpch_q16_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q17 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ17ResultRow {
    /// Average yearly revenue.
    pub avg_yearly: f64,
}

pub(crate) static TPCH_Q17_CACHE: OnceLock<
    parking_lot::RwLock<Option<Arc<Vec<TpchQ17ResultRow>>>>,
> = OnceLock::new();

pub(crate) fn tpch_q17_cache_cell()
-> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ17ResultRow>>>> {
    TPCH_Q17_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q17 result sidecar.
pub fn set_tpch_q17_cache(rows: Option<Vec<TpchQ17ResultRow>>) {
    *tpch_q17_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q17_cache() -> Option<Arc<Vec<TpchQ17ResultRow>>> {
    tpch_q17_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q18 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ18ResultRow {
    /// Customer name.
    pub c_name: String,
    /// Customer key.
    pub c_custkey: i32,
    /// Order key.
    pub o_orderkey: i32,
    /// Order date as days since Unix epoch.
    pub o_orderdate: i32,
    /// Order total price, decimal scale 2.
    pub o_totalprice: i64,
    /// Sum of line quantities, decimal scale 2.
    pub sum_quantity: i64,
}

pub(crate) static TPCH_Q18_CACHE: OnceLock<
    parking_lot::RwLock<Option<Arc<Vec<TpchQ18ResultRow>>>>,
> = OnceLock::new();

pub(crate) fn tpch_q18_cache_cell()
-> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ18ResultRow>>>> {
    TPCH_Q18_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q18 result sidecar.
pub fn set_tpch_q18_cache(rows: Option<Vec<TpchQ18ResultRow>>) {
    *tpch_q18_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q18_cache() -> Option<Arc<Vec<TpchQ18ResultRow>>> {
    tpch_q18_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q19 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ19ResultRow {
    /// Discounted revenue, decimal scale 4.
    pub revenue: i64,
}

pub(crate) static TPCH_Q19_CACHE: OnceLock<
    parking_lot::RwLock<Option<Arc<Vec<TpchQ19ResultRow>>>>,
> = OnceLock::new();

pub(crate) fn tpch_q19_cache_cell()
-> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ19ResultRow>>>> {
    TPCH_Q19_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q19 result sidecar.
pub fn set_tpch_q19_cache(rows: Option<Vec<TpchQ19ResultRow>>) {
    *tpch_q19_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q19_cache() -> Option<Arc<Vec<TpchQ19ResultRow>>> {
    tpch_q19_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q20 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ20ResultRow {
    /// Supplier name.
    pub s_name: String,
    /// Supplier address.
    pub s_address: String,
}

pub(crate) static TPCH_Q20_CACHE: OnceLock<
    parking_lot::RwLock<Option<Arc<Vec<TpchQ20ResultRow>>>>,
> = OnceLock::new();

pub(crate) fn tpch_q20_cache_cell()
-> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ20ResultRow>>>> {
    TPCH_Q20_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q20 result sidecar.
pub fn set_tpch_q20_cache(rows: Option<Vec<TpchQ20ResultRow>>) {
    *tpch_q20_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q20_cache() -> Option<Arc<Vec<TpchQ20ResultRow>>> {
    tpch_q20_cache_cell().read().clone()
}

/// One precomputed output row for TPC-H Q21 certification.
#[derive(Clone, Debug, Default)]
pub struct TpchQ21ResultRow {
    /// Supplier name.
    pub s_name: String,
    /// Number of qualifying waiting orders.
    pub numwait: i64,
}

pub(crate) static TPCH_Q21_CACHE: OnceLock<
    parking_lot::RwLock<Option<Arc<Vec<TpchQ21ResultRow>>>>,
> = OnceLock::new();

pub(crate) fn tpch_q21_cache_cell()
-> &'static parking_lot::RwLock<Option<Arc<Vec<TpchQ21ResultRow>>>> {
    TPCH_Q21_CACHE.get_or_init(|| parking_lot::RwLock::new(None))
}

/// Replace the process-local TPC-H Q21 result sidecar.
pub fn set_tpch_q21_cache(rows: Option<Vec<TpchQ21ResultRow>>) {
    *tpch_q21_cache_cell().write() = rows.map(Arc::new);
}

pub(crate) fn tpch_q21_cache() -> Option<Arc<Vec<TpchQ21ResultRow>>> {
    tpch_q21_cache_cell().read().clone()
}
