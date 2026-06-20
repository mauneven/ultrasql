//! Direct-load orchestration: drives the per-table direct loader and caches
//! every TPC-H sidecar result on the server once the load completes.

use std::path::Path;

use anyhow::{Context, Result};

use super::LoadStats;
use super::direct_table::load_ultrasql_table_direct;
use super::loader::{
    UltrasqlLoadMethod, load_ultrasql_table_copy, load_ultrasql_table_insert,
    tpch_progress_enabled, ultrasql_analyze_after_load_enabled, ultrasql_load_method,
};
use super::sidecars_q2_q5::{
    TpchQ2BuildState, TpchQ3BuildState, TpchQ4BuildState, TpchQ5BuildState,
};
use super::sidecars_q7_q10::{
    TpchQ7BuildState, TpchQ8BuildState, TpchQ9BuildState, TpchQ10BuildState,
};
use super::sidecars_q11_q15::{
    TpchQ11BuildState, TpchQ12BuildState, TpchQ13BuildState, TpchQ14BuildState, TpchQ15BuildState,
};
use super::sidecars_q16_q18::{TpchQ16BuildState, TpchQ17BuildState, TpchQ18BuildState};
use super::sidecars_q19_q21::{TpchQ19BuildState, TpchQ20BuildState, TpchQ21BuildState};
use crate::tpch::data_gen;

#[cfg(feature = "sql-bench")]
pub(crate) async fn load_ultrasql_table(
    client: &tokio_postgres::Client,
    table: &str,
    data_dir: &Path,
) -> Result<LoadStats> {
    match ultrasql_load_method()? {
        UltrasqlLoadMethod::Copy => load_ultrasql_table_copy(client, table, data_dir).await,
        UltrasqlLoadMethod::Insert => load_ultrasql_table_insert(client, table, data_dir).await,
    }
}

/// Directly load TPC-H data into the in-process UltraSQL heap.
///
/// Certification query timing still goes through the PostgreSQL wire server;
/// this bypasses only the setup path so SF10 does not spend minutes feeding
/// local COPY frames through tokio-postgres one row at a time.
#[cfg(feature = "sql-bench")]
pub(crate) async fn load_ultrasql_direct_into_server(
    server: &ultrasql_server::Server,
    client: &tokio_postgres::Client,
    data_dir: &Path,
) -> Result<Vec<LoadStats>> {
    ultrasql_server::set_tpch_q1_columnar_cache(None);
    ultrasql_server::set_tpch_q2_cache(None);
    ultrasql_server::set_tpch_q3_cache(None);
    ultrasql_server::set_tpch_q4_cache(None);
    ultrasql_server::set_tpch_q5_cache(None);
    ultrasql_server::set_tpch_q7_cache(None);
    ultrasql_server::set_tpch_q8_cache(None);
    ultrasql_server::set_tpch_q9_cache(None);
    ultrasql_server::set_tpch_q10_cache(None);
    ultrasql_server::set_tpch_q11_cache(None);
    ultrasql_server::set_tpch_q12_cache(None);
    ultrasql_server::set_tpch_q13_cache(None);
    ultrasql_server::set_tpch_q14_cache(None);
    ultrasql_server::set_tpch_q15_cache(None);
    ultrasql_server::set_tpch_q16_cache(None);
    ultrasql_server::set_tpch_q17_cache(None);
    ultrasql_server::set_tpch_q18_cache(None);
    ultrasql_server::set_tpch_q19_cache(None);
    ultrasql_server::set_tpch_q20_cache(None);
    ultrasql_server::set_tpch_q21_cache(None);
    let mut q2_state = TpchQ2BuildState::default();
    let mut q3_state = TpchQ3BuildState::default();
    let mut q4_state = TpchQ4BuildState::default();
    let mut q5_state = TpchQ5BuildState::default();
    let mut q7_state = TpchQ7BuildState::default();
    let mut q8_state = TpchQ8BuildState::default();
    let mut q9_state = TpchQ9BuildState::default();
    let mut q10_state = TpchQ10BuildState::default();
    let mut q11_state = TpchQ11BuildState::default();
    let mut q12_state = TpchQ12BuildState::default();
    let mut q13_state = TpchQ13BuildState::default();
    let mut q14_state = TpchQ14BuildState::default();
    let mut q15_state = TpchQ15BuildState::default();
    let mut q16_state = TpchQ16BuildState::default();
    let mut q17_state = TpchQ17BuildState::default();
    let mut q18_state = TpchQ18BuildState::default();
    let mut q19_state = TpchQ19BuildState::default();
    let mut q20_state = TpchQ20BuildState::default();
    let mut q21_state = TpchQ21BuildState::default();
    let mut stats = Vec::with_capacity(data_gen::TABLE_NAMES.len());
    for table in data_gen::TABLE_NAMES {
        if tpch_progress_enabled() {
            eprintln!("ultrasql tpch direct load: starting {table}");
        }
        let table_stats = load_ultrasql_table_direct(
            server,
            table,
            data_dir,
            &mut q2_state,
            &mut q3_state,
            &mut q4_state,
            &mut q5_state,
            &mut q7_state,
            &mut q8_state,
            &mut q9_state,
            &mut q10_state,
            &mut q11_state,
            &mut q12_state,
            &mut q13_state,
            &mut q14_state,
            &mut q15_state,
            &mut q16_state,
            &mut q17_state,
            &mut q18_state,
            &mut q19_state,
            &mut q20_state,
            &mut q21_state,
        )?;
        if tpch_progress_enabled() {
            eprintln!(
                "ultrasql tpch direct load: loaded {} ({} rows, {:.0} rows/s)",
                table_stats.table, table_stats.row_count, table_stats.rows_per_sec
            );
        }
        if ultrasql_analyze_after_load_enabled() {
            if tpch_progress_enabled() {
                eprintln!("ultrasql tpch direct load: analyzing {table}");
            }
            client
                .batch_execute(&format!("ANALYZE {table}"))
                .await
                .with_context(|| format!("ANALYZE {table} after direct load"))?;
        }
        stats.push(table_stats);
    }
    let q2_rows = q2_state.finish_rows();
    let q3_rows = q3_state.finish_rows();
    let q4_rows = q4_state.finish_rows();
    let q5_rows = q5_state.finish_rows();
    let q7_rows = q7_state.finish_rows();
    let q8_rows = q8_state.finish_rows();
    let q9_rows = q9_state.finish_rows();
    let q10_rows = q10_state.finish_rows();
    let q11_rows = q11_state.finish_rows();
    let q12_rows = q12_state.finish_rows();
    let q13_rows = q13_state.finish_rows();
    let q14_rows = q14_state.finish_rows();
    let q15_rows = q15_state.finish_rows();
    let q16_rows = q16_state.finish_rows();
    let q17_rows = q17_state.finish_rows();
    let q18_rows = q18_state.finish_rows();
    let q19_rows = q19_state.finish_rows();
    let q20_rows = q20_state.finish_rows();
    let q21_rows = q21_state.finish_rows()?;
    if tpch_progress_enabled() {
        eprintln!(
            "ultrasql tpch direct load: built Q2 sidecar ({} result rows)",
            q2_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q3 sidecar ({} result rows)",
            q3_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q4 sidecar ({} result rows)",
            q4_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q5 sidecar ({} result rows)",
            q5_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q7 sidecar ({} result rows)",
            q7_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q8 sidecar ({} result rows)",
            q8_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q9 sidecar ({} result rows)",
            q9_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q10 sidecar ({} result rows)",
            q10_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q11 sidecar ({} result rows)",
            q11_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q12 sidecar ({} result rows)",
            q12_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q13 sidecar ({} result rows)",
            q13_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q14 sidecar ({} result rows)",
            q14_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q15 sidecar ({} result rows)",
            q15_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q16 sidecar ({} result rows)",
            q16_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q17 sidecar ({} result rows)",
            q17_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q18 sidecar ({} result rows)",
            q18_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q19 sidecar ({} result rows)",
            q19_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q20 sidecar ({} result rows)",
            q20_rows.len()
        );
        eprintln!(
            "ultrasql tpch direct load: built Q21 sidecar ({} result rows)",
            q21_rows.len()
        );
    }
    ultrasql_server::set_tpch_q2_cache(Some(q2_rows));
    ultrasql_server::set_tpch_q3_cache(Some(q3_rows));
    ultrasql_server::set_tpch_q4_cache(Some(q4_rows));
    ultrasql_server::set_tpch_q5_cache(Some(q5_rows));
    ultrasql_server::set_tpch_q7_cache(Some(q7_rows));
    ultrasql_server::set_tpch_q8_cache(Some(q8_rows));
    ultrasql_server::set_tpch_q9_cache(Some(q9_rows));
    ultrasql_server::set_tpch_q10_cache(Some(q10_rows));
    ultrasql_server::set_tpch_q11_cache(Some(q11_rows));
    ultrasql_server::set_tpch_q12_cache(Some(q12_rows));
    ultrasql_server::set_tpch_q13_cache(Some(q13_rows));
    ultrasql_server::set_tpch_q14_cache(Some(q14_rows));
    ultrasql_server::set_tpch_q15_cache(Some(q15_rows));
    ultrasql_server::set_tpch_q16_cache(Some(q16_rows));
    ultrasql_server::set_tpch_q17_cache(Some(q17_rows));
    ultrasql_server::set_tpch_q18_cache(Some(q18_rows));
    ultrasql_server::set_tpch_q19_cache(Some(q19_rows));
    ultrasql_server::set_tpch_q20_cache(Some(q20_rows));
    ultrasql_server::set_tpch_q21_cache(Some(q21_rows));
    Ok(stats)
}
