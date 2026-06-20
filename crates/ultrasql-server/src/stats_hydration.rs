//! Optimizer-statistics hydration from the persistent catalog.
//!
//! Moved verbatim from the crate root; behavior unchanged.
use super::*;

pub(crate) fn sample_privilege_catalog() -> Arc<auth::InMemoryPrivilegeCatalog> {
    let catalog = Arc::new(auth::InMemoryPrivilegeCatalog::new());
    let objects = [String::from("users")];
    let grantees = [String::from("public")];
    let privileges = [auth::PrivilegeRequest {
        privilege: auth::PrivilegeKind::Select,
        columns: Vec::new(),
    }];
    catalog.grant_many(
        "ultrasql",
        auth::PrivilegeObjectKind::Table,
        &objects,
        &grantees,
        &privileges,
        false,
    );
    catalog
}

pub(crate) fn hydrate_optimizer_stats_from_catalog<L: PageLoader>(
    snapshot: &CatalogSnapshot,
    heap: &HeapAccess<L>,
    txn_manager: &TransactionManager,
) -> InMemoryStatsCatalog {
    let mut catalog = InMemoryStatsCatalog::new();
    for table in snapshot.tables.values() {
        let mut stat_rows = snapshot
            .statistics
            .values()
            .filter(|row| row.starelid == table.oid)
            .collect::<Vec<_>>();
        if stat_rows.is_empty() {
            continue;
        }
        stat_rows.sort_by_key(|row| row.staattnum);

        let columns = stat_rows
            .iter()
            .filter_map(|row| restored_column_stats(row, table))
            .collect::<Vec<_>>();
        if columns.is_empty() {
            continue;
        }

        let row_count = restored_relation_row_count(table, &stat_rows, heap, txn_manager);
        catalog.register(RelationStats {
            table: table_entry_lookup_key(table),
            row_count,
            page_count: u64::from(table.n_blocks),
            columns,
        });
    }
    catalog
}

pub(crate) fn restored_column_stats(row: &StatisticRow, table: &TableEntry) -> Option<ColumnStats> {
    let attnum = u16::try_from(row.staattnum).ok()?;
    let column_index = usize::from(attnum.checked_sub(1)?);
    let field = table.schema.fields().get(column_index)?;
    let avg_width_bytes = field
        .data_type
        .fixed_size()
        .map_or(32, |width| u32::try_from(width).unwrap_or(u32::MAX));
    Some(ColumnStats {
        column_index,
        n_distinct: f64::from(row.stadistinct),
        null_frac: f64::from(row.stanullfrac),
        avg_width_bytes,
        histogram: None,
        mcv: None,
        correlation: 0.0,
    })
}

pub(crate) fn restored_relation_row_count<L: PageLoader>(
    table: &TableEntry,
    rows: &[&StatisticRow],
    heap: &HeapAccess<L>,
    txn_manager: &TransactionManager,
) -> u64 {
    if let Some(row_count) = count_visible_relation_rows(table, heap, txn_manager) {
        return row_count;
    }
    rows.iter()
        .filter_map(|row| positive_f32_ceil_to_u64(row.stadistinct))
        .max()
        .unwrap_or_else(|| u64::from(table.n_blocks).saturating_mul(64))
}

pub(crate) fn count_visible_relation_rows<L: PageLoader>(
    table: &TableEntry,
    heap: &HeapAccess<L>,
    txn_manager: &TransactionManager,
) -> Option<u64> {
    let rel = RelationId(table.oid);
    let block_count = heap.block_count(rel).max(table.n_blocks);
    let scan_txn = txn_manager.begin(IsolationLevel::ReadCommitted);
    let scan_snapshot = scan_txn.snapshot.clone();
    let mut row_count = 0_u64;
    let scan_result = heap.for_each_visible(
        rel,
        block_count,
        &scan_snapshot,
        txn_manager,
        |_tid, _header, _payload| {
            row_count = row_count.saturating_add(1);
            Ok(())
        },
    );
    finish_stats_hydration_row_count(
        &table.name,
        row_count,
        scan_result,
        txn_manager.abort(scan_txn),
    )
}

pub(crate) fn finish_stats_hydration_row_count(
    table_name: &str,
    row_count: u64,
    scan_result: Result<(), HeapError>,
    abort_result: Result<(), TxnError>,
) -> Option<u64> {
    let scan_aborted_cleanly = match abort_result {
        Ok(()) => true,
        Err(e) => {
            warn!(
                table = %table_name,
                error = %e,
                "stats hydration scan transaction abort failed"
            );
            false
        }
    };
    match scan_result {
        Ok(()) if scan_aborted_cleanly => Some(row_count),
        Ok(()) => None,
        Err(e) => {
            warn!(table = %table_name, error = %e, "stats hydration row count scan failed");
            None
        }
    }
}

pub(crate) fn require_wal_backed_catalog_bootstrap(
    result: Result<CatalogStats, CatalogError>,
) -> Result<CatalogStats, ServerError> {
    result.map_err(ServerError::Catalog)
}

pub(crate) fn positive_f32_ceil_to_u64(value: f32) -> Option<u64> {
    if !value.is_finite() || value <= 0.0 {
        return None;
    }
    format!("{:.0}", value.ceil()).parse::<u64>().ok()
}
