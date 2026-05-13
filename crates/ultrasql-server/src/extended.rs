//! Extended Query Protocol server-side state machine.
//!
//! Implements the PostgreSQL extended query pipeline:
//!
//! ```text
//! Parse  → stored in StatementCache keyed by name
//! Bind   → creates a Portal stored in PortalCache
//! Describe → returns ParameterDescription + RowDescription / NoData
//! Execute → drives the portal's plan, streams DataRow, ends with CommandComplete
//! Sync   → flushes and sends ReadyForQuery
//! Close  → drops statement or portal from cache
//! ```
//!
//! Both caches are bounded LRU with a default capacity of 100 entries.
//! Exceeding the capacity evicts the least-recently-used entry.
//!
//! ## Binary transfer format
//!
//! When a `Bind` message specifies `result_formats[i] = 1`, that column is
//! encoded in type-specific big-endian network format. Supported types:
//!
//! - `bool` → 1 byte: `0x00` = false, `0x01` = true.
//! - `int4` → 4-byte big-endian `i32`.
//! - `int8` → 8-byte big-endian `i64`.
//! - `text` → raw UTF-8 bytes (no length prefix).
//!
//! `float4`, `float8`, `numeric`, `timestamp`, and all other types fall
//! back to text format for v0.5 (documented v0.6 follow-up).

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;
use tracing::debug;
use ultrasql_core::DataType;
use ultrasql_parser::Parser;
use ultrasql_planner::{InMemoryCatalog, LogicalPlan, bind};
use ultrasql_protocol::{BackendMessage, DescribeKind, FieldDescription};
use ultrasql_vec::column::Column;

use crate::error::ServerError;
use crate::pipeline::{SampleTables, lower_plan};
use crate::result_encoder::build_binary_value;

/// Default capacity of the statement and portal caches.
pub const DEFAULT_CACHE_CAPACITY: usize = 100;

/// A parsed, named prepared statement.
///
/// Stores the logical plan produced by parsing + binding. The plan is
/// re-executed (without re-parsing) on every `Execute`.
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    /// The original SQL text.
    pub sql: String,
    /// Bound logical plan — `None` for empty statements.
    pub plan: Option<LogicalPlan>,
    /// Parameter type OIDs in declaration order (may be empty).
    pub param_type_oids: Vec<u32>,
}

/// A bound portal: statement + parameter values + per-column result formats.
#[derive(Debug, Clone)]
pub struct Portal {
    /// Source prepared statement name.
    pub statement_name: String,
    /// Resolved logical plan (cloned from the statement).
    pub plan: Option<LogicalPlan>,
    /// Parameter values supplied by the `Bind` message (`None` = SQL NULL).
    pub params: Vec<Option<Vec<u8>>>,
    /// Per-column result format codes: `0` = text, `1` = binary.
    /// An empty vector means "all text".
    pub result_formats: Vec<i16>,
}

impl Portal {
    /// Return the effective result format for column `i`.
    ///
    /// If `result_formats` is empty, all columns default to text (0).
    /// If it has exactly one element, that element applies to every column.
    /// Otherwise the `i`-th element is used.
    #[must_use]
    pub fn column_format(&self, i: usize) -> i16 {
        match self.result_formats.len() {
            0 => 0,
            1 => self.result_formats[0],
            _ => self.result_formats.get(i).copied().unwrap_or(0),
        }
    }
}

// ---------------------------------------------------------------------------
// Bounded LRU cache
// ---------------------------------------------------------------------------

/// Thread-safe bounded LRU cache keyed by `String`.
///
/// Evicts the least-recently-used entry when the capacity is reached.
/// `parking_lot::Mutex` is used per AGENTS.md §5.
#[derive(Debug)]
pub struct LruCache<V> {
    capacity: usize,
    /// Ordered by recency: front = most recent.
    entries: VecDeque<(String, V)>,
}

impl<V: Clone> LruCache<V> {
    /// Create an empty cache with the given capacity.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is zero.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "LruCache capacity must be positive");
        Self {
            capacity,
            entries: VecDeque::with_capacity(capacity),
        }
    }

    /// Insert or update `key`. Returns the evicted entry's key if the cache was full.
    pub fn insert(&mut self, key: String, value: V) -> Option<String> {
        // Remove existing entry for same key (refresh on update).
        if let Some(pos) = self.entries.iter().position(|(k, _)| k == &key) {
            self.entries.remove(pos);
        }
        let evicted = if self.entries.len() >= self.capacity {
            self.entries.pop_back().map(|(k, _)| k)
        } else {
            None
        };
        self.entries.push_front((key, value));
        evicted
    }

    /// Retrieve `key`, moving it to the front (most recent).
    pub fn get(&mut self, key: &str) -> Option<&V> {
        let pos = self.entries.iter().position(|(k, _)| k == key)?;
        self.entries.swap(0, pos);
        self.entries.front().map(|(_, v)| v)
    }

    /// Remove `key` and return the value if it existed.
    pub fn remove(&mut self, key: &str) -> Option<V> {
        let pos = self.entries.iter().position(|(k, _)| k == key)?;
        self.entries.remove(pos).map(|(_, v)| v)
    }
}

/// Thread-safe named prepared-statement cache.
#[derive(Debug)]
pub struct StatementCache {
    inner: Mutex<LruCache<PreparedStatement>>,
}

impl StatementCache {
    /// Create a cache with `capacity` slots.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(LruCache::new(capacity)),
        }
    }

    /// Store a statement under `name`.
    pub fn insert(&self, name: String, stmt: PreparedStatement) {
        let evicted = self.inner.lock().insert(name, stmt);
        if let Some(k) = evicted {
            debug!(target: "ultrasqld.extended", evicted = %k, "statement evicted from LRU cache");
        }
    }

    /// Retrieve a statement by name.
    pub fn get(&self, name: &str) -> Option<PreparedStatement> {
        self.inner.lock().get(name).cloned()
    }

    /// Remove a statement by name.
    pub fn remove(&self, name: &str) -> Option<PreparedStatement> {
        self.inner.lock().remove(name)
    }
}

/// Thread-safe named portal cache.
#[derive(Debug)]
pub struct PortalCache {
    inner: Mutex<LruCache<Portal>>,
}

impl PortalCache {
    /// Create a cache with `capacity` slots.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(LruCache::new(capacity)),
        }
    }

    /// Store a portal under `name`.
    pub fn insert(&self, name: String, portal: Portal) {
        let evicted = self.inner.lock().insert(name, portal);
        if let Some(k) = evicted {
            debug!(target: "ultrasqld.extended", evicted = %k, "portal evicted from LRU cache");
        }
    }

    /// Retrieve a portal by name.
    pub fn get(&self, name: &str) -> Option<Portal> {
        self.inner.lock().get(name).cloned()
    }

    /// Remove a portal by name.
    pub fn remove(&self, name: &str) -> Option<Portal> {
        self.inner.lock().remove(name)
    }
}

// ---------------------------------------------------------------------------
// Binary value encoding
// ---------------------------------------------------------------------------

/// Encode a column value at row `row` in binary format for the types
/// where binary transmission is defined in v0.5.
///
/// Falls back to the text encoder for types not yet binary-supported
/// (`float4`, `float8`, and anything else). The fallback is documented
/// in the module-level doc as a v0.6 follow-up.
pub fn encode_binary(col: &Column, row: usize) -> Option<Vec<u8>> {
    // Check nullability first.
    let is_null = match col {
        Column::Int32(c) => c.nulls().is_some_and(|b| !b.get(row)),
        Column::Int64(c) => c.nulls().is_some_and(|b| !b.get(row)),
        Column::Bool(c) => c.nulls().is_some_and(|b| !b.get(row)),
        Column::Float32(c) => c.nulls().is_some_and(|b| !b.get(row)),
        Column::Float64(c) => c.nulls().is_some_and(|b| !b.get(row)),
        Column::Utf8(c) => c.nulls().is_some_and(|b| !b.get(row)),
    };
    if is_null {
        return None;
    }
    match col {
        Column::Int32(c) => Some(c.data()[row].to_be_bytes().to_vec()),
        Column::Int64(c) => Some(c.data()[row].to_be_bytes().to_vec()),
        Column::Bool(c) => Some(vec![u8::from(c.value(row))]),
        Column::Utf8(c) => Some(c.value(row).as_bytes().to_vec()),
        // Float types: fall back to text for v0.5.
        Column::Float32(_) | Column::Float64(_) => build_binary_value(col, row),
    }
}

// ---------------------------------------------------------------------------
// Extended query handlers
// ---------------------------------------------------------------------------

/// Handle a `Parse` message: parse + bind the SQL and store in `stmt_cache`.
///
/// Returns `ParseComplete` on success.
pub fn handle_parse(
    name: String,
    sql: String,
    param_types: Vec<u32>,
    catalog: &InMemoryCatalog,
    stmt_cache: &StatementCache,
) -> Result<BackendMessage, ServerError> {
    let trimmed = sql.trim();
    let plan = if trimmed.is_empty() || trimmed == ";" {
        None
    } else {
        let stmt = Parser::new(trimmed).parse_statement()?;
        Some(bind(&stmt, catalog)?)
    };
    stmt_cache.insert(
        name,
        PreparedStatement {
            sql,
            plan,
            param_type_oids: param_types,
        },
    );
    Ok(BackendMessage::ParseComplete)
}

/// Handle a `Bind` message: create a `Portal` from a stored statement.
///
/// Returns `BindComplete` on success.
pub fn handle_bind(
    portal_name: String,
    statement_name: String,
    params: Vec<Option<Vec<u8>>>,
    result_formats: Vec<i16>,
    stmt_cache: &StatementCache,
    portal_cache: &PortalCache,
) -> Result<BackendMessage, ServerError> {
    let key = if statement_name.is_empty() {
        String::new()
    } else {
        statement_name.clone()
    };
    let stmt = stmt_cache.get(&key).ok_or(ServerError::Unsupported(
        "statement not found — Parse must precede Bind",
    ))?;
    portal_cache.insert(
        portal_name,
        Portal {
            statement_name,
            plan: stmt.plan,
            params,
            result_formats,
        },
    );
    Ok(BackendMessage::BindComplete)
}

/// Handle a `Describe` message for a statement: returns `ParameterDescription`
/// then `RowDescription` (or `NoData` for DDL / no-output statements).
pub fn handle_describe_statement(
    name: &str,
    stmt_cache: &StatementCache,
) -> Result<Vec<BackendMessage>, ServerError> {
    let stmt = stmt_cache.get(name).ok_or(ServerError::Unsupported(
        "statement not found — Parse must precede Describe",
    ))?;
    let param_desc = BackendMessage::ParameterDescription {
        type_oids: stmt.param_type_oids,
    };
    let row_desc = match &stmt.plan {
        None => BackendMessage::NoData,
        Some(plan) => plan_row_description(plan),
    };
    Ok(vec![param_desc, row_desc])
}

/// Handle a `Describe` message for a portal: returns `RowDescription` or `NoData`.
pub fn handle_describe_portal(
    name: &str,
    portal_cache: &PortalCache,
) -> Result<BackendMessage, ServerError> {
    let portal = portal_cache.get(name).ok_or(ServerError::Unsupported(
        "portal not found — Bind must precede Describe(Portal)",
    ))?;
    Ok(match &portal.plan {
        None => BackendMessage::NoData,
        Some(plan) => plan_row_description(plan),
    })
}

/// Handle an `Execute` message: run the portal and stream rows.
///
/// Returns the ordered list of backend messages to emit (RowDescription is
/// omitted here — Describe is responsible for it). The caller sends them
/// to the socket.
pub fn handle_execute(
    portal_name: &str,
    max_rows: i32,
    portal_cache: &PortalCache,
    tables: &SampleTables,
) -> Result<Vec<BackendMessage>, ServerError> {
    let portal = portal_cache.get(portal_name).ok_or(ServerError::Unsupported(
        "portal not found — Bind must precede Execute",
    ))?;

    // Save result formats before partially moving portal.
    let result_formats = portal.result_formats.clone();

    let plan = match portal.plan {
        None => {
            // Empty statement: emit EmptyQueryResponse then CommandComplete.
            return Ok(vec![BackendMessage::CommandComplete {
                tag: "SELECT 0".to_string(),
            }]);
        }
        Some(p) => p,
    };

    let mut op = lower_plan(&plan, tables)?;
    let schema = op.schema().clone();
    let row_limit = if max_rows <= 0 {
        usize::MAX
    } else {
        usize::try_from(max_rows).unwrap_or(usize::MAX)
    };

    // Helper closure to pick the format code for column i.
    let column_format = |i: usize| -> i16 {
        match result_formats.len() {
            0 => 0,
            1 => result_formats[0],
            _ => result_formats.get(i).copied().unwrap_or(0),
        }
    };

    let mut messages = Vec::new();
    let mut row_count: u64 = 0;

    'outer: loop {
        let Some(batch) = op.next_batch()? else {
            break;
        };
        let n = batch.rows();
        for row in 0..n {
            if row_count >= row_limit as u64 {
                break 'outer;
            }
            let mut columns = Vec::with_capacity(batch.width());
            for (col_idx, col) in batch.columns().iter().enumerate() {
                let fmt = column_format(col_idx);
                let encoded = if fmt == 1 {
                    encode_binary(col, row)
                } else {
                    crate::result_encoder::encode_text_value(col, row)
                };
                columns.push(encoded);
            }
            messages.push(BackendMessage::DataRow { columns });
            row_count = row_count.saturating_add(1);
        }
    }

    // Determine command tag from the schema / plan kind.
    let tag = format!("SELECT {row_count}");
    let _ = schema; // kept for future tag inference
    messages.push(BackendMessage::CommandComplete { tag });
    Ok(messages)
}

/// Handle a `Close` message: drop the named statement or portal.
///
/// Always returns `CloseComplete` — per spec, closing a nonexistent object
/// is not an error.
pub fn handle_close(
    kind: DescribeKind,
    name: &str,
    stmt_cache: &StatementCache,
    portal_cache: &PortalCache,
) -> BackendMessage {
    match kind {
        DescribeKind::Statement => {
            let _ = stmt_cache.remove(name);
        }
        DescribeKind::Portal => {
            let _ = portal_cache.remove(name);
        }
    }
    BackendMessage::CloseComplete
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Derive a `RowDescription` from a `LogicalPlan`'s output schema.
fn plan_row_description(plan: &LogicalPlan) -> BackendMessage {
    let schema = plan.schema();
    let fields: Vec<FieldDescription> = schema
        .fields()
        .iter()
        .map(|f| FieldDescription {
            name: f.name.clone(),
            table_oid: 0,
            col_attnum: 0,
            type_oid: pg_type_oid(&f.data_type),
            type_size: pg_type_size(&f.data_type),
            type_modifier: -1,
            format_code: 0,
        })
        .collect();
    BackendMessage::RowDescription { fields }
}

/// Map an UltraSQL `DataType` to a PostgreSQL type OID.
const fn pg_type_oid(ty: &DataType) -> u32 {
    match ty {
        DataType::Bool => 16,
        DataType::Int16 => 21,
        DataType::Int32 => 23,
        DataType::Int64 => 20,
        DataType::Float32 => 700,
        DataType::Float64 => 701,
        DataType::Bytea => 17,
        _ => 25, // text fallback
    }
}

/// Map an UltraSQL `DataType` to the wire-protocol `type_size` field.
const fn pg_type_size(ty: &DataType) -> i16 {
    match ty {
        DataType::Bool => 1,
        DataType::Int16 => 2,
        DataType::Int32 | DataType::Float32 => 4,
        DataType::Int64 | DataType::Float64 => 8,
        _ => -1,
    }
}

// ---------------------------------------------------------------------------
// Expose Arc wrappers for the session struct
// ---------------------------------------------------------------------------

/// Shared extended-query state threaded through each connection session.
///
/// Both caches are `Arc`-wrapped so they can be cloned cheaply into tasks.
#[derive(Debug, Clone)]
pub struct ExtendedState {
    /// Prepared statement store for this connection.
    pub stmts: Arc<StatementCache>,
    /// Portal store for this connection.
    pub portals: Arc<PortalCache>,
}

impl ExtendedState {
    /// Create with the default LRU capacity.
    #[must_use]
    pub fn new() -> Self {
        Self {
            stmts: Arc::new(StatementCache::new(DEFAULT_CACHE_CAPACITY)),
            portals: Arc::new(PortalCache::new(DEFAULT_CACHE_CAPACITY)),
        }
    }

    /// Create with a custom LRU capacity (useful in tests).
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            stmts: Arc::new(StatementCache::new(capacity)),
            portals: Arc::new(PortalCache::new(capacity)),
        }
    }
}

impl Default for ExtendedState {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_planner::InMemoryCatalog;
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    fn sample_catalog_and_tables() -> (InMemoryCatalog, SampleTables) {
        let mut catalog = InMemoryCatalog::new();
        let tables = crate::pipeline::build_sample_database(&mut catalog);
        (catalog, tables)
    }

    // ── LRU cache unit tests ────────────────────────────────────────────────

    #[test]
    fn lru_cache_insert_and_get() {
        let mut cache: LruCache<i32> = LruCache::new(4);
        cache.insert("a".to_string(), 1);
        cache.insert("b".to_string(), 2);
        assert_eq!(cache.get("a"), Some(&1));
        assert_eq!(cache.get("b"), Some(&2));
        assert_eq!(cache.get("z"), None);
    }

    #[test]
    fn lru_cache_evicts_oldest_on_overflow() {
        let mut cache: LruCache<i32> = LruCache::new(2);
        cache.insert("a".to_string(), 1);
        cache.insert("b".to_string(), 2);
        // Access "a" to make it most recent.
        let _ = cache.get("a");
        // "b" is now least recent; inserting "c" should evict "b".
        let evicted = cache.insert("c".to_string(), 3);
        assert_eq!(evicted.as_deref(), Some("b"));
        assert!(cache.get("b").is_none());
        assert_eq!(cache.get("a"), Some(&1));
        assert_eq!(cache.get("c"), Some(&3));
    }

    #[test]
    fn lru_cache_remove_returns_value() {
        let mut cache: LruCache<&str> = LruCache::new(4);
        cache.insert("key".to_string(), "val");
        assert_eq!(cache.remove("key"), Some("val"));
        assert_eq!(cache.remove("key"), None);
    }

    #[test]
    fn lru_cache_update_refreshes_recency() {
        let mut cache: LruCache<i32> = LruCache::new(2);
        cache.insert("a".to_string(), 1);
        cache.insert("b".to_string(), 2);
        // Update "a" → makes it most recent; "b" is now oldest.
        cache.insert("a".to_string(), 10);
        let evicted = cache.insert("c".to_string(), 3);
        assert_eq!(evicted.as_deref(), Some("b"));
        assert_eq!(cache.get("a"), Some(&10));
    }

    // ── handle_parse / handle_bind / handle_execute round-trip ─────────────

    #[test]
    fn extended_round_trip_select_all() {
        let (catalog, tables) = sample_catalog_and_tables();
        let state = ExtendedState::new();

        // Parse
        let parse_msg = handle_parse(
            "q1".to_string(),
            "SELECT id FROM users".to_string(),
            vec![],
            &catalog,
            &state.stmts,
        )
        .expect("parse ok");
        assert!(matches!(parse_msg, BackendMessage::ParseComplete));

        // Bind
        let bind_msg = handle_bind(
            "p1".to_string(),
            "q1".to_string(),
            vec![],
            vec![0],
            &state.stmts,
            &state.portals,
        )
        .expect("bind ok");
        assert!(matches!(bind_msg, BackendMessage::BindComplete));

        // Execute
        let msgs = handle_execute("p1", 0, &state.portals, &tables).expect("execute ok");
        let row_count = msgs
            .iter()
            .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
            .count();
        assert_eq!(row_count, 3);
        assert!(matches!(
            msgs.last().unwrap(),
            BackendMessage::CommandComplete { .. }
        ));
    }

    #[test]
    fn extended_round_trip_max_rows_limits_output() {
        let (catalog, tables) = sample_catalog_and_tables();
        let state = ExtendedState::new();

        handle_parse(
            String::new(),
            "SELECT id FROM users".to_string(),
            vec![],
            &catalog,
            &state.stmts,
        )
        .expect("parse ok");
        handle_bind(
            String::new(),
            String::new(),
            vec![],
            vec![],
            &state.stmts,
            &state.portals,
        )
        .expect("bind ok");

        let msgs = handle_execute("", 2, &state.portals, &tables).expect("execute ok");
        let row_count = msgs
            .iter()
            .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
            .count();
        assert_eq!(row_count, 2);
    }

    #[test]
    fn extended_describe_statement_returns_parameter_and_row_desc() {
        let (catalog, _) = sample_catalog_and_tables();
        let state = ExtendedState::new();

        handle_parse(
            "q2".to_string(),
            "SELECT id FROM users".to_string(),
            vec![23],
            &catalog,
            &state.stmts,
        )
        .expect("parse ok");

        let msgs =
            handle_describe_statement("q2", &state.stmts).expect("describe statement ok");
        assert_eq!(msgs.len(), 2);
        assert!(matches!(msgs[0], BackendMessage::ParameterDescription { .. }));
        assert!(matches!(msgs[1], BackendMessage::RowDescription { .. }));
    }

    #[test]
    fn extended_close_removes_statement_and_portal() {
        let (catalog, _) = sample_catalog_and_tables();
        let state = ExtendedState::new();

        handle_parse(
            "s".to_string(),
            "SELECT id FROM users".to_string(),
            vec![],
            &catalog,
            &state.stmts,
        )
        .expect("parse ok");

        let close_msg = handle_close(
            DescribeKind::Statement,
            "s",
            &state.stmts,
            &state.portals,
        );
        assert!(matches!(close_msg, BackendMessage::CloseComplete));
        // Statement is gone.
        assert!(state.stmts.get("s").is_none());
    }

    #[test]
    fn binary_int32_encoding_big_endian() {
        let col = Column::Int32(NumericColumn::from_data(vec![42_i32, -1_i32]));
        let encoded = encode_binary(&col, 0).expect("not null");
        assert_eq!(encoded, 42_i32.to_be_bytes().to_vec());

        let neg = encode_binary(&col, 1).expect("not null");
        assert_eq!(neg, (-1_i32).to_be_bytes().to_vec());
    }

    #[test]
    fn binary_bool_encoding() {
        use ultrasql_vec::column::BoolColumn;
        let col = Column::Bool(BoolColumn::from_data(vec![true, false]));
        let t = encode_binary(&col, 0).expect("not null");
        assert_eq!(t, vec![1u8]);
        let f = encode_binary(&col, 1).expect("not null");
        assert_eq!(f, vec![0u8]);
    }

    #[test]
    fn portal_column_format_fallback_rules() {
        let portal = Portal {
            statement_name: String::new(),
            plan: None,
            params: vec![],
            result_formats: vec![],
        };
        assert_eq!(portal.column_format(0), 0);
        assert_eq!(portal.column_format(99), 0);

        let portal_one = Portal {
            result_formats: vec![1],
            ..portal.clone()
        };
        assert_eq!(portal_one.column_format(0), 1);
        assert_eq!(portal_one.column_format(5), 1);

        let portal_multi = Portal {
            result_formats: vec![0, 1, 0],
            ..portal
        };
        assert_eq!(portal_multi.column_format(0), 0);
        assert_eq!(portal_multi.column_format(1), 1);
        assert_eq!(portal_multi.column_format(2), 0);
    }
}
