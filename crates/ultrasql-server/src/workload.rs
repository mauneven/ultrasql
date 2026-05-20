//! Workload recorder for statement timings, plan hashes, and slow query logs.

use std::collections::{HashMap, VecDeque, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::time::Duration;

use ultrasql_planner::LogicalPlan;

/// Aggregated pg_stat_statements-style metrics for one normalized query.
#[derive(Clone, Debug, PartialEq)]
pub struct WorkloadStatementStats {
    /// Stable hash of the normalized SQL text.
    pub query_id: u64,
    /// Redacted SQL text. Extended Query entries keep `$N` placeholders.
    pub query: String,
    /// Number of executions observed.
    pub calls: u64,
    /// Sum of execution durations.
    pub total_exec_time: Duration,
    /// Fastest execution duration.
    pub min_exec_time: Duration,
    /// Slowest execution duration.
    pub max_exec_time: Duration,
    /// Rows reported by command tags.
    pub rows: u64,
    /// Number of executions that returned a query-scoped error.
    pub errors: u64,
    /// Stable hash of the bound logical plan shape when available.
    pub plan_hash: u64,
    /// Number of bind parameters supplied by the client.
    pub bind_param_count: u32,
    /// Whether concrete bind values were redacted from this record.
    pub bind_params_redacted: bool,
    /// Last query-scoped error text, if any.
    pub last_error: Option<String>,
}

/// One slow-query log entry.
#[derive(Clone, Debug, PartialEq)]
pub struct SlowQueryRecord {
    /// Stable hash of the normalized SQL text.
    pub query_id: u64,
    /// Redacted SQL text. Extended Query entries keep `$N` placeholders.
    pub query: String,
    /// Statement duration.
    pub elapsed: Duration,
    /// Rows reported by command tags.
    pub rows: u64,
    /// Query-scoped error text, if the statement failed.
    pub error: Option<String>,
    /// Stable hash of the bound logical plan shape when available.
    pub plan_hash: u64,
    /// Number of bind parameters supplied by the client.
    pub bind_param_count: u32,
    /// Whether concrete bind values were redacted from this record.
    pub bind_params_redacted: bool,
}

/// One completed query execution submitted to the recorder.
#[derive(Clone, Debug, PartialEq)]
pub struct WorkloadQueryRecord {
    /// SQL text as seen by the execution path.
    pub query: String,
    /// Stable hash of the bound logical plan shape when available.
    pub plan_hash: u64,
    /// Statement duration.
    pub elapsed: Duration,
    /// Rows reported by command tags.
    pub rows: u64,
    /// Query-scoped error text, if the statement failed.
    pub error: Option<String>,
    /// Number of bind parameters supplied by the client.
    pub bind_param_count: u32,
    /// Whether concrete bind values were redacted from this record.
    pub bind_params_redacted: bool,
}

/// In-process workload recorder.
#[derive(Debug)]
pub struct WorkloadRecorder {
    stats: parking_lot::Mutex<HashMap<u64, WorkloadStatementStats>>,
    slow_log: parking_lot::Mutex<VecDeque<SlowQueryRecord>>,
    slow_query_threshold: parking_lot::RwLock<Option<Duration>>,
    slow_log_capacity: usize,
}

impl Default for WorkloadRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkloadRecorder {
    /// Create an empty recorder with slow logging disabled.
    #[must_use]
    pub fn new() -> Self {
        Self {
            stats: parking_lot::Mutex::new(HashMap::new()),
            slow_log: parking_lot::Mutex::new(VecDeque::new()),
            slow_query_threshold: parking_lot::RwLock::new(None),
            slow_log_capacity: 1024,
        }
    }

    /// Set the minimum duration for slow-query log entries.
    pub fn set_slow_query_threshold(&self, threshold: Duration) {
        *self.slow_query_threshold.write() = Some(threshold);
    }

    /// Disable slow-query logging.
    pub fn disable_slow_query_log(&self) {
        *self.slow_query_threshold.write() = None;
    }

    /// Record one completed statement.
    pub fn record(&self, record: WorkloadQueryRecord) {
        let query = normalize_query(&record.query);
        if query.is_empty() {
            return;
        }
        let query_id = stable_hash(&query);
        let plan_hash = if record.plan_hash == 0 {
            stable_hash(&format!("sql:{query}"))
        } else {
            record.plan_hash
        };
        let error = record.error.map(|err| truncate_error(&err));
        let mut stats = self.stats.lock();
        stats
            .entry(query_id)
            .and_modify(|stat| {
                stat.calls = stat.calls.saturating_add(1);
                stat.total_exec_time = stat.total_exec_time.saturating_add(record.elapsed);
                stat.min_exec_time = stat.min_exec_time.min(record.elapsed);
                stat.max_exec_time = stat.max_exec_time.max(record.elapsed);
                stat.rows = stat.rows.saturating_add(record.rows);
                if error.is_some() {
                    stat.errors = stat.errors.saturating_add(1);
                    stat.last_error.clone_from(&error);
                }
                stat.plan_hash = plan_hash;
                stat.bind_param_count = stat.bind_param_count.max(record.bind_param_count);
                stat.bind_params_redacted |= record.bind_params_redacted;
            })
            .or_insert_with(|| WorkloadStatementStats {
                query_id,
                query: query.clone(),
                calls: 1,
                total_exec_time: record.elapsed,
                min_exec_time: record.elapsed,
                max_exec_time: record.elapsed,
                rows: record.rows,
                errors: u64::from(error.is_some()),
                plan_hash,
                bind_param_count: record.bind_param_count,
                bind_params_redacted: record.bind_params_redacted,
                last_error: error.clone(),
            });
        drop(stats);

        let Some(threshold) = *self.slow_query_threshold.read() else {
            return;
        };
        if record.elapsed < threshold {
            return;
        }
        let mut slow_log = self.slow_log.lock();
        if slow_log.len() == self.slow_log_capacity {
            slow_log.pop_front();
        }
        slow_log.push_back(SlowQueryRecord {
            query_id,
            query,
            elapsed: record.elapsed,
            rows: record.rows,
            error,
            plan_hash,
            bind_param_count: record.bind_param_count,
            bind_params_redacted: record.bind_params_redacted,
        });
    }

    /// Return statement stats sorted by query text.
    #[must_use]
    pub fn snapshot(&self) -> Vec<WorkloadStatementStats> {
        let mut stats = self.stats.lock().values().cloned().collect::<Vec<_>>();
        stats.sort_by(|left, right| left.query.cmp(&right.query));
        stats
    }

    /// Return slow-query records in insertion order.
    #[must_use]
    pub fn slow_queries(&self) -> Vec<SlowQueryRecord> {
        self.slow_log.lock().iter().cloned().collect()
    }
}

/// Hash a bound logical plan without exposing literal values to logs.
#[must_use]
pub fn plan_hash_for_plan(plan: &LogicalPlan) -> u64 {
    stable_hash(&format!("{plan:?}"))
}

/// Fallback hash used when execution bypasses a bound plan handle.
#[must_use]
pub fn plan_hash_for_sql(sql: &str) -> u64 {
    stable_hash(&format!("sql:{}", normalize_query(sql)))
}

fn normalize_query(query: &str) -> String {
    query.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn truncate_error(error: &str) -> String {
    const LIMIT: usize = 512;
    error.chars().take(LIMIT).collect()
}

fn stable_hash<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    let hash = hasher.finish();
    if hash == 0 { 1 } else { hash }
}
