//! Workload recorder for statement timings, plan hashes, and slow query logs.

use std::collections::{HashMap, VecDeque, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::time::Duration;

use ultrasql_planner::LogicalPlan;

const LATENCY_BUCKETS_US: [u64; 9] = [
    100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000,
];
const LATENCY_BUCKET_COUNT: usize = LATENCY_BUCKETS_US.len() + 1;

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

/// One cumulative latency histogram bucket.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkloadLatencyBucket {
    /// Inclusive upper bound in microseconds. `u64::MAX` represents `+Inf`.
    pub le_us: u64,
    /// Cumulative observations up to this bound.
    pub count: u64,
}

/// Process-local query latency histogram snapshot.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WorkloadLatencyHistogram {
    /// Cumulative bucket counts.
    pub buckets: Vec<WorkloadLatencyBucket>,
    /// Total observations.
    pub count: u64,
    /// Sum of observed query latencies in microseconds.
    pub sum_us: u64,
}

/// One active `pg_stat_progress_vacuum` row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VacuumProgressSnapshot {
    /// PostgreSQL backend pid owning the VACUUM.
    pub pid: i32,
    /// Database OID. UltraSQL exposes one database in this wave.
    pub datid: i64,
    /// Database name.
    pub datname: String,
    /// Relation OID being vacuumed.
    pub relid: i64,
    /// Current VACUUM phase.
    pub phase: String,
    /// Heap blocks in the relation at VACUUM start.
    pub heap_blks_total: i64,
    /// Heap blocks scanned so far.
    pub heap_blks_scanned: i64,
    /// Heap blocks vacuumed so far.
    pub heap_blks_vacuumed: i64,
}

/// One active `pg_stat_progress_analyze` row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyzeProgressSnapshot {
    /// PostgreSQL backend pid owning ANALYZE; 0 means background worker.
    pub pid: i32,
    /// Database OID. UltraSQL exposes one database in this wave.
    pub datid: i64,
    /// Database name.
    pub datname: String,
    /// Relation OID being analyzed.
    pub relid: i64,
    /// Current ANALYZE phase.
    pub phase: String,
    /// Heap blocks in the relation at ANALYZE start.
    pub sample_blks_total: i64,
    /// Heap blocks scanned so far.
    pub sample_blks_scanned: i64,
}

/// One active `pg_stat_progress_create_index` row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateIndexProgressSnapshot {
    /// PostgreSQL backend pid owning CREATE INDEX.
    pub pid: i32,
    /// Database OID. UltraSQL exposes one database in this wave.
    pub datid: i64,
    /// Database name.
    pub datname: String,
    /// Relation OID being indexed.
    pub relid: i64,
    /// Index relation OID being built.
    pub index_relid: i64,
    /// Current CREATE INDEX phase.
    pub phase: String,
    /// Heap blocks in the relation at CREATE INDEX start.
    pub blocks_total: i64,
    /// Heap blocks processed so far.
    pub blocks_done: i64,
}

/// Cumulative usage counters for one SQL index.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IndexUsageStats {
    /// Index relation OID.
    pub indexrelid: u32,
    /// Number of executor paths that used this index.
    pub idx_scan: u64,
    /// Index entries returned by the access method before heap visibility.
    pub idx_tup_read: u64,
    /// Heap rows fetched and found visible through this index.
    pub idx_tup_fetch: u64,
}

#[derive(Debug, Default)]
struct LatencyHistogramState {
    bucket_counts: [u64; LATENCY_BUCKET_COUNT],
    count: u64,
    sum_us: u64,
}

impl LatencyHistogramState {
    fn observe(&mut self, elapsed: Duration) {
        let elapsed_us = duration_as_micros_saturated(elapsed);
        self.count = self.count.saturating_add(1);
        self.sum_us = self.sum_us.saturating_add(elapsed_us);
        let bucket = LATENCY_BUCKETS_US
            .iter()
            .position(|upper| elapsed_us <= *upper)
            .unwrap_or(LATENCY_BUCKET_COUNT - 1);
        self.bucket_counts[bucket] = self.bucket_counts[bucket].saturating_add(1);
    }

    fn snapshot(&self) -> WorkloadLatencyHistogram {
        let mut buckets = Vec::with_capacity(LATENCY_BUCKET_COUNT);
        let mut cumulative = 0_u64;
        for (idx, upper) in LATENCY_BUCKETS_US.iter().copied().enumerate() {
            cumulative = cumulative.saturating_add(self.bucket_counts[idx]);
            buckets.push(WorkloadLatencyBucket {
                le_us: upper,
                count: cumulative,
            });
        }
        cumulative = cumulative.saturating_add(self.bucket_counts[LATENCY_BUCKET_COUNT - 1]);
        buckets.push(WorkloadLatencyBucket {
            le_us: u64::MAX,
            count: cumulative,
        });
        WorkloadLatencyHistogram {
            buckets,
            count: self.count,
            sum_us: self.sum_us,
        }
    }
}

/// In-process workload recorder.
#[derive(Debug)]
pub struct WorkloadRecorder {
    stats: parking_lot::Mutex<HashMap<u64, WorkloadStatementStats>>,
    slow_log: parking_lot::Mutex<VecDeque<SlowQueryRecord>>,
    slow_query_threshold: parking_lot::RwLock<Option<Duration>>,
    latency_histogram: parking_lot::Mutex<LatencyHistogramState>,
    vacuum_progress: parking_lot::Mutex<HashMap<u32, VacuumProgressSnapshot>>,
    analyze_progress: parking_lot::Mutex<HashMap<u32, AnalyzeProgressSnapshot>>,
    create_index_progress: parking_lot::Mutex<HashMap<u32, CreateIndexProgressSnapshot>>,
    index_usage: parking_lot::Mutex<HashMap<u32, IndexUsageStats>>,
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
            latency_histogram: parking_lot::Mutex::new(LatencyHistogramState::default()),
            vacuum_progress: parking_lot::Mutex::new(HashMap::new()),
            analyze_progress: parking_lot::Mutex::new(HashMap::new()),
            create_index_progress: parking_lot::Mutex::new(HashMap::new()),
            index_usage: parking_lot::Mutex::new(HashMap::new()),
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
        self.latency_histogram.lock().observe(record.elapsed);
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

    /// Return cumulative query latency histogram counters.
    #[must_use]
    pub fn latency_histogram(&self) -> WorkloadLatencyHistogram {
        self.latency_histogram.lock().snapshot()
    }

    /// Start or replace one active VACUUM progress row.
    pub fn begin_vacuum(&self, pid: u32, relid: u32, heap_blks_total: u32) {
        self.vacuum_progress.lock().insert(
            pid,
            VacuumProgressSnapshot {
                pid: u32_to_i32_saturated(pid),
                datid: 1,
                datname: "ultrasql".to_string(),
                relid: i64::from(relid),
                phase: "initializing".to_string(),
                heap_blks_total: i64::from(heap_blks_total),
                heap_blks_scanned: 0,
                heap_blks_vacuumed: 0,
            },
        );
    }

    /// Update counters and phase for an active VACUUM progress row.
    pub fn update_vacuum(
        &self,
        pid: u32,
        phase: impl Into<String>,
        heap_blks_scanned: u32,
        heap_blks_vacuumed: u32,
    ) {
        if let Some(row) = self.vacuum_progress.lock().get_mut(&pid) {
            row.phase = phase.into();
            row.heap_blks_scanned = i64::from(heap_blks_scanned);
            row.heap_blks_vacuumed = i64::from(heap_blks_vacuumed);
        }
    }

    /// Clear one active VACUUM progress row.
    pub fn finish_vacuum(&self, pid: u32) {
        self.vacuum_progress.lock().remove(&pid);
    }

    /// Return active VACUUM progress rows ordered by pid.
    #[must_use]
    pub fn vacuum_progress(&self) -> Vec<VacuumProgressSnapshot> {
        let mut rows = self
            .vacuum_progress
            .lock()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by_key(|row| row.pid);
        rows
    }

    /// Start or replace one active CREATE INDEX progress row.
    pub fn begin_create_index(&self, pid: u32, relid: u32, index_relid: u32, blocks_total: u32) {
        self.create_index_progress.lock().insert(
            pid,
            CreateIndexProgressSnapshot {
                pid: u32_to_i32_saturated(pid),
                datid: 1,
                datname: "ultrasql".to_string(),
                relid: i64::from(relid),
                index_relid: i64::from(index_relid),
                phase: "initializing".to_string(),
                blocks_total: i64::from(blocks_total),
                blocks_done: 0,
            },
        );
    }

    /// Update counters and phase for an active CREATE INDEX progress row.
    pub fn update_create_index(&self, pid: u32, phase: impl Into<String>, blocks_done: u32) {
        if let Some(row) = self.create_index_progress.lock().get_mut(&pid) {
            row.phase = phase.into();
            row.blocks_done = i64::from(blocks_done);
        }
    }

    /// Clear one active CREATE INDEX progress row.
    pub fn finish_create_index(&self, pid: u32) {
        self.create_index_progress.lock().remove(&pid);
    }

    /// Return active CREATE INDEX progress rows ordered by pid.
    #[must_use]
    pub fn create_index_progress(&self) -> Vec<CreateIndexProgressSnapshot> {
        let mut rows = self
            .create_index_progress
            .lock()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by_key(|row| row.pid);
        rows
    }

    /// Start or replace one active ANALYZE progress row.
    pub fn begin_analyze(&self, pid: u32, relid: u32, sample_blks_total: u32) {
        self.analyze_progress.lock().insert(
            pid,
            AnalyzeProgressSnapshot {
                pid: u32_to_i32_saturated(pid),
                datid: 1,
                datname: "ultrasql".to_string(),
                relid: i64::from(relid),
                phase: "initializing".to_string(),
                sample_blks_total: i64::from(sample_blks_total),
                sample_blks_scanned: 0,
            },
        );
    }

    /// Update counters and phase for an active ANALYZE progress row.
    pub fn update_analyze(&self, pid: u32, phase: impl Into<String>, sample_blks_scanned: u32) {
        if let Some(row) = self.analyze_progress.lock().get_mut(&pid) {
            row.phase = phase.into();
            row.sample_blks_scanned = i64::from(sample_blks_scanned);
        }
    }

    /// Clear one active ANALYZE progress row.
    pub fn finish_analyze(&self, pid: u32) {
        self.analyze_progress.lock().remove(&pid);
    }

    /// Return active ANALYZE progress rows ordered by pid.
    #[must_use]
    pub fn analyze_progress(&self) -> Vec<AnalyzeProgressSnapshot> {
        let mut rows = self
            .analyze_progress
            .lock()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by_key(|row| row.pid);
        rows
    }

    /// Add one completed index access to cumulative `pg_stat_user_indexes`
    /// counters.
    pub fn record_index_usage(&self, indexrelid: u32, tuples_read: u64, tuples_fetched: u64) {
        let mut usage = self.index_usage.lock();
        usage
            .entry(indexrelid)
            .and_modify(|stats| {
                stats.idx_scan = stats.idx_scan.saturating_add(1);
                stats.idx_tup_read = stats.idx_tup_read.saturating_add(tuples_read);
                stats.idx_tup_fetch = stats.idx_tup_fetch.saturating_add(tuples_fetched);
            })
            .or_insert(IndexUsageStats {
                indexrelid,
                idx_scan: 1,
                idx_tup_read: tuples_read,
                idx_tup_fetch: tuples_fetched,
            });
    }

    /// Return cumulative usage counters for one index, if any.
    #[must_use]
    pub fn index_usage_for(&self, indexrelid: u32) -> IndexUsageStats {
        self.index_usage
            .lock()
            .get(&indexrelid)
            .cloned()
            .unwrap_or(IndexUsageStats {
                indexrelid,
                ..IndexUsageStats::default()
            })
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

fn duration_as_micros_saturated(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn u32_to_i32_saturated(value: u32) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workload_recorder_tracks_latency_histogram() {
        let recorder = WorkloadRecorder::new();
        recorder.record(WorkloadQueryRecord {
            query: "SELECT 1".to_string(),
            plan_hash: 0,
            elapsed: Duration::from_micros(250),
            rows: 1,
            error: None,
            bind_param_count: 0,
            bind_params_redacted: false,
        });
        recorder.record(WorkloadQueryRecord {
            query: "SELECT 2".to_string(),
            plan_hash: 0,
            elapsed: Duration::from_micros(2_500),
            rows: 1,
            error: None,
            bind_param_count: 0,
            bind_params_redacted: false,
        });

        let histogram = recorder.latency_histogram();
        assert_eq!(histogram.count, 2);
        assert_eq!(histogram.sum_us, 2_750);
        assert!(
            histogram
                .buckets
                .iter()
                .any(|bucket| bucket.le_us == 500 && bucket.count == 1)
        );
        assert!(
            histogram
                .buckets
                .iter()
                .any(|bucket| bucket.le_us == u64::MAX && bucket.count == 2)
        );
    }

    #[test]
    fn workload_recorder_tracks_vacuum_progress_lifecycle() {
        let recorder = WorkloadRecorder::new();
        recorder.begin_vacuum(7, 42, 9);
        recorder.update_vacuum(7, "vacuuming heap", 4, 3);

        let rows = recorder.vacuum_progress();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0],
            VacuumProgressSnapshot {
                pid: 7,
                datid: 1,
                datname: "ultrasql".to_string(),
                relid: 42,
                phase: "vacuuming heap".to_string(),
                heap_blks_total: 9,
                heap_blks_scanned: 4,
                heap_blks_vacuumed: 3,
            }
        );

        recorder.finish_vacuum(7);
        assert!(recorder.vacuum_progress().is_empty());
    }

    #[test]
    fn workload_recorder_tracks_analyze_progress_lifecycle() {
        let recorder = WorkloadRecorder::new();
        recorder.begin_analyze(8, 43, 10);
        recorder.update_analyze(8, "computing statistics", 10);

        let rows = recorder.analyze_progress();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0],
            AnalyzeProgressSnapshot {
                pid: 8,
                datid: 1,
                datname: "ultrasql".to_string(),
                relid: 43,
                phase: "computing statistics".to_string(),
                sample_blks_total: 10,
                sample_blks_scanned: 10,
            }
        );

        recorder.finish_analyze(8);
        assert!(recorder.analyze_progress().is_empty());
    }

    #[test]
    fn workload_recorder_tracks_create_index_progress_lifecycle() {
        let recorder = WorkloadRecorder::new();
        recorder.begin_create_index(9, 44, 45, 11);
        recorder.update_create_index(9, "building index", 7);

        let rows = recorder.create_index_progress();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0],
            CreateIndexProgressSnapshot {
                pid: 9,
                datid: 1,
                datname: "ultrasql".to_string(),
                relid: 44,
                index_relid: 45,
                phase: "building index".to_string(),
                blocks_total: 11,
                blocks_done: 7,
            }
        );

        recorder.finish_create_index(9);
        assert!(recorder.create_index_progress().is_empty());
    }

    #[test]
    fn workload_recorder_accumulates_index_usage() {
        let recorder = WorkloadRecorder::new();
        recorder.record_index_usage(99, 3, 2);
        recorder.record_index_usage(99, 4, 4);

        assert_eq!(
            recorder.index_usage_for(99),
            IndexUsageStats {
                indexrelid: 99,
                idx_scan: 2,
                idx_tup_read: 7,
                idx_tup_fetch: 6,
            }
        );
        assert_eq!(
            recorder.index_usage_for(100),
            IndexUsageStats {
                indexrelid: 100,
                ..IndexUsageStats::default()
            }
        );
    }
}
