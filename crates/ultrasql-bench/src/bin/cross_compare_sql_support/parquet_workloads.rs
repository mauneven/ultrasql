//! Parquet wire workloads: the arena `read_parquet` smoke (scan,
//! projection/predicate pushdown, row-group pruning) and the
//! object-store range-only smoke backed by an in-process mock S3.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use ultrasql_objectstore::{object_range_cache_metrics, override_s3_endpoint_for_process};

use super::types::{
    ObjectParquetRangeMetrics, PARQUET_SMOKE_MIN_ROWS, PARQUET_SMOKE_ROW_GROUP_ROWS,
    ParquetSmokeMetrics,
};
use super::util::{connect_sql_server, measure_simple_query, sql_string};

pub(crate) async fn run_parquet_smoke(
    server: SocketAddr,
    requested_rows: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<ParquetSmokeMetrics> {
    let rows = requested_rows.max(PARQUET_SMOKE_MIN_ROWS);
    let dir = tempfile::tempdir().context("create parquet smoke tempdir")?;
    let parquet_path = dir.path().join("arena_parquet_smoke.parquet");
    write_parquet_smoke_file(&parquet_path, rows)?;

    let (client, conn_handle) = connect_sql_server(server).await?;
    let path_sql = sql_string(&parquet_path);
    let pruning_threshold = rows / 2;
    let workloads = [
        (
            "scan",
            format!("SELECT COUNT(*) FROM read_parquet({path_sql})"),
        ),
        (
            "projection",
            format!("SELECT metric FROM read_parquet({path_sql})"),
        ),
        (
            "predicate",
            format!("SELECT COUNT(*) FROM read_parquet({path_sql}) WHERE category = 'alpha'"),
        ),
        (
            "row_group_pruning",
            format!(
                "SELECT COUNT(*) FROM read_parquet({path_sql}) WHERE id >= {pruning_threshold}"
            ),
        ),
    ];

    let scan = measure_simple_query(
        &client,
        workloads[0].0,
        &workloads[0].1,
        warmup,
        total_iters,
    )
    .await?;
    iters_us.extend(scan.samples_us.iter().copied());
    let projection = measure_simple_query(
        &client,
        workloads[1].0,
        &workloads[1].1,
        warmup,
        total_iters,
    )
    .await?;
    let predicate = measure_simple_query(
        &client,
        workloads[2].0,
        &workloads[2].1,
        warmup,
        total_iters,
    )
    .await?;
    let row_group_pruning = measure_simple_query(
        &client,
        workloads[3].0,
        &workloads[3].1,
        warmup,
        total_iters,
    )
    .await?;

    drop(client);
    conn_handle.abort();
    Ok(ParquetSmokeMetrics {
        rows,
        scan_us: scan.median_us,
        projection_pushdown_us: projection.median_us,
        predicate_pushdown_us: predicate.median_us,
        row_group_pruning_us: row_group_pruning.median_us,
        scan_samples_us: scan.samples_us,
        projection_pushdown_samples_us: projection.samples_us,
        predicate_pushdown_samples_us: predicate.samples_us,
        row_group_pruning_samples_us: row_group_pruning.samples_us,
        answer: serde_json::json!({
            "scan_rows": scan.rows,
            "projection_rows": projection.rows.len(),
            "predicate_rows": predicate.rows,
            "row_group_pruning_rows": row_group_pruning.rows,
            "row_group_pruning_threshold": pruning_threshold,
            "source": "generated_arrow_parquet_with_flushed_row_groups",
        }),
    })
}

fn write_parquet_smoke_file(path: &Path, rows: usize) -> Result<()> {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("category", ArrowDataType::Utf8, false),
        ArrowField::new("metric", ArrowDataType::Int64, false),
    ]));
    let file = std::fs::File::create(path)
        .with_context(|| format!("create parquet smoke file {}", path.display()))?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)
        .with_context(|| format!("open parquet smoke writer {}", path.display()))?;
    for start in (0..rows).step_by(PARQUET_SMOKE_ROW_GROUP_ROWS) {
        let end = (start + PARQUET_SMOKE_ROW_GROUP_ROWS).min(rows);
        let ids = (start..end)
            .map(|row| i64::try_from(row).unwrap_or(i64::MAX))
            .collect::<Vec<_>>();
        let categories = (start..end)
            .map(|row| match row % 4 {
                0 => "alpha",
                1 => "beta",
                2 => "gamma",
                _ => "delta",
            })
            .collect::<Vec<_>>();
        let metrics = (start..end)
            .map(|row| i64::try_from(row.wrapping_mul(17) % 1_000).unwrap_or(0))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(categories)),
                Arc::new(Int64Array::from(metrics)),
            ],
        )
        .context("build parquet smoke record batch")?;
        writer
            .write(&batch)
            .with_context(|| format!("write parquet smoke rows [{start}, {end})"))?;
        writer
            .flush()
            .with_context(|| format!("flush parquet smoke row group ending at {end}"))?;
    }
    writer
        .close()
        .with_context(|| format!("close parquet smoke file {}", path.display()))?;
    Ok(())
}

pub(crate) async fn run_object_parquet_range_smoke(
    server: SocketAddr,
    _requested_rows: usize,
    warmup: usize,
    total_iters: usize,
    iters_us: &mut Vec<f64>,
) -> Result<ObjectParquetRangeMetrics> {
    let rows = 3;
    let dir = tempfile::tempdir().context("create object parquet tempdir")?;
    let parquet_path = dir.path().join("object_range.parquet");
    write_object_range_parquet_file(&parquet_path, rows)?;
    let object_bytes = std::fs::read(&parquet_path)
        .with_context(|| format!("read object parquet {}", parquet_path.display()))?;
    let object_len = object_bytes.len();
    let whole_object_range = format!("bytes=0-{}", object_len.saturating_sub(1));
    let score_ranges = parquet_column_ranges(&parquet_path, "score")?;
    let mock = BenchMockS3::range_only(vec![(
        "/lake/parquet/object_range.parquet",
        object_bytes.clone(),
    )])?;
    let _endpoint_override = override_s3_endpoint_for_process(mock.endpoint.clone());

    let (client, conn_handle) = connect_sql_server(server).await?;
    let query =
        "SELECT name FROM read_parquet('s3://lake/parquet/object_range.parquet') WHERE id >= 100";
    let cache_metrics_before = object_range_cache_metrics();
    let timed =
        measure_simple_query(&client, "object_parquet_range", query, warmup, total_iters).await?;
    iters_us.extend(timed.samples_us.iter().copied());
    drop(client);
    conn_handle.abort();

    let object_requests = mock
        .requests()
        .into_iter()
        .filter(|request| request.path == "/lake/parquet/object_range.parquet")
        .collect::<Vec<_>>();
    if object_requests.is_empty() {
        anyhow::bail!("object Parquet range smoke made no object requests");
    }
    if object_requests
        .iter()
        .any(|request| request.range.is_none())
    {
        anyhow::bail!("object Parquet range smoke made a full-object request");
    }
    let length_probe_seen = object_requests
        .iter()
        .any(|request| request.range.as_deref() == Some("bytes=0-0"));
    if !length_probe_seen {
        anyhow::bail!("object Parquet range smoke did not issue bytes=0-0 length probe");
    }
    let whole_object_fetched = object_requests
        .iter()
        .any(|request| request.range.as_deref() == Some(whole_object_range.as_str()));
    if whole_object_fetched {
        anyhow::bail!("object Parquet range smoke fetched the whole object");
    }
    let projected_out_column_fetched = object_requests
        .iter()
        .any(|request| request_overlaps_any_range(request, &score_ranges));
    if projected_out_column_fetched {
        anyhow::bail!(
            "object Parquet range smoke fetched projected-out score column chunks: requests={object_requests:?} score_ranges={score_ranges:?}"
        );
    }
    let requested_range_bytes = object_requests
        .iter()
        .filter_map(|request| request.range.as_deref().and_then(request_range_bounds))
        .map(|(start, end)| end.saturating_sub(start).saturating_add(1))
        .sum();
    let requests = object_requests
        .iter()
        .map(|request| {
            serde_json::json!({
                "path": request.path,
                "range": request.range,
            })
        })
        .collect::<Vec<_>>();
    let cache_metrics_after = object_range_cache_metrics();

    Ok(ObjectParquetRangeMetrics {
        query_median_us: timed.median_us,
        samples_us: timed.samples_us,
        answer: serde_json::json!({
            "rows": timed.rows,
            "source": "local_s3_range_only_mock",
        }),
        object_bytes: object_len,
        range_request_count: object_requests.len(),
        requested_range_bytes,
        remote_bytes: cache_metrics_after
            .remote_bytes
            .saturating_sub(cache_metrics_before.remote_bytes),
        cache_hits: cache_metrics_after
            .cache_hits
            .saturating_sub(cache_metrics_before.cache_hits),
        cache_misses: cache_metrics_after
            .cache_misses
            .saturating_sub(cache_metrics_before.cache_misses),
        length_probe_seen,
        whole_object_fetched,
        projected_out_column_fetched,
        requests,
    })
}

fn write_object_range_parquet_file(path: &Path, _rows: usize) -> Result<()> {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("name", ArrowDataType::Utf8, false),
        ArrowField::new("score", ArrowDataType::Int64, false),
    ]));
    let file = std::fs::File::create(path)
        .with_context(|| format!("create object range parquet {}", path.display()))?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)
        .with_context(|| format!("open object range parquet writer {}", path.display()))?;
    write_object_range_batch(
        &mut writer,
        Arc::clone(&schema),
        &[(10, "Alpha", 1), (20, "Beta", 2)],
    )?;
    writer
        .flush()
        .context("flush first object range row group")?;
    write_object_range_batch(&mut writer, schema, &[(100, "Zed", 99)])?;
    writer.close().context("close object range parquet")?;
    Ok(())
}

fn write_object_range_batch(
    writer: &mut ArrowWriter<std::fs::File>,
    schema: Arc<ArrowSchema>,
    rows: &[(i64, &str, i64)],
) -> Result<()> {
    let ids = rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>();
    let names = rows.iter().map(|(_, name, _)| *name).collect::<Vec<_>>();
    let scores = rows.iter().map(|(_, _, score)| *score).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(scores)),
        ],
    )
    .context("build object range parquet batch")?;
    writer
        .write(&batch)
        .context("write object range parquet row group")?;
    Ok(())
}

fn parquet_column_ranges(path: &Path, column: &str) -> Result<Vec<(u64, u64)>> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("open parquet metadata {}", path.display()))?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).context("read parquet metadata")?;
    let column_index = builder
        .schema()
        .fields()
        .iter()
        .position(|field| field.name() == column)
        .with_context(|| format!("metadata column {column} missing"))?;
    Ok((0..builder.metadata().num_row_groups())
        .map(|row_group| {
            let (start, len) = builder
                .metadata()
                .row_group(row_group)
                .column(column_index)
                .byte_range();
            (start, start + len.saturating_sub(1))
        })
        .collect())
}

struct BenchMockS3 {
    endpoint: String,
    shutdown: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
    requests: Arc<Mutex<Vec<BenchMockS3Request>>>,
}

impl BenchMockS3 {
    fn range_only(objects: Vec<(&str, Vec<u8>)>) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").context("bind object range mock")?;
        listener
            .set_nonblocking(true)
            .context("object range mock nonblocking")?;
        let endpoint = format!(
            "http://{}",
            listener
                .local_addr()
                .context("read object range mock addr")?
        );
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = Arc::clone(&shutdown);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let thread_requests = Arc::clone(&requests);
        let objects = objects
            .into_iter()
            .map(|(path, body)| (path.to_owned(), body))
            .collect::<BTreeMap<_, _>>();
        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((mut stream, _addr)) => {
                        handle_bench_mock_s3_stream(&mut stream, &objects, &thread_requests);
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_err) => break,
                }
            }
        });
        Ok(Self {
            endpoint,
            shutdown,
            handle: Some(handle),
            requests,
        })
    }

    fn requests(&self) -> Vec<BenchMockS3Request> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl Drop for BenchMockS3 {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(addr) = self.endpoint.strip_prefix("http://") {
            let _ = TcpStream::connect(addr);
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[derive(Clone, Debug)]
struct BenchMockS3Request {
    path: String,
    range: Option<String>,
}

fn handle_bench_mock_s3_stream(
    stream: &mut TcpStream,
    objects: &BTreeMap<String, Vec<u8>>,
    requests: &Arc<Mutex<Vec<BenchMockS3Request>>>,
) {
    let mut buf = [0_u8; 4096];
    let Ok(n) = stream.read(&mut buf) else {
        return;
    };
    let request = String::from_utf8_lossy(&buf[..n]);
    let range = header_value(&request, "range");
    let Some(target) = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
    else {
        return;
    };
    let (path, _query) = target.split_once('?').unwrap_or((target, ""));
    requests
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(BenchMockS3Request {
            path: path.to_owned(),
            range: range.clone(),
        });
    if let Some(body) = objects.get(path) {
        if let Some(range) = range {
            if let Some((start, end)) = parse_bytes_range(&range, body.len()) {
                write_mock_range_response(stream, body, start, end);
            } else {
                write_mock_response(stream, 416, "text/plain", b"bad range");
            }
        } else {
            write_mock_response(stream, 400, "text/plain", b"range required");
        }
    } else {
        write_mock_response(stream, 404, "text/plain", b"not found");
    }
}

fn header_value(request: &str, name: &str) -> Option<String> {
    let prefix = format!("{name}:");
    request.lines().find_map(|line| {
        line.to_ascii_lowercase()
            .strip_prefix(&prefix)
            .map(|_| line[prefix.len()..].trim().to_owned())
    })
}

fn parse_bytes_range(range: &str, len: usize) -> Option<(usize, usize)> {
    let range = range.strip_prefix("bytes=")?;
    let (start, end) = range.split_once('-')?;
    let start = start.parse::<usize>().ok()?;
    let end = end.parse::<usize>().ok()?.min(len.checked_sub(1)?);
    (start <= end && end < len).then_some((start, end))
}

fn write_mock_response(stream: &mut TcpStream, status: u16, content_type: &str, body: &[u8]) {
    let reason = match status {
        200 => "OK",
        206 => "Partial Content",
        400 => "Bad Request",
        404 => "Not Found",
        416 => "Range Not Satisfiable",
        _ => "Status",
    };
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(body);
}

fn write_mock_range_response(stream: &mut TcpStream, body: &[u8], start: usize, end: usize) {
    let slice = &body[start..=end];
    let header = format!(
        "HTTP/1.1 206 Partial Content\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nConnection: close\r\n\r\n",
        slice.len(),
        body.len()
    );
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(slice);
}

fn request_overlaps_any_range(request: &BenchMockS3Request, ranges: &[(u64, u64)]) -> bool {
    let Some((start, end)) = request.range.as_deref().and_then(request_range_bounds) else {
        return false;
    };
    ranges
        .iter()
        .any(|(range_start, range_end)| start <= *range_end && end >= *range_start)
}

fn request_range_bounds(range: &str) -> Option<(u64, u64)> {
    let range = range.strip_prefix("bytes=")?;
    let (start, end) = range.split_once('-')?;
    Some((start.parse().ok()?, end.parse().ok()?))
}
