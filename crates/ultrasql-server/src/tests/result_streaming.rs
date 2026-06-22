//! Byte-identity, bounded-memory, and mid-stream-error harness for the
//! windowed SELECT streaming path (design §7).
//!
//! The primary correctness gate is byte identity: the concatenation of
//! the streamed windows must equal — byte for byte — the single body the
//! legacy whole-buffer path produces, across a battery of row counts,
//! column shapes, and window high-water marks. A `MockOperator` lets us
//! drive exact batch sequences deterministically without a full plan.

use bytes::BytesMut;
use ultrasql_core::{DataType, Field, Schema};
use ultrasql_executor::{ExecError, Operator};
use ultrasql_protocol::{BackendMessage, decode_backend};
use ultrasql_vec::Batch;
use ultrasql_vec::bitmap::Bitmap;
use ultrasql_vec::column::{BoolColumn, Column, NumericColumn, StringColumn};

use crate::result_encoder::{
    STREAM_WINDOW_HIGH_WATER_BYTES, StreamingSelect, TextEncodingOptions, encode_window,
    stream_select_with_options,
};

/// A deterministic operator that hands back a fixed list of pre-built
/// batches, optionally erroring after a configured number of batches.
#[derive(Debug)]
struct MockOperator {
    schema: Schema,
    batches: std::vec::IntoIter<Batch>,
    /// When `Some(k)`, return `Err` once `k` batches have been emitted.
    err_after: Option<usize>,
    emitted: usize,
}

impl MockOperator {
    fn new(schema: Schema, batches: Vec<Batch>) -> Self {
        Self {
            schema,
            batches: batches.into_iter(),
            err_after: None,
            emitted: 0,
        }
    }

    fn erroring_after(schema: Schema, batches: Vec<Batch>, err_after: usize) -> Self {
        Self {
            schema,
            batches: batches.into_iter(),
            err_after: Some(err_after),
            emitted: 0,
        }
    }
}

impl Operator for MockOperator {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if let Some(k) = self.err_after
            && self.emitted >= k
        {
            return Err(ExecError::TypeMismatch("mock mid-stream error".to_owned()));
        }
        match self.batches.next() {
            Some(b) => {
                self.emitted += 1;
                Ok(Some(b))
            }
            None => Ok(None),
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn boxed(op: MockOperator) -> Box<dyn Operator> {
    Box::new(op)
}

/// Drive the windowed encoder over `op` and return the concatenation of
/// every window's bytes, prefixed with the `RowDescription` exactly as
/// `begin_streaming_select` emits it. `high_water` is deliberately tiny
/// in most tests to force many windows.
fn encode_windowed(
    op: Box<dyn Operator>,
    opts: &TextEncodingOptions,
    high_water: usize,
) -> BytesMut {
    let mut handle = StreamingSelect::for_test(op, opts.clone());
    let mut out = BytesMut::new();
    // RowDescription is shipped by window 0 in production; mirror that
    // here so the concatenation lines up with the reference body.
    let row_desc = build_row_description_for_test(handle.schema());
    ultrasql_protocol::encode_backend(&row_desc, &mut out);
    loop {
        let mut win = BytesMut::new();
        match encode_window(&mut handle, &mut win, high_water) {
            Ok(more) => {
                out.extend_from_slice(&win);
                if !more {
                    break;
                }
            }
            Err(e) => panic!("unexpected encode error: {e}"),
        }
    }
    // The running counter must equal the count embedded in the trailing
    // `CommandComplete` tag the windowed path just wrote.
    let tag_count = command_complete_count(&out);
    assert_eq!(
        handle.rows(),
        tag_count,
        "StreamingSelect::rows() ({}) disagrees with the CommandComplete tag ({tag_count})",
        handle.rows()
    );
    out
}

/// Decode the `SELECT <n>` count out of a fully-encoded body's trailing
/// `CommandComplete` frame.
fn command_complete_count(body: &BytesMut) -> u64 {
    let mut b = body.clone();
    let mut count = 0u64;
    while !b.is_empty() {
        if let Some(BackendMessage::CommandComplete { tag }) =
            decode_backend(&mut b).expect("decode frame")
        {
            count = tag
                .rsplit(' ')
                .next()
                .and_then(|n| n.parse().ok())
                .expect("SELECT <n> tag");
        }
    }
    count
}

/// Reference body: the legacy whole-buffer encoder.
fn encode_reference(op: &mut dyn Operator, opts: &TextEncodingOptions) -> BytesMut {
    let mut buf = BytesMut::new();
    stream_select_with_options(op, &mut buf, opts).expect("reference encode");
    buf
}

/// Re-build the `RowDescription` the way `result_encoder` does. We decode
/// the reference body's first frame to obtain the canonical bytes rather
/// than re-implementing the OID mapping; this keeps the harness honest
/// about field metadata.
fn build_row_description_for_test(schema: &Schema) -> BackendMessage {
    // Encode a zero-row body through the reference path and pull the
    // RowDescription back out — guarantees identical field encoding.
    let mut op = MockOperator::new(schema.clone(), vec![]);
    let body = encode_reference(&mut op, &TextEncodingOptions::default());
    let mut b = body;
    match decode_backend(&mut b).expect("decode rowdesc") {
        Some(msg @ BackendMessage::RowDescription { .. }) => msg,
        other => panic!("expected RowDescription, got {other:?}"),
    }
}

// ---------- column / batch builders ----------

fn int32_col(data: Vec<i32>) -> Column {
    Column::Int32(NumericColumn::from_data(data))
}

fn int64_col(data: Vec<i64>) -> Column {
    Column::Int64(NumericColumn::from_data(data))
}

fn nulls_from(valid: &[bool]) -> Bitmap {
    let mut b = Bitmap::new(valid.len(), false);
    for (i, v) in valid.iter().enumerate() {
        b.set(i, *v); // 1 = valid, 0 = null
    }
    b
}

fn int32_col_nullable(data: Vec<i32>, valid: &[bool]) -> Column {
    Column::Int32(NumericColumn::with_nulls(data, nulls_from(valid)).unwrap())
}

fn text_col(rows: Vec<&str>) -> Column {
    Column::Utf8(StringColumn::from_data(rows.into_iter().map(str::to_owned)))
}

fn text_col_nullable(rows: Vec<Option<&str>>) -> Column {
    let valid: Vec<bool> = rows.iter().map(Option::is_some).collect();
    let data: Vec<String> = rows.iter().map(|r| r.unwrap_or("").to_owned()).collect();
    Column::Utf8(StringColumn::with_nulls(data, nulls_from(&valid)).unwrap())
}

fn float64_col(data: Vec<f64>) -> Column {
    Column::Float64(NumericColumn::from_data(data))
}

fn bool_col(data: Vec<bool>) -> Column {
    Column::Bool(BoolColumn::from_data(data))
}

fn batch(cols: Vec<Column>) -> Batch {
    Batch::new(cols).expect("batch")
}

// ---------- §7.1 byte-identity sweep ----------

/// Assert windowed concatenation == reference body for every high-water
/// mark, and re-decode the windowed body into whole frames.
fn assert_byte_identical(schema: &Schema, batches: Vec<Batch>, opts: &TextEncodingOptions) {
    let mut ref_op = MockOperator::new(schema.clone(), batches.clone());
    let reference = encode_reference(&mut ref_op, opts);

    for &hw in &[
        1_usize,
        8,
        64,
        4096,
        STREAM_WINDOW_HIGH_WATER_BYTES,
        usize::MAX / 2, // larger-than-body: single window
    ] {
        let op = boxed(MockOperator::new(schema.clone(), batches.clone()));
        let windowed = encode_windowed(op, opts, hw);
        assert_eq!(
            windowed.as_ref(),
            reference.as_ref(),
            "byte mismatch at high_water={hw}; first diff at offset {}",
            first_diff(windowed.as_ref(), reference.as_ref())
        );
        assert_whole_frames(&windowed);
    }
}

fn first_diff(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

/// Decode the streamed concatenation and assert it is exactly
/// `RowDescription · DataRow* · CommandComplete` with no partial frame.
fn assert_whole_frames(body: &BytesMut) {
    let mut b = body.clone();
    let mut saw_rowdesc = false;
    let mut saw_complete = false;
    let mut data_rows = 0usize;
    while !b.is_empty() {
        match decode_backend(&mut b).expect("decode frame") {
            Some(BackendMessage::RowDescription { .. }) => {
                assert!(!saw_rowdesc, "duplicate RowDescription");
                saw_rowdesc = true;
            }
            Some(BackendMessage::DataRow { .. }) => {
                assert!(!saw_complete, "DataRow after CommandComplete");
                data_rows += 1;
            }
            Some(BackendMessage::CommandComplete { .. }) => {
                assert!(!saw_complete, "duplicate CommandComplete");
                saw_complete = true;
            }
            Some(other) => panic!("unexpected frame in streamed body: {other:?}"),
            None => panic!("streamed body ended on a partial frame"),
        }
    }
    assert!(saw_rowdesc, "missing RowDescription");
    assert!(saw_complete, "missing CommandComplete");
    let _ = data_rows;
}

#[test]
fn byte_identity_zero_rows() {
    let schema = Schema::new([
        Field::required("a", DataType::Int32),
        Field::required("b", DataType::Int32),
    ])
    .unwrap();
    assert_byte_identical(&schema, vec![], &TextEncodingOptions::default());
}

#[test]
fn byte_identity_single_row_int32_pair_fast_path() {
    let schema = Schema::new([
        Field::required("a", DataType::Int32),
        Field::required("b", DataType::Int32),
    ])
    .unwrap();
    let batches = vec![batch(vec![int32_col(vec![42]), int32_col(vec![-7])])];
    assert_byte_identical(&schema, batches, &TextEncodingOptions::default());
}

#[test]
fn byte_identity_int32_pair_many_batches() {
    let schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::required("val", DataType::Int32),
    ])
    .unwrap();
    let mut batches = Vec::new();
    for chunk in 0..16 {
        let base = chunk * 100;
        let a: Vec<i32> = (0..100).map(|i| base + i).collect();
        let b: Vec<i32> = (0..100).map(|i| -(base + i)).collect();
        batches.push(batch(vec![int32_col(a), int32_col(b)]));
    }
    assert_byte_identical(&schema, batches, &TextEncodingOptions::default());
}

#[test]
fn byte_identity_int32_int64_pair_fast_path_with_nulls() {
    let schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("rn", DataType::Int64),
    ])
    .unwrap();
    let a = int32_col_nullable(vec![1, 2, 3, 4], &[true, true, true, true]);
    let b = Column::Int64(
        NumericColumn::with_nulls(
            vec![10_i64, 0, 30, 0],
            nulls_from(&[true, false, true, false]),
        )
        .unwrap(),
    );
    assert_byte_identical(
        &schema,
        vec![batch(vec![a, b])],
        &TextEncodingOptions::default(),
    );
}

#[test]
fn byte_identity_general_mixed_types() {
    let schema = Schema::new([
        Field::required("i32", DataType::Int32),
        Field::required("i64", DataType::Int64),
        Field::required("txt", DataType::Text { max_len: None }),
        Field::required("f64", DataType::Float64),
        Field::required("b", DataType::Bool),
    ])
    .unwrap();
    let cols = vec![
        int32_col(vec![1, -2, i32::MIN, i32::MAX, 0]),
        int64_col(vec![100, -200, i64::MIN, i64::MAX, 0]),
        text_col(vec!["alpha", "", "Ünïcödé", "x", "with\tnewline\nand more"]),
        float64_col(vec![
            1.5,
            -0.0,
            f64::INFINITY,
            f64::NAN,
            std::f64::consts::PI,
        ]),
        bool_col(vec![true, false, true, false, true]),
    ];
    assert_byte_identical(&schema, vec![batch(cols)], &TextEncodingOptions::default());
}

#[test]
fn byte_identity_nulls_everywhere() {
    let schema = Schema::new([
        Field::nullable("i32", DataType::Int32),
        Field::nullable("txt", DataType::Text { max_len: None }),
    ])
    .unwrap();
    let cols = vec![
        int32_col_nullable(vec![0, 0, 0], &[false, false, false]),
        text_col_nullable(vec![None, None, None]),
    ];
    assert_byte_identical(&schema, vec![batch(cols)], &TextEncodingOptions::default());
}

#[test]
fn byte_identity_long_strings_span_a_window() {
    let schema = Schema::new([Field::required("s", DataType::Text { max_len: None })]).unwrap();
    // Each string is far larger than the small high-water marks, so a
    // single DataRow exceeds a window; the encoder must still keep whole
    // frames and stay byte-identical.
    let long = "x".repeat(10_000);
    let multibyte = "café—😀—".repeat(500);
    let cols = vec![text_col(vec![long.as_str(), multibyte.as_str(), "short"])];
    assert_byte_identical(&schema, vec![batch(cols)], &TextEncodingOptions::default());
}

#[test]
fn byte_identity_wide_row() {
    let mut fields = Vec::new();
    for i in 0..32 {
        fields.push(Field::required(format!("c{i}"), DataType::Int32));
    }
    let schema = Schema::new(fields).unwrap();
    let cols: Vec<Column> = (0..32).map(|i| int32_col(vec![i, i * 2, i * 3])).collect();
    assert_byte_identical(&schema, vec![batch(cols)], &TextEncodingOptions::default());
}

#[test]
fn byte_identity_window_boundary_row_counts() {
    // Row counts straddling typical window edges, to be sure boundary
    // placement never changes bytes regardless of where a window ends.
    let schema = Schema::new([
        Field::required("a", DataType::Int32),
        Field::required("b", DataType::Int32),
    ])
    .unwrap();
    for n in [0i32, 1, 2, 7, 63, 64, 65, 1000, 4095, 4096, 4097] {
        let a: Vec<i32> = (0..n).collect();
        let b: Vec<i32> = (0..n).map(|i| i + 1).collect();
        let batches = if n == 0 {
            vec![]
        } else {
            vec![batch(vec![int32_col(a), int32_col(b)])]
        };
        assert_byte_identical(&schema, batches, &TextEncodingOptions::default());
    }
}

#[test]
fn byte_identity_one_million_rows() {
    let schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::required("val", DataType::Int32),
    ])
    .unwrap();
    let mut batches = Vec::new();
    let chunk = 8192;
    let total = 1_000_000i32;
    let mut start = 0i32;
    while start < total {
        let end = (start + chunk).min(total);
        let a: Vec<i32> = (start..end).collect();
        let b: Vec<i32> = (start..end).map(|i| i.wrapping_mul(31)).collect();
        batches.push(batch(vec![int32_col(a), int32_col(b)]));
        start = end;
    }
    // One representative tiny high-water (forces ~hundreds of thousands
    // of windows) plus the production mark; full sweep would be slow.
    let mut ref_op = MockOperator::new(schema.clone(), batches.clone());
    let reference = encode_reference(&mut ref_op, &TextEncodingOptions::default());
    for &hw in &[64usize, STREAM_WINDOW_HIGH_WATER_BYTES] {
        let op = boxed(MockOperator::new(schema.clone(), batches.clone()));
        let windowed = encode_windowed(op, &TextEncodingOptions::default(), hw);
        assert_eq!(
            windowed.as_ref(),
            reference.as_ref(),
            "1M-row byte mismatch at high_water={hw}"
        );
    }
}

// ---------- §7.2 bounded-peak-memory ----------

/// Build N rows worth of 8192-row `(Int32, Int32)` batches.
fn int32_pair_batches(total: i32, chunk: i32) -> Vec<Batch> {
    let mut batches = Vec::new();
    let mut start = 0i32;
    while start < total {
        let end = (start + chunk).min(total);
        let a: Vec<i32> = (start..end).collect();
        let b: Vec<i32> = (start..end).collect();
        batches.push(batch(vec![int32_col(a), int32_col(b)]));
        start = end;
    }
    batches
}

/// Drive the windowed encoder over a reused buffer (as the dispatcher
/// does) and return the peak capacity the window buffer reached.
fn peak_window_capacity(schema: &Schema, batches: Vec<Batch>) -> usize {
    let mut handle = StreamingSelect::for_test(
        boxed(MockOperator::new(schema.clone(), batches)),
        TextEncodingOptions::default(),
    );
    let mut win = BytesMut::new();
    let mut max_cap = 0usize;
    loop {
        win.clear();
        let more = encode_window(&mut handle, &mut win, STREAM_WINDOW_HIGH_WATER_BYTES).unwrap();
        max_cap = max_cap.max(win.capacity());
        if !more {
            break;
        }
    }
    max_cap
}

#[test]
fn bounded_window_buffer_capacity_stays_flat() {
    let schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::required("val", DataType::Int32),
    ])
    .unwrap();
    let chunk = 8192;

    // The headline property: peak buffer capacity does NOT grow as the
    // result cardinality scales 10×/100×. Once the reused buffer can hold
    // one window + one overshooting batch it stops growing, so the peak is
    // identical regardless of how many rows follow — that is the bounded-
    // memory guarantee. (`BytesMut` grows geometrically, so the absolute
    // peak is a fixed multiple of the window size, not an exact sum.)
    let cap_100k = peak_window_capacity(&schema, int32_pair_batches(100_000, chunk));
    let cap_1m = peak_window_capacity(&schema, int32_pair_batches(1_000_000, chunk));
    let cap_10m = peak_window_capacity(&schema, int32_pair_batches(10_000_000, chunk));
    assert_eq!(
        cap_100k, cap_1m,
        "peak window capacity grew between 100k and 1M rows ({cap_100k} -> {cap_1m})"
    );
    assert_eq!(
        cap_1m, cap_10m,
        "peak window capacity grew between 1M and 10M rows ({cap_1m} -> {cap_10m})"
    );

    // And a generous absolute ceiling so a regression that lets the buffer
    // balloon is still caught. `BytesMut` may round the high-water + one
    // batch up to the next growth step, so allow up to 4× the window mark.
    let absolute_ceiling = 4 * STREAM_WINDOW_HIGH_WATER_BYTES;
    assert!(
        cap_1m <= absolute_ceiling,
        "peak window capacity {cap_1m} exceeded the absolute ceiling {absolute_ceiling}"
    );
}

// ---------- §7.4 mid-stream error (encoder level) ----------

#[test]
fn mid_stream_error_propagates_after_partial_windows() {
    let schema = Schema::new([
        Field::required("a", DataType::Int32),
        Field::required("b", DataType::Int32),
    ])
    .unwrap();
    let mut batches = Vec::new();
    for chunk in 0..8 {
        let base = chunk * 50;
        batches.push(batch(vec![
            int32_col((base..base + 50).collect()),
            int32_col((base..base + 50).collect()),
        ]));
    }
    // Error after 4 batches.
    let op = boxed(MockOperator::erroring_after(schema.clone(), batches, 4));
    let mut handle = StreamingSelect::for_test(op, TextEncodingOptions::default());

    let mut flushed_windows = 0usize;
    let mut got_error = false;
    let mut total = BytesMut::new();
    loop {
        let mut win = BytesMut::new();
        match encode_window(&mut handle, &mut win, 64) {
            Ok(more) => {
                total.extend_from_slice(&win);
                flushed_windows += 1;
                if !more {
                    break;
                }
            }
            Err(_) => {
                got_error = true;
                break;
            }
        }
    }
    assert!(got_error, "expected a mid-stream operator error");
    assert!(
        flushed_windows >= 1,
        "expected at least one window flushed before the error"
    );
    // Everything flushed before the error must be whole frames, and
    // crucially must NOT contain a CommandComplete (the stream did not
    // complete normally).
    let mut b = total.clone();
    let mut saw_complete = false;
    while !b.is_empty() {
        match decode_backend(&mut b).expect("decode pre-error frame") {
            Some(BackendMessage::CommandComplete { .. }) => saw_complete = true,
            Some(_) => {}
            None => panic!("pre-error body ended on a partial frame"),
        }
    }
    assert!(
        !saw_complete,
        "a CommandComplete must not be emitted before a mid-stream error"
    );
}

#[test]
fn mid_stream_error_in_window_zero_is_clean() {
    // Error before any batch: window 0 encode fails. The dispatcher maps
    // this to the buffered (today's) error path; at the encoder level we
    // just confirm the error surfaces and no CommandComplete was written.
    let schema = Schema::new([Field::required("a", DataType::Int32)]).unwrap();
    let op = boxed(MockOperator::erroring_after(schema, vec![], 0));
    let mut handle = StreamingSelect::for_test(op, TextEncodingOptions::default());
    let mut win = BytesMut::new();
    let err = encode_window(&mut handle, &mut win, STREAM_WINDOW_HIGH_WATER_BYTES);
    assert!(err.is_err(), "expected window-0 error");
}

// ---------- §7.1 / §7.3 end-to-end over the real wire path ----------

mod e2e {
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};

    use bytes::BytesMut;
    use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
    use ultrasql_protocol::{BackendMessage, FrontendMessage, decode_backend, encode_frontend};

    use crate::result_encoder::STREAM_WINDOW_HIGH_WATER_BYTES;
    use crate::{Server, handle_connection};

    async fn send(io: &mut DuplexStream, msg: &FrontendMessage) {
        let mut buf = BytesMut::new();
        encode_frontend(msg, &mut buf);
        io.write_all(&buf).await.expect("write");
        io.flush().await.expect("flush");
    }

    async fn startup(io: &mut DuplexStream) {
        send(
            io,
            &FrontendMessage::StartupMessage {
                protocol_major: 3,
                protocol_minor: 0,
                params: vec![("user".to_string(), "tester".to_string())],
            },
        )
        .await;
        let _ = read_until_ready(io).await;
    }

    /// Read raw bytes until a `ReadyForQuery` frame has been fully
    /// decoded, returning the complete byte capture *and* the decoded
    /// messages.
    async fn read_until_ready(io: &mut DuplexStream) -> (Vec<u8>, Vec<BackendMessage>) {
        use tokio::io::AsyncReadExt;
        let mut raw = Vec::new();
        let mut decode = BytesMut::new();
        let mut msgs = Vec::new();
        let mut tmp = [0u8; 4096];
        loop {
            // Drain whatever we can decode so far.
            loop {
                let mut probe = decode.clone();
                match decode_backend(&mut probe).expect("decode") {
                    Some(msg) => {
                        let consumed = decode.len() - probe.len();
                        let _ = decode.split_to(consumed);
                        let done = matches!(msg, BackendMessage::ReadyForQuery { .. });
                        msgs.push(msg);
                        if done {
                            return (raw, msgs);
                        }
                    }
                    None => break,
                }
            }
            let n = io.read(&mut tmp).await.expect("read");
            if n == 0 {
                return (raw, msgs);
            }
            raw.extend_from_slice(&tmp[..n]);
            decode.extend_from_slice(&tmp[..n]);
        }
    }

    /// Create a table and bulk-insert `rows` rows of `(id, payload)`
    /// where payload is a 64-char string, so a full-table SELECT body
    /// comfortably exceeds the window high-water mark.
    async fn seed_wide_table(io: &mut DuplexStream, table: &str, rows: usize) {
        send(
            io,
            &FrontendMessage::Query {
                sql: format!("CREATE TABLE {table} (id INT NOT NULL, payload TEXT)"),
            },
        )
        .await;
        let _ = read_until_ready(io).await;

        // Insert in chunks so no single SQL string is gigantic.
        let payload = "x".repeat(64);
        let chunk = 500;
        let mut start = 0usize;
        while start < rows {
            let end = (start + chunk).min(rows);
            let mut values = String::new();
            for i in start..end {
                if i > start {
                    values.push(',');
                }
                values.push_str(&format!("({i}, '{payload}')"));
            }
            send(
                io,
                &FrontendMessage::Query {
                    sql: format!("INSERT INTO {table} (id, payload) VALUES {values}"),
                },
            )
            .await;
            let _ = read_until_ready(io).await;
            start = end;
        }
    }

    fn count_kind(msgs: &[BackendMessage], f: impl Fn(&BackendMessage) -> bool) -> usize {
        msgs.iter().filter(|m| f(m)).count()
    }

    #[tokio::test]
    async fn large_select_streams_well_formed_response() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = Arc::new(Server::with_sample_database());
        let handle = tokio::spawn(handle_connection(server_side, state));

        startup(&mut client).await;
        let rows = 6_000; // ~80 bytes/row wire => well over 256 KiB => streams
        seed_wide_table(&mut client, "stream_t", rows).await;

        send(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT id, payload FROM stream_t".to_string(),
            },
        )
        .await;
        let (raw, msgs) = read_until_ready(&mut client).await;

        // Sanity: the body is large enough to have actually streamed.
        assert!(
            raw.len() > STREAM_WINDOW_HIGH_WATER_BYTES,
            "test result ({} bytes) did not exceed the streaming threshold",
            raw.len()
        );
        // Exactly one RowDescription, one CommandComplete, N DataRows, and
        // a trailing ReadyForQuery 'I' — the streamed windows reassemble
        // into a single well-formed logical response.
        assert_eq!(
            count_kind(&msgs, |m| matches!(
                m,
                BackendMessage::RowDescription { .. }
            )),
            1
        );
        assert_eq!(
            count_kind(&msgs, |m| matches!(m, BackendMessage::DataRow { .. })),
            rows
        );
        let cc = msgs
            .iter()
            .find_map(|m| match m {
                BackendMessage::CommandComplete { tag } => Some(tag.clone()),
                _ => None,
            })
            .expect("CommandComplete");
        assert_eq!(cc, format!("SELECT {rows}"));
        assert!(matches!(
            msgs.last().unwrap(),
            BackendMessage::ReadyForQuery { status: b'I' }
        ));
        // Frame order: RowDescription first, CommandComplete then
        // ReadyForQuery last (no DataRow after CommandComplete).
        let rd_pos = msgs
            .iter()
            .position(|m| matches!(m, BackendMessage::RowDescription { .. }))
            .unwrap();
        let cc_pos = msgs
            .iter()
            .position(|m| matches!(m, BackendMessage::CommandComplete { .. }))
            .unwrap();
        assert_eq!(rd_pos, 0, "RowDescription must be first");
        assert_eq!(
            cc_pos,
            msgs.len() - 2,
            "CommandComplete must precede ReadyForQuery"
        );

        send(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("join").expect("clean exit");
    }

    #[tokio::test]
    async fn streamed_response_round_trips_under_an_explicit_transaction() {
        // Inside BEGIN…the streamed SELECT keeps the block open ('T') and
        // the txn is committed later by COMMIT, not by the drive loop.
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = Arc::new(Server::with_sample_database());
        let handle = tokio::spawn(handle_connection(server_side, state));

        startup(&mut client).await;
        let rows = 6_000;
        seed_wide_table(&mut client, "stream_tx", rows).await;

        send(
            &mut client,
            &FrontendMessage::Query {
                sql: "BEGIN".to_string(),
            },
        )
        .await;
        let _ = read_until_ready(&mut client).await;

        send(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT id, payload FROM stream_tx".to_string(),
            },
        )
        .await;
        let (raw, msgs) = read_until_ready(&mut client).await;
        assert!(raw.len() > STREAM_WINDOW_HIGH_WATER_BYTES);
        assert_eq!(
            count_kind(&msgs, |m| matches!(m, BackendMessage::DataRow { .. })),
            rows
        );
        // Inside an explicit block the trailing status is 'T'.
        assert!(matches!(
            msgs.last().unwrap(),
            BackendMessage::ReadyForQuery { status: b'T' }
        ));

        send(
            &mut client,
            &FrontendMessage::Query {
                sql: "COMMIT".to_string(),
            },
        )
        .await;
        let (_, after) = read_until_ready(&mut client).await;
        assert!(matches!(
            after.last().unwrap(),
            BackendMessage::ReadyForQuery { status: b'I' }
        ));

        send(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("join").expect("clean exit");
    }

    // ---------- §7.3 slow-client backpressure ----------

    /// Shared state for the backpressure "valve".
    struct ValveState {
        is_open: AtomicBool,
        written: AtomicUsize,
        waker: Mutex<Option<Waker>>,
    }

    impl ValveState {
        fn open(&self) {
            self.is_open.store(true, Ordering::Release);
            if let Some(w) = self.waker.lock().unwrap().take() {
                w.wake();
            }
        }
    }

    /// An `AsyncWrite`/`AsyncRead` "valve": writes return `Pending` while
    /// the valve is closed, modelling a client that has stopped reading.
    /// The pending write's `Waker` is stored so the test can re-poll the
    /// server task the instant it opens the valve (no `Notify` race).
    /// Reads delegate to the inner duplex so the frontend still drives the
    /// session; a counter records bytes pushed so the test can assert the
    /// operator was throttled while closed.
    struct Valve {
        inner: DuplexStream,
        state: Arc<ValveState>,
    }

    impl AsyncWrite for Valve {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            if !self.state.is_open.load(Ordering::Acquire) {
                *self.state.waker.lock().unwrap() = Some(cx.waker().clone());
                return Poll::Pending;
            }
            // Drain into the inner duplex so the bytes are consumed.
            match Pin::new(&mut self.inner).poll_write(cx, buf) {
                Poll::Ready(Ok(m)) => {
                    self.state.written.fetch_add(m, Ordering::AcqRel);
                    Poll::Ready(Ok(m))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Pending,
            }
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    impl AsyncRead for Valve {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    #[tokio::test]
    async fn slow_client_throttles_the_operator_pull() {
        // Drive the seed phase over a plain duplex, then hand the session a
        // valve that starts closed. While closed, the server must stall
        // inside the per-window write and therefore cannot push the whole
        // result; once opened, the full result arrives well-formed.
        let (mut client, server_side) = tokio::io::duplex(64 * 1024);
        let state = Arc::new(Server::with_sample_database());

        let valve_state = Arc::new(ValveState {
            is_open: AtomicBool::new(true),
            written: AtomicUsize::new(0),
            waker: Mutex::new(None),
        });
        let valve = Valve {
            inner: server_side,
            state: valve_state.clone(),
        };
        let handle = tokio::spawn(handle_connection(valve, state));

        startup(&mut client).await;
        let rows = 8_000;
        seed_wide_table(&mut client, "stream_slow", rows).await;

        // Close the valve, then issue the large SELECT. The server will
        // encode window 0, attempt to write it, and stall.
        valve_state.is_open.store(false, Ordering::Release);
        let baseline = valve_state.written.load(Ordering::Acquire);
        send(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT id, payload FROM stream_slow".to_string(),
            },
        )
        .await;

        // Give the server task time to run up against the closed valve.
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        let stalled = valve_state.written.load(Ordering::Acquire) - baseline;
        // With the valve closed, the server cannot push *any* of the
        // result body: the per-window `write_all` pends, suspending the
        // drive loop, so the operator is throttled to the client's (zero)
        // drain rate. The whole result is ~640 KiB; the server pushing
        // bounded-or-zero bytes here is the bounded-memory guarantee.
        assert!(
            stalled <= STREAM_WINDOW_HIGH_WATER_BYTES,
            "server pushed {stalled} bytes while the client was blocked; \
             operator was not throttled"
        );

        // Open the valve (wakes the parked write) and drain the rest; the
        // full result must arrive byte-coherent.
        valve_state.open();
        let (_, msgs) = read_until_ready(&mut client).await;
        assert_eq!(
            count_kind(&msgs, |m| matches!(m, BackendMessage::DataRow { .. })),
            rows
        );
        assert!(matches!(
            msgs.last().unwrap(),
            BackendMessage::ReadyForQuery { status: b'I' }
        ));

        send(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("join").expect("clean exit");
    }

    // ---------- consumer-gating regression tests ----------
    //
    // These cover the consumers that receive a `SelectResult` but cannot
    // drive a streaming handle. Before the streaming-gating fix they each
    // requested streaming unconditionally and broke: the batch / embedded
    // paths shipped only window 0 (no CommandComplete) and dropped the
    // streaming handle holding the autocommit txn uncommitted, pinning the
    // XID as in-progress forever. With `allow_streaming` supplied by the
    // dispatch context, these consumers take the whole-buffer path and the
    // bugs cannot occur.

    /// True when no transaction is currently in progress: the oldest
    /// in-progress XID has caught up to the next-to-allocate XID, i.e. the
    /// CLOG holds no `InProgress` entry. A leaked streaming handle would
    /// pin its XID `InProgress`, so this would be false.
    fn no_xid_leaked(state: &Server) -> bool {
        state.txn_manager.oldest_in_progress() == state.txn_manager.next_xid()
    }

    /// BUG 1 — multi-statement Simple-Query batch with a large leading
    /// SELECT. The big SELECT must return ALL its rows with its OWN
    /// CommandComplete, the trailing statement must still run, and the
    /// autocommit XID must not leak.
    ///
    /// Fails before the fix: the batch path (`encode_query_result_body`)
    /// ignored `result.streaming`, so the big SELECT shipped only window 0
    /// with no CommandComplete (the second statement's reply merged into
    /// the corrupt stream) and the dropped handle left the XID InProgress.
    #[tokio::test]
    async fn batch_with_large_leading_select_returns_all_rows_and_leaks_no_xid() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = Arc::new(Server::with_sample_database());
        let handle = tokio::spawn(handle_connection(server_side, Arc::clone(&state)));

        startup(&mut client).await;
        let rows = 6_000; // body well over the 256 KiB window high-water
        seed_wide_table(&mut client, "batch_big", rows).await;

        // One Simple-Query message carrying TWO statements: a large SELECT
        // followed by a trivial one. The whole batch shares a single
        // trailing ReadyForQuery.
        send(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT id, payload FROM batch_big; SELECT 1".to_string(),
            },
        )
        .await;
        let (_, msgs) = read_until_ready(&mut client).await;

        // The big SELECT returns every row, with its OWN CommandComplete.
        assert_eq!(
            count_kind(&msgs, |m| matches!(m, BackendMessage::DataRow { .. })),
            // rows from the big SELECT + 1 row from `SELECT 1`
            rows + 1,
            "batch did not return all rows of the large leading SELECT"
        );
        let tags: Vec<String> = msgs
            .iter()
            .filter_map(|m| match m {
                BackendMessage::CommandComplete { tag } => Some(tag.clone()),
                _ => None,
            })
            .collect();
        assert!(
            tags.iter().any(|t| t == &format!("SELECT {rows}")),
            "missing the large SELECT's CommandComplete; got tags {tags:?}"
        );
        // Both statements completed: two CommandCompletes (SELECT rows, SELECT 1).
        assert_eq!(
            tags,
            vec![format!("SELECT {rows}"), "SELECT 1".to_string()],
            "the trailing statement did not run after the large SELECT"
        );
        // Exactly one trailing ReadyForQuery for the whole batch.
        assert!(matches!(
            msgs.last().unwrap(),
            BackendMessage::ReadyForQuery { status: b'I' }
        ));

        // No leaked XID: every autocommit txn from the batch is terminal.
        assert!(
            no_xid_leaked(&state),
            "batch leaked an in-progress XID (oldest_in_progress={:?}, next_xid={:?})",
            state.txn_manager.oldest_in_progress(),
            state.txn_manager.next_xid()
        );

        send(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("join").expect("clean exit");
    }

    /// §7.5 — the buffered-vs-streaming decision boundary. A result just
    /// under the window high-water (buffered) and one just over it
    /// (streamed) must both return byte-correct, well-formed responses with
    /// the correct row counts.
    #[tokio::test]
    async fn buffered_and_streamed_boundary_both_round_trip() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = Arc::new(Server::with_sample_database());
        let handle = tokio::spawn(handle_connection(server_side, Arc::clone(&state)));

        startup(&mut client).await;

        // ~80 wire bytes/row; pick counts that straddle 256 KiB.
        let per_row = 80usize;
        let under = STREAM_WINDOW_HIGH_WATER_BYTES / per_row / 2; // comfortably buffered
        let over = (STREAM_WINDOW_HIGH_WATER_BYTES / per_row) * 3; // comfortably streamed

        for (table, n) in [("boundary_under", under), ("boundary_over", over)] {
            seed_wide_table(&mut client, table, n).await;
            send(
                &mut client,
                &FrontendMessage::Query {
                    sql: format!("SELECT id, payload FROM {table}"),
                },
            )
            .await;
            let (_, msgs) = read_until_ready(&mut client).await;
            assert_eq!(
                count_kind(&msgs, |m| matches!(m, BackendMessage::DataRow { .. })),
                n,
                "{table}: wrong DataRow count"
            );
            assert_eq!(
                command_tag_of(&msgs).as_deref(),
                Some(format!("SELECT {n}").as_str())
            );
            assert_eq!(
                count_kind(&msgs, |m| matches!(
                    m,
                    BackendMessage::RowDescription { .. }
                )),
                1
            );
            assert!(matches!(
                msgs.last().unwrap(),
                BackendMessage::ReadyForQuery { status: b'I' }
            ));
        }
        assert!(no_xid_leaked(&state), "boundary round-trip leaked an XID");

        send(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("join").expect("clean exit");
    }

    /// Local helper: the `CommandComplete` tag from a decoded message run.
    fn command_tag_of(msgs: &[BackendMessage]) -> Option<String> {
        msgs.iter().find_map(|m| match m {
            BackendMessage::CommandComplete { tag } => Some(tag.clone()),
            _ => None,
        })
    }
}
