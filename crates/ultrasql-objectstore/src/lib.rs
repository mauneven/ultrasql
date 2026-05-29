//! Synchronous object-storage helpers for SQL file table functions.
//!
//! The public surface is intentionally small: parse object-store URIs,
//! expand single-bucket wildcard patterns, and read full or ranged object bytes. Query
//! planning uses the same helpers as execution so schema inference and row
//! production agree on listing order and error messages.

use std::collections::{HashMap, VecDeque};
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};

use chrono::Utc;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

/// Result type for object-store helpers.
pub type Result<T> = std::result::Result<T, ObjectStoreError>;

/// Object-store URI, listing, or HTTP error.
#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreError {
    /// The URI could not be parsed.
    #[error("{0}")]
    InvalidUri(String),
    /// A required endpoint is missing.
    #[error("{0}")]
    MissingEndpoint(String),
    /// HTTP request failed.
    #[error("{0}")]
    Http(String),
    /// Requested byte range cannot be represented.
    #[error("{0}")]
    InvalidRange(String),
    /// Object listing returned no matches.
    #[error("{0}")]
    NoMatches(String),
}

/// One concrete object selected by literal URI or wildcard expansion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectLocation {
    uri: ObjectUri,
}

impl ObjectLocation {
    /// Return the original SQL-display URI for diagnostics and virtual columns.
    pub fn display_uri(&self) -> String {
        self.uri.display_uri()
    }
}

/// Bytes returned by a ranged object read and optional total object size.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectRangeRead {
    bytes: Vec<u8>,
    object_size: Option<u64>,
}

/// Process-local object range cache counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObjectRangeCacheMetrics {
    /// Bytes fetched from remote object-store range GETs.
    pub remote_bytes: u64,
    /// Remote range GET count after cache misses.
    pub range_requests: u64,
    /// Logical range reads served from cache.
    pub cache_hits: u64,
    /// Logical range reads that missed cache and fetched remote bytes.
    pub cache_misses: u64,
}

impl ObjectRangeRead {
    /// Return bytes fetched for the requested range.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Consume the range response and return its bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Return total object size when the server supplied `Content-Range`.
    pub fn object_size(&self) -> Option<u64> {
        self.object_size
    }
}

/// Return true when `spec` is an object-store URI handled by this crate.
pub fn is_object_store_uri(spec: &str) -> bool {
    ObjectScheme::from_spec(spec).is_some()
}

/// Expand one or more object-store URI specs.
///
/// Literal objects keep argument order. Wildcard patterns are expanded by
/// listing the literal prefix before the first `*` or `?`, filtering with the
/// same wildcard rules as local scans, and sorting matched keys.
pub fn expand_object_store_specs(patterns: &[String]) -> Result<Vec<ObjectLocation>> {
    if patterns.is_empty() {
        return Err(ObjectStoreError::NoMatches(
            "object store path list cannot be empty".to_owned(),
        ));
    }
    let mut objects = Vec::new();
    for pattern in patterns {
        objects.extend(expand_object_store_pattern(pattern)?);
    }
    Ok(objects)
}

/// Read object bytes from a concrete object location.
pub fn read_object_bytes(location: &ObjectLocation) -> Result<Vec<u8>> {
    let request = object_request(&location.uri, Vec::new())?;
    get_response(request, ResponseExpectation::Success).map(ObjectResponse::into_bytes)
}

/// Read a byte range from a concrete object location.
///
/// `start` is zero-based and `len` is the requested byte count. Empty ranges
/// return an empty buffer without issuing an HTTP request.
pub fn read_object_range(location: &ObjectLocation, start: u64, len: u64) -> Result<Vec<u8>> {
    read_object_range_with_metadata(location, start, len).map(ObjectRangeRead::into_bytes)
}

/// Read a byte range and return response metadata needed by columnar readers.
///
/// `object_size` is populated from `Content-Range` when the object store
/// includes the total length, as S3-compatible stores do for byte ranges.
pub fn read_object_range_with_metadata(
    location: &ObjectLocation,
    start: u64,
    len: u64,
) -> Result<ObjectRangeRead> {
    if len == 0 {
        return Ok(ObjectRangeRead {
            bytes: Vec::new(),
            object_size: None,
        });
    }
    let end = start.checked_add(len - 1).ok_or_else(|| {
        ObjectStoreError::InvalidRange(format!(
            "object range overflows u64: start={start} len={len}"
        ))
    })?;
    let mut request = object_request(&location.uri, Vec::new())?;
    request
        .headers
        .push(("range", format!("bytes={start}-{end}")));
    let cache_key = ObjectRangeCacheKey {
        url: request.url.clone(),
        start,
        len,
    };
    if let Some(cached) = object_range_cache_lookup(&cache_key) {
        OBJECT_RANGE_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
        return Ok(cached);
    }
    OBJECT_RANGE_CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
    let response = get_response(request, ResponseExpectation::PartialContent)?;
    OBJECT_RANGE_REQUESTS.fetch_add(1, Ordering::Relaxed);
    OBJECT_RANGE_REMOTE_BYTES.fetch_add(
        u64::try_from(response.bytes.len()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    let range = ObjectRangeRead {
        bytes: response.bytes,
        object_size: response.object_size,
    };
    object_range_cache_insert(cache_key, range.clone());
    Ok(range)
}

/// Return current process-local object range cache counters.
pub fn object_range_cache_metrics() -> ObjectRangeCacheMetrics {
    ObjectRangeCacheMetrics {
        remote_bytes: OBJECT_RANGE_REMOTE_BYTES.load(Ordering::Relaxed),
        range_requests: OBJECT_RANGE_REQUESTS.load(Ordering::Relaxed),
        cache_hits: OBJECT_RANGE_CACHE_HITS.load(Ordering::Relaxed),
        cache_misses: OBJECT_RANGE_CACHE_MISSES.load(Ordering::Relaxed),
    }
}

/// Expand `patterns`, read the first object, and return both location and bytes.
pub fn read_first_object_bytes(patterns: &[String]) -> Result<(ObjectLocation, Vec<u8>)> {
    let objects = expand_object_store_specs(patterns)?;
    let first = objects
        .first()
        .ok_or_else(|| ObjectStoreError::NoMatches("object store path list is empty".to_owned()))?
        .clone();
    let bytes = read_object_bytes(&first)?;
    Ok((first, bytes))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObjectScheme {
    S3,
    R2,
    Gcs,
}

impl ObjectScheme {
    fn from_spec(spec: &str) -> Option<Self> {
        let (scheme, _) = spec.split_once("://")?;
        match scheme.to_ascii_lowercase().as_str() {
            "s3" => Some(Self::S3),
            "r2" => Some(Self::R2),
            "gs" | "gcs" => Some(Self::Gcs),
            _ => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::R2 => "r2",
            Self::Gcs => "gs",
        }
    }

    const fn service(self) -> &'static str {
        match self {
            Self::S3 | Self::R2 | Self::Gcs => "s3",
        }
    }

    fn endpoint(self) -> Result<Option<String>> {
        match self {
            Self::S3 => Ok(s3_endpoint_override().or_else(|| {
                first_env(&[
                    "ULTRASQL_S3_ENDPOINT",
                    "AWS_ENDPOINT_URL_S3",
                    "AWS_ENDPOINT_URL",
                ])
            })),
            Self::R2 => first_env(&["ULTRASQL_R2_ENDPOINT", "AWS_ENDPOINT_URL_S3"])
                .map(Some)
                .ok_or_else(|| {
                    ObjectStoreError::MissingEndpoint(
                        "r2:// requires ULTRASQL_R2_ENDPOINT".to_owned(),
                    )
                }),
            Self::Gcs => Ok(first_env(&["ULTRASQL_GCS_ENDPOINT"])
                .or_else(|| Some("https://storage.googleapis.com".to_owned()))),
        }
    }
}

static S3_ENDPOINT_OVERRIDE: OnceLock<Mutex<Option<String>>> = OnceLock::new();
static S3_ENDPOINT_OVERRIDE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// Guard for a process-local S3 endpoint override.
///
/// This is intended for in-process tests and benchmark smoke drivers that need
/// object-store SQL paths to talk to a local mock endpoint without mutating the
/// process environment. Dropping the guard restores the previous override.
#[derive(Debug)]
pub struct S3EndpointOverrideGuard {
    _lock: MutexGuard<'static, ()>,
    previous: Option<String>,
}

impl Drop for S3EndpointOverrideGuard {
    fn drop(&mut self) {
        replace_s3_endpoint_override(self.previous.take());
    }
}

/// Overrides the S3 endpoint for this process until the returned guard drops.
#[must_use]
pub fn override_s3_endpoint_for_process(endpoint: impl Into<String>) -> S3EndpointOverrideGuard {
    let lock = lock_unpoisoned(S3_ENDPOINT_OVERRIDE_LOCK.get_or_init(|| Mutex::new(())));
    S3EndpointOverrideGuard {
        _lock: lock,
        previous: replace_s3_endpoint_override(Some(endpoint.into())),
    }
}

fn s3_endpoint_override() -> Option<String> {
    lock_unpoisoned(S3_ENDPOINT_OVERRIDE.get_or_init(|| Mutex::new(None))).clone()
}

fn replace_s3_endpoint_override(value: Option<String>) -> Option<String> {
    let mut guard = lock_unpoisoned(S3_ENDPOINT_OVERRIDE.get_or_init(|| Mutex::new(None)));
    std::mem::replace(&mut *guard, value)
}

fn lock_unpoisoned<T>(mutex: &'static Mutex<T>) -> MutexGuard<'static, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObjectUri {
    scheme: ObjectScheme,
    bucket: String,
    key: String,
}

impl ObjectUri {
    fn parse(spec: &str) -> Result<Self> {
        let scheme = ObjectScheme::from_spec(spec).ok_or_else(|| {
            ObjectStoreError::InvalidUri(format!("object store URI scheme unsupported: {spec}"))
        })?;
        let rest = spec
            .split_once("://")
            .map(|(_, rest)| rest)
            .ok_or_else(|| {
                ObjectStoreError::InvalidUri(format!("object store URI missing scheme: {spec}"))
            })?;
        let (bucket, key) = rest.split_once('/').ok_or_else(|| {
            ObjectStoreError::InvalidUri(format!(
                "object store URI must include bucket and key: {spec}"
            ))
        })?;
        if bucket.is_empty() || key.is_empty() {
            return Err(ObjectStoreError::InvalidUri(format!(
                "object store URI must include bucket and key: {spec}"
            )));
        }
        Ok(Self {
            scheme,
            bucket: bucket.to_owned(),
            key: key.to_owned(),
        })
    }

    fn display_uri(&self) -> String {
        format!("{}://{}/{}", self.scheme.as_str(), self.bucket, self.key)
    }
}

#[derive(Clone, Debug)]
struct ObjectRequest {
    scheme: ObjectScheme,
    method: &'static str,
    url: String,
    host: String,
    canonical_uri: String,
    canonical_query: String,
    headers: Vec<(&'static str, String)>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResponseExpectation {
    Success,
    PartialContent,
}

#[derive(Debug)]
struct ObjectResponse {
    bytes: Vec<u8>,
    object_size: Option<u64>,
}

impl ObjectResponse {
    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

#[derive(Clone, Debug)]
struct Credentials {
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
    region: String,
}

fn expand_object_store_pattern(pattern: &str) -> Result<Vec<ObjectLocation>> {
    let uri = ObjectUri::parse(pattern)?;
    if !contains_wildcard(&uri.key) {
        return Ok(vec![ObjectLocation { uri }]);
    }

    let prefix = literal_prefix(&uri.key);
    let mut query = vec![
        ("list-type".to_owned(), "2".to_owned()),
        ("prefix".to_owned(), prefix),
    ];
    let mut matches = Vec::new();
    loop {
        let request = object_request(&uri.with_key(""), query.clone())?;
        let body =
            String::from_utf8(get_response(request, ResponseExpectation::Success)?.into_bytes())
                .map_err(|err| {
                    ObjectStoreError::Http(format!("object listing returned invalid UTF-8: {err}"))
                })?;
        for key in parse_xml_tag_values(&body, "Key") {
            if wildcard_match(&uri.key, &key) {
                matches.push(ObjectLocation {
                    uri: ObjectUri {
                        scheme: uri.scheme,
                        bucket: uri.bucket.clone(),
                        key,
                    },
                });
            }
        }
        if !xml_tag_is_true(&body, "IsTruncated") {
            break;
        }
        let Some(token) = parse_xml_tag_values(&body, "NextContinuationToken").pop() else {
            return Err(ObjectStoreError::Http(
                "object listing was truncated without continuation token".to_owned(),
            ));
        };
        query.retain(|(key, _)| key != "continuation-token");
        query.push(("continuation-token".to_owned(), token));
    }

    matches.sort_by_key(ObjectLocation::display_uri);
    if matches.is_empty() {
        return Err(ObjectStoreError::NoMatches(format!(
            "object store pattern matched no objects: {pattern}"
        )));
    }
    Ok(matches)
}

impl ObjectUri {
    fn with_key(&self, key: &str) -> Self {
        Self {
            scheme: self.scheme,
            bucket: self.bucket.clone(),
            key: key.to_owned(),
        }
    }
}

fn object_request(uri: &ObjectUri, query: Vec<(String, String)>) -> Result<ObjectRequest> {
    let endpoint = uri.scheme.endpoint()?;
    let query = canonical_query_string(query);
    let (url, canonical_uri) = if let Some(endpoint) = endpoint {
        let endpoint = endpoint.trim_end_matches('/');
        let path = if uri.key.is_empty() {
            format!("/{}", percent_encode_path(&uri.bucket))
        } else {
            format!(
                "/{}/{}",
                percent_encode_path(&uri.bucket),
                percent_encode_path(&uri.key)
            )
        };
        let url = append_query(format!("{endpoint}{path}"), &query);
        (url, path)
    } else {
        let path = if uri.key.is_empty() {
            "/".to_owned()
        } else {
            format!("/{}", percent_encode_path(&uri.key))
        };
        let base = format!("https://{}.s3.amazonaws.com", uri.bucket);
        let url = append_query(format!("{base}{path}"), &query);
        (url, path)
    };
    let host = host_from_url(&url)?;
    Ok(ObjectRequest {
        scheme: uri.scheme,
        method: "GET",
        url,
        host,
        canonical_uri,
        canonical_query: query,
        headers: Vec::new(),
    })
}

fn get_response(
    request: ObjectRequest,
    expectation: ResponseExpectation,
) -> Result<ObjectResponse> {
    let mut builder = ureq::get(&request.url);
    if let Some(credentials) = credentials_for(request.scheme) {
        for (name, value) in signed_headers(&request, &credentials) {
            builder = builder.header(name, value);
        }
    } else {
        for (name, value) in &request.headers {
            builder = builder.header(*name, value.clone());
        }
    }
    let mut response = builder
        .call()
        .map_err(|err| ObjectStoreError::Http(format!("object GET {}: {err}", request.url)))?;
    let status = response.status();
    match expectation {
        ResponseExpectation::Success if !status.is_success() => {
            return Err(ObjectStoreError::Http(format!(
                "object GET {} returned {status}",
                request.url
            )));
        }
        ResponseExpectation::PartialContent if status.as_u16() != 206 => {
            return Err(ObjectStoreError::Http(format!(
                "object range GET {} returned {status}, expected 206 Partial Content",
                request.url
            )));
        }
        ResponseExpectation::Success | ResponseExpectation::PartialContent => {}
    }
    let object_size = response
        .headers()
        .get("content-range")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_content_range_size);
    let full_read_limit =
        (expectation == ResponseExpectation::Success).then(object_full_read_limit_bytes);
    if let Some(limit) = full_read_limit
        && let Some(content_length) = response
            .headers()
            .get("content-length")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
        && content_length > limit
    {
        return Err(ObjectStoreError::Http(format!(
            "object GET {} body exceeds limit: content-length={content_length} limit={limit}",
            request.url
        )));
    }
    let body_read_limit = full_read_limit.map_or(u64::MAX, |limit| limit.saturating_add(1));
    let bytes = response
        .body_mut()
        .with_config()
        .limit(body_read_limit)
        .read_to_vec()
        .map_err(|err| ObjectStoreError::Http(format!("object GET {} body: {err}", request.url)))?;
    if let Some(limit) = full_read_limit {
        let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if len > limit {
            return Err(ObjectStoreError::Http(format!(
                "object GET {} body exceeds limit: bytes={len} limit={limit}",
                request.url
            )));
        }
    }
    Ok(ObjectResponse { bytes, object_size })
}

const OBJECT_RANGE_CACHE_MAX_ENTRIES: usize = 1024;
const OBJECT_RANGE_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_OBJECT_FULL_READ_LIMIT_BYTES: u64 = 128 * 1024 * 1024;

fn object_full_read_limit_bytes() -> u64 {
    first_env(&["ULTRASQL_OBJECT_FULL_READ_LIMIT_BYTES"])
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_OBJECT_FULL_READ_LIMIT_BYTES)
}

static OBJECT_RANGE_REMOTE_BYTES: AtomicU64 = AtomicU64::new(0);
static OBJECT_RANGE_REQUESTS: AtomicU64 = AtomicU64::new(0);
static OBJECT_RANGE_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
static OBJECT_RANGE_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);
static OBJECT_RANGE_CACHE: OnceLock<Mutex<ObjectRangeCache>> = OnceLock::new();

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ObjectRangeCacheKey {
    url: String,
    start: u64,
    len: u64,
}

#[derive(Debug, Default)]
struct ObjectRangeCache {
    entries: HashMap<ObjectRangeCacheKey, ObjectRangeRead>,
    order: VecDeque<ObjectRangeCacheKey>,
    bytes: u64,
}

impl ObjectRangeCache {
    fn lookup(&self, key: &ObjectRangeCacheKey) -> Option<ObjectRangeRead> {
        self.entries.get(key).cloned()
    }

    fn insert(&mut self, key: ObjectRangeCacheKey, value: ObjectRangeRead) {
        if self.entries.contains_key(&key) {
            return;
        }
        let entry_bytes = u64::try_from(value.bytes.len()).unwrap_or(u64::MAX);
        if entry_bytes > OBJECT_RANGE_CACHE_MAX_BYTES {
            return;
        }
        self.bytes = self.bytes.saturating_add(entry_bytes);
        self.order.push_back(key.clone());
        self.entries.insert(key, value);
        self.evict_over_budget();
    }

    fn evict_over_budget(&mut self) {
        while (self.entries.len() > OBJECT_RANGE_CACHE_MAX_ENTRIES
            || self.bytes > OBJECT_RANGE_CACHE_MAX_BYTES)
            && !self.order.is_empty()
        {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(value) = self.entries.remove(&oldest) {
                self.bytes = self
                    .bytes
                    .saturating_sub(u64::try_from(value.bytes.len()).unwrap_or(u64::MAX));
            }
        }
    }

    #[cfg(test)]
    fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
        self.bytes = 0;
    }
}

fn object_range_cache() -> &'static Mutex<ObjectRangeCache> {
    OBJECT_RANGE_CACHE.get_or_init(|| Mutex::new(ObjectRangeCache::default()))
}

fn object_range_cache_lookup(key: &ObjectRangeCacheKey) -> Option<ObjectRangeRead> {
    object_range_cache().lock().ok()?.lookup(key)
}

fn object_range_cache_insert(key: ObjectRangeCacheKey, value: ObjectRangeRead) {
    if let Ok(mut cache) = object_range_cache().lock() {
        cache.insert(key, value);
    }
}

#[cfg(test)]
fn reset_object_range_cache_for_tests() {
    if let Ok(mut cache) = object_range_cache().lock() {
        cache.clear();
    }
    OBJECT_RANGE_REMOTE_BYTES.store(0, Ordering::Relaxed);
    OBJECT_RANGE_REQUESTS.store(0, Ordering::Relaxed);
    OBJECT_RANGE_CACHE_HITS.store(0, Ordering::Relaxed);
    OBJECT_RANGE_CACHE_MISSES.store(0, Ordering::Relaxed);
}

fn host_from_url(url: &str) -> Result<String> {
    let (_, rest) = url
        .split_once("://")
        .ok_or_else(|| ObjectStoreError::InvalidUri(format!("object URL missing scheme: {url}")))?;
    let host = rest
        .split(['/', '?'])
        .next()
        .filter(|host| !host.is_empty())
        .ok_or_else(|| ObjectStoreError::InvalidUri(format!("object URL missing host: {url}")))?;
    Ok(host.to_owned())
}

fn credentials_for(scheme: ObjectScheme) -> Option<Credentials> {
    match scheme {
        ObjectScheme::S3 | ObjectScheme::Gcs => credentials_from_env(
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            default_region("us-east-1"),
        ),
        ObjectScheme::R2 => credentials_from_env(
            "R2_ACCESS_KEY_ID",
            "R2_SECRET_ACCESS_KEY",
            "R2_SESSION_TOKEN",
            default_region("auto"),
        )
        .or_else(|| {
            credentials_from_env(
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
                default_region("auto"),
            )
        }),
    }
}

fn credentials_from_env(
    access_key: &str,
    secret_key: &str,
    session_token: &str,
    region: String,
) -> Option<Credentials> {
    Some(Credentials {
        access_key: env::var(access_key).ok()?,
        secret_key: env::var(secret_key).ok()?,
        session_token: env::var(session_token).ok(),
        region,
    })
}

fn default_region(fallback: &str) -> String {
    first_env(&["AWS_REGION", "AWS_DEFAULT_REGION"]).unwrap_or_else(|| fallback.to_owned())
}

fn signed_headers(
    request: &ObjectRequest,
    credentials: &Credentials,
) -> Vec<(&'static str, String)> {
    let now = Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let payload_hash = "UNSIGNED-PAYLOAD";

    let mut canonical_headers = vec![
        ("host", request.host.clone()),
        ("x-amz-content-sha256", payload_hash.to_owned()),
        ("x-amz-date", amz_date.clone()),
    ];
    canonical_headers.extend(request.headers.iter().cloned());
    if let Some(token) = &credentials.session_token {
        canonical_headers.push(("x-amz-security-token", token.clone()));
    }
    canonical_headers.sort_by_key(|(name, _)| *name);
    let signed_header_names = canonical_headers
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(";");
    let canonical_header_text = canonical_headers
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect::<String>();
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        request.method,
        request.canonical_uri,
        request.canonical_query,
        canonical_header_text,
        signed_header_names,
        payload_hash
    );
    let scope = format!(
        "{}/{}/{}/aws4_request",
        date,
        credentials.region,
        request.scheme.service()
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date,
        scope,
        sha256_hex(canonical_request.as_bytes())
    );
    let signing_key = signing_key(
        &credentials.secret_key,
        &date,
        &credentials.region,
        request.scheme.service(),
    );
    let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        credentials.access_key, scope, signed_header_names, signature
    );

    let mut headers = request.headers.clone();
    headers.extend([
        ("host", request.host.clone()),
        ("x-amz-content-sha256", payload_hash.to_owned()),
        ("x-amz-date", amz_date),
        ("authorization", authorization),
    ]);
    if let Some(token) = &credentials.session_token {
        headers.push(("x-amz-security-token", token.clone()));
    }
    headers
}

fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

fn hmac_sha256(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key)
        .expect("HMAC-SHA256 accepts signing keys of any length");
    mac.update(msg);
    mac.finalize().into_bytes().to_vec()
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn first_env(names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| env::var(name).ok())
}

fn append_query(mut url: String, query: &str) -> String {
    if !query.is_empty() {
        url.push('?');
        url.push_str(query);
    }
    url
}

fn canonical_query_string(mut query: Vec<(String, String)>) -> String {
    query.sort();
    query
        .into_iter()
        .map(|(key, value)| format!("{}={}", percent_encode(&key), percent_encode(&value)))
        .collect::<Vec<_>>()
        .join("&")
}

fn parse_content_range_size(value: &str) -> Option<u64> {
    let (_, size) = value.rsplit_once('/')?;
    if size == "*" {
        return None;
    }
    size.parse().ok()
}

fn literal_prefix(pattern: &str) -> String {
    let first_wildcard = pattern
        .char_indices()
        .find_map(|(idx, ch)| matches!(ch, '*' | '?').then_some(idx))
        .unwrap_or(pattern.len());
    pattern[..first_wildcard].to_owned()
}

fn contains_wildcard(s: &str) -> bool {
    s.chars().any(|ch| matches!(ch, '*' | '?'))
}

fn wildcard_match(pattern: &str, text: &str) -> bool {
    let pattern = pattern.chars().collect::<Vec<_>>();
    let text = text.chars().collect::<Vec<_>>();
    let mut dp = vec![vec![false; text.len() + 1]; pattern.len() + 1];
    dp[0][0] = true;
    for (i, ch) in pattern.iter().enumerate() {
        if *ch == '*' {
            dp[i + 1][0] = dp[i][0];
        }
    }
    for (i, pattern_ch) in pattern.iter().enumerate() {
        for (j, text_ch) in text.iter().enumerate() {
            dp[i + 1][j + 1] = match pattern_ch {
                '*' => dp[i][j + 1] || dp[i + 1][j],
                '?' => dp[i][j],
                ch => dp[i][j] && ch == text_ch,
            };
        }
    }
    dp[pattern.len()][text.len()]
}

fn percent_encode_path(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| {
            if byte == b'/' {
                "/".to_owned()
            } else {
                percent_encode_byte(byte)
            }
            .chars()
            .collect::<Vec<_>>()
        })
        .collect()
}

fn percent_encode(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| percent_encode_byte(byte).chars().collect::<Vec<_>>())
        .collect()
}

fn percent_encode_byte(byte: u8) -> String {
    if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
        char::from(byte).to_string()
    } else {
        format!("%{byte:02X}")
    }
}

fn parse_xml_tag_values(input: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut rest = input;
    let mut values = Vec::new();
    while let Some(start) = rest.find(&open) {
        let value_start = start + open.len();
        let Some(end) = rest[value_start..].find(&close) else {
            break;
        };
        values.push(xml_decode(&rest[value_start..value_start + end]));
        rest = &rest[value_start + end + close.len()..];
    }
    values
}

fn xml_tag_is_true(input: &str, tag: &str) -> bool {
    parse_xml_tag_values(input, tag)
        .first()
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

fn xml_decode(input: &str) -> String {
    input
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Mutex, MutexGuard, OnceLock as TestOnceLock, mpsc};
    use std::thread;

    #[test]
    fn wildcard_match_supports_file_patterns() {
        assert!(wildcard_match("logs/*.csv", "logs/a.csv"));
        assert!(wildcard_match("logs/part-?.csv", "logs/part-a.csv"));
        assert!(!wildcard_match("logs/part-?.csv", "logs/part-ab.csv"));
    }

    #[test]
    fn parses_s3_uri() {
        let uri = ObjectUri::parse("s3://bucket/path/file.csv").expect("parse uri");
        assert_eq!(uri.bucket, "bucket");
        assert_eq!(uri.key, "path/file.csv");
        assert_eq!(uri.display_uri(), "s3://bucket/path/file.csv");
    }

    #[test]
    fn rejects_empty_and_invalid_specs() {
        let empty: Vec<String> = Vec::new();
        let err = expand_object_store_specs(&empty).expect_err("empty patterns rejected");
        assert!(err.to_string().contains("path list cannot be empty"));

        let err = ObjectUri::parse("file:///tmp/a.csv").expect_err("unsupported scheme");
        assert!(err.to_string().contains("scheme unsupported"));

        let err = ObjectUri::parse("s3://bucket").expect_err("missing key rejected");
        assert!(err.to_string().contains("must include bucket and key"));
    }

    #[test]
    fn xml_key_parser_decodes_entities() {
        let xml =
            "<ListBucketResult><Contents><Key>a&amp;b.csv</Key></Contents></ListBucketResult>";
        assert_eq!(parse_xml_tag_values(xml, "Key"), vec!["a&b.csv"]);
    }

    #[test]
    fn object_request_uses_endpoint_path_style_and_sorted_query() {
        let _test_guard = objectstore_env_test_lock();
        let _guard = override_s3_endpoint_for_process("http://127.0.0.1:9000/");
        let uri = ObjectUri::parse("s3://my-bucket/path with space/file.csv").expect("uri");

        let request = object_request(
            &uri,
            vec![
                ("prefix".to_owned(), "logs/a b/".to_owned()),
                ("list-type".to_owned(), "2".to_owned()),
            ],
        )
        .expect("object request");

        assert_eq!(request.host, "127.0.0.1:9000");
        assert_eq!(
            request.url,
            "http://127.0.0.1:9000/my-bucket/path%20with%20space/file.csv?list-type=2&prefix=logs%2Fa%20b%2F"
        );
        assert_eq!(
            request.canonical_uri,
            "/my-bucket/path%20with%20space/file.csv"
        );
        assert_eq!(
            request.canonical_query,
            "list-type=2&prefix=logs%2Fa%20b%2F"
        );
    }

    #[test]
    fn s3_endpoint_override_recovers_from_poisoned_guard_lock() {
        let _test_guard = objectstore_env_test_lock();

        let _ = std::panic::catch_unwind(|| {
            let _guard = override_s3_endpoint_for_process("http://127.0.0.1:9001/");
            panic!("poison endpoint override lock");
        });

        let _guard = override_s3_endpoint_for_process("http://127.0.0.1:9002/");
        assert_eq!(
            s3_endpoint_override().as_deref(),
            Some("http://127.0.0.1:9002/")
        );
    }

    #[test]
    fn zero_length_range_returns_empty_without_http() {
        let location = ObjectLocation {
            uri: ObjectUri::parse("s3://bucket/path/file.parquet").expect("uri"),
        };

        let range = read_object_range_with_metadata(&location, 42, 0).expect("empty range");

        assert!(range.bytes().is_empty());
        assert_eq!(range.object_size(), None);
    }

    #[test]
    fn range_overflow_is_rejected_before_http() {
        let location = ObjectLocation {
            uri: ObjectUri::parse("s3://bucket/path/file.parquet").expect("uri"),
        };

        let err =
            read_object_range_with_metadata(&location, u64::MAX, 2).expect_err("range overflow");

        assert!(err.to_string().contains("object range overflows u64"));
    }

    #[test]
    fn signed_headers_include_session_token_and_range() {
        let request = ObjectRequest {
            scheme: ObjectScheme::R2,
            method: "GET",
            url: "https://example.invalid/bucket/object".to_owned(),
            host: "example.invalid".to_owned(),
            canonical_uri: "/bucket/object".to_owned(),
            canonical_query: "list-type=2".to_owned(),
            headers: vec![("range", "bytes=4-9".to_owned())],
        };
        let credentials = Credentials {
            access_key: "access".to_owned(),
            secret_key: "secret".to_owned(),
            session_token: Some("token".to_owned()),
            region: "auto".to_owned(),
        };

        let headers = signed_headers(&request, &credentials);

        assert!(headers.iter().any(|(name, value)| {
            *name == "authorization"
                && value.contains("Credential=access/")
                && value.contains("/auto/s3/aws4_request")
                && value.contains(
                    "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date;x-amz-security-token",
                )
        }));
        assert!(
            headers
                .iter()
                .any(|(name, value)| *name == "x-amz-security-token" && value == "token")
        );
        assert!(
            headers
                .iter()
                .any(|(name, value)| *name == "range" && value == "bytes=4-9")
        );
    }

    #[test]
    fn wildcard_listing_follows_continuation_and_sorts_matches() {
        let _test_guard = objectstore_env_test_lock();
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let (request_tx, request_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let first_body = concat!(
                "<ListBucketResult>",
                "<IsTruncated>true</IsTruncated>",
                "<NextContinuationToken>token-1</NextContinuationToken>",
                "<Contents><Key>logs/b.csv</Key></Contents>",
                "<Contents><Key>logs/ignore.txt</Key></Contents>",
                "</ListBucketResult>"
            );
            let second_body = concat!(
                "<ListBucketResult>",
                "<IsTruncated>false</IsTruncated>",
                "<Contents><Key>logs/a.csv</Key></Contents>",
                "</ListBucketResult>"
            );
            for body in [first_body, second_body] {
                let (mut stream, _) = listener.accept().expect("accept request");
                let mut request = Vec::new();
                let mut buf = [0_u8; 1024];
                loop {
                    let read = stream.read(&mut buf).expect("read request");
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buf[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                request_tx
                    .send(String::from_utf8(request).expect("request utf8"))
                    .expect("send request");
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                )
                .expect("write response");
            }
        });

        let _guard = override_s3_endpoint_for_process(endpoint);
        let objects = expand_object_store_specs(&["s3://bucket/logs/*.csv".to_owned()])
            .expect("expand wildcard");

        let uris = objects
            .iter()
            .map(ObjectLocation::display_uri)
            .collect::<Vec<_>>();
        assert_eq!(
            uris,
            vec!["s3://bucket/logs/a.csv", "s3://bucket/logs/b.csv"]
        );
        let first_request = request_rx.recv().expect("first request");
        assert!(first_request.starts_with("GET /bucket?list-type=2&prefix=logs%2F"));
        let second_request = request_rx.recv().expect("second request");
        assert!(second_request.contains("continuation-token=token-1"));
        handle.join().expect("mock server done");
    }

    #[test]
    fn read_first_object_bytes_reads_literal_object() {
        let _test_guard = objectstore_env_test_lock();
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let (request_tx, request_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept request");
            let mut request = Vec::new();
            let mut buf = [0_u8; 1024];
            loop {
                let read = stream.read(&mut buf).expect("read request");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            request_tx
                .send(String::from_utf8(request).expect("request utf8"))
                .expect("send request");
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc")
                .expect("write response");
        });

        let _guard = override_s3_endpoint_for_process(endpoint);
        let (location, bytes) = read_first_object_bytes(&["s3://bucket/path/file.csv".to_owned()])
            .expect("read first object");

        assert_eq!(location.display_uri(), "s3://bucket/path/file.csv");
        assert_eq!(bytes, b"abc");
        let request = request_rx.recv().expect("request text");
        assert!(request.starts_with("GET /bucket/path/file.csv HTTP/1.1"));
        handle.join().expect("mock server done");
    }

    #[test]
    fn read_first_object_bytes_rejects_configured_oversized_body() {
        let _test_guard = objectstore_env_test_lock();
        // SAFETY: objectstore_env_test_lock serializes process-env mutation in
        // this crate's tests.
        unsafe {
            std::env::set_var("ULTRASQL_OBJECT_FULL_READ_LIMIT_BYTES", "3");
        }
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept request");
            let mut request = Vec::new();
            let mut buf = [0_u8; 1024];
            loop {
                let read = stream.read(&mut buf).expect("read request");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nabcd")
                .expect("write response");
        });

        let _guard = override_s3_endpoint_for_process(endpoint);
        let err = read_first_object_bytes(&["s3://bucket/path/file.csv".to_owned()])
            .expect_err("oversized object rejected");

        assert!(err.to_string().contains("body exceeds limit"));
        handle.join().expect("mock server done");
        // SAFETY: objectstore_env_test_lock serializes process-env mutation in
        // this crate's tests.
        unsafe {
            std::env::remove_var("ULTRASQL_OBJECT_FULL_READ_LIMIT_BYTES");
        }
    }

    #[test]
    fn read_object_range_requests_byte_slice() {
        let _test_guard = objectstore_env_test_lock();
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let (request_tx, request_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept request");
            let mut request = Vec::new();
            let mut buf = [0_u8; 1024];
            loop {
                let read = stream.read(&mut buf).expect("read request");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            request_tx
                .send(String::from_utf8(request).expect("request utf8"))
                .expect("send request");
            stream
                .write_all(
                    b"HTTP/1.1 206 Partial Content\r\nContent-Length: 6\r\nContent-Range: bytes 4-9/16\r\n\r\n456789",
                )
                .expect("write response");
        });

        let _guard = override_s3_endpoint_for_process(endpoint);
        let objects = expand_object_store_specs(&["s3://bucket/path/file.parquet".to_owned()])
            .expect("expand object");
        let range = read_object_range_with_metadata(&objects[0], 4, 6).expect("read range");

        assert_eq!(range.bytes(), b"456789");
        assert_eq!(range.object_size(), Some(16));
        let request = request_rx.recv().expect("request text");
        assert!(request.starts_with("GET /bucket/path/file.parquet HTTP/1.1"));
        assert!(request.contains("\r\nrange: bytes=4-9\r\n"));
        handle.join().expect("mock server done");
    }

    #[test]
    fn read_object_range_cache_reuses_identical_ranges() {
        let _test_guard = objectstore_env_test_lock();
        reset_object_range_cache_for_tests();
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let (request_tx, request_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept request");
            let mut request = Vec::new();
            let mut buf = [0_u8; 1024];
            loop {
                let read = stream.read(&mut buf).expect("read request");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            request_tx
                .send(String::from_utf8(request).expect("request utf8"))
                .expect("send request");
            stream
                .write_all(
                    b"HTTP/1.1 206 Partial Content\r\nContent-Length: 6\r\nContent-Range: bytes 4-9/16\r\n\r\n456789",
                )
                .expect("write response");
        });

        let _guard = override_s3_endpoint_for_process(endpoint);
        let objects = expand_object_store_specs(&["s3://bucket/path/file.parquet".to_owned()])
            .expect("expand object");
        let first = read_object_range_with_metadata(&objects[0], 4, 6).expect("first range");
        let second = read_object_range_with_metadata(&objects[0], 4, 6).expect("cached range");

        assert_eq!(first.bytes(), b"456789");
        assert_eq!(second.bytes(), b"456789");
        let request = request_rx.recv().expect("request text");
        assert!(request.contains("\r\nrange: bytes=4-9\r\n"));
        assert!(
            request_rx.try_recv().is_err(),
            "cache should avoid second HTTP GET"
        );
        let metrics = object_range_cache_metrics();
        assert_eq!(metrics.range_requests, 1);
        assert_eq!(metrics.remote_bytes, 6);
        assert_eq!(metrics.cache_misses, 1);
        assert_eq!(metrics.cache_hits, 1);
        handle.join().expect("mock server done");
        reset_object_range_cache_for_tests();
    }

    fn objectstore_env_test_lock() -> MutexGuard<'static, ()> {
        static LOCK: TestOnceLock<Mutex<()>> = TestOnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .expect("objectstore env test lock")
    }
}
