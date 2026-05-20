//! Synchronous object-storage helpers for SQL file table functions.
//!
//! The public surface is intentionally small: parse object-store URIs,
//! expand single-bucket wildcard patterns, and read full or ranged object bytes. Query
//! planning uses the same helpers as execution so schema inference and row
//! production agree on listing order and error messages.

use std::env;

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
    let response = get_response(request, ResponseExpectation::PartialContent)?;
    Ok(ObjectRangeRead {
        bytes: response.bytes,
        object_size: response.object_size,
    })
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
            Self::S3 => Ok(first_env(&[
                "ULTRASQL_S3_ENDPOINT",
                "AWS_ENDPOINT_URL_S3",
                "AWS_ENDPOINT_URL",
            ])),
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
    let bytes = response
        .body_mut()
        .with_config()
        .limit(u64::MAX)
        .read_to_vec()
        .map_err(|err| ObjectStoreError::Http(format!("object GET {} body: {err}", request.url)))?;
    Ok(ObjectResponse { bytes, object_size })
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
    use std::sync::mpsc;
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
    fn xml_key_parser_decodes_entities() {
        let xml =
            "<ListBucketResult><Contents><Key>a&amp;b.csv</Key></Contents></ListBucketResult>";
        assert_eq!(parse_xml_tag_values(xml, "Key"), vec!["a&b.csv"]);
    }

    #[test]
    fn read_object_range_requests_byte_slice() {
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

        let _guard = EnvVarGuard::set("ULTRASQL_S3_ENDPOINT", endpoint);
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

    struct EnvVarGuard {
        name: &'static str,
        old: Option<String>,
    }

    impl EnvVarGuard {
        fn set(name: &'static str, value: String) -> Self {
            let old = env::var(name).ok();
            // SAFETY: this test mutates a single process environment key before
            // starting the client request and restores it before returning.
            unsafe {
                env::set_var(name, value);
            }
            Self { name, old }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            // SAFETY: restores the environment key owned by this guard.
            unsafe {
                if let Some(value) = &self.old {
                    env::set_var(self.name, value);
                } else {
                    env::remove_var(self.name);
                }
            }
        }
    }
}
