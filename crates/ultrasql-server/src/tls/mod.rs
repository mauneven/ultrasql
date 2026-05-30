//! TLS upgrade for PostgreSQL wire-protocol connections.
//!
//! PostgreSQL's startup sequence allows a client to negotiate TLS by
//! sending an [`SSL_REQUEST_MAGIC`] sentinel before the regular startup
//! message. The server replies with `'S'` to accept or `'N'` to decline,
//! then (on accept) performs a TLS handshake over the same TCP stream.
//!
//! This module provides:
//!
//! - [`TlsConfig`] — paths to the PEM certificate and PKCS#8 private key
//!   files that the server presents to clients.
//! - [`TlsHandshake`] — an async helper that loads a [`rustls::ServerConfig`]
//!   from a [`TlsConfig`] and upgrades any `AsyncRead + AsyncWrite` stream
//!   to a TLS stream via [`tokio_rustls`].
//! - [`TlsError`] — the error type for TLS operations.
//!
//! # `SSLRequest` handling
//!
//! The PostgreSQL wire protocol sends the `SSLRequest` as a 4-byte length
//! field (`0x00_00_00_08`) followed by the 4-byte magic `0x04_D2_16_2F`
//! (decimal `80877103`). The connection handler should detect this before
//! normal startup and delegate to [`TlsHandshake`].
//!
//! # Key loading
//!
//! Certificate files must be PEM-formatted. The private key must be
//! PKCS#8-encoded (the format produced by `openssl genpkey` and by
//! [`rcgen` crate](https://docs.rs/rcgen)). PKCS#1 RSA keys (the traditional `-----BEGIN RSA PRIVATE KEY-----`
//! format) are not supported by this loader; convert with
//! `openssl pkcs8 -topk8 -nocrypt`.
//!
//! # `mTLS` / `ca_file`
//!
//! The `ca_file` field of [`TlsConfig`] is reserved for future client-
//! certificate verification. It is accepted by the parser but is not
//! wired into the [`rustls::ServerConfig`] yet.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::server::TlsStream;

/// The 4-byte magic number that PostgreSQL clients send to request TLS.
///
/// Value: `0x04_D2_16_2F` = decimal `80877103`.
pub const SSL_REQUEST_MAGIC: u32 = 80_877_103;

// ── Error ─────────────────────────────────────────────────────────────────────

/// Errors produced during TLS configuration loading or handshake.
#[derive(Debug, Error)]
pub enum TlsError {
    /// An I/O error while reading the certificate or key file.
    #[error("TLS I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// No valid certificate was found in the certificate PEM file.
    #[error("no certificate found in {path:?}")]
    NoCertificate {
        /// Path to the certificate file.
        path: PathBuf,
    },

    /// No valid PKCS#8 private key was found in the key PEM file.
    #[error("no PKCS#8 private key found in {path:?}")]
    NoPrivateKey {
        /// Path to the key file.
        path: PathBuf,
    },

    /// The rustls configuration was rejected (e.g. certificate/key mismatch).
    #[error("rustls config error: {0}")]
    Rustls(#[from] rustls::Error),

    /// A PEM file failed to parse.
    #[error("PEM parse error: {0}")]
    Pem(#[from] rustls::pki_types::pem::Error),
}

// ── TlsConfig ─────────────────────────────────────────────────────────────────

/// Paths to PEM files needed to set up a TLS listener.
///
/// # Field semantics
///
/// - `cert_file`: path to the PEM file containing the server certificate
///   (and optionally the intermediate chain).
/// - `key_file`: path to the PEM file containing the PKCS#8 private key.
/// - `ca_file`: optional path for future mTLS client-certificate
///   verification (not yet wired).
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to the server's PEM-encoded certificate file.
    pub cert_file: PathBuf,
    /// Path to the server's PKCS#8 PEM-encoded private key file.
    pub key_file: PathBuf,
    /// Optional path to the CA certificate for client verification (mTLS).
    pub ca_file: Option<PathBuf>,
}

// ── TlsHandshake ──────────────────────────────────────────────────────────────

/// Utilities for loading a [`rustls::ServerConfig`] and upgrading streams.
///
/// This type has no instance state; all methods are inherent.
#[derive(Debug)]
pub struct TlsHandshake;

impl TlsHandshake {
    /// Build a [`rustls::ServerConfig`] from the given [`TlsConfig`].
    ///
    /// Reads the certificate and private key files from disk, parses them
    /// with the `PemObject` API from `rustls-pki-types`, and returns a
    /// configured [`rustls::ServerConfig`] ready for use with
    /// [`TlsHandshake::upgrade`].
    ///
    /// # Errors
    ///
    /// - [`TlsError::Io`] if a file cannot be read.
    /// - [`TlsError::Pem`] if a PEM file is malformed.
    /// - [`TlsError::NoCertificate`] if the cert file contains no valid cert.
    /// - [`TlsError::NoPrivateKey`] if the key file contains no valid
    ///   PKCS#8 key.
    /// - [`TlsError::Rustls`] if rustls rejects the configuration.
    pub fn build_server_config(cfg: &TlsConfig) -> Result<Arc<rustls::ServerConfig>, TlsError> {
        // Load certificates.
        let cert_pem = read_regular_pem_file(&cfg.cert_file)?;
        let certs: Vec<CertificateDer<'static>> =
            CertificateDer::pem_slice_iter(&cert_pem).collect::<Result<Vec<_>, _>>()?;
        if certs.is_empty() {
            return Err(TlsError::NoCertificate {
                path: cfg.cert_file.clone(),
            });
        }

        // Load private key (PKCS#8 only).
        let key_pem = read_regular_pem_file(&cfg.key_file)?;
        let private_key = match PrivatePkcs8KeyDer::from_pem_slice(&key_pem) {
            Ok(k) => PrivateKeyDer::Pkcs8(k),
            Err(rustls::pki_types::pem::Error::NoItemsFound) => {
                return Err(TlsError::NoPrivateKey {
                    path: cfg.key_file.clone(),
                });
            }
            Err(e) => return Err(TlsError::Pem(e)),
        };

        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, private_key)?;

        Ok(Arc::new(server_config))
    }

    /// Upgrade a raw async stream to a TLS stream using the provided
    /// [`rustls::ServerConfig`].
    ///
    /// This performs the full TLS handshake. The returned
    /// [`TlsStream`] can be used as any `AsyncRead + AsyncWrite` stream.
    ///
    /// # Errors
    ///
    /// Returns [`TlsError::Io`] if the handshake fails.
    pub async fn upgrade<S>(
        stream: S,
        config: Arc<rustls::ServerConfig>,
    ) -> Result<TlsStream<S>, TlsError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let acceptor = tokio_rustls::TlsAcceptor::from(config);
        let tls_stream = acceptor.accept(stream).await?;
        Ok(tls_stream)
    }
}

const DEFAULT_TLS_PEM_LIMIT_BYTES: u64 = 8 * 1024 * 1024;

fn read_regular_pem_file(path: &Path) -> Result<Vec<u8>, TlsError> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(TlsError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("TLS PEM path {} is not a regular file", path.display()),
        )));
    }
    let limit = tls_pem_limit_bytes();
    if metadata.len() > limit {
        return Err(TlsError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "TLS PEM file exceeds limit: {} size={} limit={limit}",
                path.display(),
                metadata.len()
            ),
        )));
    }
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path).map_err(|err| {
        #[cfg(unix)]
        if err.raw_os_error() == Some(libc::ELOOP) {
            return TlsError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("TLS PEM path {} is not a regular file", path.display()),
            ));
        }
        TlsError::Io(err)
    })?;
    let mut bytes = Vec::new();
    let mut limited = std::io::Read::take(file, limit.saturating_add(1));
    std::io::Read::read_to_end(&mut limited, &mut bytes)?;
    let read_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if read_len > limit {
        return Err(TlsError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "TLS PEM file exceeds limit: {} size={read_len} limit={limit}",
                path.display()
            ),
        )));
    }
    Ok(bytes)
}

fn tls_pem_limit_bytes() -> u64 {
    std::env::var("ULTRASQL_TLS_PEM_LIMIT_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_TLS_PEM_LIMIT_BYTES)
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Generate a self-signed certificate and private key using `rcgen`,
    /// return `(cert_pem, key_pem)` as byte vectors.
    fn generate_self_signed() -> (Vec<u8>, Vec<u8>) {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
                .expect("generate self-signed cert");
        let cert_pem = cert.pem().into_bytes();
        let key_pem = signing_key.serialize_pem().into_bytes();
        (cert_pem, key_pem)
    }

    /// Build a [`rustls::ServerConfig`] entirely in memory (no files) for
    /// testing purposes.
    fn build_server_config_from_bytes(
        cert_pem: &[u8],
        key_pem: &[u8],
    ) -> Arc<rustls::ServerConfig> {
        let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(cert_pem)
            .collect::<Result<Vec<_>, _>>()
            .expect("parse certs");
        let private_key =
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from_pem_slice(key_pem).expect("parse key"));
        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, private_key)
            .expect("build config");
        Arc::new(config)
    }

    /// Build a rustls client config that accepts any server certificate
    /// (for testing only).
    fn build_dangerous_client_config() -> Arc<rustls::ClientConfig> {
        use rustls::client::danger::{
            HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
        };
        use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
        use rustls::{DigitallySignedStruct, Error, SignatureScheme};

        #[derive(Debug)]
        struct NoVerifier;

        impl ServerCertVerifier for NoVerifier {
            fn verify_server_cert(
                &self,
                _end_entity: &CertificateDer<'_>,
                _intermediates: &[CertificateDer<'_>],
                _server_name: &ServerName<'_>,
                _ocsp: &[u8],
                _now: UnixTime,
            ) -> Result<ServerCertVerified, Error> {
                Ok(ServerCertVerified::assertion())
            }

            fn verify_tls12_signature(
                &self,
                _message: &[u8],
                _cert: &CertificateDer<'_>,
                _dss: &DigitallySignedStruct,
            ) -> Result<HandshakeSignatureValid, Error> {
                Ok(HandshakeSignatureValid::assertion())
            }

            fn verify_tls13_signature(
                &self,
                _message: &[u8],
                _cert: &CertificateDer<'_>,
                _dss: &DigitallySignedStruct,
            ) -> Result<HandshakeSignatureValid, Error> {
                Ok(HandshakeSignatureValid::assertion())
            }

            fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
                rustls::crypto::ring::default_provider()
                    .signature_verification_algorithms
                    .supported_schemes()
            }
        }

        let config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifier))
            .with_no_client_auth();
        Arc::new(config)
    }

    #[tokio::test]
    async fn tls_handshake_round_trip_with_self_signed_cert() {
        let (cert_pem, key_pem) = generate_self_signed();
        let server_config = build_server_config_from_bytes(&cert_pem, &key_pem);

        // Create an in-memory duplex stream.
        let (client_io, server_io) = tokio::io::duplex(65536);

        // Drive the server-side TLS upgrade.
        let server_handle = tokio::spawn(async move {
            let mut tls = TlsHandshake::upgrade(server_io, server_config)
                .await
                .expect("server TLS upgrade");
            // Echo one byte back.
            let mut buf = [0u8; 1];
            tls.read_exact(&mut buf).await.expect("server read");
            tls.write_all(&buf).await.expect("server write");
            tls.flush().await.expect("flush");
        });

        // Drive the client-side TLS handshake.
        let client_config = build_dangerous_client_config();
        let connector = tokio_rustls::TlsConnector::from(client_config);
        let server_name = rustls::pki_types::ServerName::try_from("localhost")
            .expect("server name")
            .to_owned();
        let mut tls_client = connector
            .connect(server_name, client_io)
            .await
            .expect("client TLS connect");

        // Send one byte, expect echo.
        tls_client.write_all(&[0x42]).await.expect("write");
        tls_client.flush().await.expect("flush");
        let mut reply = [0u8; 1];
        tls_client.read_exact(&mut reply).await.expect("read");
        assert_eq!(reply[0], 0x42);

        server_handle.await.expect("server task");
    }

    #[test]
    fn ssl_request_magic_constant_matches_postgres_spec() {
        // PostgreSQL wire protocol: SSLRequest magic = 80877103.
        assert_eq!(SSL_REQUEST_MAGIC, 80_877_103u32);
        // The same value in hex is 0x04D2162F.
        assert_eq!(SSL_REQUEST_MAGIC, 0x04_D2_16_2F);
    }

    #[test]
    fn build_server_config_rejects_missing_cert_file() {
        let cfg = TlsConfig {
            cert_file: PathBuf::from("/nonexistent/cert.pem"),
            key_file: PathBuf::from("/nonexistent/key.pem"),
            ca_file: None,
        };
        let err = TlsHandshake::build_server_config(&cfg).expect_err("should fail");
        assert!(matches!(err, TlsError::Io(_)));
    }

    #[test]
    fn build_server_config_rejects_empty_cert_file() {
        use std::io::Write;
        // Write a key-only PEM file (no cert).
        let (_cert_pem, key_pem) = generate_self_signed();

        // Write to temp files.
        let mut cert_file = tempfile::NamedTempFile::new().expect("tempfile");
        let mut key_file = tempfile::NamedTempFile::new().expect("tempfile");

        // Write no certs (empty file).
        cert_file.write_all(b"").expect("write cert");
        key_file.write_all(&key_pem).expect("write key");

        let cfg = TlsConfig {
            cert_file: cert_file.path().to_owned(),
            key_file: key_file.path().to_owned(),
            ca_file: None,
        };
        let err = TlsHandshake::build_server_config(&cfg).expect_err("should fail");
        assert!(matches!(err, TlsError::NoCertificate { .. }));
    }

    #[test]
    fn build_server_config_from_valid_files() {
        use std::io::Write;
        let (cert_pem, key_pem) = generate_self_signed();

        let mut cert_file = tempfile::NamedTempFile::new().expect("tempfile");
        let mut key_file = tempfile::NamedTempFile::new().expect("tempfile");
        cert_file.write_all(&cert_pem).expect("write cert");
        key_file.write_all(&key_pem).expect("write key");

        let cfg = TlsConfig {
            cert_file: cert_file.path().to_owned(),
            key_file: key_file.path().to_owned(),
            ca_file: None,
        };
        let _config = TlsHandshake::build_server_config(&cfg).expect("build ok");
    }

    #[cfg(unix)]
    #[test]
    fn build_server_config_rejects_symlinked_pem_files() {
        let _env_guard = tls_env_test_lock();
        // SAFETY: tls_env_test_lock serializes process-env mutation in this
        // module's tests.
        unsafe {
            std::env::remove_var("ULTRASQL_TLS_PEM_LIMIT_BYTES");
        }

        use std::io::Write;
        use std::os::unix::fs::symlink;

        let (cert_pem, key_pem) = generate_self_signed();
        let dir = tempfile::tempdir().expect("tempdir");
        let real_cert = dir.path().join("real-cert.pem");
        let real_key = dir.path().join("real-key.pem");
        let cert_link = dir.path().join("cert-link.pem");
        let key_link = dir.path().join("key-link.pem");
        std::fs::File::create(&real_cert)
            .expect("cert file")
            .write_all(&cert_pem)
            .expect("write cert");
        std::fs::File::create(&real_key)
            .expect("key file")
            .write_all(&key_pem)
            .expect("write key");
        symlink(&real_cert, &cert_link).expect("cert symlink");
        symlink(&real_key, &key_link).expect("key symlink");

        let cert_cfg = TlsConfig {
            cert_file: cert_link,
            key_file: real_key.clone(),
            ca_file: None,
        };
        let Err(cert_err) = TlsHandshake::build_server_config(&cert_cfg) else {
            panic!("symlinked cert should be rejected");
        };
        assert!(cert_err.to_string().contains("regular file"));

        let key_cfg = TlsConfig {
            cert_file: real_cert,
            key_file: key_link,
            ca_file: None,
        };
        let Err(key_err) = TlsHandshake::build_server_config(&key_cfg) else {
            panic!("symlinked key should be rejected");
        };
        assert!(key_err.to_string().contains("regular file"));
    }

    #[test]
    fn read_regular_pem_file_rejects_configured_oversized_file() {
        let _env_guard = tls_env_test_lock();
        // SAFETY: tls_env_test_lock serializes process-env mutation in this
        // module's tests.
        unsafe {
            std::env::set_var("ULTRASQL_TLS_PEM_LIMIT_BYTES", "3");
        }
        let pem = tempfile::NamedTempFile::new().expect("pem file");
        std::fs::write(pem.path(), b"abcd").expect("write pem");

        let err = read_regular_pem_file(pem.path()).expect_err("oversized PEM rejected");

        assert!(err.to_string().contains("TLS PEM file exceeds limit"));
        // SAFETY: tls_env_test_lock serializes process-env mutation in this
        // module's tests.
        unsafe {
            std::env::remove_var("ULTRASQL_TLS_PEM_LIMIT_BYTES");
        }
    }

    fn tls_env_test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .expect("tls env test lock")
    }
}
