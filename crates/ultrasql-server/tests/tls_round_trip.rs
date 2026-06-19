//! End-to-end test: a client `SSLRequest` upgrades the connection to TLS and
//! the PostgreSQL handshake then runs over the encrypted stream.

use std::net::SocketAddr;
use std::sync::Arc;

use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, ServerName};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_postgres::NoTls;
use ultrasql_server::tls::{TlsConfig, TlsHandshake};
use ultrasql_server::{Server, bind_listener, serve_listener_with_shutdown};

/// PostgreSQL `SSLRequest`: int32 length = 8, int32 code = 80877103.
const SSL_REQUEST_CODE: i32 = 80_877_103;

#[tokio::test]
async fn ssl_request_upgrades_to_tls_then_runs_query() {
    // A process-wide crypto provider is required by the rustls builders.
    let _ = rustls::crypto::ring::default_provider().install_default();

    // 1. Self-signed certificate for "localhost".
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).expect("generate cert");
    let cert_der: CertificateDer<'static> = cert.der().clone();
    let dir = tempfile::TempDir::new().expect("temp dir");
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, cert.pem()).expect("write cert");
    std::fs::write(&key_path, signing_key.serialize_pem()).expect("write key");

    // 2. A TLS-enabled server (Trust auth — this test exercises the transport).
    let server_config = TlsHandshake::build_server_config(&TlsConfig {
        cert_file: cert_path,
        key_file: key_path,
        ca_file: None,
    })
    .expect("build server TLS config");
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database().with_tls(server_config));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_handle = tokio::spawn(serve_listener_with_shutdown(listener, server, async move {
        let _ = shutdown_rx.await;
    }));

    // 3. Raw TCP: send SSLRequest, expect the server to accept with 'S'.
    let mut tcp = TcpStream::connect(bound).await.expect("tcp connect");
    tcp.write_all(&8_i32.to_be_bytes())
        .await
        .expect("write length");
    tcp.write_all(&SSL_REQUEST_CODE.to_be_bytes())
        .await
        .expect("write ssl code");
    tcp.flush().await.expect("flush");
    let mut resp = [0_u8; 1];
    tcp.read_exact(&mut resp).await.expect("read ssl response");
    assert_eq!(resp[0], b'S', "server must accept the TLS upgrade");

    // 4. Complete the TLS handshake, trusting the self-signed certificate.
    let mut roots = RootCertStore::empty();
    roots.add(cert_der).expect("add root cert");
    let client_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
    let server_name = ServerName::try_from("localhost").expect("server name");
    let tls = connector
        .connect(server_name, tcp)
        .await
        .expect("tls handshake");

    // 5. Run the PostgreSQL startup over the encrypted stream. `connect_raw`
    //    with `NoTls` drives the handshake over the already-established TLS
    //    stream (no second SSLRequest).
    let (client, connection) = tokio_postgres::Config::new()
        .user("tester")
        .connect_raw(tls, NoTls)
        .await
        .expect("postgres startup over TLS");
    let conn_handle = tokio::spawn(async move {
        let _ = connection.await;
    });

    let rows = client.query("SELECT 1", &[]).await.expect("query over TLS");
    assert_eq!(rows[0].get::<_, i32>(0), 1);

    drop(client);
    let _ = conn_handle.await;
    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), server_handle).await;
}

/// Plaintext pipelined after the SSLRequest (before the TLS handshake) must not
/// be admitted into the encrypted session — the server rejects the connection
/// (cf. CVE-2021-23214).
#[tokio::test]
async fn plaintext_after_ssl_request_is_rejected() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).expect("generate cert");
    let dir = tempfile::TempDir::new().expect("temp dir");
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, cert.pem()).expect("write cert");
    std::fs::write(&key_path, signing_key.serialize_pem()).expect("write key");
    let server_config = TlsHandshake::build_server_config(&TlsConfig {
        cert_file: cert_path,
        key_file: key_path,
        ca_file: None,
    })
    .expect("build server TLS config");
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database().with_tls(server_config));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_handle = tokio::spawn(serve_listener_with_shutdown(listener, server, async move {
        let _ = shutdown_rx.await;
    }));

    // SSLRequest immediately followed by plaintext, in a single write so the
    // bytes arrive together and the server sees buffered data after the
    // SSLRequest.
    let mut tcp = TcpStream::connect(bound).await.expect("tcp connect");
    let mut packet = Vec::new();
    packet.extend_from_slice(&8_i32.to_be_bytes());
    packet.extend_from_slice(&SSL_REQUEST_CODE.to_be_bytes());
    packet.extend_from_slice(b"injected plaintext payload");
    tcp.write_all(&packet).await.expect("write");
    tcp.flush().await.expect("flush");

    let mut resp = [0_u8; 16];
    let n = tokio::time::timeout(std::time::Duration::from_secs(2), tcp.read(&mut resp))
        .await
        .expect("read does not hang")
        .unwrap_or(0);
    assert!(
        n == 0 || resp[0] != b'S',
        "pipelined plaintext after SSLRequest must not be admitted into a TLS session"
    );

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), server_handle).await;
}

/// When no certificate is configured the server declines TLS with 'N' and the
/// client continues in plaintext.
#[tokio::test]
async fn ssl_request_declined_without_cert() {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_handle = tokio::spawn(serve_listener_with_shutdown(listener, server, async move {
        let _ = shutdown_rx.await;
    }));

    let mut tcp = TcpStream::connect(bound).await.expect("tcp connect");
    tcp.write_all(&8_i32.to_be_bytes())
        .await
        .expect("write length");
    tcp.write_all(&SSL_REQUEST_CODE.to_be_bytes())
        .await
        .expect("write ssl code");
    tcp.flush().await.expect("flush");
    let mut resp = [0_u8; 1];
    tcp.read_exact(&mut resp).await.expect("read ssl response");
    assert_eq!(resp[0], b'N', "server with no cert must decline TLS");

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), server_handle).await;
}
