use std::fmt;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener_with_shutdown};

pub struct RunningServer {
    pub server: Arc<Server>,
    pub client: tokio_postgres::Client,
    pub bound: SocketAddr,
    conn_handle: tokio::task::JoinHandle<()>,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
    shutdown_tx: oneshot::Sender<()>,
}

impl fmt::Debug for RunningServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunningServer")
            .field("bound", &self.bound)
            .finish_non_exhaustive()
    }
}

pub async fn start_persistent_server(data_dir: &Path, application_name: &str) -> RunningServer {
    start_server(
        Arc::new(Server::init(data_dir).expect("persistent server init")),
        application_name,
    )
    .await
}

pub async fn start_sample_server(application_name: &str) -> RunningServer {
    start_server(Arc::new(Server::with_sample_database()), application_name).await
}

pub async fn connect_as(
    bound: SocketAddr,
    user: &str,
    application_name: &str,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let conn_str = format!(
        "host={host} port={port} user={user} application_name={application_name}",
        host = bound.ip(),
        port = bound.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("tokio-postgres connect");
    let handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    (client, handle)
}

pub fn make_data_dir_private(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .expect("chmod temp data dir");
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}

async fn start_server(server: Arc<Server>, application_name: &str) -> RunningServer {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_handle = tokio::spawn(serve_listener_with_shutdown(
        listener,
        Arc::clone(&server),
        async move {
            let _ = shutdown_rx.await;
        },
    ));

    let conn_str = format!(
        "host={host} port={port} user=tester application_name={application_name}",
        host = bound.ip(),
        port = bound.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("tokio-postgres connect");
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    RunningServer {
        server,
        client,
        bound,
        conn_handle,
        server_handle,
        shutdown_tx,
    }
}

pub async fn shutdown(running: RunningServer) {
    let RunningServer {
        server,
        client,
        bound: _,
        conn_handle,
        server_handle,
        shutdown_tx,
    } = running;

    drop(client);
    tokio::time::timeout(Duration::from_secs(2), conn_handle)
        .await
        .expect("connection task exits")
        .expect("connection task joins");
    let _ = shutdown_tx.send(());
    tokio::time::timeout(Duration::from_secs(2), server_handle)
        .await
        .expect("server shutdown completes")
        .expect("server task joins")
        .expect("listener exits cleanly");
    drop(server);
}
