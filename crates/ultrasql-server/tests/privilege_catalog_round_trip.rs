//! Wire-level coverage for privilege DDL.

pub mod support;

use std::net::SocketAddr;

use support::{shutdown, start_persistent_server, start_sample_server};
use tokio_postgres::{NoTls, error::SqlState};
use ultrasql_server::Server;

#[path = "privilege_catalog_round_trip/column_and_role_acl.rs"]
mod column_and_role_acl;
#[path = "privilege_catalog_round_trip/default_and_persistence.rs"]
mod default_and_persistence;
#[path = "privilege_catalog_round_trip/grant_revoke_acl.rs"]
mod grant_revoke_acl;
#[path = "privilege_catalog_round_trip/grant_validation.rs"]
mod grant_validation;
#[path = "privilege_catalog_round_trip/metadata_rebuild.rs"]
mod metadata_rebuild;

async fn connect_as(
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

fn assert_insufficient_privilege(err: tokio_postgres::Error) {
    let db = err.as_db_error().expect("database error");
    assert_eq!(
        db.code(),
        &SqlState::INSUFFICIENT_PRIVILEGE,
        "{}",
        db.message()
    );
}
