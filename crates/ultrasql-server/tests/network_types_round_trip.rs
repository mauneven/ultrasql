//! End-to-end `INET` / `CIDR` / `MACADDR` / `MACADDR8` storage and operators.

pub mod support;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use support::{shutdown, start_sample_server};
use tokio_postgres::SimpleQueryMessage;

fn simple_rows(messages: Vec<SimpleQueryMessage>) -> Vec<Vec<String>> {
    messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|idx| row.get(idx).expect("column").to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

async fn copy_in_payload(client: &tokio_postgres::Client, sql: &str, payload: &[u8]) -> u64 {
    let sink = client
        .copy_in::<_, Bytes>(sql)
        .await
        .expect("copy in starts");
    futures::pin_mut!(sink);
    sink.as_mut()
        .send(Bytes::from(payload.to_vec()))
        .await
        .expect("copy bytes sent");
    sink.finish().await.expect("copy finishes")
}

async fn collect_copy_out(stream: tokio_postgres::CopyOutStream) -> Vec<u8> {
    let mut stream = Box::pin(stream);
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk.expect("copy chunk"));
    }
    out
}

#[tokio::test]
async fn network_types_storage_ops_and_wire_round_trip() {
    let running = start_sample_server("network_types_round_trip").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE network_probe (\
                id INT, \
                host INET, \
                net CIDR, \
                mac MACADDR, \
                mac8 MACADDR8\
            )",
        )
        .await
        .expect("create network table");

    client
        .batch_execute(
            "INSERT INTO network_probe VALUES \
             (1, INET '192.168.1.5/24', CIDR '192.168.1.0/24', \
                 MACADDR '08-00-2B-01-02-03', MACADDR8 '08:00:2b:01:02:03')",
        )
        .await
        .expect("insert network values");

    let values = simple_rows(
        client
            .simple_query("SELECT host, net, mac, mac8 FROM network_probe")
            .await
            .expect("select network values"),
    );
    assert_eq!(
        values,
        vec![vec![
            "192.168.1.5/24".to_owned(),
            "192.168.1.0/24".to_owned(),
            "08:00:2b:01:02:03".to_owned(),
            "08:00:2b:ff:fe:01:02:03".to_owned(),
        ]]
    );

    let ops = simple_rows(
        client
            .simple_query(
                "SELECT \
                    host << INET '192.168.0.0/16', \
                    host <<= net, \
                    net >> INET '192.168.1.10/32', \
                    net >>= host, \
                    net && INET '192.168.1.128/25', \
                    host + 5, \
                    host - 4, \
                    host - INET '192.168.1.1', \
                    mac & MACADDR 'ff:ff:ff:00:00:00', \
                    ~ MACADDR 'ff:ff:ff:00:00:00' \
                 FROM network_probe",
            )
            .await
            .expect("select network ops"),
    );
    assert_eq!(
        ops,
        vec![vec![
            "t".to_owned(),
            "t".to_owned(),
            "t".to_owned(),
            "t".to_owned(),
            "t".to_owned(),
            "192.168.1.10/24".to_owned(),
            "192.168.1.1/24".to_owned(),
            "4".to_owned(),
            "08:00:2b:00:00:00".to_owned(),
            "00:00:00:ff:ff:ff".to_owned(),
        ]]
    );

    let inspectors = simple_rows(
        client
            .simple_query(
                "SELECT host(host), host(net), family(host), masklen(host), masklen(net) \
                 FROM network_probe",
            )
            .await
            .expect("select network inspector functions"),
    );
    assert_eq!(
        inspectors,
        vec![vec![
            "192.168.1.5".to_owned(),
            "192.168.1.0".to_owned(),
            "4".to_owned(),
            "24".to_owned(),
            "24".to_owned(),
        ]]
    );

    let stmt = client
        .prepare("SELECT host, net, mac, mac8 FROM network_probe")
        .await
        .expect("prepare network select");
    let oids: Vec<u32> = stmt
        .columns()
        .iter()
        .map(|column| column.type_().oid())
        .collect();
    assert_eq!(oids, vec![869, 650, 829, 774]);

    let bad_cidr = client
        .batch_execute("INSERT INTO network_probe VALUES (2, INET '10.0.0.1', CIDR '10.0.0.1/24', MACADDR '00:00:00:00:00:00', MACADDR8 '00:00:00:00:00:00:00:00')")
        .await
        .expect_err("CIDR with host bits must fail");
    assert!(bad_cidr.code().is_some());

    client
        .batch_execute("CREATE TABLE network_copy (id INT, host INET, net CIDR, mac MACADDR)")
        .await
        .expect("create network copy table");
    let copied = copy_in_payload(
        client,
        "COPY network_copy FROM STDIN",
        b"1\t10.0.0.9/24\t10.0.0.0/24\t08:00:2b:01:02:03\n",
    )
    .await;
    assert_eq!(copied, 1);
    let out = collect_copy_out(
        client
            .copy_out("COPY network_copy TO STDOUT")
            .await
            .expect("copy network out"),
    )
    .await;
    assert_eq!(out, b"1\t10.0.0.9/24\t10.0.0.0/24\t08:00:2b:01:02:03\n");

    shutdown(running).await;
}
