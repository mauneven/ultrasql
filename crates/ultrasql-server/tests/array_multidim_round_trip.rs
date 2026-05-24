//! End-to-end multi-dimensional array storage, coercion, and wire metadata.

mod support;

use support::{shutdown, start_sample_server};
use tokio_postgres::SimpleQueryMessage;

fn simple_rows(messages: Vec<SimpleQueryMessage>) -> Vec<Vec<Option<String>>> {
    messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|idx| row.get(idx).map(str::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn multidimensional_arrays_store_dimensions_and_wire_oid() {
    let running = start_sample_server("array_multidim_round_trip").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE array_probe (id INT, matrix INT[][])")
        .await
        .expect("create multidimensional array table");

    client
        .batch_execute("INSERT INTO array_probe VALUES (1, [[1, 2], [3, 4]])")
        .await
        .expect("insert multidimensional array");
    client
        .batch_execute("INSERT INTO array_probe VALUES (2, [[1::smallint, 2::bigint], [3, 4]])")
        .await
        .expect("insert multidimensional array with numeric coercions");

    let rows = simple_rows(
        client
            .simple_query(
                "SELECT matrix, \
                        array_length(matrix, 1), \
                        array_length(matrix, 2), \
                        array_length(matrix, 3) \
                 FROM array_probe WHERE id = 1",
            )
            .await
            .expect("select multidimensional array"),
    );
    assert_eq!(
        rows,
        vec![vec![
            Some("{{1,2},{3,4}}".to_owned()),
            Some("2".to_owned()),
            Some("2".to_owned()),
            None,
        ]]
    );

    let coerced = simple_rows(
        client
            .simple_query("SELECT matrix FROM array_probe WHERE id = 2")
            .await
            .expect("select coerced multidimensional array"),
    );
    assert_eq!(coerced, vec![vec![Some("{{1,2},{3,4}}".to_owned())]]);

    let flattened = simple_rows(
        client
            .simple_query("SELECT array_to_string(matrix, ':') FROM array_probe WHERE id = 1")
            .await
            .expect("array_to_string multidimensional array"),
    );
    assert_eq!(flattened, vec![vec![Some("1:2:3:4".to_owned())]]);

    let unnested = simple_rows(
        client
            .simple_query("SELECT * FROM unnest([[1, 2], [3, 4]])")
            .await
            .expect("unnest multidimensional array"),
    );
    assert_eq!(
        unnested,
        vec![
            vec![Some("1".to_owned())],
            vec![Some("2".to_owned())],
            vec![Some("3".to_owned())],
            vec![Some("4".to_owned())],
        ]
    );

    let stmt = client
        .prepare("SELECT matrix FROM array_probe WHERE id = 1")
        .await
        .expect("prepare matrix select");
    assert_eq!(stmt.columns()[0].type_().oid(), 1007);

    shutdown(running).await;
}
