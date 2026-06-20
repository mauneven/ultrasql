use super::*;

#[test]
fn privilege_metadata_rejects_duplicate_grant_keys_on_rebuild() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    support::make_data_dir_private(data_dir.path());
    std::fs::write(
        data_dir.path().join("pg_privileges.meta"),
        concat!(
            "# ultrasql privilege runtime v1\n",
            "grant\ttable\tdup_acl\tpublic\tselect\t\tultrasql\tfalse\n",
            "grant\ttable\tdup_acl\tpublic\tselect\t\tultrasql\ttrue\n"
        ),
    )
    .expect("write duplicate privilege metadata");

    let err = Server::init(data_dir.path()).expect_err("duplicate privilege metadata rejected");
    assert!(
        err.to_string().contains("duplicate privilege metadata"),
        "expected duplicate privilege metadata rejection, got {err}"
    );
}

#[test]
fn privilege_metadata_rejects_duplicate_default_grant_keys_on_rebuild() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    support::make_data_dir_private(data_dir.path());
    std::fs::write(
        data_dir.path().join("pg_privileges.meta"),
        concat!(
            "# ultrasql privilege runtime v1\n",
            "default\tultrasql\t\ttable\tpublic\tselect\tultrasql\tfalse\n",
            "default\tultrasql\t\ttable\tpublic\tselect\tultrasql\ttrue\n"
        ),
    )
    .expect("write duplicate default privilege metadata");

    let err =
        Server::init(data_dir.path()).expect_err("duplicate default privilege metadata rejected");
    assert!(
        err.to_string()
            .contains("duplicate default privilege metadata"),
        "expected duplicate default privilege metadata rejection, got {err}"
    );
}

#[test]
fn privilege_metadata_rejects_unknown_role_refs_on_rebuild() {
    let cases = [
        (
            "grant grantee",
            "grant\ttable\tacl_target\tmissing_role\tselect\t\tultrasql\tfalse\n",
            "missing_role",
        ),
        (
            "grant grantor",
            "grant\ttable\tacl_target\tpublic\tselect\t\tmissing_role\tfalse\n",
            "missing_role",
        ),
        (
            "default owner",
            "default\tmissing_role\t\ttable\tpublic\tselect\tultrasql\tfalse\n",
            "missing_role",
        ),
        (
            "default grantee",
            "default\tultrasql\t\ttable\tmissing_role\tselect\tultrasql\tfalse\n",
            "missing_role",
        ),
        (
            "default grantor",
            "default\tultrasql\t\ttable\tpublic\tselect\tmissing_role\tfalse\n",
            "missing_role",
        ),
    ];

    for (case, row, role) in cases {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        support::make_data_dir_private(data_dir.path());
        std::fs::write(
            data_dir.path().join("pg_privileges.meta"),
            format!("# ultrasql privilege runtime v1\n{row}"),
        )
        .expect("write privilege metadata with unknown role");

        let err = match Server::init(data_dir.path()) {
            Ok(_) => panic!("{case} should reject unknown role refs"),
            Err(err) => err,
        };
        let message = err.to_string();
        assert!(
            message.contains("unknown privilege metadata role")
                && message.contains(role)
                && message.contains("line 2"),
            "{case} expected unknown role rejection for {role}, got {message}"
        );
    }
}

#[test]
fn privilege_metadata_rejects_unknown_column_refs_on_rebuild() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    support::make_data_dir_private(data_dir.path());
    std::fs::write(
        data_dir.path().join("pg_privileges.meta"),
        concat!(
            "# ultrasql privilege runtime v1\n",
            "grant\ttable\tusers\tpublic\tupdate\tmissing_column\tultrasql\tfalse\n"
        ),
    )
    .expect("write privilege metadata with unknown column");

    let err = match Server::init(data_dir.path()) {
        Ok(_) => panic!("unknown column privilege metadata should be rejected"),
        Err(err) => err,
    };
    let message = err.to_string();
    assert!(
        message.contains("unknown privilege metadata column")
            && message.contains("missing_column")
            && message.contains("line 2"),
        "expected unknown column rejection, got {message}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_table_removes_table_privilege_grants() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    support::make_data_dir_private(data_dir.path());
    let metadata_path = data_dir.path().join("pg_privileges.meta");

    let running = start_persistent_server(data_dir.path(), "privilege_drop_table").await;
    let client = &running.client;
    client
        .batch_execute("CREATE ROLE tester SUPERUSER LOGIN")
        .await
        .expect("register admin role");
    client
        .batch_execute("CREATE ROLE analyst LOGIN")
        .await
        .expect("create analyst role");
    client
        .batch_execute("CREATE TABLE privilege_drop (id INT, secret TEXT)")
        .await
        .expect("create privilege table");
    client
        .batch_execute("GRANT SELECT ON TABLE privilege_drop TO analyst")
        .await
        .expect("grant table select");
    client
        .batch_execute("GRANT UPDATE(id) ON TABLE privilege_drop TO analyst")
        .await
        .expect("grant column update");
    let before_drop = std::fs::read_to_string(&metadata_path).expect("privilege metadata exists");
    assert!(
        before_drop.contains("privilege_drop"),
        "privilege metadata should record grants before drop: {before_drop}"
    );

    client
        .batch_execute("DROP TABLE privilege_drop")
        .await
        .expect("drop privilege table");
    let stale = client
        .query_one(
            "SELECT \
                has_table_privilege('analyst', 'privilege_drop', 'SELECT'), \
                has_column_privilege('analyst', 'privilege_drop', 'id', 'UPDATE')",
            &[],
        )
        .await
        .expect("privilege checks after drop");
    assert!(
        !stale.get::<_, bool>(0),
        "dropped table must clear object-level grants"
    );
    assert!(
        !stale.get::<_, bool>(1),
        "dropped table must clear column-level grants"
    );
    shutdown(running).await;

    let after_drop = std::fs::read_to_string(&metadata_path).expect("privilege metadata exists");
    assert!(
        !after_drop.contains("privilege_drop"),
        "dropped table grants must be removed from privilege metadata: {after_drop}"
    );

    let running = start_persistent_server(data_dir.path(), "privilege_drop_table_recreate").await;
    running
        .client
        .batch_execute("CREATE TABLE privilege_drop (id INT, secret TEXT)")
        .await
        .expect("recreate privilege table");
    let recreated = running
        .client
        .query_one(
            "SELECT \
                has_table_privilege('analyst', 'privilege_drop', 'SELECT'), \
                has_column_privilege('analyst', 'privilege_drop', 'id', 'UPDATE')",
            &[],
        )
        .await
        .expect("privilege checks after recreate");
    assert!(
        !recreated.get::<_, bool>(0),
        "recreated table must not inherit stale object grant"
    );
    assert!(
        !recreated.get::<_, bool>(1),
        "recreated table must not inherit stale column grant"
    );

    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_sequence_removes_sequence_privilege_grants() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    support::make_data_dir_private(data_dir.path());
    let metadata_path = data_dir.path().join("pg_privileges.meta");

    let running = start_persistent_server(data_dir.path(), "privilege_drop_sequence").await;
    let client = &running.client;
    client
        .batch_execute("CREATE ROLE tester SUPERUSER LOGIN")
        .await
        .expect("register admin role");
    client
        .batch_execute("CREATE ROLE analyst LOGIN")
        .await
        .expect("create analyst role");
    client
        .batch_execute("CREATE SEQUENCE privilege_drop_seq")
        .await
        .expect("create privilege sequence");
    client
        .batch_execute("GRANT USAGE, SELECT ON SEQUENCE privilege_drop_seq TO analyst")
        .await
        .expect("grant sequence privileges");
    let before_drop = std::fs::read_to_string(&metadata_path).expect("privilege metadata exists");
    assert!(
        before_drop.contains("privilege_drop_seq"),
        "privilege metadata should record sequence grants before drop: {before_drop}"
    );

    client
        .batch_execute("DROP SEQUENCE privilege_drop_seq")
        .await
        .expect("drop privilege sequence");
    let stale = client
        .query_one(
            "SELECT \
                has_sequence_privilege('analyst', 'privilege_drop_seq', 'USAGE'), \
                has_sequence_privilege('analyst', 'privilege_drop_seq', 'SELECT')",
            &[],
        )
        .await
        .expect("privilege checks after sequence drop");
    assert!(
        !stale.get::<_, bool>(0),
        "dropped sequence must clear USAGE grants"
    );
    assert!(
        !stale.get::<_, bool>(1),
        "dropped sequence must clear SELECT grants"
    );
    shutdown(running).await;

    let after_drop = std::fs::read_to_string(&metadata_path).expect("privilege metadata exists");
    assert!(
        !after_drop.contains("privilege_drop_seq"),
        "dropped sequence grants must be removed from privilege metadata: {after_drop}"
    );

    let running =
        start_persistent_server(data_dir.path(), "privilege_drop_sequence_recreate").await;
    running
        .client
        .batch_execute("CREATE SEQUENCE privilege_drop_seq")
        .await
        .expect("recreate privilege sequence");
    let recreated = running
        .client
        .query_one(
            "SELECT \
                has_sequence_privilege('analyst', 'privilege_drop_seq', 'USAGE'), \
                has_sequence_privilege('analyst', 'privilege_drop_seq', 'SELECT')",
            &[],
        )
        .await
        .expect("privilege checks after sequence recreate");
    assert!(
        !recreated.get::<_, bool>(0),
        "recreated sequence must not inherit stale USAGE grant"
    );
    assert!(
        !recreated.get::<_, bool>(1),
        "recreated sequence must not inherit stale SELECT grant"
    );

    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_table_removes_owned_sequence_privilege_grants() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    support::make_data_dir_private(data_dir.path());
    let metadata_path = data_dir.path().join("pg_privileges.meta");
    let sequence_name = "privilege_owned_seq_id_seq";

    let running = start_persistent_server(data_dir.path(), "privilege_owned_sequence").await;
    let client = &running.client;
    client
        .batch_execute("CREATE ROLE tester SUPERUSER LOGIN")
        .await
        .expect("register admin role");
    client
        .batch_execute("CREATE ROLE analyst LOGIN")
        .await
        .expect("create analyst role");
    client
        .batch_execute("CREATE TABLE privilege_owned_seq (id SERIAL)")
        .await
        .expect("create serial table");
    client
        .batch_execute("GRANT USAGE ON SEQUENCE privilege_owned_seq_id_seq TO analyst")
        .await
        .expect("grant owned sequence usage");
    let before_drop = std::fs::read_to_string(&metadata_path).expect("privilege metadata exists");
    assert!(
        before_drop.contains(sequence_name),
        "privilege metadata should record owned-sequence grant before drop: {before_drop}"
    );

    client
        .batch_execute("DROP TABLE privilege_owned_seq")
        .await
        .expect("drop serial table");
    let stale = client
        .query_one(
            "SELECT has_sequence_privilege('analyst', 'privilege_owned_seq_id_seq', 'USAGE')",
            &[],
        )
        .await
        .expect("owned sequence privilege check after table drop");
    assert!(
        !stale.get::<_, bool>(0),
        "dropping a table must clear grants on its owned sequence"
    );
    shutdown(running).await;

    let after_drop = std::fs::read_to_string(&metadata_path).expect("privilege metadata exists");
    assert!(
        !after_drop.contains(sequence_name),
        "owned sequence grants must be removed from privilege metadata: {after_drop}"
    );

    let running =
        start_persistent_server(data_dir.path(), "privilege_owned_sequence_recreate").await;
    running
        .client
        .batch_execute("CREATE TABLE privilege_owned_seq (id SERIAL)")
        .await
        .expect("recreate serial table");
    let recreated = running
        .client
        .query_one(
            "SELECT has_sequence_privilege('analyst', 'privilege_owned_seq_id_seq', 'USAGE')",
            &[],
        )
        .await
        .expect("owned sequence privilege check after table recreate");
    assert!(
        !recreated.get::<_, bool>(0),
        "recreated owned sequence must not inherit stale grant"
    );

    shutdown(running).await;
}
