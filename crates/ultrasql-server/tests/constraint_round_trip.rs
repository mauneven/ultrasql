//! End-to-end runtime constraint enforcement tests.
//!
//! Covers NOT NULL (`23502`), CHECK (`23514`), UNIQUE / PRIMARY KEY
//! (`23505`), DEFAULT, and the basic non-deferrable FOREIGN KEY
//! (`23503`) slice wired for v0.8.

mod support;

use support::{shutdown as graceful_shutdown, start_persistent_server, start_sample_server};

/// `INSERT INTO t VALUES (NULL, ...)` on a NOT NULL column fails with
/// SQLSTATE `23502`.
#[tokio::test]
async fn insert_null_into_not_null_column_returns_23502() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT)")
        .await
        .expect("create");

    let err = client
        .batch_execute("INSERT INTO t VALUES (NULL, 10)")
        .await
        .expect_err("NOT NULL column rejects NULL");
    let sqlstate = err.code().expect("server-sent SQLSTATE present");
    assert_eq!(
        sqlstate.code(),
        "23502",
        "expected not_null_violation, got {err:?}"
    );

    // The rejected row must not land in the heap.
    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after rejected INSERT");
    assert!(rows.is_empty(), "rejected INSERT must not leak rows");

    graceful_shutdown(running).await;
}

/// `INSERT INTO t VALUES (..., NULL)` on a nullable column succeeds.
#[tokio::test]
async fn insert_null_into_nullable_column_succeeds() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, NULL)")
        .await
        .expect("nullable column accepts NULL");

    let rows = client
        .query("SELECT id, v FROM t", &[])
        .await
        .expect("select after INSERT");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    let v: Option<i32> = rows[0].get(1);
    assert!(v.is_none(), "nullable column carries NULL");

    graceful_shutdown(running).await;
}

/// Omitted INSERT columns with no DEFAULT are filled with NULL.
#[tokio::test]
async fn insert_column_list_omitted_nullable_columns_fill_null() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t (id) VALUES (1)")
        .await
        .expect("nullable omitted column fills NULL");

    let rows = client
        .query("SELECT id, v FROM t", &[])
        .await
        .expect("select after INSERT");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    let v: Option<i32> = rows[0].get(1);
    assert!(v.is_none(), "omitted nullable column carries NULL");

    graceful_shutdown(running).await;
}

/// INSERT column lists map source positions to named target columns,
/// not physical table order.
#[tokio::test]
async fn insert_column_list_respects_target_order() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t (v, id) VALUES (20, 2)")
        .await
        .expect("column list order maps values");

    let rows = client
        .query("SELECT id, v FROM t", &[])
        .await
        .expect("select after INSERT");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 2);
    assert_eq!(rows[0].get::<_, i32>(1), 20);

    graceful_shutdown(running).await;
}

/// Multi-row INSERT where one row violates NOT NULL must be atomic in
/// the sense that the rejected statement leaves no rows behind.
#[tokio::test]
async fn multi_row_insert_aborts_on_not_null_violation() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)")
        .await
        .expect("create");

    let err = client
        .batch_execute("INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30)")
        .await
        .expect_err("statement rejects on NOT NULL violation");
    let sqlstate = err.code().expect("server-sent SQLSTATE present");
    assert_eq!(sqlstate.code(), "23502");

    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after rejected multi-row INSERT");
    assert!(
        rows.is_empty(),
        "rejected multi-row INSERT must not leak partial rows, got {rows:?}"
    );

    graceful_shutdown(running).await;
}

/// `UPDATE t SET col = NULL` on a NOT NULL column fails with
/// SQLSTATE `23502` and leaves the original tuple visible.
#[tokio::test]
async fn update_null_into_not_null_column_returns_23502_and_preserves_row() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10)")
        .await
        .expect("insert");

    let err = client
        .batch_execute("UPDATE t SET v = NULL WHERE id = 1")
        .await
        .expect_err("NOT NULL column rejects UPDATE to NULL");
    let sqlstate = err.code().expect("server-sent SQLSTATE present");
    assert_eq!(
        sqlstate.code(),
        "23502",
        "expected not_null_violation, got {err:?}"
    );

    let rows = client
        .query("SELECT id, v FROM t", &[])
        .await
        .expect("select after rejected UPDATE");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, i32>(1), 10);

    graceful_shutdown(running).await;
}

/// Runtime errors inside UPDATE assignments keep their SQLSTATE and do
/// not mutate the row.
#[tokio::test]
async fn update_assignment_runtime_cast_error_returns_22p02() {
    let running = start_sample_server("constraint_update_cast_test").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE t (id INT, raw TEXT);
             INSERT INTO t VALUES (1, 'not-int')",
        )
        .await
        .expect("setup");

    let err = client
        .batch_execute("UPDATE t SET id = CAST(raw AS INTEGER)")
        .await
        .expect_err("UPDATE assignment runtime cast rejects row");
    let sqlstate = err.code().expect("server-sent SQLSTATE present");
    assert_eq!(sqlstate.code(), "22P02");

    let row = client
        .query_one("SELECT id FROM t", &[])
        .await
        .expect("select after rejected UPDATE assignment");
    assert_eq!(row.get::<_, i32>(0), 1);

    graceful_shutdown(running).await;
}

/// Omitted columns with DEFAULT expressions get the default value, while
/// explicit NULL remains NULL on nullable columns.
#[tokio::test]
async fn insert_omitted_column_uses_default_expression() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT DEFAULT 7)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t (id) VALUES (1)")
        .await
        .expect("omitted column uses default");
    client
        .batch_execute("INSERT INTO t (id, v) VALUES (2, NULL)")
        .await
        .expect("explicit NULL is not rewritten to default");

    let rows = client
        .query("SELECT id, v FROM t ORDER BY id", &[])
        .await
        .expect("select after defaults");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, i32>(1), 7);
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    let v: Option<i32> = rows[1].get(1);
    assert!(v.is_none());

    graceful_shutdown(running).await;
}

/// Runtime errors inside DEFAULT expressions keep their SQLSTATE.
#[tokio::test]
async fn default_expression_runtime_error_returns_sqlstate() {
    let running = start_sample_server("constraint_default_cast_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT DEFAULT (1 / 0))")
        .await
        .expect("create");

    let err = client
        .batch_execute("INSERT INTO t (id) VALUES (1)")
        .await
        .expect_err("DEFAULT runtime expression rejects row");
    let sqlstate = err.code().expect("server-sent SQLSTATE present");
    assert_eq!(sqlstate.code(), "22012");

    let rows = client
        .query("SELECT id, v FROM t", &[])
        .await
        .expect("select after rejected DEFAULT");
    assert!(rows.is_empty());

    graceful_shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_expression_survives_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "constraint_restart_test").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE default_restart (id INT NOT NULL, v INT DEFAULT 7)")
        .await
        .expect("create");
    graceful_shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "constraint_restart_test").await;
    let client = &running.client;
    client
        .batch_execute("INSERT INTO default_restart (id) VALUES (1)")
        .await
        .expect("omitted column uses default after restart");
    let rows = client
        .query("SELECT id, v FROM default_restart", &[])
        .await
        .expect("select default row");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, i32>(1), 7);
    graceful_shutdown(running).await;
}

/// Stored generated columns are computed on INSERT and recomputed on UPDATE.
#[tokio::test]
async fn generated_stored_column_is_computed_and_recomputed() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t (a INT, b INT GENERATED ALWAYS AS (a + 1) STORED)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t (a) VALUES (4)")
        .await
        .expect("insert computes generated column");

    let rows = client
        .query("SELECT a, b FROM t", &[])
        .await
        .expect("select generated");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 4);
    assert_eq!(rows[0].get::<_, i32>(1), 5);

    client
        .batch_execute("UPDATE t SET a = 8 WHERE a = 4")
        .await
        .expect("update recomputes generated column");
    let updated = client
        .query("SELECT a, b FROM t", &[])
        .await
        .expect("select generated after update");
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].get::<_, i32>(0), 8);
    assert_eq!(updated[0].get::<_, i32>(1), 9);

    graceful_shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generated_stored_column_survives_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "constraint_restart_test").await;
    let client = &running.client;
    client
        .batch_execute(
            "CREATE TABLE generated_restart (a INT, b INT GENERATED ALWAYS AS (a + 1) STORED)",
        )
        .await
        .expect("create");
    graceful_shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "constraint_restart_test").await;
    let client = &running.client;
    client
        .batch_execute("INSERT INTO generated_restart (a) VALUES (4)")
        .await
        .expect("generated column computes after restart");
    let rows = client
        .query("SELECT a, b FROM generated_restart", &[])
        .await
        .expect("select generated row");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 4);
    assert_eq!(rows[0].get::<_, i32>(1), 5);
    let err = client
        .batch_execute("INSERT INTO generated_restart VALUES (1, 99)")
        .await
        .expect_err("explicit generated insert rejected after restart");
    assert_eq!(err.code().expect("SQLSTATE").code(), "428C9");
    graceful_shutdown(running).await;
}

/// Explicit INSERT/UPDATE values for stored generated columns are rejected.
#[tokio::test]
async fn generated_stored_column_rejects_explicit_values() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t (a INT, b INT GENERATED ALWAYS AS (a + 1) STORED)")
        .await
        .expect("create");

    let insert_err = client
        .batch_execute("INSERT INTO t VALUES (1, 2)")
        .await
        .expect_err("explicit generated INSERT rejected");
    assert_eq!(insert_err.code().expect("SQLSTATE").code(), "428C9");

    client
        .batch_execute("INSERT INTO t (a) VALUES (1)")
        .await
        .expect("insert valid row");
    let update_err = client
        .batch_execute("UPDATE t SET b = 99 WHERE a = 1")
        .await
        .expect_err("explicit generated UPDATE rejected");
    assert_eq!(update_err.code().expect("SQLSTATE").code(), "428C9");

    graceful_shutdown(running).await;
}

/// CHECK constraints reject false predicates on INSERT and keep the heap
/// unchanged.
#[tokio::test]
async fn check_constraint_rejects_insert_with_23514() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL CHECK (id > 0), v INT)")
        .await
        .expect("create");

    let err = client
        .batch_execute("INSERT INTO t VALUES (-1, 10)")
        .await
        .expect_err("CHECK rejects row");
    let sqlstate = err.code().expect("server-sent SQLSTATE present");
    assert_eq!(sqlstate.code(), "23514");

    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after rejected CHECK");
    assert!(rows.is_empty());

    graceful_shutdown(running).await;
}

/// Runtime errors inside CHECK predicates keep their SQLSTATE instead
/// of becoming an internal execution failure.
#[tokio::test]
async fn check_constraint_runtime_cast_error_returns_22p02() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t (raw TEXT CHECK (CAST(raw AS INTEGER) > 0))")
        .await
        .expect("create");

    let err = client
        .batch_execute("INSERT INTO t VALUES ('not-int')")
        .await
        .expect_err("CHECK runtime cast rejects row");
    let sqlstate = err.code().expect("server-sent SQLSTATE present");
    assert_eq!(sqlstate.code(), "22P02");

    let rows = client
        .query("SELECT raw FROM t", &[])
        .await
        .expect("select after rejected CHECK cast");
    assert!(rows.is_empty());

    graceful_shutdown(running).await;
}

/// CHECK constraints also run after UPDATE assignments.
#[tokio::test]
async fn check_constraint_rejects_update_and_preserves_row() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT CHECK (v >= 0))")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10)")
        .await
        .expect("insert");

    let err = client
        .batch_execute("UPDATE t SET v = -5 WHERE id = 1")
        .await
        .expect_err("CHECK rejects update");
    let sqlstate = err.code().expect("server-sent SQLSTATE present");
    assert_eq!(sqlstate.code(), "23514");

    let rows = client
        .query("SELECT v FROM t WHERE id = 1", &[])
        .await
        .expect("select after rejected update");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 10);

    graceful_shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn check_constraint_survives_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "constraint_restart_test").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE check_restart (id INT NOT NULL CHECK (id > 0), v INT)")
        .await
        .expect("create");
    graceful_shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "constraint_restart_test").await;
    let client = &running.client;
    let err = client
        .batch_execute("INSERT INTO check_restart VALUES (-1, 10)")
        .await
        .expect_err("CHECK rejects row after restart");
    let sqlstate = err.code().expect("server-sent SQLSTATE present");
    assert_eq!(sqlstate.code(), "23514");
    graceful_shutdown(running).await;
}

/// PRIMARY KEY creates a unique B-tree and implies NOT NULL.
#[tokio::test]
async fn primary_key_enforces_not_null_and_unique() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10)")
        .await
        .expect("insert");

    let null_err = client
        .batch_execute("INSERT INTO t VALUES (NULL, 20)")
        .await
        .expect_err("PRIMARY KEY rejects NULL");
    assert_eq!(null_err.code().expect("SQLSTATE").code(), "23502");

    let duplicate_err = client
        .batch_execute("INSERT INTO t VALUES (1, 30)")
        .await
        .expect_err("PRIMARY KEY rejects duplicate");
    assert_eq!(duplicate_err.code().expect("SQLSTATE").code(), "23505");

    let rows = client
        .query("SELECT id, v FROM t", &[])
        .await
        .expect("select after rejected pk inserts");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, i32>(1), 10);

    graceful_shutdown(running).await;
}

#[tokio::test]
async fn primary_key_allows_update_of_non_key_columns() {
    let running = start_sample_server("constraint_pk_update_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE pk_update (id INT PRIMARY KEY, label TEXT)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO pk_update VALUES (1, 'before')")
        .await
        .expect("insert");
    client
        .batch_execute("UPDATE pk_update SET label = 'after' WHERE id = 1")
        .await
        .expect("non-key update should keep primary-key index valid");

    let row = client
        .query_one("SELECT label FROM pk_update WHERE id = 1", &[])
        .await
        .expect("select updated row");
    assert_eq!(row.get::<_, String>(0), "after");

    graceful_shutdown(running).await;
}

#[tokio::test]
async fn primary_key_survives_add_column_then_update() {
    let running = start_sample_server("constraint_pk_alter_update_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE pk_alter_update (id INT PRIMARY KEY, label TEXT)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO pk_alter_update VALUES (1, 'before')")
        .await
        .expect("insert");
    client
        .batch_execute("ALTER TABLE pk_alter_update ADD COLUMN applied_by TEXT")
        .await
        .expect("add column");
    client
        .batch_execute("UPDATE pk_alter_update SET applied_by = 'flyway' WHERE id = 1")
        .await
        .expect("update after add column should keep primary-key index valid");

    let row = client
        .query_one(
            "SELECT label, applied_by FROM pk_alter_update WHERE id = 1",
            &[],
        )
        .await
        .expect("select updated row");
    assert_eq!(row.get::<_, String>(0), "before");
    assert_eq!(row.get::<_, String>(1), "flyway");

    graceful_shutdown(running).await;
}

#[tokio::test]
async fn alter_add_column_resizes_runtime_column_metadata() {
    let running = start_sample_server("constraint_alter_metadata_width_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE alter_metadata_width (id SERIAL PRIMARY KEY, label TEXT)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO alter_metadata_width (label) VALUES ('alpha')")
        .await
        .expect("insert row");
    client
        .batch_execute("ALTER TABLE alter_metadata_width ADD COLUMN applied_by TEXT")
        .await
        .expect("add column");
    client
        .batch_execute("UPDATE alter_metadata_width SET applied_by = 'alembic' WHERE id = 1")
        .await
        .expect("ALTER ADD COLUMN keeps runtime metadata width in sync");

    let row = client
        .query_one(
            "SELECT label, applied_by FROM alter_metadata_width WHERE id = 1",
            &[],
        )
        .await
        .expect("select updated row");
    assert_eq!(row.get::<_, String>(0), "alpha");
    assert_eq!(row.get::<_, String>(1), "alembic");

    graceful_shutdown(running).await;
}

/// `ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY` builds the same unique
/// enforcement used by inline primary keys.
#[tokio::test]
async fn alter_table_add_primary_key_constraint_enforces_unique() {
    let running = start_sample_server("constraint_alter_pk_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE alter_pk (id INT NOT NULL, v INT)")
        .await
        .expect("create");
    client
        .batch_execute("ALTER TABLE alter_pk ADD CONSTRAINT alter_pk_pk PRIMARY KEY (id)")
        .await
        .expect("add primary key");
    client
        .batch_execute("INSERT INTO alter_pk VALUES (1, 10)")
        .await
        .expect("insert");

    let duplicate_err = client
        .batch_execute("INSERT INTO alter_pk VALUES (1, 20)")
        .await
        .expect_err("ALTER-added PRIMARY KEY rejects duplicate");
    assert_eq!(duplicate_err.code().expect("SQLSTATE").code(), "23505");

    graceful_shutdown(running).await;
}

/// Basic non-deferrable FOREIGN KEY enforcement: child writes must find
/// a parent row, and parent key deletes/updates are restricted while a
/// child references them.
#[tokio::test]
async fn foreign_key_rejects_missing_parent_and_restricts_parent_key() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .await
        .expect("create parent");
    client
        .batch_execute("CREATE TABLE child (parent_id INT REFERENCES parent(id), v INT)")
        .await
        .expect("create child");
    client
        .batch_execute("INSERT INTO parent VALUES (1)")
        .await
        .expect("insert parent");
    client
        .batch_execute("INSERT INTO child VALUES (1, 10)")
        .await
        .expect("insert child");

    let missing = client
        .batch_execute("INSERT INTO child VALUES (2, 20)")
        .await
        .expect_err("missing parent rejected");
    assert_eq!(missing.code().expect("SQLSTATE").code(), "23503");

    let child_update = client
        .batch_execute("UPDATE child SET parent_id = 2 WHERE v = 10")
        .await
        .expect_err("child update to missing parent rejected");
    assert_eq!(child_update.code().expect("SQLSTATE").code(), "23503");

    let parent_delete = client
        .batch_execute("DELETE FROM parent WHERE id = 1")
        .await
        .expect_err("parent delete restricted");
    assert_eq!(parent_delete.code().expect("SQLSTATE").code(), "23503");

    let parent_update = client
        .batch_execute("UPDATE parent SET id = 3 WHERE id = 1")
        .await
        .expect_err("parent key update restricted");
    assert_eq!(parent_update.code().expect("SQLSTATE").code(), "23503");

    graceful_shutdown(running).await;
}

#[tokio::test]
async fn foreign_key_respects_schema_qualifier() {
    let running = start_sample_server("constraint_fk_schema_qualifier_guard").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE TABLE fk_guard_parent (id INT PRIMARY KEY)",
        )
        .await
        .expect("create public parent and separate schema");

    client
        .batch_execute(
            "CREATE TABLE fk_guard_child (\
                parent_id INT REFERENCES app.fk_guard_parent(id), \
                v INT\
             )",
        )
        .await
        .expect_err("qualified FOREIGN KEY target must not resolve public table");

    client
        .query("SELECT parent_id FROM fk_guard_child", &[])
        .await
        .expect_err("rejected child table must not be created");

    client
        .batch_execute("DROP TABLE fk_guard_parent; DROP SCHEMA app")
        .await
        .expect("cleanup FOREIGN KEY qualifier guard");

    graceful_shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn foreign_key_survives_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "constraint_restart_test").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE fk_parent_restart (id INT PRIMARY KEY)")
        .await
        .expect("create parent");
    client
        .batch_execute(
            "CREATE TABLE fk_child_restart (parent_id INT REFERENCES fk_parent_restart(id), v INT)",
        )
        .await
        .expect("create child");
    client
        .batch_execute("INSERT INTO fk_parent_restart VALUES (1)")
        .await
        .expect("insert parent");
    graceful_shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "constraint_restart_test").await;
    let client = &running.client;
    let missing = client
        .batch_execute("INSERT INTO fk_child_restart VALUES (2, 20)")
        .await
        .expect_err("missing parent rejected after restart");
    assert_eq!(missing.code().expect("SQLSTATE").code(), "23503");
    graceful_shutdown(running).await;
}

/// `DROP TABLE parent` respects live FK dependencies; CASCADE removes
/// the child-side FK while keeping the child table.
#[tokio::test]
async fn drop_table_restricts_and_cascade_drops_foreign_key_dependency() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE parent_drop (id INT PRIMARY KEY)")
        .await
        .expect("create parent");
    client
        .batch_execute("CREATE TABLE child_drop (parent_id INT REFERENCES parent_drop(id))")
        .await
        .expect("create child");

    let restricted = client
        .batch_execute("DROP TABLE parent_drop")
        .await
        .expect_err("drop parent without cascade rejected");
    assert_eq!(restricted.code().expect("SQLSTATE").code(), "2BP01");

    client
        .batch_execute("DROP TABLE parent_drop CASCADE")
        .await
        .expect("cascade drops child-side FK");
    client
        .batch_execute("INSERT INTO child_drop VALUES (123)")
        .await
        .expect("child table remains after FK dependency dropped");

    let rows = client
        .query("SELECT parent_id FROM child_drop", &[])
        .await
        .expect("child still queryable");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 123);

    graceful_shutdown(running).await;
}

/// `ON DELETE CASCADE` removes referencing child rows and keeps child
/// indexes in sync.
#[tokio::test]
async fn foreign_key_on_delete_cascade_deletes_child_rows() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .await
        .expect("create parent");
    client
        .batch_execute(
            "CREATE TABLE child (\
             parent_id INT REFERENCES parent(id) ON DELETE CASCADE, \
             v INT)",
        )
        .await
        .expect("create child");
    client
        .batch_execute("CREATE INDEX child_parent_idx ON child(parent_id)")
        .await
        .expect("create child index");
    client
        .batch_execute("INSERT INTO parent VALUES (1), (2)")
        .await
        .expect("insert parents");
    client
        .batch_execute("INSERT INTO child VALUES (1, 10), (2, 20)")
        .await
        .expect("insert children");

    client
        .batch_execute("DELETE FROM parent WHERE id = 1")
        .await
        .expect("delete parent cascades");

    let rows = client
        .query("SELECT parent_id, v FROM child ORDER BY v", &[])
        .await
        .expect("select children after cascade");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 2);
    assert_eq!(rows[0].get::<_, i32>(1), 20);

    let index_rows = client
        .query("SELECT v FROM child WHERE parent_id = 1", &[])
        .await
        .expect("index probe after cascade");
    assert!(
        index_rows.is_empty(),
        "cascaded delete must remove child index entries"
    );

    graceful_shutdown(running).await;
}

/// `ON DELETE SET NULL` rewrites referencing child rows and removes
/// old child index entries.
#[tokio::test]
async fn foreign_key_on_delete_set_null_updates_child_rows() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .await
        .expect("create parent");
    client
        .batch_execute(
            "CREATE TABLE child (\
             parent_id INT REFERENCES parent(id) ON DELETE SET NULL, \
             v INT)",
        )
        .await
        .expect("create child");
    client
        .batch_execute("CREATE INDEX child_parent_null_idx ON child(parent_id)")
        .await
        .expect("create child index");
    client
        .batch_execute("INSERT INTO parent VALUES (1), (2)")
        .await
        .expect("insert parents");
    client
        .batch_execute("INSERT INTO child VALUES (1, 10), (2, 20)")
        .await
        .expect("insert children");

    client
        .batch_execute("DELETE FROM parent WHERE id = 1")
        .await
        .expect("delete parent sets child FK null");

    let rows = client
        .query("SELECT parent_id, v FROM child ORDER BY v", &[])
        .await
        .expect("select children after SET NULL");
    assert_eq!(rows.len(), 2);
    let parent_id: Option<i32> = rows[0].get(0);
    assert!(parent_id.is_none());
    assert_eq!(rows[0].get::<_, i32>(1), 10);
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[1].get::<_, i32>(1), 20);

    let index_rows = client
        .query("SELECT v FROM child WHERE parent_id = 1", &[])
        .await
        .expect("index probe after SET NULL");
    assert!(
        index_rows.is_empty(),
        "SET NULL must remove old child index entry"
    );

    graceful_shutdown(running).await;
}

/// `ON DELETE SET DEFAULT` evaluates the child column default and rewrites
/// child indexes to the replacement key.
#[tokio::test]
async fn foreign_key_on_delete_set_default_updates_child_rows() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .await
        .expect("create parent");
    client
        .batch_execute(
            "CREATE TABLE child (\
             parent_id INT DEFAULT 7 REFERENCES parent(id) ON DELETE SET DEFAULT, \
             v INT)",
        )
        .await
        .expect("create child");
    client
        .batch_execute("CREATE INDEX child_parent_default_idx ON child(parent_id)")
        .await
        .expect("create child index");
    client
        .batch_execute("INSERT INTO parent VALUES (1), (7)")
        .await
        .expect("insert parents");
    client
        .batch_execute("INSERT INTO child VALUES (1, 10)")
        .await
        .expect("insert child");

    client
        .batch_execute("DELETE FROM parent WHERE id = 1")
        .await
        .expect("delete parent sets child FK default");

    let rows = client
        .query("SELECT parent_id, v FROM child", &[])
        .await
        .expect("select children after SET DEFAULT");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 7);
    assert_eq!(rows[0].get::<_, i32>(1), 10);

    let index_rows = client
        .query("SELECT v FROM child WHERE parent_id = 7", &[])
        .await
        .expect("index probe after SET DEFAULT");
    assert_eq!(index_rows.len(), 1);
    assert_eq!(index_rows[0].get::<_, i32>(0), 10);

    graceful_shutdown(running).await;
}

/// `ON UPDATE CASCADE` propagates parent key changes into child rows and
/// keeps child indexes in sync.
#[tokio::test]
async fn foreign_key_on_update_cascade_updates_child_rows() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .await
        .expect("create parent");
    client
        .batch_execute(
            "CREATE TABLE child (\
             parent_id INT REFERENCES parent(id) ON UPDATE CASCADE, \
             v INT)",
        )
        .await
        .expect("create child");
    client
        .batch_execute("CREATE INDEX child_parent_update_cascade_idx ON child(parent_id)")
        .await
        .expect("create child index");
    client
        .batch_execute("INSERT INTO parent VALUES (1), (2)")
        .await
        .expect("insert parents");
    client
        .batch_execute("INSERT INTO child VALUES (1, 10), (2, 20)")
        .await
        .expect("insert children");

    client
        .batch_execute("UPDATE parent SET id = 3 WHERE id = 1")
        .await
        .expect("update parent cascades");

    let rows = client
        .query("SELECT parent_id, v FROM child ORDER BY v", &[])
        .await
        .expect("select children after ON UPDATE CASCADE");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 3);
    assert_eq!(rows[0].get::<_, i32>(1), 10);
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[1].get::<_, i32>(1), 20);

    let old_key_rows = client
        .query("SELECT v FROM child WHERE parent_id = 1", &[])
        .await
        .expect("old index probe after ON UPDATE CASCADE");
    assert!(old_key_rows.is_empty());

    graceful_shutdown(running).await;
}

/// `ON UPDATE SET NULL` rewrites referencing child rows to NULL.
#[tokio::test]
async fn foreign_key_on_update_set_null_updates_child_rows() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .await
        .expect("create parent");
    client
        .batch_execute(
            "CREATE TABLE child (\
             parent_id INT REFERENCES parent(id) ON UPDATE SET NULL, \
             v INT)",
        )
        .await
        .expect("create child");
    client
        .batch_execute("INSERT INTO parent VALUES (1), (2)")
        .await
        .expect("insert parents");
    client
        .batch_execute("INSERT INTO child VALUES (1, 10), (2, 20)")
        .await
        .expect("insert children");

    client
        .batch_execute("UPDATE parent SET id = 3 WHERE id = 1")
        .await
        .expect("update parent sets child FK null");

    let rows = client
        .query("SELECT parent_id, v FROM child ORDER BY v", &[])
        .await
        .expect("select children after ON UPDATE SET NULL");
    assert_eq!(rows.len(), 2);
    let parent_id: Option<i32> = rows[0].get(0);
    assert!(parent_id.is_none());
    assert_eq!(rows[0].get::<_, i32>(1), 10);
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[1].get::<_, i32>(1), 20);

    graceful_shutdown(running).await;
}

/// `ON UPDATE SET DEFAULT` evaluates the child default and rewrites rows.
#[tokio::test]
async fn foreign_key_on_update_set_default_updates_child_rows() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .await
        .expect("create parent");
    client
        .batch_execute(
            "CREATE TABLE child (\
             parent_id INT DEFAULT 7 REFERENCES parent(id) ON UPDATE SET DEFAULT, \
             v INT)",
        )
        .await
        .expect("create child");
    client
        .batch_execute("INSERT INTO parent VALUES (1), (7)")
        .await
        .expect("insert parents");
    client
        .batch_execute("INSERT INTO child VALUES (1, 10)")
        .await
        .expect("insert child");

    client
        .batch_execute("UPDATE parent SET id = 3 WHERE id = 1")
        .await
        .expect("update parent sets child FK default");

    let rows = client
        .query("SELECT parent_id, v FROM child", &[])
        .await
        .expect("select children after ON UPDATE SET DEFAULT");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 7);
    assert_eq!(rows[0].get::<_, i32>(1), 10);

    graceful_shutdown(running).await;
}

/// `DEFERRABLE INITIALLY DEFERRED` foreign keys are checked at COMMIT,
/// so child-before-parent writes inside one transaction can succeed.
#[tokio::test]
async fn deferrable_foreign_key_allows_child_before_parent_until_commit() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .await
        .expect("create parent");
    client
        .batch_execute(
            "CREATE TABLE child (\
             parent_id INT REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED, \
             v INT)",
        )
        .await
        .expect("create child");

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("INSERT INTO child VALUES (42, 1)")
        .await
        .expect("deferred child insert");
    client
        .batch_execute("INSERT INTO parent VALUES (42)")
        .await
        .expect("parent insert");
    client.batch_execute("COMMIT").await.expect("commit");

    let rows = client
        .query("SELECT v FROM child WHERE parent_id = 42", &[])
        .await
        .expect("select child");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);

    graceful_shutdown(running).await;
}

/// A deferred FK violation surfaces at COMMIT with SQLSTATE `23503`.
#[tokio::test]
async fn deferrable_foreign_key_violation_fails_at_commit() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .await
        .expect("create parent");
    client
        .batch_execute(
            "CREATE TABLE child (\
             parent_id INT REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED, \
             v INT)",
        )
        .await
        .expect("create child");

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("INSERT INTO child VALUES (99, 1)")
        .await
        .expect("deferred child insert");
    let err = client
        .batch_execute("COMMIT")
        .await
        .expect_err("COMMIT must reject missing parent");
    assert_eq!(err.code().expect("SQLSTATE").code(), "23503");

    let rows = client
        .query("SELECT v FROM child", &[])
        .await
        .expect("select child after failed commit");
    assert!(rows.is_empty());

    graceful_shutdown(running).await;
}

/// `EXCLUDE USING gist` rejects overlapping range keys and reports
/// PostgreSQL's `exclusion_violation` SQLSTATE.
#[tokio::test]
async fn exclusion_constraint_rejects_overlapping_int4range() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE bookings (\
             room INT NOT NULL, \
             during INT4RANGE NOT NULL, \
             EXCLUDE USING gist (room WITH =, during WITH &&))",
        )
        .await
        .expect("create bookings");

    client
        .batch_execute(
            "INSERT INTO bookings VALUES \
             (101, '[1,10)'::int4range), \
             (101, '[10,20)'::int4range), \
             (102, '[5,15)'::int4range)",
        )
        .await
        .expect("non-overlapping ranges insert");

    let err = client
        .batch_execute("INSERT INTO bookings VALUES (101, '[5,6)'::int4range)")
        .await
        .expect_err("overlapping range should violate exclusion constraint");
    assert_eq!(err.code().expect("SQLSTATE").code(), "23P01");

    let rows = client
        .query(
            "SELECT room FROM bookings WHERE during && '[6,7)'::int4range",
            &[],
        )
        .await
        .expect("range overlap query");
    let rooms: Vec<i32> = rows.iter().map(|row| row.get(0)).collect();
    assert_eq!(rooms, vec![101, 102]);

    graceful_shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exclusion_constraint_survives_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "constraint_restart_test").await;
    let client = &running.client;
    client
        .batch_execute(
            "CREATE TABLE bookings_restart (\
             room INT NOT NULL, \
             during INT4RANGE NOT NULL, \
             EXCLUDE USING gist (room WITH =, during WITH &&))",
        )
        .await
        .expect("create bookings");
    client
        .batch_execute("INSERT INTO bookings_restart VALUES (101, '[1,10)'::int4range)")
        .await
        .expect("insert booking");
    graceful_shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "constraint_restart_test").await;
    let client = &running.client;
    let err = client
        .batch_execute("INSERT INTO bookings_restart VALUES (101, '[5,6)'::int4range)")
        .await
        .expect_err("overlap rejected after restart");
    assert_eq!(err.code().expect("SQLSTATE").code(), "23P01");
    graceful_shutdown(running).await;
}

/// Geometric `&&` uses GiST-style bounding-box overlap semantics.
#[tokio::test]
async fn geometric_overlap_predicate_filters_boxes() {
    let running = start_sample_server("constraint_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE shapes (id INT NOT NULL, b BOX NOT NULL)")
        .await
        .expect("create shapes");
    client
        .batch_execute(
            "INSERT INTO shapes VALUES \
             (1, '((0,0),(10,10))'::box), \
             (2, '((20,20),(30,30))'::box)",
        )
        .await
        .expect("insert boxes");

    let rows = client
        .query("SELECT id FROM shapes WHERE b && '((5,5),(6,6))'::box", &[])
        .await
        .expect("geometry overlap query");
    let ids: Vec<i32> = rows.iter().map(|row| row.get(0)).collect();
    assert_eq!(ids, vec![1]);

    graceful_shutdown(running).await;
}
