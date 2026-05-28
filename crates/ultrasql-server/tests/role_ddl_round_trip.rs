//! Wire-level coverage for role-management DDL.

mod support;

use support::{shutdown, start_sample_server};

#[tokio::test]
async fn create_alter_drop_role_and_user_update_catalog_views() {
    let running = start_sample_server("role_ddl_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE ROLE analytics NOLOGIN CREATEDB CREATEROLE")
        .await
        .expect("create role");
    client
        .batch_execute("ALTER ROLE analytics LOGIN NOCREATEDB")
        .await
        .expect("alter role");
    client
        .batch_execute("CREATE USER app_user PASSWORD 'secret' NOSUPERUSER CREATEDB")
        .await
        .expect("create user");

    let role = client
        .query_one(
            "SELECT rolcanlogin, rolcreatedb, rolcreaterole \
             FROM pg_catalog.pg_roles \
             WHERE rolname = 'analytics'",
            &[],
        )
        .await
        .expect("analytics role visible");
    assert!(role.get::<_, bool>(0), "ALTER ROLE should enable LOGIN");
    assert!(
        !role.get::<_, bool>(1),
        "ALTER ROLE should disable CREATEDB"
    );
    assert!(role.get::<_, bool>(2), "CREATE ROLE should keep CREATEROLE");

    let user = client
        .query_one(
            "SELECT rolcanlogin, rolcreatedb, rolsuper \
             FROM pg_catalog.pg_roles \
             WHERE rolname = 'app_user'",
            &[],
        )
        .await
        .expect("app_user role visible");
    assert!(user.get::<_, bool>(0), "CREATE USER implies LOGIN");
    assert!(user.get::<_, bool>(1), "CREATEDB option should persist");
    assert!(!user.get::<_, bool>(2), "NOSUPERUSER option should persist");

    let app_user_rows = client
        .query(
            "SELECT usename FROM pg_catalog.pg_user WHERE usename = 'app_user'",
            &[],
        )
        .await
        .expect("pg_user query");
    assert_eq!(app_user_rows.len(), 1, "CREATE USER appears in pg_user");

    client
        .batch_execute("CREATE ROLE IF NOT EXISTS analytics")
        .await
        .expect("idempotent create role");
    client
        .batch_execute("DROP USER app_user")
        .await
        .expect("drop user");
    client
        .batch_execute("DROP ROLE IF EXISTS missing_role")
        .await
        .expect("idempotent drop role");
    client
        .batch_execute("DROP ROLE analytics")
        .await
        .expect("drop role");

    let remaining = client
        .query_one(
            "SELECT COUNT(*) FROM pg_catalog.pg_roles \
             WHERE rolname IN ('analytics', 'app_user')",
            &[],
        )
        .await
        .expect("roles removed");
    assert_eq!(remaining.get::<_, i64>(0), 0);

    shutdown(running).await;
}
