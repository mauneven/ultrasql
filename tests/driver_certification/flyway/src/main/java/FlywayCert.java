import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;

public final class FlywayCert {
    private FlywayCert() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: FlywayCert JDBC_URL MIGRATIONS_DIR");
        }
        String jdbcUrl = args[0];
        Path migrations = Path.of(args[1]).toAbsolutePath();

        Flyway flyway =
                Flyway.configure()
                        .dataSource(jdbcUrl, "driver_cert", "")
                        .configuration(
                                Map.of("flyway.postgresql.transactional.lock", "false"))
                        .locations("filesystem:" + migrations)
                        .schemas("public")
                        .defaultSchema("public")
                        .createSchemas(false)
                        .baselineOnMigrate(true)
                        .baselineVersion("0")
                        .executeInTransaction(false)
                        .cleanDisabled(true)
                        .load();
        MigrateResult result = flyway.migrate();
        if (result.migrationsExecuted != 2) {
            throw new AssertionError(
                    "expected 2 Flyway migrations, got " + result.migrationsExecuted);
        }

        try (Connection conn = DriverManager.getConnection(jdbcUrl, "driver_cert", "");
                Statement stmt = conn.createStatement()) {
            assertOne(
                    stmt,
                    "SELECT label, applied_by FROM flyway_cert WHERE id = 1",
                    "alpha",
                    "flyway");
            assertCount(
                    stmt,
                    "SELECT COUNT(*) FROM flyway_schema_history WHERE success = true AND type = 'SQL'",
                    2);
        }

        MigrateResult second = flyway.migrate();
        if (second.migrationsExecuted != 0) {
            throw new AssertionError(
                    "expected idempotent Flyway migrate to run 0 migrations, got "
                            + second.migrationsExecuted);
        }
    }

    private static void assertOne(Statement stmt, String sql, String left, String right)
            throws Exception {
        try (ResultSet rs = stmt.executeQuery(sql)) {
            if (!rs.next()) {
                throw new AssertionError("no row for " + sql);
            }
            if (!left.equals(rs.getString(1)) || !right.equals(rs.getString(2))) {
                throw new AssertionError(
                        "unexpected row: " + rs.getString(1) + ", " + rs.getString(2));
            }
            if (rs.next()) {
                throw new AssertionError("more than one row for " + sql);
            }
        }
    }

    private static void assertCount(Statement stmt, String sql, int expected) throws Exception {
        try (ResultSet rs = stmt.executeQuery(sql)) {
            if (!rs.next()) {
                throw new AssertionError("no count row for " + sql);
            }
            int actual = rs.getInt(1);
            if (actual != expected) {
                throw new AssertionError("expected " + expected + " rows, got " + actual);
            }
        }
    }
}
