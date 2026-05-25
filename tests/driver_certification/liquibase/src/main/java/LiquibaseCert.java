import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.core.PostgresDatabase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.DirectoryResourceAccessor;

public final class LiquibaseCert {
    private LiquibaseCert() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: LiquibaseCert JDBC_URL CHANGELOG");
        }
        String jdbcUrl = args[0];
        Path changelog = Path.of(args[1]).toAbsolutePath();
        Path baseDir = changelog.getParent();

        try (Connection conn = DriverManager.getConnection(jdbcUrl, "driver_cert", "")) {
            conn.setAutoCommit(true);
            Database database = new UltraSqlLiquibaseDatabase();
            database.setConnection(new JdbcConnection(conn));
            database.setDefaultSchemaName("public");
            try (DirectoryResourceAccessor resources = new DirectoryResourceAccessor(baseDir);
                    Liquibase liquibase =
                            new Liquibase(changelog.getFileName().toString(), resources, database)) {
                liquibase.update(new Contexts(), new LabelExpression());
            }
        }

        try (Connection conn = DriverManager.getConnection(jdbcUrl, "driver_cert", "");
                Statement stmt = conn.createStatement()) {
            assertOne(
                    stmt,
                    "SELECT label, applied_by FROM liquibase_cert WHERE id = 1",
                    "alpha",
                    "liquibase");
            assertCount(stmt, "SELECT COUNT(*) FROM databasechangelog", 2);
            assertCount(stmt, "SELECT COUNT(*) FROM databasechangeloglock", 1);
        }
    }

    public static final class UltraSqlLiquibaseDatabase extends PostgresDatabase {
        @Override
        public boolean supportsDDLInTransaction() {
            return false;
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
