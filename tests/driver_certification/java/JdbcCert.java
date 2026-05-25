import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

final class JdbcCert {
    private JdbcCert() {
    }

    private static void fail(String context, String message) {
        throw new AssertionError(context + ": " + message);
    }

    private static void assertTrue(String context, boolean value) {
        if (!value) {
            fail(context, "assertion failed");
        }
    }

    private static void assertRow(String context, ResultSet rs, int id, String text) throws SQLException {
        assertTrue(context + " has row", rs.next());
        int actualId = rs.getInt(1);
        String actualText = rs.getString(2);
        if (actualId != id || !text.equals(actualText)) {
            fail(context, "expected (" + id + ", " + text + "), got (" + actualId + ", " + actualText + ")");
        }
    }

    private static void assertNoMoreRows(String context, ResultSet rs) throws SQLException {
        if (rs.next()) {
            fail(context, "expected no more rows");
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("usage: JdbcCert JDBC_URL");
            System.exit(2);
        }

        try (Connection conn = DriverManager.getConnection(args[0])) {
            conn.setAutoCommit(true);

            try (PreparedStatement ps = conn.prepareStatement("SELECT id, name FROM users WHERE id = ?")) {
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertRow("JDBC PostgreSQL driver parameterized SELECT", rs, 1, "Ada");
                    assertNoMoreRows("JDBC PostgreSQL driver parameterized SELECT", rs);
                }
            }

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE jdbc_cert (id INT NOT NULL, label TEXT)");
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO jdbc_cert VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setString(2, "alpha");
                ps.executeUpdate();
                ps.setInt(1, 2);
                ps.setString(2, "beta");
                ps.executeUpdate();
            }
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT id, label FROM jdbc_cert ORDER BY id")) {
                assertRow("JDBC PostgreSQL driver parameterized INSERT", rs, 1, "alpha");
                assertRow("JDBC PostgreSQL driver parameterized INSERT", rs, 2, "beta");
                assertNoMoreRows("JDBC PostgreSQL driver parameterized INSERT", rs);
            }

            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO jdbc_cert VALUES (?, ?)")) {
                ps.setInt(1, 3);
                ps.setString(2, "rollback");
                ps.executeUpdate();
            }
            conn.rollback();
            conn.setAutoCommit(true);
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM jdbc_cert")) {
                assertTrue("JDBC PostgreSQL driver rollback count has row", rs.next());
                int count = rs.getInt(1);
                if (count != 2) {
                    fail("JDBC PostgreSQL driver explicit transaction rollback", "expected 2, got " + count);
                }
            }

            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT missing_column FROM jdbc_cert");
                fail("JDBC PostgreSQL driver failed transaction", "expected missing-column failure");
            } catch (SQLException expected) {
                conn.rollback();
            }
            conn.setAutoCommit(true);
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT id, label FROM jdbc_cert ORDER BY id")) {
                assertRow("JDBC PostgreSQL driver recovery after error", rs, 1, "alpha");
                assertRow("JDBC PostgreSQL driver recovery after error", rs, 2, "beta");
                assertNoMoreRows("JDBC PostgreSQL driver recovery after error", rs);
            }
        }
    }
}
