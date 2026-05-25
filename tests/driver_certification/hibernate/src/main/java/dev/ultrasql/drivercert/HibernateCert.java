package dev.ultrasql.drivercert;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;

public final class HibernateCert {
    private HibernateCert() {
    }

    @Entity(name = "HibernateCertRecord")
    @Table(name = "hibernate_cert")
    public static final class HibernateCertRecord {
        @Id
        @Column(name = "id")
        private Integer id;

        @Column(name = "label", nullable = false)
        private String label;

        public HibernateCertRecord() {
        }

        public HibernateCertRecord(Integer id, String label) {
            this.id = id;
            this.label = label;
        }

        public Integer id() {
            return id;
        }

        public String label() {
            return label;
        }
    }

    private static void fail(String context, String message) {
        throw new AssertionError(context + ": " + message);
    }

    private static void assertRows(String context, List<HibernateCertRecord> rows, Object[][] expected) {
        if (rows.size() != expected.length) {
            fail(context, "expected " + expected.length + " rows, got " + rows.size());
        }
        for (int i = 0; i < expected.length; i++) {
            HibernateCertRecord row = rows.get(i);
            if (!row.id().equals(expected[i][0]) || !row.label().equals(expected[i][1])) {
                fail(
                    context,
                    "row " + i + " expected (" + expected[i][0] + ", " + expected[i][1]
                        + "), got (" + row.id() + ", " + row.label() + ")"
                );
            }
        }
    }

    private static SessionFactory buildSessionFactory(String jdbcUrl) {
        Properties settings = new Properties();
        settings.put("hibernate.connection.driver_class", "org.postgresql.Driver");
        settings.put("hibernate.connection.url", jdbcUrl);
        settings.put("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
        settings.put("hibernate.hbm2ddl.auto", "none");
        settings.put("hibernate.show_sql", "false");
        settings.put("hibernate.format_sql", "false");
        settings.put("hibernate.jdbc.time_zone", "UTC");
        settings.put("hibernate.temp.use_jdbc_metadata_defaults", "false");

        StandardServiceRegistry registry = new StandardServiceRegistryBuilder()
            .applySettings(settings)
            .build();
        try {
            return new MetadataSources(registry)
                .addAnnotatedClass(HibernateCertRecord.class)
                .buildMetadata()
                .buildSessionFactory();
        } catch (RuntimeException err) {
            StandardServiceRegistryBuilder.destroy(registry);
            throw err;
        }
    }

    private static void createTable(String jdbcUrl) {
        try (
            Connection connection = DriverManager.getConnection(jdbcUrl);
            Statement statement = connection.createStatement()
        ) {
            connection.setAutoCommit(true);
            statement.execute("CREATE TABLE hibernate_cert (id INT NOT NULL, label TEXT NOT NULL)");
        } catch (SQLException err) {
            throw new RuntimeException("create hibernate_cert table", err);
        }
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("usage: HibernateCert JDBC_URL");
            System.exit(2);
        }

        createTable(args[0]);
        try (SessionFactory sessionFactory = buildSessionFactory(args[0])) {
            try (Session session = sessionFactory.openSession()) {
                Transaction tx = session.beginTransaction();
                session.persist(new HibernateCertRecord(1, "alpha"));
                session.persist(new HibernateCertRecord(2, "beta"));
                tx.commit();
            }

            try (Session session = sessionFactory.openSession()) {
                List<HibernateCertRecord> rows = session
                    .createSelectionQuery(
                        "from HibernateCertRecord order by id",
                        HibernateCertRecord.class
                    )
                    .getResultList();
                assertRows(
                    "Hibernate ORM persist/query",
                    rows,
                    new Object[][] {{1, "alpha"}, {2, "beta"}}
                );
            }

            try (Session session = sessionFactory.openSession()) {
                Transaction tx = session.beginTransaction();
                session.persist(new HibernateCertRecord(3, "rollback"));
                tx.rollback();
            }
            try (Session session = sessionFactory.openSession()) {
                Long count = session
                    .createSelectionQuery("select count(r) from HibernateCertRecord r", Long.class)
                    .getSingleResult();
                if (count != 2L) {
                    fail("Hibernate ORM transaction rollback", "expected 2, got " + count);
                }
            }

            try (Session session = sessionFactory.openSession()) {
                Transaction tx = session.beginTransaction();
                try {
                    session.createNativeQuery("SELECT missing_column FROM hibernate_cert", Object.class)
                        .getResultList();
                    fail("Hibernate ORM failed transaction", "expected missing-column failure");
                } catch (RuntimeException expected) {
                    tx.rollback();
                }
            }
            try (Session session = sessionFactory.openSession()) {
                List<HibernateCertRecord> rows = session
                    .createSelectionQuery(
                        "from HibernateCertRecord order by id",
                        HibernateCertRecord.class
                    )
                    .getResultList();
                assertRows(
                    "Hibernate ORM recovery after error",
                    rows,
                    new Object[][] {{1, "alpha"}, {2, "beta"}}
                );
            }
        }
    }
}
