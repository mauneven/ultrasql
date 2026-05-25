#include <libpq-fe.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define INT4OID 23
#define TEXTOID 25

static void fail(PGconn *conn, PGresult *res, const char *context) {
    fprintf(stderr, "libpq certification failed at %s\n", context);
    if (res != NULL) {
        fprintf(stderr, "result status: %s\n", PQresStatus(PQresultStatus(res)));
        fprintf(stderr, "result error: %s\n", PQresultErrorMessage(res));
        PQclear(res);
    }
    if (conn != NULL) {
        fprintf(stderr, "connection error: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
    }
    exit(1);
}

static void expect_status(PGconn *conn, PGresult *res, ExecStatusType expected, const char *context) {
    if (PQresultStatus(res) != expected) {
        fail(conn, res, context);
    }
}

static void expect_value(PGconn *conn, PGresult *res, int row, int col, const char *expected, const char *context) {
    char *actual = PQgetvalue(res, row, col);
    if (actual == NULL || strcmp(actual, expected) != 0) {
        fprintf(stderr, "%s: expected %s, got %s\n", context, expected, actual == NULL ? "<null>" : actual);
        fail(conn, res, context);
    }
}

static PGresult *exec_ok(PGconn *conn, const char *sql, ExecStatusType expected, const char *context) {
    PGresult *res = PQexec(conn, sql);
    if (res == NULL) {
        fail(conn, res, context);
    }
    expect_status(conn, res, expected, context);
    return res;
}

static PGresult *exec_params_ok(
    PGconn *conn,
    const char *sql,
    int n_params,
    const Oid *param_types,
    const char *const *param_values,
    ExecStatusType expected,
    const char *context
) {
    PGresult *res = PQexecParams(
        conn,
        sql,
        n_params,
        param_types,
        param_values,
        NULL,
        NULL,
        0
    );
    if (res == NULL) {
        fail(conn, res, context);
    }
    expect_status(conn, res, expected, context);
    return res;
}

int main(int argc, char **argv) {
    if (argc != 2) {
        fprintf(stderr, "usage: libpq_cert DSN\n");
        return 2;
    }

    PGconn *conn = PQconnectdb(argv[1]);
    if (PQstatus(conn) != CONNECTION_OK) {
        fail(conn, NULL, "connect");
    }

    PGresult *res = exec_ok(conn, "SELECT id, name FROM users ORDER BY id", PGRES_TUPLES_OK, "simple select");
    if (PQntuples(res) != 3 || PQnfields(res) != 2) {
        fail(conn, res, "simple select shape");
    }
    expect_value(conn, res, 0, 0, "1", "simple select row 1 id");
    expect_value(conn, res, 0, 1, "Ada", "simple select row 1 name");
    expect_value(conn, res, 1, 0, "2", "simple select row 2 id");
    expect_value(conn, res, 1, 1, "Grace", "simple select row 2 name");
    expect_value(conn, res, 2, 0, "3", "simple select row 3 id");
    expect_value(conn, res, 2, 1, "Linus", "simple select row 3 name");
    PQclear(res);

    res = exec_ok(conn, "CREATE TABLE libpq_cert (id INT NOT NULL, label TEXT)", PGRES_COMMAND_OK, "create table");
    PQclear(res);

    const Oid insert_types[2] = {INT4OID, TEXTOID};
    const char *insert_one[2] = {"1", "alpha"};
    res = exec_params_ok(
        conn,
        "INSERT INTO libpq_cert VALUES ($1, $2)",
        2,
        insert_types,
        insert_one,
        PGRES_COMMAND_OK,
        "param insert one"
    );
    PQclear(res);

    const char *insert_two[2] = {"2", "beta"};
    res = exec_params_ok(
        conn,
        "INSERT INTO libpq_cert VALUES ($1, $2)",
        2,
        insert_types,
        insert_two,
        PGRES_COMMAND_OK,
        "param insert two"
    );
    PQclear(res);

    const Oid select_types[1] = {INT4OID};
    const char *select_id[1] = {"2"};
    res = exec_params_ok(
        conn,
        "SELECT id, label FROM libpq_cert WHERE id = $1",
        1,
        select_types,
        select_id,
        PGRES_TUPLES_OK,
        "param select"
    );
    if (PQntuples(res) != 1 || PQnfields(res) != 2) {
        fail(conn, res, "param select shape");
    }
    expect_value(conn, res, 0, 0, "2", "param select id");
    expect_value(conn, res, 0, 1, "beta", "param select label");
    PQclear(res);

    res = exec_ok(conn, "BEGIN", PGRES_COMMAND_OK, "begin rollback test");
    PQclear(res);
    const char *insert_three[2] = {"3", "rollback"};
    res = exec_params_ok(
        conn,
        "INSERT INTO libpq_cert VALUES ($1, $2)",
        2,
        insert_types,
        insert_three,
        PGRES_COMMAND_OK,
        "rollback insert"
    );
    PQclear(res);
    res = exec_ok(conn, "ROLLBACK", PGRES_COMMAND_OK, "rollback");
    PQclear(res);
    res = exec_ok(conn, "SELECT COUNT(*) FROM libpq_cert", PGRES_TUPLES_OK, "rollback count");
    expect_value(conn, res, 0, 0, "2", "rollback count value");
    PQclear(res);

    res = exec_ok(conn, "BEGIN", PGRES_COMMAND_OK, "begin failed tx");
    PQclear(res);
    res = PQexec(conn, "SELECT missing_column FROM libpq_cert");
    if (res == NULL || PQresultStatus(res) != PGRES_FATAL_ERROR) {
        fail(conn, res, "expected failed statement");
    }
    if (PQtransactionStatus(conn) != PQTRANS_INERROR) {
        fail(conn, res, "transaction status after failed statement");
    }
    PQclear(res);
    res = exec_ok(conn, "ROLLBACK", PGRES_COMMAND_OK, "rollback failed tx");
    PQclear(res);
    if (PQtransactionStatus(conn) != PQTRANS_IDLE) {
        fail(conn, NULL, "transaction status after rollback");
    }

    PQfinish(conn);
    return 0;
}
