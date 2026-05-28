#!/usr/bin/env python3
"""Certify UltraSQL with stock SQL client drivers.

The harness starts a real `ultrasqld` process on an ephemeral localhost
port and drives it through stock psql meta-commands, direct libpq, psycopg2, psycopg3,
SQLAlchemy, Django ORM, Rails ActiveRecord, node-postgres, Go lib/pq,
Go pgx, GORM, the JDBC PostgreSQL driver, Hibernate ORM, Npgsql, Prisma,
Diesel, and GUI introspection query families used by pgAdmin, DBeaver,
DataGrip, Flyway, Liquibase, and Alembic.
It intentionally uses only public driver APIs so failures represent
client-visible wire behavior gaps.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import socket
import ssl
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


HOST = "127.0.0.1"
NODE_PG_VERSION = "8.21.0"
SQLALCHEMY_VERSION = "2.0.50"
DJANGO_VERSION = "6.0.5"
ACTIVE_RECORD_VERSION = "8.1.3"
RUBY_PG_VERSION = "1.6.3"
GO_LIBPQ_VERSION = "1.12.3"
GO_PGX_VERSION = "5.9.2"
GORM_VERSION = "1.31.1"
GORM_POSTGRES_VERSION = "1.6.0"
JDBC_VERSION = "42.7.11"
JDBC_SHA256 = "1981b31d3993c58702783c1cddf10a34e48c1f413d70ff1cb6def0a143484647"
HIBERNATE_VERSION = "7.3.5.Final"
APACHE_MAVEN_VERSION = "3.9.11"
NPGSQL_VERSION = "10.0.2"
PRISMA_VERSION = "7.8.0"
DIESEL_VERSION = "2.3.9"
FLYWAY_VERSION = "12.6.2"
LIQUIBASE_VERSION = "5.0.3"
ALEMBIC_VERSION = "1.18.4"


class CertificationFailure(AssertionError):
    """Driver-visible certification failure."""


@dataclass(frozen=True)
class DriverResult:
    """One successful driver certification record."""

    driver: str
    version: str
    checks: list[str]


def assert_equal(actual: Any, expected: Any, context: str) -> None:
    """Fail with context-rich assertion text."""

    if actual != expected:
        raise CertificationFailure(f"{context}: expected {expected!r}, got {actual!r}")


def require_tool(name: str, install_hint: str) -> str:
    """Return a required tool path or fail with install guidance."""

    path = shutil.which(name)
    if path is None:
        raise CertificationFailure(f"{name} not found on PATH; {install_hint}")
    return path


def run_checked(
    cmd: list[str],
    context: str,
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a command and fail with captured output if it exits non-zero."""

    completed = subprocess.run(
        cmd,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        raise CertificationFailure(
            f"{context}\n"
            f"command: {' '.join(cmd)}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    return completed


def download_file(url: str, target: Path) -> None:
    """Download `url` to `target` using HTTPS."""

    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        with urllib.request.urlopen(url, timeout=60) as response:
            target.write_bytes(response.read())
        return
    except (ssl.SSLError, urllib.error.URLError) as exc:
        reason = getattr(exc, "reason", exc)
        if not isinstance(reason, ssl.SSLError):
            raise

    curl = shutil.which("curl")
    if curl is None:
        raise CertificationFailure(
            "Python HTTPS certificate verification failed and curl was not found "
            f"while downloading {url}"
        )
    run_checked(
        [
            curl,
            "--fail",
            "--location",
            "--show-error",
            "--proto",
            "=https",
            "--tlsv1.2",
            "--output",
            str(target),
            url,
        ],
        f"curl download failed for {url}",
    )


def ensure_maven() -> list[str]:
    """Return a Maven command, downloading a pinned Apache Maven if needed."""

    system_maven = shutil.which("mvn")
    if system_maven is not None:
        return [system_maven]

    base_dir = repo_root() / "target" / "driver-certification" / "maven"
    archive = base_dir / f"apache-maven-{APACHE_MAVEN_VERSION}-bin.tar.gz"
    checksum = archive.with_suffix(archive.suffix + ".sha512")
    install_dir = base_dir / f"apache-maven-{APACHE_MAVEN_VERSION}"
    mvn = install_dir / "bin" / ("mvn.cmd" if os.name == "nt" else "mvn")
    if mvn.exists():
        return [str(mvn)]

    url_base = (
        "https://archive.apache.org/dist/maven/maven-3/"
        f"{APACHE_MAVEN_VERSION}/binaries"
    )
    if not archive.exists():
        download_file(f"{url_base}/{archive.name}", archive)
    if not checksum.exists():
        download_file(f"{url_base}/{checksum.name}", checksum)

    expected = checksum.read_text(encoding="utf-8").split()[0].lower()
    actual = hashlib.sha512(archive.read_bytes()).hexdigest()
    if actual != expected:
        raise CertificationFailure(
            f"Apache Maven digest mismatch: expected {expected}, got {actual}"
        )

    with tarfile.open(archive, "r:gz") as tar:
        tar.extractall(base_dir, filter="data")
    if not mvn.exists():
        raise CertificationFailure(f"Apache Maven binary not found after extraction at {mvn}")
    return [str(mvn)]


def repo_root() -> Path:
    """Return repository root from this script location."""

    return Path(__file__).resolve().parents[2]


def free_port() -> int:
    """Ask the OS for an available loopback TCP port."""

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((HOST, 0))
        return int(sock.getsockname()[1])


def wait_for_tcp(port: int, proc: subprocess.Popen[str], timeout: float = 10.0) -> None:
    """Wait until `ultrasqld` accepts TCP connections."""

    deadline = time.monotonic() + timeout
    last_error: OSError | None = None
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise CertificationFailure(
                f"ultrasqld exited before accepting connections with code {proc.returncode}"
            )
        try:
            with socket.create_connection((HOST, port), timeout=0.25):
                return
        except OSError as exc:
            last_error = exc
            time.sleep(0.05)
    raise CertificationFailure(f"ultrasqld did not accept TCP within {timeout}s: {last_error}")


def start_ultrasqld(binary: Path) -> tuple[subprocess.Popen[str], int]:
    """Start `ultrasqld` on an ephemeral localhost port."""

    if not binary.exists():
        raise CertificationFailure(
            f"ultrasqld binary not found at {binary}; run "
            "`cargo build -p ultrasql-server --bin ultrasqld` or pass --ultrasqld"
        )
    port = free_port()
    env = os.environ.copy()
    env.setdefault("RUST_BACKTRACE", "1")
    proc = subprocess.Popen(
        [
            str(binary),
            "--listen",
            f"{HOST}:{port}",
            "--log-level",
            "warn",
            "--idle-session-timeout-ms",
            "10000",
        ],
        cwd=repo_root(),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_for_tcp(port, proc)
    except BaseException:
        stop_ultrasqld(proc)
        raise
    return proc, port


def stop_ultrasqld(proc: subprocess.Popen[str]) -> tuple[str, str]:
    """Terminate `ultrasqld` and return captured output."""

    if proc.poll() is None:
        proc.terminate()
    try:
        return proc.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        return proc.communicate(timeout=5)


def dsn_uri(port: int, application_name: str) -> str:
    """Build a libpq URI with TLS disabled for local certification."""

    return (
        f"postgresql://driver_cert@{HOST}:{port}/ultrasql"
        f"?sslmode=disable&application_name={application_name}"
    )


def jdbc_url(port: int) -> str:
    """Build a JDBC URL for the PostgreSQL driver."""

    return (
        f"jdbc:postgresql://{HOST}:{port}/ultrasql"
        "?user=driver_cert"
        "&sslmode=disable"
        "&ApplicationName=driver_cert_jdbc"
        "&connectTimeout=5"
    )


def npgsql_dsn(port: int) -> str:
    """Build an Npgsql connection string."""

    return (
        f"Host={HOST};"
        f"Port={port};"
        "Username=driver_cert;"
        "Database=ultrasql;"
        "SSL Mode=Disable;"
        "Timeout=5;"
        "Command Timeout=5;"
        "Application Name=driver_cert_npgsql;"
        "Server Compatibility Mode=NoTypeLoading;"
        "Include Error Detail=true"
    )


def sqlalchemy_url(port: int) -> str:
    """Build a SQLAlchemy URL using the psycopg3 PostgreSQL dialect."""

    return (
        f"postgresql+psycopg://driver_cert@{HOST}:{port}/ultrasql"
        "?sslmode=disable&application_name=driver_cert_sqlalchemy"
        "&connect_timeout=5"
    )


def django_database_config(port: int) -> dict[str, Any]:
    """Build Django DATABASES['default'] settings for UltraSQL."""

    return {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "ultrasql",
        "USER": "driver_cert",
        "HOST": HOST,
        "PORT": str(port),
        "OPTIONS": {
            "sslmode": "disable",
            "application_name": "driver_cert_django",
            "connect_timeout": 5,
        },
    }


def gorm_dsn(port: int) -> str:
    """Build a pgx-style DSN for GORM's PostgreSQL dialector."""

    return (
        f"host={HOST} "
        f"port={port} "
        "user=driver_cert "
        "dbname=ultrasql "
        "sslmode=disable "
        "connect_timeout=5 "
        "application_name=driver_cert_gorm "
        "TimeZone=UTC"
    )


def pg_config_value(flag: str) -> str:
    """Read one value from pg_config."""

    pg_config = shutil.which("pg_config")
    if pg_config is None:
        raise CertificationFailure("pg_config not found on PATH; install libpq development files")
    return subprocess.check_output([pg_config, flag], text=True).strip()


def compile_libpq_cert(tmpdir: Path) -> Path:
    """Compile the libpq C certification program."""

    cc = os.environ.get("CC", "cc")
    source = repo_root() / "tests" / "driver_certification" / "libpq_cert.c"
    binary = tmpdir / "libpq_cert"
    include_dir = pg_config_value("--includedir")
    lib_dir = pg_config_value("--libdir")
    cmd = [
        cc,
        str(source),
        "-I",
        include_dir,
        "-L",
        lib_dir,
        f"-Wl,-rpath,{lib_dir}",
        "-lpq",
        "-o",
        str(binary),
    ]
    completed = subprocess.run(
        cmd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        raise CertificationFailure(
            "libpq certification program failed to compile\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    return binary


def certify_libpq(port: int) -> DriverResult:
    """Certify direct libpq C API calls."""

    with tempfile.TemporaryDirectory(prefix="ultrasql-libpq-cert-") as raw_tmpdir:
        cert_binary = compile_libpq_cert(Path(raw_tmpdir))
        env = os.environ.copy()
        lib_dir = pg_config_value("--libdir")
        env["LD_LIBRARY_PATH"] = os.pathsep.join(
            part for part in [lib_dir, env.get("LD_LIBRARY_PATH", "")] if part
        )
        env["DYLD_LIBRARY_PATH"] = os.pathsep.join(
            part for part in [lib_dir, env.get("DYLD_LIBRARY_PATH", "")] if part
        )
        completed = subprocess.run(
            [str(cert_binary), dsn_uri(port, "driver_cert_libpq")],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            check=False,
        )
    if completed.returncode != 0:
        raise CertificationFailure(
            "libpq certification program failed\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    return DriverResult(
        driver="libpq",
        version=pg_config_value("--version"),
        checks=[
            "startup",
            "simple_select",
            "pqexecparams_select",
            "pqexecparams_insert",
            "explicit_transaction_rollback",
            "failed_transaction_recovery",
        ],
    )


def certify_psql_meta_commands(port: int) -> DriverResult:
    """Certify stock psql meta-commands against UltraSQL catalog SQL."""

    psql = require_tool("psql", "install PostgreSQL client tools")
    script = r"""
\set ON_ERROR_STOP on
\pset pager off
CREATE TABLE psql_meta_table (id INT NOT NULL, label TEXT);
COMMENT ON TABLE psql_meta_table IS 'psql meta table';
CREATE INDEX psql_meta_table_label_idx ON psql_meta_table(label);
CREATE SEQUENCE psql_meta_seq;
CREATE MATERIALIZED VIEW psql_meta_mv AS SELECT id, label FROM psql_meta_table;
CREATE ROLE psql_meta_role LOGIN;
\d psql_meta_table
\dt
\di
\df
\dv
\du
\l
\dn
"""
    completed = subprocess.run(
        [
            psql,
            "-X",
            "-E",
            "--no-password",
            "--set",
            "ON_ERROR_STOP=1",
            dsn_uri(port, "driver_cert_psql_meta"),
        ],
        input=script,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        raise CertificationFailure(
            "psql meta-command certification failed\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    for needle in [
        "psql_meta_table",
        "psql_meta_table_label_idx",
        "psql_meta_role",
        "ultrasql",
        "public",
    ]:
        if needle not in completed.stdout:
            raise CertificationFailure(f"psql meta-command output missing {needle}")

    version = run_checked([psql, "--version"], "psql version probe failed").stdout.strip()
    return DriverResult(
        driver="psql meta-commands",
        version=version,
        checks=[
            r"\d",
            r"\dt",
            r"\di",
            r"\df",
            r"\dv",
            r"\du",
            r"\l",
            r"\dn",
        ],
    )


def certify_gui_introspection(port: int) -> DriverResult:
    """Certify schema-browser catalog probes used by GUI clients."""

    try:
        import psycopg
    except ImportError as exc:
        raise CertificationFailure(
            "psycopg missing; install tests/driver_certification/requirements.txt"
        ) from exc

    with psycopg.connect(
        **psycopg_kwargs(port, "driver_cert_gui_introspection"),
        autocommit=True,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE gui_introspection_cert ("
                "id INT PRIMARY KEY, label TEXT)"
            )
            cur.execute("CREATE INDEX gui_introspection_label_idx ON gui_introspection_cert(label)")
            cur.execute("COMMENT ON TABLE gui_introspection_cert IS 'gui introspection table'")
            cur.execute("COMMENT ON COLUMN gui_introspection_cert.label IS 'gui label'")

            cur.execute(
                "SELECT n.oid, n.nspname, pg_catalog.pg_get_userbyid(n.nspowner), "
                "n.nspacl, pg_catalog.obj_description(n.oid, 'pg_namespace') "
                "FROM pg_catalog.pg_namespace n "
                "WHERE NOT pg_catalog.pg_is_other_temp_schema(n.oid) "
                "AND n.nspname NOT IN ('pg_catalog', 'information_schema') "
                "ORDER BY n.nspname"
            )
            assert_equal(
                [row[1] for row in cur.fetchall()],
                ["public"],
                "pgAdmin schema browser catalog probe",
            )

            cur.execute(
                "SELECT c.oid, n.nspname, c.relname, c.relkind, c.relowner, "
                "c.relacl, c.reloptions, "
                "pg_catalog.obj_description(c.oid, 'pg_class') AS description "
                "FROM pg_catalog.pg_class c "
                "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = 'public' "
                "AND c.relname = 'gui_introspection_cert' "
                "ORDER BY c.relname"
            )
            rows = cur.fetchall()
            assert_equal(len(rows), 1, "DBeaver table browser row count")
            assert_equal(rows[0][2], "gui_introspection_cert", "DBeaver table browser relname")

            cur.execute(
                "SELECT a.attname, a.attnum, a.attnotnull, a.attacl, a.attoptions, "
                "t.typname, t.typowner, pg_catalog.format_type(a.atttypid, a.atttypmod), "
                "pg_catalog.col_description(a.attrelid, a.attnum), "
                "pg_catalog.pg_get_serial_sequence('gui_introspection_cert', a.attname) "
                "FROM pg_catalog.pg_attribute a "
                "JOIN pg_catalog.pg_type t ON t.oid = a.atttypid "
                "WHERE a.attrelid = 'gui_introspection_cert'::pg_catalog.regclass "
                "AND a.attnum > 0 "
                "AND NOT a.attisdropped "
                "ORDER BY a.attnum"
            )
            assert_equal(
                [row[0] for row in cur.fetchall()],
                ["id", "label"],
                "DataGrip column browser catalog probe",
            )

            cur.execute(
                "SELECT table_schema, table_name, table_type "
                "FROM information_schema.tables "
                "WHERE table_schema = 'public' "
                "AND table_name = 'gui_introspection_cert'"
            )
            assert_equal(
                cur.fetchall(),
                [("public", "gui_introspection_cert", "BASE TABLE")],
                "GUI information_schema table probe",
            )

            cur.execute(
                "SELECT indexname FROM pg_catalog.pg_indexes "
                "WHERE schemaname = 'public' "
                "AND tablename = 'gui_introspection_cert' "
                "ORDER BY indexname"
            )
            index_names = {row[0] for row in cur.fetchall()}
            if "gui_introspection_label_idx" not in index_names:
                raise CertificationFailure(
                    f"GUI index browser probe missing label index: {sorted(index_names)!r}"
                )

    return DriverResult(
        driver="GUI introspection probes",
        version="pgAdmin/DBeaver/DataGrip catalog query suite",
        checks=[
            "pgadmin_schema_browser",
            "dbeaver_table_browser",
            "datagrip_column_browser",
            "information_schema_tables",
            "index_browser",
        ],
    )


def certify_node_pg(port: int) -> DriverResult:
    """Certify node-postgres (`pg`) parameter binding and transaction recovery."""

    require_tool("node", "install Node.js")
    node_dir = repo_root() / "tests" / "driver_certification" / "node"
    script = node_dir / "node_pg_cert.mjs"
    package_json = node_dir / "node_modules" / "pg" / "package.json"
    if not package_json.exists():
        raise CertificationFailure(
            "node-postgres dependency missing; run "
            "`pnpm --dir tests/driver_certification/node install --frozen-lockfile`"
        )

    run_checked(
        ["node", str(script), dsn_uri(port, "driver_cert_node_pg")],
        "node-postgres certification program failed",
        cwd=node_dir,
    )
    version = run_checked(
        [
            "node",
            "-e",
            "process.stdout.write(require('./node_modules/pg/package.json').version)",
        ],
        "node-postgres version probe failed",
        cwd=node_dir,
    ).stdout

    return DriverResult(
        driver="node-postgres",
        version=version or NODE_PG_VERSION,
        checks=[
            "startup",
            "extended_parameter_select",
            "extended_parameter_insert",
            "explicit_transaction_rollback",
            "failed_transaction_recovery",
        ],
    )


def certify_go_drivers(port: int) -> list[DriverResult]:
    """Certify Go lib/pq, pgx, and GORM through one Go program."""

    require_tool("go", "install Go")
    go_dir = repo_root() / "tests" / "driver_certification" / "go"
    run_checked(
        ["go", "run", ".", dsn_uri(port, "driver_cert_go")],
        "Go lib/pq / pgx certification program failed",
        cwd=go_dir,
    )
    return [
        DriverResult(
            driver="lib/pq",
            version=GO_LIBPQ_VERSION,
            checks=[
                "startup",
                "extended_parameter_select",
                "extended_parameter_insert",
                "explicit_transaction_rollback",
                "failed_transaction_recovery",
            ],
        ),
        DriverResult(
            driver="pgx",
            version=GO_PGX_VERSION,
            checks=[
                "startup",
                "extended_parameter_select",
                "extended_parameter_insert",
                "explicit_transaction_rollback",
                "failed_transaction_recovery",
            ],
        ),
        DriverResult(
            driver="GORM",
            version=f"{GORM_VERSION} (gorm.io/driver/postgres {GORM_POSTGRES_VERSION})",
            checks=[
                "startup",
                "auto_migrate",
                "parameter_select",
                "orm_create_query",
                "explicit_transaction_rollback",
                "failed_transaction_recovery",
            ],
        ),
    ]


def certify_diesel(port: int) -> DriverResult:
    """Certify Diesel's PostgreSQL backend and query DSL traffic."""

    require_tool("cargo", "install Rust and Cargo")
    diesel_dir = repo_root() / "tests" / "driver_certification" / "diesel"
    run_checked(
        [
            "cargo",
            "run",
            "--quiet",
            "--manifest-path",
            str(diesel_dir / "Cargo.toml"),
            "--",
            dsn_uri(port, "driver_cert_diesel"),
        ],
        "Diesel certification program failed",
    )

    return DriverResult(
        driver="Diesel",
        version=DIESEL_VERSION,
        checks=[
            "startup",
            "query_dsl_parameter_select",
            "insert_query",
            "explicit_transaction_rollback",
            "failed_transaction_recovery",
        ],
    )


def ensure_jdbc_jar() -> Path:
    """Download the pinned JDBC PostgreSQL driver and verify its digest."""

    target_dir = repo_root() / "target" / "driver-certification" / "java"
    target_dir.mkdir(parents=True, exist_ok=True)
    jar = target_dir / f"postgresql-{JDBC_VERSION}.jar"
    if not jar.exists():
        url = (
            "https://repo1.maven.org/maven2/org/postgresql/postgresql/"
            f"{JDBC_VERSION}/postgresql-{JDBC_VERSION}.jar"
        )
        require_tool("curl", "install curl or preseed the JDBC driver jar")
        run_checked(
            ["curl", "-fsSL", "--proto", "=https", "--tlsv1.2", "-o", str(jar), url],
            "JDBC PostgreSQL driver jar download failed",
        )

    digest = hashlib.sha256(jar.read_bytes()).hexdigest()
    if digest != JDBC_SHA256:
        raise CertificationFailure(
            f"JDBC PostgreSQL driver jar digest mismatch: expected {JDBC_SHA256}, got {digest}"
        )
    return jar


def certify_jdbc(port: int) -> DriverResult:
    """Certify the JDBC PostgreSQL driver."""

    require_tool("javac", "install a JDK")
    require_tool("java", "install a JDK")
    jar = ensure_jdbc_jar()
    source = repo_root() / "tests" / "driver_certification" / "java" / "JdbcCert.java"

    with tempfile.TemporaryDirectory(prefix="ultrasql-jdbc-cert-") as raw_tmpdir:
        classes = Path(raw_tmpdir)
        run_checked(
            ["javac", "-cp", str(jar), "-d", str(classes), str(source)],
            "JDBC PostgreSQL driver certification program failed to compile",
        )
        run_checked(
            [
                "java",
                "-cp",
                os.pathsep.join([str(classes), str(jar)]),
                "JdbcCert",
                jdbc_url(port),
            ],
            "JDBC PostgreSQL driver certification program failed",
        )

    return DriverResult(
        driver="JDBC PostgreSQL driver",
        version=JDBC_VERSION,
        checks=[
            "startup",
            "prepared_statement_select",
            "prepared_statement_insert",
            "explicit_transaction_rollback",
            "failed_transaction_recovery",
        ],
    )


def certify_hibernate(port: int) -> DriverResult:
    """Certify Hibernate ORM SessionFactory and Session traffic."""

    require_tool("java", "install a JDK")
    require_tool("javac", "install a JDK")
    hibernate_dir = repo_root() / "tests" / "driver_certification" / "hibernate"
    run_checked(
        [
            *ensure_maven(),
            "-q",
            "-DskipTests",
            "compile",
            "exec:java",
            f"-Dexec.args={jdbc_url(port)}",
        ],
        "Hibernate ORM certification program failed",
        cwd=hibernate_dir,
    )

    return DriverResult(
        driver="Hibernate ORM",
        version=HIBERNATE_VERSION,
        checks=[
            "startup",
            "session_factory",
            "session_persist_query",
            "explicit_transaction_rollback",
            "failed_transaction_recovery",
        ],
    )


def run_maven_java_cert(
    project_dir: Path,
    main_class: str,
    args: list[str],
    context: str,
) -> None:
    """Compile a Maven Java cert project and run its main class directly."""

    classpath_file = project_dir / "target" / "classpath.txt"
    run_checked(
        [
            *ensure_maven(),
            "-q",
            "-DskipTests",
            "compile",
            "dependency:build-classpath",
            f"-Dmdep.outputFile={classpath_file}",
        ],
        f"{context} dependency resolution failed",
        cwd=project_dir,
    )
    classpath = classpath_file.read_text(encoding="utf-8").strip()
    runtime_classpath = os.pathsep.join(
        part
        for part in [
            str(project_dir / "target" / "classes"),
            classpath,
        ]
        if part
    )
    run_checked(
        ["java", "-cp", runtime_classpath, main_class, *args],
        context,
        cwd=project_dir,
    )


def certify_flyway(port: int) -> DriverResult:
    """Certify Flyway versioned SQL migrations through its Java API."""

    require_tool("java", "install a JDK")
    require_tool("javac", "install a JDK")
    flyway_dir = repo_root() / "tests" / "driver_certification" / "flyway"
    migrations_dir = flyway_dir / "sql"
    run_maven_java_cert(
        flyway_dir,
        "FlywayCert",
        [jdbc_url(port), str(migrations_dir)],
        "Flyway certification program failed",
    )

    return DriverResult(
        driver="Flyway",
        version=FLYWAY_VERSION,
        checks=[
            "startup",
            "versioned_sql_migrate",
            "schema_history_table",
            "ddl_dml_migration",
            "idempotent_repair_free_migrate",
        ],
    )


def certify_liquibase(port: int) -> DriverResult:
    """Certify Liquibase changelog migrations through its Java API."""

    require_tool("java", "install a JDK")
    require_tool("javac", "install a JDK")
    liquibase_dir = repo_root() / "tests" / "driver_certification" / "liquibase"
    changelog = liquibase_dir / "changelog.xml"
    run_maven_java_cert(
        liquibase_dir,
        "LiquibaseCert",
        [jdbc_url(port), str(changelog)],
        "Liquibase certification program failed",
    )

    return DriverResult(
        driver="Liquibase",
        version=LIQUIBASE_VERSION,
        checks=[
            "startup",
            "xml_changelog_update",
            "databasechangelog_table",
            "databasechangeloglock_table",
            "ddl_dml_changesets",
        ],
    )


def certify_npgsql(port: int) -> DriverResult:
    """Certify the Npgsql .NET PostgreSQL driver."""

    require_tool("dotnet", "install .NET SDK 8 or run CI driver certification")
    project = (
        repo_root()
        / "tests"
        / "driver_certification"
        / "dotnet"
        / "Ultrasql.DriverCertification.csproj"
    )
    run_checked(
        [
            "dotnet",
            "run",
            "--project",
            str(project),
            "--configuration",
            "Release",
            "--no-restore",
            "--",
            npgsql_dsn(port),
        ],
        "Npgsql certification program failed",
    )

    return DriverResult(
        driver="Npgsql",
        version=NPGSQL_VERSION,
        checks=[
            "startup",
            "extended_parameter_select",
            "extended_parameter_insert",
            "explicit_transaction_rollback",
            "failed_transaction_recovery",
        ],
    )


def write_alembic_environment(base_dir: Path, url: str) -> None:
    """Write a minimal Alembic migration environment."""

    versions = base_dir / "versions"
    versions.mkdir(parents=True, exist_ok=True)
    (base_dir / "env.py").write_text(
        """
from alembic import context
from sqlalchemy import create_engine

config = context.config


def run_migrations_online():
    engine = create_engine(
        config.get_main_option("sqlalchemy.url"),
        future=True,
        isolation_level="AUTOCOMMIT",
    )
    with engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=None,
            transactional_ddl=False,
        )
        with context.begin_transaction():
            context.run_migrations()


run_migrations_online()
""".lstrip(),
        encoding="utf-8",
    )
    (base_dir / "script.py.mako").write_text(
        """
\"\"\"${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}
\"\"\"

from alembic import op
import sqlalchemy as sa


revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}


def upgrade() -> None:
    ${upgrades if upgrades else "pass"}


def downgrade() -> None:
    ${downgrades if downgrades else "pass"}
""".lstrip(),
        encoding="utf-8",
    )
    (versions / "001_create_alembic_cert.py").write_text(
        """
from alembic import op
import sqlalchemy as sa


revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "alembic_cert",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("label", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.bulk_insert(
        sa.table(
            "alembic_cert",
            sa.column("id", sa.Integer()),
            sa.column("label", sa.Text()),
        ),
        [{"id": 1, "label": "alpha"}],
    )
    op.add_column("alembic_cert", sa.Column("applied_by", sa.Text()))
    op.execute("UPDATE alembic_cert SET applied_by = 'alembic' WHERE id = 1")
""".lstrip(),
        encoding="utf-8",
    )
    (base_dir / "alembic.ini").write_text(
        f"""
[alembic]
script_location = {base_dir}
sqlalchemy.url = {url}
""".lstrip(),
        encoding="utf-8",
    )


def certify_alembic(port: int) -> DriverResult:
    """Certify Alembic migrations through its public command API."""

    try:
        import alembic
        import sqlalchemy
        from alembic import command
        from alembic.config import Config
        from sqlalchemy import text
    except ImportError as exc:
        raise CertificationFailure(
            "Alembic missing; install tests/driver_certification/requirements.txt"
        ) from exc

    url = sqlalchemy_url(port)
    with tempfile.TemporaryDirectory(prefix="ultrasql-alembic-cert-") as raw_tmpdir:
        base_dir = Path(raw_tmpdir)
        write_alembic_environment(base_dir, url)
        config = Config(str(base_dir / "alembic.ini"))
        command.upgrade(config, "head")

        engine = sqlalchemy.create_engine(url, future=True)
        try:
            with engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT label, applied_by "
                        "FROM alembic_cert "
                        "WHERE id = :id"
                    ),
                    {"id": 1},
                ).one()
                assert_equal(tuple(row), ("alpha", "alembic"), "Alembic migrated row")
                revision = conn.execute(text("SELECT version_num FROM alembic_version")).one()
                assert_equal(tuple(revision), ("001",), "Alembic version table")
        finally:
            engine.dispose()

    return DriverResult(
        driver="Alembic",
        version=getattr(alembic, "__version__", ALEMBIC_VERSION),
        checks=[
            "startup",
            "upgrade_head",
            "version_table",
            "ddl_dml_revision",
            "sqlalchemy_connection_reuse",
        ],
    )


def psycopg_kwargs(port: int, application_name: str) -> dict[str, Any]:
    """Common connection kwargs for Python PostgreSQL drivers."""

    return {
        "host": HOST,
        "port": port,
        "user": "driver_cert",
        "dbname": "ultrasql",
        "sslmode": "disable",
        "connect_timeout": 5,
        "application_name": application_name,
    }


def certify_psycopg2(port: int) -> DriverResult:
    """Certify psycopg2 parameter binding and transaction recovery."""

    try:
        import psycopg2
    except ImportError as exc:
        raise CertificationFailure(
            "psycopg2 missing; install tests/driver_certification/requirements.txt"
        ) from exc

    conn = psycopg2.connect(**psycopg_kwargs(port, "driver_cert_psycopg2"))
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM users WHERE id = %s", (2,))
            assert_equal(cur.fetchall(), [(2, "Grace")], "psycopg2 parameterized SELECT")

            cur.execute("CREATE TABLE psycopg2_cert (id INT NOT NULL, label TEXT)")
            cur.execute("INSERT INTO psycopg2_cert VALUES (%s, %s)", (1, "alpha"))
            cur.execute("INSERT INTO psycopg2_cert VALUES (%s, %s)", (2, "beta"))
            cur.execute("SELECT id, label FROM psycopg2_cert ORDER BY id")
            assert_equal(
                cur.fetchall(),
                [(1, "alpha"), (2, "beta")],
                "psycopg2 parameterized INSERT",
            )

            cur.execute("BEGIN")
            cur.execute("INSERT INTO psycopg2_cert VALUES (%s, %s)", (3, "rollback"))
            cur.execute("ROLLBACK")
            cur.execute("SELECT COUNT(*) FROM psycopg2_cert")
            assert_equal(cur.fetchall(), [(2,)], "psycopg2 explicit transaction rollback")

            cur.execute("BEGIN")
            try:
                cur.execute("SELECT missing_column FROM psycopg2_cert")
            except Exception:
                cur.execute("ROLLBACK")
            else:
                raise CertificationFailure("psycopg2 expected missing-column failure")
            cur.execute("SELECT id FROM psycopg2_cert ORDER BY id")
            assert_equal(cur.fetchall(), [(1,), (2,)], "psycopg2 recovery after error")
    finally:
        conn.close()

    return DriverResult(
        driver="psycopg2",
        version=psycopg2.__version__,
        checks=[
            "startup",
            "extended_parameter_select",
            "extended_parameter_insert",
            "explicit_transaction_rollback",
            "failed_transaction_recovery",
        ],
    )


def certify_psycopg3(port: int) -> DriverResult:
    """Certify psycopg3 parameter binding and transaction recovery."""

    try:
        import psycopg
    except ImportError as exc:
        raise CertificationFailure(
            "psycopg missing; install tests/driver_certification/requirements.txt"
        ) from exc

    with psycopg.connect(
        **psycopg_kwargs(port, "driver_cert_psycopg3"),
        autocommit=True,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM users WHERE id = %s", (3,))
            assert_equal(cur.fetchall(), [(3, "Linus")], "psycopg3 parameterized SELECT")

            cur.execute("CREATE TABLE psycopg3_cert (id INT NOT NULL, label TEXT)")
            cur.execute("INSERT INTO psycopg3_cert VALUES (%s, %s)", (1, "alpha"))
            cur.execute("INSERT INTO psycopg3_cert VALUES (%s, %s)", (2, "beta"))
            cur.execute("SELECT id, label FROM psycopg3_cert ORDER BY id")
            assert_equal(
                cur.fetchall(),
                [(1, "alpha"), (2, "beta")],
                "psycopg3 parameterized INSERT",
            )

            cur.execute("BEGIN")
            cur.execute("INSERT INTO psycopg3_cert VALUES (%s, %s)", (3, "rollback"))
            cur.execute("ROLLBACK")
            cur.execute("SELECT COUNT(*) FROM psycopg3_cert")
            assert_equal(cur.fetchall(), [(2,)], "psycopg3 explicit transaction rollback")

            cur.execute("BEGIN")
            try:
                cur.execute("SELECT missing_column FROM psycopg3_cert")
            except Exception:
                cur.execute("ROLLBACK")
            else:
                raise CertificationFailure("psycopg3 expected missing-column failure")
            cur.execute("SELECT id FROM psycopg3_cert ORDER BY id")
            assert_equal(cur.fetchall(), [(1,), (2,)], "psycopg3 recovery after error")

    return DriverResult(
        driver="psycopg3",
        version=psycopg.__version__,
        checks=[
            "startup",
            "extended_parameter_select",
            "extended_parameter_insert",
            "explicit_transaction_rollback",
            "failed_transaction_recovery",
        ],
    )


def certify_sqlalchemy(port: int) -> DriverResult:
    """Certify SQLAlchemy Core metadata and ORM Session traffic."""

    try:
        import sqlalchemy
        from sqlalchemy import Column, Integer, MetaData, String, Table, func, select, text
        from sqlalchemy.orm import Session, registry
    except ImportError as exc:
        raise CertificationFailure(
            "SQLAlchemy missing; install tests/driver_certification/requirements.txt"
        ) from exc

    mapper_registry = registry()
    metadata = MetaData()
    table = Table(
        "sqlalchemy_cert",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("label", String(40), nullable=False),
    )

    class SqlAlchemyCert:
        """Minimal mapped class used by the certification harness."""

    mapper_registry.map_imperatively(SqlAlchemyCert, table)

    engine = sqlalchemy.create_engine(sqlalchemy_url(port), future=True)
    try:
        ddl_engine = engine.execution_options(isolation_level="AUTOCOMMIT")
        metadata.create_all(ddl_engine)
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT id, name FROM users WHERE id = :id"),
                {"id": 1},
            ).one()
            assert_equal(tuple(row), (1, "Ada"), "SQLAlchemy parameterized SELECT")

        with Session(engine) as session:
            session.add_all(
                [
                    SqlAlchemyCert(id=1, label="alpha"),
                    SqlAlchemyCert(id=2, label="beta"),
                ]
            )
            session.commit()

        with Session(engine) as session:
            rows = session.execute(
                select(table.c.id, table.c.label).order_by(table.c.id)
            ).all()
            assert_equal(
                [tuple(row) for row in rows],
                [(1, "alpha"), (2, "beta")],
                "SQLAlchemy ORM Session insert/query",
            )

        try:
            with Session(engine) as session:
                with session.begin():
                    session.add(SqlAlchemyCert(id=3, label="rollback"))
                    raise RuntimeError("rollback SQLAlchemy transaction")
        except RuntimeError as exc:
            if str(exc) != "rollback SQLAlchemy transaction":
                raise
        with Session(engine) as session:
            count = session.scalar(select(func.count()).select_from(table))
            assert_equal(count, 2, "SQLAlchemy ORM transaction rollback")

        with engine.connect() as conn:
            transaction = conn.begin()
            try:
                conn.execute(text("SELECT missing_column FROM sqlalchemy_cert"))
            except Exception:
                transaction.rollback()
            else:
                transaction.rollback()
                raise CertificationFailure("SQLAlchemy expected missing-column failure")
            rows = conn.execute(select(table.c.id).order_by(table.c.id)).all()
            assert_equal(
                [tuple(row) for row in rows],
                [(1,), (2,)],
                "SQLAlchemy recovery after error",
            )
    finally:
        engine.dispose()

    return DriverResult(
        driver="SQLAlchemy",
        version=sqlalchemy.__version__,
        checks=[
            "startup",
            "metadata_create_all_autocommit",
            "core_parameter_select",
            "orm_session_insert_query",
            "explicit_transaction_rollback",
            "failed_transaction_recovery",
        ],
    )


def certify_django(port: int) -> DriverResult:
    """Certify Django ORM model schema creation and QuerySet traffic."""

    try:
        import django
        from django.conf import settings
        from django.db import connection, models, transaction
    except ImportError as exc:
        raise CertificationFailure(
            "Django missing; install tests/driver_certification/requirements.txt"
        ) from exc

    if not settings.configured:
        settings.configure(
            DATABASES={"default": django_database_config(port)},
            DEFAULT_AUTO_FIELD="django.db.models.AutoField",
            INSTALLED_APPS=[],
            SECRET_KEY="ultrasql-driver-certification",
            USE_TZ=False,
        )
        django.setup()

    class DjangoCert(models.Model):
        """Minimal Django model used by the certification harness."""

        id = models.IntegerField(primary_key=True)
        label = models.CharField(max_length=40)

        class Meta:
            app_label = "driver_cert"
            db_table = "django_cert"

    with connection.schema_editor(atomic=False) as schema:
        schema.create_model(DjangoCert)

    with connection.cursor() as cursor:
        cursor.execute("SELECT id, name FROM users WHERE id = %s", [2])
        assert_equal(cursor.fetchall(), [(2, "Grace")], "Django cursor parameterized SELECT")

    DjangoCert.objects.create(id=1, label="alpha")
    DjangoCert.objects.create(id=2, label="beta")
    rows = list(DjangoCert.objects.order_by("id").values_list("id", "label"))
    assert_equal(
        rows,
        [(1, "alpha"), (2, "beta")],
        "Django ORM create/query",
    )

    try:
        with transaction.atomic():
            DjangoCert.objects.create(id=3, label="rollback")
            raise RuntimeError("rollback Django transaction")
    except RuntimeError as exc:
        if str(exc) != "rollback Django transaction":
            raise
    assert_equal(DjangoCert.objects.count(), 2, "Django transaction rollback")

    try:
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute("SELECT missing_column FROM django_cert")
    except Exception:
        pass
    else:
        raise CertificationFailure("Django expected missing-column failure")
    rows = list(DjangoCert.objects.order_by("id").values_list("id"))
    assert_equal(rows, [(1,), (2,)], "Django recovery after error")

    connection.close()

    return DriverResult(
        driver="Django ORM",
        version=django.get_version(),
        checks=[
            "startup",
            "schema_editor_create_model_autocommit",
            "cursor_parameter_select",
            "orm_create_queryset",
            "explicit_transaction_rollback",
            "failed_transaction_recovery",
        ],
    )


def certify_rails_active_record(port: int) -> DriverResult:
    """Certify Rails ActiveRecord connection, schema, and model traffic."""

    require_tool("ruby", "install Ruby 3.2+")
    require_tool("bundle", "install Bundler")
    rails_dir = repo_root() / "tests" / "driver_certification" / "rails"
    script = rails_dir / "active_record_cert.rb"
    run_checked(
        ["bundle", "exec", "ruby", str(script), dsn_uri(port, "driver_cert_rails")],
        "Rails ActiveRecord certification program failed",
        cwd=rails_dir,
    )

    return DriverResult(
        driver="Rails ActiveRecord",
        version=f"activerecord {ACTIVE_RECORD_VERSION}, pg {RUBY_PG_VERSION}",
        checks=[
            "startup",
            "schema_create_table",
            "orm_create_query",
            "explicit_transaction_rollback",
            "failed_transaction_recovery",
        ],
    )


def certify_prisma(port: int) -> DriverResult:
    """Certify Prisma Client raw queries, model traffic, and transactions."""

    require_tool("node", "install Node.js")
    require_tool("pnpm", "install pnpm")
    node_dir = repo_root() / "tests" / "driver_certification" / "node"
    schema = node_dir / "prisma" / "schema.prisma"
    script = node_dir / "prisma_cert.mjs"
    env = os.environ.copy()
    env["DATABASE_URL"] = dsn_uri(port, "driver_cert_prisma")

    run_checked(
        ["pnpm", "exec", "prisma", "generate", "--schema", str(schema)],
        "Prisma Client generation failed",
        cwd=node_dir,
        env=env,
    )
    run_checked(
        ["node", str(script), dsn_uri(port, "driver_cert_prisma")],
        "Prisma certification program failed",
        cwd=node_dir,
        env=env,
    )

    return DriverResult(
        driver="Prisma",
        version=PRISMA_VERSION,
        checks=[
            "startup",
            "client_generate",
            "raw_parameter_select",
            "client_create_query",
            "explicit_transaction_rollback",
            "failed_transaction_recovery",
        ],
    )


def write_report(path: Path, binary: Path, port: int, results: list[DriverResult]) -> None:
    """Write machine-readable certification evidence."""

    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ultrasqld": str(binary),
        "host": HOST,
        "port": port,
        "drivers": [
            {
                "driver": result.driver,
                "version": result.version,
                "checks": result.checks,
                "status": "pass",
            }
            for result in results
        ],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    """Parse CLI flags."""

    default_binary = repo_root() / "target" / "debug" / "ultrasqld"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ultrasqld",
        type=Path,
        default=default_binary,
        help="path to ultrasqld binary",
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        default=repo_root() / "target" / "driver-certification.json",
        help="path for certification JSON report",
    )
    return parser.parse_args()


def main() -> int:
    """Run all driver certification checks."""

    args = parse_args()
    proc: subprocess.Popen[str] | None = None
    port = 0
    try:
        proc, port = start_ultrasqld(args.ultrasqld)
        results = [
            certify_psql_meta_commands(port),
            certify_gui_introspection(port),
            certify_libpq(port),
            certify_psycopg2(port),
            certify_psycopg3(port),
            certify_sqlalchemy(port),
            certify_django(port),
            certify_rails_active_record(port),
            certify_node_pg(port),
            *certify_go_drivers(port),
            certify_jdbc(port),
            certify_hibernate(port),
            certify_flyway(port),
            certify_liquibase(port),
            certify_npgsql(port),
            certify_alembic(port),
            certify_prisma(port),
            certify_diesel(port),
        ]
        write_report(args.json_output, args.ultrasqld, port, results)
    except BaseException as exc:
        if proc is not None:
            stdout, stderr = stop_ultrasqld(proc)
            if stdout:
                print("ultrasqld stdout:", stdout, file=sys.stderr)
            if stderr:
                print("ultrasqld stderr:", stderr, file=sys.stderr)
        print(f"driver certification failed: {exc}", file=sys.stderr)
        return 1
    else:
        if proc is not None:
            stop_ultrasqld(proc)
        for result in results:
            print(f"{result.driver} {result.version}: pass")
        print(f"report: {args.json_output}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
