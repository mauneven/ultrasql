import pg from "pg";

const dsn = process.argv[2];
if (!dsn) {
  console.error("usage: node_pg_cert.mjs DSN");
  process.exit(2);
}

function assertRows(actual, expected, context) {
  const actualJson = JSON.stringify(actual);
  const expectedJson = JSON.stringify(expected);
  if (actualJson !== expectedJson) {
    throw new Error(`${context}: expected ${expectedJson}, got ${actualJson}`);
  }
}

const client = new pg.Client({
  connectionString: dsn,
  ssl: false,
  application_name: "driver_cert_node_pg"
});

try {
  await client.connect();

  let result = await client.query("SELECT id, name FROM users WHERE id = $1", [1]);
  assertRows(result.rows, [{ id: 1, name: "Ada" }], "node-postgres parameterized SELECT");

  await client.query("CREATE TABLE node_pg_cert (id INT NOT NULL, label TEXT)");
  await client.query("INSERT INTO node_pg_cert VALUES ($1, $2)", [1, "alpha"]);
  await client.query("INSERT INTO node_pg_cert VALUES ($1, $2)", [2, "beta"]);
  result = await client.query("SELECT id, label FROM node_pg_cert ORDER BY id");
  assertRows(
    result.rows,
    [
      { id: 1, label: "alpha" },
      { id: 2, label: "beta" }
    ],
    "node-postgres parameterized INSERT"
  );

  await client.query("BEGIN");
  await client.query("INSERT INTO node_pg_cert VALUES ($1, $2)", [3, "rollback"]);
  await client.query("ROLLBACK");
  result = await client.query("SELECT COUNT(*) AS count FROM node_pg_cert");
  const rowCount = Number(result.rows[0].count);
  if (rowCount !== 2) {
    throw new Error(`node-postgres explicit transaction rollback: expected 2, got ${rowCount}`);
  }

  await client.query("BEGIN");
  try {
    await client.query("SELECT missing_column FROM node_pg_cert");
    throw new Error("node-postgres expected missing-column failure");
  } catch (err) {
    if (err.message === "node-postgres expected missing-column failure") {
      throw err;
    }
    await client.query("ROLLBACK");
  }
  result = await client.query("SELECT id FROM node_pg_cert ORDER BY id");
  assertRows(result.rows, [{ id: 1 }, { id: 2 }], "node-postgres recovery after error");
} finally {
  await client.end();
}
