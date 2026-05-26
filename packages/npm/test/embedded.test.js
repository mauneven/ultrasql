const assert = require("node:assert/strict");
const path = require("node:path");
const test = require("node:test");

process.env.ULTRASQL_NATIVE_PATH = path.join(__dirname, "native-stub.js");

const { Database, bindSql } = require("..");

test("exports sqlite-style embedded database wrapper", async () => {
  const db = await Database.open(":memory:");

  assert.equal(db.run("CREATE TABLE lorem (info TEXT)").commandTag, "CREATE TABLE");

  const stmt = db.prepare("INSERT INTO lorem VALUES (?)");
  assert.equal(stmt.run("O'Brien").commandTag, "INSERT 0 1");
  stmt.finalize();

  const rows = db.all("SELECT id, name FROM users ORDER BY id");
  assert.deepEqual(rows, [
    { id: "1", name: "Ada" },
    { id: "2", name: "Grace" },
  ]);

  const seen = [];
  const count = db.each("SELECT id, name FROM users ORDER BY id", (err, row) => {
    assert.equal(err, null);
    seen.push(row.name);
  });
  assert.equal(count, 2);
  assert.deepEqual(seen, ["Ada", "Grace"]);
});

test("bindSql replaces placeholders outside SQL string and comment bodies", () => {
  assert.equal(
    bindSql("SELECT '?', ? -- ?\n", ["O'Brien"]),
    "SELECT '?', 'O''Brien' -- ?\n"
  );
});
